//! PBF ingestion pipeline - Step 1

use anyhow::{Context, Result};
use osmpbf::{Element, ElementReader};
use sha2::{Digest as Sha2Digest, Sha256};
use std::path::{Path, PathBuf};

use crate::formats::{Member, MemberKind, Relation, RelationsFile, Way, WaysFile};
use crate::formats::{NodeSignals, NodeSignalsFile};
use crate::formats::{nodes_sa, nodes_si};

/// (nodes, signal_node_ids) accumulated from one PBF blob during the parallel
/// node pass (#421). Aliased to keep the rayon closure return type within
/// clippy's type-complexity budget.
type NodeBlob = (Vec<(i64, f64, f64)>, Vec<i64>);

pub struct IngestConfig {
    pub input: PathBuf,
    pub outdir: PathBuf,
}

pub struct IngestResult {
    pub nodes_count: u64,
    pub signal_nodes_count: u64,
    pub ways_count: u64,
    pub relations_count: u64,
    pub nodes_sa_file: PathBuf,
    pub nodes_si_file: PathBuf,
    pub node_signals_file: PathBuf,
    pub ways_file: PathBuf,
    pub relations_file: PathBuf,
}

/// Run the complete 3-pass ingestion pipeline
pub fn run_ingest(config: IngestConfig) -> Result<IngestResult> {
    println!("🦋 Starting Step 1: PBF Ingest");
    println!("📂 Input: {}", config.input.display());
    println!("📂 Output: {}", config.outdir.display());
    println!();

    // Create output directory
    std::fs::create_dir_all(&config.outdir).context("Failed to create output directory")?;

    // Calculate input file SHA-256
    println!("Computing input file SHA-256...");
    let input_sha256 = compute_file_sha256(&config.input)?;
    println!("  ✓ SHA-256: {}", hex::encode(input_sha256));

    // Pass 1: Extract nodes (including traffic signals)
    println!("Pass 1/3: Processing nodes...");
    let node_result = extract_nodes(&config.input)?;
    println!("  ✓ Found {} nodes", node_result.nodes.len());
    println!(
        "  ✓ Found {} traffic signal nodes",
        node_result.signal_node_ids.len()
    );

    let nodes_sa_file = config.outdir.join("nodes.sa");
    let nodes_si_file = config.outdir.join("nodes.si");
    let node_signals_file = config.outdir.join("node_signals.bin");

    nodes_sa::write(&nodes_sa_file, &node_result.nodes, &input_sha256)?;
    println!("  ✓ Wrote {}", nodes_sa_file.display());

    nodes_si::write(&nodes_si_file, &node_result.nodes)?;
    println!("  ✓ Wrote {}", nodes_si_file.display());

    let signals = NodeSignals::new(node_result.signal_node_ids.clone());
    NodeSignalsFile::write(&node_signals_file, &signals, &input_sha256)?;
    println!("  ✓ Wrote {}", node_signals_file.display());

    // Pass 2: Extract ways
    println!("Pass 2/3: Processing ways...");
    let ways = extract_ways(&config.input)?;
    println!("  ✓ Found {} ways", ways.len());

    let ways_file = config.outdir.join("ways.raw");
    WaysFile::write(&ways_file, &ways)?;
    println!("  ✓ Wrote {}", ways_file.display());

    // Pass 3: Extract relations (filtered for restrictions)
    println!("Pass 3/3: Processing relations...");
    let relations = extract_relations(&config.input)?;
    println!("  ✓ Found {} relations (restrictions)", relations.len());

    let relations_file = config.outdir.join("relations.raw");
    RelationsFile::write(&relations_file, &relations)?;
    println!("  ✓ Wrote {}", relations_file.display());

    println!();
    println!("✅ Ingestion complete!");

    Ok(IngestResult {
        nodes_count: node_result.nodes.len() as u64,
        signal_nodes_count: node_result.signal_node_ids.len() as u64,
        ways_count: ways.len() as u64,
        relations_count: relations.len() as u64,
        nodes_sa_file,
        nodes_si_file,
        node_signals_file,
        ways_file,
        relations_file,
    })
}

/// Compute SHA-256 hash of a file
fn compute_file_sha256<P: AsRef<Path>>(path: P) -> Result<[u8; 32]> {
    use std::io::Read;

    let mut file = std::fs::File::open(path.as_ref())
        .with_context(|| format!("Failed to open {} for hashing", path.as_ref().display()))?;

    let mut hasher = Sha256::new();
    let mut buffer = vec![0u8; 8192];

    loop {
        let n = file.read(&mut buffer)?;
        if n == 0 {
            break;
        }
        hasher.update(&buffer[..n]);
    }

    let result = hasher.finalize();
    let mut hash = [0u8; 32];
    hash.copy_from_slice(&result);
    Ok(hash)
}

/// Result of node extraction including traffic signals
struct NodeExtractionResult {
    nodes: Vec<(i64, f64, f64)>,
    signal_node_ids: Vec<i64>,
}

/// Extract all nodes from PBF, also collecting traffic signal node IDs.
///
/// #421: decode PBF blobs in parallel (osmpbf blobs are independent). Each blob
/// accumulates into LOCAL Vecs — no per-element allocation, no lock contention —
/// then per-blob results are merged append-smaller-into-larger to bound copying.
/// par_bridge yields blobs in arbitrary order, so the result is sorted to a
/// deterministic total order afterwards (node ids are unique in OSM, so the
/// lat/lon tiebreak never triggers and the output matches the serial id-sorted
/// baseline byte-for-byte). Peak RSS stays below the (ways-pass) build peak.
fn extract_nodes<P: AsRef<Path>>(path: P) -> Result<NodeExtractionResult> {
    use osmpbf::{BlobDecode, BlobReader};
    use rayon::prelude::*;

    let reader = BlobReader::from_path(path)?;

    let (mut nodes, mut signal_node_ids) = reader
        .par_bridge()
        .map(|blob| -> Result<NodeBlob> {
            let mut nodes = Vec::new();
            let mut signals = Vec::new();
            if let BlobDecode::OsmData(block) = blob?.decode()? {
                for element in block.elements() {
                    match element {
                        Element::Node(node) => {
                            nodes.push((node.id(), node.lat(), node.lon()));
                            for (key, value) in node.tags() {
                                if key == "highway" && value == "traffic_signals" {
                                    signals.push(node.id());
                                    break;
                                }
                            }
                        }
                        Element::DenseNode(node) => {
                            nodes.push((node.id(), node.lat(), node.lon()));
                            for (key, value) in node.tags() {
                                if key == "highway" && value == "traffic_signals" {
                                    signals.push(node.id());
                                    break;
                                }
                            }
                        }
                        _ => {}
                    }
                }
            }
            Ok((nodes, signals))
        })
        .reduce(
            || Ok((Vec::new(), Vec::new())),
            |a, b| {
                let (mut an, mut asig) = a?;
                let (mut bn, mut bsig) = b?;
                if bn.len() > an.len() {
                    std::mem::swap(&mut an, &mut bn);
                }
                an.extend(bn);
                if bsig.len() > asig.len() {
                    std::mem::swap(&mut asig, &mut bsig);
                }
                asig.extend(bsig);
                Ok((an, asig))
            },
        )
        .context("Failed to read nodes")?;

    // Deterministic total order. id is unique in OSM so the lat/lon tiebreak is
    // pure insurance; the sorted sequence is identical to the serial baseline.
    nodes.sort_unstable_by(|a, b| {
        a.0.cmp(&b.0)
            .then_with(|| a.1.total_cmp(&b.1))
            .then_with(|| a.2.total_cmp(&b.2))
    });
    signal_node_ids.sort_unstable();
    signal_node_ids.dedup();

    Ok(NodeExtractionResult {
        nodes,
        signal_node_ids,
    })
}

/// Extract all ways from PBF
fn extract_ways<P: AsRef<Path>>(path: P) -> Result<Vec<Way>> {
    use std::sync::Mutex;

    let reader = ElementReader::from_path(path)?;
    let ways = Mutex::new(Vec::new());

    reader
        .for_each(|element| {
            if let Element::Way(way) = element {
                let id = way.id();
                let node_ids: Vec<i64> = way.refs().collect();

                let tags: Vec<(String, String)> = way
                    .tags()
                    .map(|(k, v)| (k.to_string(), v.to_string()))
                    .collect();

                ways.lock().unwrap().push(Way {
                    id,
                    nodes: node_ids,
                    tags,
                });
            }
        })
        .context("Failed to read ways")?;

    let mut ways = ways.into_inner().unwrap();

    // Sort by ID for determinism
    ways.sort_by_key(|w| w.id);

    Ok(ways)
}

/// Extract relations from PBF, filtering for turn restrictions
fn extract_relations<P: AsRef<Path>>(path: P) -> Result<Vec<Relation>> {
    use osmpbf::{BlobDecode, BlobReader};
    use rayon::prelude::*;

    let reader = BlobReader::from_path(path)?;

    // #421: parallel blob decode (relations are a tiny fraction of elements;
    // the cost is the full-file decode, which parallelises). Per-blob local
    // Vecs, merged append-into-larger; sorted by unique id afterwards so the
    // output is identical to the serial baseline.
    let mut relations = reader
        .par_bridge()
        .map(|blob| -> Result<Vec<Relation>> {
            let mut out = Vec::new();
            if let BlobDecode::OsmData(block) = blob?.decode()? {
                for element in block.elements() {
                    if let Element::Relation(relation) = element {
                        let tags: Vec<(String, String)> = relation
                            .tags()
                            .map(|(k, v)| (k.to_string(), v.to_string()))
                            .collect();

                        // Filter: type=restriction or restriction-related tags.
                        let is_restriction = tags.iter().any(|(k, v)| {
                            (k == "type" && v == "restriction")
                                || k.starts_with("restriction")
                                || k == "except"
                        });
                        if !is_restriction {
                            continue;
                        }

                        let members: Vec<Member> = relation
                            .members()
                            .filter_map(|member| {
                                let kind = match member.member_type {
                                    osmpbf::RelMemberType::Node => MemberKind::Node,
                                    osmpbf::RelMemberType::Way => MemberKind::Way,
                                    osmpbf::RelMemberType::Relation => return None,
                                };
                                Some(Member {
                                    role: member.role().unwrap_or("").to_string(),
                                    kind,
                                    ref_id: member.member_id,
                                })
                            })
                            .collect();

                        out.push(Relation {
                            id: relation.id(),
                            members,
                            tags,
                        });
                    }
                }
            }
            Ok(out)
        })
        .reduce(
            || Ok(Vec::new()),
            |a, b| {
                let mut av = a?;
                let mut bv = b?;
                if bv.len() > av.len() {
                    std::mem::swap(&mut av, &mut bv);
                }
                av.extend(bv);
                Ok(av)
            },
        )
        .context("Failed to read relations")?;

    // Sort by unique ID for determinism.
    relations.sort_by_key(|r| r.id);

    Ok(relations)
}

//! PBF ingestion pipeline - Step 1

use anyhow::{Context, Result};
use osmpbf::{Element, ElementReader};
use sha2::{Digest as Sha2Digest, Sha256};
use std::path::{Path, PathBuf};

use crate::formats::{Member, MemberKind, Relation, RelationsFile, Way, WaysFile};
use crate::formats::{NodeSignals, NodeSignalsFile};
use crate::formats::{nodes_sa, nodes_si};

#[cfg(test)]
mod tests;

/// One node as decoded from the PBF, before the newest-version dedupe (#652).
/// 32 bytes; the version and the signal flag travel with the node so that a
/// duplicated id keeps the coordinates AND the tags of its newest version.
#[derive(Clone, Copy, Debug)]
struct RawNode {
    id: i64,
    version: i32,
    signal: bool,
    lat: f64,
    lon: f64,
}

/// A way or relation with the version it was read with (#652).
struct Versioned<T> {
    version: i32,
    item: T,
}

/// Keep one object per id: the highest version, as `osmium merge` would have
/// kept it if the two extracts had been taken on the same day (#652).
///
/// Invariant (the step1 lock enforces it): `items` sorted by `key(item)` with
/// the ties ordered so that the last one of each id group is the survivor,
/// i.e. by `(id, version, deterministic content order)`. A merged extract
/// carries an object once per version it was seen in (same id and version →
/// one copy, different versions → both); an extract without metadata has
/// every version at 0 and never duplicates. Returns the number of copies
/// dropped.
fn keep_newest<T>(items: &mut Vec<T>, id: impl Fn(&T) -> i64) -> usize {
    let n = items.len();
    let mut w = 0usize;
    for r in 0..n {
        let last_of_group = r + 1 == n || id(&items[r + 1]) != id(&items[r]);
        if last_of_group {
            if w != r {
                items.swap(w, r);
            }
            w += 1;
        }
    }
    items.truncate(w);
    n - w
}

/// Compare two members for the deterministic tie order of `keep_newest`.
fn member_key(m: &Member) -> (&str, u8, i64) {
    (m.role.as_str(), m.kind as u8, m.ref_id)
}

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
    if node_result.dropped > 0 {
        println!(
            "  ✓ Dropped {} older copies of nodes seen in several versions (#652)",
            node_result.dropped
        );
    }
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
    let (ways, dropped_ways) = extract_ways(&config.input)?;
    println!("  ✓ Found {} ways", ways.len());
    if dropped_ways > 0 {
        println!(
            "  ✓ Dropped {} older copies of ways seen in several versions (#652)",
            dropped_ways
        );
    }

    let ways_file = config.outdir.join("ways.raw");
    WaysFile::write(&ways_file, &ways)?;
    println!("  ✓ Wrote {}", ways_file.display());

    // Pass 3: Extract relations (filtered for restrictions)
    println!("Pass 3/3: Processing relations...");
    let (relations, dropped_relations) = extract_relations(&config.input)?;
    println!("  ✓ Found {} relations (restrictions)", relations.len());
    if dropped_relations > 0 {
        println!(
            "  ✓ Dropped {} older copies of relations seen in several versions (#652)",
            dropped_relations
        );
    }

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
    /// Older copies dropped by the newest-version dedupe (#652).
    dropped: usize,
}

/// Extract all nodes from PBF, also collecting traffic signal node IDs.
///
/// #421: decode PBF blobs in parallel (osmpbf blobs are independent). Each blob
/// accumulates into LOCAL Vecs — no per-element allocation, no lock contention —
/// then per-blob results are merged append-smaller-into-larger to bound copying.
/// par_bridge yields blobs in arbitrary order, so the result is sorted to a
/// deterministic total order afterwards. Peak RSS stays below the (ways-pass)
/// build peak.
///
/// #652: a merged extract (two regions, two dates) carries a border node once
/// per version it was edited in. The sort key is `(id, version, lat, lon,
/// signal)` and `keep_newest` keeps the last of each id group, so the newest
/// version wins and its own `highway=traffic_signals` tag decides whether the
/// node is a signal. A single extract has unique ids and is unchanged.
fn extract_nodes<P: AsRef<Path>>(path: P) -> Result<NodeExtractionResult> {
    use osmpbf::{BlobDecode, BlobReader};
    use rayon::prelude::*;

    let reader = BlobReader::from_path(path)?;

    let mut raw: Vec<RawNode> = reader
        .par_bridge()
        .map(|blob| -> Result<Vec<RawNode>> {
            let mut nodes = Vec::new();
            if let BlobDecode::OsmData(block) = blob?.decode()? {
                for element in block.elements() {
                    match element {
                        Element::Node(node) => nodes.push(RawNode {
                            id: node.id(),
                            version: node.info().version().unwrap_or(0),
                            signal: node
                                .tags()
                                .any(|(key, value)| key == "highway" && value == "traffic_signals"),
                            lat: node.lat(),
                            lon: node.lon(),
                        }),
                        Element::DenseNode(node) => nodes.push(RawNode {
                            id: node.id(),
                            version: node.info().map_or(0, |info| info.version()),
                            signal: node
                                .tags()
                                .any(|(key, value)| key == "highway" && value == "traffic_signals"),
                            lat: node.lat(),
                            lon: node.lon(),
                        }),
                        _ => {}
                    }
                }
            }
            Ok(nodes)
        })
        .reduce(
            || Ok(Vec::new()),
            |a, b| {
                let mut an = a?;
                let mut bn = b?;
                if bn.len() > an.len() {
                    std::mem::swap(&mut an, &mut bn);
                }
                an.extend(bn);
                Ok(an)
            },
        )
        .context("Failed to read nodes")?;

    // Deterministic total order; the last of each id group is the newest copy.
    raw.sort_unstable_by(|a, b| {
        a.id.cmp(&b.id)
            .then_with(|| a.version.cmp(&b.version))
            .then_with(|| a.lat.total_cmp(&b.lat))
            .then_with(|| a.lon.total_cmp(&b.lon))
            .then_with(|| a.signal.cmp(&b.signal))
    });
    let dropped = keep_newest(&mut raw, |n| n.id);

    let signal_node_ids: Vec<i64> = raw.iter().filter(|n| n.signal).map(|n| n.id).collect();
    let nodes: Vec<(i64, f64, f64)> = raw.iter().map(|n| (n.id, n.lat, n.lon)).collect();

    Ok(NodeExtractionResult {
        nodes,
        signal_node_ids,
        dropped,
    })
}

/// Extract all ways from PBF. Returns the ways and the number of older copies
/// dropped (#652: one object per id, the newest version wins).
fn extract_ways<P: AsRef<Path>>(path: P) -> Result<(Vec<Way>, usize)> {
    let reader = ElementReader::from_path(path)?;
    let mut ways: Vec<Versioned<Way>> = Vec::new();

    // `for_each` walks the blobs in file order on this thread.
    reader
        .for_each(|element| {
            if let Element::Way(way) = element {
                let id = way.id();
                let node_ids: Vec<i64> = way.refs().collect();

                let tags: Vec<(String, String)> = way
                    .tags()
                    .map(|(k, v)| (k.to_string(), v.to_string()))
                    .collect();

                ways.push(Versioned {
                    version: way.info().version().unwrap_or(0),
                    item: Way {
                        id,
                        nodes: node_ids,
                        tags,
                    },
                });
            }
        })
        .context("Failed to read ways")?;

    // Sort by (id, version, content) for determinism; keep the newest per id.
    ways.sort_by(|a, b| {
        a.item
            .id
            .cmp(&b.item.id)
            .then_with(|| a.version.cmp(&b.version))
            .then_with(|| a.item.nodes.cmp(&b.item.nodes))
            .then_with(|| a.item.tags.cmp(&b.item.tags))
    });
    let dropped = keep_newest(&mut ways, |w| w.item.id);

    Ok((ways.into_iter().map(|w| w.item).collect(), dropped))
}

/// Extract relations from PBF, filtering for turn restrictions. Returns the
/// relations and the number of older copies dropped (#652).
fn extract_relations<P: AsRef<Path>>(path: P) -> Result<(Vec<Relation>, usize)> {
    use osmpbf::{BlobDecode, BlobReader};
    use rayon::prelude::*;

    let reader = BlobReader::from_path(path)?;

    // #421: parallel blob decode (relations are a tiny fraction of elements;
    // the cost is the full-file decode, which parallelises). Per-blob local
    // Vecs, merged append-into-larger; sorted by (id, version, content)
    // afterwards so the output is identical to the serial baseline.
    let mut relations: Vec<Versioned<Relation>> = reader
        .par_bridge()
        .map(|blob| -> Result<Vec<Versioned<Relation>>> {
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

                        out.push(Versioned {
                            version: relation.info().version().unwrap_or(0),
                            item: Relation {
                                id: relation.id(),
                                members,
                                tags,
                            },
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

    // Sort by (id, version, content) for determinism; keep the newest per id.
    relations.sort_by(|a, b| {
        a.item
            .id
            .cmp(&b.item.id)
            .then_with(|| a.version.cmp(&b.version))
            .then_with(|| a.item.tags.cmp(&b.item.tags))
            .then_with(|| {
                a.item
                    .members
                    .iter()
                    .map(member_key)
                    .cmp(b.item.members.iter().map(member_key))
            })
    });
    let dropped = keep_newest(&mut relations, |r| r.item.id);

    Ok((relations.into_iter().map(|r| r.item).collect(), dropped))
}

//! Step 2: Modal profiling pipeline (declarative model evaluation)
//!
//! Auto-discovers `*.model.json` files, compiles them against tag dictionaries,
//! and evaluates every way/relation through each model. No hardcoded profiles.

use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, HashMap};
use std::path::{Path, PathBuf};

use super::{CompiledModel, compile_model, evaluate_turn_full, evaluate_way};
use crate::density::{DensityClassifier, WayTagsView};
use crate::formats::{TurnRule, WayAttr, turn_rules, way_attrs};
use crate::profile_abi::{Mode, TurnRuleKind, WayOutput};

pub struct ProfileConfig {
    pub ways_path: PathBuf,
    pub relations_path: PathBuf,
    pub models_dir: PathBuf,
    pub outdir: PathBuf,
    /// Strategy used to assign `DensityClass` per way. Defaults to OsmTag.
    pub density_classifier: DensityClassifier,
}

impl Default for ProfileConfig {
    fn default() -> Self {
        Self {
            ways_path: PathBuf::new(),
            relations_path: PathBuf::new(),
            models_dir: PathBuf::new(),
            outdir: PathBuf::new(),
            density_classifier: DensityClassifier::OsmTag,
        }
    }
}

/// Per-mode output paths produced by Step 2
#[derive(Debug)]
pub struct ModeProfileOutput {
    pub mode_name: String,
    pub mode_index: u8,
    pub way_attrs_path: PathBuf,
    pub turn_rules_path: PathBuf,
}

pub struct ProfileResult {
    pub modes: Vec<ModeProfileOutput>,
    pub profile_meta_path: PathBuf,
}

impl ProfileResult {
    /// Build a HashMap<String, PathBuf> of way_attrs files keyed by mode name
    pub fn way_attrs_by_name(&self) -> HashMap<String, PathBuf> {
        self.modes
            .iter()
            .map(|m| (m.mode_name.clone(), m.way_attrs_path.clone()))
            .collect()
    }

    /// Build a HashMap<String, PathBuf> of turn_rules files keyed by mode name
    pub fn turn_rules_by_name(&self) -> HashMap<String, PathBuf> {
        self.modes
            .iter()
            .map(|m| (m.mode_name.clone(), m.turn_rules_path.clone()))
            .collect()
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ProfileMeta {
    pub abi_version: u32,
    // #425: BTreeMap (sorted keys) so profile_meta.json serialises deterministically.
    // HashMap iteration order is randomised per process, and profile_meta_sha256 is
    // part of Step2LockFile — HashMap here made the step2 lock non-reproducible.
    pub profile_versions: BTreeMap<String, u32>,
    pub highway_classes: BTreeMap<u16, String>,
    pub surface_classes: BTreeMap<u16, String>,
    pub class_bits: BTreeMap<String, u32>,
    pub ways_sha256: String,
    pub relations_sha256: String,
    pub way_attrs_sha256: BTreeMap<String, String>,
    pub turn_rules_sha256: BTreeMap<String, String>,
}

pub fn build_highway_classes() -> HashMap<u16, String> {
    let mut classes = HashMap::new();
    classes.insert(1, "motorway".to_string());
    classes.insert(2, "trunk".to_string());
    classes.insert(3, "primary".to_string());
    classes.insert(4, "secondary".to_string());
    classes.insert(5, "tertiary".to_string());
    classes.insert(6, "unclassified".to_string());
    classes.insert(7, "residential".to_string());
    classes.insert(8, "motorway_link".to_string());
    classes.insert(9, "trunk_link".to_string());
    classes.insert(10, "primary_link".to_string());
    classes.insert(11, "secondary_link".to_string());
    classes.insert(12, "tertiary_link".to_string());
    classes.insert(13, "living_street".to_string());
    classes.insert(14, "service".to_string());
    classes.insert(15, "track".to_string());
    classes.insert(16, "pedestrian".to_string());
    classes.insert(17, "footway".to_string());
    classes.insert(18, "path".to_string());
    classes.insert(19, "steps".to_string());
    classes.insert(20, "cycleway".to_string());
    classes
}

pub fn build_surface_classes() -> HashMap<u16, String> {
    let mut classes = HashMap::new();
    classes.insert(0, "unknown".to_string());
    classes.insert(1, "paved".to_string());
    classes.insert(2, "asphalt".to_string());
    classes.insert(3, "concrete".to_string());
    classes.insert(4, "paving_stones".to_string());
    classes.insert(5, "unpaved".to_string());
    classes.insert(6, "compacted".to_string());
    classes.insert(7, "gravel".to_string());
    classes.insert(8, "dirt".to_string());
    classes.insert(9, "ground".to_string());
    classes.insert(10, "grass".to_string());
    classes.insert(11, "sand".to_string());
    classes
}

pub fn build_class_bits() -> HashMap<String, u32> {
    let mut bits = HashMap::new();
    bits.insert("access_fwd".to_string(), 0);
    bits.insert("access_rev".to_string(), 1);
    bits.insert("oneway_shift".to_string(), 2);
    bits.insert("toll".to_string(), 4);
    bits.insert("ferry".to_string(), 5);
    bits.insert("tunnel".to_string(), 6);
    bits.insert("bridge".to_string(), 7);
    bits.insert("link".to_string(), 8);
    bits.insert("residential".to_string(), 9);
    bits.insert("track".to_string(), 10);
    bits.insert("cycleway".to_string(), 11);
    bits.insert("footway".to_string(), 12);
    bits.insert("living_street".to_string(), 13);
    bits.insert("service".to_string(), 14);
    bits.insert("construction".to_string(), 15);
    bits
}

fn compute_file_sha256<P: AsRef<Path>>(path: P) -> Result<String> {
    use sha2::{Digest, Sha256};
    use std::fs::File;
    use std::io::Read;

    let mut file = File::open(path)?;
    let mut hasher = Sha256::new();
    let mut buffer = [0u8; 8192];

    loop {
        let n = file.read(&mut buffer)?;
        if n == 0 {
            break;
        }
        hasher.update(&buffer[..n]);
    }

    Ok(hex::encode(hasher.finalize()))
}

/// Run Step 2 profiling pipeline using auto-discovered JSON model files
pub fn run_profiling(config: ProfileConfig) -> Result<ProfileResult> {
    println!("Starting Step 2: Modal Profiling (declarative models)");
    println!("  Ways: {}", config.ways_path.display());
    println!("  Relations: {}", config.relations_path.display());
    println!("  Models: {}", config.models_dir.display());
    println!("  Output: {}", config.outdir.display());
    println!();

    std::fs::create_dir_all(&config.outdir).context("Failed to create output directory")?;

    // Discover models from *.model.json files
    let modes = super::discover_modes(&config.models_dir)?;
    println!("Discovered {} modes:", modes.len());
    for m in &modes {
        println!("  [{}] {}", m.index, m.name);
    }
    println!();

    // Load dictionaries from ways.raw
    println!("Loading dictionaries from ways.raw...");
    let (key_dict, val_dict, dict_k_sha256, dict_v_sha256) =
        crate::formats::WaysFile::read_dictionaries(&config.ways_path)?;
    println!("  key dictionary: {} entries", key_dict.len());
    println!("  value dictionary: {} entries", val_dict.len());

    // Compile all models against the way dictionaries
    let compiled_models: Vec<CompiledModel> = modes
        .iter()
        .map(|mode_info| {
            let model_path = super::model_file_path(&config.models_dir, &mode_info.name);
            let schema = super::load_model_schema(&model_path)?;
            let sha256 = super::compute_model_sha256(&model_path)?;
            Ok(compile_model(
                &schema,
                mode_info.index,
                sha256,
                &key_dict,
                &val_dict,
            ))
        })
        .collect::<Result<Vec<_>>>()?;
    println!("  compiled {} models", compiled_models.len());

    // Stream and process ways through all compiled models
    println!();
    println!("Streaming and processing ways...");
    let n_modes = modes.len();
    let mut way_attrs_per_mode: Vec<Vec<WayAttr>> = vec![Vec::new(); n_modes];

    // Density classifier: same call for every mode (density is mode-agnostic).
    if config.density_classifier == DensityClassifier::ExternalParquet {
        anyhow::bail!(
            "density classifier 'external-parquet' is not implemented in this build; \
             use --density-classifier osm-tag (the external classification plug-in is a follow-up)"
        );
    }

    // Reverse-map highway_class u16 -> highway name for the density classifier.
    let highway_classes = build_highway_classes();

    // #420: parallelise the per-way evaluation. Per way the work (density
    // classify + one evaluate_way per mode) is independent and read-only over
    // the compiled models + dictionaries. We pull the serial decode stream in
    // BOUNDED chunks and rayon-evaluate each chunk: peak RSS stays flat (only
    // one chunk of decoded ways is resident on top of the already-resident
    // per-mode output Vecs — it does NOT scale with file size). Output bytes are
    // order-independent (way_attrs is written sorted by the unique way_id), but
    // we accumulate in stream order anyway for robustness. density_hist is an
    // exact integer sum, so order does not affect it.
    use rayon::prelude::*;
    const CHUNK_WAYS: usize = 65_536;
    let density_classifier = config.density_classifier;

    let mut way_stream = crate::formats::WaysFile::stream_ways(&config.ways_path)?;
    let mut count = 0u64;
    let mut next_progress = 1_000_000u64;
    let mut density_hist: [u64; 5] = [0; 5];
    let mut chunk: Vec<(i64, Vec<u32>, Vec<u32>)> = Vec::with_capacity(CHUNK_WAYS);

    loop {
        // Fill one bounded chunk from the (serial) decode stream.
        chunk.clear();
        for result in way_stream.by_ref() {
            let (way_id, keys, vals, _nodes) = result?;
            chunk.push((way_id, keys, vals));
            if chunk.len() >= CHUNK_WAYS {
                break;
            }
        }
        if chunk.is_empty() {
            break;
        }

        // Evaluate the chunk in parallel; collect() preserves chunk index order.
        let results: Vec<(i64, u8, Vec<WayOutput>)> = chunk
            .par_iter()
            .map(|(way_id, keys, vals)| {
                // Density class is mode-agnostic — compute once per way (one
                // extra eval just to resolve the highway tag; any model works
                // since they share dictionaries).
                let out0 = evaluate_way(&compiled_models[0], keys, vals, &val_dict);
                let highway_name = highway_classes
                    .get(&out0.highway_class)
                    .map(|s| s.as_str())
                    .unwrap_or("");
                let view = WayTagsView {
                    keys: keys.as_slice(),
                    vals: vals.as_slice(),
                    key_dict: &key_dict,
                    val_dict: &val_dict,
                };
                let dclass =
                    crate::density::classify_osm_tag(density_classifier, highway_name, &view)
                        .to_u8();
                let outputs: Vec<WayOutput> = compiled_models
                    .iter()
                    .map(|compiled| {
                        let mut output = evaluate_way(compiled, keys, vals, &val_dict);
                        output.density_class = dclass;
                        output
                    })
                    .collect();
                (*way_id, dclass, outputs)
            })
            .collect();

        // Accumulate serially (deterministic).
        for (way_id, dclass, outputs) in results {
            density_hist[dclass as usize] += 1;
            for (i, output) in outputs.into_iter().enumerate() {
                way_attrs_per_mode[i].push(WayAttr { way_id, output });
            }
        }

        count += chunk.len() as u64;
        if count >= next_progress {
            println!("  processed {} ways...", count);
            next_progress += 1_000_000;
        }
    }

    println!("  processed {} ways across {} modes", count, n_modes);
    println!(
        "  density histogram: urban_high={} urban_medium={} urban_low={} suburban={} rural={}",
        density_hist[0], density_hist[1], density_hist[2], density_hist[3], density_hist[4]
    );

    // Write way_attrs files
    println!();
    println!("Writing way_attrs files...");
    let mut mode_outputs: Vec<ModeProfileOutput> = Vec::new();

    for (i, mode_info) in modes.iter().enumerate() {
        let filename = format!("way_attrs.{}.bin", mode_info.name);
        let path = config.outdir.join(&filename);
        let mode = Mode(mode_info.index);

        way_attrs::write(
            &path,
            mode,
            &way_attrs_per_mode[i],
            &dict_k_sha256,
            &dict_v_sha256,
        )?;
        println!(
            "  wrote {} ({} ways)",
            filename,
            way_attrs_per_mode[i].len()
        );

        mode_outputs.push(ModeProfileOutput {
            mode_name: mode_info.name.clone(),
            mode_index: mode_info.index,
            way_attrs_path: path,
            turn_rules_path: PathBuf::new(), // filled below
        });
    }

    // Drop way attrs to free memory before processing relations
    drop(way_attrs_per_mode);

    // Load dictionaries from relations.raw
    println!();
    println!("Loading dictionaries from relations.raw...");
    let (rel_key_dict, rel_val_dict, rel_dict_k_sha256, rel_dict_v_sha256) =
        crate::formats::RelationsFile::read_dictionaries(&config.relations_path)?;
    println!("  key dictionary: {} entries", rel_key_dict.len());
    println!("  value dictionary: {} entries", rel_val_dict.len());

    // Compile models against relation dictionaries for turn restriction evaluation
    let compiled_turn_models: Vec<CompiledModel> = modes
        .iter()
        .map(|mode_info| {
            let model_path = super::model_file_path(&config.models_dir, &mode_info.name);
            let schema = super::load_model_schema(&model_path)?;
            let sha256 = super::compute_model_sha256(&model_path)?;
            Ok(compile_model(
                &schema,
                mode_info.index,
                sha256,
                &rel_key_dict,
                &rel_val_dict,
            ))
        })
        .collect::<Result<Vec<_>>>()?;

    // Build reverse indexes for relations
    let rel_key_reverse: HashMap<&str, u32> = rel_key_dict
        .iter()
        .map(|(id, s)| (s.as_str(), *id))
        .collect();
    let rel_val_reverse: HashMap<&str, u32> = rel_val_dict
        .iter()
        .map(|(id, s)| (s.as_str(), *id))
        .collect();

    // Load relations with resolved tags
    println!();
    println!("Loading relations...");
    let relations = crate::formats::RelationsFile::read(&config.relations_path)?;
    println!("  loaded {} relations", relations.len());

    // Process turn restrictions through all models
    let mut turn_rules_per_mode: Vec<Vec<TurnRule>> = vec![Vec::new(); n_modes];

    for relation in relations.iter() {
        let mut keys = Vec::new();
        let mut vals = Vec::new();
        for (k, v) in &relation.tags {
            if let (Some(&k_id), Some(&v_id)) = (
                rel_key_reverse.get(k.as_str()),
                rel_val_reverse.get(v.as_str()),
            ) {
                keys.push(k_id);
                vals.push(v_id);
            }
        }

        let (via_node_id, from_way_id, to_way_id) = extract_turn_triple(&relation.members);
        if via_node_id == 0 || from_way_id == 0 || to_way_id == 0 {
            continue;
        }

        for (i, compiled) in compiled_turn_models.iter().enumerate() {
            let (kind, applies, penalty_s, is_time_dep) =
                evaluate_turn_full(compiled, &keys, &vals, &rel_key_dict, &rel_val_dict);

            if applies && kind != TurnRuleKind::None {
                turn_rules_per_mode[i].push(TurnRule {
                    via_node_id,
                    from_way_id,
                    to_way_id,
                    kind,
                    penalty_s,
                    is_time_dep: if is_time_dep { 1 } else { 0 },
                });
            }
        }
    }

    for (i, mode_info) in modes.iter().enumerate() {
        println!(
            "  turn restrictions for {}: {} rules",
            mode_info.name,
            turn_rules_per_mode[i].len()
        );
        turn_rules_per_mode[i].sort_unstable();
    }

    // Write turn_rules files
    println!();
    println!("Writing turn_rules files...");
    for (i, mode_info) in modes.iter().enumerate() {
        let filename = format!("turn_rules.{}.bin", mode_info.name);
        let path = config.outdir.join(&filename);
        let mode = Mode(mode_info.index);

        turn_rules::write(
            &path,
            mode,
            &turn_rules_per_mode[i],
            &rel_dict_k_sha256,
            &rel_dict_v_sha256,
        )?;
        println!(
            "  wrote {} ({} rules)",
            filename,
            turn_rules_per_mode[i].len()
        );

        mode_outputs[i].turn_rules_path = path;
    }

    // Generate profile_meta.json
    println!();
    println!("Generating profile_meta.json...");
    let profile_meta_path = config.outdir.join("profile_meta.json");

    let ways_sha256 = compute_file_sha256(&config.ways_path)?;
    let relations_sha256 = compute_file_sha256(&config.relations_path)?;

    let mut way_attrs_sha256 = BTreeMap::new();
    let mut turn_rules_sha256 = BTreeMap::new();
    let mut profile_versions = BTreeMap::new();

    for out in &mode_outputs {
        way_attrs_sha256.insert(
            out.mode_name.clone(),
            compute_file_sha256(&out.way_attrs_path)?,
        );
        turn_rules_sha256.insert(
            out.mode_name.clone(),
            compute_file_sha256(&out.turn_rules_path)?,
        );
        let model_path = super::model_file_path(&config.models_dir, &out.mode_name);
        let schema = super::load_model_schema(&model_path)?;
        profile_versions.insert(out.mode_name.clone(), schema.version);
    }

    let meta = ProfileMeta {
        abi_version: 2,
        profile_versions,
        highway_classes: build_highway_classes().into_iter().collect(),
        surface_classes: build_surface_classes().into_iter().collect(),
        class_bits: build_class_bits().into_iter().collect(),
        ways_sha256,
        relations_sha256,
        way_attrs_sha256,
        turn_rules_sha256,
    };

    let meta_json = serde_json::to_string_pretty(&meta)?;
    std::fs::write(&profile_meta_path, meta_json)?;
    println!("  wrote profile_meta.json");

    // Write build_manifest.json
    let manifest = super::BuildManifest {
        build_timestamp: chrono::Utc::now().to_rfc3339(),
        pipeline_version: "3.0.0".to_string(),
        modes: modes
            .iter()
            .map(|m| {
                let model_path = super::model_file_path(&config.models_dir, &m.name);
                let sha256 = super::compute_model_sha256(&model_path).unwrap_or([0u8; 32]);
                super::ManifestMode {
                    name: m.name.clone(),
                    index: m.index,
                    model_version: 1,
                    model_sha256: hex::encode(sha256),
                }
            })
            .collect(),
    };
    let manifest_path = config.outdir.join("build_manifest.json");
    manifest.write_to(&manifest_path)?;
    println!("  wrote build_manifest.json");

    // Generate step2.lock.json
    let step1_lock_path = config
        .ways_path
        .parent()
        .ok_or_else(|| anyhow::anyhow!("ways_path has no parent directory"))?
        .join("step1.lock.json");

    let mut way_attrs_files_mode: HashMap<String, PathBuf> = HashMap::new();
    let mut turn_rules_files_mode: HashMap<String, PathBuf> = HashMap::new();
    for out in &mode_outputs {
        way_attrs_files_mode.insert(out.mode_name.clone(), out.way_attrs_path.clone());
        turn_rules_files_mode.insert(out.mode_name.clone(), out.turn_rules_path.clone());
    }

    let step2_lock = crate::validate::Step2LockFile::create(
        &step1_lock_path,
        &config.ways_path,
        &config.relations_path,
        &way_attrs_files_mode,
        &turn_rules_files_mode,
        &profile_meta_path,
    )?;

    let step2_lock_path = config.outdir.join("step2.lock.json");
    step2_lock.write(&step2_lock_path)?;

    crate::validate::verify_step2_lock_conditions(
        &step2_lock_path,
        &config.ways_path,
        &config.relations_path,
        &way_attrs_files_mode,
        &turn_rules_files_mode,
        &profile_meta_path,
    )?;

    println!();
    println!("Profiling complete!");
    println!("  Lock file: {}", step2_lock_path.display());

    Ok(ProfileResult {
        modes: mode_outputs,
        profile_meta_path,
    })
}

fn extract_turn_triple(members: &[crate::formats::Member]) -> (i64, i64, i64) {
    use crate::formats::MemberKind;

    let mut via_node = 0i64;
    let mut from_way = 0i64;
    let mut to_way = 0i64;

    for member in members {
        match member.role.as_str() {
            "via" if matches!(member.kind, MemberKind::Node) => via_node = member.ref_id,
            "from" if matches!(member.kind, MemberKind::Way) => from_way = member.ref_id,
            "to" if matches!(member.kind, MemberKind::Way) => to_way = member.ref_id,
            _ => {}
        }
    }

    (via_node, from_way, to_way)
}

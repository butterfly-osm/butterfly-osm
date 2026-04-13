//! Step 2: Modal profiling pipeline (declarative model evaluation)
//!
//! Auto-discovers `*.model.json` files, compiles them against tag dictionaries,
//! and evaluates every way/relation through each model. No hardcoded profiles.

use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::{Path, PathBuf};

use super::{CompiledModel, compile_model, evaluate_turn_full, evaluate_way};
use crate::formats::{TurnRule, WayAttr, turn_rules, way_attrs};
use crate::profile_abi::{Mode, TurnRuleKind};

pub struct ProfileConfig {
    pub ways_path: PathBuf,
    pub relations_path: PathBuf,
    pub models_dir: PathBuf,
    pub outdir: PathBuf,
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
    pub profile_versions: HashMap<String, u32>,
    pub highway_classes: HashMap<u16, String>,
    pub surface_classes: HashMap<u16, String>,
    pub class_bits: HashMap<String, u32>,
    pub ways_sha256: String,
    pub relations_sha256: String,
    pub way_attrs_sha256: HashMap<String, String>,
    pub turn_rules_sha256: HashMap<String, String>,
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

    let way_stream = crate::formats::WaysFile::stream_ways(&config.ways_path)?;
    let mut count = 0u64;

    for result in way_stream {
        let (way_id, keys, vals, _nodes) = result?;

        for (i, compiled) in compiled_models.iter().enumerate() {
            let output = evaluate_way(compiled, &keys, &vals, &val_dict);
            way_attrs_per_mode[i].push(WayAttr { way_id, output });
        }

        count += 1;
        if count.is_multiple_of(1_000_000) {
            println!("  processed {} ways...", count);
        }
    }

    println!("  processed {} ways across {} modes", count, n_modes);

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
            let (kind, applies, penalty_ds, is_time_dep) =
                evaluate_turn_full(compiled, &keys, &vals, &rel_key_dict, &rel_val_dict);

            if applies && kind != TurnRuleKind::None {
                turn_rules_per_mode[i].push(TurnRule {
                    via_node_id,
                    from_way_id,
                    to_way_id,
                    kind,
                    penalty_ds,
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

    let mut way_attrs_sha256 = HashMap::new();
    let mut turn_rules_sha256 = HashMap::new();
    let mut profile_versions = HashMap::new();

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
        highway_classes: build_highway_classes(),
        surface_classes: build_surface_classes(),
        class_bits: build_class_bits(),
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

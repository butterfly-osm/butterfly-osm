///! Step 2: Modal profiling pipeline
///!
///! Processes ways.raw and relations.raw through routing profiles to generate
///! per-mode attributes and turn restrictions.

use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::PathBuf;

use crate::formats::{way_attrs, turn_rules, WayAttr, TurnRule};
use crate::profile_abi::{Mode, Profile, WayInput, TurnInput, TurnRuleKind};
use crate::profiles::{CarProfile, BikeProfile, FootProfile};

pub struct ProfileConfig {
    pub ways_path: PathBuf,
    pub relations_path: PathBuf,
    pub outdir: PathBuf,
}

pub struct ProfileResult {
    pub way_attrs_files: HashMap<Mode, PathBuf>,
    pub turn_rules_files: HashMap<Mode, PathBuf>,
    pub profile_meta_path: PathBuf,
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
    bits.insert("oneway_shift".to_string(), 2); // bits 2-3 encode oneway
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

fn compute_file_sha256<P: AsRef<std::path::Path>>(path: P) -> Result<String> {
    use sha2::{Sha256, Digest};
    use std::fs::File;
    use std::io::Read;

    let mut file = File::open(path)?;
    let mut hasher = Sha256::new();
    let mut buffer = [0u8; 8192];

    loop {
        let n = file.read(&mut buffer)?;
        if n == 0 { break; }
        hasher.update(&buffer[..n]);
    }

    Ok(format!("{:x}", hasher.finalize()))
}

/// Run Step 2 profiling pipeline
pub fn run_profiling(config: ProfileConfig) -> Result<ProfileResult> {
    println!("🦋 Starting Step 2: Modal Profiling");
    println!("📂 Ways: {}", config.ways_path.display());
    println!("📂 Relations: {}", config.relations_path.display());
    println!("📂 Output: {}", config.outdir.display());
    println!();

    std::fs::create_dir_all(&config.outdir)
        .context("Failed to create output directory")?;

    // Load dictionaries from ways.raw
    println!("Loading dictionaries from ways.raw...");
    let (key_dict, val_dict, dict_k_sha256, dict_v_sha256) =
        crate::formats::WaysFile::read_dictionaries(&config.ways_path)?;
    println!("  ✓ Key dictionary: {} entries", key_dict.len());
    println!("  ✓ Value dictionary: {} entries", val_dict.len());

    // Stream and process ways (no RAM loading!)
    println!("Streaming and processing ways...");
    let mut way_attrs_car = Vec::new();
    let mut way_attrs_bike = Vec::new();
    let mut way_attrs_foot = Vec::new();

    let way_stream = crate::formats::WaysFile::stream_ways(&config.ways_path)?;
    let mut count = 0u64;

    for result in way_stream {
        let (way_id, keys, vals, _nodes) = result?;

        let input = WayInput {
            kv_keys: &keys,
            kv_vals: &vals,
        };

        // Process through each profile
        way_attrs_car.push(WayAttr {
            way_id,
            output: CarProfile::process_way(input),
        });

        way_attrs_bike.push(WayAttr {
            way_id,
            output: BikeProfile::process_way(input),
        });

        way_attrs_foot.push(WayAttr {
            way_id,
            output: FootProfile::process_way(input),
        });

        count += 1;
        if count % 1_000_000 == 0 {
            println!("  Processed {} ways...", count);
        }
    }

    println!("  ✓ Processed {} ways", count);

    // Write way_attrs files
    println!();
    println!("Writing way_attrs files...");
    let mut way_attrs_files = HashMap::new();

    for mode in Mode::all() {
        let attrs = match mode {
            Mode::Car => &way_attrs_car,
            Mode::Bike => &way_attrs_bike,
            Mode::Foot => &way_attrs_foot,
        };

        let filename = format!("way_attrs.{}.bin", mode.name());
        let path = config.outdir.join(&filename);

        way_attrs::write(&path, *mode, attrs, &dict_k_sha256, &dict_v_sha256)?;
        println!("  ✓ Wrote {} ({} ways)", filename, attrs.len());

        way_attrs_files.insert(*mode, path);
    }

    // Load dictionaries from relations.raw
    println!();
    println!("Loading dictionaries from relations.raw...");
    let (rel_key_dict, rel_val_dict, rel_dict_k_sha256, rel_dict_v_sha256) =
        crate::formats::RelationsFile::read_dictionaries(&config.relations_path)?;
    println!("  ✓ Key dictionary: {} entries", rel_key_dict.len());
    println!("  ✓ Value dictionary: {} entries", rel_val_dict.len());

    // Build reverse indexes for relations
    let rel_key_reverse: HashMap<&str, u32> = rel_key_dict.iter().map(|(id, s)| (s.as_str(), *id)).collect();
    let rel_val_reverse: HashMap<&str, u32> = rel_val_dict.iter().map(|(id, s)| (s.as_str(), *id)).collect();

    // Load relations with resolved tags
    println!("Loading relations...");
    let relations = crate::formats::RelationsFile::read(&config.relations_path)?;
    println!("  ✓ Loaded {} relations", relations.len());

    // Process turn restrictions
    let mut turn_rules_car = Vec::new();
    let mut turn_rules_bike = Vec::new();
    let mut turn_rules_foot = Vec::new();

    for relation in relations.iter() {
        // Build tag ID arrays using O(1) reverse index lookups
        let mut keys = Vec::new();
        let mut vals = Vec::new();
        for (k, v) in &relation.tags {
            if let (Some(&k_id), Some(&v_id)) = (rel_key_reverse.get(k.as_str()), rel_val_reverse.get(v.as_str())) {
                keys.push(k_id);
                vals.push(v_id);
            }
        }

        let input = TurnInput {
            tags_keys: &keys,
            tags_vals: &vals,
        };

        // Extract via_node, from_way, to_way from members
        let (via_node_id, from_way_id, to_way_id) = extract_turn_triple(&relation.members);
        if via_node_id == 0 || from_way_id == 0 || to_way_id == 0 {
            continue; // Invalid or via=way (needs expansion)
        }

        // Process through each profile
        let car_output = CarProfile::process_turn(input);
        if car_output.kind != TurnRuleKind::None {
            turn_rules_car.push(TurnRule {
                via_node_id,
                from_way_id,
                to_way_id,
                kind: car_output.kind,
                penalty_ds: car_output.penalty_ds,
                is_time_dep: if car_output.is_time_dependent { 1 } else { 0 },
            });
        }

        let bike_output = BikeProfile::process_turn(input);
        if bike_output.kind != TurnRuleKind::None {
            turn_rules_bike.push(TurnRule {
                via_node_id,
                from_way_id,
                to_way_id,
                kind: bike_output.kind,
                penalty_ds: bike_output.penalty_ds,
                is_time_dep: if bike_output.is_time_dependent { 1 } else { 0 },
            });
        }

        let foot_output = FootProfile::process_turn(input);
        if foot_output.kind != TurnRuleKind::None {
            turn_rules_foot.push(TurnRule {
                via_node_id,
                from_way_id,
                to_way_id,
                kind: foot_output.kind,
                penalty_ds: foot_output.penalty_ds,
                is_time_dep: if foot_output.is_time_dependent { 1 } else { 0 },
            });
        }
    }

    println!("  ✓ Extracted turn restrictions: car={}, bike={}, foot={}",
        turn_rules_car.len(), turn_rules_bike.len(), turn_rules_foot.len());

    // Write turn_rules files
    println!();
    println!("Writing turn_rules files...");
    let mut turn_rules_files = HashMap::new();

    for mode in Mode::all() {
        let rules = match mode {
            Mode::Car => &turn_rules_car,
            Mode::Bike => &turn_rules_bike,
            Mode::Foot => &turn_rules_foot,
        };

        let filename = format!("turn_rules.{}.bin", mode.name());
        let path = config.outdir.join(&filename);

        turn_rules::write(&path, *mode, rules, &rel_dict_k_sha256, &rel_dict_v_sha256)?;
        println!("  ✓ Wrote {} ({} rules)", filename, rules.len());

        turn_rules_files.insert(*mode, path);
    }

    // Generate profile_meta.json
    println!();
    println!("Generating profile_meta.json...");
    let profile_meta_path = config.outdir.join("profile_meta.json");

    // Compute SHA-256 for all artifacts
    let ways_sha256 = compute_file_sha256(&config.ways_path)?;
    let relations_sha256 = compute_file_sha256(&config.relations_path)?;

    let mut way_attrs_sha256 = HashMap::new();
    let mut turn_rules_sha256 = HashMap::new();

    for mode in Mode::all() {
        let way_attrs_path = &way_attrs_files[mode];
        let turn_rules_path = &turn_rules_files[mode];

        way_attrs_sha256.insert(
            mode.name().to_string(),
            compute_file_sha256(way_attrs_path)?
        );
        turn_rules_sha256.insert(
            mode.name().to_string(),
            compute_file_sha256(turn_rules_path)?
        );
    }

    // Build profile versions
    let mut profile_versions = HashMap::new();
    profile_versions.insert("car".to_string(), CarProfile::version());
    profile_versions.insert("bike".to_string(), BikeProfile::version());
    profile_versions.insert("foot".to_string(), FootProfile::version());

    let meta = ProfileMeta {
        abi_version: 1,
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
    println!("  ✓ Wrote profile_meta.json");

    // Generate step2.lock.json
    let step1_lock_path = config.ways_path.parent()
        .ok_or_else(|| anyhow::anyhow!("ways_path has no parent directory"))?
        .join("step1.lock.json");

    let step2_lock = crate::validate::Step2LockFile::create(
        &step1_lock_path,
        &config.ways_path,
        &config.relations_path,
        &way_attrs_files,
        &turn_rules_files,
        &profile_meta_path,
    )?;

    let step2_lock_path = config.outdir.join("step2.lock.json");
    step2_lock.write(&step2_lock_path)?;

    // Verify all lock conditions (A-E)
    crate::validate::verify_step2_lock_conditions(
        &step2_lock_path,
        &config.ways_path,
        &config.relations_path,
        &way_attrs_files,
        &turn_rules_files,
        &profile_meta_path,
    )?;

    println!();
    println!("✅ Profiling complete!");
    println!("📋 Lock file: {}", step2_lock_path.display());

    Ok(ProfileResult {
        way_attrs_files,
        turn_rules_files,
        profile_meta_path,
    })
}

/// Find ID by string in dictionary (reverse lookup)
fn find_id_by_string(dict: &HashMap<u32, String>, s: &str) -> Option<u32> {
    dict.iter().find(|(_, v)| v.as_str() == s).map(|(k, _)| *k)
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

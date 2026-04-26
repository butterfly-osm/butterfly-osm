//! Pack / inspect for the unified `butterfly.dat` container.
//!
//! Reads a `data_dir/step{1..8}/` tree and emits a single file with
//! every artefact + per-section CRCs + a directory at the tail. See
//! `formats::butterfly_dat` for the exact byte layout.
//!
//! Decisions:
//! * Section names use a stable string scheme so the loader does not
//!   have to know the file system layout. `step1/nodes.sa`,
//!   `step5/filtered.<mode>.ebg`, `step8/cch.w.<mode>.u32`, etc.
//! * The pack walks the source tree by *globbing* per-step
//!   directories, so newly-added files (e.g. traffic-customised
//!   weight files from #84) are picked up automatically as long as
//!   they follow the `cch.w.*.u32` / `cch.d.*.u32` filename pattern
//!   in `step8/`.
//! * Optional inputs (e.g. `node_signals.bin`, mode-mask bitsets) are
//!   skipped silently if absent.

use anyhow::{Context, Result};
use std::path::{Path, PathBuf};

use crate::formats::butterfly_dat::{Container, ContainerWriter, SectionKind};

/// Section name for the JSON manifest that lists modes + bundle ids.
/// Lives at the top of the `shared/` namespace so legacy tooling can
/// ignore it (it has the synthetic `Unknown` kind on disk).
const MANIFEST_NAME: &str = "shared/manifest.json";

/// Resolve a step subdirectory the same way the server does:
/// exact match first, then any directory whose name starts with
/// `step{N}` (alphabetically lowest).
fn find_step_dir(data_dir: &Path, step: &str) -> Result<PathBuf> {
    let exact = data_dir.join(step);
    if exact.exists() {
        return Ok(exact);
    }
    let mut matches: Vec<PathBuf> = Vec::new();
    for entry in
        std::fs::read_dir(data_dir).with_context(|| format!("reading {}", data_dir.display()))?
    {
        let entry = entry?;
        let name = entry.file_name();
        let s = name.to_string_lossy();
        if s.starts_with(step) && entry.file_type()?.is_dir() {
            matches.push(entry.path());
        }
    }
    matches.sort();
    matches
        .into_iter()
        .next()
        .ok_or_else(|| anyhow::anyhow!("could not find {} dir under {}", step, data_dir.display()))
}

/// Append a section if the file exists; silently skip otherwise.
/// Logs the size on append so the operator sees what was packed.
fn maybe_append(
    w: &mut ContainerWriter,
    kind: SectionKind,
    name: &str,
    path: &Path,
) -> Result<bool> {
    if !path.exists() {
        return Ok(false);
    }
    let size = std::fs::metadata(path)?.len();
    println!(
        "  + [{:>5} MiB] {:<28} <- {}",
        size / (1024 * 1024),
        name,
        path.display()
    );
    w.append_file(kind, name, path)
        .with_context(|| format!("packing {} from {}", name, path.display()))?;
    Ok(true)
}

/// Glob a directory for files matching `prefix.*.suffix`. Returns the
/// embedded mode token together with the absolute path. Sorted by
/// mode name for determinism.
fn glob_per_mode(dir: &Path, prefix: &str, suffix: &str) -> Result<Vec<(String, PathBuf)>> {
    if !dir.exists() {
        return Ok(Vec::new());
    }
    let mut out = Vec::new();
    let prefix = format!("{}.", prefix);
    for entry in std::fs::read_dir(dir).with_context(|| format!("reading {}", dir.display()))? {
        let entry = entry?;
        let n = entry.file_name();
        let s = n.to_string_lossy();
        if !s.starts_with(&prefix) || !s.ends_with(suffix) {
            continue;
        }
        let mode = &s[prefix.len()..s.len() - suffix.len()];
        if mode.is_empty() {
            continue;
        }
        out.push((mode.to_string(), entry.path()));
    }
    out.sort_by(|a, b| a.0.cmp(&b.0));
    Ok(out)
}

/// Implementation of the `pack` subcommand.
pub fn pack(data_dir: &Path, out: &Path, step_prefix: Option<&str>) -> Result<()> {
    println!("packing {} → {}", data_dir.display(), out.display());

    let step1 = find_step_dir(data_dir, step_prefix.unwrap_or("step1"))?;
    let step2 = find_step_dir(data_dir, step_prefix.unwrap_or("step2"))?;
    let step3 = find_step_dir(data_dir, step_prefix.unwrap_or("step3"))?;
    let step4 = find_step_dir(data_dir, step_prefix.unwrap_or("step4"))?;
    let step5 = find_step_dir(data_dir, step_prefix.unwrap_or("step5"))?;
    let step6 = find_step_dir(data_dir, step_prefix.unwrap_or("step6"))?;
    let step7 = find_step_dir(data_dir, step_prefix.unwrap_or("step7"))?;
    let step8 = find_step_dir(data_dir, step_prefix.unwrap_or("step8"))?;

    if let Some(parent) = out.parent()
        && !parent.as_os_str().is_empty()
    {
        std::fs::create_dir_all(parent)?;
    }
    let mut w = ContainerWriter::create(out)?;

    // ---- Shared global tables (mode-agnostic) -----------------------
    // Step 1 ingest output.
    maybe_append(
        &mut w,
        SectionKind::NodesSa,
        "shared/step1.nodes.sa",
        &step1.join("nodes.sa"),
    )?;
    maybe_append(
        &mut w,
        SectionKind::NodesSi,
        "shared/step1.nodes.si",
        &step1.join("nodes.si"),
    )?;
    maybe_append(
        &mut w,
        SectionKind::WaysRaw,
        "shared/step1.ways.raw",
        &step1.join("ways.raw"),
    )?;
    maybe_append(
        &mut w,
        SectionKind::RelationsRaw,
        "shared/step1.relations.raw",
        &step1.join("relations.raw"),
    )?;
    maybe_append(
        &mut w,
        SectionKind::NodeSignals,
        "shared/step1.node_signals.bin",
        &step1.join("node_signals.bin"),
    )?;
    // NBG (build-time intermediate, but the geo + node_map are read at
    // server startup — keep them in `shared/`).
    maybe_append(
        &mut w,
        SectionKind::NbgCsr,
        "shared/nbg.csr",
        &step3.join("nbg.csr"),
    )?;
    maybe_append(
        &mut w,
        SectionKind::NbgGeo,
        "shared/nbg.geo",
        &step3.join("nbg.geo"),
    )?;
    maybe_append(
        &mut w,
        SectionKind::NbgNodeMap,
        "shared/nbg.node_map",
        &step3.join("nbg.node_map"),
    )?;
    // EBG (mode-agnostic).
    maybe_append(
        &mut w,
        SectionKind::EbgNodes,
        "shared/ebg.nodes",
        &step4.join("ebg.nodes"),
    )?;
    maybe_append(
        &mut w,
        SectionKind::EbgCsr,
        "shared/ebg.csr",
        &step4.join("ebg.csr"),
    )?;
    maybe_append(
        &mut w,
        SectionKind::EbgTurnTable,
        "shared/ebg.turn_table",
        &step4.join("ebg.turn_table"),
    )?;

    // ---- Per-mode bundles -------------------------------------------
    // Modes are discovered from `step5/w.<mode>.u32` to match the
    // server's `discover_modes()` rule. We keep step2 way_attrs +
    // turn_rules under the per-mode bundle: they are consumed mode-
    // by-mode at server startup (e.g. exclude flags for `car`).
    let mut modes: Vec<String> = glob_per_mode(&step5, "w", ".u32")?
        .into_iter()
        .map(|(m, _)| m)
        .collect();
    modes.sort();
    modes.dedup();

    for mode in &modes {
        // step2 attrs/rules live with the mode they belong to.
        let way_attrs = step2.join(format!("way_attrs.{}.bin", mode));
        maybe_append(
            &mut w,
            SectionKind::WayAttrs,
            &format!("mode/{}/way_attrs", mode),
            &way_attrs,
        )?;
        let turn_rules = step2.join(format!("turn_rules.{}.bin", mode));
        maybe_append(
            &mut w,
            SectionKind::TurnRules,
            &format!("mode/{}/turn_rules", mode),
            &turn_rules,
        )?;
        // step5: filtered EBG, weights, mask.
        let filtered = step5.join(format!("filtered.{}.ebg", mode));
        maybe_append(
            &mut w,
            SectionKind::FilteredEbg,
            &format!("mode/{}/filtered_ebg", mode),
            &filtered,
        )?;
        let weights_time = step5.join(format!("w.{}.u32", mode));
        maybe_append(
            &mut w,
            SectionKind::NodeWeightsTime,
            &format!("mode/{}/node_weights.time", mode),
            &weights_time,
        )?;
        let weights_turn = step5.join(format!("t.{}.u32", mode));
        maybe_append(
            &mut w,
            SectionKind::NodeWeightsTurn,
            &format!("mode/{}/node_weights.turn", mode),
            &weights_turn,
        )?;
        let mask = step5.join(format!("mask.{}.bitset", mode));
        maybe_append(
            &mut w,
            SectionKind::ModeMask,
            &format!("mode/{}/mask", mode),
            &mask,
        )?;
        // step6 ordering. Lifted variants are intentionally skipped.
        let order = step6.join(format!("order.{}.ebg", mode));
        maybe_append(
            &mut w,
            SectionKind::OrderEbg,
            &format!("mode/{}/order", mode),
            &order,
        )?;
        // step7 topology.
        let topo = step7.join(format!("cch.{}.topo", mode));
        maybe_append(
            &mut w,
            SectionKind::CchTopo,
            &format!("mode/{}/topo", mode),
            &topo,
        )?;
        // step8 customised weights.
        let cch_w = step8.join(format!("cch.w.{}.u32", mode));
        maybe_append(
            &mut w,
            SectionKind::CchWeightsTime,
            &format!("mode/{}/weights.time", mode),
            &cch_w,
        )?;
        let cch_d = step8.join(format!("cch.d.{}.u32", mode));
        maybe_append(
            &mut w,
            SectionKind::CchWeightsDist,
            &format!("mode/{}/weights.dist", mode),
            &cch_d,
        )?;
    }

    // ---- Manifest ---------------------------------------------------
    // Lists the modes packed and their bundle ids. For now, every mode
    // is a singleton bundle (bundle_id == mode_name); the topology-
    // groups follow-up (#146) will let multiple modes share one bundle.
    // The manifest is a JSON object so future fields land cleanly.
    let manifest = build_manifest(&modes);
    w.append_bytes(SectionKind::Unknown, MANIFEST_NAME, manifest.as_bytes())?;

    let n_sec = w.len();
    w.finalize()?;

    let final_size = std::fs::metadata(out)?.len();
    println!(
        "wrote {} sections, {:.2} GiB → {}",
        n_sec,
        final_size as f64 / (1024.0 * 1024.0 * 1024.0),
        out.display()
    );
    Ok(())
}

/// Build the JSON manifest payload listing the packed modes. The JSON
/// shape is deliberately small + extensible: arrays of strings, every
/// mode mapped to a bundle id equal to its name (one bundle per mode is
/// the only shape this ticket ships; #146 generalises to N-mode-per-
/// bundle). Unknown JSON fields round-trip through `unpack` because the
/// section is byte-copied.
fn build_manifest(modes: &[String]) -> String {
    use std::fmt::Write;
    let mut s = String::from("{\n  \"version\": 1,\n  \"modes\": [");
    for (i, m) in modes.iter().enumerate() {
        if i > 0 {
            s.push_str(", ");
        }
        let _ = write!(s, "\"{}\"", m.replace('"', "\\\""));
    }
    s.push_str("],\n  \"bundles\": {");
    for (i, m) in modes.iter().enumerate() {
        if i > 0 {
            s.push_str(", ");
        }
        let esc = m.replace('"', "\\\"");
        let _ = write!(s, "\"{0}\": [\"{0}\"]", esc);
    }
    s.push_str("}\n}\n");
    s
}

/// Map a `SectionEntry` back to the on-disk path inside a `step{N}/`
/// tree, mirroring what `pack` consumed. Handles both the new
/// `shared/`+`mode/<m>/...` schema and the legacy `stepN/...` schema
/// from earlier container builds, so old containers still round-trip.
fn path_for_section(out_dir: &Path, name: &str) -> Option<PathBuf> {
    // ---- New schema -------------------------------------------------
    if name == MANIFEST_NAME {
        return Some(out_dir.join("manifest.json"));
    }
    if let Some(rest) = name.strip_prefix("shared/") {
        // shared/step1.<file> → step1/<file>
        if let Some(file) = rest.strip_prefix("step1.") {
            return Some(out_dir.join("step1").join(file));
        }
        // shared/nbg.<x> → step3/nbg.<x>
        if let Some(_n) = rest.strip_prefix("nbg.") {
            return Some(out_dir.join("step3").join(rest));
        }
        // shared/ebg.<x> → step4/ebg.<x>
        if let Some(_n) = rest.strip_prefix("ebg.") {
            return Some(out_dir.join("step4").join(rest));
        }
        return None;
    }
    if let Some(rest) = name.strip_prefix("mode/") {
        let slash = rest.find('/')?;
        let mode = &rest[..slash];
        let leaf = &rest[slash + 1..];
        return match leaf {
            "way_attrs" => Some(
                out_dir
                    .join("step2")
                    .join(format!("way_attrs.{}.bin", mode)),
            ),
            "turn_rules" => Some(
                out_dir
                    .join("step2")
                    .join(format!("turn_rules.{}.bin", mode)),
            ),
            "filtered_ebg" => Some(out_dir.join("step5").join(format!("filtered.{}.ebg", mode))),
            "node_weights.time" => Some(out_dir.join("step5").join(format!("w.{}.u32", mode))),
            "node_weights.turn" => Some(out_dir.join("step5").join(format!("t.{}.u32", mode))),
            "mask" => Some(out_dir.join("step5").join(format!("mask.{}.bitset", mode))),
            "order" => Some(out_dir.join("step6").join(format!("order.{}.ebg", mode))),
            "topo" => Some(out_dir.join("step7").join(format!("cch.{}.topo", mode))),
            "weights.time" => Some(out_dir.join("step8").join(format!("cch.w.{}.u32", mode))),
            "weights.dist" => Some(out_dir.join("step8").join(format!("cch.d.{}.u32", mode))),
            _ => None,
        };
    }

    // ---- Legacy `stepN/...` schema (older containers) ---------------
    legacy_path_for_section(out_dir, name)
}

fn legacy_path_for_section(out_dir: &Path, name: &str) -> Option<PathBuf> {
    // The mapping mirrors `pack` exactly. Any section we do not
    // recognise is left out; `unpack` reports it as a warning.
    if let Some(rest) = name.strip_prefix("step1/") {
        return Some(out_dir.join("step1").join(rest));
    }
    if let Some(rest) = name.strip_prefix("step2/") {
        // step2 sections are `way_attrs.<mode>` / `turn_rules.<mode>`;
        // re-add the `.bin` suffix the input directory used.
        if let Some(mode) = rest.strip_prefix("way_attrs.") {
            return Some(
                out_dir
                    .join("step2")
                    .join(format!("way_attrs.{}.bin", mode)),
            );
        }
        if let Some(mode) = rest.strip_prefix("turn_rules.") {
            return Some(
                out_dir
                    .join("step2")
                    .join(format!("turn_rules.{}.bin", mode)),
            );
        }
        return None;
    }
    if let Some(rest) = name.strip_prefix("step3/") {
        return Some(out_dir.join("step3").join(rest));
    }
    if let Some(rest) = name.strip_prefix("step4/") {
        return Some(out_dir.join("step4").join(rest));
    }
    if let Some(rest) = name.strip_prefix("step5/") {
        // Restore the trailing extension: filtered.<mode> -> filtered.<mode>.ebg, etc.
        if let Some(mode) = rest.strip_prefix("filtered.") {
            return Some(out_dir.join("step5").join(format!("filtered.{}.ebg", mode)));
        }
        if let Some(mode) = rest.strip_prefix("w.") {
            return Some(out_dir.join("step5").join(format!("w.{}.u32", mode)));
        }
        if let Some(mode) = rest.strip_prefix("t.") {
            return Some(out_dir.join("step5").join(format!("t.{}.u32", mode)));
        }
        if let Some(mode) = rest.strip_prefix("mask.") {
            return Some(out_dir.join("step5").join(format!("mask.{}.bitset", mode)));
        }
        return None;
    }
    if let Some(rest) = name.strip_prefix("step6/") {
        if let Some(mode) = rest.strip_prefix("order.") {
            return Some(out_dir.join("step6").join(format!("order.{}.ebg", mode)));
        }
        return None;
    }
    if let Some(rest) = name.strip_prefix("step7/") {
        if let Some(mode) = rest.strip_prefix("cch.") {
            return Some(out_dir.join("step7").join(format!("cch.{}.topo", mode)));
        }
        return None;
    }
    if let Some(rest) = name.strip_prefix("step8/") {
        if let Some(mode) = rest.strip_prefix("cch.w.") {
            return Some(out_dir.join("step8").join(format!("cch.w.{}.u32", mode)));
        }
        if let Some(mode) = rest.strip_prefix("cch.d.") {
            return Some(out_dir.join("step8").join(format!("cch.d.{}.u32", mode)));
        }
        return None;
    }
    None
}

/// Implementation of the `unpack` subcommand. Inverse of `pack`: writes
/// every section back to the canonical `step{N}/file` path under
/// `out_dir`. Validates each section's CRC during the copy.
///
/// `out_dir` must not exist (so the inverse mapping is unambiguous).
pub fn unpack(path: &Path, out_dir: &Path) -> Result<()> {
    if out_dir.exists() {
        anyhow::bail!(
            "output directory {} already exists; refusing to overwrite",
            out_dir.display()
        );
    }
    std::fs::create_dir_all(out_dir)?;

    let c = Container::open(path)?;
    println!(
        "unpacking {} ({} sections) → {}",
        path.display(),
        c.n_sections,
        out_dir.display()
    );

    for sec in &c.sections {
        let out_path = path_for_section(out_dir, &sec.name).ok_or_else(|| {
            anyhow::anyhow!(
                "section '{}' does not match the standard step{{N}}/... layout; \
                 cannot map back to a file path",
                sec.name
            )
        })?;
        if let Some(parent) = out_path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let bytes = c.read_section_verified(path, sec)?;
        std::fs::write(&out_path, &bytes)?;
        println!(
            "  -> [{:>5} MiB] {:<32} -> {}",
            bytes.len() / (1024 * 1024),
            sec.name,
            out_path.display()
        );
    }
    println!("OK");
    Ok(())
}

/// Implementation of the `inspect` subcommand.
pub fn inspect(path: &Path, verify: bool, verify_full: bool) -> Result<()> {
    let c = Container::open(path)?;
    println!(
        "{} (version {}, {} sections, dir@{}+{}b)",
        path.display(),
        c.version,
        c.n_sections,
        c.dir_offset,
        c.dir_len,
    );
    println!(
        "{:<6} {:<28} {:<32} {:>14} {:>14} {:>16}",
        "idx", "kind", "name", "offset", "length", "crc"
    );
    for (i, sec) in c.sections.iter().enumerate() {
        println!(
            "{:<6} {:<28} {:<32} {:>14} {:>14} 0x{:016X}",
            i,
            sec.kind.label(),
            sec.name,
            sec.offset,
            sec.len,
            sec.crc,
        );
    }

    if verify {
        println!();
        println!("verifying {} per-section CRCs ...", c.n_sections);
        for sec in &c.sections {
            let _ = c.read_section_verified(path, sec)?;
        }
        println!("OK");
    }
    if verify_full {
        println!();
        println!("verifying full-file CRC ...");
        c.verify_file_crc(path)?;
        println!("OK");
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use tempfile::TempDir;

    fn write_file(p: &Path, body: &[u8]) -> Result<()> {
        if let Some(parent) = p.parent() {
            fs::create_dir_all(parent)?;
        }
        fs::write(p, body)?;
        Ok(())
    }

    /// Build a synthetic data dir with a couple of files in step1/
    /// and step5/ + step8/ so we can prove the per-mode globbing works.
    fn synth_dir() -> Result<TempDir> {
        let tmp = TempDir::new()?;
        let root = tmp.path();

        write_file(&root.join("step1").join("nodes.sa"), b"sa-bytes")?;
        write_file(&root.join("step1").join("nodes.si"), b"si-bytes")?;
        write_file(&root.join("step1").join("ways.raw"), b"ways-raw")?;
        write_file(&root.join("step1").join("relations.raw"), b"rel-raw")?;
        write_file(&root.join("step3").join("nbg.csr"), b"csr")?;
        write_file(&root.join("step3").join("nbg.geo"), b"geo")?;
        write_file(&root.join("step3").join("nbg.node_map"), b"map")?;
        write_file(&root.join("step4").join("ebg.nodes"), b"en")?;
        write_file(&root.join("step4").join("ebg.csr"), b"ec")?;
        write_file(&root.join("step4").join("ebg.turn_table"), b"tt")?;

        // Per-mode samples for step2 / step5 / step6 / step7 / step8.
        write_file(&root.join("step2").join("way_attrs.car.bin"), b"wa-car")?;
        write_file(&root.join("step2").join("way_attrs.bike.bin"), b"wa-bike")?;
        write_file(&root.join("step2").join("turn_rules.car.bin"), b"tr-car")?;
        write_file(&root.join("step2").join("turn_rules.bike.bin"), b"tr-bike")?;

        write_file(&root.join("step5").join("filtered.car.ebg"), b"fil-car")?;
        write_file(&root.join("step5").join("filtered.bike.ebg"), b"fil-bike")?;
        write_file(&root.join("step5").join("w.car.u32"), b"wcar")?;
        write_file(&root.join("step5").join("w.bike.u32"), b"wbike")?;
        write_file(&root.join("step5").join("t.car.u32"), b"tcar")?;
        write_file(&root.join("step5").join("t.bike.u32"), b"tbike")?;
        write_file(&root.join("step5").join("mask.car.bitset"), b"mc")?;
        write_file(&root.join("step5").join("mask.bike.bitset"), b"mb")?;

        write_file(&root.join("step6").join("order.car.ebg"), b"o-car")?;
        write_file(&root.join("step6").join("order.bike.ebg"), b"o-bike")?;
        // Lifted variants must be skipped.
        write_file(
            &root.join("step6").join("order.lifted.car.ebg"),
            b"o-lifted",
        )?;

        write_file(&root.join("step7").join("cch.car.topo"), b"cch-car")?;
        write_file(&root.join("step7").join("cch.bike.topo"), b"cch-bike")?;

        write_file(&root.join("step8").join("cch.w.car.u32"), b"wcar-cch")?;
        write_file(&root.join("step8").join("cch.w.bike.u32"), b"wbike-cch")?;
        write_file(&root.join("step8").join("cch.d.car.u32"), b"dcar-cch")?;
        // Future #84 traffic-customised file: pack must accept it
        // without knowing what `car_p3` means.
        write_file(&root.join("step8").join("cch.w.car_p3.u32"), b"wcarp3")?;

        Ok(tmp)
    }

    #[test]
    fn pack_synth_then_inspect() -> Result<()> {
        let tmp = synth_dir()?;
        let out = tmp.path().join("test.butterfly");
        pack(tmp.path(), &out, None)?;
        let c = Container::open(&out)?;

        // shared global tables
        assert!(c.get("shared/step1.nodes.sa").is_some());
        assert!(c.get("shared/step1.nodes.si").is_some());
        assert!(c.get("shared/step1.ways.raw").is_some());
        assert!(c.get("shared/step1.relations.raw").is_some());
        // node_signals optional, missing is OK
        assert!(c.get("shared/step1.node_signals.bin").is_none());
        assert!(c.get("shared/nbg.csr").is_some());
        assert!(c.get("shared/ebg.nodes").is_some());

        // mode bundles (sorted alphabetically by mode)
        assert_eq!(c.list_modes(), vec!["bike".to_string(), "car".to_string()]);
        let way_attrs: Vec<&str> = c
            .iter_kind(SectionKind::WayAttrs)
            .map(|s| s.name.as_str())
            .collect();
        assert_eq!(way_attrs, vec!["mode/bike/way_attrs", "mode/car/way_attrs"]);

        // Lifted ordering must NOT appear under any mode bundle.
        let orders: Vec<&str> = c
            .iter_kind(SectionKind::OrderEbg)
            .map(|s| s.name.as_str())
            .collect();
        assert_eq!(orders, vec!["mode/bike/order", "mode/car/order"]);

        // sections_with_prefix walks bundles cleanly.
        let car_sections: Vec<&str> = c
            .sections_with_prefix("mode/car/")
            .map(|s| s.name.as_str())
            .collect();
        assert!(car_sections.contains(&"mode/car/topo"));
        assert!(car_sections.contains(&"mode/car/weights.time"));
        assert!(car_sections.contains(&"mode/car/order"));

        // Manifest is present and parseable as JSON-ish (we don't pull
        // in serde_json just for this assertion; substring is enough).
        let manifest = c.get(MANIFEST_NAME).expect("manifest missing");
        let mbytes = c.read_section_verified(&out, manifest)?;
        let mtxt = std::str::from_utf8(&mbytes).unwrap();
        assert!(mtxt.contains("\"modes\""));
        assert!(mtxt.contains("\"car\""));
        assert!(mtxt.contains("\"bike\""));

        // CRCs verify end-to-end.
        c.verify_file_crc(&out)?;
        for sec in &c.sections {
            let bytes = c.read_section_verified(&out, sec)?;
            if sec.name == "shared/step1.nodes.sa" {
                assert_eq!(&bytes, b"sa-bytes");
            }
        }
        Ok(())
    }

    #[test]
    fn inspect_runs_clean() -> Result<()> {
        let tmp = synth_dir()?;
        let out = tmp.path().join("test.butterfly");
        pack(tmp.path(), &out, None)?;
        // No assertions on stdout here; we just want the call path to
        // not panic on a real pack output.
        inspect(&out, true, true)?;
        Ok(())
    }

    #[test]
    fn unpack_is_byte_for_byte_round_trip() -> Result<()> {
        let tmp = synth_dir()?;
        let container = tmp.path().join("rt.butterfly");
        pack(tmp.path(), &container, None)?;

        let unpacked = tmp.path().join("rt-out");
        unpack(&container, &unpacked)?;

        // Spot-check a handful of files for byte equality.
        let pairs: &[(&str, &str)] = &[
            ("step1/nodes.sa", "step1/nodes.sa"),
            ("step1/ways.raw", "step1/ways.raw"),
            ("step2/way_attrs.car.bin", "step2/way_attrs.car.bin"),
            ("step2/turn_rules.bike.bin", "step2/turn_rules.bike.bin"),
            ("step5/filtered.car.ebg", "step5/filtered.car.ebg"),
            ("step6/order.bike.ebg", "step6/order.bike.ebg"),
            ("step7/cch.car.topo", "step7/cch.car.topo"),
            ("step8/cch.w.car.u32", "step8/cch.w.car.u32"),
        ];
        for (src, dst) in pairs {
            let original = fs::read(tmp.path().join(src))?;
            let restored = fs::read(unpacked.join(dst))?;
            assert_eq!(original, restored, "byte mismatch for {} ↔ {}", src, dst);
        }

        // Files that pack skipped (lifted) must NOT show up in the
        // unpacked tree.
        assert!(!unpacked.join("step6/order.lifted.car.ebg").exists());
        Ok(())
    }

    #[test]
    fn unpack_refuses_existing_dir() -> Result<()> {
        let tmp = synth_dir()?;
        let container = tmp.path().join("rt.butterfly");
        pack(tmp.path(), &container, None)?;

        let existing = tmp.path().join("already-here");
        fs::create_dir_all(&existing)?;
        let res = unpack(&container, &existing);
        assert!(res.is_err());
        Ok(())
    }
}

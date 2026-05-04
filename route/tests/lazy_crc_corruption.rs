//! Integration test: corrupted section in a `.butterfly` container
//! transitions to `Failed` and produces a 503-style error from the
//! lazy gate, satisfying the #160 acceptance gate #5.
//!
//! Builds a tiny synthetic container, flips a byte inside one named
//! section's payload, opens it lazily, and asserts that the first
//! `section_bytes` call surfaces the CRC mismatch (the section name
//! appears in the error reason) and leaves the section permanently in
//! `Failed` state. A second call returns the same error — corruption
//! is not retried.

use butterfly_route::formats::butterfly_dat::{ContainerWriter, SectionKind};
use butterfly_route::formats::lazy_verify::{LazyContainer, SectionVerifyState};
use std::io::{Seek, SeekFrom, Write};
use tempfile::NamedTempFile;

fn write_demo(path: &std::path::Path) -> anyhow::Result<()> {
    let mut w = ContainerWriter::create(path)?;
    w.append_bytes(
        SectionKind::EbgNodes,
        "shared/ebg.nodes",
        b"hello ebg nodes",
    )?;
    w.append_bytes(SectionKind::CchTopo, "shared/cch.topo", b"shared topology")?;
    w.append_bytes(
        SectionKind::CchWeightsTime,
        "mode/car/weights.time",
        b"car time weights data",
    )?;
    w.append_bytes(
        SectionKind::CchWeightsTime,
        "mode/bike/weights.time",
        b"bike time weights data",
    )?;
    w.finalize()
}

#[test]
fn corrupted_section_fails_at_first_access_with_section_name_in_reason() -> anyhow::Result<()> {
    let tmp = NamedTempFile::new()?;
    write_demo(tmp.path())?;

    // Read the manifest to get the offset of the section we want to
    // corrupt. We use a fresh `LazyContainer` here (vs the one we'll
    // attack below) so we can be sure no state leaks across opens.
    let target_offset = {
        let lc = LazyContainer::open_lazy(tmp.path())?;
        lc.runtime("mode/bike/weights.time").unwrap().offset
    };

    // Flip one byte inside the payload of the targeted section.
    {
        let mut f = std::fs::OpenOptions::new().write(true).open(tmp.path())?;
        f.seek(SeekFrom::Start(target_offset + 3))?;
        f.write_all(&[0x00])?;
    }

    // Open lazily — manifest read still succeeds (directory CRC is
    // intact; only payload bytes were touched).
    let lc = LazyContainer::open_lazy(tmp.path())?;

    // Untouched section: still verifies cleanly on first access.
    let ok_bytes = lc.section_bytes("shared/cch.topo")?;
    assert_eq!(ok_bytes, b"shared topology");
    assert_eq!(
        lc.runtime("shared/cch.topo").unwrap().state(),
        SectionVerifyState::Verified
    );

    // Corrupted section: first access errors out with the section name
    // and the CRC mismatch reason embedded in the error message.
    let res = lc.section_bytes("mode/bike/weights.time");
    let err_str = match res {
        Ok(_) => panic!("expected CRC mismatch error, got Ok"),
        Err(e) => e.to_string(),
    };
    assert!(
        err_str.contains("mode/bike/weights.time"),
        "error must name the corrupted section, got: {}",
        err_str
    );
    assert!(
        err_str.contains("CRC mismatch"),
        "error must describe the CRC mismatch, got: {}",
        err_str
    );

    let rt = lc.runtime("mode/bike/weights.time").unwrap();
    assert_eq!(rt.state(), SectionVerifyState::Failed);
    let reason = rt.failure_reason().expect("failure_reason set");
    assert!(reason.contains("CRC mismatch"));

    // Second access must keep returning Failed — corruption is sticky.
    let res2 = lc.section_bytes("mode/bike/weights.time");
    assert!(res2.is_err());
    assert_eq!(rt.state(), SectionVerifyState::Failed);

    // The other-mode section is independent and still healthy.
    let ok_other = lc.section_bytes("mode/car/weights.time")?;
    assert_eq!(ok_other, b"car time weights data");
    Ok(())
}

#[test]
fn lazy_open_is_independent_per_container_handle() -> anyhow::Result<()> {
    // Two LazyContainer instances over the same file have independent
    // verifier state; one's failure does not poison the other. (Our
    // production server holds a single LazyContainer per ServerState,
    // so this is a defensive sanity check on the runtime independence
    // rather than a path the server itself exercises.)
    let tmp = NamedTempFile::new()?;
    write_demo(tmp.path())?;

    let lc_a = LazyContainer::open_lazy(tmp.path())?;
    let lc_b = LazyContainer::open_lazy(tmp.path())?;

    let _ = lc_a.section_bytes("shared/ebg.nodes")?;
    assert_eq!(
        lc_a.runtime("shared/ebg.nodes").unwrap().state(),
        SectionVerifyState::Verified
    );
    // lc_b's runtime for the same section is still Unverified.
    assert_eq!(
        lc_b.runtime("shared/ebg.nodes").unwrap().state(),
        SectionVerifyState::Unverified
    );
    Ok(())
}

//! `butterfly-gate` — the post-deploy correctness gate as one static binary
//! (#646). Same CLI, same output contract and same 36 checks as the Python
//! gate it replaced (`bench/postdeploy_gate.py`, deleted): `== name ==` banners,
//! `[PASS] / [FAIL] / [SKIP]` lines, `GATE: PASS|FAIL (s)`, exit 0/1.
//!
//! Usage
//! -----
//!     BUTTERFLY_REFS_DIR=/path/to/reference-trips \
//!     butterfly-gate --base http://localhost:3001 [--trips od.csv] [--quick]
//!         [--no-flight] [--flight-base grpc://host:port]
//!     butterfly-gate --list-gates

#![allow(clippy::too_many_arguments)]

mod ctx;
mod flight;
mod gates_flight;
mod gates_iso;
mod gates_refs;
mod gates_rest;
mod geom;
mod http;
mod pyrandom;
#[cfg(test)]
mod tests;

use std::path::PathBuf;
use std::time::Instant;

use clap::Parser;

use ctx::{Ctx, DEFAULT_TRIPS, LEGACY_TRIPS_DISTANCE, RefsErr, Thresholds, check, refs_path};
use http::GateErr;

/// Why a gate did not return a verdict of its own.
#[derive(Debug)]
pub enum GateFail {
    /// `$BUTTERFLY_REFS_DIR` unset: the gate SKIPs by name.
    Retired(String),
    /// A refs path that is not a directory: FAIL by name.
    Unavailable(String),
    /// Anything else the gate did not catch: FAIL by name with the message.
    Raised(String),
}

impl From<GateErr> for GateFail {
    fn from(e: GateErr) -> Self {
        GateFail::Raised(format!(
            "raised GateErr: {}",
            gates_rest::trunc(&e.to_string(), 200)
        ))
    }
}

impl From<RefsErr> for GateFail {
    fn from(e: RefsErr) -> Self {
        match e {
            RefsErr::Retired(s) => GateFail::Retired(s),
            RefsErr::Unavailable(s) => GateFail::Unavailable(s),
        }
    }
}

#[derive(Parser)]
#[command(
    name = "butterfly-gate",
    about = "Post-deploy correctness gate for a live butterfly server"
)]
struct Cli {
    /// e.g. http://localhost:3001
    #[arg(long)]
    base: Option<String>,
    /// duration reference set (default: od_typical.csv under $BUTTERFLY_REFS_DIR)
    #[arg(long)]
    trips: Option<String>,
    /// route-length reference set (default: od.csv under $BUTTERFLY_REFS_DIR)
    #[arg(long)]
    distance_trips: Option<String>,
    /// time-stamped reference sets <prefix>_{typical,best,worst}.csv (default prefix: 'od' under $BUTTERFLY_REFS_DIR)
    #[arg(long)]
    refs_prefix: Option<String>,
    /// skip the 1000-trip ground truth
    #[arg(long)]
    quick: bool,
    /// skip every Arrow Flight gate
    #[arg(long)]
    no_flight: bool,
    /// override the Flight URI (default: REST host, port+1)
    #[arg(long)]
    flight_base: Option<String>,
    /// print the gate names and exit 0 (CI smoke)
    #[arg(long)]
    list_gates: bool,
}

type Thunk<'a> = Box<dyn Fn(&Ctx) -> Result<bool, GateFail> + 'a>;

/// (name, needs_flight, thunk). ONE list — `--list-gates` prints it, main
/// runs it, CI smoke-tests it. The ORDER is load-bearing (#612).
fn build_gates<'a>(cli: &'a Cli, names: &'a [&'a str]) -> Vec<(&'static str, bool, Thunk<'a>)> {
    let trips = cli.trips.clone();
    let distance_trips = cli.distance_trips.clone();
    let refs_prefix = cli.refs_prefix.clone();
    let mut gates: Vec<(&'static str, bool, Thunk<'a>)> = vec![
        ("fixtures", false, Box::new(gates_rest::gate_fixtures)),
        ("symmetry", false, Box::new(gates_rest::gate_symmetry)),
        (
            "route_table_agreement",
            false,
            Box::new(gates_rest::gate_route_table_agreement),
        ),
        (
            "isochrone_topology",
            false,
            Box::new(gates_iso::gate_isochrone_topology),
        ),
        (
            "isochrone_reach_truth",
            false,
            Box::new(gates_iso::gate_isochrone_reach_truth),
        ),
        (
            "isochrone_upper_bound",
            false,
            Box::new(gates_iso::gate_isochrone_upper_bound),
        ),
        (
            "bands",
            false,
            Box::new(move |c: &Ctx| gates_refs::gate_bands(c, refs_prefix.as_deref())),
        ),
        (
            "ticket_invariants",
            false,
            Box::new(move |_c: &Ctx| gates_rest::gate_ticket_invariants(names)),
        ),
        (
            "lopsided_matrix",
            false,
            Box::new(gates_rest::gate_lopsided),
        ),
        // AFTER lopsided_matrix, and that is load-bearing (#612).
        (
            "isodistance_truth",
            false,
            Box::new(gates_iso::gate_isodistance_truth),
        ),
        (
            "radius_prune",
            false,
            Box::new(gates_rest::gate_radius_prune),
        ),
        (
            "radius_exactness",
            false,
            Box::new(gates_rest::gate_radius_exactness),
        ),
        (
            "recustomized_distance",
            false,
            Box::new(gates_rest::gate_recustomized_distance),
        ),
        (
            "mode_coherence",
            false,
            Box::new(gates_rest::gate_mode_coherence),
        ),
        (
            "one_way_routable",
            false,
            Box::new(gates_rest::gate_one_way_routable),
        ),
        ("graph_holes", false, Box::new(gates_rest::gate_graph_holes)),
        (
            "motorway_speed_floor",
            false,
            Box::new(gates_rest::gate_motorway_speed_floor),
        ),
        (
            "exclude_motorway",
            false,
            Box::new(gates_rest::gate_exclude_motorway),
        ),
        // AFTER exclude_motorway: car's two exclude masks are warm.
        (
            "isochrone_transports_agree",
            true,
            Box::new(gates_iso::gate_isochrone_transports_agree),
        ),
        (
            "flight_isochrone_batch",
            true,
            Box::new(gates_iso::gate_flight_isochrone_batch),
        ),
        (
            "edges_batch",
            true,
            Box::new(gates_flight::gate_edges_batch),
        ),
        (
            "matrix_sparse",
            true,
            Box::new(gates_flight::gate_matrix_sparse),
        ),
        (
            "matrix_sparse_streaming",
            true,
            Box::new(gates_flight::gate_matrix_sparse_streaming),
        ),
        (
            "flight_completeness",
            true,
            Box::new(gates_flight::gate_flight_completeness),
        ),
        (
            "matrix_distance_consistency",
            true,
            Box::new(gates_flight::gate_matrix_distance_consistency),
        ),
        (
            "bounded_matrix_exactness",
            true,
            Box::new(gates_flight::gate_bounded_matrix_exactness),
        ),
        (
            "route_batch_geometry",
            true,
            Box::new(gates_flight::gate_route_batch_geometry),
        ),
        (
            "route_batch_agrees_with_route",
            true,
            Box::new(gates_flight::gate_route_batch_agrees_with_route),
        ),
        (
            "route_batch_max_meters",
            true,
            Box::new(gates_flight::gate_route_batch_max_meters),
        ),
        (
            "catchment_containment",
            true,
            Box::new(gates_iso::gate_catchment_containment),
        ),
        (
            "all_endpoints_smoke",
            false,
            Box::new(gates_rest::gate_all_endpoints_smoke),
        ),
        (
            "transit_feeds",
            false,
            Box::new(gates_rest::gate_transit_feeds),
        ),
        (
            "edges_flow_storm",
            true,
            Box::new(gates_rest::gate_edges_flow_storm),
        ),
    ];
    if !cli.quick {
        let t1 = trips.clone();
        gates.push((
            "ground_truth_duration",
            false,
            Box::new(move |c: &Ctx| {
                let p = refs_path(c.refs_dir.as_deref(), DEFAULT_TRIPS, t1.as_deref())?;
                gates_refs::gate_ground_truth(c, &p, "duration")
            }),
        ));
        gates.push((
            "ground_truth_distance",
            false,
            Box::new(move |c: &Ctx| {
                let p = refs_path(
                    c.refs_dir.as_deref(),
                    LEGACY_TRIPS_DISTANCE,
                    distance_trips.as_deref(),
                )?;
                gates_refs::gate_ground_truth(c, &p, "distance")
            }),
        ));
        gates.push((
            "route_choice",
            false,
            Box::new(move |c: &Ctx| {
                let p = refs_path(c.refs_dir.as_deref(), DEFAULT_TRIPS, trips.as_deref())?;
                gates_refs::gate_route_choice(c, &p)
            }),
        ));
    }
    gates
}

/// Every registered gate name (the thunks are never called).
fn gate_names(cli: &Cli) -> Vec<&'static str> {
    build_gates(cli, &[])
        .into_iter()
        .map(|(n, _, _)| n)
        .collect()
}

fn main() {
    let cli = Cli::parse();
    let names = gate_names(&cli);
    if cli.list_gates {
        for (name, needs_flight, _) in build_gates(&cli, &names) {
            println!("{name}{}", if needs_flight { " [flight]" } else { "" });
        }
        std::process::exit(0);
    }
    let Some(base) = cli.base.clone() else {
        eprintln!("error: --base is required");
        std::process::exit(2);
    };
    let base = base.trim_end_matches('/').to_string();
    let flight = !cli.no_flight;
    println!(
        "post-deploy gate against {base}{}",
        if flight { "" } else { " (--no-flight)" }
    );
    let refs_dir: Option<PathBuf> = std::env::var_os("BUTTERFLY_REFS_DIR")
        .filter(|v| !v.is_empty())
        .map(PathBuf::from);
    let mut t = Thresholds::default();
    t.apply_windows(refs_dir.as_deref());
    t.print();
    let ctx = Ctx::new(base, t, flight, cli.flight_base.clone(), refs_dir);
    let mut ok = true;
    let t0 = Instant::now();
    for (name, needs_flight, thunk) in build_gates(&cli, &names) {
        if needs_flight && !flight {
            println!("== {name} ==");
            println!("  [SKIP] --no-flight");
            continue;
        }
        let started = Instant::now();
        match thunk(&ctx) {
            Ok(v) => ok &= v,
            Err(GateFail::Retired(e)) => {
                println!("== {name} ==");
                println!("  [SKIP] {e}");
            }
            Err(GateFail::Unavailable(e)) => ok &= check(name, false, &e),
            Err(GateFail::Raised(e)) => ok &= check(name, false, &e),
        }
        println!("  ({name}: {:.1}s)", started.elapsed().as_secs_f64());
    }
    println!(
        "\nGATE: {} ({:.1}s)",
        if ok { "PASS" } else { "FAIL" },
        t0.elapsed().as_secs_f64()
    );
    std::process::exit(if ok { 0 } else { 1 });
}

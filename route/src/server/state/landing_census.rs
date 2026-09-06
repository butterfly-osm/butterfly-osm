//! Per-class census of the boot recustomization landing (#608/#609).
//!
//! The mapping in [`super::recustomize`] used to report one number —
//! `matched` — which counts an edge that FOUND a row, not an edge that ends
//! up SERVING it. Between the two sit four distinct ways a measurement dies,
//! and #608 was diagnosed only because someone probed the served weights
//! from the outside and found whole road classes served at free-flow. That
//! is a diagnosis the engine should hand over itself, per class, at every
//! cold boot:
//!
//! * **inaccessible** — the edge is not part of this mode's network at all
//!   (`w == 0`); the observed table cannot land on it and should not.
//! * **no OSM id** — tail or head NBG node has no OSM id (container gap).
//! * **no row** — the edge is on the network but the table has no value for
//!   it: a genuine coverage gap, which the model exists to fill.
//! * **floored** — a value WAS found and then discarded by the free-flow
//!   floor, so the edge is served exactly as if it had never been measured.
//!   This is the one nobody was counting, and it was the bulk of the loss.
//!
//! Plus the two numbers that say whether the landing is faithful at all:
//! the median observed ratio per class (what the table says) and the median
//! SERVED ratio per class (`free-flow / served`, before the global level
//! anchor, which is a uniform multiplier and so cannot change their ratio).
//! `served / observed` at 1.00 means the class is replicated; above 1.00
//! means the class is served faster than the data says.

/// Highway classes as the shipped `models/*.model.json` number them
/// (`highway_class` block). Kept here as a REPORTING table only — nothing
/// in the derivation branches on it — and pinned by
/// `census_class_names_match_the_shipped_car_model`, so a model that
/// renumbers its classes cannot silently mislabel this report.
const CLASS_NAMES: [(u16, &str); 16] = [
    (1, "motorway"),
    (2, "motorway_link"),
    (3, "trunk"),
    (4, "trunk_link"),
    (5, "primary"),
    (6, "primary_link"),
    (7, "secondary"),
    (8, "secondary_link"),
    (9, "tertiary"),
    (10, "tertiary_link"),
    (11, "unclassified"),
    (12, "residential"),
    (13, "service"),
    (14, "living_street"),
    (15, "track"),
    (20, "path"),
];

/// Classes above this are reported in one `other` bucket (`construction`
/// is 99 in the shipped models, and an unknown id is not worth a row).
const MAX_CLASS: usize = 32;

fn class_name(class: u16) -> Option<&'static str> {
    CLASS_NAMES
        .iter()
        .find_map(|&(id, name)| (id == class).then_some(name))
}

/// One row of the census. Counts are disjoint by construction: every EBG
/// node of the class lands in exactly one of `inaccessible`, `no_osm_id`,
/// `no_row` or (`via_segments` + `via_junction`), and `floored` is a subset
/// of the matched two.
#[derive(Default, Clone)]
struct ClassRow {
    edges: u64,
    inaccessible: u64,
    no_osm_id: u64,
    no_row: u64,
    via_segments: u64,
    via_junction: u64,
    floored: u64,
    /// Seconds the floor threw away: Σ (free-flow − (observed − turn)) over
    /// the floored edges — what the network would otherwise be slower by.
    floored_lost_s: f64,
    /// Rows measured ABOVE the legal limit, truncated to it before the
    /// weight is derived (#609: nobody had counted this asymmetry either).
    truncated_high: u64,
    observed: Vec<f32>,
    served: Vec<f32>,
}

/// Per-class landing census, indexed by highway class id.
pub(super) struct LandingCensus<'a> {
    rows: Vec<ClassRow>,
    classes: &'a [u16],
}

impl<'a> LandingCensus<'a> {
    /// `classes[i]` is the highway class of EBG node `i` (an empty slice
    /// disables per-class attribution: everything lands in one bucket).
    pub(super) fn new(classes: &'a [u16]) -> Self {
        Self {
            rows: vec![ClassRow::default(); MAX_CLASS + 1],
            classes,
        }
    }

    #[inline]
    fn row(&mut self, node: usize) -> &mut ClassRow {
        let class = self.classes.get(node).copied().unwrap_or(0) as usize;
        let idx = if class > MAX_CLASS { MAX_CLASS } else { class };
        &mut self.rows[idx]
    }

    #[inline]
    pub(super) fn inaccessible(&mut self, node: usize) {
        let r = self.row(node);
        r.edges += 1;
        r.inaccessible += 1;
    }

    #[inline]
    pub(super) fn no_osm_id(&mut self, node: usize) {
        let r = self.row(node);
        r.edges += 1;
        r.no_osm_id += 1;
    }

    #[inline]
    pub(super) fn no_row(&mut self, node: usize) {
        let r = self.row(node);
        r.edges += 1;
        r.no_row += 1;
    }

    /// A row landed on this edge. `observed` is the table's speed ratio,
    /// `served` the ratio the engine will actually serve (free-flow over
    /// the new weight); `floored_lost_s` is 0 unless the free-flow floor
    /// bound, in which case it is what the floor threw away, in seconds.
    #[inline]
    pub(super) fn matched(
        &mut self,
        node: usize,
        via_segments: bool,
        observed: f32,
        served: f32,
        floored_lost_s: f64,
    ) {
        let r = self.row(node);
        r.edges += 1;
        if via_segments {
            r.via_segments += 1;
        } else {
            r.via_junction += 1;
        }
        if floored_lost_s > 0.0 {
            r.floored += 1;
            r.floored_lost_s += floored_lost_s;
        }
        if observed > 1.0 {
            r.truncated_high += 1;
        }
        r.observed.push(observed);
        r.served.push(served);
    }

    /// Log one line per non-empty class plus a total line. `column` names
    /// the value column the pass ran on (typical / best / worst), so the
    /// three passes of a cold boot stay tellable apart.
    pub(super) fn log(&mut self, column: &str) {
        let mut total = ClassRow::default();
        for idx in 0..self.rows.len() {
            let row = std::mem::take(&mut self.rows[idx]);
            if row.edges == 0 {
                continue;
            }
            let name = class_name(idx as u16)
                .map(|s| s.to_string())
                .unwrap_or_else(|| format!("class_{idx}"));
            log_row(column, &name, &row);
            total.merge(row);
        }
        log_row(column, "ALL", &total);
    }
}

impl ClassRow {
    fn merge(&mut self, mut other: ClassRow) {
        self.edges += other.edges;
        self.inaccessible += other.inaccessible;
        self.no_osm_id += other.no_osm_id;
        self.no_row += other.no_row;
        self.via_segments += other.via_segments;
        self.via_junction += other.via_junction;
        self.floored += other.floored;
        self.floored_lost_s += other.floored_lost_s;
        self.truncated_high += other.truncated_high;
        self.observed.append(&mut other.observed);
        self.served.append(&mut other.served);
    }
}

fn median(xs: &mut [f32]) -> f64 {
    if xs.is_empty() {
        return f64::NAN;
    }
    let mid = xs.len() / 2;
    xs.select_nth_unstable_by(mid, |a, b| a.total_cmp(b));
    xs[mid] as f64
}

fn log_row(column: &str, class: &str, row: &ClassRow) {
    let mut observed = row.observed.clone();
    let mut served = row.served.clone();
    let obs_p50 = median(&mut observed);
    let served_p50 = median(&mut served);
    let matched = row.via_segments + row.via_junction;
    let on_network = row.edges - row.inaccessible;
    let frac = |n: u64| {
        if on_network == 0 {
            0.0
        } else {
            n as f64 / on_network as f64
        }
    };
    tracing::info!(
        column,
        class,
        edges = row.edges,
        inaccessible = row.inaccessible,
        on_network,
        matched,
        via_segments = row.via_segments,
        via_junction = row.via_junction,
        no_osm_id = row.no_osm_id,
        no_row = row.no_row,
        no_row_frac = frac(row.no_row),
        floored = row.floored,
        floored_frac = frac(row.floored),
        floored_lost_s = row.floored_lost_s,
        truncated_high = row.truncated_high,
        truncated_high_frac = if matched == 0 {
            0.0
        } else {
            row.truncated_high as f64 / matched as f64
        },
        observed_p50 = obs_p50,
        served_p50 = served_p50,
        served_over_observed = served_p50 / obs_p50,
        "edge recustomize census (#608)"
    );
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The reporting table must name the classes the shipped models number.
    /// A model that renumbers `highway_class` would otherwise print a census
    /// labelled with someone else's classes — the exact way a diagnosis
    /// becomes a lie.
    #[test]
    fn census_class_names_match_the_shipped_car_model() {
        let path = concat!(env!("CARGO_MANIFEST_DIR"), "/../models/car.model.json");
        let text = std::fs::read_to_string(path).expect("shipped car model");
        let json: serde_json::Value = serde_json::from_str(&text).expect("valid json");
        let map = json["highway_class"]
            .as_object()
            .expect("highway_class block");
        for (highway, id) in map {
            let id = id.as_u64().expect("numeric class") as u16;
            let Some(name) = class_name(id) else {
                // Ids the census does not name (construction = 99) fall in
                // the last bucket by design.
                assert!(
                    id as usize > MAX_CLASS,
                    "class {id} ({highway}) is reported but unnamed"
                );
                continue;
            };
            // footway / path / cycleway / pedestrian / steps deliberately
            // share one id in the shipped models; the census calls it "path".
            let shared_id_20 = id == 20 && name == "path";
            assert!(
                name == highway || shared_id_20,
                "class {id} is '{highway}' in the model but '{name}' in the census"
            );
        }
    }

    #[test]
    fn census_counts_are_disjoint_and_medians_are_per_class() {
        let classes = [1u16, 1, 12, 12];
        let mut c = LandingCensus::new(&classes);
        c.inaccessible(0);
        c.matched(1, true, 0.5, 0.5, 0.0);
        c.matched(2, false, 0.25, 1.0, 7.5);
        c.no_row(3);
        assert_eq!(c.rows[1].edges, 2);
        assert_eq!(c.rows[1].inaccessible, 1);
        assert_eq!(c.rows[1].via_segments, 1);
        assert_eq!(c.rows[12].no_row, 1);
        assert_eq!(c.rows[12].floored, 1);
        assert_eq!(c.rows[12].floored_lost_s, 7.5);
        let mut obs = c.rows[12].observed.clone();
        assert_eq!(median(&mut obs), 0.25);
    }
}

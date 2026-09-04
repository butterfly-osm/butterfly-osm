//! One resolved region per query (#577).
//!
//! [`super::regions::RegionsState`] is the axum state of a dozen
//! handlers, but only `/route` and `/match` ever *solve* a cross-region
//! query (via [`super::cross_region::solve_cross_region`]). Every other
//! surface opened with the same prologue — take a start instant, run a
//! `dispatch_*`, keep `(state, region_id)`, reject mixed-region input —
//! and closed by recording the per-region metric with that instant.
//!
//! [`QueryContext`] is that prologue, once. Its constructors take the
//! start instant **before** dispatching (so the recorded duration keeps
//! covering the snap, exactly as the hand-written prologues did) and
//! return the [`super::regions::DispatchError`] unchanged: each surface
//! still renders its own status code and wording (REST `ErrorResponse`,
//! `/trip` + `/match`'s `{code,message}` shape, Flight's `Status`), and
//! still does so at the exact point in the handler where the prologue
//! sat, so no validation is re-ordered.
//!
//! It is deliberately **not** an axum extractor: the dispatch inputs
//! (`lon`/`lat`, `origin_*`/`destination_*`, a JSON body's coordinate
//! list) differ per surface, and every handler validates something
//! before dispatching — an extractor would run the snap first and
//! change which error a bad request gets.

use std::sync::Arc;
use std::time::Instant;

use super::regions::{DispatchError, RegionsState};
use super::state::ServerState;

/// The resolved single region for one query, plus the instant the
/// handler started measuring.
///
/// Handlers that never cross regions carry this instead of reaching
/// back into the multi-region state.
pub struct QueryContext {
    /// The winning region's `ServerState` (lazy-loaded by the dispatch).
    pub state: Arc<ServerState>,
    /// The winning region's id — the `region` label of every metric
    /// this query emits.
    pub region_id: String,
    /// The winning region's index in
    /// [`super::regions::RegionsState::regions`]. Bulk preflights that
    /// confirm the *rest* of their input against the same region use
    /// it (see [`super::regions::RegionsState::confirm_in_region`]).
    pub region_idx: usize,
    /// Taken before the dispatch, so [`Self::record`] observes the snap
    /// as well as the query itself.
    pub started: Instant,
}

impl QueryContext {
    /// Single-coordinate dispatch (`/nearest`, `/isochrone`, Flight
    /// `isochrone` / `catchment`).
    pub fn from_point(
        regions: &RegionsState,
        lon: f64,
        lat: f64,
        mode: &str,
    ) -> Result<Self, DispatchError> {
        let started = Instant::now();
        let (state, region_id, region_idx) = regions.dispatch_single(lon, lat, mode)?;
        Ok(Self {
            state,
            region_id,
            region_idx,
            started,
        })
    }

    /// Source/destination dispatch (`/transit`, `/transit/bulk`, Flight
    /// `matrix` / `route_batch` / `edges_batch`). Both endpoints must
    /// land in the same region; mixed input is
    /// [`DispatchError::CrossRegion`].
    pub fn from_pair(
        regions: &RegionsState,
        origin_lon: f64,
        origin_lat: f64,
        destination_lon: f64,
        destination_lat: f64,
        mode: &str,
    ) -> Result<Self, DispatchError> {
        let started = Instant::now();
        let (state, region_id, region_idx) = regions.dispatch_p2p(
            origin_lon,
            origin_lat,
            destination_lon,
            destination_lat,
            mode,
        )?;
        Ok(Self {
            state,
            region_id,
            region_idx,
            started,
        })
    }

    /// Many-coordinate dispatch (`/table`, `/trip`, `/catchment`,
    /// `/isochrone/bulk`). Every coordinate must land in the same
    /// region; mixed input is [`DispatchError::CrossRegion`].
    pub fn from_points<I>(
        regions: &RegionsState,
        coords: I,
        mode: &str,
    ) -> Result<Self, DispatchError>
    where
        I: IntoIterator<Item = (f64, f64)>,
    {
        let started = Instant::now();
        let (state, region_id, region_idx) = regions.dispatch_many(coords, mode)?;
        Ok(Self {
            state,
            region_id,
            region_idx,
            started,
        })
    }

    /// The primary region, no dispatch. For the degenerate request that
    /// carries no coordinate to snap and is about to fail validation
    /// anyway — `/catchment` with an empty `stores` or `clients` list —
    /// so the validation below it still runs against a real state and
    /// reports the primary region's id.
    pub fn primary(regions: &RegionsState) -> Self {
        let started = Instant::now();
        Self {
            state: regions.primary(),
            region_id: regions.regions[0].id.clone(),
            region_idx: 0,
            started,
        }
    }

    /// Emit this query's per-region metrics. Same call, same labels as
    /// the hand-written `record_query(&region_id, endpoint, elapsed)`
    /// epilogue it replaces — see
    /// [`super::region_metrics::record_query`].
    pub fn record(&self, endpoint: &str) {
        super::region_metrics::record_query(
            &self.region_id,
            endpoint,
            self.started.elapsed().as_secs_f64(),
        );
    }
}

//! OpenAPI specification with utoipa

use crate::routes::{
    BboxInfo, ErrorResponse, ProbeSnapQuery, ProbeSnapResponse, TelemetryQuery, TelemetryResponse,
    ValidationStatus,
};
use butterfly_extract::{
    CanonicalNodeProbe, DensityClass, GlobalPercentiles, TileId, TileMetrics, TilePercentiles,
    TileTelemetry,
};
use utoipa::OpenApi;

#[derive(OpenApi)]
#[openapi(
    paths(
        crate::routes::get_telemetry,
        crate::routes::probe_snap,
        crate::routes::graph_stats,
        crate::routes::graph_edge
    ),
    components(
        schemas(
            TelemetryQuery,
            TelemetryResponse,
            BboxInfo,
            ErrorResponse,
            ProbeSnapQuery,
            ProbeSnapResponse,
            ValidationStatus,
            CanonicalNodeProbe,
            TileTelemetry,
            TileMetrics,
            TilePercentiles,
            DensityClass,
            TileId,
            GlobalPercentiles
        )
    ),
    tags(
        (name = "telemetry", description = "Spatial density telemetry endpoints"),
        (name = "probe", description = "Canonical mapping validation endpoints"),
        (name = "graph", description = "Graph debugging and inspection endpoints"),
        (name = "routing", description = "Routing endpoints")
    ),
    info(
        title = "Butterfly OSM API",
        version = "2.0.0",
        description = "High-performance OpenStreetMap routing and telemetry API",
        contact(
            name = "Butterfly OSM",
            url = "https://github.com/butterfly-osm/butterfly-osm"
        )
    )
)]
pub struct ApiDoc;

/// Generate OpenAPI specification
pub fn openapi_spec() -> utoipa::openapi::OpenApi {
    ApiDoc::openapi()
}

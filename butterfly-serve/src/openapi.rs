//! OpenAPI specification with utoipa

use utoipa::OpenApi;
use crate::routes::{TelemetryQuery, TelemetryResponse, BboxInfo, ErrorResponse};
use butterfly_extract::{TileTelemetry, TileMetrics, TilePercentiles, DensityClass, TileId, GlobalPercentiles};

#[derive(OpenApi)]
#[openapi(
    paths(
        crate::routes::get_telemetry
    ),
    components(
        schemas(
            TelemetryQuery,
            TelemetryResponse,
            BboxInfo,
            ErrorResponse,
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

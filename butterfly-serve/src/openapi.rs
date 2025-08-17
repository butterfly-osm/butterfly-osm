//! OpenAPI specification with utoipa

use utoipa::OpenApi;

#[derive(OpenApi)]
#[openapi(
    paths(),
    components(),
    tags(
        (name = "routing", description = "Routing endpoints")
    )
)]
pub struct ApiDoc;

/// Generate OpenAPI specification
pub fn openapi_spec() -> utoipa::openapi::OpenApi {
    ApiDoc::openapi()
}

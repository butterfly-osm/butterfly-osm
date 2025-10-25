use crate::graph::RouteGraph;
use crate::route::find_route;
use axum::{
    extract::State,
    http::StatusCode,
    routing::post,
    Json, Router,
};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use tower_http::cors::CorsLayer;
use utoipa::{OpenApi, ToSchema};
use utoipa_swagger_ui::SwaggerUi;

#[derive(OpenApi)]
#[openapi(
    paths(route_handler),
    components(schemas(RouteRequest, RouteResponse, ErrorResponse))
)]
struct ApiDoc;

#[derive(Debug, Deserialize, ToSchema)]
pub struct RouteRequest {
    /// Starting point coordinates [latitude, longitude]
    #[schema(example = json!([50.8503, 4.3517]))]
    pub from: [f64; 2],

    /// Destination coordinates [latitude, longitude]
    #[schema(example = json!([51.2194, 4.4025]))]
    pub to: [f64; 2],
}

#[derive(Debug, Serialize, ToSchema)]
pub struct RouteResponse {
    /// Estimated distance in meters
    #[schema(example = 28964.0)]
    pub distance_meters: f64,

    /// Estimated travel time in seconds
    #[schema(example = 1932.0)]
    pub time_seconds: f64,

    /// Travel time in minutes (convenience field)
    #[schema(example = 32.2)]
    pub time_minutes: f64,

    /// Number of nodes in the route path
    #[schema(example = 880)]
    pub node_count: usize,
}

#[derive(Debug, Serialize, ToSchema)]
pub struct ErrorResponse {
    /// Error message
    pub error: String,
}

#[utoipa::path(
    post,
    path = "/route",
    request_body = RouteRequest,
    responses(
        (status = 200, description = "Route found successfully", body = RouteResponse),
        (status = 400, description = "Invalid request", body = ErrorResponse),
        (status = 500, description = "Route not found or server error", body = ErrorResponse)
    ),
    tag = "routing"
)]
async fn route_handler(
    State(graph): State<Arc<RouteGraph>>,
    Json(req): Json<RouteRequest>,
) -> Result<Json<RouteResponse>, (StatusCode, Json<ErrorResponse>)> {
    let from = (req.from[0], req.from[1]);
    let to = (req.to[0], req.to[1]);

    match find_route(&graph, from, to) {
        Ok(result) => Ok(Json(RouteResponse {
            distance_meters: result.distance_meters,
            time_seconds: result.time_seconds,
            time_minutes: result.time_seconds / 60.0,
            node_count: result.node_count,
        })),
        Err(e) => Err((
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(ErrorResponse {
                error: e.to_string(),
            }),
        )),
    }
}

pub async fn run_server(graph: RouteGraph, port: u16) -> anyhow::Result<()> {
    let graph = Arc::new(graph);

    let app = Router::new()
        .merge(SwaggerUi::new("/docs").url("/api-docs/openapi.json", ApiDoc::openapi()))
        .route("/route", post(route_handler))
        .layer(CorsLayer::permissive())
        .with_state(graph);

    let addr = format!("0.0.0.0:{}", port);
    println!("🚀 Server starting on http://{}", addr);
    println!("📚 API docs available at http://{}/docs", addr);

    let listener = tokio::net::TcpListener::bind(&addr).await?;
    axum::serve(listener, app).await?;

    Ok(())
}

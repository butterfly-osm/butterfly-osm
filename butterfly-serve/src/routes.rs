//! Route handlers

use axum::response::Json as ResponseJson;
use serde_json::Value;

/// Route endpoint placeholder
pub async fn route_handler() -> ResponseJson<Value> {
    ResponseJson(serde_json::json!({"message": "Route endpoint not implemented"}))
}

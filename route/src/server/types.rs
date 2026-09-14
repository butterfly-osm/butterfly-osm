//! Shared types used by multiple API handler modules

use axum::Json;
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

use crate::model::types::Mode;

/// THE error body of the REST surface: every endpoint that refuses a
/// request answers with this shape, and `error` is the field to read.
///
/// `code` and `message` are the deprecation window of #576. `/trip` and
/// `/match` used to answer with an ad-hoc `{"code": "InvalidValue",
/// "message": ...}` body that no OpenAPI component described; switching
/// them to `{error}` outright would break any client reading `message`.
/// For one release those two endpoints therefore emit BOTH — `error`
/// plus the two legacy fields, carrying the identical text — via
/// [`ErrorResponse::with_deprecated_fields`]. Every other endpoint
/// leaves them `None`, and `skip_serializing_if` keeps them out of the
/// body entirely, so the eight endpoints that already served `{error}`
/// are byte-identical to before.
///
/// Removing the window is deleting the two fields, the constructor that
/// sets them, and the two call sites — see CHANGELOG (deprecated in
/// 2.1.0, removed in 2.2.0).
#[derive(Debug, Serialize, ToSchema)]
pub struct ErrorResponse {
    /// Human-readable description of what was refused.
    pub error: String,
    /// Deprecated (#576), `/trip` and `/match` only: the legacy constant
    /// of that call site (`InvalidValue`, `NoSegment`, `NoMatch`,
    /// `InternalError`). Read `error` and the HTTP status instead.
    #[serde(skip_serializing_if = "Option::is_none")]
    #[schema(deprecated)]
    pub code: Option<String>,
    /// Deprecated (#576), `/trip` and `/match` only: a verbatim copy of
    /// `error`. Read `error` instead.
    #[serde(skip_serializing_if = "Option::is_none")]
    #[schema(deprecated)]
    pub message: Option<String>,
}

impl ErrorResponse {
    /// The documented body: `error` alone.
    pub fn new(error: impl Into<String>) -> Self {
        Self {
            error: error.into(),
            code: None,
            message: None,
        }
    }

    /// The same body plus the two legacy fields `/trip` and `/match`
    /// answered with before #576: `code` is the legacy constant of that
    /// call site (`InvalidValue`, `NoSegment`, `NoMatch`,
    /// `InternalError`) and `message` repeats `error` verbatim.
    /// Deprecation window only — see the type docs.
    pub fn with_deprecated_fields(mut self, code: &str) -> Self {
        self.code = Some(code.to_string());
        self.message = Some(self.error.clone());
        self
    }
}

/// Directional role of a snap query (#197). The packed snap index
/// stores one EBG node per directed edge in the underlying NBG, so
/// the geometrically-closest sample to a coordinate may have valid
/// outgoing transitions but no valid incoming transitions in the
/// requested mode (and vice versa). Returning that node for the
/// "wrong" role would cause /route to 404 even though a route
/// exists. The server picks the per-mode role bitset to apply based
/// on this enum.
///
/// `Src` is the current Rust default (via `#[default]`) and the
/// `/nearest` HTTP default (via `#[serde(default)]` on the request
/// struct), matching what most callers want. `Either` is the legacy
/// *behaviour* — the unfiltered snap that was the only option before
/// #197 — kept available for callers that explicitly want it (e.g.
/// `/isochrone` from a single point, where that point is *always* a
/// source but historically went through the unfiltered snap).
/// Practical usage:
///   - `/route` source point → `Src`
///   - `/route` destination point → `Dst`
///   - `/nearest` defaults to `Src` (current default), with
///     `role=src|dst|either` as a query parameter.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Serialize, ToSchema, Default)]
#[serde(rename_all = "lowercase")]
pub enum SnapRole {
    /// Source role: snap candidates must have at least one mode-valid
    /// outbound arc and be able to reach the main routing core.
    #[default]
    Src,
    /// Destination role: snap candidates must have at least one
    /// mode-valid inbound arc and be reachable from the main routing
    /// core.
    Dst,
    /// No role filter; behaves like the legacy snap.
    Either,
}

impl SnapRole {
    /// Resolve to the EBG-id-indexed bitset to use as the snap
    /// `role_filter`, or `None` for the unfiltered legacy behaviour.
    pub fn role_filter<'a>(
        &self,
        mode_data: &'a crate::server::state::ModeData,
    ) -> Option<&'a [u64]> {
        match self {
            SnapRole::Src => Some(&mode_data.has_outbound),
            SnapRole::Dst => Some(&mode_data.has_inbound),
            SnapRole::Either => None,
        }
    }
}

/// A waypoint with snapped location (used by table and trip responses)
#[derive(Debug, Serialize, ToSchema)]
pub struct Waypoint {
    /// Snapped location [lon, lat]
    pub location: [f64; 2],
    /// Name (empty for now)
    pub name: String,
}

/// Validate that a coordinate is within valid bounds.
pub fn validate_coord(lon: f64, lat: f64, label: &str) -> Result<(), String> {
    if !(-180.0..=180.0).contains(&lon) {
        return Err(format!(
            "{} longitude {} is outside valid range [-180, 180]",
            label, lon
        ));
    }
    if !(-90.0..=90.0).contains(&lat) {
        return Err(format!(
            "{} latitude {} is outside valid range [-90, 90]",
            label, lat
        ));
    }
    if lon.is_nan() || lat.is_nan() {
        return Err(format!("{} coordinates contain NaN", label));
    }
    Ok(())
}

/// Largest number of coordinates a matrix request may carry on EITHER
/// side.
///
/// This is the ceiling that actually bounds a matrix request's memory.
/// Cells are not: the engine's dense `S x T` allocation only happens
/// below the bucket/PHAST branch threshold, above it the Flight producer
/// tiles the source dimension and streams, and `/table` caps cells
/// separately because it must materialise a whole JSON grid. What scales
/// with NEITHER of those is the per-endpoint state — one snap through
/// the spatial index, up to six phantom seeds, a snapped coordinate, a
/// rank, a neighbour row — which is `O(S + T)` and, until this guard,
/// had no bound at all on the Flight `matrix` action. A ticket-sized
/// batch (tens of thousands of origins) is far under it; a request with
/// tens of millions of coordinates is refused before the first snap
/// instead of spending minutes building state for a matrix nobody can
/// consume.
///
/// A million per side sits an order of magnitude above the largest
/// documented shape (hundreds of thousands of origins against thousands
/// of destinations, streamed sparse) and below what the 64 MiB gRPC
/// ticket can carry anyway — it is a rail against a request that could
/// only be a mistake, not a throttle on real ones.
pub const MAX_MATRIX_ENDPOINTS: usize = 1_000_000;

/// Largest number of cells `POST /table` will answer. `/table` builds the
/// full grid in memory as JSON before it can reply, so it needs a cell
/// ceiling on top of [`MAX_MATRIX_ENDPOINTS`]; the Flight `matrix` action
/// streams and does not.
pub const MAX_TABLE_CELLS: usize = 10_000_000;

/// Refuse a matrix request whose endpoint counts exceed
/// [`MAX_MATRIX_ENDPOINTS`], naming the count that busted the limit and
/// the limit itself. Shared by `POST /table` and the Flight `matrix`
/// action so the two surfaces cannot drift apart on what they accept.
///
/// Applied before any snapping, so an over-large request costs a string
/// comparison rather than a spatial-index sweep per coordinate. It also
/// makes `n_origins * n_destinations` safe to compute afterwards.
pub fn validate_matrix_endpoints(n_origins: usize, n_destinations: usize) -> Result<(), String> {
    for (n, side) in [(n_origins, "sources"), (n_destinations, "destinations")] {
        if n > MAX_MATRIX_ENDPOINTS {
            return Err(format!(
                "too many {side}: {n} exceeds the limit of {MAX_MATRIX_ENDPOINTS} per side. \
                 Split the request into batches of at most {MAX_MATRIX_ENDPOINTS}"
            ));
        }
    }
    Ok(())
}

/// Refuse a `/table` request whose grid exceeds [`MAX_TABLE_CELLS`].
/// Call [`validate_matrix_endpoints`] first — it is what keeps the
/// multiplication below from overflowing.
pub fn validate_table_cells(n_origins: usize, n_destinations: usize) -> Result<(), String> {
    let cells = n_origins * n_destinations;
    if cells > MAX_TABLE_CELLS {
        return Err(format!(
            "matrix too large: {n_origins}x{n_destinations} = {cells} cells exceeds the limit \
             of {MAX_TABLE_CELLS}. Use the Flight `matrix` action for large matrices"
        ));
    }
    Ok(())
}

/// Parse mode string to Mode using dynamic lookup in state's mode_lookup table
pub fn parse_mode(
    s: &str,
    mode_lookup: &std::collections::HashMap<String, u8>,
) -> Result<Mode, String> {
    let s_lower = s.to_lowercase();
    match mode_lookup.get(&s_lower) {
        Some(&idx) => Ok(Mode(idx)),
        None => {
            let mut available: Vec<&str> = mode_lookup.keys().map(|s| s.as_str()).collect();
            available.sort(); // deterministic error message
            Err(format!(
                "Invalid mode: {}. Available: {}.",
                s,
                available.join(", ")
            ))
        }
    }
}

/// Helper: return a 400 Bad Request JSON error response
pub fn bad_request(error: String) -> (axum::http::StatusCode, Json<ErrorResponse>) {
    (
        axum::http::StatusCode::BAD_REQUEST,
        Json(ErrorResponse::new(error)),
    )
}

/// A 400 in the `/trip` and `/match` body of the #576 deprecation
/// window: the documented `error` field plus the legacy `code` /
/// `message` pair. Closing the window is deleting this function and
/// pointing its call sites at [`bad_request`].
pub fn bad_request_deprecated(
    error: impl Into<String>,
) -> (axum::http::StatusCode, Json<ErrorResponse>) {
    (
        axum::http::StatusCode::BAD_REQUEST,
        Json(ErrorResponse::new(error).with_deprecated_fields("InvalidValue")),
    )
}

// ===========================================================================
// Input that cannot be honoured is refused, never ignored (#612)
// ===========================================================================

/// Rewrite one serde rejection into the sentence a caller needs.
///
/// `axum`'s own rejection body is plain text — it never reaches
/// [`ErrorResponse`] — and it opens with the transport detail
/// (`Failed to deserialize query string: …`) rather than with the thing the
/// caller got wrong. serde's own half is exactly right, though: it NAMES the
/// offending key and lists what the endpoint does accept. So the transport
/// prefix is stripped, the serde half is kept verbatim, and the unknown-key
/// case gains the one fact the caller cannot infer from a 400 — that the
/// parameter was refused *because* it could not be honoured, not because it
/// was malformed.
fn refusal_message(kind: &str, body_text: &str) -> String {
    /// The transport half of every axum rejection this wraps. Anything
    /// else (a missing `Content-Type`, a body-read failure) has no serde
    /// half to keep and is passed through whole.
    const TRANSPORT_PREFIXES: &[&str] = &[
        "Failed to deserialize query string: ",
        "Failed to deserialize the JSON body into the target type: ",
        "Failed to parse the request body as JSON: ",
    ];
    const UNKNOWN: &str = "unknown field ";
    let detail = TRANSPORT_PREFIXES
        .iter()
        .find_map(|p| body_text.strip_prefix(p))
        .unwrap_or(body_text);
    // `unknown field` is not always at the start: the query-string
    // deserializer prefixes the failing key (`metric: unknown field
    // \`metric\`, expected \`lon\` or \`mode\``), the JSON one does not.
    // Both carry the list of names the endpoint DOES accept after it,
    // which is the half worth serving verbatim.
    match detail.find(UNKNOWN) {
        Some(at) => format!(
            "unknown {kind} {}. \
             A {kind} this endpoint cannot honour is refused, not ignored",
            &detail[at + UNKNOWN.len()..]
        ),
        None => format!("invalid {kind}s: {detail}"),
    }
}

/// `GET` query parameters, with an unknown one refused by name (#612).
///
/// Two things separate this from a bare [`axum::extract::Query`], and the
/// isochrone defect needed both. The request type carries
/// `#[serde(deny_unknown_fields)]`, so a parameter the endpoint cannot
/// honour — `metric=distance` on a build that has no such parameter, a
/// typo, a spelling from an older release — is an error instead of a
/// silently dropped key that returns a confident answer to a question
/// nobody asked. And the refusal is rendered as [`ErrorResponse`], the one
/// documented error shape of the REST surface (#576), instead of axum's
/// plain-text rejection which no OpenAPI component describes.
///
/// This is the web-surface half of #548, which closed the same hole on the
/// machine-facing Flight params.
pub struct ValidatedQuery<T>(pub T);

impl<T, S> axum::extract::FromRequestParts<S> for ValidatedQuery<T>
where
    T: serde::de::DeserializeOwned,
    S: Send + Sync,
{
    type Rejection = (axum::http::StatusCode, Json<ErrorResponse>);

    async fn from_request_parts(
        parts: &mut axum::http::request::Parts,
        state: &S,
    ) -> Result<Self, Self::Rejection> {
        match axum::extract::Query::<T>::from_request_parts(parts, state).await {
            Ok(axum::extract::Query(value)) => Ok(Self(value)),
            Err(rejection) => Err(bad_request(refusal_message(
                "query parameter",
                &rejection.body_text(),
            ))),
        }
    }
}

/// A JSON request body, with an unknown field refused by name (#612) — the
/// `POST` mirror of [`ValidatedQuery`]. Same two reasons: the body type
/// denies unknown fields, and the refusal is [`ErrorResponse`].
///
/// The #576 deprecation fields are deliberately NOT emitted here, on
/// `/trip` and `/match` included: those two carry `code`/`message` for the
/// 400s they already answered before #576, and no client can have been
/// reading a legacy body for a refusal that did not exist until now.
pub struct ValidatedJson<T>(pub T);

impl<T, S> axum::extract::FromRequest<S> for ValidatedJson<T>
where
    T: serde::de::DeserializeOwned,
    S: Send + Sync,
{
    type Rejection = (axum::http::StatusCode, Json<ErrorResponse>);

    async fn from_request(req: axum::extract::Request, state: &S) -> Result<Self, Self::Rejection> {
        match Json::<T>::from_request(req, state).await {
            Ok(Json(value)) => Ok(Self(value)),
            // 400, not axum's 422 for a well-formed body of the wrong
            // shape: every other refusal on this surface is a 400 and
            // that is the status the OpenAPI document promises.
            Err(rejection) => Err(bad_request(refusal_message(
                "body field",
                &rejection.body_text(),
            ))),
        }
    }
}

/// Get the location (lon, lat) of an EBG node
pub fn get_node_location(state: &super::state::ServerState, node_id: u32) -> [f64; 2] {
    let node = &state.ebg_nodes.nodes[node_id as usize];
    // EBG node has geom_idx pointing to NBG edge index. Read the first
    // polyline vertex via the flat edge geometry (#155); falls back to
    // [0.0, 0.0] for empty polylines, matching the legacy behaviour.
    let polyline = state.edge_geom.polyline(node.geom_idx);
    if !polyline.is_empty() {
        let (lon, lat) = polyline.at(0);
        return [lon, lat];
    }
    [0.0, 0.0]
}

#[cfg(test)]
mod validated_extractor_tests {
    //! #612: the extractor is what turns `deny_unknown_fields` into a
    //! REFUSAL a caller can read. The struct attribute alone is checked
    //! per request type in `api.rs`; what is pinned here is the wiring —
    //! that a query string carrying an unknown key comes back as 400 with
    //! the documented [`ErrorResponse`] body, naming the key, and that a
    //! clean query string still parses.
    use super::*;
    use axum::extract::FromRequestParts;
    use axum::http::{Request, StatusCode};

    #[derive(Debug, Deserialize)]
    #[serde(deny_unknown_fields)]
    struct Probe {
        lon: f64,
        #[serde(default)]
        mode: Option<String>,
    }

    fn extract(uri: &str) -> Result<Probe, (StatusCode, String)> {
        let (mut parts, ()) = Request::builder().uri(uri).body(()).unwrap().into_parts();
        let out = futures::executor::block_on(ValidatedQuery::<Probe>::from_request_parts(
            &mut parts,
            &(),
        ));
        match out {
            Ok(ValidatedQuery(v)) => Ok(v),
            Err((code, Json(body))) => {
                assert!(
                    body.code.is_none() && body.message.is_none(),
                    "a refusal invented by #612 has no legacy client to keep: {body:?}"
                );
                Err((code, body.error))
            }
        }
    }

    #[test]
    fn a_clean_query_string_still_parses() {
        let ok = extract("/isochrone?lon=4.35&mode=car").expect("valid query");
        assert_eq!(ok.lon, 4.35);
        assert_eq!(ok.mode.as_deref(), Some("car"));
    }

    #[test]
    fn an_unknown_query_parameter_is_refused_by_name() {
        // The exact shape of the #612 report: the parameter used to be
        // dropped and the endpoint answered 200 with an identical body.
        for uri in [
            "/isochrone?lon=4.35&metric=distance",
            "/isochrone?lon=4.35&pouet=42",
        ] {
            let (code, error) = extract(uri).expect_err(uri);
            assert_eq!(code, StatusCode::BAD_REQUEST, "{uri}");
            let named = error.contains("metric") || error.contains("pouet");
            assert!(named, "the refusal must name the parameter, got: {error}");
            assert!(
                error.starts_with("unknown query parameter"),
                "and say what kind of thing it refused, got: {error}"
            );
            assert!(
                error.contains("refused, not ignored"),
                "and why, got: {error}"
            );
        }
    }

    #[test]
    fn a_malformed_value_is_still_a_400_in_the_documented_shape() {
        let (code, error) = extract("/isochrone?lon=north").expect_err("not a float");
        assert_eq!(code, StatusCode::BAD_REQUEST);
        assert!(
            error.starts_with("invalid query parameters: "),
            "a bad VALUE is not an unknown KEY and must not claim to be: {error}"
        );
    }

    /// The transport prefix is stripped only when it is actually there:
    /// a rejection with no serde half (a missing `Content-Type`, say)
    /// must survive whole rather than be cut at its first colon.
    #[test]
    fn a_rejection_without_a_serde_half_is_passed_through_whole() {
        let whole = "Expected request with `Content-Type: application/json`";
        assert_eq!(
            refusal_message("body field", whole),
            format!("invalid body fields: {whole}")
        );
    }
}

#[cfg(test)]
mod error_body_tests {
    use super::*;

    /// The eight endpoints that already served `{error}` must keep
    /// serving exactly that: the #576 deprecation fields are skipped
    /// when unset, so their bodies are byte-identical to before.
    #[test]
    fn documented_body_is_the_error_field_alone() {
        let body = serde_json::to_value(ErrorResponse::new("nope")).unwrap();
        assert_eq!(body, serde_json::json!({"error": "nope"}));
    }

    /// `/trip` and `/match` serve the documented field AND the two
    /// legacy fields for one release, carrying the identical text, so a
    /// client reading `message` keeps working while `error` becomes
    /// available (#576).
    #[test]
    fn deprecated_body_carries_the_documented_field_and_the_legacy_pair() {
        let body =
            serde_json::to_value(ErrorResponse::new("nope").with_deprecated_fields("InvalidValue"))
                .unwrap();
        assert_eq!(
            body,
            serde_json::json!({
                "error": "nope",
                "code": "InvalidValue",
                "message": "nope",
            })
        );
    }
}

//! REST API for health checks and dynamic configuration
//!
//! Endpoints:
//! - GET /health - Gateway health status
//! - POST /config - Update rate limit
//! - GET /metrics - Prometheus metrics

use crate::dam::DamFilter;
use crate::metrics::GatewayMetrics;
use axum::{
    extract::State,
    http::StatusCode,
    response::IntoResponse,
    routing::{get, post},
    Json, Router,
};
use serde::{Deserialize, Serialize};
use std::sync::Arc;

/// Shared application state for API handlers
#[derive(Clone)]
pub struct ApiState {
    pub dam: Arc<DamFilter>,
    pub metrics: Arc<GatewayMetrics>,
}

/// Health check response
#[derive(Serialize)]
pub struct HealthResponse {
    pub status: &'static str,
    pub queue_depth: usize,
    pub rate_limit: u64,
    pub available_tokens: u64,
    pub dropped_total: u64,
}

/// Config update request
#[derive(Deserialize)]
pub struct ConfigRequest {
    pub new_rate: u64,
}

/// Config update response
#[derive(Serialize)]
pub struct ConfigResponse {
    pub success: bool,
    pub previous_rate: u64,
    pub new_rate: u64,
}

/// Build the API router
pub fn create_router(state: ApiState) -> Router {
    Router::new()
        .route("/health", get(health_handler))
        .route("/config", post(config_handler))
        .route("/metrics", get(metrics_handler))
        .with_state(state)
}

/// GET /health - Return gateway status
async fn health_handler(State(state): State<ApiState>) -> Json<HealthResponse> {
    Json(HealthResponse {
        status: "ok",
        queue_depth: state.dam.queue_depth(),
        rate_limit: state.dam.get_rate(),
        available_tokens: state.dam.available_tokens(),
        dropped_total: state.dam.dropped_count(),
    })
}

/// POST /config - Update rate limit dynamically
async fn config_handler(
    State(state): State<ApiState>,
    Json(req): Json<ConfigRequest>,
) -> Json<ConfigResponse> {
    let previous_rate = state.dam.get_rate();
    state.dam.set_rate(req.new_rate);
    
    // Update metrics gauge
    state.metrics.rate_limit.set(req.new_rate as i64);

    Json(ConfigResponse {
        success: true,
        previous_rate,
        new_rate: req.new_rate,
    })
}

/// GET /metrics - Prometheus metrics endpoint
async fn metrics_handler(State(state): State<ApiState>) -> impl IntoResponse {
    // Update live gauges before rendering
    state.metrics.queue_depth.set(state.dam.queue_depth() as i64);
    state.metrics.rate_limit.set(state.dam.get_rate() as i64);
    state.metrics.available_tokens.set(state.dam.available_tokens() as i64);

    (
        StatusCode::OK,
        [("content-type", "text/plain; charset=utf-8")],
        state.metrics.render(),
    )
}

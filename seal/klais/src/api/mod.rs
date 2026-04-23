//! REST API for KLAIS control and monitoring.

use std::sync::Arc;
use axum::{
    extract::State,
    http::StatusCode,
    response::IntoResponse,
    routing::{get, post},
    Json, Router,
};
use serde::{Deserialize, Serialize};

use crate::control::ControlPlane;
use crate::dam::DamFilter;

/// API state shared across handlers
pub struct ApiState {
    pub dam: Arc<DamFilter>,
    pub control: Arc<ControlPlane>,
}

/// Health check response
#[derive(Debug, Serialize)]
pub struct HealthResponse {
    pub status: String,
    pub queue_depth: usize,
    pub rate_limit: u64,
    pub version: String,
}

/// Config update request
#[derive(Debug, Deserialize)]
pub struct ConfigRequest {
    pub new_rate: u64,
}

/// Config update response
#[derive(Debug, Serialize)]
pub struct ConfigResponse {
    pub success: bool,
    pub new_rate: u64,
    pub message: String,
}

/// Statistics response
#[derive(Debug, Serialize)]
pub struct StatsResponse {
    pub dam: crate::dam::DamStatsSnapshot,
    pub control: crate::control::ControlState,
}

/// Error response
#[derive(Debug, Serialize)]
pub struct ErrorResponse {
    pub error: String,
}

/// Health check handler
async fn health_handler(
    State(state): State<Arc<ApiState>>,
) -> Json<HealthResponse> {
    Json(HealthResponse {
        status: "ok".to_string(),
        queue_depth: state.dam.queue_depth(),
        rate_limit: state.dam.get_rate(),
        version: crate::VERSION.to_string(),
    })
}

/// Config update handler
async fn config_handler(
    State(state): State<Arc<ApiState>>,
    Json(req): Json<ConfigRequest>,
) -> Result<Json<ConfigResponse>, (StatusCode, Json<ErrorResponse>)> {
    if req.new_rate == 0 {
        return Err((
            StatusCode::BAD_REQUEST,
            Json(ErrorResponse {
                error: "new_rate must be > 0".to_string(),
            }),
        ));
    }

    state.control.set_rate(req.new_rate);
    
    Ok(Json(ConfigResponse {
        success: true,
        new_rate: state.dam.get_rate(),
        message: format!("Rate limit updated to {}", req.new_rate),
    }))
}

/// Statistics handler
async fn stats_handler(
    State(state): State<Arc<ApiState>>,
) -> Json<StatsResponse> {
    Json(StatsResponse {
        dam: state.dam.stats().snapshot(),
        control: state.control.state(),
    })
}

/// Metrics handler (Prometheus format)
async fn metrics_handler(
    State(state): State<Arc<ApiState>>,
) -> impl IntoResponse {
    let dam_stats = state.dam.stats().snapshot();
    let control_state = state.control.state();
    
    let metrics = format!(
        r#"# HELP klais_dam_received_total Total packets received
# TYPE klais_dam_received_total counter
klais_dam_received_total {}

# HELP klais_dam_passed_total Packets passed through dam
# TYPE klais_dam_passed_total counter
klais_dam_passed_total {}

# HELP klais_dam_queued_total Packets queued
# TYPE klais_dam_queued_total counter
klais_dam_queued_total {}

# HELP klais_dam_dropped_total Packets dropped
# TYPE klais_dam_dropped_total counter
klais_dam_dropped_total {}

# HELP klais_dam_queue_depth Current queue depth
# TYPE klais_dam_queue_depth gauge
klais_dam_queue_depth {}

# HELP klais_dam_rate_limit Current rate limit
# TYPE klais_dam_rate_limit gauge
klais_dam_rate_limit {}

# HELP klais_inference_burst_probability Predicted burst probability
# TYPE klais_inference_burst_probability gauge
klais_inference_burst_probability {}

# HELP klais_inference_anomaly_score Current anomaly score
# TYPE klais_inference_anomaly_score gauge
klais_inference_anomaly_score {}

# HELP klais_control_pid_output PID controller output
# TYPE klais_control_pid_output gauge
klais_control_pid_output {}
"#,
        dam_stats.received,
        dam_stats.passed,
        dam_stats.queued,
        dam_stats.dropped,
        dam_stats.queue_depth,
        control_state.current_rate,
        control_state.last_inference.burst_probability,
        control_state.last_inference.anomaly_score,
        control_state.pid_output,
    );
    
    (StatusCode::OK, metrics)
}

/// Create the API router
pub fn create_router(state: Arc<ApiState>) -> Router {
    Router::new()
        .route("/health", get(health_handler))
        .route("/config", post(config_handler))
        .route("/stats", get(stats_handler))
        .route("/metrics", get(metrics_handler))
        .with_state(state)
}

/// Start the API server
pub async fn run_server(
    addr: &str,
    dam: Arc<DamFilter>,
    control: Arc<ControlPlane>,
) -> Result<(), std::io::Error> {
    let state = Arc::new(ApiState { dam, control });
    let router = create_router(state);
    
    let listener = tokio::net::TcpListener::bind(addr).await?;
    tracing::info!(addr = %addr, "API server started");
    
    axum::serve(listener, router).await?;
    
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::dam::DamConfig;
    use crate::control::ControlConfig;
    use tokio::sync::mpsc;
    use axum::body::Body;
    use axum::http::Request;
    use tower::ServiceExt;

    fn create_test_state() -> Arc<ApiState> {
        let (tx, _rx) = mpsc::channel(100);
        let dam = Arc::new(DamFilter::new(DamConfig::default(), tx));
        let control = Arc::new(ControlPlane::new(ControlConfig::default(), dam.clone()));
        Arc::new(ApiState { dam, control })
    }

    #[tokio::test]
    async fn test_health_endpoint() {
        let state = create_test_state();
        let router = create_router(state);
        
        let response = router
            .oneshot(Request::builder().uri("/health").body(Body::empty()).unwrap())
            .await
            .unwrap();
        
        assert_eq!(response.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn test_config_endpoint() {
        let state = create_test_state();
        let router = create_router(state);
        
        let response = router
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/config")
                    .header("content-type", "application/json")
                    .body(Body::from(r#"{"new_rate": 1000}"#))
                    .unwrap()
            )
            .await
            .unwrap();
        
        assert_eq!(response.status(), StatusCode::OK);
    }
}

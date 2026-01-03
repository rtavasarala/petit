//! HTTP API module for the petit scheduler.
//!
//! Provides REST endpoints for triggering jobs, querying status, and controlling the scheduler.

mod errors;
mod handlers;
mod responses;

pub use errors::ApiError;
pub use handlers::ApiState;
pub use responses::*;

use axum::{
    routing::{get, post},
    Router,
};
use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::net::TcpListener;
use tower_http::cors::{Any, CorsLayer};
use tower_http::trace::TraceLayer;

use crate::core::job::Job;
use crate::core::types::JobId;
use crate::scheduler::SchedulerHandle;
use crate::storage::Storage;

/// Configuration for the API server.
#[derive(Debug, Clone)]
pub struct ApiConfig {
    /// Host to bind to.
    pub host: String,
    /// Port to bind to.
    pub port: u16,
}

impl Default for ApiConfig {
    fn default() -> Self {
        Self {
            host: "127.0.0.1".to_string(),
            port: 8565,
        }
    }
}

impl ApiConfig {
    /// Create a new API config with custom host and port.
    pub fn new(host: impl Into<String>, port: u16) -> Self {
        Self {
            host: host.into(),
            port,
        }
    }

    /// Get the socket address.
    pub fn socket_addr(&self) -> SocketAddr {
        format!("{}:{}", self.host, self.port)
            .parse()
            .expect("invalid socket address")
    }
}

/// Build the API router with all endpoints.
pub fn build_router<S: Storage + 'static>(state: ApiState<S>) -> Router {
    Router::new()
        // Health check
        .route("/api/health", get(handlers::health))
        // Scheduler control
        .route(
            "/api/scheduler/state",
            get(handlers::get_scheduler_state::<S>),
        )
        .route("/api/scheduler/pause", post(handlers::pause_scheduler::<S>))
        .route(
            "/api/scheduler/resume",
            post(handlers::resume_scheduler::<S>),
        )
        // Jobs
        .route("/api/jobs", get(handlers::list_jobs::<S>))
        .route("/api/jobs/{job_id}", get(handlers::get_job::<S>))
        .route(
            "/api/jobs/{job_id}/trigger",
            post(handlers::trigger_job::<S>),
        )
        .route("/api/jobs/{job_id}/runs", get(handlers::list_runs::<S>))
        // Runs
        .route("/api/runs/{run_id}", get(handlers::get_run::<S>))
        .route("/api/runs/{run_id}/tasks", get(handlers::list_tasks::<S>))
        // Middleware
        .layer(TraceLayer::new_for_http())
        .layer(
            CorsLayer::new()
                .allow_origin(Any)
                .allow_methods(Any)
                .allow_headers(Any),
        )
        .with_state(state)
}

/// Create the API state from scheduler components.
pub fn create_api_state<S: Storage>(
    handle: SchedulerHandle,
    storage: Arc<S>,
    jobs: Vec<Job>,
) -> ApiState<S> {
    let jobs_map: HashMap<JobId, Job> = jobs
        .into_iter()
        .map(|j| (j.id().clone(), j))
        .collect();

    ApiState {
        handle,
        storage,
        jobs: Arc::new(jobs_map),
    }
}

/// Start the API server.
///
/// This function spawns the server and returns a handle to the task.
/// The server runs until the task is aborted or the process exits.
pub async fn start_server<S: Storage + 'static>(
    config: ApiConfig,
    state: ApiState<S>,
) -> std::io::Result<tokio::task::JoinHandle<()>> {
    let router = build_router(state);
    let addr = config.socket_addr();

    let listener = TcpListener::bind(addr).await?;
    tracing::info!("API server listening on http://{}", addr);

    let handle = tokio::spawn(async move {
        if let Err(e) = axum::serve(listener, router).await {
            tracing::error!("API server error: {}", e);
        }
    });

    Ok(handle)
}

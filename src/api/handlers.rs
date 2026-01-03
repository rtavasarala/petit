//! API request handlers.

use axum::{
    extract::{Path, Query, State},
    Json,
};
use serde::Deserialize;
use std::collections::HashMap;
use std::sync::Arc;

use crate::core::job::Job;
use crate::core::types::{JobId, RunId};
use crate::scheduler::SchedulerHandle;
use crate::storage::Storage;

use super::errors::ApiError;
use super::responses::{
    HealthResponse, JobListResponse, JobResponse, MessageResponse, RunListResponse, RunResponse,
    SchedulerStateResponse, TaskListResponse, TaskStateResponse, TriggerResponse,
};

/// Shared application state for API handlers.
pub struct ApiState<S: Storage> {
    pub handle: SchedulerHandle,
    pub storage: Arc<S>,
    pub jobs: Arc<HashMap<JobId, Job>>,
}

impl<S: Storage> Clone for ApiState<S> {
    fn clone(&self) -> Self {
        Self {
            handle: self.handle.clone(),
            storage: Arc::clone(&self.storage),
            jobs: Arc::clone(&self.jobs),
        }
    }
}

/// Query parameters for list_runs endpoint.
#[derive(Debug, Deserialize)]
pub struct ListRunsQuery {
    #[serde(default = "default_limit")]
    pub limit: usize,
}

fn default_limit() -> usize {
    20
}

/// Health check endpoint.
pub async fn health() -> Json<HealthResponse> {
    Json(HealthResponse::default())
}

/// Get scheduler state.
pub async fn get_scheduler_state<S: Storage + 'static>(
    State(state): State<ApiState<S>>,
) -> Json<SchedulerStateResponse> {
    let scheduler_state = state.handle.state().await;
    Json(SchedulerStateResponse::from(scheduler_state))
}

/// Pause the scheduler.
pub async fn pause_scheduler<S: Storage + 'static>(
    State(state): State<ApiState<S>>,
) -> Result<Json<MessageResponse>, ApiError> {
    state.handle.pause().await?;
    Ok(Json(MessageResponse {
        message: "scheduler paused".to_string(),
    }))
}

/// Resume the scheduler.
pub async fn resume_scheduler<S: Storage + 'static>(
    State(state): State<ApiState<S>>,
) -> Result<Json<MessageResponse>, ApiError> {
    state.handle.resume().await?;
    Ok(Json(MessageResponse {
        message: "scheduler resumed".to_string(),
    }))
}

/// List all jobs.
pub async fn list_jobs<S: Storage + 'static>(
    State(state): State<ApiState<S>>,
) -> Json<JobListResponse> {
    let jobs: Vec<JobResponse> = state.jobs.values().map(JobResponse::from).collect();
    let count = jobs.len();
    Json(JobListResponse { jobs, count })
}

/// Get a specific job.
pub async fn get_job<S: Storage + 'static>(
    State(state): State<ApiState<S>>,
    Path(job_id): Path<String>,
) -> Result<Json<JobResponse>, ApiError> {
    let job_id = JobId::new(&job_id);
    let job = state
        .jobs
        .get(&job_id)
        .ok_or_else(|| ApiError::NotFound(format!("job not found: {}", job_id)))?;
    Ok(Json(JobResponse::from(job)))
}

/// Trigger a job.
pub async fn trigger_job<S: Storage + 'static>(
    State(state): State<ApiState<S>>,
    Path(job_id): Path<String>,
) -> Result<Json<TriggerResponse>, ApiError> {
    let run_id = state.handle.trigger(job_id.as_str()).await?;
    Ok(Json(TriggerResponse {
        run_id: run_id.to_string(),
        job_id: job_id.clone(),
        message: format!("job '{}' triggered", job_id),
    }))
}

/// List runs for a job.
pub async fn list_runs<S: Storage + 'static>(
    State(state): State<ApiState<S>>,
    Path(job_id): Path<String>,
    Query(query): Query<ListRunsQuery>,
) -> Result<Json<RunListResponse>, ApiError> {
    let job_id = JobId::new(&job_id);

    // Verify job exists
    if !state.jobs.contains_key(&job_id) {
        return Err(ApiError::NotFound(format!("job not found: {}", job_id)));
    }

    let runs = state.storage.list_runs(&job_id, query.limit).await?;
    let runs: Vec<RunResponse> = runs.into_iter().map(RunResponse::from).collect();
    let count = runs.len();
    Ok(Json(RunListResponse { runs, count }))
}

/// Get a specific run.
pub async fn get_run<S: Storage + 'static>(
    State(state): State<ApiState<S>>,
    Path(run_id): Path<String>,
) -> Result<Json<RunResponse>, ApiError> {
    let run_id = RunId::from_string(&run_id)
        .map_err(|_| ApiError::NotFound(format!("invalid run id: {}", run_id)))?;
    let run = state.storage.get_run(&run_id).await?;
    Ok(Json(RunResponse::from(run)))
}

/// List task states for a run.
pub async fn list_tasks<S: Storage + 'static>(
    State(state): State<ApiState<S>>,
    Path(run_id): Path<String>,
) -> Result<Json<TaskListResponse>, ApiError> {
    let run_id = RunId::from_string(&run_id)
        .map_err(|_| ApiError::NotFound(format!("invalid run id: {}", run_id)))?;

    // Verify run exists
    let _ = state.storage.get_run(&run_id).await?;

    let tasks = state.storage.list_task_states(&run_id).await?;
    let tasks: Vec<TaskStateResponse> = tasks.into_iter().map(TaskStateResponse::from).collect();
    let count = tasks.len();
    Ok(Json(TaskListResponse { tasks, count }))
}

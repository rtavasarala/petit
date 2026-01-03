//! API response types.

use serde::Serialize;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use crate::core::job::Job;
use crate::scheduler::SchedulerState;
use crate::storage::{RunStatus, StoredRun, StoredTaskState, TaskRunStatus};

/// Convert SystemTime to milliseconds since Unix epoch.
fn system_time_to_millis(time: SystemTime) -> u64 {
    time.duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

/// Convert Duration to milliseconds.
fn duration_to_millis(duration: Duration) -> u64 {
    duration.as_millis() as u64
}

/// Health check response.
#[derive(Debug, Serialize)]
pub struct HealthResponse {
    pub status: &'static str,
    pub version: &'static str,
}

impl Default for HealthResponse {
    fn default() -> Self {
        Self {
            status: "ok",
            version: env!("CARGO_PKG_VERSION"),
        }
    }
}

/// Scheduler state response.
#[derive(Debug, Serialize)]
pub struct SchedulerStateResponse {
    pub state: String,
    pub is_running: bool,
    pub is_paused: bool,
}

impl From<SchedulerState> for SchedulerStateResponse {
    fn from(state: SchedulerState) -> Self {
        Self {
            state: format!("{:?}", state).to_lowercase(),
            is_running: state == SchedulerState::Running,
            is_paused: state == SchedulerState::Paused,
        }
    }
}

/// Job summary for list responses.
#[derive(Debug, Serialize)]
pub struct JobResponse {
    pub id: String,
    pub name: String,
    pub enabled: bool,
    pub scheduled: bool,
    pub task_count: usize,
}

impl From<&Job> for JobResponse {
    fn from(job: &Job) -> Self {
        Self {
            id: job.id().to_string(),
            name: job.name().to_string(),
            enabled: job.is_enabled(),
            scheduled: job.is_scheduled(),
            task_count: job.dag().len(),
        }
    }
}

/// List of jobs response.
#[derive(Debug, Serialize)]
pub struct JobListResponse {
    pub jobs: Vec<JobResponse>,
    pub count: usize,
}

/// Trigger response.
#[derive(Debug, Serialize)]
pub struct TriggerResponse {
    pub run_id: String,
    pub job_id: String,
    pub message: String,
}

/// Run status response.
#[derive(Debug, Serialize)]
pub struct RunResponse {
    pub id: String,
    pub job_id: String,
    pub status: String,
    pub started_at: u64,
    pub ended_at: Option<u64>,
    pub duration_ms: Option<u64>,
    pub error: Option<String>,
}

impl From<StoredRun> for RunResponse {
    fn from(run: StoredRun) -> Self {
        Self {
            id: run.id.to_string(),
            job_id: run.job_id.to_string(),
            status: run_status_to_string(run.status),
            started_at: system_time_to_millis(run.started_at),
            ended_at: run.ended_at.map(system_time_to_millis),
            duration_ms: run.duration.map(duration_to_millis),
            error: run.error,
        }
    }
}

fn run_status_to_string(status: RunStatus) -> String {
    match status {
        RunStatus::Pending => "pending",
        RunStatus::Running => "running",
        RunStatus::Completed => "completed",
        RunStatus::Failed => "failed",
        RunStatus::Interrupted => "interrupted",
        RunStatus::Cancelled => "cancelled",
    }
    .to_string()
}

/// List of runs response.
#[derive(Debug, Serialize)]
pub struct RunListResponse {
    pub runs: Vec<RunResponse>,
    pub count: usize,
}

/// Task state response.
#[derive(Debug, Serialize)]
pub struct TaskStateResponse {
    pub task_id: String,
    pub run_id: String,
    pub status: String,
    pub attempts: u32,
    pub started_at: Option<u64>,
    pub ended_at: Option<u64>,
    pub duration_ms: Option<u64>,
    pub error: Option<String>,
}

impl From<StoredTaskState> for TaskStateResponse {
    fn from(state: StoredTaskState) -> Self {
        Self {
            task_id: state.task_id.to_string(),
            run_id: state.run_id.to_string(),
            status: task_status_to_string(state.status),
            attempts: state.attempts,
            started_at: state.started_at.map(system_time_to_millis),
            ended_at: state.ended_at.map(system_time_to_millis),
            duration_ms: state.duration.map(duration_to_millis),
            error: state.error,
        }
    }
}

fn task_status_to_string(status: TaskRunStatus) -> String {
    match status {
        TaskRunStatus::Pending => "pending",
        TaskRunStatus::Running => "running",
        TaskRunStatus::Completed => "completed",
        TaskRunStatus::Failed => "failed",
        TaskRunStatus::Skipped => "skipped",
    }
    .to_string()
}

/// List of task states response.
#[derive(Debug, Serialize)]
pub struct TaskListResponse {
    pub tasks: Vec<TaskStateResponse>,
    pub count: usize,
}

/// Simple message response.
#[derive(Debug, Serialize)]
pub struct MessageResponse {
    pub message: String,
}

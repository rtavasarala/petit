# HTTP API Implementation Plan for Petit Scheduler

## Overview
Add an HTTP API server that runs alongside the scheduler, enabling external CLI/tooling to trigger jobs and query status.

## API Endpoints

| Method | Endpoint | Description |
|--------|----------|-------------|
| `POST` | `/api/jobs/{job_id}/trigger` | Trigger a manual job run, returns `RunId` |
| `GET` | `/api/jobs` | List all registered jobs |
| `GET` | `/api/jobs/{job_id}` | Get job details |
| `GET` | `/api/jobs/{job_id}/runs` | List recent runs for a job (with `?limit=N`) |
| `GET` | `/api/runs/{run_id}` | Get run status and details |
| `GET` | `/api/runs/{run_id}/tasks` | Get task states for a run |
| `GET` | `/api/health` | Health check endpoint |
| `GET` | `/api/scheduler/state` | Get scheduler state (running/paused) |
| `POST` | `/api/scheduler/pause` | Pause scheduled triggers |
| `POST` | `/api/scheduler/resume` | Resume scheduled triggers |

## Implementation Steps

### 1. Add dependencies to Cargo.toml
- `axum` - HTTP framework (integrates well with existing tokio runtime)
- `tower-http` - Middleware (CORS, tracing)

### 2. Create API module structure
```
src/api/
├── mod.rs          # Module exports, router construction
├── handlers.rs     # Request handlers
├── responses.rs    # JSON response types
└── errors.rs       # Error type -> HTTP status mapping
```

### 3. Refactor storage to be shareable
Currently `Scheduler::new()` takes ownership of storage. Need to:
- Change to `Arc<S>` where `S: Storage`
- Pass same `Arc<S>` to both scheduler and API layer

**Files to modify:**
- `src/scheduler/engine.rs` - Change `storage: S` to `storage: Arc<S>`
- `src/main.rs` - Wrap storage in Arc before passing

### 4. Create shared application state
```rust
pub struct ApiState<S: Storage> {
    pub handle: SchedulerHandle,
    pub storage: Arc<S>,
    pub jobs: Arc<HashMap<JobId, Job>>,  // For job metadata lookup
}
```

### 5. Implement handlers
Map existing storage/handle methods to HTTP endpoints with proper error handling.

### 6. Add CLI flags for API server
- API server enabled by default
- `--no-api` flag to disable HTTP server
- `--api-port` to configure port (default: 8565)
- `--api-host` to configure bind address (default: 127.0.0.1)

### 7. Update main.rs to spawn API server
Run HTTP server as separate tokio task alongside scheduler.

## Error Mapping

| Error | HTTP Status |
|-------|-------------|
| `JobNotFound` | 404 |
| `StorageError::NotFound` | 404 |
| `DependencyNotSatisfied` | 409 Conflict |
| `MaxConcurrentRunsExceeded` | 429 Too Many Requests |
| `NotRunning` | 503 Service Unavailable |
| Other | 500 Internal Server Error |

## Files to Create/Modify

| File | Action |
|------|--------|
| `src/api/mod.rs` | Create |
| `src/api/handlers.rs` | Create |
| `src/api/responses.rs` | Create |
| `src/api/errors.rs` | Create |
| `src/lib.rs` | Add `pub mod api;` |
| `src/main.rs` | Add CLI flags, spawn API server |
| `src/scheduler/engine.rs` | Refactor to use `Arc<S>` |
| `Cargo.toml` | Add axum, tower-http |

## Example Usage

```bash
# Start scheduler (API enabled by default on port 8565)
petit --db petit.db run examples/jobs

# Start without API
petit --db petit.db run examples/jobs --no-api

# Custom port
petit --db petit.db run examples/jobs --api-port 9000

# Trigger job from another terminal
curl -X POST http://localhost:8565/api/jobs/hello_world/trigger

# Check run status
curl http://localhost:8565/api/runs/<run-id>

# List jobs
curl http://localhost:8565/api/jobs
```

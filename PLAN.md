# Petit Implementation Plan - Test-Driven Development

## Approach

- **One phase at a time**: Each phase delivered individually for your review
- **Compilable test stubs**: Real Rust code with `todo!()` macros showing the exact API
- Tests define the contract; you provide feedback; then implementation follows
- Tests serve as executable documentation

---

## Phase 1: Core Types & Task Abstraction

**Goal**: Define the fundamental types and Task trait

**Tests to write** (`src/core/task.rs`):
```rust
// Test: Define a simple task using the trait
// Test: Task returns success with output
// Test: Task returns error with message
// Test: Task accesses context inputs
// Test: Task writes to context outputs
// Test: Default resource requirements
// Test: Custom resource requirements
// Test: Default retry policy (no retries)
// Test: Custom retry policy
```

**Files**:
- `src/lib.rs` - crate root
- `src/core/mod.rs` - core module
- `src/core/types.rs` - TaskId, JobId, RunId, DagId
- `src/core/task.rs` - Task trait, TaskError
- `src/core/resource.rs` - ResourceRequirements, SystemResources
- `src/core/retry.rs` - RetryPolicy, RetryCondition
- `src/core/context.rs` - TaskContext, ContextReader, ContextWriter

---

## Phase 2: DAG Structure & Validation

**Goal**: Define DAG structure with dependency validation

**Tests to write** (`src/core/dag.rs`):
```rust
// Test: Create empty DAG
// Test: Add single task to DAG
// Test: Add task with dependency
// Test: Get topological order of tasks
// Test: Detect cycle in DAG (should error)
// Test: Detect missing dependency (should error)
// Test: Task conditions (AllSuccess, OnFailure, AllDone)
// Test: Get ready tasks (no pending dependencies)
// Test: Get downstream tasks of a given task
```

**Files**:
- `src/core/dag.rs` - Dag, TaskNode, TaskCondition

---

## Phase 3: Command Task

**Goal**: External command wrapper for shell/script execution

**Tests to write** (`src/execution/command.rs`):
```rust
// Test: Create command task with program and args
// Test: Command task implements Task trait
// Test: Execute simple command (echo)
// Test: Command with environment variables
// Test: Command with working directory
// Test: Command returns exit code on failure
// Test: Command captures stdout/stderr
// Test: Command with timeout
```

**Files**:
- `src/execution/mod.rs`
- `src/execution/command.rs` - CommandTask

---

## Phase 4: Resource Management

**Goal**: Resource pools and acquisition

**Tests to write** (`src/execution/resource.rs`):
```rust
// Test: Create resource pool with capacity
// Test: Acquire available resource (succeeds)
// Test: Acquire unavailable resource (blocks or fails)
// Test: Release resource returns to pool
// Test: Multiple pools (gpu, db_conn)
// Test: Partial acquisition (need 2, only 1 available)
// Test: ResourceGuard auto-releases on drop
// Test: System resource tracking (cpu, memory)
```

**Files**:
- `src/execution/resource.rs` - ResourceManager, ResourcePool, ResourceGuard

---

## Phase 5: Task Executor

**Goal**: Execute individual tasks with resource handling

**Tests to write** (`src/execution/executor.rs`):
```rust
// Test: Execute single task successfully
// Test: Execute task that fails
// Test: Execute task with retry on failure
// Test: Retry respects fixed delay
// Test: Retry stops after max attempts
// Test: Execute task waits for resources
// Test: Concurrent task execution respects limit
// Test: Task timeout enforcement
```

**Files**:
- `src/execution/executor.rs` - Executor, TaskResult

---

## Phase 6: DAG Executor

**Goal**: Execute full DAG respecting dependencies

**Tests to write** (`src/execution/dag_executor.rs`):
```rust
// Test: Execute single-task DAG
// Test: Execute linear DAG (A -> B -> C)
// Test: Execute diamond DAG (A -> B,C -> D)
// Test: Parallel execution of independent tasks
// Test: Task failure stops downstream tasks
// Test: OnFailure condition runs on upstream failure
// Test: AllDone condition runs regardless of status
// Test: Context flows from upstream to downstream
// Test: DAG execution respects concurrency limit
// Test: Collect all task results at end
```

**Files**:
- `src/execution/dag_executor.rs` - DagExecutor, DagResult

---

## Phase 7: Events

**Goal**: Lifecycle event emission and handling

**Tests to write** (`src/events/mod.rs`):
```rust
// Test: Emit TaskStarted event
// Test: Emit TaskCompleted event with duration
// Test: Emit TaskFailed event with error
// Test: Emit TaskRetrying event with attempt count
// Test: Emit JobStarted/JobCompleted events
// Test: Register event handler
// Test: Multiple handlers receive same event
// Test: Event timestamps are accurate
```

**Files**:
- `src/events/mod.rs` - Event enum, EventHandler trait, EventBus

---

## Phase 8: Storage Trait & In-Memory Implementation

**Goal**: Storage abstraction with in-memory backend

**Tests to write** (`src/storage/memory.rs`):
```rust
// Test: Save and retrieve job
// Test: List all jobs
// Test: Delete job
// Test: Save and retrieve job run
// Test: List runs for a job (with limit)
// Test: Save and retrieve task state
// Test: Get incomplete runs
// Test: Mark run as interrupted
// Test: Update run status
// Test: Storage is thread-safe (concurrent access)
```

**Files**:
- `src/storage/mod.rs` - Storage trait, StorageError
- `src/storage/memory.rs` - InMemoryStorage

---

## Phase 9: SQLite Storage

**Goal**: Persistent SQLite backend

**Tests to write** (`src/storage/sqlite.rs`):
```rust
// Test: Initialize database schema
// Test: Save and retrieve job (persists across restart)
// Test: All Storage trait methods (same as in-memory)
// Test: Transaction rollback on error
// Test: Concurrent writes are serialized
// Test: Migration from empty database
```

**Files**:
- `src/storage/sqlite.rs` - SqliteStorage
- `migrations/` - SQL schema files

---

## Phase 10: Schedule Parsing

**Goal**: Extended cron expression parsing

**Tests to write** (`src/core/schedule.rs`):
```rust
// Test: Parse standard 5-field cron
// Test: Parse extended 6-field cron (with seconds)
// Test: Parse @daily shortcut
// Test: Parse @hourly shortcut
// Test: Parse @weekly shortcut
// Test: Parse @every 5m interval
// Test: Get next occurrence from now
// Test: Get next N occurrences
// Test: Timezone-aware scheduling
// Test: Invalid cron expression returns error
```

**Files**:
- `src/core/schedule.rs` - Schedule, ScheduleError

---

## Phase 11: Job Definition

**Goal**: Job structure with schedule and dependencies

**Tests to write** (`src/core/job.rs`):
```rust
// Test: Create job with DAG and schedule
// Test: Job without schedule (manual trigger only)
// Test: Job with cross-job dependency
// Test: Job dependency conditions (LastSuccess, WithinWindow)
// Test: Job-level config passed to tasks
// Test: Job concurrency limit
// Test: Validate job (DAG valid, dependencies exist)
```

**Files**:
- `src/core/job.rs` - Job, JobDependency, DependencyCondition

---

## Phase 12: YAML Configuration

**Goal**: Parse job definitions from YAML

**Tests to write** (`src/config/yaml.rs`):
```rust
// Test: Parse minimal job YAML
// Test: Parse job with all fields
// Test: Parse task with command type
// Test: Parse task dependencies
// Test: Parse resource requirements
// Test: Parse retry policy
// Test: Parse schedule with timezone
// Test: Parse cross-job dependencies
// Test: Validation errors for invalid YAML
// Test: Load global config (petit.yaml)
```

**Files**:
- `src/config/mod.rs`
- `src/config/yaml.rs` - YamlLoader, JobConfig, GlobalConfig

---

## Phase 13: Scheduler Engine

**Goal**: Main scheduling loop

**Tests to write** (`src/scheduler/engine.rs`):
```rust
// Test: Scheduler triggers job at scheduled time
// Test: Scheduler respects cross-job dependencies
// Test: Manual job trigger
// Test: Pause and resume scheduler
// Test: Scheduler recovers interrupted runs on startup
// Test: Scheduler marks interrupted tasks per retry policy
// Test: Concurrent job runs (if allowed)
// Test: Scheduler emits events
```

**Files**:
- `src/scheduler/mod.rs`
- `src/scheduler/engine.rs` - Scheduler, SchedulerHandle

---

## Phase 14: Test Harness

**Goal**: Testing utilities for users of the library

**Tests to write** (`src/testing/mod.rs`):
```rust
// Test: MockTaskContext provides inputs
// Test: MockTaskContext captures outputs
// Test: TestHarness runs DAG with in-memory storage
// Test: Deterministic time control (advance time)
// Test: Verify execution order with timeline
// Test: Inject task failures for testing
// Test: FailingTask helper (fails N times then succeeds)
```

**Files**:
- `src/testing/mod.rs` - MockTaskContext, TestHarness, FailingTask

---

## Phase 15: Integration & Documentation Tests

**Goal**: End-to-end scenarios as doc tests

**Tests to write** (`tests/integration/`):
```rust
// Test: Complete workflow - define job in YAML, load, execute
// Test: Recovery scenario - interrupt and restart
// Test: Resource contention - multiple tasks competing
// Test: Complex DAG with mixed conditions
// Test: Cross-job dependency chain
```

**Files**:
- `tests/integration/workflow.rs`
- `tests/integration/recovery.rs`
- `tests/integration/resources.rs`

---

## Implementation Order

```
Phase 1 ──► Phase 2 ──► Phase 3
   │           │           │
   ▼           ▼           ▼
Phase 4 ──► Phase 5 ──► Phase 6
                           │
Phase 7 ◄──────────────────┘
   │
   ▼
Phase 8 ──► Phase 9
   │
   ▼
Phase 10 ─► Phase 11 ─► Phase 12
                           │
                           ▼
                       Phase 13 ─► Phase 14 ─► Phase 15
```

## Workflow Per Phase

1. I write compilable test stubs with `todo!()` showing exact API signatures
2. You review and provide feedback on the API design
3. I implement code to make tests pass
4. Move to next phase

---

## File Structure After All Phases

```
src/
├── lib.rs
├── core/
│   ├── mod.rs
│   ├── types.rs
│   ├── task.rs
│   ├── resource.rs
│   ├── retry.rs
│   ├── context.rs
│   ├── dag.rs
│   ├── schedule.rs
│   └── job.rs
├── execution/
│   ├── mod.rs
│   ├── command.rs
│   ├── resource.rs
│   ├── executor.rs
│   └── dag_executor.rs
├── events/
│   └── mod.rs
├── storage/
│   ├── mod.rs
│   ├── memory.rs
│   └── sqlite.rs
├── config/
│   ├── mod.rs
│   └── yaml.rs
├── scheduler/
│   ├── mod.rs
│   └── engine.rs
└── testing/
    └── mod.rs
tests/
└── integration/
    ├── workflow.rs
    ├── recovery.rs
    └── resources.rs
```

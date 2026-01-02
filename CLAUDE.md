# Petit - Project Context

## Overview

Petit is a minimal, lightweight task orchestrator written in Rust. It provides cron-like scheduling with a text-based user interface, designed for single-machine deployments.

## Key Design Decisions

- **Async execution** with configurable concurrency limits
- **Resource management** via abstract slots (e.g., "gpu", "db_conn") and system constraints (CPU, memory)
- **Fixed delay retry** policy for failed tasks
- **YAML configuration** for job definitions
- **Extended cron** syntax (6-field with seconds, timezone support)
- **Key-value context** for inter-task data flow
- **Cross-job dependencies** supported
- **TUI** available via feature flag (`--features tui`)

## Architecture

```
src/
├── core/           # Types, Task trait, DAG, scheduling
├── execution/      # Executor, resource management, command tasks
├── events/         # Lifecycle events
├── storage/        # In-memory and SQLite backends
├── config/         # YAML parsing
├── scheduler/      # Main scheduling loop
└── testing/        # Test harness utilities
```

## Development Approach

**Test-driven development (TDD)**:
- Write tests first that define the API contract
- Tests serve as executable documentation
- Implementation follows to make tests pass
- Red-green-refactor cycle

**Git workflow**:
- Use feature branches for new features/phases
- Small edits can go directly to main
- Create PRs for significant changes

See `PLAN.md` for the phased implementation plan.
See `SPEC.md` for detailed technical specification.

## Current Status

**Phase 1 Complete**: Core types and Task abstraction (test stubs only)
- `TaskId`, `JobId`, `RunId`, `DagId`
- `ResourceRequirements`, `SystemResources`
- `RetryPolicy`, `RetryCondition`
- `TaskContext`, `ContextReader`, `ContextWriter`
- `Task` trait, `TaskError`

## Running Tests

```bash
cargo test
```

Note: Tests will panic with `todo!()` until implementations are complete.

## Building

```bash
cargo build
cargo build --features tui  # With TUI support (not yet implemented)
```

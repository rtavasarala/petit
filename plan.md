# Context refactor plan (no clones/snapshots)

## Goal
Eliminate per-task cloning/snapshot merging while keeping task isolation, deterministic outputs, and concurrency safety.

## Current behavior summary
- Shared context store is `HashMap<String, Value>` behind `Arc<RwLock<_>>`.
- `TaskContext` provides `ContextReader` (read) and `ContextWriter` (write).
- `DagExecutor` clones the entire store per task, runs task, and merges the full snapshot back.
- `CommandTask` writes `{task_id}.stdout`, `{task_id}.stderr`, `{task_id}.exit_code` into context.

## Proposed model
- Shared store is global and read-only for tasks.
- Each task writes into a task-local output buffer.
- On task completion, only buffered writes are merged into the shared store.

## API changes
- `ContextReader` stays read-only; remove/limit `set_raw`.
- `ContextWriter` becomes buffered:
  - Internal `writes: HashMap<String, Value>`.
  - `set` inserts into `writes` (after prefixing and serialization).
  - Add `drain_writes()`/`into_writes()` for executor merge.
- `TaskContext` keeps `inputs` as shared reader and `outputs` as buffered writer.
- `DagExecutor` merges only `outputs` buffer after each task.

## Concurrency behavior
- Tasks read from shared store without cloning.
- Task outputs are applied only on completion, in dependency order.
- No accidental overwrites from full-store merges.

## Migration steps
1) Add buffered write support to `ContextWriter` and `drain_writes()`.
2) Update `DagExecutor` to:
   - Stop cloning the store.
   - Merge only buffered outputs.
3) Remove or restrict `ContextReader::set_raw` (move to internal store type if needed).
4) Update affected tests and any code that relied on full-store merging.
5) Verify `CommandTask` behavior unchanged (stdout/stderr/exit_code still written).

## Edge cases & safeguards
- Optional: add explicit `set_global` or `set_raw` on writer to allow unprefixed keys.
- Optional: collision detection for writes to existing keys.
- Ensure exit logs are still captured and emitted via events.

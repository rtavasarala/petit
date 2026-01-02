//! Task execution engine.
//!
//! This module provides the execution infrastructure for running tasks,
//! including external command execution and task orchestration.

mod command;
mod dag_executor;
mod executor;

pub use command::{CommandTask, CommandTaskBuilder};
pub use dag_executor::{DagExecutor, DagResult, TaskStatus};
pub use executor::{TaskExecutor, TaskResult};

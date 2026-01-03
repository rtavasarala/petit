//! TUI dashboard for monitoring Petit orchestrator state.
//!
//! This module provides a read-only terminal interface for viewing
//! jobs, runs, tasks, and events from a SQLite database.

mod app;
mod error;
mod event;
mod reader;
mod theme;
pub mod ui;

pub use app::App;
pub use error::TuiError;
pub use event::{AppEvent, EventLoop};
pub use reader::TuiReader;
pub use theme::Theme;

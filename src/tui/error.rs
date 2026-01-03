//! TUI-specific error types.

use std::path::PathBuf;
use thiserror::Error;

/// Errors that can occur in the TUI application.
#[derive(Debug, Error)]
pub enum TuiError {
    /// Database connection or query error.
    #[error("Database error: {0}")]
    Database(String),

    /// Database file not found.
    #[error("Database not found at path: {0}")]
    DatabaseNotFound(PathBuf),

    /// IO error (terminal, file operations).
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),

    /// Terminal rendering error.
    #[error("Terminal error: {0}")]
    Terminal(String),
}

impl From<sqlx::Error> for TuiError {
    fn from(err: sqlx::Error) -> Self {
        TuiError::Database(err.to_string())
    }
}

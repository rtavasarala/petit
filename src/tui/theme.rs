//! Color theme definitions for the TUI.

use ratatui::style::{Color, Modifier, Style};

/// Dark theme colors for the TUI.
pub struct Theme;

impl Theme {
    // Status colors
    pub const SUCCESS: Color = Color::Green;
    pub const FAILURE: Color = Color::Red;
    pub const RUNNING: Color = Color::Yellow;
    pub const PENDING: Color = Color::Blue;
    pub const SKIPPED: Color = Color::DarkGray;
    pub const DISABLED: Color = Color::DarkGray;

    // UI colors
    pub const HEADER_BG: Color = Color::DarkGray;
    pub const HEADER_FG: Color = Color::White;
    pub const SELECTED_BG: Color = Color::DarkGray;
    pub const BORDER: Color = Color::Gray;
    pub const TEXT: Color = Color::White;
    pub const TEXT_DIM: Color = Color::DarkGray;
    pub const ACCENT: Color = Color::Cyan;

    /// Style for completed/successful items.
    pub fn success() -> Style {
        Style::default().fg(Self::SUCCESS)
    }

    /// Style for failed items.
    pub fn failure() -> Style {
        Style::default().fg(Self::FAILURE)
    }

    /// Style for running/in-progress items.
    pub fn running() -> Style {
        Style::default().fg(Self::RUNNING)
    }

    /// Style for pending items.
    pub fn pending() -> Style {
        Style::default().fg(Self::PENDING)
    }

    /// Style for skipped/disabled items.
    pub fn skipped() -> Style {
        Style::default().fg(Self::SKIPPED)
    }

    /// Style for selected/highlighted items.
    pub fn selected() -> Style {
        Style::default()
            .bg(Self::SELECTED_BG)
            .add_modifier(Modifier::BOLD)
    }

    /// Style for the header/status bar.
    pub fn header() -> Style {
        Style::default().bg(Self::HEADER_BG).fg(Self::HEADER_FG)
    }

    /// Style for tab labels.
    pub fn tab_active() -> Style {
        Style::default()
            .fg(Self::ACCENT)
            .add_modifier(Modifier::BOLD)
    }

    /// Style for inactive tabs.
    pub fn tab_inactive() -> Style {
        Style::default().fg(Self::TEXT_DIM)
    }

    /// Style for borders.
    pub fn border() -> Style {
        Style::default().fg(Self::BORDER)
    }

    /// Style for normal text.
    pub fn text() -> Style {
        Style::default().fg(Self::TEXT)
    }

    /// Style for dimmed/secondary text.
    pub fn text_dim() -> Style {
        Style::default().fg(Self::TEXT_DIM)
    }
}

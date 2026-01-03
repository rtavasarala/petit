//! Status bar widget showing dashboard statistics.

use ratatui::{
    layout::Rect,
    style::{Color, Modifier, Style},
    text::{Line, Span},
    widgets::Paragraph,
    Frame,
};

use crate::tui::app::App;
use crate::tui::theme::Theme;

/// Render the status bar.
pub fn render(app: &App, frame: &mut Frame, area: Rect) {
    let mut spans = vec![
        Span::styled(" petit-tui ", Style::default().fg(Theme::ACCENT).add_modifier(Modifier::BOLD)),
        Span::raw("│ "),
        Span::styled(
            format!("Jobs: {}", app.stats.total_jobs),
            Style::default().fg(Theme::TEXT),
        ),
        Span::raw(" "),
        Span::styled(
            format!("({})", app.stats.enabled_jobs),
            Theme::text_dim(),
        ),
    ];

    // Running indicator
    if app.stats.running_now > 0 {
        spans.push(Span::raw(" │ "));
        spans.push(Span::styled(
            format!("Running: {}", app.stats.running_now),
            Style::default().fg(Theme::RUNNING).add_modifier(Modifier::BOLD),
        ));
    }

    // Pending indicator
    if app.stats.pending_now > 0 {
        spans.push(Span::raw(" │ "));
        spans.push(Span::styled(
            format!("Pending: {}", app.stats.pending_now),
            Style::default().fg(Theme::PENDING),
        ));
    }

    // Refresh indicator
    if app.refreshing {
        spans.push(Span::raw(" │ "));
        spans.push(Span::styled("⟳", Style::default().fg(Theme::ACCENT)));
    }

    // Error message
    if let Some(ref error) = app.error_message {
        spans.push(Span::raw(" │ "));
        spans.push(Span::styled(
            format!("Error: {}", truncate(error, 40)),
            Style::default().fg(Color::Red),
        ));
    }

    // Current time (right-aligned would need more complex layout)
    let now = chrono::Local::now();
    spans.push(Span::raw(" │ "));
    spans.push(Span::styled(
        now.format("%H:%M:%S").to_string(),
        Theme::text_dim(),
    ));

    let paragraph = Paragraph::new(Line::from(spans)).style(Theme::header());
    frame.render_widget(paragraph, area);
}

/// Truncate a string to a maximum length.
fn truncate(s: &str, max_len: usize) -> String {
    if s.len() <= max_len {
        s.to_string()
    } else {
        format!("{}...", &s[..max_len - 3])
    }
}

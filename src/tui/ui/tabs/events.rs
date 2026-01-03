//! Events tab renderer.
//!
//! Shows recent activity as a log of run state changes.

use ratatui::{
    Frame,
    layout::Rect,
    style::Modifier,
    text::{Line, Span},
    widgets::{Block, Borders, List, ListItem},
};

use crate::storage::RunStatus;
use crate::tui::app::App;
use crate::tui::theme::Theme;

/// Render the events tab.
pub fn render(app: &mut App, frame: &mut Frame, area: Rect) {
    // Events tab shows recent runs as activity log
    let items: Vec<ListItem> = app
        .runs
        .iter()
        .map(|run| {
            // Format as event-like entry
            let (event_type, event_style) = match run.status {
                RunStatus::Completed => ("COMPLETED", Theme::success()),
                RunStatus::Failed => ("FAILED", Theme::failure()),
                RunStatus::Running => ("STARTED", Theme::running()),
                RunStatus::Pending => ("PENDING", Theme::pending()),
                RunStatus::Interrupted => ("INTERRUPTED", Theme::failure()),
                RunStatus::Cancelled => ("CANCELLED", Theme::skipped()),
            };

            // Timestamp
            let timestamp = format_timestamp(run.started_at);

            // Duration for completed runs
            let duration_info = if matches!(run.status, RunStatus::Completed | RunStatus::Failed) {
                run.duration
                    .map(|d| format!(" ({})", App::format_duration(d)))
                    .unwrap_or_default()
            } else {
                String::new()
            };

            let spans = vec![
                Span::styled(format!("{} ", timestamp), Theme::text_dim()),
                Span::styled(
                    format!("{:<12}", event_type),
                    event_style.add_modifier(Modifier::BOLD),
                ),
                Span::raw(" "),
                Span::styled(run.job_id.as_str().to_string(), Theme::text()),
                Span::styled(duration_info, Theme::text_dim()),
            ];

            ListItem::new(Line::from(spans))
        })
        .collect();

    let list = List::new(items)
        .block(
            Block::default()
                .title(format!(" Activity Log ({}) ", app.runs.len()))
                .borders(Borders::ALL)
                .style(Theme::border()),
        )
        .highlight_style(Theme::selected().add_modifier(Modifier::BOLD))
        .highlight_symbol("▶ ");

    frame.render_stateful_widget(list, area, &mut app.runs_list_state);
}

/// Format a SystemTime as a timestamp string.
fn format_timestamp(time: std::time::SystemTime) -> String {
    use std::time::UNIX_EPOCH;

    let duration = time.duration_since(UNIX_EPOCH).unwrap_or_default();
    let secs = duration.as_secs() as i64;

    // Convert to chrono DateTime
    if let Some(dt) = chrono::DateTime::from_timestamp(secs, 0) {
        let local: chrono::DateTime<chrono::Local> = dt.into();
        local.format("%H:%M:%S").to_string()
    } else {
        "??:??:??".to_string()
    }
}

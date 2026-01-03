//! Runs tab renderer.

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

/// Render the runs tab.
pub fn render(app: &mut App, frame: &mut Frame, area: Rect) {
    let items: Vec<ListItem> = app
        .runs
        .iter()
        .map(|run| {
            // Status indicator and style
            let (status_icon, status_style) = match run.status {
                RunStatus::Completed => ("✓", Theme::success()),
                RunStatus::Failed => ("✗", Theme::failure()),
                RunStatus::Running => ("⟳", Theme::running()),
                RunStatus::Pending => ("…", Theme::pending()),
                RunStatus::Interrupted => ("!", Theme::failure()),
                RunStatus::Cancelled => ("⊘", Theme::skipped()),
            };

            // Run ID (truncated)
            let run_id = &run.id.to_string()[..8];

            // Duration
            let duration = run
                .duration
                .map(App::format_duration)
                .unwrap_or_else(|| "-".to_string());

            // Time ago
            let time_ago = App::format_relative(run.started_at);

            // Error (truncated)
            let error_span = if let Some(ref error) = run.error {
                let truncated = if error.len() > 30 {
                    format!("{}...", &error[..27])
                } else {
                    error.clone()
                };
                vec![Span::raw(" │ "), Span::styled(truncated, Theme::failure())]
            } else {
                vec![]
            };

            let mut spans = vec![
                Span::styled(format!("{} ", status_icon), status_style),
                Span::styled(format!("{} ", run_id), Theme::text()),
                Span::styled(format!("{:<12} ", run.job_id.as_str()), Theme::text_dim()),
                Span::styled(format!("{:>8}", duration), Theme::text_dim()),
                Span::raw(" │ "),
                Span::styled(time_ago, Theme::text_dim()),
            ];
            spans.extend(error_span);

            ListItem::new(Line::from(spans))
        })
        .collect();

    let title = if let Some(job) = app.selected_job() {
        format!(" Runs for {} ({}) ", job.job.name, app.runs.len())
    } else {
        format!(" Recent Runs ({}) ", app.runs.len())
    };

    let list = List::new(items)
        .block(
            Block::default()
                .title(title)
                .borders(Borders::ALL)
                .style(Theme::border()),
        )
        .highlight_style(Theme::selected().add_modifier(Modifier::BOLD))
        .highlight_symbol("▶ ");

    frame.render_stateful_widget(list, area, &mut app.runs_list_state);
}

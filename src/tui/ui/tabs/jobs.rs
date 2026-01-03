//! Jobs tab renderer.

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

/// Render the jobs tab.
pub fn render(app: &mut App, frame: &mut Frame, area: Rect) {
    let items: Vec<ListItem> = app
        .jobs
        .iter()
        .map(|job_stats| {
            let job = &job_stats.job;

            // Status indicator
            let status_icon = if job.enabled { "●" } else { "○" };
            let status_style = if job.enabled {
                Theme::success()
            } else {
                Theme::skipped()
            };

            // Schedule or "manual"
            let schedule = job.schedule.as_deref().unwrap_or("manual");

            // Last run info
            let last_run_info = if let Some(ref last) = job_stats.last_run {
                let status_str = match last.status {
                    RunStatus::Completed => Span::styled("✓", Theme::success()),
                    RunStatus::Failed => Span::styled("✗", Theme::failure()),
                    RunStatus::Running => Span::styled("⟳", Theme::running()),
                    RunStatus::Pending => Span::styled("…", Theme::pending()),
                    RunStatus::Interrupted => Span::styled("!", Theme::failure()),
                    RunStatus::Cancelled => Span::styled("⊘", Theme::skipped()),
                };
                let time_ago = App::format_relative(last.started_at);
                vec![
                    Span::raw(" │ "),
                    status_str,
                    Span::raw(" "),
                    Span::styled(time_ago, Theme::text_dim()),
                ]
            } else {
                vec![Span::styled(" │ never run", Theme::text_dim())]
            };

            // Success rate
            let success_rate = if job_stats.total_runs > 0 {
                let rate = (job_stats.successful_runs as f64 / job_stats.total_runs as f64) * 100.0;
                format!("{:.0}%", rate)
            } else {
                "-".to_string()
            };

            let mut spans = vec![
                Span::styled(format!("{} ", status_icon), status_style),
                Span::styled(format!("{:<20}", job.name), Theme::text()),
                Span::styled(format!(" {:15}", schedule), Theme::text_dim()),
                Span::styled(format!(" {:>4}", success_rate), Theme::text_dim()),
            ];
            spans.extend(last_run_info);

            ListItem::new(Line::from(spans))
        })
        .collect();

    let title = format!(" Jobs ({}) ", app.jobs.len());
    let list = List::new(items)
        .block(
            Block::default()
                .title(title)
                .borders(Borders::ALL)
                .style(Theme::border()),
        )
        .highlight_style(Theme::selected().add_modifier(Modifier::BOLD))
        .highlight_symbol("▶ ");

    frame.render_stateful_widget(list, area, &mut app.jobs_list_state);
}

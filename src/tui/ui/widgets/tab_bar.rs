//! Tab bar widget for navigation.

use ratatui::{
    Frame,
    layout::Rect,
    text::{Line, Span},
    widgets::Paragraph,
};

use crate::tui::app::{App, Tab};
use crate::tui::theme::Theme;

/// Render the tab bar.
pub fn render(app: &App, frame: &mut Frame, area: Rect) {
    let mut spans = vec![Span::raw(" ")];

    for (i, tab) in Tab::all().iter().enumerate() {
        if i > 0 {
            spans.push(Span::styled(" │ ", Theme::text_dim()));
        }

        let style = if *tab == app.current_tab {
            Theme::tab_active()
        } else {
            Theme::tab_inactive()
        };

        let prefix = if *tab == app.current_tab { "[" } else { " " };
        let suffix = if *tab == app.current_tab { "]" } else { " " };

        spans.push(Span::styled(prefix, style));
        spans.push(Span::styled(tab.name(), style));
        spans.push(Span::styled(suffix, style));
    }

    // Add context info based on current tab
    match app.current_tab {
        Tab::Runs => {
            if let Some(job) = app.selected_job() {
                spans.push(Span::styled(" │ ", Theme::text_dim()));
                spans.push(Span::styled(
                    format!("Job: {}", job.job.name),
                    Theme::text_dim(),
                ));
            }
        }
        Tab::Tasks => {
            if let Some(run) = app.selected_run() {
                spans.push(Span::styled(" │ ", Theme::text_dim()));
                spans.push(Span::styled(
                    format!("Run: {}", &run.id.to_string()[..8]),
                    Theme::text_dim(),
                ));
            }
        }
        _ => {}
    }

    let paragraph = Paragraph::new(Line::from(spans));
    frame.render_widget(paragraph, area);
}

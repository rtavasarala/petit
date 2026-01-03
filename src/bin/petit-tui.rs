//! Petit TUI - Terminal dashboard for monitoring the Petit orchestrator.
//!
//! Usage:
//!   petit-tui --db ./petit.db
//!   PETIT_DB=./petit.db petit-tui

use std::io;
use std::path::PathBuf;
use std::time::Duration;

use clap::Parser;
use crossterm::{
    execute,
    terminal::{disable_raw_mode, enable_raw_mode, EnterAlternateScreen, LeaveAlternateScreen},
};
use ratatui::{backend::CrosstermBackend, Terminal};

use petit::tui::{App, AppEvent, EventLoop, TuiError, TuiReader};
use petit::tui::ui::render;

/// Petit TUI - Monitor your orchestrator in the terminal.
#[derive(Parser)]
#[command(name = "petit-tui")]
#[command(about = "Terminal dashboard for monitoring the Petit orchestrator")]
struct Cli {
    /// Path to the SQLite database file.
    #[arg(short, long, env = "PETIT_DB")]
    db: PathBuf,
}

#[tokio::main]
async fn main() -> Result<(), TuiError> {
    let cli = Cli::parse();

    // Open database connection
    let reader = TuiReader::open(&cli.db).await?;

    // Setup terminal
    enable_raw_mode()?;
    let mut stdout = io::stdout();
    execute!(stdout, EnterAlternateScreen)?;
    let backend = CrosstermBackend::new(stdout);
    let mut terminal = Terminal::new(backend).map_err(|e| TuiError::Terminal(e.to_string()))?;

    // Create application
    let mut app = App::new(reader).await?;

    // Create event loop with 250ms tick rate
    let mut events = EventLoop::new(Duration::from_millis(250));

    // Main loop
    let result = run_app(&mut terminal, &mut app, &mut events).await;

    // Restore terminal
    disable_raw_mode()?;
    execute!(terminal.backend_mut(), LeaveAlternateScreen)?;
    terminal.show_cursor().map_err(|e| TuiError::Terminal(e.to_string()))?;

    result
}

/// Run the main application loop.
async fn run_app(
    terminal: &mut Terminal<CrosstermBackend<io::Stdout>>,
    app: &mut App,
    events: &mut EventLoop,
) -> Result<(), TuiError> {
    loop {
        // Draw UI
        terminal
            .draw(|frame| render(app, frame))
            .map_err(|e| TuiError::Terminal(e.to_string()))?;

        // Handle events
        if let Some(event) = events.next().await {
            match event {
                AppEvent::Key(key) => {
                    app.handle_key(key).await;
                }
                AppEvent::Tick => {
                    app.refresh().await;
                }
            }
        }

        // Check if we should quit
        if app.should_quit {
            return Ok(());
        }
    }
}

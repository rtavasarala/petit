//! Event handling for the TUI.
//!
//! Manages keyboard input and periodic refresh ticks.

use crossterm::event::{self, Event, KeyEvent};
use std::time::Duration;
use tokio::sync::mpsc;

/// Events that can occur in the TUI application.
#[derive(Debug)]
pub enum AppEvent {
    /// Keyboard input event.
    Key(KeyEvent),
    /// Periodic tick for data refresh.
    Tick,
}

/// Event loop that aggregates keyboard input and timer ticks.
pub struct EventLoop {
    rx: mpsc::Receiver<AppEvent>,
    _tx: mpsc::Sender<AppEvent>,
}

impl EventLoop {
    /// Create a new event loop with the specified tick rate.
    pub fn new(tick_rate: Duration) -> Self {
        let (tx, rx) = mpsc::channel(32);

        // Spawn keyboard input handler
        let tx_input = tx.clone();
        tokio::spawn(async move {
            loop {
                // Poll for keyboard events with a short timeout
                if event::poll(Duration::from_millis(50)).unwrap_or(false) {
                    if let Ok(Event::Key(key)) = event::read() {
                        if tx_input.send(AppEvent::Key(key)).await.is_err() {
                            break;
                        }
                    }
                }
            }
        });

        // Spawn tick timer
        let tx_tick = tx.clone();
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(tick_rate);
            loop {
                interval.tick().await;
                if tx_tick.send(AppEvent::Tick).await.is_err() {
                    break;
                }
            }
        });

        Self { rx, _tx: tx }
    }

    /// Wait for the next event.
    pub async fn next(&mut self) -> Option<AppEvent> {
        self.rx.recv().await
    }
}

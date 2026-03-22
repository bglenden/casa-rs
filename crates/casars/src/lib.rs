mod app;
mod config;
mod execution;
mod registry;
mod theme;
mod ui;

use std::io::{self, Stdout};
use std::time::{Duration, Instant};

use crossterm::event::{self, Event};
use crossterm::event::{
    DisableBracketedPaste, DisableMouseCapture, EnableBracketedPaste, EnableMouseCapture,
};
use crossterm::execute;
use crossterm::terminal::{
    EnterAlternateScreen, LeaveAlternateScreen, disable_raw_mode, enable_raw_mode,
};
use ratatui::Terminal;
use ratatui::backend::CrosstermBackend;
use thiserror::Error;

use crate::app::AppState;
use crate::registry::listobs_app;

/// Errors surfaced by the `casars` launcher.
#[derive(Debug, Error)]
pub enum CasarsError {
    #[error("terminal setup failed: {0}")]
    TerminalSetup(#[source] io::Error),
    #[error("terminal teardown failed: {0}")]
    TerminalTeardown(#[source] io::Error),
    #[error("launcher failed: {0}")]
    Launcher(String),
}

/// Run the `casars` terminal user interface.
pub fn run() -> Result<(), CasarsError> {
    let app_entry = listobs_app();
    let schema_result = app_entry.load_schema();
    let mut app = match schema_result {
        Ok(schema) => AppState::from_schema(app_entry, schema),
        Err(error) => AppState::schema_error(app_entry, error),
    };

    let mut terminal = TerminalGuard::enter()?;
    let mut last_tick = Instant::now();

    loop {
        let area = terminal
            .terminal
            .size()
            .map_err(CasarsError::TerminalSetup)?;
        let layout = ui::compute_layout(
            ratatui::layout::Rect::new(0, 0, area.width, area.height),
            &app,
        );
        terminal
            .terminal
            .draw(|frame| ui::draw(frame, &app, &layout))
            .map_err(CasarsError::TerminalSetup)?;

        app.drain_execution_events();
        if app.should_quit() {
            break;
        }

        let tick_rate = Duration::from_millis(100);
        let timeout = tick_rate.saturating_sub(last_tick.elapsed());
        if event::poll(timeout).map_err(CasarsError::TerminalSetup)? {
            let event = event::read().map_err(CasarsError::TerminalSetup)?;
            match event {
                Event::Key(key_event) => app.handle_key_event(key_event),
                Event::Paste(text) => app.handle_paste(text),
                Event::Mouse(mouse_event) => app.handle_mouse_event(mouse_event, &layout),
                _ => {}
            }
        }

        if last_tick.elapsed() >= tick_rate {
            app.on_tick();
            last_tick = Instant::now();
        }
    }

    terminal.leave()?;
    Ok(())
}

struct TerminalGuard {
    terminal: Terminal<CrosstermBackend<Stdout>>,
    active: bool,
}

impl TerminalGuard {
    fn enter() -> Result<Self, CasarsError> {
        enable_raw_mode().map_err(CasarsError::TerminalSetup)?;
        let mut stdout = io::stdout();
        execute!(
            stdout,
            EnterAlternateScreen,
            EnableMouseCapture,
            EnableBracketedPaste
        )
        .map_err(CasarsError::TerminalSetup)?;
        let backend = CrosstermBackend::new(stdout);
        let terminal = Terminal::new(backend).map_err(CasarsError::TerminalSetup)?;
        Ok(Self {
            terminal,
            active: true,
        })
    }

    fn leave(&mut self) -> Result<(), CasarsError> {
        if !self.active {
            return Ok(());
        }
        disable_raw_mode().map_err(CasarsError::TerminalTeardown)?;
        execute!(
            self.terminal.backend_mut(),
            DisableBracketedPaste,
            DisableMouseCapture,
            LeaveAlternateScreen
        )
        .map_err(CasarsError::TerminalTeardown)?;
        self.active = false;
        Ok(())
    }
}

impl Drop for TerminalGuard {
    fn drop(&mut self) {
        let _ = self.leave();
    }
}

#[cfg(test)]
mod tests;

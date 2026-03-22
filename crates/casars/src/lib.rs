// SPDX-License-Identifier: LGPL-3.0-or-later
mod app;
mod browser_client;
mod clipboard;
mod config;
mod execution;
mod registry;
mod startup;
mod theme;
mod ui;

use std::io::{self, Stdout, Write as _};
use std::time::{Duration, Instant};

use crossterm::event::{self, Event};
use crossterm::event::{
    DisableBracketedPaste, DisableMouseCapture, EnableBracketedPaste, EnableMouseCapture, KeyCode,
    KeyEvent, KeyEventKind, KeyModifiers,
};
use crossterm::execute;
use crossterm::terminal::{
    EnterAlternateScreen, LeaveAlternateScreen, disable_raw_mode, enable_raw_mode,
};
use ratatui::Terminal;
use ratatui::backend::CrosstermBackend;
use thiserror::Error;

use crate::app::AppState;
use crate::registry::{RegistryApp, registered_apps, resolve_app};
use crate::startup::{StartupLaunch, StartupSelection, StartupValue, parse_startup_args};

enum RunOutcome {
    Quit,
    Launcher,
}

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
    run_with_cli_args(std::iter::empty::<std::ffi::OsString>())
}

/// Run the `casars` terminal user interface for a specific registered app.
pub fn run_with_app(app_id: Option<&str>) -> Result<(), CasarsError> {
    let selection = match app_id {
        Some(id) => StartupSelection::App(StartupLaunch {
            app: resolve_app(Some(id)).map_err(CasarsError::Launcher)?,
            prefill: Vec::new(),
            auto_run: false,
        }),
        None => StartupSelection::Launcher,
    };
    run_with_selection(selection)
}

/// Run the `casars` terminal user interface from raw startup arguments.
pub fn run_with_cli_args(
    args: impl IntoIterator<Item = std::ffi::OsString>,
) -> Result<(), CasarsError> {
    let selection = parse_startup_args(args).map_err(CasarsError::Launcher)?;
    match selection {
        StartupSelection::PrintText(text) => {
            let mut stdout = io::stdout();
            stdout
                .write_all(text.as_bytes())
                .map_err(CasarsError::TerminalSetup)?;
            stdout.flush().map_err(CasarsError::TerminalSetup)?;
            Ok(())
        }
        selection => run_with_selection(selection),
    }
}

fn run_with_selection(selection: StartupSelection) -> Result<(), CasarsError> {
    let mut terminal = TerminalGuard::enter()?;
    let mut launch = match selection {
        StartupSelection::App(launch) => launch,
        StartupSelection::Launcher => match choose_app(&mut terminal)? {
            Some(app) => StartupLaunch {
                app,
                prefill: Vec::new(),
                auto_run: false,
            },
            None => {
                terminal.leave()?;
                return Ok(());
            }
        },
        StartupSelection::PrintText(_) => unreachable!("print-only startup handled above"),
    };

    loop {
        match run_selected_app(
            &mut terminal,
            launch.app.clone(),
            &launch.prefill,
            launch.auto_run,
        )? {
            RunOutcome::Quit => break,
            RunOutcome::Launcher => {
                let Some(next_app) = choose_app(&mut terminal)? else {
                    break;
                };
                launch = StartupLaunch {
                    app: next_app,
                    prefill: Vec::new(),
                    auto_run: false,
                };
            }
        }
    }

    terminal.leave()?;
    Ok(())
}

fn run_selected_app(
    terminal: &mut TerminalGuard,
    app_entry: RegistryApp,
    prefill: &[crate::startup::StartupPrefill],
    auto_run: bool,
) -> Result<RunOutcome, CasarsError> {
    let schema_result = app_entry.load_schema();
    let mut app = match schema_result {
        Ok(schema) => AppState::from_schema(app_entry, schema),
        Err(error) => AppState::schema_error(app_entry, error),
    };
    for value in prefill {
        let result = match &value.value {
            StartupValue::Text(text) => app.apply_startup_text_value(&value.id, text.clone()),
            StartupValue::Toggle(enabled) => app.apply_startup_toggle_value(&value.id, *enabled),
        };
        result.map_err(CasarsError::Launcher)?;
    }
    if auto_run {
        app.start_run_on_launch();
    }

    let mut last_tick = Instant::now();

    loop {
        let area = terminal
            .terminal
            .size()
            .map_err(CasarsError::TerminalSetup)?;
        let provisional_layout = ui::compute_layout(
            ratatui::layout::Rect::new(0, 0, area.width, area.height),
            &app,
        );
        app.sync_browser_viewport(
            provisional_layout.result_content.width,
            provisional_layout.result_content.height,
        );
        let layout = ui::compute_layout(
            ratatui::layout::Rect::new(0, 0, area.width, area.height),
            &app,
        );
        app.cache_output_layout(&layout);
        terminal
            .terminal
            .draw(|frame| ui::draw(frame, &app, &layout))
            .map_err(CasarsError::TerminalSetup)?;

        app.drain_execution_events();
        if app.should_quit() {
            return Ok(RunOutcome::Quit);
        }
        if app.should_return_to_launcher() {
            return Ok(RunOutcome::Launcher);
        }

        let tick_rate = Duration::from_millis(100);
        let timeout = tick_rate.saturating_sub(last_tick.elapsed());
        if event::poll(timeout).map_err(CasarsError::TerminalSetup)? {
            let event = event::read().map_err(CasarsError::TerminalSetup)?;
            match event {
                Event::Key(key_event) => {
                    if is_suspend_key(key_event) {
                        terminal.suspend_and_resume()?;
                        last_tick = Instant::now();
                        continue;
                    }
                    app.handle_key_event(key_event)
                }
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
}

fn choose_app(terminal: &mut TerminalGuard) -> Result<Option<RegistryApp>, CasarsError> {
    let apps = registered_apps();
    let mut selected = 0usize;
    let mut last_tick = Instant::now();

    loop {
        terminal
            .terminal
            .draw(|frame| ui::draw_launcher(frame, &apps, selected))
            .map_err(CasarsError::TerminalSetup)?;

        let tick_rate = Duration::from_millis(100);
        let timeout = tick_rate.saturating_sub(last_tick.elapsed());
        if event::poll(timeout).map_err(CasarsError::TerminalSetup)? {
            let event = event::read().map_err(CasarsError::TerminalSetup)?;
            if let Event::Key(key_event) = event {
                if is_suspend_key(key_event) {
                    terminal.suspend_and_resume()?;
                    last_tick = Instant::now();
                    continue;
                }
                match key_event.code {
                    KeyCode::Up | KeyCode::Char('k')
                        if key_event.modifiers.is_empty()
                            || key_event.modifiers == KeyModifiers::SHIFT =>
                    {
                        selected = selected.saturating_sub(1);
                    }
                    KeyCode::Down | KeyCode::Char('j')
                        if key_event.modifiers.is_empty()
                            || key_event.modifiers == KeyModifiers::SHIFT =>
                    {
                        selected = (selected + 1).min(apps.len().saturating_sub(1));
                    }
                    KeyCode::Enter => return Ok(apps.get(selected).cloned()),
                    KeyCode::Esc | KeyCode::Char('q') => return Ok(None),
                    _ => {}
                }
            }
        }

        if last_tick.elapsed() >= tick_rate {
            last_tick = Instant::now();
        }
    }
}

struct TerminalGuard {
    terminal: Terminal<CrosstermBackend<Stdout>>,
    active: bool,
}

impl TerminalGuard {
    fn enter() -> Result<Self, CasarsError> {
        let terminal = Self::activate_terminal()?;
        Ok(Self {
            terminal,
            active: true,
        })
    }

    fn activate_terminal() -> Result<Terminal<CrosstermBackend<Stdout>>, CasarsError> {
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
        Terminal::new(backend).map_err(CasarsError::TerminalSetup)
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

    #[cfg(unix)]
    fn suspend_and_resume(&mut self) -> Result<(), CasarsError> {
        self.leave()?;
        // SAFETY: `raise(SIGTSTP)` sends the standard suspend signal to the current process.
        let status = unsafe { libc::raise(libc::SIGTSTP) };
        if status != 0 {
            return Err(CasarsError::TerminalTeardown(io::Error::last_os_error()));
        }
        self.terminal = Self::activate_terminal()?;
        self.active = true;
        Ok(())
    }

    #[cfg(not(unix))]
    fn suspend_and_resume(&mut self) -> Result<(), CasarsError> {
        Ok(())
    }
}

pub(crate) fn is_suspend_key(key_event: KeyEvent) -> bool {
    if key_event.kind == KeyEventKind::Release {
        return false;
    }
    matches!(key_event.code, KeyCode::Char('z') | KeyCode::Char('Z'))
        && key_event.modifiers.contains(KeyModifiers::CONTROL)
}

impl Drop for TerminalGuard {
    fn drop(&mut self) {
        let _ = self.leave();
    }
}

#[cfg(test)]
mod tests;

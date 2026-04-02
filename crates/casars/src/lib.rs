// SPDX-License-Identifier: LGPL-3.0-or-later
mod app;
mod browser_client;
mod clipboard;
mod config;
mod execution;
mod graphics;
mod movie_perf;
mod pane_manager;
mod registry;
mod startup;
mod theme;
mod ui;

use std::fs::OpenOptions;
use std::io::{self, Stdout, Write as _};
use std::num::NonZeroU32;
use std::sync::OnceLock;
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
use ratatui_graphics::{
    KittyAnimationControl, KittyAnimationGap, KittyAnimationPlaybackState, KittyLayerHandle,
    KittyLayerManager, KittyPaneSlotId, KittyPlacement, KittyStoredImageId, KittyStoredImageStore,
    Picker, TerminalCapabilities,
};
use thiserror::Error;

use crate::app::AppState;
use crate::registry::{RegistryApp, registered_apps, resolve_app};
use crate::startup::{StartupLaunch, StartupSelection, StartupValue, parse_startup_args};

const KITTY_MOVIE_OVERLAY_ID_BASE: u32 = 1_000_000;

fn software_direct_movie_overlay_enabled() -> bool {
    std::env::var_os("CASARS_IMEXPLORE_DISABLE_DIRECT_OVERLAY").is_none()
}

fn kitty_animation_movie_overlay_enabled() -> bool {
    std::env::var_os("CASARS_IMEXPLORE_ENABLE_KITTY_ANIMATION_OVERLAY").is_some()
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum KittyMovieOverlayMode {
    Disabled,
    SoftwareDirect,
    KittyAnimation,
}

fn kitty_movie_overlay_mode(capabilities: &TerminalCapabilities) -> KittyMovieOverlayMode {
    if !capabilities.direct_kitty_layers {
        return KittyMovieOverlayMode::Disabled;
    }
    if capabilities.direct_kitty_animations && kitty_animation_movie_overlay_enabled() {
        return KittyMovieOverlayMode::KittyAnimation;
    }
    if software_direct_movie_overlay_enabled() {
        return KittyMovieOverlayMode::SoftwareDirect;
    }
    KittyMovieOverlayMode::Disabled
}

#[cfg(test)]
fn test_env_lock() -> std::sync::MutexGuard<'static, ()> {
    use std::sync::{Mutex, OnceLock};

    static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
    LOCK.get_or_init(|| Mutex::new(()))
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

enum RunOutcome {
    Quit,
    Launcher,
}

struct KittyMovieOverlay {
    mode: KittyMovieOverlayMode,
    manager: Option<KittyLayerManager>,
    software_store: Option<KittyStoredImageStore>,
    software_slot: Option<KittyPaneSlotId>,
    handle: Option<KittyLayerHandle>,
    software_current_image: Option<KittyStoredImageId>,
    active_movie_key: Option<u64>,
    active_axis: Option<usize>,
    active_axis_index: Option<usize>,
    active_canvas: Option<ratatui::layout::Rect>,
    uploaded_axis_indices: Vec<usize>,
    seen_axis_indices: Vec<bool>,
    active_fps: f64,
    seeding_started_at: Option<Instant>,
    looping_started_at: Option<Instant>,
    looping: bool,
}

impl KittyMovieOverlay {
    fn new() -> Result<Self, CasarsError> {
        let picker = Picker::from_query_stdio().unwrap_or_else(|_| Picker::halfblocks());
        let capabilities = TerminalCapabilities::from_picker(&picker);
        let mode = kitty_movie_overlay_mode(&capabilities);
        movie_debug_log(format!(
            "capabilities: panel={:?} direct_layers={} direct_animations={} overlay_mode={:?}",
            capabilities.panel_protocol,
            capabilities.direct_kitty_layers,
            capabilities.direct_kitty_animations,
            mode
        ));
        let (manager, software_store, software_slot, handle) = match mode {
            KittyMovieOverlayMode::KittyAnimation => {
                let mut manager = KittyLayerManager::with_starting_ids(
                    KITTY_MOVIE_OVERLAY_ID_BASE,
                    KITTY_MOVIE_OVERLAY_ID_BASE,
                )
                .map_err(map_kitty_error)?;
                let handle = manager.allocate().map_err(map_kitty_error)?;
                (Some(manager), None, None, Some(handle))
            }
            KittyMovieOverlayMode::SoftwareDirect => {
                let mut store = KittyStoredImageStore::with_starting_ids(
                    KITTY_MOVIE_OVERLAY_ID_BASE,
                    KITTY_MOVIE_OVERLAY_ID_BASE,
                )
                .map_err(map_kitty_error)?;
                let slot = store.allocate_slot().map_err(map_kitty_error)?;
                (None, Some(store), Some(slot), None)
            }
            KittyMovieOverlayMode::Disabled => (None, None, None, None),
        };
        Ok(Self {
            mode,
            manager,
            software_store,
            software_slot,
            handle,
            software_current_image: None,
            active_movie_key: None,
            active_axis: None,
            active_axis_index: None,
            active_canvas: None,
            uploaded_axis_indices: Vec::new(),
            seen_axis_indices: Vec::new(),
            active_fps: 0.0,
            seeding_started_at: None,
            looping_started_at: None,
            looping: false,
        })
    }

    fn refresh(
        &mut self,
        terminal: &mut Terminal<CrosstermBackend<Stdout>>,
        app: &mut AppState,
        layout: &ui::UiLayout,
    ) -> Result<(), CasarsError> {
        if self.mode == KittyMovieOverlayMode::Disabled {
            return Ok(());
        }
        if self.mode == KittyMovieOverlayMode::KittyAnimation && self.looping {
            if app.image_movie_terminal_looping_active() {
                self.refresh_looping_only(terminal.backend_mut(), app)?;
                return Ok(());
            }
            self.clear(terminal.backend_mut(), app, true)?;
            return Ok(());
        }
        let frame = app.current_direct_image_movie_frame(layout);
        if self.mode == KittyMovieOverlayMode::KittyAnimation {
            match frame {
                Some(frame) => self.refresh_for_frame(terminal.backend_mut(), app, frame)?,
                None => self.clear(terminal.backend_mut(), app, true)?,
            }
        } else {
            self.refresh_software_frame(terminal.backend_mut(), app, frame)?;
        }
        Ok(())
    }

    fn abandon_for_terminal_reset(&mut self, app: &mut AppState) {
        self.reset_state();
        app.set_image_movie_direct_overlay(false);
        app.set_image_movie_terminal_looping(false);
    }

    fn clear(
        &mut self,
        out: &mut CrosstermBackend<Stdout>,
        app: &mut AppState,
        sync_current_frame: bool,
    ) -> Result<(), CasarsError> {
        if self.active_movie_key.is_none() {
            return Ok(());
        }
        if sync_current_frame
            && self.looping
            && let (Some(axis), Some(axis_index)) = (
                self.active_axis,
                self.estimated_current_axis_index(Instant::now()),
            )
        {
            app.sync_image_non_display_axis_index(axis, axis_index);
        }
        app.set_image_movie_direct_overlay(false);
        app.set_image_movie_terminal_looping(false);
        if let Some(manager) = &self.manager {
            if self.mode == KittyMovieOverlayMode::KittyAnimation {
                if let Some(handle) = self.handle {
                    manager
                        .clear_and_delete(out, handle)
                        .map_err(map_kitty_error)?;
                }
            }
        }
        if let Some(store) = &mut self.software_store {
            if let Some(slot) = self.software_slot {
                store.clear_slot(out, slot).map_err(map_kitty_error)?;
            }
            if let Some(image) = self.software_current_image.take() {
                store.delete_image(out, image).map_err(map_kitty_error)?;
            }
        }
        movie_debug_log(format!(
            "clear overlay sync_current_frame={} looping={}",
            sync_current_frame, self.looping
        ));
        self.reset_state();
        Ok(())
    }

    fn hide_visible(
        &mut self,
        out: &mut CrosstermBackend<Stdout>,
        app: &mut AppState,
    ) -> Result<(), CasarsError> {
        if self.active_movie_key.is_none() {
            return Ok(());
        }
        app.set_image_movie_direct_overlay(false);
        app.set_image_movie_terminal_looping(false);
        if self.mode == KittyMovieOverlayMode::KittyAnimation {
            if let (Some(manager), Some(handle)) = (&self.manager, self.handle) {
                manager
                    .clear_placement(out, handle)
                    .map_err(map_kitty_error)?;
            }
        } else if let Some(store) = &mut self.software_store
            && let Some(slot) = self.software_slot
        {
            store.clear_slot(out, slot).map_err(map_kitty_error)?;
        }
        movie_debug_log("hide overlay for immediate exit");
        self.reset_state();
        Ok(())
    }

    fn refresh_for_frame(
        &mut self,
        out: &mut CrosstermBackend<Stdout>,
        app: &mut AppState,
        frame: crate::app::ImageDirectMovieFrame,
    ) -> Result<(), CasarsError> {
        if self.active_movie_key != Some(frame.movie_key) {
            self.clear(out, app, false)?;
            self.active_movie_key = Some(frame.movie_key);
            self.active_axis = Some(frame.axis);
            self.active_axis_index = Some(frame.axis_index);
            self.active_canvas = Some(frame.canvas);
            self.seen_axis_indices = vec![false; frame.axis_length];
            self.uploaded_axis_indices.clear();
            self.active_fps = frame.fps;
            self.seeding_started_at = Some(Instant::now());
            movie_debug_log(format!(
                "start movie key={} axis={} len={} fps={}",
                frame.movie_key, frame.axis, frame.axis_length, frame.fps
            ));
        }
        app.set_image_movie_direct_overlay(true);

        if self.active_canvas != Some(frame.canvas) {
            self.active_canvas = Some(frame.canvas);
            if let (Some(manager), Some(handle)) = (&self.manager, self.handle)
                && !self.uploaded_axis_indices.is_empty()
            {
                manager
                    .place(
                        out,
                        handle,
                        KittyPlacement {
                            rect: frame.canvas,
                            z_index: 384,
                            preserve_cursor: true,
                        },
                    )
                    .map_err(map_kitty_error)?;
                app.note_image_plane_direct_presented(frame.render_request_key_hash);
            }
        }

        if self.looping {
            self.active_axis_index = Some(frame.axis_index);
            if let (Some(manager), Some(handle)) = (&self.manager, self.handle) {
                manager
                    .place(
                        out,
                        handle,
                        KittyPlacement {
                            rect: frame.canvas,
                            z_index: 384,
                            preserve_cursor: true,
                        },
                    )
                    .map_err(map_kitty_error)?;
                app.note_image_plane_direct_presented(frame.render_request_key_hash);
            }
            if (self.active_fps - frame.fps).abs() > f64::EPSILON {
                self.control_animation(out, KittyAnimationPlaybackState::Looping, frame.fps)?;
                self.active_fps = frame.fps;
                self.looping_started_at = Some(Instant::now());
            }
            return Ok(());
        }

        let Some(manager) = &self.manager else {
            return Ok(());
        };
        let Some(handle) = self.handle else {
            return Ok(());
        };

        if !self
            .seen_axis_indices
            .get(frame.axis_index)
            .copied()
            .unwrap_or(false)
        {
            if self.uploaded_axis_indices.is_empty() {
                manager
                    .upload_and_place_rgba(
                        out,
                        handle,
                        &frame.rendered_image,
                        KittyPlacement {
                            rect: frame.canvas,
                            z_index: 384,
                            preserve_cursor: true,
                        },
                    )
                    .map_err(map_kitty_error)?;
                manager
                    .control_animation(
                        out,
                        handle,
                        KittyAnimationControl {
                            state: Some(KittyAnimationPlaybackState::Loading),
                            current_frame: Some(first_movie_frame()),
                            frame_number: Some(first_movie_frame()),
                            gap: Some(movie_gap(frame.fps)),
                            loops: Some(loop_forever()),
                        },
                    )
                    .map_err(map_kitty_error)?;
                app.note_image_plane_direct_presented(frame.render_request_key_hash);
            } else {
                manager
                    .append_animation_frame_rgba(
                        out,
                        handle,
                        &frame.rendered_image,
                        Some(movie_gap(frame.fps)),
                    )
                    .map_err(map_kitty_error)?;
                app.note_image_plane_direct_presented(frame.render_request_key_hash);
            }
            if let Some(seen) = self.seen_axis_indices.get_mut(frame.axis_index) {
                *seen = true;
            }
            self.uploaded_axis_indices.push(frame.axis_index);
            self.active_axis_index = Some(frame.axis_index);
            movie_debug_log(format!(
                "seed frame axis_index={} count={}/{}",
                frame.axis_index,
                self.uploaded_axis_indices.len(),
                frame.axis_length
            ));
        }

        if self.uploaded_axis_indices.len() == frame.axis_length {
            self.control_animation_with(
                out,
                KittyAnimationControl {
                    state: Some(KittyAnimationPlaybackState::Looping),
                    current_frame: movie_frame_number(self.uploaded_axis_indices.len()),
                    gap: Some(movie_gap(frame.fps)),
                    loops: Some(loop_forever()),
                    ..KittyAnimationControl::default()
                },
            )?;
            self.active_fps = frame.fps;
            self.looping_started_at = Some(Instant::now());
            self.looping = true;
            app.set_image_movie_terminal_looping(true);
            let seeding_elapsed = self
                .seeding_started_at
                .map(|start| start.elapsed())
                .unwrap_or_default();
            let seeding_fps = if seeding_elapsed.is_zero() {
                0.0
            } else {
                frame.axis_length as f64 / seeding_elapsed.as_secs_f64()
            };
            movie_debug_log(format!(
                "terminal looping enabled axis={} frames={} fps={} seeding_ms={} seed_fps={:.2}",
                frame.axis,
                frame.axis_length,
                frame.fps,
                seeding_elapsed.as_millis(),
                seeding_fps
            ));
        }

        Ok(())
    }

    fn refresh_software_frame(
        &mut self,
        out: &mut CrosstermBackend<Stdout>,
        app: &mut AppState,
        frame: Option<crate::app::ImageDirectMovieFrame>,
    ) -> Result<(), CasarsError> {
        let Some(frame) = frame else {
            if app.image_movie_active() {
                return Ok(());
            }
            self.clear(out, app, true)?;
            return Ok(());
        };
        let movie_changed =
            self.active_movie_key != Some(frame.movie_key) || self.active_axis != Some(frame.axis);
        if movie_changed {
            self.clear(out, app, false)?;
            self.active_movie_key = Some(frame.movie_key);
            self.active_axis = Some(frame.axis);
            self.active_canvas = Some(frame.canvas);
            self.active_fps = frame.fps;
        }

        let placement = KittyPlacement {
            rect: frame.canvas,
            z_index: 384,
            preserve_cursor: true,
        };
        let Some(store) = self.software_store.as_mut() else {
            return Ok(());
        };
        let Some(slot) = self.software_slot else {
            return Ok(());
        };
        let (image, info) = store
            .store_rgba(out, &frame.rendered_image)
            .map_err(map_kitty_error)?;
        store
            .place_in_slot(out, slot, image, placement)
            .map_err(map_kitty_error)?;
        if let Some(previous) = self.software_current_image.replace(image) {
            store.delete_image(out, previous).map_err(map_kitty_error)?;
        }
        movie_debug_log(format!(
            "software upload axis_index={} image={} bytes={} total_store_bytes={}",
            frame.axis_index,
            image.raw(),
            info.bytes,
            store.total_bytes()
        ));
        app.note_image_plane_direct_presented(frame.render_request_key_hash);

        self.active_movie_key = Some(frame.movie_key);
        self.active_axis = Some(frame.axis);
        self.active_axis_index = Some(frame.axis_index);
        self.active_canvas = Some(frame.canvas);
        self.active_fps = frame.fps;
        app.set_image_movie_direct_overlay(true);
        Ok(())
    }

    fn refresh_looping_only(
        &mut self,
        out: &mut CrosstermBackend<Stdout>,
        app: &mut AppState,
    ) -> Result<(), CasarsError> {
        if let Some(fps) = app.image_movie_fps()
            && (self.active_fps - fps).abs() > f64::EPSILON
        {
            self.control_animation(out, KittyAnimationPlaybackState::Looping, fps)?;
            self.active_fps = fps;
            self.looping_started_at = Some(Instant::now());
        }
        Ok(())
    }

    fn control_animation(
        &self,
        out: &mut CrosstermBackend<Stdout>,
        state: KittyAnimationPlaybackState,
        fps: f64,
    ) -> Result<(), CasarsError> {
        self.control_animation_with(
            out,
            KittyAnimationControl {
                state: Some(state),
                gap: Some(movie_gap(fps)),
                loops: Some(loop_forever()),
                ..KittyAnimationControl::default()
            },
        )
    }

    fn control_animation_with(
        &self,
        out: &mut CrosstermBackend<Stdout>,
        control: KittyAnimationControl,
    ) -> Result<(), CasarsError> {
        let Some(manager) = &self.manager else {
            return Ok(());
        };
        let Some(handle) = self.handle else {
            return Ok(());
        };
        manager
            .control_animation(out, handle, control)
            .map_err(map_kitty_error)
    }

    fn estimated_current_axis_index(&self, now: Instant) -> Option<usize> {
        if !self.looping || self.uploaded_axis_indices.is_empty() {
            return None;
        }
        let start = self.looping_started_at?;
        let elapsed = now.saturating_duration_since(start).as_secs_f64();
        let frame_offset = (elapsed * self.active_fps).floor() as usize;
        self.uploaded_axis_indices
            .get(frame_offset % self.uploaded_axis_indices.len())
            .copied()
    }

    fn reset_state(&mut self) {
        self.active_movie_key = None;
        self.active_axis = None;
        self.active_axis_index = None;
        self.active_canvas = None;
        self.software_current_image = None;
        self.uploaded_axis_indices.clear();
        self.seen_axis_indices.clear();
        self.active_fps = 0.0;
        self.seeding_started_at = None;
        self.looping_started_at = None;
        self.looping = false;
    }
}

fn movie_gap(fps: f64) -> KittyAnimationGap {
    KittyAnimationGap::Timed(Duration::from_secs_f64(1.0 / fps.max(0.001)))
}

fn loop_forever() -> NonZeroU32 {
    NonZeroU32::new(1).expect("kitty loop count")
}

fn first_movie_frame() -> NonZeroU32 {
    movie_frame_number(1).expect("first kitty movie frame")
}

fn movie_frame_number(frame: usize) -> Option<NonZeroU32> {
    u32::try_from(frame).ok().and_then(NonZeroU32::new)
}

fn map_kitty_error(error: ratatui_graphics::KittyLayerError) -> CasarsError {
    CasarsError::TerminalSetup(io::Error::other(error.to_string()))
}

pub(crate) fn movie_debug_log(message: impl AsRef<str>) {
    if std::env::var_os("CASARS_MOVIE_DEBUG").is_none() {
        return;
    }
    static STARTED_AT: OnceLock<Instant> = OnceLock::new();
    let started_at = STARTED_AT.get_or_init(Instant::now);
    let Ok(mut file) = OpenOptions::new()
        .create(true)
        .append(true)
        .open("/tmp/casars-imexplore-movie.log")
    else {
        return;
    };
    let _ = writeln!(
        file,
        "[+{:>7} ms] {}",
        started_at.elapsed().as_millis(),
        message.as_ref()
    );
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
    let mut kitty_movie_overlay = KittyMovieOverlay::new()?;

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
            provisional_layout.form_inner.height,
        );
        let layout = ui::compute_layout(
            ratatui::layout::Rect::new(0, 0, area.width, area.height),
            &app,
        );
        app.cache_output_layout(&layout);
        if let Some(outcome) = drain_runtime_events(
            terminal,
            &mut app,
            &layout,
            &mut kitty_movie_overlay,
            &mut last_tick,
        )? {
            return Ok(outcome);
        }
        app.prepare_graphics(&layout);
        terminal
            .terminal
            .draw(|frame| ui::draw(frame, &app, &layout))
            .map_err(CasarsError::TerminalSetup)?;
        app.note_image_plane_presented();
        kitty_movie_overlay.refresh(&mut terminal.terminal, &mut app, &layout)?;

        app.drain_execution_events();
        if app.should_quit() {
            kitty_movie_overlay.clear(terminal.terminal.backend_mut(), &mut app, false)?;
            return Ok(RunOutcome::Quit);
        }
        if app.should_return_to_launcher() {
            kitty_movie_overlay.clear(terminal.terminal.backend_mut(), &mut app, false)?;
            return Ok(RunOutcome::Launcher);
        }

        let tick_rate = app.preferred_tick_rate();
        let timeout = tick_rate.saturating_sub(last_tick.elapsed());
        if event::poll(timeout).map_err(CasarsError::TerminalSetup)? {
            let event = event::read().map_err(CasarsError::TerminalSetup)?;
            if let Some(outcome) = handle_runtime_event(
                terminal,
                &mut app,
                &layout,
                &mut kitty_movie_overlay,
                &mut last_tick,
                event,
            )? {
                return Ok(outcome);
            }
            if let Some(outcome) = drain_runtime_events(
                terminal,
                &mut app,
                &layout,
                &mut kitty_movie_overlay,
                &mut last_tick,
            )? {
                return Ok(outcome);
            }
        }

        if last_tick.elapsed() >= tick_rate {
            app.on_tick();
            last_tick = Instant::now();
        }
    }
}

fn drain_runtime_events(
    terminal: &mut TerminalGuard,
    app: &mut AppState,
    layout: &ui::UiLayout,
    kitty_movie_overlay: &mut KittyMovieOverlay,
    last_tick: &mut Instant,
) -> Result<Option<RunOutcome>, CasarsError> {
    while event::poll(Duration::ZERO).map_err(CasarsError::TerminalSetup)? {
        let event = event::read().map_err(CasarsError::TerminalSetup)?;
        if let Some(outcome) =
            handle_runtime_event(terminal, app, layout, kitty_movie_overlay, last_tick, event)?
        {
            return Ok(Some(outcome));
        }
    }
    Ok(None)
}

fn handle_runtime_event(
    terminal: &mut TerminalGuard,
    app: &mut AppState,
    layout: &ui::UiLayout,
    kitty_movie_overlay: &mut KittyMovieOverlay,
    last_tick: &mut Instant,
    event: Event,
) -> Result<Option<RunOutcome>, CasarsError> {
    match event {
        Event::Key(key_event) => {
            if is_suspend_key(key_event) {
                kitty_movie_overlay.abandon_for_terminal_reset(app);
                terminal.suspend_and_resume()?;
                *last_tick = Instant::now();
                return Ok(None);
            }
            app.handle_key_event(key_event);
        }
        Event::Paste(text) => app.handle_paste(text),
        Event::Mouse(mouse_event) => app.handle_mouse_event(mouse_event, layout),
        _ => {}
    }
    if app.should_quit() {
        kitty_movie_overlay.hide_visible(terminal.terminal.backend_mut(), app)?;
        return Ok(Some(RunOutcome::Quit));
    }
    if app.should_return_to_launcher() {
        kitty_movie_overlay.hide_visible(terminal.terminal.backend_mut(), app)?;
        return Ok(Some(RunOutcome::Launcher));
    }
    Ok(None)
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

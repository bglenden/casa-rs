// SPDX-License-Identifier: LGPL-3.0-or-later
//! Shared staged movie benchmark support.
#![allow(dead_code)]

use std::error::Error;
use std::fmt;
use std::fs::{self, File};
use std::io::{BufWriter, Write};
use std::io::{Stdout, stdout};
use std::num::NonZeroU32;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::mpsc::{self, Receiver, Sender, TryRecvError};
use std::thread;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use casacore_imagebrowser_protocol::{
    ImageBrowserParameters, ImageBrowserPreviewRequest, ImageBrowserSnapshot, ImageBrowserViewport,
    ImagePlaneContentMode,
};
use casacore_images::ImageBrowserSession;
use casars::movie_stage_support::{
    OrderedReadyBuffer, PlanePaneRenderOptions, QueueDepthTracker, ReadyInsertResult,
    WorkerActivity, render_plane_pane_from_snapshot,
};
use crossterm::cursor::{Hide, Show};
use crossterm::event::{self, Event, KeyCode, KeyEventKind, KeyModifiers};
use crossterm::execute;
use crossterm::terminal::{
    self, Clear, ClearType, EnterAlternateScreen, LeaveAlternateScreen, disable_raw_mode,
    enable_raw_mode,
};
use image::imageops::{FilterType, overlay};
use image::{DynamicImage, Rgba, RgbaImage};
use ratatui::{
    Terminal,
    backend::CrosstermBackend,
    layout::{Constraint, Direction, Layout, Rect},
    style::{Color, Style},
    widgets::{Block, Borders, Paragraph},
};
use ratatui_graphics::{
    KittyLayerHandle, KittyLayerManager, KittyPlacement, PanelProtocol, Picker, Resize,
    TerminalCapabilities, build_panel_protocol_from_rgba_owned,
};
use ratatui_image::Image as PanelImage;
use serde::Serialize;

const DEFAULT_IMAGE_PATH: &str = "/Volumes/home/casatestdata/unittest/imval/n4826_bima.im";
const DEFAULT_OUTPUT_DIR: &str = "/tmp/imexplore-movie-stages";
const STAGE1_NAME: &str = "stage1";
const STAGE2_NAME: &str = "stage2";
const STAGE3_NAME: &str = "stage3";
const DEFAULT_VIEWPORT_WIDTH_CELLS: u16 = 160;
const DEFAULT_VIEWPORT_HEIGHT_CELLS: u16 = 48;
const DEFAULT_BITMAP_WIDTH: u16 = 1320;
const DEFAULT_BITMAP_HEIGHT: u16 = 588;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Stage1Mode {
    RenderOnly,
    PreviewRender,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Stage2Mode {
    UploadEachFrame,
    PreloadThenPlace,
}

impl Stage2Mode {
    fn as_str(self) -> &'static str {
        match self {
            Self::UploadEachFrame => "upload-each-frame",
            Self::PreloadThenPlace => "preload-then-place",
        }
    }
}

impl fmt::Display for Stage2Mode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl Stage1Mode {
    fn as_str(self) -> &'static str {
        match self {
            Self::RenderOnly => "render-only",
            Self::PreviewRender => "preview-render",
        }
    }
}

impl fmt::Display for Stage1Mode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum PhaseKind {
    Warmup,
    Max,
    Sustained,
}

impl PhaseKind {
    fn as_str(self) -> &'static str {
        match self {
            Self::Warmup => "warmup",
            Self::Max => "max",
            Self::Sustained => "sustained",
        }
    }
}

#[derive(Debug, Clone)]
pub struct Stage1Config {
    pub mode: Stage1Mode,
    pub image_path: PathBuf,
    pub output_dir: PathBuf,
    pub target_fps: f64,
    pub warmup_loops: usize,
    pub measure_loops: usize,
    pub preview_workers: usize,
    pub render_workers: usize,
    pub ready_buffer: usize,
    pub bitmap_width: u16,
    pub bitmap_height: u16,
    pub axis_position: usize,
    pub save_first_frame: Option<PathBuf>,
    pub save_contact_sheet: Option<PathBuf>,
}

#[derive(Debug, Clone)]
pub struct Stage2Config {
    pub mode: Stage2Mode,
    pub image_path: PathBuf,
    pub output_dir: PathBuf,
    pub target_fps: f64,
    pub warmup_loops: usize,
    pub measure_loops: usize,
    pub preview_workers: usize,
    pub render_workers: usize,
    pub ready_buffer: usize,
    pub bitmap_width: u16,
    pub bitmap_height: u16,
    pub axis_position: usize,
}

#[derive(Debug, Clone)]
pub struct Stage3Config {
    pub image_path: PathBuf,
    pub output_dir: PathBuf,
    pub target_fps: f64,
    pub warmup_loops: usize,
    pub measure_loops: usize,
    pub preview_workers: usize,
    pub render_workers: usize,
    pub ready_buffer: usize,
    pub bitmap_width: u16,
    pub bitmap_height: u16,
    pub axis_position: usize,
}

impl Default for Stage1Config {
    fn default() -> Self {
        Self {
            mode: Stage1Mode::PreviewRender,
            image_path: PathBuf::from(DEFAULT_IMAGE_PATH),
            output_dir: PathBuf::from(DEFAULT_OUTPUT_DIR),
            target_fps: 30.0,
            warmup_loops: 1,
            measure_loops: 3,
            preview_workers: 2,
            render_workers: 4,
            ready_buffer: 32,
            bitmap_width: DEFAULT_BITMAP_WIDTH,
            bitmap_height: DEFAULT_BITMAP_HEIGHT,
            axis_position: 0,
            save_first_frame: None,
            save_contact_sheet: None,
        }
    }
}

impl Stage1Config {
    pub fn parse_from_env_args() -> Result<Self, Box<dyn Error>> {
        Self::parse_from_iter(std::env::args().skip(1))
    }

    pub fn parse_from_iter<I>(args: I) -> Result<Self, Box<dyn Error>>
    where
        I: IntoIterator<Item = String>,
    {
        let mut config = Self::default();
        let mut args = args.into_iter();
        while let Some(arg) = args.next() {
            match arg.as_str() {
                "--help" | "-h" => {
                    print_stage1_help();
                    std::process::exit(0);
                }
                "--mode" => {
                    let value = next_value(&mut args, "--mode")?;
                    config.mode = parse_mode(&value)?;
                }
                "--image" => {
                    config.image_path = PathBuf::from(next_value(&mut args, "--image")?);
                }
                "--output-dir" => {
                    config.output_dir = PathBuf::from(next_value(&mut args, "--output-dir")?);
                }
                "--target-fps" => {
                    config.target_fps =
                        parse_positive_f64(&next_value(&mut args, "--target-fps")?)?;
                }
                "--warmup-loops" => {
                    config.warmup_loops =
                        parse_positive_usize(&next_value(&mut args, "--warmup-loops")?)?;
                }
                "--measure-loops" => {
                    config.measure_loops =
                        parse_positive_usize(&next_value(&mut args, "--measure-loops")?)?;
                }
                "--preview-workers" => {
                    config.preview_workers =
                        parse_positive_usize(&next_value(&mut args, "--preview-workers")?)?;
                }
                "--render-workers" => {
                    config.render_workers =
                        parse_positive_usize(&next_value(&mut args, "--render-workers")?)?;
                }
                "--ready-buffer" => {
                    config.ready_buffer =
                        parse_positive_usize(&next_value(&mut args, "--ready-buffer")?)?;
                }
                "--bitmap-width" => {
                    config.bitmap_width =
                        parse_positive_u16(&next_value(&mut args, "--bitmap-width")?)?;
                }
                "--bitmap-height" => {
                    config.bitmap_height =
                        parse_positive_u16(&next_value(&mut args, "--bitmap-height")?)?;
                }
                "--axis-position" => {
                    config.axis_position = parse_usize(&next_value(&mut args, "--axis-position")?)?;
                }
                "--save-first-frame" => {
                    config.save_first_frame =
                        Some(PathBuf::from(next_value(&mut args, "--save-first-frame")?));
                }
                "--save-contact-sheet" => {
                    config.save_contact_sheet = Some(PathBuf::from(next_value(
                        &mut args,
                        "--save-contact-sheet",
                    )?));
                }
                other => {
                    return Err(format!("unknown option: {other}").into());
                }
            }
        }
        Ok(config)
    }

    fn viewport(&self) -> ImageBrowserViewport {
        ImageBrowserViewport::with_plane_pixels(
            DEFAULT_VIEWPORT_WIDTH_CELLS,
            DEFAULT_VIEWPORT_HEIGHT_CELLS,
            0,
            self.bitmap_width,
            self.bitmap_height,
        )
    }
}

impl Default for Stage2Config {
    fn default() -> Self {
        Self {
            mode: Stage2Mode::UploadEachFrame,
            image_path: PathBuf::from(DEFAULT_IMAGE_PATH),
            output_dir: PathBuf::from(DEFAULT_OUTPUT_DIR),
            target_fps: 30.0,
            warmup_loops: 1,
            measure_loops: 3,
            preview_workers: 2,
            render_workers: 4,
            ready_buffer: 32,
            bitmap_width: DEFAULT_BITMAP_WIDTH,
            bitmap_height: DEFAULT_BITMAP_HEIGHT,
            axis_position: 0,
        }
    }
}

impl Stage2Config {
    pub fn parse_from_env_args() -> Result<Self, Box<dyn Error>> {
        Self::parse_from_iter(std::env::args().skip(1))
    }

    pub fn parse_from_iter<I>(args: I) -> Result<Self, Box<dyn Error>>
    where
        I: IntoIterator<Item = String>,
    {
        let mut config = Self::default();
        let mut args = args.into_iter();
        while let Some(arg) = args.next() {
            match arg.as_str() {
                "--help" | "-h" => {
                    print_stage2_help();
                    std::process::exit(0);
                }
                "--mode" => {
                    config.mode = parse_stage2_mode(&next_value(&mut args, "--mode")?)?;
                }
                "--image" => {
                    config.image_path = PathBuf::from(next_value(&mut args, "--image")?);
                }
                "--output-dir" => {
                    config.output_dir = PathBuf::from(next_value(&mut args, "--output-dir")?);
                }
                "--target-fps" => {
                    config.target_fps =
                        parse_positive_f64(&next_value(&mut args, "--target-fps")?)?;
                }
                "--warmup-loops" => {
                    config.warmup_loops =
                        parse_positive_usize(&next_value(&mut args, "--warmup-loops")?)?;
                }
                "--measure-loops" => {
                    config.measure_loops =
                        parse_positive_usize(&next_value(&mut args, "--measure-loops")?)?;
                }
                "--preview-workers" => {
                    config.preview_workers =
                        parse_positive_usize(&next_value(&mut args, "--preview-workers")?)?;
                }
                "--render-workers" => {
                    config.render_workers =
                        parse_positive_usize(&next_value(&mut args, "--render-workers")?)?;
                }
                "--ready-buffer" => {
                    config.ready_buffer =
                        parse_positive_usize(&next_value(&mut args, "--ready-buffer")?)?;
                }
                "--bitmap-width" => {
                    config.bitmap_width =
                        parse_positive_u16(&next_value(&mut args, "--bitmap-width")?)?;
                }
                "--bitmap-height" => {
                    config.bitmap_height =
                        parse_positive_u16(&next_value(&mut args, "--bitmap-height")?)?;
                }
                "--axis-position" => {
                    config.axis_position = parse_usize(&next_value(&mut args, "--axis-position")?)?;
                }
                other => return Err(format!("unknown option: {other}").into()),
            }
        }
        Ok(config)
    }

    fn viewport(&self) -> ImageBrowserViewport {
        ImageBrowserViewport::with_plane_pixels(
            DEFAULT_VIEWPORT_WIDTH_CELLS,
            DEFAULT_VIEWPORT_HEIGHT_CELLS,
            0,
            self.bitmap_width,
            self.bitmap_height,
        )
    }
}

impl Default for Stage3Config {
    fn default() -> Self {
        Self {
            image_path: PathBuf::from(DEFAULT_IMAGE_PATH),
            output_dir: PathBuf::from(DEFAULT_OUTPUT_DIR),
            target_fps: 30.0,
            warmup_loops: 1,
            measure_loops: 3,
            preview_workers: 2,
            render_workers: 4,
            ready_buffer: 32,
            bitmap_width: DEFAULT_BITMAP_WIDTH,
            bitmap_height: DEFAULT_BITMAP_HEIGHT,
            axis_position: 0,
        }
    }
}

impl Stage3Config {
    pub fn parse_from_env_args() -> Result<Self, Box<dyn Error>> {
        Self::parse_from_iter(std::env::args().skip(1))
    }

    pub fn parse_from_iter<I>(args: I) -> Result<Self, Box<dyn Error>>
    where
        I: IntoIterator<Item = String>,
    {
        let mut config = Self::default();
        let mut args = args.into_iter();
        while let Some(arg) = args.next() {
            match arg.as_str() {
                "--help" | "-h" => {
                    print_stage3_help();
                    std::process::exit(0);
                }
                "--image" => {
                    config.image_path = PathBuf::from(next_value(&mut args, "--image")?);
                }
                "--output-dir" => {
                    config.output_dir = PathBuf::from(next_value(&mut args, "--output-dir")?);
                }
                "--target-fps" => {
                    config.target_fps =
                        parse_positive_f64(&next_value(&mut args, "--target-fps")?)?;
                }
                "--warmup-loops" => {
                    config.warmup_loops =
                        parse_positive_usize(&next_value(&mut args, "--warmup-loops")?)?;
                }
                "--measure-loops" => {
                    config.measure_loops =
                        parse_positive_usize(&next_value(&mut args, "--measure-loops")?)?;
                }
                "--preview-workers" => {
                    config.preview_workers =
                        parse_positive_usize(&next_value(&mut args, "--preview-workers")?)?;
                }
                "--render-workers" => {
                    config.render_workers =
                        parse_positive_usize(&next_value(&mut args, "--render-workers")?)?;
                }
                "--ready-buffer" => {
                    config.ready_buffer =
                        parse_positive_usize(&next_value(&mut args, "--ready-buffer")?)?;
                }
                "--bitmap-width" => {
                    config.bitmap_width =
                        parse_positive_u16(&next_value(&mut args, "--bitmap-width")?)?;
                }
                "--bitmap-height" => {
                    config.bitmap_height =
                        parse_positive_u16(&next_value(&mut args, "--bitmap-height")?)?;
                }
                "--axis-position" => {
                    config.axis_position = parse_usize(&next_value(&mut args, "--axis-position")?)?;
                }
                other => return Err(format!("unknown option: {other}").into()),
            }
        }
        Ok(config)
    }

    fn viewport(&self) -> ImageBrowserViewport {
        ImageBrowserViewport::with_plane_pixels(
            DEFAULT_VIEWPORT_WIDTH_CELLS,
            DEFAULT_VIEWPORT_HEIGHT_CELLS,
            0,
            self.bitmap_width,
            self.bitmap_height,
        )
    }
}

struct Stage2TerminalSession {
    stdout: Stdout,
}

impl Stage2TerminalSession {
    fn enter() -> Result<Self, Box<dyn Error>> {
        enable_raw_mode()?;
        let mut stdout = stdout();
        execute!(stdout, EnterAlternateScreen, Hide, Clear(ClearType::All))?;
        Ok(Self { stdout })
    }

    fn stdout_mut(&mut self) -> &mut Stdout {
        &mut self.stdout
    }

    fn placement_rect(&self) -> Result<Rect, Box<dyn Error>> {
        let (cols, rows) = terminal::size()?;
        Ok(Rect::new(0, 0, cols.max(1), rows.max(1)))
    }
}

impl Drop for Stage2TerminalSession {
    fn drop(&mut self) {
        let _ = execute!(
            self.stdout,
            Show,
            Clear(ClearType::All),
            LeaveAlternateScreen
        );
        let _ = disable_raw_mode();
    }
}

struct Stage2KittyPresenter {
    mode: Stage2Mode,
    manager: KittyLayerManager,
    shared_placement_id: NonZeroU32,
    cached_handles: Vec<Option<KittyLayerHandle>>,
    current_handle: Option<KittyLayerHandle>,
}

impl Stage2KittyPresenter {
    fn new(frame_count: usize) -> Result<Self, Box<dyn Error>> {
        let picker = Picker::from_query_stdio().unwrap_or_else(|_| Picker::halfblocks());
        let capabilities = TerminalCapabilities::from_picker(&picker);
        if !capabilities.direct_kitty_layers {
            return Err("terminal does not support direct Kitty layers".into());
        }
        let mut manager = KittyLayerManager::new();
        let shared_placement_id = manager.allocate_placement_id()?;
        Ok(Self {
            mode: Stage2Mode::UploadEachFrame,
            manager,
            shared_placement_id,
            cached_handles: vec![None; frame_count],
            current_handle: None,
        })
    }

    fn set_mode(&mut self, mode: Stage2Mode) {
        self.mode = mode;
    }

    fn present(
        &mut self,
        out: &mut Stdout,
        occurrence_index: usize,
        image: &DynamicImage,
        rect: Rect,
    ) -> Result<u64, Box<dyn Error>> {
        let start = Instant::now();
        let placement = KittyPlacement {
            rect,
            z_index: 384,
            preserve_cursor: true,
        };
        match self.mode {
            Stage2Mode::UploadEachFrame => {
                let image_id = self.manager.allocate_image_id()?;
                let handle = KittyLayerHandle::new(image_id, self.shared_placement_id);
                self.manager
                    .upload_and_place_rgba(out, handle, &image.to_rgba8(), placement)?;
                if let Some(previous) = self.current_handle.replace(handle) {
                    self.manager.delete_image(out, previous)?;
                }
            }
            Stage2Mode::PreloadThenPlace => {
                let handle = if let Some(handle) = self.cached_handles[occurrence_index] {
                    handle
                } else {
                    let image_id = self.manager.allocate_image_id()?;
                    let handle = KittyLayerHandle::new(image_id, self.shared_placement_id);
                    self.manager.upload_rgba(out, handle, &image.to_rgba8())?;
                    self.cached_handles[occurrence_index] = Some(handle);
                    handle
                };
                self.manager.place(out, handle, placement)?;
                self.current_handle = Some(handle);
            }
        }
        Ok(start.elapsed().as_nanos() as u64)
    }

    fn cleanup(&mut self, out: &mut Stdout) -> Result<(), Box<dyn Error>> {
        let current_is_cached = self.current_handle.is_some_and(|current| {
            self.cached_handles
                .iter()
                .flatten()
                .any(|cached| cached.image_id() == current.image_id())
        });
        if let Some((min_id, max_id)) = self
            .cached_handles
            .iter()
            .flatten()
            .map(|handle| handle.image_id())
            .fold(None, |acc, image_id| match acc {
                None => Some((image_id, image_id)),
                Some((min_id, max_id)) => Some((min_id.min(image_id), max_id.max(image_id))),
            })
        {
            self.manager.delete_image_range(out, min_id, max_id)?;
        }
        if let Some(current) = self.current_handle.take() {
            if !current_is_cached {
                let _ = self.manager.delete_image(out, current);
            }
        }
        Ok(())
    }
}

struct Stage3TerminalSession {
    terminal: Terminal<CrosstermBackend<Stdout>>,
    picker: Picker,
    current_protocol: Option<PanelProtocol>,
    status_line: String,
}

impl Stage3TerminalSession {
    fn enter() -> Result<Self, Box<dyn Error>> {
        enable_raw_mode()?;
        let mut stdout = stdout();
        execute!(stdout, EnterAlternateScreen, Hide, Clear(ClearType::All))?;
        let backend = CrosstermBackend::new(stdout);
        let mut terminal = Terminal::new(backend)?;
        terminal.hide_cursor()?;
        let picker = Picker::from_query_stdio().unwrap_or_else(|_| Picker::halfblocks());
        let mut session = Self {
            terminal,
            picker,
            current_protocol: None,
            status_line: "Waiting for first frame...".to_string(),
        };
        session.draw_frame()?;
        Ok(session)
    }

    fn panel_area(&self) -> Result<Rect, Box<dyn Error>> {
        let size = self.terminal.size()?;
        let layout = Layout::default()
            .direction(Direction::Vertical)
            .constraints([Constraint::Min(4), Constraint::Length(3)])
            .split(size.into());
        let block = Block::default()
            .borders(Borders::ALL)
            .title("Stage 3 Plane Pane")
            .border_style(Style::default().fg(Color::Green));
        Ok(block.inner(layout[0]))
    }

    fn font_pixels(&self) -> (u16, u16) {
        self.picker.font_size()
    }

    fn present(
        &mut self,
        occurrence_index: usize,
        image: &DynamicImage,
        phase_kind: PhaseKind,
    ) -> Result<u64, Box<dyn Error>> {
        let panel_area = self.panel_area()?;
        if panel_area.is_empty() {
            return Ok(0);
        }
        let rgba = image.to_rgba8();
        let present_started = Instant::now();
        let prepared = build_panel_protocol_from_rgba_owned(
            &self.picker,
            Resize::Fit(None),
            panel_area,
            rgba,
        )?;
        self.current_protocol = Some(prepared.protocol);
        self.status_line = format!(
            "phase={} occurrence={} protocol={:?} font={}x{}",
            phase_kind.as_str(),
            occurrence_index,
            self.picker.protocol_type(),
            self.font_pixels().0,
            self.font_pixels().1,
        );
        self.draw_frame()?;
        Ok(present_started.elapsed().as_nanos() as u64)
    }

    fn draw_frame(&mut self) -> Result<(), Box<dyn Error>> {
        let status = self.status_line.clone();
        let protocol = self.current_protocol.as_ref();
        self.terminal.draw(|frame| {
            let layout = Layout::default()
                .direction(Direction::Vertical)
                .constraints([Constraint::Min(4), Constraint::Length(3)])
                .split(frame.area());

            let panel_block = Block::default()
                .borders(Borders::ALL)
                .title("Stage 3 Plane Pane")
                .border_style(Style::default().fg(Color::Green));
            let inner = panel_block.inner(layout[0]);
            frame.render_widget(panel_block, layout[0]);
            if !inner.is_empty() {
                if let Some(protocol) = protocol {
                    frame.render_widget(PanelImage::new(protocol), inner);
                } else {
                    frame.render_widget(
                        Paragraph::new("Waiting for first frame...")
                            .style(Style::default().fg(Color::DarkGray)),
                        inner,
                    );
                }
            }

            let footer = Paragraph::new(status.clone()).block(
                Block::default()
                    .borders(Borders::ALL)
                    .title("Status")
                    .border_style(Style::default().fg(Color::DarkGray)),
            );
            frame.render_widget(footer, layout[1]);
        })?;
        Ok(())
    }
}

impl Drop for Stage3TerminalSession {
    fn drop(&mut self) {
        let _ = disable_raw_mode();
        let _ = execute!(
            self.terminal.backend_mut(),
            Show,
            Clear(ClearType::All),
            LeaveAlternateScreen
        );
        let _ = self.terminal.show_cursor();
    }
}

#[derive(Debug, Clone)]
struct OccurrenceSpec {
    occurrence_index: usize,
    non_display_indices: Vec<usize>,
}

#[derive(Debug, Clone)]
struct TemplateContext {
    viewport: ImageBrowserViewport,
    parameters: ImageBrowserParameters,
    plane_content_mode: ImagePlaneContentMode,
    occurrences: Vec<OccurrenceSpec>,
}

#[derive(Debug, Clone)]
struct PrecomputedFrameInput {
    occurrence_index: usize,
    cache_result: String,
    snapshot: Arc<ImageBrowserSnapshot>,
}

#[derive(Debug)]
struct PreviewJob {
    sequence: u64,
    loop_index: usize,
    occurrence_index: usize,
    non_display_indices: Vec<usize>,
}

#[derive(Debug)]
struct RenderJob {
    sequence: u64,
    loop_index: usize,
    occurrence_index: usize,
    cache_result: String,
    preview_ns: u64,
    snapshot: Arc<ImageBrowserSnapshot>,
}

#[derive(Debug)]
struct PreviewResult {
    sequence: u64,
    loop_index: usize,
    occurrence_index: usize,
    cache_result: String,
    preview_ns: u64,
    snapshot: Arc<ImageBrowserSnapshot>,
}

#[derive(Debug)]
struct RenderResult {
    sequence: u64,
    loop_index: usize,
    occurrence_index: usize,
    cache_result: String,
    preview_ns: u64,
    render_ns: u64,
    image: DynamicImage,
}

#[derive(Debug, Clone, Serialize)]
struct FrameEvent {
    kind: &'static str,
    stage: &'static str,
    mode: String,
    phase: String,
    sequence: u64,
    loop_index: usize,
    occurrence_index: usize,
    preview_ns: u64,
    render_ns: u64,
    present_ns: u64,
    ready_buffer_size: usize,
    preview_active_workers: usize,
    render_active_workers: usize,
    preview_queue_depth: usize,
    render_queue_depth: usize,
    cache_result: String,
    achieved_fps: f64,
    stale_count: u64,
    dropped_count: u64,
    late: bool,
}

#[derive(Debug, Clone, Serialize)]
struct SummaryEvent {
    kind: &'static str,
    stage: &'static str,
    mode: String,
    phase: String,
    frame_count: usize,
    target_fps: f64,
    achieved_fps: f64,
    preview_p50_ns: u64,
    preview_p95_ns: u64,
    render_p50_ns: u64,
    render_p95_ns: u64,
    present_p50_ns: u64,
    present_p95_ns: u64,
    ready_buffer_max: usize,
    preview_queue_max: usize,
    render_queue_max: usize,
    preview_max_active: usize,
    render_max_active: usize,
    stale_count: u64,
    dropped_count: u64,
    late_count: u64,
    gate_pass: bool,
}

#[derive(Debug, Default)]
struct PhaseStats {
    preview_ns: Vec<u64>,
    render_ns: Vec<u64>,
    present_ns: Vec<u64>,
    stale_count: u64,
    dropped_count: u64,
    late_count: u64,
}

impl PhaseStats {
    fn record(&mut self, frame: &FrameEvent) {
        self.preview_ns.push(frame.preview_ns);
        self.render_ns.push(frame.render_ns);
        self.present_ns.push(frame.present_ns);
        self.stale_count = frame.stale_count;
        self.dropped_count = frame.dropped_count;
        if frame.late {
            self.late_count += 1;
        }
    }
}

#[derive(Debug, Clone, Copy)]
struct PacingResult {
    achieved_fps: f64,
    late: bool,
}

#[derive(Debug, Default, Clone, Copy)]
struct PhaseCadence {
    first_presented_at: Option<Instant>,
    last_presented_at: Option<Instant>,
    presented_frames: usize,
}

impl PhaseCadence {
    fn record(&mut self, presented_at: Instant) {
        if self.first_presented_at.is_none() {
            self.first_presented_at = Some(presented_at);
        }
        self.last_presented_at = Some(presented_at);
        self.presented_frames += 1;
    }

    fn achieved_fps(self) -> Option<f64> {
        let first = self.first_presented_at?;
        let last = self.last_presented_at?;
        if self.presented_frames < 2 {
            return None;
        }
        let elapsed = last.saturating_duration_since(first);
        Some(
            (self.presented_frames.saturating_sub(1)) as f64
                / elapsed.as_secs_f64().max(f64::EPSILON),
        )
    }
}

struct TraceWriter {
    jsonl: BufWriter<File>,
    log: BufWriter<File>,
}

impl TraceWriter {
    fn new(
        stage: &'static str,
        mode_label: &str,
        output_dir: &Path,
    ) -> Result<Self, Box<dyn Error>> {
        fs::create_dir_all(output_dir)?;
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_else(|_| Duration::from_secs(0))
            .as_secs();
        let stem = format!("imexplore-movie-{}-{}-{}", stage, mode_label, timestamp);
        let jsonl_path = output_dir.join(format!("{stem}.jsonl"));
        let log_path = output_dir.join(format!("{stem}.log"));
        let jsonl = BufWriter::new(File::create(&jsonl_path)?);
        let mut log = BufWriter::new(File::create(&log_path)?);
        writeln!(log, "stage={stage} mode={mode_label}")?;
        writeln!(log, "jsonl={}", jsonl_path.display())?;
        writeln!(log, "log={}", log_path.display())?;
        Ok(Self { jsonl, log })
    }

    fn frame(&mut self, event: &FrameEvent) -> Result<(), Box<dyn Error>> {
        serde_json::to_writer(&mut self.jsonl, event)?;
        writeln!(self.jsonl)?;
        writeln!(
            self.log,
            "frame phase={} sequence={} loop={} occurrence={} preview_ms={:.3} render_ms={:.3} ready={} preview_active={} render_active={} preview_q={} render_q={} fps={:.2} late={}",
            event.phase,
            event.sequence,
            event.loop_index,
            event.occurrence_index,
            ns_to_ms(event.preview_ns),
            ns_to_ms(event.render_ns),
            event.ready_buffer_size,
            event.preview_active_workers,
            event.render_active_workers,
            event.preview_queue_depth,
            event.render_queue_depth,
            event.achieved_fps,
            event.late,
        )?;
        Ok(())
    }

    fn summary(&mut self, event: &SummaryEvent) -> Result<(), Box<dyn Error>> {
        serde_json::to_writer(&mut self.jsonl, event)?;
        writeln!(self.jsonl)?;
        writeln!(
            self.log,
            "summary phase={} frames={} target_fps={:.2} achieved_fps={:.2} preview_p50_ms={:.3} preview_p95_ms={:.3} render_p50_ms={:.3} render_p95_ms={:.3} ready_max={} preview_q_max={} render_q_max={} preview_max_active={} render_max_active={} stale={} dropped={} late={} gate_pass={}",
            event.phase,
            event.frame_count,
            event.target_fps,
            event.achieved_fps,
            ns_to_ms(event.preview_p50_ns),
            ns_to_ms(event.preview_p95_ns),
            ns_to_ms(event.render_p50_ns),
            ns_to_ms(event.render_p95_ns),
            event.ready_buffer_max,
            event.preview_queue_max,
            event.render_queue_max,
            event.preview_max_active,
            event.render_max_active,
            event.stale_count,
            event.dropped_count,
            event.late_count,
            event.gate_pass,
        )?;
        self.flush()?;
        Ok(())
    }

    fn log_line(&mut self, line: impl AsRef<str>) -> Result<(), Box<dyn Error>> {
        writeln!(self.log, "{}", line.as_ref())?;
        Ok(())
    }

    fn flush(&mut self) -> Result<(), Box<dyn Error>> {
        self.jsonl.flush()?;
        self.log.flush()?;
        Ok(())
    }
}

pub fn run_stage1(config: Stage1Config) -> Result<(), Box<dyn Error>> {
    // This runs before any worker threads are spawned, so process-wide env mutation is safe here.
    unsafe {
        std::env::set_var("CASARS_IMEXPLORE_PERF", "1");
    }
    let mut trace = TraceWriter::new(STAGE1_NAME, config.mode.as_str(), &config.output_dir)?;
    trace.log_line(format!("image={}", config.image_path.display()))?;
    trace.log_line(format!(
        "target_fps={:.2} warmup_loops={} measure_loops={} preview_workers={} render_workers={} ready_buffer={} bitmap={}x{} axis_position={}",
        config.target_fps,
        config.warmup_loops,
        config.measure_loops,
        config.preview_workers,
        config.render_workers,
        config.ready_buffer,
        config.bitmap_width,
        config.bitmap_height,
        config.axis_position,
    ))?;
    if let Some(path) = &config.save_first_frame {
        trace.log_line(format!("save_first_frame={}", path.display()))?;
    }
    if let Some(path) = &config.save_contact_sheet {
        trace.log_line(format!("save_contact_sheet={}", path.display()))?;
    }

    let template =
        build_template_context(&config.image_path, config.viewport(), config.axis_position)?;
    trace.log_line(format!(
        "occurrences={} selected_axis_position={}",
        template.occurrences.len(),
        config.axis_position,
    ))?;
    maybe_save_contact_sheet(&config, &template)?;

    match config.mode {
        Stage1Mode::RenderOnly => {
            let precomputed = precompute_cycle(&config, &template, &mut trace)?;
            run_render_only(&config, &precomputed, &mut trace)?;
        }
        Stage1Mode::PreviewRender => {
            run_preview_render(&config, &template, &mut trace)?;
        }
    }
    trace.flush()?;
    Ok(())
}

pub fn run_stage2(config: Stage2Config) -> Result<(), Box<dyn Error>> {
    // This runs before any worker threads are spawned, so process-wide env mutation is safe here.
    unsafe {
        std::env::set_var("CASARS_IMEXPLORE_PERF", "1");
    }
    let mut trace = TraceWriter::new(STAGE2_NAME, config.mode.as_str(), &config.output_dir)?;
    trace.log_line(format!("image={}", config.image_path.display()))?;
    trace.log_line(format!(
        "target_fps={:.2} warmup_loops={} measure_loops={} preview_workers={} render_workers={} ready_buffer={} bitmap={}x{} axis_position={}",
        config.target_fps,
        config.warmup_loops,
        config.measure_loops,
        config.preview_workers,
        config.render_workers,
        config.ready_buffer,
        config.bitmap_width,
        config.bitmap_height,
        config.axis_position,
    ))?;

    let template =
        build_template_context(&config.image_path, config.viewport(), config.axis_position)?;
    trace.log_line(format!(
        "occurrences={} selected_axis_position={}",
        template.occurrences.len(),
        config.axis_position,
    ))?;

    let preview_activity = WorkerActivity::default();
    let preview_queue = QueueDepthTracker::default();
    let render_activity = WorkerActivity::default();
    let render_queue = QueueDepthTracker::default();
    let (preview_ready_tx, preview_ready_rx) = mpsc::channel::<Result<PreviewResult, String>>();
    let (render_ready_tx, render_ready_rx) = mpsc::channel::<Result<RenderResult, String>>();
    let preview_senders = spawn_preview_workers(
        &Stage1Config {
            mode: Stage1Mode::PreviewRender,
            image_path: config.image_path.clone(),
            output_dir: config.output_dir.clone(),
            target_fps: config.target_fps,
            warmup_loops: config.warmup_loops,
            measure_loops: config.measure_loops,
            preview_workers: config.preview_workers,
            render_workers: config.render_workers,
            ready_buffer: config.ready_buffer,
            bitmap_width: config.bitmap_width,
            bitmap_height: config.bitmap_height,
            axis_position: config.axis_position,
            save_first_frame: None,
            save_contact_sheet: None,
        },
        &template,
        preview_activity.clone(),
        preview_queue.clone(),
        preview_ready_tx,
    )?;
    let render_senders = spawn_render_workers(
        config.render_workers,
        config.bitmap_width,
        config.bitmap_height,
        render_activity.clone(),
        render_queue.clone(),
        render_ready_tx,
    );

    let mut terminal = Stage2TerminalSession::enter()?;
    let mut presenter = Stage2KittyPresenter::new(template.occurrences.len())?;
    presenter.set_mode(config.mode);

    for (phase_kind, loop_count) in [
        (PhaseKind::Warmup, config.warmup_loops),
        (PhaseKind::Max, config.measure_loops),
        (PhaseKind::Sustained, config.measure_loops),
    ] {
        let summary = run_stage2_phase(
            phase_kind,
            loop_count,
            &config,
            &template,
            &preview_senders,
            &preview_ready_rx,
            &render_senders,
            &render_ready_rx,
            &preview_activity,
            &preview_queue,
            &render_activity,
            &render_queue,
            &mut terminal,
            &mut presenter,
            &mut trace,
        )?;
        trace.summary(&summary)?;
    }

    presenter.cleanup(terminal.stdout_mut())?;
    trace.flush()?;
    Ok(())
}

pub fn run_stage3(config: Stage3Config) -> Result<(), Box<dyn Error>> {
    unsafe {
        std::env::set_var("CASARS_IMEXPLORE_PERF", "1");
    }
    let mut trace = TraceWriter::new(STAGE3_NAME, "ratatui-panel", &config.output_dir)?;
    trace.log_line(format!("image={}", config.image_path.display()))?;
    trace.log_line(format!(
        "target_fps={:.2} warmup_loops={} measure_loops={} preview_workers={} render_workers={} ready_buffer={} bitmap={}x{} axis_position={}",
        config.target_fps,
        config.warmup_loops,
        config.measure_loops,
        config.preview_workers,
        config.render_workers,
        config.ready_buffer,
        config.bitmap_width,
        config.bitmap_height,
        config.axis_position,
    ))?;

    let template =
        build_template_context(&config.image_path, config.viewport(), config.axis_position)?;
    trace.log_line(format!(
        "occurrences={} selected_axis_position={}",
        template.occurrences.len(),
        config.axis_position,
    ))?;

    let preview_activity = WorkerActivity::default();
    let preview_queue = QueueDepthTracker::default();
    let render_activity = WorkerActivity::default();
    let render_queue = QueueDepthTracker::default();
    let (preview_ready_tx, preview_ready_rx) = mpsc::channel::<Result<PreviewResult, String>>();
    let (render_ready_tx, render_ready_rx) = mpsc::channel::<Result<RenderResult, String>>();
    let preview_senders = spawn_preview_workers(
        &Stage1Config {
            mode: Stage1Mode::PreviewRender,
            image_path: config.image_path.clone(),
            output_dir: config.output_dir.clone(),
            target_fps: config.target_fps,
            warmup_loops: config.warmup_loops,
            measure_loops: config.measure_loops,
            preview_workers: config.preview_workers,
            render_workers: config.render_workers,
            ready_buffer: config.ready_buffer,
            bitmap_width: config.bitmap_width,
            bitmap_height: config.bitmap_height,
            axis_position: config.axis_position,
            save_first_frame: None,
            save_contact_sheet: None,
        },
        &template,
        preview_activity.clone(),
        preview_queue.clone(),
        preview_ready_tx,
    )?;
    let render_senders = spawn_render_workers(
        config.render_workers,
        config.bitmap_width,
        config.bitmap_height,
        render_activity.clone(),
        render_queue.clone(),
        render_ready_tx,
    );

    let mut terminal = Stage3TerminalSession::enter()?;
    for (phase_kind, loop_count) in [
        (PhaseKind::Warmup, config.warmup_loops),
        (PhaseKind::Max, config.measure_loops),
        (PhaseKind::Sustained, config.measure_loops),
    ] {
        let summary = run_stage3_phase(
            phase_kind,
            loop_count,
            &config,
            &template,
            &preview_senders,
            &preview_ready_rx,
            &render_senders,
            &render_ready_rx,
            &preview_activity,
            &preview_queue,
            &render_activity,
            &render_queue,
            &mut terminal,
            &mut trace,
        )?;
        trace.summary(&summary)?;
    }

    trace.flush()?;
    Ok(())
}

fn build_template_context(
    image_path: &Path,
    viewport: ImageBrowserViewport,
    axis_position: usize,
) -> Result<TemplateContext, Box<dyn Error>> {
    let mut session = ImageBrowserSession::open(image_path, viewport)?;
    let snapshot = session.snapshot()?;
    if snapshot.non_display_axes.is_empty() {
        return Err("image has no non-display axis to animate".into());
    }
    let axis_state = snapshot
        .non_display_axes
        .get(axis_position)
        .ok_or_else(|| {
            format!(
                "axis position {} is out of range for {} non-display axes",
                axis_position,
                snapshot.non_display_axes.len()
            )
        })?;
    let base_indices = snapshot
        .non_display_axes
        .iter()
        .map(|axis| axis.index)
        .collect::<Vec<_>>();
    let occurrences = (0..axis_state.length)
        .map(|occurrence_index| {
            let mut indices = base_indices.clone();
            indices[axis_position] = occurrence_index;
            OccurrenceSpec {
                occurrence_index,
                non_display_indices: indices,
            }
        })
        .collect();
    Ok(TemplateContext {
        viewport,
        parameters: snapshot.parameters,
        plane_content_mode: ImagePlaneContentMode::Raster,
        occurrences,
    })
}

fn precompute_cycle(
    config: &Stage1Config,
    template: &TemplateContext,
    trace: &mut TraceWriter,
) -> Result<Vec<PrecomputedFrameInput>, Box<dyn Error>> {
    let mut session = ImageBrowserSession::open_with_parameters(
        &config.image_path,
        template.viewport,
        Some(&template.parameters),
    )?;
    let mut frames = Vec::with_capacity(template.occurrences.len());
    for occurrence in &template.occurrences {
        let request = ImageBrowserPreviewRequest {
            viewport: template.viewport,
            parameters: template.parameters.clone(),
            plane_content_mode: template.plane_content_mode,
            non_display_indices: occurrence.non_display_indices.clone(),
            include_profile: false,
        };
        let preview_started = Instant::now();
        let preview = session.preview_occurrence(&request)?;
        let _preview_ns = preview_started.elapsed().as_nanos() as u64;
        let cache_result = preview
            .snapshot
            .backend_timing
            .as_ref()
            .map(|timing| format!("{:?}", timing.plane_cache_result))
            .unwrap_or_else(|| "unknown".to_string());
        frames.push(PrecomputedFrameInput {
            occurrence_index: occurrence.occurrence_index,
            cache_result,
            snapshot: Arc::new(*preview.snapshot),
        });
    }
    trace.log_line(format!("precomputed_cycle_frames={}", frames.len()))?;
    Ok(frames)
}

fn run_render_only(
    config: &Stage1Config,
    precomputed: &[PrecomputedFrameInput],
    trace: &mut TraceWriter,
) -> Result<(), Box<dyn Error>> {
    let render_activity = WorkerActivity::default();
    let render_queue = QueueDepthTracker::default();
    let (render_ready_tx, render_ready_rx) = mpsc::channel::<Result<RenderResult, String>>();
    let render_senders = spawn_render_workers(
        config.render_workers,
        config.bitmap_width,
        config.bitmap_height,
        render_activity.clone(),
        render_queue.clone(),
        render_ready_tx,
    );

    for (phase_kind, loop_count) in phase_loops(config) {
        let phase = run_render_phase_from_precomputed(
            phase_kind,
            loop_count,
            config,
            precomputed,
            &render_senders,
            &render_ready_rx,
            &render_activity,
            &render_queue,
            trace,
        )?;
        trace.summary(&phase)?;
    }

    drop(render_senders);
    Ok(())
}

fn run_preview_render(
    config: &Stage1Config,
    template: &TemplateContext,
    trace: &mut TraceWriter,
) -> Result<(), Box<dyn Error>> {
    let preview_activity = WorkerActivity::default();
    let preview_queue = QueueDepthTracker::default();
    let render_activity = WorkerActivity::default();
    let render_queue = QueueDepthTracker::default();
    let (preview_ready_tx, preview_ready_rx) = mpsc::channel::<Result<PreviewResult, String>>();
    let (render_ready_tx, render_ready_rx) = mpsc::channel::<Result<RenderResult, String>>();
    let preview_senders = spawn_preview_workers(
        config,
        template,
        preview_activity.clone(),
        preview_queue.clone(),
        preview_ready_tx,
    )?;
    let render_senders = spawn_render_workers(
        config.render_workers,
        config.bitmap_width,
        config.bitmap_height,
        render_activity.clone(),
        render_queue.clone(),
        render_ready_tx,
    );

    for (phase_kind, loop_count) in phase_loops(config) {
        let summary = run_preview_render_phase(
            phase_kind,
            loop_count,
            config,
            template,
            &preview_senders,
            &preview_ready_rx,
            &render_senders,
            &render_ready_rx,
            &preview_activity,
            &preview_queue,
            &render_activity,
            &render_queue,
            trace,
        )?;
        trace.summary(&summary)?;
    }

    drop(preview_senders);
    drop(render_senders);
    Ok(())
}

fn spawn_preview_workers(
    config: &Stage1Config,
    template: &TemplateContext,
    preview_activity: WorkerActivity,
    preview_queue: QueueDepthTracker,
    result_tx: Sender<Result<PreviewResult, String>>,
) -> Result<Vec<Sender<PreviewJob>>, Box<dyn Error>> {
    let worker_count = config.preview_workers.max(1);
    let mut senders = Vec::with_capacity(worker_count);
    for worker_index in 0..worker_count {
        let (job_tx, job_rx) = mpsc::channel::<PreviewJob>();
        let mut session = ImageBrowserSession::open_with_parameters(
            &config.image_path,
            template.viewport,
            Some(&template.parameters),
        )?;
        let worker_preview_activity = preview_activity.clone();
        let worker_preview_queue = preview_queue.clone();
        let worker_result_tx = result_tx.clone();
        let viewport = template.viewport;
        let parameters = template.parameters.clone();
        let plane_content_mode = template.plane_content_mode;
        thread::Builder::new()
            .name(format!("imexplore-stage1-preview-{worker_index}"))
            .spawn(move || {
                while let Ok(job) = job_rx.recv() {
                    let _guard = worker_preview_activity.enter();
                    let request = ImageBrowserPreviewRequest {
                        viewport,
                        parameters: parameters.clone(),
                        plane_content_mode,
                        non_display_indices: job.non_display_indices,
                        include_profile: false,
                    };
                    let preview_started = Instant::now();
                    let result = session
                        .preview_occurrence(&request)
                        .map_err(|error| error.to_string());
                    let preview_ns = preview_started.elapsed().as_nanos() as u64;
                    worker_preview_queue.pop();
                    match result {
                        Ok(payload) => {
                            let cache_result = payload
                                .snapshot
                                .backend_timing
                                .as_ref()
                                .map(|timing| format!("{:?}", timing.plane_cache_result))
                                .unwrap_or_else(|| "unknown".to_string());
                            let preview = PreviewResult {
                                sequence: job.sequence,
                                loop_index: job.loop_index,
                                occurrence_index: job.occurrence_index,
                                cache_result,
                                preview_ns,
                                snapshot: Arc::new(*payload.snapshot),
                            };
                            if worker_result_tx.send(Ok(preview)).is_err() {
                                return;
                            }
                        }
                        Err(error) => {
                            let _ = worker_result_tx.send(Err(error));
                            return;
                        }
                    }
                }
            })?;
        senders.push(job_tx);
    }
    Ok(senders)
}

fn spawn_render_workers(
    worker_count: usize,
    bitmap_width: u16,
    bitmap_height: u16,
    render_activity: WorkerActivity,
    render_queue: QueueDepthTracker,
    result_tx: Sender<Result<RenderResult, String>>,
) -> Vec<Sender<RenderJob>> {
    let worker_count = worker_count.max(1);
    let mut senders = Vec::with_capacity(worker_count);
    for worker_index in 0..worker_count {
        let (job_tx, job_rx) = mpsc::channel::<RenderJob>();
        let worker_render_activity = render_activity.clone();
        let worker_render_queue = render_queue.clone();
        let worker_result_tx = result_tx.clone();
        thread::Builder::new()
            .name(format!("imexplore-stage1-render-{worker_index}"))
            .spawn(move || {
                while let Ok(job) = job_rx.recv() {
                    let _guard = worker_render_activity.enter();
                    let render_started = Instant::now();
                    let result = render_plane_pane_from_snapshot(
                        u32::from(bitmap_width),
                        u32::from(bitmap_height),
                        &job.snapshot,
                        PlanePaneRenderOptions::default(),
                    )
                    .map_err(|error| error.to_string());
                    let render_ns = render_started.elapsed().as_nanos() as u64;
                    worker_render_queue.pop();
                    match result {
                        Ok(image) => {
                            let output = RenderResult {
                                sequence: job.sequence,
                                loop_index: job.loop_index,
                                occurrence_index: job.occurrence_index,
                                cache_result: job.cache_result,
                                preview_ns: job.preview_ns,
                                render_ns,
                                image,
                            };
                            if worker_result_tx.send(Ok(output)).is_err() {
                                return;
                            }
                        }
                        Err(error) => {
                            let _ = worker_result_tx.send(Err(error));
                            return;
                        }
                    }
                }
            })
            .expect("spawn render worker");
        senders.push(job_tx);
    }
    senders
}

#[allow(clippy::too_many_arguments)]
fn run_render_phase_from_precomputed(
    phase_kind: PhaseKind,
    loop_count: usize,
    config: &Stage1Config,
    precomputed: &[PrecomputedFrameInput],
    render_senders: &[Sender<RenderJob>],
    render_ready_rx: &Receiver<Result<RenderResult, String>>,
    render_activity: &WorkerActivity,
    render_queue: &QueueDepthTracker,
    trace: &mut TraceWriter,
) -> Result<SummaryEvent, Box<dyn Error>> {
    let total_frames = precomputed.len() * loop_count;
    let mut ready = OrderedReadyBuffer::new(config.ready_buffer);
    let phase_start = Instant::now();
    let mut phase_stats = PhaseStats::default();
    let mut cadence = PhaseCadence::default();
    let mut next_submit = 0usize;
    let mut render_inflight = 0usize;
    let mut worker_index = 0usize;
    let mut saved_example_frame = false;

    while (ready.next_sequence() as usize) < total_frames {
        while next_submit < total_frames && render_inflight + ready.len() < config.ready_buffer {
            let frame = &precomputed[next_submit % precomputed.len()];
            let job = RenderJob {
                sequence: next_submit as u64,
                loop_index: next_submit / precomputed.len(),
                occurrence_index: frame.occurrence_index,
                cache_result: frame.cache_result.clone(),
                preview_ns: 0,
                snapshot: Arc::clone(&frame.snapshot),
            };
            render_queue.push();
            render_senders[worker_index % render_senders.len()].send(job)?;
            render_inflight += 1;
            worker_index = worker_index.wrapping_add(1);
            next_submit += 1;
        }

        drain_render_results(
            &mut ready,
            &mut render_inflight,
            render_ready_rx,
            &mut phase_stats,
        )?;

        if let Some((sequence, frame)) = ready.pop_next() {
            if !saved_example_frame {
                maybe_save_example_frame(config, &frame.image)?;
                saved_example_frame = true;
            }
            let pacing = pace_phase(phase_kind, phase_start, sequence + 1, config.target_fps);
            cadence.record(Instant::now());
            let event = frame_event_from_result(
                config.mode.as_str(),
                STAGE1_NAME,
                phase_kind,
                sequence,
                frame,
                &ready,
                &WorkerActivity::default(),
                render_activity,
                &QueueDepthTracker::default(),
                render_queue,
                0,
                phase_stats.stale_count,
                phase_stats.dropped_count,
                pacing,
            );
            phase_stats.record(&event);
            trace.frame(&event)?;
            continue;
        }

        wait_for_render_result_blocking(
            &mut ready,
            &mut render_inflight,
            render_ready_rx,
            &mut phase_stats,
        )?;
    }

    Ok(summary_event_from_phase(
        config.mode.as_str(),
        STAGE1_NAME,
        phase_kind,
        total_frames,
        config.target_fps,
        phase_start.elapsed(),
        cadence,
        &phase_stats,
        &ready,
        &QueueDepthTracker::default(),
        render_queue,
        &WorkerActivity::default(),
        render_activity,
    ))
}

#[allow(clippy::too_many_arguments)]
fn run_preview_render_phase(
    phase_kind: PhaseKind,
    loop_count: usize,
    config: &Stage1Config,
    template: &TemplateContext,
    preview_senders: &[Sender<PreviewJob>],
    preview_ready_rx: &Receiver<Result<PreviewResult, String>>,
    render_senders: &[Sender<RenderJob>],
    render_ready_rx: &Receiver<Result<RenderResult, String>>,
    preview_activity: &WorkerActivity,
    preview_queue: &QueueDepthTracker,
    render_activity: &WorkerActivity,
    render_queue: &QueueDepthTracker,
    trace: &mut TraceWriter,
) -> Result<SummaryEvent, Box<dyn Error>> {
    let total_frames = template.occurrences.len() * loop_count;
    let mut ready = OrderedReadyBuffer::new(config.ready_buffer);
    let phase_start = Instant::now();
    let mut phase_stats = PhaseStats::default();
    let mut cadence = PhaseCadence::default();
    let mut next_submit = 0usize;
    let mut preview_inflight = 0usize;
    let mut render_inflight = 0usize;
    let mut preview_worker_index = 0usize;
    let mut render_worker_index = 0usize;
    let mut saved_example_frame = false;

    while (ready.next_sequence() as usize) < total_frames {
        while next_submit < total_frames
            && preview_inflight + render_inflight + ready.len() < config.ready_buffer
        {
            let occurrence = &template.occurrences[next_submit % template.occurrences.len()];
            let job = PreviewJob {
                sequence: next_submit as u64,
                loop_index: next_submit / template.occurrences.len(),
                occurrence_index: occurrence.occurrence_index,
                non_display_indices: occurrence.non_display_indices.clone(),
            };
            preview_queue.push();
            preview_senders[preview_worker_index % preview_senders.len()].send(job)?;
            preview_worker_index = preview_worker_index.wrapping_add(1);
            preview_inflight += 1;
            next_submit += 1;
        }

        drain_preview_results(
            &mut preview_inflight,
            &mut render_inflight,
            preview_ready_rx,
            render_senders,
            &mut render_worker_index,
            render_queue,
        )?;
        drain_render_results(
            &mut ready,
            &mut render_inflight,
            render_ready_rx,
            &mut phase_stats,
        )?;

        if let Some((sequence, frame)) = ready.pop_next() {
            if !saved_example_frame {
                maybe_save_example_frame(config, &frame.image)?;
                saved_example_frame = true;
            }
            let pacing = pace_phase(phase_kind, phase_start, sequence + 1, config.target_fps);
            cadence.record(Instant::now());
            let event = frame_event_from_result(
                config.mode.as_str(),
                STAGE1_NAME,
                phase_kind,
                sequence,
                frame,
                &ready,
                preview_activity,
                render_activity,
                preview_queue,
                render_queue,
                0,
                phase_stats.stale_count,
                phase_stats.dropped_count,
                pacing,
            );
            phase_stats.record(&event);
            trace.frame(&event)?;
            continue;
        }

        wait_for_progress(
            &mut ready,
            &mut preview_inflight,
            &mut render_inflight,
            preview_ready_rx,
            render_senders,
            &mut render_worker_index,
            render_ready_rx,
            render_queue,
            &mut phase_stats,
        )?;
    }

    Ok(summary_event_from_phase(
        config.mode.as_str(),
        STAGE1_NAME,
        phase_kind,
        total_frames,
        config.target_fps,
        phase_start.elapsed(),
        cadence,
        &phase_stats,
        &ready,
        preview_queue,
        render_queue,
        preview_activity,
        render_activity,
    ))
}

#[allow(clippy::too_many_arguments)]
fn run_stage2_phase(
    phase_kind: PhaseKind,
    loop_count: usize,
    config: &Stage2Config,
    template: &TemplateContext,
    preview_senders: &[Sender<PreviewJob>],
    preview_ready_rx: &Receiver<Result<PreviewResult, String>>,
    render_senders: &[Sender<RenderJob>],
    render_ready_rx: &Receiver<Result<RenderResult, String>>,
    preview_activity: &WorkerActivity,
    preview_queue: &QueueDepthTracker,
    render_activity: &WorkerActivity,
    render_queue: &QueueDepthTracker,
    terminal: &mut Stage2TerminalSession,
    presenter: &mut Stage2KittyPresenter,
    trace: &mut TraceWriter,
) -> Result<SummaryEvent, Box<dyn Error>> {
    let total_frames = template.occurrences.len() * loop_count;
    let mut ready = OrderedReadyBuffer::new(config.ready_buffer);
    let phase_start = Instant::now();
    let mut phase_stats = PhaseStats::default();
    let mut cadence = PhaseCadence::default();
    let mut next_submit = 0usize;
    let mut preview_inflight = 0usize;
    let mut render_inflight = 0usize;
    let mut preview_worker_index = 0usize;
    let mut render_worker_index = 0usize;
    let mut processed_frames = 0usize;

    while processed_frames < total_frames {
        if stage2_stop_requested()? {
            trace.log_line(format!("phase={} stop_requested=true", phase_kind.as_str()))?;
            break;
        }

        while next_submit < total_frames
            && preview_inflight + render_inflight + ready.len() < config.ready_buffer
        {
            let occurrence = &template.occurrences[next_submit % template.occurrences.len()];
            let job = PreviewJob {
                sequence: next_submit as u64,
                loop_index: next_submit / template.occurrences.len(),
                occurrence_index: occurrence.occurrence_index,
                non_display_indices: occurrence.non_display_indices.clone(),
            };
            preview_queue.push();
            preview_senders[preview_worker_index % preview_senders.len()].send(job)?;
            preview_worker_index = preview_worker_index.wrapping_add(1);
            preview_inflight += 1;
            next_submit += 1;
        }

        drain_preview_results(
            &mut preview_inflight,
            &mut render_inflight,
            preview_ready_rx,
            render_senders,
            &mut render_worker_index,
            render_queue,
        )?;
        drain_render_results(
            &mut ready,
            &mut render_inflight,
            render_ready_rx,
            &mut phase_stats,
        )?;

        if let Some((sequence, frame)) = ready.pop_next() {
            let placement_rect = terminal.placement_rect()?;
            let present_ns = presenter.present(
                terminal.stdout_mut(),
                frame.occurrence_index,
                &frame.image,
                placement_rect,
            )?;
            let pacing = pace_phase(phase_kind, phase_start, sequence + 1, config.target_fps);
            cadence.record(Instant::now());
            let event = frame_event_from_result(
                config.mode.as_str(),
                STAGE2_NAME,
                phase_kind,
                sequence,
                frame,
                &ready,
                preview_activity,
                render_activity,
                preview_queue,
                render_queue,
                present_ns,
                phase_stats.stale_count,
                phase_stats.dropped_count,
                pacing,
            );
            phase_stats.record(&event);
            trace.frame(&event)?;
            processed_frames += 1;
            continue;
        }

        wait_for_progress(
            &mut ready,
            &mut preview_inflight,
            &mut render_inflight,
            preview_ready_rx,
            render_senders,
            &mut render_worker_index,
            render_ready_rx,
            render_queue,
            &mut phase_stats,
        )?;
    }

    Ok(summary_event_from_phase(
        config.mode.as_str(),
        STAGE2_NAME,
        phase_kind,
        processed_frames,
        config.target_fps,
        phase_start.elapsed(),
        cadence,
        &phase_stats,
        &ready,
        preview_queue,
        render_queue,
        preview_activity,
        render_activity,
    ))
}

#[allow(clippy::too_many_arguments)]
fn run_stage3_phase(
    phase_kind: PhaseKind,
    loop_count: usize,
    config: &Stage3Config,
    template: &TemplateContext,
    preview_senders: &[Sender<PreviewJob>],
    preview_ready_rx: &Receiver<Result<PreviewResult, String>>,
    render_senders: &[Sender<RenderJob>],
    render_ready_rx: &Receiver<Result<RenderResult, String>>,
    preview_activity: &WorkerActivity,
    preview_queue: &QueueDepthTracker,
    render_activity: &WorkerActivity,
    render_queue: &QueueDepthTracker,
    terminal: &mut Stage3TerminalSession,
    trace: &mut TraceWriter,
) -> Result<SummaryEvent, Box<dyn Error>> {
    let total_frames = template.occurrences.len() * loop_count;
    let mut ready = OrderedReadyBuffer::new(config.ready_buffer);
    let phase_start = Instant::now();
    let mut phase_stats = PhaseStats::default();
    let mut cadence = PhaseCadence::default();
    let mut next_submit = 0usize;
    let mut preview_inflight = 0usize;
    let mut render_inflight = 0usize;
    let mut preview_worker_index = 0usize;
    let mut render_worker_index = 0usize;
    let mut processed_frames = 0usize;

    while processed_frames < total_frames {
        if stage2_stop_requested()? {
            trace.log_line(format!("phase={} stop_requested=true", phase_kind.as_str()))?;
            break;
        }

        while next_submit < total_frames
            && preview_inflight + render_inflight + ready.len() < config.ready_buffer
        {
            let occurrence = &template.occurrences[next_submit % template.occurrences.len()];
            let job = PreviewJob {
                sequence: next_submit as u64,
                loop_index: next_submit / template.occurrences.len(),
                occurrence_index: occurrence.occurrence_index,
                non_display_indices: occurrence.non_display_indices.clone(),
            };
            preview_queue.push();
            preview_senders[preview_worker_index % preview_senders.len()].send(job)?;
            preview_worker_index = preview_worker_index.wrapping_add(1);
            preview_inflight += 1;
            next_submit += 1;
        }

        drain_preview_results(
            &mut preview_inflight,
            &mut render_inflight,
            preview_ready_rx,
            render_senders,
            &mut render_worker_index,
            render_queue,
        )?;
        drain_render_results(
            &mut ready,
            &mut render_inflight,
            render_ready_rx,
            &mut phase_stats,
        )?;

        if let Some((sequence, frame)) = ready.pop_next() {
            let present_ns = terminal.present(frame.occurrence_index, &frame.image, phase_kind)?;
            let pacing = pace_phase(phase_kind, phase_start, sequence + 1, config.target_fps);
            cadence.record(Instant::now());
            let event = frame_event_from_result(
                "ratatui-panel",
                STAGE3_NAME,
                phase_kind,
                sequence,
                frame,
                &ready,
                preview_activity,
                render_activity,
                preview_queue,
                render_queue,
                present_ns,
                phase_stats.stale_count,
                phase_stats.dropped_count,
                pacing,
            );
            phase_stats.record(&event);
            trace.frame(&event)?;
            processed_frames += 1;
            continue;
        }

        wait_for_progress(
            &mut ready,
            &mut preview_inflight,
            &mut render_inflight,
            preview_ready_rx,
            render_senders,
            &mut render_worker_index,
            render_ready_rx,
            render_queue,
            &mut phase_stats,
        )?;
    }

    Ok(summary_event_from_phase(
        "ratatui-panel",
        STAGE3_NAME,
        phase_kind,
        processed_frames,
        config.target_fps,
        phase_start.elapsed(),
        cadence,
        &phase_stats,
        &ready,
        preview_queue,
        render_queue,
        preview_activity,
        render_activity,
    ))
}

fn drain_preview_results(
    preview_inflight: &mut usize,
    render_inflight: &mut usize,
    preview_ready_rx: &Receiver<Result<PreviewResult, String>>,
    render_senders: &[Sender<RenderJob>],
    render_worker_index: &mut usize,
    render_queue: &QueueDepthTracker,
) -> Result<(), Box<dyn Error>> {
    loop {
        match preview_ready_rx.try_recv() {
            Ok(Ok(preview)) => {
                *preview_inflight = preview_inflight.saturating_sub(1);
                render_queue.push();
                render_senders[*render_worker_index % render_senders.len()].send(RenderJob {
                    sequence: preview.sequence,
                    loop_index: preview.loop_index,
                    occurrence_index: preview.occurrence_index,
                    cache_result: preview.cache_result,
                    preview_ns: preview.preview_ns,
                    snapshot: preview.snapshot,
                })?;
                *render_worker_index = render_worker_index.wrapping_add(1);
                *render_inflight += 1;
            }
            Ok(Err(error)) => return Err(error.into()),
            Err(TryRecvError::Empty) => return Ok(()),
            Err(TryRecvError::Disconnected) => {
                return Err("preview worker channel disconnected".into());
            }
        }
    }
}

fn drain_render_results(
    ready: &mut OrderedReadyBuffer<RenderResult>,
    render_inflight: &mut usize,
    render_ready_rx: &Receiver<Result<RenderResult, String>>,
    stats: &mut PhaseStats,
) -> Result<(), Box<dyn Error>> {
    loop {
        match render_ready_rx.try_recv() {
            Ok(Ok(frame)) => {
                *render_inflight = render_inflight.saturating_sub(1);
                match ready.insert(frame.sequence, frame) {
                    ReadyInsertResult::Inserted | ReadyInsertResult::ReplacedExisting => {}
                    ReadyInsertResult::Stale => {
                        stats.stale_count = stats.stale_count.saturating_add(1);
                    }
                    ReadyInsertResult::RejectedAtCapacity => {
                        stats.dropped_count = stats.dropped_count.saturating_add(1);
                    }
                }
            }
            Ok(Err(error)) => return Err(error.into()),
            Err(TryRecvError::Empty) => return Ok(()),
            Err(TryRecvError::Disconnected) => {
                return Err("render worker channel disconnected".into());
            }
        }
    }
}

#[allow(clippy::too_many_arguments)]
fn wait_for_progress(
    ready: &mut OrderedReadyBuffer<RenderResult>,
    preview_inflight: &mut usize,
    render_inflight: &mut usize,
    preview_ready_rx: &Receiver<Result<PreviewResult, String>>,
    render_senders: &[Sender<RenderJob>],
    render_worker_index: &mut usize,
    render_ready_rx: &Receiver<Result<RenderResult, String>>,
    render_queue: &QueueDepthTracker,
    stats: &mut PhaseStats,
) -> Result<(), Box<dyn Error>> {
    loop {
        match render_ready_rx.try_recv() {
            Ok(Ok(frame)) => {
                *render_inflight = render_inflight.saturating_sub(1);
                match ready.insert(frame.sequence, frame) {
                    ReadyInsertResult::Inserted | ReadyInsertResult::ReplacedExisting => {}
                    ReadyInsertResult::Stale => {
                        stats.stale_count = stats.stale_count.saturating_add(1);
                    }
                    ReadyInsertResult::RejectedAtCapacity => {
                        stats.dropped_count = stats.dropped_count.saturating_add(1);
                    }
                }
                return Ok(());
            }
            Ok(Err(error)) => return Err(error.into()),
            Err(TryRecvError::Disconnected) => {
                return Err("render worker channel disconnected".into());
            }
            Err(TryRecvError::Empty) => {}
        }
        match preview_ready_rx.try_recv() {
            Ok(Ok(preview)) => {
                *preview_inflight = preview_inflight.saturating_sub(1);
                render_queue.push();
                render_senders[*render_worker_index % render_senders.len()].send(RenderJob {
                    sequence: preview.sequence,
                    loop_index: preview.loop_index,
                    occurrence_index: preview.occurrence_index,
                    cache_result: preview.cache_result,
                    preview_ns: preview.preview_ns,
                    snapshot: preview.snapshot,
                })?;
                *render_worker_index = render_worker_index.wrapping_add(1);
                *render_inflight += 1;
                return Ok(());
            }
            Ok(Err(error)) => return Err(error.into()),
            Err(TryRecvError::Disconnected) => {
                return Err("preview worker channel disconnected".into());
            }
            Err(TryRecvError::Empty) => {}
        }
        thread::sleep(Duration::from_millis(1));
    }
}

fn wait_for_render_result_blocking(
    ready: &mut OrderedReadyBuffer<RenderResult>,
    render_inflight: &mut usize,
    render_ready_rx: &Receiver<Result<RenderResult, String>>,
    stats: &mut PhaseStats,
) -> Result<(), Box<dyn Error>> {
    loop {
        match render_ready_rx.recv() {
            Ok(Ok(frame)) => {
                *render_inflight = render_inflight.saturating_sub(1);
                match ready.insert(frame.sequence, frame) {
                    ReadyInsertResult::Inserted | ReadyInsertResult::ReplacedExisting => {
                        return Ok(());
                    }
                    ReadyInsertResult::Stale => {
                        stats.stale_count = stats.stale_count.saturating_add(1);
                    }
                    ReadyInsertResult::RejectedAtCapacity => {
                        stats.dropped_count = stats.dropped_count.saturating_add(1);
                    }
                }
            }
            Ok(Err(error)) => return Err(error.into()),
            Err(_) => return Err("render worker channel disconnected".into()),
        }
    }
}

#[allow(clippy::too_many_arguments)]
fn frame_event_from_result(
    mode_label: &str,
    stage: &'static str,
    phase_kind: PhaseKind,
    sequence: u64,
    frame: RenderResult,
    ready: &OrderedReadyBuffer<RenderResult>,
    preview_activity: &WorkerActivity,
    render_activity: &WorkerActivity,
    preview_queue: &QueueDepthTracker,
    render_queue: &QueueDepthTracker,
    present_ns: u64,
    stale_count: u64,
    dropped_count: u64,
    pacing: PacingResult,
) -> FrameEvent {
    let _image = frame.image;
    FrameEvent {
        kind: "frame",
        stage,
        mode: mode_label.to_string(),
        phase: phase_kind.as_str().to_string(),
        sequence,
        loop_index: frame.loop_index,
        occurrence_index: frame.occurrence_index,
        preview_ns: frame.preview_ns,
        render_ns: frame.render_ns,
        present_ns,
        ready_buffer_size: ready.len(),
        preview_active_workers: preview_activity.active(),
        render_active_workers: render_activity.active(),
        preview_queue_depth: preview_queue.current(),
        render_queue_depth: render_queue.current(),
        cache_result: frame.cache_result,
        achieved_fps: pacing.achieved_fps,
        stale_count,
        dropped_count,
        late: pacing.late,
    }
}

fn pace_phase(
    phase_kind: PhaseKind,
    phase_start: Instant,
    completed_frames: u64,
    target_fps: f64,
) -> PacingResult {
    if !matches!(phase_kind, PhaseKind::Sustained) || target_fps <= 0.0 {
        return PacingResult {
            achieved_fps: completed_frames as f64
                / phase_start.elapsed().as_secs_f64().max(f64::EPSILON),
            late: false,
        };
    }
    let due = Duration::from_secs_f64(completed_frames as f64 / target_fps);
    loop {
        let elapsed = phase_start.elapsed();
        if elapsed >= due {
            let settled = phase_start.elapsed();
            return PacingResult {
                achieved_fps: completed_frames as f64 / settled.as_secs_f64().max(f64::EPSILON),
                late: settled > due + Duration::from_micros(500),
            };
        }
        let remaining = due - elapsed;
        if remaining > Duration::from_millis(2) {
            thread::sleep(remaining - Duration::from_millis(1));
        } else {
            std::hint::spin_loop();
        }
    }
}

#[allow(clippy::too_many_arguments)]
fn summary_event_from_phase(
    mode_label: &str,
    stage: &'static str,
    phase_kind: PhaseKind,
    frame_count: usize,
    target_fps: f64,
    elapsed: Duration,
    cadence: PhaseCadence,
    stats: &PhaseStats,
    ready: &OrderedReadyBuffer<RenderResult>,
    preview_queue: &QueueDepthTracker,
    render_queue: &QueueDepthTracker,
    preview_activity: &WorkerActivity,
    render_activity: &WorkerActivity,
) -> SummaryEvent {
    let achieved_fps = if matches!(phase_kind, PhaseKind::Sustained) {
        cadence
            .achieved_fps()
            .unwrap_or_else(|| frame_count as f64 / elapsed.as_secs_f64().max(f64::EPSILON))
    } else {
        frame_count as f64 / elapsed.as_secs_f64().max(f64::EPSILON)
    };
    SummaryEvent {
        kind: "summary",
        stage,
        mode: mode_label.to_string(),
        phase: phase_kind.as_str().to_string(),
        frame_count,
        target_fps,
        achieved_fps,
        preview_p50_ns: percentile_ns(&stats.preview_ns, 0.50),
        preview_p95_ns: percentile_ns(&stats.preview_ns, 0.95),
        render_p50_ns: percentile_ns(&stats.render_ns, 0.50),
        render_p95_ns: percentile_ns(&stats.render_ns, 0.95),
        present_p50_ns: percentile_ns(&stats.present_ns, 0.50),
        present_p95_ns: percentile_ns(&stats.present_ns, 0.95),
        ready_buffer_max: ready.max_len(),
        preview_queue_max: preview_queue.max_depth(),
        render_queue_max: render_queue.max_depth(),
        preview_max_active: preview_activity.max_active(),
        render_max_active: render_activity.max_active(),
        stale_count: stats.stale_count,
        dropped_count: stats.dropped_count,
        late_count: stats.late_count,
        gate_pass: !matches!(phase_kind, PhaseKind::Sustained) || achieved_fps >= target_fps,
    }
}

fn phase_loops(config: &Stage1Config) -> [(PhaseKind, usize); 3] {
    [
        (PhaseKind::Warmup, config.warmup_loops),
        (PhaseKind::Max, config.measure_loops),
        (PhaseKind::Sustained, config.measure_loops),
    ]
}

fn percentile_ns(values: &[u64], quantile: f64) -> u64 {
    if values.is_empty() {
        return 0;
    }
    let mut sorted = values.to_vec();
    sorted.sort_unstable();
    let index = ((sorted.len() - 1) as f64 * quantile.clamp(0.0, 1.0)).round() as usize;
    sorted[index.min(sorted.len() - 1)]
}

fn stage2_stop_requested() -> Result<bool, Box<dyn Error>> {
    while event::poll(Duration::from_millis(0))? {
        match event::read()? {
            Event::Key(key)
                if key.kind == KeyEventKind::Press
                    && (matches!(key.code, KeyCode::Char('q') | KeyCode::Esc)
                        || (matches!(key.code, KeyCode::Char('c'))
                            && key.modifiers.contains(KeyModifiers::CONTROL))) =>
            {
                return Ok(true);
            }
            _ => {}
        }
    }
    Ok(false)
}

fn parse_mode(value: &str) -> Result<Stage1Mode, Box<dyn Error>> {
    match value {
        "render-only" => Ok(Stage1Mode::RenderOnly),
        "preview-render" => Ok(Stage1Mode::PreviewRender),
        other => Err(format!("unsupported mode: {other}").into()),
    }
}

fn parse_stage2_mode(value: &str) -> Result<Stage2Mode, Box<dyn Error>> {
    match value {
        "upload-each-frame" => Ok(Stage2Mode::UploadEachFrame),
        "preload-then-place" => Ok(Stage2Mode::PreloadThenPlace),
        other => Err(format!("unsupported Stage 2 mode: {other}").into()),
    }
}

fn next_value<I>(args: &mut I, flag: &str) -> Result<String, Box<dyn Error>>
where
    I: Iterator<Item = String>,
{
    args.next()
        .ok_or_else(|| format!("missing value for {flag}").into())
}

fn parse_positive_f64(value: &str) -> Result<f64, Box<dyn Error>> {
    let parsed = value.parse::<f64>()?;
    if parsed <= 0.0 {
        return Err(format!("expected positive number, got {value}").into());
    }
    Ok(parsed)
}

fn parse_usize(value: &str) -> Result<usize, Box<dyn Error>> {
    Ok(value.parse::<usize>()?)
}

fn parse_positive_usize(value: &str) -> Result<usize, Box<dyn Error>> {
    let parsed = parse_usize(value)?;
    if parsed == 0 {
        return Err(format!("expected positive integer, got {value}").into());
    }
    Ok(parsed)
}

fn parse_positive_u16(value: &str) -> Result<u16, Box<dyn Error>> {
    let parsed = value.parse::<u16>()?;
    if parsed == 0 {
        return Err(format!("expected positive integer, got {value}").into());
    }
    Ok(parsed)
}

fn ns_to_ms(value: u64) -> f64 {
    value as f64 / 1_000_000.0
}

fn maybe_save_example_frame(
    config: &Stage1Config,
    image: &DynamicImage,
) -> Result<(), Box<dyn Error>> {
    let Some(path) = &config.save_first_frame else {
        return Ok(());
    };
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }
    image.save(path)?;
    Ok(())
}

fn maybe_save_contact_sheet(
    config: &Stage1Config,
    template: &TemplateContext,
) -> Result<(), Box<dyn Error>> {
    let Some(path) = &config.save_contact_sheet else {
        return Ok(());
    };
    let mut session = ImageBrowserSession::open_with_parameters(
        &config.image_path,
        template.viewport,
        Some(&template.parameters),
    )?;
    let mut frames = Vec::with_capacity(template.occurrences.len());
    for occurrence in &template.occurrences {
        let preview = session.preview_occurrence(&ImageBrowserPreviewRequest {
            viewport: template.viewport,
            parameters: template.parameters.clone(),
            plane_content_mode: template.plane_content_mode,
            non_display_indices: occurrence.non_display_indices.clone(),
            include_profile: false,
        })?;
        let frame = render_plane_pane_from_snapshot(
            u32::from(config.bitmap_width),
            u32::from(config.bitmap_height),
            &preview.snapshot,
            PlanePaneRenderOptions::default(),
        )?;
        frames.push(frame);
    }
    if frames.is_empty() {
        return Ok(());
    }
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }
    let sheet = build_contact_sheet(&frames, 5);
    sheet.save(path)?;
    Ok(())
}

fn build_contact_sheet(frames: &[DynamicImage], columns: usize) -> DynamicImage {
    let columns = columns.max(1);
    let rows = frames.len().div_ceil(columns);
    let thumb_width = (frames[0].width() / columns.max(1) as u32).max(1);
    let thumb_height = ((frames[0].height() as f64)
        * (thumb_width as f64 / frames[0].width() as f64))
        .round()
        .max(1.0) as u32;
    let margin = 8u32;
    let gutter = 8u32;
    let sheet_width =
        margin * 2 + columns as u32 * thumb_width + (columns.saturating_sub(1) as u32) * gutter;
    let sheet_height =
        margin * 2 + rows as u32 * thumb_height + (rows.saturating_sub(1) as u32) * gutter;
    let mut canvas = RgbaImage::from_pixel(sheet_width, sheet_height, Rgba([9, 14, 20, 255]));
    for (index, frame) in frames.iter().enumerate() {
        let row = index / columns;
        let column = index % columns;
        let x = margin + column as u32 * (thumb_width + gutter);
        let y = margin + row as u32 * (thumb_height + gutter);
        let thumbnail = frame.resize_exact(thumb_width, thumb_height, FilterType::Triangle);
        overlay(
            &mut canvas,
            &thumbnail.to_rgba8(),
            i64::from(x),
            i64::from(y),
        );
    }
    DynamicImage::ImageRgba8(canvas)
}

fn print_stage1_help() {
    println!(
        "\
imexplore_movie_stage1

Usage:
  cargo run --release -p casars --example imexplore_movie_stage1 -- [options]

Options:
  --mode <render-only|preview-render>   Benchmark submode (default: preview-render)
  --image <path>                        CASA image path (default: {DEFAULT_IMAGE_PATH})
  --output-dir <path>                   Output directory (default: {DEFAULT_OUTPUT_DIR})
  --target-fps <fps>                    Sustained benchmark target FPS (default: 30)
  --warmup-loops <n>                    Warmup loop count (default: 1)
  --measure-loops <n>                   Loop count for max/sustained phases (default: 3)
  --preview-workers <n>                 Preview worker pool size (default: 2)
  --render-workers <n>                  Render worker pool size (default: 4)
  --ready-buffer <n>                    Ready-frame buffer capacity (default: 32)
  --bitmap-width <px>                   Output bitmap width (default: {DEFAULT_BITMAP_WIDTH})
  --bitmap-height <px>                  Output bitmap height (default: {DEFAULT_BITMAP_HEIGHT})
  --axis-position <n>                   Non-display axis position to animate (default: 0)
  --save-first-frame <path>             Save the first rendered frame as a PNG
  --save-contact-sheet <path>           Save the first-cycle contact sheet as a PNG
  --help                                Show this help
"
    );
}

fn print_stage2_help() {
    println!(
        "\
imexplore_movie_stage2

Usage:
  cargo run --release -p casars --example imexplore_movie_stage2 -- [options]

Options:
  --mode <upload-each-frame|preload-then-place>  Ghostty playback mode (default: upload-each-frame)
  --image <path>                                 CASA image path (default: {DEFAULT_IMAGE_PATH})
  --output-dir <path>                            Output directory (default: {DEFAULT_OUTPUT_DIR})
  --target-fps <fps>                             Sustained benchmark target FPS (default: 30)
  --warmup-loops <n>                             Warmup loop count (default: 1)
  --measure-loops <n>                            Loop count for max/sustained phases (default: 3)
  --preview-workers <n>                          Preview worker pool size (default: 2)
  --render-workers <n>                           Render worker pool size (default: 4)
  --ready-buffer <n>                             Ready-frame buffer capacity (default: 32)
  --bitmap-width <px>                            Output bitmap width (default: {DEFAULT_BITMAP_WIDTH})
  --bitmap-height <px>                           Output bitmap height (default: {DEFAULT_BITMAP_HEIGHT})
  --axis-position <n>                            Non-display axis position to animate (default: 0)
  --help                                         Show this help
"
    );
}

fn print_stage3_help() {
    println!(
        "\
imexplore_movie_stage3

Usage:
  cargo run --release -p casars --example imexplore_movie_stage3 -- [options]

Options:
  --image <path>                                 CASA image path (default: {DEFAULT_IMAGE_PATH})
  --output-dir <path>                            Output directory (default: {DEFAULT_OUTPUT_DIR})
  --target-fps <fps>                             Sustained benchmark target FPS (default: 30)
  --warmup-loops <n>                             Warmup loop count (default: 1)
  --measure-loops <n>                            Loop count for max/sustained phases (default: 3)
  --preview-workers <n>                          Preview worker pool size (default: 2)
  --render-workers <n>                           Render worker pool size (default: 4)
  --ready-buffer <n>                             Ready-frame buffer capacity (default: 32)
  --bitmap-width <px>                            Output bitmap width (default: {DEFAULT_BITMAP_WIDTH})
  --bitmap-height <px>                           Output bitmap height (default: {DEFAULT_BITMAP_HEIGHT})
  --axis-position <n>                            Non-display axis position to animate (default: 0)
  --help                                         Show this help
"
    );
}

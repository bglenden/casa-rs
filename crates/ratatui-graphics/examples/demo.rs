// SPDX-License-Identifier: LGPL-3.0-or-later
mod common;

use std::{
    env,
    io::{Stdout, Write, stdout},
    time::Duration,
};

use anyhow::Result;
use ratatui::{
    Frame, Terminal,
    backend::CrosstermBackend,
    crossterm::{
        event::{
            self, DisableMouseCapture, EnableMouseCapture, Event, KeyCode, KeyEventKind,
            MouseButton, MouseEvent, MouseEventKind,
        },
        execute,
        terminal::{EnterAlternateScreen, LeaveAlternateScreen, disable_raw_mode, enable_raw_mode},
    },
    layout::{Constraint, Direction, Layout, Rect},
    style::{Color, Modifier, Style},
    text::{Line, Span, Text},
    widgets::{Block, Borders, Paragraph, Wrap},
};
use ratatui_graphics::{
    ImageLayers, KittyLayerHandle, KittyLayerManager, KittyPlacement, PanelRenderer, Picker,
    Resize, TerminalCapabilities, apply_opacity, fit_pixels_preserving_aspect,
    prepare_image_layers,
};
use ratatui_image::Image as PanelImage;

use common::{
    DIRECT_PLOT_PIXEL_HEIGHT, DIRECT_PLOT_PIXEL_WIDTH, PLOT_ASPECT_HEIGHT, PLOT_ASPECT_WIDTH,
    PlotBackgroundPreset, ScientificPlotTheme, center_rect, plot_theme_for_preset, rect_contains,
    render_scientific_plot, scale_rect, terminal_background_or_default,
};

const DEFAULT_PANEL_WIDTH: u16 = 42;
const MIN_PANEL_WIDTH: u16 = 26;
const MIN_MAIN_WIDTH: u16 = 40;
const IDLE_POLL_INTERVAL_MS: u64 = 16;
const ACTIVE_POLL_INTERVAL_MS: u64 = 4;

fn main() -> Result<()> {
    let mut terminal = init_terminal()?;
    let app_result = run_app(&mut terminal);
    restore_terminal(&mut terminal)?;
    app_result
}

fn init_terminal() -> Result<Terminal<CrosstermBackend<Stdout>>> {
    enable_raw_mode()?;
    let mut stdout = stdout();
    execute!(stdout, EnterAlternateScreen, EnableMouseCapture)?;
    let backend = CrosstermBackend::new(stdout);
    let mut terminal = Terminal::new(backend)?;
    terminal.hide_cursor()?;
    Ok(terminal)
}

fn restore_terminal(terminal: &mut Terminal<CrosstermBackend<Stdout>>) -> Result<()> {
    disable_raw_mode()?;
    execute!(
        terminal.backend_mut(),
        LeaveAlternateScreen,
        DisableMouseCapture
    )?;
    terminal.show_cursor()?;
    Ok(())
}

fn run_app(terminal: &mut Terminal<CrosstermBackend<Stdout>>) -> Result<()> {
    let picker = Picker::from_query_stdio().unwrap_or_else(|_| Picker::halfblocks());
    let capabilities = TerminalCapabilities::from_picker(&picker);
    let terminal_background = terminal_background_or_default();
    let plot_background_preset = PlotBackgroundPreset::Terminal;
    let plot_theme = plot_theme_for_preset(plot_background_preset, terminal_background);
    let direct_plot = render_scientific_plot(
        DIRECT_PLOT_PIXEL_WIDTH,
        DIRECT_PLOT_PIXEL_HEIGHT,
        plot_theme,
    )?;
    let direct_plot_layers = prepare_image_layers(&direct_plot, plot_theme.background(), 8);

    let mut app = App::new(
        picker,
        capabilities,
        terminal_background,
        plot_background_preset,
        plot_theme,
        direct_plot_layers,
    )?;

    loop {
        let changed = app
            .panel_renderer
            .pump()
            .map_err(|err| anyhow::anyhow!(err.to_string()))?;
        if changed {
            app.panel_pixels = app.panel_renderer.image_size();
        }

        terminal.draw(|frame| app.draw(frame))?;

        if !app.dragging_divider {
            app.ensure_panel_render()?;
        }
        if app.graphics_dirty && !app.dragging_divider {
            app.refresh_direct_graphics()?;
            app.graphics_dirty = false;
        }

        let poll_interval = if app.dragging_divider || app.graphics_dirty {
            ACTIVE_POLL_INTERVAL_MS
        } else {
            IDLE_POLL_INTERVAL_MS
        };

        if event::poll(Duration::from_millis(poll_interval))? {
            match event::read()? {
                Event::Key(key)
                    if key.kind == KeyEventKind::Press && app.handle_key(key.code)? =>
                {
                    break;
                }
                Event::Mouse(mouse) => app.handle_mouse(mouse),
                Event::Resize(_, _) => app.graphics_dirty = true,
                _ => {}
            }
        }
    }

    app.clear_direct_graphics()?;
    Ok(())
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
struct UiLayout {
    header: Rect,
    footer: Rect,
    body: Rect,
    main: Rect,
    main_inner: Rect,
    divider: Rect,
    panel: Rect,
    panel_inner: Rect,
    overlay: Rect,
    underlay: Rect,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct PanelRequestKey {
    area: Rect,
    theme: ScientificPlotTheme,
}

struct App {
    picker: Picker,
    capabilities: TerminalCapabilities,
    terminal_background: [u8; 3],
    plot_background_preset: PlotBackgroundPreset,
    plot_theme: ScientificPlotTheme,
    direct_plot_layers: ImageLayers,
    panel_renderer: PanelRenderer<ScientificPlotTheme, anyhow::Error>,
    panel_request_key: Option<PanelRequestKey>,
    desired_panel_area: Rect,
    panel_pixels: Option<(u32, u32)>,
    panel_width: u16,
    dragging_divider: bool,
    overlay_enabled: bool,
    underlay_enabled: bool,
    cover_text_mode: bool,
    opaque_plot_background: bool,
    opacity_percent: u8,
    terminal_label: String,
    last_layout: UiLayout,
    graphics_dirty: bool,
    kitty_manager: Option<KittyLayerManager>,
    overlay_layer: Option<KittyLayerHandle>,
    underlay_layer: Option<KittyLayerHandle>,
}

impl App {
    fn new(
        picker: Picker,
        capabilities: TerminalCapabilities,
        terminal_background: [u8; 3],
        plot_background_preset: PlotBackgroundPreset,
        plot_theme: ScientificPlotTheme,
        direct_plot_layers: ImageLayers,
    ) -> Result<Self> {
        let panel_renderer = PanelRenderer::new(picker.clone(), Resize::Fit(None), |job| {
            let (pixel_width, pixel_height) = fit_pixels_preserving_aspect(
                job.max_pixel_width,
                job.max_pixel_height,
                PLOT_ASPECT_WIDTH,
                PLOT_ASPECT_HEIGHT,
            )?;
            render_scientific_plot(pixel_width.max(1), pixel_height.max(1), job.input)
        })?;

        let terminal_label = env::var("TERM_PROGRAM")
            .or_else(|_| env::var("TERM"))
            .unwrap_or_else(|_| "unknown".to_string());

        let mut kitty_manager = capabilities
            .direct_kitty_layers
            .then(KittyLayerManager::new);
        let overlay_layer = kitty_manager
            .as_mut()
            .map(KittyLayerManager::allocate)
            .transpose()?;
        let underlay_layer = kitty_manager
            .as_mut()
            .map(KittyLayerManager::allocate)
            .transpose()?;

        Ok(Self {
            picker,
            capabilities,
            terminal_background,
            plot_background_preset,
            plot_theme,
            direct_plot_layers,
            panel_renderer,
            panel_request_key: None,
            desired_panel_area: Rect::default(),
            panel_pixels: None,
            panel_width: DEFAULT_PANEL_WIDTH,
            dragging_divider: false,
            overlay_enabled: true,
            underlay_enabled: true,
            cover_text_mode: false,
            opaque_plot_background: false,
            opacity_percent: 58,
            terminal_label,
            last_layout: UiLayout::default(),
            graphics_dirty: true,
            kitty_manager,
            overlay_layer,
            underlay_layer,
        })
    }

    fn draw(&mut self, frame: &mut Frame<'_>) {
        let layout = self.compute_layout(frame.area());
        self.last_layout = layout;
        self.desired_panel_area = layout.panel_inner;

        frame.render_widget(self.header_widget(), layout.header);
        frame.render_widget(self.main_block(), layout.main);
        frame.render_widget(self.footer_widget(), layout.footer);
        frame.render_widget(self.divider_widget(), layout.divider);
        frame.render_widget(self.panel_block(), layout.panel);

        if !layout.main_inner.is_empty() {
            let paragraph = Paragraph::new(self.main_text())
                .wrap(Wrap { trim: false })
                .scroll((0, 0));
            frame.render_widget(paragraph, layout.main_inner);
        }

        if !layout.panel_inner.is_empty() {
            if let Some(protocol) = self.panel_renderer.protocol() {
                let image_area = center_rect(layout.panel_inner, protocol.area());
                frame.render_widget(PanelImage::new(protocol), image_area);
            }

            if self.panel_renderer.is_pending() && self.panel_renderer.protocol().is_none() {
                let pending =
                    Paragraph::new("Rendering plot...").style(Style::default().fg(Color::DarkGray));
                frame.render_widget(pending, layout.panel_inner);
            }
        }
    }

    fn ensure_panel_render(&mut self) -> Result<()> {
        let area = self.desired_panel_area;
        if area.is_empty() {
            return Ok(());
        }

        let request_key = PanelRequestKey {
            area,
            theme: self.plot_theme,
        };
        if self.panel_request_key == Some(request_key) {
            return Ok(());
        }

        let font_size = self.picker.font_size();
        self.panel_renderer.request(
            area,
            u32::from(area.width) * u32::from(font_size.0),
            u32::from(area.height) * u32::from(font_size.1),
            self.plot_theme,
        )?;
        self.panel_request_key = Some(request_key);
        Ok(())
    }

    fn handle_key(&mut self, code: KeyCode) -> Result<bool> {
        match code {
            KeyCode::Char('q') | KeyCode::Esc => Ok(true),
            KeyCode::Char('o') => {
                if self.capabilities.direct_kitty_layers {
                    self.overlay_enabled = !self.overlay_enabled;
                    self.graphics_dirty = true;
                }
                Ok(false)
            }
            KeyCode::Char('u') => {
                if self.capabilities.direct_kitty_layers {
                    self.underlay_enabled = !self.underlay_enabled;
                    self.graphics_dirty = true;
                }
                Ok(false)
            }
            KeyCode::Char('b') => {
                if self.capabilities.direct_kitty_layers {
                    self.cover_text_mode = !self.cover_text_mode;
                    self.graphics_dirty = true;
                }
                Ok(false)
            }
            KeyCode::Char('p') => {
                if self.capabilities.direct_kitty_layers {
                    self.opaque_plot_background = !self.opaque_plot_background;
                    self.graphics_dirty = true;
                }
                Ok(false)
            }
            KeyCode::Char('c') => {
                self.cycle_plot_background()?;
                Ok(false)
            }
            KeyCode::Char('+') | KeyCode::Char('=') => {
                self.opacity_percent = self.opacity_percent.saturating_add(5).min(100);
                self.graphics_dirty = true;
                Ok(false)
            }
            KeyCode::Char('-') | KeyCode::Char('_') => {
                self.opacity_percent = self.opacity_percent.saturating_sub(5).max(5);
                self.graphics_dirty = true;
                Ok(false)
            }
            _ => Ok(false),
        }
    }

    fn cycle_plot_background(&mut self) -> Result<()> {
        self.plot_background_preset = self.plot_background_preset.next();
        self.plot_theme =
            plot_theme_for_preset(self.plot_background_preset, self.terminal_background);
        let direct_plot = render_scientific_plot(
            DIRECT_PLOT_PIXEL_WIDTH,
            DIRECT_PLOT_PIXEL_HEIGHT,
            self.plot_theme,
        )?;
        self.direct_plot_layers =
            prepare_image_layers(&direct_plot, self.plot_theme.background(), 8);
        self.panel_request_key = None;
        self.graphics_dirty = true;
        Ok(())
    }

    fn handle_mouse(&mut self, mouse: MouseEvent) {
        match mouse.kind {
            MouseEventKind::Down(MouseButton::Left)
                if rect_contains(self.last_layout.divider, mouse.column, mouse.row) =>
            {
                self.dragging_divider = true;
            }
            MouseEventKind::Drag(MouseButton::Left) if self.dragging_divider => {
                self.update_panel_width(mouse.column);
            }
            MouseEventKind::Up(MouseButton::Left) => {
                self.dragging_divider = false;
                self.panel_request_key = None;
                self.graphics_dirty = true;
            }
            _ => {}
        }
    }

    fn compute_layout(&self, area: Rect) -> UiLayout {
        let vertical = Layout::default()
            .direction(Direction::Vertical)
            .constraints([
                Constraint::Length(4),
                Constraint::Min(12),
                Constraint::Length(3),
            ])
            .split(area);

        let body = vertical[1];
        let max_panel = body
            .width
            .saturating_sub(MIN_MAIN_WIDTH + 1)
            .max(MIN_PANEL_WIDTH);
        let panel_width = self.panel_width.clamp(MIN_PANEL_WIDTH, max_panel);
        let main_width = body.width.saturating_sub(panel_width + 1);

        let main = Rect::new(body.x, body.y, main_width, body.height);
        let divider = Rect::new(main.x + main.width, body.y, 1, body.height);
        let panel = Rect::new(divider.x + divider.width, body.y, panel_width, body.height);

        let main_block = self.main_block();
        let panel_block = self.panel_block();
        let main_inner = main_block.inner(main);
        let panel_inner = panel_block.inner(panel);

        let underlay = scale_rect(main_inner, 0.78, 0.60, 0.52, 0.28);
        let overlay = scale_rect(main_inner, 0.38, 0.32, 0.58, 0.07);

        UiLayout {
            header: vertical[0],
            footer: vertical[2],
            body,
            main,
            main_inner,
            divider,
            panel,
            panel_inner,
            overlay,
            underlay,
        }
    }

    fn header_widget(&self) -> Paragraph<'static> {
        let lines = vec![
            Line::from(vec![
                Span::styled(
                    "Ghostty graphics demo",
                    Style::default()
                        .fg(Color::Cyan)
                        .add_modifier(Modifier::BOLD),
                ),
                Span::raw("  "),
                Span::styled(
                    "plotters + ratatui-image + Kitty graphics",
                    Style::default().fg(Color::Gray),
                ),
            ]),
            Line::from(
                "Mouse drag the divider. `o` toggles an image above text, `u` toggles an image beneath text, `b` brings both plots on top and makes them opaque, `p` makes the floating plot background opaque, `c` cycles plot background themes, `+/-` changes alpha, `q` exits.",
            ),
            Line::from(format!(
                "Terminal: {} | ratatui-image protocol: {:?} | direct Kitty layers: {}",
                self.terminal_label,
                self.capabilities.panel_protocol,
                if self.capabilities.direct_kitty_layers {
                    "enabled"
                } else {
                    "unavailable"
                }
            )),
        ];

        Paragraph::new(lines).block(
            Block::default()
                .borders(Borders::ALL)
                .title("Controls")
                .border_style(Style::default().fg(Color::DarkGray)),
        )
    }

    fn main_block(&self) -> Block<'static> {
        Block::default()
            .borders(Borders::ALL)
            .title("Text surface")
            .border_style(Style::default().fg(Color::DarkGray))
    }

    fn panel_block(&self) -> Block<'static> {
        Block::default()
            .borders(Borders::ALL)
            .title("Scientific plot panel")
            .border_style(Style::default().fg(Color::Green))
    }

    fn divider_widget(&self) -> Paragraph<'static> {
        let style = if self.dragging_divider {
            Style::default().bg(Color::LightCyan).fg(Color::Black)
        } else {
            Style::default().bg(Color::DarkGray).fg(Color::Gray)
        };
        let fill = " ".repeat(self.last_layout.divider.height as usize);
        Paragraph::new(fill).style(style)
    }

    fn footer_widget(&self) -> Paragraph<'static> {
        let overlay = if self.overlay_enabled { "on" } else { "off" };
        let underlay = if self.underlay_enabled { "on" } else { "off" };
        let plot_mode = if self.cover_text_mode {
            "cover text"
        } else {
            "initial"
        };
        let plot_background = if self.opaque_plot_background {
            "opaque"
        } else {
            "transparent"
        };
        let plot_theme = self.plot_background_preset.label();
        let plot_dims = self
            .panel_pixels
            .map(|(w, h)| format!("{w}x{h}"))
            .unwrap_or_else(|| "pending".to_string());
        let lines = vec![
            Line::from(format!(
                "Panel width: {} cols | overlay: {} | underlay: {} | mode: {} | plot bg: {} | theme: {} | opacity: {}%",
                self.panel_width,
                overlay,
                underlay,
                plot_mode,
                plot_background,
                plot_theme,
                self.opacity_percent
            )),
            Line::from(format!(
                "Panel plot raster: {plot_dims} | Ghostty-specific layering uses Kitty z-index values above and below the text block."
            )),
        ];

        Paragraph::new(lines).block(
            Block::default()
                .borders(Borders::ALL)
                .title("Status")
                .border_style(Style::default().fg(Color::DarkGray)),
        )
    }

    fn main_text(&self) -> Text<'static> {
        let mut lines = vec![
            Line::from(vec![
                "Synthetic experiment: damped oscillation fit with sparse observations.".into(),
            ]),
            Line::from(vec![
                "The right panel shows the same plot rendered inside a ratatui-defined rectangle."
                    .into(),
            ]),
            Line::from(vec![
                "The left side is plain text so the Kitty graphics layers can sit above or below it."
                    .into(),
            ]),
            Line::from(""),
            Line::from(vec![
                Span::styled(
                    "Overlay",
                    Style::default()
                        .fg(Color::Yellow)
                        .add_modifier(Modifier::BOLD),
                ),
                Span::raw(": the plot is painted over the text with a positive z-index."),
            ]),
            Line::from(vec![
                Span::styled(
                    "Underlay",
                    Style::default()
                        .fg(Color::Green)
                        .add_modifier(Modifier::BOLD),
                ),
                Span::raw(": the same plot is painted under the text with a negative z-index."),
            ]),
            Line::from(vec![
                Span::styled(
                    "Opacity",
                    Style::default()
                        .fg(Color::Magenta)
                        .add_modifier(Modifier::BOLD),
                ),
                Span::raw(
                    ": alpha comes from the raster itself, so the app re-emits a new RGBA image when you change it.",
                ),
            ]),
            Line::from(""),
            Line::from("Notebook summary"),
            Line::from("  model: y(t) = exp(-0.12 t) sin(2.3 t) + 0.11 cos(5.4 t)"),
            Line::from("  window: 0.0 s to 18.0 s"),
            Line::from("  sample count: 24 observations"),
            Line::from("  residual std. dev.: 0.083"),
            Line::from(""),
            Line::from("Observations"),
            Line::from("  - The panel image stays clipped to the ratatui block as you drag the divider."),
            Line::from("  - The floating plot uses direct Kitty placement and ignores ratatui clipping."),
            Line::from("  - Ghostty handles the Kitty placeholder path well for the panel and z-index for the floating layers."),
            Line::from(""),
            Line::from("Lab note"),
            Line::from("  A transparent plot overlay is useful for thresholds, annotations, and transient diagnostics."),
            Line::from("  A translucent underlay is useful for heatmaps, fitted surfaces, or watermark-style reference data."),
            Line::from("  If you need richer compositing than this, pre-compose in the image before sending it."),
        ];

        lines.extend(
            [
                "",
                "Drag the splitter several times to force repeated ratatui layout changes while the direct Kitty layers stay independent.",
                "That difference is the main architectural distinction between the two rendering paths in Ghostty.",
            ]
            .into_iter()
            .map(Line::from),
        );

        Text::from(lines)
    }

    fn update_panel_width(&mut self, mouse_x: u16) {
        let body = self.last_layout.body;
        if body.width <= MIN_MAIN_WIDTH + MIN_PANEL_WIDTH + 1 {
            return;
        }

        let body_right = body.x + body.width;
        let clamped_x = mouse_x.clamp(body.x + MIN_MAIN_WIDTH, body_right - MIN_PANEL_WIDTH - 1);
        self.panel_width = body_right.saturating_sub(clamped_x + 1);
        self.graphics_dirty = true;
    }

    fn refresh_direct_graphics(&self) -> Result<()> {
        let (manager, overlay_layer, underlay_layer) =
            match (&self.kitty_manager, self.overlay_layer, self.underlay_layer) {
                (Some(manager), Some(overlay_layer), Some(underlay_layer))
                    if self.capabilities.direct_kitty_layers =>
                {
                    (manager, overlay_layer, underlay_layer)
                }
                _ => return Ok(()),
            };

        let (underlay_opacity, underlay_z) = if self.cover_text_mode {
            (100, 350)
        } else {
            (self.opacity_percent.saturating_mul(3) / 4, -200)
        };
        let (overlay_opacity, overlay_z) = if self.cover_text_mode {
            (100, 400)
        } else {
            (self.opacity_percent, 300)
        };
        let base_plot = if self.opaque_plot_background {
            &self.direct_plot_layers.opaque
        } else {
            &self.direct_plot_layers.keyed_background
        };

        let mut out = stdout();
        manager.clear_and_delete(&mut out, overlay_layer)?;
        manager.clear_and_delete(&mut out, underlay_layer)?;

        if self.underlay_enabled && !self.last_layout.underlay.is_empty() {
            let image = apply_opacity(base_plot, underlay_opacity);
            manager.upload_and_place_rgba(
                &mut out,
                underlay_layer,
                &image,
                KittyPlacement {
                    rect: self.last_layout.underlay,
                    z_index: underlay_z,
                    preserve_cursor: true,
                },
            )?;
        }

        if self.overlay_enabled && !self.last_layout.overlay.is_empty() {
            let image = apply_opacity(base_plot, overlay_opacity);
            manager.upload_and_place_rgba(
                &mut out,
                overlay_layer,
                &image,
                KittyPlacement {
                    rect: self.last_layout.overlay,
                    z_index: overlay_z,
                    preserve_cursor: true,
                },
            )?;
        }

        out.flush()?;
        Ok(())
    }

    fn clear_direct_graphics(&self) -> Result<()> {
        let (manager, overlay_layer, underlay_layer) =
            match (&self.kitty_manager, self.overlay_layer, self.underlay_layer) {
                (Some(manager), Some(overlay_layer), Some(underlay_layer))
                    if self.capabilities.direct_kitty_layers =>
                {
                    (manager, overlay_layer, underlay_layer)
                }
                _ => return Ok(()),
            };

        let mut out = stdout();
        manager.clear_and_delete(&mut out, overlay_layer)?;
        manager.clear_and_delete(&mut out, underlay_layer)?;
        out.flush()?;
        Ok(())
    }
}

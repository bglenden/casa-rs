// SPDX-License-Identifier: LGPL-3.0-or-later
use std::{
    env,
    io::{Stdout, Write, stdout},
    time::Duration,
};

use anyhow::Result;
use image::{Rgba, RgbaImage};
use ratatui::{
    Frame, Terminal,
    backend::CrosstermBackend,
    crossterm::{
        event::{self, DisableMouseCapture, EnableMouseCapture, Event, KeyCode, KeyEventKind},
        execute,
        terminal::{EnterAlternateScreen, LeaveAlternateScreen, disable_raw_mode, enable_raw_mode},
    },
    layout::{Constraint, Direction, Layout, Rect},
    style::{Color, Modifier, Style},
    text::{Line, Span, Text},
    widgets::{Block, Borders, Paragraph, Wrap},
};
use ratatui_graphics::{
    KittyLayerHandle, KittyLayerManager, KittyPlacement, Picker, TerminalCapabilities,
    apply_opacity,
};

const POLL_INTERVAL_MS: u64 = 16;

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
    let terminal_label = env::var("TERM_PROGRAM")
        .or_else(|_| env::var("TERM"))
        .unwrap_or_else(|_| "unknown".to_string());
    let mut app = App::new(capabilities, terminal_label)?;

    loop {
        terminal.draw(|frame| app.draw(frame))?;

        if app.graphics_dirty {
            app.refresh_overlay()?;
            app.graphics_dirty = false;
        }

        if event::poll(Duration::from_millis(POLL_INTERVAL_MS))? {
            match event::read()? {
                Event::Key(key) if key.kind == KeyEventKind::Press => match key.code {
                    KeyCode::Char('q') | KeyCode::Esc => break,
                    KeyCode::Char('+') | KeyCode::Char('=') => {
                        app.opacity_percent = app.opacity_percent.saturating_add(5).min(100);
                        app.graphics_dirty = true;
                    }
                    KeyCode::Char('-') | KeyCode::Char('_') => {
                        app.opacity_percent = app.opacity_percent.saturating_sub(5).max(5);
                        app.graphics_dirty = true;
                    }
                    _ => {}
                },
                Event::Resize(_, _) => app.graphics_dirty = true,
                _ => {}
            }
        }
    }

    app.clear_overlay()?;
    Ok(())
}

struct App {
    capabilities: TerminalCapabilities,
    terminal_label: String,
    opacity_percent: u8,
    last_overlay: Rect,
    graphics_dirty: bool,
    kitty_manager: Option<KittyLayerManager>,
    overlay_layer: Option<KittyLayerHandle>,
}

impl App {
    fn new(capabilities: TerminalCapabilities, terminal_label: String) -> Result<Self> {
        let mut kitty_manager = capabilities
            .direct_kitty_layers
            .then(KittyLayerManager::new);
        let overlay_layer = kitty_manager
            .as_mut()
            .map(KittyLayerManager::allocate)
            .transpose()?;

        Ok(Self {
            capabilities,
            terminal_label,
            opacity_percent: 62,
            last_overlay: Rect::default(),
            graphics_dirty: true,
            kitty_manager,
            overlay_layer,
        })
    }

    fn draw(&mut self, frame: &mut Frame<'_>) {
        let layout = Layout::default()
            .direction(Direction::Vertical)
            .constraints([
                Constraint::Length(4),
                Constraint::Min(10),
                Constraint::Length(3),
            ])
            .split(frame.area());

        let header = Paragraph::new(format!(
            "Direct Kitty overlay example. Press +/- to change opacity, q to exit.\nTerminal: {} | panel protocol: {:?} | direct Kitty layers: {}",
            self.terminal_label,
            self.capabilities.panel_protocol,
            if self.capabilities.direct_kitty_layers {
                "enabled"
            } else {
                "unavailable"
            },
        ))
        .block(
            Block::default()
                .borders(Borders::ALL)
                .title("ratatui-graphics Kitty example")
                .border_style(Style::default().fg(Color::DarkGray)),
        );
        frame.render_widget(header, layout[0]);

        let body_block = Block::default()
            .borders(Borders::ALL)
            .title("Text surface")
            .border_style(Style::default().fg(Color::DarkGray));
        let body_inner = body_block.inner(layout[1]);
        frame.render_widget(body_block, layout[1]);
        self.last_overlay = scale_rect(body_inner, 0.68, 0.40, 0.18, 0.30);

        if !body_inner.is_empty() {
            let text = Text::from(vec![
                Line::from(vec![
                    Span::styled(
                        "Kitty overlays are outside ratatui's clipping model. ",
                        Style::default().add_modifier(Modifier::BOLD),
                    ),
                    Span::raw(
                        "This example paints a translucent diagnostic layer over the text block.",
                    ),
                ]),
                Line::from(""),
                Line::from(
                    "The library now exposes KittyLayerManager and KittyLayerHandle so the app no longer has to manage raw image ids manually.",
                ),
                Line::from(""),
                Line::from(
                    "If direct Kitty layers are unavailable, the text remains and no overlay is emitted.",
                ),
            ]);
            frame.render_widget(Paragraph::new(text).wrap(Wrap { trim: false }), body_inner);
        }

        let footer = Paragraph::new(format!(
            "Overlay opacity: {}% | overlay rect: {}x{} cells",
            self.opacity_percent, self.last_overlay.width, self.last_overlay.height
        ))
        .block(
            Block::default()
                .borders(Borders::ALL)
                .title("Status")
                .border_style(Style::default().fg(Color::DarkGray)),
        );
        frame.render_widget(footer, layout[2]);
    }

    fn refresh_overlay(&self) -> Result<()> {
        let (manager, handle) = match (&self.kitty_manager, self.overlay_layer) {
            (Some(manager), Some(handle)) if self.capabilities.direct_kitty_layers => {
                (manager, handle)
            }
            _ => return Ok(()),
        };

        let mut out = stdout();
        manager.clear_and_delete(&mut out, handle)?;
        if self.last_overlay.is_empty() {
            out.flush()?;
            return Ok(());
        }

        let image = apply_opacity(&generate_overlay_image(640, 280), self.opacity_percent);
        manager.upload_and_place_rgba(
            &mut out,
            handle,
            &image,
            KittyPlacement {
                rect: self.last_overlay,
                z_index: 320,
                preserve_cursor: true,
            },
        )?;
        out.flush()?;
        Ok(())
    }

    fn clear_overlay(&self) -> Result<()> {
        let (manager, handle) = match (&self.kitty_manager, self.overlay_layer) {
            (Some(manager), Some(handle)) if self.capabilities.direct_kitty_layers => {
                (manager, handle)
            }
            _ => return Ok(()),
        };
        let mut out = stdout();
        manager.clear_and_delete(&mut out, handle)?;
        out.flush()?;
        Ok(())
    }
}

fn scale_rect(area: Rect, width_scale: f32, height_scale: f32, x_scale: f32, y_scale: f32) -> Rect {
    if area.is_empty() {
        return Rect::default();
    }

    let width = ((area.width as f32) * width_scale).round() as u16;
    let height = ((area.height as f32) * height_scale).round() as u16;
    let width = width.clamp(1, area.width);
    let height = height.clamp(1, area.height);

    let max_x = area.width.saturating_sub(width);
    let max_y = area.height.saturating_sub(height);
    let x_offset = ((max_x as f32) * x_scale).round() as u16;
    let y_offset = ((max_y as f32) * y_scale).round() as u16;

    Rect::new(area.x + x_offset, area.y + y_offset, width, height)
}

fn generate_overlay_image(width: u32, height: u32) -> RgbaImage {
    let mut image = RgbaImage::new(width, height);
    for (x, y, pixel) in image.enumerate_pixels_mut() {
        let wave =
            (((x as f32 / width as f32) * std::f32::consts::TAU * 2.0).sin() * 0.5 + 0.5) * 255.0;
        let band =
            (((y as f32 / height as f32) * std::f32::consts::TAU * 3.0).cos() * 0.5 + 0.5) * 255.0;
        let alpha = if (x + y) % 21 < 10 { 180 } else { 70 };
        *pixel = Rgba([wave as u8, band as u8, 220, alpha]);
    }
    image
}

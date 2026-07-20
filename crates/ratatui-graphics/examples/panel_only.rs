// SPDX-License-Identifier: LGPL-3.0-or-later
mod common;

use std::{
    io::{Stdout, stdout},
    time::Duration,
};

use anyhow::Result;
use ratatui::{
    Frame, Terminal,
    backend::CrosstermBackend,
    crossterm::{
        event::{self, DisableMouseCapture, EnableMouseCapture, Event, KeyCode, KeyEventKind},
        execute,
        terminal::{EnterAlternateScreen, LeaveAlternateScreen, disable_raw_mode, enable_raw_mode},
    },
    layout::{Constraint, Direction, Layout, Rect},
    style::{Color, Style},
    widgets::{Block, Borders, Paragraph},
};
use ratatui_graphics::{
    PanelProtocolRenderError, PanelProtocolRequest, PanelSchedulePolicy, PanelScheduler, Picker,
    PreparedPanelProtocol, Resize, fit_pixels_preserving_aspect, render_panel_protocol,
};
use ratatui_image::Image as PanelImage;

use common::{
    PLOT_ASPECT_HEIGHT, PLOT_ASPECT_WIDTH, ScientificPlotTheme, center_rect,
    render_scientific_plot, terminal_background_or_default,
};

const POLL_INTERVAL_MS: u64 = 16;
type PlotPanelScheduler = PanelScheduler<
    PanelProtocolRequest<ScientificPlotTheme>,
    PreparedPanelProtocol,
    PanelProtocolRenderError<anyhow::Error>,
>;

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
    let theme = ScientificPlotTheme::from_background(terminal_background_or_default());
    let mut app = App::new(picker, theme)?;

    loop {
        let changed = app
            .panel_renderer
            .pump_latest(1)
            .map_err(|err| anyhow::anyhow!(err.to_string()))?;
        if changed {
            app.panel_pixels = app.panel_renderer.latest().map(|completion| {
                (
                    completion.output.image_width,
                    completion.output.image_height,
                )
            });
        }

        terminal.draw(|frame| app.draw(frame))?;
        app.ensure_panel_render()?;

        if event::poll(Duration::from_millis(POLL_INTERVAL_MS))? {
            match event::read()? {
                Event::Key(key) if key.kind == KeyEventKind::Press => match key.code {
                    KeyCode::Char('q') | KeyCode::Esc => break,
                    _ => {}
                },
                Event::Resize(_, _) => app.panel_request_key = None,
                _ => {}
            }
        }
    }

    Ok(())
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct PanelRequestKey {
    area: Rect,
    theme: ScientificPlotTheme,
}

struct App {
    picker: Picker,
    theme: ScientificPlotTheme,
    panel_renderer: PlotPanelScheduler,
    desired_panel_area: Rect,
    panel_request_key: Option<PanelRequestKey>,
    panel_pixels: Option<(u32, u32)>,
}

impl App {
    fn new(picker: Picker, theme: ScientificPlotTheme) -> Result<Self> {
        let worker_picker = picker.clone();
        let panel_renderer = PanelScheduler::new(PanelSchedulePolicy::LatestWins, move |job| {
            render_panel_protocol(&worker_picker, Resize::Fit(None), job, |request| {
                let (pixel_width, pixel_height) = fit_pixels_preserving_aspect(
                    request.max_pixel_width,
                    request.max_pixel_height,
                    PLOT_ASPECT_WIDTH,
                    PLOT_ASPECT_HEIGHT,
                )?;
                render_scientific_plot(pixel_width.max(1), pixel_height.max(1), request.input)
            })
        })?;

        Ok(Self {
            picker,
            theme,
            panel_renderer,
            desired_panel_area: Rect::default(),
            panel_request_key: None,
            panel_pixels: None,
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

        let header = Paragraph::new(
            "Panel-only example: the plot is rendered into a ratatui-defined panel via PanelScheduler.\nResize the terminal to request a new plot raster. Press q to exit.",
        )
        .block(
            Block::default()
                .borders(Borders::ALL)
                .title("ratatui-graphics panel example")
                .border_style(Style::default().fg(Color::DarkGray)),
        );
        frame.render_widget(header, layout[0]);

        let panel_block = Block::default()
            .borders(Borders::ALL)
            .title("Scientific plot")
            .border_style(Style::default().fg(Color::Green));
        let panel_inner = panel_block.inner(layout[1]);
        self.desired_panel_area = panel_inner;
        frame.render_widget(panel_block, layout[1]);

        if !panel_inner.is_empty() {
            if let Some(protocol) = self
                .panel_renderer
                .latest()
                .and_then(|completion| completion.output.protocol.as_ref())
            {
                let image_area = center_rect(panel_inner, protocol.area());
                frame.render_widget(PanelImage::new(protocol), image_area);
            } else if self.panel_renderer.is_pending() {
                let pending =
                    Paragraph::new("Rendering plot...").style(Style::default().fg(Color::DarkGray));
                frame.render_widget(pending, panel_inner);
            }
        }

        let panel_pixels = self
            .panel_pixels
            .map(|(w, h)| format!("{w}x{h}"))
            .unwrap_or_else(|| "pending".to_string());
        let footer = Paragraph::new(format!(
            "Protocol: {:?} | font size: {:?} | plot raster: {panel_pixels}",
            self.picker.protocol_type(),
            self.picker.font_size(),
        ))
        .block(
            Block::default()
                .borders(Borders::ALL)
                .title("Status")
                .border_style(Style::default().fg(Color::DarkGray)),
        );
        frame.render_widget(footer, layout[2]);
    }

    fn ensure_panel_render(&mut self) -> Result<()> {
        let area = self.desired_panel_area;
        if area.is_empty() {
            return Ok(());
        }

        let request_key = PanelRequestKey {
            area,
            theme: self.theme,
        };
        if self.panel_request_key == Some(request_key) {
            return Ok(());
        }

        let font_size = self.picker.font_size();
        self.panel_renderer.submit(
            1,
            0,
            PanelProtocolRequest {
                area,
                max_pixel_width: u32::from(area.width) * u32::from(font_size.0),
                max_pixel_height: u32::from(area.height) * u32::from(font_size.1),
                build_protocol: true,
                input: self.theme,
            },
        )?;
        self.panel_request_key = Some(request_key);
        Ok(())
    }
}

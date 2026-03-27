// SPDX-License-Identifier: LGPL-3.0-or-later
#![allow(dead_code)]

use std::time::Duration;

use anyhow::Result;
use image::DynamicImage;
use plotters::prelude::*;
use plotters::style::Color as PlottersColor;
use ratatui::layout::Rect;
use ratatui_graphics::PlottersBitmap;
#[cfg(feature = "terminal-detect")]
use ratatui_graphics::detect_terminal_background;

pub const PLOT_ASPECT_WIDTH: u32 = 14;
pub const PLOT_ASPECT_HEIGHT: u32 = 9;
pub const DIRECT_PLOT_PIXEL_WIDTH: u32 = 1400;
pub const DIRECT_PLOT_PIXEL_HEIGHT: u32 = 900;
pub const DEFAULT_TERMINAL_BACKGROUND: [u8; 3] = [250, 249, 246];
pub const DEFAULT_BG_QUERY_TIMEOUT: Duration = Duration::from_millis(120);

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ScientificPlotTheme {
    background: [u8; 3],
    axis_text: [u8; 3],
    axis_line: [u8; 3],
    label_text: [u8; 3],
    grid_light: [u8; 3],
    grid_bold: [u8; 3],
    legend_fill: [u8; 3],
    legend_border: [u8; 3],
    fit_line: [u8; 3],
    reference_line: [u8; 3],
    observation_point: [u8; 3],
}

impl ScientificPlotTheme {
    pub fn from_background(background: [u8; 3]) -> Self {
        let dark_background = perceived_luminance(background) < 0.52;

        if dark_background {
            Self {
                background,
                axis_text: mix_rgb(background, [255, 255, 255], 0.88),
                axis_line: mix_rgb(background, [255, 255, 255], 0.42),
                label_text: mix_rgb(background, [255, 255, 255], 0.70),
                grid_light: mix_rgb(background, [255, 255, 255], 0.12),
                grid_bold: mix_rgb(background, [255, 255, 255], 0.22),
                legend_fill: mix_rgb(background, [255, 255, 255], 0.08),
                legend_border: mix_rgb(background, [255, 255, 255], 0.24),
                fit_line: [105, 170, 255],
                reference_line: [132, 205, 154],
                observation_point: [255, 128, 118],
            }
        } else {
            Self {
                background,
                axis_text: mix_rgb(background, [0, 0, 0], 0.82),
                axis_line: mix_rgb(background, [0, 0, 0], 0.40),
                label_text: mix_rgb(background, [0, 0, 0], 0.68),
                grid_light: mix_rgb(background, [0, 0, 0], 0.10),
                grid_bold: mix_rgb(background, [0, 0, 0], 0.18),
                legend_fill: mix_rgb(background, [255, 255, 255], 0.32),
                legend_border: mix_rgb(background, [0, 0, 0], 0.20),
                fit_line: [30, 78, 162],
                reference_line: [73, 133, 96],
                observation_point: [191, 67, 57],
            }
        }
    }

    pub fn background(self) -> [u8; 3] {
        self.background
    }

    pub fn background_color(self) -> RGBColor {
        rgb_color(self.background)
    }

    pub fn axis_text_color(self) -> RGBColor {
        rgb_color(self.axis_text)
    }

    pub fn axis_line_color(self) -> RGBColor {
        rgb_color(self.axis_line)
    }

    pub fn label_text_color(self) -> RGBColor {
        rgb_color(self.label_text)
    }

    pub fn grid_light_color(self) -> RGBColor {
        rgb_color(self.grid_light)
    }

    pub fn grid_bold_color(self) -> RGBColor {
        rgb_color(self.grid_bold)
    }

    pub fn legend_fill_color(self) -> RGBColor {
        rgb_color(self.legend_fill)
    }

    pub fn legend_border_color(self) -> RGBColor {
        rgb_color(self.legend_border)
    }

    pub fn fit_line_color(self) -> RGBColor {
        rgb_color(self.fit_line)
    }

    pub fn reference_line_color(self) -> RGBColor {
        rgb_color(self.reference_line)
    }

    pub fn observation_point_color(self) -> RGBColor {
        rgb_color(self.observation_point)
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PlotBackgroundPreset {
    Terminal,
    WarmPaper,
    Mist,
    Graphite,
    Moss,
}

impl PlotBackgroundPreset {
    pub fn next(self) -> Self {
        match self {
            Self::Terminal => Self::WarmPaper,
            Self::WarmPaper => Self::Mist,
            Self::Mist => Self::Graphite,
            Self::Graphite => Self::Moss,
            Self::Moss => Self::Terminal,
        }
    }

    pub fn label(self) -> &'static str {
        match self {
            Self::Terminal => "terminal",
            Self::WarmPaper => "warm paper",
            Self::Mist => "mist",
            Self::Graphite => "graphite",
            Self::Moss => "moss",
        }
    }

    pub fn background(self, terminal_background: [u8; 3]) -> [u8; 3] {
        match self {
            Self::Terminal => terminal_background,
            Self::WarmPaper => [243, 237, 226],
            Self::Mist => [228, 236, 244],
            Self::Graphite => [34, 40, 52],
            Self::Moss => [226, 235, 224],
        }
    }
}

pub fn terminal_background_or_default() -> [u8; 3] {
    #[cfg(feature = "terminal-detect")]
    {
        detect_terminal_background(DEFAULT_BG_QUERY_TIMEOUT).unwrap_or(DEFAULT_TERMINAL_BACKGROUND)
    }

    #[cfg(not(feature = "terminal-detect"))]
    {
        DEFAULT_TERMINAL_BACKGROUND
    }
}

pub fn plot_theme_for_preset(
    plot_background_preset: PlotBackgroundPreset,
    terminal_background: [u8; 3],
) -> ScientificPlotTheme {
    ScientificPlotTheme::from_background(plot_background_preset.background(terminal_background))
}

pub fn render_scientific_plot(
    width: u32,
    height: u32,
    plot_theme: ScientificPlotTheme,
) -> Result<DynamicImage> {
    let mut canvas = PlottersBitmap::new(width, height)?;
    let root = canvas.backend().into_drawing_area();
    let axis_text = plot_theme.axis_text_color();
    let axis_line = plot_theme.axis_line_color();
    let label_text = plot_theme.label_text_color();
    let grid_light = plot_theme.grid_light_color();
    let grid_bold = plot_theme.grid_bold_color();
    let fit_line = plot_theme.fit_line_color();
    let reference_line = plot_theme.reference_line_color();
    let observation_point = plot_theme.observation_point_color();
    let legend_fill = plot_theme.legend_fill_color();
    let legend_border = plot_theme.legend_border_color();

    root.fill(&plot_theme.background_color())?;

    let mut chart = ChartBuilder::on(&root)
        .margin(22)
        .x_label_area_size(44)
        .y_label_area_size(56)
        .build_cartesian_2d(0.0_f64..18.0_f64, -1.4_f64..1.4_f64)?;

    chart
        .configure_mesh()
        .x_desc("time (s)")
        .y_desc("signal amplitude")
        .axis_desc_style(("sans-serif", 24).into_font().color(&axis_text))
        .axis_style(axis_line)
        .label_style(("sans-serif", 18).into_font().color(&label_text))
        .light_line_style(grid_light)
        .bold_line_style(grid_bold)
        .draw()?;

    let model = (0..600).map(|idx| {
        let x = 18.0 * (idx as f64) / 599.0;
        (x, scientific_signal(x))
    });

    let reference = (0..600).map(|idx| {
        let x = 18.0 * (idx as f64) / 599.0;
        (x, 0.86 * (-0.09 * x).exp() * (2.05 * x + 0.22).sin())
    });

    let observations: Vec<(f64, f64)> = (0..24)
        .map(|idx| {
            let x = 0.55 + (idx as f64) * 0.72;
            let perturbation = 0.08 * (x * 1.7).cos() - 0.04 * (x * 0.35).sin();
            (x, scientific_signal(x) + perturbation)
        })
        .collect();

    chart
        .draw_series(LineSeries::new(model, fit_line.stroke_width(4)))?
        .label("fit")
        .legend(|(x, y)| PathElement::new(vec![(x, y), (x + 22, y)], fit_line.stroke_width(4)));

    chart
        .draw_series(LineSeries::new(
            reference,
            reference_line.mix(0.75).stroke_width(2),
        ))?
        .label("reference")
        .legend(|(x, y)| {
            PathElement::new(
                vec![(x, y), (x + 22, y)],
                reference_line.mix(0.75).stroke_width(2),
            )
        });

    chart
        .draw_series(PointSeries::of_element(
            observations.iter().copied(),
            6,
            observation_point.filled(),
            &|coord, size, style| {
                EmptyElement::at(coord)
                    + Circle::new((0, 0), size, style)
                    + Circle::new((0, 0), size + 3, observation_point.mix(0.20))
            },
        ))?
        .label("observations")
        .legend(|(x, y)| Circle::new((x + 11, y), 5, observation_point.filled()));

    chart
        .configure_series_labels()
        .background_style(legend_fill)
        .border_style(legend_border)
        .label_font(("sans-serif", 18).into_font().color(&label_text))
        .draw()?;

    drop(chart);
    root.present()?;
    drop(root);

    canvas.into_dynamic_image().map_err(Into::into)
}

pub fn rect_contains(rect: Rect, x: u16, y: u16) -> bool {
    x >= rect.x && y >= rect.y && x < rect.x + rect.width && y < rect.y + rect.height
}

pub fn scale_rect(
    area: Rect,
    width_scale: f32,
    height_scale: f32,
    x_scale: f32,
    y_scale: f32,
) -> Rect {
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

pub fn center_rect(container: Rect, content: Rect) -> Rect {
    let width = content.width.min(container.width);
    let height = content.height.min(container.height);
    let x = container.x + container.width.saturating_sub(width) / 2;
    let y = container.y + container.height.saturating_sub(height) / 2;
    Rect::new(x, y, width, height)
}

fn scientific_signal(x: f64) -> f64 {
    (-0.12 * x).exp() * (2.30 * x).sin() + 0.11 * (5.40 * x).cos()
}

fn perceived_luminance(color: [u8; 3]) -> f32 {
    (0.299 * f32::from(color[0]) + 0.587 * f32::from(color[1]) + 0.114 * f32::from(color[2]))
        / 255.0
}

fn mix_rgb(base: [u8; 3], target: [u8; 3], amount: f32) -> [u8; 3] {
    let blend = amount.clamp(0.0, 1.0);
    [
        mix_channel(base[0], target[0], blend),
        mix_channel(base[1], target[1], blend),
        mix_channel(base[2], target[2], blend),
    ]
}

fn mix_channel(base: u8, target: u8, amount: f32) -> u8 {
    let mixed = f32::from(base) + (f32::from(target) - f32::from(base)) * amount;
    mixed.round().clamp(0.0, 255.0) as u8
}

fn rgb_color(color: [u8; 3]) -> RGBColor {
    RGBColor(color[0], color[1], color[2])
}

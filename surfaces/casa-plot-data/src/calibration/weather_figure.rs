// SPDX-License-Identifier: LGPL-3.0-or-later
//! Prepare the five panels of the CASA weather summary for the shared renderer.

use std::f64::consts::FRAC_PI_2;
use std::path::PathBuf;
use std::sync::Arc;

use casa_calibration::weather::{WeatherPlotData, weather_date};
use casa_measures_data::{MeasuresRuntime, PlanetaryTable, RuntimePolicy};
use casa_types::measures::direction::{DirectionRef, MDirection};
use casa_types::measures::{EpochRef, MEpoch, MPosition, MeasFrame};
use casa_types::quanta::Quantity;
use serde::Serialize;

use crate::plot_document::{
    PlotAxisScale, PlotDocument, PlotDocumentAnnotation, PlotDocumentAxis, PlotDocumentLayer,
    PlotDocumentPanel, PlotLayerKind, PlotLayerStyle, PlotPayloadStrategy, PlotTextStyle,
};

/// Five-panel weather summary document and rendering metadata for `plotweather`.
#[derive(Serialize)]
pub struct WeatherFigure {
    /// Output plot image path.
    pub output_file: PathBuf,
    /// Shared five-panel weather plot document.
    pub document: PlotDocument,
    /// X-axis tick coordinates.
    pub time_ticks: [f64; 3],
    /// Formatted tick labels.
    pub time_labels: [String; 3],
    /// Wind vector arrow rotation angles in degrees.
    pub wind_rotations: Vec<f64>,
}

/// Build a five-panel `plotweather` figure from station telemetry and opacity spectra.
pub fn prepare(
    data: &WeatherPlotData,
    vis: &str,
    plot_name: &str,
) -> Result<WeatherFigure, String> {
    const UNIX_EPOCH_MJD: f64 = 40_587.0;
    let dates = data
        .time_seconds
        .iter()
        .map(|&seconds| weather_date(seconds))
        .collect::<Vec<_>>();
    let time = dates
        .iter()
        .map(|date| Quantity::read_at(date, 0.0).map(|days| days.value() - UNIX_EPOCH_MJD))
        .collect::<Result<Vec<_>, _>>()
        .map_err(|error| error.to_string())?;
    let first = time[0];
    let last = time[time.len() - 1];
    let pad = (last - first) * 0.03;
    let xlim = [first - pad, last + pad];
    let [earliest, latest] = bounds(&time);
    let time_ticks = [earliest, (earliest + latest) * 0.5, latest];
    let [first_label, mid, end] = [0, dates.len() / 2, dates.len() - 1].map(|row| &dates[row]);
    let time_labels = [
        first_label.to_owned(),
        mid.get(11..).unwrap_or_default().into(),
        end.get(11..).unwrap_or_default().into(),
    ];

    let mut sun = solar_panel(&time, &data.time_seconds, xlim)?;
    sun.title = format!("Weather Summary for {vis}");
    let (wind, wind_rotations) = wind_panel(&time, data, xlim)?;
    let temperature = panel(
        "temperature",
        "",
        "temp,dew",
        xlim,
        bounds_all(&[&data.temperature_c, &data.dew_point_c]),
        vec![
            line("temperature", "", &time, &data.temperature_c, "#FF6600"),
            line("dew-point", "", &time, &data.dew_point_c, "#006666"),
        ],
    );
    let pwv = [
        data.seasonal_water_mm.as_slice(),
        &data.station_water_mm,
        &data.weighted_water_mm,
    ];
    let [season, station, blend] = pwv.map(|values| ordered_bounds(values.iter().copied()));
    let [min, _] = ordered_bounds([blend[0], station[0], season[0]].into_iter());
    let [_, max] = ordered_bounds([blend[1], station[1], season[1]].into_iter());
    let water_limits = finite_limits([0.8 * min, 1.2 * max])?;
    let water = panel(
        "water",
        "",
        "PWV (mm)",
        xlim,
        water_limits,
        models(&time, pwv),
    );
    let opacity = panel(
        "opacity",
        "Frequency (GHz)",
        "Tau_Z (nepers)",
        bounds(&data.frequency_ghz),
        [0.0, 0.25],
        models(
            &data.frequency_ghz,
            [
                &data.seasonal_opacity,
                &data.station_opacity,
                &data.weighted_opacity,
            ],
        ),
    );
    let path = if !plot_name.is_empty() {
        plot_name.to_owned()
    } else if vis.ends_with('/') {
        format!("{}{}.plotweather.png", vis, vis.trim_end_matches('/'))
    } else {
        format!("{vis}.plotweather.png")
    };
    let document = PlotDocument {
        id: "plotweather".into(),
        title: String::new(),
        subtitle: String::new(),
        header_lines: Vec::new(),
        axes: Vec::new(),
        layers: Vec::new(),
        annotations: Vec::new(),
        panels: vec![sun, wind, temperature, water, opacity],
        show_legend: false,
        layout: None,
    };
    document
        .validate_payload(0)
        .map_err(|error| error.to_string())?;
    Ok(WeatherFigure {
        output_file: path.into(),
        document,
        time_ticks,
        time_labels,
        wind_rotations,
    })
}

fn solar_panel(time: &[f64], seconds: &[f64], xlim: [f64; 2]) -> Result<PlotDocumentPanel, String> {
    let measures = Arc::new(
        MeasuresRuntime::open_discovered(RuntimePolicy::default())
            .map_err(|error| error.to_string())?,
    );
    let position = MPosition::casa_observatory("VLA", measures.as_ref())
        .ok_or("VLA is missing from the observatory catalog")?;
    let frame = MeasFrame::new()
        .with_position(position)
        .with_measures(measures);
    let ephemeris = PlanetaryTable::open_discovered().map_err(|error| error.to_string())?;
    let sun = MDirection::from_angles(0.0, FRAC_PI_2, DirectionRef::SUN);
    let mut layers = Vec::with_capacity(31);
    for (point, row) in samples(time.len(), 30).enumerate() {
        let frame = frame
            .clone()
            .with_epoch(MEpoch::from_mjd_seconds(seconds[row], EpochRef::UTC));
        let (direction, distance) = ephemeris
            .geocentric_direction(&sun, &frame)
            .map_err(|error| error.to_string())?;
        let height = direction
            .convert_with_parallax(DirectionRef::AZELGEO, &frame, distance)
            .map_err(|error| error.to_string())?
            .latitude_rad()
            .to_degrees();
        let color = if height < 0.0 { "black" } else { "#FFCC00" };
        let mut markers = line(
            &format!("sun-{point}"),
            "",
            &[time[row]; 2],
            &[height; 2],
            color,
        );
        markers.line_width = 1.5;
        let style = markers.style.as_mut().expect("weather marker style");
        style.marker = Some("H".into());
        style.line_style = Some("None".into());
        layers.push(markers);
    }
    let mut horizon = line(
        "horizon",
        "",
        &[time[0], time[time.len() - 1]],
        &[0.0; 2],
        "brown",
    );
    horizon.line_width = 1.5;
    layers.push(horizon);
    Ok(panel("solar", "", "solar el", xlim, [-90.0, 90.0], layers))
}

fn wind_panel(
    time: &[f64],
    data: &WeatherPlotData,
    xlim: [f64; 2],
) -> Result<(PlotDocumentPanel, Vec<f64>), String> {
    let [lower, upper] = ordered_bounds(data.wind_m_s.iter().copied());
    let range = upper - lower;
    let x_offset = -(time[time.len() - 1] - time[0]) * 0.01;
    let y_offset = -range * 0.08;
    let mut invisible_marker = line(
        "wind-speed",
        "",
        time,
        &data
            .wind_m_s
            .iter()
            .map(|value| value + 0.3)
            .collect::<Vec<_>>(),
        "black",
    );
    invisible_marker.opacity = 0.0;
    invisible_marker.line_width = 1.5;
    let style = invisible_marker
        .style
        .as_mut()
        .expect("weather marker style");
    style.marker = Some(".".into());
    style.line_style = Some("None".into());
    let mut panel = panel(
        "wind",
        "",
        "wind (m/s)",
        xlim,
        finite_limits([lower - range * 0.03, upper + range * 0.03])?,
        vec![invisible_marker],
    );
    let mut rotations = Vec::with_capacity(60);
    for (point, row) in samples(time.len(), 60).enumerate() {
        rotations.push(data.wind_arrow_degrees[row]);
        panel.annotations.push(PlotDocumentAnnotation {
            id: format!("wind-{point}"),
            x: time[row] + x_offset,
            y: data.wind_m_s[row] + y_offset,
            text: "-->".into(),
            style: Some(PlotTextStyle {
                axes_relative: false,
                font_size: Some("12".into()),
                horizontal_alignment: None,
                vertical_alignment: None,
                background: None,
            }),
        });
    }
    Ok((panel, rotations))
}

fn samples(length: usize, count: usize) -> impl Iterator<Item = usize> {
    (0..count).map(move |point| point * (length - 1) / (count - 1))
}

fn models(x: &[f64], values: [&[f64]; 3]) -> Vec<PlotDocumentLayer> {
    [
        ("seasonal", "seasonal model", "#A6A6A6"),
        ("station", "weather station", "#4D4DFF"),
        ("weighted", "weighted", "#92B5F2"),
    ]
    .into_iter()
    .zip(values)
    .map(|((id, label, color), y)| line(id, label, x, y, color))
    .collect()
}

fn bounds(values: &[f64]) -> [f64; 2] {
    bounds_all(&[values])
}

fn bounds_all(values: &[&[f64]]) -> [f64; 2] {
    values
        .iter()
        .flat_map(|values| values.iter())
        .copied()
        .fold(
            [f64::INFINITY, f64::NEG_INFINITY],
            |[lower, upper], value| [lower.min(value), upper.max(value)],
        )
}

// Python's min/max compare each new element to the current extremum, so a
// NaN in the first position remains the extremum, while later NaNs are skipped.
fn ordered_bounds(mut values: impl Iterator<Item = f64>) -> [f64; 2] {
    let first = values.next().expect("nonempty weather data");
    values.fold([first, first], |[lower, upper], value| {
        [
            if value < lower { value } else { lower },
            if value > upper { value } else { upper },
        ]
    })
}

fn finite_limits(limits: [f64; 2]) -> Result<[f64; 2], String> {
    if limits.iter().all(|limit| limit.is_finite()) {
        Ok(limits)
    } else {
        Err("Axis limits cannot be NaN or Inf".into())
    }
}

fn axis(id: &str, label: &str, [lower, upper]: [f64; 2]) -> PlotDocumentAxis {
    PlotDocumentAxis {
        id: id.into(),
        label: label.into(),
        lower,
        upper,
        unit: String::new(),
        scale: PlotAxisScale::Linear,
        lane_labels: Vec::new(),
        draws_on_trailing_edge: false,
    }
}

fn panel(
    id: &str,
    x_label: &str,
    y_label: &str,
    xlim: [f64; 2],
    ylim: [f64; 2],
    layers: Vec<PlotDocumentLayer>,
) -> PlotDocumentPanel {
    PlotDocumentPanel {
        id: id.into(),
        title: String::new(),
        title_font_size: None,
        axes: vec![axis("x", x_label, xlim), axis("y", y_label, ylim)],
        layers,
        annotations: Vec::new(),
        aspect: None,
        frame_visible: None,
    }
}

fn line(id: &str, title: &str, x: &[f64], y: &[f64], color: &str) -> PlotDocumentLayer {
    PlotDocumentLayer {
        id: id.into(),
        title: title.into(),
        kind: PlotLayerKind::Line,
        x_axis_id: "x".into(),
        y_axis_id: "y".into(),
        x_values: x.to_vec(),
        y_values: y.to_vec(),
        point_labels: Vec::new(),
        point_symbol_sizes: Vec::new(),
        interval_x_start: Vec::new(),
        interval_x_end: Vec::new(),
        interval_y: Vec::new(),
        interval_height: Vec::new(),
        interval_labels: Vec::new(),
        provenance: Vec::new(),
        color_group: color.into(),
        symbol_size: 0.0,
        line_width: 2.0,
        opacity: 1.0,
        source_sample_count: x.len() as u64,
        payload_strategy: PlotPayloadStrategy::PointCloud,
        provenance_summary: String::new(),
        data: None,
        style: Some(PlotLayerStyle {
            color: Some(color.into()),
            marker: None,
            line_style: None,
        }),
    }
}

// SPDX-License-Identifier: LGPL-3.0-or-later
//! Weather-station and seasonal zenith opacity from EVLA Memo 143.

mod opacity;

use std::path::Path;

use casa_ms::MeasurementSet;
use casa_tables::{Table, TableOptions};
use casa_types::ScalarValue;
use casa_types::quanta::format::{YMD, format_time};

use opacity::OpacityFit;

#[derive(serde::Serialize)]
pub(crate) struct WeatherEstimate {
    pub(crate) opacity: Vec<f64>,
    pub(crate) windows_ghz: Vec<f64>,
    pub(crate) plot: Option<WeatherPlotData>,
}

#[derive(serde::Serialize)]
pub(crate) struct WeatherPlotData {
    pub(crate) time_seconds: Vec<f64>,
    pub(crate) temperature_c: Vec<f64>,
    pub(crate) dew_point_c: Vec<f64>,
    pub(crate) wind_m_s: Vec<f64>,
    pub(crate) wind_arrow_degrees: Vec<f64>,
    pub(crate) station_water_mm: Vec<f64>,
    pub(crate) seasonal_water_mm: Vec<f64>,
    pub(crate) weighted_water_mm: Vec<f64>,
    pub(crate) frequency_ghz: Vec<f64>,
    pub(crate) station_opacity: Vec<f64>,
    pub(crate) seasonal_opacity: Vec<f64>,
    pub(crate) weighted_opacity: Vec<f64>,
}

struct WeatherRows {
    seconds: Vec<f64>,
    temperature: Vec<f64>,
    dew_point: Vec<f64>,
    wind_speed: Vec<f64>,
    wind_direction: Vec<f64>,
}

pub(crate) fn estimate_weather(
    ms: &MeasurementSet,
    path: &Path,
    seasonal_weight: f64,
    do_plot: bool,
) -> Result<WeatherEstimate, String> {
    let windows = ms.spectral_window().map_err(display)?;
    let windows_ghz = (0..windows.row_count())
        .map(|row| windows.ref_frequency(row).map(|hz| hz * 1e-9))
        .collect::<Result<Vec<_>, _>>()
        .map_err(display)?;
    let rows = weather_rows(path)?;
    let first_time = match &rows {
        Some(rows) => rows.seconds[0],
        None => numbers(ms.main_table(), "TIME")?
            .into_iter()
            .reduce(f64::min)
            .ok_or("MeasurementSet has no visibility times")?,
    };
    let seasonal = seasonal_water(first_time)?;
    let (station, weighted) = match &rows {
        Some(rows) => {
            let station = rows
                .dew_point
                .iter()
                .zip(&rows.temperature)
                .map(|(&dew, &temperature)| station_water(dew, temperature))
                .collect::<Vec<_>>();
            let weighted = station
                .iter()
                .map(|value| (1.0 - seasonal_weight) * value + seasonal_weight * seasonal)
                .collect();
            (station, weighted)
        }
        None => (vec![seasonal], vec![seasonal]),
    };
    let plot_rows = rows.filter(|_| do_plot);
    let curves = OpacityFit::new(&windows_ghz, plot_rows.is_some())?;
    let mean_weighted = mean(&weighted);
    let opacity = curves.windows(&windows_ghz, mean_weighted);
    let plot = plot_rows.map(|rows| WeatherPlotData {
        seasonal_water_mm: vec![seasonal; rows.seconds.len()],
        time_seconds: rows.seconds,
        temperature_c: rows.temperature,
        dew_point_c: rows.dew_point,
        wind_m_s: rows.wind_speed,
        wind_arrow_degrees: rows
            .wind_direction
            .into_iter()
            .map(|rad| 270.0 - rad.to_degrees())
            .collect(),
        station_opacity: curves.evaluate(mean(&station)),
        seasonal_opacity: curves.evaluate(seasonal),
        weighted_opacity: curves.evaluate(mean_weighted),
        station_water_mm: station,
        weighted_water_mm: weighted,
        frequency_ghz: curves.frequencies_ghz(),
    });
    Ok(WeatherEstimate {
        opacity,
        windows_ghz,
        plot,
    })
}

fn weather_rows(path: &Path) -> Result<Option<WeatherRows>, String> {
    let weather = path.join("WEATHER");
    if !weather.is_dir() {
        return Ok(None);
    }
    let Ok(table) = Table::open(TableOptions::new(weather)) else {
        return Ok(None);
    };
    let Ok(seconds) = numbers(&table, "TIME") else {
        return Ok(None);
    };
    if seconds.is_empty() {
        return Ok(None);
    }
    let celsius = |column| {
        numbers(&table, column)
            .map(|values| values.into_iter().map(|kelvin| kelvin - 273.15).collect())
    };
    // The CASA task reads these columns alongside the plotted values,
    // including when it returns just the opacity.
    for column in ["PRESSURE", "REL_HUMIDITY"] {
        numbers(&table, column)?;
    }
    Ok(Some(WeatherRows {
        seconds,
        temperature: celsius("TEMPERATURE")?,
        dew_point: celsius("DEW_POINT")?,
        wind_speed: numbers(&table, "WIND_SPEED")?,
        wind_direction: numbers(&table, "WIND_DIRECTION")?,
    }))
}

fn numbers(table: &Table, column: &str) -> Result<Vec<f64>, String> {
    table
        .column_accessor(column)
        .map_err(display)?
        .scalar_cells_owned()
        .map_err(display)?
        .into_iter()
        .enumerate()
        .map(|(row, value)| match value {
            Some(ScalarValue::Float32(number)) => Ok(f64::from(number)),
            Some(ScalarValue::Float64(number)) => Ok(number),
            _ => Err(format!("invalid {column} at row {row}")),
        })
        .collect()
}

fn station_water(dew_c: f64, temperature_c: f64) -> f64 {
    let vapor_pressure = (1.81 + 17.27 * dew_c / (dew_c + 237.3)).exp();
    324.7 * vapor_pressure / (temperature_c + 273.15)
}

fn seasonal_water(time_seconds: f64) -> Result<f64, String> {
    if !time_seconds.is_finite() {
        return Err("weather time must be finite".into());
    }
    let date = weather_date(time_seconds);
    let component = |range: std::ops::Range<usize>| {
        date.get(range)
            .ok_or("invalid weather date".to_string())?
            .parse::<f64>()
            .map_err(display)
    };
    let mut day = 30.0 * (component(5..7)? - 1.0) + component(8..10)?;
    day += 5.0 * (day / 365.0);
    if day > 199.0 {
        day -= 365.0;
    }
    let adjusted = day + 165.0;
    Ok(-1.71 + 1.3647 * (22.1 - 0.178 * adjusted + 0.00044 * adjusted * adjusted))
}

fn weather_date(time_seconds: f64) -> String {
    let initial = format_time(time_seconds / 86_400.0, YMD, 0);
    if initial.contains("24:00:00") {
        format_time((time_seconds + 0.5) / 86_400.0, YMD, 0)
    } else {
        initial
    }
}

fn mean(values: &[f64]) -> f64 {
    values.iter().sum::<f64>() / values.len() as f64
}

fn display(error: impl std::fmt::Display) -> String {
    error.to_string()
}

// SPDX-License-Identifier: LGPL-3.0-or-later
//! Weather-station and seasonal zenith opacity from EVLA Memo 143.

mod opacity;

use std::path::Path;

use casa_tables::{Table, TableOptions};
use casa_types::ScalarValue;
use casa_types::quanta::format::{YMD, format_time};

/// Opacity and station measurements prepared for `plotweather`.
pub struct WeatherEstimate {
    /// Zenith opacity per spectral window.
    pub opacity: Vec<f64>,
    /// Spectral window reference frequencies in GHz.
    pub windows_ghz: Vec<f64>,
    /// Time-series weather station data when plotting is enabled.
    pub plot_data: Option<WeatherPlotData>,
}

/// Weather station series and derived opacities for `plotweather`.
pub struct WeatherPlotData {
    /// Sample timestamps in MJD seconds.
    pub time_seconds: Vec<f64>,
    /// Ambient temperatures in Celsius.
    pub temperature_c: Vec<f64>,
    /// Dew-point temperatures in Celsius.
    pub dew_point_c: Vec<f64>,
    /// Wind speeds in m/s.
    pub wind_m_s: Vec<f64>,
    /// Wind vector angles in degrees.
    pub wind_arrow_degrees: Vec<f64>,
    /// Weather-station PWV values in mm.
    pub station_water_mm: Vec<f64>,
    /// Seasonal PWV model values in mm.
    pub seasonal_water_mm: Vec<f64>,
    /// Weighted PWV values in mm.
    pub weighted_water_mm: Vec<f64>,
    /// Opacity spectrum frequency grid in GHz.
    pub frequency_ghz: Vec<f64>,
    /// Opacity spectrum for mean station PWV.
    pub station_opacity: Vec<f64>,
    /// Opacity spectrum for seasonal PWV.
    pub seasonal_opacity: Vec<f64>,
    /// Opacity spectrum for mean weighted PWV.
    pub weighted_opacity: Vec<f64>,
}

struct WeatherRows {
    seconds: Vec<f64>,
    temperature: Vec<f64>,
    dew_point: Vec<f64>,
    wind_speed: Vec<f64>,
    wind_direction: Vec<f64>,
}

/// Estimate zenith opacities and optional station plot series for a MeasurementSet.
pub fn estimate_weather(
    path: &Path,
    seasonal_weight: f64,
    do_plot: bool,
) -> Result<WeatherEstimate, String> {
    // CASA reads these literal table directories. An unreadable optional
    // WEATHER table is a seasonal fallback, even if it cannot be opened as
    // part of a complete native MeasurementSet.
    let rows = weather_rows(path)?;
    let windows = Table::open(TableOptions::new(path.join("SPECTRAL_WINDOW"))).map_err(display)?;
    let windows_ghz = numbers(&windows, "REF_FREQUENCY")?
        .into_iter()
        .map(|hz| hz * 1e-9)
        .collect::<Vec<_>>();
    let first_time = match &rows {
        Some(rows) => rows.seconds[0],
        None => {
            let main = Table::open(TableOptions::new(path)).map_err(display)?;
            first_visibility_time(&main)?
        }
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
    let mean_weighted = mean(&weighted);
    let opacity = opacity::windows(&windows_ghz, mean_weighted)?;
    let plot_data = plot_rows.map(|rows| WeatherPlotData {
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
        station_opacity: opacity::spectrum(mean(&station)),
        seasonal_opacity: opacity::spectrum(seasonal),
        weighted_opacity: opacity::spectrum(mean_weighted),
        station_water_mm: station,
        weighted_water_mm: weighted,
        frequency_ghz: opacity::frequencies(),
    });
    Ok(WeatherEstimate {
        opacity,
        windows_ghz,
        plot_data,
    })
}

fn weather_rows(path: &Path) -> Result<Option<WeatherRows>, String> {
    let weather = path.join("WEATHER");
    if !weather.is_dir() {
        tracing::info!(
            "could not find a weather table, using seasonal model only and turning off plots"
        );
        return Ok(None);
    }
    let station = Table::open(TableOptions::new(weather))
        .ok()
        .and_then(|table| numbers(&table, "TIME").ok().map(|seconds| (table, seconds)));
    let Some((table, seconds)) = station.filter(|(_, seconds)| !seconds.is_empty()) else {
        tracing::info!(
            "could not open weather table, using seasonal model only and turning off plots"
        );
        return Ok(None);
    };
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
        .map(|(row, value)| number(value, column, row))
        .collect()
}

fn first_visibility_time(table: &Table) -> Result<f64, String> {
    let column = table.column_accessor("TIME").map_err(display)?;
    let mut earliest = f64::INFINITY;
    const ROWS_PER_READ: usize = 65_536;
    for start in (0..table.row_count()).step_by(ROWS_PER_READ) {
        let stop = start.saturating_add(ROWS_PER_READ).min(table.row_count());
        let indices = (start..stop).collect::<Vec<_>>();
        let cells = column
            .scalar_cells_owned_for_rows(&indices)
            .map_err(display)?;
        for (row, value) in indices.into_iter().zip(cells) {
            earliest = earliest.min(number(value, "TIME", row)?);
        }
    }
    if earliest.is_infinite() {
        Err("MeasurementSet has no visibility times".into())
    } else {
        Ok(earliest)
    }
}

fn number(value: Option<ScalarValue>, column: &str, row: usize) -> Result<f64, String> {
    match value {
        Some(ScalarValue::Float32(number)) => Ok(f64::from(number)),
        Some(ScalarValue::Float64(number)) => Ok(number),
        _ => Err(format!("invalid {column} at row {row}")),
    }
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

/// Format an MJD seconds timestamp using CASA's weather summary date format.
pub fn weather_date(time_seconds: f64) -> String {
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

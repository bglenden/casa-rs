// SPDX-License-Identifier: LGPL-3.0-or-later
//! Atmospheric transmission and sky temperature over the selected bandpass.

use std::path::Path;
use std::sync::Arc;

use casa_atmosphere::{
    AbsorptionProfile, AtmosphereProfile, AtmosphereType, ProfileParameters, Sky, SpectralGrid,
};
use casa_measures_data::{MeasuresRuntime, RuntimePolicy};
use casa_tables::{Table, TableOptions};
use casa_types::measures::direction::{DirectionRef, MDirection};
use casa_types::measures::{EpochRef, MEpoch, MPosition, MeasFrame, PositionRef};
use casa_types::{ArrayValue, ScalarValue, Value};
use serde::Serialize;

use super::table::{SolutionRow, SolutionWindow, Solutions, array};

pub(crate) struct AtmosphericOptions<'a> {
    pub(crate) transmission: bool,
    pub(crate) sky_temperature: bool,
    pub(crate) water: &'a str,
    pub(crate) reference_field: &'a str,
    pub(crate) image_sideband: bool,
    pub(crate) lo_ghz: Option<f64>,
    pub(crate) parent_ms: Option<&'a Path>,
    pub(crate) show_points: bool,
}

#[derive(Clone, Serialize)]
pub(super) struct AtmosphericCurve {
    pub(super) x: Vec<f64>,
    pub(super) y: Vec<f64>,
    image: bool,
    sky_temperature: bool,
    show_points: bool,
    label: String,
}

pub(super) struct AtmosphereContext<'a> {
    options: &'a AtmosphericOptions<'a>,
    measurement_set: &'a Path,
    site_name: String,
    measures: Arc<MeasuresRuntime>,
    fields: Table,
    antennas: Table,
    weather: Option<Table>,
}

impl<'a> AtmosphereContext<'a> {
    pub(super) fn open(
        source: &'a Solutions,
        options: &'a AtmosphericOptions<'a>,
    ) -> Result<Option<Self>, String> {
        if !(options.transmission || options.sky_temperature) {
            return Ok(None);
        }
        let Some(measurement_set) = source.measurement_set.as_deref() else {
            return Ok(None);
        };
        let observations = open(measurement_set, "OBSERVATION")?;
        let site_name = text_cell(&observations, 0, "TELESCOPE_NAME")
            .filter(|name| !name.is_empty())
            .unwrap_or("ALMA")
            .to_string();
        Ok(Some(Self {
            options,
            measurement_set,
            site_name,
            measures: Arc::new(
                MeasuresRuntime::open_discovered(RuntimePolicy::default())
                    .map_err(|error| error.to_string())?,
            ),
            fields: open(measurement_set, "FIELD")?,
            antennas: open(measurement_set, "ANTENNA")?,
            weather: open(measurement_set, "WEATHER").ok(),
        }))
    }

    pub(super) fn curves(
        &self,
        window: &SolutionWindow,
        row: SolutionRow,
        frequency_axis: bool,
    ) -> Result<Vec<AtmosphericCurve>, String> {
        let frequency = &window.sky_frequencies_hz;
        if frequency.len() < 2 {
            return Ok(Vec::new());
        }
        let position =
            antenna_position(&self.antennas, row.antenna).ok_or("missing antenna position")?;
        let altitude_m = position
            .convert_to(PositionRef::WGS84)
            .map_err(|error| error.to_string())?
            .values()[2];
        let airmass = self.airmass(row, &position)?;
        let water_mm = self.water(row.time);
        let weather = self
            .weather
            .as_ref()
            .map(|table| conditions(table, row.time))
            .unwrap_or_default();
        let parameters = ProfileParameters {
            altitude_m,
            ground_pressure_hpa: weather.pressure_hpa,
            ground_temperature_k: weather.temperature_k,
            relative_humidity_percent: weather.humidity_percent,
            water_vapour_scale_height_km: 1.0,
            pressure_step_hpa: 5.0,
            top_altitude_km: 60.0,
            atmosphere: AtmosphereType::MidlatitudeWinter,
            ..ProfileParameters::default()
        };
        let mut curves = vec![self.evaluate(
            window,
            frequency,
            &parameters,
            water_mm,
            airmass,
            frequency_axis,
            None,
        )?];
        if self.options.image_sideband
            && let Some(lo) = self.local_oscillator(window.original_id)
        {
            let lo_hz = lo * 1e9;
            let image = frequency
                .iter()
                .map(|frequency| 2.0 * lo_hz - frequency)
                .collect::<Vec<_>>();
            curves.push(self.evaluate(
                window,
                &image,
                &parameters,
                water_mm,
                airmass,
                frequency_axis,
                Some(lo_hz),
            )?);
        }
        Ok(curves)
    }

    fn evaluate(
        &self,
        window: &SolutionWindow,
        frequency: &[f64],
        parameters: &ProfileParameters,
        water_mm: f64,
        airmass: f64,
        frequency_axis: bool,
        image_lo_hz: Option<f64>,
    ) -> Result<AtmosphericCurve, String> {
        let mut channels = frequency.len();
        while channels > 512 {
            channels /= 2;
        }
        let center = (frequency[frequency.len() / 2 - 1] + frequency[frequency.len() / 2]) * 0.5;
        let separation = (frequency[frequency.len() - 1] - frequency[0]) / (channels - 1) as f64;
        let mut grid = SpectralGrid::new();
        let reference_channel = channels / 2;
        let offset = if channels.is_multiple_of(2) {
            0.5 * separation.abs()
        } else {
            0.0
        };
        grid.add_regular(
            channels,
            reference_channel,
            center + offset,
            separation.abs(),
        )
        .map_err(|error| error.to_string())?;
        let model_frequencies = grid.frequencies_hz().collect::<Vec<_>>();
        let profile = AtmosphereProfile::new(parameters).map_err(|error| error.to_string())?;
        let absorption = AbsorptionProfile::new(profile, grid);
        let water_m = water_mm * 1e-3;
        let zenith = Sky::zenith(water_m);
        let mut y = (0..channels)
            .map(|channel| {
                let tau =
                    absorption.dry_opacity(channel) + absorption.wet_opacity(channel, water_m);
                let transmission = (-airmass * tau).exp();
                if self.options.sky_temperature {
                    absorption
                        .tebb_sky(channel, &zenith)
                        .map(|tebb| tebb * (-airmass * tau).exp_m1() / (-tau).exp_m1())
                        .map_err(|error| error.to_string())
                } else {
                    Ok(transmission)
                }
            })
            .collect::<Result<Vec<_>, _>>()?;
        let average = frequency.iter().sum::<f64>() / frequency.len() as f64;
        let high_reference = window.reference_frequency_hz.is_some_and(|hz| hz > average);
        let reverse = high_reference == (window.sideband % 2 == 0);
        let image = image_lo_hz.is_some();
        let mut x = if frequency_axis {
            model_frequencies
                .into_iter()
                .map(|hz| image_lo_hz.map_or(hz, |lo| 2.0 * lo - hz) * 1e-9)
                .collect()
        } else {
            if reverse {
                y.reverse();
            }
            (0..channels)
                .map(|channel| channel as f64)
                .collect::<Vec<_>>()
        };
        if !frequency_axis && image {
            x.reverse();
        }
        Ok(AtmosphericCurve {
            x,
            y,
            image,
            sky_temperature: self.options.sky_temperature,
            show_points: self.options.show_points,
            label: format!("PWV {water_mm:.2}mm, airmass {airmass:.3}, maxAlt 60km"),
        })
    }

    fn airmass(&self, row: SolutionRow, antenna: &MPosition) -> Result<f64, String> {
        let field_id = if self.options.reference_field.is_empty() {
            row.field
        } else {
            self.options
                .reference_field
                .parse::<i32>()
                .ok()
                .or_else(|| {
                    (0..self.fields.row_count())
                        .find(|&id| {
                            text_cell(&self.fields, id, "NAME")
                                == Some(self.options.reference_field)
                        })
                        .and_then(|id| i32::try_from(id).ok())
                })
                .unwrap_or(row.field)
        };
        let field = usize::try_from(field_id).map_err(|error| error.to_string())?;
        let reference = array(&self.fields, field, "REFERENCE_DIR")?;
        let ArrayValue::Float64(reference) = reference else {
            return Err("invalid FIELD reference direction".into());
        };
        let mut angles = reference.iter().copied();
        let ra = angles.next().ok_or("missing field right ascension")?;
        let dec = angles.next().ok_or("missing field declination")?;
        let position = MPosition::casa_observatory(&self.site_name, self.measures.as_ref())
            .unwrap_or_else(|| antenna.clone());
        let frame = MeasFrame::new()
            .with_measures(self.measures.clone())
            .with_epoch(MEpoch::from_mjd_seconds(row.time, EpochRef::UTC))
            .with_position(position);
        let direction = MDirection::from_angles(ra, dec, DirectionRef::J2000)
            .convert_to(DirectionRef::AZEL, &frame)
            .map_err(|error| error.to_string())?;
        let elevation = direction.latitude_rad();
        Ok(if elevation <= 3.0_f64.to_radians() {
            std::f64::consts::SQRT_2
        } else {
            elevation.sin().recip()
        })
    }

    fn water(&self, time: f64) -> f64 {
        let default = if self.site_name.contains("ALMA") {
            1.0
        } else {
            5.0
        };
        if self.options.water.is_empty() {
            return 1.0;
        }
        if !self.options.water.contains("auto") {
            return self.options.water.parse().unwrap_or(default);
        }
        ["ASDM_CALWVR", "ASDM_CALATMOSPHERE"]
            .iter()
            .find_map(|name| {
                let table = open(self.measurement_set, name).ok()?;
                let index = nearest_time(&table, "startValidTime", time)?;
                match table.cell_accessor(index, "water").ok()?.value().ok()?? {
                    Value::Scalar(ScalarValue::Float64(value)) if *value >= 1e-8 => {
                        Some(*value * 1000.0)
                    }
                    Value::Array(ArrayValue::Float64(values)) => values
                        .iter()
                        .copied()
                        .next()
                        .filter(|value| *value >= 1e-8)
                        .map(|value| value * 1000.0),
                    _ => None,
                }
            })
            .unwrap_or(default)
    }

    fn local_oscillator(&self, window: i32) -> Option<f64> {
        if let Some(value) = self.options.lo_ghz {
            return Some(value);
        }
        [Some(self.measurement_set), self.options.parent_ms]
            .into_iter()
            .flatten()
            .find_map(|ms| {
                let table = open(ms, "ASDM_RECEIVER").ok()?;
                let key = format!("SpectralWindow_{window}");
                let row = (0..table.row_count())
                    .find(|&row| text_cell(&table, row, "spectralWindowId") == Some(&key))?;
                match array(&table, row, "freqLO").ok()? {
                    ArrayValue::Float64(values) => values.iter().next().map(|hz| hz * 1e-9),
                    _ => None,
                }
            })
    }
}

struct Conditions {
    pressure_hpa: f64,
    temperature_k: f64,
    humidity_percent: f64,
}

impl Default for Conditions {
    fn default() -> Self {
        Self {
            pressure_hpa: 563.0,
            temperature_k: 273.15,
            humidity_percent: 20.0,
        }
    }
}

fn conditions(weather: &Table, time: f64) -> Conditions {
    let default = Conditions::default();
    let Some(row) = nearest_time(weather, "TIME", time) else {
        return default;
    };
    let pressure = scalar_number(weather, row, "PRESSURE").unwrap_or(default.pressure_hpa);
    let unit = weather
        .column_keywords("PRESSURE")
        .and_then(|record| record.get("QuantumUnits"))
        .and_then(|value| match value {
            Value::Array(ArrayValue::String(units)) => units.iter().next().map(String::as_str),
            _ => None,
        });
    let hpa = if unit == Some("Pa") && pressure / 100.0 >= 100.0 {
        pressure / 100.0
    } else {
        pressure
    };
    let temperature = scalar_number(weather, row, "TEMPERATURE").unwrap_or(default.temperature_k);
    let humidity = scalar_number(weather, row, "REL_HUMIDITY").unwrap_or(default.humidity_percent);
    Conditions {
        pressure_hpa: if hpa > 0.0 { hpa } else { default.pressure_hpa },
        temperature_k: if temperature > 100.0 {
            temperature
        } else {
            temperature + 273.15
        },
        humidity_percent: if humidity > 0.0 {
            humidity
        } else {
            default.humidity_percent
        },
    }
}

fn antenna_position(table: &Table, antenna: i32) -> Option<MPosition> {
    let ArrayValue::Float64(coordinates) =
        array(table, usize::try_from(antenna).ok()?, "POSITION").ok()?
    else {
        return None;
    };
    let [x, y, z] = coordinates.as_slice()? else {
        return None;
    };
    Some(MPosition::new_itrf(*x, *y, *z))
}

fn nearest_time(table: &Table, column: &str, requested: f64) -> Option<usize> {
    (0..table.row_count())
        .filter_map(|row| Some((row, (scalar_number(table, row, column)? - requested).abs())))
        .min_by(|left, right| left.1.total_cmp(&right.1))
        .map(|(row, _)| row)
}

fn scalar_number(table: &Table, row: usize, name: &str) -> Option<f64> {
    match table.cell_accessor(row, name).ok()?.scalar().ok()? {
        ScalarValue::Float32(value) => Some(f64::from(*value)),
        ScalarValue::Float64(value) => Some(*value),
        _ => None,
    }
}

fn text_cell<'a>(table: &'a Table, row: usize, name: &str) -> Option<&'a str> {
    match table.cell_accessor(row, name).ok()?.scalar().ok()? {
        ScalarValue::String(value) => Some(value),
        _ => None,
    }
}

fn open(root: &Path, subtable: &str) -> Result<Table, String> {
    Table::open(TableOptions::new(root.join(subtable))).map_err(|error| error.to_string())
}

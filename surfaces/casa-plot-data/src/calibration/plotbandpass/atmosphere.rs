// SPDX-License-Identifier: LGPL-3.0-or-later
//! Atmospheric transmission and sky temperature over the selected bandpass.

use std::collections::BTreeMap;
use std::path::Path;
use std::sync::Arc;

use casa_atmosphere::{
    AbsorptionProfile, AtmosphereProfile, AtmosphereType, ProfileParameters, Sky, SpectralGrid,
};
use casa_core::measures::direction::{DirectionRef, MDirection};
use casa_core::measures::{EpochRef, MEpoch, MPosition, MeasFrame, PositionRef};
use casa_core::{ArrayValue, ScalarValue, Value};
use casa_measures::{MeasuresRuntime, RuntimePolicy};
use casa_tables::{Table, TableOptions};
use serde::Serialize;

use super::spectrum::median;
use super::table::{SolutionRow, SolutionWindow, Solutions, array, integers, numbers};

/// Controls for atmospheric absorption and emission curves over a bandpass panel.
pub struct AtmosphericOptions<'a> {
    /// Whether to compute atmospheric transmission.
    pub transmission: bool,
    /// Whether to compute atmospheric sky brightness temperature.
    pub sky_temperature: bool,
    /// Precipitable water-vapor setting in millimeters, or the automatic CASA token.
    pub water: &'a str,
    /// Field selector for determining the observing direction and airmass.
    pub reference_field: &'a str,
    /// Whether to include the reflected image sideband.
    pub image_sideband: bool,
    /// Optional local oscillator frequency in GHz.
    pub lo_ghz: Option<f64>,
    /// Parent MeasurementSet to use for observing geometry and weather data.
    pub parent_ms: Option<&'a Path>,
    /// Whether to draw overlay samples as individual points.
    pub show_points: bool,
    /// Whether the plotted curves combine more than one solution time.
    pub time_overlay: bool,
}

#[derive(Clone, Serialize)]
pub struct AtmosphericCurve {
    pub(super) x: Vec<f64>,
    pub(super) y: Vec<f64>,
    image: bool,
    sky_temperature: bool,
    show_points: bool,
    label: String,
}

pub(super) struct AtmosphereContext<'a> {
    options: &'a AtmosphericOptions<'a>,
    site_name: String,
    measures: Arc<MeasuresRuntime>,
    fields: Table,
    antennas: Table,
    weather: Option<Table>,
    pointing: Option<Table>,
    scan_times: BTreeMap<i32, Vec<ScanTimes>>,
    water_samples: Vec<(f64, f64)>,
    oscillators: BTreeMap<i32, f64>,
}

struct LineOfSight {
    profile: ProfileParameters,
    water_mm: f64,
    airmass: f64,
    field: i32,
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
            site_name,
            measures: Arc::new(
                MeasuresRuntime::open_discovered(RuntimePolicy::default())
                    .map_err(|error| error.to_string())?,
            ),
            fields: open(measurement_set, "FIELD")?,
            antennas: open(measurement_set, "ANTENNA")?,
            weather: open_optional(measurement_set, "WEATHER"),
            pointing: open_optional(measurement_set, "POINTING"),
            scan_times: scan_times(measurement_set)?,
            water_samples: if options.water.contains("auto") {
                water_samples(measurement_set)
            } else {
                Vec::new()
            },
            oscillators: if options.image_sideband && options.lo_ghz.is_none() {
                receiver_oscillators(measurement_set, options.parent_ms)
            } else {
                BTreeMap::new()
            },
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
        let field = self.reference_field(row.field);
        let scan = self.scan_times.get(&field).and_then(|scans| {
            if self.options.reference_field.is_empty() {
                scans.iter().min_by(|a, b| {
                    (a.mean - row.time)
                        .abs()
                        .total_cmp(&(b.mean - row.time).abs())
                })
            } else {
                scans.iter().min_by(|a, b| a.mean.total_cmp(&b.mean))
            }
        });
        let airmass = self.airmass(row, field, scan, &position)?;
        let timestamp = if self.options.reference_field.is_empty() {
            row.time
        } else {
            scan.map_or(row.time, |scan| scan.mean)
        };
        let water_mm = self.water(timestamp, row.interval);
        let weather = self
            .weather
            .as_ref()
            .map(|table| conditions(table, scan.map_or([row.time; 2], |scan| scan.bounds)))
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
        let conditions = LineOfSight {
            profile: parameters,
            water_mm,
            airmass,
            field,
        };
        let mut curves =
            vec![self.evaluate(window, frequency, &conditions, frequency_axis, None)?];
        if self.options.image_sideband
            && let Some(lo) = self.local_oscillator(window.original_id)
        {
            let lo_hz = lo * 1e9;
            let image = frequency
                .iter()
                .map(|frequency| 2.0 * lo_hz - frequency)
                .collect::<Vec<_>>();
            curves.push(self.evaluate(window, &image, &conditions, frequency_axis, Some(lo_hz))?);
        }
        Ok(curves)
    }

    fn evaluate(
        &self,
        window: &SolutionWindow,
        frequency: &[f64],
        conditions: &LineOfSight,
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
        let profile =
            AtmosphereProfile::new(&conditions.profile).map_err(|error| error.to_string())?;
        let absorption = AbsorptionProfile::new(profile, grid);
        let LineOfSight {
            water_mm,
            airmass,
            field,
            ..
        } = *conditions;
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
            let step = (frequency.len() - 1) as f64 / (channels - 1) as f64;
            (0..channels)
                .map(|channel| channel as f64 * step)
                .collect::<Vec<_>>()
        };
        if image {
            x.reverse();
            if frequency_axis {
                y.reverse();
            }
        }
        Ok(AtmosphericCurve {
            x,
            y,
            image,
            sky_temperature: self.options.sky_temperature,
            show_points: self.options.show_points,
            label: if self.options.time_overlay {
                format!("PWV {water_mm:.2}mm, airmass {airmass:.2}, maxAlt 60km (field {field})")
            } else {
                format!("PWV {water_mm:.2}mm, airmass {airmass:.3}, maxAlt 60km")
            },
        })
    }

    fn reference_field(&self, default: i32) -> i32 {
        if self.options.reference_field.is_empty() {
            return default;
        }
        self.options
            .reference_field
            .parse::<i32>()
            .ok()
            .or_else(|| {
                (0..self.fields.row_count())
                    .find(|&id| {
                        text_cell(&self.fields, id, "NAME") == Some(self.options.reference_field)
                    })
                    .and_then(|id| i32::try_from(id).ok())
            })
            .unwrap_or(default)
    }

    fn airmass(
        &self,
        row: SolutionRow,
        field_id: i32,
        scan: Option<&ScanTimes>,
        antenna: &MPosition,
    ) -> Result<f64, String> {
        if !self.site_name.contains("VLA")
            && !self.site_name.contains("NRO")
            && let (Some(pointing), Some(scan)) = (&self.pointing, scan)
            && let Some(elevation) = pointing_elevation(pointing, row.antenna, scan.bounds)
        {
            return Ok(airmass_for_elevation(elevation));
        }
        let field = usize::try_from(field_id).map_err(|error| error.to_string())?;
        let reference = array(&self.fields, field, "DELAY_DIR")?;
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
            .with_epoch(MEpoch::from_mjd_seconds(
                scan.map_or(row.time, |scan| scan.mean),
                EpochRef::UTC,
            ))
            .with_position(position);
        let direction = MDirection::from_angles(ra, dec, DirectionRef::J2000)
            .convert_to(DirectionRef::AZEL, &frame)
            .map_err(|error| error.to_string())?;
        Ok(airmass_for_elevation(direction.latitude_rad()))
    }

    fn water(&self, time: f64, interval: f64) -> f64 {
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
        let start = time - interval * 0.5;
        let end = time + interval * 0.5;
        let mut water = self
            .water_samples
            .iter()
            .filter_map(|&(epoch, value)| (epoch > start && epoch < end).then_some(value))
            .collect::<Vec<_>>();
        if water.is_empty() {
            let closest = self
                .water_samples
                .iter()
                .map(|(epoch, _)| (epoch - start).abs())
                .min_by(f64::total_cmp)
                .unwrap_or(f64::INFINITY);
            water.extend(self.water_samples.iter().filter_map(|&(epoch, value)| {
                (((epoch - start).abs() - closest).abs() < 1.0).then_some(value)
            }));
        }
        let measured = 1000.0 * median(&water);
        if measured >= 1e-5 { measured } else { default }
    }

    fn local_oscillator(&self, window: i32) -> Option<f64> {
        self.options
            .lo_ghz
            .or_else(|| self.oscillators.get(&window).copied())
    }
}

fn water_samples(ms: &Path) -> Vec<(f64, f64)> {
    for name in ["ASDM_CALWVR", "ASDM_CALATMOSPHERE"] {
        let Some(table) = open_optional(ms, name).filter(|table| table.row_count() > 0) else {
            continue;
        };
        return (0..table.row_count())
            .filter_map(|row| {
                let epoch = scalar_number(&table, row, "startValidTime")?;
                let water = match table.cell_accessor(row, "water").ok()?.value().ok()?? {
                    Value::Scalar(ScalarValue::Float64(value)) => *value,
                    Value::Array(ArrayValue::Float64(values)) => values.iter().copied().next()?,
                    _ => return None,
                };
                Some((epoch, water))
            })
            .collect();
    }
    Vec::new()
}

fn receiver_oscillators(ms: &Path, parent: Option<&Path>) -> BTreeMap<i32, f64> {
    let Some(receiver) = open_optional(ms, "ASDM_RECEIVER") else {
        return BTreeMap::new();
    };
    let mut by_window = BTreeMap::new();
    for row in 0..receiver.row_count() {
        if let Some(id) = text_cell(&receiver, row, "spectralWindowId")
            .and_then(|id| id.rsplit_once('_'))
            .and_then(|(_, id)| id.parse::<i32>().ok())
            && let Ok(ArrayValue::Float64(frequencies)) = array(&receiver, row, "freqLO")
            && let Some(hz) = frequencies.iter().next()
        {
            by_window.entry(id).or_insert(hz * 1e-9);
        }
    }
    let Some(windows) = open_optional(ms, "SPECTRAL_WINDOW") else {
        return by_window;
    };
    if by_window
        .last_key_value()
        .is_none_or(|(&last, _)| last as usize + 1 == windows.row_count())
    {
        return by_window;
    }
    let Some(original) = parent.and_then(|path| open_optional(path, "SPECTRAL_WINDOW")) else {
        return BTreeMap::new();
    };
    let names = (0..original.row_count())
        .filter_map(|row| Some((text_cell(&original, row, "NAME")?, row as i32)))
        .collect::<BTreeMap<_, _>>();
    (0..windows.row_count())
        .filter_map(|row| {
            let name = text_cell(&windows, row, "NAME")?;
            let unrenumbered = names.get(name)?;
            Some((row as i32, *by_window.get(unrenumbered)?))
        })
        .collect()
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

fn conditions(weather: &Table, [start, stop]: [f64; 2]) -> Conditions {
    let default = Conditions::default();
    let mut rows = (0..weather.row_count())
        .filter(|&row| {
            scalar_number(weather, row, "TIME").is_some_and(|time| start <= time && time <= stop)
        })
        .collect::<Vec<_>>();
    if rows.is_empty() {
        let Some(row) = nearest_time(weather, "TIME", start) else {
            return default;
        };
        rows.push(row);
    }
    let reported = |column| {
        median(
            &rows
                .iter()
                .filter_map(|&row| scalar_number(weather, row, column))
                .collect::<Vec<_>>(),
        )
    };
    let pressure = reported("PRESSURE");
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
    let temperature = reported("TEMPERATURE");
    let humidity = reported("REL_HUMIDITY");
    Conditions {
        pressure_hpa: if hpa > 0.0 { hpa } else { default.pressure_hpa },
        temperature_k: if !temperature.is_finite() {
            default.temperature_k
        } else if temperature > 100.0 {
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

struct ScanTimes {
    bounds: [f64; 2],
    mean: f64,
}

fn scan_times(ms: &Path) -> Result<BTreeMap<i32, Vec<ScanTimes>>, String> {
    let table = Table::open(TableOptions::new(ms)).map_err(|error| error.to_string())?;
    let fields = integers(&table, "FIELD_ID")?;
    let scans = integers(&table, "SCAN_NUMBER")?;
    let times = numbers(&table, "TIME")?;
    let mut keyed = BTreeMap::<(i32, i32), Vec<f64>>::new();
    for ((field, scan), time) in fields.into_iter().zip(scans).zip(times) {
        if time.is_finite() {
            keyed.entry((field, scan)).or_default().push(time);
        }
    }
    let mut result = BTreeMap::<i32, Vec<ScanTimes>>::new();
    for ((field, _), mut times) in keyed {
        times.sort_unstable_by(f64::total_cmp);
        times.dedup();
        result.entry(field).or_default().push(ScanTimes {
            bounds: [times[0], times[times.len() - 1]],
            mean: times.iter().sum::<f64>() / times.len() as f64,
        });
    }
    Ok(result)
}

fn pointing_elevation(table: &Table, antenna: i32, [start, stop]: [f64; 2]) -> Option<f64> {
    let rows = (0..table.row_count())
        .filter_map(|row| {
            let ScalarValue::Int32(id) =
                table.cell_accessor(row, "ANTENNA_ID").ok()?.scalar().ok()?
            else {
                return None;
            };
            if *id != antenna {
                return None;
            }
            let time = scalar_number(table, row, "TIME")?;
            let ArrayValue::Float64(direction) = array(table, row, "DIRECTION").ok()? else {
                return None;
            };
            let elevation = direction.iter().nth(1).copied()?;
            Some((time, elevation))
        })
        .collect::<Vec<_>>();
    let first = rows.iter().position(|&(time, _)| time > start);
    let last = rows.iter().rposition(|&(time, _)| time < stop);
    if let (Some(first), Some(last)) = (first, last)
        && first <= last
    {
        return Some(median(
            &rows[first..=last]
                .iter()
                .map(|&(_, elevation)| elevation)
                .collect::<Vec<_>>(),
        ));
    }
    if let Some(first) = first {
        return Some(rows[first].1);
    }
    (!rows.is_empty()).then(|| {
        median(
            &rows
                .iter()
                .map(|&(_, elevation)| elevation)
                .collect::<Vec<_>>(),
        )
    })
}

fn airmass_for_elevation(elevation: f64) -> f64 {
    if elevation <= 3.0_f64.to_radians() {
        std::f64::consts::SQRT_2
    } else {
        elevation.sin().recip()
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

fn open_optional(root: &Path, subtable: &str) -> Option<Table> {
    root.join(subtable)
        .is_dir()
        .then(|| open(root, subtable).ok())
        .flatten()
}

fn open(root: &Path, subtable: &str) -> Result<Table, String> {
    Table::open(TableOptions::new(root.join(subtable))).map_err(|error| error.to_string())
}

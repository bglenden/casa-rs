// SPDX-License-Identifier: LGPL-3.0-or-later
//! Apply the PlotMS atmospheric model to calibration-table plotting chunks.

use std::borrow::Cow;
use std::collections::{BTreeMap, BTreeSet};
use std::path::Path;
use std::sync::Arc;

use crate::{TaskFailure, TaskInvocation};
use casa_measures_data::{MeasuresRuntime, RuntimePolicy};
use casa_tables::Table;
use casa_types::measures::direction::{DirectionRef, MDirection};
use casa_types::measures::{EpochRef, MEpoch, MPosition, MeasFrame};
use casa_types::{ArrayValue, ScalarValue, Value};

use super::PlotSeries;
use super::axis::Axis;
use super::iteration::PlotPanels;
use super::samples::Point;
use super::selection::{Expressions, Selection};
use super::table_data::{CalibrationTable, array, integer, number, subtable, text};
use casa_ms::{MeasurementSet, MsSelection, SubtableId};
use casa_plot_data::msexplore::atmosphere::{AtmosphericConditions, Overlay};

pub(in crate::ms::msexplore) struct Options {
    pub(in crate::ms::msexplore) kind: Overlay,
    pub(in crate::ms::msexplore) image: bool,
}

impl Options {
    pub(in crate::ms::msexplore) fn from_task(
        task: &TaskInvocation<'_>,
    ) -> Result<Option<Self>, TaskFailure> {
        let kind = if task.flag("showatm")? {
            Overlay::Transmission
        } else if task.flag("showtsky")? {
            Overlay::SkyTemperature
        } else {
            return Ok(None);
        };
        if let Some(axis) = task.text("xaxis")?.filter(|axis| !axis.is_empty())
            && !matches!(Axis::parse(&axis), Ok(Axis::Channel | Axis::Frequency))
        {
            return Err(TaskFailure::validation(
                "xaxis must be channel or frequency for showatm and showtsky",
            )
            .at("xaxis"));
        }
        Ok(Some(Self {
            kind,
            image: task.flag("showimage")?,
        }))
    }

    pub(in crate::ms::msexplore) fn axis(&self) -> Axis {
        match self.kind {
            Overlay::Transmission => Axis::AtmosphericTransmission,
            Overlay::SkyTemperature => Axis::SkyTemperature,
        }
    }
}

pub(in crate::ms::msexplore) type Chunk = (i32, i32);

pub(super) fn append_overlays(
    table: &CalibrationTable,
    path: &Path,
    selection: &Selection,
    expressions: &Expressions<'_>,
    options: &Options,
    panels: &mut PlotPanels,
    averaged: bool,
) -> Result<(), TaskFailure> {
    let context = Context::open(table, path, selection, expressions, options.image)
        .map_err(TaskFailure::execution)?;
    let mut models = BTreeMap::new();
    for panel in &mut panels.panels {
        let Some(source) = panel.series.first() else {
            continue;
        };
        let x = source.x;
        let chunks = group_channels(&source.points);
        let mut signal = Vec::new();
        let mut image = Vec::new();
        for (chunk, points, receptors) in chunks {
            let coordinates = points[0].coordinates;
            let full_grid = if averaged {
                None
            } else {
                let solution = table
                    .solution(coordinates)
                    .map_err(TaskFailure::execution)?;
                let channels = solution.values.shape()[1];
                usize::try_from(chunk.0)
                    .ok()
                    .and_then(|id| table.windows.get(id))
                    .and_then(|window| window.frequencies.get(..channels))
                    .filter(|_| channels > 1)
            };
            // The unaveraged CASA cache samples the full band before applying
            // selected channels. Rebuilding a regular grid across a gapped
            // selection would move atmospheric lines to different channels.
            let frequencies = full_grid.map_or_else(
                || {
                    Cow::Owned(
                        points
                            .iter()
                            .map(|point| point.coordinates.frequency_ghz * 1e9)
                            .collect(),
                    )
                },
                Cow::Borrowed,
            );
            let key = (
                chunk,
                frequencies
                    .iter()
                    .map(|value| value.to_bits())
                    .collect::<Vec<_>>(),
            );
            let (transmission, image_band) = match models.entry(key) {
                std::collections::btree_map::Entry::Occupied(entry) => entry.into_mut(),
                std::collections::btree_map::Entry::Vacant(entry) => {
                    let conditions = context.conditions(chunk);
                    let signal = conditions
                        .curve(&frequencies, options.kind)
                        .map_err(|error| TaskFailure::execution(error.to_string()))?;
                    let image = if options.image {
                        context
                            .image_curve(chunk.0, &frequencies, &conditions, options.kind)
                            .map_err(|error| TaskFailure::execution(error.to_string()))?
                    } else {
                        Vec::new()
                    };
                    entry.insert((signal, image))
                }
            };
            for (index, point) in points.into_iter().enumerate() {
                let source_channel = if full_grid.is_some() {
                    point.coordinates.channel
                } else {
                    index
                };
                let mut point = point;
                point.coordinates.receptor = receptors[index % receptors.len()];
                signal.push(Point {
                    y: transmission[source_channel],
                    ..point
                });
                if options.image {
                    image.push(Point {
                        y: image_band[source_channel],
                        ..point
                    });
                }
            }
        }
        panel.overlay.push(PlotSeries {
            x,
            y: options.axis(),
            points: signal,
        });
        if options.image {
            panel.overlay.push(PlotSeries {
                x,
                y: Axis::ImageSideband,
                points: image,
            });
        }
    }
    Ok(())
}

#[derive(Default)]
struct ChunkChannels {
    channels: BTreeMap<usize, Point>,
    receptors: BTreeSet<i32>,
}

fn group_channels(points: &[Point]) -> Vec<(Chunk, Vec<Point>, Vec<i32>)> {
    let mut order = Vec::new();
    let mut groups = BTreeMap::<_, ChunkChannels>::new();
    for point in points {
        let coordinates = point.coordinates;
        let key = (
            (coordinates.window, coordinates.scan),
            coordinates.field,
            coordinates.time.to_bits(),
        );
        let group = groups.entry(key).or_insert_with(|| {
            order.push(key);
            ChunkChannels::default()
        });
        group
            .channels
            .entry(coordinates.channel)
            .and_modify(|representative| representative.flagged &= point.flagged)
            .or_insert(*point);
        group.receptors.insert(coordinates.receptor);
    }
    order
        .into_iter()
        .map(|key| {
            let group = groups.remove(&key).expect("recorded overlay chunk");
            (
                key.0,
                group.channels.into_values().collect(),
                group.receptors.into_iter().collect(),
            )
        })
        .collect()
}

pub(in crate::ms::msexplore) struct Context {
    telescope: String,
    measures: Arc<MeasuresRuntime>,
    position: Option<MPosition>,
    field: Table,
    pointing: Option<Table>,
    directions: BTreeMap<Chunk, Vec<(f64, i32)>>,
    ms_times: BTreeMap<Chunk, [f64; 2]>,
    weather: Weather,
    water: Vec<(f64, f64)>,
    oscillators: Vec<f64>,
}

impl Context {
    fn open(
        source: &CalibrationTable,
        path: &Path,
        selection: &Selection,
        expressions: &Expressions<'_>,
        image: bool,
    ) -> Result<Self, String> {
        let field = subtable(&source.table, path, "FIELD")?;
        let observation = subtable(&source.table, path, "OBSERVATION")?;
        let telescope = text(&observation, 0, "TELESCOPE_NAME")
            .unwrap_or("")
            .to_owned();
        let ms_path = match source.table.keywords().get("MSName") {
            Some(Value::Scalar(ScalarValue::String(name))) if !name.is_empty() => {
                Some(path.parent().unwrap_or_else(|| Path::new(".")).join(name))
            }
            _ => None,
        };
        let ms = ms_path
            .as_deref()
            .and_then(|path| MeasurementSet::open(path).ok());
        let mut directions = BTreeMap::<_, Vec<_>>::new();
        for row in 0..source.rows() {
            let coordinates = source.coordinates(row)?;
            if selection.includes(&coordinates) {
                directions
                    .entry((coordinates.window, coordinates.scan))
                    .or_default()
                    .push((coordinates.time, coordinates.field));
            }
        }
        let mut context = Self::new(telescope, field, ms.as_ref(), directions, image)?;
        if !context.weather.samples.is_empty()
            && let Some(ms) = &ms
        {
            context.ms_times = measurement_times(ms, expressions)?;
        }
        Ok(context)
    }

    pub(in crate::ms::msexplore) fn open_ms(
        ms: &MeasurementSet,
        selection: &MsSelection,
        image: bool,
    ) -> Result<Self, String> {
        let path = ms
            .path()
            .ok_or("PlotMS atmosphere requires an on-disk MeasurementSet")?;
        let field = subtable(ms.main_table(), path, "FIELD")?;
        let observation = ms
            .subtable(SubtableId::Observation)
            .ok_or("missing OBSERVATION table")?;
        let telescope = text(observation, 0, "TELESCOPE_NAME")
            .unwrap_or("")
            .to_owned();
        let samples = selected_observation_samples(ms, selection)?;
        let times = samples
            .iter()
            .map(|(&chunk, rows)| (chunk, time_bounds(rows)))
            .collect();
        let mut context = Self::new(telescope, field, Some(ms), samples, image)?;
        context.ms_times = times;
        Ok(context)
    }

    fn new(
        telescope: String,
        field: Table,
        ms: Option<&MeasurementSet>,
        directions: BTreeMap<Chunk, Vec<(f64, i32)>>,
        image: bool,
    ) -> Result<Self, String> {
        let measures = Arc::new(
            MeasuresRuntime::open_discovered(RuntimePolicy::default())
                .map_err(|error| error.to_string())?,
        );
        let position = MPosition::casa_observatory(&telescope, measures.as_ref());
        let weather = ms
            .map(|ms| Weather::read(ms, &telescope))
            .unwrap_or_default();
        let pointing = ms.and_then(|ms| attached(ms, "POINTING"));
        let water = ms.map(read_water).unwrap_or_default();
        let oscillators = if image {
            ms.map(read_oscillators).unwrap_or_default()
        } else {
            Vec::new()
        };
        Ok(Self {
            telescope,
            measures,
            position,
            field,
            pointing,
            directions,
            ms_times: BTreeMap::new(),
            weather,
            water,
            oscillators,
        })
    }

    pub(in crate::ms::msexplore) fn conditions(&self, chunk: Chunk) -> AtmosphericConditions {
        let rows = self.directions.get(&chunk).map_or(&[][..], Vec::as_slice);
        let bounds = time_bounds(rows);
        let pointing = self.pointing.as_ref().and_then(|table| {
            mean((0..table.row_count()).filter_map(|row| {
                let time = number(table, row, "TIME")?;
                (integer(table, row, "ANTENNA_ID") == Some(0)
                    && time >= bounds[0]
                    && time <= bounds[1])
                    .then(|| self.elevation(table, row, "DIRECTION", time))
                    .flatten()
            }))
        });
        let angle = |elevation: f64| 1.0 / elevation.sin();
        let airmass = if let Some(elevation) = pointing.filter(|elevation| *elevation != 0.0) {
            angle(elevation)
        } else {
            let epoch = mean(rows.iter().map(|&(time, _)| time)).unwrap_or_default();
            let fields = rows
                .iter()
                .map(|&(_, field)| field)
                .collect::<BTreeSet<_>>();
            mean(fields.into_iter().map(|field| {
                let elevation = usize::try_from(field)
                    .ok()
                    .and_then(|field| self.elevation(&self.field, field, "DELAY_DIR", epoch))
                    .unwrap_or_default();
                angle(if elevation.to_degrees() <= 3.0 {
                    45_f64.to_radians()
                } else {
                    elevation
                })
            }))
            .unwrap_or_else(|| angle(45_f64.to_radians()))
        };
        let mut conditions = AtmosphericConditions::new(&self.telescope, airmass);
        let mut water = nearest_samples(&self.water, bounds);
        water.sort_by(f64::total_cmp);
        if !water.is_empty() {
            let middle = water.len() / 2;
            let pwv = if water.len().is_multiple_of(2) {
                (water[middle - 1] + water[middle]) * 0.5
            } else {
                water[middle]
            };
            if pwv != 0.0 {
                conditions.water_m = pwv;
            }
        }
        if let Some(&bounds) = self.ms_times.get(&chunk) {
            self.weather.apply(bounds, &mut conditions);
        }
        conditions
    }

    fn elevation(&self, table: &Table, row: usize, name: &str, time: f64) -> Option<f64> {
        let keywords = table.column_keywords(name)?;
        let Value::Record(measure) = keywords.get("MEASINFO")? else {
            return None;
        };
        let Value::Scalar(ScalarValue::String(reference)) = measure.get("Ref")? else {
            return None;
        };
        let ArrayValue::Float64(direction) = array(table, row, name).ok()? else {
            return None;
        };
        let mut values = direction.iter().copied();
        let mut first = values.next()?;
        let mut second = values.next()?;
        if let Some(Value::Array(ArrayValue::String(units))) = keywords.get("QuantumUnits") {
            let mut units = units.iter();
            first = angular_radians(first, units.next()?);
            second = angular_radians(second, units.next()?);
        }
        if reference.contains("AZEL") {
            return Some(second);
        }
        if reference != "J2000" {
            return Some(0.0);
        }
        let frame = MeasFrame::new()
            .with_measures(self.measures.clone())
            .with_epoch(MEpoch::from_mjd_seconds(time, EpochRef::UTC))
            .with_position(self.position.clone()?);
        MDirection::from_angles(first, second, DirectionRef::J2000)
            .convert_to(DirectionRef::AZEL, &frame)
            .ok()
            .map(|direction| direction.latitude_rad())
    }

    pub(in crate::ms::msexplore) fn image_curve(
        &self,
        window: i32,
        frequencies: &[f64],
        conditions: &AtmosphericConditions,
        overlay: Overlay,
    ) -> Result<Vec<f64>, casa_atmosphere::AtmosphereError> {
        let lo = usize::try_from(window)
            .ok()
            .and_then(|window| self.oscillators.get(window))
            .copied();
        conditions.image_curve(frequencies, lo, overlay)
    }
}

fn selected_observation_samples(
    ms: &MeasurementSet,
    selection: &MsSelection,
) -> Result<BTreeMap<Chunk, Vec<(f64, i32)>>, String> {
    use casa_plot_data::msexplore::{selected_f64_values, selected_i32_values};

    let rows = casa_ms::selection::resolve_casa_row_indices(ms, selection)
        .map_err(|error| error.to_string())?;
    let data_descriptions = ms.data_description().map_err(|error| error.to_string())?;
    let windows = (0..data_descriptions.row_count())
        .map(|row| data_descriptions.spectral_window_id(row))
        .collect::<Result<Vec<_>, _>>()
        .map_err(|error| error.to_string())?;
    let main = ms.main_table();
    let times = selected_f64_values(main, "TIME", &rows)?;
    let fields = selected_i32_values(main, "FIELD_ID", &rows)?;
    let scans = selected_i32_values(main, "SCAN_NUMBER", &rows)?;
    let descriptions = selected_i32_values(main, "DATA_DESC_ID", &rows)?;
    let mut samples = BTreeMap::<_, Vec<_>>::new();
    for (((time, field), scan), description) in
        times.into_iter().zip(fields).zip(scans).zip(descriptions)
    {
        let window = *windows
            .get(description as usize)
            .ok_or("invalid DATA_DESC_ID")?;
        samples
            .entry((window, scan))
            .or_default()
            .push((time, field));
    }
    Ok(samples)
}

fn time_bounds(rows: &[(f64, i32)]) -> [f64; 2] {
    rows.iter()
        .map(|&(time, _)| time)
        .fold([f64::INFINITY, f64::NEG_INFINITY], |[min, max], time| {
            [min.min(time), max.max(time)]
        })
}

fn angular_radians(value: f64, unit: &str) -> f64 {
    match unit {
        "deg" => value.to_radians(),
        "arcmin" => (value / 60.0).to_radians(),
        "arcsec" => (value / 3600.0).to_radians(),
        _ => value,
    }
}

fn mean(values: impl IntoIterator<Item = f64>) -> Option<f64> {
    let (sum, count) = values
        .into_iter()
        .fold((0.0, 0), |(sum, count), value| (sum + value, count + 1));
    (count > 0).then(|| sum / f64::from(count))
}

fn nearest_samples<T: Copy>(samples: &[(f64, T)], bounds: [f64; 2]) -> Vec<T> {
    let mut closest = f64::INFINITY;
    let mut values = Vec::new();
    for &(time, value) in samples {
        let distance = (bounds[0] - time).max(time - bounds[1]).max(0.0);
        if distance < closest {
            closest = distance;
            values.clear();
        }
        if distance == closest {
            values.push(value);
        }
    }
    values
}

fn attached(ms: &MeasurementSet, name: &str) -> Option<Table> {
    let path = ms.path()?;
    ms.main_table().keywords().get(name)?.as_table_ref()?;
    subtable(ms.main_table(), path, name).ok()
}

fn read_water(ms: &MeasurementSet) -> Vec<(f64, f64)> {
    let Some(wvr) = attached(ms, "ASDM_CALWVR") else {
        return Vec::new();
    };
    if wvr.row_count() > 0 {
        return (0..wvr.row_count())
            .filter_map(|row| {
                Some((
                    number(&wvr, row, "startValidTime")?,
                    number(&wvr, row, "water")?,
                ))
            })
            .collect();
    }
    let Some(atm) = attached(ms, "ASDM_CALATMOSPHERE") else {
        return Vec::new();
    };
    (0..atm.row_count())
        .filter_map(|row| {
            let ArrayValue::Float64(values) = array(&atm, row, "water").ok()? else {
                return None;
            };
            Some((number(&atm, row, "startValidTime")?, *values.iter().next()?))
        })
        .collect()
}

fn read_oscillators(ms: &MeasurementSet) -> Vec<f64> {
    let Some(receiver) = attached(ms, "ASDM_RECEIVER") else {
        return Vec::new();
    };
    let mut seen = BTreeSet::new();
    let mut seen_wvr = false;
    let mut max_window = 0;
    let mut values = Vec::new();
    for row in 0..receiver.row_count() {
        let Some(id) = text(&receiver, row, "spectralWindowId") else {
            continue;
        };
        if let Some(window) = id
            .rsplit('_')
            .next()
            .and_then(|id| id.parse::<usize>().ok())
        {
            max_window = max_window.max(window);
        }
        if !seen.insert(id) {
            continue;
        }
        if text(&receiver, row, "name").is_some_and(|name| name.contains("WVR"))
            && std::mem::replace(&mut seen_wvr, true)
        {
            continue;
        }
        if let Ok(ArrayValue::Float64(freq)) = array(&receiver, row, "freqLO")
            && let Some(lo) = freq.iter().next()
        {
            values.push(*lo);
        }
    }
    if ms
        .subtable(SubtableId::SpectralWindow)
        .is_some_and(|window| window.row_count() == max_window + 1)
    {
        values
    } else {
        Vec::new()
    }
}

#[derive(Default)]
struct Weather {
    samples: Vec<(f64, [f64; 3])>,
    pressure_pa: bool,
    temperature_c: bool,
}

impl Weather {
    fn read(ms: &MeasurementSet, telescope: &str) -> Self {
        let Some(table) = ms.subtable(SubtableId::Weather) else {
            return Self::default();
        };
        let (Some(pressure_units), Some(temperature_units)) = (
            first_unit(table, "PRESSURE"),
            first_unit(table, "TEMPERATURE"),
        ) else {
            return Self::default();
        };
        let pressure_pa = pressure_units == "Pa";
        let temperature_c = temperature_units == "C";
        let filter_pa = pressure_units != "hPa"
            && !(pressure_pa
                && (0..table.row_count())
                    .filter_map(|row| number(table, row, "PRESSURE"))
                    .any(|pressure| pressure > 0.0 && pressure < 1013.25));
        let itinerant_station = (telescope == "ALMA")
            .then(|| {
                let stations = attached(ms, "ASDM_STATION")?;
                (0..stations.row_count())
                    .find(|&row| text(&stations, row, "name") == Some("MeteoItinerant"))
                    .and_then(|row| i32::try_from(row).ok())
            })
            .flatten();
        let samples = (0..table.row_count())
            .filter_map(|row| {
                if itinerant_station.is_some()
                    && integer(table, row, "NS_WX_STATION_ID") == itinerant_station
                {
                    return None;
                }
                let pressure = number(table, row, "PRESSURE")?;
                let humidity = number(table, row, "REL_HUMIDITY")?;
                let temperature = number(table, row, "TEMPERATURE")?;
                let allowed_pressure = if filter_pa {
                    45000.0..=90000.0
                } else {
                    450.0..=900.0
                };
                let allowed_temperature = if temperature_c {
                    -30.0..=30.0
                } else {
                    f64::from(243.15_f32)..=f64::from(303.15_f32)
                };
                if allowed_pressure.contains(&pressure)
                    && allowed_temperature.contains(&temperature)
                    && (-10.0..=110.0).contains(&humidity)
                {
                    Some((
                        number(table, row, "TIME")?,
                        [pressure, humidity, temperature],
                    ))
                } else {
                    None
                }
            })
            .collect();
        Self {
            samples,
            pressure_pa,
            temperature_c,
        }
    }

    fn apply(&self, bounds: [f64; 2], conditions: &mut AtmosphericConditions) {
        let values = nearest_samples(&self.samples, bounds);
        let averaged =
            |index| mean(values.iter().map(|value| value[index])).unwrap_or_default() as f32;
        let mut pressure = averaged(0);
        let humidity = averaged(1);
        let mut temperature = averaged(2);
        if self.pressure_pa && pressure > 1013.25 {
            pressure /= 100.0;
        }
        if self.temperature_c && temperature > 0.0 {
            temperature += 273.15_f32;
        }
        if pressure != 0.0 {
            conditions.profile.ground_pressure_hpa = f64::from(pressure);
        }
        if humidity != 0.0 {
            conditions.profile.relative_humidity_percent = f64::from(humidity);
        }
        if temperature != 0.0 {
            conditions.profile.ground_temperature_k = f64::from(temperature);
        }
    }
}

fn first_unit<'a>(table: &'a Table, name: &str) -> Option<&'a str> {
    let keywords = table.column_keywords(name)?;
    let Value::Array(ArrayValue::String(units)) = keywords.get("QuantumUnits")? else {
        return None;
    };
    units.iter().next().map(String::as_str)
}

fn measurement_times(
    ms: &MeasurementSet,
    expressions: &Expressions<'_>,
) -> Result<BTreeMap<Chunk, [f64; 2]>, String> {
    let expression = |value: &str| (!value.is_empty()).then(|| value.to_owned());
    let selection = MsSelection {
        selectdata: true,
        antenna: expression(expressions.antenna),
        field: expression(expressions.field),
        spw: expression(expressions.spw),
        scan: expression(expressions.scan),
        timerange: expression(expressions.timerange),
        observation: expression(expressions.observation),
        ..MsSelection::default()
    };
    let rows = casa_ms::selection::resolve_row_indices_with_system_budget(ms, &selection)
        .map_err(|error| error.to_string())?;
    let data_descriptions = ms.data_description().map_err(|error| error.to_string())?;
    let windows = (0..data_descriptions.row_count())
        .map(|row| data_descriptions.spectral_window_id(row))
        .collect::<Result<Vec<_>, _>>()
        .map_err(|error| error.to_string())?;
    let main = ms.main_table();
    let mut chunks = BTreeMap::<_, [f64; 2]>::new();
    for row in rows {
        let Some(time) = number(main, row, "TIME") else {
            continue;
        };
        let Some(scan) = integer(main, row, "SCAN_NUMBER") else {
            continue;
        };
        let Some(window) = integer(main, row, "DATA_DESC_ID")
            .and_then(|id| usize::try_from(id).ok())
            .and_then(|id| windows.get(id))
        else {
            continue;
        };
        let bounds = chunks.entry((*window, scan)).or_insert([time; 2]);
        *bounds = [bounds[0].min(time), bounds[1].max(time)];
    }
    Ok(chunks)
}

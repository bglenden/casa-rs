// SPDX-License-Identifier: LGPL-3.0-or-later
//! Integration and calibration records for single-dish MeasurementSets.

use std::collections::{BTreeMap, HashMap};

use casa_core::Complex32;
use casa_core::{ArrayValue, RecordValue, ScalarValue, Value};
use ndarray::{Array2, ArrayD, ShapeBuilder};

use super::metadata::{direction_cell, insert, state_row, without};
use super::{Basis, Direction, Integration, Plane, Weather};
use casa_ms::schema::{self, SubtableId};
use casa_ms::subtable_rows::{
    b, bool_array, complex_array, f, f32_array, i, i32_array, row_from_defs, string_array,
    subtable_mut,
};
use casa_ms::{MeasurementSet, MsError, MsResult, OptionalMainColumn};

pub(super) struct Writer {
    pub(super) ms: MeasurementSet,
    column: OptionalMainColumn,
    descriptions: HashMap<(i32, i32), i32>,
    polarizations: HashMap<Vec<i32>, i32>,
    states: HashMap<(&'static str, i32), i32>,
    feeds: HashMap<(i32, i32, i32, Basis), i32>,
    reference_feed: Option<i32>,
    pointing: BTreeMap<(i32, u64), (Direction, f64)>,
    weather: Vec<((i32, Weather), TimeSpan)>,
    calibration: Vec<(Calibration, TimeSpan)>,
}

#[derive(Clone, Copy)]
struct TimeSpan {
    time: f64,
    interval: f64,
}

#[derive(PartialEq)]
struct Temperatures {
    spectral: bool,
    width: usize,
    count: usize,
    values: Vec<f32>,
}

#[derive(PartialEq)]
struct Calibration {
    antenna: i32,
    feed: i32,
    window: i32,
    system: Temperatures,
    calibration: Option<Temperatures>,
}

impl Writer {
    pub(super) fn new(ms: MeasurementSet, column: OptionalMainColumn) -> Self {
        Self {
            ms,
            column,
            descriptions: HashMap::new(),
            polarizations: HashMap::new(),
            states: HashMap::new(),
            feeds: HashMap::new(),
            reference_feed: None,
            pointing: BTreeMap::new(),
            weather: Vec::new(),
            calibration: Vec::new(),
        }
    }

    pub(super) fn write(
        &mut self,
        row: &Integration,
        planes: BTreeMap<usize, Plane>,
        channels: usize,
    ) -> MsResult<()> {
        let indices = planes.keys().copied().collect::<Vec<_>>();
        let correlations = row.basis.correlations(&indices)?;
        if planes.values().any(|plane| {
            plane.data.len() != channels
                || plane
                    .flag
                    .as_ref()
                    .is_some_and(|flags| flags.len() != channels)
        }) {
            return Err(MsError::InvalidInput(format!(
                "Imported spectral window {} expects {channels} channels",
                row.window
            )));
        }
        let codes = correlations
            .iter()
            .map(|&(code, _)| code)
            .collect::<Vec<_>>();
        let polarization = self.polarization_id(&codes)?;
        let description = self.description_id(row.window, polarization)?;
        self.record_feed(row)?;
        self.record_pointing(row);
        self.record_conditions(row, &correlations, &planes, channels);
        let state = match self.states.get(&row.state) {
            Some(&state) => state,
            None => {
                let state = state_row(&mut self.ms, row.state.0, row.state.1)?;
                self.states.insert(row.state, state);
                state
            }
        };
        let data = main_row(
            row,
            description,
            state,
            &correlations,
            &planes,
            channels,
            self.column,
        );
        self.ms.main_table_mut().add_row(data)?;
        Ok(())
    }

    fn polarization_id(&mut self, codes: &[i32]) -> MsResult<i32> {
        if let Some(&id) = self.polarizations.get(codes) {
            return Ok(id);
        }
        let n = codes.len();
        let pairs = codes
            .iter()
            .map(|&code| match code {
                6 | 10 => [0, 1], // RL and XY
                7 | 11 => [1, 0], // LR and YX
                8 | 12 => [1, 1], // LL and YY
                _ => [0, 0],
            })
            .collect::<Vec<_>>();
        let receptors = (0..2)
            .flat_map(|receptor| pairs.iter().map(move |pair| pair[receptor]))
            .collect::<Vec<_>>();
        let id = insert(
            &mut self.ms,
            SubtableId::Polarization,
            &[
                ("CORR_TYPE", i32_array(codes, vec![n])),
                ("CORR_PRODUCT", i32_array(&receptors, vec![2, n])),
                ("NUM_CORR", i(n as i32)),
            ],
        )?;
        self.polarizations.insert(codes.to_vec(), id);
        Ok(id)
    }

    fn description_id(&mut self, window: i32, polarization: i32) -> MsResult<i32> {
        if let Some(&id) = self.descriptions.get(&(window, polarization)) {
            return Ok(id);
        }
        let id = insert(
            &mut self.ms,
            SubtableId::DataDescription,
            &[
                ("POLARIZATION_ID", i(polarization)),
                ("SPECTRAL_WINDOW_ID", i(window)),
            ],
        )?;
        self.descriptions.insert((window, polarization), id);
        Ok(id)
    }

    fn record_feed(&mut self, row: &Integration) -> MsResult<()> {
        let basis = if row.basis == Basis::Circular {
            Basis::Circular
        } else {
            Basis::Linear
        };
        let key = (row.antenna, row.feed, row.window, basis);
        if let std::collections::hash_map::Entry::Vacant(entry) = self.feeds.entry(key) {
            let id = insert(
                &mut self.ms,
                SubtableId::Feed,
                &[
                    ("ANTENNA_ID", i(row.antenna)),
                    ("FEED_ID", i(row.feed)),
                    ("NUM_RECEPTORS", i(2)),
                    ("SPECTRAL_WINDOW_ID", i(row.window)),
                    (
                        "BEAM_OFFSET",
                        casa_ms::subtable_rows::f64_array(&[0.; 4], vec![2, 2]),
                    ),
                    ("POLARIZATION_TYPE", string_array(&basis.feeds(), vec![2])),
                    (
                        "POL_RESPONSE",
                        complex_array(&[Complex32::default(); 4], vec![2, 2]),
                    ),
                    (
                        "RECEPTOR_ANGLE",
                        casa_ms::subtable_rows::f64_array(&[0.; 2], vec![2]),
                    ),
                ],
            )?;
            entry.insert(id);
        }
        self.reference_feed.get_or_insert(row.feed);
        Ok(())
    }

    fn record_pointing(&mut self, row: &Integration) {
        if self.reference_feed == Some(row.feed) {
            self.pointing
                .entry((row.antenna, row.time.to_bits()))
                .or_insert((row.direction, row.interval));
        }
    }

    fn record_conditions(
        &mut self,
        row: &Integration,
        correlations: &[(i32, usize)],
        planes: &BTreeMap<usize, Plane>,
        channels: usize,
    ) {
        let time = TimeSpan {
            time: row.time,
            interval: row.interval,
        };
        if let Some(weather) = row.weather {
            update_unique(&mut self.weather, (row.antenna, weather), time);
        }
        if !row.basis.has_parallel_hands() {
            return;
        }
        let hand_indices = correlations
            .iter()
            .map(|&(_, index)| index)
            .filter(|&index| index < 2);
        let hands = hand_indices
            .map(|index| &planes[&index])
            .collect::<Vec<_>>();
        let system = temperatures(&hands, channels, |plane| &plane.system_temperature);
        if let Some(system) = system {
            let mut calibration = Calibration {
                antenna: row.antenna,
                feed: row.feed,
                window: row.window,
                system,
                calibration: temperatures(&hands, channels, |plane| &plane.calibration_temperature),
            };
            if correlations.len() != hands.len() {
                calibration.system.reduce_to_scalars();
                if let Some(temperature) = &mut calibration.calibration {
                    temperature.reduce_to_scalars();
                }
            }
            update_unique(&mut self.calibration, calibration, time);
        }
    }

    pub(super) fn finish(mut self) -> MsResult<MeasurementSet> {
        let pointing = subtable_mut(&mut self.ms, SubtableId::Pointing)?;
        for ((antenna, time), (direction, interval)) in self.pointing {
            let (degree, direction) = direction_cell(direction);
            let record = row_from_defs(
                schema::pointing::REQUIRED_COLUMNS,
                &[
                    ("ANTENNA_ID", i(antenna)),
                    ("TIME", f(f64::from_bits(time))),
                    ("INTERVAL", f(interval)),
                    ("NUM_POLY", i(degree)),
                    ("DIRECTION", direction),
                ],
            );
            pointing.add_row(without(record, &["TARGET"]))?;
        }
        let weather = subtable_mut(&mut self.ms, SubtableId::Weather)?;
        for ((antenna, sample), span) in self.weather {
            let mut row = row_from_defs(
                schema::weather::REQUIRED_COLUMNS,
                &[
                    ("ANTENNA_ID", i(antenna)),
                    ("TIME", f(span.time)),
                    ("INTERVAL", f(span.interval)),
                ],
            );
            for (name, value) in [
                "TEMPERATURE",
                "PRESSURE",
                "REL_HUMIDITY",
                "WIND_SPEED",
                "WIND_DIRECTION",
            ]
            .into_iter()
            .zip(sample.0)
            {
                row.upsert(name, Value::Scalar(ScalarValue::Float32(value)));
            }
            weather.add_row(row)?;
        }
        let syscal = subtable_mut(&mut self.ms, SubtableId::SysCal)?;
        for (sample, span) in self.calibration {
            let mut row = row_from_defs(
                schema::syscal::REQUIRED_COLUMNS,
                &[
                    ("ANTENNA_ID", i(sample.antenna)),
                    ("TIME", f(span.time)),
                    ("INTERVAL", f(span.interval)),
                    ("FEED_ID", i(sample.feed)),
                    ("SPECTRAL_WINDOW_ID", i(sample.window)),
                ],
            );
            sample.system.add_cell(&mut row, "TSYS", "TSYS_SPECTRUM");
            if let Some(temperature) = sample.calibration {
                temperature.add_cell(&mut row, "TCAL", "TCAL_SPECTRUM");
            }
            syscal.add_row(row)?;
        }
        Ok(self.ms)
    }
}

fn main_row(
    row: &Integration,
    description: i32,
    state: i32,
    correlations: &[(i32, usize)],
    planes: &BTreeMap<usize, Plane>,
    channels: usize,
    column: OptionalMainColumn,
) -> RecordValue {
    let ordered = correlations
        .iter()
        .map(|&(_, plane)| &planes[&plane])
        .collect::<Vec<_>>();
    let count = ordered.len();
    let full = count == 4 && row.basis.has_parallel_hands();
    let data = visibility_data(&ordered, channels, full, column);
    let flags = if ordered.iter().all(|plane| plane.flag.is_none()) {
        Array2::<bool>::default((count, channels).f()).into_dyn()
    } else {
        let flagged =
            |plane: &Plane, channel| plane.flag.as_ref().is_some_and(|flags| flags[channel]);
        channel_major(&ordered, channels, |plane, channel| {
            if full && plane.index >= 2 {
                flagged(ordered[1], channel) || flagged(ordered[2], channel)
            } else {
                flagged(plane, channel)
            }
        })
    };
    let noise = f32_array(&vec![1.; count], vec![count]);
    let mut record = row_from_defs(
        schema::main_table::REQUIRED_COLUMNS,
        &[
            ("TIME", f(row.time)),
            ("TIME_CENTROID", f(row.time)),
            ("INTERVAL", f(row.interval)),
            ("EXPOSURE", f(row.interval)),
            ("SCAN_NUMBER", i(row.scan)),
            ("ANTENNA1", i(row.antenna)),
            ("ANTENNA2", i(row.antenna)),
            ("FIELD_ID", i(row.field)),
            ("FEED1", i(row.feed)),
            ("FEED2", i(row.feed)),
            ("DATA_DESC_ID", i(description)),
            ("STATE_ID", i(state)),
            ("FLAG_ROW", b(planes.values().any(|plane| plane.flag_row))),
            ("FLAG_CATEGORY", bool_array(&[], vec![0, 0, 0])),
            ("WEIGHT", noise.clone()),
            ("SIGMA", noise),
        ],
    );
    record.upsert("FLAG", Value::Array(ArrayValue::Bool(flags)));
    record.upsert(column.name(), data);
    record
}

fn visibility_data(
    planes: &[&Plane],
    channels: usize,
    full: bool,
    column: OptionalMainColumn,
) -> Value {
    if column == OptionalMainColumn::FloatData {
        let data = channel_major(planes, channels, |plane, channel| {
            if full && plane.index == 3 {
                planes[1].data[channel]
            } else {
                plane.data[channel]
            }
        });
        return Value::Array(ArrayValue::Float32(data));
    }
    let data = channel_major(planes, channels, |plane, channel| {
        match (full, plane.index) {
            (true, 2) => Complex32::new(planes[1].data[channel], planes[2].data[channel]),
            (true, 3) => Complex32::new(planes[1].data[channel], -planes[2].data[channel]),
            _ => Complex32::new(plane.data[channel], 0.),
        }
    });
    Value::Array(ArrayValue::Complex32(data))
}

/// Construct contiguous casacore-order cells directly from the input planes.
fn channel_major<T>(
    planes: &[&Plane],
    channels: usize,
    sample: impl Fn(&Plane, usize) -> T,
) -> ArrayD<T> {
    let mut values = Vec::with_capacity(planes.len() * channels);
    for channel in 0..channels {
        values.extend(planes.iter().map(|plane| sample(plane, channel)));
    }
    Array2::from_shape_vec((planes.len(), channels).f(), values)
        .expect("one value per correlation and channel")
        .into_dyn()
}

fn update_unique<T: PartialEq>(records: &mut Vec<(T, TimeSpan)>, value: T, span: TimeSpan) {
    if let Some((_, previous)) = records.iter_mut().find(|(existing, _)| *existing == value) {
        let lower = previous.time - previous.interval / 2.;
        let upper = previous.time + previous.interval / 2.;
        let start = lower.min(span.time - span.interval / 2.);
        let end = upper.max(span.time + span.interval / 2.);
        if start != lower || end != upper {
            *previous = TimeSpan {
                time: (start + end) / 2.,
                interval: end - start,
            };
        }
    } else {
        records.push((value, span));
    }
}

fn temperatures(
    hands: &[&Plane],
    channels: usize,
    extract: impl Fn(&Plane) -> &[f32],
) -> Option<Temperatures> {
    let inputs = hands.iter().map(|plane| extract(plane)).collect::<Vec<_>>();
    if channels > 1
        && !inputs
            .iter()
            .any(|values| values.iter().any(|&value| value > 0.))
    {
        return None;
    }
    let spectral = channels == 1
        || inputs
            .iter()
            .any(|values| values.len() == channels && values[1..].iter().any(|&value| value > 0.));
    let width = if spectral { channels } else { 1 };
    let values = inputs
        .into_iter()
        .flat_map(|input| {
            (0..width).map(move |channel| {
                if channel == 0 || input.len() == channels {
                    input.get(channel).copied().unwrap_or(-1.)
                } else {
                    -1.
                }
            })
        })
        .collect::<Vec<_>>();
    if values.iter().all(|&value| value == 0.) || values.iter().all(|&value| value == 1.) {
        return None;
    }
    Some(Temperatures {
        spectral,
        width,
        count: hands.len(),
        values,
    })
}

impl Temperatures {
    fn add_cell(self, row: &mut RecordValue, scalar: &str, array: &str) {
        if self.spectral {
            row.upsert(array, f32_array(&self.values, vec![self.count, self.width]));
        } else {
            row.upsert(scalar, f32_array(&self.values, vec![self.count]));
        }
    }

    fn reduce_to_scalars(&mut self) {
        self.values = self.values.iter().step_by(self.width).copied().collect();
        self.spectral = false;
        self.width = 1;
    }
}

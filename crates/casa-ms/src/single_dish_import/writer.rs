// SPDX-License-Identifier: LGPL-3.0-or-later
//! Integration and calibration records for single-dish MeasurementSets.

use std::collections::{BTreeMap, HashMap};

use casa_types::{RecordValue, ScalarValue, Value};
use num_complex::Complex32;

use super::metadata::{direction_cell, insert, state_row, without};
use super::{Basis, Integration, Plane, Weather};
use crate::schema::{self, SubtableId};
use crate::subtable_rows::{
    b, bool_array, complex_array, f, f32_array, i, i32_array, row_from_defs, string_array,
    subtable_mut,
};
use crate::{MeasurementSet, MsError, MsResult, OptionalMainColumn};

pub(super) struct Writer {
    pub(super) ms: MeasurementSet,
    column: OptionalMainColumn,
    descriptions: HashMap<(i32, i32), i32>,
    polarizations: HashMap<Vec<i32>, i32>,
    states: HashMap<(&'static str, i32), i32>,
    feeds: HashMap<(i32, i32, Basis), i32>,
    reference_feed: Option<i32>,
    pointing_time: Option<f64>,
    weather: Vec<(Weather, TimeSpan)>,
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
            pointing_time: None,
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
        if planes
            .values()
            .any(|plane| plane.data.len() != channels || plane.flag.len() != channels)
        {
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
        self.record_pointing(row)?;
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
        let key = (row.feed, row.window, basis);
        if let std::collections::hash_map::Entry::Vacant(entry) = self.feeds.entry(key) {
            let id = insert(
                &mut self.ms,
                SubtableId::Feed,
                &[
                    ("FEED_ID", i(row.feed)),
                    ("NUM_RECEPTORS", i(2)),
                    ("SPECTRAL_WINDOW_ID", i(row.window)),
                    (
                        "BEAM_OFFSET",
                        crate::subtable_rows::f64_array(&[0.; 4], vec![2, 2]),
                    ),
                    ("POLARIZATION_TYPE", string_array(&basis.feeds(), vec![2])),
                    (
                        "POL_RESPONSE",
                        complex_array(&[Complex32::default(); 4], vec![2, 2]),
                    ),
                    (
                        "RECEPTOR_ANGLE",
                        crate::subtable_rows::f64_array(&[0.; 2], vec![2]),
                    ),
                ],
            )?;
            entry.insert(id);
        }
        self.reference_feed.get_or_insert(row.feed);
        Ok(())
    }

    fn record_pointing(&mut self, row: &Integration) -> MsResult<()> {
        if self.reference_feed != Some(row.feed) || self.pointing_time == Some(row.time) {
            return Ok(());
        }
        self.pointing_time = Some(row.time);
        let (degree, direction) = direction_cell(row.direction);
        let record = row_from_defs(
            schema::pointing::REQUIRED_COLUMNS,
            &[
                ("TIME", f(row.time)),
                ("INTERVAL", f(row.interval)),
                ("NUM_POLY", i(degree)),
                ("DIRECTION", direction),
            ],
        );
        subtable_mut(&mut self.ms, SubtableId::Pointing)?.add_row(without(record, &["TARGET"]))?;
        Ok(())
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
            update_unique(&mut self.weather, weather, time);
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
        let weather = subtable_mut(&mut self.ms, SubtableId::Weather)?;
        for (sample, span) in self.weather {
            let mut row = row_from_defs(
                schema::weather::REQUIRED_COLUMNS,
                &[("TIME", f(span.time)), ("INTERVAL", f(span.interval))],
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
    let count = correlations.len();
    let shape = vec![count, channels];
    let full = count == 4 && row.basis.has_parallel_hands();
    let data = if full {
        let mut data = Vec::with_capacity(4 * channels);
        for &(_, plane) in correlations {
            if plane < 2 {
                data.extend(
                    planes[&plane]
                        .data
                        .iter()
                        .map(|&real| Complex32::new(real, 0.)),
                );
            } else {
                let sign = if plane == 2 { 1.0 } else { -1.0 };
                data.extend(
                    planes[&2]
                        .data
                        .iter()
                        .zip(&planes[&3].data)
                        .map(|(&real, &imag)| Complex32::new(real, sign * imag)),
                );
            }
        }
        complex_array(&data, shape.clone())
    } else {
        let data = correlations
            .iter()
            .flat_map(|&(_, plane)| planes[&plane].data.iter().copied())
            .collect::<Vec<_>>();
        if column == OptionalMainColumn::FloatData {
            f32_array(&data, shape.clone())
        } else {
            complex_array(
                &data
                    .into_iter()
                    .map(|real| Complex32::new(real, 0.))
                    .collect::<Vec<_>>(),
                shape.clone(),
            )
        }
    };
    let mut flags = Vec::with_capacity(count * channels);
    for &(_, plane) in correlations {
        if full && plane > 1 {
            flags.extend(
                planes[&2]
                    .flag
                    .iter()
                    .zip(&planes[&3].flag)
                    .map(|(&left, &right)| left || right),
            );
        } else {
            flags.extend_from_slice(&planes[&plane].flag);
        }
    }
    let noise = f32_array(&vec![1.; count], vec![count]);
    let mut record = row_from_defs(
        schema::main_table::REQUIRED_COLUMNS,
        &[
            ("TIME", f(row.time)),
            ("TIME_CENTROID", f(row.time)),
            ("INTERVAL", f(row.interval)),
            ("EXPOSURE", f(row.interval)),
            ("SCAN_NUMBER", i(row.scan)),
            ("FIELD_ID", i(row.field)),
            ("FEED1", i(row.feed)),
            ("FEED2", i(row.feed)),
            ("DATA_DESC_ID", i(description)),
            ("STATE_ID", i(state)),
            ("FLAG", bool_array(&flags, shape)),
            ("FLAG_ROW", b(planes.values().any(|plane| plane.flag_row))),
            ("FLAG_CATEGORY", bool_array(&[], vec![0, 0, 0])),
            ("WEIGHT", noise.clone()),
            ("SIGMA", noise),
        ],
    );
    record.upsert(column.name(), data);
    record
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

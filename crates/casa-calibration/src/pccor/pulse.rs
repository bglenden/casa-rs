// SPDX-License-Identifier: LGPL-3.0-or-later
//! Time-resolved tone phases and selected-interval pulse calibration.

use std::collections::BTreeMap;
use std::f64::consts::TAU;
use std::path::Path;

use casa_tables::RequiredScalarColumnValues::{Float64, Int32};
use casa_tables::{Table, TableOptions};
use casa_types::{ArrayValue, Complex64};
use ndarray::ArrayD;

use super::PccorError;
use super::selection::Selection;

pub(super) type Pair = (i32, i32);

#[derive(Default)]
pub(super) struct PulseSeries {
    pub(super) samples: Vec<PulseSample>,
    pub(super) mean_phase: [Vec<f64>; 2],
    pub(super) mean_delay: [f64; 2],
    phase_sum: [Vec<Complex64>; 2],
    delay_sum: [f64; 2],
    count: u32,
}

pub(super) struct PulseSample {
    pub(super) time: f64,
    pub(super) phase: [f64; 2],
    pub(super) delay: [f64; 2],
    pub(super) cable: f64,
}

pub(super) fn read(
    ms_path: &Path,
    selection: &Selection,
) -> Result<BTreeMap<Pair, PulseSeries>, PccorError> {
    let unreadable = |error: casa_tables::TableError| {
        tracing::debug!(%error, path = %ms_path.display(), "cannot read PHASE_CAL");
        missing_subtable(ms_path)
    };
    let table = Table::open(TableOptions::new(ms_path.join("PHASE_CAL"))).map_err(unreadable)?;
    let mut columns = table
        .required_scalar_columns_owned(&["ANTENNA_ID", "SPECTRAL_WINDOW_ID", "TIME", "CABLE_CAL"])
        .map_err(unreadable)?;
    let (Some(Int32(antennas)), Some(Int32(windows)), Some(Float64(times)), Some(Float64(cables))) = (
        columns.remove("ANTENNA_ID"),
        columns.remove("SPECTRAL_WINDOW_ID"),
        columns.remove("TIME"),
        columns.remove("CABLE_CAL"),
    ) else {
        return Err(missing_subtable(ms_path));
    };
    let tones = table
        .column_accessor("TONE_FREQUENCY")
        .map_err(unreadable)?;
    let phases = table.column_accessor("PHASE_CAL").map_err(unreadable)?;
    let [start, stop] = selection.interval;
    let placeholder_tones = if times
        .first()
        .is_some_and(|time| (start..=stop).contains(time))
    {
        let phasors = phases.array_cell(0).map_err(unreadable)?;
        *phasors
            .shape()
            .get(1)
            .ok_or_else(|| missing_subtable(ms_path))?
    } else {
        1
    };
    let mut series = BTreeMap::<Pair, PulseSeries>::new();
    let rows: Vec<_> = antennas
        .iter()
        .zip(&windows)
        .enumerate()
        .filter_map(|(row, (antenna, window))| {
            (selection.antennas.contains(antenna) && selection.windows.contains(window))
                .then_some(row)
        })
        .collect();
    for chunk in rows.chunks(4096) {
        let frequency_cells = tones
            .array_cells_owned_uncached(chunk)
            .map_err(unreadable)?;
        let phase_cells = phases
            .array_cells_owned_uncached(chunk)
            .map_err(unreadable)?;
        for ((&row, frequencies), phasors) in chunk.iter().zip(frequency_cells).zip(phase_cells) {
            let (Some(ArrayValue::Float64(frequencies)), Some(phasors)) = (frequencies, phasors)
            else {
                return Err(missing_subtable(ms_path));
            };
            let pulse = series.entry((antennas[row], windows[row])).or_default();
            pulse.observe(
                times[row],
                cables[row],
                &frequencies,
                &phasors,
                selection.interval,
            )?;
        }
    }
    for &antenna in &selection.antennas {
        for &window in &selection.windows {
            let series = series.entry((antenna, window)).or_default();
            if antenna == selection.reference && series.count == 0 {
                let name = &selection.antenna_names[antenna as usize];
                return Err(PccorError::Execution(format!(
                    "Missing pulse cal data for the reference antenna ({name}) nothing to do.",
                )));
            }
            series.finish(placeholder_tones);
        }
    }
    Ok(series)
}

fn missing_subtable(ms_path: &Path) -> PccorError {
    PccorError::Execution(format!(
        "{} does not contain a PHASE_CAL subtable",
        ms_path.display()
    ))
}

impl PulseSeries {
    fn observe(
        &mut self,
        time: f64,
        cable: f64,
        frequencies: &ArrayD<f64>,
        phasors: &ArrayValue,
        [start, stop]: [f64; 2],
    ) -> Result<(), PccorError> {
        let bad_shape = || PccorError::Execution("Invalid PHASE_CAL tone or receptor shape".into());
        let &[2, tones] = phasors.shape() else {
            return Err(bad_shape());
        };
        if tones == 0
            || frequencies.shape() != [2, tones]
            || !matches!(phasors, ArrayValue::Complex32(_) | ArrayValue::Complex64(_))
        {
            return Err(bad_shape());
        }
        // casatools promotes the on-disk complex values to doubles.
        let phasor = |hand, tone| match phasors {
            ArrayValue::Complex32(values) => {
                let value = values[[hand, tone]];
                Complex64::new(f64::from(value.re), f64::from(value.im))
            }
            ArrayValue::Complex64(values) => values[[hand, tone]],
            _ => unreachable!("checked complex tone type"),
        };
        let phase = [0, 1].map(|hand| phasor(hand, 0).arg());
        let delay = [0, 1].map(|hand| {
            let last = tones - 1;
            (phasor(hand, last).arg() - phase[hand])
                / (frequencies[[hand, last]] - frequencies[[hand, 0]])
                / TAU
        });
        self.samples.push(PulseSample {
            time,
            phase,
            delay,
            cable,
        });
        if (start..=stop).contains(&time) {
            self.add_to_mean(tones, phasor, delay)?;
        }
        Ok(())
    }

    fn add_to_mean(
        &mut self,
        tones: usize,
        phasor: impl Fn(usize, usize) -> Complex64,
        delay: [f64; 2],
    ) -> Result<(), PccorError> {
        if self.count == 0 {
            self.phase_sum = [0, 1].map(|hand| (0..tones).map(|tone| phasor(hand, tone)).collect());
            self.delay_sum = delay;
        } else {
            if tones != self.phase_sum[0].len() {
                return Err(PccorError::Execution(
                    "Inconsistent PHASE_CAL tone count".into(),
                ));
            }
            for (hand, sum) in self.phase_sum.iter_mut().enumerate() {
                self.delay_sum[hand] += delay[hand];
                for (tone, value) in sum.iter_mut().enumerate() {
                    *value += phasor(hand, tone);
                }
            }
        }
        self.count += 1;
        Ok(())
    }

    fn finish(&mut self, placeholder_tones: usize) {
        if self.count == 0 {
            // CASA creates one synthetic pulse at the first table time for a
            // missing antenna/window. When in range, it contributes every tone.
            let tones = if self.samples.is_empty() {
                placeholder_tones
            } else {
                1
            };
            self.mean_phase = [vec![0.; tones], vec![0.; tones]];
            return;
        }
        let count = f64::from(self.count);
        self.mean_phase = std::mem::take(&mut self.phase_sum)
            .map(|sum| sum.into_iter().map(|value| (value / count).arg()).collect());
        self.mean_delay = self.delay_sum.map(|sum| sum / count);
    }
}

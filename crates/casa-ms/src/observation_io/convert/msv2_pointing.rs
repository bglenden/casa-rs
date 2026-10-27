// SPDX-License-Identifier: LGPL-3.0-or-later
//! An MSv2 destination's `POINTING`: the samples the plan's reads gather
//! through the boundary ([`Observation::pointing`], [`PointingPlan::reads`]),
//! one row per sample, in (time, antenna) order as casacore's
//! `MSPointingColumns::pointingIndex` expects it. An MSv4 source states no
//! interval, so its rows have `INTERVAL` 0 (which casacore reads as covering
//! half the gap to the antenna's next row, and casa-rs's reader as covering
//! nothing); it holds no target, so `TARGET` repeats the direction, as MSv2
//! writers without a separate target write it.

use std::mem::size_of;

use casa_observation::{AntennaId, Observation, ObservationError, PointingSeries};
use casa_tables::table_measures::{MeasureType, TableMeasDesc};
use casa_types::measures::direction::DirectionRef;
use casa_types::measures::{EpochView, MjdHighPrec};
use ndarray::ArrayView1;

use super::pointing_plan::PointingPlan;
use crate::MeasurementSet;
use crate::observation_io::update::storage;
use crate::schema::{self, SubtableId};
use crate::subtable_rows::{b, f, f64_array, i, row_from_defs, s, subtable_mut};

/// One gathered `POINTING` row, its times in MJD seconds; its coefficients,
/// `[axis][term]`, are `terms` per axis from `start` in the gathered
/// coefficients.
#[derive(Clone, Copy, Debug)]
struct Row {
    /// The time's bit pattern, so that equal times compare equal.
    time_bits: u64,
    antenna: AntennaId,
    /// The row's place among its antenna's samples at its time.
    repeat: u32,
    interval_s: f64,
    origin_s: f64,
    start: usize,
    terms: usize,
}

/// The bytes the writer holds for `samples` gathered samples of
/// `coefficients` coefficients in all: it reserves exactly that and
/// removes repeats in place.
pub(super) fn gathered_bytes(samples: usize, coefficients: usize) -> usize {
    samples
        .saturating_mul(size_of::<Row>())
        .saturating_add(coefficients.saturating_mul(size_of::<f64>()))
}

/// Write `observation`'s pointing, as `plan` carries it, into `ms`'s
/// `POINTING`. Reads that hold the same sample (an MSv4's datasets each
/// hold their antennas' pointing) write it once: a sample is the k-th at
/// its antenna and time, and different samples there are refused.
pub(super) fn write(
    ms: &mut MeasurementSet,
    observation: &dyn Observation,
    plan: &PointingPlan,
) -> Result<(), ObservationError> {
    let catalogue = observation.catalogue();
    let (samples, held) = plan.gathered();
    let mut rows = Vec::with_capacity(samples);
    let mut coefficients = Vec::with_capacity(held);
    let mut frame = None;
    for read in plan.reads() {
        let series = observation.pointing(read)?;
        if series.antennas.is_empty() {
            continue;
        }
        if frame.is_some_and(|known| known != series.frame) {
            return Err(ObservationError::Unsupported {
                what: "pointing in different frames in different partitions",
            });
        }
        frame = Some(series.frame);
        let entry = catalogue.partition(read.partition);
        gather(&series, entry, &mut rows, &mut coefficients);
    }
    let Some(frame) = frame else {
        return Ok(());
    };
    rows.sort_by(|a, b| {
        f64::from_bits(a.time_bits)
            .total_cmp(&f64::from_bits(b.time_bits))
            .then(a.antenna.cmp(&b.antenna))
            .then(a.repeat.cmp(&b.repeat))
    });
    // Remove repeats in place: a sample two reads gathered is written once.
    let key = |row: &Row| (row.time_bits, row.antenna, row.repeat);
    let mut kept = 0;
    for index in 0..rows.len() {
        if kept > 0 && key(&rows[kept - 1]) == key(&rows[index]) {
            if !same_sample(&rows[kept - 1], &rows[index], &coefficients) {
                return Err(ObservationError::Unsupported {
                    what: "partitions with different pointing at one antenna and time",
                });
            }
            continue;
        }
        rows[kept] = rows[index];
        kept += 1;
    }
    rows.truncate(kept);
    let terms = rows.iter().map(|row| row.terms).max().unwrap_or(1);
    let table = subtable_mut(ms, SubtableId::Pointing).map_err(storage)?;
    if frame != DirectionRef::J2000 {
        for column in ["DIRECTION", "TARGET"] {
            TableMeasDesc::new_fixed(column, MeasureType::Direction, frame.as_str())
                .write(table)
                .map_err(storage)?;
        }
    }
    for row in rows {
        // `[2, terms]`, each axis's terms padded to the most any row has.
        let mut direction = Vec::with_capacity(2 * terms);
        for axis in 0..2 {
            let own = row.start + axis * row.terms;
            direction.extend_from_slice(&coefficients[own..own + row.terms]);
            direction.extend(std::iter::repeat_n(0.0, terms - row.terms));
        }
        let direction = f64_array(&direction, vec![2, terms]);
        let values = row_from_defs(
            schema::pointing::REQUIRED_COLUMNS,
            &[
                ("ANTENNA_ID", i(row.antenna.0 as i32)),
                ("TIME", f(f64::from_bits(row.time_bits))),
                ("INTERVAL", f(row.interval_s)),
                ("NAME", s("")),
                ("NUM_POLY", i(terms as i32 - 1)),
                ("TIME_ORIGIN", f(row.origin_s)),
                ("DIRECTION", direction.clone()),
                ("TARGET", direction),
                ("TRACKING", b(true)),
            ],
        );
        table.add_row(values).map_err(storage)?;
    }
    Ok(())
}

/// Add `series`' samples (in `entry`'s time system) to `rows`, their
/// coefficients to `coefficients`.
fn gather(
    series: &PointingSeries,
    entry: &casa_observation::PartitionEntry,
    rows: &mut Vec<Row>,
    coefficients: &mut Vec<f64>,
) {
    // Times in the MSv2's own system: MJD seconds.
    let mjd = |times: &[f64]| -> Vec<f64> {
        let view = EpochView::new(entry.time_scale, entry.time_origin, ArrayView1::from(times));
        (0..times.len())
            .map(|time| view.seconds_since(time, MjdHighPrec::new(0.0, 0.0)))
            .collect()
    };
    for (antenna, samples) in &series.antennas {
        let times = mjd(samples.times_s());
        let origins = mjd(samples.origins_s());
        let terms = samples.terms();
        let mut repeat = 0;
        for (sample, time) in times.iter().enumerate() {
            repeat = if sample > 0 && times[sample - 1].to_bits() == time.to_bits() {
                repeat + 1
            } else {
                0
            };
            rows.push(Row {
                time_bits: time.to_bits(),
                antenna: *antenna,
                repeat,
                interval_s: samples
                    .intervals_s()
                    .map_or(0.0, |intervals| intervals[sample]),
                origin_s: origins[sample],
                start: coefficients.len(),
                terms,
            });
            coefficients.extend_from_slice(
                &samples.coefficients()[sample * 2 * terms..(sample + 1) * 2 * terms],
            );
        }
    }
}

/// Whether two gathered rows are one sample: the same interval, origin and
/// polynomial, terms beyond either's own being zero (a read pads each
/// antenna's terms to the most any of its antennas has).
fn same_sample(a: &Row, b: &Row, coefficients: &[f64]) -> bool {
    let term = |row: &Row, axis: usize, term: usize| {
        if term < row.terms {
            coefficients[row.start + axis * row.terms + term]
        } else {
            0.0
        }
    };
    let terms = a.terms.max(b.terms);
    a.interval_s.to_bits() == b.interval_s.to_bits()
        && a.origin_s.to_bits() == b.origin_s.to_bits()
        && (0..2).all(|axis| {
            (0..terms).all(|index| term(a, axis, index).to_bits() == term(b, axis, index).to_bits())
        })
}

#[cfg(test)]
mod tests {
    use casa_observation::AntennaId;

    use super::{Row, same_sample};

    /// One sample read with its antenna's own terms and with terms padded
    /// to another antenna's is the same sample; a different term is not.
    #[test]
    fn padding_does_not_make_a_different_sample() {
        let row = |start, terms| Row {
            time_bits: 1.0_f64.to_bits(),
            antenna: AntennaId(1),
            repeat: 0,
            interval_s: 1.0,
            origin_s: 0.5,
            start,
            terms,
        };
        // [ra, dec]; [ra, 0, dec, 0]; [ra, 1e-6, dec, 0].
        let coefficients = [1.0, -0.5, 1.0, 0.0, -0.5, 0.0, 1.0, 1e-6, -0.5, 0.0];
        assert!(same_sample(&row(0, 1), &row(2, 2), &coefficients));
        assert!(!same_sample(&row(0, 1), &row(6, 2), &coefficients));
    }
}

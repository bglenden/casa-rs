// SPDX-License-Identifier: LGPL-3.0-or-later
//! An MSv2 destination's `POINTING`: every partition's pointing samples
//! through the boundary ([`Observation::pointing`]), one row per antenna and
//! time. An MSv4 source states no interval, so its rows have `INTERVAL` 0
//! (a row covering no time beyond its own); it holds no target, so `TARGET`
//! repeats the direction, as MSv2 writers without a separate target write
//! it.

use std::collections::BTreeMap;

use casa_observation::{
    AntennaId, Observation, ObservationError, PointingDirection, PointingRequest,
};
use casa_tables::table_measures::{MeasureType, TableMeasDesc};
use casa_types::measures::direction::DirectionRef;
use casa_types::measures::{EpochView, MjdHighPrec};
use ndarray::ArrayView1;

use crate::MeasurementSet;
use crate::observation_io::update::storage;
use crate::schema::{self, SubtableId};
use crate::subtable_rows::{b, f, f64_array, i, row_from_defs, s, subtable_mut};

/// One `POINTING` row, its times in MJD seconds.
#[derive(Clone, Debug, PartialEq)]
struct Row {
    interval_s: f64,
    origin_s: f64,
    /// `[axis][term]`.
    coefficients: Vec<f64>,
}

/// Write `observation`'s pointing into `ms`'s `POINTING`. Partitions that
/// hold the same antenna and time (an MSv4's datasets each hold their
/// antennas' pointing) write it once; different samples there are refused.
pub(super) fn write(
    ms: &mut MeasurementSet,
    observation: &dyn Observation,
) -> Result<(), ObservationError> {
    let catalogue = observation.catalogue();
    let mut rows = BTreeMap::<(AntennaId, u64), Row>::new();
    let mut frame = None;
    let mut terms = 1;
    for id in catalogue.partition_ids() {
        let entry = catalogue.partition(id);
        let Some(group) = entry.data_groups.first() else {
            continue;
        };
        let grid = observation.grid(id, &group.name)?;
        let (Some(first), Some(last)) = (grid.times_s.first(), grid.times_s.last()) else {
            continue;
        };
        let antennas: Vec<AntennaId> = grid
            .baselines
            .iter()
            .flat_map(|baseline| [baseline.antenna1, baseline.antenna2])
            .collect::<std::collections::BTreeSet<_>>()
            .into_iter()
            .collect();
        let series = observation.pointing(&PointingRequest {
            partition: id,
            direction: PointingDirection::Beam,
            antennas,
            times: *first..=*last,
            budget_bytes: usize::MAX,
        })?;
        if series.antennas.is_empty() {
            continue;
        }
        if frame.is_some_and(|known| known != series.frame) {
            return Err(ObservationError::Unsupported {
                what: "pointing in different frames in different partitions",
            });
        }
        frame = Some(series.frame);
        terms = terms.max(series.terms);
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
            let width = 2 * series.terms;
            for (sample, time) in times.iter().enumerate() {
                let row = Row {
                    interval_s: samples
                        .intervals_s()
                        .map_or(0.0, |intervals| intervals[sample]),
                    origin_s: origins[sample],
                    coefficients: samples.coefficients()[sample * width..(sample + 1) * width]
                        .to_vec(),
                };
                match rows.get(&(*antenna, time.to_bits())) {
                    Some(known) if *known != row => {
                        return Err(ObservationError::Unsupported {
                            what: "partitions with different pointing at one antenna and time",
                        });
                    }
                    Some(_) => {}
                    None => {
                        rows.insert((*antenna, time.to_bits()), row);
                    }
                }
            }
        }
    }
    let Some(frame) = frame else {
        return Ok(());
    };
    let table = subtable_mut(ms, SubtableId::Pointing).map_err(storage)?;
    if frame != DirectionRef::J2000 {
        for column in ["DIRECTION", "TARGET"] {
            TableMeasDesc::new_fixed(column, MeasureType::Direction, frame.as_str())
                .write(table)
                .map_err(storage)?;
        }
    }
    for ((antenna, time), row) in rows {
        // `[2, terms]`, each axis's terms padded to the most any partition has.
        let own = row.coefficients.len() / 2;
        let mut direction = Vec::with_capacity(2 * terms);
        for axis in 0..2 {
            direction.extend_from_slice(&row.coefficients[axis * own..(axis + 1) * own]);
            direction.extend(std::iter::repeat_n(0.0, terms - own));
        }
        let direction = f64_array(&direction, vec![2, terms]);
        let values = row_from_defs(
            schema::pointing::REQUIRED_COLUMNS,
            &[
                ("ANTENNA_ID", i(antenna.0 as i32)),
                ("TIME", f(f64::from_bits(time))),
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

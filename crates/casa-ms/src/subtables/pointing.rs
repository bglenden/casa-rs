// SPDX-License-Identifier: LGPL-3.0-or-later
//! Typed wrappers for the POINTING subtable.
//!
//! Cf. C++ `MSPointingColumns`.

use std::cmp::Reverse;
use std::collections::{BTreeMap, BinaryHeap};

use casa_types::{ArrayValue, measures::direction::MDirection};

use crate::derived::engine::resolve_direction_reference;
use crate::schema::{self, SubtableId};
use crate::{MsError, MsResult};

crate::subtables::define_generic_subtable!(
    read = MsPointing,
    write = MsPointingMut,
    id = SubtableId::Pointing,
    required = schema::pointing::REQUIRED_COLUMNS,
    optional = schema::pointing::OPTIONAL_COLUMNS,
    table_name = "POINTING",
    read_doc = "Read-only typed wrapper for the POINTING subtable.",
    write_doc = "Mutable typed wrapper for the POINTING subtable."
);

impl MsPointing<'_> {
    /// Read the pointing used by CASA's legacy visibility buffer for an
    /// antenna and visibility time. The returned direction is the recorded
    /// direction at the pointing row's epoch.
    ///
    /// # Errors
    /// The POINTING metadata or selected direction cell is invalid.
    pub fn legacy_direction(&self, antenna: i32, time: f64) -> MsResult<Option<MDirection>> {
        self.legacy_directions(&[(antenna, time)])
            .map(|mut directions| directions.pop().flatten())
    }

    /// Resolve an antenna and epoch for each legacy visibility buffer.
    /// The POINTING metadata is traversed once and a direction cell is
    /// opened once for all queries that select that row.
    /// # Errors
    /// The POINTING metadata or a selected direction cell is invalid.
    pub fn legacy_directions(&self, buffers: &[(i32, f64)]) -> MsResult<Vec<Option<MDirection>>> {
        if buffers.is_empty() {
            return Ok(Vec::new());
        }
        let mut by_antenna = BTreeMap::<_, (Vec<_>, Vec<_>)>::new();
        for (index, &(antenna, time)) in buffers.iter().enumerate() {
            by_antenna.entry(antenna).or_default().0.push((index, time));
        }
        for row in 0..self.row_count() {
            if let Some((_, samples)) = by_antenna.get_mut(&self.i32(row, "ANTENNA_ID")?) {
                samples.push((row, self.f64(row, "TIME")?, self.f64(row, "INTERVAL")?));
            }
        }
        let mut selected = vec![None; buffers.len()];
        for (queries, samples) in by_antenna.into_values() {
            select_legacy_rows(queries, &samples, &mut selected);
        }
        let mut directions = BTreeMap::new();
        selected
            .into_iter()
            .map(|row| {
                let Some(row) = row else {
                    return Ok(None);
                };
                if let Some(direction) = directions.get(&row) {
                    return Ok(Some(MDirection::clone(direction)));
                }
                let direction = self.read_legacy_direction(row)?;
                directions.insert(row, direction.clone());
                Ok(Some(direction))
            })
            .collect()
    }

    fn read_legacy_direction(&self, row: usize) -> MsResult<MDirection> {
        let ArrayValue::Float64(values) = self.array(row, "DIRECTION")? else {
            return Err(MsError::InvalidInput(
                "POINTING DIRECTION must be double".into(),
            ));
        };
        let (Some(&lon), Some(&lat)) = (
            values.get(ndarray::IxDyn(&[0, 0])),
            values.get(ndarray::IxDyn(&[1, 0])),
        ) else {
            return Err(MsError::InvalidInput(
                "invalid POINTING DIRECTION shape".into(),
            ));
        };
        let reference = resolve_direction_reference(self.table(), "POINTING", "DIRECTION", row)?;
        Ok(MDirection::from_angles(lon, lat, reference))
    }
}

fn select_legacy_rows(
    mut queries: Vec<(usize, f64)>,
    samples: &[(usize, f64, f64)],
    selected: &mut [Option<usize>],
) {
    let mut spans = Vec::with_capacity(samples.len());
    let mut point_samples = Vec::new();
    // Equal adjacent epochs share the next distinct sample even when the
    // original POINTING rows are unsorted or include duplicate epochs.
    let groups = samples.chunk_by(|left, right| left.1 == right.1);
    let mut groups = groups.peekable();
    let mut previous = None;
    while let Some(group) = groups.next() {
        let epoch = group[0].1;
        let neighbor = groups.peek().map(|group| group[0].1).or(previous);
        for &(row, _, interval) in group {
            let radius = if interval > 0. {
                interval / 2.
            } else {
                if interval <= 0. {
                    point_samples.push((row, epoch));
                }
                neighbor.map_or(f64::INFINITY, |time| (epoch - time).abs() / 2.)
            };
            spans.push((row, epoch, epoch - radius, epoch + radius));
        }
        previous = Some(epoch);
    }
    let mut by_start: Vec<_> = (0..spans.len())
        .filter(|&index| !spans[index].2.is_nan())
        .collect();
    by_start.sort_unstable_by(|&a, &b| spans[a].2.total_cmp(&spans[b].2));
    point_samples.sort_unstable_by(|a, b| a.1.total_cmp(&b.1).then(a.0.cmp(&b.0)));
    point_samples.dedup_by(|a, b| a.1 == b.1);
    queries.sort_unstable_by(|a, b| a.1.total_cmp(&b.1));
    let mut active = BinaryHeap::new();
    let mut next = 0;
    for (output, time) in queries {
        while next < by_start.len() && spans[by_start[next]].2 <= time {
            active.push(Reverse(by_start[next]));
            next += 1;
        }
        while active
            .peek()
            .is_some_and(|&Reverse(index)| spans[index].3 < time)
        {
            active.pop();
        }
        let covering = active
            .peek()
            .map(|&Reverse(index)| (spans[index].0, spans[index].1));
        let split = point_samples.partition_point(|sample| sample.1 < time);
        let nearest = point_samples
            .get(split)
            .into_iter()
            .chain(
                split
                    .checked_sub(1)
                    .and_then(|index| point_samples.get(index)),
            )
            .min_by(|a, b| {
                (a.1 - time)
                    .abs()
                    .total_cmp(&(b.1 - time).abs())
                    .then(a.0.cmp(&b.0))
            })
            .copied();
        selected[output] = match (covering, nearest) {
            (Some(covering), Some(nearest))
                if (nearest.1 - time).abs() <= (covering.1 - time).abs() =>
            {
                Some(nearest.0)
            }
            (covering, nearest) => covering.or(nearest).map(|sample| sample.0),
        };
    }
}

#[cfg(test)]
mod tests {
    use super::select_legacy_rows;

    #[test]
    fn legacy_queries_keep_row_order_for_overlaps_and_duplicate_epochs() {
        let samples = [
            (10, 10., 6.),
            (11, 14., 10.),
            (12, 25., 0.),
            (13, 25., 0.),
            (14, 33., -1.),
            (15, 30., 6.),
        ];
        let queries = [30., 10., 13., 14., 0., 20., 25., 29., 33.];
        let mut selected = [None; 9];
        select_legacy_rows(
            queries.into_iter().enumerate().collect(),
            &samples,
            &mut selected,
        );
        assert_eq!(selected, [15, 10, 10, 11, 12, 12, 12, 12, 14].map(Some));
    }
}

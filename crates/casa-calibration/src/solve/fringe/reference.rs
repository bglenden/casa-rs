// SPDX-License-Identifier: LGPL-3.0-or-later
//! Keep fringe parameters continuous when the preferred reference drops out.

use std::collections::BTreeSet;

use casa_core::Complex32;
use casa_ms::MeasurementSet;

use super::{SolutionRow, error};
use crate::GainSolveError;

pub(super) fn connect_intervals(
    ms: &MeasurementSet,
    rows: &mut [SolutionRow],
    antenna_count: usize,
    preferences: impl Iterator<Item = i32>,
    strict: bool,
) -> Result<(), GainSolveError> {
    if rows.is_empty() {
        return Ok(());
    }
    let mut candidates = preferences.collect::<Vec<_>>();
    let primary = candidates[0];
    let mut previous_reference = primary;
    if strict {
        candidates.truncate(1);
    } else {
        let antenna = ms.antenna().map_err(error)?;
        let anchor = antenna
            .position(candidates[candidates.len() - 1] as usize)
            .map_err(error)?;
        let mut nearby = (0..antenna_count as i32)
            .filter(|id| !candidates.contains(id))
            .map(|id| {
                let position = antenna.position(id as usize).map_err(error)?;
                let distance = position
                    .into_iter()
                    .zip(anchor)
                    .map(|(a, b)| (a - b).powi(2))
                    .sum::<f64>();
                Ok((id, distance))
            })
            .collect::<Result<Vec<_>, GainSolveError>>()?;
        nearby.sort_by(|a, b| a.1.total_cmp(&b.1));
        candidates.extend(nearby.into_iter().map(|(id, _)| id));
    }

    let mut previous = None::<(i32, Vec<Reference>)>;
    let mut intervals = rows.chunks_mut(antenna_count).collect::<Vec<_>>();
    intervals.sort_by(|left, right| {
        left[0]
            .spw_id
            .cmp(&right[0].spw_id)
            .then_with(|| left[0].time_seconds.total_cmp(&right[0].time_seconds))
    });
    let second_hand_windows = intervals
        .iter()
        .flat_map(|rows| rows.iter())
        .filter(|row| has_hand(row, 1))
        .map(|row| row.spw_id)
        .collect::<BTreeSet<_>>();
    for rows in intervals {
        let spw = rows[0].spw_id;
        let hands = usize::from(second_hand_windows.contains(&spw)) + 1;
        let prior = previous
            .as_ref()
            .filter(|(window, _)| *window == spw)
            .map(|(_, previous)| previous.as_slice());
        let chosen = std::iter::once(primary)
            .chain(std::iter::once(previous_reference))
            .chain(candidates.iter().copied())
            .find_map(|antenna| {
                // The fringe solver writes every ANTENNA row in ID order.
                let index = usize::try_from(antenna).ok()?;
                let current = rows.get(index)?;
                let valid = (0..hands).all(|hand| {
                    has_hand(current, hand)
                        && prior.is_none_or(|previous| previous[index].hands[hand])
                });
                valid.then_some((antenna, index))
            });
        if let Some((antenna, index)) = chosen {
            let offset = std::array::from_fn::<_, 8, _>(|parameter| {
                rows[index].gains[parameter]
                    - prior.map_or(Complex32::default(), |prior| prior[index].values[parameter])
            });
            for row in rows.iter_mut() {
                row.refant_id = antenna;
                for (value, delta) in row.gains.iter_mut().zip(offset) {
                    *value -= delta;
                }
            }
            previous_reference = antenna;
            previous = Some((
                spw,
                rows.iter()
                    .map(|row| Reference {
                        hands: [has_hand(row, 0), has_hand(row, 1)],
                        values: std::array::from_fn(|i| row.gains[i]),
                    })
                    .collect(),
            ));
        } else {
            for row in rows {
                row.flags.fill(true);
            }
        }
    }
    Ok(())
}

fn has_hand(row: &SolutionRow, hand: usize) -> bool {
    row.flags[hand * 4..hand * 4 + 4].iter().any(|&flag| !flag)
}

struct Reference {
    hands: [bool; 2],
    values: [Complex32; 8],
}

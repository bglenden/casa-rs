// SPDX-License-Identifier: LGPL-3.0-or-later
//! Observation scan ranges shared by physical and virtual concatenation.

use std::{
    collections::{BTreeMap, BTreeSet},
    fmt::Write as _,
};

use super::main_rows::{ScalarColumns, integer, sparse_id};
use casa_ms::{MsError, MsResult};

#[derive(Default)]
pub(super) struct ScanBounds {
    order: Vec<i32>,
    ranges: BTreeMap<i32, (i32, i32)>,
    max_scan: i32,
}

impl ScanBounds {
    pub(super) fn from_main(columns: &ScalarColumns) -> MsResult<Self> {
        let mut bounds = Self::default();
        for row in 0..columns["SCAN_NUMBER"].len() {
            bounds.observe(
                integer(columns, "OBSERVATION_ID", row)?,
                integer(columns, "SCAN_NUMBER", row)?,
            );
        }
        Ok(bounds)
    }

    pub(super) fn observe(&mut self, id: i32, scan: i32) {
        self.max_scan = self.max_scan.max(scan);
        use std::collections::btree_map::Entry;
        match self.ranges.entry(id) {
            Entry::Vacant(entry) => {
                self.order.push(id);
                entry.insert((scan, scan));
            }
            Entry::Occupied(mut entry) => {
                let (min, max) = entry.get_mut();
                *min = (*min).min(scan);
                *max = (*max).max(scan);
            }
        }
    }

    pub(super) fn remap_observations(self, ids: &[usize]) -> MsResult<Self> {
        if ids.is_empty() {
            return Ok(self);
        }
        let mut remapped = Self::default();
        for id in self.order {
            let (first, last) = self.ranges[&id];
            let target = sparse_id(ids, id)?;
            remapped.observe(target, first);
            remapped.observe(target, last);
        }
        // The stored final line contains the last donor's maximum scan.
        // It can differ from the largest scan in the accumulated ranges.
        remapped.max_scan = self.max_scan;
        Ok(remapped)
    }

    pub(super) fn offsets(
        &self,
        donor: &ScalarColumns,
        observation_map: &[usize],
    ) -> MsResult<ScanOffsets> {
        let min_donor = (0..donor["SCAN_NUMBER"].len())
            .map(|row| integer(donor, "SCAN_NUMBER", row))
            .try_fold(i32::MAX, |minimum, scan| {
                Ok::<_, MsError>(minimum.min(scan?))
            })?;
        Ok(self.offsets_from_minimum(min_donor, observation_map))
    }

    pub(super) fn offsets_from_minimum(
        &self,
        min_donor: i32,
        observation_map: &[usize],
    ) -> ScanOffsets {
        let counterpart: BTreeSet<_> = observation_map.iter().copied().collect();
        let mut offsets = BTreeMap::new();
        let mut previous_max = None;
        for &id in &self.order {
            let (min, max) = self.ranges[&id];
            let shared = usize::try_from(id).is_ok_and(|id| counterpart.contains(&id));
            let shift = match previous_max.filter(|_| shared) {
                Some(previous) => i64::from(previous) + 1 - i64::from(min_donor),
                None => i64::from(min) - 1,
            };
            offsets.insert(id, shift.max(0));
            previous_max = Some(max);
        }
        ScanOffsets {
            offsets,
            default: (i64::from(self.max_scan) + 1 - i64::from(min_donor)).max(0),
        }
    }

    pub(super) fn parse(text: &str) -> MsResult<Self> {
        let mut words = text.split_whitespace();
        let count: usize = words
            .next()
            .and_then(|word| word.parse().ok())
            .ok_or_else(invalid_aux)?;
        let mut result = Self::default();
        for _ in 0..count {
            let id = next_i32(&mut words)?;
            let first = next_i32(&mut words)?;
            let last = next_i32(&mut words)?;
            if first > last || result.ranges.insert(id, (first, last)).is_some() {
                return Err(invalid_aux());
            }
            result.order.push(id);
        }
        result.max_scan = next_i32(&mut words)?;
        if words.next().is_some() {
            return Err(invalid_aux());
        }
        Ok(result)
    }

    pub(super) fn render(&self, last_donor_scan: i32) -> String {
        let mut text = String::new();
        writeln!(text, "{}", self.order.len()).expect("write scan count");
        for id in &self.order {
            let (first, last) = self.ranges[id];
            writeln!(text, "{id}\n{first}\n{last}").expect("write scan bounds");
        }
        writeln!(text, "{last_donor_scan}").expect("write last scan");
        text
    }
}

pub(super) struct ScanOffsets {
    offsets: BTreeMap<i32, i64>,
    default: i64,
}

impl ScanOffsets {
    pub(super) fn remap(&self, old_id: i32, new_id: i32, scan: i32) -> MsResult<i32> {
        if old_id == new_id {
            return Ok(scan);
        }
        let offset = self.offsets.get(&new_id).unwrap_or(&self.default);
        i32::try_from(i64::from(scan) + offset)
            .map_err(|_| MsError::InvalidInput("concatenated SCAN_NUMBER exceeds int32".into()))
    }
}

fn next_i32(words: &mut std::str::SplitWhitespace<'_>) -> MsResult<i32> {
    words
        .next()
        .and_then(|word| word.parse().ok())
        .ok_or_else(invalid_aux)
}

fn invalid_aux() -> MsError {
    MsError::InvalidInput("invalid virtual concatenation scan file".into())
}

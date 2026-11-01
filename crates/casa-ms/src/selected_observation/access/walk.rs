// SPDX-License-Identifier: LGPL-3.0-or-later
//! The walk a source with several selected DATA_DESCRIPTIONs takes: its
//! selected rows one DATA_DESCRIPTION after another (#659).

use super::{
    BoundObservationSource, BoundObservationSourceError, MainRowWalk, MsReadPlan,
    SelectedObservationRow,
};

impl BoundObservationSource {
    /// Bytes the walk of `selected_rows` rows over `descriptions` selected
    /// DATA_DESCRIPTIONs holds: none with one, whose traversals walk MAIN in
    /// storage order.
    pub(in crate::selected_observation) const fn walk_bytes(
        descriptions: usize,
        selected_rows: usize,
    ) -> Option<usize> {
        if descriptions > 1 {
            MainRowWalk::bytes(selected_rows, descriptions)
        } else {
            Some(0)
        }
    }

    /// The selected rows grouped by DATA_DESCRIPTION in the coordinates'
    /// order, ascending within each: one scan of MAIN counts each group's
    /// rows and a second places them, so the walk is allocated once at its
    /// size.
    pub(super) fn row_walk(
        &self,
        plan: MsReadPlan,
    ) -> Result<MainRowWalk, BoundObservationSourceError> {
        let group_of = |row: SelectedObservationRow| {
            let id = u32::try_from(row.data_description_id()).ok();
            self.coordinates
                .iter()
                .position(|coordinates| {
                    Some(coordinates.data_description.data_description_id()) == id
                })
                .ok_or(
                    BoundObservationSourceError::DataDescriptionCoordinateMismatch {
                        data_description_id: id.unwrap_or(u32::MAX),
                    },
                )
        };
        let mut counts = vec![0_usize; self.coordinates.len()];
        let mut cursor = self
            .measurement_set
            .main_row_selection_cursor(plan)?
            .with_scan_numbers(self.row_predicate.needs_scan_numbers());
        while let Some(row) = cursor.next(&self.measurement_set)? {
            if self.row_predicate.matches(row) {
                counts[group_of(row)?] += 1;
            }
        }
        let mut ends = Vec::with_capacity(counts.len());
        let mut next = Vec::with_capacity(counts.len());
        let mut total = 0_usize;
        for count in counts {
            next.push(total);
            total += count;
            ends.push(total);
        }
        let mut rows = vec![0_u32; total];
        let mut cursor = self
            .measurement_set
            .main_row_selection_cursor(plan)?
            .with_scan_numbers(self.row_predicate.needs_scan_numbers());
        while let Some(row) = cursor.next(&self.measurement_set)? {
            if self.row_predicate.matches(row) {
                let group = group_of(row)?;
                rows[next[group]] = u32::try_from(row.physical_row())
                    .map_err(|_| BoundObservationSourceError::MeasurementOverflow)?;
                next[group] += 1;
            }
        }
        Ok(MainRowWalk::new(rows, ends))
    }
}

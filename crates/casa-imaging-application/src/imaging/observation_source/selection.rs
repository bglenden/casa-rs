// SPDX-License-Identifier: LGPL-3.0-or-later
//! What the batch route reads: the compiled selection resolved to the
//! observation's partitions, and the per-sample predicate the request
//! cannot state.

use casa_imaging_model::{IdSelection, UvSelection};
use casa_imaging_runtime::pass::SourceError;
use casa_observation::{Correlation, FieldSelection, PartitionId, VisibilityBatch};

/// One partition the selection reads.
#[derive(Clone, Debug, PartialEq)]
pub(crate) struct SelectedPartition {
    /// The partition.
    pub(crate) id: PartitionId,
    /// The spectral window number tasks select by (MSv2 `SPECTRAL_WINDOW`
    /// row): what kernel sets and continuum rules are keyed on.
    pub(crate) spectral_window: u32,
    /// The selected native channels, ascending.
    pub(crate) channels: Vec<u32>,
    /// The correlation products the operator routes, in its order.
    pub(crate) correlations: Vec<Correlation>,
    /// The window's reference wavelength, when a UV-distance interval is
    /// in wavelengths.
    pub(crate) wavelength_m: Option<f64>,
}

/// The compiled selection resolved to the observation's partitions: what
/// [`super::ObservationSource`] binds and tests.
///
/// Windows, channels, correlations, the data group and fields map onto
/// each partition's [`AccessRequest`](casa_observation::AccessRequest);
/// intents onto the partitions chosen, since a partition's samples share
/// theirs; the UV distance is tested per sample as the source packs it.
#[derive(Clone, Debug, PartialEq)]
pub(crate) struct PartitionSelection {
    /// The data group whose visibilities are imaged.
    pub(crate) data_group: String,
    /// The partitions, in traversal order.
    pub(crate) partitions: Vec<SelectedPartition>,
    /// The fields.
    pub(crate) fields: FieldSelection,
    /// The exact scan numbers; the batch catalogue resolves its ScanIds.
    pub(crate) scans: IdSelection,
    /// The UV-distance intervals.
    pub(crate) uv: UvSelection,
}

impl PartitionSelection {
    /// Bytes owned by the scan predicate, including its fixed representation.
    pub(crate) fn scan_selection_bytes(&self) -> usize {
        size_of::<IdSelection>()
            + self
                .scans
                .allocated_bytes()
                .expect("allocated selector fits usize")
    }

    /// Whether the scan at one batch time is selected. A requested role must
    /// be present; catalogue IDs are validated when the batch is built.
    pub(crate) fn scan_holds(
        &self,
        batch: &VisibilityBatch<'_>,
        time: usize,
    ) -> Result<bool, SourceError> {
        if matches!(self.scans, IdSelection::All) {
            return Ok(true);
        }
        let scans = batch
            .scan
            .as_ref()
            .ok_or("the batch carries no requested scans")?;
        Ok(batch.catalogue.scans[scans[time].0 as usize]
            .number
            .is_some_and(|number| self.scans.holds(i64::from(number))))
    }

    /// Whether a sample of partition `partition` with stored UVW `uvw_m` is
    /// selected by UV distance.
    pub(crate) fn uv_holds(&self, partition: &SelectedPartition, uvw_m: [f64; 3]) -> bool {
        self.uv.holds(uvw_m, partition.wavelength_m)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn scan_selection_charge_tracks_owned_capacity() {
        let mut selection = PartitionSelection {
            data_group: String::new(),
            partitions: vec![],
            fields: FieldSelection::All,
            scans: IdSelection::All,
            uv: UvSelection::All,
        };
        let baseline = selection.scan_selection_bytes();
        assert_eq!(baseline, size_of::<IdSelection>());
        let mut ids = Vec::with_capacity(19);
        ids.extend([1, 3]);
        let bytes = ids.capacity() * size_of::<u32>();
        selection.scans = IdSelection::Only(ids);
        assert_eq!(selection.scan_selection_bytes(), baseline + bytes);
        let mut ranges = Vec::with_capacity(17);
        ranges.push((2, i32::MAX as u32));
        let bytes = ranges.capacity() * size_of::<(u32, u32)>();
        selection.scans = IdSelection::Ranges(ranges);
        assert_eq!(selection.scan_selection_bytes(), baseline + bytes);
    }
}

// SPDX-License-Identifier: LGPL-3.0-or-later
//! What the batch route reads: the compiled selection resolved to the
//! observation's partitions, and the per-sample predicate the request
//! cannot state.

use casa_imaging_model::{BaselineSelection, IdSelection, TimeSelection, UvSelection};
use casa_imaging_runtime::pass::SourceError;
use casa_observation::{
    Correlation, FieldSelection, OrderedBaseline, PartitionId, VisibilityBatch,
};
use casa_types::measures::MjdHighPrec;

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
    /// The window's per-time frequency selection, when a time reads only
    /// some of `channels` ([`casa_imaging_model::FrequencyBands`]).
    pub(crate) frequency_bands: Option<casa_imaging_model::FrequencyBands>,
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
/// theirs; scan per time, and baseline and UV distance per sample, as the
/// source packs it.
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
    /// The baselines the antenna selection holds.
    pub(crate) baselines: BaselineSelection,
    /// The times the `timerange` selection holds.
    pub(crate) times: TimeSelection,
    /// The UV-distance intervals.
    pub(crate) uv: UvSelection,
}

impl PartitionSelection {
    /// Bytes owned by the run's per-time and per-sample predicates (scan
    /// and baseline), including their fixed representations.
    pub(crate) fn predicate_bytes(&self) -> usize {
        size_of::<IdSelection>()
            + self
                .scans
                .allocated_bytes()
                .expect("allocated selector fits usize")
            + size_of::<BaselineSelection>()
            + self.baselines.allocated_bytes()
            + size_of::<TimeSelection>()
            + self.times.allocated_bytes()
    }

    /// Whether the antenna selection holds `baseline`.
    pub(crate) fn baseline_holds(&self, baseline: OrderedBaseline) -> bool {
        self.baselines
            .holds(baseline.antenna1.0, baseline.antenna2.0)
    }

    /// Whether the scan and the time of one batch time are selected; a time
    /// is tested as CASA's TaQL tests `TIME`, in seconds since MJD 0.
    pub(crate) fn time_holds(
        &self,
        batch: &VisibilityBatch<'_>,
        time: usize,
    ) -> Result<bool, SourceError> {
        if !self
            .times
            .holds(batch.time.seconds_since(time, MjdHighPrec::new(0.0, 0.0)))
        {
            return Ok(false);
        }
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
    fn predicate_charge_tracks_owned_capacity() {
        let mut selection = PartitionSelection {
            data_group: String::new(),
            partitions: vec![],
            fields: FieldSelection::All,
            scans: IdSelection::All,
            baselines: BaselineSelection::All,
            times: TimeSelection::All,
            uv: UvSelection::All,
        };
        let baseline = selection.predicate_bytes();
        assert_eq!(
            baseline,
            size_of::<IdSelection>() + size_of::<BaselineSelection>() + size_of::<TimeSelection>()
        );
        let mut ids = Vec::with_capacity(19);
        ids.extend([1, 3]);
        let bytes = ids.capacity() * size_of::<u32>();
        selection.scans = IdSelection::Only(ids);
        assert_eq!(selection.predicate_bytes(), baseline + bytes);
        let mut ranges = Vec::with_capacity(17);
        ranges.push((2, i32::MAX as u32));
        let bytes = ranges.capacity() * size_of::<(u32, u32)>();
        selection.scans = IdSelection::Ranges(ranges);
        assert_eq!(selection.predicate_bytes(), baseline + bytes);
        selection.scans = IdSelection::All;
        let pairs = BaselineSelection::from_pairs(5, |antenna1, antenna2| antenna1 != antenna2);
        let bytes = pairs.allocated_bytes();
        assert!(bytes >= 25);
        selection.baselines = pairs;
        assert_eq!(selection.predicate_bytes(), baseline + bytes);
        let conditions = Vec::with_capacity(3);
        let bytes = bytes + 3 * size_of::<casa_imaging_model::TimeCondition>();
        selection.times = TimeSelection::AnyOf(conditions);
        assert_eq!(selection.predicate_bytes(), baseline + bytes);
    }
}

// SPDX-License-Identifier: LGPL-3.0-or-later
//! What the batch route reads: the compiled selection resolved to the
//! observation's partitions, and the per-sample predicate the request
//! cannot state.

use casa_imaging_model::UvSelection;
use casa_observation::{Correlation, FieldSelection, PartitionId};

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
    /// The UV-distance intervals.
    pub(crate) uv: UvSelection,
}

impl PartitionSelection {
    /// Whether a sample of partition `partition` with stored UVW `uvw_m` is
    /// selected by UV distance.
    pub(crate) fn uv_holds(&self, partition: &SelectedPartition, uvw_m: [f64; 3]) -> bool {
        self.uv.holds(uvw_m, partition.wavelength_m)
    }
}

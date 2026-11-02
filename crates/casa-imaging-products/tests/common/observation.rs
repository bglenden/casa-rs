// SPDX-License-Identifier: LGPL-3.0-or-later

//! The selected observation and product policies every product fixture
//! problem compiles against: one measurement set with two selected
//! partitions in two spectral windows, Stokes I only.

use casa_imaging_model::{
    CorrelationProduct, FlagPolicy, IdSelection, IntentSelection, ObservationSelection,
    ObservationSourceInput, ObservationSourceProvenance, PartitionId, PartitionSelection,
    PrimaryBeamValidityPolicy, ProductBlankingPolicy, ProductSupportComparison,
    ProductValidityPolicies, RowSelection, SelectedColumns, SpectralWindowSelection,
    TaylorSupportReference, TaylorValidityPolicy, UvSelection, VisibilityColumn, WeightColumn,
};
use casa_types::Correlation;

/// One measurement set selecting `2 + seed` samples of two partitions, each
/// in its own spectral window, with provenance `fixture://<label>/<seed>`.
pub fn source(seed: u8, label: &str) -> ObservationSourceInput {
    let partition = |id, window| {
        PartitionSelection::new(
            PartitionId(id),
            window,
            vec![CorrelationProduct::new(0, Correlation::I)],
        )
    };
    ObservationSourceInput::new(
        ObservationSourceProvenance::new(format!("fixture://{label}/{seed}")),
        ObservationSelection::new(
            // The seed sets the selected samples, so sources from different
            // seeds are different observations.
            2 + u64::from(seed),
            RowSelection::new(IdSelection::All, UvSelection::All, IntentSelection::All),
            vec![partition(0, 0), partition(1, 1)],
            vec![
                SpectralWindowSelection::new(0, vec![0]),
                SpectralWindowSelection::new(1, vec![1]),
            ],
        ),
        SelectedColumns::new(
            VisibilityColumn::Data,
            FlagPolicy::FlagOrFlagRow,
            WeightColumn::Weight,
        ),
        false,
    )
}

/// Primary-beam support strictly above 0.2 and Taylor support strictly above
/// `taylor_fraction` of the principal residual's positive maximum, both
/// blanked to zero.
pub fn validity(taylor_fraction: f32) -> ProductValidityPolicies {
    ProductValidityPolicies::new(
        PrimaryBeamValidityPolicy::new(
            0.2,
            ProductSupportComparison::StrictlyGreater,
            ProductBlankingPolicy::Zero,
        )
        .expect("valid primary-beam policy"),
        TaylorValidityPolicy::new(
            TaylorSupportReference::PrincipalResidualTaylor0PositiveMaximum,
            taylor_fraction,
            ProductSupportComparison::StrictlyGreater,
            ProductBlankingPolicy::Zero,
        )
        .expect("valid Taylor policy"),
    )
}

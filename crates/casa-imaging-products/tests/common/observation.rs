// SPDX-License-Identifier: LGPL-3.0-or-later

//! The selected observation and product policies every product fixture
//! problem compiles against: one measurement set with two selected rows in
//! two spectral windows, Stokes I only.

use casa_imaging_model::{
    CorrelationProduct, CorrelationSelection, CorrelationType, DataDescriptionSelection,
    FlagPolicy, IdSelection, IntentSelection, ObservationSelection, ObservationSourceInput,
    ObservationSourceProvenance, PrimaryBeamValidityPolicy, ProductBlankingPolicy,
    ProductSupportComparison, ProductValidityPolicies, RowSelection, SelectedColumns,
    SelectedMainRow, SelectedRows, SpectralWindowSelection, TaylorSupportReference,
    TaylorValidityPolicy, UvSelection, VisibilityColumn, WeightColumn,
};

/// One measurement set selecting rows 0 and 2 of `3 + seed`, each in its own
/// spectral window, with provenance `fixture://<label>/<seed>`.
pub fn source(seed: u8, label: &str) -> ObservationSourceInput {
    ObservationSourceInput::new(
        ObservationSourceProvenance::new(format!("fixture://{label}/{seed}")),
        ObservationSelection::new(
            // The seed sets the MeasurementSet's row count, so sources from
            // different seeds are different observations.
            SelectedRows::from_ordered_main_rows(
                3 + u64::from(seed),
                [SelectedMainRow::new(0, 0), SelectedMainRow::new(2, 1)],
            )
            .expect("two selected rows"),
            RowSelection::new(IdSelection::All, UvSelection::All, IntentSelection::All),
            vec![
                DataDescriptionSelection::new(0, 0, 0),
                DataDescriptionSelection::new(1, 1, 0),
            ],
            vec![
                SpectralWindowSelection::new(0, vec![0]),
                SpectralWindowSelection::new(1, vec![1]),
            ],
            vec![CorrelationSelection::new(
                0,
                vec![CorrelationProduct::new(0, CorrelationType::StokesI)],
            )],
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

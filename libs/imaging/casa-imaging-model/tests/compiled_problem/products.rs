// SPDX-License-Identifier: LGPL-3.0-or-later

use super::*;

pub(super) fn compile_product_set(
    requested: Vec<ProductKind>,
    instrument_response: InstrumentResponse,
) -> Result<casa_imaging_model::CompiledProblem, CompileProblemError> {
    let restoring_beam = if requested.contains(&ProductKind::RestoredImage) {
        RestoringBeamPolicy::PerPlane
    } else {
        RestoringBeamPolicy::None
    };
    let science = ScientificContract::new(
        SpectralContract::new(SpectralSamplingLaw::IDENTITY, SpectralCoupling::Independent),
        MeasurementEquationContract::new(instrument_response, inner_products()),
    );
    let science = if instrument_response == InstrumentResponse::PrimaryBeam {
        science.with_instrument_model(
            InstrumentModel::CasaAlmaAcaHeterogeneousInterferometricResponseV1,
        )
    } else {
        science
    };
    compile_request(
        ProblemSpecification::new(
            science,
            reconstruction(),
            weighting(),
            ProductRequirements::new(
                requested,
                ProductNormalization::UnitResponse,
                restoring_beam,
                product_validity(),
            ),
            read_only_transaction(),
            numerics(false),
        ),
        observation(),
    )
}

#[test]
pub(super) fn equivalent_science_has_one_canonical_compiled_problem() {
    let first = compile_request(specification(false), observation()).expect("compile first");
    let reordered = compile_request(specification(true), observation()).expect("compile reordered");

    assert_eq!(first, reordered);
    assert_eq!(
        first.required_capabilities(),
        reordered.required_capabilities()
    );
    assert_eq!(first.observation(), reordered.observation());
    assert_eq!(first.reconstruction(), reordered.reconstruction());
    assert_eq!(first.weighting(), reordered.weighting());
    assert_eq!(first.products(), reordered.products());
    assert_eq!(first.numerics(), reordered.numerics());
}

#[test]
pub(super) fn compiler_owns_the_exact_product_list_and_atomic_publication_contract() {
    let compiled = compile_request(specification(false), observation()).expect("compile problem");
    let list = compiled.product_list();
    let reordered = compile_request(specification(true), observation()).expect("compile reordered");

    assert_eq!(list, reordered.product_list());
    assert_eq!(
        list.entries()
            .iter()
            .filter_map(|entry| entry.name())
            .collect::<Vec<_>>(),
        vec![
            ".psf.tt0",
            ".psf.tt1",
            ".psf.tt2",
            ".residual.tt0",
            ".residual.tt1",
            ".model.tt0",
            ".model.tt1",
            ".image.tt0",
            ".image.tt1",
            ".sumwt.tt0",
            ".sumwt.tt1",
            ".sumwt.tt2",
            ".sensitivity",
            ".alpha",
        ]
    );

    let restored = list
        .entries()
        .iter()
        .find(|entry| entry.role() == ProductRole::RestoredImage(ProductTerm::Taylor(0)))
        .expect("restored Taylor-zero product");
    assert_eq!(restored.axes().kind(), ProductAxisKind::SkyImage);
    assert_eq!(restored.axes().shape(), [512, 512, 1, 1]);
    assert_eq!(restored.unit(), ProductUnit::JyPerBeam);
    assert_eq!(
        restored.beam(),
        ProductBeamRule::Restoring(RestoringBeamPolicy::PerPlane)
    );
    // CASA restores every plane, a blank one's model included.
    assert_eq!(restored.validity(), ProductValidityRule::All);
    assert!(list.publication().members().contains(&restored.id()));

    let spectral_index = list
        .entries()
        .iter()
        .find(|entry| entry.role() == ProductRole::SpectralIndex)
        .expect("spectral-index product");
    assert_eq!(spectral_index.unit(), ProductUnit::Dimensionless);
    assert_eq!(
        spectral_index.validity(),
        ProductValidityRule::Taylor(product_validity().taylor())
    );

    assert_eq!(
        list.publication().members(),
        list.entries()
            .iter()
            .filter(|entry| entry.name().is_some())
            .map(|entry| entry.id())
            .collect::<Vec<_>>()
    );
}

#[test]
pub(super) fn uncorrected_mask_is_separate_from_numeric_support_and_binds_publication() {
    let validity = product_validity()
        .with_uncorrected_mask(casa_imaging_model::UncorrectedImageMaskPolicy::PrimaryBeam);
    let compile_with_policy = |validity| {
        compile_request(
            ProblemSpecification::new(
                science(),
                reconstruction(),
                weighting(),
                ProductRequirements::new(
                    vec![
                        ProductKind::Residual,
                        ProductKind::Model,
                        ProductKind::RestoredImage,
                        ProductKind::PrimaryBeam,
                    ],
                    ProductNormalization::UnitResponse,
                    RestoringBeamPolicy::PerPlane,
                    validity,
                ),
                read_only_transaction(),
                numerics(false),
            ),
            observation(),
        )
        .expect("compile explicit primary-beam mask policy")
    };
    let compiled = compile_with_policy(validity);
    let absent = compile_with_policy(product_validity());

    for (role, rule) in [
        (
            ProductRole::Residual(ProductTerm::Taylor(0)),
            ProductValidityRule::FinalNormalState,
        ),
        (
            ProductRole::RestoredImage(ProductTerm::Taylor(0)),
            ProductValidityRule::All,
        ),
    ] {
        assert_eq!(
            compiled
                .product_list()
                .entry(role)
                .expect("uncorrected product")
                .validity(),
            rule,
        );
        assert_eq!(
            compiled
                .product_list()
                .entry(role)
                .unwrap()
                .storage()
                .pixel_mask(),
            casa_imaging_model::ProductPixelMask::Explicit(ProductValidityRule::PrimaryBeam(
                validity.primary_beam()
            )),
        );
        assert_eq!(
            absent
                .product_list()
                .entry(role)
                .unwrap()
                .storage()
                .pixel_mask(),
            casa_imaging_model::ProductPixelMask::Absent
        );
    }
}

#[test]
pub(super) fn single_and_taylor_storage_contracts_preserve_science_and_exact_casa_metadata() {
    use casa_imaging_model::{ProductPixelMask, UncorrectedImageMaskPolicy};
    for taylor in [false, true] {
        for mask in [
            UncorrectedImageMaskPolicy::None,
            UncorrectedImageMaskPolicy::PrimaryBeam,
        ] {
            let validity = product_validity().with_uncorrected_mask(mask);
            let mut requested = vec![
                ProductKind::Psf,
                ProductKind::Residual,
                ProductKind::Model,
                ProductKind::RestoredImage,
                ProductKind::PrimaryBeam,
                ProductKind::PbCorrectedImage,
            ];
            if taylor {
                requested.extend([
                    ProductKind::TaylorTerms,
                    ProductKind::SpectralIndex,
                    ProductKind::SpectralIndexError,
                    ProductKind::PbCorrectedSpectralIndex,
                ]);
            }
            let reconstruction = if taylor {
                reconstruction()
            } else {
                ReconstructionContract::new(
                    ReconstructionBasis::Constant,
                    ReconstructionAlgorithm::Hogbom,
                    ReconstructionControls::new(100, 0.1, 0.0),
                    PolarizationContract::new(vec![PolarizationCoordinate::StokesI]),
                )
            };
            let compiled = compile_request(
                ProblemSpecification::new(
                    science(),
                    reconstruction,
                    weighting(),
                    ProductRequirements::new(
                        requested,
                        ProductNormalization::UnitResponse,
                        RestoringBeamPolicy::PerPlane,
                        validity,
                    ),
                    read_only_transaction(),
                    numerics(false),
                ),
                observation(),
            )
            .expect("storage contract matrix");
            let list = compiled.product_list();
            let terms = if taylor {
                vec![ProductTerm::Taylor(0), ProductTerm::Taylor(1)]
            } else {
                vec![ProductTerm::Single]
            };
            let pb_mask = ProductPixelMask::Explicit(ProductValidityRule::PrimaryBeam(
                validity.primary_beam(),
            ));
            for term in terms {
                let psf = list.entry(ProductRole::Psf(term)).unwrap();
                assert_eq!(psf.unit(), ProductUnit::JyPerBeam);
                assert_eq!(psf.storage().unit(), None);
                assert_eq!(psf.storage().pixel_mask(), ProductPixelMask::Absent);
                let residual = list.entry(ProductRole::Residual(term)).unwrap();
                assert_eq!(residual.unit(), ProductUnit::JyPerBeam);
                assert_eq!(residual.storage().unit(), None);
                assert!(!residual.storage().attach_beam());
                assert_eq!(residual.validity(), ProductValidityRule::FinalNormalState);
                let image = list.entry(ProductRole::RestoredImage(term)).unwrap();
                assert_eq!(image.storage().unit(), Some(ProductUnit::JyPerBeam));
                assert!(image.storage().attach_beam());
                for member in [residual, image] {
                    assert_eq!(
                        member.storage().pixel_mask(),
                        if mask == UncorrectedImageMaskPolicy::PrimaryBeam {
                            pb_mask
                        } else {
                            ProductPixelMask::Absent
                        }
                    );
                }
                assert_eq!(
                    list.entry(ProductRole::PbCorrectedImage(term))
                        .unwrap()
                        .storage()
                        .pixel_mask(),
                    pb_mask
                );
            }
            let response_term = if taylor {
                ProductTerm::Taylor(0)
            } else {
                ProductTerm::Single
            };
            assert_eq!(
                list.entry(ProductRole::PrimaryBeam(response_term))
                    .unwrap()
                    .storage()
                    .pixel_mask(),
                pb_mask
            );
            if taylor {
                assert_eq!(
                    list.entry(ProductRole::PrimaryBeam(ProductTerm::Taylor(1)))
                        .unwrap()
                        .storage()
                        .pixel_mask(),
                    ProductPixelMask::Absent
                );
                for role in [ProductRole::SpectralIndex, ProductRole::SpectralIndexError] {
                    assert_eq!(
                        list.entry(role).unwrap().storage().pixel_mask(),
                        ProductPixelMask::Explicit(ProductValidityRule::Taylor(validity.taylor()))
                    );
                }
                assert_eq!(
                    list.entry(ProductRole::PbCorrectedSpectralIndex)
                        .unwrap()
                        .storage()
                        .pixel_mask(),
                    ProductPixelMask::Explicit(ProductValidityRule::TaylorAndPrimaryBeam {
                        taylor: validity.taylor(),
                        primary_beam: validity.primary_beam(),
                    })
                );
            }
        }
    }
}

#[test]
pub(super) fn publishing_a_primary_beam_does_not_change_uncorrected_product_validity() {
    let compiled = compile_product_set(
        vec![
            ProductKind::Psf,
            ProductKind::Residual,
            ProductKind::Model,
            ProductKind::RestoredImage,
            ProductKind::SumWeights,
            ProductKind::PrimaryBeam,
        ],
        InstrumentResponse::PrimaryBeam,
    )
    .expect("compile unit-response products with a published primary beam");
    let list = compiled.product_list();

    for (role, rule) in [
        (
            ProductRole::Residual(ProductTerm::Taylor(0)),
            ProductValidityRule::FinalNormalState,
        ),
        (
            ProductRole::RestoredImage(ProductTerm::Taylor(0)),
            ProductValidityRule::All,
        ),
    ] {
        assert_eq!(
            list.entry(role).expect("uncorrected product").validity(),
            rule
        );
    }
    assert_eq!(
        list.entry(ProductRole::PrimaryBeam(ProductTerm::Taylor(0)))
            .expect("published primary beam")
            .validity(),
        ProductValidityRule::PrimaryBeam(product_validity().primary_beam()),
    );
}

#[test]
pub(super) fn spectral_index_error_and_pb_correction_name_every_scientific_input() {
    let products = ProductRequirements::new(
        products(false)
            .products()
            .iter()
            .copied()
            .chain([
                ProductKind::PrimaryBeam,
                ProductKind::SpectralIndexError,
                ProductKind::PbCorrectedSpectralIndex,
            ])
            .collect(),
        ProductNormalization::FlatNoise,
        RestoringBeamPolicy::PerPlane,
        product_validity(),
    );
    let compiled = compile_request(
        ProblemSpecification::new(
            ScientificContract::new(
                SpectralContract::new(SpectralSamplingLaw::IDENTITY, SpectralCoupling::Independent),
                MeasurementEquationContract::new(InstrumentResponse::PrimaryBeam, inner_products()),
            )
            .with_instrument_model(
                InstrumentModel::CasaAlmaAcaHeterogeneousInterferometricResponseV1,
            ),
            reconstruction(),
            weighting(),
            products,
            read_only_transaction(),
            numerics(false),
        ),
        observation(),
    )
    .expect("compile PB-corrected Taylor products");
    let list = compiled.product_list();
    let alpha = list
        .entry(ProductRole::SpectralIndex)
        .expect("spectral-index product");
    let alpha_error = list
        .entry(ProductRole::SpectralIndexError)
        .expect("spectral-index-error product");
    assert!(list.publication().members().contains(&alpha.id()));
    assert!(list.publication().members().contains(&alpha_error.id()));

    let pb_alpha = list
        .entry(ProductRole::PrimaryBeamSpectralIndex)
        .expect("internal primary-beam spectral index");
    assert_eq!(pb_alpha.name(), None);
    assert!(!list.publication().members().contains(&pb_alpha.id()));
    assert_eq!(
        list.entry(ProductRole::PrimaryBeam(ProductTerm::Taylor(0)))
            .expect("primary-beam Taylor-zero product")
            .validity(),
        ProductValidityRule::PrimaryBeam(product_validity().primary_beam())
    );
    assert!(
        list.entry(ProductRole::PrimaryBeam(ProductTerm::Taylor(1)))
            .is_none(),
        "CASA writes .pb.tt1 only for a pbcor"
    );
    assert!(
        list.entries()
            .iter()
            .all(|entry| entry.name() != Some(".pb.alpha"))
    );

    let corrected_alpha = list
        .entry(ProductRole::PbCorrectedSpectralIndex)
        .expect("PB-corrected spectral index");
    assert_eq!(corrected_alpha.beam(), ProductBeamRule::Inherit(alpha.id()));
    assert!(list.publication().members().contains(&corrected_alpha.id()));
}

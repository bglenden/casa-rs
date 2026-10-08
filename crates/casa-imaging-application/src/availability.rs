// SPDX-License-Identifier: LGPL-3.0-or-later
//! Installed imaging implementation availability at the application boundary.

use std::{error::Error, fmt};

use casa_imaging_model::{
    CompiledProblem, ImageDomainRole, InstrumentResponse, PolarizationCoordinate, ProductKind,
    ReconstructionBasis, RequiredCapability, SpectralKernel,
};

/// A task-surface requirement not represented by [`CompiledProblem`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum TaskRequirement {
    /// Spectral-cube task surface.
    SpectralCube,
    /// Cubedata task surface.
    SpectralCubedata,
    /// Moving-source REST-frame cube task surface.
    SpectralCubeSource,
    /// Multi-term continuum reconstruction through cube major cycles.
    SpectralMtmfsViaCube,
    /// Mosaic gridder request.
    MosaicGridder,
    /// W-projection gridder request.
    WProjection,
    /// A/W-projection gridder request.
    AwProjection,
    /// Auto-multithreshold masking request.
    Automasking,
    /// Standalone CLEAN-mask product request.
    MaskProduct,
    /// Initial model supplied by the caller.
    StartModel,
    /// `MODEL_DATA` persistence request.
    ModelColumnWrite,
    /// Serial CPU execution selected explicitly.
    SerialCpu,
    /// Automatic execution selection.
    ExecutionAuto,
    /// Fixed-tile CPU execution override.
    FixedTileCpu,
    /// Metal gridding override.
    MetalGridder,
    /// Grouped Metal row-run gridding override.
    MetalRowRunGroupedGridder,
    /// Non-Stokes-I or raw-correlation selection.
    PolarizationSelection,
    /// UV tapering.
    UvTaper,
    /// Per-channel weighting-density control.
    PerChannelWeightDensity,
    /// Explicit W-projection plane budget.
    WProjectionPlanes,
    /// Explicit source-stream memory target.
    MemoryTarget,
}

impl TaskRequirement {
    /// Complete stable task-only capability catalog for the current application
    /// contract.
    pub const ALL: [Self; 21] = [
        Self::SpectralCube,
        Self::SpectralCubedata,
        Self::SpectralCubeSource,
        Self::SpectralMtmfsViaCube,
        Self::MosaicGridder,
        Self::WProjection,
        Self::AwProjection,
        Self::Automasking,
        Self::MaskProduct,
        Self::StartModel,
        Self::ModelColumnWrite,
        Self::SerialCpu,
        Self::ExecutionAuto,
        Self::FixedTileCpu,
        Self::MetalGridder,
        Self::MetalRowRunGroupedGridder,
        Self::PolarizationSelection,
        Self::UvTaper,
        Self::PerChannelWeightDensity,
        Self::WProjectionPlanes,
        Self::MemoryTarget,
    ];

    /// Return the stable application-catalog identity.
    #[must_use]
    pub const fn catalog_id(self) -> &'static str {
        match self {
            Self::SpectralCube => "spectral_cube",
            Self::SpectralCubedata => "spectral_cubedata",
            Self::SpectralCubeSource => "spectral_cubesource",
            Self::SpectralMtmfsViaCube => "spectral_mtmfs_via_cube",
            Self::MosaicGridder => "mosaic_gridder",
            Self::WProjection => "w_projection",
            Self::AwProjection => "aw_projection",
            Self::Automasking => "automasking",
            Self::MaskProduct => "mask_product",
            Self::StartModel => "start_model",
            Self::ModelColumnWrite => "model_column_write",
            Self::SerialCpu => "serial_cpu",
            Self::ExecutionAuto => "execution_auto",
            Self::FixedTileCpu => "fixed_tile_cpu",
            Self::MetalGridder => "metal_gridder",
            Self::MetalRowRunGroupedGridder => "metal_row_run_grouped_gridder",
            Self::PolarizationSelection => "polarization_selection",
            Self::UvTaper => "uv_taper",
            Self::PerChannelWeightDensity => "per_channel_weight_density",
            Self::WProjectionPlanes => "w_projection_planes",
            Self::MemoryTarget => "memory_target",
        }
    }
}

/// One typed requirement not implemented by the installed imaging build.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum UnsupportedRequirement {
    /// A compiler-derived capability has no installed implementation.
    Capability(RequiredCapability),
    /// A task-only requirement has no installed implementation.
    Task(TaskRequirement),
    /// Non-Stokes-I or multi-polarization execution requires an independent-plane basis.
    IndependentBasisForPolarizationSelection,
    /// The implementation requires a scalar measurement equation.
    ScalarInstrumentResponse,
    /// The cube spectral kernel is neither nearest nor linear; cubic waits
    /// for #42, after IF-4.
    NearestOrLinearCubeInterpolation,
}

impl UnsupportedRequirement {
    /// Return the stable reason family used by typed transport projections.
    #[must_use]
    pub const fn catalog_kind(self) -> &'static str {
        match self {
            Self::Capability(_) => "capability",
            Self::Task(_) => "task",
            Self::IndependentBasisForPolarizationSelection
            | Self::ScalarInstrumentResponse
            | Self::NearestOrLinearCubeInterpolation => "constraint",
        }
    }

    /// Return the exact stable reason identity exposed by provider projections.
    #[must_use]
    pub fn catalog_id(self) -> String {
        match self {
            Self::Capability(requirement) => {
                format!("capability.{}", requirement.catalog_id())
            }
            Self::Task(requirement) => format!("task.{}", requirement.catalog_id()),
            Self::IndependentBasisForPolarizationSelection => {
                "constraint.independent_basis_for_polarization_selection".to_string()
            }
            Self::ScalarInstrumentResponse => "constraint.scalar_instrument_response".to_string(),
            Self::NearestOrLinearCubeInterpolation => {
                "constraint.nearest_or_linear_cube_interpolation".to_string()
            }
        }
    }
}

/// Owner-typed scientific or task capability represented in the installed
/// application catalog.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ImagingCapabilityRequirement {
    /// Compiler-derived backend-independent capability.
    Scientific(RequiredCapability),
    /// Task-only capability not represented by the compiled problem.
    Task(TaskRequirement),
}

impl ImagingCapabilityRequirement {
    /// Return the stable requirement identity.
    #[must_use]
    pub fn catalog_id(self) -> String {
        match self {
            Self::Scientific(requirement) => {
                format!("capability.{}", requirement.catalog_id())
            }
            Self::Task(requirement) => format!("task.{}", requirement.catalog_id()),
        }
    }

    /// Return the stable requirement kind used by transport projections.
    #[must_use]
    pub const fn catalog_kind(self) -> &'static str {
        match self {
            Self::Scientific(_) => "scientific",
            Self::Task(_) => "task",
        }
    }
}

/// One application-owned capability and its exact installed-build status.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ImagingCapabilityCatalogEntry {
    requirement: ImagingCapabilityRequirement,
    unsupported: Option<UnsupportedRequirement>,
}

impl ImagingCapabilityCatalogEntry {
    /// Return the typed requirement.
    #[must_use]
    pub const fn requirement(&self) -> ImagingCapabilityRequirement {
        self.requirement
    }

    /// Return the exact typed unavailability reason, or `None` when supported.
    #[must_use]
    pub const fn unsupported(&self) -> Option<UnsupportedRequirement> {
        self.unsupported
    }
}

/// Return every stable scientific, product, and task capability understood by
/// the current request/application contract with its exact installed status.
#[must_use]
pub fn installed_imaging_capability_catalog() -> Vec<ImagingCapabilityCatalogEntry> {
    let mut catalog = RequiredCapability::catalog()
        .into_iter()
        .map(|requirement| ImagingCapabilityCatalogEntry {
            requirement: ImagingCapabilityRequirement::Scientific(requirement),
            unsupported: (!supports_capability(requirement))
                .then_some(UnsupportedRequirement::Capability(requirement)),
        })
        .collect::<Vec<_>>();
    catalog.extend(TaskRequirement::ALL.into_iter().map(|requirement| {
        ImagingCapabilityCatalogEntry {
            requirement: ImagingCapabilityRequirement::Task(requirement),
            unsupported: (!supports_task(requirement))
                .then_some(UnsupportedRequirement::Task(requirement)),
        }
    }));
    catalog.sort_by_key(|entry| entry.requirement.catalog_id());
    catalog
}

/// Typed fail-closed result returned before physical planning or execution.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ImplementationUnavailable {
    unsupported: Vec<UnsupportedRequirement>,
}

impl ImplementationUnavailable {
    /// Return every unsupported requirement in deterministic order.
    #[must_use]
    pub fn unsupported(&self) -> &[UnsupportedRequirement] {
        &self.unsupported
    }
}

impl fmt::Display for ImplementationUnavailable {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "imaging request requires unsupported installed-implementation contract items: {:?}",
            self.unsupported
        )
    }
}

impl Error for ImplementationUnavailable {}

/// Require the compiled problem and task-only constraints to be supported by
/// the implementation installed in this build.
pub fn validate_installed_implementation(
    problem: &CompiledProblem,
    task_requirements: impl IntoIterator<Item = TaskRequirement>,
) -> Result<(), ImplementationUnavailable> {
    let mut unsupported = problem
        .required_capabilities()
        .iter()
        .copied()
        .filter(|capability| !supports_capability(*capability))
        .map(UnsupportedRequirement::Capability)
        .collect::<Vec<_>>();

    unsupported.extend(
        task_requirements
            .into_iter()
            .filter(|requirement| !supports_task(*requirement))
            .map(UnsupportedRequirement::Task),
    );

    debug_assert_eq!(
        problem.geometry().domains()[0].role(),
        &ImageDomainRole::Main
    );
    if coupled_basis_requires_independent_polarization(
        problem.reconstruction().basis(),
        problem.reconstruction().polarization().coordinates(),
    ) {
        unsupported.push(UnsupportedRequirement::IndependentBasisForPolarizationSelection);
    }
    let response = problem
        .science()
        .measurement_equation()
        .instrument_response();
    if response != InstrumentResponse::Scalar || problem.science().instrument_model().is_some() {
        unsupported.push(UnsupportedRequirement::ScalarInstrumentResponse);
    }
    if matches!(
        problem.reconstruction().basis(),
        ReconstructionBasis::ChannelLocal { .. }
    ) && !matches!(
        problem.science().spectral().sampling().kernel(),
        SpectralKernel::Identity | SpectralKernel::Nearest | SpectralKernel::Linear
    ) {
        unsupported.push(UnsupportedRequirement::NearestOrLinearCubeInterpolation);
    }
    unsupported.sort_unstable();
    unsupported.dedup();
    if unsupported.is_empty() {
        Ok(())
    } else {
        Err(ImplementationUnavailable { unsupported })
    }
}

fn coupled_basis_requires_independent_polarization(
    basis: ReconstructionBasis,
    coordinates: &[PolarizationCoordinate],
) -> bool {
    matches!(
        basis,
        ReconstructionBasis::Taylor { .. } | ReconstructionBasis::TaylorViaChannelMajor { .. }
    ) && coordinates != [PolarizationCoordinate::StokesI]
}

/// Task surfaces the major-cycle pass runs. W projection, A projection and
/// mosaic (with `mvc`, which always carries a primary beam) wait for their
/// convolution-function sets (IF-3, #652); Metal gridding waits for the
/// Metal backend (IF-4, #653).
const fn supports_task(requirement: TaskRequirement) -> bool {
    matches!(
        requirement,
        TaskRequirement::SpectralCube
            | TaskRequirement::SpectralCubedata
            | TaskRequirement::SpectralCubeSource
            | TaskRequirement::PolarizationSelection
            | TaskRequirement::Automasking
            | TaskRequirement::MaskProduct
            | TaskRequirement::ModelColumnWrite
            | TaskRequirement::PerChannelWeightDensity
            | TaskRequirement::SerialCpu
            | TaskRequirement::FixedTileCpu
    )
}

/// Scientific capabilities of the major-cycle pass. Faceted geometry has no
/// pass implementation until #664, after IF-4; the primary-beam
/// response, W and A projection and the mosaic weight products wait for
/// IF-3 (#652).
const fn supports_capability(capability: RequiredCapability) -> bool {
    matches!(
        capability,
        RequiredCapability::Polarization(_)
            | RequiredCapability::SpectralFrameTransform
            | RequiredCapability::SpectralResampling
            | RequiredCapability::CommonBeamSpectralCoupling
            | RequiredCapability::SequentialContinuumTransform
            | RequiredCapability::ConstantBasis
            | RequiredCapability::MultiDomainGeometry
            | RequiredCapability::TaylorBasis
            | RequiredCapability::ChannelLocalBasis
            | RequiredCapability::DirtyReconstruction
            | RequiredCapability::HogbomReconstruction
            | RequiredCapability::ClarkReconstruction
            | RequiredCapability::MultiscaleReconstruction
            | RequiredCapability::MtmfsReconstruction
            | RequiredCapability::NaturalWeighting
            | RequiredCapability::UniformWeighting
            | RequiredCapability::BriggsWeighting
            | RequiredCapability::BriggsBandwidthTaperWeighting
            | RequiredCapability::UnitResponseNormalization
            | RequiredCapability::FlatNoiseNormalization
            | RequiredCapability::FlatSkyNormalization
            | RequiredCapability::Product(ProductKind::Psf)
            | RequiredCapability::Product(ProductKind::Residual)
            | RequiredCapability::Product(ProductKind::Model)
            | RequiredCapability::Product(ProductKind::RestoredImage)
            | RequiredCapability::Product(ProductKind::SumWeights)
            | RequiredCapability::Product(ProductKind::Mask)
            | RequiredCapability::Product(ProductKind::Beam)
            | RequiredCapability::Product(ProductKind::PrimaryBeam)
            | RequiredCapability::Product(ProductKind::PbCorrectedImage)
            | RequiredCapability::Product(ProductKind::TaylorTerms)
            | RequiredCapability::Product(ProductKind::SpectralIndex)
            | RequiredCapability::Product(ProductKind::SpectralIndexError)
    )
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;

    use casa_imaging_model::PolarizationCoordinate;

    use super::*;

    #[test]
    fn convolution_function_sets_and_metal_wait_for_their_tickets() {
        for task in [
            TaskRequirement::MosaicGridder,
            TaskRequirement::WProjection,
            TaskRequirement::WProjectionPlanes,
            TaskRequirement::AwProjection,
            TaskRequirement::SpectralMtmfsViaCube,
            TaskRequirement::MetalGridder,
            TaskRequirement::MetalRowRunGroupedGridder,
        ] {
            assert!(!supports_task(task), "{task:?}");
        }
        for capability in [
            RequiredCapability::WProjection,
            RequiredCapability::AwProjection,
            RequiredCapability::PrimaryBeamResponse,
            RequiredCapability::FacetedGeometry,
            RequiredCapability::Product(ProductKind::Weight),
            RequiredCapability::Product(ProductKind::Sensitivity),
            RequiredCapability::Product(ProductKind::PbCorrectedSpectralIndex),
        ] {
            assert!(!supports_capability(capability), "{capability:?}");
        }
    }

    #[test]
    fn the_pass_runs_cubes_masks_writes_and_publication_beams() {
        for task in [
            TaskRequirement::SpectralCube,
            TaskRequirement::SpectralCubedata,
            TaskRequirement::SpectralCubeSource,
            TaskRequirement::PerChannelWeightDensity,
            TaskRequirement::Automasking,
            TaskRequirement::ModelColumnWrite,
        ] {
            assert!(supports_task(task), "{task:?}");
        }
        for product in [ProductKind::PrimaryBeam, ProductKind::PbCorrectedImage] {
            assert!(supports_capability(RequiredCapability::Product(product)));
        }
    }

    #[test]
    fn coupled_basis_polarization_constraint_covers_taylor() {
        for basis in [
            ReconstructionBasis::Taylor { terms: 2 },
            ReconstructionBasis::TaylorViaChannelMajor {
                terms: 2,
                channels: 4,
            },
        ] {
            assert!(!coupled_basis_requires_independent_polarization(
                basis,
                &[PolarizationCoordinate::StokesI]
            ));
            assert!(coupled_basis_requires_independent_polarization(
                basis,
                &[PolarizationCoordinate::StokesQ]
            ));
            assert!(coupled_basis_requires_independent_polarization(
                basis,
                &[PolarizationCoordinate::CircularRl]
            ));
            assert!(coupled_basis_requires_independent_polarization(
                basis,
                &[
                    PolarizationCoordinate::StokesI,
                    PolarizationCoordinate::StokesQ,
                ]
            ));
        }
    }

    #[test]
    fn t34_standard_polarization_routes_are_installed_without_full_mueller() {
        for coordinate in [
            PolarizationCoordinate::StokesQ,
            PolarizationCoordinate::StokesU,
            PolarizationCoordinate::StokesV,
            PolarizationCoordinate::LinearXy,
            PolarizationCoordinate::CircularRl,
        ] {
            assert!(supports_capability(RequiredCapability::Polarization(
                coordinate
            )));
        }
        assert!(!supports_capability(
            RequiredCapability::FullMuellerResponse
        ));

        let catalog = installed_imaging_capability_catalog();
        let stokes_q = RequiredCapability::Polarization(PolarizationCoordinate::StokesQ);
        assert_eq!(
            catalog
                .iter()
                .find(|entry| {
                    entry.requirement() == ImagingCapabilityRequirement::Scientific(stokes_q)
                })
                .and_then(ImagingCapabilityCatalogEntry::unsupported),
            None
        );
        let mueller = RequiredCapability::FullMuellerResponse;
        assert_eq!(
            catalog
                .iter()
                .find(|entry| {
                    entry.requirement() == ImagingCapabilityRequirement::Scientific(mueller)
                })
                .and_then(ImagingCapabilityCatalogEntry::unsupported),
            Some(UnsupportedRequirement::Capability(mueller))
        );
    }

    #[test]
    fn capability_catalog_is_complete_unique_and_exactly_typed() {
        let catalog = installed_imaging_capability_catalog();
        let ids = catalog
            .iter()
            .map(|entry| entry.requirement().catalog_id())
            .collect::<BTreeSet<_>>();
        assert_eq!(ids.len(), catalog.len());
        assert_eq!(
            catalog
                .iter()
                .find(|entry| {
                    entry.requirement()
                        == ImagingCapabilityRequirement::Task(TaskRequirement::AwProjection)
                })
                .and_then(ImagingCapabilityCatalogEntry::unsupported),
            Some(UnsupportedRequirement::Task(TaskRequirement::AwProjection))
        );
        assert!(
            catalog
                .iter()
                .find(|entry| {
                    entry.requirement()
                        == ImagingCapabilityRequirement::Scientific(RequiredCapability::Product(
                            ProductKind::Sensitivity,
                        ))
                })
                .is_some_and(|entry| entry.unsupported().is_some())
        );
        assert!(catalog.iter().any(|entry| {
            entry.requirement()
                == ImagingCapabilityRequirement::Scientific(RequiredCapability::Product(
                    ProductKind::RestoredImage,
                ))
                && entry.unsupported().is_none()
        }));
    }
}

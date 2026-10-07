// SPDX-License-Identifier: LGPL-3.0-or-later
//! Installed imaging implementation availability at the application boundary.

use std::{error::Error, fmt};

use casa_imaging_model::{
    CompiledProblem, ImageDomainRole, InstrumentModel, InstrumentResponse, PolarizationCoordinate,
    ProductKind, ReconstructionBasis, RequiredCapability, UvwCoordinateLaw,
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
    /// Facet execution currently requires the constant spectral basis.
    ConstantBasisForFacets,
    /// Non-Stokes-I or multi-polarization execution requires an independent-plane basis.
    IndependentBasisForPolarizationSelection,
    /// The implementation requires a scalar measurement equation.
    ScalarInstrumentResponse,
    /// W projection is not installed for mosaic UVW geometry.
    WProjectionWithMosaic,
    /// Metal cube initial wave cannot size coarse output channels; use the CPU
    /// backend. Each output channel must span at most one selected native
    /// channel spacing, which must be known before planning.
    MetalCubeCoarseOutputChannels,
}

impl UnsupportedRequirement {
    /// Return the stable reason family used by typed transport projections.
    #[must_use]
    pub const fn catalog_kind(self) -> &'static str {
        match self {
            Self::Capability(_) => "capability",
            Self::Task(_) => "task",
            Self::ConstantBasisForFacets
            | Self::IndependentBasisForPolarizationSelection
            | Self::ScalarInstrumentResponse
            | Self::WProjectionWithMosaic
            | Self::MetalCubeCoarseOutputChannels => "constraint",
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
            Self::ConstantBasisForFacets => "constraint.constant_basis_for_facets".to_string(),
            Self::IndependentBasisForPolarizationSelection => {
                "constraint.independent_basis_for_polarization_selection".to_string()
            }
            Self::ScalarInstrumentResponse => "constraint.scalar_instrument_response".to_string(),
            Self::WProjectionWithMosaic => "constraint.w_projection_with_mosaic".to_string(),
            Self::MetalCubeCoarseOutputChannels => {
                "constraint.metal_cube_coarse_output_channels".to_string()
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

    let mut coarse_metal_cube = false;
    unsupported.extend(
        task_requirements
            .into_iter()
            .filter(|requirement| {
                if *requirement == TaskRequirement::MetalGridder {
                    let cube = problem.geometry().spectral().output_channels() >= 2
                        && casa_imaging_runtime::CubePhase::supports(problem).unwrap_or(false)
                        && matches!(
                            problem.reconstruction().basis(),
                            ReconstructionBasis::ChannelLocal { .. }
                        );
                    let scalar = matches!(
                        problem.reconstruction().basis(),
                        ReconstructionBasis::Constant
                    ) && casa_imaging_runtime::supports_metal_normal(problem);
                    coarse_metal_cube = cube
                        && metal_cube_output_to_native_width(problem)
                            .is_none_or(|ratio| ratio > 1.0 + METAL_CUBE_WIDTH_TOLERANCE);
                    return !supports_task(*requirement) || !(cube || scalar);
                }
                !supports_task(*requirement)
            })
            .map(UnsupportedRequirement::Task),
    );
    if coarse_metal_cube {
        unsupported.push(UnsupportedRequirement::MetalCubeCoarseOutputChannels);
    }

    debug_assert_eq!(
        problem.geometry().domains()[0].role(),
        &ImageDomainRole::Main
    );
    let is_faceted = problem
        .geometry()
        .domains()
        .iter()
        .any(|domain| domain.facets().len() != 1);
    if is_faceted && problem.reconstruction().basis() != ReconstructionBasis::Constant {
        unsupported.push(UnsupportedRequirement::ConstantBasisForFacets);
    }
    if coupled_basis_requires_independent_polarization(
        problem.reconstruction().basis(),
        problem.reconstruction().polarization().coordinates(),
    ) {
        unsupported.push(UnsupportedRequirement::IndependentBasisForPolarizationSelection);
    }
    let installed_response = instrument_response_is_installed(
        problem
            .science()
            .measurement_equation()
            .instrument_response(),
        problem.science().instrument_model(),
        problem.reconstruction().basis(),
        matches!(
            problem.geometry().centres().pointing(),
            casa_imaging_model::PointingCentreLaw::Observation(_)
        ),
    );
    if !installed_response {
        unsupported.push(UnsupportedRequirement::ScalarInstrumentResponse);
    }
    if matches!(
        problem.geometry().uvw(),
        UvwCoordinateLaw::MosaicPhaseTrackingCentre
    ) && problem
        .science()
        .measurement_equation()
        .w_projection()
        .is_some()
    {
        unsupported.push(UnsupportedRequirement::WProjectionWithMosaic);
    }
    unsupported.sort_unstable();
    unsupported.dedup();
    if unsupported.is_empty() {
        Ok(())
    } else {
        Err(ImplementationUnavailable { unsupported })
    }
}

/// Output/native width ratio still treated as one native channel per output,
/// so equal widths survive per-row frequency-frame conversion.
const METAL_CUBE_WIDTH_TOLERANCE: f64 = 1.0e-3;

/// The Metal cube sizes its first initial wave before observing rows, for one
/// CASA fine sample per output channel and row. Compare the output increment
/// with the first selected native pair, which seeds that row-local CASA grid.
fn metal_cube_output_to_native_width(problem: &CompiledProblem) -> Option<f64> {
    let spectral = problem.geometry().spectral();
    let output_hz = spectral.channel_centre_hz(1)? - spectral.channel_centre_hz(0)?;
    let [source] = problem.selected_observation().read_set().sources() else {
        return None;
    };
    let [spectral_window] = source.selection().spectral_windows() else {
        return None;
    };
    let &[first, second, ..] = spectral_window.channel_indices() else {
        return None;
    };
    let catalog = spectral_window.coordinate_catalog()?;
    let native_hz = catalog.channel_frequency_hz(second as usize)?
        - catalog.channel_frequency_hz(first as usize)?;
    Some((output_hz / native_hz).abs())
}

fn instrument_response_is_installed(
    response: InstrumentResponse,
    model: Option<InstrumentModel>,
    basis: ReconstructionBasis,
    observation_pointing: bool,
) -> bool {
    matches!(
        (response, model, basis, observation_pointing),
        (InstrumentResponse::Scalar, None, _, _)
            | (
                InstrumentResponse::PrimaryBeam,
                Some(InstrumentModel::CasaAca7mInterferometricDirectPbV1),
                ReconstructionBasis::TaylorViaChannelMajor { .. },
                false,
            )
            | (
                InstrumentResponse::PrimaryBeam,
                Some(InstrumentModel::CasaAlmaAcaHeterogeneousInterferometricResponseV1),
                ReconstructionBasis::Constant | ReconstructionBasis::ChannelLocal { .. },
                true,
            )
            | (
                InstrumentResponse::PrimaryBeam,
                Some(InstrumentModel::CasaEvlaWidebandAwV1),
                _,
                _,
            )
    )
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

const fn supports_task(requirement: TaskRequirement) -> bool {
    if matches!(requirement, TaskRequirement::MetalGridder) {
        return cfg!(all(target_os = "macos", not(coverage)));
    }
    matches!(
        requirement,
        TaskRequirement::SpectralCube
            | TaskRequirement::SpectralCubedata
            | TaskRequirement::SpectralCubeSource
            | TaskRequirement::SpectralMtmfsViaCube
            | TaskRequirement::MosaicGridder
            | TaskRequirement::WProjection
            | TaskRequirement::AwProjection
            | TaskRequirement::WProjectionPlanes
            | TaskRequirement::PolarizationSelection
            | TaskRequirement::Automasking
            | TaskRequirement::MaskProduct
            | TaskRequirement::ModelColumnWrite
            | TaskRequirement::PerChannelWeightDensity
            | TaskRequirement::SerialCpu
            | TaskRequirement::FixedTileCpu
    )
}

const fn supports_capability(capability: RequiredCapability) -> bool {
    matches!(
        capability,
        RequiredCapability::Polarization(_)
            | RequiredCapability::SpectralFrameTransform
            | RequiredCapability::SpectralResampling
            | RequiredCapability::CommonBeamSpectralCoupling
            | RequiredCapability::SequentialContinuumTransform
            | RequiredCapability::ConstantBasis
            | RequiredCapability::FacetedGeometry
            | RequiredCapability::MultiDomainGeometry
            | RequiredCapability::WProjection
            | RequiredCapability::AwProjection
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
            | RequiredCapability::PrimaryBeamResponse
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
            | RequiredCapability::Product(ProductKind::Weight)
            | RequiredCapability::Product(ProductKind::Sensitivity)
            | RequiredCapability::Product(ProductKind::PrimaryBeam)
            | RequiredCapability::Product(ProductKind::PbCorrectedImage)
            | RequiredCapability::Product(ProductKind::TaylorTerms)
            | RequiredCapability::Product(ProductKind::SpectralIndex)
            | RequiredCapability::Product(ProductKind::SpectralIndexError)
            | RequiredCapability::Product(ProductKind::PbCorrectedSpectralIndex)
    )
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;

    use casa_imaging_model::PolarizationCoordinate;

    use super::*;

    #[test]
    fn t47_mosaic_gridder_is_installed_at_the_application_boundary() {
        assert!(supports_task(TaskRequirement::MosaicGridder));
    }

    #[test]
    fn t47_per_channel_weight_density_is_installed_at_the_application_boundary() {
        assert!(supports_task(TaskRequirement::PerChannelWeightDensity));
    }

    #[test]
    fn t47_mosaic_products_are_installed_at_the_application_boundary() {
        for product in [
            ProductKind::Weight,
            ProductKind::Sensitivity,
            ProductKind::PbCorrectedSpectralIndex,
        ] {
            assert!(supports_capability(RequiredCapability::Product(product)));
        }
    }

    #[test]
    fn t41_primary_beam_response_is_installed_at_the_application_boundary() {
        assert!(supports_capability(RequiredCapability::PrimaryBeamResponse));
    }

    #[test]
    fn direct_and_heterogeneous_primary_beams_have_disjoint_installed_routes() {
        assert!(instrument_response_is_installed(
            InstrumentResponse::PrimaryBeam,
            Some(InstrumentModel::CasaAca7mInterferometricDirectPbV1),
            ReconstructionBasis::TaylorViaChannelMajor {
                terms: 2,
                channels: 16,
            },
            false,
        ));
        assert!(!instrument_response_is_installed(
            InstrumentResponse::PrimaryBeam,
            Some(InstrumentModel::CasaAlmaAcaHeterogeneousInterferometricResponseV1),
            ReconstructionBasis::TaylorViaChannelMajor {
                terms: 2,
                channels: 16,
            },
            false,
        ));
        assert!(instrument_response_is_installed(
            InstrumentResponse::PrimaryBeam,
            Some(InstrumentModel::CasaAlmaAcaHeterogeneousInterferometricResponseV1),
            ReconstructionBasis::Constant,
            true,
        ));
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
    fn metal_catalog_matches_the_compiled_platform() {
        let catalog = installed_imaging_capability_catalog();
        let metal = catalog
            .iter()
            .find(|entry| {
                entry.requirement()
                    == ImagingCapabilityRequirement::Task(TaskRequirement::MetalGridder)
            })
            .unwrap();
        assert_eq!(
            metal.unsupported().is_none(),
            cfg!(all(target_os = "macos", not(coverage)))
        );
        assert!(!supports_task(TaskRequirement::MetalRowRunGroupedGridder));
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
            None
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
                .is_some_and(|entry| entry.unsupported().is_none())
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

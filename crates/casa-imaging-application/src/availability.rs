// SPDX-License-Identifier: LGPL-3.0-or-later
//! Installed imaging implementation availability at the application boundary.

use std::{error::Error, fmt};

use casa_imaging_model::{
    CompiledProblem, ImageDomainRole, InstrumentResponse, ModelStateIdentity, PhaseCentreLaw,
    PolarizationCoordinate, ProductKind, ReconstructionBasis, RequiredCapability,
};

/// A task-surface requirement not represented by [`CompiledProblem`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum TaskRequirement {
    /// Spectral-cube task surface.
    SpectralCube,
    /// Cubedata task surface.
    SpectralCubedata,
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
    /// Metal row-run gridding override.
    MetalRowRunGridder,
    /// Grouped Metal row-run gridding override.
    MetalRowRunGroupedGridder,
    /// Automatic FFT selection.
    FftAuto,
    /// RustFFT override.
    RustFft,
    /// Accelerate FFT override.
    Accelerate,
    /// FFTW override.
    Fftw,
    /// Metal MPSGraph FFT override.
    MetalMpsGraph,
    /// Task controls outside the installed spectral-cycle contract.
    UnsupportedControls,
}

impl TaskRequirement {
    /// Complete stable task-only capability catalog for the current application
    /// contract.
    pub const ALL: [Self; 21] = [
        Self::SpectralCube,
        Self::SpectralCubedata,
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
        Self::MetalRowRunGridder,
        Self::MetalRowRunGroupedGridder,
        Self::FftAuto,
        Self::RustFft,
        Self::Accelerate,
        Self::Fftw,
        Self::MetalMpsGraph,
        Self::UnsupportedControls,
    ];

    /// Return the stable application-catalog identity.
    #[must_use]
    pub const fn catalog_id(self) -> &'static str {
        match self {
            Self::SpectralCube => "spectral_cube",
            Self::SpectralCubedata => "spectral_cubedata",
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
            Self::MetalRowRunGridder => "metal_row_run_gridder",
            Self::MetalRowRunGroupedGridder => "metal_row_run_grouped_gridder",
            Self::FftAuto => "fft_auto",
            Self::RustFft => "rust_fft",
            Self::Accelerate => "accelerate_fft",
            Self::Fftw => "fftw",
            Self::MetalMpsGraph => "metal_mps_graph",
            Self::UnsupportedControls => "unsupported_controls",
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
    /// The implementation requires exactly one observation source.
    SingleObservationSource,
    /// The implementation requires one facet.
    SingleFacet,
    /// The implementation requires a fixed phase centre.
    FixedPhaseCentre,
    /// The implementation does not accept an initial model.
    EmptyInitialModel,
    /// The implementation does not write `MODEL_DATA`.
    NoModelColumnWrite,
    /// The implementation requires a scalar measurement equation.
    ScalarInstrumentResponse,
}

impl UnsupportedRequirement {
    /// Return the exact stable reason identity exposed by provider projections.
    #[must_use]
    pub fn catalog_id(self) -> String {
        match self {
            Self::Capability(requirement) => {
                format!("capability.{}", requirement.catalog_id())
            }
            Self::Task(requirement) => format!("task.{}", requirement.catalog_id()),
            Self::SingleObservationSource => "constraint.single_observation_source".to_string(),
            Self::SingleFacet => "constraint.single_facet".to_string(),
            Self::FixedPhaseCentre => "constraint.fixed_phase_centre".to_string(),
            Self::EmptyInitialModel => "constraint.empty_initial_model".to_string(),
            Self::NoModelColumnWrite => "constraint.no_model_column_write".to_string(),
            Self::ScalarInstrumentResponse => "constraint.scalar_instrument_response".to_string(),
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
    let mut requirements = vec![
        RequiredCapability::MultiDomainGeometry,
        RequiredCapability::FacetedGeometry,
        RequiredCapability::SpectralFrameTransform,
        RequiredCapability::SpectralResampling,
        RequiredCapability::SequentialContinuumTransform,
        RequiredCapability::CommonBeamSpectralCoupling,
        RequiredCapability::PrimaryBeamResponse,
        RequiredCapability::FullMuellerResponse,
        RequiredCapability::UvTaper,
        RequiredCapability::ConstantBasis,
        RequiredCapability::TaylorBasis,
        RequiredCapability::ChannelLocalBasis,
        RequiredCapability::JointContinuumLineReconstruction,
        RequiredCapability::DirtyReconstruction,
        RequiredCapability::HogbomReconstruction,
        RequiredCapability::ClarkReconstruction,
        RequiredCapability::MultiscaleReconstruction,
        RequiredCapability::MtmfsReconstruction,
        RequiredCapability::NaturalWeighting,
        RequiredCapability::UniformWeighting,
        RequiredCapability::BriggsWeighting,
        RequiredCapability::BriggsBandwidthTaperWeighting,
        RequiredCapability::UnitResponseNormalization,
        RequiredCapability::FlatNoiseNormalization,
        RequiredCapability::FlatSkyNormalization,
    ];
    requirements.extend(
        PolarizationCoordinate::ALL
            .into_iter()
            .map(RequiredCapability::Polarization),
    );
    requirements.extend(
        ProductKind::ALL
            .into_iter()
            .map(RequiredCapability::Product),
    );

    let mut catalog = requirements
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

    if problem.inputs().observation_snapshot().sources().len() != 1 {
        unsupported.push(UnsupportedRequirement::SingleObservationSource);
    }
    debug_assert_eq!(
        problem.geometry().domains()[0].role(),
        &ImageDomainRole::Main
    );
    if problem
        .geometry()
        .domains()
        .iter()
        .any(|domain| domain.facets().len() != 1)
    {
        unsupported.push(UnsupportedRequirement::SingleFacet);
    }
    if !matches!(
        problem.geometry().centres().phase_tracking(),
        PhaseCentreLaw::Fixed(_)
    ) {
        unsupported.push(UnsupportedRequirement::FixedPhaseCentre);
    }
    if !matches!(problem.inputs().model(), ModelStateIdentity::Empty) {
        unsupported.push(UnsupportedRequirement::EmptyInitialModel);
    }
    if problem
        .science()
        .measurement_equation()
        .instrument_response()
        != InstrumentResponse::Scalar
    {
        unsupported.push(UnsupportedRequirement::ScalarInstrumentResponse);
    }
    if !matches!(
        problem.reconstruction().basis(),
        ReconstructionBasis::Taylor { .. }
    ) {
        for product in [ProductKind::PrimaryBeam, ProductKind::PbCorrectedImage] {
            if problem.products().products().contains(&product) {
                unsupported.push(UnsupportedRequirement::Capability(
                    RequiredCapability::Product(product),
                ));
            }
        }
    }

    unsupported.sort_unstable();
    unsupported.dedup();
    if unsupported.is_empty() {
        Ok(())
    } else {
        Err(ImplementationUnavailable { unsupported })
    }
}

const fn supports_task(requirement: TaskRequirement) -> bool {
    matches!(
        requirement,
        TaskRequirement::SpectralCube
            | TaskRequirement::SpectralCubedata
            | TaskRequirement::Automasking
            | TaskRequirement::MaskProduct
            | TaskRequirement::ModelColumnWrite
            | TaskRequirement::SerialCpu
            | TaskRequirement::FixedTileCpu
            | TaskRequirement::RustFft
    )
}

const fn supports_capability(capability: RequiredCapability) -> bool {
    matches!(
        capability,
        RequiredCapability::Polarization(PolarizationCoordinate::StokesI)
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
            | RequiredCapability::JointContinuumLineReconstruction
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

    use super::*;

    #[test]
    fn t46_joint_reconstruction_is_installed_at_the_application_boundary() {
        assert!(supports_capability(
            RequiredCapability::JointContinuumLineReconstruction
        ));
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
        assert_eq!(
            catalog
                .iter()
                .find(|entry| {
                    entry.requirement()
                        == ImagingCapabilityRequirement::Scientific(RequiredCapability::Product(
                            ProductKind::Sensitivity,
                        ))
                })
                .and_then(ImagingCapabilityCatalogEntry::unsupported),
            Some(UnsupportedRequirement::Capability(
                RequiredCapability::Product(ProductKind::Sensitivity)
            ))
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

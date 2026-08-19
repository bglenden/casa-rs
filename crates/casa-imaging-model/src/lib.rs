// SPDX-License-Identifier: LGPL-3.0-or-later
#![warn(missing_docs)]
//! Logical, backend-independent imaging problem compilation.

mod compiled_problem;

pub use compiled_problem::{
    CompileProblemError, CompiledGeometryId, CompiledProblem, CompiledProblemId, FieldGeometry,
    FiniteValuePolicy, GeometryContract, ImagingRequest, ImagingRequestVersion, InstrumentResponse,
    LogicalIdentity, MeasurementEquationContract, ModelStateIdentity, NumericPrecision,
    NumericalStage, NumericsContract, NumericsContractId, ObservationSnapshotId,
    PolarizationContract, PolarizationCoordinate, ProblemInputIdentities, ProblemSpecification,
    ProductKind, ProductNormalization, ProductRequirements, ProjectionGeometry,
    ReconstructionAlgorithm, ReconstructionBasis, ReconstructionContract, ReconstructionControls,
    ReductionPolicy, ReferenceDataKind, RequiredCapability, RestoringBeamPolicy,
    ScientificContract, SpectralContract, SpectralCoupling, SpectralFrame, SpectralSampling,
    StageErrorBudget, UvTaper, WeightDensityScope, WeightingContract, WeightingScheme, compile,
};

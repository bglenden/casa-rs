// SPDX-License-Identifier: LGPL-3.0-or-later
#![warn(missing_docs)]
//! Logical, backend-independent imaging problem compilation.

mod compiled_problem;

pub use compiled_problem::{
    CompileProblemError, CompiledGeometryId, CompiledProblem, CompiledProblemId, FiniteValuePolicy,
    LogicalIdentity, ModelStateIdentity, NumericPrecision, NumericalStage, NumericsContract,
    ObservationSnapshotId, ProblemInputIdentities, ProblemSpecification, ProductKind,
    ProductNormalization, ProductRequirements, ReconstructionAlgorithm, ReconstructionBasis,
    ReconstructionContract, ReconstructionControls, ReductionPolicy, ReferenceDataKind,
    RequiredCapability, RestoringBeamPolicy, StageErrorBudget, WeightDensityScope,
    WeightingContract, WeightingScheme, compile_problem,
};

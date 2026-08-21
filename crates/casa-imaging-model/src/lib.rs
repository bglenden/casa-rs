// SPDX-License-Identifier: LGPL-3.0-or-later
#![warn(missing_docs)]
//! Logical, backend-independent imaging problem compilation.

mod compiled_problem;
mod complete_data_operator_output;
mod geometry;
mod measurement_equation;
mod model_generation;
mod normal_state_generation;
mod observation;
mod product_graph;
mod selected_observation;
mod selected_observation_sample;
mod transaction;

pub use compiled_problem::{
    CompileProblemError, CompiledProblem, CompiledProblemId, FiniteValuePolicy, ImagingRequest,
    ImagingRequestVersion, InstrumentResponse, LogicalIdentity, MeasurementEquationContract,
    ModelStateIdentity, NumericPrecision, NumericalStage, NumericsContract, NumericsContractId,
    PolarizationContract, PolarizationCoordinate, PrimaryBeamValidityPolicy,
    ProblemInputIdentities, ProblemSpecification, ProductBlankingPolicy, ProductKind,
    ProductNormalization, ProductRequirements, ProductSupportComparison, ProductValidityPolicies,
    ProductValidityPolicyError, ReconstructionAlgorithm, ReconstructionBasis,
    ReconstructionContract, ReconstructionControls, ReductionPolicy, ReferenceDataKind,
    RequiredCapability, RestoringBeamPolicy, ScientificContract, SpectralContract,
    SpectralCoupling, SpectralSampling, StageErrorBudget, TaylorSupportReference,
    TaylorValidityPolicy, UvTaper, WeightDensityScope, WeightingContract, WeightingScheme, compile,
};

pub use complete_data_operator_output::{
    CompleteDataOperatorOutputId, CompleteDataPrimitiveCatalog, CompleteDataPrimitiveId,
    CompleteDataPrimitiveKind,
};

pub use geometry::{
    AxisOrder, CentreLaws, CompileGeometryError, CompiledGeometry, CompiledGeometryId,
    CompiledImageDomain, DelayCentreLaw, DirectionCoordinateSpec, DirectionFrame,
    DopplerConvention, Epoch, FacetLayout, FacetWindow, FrequencyFrame, GeometryInput, ImageAxis,
    ImageDomainRole, ImageDomainSpec, ImageShape, ItrfPosition, MissingPointingPolicy,
    ObservationPointingLaw, PhaseCentreLaw, PointingCentreLaw, PointingDirectionColumn,
    PointingDirectionSemantic, PointingExtrapolation, PointingInterpolation, PointingTimeSampling,
    Projection, RestFrequency, SkyDirection, SpectralCoordinateSpec, SpectralFrameAnchor,
    SpectralWcs, TimeScale, UvwAxes, UvwCoordinateLaw, UvwUnit, VisibilityPhaseConvention,
};

pub use measurement_equation::{
    DeclaredInnerProducts, MeasurementOperatorContract, ModelCoefficientSpace, ModelInnerProduct,
    NormalEquationContract, NormalEquationContractId, NormalEquationForm, NormalStateNormalization,
    NormalStateSpace, PairedMeasurementTransform, PairedTransformKind, ProductBoundaryOperation,
    ProductNormalizationBoundary, VisibilityInnerProduct, VisibilitySampleSpace,
    WeightingGenerationCompletionError, WeightingGenerationCompletionEvidence,
    WeightingGenerationId, WeightingOperatorContract, WeightingSource, WeightingSourceCompletion,
};

pub use model_generation::{
    ModelGeneration, ModelGenerationCommitment, ModelGenerationCommitmentError,
    ModelGenerationCommitmentId, ModelGenerationCompletionEvidence, ModelGenerationError,
    ModelGenerationId,
};

pub use normal_state_generation::{
    FinalReconciliationCommitment, FinalReconciliationCommitmentError,
    FinalReconciliationCommitmentId,
};

pub use selected_observation::{
    SelectedObservationCommitment, SelectedObservationCommitmentId,
    SelectedObservationInspectionError, SelectedSampleEvaluation,
};

pub use selected_observation_sample::{
    SelectedObservationGenerationId, SelectedObservationSample, SelectedPointingDirections,
    SelectedPredictionTarget, SelectedSampleAddress, SelectedSampleCoordinates,
    SelectedSampleMetadata, SelectedVisibilitySample,
};

pub use observation::{
    AntennaBaseline, AntennaSelection, ColumnGeneration, CompileObservationError, ConsistencyToken,
    CorrelationProduct, CorrelationSelection, CorrelationType, DataDescriptionSelection,
    FlagPolicy, IdSelection, IntentSelection, MeasurementSetIdentity, MetadataGeneration,
    MetadataTableKind, ModelColumnState, MsColumnKind, ObservationConsistencyError,
    ObservationProvenanceId, ObservationSelection, ObservationSnapshot, ObservationSnapshotId,
    ObservationSnapshotInput, ObservationSource, ObservationSourceInput,
    ObservationSourceProvenance, ObservationSourceState, ObservationState, ResolvedIntent,
    RowSelection, SelectedColumns, SelectedMainRow, SelectedRowManifestValidationError,
    SelectedRowSequenceError, SelectedRowSequenceId, SelectedRows, SelectionBound,
    SourceGenerations, SpectralWindowSelection, TimeRange, TimeSelection, UvDistanceRange,
    UvDistanceUnit, UvSelection, VisibilityColumn, WeightColumn, compile_observation,
};

pub use transaction::{
    MeasurementSetReadAccess, ModelColumnInitialization, ModelColumnPrecondition, ModelColumnWrite,
    ModelColumnWriteAccess, ModelColumnWriteDisposition, ObservationReadSet,
    ObservationTransactionContract, ObservationTransactionId, ObservationTransactionRequirements,
    ObservationWriteSet, compile_observation_transaction,
};

pub use product_graph::{
    PlannedProductGeneration, ProductArtifactId, ProductAxes, ProductAxisKind, ProductBeamRule,
    ProductElementRepresentation, ProductGeneration, ProductGenerationAuthority,
    ProductGenerationAuthorityError, ProductGenerationError, ProductGenerationId,
    ProductGenerationSeal, ProductGraph, ProductGraphId, ProductNode, ProductNodeId,
    ProductPayloadEnvelope, ProductPublication, ProductPublicationJoin, ProductRole, ProductSchema,
    ProductSource, ProductSourceBinding, ProductSourceCommitment, ProductSourceCompletionEvidence,
    ProductSourceGenerationId, ProductSourceId, ProductSourceRole, ProductTerm, ProductUnit,
    ProductValidityRule,
};

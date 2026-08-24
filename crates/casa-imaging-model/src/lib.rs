// SPDX-License-Identifier: LGPL-3.0-or-later
#![warn(missing_docs)]
//! Logical, backend-independent imaging problem compilation.

mod compiled_problem;
mod geometry;
mod measurement_equation;
mod model_state;
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
    validate_compiled_problem_identity,
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
    NormalEquationContract, NormalEquationForm, NormalStateNormalization, NormalStateSpace,
    PairedMeasurementTransform, PairedTransformKind, ProductBoundaryOperation,
    ProductNormalizationBoundary, VisibilityInnerProduct, VisibilitySampleSpace,
    WeightingCommitmentId, WeightingOperatorContract, WeightingSource,
};

pub use model_state::{
    ModelBasisConversionRegistry, ModelBounds, ModelCell, ModelContractError, ModelDeltaTerm,
    ModelDirectionConversionRegistry, ModelExecutionAttemptId, ModelInputCommitment,
    ModelInputCommitmentIdentity, ModelInvalidContributorPolicy, ModelLifecycleContract,
    ModelLifecycleContractId, ModelLifecycleRequirements, ModelPolarizationConversionRegistry,
    ModelReprojectedSeedProjection, ModelReprojectionPolicy, ModelSample, ModelSourceShape,
    ModelStateEncoding, ModelSupport, ModelSupportSemantics, ModelUncoveredTargetPolicy,
    ModelValue, model_reprojected_seed_mapping_identity, model_support_identity,
    validate_model_lifecycle_contract_identity, validate_model_reprojection_contract_identity,
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

pub use selected_observation::{
    SelectedObservationCommitment, SelectedObservationCommitmentId,
    SelectedObservationInspectionError, SelectedObservationPassError, SelectedSampleEvaluation,
};

pub use selected_observation_sample::{
    SelectedObservationGenerationId, SelectedObservationSample, SelectedPointingDirections,
    SelectedPredictionTarget, SelectedSampleAddress, SelectedSampleCoordinates,
    SelectedSampleMetadata, SelectedVisibilitySample,
};

pub use product_graph::{
    AtomicStoreProtocol, ProductAxes, ProductAxisKind, ProductBeamRule, ProductGraph,
    ProductGraphId, ProductNode, ProductNodeId, ProductPublication, ProductRole, ProductSchema,
    ProductTerm, ProductUnit, ProductValidityRule,
};

pub use transaction::{
    MeasurementSetReadAccess, ModelColumnInitialization, ModelColumnPrecondition, ModelColumnWrite,
    ModelColumnWriteAccess, ModelColumnWriteDisposition, ObservationReadSet,
    ObservationTransactionContract, ObservationTransactionId, ObservationTransactionRequirements,
    ObservationWriteSet,
};

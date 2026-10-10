// SPDX-License-Identifier: LGPL-3.0-or-later
#![warn(missing_docs)]
//! Logical, backend-independent imaging problem compilation.

mod compiled_problem;
mod geometry;
mod measurement_equation;
mod model_state;
mod native_aw;
mod observation;
mod product_graph;
mod selected_observation;
mod selected_observation_sample;
mod transaction;
mod visibility_transform;

pub use compiled_problem::{
    AwProjectionContract, AwProjectionContractError, CompileProblemError, CompiledProblem,
    CompiledProblemId, FiniteValuePolicy, HogbomIterationAccounting, InstrumentModel,
    InstrumentResponse, LogicalIdentity, MeasurementEquationContract, ModelStateIdentity,
    NumericPrecision, NumericalStage, NumericsContract, NumericsContractId, PolarizationContract,
    PolarizationCoordinate, PrimaryBeamValidityPolicy, ProblemInput, ProblemInputIdentities,
    ProblemSpecification, ProductBlankingPolicy, ProductKind, ProductNormalization,
    ProductRequirements, ProductSupportComparison, ProductValidityPolicies,
    ProductValidityPolicyError, ReconstructionAlgorithm, ReconstructionBasis,
    ReconstructionContract, ReconstructionControls, ReductionPolicy, ReferenceDataKind,
    RequiredCapability, RestoringBeamPolicy, ScientificContract, SpectralContract,
    SpectralCoupling, SpectralCovariance, SpectralEdgePolicy, SpectralKernel, SpectralSamplingLaw,
    StageErrorBudget, TaylorSupportReference, TaylorValidityPolicy, UncorrectedImageMaskPolicy,
    UvTaper, WProjectionContract, WProjectionContractError, WStatistics, WeightDensityScope,
    WeightingContract, WeightingScheme, compile, validate_compiled_problem_identity,
};

pub use geometry::{
    AxisOrder, CentreLaws, CompileGeometryError, CompiledGeometry, CompiledGeometryId,
    CompiledImageDomain, DelayCentreLaw, DirectionCoordinateSpec, DirectionFrame,
    DopplerConvention, Epoch, FacetLayout, FacetWindow, FrequencyFrame, GeometryInput, ImageAxis,
    ImageDomainRole, ImageDomainSpec, ImageShape, ItrfPosition, MissingPointingPolicy,
    ObservationPointingLaw, PhaseCentreLaw, PointingCentreLaw, PointingDirectionColumn,
    PointingDirectionSemantic, PointingExtrapolation, PointingInterpolation, PointingTimeSampling,
    Projection, PsfPhaseCentreLaw, RestFrequency, SkyDirection, SpectralCoordinateSpec,
    SpectralFrameAnchor, SpectralWcs, TimeScale, UvwAxes, UvwCoordinateLaw, UvwUnit,
    VisibilityPhaseConvention,
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
    ModelSourceSupportInspection, ModelStateEncoding, ModelSupport, ModelSupportSemantics,
    ModelUncoveredTargetPolicy, ModelValue, model_reprojected_seed_mapping_identity,
    model_support_identity, try_model_support_identity, validate_model_lifecycle_contract_identity,
    validate_model_reprojection_contract_identity,
};

pub use observation::{
    AntennaBaseline, AntennaSelection, CompileObservationError, CorrelationProduct,
    CorrelationSelection, CorrelationType, DataDescriptionSelection, FlagPolicy, IdSelection,
    IntentSelection, MsColumnKind, ObservationProvenanceId, ObservationSelection,
    ObservationSnapshot, ObservationSnapshotId, ObservationSnapshotInput, ObservationSource,
    ObservationSourceInput, ObservationSourceProvenance, ResolvedIntent, RowSelection,
    SelectedColumns, SelectedMainRow, SelectedRowManifestValidationError, SelectedRowSequenceError,
    SelectedRowSequenceId, SelectedRows, SelectedRowsBuilder, SelectionBound,
    SpectralWindowCoordinateCatalog, SpectralWindowSelection, TimeRange, TimeSelection,
    UvDistanceRange, UvDistanceUnit, UvSelection, VisibilityColumn, WeightColumn,
    compile_observation,
};

pub use native_aw::{
    EvlaAwCellRequest, EvlaDishSurface, NativeAwFrequencyGroup, NativeAwGrid, NativeAwRequestError,
    NativeAwRequestInput, NativeAwTerms,
};

pub use selected_observation::{
    SelectedObservationCommitment, SelectedObservationCommitmentId, SelectedSampleEvaluation,
};

mod selected_numeric;
pub use selected_numeric::{SelectedNumericRow, SelectedNumericVisibility, SelectedNumericWeights};

pub use selected_observation_sample::{
    AntennaResponseClass, SelectedAntennaResponses, SelectedImageDomainProjection,
    SelectedImageDomainProjections, SelectedObservationRunChannel, SelectedObservationRunRow,
    SelectedPhaseCentreProjection, SelectedPointingDirections, SelectedSampleCoordinates,
    SelectedSampleMetadata,
};

pub use product_graph::{
    IndependentProductStoreProtocol, ProductAxes, ProductAxisKind, ProductBeamRule, ProductGraph,
    ProductGraphId, ProductNode, ProductNodeId, ProductPixelMask, ProductPublication, ProductRole,
    ProductSchema, ProductStorageContract, ProductTerm, ProductUnit, ProductValidityRule,
};

pub use transaction::{
    CorrectedDataWrite, MeasurementSetReadAccess, ModelColumnWrite, ObservationReadSet,
    ObservationTransactionCompileError, ObservationTransactionContract, ObservationTransactionId,
    ObservationTransactionRequirements, ObservationWriteSet, SelectedVisibilityWriteAccess,
};

pub use visibility_transform::{
    ContinuumChannelRole, ContinuumChannelUse, ContinuumCovariancePolicy, ContinuumFitRule,
    ContinuumTransformContractError, SequentialContinuumTransform,
};

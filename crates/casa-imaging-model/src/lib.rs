// SPDX-License-Identifier: LGPL-3.0-or-later
#![warn(missing_docs)]
//! Logical, backend-independent imaging problem compilation.

mod aw_pointing;
mod batch_geometry;
mod compiled_problem;
mod frame_refs;
mod geometry;
mod image_size;
mod measurement_equation;
mod model_state;
mod native_aw;
mod observation;
mod pointing_law;
mod product_list;
mod selected_observation_sample;
mod transaction;
mod uvw_projection;
mod visibility_transform;

pub use compiled_problem::{
    AwProjectionContract, AwProjectionContractError, CompileProblemError, CompiledProblem,
    CycleThresholdRule, FiniteValuePolicy, HogbomIterationAccounting, InstrumentModel,
    InstrumentResponse, MeasurementEquationContract, NumericPrecision, NumericalStage,
    NumericsContract, PolarizationContract, PolarizationCoordinate, PrimaryBeamValidityPolicy,
    ProblemInput, ProblemSpecification, ProductBlankingPolicy, ProductKind, ProductNormalization,
    ProductRequirements, ProductSupportComparison, ProductValidityPolicies,
    ProductValidityPolicyError, ReconstructionAlgorithm, ReconstructionBasis,
    ReconstructionContract, ReconstructionControls, ReductionPolicy, RequiredCapability,
    RestoringBeamPolicy, ScientificContract, SpectralContract, SpectralCoupling,
    SpectralCovariance, SpectralEdgePolicy, SpectralKernel, SpectralSamplingLaw, StageErrorBudget,
    TaylorSupportReference, TaylorValidityPolicy, UncorrectedImageMaskPolicy, UvTaper,
    WProjectionContract, WProjectionContractError, WStatistics, WeightDensityScope,
    WeightingContract, WeightingScheme, compile,
};

pub use geometry::{
    AxisOrder, CentreLaws, CompileGeometryError, CompiledGeometry, CompiledImageDomain,
    DirectionCoordinateSpec, DirectionFrame, DopplerConvention, Epoch, FacetLayout, FacetWindow,
    FrequencyFrame, GeometryInput, ImageAxis, ImageDomainRole, ImageDomainSpec, ImageShape,
    ItrfPosition, MissingPointingPolicy, ObservationPointingLaw, PhaseCentreLaw, PointingCentreLaw,
    PointingDirectionColumn, PointingDirectionSemantic, PointingExtrapolation,
    PointingInterpolation, PointingTimeSampling, Projection, PsfPhaseCentreLaw, RestFrequency,
    SkyDirection, SpectralCoordinateSpec, SpectralFrameAnchor, SpectralWcs, TimeScale, UvwAxes,
    UvwCoordinateLaw, UvwUnit, VisibilityPhaseConvention,
};
pub use image_size::optimum_image_size;

pub use measurement_equation::{
    DeclaredInnerProducts, MeasurementOperatorContract, ModelCoefficientSpace, ModelInnerProduct,
    NormalEquationContract, NormalEquationForm, NormalStateNormalization, NormalStateSpace,
    PairedMeasurementTransform, PairedTransformKind, ProductBoundaryOperation,
    ProductNormalizationBoundary, VisibilityInnerProduct, VisibilitySampleSpace,
    WeightingOperatorContract, WeightingSource,
};

pub use model_state::{
    ModelBounds, ModelCell, ModelContractError, ModelDeltaTerm, ModelLifecycleContract,
    ModelLifecycleRequirements, ModelSample, ModelSourceShape, ModelSupport, ModelValue,
};

pub use observation::{
    CompileObservationError, CorrelationProduct, CorrelationSelection, DataDescriptionSelection,
    FlagPolicy, IdSelection, IntentSelection, MsColumnKind, ObservationSelection,
    ObservationSnapshot, ObservationSnapshotInput, ObservationSource, ObservationSourceInput,
    ObservationSourceProvenance, ResolvedIntent, RowSelection, SelectedColumns, SelectedMainRow,
    SelectedRowSequenceError, SelectedRows, SelectedRowsBuilder, SelectionBound,
    SpectralWindowCoordinateCatalog, SpectralWindowSelection, UvDistanceRange, UvDistanceUnit,
    UvSelection, VisibilityColumn, WeightColumn, compile_observation,
};

pub use native_aw::{
    EvlaAwCellRequest, EvlaDishSurface, NativeAwFrequencyGroup, NativeAwGrid, NativeAwRequestError,
    NativeAwRequestInput, NativeAwTerms,
};

mod selected_numeric;
pub use selected_numeric::{SelectedNumericRow, SelectedNumericVisibility, SelectedNumericWeights};

pub use selected_observation_sample::{
    AntennaResponseClass, SelectedAntennaResponses, SelectedImageDomainProjection,
    SelectedImageDomainProjections, SelectedObservationRunChannel, SelectedObservationRunRow,
    SelectedPhaseCentreProjection, SelectedPointingDirections, SelectedSampleCoordinates,
    SelectedSampleMetadata,
};

pub use product_list::{
    ProductAxes, ProductAxisKind, ProductBeamRule, ProductEntry, ProductId, ProductList,
    ProductPixelMask, ProductPublication, ProductRole, ProductStorageContract, ProductTerm,
    ProductUnit, ProductValidityRule,
};

pub use transaction::{
    CorrectedDataWrite, MeasurementSetReadAccess, ModelColumnWrite, ObservationReadSet,
    ObservationTransactionCompileError, ObservationTransactionContract,
    ObservationTransactionRequirements, ObservationWriteSet, SelectedVisibilityWriteAccess,
};

pub use aw_pointing::{
    AwChartPixels, AwPointingEpochs, AwPointingError, AwPointingGrouping, aw_grouped_pixels,
};
pub use batch_geometry::{
    BatchGeometry, BatchGeometryError, ChartProjection, GeometryPlan, GeometryTraversal,
};
pub use pointing_law::{PointingLawError, PointingSample, PointingSamples};
pub use uvw_projection::GridFtProjection;
pub use visibility_transform::{
    ContinuumChannelRole, ContinuumChannelUse, ContinuumCovariancePolicy, ContinuumFitRule,
    ContinuumTransformContractError, SequentialContinuumTransform,
};

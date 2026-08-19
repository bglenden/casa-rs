// SPDX-License-Identifier: LGPL-3.0-or-later
#![warn(missing_docs)]
//! Logical, backend-independent imaging problem compilation.

mod compiled_problem;
mod geometry;
mod observation;

pub use compiled_problem::{
    CompileProblemError, CompiledProblem, CompiledProblemId, FiniteValuePolicy, ImagingRequest,
    ImagingRequestVersion, InstrumentResponse, LogicalIdentity, MeasurementEquationContract,
    ModelStateIdentity, NumericPrecision, NumericalStage, NumericsContract, NumericsContractId,
    PolarizationContract, PolarizationCoordinate, ProblemInputIdentities, ProblemSpecification,
    ProductKind, ProductNormalization, ProductRequirements, ReconstructionAlgorithm,
    ReconstructionBasis, ReconstructionContract, ReconstructionControls, ReductionPolicy,
    ReferenceDataKind, RequiredCapability, RestoringBeamPolicy, ScientificContract,
    SpectralContract, SpectralCoupling, SpectralSampling, StageErrorBudget, UvTaper,
    WeightDensityScope, WeightingContract, WeightingScheme, compile,
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

pub use observation::{
    AntennaBaseline, AntennaSelection, ColumnGeneration, CompileObservationError, ConsistencyToken,
    CorrelationProduct, CorrelationSelection, CorrelationType, FlagPolicy, IdSelection,
    IntentSelection, MeasurementSetIdentity, MetadataGeneration, MetadataTableKind, MsColumnKind,
    ObservationConsistencyError, ObservationProvenanceId, ObservationSelection,
    ObservationSnapshot, ObservationSnapshotId, ObservationSnapshotInput, ObservationSource,
    ObservationSourceInput, ObservationSourceProvenance, ObservationSourceState, ObservationState,
    ResolvedIntent, RowSelection, SelectedColumns, SelectedRows, SelectionBound, SourceGenerations,
    SpectralWindowSelection, TimeRange, TimeSelection, UvDistanceRange, UvDistanceUnit,
    UvSelection, VisibilityColumn, WeightColumn, compile_observation,
};

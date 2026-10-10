// SPDX-License-Identifier: LGPL-3.0-or-later

//! The failures of compiling a request against its MeasurementSet and of
//! deploying the compiled run, each one a user can act on.

use std::collections::BTreeSet;
use std::path::PathBuf;

use casa_coordinates::ProjectionType;
use casa_imaging_model::InstrumentModel;
use casa_types::measures::{
    direction::DirectionRef, doppler::DopplerRef, epoch::EpochRef, frequency::FrequencyRef,
};

/// Why a request could not be compiled against its MeasurementSet, or the
/// compiled run could not be deployed.
///
/// Every variant names something the user can change: the request, the
/// MeasurementSet, an input file or the resource policy. A failure no user
/// action can cause is a bug and panics with the invariant it broke.
#[derive(Debug, thiserror::Error)]
pub enum PrepareError {
    /// The MeasurementSet, or one of its subtables or columns, could not be
    /// read.
    #[error(transparent)]
    MeasurementSet(#[from] casa_ms::MsError),
    /// The selected rows are not in ascending MAIN order or name a data
    /// description twice out of sequence.
    #[error(transparent)]
    SelectedRows(#[from] casa_imaging_model::SelectedRowSequenceError),
    /// The selected observation could not be resolved against its
    /// MeasurementSet.
    #[error(transparent)]
    Resolution(#[from] casa_ms::ObservationOwnerError),
    /// The resolved observation could not be compiled.
    #[error(transparent)]
    Observation(#[from] casa_imaging_model::CompileObservationError),
    /// A named, external or attached ephemeris could not be bound.
    #[error(transparent)]
    Ephemeris(#[from] casa_ms::SelectedObservationEphemerisError),
    /// A direction could not be converted between frames.
    #[error(transparent)]
    Measures(#[from] casa_types::measures::MeasureError),
    /// A mask box, mask plan or mask reprojection is invalid.
    #[error(transparent)]
    Mask(#[from] casa_imaging_reconstruction::MaskError),
    /// The image's model-state bounds are invalid (no pixels, planes or
    /// polarizations).
    #[error(transparent)]
    ModelBounds(#[from] casa_imaging_model::ModelContractError),
    /// `normtype` has no direction-dependent image response.
    #[error(transparent)]
    ImageResponse(#[from] casa_imaging_reconstruction::ImageResponseError),
    /// `pblimit` is outside the primary-beam validity contract.
    #[error(transparent)]
    PrimaryBeamValidity(#[from] casa_imaging_model::ProductValidityPolicyError),
    /// The W-projection planes or W range are outside its contract.
    #[error(transparent)]
    WProjection(#[from] casa_imaging_model::WProjectionContractError),
    /// The AW-projection parameters are outside its contract.
    #[error(transparent)]
    AwProjection(#[from] casa_imaging_model::AwProjectionContractError),
    /// `fitspw` and `fitorder` are outside the continuum transform's contract.
    #[error(transparent)]
    ContinuumTransform(#[from] casa_imaging_model::ContinuumTransformContractError),
    /// `psfcutoff` is not in (0, 1).
    #[error("psfcutoff {psfcutoff} must be in (0, 1)")]
    PsfCutoff {
        /// The requested cutoff.
        psfcutoff: f64,
    },
    /// Two image domains share an output name.
    #[error("image domains must have distinct output names")]
    DuplicateOutput,
    /// The selection names no MAIN row.
    #[error("selection resolved to no rows")]
    NoSelectedRows,
    /// The `spw` or `ddid` selection names no data description.
    #[error("selection resolved to no data descriptions")]
    NoSelectedDataDescriptions,
    /// A requested `DATA_DESC_ID` is negative or binds a negative
    /// spectral-window or polarization id.
    #[error("DATA_DESC_ID {ddid} does not bind a spectral window and polarization")]
    InvalidDataDescription {
        /// The data description.
        ddid: i32,
    },
    /// A selected MAIN row holds a negative id.
    #[error("selected MAIN rows hold a negative {column} ({value})")]
    NegativeMainId {
        /// The MAIN column.
        column: &'static str,
        /// The stored value.
        value: i32,
    },
    /// `channel_start` and `channel_count` select no channel, or run past
    /// the window's last channel.
    #[error(
        "channel_start {start} and channel_count {count} do not fit SPW {spw_id}'s {channels} channels"
    )]
    ChannelRange {
        /// The spectral window.
        spw_id: usize,
        /// The first requested channel.
        start: usize,
        /// The requested channel count.
        count: usize,
        /// The window's channel count.
        channels: usize,
    },
    /// The cube's output axis and the `spw` selector share no source channel.
    #[error("cube axis and SPW selector have no common source channels")]
    NoCommonCubeChannels,
    /// A cube's first output channel does not fit the cube axis.
    #[error("cube start channel {channel} exceeds the cube axis range")]
    CubeStartChannel {
        /// The requested channel.
        channel: usize,
    },
    /// The cube's output frequency increment is zero or not finite.
    #[error("cube output frequency increment must be finite and non-zero (got {increment_hz} Hz)")]
    CubeIncrement {
        /// The derived increment.
        increment_hz: f64,
    },
    /// The cube asks for a Doppler convention native imaging has no law for.
    #[error("cube Doppler convention {convention} is not supported")]
    UnsupportedDoppler {
        /// The convention.
        convention: DopplerRef,
    },
    /// A selected spectral window's `MEAS_FREQ_REF` is not a frequency frame.
    #[error("SPW {spw_id} has an unknown frequency frame code {code}")]
    UnknownSourceFrame {
        /// The spectral window.
        spw_id: usize,
        /// The stored `MEAS_FREQ_REF`.
        code: i32,
    },
    /// The selected spectral windows are stored in different frames.
    #[error("selected spectral windows use different source frequency frames")]
    MixedSourceFrames,
    /// Native imaging has no law for this frequency frame.
    #[error("native imaging does not support the frequency frame {reference}")]
    UnsupportedFrequencyFrame {
        /// The frame.
        reference: FrequencyRef,
    },
    /// Native imaging has no law for the MeasurementSet's time scale.
    #[error("native imaging does not support the MeasurementSet epoch reference {reference}")]
    UnsupportedEpochReference {
        /// The epoch reference.
        reference: EpochRef,
    },
    /// A stage that images one spectral window was given several.
    #[error("{stage} requires exactly one selected spectral window; {selected} are selected")]
    OneSpectralWindow {
        /// The stage.
        stage: &'static str,
        /// The number selected.
        selected: usize,
    },
    /// `fitspw` does not name exactly the selected spectral window.
    #[error("continuum fit selector must name exactly the selected spectral window")]
    ContinuumFitWindow,
    /// `fitorder` is beyond the continuum fit's range.
    #[error("continuum fit order {order} exceeds the supported range")]
    ContinuumFitOrder {
        /// The requested order.
        order: usize,
    },
    /// The image centre's field is not among the selected fields.
    #[error("phase-center FIELD_ID {field} is not part of selected fields {selected:?}")]
    PhaseCentreField {
        /// The requested field.
        field: i32,
        /// The selected fields.
        selected: BTreeSet<i32>,
    },
    /// `phasecenter` is not `J2000 lon lat`.
    #[error(
        "phasecenter {text:?} must be 'J2000 lon lat', for example 'J2000 19:59:28.500 +40.44.01.50'"
    )]
    PhaseCentreSyntax {
        /// The given text.
        text: String,
    },
    /// A `phasecenter` angle is neither sexagesimal nor `rad` or `deg`.
    #[error("unsupported phasecenter angle {angle:?}")]
    PhaseCentreAngle {
        /// The given angle.
        angle: String,
    },
    /// A moving source's fields are only partly attached to ephemerides.
    #[error("moving-source selection mixes FIELD rows with and without attached ephemerides")]
    MixedEphemerisFields,
    /// A source-frame cube needs an ephemeris phase centre.
    #[error("source-frame cube imaging requires a moving phase centre")]
    MovingPhaseCentre,
    /// A source-frame cube needs the source's rest frequency.
    #[error("source-frame cube imaging requires REST_FREQUENCY metadata")]
    RestFrequencyMissing,
    /// The selected SOURCE rows give different rest frequencies.
    #[error("selected SOURCE rows disagree on REST_FREQUENCY metadata")]
    RestFrequencyConflict,
    /// The selected observations disagree on telescope or observer.
    #[error(
        "image observation metadata requires consistent telescope and observer labels for \
         selected OBSERVATION_IDs {observation_ids:?}; found {labels:?}"
    )]
    ObservationLabels {
        /// The selected observations.
        observation_ids: BTreeSet<i32>,
        /// The `(telescope, observer)` pairs found.
        labels: BTreeSet<(String, String)>,
    },
    /// The MeasurementSet has neither `CORRECTED_DATA` nor `DATA`.
    #[error("MS has neither CORRECTED_DATA nor DATA")]
    NoVisibilityColumn,
    /// A selected polarization holds a correlation code with no law.
    #[error("unsupported correlation code {code}")]
    UnsupportedCorrelation {
        /// The stored `CORR_TYPE`.
        code: i32,
    },
    /// The ANTENNA table is empty; a primary beam needs dish diameters.
    #[error("a primary-beam response requires ANTENNA dish metadata")]
    NoAntennaDishes,
    /// ALMA's analytic beam needs one dish class.
    #[error(
        "ALMA primary-beam publication requires one homogeneous dish class; row {row} has \
         diameter {diameter_m} m"
    )]
    MixedAlmaDishes {
        /// The ANTENNA row.
        row: usize,
        /// Its dish diameter.
        diameter_m: f64,
    },
    /// The OBSERVATION table names no telescope.
    #[error("standard primary-beam publication requires OBSERVATION telescope metadata")]
    NoTelescope,
    /// No analytic primary beam is installed for these telescopes.
    #[error(
        "standard primary-beam publication has no installed analytic model for telescope set \
         {telescopes:?}"
    )]
    NoAnalyticPrimaryBeam {
        /// The OBSERVATION telescope names.
        telescopes: Vec<String>,
    },
    /// The gridder's instrument response does not cover these telescopes.
    #[error("requested instrument response is unsupported for observation metadata {telescopes:?}")]
    UnsupportedInstrument {
        /// The OBSERVATION telescope names.
        telescopes: Vec<String>,
    },
    /// An antenna's dish is outside the instrument response.
    #[error("the {model:?} response does not cover ANTENNA row {row}'s {diameter_m} m dish")]
    DishOutsideResponse {
        /// The response.
        model: InstrumentModel,
        /// The ANTENNA row.
        row: usize,
        /// Its dish diameter.
        diameter_m: f64,
    },
    /// Native aperture generation models the EVLA only.
    #[error("native aperture generation requires an EVLA observation")]
    NativeAwTelescope,
    /// Native EVLA generation models 25 m dishes only.
    #[error("native EVLA generation requires the explicit homogeneous 25 m dish model")]
    NativeAwDishes,
    /// The EVLA surface file could not be read.
    #[error("cannot read the EVLA surface {}: {source}", path.display())]
    EvlaSurface {
        /// The surface file.
        path: PathBuf,
        /// The file-system failure.
        source: std::io::Error,
    },
    /// The EVLA surface file is not UTF-8 text.
    #[error("the EVLA surface {} is not UTF-8 text", path.display())]
    EvlaSurfaceText {
        /// The surface file.
        path: PathBuf,
    },
    /// The EVLA surface file is larger than its reference-data bound.
    #[error("native EVLA surface exceeds the 1 MiB reference-data bound")]
    NativeAwSurfaceSize,
    /// The native AW request (the surface, grid or frequencies) is invalid.
    #[error(transparent)]
    NativeAwRequest(#[from] casa_imaging_model::NativeAwRequestError),
    /// Native EVLA AW images Stokes I or one circular parallel hand.
    #[error("native EVLA AW currently supports Stokes I or one circular parallel hand")]
    NativeAwPolarization,
    /// The FEED table's `RECEPTOR_ANGLE` is not `Float64`.
    #[error("native EVLA feed receptor angles require Float64 metadata")]
    FeedAngleType,
    /// A FEED row has no finite first receptor angle.
    #[error("native EVLA feed has no finite receptor-zero angle")]
    FeedAngleNotFinite,
    /// The applicable FEED rows give different receptor angles.
    #[error("native EVLA receptor-zero angle is ambiguous for the selected epoch/SPW")]
    FeedAngleAmbiguous,
    /// No FEED row applies to antenna zero at the selected epoch and window.
    #[error("native EVLA request has no applicable antenna-zero feed metadata")]
    NoApplicableFeed,
    /// Native AW reads the parallactic angle of an unflagged
    /// cross-correlation row, and the selection has none.
    #[error("native AW has no unflagged cross-correlation row")]
    NoCrossCorrelationRow,
    /// `cf_resident_mb` is larger than the address space.
    #[error("cf_resident_mb {megabytes} exceeds addressable memory")]
    CfResidentSize {
        /// The requested megabytes.
        megabytes: usize,
    },
    /// A reuse-only native AW cache lacks cells the request names.
    #[error("native AW cache reuse found {present} of the {expected} cells the request names")]
    AwCacheIncomplete {
        /// Cells present.
        present: usize,
        /// Cells the request names.
        expected: usize,
    },
    /// The native AW cache could not be cleared or its cells generated.
    #[error(transparent)]
    AwCatalog(#[from] casa_imaging_operator::AwCatalogError),
    /// The image is too large to address its model samples.
    #[error("the image's model samples exceed addressable memory")]
    ImageSize,
    /// The selected spectral envelope leaves no room in the row budget.
    #[error("selected spectral envelope exhausts the row traversal budget")]
    SpectralEnvelopeBudget,
    /// A mask image could not be read.
    #[error(transparent)]
    MaskImage(#[from] casa_images::ImageError),
    /// A reconstruction mask image is complex-valued.
    #[error("reconstruction masks require a real CASA image")]
    ComplexMaskImage,
    /// A reconstruction mask image is not one direction plane.
    #[error(
        "reconstruction mask must contain one two-dimensional direction plane; shape {shape:?}"
    )]
    MaskImageShape {
        /// The image shape.
        shape: Vec<usize>,
    },
    /// A reconstruction mask image has no direction coordinate.
    #[error("mask image has no direction coordinate")]
    MaskImageDirection,
    /// Native mask reprojection supports SIN only.
    #[error("native mask reprojection currently requires SIN coordinates, not {projection:?}")]
    MaskImageProjection {
        /// The mask's projection.
        projection: ProjectionType,
    },
    /// The mask's direction frame has no native law.
    #[error("mask direction frame {frame} is not supported by native imaging")]
    MaskImageFrame {
        /// The mask's frame.
        frame: DirectionRef,
    },
    /// The outlier file could not be read or describes no valid domain.
    #[error("outlier file {}: {problem}", path.display())]
    OutlierFile {
        /// The outlier file.
        path: PathBuf,
        /// What is wrong with it.
        problem: OutlierProblem,
    },
    /// An output directory could not be created.
    #[error("cannot create output directory {}: {source}", path.display())]
    OutputDirectory {
        /// The directory.
        path: PathBuf,
        /// The file-system failure.
        source: std::io::Error,
    },
}

/// What is wrong with an outlier file.
#[derive(Debug, thiserror::Error)]
pub enum OutlierProblem {
    /// The file could not be read.
    #[error("cannot read: {0}")]
    Read(#[source] std::io::Error),
    /// The file defines no image domain.
    #[error("did not define any image domains")]
    Empty,
    /// A line is not one `parameter=value` pair.
    #[error("line {line} must contain one parameter=value pair")]
    NotAPair {
        /// The 1-based line.
        line: usize,
    },
    /// A line holds more than one `=`.
    #[error("line {line} contains more than one '='")]
    SeveralEquals {
        /// The 1-based line.
        line: usize,
    },
    /// A line repeats a field of its image.
    #[error("line {line} repeats field {key:?}")]
    RepeatedField {
        /// The 1-based line.
        line: usize,
        /// The field.
        key: String,
    },
    /// An image sets fields native imaging does not support.
    #[error("outlier image {ordinal} contains unsupported field(s): {}", fields.join(", "))]
    UnsupportedFields {
        /// The image, counted from zero.
        ordinal: usize,
        /// The fields.
        fields: Vec<String>,
    },
    /// An image lacks a required field.
    #[error("outlier image {ordinal} is missing required {key}")]
    MissingField {
        /// The image, counted from zero.
        ordinal: usize,
        /// The field.
        key: &'static str,
    },
    /// A field takes a value outside the installed multi-domain slice.
    #[error("outlier field {key}={value:?} is outside the installed multi-domain slice")]
    OutsideSlice {
        /// The field.
        key: &'static str,
        /// Its value.
        value: String,
    },
    /// `imsize` is not a positive square scalar or pair.
    #[error("outlier imsize must be a positive square scalar or pair")]
    ImageSize,
    /// `cell` is not one positive arcsec value or an equal pair.
    #[error("outlier cell must be one positive arcsec value or an equal pair")]
    Cell,
    /// A `cell` value is not a number.
    #[error("invalid outlier cell {text:?}")]
    CellValue {
        /// The given value.
        text: String,
    },
    /// `mask` is not `circle[[xpix,ypix],rpix]` in finite pixels.
    #[error("outlier mask must use circle[[xpix,ypix],rpix] with finite pix values")]
    MaskSyntax,
    /// The mask circle's centre is outside its image or its radius negative.
    #[error("outlier circle mask exceeds its image domain")]
    MaskOutside,
}

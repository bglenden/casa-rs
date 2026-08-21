// SPDX-License-Identifier: LGPL-3.0-or-later

//! Immutable celestial, UVW, image-domain, and spectral coordinate laws.

use std::{cmp::Ordering, collections::BTreeSet, f64::consts::TAU, fmt};

use thiserror::Error;

use crate::compiled_problem::{
    CanonicalEncoder, LogicalIdentity, ProblemInputIdentities, ReferenceDataKind,
};

const COMPILED_GEOMETRY_IDENTITY_DOMAIN: &[u8] = b"casa-rs-compiled-geometry";
const COMPILED_GEOMETRY_IDENTITY_VERSION: u32 = 1;

/// Stable compiler-derived identity of immutable compiled geometry.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct CompiledGeometryId(LogicalIdentity);

impl CompiledGeometryId {
    /// Identity schema version used by the canonical encoder.
    pub const SCHEMA_VERSION: u32 = COMPILED_GEOMETRY_IDENTITY_VERSION;

    /// Return the exact SHA-256 digest.
    #[must_use]
    pub const fn as_bytes(self) -> [u8; 32] {
        self.0.as_bytes()
    }

    #[cfg(test)]
    pub(crate) const fn from_sha256_for_test(digest: [u8; 32]) -> Self {
        Self(LogicalIdentity::from_sha256(digest))
    }
}

impl fmt::Debug for CompiledGeometryId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("CompiledGeometryId(")?;
        write_hex(formatter, &self.as_bytes())?;
        formatter.write_str(")")
    }
}

impl fmt::Display for CompiledGeometryId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write_hex(formatter, &self.as_bytes())
    }
}

/// Celestial reference frame attached to a sky direction.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum DirectionFrame {
    /// International Celestial Reference System.
    Icrs,
    /// Mean equatorial coordinates at J2000.
    J2000,
    /// Mean equatorial coordinates at B1950.
    B1950,
    /// Galactic longitude and latitude.
    Galactic,
}

/// A longitude/latitude direction in one explicit celestial frame.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct SkyDirection {
    frame: DirectionFrame,
    longitude_rad: f64,
    latitude_rad: f64,
}

impl SkyDirection {
    /// Construct an uncompiled direction in radians.
    #[must_use]
    pub const fn new(frame: DirectionFrame, longitude_rad: f64, latitude_rad: f64) -> Self {
        Self {
            frame,
            longitude_rad,
            latitude_rad,
        }
    }

    /// Return the direction frame.
    #[must_use]
    pub const fn frame(self) -> DirectionFrame {
        self.frame
    }

    /// Return canonical longitude in radians after compilation.
    #[must_use]
    pub const fn longitude_rad(self) -> f64 {
        self.longitude_rad
    }

    /// Return latitude in radians.
    #[must_use]
    pub const fn latitude_rad(self) -> f64 {
        self.latitude_rad
    }

    fn canonicalize(mut self) -> Result<Self, CompileGeometryError> {
        if !(self.longitude_rad.is_finite()
            && self.latitude_rad.is_finite()
            && (-std::f64::consts::FRAC_PI_2..=std::f64::consts::FRAC_PI_2)
                .contains(&self.latitude_rad))
        {
            return Err(CompileGeometryError::InvalidSkyDirection);
        }
        self.longitude_rad = canonical_longitude(self.longitude_rad);
        self.latitude_rad = canonical_zero(self.latitude_rad);
        Ok(self)
    }
}

/// Celestial map projection represented by the direction-coordinate law.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Projection {
    /// Orthographic sine projection used by the current imaging surface.
    Sin,
}

/// Exact two-dimensional direction WCS requested for an image domain.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct DirectionCoordinateSpec {
    projection: Projection,
    reference_direction: SkyDirection,
    reference_pixel: [f64; 2],
    increment_rad: [f64; 2],
    pc: [[f64; 2]; 2],
    pole_deg: [f64; 2],
}

impl DirectionCoordinateSpec {
    /// Construct an exact FITS-WCS-style direction law.
    #[must_use]
    pub const fn new(
        projection: Projection,
        reference_direction: SkyDirection,
        reference_pixel: [f64; 2],
        increment_rad: [f64; 2],
        pc: [[f64; 2]; 2],
        pole_deg: [f64; 2],
    ) -> Self {
        Self {
            projection,
            reference_direction,
            reference_pixel,
            increment_rad,
            pc,
            pole_deg,
        }
    }

    /// Return the projection.
    #[must_use]
    pub const fn projection(self) -> Projection {
        self.projection
    }

    /// Return the explicit framed reference direction.
    #[must_use]
    pub const fn reference_direction(self) -> SkyDirection {
        self.reference_direction
    }

    /// Return the zero-based reference pixel.
    #[must_use]
    pub const fn reference_pixel(self) -> [f64; 2] {
        self.reference_pixel
    }

    /// Return signed radians per pixel for the two direction axes.
    #[must_use]
    pub const fn increment_rad(self) -> [f64; 2] {
        self.increment_rad
    }

    /// Return the exact WCS PC matrix.
    #[must_use]
    pub const fn pc(self) -> [[f64; 2]; 2] {
        self.pc
    }

    /// Return longitude and latitude pole metadata in degrees.
    #[must_use]
    pub const fn pole_deg(self) -> [f64; 2] {
        self.pole_deg
    }

    /// Replace the reference pixel in an uncompiled specification.
    #[must_use]
    pub const fn with_reference_pixel(mut self, reference_pixel: [f64; 2]) -> Self {
        self.reference_pixel = reference_pixel;
        self
    }

    fn canonicalize(mut self) -> Result<Self, CompileGeometryError> {
        self.reference_direction = self.reference_direction.canonicalize()?;
        if self.reference_pixel.iter().any(|value| !value.is_finite()) {
            return Err(CompileGeometryError::InvalidDirectionWcs);
        }
        if self
            .increment_rad
            .iter()
            .any(|value| !value.is_finite() || *value == 0.0)
            || self.pc.iter().flatten().any(|value| !value.is_finite())
            || self.pole_deg.iter().any(|value| !value.is_finite())
            || !(-90.0..=90.0).contains(&self.pole_deg[1])
        {
            return Err(CompileGeometryError::InvalidDirectionWcs);
        }
        let determinant = self.pc[0][0] * self.pc[1][1] - self.pc[0][1] * self.pc[1][0];
        if !determinant.is_finite() || determinant == 0.0 {
            return Err(CompileGeometryError::SingularDirectionMatrix);
        }
        canonicalize_f64_slice(&mut self.reference_pixel);
        canonicalize_f64_slice(&mut self.increment_rad);
        canonicalize_f64_slice(&mut self.pc[0]);
        canonicalize_f64_slice(&mut self.pc[1]);
        canonicalize_f64_slice(&mut self.pole_deg);
        self.pole_deg[0] = canonical_degrees(self.pole_deg[0]);
        Ok(self)
    }
}

/// Logical placement of each image-array axis.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum ImageAxis {
    /// Celestial longitude axis.
    DirectionLongitude,
    /// Celestial latitude axis.
    DirectionLatitude,
    /// Polarization axis; its values are reconstruction-owned.
    Polarization,
    /// Spectral axis.
    Spectral,
}

/// Explicit placement of the four logical image axes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AxisOrder {
    positions: [ImageAxis; 4],
}

impl AxisOrder {
    /// Construct an uncompiled axis placement.
    #[must_use]
    pub const fn new(positions: [ImageAxis; 4]) -> Self {
        Self { positions }
    }

    /// Return axes in storage order.
    #[must_use]
    pub const fn positions(&self) -> &[ImageAxis; 4] {
        &self.positions
    }

    fn validate(&self) -> Result<(), CompileGeometryError> {
        let unique = self.positions.into_iter().collect::<BTreeSet<_>>();
        if unique.len() != self.positions.len() {
            return Err(CompileGeometryError::InvalidAxisOrder);
        }
        Ok(())
    }
}

/// Two-dimensional pixel shape of one user-visible image domain.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ImageShape {
    width: usize,
    height: usize,
}

impl ImageShape {
    /// Construct an uncompiled image shape.
    #[must_use]
    pub const fn new(width: usize, height: usize) -> Self {
        Self { width, height }
    }

    /// Return `[width, height]`.
    #[must_use]
    pub const fn pixels(self) -> [usize; 2] {
        [self.width, self.height]
    }
}

/// User-visible purpose of one image domain.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub enum ImageDomainRole {
    /// Sole primary reconstructed image domain.
    Main,
    /// Named user-visible outlier field.
    Outlier(String),
}

/// User-visible facet subdivision of an image domain.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FacetLayout {
    /// The domain is not user-faceted.
    Single,
    /// Exact regular column/row subdivision.
    Regular {
        /// Number of columns.
        columns: usize,
        /// Number of rows.
        rows: usize,
    },
}

/// Uncompiled user-visible image-domain specification.
#[derive(Debug, Clone, PartialEq)]
pub struct ImageDomainSpec {
    role: ImageDomainRole,
    shape: ImageShape,
    direction: DirectionCoordinateSpec,
    facets: FacetLayout,
    axes: AxisOrder,
}

impl ImageDomainSpec {
    /// Construct an image-domain specification.
    #[must_use]
    pub const fn new(
        role: ImageDomainRole,
        shape: ImageShape,
        direction: DirectionCoordinateSpec,
        facets: FacetLayout,
        axes: AxisOrder,
    ) -> Self {
        Self {
            role,
            shape,
            direction,
            facets,
            axes,
        }
    }

    /// Return the role.
    #[must_use]
    pub const fn role(&self) -> &ImageDomainRole {
        &self.role
    }

    /// Return the direction-coordinate specification.
    #[must_use]
    pub const fn direction(&self) -> &DirectionCoordinateSpec {
        &self.direction
    }

    /// Return the uncompiled image shape.
    #[must_use]
    pub const fn shape(&self) -> ImageShape {
        self.shape
    }

    /// Return the uncompiled facet layout.
    #[must_use]
    pub const fn facets(&self) -> FacetLayout {
        self.facets
    }

    /// Return the uncompiled image-axis placement.
    #[must_use]
    pub const fn axes(&self) -> &AxisOrder {
        &self.axes
    }

    /// Replace the role in an uncompiled specification.
    #[must_use]
    pub fn with_role(mut self, role: ImageDomainRole) -> Self {
        self.role = role;
        self
    }

    /// Replace the direction law in an uncompiled specification.
    #[must_use]
    pub const fn with_direction(mut self, direction: DirectionCoordinateSpec) -> Self {
        self.direction = direction;
        self
    }

    /// Replace the image shape in an uncompiled specification.
    #[must_use]
    pub const fn with_shape(mut self, shape: ImageShape) -> Self {
        self.shape = shape;
        self
    }

    /// Replace facet subdivision in an uncompiled specification.
    #[must_use]
    pub const fn with_facets(mut self, facets: FacetLayout) -> Self {
        self.facets = facets;
        self
    }

    /// Replace image-axis placement in an uncompiled specification.
    #[must_use]
    pub const fn with_axes(mut self, axes: AxisOrder) -> Self {
        self.axes = axes;
        self
    }
}

/// Missing-row policy for observation pointing metadata.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MissingPointingPolicy {
    /// Reject missing pointing metadata.
    Reject,
    /// Explicitly substitute the phase-tracking centre.
    UsePhaseTrackingCentre,
}

/// MeasurementSet POINTING column selected as the direction source.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PointingDirectionColumn {
    /// The POINTING `DIRECTION` column.
    Direction,
    /// The POINTING `TARGET` column.
    Target,
}

/// Scientific meaning assigned to the selected POINTING direction column.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PointingDirectionSemantic {
    /// Per-antenna boresight direction from `DIRECTION`.
    AntennaBoresight,
    /// Intended tracking target direction from `TARGET`.
    TrackingTarget,
}

/// Visibility timestamp used to sample the POINTING time series.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PointingTimeSampling {
    /// Sample at the visibility row's `TIME` value.
    VisibilityTime,
    /// Sample at the visibility row's `TIME_CENTROID` value.
    VisibilityTimeCentroid,
}

/// Interpolation between bracketing POINTING rows.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PointingInterpolation {
    /// Select the nearest POINTING row in time.
    Nearest,
    /// Interpolate on the shorter great-circle arc between bracketing rows.
    GreatCircleShortestArc,
}

/// Policy outside the time span covered by POINTING rows.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PointingExtrapolation {
    /// Reject samples outside the covered time span.
    Reject,
    /// Hold the nearest endpoint row outside the covered time span.
    HoldNearest,
}

/// Complete declarative law for selecting observation POINTING directions.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ObservationPointingLaw {
    direction_column: PointingDirectionColumn,
    direction_semantic: PointingDirectionSemantic,
    time_sampling: PointingTimeSampling,
    interpolation: PointingInterpolation,
    extrapolation: PointingExtrapolation,
    missing: MissingPointingPolicy,
}

impl ObservationPointingLaw {
    /// Construct an explicit observation-pointing law.
    #[must_use]
    pub const fn new(
        direction_column: PointingDirectionColumn,
        direction_semantic: PointingDirectionSemantic,
        time_sampling: PointingTimeSampling,
        interpolation: PointingInterpolation,
        extrapolation: PointingExtrapolation,
        missing: MissingPointingPolicy,
    ) -> Self {
        Self {
            direction_column,
            direction_semantic,
            time_sampling,
            interpolation,
            extrapolation,
            missing,
        }
    }

    /// Return the selected POINTING direction column.
    #[must_use]
    pub const fn direction_column(self) -> PointingDirectionColumn {
        self.direction_column
    }

    /// Return the meaning assigned to the selected direction column.
    #[must_use]
    pub const fn direction_semantic(self) -> PointingDirectionSemantic {
        self.direction_semantic
    }

    /// Return the visibility timestamp used to sample POINTING rows.
    #[must_use]
    pub const fn time_sampling(self) -> PointingTimeSampling {
        self.time_sampling
    }

    /// Return the interpolation law between POINTING rows.
    #[must_use]
    pub const fn interpolation(self) -> PointingInterpolation {
        self.interpolation
    }

    /// Return the extrapolation law outside POINTING row coverage.
    #[must_use]
    pub const fn extrapolation(self) -> PointingExtrapolation {
        self.extrapolation
    }

    /// Return the policy for an antenna or interval without POINTING rows.
    #[must_use]
    pub const fn missing(self) -> MissingPointingPolicy {
        self.missing
    }

    /// Replace the POINTING column and its scientific meaning together.
    #[must_use]
    pub const fn with_direction(
        mut self,
        direction_column: PointingDirectionColumn,
        direction_semantic: PointingDirectionSemantic,
    ) -> Self {
        self.direction_column = direction_column;
        self.direction_semantic = direction_semantic;
        self
    }

    /// Replace the visibility timestamp used to sample POINTING rows.
    #[must_use]
    pub const fn with_time_sampling(mut self, time_sampling: PointingTimeSampling) -> Self {
        self.time_sampling = time_sampling;
        self
    }

    /// Replace interpolation between POINTING rows.
    #[must_use]
    pub const fn with_interpolation(mut self, interpolation: PointingInterpolation) -> Self {
        self.interpolation = interpolation;
        self
    }

    /// Replace extrapolation outside POINTING row coverage.
    #[must_use]
    pub const fn with_extrapolation(mut self, extrapolation: PointingExtrapolation) -> Self {
        self.extrapolation = extrapolation;
        self
    }

    /// Replace the missing-row policy.
    #[must_use]
    pub const fn with_missing(mut self, missing: MissingPointingPolicy) -> Self {
        self.missing = missing;
        self
    }
}

/// Law selecting the phase-tracking centre.
#[derive(Debug, Clone, PartialEq)]
pub enum PhaseCentreLaw {
    /// Use the selected observation field phase centre.
    Observation,
    /// Use one fixed framed direction.
    Fixed(SkyDirection),
    /// Use a named moving target from bound ephemeris data.
    Ephemeris(String),
}

/// Law selecting the delay centre.
#[derive(Debug, Clone, PartialEq)]
pub enum DelayCentreLaw {
    /// Use the compiled phase-tracking centre.
    PhaseTrackingCentre,
    /// Use the observation delay centre.
    Observation,
    /// Use one fixed framed direction.
    Fixed(SkyDirection),
}

/// Law selecting antenna pointing directions.
#[derive(Debug, Clone, PartialEq)]
pub enum PointingCentreLaw {
    /// Use the phase-tracking centre for every sample.
    PhaseTrackingCentre,
    /// Use observation POINTING rows according to a complete sampling law.
    Observation(ObservationPointingLaw),
    /// Use one fixed framed direction.
    Fixed(SkyDirection),
}

/// Phase, delay, and pointing centre laws, without evaluated sample arrays.
#[derive(Debug, Clone, PartialEq)]
pub struct CentreLaws {
    phase_tracking: PhaseCentreLaw,
    delay: DelayCentreLaw,
    pointing: PointingCentreLaw,
}

impl CentreLaws {
    /// Construct all centre-selection laws explicitly.
    #[must_use]
    pub const fn new(
        phase_tracking: PhaseCentreLaw,
        delay: DelayCentreLaw,
        pointing: PointingCentreLaw,
    ) -> Self {
        Self {
            phase_tracking,
            delay,
            pointing,
        }
    }

    /// Return the phase-tracking centre law.
    #[must_use]
    pub const fn phase_tracking(&self) -> &PhaseCentreLaw {
        &self.phase_tracking
    }

    /// Return the delay-centre law.
    #[must_use]
    pub const fn delay(&self) -> &DelayCentreLaw {
        &self.delay
    }

    /// Return the pointing-centre law.
    #[must_use]
    pub const fn pointing(&self) -> &PointingCentreLaw {
        &self.pointing
    }
}

/// UVW coordinate convention declared by compiled geometry.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UvwCoordinateLaw {
    /// MeasurementSet MAIN UVW in metres: `u` east, `v` north, and `w` toward
    /// the phase-tracking centre, with prediction phase `exp(-i 2πν delay)`.
    PhaseTrackingCentre,
}

/// Unit of UVW coordinates read from an observation snapshot.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UvwUnit {
    /// Metres, as stored in MeasurementSet MAIN.
    Metres,
}

/// Orientation of the UVW basis.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UvwAxes {
    /// `u` east, `v` north, and `w` toward the phase-tracking centre.
    EastNorthPhaseTrackingCentre,
}

/// Visibility phase convention paired with the UVW basis.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VisibilityPhaseConvention {
    /// Prediction uses `exp(-i 2π frequency * geometric_delay)`.
    NegativeTwoPiFrequencyDelay,
}

impl UvwCoordinateLaw {
    /// Return the UVW unit.
    #[must_use]
    pub const fn unit(self) -> UvwUnit {
        match self {
            Self::PhaseTrackingCentre => UvwUnit::Metres,
        }
    }

    /// Return the UVW axis orientation.
    #[must_use]
    pub const fn axes(self) -> UvwAxes {
        match self {
            Self::PhaseTrackingCentre => UvwAxes::EastNorthPhaseTrackingCentre,
        }
    }

    /// Return the prediction phase convention.
    #[must_use]
    pub const fn prediction_phase(self) -> VisibilityPhaseConvention {
        match self {
            Self::PhaseTrackingCentre => VisibilityPhaseConvention::NegativeTwoPiFrequencyDelay,
        }
    }
}

/// Spectral frequency reference frame.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FrequencyFrame {
    /// Observer topocentric frame.
    Topocentric,
    /// Solar-system barycentric frame.
    Barycentric,
    /// Kinematic Local Standard of Rest.
    Lsrk,
}

/// Time scale attached to a spectral-frame epoch.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TimeScale {
    /// Coordinated Universal Time.
    Utc,
    /// International Atomic Time.
    Tai,
    /// Terrestrial Time.
    Tt,
    /// Barycentric Dynamical Time.
    Tdb,
}

/// Exact epoch used to anchor a spectral frame transformation.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Epoch {
    mjd_days: f64,
    scale: TimeScale,
}

impl Epoch {
    /// Construct an epoch as Modified Julian Date in an explicit time scale.
    #[must_use]
    pub const fn new(mjd_days: f64, scale: TimeScale) -> Self {
        Self { mjd_days, scale }
    }

    /// Return Modified Julian Date in days.
    #[must_use]
    pub const fn mjd_days(self) -> f64 {
        self.mjd_days
    }

    /// Return the time scale.
    #[must_use]
    pub const fn scale(self) -> TimeScale {
        self.scale
    }
}

/// Observatory position in the ITRF Cartesian frame, in metres.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct ItrfPosition {
    metres: [f64; 3],
}

/// Context required to convert between distinct spectral frequency frames.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum SpectralFrameAnchor {
    /// Source and output frames are identical, so conversion is not applicable.
    NotApplicable,
    /// Exact Measures-frame context for a requested conversion.
    Conversion {
        /// Conversion epoch and time scale.
        epoch: Epoch,
        /// Framed conversion direction.
        direction: SkyDirection,
        /// Observatory position in ITRF metres.
        observatory_position: ItrfPosition,
    },
}

impl ItrfPosition {
    /// Construct an ITRF Cartesian position in metres.
    #[must_use]
    pub const fn new(x_metres: f64, y_metres: f64, z_metres: f64) -> Self {
        Self {
            metres: [x_metres, y_metres, z_metres],
        }
    }

    /// Return ITRF Cartesian metres.
    #[must_use]
    pub const fn metres(self) -> [f64; 3] {
        self.metres
    }
}

/// Exact output spectral WCS law, not an evaluated sampling transform.
#[derive(Debug, Clone, PartialEq)]
pub enum SpectralWcs {
    /// Linear frequency coordinate.
    Linear {
        /// Number of output coordinate samples.
        channels: usize,
        /// Zero-based reference pixel.
        reference_pixel: f64,
        /// Reference frequency in hertz.
        reference_frequency_hz: f64,
        /// Signed increment in hertz per pixel.
        increment_hz: f64,
    },
    /// Explicit nonlinear spectral-axis coordinates and channel edges.
    Tabular {
        /// Strictly monotonic N channel-centre frequencies in hertz.
        channel_centres_hz: Vec<f64>,
        /// Strictly monotonic N+1 channel boundaries in hertz.
        channel_boundaries_hz: Vec<f64>,
    },
}

/// Rest-frequency semantics of the output spectral coordinate.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum RestFrequency {
    /// Pure continuum coordinate with no line rest frequency.
    NotApplicable,
    /// Spectral-line rest frequency in hertz.
    Line {
        /// Exact positive rest frequency.
        hertz: f64,
    },
}

/// Velocity convention paired with line rest-frequency metadata.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DopplerConvention {
    /// Continuum coordinate; velocity conversion is not applicable.
    NotApplicable,
    /// Radio velocity convention.
    Radio,
    /// Optical velocity convention.
    Optical,
    /// Relativistic velocity convention.
    Relativistic,
}

/// Uncompiled deterministic spectral coordinate and transform specification.
#[derive(Debug, Clone, PartialEq)]
pub struct SpectralCoordinateSpec {
    source_frame: FrequencyFrame,
    output_frame: FrequencyFrame,
    anchor: SpectralFrameAnchor,
    wcs: SpectralWcs,
    rest_frequency: RestFrequency,
    doppler_convention: DopplerConvention,
}

impl SpectralCoordinateSpec {
    /// Construct a fully anchored spectral coordinate law.
    #[must_use]
    pub const fn new(
        source_frame: FrequencyFrame,
        output_frame: FrequencyFrame,
        anchor: SpectralFrameAnchor,
        wcs: SpectralWcs,
        rest_frequency: RestFrequency,
        doppler_convention: DopplerConvention,
    ) -> Self {
        Self {
            source_frame,
            output_frame,
            anchor,
            wcs,
            rest_frequency,
            doppler_convention,
        }
    }

    /// Return the source frame.
    #[must_use]
    pub const fn source_frame(&self) -> FrequencyFrame {
        self.source_frame
    }

    /// Return the output frame.
    #[must_use]
    pub const fn output_frame(&self) -> FrequencyFrame {
        self.output_frame
    }

    /// Return the explicit transform-anchor law.
    #[must_use]
    pub const fn anchor(&self) -> SpectralFrameAnchor {
        self.anchor
    }

    /// Return the exact output WCS law.
    #[must_use]
    pub const fn wcs(&self) -> &SpectralWcs {
        &self.wcs
    }

    /// Return the exact number of output spectral channels.
    #[must_use]
    pub fn output_channels(&self) -> usize {
        match &self.wcs {
            SpectralWcs::Linear { channels, .. } => *channels,
            SpectralWcs::Tabular {
                channel_centres_hz, ..
            } => channel_centres_hz.len(),
        }
    }

    /// Return one of the N exact output channel-centre frequencies in hertz.
    #[must_use]
    pub fn channel_centre_hz(&self, channel: usize) -> Option<f64> {
        match &self.wcs {
            SpectralWcs::Linear {
                channels,
                reference_pixel,
                reference_frequency_hz,
                increment_hz,
            } if channel < *channels => {
                Some(reference_frequency_hz + (channel as f64 - reference_pixel) * increment_hz)
            }
            SpectralWcs::Linear { .. } => None,
            SpectralWcs::Tabular {
                channel_centres_hz, ..
            } => channel_centres_hz.get(channel).copied(),
        }
    }

    /// Return one of the N+1 exact output channel boundaries in hertz.
    ///
    /// Linear axes derive each boundary directly from their WCS law. Tabular
    /// axes retain every supplied boundary. Compilation guarantees that all
    /// in-range results are positive, finite, and strictly monotonic.
    #[must_use]
    pub fn channel_boundary_hz(&self, boundary: usize) -> Option<f64> {
        match &self.wcs {
            SpectralWcs::Linear {
                channels,
                reference_pixel,
                reference_frequency_hz,
                increment_hz,
            } if boundary <= *channels => Some(
                reference_frequency_hz + (boundary as f64 - 0.5 - reference_pixel) * increment_hz,
            ),
            SpectralWcs::Linear { .. } => None,
            SpectralWcs::Tabular {
                channel_boundaries_hz,
                ..
            } => channel_boundaries_hz.get(boundary).copied(),
        }
    }

    /// Return rest-frequency semantics.
    #[must_use]
    pub const fn rest_frequency(&self) -> RestFrequency {
        self.rest_frequency
    }

    /// Return the velocity convention.
    #[must_use]
    pub const fn doppler_convention(&self) -> DopplerConvention {
        self.doppler_convention
    }

    /// Replace the output frame in an uncompiled specification.
    #[must_use]
    pub const fn with_output_frame(mut self, output_frame: FrequencyFrame) -> Self {
        self.output_frame = output_frame;
        self
    }

    /// Replace the transform-anchor law in an uncompiled specification.
    #[must_use]
    pub const fn with_anchor(mut self, anchor: SpectralFrameAnchor) -> Self {
        self.anchor = anchor;
        self
    }

    /// Replace rest-frequency semantics in an uncompiled specification.
    #[must_use]
    pub const fn with_rest_frequency(mut self, rest_frequency: RestFrequency) -> Self {
        self.rest_frequency = rest_frequency;
        self
    }

    /// Replace the velocity convention in an uncompiled specification.
    #[must_use]
    pub const fn with_doppler_convention(mut self, doppler_convention: DopplerConvention) -> Self {
        self.doppler_convention = doppler_convention;
        self
    }

    /// Replace the output WCS in an uncompiled specification.
    #[must_use]
    pub fn with_wcs(mut self, wcs: SpectralWcs) -> Self {
        self.wcs = wcs;
        self
    }
}

/// Complete uncompiled geometry supplied to the sole problem compiler.
#[derive(Debug, Clone, PartialEq)]
pub struct GeometryInput {
    domains: Vec<ImageDomainSpec>,
    centres: CentreLaws,
    uvw: UvwCoordinateLaw,
    spectral: SpectralCoordinateSpec,
}

impl GeometryInput {
    /// Construct geometry from coordinate laws, never evaluated sample arrays.
    #[must_use]
    pub const fn new(
        domains: Vec<ImageDomainSpec>,
        centres: CentreLaws,
        uvw: UvwCoordinateLaw,
        spectral: SpectralCoordinateSpec,
    ) -> Self {
        Self {
            domains,
            centres,
            uvw,
            spectral,
        }
    }

    /// Return uncompiled image domains.
    #[must_use]
    pub fn domains(&self) -> &[ImageDomainSpec] {
        &self.domains
    }

    /// Return the uncompiled spectral coordinate law.
    #[must_use]
    pub const fn spectral(&self) -> &SpectralCoordinateSpec {
        &self.spectral
    }

    /// Return uncompiled phase, delay, and pointing centre laws.
    #[must_use]
    pub const fn centres(&self) -> &CentreLaws {
        &self.centres
    }

    /// Replace image domains in an uncompiled specification.
    #[must_use]
    pub fn with_domains(mut self, domains: Vec<ImageDomainSpec>) -> Self {
        self.domains = domains;
        self
    }

    /// Replace the spectral law in an uncompiled specification.
    #[must_use]
    pub fn with_spectral(mut self, spectral: SpectralCoordinateSpec) -> Self {
        self.spectral = spectral;
        self
    }

    /// Replace centre-selection laws in an uncompiled specification.
    #[must_use]
    pub fn with_centres(mut self, centres: CentreLaws) -> Self {
        self.centres = centres;
        self
    }
}

/// Exact rectangular window of one user-visible facet.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FacetWindow {
    origin: [usize; 2],
    end_exclusive: [usize; 2],
}

impl FacetWindow {
    /// Return the inclusive `[x, y]` origin.
    #[must_use]
    pub const fn origin(self) -> [usize; 2] {
        self.origin
    }

    /// Return the exclusive `[x, y]` end.
    #[must_use]
    pub const fn end_exclusive(self) -> [usize; 2] {
        self.end_exclusive
    }
}

/// One canonical compiled image domain.
#[derive(Debug, Clone, PartialEq)]
pub struct CompiledImageDomain {
    role: ImageDomainRole,
    shape: ImageShape,
    direction: DirectionCoordinateSpec,
    facets: Box<[FacetWindow]>,
    axes: AxisOrder,
}

impl CompiledImageDomain {
    /// Return the user-visible domain role.
    #[must_use]
    pub const fn role(&self) -> &ImageDomainRole {
        &self.role
    }

    /// Return the image shape.
    #[must_use]
    pub const fn shape(&self) -> ImageShape {
        self.shape
    }

    /// Return the exact direction-coordinate law.
    #[must_use]
    pub const fn direction(&self) -> DirectionCoordinateSpec {
        self.direction
    }

    /// Return exact user-visible facet windows.
    #[must_use]
    pub const fn facets(&self) -> &[FacetWindow] {
        &self.facets
    }

    /// Return explicit image-axis placement.
    #[must_use]
    pub const fn axes(&self) -> &AxisOrder {
        &self.axes
    }
}

/// Immutable compiler-owned geometry accepted by planning and execution.
#[derive(Debug, Clone, PartialEq)]
pub struct CompiledGeometry {
    geometry_id: CompiledGeometryId,
    domains: Box<[CompiledImageDomain]>,
    centres: CentreLaws,
    uvw: UvwCoordinateLaw,
    spectral: SpectralCoordinateSpec,
    measures_reference: Option<LogicalIdentity>,
    ephemeris_reference: Option<LogicalIdentity>,
}

impl CompiledGeometry {
    /// Return the compiler-derived canonical identity.
    #[must_use]
    pub const fn geometry_id(&self) -> CompiledGeometryId {
        self.geometry_id
    }

    /// Return canonical domains: main first, then named outliers.
    #[must_use]
    pub const fn domains(&self) -> &[CompiledImageDomain] {
        &self.domains
    }

    /// Return phase, delay, and pointing centre laws.
    #[must_use]
    pub const fn centres(&self) -> &CentreLaws {
        &self.centres
    }

    /// Return the UVW convention.
    #[must_use]
    pub const fn uvw(&self) -> UvwCoordinateLaw {
        self.uvw
    }

    /// Return the deterministic spectral coordinate law.
    #[must_use]
    pub const fn spectral(&self) -> &SpectralCoordinateSpec {
        &self.spectral
    }

    /// Return the bound Measures snapshot used by frame transformation.
    #[must_use]
    pub const fn measures_reference(&self) -> Option<LogicalIdentity> {
        self.measures_reference
    }

    /// Return the bound ephemeris snapshot used by a moving centre law.
    #[must_use]
    pub const fn ephemeris_reference(&self) -> Option<LogicalIdentity> {
        self.ephemeris_reference
    }
}

/// Exact reason a geometry specification failed closed.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum CompileGeometryError {
    /// Exactly one main image domain is required.
    #[error("compiled geometry requires exactly one main image domain")]
    MainDomainCount,
    /// Outlier names are non-empty and unique.
    #[error("outlier domain names must be non-empty and unique")]
    InvalidOutlierName,
    /// Image dimensions must be positive.
    #[error("image dimensions must be positive")]
    EmptyImageDomain,
    /// A sky direction was non-finite or outside the latitude domain.
    #[error("sky directions require finite longitude and latitude in [-pi/2, pi/2]")]
    InvalidSkyDirection,
    /// Direction WCS values were non-finite or outside their declared domains.
    #[error("direction WCS values must be finite with non-zero increments and valid poles")]
    InvalidDirectionWcs,
    /// The direction PC matrix is not invertible.
    #[error("direction WCS PC matrix must be invertible")]
    SingularDirectionMatrix,
    /// The four logical axes were not each placed exactly once.
    #[error("image axis order must contain each logical axis exactly once")]
    InvalidAxisOrder,
    /// Facet counts must be positive.
    #[error("regular facet row and column counts must be positive")]
    EmptyFacetLayout,
    /// Regular facets would omit or overlap pixels.
    #[error("{width}x{height} image is not exactly divisible by {columns}x{rows} facets")]
    NonDivisibleFacetLayout {
        /// Image width.
        width: usize,
        /// Image height.
        height: usize,
        /// Facet columns.
        columns: usize,
        /// Facet rows.
        rows: usize,
    },
    /// Facet count cannot be represented or reserved.
    #[error("regular facet layout is too large to represent")]
    FacetLayoutTooLarge,
    /// A centre law contained an invalid direction or target.
    #[error("phase, delay, and pointing centre laws must be explicit and valid")]
    InvalidCentreLaw,
    /// The selected POINTING column and declared meaning disagree.
    #[error("POINTING DIRECTION is antenna boresight and TARGET is the tracking target")]
    InconsistentPointingDirection,
    /// A moving centre law lacked a bound ephemeris snapshot.
    #[error("an ephemeris centre law requires bound ephemeris reference data")]
    MissingEphemerisReference,
    /// A spectral frame transform lacked a bound Measures snapshot.
    #[error("a spectral frame transform requires bound measures reference data")]
    MissingMeasuresReference,
    /// A frame transform omitted its anchor or an identity law supplied one.
    #[error("spectral frame anchors are required exactly when source and output frames differ")]
    InconsistentSpectralAnchor,
    /// Spectral-frame anchor values were not finite and valid.
    #[error("spectral frame anchors require a finite epoch, direction, and ITRF position")]
    InvalidSpectralAnchor,
    /// The output spectral WCS is empty, non-finite, degenerate, or non-monotonic.
    #[error("spectral WCS must be finite, non-empty, non-degenerate, and monotonic")]
    InvalidSpectralWcs,
    /// Continuum/line rest-frequency and velocity metadata disagree.
    #[error(
        "continuum requires NotApplicable velocity metadata and line coordinates require a velocity convention"
    )]
    InconsistentVelocityMetadata,
    /// A line rest frequency was not positive and finite.
    #[error("line rest frequency must be finite and positive")]
    InvalidRestFrequency,
}

pub(crate) fn compile_geometry(
    mut input: GeometryInput,
    inputs: &ProblemInputIdentities,
) -> Result<CompiledGeometry, CompileGeometryError> {
    let main_count = input
        .domains
        .iter()
        .filter(|domain| domain.role == ImageDomainRole::Main)
        .count();
    if main_count != 1 {
        return Err(CompileGeometryError::MainDomainCount);
    }
    let mut outlier_names = BTreeSet::new();
    for domain in &input.domains {
        if let ImageDomainRole::Outlier(name) = &domain.role
            && (name.trim().is_empty() || !outlier_names.insert(name.clone()))
        {
            return Err(CompileGeometryError::InvalidOutlierName);
        }
    }
    input
        .domains
        .sort_by(|left, right| match (&left.role, &right.role) {
            (ImageDomainRole::Main, ImageDomainRole::Main) => Ordering::Equal,
            (ImageDomainRole::Main, ImageDomainRole::Outlier(_)) => Ordering::Less,
            (ImageDomainRole::Outlier(_), ImageDomainRole::Main) => Ordering::Greater,
            (ImageDomainRole::Outlier(left), ImageDomainRole::Outlier(right)) => left.cmp(right),
        });

    let mut domains = Vec::with_capacity(input.domains.len());
    for domain in input.domains {
        if domain.shape.width == 0 || domain.shape.height == 0 {
            return Err(CompileGeometryError::EmptyImageDomain);
        }
        domain.axes.validate()?;
        let direction = domain.direction.canonicalize()?;
        let facets = compile_facets(domain.shape, domain.facets)?;
        domains.push(CompiledImageDomain {
            role: domain.role,
            shape: domain.shape,
            direction,
            facets,
            axes: domain.axes,
        });
    }

    canonicalize_centres(&mut input.centres)?;
    let ephemeris_reference =
        if matches!(input.centres.phase_tracking, PhaseCentreLaw::Ephemeris(_)) {
            Some(
                reference_identity(inputs, ReferenceDataKind::Ephemeris)
                    .ok_or(CompileGeometryError::MissingEphemerisReference)?,
            )
        } else {
            None
        };
    canonicalize_spectral(&mut input.spectral)?;
    let measures_reference = if input.spectral.source_frame != input.spectral.output_frame {
        Some(
            reference_identity(inputs, ReferenceDataKind::Measures)
                .ok_or(CompileGeometryError::MissingMeasuresReference)?,
        )
    } else {
        None
    };

    let mut compiled = CompiledGeometry {
        geometry_id: CompiledGeometryId(LogicalIdentity::from_sha256([0; 32])),
        domains: domains.into_boxed_slice(),
        centres: input.centres,
        uvw: input.uvw,
        spectral: input.spectral,
        measures_reference,
        ephemeris_reference,
    };
    compiled.geometry_id = canonical_geometry_id(&compiled);
    Ok(compiled)
}

fn compile_facets(
    shape: ImageShape,
    layout: FacetLayout,
) -> Result<Box<[FacetWindow]>, CompileGeometryError> {
    let (columns, rows) = match layout {
        FacetLayout::Single => (1, 1),
        FacetLayout::Regular { columns, rows } => (columns, rows),
    };
    if columns == 0 || rows == 0 {
        return Err(CompileGeometryError::EmptyFacetLayout);
    }
    if shape.width % columns != 0 || shape.height % rows != 0 {
        return Err(CompileGeometryError::NonDivisibleFacetLayout {
            width: shape.width,
            height: shape.height,
            columns,
            rows,
        });
    }
    let width = shape.width / columns;
    let height = shape.height / rows;
    let facet_count = columns
        .checked_mul(rows)
        .ok_or(CompileGeometryError::FacetLayoutTooLarge)?;
    let mut facets = Vec::new();
    facets
        .try_reserve_exact(facet_count)
        .map_err(|_| CompileGeometryError::FacetLayoutTooLarge)?;
    for row in 0..rows {
        for column in 0..columns {
            facets.push(FacetWindow {
                origin: [column * width, row * height],
                end_exclusive: [(column + 1) * width, (row + 1) * height],
            });
        }
    }
    Ok(facets.into_boxed_slice())
}

fn canonicalize_centres(centres: &mut CentreLaws) -> Result<(), CompileGeometryError> {
    match &mut centres.phase_tracking {
        PhaseCentreLaw::Observation => {}
        PhaseCentreLaw::Fixed(direction) => *direction = direction.canonicalize()?,
        PhaseCentreLaw::Ephemeris(target) if target.trim().is_empty() => {
            return Err(CompileGeometryError::InvalidCentreLaw);
        }
        PhaseCentreLaw::Ephemeris(_) => {}
    }
    if let DelayCentreLaw::Fixed(direction) = &mut centres.delay {
        *direction = direction.canonicalize()?;
    }
    match &mut centres.pointing {
        PointingCentreLaw::PhaseTrackingCentre => {}
        PointingCentreLaw::Observation(law)
            if !matches!(
                (law.direction_column, law.direction_semantic),
                (
                    PointingDirectionColumn::Direction,
                    PointingDirectionSemantic::AntennaBoresight
                ) | (
                    PointingDirectionColumn::Target,
                    PointingDirectionSemantic::TrackingTarget
                )
            ) =>
        {
            return Err(CompileGeometryError::InconsistentPointingDirection);
        }
        PointingCentreLaw::Observation(_) => {}
        PointingCentreLaw::Fixed(direction) => *direction = direction.canonicalize()?,
    }
    Ok(())
}

fn canonicalize_spectral(
    spectral: &mut SpectralCoordinateSpec,
) -> Result<(), CompileGeometryError> {
    match (
        &mut spectral.anchor,
        spectral.source_frame == spectral.output_frame,
    ) {
        (SpectralFrameAnchor::NotApplicable, true) => {}
        (SpectralFrameAnchor::Conversion { .. }, true)
        | (SpectralFrameAnchor::NotApplicable, false) => {
            return Err(CompileGeometryError::InconsistentSpectralAnchor);
        }
        (
            SpectralFrameAnchor::Conversion {
                epoch,
                direction,
                observatory_position,
            },
            false,
        ) => {
            if !epoch.mjd_days.is_finite()
                || observatory_position
                    .metres
                    .iter()
                    .any(|value| !value.is_finite())
                || observatory_position
                    .metres
                    .iter()
                    .all(|value| *value == 0.0)
            {
                return Err(CompileGeometryError::InvalidSpectralAnchor);
            }
            *direction = direction
                .canonicalize()
                .map_err(|_| CompileGeometryError::InvalidSpectralAnchor)?;
            epoch.mjd_days = canonical_zero(epoch.mjd_days);
            canonicalize_f64_slice(&mut observatory_position.metres);
        }
    }
    match &mut spectral.wcs {
        SpectralWcs::Linear {
            channels,
            reference_pixel,
            reference_frequency_hz,
            increment_hz,
        } => {
            if *channels == 0
                || !reference_pixel.is_finite()
                || !reference_frequency_hz.is_finite()
                || *reference_frequency_hz <= 0.0
                || !increment_hz.is_finite()
                || *increment_hz == 0.0
            {
                return Err(CompileGeometryError::InvalidSpectralWcs);
            }
            let centre = |channel: usize| {
                *reference_frequency_hz + (channel as f64 - *reference_pixel) * *increment_hz
            };
            let boundary = |edge: usize| {
                *reference_frequency_hz + (edge as f64 - 0.5 - *reference_pixel) * *increment_hz
            };
            let first_boundary = boundary(0);
            let last_boundary = boundary(*channels);
            let boundary_edges = [
                (first_boundary, boundary(1)),
                (boundary(*channels - 1), last_boundary),
            ];
            let centre_edges = (*channels > 1).then(|| {
                [
                    (centre(0), centre(1)),
                    (centre(*channels - 2), centre(*channels - 1)),
                ]
            });
            // Rounded affine evaluation is monotonic, but can contain plateaus.
            // Requiring one increment to span the largest ULP of both the
            // output endpoints and multiplication terms rules out an interior
            // plateau without iterating a caller-controlled channel count.
            let terms = [
                (-0.5 - *reference_pixel) * *increment_hz,
                (*channels as f64 - 0.5 - *reference_pixel) * *increment_hz,
                -*reference_pixel * *increment_hz,
                (*channels as f64 - 1.0 - *reference_pixel) * *increment_hz,
            ];
            let required_step = [first_boundary, last_boundary]
                .into_iter()
                .chain(terms)
                .map(f64_ulp)
                .fold(0.0, f64::max);
            if !first_boundary.is_finite()
                || first_boundary <= 0.0
                || !last_boundary.is_finite()
                || last_boundary <= 0.0
                || increment_hz.abs() < required_step
                || boundary_edges
                    .into_iter()
                    .any(|(first, second)| !advances(first, second, *increment_hz))
                || centre_edges.is_some_and(|edges| {
                    edges
                        .into_iter()
                        .any(|(first, second)| !advances(first, second, *increment_hz))
                })
            {
                return Err(CompileGeometryError::InvalidSpectralWcs);
            }
            *reference_pixel = canonical_zero(*reference_pixel);
            *increment_hz = canonical_zero(*increment_hz);
        }
        SpectralWcs::Tabular {
            channel_centres_hz,
            channel_boundaries_hz,
        } => {
            if channel_centres_hz.is_empty()
                || channel_centres_hz.len().checked_add(1) != Some(channel_boundaries_hz.len())
                || channel_centres_hz
                    .iter()
                    .chain(channel_boundaries_hz.iter())
                    .any(|frequency| !frequency.is_finite() || *frequency <= 0.0)
                || !strictly_monotonic(channel_centres_hz)
                || !strictly_monotonic(channel_boundaries_hz)
                || (channel_centres_hz.len() > 1
                    && (channel_centres_hz[0] < channel_centres_hz[channel_centres_hz.len() - 1])
                        != (channel_boundaries_hz[0]
                            < channel_boundaries_hz[channel_boundaries_hz.len() - 1]))
                || channel_boundaries_hz
                    .windows(2)
                    .zip(channel_centres_hz.iter())
                    .any(|(edges, centre)| {
                        *centre <= edges[0].min(edges[1]) || *centre >= edges[0].max(edges[1])
                    })
            {
                return Err(CompileGeometryError::InvalidSpectralWcs);
            }
            canonicalize_f64_slice(channel_centres_hz);
            canonicalize_f64_slice(channel_boundaries_hz);
        }
    }
    match (spectral.rest_frequency, spectral.doppler_convention) {
        (RestFrequency::NotApplicable, DopplerConvention::NotApplicable) => {}
        (RestFrequency::NotApplicable, _)
        | (RestFrequency::Line { .. }, DopplerConvention::NotApplicable) => {
            return Err(CompileGeometryError::InconsistentVelocityMetadata);
        }
        (RestFrequency::Line { hertz }, _) if !(hertz.is_finite() && hertz > 0.0) => {
            return Err(CompileGeometryError::InvalidRestFrequency);
        }
        (RestFrequency::Line { hertz }, _) => {
            spectral.rest_frequency = RestFrequency::Line {
                hertz: canonical_zero(hertz),
            };
        }
    }
    Ok(())
}

fn strictly_monotonic(values: &[f64]) -> bool {
    values.len() == 1
        || values.windows(2).all(|pair| pair[0] < pair[1])
        || values.windows(2).all(|pair| pair[0] > pair[1])
}

fn advances(first: f64, second: f64, increment_hz: f64) -> bool {
    if increment_hz.is_sign_positive() {
        first < second
    } else {
        first > second
    }
}

fn f64_ulp(value: f64) -> f64 {
    let magnitude = value.abs();
    if magnitude == 0.0 {
        f64::from_bits(1)
    } else {
        f64::from_bits(magnitude.to_bits() + 1) - magnitude
    }
}

fn reference_identity(
    inputs: &ProblemInputIdentities,
    wanted: ReferenceDataKind,
) -> Option<LogicalIdentity> {
    inputs
        .reference_data()
        .iter()
        .find_map(|(kind, identity)| (*kind == wanted).then_some(*identity))
}

fn canonical_geometry_id(geometry: &CompiledGeometry) -> CompiledGeometryId {
    let mut encoder = CanonicalEncoder::new();
    encoder.bytes(COMPILED_GEOMETRY_IDENTITY_DOMAIN);
    encoder.u32(COMPILED_GEOMETRY_IDENTITY_VERSION);
    encoder.usize(geometry.domains.len());
    for domain in &geometry.domains {
        match &domain.role {
            ImageDomainRole::Main => encoder.u8(0),
            ImageDomainRole::Outlier(name) => {
                encoder.u8(1);
                encoder.bytes(name.as_bytes());
            }
        }
        encoder.usize(domain.shape.width);
        encoder.usize(domain.shape.height);
        encode_direction_coordinate(&mut encoder, domain.direction);
        encoder.usize(domain.facets.len());
        for facet in &domain.facets {
            for value in facet.origin.into_iter().chain(facet.end_exclusive) {
                encoder.usize(value);
            }
        }
        for axis in domain.axes.positions {
            encoder.u8(image_axis_tag(axis));
        }
    }
    encode_centres(&mut encoder, &geometry.centres);
    encoder.u8(match geometry.uvw {
        UvwCoordinateLaw::PhaseTrackingCentre => 0,
    });
    encode_spectral(&mut encoder, &geometry.spectral);
    encode_optional_identity(&mut encoder, geometry.measures_reference);
    encode_optional_identity(&mut encoder, geometry.ephemeris_reference);
    CompiledGeometryId(LogicalIdentity::from_sha256(encoder.finish()))
}

fn encode_direction_coordinate(encoder: &mut CanonicalEncoder, direction: DirectionCoordinateSpec) {
    encoder.u8(match direction.projection {
        Projection::Sin => 0,
    });
    encode_sky_direction(encoder, direction.reference_direction);
    for value in direction
        .reference_pixel
        .into_iter()
        .chain(direction.increment_rad)
        .chain(direction.pc.into_iter().flatten())
        .chain(direction.pole_deg)
    {
        encoder.f64(value);
    }
}

pub(crate) fn encode_sky_direction(encoder: &mut CanonicalEncoder, direction: SkyDirection) {
    encoder.u8(direction_frame_tag(direction.frame));
    encoder.f64(direction.longitude_rad);
    encoder.f64(direction.latitude_rad);
}

fn encode_centres(encoder: &mut CanonicalEncoder, centres: &CentreLaws) {
    match &centres.phase_tracking {
        PhaseCentreLaw::Observation => encoder.u8(0),
        PhaseCentreLaw::Fixed(direction) => {
            encoder.u8(1);
            encode_sky_direction(encoder, *direction);
        }
        PhaseCentreLaw::Ephemeris(target) => {
            encoder.u8(2);
            encoder.bytes(target.as_bytes());
        }
    }
    match centres.delay {
        DelayCentreLaw::PhaseTrackingCentre => encoder.u8(0),
        DelayCentreLaw::Observation => encoder.u8(1),
        DelayCentreLaw::Fixed(direction) => {
            encoder.u8(2);
            encode_sky_direction(encoder, direction);
        }
    }
    match centres.pointing {
        PointingCentreLaw::PhaseTrackingCentre => encoder.u8(0),
        PointingCentreLaw::Observation(law) => {
            encoder.u8(1);
            encoder.u8(match law.direction_column {
                PointingDirectionColumn::Direction => 0,
                PointingDirectionColumn::Target => 1,
            });
            encoder.u8(match law.direction_semantic {
                PointingDirectionSemantic::AntennaBoresight => 0,
                PointingDirectionSemantic::TrackingTarget => 1,
            });
            encoder.u8(match law.time_sampling {
                PointingTimeSampling::VisibilityTime => 0,
                PointingTimeSampling::VisibilityTimeCentroid => 1,
            });
            encoder.u8(match law.interpolation {
                PointingInterpolation::Nearest => 0,
                PointingInterpolation::GreatCircleShortestArc => 1,
            });
            encoder.u8(match law.extrapolation {
                PointingExtrapolation::Reject => 0,
                PointingExtrapolation::HoldNearest => 1,
            });
            encoder.u8(match law.missing {
                MissingPointingPolicy::Reject => 0,
                MissingPointingPolicy::UsePhaseTrackingCentre => 1,
            });
        }
        PointingCentreLaw::Fixed(direction) => {
            encoder.u8(2);
            encode_sky_direction(encoder, direction);
        }
    }
}

fn encode_spectral(encoder: &mut CanonicalEncoder, spectral: &SpectralCoordinateSpec) {
    encoder.u8(frequency_frame_tag(spectral.source_frame));
    encoder.u8(frequency_frame_tag(spectral.output_frame));
    match spectral.anchor {
        SpectralFrameAnchor::NotApplicable => encoder.u8(0),
        SpectralFrameAnchor::Conversion {
            epoch,
            direction,
            observatory_position,
        } => {
            encoder.u8(1);
            encoder.f64(epoch.mjd_days);
            encoder.u8(time_scale_tag(epoch.scale));
            encode_sky_direction(encoder, direction);
            for value in observatory_position.metres {
                encoder.f64(value);
            }
        }
    }
    match &spectral.wcs {
        SpectralWcs::Linear {
            channels,
            reference_pixel,
            reference_frequency_hz,
            increment_hz,
        } => {
            encoder.u8(0);
            encoder.usize(*channels);
            encoder.f64(*reference_pixel);
            encoder.f64(*reference_frequency_hz);
            encoder.f64(*increment_hz);
        }
        SpectralWcs::Tabular {
            channel_centres_hz,
            channel_boundaries_hz,
        } => {
            encoder.u8(1);
            encoder.usize(channel_centres_hz.len());
            for frequency in channel_centres_hz {
                encoder.f64(*frequency);
            }
            encoder.usize(channel_boundaries_hz.len());
            for frequency in channel_boundaries_hz {
                encoder.f64(*frequency);
            }
        }
    }
    match spectral.rest_frequency {
        RestFrequency::NotApplicable => encoder.u8(0),
        RestFrequency::Line { hertz } => {
            encoder.u8(1);
            encoder.f64(hertz);
        }
    }
    encoder.u8(match spectral.doppler_convention {
        DopplerConvention::NotApplicable => 0,
        DopplerConvention::Radio => 1,
        DopplerConvention::Optical => 2,
        DopplerConvention::Relativistic => 3,
    });
}

fn encode_optional_identity(encoder: &mut CanonicalEncoder, identity: Option<LogicalIdentity>) {
    match identity {
        None => encoder.u8(0),
        Some(identity) => {
            encoder.u8(1);
            encoder.identity(identity);
        }
    }
}

fn image_axis_tag(axis: ImageAxis) -> u8 {
    match axis {
        ImageAxis::DirectionLongitude => 0,
        ImageAxis::DirectionLatitude => 1,
        ImageAxis::Polarization => 2,
        ImageAxis::Spectral => 3,
    }
}

fn direction_frame_tag(frame: DirectionFrame) -> u8 {
    match frame {
        DirectionFrame::Icrs => 0,
        DirectionFrame::J2000 => 1,
        DirectionFrame::B1950 => 2,
        DirectionFrame::Galactic => 3,
    }
}

pub(crate) fn frequency_frame_tag(frame: FrequencyFrame) -> u8 {
    match frame {
        FrequencyFrame::Topocentric => 0,
        FrequencyFrame::Barycentric => 1,
        FrequencyFrame::Lsrk => 2,
    }
}

pub(crate) fn time_scale_tag(scale: TimeScale) -> u8 {
    match scale {
        TimeScale::Utc => 0,
        TimeScale::Tai => 1,
        TimeScale::Tt => 2,
        TimeScale::Tdb => 3,
    }
}

fn canonical_longitude(longitude: f64) -> f64 {
    canonical_zero(longitude.rem_euclid(TAU))
}

fn canonical_degrees(degrees: f64) -> f64 {
    canonical_zero(degrees.rem_euclid(360.0))
}

fn canonical_zero(value: f64) -> f64 {
    if value == 0.0 { 0.0 } else { value }
}

fn canonicalize_f64_slice(values: &mut [f64]) {
    for value in values {
        *value = canonical_zero(*value);
    }
}

fn write_hex(formatter: &mut fmt::Formatter<'_>, bytes: &[u8]) -> fmt::Result {
    for byte in bytes {
        write!(formatter, "{byte:02x}")?;
    }
    Ok(())
}

// SPDX-License-Identifier: LGPL-3.0-or-later

//! Immutable celestial, UVW, image-domain, and spectral coordinate laws.

use std::{cmp::Ordering, collections::BTreeSet, f64::consts::TAU};

use casa_core::measures::PreparedFrequencyConversion;
use thiserror::Error;

use crate::sky_projection::Projection;

mod spectral;

pub use spectral::DopplerConvention;
pub use spectral::Epoch;
pub use spectral::FrequencyFrame;
pub use spectral::ItrfPosition;
pub use spectral::RestFrequency;
pub use spectral::SourceLsrkGrid;
pub use spectral::SpectralCoordinateSpec;
pub use spectral::SpectralFrameAnchor;
pub use spectral::SpectralWcs;
pub use spectral::TimeScale;
use spectral::*;

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

    /// Replace the framed reference direction in an uncompiled specification.
    #[must_use]
    pub const fn with_reference_direction(mut self, reference_direction: SkyDirection) -> Self {
        self.reference_direction = reference_direction;
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

/// Phase centre used when constructing a chart's point-spread function.
///
/// This is a scientific image-domain law. It is deliberately independent of
/// execution tiling and of the observation's phase-tracking centre.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum PsfPhaseCentreLaw {
    /// Use the chart's direction-coordinate reference direction.
    ImageDomainReference,
    /// Use one explicit framed direction.
    Fixed(SkyDirection),
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
    psf_phase_centre: PsfPhaseCentreLaw,
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
            psf_phase_centre: PsfPhaseCentreLaw::ImageDomainReference,
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

    /// Return the requested PSF phase-centre law.
    #[must_use]
    pub const fn psf_phase_centre(&self) -> PsfPhaseCentreLaw {
        self.psf_phase_centre
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

    /// Replace the PSF phase-centre law in an uncompiled specification.
    #[must_use]
    pub const fn with_psf_phase_centre(mut self, psf_phase_centre: PsfPhaseCentreLaw) -> Self {
        self.psf_phase_centre = psf_phase_centre;
        self
    }
}

/// Missing-row policy for observation pointing metadata.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MissingPointingPolicy {
    /// Reject missing pointing metadata.
    Reject,
    /// Substitute the visibility field's phase direction when an antenna
    /// has no pointing sample, as CASA AW projection does.
    UseFieldPhaseCentre,
    /// Point every row at its field's direction when the store holds no
    /// pointing sample at all (no POINTING table, or an empty one), as
    /// CASA's mosaic does (`SimplePBConvFunc::pointingDirAnt1` takes the
    /// phase centre unless the table is readable and has rows); reject an
    /// antenna without samples in a store that has some.
    FieldCentreWithoutPointingTable,
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

/// Law selecting antenna pointing directions.
#[derive(Debug, Clone, PartialEq)]
pub enum PointingCentreLaw {
    /// Use the phase-tracking centre for every sample.
    PhaseTrackingCentre,
    /// Use each sample's FIELD direction: where a mosaic or A-projection
    /// pointing sits without the POINTING table (tclean `usepointing=False`,
    /// `VisBuffer2::direction1` from the field).
    FieldCentre,
    /// Use observation POINTING rows according to a complete sampling law.
    Observation(ObservationPointingLaw),
    /// Use one fixed framed direction.
    Fixed(SkyDirection),
}

/// Phase-tracking and pointing centre laws, without evaluated sample arrays.
#[derive(Debug, Clone, PartialEq)]
pub struct CentreLaws {
    phase_tracking: PhaseCentreLaw,
    pointing: PointingCentreLaw,
}

impl CentreLaws {
    /// Construct all centre-selection laws explicitly.
    #[must_use]
    pub const fn new(phase_tracking: PhaseCentreLaw, pointing: PointingCentreLaw) -> Self {
        Self {
            phase_tracking,
            pointing,
        }
    }

    /// Return the phase-tracking centre law.
    #[must_use]
    pub const fn phase_tracking(&self) -> &PhaseCentreLaw {
        &self.phase_tracking
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
    /// CASA MosaicFT/AWProjectFT `girarUVW` projection from each observation field to the
    /// compiled image centre, paired with the reconstruction phase convention.
    MosaicPhaseTrackingCentre,
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
            Self::PhaseTrackingCentre | Self::MosaicPhaseTrackingCentre => UvwUnit::Metres,
        }
    }

    /// Return the UVW axis orientation.
    #[must_use]
    pub const fn axes(self) -> UvwAxes {
        match self {
            Self::PhaseTrackingCentre | Self::MosaicPhaseTrackingCentre => {
                UvwAxes::EastNorthPhaseTrackingCentre
            }
        }
    }

    /// Return the prediction phase convention.
    #[must_use]
    pub const fn prediction_phase(self) -> VisibilityPhaseConvention {
        match self {
            Self::PhaseTrackingCentre | Self::MosaicPhaseTrackingCentre => {
                VisibilityPhaseConvention::NegativeTwoPiFrequencyDelay
            }
        }
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
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct FacetWindow {
    origin: [usize; 2],
    end_exclusive: [usize; 2],
    direction: DirectionCoordinateSpec,
    phase_centre: SkyDirection,
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

    /// Return CASA's integer centre pixel in facet-local coordinates.
    #[must_use]
    pub const fn local_centre_pixel(self) -> [usize; 2] {
        [
            (self.end_exclusive[0] - self.origin[0]) / 2,
            (self.end_exclusive[1] - self.origin[1]) / 2,
        ]
    }

    /// Return the exact parent-WCS subwindow used by this facet chart.
    ///
    /// The chart retains the domain's common tangent direction and shifts only
    /// the reference pixel into facet-local coordinates.
    #[must_use]
    pub const fn direction(self) -> DirectionCoordinateSpec {
        self.direction
    }

    /// Return the phase centre used by paired facet operators.
    #[must_use]
    pub const fn phase_centre(self) -> SkyDirection {
        self.phase_centre
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
    psf_phase_centre: SkyDirection,
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

    /// Return the model phase centre for this chart.
    ///
    /// Model coefficients are defined on the chart WCS, so this is exactly
    /// the compiled direction-coordinate reference direction.
    #[must_use]
    pub const fn model_phase_centre(&self) -> SkyDirection {
        self.direction.reference_direction()
    }

    /// Return the independently compiled PSF sampling phase centre.
    #[must_use]
    pub const fn psf_phase_centre(&self) -> SkyDirection {
        self.psf_phase_centre
    }
}

/// Immutable compiler-owned geometry accepted by planning and execution.
#[derive(Debug, Clone, PartialEq)]
pub struct CompiledGeometry {
    domains: Box<[CompiledImageDomain]>,
    centres: CentreLaws,
    uvw: UvwCoordinateLaw,
    spectral: SpectralCoordinateSpec,
}

impl CompiledGeometry {
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
    /// A facet centre fell outside the compiled direction projection.
    #[error("a facet centre pixel falls outside the direction projection")]
    FacetCentreOutsideProjection,
    /// A centre law contained an invalid direction or target.
    #[error("phase, delay, and pointing centre laws must be explicit and valid")]
    InvalidCentreLaw,
    /// The selected POINTING column and declared meaning disagree.
    #[error("POINTING DIRECTION is antenna boresight and TARGET is the tracking target")]
    InconsistentPointingDirection,
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
        let psf_phase_centre = match domain.psf_phase_centre {
            PsfPhaseCentreLaw::ImageDomainReference => direction.reference_direction(),
            PsfPhaseCentreLaw::Fixed(direction) => direction.canonicalize()?,
        };
        let facets = compile_facets(domain.shape, domain.facets, direction)?;
        domains.push(CompiledImageDomain {
            role: domain.role,
            shape: domain.shape,
            direction,
            facets,
            axes: domain.axes,
            psf_phase_centre,
        });
    }

    canonicalize_centres(&mut input.centres)?;
    canonicalize_spectral(&mut input.spectral)?;
    Ok(CompiledGeometry {
        domains: domains.into_boxed_slice(),
        centres: input.centres,
        uvw: input.uvw,
        spectral: input.spectral,
    })
}

fn compile_facets(
    shape: ImageShape,
    layout: FacetLayout,
    direction: DirectionCoordinateSpec,
) -> Result<Box<[FacetWindow]>, CompileGeometryError> {
    let (columns, rows) = match layout {
        FacetLayout::Single => (1, 1),
        FacetLayout::Regular { columns, rows } => (columns, rows),
    };
    if columns == 0 || rows == 0 {
        return Err(CompileGeometryError::EmptyFacetLayout);
    }
    if !shape.width.is_multiple_of(columns) || !shape.height.is_multiple_of(rows) {
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
            let origin = [column * width, row * height];
            let end_exclusive = [(column + 1) * width, (row + 1) * height];
            let facet_direction = direction.with_reference_pixel([
                direction.reference_pixel()[0] - origin[0] as f64,
                direction.reference_pixel()[1] - origin[1] as f64,
            ]);
            let phase_centre = if columns == 1 && rows == 1 {
                direction.reference_direction()
            } else {
                direction_at_pixel(facet_direction, [(width / 2) as f64, (height / 2) as f64])?
            };
            facets.push(FacetWindow {
                origin,
                end_exclusive,
                direction: facet_direction,
                phase_centre,
            });
        }
    }
    Ok(facets.into_boxed_slice())
}

fn direction_at_pixel(
    direction: DirectionCoordinateSpec,
    pixel: [f64; 2],
) -> Result<SkyDirection, CompileGeometryError> {
    let [longitude, latitude] = direction
        .coordinate()
        .pixel_to_world(pixel)
        .map_err(|_| CompileGeometryError::FacetCentreOutsideProjection)?;
    SkyDirection::new(direction.reference_direction().frame(), longitude, latitude).canonicalize()
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
    match &mut centres.pointing {
        PointingCentreLaw::PhaseTrackingCentre | PointingCentreLaw::FieldCentre => {}
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

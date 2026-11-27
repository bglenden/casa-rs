// SPDX-License-Identifier: LGPL-3.0-or-later

use super::*;

/// Spectral frequency reference frame.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FrequencyFrame {
    /// Source rest frame.
    Rest,
    /// Observer topocentric frame.
    Topocentric,
    /// Solar-system barycentric frame.
    Barycentric,
    /// Kinematic Local Standard of Rest.
    Lsrk,
}

impl FrequencyFrame {
    /// Whether a cube in this output frame is gridded through LSRK, as
    /// CASA grids it: `SIImageStore` gives the grid of an image in any
    /// frame but LSRK and REST an LSRK conversion layer at the image's
    /// epoch, and `FTMachine::matchChannel` converts each row to LSRK at
    /// its own time. Such a cube's channels are fixed in LSRK, so even one
    /// in its rows' own frame needs a [`SpectralFrameAnchor::Conversion`].
    #[must_use]
    pub const fn gridded_through_lsrk(self) -> bool {
        matches!(self, Self::Topocentric | Self::Barycentric)
    }
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
    pub(super) mjd_days: f64,
    pub(super) scale: TimeScale,
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
    pub(super) metres: [f64; 3],
}

/// Context required to convert between distinct spectral frequency frames,
/// or to grid a cube through LSRK
/// ([`FrequencyFrame::gridded_through_lsrk`]).
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum SpectralFrameAnchor {
    /// Source and output frames are identical and the rows grid at their
    /// own frequencies, so conversion is not applicable.
    NotApplicable,
    /// Exact Measures-frame context for a requested conversion: the image's
    /// epoch, reference direction and observatory, at which an output frame
    /// gridded through LSRK is fixed to LSRK.
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

/// An LSRK interpolation grid retained alongside the reported TOPO axis.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct SourceLsrkGrid {
    /// Exact centre of plane zero in the source coordinate.
    pub first_hz: f64,
    /// Signed measured channel width in the source coordinate.
    pub increment_hz: f64,
    /// Prepared conversions from the reported coordinate to LSRK and back.
    /// They evaluate each output centre and map density lookups to pixels.
    pub reporting_conversions: Option<[PreparedFrequencyConversion; 2]>,
    /// The requested last centre lies beyond the range of native channel
    /// centres, even though its evaluated centre equals the stored edge.
    pub terminal_plane_outside: bool,
}

/// Uncompiled deterministic spectral coordinate and transform specification.
#[derive(Debug, Clone, PartialEq)]
pub struct SpectralCoordinateSpec {
    pub(super) source_frame: FrequencyFrame,
    pub(super) output_frame: FrequencyFrame,
    pub(super) anchor: SpectralFrameAnchor,
    pub(super) wcs: SpectralWcs,
    pub(super) source_lsrk_grid: Option<SourceLsrkGrid>,
    pub(super) rest_frequency: RestFrequency,
    pub(super) doppler_convention: DopplerConvention,
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
            source_lsrk_grid: None,
            rest_frequency,
            doppler_convention,
        }
    }

    /// Retain the LSRK sampling coordinate alongside the reported TOPO WCS.
    /// Channel-index grids can also record their terminal source coverage.
    #[must_use]
    pub const fn with_source_lsrk_grid(mut self, grid: Option<SourceLsrkGrid>) -> Self {
        self.source_lsrk_grid = grid;
        self
    }

    /// Whether rows and the interpolation grid are both evaluated in LSRK.
    #[must_use]
    pub const fn has_source_lsrk_grid(&self) -> bool {
        self.source_lsrk_grid.is_some()
    }

    /// Prepared conversions to and from the retained LSRK grid.
    #[must_use]
    pub const fn reporting_conversions(&self) -> Option<[PreparedFrequencyConversion; 2]> {
        match self.source_lsrk_grid {
            Some(grid) => grid.reporting_conversions,
            None => None,
        }
    }

    /// Whether the source span excludes the final plane from gridding.
    #[must_use]
    pub const fn terminal_plane_outside(&self) -> bool {
        matches!(self.source_lsrk_grid, Some(grid) if grid.terminal_plane_outside)
    }

    /// Linear interpolation coordinates in the frame the row frequencies
    /// use. Returns the reference pixel, its centre and the increment.
    #[must_use]
    pub fn linear_grid(&self) -> Option<[f64; 3]> {
        if let Some(grid) = self.source_lsrk_grid {
            return Some([0.0, grid.first_hz, grid.increment_hz]);
        }
        match self.wcs {
            SpectralWcs::Linear {
                reference_pixel,
                reference_frequency_hz,
                increment_hz,
                ..
            } => Some([reference_pixel, reference_frequency_hz, increment_hz]),
            SpectralWcs::Tabular { .. } => None,
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
        self.source_lsrk_grid = None;
        self
    }

    /// Replace the transform-anchor law in an uncompiled specification.
    #[must_use]
    pub const fn with_anchor(mut self, anchor: SpectralFrameAnchor) -> Self {
        self.anchor = anchor;
        self.source_lsrk_grid = None;
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
        self.source_lsrk_grid = None;
        self
    }

    /// Replace the output WCS in an uncompiled specification.
    #[must_use]
    pub fn with_wcs(mut self, wcs: SpectralWcs) -> Self {
        self.wcs = wcs;
        self.source_lsrk_grid = None;
        self
    }
}

pub(super) fn canonicalize_spectral(
    spectral: &mut SpectralCoordinateSpec,
) -> Result<(), CompileGeometryError> {
    // Distinct frames need a conversion; one frame allows one only to be
    // gridded through LSRK.
    if let Some(grid) = spectral.source_lsrk_grid {
        let (first, step) = (grid.first_hz, grid.increment_hz);
        let SpectralWcs::Linear {
            channels,
            increment_hz,
            reference_pixel: 0.0,
            ..
        } = spectral.wcs
        else {
            return Err(CompileGeometryError::InvalidSpectralWcs);
        };
        let last = first + channels.saturating_sub(1) as f64 * step;
        let valid_source = match spectral.source_frame {
            FrequencyFrame::Lsrk if grid.reporting_conversions.is_none() => matches!(
                spectral.doppler_convention,
                DopplerConvention::Radio | DopplerConvention::NotApplicable
            ),
            FrequencyFrame::Topocentric | FrequencyFrame::Lsrk => {
                grid.reporting_conversions.is_some()
                    && matches!(spectral.anchor, SpectralFrameAnchor::Conversion { .. })
                    && !grid.terminal_plane_outside
            }
            _ => false,
        };
        if !valid_source
            || spectral.output_frame != FrequencyFrame::Topocentric
            || channels < 2
            || !step.is_normal()
            || step.is_sign_positive() != increment_hz.is_sign_positive()
            || !first.is_finite()
            || first <= 0.0
            || !last.is_finite()
            || last <= 0.0
            || !advances(first, first + step, step)
            || !advances(last - step, last, step)
        {
            return Err(CompileGeometryError::InvalidSpectralWcs);
        }
    }
    let converts = spectral.source_frame != spectral.output_frame;
    let allowed = converts || spectral.output_frame.gridded_through_lsrk();
    match (&mut spectral.anchor, allowed) {
        (SpectralFrameAnchor::NotApplicable, _) if !converts => {}
        (SpectralFrameAnchor::Conversion { .. }, false)
        | (SpectralFrameAnchor::NotApplicable, _) => {
            return Err(CompileGeometryError::InconsistentSpectralAnchor);
        }
        (
            SpectralFrameAnchor::Conversion {
                epoch,
                direction,
                observatory_position,
            },
            true,
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

pub(super) fn strictly_monotonic(values: &[f64]) -> bool {
    values.len() == 1
        || values.windows(2).all(|pair| pair[0] < pair[1])
        || values.windows(2).all(|pair| pair[0] > pair[1])
}

pub(super) fn advances(first: f64, second: f64, increment_hz: f64) -> bool {
    if increment_hz.is_sign_positive() {
        first < second
    } else {
        first > second
    }
}

pub(super) fn f64_ulp(value: f64) -> f64 {
    let magnitude = value.abs();
    if magnitude == 0.0 {
        f64::from_bits(1)
    } else {
        f64::from_bits(magnitude.to_bits() + 1) - magnitude
    }
}

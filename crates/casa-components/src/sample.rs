// SPDX-License-Identifier: LGPL-3.0-or-later
//! A component sampled at an image's pixel directions, as CASA's
//! `SkyCompRep::sample` and the shapes' `sample` methods sample it
//! (`components/ComponentModels`) when `ia.modify` adds a component list to
//! an image.
//!
//! Each shape gives every pixel direction a scale:
//! - **a point:** 1 when the direction lies within half a pixel of the
//!   component along both axes, else 0 (`PointShape::dirIsInPixel`);
//! - **a Gaussian:** the pixel's area times the unit-flux Gaussian at the
//!   direction's offset, out to four major axes (`GaussianShape::sample`);
//! - **a disk:** the pixel's area over the disk's, for a direction inside
//!   the ellipse (`DiskShape::_calcSample`).
//!
//! The offsets are casacore's: the separation and position angle from the
//! component to the direction (`MVDirection::separation` and
//! `MVDirection::positionAngle`), the angle counted from north through east.
//! A pixel gains the component's Stokes flux in Jy, sampled by its spectral
//! model at each frequency
//! ([`SpectralModel::sample_stokes`](crate::SpectralModel::sample_stokes)),
//! times the scale.
//!
//! The component's reference direction is first converted into the frame of
//! the pixel directions with no measures frame, as CASA converts it; a
//! conversion that needs an epoch or a position (to AZEL, say) is refused. A
//! limb-darkened disk is refused, as its visibility is.
//!
//! The frequencies are taken in the frame of the spectral model's reference
//! frequency, as the visibility takes them: a caller sampling an image in
//! another frame converts them first (CASA converts the reference frequency
//! into the image's frame, `refFreqInFrame`, which gives the same ratio).

use std::f64::consts::{FRAC_PI_4, PI};

use casa_types::measures::direction::{DirectionRef, MDirection};
use casa_types::measures::frame::MeasFrame;
use ndarray::Array3;

use crate::SkyComponent;
use crate::error::ComponentError;
use crate::shape::{ComponentShape, ShapeKind};

/// The pixels a component is sampled on: the frame of their directions and
/// the size of a pixel along each axis.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct PixelGrid {
    /// The frame of the pixel directions (the image's direction coordinate).
    pub frame: DirectionRef,
    /// The pixel's size along the longitude axis, rad (the increment's
    /// magnitude).
    pub longitude_size_rad: f64,
    /// The pixel's size along the latitude axis, rad.
    pub latitude_size_rad: f64,
}

impl PixelGrid {
    /// The pixel's solid angle, sr, as CASA forms it
    /// (`pixelLatSize.radian() * pixelLongSize.radian()`).
    fn area(&self) -> f64 {
        self.latitude_size_rad * self.longitude_size_rad
    }
}

/// `MVPosition::getLong`: the longitude of `xyz`, 0 at a pole.
fn longitude(xyz: [f64; 3]) -> f64 {
    if xyz[0] != 0.0 || xyz[1] != 0.0 {
        xyz[1].atan2(xyz[0])
    } else {
        0.0
    }
}

/// `MVDirection::separation`: the angle from the unit vector `from` to
/// `to` (normalised first).
fn separation(from: [f64; 3], to: [f64; 3]) -> f64 {
    let length = (to[0] * to[0] + to[1] * to[1] + to[2] * to[2]).sqrt();
    let length = if length > 0.0 { length } else { 1.0 };
    let half = ((from[0] - to[0] / length).powi(2)
        + (from[1] - to[1] / length).powi(2)
        + (from[2] - to[2] / length).powi(2))
    .sqrt()
        / 2.0;
    2.0 * half.min(1.0).asin()
}

/// `MVDirection::positionAngle`: the position angle of `to` seen from the
/// unit vector `from`, north through east.
fn position_angle(from: [f64; 3], to: [f64; 3]) -> f64 {
    let longitude_difference = longitude(from) - longitude(to);
    let sin_lat1 = from[2];
    let sin_lat2 = to[2];
    let cos_lat2 = (1.0 - sin_lat2 * sin_lat2).abs().sqrt();
    let s1 = -cos_lat2 * longitude_difference.sin();
    let c1 = (1.0 - sin_lat1 * sin_lat1).abs().sqrt() * sin_lat2
        - sin_lat1 * cos_lat2 * longitude_difference.cos();
    if s1 != 0.0 || c1 != 0.0 {
        s1.atan2(c1)
    } else {
        0.0
    }
}

/// casacore's `Gaussian2DParam::fwhm2int`, `1/√(ln 16)`.
fn fwhm_to_width() -> f64 {
    1.0 / 16.0_f64.ln().sqrt()
}

impl ComponentShape {
    /// The shape's scale at each of `directions` (direction cosines in
    /// `grid.frame`): see the module documentation.
    ///
    /// # Errors
    ///
    /// [`ComponentError::Direction`] when the reference direction cannot be
    /// converted into `grid.frame` without a measures frame, and
    /// [`ComponentError::ImageSample`] for a limb-darkened disk.
    pub fn pixel_scales(
        &self,
        directions: &[[f64; 3]],
        grid: &PixelGrid,
    ) -> Result<Vec<f64>, ComponentError> {
        let centre = self.direction_in(grid.frame)?;
        Ok(match self.kind() {
            ShapeKind::Point => {
                let near = grid.latitude_size_rad.max(grid.longitude_size_rad);
                directions
                    .iter()
                    .map(|&direction| {
                        let separation = separation(centre, direction).abs();
                        if separation > near {
                            return 0.0;
                        }
                        let angle = position_angle(centre, direction);
                        let inside = (separation * angle.sin()).abs()
                            <= grid.longitude_size_rad / 2.0
                            && (separation * angle.cos()).abs() <= grid.latitude_size_rad / 2.0;
                        if inside { 1.0 } else { 0.0 }
                    })
                    .collect()
            }
            ShapeKind::Gaussian { axes, .. } => {
                // `Gaussian2D` with its major axis along y, unit flux.
                let width = fwhm_to_width();
                let major = axes.major_axis();
                let ratio = axes.minor_axis() / major;
                let height = 1.0 / ((major * major * ratio * PI).abs() * width * width);
                let (sin_pa, cos_pa) = axes.position_angle().sin_cos();
                let area = grid.area();
                let cutoff = 4.0 * major;
                directions
                    .iter()
                    .map(|&direction| {
                        let separation = separation(centre, direction);
                        if separation > cutoff {
                            return 0.0;
                        }
                        let angle = -position_angle(centre, direction);
                        let (x, y) = (separation * angle.sin(), separation * angle.cos());
                        let across = (cos_pa * x + sin_pa * y) / (major * ratio * width);
                        let along = (-sin_pa * x + cos_pa * y) / (major * width);
                        area * height * (-(across * across + along * along)).exp()
                    })
                    .collect()
            }
            ShapeKind::Disk { axes, .. } => {
                let major_radius = axes.major_axis() / 2.0;
                let minor_radius = axes.minor_axis() / 2.0;
                let value = grid.area() / (FRAC_PI_4 * axes.major_axis() * axes.minor_axis());
                directions
                    .iter()
                    .map(|&direction| {
                        let separation = separation(centre, direction);
                        if separation > major_radius {
                            return 0.0;
                        }
                        let angle = position_angle(centre, direction) - axes.position_angle();
                        let along = (separation * angle.cos()).abs();
                        let across = (separation * angle.sin()).abs();
                        let inside = along <= major_radius
                            && across <= minor_radius
                            && across
                                <= minor_radius * (1.0 - (along / major_radius).powi(2)).sqrt();
                        if inside { value } else { 0.0 }
                    })
                    .collect()
            }
            ShapeKind::LimbDarkenedDisk { .. } => {
                return Err(ComponentError::ImageSample {
                    shape: "limb-darkened disk",
                    reason: "casa-rs does not yet sample its brightness profile".to_string(),
                });
            }
        })
    }

    /// The reference direction's cosines in `frame`.
    fn direction_in(&self, frame: DirectionRef) -> Result<[f64; 3], ComponentError> {
        let direction: &MDirection = self.reference_direction();
        direction
            .convert_to(frame, &MeasFrame::default())
            .map(|converted| converted.cosines())
            .map_err(|error| ComponentError::Direction {
                from: direction.refer().as_str().to_string(),
                to: frame.as_str().to_string(),
                reason: error.to_string(),
            })
    }
}

impl SkyComponent {
    /// Add the component, sampled at `directions` (cosines in
    /// `grid.frame`) and `frequencies_hz`, to `samples`, indexed
    /// `[Stokes I/Q/U/V, direction, frequency]` (`SkyCompRep::sample`).
    ///
    /// # Errors
    ///
    /// Those of [`ComponentShape::pixel_scales`],
    /// [`Flux::stokes_jy_values`](crate::Flux::stokes_jy_values) and
    /// [`SpectralModel::sample_stokes`](crate::SpectralModel::sample_stokes).
    ///
    /// # Panics
    ///
    /// When `samples` is not `[4, directions.len(), frequencies_hz.len()]`.
    pub fn sample_pixels(
        &self,
        samples: &mut Array3<f64>,
        directions: &[[f64; 3]],
        grid: &PixelGrid,
        frequencies_hz: &[f64],
    ) -> Result<(), ComponentError> {
        assert_eq!(
            samples.shape(),
            [4, directions.len(), frequencies_hz.len()],
            "samples are [Stokes, direction, frequency]"
        );
        let iquv = self.flux().stokes_jy_values()?;
        let scales = self.shape().pixel_scales(directions, grid)?;
        let spectra = frequencies_hz
            .iter()
            .map(|&frequency| self.spectrum().sample_stokes(frequency, iquv))
            .collect::<Result<Vec<_>, _>>()?;
        for (direction, &scale) in scales.iter().enumerate() {
            if scale == 0.0 {
                continue;
            }
            for (frequency, stokes) in spectra.iter().enumerate() {
                for (index, value) in stokes.iter().enumerate() {
                    samples[[index, direction, frequency]] += scale * value;
                }
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests;

// SPDX-License-Identifier: LGPL-3.0-or-later

use std::f64::consts::FRAC_PI_2;

use casa_components::{
    ComponentShape, DirectionError, Ellipse, EllipseErrors, Flux, Polarisation, ShapeKind,
    SkyComponent, SpectralModel,
};
use casa_coordinates::{Coordinate, StokesType};
use casa_core::measures::direction::MDirection;
use casa_core::measures::frequency::MFrequency;
use casa_core::quanta::{Quantity, Unit};
use num_complex::Complex64;

use crate::{GaussianBeam, ImageError};

use super::super::decompose::Parameters;
use super::super::findsources::components::{PixelEllipse, SkyPlane, gaussian_area};
use super::super::region::SpatialShape;
use super::deconvolution::deconvolve;
use super::planes::{FitPlane, FitSelection};
use super::record::Measurement;
use super::summary::SummaryRow;
use super::uncertainty::{Noise, Uncertainty};

pub(super) struct FitGeometry<'a> {
    pub(super) sky: SkyPlane<'a>,
    pub(super) selection: &'a FitSelection,
    pub(super) channel: usize,
    pub(super) frequency: MFrequency,
}

pub(super) struct FitComponent {
    pub(super) shape: SkyComponent,
    pub(super) deconvolved: Option<SkyComponent>,
    pub(super) summary: SummaryRow,
    pub(super) measurement: Measurement,
}

impl FitGeometry<'_> {
    pub(super) fn component(
        &self,
        plane: &FitPlane,
        fit: &Parameters,
        fixed: [bool; 6],
        noise: &Noise,
    ) -> Result<FitComponent, ImageError> {
        let [peak, x, y, major, ratio, pa, ..] = *fit;
        let stride = self.selection.sky_stride.map(|stride| stride as f64);
        let pixels = [x, y];
        let absolute = std::array::from_fn(|i| {
            self.selection.region.start[self.selection.sky_axes[i]] as f64 + pixels[i] * stride[i]
        });
        let world = self.sky.world(absolute)?;
        let ellipse = self.sky.world_ellipse(
            absolute,
            world,
            PixelEllipse {
                major,
                minor: ratio * major,
                x_to_y: pa + FRAC_PI_2,
            },
            stride,
        )?;
        let angular = [
            ellipse.major_axis(),
            ellipse.minor_axis(),
            ellipse.position_angle(),
        ];
        let area = gaussian_area(angular[0], angular[1]);
        let (integrated, flux_unit) = self.sky.integrated_flux(peak, area);
        let level = noise.uncertainty(peak, integrated, angular, fixed);
        let direction = MDirection::from_angles(
            world[0],
            world[1],
            self.sky.direction.conversion_direction_ref(),
        );
        let shape = component_shape(ellipse, direction.clone(), &level)?;
        let stokes = stokes_index(self.selection.stokes);
        let mut value = [Complex64::ZERO; 4];
        let mut errors = value;
        value[stokes] = Complex64::new(integrated, 0.);
        errors[stokes] = Complex64::new(level.flux, 0.);
        let flux = Flux::new(value, flux_unit, Polarisation::Stokes)
            .map_err(invalid)?
            .with_errors(errors);
        let mut spectrum = SpectralModel::constant(self.frequency.clone());
        spectrum
            .set_frequency_unit(Unit::new("GHz").expect("standard unit"))
            .map_err(invalid)?;
        let shape = SkyComponent::new(flux, shape, spectrum);
        let [lon, lat] = [absolute[0], absolute[1]];
        let increments = self.sky.direction.increment();
        let pixel_area = (increments[0] * increments[1]).abs();
        let mut measurement = Measurement {
            channel: self.channel,
            pixels: if self.selection.sky_axes[0] < self.selection.sky_axes[1] {
                [lon, lat]
            } else {
                [lat, lon]
            },
            peak: [peak, level.peak],
            sum: plane.ellipse_sum(&SpatialShape::Ellipse {
                cx: x,
                cy: y,
                semi_major: angular[0] / (2. * increments[0].abs() * stride[0]),
                semi_minor: angular[1] / (2. * increments[1].abs() * stride[1]),
                cos_pa: angular[2].cos(),
                sin_pa: angular[2].sin(),
            }),
            brightness: self.sky.brightness.to_owned(),
            beam: self.sky.beam,
            beam_in_pixels: self.sky.beam.map_or(0., |beam| beam.area() / pixel_area),
            is_point: false,
            deconvolved_peak: None,
        };
        let mut summary = SummaryRow {
            channel: self.channel,
            peak: measurement.peak,
            flux: [integrated, level.flux],
            direction: world,
            direction_errors: level.direction,
            convolved: [
                angular[0],
                angular[1],
                angular[2],
                level.major,
                level.minor,
                level.angle,
            ],
            deconvolved: [0.; 6],
            frequency_hz: self.frequency.hz(),
        };
        let deconvolved = if let Some(beam) = self.sky.beam {
            let convolved = GaussianBeam::new(angular[0], angular[1], angular[2]);
            let mut result = shape.clone();
            let fitted = deconvolve(convolved, beam, &level);
            measurement.is_point = fitted.is_point;
            if let Some((source, axis_error)) = fitted.gaussian {
                let ellipse = Ellipse::new(source.major, source.minor, source.position_angle)
                    .map_err(invalid)?;
                *result.shape_mut() = component_shape(ellipse, direction, &axis_error)?;
                let ratio = area / source.area();
                if ratio < 1e6 {
                    measurement.deconvolved_peak = Some(measurement.peak.map(|peak| peak * ratio));
                }
                summary.deconvolved = [
                    source.major,
                    source.minor,
                    source.position_angle,
                    axis_error.major,
                    axis_error.minor,
                    axis_error.angle,
                ];
            } else {
                *result.shape_mut() = ComponentShape::point(direction)
                    .with_direction_error(shape.shape().direction_error().clone());
            }
            Some(result)
        } else {
            None
        };
        Ok(FitComponent {
            shape,
            deconvolved,
            summary,
            measurement,
        })
    }
}

fn component_shape(
    axes: Ellipse,
    direction: MDirection,
    errors: &Uncertainty,
) -> Result<ComponentShape, ImageError> {
    const ARCSEC_PER_RAD: f64 = 180. * 3600. / std::f64::consts::PI;
    let arcsec = Unit::new("arcsec").expect("standard unit");
    let longitude = Quantity::with_unit(errors.direction[0] * ARCSEC_PER_RAD, arcsec.clone());
    let latitude = Quantity::with_unit(errors.direction[1] * ARCSEC_PER_RAD, arcsec);
    let direction_error = DirectionError::new(latitude, longitude).map_err(invalid)?;
    let kind = ShapeKind::Gaussian {
        axes,
        errors: EllipseErrors {
            major_axis: errors.major,
            minor_axis: errors.minor,
            position_angle: errors.angle,
        },
    };
    ComponentShape::new(direction, kind)
        .map(|shape| shape.with_direction_error(direction_error))
        .map_err(invalid)
}

fn stokes_index(stokes: StokesType) -> usize {
    match stokes {
        StokesType::Q => 1,
        StokesType::U => 2,
        StokesType::V => 3,
        _ => 0,
    }
}

fn invalid(error: impl std::fmt::Display) -> ImageError {
    ImageError::CasaSelection(error.to_string())
}

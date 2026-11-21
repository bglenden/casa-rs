// SPDX-License-Identifier: LGPL-3.0-or-later

use casa_coordinates::{Coordinate, DirectionCoordinate};
use casa_types::quanta::{Quantity, Unit};

use crate::ImageError;

use super::super::region::point_in_polygon_f64;
use super::invalid;

enum Footprint {
    Box([[f64; 2]; 4]),
    Circle(f64),
}

pub(super) struct Window {
    footprint: Footprint,
    lower: [isize; 2],
    upper: [isize; 2],
}

impl Window {
    pub(super) fn new(
        direction: &DirectionCoordinate,
        shape: [usize; 2],
        x_length: &Quantity,
        y_length: Option<&Quantity>,
    ) -> Result<Self, ImageError> {
        let increments: [f64; 2] = direction
            .increment()
            .try_into()
            .map_err(|_| invalid("A direction coordinate with two pixel axes is required"))?;
        let absolute = increments.map(f64::abs);
        let radians = Unit::new("rad").expect("radian unit");
        let length = |q: &Quantity, axis: usize| -> Result<f64, ImageError> {
            let name = if axis == 0 { "xLength" } else { "yLength" };
            let unit = q.unit().name();
            if unit.starts_with("pix") {
                Ok(q.value() * absolute[axis])
            } else {
                q.get_value_in(&radians)
                    .map_err(|_| invalid(format!("{name} units {unit} must be pixel or angular")))
            }
        };
        let mut x = length(x_length, 0)?;
        let y = y_length.map(|q| length(q, 1)).transpose()?;
        if y.is_none() {
            x *= 0.5;
        }
        let mut pixels = [x / absolute[0], x / absolute[0]];
        if let Some(y) = y.filter(|&y| y > 0.) {
            pixels[1] = y / absolute[1];
        }
        for (axis, (&length, &extent)) in pixels.iter().zip(&shape).enumerate() {
            let name = if axis == 0 { "x" } else { "y" };
            if !length.is_finite() || length >= extent as f64 - 4. {
                return Err(invalid(format!(
                    "{name} region length is nearly as large as or larger than the subimage extent of {extent} pixels. Such a configuration is not supported"
                )));
            }
            if length <= 0.5 {
                return Err(invalid(format!(
                    "{name} region length is only {length} pixels. Such a configuration is not supported"
                )));
            }
        }
        if let Some(y) = y.filter(|&y| y > 0.) {
            let center = shape.map(|length| (length / 2) as f64);
            let [lon, lat] = direction.pixel_to_world(center)?;
            let mut corners = [[0.; 2]; 4];
            for ([sx, sy], pixel) in [[-1., -1.], [1., -1.], [1., 1.], [-1., 1.]]
                .into_iter()
                .zip(&mut corners)
            {
                let (s, c) = (sx * increments[0].signum() * x * 0.5).sin_cos();
                let (sd, cd) = (lat + sy * increments[1].signum() * y * 0.5).sin_cos();
                let at = [lon + s.atan2(c * cd), (c * sd).atan2((c * cd).hypot(s))];
                let projected = direction.world_to_pixel(at)?;
                *pixel = std::array::from_fn(|i| f64::from(projected[i] as f32) - center[i]);
            }
            let lower = std::array::from_fn(|i| {
                (corners.iter().map(|p| p[i]).fold(0., f64::min) - 1e-9).ceil() as isize
            });
            let upper = std::array::from_fn(|i| {
                (corners.iter().map(|p| p[i]).fold(0., f64::max) + 1e-9).floor() as isize
            });
            Ok(Self {
                footprint: Footprint::Box(corners),
                lower,
                upper,
            })
        } else {
            if x_length.unit().name() == "pix"
                && (absolute[0] - absolute[1]).abs() > 1e-13 * absolute[0].max(absolute[1])
            {
                return Err(invalid(
                    "AnnCircle should not be used when radius is in pixels and pixels are not square. Use AnnEllipse instead.",
                ));
            }
            let radius = pixels[0];
            let extent = (radius * (1. + 1e-7)).floor() as isize;
            Ok(Self {
                footprint: Footprint::Circle(radius),
                lower: [-extent; 2],
                upper: [extent; 2],
            })
        }
    }

    pub(super) fn bounds(
        &self,
        first: [usize; 2],
        last: [usize; 2],
        shape: [usize; 2],
    ) -> ([usize; 2], [usize; 2]) {
        let lower = std::array::from_fn(|i| first[i].saturating_add_signed(self.lower[i]));
        let upper = std::array::from_fn(|i| {
            last[i]
                .saturating_add_signed(self.upper[i])
                .min(shape[i] - 1)
                + 1
        });
        (lower, upper)
    }

    pub(super) fn is_single_pixel(&self) -> bool {
        self.lower == [0; 2] && self.upper == [0; 2]
    }

    pub(super) fn contains(&self, center: [usize; 2], at: [usize; 2]) -> bool {
        let dx = at[0] as f64 - center[0] as f64;
        let dy = at[1] as f64 - center[1] as f64;
        match &self.footprint {
            Footprint::Box(vertices) => point_in_polygon_f64(dx, dy, vertices),
            Footprint::Circle(radius) => dx * dx + dy * dy <= radius * radius * (1. + 1e-7),
        }
    }
}

// SPDX-License-Identifier: LGPL-3.0-or-later

use casa_coordinates::{Coordinate, DirectionCoordinate};
use casa_core::quanta::{Quantity, Unit};

use casa_images::ImageError;

use super::invalid;

enum Footprint {
    Box(Vec<Vec<[isize; 2]>>),
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
                *pixel = projected.map(|coordinate| coordinate as f32);
            }
            Self::rectangle(corners, shape, center.map(|pixel| pixel as isize))
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

    fn rectangle(
        corners: [[f32; 2]; 4],
        shape: [usize; 2],
        center: [isize; 2],
    ) -> Result<Self, ImageError> {
        let first: [isize; 2] = std::array::from_fn(|axis| {
            truncate_start(
                corners
                    .iter()
                    .map(|p| p[axis])
                    .fold(f32::INFINITY, f32::min),
            )
        });
        let last: [isize; 2] = std::array::from_fn(|axis| {
            truncate_end(
                corners
                    .iter()
                    .map(|p| p[axis])
                    .fold(f32::NEG_INFINITY, f32::max),
                shape[axis] as isize - 1,
            )
        });
        if (0..2).any(|i| last[i] < first[i]) {
            return Err(invalid("LCPolygon - entire polygon is outside the lattice"));
        }
        // LCPolygon converts projected corners to Float and rounds intersections
        // that are numerically on a pixel center. Keep its scanline convention
        // as a compact set of horizontal ranges.
        let segments: [([f32; 2], [f32; 2]); 4] =
            std::array::from_fn(|i| (corners[i], corners[(i + 1) % 4]));
        let mut previous = segments.iter().rposition(|&(a, b)| !near(a[1], b[1]));
        let mut edges = Vec::with_capacity(4);
        let mut rows = vec![Vec::new(); (last[1] - first[1] + 1) as usize];
        let append = |row: &mut Vec<[isize; 2]>, lo: f32, hi: f32| {
            let offset = first[0] - center[0];
            let lo = truncate_start(lo - first[0] as f32);
            let hi = truncate_end(hi - first[0] as f32, last[0] - first[0]);
            if hi >= lo {
                row.push([lo + offset, hi + offset]);
            }
        };
        for (i, ([x0, y0], [x1, y1])) in segments.into_iter().enumerate() {
            if near(y0, y1) {
                let y = y0 as isize;
                if (first[1]..=last[1]).contains(&y) && near(y as f32, y0) {
                    append(&mut rows[(y - first[1]) as usize], x0.min(x1), x0.max(x1));
                }
            } else {
                let prior = previous.expect("each sloped polygon edge has a predecessor");
                let (a, b) = segments[prior];
                let include_start = (b[1] > a[1]) != (y1 > y0);
                let slope = (x0 - x1) / (y0 - y1);
                edges.push((y0, y1, slope, x0 - slope * y0, include_start));
                previous = Some(i);
            }
        }
        for (y, row) in (first[1]..=last[1]).zip(&mut rows) {
            let y = y as f32;
            let mut cross = [0.; 4];
            let mut used = 0;
            for &(y0, y1, slope, offset, include_start) in &edges {
                let starts = near(y, y0);
                if (include_start && starts)
                    || (!starts && ((y > y0.min(y1) && y < y0.max(y1)) || near(y, y1)))
                {
                    cross[used] = slope * y + offset;
                    used += 1;
                }
            }
            cross[..used].sort_unstable_by(f32::total_cmp);
            for &[lo, hi] in cross[..used].as_chunks::<2>().0 {
                append(row, lo, hi);
            }
        }
        if rows.iter().all(Vec::is_empty) {
            return Err(invalid("LCPolygon - polygon does not contain any pixel"));
        }
        Ok(Self {
            footprint: Footprint::Box(rows),
            lower: std::array::from_fn(|i| first[i] - center[i]),
            upper: std::array::from_fn(|i| last[i] - center[i]),
        })
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
        let dx = at[0] as isize - center[0] as isize;
        let dy = at[1] as isize - center[1] as isize;
        match &self.footprint {
            Footprint::Box(rows) => rows.get((dy - self.lower[1]) as usize).is_some_and(|row| {
                row.iter()
                    .any(|&[first, last]| (first..=last).contains(&dx))
            }),
            Footprint::Circle(radius) => {
                (dx as f64).powi(2) + (dy as f64).powi(2) <= radius * radius * (1. + 1e-7)
            }
        }
    }
}

fn near(a: f32, b: f32) -> bool {
    let scale = if a == 0. || b == 0. {
        1.
    } else {
        a.abs().max(b.abs())
    };
    (a - b).abs() <= 1e-5 * scale
}

fn truncate_start(value: f32) -> isize {
    truncate_near(value, value + 1.).max(0)
}

fn truncate_end(value: f32, limit: isize) -> isize {
    truncate_near(value, value).min(limit)
}

fn truncate_near(value: f32, fallback: f32) -> isize {
    let integer = (value + 0.1).floor();
    if near(value, integer) {
        integer as isize
    } else {
        fallback as isize
    }
}

// SPDX-License-Identifier: LGPL-3.0-or-later
//! Direction-plane geometry for CASA's rotation and strip collapse.

use std::f64::consts::{FRAC_PI_2, PI};

use casa_coordinates::{Coordinate, DirectionCoordinate};
use ndarray::array;

use casa_images::ImageError;

pub(super) struct Strip {
    source: DirectionCoordinate,
    rotation: Option<DirectionCoordinate>,
    pub(super) lower: [i64; 2],
    pub(super) shape: [usize; 2],
}

impl Strip {
    pub(super) fn new(
        direction: &DirectionCoordinate,
        [start, end]: [[f64; 2]; 2],
        image_shape: [usize; 2],
        width: usize,
        include_half_pixel_ties: bool,
    ) -> Result<Self, ImageError> {
        if start == end {
            return Err(ImageError::CasaSelection(
                "Start and end pixels must be different".into(),
            ));
        }
        let dx = end[0] - start[0];
        let dy = end[1] - start[1];
        let angle = if dy == 0.0 {
            if dx > 0.0 { 0.0 } else { PI }
        } else {
            dx.atan2(dy) - FRAC_PI_2
        };
        let half = (width - 1) as f64 / 2.0;
        let margins = [-half * angle.sin(), half * angle.cos()];
        for axis in 0..2 {
            let lower = start[axis].min(end[axis]) - margins[axis].abs();
            let upper = start[axis].max(end[axis]) + margins[axis].abs();
            if !lower.is_finite()
                || !upper.is_finite()
                || lower < 2.0
                || upper > image_shape[axis] as f64 - 3.0
            {
                return Err(ImageError::CasaSelection(
                    "The endpoints and half width must remain at least two pixels from the image edges".into(),
                ));
            }
        }
        let mut source = direction.clone();
        let mut rotation = None;
        let mut first = start;
        let mut last = end;
        if (angle.to_degrees() % 360.0).abs() > 0.001 {
            let reference = rotation_origin(start, end);
            source = source
                .with_reference_pixel(reference)
                .with_reference_value(direction.pixel_to_world(reference)?);
            let (sin, cos) = angle.sin_cos();
            // Casacore stores the transpose of the FITS PC transform.
            let matrix = source.pc_matrix().dot(&array![[cos, sin], [-sin, cos]]);
            let rotated = source.clone().with_pc_matrix(matrix);
            first = rotated.world_to_pixel(source.pixel_to_world(start)?)?;
            last = rotated.world_to_pixel(source.pixel_to_world(end)?)?;
            rotation = Some(rotated);
        }
        let (lower_y, upper_y) = transverse_rows(
            first[1],
            last[1],
            half,
            rotation.is_some() && include_half_pixel_ties,
        );
        let lower = [first[0].round() as i64, lower_y];
        let upper = [last[0].round() as i64, upper_y];
        if lower[0] > upper[0] || lower[1] > upper[1] {
            return Err(ImageError::CasaSelection(
                "PV rotation produced an empty strip".into(),
            ));
        }
        let shape = [
            (upper[0] - lower[0] + 1) as usize,
            (upper[1] - lower[1] + 1) as usize,
        ];
        Ok(Self {
            source,
            rotation,
            lower,
            shape,
        })
    }

    pub(super) fn rotated(&self) -> bool {
        self.rotation.is_some()
    }

    pub(super) fn input_offsets(&self) -> Result<Vec<Vec<[f64; 2]>>, ImageError> {
        (0..self.shape[0])
            .map(|step| {
                (0..self.shape[1])
                    .map(|across| self.input_pixel(step, across))
                    .collect()
            })
            .collect()
    }

    pub(super) fn input_pixel(&self, x: usize, y: usize) -> Result<[f64; 2], ImageError> {
        let pixel = [
            self.lower[0] as f64 + x as f64,
            self.lower[1] as f64 + y as f64,
        ];
        match &self.rotation {
            Some(rotated) => Ok(self.source.world_to_pixel(rotated.pixel_to_world(pixel)?)?),
            None => Ok(pixel),
        }
    }

    pub(super) fn collapsed_direction(&self) -> Result<DirectionCoordinate, ImageError> {
        let direction = self.rotation.as_ref().unwrap_or(&self.source);
        let pixel = direction.reference_pixel();
        let lower = self.lower.map(|x| x as f64);
        let upper = [
            lower[0] + (self.shape[0] - 1) as f64,
            lower[1] + (self.shape[1] - 1) as f64,
        ];
        let lat_lower = direction.pixel_to_world(lower)?[1];
        let lat_upper = direction.pixel_to_world(upper)?[1];
        Ok(direction
            .clone()
            .with_reference_pixel([pixel[0] - self.lower[0] as f64, 0.0])
            .with_reference_value([
                direction.reference_value()[0],
                (lat_lower + lat_upper) / 2.0,
            ]))
    }

    pub(super) fn offset_increment(
        &self,
        direction: &DirectionCoordinate,
    ) -> Result<f64, ImageError> {
        let [lon0, lat0] = direction.pixel_to_world([0.0, 0.0])?;
        let count = self.shape[0] as f64;
        let [lon1, lat1] = direction.pixel_to_world([count, 0.0])?;
        let haversine = ((lat1 - lat0) / 2.0).sin().powi(2)
            + lat0.cos() * lat1.cos() * ((lon1 - lon0) / 2.0).sin().powi(2);
        Ok(2.0 * haversine.clamp(0.0, 1.0).sqrt().asin() / count)
    }
}

fn transverse_rows(first: f64, last: f64, half_width: f64, include_ties: bool) -> (i64, i64) {
    // Both endpoints of a rotated strip should occupy the same row. At a
    // half-pixel center, both neighboring rows are equally close. Include
    // both ties at the transverse boundaries independent of WCS roundoff.
    let midpoint = (first + last) / 2.0;
    let tie = midpoint.floor() + 0.5;
    const WCS_ROUNDOFF: f64 = 1e-7;
    if include_ties && (first - tie).abs() < WCS_ROUNDOFF && (last - tie).abs() < WCS_ROUNDOFF {
        (
            (tie - half_width).floor() as i64,
            (tie + half_width).ceil() as i64,
        )
    } else {
        (
            (first - half_width).round() as i64,
            (last + half_width).round() as i64,
        )
    }
}

fn rotation_origin(start: [f64; 2], end: [f64; 2]) -> [f64; 2] {
    let midpoint = [(start[0] + end[0]) / 2.0, (start[1] + end[1]) / 2.0];
    // PVGenerator first tries an integer row near the midpoint. Keep the
    // exact midpoint when the trial point is outside its reference bounds.
    if (end[1] - end[0]).abs() <= 1.0 {
        return midpoint;
    }
    let y = midpoint[1].trunc();
    let x = if (y - midpoint[1]).abs() <= 1e-8 * midpoint[1].abs() {
        midpoint[0]
    } else {
        (start[0] * (end[1] - y) + end[0] * (y - start[1])) / (end[1] - start[1])
    };
    if (start[0].min(start[1])..=start[0].max(start[1])).contains(&x) {
        [x, y]
    } else {
        midpoint
    }
}

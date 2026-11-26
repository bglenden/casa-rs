// SPDX-License-Identifier: LGPL-3.0-or-later

use casa_images::ImageError;

use super::super::region::{SpatialShape, shape_contains};
use super::planes::FitPlane;

#[derive(Default)]
pub(super) struct Statistics {
    count: usize,
    mean: f64,
    variance: f64,
    squares: f64,
}

impl Statistics {
    fn add(&mut self, value: f64) {
        self.count += 1;
        let delta = value - self.mean;
        self.mean += delta / self.count as f64;
        self.variance += delta * (value - self.mean);
        self.squares += value * value;
    }

    pub(super) fn sigma(&self) -> f64 {
        (self.variance / self.count.saturating_sub(1) as f64).sqrt()
    }

    pub(super) fn rms(&self) -> f64 {
        (self.squares / self.count as f64).sqrt()
    }
}

pub(super) struct FitStatistics {
    pub(super) used: usize,
    pub(super) input: Statistics,
    pub(super) residual: Statistics,
}

pub(super) enum IntensityRange {
    Any,
    Include([f32; 2]),
    Exclude([f32; 2]),
}

impl IntensityRange {
    pub(super) fn new(include: &[f64], exclude: &[f64]) -> Result<Self, ImageError> {
        match (bounds(include), bounds(exclude)) {
            (Some(_), Some(_)) => Err(ImageError::CasaSelection(
                "You cannot give both an include and an exclude pixel range".into(),
            )),
            (Some(bounds), None) => Ok(Self::Include(bounds)),
            (None, Some(bounds)) => Ok(Self::Exclude(bounds)),
            (None, None) => Ok(Self::Any),
        }
    }

    fn includes(&self, value: f32) -> bool {
        match *self {
            Self::Any => true,
            Self::Include([low, high]) => value >= low && value <= high,
            Self::Exclude([low, high]) => value < low || value > high,
        }
    }
}

fn bounds(values: &[f64]) -> Option<[f32; 2]> {
    let [a, b] = match values {
        [-1.] => return None,
        [only] => [*only as f32; 2],
        [a, b] => [*a as f32, *b as f32],
        _ => return None,
    };
    let near = a == b || (a - b).abs() <= 1e-5 * a.abs().max(b.abs());
    Some(if near {
        [-a.abs(), a.abs()]
    } else {
        [a.min(b), a.max(b)]
    })
}

impl FitPlane {
    pub(super) fn selected_samples<'a>(
        &'a self,
        range: &'a IntensityRange,
    ) -> impl Iterator<Item = ([f64; 2], f32)> + Clone + 'a {
        let width = self.shape[0];
        self.values
            .iter()
            .copied()
            .zip(self.validity.iter().copied())
            .enumerate()
            .filter(move |(_, (value, valid))| *valid && range.includes(*value))
            .map(move |(index, (value, _))| {
                ([(index % width) as f64, (index / width) as f64], value)
            })
    }

    pub(super) fn selected_rms(&self, model_value: impl Fn([f64; 2]) -> f64) -> f64 {
        let mut sum = 0.;
        let mut count = 0;
        for (position, value) in self.selected_samples(&IntensityRange::Any) {
            sum += (f64::from(value) - model_value(position)).powi(2);
            count += 1;
        }
        (sum / f64::from(count)).sqrt()
    }

    pub(super) fn statistics(
        &self,
        range: &IntensityRange,
        model: impl Fn([f64; 2]) -> f64,
    ) -> FitStatistics {
        let mut input = Statistics::default();
        let mut residual = Statistics::default();
        let mut used = 0;
        for (position, pixel) in self.selected_samples(&IntensityRange::Any) {
            used += usize::from(range.includes(pixel));
            let pixel = f64::from(pixel);
            input.add(pixel);
            residual.add(pixel - model(position));
        }
        FitStatistics {
            used,
            input,
            residual,
        }
    }

    pub(super) fn ellipse_sum(&self, aperture: &SpatialShape) -> f64 {
        self.selected_samples(&IntensityRange::Any)
            .filter(|&([x, y], _)| shape_contains(aperture, x as usize, y as usize))
            .map(|(_, pixel)| f64::from(pixel))
            .sum()
    }
}

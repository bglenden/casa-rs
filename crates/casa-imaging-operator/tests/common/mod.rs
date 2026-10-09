// SPDX-License-Identifier: LGPL-3.0-or-later
//! Shared fixtures for the operator laws: a small standard operator and a
//! deterministic sample generator.

#![allow(dead_code)]

use casa_imaging_model::{CorrelationType, PolarizationCoordinate};
use casa_imaging_operator::{
    Basis, CfKey, GridGeometry, GridPadding, GridPrecision, ImageExtent, MeasurementOperator,
    Placement, PolarizationRouting, SampleBuffer, Spheroidal,
};
use num_complex::{Complex32, Complex64};

pub const IMAGE: usize = 64;
pub const INCREMENT_RAD: [f64; 2] = [-2.0e-5, 2.0e-5];

pub fn geometry() -> GridGeometry {
    GridGeometry::new(
        ImageExtent {
            shape: [IMAGE, IMAGE],
            increment_rad: INCREMENT_RAD,
            reference_pixel: [IMAGE / 2, IMAGE / 2],
        },
        GridPadding::CasaComposite,
    )
    .expect("valid geometry")
}

pub fn operator(
    precision: GridPrecision,
    basis: Basis,
    correlations: &[CorrelationType],
    requested: &[PolarizationCoordinate],
) -> MeasurementOperator {
    let geometry = geometry();
    let polarization = PolarizationRouting::compile(correlations, requested).expect("routing");
    let cf = Spheroidal::new(&geometry, &polarization);
    MeasurementOperator::new(geometry, basis, polarization, Box::new(cf), precision)
}

/// xorshift64*: deterministic, dependency-free.
pub struct Rng(u64);

impl Rng {
    pub fn new(seed: u64) -> Self {
        Self(seed.max(1))
    }

    pub fn next_u64(&mut self) -> u64 {
        let mut x = self.0;
        x ^= x >> 12;
        x ^= x << 25;
        x ^= x >> 27;
        self.0 = x;
        x.wrapping_mul(0x2545_F491_4F6C_DD1D)
    }

    /// Uniform in `[0, 1)`.
    pub fn unit(&mut self) -> f64 {
        (self.next_u64() >> 11) as f64 / (1u64 << 53) as f64
    }

    /// Uniform in `[-1, 1)`.
    pub fn signed(&mut self) -> f64 {
        2.0 * self.unit() - 1.0
    }

    pub fn complex(&mut self) -> Complex64 {
        Complex64::new(self.signed(), self.signed())
    }
}

/// Random placements whose seven-tap support fits the padded grid, with
/// random phases and `w` signs, on `planes` planes.
pub fn placements(
    geometry: &GridGeometry,
    count: usize,
    planes: u32,
    rng: &mut Rng,
) -> Vec<Placement> {
    let [nx, ny] = geometry.grid_shape();
    let [sx, sy] = geometry.scale();
    let mut out = Vec::with_capacity(count);
    while out.len() < count {
        let u = rng.signed() * 0.45 * nx as f64 / sx.abs();
        let v = rng.signed() * 0.45 * ny as f64 / sy.abs();
        let location = geometry.locate(u, v, 100);
        if !geometry.fits(location, [3, 3]) {
            continue;
        }
        let w = rng.signed() * 50.0;
        out.push(Placement {
            u,
            v,
            w,
            prediction_w_positive: w > 0.0,
            phase: rng.signed() * std::f64::consts::PI,
            plane: (rng.next_u64() % u64::from(planes)) as u32,
            spectral: 0.0,
            cf: CfKey::default(),
            gradient: [0.0, 0.0],
        });
    }
    out
}

/// Random raw visibilities (per placement and polarization) and weights.
pub fn samples(placements: &[Placement], npol: usize, rng: &mut Rng) -> (Vec<Complex64>, Vec<f64>) {
    let values = (0..placements.len() * npol)
        .map(|_| rng.complex())
        .collect();
    let weights = (0..placements.len() * npol)
        .map(|_| 0.5 + 1.5 * rng.unit())
        .collect();
    (values, weights)
}

/// Pre-multiplied block: `W · V · e^{iφ}`.
pub fn buffer(
    placements: &[Placement],
    values: &[Complex64],
    weights: &[f64],
    npol: usize,
) -> SampleBuffer {
    let mut buffer = SampleBuffer::new(npol);
    let mut block_values = vec![Complex32::default(); npol];
    let mut block_weights = vec![0.0_f32; npol];
    for (index, placement) in placements.iter().enumerate() {
        for pol in 0..npol {
            let weight = weights[index * npol + pol];
            let value = values[index * npol + pol] * Complex64::from_polar(weight, placement.phase);
            block_values[pol] = Complex32::new(value.re as f32, value.im as f32);
            block_weights[pol] = weight as f32;
        }
        buffer.push(*placement, &block_values, &block_weights);
    }
    buffer
}

pub fn max_abs(values: impl Iterator<Item = f32>) -> f64 {
    values.fold(0.0_f64, |peak, value| peak.max(f64::from(value).abs()))
}

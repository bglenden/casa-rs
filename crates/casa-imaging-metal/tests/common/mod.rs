// SPDX-License-Identifier: LGPL-3.0-or-later
//! Kernel sets, operators and samples for the Metal-versus-CPU laws.

#![allow(dead_code, reason = "each law file uses a subset of the fixtures")]

use casa_imaging_metal::MetalBackend;
use casa_imaging_model::{CorrelationType, PolarizationCoordinate};
use casa_imaging_operator::{
    Basis, CellHold, CfKey, ConvolutionFunctionSet, GridAccumulator, GridGeometry, GridPadding,
    GridPrecision, GridScalar, ImageCorrection, ImageExtent, KernelNormalisation,
    MeasurementOperator, ModelImages, ModelPlane, MuellerRouting, Placement, PolarizationRouting,
    RowContext, SampleBuffer, Spheroidal, TapLayout, Tile,
};
use ndarray::Array2;
use num_complex::{Complex32, Complex64};

pub const IMAGE: usize = 64;
const INCREMENT_RAD: [f64; 2] = [-2.0e-5, 2.0e-5];
const XX_YY: [CorrelationType; 2] = [CorrelationType::LinearXx, CorrelationType::LinearYy];
const XX_YY_REQUESTED: [PolarizationCoordinate; 2] = [
    PolarizationCoordinate::LinearXx,
    PolarizationCoordinate::LinearYy,
];

/// `None` (and a note on stderr) when the host has no Metal device.
pub fn metal(operator: &MeasurementOperator) -> Option<MetalBackend<'_>> {
    match MetalBackend::new(operator.cf()) {
        Ok(backend) => Some(backend),
        Err(error) => {
            eprintln!("skipped: {error}");
            None
        }
    }
}

pub fn geometry() -> GridGeometry {
    geometry_of(IMAGE)
}

fn geometry_of(image: usize) -> GridGeometry {
    GridGeometry::new(
        ImageExtent {
            shape: [image, image],
            increment_rad: INCREMENT_RAD,
            reference_pixel: [image / 2, image / 2],
        },
        GridPadding::CasaComposite,
    )
    .expect("valid geometry")
}

fn routing() -> PolarizationRouting {
    PolarizationRouting::compile(&XX_YY, &XX_YY_REQUESTED).expect("routing")
}

fn correction() -> ImageCorrection {
    correction_of(IMAGE)
}

fn correction_of(image: usize) -> ImageCorrection {
    let template = Spheroidal::new(&geometry_of(image), &routing());
    let correction = template.image_correction();
    ImageCorrection::new(correction.x().to_vec(), correction.y().to_vec())
}

/// An `f32` operator over an `image × image` grid with complex dense taps
/// of `support` taps per axis, for throughput measurements.
pub fn wide_dense_operator(image: usize, support: u16, rng: &mut Rng) -> MeasurementOperator {
    let mut cf = Dense::new(support, 8, rng);
    cf.correction = correction_of(image);
    MeasurementOperator::new(
        geometry_of(image),
        Basis::Constant,
        routing(),
        Box::new(cf),
        GridPrecision::F32,
    )
}

/// The kernel sets every law runs on.
#[derive(Clone, Copy, Debug)]
pub enum Kernels {
    /// The standard seven-tap spheroidal set.
    Spheroidal,
    /// A five-tap separable set with random rows and weight taps.
    Separable,
    /// Complex dense taps on two Mueller planes with leakage routing.
    Dense,
}

pub const ALL_KERNELS: [Kernels; 3] = [Kernels::Spheroidal, Kernels::Separable, Kernels::Dense];

/// An `f32` operator over `[XX, YY]` with the kernel set `kernels`.
pub fn operator(kernels: Kernels, basis: Basis, rng: &mut Rng) -> MeasurementOperator {
    let cf: Box<dyn ConvolutionFunctionSet> = match kernels {
        Kernels::Spheroidal => Box::new(Spheroidal::new(&geometry(), &routing())),
        Kernels::Separable => Box::new(Separable::new(5, 4, rng)),
        Kernels::Dense => Box::new(Dense::new(5, 4, rng)),
    };
    MeasurementOperator::new(geometry(), basis, routing(), cf, GridPrecision::F32)
}

/// A separable real kernel of odd `support` with random positive rows; its
/// weight taps are the same rows.
struct Separable {
    rows: Vec<f32>,
    support: u16,
    oversampling: u16,
    mueller: MuellerRouting,
    correction: ImageCorrection,
}

impl Separable {
    fn new(support: u16, oversampling: u16, rng: &mut Rng) -> Self {
        let count = (usize::from(oversampling) + 1) * usize::from(support);
        Self {
            rows: (0..count).map(|_| 0.1 + rng.unit() as f32).collect(),
            support,
            oversampling,
            mueller: MuellerRouting::scalar(&[Some(0), Some(1)], 2),
            correction: correction(),
        }
    }

    fn layout(&self) -> TapLayout<'_> {
        TapLayout::SeparableReal {
            rows: &self.rows,
            support: self.support,
            oversampling: self.oversampling,
        }
    }
}

impl ConvolutionFunctionSet for Separable {
    fn key(&self, _row: &RowContext, _freq_hz: f64, _w_lambda: f64) -> CfKey {
        CfKey::default()
    }
    fn taps<'s>(&'s self, _key: CfKey, _hold: &'s mut CellHold) -> TapLayout<'s> {
        self.layout()
    }
    fn max_half_support(&self) -> [u16; 2] {
        [self.support / 2; 2]
    }
    fn weight_taps<'s>(&'s self, _key: CfKey, _hold: &'s mut CellHold) -> Option<TapLayout<'s>> {
        Some(self.layout())
    }
    fn mueller(&self) -> &MuellerRouting {
        &self.mueller
    }
    fn image_correction(&self) -> &ImageCorrection {
        &self.correction
    }
    fn normalisation(&self) -> KernelNormalisation {
        KernelNormalisation::KernelSum
    }
    fn pointing_ramp(&self) -> bool {
        true
    }
}

/// Complex random taps on two Mueller planes per cell, for two cells
/// (`CfKey { cube: 0 | 1 }`). Visibility XX reads grid XX through plane 0
/// and grid YY through plane 1 (YY symmetrically), and the conjugate table
/// swaps the planes, so the w-sign swap is exercised.
struct Dense {
    cells: [Vec<Complex32>; 2],
    support: u16,
    oversampling: u16,
    mueller: MuellerRouting,
    correction: ImageCorrection,
}

impl Dense {
    fn new(support: u16, oversampling: u16, rng: &mut Rng) -> Self {
        let fine = usize::from(oversampling) + 1;
        let count = fine * fine * 2 * usize::from(support).pow(2);
        let mut cell = || {
            (0..count)
                .map(|_| Complex32::new(rng.signed() as f32, rng.signed() as f32))
                .collect()
        };
        let leak =
            |own: u8, other: u8| vec![vec![Some(own), Some(other)], vec![Some(other), Some(own)]];
        Self {
            cells: [cell(), cell()],
            support,
            oversampling,
            mueller: MuellerRouting {
                direct: leak(0, 1),
                conjugate: leak(1, 0),
            },
            correction: correction(),
        }
    }
}

impl ConvolutionFunctionSet for Dense {
    fn key(&self, _row: &RowContext, _freq_hz: f64, _w_lambda: f64) -> CfKey {
        CfKey::default()
    }
    fn taps<'s>(&'s self, key: CfKey, _hold: &'s mut CellHold) -> TapLayout<'s> {
        TapLayout::Dense {
            data: &self.cells[usize::from(key.cube)],
            support: [self.support, self.support],
            oversampling: self.oversampling,
            mueller_planes: 2,
        }
    }
    fn max_half_support(&self) -> [u16; 2] {
        [self.support / 2; 2]
    }
    fn weight_taps<'s>(&'s self, key: CfKey, hold: &'s mut CellHold) -> Option<TapLayout<'s>> {
        Some(self.taps(
            CfKey {
                group: 0,
                cube: 1 - key.cube,
            },
            hold,
        ))
    }
    fn mueller(&self) -> &MuellerRouting {
        &self.mueller
    }
    fn normalisation(&self) -> KernelNormalisation {
        KernelNormalisation::KernelSum
    }
    fn pointing_ramp(&self) -> bool {
        true
    }
    fn image_correction(&self) -> &ImageCorrection {
        &self.correction
    }
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
}

/// Random placements whose support lies inside `tile` on `planes` planes,
/// with random `w` signs, phases, Taylor variables and (for dense kernels)
/// pointing gradients.
pub fn placements(
    operator: &MeasurementOperator,
    tile: Tile,
    count: usize,
    planes: u32,
    rng: &mut Rng,
) -> Vec<Placement> {
    let geometry = operator.geometry();
    let [nx, ny] = geometry.grid_shape();
    let [sx, sy] = geometry.scale();
    let mut hold = CellHold::new();
    let dense = matches!(
        operator.cf().taps(CfKey::default(), &mut hold),
        TapLayout::Dense { .. }
    );
    let mut out = Vec::with_capacity(count);
    while out.len() < count {
        let cf = CfKey {
            group: 0,
            cube: if dense {
                (rng.next_u64() % 2) as u16
            } else {
                0
            },
        };
        let taps = operator.cf().taps(cf, &mut hold);
        let half = taps.half_support().map(i64::from);
        let u = rng.signed() * 0.5 * nx as f64 / sx.abs();
        let v = rng.signed() * 0.5 * ny as f64 / sy.abs();
        let location = geometry.locate(u, v, taps.oversampling());
        let inside = |anchor: i64, half: i64, origin: usize, extent: usize| {
            anchor - half >= origin as i64 && anchor + half < (origin + extent) as i64
        };
        if !inside(location.x, half[0], tile.origin[0], tile.shape[0])
            || !inside(location.y, half[1], tile.origin[1], tile.shape[1])
        {
            continue;
        }
        out.push(Placement {
            u,
            v,
            w: rng.signed() * 50.0,
            phase: rng.signed() * std::f64::consts::PI,
            plane: (rng.next_u64() % u64::from(planes)) as u32,
            spectral: (0.3 * rng.signed()) as f32,
            cf,
            gradient: if dense {
                [rng.signed() as f32, rng.signed() as f32]
            } else {
                [0.0, 0.0]
            },
        });
    }
    out
}

/// A block of `W · V · e^{iφ}` with random visibilities and weights.
pub fn block(placements: &[Placement], rng: &mut Rng) -> SampleBuffer {
    let mut buffer = SampleBuffer::new(2);
    for placement in placements {
        let mut values = [Complex32::default(); 2];
        let mut weights = [0.0_f32; 2];
        for pol in 0..2 {
            let weight = 0.5 + 1.5 * rng.unit();
            let value = Complex64::new(rng.signed(), rng.signed())
                * Complex64::from_polar(weight, placement.phase);
            values[pol] = Complex32::new(value.re as f32, value.im as f32);
            weights[pol] = weight as f32;
        }
        buffer.push(*placement, &values, &weights);
    }
    buffer
}

/// Random model images for every plane and term of `operator`.
pub fn model(operator: &MeasurementOperator, rng: &mut Rng) -> ModelImages {
    let basis = operator.basis();
    let images = basis.data_terms() * 2;
    ModelImages {
        first_plane: 0,
        planes: (0..basis.planes())
            .map(|_| ModelPlane {
                images: (0..images)
                    .map(|_| Array2::from_shape_fn((IMAGE, IMAGE), |_| rng.signed() as f32))
                    .collect(),
            })
            .collect(),
    }
}

/// `max |metal − cpu| ≤ 1e-4 · max |cpu|` over two cell or sample arrays.
pub fn assert_close(metal: &[Complex32], cpu: &[Complex32], label: &str) {
    assert_eq!(metal.len(), cpu.len(), "{label}: lengths");
    let scale = cpu
        .iter()
        .fold(0.0_f32, |peak, value| peak.max(value.norm()));
    let worst = metal
        .iter()
        .zip(cpu)
        .fold(0.0_f32, |worst, (m, c)| worst.max((m - c).norm()));
    assert!(scale > 0.0, "{label}: the reference is empty");
    assert!(
        worst <= 1.0e-4 * scale,
        "{label}: largest difference {worst} against a peak of {scale}"
    );
}

/// Grid cells and `sumwt` of both accumulators agree: cells to 1e-4 of the
/// peak, `sumwt` exactly (both accumulate it in `f64` in sample order).
pub fn assert_same_grids(metal: &GridAccumulator, cpu: &GridAccumulator, label: &str) {
    assert_close(
        <f32 as GridScalar>::cells(metal.storage()),
        <f32 as GridScalar>::cells(cpu.storage()),
        label,
    );
    assert_eq!(metal.sumwt(), cpu.sumwt(), "{label}: sumwt");
}

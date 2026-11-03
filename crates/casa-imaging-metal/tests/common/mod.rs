// SPDX-License-Identifier: LGPL-3.0-or-later
//! Kernel sets, operators and samples for the Metal-versus-CPU laws.

#![allow(dead_code, reason = "each law file uses a subset of the fixtures")]

use casa_imaging_metal::MetalBackend;
use casa_imaging_model::PolarizationCoordinate;
use casa_imaging_operator::{
    AiryDish, Basis, CellHold, CfKey, ConvolutionFunctionSet, DenseCell, DeviceFailure,
    GridAccumulator, GridGeometry, GridPadding, GridPrecision, GridScalar, ImageCorrection,
    ImageExtent, KernelNormalisation, MeasurementOperator, ModelImages, ModelPlane, MosaicPb,
    MosaicRules, MosaicWindow, MuellerRouting, OperatorError, PaddingFactor, Placement,
    PolarizationRouting, PrimaryBeam, RowContext, SampleBuffer, Spheroidal, TapFootprint,
    TapLayout, Tile, WPlaneCount, WPlanes, pair_plane,
};
use casa_types::Correlation;
use ndarray::Array2;
use num_complex::{Complex32, Complex64};

pub const IMAGE: usize = 64;
const INCREMENT_RAD: [f64; 2] = [-2.0e-5, 2.0e-5];
const XX_YY: [Correlation; 2] = [Correlation::XX, Correlation::YY];
const XX_YY_REQUESTED: [PolarizationCoordinate; 2] = [
    PolarizationCoordinate::LinearXx,
    PolarizationCoordinate::LinearYy,
];

/// `None` (and a note on stderr) only when the host has no Metal device;
/// any other failure to make the backend is the law's (#793 review, Astra).
pub fn metal(operator: &MeasurementOperator) -> Option<MetalBackend<'_>> {
    match MetalBackend::new(operator.cf(), false) {
        Ok(backend) => Some(backend),
        Err(OperatorError::Device(DeviceFailure::Unavailable)) => {
            eprintln!("skipped: no Metal device");
            None
        }
        Err(error) => panic!("the Metal backend could not be made: {error}"),
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
        GridPadding::CasaComposite(PaddingFactor::TCLEAN),
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
    /// The dense set with eleven taps per axis: wider than one thread
    /// serves, so each sample takes a SIMD group.
    WideDense,
    /// The real heterogeneous ALMA mosaic primary beams (`MosaicPb`, #787):
    /// dense complex cells ramped by each placement's pointing gradient,
    /// weight cells of the imaging cells' support, for the 12 m and 7 m
    /// dish classes (three pair planes, `CfKey::group`) at two frequency
    /// cells an octave apart ([`MOSAIC_FREQUENCIES_HZ`], `CfKey::cube`), on
    /// a [`MOSAIC_IMAGE`]² image wide enough for the keys' supports to
    /// differ (half supports 6, 7 and 9;
    /// `the_mosaic_sets_cells_differ_in_support_but_share_it_across_modes`).
    Mosaic,
}

pub const ALL_KERNELS: [Kernels; 5] = [
    Kernels::Spheroidal,
    Kernels::Separable,
    Kernels::Dense,
    Kernels::WideDense,
    Kernels::Mosaic,
];

/// The mosaic fixture's dish diameters, CASA's ALMA 12 m and ACA 7 m
/// classes in CASA's order.
pub const MOSAIC_DISHES_M: [f64; 2] = [12.0, 7.0];
/// The mosaic fixture's frequency cells, one spectral window each.
pub const MOSAIC_FREQUENCIES_HZ: [f64; 2] = [0.5e11, 1.0e11];
/// The mosaic fixture's image side: at [`IMAGE`] the screen's lattice is
/// too small for any beam's support to pass the six-tap floor, so every
/// key would lend one support.
pub const MOSAIC_IMAGE: usize = 96;

impl Kernels {
    /// Every cell key the set lends: one for the single-cell sets, the two
    /// cubes of the dense sets, and the mosaic's pair planes by frequency
    /// cell.
    pub fn keys(self) -> Vec<CfKey> {
        match self {
            Self::Spheroidal | Self::Separable => vec![CfKey::default()],
            Self::Dense | Self::WideDense => (0..2).map(|cube| CfKey { group: 0, cube }).collect(),
            Self::Mosaic => {
                let dishes = MOSAIC_DISHES_M.len();
                let mut keys = Vec::new();
                for first in 0..dishes {
                    for second in first..dishes {
                        let group = u16::try_from(pair_plane([first as u8, second as u8], dishes))
                            .expect("pair plane fits u16");
                        for cube in 0..MOSAIC_FREQUENCIES_HZ.len() as u16 {
                            keys.push(CfKey { group, cube });
                        }
                    }
                }
                keys
            }
        }
    }
}

/// An `f32` operator over `[XX, YY]` with the kernel set `kernels`, on the
/// [`IMAGE`]² image, or the [`MOSAIC_IMAGE`]² one for the mosaic set.
pub fn operator(kernels: Kernels, basis: Basis, rng: &mut Rng) -> MeasurementOperator {
    let (cf, geometry): (Box<dyn ConvolutionFunctionSet>, _) = match kernels {
        Kernels::Spheroidal => (
            Box::new(Spheroidal::new(&geometry(), &routing())),
            geometry(),
        ),
        Kernels::Separable => (Box::new(Separable::new(5, 4, rng)), geometry()),
        Kernels::Dense => (Box::new(Dense::new(5, 4, rng)), geometry()),
        Kernels::WideDense => (Box::new(Dense::new(11, 4, rng)), geometry()),
        Kernels::Mosaic => (Box::new(mosaic_set()), geometry_of(MOSAIC_IMAGE)),
    };
    MeasurementOperator::new(geometry, basis, routing(), cf, GridPrecision::F32)
}

/// The heterogeneous ALMA mosaic set on the [`MOSAIC_IMAGE`]² grid
/// ([`Kernels::Mosaic`]): one spectral window a frequency cell, the screen
/// sized at the lowest frequency, whose beams are the widest.
fn mosaic_set() -> MosaicPb {
    let geometry = geometry_of(MOSAIC_IMAGE);
    let window = |spectral_window: u32, hz: f64| MosaicWindow {
        spectral_window,
        window_frequencies_hz: vec![hz - 1.0e8, hz],
        channel_width_hz: 1.0e6,
        selected_frequencies_hz: vec![hz],
    };
    let dishes = MOSAIC_DISHES_M
        .map(|diameter_m| PrimaryBeam::from(AiryDish::casa_alma(diameter_m, &geometry)));
    let windows = MOSAIC_FREQUENCIES_HZ
        .iter()
        .enumerate()
        .map(|(index, &hz)| window(index as u32, hz))
        .collect::<Vec<_>>();
    MosaicPb::new(
        MosaicRules::Tclean,
        &geometry,
        &routing(),
        MOSAIC_FREQUENCIES_HZ[0],
        &dishes,
        &windows,
    )
    .expect("mosaic set")
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
    /// The rows, lent as the imaging and the weight taps.
    fn tap_footprint(&self) -> TapFootprint {
        TapFootprint::of([self.layout(), self.layout()])
    }
}

/// Complex random taps on two Mueller planes per cell, for two cells
/// (`CfKey { cube: 0 | 1 }`). Visibility XX reads grid XX through plane 0
/// and grid YY through plane 1 (YY symmetrically). Independent tables for
/// each direction and w sign exercise all four routing choices.
struct Dense {
    cells: [DenseCell; 2],
    support: u16,
    mueller: MuellerRouting,
    correction: ImageCorrection,
}

impl Dense {
    fn new(support: u16, oversampling: u16, rng: &mut Rng) -> Self {
        let fine = usize::from(oversampling) + 1;
        let count = fine * fine * 2 * usize::from(support).pow(2);
        let mut cell = || {
            let data = (0..count)
                .map(|_| Complex32::new(rng.signed() as f32, rng.signed() as f32))
                .collect();
            DenseCell::new(data, [support, support], oversampling, 2)
        };
        let leak =
            |own: u8, other: u8| vec![vec![Some(own), Some(other)], vec![Some(other), Some(own)]];
        Self {
            cells: [cell(), cell()],
            support,
            mueller: MuellerRouting {
                adjoint: [leak(1, 0), leak(0, 1)],
                // Exercise forward routes that are not a swap of the
                // adjoint tables, as CASA's AW routing requires.
                forward: [leak(0, 0), leak(1, 1)],
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
        self.cells[usize::from(key.cube)].layout()
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
    /// Both cells, each lent as a cube's imaging taps and the other cube's
    /// weight taps.
    fn tap_footprint(&self) -> TapFootprint {
        TapFootprint::of(self.cells.iter().chain(&self.cells).map(DenseCell::layout))
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

/// Whether the set's cells are dense (and so carry a pointing ramp).
fn dense_cells(operator: &MeasurementOperator, keys: &[CfKey]) -> bool {
    let mut hold = CellHold::new();
    matches!(
        operator.cf().taps(keys[0], &mut hold),
        TapLayout::Dense { .. }
    )
}

/// One random placement of cell `cf`, with a random `w` sign, phase, Taylor
/// variable and (for dense kernels) pointing gradient, on one of `planes`.
fn placement(cf: CfKey, u: f64, v: f64, planes: u32, dense: bool, rng: &mut Rng) -> Placement {
    Placement {
        u,
        v,
        w: rng.signed() * 50.0,
        // Independent of the sign of `w`, as an AW row's can be.
        prediction_w_positive: rng.next_u64().is_multiple_of(2),
        phase: rng.signed() * std::f64::consts::PI,
        plane: (rng.next_u64() % u64::from(planes)) as u32,
        spectral: (0.3 * rng.signed()) as f32,
        cf,
        gradient: if dense {
            [rng.signed() as f32, rng.signed() as f32]
        } else {
            [0.0, 0.0]
        },
    }
}

/// Random placements of the cells `keys`, drawn evenly, whose support lies
/// inside `tile` on `planes` planes, with random `w` signs, phases, Taylor
/// variables and (for dense kernels) pointing gradients.
pub fn placements(
    operator: &MeasurementOperator,
    keys: &[CfKey],
    tile: Tile,
    count: usize,
    planes: u32,
    rng: &mut Rng,
) -> Vec<Placement> {
    let geometry = operator.geometry();
    let [nx, ny] = geometry.grid_shape();
    let [sx, sy] = geometry.scale();
    let mut hold = CellHold::new();
    let dense = dense_cells(operator, keys);
    let mut out = Vec::with_capacity(count);
    while out.len() < count {
        let cf = keys[(rng.next_u64() % keys.len() as u64) as usize];
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
        out.push(placement(cf, u, v, planes, dense, rng));
    }
    out
}

/// For every cell of `keys`, the placements whose last tap touches each of
/// `tile`'s four edges, anchored at a cell centre on the tile's middle row
/// or column, one per sign of `w` (#793 review): the samples the on-grid
/// test of each mode decides.
pub fn edge_placements(
    operator: &MeasurementOperator,
    keys: &[CfKey],
    tile: Tile,
    rng: &mut Rng,
) -> Vec<Placement> {
    let geometry = operator.geometry();
    let [nx, ny] = geometry.grid_shape();
    let [sx, sy] = geometry.scale();
    let mut hold = CellHold::new();
    let dense = dense_cells(operator, keys);
    let mut out = Vec::new();
    for &cf in keys {
        let half = operator.cf().taps(cf, &mut hold).half_support();
        let [x0, y0] = tile.origin.map(|origin| origin as i64);
        let [x1, y1] = [
            (tile.origin[0] + tile.shape[0]) as i64 - 1,
            (tile.origin[1] + tile.shape[1]) as i64 - 1,
        ];
        let (mid_x, mid_y) = ((x0 + x1) / 2, (y0 + y1) / 2);
        let anchors = [
            (x0 + i64::from(half[0]), mid_y),
            (x1 - i64::from(half[0]), mid_y),
            (mid_x, y0 + i64::from(half[1])),
            (mid_x, y1 - i64::from(half[1])),
        ];
        for (x, y) in anchors {
            // `GridGeometry::locate` anchors at `round(u · s_x + n_x / 2)`.
            let u = (x - (nx / 2) as i64) as f64 / sx;
            let v = (y - (ny / 2) as i64) as f64 / sy;
            for w_positive in [false, true] {
                let mut placed = placement(cf, u, v, 1, dense, rng);
                placed.w = if w_positive { 25.0 } else { -25.0 };
                out.push(placed);
            }
        }
    }
    out
}

/// A wide field (1 mrad cells, 3.7° across [`IMAGE`] pixels), where the
/// W-projection set's planes reach phases of radians and its supports
/// differ from plane to plane.
const W_INCREMENT_RAD: [f64; 2] = [-1.0e-3, 1.0e-3];

/// An `f32` operator over `[XX, YY]` with sixteen W-projection planes
/// (`WPlanes`, CASA's `WProjectFT` kernels, [`GridEdge::Sectioned`] edge)
/// on a wide-field [`IMAGE`]² image ([`W_INCREMENT_RAD`]).
///
/// [`GridEdge::Sectioned`]: casa_imaging_operator::GridEdge::Sectioned
pub fn w_planes_operator() -> MeasurementOperator {
    let geometry = GridGeometry::new(
        ImageExtent {
            shape: [IMAGE, IMAGE],
            increment_rad: W_INCREMENT_RAD,
            reference_pixel: [IMAGE / 2, IMAGE / 2],
        },
        GridPadding::CasaComposite(PaddingFactor::TCLEAN),
    )
    .expect("a wide geometry");
    let planes = WPlanes::new(&geometry, &routing(), WPlaneCount::Fixed(16)).expect("W planes");
    MeasurementOperator::new(
        geometry,
        Basis::Constant,
        routing(),
        Box::new(planes),
        GridPrecision::F32,
    )
}

/// W-projection samples about each of the grid's four edges, for three
/// planes of different supports and both signs of `w`, a random fraction
/// of a cell from their anchors: supports overrunning the edge by one and
/// by three cells with the anchor on the grid, the anchor one and three
/// cells off the grid with the support reaching back, a support whose one
/// tap on the grid is its last, one that ends a cell short of the grid, and
/// (on the high edges) one whose last tap lies on the grid's last column or
/// row, which the sectors leave out (#980 review).
pub fn sectioned_edge_placements(operator: &MeasurementOperator, rng: &mut Rng) -> Vec<Placement> {
    let geometry = operator.geometry();
    let n = geometry.grid_shape().map(|n| n as i64);
    let [sx, sy] = geometry.scale();
    let max_w = 0.25 / W_INCREMENT_RAD[1];
    let context = RowContext {
        original_uvw_m: None,
        time_s: 0.0,
        antennas: [0, 1],
        antenna_types: [0, 0],
        parallactic_angle_rad: [0.0; 2],
        field: 0,
        spectral_window: 0,
    };
    let mut hold = CellHold::new();
    let mut out = Vec::new();
    for fraction in [0.1, 0.45, 0.9] {
        for sign in [-1.0, 1.0] {
            let w = sign * fraction * max_w;
            let cf = operator.cf().key(&context, 1.0e9, w);
            let half = operator
                .cf()
                .taps(cf, &mut hold)
                .half_support()
                .map(i64::from);
            for axis in 0..2 {
                let h = half[axis];
                let last = n[axis] - 1;
                let low = [h - 1, h - 3, -1, -3, -h, -h - 1];
                let high = [
                    last - h,
                    last - h + 1,
                    last - h + 3,
                    last + 1,
                    last + 3,
                    last + h,
                    last + h + 1,
                ];
                for edge in low.into_iter().chain(high) {
                    let mut cell = n.map(|n| n / 2);
                    cell[1 - axis] += (rng.next_u64() % 9) as i64 - 4;
                    cell[axis] = edge;
                    let fine = [rng.signed() * 0.45, rng.signed() * 0.45];
                    out.push(Placement {
                        u: (cell[0] as f64 + fine[0] - (n[0] / 2) as f64) / sx,
                        v: (cell[1] as f64 + fine[1] - (n[1] / 2) as f64) / sy,
                        w,
                        prediction_w_positive: w > 0.0,
                        phase: rng.signed() * std::f64::consts::PI,
                        plane: 0,
                        spectral: 0.0,
                        cf,
                        gradient: [0.0; 2],
                    });
                }
            }
        }
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
    let [width, height] = operator.geometry().image().shape;
    ModelImages {
        first_plane: 0,
        planes: (0..basis.planes())
            .map(|_| ModelPlane {
                images: (0..images)
                    .map(|_| Array2::from_shape_fn((height, width), |_| rng.signed() as f32))
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

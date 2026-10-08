// SPDX-License-Identifier: LGPL-3.0-or-later
//! Synthetic native rows, operators and a pass runner for the pass laws.

use casa_imaging_model::{CorrelationType, PolarizationCoordinate};
use casa_imaging_operator::{
    Basis, CpuBackend, GridGeometry, GridPadding, GridPrecision, ImageExtent, MeasurementOperator,
    ModeSet, ModelImages, ModelPlane, ModelPrescale, NativeRow, NormalImages, PlaneRange,
    PolarizationRouting, PredictionScratch, PreparedModelGrids, RowContext, SpectralAxis,
    SpectralKernel, SpectralResampler, Spheroidal, WeightingGeneration,
};
use casa_imaging_runtime::pass::{
    BoundedSource, Cancel, DomainProjection, MajorCyclePass, ModelPreparation, NativeBlock,
    NativeRowHeader, Partition, PassDomain, PassError, PassSummary, Residency, RowAddress,
    SourceError, WorkerTeam, run_major_cycle,
};
use ndarray::Array2;
use num_complex::Complex32;

pub const IMAGE: usize = 64;
pub const INCREMENT_RAD: [f64; 2] = [-2.0e-5, 2.0e-5];
pub const CHANNELS: usize = 6;
pub const FIRST_HZ: f64 = 1.0e9;
pub const WIDTH_HZ: f64 = 4.0e6;
const SPEED_OF_LIGHT: f64 = 299_792_458.0;

pub struct Rng(pub u64);

impl Rng {
    pub fn unit(&mut self) -> f64 {
        let mut x = self.0;
        x ^= x >> 12;
        x ^= x << 25;
        x ^= x >> 27;
        self.0 = x;
        (x.wrapping_mul(0x2545_F491_4F6C_DD1D) >> 11) as f64 / (1u64 << 53) as f64
    }

    pub fn signed(&mut self) -> f64 {
        2.0 * self.unit() - 1.0
    }
}

/// One synthetic row: its projection on every domain and its values,
/// weights and flags over every channel and both correlations.
pub struct Row {
    pub projections: Vec<DomainProjection>,
    pub values: Vec<Complex32>,
    pub weights: Vec<f32>,
    pub flags: Vec<bool>,
}

/// An in-memory source delivering the same rows in blocks of 37, recording
/// whether each traversal was allowed to restrict its channels.
pub struct Rows {
    pub rows: Vec<Row>,
    pub frequencies: Vec<f64>,
    next: usize,
    pub restricted: Vec<bool>,
}

/// `CHANNELS` native channels `WIDTH_HZ` apart from `FIRST_HZ`.
pub fn frequencies() -> Vec<f64> {
    (0..CHANNELS)
        .map(|channel| FIRST_HZ + channel as f64 * WIDTH_HZ)
        .collect()
}

impl Rows {
    /// Rows at `frequencies()` whose baselines stay well inside the padded
    /// grid of `geometry` at every channel; about one sample in twenty is
    /// flagged.
    pub fn random(count: usize, seed: u64, geometry: &GridGeometry) -> Self {
        Self::at(count, seed, geometry, frequencies())
    }

    /// Rows at `frequencies` on one domain.
    pub fn at(count: usize, seed: u64, geometry: &GridGeometry, frequencies: Vec<f64>) -> Self {
        let mut rng = Rng(seed);
        let [nx, ny] = geometry.grid_shape();
        let [sx, sy] = geometry.scale();
        let top_hz = frequencies.iter().fold(0.0_f64, |top, f| top.max(*f));
        let cells = frequencies.len() * 2;
        let rows = (0..count)
            .map(|_| {
                let u = rng.signed() * 0.4 * nx as f64 / sx.abs() * SPEED_OF_LIGHT / top_hz;
                let v = rng.signed() * 0.4 * ny as f64 / sy.abs() * SPEED_OF_LIGHT / top_hz;
                let weight = (0.5 + rng.unit()) as f32;
                Row {
                    projections: vec![DomainProjection {
                        uvw_m: [u, v, rng.signed() * 20.0],
                        phase_shift_m: rng.signed() * 0.05,
                    }],
                    values: (0..cells)
                        .map(|_| Complex32::new(rng.signed() as f32, rng.signed() as f32))
                        .collect(),
                    weights: vec![weight; cells],
                    flags: (0..cells).map(|_| rng.unit() < 0.05).collect(),
                }
            })
            .collect();
        Self {
            rows,
            frequencies,
            next: 0,
            restricted: Vec::new(),
        }
    }

    /// Project every row on a further domain: its baseline rotated by
    /// `angle_rad` in the uv plane, as a phase-centre rotation turns it, and
    /// a path-length shift of its own.
    pub fn add_domain(&mut self, angle_rad: f64, seed: u64) {
        let mut rng = Rng(seed);
        let (sin, cos) = angle_rad.sin_cos();
        for row in &mut self.rows {
            let [u, v, w] = row.projections[0].uvw_m;
            row.projections.push(DomainProjection {
                uvw_m: [u * cos - v * sin, u * sin + v * cos, w],
                phase_shift_m: 0.1 + rng.signed() * 0.05,
            });
        }
    }

    /// Native row `index` as domain `domain`'s operator reads it.
    pub fn native(&self, domain: usize, index: usize) -> NativeRow<'_> {
        let row = &self.rows[index];
        NativeRow {
            uvw_m: row.projections[domain].uvw_m,
            phase_shift_m: row.projections[domain].phase_shift_m,
            frequencies_hz: &self.frequencies,
            values: &row.values,
            weights: &row.weights,
            flags: &row.flags,
            row_flag: false,
            context: context(),
        }
    }

    /// Replace every row's data with the sum of `models`' predictions, one
    /// `(domain, operator, resampler, model)` each; returns the largest
    /// predicted amplitude.
    pub fn predict(
        &mut self,
        models: &[(
            usize,
            &MeasurementOperator,
            &SpectralResampler,
            &PreparedModelGrids,
        )],
    ) -> f32 {
        let mut backend = CpuBackend::new();
        let mut scratch = PredictionScratch::default();
        let mut largest = 0.0_f32;
        for index in 0..self.rows.len() {
            let mut sum = vec![Complex32::default(); self.rows[index].values.len()];
            let mut predicted = sum.clone();
            for (domain, operator, resampler, model) in models {
                resampler
                    .predict_row(
                        operator,
                        &mut backend,
                        model,
                        &self.native(*domain, index),
                        &mut scratch,
                        &mut predicted,
                    )
                    .expect("predict");
                for (total, value) in sum.iter_mut().zip(&predicted) {
                    *total += value;
                }
            }
            largest = sum.iter().fold(largest, |m, v| m.max(v.norm()));
            self.rows[index].values = sum;
        }
        largest
    }
}

pub fn context() -> RowContext {
    RowContext {
        time_s: 0.0,
        antennas: [0, 1],
        parallactic_angle_rad: [0.0; 2],
        field: 0,
        pointing_offset_rad: [0.0; 2],
    }
}

impl BoundedSource for Rows {
    fn begin(&mut self, _: PlaneRange, restrict: bool) -> Result<(), SourceError> {
        self.next = 0;
        self.restricted.push(restrict);
        Ok(())
    }

    fn fill(&mut self, block: &mut NativeBlock) -> Result<bool, SourceError> {
        if self.next == self.rows.len() {
            return Ok(false);
        }
        let channels = (0..self.frequencies.len() as u32).collect::<Vec<_>>();
        block.reset(self.rows[0].projections.len(), &channels, &[0, 1]);
        let end = (self.next + 37).min(self.rows.len());
        for (index, row) in self.rows[self.next..end].iter().enumerate() {
            block.push_row(
                NativeRowHeader {
                    row_flag: false,
                    context: context(),
                    address: RowAddress {
                        physical_row: (self.next + index) as u64,
                        data_description: 0,
                    },
                },
                &row.projections,
                &self.frequencies,
                &row.values,
                &row.weights,
                &row.flags,
            );
        }
        self.next = end;
        Ok(true)
    }
}

pub fn geometry(image: usize) -> GridGeometry {
    GridGeometry::new(
        ImageExtent {
            shape: [image, image],
            increment_rad: INCREMENT_RAD,
            reference_pixel: [image / 2, image / 2],
        },
        GridPadding::CasaComposite,
    )
    .expect("geometry")
}

/// A Stokes-I operator from linear feeds on an `image`² grid.
pub fn operator_of(image: usize, precision: GridPrecision, basis: Basis) -> MeasurementOperator {
    let geometry = geometry(image);
    let polarization = PolarizationRouting::compile(
        &[CorrelationType::LinearXx, CorrelationType::LinearYy],
        &[PolarizationCoordinate::StokesI],
    )
    .expect("routing");
    let cf = Spheroidal::new(&geometry, &polarization);
    MeasurementOperator::new(geometry, basis, polarization, Box::new(cf), precision)
}

pub fn operator(precision: GridPrecision, basis: Basis) -> MeasurementOperator {
    operator_of(IMAGE, precision, basis)
}

/// A cube resampler mapping native channel `c` to plane `c / 2`.
pub fn nearest_cube() -> SpectralResampler {
    SpectralResampler::channel_local(
        SpectralAxis::new(
            FIRST_HZ + WIDTH_HZ / 2.0,
            2.0 * WIDTH_HZ,
            (CHANNELS / 2) as u32,
        )
        .expect("axis"),
        SpectralKernel::Nearest,
    )
}

/// A linear cube whose channels are 1.5 native widths wide and start off the
/// native grid, so output samples fall between native channels.
pub fn offset_linear_cube() -> SpectralResampler {
    SpectralResampler::channel_local(
        SpectralAxis::new(FIRST_HZ + 1.3 * WIDTH_HZ, 1.5 * WIDTH_HZ, 3).expect("axis"),
        SpectralKernel::Linear,
    )
}

/// A sparse random model of `planes` planes of `image`² pixels.
pub fn sparse_model(image: usize, planes: usize, seed: u64) -> ModelImages {
    let mut rng = Rng(seed);
    ModelImages {
        first_plane: 0,
        planes: (0..planes)
            .map(|_| ModelPlane {
                images: vec![Array2::from_shape_fn((image, image), |_| {
                    if rng.unit() < 0.02 {
                        rng.signed() as f32
                    } else {
                        0.0
                    }
                })],
            })
            .collect(),
    }
}

/// The prepared grids of a window of `model`'s planes.
pub fn window_of(
    operator: &MeasurementOperator,
    model: &ModelImages,
    planes: PlaneRange,
) -> PreparedModelGrids {
    let window = ModelImages {
        first_plane: planes.start,
        planes: model.planes[planes.start as usize..planes.end as usize].to_vec(),
    };
    operator
        .prepare_model(&window, ModelPrescale::Unit)
        .expect("model window")
}

/// A plane-partitioned domain for `workers` owners.
pub fn planes<'a>(
    operator: &'a MeasurementOperator,
    resampler: &'a SpectralResampler,
    workers: usize,
) -> PassDomain<'a> {
    PassDomain {
        operator,
        resampler,
        partition: Partition::Planes { owners: workers },
    }
}

/// What one run of a pass needs besides its domains and source.
pub struct Run<'a> {
    pub residency: Residency,
    pub workers: usize,
    pub model: Option<&'a ModelPreparation<'a>>,
    pub modes: ModeSet,
    pub native_spacing_hz: f64,
}

impl Run<'_> {
    /// Data and PSF without a model, every plane resident, one worker.
    pub fn initial() -> Self {
        Self {
            residency: Residency::All,
            workers: 1,
            model: None,
            modes: ModeSet::DATA_PSF,
            native_spacing_hz: WIDTH_HZ,
        }
    }
}

/// Run one pass; returns each domain's images joined over the waves, and
/// the summary.
pub fn try_run(
    domains: &[PassDomain<'_>],
    source: &mut Rows,
    run: &Run<'_>,
) -> Result<(Vec<NormalImages>, PassSummary), PassError> {
    let weighting = WeightingGeneration::Natural { taper: None };
    let pass = MajorCyclePass {
        domains,
        weighting: &weighting,
        modes: run.modes,
        model: run.model,
        residency: run.residency,
        native_spacing_hz: run.native_spacing_hz,
    };
    let team = WorkerTeam::new(run.workers).expect("team");
    let mut joined: Vec<Option<NormalImages>> = (0..domains.len()).map(|_| None).collect();
    let summary = run_major_cycle(
        &pass,
        source,
        &team,
        &Cancel::new(),
        &mut |domain, images| {
            match &mut joined[domain] {
                Some(joined) => {
                    assert_eq!(
                        images.first_plane,
                        joined.first_plane + joined.planes.len() as u32,
                        "waves arrive in plane order"
                    );
                    joined.planes.extend(images.planes);
                }
                slot @ None => *slot = Some(images),
            }
            Ok(())
        },
        None,
    )?;
    Ok((
        joined
            .into_iter()
            .map(|images| images.expect("every domain has images"))
            .collect(),
        summary,
    ))
}

/// [`try_run`] for a pass that must succeed and place samples.
pub fn run(domains: &[PassDomain<'_>], source: &mut Rows, run: &Run<'_>) -> Vec<NormalImages> {
    let (images, summary) = try_run(domains, source, run).expect("pass");
    assert!(summary.samples > 0 && summary.blocks > 0);
    images
}

/// The largest absolute value of `images`' data planes, each divided by its
/// `sumwt`.
pub fn normalised_peak(images: &NormalImages) -> f64 {
    let mut peak = 0.0_f64;
    for plane in 0..images.planes.len() {
        let sumwt = images.data_sumwt(plane, 0, 0);
        if sumwt <= 0.0 {
            continue;
        }
        let image = images.data(plane, 0, 0);
        peak = image
            .iter()
            .fold(peak, |m, v| m.max(f64::from(*v).abs() / sumwt));
    }
    peak
}

pub fn worst_relative(a: &NormalImages, b: &NormalImages) -> f64 {
    assert_eq!(a.planes.len(), b.planes.len());
    let mut worst = 0.0_f64;
    for (pa, pb) in a.planes.iter().zip(&b.planes) {
        for (ia, ib) in pa
            .data
            .iter()
            .chain(&pa.psf)
            .zip(pb.data.iter().chain(&pb.psf))
        {
            let peak = ia
                .iter()
                .fold(0.0_f64, |m, v| m.max(f64::from(*v).abs()))
                .max(f64::MIN_POSITIVE);
            for (x, y) in ia.iter().zip(ib) {
                worst = worst.max(f64::from(x - y).abs() / peak);
            }
        }
        for (x, y) in pa.sumwt.iter().zip(&pb.sumwt) {
            worst = worst.max((x - y).abs() / x.abs().max(f64::MIN_POSITIVE));
        }
    }
    worst
}

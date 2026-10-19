// SPDX-License-Identifier: LGPL-3.0-or-later
//! Synthetic native rows, operators and a pass runner for the pass laws.

use std::sync::atomic::{AtomicUsize, Ordering};

use casa_imaging_model::{CorrelationType, PolarizationCoordinate};
use casa_imaging_operator::{
    Basis, CellHold, CfKey, ConvolutionFunctionSet, CpuBackend, GridGeometry, GridPadding,
    GridPrecision, ImageCorrection, ImageExtent, KernelNormalisation, MeasurementOperator, ModeSet,
    ModelImages, ModelPlane, ModelPrescale, MuellerRouting, NativeRow, NormalImages, PlaneRange,
    PolarizationRouting, PredictionScratch, PreparedModelGrids, RowContext, RowSpectrum,
    SpectralAxis, SpectralKernel, SpectralResampler, Spheroidal, TapLayout, WeightingGeneration,
};
use casa_imaging_runtime::pass::{
    BackendChoice, BlockShape, BoundedSource, CHUNK_PLACEMENTS, Cancel, DomainProjection,
    MajorCyclePass, ModelPreparation, NativeBlock, NativeRowHeader, Partition, PassDomain,
    PassError, PassSummary, Residency, RowAddress, SampleContent, SourceError, WeightCache,
    WorkerTeam, run_major_cycle,
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

/// The most rows a [`Rows`] block holds.
const BLOCK_ROWS: usize = 37;

/// An in-memory source delivering the same rows in blocks of 37, or of
/// `block_rows` when a law sets fewer, recording whether each traversal was
/// allowed to restrict its channels.
pub struct Rows {
    pub rows: Vec<Row>,
    pub frequencies: Vec<f64>,
    /// Rows per block, at most 37.
    pub block_rows: usize,
    next: usize,
    pub restricted: Vec<bool>,
    /// What each traversal asked for; a flags-and-weights traversal
    /// delivers zero values.
    pub contents: Vec<SampleContent>,
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
                        pointing_offset_rad: [0.0; 2],
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
            block_rows: BLOCK_ROWS,
            next: 0,
            restricted: Vec::new(),
            contents: Vec::new(),
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
                pointing_offset_rad: [0.0; 2],
            });
        }
    }

    /// Native row `index` as domain `domain`'s operator reads it.
    pub fn native(&self, domain: usize, index: usize) -> NativeRow<'_> {
        let row = &self.rows[index];
        NativeRow {
            uvw_m: row.projections[domain].uvw_m,
            phase_shift_m: row.projections[domain].phase_shift_m,
            pointing_offset_rad: row.projections[domain].pointing_offset_rad,
            frequencies_hz: &self.frequencies,
            values: &row.values,
            weights: &row.weights,
            flags: &row.flags,
            row_flag: false,
            context: context(index),
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

/// Row `index`'s context: successive rows cycle through five baselines, as
/// a measurement set's time-ordered rows do, so a pass that groups a
/// block's rows by baseline reorders them.
pub fn context(index: usize) -> RowContext {
    RowContext {
        original_uvw_m: None,
        time_s: 0.0,
        antennas: [0, 1 + (index % 5) as u32],
        antenna_types: [0, 0],
        parallactic_angle_rad: [0.0; 2],
        field: 0,
        spectral_window: 0,
    }
}

impl BoundedSource for Rows {
    fn begin(
        &mut self,
        _: PlaneRange,
        restrict: bool,
        content: SampleContent,
    ) -> Result<(), SourceError> {
        self.next = 0;
        self.restricted.push(restrict);
        self.contents.push(content);
        Ok(())
    }

    fn fill(&mut self, block: &mut NativeBlock) -> Result<bool, SourceError> {
        if self.next == self.rows.len() {
            return Ok(false);
        }
        let channels = (0..self.frequencies.len() as u32).collect::<Vec<_>>();
        block.reset(self.rows[0].projections.len(), &channels, &[0, 1]);
        let end = (self.next + self.block_rows.min(BLOCK_ROWS)).min(self.rows.len());
        let unread = self.contents.last() == Some(&SampleContent::FlagsAndWeights);
        for (index, row) in self.rows[self.next..end].iter().enumerate() {
            let zeros = vec![Complex32::default(); row.values.len()];
            block.push_row(
                NativeRowHeader {
                    row_flag: false,
                    context: context(self.next + index),
                    address: RowAddress {
                        physical_row: (self.next + index) as u64,
                        data_description: 0,
                    },
                },
                &row.projections,
                &self.frequencies,
                if unread { &zeros } else { &row.values },
                &row.weights,
                &row.flags,
            );
        }
        self.next = end;
        Ok(true)
    }

    fn maximum_block(&self) -> BlockShape {
        let frequencies = &self.frequencies;
        let channels = frequencies.len();
        BlockShape {
            rows: BLOCK_ROWS,
            spectrum: RowSpectrum {
                channels,
                first_spacing_hz: if channels > 1 {
                    (frequencies[1] - frequencies[0]).abs()
                } else {
                    0.0
                },
                span_hz: (frequencies[channels - 1] - frequencies[0]).abs(),
            },
            correlations: 2,
            domains: self.rows.first().map_or(1, |row| row.projections.len()),
        }
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
    operator_with(image, precision, basis, |geometry, polarization| {
        Box::new(Spheroidal::new(geometry, polarization))
    })
}

/// A Stokes-I operator from linear feeds on an `image`² grid, gridding with
/// the kernel set `kernels` builds for its geometry and polarization.
pub fn operator_with(
    image: usize,
    precision: GridPrecision,
    basis: Basis,
    kernels: impl FnOnce(&GridGeometry, &PolarizationRouting) -> Box<dyn ConvolutionFunctionSet>,
) -> MeasurementOperator {
    let geometry = geometry(image);
    let polarization = PolarizationRouting::compile(
        &[CorrelationType::LinearXx, CorrelationType::LinearYy],
        &[PolarizationCoordinate::StokesI],
    )
    .expect("routing");
    let cf = kernels(&geometry, &polarization);
    MeasurementOperator::new(geometry, basis, polarization, cf, precision)
}

/// The spheroidal kernels for the pipeline laws (#784 review). With
/// `weight_image` they also grid a weight image, with the imaging taps, so
/// a region pass routes every placement to the centre row's owner too. With
/// `panic_at`, the `n`-th lend of the imaging taps panics: only an owner's
/// accumulation asks for them, since routing and placement read the
/// spheroidal's own answers and the PSF and weight image borrow its taps
/// directly.
pub struct TestKernels {
    inner: Spheroidal,
    weight_image: bool,
    panic_at: Option<usize>,
    lent: AtomicUsize,
    /// Whether every lend of the weight taps panics: only the weight
    /// owner's grid of its summed groups lends them (#787).
    weight_panics: bool,
}

impl TestKernels {
    /// The kernels on `geometry` and `polarization`.
    pub fn new(
        geometry: &GridGeometry,
        polarization: &PolarizationRouting,
        weight_image: bool,
        panic_at: Option<usize>,
    ) -> Self {
        Self {
            inner: Spheroidal::new(geometry, polarization),
            weight_image,
            panic_at,
            lent: AtomicUsize::new(0),
            weight_panics: false,
        }
    }

    /// These kernels, panicking whenever the weight taps are lent.
    #[must_use]
    pub fn panicking_weights(self) -> Self {
        Self {
            weight_panics: true,
            ..self
        }
    }
}

impl ConvolutionFunctionSet for TestKernels {
    fn key(&self, row: &RowContext, freq_hz: f64, w_lambda: f64) -> CfKey {
        self.inner.key(row, freq_hz, w_lambda)
    }

    fn uniform_key(&self) -> Option<CfKey> {
        self.inner.uniform_key()
    }

    fn taps<'s>(&'s self, key: CfKey, hold: &'s mut CellHold) -> TapLayout<'s> {
        let lent = self.lent.fetch_add(1, Ordering::Relaxed) + 1;
        assert_ne!(self.panic_at, Some(lent), "the test kernels panic");
        self.inner.taps(key, hold)
    }

    fn gridding_half_support(&self, key: CfKey) -> [u16; 2] {
        self.inner.gridding_half_support(key)
    }

    fn gridding_oversampling(&self, key: CfKey) -> u16 {
        self.inner.gridding_oversampling(key)
    }

    fn max_half_support(&self) -> [u16; 2] {
        self.inner.max_half_support()
    }

    fn weight_taps<'s>(&'s self, key: CfKey, hold: &'s mut CellHold) -> Option<TapLayout<'s>> {
        assert!(!self.weight_panics, "the test kernels' weight taps panic");
        self.weight_image.then(|| self.inner.taps(key, hold))
    }

    fn mueller(&self) -> &MuellerRouting {
        self.inner.mueller()
    }

    fn prediction_w(&self, row: &RowContext, freq_hz: f64, w_lambda: f64) -> f64 {
        self.inner.prediction_w(row, freq_hz, w_lambda)
    }

    fn image_correction(&self) -> &ImageCorrection {
        self.inner.image_correction()
    }

    fn normalisation(&self) -> KernelNormalisation {
        self.inner.normalisation()
    }

    fn pointing_ramp(&self) -> bool {
        self.inner.pointing_ramp()
    }

    fn psf_taps<'s>(&'s self, key: CfKey, hold: &'s mut CellHold) -> TapLayout<'s> {
        self.inner.psf_taps(key, hold)
    }

    fn prediction_taps<'s>(&'s self, key: CfKey, hold: &'s mut CellHold) -> TapLayout<'s> {
        self.inner.prediction_taps(key, hold)
    }

    fn placement_half_support(&self, key: CfKey, hold: &mut CellHold) -> [u16; 2] {
        self.inner.placement_half_support(key, hold)
    }

    fn admits(&self, w_lambda: f64) -> bool {
        self.inner.admits(w_lambda)
    }

    /// The spheroidal's cell, lent again as the weight taps when the
    /// kernels grid a weight image.
    fn tap_footprint(&self) -> casa_imaging_operator::TapFootprint {
        let inner = self.inner.tap_footprint();
        if self.weight_image {
            casa_imaging_operator::TapFootprint {
                tap_bytes: 2 * inner.tap_bytes,
                norm_bytes: 2 * inner.norm_bytes,
                cells: 2 * inner.cells,
            }
        } else {
            inner
        }
    }

    fn corrects_weight_image(&self) -> bool {
        self.inner.corrects_weight_image()
    }

    fn weight_image_magnitude(&self) -> bool {
        self.inner.weight_image_magnitude()
    }
}

/// [`operator_with`] the [`TestKernels`] on the fixture's `IMAGE`² grid.
pub fn test_kernels_operator(weight_image: bool, panic_at: Option<usize>) -> MeasurementOperator {
    operator_with(
        IMAGE,
        GridPrecision::F64,
        Basis::Constant,
        |geometry, polarization| {
            Box::new(TestKernels::new(
                geometry,
                polarization,
                weight_image,
                panic_at,
            ))
        },
    )
}

/// [`test_kernels_operator`] with a weight image whose taps panic when
/// lent ([`TestKernels::panicking_weights`]).
pub fn panicking_weights_operator() -> MeasurementOperator {
    operator_with(
        IMAGE,
        GridPrecision::F64,
        Basis::Constant,
        |geometry, polarization| {
            Box::new(TestKernels::new(geometry, polarization, true, None).panicking_weights())
        },
    )
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
    pub backend: BackendChoice,
    pub weights: Option<&'a WeightCache>,
    pub chunk_placements: usize,
    pub chunk_sets: usize,
}

impl Run<'_> {
    /// Data and PSF without a model, every plane resident, one worker, on
    /// the CPU, weighing every sample, in one set of chunks of the default
    /// size.
    pub fn initial() -> Self {
        Self {
            residency: Residency::All,
            workers: 1,
            model: None,
            modes: ModeSet::DATA_PSF,
            native_spacing_hz: WIDTH_HZ,
            backend: BackendChoice::Cpu,
            weights: None,
            chunk_placements: CHUNK_PLACEMENTS,
            chunk_sets: 1,
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
    try_run_on(
        domains,
        source,
        run,
        &WorkerTeam::new(run.workers).expect("team"),
    )
}

/// [`try_run`] on `team`, whatever `run.workers` says.
pub fn try_run_on(
    domains: &[PassDomain<'_>],
    source: &mut Rows,
    run: &Run<'_>,
    team: &WorkerTeam,
) -> Result<(Vec<NormalImages>, PassSummary), PassError> {
    let weighting = WeightingGeneration::Natural { taper: None };
    let pass = MajorCyclePass {
        domains,
        weighting: &weighting,
        modes: run.modes,
        model: run.model,
        residency: run.residency,
        native_spacing_hz: run.native_spacing_hz,
        backend: run.backend,
        metal_stage_timing: false,
        weights: run.weights,
        chunk_placements: run.chunk_placements,
        chunk_sets: run.chunk_sets,
    };
    let mut joined: Vec<Option<NormalImages>> = (0..domains.len()).map(|_| None).collect();
    let summary = run_major_cycle(
        &pass,
        source,
        team,
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

/// The largest absolute value of `images`' data planes, each divided by the
/// `sumwt` of the same plane of `sums`: a pass without a model over the same
/// rows, since a residual pass sums no weights. Every plane of `sums` must
/// carry weight, so a pass that lost its weights cannot make the peak
/// vanish.
pub fn normalised_peak(images: &NormalImages, sums: &NormalImages) -> f64 {
    assert_eq!(images.planes.len(), sums.planes.len());
    let mut peak = 0.0_f64;
    for plane in 0..images.planes.len() {
        let sumwt = sums.data_sumwt(plane, 0, 0);
        assert!(sumwt > 0.0, "plane {plane} of the weighing pass has weight");
        let image = images.data(plane, 0, 0);
        peak = image
            .iter()
            .fold(peak, |m, v| m.max(f64::from(*v).abs() / sumwt));
    }
    peak
}

/// The worst difference between `a` and `b`: over their data, PSF and
/// weight images, each relative to its own peak, and their `sumwt`.
pub fn worst_relative(a: &NormalImages, b: &NormalImages) -> f64 {
    assert_eq!(a.planes.len(), b.planes.len());
    let mut worst = 0.0_f64;
    for (pa, pb) in a.planes.iter().zip(&b.planes) {
        for (ia, ib) in pa
            .data
            .iter()
            .chain(&pa.psf)
            .chain(&pa.weight)
            .zip(pb.data.iter().chain(&pb.psf).chain(&pb.weight))
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

// SPDX-License-Identifier: LGPL-3.0-or-later
//! Laws of the major-cycle pass on synthetic native rows: partition and
//! worker-count invariance, waves equal one resident pass, the residual of a
//! predicted model vanishes, the streamed density grid equals the one-shot
//! build, and cancellation stops the pass.

use casa_imaging_model::{CorrelationType, PolarizationCoordinate};
use casa_imaging_operator::{
    Basis, CpuBackend, DensityCellRule, DensityGridShape, GridBackend, GridGeometry, GridPadding,
    GridPrecision, ImageExtent, MeasurementOperator, ModeSet, ModelImages, ModelPlane,
    ModelPrescale, NormalImages, PlaneRange, PolarizationRouting, RowContext, SampleBuffer,
    SpectralAxis, SpectralKernel, SpectralResampler, Spheroidal, WeightingGeneration,
    build_density_grid,
};
use casa_imaging_runtime::pass::{
    BoundedSource, Cancel, MajorCyclePass, NativeBlock, NativeRowHeader, Partition, PassError,
    Residency, RowAddress, SourceError, WorkerTeam, run_density_pass, run_major_cycle,
};
use ndarray::Array2;
use num_complex::Complex32;

const IMAGE: usize = 64;
const INCREMENT_RAD: [f64; 2] = [-2.0e-5, 2.0e-5];
const CHANNELS: usize = 6;
const FIRST_HZ: f64 = 1.0e9;
const WIDTH_HZ: f64 = 4.0e6;
const SPEED_OF_LIGHT: f64 = 299_792_458.0;

struct Rng(u64);

impl Rng {
    fn unit(&mut self) -> f64 {
        let mut x = self.0;
        x ^= x >> 12;
        x ^= x << 25;
        x ^= x >> 27;
        self.0 = x;
        (x.wrapping_mul(0x2545_F491_4F6C_DD1D) >> 11) as f64 / (1u64 << 53) as f64
    }

    fn signed(&mut self) -> f64 {
        2.0 * self.unit() - 1.0
    }
}

/// One synthetic row: uvw, values, weights, flags over every channel.
struct Row {
    uvw_m: [f64; 3],
    phase_shift_m: f64,
    values: Vec<Complex32>,
    weights: Vec<f32>,
    flags: Vec<bool>,
}

/// An in-memory source delivering the same rows in blocks of `block_rows`.
struct Rows {
    rows: Vec<Row>,
    block_rows: usize,
    next: usize,
    traversals: usize,
}

fn frequencies() -> Vec<f64> {
    (0..CHANNELS)
        .map(|channel| FIRST_HZ + channel as f64 * WIDTH_HZ)
        .collect()
}

impl Rows {
    /// Rows whose baselines stay well inside the padded grid at every
    /// channel; about one sample in twenty is flagged.
    fn random(count: usize, seed: u64, geometry: &GridGeometry) -> Self {
        let mut rng = Rng(seed);
        let [nx, ny] = geometry.grid_shape();
        let [sx, sy] = geometry.scale();
        let top_hz = FIRST_HZ + CHANNELS as f64 * WIDTH_HZ;
        let rows = (0..count)
            .map(|_| {
                let u = rng.signed() * 0.4 * nx as f64 / sx.abs() * SPEED_OF_LIGHT / top_hz;
                let v = rng.signed() * 0.4 * ny as f64 / sy.abs() * SPEED_OF_LIGHT / top_hz;
                let cells = CHANNELS * 2;
                let weight = (0.5 + rng.unit()) as f32;
                Row {
                    uvw_m: [u, v, rng.signed() * 20.0],
                    phase_shift_m: rng.signed() * 0.05,
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
            block_rows: 37,
            next: 0,
            traversals: 0,
        }
    }
}

impl BoundedSource for Rows {
    fn begin(&mut self, _: PlaneRange) -> Result<(), SourceError> {
        self.next = 0;
        self.traversals += 1;
        Ok(())
    }

    fn fill(&mut self, block: &mut NativeBlock) -> Result<bool, SourceError> {
        if self.next == self.rows.len() {
            return Ok(false);
        }
        let channels = (0..CHANNELS as u32).collect::<Vec<_>>();
        block.reset(&channels, &[0, 1]);
        let end = (self.next + self.block_rows).min(self.rows.len());
        let frequencies = frequencies();
        for (index, row) in self.rows[self.next..end].iter().enumerate() {
            block.push_row(
                NativeRowHeader {
                    uvw_m: row.uvw_m,
                    phase_shift_m: row.phase_shift_m,
                    row_flag: false,
                    context: RowContext {
                        time_s: 0.0,
                        antennas: [0, 1],
                        parallactic_angle_rad: [0.0; 2],
                        field: 0,
                        pointing_offset_rad: [0.0; 2],
                    },
                    address: RowAddress {
                        physical_row: (self.next + index) as u64,
                        data_description: 0,
                    },
                },
                &frequencies,
                &row.values,
                &row.weights,
                &row.flags,
            );
        }
        self.next = end;
        Ok(true)
    }
}

fn geometry() -> GridGeometry {
    GridGeometry::new(
        ImageExtent {
            shape: [IMAGE, IMAGE],
            increment_rad: INCREMENT_RAD,
            reference_pixel: [IMAGE / 2, IMAGE / 2],
        },
        GridPadding::CasaComposite,
    )
    .expect("geometry")
}

fn operator(precision: GridPrecision, basis: Basis) -> MeasurementOperator {
    let geometry = geometry();
    let polarization = PolarizationRouting::compile(
        &[CorrelationType::LinearXx, CorrelationType::LinearYy],
        &[PolarizationCoordinate::StokesI],
    )
    .expect("routing");
    let cf = Spheroidal::new(&geometry, &polarization);
    MeasurementOperator::new(geometry, basis, polarization, Box::new(cf), precision)
}

/// A cube resampler mapping native channel `c` to plane `c / 2`.
fn cube() -> SpectralResampler {
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

#[allow(clippy::too_many_arguments)]
fn run(
    operator: &MeasurementOperator,
    resampler: &SpectralResampler,
    source: &mut Rows,
    partition: Partition,
    residency: Residency,
    workers: usize,
    model: Option<&casa_imaging_runtime::pass::ModelPreparation<'_>>,
    modes: ModeSet,
) -> NormalImages {
    let weighting = WeightingGeneration::Natural { taper: None };
    let pass = MajorCyclePass {
        operator,
        resampler,
        weighting: &weighting,
        modes,
        model,
        partition,
        residency,
    };
    let team = WorkerTeam::new(workers).expect("team");
    let mut waves = Vec::new();
    let summary = run_major_cycle(
        &pass,
        source,
        &team,
        &Cancel::new(),
        &mut |images| {
            waves.push(images);
            Ok(())
        },
        None,
    )
    .expect("pass");
    assert!(summary.samples > 0 && summary.blocks > 0);
    let mut joined = waves.remove(0);
    for wave in waves {
        assert_eq!(
            wave.first_plane,
            joined.first_plane + joined.planes.len() as u32
        );
        joined.planes.extend(wave.planes);
    }
    joined
}

fn worst_relative(a: &NormalImages, b: &NormalImages) -> f64 {
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

#[test]
fn plane_partition_is_bitwise_invariant_across_workers_and_waves_in_f64() {
    let operator = operator(GridPrecision::F64, Basis::ChannelLocal { planes: 3 });
    let resampler = cube();
    let mut rows = Rows::random(400, 3, operator.geometry());
    let reference = run(
        &operator,
        &resampler,
        &mut rows,
        Partition::Planes { owners: 1 },
        Residency::All,
        1,
        None,
        ModeSet::DATA_PSF,
    );
    for (workers, residency) in [
        (3, Residency::All),
        (2, Residency::All),
        (2, Residency::Waves { planes_per_wave: 2 }),
        (1, Residency::Waves { planes_per_wave: 1 }),
    ] {
        let images = run(
            &operator,
            &resampler,
            &mut rows,
            Partition::Planes { owners: workers },
            residency,
            workers,
            None,
            ModeSet::DATA_PSF,
        );
        assert_eq!(images, reference, "{workers} workers, {residency:?}");
    }
}

#[test]
fn region_partition_matches_one_owner() {
    for (precision, tolerance) in [(GridPrecision::F64, 1.0e-6), (GridPrecision::F32, 1.0e-4)] {
        let operator = operator(precision, Basis::Constant);
        let resampler = SpectralResampler::direct(Basis::Constant).expect("direct");
        let mut rows = Rows::random(500, 5, operator.geometry());
        let reference = run(
            &operator,
            &resampler,
            &mut rows,
            Partition::Planes { owners: 1 },
            Residency::All,
            1,
            None,
            ModeSet::DATA_PSF,
        );
        for workers in [2, 4] {
            let images = run(
                &operator,
                &resampler,
                &mut rows,
                Partition::regions(operator.geometry(), workers, 3),
                Residency::All,
                workers,
                None,
                ModeSet::DATA_PSF,
            );
            let worst = worst_relative(&reference, &images);
            assert!(
                worst <= tolerance,
                "{precision:?} {workers} regions: {worst}"
            );
        }
    }
}

/// Data that are exactly the prediction of a model leave no residual:
/// predict the model at every native sample, then run a residual pass
/// with the same model.
#[test]
fn residual_pass_of_the_predicted_model_vanishes() {
    let operator = operator(GridPrecision::F64, Basis::Constant);
    let resampler = SpectralResampler::direct(Basis::Constant).expect("direct");
    let mut rows = Rows::random(300, 11, operator.geometry());
    let mut rng = Rng(19);
    let model = ModelImages {
        first_plane: 0,
        planes: vec![ModelPlane {
            images: vec![Array2::from_shape_fn((IMAGE, IMAGE), |_| {
                if rng.unit() < 0.02 {
                    rng.signed() as f32
                } else {
                    0.0
                }
            })],
        }],
    };
    let prepared = operator
        .prepare_model(&model, ModelPrescale::Unit)
        .expect("model");
    // Replace each row's data with the prediction at its native channels.
    let weighting = WeightingGeneration::Natural { taper: None };
    let mut backend = CpuBackend::new();
    let mut buffer = SampleBuffer::new(2);
    let frequencies = frequencies();
    let mut largest = 0.0_f64;
    for row in &mut rows.rows {
        for (channel, frequency_hz) in frequencies.iter().enumerate() {
            let probe = NativeRowProbe::new(row, channel, *frequency_hz);
            buffer.clear();
            resampler
                .place(&operator, &weighting, &probe.row(), &mut buffer)
                .expect("place");
            if buffer.is_empty() {
                continue;
            }
            let mut predicted = vec![Complex32::default(); 2];
            backend
                .apply(
                    &buffer.block(),
                    operator.cf(),
                    casa_imaging_operator::Work::Predict {
                        model: &prepared,
                        out: &mut predicted,
                    },
                )
                .expect("predict");
            row.values[channel * 2..channel * 2 + 2].copy_from_slice(&predicted);
            largest = largest.max(f64::from(predicted[0].norm()));
        }
    }
    let prepare = |planes: PlaneRange| -> Result<_, PassError> {
        assert_eq!(planes, PlaneRange::single(0));
        Ok(prepared.clone())
    };
    let residual = run(
        &operator,
        &resampler,
        &mut rows,
        Partition::regions(operator.geometry(), 2, 3),
        Residency::All,
        2,
        Some(&prepare),
        ModeSet::DATA,
    );
    let sumwt = residual.data_sumwt(0, 0, 0);
    let peak = residual
        .data(0, 0, 0)
        .iter()
        .fold(0.0_f64, |m, v| m.max(f64::from(*v).abs()))
        / sumwt;
    assert!(largest > 0.0);
    assert!(peak < 1.0e-5 * largest, "residual peak {peak} of {largest}");
}

/// A linear cube whose channels are 1.5 native widths wide and start off the
/// native grid, so output samples fall between native channels.
fn offset_linear_cube() -> SpectralResampler {
    SpectralResampler::channel_local(
        SpectralAxis::new(FIRST_HZ + 1.3 * WIDTH_HZ, 1.5 * WIDTH_HZ, 3).expect("axis"),
        SpectralKernel::Linear,
    )
}

/// A sparse random model of `planes` cube planes.
fn sparse_model(planes: usize, seed: u64) -> ModelImages {
    let mut rng = Rng(seed);
    ModelImages {
        first_plane: 0,
        planes: (0..planes)
            .map(|_| ModelPlane {
                images: vec![Array2::from_shape_fn((IMAGE, IMAGE), |_| {
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
fn window_of(
    operator: &MeasurementOperator,
    model: &ModelImages,
    planes: PlaneRange,
) -> casa_imaging_operator::PreparedModelGrids {
    let window = ModelImages {
        first_plane: planes.start,
        planes: model.planes[planes.start as usize..planes.end as usize].to_vec(),
    };
    operator
        .prepare_model(&window, ModelPrescale::Unit)
        .expect("model window")
}

fn native_row<'a>(row: &'a Row, frequencies: &'a [f64]) -> casa_imaging_operator::NativeRow<'a> {
    casa_imaging_operator::NativeRow {
        uvw_m: row.uvw_m,
        phase_shift_m: row.phase_shift_m,
        frequencies_hz: frequencies,
        values: &row.values,
        weights: &row.weights,
        flags: &row.flags,
        row_flag: false,
        context: RowContext {
            time_s: 0.0,
            antennas: [0, 1],
            parallactic_angle_rad: [0.0; 2],
            field: 0,
            pointing_offset_rad: [0.0; 2],
        },
    }
}

/// CASA forms a linear cube's residual at native channels
/// (`interpolateFrequencyFromgrid`, `SIMapperCollection::grid`): data that
/// are the model's native-channel prediction leave exactly nothing, even
/// where output samples fall between native channels, in one resident pass
/// and in waves whose model windows carry a one-plane halo.
#[test]
fn linear_cube_residual_of_the_predicted_model_vanishes_at_native_channels() {
    let operator = operator(GridPrecision::F64, Basis::ChannelLocal { planes: 3 });
    let resampler = offset_linear_cube();
    assert!(resampler.forms_native_residuals());
    let model = sparse_model(3, 23);
    let full = window_of(&operator, &model, PlaneRange::new(0, 3));
    let mut rows = Rows::random(300, 29, operator.geometry());
    let frequencies = frequencies();
    let mut backend = CpuBackend::new();
    let mut scratch = casa_imaging_operator::PredictionScratch::default();
    let mut largest = 0.0_f32;
    for row in &mut rows.rows {
        let mut predicted = vec![Complex32::default(); row.values.len()];
        resampler
            .predict_row(
                &operator,
                &mut backend,
                &full,
                &native_row(row, &frequencies),
                &mut scratch,
                &mut predicted,
            )
            .expect("predict");
        largest = predicted.iter().fold(largest, |m, v| m.max(v.norm()));
        row.values = predicted;
    }
    assert!(largest > 0.0);
    let dirty = run(
        &operator,
        &resampler,
        &mut rows,
        Partition::Planes { owners: 1 },
        Residency::All,
        1,
        None,
        ModeSet::DATA,
    );
    assert!(
        dirty
            .planes
            .iter()
            .flat_map(|plane| &plane.data)
            .any(|image| image.iter().any(|value| *value != 0.0)),
        "the predicted data image something"
    );
    let prepare =
        |planes: PlaneRange| -> Result<_, PassError> { Ok(window_of(&operator, &model, planes)) };
    for (workers, residency) in [
        (1, Residency::All),
        (2, Residency::Waves { planes_per_wave: 1 }),
        (3, Residency::Waves { planes_per_wave: 2 }),
    ] {
        let residual = run(
            &operator,
            &resampler,
            &mut rows,
            Partition::Planes { owners: workers },
            residency,
            workers,
            Some(&prepare),
            ModeSet::DATA,
        );
        for (index, plane) in residual.planes.iter().enumerate() {
            for image in &plane.data {
                assert!(
                    image.iter().all(|value| *value == 0.0),
                    "{residency:?}: plane {index} keeps a residual"
                );
            }
        }
    }
}

/// With a model, a linear cube's residual pass in waves equals one resident
/// pass bit for bit: each wave's model window holds the neighbouring planes
/// its native-channel predictions interpolate.
#[test]
fn linear_cube_residual_waves_equal_one_resident_pass() {
    let operator = operator(GridPrecision::F64, Basis::ChannelLocal { planes: 3 });
    let resampler = offset_linear_cube();
    let model = sparse_model(3, 31);
    let mut rows = Rows::random(300, 37, operator.geometry());
    let prepare =
        |planes: PlaneRange| -> Result<_, PassError> { Ok(window_of(&operator, &model, planes)) };
    let reference = run(
        &operator,
        &resampler,
        &mut rows,
        Partition::Planes { owners: 1 },
        Residency::All,
        1,
        Some(&prepare),
        ModeSet::DATA,
    );
    for (workers, residency) in [
        (2, Residency::Waves { planes_per_wave: 1 }),
        (3, Residency::Waves { planes_per_wave: 2 }),
    ] {
        let images = run(
            &operator,
            &resampler,
            &mut rows,
            Partition::Planes { owners: workers },
            residency,
            workers,
            Some(&prepare),
            ModeSet::DATA,
        );
        assert_eq!(images, reference, "{workers} workers, {residency:?}");
    }
}

/// One row restricted to one native channel, for the prediction probe.
struct NativeRowProbe<'a> {
    row: &'a Row,
    channel: usize,
    frequency_hz: [f64; 1],
}

impl<'a> NativeRowProbe<'a> {
    fn new(row: &'a Row, channel: usize, frequency_hz: f64) -> Self {
        Self {
            row,
            channel,
            frequency_hz: [frequency_hz],
        }
    }

    fn row(&self) -> casa_imaging_operator::NativeRow<'_> {
        let cells = self.channel * 2..self.channel * 2 + 2;
        casa_imaging_operator::NativeRow {
            uvw_m: self.row.uvw_m,
            phase_shift_m: self.row.phase_shift_m,
            frequencies_hz: &self.frequency_hz,
            values: &self.row.values[cells.clone()],
            weights: &self.row.weights[cells],
            flags: &[false, false],
            row_flag: false,
            context: RowContext {
                time_s: 0.0,
                antennas: [0, 1],
                parallactic_angle_rad: [0.0; 2],
                field: 0,
                pointing_offset_rad: [0.0; 2],
            },
        }
    }
}

#[test]
fn streamed_density_grid_equals_the_one_shot_build() {
    let operator = operator(GridPrecision::F64, Basis::ChannelLocal { planes: 3 });
    let resampler = cube();
    let mut rows = Rows::random(250, 23, operator.geometry());
    // Two padding planes on each side of the three output channels.
    let shape = DensityGridShape {
        width: IMAGE,
        height: IMAGE,
        planes: 7,
        padding: 2,
        increment_rad: INCREMENT_RAD,
        rule: DensityCellRule::Cube,
    };
    let mut buffer = SampleBuffer::new(1);
    let frequencies = frequencies();
    for row in &rows.rows {
        let header = casa_imaging_operator::NativeRow {
            uvw_m: row.uvw_m,
            phase_shift_m: row.phase_shift_m,
            frequencies_hz: &frequencies,
            values: &row.values,
            weights: &row.weights,
            flags: &row.flags,
            row_flag: false,
            context: RowContext {
                time_s: 0.0,
                antennas: [0, 1],
                parallactic_angle_rad: [0.0; 2],
                field: 0,
                pointing_offset_rad: [0.0; 2],
            },
        };
        resampler
            .place_density(&operator, &header, &shape, &mut buffer)
            .expect("density");
    }
    let expected = build_density_grid(std::iter::once(buffer.block()), shape);
    for workers in [1, 3] {
        let team = WorkerTeam::new(workers).expect("team");
        let streamed = run_density_pass(
            &operator,
            &resampler,
            shape,
            &mut rows,
            &team,
            &Cancel::new(),
        )
        .expect("density pass");
        assert_eq!(streamed, expected, "{workers} workers");
    }
}

#[test]
fn a_cancelled_pass_stops_with_a_typed_error() {
    let operator = operator(GridPrecision::F64, Basis::Constant);
    let resampler = SpectralResampler::direct(Basis::Constant).expect("direct");
    let mut rows = Rows::random(100, 29, operator.geometry());
    let weighting = WeightingGeneration::Natural { taper: None };
    let pass = MajorCyclePass {
        operator: &operator,
        resampler: &resampler,
        weighting: &weighting,
        modes: ModeSet::DATA,
        model: None,
        partition: Partition::Planes { owners: 1 },
        residency: Residency::All,
    };
    let cancel = Cancel::new();
    cancel.cancel();
    let team = WorkerTeam::new(2).expect("team");
    let result = run_major_cycle(
        &pass,
        &mut rows,
        &team,
        &cancel,
        &mut |_| panic!("a cancelled pass hands back no images"),
        None,
    );
    assert!(matches!(result, Err(PassError::Cancelled)), "{result:?}");
}

#[test]
fn the_point_source_is_flux_times_the_psf_through_the_pass() {
    // A unit-flux source at the phase centre observed with unit weights:
    // after normalisation the dirty peak equals the PSF peak, 1.
    let operator = operator(GridPrecision::F64, Basis::Constant);
    let resampler = SpectralResampler::direct(Basis::Constant).expect("direct");
    let mut rows = Rows::random(300, 31, operator.geometry());
    for row in &mut rows.rows {
        row.phase_shift_m = 0.0;
        row.values.fill(Complex32::new(1.0, 0.0));
        row.flags.fill(false);
    }
    let images = run(
        &operator,
        &resampler,
        &mut rows,
        Partition::regions(operator.geometry(), 3, 3),
        Residency::All,
        3,
        None,
        ModeSet::DATA_PSF,
    );
    let sumwt = images.psf_sumwt(0, 0, 0);
    let expected = rows
        .rows
        .iter()
        .map(|row| f64::from(row.weights[0]))
        .sum::<f64>()
        * CHANNELS as f64
        * 2.0;
    // Stokes I alone grids both parallel hands, so sumwt counts both; the
    // tap rows are unit-sum to f32 precision.
    assert!(
        (sumwt - expected).abs() <= 1.0e-6 * expected,
        "{sumwt} vs {expected}"
    );
    let centre = IMAGE / 2;
    let dirty = f64::from(images.data(0, 0, 0)[(centre, centre)]) / sumwt;
    let psf = f64::from(images.psf(0, 0, 0)[(centre, centre)]) / sumwt;
    assert!((psf - 1.0).abs() < 1.0e-3, "psf peak {psf}");
    assert!((dirty - psf).abs() < 1.0e-9, "dirty {dirty} psf {psf}");
}

// SPDX-License-Identifier: LGPL-3.0-or-later
//! Partition and worker-count invariance, the streamed density grid,
//! cancellation and the point-source law.

use casa_imaging_operator::{
    Basis, DensityCellRule, DensityGridShape, GridPrecision, ModeSet, PlaneRange, SampleBuffer,
    SpectralResampler, WeightingGeneration, build_density_grid,
};
use casa_imaging_runtime::pass::{
    BackendChoice, BoundedSource, Cancel, MajorCyclePass, NativeBlock, Partition, PassDomain,
    PassError, Residency, SourceError, WorkerTeam, run_density_pass, run_major_cycle,
};
use num_complex::Complex32;

use crate::fixture::{
    CHANNELS, IMAGE, INCREMENT_RAD, Rows, Run, WIDTH_HZ, nearest_cube, operator, planes, run,
    worst_relative,
};

#[test]
fn plane_partition_is_bitwise_invariant_across_workers_and_waves_in_f64() {
    let operator = operator(GridPrecision::F64, Basis::ChannelLocal { planes: 3 });
    let resampler = nearest_cube();
    let mut rows = Rows::random(400, 3, operator.geometry());
    let reference = run(
        &[planes(&operator, &resampler, 1)],
        &mut rows,
        &Run::initial(),
    );
    for (workers, residency) in [
        (3, Residency::All),
        (2, Residency::All),
        (2, Residency::Waves { planes_per_wave: 2 }),
        (1, Residency::Waves { planes_per_wave: 1 }),
    ] {
        let images = run(
            &[planes(&operator, &resampler, workers)],
            &mut rows,
            &Run {
                residency,
                workers,
                ..Run::initial()
            },
        );
        assert_eq!(images, reference, "{workers} workers, {residency:?}");
    }
    // A data and PSF pass reads restricted traversals: no prediction needs
    // a whole row.
    assert!(rows.restricted.iter().all(|restricted| *restricted));
}

#[test]
fn region_partition_matches_one_owner() {
    for (precision, tolerance) in [(GridPrecision::F64, 1.0e-6), (GridPrecision::F32, 1.0e-4)] {
        let operator = operator(precision, Basis::Constant);
        let resampler = SpectralResampler::direct(Basis::Constant).expect("direct");
        let mut rows = Rows::random(500, 5, operator.geometry());
        let reference = run(
            &[planes(&operator, &resampler, 1)],
            &mut rows,
            &Run::initial(),
        );
        for workers in [2, 4] {
            let images = run(
                &[PassDomain {
                    operator: &operator,
                    resampler: &resampler,
                    partition: Partition::regions(&operator, workers),
                }],
                &mut rows,
                &Run {
                    workers,
                    ..Run::initial()
                },
            );
            let worst = worst_relative(&reference[0], &images[0]);
            assert!(
                worst <= tolerance,
                "{precision:?} {workers} regions: {worst}"
            );
        }
    }
}

#[test]
fn streamed_density_grid_equals_the_one_shot_build() {
    let operator = operator(GridPrecision::F64, Basis::ChannelLocal { planes: 3 });
    let resampler = nearest_cube();
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
    for index in 0..rows.rows.len() {
        resampler
            .place_density(&operator, &rows.native(0, index), &shape, &mut buffer)
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
    let domains = [planes(&operator, &resampler, 1)];
    let pass = MajorCyclePass {
        domains: &domains,
        weighting: &weighting,
        modes: ModeSet::DATA,
        model: None,
        residency: Residency::All,
        native_spacing_hz: WIDTH_HZ,
        backend: BackendChoice::Cpu,
    };
    let cancel = Cancel::new();
    cancel.cancel();
    let team = WorkerTeam::new(2).expect("team");
    let result = run_major_cycle(
        &pass,
        &mut rows,
        &team,
        &cancel,
        &mut |_, _| panic!("a cancelled pass hands back no images"),
        None,
    );
    assert!(matches!(result, Err(PassError::Cancelled)), "{result:?}");
}

/// Rows whose source cancels the pass once it has filled `after` blocks.
struct CancellingRows {
    rows: Rows,
    cancel: Cancel,
    after: usize,
    filled: usize,
}

impl BoundedSource for CancellingRows {
    fn begin(&mut self, planes: PlaneRange, restrict: bool) -> Result<(), SourceError> {
        self.rows.begin(planes, restrict)
    }

    fn fill(&mut self, block: &mut NativeBlock) -> Result<bool, SourceError> {
        let more = self.rows.fill(block)?;
        self.filled += usize::from(more);
        if self.filled == self.after {
            self.cancel.cancel();
        }
        Ok(more)
    }
}

/// Cancellation during a pass stops it within one block: the source fills
/// no block after the one during which the pass was cancelled, and no wave
/// hands back images.
#[test]
fn a_pass_cancelled_midway_reads_no_further_block() {
    let operator = operator(GridPrecision::F64, Basis::Constant);
    let resampler = SpectralResampler::direct(Basis::Constant).expect("direct");
    let weighting = WeightingGeneration::Natural { taper: None };
    let domains = [planes(&operator, &resampler, 1)];
    let pass = MajorCyclePass {
        domains: &domains,
        weighting: &weighting,
        modes: ModeSet::DATA,
        model: None,
        residency: Residency::All,
        native_spacing_hz: WIDTH_HZ,
        backend: BackendChoice::Cpu,
    };
    for workers in [1, 3] {
        let cancel = Cancel::new();
        let mut rows = CancellingRows {
            // Nine blocks of the fixture's 37 rows.
            rows: Rows::random(9 * 37, 41, operator.geometry()),
            cancel: cancel.clone(),
            after: 3,
            filled: 0,
        };
        let team = WorkerTeam::new(workers).expect("team");
        let result = run_major_cycle(
            &pass,
            &mut rows,
            &team,
            &cancel,
            &mut |_, _| panic!("a cancelled pass hands back no images"),
            None,
        );
        assert!(matches!(result, Err(PassError::Cancelled)), "{result:?}");
        assert_eq!(rows.filled, 3, "{workers} workers");
    }
}

#[test]
fn the_point_source_is_flux_times_the_psf_through_the_pass() {
    // A unit-flux source at the phase centre observed with unit weights:
    // after normalisation the dirty peak equals the PSF peak, 1.
    let operator = operator(GridPrecision::F64, Basis::Constant);
    let resampler = SpectralResampler::direct(Basis::Constant).expect("direct");
    let mut rows = Rows::random(300, 31, operator.geometry());
    for row in &mut rows.rows {
        row.projections[0].phase_shift_m = 0.0;
        row.values.fill(Complex32::new(1.0, 0.0));
        row.flags.fill(false);
    }
    let images = run(
        &[PassDomain {
            operator: &operator,
            resampler: &resampler,
            partition: Partition::regions(&operator, 3),
        }],
        &mut rows,
        &Run {
            workers: 3,
            ..Run::initial()
        },
    );
    let images = &images[0];
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

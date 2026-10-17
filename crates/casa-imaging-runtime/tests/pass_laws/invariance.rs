// SPDX-License-Identifier: LGPL-3.0-or-later
//! Partition and worker-count invariance, the streamed density grid,
//! cancellation and the point-source law.

use casa_imaging_operator::{
    Basis, DensityCellRule, DensityGridShape, GridPrecision, ModeSet, NormalImages, PlaneRange,
    SampleBuffer, SpectralResampler, WeightingGeneration, build_density_grid,
};
use casa_imaging_runtime::pass::{
    BackendChoice, BlockShape, BoundedSource, CHUNK_PLACEMENTS, Cancel, MajorCyclePass,
    ModelPreparation, NativeBlock, Partition, PassDomain, PassError, Residency, SampleContent,
    SourceError, VisibilitySink, WeightCache, WorkerTeam, needs_row_work, run_density_pass,
    run_major_cycle, run_row_work_pass,
};
use num_complex::Complex32;

use crate::fixture::{
    CHANNELS, IMAGE, INCREMENT_RAD, Rows, Run, WIDTH_HZ, nearest_cube, operator, planes, run,
    sparse_model, try_run, window_of, worst_relative,
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
    // a whole row. It grids the values, so it reads them.
    assert!(rows.restricted.iter().all(|restricted| *restricted));
    assert!(
        rows.contents
            .iter()
            .all(|content| *content == SampleContent::Visibilities)
    );
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
        // Chunks of 8 placements cut each block into many slices; the
        // default and the largest region chunks take a block in one (#736).
        for workers in [2, 4] {
            for chunk_placements in [8, CHUNK_PLACEMENTS, 1 << 18] {
                let images = run(
                    &[PassDomain {
                        operator: &operator,
                        resampler: &resampler,
                        partition: Partition::regions(&operator, workers),
                    }],
                    &mut rows,
                    &Run {
                        workers,
                        chunk_placements,
                        ..Run::initial()
                    },
                );
                let worst = worst_relative(&reference[0], &images[0]);
                assert!(
                    worst <= tolerance,
                    "{precision:?} {workers} regions, chunks of {chunk_placements}: {worst}"
                );
            }
        }
    }
}

/// A region pass reports its gridding work by anchor row, and strips cut to
/// equal shares of it (#736) grid what one owner grids, to rounding. Plane
/// owners report none.
#[test]
fn balanced_regions_from_a_pass_row_work_match_one_owner() {
    let operator = operator(GridPrecision::F64, Basis::Constant);
    let resampler = SpectralResampler::direct(Basis::Constant).expect("direct");
    let mut rows = Rows::random(500, 7, operator.geometry());
    let reference = run(
        &[planes(&operator, &resampler, 1)],
        &mut rows,
        &Run::initial(),
    );
    let regions = [PassDomain {
        operator: &operator,
        resampler: &resampler,
        partition: Partition::regions(&operator, 3),
    }];
    let (_, summary) = try_run(
        &regions,
        &mut rows,
        &Run {
            workers: 3,
            ..Run::initial()
        },
    )
    .expect("pass");
    let [Some(row_work)] = summary.row_work.as_slice() else {
        panic!("one region-split domain reports its work");
    };
    assert_eq!(row_work.len(), operator.geometry().grid_shape()[1]);
    // The spheroidal kernel's 7 × 7 taps for every placed sample.
    assert_eq!(row_work.iter().sum::<u64>(), 49 * summary.samples);
    let balanced = Partition::regions_balanced(&operator, 3, row_work);
    let images = run(
        &[PassDomain {
            operator: &operator,
            resampler: &resampler,
            partition: balanced,
        }],
        &mut rows,
        &Run {
            workers: 3,
            ..Run::initial()
        },
    );
    let worst = worst_relative(&reference[0], &images[0]);
    assert!(worst <= 1.0e-6, "balanced regions: {worst}");

    let cube = crate::fixture::operator(GridPrecision::F64, Basis::ChannelLocal { planes: 3 });
    let cube_resampler = nearest_cube();
    let (_, summary) = try_run(
        &[planes(&cube, &cube_resampler, 2)],
        &mut Rows::random(100, 11, cube.geometry()),
        &Run {
            workers: 2,
            ..Run::initial()
        },
    )
    .expect("pass");
    assert_eq!(
        summary.row_work,
        vec![None],
        "plane owners report no row work"
    );
}

/// The row-work pass measures, from flags and weights alone, the row work a
/// first pass over the same strips reports, on any number of workers; and
/// only region-split domains with several owners ask for it.
#[test]
fn the_row_work_pass_measures_what_a_first_pass_reports() {
    let operator = operator(GridPrecision::F64, Basis::Constant);
    let resampler = SpectralResampler::direct(Basis::Constant).expect("direct");
    let mut rows = Rows::random(500, 31, operator.geometry());
    let regions = [PassDomain {
        operator: &operator,
        resampler: &resampler,
        partition: Partition::regions(&operator, 3),
    }];
    let (_, summary) = try_run(
        &regions,
        &mut rows,
        &Run {
            workers: 3,
            ..Run::initial()
        },
    )
    .expect("pass");
    assert!(summary.row_work.iter().all(Option::is_some));
    let weighting = WeightingGeneration::Natural { taper: None };
    rows.contents.clear();
    for workers in [1, 3] {
        let team = WorkerTeam::new(workers).expect("team");
        let measured = run_row_work_pass(&regions, &weighting, &mut rows, &team, &Cancel::new())
            .expect("row-work pass");
        assert_eq!(measured, summary.row_work, "{workers} workers");
    }
    assert_eq!(rows.contents, [SampleContent::FlagsAndWeights; 2]);
    assert!(needs_row_work(&regions));

    let cube = crate::fixture::operator(GridPrecision::F64, Basis::ChannelLocal { planes: 3 });
    let cube_resampler = nearest_cube();
    assert!(!needs_row_work(&[planes(&cube, &cube_resampler, 2)]));
    assert!(!needs_row_work(&[PassDomain {
        operator: &operator,
        resampler: &resampler,
        partition: Partition::regions(&operator, 1),
    }]));
}

/// The density pass reads flags and weights only: its source delivers zero
/// values, and the grid equals the one built from the rows' own values.
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
    assert_eq!(rows.contents, [SampleContent::FlagsAndWeights; 2]);
}

/// Kept weights place exactly what weighing places: the pass that fills the
/// cache and the passes that read it grid the same images, bit for bit, as
/// one that weighs every sample, flagged samples included.
#[test]
fn kept_weights_grid_exactly_what_weighing_grids() {
    let operator = operator(GridPrecision::F64, Basis::Constant);
    let resampler = SpectralResampler::direct(Basis::Constant).expect("direct");
    let mut rows = Rows::random(300, 43, operator.geometry());
    let reference = run(
        &[planes(&operator, &resampler, 1)],
        &mut rows,
        &Run::initial(),
    );
    let cache = WeightCache::new(rows.rows.len() * CHANNELS);
    for (pass, workers) in [(0, 1), (1, 3), (2, 1)] {
        let images = run(
            &[planes(&operator, &resampler, 1)],
            &mut rows,
            &Run {
                workers,
                weights: Some(&cache),
                ..Run::initial()
            },
        );
        assert_eq!(images, reference, "pass {pass}, {workers} workers");
        assert!(cache.filled(), "a complete pass fills the cache");
    }
}

/// A residual pass of `prepare`'s model under `weighting`, reading or
/// filling `weights`: its images, and the prediction it hands its sink for
/// each physical row.
fn writing_pass(
    domains: &[PassDomain<'_>],
    rows: &mut Rows,
    (weighting, weights): (&WeightingGeneration, Option<&WeightCache>),
    prepare: &ModelPreparation<'_>,
    workers: usize,
) -> (Vec<NormalImages>, Vec<Option<Vec<Complex32>>>) {
    let pass = MajorCyclePass {
        domains,
        weighting,
        modes: ModeSet::DATA,
        model: Some(prepare),
        residency: Residency::All,
        native_spacing_hz: WIDTH_HZ,
        backend: BackendChoice::Cpu,
        metal_stage_timing: false,
        weights,
        chunk_placements: CHUNK_PLACEMENTS,
    };
    let mut written = vec![None; rows.rows.len()];
    let mut write = |block: &NativeBlock, predictions: &[Complex32]| {
        let cells = block.channels() * block.correlations();
        for index in 0..block.len() {
            let row = block.header(index).address.physical_row as usize;
            written[row] = Some(predictions[index * cells..(index + 1) * cells].to_vec());
        }
        Ok::<_, SourceError>(())
    };
    let mut sink = VisibilitySink {
        predictions: true,
        write: &mut write,
    };
    let mut images = Vec::new();
    run_major_cycle(
        &pass,
        rows,
        &WorkerTeam::new(workers).expect("team"),
        &Cancel::new(),
        &mut |_, wave| {
            images.push(wave);
            Ok(())
        },
        Some(&mut sink),
    )
    .expect("pass");
    (images, written)
}

/// #718 (Astra gate answer 4): kept weights under density weighting grid
/// what weighing grids, bit for bit, when the passes that read them meet
/// the rows in other blocks than the pass that filled them, and with a
/// model whose predictions the passes write: the sink receives the same
/// prediction for every row.
#[test]
fn kept_density_weights_hold_across_blocks_and_model_writing() {
    let operator = operator(GridPrecision::F64, Basis::Constant);
    let resampler = SpectralResampler::direct(Basis::Constant).expect("direct");
    let mut rows = Rows::random(300, 47, operator.geometry());
    let shape = DensityGridShape {
        width: IMAGE,
        height: IMAGE,
        planes: 1,
        padding: 0,
        increment_rad: INCREMENT_RAD,
        rule: DensityCellRule::Standard,
    };
    let team = WorkerTeam::new(1).expect("team");
    let grid = run_density_pass(
        &operator,
        &resampler,
        shape,
        &mut rows,
        &team,
        &Cancel::new(),
    )
    .expect("density pass");
    let weighting = WeightingGeneration::density(grid, Some(0.5), None, None).expect("Briggs");
    let model = sparse_model(IMAGE, 1, 53);
    let prepare = |_: usize, planes: PlaneRange| -> Result<_, PassError> {
        Ok(window_of(&operator, &model, planes))
    };
    let domains = [planes(&operator, &resampler, 1)];
    let (reference, predicted) = writing_pass(&domains, &mut rows, (&weighting, None), &prepare, 1);
    assert!(
        predicted.iter().all(Option::is_some),
        "every row is written"
    );
    let cache = WeightCache::new(rows.rows.len() * CHANNELS);
    for (pass, block_rows, workers) in [(0, 37, 1), (1, 11, 3), (2, 23, 1)] {
        rows.block_rows = block_rows;
        let (images, written) = writing_pass(
            &domains,
            &mut rows,
            (&weighting, Some(&cache)),
            &prepare,
            workers,
        );
        let label = format!("pass {pass}, blocks of {block_rows}, {workers} workers");
        assert!(cache.filled(), "{label}: a complete pass fills the cache");
        assert_eq!(images, reference, "{label}: images");
        assert_eq!(written, predicted, "{label}: predictions");
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
        metal_stage_timing: false,
        weights: None,
        chunk_placements: CHUNK_PLACEMENTS,
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
    fn begin(
        &mut self,
        planes: PlaneRange,
        restrict: bool,
        content: SampleContent,
    ) -> Result<(), SourceError> {
        self.rows.begin(planes, restrict, content)
    }

    fn fill(&mut self, block: &mut NativeBlock) -> Result<bool, SourceError> {
        let more = self.rows.fill(block)?;
        self.filled += usize::from(more);
        if self.filled == self.after {
            self.cancel.cancel();
        }
        Ok(more)
    }

    fn maximum_block(&self) -> BlockShape {
        self.rows.maximum_block()
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
        metal_stage_timing: false,
        weights: None,
        chunk_placements: CHUNK_PLACEMENTS,
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

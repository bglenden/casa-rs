// SPDX-License-Identifier: LGPL-3.0-or-later
//! Partition and worker-count invariance, the streamed density grid,
//! cancellation and the point-source law.

use casa_imaging_model::PolarizationCoordinate;
use casa_imaging_operator::{
    Basis, CellHold, DensityCellRule, DensityGridShape, GridGeometry, GridPadding, GridPrecision,
    ImageExtent, MeasurementOperator, ModeSet, NormalImages, PaddingFactor, PlaneRange,
    PolarizationRouting, SampleBuffer, SpectralResampler, WPlaneCount, WPlanes,
    WeightingGeneration, build_density_grid,
};
use casa_imaging_runtime::pass::{
    BackendChoice, BlockShape, BoundedSource, CHUNK_PLACEMENTS, Cancel, MajorCyclePass,
    ModelPreparation, NativeBlock, PLACEMENT_WORK, Partition, PassDomain, PassError, Residency,
    SampleContent, SourceError, VisibilitySink, WEIGHT_SUM_WORK, WeightCache, WorkerTeam,
    needs_row_work, run_density_pass, run_major_cycle, run_row_work_pass,
};
use casa_types::Correlation;
use num_complex::Complex32;

use crate::fixture::{
    CHANNELS, FIRST_HZ, IMAGE, INCREMENT_RAD, Rows, Run, WIDTH_HZ, keyed_weight_groups,
    keyed_weights_operator, nearest_cube, operator, panicking_weights_operator, planes, run,
    sparse_model, test_kernels_operator, try_run, try_run_on, window_of, worst_relative,
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

/// The groups an owner's weight table holds before it grids them
/// (`WEIGHT_GROUPS`, #787), which the pass keeps to itself.
const WEIGHT_TABLE_GROUPS: usize = 1 << 14;

/// f32's unit roundoff, 2^-24: the most rounding to f32 moves a value,
/// relative to it.
const F32_ROUNDING: f64 = f32::EPSILON as f64 / 2.0;

/// f64's own rounding over the sums and transforms of a pass of the cube
/// below, about 10^5 samples a plane: far below f32's.
const F64_SLACK: f64 = 1.0e-10;

/// #792 review (Astra), #810: on a weighted cube whose weight tables fill,
/// one owner holding every plane, three owners, and waves of one and two
/// planes grid the same weight images and weight `sumwt` to the rounding
/// of their summed groups, and the same data and PSF bit for bit. The
/// keyed kernels give each row's samples a cell from its w; every plane
/// has more groups than a table holds, thousands of them gathering rows
/// far apart, so each owner and wave cuts their sums into other batches.
///
/// The tolerance is the arithmetic's, not the probe's (Astra saw 1.1e-7):
/// - every cell lends the spheroidal's real taps at the uv origin, so a
///   plane's weight grid is its groups' summed weights times one stamp;
/// - a group's f32 weights sum in f64 exactly, and each batch rounds its
///   partial sum to f32 once, within 2^-24 of it: the weights are
///   positive, so a plane's total and its weight `sumwt` are within 2^-24
///   of the exact ones however the batches split them, and two runs agree
///   within 2^-23;
/// - the weight images are one linear map of those totals, each rounded to
///   f32 once more, so two agree within 2^-22 of their peak.
///
/// Data and PSF do not pass through the table.
#[test]
fn weighted_cube_weight_images_agree_across_owners_and_waves_to_the_rounding_of_their_sums() {
    const CELLS: u16 = 12_000;
    let cube = keyed_weights_operator(Basis::ChannelLocal { planes: 3 }, CELLS);
    let resampler = nearest_cube();
    let mut rows = Rows::random(40_000, 67, cube.geometry());
    for (plane, (groups, gathered)) in keyed_weight_groups(&rows, CELLS).into_iter().enumerate() {
        assert!(
            groups > WEIGHT_TABLE_GROUPS && gathered > groups / 3,
            "plane {plane}: {groups} groups fill a table, {gathered} of them gather several rows"
        );
    }
    let pass = |workers, residency| Run {
        workers,
        residency,
        modes: ModeSet::ALL,
        ..Run::initial()
    };
    let reference = run(
        &[planes(&cube, &resampler, 1)],
        &mut rows,
        &pass(1, Residency::All),
    );
    for (workers, residency) in [
        (3, Residency::All),
        (1, Residency::Waves { planes_per_wave: 1 }),
        (3, Residency::Waves { planes_per_wave: 1 }),
        (1, Residency::Waves { planes_per_wave: 2 }),
        (2, Residency::Waves { planes_per_wave: 2 }),
    ] {
        let images = run(
            &[planes(&cube, &resampler, workers)],
            &mut rows,
            &pass(workers, residency),
        );
        for (plane, (ours, theirs)) in images[0]
            .planes
            .iter()
            .zip(&reference[0].planes)
            .enumerate()
        {
            let label = format!("{workers} owners, {residency:?}, plane {plane}");
            assert_eq!(ours.data, theirs.data, "{label}: data");
            assert_eq!(ours.psf, theirs.psf, "{label}: PSF");
            let terms = ours.data.len() + ours.psf.len();
            assert_eq!(ours.sumwt[..terms], theirs.sumwt[..terms], "{label}");
            assert!(!theirs.weight.is_empty(), "{label}: a weight image");
            for (image, against) in ours.weight.iter().zip(&theirs.weight) {
                let peak = against
                    .iter()
                    .fold(0.0_f64, |m, v| m.max(f64::from(v.abs())));
                assert!(peak > 0.0, "{label}: a weight image");
                let worst = image
                    .iter()
                    .zip(against)
                    .map(|(a, b)| f64::from(a - b).abs() / peak)
                    .fold(0.0, f64::max);
                assert!(
                    worst <= 4.0 * F32_ROUNDING + F64_SLACK,
                    "{label}: weight image {worst:e}"
                );
            }
            for (sum, against) in ours.sumwt[terms..].iter().zip(&theirs.sumwt[terms..]) {
                assert!(
                    (sum - against).abs() <= (2.0 * F32_ROUNDING + F64_SLACK) * against.abs(),
                    "{label}: weight sumwt {sum} against {against}"
                );
            }
        }
    }
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

/// #449: a region pass on the CPU that pipelines its owners over two sets of
/// chunks, each owner going on to its next slice once its own earlier
/// slices are done, grids exactly what one set grids, bit for bit: every
/// owner accumulates the same samples in the same order. Over blocks of the
/// fixture's 37 rows, in many small slices and in one slice a block, on two
/// and four owners: data and PSF; data with a model subtracted; every mode
/// with a weight image, which the centre row's owner grids from every
/// placement on its own lane, on equal strips and on the strips #783 cuts
/// for that work; and a three-plane cube in one-plane waves, which take the
/// owners and chunks back at every wave (#784 review, Astra 4 and Fable 2).
#[test]
fn pipelined_owners_grid_what_one_set_grids() {
    #[derive(Clone, Copy)]
    struct PipelineCase<'a> {
        name: &'a str,
        operator: &'a MeasurementOperator,
        resampler: &'a SpectralResampler,
        modes: ModeSet,
        model: Option<&'a ModelPreparation<'a>>,
        residency: Residency,
        weighed: bool,
    }

    let operator = operator(GridPrecision::F64, Basis::Constant);
    let weighted = test_kernels_operator(true, None);
    let cube = crate::fixture::operator(GridPrecision::F64, Basis::ChannelLocal { planes: 3 });
    let resampler = SpectralResampler::direct(Basis::Constant).expect("direct");
    let cube_resampler = nearest_cube();
    let mut rows = Rows::random(500, 13, operator.geometry());
    let model = sparse_model(IMAGE, 1, 59);
    let prepare = |_: usize, planes: PlaneRange| -> Result<_, PassError> {
        Ok(window_of(&operator, &model, planes))
    };
    let case = |name, operator, resampler, modes, model| PipelineCase {
        name,
        operator,
        resampler,
        modes,
        model,
        residency: Residency::All,
        weighed: false,
    };
    let cases = [
        case(
            "data and PSF",
            &operator,
            &resampler,
            ModeSet::DATA_PSF,
            None,
        ),
        case(
            "a model",
            &operator,
            &resampler,
            ModeSet::DATA,
            Some(&prepare),
        ),
        case("a weight image", &weighted, &resampler, ModeSet::ALL, None),
        PipelineCase {
            weighed: true,
            ..case(
                "a weight image on weighed strips",
                &weighted,
                &resampler,
                ModeSet::ALL,
                None,
            )
        },
        PipelineCase {
            residency: Residency::Waves { planes_per_wave: 1 },
            ..case(
                "cube waves",
                &cube,
                &cube_resampler,
                ModeSet::DATA_PSF,
                None,
            )
        },
    ];
    for workers in [2, 4] {
        for chunk_placements in [8, CHUNK_PLACEMENTS] {
            for PipelineCase {
                name,
                operator,
                resampler,
                modes,
                model,
                residency,
                weighed,
            } in cases
            {
                let partition = if weighed {
                    weighed_strips(operator, resampler, workers, modes, &mut rows)
                } else {
                    Partition::regions(operator, workers)
                };
                let domains = [PassDomain {
                    operator,
                    resampler,
                    partition,
                }];
                let pass = |chunk_sets| Run {
                    workers,
                    chunk_placements,
                    chunk_sets,
                    modes,
                    model,
                    residency,
                    ..Run::initial()
                };
                let label = format!("{name}, {workers} owners, chunks of {chunk_placements}");
                let (one, summary) = try_run(&domains, &mut rows, &pass(1)).expect("one set");
                assert!(summary.blocks > 1, "{label}: several blocks");
                let (two, _) = try_run(&domains, &mut rows, &pass(2)).expect("two sets");
                assert_eq!(one, two, "{label}");
                if modes.weight {
                    assert!(
                        two.iter()
                            .flat_map(|images| &images.planes)
                            .flat_map(|plane| &plane.weight)
                            .any(|image| image.iter().any(|value| *value != 0.0)),
                        "{label}: a weight image"
                    );
                }
            }
        }
    }
}

/// The strips a pass of `modes` that grids the weight image cuts on `owners`
/// region owners, from the row work it measures over `rows` first (#783).
/// The centre row's owner, which sums the weight image, carries that work
/// too. Since the weight image is summed by contribution (#787), that work
/// is one unit a placement, so the strips may equal the data's.
fn weighed_strips(
    operator: &MeasurementOperator,
    resampler: &SpectralResampler,
    owners: usize,
    modes: ModeSet,
    rows: &mut Rows,
) -> Partition {
    let measured = run_row_work_pass(
        &[PassDomain {
            operator,
            resampler,
            partition: Partition::regions(operator, owners),
        }],
        &WeightingGeneration::Natural { taper: None },
        rows,
        &WorkerTeam::new(owners).expect("team"),
        &Cancel::new(),
    )
    .expect("row-work pass");
    let data = measured.rows[0].as_deref().expect("a histogram");
    let work = measured.modes[0]
        .weighed(data, modes)
        .expect("a weight pass weighs its strips");
    Partition::regions_balanced(operator, owners, &work)
}

/// #784 review (Astra 1, Fable 1): an owner that panics in a pipelined pass,
/// whether or not its lane has a second set queued behind the one it
/// panics in, fails the pass with [`PassError::OwnerPanicked`] within a
/// bound, rather than leaving the placer waiting for that lane's sets. The
/// test kernels panic at the `n`-th lend of their imaging taps, which only
/// the owners' accumulation asks for.
#[test]
fn a_pipelined_pass_whose_owner_panics_fails_with_the_typed_error() {
    for panic_at in [1, 40, 400] {
        let (sender, receiver) = std::sync::mpsc::channel();
        std::thread::spawn(move || {
            let operator = test_kernels_operator(false, Some(panic_at));
            let resampler = SpectralResampler::direct(Basis::Constant).expect("direct");
            let domains = [PassDomain {
                operator: &operator,
                resampler: &resampler,
                partition: Partition::regions(&operator, 2),
            }];
            let mut rows = Rows::random(500, 13, operator.geometry());
            let run = Run {
                workers: 2,
                modes: ModeSet::DATA,
                chunk_placements: 8,
                chunk_sets: 2,
                ..Run::initial()
            };
            let outcome = match try_run(&domains, &mut rows, &run) {
                Err(PassError::OwnerPanicked) => Ok(()),
                Err(other) => Err(other.to_string()),
                Ok(_) => Err("the pass succeeded".to_owned()),
            };
            sender.send(outcome).expect("the law waits");
        });
        let outcome = receiver
            .recv_timeout(std::time::Duration::from_secs(60))
            .unwrap_or_else(|_| panic!("panic at lend {panic_at}: the pass hung"));
        assert_eq!(outcome, Ok(()), "panic at lend {panic_at}");
    }
}

/// #792 review (Astra 1): a panic while the weight owner grids its summed
/// weight image at the wave's end fails the pass with
/// [`PassError::OwnerPanicked`] within a bound. That holds on one set of
/// chunks and on two (pipelined), on region owners and on plane owners. The
/// test kernels' weight taps panic when lent, which only that grid does.
#[test]
fn a_panic_gridding_the_summed_weight_image_fails_with_the_typed_error() {
    for (chunk_sets, planes) in [(1, false), (2, false), (1, true)] {
        let (sender, receiver) = std::sync::mpsc::channel();
        std::thread::spawn(move || {
            let operator = panicking_weights_operator();
            let resampler = SpectralResampler::direct(Basis::Constant).expect("direct");
            let partition = if planes {
                Partition::Planes { owners: 2 }
            } else {
                Partition::regions(&operator, 2)
            };
            let domains = [PassDomain {
                operator: &operator,
                resampler: &resampler,
                partition,
            }];
            let mut rows = Rows::random(100, 13, operator.geometry());
            let run = Run {
                workers: 2,
                modes: ModeSet::ALL,
                chunk_placements: 8,
                chunk_sets,
                ..Run::initial()
            };
            let outcome = match try_run(&domains, &mut rows, &run) {
                Err(PassError::OwnerPanicked) => Ok(()),
                Err(other) => Err(other.to_string()),
                Ok(_) => Err("the pass succeeded".to_owned()),
            };
            sender.send(outcome).expect("the law waits");
        });
        let label = format!("{chunk_sets} sets, plane owners {planes}");
        let outcome = receiver
            .recv_timeout(std::time::Duration::from_secs(60))
            .unwrap_or_else(|_| panic!("{label}: the pass hung"));
        assert_eq!(outcome, Ok(()), "{label}");
    }
}

/// #784 review (Astra 2, Fable 4): a pass asked for two sets of chunks that
/// cannot wait off its team's threads places into one set and grids what
/// one set grids. That holds on a team of one, and for two passes started
/// from the two threads of the team they run on, which would otherwise
/// each hold a thread their owners need.
#[test]
fn a_pass_that_cannot_wait_off_its_team_grids_on_one_set() {
    let (sender, receiver) = std::sync::mpsc::channel();
    std::thread::spawn(move || {
        let operator = operator(GridPrecision::F64, Basis::Constant);
        let resampler = SpectralResampler::direct(Basis::Constant).expect("direct");
        let domains = [PassDomain {
            operator: &operator,
            resampler: &resampler,
            partition: Partition::regions(&operator, 2),
        }];
        let pass = |workers, chunk_sets| Run {
            workers,
            chunk_placements: 8,
            chunk_sets,
            ..Run::initial()
        };
        let mut rows = Rows::random(300, 13, operator.geometry());
        let (reference, _) = try_run(&domains, &mut rows, &pass(2, 1)).expect("one set");
        let (alone, _) = try_run(&domains, &mut rows, &pass(1, 2)).expect("a team of one");
        let team = WorkerTeam::new(2).expect("team");
        let mut nested = [
            (Rows::random(300, 13, operator.geometry()), None),
            (Rows::random(300, 13, operator.geometry()), None),
        ];
        team.for_each_mut(&mut nested, |_, (rows, images)| {
            *images = Some(try_run_on(&domains, rows, &pass(2, 2), &team)?.0);
            Ok::<_, PassError>(())
        })
        .expect("nested passes");
        let outcome = (alone == reference
            && nested
                .iter()
                .all(|(_, images)| images.as_ref() == Some(&reference)))
        .then_some(())
        .ok_or("a pass that cannot pipeline gridded otherwise");
        sender.send(outcome).expect("the law waits");
    });
    let outcome = receiver
        .recv_timeout(std::time::Duration::from_secs(60))
        .expect("the passes hung");
    assert_eq!(outcome, Ok(()));
}

/// #449: a pass whose owners pipeline over two sets of chunks, cancelled
/// midway, stops within one block with the typed error, every owner's task
/// joined, as a pass on one set does.
#[test]
fn a_pipelined_pass_cancelled_midway_stops_within_a_block() {
    let operator = operator(GridPrecision::F64, Basis::Constant);
    let resampler = SpectralResampler::direct(Basis::Constant).expect("direct");
    let weighting = WeightingGeneration::Natural { taper: None };
    let domains = [PassDomain {
        operator: &operator,
        resampler: &resampler,
        partition: Partition::regions(&operator, 3),
    }];
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
        chunk_placements: 8,
        chunk_sets: 2,
    };
    let cancel = Cancel::new();
    let mut rows = CancellingRows {
        rows: Rows::random(9 * 37, 43, operator.geometry()),
        cancel: cancel.clone(),
        after: 3,
        filled: 0,
    };
    let team = WorkerTeam::new(3).expect("team");
    let result = run_major_cycle(
        &pass,
        &mut rows,
        &team,
        &cancel,
        &mut |_, _| panic!("a cancelled pass hands back no images"),
        None,
    );
    assert!(matches!(result, Err(PassError::Cancelled)), "{result:?}");
    assert_eq!(rows.filled, 3);
}

/// A region pass reports its gridding work by anchor row, and strips cut to
/// equal shares of it (#736) grid what one owner grids, to rounding. Plane
/// owners and a lone region owner report none.
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
    // The spheroidal kernel's 7 × 7 taps and a placement's own work for
    // every placed sample.
    assert_eq!(
        row_work.iter().sum::<u64>(),
        (49 + PLACEMENT_WORK) * summary.samples
    );
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
    let (_, summary) = try_run(
        &[PassDomain {
            operator: &operator,
            resampler: &resampler,
            partition: Partition::regions(&operator, 1),
        }],
        &mut rows,
        &Run::initial(),
    )
    .expect("pass");
    assert_eq!(
        summary.row_work,
        vec![None],
        "a lone region owner measures no row work"
    );
}

/// The row work a first pass of `weighting` over `domains` on three workers
/// reports ([`PassSummary::row_work`]), its weights kept in `weights`.
fn first_pass_row_work(
    domains: &[PassDomain<'_>],
    rows: &mut Rows,
    weighting: &WeightingGeneration,
    weights: Option<&WeightCache>,
) -> Vec<Option<Vec<u64>>> {
    let pass = MajorCyclePass {
        domains,
        weighting,
        modes: Run::initial().modes,
        model: None,
        residency: Residency::All,
        native_spacing_hz: WIDTH_HZ,
        backend: BackendChoice::Cpu,
        metal_stage_timing: false,
        weights,
        chunk_placements: CHUNK_PLACEMENTS,
        chunk_sets: 1,
    };
    let team = WorkerTeam::new(3).expect("team");
    run_major_cycle(&pass, rows, &team, &Cancel::new(), &mut |_, _| Ok(()), None)
        .expect("pass")
        .row_work
}

/// #792 review (Fable 6): the row-work pass charges the weight image one sum
/// a placement ([`WEIGHT_SUM_WORK`]) and a kernel stamp a group it grids
/// (#787). The test kernels' weight cell is the spheroidal's own, and their
/// weight groups are the two signs of w, so each chunk makes at most two
/// stamps.
#[test]
fn the_row_work_pass_charges_a_sum_a_placement_and_a_stamp_a_group() {
    let operator = test_kernels_operator(true, None);
    let resampler = SpectralResampler::direct(Basis::Constant).expect("direct");
    let regions = [PassDomain {
        operator: &operator,
        resampler: &resampler,
        partition: Partition::regions(&operator, 3),
    }];
    let mut rows = Rows::random(500, 31, operator.geometry());
    let weighting = WeightingGeneration::Natural { taper: None };
    // The spheroidal's 7 × 7 taps beside a placement's work.
    let stamp = 49 + PLACEMENT_WORK;
    for workers in [1, 3] {
        let team = WorkerTeam::new(workers).expect("team");
        let measured = run_row_work_pass(&regions, &weighting, &mut rows, &team, &Cancel::new())
            .expect("row-work pass");
        let [modes] = measured.modes.as_slice() else {
            panic!("one domain's mode work");
        };
        assert_eq!(modes.data % stamp, 0, "{workers} workers: whole placements");
        let placements = modes.data / stamp;
        let stamps = modes.weight - placements * WEIGHT_SUM_WORK;
        assert_eq!(stamps % stamp, 0, "{workers} workers: whole stamps");
        let chunks = 4 * workers as u64;
        assert!(
            (1..=2 * chunks).contains(&(stamps / stamp)),
            "{workers} workers: {} stamps over {placements} placements",
            stamps / stamp
        );
    }
}

/// The row-work pass measures, from flags and weights alone, the row work a
/// first pass over the same strips reports, on one and three workers: under
/// natural weighting; under Briggs weighting with the first pass keeping
/// its weights, as a run's first pass does; and with W-projection kernels,
/// whose taps vary with w (#742 review, Astra and Fable). Its mode work
/// agrees with the histogram. Only domains split among several region
/// owners ask for it.
#[test]
fn the_row_work_pass_measures_what_a_first_pass_reports() {
    let spheroidal = operator(GridPrecision::F64, Basis::Constant);
    let resampler = SpectralResampler::direct(Basis::Constant).expect("direct");
    let polarization = PolarizationRouting::compile(
        &[Correlation::XX, Correlation::YY],
        &[PolarizationCoordinate::StokesI],
    )
    .expect("routing");
    // A field of about 7° (2 mrad cells), wide enough for the w-term to
    // widen the kernels over the rows' w.
    let wide = GridGeometry::new(
        ImageExtent {
            shape: [IMAGE, IMAGE],
            increment_rad: [-2.0e-3, 2.0e-3],
            reference_pixel: [IMAGE / 2, IMAGE / 2],
        },
        GridPadding::CasaComposite(PaddingFactor::TCLEAN),
    )
    .expect("geometry");
    let kernels = WPlanes::new(&wide, &polarization, WPlaneCount::Fixed(16)).expect("planes");
    let wproject = MeasurementOperator::new(
        wide,
        Basis::Constant,
        polarization,
        Box::new(kernels),
        GridPrecision::F64,
    );
    let mut wide_rows = Rows::random(500, 37, wproject.geometry());
    let half_support = |w_lambda: f64| {
        let cf = wproject.cf();
        let key = cf.key(&crate::fixture::context(0), FIRST_HZ, w_lambda);
        cf.taps(key, &mut CellHold::new()).half_support()
    };
    let widest_w = wide_rows
        .rows
        .iter()
        .map(|row| (row.projections[0].uvw_m[2] * FIRST_HZ / 299_792_458.0).abs())
        .fold(0.0, f64::max);
    assert_ne!(
        half_support(0.0),
        half_support(widest_w),
        "W-projection supports vary over the rows' w"
    );
    let mut rows = Rows::random(500, 31, spheroidal.geometry());
    let shape = DensityGridShape {
        width: IMAGE,
        height: IMAGE,
        planes: 1,
        padding: 0,
        increment_rad: INCREMENT_RAD,
        rule: DensityCellRule::Standard,
    };
    let density = run_density_pass(
        &spheroidal,
        &resampler,
        shape,
        &mut rows,
        &WorkerTeam::new(1).expect("team"),
        &Cancel::new(),
    )
    .expect("density pass");
    let briggs = WeightingGeneration::density(density, Some(0.5), None, None).expect("Briggs");
    let natural = WeightingGeneration::Natural { taper: None };
    let cache = WeightCache::new(rows.rows.len() * CHANNELS);
    for (label, operator, weighting, weights, wide_field) in [
        ("natural", &spheroidal, &natural, None, false),
        (
            "Briggs, kept weights",
            &spheroidal,
            &briggs,
            Some(&cache),
            false,
        ),
        ("W-projection", &wproject, &natural, None, true),
    ] {
        let rows = if wide_field {
            &mut wide_rows
        } else {
            &mut rows
        };
        let regions = [PassDomain {
            operator,
            resampler: &resampler,
            partition: Partition::regions(operator, 3),
        }];
        let reported = first_pass_row_work(&regions, rows, weighting, weights);
        assert!(reported.iter().all(Option::is_some), "{label}");
        rows.contents.clear();
        for workers in [1, 3] {
            let team = WorkerTeam::new(workers).expect("team");
            let measured = run_row_work_pass(&regions, weighting, rows, &team, &Cancel::new())
                .expect("row-work pass");
            assert_eq!(measured.rows, reported, "{label}, {workers} workers");
            // The mode work counts the same placements (#783 review,
            // Fable): the data mode's is the histogram's total, and these
            // sets grid the PSF with their imaging taps and have no weight
            // image.
            let [modes] = measured.modes.as_slice() else {
                panic!("{label}: one domain's mode work");
            };
            let total = measured.rows[0]
                .as_deref()
                .map_or(0, |rows| rows.iter().sum());
            assert_eq!(modes.data, total, "{label}, {workers} workers");
            assert_eq!(modes.psf, modes.data, "{label}, {workers} workers");
            assert_eq!(modes.weight, 0, "{label}, {workers} workers");
        }
        assert_eq!(rows.contents, [SampleContent::FlagsAndWeights; 2]);
        assert!(needs_row_work(&regions));
    }
    assert!(cache.filled(), "the Briggs pass kept its weights");

    let cube = crate::fixture::operator(GridPrecision::F64, Basis::ChannelLocal { planes: 3 });
    let cube_resampler = nearest_cube();
    assert!(!needs_row_work(&[planes(&cube, &cube_resampler, 2)]));
    assert!(!needs_row_work(&[PassDomain {
        operator: &spheroidal,
        resampler: &resampler,
        partition: Partition::regions(&spheroidal, 1),
    }]));
}

/// Strips cut from extreme row work grid what one owner grids (#742
/// review, Fable): work on one row (the centre, the first, the last) or on
/// two; no work, an empty histogram and one of the wrong length, which fall
/// back to equal strips; a strip per row, each narrower than the 7 × 7
/// kernel; and more owners than rows.
#[test]
fn strips_cut_from_extreme_row_work_grid_what_one_owner_grids() {
    let operator = operator(GridPrecision::F64, Basis::Constant);
    let resampler = SpectralResampler::direct(Basis::Constant).expect("direct");
    let mut rows = Rows::random(500, 13, operator.geometry());
    let reference = run(
        &[planes(&operator, &resampler, 1)],
        &mut rows,
        &Run::initial(),
    );
    let ny = operator.geometry().grid_shape()[1];
    let on_rows = |placed: &[(usize, u64)]| {
        let mut work = vec![0; ny];
        for &(row, taps) in placed {
            work[row] = taps;
        }
        work
    };
    for (owners, workers, work) in [
        (3, 3, on_rows(&[(ny / 2, 1)])),
        (4, 4, on_rows(&[(ny - 1, 1)])),
        (3, 3, on_rows(&[(0, 1)])),
        (5, 4, on_rows(&[(ny / 3, 5), (2 * ny / 3, 7)])),
        (3, 3, vec![0; ny]),
        (3, 3, Vec::new()),
        (3, 3, vec![1; ny - 1]),
        (ny, 4, vec![1; ny]),
        (ny + 10, 4, on_rows(&[(ny / 2, 1)])),
    ] {
        let partition = Partition::regions_balanced(&operator, owners, &work);
        let images = run(
            &[PassDomain {
                operator: &operator,
                resampler: &resampler,
                partition,
            }],
            &mut rows,
            &Run {
                workers,
                ..Run::initial()
            },
        );
        let worst = worst_relative(&reference[0], &images[0]);
        assert!(
            worst <= 1.0e-6,
            "{owners} owners over {} rows of work: {worst}",
            work.len()
        );
    }
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
        chunk_sets: 1,
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
        chunk_sets: 1,
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
        chunk_sets: 1,
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

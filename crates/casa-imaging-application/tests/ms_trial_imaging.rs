// SPDX-License-Identifier: LGPL-3.0-or-later
//! MSv2/MSv4 trial, example A (`docs/measurement-set/trial/README.md`): a
//! few dirty-image channel planes formed by the production measurement
//! operator and major-cycle pass from both stores through the shared
//! [`casa_observation::VisibilityBatch`], compared with the production
//! imager on the MSv2.
//!
//! `cargo test -p casa-imaging-application --test ms_trial_imaging -- --nocapture`
//! prints the measurements the evidence records.

#[path = "common/imaging.rs"]
mod imaging;
#[path = "ms_trial/source.rs"]
mod source;
#[path = "ms_trial/work_units.rs"]
mod work_units;

use std::fs::File;
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

use casa_images::PagedImage;
use casa_imaging_application::{GridPrecision, ResourcePolicy};
use casa_imaging_model::{CorrelationType, PolarizationCoordinate};
use casa_imaging_operator::{
    Basis, GridGeometry, GridPadding, ImageExtent, MeasurementOperator, ModeSet, NormalImages,
    PaddingFactor, PolarizationRouting, SpectralAxis, SpectralKernel, SpectralResampler,
    Spheroidal, WeightingGeneration,
};
use casa_imaging_runtime::pass::{
    BackendChoice, CHUNK_PLACEMENTS, Cancel, MajorCyclePass, Partition, PassDomain, Residency,
    WorkerTeam, run_major_cycle,
};
use casa_ms::observation_io::{OpenOptions, open as open_store};
use casa_observation::{Correlation, Observation, PartitionId};
use flate2::read::GzDecoder;
use ndarray::{Array2, ArrayD, Axis};
use source::{BatchSource, SourceTrace, per_times, request};
use tar::Archive;

/// Image size and cell: the fixture's natural beam spans about four cells.
const IMSIZE: usize = 64;
const CELL_ARCSEC: f64 = 0.012;
/// The planes: native channels 2 … 5 of the eight.
const CHANNELS: std::ops::Range<usize> = 2..6;

fn fixture(root: &Path, name: &str) -> PathBuf {
    let archive = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../casa-ms/tests/fixtures/ms_trial")
        .join(format!("{name}.tgz"));
    Archive::new(GzDecoder::new(File::open(archive).expect("fixture")))
        .unpack(root)
        .expect("unpack");
    root.join(name)
}

/// One plane's dirty image normalized by its `sumwt` and PSF by its peak,
/// as the production products are, and the `sumwt`.
#[derive(Clone, Debug)]
struct Plane {
    image: Array2<f32>,
    psf: Array2<f32>,
    sumwt: f64,
}

/// The production imager's dirty cube of the MSv2: CASA `tclean` with
/// `niter=0`, natural weighting and a four-channel LSRK cube.
fn production(ms: &Path, root: &Path) -> (Vec<Plane>, Duration) {
    let image_name = root.join("production");
    let request = imaging::request(serde_json::json!({
        "vis": ms,
        "imagename": image_name,
        "imsize": IMSIZE,
        "cell": format!("{CELL_ARCSEC}arcsec"),
        "field": "0",
        "spw": format!("0:{}~{}", CHANNELS.start, CHANNELS.end - 1),
        "channel_start": CHANNELS.start,
        "channel_count": CHANNELS.len(),
        "specmode": "cube",
        "outframe": "LSRK",
        "start": CHANNELS.start.to_string(),
        "width": "1",
        "datacolumn": "DATA",
        "weighting": "natural",
        "niter": 0,
    }));
    let started = Instant::now();
    casa_imaging_application::execute(&request, imaging::context(ResourcePolicy::Balanced))
        .expect("production imaging");
    let elapsed = started.elapsed();
    let read = |suffix: &str| {
        let image =
            PagedImage::<f32>::open(PathBuf::from(format!("{}{suffix}", image_name.display())))
                .expect("open product");
        let shape = image.shape().to_vec();
        let values: ArrayD<f32> = image
            .get_slice(&vec![0; shape.len()], &shape)
            .expect("read");
        values
    };
    let (image, psf, sumwt) = (read(".image"), read(".psf"), read(".sumwt"));
    // Products are [x, y, stokes, channel]; planes here are [y][x].
    let plane = |cube: &ArrayD<f32>, channel: usize| {
        cube.index_axis(Axis(3), channel)
            .index_axis(Axis(2), 0)
            .t()
            .to_owned()
            .into_dimensionality()
            .expect("2-D")
    };
    let planes = (0..CHANNELS.len())
        .map(|channel| Plane {
            image: plane(&image, channel),
            psf: plane(&psf, channel),
            sumwt: f64::from(
                sumwt
                    .index_axis(Axis(3), channel)
                    .iter()
                    .next()
                    .copied()
                    .expect("sumwt"),
            ),
        })
        .collect();
    (planes, elapsed)
}

/// The production operator for the same image: standard spheroidal kernels
/// on the tclean-padded grid, Stokes I from all four stored correlations
/// (the production imager routes every selected correlation, so a flagged
/// cross hand drops its sample from Stokes I, as CASA's does), one plane per
/// channel.
fn operator(planes: u32) -> MeasurementOperator {
    let cell = (CELL_ARCSEC / 3_600.0_f64).to_radians();
    let geometry = GridGeometry::new(
        ImageExtent {
            shape: [IMSIZE, IMSIZE],
            increment_rad: [-cell, cell],
            reference_pixel: [IMSIZE / 2, IMSIZE / 2],
        },
        GridPadding::CasaComposite(PaddingFactor::TCLEAN),
    )
    .expect("geometry");
    let polarization = PolarizationRouting::compile(
        &[
            CorrelationType::LinearXx,
            CorrelationType::LinearXy,
            CorrelationType::LinearYx,
            CorrelationType::LinearYy,
        ],
        &[PolarizationCoordinate::StokesI],
    )
    .expect("routing");
    let kernels = Box::new(Spheroidal::new(&geometry, &polarization));
    MeasurementOperator::new(
        geometry,
        Basis::ChannelLocal { planes },
        polarization,
        kernels,
        GridPrecision::F64,
    )
}

/// The output axis: the native channels' centres and width.
fn axis(observation: &dyn Observation) -> SpectralAxis {
    let partition = &observation.catalogue().partitions[0];
    SpectralAxis::new(
        partition.frequencies_hz[CHANNELS.start],
        partition.channel_widths_hz[CHANNELS.start],
        CHANNELS.len() as u32,
    )
    .expect("axis")
}

/// What one trial pass measured.
struct TrialRun {
    planes: Vec<Plane>,
    trace: SourceTrace,
    pass: Duration,
    waves: usize,
}

/// The production pass over `observation` through the batch source, with
/// `times` whole times per batch.
fn trial(
    observation: &dyn Observation,
    residency: Residency,
    times: usize,
    workers: usize,
) -> TrialRun {
    let operator = operator(CHANNELS.len() as u32);
    let resampler = SpectralResampler::channel_local(axis(observation), SpectralKernel::Linear);
    let domains = [PassDomain {
        operator: &operator,
        resampler: &resampler,
        partition: Partition::Planes { owners: workers },
    }];
    let weighting = WeightingGeneration::Natural { taper: None };
    let pass = MajorCyclePass {
        domains: &domains,
        weighting: &weighting,
        modes: ModeSet::DATA_PSF,
        model: None,
        residency,
        native_spacing_hz: 128.0e6,
        backend: BackendChoice::Cpu,
        metal_stage_timing: false,
        weights: None,
        chunk_placements: CHUNK_PLACEMENTS,
        chunk_sets: 1,
    };
    let batch_bytes = per_times(
        observation,
        request(PartitionId(0), CHANNELS, true, 0),
        times,
    )
    .batch_bytes;
    let mut source = BatchSource::new(
        observation,
        CHANNELS,
        axis(observation),
        &[
            Correlation::XX,
            Correlation::XY,
            Correlation::YX,
            Correlation::YY,
        ],
        batch_bytes,
    );
    let mut waves: Vec<NormalImages> = Vec::new();
    let started = Instant::now();
    run_major_cycle(
        &pass,
        &mut source,
        &WorkerTeam::new(workers).expect("team"),
        &Cancel::new(),
        &mut |_, images| {
            waves.push(images);
            Ok(())
        },
        None,
    )
    .expect("pass");
    let pass_time = started.elapsed();
    let mut planes = Vec::new();
    for images in &waves {
        for plane in 0..images.planes.len() {
            let sumwt = images.data_sumwt(plane, 0, 0);
            let psf = images.psf(plane, 0, 0);
            let peak = psf.iter().fold(f32::MIN, |peak, v| peak.max(*v));
            planes.push(Plane {
                image: images
                    .data(plane, 0, 0)
                    .mapv(|v| (f64::from(v) / sumwt) as f32),
                psf: psf.mapv(|v| v / peak),
                sumwt,
            });
        }
    }
    TrialRun {
        planes,
        trace: source.trace,
        pass: pass_time,
        waves: waves.len(),
    }
}

/// RMS of `a - b` over the RMS of `a`. A NaN or infinite pixel in either
/// plane, or a non-zero difference from an all-zero reference, gives
/// infinity: a comparison can never read a missing or poisoned value as
/// agreement.
///
/// # Panics
///
/// If the planes' shapes differ.
fn nrms(a: &Array2<f32>, b: &Array2<f32>) -> f64 {
    assert_eq!(a.shape(), b.shape(), "plane shapes");
    if a.iter().chain(b).any(|value| !value.is_finite()) {
        return f64::INFINITY;
    }
    let (mut error, mut scale) = (0.0, 0.0);
    for (x, y) in a.iter().zip(b) {
        error += (f64::from(*x) - f64::from(*y)).powi(2);
        scale += f64::from(*x).powi(2);
    }
    if scale == 0.0 {
        return if error == 0.0 { 0.0 } else { f64::INFINITY };
    }
    (error / scale).sqrt()
}

/// `|a - b| / |a|`, infinite unless both are finite and comparable.
fn relative(a: f64, b: f64) -> f64 {
    if !(a.is_finite() && b.is_finite()) {
        f64::INFINITY
    } else if a == b {
        0.0
    } else if a == 0.0 {
        f64::INFINITY
    } else {
        ((a - b) / a).abs()
    }
}

/// The larger error, a NaN counting as the largest: `f64::max` would keep
/// the other operand and let a NaN through as zero.
fn worse(a: f64, b: f64) -> f64 {
    if a.is_nan() || b.is_nan() {
        f64::INFINITY
    } else {
        a.max(b)
    }
}

/// The worst image NRMS, PSF NRMS and relative `sumwt` over planes.
///
/// # Panics
///
/// If the plane counts or any plane's shape differ.
fn worst(a: &[Plane], b: &[Plane]) -> (f64, f64, f64) {
    assert_eq!(a.len(), b.len(), "plane counts");
    a.iter()
        .zip(b)
        .fold((0.0_f64, 0.0_f64, 0.0_f64), |(i, p, s), (a, b)| {
            (
                worse(i, nrms(&a.image, &b.image)),
                worse(p, nrms(&a.psf, &b.psf)),
                worse(s, relative(a.sumwt, b.sumwt)),
            )
        })
}

/// Open both fixtures through the one store-choosing entry point; nothing
/// past here knows which format each is.
fn open(root: &Path) -> (Box<dyn Observation>, Box<dyn Observation>) {
    let options = OpenOptions::default();
    (
        open_store(fixture(root, "ms_trial.ms"), &options).expect("v2"),
        open_store(fixture(root, "ms_trial.ps.zarr"), &options).expect("v4"),
    )
}

/// The comparators fail rather than pass on what they cannot compare: one
/// NaN pixel, a NaN `sumwt`, a mismatched plane shape or plane count.
#[test]
fn comparators_never_pass_a_nan_or_a_mismatch() {
    let plane = |value: f32| Plane {
        image: Array2::from_elem((4, 4), value),
        psf: Array2::from_elem((4, 4), 1.0),
        sumwt: 2.0,
    };
    let reference = vec![plane(1.0), plane(2.0)];
    assert_eq!(worst(&reference, &reference), (0.0, 0.0, 0.0));
    let mut poisoned = reference.clone();
    poisoned[1].image[[2, 3]] = f32::NAN;
    assert_eq!(worst(&reference, &poisoned).0, f64::INFINITY, "a NaN pixel");
    let mut poisoned = reference.clone();
    poisoned[0].sumwt = f64::NAN;
    assert_eq!(worst(&reference, &poisoned).2, f64::INFINITY, "a NaN sumwt");
    let mut misshapen = reference.clone();
    misshapen[0].image = Array2::zeros((4, 5));
    assert!(std::panic::catch_unwind(|| worst(&reference, &misshapen)).is_err());
    assert!(std::panic::catch_unwind(|| worst(&reference, &reference[..1])).is_err());
}

/// The reusable consumer modules see only the shared interface: they name
/// no adapter, cursor or crate of either store.
#[test]
fn consumer_modules_import_no_store_type() {
    for (module, text) in [
        ("source.rs", include_str!("ms_trial/source.rs")),
        ("work_units.rs", include_str!("ms_trial/work_units.rs")),
    ] {
        for store in ["casa_ms", "Msv2", "Msv4"] {
            assert!(!text.contains(store), "{module} names {store}");
        }
    }
}

/// The dirty planes from both stores through the shared batch equal the
/// production imager's on the MSv2, image, PSF and `sumwt`, with every
/// plane resident and with one-plane waves that each bind their own
/// channel window. The v2 and v4 batches carry the same values in the same
/// order (both are the partition's grid), so their images are identical.
#[test]
fn dirty_planes_from_both_stores_equal_the_production_imager() {
    let root = tempfile::tempdir().expect("root");
    let (production, production_time) =
        production(&fixture(root.path(), "ms_trial.ms"), root.path());
    let (v2, v4) = open(root.path());
    println!("production imager (end to end): {production_time:?}");
    for (label, residency) in [
        ("resident", Residency::All),
        ("one-plane waves", Residency::Waves { planes_per_wave: 1 }),
    ] {
        let mut runs = Vec::new();
        for (store, observation) in [("v2", &*v2), ("v4", &*v4)] {
            let run = trial(observation, residency, 4, 1);
            let (image, psf, sumwt) = worst(&production, &run.planes);
            println!(
                "{store} {label}: waves {}, image NRMS {image:.2e}, PSF NRMS {psf:.2e}, sumwt {sumwt:.2e}; \
                 pass {:?} (read {:?}, pack {:?}); traversals {:?}; {:?}; packed {} elements; buffers {} B",
                run.waves,
                run.pass,
                run.trace.read,
                run.trace.pack,
                run.trace.traversals,
                run.trace.io,
                run.trace.packed_elements,
                run.trace.buffer_bytes,
            );
            assert!(
                image < 1e-5 && psf < 1e-5 && sumwt < 1e-6,
                "{store} {label}"
            );
            runs.push(run);
        }
        let (image, psf, sumwt) = worst(&runs[0].planes, &runs[1].planes);
        assert_eq!(
            (image, psf, sumwt),
            (0.0, 0.0, 0.0),
            "{label}: v2 and v4 identical"
        );
    }
}

/// The source's batch size changes neither the planes nor what the
/// adapters decode beyond their batch boundaries.
#[test]
fn dirty_planes_do_not_depend_on_batch_size() {
    let root = tempfile::tempdir().expect("root");
    let (v2, v4) = open(root.path());
    let reference = trial(&*v2, Residency::All, 12, 1).planes;
    for (store, observation) in [("v2", &*v2), ("v4", &*v4)] {
        for times in [1, 3, 7] {
            let run = trial(observation, Residency::All, times, 2);
            let (image, psf, sumwt) = worst(&reference, &run.planes);
            println!(
                "{store}, {times} times per batch, 2 workers: image NRMS {image:.2e}, PSF {psf:.2e}, \
                 sumwt {sumwt:.2e}; {} batches, {} decode units",
                run.trace.io.batches, run.trace.io.decode_units
            );
            assert!(
                image < 1e-6 && psf < 1e-6 && sumwt < 1e-9,
                "{store} {times}"
            );
        }
    }
}

/// Example A's work units: one channel window per unit, one unit held back.
#[test]
fn channel_work_units_retire_independently() {
    let root = tempfile::tempdir().expect("root");
    let (v2, v4) = open(root.path());
    let reference = trial(&*v2, Residency::All, 12, 1).planes;
    for (store, observation) in [("v2", &*v2), ("v4", &*v4)] {
        for strategy in [
            work_units::Strategy::SharedBatch,
            work_units::Strategy::PackedPieces,
        ] {
            let report = work_units::run(
                observation,
                strategy,
                &operator(1),
                &axis(observation),
                CHANNELS,
            );
            println!("{store} {strategy:?}: {report}");
            assert_eq!(report.planes.len(), reference.len());
            for (plane, (image, sumwt)) in report.planes.iter().enumerate() {
                let reference = &reference[plane];
                assert!(
                    nrms(&reference.image, image) < 1e-6,
                    "{store} {strategy:?} plane {plane}"
                );
                assert!(relative(reference.sumwt, *sumwt) < 1e-9);
            }
            report.check(strategy);
        }
    }
}

/// Median and minimum of `samples`.
fn spread(mut samples: Vec<Duration>) -> (Duration, Duration) {
    samples.sort();
    (samples[samples.len() / 2], samples[0])
}

/// Narrow matched timings for the evidence, in a release build:
/// `cargo test --release -p casa-imaging-application --test ms_trial_imaging -- --ignored --nocapture`.
/// The production imager's dirty-pass phase (its MSv2 source and the same
/// pass) against the same pass reading v2 and v4 batches, 25 runs each.
/// Diagnostics on a tiny fixture, not performance acceptance.
#[test]
#[ignore = "timing diagnostics; run in release"]
fn narrow_timings() {
    const RUNS: usize = 25;
    let root = tempfile::tempdir().expect("root");
    let ms = fixture(root.path(), "ms_trial.ms");
    let mut production_phase = Vec::new();
    for run in 0..RUNS {
        let image_name = root.path().join(format!("timing-{run}"));
        let summary = root.path().join(format!("timing-{run}.json"));
        let request = imaging::request(serde_json::json!({
            "vis": ms,
            "imagename": image_name,
            "imsize": IMSIZE,
            "cell": format!("{CELL_ARCSEC}arcsec"),
            "field": "0",
            "spw": format!("0:{}~{}", CHANNELS.start, CHANNELS.end - 1),
            "channel_start": CHANNELS.start,
            "channel_count": CHANNELS.len(),
            "specmode": "cube",
            "outframe": "LSRK",
            "start": CHANNELS.start.to_string(),
            "width": "1",
            "datacolumn": "DATA",
            "weighting": "natural",
            "niter": 0,
        }));
        let mut context = imaging::context(ResourcePolicy::Explicit {
            workers: 1,
            memory: 2 << 30,
        });
        context.summary = Some(casa_imaging_application::SummaryTarget {
            path: summary.clone(),
            request: serde_json::Value::Null,
        });
        casa_imaging_application::execute(&request, context).expect("production");
        let summary: serde_json::Value =
            serde_json::from_reader(File::open(&summary).expect("summary")).expect("json");
        let phases = summary["phases"].as_array().expect("phases");
        if run == 0 {
            let names: Vec<_> = phases
                .iter()
                .map(|phase| {
                    format!(
                        "{} {:.2} ms",
                        phase["name"],
                        phase["seconds"].as_f64().unwrap_or(0.0) * 1e3
                    )
                })
                .collect();
            println!("production phases: {names:?}");
        }
        let pass = phases
            .iter()
            .find(|phase| {
                phase["name"]
                    .as_str()
                    .is_some_and(|name| name.contains("pass") || name.contains("major"))
            })
            .expect("a pass phase");
        production_phase.push(Duration::from_secs_f64(
            pass["seconds"].as_f64().expect("seconds"),
        ));
    }
    let (median, min) = spread(production_phase);
    println!("production dirty pass phase: median {median:?}, min {min:?}");
    let (v2, v4) = open(root.path());
    for (store, observation) in [("v2", &*v2), ("v4", &*v4)] {
        let runs: Vec<TrialRun> = (0..RUNS)
            .map(|_| trial(observation, Residency::All, 12, 1))
            .collect();
        let (pass, pass_min) = spread(runs.iter().map(|run| run.pass).collect());
        let (read, read_min) = spread(runs.iter().map(|run| run.trace.read).collect());
        let (pack, pack_min) = spread(runs.iter().map(|run| run.trace.pack).collect());
        println!(
            "{store} batch pass: median {pass:?} (min {pass_min:?}); adapter read median {read:?} \
             (min {read_min:?}); pack median {pack:?} (min {pack_min:?})"
        );
    }
}

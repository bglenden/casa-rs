// SPDX-License-Identifier: LGPL-3.0-or-later
//! Count the whole total-power image/weight, streaming scratch and worker team.
use super::*;
use casa_coordinates::{DirectionCoordinate, Projection, ProjectionType};
use casa_imaging_application::single_dish::{TotalPowerRequest, grid_observation};
use casa_imaging_singledish::GridFunction;
use casa_types::measures::direction::DirectionRef;

#[test]
fn grid_weight_and_streaming_are_admitted_and_retained() {
    if !alone("single_dish::grid_weight_and_streaming_are_admitted_and_retained") {
        return;
    }
    let root = tempfile::tempdir().unwrap();
    let data =
        Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/data/tsdimaging_task_inputs.tar.gz");
    tar::Archive::new(flate2::read::GzDecoder::new(
        std::fs::File::open(data).unwrap(),
    ))
    .unpack(root.path())
    .unwrap();
    let observation =
        casa_ms::observation_io::open(root.path().join("map.ms"), &Default::default()).unwrap();
    let mut request = TotalPowerRequest {
        direction: DirectionCoordinate::new(
            DirectionRef::J2000,
            Projection::new(ProjectionType::SIN),
            [270f64.to_radians(), (-23f64).to_radians()],
            [-3f64.to_radians() / 3600., 3f64.to_radians() / 3600.],
            [64., 64.],
        ),
        size: [128, 128],
        channels: 0..3,
        intent: "OBSERVE_TARGET#ON_SOURCE".into(),
        kernel: GridFunction::Spheroidal { support: 6 },
        rest_frequency_hz: None,
        windows: Vec::new(),
        antennas: Vec::new(),
    };
    let context = RunContext {
        host: HOST,
        ..imaging::context(ResourcePolicy::Explicit {
            workers: 4,
            memory: AMPLE,
        })
    };
    drop(grid_observation(observation.as_ref(), &request, &context).unwrap());
    let mut retained = Vec::new();
    for size in [128, 256] {
        request.size = [size, size];
        FIXED.store(held_by_process(), Ordering::Relaxed);
        PEAK.store(0, Ordering::Relaxed);
        EXCESS.store(i64::MIN, Ordering::Relaxed);
        RESERVED_PEAK.store(0, Ordering::Relaxed);
        STARTED.get_or_init(Instant::now);
        BASELINE.store(LIVE.load(Ordering::Relaxed), Ordering::Relaxed);
        MEASURING.store(true, Ordering::Relaxed);
        let result = grid_observation(observation.as_ref(), &request, &context);
        MEASURING.store(false, Ordering::Relaxed);
        let output = result.unwrap();
        let excess = EXCESS.load(Ordering::Relaxed);
        println!(
            "size={size} heap={} reserved={} excess={excess}",
            PEAK.load(Ordering::Relaxed),
            RESERVED_PEAK.load(Ordering::Relaxed)
        );
        assert!(
            excess <= 0,
            "total-power allocation escaped admission: {excess}"
        );
        retained.push(output.arrays.reservation.memory());
        assert_eq!(output.arrays.value.image.len(), size * size * 3);
        assert_eq!(output.arrays.value.weight.len(), size * size * 3);
    }
    assert_eq!(retained[1] - retained[0], 8 * (256 * 256 - 128 * 128) * 3);
    let refused = RunContext {
        policy: ResourcePolicy::Explicit {
            workers: 4,
            memory: 1,
        },
        ..context
    };
    assert!(grid_observation(observation.as_ref(), &request, &refused).is_err());
}

#[test]
fn task_opening_masks_beam_and_publication_are_admitted() {
    if !alone("single_dish::task_opening_masks_beam_and_publication_are_admitted") {
        return;
    }
    let root = tempfile::tempdir().unwrap();
    let data =
        Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/data/tsdimaging_task_inputs.tar.gz");
    tar::Archive::new(flate2::read::GzDecoder::new(
        std::fs::File::open(data).unwrap(),
    ))
    .unpack(root.path())
    .unwrap();
    let bundle = casa_provider_contracts::builtin_surface_bundle("tsdimaging").unwrap();
    let sparse = json!({"infiles":root.path().join("map.ms"), "outfile":root.path().join("output"), "overwrite":true, "imsize":[128,128], "cell":"3arcsec", "phasecenter":"J2000 270deg -23deg", "gridfunction":"SF", "convsupport":6});
    let values = casa_task_runtime::resolve_plain_values(
        bundle,
        root.path().to_owned(),
        sparse.as_object().unwrap(),
    )
    .unwrap();
    let context = RunContext {
        host: HOST,
        ..imaging::context(ResourcePolicy::Explicit {
            workers: 4,
            memory: AMPLE,
        })
    };
    drop(casa_imaging_application::single_dish::image_from_parameters(&values, &context).unwrap());
    FIXED.store(held_by_process(), Ordering::Relaxed);
    PEAK.store(0, Ordering::Relaxed);
    EXCESS.store(i64::MIN, Ordering::Relaxed);
    RESERVED_PEAK.store(0, Ordering::Relaxed);
    STARTED.get_or_init(Instant::now);
    BASELINE.store(LIVE.load(Ordering::Relaxed), Ordering::Relaxed);
    MEASURING.store(true, Ordering::Relaxed);
    let result = casa_imaging_application::single_dish::image_from_parameters(&values, &context);
    MEASURING.store(false, Ordering::Relaxed);
    let output = result.unwrap();
    let excess = EXCESS.load(Ordering::Relaxed);
    println!(
        "whole task heap={} reserved={} excess={excess}",
        PEAK.load(Ordering::Relaxed),
        RESERVED_PEAK.load(Ordering::Relaxed)
    );
    assert!(excess <= 0, "task allocation escaped admission: {excess}");
    assert!(output.image.is_dir() && output.weight.is_dir());
}

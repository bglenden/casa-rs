// SPDX-License-Identifier: LGPL-3.0-or-later
//! The task adapter reads actual CASA MS data through casa-observation.
use base64::{Engine, engine::general_purpose::STANDARD};
use casa_coordinates::{DirectionCoordinate, Projection, ProjectionType};
use casa_core::measures::direction::DirectionRef;
use casa_imaging_application::{
    Cancel, RunContext,
    single_dish::{TotalPowerProducts, TotalPowerRequest, image_observation},
};
use casa_imaging_runtime::{HostResources, ResourcePolicy};
use casa_imaging_singledish::{GridFunction, KernelParameters};
use flate2::read::GzDecoder;
use serde_json::Value;
use std::path::Path;

const ARCSEC: f64 = std::f64::consts::PI / (180. * 3600.);

fn pixels(value: &Value) -> Vec<f32> {
    STANDARD
        .decode(value.as_str().unwrap())
        .unwrap()
        .as_chunks::<4>()
        .0
        .iter()
        .map(|v| f32::from_le_bytes(*v))
        .collect()
}
fn compare(actual: &[f32], expected: &Value) {
    let expected = pixels(expected);
    assert_eq!(actual.len(), expected.len());
    let mut error = 0.;
    let mut power = 0.;
    for (&a, e) in actual.iter().zip(expected) {
        assert!(a.is_finite() && e.is_finite());
        error += (f64::from(a) - f64::from(e)).powi(2);
        power += f64::from(e).powi(2);
    }
    let nrms = (error / power).sqrt();
    println!("NRMS {nrms:e}");
    assert!(nrms <= 1e-3);
}
fn kernel(case: &Value) -> GridFunction {
    let p = &case["params"];
    let parameter = |name: &str| {
        p[name].as_f64().unwrap_or_else(|| {
            let text = p[name].as_str().unwrap_or("-1");
            text.strip_suffix("arcsec")
                .map(|v| v.parse::<f64>().unwrap() / 3.)
                .unwrap_or(-1.)
        })
    };
    GridFunction::from_casa(
        p["gridfunction"].as_str().unwrap(),
        KernelParameters {
            convsupport: p["convsupport"].as_i64().unwrap_or(-1),
            truncate: parameter("truncate"),
            gwidth: parameter("gwidth"),
            jwidth: parameter("jwidth"),
        },
    )
    .unwrap()
}
#[test]
fn grids_actual_casa_total_power_measurement_sets() {
    let data = Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/data");
    let oracle: Value = serde_json::from_reader(GzDecoder::new(
        std::fs::File::open(data.join("tsdimaging_task_casa.json.gz")).unwrap(),
    ))
    .unwrap();
    let scratch = tempfile::tempdir().unwrap();
    tar::Archive::new(GzDecoder::new(
        std::fs::File::open(data.join("tsdimaging_task_inputs.tar.gz")).unwrap(),
    ))
    .unpack(scratch.path())
    .unwrap();
    for case in oracle["cases"].as_array().unwrap() {
        let name = case["name"].as_str().unwrap();
        println!("CASE {name}");
        let ms = scratch
            .path()
            .join(format!("{}.ms", case["ms"].as_str().unwrap()));
        let observation = casa_ms::observation_io::open(ms, &Default::default()).unwrap();
        let request = TotalPowerRequest {
            direction: DirectionCoordinate::new(
                DirectionRef::J2000,
                Projection::new(ProjectionType::SIN),
                [270f64.to_radians(), (-23f64).to_radians()],
                [-3. * ARCSEC, 3. * ARCSEC],
                [20., 20.],
            ),
            size: [40, 40],
            channels: 0..3,
            intent: "OBSERVE_TARGET#ON_SOURCE".into(),
            kernel: kernel(case),
            rest_frequency_hz: None,
            windows: Vec::new(),
            antennas: Vec::new(),
        };
        let result = image_observation(
            observation.as_ref(),
            &request,
            &TotalPowerProducts {
                outfile: scratch.path().join(name),
                overwrite: false,
                minweight: 0.1,
                brightness_unit: "Jy/beam".into(),
            },
            &RunContext {
                host: HostResources {
                    threads: 4,
                    performance_cores: 4,
                    available_memory: 2 << 30,
                    metal: false,
                },
                policy: ResourcePolicy::Explicit {
                    workers: 1,
                    memory: 1 << 30,
                },
                cancel: Cancel::new(),
                summary: None,
            },
        )
        .unwrap();
        let expected = if name == "sf_gapped" {
            oracle["cases"]
                .as_array()
                .unwrap()
                .iter()
                .find(|c| c["name"] == "sf6")
                .unwrap()
        } else {
            case
        };
        let image = casa_images::PagedImage::<f32>::open(&result.image).unwrap();
        let weight = casa_images::PagedImage::<f32>::open(&result.weight).unwrap();
        compare(
            image.get().unwrap().as_slice_memory_order().unwrap(),
            &expected["image"]["pixels"],
        );
        compare(
            weight.get().unwrap().as_slice_memory_order().unwrap(),
            &expected["weight"]["pixels"],
        );
        let mask = image.get_mask().unwrap().unwrap();
        if name != "sf_small_weights" {
            let expected_mask = STANDARD
                .decode(expected["image"]["mask"].as_str().unwrap())
                .unwrap();
            assert_eq!(
                mask.as_slice_memory_order()
                    .unwrap()
                    .iter()
                    .map(|v| u8::from(*v))
                    .collect::<Vec<_>>(),
                expected_mask,
                "{name} mask"
            );
        } else {
            // CASA formats this tiny threshold with six decimals and masks at
            // zero. The existing SD-core deviation keeps the true threshold.
            let weights = pixels(&expected["weight"]["pixels"]);
            let mut positive: Vec<_> = weights.iter().copied().filter(|v| *v > 0.).collect();
            positive.sort_by(f32::total_cmp);
            let middle = positive.len() / 2;
            let median = if positive.len() % 2 == 0 {
                (f64::from(positive[middle - 1]) + f64::from(positive[middle])) / 2.
            } else {
                f64::from(positive[middle])
            };
            assert!(
                mask.as_slice_memory_order()
                    .unwrap()
                    .iter()
                    .zip(weights)
                    .all(|(&valid, weight)| valid == (f64::from(weight) > median * 0.1))
            );
        }
        assert_eq!(image.units(), "Jy/beam");
        assert_eq!(weight.units(), "");
        let raw = case["image"]["beam_arcsec"].as_f64().unwrap();
        let beam = result.beam.unwrap().major / ARCSEC;
        let relative = beam / raw - 1.;
        let allowed = match request.kernel {
            GridFunction::Box { .. } => -1e-7..=1e-7,
            GridFunction::Spheroidal { .. } => 0.0..=1e-3,
            GridFunction::Gauss { .. } => 5e-3..=4e-2,
            GridFunction::Gjinc { .. } => -4e-3..=3e-4,
            GridFunction::PrimaryBeam => -1e-3..=0.0,
        };
        println!("beam {beam} vs CASA {raw}, relative {relative:e}");
        assert!(
            allowed.contains(&relative),
            "{name}: beam departure outside the existing core's approved bounds"
        );
    }
}

#[test]
fn converted_msv4_keeps_total_power_pixels_weights_and_pointings() {
    use casa_ms::observation_io::convert::{ConversionOptions, LossKind, PaddingRows, to_msv4};
    let root = tempfile::tempdir().unwrap();
    let data = Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/data");
    tar::Archive::new(GzDecoder::new(
        std::fs::File::open(data.join("tsdimaging_task_inputs.tar.gz")).unwrap(),
    ))
    .unpack(root.path())
    .unwrap();
    let source = root.path().join("map.ms");
    let destination = root.path().join("map.ps.zarr");
    to_msv4(
        &source,
        &destination,
        &ConversionOptions {
            data_groups: None,
            accept: vec![
                LossKind::Sigma,
                LossKind::Tables,
                LossKind::Columns,
                LossKind::Defaulted,
                LossKind::RowFlags,
            ],
            batch_bytes: 1 << 20,
            workers: 1,
            chunks: [32, 0, 0, 0],
            codec: casa_ms::observation_io::Msv4Codec::BloscLz4,
            padding: PaddingRows::Flagged,
            creator: "casa-rs".into(),
            creation_date: "2026-11-15T00:00:00+00:00".into(),
        },
    )
    .unwrap();
    let request = TotalPowerRequest {
        direction: DirectionCoordinate::new(
            DirectionRef::J2000,
            Projection::new(ProjectionType::SIN),
            [270f64.to_radians(), (-23f64).to_radians()],
            [-3. * ARCSEC, 3. * ARCSEC],
            [20., 20.],
        ),
        size: [40, 40],
        channels: 0..3,
        intent: "OBSERVE_TARGET#ON_SOURCE".into(),
        kernel: GridFunction::Spheroidal { support: 6 },
        rest_frequency_hz: None,
        windows: Vec::new(),
        antennas: Vec::new(),
    };
    let context = RunContext {
        host: HostResources {
            threads: 4,
            performance_cores: 4,
            available_memory: 2 << 30,
            metal: false,
        },
        policy: ResourcePolicy::Explicit {
            workers: 1,
            memory: 1 << 30,
        },
        cancel: Cancel::new(),
        summary: None,
    };
    let mut outcomes = Vec::new();
    for path in [source, destination] {
        let observation = casa_ms::observation_io::open(path, &Default::default()).unwrap();
        outcomes.push(
            casa_imaging_application::single_dish::grid_observation(
                observation.as_ref(),
                &request,
                &context,
            )
            .unwrap(),
        );
    }
    assert_eq!(outcomes[0].arrays.value, outcomes[1].arrays.value);
    assert_eq!(outcomes[0].sampling.len(), outcomes[1].sampling.len());
    for (a, b) in outcomes[0].sampling.iter().zip(&outcomes[1].sampling) {
        assert_eq!(a.direction_rad, b.direction_rad);
        // MSv4 uses its own time origin; raster sampling depends on intervals.
        assert!(
            ((a.time_s - outcomes[0].sampling[0].time_s)
                - (b.time_s - outcomes[1].sampling[0].time_s))
                .abs()
                <= 1e-6
        );
    }
}

#[test]
fn preparation_failure_preserves_both_previous_products() {
    let root = tempfile::tempdir().unwrap();
    let data = Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/data");
    tar::Archive::new(GzDecoder::new(
        std::fs::File::open(data.join("tsdimaging_task_inputs.tar.gz")).unwrap(),
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
            [-3. * ARCSEC, 3. * ARCSEC],
            [20., 20.],
        ),
        size: [40, 40],
        channels: 0..3,
        intent: "OBSERVE_TARGET#ON_SOURCE".into(),
        kernel: GridFunction::BOX,
        rest_frequency_hz: None,
        windows: Vec::new(),
        antennas: Vec::new(),
    };
    let mut products = TotalPowerProducts {
        outfile: root.path().join("image"),
        overwrite: false,
        minweight: 0.1,
        brightness_unit: "Jy/beam".into(),
    };
    let context = RunContext {
        host: HostResources {
            threads: 4,
            performance_cores: 4,
            available_memory: 2 << 30,
            metal: false,
        },
        policy: ResourcePolicy::Explicit {
            workers: 1,
            memory: 1 << 30,
        },
        cancel: Cancel::new(),
        summary: None,
    };
    let result = image_observation(observation.as_ref(), &request, &products, &context).unwrap();
    let before: Vec<_> = [&result.image, &result.weight]
        .map(|path| {
            casa_images::PagedImage::<f32>::open(path)
                .unwrap()
                .get()
                .unwrap()
        })
        .into();
    assert!(image_observation(observation.as_ref(), &request, &products, &context).is_err());
    products.overwrite = true;
    request.size = [usize::MAX, 40];
    assert!(image_observation(observation.as_ref(), &request, &products, &context).is_err());
    for (path, expected) in [&result.image, &result.weight].into_iter().zip(before) {
        assert_eq!(
            casa_images::PagedImage::<f32>::open(path)
                .unwrap()
                .get()
                .unwrap(),
            expected
        );
    }
}

#[test]
fn pointing_frames_coverage_fallback_and_explicit_later_refusals() {
    let data = Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/data");
    let oracle: Value = serde_json::from_reader(GzDecoder::new(
        std::fs::File::open(data.join("tsdimaging_pointing_casa.json.gz")).unwrap(),
    ))
    .unwrap();
    assert_eq!(oracle["casa_version"], "6.7.6.14");
    let scratch = tempfile::tempdir().unwrap();
    tar::Archive::new(GzDecoder::new(
        std::fs::File::open(data.join("tsdimaging_pointing_inputs.tar.gz")).unwrap(),
    ))
    .unpack(scratch.path())
    .unwrap();
    for case in oracle["cases"].as_array().unwrap() {
        let name = case["name"].as_str().unwrap();
        if name == "target_column" {
            continue;
        } // Nondefault column refusal belongs to the task parameter boundary (#938).
        println!("POINTING CASE {name}");
        let observation = casa_ms::observation_io::open(
            scratch.path().join(format!("{name}.ms")),
            &Default::default(),
        )
        .unwrap();
        let request = TotalPowerRequest {
            direction: DirectionCoordinate::new(
                DirectionRef::J2000,
                Projection::new(ProjectionType::SIN),
                [270f64.to_radians(), (-23f64).to_radians()],
                [-3. * ARCSEC, 3. * ARCSEC],
                [20., 20.],
            ),
            size: [40, 40],
            channels: 0..3,
            intent: "OBSERVE_TARGET#ON_SOURCE".into(),
            kernel: GridFunction::Spheroidal { support: 6 },
            rest_frequency_hz: None,
            windows: Vec::new(),
            antennas: Vec::new(),
        };
        let result = image_observation(
            observation.as_ref(),
            &request,
            &TotalPowerProducts {
                outfile: scratch.path().join(name),
                overwrite: false,
                minweight: 0.1,
                brightness_unit: "Jy/beam".into(),
            },
            &RunContext {
                host: HostResources {
                    threads: 4,
                    performance_cores: 4,
                    available_memory: 2 << 30,
                    metal: false,
                },
                policy: ResourcePolicy::Explicit {
                    workers: 1,
                    memory: 1 << 30,
                },
                cancel: Cancel::new(),
                summary: None,
            },
        );
        if name == "between_pointings" {
            assert!(matches!(
                result,
                Err(
                    casa_imaging_application::single_dish::TotalPowerError::Unsupported {
                        behavior: "POINTING time interpolation",
                        ..
                    }
                )
            ));
        } else if name == "flagged_state" {
            assert!(case.get("error").is_some());
            assert!(result.is_err());
        } else {
            if name == "empty_pointing" {
                assert!(case.get("error").is_some() && case.get("control").is_some());
            } else {
                assert!(case.get("error").is_none(), "CASA reference failed: {case}");
            }
            let result = result.unwrap();
            let image = casa_images::PagedImage::<f32>::open(result.image).unwrap();
            let weight = casa_images::PagedImage::<f32>::open(result.weight).unwrap();
            compare(
                image.get().unwrap().as_slice_memory_order().unwrap(),
                &case["image"]["pixels"],
            );
            compare(
                weight.get().unwrap().as_slice_memory_order().unwrap(),
                &case["weight"]["pixels"],
            );
        }
    }
}

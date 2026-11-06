// SPDX-License-Identifier: LGPL-3.0-or-later
//! Both MVC product families consume the same admitted primary beam.

use casa_imaging_deconvolution::{ModelLifecycle, ModelStoragePlan};
use casa_imaging_products::{
    ContinuumProductInputs, PlannedContinuumGeneration, read_mvc_cube_plane,
};
use casa_imaging_runtime::HostResources;
use casa_imaging_runtime::pass::{Cancel, WorkerTeam};
use serde_json::json;

use super::{MvcProductSource, ProductSource, publish_mvc_products};
use crate::{ApplicationNative, RunContext, mvc, request::tests::request};

use crate::mvc_compile_law::{synthetic_observation, synthetic_pass};

#[test]
fn mvc_native_staging_uses_the_admitted_beam_for_both_families() {
    let root = tempfile::tempdir().unwrap();
    let ms = synthetic_observation::observation(root.path(), 6);
    let mut request = request(json!({
        "vis": ms, "imagename": root.path().join("mvc-stage"),
        "specmode": "mfs", "deconvolver": "mtmfs", "nterms": 2,
        "reffreq": "45GHz", "niter": 0, "backend": "cpu", "imsize": 64,
        "cell": "8arcsec", "write_pb": true, "pblimit": -0.1,
    }));
    request.mvc_output_channels = std::num::NonZeroUsize::new(3);
    let context = RunContext {
        host: HostResources::detect().unwrap(),
        policy: request.resource_policy(),
        cancel: Cancel::default(),
        summary: None,
    };
    let resolved = std::cell::Cell::new(None);
    let (cube, input, native, _resident) =
        crate::compile_run(&request, &context, &resolved).unwrap();
    let view = input.mvc_taylor.unwrap();
    let view = view.compile_from_cube(&cube).unwrap();
    let taylor = view.problem;
    let lifecycle = ModelLifecycle::new(&cube, ModelStoragePlan::resident(1 << 16).unwrap());
    let model = lifecycle
        .prepare_final_model(lifecycle.initial_empty().unwrap(), [])
        .unwrap();
    let scene = synthetic_pass::Scene::new(&cube).with_point(0, [32, 32], &[1.0, 0.7, 0.3]);
    let cube_science = scene.reconcile_initial(&cube, model);
    let ApplicationNative {
        runtime,
        publication,
        aw_catalog: _,
    } = native;
    let converted = mvc::assemble_taylor_completion(
        mvc::TaylorAssemblyInputs {
            cube_problem: &cube,
            cube: &cube_science,
            taylor_problem: &taylor,
            expansion_reference_hz: view.expansion_reference_hz,
            controls: publication.controls,
            centres: publication.primary_beam_centres.as_ref(),
        },
        (&runtime.host, &runtime.resource_policy, &runtime.cancel),
    )
    .unwrap();
    let mean = converted.value.primary_beam.clone();
    let ordinary = ContinuumProductInputs::from_major_cycle(&cube, &cube_science);
    let plan = PlannedContinuumGeneration::new(&ordinary, &publication.controls).unwrap();
    let corrected = ContinuumProductInputs::from_major_cycle(&cube, &cube_science)
        .with_cube_taylor_primary_beam(&mean)
        .unwrap();
    let expected = read_mvc_cube_plane(&plan, &corrected, 0).unwrap();
    let uncorrected = read_mvc_cube_plane(&plan, &ordinary, 0).unwrap();
    let index = expected
        .residual
        .iter()
        .zip(&uncorrected.residual)
        .position(|(a, b)| (a - b).abs() > 1e-5)
        .expect("the fixture distinguishes PB correction");
    let expected_taylor_residual: Vec<f32> = converted
        .value
        .scientific
        .normal_state()
        .read_window(0..1)
        .unwrap()
        .coefficient_term(0, 0)
        .unwrap()
        .residual()
        .iter()
        .map(|value| value.re as f32)
        .collect();
    assert!(
        expected_taylor_residual
            .iter()
            .any(|value| value.abs() > 0.01)
    );
    let team = WorkerTeam::new(1).unwrap();
    let mut lanes = (0, None);
    let mut published = Vec::new();
    let result = publish_mvc_products(
        MvcProductSource {
            cube_problem: &cube,
            cube: ProductSource {
                scientific: cube_science,
                masks: None,
                facet_sum_weights: Default::default(),
                saved_psf_beams: None,
                vla_selection_hz: None,
                mvc_beam: None,
            },
            taylor_problem: &taylor,
            taylor: converted,
            taylor_coordinates: view.coordinates,
        },
        &runtime,
        publication,
        &team,
        &mut published,
        &mut lanes,
    )
    .unwrap();
    assert!(result.taylor_names.iter().any(|name| name == ".pb.tt0"));
    assert!(
        result
            .taylor_names
            .iter()
            .any(|name| name == ".residual.tt0")
    );
    let retained_names = result.taylor_names.capacity() * size_of::<String>()
        + result
            .taylor_names
            .iter()
            .map(String::capacity)
            .sum::<usize>();
    assert!(result.names_reservation.memory() >= retained_names as u64);
    assert!(
        result
            .cube
            .planned
            .members()
            .iter()
            .any(|member| member.name() == ".residual")
    );
    assert!(published.iter().any(|name| name.ends_with(".residual")));
    assert!(published.iter().any(|name| name.ends_with(".pb.tt0")));
    let pixel = [index / 64, index % 64, 0, 0];
    let read = |suffix: &str| {
        let image =
            casa_images::PagedImage::<f32>::open(root.path().join(format!("mvc-stage{suffix}")))
                .unwrap();
        *image
            .get_slice(&pixel, &[1, 1, 1, 1])
            .unwrap()
            .first()
            .unwrap()
    };
    assert!((read(".residual") - expected.residual[index]).abs() < 1e-6);
    let read_plane = |suffix: &str| {
        casa_images::PagedImage::<f32>::open(root.path().join(format!("mvc-stage{suffix}")))
            .unwrap()
            .get()
            .unwrap()
    };
    let actual_residual = read_plane(".residual.tt0");
    let actual_pb = read_plane(".pb.tt0");
    for x in 0..64 {
        for y in 0..64 {
            let index = x * 64 + y;
            assert!(
                (actual_residual[[x, y, 0, 0]] - expected_taylor_residual[index]).abs() < 1e-6,
                "Taylor residual at {x}, {y}"
            );
            assert_eq!(
                actual_pb[[x, y, 0, 0]],
                mean[index],
                "Taylor PB at {x}, {y}"
            );
        }
    }
}

/// A cancelled owner stops before admission or creating any output family.
#[test]
fn mvc_native_publication_honours_precancel_before_admission() {
    let root = tempfile::tempdir().unwrap();
    let ms = synthetic_observation::observation(root.path(), 6);
    let mut request = request(json!({
        "vis": ms, "imagename": root.path().join("mvc-cancelled"),
        "specmode": "mfs", "deconvolver": "mtmfs", "nterms": 2,
        "niter": 0, "backend": "cpu", "imsize": 64, "cell": "0.3arcsec",
        "write_pb": true,
    }));
    request.mvc_output_channels = std::num::NonZeroUsize::new(3);
    let context = RunContext {
        host: HostResources::detect().unwrap(),
        policy: request.resource_policy(),
        cancel: Cancel::default(),
        summary: None,
    };
    let (cube, input, native, _resident) =
        crate::compile_run(&request, &context, &std::cell::Cell::new(None)).unwrap();
    let view = input.mvc_taylor.unwrap();
    let view = view.compile_from_cube(&cube).unwrap();
    let taylor = view.problem;
    let lifecycle = ModelLifecycle::new(&cube, ModelStoragePlan::resident(1 << 16).unwrap());
    let model = lifecycle
        .prepare_final_model(lifecycle.initial_empty().unwrap(), [])
        .unwrap();
    let scene = synthetic_pass::Scene::new(&cube).with_point(0, [32, 32], &[1.0, 0.7, 0.3]);
    let cube_science = scene.reconcile_initial(&cube, model);
    let ApplicationNative {
        mut runtime,
        publication,
        aw_catalog: _,
    } = native;
    let converted = mvc::assemble_taylor_completion(
        mvc::TaylorAssemblyInputs {
            cube_problem: &cube,
            cube: &cube_science,
            taylor_problem: &taylor,
            expansion_reference_hz: view.expansion_reference_hz,
            controls: publication.controls,
            centres: publication.primary_beam_centres.as_ref(),
        },
        (&runtime.host, &runtime.resource_policy, &runtime.cancel),
    )
    .unwrap();
    runtime.cancel.cancel();
    runtime.resource_policy = casa_imaging_runtime::ResourcePolicy::Explicit {
        workers: 1,
        memory: 1,
    };
    let mut published = Vec::new();
    let result = publish_mvc_products(
        MvcProductSource {
            cube_problem: &cube,
            cube: ProductSource {
                scientific: cube_science,
                masks: None,
                facet_sum_weights: Default::default(),
                saved_psf_beams: None,
                vla_selection_hz: None,
                mvc_beam: None,
            },
            taylor_problem: &taylor,
            taylor: converted,
            taylor_coordinates: view.coordinates,
        },
        &runtime,
        publication,
        &WorkerTeam::new(1).unwrap(),
        &mut published,
        &mut (0, None),
    );
    assert!(matches!(result, Err(super::NativeError::Cancelled(_))));
    assert!(published.is_empty());
    assert!(!root.path().join("mvc-cancelled.psf").exists());
}

/// A failure while staging Taylor products discards the already staged cube.
#[test]
fn mvc_native_publication_discards_cube_when_taylor_staging_fails() {
    let root = tempfile::tempdir().unwrap();
    let ms = synthetic_observation::observation(root.path(), 6);
    let mut request = request(json!({
        "vis": ms, "imagename": root.path().join("mvc-failed"),
        "specmode": "mfs", "deconvolver": "mtmfs", "nterms": 2,
        "niter": 0, "backend": "cpu", "imsize": 64, "cell": "0.3arcsec",
        "write_pb": true,
    }));
    request.mvc_output_channels = std::num::NonZeroUsize::new(3);
    let context = RunContext {
        host: HostResources::detect().unwrap(),
        policy: request.resource_policy(),
        cancel: Cancel::default(),
        summary: None,
    };
    let (cube, input, native, _resident) =
        crate::compile_run(&request, &context, &std::cell::Cell::new(None)).unwrap();
    let view = input.mvc_taylor.unwrap();
    let view = view.compile_from_cube(&cube).unwrap();
    let taylor = view.problem;
    let lifecycle = ModelLifecycle::new(&cube, ModelStoragePlan::resident(1 << 16).unwrap());
    let model = lifecycle
        .prepare_final_model(lifecycle.initial_empty().unwrap(), [])
        .unwrap();
    let scene = synthetic_pass::Scene::new(&cube).with_point(0, [32, 32], &[1.0, 0.7, 0.3]);
    let cube_science = scene.reconcile_initial(&cube, model);
    let ApplicationNative {
        runtime,
        publication,
        aw_catalog: _,
    } = native;
    let converted = mvc::assemble_taylor_completion(
        mvc::TaylorAssemblyInputs {
            cube_problem: &cube,
            cube: &cube_science,
            taylor_problem: &taylor,
            expansion_reference_hz: view.expansion_reference_hz,
            controls: publication.controls,
            centres: publication.primary_beam_centres.as_ref(),
        },
        (&runtime.host, &runtime.resource_policy, &runtime.cancel),
    )
    .unwrap();
    let mut published = Vec::new();
    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        publish_mvc_products(
            MvcProductSource {
                cube_problem: &cube,
                cube: ProductSource {
                    scientific: cube_science,
                    masks: None,
                    facet_sum_weights: Default::default(),
                    saved_psf_beams: None,
                    vla_selection_hz: None,
                    mvc_beam: None,
                },
                taylor_problem: &cube,
                taylor: converted,
                taylor_coordinates: view.coordinates,
            },
            &runtime,
            publication,
            &WorkerTeam::new(1).unwrap(),
            &mut published,
            &mut (0, None),
        )
    }));
    assert!(
        result.is_err(),
        "the deliberately mismatched Taylor problem must fail"
    );
    assert!(published.is_empty());
    assert!(!root.path().join("mvc-failed.psf").exists());
    assert!(
        std::fs::read_dir(root.path()).unwrap().all(|entry| {
            let name = entry.unwrap().file_name();
            let name = name.to_string_lossy();
            !(name.contains("mvc-failed") && name.contains("casa-rs-stage"))
        }),
        "failed publication discarded its staging"
    );
}

/// A real inventory admission failure discards both staged families.
#[test]
fn mvc_native_publication_discards_staging_when_inventory_admission_fails() {
    // Admission is process-wide; isolate the exact-budget law from other
    // tests' concurrent reservations.
    const ALONE: &str = "CASA_RS_MVC_INVENTORY_LAW_ALONE";
    if std::env::var_os(ALONE).is_none() {
        let name = std::thread::current()
            .name()
            .expect("libtest names the test")
            .to_owned();
        let output = std::process::Command::new(std::env::current_exe().unwrap())
            .args([name.as_str(), "--exact", "--test-threads=1", "--nocapture"])
            .env(ALONE, "1")
            .output()
            .expect("the isolated law runs");
        assert!(
            output.status.success(),
            "isolated inventory law: {}\n{}\n{}",
            output.status,
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
        return;
    }
    let root = tempfile::tempdir().unwrap();
    let ms = synthetic_observation::observation(root.path(), 6);
    let mut request = request(json!({
        "vis": ms, "imagename": root.path().join("mvc-no-inventory"),
        "specmode": "mfs", "deconvolver": "mtmfs", "nterms": 2,
        "niter": 0, "backend": "cpu", "imsize": 64, "cell": "0.3arcsec",
        "write_pb": true,
    }));
    request.mvc_output_channels = std::num::NonZeroUsize::new(3);
    let context = RunContext {
        host: HostResources::detect().unwrap(),
        policy: request.resource_policy(),
        cancel: Cancel::default(),
        summary: None,
    };
    let (cube, input, native, _resident) =
        crate::compile_run(&request, &context, &std::cell::Cell::new(None)).unwrap();
    let view = input.mvc_taylor.unwrap();
    let view = view.compile_from_cube(&cube).unwrap();
    let taylor = view.problem;
    let lifecycle = ModelLifecycle::new(&cube, ModelStoragePlan::resident(1 << 16).unwrap());
    let model = lifecycle
        .prepare_final_model(lifecycle.initial_empty().unwrap(), [])
        .unwrap();
    let scene = synthetic_pass::Scene::new(&cube).with_point(0, [32, 32], &[1.0, 0.7, 0.3]);
    let cube_science = scene.reconcile_initial(&cube, model);
    let ApplicationNative {
        mut runtime,
        publication,
        aw_catalog: _,
    } = native;
    let converted = mvc::assemble_taylor_completion(
        mvc::TaylorAssemblyInputs {
            cube_problem: &cube,
            cube: &cube_science,
            taylor_problem: &taylor,
            expansion_reference_hz: view.expansion_reference_hz,
            controls: publication.controls,
            centres: publication.primary_beam_centres.as_ref(),
        },
        (&runtime.host, &runtime.resource_policy, &runtime.cancel),
    )
    .unwrap();
    // Permit exactly both product generations, but one byte less than the
    // retained Taylor inventory needs. Both families therefore stage before
    // admission for their names fails.
    let cube_plan = PlannedContinuumGeneration::for_problem(&cube, &publication.controls).unwrap();
    let taylor_plan =
        PlannedContinuumGeneration::for_problem(&taylor, &publication.controls).unwrap();
    let window = casa_imaging_products::ProductStoragePlan::new(1, 1).unwrap();
    let cube_demand = cube_plan
        .planned_demand(&cube, window, &publication.writer)
        .unwrap()
        .peak_residency_bytes();
    let taylor_writer = publication
        .writer
        .with_coordinates(view.coordinates.clone());
    let taylor_demand = taylor_plan
        .planned_demand(&taylor, window, &taylor_writer)
        .unwrap()
        .peak_residency_bytes();
    drop(taylor_writer);
    let name_bytes = taylor_plan.members().len() * size_of::<String>()
        + taylor_plan
            .members()
            .iter()
            .map(|member| member.name().len())
            .sum::<usize>();
    assert!(name_bytes > 1);
    runtime.resource_policy = casa_imaging_runtime::ResourcePolicy::Explicit {
        workers: 1,
        memory: casa_imaging_runtime::reserved_memory()
            + cube_demand
            + taylor_demand
            + name_bytes as u64
            - 1,
    };
    let mut published = Vec::new();
    let result = publish_mvc_products(
        MvcProductSource {
            cube_problem: &cube,
            cube: ProductSource {
                scientific: cube_science,
                masks: None,
                facet_sum_weights: Default::default(),
                saved_psf_beams: None,
                vla_selection_hz: None,
                mvc_beam: None,
            },
            taylor_problem: &taylor,
            taylor: converted,
            taylor_coordinates: view.coordinates,
        },
        &runtime,
        publication,
        &WorkerTeam::new(1).unwrap(),
        &mut published,
        &mut (0, None),
    );
    assert!(
        matches!(result, Err(super::NativeError::Admission(error)) if error.phase == "MVC product inventory")
    );
    assert!(published.is_empty());
    assert!(!root.path().join("mvc-no-inventory.psf").exists());
    assert!(std::fs::read_dir(root.path()).unwrap().all(|entry| {
        let name = entry.unwrap().file_name();
        let name = name.to_string_lossy();
        !(name.contains("mvc-no-inventory") && name.contains("casa-rs-stage"))
    }));
}

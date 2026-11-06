// SPDX-License-Identifier: LGPL-3.0-or-later

//! The MVC cube's independently selected source channels and output axis.

#[path = "../tests/common/synthetic_observation.rs"]
pub(super) mod synthetic_observation;
#[allow(
    dead_code,
    reason = "the shared synthetic pass fixture also supports refresh tests"
)]
#[path = "../../casa-imaging-products/tests/common/synthetic_pass.rs"]
pub(super) mod synthetic_pass;

use serde_json::json;

use crate::{Cancel, HostResources, RunContext};

/// Drive the real survey and problem compiler, with six selected source
/// channels and a two-plane cube. The 256-MHz width would otherwise infer
/// three planes, so this distinguishes the explicitly resolved depth.
#[test]
fn mvc_cube_compiles_its_own_output_depth() {
    let root = tempfile::tempdir().unwrap();
    let ms = synthetic_observation::observation(root.path(), 6);
    let mut request = crate::request::tests::request(json!({
        "vis": ms, "imagename": root.path().join("mvc"),
        "specmode": "cube", "start": "43.936GHz", "width": "256MHz",
        "nchan": -1, "backend": "cpu", "imsize": 64, "cell": "0.3arcsec",
    }));
    request.mvc_output_channels = std::num::NonZeroUsize::new(2);
    let context = RunContext {
        host: HostResources::detect().unwrap(),
        policy: request.resource_policy(),
        cancel: Cancel::default(),
        summary: None,
    };
    let resolved = std::cell::Cell::new(None);
    let (problem, _, _, _resident) = crate::compile_run(&request, &context, &resolved).unwrap();
    let spectral = problem.geometry().spectral();
    assert_eq!(spectral.output_channels(), 2);
    assert!(
        (spectral.channel_centre_hz(1).unwrap() - spectral.channel_centre_hz(0).unwrap() - 256e6)
            .abs()
            < 1.0
    );
}

/// The MVC major cycle uses the full surveyed continuum band to construct
/// a nearest-neighbor, channel-local cube, not a direct Taylor gridding pass.
#[test]
fn mvc_compiles_the_cube_view_from_the_full_selected_band() {
    let root = tempfile::tempdir().unwrap();
    let ms = synthetic_observation::observation(root.path(), 6);
    let mut request = crate::request::tests::request(json!({
        "vis": ms, "imagename": root.path().join("mvc-band"),
        "specmode": "mfs", "deconvolver": "mtmfs", "nterms": 2,
        "reffreq": "45GHz", "niter": 0, "backend": "cpu", "imsize": 64,
        "cell": "0.3arcsec", "write_pb": false,
    }));
    request.mvc_output_channels = std::num::NonZeroUsize::new(3);
    let context = RunContext {
        host: HostResources::detect().unwrap(),
        policy: request.resource_policy(),
        cancel: Cancel::default(),
        summary: None,
    };
    let resolved = std::cell::Cell::new(None);
    let (problem, input, native, _resident) =
        crate::compile_run(&request, &context, &resolved).unwrap();
    let spectral = problem.geometry().spectral();
    assert_eq!(spectral.output_channels(), 3);
    assert!((spectral.channel_centre_hz(0).unwrap() - 44.064e9).abs() < 1.0);
    assert!((spectral.channel_centre_hz(2).unwrap() - 44.576e9).abs() < 1.0);
    assert_eq!(
        problem.reconstruction().basis(),
        casa_imaging_model::ReconstructionBasis::ChannelLocal { channels: 3 }
    );
    assert_eq!(
        problem.science().spectral().sampling().kernel(),
        casa_imaging_model::SpectralKernel::Nearest
    );
    let list = problem.product_list();
    let names = list
        .publication()
        .members()
        .iter()
        .map(|id| list.entries()[id.ordinal()].name().unwrap())
        .collect::<Vec<_>>();
    assert_eq!(names, [".psf", ".residual", ".sumwt", ".pb"]);
    assert!(
        !request.write_pb,
        "compiling the view leaves the parent request unchanged"
    );
    drop(crate::native::prepare(&problem, input, native).unwrap());
    // The first native slice refuses cleaning rather than silently ignoring
    // the parent's requested iterations.
    drop(_resident);
    request.niter = 1;
    assert!(matches!(
        crate::compile_run(&request, &context, &resolved),
        Err(crate::ApplicationDispatchError::Preparation(
            crate::compile::PrepareError::MvcExecutionUnsupported
        ))
    ));
    request.niter = 0;
    request.mvc_output_channels = None;
    request.mvc_auto_channels = true;
    let (auto, _, _, _resident) = crate::compile_run(&request, &context, &resolved).unwrap();
    assert_eq!(auto.geometry().spectral().output_channels(), 3);
    assert!((auto.geometry().spectral().channel_centre_hz(1).unwrap() - 44.320e9).abs() < 1.0);
}

/// Both problems are derived from the same selected observation. The Taylor
/// image coordinate follows the band midpoint, while the requested reffreq
/// remains the independent polynomial expansion reference.
#[test]
fn mvc_compiles_both_views_from_one_survey_with_distinct_taylor_reference() {
    use casa_coordinates::CoordinateType;
    use casa_imaging_model::{ProblemInput, ReconstructionBasis, compile, compile_observation};

    let root = tempfile::tempdir().unwrap();
    let ms = synthetic_observation::observation(root.path(), 6);
    let mut request = crate::request::tests::request(json!({
        "vis": ms, "imagename": root.path().join("mvc-both"),
        "specmode": "mfs", "deconvolver": "mtmfs", "nterms": 2,
        "reffreq": "45GHz", "niter": 0, "backend": "cpu", "imsize": 64,
        "cell": "0.3arcsec", "write_pb": false,
    }));
    request.mvc_output_channels = std::num::NonZeroUsize::new(3);
    let context = RunContext {
        host: HostResources::detect().unwrap(),
        policy: request.resource_policy(),
        cancel: Cancel::default(),
        summary: None,
    };
    let prepared = crate::compile::prepare(&request, &context).unwrap();
    let taylor = prepared.mvc_taylor.unwrap();
    let snapshot = compile_observation(prepared.snapshot).unwrap();
    let cube = compile(ProblemInput::new(
        prepared.specification,
        prepared.geometry,
        snapshot,
        prepared.model_lifecycle,
    ))
    .unwrap();
    let taylor = taylor.compile_from_cube(&cube).unwrap();
    let mt = &taylor.problem;
    assert_eq!(cube.observation(), mt.observation());
    assert_eq!(cube.geometry().spectral().output_channels(), 3);
    assert_eq!(mt.geometry().spectral().output_channels(), 1);
    assert_eq!(
        mt.reconstruction().basis(),
        ReconstructionBasis::Taylor { terms: 2 }
    );
    assert_eq!(taylor.expansion_reference_hz, 45e9);
    let list = mt.product_list();
    let names = list
        .publication()
        .members()
        .iter()
        .map(|id| list.entries()[id.ordinal()].name().unwrap())
        .collect::<Vec<_>>();
    assert!(
        names.contains(&".pb.tt0"),
        "the conversion needs a Taylor PB"
    );
    let coords = &taylor.coordinates[0].1;
    let spectral = coords.find_coordinate(CoordinateType::Spectral).unwrap();
    assert!((coords.coordinate(spectral).reference_value()[0] - 44.320e9).abs() < 1.0);
    assert!((coords.coordinate(spectral).increment()[0] - 768e6).abs() < 1.0);
    drop(prepared.survey);
    drop(prepared.mask_resident);
    request.reffreq = None;
    let default = crate::compile::prepare(&request, &context)
        .unwrap()
        .mvc_taylor
        .unwrap();
    assert!((default.expansion_reference_hz - 44.320e9).abs() < 1.0);
}

/// The native MVC handoff keeps its Taylor completion and PB under a memory
/// reservation and uses the same bounded reduction as the product laws.
#[test]
fn mvc_taylor_completion_is_reduced_and_admitted() {
    use casa_imaging_deconvolution::{ModelLifecycle, ModelStoragePlan, NormalBasis};
    use casa_imaging_model::{ProblemInput, compile, compile_observation};
    use casa_imaging_products::{
        AnalyticPrimaryBeamModel, ContinuumProductControls, ContinuumProductInputs,
        PlannedContinuumGeneration, reduce_mvc_cube,
    };
    let root = tempfile::tempdir().unwrap();
    let ms = synthetic_observation::observation(root.path(), 6);
    let mut request = crate::request::tests::request(json!({
        "vis": ms, "imagename": root.path().join("mvc-completion"),
        "specmode": "mfs", "deconvolver": "mtmfs", "nterms": 2,
        "reffreq": "45GHz", "niter": 0, "backend": "cpu", "imsize": 64,
        "cell": "0.3arcsec", "write_pb": true,
    }));
    request.mvc_output_channels = std::num::NonZeroUsize::new(3);
    let context = RunContext {
        host: HostResources::detect().unwrap(),
        policy: request.resource_policy(),
        cancel: Cancel::default(),
        summary: None,
    };
    let prepared = crate::compile::prepare(&request, &context).unwrap();
    let view = prepared.mvc_taylor.unwrap();
    let snapshot = compile_observation(prepared.snapshot).unwrap();
    let cube = compile(ProblemInput::new(
        prepared.specification,
        prepared.geometry,
        snapshot.clone(),
        prepared.model_lifecycle,
    ))
    .unwrap();
    let taylor = compile(ProblemInput::new(
        view.specification.clone(),
        view.geometry.clone(),
        snapshot,
        view.model_lifecycle,
    ))
    .unwrap();
    let source = &cube.observation().sources()[0];
    let other_snapshot =
        compile_observation(casa_imaging_model::ObservationSnapshotInput::new(vec![
            casa_imaging_model::ObservationSourceInput::new(
                casa_imaging_model::ObservationSourceProvenance::new(
                    "different-observation.ms".into(),
                ),
                source.selection().clone(),
                source.columns(),
                source.corrected_data_present(),
            ),
        ]))
        .unwrap();
    let other_taylor = compile(ProblemInput::new(
        view.specification,
        view.geometry,
        other_snapshot,
        view.model_lifecycle,
    ))
    .unwrap();
    let lifecycle = ModelLifecycle::new(&cube, ModelStoragePlan::resident(1 << 16).unwrap());
    let model = lifecycle
        .prepare_final_model(lifecycle.initial_empty().unwrap(), [])
        .unwrap();
    let scene = synthetic_pass::Scene::new(&cube).with_point(0, [32, 32], &[1.0, 1.0, 1.0]);
    let cube_completion = scene.reconcile_initial(&cube, model);
    let controls = ContinuumProductControls::default()
        .with_primary_beam_model(AnalyticPrimaryBeamModel::CasaVlaBand);
    let inputs = ContinuumProductInputs::from_major_cycle(&cube, &cube_completion);
    let plan = PlannedContinuumGeneration::new(&inputs, &controls).unwrap();
    let cells = 64 * 64;
    let mut pb = vec![0.0; cells];
    let mut psf = vec![0.0; 3 * cells];
    let mut residual = vec![0.0; 2 * cells];
    let mut sumwt = [0.0; 3];
    reduce_mvc_cube(
        &plan,
        &inputs,
        view.expansion_reference_hz,
        &mut pb,
        &mut psf,
        &mut residual,
        &mut sumwt,
    )
    .unwrap();
    let mismatched = crate::mvc::assemble_taylor_completion(
        crate::mvc::TaylorAssemblyInputs {
            cube_problem: &cube,
            cube: &cube_completion,
            taylor_problem: &other_taylor,
            expansion_reference_hz: view.expansion_reference_hz,
            controls,
            centres: None,
        },
        (&context.host, &context.policy, &context.cancel),
    );
    assert!(
        matches!(mismatched, Err(crate::native::NativeError::Other(error)) if error.to_string().contains("same observation"))
    );
    let admitted = crate::mvc::assemble_taylor_completion(
        crate::mvc::TaylorAssemblyInputs {
            cube_problem: &cube,
            cube: &cube_completion,
            taylor_problem: &taylor,
            expansion_reference_hz: view.expansion_reference_hz,
            controls,
            centres: None,
        },
        (&context.host, &context.policy, &context.cancel),
    )
    .unwrap();
    assert_eq!(admitted.value.primary_beam, pb);
    let normal = admitted.value.scientific.normal_state();
    assert_eq!(normal.basis(), NormalBasis::Taylor);
    let window = normal.read_window(0..1).unwrap();
    for term in 0..3 {
        let moment = window.normal_moment(term, 0).unwrap();
        assert_eq!(
            window.published_sum_weight(term, 0).unwrap(),
            f64::from(sumwt[term])
        );
        for (actual, expected) in moment
            .normal_approximation()
            .iter()
            .zip(&psf[term * cells..(term + 1) * cells])
        {
            assert_eq!(actual.re, f64::from(*expected));
        }
        if term < 2 {
            for (actual, expected) in window
                .coefficient_term(term, 0)
                .unwrap()
                .residual()
                .iter()
                .zip(&residual[term * cells..(term + 1) * cells])
            {
                assert_eq!(actual.re, f64::from(*expected));
            }
        }
    }
    let live = admitted.value.scientific.resident_bytes().unwrap()
        + u64::try_from(admitted.value.primary_beam.capacity() * size_of::<f32>()).unwrap();
    let pass_planes = u64::try_from(5 * cells * size_of::<f32>()).unwrap();
    assert!(
        admitted.reservation.memory() >= live + pass_planes,
        "the retained Taylor state, PB and input pass planes overlap under admission"
    );
    let tight = casa_imaging_runtime::ResourcePolicy::Explicit {
        workers: 1,
        memory: 1,
    };
    assert!(matches!(
        crate::mvc::assemble_taylor_completion(
            crate::mvc::TaylorAssemblyInputs {
                cube_problem: &cube,
                cube: &cube_completion,
                taylor_problem: &taylor,
                expansion_reference_hz: view.expansion_reference_hz,
                controls,
                centres: None
            },
            (&context.host, &tight, &context.cancel),
        ),
        Err(crate::native::NativeError::Admission(_))
    ));
    let cancelled = Cancel::default();
    cancelled.cancel();
    assert!(
        matches!(
            crate::mvc::assemble_taylor_completion(
                crate::mvc::TaylorAssemblyInputs {
                    cube_problem: &cube,
                    cube: &cube_completion,
                    taylor_problem: &taylor,
                    expansion_reference_hz: view.expansion_reference_hz,
                    controls,
                    centres: None
                },
                (&context.host, &tight, &cancelled),
            ),
            Err(crate::native::NativeError::Cancelled(_))
        ),
        "a pre-cancelled conversion stops before attempting admission"
    );
    assert!(matches!(
        crate::mvc::assemble_taylor_completion(
            crate::mvc::TaylorAssemblyInputs {
                cube_problem: &cube,
                cube: &cube_completion,
                taylor_problem: &taylor,
                expansion_reference_hz: view.expansion_reference_hz,
                controls,
                centres: None
            },
            (&context.host, &context.policy, &cancelled)
        ),
        Err(crate::native::NativeError::Cancelled(_))
    ));
}

/// W and AW planning use the same surveyed frequency range to express the
/// observed maximum W in wavelengths, without copying spectral windows.
#[test]
fn w_and_aw_projection_contracts_use_the_same_observed_frequency_range() {
    use casa_imaging_model::{ProblemInput, compile, compile_observation};
    let root = tempfile::tempdir().unwrap();
    let ms = synthetic_observation::observation_as(root.path(), 6, "EVLA");
    let compile_gridder = |gridder: &str| {
        let request = crate::request::tests::request(json!({
            "vis": ms, "imagename": root.path().join(format!("projection-{gridder}")),
            "specmode": "mfs", "gridder": gridder, "wprojplanes": 16,
            "cfcache": root.path().join("unused-cf"), "niter": 0,
            "backend": "cpu", "imsize": 64, "cell": "0.3arcsec",
        }));
        let context = RunContext {
            host: HostResources::detect().unwrap(),
            policy: request.resource_policy(),
            cancel: Cancel::default(),
            summary: None,
        };
        let prepared = crate::compile::prepare(&request, &context).unwrap();
        let observation = compile_observation(prepared.snapshot).unwrap();
        compile(ProblemInput::new(
            prepared.specification,
            prepared.geometry,
            observation,
            prepared.model_lifecycle,
        ))
        .unwrap()
    };
    let w = compile_gridder("wproject");
    let aw = compile_gridder("awproject");
    let w_max = w
        .science()
        .measurement_equation()
        .w_projection()
        .unwrap()
        .maximum_abs_w_lambda();
    let aw_max = aw
        .science()
        .measurement_equation()
        .aw_projection()
        .unwrap()
        .maximum_abs_w_lambda();
    assert!(w_max > 1.0, "the fixture must have a nonzero W range");
    assert!(
        (w_max - aw_max).abs() < 1e-9 * w_max,
        "W={w_max}, AW={aw_max}"
    );
}

/// A native dirty MVC run grids the cube, reduces it and publishes both
/// families under one task outcome. The public task reader remains guarded.
#[test]
fn mvc_native_run_publishes_both_families() {
    let root = tempfile::tempdir().unwrap();
    let ms = synthetic_observation::observation(root.path(), 6);
    let prefix = root.path().join("mvc-native");
    let mut request = crate::request::tests::request(json!({
        "vis": ms, "imagename": prefix,
        "specmode": "mfs", "deconvolver": "mtmfs", "nterms": 2,
        "reffreq": "45GHz", "niter": 0, "restoration": false,
        "backend": "cpu", "imsize": 256, "cell": "0.3arcsec",
        "weighting": "natural", "write_pb": true,
    }));
    request.mvc_output_channels = std::num::NonZeroUsize::new(3);
    let context = RunContext {
        host: HostResources::detect().unwrap(),
        policy: request.resource_policy(),
        cancel: Cancel::default(),
        summary: None,
    };
    let outcome = crate::execute(&request, context).expect("native MVC run");
    let mut names = outcome.product_names();
    names.sort();
    assert_eq!(
        names,
        [
            ".pb",
            ".pb.tt0",
            ".psf",
            ".psf.tt0",
            ".psf.tt1",
            ".psf.tt2",
            ".residual",
            ".residual.tt0",
            ".residual.tt1",
            ".sumwt",
            ".sumwt.tt0",
            ".sumwt.tt1",
            ".sumwt.tt2",
        ]
    );
    assert_eq!(outcome.plane_layout(), [1, 1, 1]);
    assert_eq!(outcome.major_cycle_count, 1);
    assert_eq!(outcome.total_minor_iterations, 0);
    for name in &names {
        assert!(
            std::path::PathBuf::from(format!("{}{name}", prefix.display())).exists(),
            "missing published {name}"
        );
    }
    assert_eq!(outcome.summary.products.len(), names.len());
}

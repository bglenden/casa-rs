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
    drop(_resident);
    request.niter = 1;
    request.pblimit = 0.8;
    {
        let (cube, input, _, _resident) =
            crate::compile_run(&request, &context, &resolved).unwrap();
        let taylor = input.mvc_taylor.unwrap().compile_from_cube(&cube).unwrap();
        assert_eq!(cube.reconstruction().controls().max_minor_iterations(), 0);
        assert_eq!(
            taylor
                .problem
                .reconstruction()
                .controls()
                .max_minor_iterations(),
            1
        );
    }
    // The run still refuses an unsupported noise threshold when cleaning.
    request.nsigma = 1.0;
    assert!(matches!(
        crate::compile_run(&request, &context, &resolved),
        Err(crate::ApplicationDispatchError::Preparation(
            crate::compile::PrepareError::MvcExecutionUnsupported
        ))
    ));
    request.niter = 0;
    request.nsigma = 0.0;
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

/// CASA's `copy_image` derives the Taylor spectral header from the cube
/// WCS. An offset phase centre makes the cube conversion differ from the
/// LSRK envelope of the observed field.
#[test]
fn mvc_taylor_header_uses_the_published_cube_axis() {
    use casa_coordinates::CoordinateType;
    use casa_ms::MeasurementSet;
    use casa_types::measures::frequency::FrequencyRef;

    let root = tempfile::tempdir().unwrap();
    let path = synthetic_observation::two_window_observation(root.path(), 6);
    let mut ms = MeasurementSet::open(&path).unwrap();
    for spw in 0..2 {
        ms.spectral_window_mut()
            .unwrap()
            .set_i32(spw, "MEAS_FREQ_REF", FrequencyRef::TOPO.casacore_code())
            .unwrap();
    }
    ms.save().unwrap();
    drop(ms);
    let mut request = crate::request::tests::request(json!({
        "vis": path, "imagename": root.path().join("mvc-axis"),
        "specmode": "mfs", "deconvolver": "mtmfs", "nterms": 2,
        "phasecenter": "J2000 18h00m00.000 -23d15m00.000",
        "niter": 0, "backend": "cpu", "imsize": 64, "cell": "0.3arcsec",
    }));
    request.mvc_output_channels = std::num::NonZeroUsize::new(3);
    let context = RunContext {
        host: HostResources::detect().unwrap(),
        policy: request.resource_policy(),
        cancel: Cancel::default(),
        summary: None,
    };
    let resolved = std::cell::Cell::new(None);
    let (cube, input, _, _resident) = crate::compile_run(&request, &context, &resolved).unwrap();
    let spectral = cube.geometry().spectral();
    let first = spectral.channel_centre_hz(0).unwrap();
    let last = spectral.channel_centre_hz(2).unwrap();
    let width = spectral.channel_centre_hz(1).unwrap() - first;
    let midpoint = (first + last) / 2.0;
    let taylor = input.mvc_taylor.unwrap();
    assert!((taylor.expansion_reference_hz - midpoint).abs() > 1.0);
    let coordinates = &taylor.coordinates[0].1;
    let axis = coordinates.coordinate(
        coordinates
            .find_coordinate(CoordinateType::Spectral)
            .unwrap(),
    );
    let reference = axis.reference_value()[0];
    assert!(
        (reference - midpoint).abs() < 1e-3,
        "Taylor reference {reference}, cube midpoint {midpoint}"
    );
    assert!((axis.increment()[0] - 3.0 * width).abs() < 1e-3);
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
    assert_taylor_model_is_carried(
        &cube,
        &cube_completion,
        &taylor,
        view.expansion_reference_hz,
        controls,
        &context,
        &pb,
    );
    let live = admitted.value.scientific.resident_bytes().unwrap()
        + u64::try_from(admitted.value.primary_beam.capacity() * size_of::<f32>()).unwrap();
    let pass_planes = u64::try_from(5 * cells * size_of::<f32>()).unwrap();
    assert!(
        admitted.reservation.memory() >= live + pass_planes,
        "the retained Taylor state, PB and input pass planes overlap under admission"
    );
    assert_taylor_admission(
        &cube,
        &cube_completion,
        &taylor,
        view.expansion_reference_hz,
        controls,
        &context,
    );
}

fn assert_taylor_admission(
    cube: &casa_imaging_model::CompiledProblem,
    cube_completion: &casa_imaging_deconvolution::MajorCycleCompletion,
    taylor: &casa_imaging_model::CompiledProblem,
    reference_hz: f64,
    controls: casa_imaging_products::ContinuumProductControls,
    context: &RunContext,
) {
    let tight = casa_imaging_runtime::ResourcePolicy::Explicit {
        workers: 1,
        memory: 1,
    };
    assert!(matches!(
        crate::mvc::assemble_taylor_completion(
            crate::mvc::TaylorAssemblyInputs {
                cube_problem: cube,
                cube: cube_completion,
                taylor_problem: taylor,
                expansion_reference_hz: reference_hz,
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
                    cube_problem: cube,
                    cube: cube_completion,
                    taylor_problem: taylor,
                    expansion_reference_hz: reference_hz,
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
                cube_problem: cube,
                cube: cube_completion,
                taylor_problem: taylor,
                expansion_reference_hz: reference_hz,
                controls,
                centres: None
            },
            (&context.host, &context.policy, &cancelled)
        ),
        Err(crate::native::NativeError::Cancelled(_))
    ));
}

fn assert_taylor_model_is_carried(
    cube: &casa_imaging_model::CompiledProblem,
    cube_completion: &casa_imaging_deconvolution::MajorCycleCompletion,
    taylor: &casa_imaging_model::CompiledProblem,
    reference_hz: f64,
    controls: casa_imaging_products::ContinuumProductControls,
    context: &RunContext,
    expected_pb: &[f32],
) {
    use casa_imaging_deconvolution::{ModelLifecycle, ModelStoragePlan};
    let cells = 64 * 64;
    // A minor cycle's prepared Taylor model must survive reassembly around
    // the refreshed normal state, including its pending update.
    let taylor_lifecycle = ModelLifecycle::new(taylor, ModelStoragePlan::resident(cells).unwrap());
    let mut initial = vec![0.0_f32; 2 * cells];
    let point = 32 * 64 + 32;
    initial[point] = 1.0;
    initial[cells + point] = -0.75;
    let prepared_model = taylor_lifecycle
        .prepare_final_model(
            taylor_lifecycle.initial_from_start_model(&initial).unwrap(),
            [casa_imaging_model::ModelDeltaTerm::new(
                casa_imaging_model::ModelCell::new(0, 0, 0, [32, 32]),
                casa_imaging_model::ModelValue::new(0.5).unwrap(),
            )],
        )
        .unwrap();
    let carried = crate::mvc::assemble_taylor_with_model(
        crate::mvc::TaylorAssemblyInputs {
            cube_problem: cube,
            cube: cube_completion,
            taylor_problem: taylor,
            expansion_reference_hz: reference_hz,
            controls,
            centres: None,
        },
        Some(prepared_model),
        None,
        (&context.host, &context.policy, &context.cancel),
    )
    .unwrap();
    let model = carried.value.scientific.final_model();
    assert_eq!(
        model.read_samples(point..point + 1).unwrap()[0]
            .value()
            .value(),
        1.5
    );
    assert_eq!(
        model
            .read_samples(cells + point..cells + point + 1)
            .unwrap()[0]
            .value()
            .value(),
        -0.75
    );
    assert_eq!(carried.value.primary_beam, expected_pb);
    let lifecycle = ModelLifecycle::new(cube, ModelStoragePlan::resident(1 << 16).unwrap());
    let wrong_model = lifecycle
        .prepare_final_model(lifecycle.initial_empty().unwrap(), [])
        .unwrap();
    assert!(matches!(
        crate::mvc::assemble_taylor_with_model(
            crate::mvc::TaylorAssemblyInputs {
                cube_problem: cube, cube: cube_completion, taylor_problem: taylor,
                expansion_reference_hz: reference_hz, controls, centres: None,
            },
            Some(wrong_model),
            None,
            (&context.host, &context.policy, &context.cancel),
        ),
        Err(crate::native::NativeError::Other(error)) if error.to_string().contains("model does not match")
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

/// A test-only backing with the same samples and no reported resident charge.
/// Comparing it with the real resident owner isolates the model's charge from
/// the other projection workspace. It is not used for a live memory bound.
#[derive(Debug)]
struct UnchargedModelFactory {
    cancel_on_last_write: Option<Cancel>,
}

#[derive(Debug)]
struct UnchargedModel {
    samples: Vec<casa_imaging_model::ModelSample>,
    cancel_on_last_write: Option<Cancel>,
}

impl casa_imaging_deconvolution::ModelStorageFactory for UnchargedModelFactory {
    fn create(
        &self,
        count: usize,
    ) -> Result<
        Box<dyn casa_imaging_deconvolution::ModelSampleStorage>,
        casa_imaging_deconvolution::ModelLifecycleError,
    > {
        let zero = casa_imaging_model::ModelSample::valid(
            casa_imaging_model::ModelValue::new(0.0).unwrap(),
        );
        Ok(Box::new(UnchargedModel {
            samples: vec![zero; count],
            cancel_on_last_write: self.cancel_on_last_write.clone(),
        }))
    }
}

impl casa_imaging_deconvolution::ModelSampleStorage for UnchargedModel {
    fn sample_count(&self) -> usize {
        self.samples.len()
    }
    fn read(
        &self,
        start: usize,
        out: &mut [casa_imaging_model::ModelSample],
    ) -> Result<(), casa_imaging_deconvolution::ModelLifecycleError> {
        out.copy_from_slice(&self.samples[start..start + out.len()]);
        Ok(())
    }
    fn write(
        &mut self,
        start: usize,
        samples: &[casa_imaging_model::ModelSample],
    ) -> Result<(), casa_imaging_deconvolution::ModelLifecycleError> {
        self.samples[start..start + samples.len()].copy_from_slice(samples);
        if start + samples.len() == self.samples.len()
            && let Some(cancel) = &self.cancel_on_last_write
        {
            cancel.cancel();
        }
        Ok(())
    }
    fn apply_updates(
        &self,
        _updates: &[casa_imaging_deconvolution::ModelSampleUpdate],
        _precision: casa_imaging_model::NumericPrecision,
        _bound: f64,
    ) -> Result<f64, casa_imaging_deconvolution::ModelLifecycleError> {
        unreachable!("the projection creates an initial model without updates")
    }
}

/// The cube model owner receives canonical channel planes formed from the
/// Taylor model and each channel's real PB. Its separate admission remains
/// held with the resulting model (#1069).
#[test]
fn mvc_projected_model_uses_each_cube_frequency_and_beam_under_admission() {
    use casa_imaging_deconvolution::{ModelLifecycle, ModelStoragePlan};
    use casa_imaging_model::{ProblemInput, compile, compile_observation};
    use casa_imaging_products::{
        AnalyticPrimaryBeamModel, ContinuumProductControls, ContinuumProductInputs,
        PlannedContinuumGeneration, read_mvc_cube_primary_beam,
    };
    let root = tempfile::tempdir().unwrap();
    let ms = synthetic_observation::observation(root.path(), 6);
    let mut request = crate::request::tests::request(json!({
        "vis": ms, "imagename": root.path().join("mvc-model"),
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
        view.specification,
        view.geometry,
        snapshot,
        view.model_lifecycle,
    ))
    .unwrap();
    let cube_lifecycle = ModelLifecycle::new(&cube, ModelStoragePlan::resident(1 << 16).unwrap());
    let empty = cube_lifecycle
        .prepare_final_model(cube_lifecycle.initial_empty().unwrap(), [])
        .unwrap();
    let scene = synthetic_pass::Scene::new(&cube).with_point(0, [32, 32], &[1.0, 1.0, 1.0]);
    let cube_completion = scene.reconcile_initial(&cube, empty);
    let controls = ContinuumProductControls::default()
        .with_primary_beam_model(AnalyticPrimaryBeamModel::CasaVlaBand);
    let taylor_completion = crate::mvc::assemble_taylor_completion(
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
    let cells = 64 * 64;
    let points = [32 * 64 + 32, 10 * 64 + 20];
    let mut coefficients = vec![0.0_f32; 2 * cells];
    for &point in &points {
        coefficients[point] = 1.25;
        coefficients[cells + point] = -0.75;
    }
    let taylor_lifecycle = ModelLifecycle::new(&taylor, ModelStoragePlan::resident(cells).unwrap());
    let taylor_model = taylor_lifecycle
        .initial_from_start_model(&coefficients)
        .unwrap();
    // An asymmetric common PB distinguishes the model's y-major order
    // from the product plane's x-major order at the off-axis point.
    let mut mean_pb = taylor_completion.value.primary_beam.clone();
    mean_pb[20 * 64 + 10] = 0.7;
    mean_pb[10 * 64 + 20] = 0.9;
    let projection_inputs = || crate::mvc::MvcModelProjectionInputs {
        cube_problem: &cube,
        cube: &cube_completion,
        cube_lifecycle: &cube_lifecycle,
        taylor_problem: &taylor,
        taylor_model: &taylor_model,
        mean_pb: &mean_pb,
        saved_pb: None,
        expansion_reference_hz: view.expansion_reference_hz,
        controls,
        centres: None,
    };
    let projected = crate::mvc::project_mvc_model(
        projection_inputs(),
        (&context.host, &context.policy, &context.cancel),
    )
    .unwrap();
    assert!(projected.reservation.memory() >= projected.value.resident_bytes());
    let uncharged_lifecycle = ModelLifecycle::new(
        &cube,
        ModelStoragePlan::new(
            std::sync::Arc::new(UnchargedModelFactory {
                cancel_on_last_write: None,
            }),
            1 << 16,
        )
        .unwrap(),
    );
    let mut uncharged_inputs = projection_inputs();
    uncharged_inputs.cube_lifecycle = &uncharged_lifecycle;
    let uncharged = crate::mvc::project_mvc_model(
        uncharged_inputs,
        (&context.host, &context.policy, &context.cancel),
    )
    .unwrap();
    assert_eq!(
        projected.reservation.memory() - uncharged.reservation.memory(),
        projected.value.resident_bytes(),
        "the actual model owner adds its resident bytes to admission"
    );
    let inputs = ContinuumProductInputs::from_major_cycle(&cube, &cube_completion);
    let planned = PlannedContinuumGeneration::new(&inputs, &controls).unwrap();
    for channel in 0..3 {
        let pb = read_mvc_cube_primary_beam(&planned, &inputs, channel).unwrap();
        let freq = cube
            .geometry()
            .spectral()
            .channel_centre_hz(channel)
            .unwrap();
        let factor = (freq - 45e9) / 45e9;
        for &point in &points {
            let polynomial = (1.25_f64 + factor * -0.75_f64) as f32;
            let (x, y) = (point % 64, point / 64);
            let product_pixel = x * 64 + y;
            let expected = polynomial * pb.pixels[product_pixel] / mean_pb[product_pixel];
            let index = channel * cells + point;
            let actual = projected.value.read_samples(index..index + 1).unwrap()[0]
                .value()
                .value() as f32;
            assert_eq!(actual, expected, "channel {channel}, pixel {point}");
            let uncharged_actual = uncharged.value.read_samples(index..index + 1).unwrap()[0]
                .value()
                .value() as f32;
            assert_eq!(uncharged_actual, actual);
        }
    }
    drop(uncharged);
    for channel in 0..3 {
        let transposed = channel * cells + 20 * 64 + 10;
        assert_eq!(
            projected
                .value
                .read_samples(transposed..transposed + 1)
                .unwrap()[0]
                .value()
                .value(),
            0.0
        );
    }
    let tight = casa_imaging_runtime::ResourcePolicy::Explicit {
        workers: 1,
        memory: 1,
    };
    assert!(matches!(
        crate::mvc::project_mvc_model(
            projection_inputs(),
            (&context.host, &tight, &context.cancel)
        ),
        Err(crate::native::NativeError::Admission(_))
    ));
    let late_cancel = Cancel::default();
    let late_lifecycle = ModelLifecycle::new(
        &cube,
        ModelStoragePlan::new(
            std::sync::Arc::new(UnchargedModelFactory {
                cancel_on_last_write: Some(late_cancel.clone()),
            }),
            1 << 16,
        )
        .unwrap(),
    );
    let mut late_inputs = projection_inputs();
    late_inputs.cube_lifecycle = &late_lifecycle;
    assert!(matches!(
        crate::mvc::project_mvc_model(late_inputs, (&context.host, &context.policy, &late_cancel)),
        Err(crate::native::NativeError::Cancelled(_))
    ));
    let cancelled = Cancel::default();
    cancelled.cancel();
    assert!(matches!(
        crate::mvc::project_mvc_model(projection_inputs(), (&context.host, &tight, &cancelled)),
        Err(crate::native::NativeError::Cancelled(_))
    ));
}

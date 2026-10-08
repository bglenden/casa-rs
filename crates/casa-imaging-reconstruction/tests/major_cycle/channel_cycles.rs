// SPDX-License-Identifier: LGPL-3.0-or-later

//! Channel-local (T38/T55) reconstruction cycles over cube normal states:
//! output-channel order, per-channel iteration limits, zero-weight planes,
//! prepared-plane coverage, and the casacore Högbom oracle.

use super::*;

#[test]
fn t38_two_channel_hogbom_cycle_is_ordered_and_model_plane_complete() {
    let problem = t38_cube_problem(238);
    let (lifecycle, normal, continuation) = initial_round(&problem, &scene(&problem), 239);
    let coordinate = problem.geometry().domains()[0].direction();
    let mask = ReconstructionMask::full_plane(
        problem.problem_id(),
        continuation.generation().generation_id(),
        coordinate,
        normal.shape(),
    )
    .expect("shared cube mask");
    let program = MinorCycleProgram::for_algorithm(
        ReconstructionAlgorithm::Hogbom,
        problem.reconstruction().controls(),
    )
    .expect("cube Högbom program")
    .record_component_sequence(16)
    .expect("bounded component evidence");
    let result = ReconstructionCycle::new(ChannelCyclePolicy::Independent, program)
        .run(&lifecycle, continuation.generation(), &normal, &mask)
        .expect("two-channel reconstruction cycle");

    let channels = result.evidence().channels();
    assert_eq!(channels.len(), 2);
    assert_eq!(
        channels
            .iter()
            .map(|channel| channel.output_channel())
            .collect::<Vec<_>>(),
        vec![0, 1],
        "cycle evidence is always in output-channel order"
    );
    assert!(channels.iter().all(|channel| {
        channel
            .minor_cycle()
            .is_some_and(|evidence| evidence.iterations() > 0)
    }));
    let delta = result
        .delta()
        .expect("both channel planes update the model");
    assert_eq!(
        delta
            .terms()
            .iter()
            .map(|term| term.cell().coefficient())
            .collect::<std::collections::BTreeSet<_>>(),
        std::collections::BTreeSet::from([0, 1]),
        "one owner-minted delta contains every cleaned channel plane"
    );
}

#[cfg(feature = "cpp-interop-tests")]
// Reconstruct the finite-support residual implied by Rust's recorded
// components without treating casacore's working residual as a Major-Cycle
// oracle.
fn t38_finite_hogbom_residual(
    initial: &[f32],
    psf: &[f32],
    shape: [usize; 2],
    psf_peak: [usize; 2],
    components: &[([usize; 2], f32)],
) -> Vec<f32> {
    let mut residual = initial.to_vec();
    for (component, flux) in components {
        for x in 0..shape[0] {
            let Some(source_x) = (x + psf_peak[0]).checked_sub(component[0]) else {
                continue;
            };
            if source_x >= shape[0] {
                continue;
            }
            for y in 0..shape[1] {
                let Some(source_y) = (y + psf_peak[1]).checked_sub(component[1]) else {
                    continue;
                };
                if source_y < shape[1] {
                    residual[x * shape[1] + y] -= flux * psf[source_x * shape[1] + source_y];
                }
            }
        }
    }
    residual
}

#[cfg(feature = "cpp-interop-tests")]
#[test]
fn t38_casacore_minor_cycle_and_paired_final_residual_are_split_oracles() {
    use casa_imaging_reconstruction::MaskBox;
    use casa_test_support::hogbom_interop::HogbomOracle;

    // Independent channels each receive the iteration limit: two components
    // per channel, compared step by step with casacore.
    let controls = ReconstructionControls::new(2, 0.5, 0.0).with_noise_sigma(0.0);
    let problem = t38_cube_problem_with_controls(246, 2, controls);
    let scene = scene(&problem);
    let (_, normal, continuation) = initial_round(&problem, &scene, 247);
    let (mut lifecycle, carried) = ModelLifecycle::continue_from(
        ExecutableModelProblem::from_compiled(problem.clone()).expect("continued cube problem"),
        attempt(248),
        2,
        continuation,
        casa_imaging_reconstruction::ModelStoragePlan::resident(usize::MAX)
            .expect("positive model window"),
    )
    .expect("continue the initial final model");

    // Keep the mask finite and interior, away from the source, so both
    // implementations clean the same unambiguous two-component trajectory.
    let mask = ReconstructionMask::from_boxes(
        problem.problem_id(),
        carried.generation_id(),
        problem.geometry().domains()[0].direction(),
        normal.shape(),
        [MaskBox::new([2, 4], [4, 4]).expect("interior CLEAN strip")],
    )
    .expect("identical interior CASA/Rust mask");
    let mut mask_pixels = Vec::with_capacity(normal.shape()[0] * normal.shape()[1]);
    for x in 0..normal.shape()[0] {
        for y in 0..normal.shape()[1] {
            mask_pixels.push(mask.contains([x, y]));
        }
    }
    let program = MinorCycleProgram::for_algorithm(
        ReconstructionAlgorithm::Hogbom,
        problem.reconstruction().controls(),
    )
    .expect("identical Högbom controls")
    .record_component_sequence(4)
    .expect("bounded component sequence");
    let rust = ReconstructionCycle::new(ChannelCyclePolicy::Independent, program)
        .run(&lifecycle, &carried, &normal, &mask)
        .expect("Rust channel-local minor cycle");
    assert_eq!(rust.evidence().iterations(), 4);
    assert!(rust.evidence().requests_reconciliation());

    for (channel_index, channel) in rust.evidence().channels().iter().enumerate() {
        let window = normal
            .read_window(channel_index..channel_index + 1)
            .expect("channel normal window");
        let plane = window
            .polarization_plane(0, 0)
            .expect("channel normal plane");
        let psf = plane
            .normal_approximation()
            .iter()
            .map(|value| value.re as f32)
            .collect::<Vec<_>>();
        let residual = plane
            .residual()
            .iter()
            .map(|value| value.re as f32)
            .collect::<Vec<_>>();
        let casa_steps = [1, 2].map(|iterations| {
            HogbomOracle::clean_minor_cycle_2d_masked(
                &psf,
                &residual,
                plane.shape(),
                &mask_pixels,
                0.5,
                0.0,
                iterations,
            )
            .expect("CASA/casacore masked Högbom oracle")
        });
        let evidence = channel.minor_cycle().expect("valid channel evidence");
        assert_eq!(evidence.iterations(), 2);
        let components = evidence
            .recorded_component_sequence()
            .expect("recorded T38 component trajectory");
        assert_eq!(components.len(), 2);

        let mut cumulative_model = vec![0.0_f32; psf.len()];
        for (step, (component, casa)) in components.iter().zip(&casa_steps).enumerate() {
            let pixel = component.cell().pixel();
            cumulative_model[pixel[0] * plane.shape()[1] + pixel[1]] += component.flux() as f32;
            assert_eq!(casa.iterdone, step + 1);
            for (rust_value, casa_value) in cumulative_model.iter().zip(&casa.model) {
                assert!((rust_value - casa_value).abs() < 1.0e-5);
            }
        }

        let casa = &casa_steps[1];
        let psf_peak = psf
            .iter()
            .map(|value| f64::from(value.abs()))
            .fold(0.0_f64, f64::max);
        let casa_terminal_peak = f64::from(casa.peak_residual_jy_per_beam) / psf_peak;
        assert!((evidence.final_peak_flux() - casa_terminal_peak).abs() < 1.0e-5);
        let mut psf_peak_index = 0;
        for index in 1..psf.len() {
            if psf[index].abs() > psf[psf_peak_index].abs() {
                psf_peak_index = index;
            }
        }
        let component_values = components
            .iter()
            .map(|component| (component.cell().pixel(), component.flux() as f32))
            .collect::<Vec<_>>();
        let rust_working_residual = t38_finite_hogbom_residual(
            &residual,
            &psf,
            plane.shape(),
            [
                psf_peak_index / plane.shape()[1],
                psf_peak_index % plane.shape()[1],
            ],
            &component_values,
        );
        for (index, ((rust_value, casa_value), selected)) in rust_working_residual
            .iter()
            .zip(&casa.residual)
            .zip(&mask_pixels)
            .enumerate()
        {
            if *selected {
                assert!(
                    (rust_value - casa_value).abs() < 1.0e-4,
                    "channel {channel_index} finite minor residual diverged at {:?}: rust={rust_value} casa={casa_value}",
                    [index / plane.shape()[1], index % plane.shape()[1]],
                );
            }
        }
    }

    // The residual refresh of the cleaned model keeps the initial PSF, sum
    // weights and validity and replaces only the residual; a fresh initial
    // pass of the same final model must reproduce it bit for bit.
    let (delta, _) = rust.into_parts();
    let preparation =
        MajorCyclePreparation::prepare(&lifecycle, carried, delta).expect("prepare final model");
    let final_join = scene.reconcile_refresh(&problem, &mut lifecycle, normal, preparation);
    assert!(final_join.model_completion().delta().is_some());
    let (paired_normal, continuation) = final_join.into_continuation();
    let paired_content = paired_normal
        .diagnostic_content_identity()
        .expect("refreshed content");

    let (mut replay_lifecycle, replay_carried) = ModelLifecycle::continue_from(
        ExecutableModelProblem::from_compiled(problem.clone()).expect("replay cube problem"),
        attempt(249),
        3,
        continuation,
        casa_imaging_reconstruction::ModelStoragePlan::resident(usize::MAX)
            .expect("positive model window"),
    )
    .expect("continue the exact final model for a fresh initial pass");
    let replayed_preparation =
        MajorCyclePreparation::prepare(&replay_lifecycle, replay_carried, None)
            .expect("prepare the carried final model");
    let replayed_join = scene.reconcile(&problem, &mut replay_lifecycle, replayed_preparation);
    assert_eq!(
        replayed_join
            .normal_state()
            .diagnostic_content_identity()
            .expect("fresh content"),
        paired_content,
        "a residual refresh is bit-exact against a fresh initial pass of the same model"
    );
    assert_eq!(replayed_join.model_completion().delta(), None);
}

#[test]
fn t38_independent_channels_receive_the_same_iteration_limit() {
    let problem = t38_cube_problem_with_controls(244, 2, ReconstructionControls::new(3, 0.5, 0.0));
    let (lifecycle, normal, continuation) = initial_round(&problem, &scene(&problem), 245);
    let mask = ReconstructionMask::full_plane(
        problem.problem_id(),
        continuation.generation().generation_id(),
        problem.geometry().domains()[0].direction(),
        normal.shape(),
    )
    .expect("shared cube mask");
    let program = MinorCycleProgram::for_algorithm(
        ReconstructionAlgorithm::Hogbom,
        problem.reconstruction().controls(),
    )
    .expect("cube Högbom program");
    let result = ReconstructionCycle::new(ChannelCyclePolicy::Independent, program)
        .run(&lifecycle, continuation.generation(), &normal, &mask)
        .expect("channel-local cube cycle");
    let iterations = result
        .evidence()
        .channels()
        .iter()
        .map(|channel| {
            channel.minor_cycle().map_or(
                0,
                casa_imaging_reconstruction::MinorCycleEvidence::iterations,
            )
        })
        .collect::<Vec<_>>();
    assert_eq!(iterations, vec![3, 3]);
    assert_eq!(result.evidence().iterations(), 6);
}

#[test]
fn t38_casa_inclusive_iteration_limit_is_applied_to_every_channel() {
    let controls = ReconstructionControls::new(2, 0.5, 0.0)
        .with_hogbom_iteration_accounting(HogbomIterationAccounting::CasaInclusive);
    let problem = t38_cube_problem_with_controls(250, 2, controls);
    let (lifecycle, normal, continuation) = initial_round(&problem, &scene(&problem), 251);
    let mask = ReconstructionMask::full_plane(
        problem.problem_id(),
        continuation.generation().generation_id(),
        problem.geometry().domains()[0].direction(),
        normal.shape(),
    )
    .expect("shared cube mask");
    let program = MinorCycleProgram::for_algorithm(
        ReconstructionAlgorithm::Hogbom,
        problem.reconstruction().controls(),
    )
    .expect("cube Högbom program");
    let result = ReconstructionCycle::new(ChannelCyclePolicy::Independent, program)
        .run(&lifecycle, continuation.generation(), &normal, &mask)
        .expect("CASA-inclusive channel-local cycle");
    let actual = result
        .evidence()
        .channels()
        .iter()
        .map(|channel| channel.minor_cycle().expect("valid channel").iterations())
        .collect::<Vec<_>>();
    let charged = result
        .evidence()
        .channels()
        .iter()
        .map(|channel| {
            channel
                .minor_cycle()
                .expect("valid channel")
                .controller_iterations()
        })
        .collect::<Vec<_>>();
    assert_eq!(actual, vec![3, 3]);
    assert_eq!(charged, vec![2, 2]);
    assert_eq!(result.evidence().iterations(), 6);
    assert_eq!(result.evidence().controller_iterations(), 4);
}

#[test]
fn t38_zero_weight_channels_are_ordered_and_never_cleaned() {
    // Channel 1 carries only zero-weight samples and no sample reaches
    // channel 2; a pass marks both planes unmapped (CASA blanks a plane
    // whose accumulated weight is zero, whatever the reason).
    let problem = t38_cube_problem_with_channels(240, 3);
    let scene = scene(&problem).with_weights(vec![1.0, 0.0, 0.0]);
    let (lifecycle, normal, continuation) = initial_round(&problem, &scene, 241);
    let mask = ReconstructionMask::full_plane(
        problem.problem_id(),
        continuation.generation().generation_id(),
        problem.geometry().domains()[0].direction(),
        normal.shape(),
    )
    .expect("shared cube mask");
    let program = MinorCycleProgram::for_algorithm(
        ReconstructionAlgorithm::Hogbom,
        problem.reconstruction().controls(),
    )
    .expect("cube Högbom program");
    let result = ReconstructionCycle::new(ChannelCyclePolicy::Independent, program)
        .run(&lifecycle, continuation.generation(), &normal, &mask)
        .expect("three-channel reconstruction cycle");
    let channels = result.evidence().channels();
    assert_eq!(
        channels
            .iter()
            .map(|channel| (channel.output_channel(), channel.validity()))
            .collect::<Vec<_>>(),
        vec![
            (0, SpectralChannelValidity::Valid),
            (1, SpectralChannelValidity::Unmapped),
            (2, SpectralChannelValidity::Unmapped),
        ]
    );
    assert!(channels[0].minor_cycle().is_some());
    assert!(channels[1].minor_cycle().is_none());
    assert!(channels[2].minor_cycle().is_none());
    assert!(result.delta().is_some_and(|delta| {
        delta
            .terms()
            .iter()
            .all(|term| term.cell().coefficient() == 0)
    }));
}

#[test]
fn t55_prepared_cube_planes_preserve_results_and_require_exact_ordered_coverage() {
    for (algorithm, blank_second_channel) in [
        (ReconstructionAlgorithm::Hogbom, false),
        (ReconstructionAlgorithm::Hogbom, true),
        (ReconstructionAlgorithm::Clark, false),
        (ReconstructionAlgorithm::Clark, true),
    ] {
        let problem = reconstruction_problem(
            240,
            8,
            3,
            ReconstructionBasis::ChannelLocal { channels: 3 },
            algorithm.clone(),
            ReconstructionControls::new(8, 0.5, 0.0)
                .with_noise_sigma(0.0)
                .with_cycle_threshold(1.0, 0.05, 0.8),
        );
        let mut scene = scene(&problem);
        if blank_second_channel {
            scene = scene.with_weights(vec![1.0, 0.0, 0.0]);
        }
        let (lifecycle, normal, continuation) = initial_round(&problem, &scene, 241);
        let base = continuation.generation();
        let mask = ReconstructionMask::full_plane(
            problem.problem_id(),
            base.generation_id(),
            problem.geometry().domains()[0].direction(),
            normal.shape(),
        )
        .expect("cube mask");
        if matches!(&algorithm, ReconstructionAlgorithm::Hogbom) && !blank_second_channel {
            let peaks = (0..3)
                .filter_map(|ordinal| {
                    let channel = normal.slab().core_range().start + ordinal;
                    let window = normal.read_window(channel..channel + 1).unwrap();
                    let plane = window.polarization_plane(0, 0).unwrap();
                    if plane.validity() != SpectralChannelValidity::Valid {
                        return None;
                    }
                    let psf_peak = plane
                        .normal_approximation()
                        .iter()
                        .map(|value| value.re.abs())
                        .fold(0.0_f64, f64::max);
                    let peak = plane
                        .residual()
                        .iter()
                        .map(|value| value.re.abs() / psf_peak)
                        .fold(0.0_f64, f64::max);
                    Some((ordinal, peak))
                })
                .collect::<Vec<_>>();
            assert!(peaks.len() >= 2);
            let (near_channel, near_peak) = peaks
                .iter()
                .copied()
                .min_by(|left, right| left.1.total_cmp(&right.1))
                .unwrap();
            let threshold = near_peak / 1.005;
            assert!(
                peaks.iter().any(|(_, peak)| *peak > 1.01 * threshold),
                "fixture needs a second active plane: {peaks:?}"
            );
            let near_cycle = ReconstructionCycle::new(
                ChannelCyclePolicy::Independent,
                MinorCycleProgram::for_algorithm(
                    ReconstructionAlgorithm::Hogbom,
                    ReconstructionControls::new(1, 0.1, threshold),
                )
                .unwrap(),
            );
            let near_result = near_cycle.run(&lifecycle, base, &normal, &mask).unwrap();
            assert_eq!(
                near_result.evidence().channels()[near_channel]
                    .minor_cycle()
                    .unwrap()
                    .iterations(),
                1,
                "a near-threshold plane remains eligible while another plane is above the global stop"
            );
        }
        let program =
            MinorCycleProgram::for_algorithm(algorithm, problem.reconstruction().controls())
                .expect("point CLEAN program")
                .record_component_sequence(16)
                .expect("bounded diagnostics");
        let cycle = ReconstructionCycle::new(ChannelCyclePolicy::Independent, program);
        let serial = cycle
            .run(&lifecycle, base, &normal, &mask)
            .expect("serial cycle");
        let thresholds = serial
            .evidence()
            .channels()
            .iter()
            .filter_map(|channel| channel.minor_cycle())
            .map(|minor| minor.cycle_threshold().expect("common cycle threshold"))
            .collect::<Vec<_>>();
        assert!(!thresholds.is_empty());
        assert!(
            thresholds
                .iter()
                .all(|threshold| *threshold == thresholds[0])
        );
        // Retain the full-window calculation as an independent numerical
        // check of the selective worker summary path.
        let mut peak = 0.0_f64;
        let mut sidelobe = 0.0_f64;
        for channel in normal.slab().core_range() {
            let window = normal.read_window(channel..channel + 1).unwrap();
            let plane = window.polarization_plane(0, 0).unwrap();
            if plane.validity() != SpectralChannelValidity::Valid {
                continue;
            }
            let psf = plane
                .normal_approximation()
                .iter()
                .map(|v| v.re as f32)
                .collect::<Vec<_>>();
            let psf_peak = psf
                .iter()
                .map(|v| f64::from(v.abs()))
                .fold(0.0_f64, f64::max);
            peak = peak.max(
                plane
                    .residual()
                    .iter()
                    .map(|v| v.re.abs() / psf_peak)
                    .fold(0.0_f64, f64::max),
            );
            sidelobe = sidelobe.max(
                casa_imaging_reconstruction::fitted_psf_sidelobe_fraction(&psf, plane.shape())
                    .unwrap(),
            );
        }
        assert_eq!(
            thresholds[0].to_bits(),
            (peak * sidelobe.clamp(0.05, 0.8)).to_bits()
        );

        // CASA derives the shared control from peaks inside the CLEAN mask,
        // not a brighter excluded source. Exercise the actual prepared path.
        let shape = normal.shape();
        let mut pixel_peaks = vec![0.0_f64; shape[0] * shape[1]];
        for channel in normal.slab().core_range() {
            let window = normal.read_window(channel..channel + 1).unwrap();
            let plane = window.polarization_plane(0, 0).unwrap();
            if plane.validity() != SpectralChannelValidity::Valid {
                continue;
            }
            let normalization = plane
                .normal_approximation()
                .iter()
                .map(|value| f64::from((value.re as f32).abs()))
                .fold(0.0_f64, f64::max);
            for (peak, value) in pixel_peaks.iter_mut().zip(plane.residual().iter()) {
                *peak = peak.max(value.re.abs() / normalization);
            }
        }
        let (index, masked_peak) = pixel_peaks
            .iter()
            .enumerate()
            .min_by(|left, right| left.1.total_cmp(right.1))
            .unwrap();
        assert!(
            *masked_peak < 0.99 * peak,
            "fixture must exclude a brighter pixel"
        );
        let pixel = [index / shape[1], index % shape[1]];
        let restricted = ReconstructionMask::from_boxes(
            problem.problem_id(),
            base.generation_id(),
            problem.geometry().domains()[0].direction(),
            shape,
            [casa_imaging_reconstruction::MaskBox::new(pixel, pixel).unwrap()],
        )
        .unwrap();
        let masked = cycle.run(&lifecycle, base, &normal, &restricted).unwrap();
        for channel in masked.evidence().channels() {
            if let Some(minor) = channel.minor_cycle() {
                assert_eq!(
                    minor.cycle_threshold().unwrap().to_bits(),
                    (masked_peak * sidelobe.clamp(0.05, 0.8)).to_bits(),
                    "cube threshold must exclude pixels outside the CLEAN mask"
                );
            }
        }

        for workers in [1, 2, 3] {
            let mut work = cycle
                .prepare_independent(&lifecycle, base, &normal, &mask)
                .expect("prepared planes");
            assert_eq!(work.plane_count(), 3);
            let planned_workspace = casa_imaging_reconstruction::runtime_adapter::ReconstructionPlaneWorkspace::for_problem(&problem)
                .expect("compiled workspace").expect("independent cube workspace");
            let actual_workspace = work.workspace();
            assert_eq!(planned_workspace.plane_count(), work.plane_count());
            assert!(actual_workspace.worker_bytes() <= planned_workspace.worker_bytes());
            assert!(actual_workspace.retained_bytes() <= planned_workspace.retained_bytes());
            assert!(matches!(
                work.execute_plane(&work.prepare_plane(0).unwrap(), 1),
                Err(casa_imaging_reconstruction::ReconstructionCycleError::InvalidPlaneCoverage)
            ));
            for start in (0..work.threshold_plane_count()).step_by(workers) {
                let end = (start + workers).min(work.threshold_plane_count());
                let statistics = std::thread::scope(|scope| {
                    let work = &work;
                    (start..end)
                        .rev()
                        .map(|ordinal| scope.spawn(move || work.plane_statistics(ordinal)))
                        .collect::<Vec<_>>()
                        .into_iter()
                        .map(|worker| worker.join().unwrap().unwrap())
                        .collect::<Vec<_>>()
                });
                for statistics in statistics.into_iter().rev() {
                    work.commit_statistics(statistics).unwrap();
                }
            }
            for start in (0..work.plane_count()).step_by(workers) {
                let end = (start + workers).min(work.plane_count());
                let inputs = (start..end)
                    .rev()
                    .map(|ordinal| work.prepare_plane(ordinal).expect("load model window"))
                    .collect::<Vec<_>>();
                let partials = std::thread::scope(|scope| {
                    let work = &work;
                    inputs
                        .into_iter()
                        .map(|input| scope.spawn(move || work.execute_plane(&input, 1)))
                        .collect::<Vec<_>>()
                        .into_iter()
                        .map(|worker| worker.join().expect("plane worker").expect("plane solve"))
                        .collect::<Vec<_>>()
                });
                for partial in partials.into_iter().rev() {
                    assert!(partial.owned_bytes() <= actual_workspace.worker_bytes());
                    work.commit_plane(partial).expect("canonical plane commit");
                }
            }
            let parallel = work.finish().expect("complete cube coverage");
            assert_eq!(
                parallel.evidence().evidence_id(),
                serial.evidence().evidence_id()
            );
            assert_eq!(
                parallel.evidence().iterations(),
                serial.evidence().iterations()
            );
            if let (Some(parallel_delta), Some(serial_delta)) = (parallel.delta(), serial.delta()) {
                assert_ne!(parallel_delta.delta_id(), serial_delta.delta_id());
            }
            assert_eq!(
                parallel.delta().map(ModelDelta::terms),
                serial.delta().map(ModelDelta::terms)
            );
        }

        use casa_imaging_reconstruction::ReconstructionCycleError::InvalidPlaneCoverage;
        let mut work = cycle
            .prepare_independent(&lifecycle, base, &normal, &mask)
            .expect("prepared planes");
        assert!(matches!(work.prepare_plane(3), Err(InvalidPlaneCoverage)));
        let out_of_order = work.plane_statistics(1).unwrap();
        assert!(matches!(
            work.commit_statistics(out_of_order),
            Err(InvalidPlaneCoverage)
        ));
        let first = work.plane_statistics(0).unwrap();
        let duplicate = work.plane_statistics(0).unwrap();
        work.commit_statistics(first).unwrap();
        assert!(matches!(
            work.commit_statistics(duplicate),
            Err(InvalidPlaneCoverage)
        ));
        for ordinal in 1..work.threshold_plane_count() {
            let statistics = work.plane_statistics(ordinal).unwrap();
            work.commit_statistics(statistics).unwrap();
        }
        let out_of_order = work
            .execute_plane(&work.prepare_plane(1).unwrap(), 1)
            .expect("second plane");
        assert!(matches!(
            work.commit_plane(out_of_order),
            Err(InvalidPlaneCoverage)
        ));
        let other_cycle = cycle.clone();
        let mut other_work = other_cycle
            .prepare_independent(&lifecycle, base, &normal, &mask)
            .expect("different prepared inputs");
        let foreign_statistics = other_work.plane_statistics(0).unwrap();
        assert!(matches!(
            work.commit_statistics(foreign_statistics),
            Err(InvalidPlaneCoverage)
        ));
        for ordinal in 0..other_work.threshold_plane_count() {
            let statistics = other_work.plane_statistics(ordinal).unwrap();
            other_work.commit_statistics(statistics).unwrap();
        }
        let foreign_input = other_work.prepare_plane(0).unwrap();
        assert!(matches!(
            work.execute_plane(&foreign_input, 1),
            Err(InvalidPlaneCoverage)
        ));
        let foreign = other_work
            .execute_plane(&foreign_input, 1)
            .expect("foreign partial");
        assert!(matches!(
            work.commit_plane(foreign),
            Err(InvalidPlaneCoverage)
        ));
        let input = work.prepare_plane(0).unwrap();
        let first = work.execute_plane(&input, 1).expect("first plane");
        let duplicate = work
            .execute_plane(&input, 1)
            .expect("duplicate first plane");
        work.commit_plane(first).expect("first commit");
        assert!(matches!(
            work.commit_plane(duplicate),
            Err(InvalidPlaneCoverage)
        ));
        assert!(matches!(work.finish(), Err(InvalidPlaneCoverage)));
    }
}

#[test]
fn t38_late_nsigma_floor_and_first_component_divergence_remain_per_channel() {
    let problem = t38_cube_problem(242);
    let (lifecycle, normal, continuation) = initial_round(&problem, &scene(&problem), 243);
    let mask = ReconstructionMask::full_plane(
        problem.problem_id(),
        continuation.generation().generation_id(),
        problem.geometry().domains()[0].direction(),
        normal.shape(),
    )
    .expect("shared cube mask");
    let baseline_program = MinorCycleProgram::for_algorithm(
        ReconstructionAlgorithm::Hogbom,
        problem.reconstruction().controls(),
    )
    .expect("compiled nsigma floor")
    .record_component_sequence(16)
    .expect("bounded sequence");
    let candidate_program = MinorCycleProgram::new(0.25, 0.5, 8)
        .expect("candidate controls")
        .record_component_sequence(16)
        .expect("bounded sequence");
    let baseline = ReconstructionCycle::new(ChannelCyclePolicy::Independent, baseline_program)
        .run(&lifecycle, continuation.generation(), &normal, &mask)
        .expect("baseline cube cycle");
    let candidate = ReconstructionCycle::new(ChannelCyclePolicy::Independent, candidate_program)
        .run(&lifecycle, continuation.generation(), &normal, &mask)
        .expect("candidate cube cycle");

    assert!(baseline.evidence().channels().iter().all(|channel| {
        channel
            .minor_cycle()
            .is_some_and(|evidence| evidence.noise_rms().is_some())
    }));
    let divergence = candidate
        .evidence()
        .first_divergence(baseline.evidence())
        .expect("gain change diverges at the first accepted component");
    assert_eq!(divergence.output_channel(), 0);
    assert_eq!(divergence.component().index(), 0);
}

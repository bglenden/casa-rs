// SPDX-License-Identifier: LGPL-3.0-or-later

//! Major cycles pairing complete-data normal states with their final models,
//! driven entirely through owner seams. Normal states come from synthetic
//! passes over a known sky (`support/synthetic_pass.rs`), appended to a
//! `MajorCycle` exactly as the runtime's major-cycle pass appends them.
//!
//! This root holds the problem shorthands, the sky and the normal-state
//! content comparison; the compiled problems come from `support/problems.rs`
//! (shared with the runtime's minor-cycle tests), and the tests live in
//! `major_cycle/reconciliation.rs` (completions, refreshes and coverage).

use casa_imaging_model::{
    ModelCell, ModelDeltaTerm, ReconstructionAlgorithm, ReconstructionBasis, ReconstructionControls,
};
use casa_imaging_reconstruction::{
    FinalNormalState, MajorCycle, MajorCycleError, SpectralChannelValidity, SpectralOperatorError,
    runtime_adapter::NormalStoragePlan,
};
use num_complex::Complex64;

#[path = "support/synthetic_pass.rs"]
mod synthetic_pass;
use synthetic_pass::{BLOCKS, SAMPLES, Scene};

#[path = "support/problems.rs"]
mod problems;
use problems::{
    empty_final_model, model_lifecycle, reconstruction_problem, reconstruction_problem_with_domains,
};

#[path = "major_cycle/reconciliation.rs"]
mod reconciliation;

fn t19_compatible_problem(observation: u8) -> casa_imaging_model::CompiledProblem {
    reconstruction_problem(
        observation,
        8,
        1,
        ReconstructionBasis::Constant,
        ReconstructionAlgorithm::Dirty,
        ReconstructionControls::new(0, 1.0, 0.0),
    )
}

/// The constant-basis problem of `t19_compatible_problem` with one outlier
/// field beside the main field.
fn two_domain_problem(observation: u8) -> casa_imaging_model::CompiledProblem {
    reconstruction_problem_with_domains(
        observation,
        8,
        1,
        2,
        ReconstructionBasis::Constant,
        ReconstructionAlgorithm::Dirty,
        ReconstructionControls::new(0, 1.0, 0.0),
    )
}

fn t38_cube_problem(observation: u8) -> casa_imaging_model::CompiledProblem {
    t38_cube_problem_with_channels(observation, 2)
}

fn t38_cube_problem_with_channels(
    observation: u8,
    channels: usize,
) -> casa_imaging_model::CompiledProblem {
    t38_cube_problem_with_controls(
        observation,
        channels,
        ReconstructionControls::new(8, 0.5, 0.0).with_noise_sigma(0.0),
    )
}

fn t38_cube_problem_with_controls(
    observation: u8,
    channels: usize,
    controls: ReconstructionControls,
) -> casa_imaging_model::CompiledProblem {
    reconstruction_problem(
        observation,
        8,
        channels,
        ReconstructionBasis::ChannelLocal { channels },
        ReconstructionAlgorithm::Hogbom,
        controls,
    )
}

/// The fixed sky every pass images: an off-diagonal point source, brighter in
/// each later channel, over a low residual field. The two-row observation
/// reaches only output channels 0 and 1, so later channels carry no weight.
fn scene(problem: &casa_imaging_model::CompiledProblem) -> Scene {
    let ReconstructionBasis::ChannelLocal { channels } = problem.reconstruction().basis() else {
        return Scene::new(problem)
            .with_point([5, 3], &[1.3])
            .with_noise(0.01);
    };
    let amplitudes = (0..channels)
        .map(|channel| 1.3 + 0.8 * channel as f64)
        .collect::<Vec<_>>();
    Scene::new(problem)
        .with_point([5, 3], &amplitudes)
        .with_weights(
            (0..channels)
                .map(|channel| if channel < 2 { 1.0 } else { 0.0 })
                .collect(),
        )
        .with_noise(0.01)
}

/// Every value one image domain of a final normal state holds.
#[derive(Debug, PartialEq)]
struct DomainContent {
    residual: Vec<Complex64>,
    normal_approximation: Vec<Complex64>,
    sensitivity: Vec<f64>,
    sum_weights: Vec<f64>,
    published_sum_weights: Vec<f64>,
    validity: Vec<SpectralChannelValidity>,
}

/// The content of every image domain of `state`, read over all its
/// channels.
fn normal_content(state: &FinalNormalState) -> Vec<DomainContent> {
    let window = state
        .read_window(0..state.channel_count())
        .expect("read the whole normal state");
    window
        .domains()
        .map(|domain| DomainContent {
            residual: domain.residual().iter().collect(),
            normal_approximation: domain.normal_approximation().iter().collect(),
            sensitivity: domain.sensitivity().iter().collect(),
            sum_weights: domain.sum_weights().to_vec(),
            published_sum_weights: domain.published_sum_weights().to_vec(),
            validity: domain.channel_validity().to_vec(),
        })
        .collect()
}

/// Content of an independent initial pass of `scene` over the empty model:
/// the data side of every major cycle of the same sky.
fn confirm_content(
    problem: &casa_imaging_model::CompiledProblem,
    scene: &Scene,
) -> Vec<DomainContent> {
    let lifecycle = model_lifecycle(problem);
    normal_content(
        scene
            .initial(problem, empty_final_model(&lifecycle))
            .normal_state(),
    )
}

fn cell(x: usize) -> ModelCell {
    ModelCell::new(0, 0, 0, [x, 0])
}

fn delta_value(value: f64) -> casa_imaging_model::ModelValue {
    casa_imaging_model::ModelValue::new(value).expect("finite model value")
}

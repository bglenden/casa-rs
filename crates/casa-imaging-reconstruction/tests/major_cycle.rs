// SPDX-License-Identifier: LGPL-3.0-or-later

//! Major-Cycle reconciliation of complete-data normal states with the model
//! lifecycle, driven entirely through owner seams. Normal states come from
//! synthetic passes over a known sky (`support/synthetic_pass.rs`), assembled
//! through `PassNormalState` exactly as the runtime's major-cycle pass
//! assembles them.
//!
//! This root holds the problem shorthands and the sky; the compiled problems
//! come from `support/problems.rs` (shared with the runtime's minor-cycle
//! tests), and the tests live in `major_cycle/reconciliation.rs`
//! (reconciliation, lineage and refreshes).

use casa_imaging_model::{
    LogicalIdentity, ModelCell, ModelDeltaTerm, ReconstructionAlgorithm, ReconstructionBasis,
    ReconstructionControls,
};
use casa_imaging_reconstruction::{
    MajorCycleError, MajorCycleOwner, MajorCyclePreparation, ModelLifecycle, ModelLifecycleError,
    PassNormalState, SpectralOperatorError, WeightingGenerationId,
    runtime_adapter::NormalStoragePlan,
};

#[path = "support/synthetic_pass.rs"]
mod synthetic_pass;
use synthetic_pass::Scene;

#[path = "support/problems.rs"]
mod problems;
use problems::{attempt, bind_lifecycle, identity, reconstruction_problem};

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

/// Content of an independent initial pass of `scene` over the empty model:
/// the data side of every reconciliation of the same sky.
fn confirm_content(
    problem: &casa_imaging_model::CompiledProblem,
    scene: &Scene,
) -> LogicalIdentity {
    let lifecycle = bind_lifecycle(problem, attempt(0xf0));
    let named = lifecycle.initial_empty().expect("confirm empty generation");
    let preparation =
        MajorCyclePreparation::prepare(&lifecycle, named, None).expect("confirm preparation");
    scene
        .initial(problem, &preparation)
        .diagnostic_content_identity()
        .expect("confirm content")
}

fn cell(x: usize) -> ModelCell {
    ModelCell::new(0, 0, 0, [x, 0])
}

fn delta_value(value: f64) -> casa_imaging_model::ModelValue {
    casa_imaging_model::ModelValue::new(value).expect("finite model value")
}

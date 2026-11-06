// SPDX-License-Identifier: LGPL-3.0-or-later

//! MVC's admitted cube-to-Taylor normal-state handoff (#1069).

use casa_imaging_deconvolution::{
    MajorCycle, MajorCycleCompletion, ModelLifecycle, ModelStoragePlan, NormalStoragePlan,
    normal_state_window_residency_bytes, resident_bytes_per_cell,
};
use casa_imaging_model::{CompiledProblem, ModelSample, ReconstructionBasis};
use casa_imaging_products::{
    ContinuumProductControls, ContinuumProductInputs, PlannedContinuumGeneration,
    PrimaryBeamCentres, ProductsError, encode_mvc_blank_taylor_planes, encode_mvc_taylor_planes,
    reduce_mvc_cube,
};
use casa_imaging_runtime::pass::Cancel;
use casa_imaging_runtime::{Admitted, Cancelled, Demand, HostResources, ResourcePolicy, admit};

use crate::native::NativeError;

/// Inputs to the Taylor conversion, all bound to this run's two views.
pub(crate) struct TaylorAssemblyInputs<'a> {
    pub(crate) cube_problem: &'a CompiledProblem,
    pub(crate) cube: &'a MajorCycleCompletion,
    pub(crate) taylor_problem: &'a CompiledProblem,
    pub(crate) expansion_reference_hz: f64,
    pub(crate) controls: ContinuumProductControls,
    pub(crate) centres: Option<&'a PrimaryBeamCentres>,
}

/// The Taylor state and common PB, kept under the returned admission.
#[derive(Debug)]
pub(crate) struct TaylorCompletion {
    pub(crate) scientific: MajorCycleCompletion,
    pub(crate) primary_beam: Vec<f32>,
}

/// Complete one Taylor normal state from the cube. Admission covers the
/// reduction's six output planes and one channel of workspace, and the
/// overlap of those planes, the widened Taylor state and its initial model.
pub(crate) fn assemble_taylor_completion(
    inputs: TaylorAssemblyInputs<'_>,
    (host, policy, cancel): (&HostResources, &ResourcePolicy, &Cancel),
) -> Result<Admitted<TaylorCompletion>, NativeError> {
    if cancel.is_cancelled() {
        return Err(Cancelled.into());
    }
    let TaylorAssemblyInputs {
        cube_problem,
        cube,
        taylor_problem,
        expansion_reference_hz,
        controls,
        centres,
    } = inputs;
    if cube_problem.observation() != taylor_problem.observation() {
        return Err(NativeError::Other(
            std::io::Error::other("MVC cube and Taylor views must use the same observation").into(),
        ));
    }
    let shape = cube.normal_state().shape();
    let domains = taylor_problem.geometry().domains();
    if domains.len() != 1
        || domains[0].shape().pixels() != shape
        || taylor_problem.reconstruction().basis() != (ReconstructionBasis::Taylor { terms: 2 })
        || taylor_problem
            .reconstruction()
            .polarization()
            .coordinates()
            .len()
            != 1
    {
        return Err(NativeError::Other(
            std::io::Error::other("MVC needs matching single-Stokes two-term views").into(),
        ));
    }
    let cells = shape[0]
        .checked_mul(shape[1])
        .ok_or(ProductsError::ResourceDemandOverflow("MVC cells"))?;
    let samples = taylor_problem.model_lifecycle().target().sample_count();
    let memory = taylor_demand(shape, samples)?;
    let reservation = admit(
        host,
        policy,
        &Demand {
            phase: "MVC Taylor conversion",
            memory,
        },
    )?;
    let mut mean = vec![0.0; cells];
    let mut psf = vec![
        0.0;
        cells
            .checked_mul(3)
            .ok_or(ProductsError::ResourceDemandOverflow("MVC PSF"))?
    ];
    let mut residual = vec![
        0.0;
        cells
            .checked_mul(2)
            .ok_or(ProductsError::ResourceDemandOverflow("MVC residual"))?
    ];
    let mut sumwt = [0.0; 3];
    let mut cube_inputs = ContinuumProductInputs::from_major_cycle(cube_problem, cube);
    if let Some(centres) = centres {
        cube_inputs = cube_inputs.with_primary_beam_centres(centres);
    }
    let plan = PlannedContinuumGeneration::new(&cube_inputs, &controls)?;
    reduce_mvc_cube(
        &plan,
        &cube_inputs,
        expansion_reference_hz,
        &mut mean,
        &mut psf,
        &mut residual,
        &mut sumwt,
    )?;
    if cancel.is_cancelled() {
        return Err(Cancelled.into());
    }
    let images = if sumwt[0] == 0.0
        && taylor_problem.products().restoring_beam()
            == casa_imaging_model::RestoringBeamPolicy::None
    {
        encode_mvc_blank_taylor_planes(shape, psf, residual, sumwt)?
    } else {
        encode_mvc_taylor_planes(shape, psf, residual, sumwt)?
    };
    // Taylor product publication reads a whole coefficient plane at once.
    // Initialization itself remains chunked to at most 1 << 16 samples.
    let model_storage = ModelStoragePlan::resident(cells).map_err(other)?;
    let lifecycle = ModelLifecycle::new(taylor_problem, model_storage);
    let model = lifecycle
        .prepare_final_model(lifecycle.initial_empty().map_err(other)?, [])
        .map_err(other)?;
    let storage = NormalStoragePlan::resident(1).map_err(other)?;
    let mut cycle = MajorCycle::initial(taylor_problem, model, storage).map_err(other)?;
    cycle.parts().1.append(images).map_err(other)?;
    let scientific = cycle.finish_read().map_err(other)?;
    if cancel.is_cancelled() {
        return Err(Cancelled.into());
    }
    Ok(Admitted {
        value: TaylorCompletion {
            scientific,
            primary_beam: mean,
        },
        reservation,
    })
}

/// A conservative bound for a two-term one-Stokes conversion's live heap.
fn taylor_demand(shape: [usize; 2], model_samples: usize) -> Result<u64, NativeError> {
    let overflow = || ProductsError::ResourceDemandOverflow("MVC Taylor conversion");
    let cells = u64::try_from(shape[0].checked_mul(shape[1]).ok_or_else(overflow)?)
        .map_err(|_| overflow())?;
    let bytes = |per_cell: u64| cells.checked_mul(per_cell).ok_or_else(overflow);
    let output = bytes(6 * size_of::<f32>() as u64)?;
    let cube_window = normal_state_window_residency_bytes(shape, 1, 1).map_err(other)?;
    let reduction = output
        .checked_add(bytes(3 * size_of::<f32>() as u64)?)
        .and_then(|n| n.checked_add(cube_window))
        .ok_or_else(overflow)?;
    let model = u64::try_from(model_samples)
        .map_err(|_| overflow())?
        .checked_mul(size_of::<ModelSample>() as u64)
        .ok_or_else(overflow)?;
    let initial_window = u64::try_from(model_samples.min(1 << 16))
        .map_err(|_| overflow())?
        .checked_mul(size_of::<ModelSample>() as u64)
        .ok_or_else(overflow)?;
    let state = bytes(resident_bytes_per_cell(2, 3, true))?;
    let assembly = output
        .checked_add(state)
        .and_then(|n| n.checked_add(model))
        .and_then(|n| n.checked_add(initial_window))
        .ok_or_else(overflow)?;
    Ok(reduction
        .max(assembly)
        .checked_add(64 * 1024)
        .ok_or_else(overflow)?)
}

fn other(error: impl std::error::Error + Send + Sync + 'static) -> NativeError {
    NativeError::Other(Box::new(error))
}

/// The Taylor model and two primary beams for one cube channel (#1069).
/// Every slice is one equally sized, x-major plane; the caller owns and
/// admits both input planes, both beams and the output window.
#[cfg_attr(
    not(test),
    allow(dead_code, reason = "MVC cleaning handoff is in #1069")
)]
pub(crate) struct MvcModelChannel<'a> {
    pub(crate) terms: [&'a [f32]; 2],
    pub(crate) channel_pb: &'a [f32],
    pub(crate) mean_pb: &'a [f32],
    pub(crate) channel_hz: f64,
    pub(crate) reference_hz: f64,
    pub(crate) cutoff: f32,
}

impl MvcModelChannel<'_> {
    /// Evaluate CASA's Float Taylor model and its frequency-dependent PB
    /// adjustment into one caller-owned cube plane.
    #[cfg_attr(
        not(test),
        allow(dead_code, reason = "MVC cleaning handoff is in #1069")
    )]
    pub(crate) fn evaluate(&self, output: &mut [f32]) -> Result<(), NativeError> {
        let invalid = || other(std::io::Error::other("invalid MVC model channel"));
        let cells = output.len();
        if cells == 0
            || self.terms.iter().any(|term| term.len() != cells)
            || self.channel_pb.len() != cells
            || self.mean_pb.len() != cells
            || !self.channel_hz.is_finite()
            || self.channel_hz <= 0.0
            || !self.reference_hz.is_finite()
            || self.reference_hz <= 0.0
            || !self.cutoff.is_finite()
            || self.cutoff < 0.0
            || self
                .terms
                .iter()
                .copied()
                .flatten()
                .any(|value| !value.is_finite())
            || self
                .channel_pb
                .iter()
                .chain(self.mean_pb)
                .any(|value| !value.is_finite() || *value < 0.0)
        {
            return Err(invalid());
        }
        let channel_max = self.channel_pb.iter().copied().fold(0.0_f32, f32::max);
        let mean_max = self.mean_pb.iter().copied().fold(0.0_f32, f32::max);
        let offset = (self.channel_hz - self.reference_hz) / self.reference_hz;
        for (index, value) in output.iter_mut().enumerate() {
            let model =
                (f64::from(self.terms[0][index]) + offset * f64::from(self.terms[1][index])) as f32;
            *value = if channel_max <= 0.0 || mean_max <= 0.0 {
                model
            } else if self.channel_pb[index] > self.cutoff {
                model * self.channel_pb[index] / self.mean_pb[index]
            } else {
                0.0
            };
            if !value.is_finite() {
                return Err(invalid());
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod model_channel_tests {
    use super::MvcModelChannel;

    /// Values captured from CASA's real three-channel clean MVC model at
    /// the source pixel after the first and third minor cycles (#1069).
    #[test]
    fn taylor_model_projects_to_casas_frequency_dependent_cube_model() {
        let frequencies = [44.0e9, 44.128e9, 44.256e9];
        let beams = [0.962_401_7_f32, 0.962_034_17, 0.961_665];
        for (terms, expected) in [
            (
                [0.481_774_9_f32, -0.441_601_57],
                [0.491_723_6_f32, 0.490_279_85, 0.488_836_23],
            ),
            (
                [1.305_975_3_f32, -1.178_515_7],
                [1.332_531_3_f32, 1.328_670_6, 1.324_810_1],
            ),
        ] {
            for channel in 0..3 {
                let mut output = [0.0];
                MvcModelChannel {
                    terms: [&terms[0..1], &terms[1..2]],
                    channel_pb: &beams[channel..channel + 1],
                    mean_pb: &[0.962_136_8],
                    channel_hz: frequencies[channel],
                    reference_hz: 45.0e9,
                    cutoff: 0.1,
                }
                .evaluate(&mut output)
                .unwrap();
                assert!((output[0] - expected[channel]).abs() < 1.0e-6);
            }
        }
    }

    /// CASA promotes the Float term into the Double Taylor expression and
    /// rounds only the complete sum back to Float (#1069).
    #[test]
    fn model_taylor_sum_rounds_after_adding_the_double_product() {
        let mut output = [0.0];
        MvcModelChannel {
            terms: [&[1.0], &[-1.0]],
            channel_pb: &[1.0],
            mean_pb: &[1.0],
            channel_hz: 2.0 + 2.0_f64.powi(-25),
            reference_hz: 1.0,
            cutoff: 0.1,
        }
        .evaluate(&mut output)
        .unwrap();
        assert_eq!(output[0], -2.0_f32.powi(-25));
    }

    /// CASA preserves a polynomial plane when either whole PB is blank;
    /// otherwise its per-pixel cutoff is strictly greater than pblimit.
    #[test]
    fn model_pb_blank_planes_and_cutoff_follow_casa() {
        let evaluate = |pb: &[f32], mean: &[f32], output: &mut [f32]| {
            MvcModelChannel {
                terms: [&[2.0, 3.0], &[1.0, 1.0]],
                channel_pb: pb,
                mean_pb: mean,
                channel_hz: 110.0,
                reference_hz: 100.0,
                cutoff: 0.1,
            }
            .evaluate(output)
            .unwrap();
        };
        let mut output = [0.0; 2];
        evaluate(&[0.0, 0.0], &[1.0, 1.0], &mut output);
        assert_eq!(output, [2.1, 3.1]);
        evaluate(&[0.5, 0.5], &[0.0, 0.0], &mut output);
        assert_eq!(output, [2.1, 3.1]);
        evaluate(&[0.1, 0.5], &[0.5, 0.25], &mut output);
        assert_eq!(output, [0.0, 6.2]);
    }

    #[test]
    fn model_projection_rejects_invalid_shapes_and_nonfinite_values() {
        let mut output = [0.0; 2];
        let mut input = MvcModelChannel {
            terms: [&[1.0, 2.0], &[1.0, 2.0]],
            channel_pb: &[0.5, 0.5],
            mean_pb: &[0.5, 0.5],
            channel_hz: 100.0,
            reference_hz: 100.0,
            cutoff: 0.1,
        };
        input.channel_pb = &[0.5];
        assert!(input.evaluate(&mut output).is_err());
        input.channel_pb = &[0.5, 0.5];
        input.mean_pb = &[0.5];
        assert!(input.evaluate(&mut output).is_err());
        input.mean_pb = &[0.5, 0.5];
        input.terms = [&[1.0], &[1.0, 2.0]];
        assert!(input.evaluate(&mut output).is_err());
        input.terms = [&[1.0, 2.0], &[1.0, 2.0]];
        input.channel_pb = &[0.5, f32::NAN];
        assert!(input.evaluate(&mut output).is_err());
        input.channel_pb = &[0.5, 0.5];
        input.mean_pb = &[0.0, 0.5];
        assert!(input.evaluate(&mut output).is_err());
        input.mean_pb = &[0.5, 0.5];
        input.reference_hz = 0.0;
        output = [17.0; 2];
        assert!(input.evaluate(&mut output).is_err());
        assert_eq!(output, [17.0; 2]);
        input.reference_hz = 100.0;
        input.channel_hz = 0.0;
        assert!(input.evaluate(&mut output).is_err());
        input.channel_hz = 100.0;
        input.cutoff = -0.1;
        assert!(input.evaluate(&mut output).is_err());
        input.cutoff = 0.1;
        input.channel_pb = &[-0.5, 0.5];
        assert!(input.evaluate(&mut output).is_err());
        input.channel_pb = &[0.5, 0.5];
        input.terms = [&[1.0, f32::NAN], &[1.0, 2.0]];
        output = [17.0; 2];
        assert!(input.evaluate(&mut output).is_err());
        assert_eq!(output, [17.0; 2]);
    }
}

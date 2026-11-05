// SPDX-License-Identifier: LGPL-3.0-or-later
//! Separate visibility traversals after the final tuned major cycle.

use casa_imaging_runtime::pass::{BlockPredictor, NativeBlock, PredictionDomain, SampleContent};
use num_complex::Complex32;

use super::{
    Admitted, ImagingError, ModeSet, ModelGeneration, PassError, PassPlan, PlaneRange, Residency,
    Run, prepare_model,
};

/// Persist continuum-corrected values on the tuned fit/output selection.
pub(super) fn write_corrected(run: &mut Run<'_>) -> Result<u64, ImagingError> {
    let target = run.visibility_write.expect("a split write has a target");
    let mut writer = target
        .begin_corrected(run.problem.visibility_transform())
        .map_err(|error| ImagingError::Pass(PassError::VisibilityWrite(error)))?;
    let planes = run.domains[0].operator.basis().planes();
    run.source
        .begin(
            PlaneRange::new(0, planes),
            false,
            SampleContent::Visibilities,
        )
        .map_err(|error| ImagingError::Pass(PassError::Source(error)))?;
    let mut block = NativeBlock::with_capacity(run.source.maximum_block());
    while run
        .source
        .fill(&mut block)
        .map_err(|error| ImagingError::Pass(PassError::Source(error)))?
    {
        if run.cancel.is_cancelled() {
            return Err(ImagingError::Pass(PassError::Cancelled));
        }
        writer
            .write(&block, &[])
            .map_err(|error| ImagingError::Pass(PassError::VisibilityWrite(error)))?;
    }
    writer
        .complete()
        .map_err(|error| ImagingError::Pass(PassError::VisibilityWrite(error)))
}

/// Reserve the final predictor and writer before either split traversal
/// can change the observation. The same reservation covers the corrected
/// traversal, whose source and writer are a subset of this demand.
pub(super) fn admit_model(run: &Run<'_>) -> Result<Admitted<PassPlan>, ImagingError> {
    let team = run.teams.streaming();
    let shape = run.source.maximum_block();
    let predictor_bytes = BlockPredictor::bytes_for(
        run.domains
            .iter()
            .map(|domain| (&domain.operator, &domain.resampler)),
        run.backend,
        shape,
        team.workers(),
    );
    // The full major-wave demand bounds model preparation; predictor scratch
    // is admitted separately. The source already admits its blocks and output.
    let plan = run.admit_pass(
        ModeSet::DATA,
        true,
        run.visibility_write_bytes + predictor_bytes,
        true,
    )?;
    if plan.value.residency != Residency::All {
        return Err(ImagingError::Pass(PassError::VisibilityWriteWaves));
    }
    Ok(plan)
}

/// Predict the final model on the parent selection without changing the
/// residual or its normal state, retaining its earlier admission.
pub(super) fn write_model(
    run: &mut Run<'_>,
    model: &ModelGeneration,
    _plan: Admitted<PassPlan>,
) -> Result<u64, ImagingError> {
    let target = run.visibility_write.expect("a split write has a target");
    let team = run.teams.streaming();
    let shape = run.source.maximum_block();
    let planes = PlaneRange::new(0, run.domains[0].operator.basis().planes());
    let models = run
        .domains
        .iter()
        .zip(&run.charts)
        .map(|(domain, chart)| {
            prepare_model(
                &domain.operator,
                model,
                (chart.domain, chart.window),
                planes,
            )
        })
        .collect::<Result<Vec<_>, _>>()?;
    let domains = run
        .domains
        .iter()
        .zip(&models)
        .map(|(domain, model)| PredictionDomain {
            operator: &domain.operator,
            resampler: &domain.resampler,
            model: model.into(),
        })
        .collect::<Vec<_>>();
    let mut predictor = BlockPredictor::new(
        &domains,
        run.backend,
        shape,
        team.workers(),
        run.metal_stage_timing,
    )?;
    let mut writer = target
        .begin_prediction(run.problem.visibility_transform())
        .map_err(|error| ImagingError::Pass(PassError::VisibilityWrite(error)))?;
    run.source
        .begin(planes, false, SampleContent::Prediction)
        .map_err(|error| ImagingError::Pass(PassError::Source(error)))?;
    let mut block = NativeBlock::with_capacity(shape);
    let mut values = vec![Complex32::default(); shape.rows * shape.channels() * shape.correlations];
    while run
        .source
        .fill(&mut block)
        .map_err(|error| ImagingError::Pass(PassError::Source(error)))?
    {
        if run.cancel.is_cancelled() {
            return Err(ImagingError::Pass(PassError::Cancelled));
        }
        let cells = block.len() * block.channels() * block.correlations();
        predictor.predict_into(&block, team, &mut values[..cells])?;
        writer
            .write(&block, &values[..cells])
            .map_err(|error| ImagingError::Pass(PassError::VisibilityWrite(error)))?;
    }
    writer
        .complete()
        .map_err(|error| ImagingError::Pass(PassError::VisibilityWrite(error)))
}

// SPDX-License-Identifier: LGPL-3.0-or-later

//! MFS uses the same selected-input owner and its existing numerical kernels.

use crate::{
    SpectralOperatorState, bounded_stream::BoundedExecution, weighting::bulk_source::BulkConsumer,
};
use casa_imaging_model::{
    CompiledProblem, ModelInputCommitment, ReconstructionBasis, WeightingScheme,
};
use casa_imaging_reconstruction::{
    SpectralOperatorSpecification,
    runtime_adapter::{NativeBlockView, NativeLayout},
};
use std::io;

pub(super) fn supports(problem: &CompiledProblem) -> io::Result<bool> {
    if !matches!(
        problem.reconstruction().basis(),
        ReconstructionBasis::Constant
    ) || !matches!(
        problem.model_lifecycle().input(),
        ModelInputCommitment::Empty
    ) || problem.weighting().scheme() != WeightingScheme::Natural
        || problem.weighting().uv_taper().is_some()
        || problem.visibility_transform().is_some()
    {
        return Ok(false);
    }
    let [source] = problem.selected_observation().read_set().sources() else {
        return Ok(false);
    };
    let ([dd], [spw], [pol]) = (
        source.selection().data_descriptions(),
        source.selection().spectral_windows(),
        source.selection().correlations(),
    ) else {
        return Ok(false);
    };
    Ok(dd.spectral_window_id() == spw.spectral_window_id()
        && dd.polarization_id() == pol.polarization_id()
        && !spw.channel_indices().is_empty()
        && SpectralOperatorSpecification::new(problem)
            .map_err(io::Error::other)?
            .supports_bulk_mfs())
}

pub(super) struct MfsConsumer(pub(super) SpectralOperatorState);

impl BulkConsumer for MfsConsumer {
    type Completion = SpectralOperatorState;
    fn consume(
        &mut self,
        block: NativeBlockView<'_>,
        layout: &NativeLayout,
        _: std::ops::Range<usize>,
        _: BoundedExecution<'_>,
    ) -> io::Result<()> {
        self.0
            .consume_bulk_mfs(block, layout)
            .map_err(io::Error::other)
    }
    fn complete(self, _: BoundedExecution<'_>) -> io::Result<Self::Completion> {
        Ok(self.0)
    }
}

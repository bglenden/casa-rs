// SPDX-License-Identifier: LGPL-3.0-or-later
//! Drive the internal MVC view preparation for the allocation law (#1069).

use crate::{ImagingRequest, RunContext};

/// A diagnostic MVC request whose output depth is independent of selection.
pub struct MvcViewProbe {
    request: ImagingRequest,
}

impl MvcViewProbe {
    /// Keep the supplied request and select a three-plane MVC cube.
    #[must_use]
    pub fn new(mut request: ImagingRequest) -> Self {
        request.mvc_output_channels = std::num::NonZeroUsize::new(3);
        Self { request }
    }

    /// Prepare both views and release them, without executing or publishing.
    ///
    /// # Errors
    ///
    /// The supplied request or its selected observation cannot be prepared.
    pub fn prepare(&self, context: &RunContext) -> Result<(), String> {
        crate::compile::prepare(&self.request, context)
            .map(drop)
            .map_err(|error| error.to_string())
    }
}

/// The admitted diagnostic cube request, held until this value drops.
pub struct MvcCubeCopy(casa_imaging_runtime::Admitted<ImagingRequest>);

impl MvcCubeCopy {
    /// The memory retained for the request copy.
    #[must_use]
    pub const fn admitted_bytes(&self) -> u64 {
        self.0.reservation.memory()
    }
}

impl MvcViewProbe {
    /// Form the admitted cube request for the counting law.
    ///
    /// # Errors
    ///
    /// The cube depth is invalid or the request copy does not fit.
    pub fn copy_cube(&self, context: &RunContext) -> Result<MvcCubeCopy, String> {
        crate::compile::mvc::cube_request(&self.request, [1e9, 1.6e9], context)
            .map(MvcCubeCopy)
            .map_err(|error| error.to_string())
    }
}

/// Compute the projection frequency range from borrowed window frequencies.
#[must_use]
pub fn projection_frequency_range(windows: &[Vec<f64>]) -> [f64; 2] {
    crate::compile::projection_frequency_range(
        windows.iter().flat_map(|window| window.iter().copied()),
    )
}

/// Whether a continuum spectral selection borrows its existing window lists.
#[must_use]
pub fn selected_channels_are_borrowed(
    windows: &[std::collections::BTreeMap<usize, Vec<usize>>],
) -> bool {
    let selected = crate::compile::spectral::continuum_channels(windows);
    matches!(&selected, std::borrow::Cow::Borrowed(_))
        && std::ptr::eq(selected.as_ptr(), windows.as_ptr())
}

impl MvcViewProbe {
    /// Prepare both views while calling hooks immediately around the Taylor view.
    ///
    /// # Errors
    ///
    /// The request or observation cannot be prepared.
    pub fn prepare_taylor_with_hooks(
        &self,
        context: &RunContext,
        before: &dyn Fn(),
        after: &dyn Fn(),
    ) -> Result<(), String> {
        crate::compile::prepare_with_view_hooks(
            &self.request,
            context,
            Some(crate::compile::ViewHooks { before, after }),
        )
        .map(drop)
        .map_err(|error| error.to_string())
    }

    /// Count the run's user-mask boxes after preparing the MVC views.
    ///
    /// # Errors
    ///
    /// The request or observation cannot be prepared.
    pub fn run_mask_box_count(&self, context: &RunContext) -> Result<Option<usize>, String> {
        let prepared =
            crate::compile::prepare(&self.request, context).map_err(|error| error.to_string())?;
        Ok(match prepared.masks.primary() {
            casa_imaging_deconvolution::ReconstructionMaskPlan::Boxes { boxes, .. } => {
                Some(boxes.len())
            }
            _ => None,
        })
    }
}

impl MvcViewProbe {
    /// Prepare and compile the Taylor view with diagnostic hooks bracketing
    /// each phase. The second hook receives the view's held reservation.
    ///
    /// # Errors
    ///
    /// The request, observation, or either view cannot be compiled.
    pub fn compile_taylor_with_hooks(
        &self,
        context: &RunContext,
        before_prepare: &dyn Fn(),
        after_prepare: &dyn Fn(),
        before_compile: &dyn Fn(u64),
        after_compile: &dyn Fn(),
    ) -> Result<(), String> {
        let prepared = crate::compile::prepare_with_view_hooks(
            &self.request,
            context,
            Some(crate::compile::ViewHooks {
                before: before_prepare,
                after: after_prepare,
            }),
        )
        .map_err(|error| error.to_string())?;
        let view = prepared.mvc_taylor.ok_or("missing Taylor view")?;
        let observation = casa_imaging_model::compile_observation(prepared.snapshot)
            .map_err(|error| error.to_string())?;
        let cube = casa_imaging_model::compile(casa_imaging_model::ProblemInput::new(
            prepared.specification,
            prepared.geometry,
            observation,
            prepared.model_lifecycle,
        ))
        .map_err(|error| error.to_string())?;
        drop(prepared.survey);
        before_compile(view.reservation.memory());
        let compiled = view.compile_from_cube(&cube);
        after_compile();
        compiled.map(drop).map_err(|error| error.to_string())
    }
}

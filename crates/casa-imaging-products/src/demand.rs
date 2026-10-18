// SPDX-License-Identifier: LGPL-3.0-or-later

//! Checked array and metadata residency owned by continuum product generation.

use std::mem::size_of;

use casa_imaging_deconvolution::NormalBasis;
use casa_imaging_model::{ProductBeamRule, ProductRole, ProductValidityRule, RestoringBeamPolicy};
use num_complex::Complex64;

use crate::{
    CasaImageWriter, ContinuumProductInputs, PlannedContinuumGeneration, ProductStoragePlan,
    ProductsError, PublishedContinuumGeneration, PublishedMember, RestoringBeam,
};

/// Checked owned-array and metadata demand for one planned continuum generation.
///
/// It describes the products' payload, validity and working arrays, and what
/// the image writer holds, so the run admits the whole phase once before
/// production starts. The generated summary's owned metadata and temporary
/// beam arrays are included; allocator bookkeeping, the borrowed plan and
/// borrowed reconstruction inputs are not charged here.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ContinuumGenerationDemand {
    storage_plan: ProductStoragePlan,
    algorithm_scratch_bytes: u64,
    retained_metadata_bytes: u64,
    beam_scratch_bytes: u64,
    writer_bytes: u64,
    maximum_member_payload_bytes: u64,
    maximum_member_validity_bytes: u64,
    maximum_window_payload_bytes: u64,
    maximum_window_validity_bytes: u64,
    peak_residency_bytes: u64,
}

impl ContinuumGenerationDemand {
    /// Explicit selected window bound used by this generation.
    #[must_use]
    pub const fn storage_plan(self) -> ProductStoragePlan {
        self.storage_plan
    }

    /// Largest resident numeric output window used by generation.
    #[must_use]
    pub const fn maximum_window_payload_bytes(self) -> u64 {
        self.maximum_window_payload_bytes
    }

    /// Largest resident stored-mask window.
    #[must_use]
    pub const fn maximum_window_validity_bytes(self) -> u64 {
        self.maximum_window_validity_bytes
    }

    /// Maximum product-algorithm working-array bytes retained with production.
    #[must_use]
    pub const fn algorithm_scratch_bytes(self) -> u64 {
        self.algorithm_scratch_bytes
    }

    /// Owned generation summary, member names and beam arrays.
    #[must_use]
    pub const fn retained_metadata_bytes(self) -> u64 {
        self.retained_metadata_bytes
    }

    /// Peak beam phase, including parallel input conversion, fitting and slots.
    #[must_use]
    pub const fn beam_scratch_bytes(self) -> u64 {
        self.beam_scratch_bytes
    }

    /// What the CASA image writer holds: one member's image buffers and
    /// metadata, and the staged members' records.
    #[must_use]
    pub const fn writer_bytes(self) -> u64 {
        self.writer_bytes
    }

    /// Largest single member's binary32 payload.
    #[must_use]
    pub const fn maximum_member_payload_bytes(self) -> u64 {
        self.maximum_member_payload_bytes
    }

    /// Largest single member's byte-addressed validity array.
    #[must_use]
    pub const fn maximum_member_validity_bytes(self) -> u64 {
        self.maximum_member_validity_bytes
    }

    /// Bounded owned arrays, retained metadata, temporary beam storage and
    /// the image writer: the products phase's one charge.
    #[must_use]
    pub const fn peak_residency_bytes(self) -> u64 {
        self.peak_residency_bytes
    }
}

/// The largest arrays one member and one window hold.
#[derive(Default)]
struct MemberBounds {
    member_payload: u64,
    member_validity: u64,
    window_payload: u64,
    window_validity: u64,
    windows: usize,
}

impl PlannedContinuumGeneration {
    /// Derive current-catalog array and metadata demand for these inputs, the
    /// ones this plan was made from.
    ///
    /// # Errors
    ///
    /// Fails when the storage plan cannot lay out a member or a byte
    /// calculation overflows.
    pub fn demand(
        &self,
        inputs: &ContinuumProductInputs<'_>,
        storage_plan: ProductStoragePlan,
        writer: &CasaImageWriter,
    ) -> Result<ContinuumGenerationDemand, ProductsError> {
        let bounds = self.member_bounds(storage_plan)?;
        let maximum_windows = bounds.windows;
        let beam_jobs = if self.requires_beam() {
            inputs
                .normal_state()
                .domain_count()
                .checked_mul(inputs.normal_state().channel_count())
                .and_then(|count| count.checked_mul(inputs.normal_state().polarization_count()))
                .ok_or(ProductsError::ResourceDemandOverflow(
                    "generation beam count",
                ))?
        } else {
            0
        };
        let workers = match inputs.normal_state().basis() {
            NormalBasis::Taylor => 1,
            _ if cfg!(unix)
                && maximum_windows == 1
                && self.members().iter().any(|member| {
                    matches!(
                        member.role(),
                        ProductRole::RestoredImage(_) | ProductRole::PbCorrectedImage(_)
                    )
                }) =>
            {
                storage_plan.maximum_workers()
            }
            _ => storage_plan
                .maximum_workers()
                .min(maximum_windows.max(beam_jobs)),
        };
        let storage_plan = ProductStoragePlan::new(storage_plan.maximum_channels(), workers)?;
        let algorithm_scratch_bytes =
            self.algorithm_scratch_bytes(inputs, &bounds, workers.min(maximum_windows))?;
        let (retained_metadata_bytes, member_beam_bytes, beam_scratch_bytes) =
            self.metadata_demand(inputs, workers.min(beam_jobs))?;
        let writer_bytes =
            writer.residency_bytes(self, bounds.window_payload, bounds.window_validity)?;
        let peak_residency_bytes = if matches!(inputs.normal_state().basis(), NormalBasis::Taylor) {
            // The coupled family is built whole, then published member by
            // member: it peaks at the larger stage, not at their sum (#738).
            let stages = taylor_stages(inputs, beam_scratch_bytes)?;
            let publishing = [
                bounds.member_payload,
                bounds.member_validity,
                bounds.window_payload,
                bounds.window_validity,
                writer_bytes,
            ]
            .into_iter()
            .try_fold(stages.retained, |sum, bytes| {
                checked_add(sum, bytes, "Taylor publication residency")
            })?;
            checked_add(
                retained_metadata_bytes,
                stages.building.max(publishing),
                "Taylor generation residency",
            )?
        } else {
            let transient_bytes = checked_add(
                algorithm_scratch_bytes,
                member_beam_bytes,
                "active member preparation",
            )?
            .max(beam_scratch_bytes);
            checked_add(
                checked_add(
                    retained_metadata_bytes,
                    transient_bytes,
                    "product generation residency",
                )?,
                writer_bytes,
                "product generation and writer residency",
            )?
        };
        Ok(ContinuumGenerationDemand {
            storage_plan,
            algorithm_scratch_bytes,
            retained_metadata_bytes,
            beam_scratch_bytes,
            writer_bytes,
            maximum_member_payload_bytes: bounds.member_payload,
            maximum_member_validity_bytes: bounds.member_validity,
            maximum_window_payload_bytes: bounds.window_payload,
            maximum_window_validity_bytes: bounds.window_validity,
            peak_residency_bytes,
        })
    }

    /// The largest member and window arrays, and the most windows a member
    /// takes.
    fn member_bounds(
        &self,
        storage_plan: ProductStoragePlan,
    ) -> Result<MemberBounds, ProductsError> {
        let mut bounds = MemberBounds {
            windows: 1,
            ..MemberBounds::default()
        };
        for member in self.members() {
            let values = member.payload_values();
            bounds.member_payload = bounds
                .member_payload
                .max(bytes_for::<f32>(values, "member payload")?);
            bounds.member_validity = bounds
                .member_validity
                .max(bytes_for::<bool>(values, "member validity")?);
            let layout = storage_plan.layout(member.axes())?;
            bounds.windows = bounds
                .windows
                .max(layout.shape()[layout.spectral_axis()].div_ceil(layout.maximum_channels()));
            let window_values = layout.maximum_values();
            bounds.window_payload = bounds
                .window_payload
                .max(bytes_for::<f32>(window_values, "product window payload")?);
            bounds.window_validity = bounds
                .window_validity
                .max(bytes_for::<bool>(window_values, "product window validity")?);
        }
        Ok(bounds)
    }

    /// The products' working arrays: the algorithm's scratch and an active
    /// window per lane.
    fn algorithm_scratch_bytes(
        &self,
        inputs: &ContinuumProductInputs<'_>,
        bounds: &MemberBounds,
        window_workers: usize,
    ) -> Result<u64, ProductsError> {
        let taylor = matches!(inputs.normal_state().basis(), NormalBasis::Taylor);
        let mut bytes = if taylor {
            checked_add(
                taylor_scratch_bytes(inputs)?,
                checked_add(
                    bounds.member_payload,
                    bounds.member_validity,
                    "coupled member arrays",
                )?,
                "coupled member production scratch",
            )?
        } else {
            generic_scratch_bytes(self, inputs)?
        };
        bytes = checked_add(
            bytes,
            checked_add(
                bounds.window_payload,
                bounds.window_validity,
                "active product window",
            )?,
            "windowed production scratch",
        )?;
        // Each lane can retain a completed window while another lane still owns
        // its plane workspace. Beam fitting joins before these windows begin.
        bytes = checked_mul(bytes, window_workers as u64, "parallel product windows")?;
        if !taylor {
            bytes = checked_add(
                bytes,
                bytes_for::<Option<crate::ProductWindow>>(window_workers, "product window slots")?,
                "parallel product slots",
            )?;
        }
        Ok(bytes)
    }

    fn metadata_demand(
        &self,
        inputs: &ContinuumProductInputs<'_>,
        workers: usize,
    ) -> Result<(u64, u64, u64), ProductsError> {
        let state = inputs.normal_state();
        let domains = state.domain_count();
        let fitted = if !self.requires_beam() {
            0
        } else {
            match state.basis() {
                NormalBasis::Taylor => 1,
                _ => domains
                    .checked_mul(state.channel_count())
                    .and_then(|count| count.checked_mul(state.polarization_count()))
                    .ok_or(ProductsError::ResourceDemandOverflow(
                        "generation beam count",
                    ))?,
            }
        };
        let restoring_policy = inputs.problem().products().restoring_beam();
        let restoring = match (restoring_policy, state.basis()) {
            (RestoringBeamPolicy::None, _) => 0,
            (_, NormalBasis::Taylor) => 1,
            _ => fitted,
        };
        let mut retained = checked_add(
            size_of::<PublishedContinuumGeneration>() as u64,
            bytes_for::<PublishedMember>(self.members().len(), "published members")?,
            "generation summary",
        )?;
        for count in [fitted, restoring] {
            retained = checked_add(
                retained,
                bytes_for::<Option<RestoringBeam>>(count, "generation beams")?,
                "generation metadata",
            )?;
        }
        let mut active_member_beams = 0;
        for member in self.members() {
            let beams = match member.beam_rule() {
                ProductBeamRule::None
                | ProductBeamRule::Restoring(RestoringBeamPolicy::None)
                | ProductBeamRule::Metadata(RestoringBeamPolicy::None) => 0,
                ProductBeamRule::Fitted => fitted / domains,
                ProductBeamRule::Restoring(RestoringBeamPolicy::Common)
                | ProductBeamRule::Metadata(RestoringBeamPolicy::Common) => 1,
                _ => restoring / domains,
            };
            let beam_bytes = bytes_for::<Option<RestoringBeam>>(beams, "member beams")?;
            active_member_beams = active_member_beams.max(beam_bytes);
            for bytes in [
                bytes_for::<u8>(member.name().len(), "member name")?,
                beam_bytes,
            ] {
                retained = checked_add(retained, bytes, "member metadata")?;
            }
        }
        let common_scratch = if restoring_policy == RestoringBeamPolicy::Common
            && matches!(
                state.basis(),
                NormalBasis::Constant | NormalBasis::ChannelLocal
            ) {
            checked_add(
                bytes_for::<RestoringBeam>(fitted / domains, "common valid beams")?,
                checked_mul(
                    bytes_for::<casa_numerics::EllipticalGaussian>(
                        fitted / domains,
                        "common numeric beams",
                    )?,
                    2,
                    "common converted and mutable numeric beams",
                )?,
                "common beam scratch",
            )?
        } else {
            0
        };
        let mut fitting_scratch = 0;
        if self.requires_beam() {
            for shape in inputs.final_model().shape().domains() {
                let shape = shape.pixels();
                let cells = checked_shape_values([shape[0], shape[1], 1, 1])?;
                let input = bytes_for::<f32>(cells, "beam real PSF")?;
                // Paged PSFs own the decoded complex plane while converting to
                // f32; fitting then retains only f32 and the fit workspace.
                let conversion = bytes_for::<Complex64>(cells, "beam complex PSF")?;
                fitting_scratch = fitting_scratch.max(
                    checked_add(conversion, input, "beam input conversion")?.max(checked_add(
                        input,
                        casa_imaging_deconvolution::psf_fit_workspace_bytes(shape),
                        "beam fitting workspace",
                    )?),
                );
            }
            fitting_scratch = checked_mul(fitting_scratch, workers as u64, "parallel beam fits")?;
            fitting_scratch = checked_add(
                fitting_scratch,
                bytes_for::<Option<Option<RestoringBeam>>>(workers, "beam summary slots")?,
                "parallel beam summaries",
            )?;
        }
        Ok((
            retained,
            active_member_beams,
            active_member_beams.max(common_scratch).max(fitting_scratch),
        ))
    }
}

fn generic_scratch_bytes(
    planned: &PlannedContinuumGeneration,
    inputs: &ContinuumProductInputs<'_>,
) -> Result<u64, ProductsError> {
    let requires_validity = planned
        .members()
        .iter()
        .any(|member| member.validity() != ProductValidityRule::All);
    let requires_restoration = planned
        .members()
        .iter()
        .any(|member| matches!(member.role(), ProductRole::RestoredImage(_)));
    let mut maximum = 0;
    let mut normal_window = 0;
    for shape in inputs.final_model().shape().domains() {
        let shape = shape.pixels();
        let cells = checked_shape_values([shape[0], shape[1], 1, 1])?;
        let plane = bytes_for::<f32>(cells, "generic plane")?;
        let validity = bytes_for::<bool>(cells, "generic validity plane")?;
        // The full-window allowance conservatively bounds field-selective reads,
        // including the scalar-to-complex conversion overlap.
        normal_window = checked_add(
            normal_window,
            casa_imaging_deconvolution::normal_state_window_residency_bytes(
                shape,
                inputs.normal_state().polarization_count(),
                1,
            )?,
            "generic normal-state input window",
        )?;
        let mut scratch = if requires_validity { validity } else { 0 };

        // Normalization overlaps one converted source plane with its result.
        scratch = scratch.max(checked_mul(plane, 2, "generic converted plane pair")?);
        if requires_restoration {
            // Restoration retains its restored result and normalized residual while
            // a Gaussian kernel and one FFTW convolution workspace live.
            scratch = scratch.max(checked_add(
                checked_mul(plane, 3, "generic restoration planes")?,
                fft_convolution_workspace_bytes(shape)?,
                "generic restoration scratch",
            )?);
        }
        maximum = maximum.max(checked_add(
            scratch,
            bytes_for::<casa_imaging_model::ModelSample>(cells, "generic model input plane")?,
            "generic loaded model and algorithm scratch",
        )?);
    }
    checked_add(
        maximum,
        normal_window,
        "generic loaded normal state and algorithm scratch",
    )
}

fn taylor_scratch_bytes(inputs: &ContinuumProductInputs<'_>) -> Result<u64, ProductsError> {
    let parts = TaylorParts::of(inputs)?;
    checked_add(
        checked_add(parts.retained, parts.transient, "Taylor algorithm scratch")?,
        parts.restoration.unwrap_or(0),
        "Taylor restoration scratch",
    )
}

/// What the coupled Taylor family holds while `TaylorProducts::build`
/// builds it, at its largest stage, and once it is built
/// ([`taylor_stages`]).
struct TaylorStages {
    building: u64,
    retained: u64,
}

/// The stages of the coupled Taylor family (#738). It reads its PSF and
/// weight moments, residual and model terms and sensitivity first, solves
/// the principal residuals, fits the beam and restores each term; the PB,
/// PB-corrected, spectral-index and mask planes come after the
/// restoration's FFT workspace is gone. Building peaks at the largest of:
/// the read family and the principal residuals beside the beam fit
/// (`beam_scratch`) or beside restoration (its kernel, the restored terms
/// and one FFT convolution's workspace); or every retained plane beside
/// the principal residuals, the PB0 source and the normal solve.
fn taylor_stages(
    inputs: &ContinuumProductInputs<'_>,
    beam_scratch: u64,
) -> Result<TaylorStages, ProductsError> {
    let parts = TaylorParts::of(inputs)?;
    let planes = |count: u64| checked_mul(count, parts.plane, "Taylor stage planes");
    // PSF and weight moments, residual and model terms, the sensitivity.
    let read = checked_add(
        planes(2 * parts.moments + 2 * parts.terms + 1)?,
        checked_mul(parts.moments, size_of::<f32>() as u64, "Taylor sum weights")?,
        "Taylor read family",
    )?;
    let solved = checked_add(read, planes(parts.terms)?, "Taylor principal residuals")?;
    let mut building = checked_add(parts.retained, parts.transient, "Taylor built family")?
        .max(checked_add(solved, beam_scratch, "Taylor beam fit")?);
    if let Some(restoration) = parts.restoration {
        // The kernel and the workspace's output are in `restoration`; the
        // terms restored before the last are beside it.
        building = building.max(checked_add(
            checked_add(solved, restoration, "Taylor restoration")?,
            planes(parts.terms.saturating_sub(1))?,
            "Taylor restored terms",
        )?);
    }
    Ok(TaylorStages {
        building,
        retained: parts.retained,
    })
}

/// The pieces of the coupled Taylor family's demand.
struct TaylorParts {
    /// One `f32` plane.
    plane: u64,
    terms: u64,
    moments: u64,
    /// Every plane the built family keeps, its validity and sum weights.
    retained: u64,
    /// The principal residuals and PB0 source the build keeps beside the
    /// family, and the normal solve.
    transient: u64,
    /// The restoring kernel and one FFT convolution's workspace, when the
    /// products restore.
    restoration: Option<u64>,
}

impl TaylorParts {
    fn of(inputs: &ContinuumProductInputs<'_>) -> Result<Self, ProductsError> {
        let state = inputs.normal_state();
        let shape = state.shape();
        let cells = checked_shape_values([shape[0], shape[1], 1, 1])?;
        let terms = u64::try_from(state.coefficient_term_count())
            .map_err(|_| ProductsError::ResourceDemandOverflow("Taylor term count"))?;
        let moments = u64::try_from(state.normal_moment_count())
            .map_err(|_| ProductsError::ResourceDemandOverflow("Taylor moment count"))?;
        let cells = u64::try_from(cells)
            .map_err(|_| ProductsError::ResourceDemandOverflow("Taylor cell count"))?;

        // TaylorProducts retains PSF and weight moments; residual, model,
        // restored, PB and PB-corrected term families; alpha, PB-corrected alpha,
        // alpha-error, sensitivity and mask;
        // two validity planes; and scalar sum weights until every planned member
        // has been copied into the produced generation.
        let retained_f32_planes = checked_add(
            checked_mul(moments, 2, "Taylor PSF and weight planes")?,
            checked_add(
                checked_mul(terms, 5, "Taylor term families")?,
                5,
                "Taylor scalar product planes",
            )?,
            "Taylor retained plane count",
        )?;
        let retained = checked_add(
            checked_mul(
                checked_mul(retained_f32_planes, cells, "Taylor retained values")?,
                size_of::<f32>() as u64,
                "Taylor retained payload bytes",
            )?,
            checked_add(
                checked_mul(
                    checked_mul(2, cells, "Taylor validity values")?,
                    size_of::<bool>() as u64,
                    "Taylor validity bytes",
                )?,
                checked_mul(moments, size_of::<f32>() as u64, "Taylor sum weights")?,
                "Taylor validity and sum-weight bytes",
            )?,
            "Taylor retained scratch",
        )?;

        // Construction additionally retains the principal residual family and PB0
        // source plane. The normal solve owns three dense term matrices plus two
        // term vectors at its peak. This intentionally follows the product owner's
        // concrete current data structures, rather than a runtime approximation.
        let transient_planes = checked_mul(
            checked_add(terms, 1, "Taylor transient plane count")?,
            checked_mul(
                cells,
                size_of::<f32>() as u64,
                "Taylor transient plane bytes",
            )?,
            "Taylor transient planes",
        )?;
        let term_square = checked_mul(terms, terms, "Taylor normal matrix values")?;
        let solve = checked_mul(
            checked_add(
                checked_mul(term_square, 3, "Taylor normal matrices")?,
                checked_mul(terms, 2, "Taylor solve vectors")?,
                "Taylor solve values",
            )?,
            size_of::<f64>() as u64,
            "Taylor solve bytes",
        )?;
        let restoration =
            if inputs.problem().products().restoring_beam() != RestoringBeamPolicy::None {
                Some(checked_add(
                    checked_mul(cells, size_of::<f32>() as u64, "Taylor restoring kernel")?,
                    fft_convolution_workspace_bytes(shape)?,
                    "Taylor convolution scratch",
                )?)
            } else {
                None
            };
        Ok(Self {
            plane: checked_mul(cells, size_of::<f32>() as u64, "Taylor plane bytes")?,
            terms,
            moments,
            retained,
            transient: checked_add(transient_planes, solve, "Taylor transient scratch")?,
            restoration,
        })
    }
}

fn fft_convolution_workspace_bytes(shape: [usize; 2]) -> Result<u64, ProductsError> {
    let cells = checked_shape_values([shape[0], shape[1], 1, 1])?;
    let complex_planes = bytes_for::<Complex64>(
        cells
            .checked_mul(2)
            .ok_or(ProductsError::ResourceDemandOverflow("FFT complex planes"))?,
        "FFT complex planes",
    )?;
    let output = bytes_for::<f32>(cells, "FFT output plane")?;
    let planning = cells
        .checked_add(64)
        .and_then(|values| values.checked_mul(size_of::<Complex64>()))
        .and_then(|bytes| bytes.checked_add(cells.checked_mul(size_of::<usize>())?))
        .ok_or(ProductsError::ResourceDemandOverflow("FFTW planning"))?;
    checked_add(
        checked_add(complex_planes, output, "FFT planes and output")?,
        u64::try_from(planning)
            .map_err(|_| ProductsError::ResourceDemandOverflow("FFTW planning"))?,
        "FFT convolution workspace",
    )
}

fn checked_shape_values(shape: [usize; 4]) -> Result<usize, ProductsError> {
    shape.into_iter().try_fold(1usize, |values, extent| {
        values
            .checked_mul(extent)
            .ok_or(ProductsError::ResourceDemandOverflow("member shape"))
    })
}

fn bytes_for<T>(count: usize, what: &'static str) -> Result<u64, ProductsError> {
    let count = u64::try_from(count).map_err(|_| ProductsError::ResourceDemandOverflow(what))?;
    checked_mul(count, size_of::<T>() as u64, what)
}

fn checked_add(left: u64, right: u64, what: &'static str) -> Result<u64, ProductsError> {
    left.checked_add(right)
        .ok_or(ProductsError::ResourceDemandOverflow(what))
}

fn checked_mul(left: u64, right: u64, what: &'static str) -> Result<u64, ProductsError> {
    left.checked_mul(right)
        .ok_or(ProductsError::ResourceDemandOverflow(what))
}

#[cfg(test)]
mod tests {
    #[test]
    fn demand_arithmetic_fails_closed() {
        assert!(matches!(
            super::checked_mul(u64::MAX, 2, "test"),
            Err(crate::ProductsError::ResourceDemandOverflow("test"))
        ));
        assert!(matches!(
            super::checked_shape_values([usize::MAX, 2, 1, 1]),
            Err(crate::ProductsError::ResourceDemandOverflow("member shape"))
        ));
    }
}

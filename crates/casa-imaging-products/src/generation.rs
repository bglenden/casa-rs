// SPDX-License-Identifier: LGPL-3.0-or-later

//! Product generation: every planned member, in publication order, a
//! bounded window at a time, straight to its image in the writer's staging.

use std::ops::Range;

use casa_imaging_deconvolution::NormalBasis;
use casa_imaging_model::{
    ProductPixelMask, ProductRole, ProductTerm, ProductValidityRule, RestoringBeamPolicy,
};

use crate::beam::{PlaneBeam, RestoringBeam, fit_plane_beam, fixed_restoring_beam, settle_beams};
use crate::error::ProductsError;
use crate::normalization::psf_peak;
use crate::plan::{
    AnalyticPrimaryBeamModel, PlannedContinuumGeneration, PlannedMember,
    PublishedContinuumGeneration,
};
use crate::plane::{
    PlaneMemberRequest, apparent_model, model_real_plane, primary_beam_plane, produce_plane_member,
    product_plane_validity, psf_normalizes, psf_real_plane, reconstruction_support_plane,
    scatter_image_polarization_plane, scatter_polarization_plane_state, zero_invalid_plane_values,
};
use crate::source::ContinuumProductInputs;
use crate::storage::{ProductMemberWriter, ProductWindow, ProductWindowLayout};
use crate::taylor::TaylorProducts;
use crate::writer::CasaImageWriter;
use crate::{ProductStoragePlan, ProductWindowExecutor};

/// Produce every planned member and write it to `output`'s staging.
///
/// Runs restoring-beam fitting, restoration, residual scaling, normalization,
/// validity, and metadata exactly once per member, in the planned publication
/// order. [`CasaImageWriter::publish`] then replaces the targets.
///
/// # Errors
///
/// Unsupported roles, failed beam fits, generated non-finite payloads, and
/// image writes that fail. A failed generation discards every member it
/// staged, so a later [`CasaImageWriter::publish`] publishes none of them.
pub fn produce_continuum_members(
    planned: &PlannedContinuumGeneration,
    inputs: &ContinuumProductInputs<'_>,
    storage_plan: ProductStoragePlan,
    execution: &impl ProductWindowExecutor,
    output: &CasaImageWriter,
) -> Result<PublishedContinuumGeneration, ProductsError> {
    if inputs.cube_taylor_primary_beam().is_some() && planned.primary_beam_model.is_none() {
        return Err(ProductsError::MissingPrimaryBeamModel);
    }
    let kept = output.staged_count();
    let produced = if inputs.normal_state().basis() == NormalBasis::Taylor {
        produce_taylor_members(planned, inputs, storage_plan, output)
    } else {
        produce_plane_members(planned, inputs, storage_plan, execution, output)
    };
    if produced.is_err() {
        output.discard_staged_after(kept);
    }
    produced
}

fn produce_plane_members(
    planned: &PlannedContinuumGeneration,
    inputs: &ContinuumProductInputs<'_>,
    storage_plan: ProductStoragePlan,
    execution: &impl ProductWindowExecutor,
    output: &CasaImageWriter,
) -> Result<PublishedContinuumGeneration, ProductsError> {
    let fitted_beams = if planned.requires_beam() {
        fit_beams(planned, inputs, storage_plan, execution)?
    } else {
        Box::new([])
    };
    let restoring_beams = restoring_beams(inputs, &fitted_beams)?;
    for member in &planned.members {
        let windows = MemberWindows::new(
            planned,
            member,
            inputs,
            storage_plan,
            &fitted_beams,
            &restoring_beams,
        )?;
        let image = output.begin(
            member,
            windows.layout,
            &planned.beams_for_member(member, &fitted_beams, &restoring_beams),
            planned.uses_weight_image,
            None,
        )?;
        windows.write(ProductMemberWriter::new(windows.layout, image), execution)?;
    }
    Ok(planned.published(fitted_beams, restoring_beams))
}

/// Fit a restoring beam to every valid PSF plane, domain by domain, then
/// channel by channel, then polarization, a wave of the team's workers at a
/// time.
fn fit_beams(
    planned: &PlannedContinuumGeneration,
    inputs: &ContinuumProductInputs<'_>,
    storage_plan: ProductStoragePlan,
    execution: &impl ProductWindowExecutor,
) -> Result<Box<[Option<RestoringBeam>]>, ProductsError> {
    let normal_state = inputs.normal_state();
    let channels = normal_state.channel_count();
    let polarizations = normal_state.polarization_count();
    let beam_count = normal_state
        .domain_count()
        .checked_mul(channels)
        .and_then(|count| count.checked_mul(polarizations))
        .ok_or(ProductsError::ResourceDemandOverflow(
            "generation beam count",
        ))?;
    let mut fitted = Vec::with_capacity(beam_count);
    for start in (0..beam_count).step_by(storage_plan.maximum_workers()) {
        let count = storage_plan.maximum_workers().min(beam_count - start);
        let mut slots = vec![None; count];
        execution.prepare(&mut slots, &|index| {
            let ordinal = start + index;
            let domain =
                &inputs.problem().geometry().domains()[ordinal / (channels * polarizations)];
            if matches!(domain.role(), casa_imaging_model::ImageDomainRole::Main)
                && let Some(saved) = inputs.saved_psf_beams()
            {
                return Ok(PlaneBeam::Fitted(
                    saved[ordinal % (channels * polarizations)],
                ));
            }
            let plane = normal_state.read_plane(
                inputs.model_domain_ordinal(domain.role()),
                (ordinal / polarizations) % channels,
                ordinal % polarizations,
            )?;
            // A freshly formed PSF is normalized before CASA fits the beam,
            // so normPSF zeros a sub-threshold peak. A reused PSF is already
            // in image units: CASA attempts the fit for every positive peak,
            // and FitGaussianPSF refuses positive peaks below 1e-7.
            let psf = psf_real_plane(&plane)?;
            let peak = psf_peak(psf.iter().copied())?;
            if !psf_normalizes(peak) {
                return Ok(if planned.reuses_single_psf && peak > 0.0 {
                    PlaneBeam::Refused
                } else {
                    PlaneBeam::Blank
                });
            }
            fit_plane_beam(
                &psf,
                plane.shape(),
                inputs.cell_size_rad_for_domain(domain.role()),
                planned.psf_cutoff(),
            )
        })?;
        fitted.extend(
            slots
                .into_iter()
                .map(|slot| slot.expect("the executor fills every slot")),
        );
    }
    Ok(settle_beams(&fitted, channels * polarizations))
}

/// The beams each plane is restored with, by the problem's restoring-beam
/// policy: none, each plane's fitted beam, one beam enclosing a domain's
/// fitted beams, or the beam the request gives. A plane with no fitted beam
/// (a blank PSF) restores with its domain's largest restoring beam, as
/// `SIImageStore::makeImageBeamSet` replaces a null beam by the beam set's
/// largest (`getMaxAreaBeam`) before `restore` restores every plane: a
/// start model regridded onto a channel no sample reaches is restored
/// there. A domain with no fitted beam restores none. A Taylor family's planes
/// are its polarizations.
pub(crate) fn restoring_beams(
    inputs: &ContinuumProductInputs<'_>,
    fitted_beams: &[Option<RestoringBeam>],
) -> Result<Box<[Option<RestoringBeam>]>, ProductsError> {
    let mut beams = policy_restoring_beams(inputs, fitted_beams)?;
    let normal_state = inputs.normal_state();
    let planes = normal_state.channel_count() * normal_state.polarization_count();
    if planes > 0 {
        for domain_beams in beams.chunks_mut(planes) {
            let largest = domain_beams
                .iter()
                .flatten()
                .copied()
                .max_by(|left, right| {
                    (left.major_fwhm_rad() * left.minor_fwhm_rad())
                        .total_cmp(&(right.major_fwhm_rad() * right.minor_fwhm_rad()))
                });
            for beam in domain_beams.iter_mut().filter(|beam| beam.is_none()) {
                *beam = largest;
            }
        }
    }
    Ok(beams)
}

/// [`restoring_beams`] before blank planes take their domain's largest.
fn policy_restoring_beams(
    inputs: &ContinuumProductInputs<'_>,
    fitted_beams: &[Option<RestoringBeam>],
) -> Result<Box<[Option<RestoringBeam>]>, ProductsError> {
    Ok(match inputs.problem().products().restoring_beam() {
        RestoringBeamPolicy::None => Box::new([]),
        RestoringBeamPolicy::PerPlane => fitted_beams.into(),
        RestoringBeamPolicy::Fixed(beam) => {
            let beam = fixed_restoring_beam(beam)?;
            fitted_beams
                .iter()
                .map(|fitted| fitted.map(|_| beam))
                .collect()
        }
        RestoringBeamPolicy::Common => {
            let normal_state = inputs.normal_state();
            let planes = normal_state.channel_count() * normal_state.polarization_count();
            let mut selected = Vec::with_capacity(fitted_beams.len());
            for domain_beams in fitted_beams.chunks(planes) {
                let valid = domain_beams.iter().flatten().copied().collect::<Vec<_>>();
                if valid.is_empty() {
                    selected.extend_from_slice(domain_beams);
                } else {
                    let common = RestoringBeam::common_enclosing(&valid)
                        .map_err(|error| ProductsError::BeamFitFailed(error.to_string()))?;
                    selected.extend(domain_beams.iter().map(|fitted| fitted.map(|_| common)));
                }
            }
            selected.into_boxed_slice()
        }
    })
}

/// What generating one member's windows needs.
struct MemberWindows<'a, 'inputs> {
    planned: &'a PlannedContinuumGeneration,
    member: &'a PlannedMember,
    inputs: &'a ContinuumProductInputs<'inputs>,
    layout: ProductWindowLayout,
    domain_ordinal: usize,
    plane_shape: [usize; 2],
    fitted_beams: &'a [Option<RestoringBeam>],
    restoring_beams: &'a [Option<RestoringBeam>],
    workers: usize,
    fft_threads: usize,
}

impl<'a, 'inputs> MemberWindows<'a, 'inputs> {
    fn new(
        planned: &'a PlannedContinuumGeneration,
        member: &'a PlannedMember,
        inputs: &'a ContinuumProductInputs<'inputs>,
        storage_plan: ProductStoragePlan,
        fitted_beams: &'a [Option<RestoringBeam>],
        restoring_beams: &'a [Option<RestoringBeam>],
    ) -> Result<Self, ProductsError> {
        let domain_ordinal = inputs.model_domain_ordinal(member.axes().domain());
        let layout = storage_plan.layout(member.axes())?;
        let windows = inputs
            .normal_state()
            .channel_count()
            .div_ceil(layout.maximum_channels());
        Ok(Self {
            planned,
            member,
            inputs,
            layout,
            domain_ordinal,
            plane_shape: inputs.final_model().shape().domains()[domain_ordinal].pixels(),
            fitted_beams,
            restoring_beams,
            workers: storage_plan.maximum_workers().min(windows),
            fft_threads: storage_plan.fft_threads(windows),
        })
    }

    /// Generate the member's windows a wave of workers at a time and write
    /// them in channel order.
    fn write(
        &self,
        mut writer: ProductMemberWriter<'_>,
        execution: &impl ProductWindowExecutor,
    ) -> Result<(), ProductsError> {
        let channels = self.inputs.normal_state().channel_count();
        let window_channels = self.layout.maximum_channels();
        let windows = channels.div_ceil(window_channels);
        for wave_start in (0..windows).step_by(self.workers) {
            let mut slots: Vec<Option<ProductWindow>> = (0..self.workers.min(windows - wave_start))
                .map(|_| None)
                .collect();
            execution.prepare(&mut slots, &|index| {
                let start = (wave_start + index) * window_channels;
                self.window(start..(start + window_channels).min(channels))
            })?;
            for slot in slots {
                writer.write(slot.expect("the executor fills every slot"))?;
            }
        }
        writer.finish()
    }

    /// One window: every polarization plane of its channels.
    fn window(&self, channels: Range<usize>) -> Result<ProductWindow, ProductsError> {
        let mut window = self.layout.window(channels.clone())?;
        let reconstruction_only = matches!(
            self.member.role,
            ProductRole::Model(ProductTerm::Single | ProductTerm::Taylor(0))
                | ProductRole::CleanMask
        );
        for channel in channels.clone() {
            for polarization in 0..self.inputs.normal_state().polarization_count() {
                let local = channel - channels.start;
                if reconstruction_only {
                    let plane = self.reconstruction_plane(channel, polarization)?;
                    self.scatter(
                        &mut window.payload,
                        &window.shape,
                        polarization,
                        local,
                        &plane,
                    );
                } else {
                    self.normal_plane(&mut window, channel, polarization, local)?;
                }
            }
        }
        Ok(window)
    }

    fn scatter<T: Copy>(
        &self,
        target: &mut [T],
        shape: &[usize; 4],
        polarization: usize,
        local_channel: usize,
        plane: &[T],
    ) {
        scatter_image_polarization_plane(
            target,
            self.member.axes().order(),
            *shape,
            polarization,
            local_channel,
            self.plane_shape,
            plane,
        );
    }

    /// A member formed from the final model or the CLEAN mask alone.
    fn reconstruction_plane(
        &self,
        channel: usize,
        polarization: usize,
    ) -> Result<Vec<f32>, ProductsError> {
        let [width, height] = self.plane_shape;
        if self.member.role == ProductRole::CleanMask {
            return Ok(reconstruction_support_plane(
                self.inputs,
                self.member.axes().domain(),
                (channel, polarization),
                width * height,
            ));
        }
        let model = model_real_plane(
            self.inputs.final_model(),
            self.domain_ordinal,
            channel,
            polarization,
            self.plane_shape,
        )?;
        if self.planned.primary_beam_model != Some(AnalyticPrimaryBeamModel::MosaicSensitivity) {
            return Ok(model);
        }
        let plane =
            self.inputs
                .normal_state()
                .read_plane(self.domain_ordinal, channel, polarization)?;
        let products = self.inputs.problem().products();
        apparent_model(
            model,
            &plane,
            products.normalization(),
            products.validity().primary_beam(),
            self.planned.primary_beam_model,
        )
    }

    /// A member formed from a normal-state plane: its values, and its pixel
    /// mask where it stores one.
    fn normal_plane(
        &self,
        window: &mut ProductWindow,
        channel: usize,
        polarization: usize,
        local_channel: usize,
    ) -> Result<(), ProductsError> {
        let plane =
            self.inputs
                .normal_state()
                .read_plane(self.domain_ordinal, channel, polarization)?;
        if matches!(self.member.role, ProductRole::SumWeights(_)) {
            // CASA's `.sumwt` is the PSF gridding's sum:
            // `FTMachine::finalizeToSkyNew` seeds the image store's sumwt
            // once, from `makePSF` (`refim_mawproject`: 8987.97 is the weight
            // cell's norms, 0.24 % above the data gridding's). The flat-noise
            // products divide by the raw sensitivity, where every such sum
            // cancels, as CASA's `divideResidualByWeight` reduces to.
            // A faceted domain publishes each facet store's own sum.
            let facets = self.inputs.facet_sum_weights(self.domain_ordinal);
            scatter_polarization_plane_state(
                &mut window.payload,
                self.member.axes(),
                window.shape,
                polarization,
                local_channel,
                |facet| match facets {
                    Some(facets) => facets[facet][polarization] as f32,
                    None => plane.sum_weight() as f32,
                },
            );
            return Ok(());
        }
        let planes = self.inputs.normal_state().polarization_count();
        let beam_index =
            (self.domain_ordinal * self.inputs.normal_state().channel_count() + channel) * planes
                + polarization;
        let mut values = produce_plane_member(&PlaneMemberRequest {
            member: self.member,
            inputs: self.inputs,
            plane: &plane,
            domain_ordinal: self.domain_ordinal,
            polarization,
            reuses_single_psf: self.planned.reuses_single_psf,
            fitted_beam: self.fitted_beams.get(beam_index).copied().flatten(),
            restoring_beam: self.restoring_beams.get(beam_index).copied().flatten(),
            primary_beam_model: self.planned.primary_beam_model,
            fft_threads: self.fft_threads,
        })?;
        if matches!(self.member.role, ProductRole::Residual(ProductTerm::Single))
            && let Some(mean_pb) = self.inputs.cube_taylor_primary_beam()
        {
            let channel_pb = primary_beam_plane(
                self.planned.primary_beam_model,
                self.inputs,
                self.member.axes().domain(),
                &plane,
            )?;
            let cutoff = self
                .inputs
                .problem()
                .products()
                .validity()
                .primary_beam()
                .cutoff();
            adjust_cube_residual(&mut values, mean_pb, &channel_pb, cutoff)?;
        }
        let validity = |rule| {
            product_plane_validity(
                rule,
                &plane,
                self.planned.primary_beam_model,
                self.inputs,
                (self.member.axes().domain(), polarization),
            )
        };
        if self.member.validity != ProductValidityRule::All {
            zero_invalid_plane_values(&mut values, &validity(self.member.validity)?);
        }
        if let ProductPixelMask::Explicit(rule) = self.member.storage.pixel_mask() {
            let support = validity(rule)?;
            self.scatter(
                &mut window.validity,
                &window.shape,
                polarization,
                local_channel,
                &support,
            );
        }
        self.scatter(
            &mut window.payload,
            &window.shape,
            polarization,
            local_channel,
            &values,
        );
        Ok(())
    }
}

/// CASA leaves an entire cube residual plane untouched when its published PB
/// is blank. Otherwise its common/channel PB ratio applies only above the
/// channel PB cutoff (`SynthesisUtilMethods::removeFreqDepPB`).
pub(crate) fn adjust_cube_residual(
    values: &mut [f32],
    mean_pb: &[f32],
    channel_pb: &[f32],
    cutoff: f32,
) -> Result<(), ProductsError> {
    if channel_pb.iter().any(|&value| value > cutoff) {
        for ((value, &common), &channel) in values.iter_mut().zip(mean_pb).zip(channel_pb) {
            *value = if channel > cutoff {
                common * *value / channel
            } else {
                0.0
            };
            if !value.is_finite() {
                return Err(ProductsError::GeneratedNonfinite);
            }
        }
    }
    Ok(())
}

fn produce_taylor_members(
    planned: &PlannedContinuumGeneration,
    inputs: &ContinuumProductInputs<'_>,
    storage_plan: ProductStoragePlan,
    output: &CasaImageWriter,
) -> Result<PublishedContinuumGeneration, ProductsError> {
    let products = TaylorProducts::build(inputs, planned, output)?;
    let fitted_beams = if planned.requires_beam() {
        products.fitted_beams()
    } else {
        Box::new([])
    };
    let restoring_beams = match inputs.problem().products().restoring_beam() {
        RestoringBeamPolicy::None => Box::new([]),
        RestoringBeamPolicy::PerPlane => products.restoring_beams(),
        // One beam restores every Stokes plane, and the image records it
        // alone, as `SDAlgorithmMSMFS::restore` sets one restoring beam.
        RestoringBeamPolicy::Common | RestoringBeamPolicy::Fixed(_) => products
            .restoring_beams()
            .iter()
            .copied()
            .find(Option::is_some)
            .into_iter()
            .collect(),
    };
    for member in &planned.members {
        let mut payload = products.payload(member.role)?;
        // An ordinary Taylor PB is formed at one frequency and zeroed by
        // `makePBImage`. MVC's supplied PB is the weighted cube PB: CASA
        // preserves its values below pblimit while marking those pixels
        // invalid. The other products still blank outside their support.
        let supplied_pb = inputs.taylor_primary_beam().is_some()
            && member.role == ProductRole::PrimaryBeam(ProductTerm::Taylor(0));
        if member.validity != ProductValidityRule::All && !supplied_pb {
            zero_invalid_plane_values(&mut payload, &products.validity(member.validity)?);
        }
        let saved_residual_mask = match member.role {
            ProductRole::RestoredImage(ProductTerm::Taylor(term))
                if planned.saved_taylor_mask_read_bytes.is_some() =>
            {
                Some(output.saved_taylor_residual_mask(member.axes().domain(), term, true)?)
            }
            _ => None,
        };
        let explicit_mask = saved_residual_mask.as_ref().map(Option::is_some);
        let validity = match (saved_residual_mask, member.storage.pixel_mask()) {
            (Some(Some(mask)), _) => mask,
            (Some(None), _)
            | (None, ProductPixelMask::Absent)
            | (
                None,
                ProductPixelMask::Explicit(
                    ProductValidityRule::All | ProductValidityRule::FinalNormalState,
                ),
            ) => vec![true; member.payload_values],
            (None, ProductPixelMask::Explicit(rule)) => products.validity(rule)?,
        };
        if payload.len() != member.payload_values || validity.len() != member.payload_values {
            return Err(ProductsError::PayloadLengthMismatch {
                expected: member.payload_values,
                actual: payload.len().max(validity.len()),
            });
        }
        let layout = storage_plan.layout(member.axes())?;
        let image = output.begin(
            member,
            layout,
            &planned.beams_for_member(member, &fitted_beams, &restoring_beams),
            planned.uses_weight_image,
            explicit_mask,
        )?;
        let mut writer = ProductMemberWriter::new(layout, image);
        writer.write_coupled(&payload, &validity)?;
        writer.finish()?;
    }
    Ok(planned.published(fitted_beams, restoring_beams))
}

#[cfg(test)]
mod mvc_adjustment_tests {
    use super::adjust_cube_residual;

    #[test]
    fn residual_correction_uses_strict_channel_support_and_skips_a_blank_plane() {
        let mut residual = [1.0, 2.0, 3.0, 4.0];
        let common = [0.5, 0.6, 0.7, 0.8];
        adjust_cube_residual(&mut residual, &common, &[0.1, 0.2, 0.4, 1.0], 0.2).unwrap();
        assert_eq!(residual, [0.0, 0.0, 0.7 * 3.0 / 0.4, 0.8 * 4.0]);
        let mut blank = [1.0, 2.0, 3.0, 4.0];
        adjust_cube_residual(&mut blank, &common, &[0.0, 0.1, 0.2, 0.0], 0.2).unwrap();
        assert_eq!(blank, [1.0, 2.0, 3.0, 4.0]);
        let mut overflow = [f32::MAX];
        assert!(adjust_cube_residual(&mut overflow, &[f32::MAX], &[1.0], 0.2).is_err());
    }
}

// SPDX-License-Identifier: LGPL-3.0-or-later

//! Product generation: every planned member, in publication order, a
//! bounded window at a time, straight to its image in the writer's staging.

use std::ops::Range;

use casa_imaging_model::{
    ProductPixelMask, ProductRole, ProductTerm, ProductValidityRule, RestoringBeamPolicy,
};
use casa_imaging_reconstruction::{NormalStateCatalog, SpectralChannelValidity};

use crate::beam::{RestoringBeam, fit_restoring_beam};
use crate::error::ProductsError;
use crate::plan::{
    AnalyticPrimaryBeamModel, PlannedContinuumGeneration, PlannedMember,
    PublishedContinuumGeneration,
};
use crate::plane::{
    PlaneMemberRequest, apparent_model, model_real_plane, produce_plane_member,
    product_plane_validity, psf_real_plane, reconstruction_support_plane,
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
    let kept = output.staged_count();
    let produced =
        if inputs.normal_state().catalog() == NormalStateCatalog::UnnormalizedTaylorBlockV1 {
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
            let plane = normal_state.read_plane(
                inputs.model_domain_ordinal(domain.role()),
                (ordinal / polarizations) % channels,
                ordinal % polarizations,
            )?;
            if plane.validity() != SpectralChannelValidity::Valid {
                return Ok(None);
            }
            fit_restoring_beam(
                &psf_real_plane(&plane)?,
                plane.shape(),
                inputs.cell_size_rad_for_domain(domain.role()),
                planned.psf_cutoff(),
            )
            .map(Some)
        })?;
        fitted.extend(
            slots
                .into_iter()
                .map(|slot| slot.expect("the executor fills every slot")),
        );
    }
    Ok(fitted.into_boxed_slice())
}

/// The beams each plane is restored with, by the problem's restoring-beam
/// policy: none, each plane's fitted beam, or one beam enclosing a domain's
/// fitted beams.
fn restoring_beams(
    inputs: &ContinuumProductInputs<'_>,
    fitted_beams: &[Option<RestoringBeam>],
) -> Result<Box<[Option<RestoringBeam>]>, ProductsError> {
    Ok(match inputs.problem().products().restoring_beam() {
        RestoringBeamPolicy::None => Box::new([]),
        RestoringBeamPolicy::PerPlane => fitted_beams.into(),
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
            fitted_beam: self.fitted_beams.get(beam_index).copied().flatten(),
            restoring_beam: self.restoring_beams.get(beam_index).copied().flatten(),
            primary_beam_model: self.planned.primary_beam_model,
            fft_threads: self.fft_threads,
        })?;
        let validity = |rule| {
            product_plane_validity(
                rule,
                &plane,
                self.planned.primary_beam_model,
                self.inputs,
                self.member.axes().domain(),
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

fn produce_taylor_members(
    planned: &PlannedContinuumGeneration,
    inputs: &ContinuumProductInputs<'_>,
    storage_plan: ProductStoragePlan,
    output: &CasaImageWriter,
) -> Result<PublishedContinuumGeneration, ProductsError> {
    let products = TaylorProducts::build(inputs, planned.psf_cutoff, planned.primary_beam_model)?;
    let fitted_beams = if planned.requires_beam() {
        vec![products.fitted_beam()].into_boxed_slice()
    } else {
        Box::new([])
    };
    let restoring_beams = match inputs.problem().products().restoring_beam() {
        RestoringBeamPolicy::None => Box::new([]),
        RestoringBeamPolicy::PerPlane | RestoringBeamPolicy::Common => {
            vec![products.restoring_beam()].into_boxed_slice()
        }
    };
    for member in &planned.members {
        let mut payload = products.payload(member.role)?;
        // As for one plane: a member with a validity rule is zero outside it,
        // as `SIImageStore::makePBImage` leaves `.pb.tt0` at pblimit.
        if member.validity != ProductValidityRule::All {
            zero_invalid_plane_values(&mut payload, &products.validity(member.validity)?);
        }
        let validity = match member.storage.pixel_mask() {
            ProductPixelMask::Absent
            | ProductPixelMask::Explicit(
                ProductValidityRule::All | ProductValidityRule::FinalNormalState,
            ) => {
                vec![true; member.payload_values]
            }
            ProductPixelMask::Explicit(rule) => products.validity(rule)?,
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
        )?;
        let mut writer = ProductMemberWriter::new(layout, image);
        writer.write_coupled(&payload, &validity)?;
        writer.finish()?;
    }
    Ok(planned.published(fitted_beams, restoring_beams))
}

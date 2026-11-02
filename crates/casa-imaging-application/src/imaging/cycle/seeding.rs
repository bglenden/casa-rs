// SPDX-License-Identifier: LGPL-3.0-or-later
//! The initial major cycle, from an empty model or a start model (#841
//! Phase 6b, #970 review).
//!
//! A start model is the model's first generation, in the units a run
//! publishes its `.model` in. Under a direction-independent response, or
//! flat sky, those are the physical units the pass predicts from, and the
//! initial pass grids the residual of the start model. Under flat noise (a
//! mosaic's or AW kernel set's `normtype`), `.model` is the physical model
//! times the PB, and CASA divides a model by the weights' PB just before it
//! predicts (`SIImageStore::divideModelByWeight`), once `makePSF` has
//! formed them. Such a run takes CASA's order: the initial pass forms the
//! PSF, weights and sums with no model, and the start model, made physical
//! with those weights, is the first generation of a refresh of that pass's
//! residual. That costs one more pass over the data, for such runs only.
//! Either way, a run started from another run's model with no minor cycle
//! gives that run's products (the T1 laws; CASA 6.7.6 does the same on a
//! 14-pointing mosaic).

use casa_imaging_deconvolution::{FinalNormalState, MajorCycle, ModelGeneration};
use casa_imaging_model::ProductNormalization;
use casa_imaging_operator::ModeSet;
use casa_imaging_products::{MinorCycleImageResponse, MosaicSensitivity};
use casa_imaging_runtime::Admitted;

use super::{Major, Run};
use crate::compile::StartModel;
use crate::imaging::ImagingError;

/// The response a start model is made physical with before the first
/// major cycle predicts it: a flat-noise one; `None` without a start
/// model, or when its units are physical already.
pub(super) fn reseed_response(
    start_model: Option<&StartModel>,
    response: Option<MinorCycleImageResponse>,
) -> Option<MinorCycleImageResponse> {
    start_model
        .and(response)
        .filter(|response| response.normalization() == ProductNormalization::FlatNoise)
}

impl Run<'_> {
    /// The initial major cycle: data and PSF, from the start model when there
    /// is one (its residual) and from an empty model otherwise; it writes
    /// visibilities when it is also final.
    pub(super) fn initial(&mut self, last: bool) -> Result<Major, ImagingError> {
        self.measure_row_work()?;
        match self.reseed {
            Some(response) => {
                let unseeded = self.initial_pass(false, false)?;
                self.reseed(unseeded, response, last)
            }
            None => self.initial_pass(self.start_model.is_some(), last),
        }
    }

    /// The initial pass, from the start model when `seeded`.
    fn initial_pass(&mut self, seeded: bool, last: bool) -> Result<Major, ImagingError> {
        // The model, and a start model's seeding beside it, are admitted
        // with the pass before either is allocated, so the pass plans its
        // waves and chunks in what seeding leaves (#970 review).
        let seeding = if seeded { self.seeding_bytes() } else { 0 };
        let Admitted {
            value: plan,
            reservation: mut pass,
        } = self.admit_pass(
            self.initial_modes(),
            seeded,
            self.lifecycle.resident_bytes() + self.write_bytes(last) + seeding,
            self.writes(last),
        )?;
        let generation = match &self.start_model {
            Some(start) if seeded => {
                // Released once the generation holds the samples.
                let _seeding = pass.split(seeding);
                let samples = start.main_samples().map_err(ImagingError::StartModel)?;
                self.lifecycle.initial_from_start_model(&samples)?
            }
            _ => self.lifecycle.initial_empty()?,
        };
        let model = self.lifecycle.prepare_final_model(generation, [])?;
        let cycle = MajorCycle::initial(self.problem, model, self.normal_storage(plan.residency)?)?;
        let major = self.reconcile(cycle, true, seeded, last, plan)?;
        self.retain(&major, pass)?;
        Ok(major)
    }

    /// The residual of the start model, made physical with `unseeded`'s
    /// weights under `response`: a refresh of the initial pass, whose PSF,
    /// weights and sums carry over.
    fn reseed(
        &mut self,
        unseeded: Major,
        response: MinorCycleImageResponse,
        last: bool,
    ) -> Result<Major, ImagingError> {
        let start = self
            .start_model
            .clone()
            .expect("a run that reseeds has a start model");
        // The retained charge covers the normal state, and the empty model,
        // which the seeded generation replaces at the same size.
        let (normal_state, empty) = unseeded.completion.into_parts();
        drop(empty);
        let seeding = self.seeding_bytes();
        let Admitted {
            value: plan,
            reservation: mut pass,
        } = self.admit_pass(
            ModeSet::DATA,
            true,
            self.write_bytes(last) + seeding,
            self.writes(last),
        )?;
        let generation = {
            let _seeding = pass.split(seeding);
            self.physical_generation(&start, &normal_state, response)?
        };
        let model = self.lifecycle.prepare_final_model(generation, [])?;
        let cycle = MajorCycle::refresh(
            self.problem,
            normal_state,
            model,
            self.normal_storage(plan.residency)?,
        )?;
        let major = self.reconcile(cycle, false, true, last, plan)?;
        self.retain(&major, pass)?;
        Ok(major)
    }

    /// The first generation from `start`, its flat-noise samples divided by
    /// the PB of `normal`'s weights where the PB supports them and zero
    /// elsewhere (`divideModelByWeight`), as the minor cycle maps solved
    /// values to the physical model.
    fn physical_generation(
        &self,
        start: &StartModel,
        normal: &FinalNormalState,
        response: MinorCycleImageResponse,
    ) -> Result<ModelGeneration, ImagingError> {
        let [nx, ny] = start.image_size();
        let polarizations = start.polarizations();
        let apparent = start.main_samples().map_err(ImagingError::StartModel)?;
        let units = ImagingError::StartModelUnits;
        let mut physical = Vec::with_capacity(apparent.len());
        // Canonical order: spectral plane, polarization, y, x (x fastest);
        // the normal state's planes are x-major (x · ny + y).
        for (plane, samples) in apparent.chunks(nx * ny).enumerate() {
            let weights = normal.read_plane(0, plane / polarizations, plane % polarizations)?;
            let sensitivity = weights.read_sensitivity()?;
            let bound = MosaicSensitivity::new(&sensitivity)
                .and_then(|bound| bound.with_normal_sum_weight(weights.sum_weight()))
                .map_err(units)?;
            for y in 0..ny {
                for x in 0..nx {
                    let value = bound
                        .apparent_to_physical(
                            f64::from(samples[y * nx + x]),
                            x * ny + y,
                            response.normalization(),
                            response.policy(),
                        )
                        .map_err(units)?;
                    physical.push(value as f32);
                }
            }
        }
        Ok(self.lifecycle.initial_from_start_model(&physical)?)
    }

    /// What seeding the model from the start model holds besides the model
    /// ([`StartModel::seeding_bytes`], and its conversion when it is made
    /// physical); 0 without one.
    pub(super) fn seeding_bytes(&self) -> u64 {
        self.start_model.as_ref().map_or(0, |start| {
            start.seeding_bytes()
                + if self.reseed.is_some() {
                    start.conversion_bytes()
                } else {
                    0
                }
        })
    }
}

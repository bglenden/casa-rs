// SPDX-License-Identifier: LGPL-3.0-or-later
//! The initial major cycle, from an empty model or a start model (#841
//! Phase 6b, #970 review).
//!
//! A start model is the model's first generation, in the units a run
//! publishes its `.model` in. Under a direction-independent response, or
//! flat sky, those are the physical units the pass predicts from, and the
//! initial pass grids the residual of the start model. Under flat noise or
//! pbsquare (a mosaic's or AW kernel set's `normtype`), `.model` is the
//! physical model times the PB (flat noise) or PB² (pbsquare), and CASA
//! divides a model through the PB or PB² of the weights just before it
//! predicts (`SIImageStore::divideModelByWeight`), once `makePSF` has
//! formed them. Such a run takes CASA's order: the initial pass forms the
//! PSF, weights and sums with no model, and the start model, made physical
//! with those weights, is the first generation of a refresh of that pass's
//! residual. That costs one more pass over the data, for such runs only.
//! Either way, a run started from another run's model with no minor cycle
//! gives that run's products (the T1 laws; CASA 6.7.6 does the same on a
//! 14-pointing mosaic).

use casa_core::Correlation;
use casa_imaging_deconvolution::{FinalNormalState, MajorCycle, MajorCycleError, ModelGeneration};
use casa_imaging_model::{CompiledProblem, ProductNormalization, ReconstructionBasis};
use casa_imaging_operator::{Basis, ModeSet, PlaneRange};
use casa_imaging_products::{MinorCycleImageResponse, MosaicSensitivity};
use casa_imaging_runtime::pass::{NativeBlock, SampleContent};
use casa_imaging_runtime::{Admitted, Demand, Reservation, admit};

use super::{Major, Run};
use crate::compile::StartModel;
use crate::imaging::ImagingError;

/// The response a start model is made physical with before the first
/// major cycle predicts it: a flat-noise or pbsquare one, through the PB
/// (flat noise) or PB² (pbsquare); `None` without a start model, or when
/// its units are physical already.
pub(super) fn reseed_response(
    start_model: Option<&StartModel>,
    response: Option<MinorCycleImageResponse>,
) -> Option<MinorCycleImageResponse> {
    start_model.and(response).filter(|response| {
        matches!(
            response.normalization(),
            ProductNormalization::FlatNoise | ProductNormalization::PbSquare
        )
    })
}

/// Each seeded image plane's flux in a start model's `samples`, in `.model`'s
/// units, as `SIImageStore::getModelFlux` sums it; a Taylor family's is its
/// first term's, under its first channel. Sorted by `[domain, channel,
/// polarization]`.
pub(super) fn plane_fluxes(
    problem: &CompiledProblem,
    start: &StartModel,
    samples: &[f32],
    domain: usize,
) -> Vec<([usize; 3], f64)> {
    let polarizations = start.polarizations();
    let [width, height] = start.image_size();
    samples
        .chunks_exact(width * height)
        .enumerate()
        .filter_map(|(index, plane)| {
            let mut key = plane_key(problem, polarizations, index)?;
            key[0] = domain;
            Some((key, plane.iter().map(|value| f64::from(*value)).sum()))
        })
        .collect()
}

/// The `[domain, channel, polarization]` of a start model's plane `index`;
/// `None` for a Taylor term after the first.
fn plane_key(problem: &CompiledProblem, polarizations: usize, index: usize) -> Option<[usize; 3]> {
    let cube = matches!(
        problem.reconstruction().basis(),
        ReconstructionBasis::ChannelLocal { .. }
    );
    let (coefficient, polarization) = (index / polarizations, index % polarizations);
    (cube || coefficient == 0).then_some([0, if cube { coefficient } else { 0 }, polarization])
}

/// Bytes held beside the model while the source reads one domain at a time.
pub(super) fn model_bytes(
    main: Option<&StartModel>,
    outliers: &[Option<StartModel>],
    reseed: bool,
) -> Option<u64> {
    let main =
        main.map(|start| start.seeding_bytes() + if reseed { start.conversion_bytes() } else { 0 });
    main.into_iter()
        .chain(outliers.iter().flatten().map(StartModel::seeding_bytes))
        .max()
}

impl Run<'_> {
    pub(super) fn explain_empty_taylor_pass(&mut self, error: ImagingError) -> ImagingError {
        if !matches!(
            error,
            ImagingError::MajorCycle(MajorCycleError::IncompleteCoverage)
        ) {
            return error;
        }
        self.first_flagged_taylor_correlation()
            .map_or(error, |correlation| {
                ImagingError::FirstTaylorCorrelationFlagged { correlation }
            })
    }

    /// Read only flags and weights after an empty initial Taylor pass.
    /// `FTMachine::setSpectralFlag` deliberately propagates any selected
    /// correlation flag to the whole channel for ordinary Stokes imaging.
    /// Report the specific pattern only when the first correlation has
    /// no unflagged sample and another one has unflagged positive weight.
    fn first_flagged_taylor_correlation(&mut self) -> Option<Correlation> {
        let operator = &self.domains.first()?.operator;
        if !matches!(operator.basis(), Basis::Taylor { .. }) {
            return None;
        }
        let correlations = operator.polarization().correlations();
        let [first, _, ..] = correlations else {
            return None;
        };
        let first = *first;
        let count = correlations.len();
        let shape = self.source.maximum_block();
        let _charge = admit(
            &self.host,
            &self.policy,
            &Demand {
                phase: "Taylor flag diagnosis",
                memory: shape.bytes(),
            },
        )
        .ok()?;
        let mut block = NativeBlock::with_capacity(shape);
        self.source
            .begin(PlaneRange::new(0, 1), false, SampleContent::FlagsAndWeights)
            .ok()?;
        let mut other_has_weight = false;
        while !self.cancel.is_cancelled() && self.source.fill(&mut block).ok()? {
            for index in 0..block.len() {
                let row = block.row(0, index);
                if row.row_flag || row.context.antennas[0] == row.context.antennas[1] {
                    continue;
                }
                for (flags, weights) in row
                    .flags
                    .chunks_exact(count)
                    .zip(row.weights.chunks_exact(count))
                {
                    if !flags[0] {
                        return None;
                    }
                    other_has_weight |= flags[1..]
                        .iter()
                        .zip(&weights[1..])
                        .any(|(&flag, &weight)| !flag && weight.is_finite() && weight > 0.0);
                }
            }
        }
        (other_has_weight && !self.cancel.is_cancelled()).then_some(first)
    }

    /// The initial major cycle: data and PSF, from the start model when there
    /// is one (its residual) and from an empty model otherwise; it writes
    /// visibilities when it is also final. A run that reads its PSF or
    /// residual forms what it does not read ([`super::continuation`]).
    pub(super) fn initial(&mut self, last: bool, mvc_start: bool) -> Result<Major, ImagingError> {
        if mvc_start {
            self.measure_row_work()?;
            return self.psf_pass();
        }
        match (self.forms.psf, self.forms.residual) {
            (true, true) => {}
            (true, false) => {
                self.measure_row_work()?;
                return self.psf_pass();
            }
            (false, true) => {
                self.measure_row_work()?;
                let read = self.read_existing(self.reseed.is_none())?;
                if let Some(response) = self.reseed {
                    return self.reseed(read, response, last);
                }
                return self.refresh(
                    read.completion,
                    (Vec::new(), Vec::new(), Reservation::none()),
                    last,
                    true,
                );
            }
            (false, false) => return self.read_existing(true),
        }
        self.measure_row_work()?;
        match self.reseed {
            Some(response) => {
                let unseeded = self.initial_pass(false, false)?;
                self.reseed(unseeded, response, last)
            }
            None => self.initial_pass(self.has_model_seed(), last),
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
        let generation = if seeded {
            let scope = pass.split(seeding);
            tracing::debug!(
                target: "casa_imaging::model_seed",
                admitted_bytes = scope.memory(),
                "seed the first model generation"
            );
            self.seed_generation()?
        } else {
            self.lifecycle.initial_empty()?
        };
        let model = self.lifecycle.prepare_final_model(generation, [])?;
        let cycle = MajorCycle::initial(self.problem, model, self.normal_storage(plan.residency)?)?;
        let major = self.reconcile(cycle, self.initial_modes(), seeded, last, plan)?;
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
        let major = self.reconcile(cycle, ModeSet::DATA, true, last, plan)?;
        self.retain(&major, pass)?;
        Ok(major)
    }

    /// The first generation from `start`, its apparent samples divided
    /// through the PB (flat noise) or PB² (pbsquare) of `normal`'s weights
    /// where that supports them and zero elsewhere (`divideModelByWeight`),
    /// as the minor cycle maps solved values to the physical model.
    pub(super) fn physical_generation(
        &mut self,
        start: &StartModel,
        normal: &FinalNormalState,
        response: MinorCycleImageResponse,
    ) -> Result<ModelGeneration, ImagingError> {
        let [nx, ny] = start.image_size();
        let polarizations = start.polarizations();
        let apparent = start.main_samples().map_err(ImagingError::StartModel)?;
        let units = ImagingError::StartModelUnits;
        let mut physical = Vec::with_capacity(apparent.len());
        let mut fluxes = Vec::new();
        // Canonical order: spectral plane, polarization, y, x (x fastest);
        // the normal state's planes are x-major (x · ny + y). Every Taylor
        // term divides by term 0's weights of its polarization
        // (`SIImageStoreMultiTerm::divideModelByWeight`).
        let taylor = (normal.basis() == casa_imaging_deconvolution::NormalBasis::Taylor)
            .then(|| normal.read_window(normal.slab().core_range()))
            .transpose()?;
        for (plane, samples) in apparent.chunks(nx * ny).enumerate() {
            let principal = taylor.as_ref().map(|window| {
                window
                    .normal_moment(0, plane % polarizations)
                    .expect("the zeroth normal moment")
            });
            let (sensitivity, sum_weight) = match principal {
                Some(moment) => (
                    std::borrow::Cow::Borrowed(moment.sensitivity()),
                    moment.sum_weight(),
                ),
                None => {
                    let weights =
                        normal.read_plane(0, plane / polarizations, plane % polarizations)?;
                    (weights.read_sensitivity()?, weights.sum_weight())
                }
            };
            // `SIImageStore::divideModelByWeight` skips a plane whose PB's
            // peak is zero: no sample reached it, or its rows all miss the
            // grid (an AW weight image with no sum, which
            // `divideImageByWeightVal` zeroes). Its model stays as it is.
            let key = plane_key(self.problem, polarizations, plane);
            let bound = MosaicSensitivity::try_new(&sensitivity).map_err(units)?;
            let Some(bound) = bound.filter(|_| sum_weight > 0.0) else {
                physical.extend_from_slice(samples);
                fluxes.extend(
                    key.map(|key| (key, samples.iter().map(|value| f64::from(*value)).sum())),
                );
                continue;
            };
            let bound = bound.with_normal_sum_weight(sum_weight).map_err(units)?;
            let mut flux = 0.0;
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
                    // `.model` keeps what `divideModelByWeight` cuts: its
                    // multiplication back can't restore it.
                    if value != 0.0 {
                        flux += f64::from(samples[y * nx + x]);
                    }
                }
            }
            fluxes.extend(key.map(|key| (key, flux)));
        }
        self.start_model_flux = fluxes;
        Ok(self.lifecycle.initial_from_start_model(&physical)?)
    }

    /// The initial generation from each field's existing image, retaining
    /// at most one field's samples beside the model. A field with no
    /// saved model supplies zeros in its own span of the compiled grid.
    pub(super) fn seed_generation(&mut self) -> Result<ModelGeneration, ImagingError> {
        if self.outlier_models.is_empty() {
            let Some(start) = &self.start_model else {
                return Ok(self.lifecycle.initial_empty()?);
            };
            let samples = start.main_samples().map_err(ImagingError::StartModel)?;
            self.start_model_flux = plane_fluxes(self.problem, start, &samples, 0);
            return Ok(self.lifecycle.initial_from_start_model(&samples)?);
        }
        let target = self.problem.model_lifecycle().target();
        let mut domains = target
            .domains()
            .iter()
            .zip(
                std::iter::once(self.start_model.as_ref())
                    .chain(self.outlier_models.iter().map(Option::as_ref)),
            )
            .enumerate();
        let mut range = 0..0;
        let mut samples = Vec::new();
        let mut fluxes = Vec::new();
        let generation = self.lifecycle.initial_from_fn(|index| {
            if index == range.end {
                let (domain, (shape, seed)) = domains.next().expect("seed for each compiled field");
                let [nx, ny] = shape.pixels();
                let count = nx * ny * target.polarizations() * target.coefficients();
                range = index..index + count;
                // Drop the previous field before opening this one's image.
                drop(std::mem::take(&mut samples));
                if let Some(seed) = seed {
                    samples = seed.main_samples().map_err(ImagingError::StartModel)?;
                    if samples.len() != count {
                        return Err(ImagingError::Model(
                            casa_imaging_deconvolution::ModelLifecycleError::SampleCountMismatch {
                                expected: count,
                                actual: samples.len(),
                            },
                        ));
                    }
                    fluxes.extend(plane_fluxes(self.problem, seed, &samples, domain));
                }
            }
            Ok(samples.get(index - range.start).copied().unwrap_or(0.0))
        })?;
        self.start_model_flux = fluxes;
        Ok(generation)
    }

    pub(super) fn has_model_seed(&self) -> bool {
        self.start_model.is_some() || self.outlier_models.iter().any(Option::is_some)
    }

    /// What seeding holds besides the model, including conversion to
    /// physical samples when necessary.
    pub(super) fn seeding_bytes(&self) -> u64 {
        model_bytes(
            self.start_model.as_ref(),
            &self.outlier_models,
            self.reseed.is_some(),
        )
        .unwrap_or(0)
    }
}

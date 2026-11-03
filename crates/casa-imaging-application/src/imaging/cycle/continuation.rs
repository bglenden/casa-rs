// SPDX-License-Identifier: LGPL-3.0-or-later
//! The first normal state of a run that does not form all of it (tclean's
//! `calcpsf=False` and `calcres=False`, `task_tclean.py`).
//!
//! CASA forms the PSF and its sums of weights in `makePSF` and the residual
//! in a major cycle before the first minor cycle; where the flags skip
//! either, the images an earlier run left at `imagename` stand. So here:
//!
//! | `calcpsf` | `calcres` | first normal state |
//! |---|---|---|
//! | true | true | one pass of data and PSF (the initial major cycle) |
//! | true | false | a pass of the PSF alone, with the residual read (or zero, with no iterations and no residual) |
//! | false | true | the PSF and sums read, then a residual pass (the initial major cycle) |
//! | false | false | everything read; no pass |
//!
//! A residual read is not a major cycle, so CASA's `nmajordone` does not
//! count it. A run of no iterations that writes the model predicts it in a
//! pass of its own (`predictModel`), whose residual is not CASA's: the one
//! read stands ([`Run::reread_residual`]).

use casa_images::ImageError;
use casa_imaging_deconvolution::{MajorCycle, MajorCycleCompletion, PassImages};
use casa_imaging_operator::{Basis, ModeSet};
use casa_imaging_runtime::pass::Residency;
use casa_imaging_runtime::{Admitted, Cancelled};

use super::{Major, Run};
use crate::imaging::ImagingError;
use crate::restart::ExistingProducts;

/// What a run forms of its first normal state: the PSF and sums of weights
/// (`calcpsf`) and the residual (`calcres`); what it does not form, it
/// reads.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct Forms {
    /// The PSF, its sums of weights and the weight image.
    pub(crate) psf: bool,
    /// The residual.
    pub(crate) residual: bool,
}

impl Forms {
    /// The run-summary phase that forms the first normal state.
    pub(crate) const fn initial_phase(self) -> &'static str {
        match (self.psf, self.residual) {
            (_, true) => "major cycle 1",
            (true, false) => "PSF",
            (false, false) => "existing products",
        }
    }

    /// Whether a run that does not clean refreshes its first normal state:
    /// it reads the PSF and forms the residual, or it reads (or has no)
    /// residual and `writes` the model.
    pub(crate) const fn refreshes_without_cleaning(self, writes: bool) -> bool {
        (!self.psf && self.residual) || (writes && !(self.psf && self.residual))
    }
}

/// Set the residual of `images`, from a pass that formed the PSF alone:
/// the earlier run's, as `existing` reads it, or zero without one.
pub(super) fn set_residual(
    existing: Option<&ExistingProducts>,
    images: &mut PassImages,
    terms: usize,
) -> Result<(), ImageError> {
    match existing {
        Some(existing) => existing.fill_residual(images),
        None => {
            let cells = images.shape[0] * images.shape[1];
            images.residual =
                vec![0.0; images.channels.len() * terms * images.polarizations * cells];
            Ok(())
        }
    }
}

impl Run<'_> {
    /// The PSF, sums of weights and residual an earlier run left, read as
    /// the first normal state of the start model (the earlier model) or of
    /// an empty one: no pass. It is charged as the initial pass would be,
    /// whose normal state it forms, and reads a window of channels at a
    /// time beside it.
    pub(super) fn read_existing(&mut self) -> Result<Major, ImagingError> {
        let existing = self
            .existing
            .clone()
            .expect("a run that reads its PSF has the earlier run's products");
        let seeding = self.seeding_bytes();
        let Admitted {
            value: plan,
            reservation: mut charge,
        } = self.admit_pass(
            self.initial_modes(),
            false,
            self.lifecycle.resident_bytes() + seeding,
            false,
        )?;
        let generation = match &self.start_model {
            Some(start) => {
                let _seeding = charge.split(seeding);
                let samples = start.main_samples().map_err(ImagingError::StartModel)?;
                self.lifecycle.initial_from_start_model(&samples)?
            }
            None => self.lifecycle.initial_empty()?,
        };
        let model = self.lifecycle.prepare_final_model(generation, [])?;
        let mut cycle =
            MajorCycle::initial(self.problem, model, self.normal_storage(plan.residency)?)?;
        let window = self.read_window(plan.residency);
        {
            let _reading = self.admit(
                "existing products",
                existing.reading_bytes(window) + existing.images_bytes(window),
            )?;
            let (_, mut state) = cycle.parts();
            for channels in windows(self.total_channels(), window) {
                if self.cancel.is_cancelled() {
                    return Err(Cancelled.into());
                }
                state.append(
                    existing
                        .pass_images(channels)
                        .map_err(ImagingError::Existing)?,
                )?;
            }
        }
        let major = Major {
            completion: cycle.finish_read()?,
            visibility: None,
        };
        self.retain(&major, charge)?;
        Ok(major)
    }

    /// The first normal state of a run that forms the PSF and reads the
    /// residual: a pass of the PSF (and weight image) alone, whose residual
    /// is the earlier run's or zero ([`set_residual`]).
    pub(super) fn psf_pass(&mut self) -> Result<Major, ImagingError> {
        let modes = ModeSet {
            data: false,
            psf: true,
            weight: self.weight_image,
        };
        let seeding = self.seeding_bytes();
        let Admitted {
            value: plan,
            reservation: mut pass,
        } = self.admit_pass(
            modes,
            false,
            self.lifecycle.resident_bytes() + seeding,
            false,
        )?;
        let generation = match &self.start_model {
            Some(start) => {
                let _seeding = pass.split(seeding);
                let samples = start.main_samples().map_err(ImagingError::StartModel)?;
                self.lifecycle.initial_from_start_model(&samples)?
            }
            None => self.lifecycle.initial_empty()?,
        };
        let model = self.lifecycle.prepare_final_model(generation, [])?;
        let cycle = MajorCycle::initial(self.problem, model, self.normal_storage(plan.residency)?)?;
        // The residual each wave takes, read beside the pass.
        let window = self.read_window(plan.residency);
        let reading = self.existing.as_ref().map_or_else(
            || self.residual_bytes(window),
            |existing| existing.reading_bytes(window) + existing.images_bytes(window),
        );
        let _reading = self.admit("existing residual", reading)?;
        let major = self.reconcile(cycle, modes, false, false, plan)?;
        self.retain(&major, pass)?;
        Ok(major)
    }

    /// `completion` with its residual read again from the earlier run's
    /// products, its PSF, sums and model kept: CASA's `predictModel` leaves
    /// the residual of a `calcres=False` run as it was. A run that read no
    /// residual publishes none, and keeps `completion` as it is.
    pub(super) fn reread_residual(
        &mut self,
        completion: MajorCycleCompletion,
    ) -> Result<Major, ImagingError> {
        let Some(existing) = self
            .existing
            .clone()
            .filter(ExistingProducts::reads_residual)
        else {
            return Ok(Major {
                completion,
                visibility: None,
            });
        };
        let (normal_state, model) = completion.into_parts();
        let total = self.total_channels();
        let polarizations = self
            .problem
            .reconstruction()
            .polarization()
            .coordinates()
            .len();
        let mut principal = Vec::with_capacity(total * polarizations);
        for channel in 0..total {
            for pol in 0..polarizations {
                principal.push(
                    normal_state
                        .read_plane(0, channel, pol)?
                        .published_sum_weight(),
                );
            }
        }
        let Admitted {
            value: plan,
            reservation: charge,
        } = self.admit_pass(ModeSet::DATA, true, 0, false)?;
        let model = self.lifecycle.prepare_final_model(model, [])?;
        let mut cycle = MajorCycle::refresh(
            self.problem,
            normal_state,
            model,
            self.normal_storage(plan.residency)?,
        )?;
        let window = self.read_window(plan.residency);
        {
            let _reading = self.admit(
                "existing residual",
                existing.reading_bytes(window) + existing.images_bytes(window),
            )?;
            let (_, mut state) = cycle.parts();
            for channels in windows(total, window) {
                let first = channels.start;
                let images = existing
                    .residual_images(channels, |channel, pol| {
                        principal[(first + channel) * polarizations + pol]
                    })
                    .map_err(ImagingError::Existing)?;
                state.append(images)?;
            }
        }
        let major = Major {
            completion: cycle.finish_read()?,
            visibility: None,
        };
        self.retain(&major, charge)?;
        Ok(major)
    }

    /// The output channels of the first normal state: a channel-local
    /// basis's channels, one otherwise.
    fn total_channels(&self) -> usize {
        self.problem.geometry().spectral().output_channels()
    }

    /// The channels read at once: a channel-local state's paged window
    /// under `residency`, the whole state otherwise.
    fn read_window(&self, residency: Residency) -> usize {
        match (self.domains[0].operator.basis(), residency) {
            (Basis::ChannelLocal { .. }, Residency::Waves { planes_per_wave }) => {
                planes_per_wave as usize
            }
            _ => self.total_channels(),
        }
    }

    /// Bytes a zero residual of `channels` channels holds.
    fn residual_bytes(&self, channels: usize) -> u64 {
        let [nx, ny] = self.problem.geometry().domains()[0].shape().pixels();
        let terms = self.domains[0].operator.basis().data_terms();
        let polarizations = self
            .problem
            .reconstruction()
            .polarization()
            .coordinates()
            .len();
        (channels * terms * polarizations * nx * ny * 4) as u64
    }
}

/// `0..total` in consecutive ranges of at most `window`.
fn windows(total: usize, window: usize) -> impl Iterator<Item = std::ops::Range<usize>> {
    let window = window.max(1);
    (0..total)
        .step_by(window)
        .map(move |start| start..(start + window).min(total))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_first_normal_state_is_formed_as_the_flags_ask() {
        let forms = |psf, residual| Forms { psf, residual };
        assert_eq!(forms(true, true).initial_phase(), "major cycle 1");
        assert_eq!(forms(false, true).initial_phase(), "major cycle 1");
        assert_eq!(forms(true, false).initial_phase(), "PSF");
        assert_eq!(forms(false, false).initial_phase(), "existing products");
        assert!(!forms(true, true).refreshes_without_cleaning(true));
        assert!(forms(false, true).refreshes_without_cleaning(false));
        assert!(forms(true, false).refreshes_without_cleaning(true));
        assert!(!forms(true, false).refreshes_without_cleaning(false));
        assert!(forms(false, false).refreshes_without_cleaning(true));
        assert_eq!(windows(7, 3).collect::<Vec<_>>(), [0..3, 3..6, 6..7]);
        let mut one = windows(1, 4);
        assert_eq!((one.next(), one.next()), (Some(0..1), None));
    }
}

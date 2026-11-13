// SPDX-License-Identifier: LGPL-3.0-or-later
//! The beam a minor cycle gives a plane with none of its own.

use std::borrow::Cow;
use std::collections::BTreeMap;

use casa_imaging_deconvolution::{
    CASA_TINY_PSF_BEAM_RAD, FinalNormalState, ModelGeneration, RestoringBeam,
};

use super::{
    MinorCycleRunError, PsfCache, RetainedPlane, normalise, plane_shape, psf_peak_value, read_psf,
};

/// Give a plane whose PSF fit was refused its domain's largest beam by area
/// (`SIImageStore::makeImageBeamSet`), as the products do. CASA's cube takes the
/// largest of each sub-cube its memory splits it into, so its answer depends on
/// the machine; this is its answer for one. A domain with no fitted plane takes
/// CASA's tiny circular beam. A plane is read again only when its beam changes,
/// which is rarely.
pub(super) fn settle_refused_fits(
    normal: &FinalNormalState,
    base: &ModelGeneration,
    taylor: bool,
    cache: &mut PsfCache,
    preserve_main: bool,
    retained: Option<&RetainedPlane>,
) -> Result<(), MinorCycleRunError> {
    let mut largest = BTreeMap::<usize, RestoringBeam>::new();
    for (key, summary) in cache.summaries.iter().filter(|(_, s)| !s.fit_refused()) {
        let beam = summary.beam();
        let area = |beam: RestoringBeam| beam.major_fwhm_rad() * beam.minor_fwhm_rad();
        largest
            .entry(key.domain)
            .and_modify(|most| {
                if area(beam) > area(*most) {
                    *most = beam;
                }
            })
            .or_insert(beam);
    }
    let reused = cache.reuses_single_psf;
    for (key, summary) in &mut cache.summaries {
        if preserve_main && key.domain == 0 {
            continue;
        }
        if !summary.fit_refused() {
            continue;
        }
        let increment = base
            .shape()
            .direction(key.domain)
            .expect("domain direction")
            .increment_rad();
        let beam = match largest.get(&key.domain).copied() {
            Some(beam) => beam,
            None => {
                let width = CASA_TINY_PSF_BEAM_RAD / increment[0].abs();
                RestoringBeam::new(width, width, 0.0)
                    .map_err(casa_imaging_deconvolution::Error::from)?
            }
        };
        if summary.needs_domain_beam() || summary.beam() != beam {
            let shape = plane_shape(normal, *key);
            let (psf, peak) = match retained.filter(|plane| plane.key == *key) {
                Some(plane) => (Cow::Borrowed(plane.psf[0].as_slice()), plane.psf_peak),
                None => {
                    let psf = read_psf(normal, taylor, *key)?;
                    let peak = psf_peak_value(&psf, shape)?;
                    (Cow::Owned(normalise(psf, peak)), peak)
                }
            };
            let cell = [1.0, (increment[1] / increment[0]).abs()];
            *summary =
                summary.settle_domain_beam(&psf, shape, beam, cell, reused.then_some(peak))?;
        }
    }
    Ok(())
}

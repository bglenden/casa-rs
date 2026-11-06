// SPDX-License-Identifier: LGPL-3.0-or-later
//! The beam a minor cycle gives a plane with none of its own.

use std::collections::BTreeMap;

use casa_imaging_deconvolution::{
    CASA_TINY_PSF_BEAM_RAD, FinalNormalState, ModelGeneration, RestoringBeam,
};

use super::{MinorCycleRunError, PsfCache, normalise, plane_shape, psf_peak_value, read_psf};

/// Give each plane of the cache with no beam of its own, its PSF peaking off
/// its centre, its domain's largest beam by area
/// (`SIImageStore::makeImageBeamSet`), as the products do. CASA's cube takes the
/// largest of each sub-cube its memory splits it into, so its answer depends on
/// the machine; this is its answer for one. A domain with no fitted plane takes
/// CASA's tiny circular beam. A plane is read again only when its beam changes,
/// which is rarely.
pub(super) fn settle_off_centre(
    normal: &FinalNormalState,
    base: &ModelGeneration,
    taylor: bool,
    cache: &mut PsfCache,
    preserve_main: bool,
) -> Result<(), MinorCycleRunError> {
    let mut largest = BTreeMap::<usize, RestoringBeam>::new();
    for (key, summary) in cache.summaries.iter().filter(|(_, s)| !s.off_centre()) {
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
    for (key, summary) in &mut cache.summaries {
        if preserve_main && key.domain == 0 {
            continue;
        }
        if !summary.off_centre() {
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
            let psf = read_psf(normal, taylor, *key)?;
            let peak = psf_peak_value(&psf, shape)?;
            let cell = [1.0, (increment[1] / increment[0]).abs()];
            *summary = summary.settle_domain_beam(&normalise(psf, peak), shape, beam, cell)?;
        }
    }
    Ok(())
}

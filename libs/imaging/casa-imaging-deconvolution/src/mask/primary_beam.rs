// SPDX-License-Identifier: LGPL-3.0-or-later

//! A static mask cut to the primary beam: tclean's `usemask='pb'`, every
//! pixel whose `.pb` exceeds `pbmask`, and a named user mask under a
//! `pbmask` above zero, kept where the `.pb` exceeds it
//! (`SynthesisDeconvolver::setupMask`, `SDMaskHandler::makePBMask`).
//!
//! tclean forms the mask once, at the run's first boundary, from the `.pb`
//! of every channel and polarization, so it holds a plane for each; the
//! next boundary keeps it ([`ReconstructionMaskPlan::next_cycle`]). The
//! owner of the minor cycle reads each plane's beam ([`MaskBeamRead`]): the
//! weight image's under a response, the telescope's analytic beam
//! otherwise.
//!
//! [`ReconstructionMaskPlan::next_cycle`]: super::ReconstructionMaskPlan::next_cycle

use std::sync::Arc;

use super::auto_planes::MaskPlaneKey;
use super::{MaskError, MaskPlanes, ReconstructionMask};

/// The pixels of one plane, x-major, whose `.pb` exceeds a level (CASA's
/// Float `pb > pbmask`); `None` for a run without a `.pb`. The plane's
/// channel counts from the normal state's first, and a Taylor family is
/// one plane, its principal beam's.
pub type MaskBeamRead<'a, E> =
    dyn Fn(MaskPlaneKey, f32) -> Result<Option<Vec<bool>>, E> + Sync + 'a;

/// Whether `level` is a `.pb` level tclean takes for `pbmask`: in `[0, 1)`
/// (`SynthesisParamsDeconv::verify`).
pub(super) fn valid_level(level: f64) -> bool {
    (0.0..1.0).contains(&level)
}

/// `mask`, a static mask of domain `domain`, cut to the pixels whose `.pb`
/// exceeds `level` in each of `planes`' channels and polarizations, as
/// `beam` reads them (`iif(pb > pbmask, mask, 0)`). The new support is
/// written in place, plane after plane; it holds one plane of the beam at
/// a time beside it.
///
/// # Errors
///
/// [`MaskError::NoPrimaryBeam`] for a run without a `.pb`, as tclean's
/// `makePBMask` refuses one; [`MaskError::ShapeMismatch`] when `mask` or a
/// beam plane does not fit the planes; or what `beam` returns.
pub(super) fn cut_to_beam<E>(
    mask: &ReconstructionMask,
    (domain, planes): (usize, MaskPlanes),
    level: f64,
    beam: Option<&MaskBeamRead<'_, E>>,
) -> Result<ReconstructionMask, E>
where
    E: From<MaskError>,
{
    let beam = beam.ok_or(MaskError::NoPrimaryBeam)?;
    let cells = mask.shape[0] * mask.shape[1];
    // One allocation, written in place and shared as it is.
    let mut support = (0..planes.count() * cells)
        .map(|_| false)
        .collect::<Arc<[bool]>>();
    let out = Arc::get_mut(&mut support).expect("a support no one else holds yet");
    let mut planes_out = out.chunks_mut(cells);
    for channel in 0..planes.channels() {
        for polarization in 0..planes.polarizations() {
            let within = mask
                .plane(channel, polarization)
                .ok_or(MaskError::ShapeMismatch)?;
            let kept = beam(
                MaskPlaneKey {
                    domain,
                    channel,
                    polarization,
                },
                level as f32,
            )?
            .ok_or(MaskError::NoPrimaryBeam)?;
            if kept.len() != cells {
                return Err(MaskError::ShapeMismatch.into());
            }
            let plane = planes_out.next().expect("a support plane per plane");
            for ((set, within), kept) in plane.iter_mut().zip(within).zip(&kept) {
                *set = *within && *kept;
            }
        }
    }
    Ok(ReconstructionMask::from_plane_support(
        mask.coordinate,
        mask.shape,
        planes,
        support,
    )?)
}

#[cfg(test)]
mod tests {
    use super::*;
    use casa_imaging_model::{DirectionCoordinateSpec, DirectionFrame, Projection, SkyDirection};

    fn grid() -> DirectionCoordinateSpec {
        DirectionCoordinateSpec::new(
            Projection::Sin,
            SkyDirection::new(DirectionFrame::J2000, 1.0, 0.5),
            [1.0, 1.0],
            [-1.0e-4, 1.0e-4],
            [[1.0, 0.0], [0.0, 1.0]],
            [180.0, 90.0],
        )
    }

    /// `makePBMask`: each plane of the mask is the static mask where its
    /// own plane's `.pb` exceeds the level, and the level reaches the beam.
    #[test]
    fn each_plane_keeps_the_static_mask_where_its_beam_exceeds_the_level() {
        let shared = ReconstructionMask::from_reprojected_support(
            grid(),
            [2, 2],
            &[true, true, false, true],
        )
        .unwrap();
        let planes = MaskPlanes::new(2, 2).unwrap();
        // Plane (c, p)'s beam: its pixel i is kept when c + p ≤ i, and
        // only at the level 0.5.
        let beam = |key: MaskPlaneKey, level: f32| {
            assert_eq!((key.domain, level), (3, 0.5));
            Ok::<_, MaskError>(Some(
                (0..4)
                    .map(|index| key.channel + key.polarization <= index)
                    .collect(),
            ))
        };
        let cut = cut_to_beam(&shared, (3, planes), 0.5, Some(&beam)).unwrap();
        assert_eq!(cut.planes(), planes);
        assert_eq!(cut.plane(0, 0).unwrap(), [true, true, false, true]);
        assert_eq!(cut.plane(0, 1).unwrap(), [false, true, false, true]);
        assert_eq!(cut.plane(1, 0).unwrap(), [false, true, false, true]);
        assert_eq!(cut.plane(1, 1).unwrap(), [false, false, false, true]);
    }

    /// A run without a `.pb` cannot make one, as `makePBMask` refuses it.
    #[test]
    fn a_run_without_a_beam_is_refused() {
        let full = ReconstructionMask::full_plane(grid(), [2, 2]).unwrap();
        let none = |_: MaskPlaneKey, _: f32| Ok::<_, MaskError>(None);
        let planes = MaskPlanes::SHARED;
        assert_eq!(
            cut_to_beam(&full, (0, planes), 0.0, Some(&none)).unwrap_err(),
            MaskError::NoPrimaryBeam
        );
        assert_eq!(
            cut_to_beam::<MaskError>(&full, (0, planes), 0.0, None).unwrap_err(),
            MaskError::NoPrimaryBeam
        );
    }
}

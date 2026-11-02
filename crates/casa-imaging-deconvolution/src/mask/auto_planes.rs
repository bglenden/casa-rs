// SPDX-License-Identifier: LGPL-3.0-or-later

//! CASA's automatic multi-threshold mask over every plane of an image
//! domain: each channel and polarization masked on its own, as tclean masks
//! a cube's channels and an image's Stokes planes
//! (`SDMaskHandler::autoMaskByMultiThreshold`'s loop over channels, then
//! polarizations).
//!
//! tclean masks a cube block by block of channels, each block an image
//! store of its own (`SynthesisDeconvolver::executeCubeMinorCycle` with
//! `CubeMinorCycleAlgorithm`, blocks from `numblockchans` on one process),
//! and a continuum image whole. A block shares
//!
//! * the beam its masks are smoothed and pruned by: the common beam of the
//!   block's PSF beams (`CasaImageBeamSet::getCommonBeam`), each CASA's fit
//!   of its plane's PSF. A plane with no data holds the largest beam of
//!   the set (`SIImageStore::makeImageBeamSet`), which leaves the common
//!   beam as it is;
//! * whether its robust noise ignores the mask: when the inherited mask
//!   covers the block's whole first plane, every plane of the block takes
//!   Chauvenet statistics (`calcRobustImageStatistics`).
//!
//! Every plane shares the image's PSF sidelobe level, the largest of its
//! planes' (`SIImageStore::getPSFSidelobeLevel`, which the cube's
//! controller hands each block), and the planes of a channel share its
//! stop: `SDMaskHandler::skipChannels` reads the first polarization's
//! threshold and counts the mask over every polarization of the channel,
//! and a stopped channel skips all of them.
//!
//! A plane with no data has a zero residual in CASA: its statistics and
//! thresholds are zero and its mask stays as it was.

use std::ops::Range;

use casa_imaging_model::DirectionCoordinateSpec;
use rayon::prelude::*;

use super::auto_multithresh::{
    AutoMaskBeam, AutoMaskCycle, AutoMaskThreshold, AutoMultithreshControls,
    AutoMultithreshEvidence, PlaneInputs, plane_mask, skip_channel, validate_auto_controls,
    validate_beam,
};
use super::{MaskError, MaskPlanes, ReconstructionMask};
use crate::{PsfSummary, RestoringBeam};

/// One plane of an image domain: the domain, the channel within the normal
/// state (from its slab's first) and the polarization.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct MaskPlaneKey {
    /// The image domain's ordinal.
    pub domain: usize,
    /// The channel, counted from the normal state's first.
    pub channel: usize,
    /// The polarization plane.
    pub polarization: usize,
}

/// One plane an automatic mask is made from, as the minor cycle reads it.
#[derive(Debug, Clone, PartialEq)]
pub struct AutoMaskPlane {
    /// The residual in the units the minor cycle cleans, CASA's
    /// `.residual` plane, x-major.
    pub residual: Vec<f64>,
    /// The plane's valid pixels, x-major.
    pub valid: Vec<bool>,
}

/// A plane's residual and valid pixels; `None` for a plane with no data.
pub type AutoMaskRead<'a, E> =
    dyn Fn(MaskPlaneKey) -> Result<Option<AutoMaskPlane>, E> + Sync + 'a;

/// A plane's PSF measurements; `None` for a plane with no data.
pub type AutoMaskPsf<'a> = dyn Fn(MaskPlaneKey) -> Option<PsfSummary> + Sync + 'a;

/// What an automatic mask reads of one major cycle's planes, from the
/// owner of the minor cycle: each plane as [`AutoMaskRead`] gives it, read
/// once per boundary, and its PSF's measurements ([`AutoMaskPsf`]), which
/// do not change.
pub struct AutoMaskSource<'a, E> {
    /// Each plane's residual and valid pixels.
    pub read: &'a AutoMaskRead<'a, E>,
    /// Each plane's PSF measurements.
    pub psf: &'a AutoMaskPsf<'a>,
}

/// One image domain's automatic mask at one boundary: a row of CASA's
/// automask summary for each plane, channel-major.
#[derive(Debug, Clone, PartialEq)]
pub struct AutoMaskEvidence {
    polarizations: usize,
    planes: Box<[AutoMultithreshEvidence]>,
}

impl AutoMaskEvidence {
    /// The polarizations of each channel.
    #[must_use]
    pub const fn polarizations(&self) -> usize {
        self.polarizations
    }

    /// Every plane's row, channel after channel.
    #[must_use]
    pub fn planes(&self) -> &[AutoMultithreshEvidence] {
        &self.planes
    }

    /// The row of `channel` and `polarization`.
    #[must_use]
    pub fn plane(&self, channel: usize, polarization: usize) -> Option<&AutoMultithreshEvidence> {
        (polarization < self.polarizations)
            .then(|| self.planes.get(channel * self.polarizations + polarization))
            .flatten()
    }

    /// The first plane's row.
    #[must_use]
    pub fn primary(&self) -> &AutoMultithreshEvidence {
        &self.planes[0]
    }

    /// Whether each channel stops evolving at later boundaries.
    pub fn stopped_channels(&self) -> impl ExactSizeIterator<Item = bool> + '_ {
        self.planes
            .chunks(self.polarizations)
            .map(|channel| channel[0].channel_stopped)
    }
}

/// Where a domain's automatic mask stands at one boundary.
pub(super) struct AutoMaskState<'a> {
    pub(super) coordinate: DirectionCoordinateSpec,
    pub(super) controls: &'a AutoMultithreshControls,
    pub(super) completed_major_cycles: usize,
    pub(super) threshold_reached: bool,
    pub(super) previous: Option<&'a ReconstructionMask>,
    /// The channels an earlier boundary stopped; empty before the first.
    pub(super) stopped_channels: &'a [bool],
}

/// One channel's new supports, polarization after polarization, and rows.
struct ChannelMask {
    support: Vec<bool>,
    rows: Vec<AutoMultithreshEvidence>,
    positive: bool,
}

/// The automatic mask of every plane of `domain`, `shape` pixels by
/// `planes` channels and polarizations, from the mask it inherits.
/// Channels are masked side by side on the current rayon pool.
pub(super) fn auto_mask_domain<E>(
    state: &AutoMaskState<'_>,
    domain: usize,
    shape: [usize; 2],
    planes: MaskPlanes,
    source: &AutoMaskSource<'_, E>,
) -> Result<(ReconstructionMask, AutoMaskEvidence), E>
where
    E: From<MaskError> + Send,
{
    validate_auto_controls(state.controls)?;
    let cells = shape[0] * shape[1];
    if state.previous.is_some_and(|mask| {
        mask.shape != shape || mask.coordinate != state.coordinate || mask.planes != planes
    }) {
        return Err(MaskError::ShapeMismatch.into());
    }
    let key = |channel, polarization| MaskPlaneKey {
        domain,
        channel,
        polarization,
    };
    let psfs = (0..planes.channels())
        .flat_map(|channel| {
            (0..planes.polarizations()).map(move |polarization| (channel, polarization))
        })
        .map(|(channel, polarization)| (source.psf)(key(channel, polarization)))
        .collect::<Vec<_>>();
    let sidelobe = psfs
        .iter()
        .flatten()
        .map(PsfSummary::sidelobe)
        .fold(0.0_f64, f64::max);
    let nothing = vec![false; cells];
    let mut support = Vec::with_capacity(planes.count() * cells);
    let mut rows = Vec::with_capacity(planes.count());
    for block in channel_blocks(planes.channels(), shape) {
        let beam = block_beam(
            &psfs[block.start * planes.polarizations()..block.end * planes.polarizations()],
            sidelobe,
            state.coordinate,
        )?;
        let first_plane_full = state
            .previous
            .and_then(|mask| mask.plane(block.start, 0))
            .is_some_and(|plane| plane.iter().all(|set| *set));
        let channels = block
            .clone()
            .into_par_iter()
            .map(|channel| {
                let inherited = |polarization| {
                    state
                        .previous
                        .and_then(|mask| mask.plane(channel, polarization))
                        .unwrap_or(&nothing)
                };
                let cycle = AutoMaskCycle {
                    completed_major_cycles: state.completed_major_cycles,
                    threshold_reached: state.threshold_reached,
                    stopped: state
                        .stopped_channels
                        .get(channel)
                        .copied()
                        .unwrap_or(false),
                };
                let mut masked = ChannelMask {
                    support: Vec::with_capacity(planes.polarizations() * cells),
                    rows: Vec::with_capacity(planes.polarizations()),
                    positive: false,
                };
                for polarization in 0..planes.polarizations() {
                    let plane = (source.read)(key(channel, polarization))?;
                    let made = plane_mask(&PlaneInputs {
                        plane: plane.as_ref(),
                        inherited: inherited(polarization),
                        shape,
                        first_plane_full,
                        cycle,
                        controls: state.controls,
                        beam,
                    })?;
                    masked.support.extend(made.support);
                    masked.rows.push(made.evidence);
                    masked.positive |= made.positive;
                }
                let before = (0..planes.polarizations())
                    .map(|polarization| inherited(polarization).iter().filter(|set| **set).count())
                    .sum();
                let now = masked.support.iter().filter(|set| **set).count();
                let stopped = skip_channel(
                    before,
                    now,
                    masked.rows[0].threshold,
                    cycle,
                    state.controls,
                );
                for row in &mut masked.rows {
                    row.channel_stopped = stopped;
                }
                Ok::<_, E>(masked)
            })
            .collect::<Result<Vec<_>, E>>()?;
        report_block(state, &block, &channels, planes.polarizations());
        for channel in channels {
            support.extend(channel.support);
            rows.extend(channel.rows);
        }
    }
    Ok((
        ReconstructionMask::from_plane_support(state.coordinate, shape, planes, support)?,
        AutoMaskEvidence {
            polarizations: planes.polarizations(),
            planes: rows.into_boxed_slice(),
        },
    ))
}

/// The blocks of channels tclean masks a cube in on one process
/// (`SynthesisDeconvolver::numblockchans`): blocks of `8e6` pixels divided
/// by the plane's, at least ten channels, or the whole cube when it has no
/// more channels than that; the last block holds what remains.
pub(super) fn channel_blocks(channels: usize, shape: [usize; 2]) -> Vec<Range<usize>> {
    let optimum = ((8.0e6 / shape[0] as f64 / shape[1] as f64) as usize).max(10);
    let size = if channels > optimum {
        optimum
    } else {
        channels
    }
    .max(1);
    (0..channels)
        .step_by(size)
        .map(|start| start..(start + size).min(channels))
        .collect()
}

/// The beam a block's masks are smoothed and pruned by, with the image's
/// sidelobe level: the common beam of its planes' PSF beams; `None` when no
/// plane of the block has data. CASA converts the beam to pixels by the
/// first direction axis's increment; the common beam is found in radians,
/// as for restoration.
fn block_beam(
    psfs: &[Option<PsfSummary>],
    sidelobe: f64,
    coordinate: DirectionCoordinateSpec,
) -> Result<Option<AutoMaskBeam>, MaskError> {
    let cell = coordinate.increment_rad()[0].abs();
    let beams = psfs
        .iter()
        .flatten()
        .map(|summary| {
            let beam = summary.beam();
            RestoringBeam::new(
                beam.major_fwhm_rad() * cell,
                beam.minor_fwhm_rad() * cell,
                beam.position_angle_rad(),
            )
        })
        .collect::<Result<Vec<_>, _>>()
        .map_err(|_| MaskError::InvalidBeamArea)?;
    if beams.is_empty() {
        return Ok(None);
    }
    let common = RestoringBeam::common_enclosing(&beams).map_err(|_| MaskError::InvalidBeamArea)?;
    let beam = AutoMaskBeam::new(
        common.major_fwhm_rad() / cell,
        common.minor_fwhm_rad() / cell,
        common.position_angle_rad(),
        sidelobe,
    );
    validate_beam(&beam)?;
    Ok(Some(beam))
}

/// What tclean logs of one block's mask: at the first boundary its
/// warnings (`autoMaskByMultiThreshold`, `noMaskCheck`), and with
/// `verbose` CASA's automask summary, a row per channel
/// (`printAutomaskSummary`).
fn report_block(
    state: &AutoMaskState<'_>,
    block: &Range<usize>,
    channels: &[ChannelMask],
    polarizations: usize,
) {
    if state.completed_major_cycles == 0 {
        if state.controls.cut_threshold >= 0.2 {
            tracing::warn!(
                "Faint regions may not be included in the final mask. Consider decreasing \
                 cutthreshold."
            );
        }
        if !channels.iter().any(|channel| channel.positive) {
            warn_no_regions(channels);
            let pruned = channels
                .iter()
                .flat_map(|channel| &channel.rows)
                .filter(|row| row.regions > 0 && row.pruned_regions == row.regions)
                .count();
            if pruned > 0 {
                tracing::warn!(
                    "{pruned} of {} channels had all regions removed by pruning. Try decreasing \
                     minbeamfrac to remove fewer regions",
                    channels.len()
                );
            }
        }
    }
    if state.controls.verbose {
        log_summary(block.start, channels, polarizations);
    }
}

/// `SDMaskHandler::noMaskCheck` of a block whose positive mask is empty.
fn warn_no_regions(channels: &[ChannelMask]) {
    tracing::warn!("No regions found by automasking");
    let rows = channels
        .iter()
        .flat_map(|channel| &channel.rows)
        .collect::<Vec<_>>();
    let [first, ..] = rows.as_slice() else {
        return;
    };
    if rows.len() == 1 {
        let kind = first.threshold.name();
        tracing::warn!(
            "Used {kind} threshold to mask, but no emission was found.Try decreasing your \
             {kind}threshold parameter if you want to capture emission in these channels."
        );
        return;
    }
    for kind in [AutoMaskThreshold::Sidelobe, AutoMaskThreshold::Noise] {
        let count = rows.iter().filter(|row| row.threshold == kind).count();
        if count > 0 {
            let name = kind.name();
            tracing::warn!(
                "{count} of {} channels used the {name} threshold to mask, but no emission was \
                 found. Try decreasing your {name}threshold parameter if you want to capture \
                 emission in these channels.",
                rows.len()
            );
        }
    }
}

/// CASA's `printAutomaskSummary` of one block, through `tracing`: a row per
/// channel, each value a list over its polarizations when it has several.
fn log_summary(first_channel: usize, channels: &[ChannelMask], polarizations: usize) {
    let list = |values: Vec<String>| {
        if polarizations == 1 {
            values.into_iter().next().unwrap_or_default()
        } else {
            format!("[{}]", values.join(", "))
        }
    };
    tracing::info!("========== automask summary ==========");
    tracing::info!(
        "chan masking? median   RMS         peak   thresh_type   thresh_value   N_reg N_pruned \
         N_grow N_grow_pruned N_neg_pix"
    );
    for (offset, channel) in channels.iter().enumerate() {
        let rows = &channel.rows;
        let column = |value: fn(&AutoMultithreshEvidence) -> String| {
            list(rows.iter().map(value).collect())
        };
        // A stopped channel was skipped before this boundary: CASA prints
        // its median, RMS and threshold as `--`.
        let masked = rows[0].masked;
        let skipped = |text: String| if masked { text } else { "--".to_string() };
        tracing::info!(
            "[C{}] {}        {}  {}  {}  {}  {}        {}  {}  {}  {}  {}",
            first_channel + offset,
            if masked { "T" } else { "F" },
            skipped(column(|row| row.median.to_string())),
            skipped(column(|row| row.robust_rms.to_string())),
            column(|row| row.peak.to_string()),
            column(|row| row.threshold.name().to_string()),
            skipped(column(|row| row.positive_threshold.to_string())),
            column(|row| row.regions.to_string()),
            column(|row| row.pruned_regions.to_string()),
            column(|row| row.grow_regions.to_string()),
            column(|row| row.grow_pruned_regions.to_string()),
            column(|row| row.negative_pixels.to_string()),
        );
    }
    tracing::info!("========== END of automask summary ==========");
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_cube_is_masked_in_casa_channel_blocks() {
        // 100 by 100: blocks of 800 channels, so a small cube is one block.
        assert_eq!(channel_blocks(3, [100, 100]), [0..3]);
        assert_eq!(channel_blocks(1, [100, 100]), [0..1]);
        // 2000 by 2000: 2 channels by size, at least ten.
        assert_eq!(channel_blocks(25, [2000, 2000]), [0..10, 10..20, 20..25]);
        assert_eq!(channel_blocks(10, [2000, 2000]), [0..10]);
        // 1000 by 1000: eight channels by size, so ten.
        assert_eq!(channel_blocks(21, [1000, 1000]), [0..10, 10..20, 20..21]);
        // 900 by 900: 9.87 truncates to nine, so ten.
        assert_eq!(channel_blocks(11, [900, 900]), [0..10, 10..11]);
        // 400 by 400: blocks of fifty.
        assert_eq!(channel_blocks(120, [400, 400]), [0..50, 50..100, 100..120]);
    }
}

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
//!   the whole image domain (`SIImageStore::makeImageBeamSet`), even when that
//!   beam belongs to a different channel block;
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
use std::sync::Arc;

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
    /// The valid pixels the residual's pixel mask keeps, x-major: its
    /// statistics, its pruned regions and its mask read only these
    /// (`SDMaskHandler::autoMask` attaches the residual's mask, the
    /// `.pb`'s above `pblimit`, `SIImageStore::divideResidualByWeight`).
    pub valid: Vec<bool>,
    /// The pixels the plane's median and noise are measured on when they
    /// are not `valid`, x-major. At a run's first boundary with `nsigma`,
    /// tclean's automatic mask takes the median and noise
    /// `SIImageStore::calcRobustRMS` measured where the `.pb` exceeds
    /// `pbmask` (`SDMaskHandler::autoMask` reuses them while `iterdone` is
    /// 0); its peak is still the pixel mask's.
    pub statistics: Option<Vec<bool>>,
}

/// A plane's residual and valid pixels; `None` for a plane with no data.
pub type AutoMaskRead<'a, E> = dyn Fn(MaskPlaneKey) -> Result<Option<AutoMaskPlane>, E> + Sync + 'a;

/// A channel-scoped reader for an automatic mask. It may share working
/// data among the channel's polarizations and observe each completed mask.
/// The reader is dropped when the channel finishes.
pub trait AutoMaskChannel<E> {
    /// Read one polarization's residual and valid pixels.
    fn read(&mut self, key: MaskPlaneKey) -> Result<Option<AutoMaskPlane>, E>;

    /// Observe the mask before the next polarization is read.
    fn masked(
        &mut self,
        key: MaskPlaneKey,
        plane: Option<&AutoMaskPlane>,
        support: &[bool],
    ) -> Result<(), E>;
}

/// Open one channel-scoped reader. At most one is live per masking worker.
pub type AutoMaskOpenChannel<'a, E> =
    dyn Fn(MaskPlaneKey) -> Result<Box<dyn AutoMaskChannel<E> + 'a>, E> + Sync + 'a;

/// A plane's PSF measurements; `None` for a plane with no data.
pub type AutoMaskPsf<'a> = dyn Fn(MaskPlaneKey) -> Option<PsfSummary> + Sync + 'a;

/// What an automatic mask reads of one major cycle's planes, from the
/// owner of the minor cycle: each plane as [`AutoMaskRead`] gives it, read
/// once per boundary, and its PSF's measurements ([`AutoMaskPsf`]), which
/// do not change; and how many channels it masks at once.
pub struct AutoMaskSource<'a, E> {
    /// Each plane's residual and valid pixels.
    pub read: &'a AutoMaskRead<'a, E>,
    /// Optional channel-scoped reader, used instead of `read` when present.
    pub channel: Option<&'a AutoMaskOpenChannel<'a, E>>,
    /// Each plane's PSF measurements.
    pub psf: &'a AutoMaskPsf<'a>,
    /// The channels masked at once: more than one masks a block's channels
    /// side by side on the current rayon pool (the owner's worker team),
    /// one masks them one after another.
    pub workers: usize,
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

/// A channel's place in the domain's new mask: its supports, polarization
/// after polarization, and its rows of the automask summary, which it
/// fills.
struct ChannelSlots<'a> {
    channel: usize,
    support: &'a mut [bool],
    rows: &'a mut [AutoMultithreshEvidence],
}

/// A row before its plane is masked.
const UNMASKED: AutoMultithreshEvidence = AutoMultithreshEvidence {
    median: 0.0,
    robust_rms: 0.0,
    peak: 0.0,
    positive_threshold: 0.0,
    low_noise_threshold: 0.0,
    negative_threshold: None,
    threshold: AutoMaskThreshold::Sidelobe,
    masked: false,
    regions: 0,
    pruned_regions: 0,
    grow_regions: 0,
    grow_pruned_regions: 0,
    negative_pixels: 0,
    changed_pixels: 0,
    channel_stopped: false,
};

/// What every channel of one block is masked with.
struct Block<'a, 'b, E> {
    state: &'a AutoMaskState<'a>,
    source: &'a AutoMaskSource<'b, E>,
    domain: usize,
    shape: [usize; 2],
    polarizations: usize,
    beam: Option<AutoMaskBeam>,
    first_plane_full: bool,
    /// An empty plane, the mask a plane inherits before the first boundary.
    nothing: &'a [bool],
}

impl<E: From<MaskError>> Block<'_, '_, E> {
    /// Mask `slots`' channel into its slots, and apply its stop
    /// (`SDMaskHandler::skipChannels`), which every plane of the channel
    /// shares: whether it added a positive (emission) region.
    fn channel(&self, slots: ChannelSlots<'_>) -> Result<bool, E> {
        let ChannelSlots {
            channel,
            support,
            rows,
        } = slots;
        let state = self.state;
        let inherited = |polarization| {
            state
                .previous
                .and_then(|mask| mask.plane(channel, polarization))
                .unwrap_or(self.nothing)
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
        let cells = self.shape[0] * self.shape[1];
        let mut reader = self
            .source
            .channel
            .map(|open| {
                open(MaskPlaneKey {
                    domain: self.domain,
                    channel,
                    polarization: 0,
                })
            })
            .transpose()?;
        let mut positive = false;
        for (polarization, (out, row)) in support.chunks_mut(cells).zip(rows.iter_mut()).enumerate()
        {
            let key = MaskPlaneKey {
                domain: self.domain,
                channel,
                polarization,
            };
            let plane = match &mut reader {
                Some(reader) => reader.read(key)?,
                None => (self.source.read)(key)?,
            };
            let made = plane_mask(&PlaneInputs {
                plane: plane.as_ref(),
                inherited: inherited(polarization),
                shape: self.shape,
                first_plane_full: self.first_plane_full,
                cycle,
                controls: state.controls,
                beam: self.beam,
            })?;
            if let Some(reader) = &mut reader {
                reader.masked(key, plane.as_ref(), &made.support)?;
            }
            out.copy_from_slice(&made.support);
            *row = made.evidence;
            positive |= made.positive;
        }
        let count = |support: &[bool]| support.iter().filter(|set| **set).count();
        let before = (0..self.polarizations)
            .map(|polarization| count(inherited(polarization)))
            .sum();
        let stopped = skip_channel(
            before,
            count(support),
            rows[0].threshold,
            cycle,
            state.controls,
        );
        for row in rows.iter_mut() {
            row.channel_stopped = stopped;
        }
        Ok(positive)
    }
}

/// The automatic mask of every plane of `domain`, `shape` pixels by
/// `planes` channels and polarizations, from the mask it inherits. Each
/// channel writes its planes and rows straight into the domain's new mask
/// and summary; a block's channels are masked side by side on the current
/// rayon pool when `source` masks more than one at once.
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
    let polarizations = planes.polarizations();
    // The materialization charge covers exactly one entry per plane.
    let mut psfs = Vec::with_capacity(planes.count());
    for channel in 0..planes.channels() {
        for polarization in 0..polarizations {
            psfs.push((source.psf)(MaskPlaneKey {
                domain,
                channel,
                polarization,
            }));
        }
    }
    let sidelobe = psfs
        .iter()
        .flatten()
        .map(PsfSummary::sidelobe)
        .fold(0.0_f64, f64::max);
    let nothing = vec![false; cells];
    // One allocation each, written in place: the support is shared as it
    // is by the mask (a trusted-length iterator collects into the `Arc`
    // without a staging copy).
    let mut support = (0..planes.count() * cells)
        .map(|_| false)
        .collect::<Arc<[bool]>>();
    let mut rows = vec![UNMASKED; planes.count()];
    let slots = Arc::get_mut(&mut support).expect("a support no one else holds yet");
    let per_channel = polarizations * cells;
    for (block, beam) in block_beams(&psfs, polarizations, shape, sidelobe, state.coordinate) {
        let masking = Block {
            state,
            source,
            domain,
            shape,
            polarizations,
            beam: beam?,
            first_plane_full: state
                .previous
                .and_then(|mask| mask.plane(block.start, 0))
                .is_some_and(|plane| plane.iter().all(|set| *set)),
            nothing: &nothing,
        };
        let block_support = &mut slots[block.start * per_channel..block.end * per_channel];
        let block_rows = &mut rows[block.start * polarizations..block.end * polarizations];
        let positive = if source.workers > 1 {
            block_support
                .par_chunks_mut(per_channel)
                .zip(block_rows.par_chunks_mut(polarizations))
                .zip(block.clone().into_par_iter())
                .map(|((support, rows), channel)| {
                    masking.channel(ChannelSlots {
                        channel,
                        support,
                        rows,
                    })
                })
                .try_reduce(|| false, |left, right| Ok(left || right))?
        } else {
            let mut positive = false;
            for ((support, rows), channel) in block_support
                .chunks_mut(per_channel)
                .zip(block_rows.chunks_mut(polarizations))
                .zip(block.clone())
            {
                positive |= masking.channel(ChannelSlots {
                    channel,
                    support,
                    rows,
                })?;
            }
            positive
        };
        report_block(
            state,
            block.start,
            &rows[block.start * polarizations..block.end * polarizations],
            positive,
            polarizations,
        );
    }
    Ok((
        ReconstructionMask::from_plane_support(state.coordinate, shape, planes, support)?,
        AutoMaskEvidence {
            polarizations,
            planes: rows.into_boxed_slice(),
        },
    ))
}

/// The blocks of channels tclean masks a cube in on one process
/// (`SynthesisDeconvolver::numblockchans`): blocks of `8e6` pixels divided
/// by the plane's, at least ten channels, or the whole cube when it has no
/// more channels than that; the last block holds what remains.
pub(super) fn channel_blocks(channels: usize, shape: [usize; 2]) -> Vec<Range<usize>> {
    let size = block_size(channels, shape);
    (0..channels)
        .step_by(size)
        .map(|start| start..(start + size).min(channels))
        .collect()
}

/// The largest block, in channels, CASA forms for an image of `shape`.
fn block_size(channels: usize, shape: [usize; 2]) -> usize {
    let optimum = ((8.0e6 / shape[0] as f64 / shape[1] as f64) as usize).max(10);
    channels.min(optimum).max(1)
}

/// The peak three simultaneously held beam arrays for one channel block:
/// fitted beams, their numerical forms and the common-beam working copy.
pub(super) fn block_beam_bytes(channels: usize, polarizations: usize, shape: [usize; 2]) -> u64 {
    let planes = channels.min(block_size(channels, shape)) * polarizations;
    let blocks = channels.div_ceil(block_size(channels, shape));
    (planes * (size_of::<RestoringBeam>() + 2 * size_of::<casa_numerics::EllipticalGaussian>())
        + blocks * size_of::<Range<usize>>()) as u64
}

/// Plan each channel block's beam from the complete image domain's beam set.
fn block_beams(
    psfs: &[Option<PsfSummary>],
    polarizations: usize,
    shape: [usize; 2],
    sidelobe: f64,
    coordinate: DirectionCoordinateSpec,
) -> impl Iterator<Item = (Range<usize>, Result<Option<AutoMaskBeam>, MaskError>)> + '_ {
    let largest = largest_beam(psfs);
    channel_blocks(psfs.len() / polarizations, shape)
        .into_iter()
        .map(move |block| {
            let beam = block_beam(
                &psfs[block.start * polarizations..block.end * polarizations],
                polarizations,
                largest,
                sidelobe,
                coordinate,
            );
            (block, beam)
        })
}

/// The largest fitted beam of the image domain, which CASA puts into each
/// blank plane before it forms channel-block image stores.
fn largest_beam(psfs: &[Option<PsfSummary>]) -> Option<RestoringBeam> {
    psfs.iter().flatten().map(PsfSummary::beam).fold(
        None,
        |largest: Option<RestoringBeam>, beam| match largest {
            Some(current) if current.area_sr() >= beam.area_sr() => Some(current),
            _ => Some(beam),
        },
    )
}

/// The beam a block's masks are smoothed and pruned by: the common beam of
/// its PSF beams, with blank planes holding the domain's largest. `None`
/// means the whole domain has no fitted beam. The common beam is found in
/// world coordinates, then its area and smoothing use both pixel increments.
fn block_beam(
    psfs: &[Option<PsfSummary>],
    polarizations: usize,
    largest: Option<RestoringBeam>,
    sidelobe: f64,
    coordinate: DirectionCoordinateSpec,
) -> Result<Option<AutoMaskBeam>, MaskError> {
    let cell = coordinate.increment_rad()[0].abs();
    let mut beams = Vec::with_capacity(psfs.len());
    // CASA iterates its channel-by-polarization beam matrix with channel
    // varying fastest, even though our plane list is channel-major.
    for polarization in 0..polarizations {
        for channel in 0..psfs.len() / polarizations {
            let summary = psfs[channel * polarizations + polarization];
            if let Some(beam) = summary.map(|summary| summary.beam()).or(largest) {
                beams.push(
                    RestoringBeam::new(
                        beam.major_fwhm_rad() * cell,
                        beam.minor_fwhm_rad() * cell,
                        beam.position_angle_rad(),
                    )
                    .map_err(|_| MaskError::InvalidBeamArea)?,
                );
            }
        }
    }
    if beams.is_empty() {
        return Ok(None);
    }
    let common = RestoringBeam::common_enclosing(&beams).map_err(|_| MaskError::InvalidBeamArea)?;
    let beam = AutoMaskBeam::new(
        common.major_fwhm_rad() / cell,
        common.minor_fwhm_rad() / cell,
        common.position_angle_rad(),
        sidelobe,
    )
    .with_cell_scale([1.0, (coordinate.increment_rad()[1] / cell).abs()]);
    validate_beam(&beam)?;
    Ok(Some(beam))
}

/// What tclean logs of one block's mask, its `rows` channel-major from
/// channel `first`: at the first boundary its warnings
/// (`autoMaskByMultiThreshold`, `noMaskCheck`) when it added no positive
/// region, and with `verbose` CASA's automask summary, a row per channel
/// (`printAutomaskSummary`).
fn report_block(
    state: &AutoMaskState<'_>,
    first: usize,
    rows: &[AutoMultithreshEvidence],
    positive: bool,
    polarizations: usize,
) {
    let channels = rows.len() / polarizations;
    if state.completed_major_cycles == 0 {
        if state.controls.cut_threshold >= 0.2 {
            tracing::warn!(
                "Faint regions may not be included in the final mask. Consider decreasing \
                 cutthreshold."
            );
        }
        if !positive {
            warn_no_regions(rows);
            let pruned = rows
                .iter()
                .filter(|row| row.regions > 0 && row.pruned_regions == row.regions)
                .count();
            if pruned > 0 {
                tracing::warn!(
                    "{pruned} of {channels} channels had all regions removed by pruning. Try \
                     decreasing minbeamfrac to remove fewer regions"
                );
            }
        }
    }
    if state.controls.verbose {
        log_summary(first, rows, polarizations);
    }
}

/// `SDMaskHandler::noMaskCheck` of a block whose positive mask is empty.
fn warn_no_regions(rows: &[AutoMultithreshEvidence]) {
    tracing::warn!("No regions found by automasking");
    let [first, ..] = rows else {
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

/// CASA's `printAutomaskSummary` of one block's `rows` from channel
/// `first_channel`, through `tracing`: a row per channel, each value a list
/// over its polarizations when it has several.
fn log_summary(first_channel: usize, rows: &[AutoMultithreshEvidence], polarizations: usize) {
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
    for (offset, rows) in rows.chunks(polarizations).enumerate() {
        let column =
            |value: fn(&AutoMultithreshEvidence) -> String| list(rows.iter().map(value).collect());
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
    fn block_beam_uses_both_direction_increments() {
        use crate::plane::PlaneShape;
        use casa_imaging_model::{DirectionFrame, Projection, SkyDirection};
        let shape = PlaneShape::new(64, 64);
        let psf = (0..shape.len())
            .map(|i| {
                let [x, y] = shape.pixel(i);
                (-((x as f64 - 32.0).powi(2) + (y as f64 - 32.0).powi(2)) / 8.0).exp()
            })
            .collect::<Vec<_>>();
        let summary = PsfSummary::new(&psf, shape).unwrap();
        let coordinate = |y| {
            DirectionCoordinateSpec::new(
                Projection::Sin,
                SkyDirection::new(DirectionFrame::J2000, 1.0, 0.5),
                [32.0, 32.0],
                [-1e-4, y],
                [[1.0, 0.0], [0.0, 1.0]],
                [180.0, 90.0],
            )
        };
        let square = block_beam(&[Some(summary)], 1, None, 0.1, coordinate(1e-4))
            .unwrap()
            .unwrap();
        let rectangular = block_beam(&[Some(summary)], 1, None, 0.1, coordinate(2e-4))
            .unwrap()
            .unwrap();
        assert_eq!(rectangular, square.with_cell_scale([1.0, 2.0]));
        assert_ne!(square, rectangular);
    }

    /// A blank plane inherits the largest beam in the image, even when that
    /// beam is in another channel block (`SIImageStore::makeImageBeamSet`).
    #[test]
    fn a_blank_plane_uses_the_image_domains_largest_beam_in_its_block() {
        use crate::plane::PlaneShape;
        use casa_imaging_model::{DirectionFrame, Projection, SkyDirection};

        let shape = PlaneShape::new(64, 64);
        let psf = (0..shape.len())
            .map(|index| {
                let [x, y] = shape.pixel(index);
                let r2 = (x as f64 - 32.0).powi(2) + (y as f64 - 32.0).powi(2);
                (-r2 / 8.0).exp()
            })
            .collect::<Vec<_>>();
        let narrow = PsfSummary::new(&psf, shape).expect("narrow beam");
        let broad = narrow.with_domain_beam(
            &psf,
            shape,
            RestoringBeam::new(10.0, 8.0, 0.0).expect("broad beam"),
        );
        let coordinate = DirectionCoordinateSpec::new(
            Projection::Sin,
            SkyDirection::new(DirectionFrame::J2000, 1.0, 0.5),
            [1.0, 1.0],
            [-1.0e-4, 1.0e-4],
            [[1.0, 0.0], [0.0, 1.0]],
            [180.0, 90.0],
        );
        let mut psfs = vec![Some(narrow); 11];
        psfs[1] = None;
        psfs[10] = Some(broad);
        let largest = largest_beam(&psfs);
        assert_eq!(largest, Some(broad.beam()));
        let mut blocks = block_beams(&psfs, 1, [900, 900], 0.1, coordinate);
        let (first, with_blank) = blocks.next().expect("first block");
        let (second, _) = blocks.next().expect("second block");
        assert_eq!((first, second), (0..10, 10..11));
        assert!(blocks.next().is_none());
        let with_blank = with_blank.unwrap();
        let mut filled = psfs[..10].to_vec();
        filled[1] = Some(broad);
        let with_explicit_broad = block_beam(&filled, 1, largest, 0.1, coordinate).unwrap();
        let without_blank = block_beam(&psfs[2..10], 1, largest, 0.1, coordinate).unwrap();
        assert_eq!(with_blank, with_explicit_broad);
        assert_ne!(with_blank, without_blank);
        assert!(
            block_beam(&[None], 1, None, 0.1, coordinate)
                .unwrap()
                .is_none()
        );
    }

    /// A narrow 25-pixel island survives its own beam, but CASA prunes it
    /// when a blank plane brings in a larger beam from the next block.
    #[test]
    fn a_blank_plane_changes_the_materialized_mask_through_its_block_beam() {
        use crate::plane::PlaneShape;
        use casa_imaging_model::{DirectionFrame, NoiseEstimate, Projection, SkyDirection};

        let psf_shape = PlaneShape::new(64, 64);
        let psf = (0..psf_shape.len())
            .map(|index| {
                let [x, y] = psf_shape.pixel(index);
                let r2 = (x as f64 - 32.0).powi(2) + (y as f64 - 32.0).powi(2);
                (-r2 / 8.0).exp()
            })
            .collect::<Vec<_>>();
        let narrow = PsfSummary::new(&psf, psf_shape).unwrap();
        let broad =
            narrow.with_domain_beam(&psf, psf_shape, RestoringBeam::new(10.0, 8.0, 0.0).unwrap());
        let coordinate = DirectionCoordinateSpec::new(
            Projection::Sin,
            SkyDirection::new(DirectionFrame::J2000, 1.0, 0.5),
            [450.0, 450.0],
            [-1.0e-4, 1.0e-4],
            [[1.0, 0.0], [0.0, 1.0]],
            [180.0, 90.0],
        );
        let controls = AutoMultithreshControls {
            sidelobe_factor: 2.0,
            noise_factor: 4.25,
            low_noise_factor: 1.5,
            negative_factor: 0.0,
            minimum_beam_fraction: 0.5,
            smooth_factor: 1.0,
            cut_threshold: 0.01,
            grow_iterations: 0,
            grow_prune: true,
            minimum_percent_change: -1.0,
            noise: NoiseEstimate::Fast,
            verbose: false,
        };
        let shape = [900, 900];
        let read = |key: MaskPlaneKey| {
            if !matches!(key.channel, 0 | 10) {
                return Ok::<_, MaskError>(None);
            }
            let mut residual = vec![0.0; shape[0] * shape[1]];
            if key.channel == 0 {
                for x in 448..453 {
                    for y in 448..453 {
                        residual[x * shape[1] + y] = 10.0;
                    }
                }
            }
            Ok(Some(AutoMaskPlane {
                residual,
                valid: vec![true; shape[0] * shape[1]],
                statistics: None,
            }))
        };
        let psfs = |key: MaskPlaneKey| match key.channel {
            0 => Some(narrow),
            10 => Some(broad),
            _ => None,
        };
        let source = AutoMaskSource {
            channel: None,
            read: &read,
            psf: &psfs,
            workers: 1,
        };
        let state = AutoMaskState {
            coordinate,
            controls: &controls,
            completed_major_cycles: 0,
            threshold_reached: false,
            previous: None,
            stopped_channels: &[],
        };
        let (mask, evidence) =
            auto_mask_domain(&state, 0, shape, MaskPlanes::new(11, 1).unwrap(), &source).unwrap();
        let row = evidence.plane(0, 0).unwrap();
        assert_eq!((row.regions, row.pruned_regions), (1, 1));
        assert!(!mask.plane(0, 0).unwrap().iter().any(|pixel| *pixel));
    }

    /// CASA's common-beam iteration visits every channel of a Stokes plane
    /// before the next Stokes plane, regardless of our plane storage order.
    #[test]
    fn a_multistokes_block_uses_casas_beam_matrix_order() {
        use crate::plane::PlaneShape;
        use casa_imaging_model::{DirectionFrame, Projection, SkyDirection};

        let shape = PlaneShape::new(64, 64);
        let psf = (0..shape.len())
            .map(|index| {
                let [x, y] = shape.pixel(index);
                let r2 = (x as f64 - 32.0).powi(2) + (y as f64 - 32.0).powi(2);
                (-r2 / 8.0).exp()
            })
            .collect::<Vec<_>>();
        let template = PsfSummary::new(&psf, shape).expect("a PSF");
        let axes = [
            (
                0.000_659_920_363_505_466_7,
                0.000_277_714_375_479_345_24,
                -1.006_629_886_598_266_8,
            ),
            (
                0.000_489_199_949_523_298_2,
                0.000_332_711_701_991_262_23,
                0.534_970_100_282_602_5,
            ),
            (
                0.000_575_668_982_898_358_4,
                0.000_251_585_111_889_876_9,
                0.498_537_038_391_696_7,
            ),
            (
                0.000_445_553_375_482_662,
                0.000_232_902_929_545_132_06,
                0.677_320_743_101_792_8,
            ),
        ];
        let summaries = axes.map(|(major, minor, pa)| {
            template.with_domain_beam(&psf, shape, RestoringBeam::new(major, minor, pa).unwrap())
        });
        let mut psfs = vec![Some(summaries[3]); 22];
        psfs[0] = None;
        psfs[1] = Some(summaries[1]);
        psfs[2] = Some(summaries[2]);
        psfs[20] = Some(summaries[0]);
        let coordinate = DirectionCoordinateSpec::new(
            Projection::Sin,
            SkyDirection::new(DirectionFrame::J2000, 1.0, 0.5),
            [1.0, 1.0],
            [-1.0, 1.0],
            [[1.0, 0.0], [0.0, 1.0]],
            [180.0, 90.0],
        );
        let mut blocks = block_beams(&psfs, 2, [900, 900], 0.1, coordinate);
        let (_, actual) = blocks.next().expect("first block");
        let actual = actual.expect("the block beam");
        let mut casa_order = Vec::with_capacity(20);
        for polarization in 0..2 {
            for channel in 0..10 {
                casa_order.push(
                    psfs[channel * 2 + polarization]
                        .unwrap_or(summaries[0])
                        .beam(),
                );
            }
        }
        let expected = RestoringBeam::common_enclosing(&casa_order).unwrap();
        let expected = AutoMaskBeam::new(
            expected.major_fwhm_rad(),
            expected.minor_fwhm_rad(),
            expected.position_angle_rad(),
            0.1,
        );
        assert_eq!(actual, Some(expected));
    }

    #[test]
    fn a_cube_is_masked_in_casa_channel_blocks() {
        // 100 by 100: blocks of 800 channels, so a small cube is one block.
        assert_eq!(channel_blocks(3, [100, 100]), vec![0..3]);
        assert_eq!(channel_blocks(1, [100, 100]), vec![0..1]);
        // 2000 by 2000: 2 channels by size, at least ten.
        assert_eq!(channel_blocks(25, [2000, 2000]), [0..10, 10..20, 20..25]);
        assert_eq!(channel_blocks(10, [2000, 2000]), vec![0..10]);
        // 1000 by 1000: eight channels by size, so ten.
        assert_eq!(channel_blocks(21, [1000, 1000]), [0..10, 10..20, 20..21]);
        // 900 by 900: 9.87 truncates to nine, so ten.
        assert_eq!(channel_blocks(11, [900, 900]), [0..10, 10..11]);
        // 400 by 400: blocks of fifty.
        assert_eq!(channel_blocks(120, [400, 400]), [0..50, 50..100, 100..120]);
    }
}

#[cfg(test)]
mod channel_law {
    use super::*;
    use casa_imaging_model::{DirectionFrame, NoiseEstimate, Projection, SkyDirection};
    use std::sync::atomic::{AtomicUsize, Ordering};

    struct Reader<'a> {
        channel: usize,
        live: &'a AtomicUsize,
        reads: &'a AtomicUsize,
        masked: &'a AtomicUsize,
    }

    impl Drop for Reader<'_> {
        fn drop(&mut self) {
            self.live.fetch_sub(1, Ordering::SeqCst);
        }
    }

    impl AutoMaskChannel<MaskError> for Reader<'_> {
        fn read(&mut self, key: MaskPlaneKey) -> Result<Option<AutoMaskPlane>, MaskError> {
            assert_eq!(key.channel, self.channel);
            self.reads.fetch_add(1, Ordering::SeqCst);
            Ok(Some(AutoMaskPlane {
                residual: vec![0.0; 64],
                valid: vec![true; 64],
                statistics: None,
            }))
        }

        fn masked(
            &mut self,
            key: MaskPlaneKey,
            _: Option<&AutoMaskPlane>,
            support: &[bool],
        ) -> Result<(), MaskError> {
            assert_eq!(key.channel, self.channel);
            assert_eq!(support.len(), 64);
            self.masked.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }
    }

    /// Channel work, including all four polarizations, owns at most one
    /// reader per worker, regardless of the cube's channel count.
    #[test]
    fn automatic_mask_scopes_readers_to_channels_in_flight() {
        const CHANNELS: usize = 21;
        const WORKERS: usize = 4;
        let live = AtomicUsize::new(0);
        let peak = AtomicUsize::new(0);
        let opened = AtomicUsize::new(0);
        let reads = AtomicUsize::new(0);
        let masked = AtomicUsize::new(0);
        let open =
            |key: MaskPlaneKey| -> Result<Box<dyn AutoMaskChannel<MaskError> + '_>, MaskError> {
                opened.fetch_add(1, Ordering::SeqCst);
                let current = live.fetch_add(1, Ordering::SeqCst) + 1;
                peak.fetch_max(current, Ordering::SeqCst);
                Ok(Box::new(Reader {
                    channel: key.channel,
                    live: &live,
                    reads: &reads,
                    masked: &masked,
                }))
            };
        let legacy = |_: MaskPlaneKey| -> Result<Option<AutoMaskPlane>, MaskError> {
            panic!("channel reader is used")
        };
        let mut values = vec![0.0; 64];
        values[36] = 1.0;
        let summary = PsfSummary::with_saved_beam(
            &values,
            crate::PlaneShape::new(8, 8),
            RestoringBeam::new(2.0, 2.0, 0.0).unwrap(),
        )
        .unwrap();
        let psf = |_: MaskPlaneKey| Some(summary);
        let source = AutoMaskSource {
            read: &legacy,
            channel: Some(&open),
            psf: &psf,
            workers: WORKERS,
        };
        let coordinate = DirectionCoordinateSpec::new(
            Projection::Sin,
            SkyDirection::new(DirectionFrame::J2000, 1.0, -0.5),
            [4.0, 4.0],
            [-1.0e-4, 1.0e-4],
            [[1.0, 0.0], [0.0, 1.0]],
            [180.0, 90.0],
        );
        let controls = AutoMultithreshControls {
            sidelobe_factor: 2.0,
            noise_factor: 4.25,
            low_noise_factor: 1.5,
            negative_factor: 0.0,
            minimum_beam_fraction: 0.0,
            smooth_factor: 1.0,
            cut_threshold: 0.01,
            grow_iterations: 0,
            grow_prune: false,
            minimum_percent_change: -1.0,
            noise: NoiseEstimate::Fast,
            verbose: false,
        };
        let state = AutoMaskState {
            coordinate,
            controls: &controls,
            completed_major_cycles: 0,
            threshold_reached: false,
            previous: None,
            stopped_channels: &[],
        };
        rayon::ThreadPoolBuilder::new()
            .num_threads(WORKERS)
            .build()
            .unwrap()
            .install(|| {
                auto_mask_domain(
                    &state,
                    0,
                    [8, 8],
                    MaskPlanes::new(CHANNELS, 4).unwrap(),
                    &source,
                )
                .unwrap();
            });
        assert_eq!(opened.load(Ordering::SeqCst), CHANNELS);
        assert_eq!(reads.load(Ordering::SeqCst), CHANNELS * 4);
        assert_eq!(masked.load(Ordering::SeqCst), CHANNELS * 4);
        assert!(peak.load(Ordering::SeqCst) <= WORKERS);
        assert_eq!(live.load(Ordering::SeqCst), 0);
    }
}

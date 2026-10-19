// SPDX-License-Identifier: LGPL-3.0-or-later
//! The row-work pass: each region-split domain's gridding work by anchor
//! row, measured before the first pass so that even it cuts balanced strips
//! (#736), and the work of the PSF and the weight image beside it, so that
//! a pass that grids the weight image cuts them balanced too.

use std::ops::Range;

use casa_imaging_operator::{ModeSet, PlaneRange, WeightingGeneration};

use super::partition::{Router, placement_work};
use super::wave::GroupCount;

/// Groups a chunk's weight-image count holds before it forgets them
/// ([`GroupCount`]). A chunk's count only estimates what the weight owner's
/// table grids. A mosaic's few groups are counted once per chunk however
/// small this is, and AW's rarely recur once a few rows have passed, so a
/// count far smaller than the table's suffices and keeps the pass light.
const COUNTED_GROUPS: usize = 1 << 10;
use super::{
    BoundedSource, Cancel, Partition, PassDomain, PassError, SampleContent, WEIGHT_SUM_WORK,
    WorkerTeam, chunk_ranges, slices, stream_blocks,
};

/// What a row-work pass measures ([`run_row_work_pass`]): each domain's
/// data gridding work by anchor row, `None` for a domain of plane owners,
/// and each domain's work in the other modes.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct MeasuredRowWork {
    /// Each domain's gridding work by anchor row.
    pub rows: Vec<Option<Vec<u64>>>,
    /// Each domain's work in every mode; zero for a domain of plane owners,
    /// which the pass skips.
    pub modes: Vec<ModeWork>,
}

impl MeasuredRowWork {
    /// The bytes a run keeps for row work of `rows` histogram rows in all
    /// over `domains` domains: the histograms and one [`ModeWork`] per
    /// domain. Both the worker fit's estimate and the kept charge use it.
    #[must_use]
    pub const fn kept_bytes(rows: usize, domains: usize) -> u64 {
        (rows * size_of::<u64>() + domains * size_of::<ModeWork>()) as u64
    }

    /// The bytes this row work holds when kept ([`Self::kept_bytes`]).
    #[must_use]
    pub fn bytes(&self) -> u64 {
        let rows = self.rows.iter().flatten().map(Vec::len).sum();
        Self::kept_bytes(rows, self.modes.len())
    }
}

/// A region-split domain's work in each mode a pass may grid, summed over
/// every placement the row work counts: the data mode's kernel taps
/// (`Router::owner_and_work`) and the PSF's taps, at each sample's own
/// position (the weight cells for an AW catalog), each with
/// [`PLACEMENT_WORK`](super::PLACEMENT_WORK); and the weight image's. The
/// weight owner sums each placement into its table ([`WEIGHT_SUM_WORK`]) and
/// stamps a weight kernel once per group it grids (#787). The pass counts
/// those groups as the table would (`GroupCount`), so a mosaic's few groups
/// cost almost nothing while AW's, about one per two placements, keep
/// half a stamp each.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct ModeWork {
    /// The data mode's work, the row work's total.
    pub data: u64,
    /// The PSF's work.
    pub psf: u64,
    /// The weight image's work.
    pub weight: u64,
}

impl ModeWork {
    /// The work by anchor row of a pass of `modes`, from the data mode's
    /// `row_work`, when the pass grids the weight image: the data and PSF
    /// modes' work, which falls on each sample's own anchor row, scaled
    /// from the data mode's; and on the centre row, whose owner grids the
    /// weight image (`Router::weight_owner`), all of the weight image's.
    /// `None` without the weight mode or its work, when `row_work` itself
    /// cuts the pass's strips.
    #[must_use]
    pub fn weighed(&self, row_work: &[u64], modes: ModeSet) -> Option<Vec<u64>> {
        if !modes.weight || self.weight == 0 || self.data == 0 || row_work.is_empty() {
            return None;
        }
        let strips = u128::from(if modes.data { self.data } else { 0 })
            + u128::from(if modes.psf { self.psf } else { 0 });
        let mut weighed = row_work
            .iter()
            .map(|&work| {
                u64::try_from(u128::from(work) * strips / u128::from(self.data)).unwrap_or(u64::MAX)
            })
            .collect::<Vec<_>>();
        let centre = weighed.len() / 2;
        weighed[centre] = weighed[centre].saturating_add(self.weight);
        Some(weighed)
    }
}

/// Row chunks per worker of a row-work pass.
const CHUNKS_PER_WORKER: usize = 4;

/// Rows a chunk of a row-work pass places at once. It keeps only a
/// histogram, so this bounds the work between the workers' meetings, not
/// memory.
const CHUNK_ROWS: usize = 1 << 12;

/// Whether some domain is split among several region owners
/// ([`Partition::splits_rows`]), the only case a row-work pass serves.
#[must_use]
pub fn needs_row_work(domains: &[PassDomain<'_>]) -> bool {
    domains.iter().any(|domain| domain.partition.splits_rows())
}

/// The grid rows a row-work pass measures for `domain`: every row of one
/// split among region owners, none otherwise.
fn measured_rows(domain: &PassDomain<'_>) -> usize {
    if domain.partition.splits_rows() {
        domain.operator.geometry().grid_shape()[1]
    } else {
        0
    }
}

/// Bytes a row-work pass over `domains` on `workers` workers holds:
/// - each chunk's histogram, allocated once at the widest measured domain's
///   grid rows;
/// - the totals it returns;
/// - each domain's router: an owner per grid row for region owners, a
///   start per owner for plane owners;
/// - the containers of chunks, routers and totals;
/// - when the first pass grids a weight image (`weight`, its kernel sets
///   having weight taps), each chunk's count of its groups per domain
///   (`GroupCount`), which is allocated only for weight placements.
///
/// The source's blocks are the source's charge ([`super::source_block_bytes`]).
#[must_use]
pub fn row_work_pass_bytes(domains: &[PassDomain<'_>], workers: usize, weight: bool) -> u64 {
    let chunks = workers * CHUNKS_PER_WORKER;
    let widest = domains.iter().map(measured_rows).max().unwrap_or(0);
    let totals = domains.iter().map(measured_rows).sum::<usize>();
    let routing = domains
        .iter()
        .map(|domain| match domain.partition {
            Partition::Regions { .. } => {
                domain.operator.geometry().grid_shape()[1] * size_of::<u16>()
            }
            Partition::Planes { owners } => (owners + 1) * size_of::<u32>(),
        })
        .sum::<usize>();
    let containers = chunks * size_of::<(Range<usize>, Vec<u64>, ModeWork, Vec<GroupCount>)>()
        + domains.len()
            * (size_of::<Router>() + size_of::<Option<Vec<u64>>>() + size_of::<ModeWork>());
    // Each chunk's weight-image group count per domain, allocated whole on
    // its first weight placement (`GroupCount`).
    let counts = if weight {
        (chunks * domains.len()) as u64 * GroupCount::bytes(COUNTED_GROUPS)
    } else {
        0
    } + (chunks * domains.len() * size_of::<GroupCount>()) as u64;
    // The team's result per chunk, collected on each call
    // ([`WorkerTeam::for_each_mut`]).
    let results = chunks * size_of::<Result<(), PassError>>();
    ((chunks * widest + totals) * size_of::<u64>() + routing + containers + results) as u64 + counts
}

/// Each domain's gridding work by anchor row over one traversal that reads
/// flags and weights only ([`SampleContent::FlagsAndWeights`]): every row
/// is placed as a pass places it, with `weighting`, and each placement adds
/// its work, its kernel's taps and
/// [`PLACEMENT_WORK`](super::PLACEMENT_WORK), to its anchor row
/// (`Router::owner_and_work`). `None`
/// for a domain of plane owners. It is what a first pass measures
/// ([`super::PassSummary::row_work`]) without reading or gridding the
/// visibilities, so the first pass's strips can be cut balanced
/// ([`Partition::regions_balanced`]). Each counted placement also adds its
/// PSF and weight work ([`ModeWork`]), from the kernel set's metadata. It
/// holds [`row_work_pass_bytes`].
pub fn run_row_work_pass(
    domains: &[PassDomain<'_>],
    weighting: &WeightingGeneration,
    source: &mut dyn BoundedSource,
    team: &WorkerTeam,
    cancel: &Cancel,
) -> Result<MeasuredRowWork, PassError> {
    let Some(main) = domains.first() else {
        return Err(PassError::NoDomains);
    };
    let planes = PlaneRange::new(0, main.operator.basis().planes());
    source
        .begin(planes, false, SampleContent::FlagsAndWeights)
        .map_err(PassError::Source)?;
    let routers = domains
        .iter()
        .map(|domain| Router::new(&domain.partition, planes))
        .collect::<Vec<_>>();
    let mut totals = domains
        .iter()
        .map(|domain| {
            let rows = measured_rows(domain);
            (rows > 0).then(|| vec![0_u64; rows])
        })
        .collect::<Vec<_>>();
    let count = team.workers() * CHUNKS_PER_WORKER;
    // Each chunk's histogram holds the widest domain's rows from the start,
    // so a later, taller domain never grows it ([`row_work_pass_bytes`]).
    let widest = domains.iter().map(measured_rows).max().unwrap_or(0);
    let mut modes = vec![ModeWork::default(); domains.len()];
    // Each chunk counts, per domain, the weight-image groups its rows start
    // (`GroupCount`), kept across slices as the weight owner's table is.
    let mut chunks = (0..count)
        .map(|_| {
            (
                0..0,
                vec![0_u64; widest],
                ModeWork::default(),
                (0..domains.len())
                    .map(|_| GroupCount::new(COUNTED_GROUPS))
                    .collect::<Vec<_>>(),
            )
        })
        .collect::<Vec<_>>();
    stream_blocks(source, cancel, false, |block| {
        for (index, (domain, router)) in domains.iter().zip(&routers).enumerate() {
            let Some(total) = &mut totals[index] else {
                continue;
            };
            let cf = domain.operator.cf();
            for slice in slices(block.len(), count, CHUNK_ROWS) {
                let mut used = 0;
                for ((range, _, _, _), rows) in chunks.iter_mut().zip(chunk_ranges(slice, count)) {
                    *range = rows;
                    used += 1;
                }
                let rows = total.len();
                team.for_each_mut(&mut chunks[..used], |_, (range, work, other, counts)| {
                    let work = &mut work[..rows];
                    work.fill(0);
                    *other = ModeWork::default();
                    let groups = &mut counts[index];
                    // In the block's locality order, as a pass places them:
                    // a baseline's rows share kernel cells, which a set that
                    // pages its cells (AW) would otherwise reload row by row.
                    for position in range.clone() {
                        domain.resampler.place_each(
                            domain.operator,
                            weighting,
                            &block.row(index, block.ordered_row(position)),
                            None,
                            |placement, _, _| {
                                if !planes.contains(placement.plane) {
                                    return;
                                }
                                let (_, placed) =
                                    router.owner_and_work(domain.operator, &placement);
                                if let Some((anchor, taps)) = placed
                                    && let Some(cell) = work.get_mut(anchor)
                                {
                                    *cell += taps;
                                    other.data += taps;
                                    other.psf += placement_work(cf.psf_half_support(placement.cf));
                                    if let Some(support) = cf.weight_half_support(placement.cf) {
                                        other.weight += WEIGHT_SUM_WORK;
                                        if groups.starts(&placement) {
                                            other.weight += placement_work(support);
                                        }
                                    }
                                }
                            },
                        )?;
                    }
                    Ok::<_, PassError>(())
                })?;
                for (_, work, other, _) in &chunks[..used] {
                    for (total, work) in total.iter_mut().zip(work) {
                        *total += work;
                    }
                    let domain = &mut modes[index];
                    domain.data += other.data;
                    domain.psf += other.psf;
                    domain.weight += other.weight;
                }
            }
        }
        Ok(())
    })?;
    Ok(MeasuredRowWork {
        rows: totals,
        modes,
    })
}

// SPDX-License-Identifier: LGPL-3.0-or-later
//! The row-work pass: each region-split domain's gridding work by anchor
//! row, measured before the first pass so that even it cuts balanced strips
//! (#736).

use casa_imaging_operator::{PlaneRange, WeightingGeneration};

use super::partition::Router;
use super::{
    BoundedSource, Cancel, Partition, PassDomain, PassError, SampleContent, WorkerTeam,
    chunk_ranges, slices, stream_blocks,
};

/// Row chunks per worker of a row-work pass.
const CHUNKS_PER_WORKER: usize = 4;

/// Rows a chunk of a row-work pass places at once. It keeps only a
/// histogram, so this bounds the work between the workers' meetings, not
/// memory.
const CHUNK_ROWS: usize = 1 << 12;

/// Whether some domain is split among several region owners, the only case
/// a row-work pass serves.
#[must_use]
pub fn needs_row_work(domains: &[PassDomain<'_>]) -> bool {
    domains.iter().any(|domain| {
        matches!(domain.partition, Partition::Regions { .. }) && domain.partition.owners() > 1
    })
}

/// Bytes a row-work pass over `domains` on `workers` workers holds: one
/// histogram per chunk of the widest region-split domain's grid rows, and
/// the totals it returns.
#[must_use]
pub fn row_work_pass_bytes(domains: &[PassDomain<'_>], workers: usize) -> u64 {
    let rows = |domain: &PassDomain<'_>| match domain.partition {
        Partition::Regions { .. } => domain.operator.geometry().grid_shape()[1],
        Partition::Planes { .. } => 0,
    };
    let widest = domains.iter().map(rows).max().unwrap_or(0);
    let totals = domains.iter().map(rows).sum::<usize>();
    ((workers * CHUNKS_PER_WORKER * widest + totals) * size_of::<u64>()) as u64
}

/// Each domain's gridding work by anchor row over one traversal that reads
/// flags and weights only ([`SampleContent::FlagsAndWeights`]): every row
/// is placed as a pass places it, with `weighting`, and each placement adds
/// its kernel's taps to its anchor row ([`Router::owner_and_work`]). `None`
/// for a domain of plane owners. It is what a first pass measures
/// ([`super::PassSummary::row_work`]) without reading or gridding the
/// visibilities, so the first pass's strips can be cut balanced
/// ([`Partition::regions_balanced`]). It holds [`row_work_pass_bytes`].
pub fn run_row_work_pass(
    domains: &[PassDomain<'_>],
    weighting: &WeightingGeneration,
    source: &mut dyn BoundedSource,
    team: &WorkerTeam,
    cancel: &Cancel,
) -> Result<Vec<Option<Vec<u64>>>, PassError> {
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
            matches!(domain.partition, Partition::Regions { .. })
                .then(|| vec![0_u64; domain.operator.geometry().grid_shape()[1]])
        })
        .collect::<Vec<_>>();
    let count = team.workers() * CHUNKS_PER_WORKER;
    let mut chunks = (0..count)
        .map(|_| (0..0, Vec::<u64>::new()))
        .collect::<Vec<_>>();
    stream_blocks(source, cancel, false, |block| {
        for (index, (domain, router)) in domains.iter().zip(&routers).enumerate() {
            let Some(total) = &mut totals[index] else {
                continue;
            };
            for slice in slices(block.len(), count, CHUNK_ROWS) {
                let mut used = 0;
                for ((range, _), rows) in chunks.iter_mut().zip(chunk_ranges(slice, count)) {
                    *range = rows;
                    used += 1;
                }
                let rows = total.len();
                team.for_each_mut(&mut chunks[..used], |_, (range, work)| {
                    work.clear();
                    work.resize(rows, 0);
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
                                }
                            },
                        )?;
                    }
                    Ok::<_, PassError>(())
                })?;
                for (_, work) in &chunks[..used] {
                    for (total, work) in total.iter_mut().zip(work) {
                        *total += work;
                    }
                }
            }
        }
        Ok(())
    })?;
    Ok(totals)
}

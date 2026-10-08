// SPDX-License-Identifier: LGPL-3.0-or-later
//! One wave of a major-cycle pass: placement on row chunks, accumulation by
//! owner, and the per-plane transforms.

use std::ops::Range;

use casa_imaging_operator::{
    CpuBackend, GridAccumulator, GridBackend, Mode, NormalImages, PlaneRange, PredictionScratch,
    PreparedModelGrids, SampleBuffer, Work,
};
use num_complex::Complex32;

use super::partition::Router;
use super::{MajorCyclePass, NativeBlock, Partition, PassError, VisibilitySink, WorkerTeam};

/// Row chunks per worker in the placement stage; enough to balance rows
/// whose channel counts differ after flagging and the support test.
const CHUNKS_PER_WORKER: usize = 4;

/// One owner's accumulator and backend scratch.
struct Owner {
    acc: Option<GridAccumulator>,
    backend: CpuBackend,
    images: Option<NormalImages>,
}

/// One row chunk's placements, routed by owner, and its prediction scratch.
struct Chunk {
    rows: Range<usize>,
    scratch: SampleBuffer,
    owned: Vec<SampleBuffer>,
    placed: u64,
    backend: CpuBackend,
    prediction: PredictionScratch,
}

pub(super) struct Wave<'w, 'p> {
    pass: &'w MajorCyclePass<'p>,
    planes: PlaneRange,
    router: Router,
    model: Option<&'w PreparedModelGrids>,
    owners: Vec<Owner>,
    chunks: Vec<Chunk>,
    predictions: Vec<Complex32>,
    samples: u64,
}

impl<'w, 'p> Wave<'w, 'p> {
    pub(super) fn new(
        pass: &'w MajorCyclePass<'p>,
        planes: PlaneRange,
        model: Option<&'w PreparedModelGrids>,
    ) -> Self {
        let router = Router::new(&pass.partition, planes);
        let owners = (0..router.owners())
            .map(|owner| {
                let (range, tile) = router.target(&pass.partition, planes, owner);
                Owner {
                    acc: Some(pass.operator.accumulator(range, tile, pass.modes)),
                    backend: CpuBackend::new(),
                    images: None,
                }
            })
            .collect();
        Self {
            pass,
            planes,
            router,
            model,
            owners,
            chunks: Vec::new(),
            predictions: Vec::new(),
            samples: 0,
        }
    }

    /// Samples placed so far.
    pub(super) const fn samples(&self) -> u64 {
        self.samples
    }

    /// Place every row of `block` and accumulate the placements; with a
    /// visibility sink, first hand it the block, with every selected
    /// sample's prediction from the wave's model when it asks for them.
    pub(super) fn consume(
        &mut self,
        block: &NativeBlock,
        team: &WorkerTeam,
        visibilities: Option<&mut VisibilitySink<'_>>,
    ) -> Result<(), PassError> {
        let npol = self.pass.operator.polarization().correlations().len();
        let owners = self.owners.len();
        let rows = block.len();
        let count = (team.workers() * CHUNKS_PER_WORKER).clamp(1, rows.max(1));
        if self.chunks.len() < count {
            self.chunks.resize_with(count, || Chunk {
                rows: 0..0,
                scratch: SampleBuffer::new(npol),
                owned: (0..owners).map(|_| SampleBuffer::new(npol)).collect(),
                placed: 0,
                backend: CpuBackend::new(),
                prediction: PredictionScratch::default(),
            });
        }
        if let Some(sink) = visibilities {
            if sink.predictions {
                self.predict(block, team, count)?;
            } else {
                self.predictions.clear();
            }
            (sink.write)(block, &self.predictions).map_err(PassError::VisibilityWrite)?;
        }
        for (index, chunk) in self.chunks[..count].iter_mut().enumerate() {
            chunk.rows = index * rows / count..(index + 1) * rows / count;
        }
        let pass = self.pass;
        let router = &self.router;
        let planes = self.planes;
        team.for_each_mut(&mut self.chunks[..count], |_, chunk| {
            chunk.placed = 0;
            chunk.owned.iter_mut().for_each(SampleBuffer::clear);
            for row in chunk.rows.clone() {
                chunk.scratch.clear();
                pass.resampler.place(
                    pass.operator,
                    pass.weighting,
                    &block.row(row),
                    &mut chunk.scratch,
                )?;
                let placed = chunk.scratch.block();
                for (index, placement) in placed.placements.iter().enumerate() {
                    if !planes.contains(placement.plane) {
                        continue;
                    }
                    let owner = router.owner(pass.operator, placement);
                    chunk.owned[owner].push(
                        *placement,
                        placed.values_of(index),
                        placed.weights_of(index),
                    );
                    chunk.placed += 1;
                }
            }
            Ok::<_, PassError>(())
        })?;
        let chunks = &self.chunks[..count];
        self.samples += chunks.iter().map(|chunk| chunk.placed).sum::<u64>();
        let model = self.model;
        team.for_each_mut(&mut self.owners, |owner_index, owner| {
            let acc = owner
                .acc
                .as_mut()
                .expect("owners accumulate until finished");
            for chunk in chunks {
                let block = chunk.owned[owner_index].block();
                if block.is_empty() {
                    continue;
                }
                accumulate(pass, &mut owner.backend, &block, model, acc)?;
            }
            Ok::<_, PassError>(())
        })
    }

    /// Model visibilities of every selected sample of `block`, `[row]
    /// [channel][correlation]`, into `self.predictions`; zero without a model.
    fn predict(
        &mut self,
        block: &NativeBlock,
        team: &WorkerTeam,
        count: usize,
    ) -> Result<(), PassError> {
        let cells = block.channels() * block.correlations();
        let rows = block.len();
        self.predictions.clear();
        self.predictions.resize(rows * cells, Complex32::default());
        let Some(model) = self.model else {
            return Ok(());
        };
        let pass = self.pass;
        let mut pieces = Vec::with_capacity(count);
        let mut remaining = self.predictions.as_mut_slice();
        for (index, chunk) in self.chunks[..count].iter_mut().enumerate() {
            let rows = index * rows / count..(index + 1) * rows / count;
            let (head, tail) = remaining.split_at_mut(rows.len() * cells);
            remaining = tail;
            pieces.push((rows, chunk, head));
        }
        team.for_each_mut(&mut pieces, |_, (rows, chunk, out)| {
            for (local, row) in rows.clone().enumerate() {
                pass.resampler.predict_row(
                    pass.operator,
                    &mut chunk.backend,
                    model,
                    &block.row(row),
                    &mut chunk.prediction,
                    &mut out[local * cells..(local + 1) * cells],
                )?;
            }
            Ok::<_, PassError>(())
        })
    }

    /// Transform the accumulated grids into this wave's normal images.
    pub(super) fn finish(mut self, team: &WorkerTeam) -> Result<NormalImages, PassError> {
        let operator = self.pass.operator;
        match &self.pass.partition {
            Partition::Planes { .. } => {
                team.for_each_mut(&mut self.owners, |_, owner| {
                    let acc = owner.acc.take().expect("each owner finishes once");
                    owner.images = Some(operator.finish(acc)?);
                    Ok::<_, PassError>(())
                })?;
                Ok(concatenate(
                    self.owners
                        .into_iter()
                        .map(|owner| owner.images.expect("every owner finished")),
                ))
            }
            Partition::Regions(_) => {
                let mut merged = operator.accumulator(self.planes, None, self.pass.modes);
                for owner in &self.owners {
                    merged.merge_from(owner.acc.as_ref().expect("owners hold their tiles"))?;
                }
                drop(self.owners);
                Ok(operator.finish(merged)?)
            }
        }
    }
}

/// Grid one owner's share of a block in every mode the pass holds; with a
/// model the data terms receive the residual `V − A·m`.
fn accumulate(
    pass: &MajorCyclePass<'_>,
    backend: &mut CpuBackend,
    block: &casa_imaging_operator::SampleBlock<'_>,
    model: Option<&PreparedModelGrids>,
    acc: &mut GridAccumulator,
) -> Result<(), PassError> {
    let cf = pass.operator.cf();
    if pass.modes.data {
        let work = match model {
            Some(model) => Work::ResidualGrid {
                model,
                acc,
                residual_out: None,
            },
            None => Work::Grid {
                mode: Mode::Data,
                acc,
            },
        };
        backend.apply(block, cf, work)?;
    }
    if pass.modes.psf {
        backend.apply(
            block,
            cf,
            Work::Grid {
                mode: Mode::Psf,
                acc,
            },
        )?;
    }
    if pass.modes.weight {
        backend.apply(
            block,
            cf,
            Work::Grid {
                mode: Mode::Weight,
                acc,
            },
        )?;
    }
    Ok(())
}

/// Join per-owner images of consecutive plane ranges.
pub(super) fn concatenate(parts: impl IntoIterator<Item = NormalImages>) -> NormalImages {
    let mut parts = parts.into_iter();
    let mut joined = parts.next().expect("a wave has at least one owner");
    for part in parts {
        assert!(
            part.pols == joined.pols
                && part.data_terms == joined.data_terms
                && part.psf_terms == joined.psf_terms
                && part.first_plane == joined.first_plane + joined.planes.len() as u32,
            "owner images must continue the plane sequence with the same layout"
        );
        joined.planes.extend(part.planes);
    }
    joined
}

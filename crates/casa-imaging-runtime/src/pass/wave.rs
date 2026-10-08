// SPDX-License-Identifier: LGPL-3.0-or-later
//! One wave of a major-cycle pass: prediction and placement on row chunks,
//! accumulation by owner for each image domain, and the per-plane
//! transforms.

use std::ops::Range;

use casa_imaging_operator::{
    CpuBackend, GridAccumulator, GridBackend, Mode, NativeRow, NormalImages, PlaneRange,
    PredictionScratch, PreparedModelGrids, SampleBuffer, Work,
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

/// One image domain's share of a wave.
struct Domain<'w> {
    router: Router,
    owners: Vec<Owner>,
    model: Option<&'w PreparedModelGrids>,
}

/// One row chunk's placements, routed by owner, its prediction scratch and
/// its rows' native-channel residuals.
struct Chunk {
    rows: Range<usize>,
    scratch: SampleBuffer,
    owned: Vec<SampleBuffer>,
    placed: u64,
    backend: CpuBackend,
    prediction: PredictionScratch,
    predicted: Vec<Complex32>,
    residual: Vec<Complex32>,
}

pub(super) struct Wave<'w, 'p> {
    pass: &'w MajorCyclePass<'p>,
    planes: PlaneRange,
    domains: Vec<Domain<'w>>,
    native_residuals: bool,
    check_spacing: bool,
    chunks: Vec<Chunk>,
    predictions: Vec<Complex32>,
    samples: u64,
}

impl<'w, 'p> Wave<'w, 'p> {
    /// A wave over `planes` with each domain's prepared `models`, forming
    /// residuals at native channels when `native_residuals`.
    pub(super) fn new(
        pass: &'w MajorCyclePass<'p>,
        planes: PlaneRange,
        models: Option<&'w [PreparedModelGrids]>,
        native_residuals: bool,
    ) -> Self {
        let domains = pass
            .domains
            .iter()
            .enumerate()
            .map(|(index, domain)| {
                let router = Router::new(&domain.partition, planes);
                let owners = (0..router.owners())
                    .map(|owner| {
                        let (range, tile) = router.target(&domain.partition, planes, owner);
                        Owner {
                            acc: Some(domain.operator.accumulator(range, tile, pass.modes)),
                            backend: CpuBackend::new(),
                            images: None,
                        }
                    })
                    .collect();
                Domain {
                    router,
                    owners,
                    model: models.map(|models| &models[index]),
                }
            })
            .collect();
        Self {
            pass,
            planes,
            domains,
            native_residuals,
            check_spacing: false,
            chunks: Vec::new(),
            predictions: Vec::new(),
            samples: 0,
        }
    }

    /// Check every row's native spacing against the pass's
    /// `native_spacing_hz`, which sized this wave's model halo.
    pub(super) fn checking_spacing(mut self, check: bool) -> Self {
        self.check_spacing = check;
        self
    }

    /// Samples placed on the first domain so far.
    pub(super) const fn samples(&self) -> u64 {
        self.samples
    }

    /// Place every row of `block` on every domain and accumulate the
    /// placements; with a visibility sink, first hand it the block, with
    /// every selected sample's prediction when it asks for them.
    ///
    /// When the wave forms residuals at native channels, each row's samples
    /// are placed from `V − Σ_d A_d·m_d` at its native channels and gridded
    /// as data; otherwise the domain's model is subtracted at each placed
    /// sample.
    pub(super) fn consume(
        &mut self,
        block: &NativeBlock,
        team: &WorkerTeam,
        visibilities: Option<&mut VisibilitySink<'_>>,
    ) -> Result<(), PassError> {
        let npol = block.correlations();
        let rows = block.len();
        if self.check_spacing {
            let bound_hz = self.pass.native_spacing_hz;
            for row in 0..rows {
                let observed_hz = block
                    .row(0, row)
                    .frequencies_hz
                    .windows(2)
                    .fold(0.0_f64, |widest, pair| {
                        widest.max((pair[1] - pair[0]).abs())
                    });
                if observed_hz > bound_hz {
                    return Err(PassError::NativeSpacing {
                        observed_hz,
                        bound_hz,
                    });
                }
            }
        }
        let count = (team.workers() * CHUNKS_PER_WORKER).clamp(1, rows.max(1));
        if self.chunks.len() < count {
            self.chunks.resize_with(count, || Chunk {
                rows: 0..0,
                scratch: SampleBuffer::new(npol),
                owned: Vec::new(),
                placed: 0,
                backend: CpuBackend::new(),
                prediction: PredictionScratch::default(),
                predicted: Vec::new(),
                residual: Vec::new(),
            });
        }
        for (index, chunk) in self.chunks[..count].iter_mut().enumerate() {
            chunk.rows = index * rows / count..(index + 1) * rows / count;
        }
        let sink_predictions = visibilities.as_ref().is_some_and(|sink| sink.predictions);
        if self.native_residuals || sink_predictions {
            self.predict(block, team, count)?;
        } else {
            self.predictions.clear();
        }
        if let Some(sink) = visibilities {
            let predictions: &[Complex32] = if sink.predictions {
                &self.predictions
            } else {
                &[]
            };
            (sink.write)(block, predictions).map_err(PassError::VisibilityWrite)?;
        }
        for index in 0..self.domains.len() {
            let placed = self.place(block, team, count, index)?;
            if index == 0 {
                self.samples += placed;
            }
        }
        Ok(())
    }

    /// Place the rows of `block` on domain `index` and accumulate them;
    /// returns the samples placed.
    fn place(
        &mut self,
        block: &NativeBlock,
        team: &WorkerTeam,
        count: usize,
        index: usize,
    ) -> Result<u64, PassError> {
        let pass = self.pass;
        let target = &pass.domains[index];
        let domain = &mut self.domains[index];
        let owners = domain.owners.len();
        let router = &domain.router;
        let planes = self.planes;
        let predictions = &self.predictions;
        let native_residuals = self.native_residuals;
        let cells = block.channels() * block.correlations();
        team.for_each_mut(&mut self.chunks[..count], |_, chunk| {
            chunk.placed = 0;
            chunk
                .owned
                .resize_with(owners, || SampleBuffer::new(block.correlations()));
            chunk.owned[..owners]
                .iter_mut()
                .for_each(SampleBuffer::clear);
            for row in chunk.rows.clone() {
                chunk.scratch.clear();
                let native = block.row(index, row);
                let native = if native_residuals {
                    let model = &predictions[row * cells..(row + 1) * cells];
                    chunk.residual.clear();
                    chunk.residual.extend(
                        native
                            .values
                            .iter()
                            .zip(model)
                            .map(|(value, model)| value - model),
                    );
                    NativeRow {
                        values: &chunk.residual,
                        ..native
                    }
                } else {
                    native
                };
                target.resampler.place(
                    target.operator,
                    pass.weighting,
                    &native,
                    &mut chunk.scratch,
                )?;
                let placed = chunk.scratch.block();
                for (sample, placement) in placed.placements.iter().enumerate() {
                    if !planes.contains(placement.plane) {
                        continue;
                    }
                    let owner = router.owner(target.operator, placement);
                    chunk.owned[owner].push(
                        *placement,
                        placed.values_of(sample),
                        placed.weights_of(sample),
                    );
                    chunk.placed += 1;
                }
            }
            Ok::<_, PassError>(())
        })?;
        let chunks = &self.chunks[..count];
        let model = domain.model.filter(|_| !native_residuals);
        team.for_each_mut(&mut domain.owners, |owner_index, owner| {
            let acc = owner
                .acc
                .as_mut()
                .expect("owners accumulate until finished");
            for chunk in chunks {
                let block = chunk.owned[owner_index].block();
                if block.is_empty() {
                    continue;
                }
                accumulate(
                    pass,
                    target.operator,
                    &mut owner.backend,
                    &block,
                    model,
                    acc,
                )?;
            }
            Ok::<_, PassError>(())
        })?;
        Ok(chunks.iter().map(|chunk| chunk.placed).sum())
    }

    /// Model visibilities of every selected sample of `block`, summed over
    /// every domain's model (`SIMapperCollection::degrid`), `[row][channel]
    /// [correlation]`, into `self.predictions`; zero without a model.
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
        if self.domains.iter().all(|domain| domain.model.is_none()) {
            return Ok(());
        }
        let pass = self.pass;
        let domains = &self.domains;
        let mut pieces = Vec::with_capacity(count);
        let mut remaining = self.predictions.as_mut_slice();
        for chunk in &mut self.chunks[..count] {
            let (head, tail) = remaining.split_at_mut(chunk.rows.len() * cells);
            remaining = tail;
            pieces.push((chunk, head));
        }
        team.for_each_mut(&mut pieces, |_, (chunk, out)| {
            chunk.predicted.resize(cells, Complex32::default());
            for (local, row) in chunk.rows.clone().enumerate() {
                let out = &mut out[local * cells..(local + 1) * cells];
                for (index, (target, domain)) in pass.domains.iter().zip(domains).enumerate() {
                    let Some(model) = domain.model else {
                        continue;
                    };
                    target.resampler.predict_row(
                        target.operator,
                        &mut chunk.backend,
                        model,
                        &block.row(index, row),
                        &mut chunk.prediction,
                        &mut chunk.predicted,
                    )?;
                    for (sum, value) in out.iter_mut().zip(&chunk.predicted) {
                        *sum += value;
                    }
                }
            }
            Ok::<_, PassError>(())
        })
    }

    /// Transform the accumulated grids into this wave's normal images, one
    /// per domain.
    pub(super) fn finish(self, team: &WorkerTeam) -> Result<Vec<NormalImages>, PassError> {
        let pass = self.pass;
        let planes = self.planes;
        self.domains
            .into_iter()
            .zip(pass.domains)
            .map(|(mut domain, target)| {
                let operator = target.operator;
                match &target.partition {
                    Partition::Planes { .. } => {
                        team.for_each_mut(&mut domain.owners, |_, owner| {
                            let acc = owner.acc.take().expect("each owner finishes once");
                            owner.images = Some(operator.finish(acc)?);
                            Ok::<_, PassError>(())
                        })?;
                        Ok(concatenate(
                            domain
                                .owners
                                .into_iter()
                                .map(|owner| owner.images.expect("every owner finished")),
                        ))
                    }
                    Partition::Regions { .. } => {
                        let mut merged = operator.accumulator(planes, None, pass.modes);
                        for owner in &domain.owners {
                            merged
                                .merge_from(owner.acc.as_ref().expect("owners hold their tiles"))?;
                        }
                        drop(domain.owners);
                        Ok(operator.finish(merged)?)
                    }
                }
            })
            .collect()
    }
}

/// Grid one owner's share of a block in every mode the pass holds; with a
/// model the data terms receive the residual `V − A·m`.
fn accumulate(
    pass: &MajorCyclePass<'_>,
    operator: &casa_imaging_operator::MeasurementOperator,
    backend: &mut CpuBackend,
    block: &casa_imaging_operator::SampleBlock<'_>,
    model: Option<&PreparedModelGrids>,
    acc: &mut GridAccumulator,
) -> Result<(), PassError> {
    let cf = operator.cf();
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

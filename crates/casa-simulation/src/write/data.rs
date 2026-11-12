// SPDX-License-Identifier: LGPL-3.0-or-later
//! Execute selected DATA phases in call order, sharing the existing numerical
//! stages and keeping the ordinary all-row workload fused in one phase.

use std::time::Duration;

use casa_imaging_runtime::pass::{BackendChoice, WorkerTeam};
use casa_types::Complex32;

use super::components::add_components;
use super::geometry::RowGeometry;
use super::pipeline::{NoiseContext, batch_scales, noise_jobs};
use super::prediction::{PredictionStage, device_noise};
use super::resources::NoiseScales;
use crate::session::Simulator;
use crate::session::error::SimulatorResult;
use crate::session::plan::DataPhase;

type Run = (usize, usize, usize);

struct PhaseStage<'s> {
    phase: &'s DataPhase,
    prediction: Option<PredictionStage<'s>>,
}

pub(super) struct DataBatch<'a> {
    pub(super) rows: &'a [RowGeometry],
    pub(super) runs: &'a [Run],
    pub(super) first_row: u64,
    pub(super) flags: Option<&'a [bool]>,
    pub(super) stored: Option<&'a [Complex32]>,
}

/// The last phase's corruption can share the fresh writer's scalar jobs.
pub(super) struct PendingNoise<'a, 's> {
    pub(super) plan: &'s DataPhase,
    pub(super) runs: &'a [Run],
    prediction: Option<&'a PredictionStage<'s>>,
    host_noise: bool,
}

impl PendingNoise<'_, '_> {
    pub(super) fn noised(&self, scan: usize) -> bool {
        self.prediction
            .is_some_and(|stage| stage.noised(scan, device_noise(self.plan, scan, self.host_noise)))
    }
}

pub(super) struct DataStage<'s> {
    session: &'s Simulator,
    phases: Vec<PhaseStage<'s>>,
    correlations: usize,
    samples: usize,
    backend: BackendChoice,
    scratch: Vec<Complex32>,
    selected_runs: Vec<Run>,
    last_host_noise: bool,
    receptors: Vec<Vec<[usize; 2]>>,
}

impl<'s> DataStage<'s> {
    pub(super) fn new(
        session: &'s Simulator,
        shape: (usize, usize),
        backend: BackendChoice,
    ) -> SimulatorResult<Option<Self>> {
        if session.data_plan().is_empty() {
            return Ok(None);
        }
        let phases = session
            .data_plan()
            .phases()
            .iter()
            .map(|phase| {
                Ok(PhaseStage {
                    phase,
                    prediction: PredictionStage::new(session, shape, backend, phase)?,
                })
            })
            .collect::<SimulatorResult<Vec<_>>>()?;
        let backend = phases
            .iter()
            .filter_map(|phase| phase.prediction.as_ref())
            .map(PredictionStage::backend)
            .next()
            .unwrap_or_else(|| session.options().resources.resolved_backend(false));
        Ok(Some(Self {
            session,
            phases,
            correlations: shape.0,
            samples: shape.0 * shape.1,
            backend,
            scratch: Vec::new(),
            selected_runs: Vec::new(),
            last_host_noise: false,
            receptors: super::window_receptors(session),
        }))
    }

    pub(super) fn backend(&self) -> BackendChoice {
        self.backend
    }

    pub(super) fn fill_time(&self) -> Duration {
        self.phases
            .iter()
            .filter_map(|phase| phase.prediction.as_ref())
            .map(PredictionStage::fill_time)
            .sum()
    }

    pub(super) fn grid_preparations(&self) -> (usize, Duration) {
        self.phases
            .iter()
            .filter_map(|phase| phase.prediction.as_ref())
            .map(PredictionStage::grid_preparations)
            .fold((0, Duration::ZERO), |a, b| (a.0 + b.0, a.1 + b.1))
    }

    pub(super) fn minimum_grid_bytes(&self) -> u64 {
        self.phases
            .iter()
            .filter_map(|phase| phase.prediction.as_ref())
            .map(PredictionStage::minimum_grid_bytes)
            .sum()
    }

    pub(super) fn extra_grid_bytes(&self) -> u64 {
        self.phases
            .iter()
            .filter_map(|phase| phase.prediction.as_ref())
            .map(PredictionStage::extra_grid_bytes)
            .sum()
    }

    pub(super) fn small_block_rows(&self, rows: usize) -> usize {
        self.phases
            .iter()
            .filter_map(|phase| phase.prediction.as_ref())
            .map(|stage| stage.small_block_rows(rows))
            .min()
            .unwrap_or(rows)
    }

    pub(super) fn bytes(&self, rows: usize, workers: usize, batch_rows: usize) -> u64 {
        let stages: u64 = self
            .phases
            .iter()
            .filter_map(|phase| phase.prediction.as_ref())
            .map(|stage| stage.bytes(rows, workers))
            .sum();
        let scratch = if self.phases.len() > 1 {
            batch_rows * self.samples * size_of::<Complex32>()
        } else {
            0
        };
        let runs = if self.session.data_plan().has_selection() {
            batch_rows.min(self.session.scans().len()) * size_of::<Run>()
        } else {
            0
        };
        let receptors = self.receptors.capacity() * size_of::<Vec<[usize; 2]>>()
            + self
                .receptors
                .iter()
                .map(|window| window.capacity() * size_of::<[usize; 2]>())
                .sum::<usize>();
        stages
            + (scratch + runs + receptors + self.phases.capacity() * size_of::<PhaseStage<'_>>())
                as u64
    }

    pub(super) fn start(
        &mut self,
        rows: usize,
        workers: usize,
        grid_bytes: u64,
        batch_rows: usize,
    ) {
        if self.phases.len() > 1 {
            self.scratch = Vec::with_capacity(batch_rows * self.samples);
        }
        if self.session.data_plan().has_selection() {
            self.selected_runs = Vec::with_capacity(batch_rows.min(self.session.scans().len()));
        }
        let mut extra = grid_bytes.saturating_sub(self.minimum_grid_bytes());
        for phase in &mut self.phases {
            if let Some(stage) = &mut phase.prediction {
                let take = extra.min(stage.extra_grid_bytes());
                extra -= take;
                stage.start(rows, workers, stage.minimum_grid_bytes() + take);
            }
        }
    }

    pub(super) fn pending_noise<'a>(&'a self, runs: &'a [Run]) -> PendingNoise<'a, 's> {
        let phase = self.phases.last().expect("nonempty DATA program");
        PendingNoise {
            plan: phase.phase,
            runs: if phase.phase.selection.is_all() {
                runs
            } else {
                &self.selected_runs
            },
            prediction: phase.prediction.as_ref(),
            host_noise: self.last_host_noise,
        }
    }

    pub(super) fn apply(
        &mut self,
        team: &WorkerTeam,
        batch: DataBatch<'_>,
        data: &mut [Complex32],
        scales: &NoiseScales,
        defer_last_noise: bool,
    ) -> SimulatorResult<()> {
        let last = self.phases.len() - 1;
        let scans = self.session.scans();
        for (index, phase) in self.phases.iter_mut().enumerate() {
            let runs =
                if phase.phase.selection.is_all() {
                    batch.runs
                } else {
                    self.selected_runs.clear();
                    self.selected_runs.extend(
                        batch.runs.iter().copied().filter(|&(scan, _, _)| {
                            phase.phase.selection.contains_scan(&scans[scan])
                        }),
                    );
                    &self.selected_runs
                };
            let host_noise = batch.stored.is_some()
                || (index > 0 && !phase.phase.replace)
                || batch
                    .flags
                    .is_some_and(|flags| flags.iter().any(|&flag| flag));
            self.last_host_noise = host_noise;
            if index == 0 {
                predict_phase(
                    phase,
                    team,
                    self.session,
                    &batch,
                    runs,
                    data,
                    self.samples,
                    host_noise,
                )?;
                combine_stored(phase.phase, runs, data, batch.stored, self.samples);
            } else {
                self.scratch.clear();
                self.scratch.resize(data.len(), Complex32::new(0.0, 0.0));
                predict_phase(
                    phase,
                    team,
                    self.session,
                    &batch,
                    runs,
                    &mut self.scratch,
                    self.samples,
                    host_noise,
                )?;
                for &(_, start, count) in runs {
                    let range = start * self.samples..(start + count) * self.samples;
                    for (value, &model) in data[range.clone()].iter_mut().zip(&self.scratch[range])
                    {
                        if phase.phase.replace {
                            *value = model;
                        } else {
                            *value += model;
                        }
                    }
                }
            }
            if !defer_last_noise || index != last {
                let pending = PendingNoise {
                    plan: phase.phase,
                    runs,
                    prediction: phase.prediction.as_ref(),
                    host_noise,
                };
                apply_noise(
                    &pending,
                    team,
                    self.session,
                    &batch,
                    data,
                    (&self.receptors, scales),
                    (self.correlations, self.samples),
                )?;
            }
        }
        Ok(())
    }
}

fn predict_phase(
    phase: &mut PhaseStage<'_>,
    team: &WorkerTeam,
    session: &Simulator,
    batch: &DataBatch<'_>,
    runs: &[Run],
    data: &mut [Complex32],
    samples: usize,
    host_noise: bool,
) -> SimulatorResult<()> {
    let plan = phase.phase;
    let scans = session.scans();
    if let Some(stage) = &mut phase.prediction {
        stage.predict(
            team,
            (scans, batch.rows),
            runs,
            batch.first_row,
            data,
            |scan| device_noise(plan, scan, host_noise),
        )?;
    }
    if plan.predicts_single_dish() {
        super::single_dish::add_single_dish((scans, batch.rows), plan, runs, (data, samples));
    }
    if let Some(flags) = batch.flags {
        for (value, &flag) in data.iter_mut().zip(flags) {
            if flag {
                *value = Complex32::new(0.0, 0.0);
            }
        }
    }
    if plan.predicts_components() {
        add_components(team, (scans, batch.rows), plan, runs, (data, samples))?;
    }
    Ok(())
}

fn combine_stored(
    phase: &DataPhase,
    runs: &[Run],
    data: &mut [Complex32],
    stored: Option<&[Complex32]>,
    samples: usize,
) {
    let Some(stored) = stored else {
        return;
    };
    if phase.replace {
        let mut position = 0;
        for &(_, start, count) in runs {
            data[position * samples..start * samples]
                .copy_from_slice(&stored[position * samples..start * samples]);
            position = start + count;
        }
        data[position * samples..].copy_from_slice(&stored[position * samples..]);
    } else {
        for (value, stored) in data.iter_mut().zip(stored) {
            *value += *stored;
        }
    }
}

fn apply_noise(
    pending: &PendingNoise<'_, '_>,
    team: &WorkerTeam,
    session: &Simulator,
    batch: &DataBatch<'_>,
    data: &mut [Complex32],
    (receptors, bound): (&[Vec<[usize; 2]>], &NoiseScales),
    (correlations, samples): (usize, usize),
) -> SimulatorResult<()> {
    let noised = |scan| pending.noised(scan);
    let scales = batch_scales(
        (pending.plan, NoiseContext::of(session), team),
        (batch.rows, pending.runs),
        &noised,
        bound,
    )?;
    let mut jobs = noise_jobs(
        (pending.plan, NoiseContext::of(session), team),
        receptors,
        (batch.rows, pending.runs),
        (batch.first_row, data),
        (samples, &scales),
        &noised,
    );
    team.for_each_mut(&mut jobs, |_, job| {
        job.run(correlations, samples);
        Ok(())
    })
}

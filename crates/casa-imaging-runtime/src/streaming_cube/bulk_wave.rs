// SPDX-License-Identifier: LGPL-3.0-or-later

//! Simultaneously resident bands consume one borrowed MS block per refill.

use crate::{bounded_stream::BoundedExecution, weighting::bulk_source::BulkConsumer};
use casa_imaging_reconstruction::runtime_adapter::{
    BandPlan, BandResult, EpochBand, NativeBlockView, NativeLayout,
};
use casa_imaging_reconstruction::{ModelGeneration, PolarizationOperator};
use std::io;

#[derive(Clone)]
struct WaveCharge {
    resident: u64,
    transitions: Vec<u64>,
    workers: usize,
}

impl WaveCharge {
    fn new(workers: usize) -> io::Result<Self> {
        if workers == 0 {
            return Err(io::Error::other("bulk wave requires a worker"));
        }
        Ok(Self {
            resident: 0,
            transitions: Vec::with_capacity(workers),
            workers,
        })
    }

    fn include(&mut self, resident: usize, transition: usize) -> io::Result<u64> {
        let resident =
            u64::try_from(resident).map_err(|_| io::Error::other("bulk resident size overflow"))?;
        let transition = u64::try_from(transition)
            .map_err(|_| io::Error::other("bulk transition size overflow"))?;
        self.resident = self
            .resident
            .checked_add(resident)
            .and_then(|bytes| {
                bytes.checked_add((size_of::<Job<'_>>() + size_of::<BandResult>()) as u64)
            })
            .ok_or_else(|| io::Error::other("bulk resident size overflow"))?;
        let index = self
            .transitions
            .partition_point(|&bytes| bytes >= transition);
        if index < self.workers {
            self.transitions.insert(index, transition);
            self.transitions.truncate(self.workers);
        }
        self.transitions
            .iter()
            .try_fold(self.resident, |sum, bytes| {
                sum.checked_add(*bytes)
                    .ok_or_else(|| io::Error::other("bulk wave size overflow"))
            })
    }
}

// Inline state is charged by WaveCharge; transitions need no extra heap owner.
#[allow(clippy::large_enum_variant)]
enum Job<'a> {
    Pending(BandPlan),
    Active(EpochBand<'a>),
    Finished(BandResult),
    Taken,
}

pub(super) struct BulkWave<'a> {
    jobs: Vec<Job<'a>>,
    model: &'a ModelGeneration,
    output_hz: &'a [f64],
    polarization: &'a PolarizationOperator,
    discover: Option<&'a mut [BandPlan]>,
}

impl<'a> BulkWave<'a> {
    pub(super) fn new(
        jobs: Vec<BandPlan>,
        model: &'a ModelGeneration,
        output_hz: &'a [f64],
        polarization: &'a PolarizationOperator,
        discover: Option<&'a mut [BandPlan]>,
    ) -> Self {
        Self {
            jobs: jobs.into_iter().map(Job::Pending).collect(),
            model,
            output_hz,
            polarization,
            discover,
        }
    }

    /// Every band retains its stable capacity. Model loading and conversion are
    /// joined synchronous leaf jobs, so at most `workers` transition deltas
    /// coexist even when the wave contains more bands.
    pub(super) fn prefix(bands: &[BandPlan], budget: u64, workers: usize) -> io::Result<usize> {
        let mut charge = WaveCharge::new(workers)?;
        let mut count = 0;
        for band in bands {
            let memory = band.memory().map_err(io::Error::other)?;
            if charge.include(memory.resident_bytes(), memory.transition_bytes())? > budget {
                break;
            }
            count += 1;
        }
        if count == 0 {
            return Err(io::Error::new(
                io::ErrorKind::OutOfMemory,
                "bulk wave cannot fit one output band",
            ));
        }
        Ok(count)
    }

    pub(super) fn bytes(bands: &[BandPlan], workers: usize) -> io::Result<u64> {
        let mut charge = WaveCharge::new(workers)?;
        let mut bytes = 0;
        for band in bands {
            let memory = band.memory().map_err(io::Error::other)?;
            bytes = charge.include(memory.resident_bytes(), memory.transition_bytes())?;
        }
        Ok(bytes)
    }
}

impl BulkConsumer for BulkWave<'_> {
    type Completion = Vec<BandResult>;

    fn consume(
        &mut self,
        block: NativeBlockView<'_>,
        layout: &NativeLayout,
        selected_channels: std::ops::Range<usize>,
        execution: BoundedExecution<'_>,
    ) -> io::Result<()> {
        if let Some(bands) = self.discover.as_deref_mut() {
            BandPlan::observe_borrowed(bands, block, self.output_hz).map_err(io::Error::other)?;
        }
        let model = self.model;
        let output_hz = self.output_hz;
        let polarization = self.polarization;
        execution.for_each_mut(&mut self.jobs, |_, job| {
            if matches!(job, Job::Pending(_)) {
                let Job::Pending(plan) = std::mem::replace(job, Job::Taken) else {
                    unreachable!()
                };
                *job = Job::Active(plan.prepare(model, None).map_err(io::Error::other)?);
            }
            let Job::Active(band) = job else {
                return Err(io::Error::other("bulk band has already completed"));
            };
            band.consume_source_window(
                block,
                layout,
                selected_channels.clone(),
                output_hz,
                polarization,
            )
            .map_err(io::Error::other)
        })
    }

    fn complete(mut self, execution: BoundedExecution<'_>) -> io::Result<Self::Completion> {
        let model = self.model;
        execution.for_each_mut(&mut self.jobs, |_, job| {
            let active = match std::mem::replace(job, Job::Taken) {
                Job::Pending(plan) => plan.prepare(model, None).map_err(io::Error::other)?,
                Job::Active(band) => band,
                _ => return Err(io::Error::other("bulk band completed twice")),
            };
            let (result, _) = active.complete(model).map_err(io::Error::other)?;
            *job = Job::Finished(result);
            Ok(())
        })?;
        self.jobs
            .into_iter()
            .map(|job| match job {
                Job::Finished(result) => Ok(result),
                _ => Err(io::Error::other("bulk band did not complete")),
            })
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::WaveCharge;
    use std::mem::size_of;

    #[test]
    fn wave_charge_retains_every_band_but_only_the_largest_worker_transitions() {
        for workers in [1, 2, 3, 4] {
            let mut charge = WaveCharge::new(workers).unwrap();
            let mut observed = Vec::new();
            let mut residents = 0_u64;
            for (resident, transient) in [(100, 50), (200, 80), (300, 20), (400, 70), (500, 90)] {
                residents += resident as u64;
                let actual = charge.include(resident, transient).unwrap();
                observed.push(transient as u64);
                observed.sort_unstable_by(|a, b| b.cmp(a));
                let headers = (size_of::<super::Job<'_>>()
                    + size_of::<casa_imaging_reconstruction::runtime_adapter::BandResult>())
                    as u64;
                let expected = residents
                    + observed.iter().take(workers).sum::<u64>()
                    + observed.len() as u64 * headers;
                assert_eq!(actual, expected);
            }
        }
        assert!(WaveCharge::new(0).is_err());
    }
}

// SPDX-License-Identifier: LGPL-3.0-or-later

//! Simultaneously resident bands consume one borrowed MS block per refill.

use crate::{bounded_stream::BoundedExecution, weighting::bulk_source::BulkConsumer};
use casa_imaging_reconstruction::runtime_adapter::{
    BandPlan, BandResult, EpochBand, NativeBlockView, NativeLayout,
};
use casa_imaging_reconstruction::{ModelGeneration, PolarizationOperator};
use std::io;

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

    /// Every band is live during a source traversal, independent of the number
    /// of threads scheduling those bands. Include output/FFT overlap per band.
    pub(super) fn prefix(bands: &[BandPlan], budget: u64) -> io::Result<usize> {
        let mut total = 0_u64;
        let mut count = 0;
        for band in bands {
            total = total
                .checked_add(Self::bytes(std::slice::from_ref(band))?)
                .ok_or_else(|| io::Error::other("bulk wave size overflow"))?;
            if total > budget {
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

    pub(super) fn bytes(bands: &[BandPlan]) -> io::Result<u64> {
        bands.iter().try_fold(0_u64, |sum, band| {
            sum.checked_add(band.memory().map_err(io::Error::other)?.peak_bytes() as u64)
                .and_then(|n| {
                    n.checked_add((size_of::<Job<'_>>() + size_of::<BandResult>()) as u64)
                })
                .ok_or_else(|| io::Error::other("bulk band size overflow"))
        })
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

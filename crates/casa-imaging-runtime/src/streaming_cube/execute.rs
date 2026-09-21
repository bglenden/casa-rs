// SPDX-License-Identifier: LGPL-3.0-or-later

//! One admitted wave of exclusive bands at an immutable model epoch. The normal
//! fold drains the returned wave before admitting another, bounding completion
//! bytes without an unbounded reorder map or a second worker pool.

use super::input::{NativeSource, NativeStore, NativeStoreReader, StoreIo, StorePlan};
use crate::bounded_stream::{
    BlockIdentity, BoundedExecution, BoundedKernelPlan, BoundedStreamError,
    BoundedStreamMeasurements, KernelPartition, PartitionedKernel, WorkIdentity,
    execute_bounded_resident,
};
use casa_imaging_reconstruction::{
    ModelGeneration, PolarizationOperator, SpectralOperatorPrimitives,
    runtime_adapter::{BandPlan, NativeBlock, NativeLayout, PreparedFft},
};
use std::{io, mem::size_of, sync::Mutex, time::Instant};

pub(super) struct BandInput {
    pub(super) plan: BandPlan,
    pub(super) prior: Option<SpectralOperatorPrimitives>,
    pub(super) fft: Option<PreparedFft>,
}

enum BandJob {
    Pending(BandInput),
    Completed(
        SpectralOperatorPrimitives,
        PreparedFft,
        (usize, usize),
        BandProfile,
    ),
}

// Diagnostic only: aggregate at block boundaries and print after the worker join.
#[derive(Default, Debug)]
struct BandProfile {
    channel: usize,
    thread: Option<std::thread::ThreadId>,
    start_nanos: u128,
    end_nanos: u128,
    cpu_nanos: Option<u64>,
    prepare_nanos: u128,
    lock_wait_nanos: u128,
    read_nanos: u128,
    consume_nanos: u128,
    complete_nanos: u128,
    blocks: u64,
}

fn thread_cpu_nanos() -> Option<u64> {
    let mut value = libc::timespec {
        tv_sec: 0,
        tv_nsec: 0,
    };
    // SAFETY: value is writable for the duration of this synchronous call.
    (unsafe { libc::clock_gettime(libc::CLOCK_THREAD_CPUTIME_ID, &mut value) } == 0)
        .then(|| value.tv_sec as u64 * 1_000_000_000 + value.tv_nsec as u64)
}

pub(super) struct WavePlan {
    kernel: BoundedKernelPlan,
    has_source: bool,
    cache_slots: usize,
    job_bytes: Vec<u64>,
    pub(super) peak_bytes: u64,
}

fn overflow() -> io::Error {
    io::Error::other("cube wave byte count overflow")
}

impl WavePlan {
    /// Select without loading image data: fresh-normal work plus a complete
    /// prior window bounds the smaller residual-only workspace. Actual jobs
    /// are checked again by `new` before any grids or workers are allocated.
    #[allow(clippy::too_many_arguments)]
    pub(super) fn refresh_prefix(
        store: StorePlan,
        bands: &[BandPlan],
        workers: usize,
        source_slots: usize,
        shared_bytes: u64,
        budget: u64,
        prior_window_bytes: u64,
    ) -> io::Result<usize> {
        let full: Vec<_> = bands.iter().map(BandPlan::full_refresh).collect();
        let mut low = 0;
        let mut high = full.len();
        while low < high {
            let count = low + (high - low).div_ceil(2);
            let projected = Self::project(
                store,
                full[..count].iter().map(|b| (b, None)),
                workers,
                source_slots,
                shared_bytes,
            )?;
            let peak = prior_window_bytes
                .checked_mul(count as u64)
                .and_then(|n| n.checked_add(projected.peak_bytes))
                .ok_or_else(overflow)?;
            if peak <= budget {
                low = count;
            } else {
                high = count - 1;
            }
        }
        if low == 0 {
            return Err(io::Error::other("cube budget cannot hold one refresh band"));
        }
        Ok(low)
    }
    /// `shared_bytes` includes live compiled inputs/model/storage/sink owners
    /// outside this wave. All jobs can retain grids or completed images at once,
    /// so charge the sum of their individual peaks, never just worker_count.
    pub(super) fn new(
        store: &NativeStore,
        jobs: &[BandInput],
        workers: usize,
        source_slots: usize,
        shared_bytes: u64,
        budget: u64,
    ) -> io::Result<Self> {
        let plan = Self::project(
            store.plan,
            jobs.iter().map(|job| (&job.plan, job.prior.as_ref())),
            workers,
            source_slots,
            shared_bytes,
        )?;
        if plan.peak_bytes > budget {
            return Err(io::Error::other("cube band wave exceeds its memory budget"));
        }
        Ok(plan)
    }

    /// Largest initial-band prefix fitting the live memory allowance. Only
    /// descriptors exist during selection; no grids, FFTs or images allocate.
    /// The caller charges all retained plan metadata in `shared_bytes`, drains
    /// each wave into bounded normal storage, then selects the next prefix.
    pub(super) fn initial_prefix(
        store: StorePlan,
        bands: &[BandPlan],
        workers: usize,
        source_slots: usize,
        shared_bytes: u64,
        budget: u64,
    ) -> io::Result<usize> {
        let mut low = 0;
        let mut high = bands.len();
        while low < high {
            let count = low + (high - low).div_ceil(2);
            let plan = Self::project(
                store,
                bands[..count].iter().map(|band| (band, None)),
                workers,
                source_slots,
                shared_bytes,
            )?;
            if plan.peak_bytes <= budget {
                low = count;
            } else {
                high = count - 1;
            }
        }
        if low == 0 {
            return Err(io::Error::other("cube budget cannot hold one initial band"));
        }
        Ok(low)
    }

    pub(super) fn project<'a>(
        store: StorePlan,
        jobs: impl ExactSizeIterator<Item = (&'a BandPlan, Option<&'a SpectralOperatorPrimitives>)>,
        workers: usize,
        source_slots: usize,
        shared_bytes: u64,
    ) -> io::Result<Self> {
        let count = jobs.len();
        if count == 0 || workers == 0 || !(1..=2).contains(&source_slots) {
            return Err(io::Error::other("invalid cube band wave"));
        }
        let workers = workers.min(count);
        let mut has_source = false;
        let mut native_tiles = 0;
        let mut job_bytes = Vec::with_capacity(count);
        let mut dynamic = 0_u64;
        let mut previous_end = None;
        for (band, prior) in jobs {
            if previous_end.is_some_and(|end| end != band.core().start) {
                return Err(io::Error::other("nonadjacent cube band wave"));
            }
            previous_end = Some(band.core().end);
            let native = band.native_range();
            has_source |= !native.is_empty();
            let input = if native.is_empty() {
                0
            } else {
                native_tiles = native_tiles.max(
                    (native.end - 1) / store.tile_channels - native.start / store.tile_channels + 1,
                );
                NativeSource::memory(store, native)?.1
            };
            let bytes = u64::try_from(band.memory(prior).map_err(io::Error::other)?.peak_bytes())
                .map_err(|_| overflow())?
                .checked_add(input)
                .ok_or_else(overflow)?;
            dynamic = dynamic.checked_add(bytes).ok_or_else(overflow)?;
            job_bytes.push(bytes);
        }
        // Collection storage and by-value conversion can overlap at the handoff.
        let headers = count
            .checked_mul(
                size_of::<BandInput>()
                    + size_of::<Mutex<Option<BandJob>>>()
                    + size_of::<(SpectralOperatorPrimitives, PreparedFft)>()
                    + 2 * size_of::<u64>(),
            )
            .and_then(|bytes| {
                bytes.checked_add(
                    size_of::<Self>() + size_of::<BandKernel<'_>>() + 3 * size_of::<Vec<()>>(),
                )
            })
            .ok_or_else(overflow)? as u64;
        let kernel = BoundedKernelPlan::new::<usize, ()>(workers, count, dynamic)
            .map_err(|error| io::Error::other(format!("invalid cube kernel plan: {error:?}")))?;
        let cache_slots = if has_source {
            store.reader_cache_slots(workers, native_tiles)?
        } else {
            0
        };
        let source = if has_source {
            store.reader_capacity_bytes(cache_slots)?
        } else {
            0
        };
        let peak_bytes = shared_bytes
            .checked_add(headers)
            .and_then(|bytes| bytes.checked_add(source))
            .and_then(|bytes| bytes.checked_add(kernel.capacity_bytes()))
            .ok_or_else(overflow)?;
        Ok(Self {
            kernel,
            has_source,
            cache_slots,
            job_bytes,
            peak_bytes,
        })
    }
}

pub(super) struct WaveResult {
    pub(super) bands: Vec<(SpectralOperatorPrimitives, PreparedFft)>,
    pub(super) source: StoreIo,
}

/// Execute only the jobs used to derive admission; no work escapes the source
/// join or the immutable model borrow. The caller drains outputs before returning
/// to its model-mutating controller or admitting another wave.
#[allow(clippy::too_many_arguments)]
pub(super) fn execute(
    store: &mut NativeStore,
    jobs: Vec<BandInput>,
    generation: &ModelGeneration,
    layout: &NativeLayout,
    output_hz: &[f64],
    polarization: &PolarizationOperator,
    workers: usize,
    source_slots: usize,
    shared_bytes: u64,
    budget: u64,
    pass: u32,
    measurements: &mut Option<BoundedStreamMeasurements>,
) -> io::Result<WaveResult> {
    let plan = WavePlan::new(store, &jobs, workers, source_slots, shared_bytes, budget)?;
    let kernel = BandKernel {
        jobs: jobs
            .into_iter()
            .map(|job| Mutex::new(Some(BandJob::Pending(job))))
            .collect(),
        generation,
        layout,
        output_hz,
        polarization,
        store_plan: store.plan,
        reader: Mutex::new(if plan.has_source {
            Some(store.reader(plan.cache_slots)?)
        } else {
            None
        }),
        job_bytes: plan.job_bytes,
        profile: std::env::var_os("CASA_RS_PROFILE_CUBE").is_some(),
        started: Instant::now(),
    };
    let result = execute_bounded_resident(plan.kernel, pass, &(), kernel);
    match result {
        Ok(outcome) => {
            *measurements = Some(outcome.measurements);
            Ok(outcome.kernel_completion)
        }
        Err(failure) => {
            *measurements = Some(*failure.measurements);
            Err(match *failure.cause {
                BoundedStreamError::Kernel(error) => error,
                error => io::Error::other(format!("bounded cube wave failed: {error:?}")),
            })
        }
    }
}

struct BandKernel<'a> {
    jobs: Vec<Mutex<Option<BandJob>>>,
    generation: &'a ModelGeneration,
    layout: &'a NativeLayout,
    output_hz: &'a [f64],
    polarization: &'a PolarizationOperator,
    store_plan: StorePlan,
    // Serialize just frame reads/decode/cache release. Gridding and FFTs stay
    // worker-local; Linux cache verification cannot race another reader.
    reader: Mutex<Option<NativeStoreReader<'a>>>,
    job_bytes: Vec<u64>,
    profile: bool,
    started: Instant,
}

impl PartitionedKernel<()> for BandKernel<'_> {
    type Partition = usize;
    type Partial = ();
    type Completion = WaveResult;
    type Error = io::Error;

    fn partition_count(&self, _: BlockIdentity, _: &()) -> io::Result<usize> {
        Ok(self.jobs.len())
    }
    fn partition(
        &self,
        _: BlockIdentity,
        _: &(),
        ordinal: usize,
    ) -> io::Result<KernelPartition<usize>> {
        Ok(KernelPartition::exclusive(
            ordinal as u64,
            ordinal as u64,
            ordinal,
        ))
    }
    fn execution_dynamic_capacity_bytes(&self, &ordinal: &usize) -> u64 {
        self.job_bytes[ordinal]
    }
    fn execute(&self, _: WorkIdentity, _: &(), &ordinal: &usize) -> io::Result<()> {
        let mut slot = self.jobs[ordinal]
            .lock()
            .map_err(|_| io::Error::other("cube band owner poisoned"))?;
        let Some(BandJob::Pending(input)) = slot.take() else {
            return Err(io::Error::other("cube band lifecycle mismatch"));
        };
        let window = input.plan.native_range();
        let mut profile = BandProfile::default();
        let cpu_started = if self.profile {
            profile.channel = input.plan.core().start;
            profile.thread = Some(std::thread::current().id());
            profile.start_nanos = self.started.elapsed().as_nanos();
            thread_cpu_nanos()
        } else {
            None
        };
        let prepare_started = self.profile.then(Instant::now);
        let mut job = input
            .plan
            .prepare(self.generation, input.prior, input.fft)
            .map_err(io::Error::other)?;
        if let Some(started) = prepare_started {
            profile.prepare_nanos = started.elapsed().as_nanos();
        }
        if !window.is_empty() {
            let mut block = NativeBlock::new(
                self.store_plan.block_rows,
                window.len(),
                self.store_plan.correlations,
            )?;
            for ordinal in 0..self.store_plan.blocks() {
                let wait_started = self.profile.then(Instant::now);
                let mut reader = self
                    .reader
                    .lock()
                    .map_err(|_| io::Error::other("cube source owner poisoned"))?;
                if let Some(started) = wait_started {
                    profile.lock_wait_nanos += started.elapsed().as_nanos();
                }
                let read_started = self.profile.then(Instant::now);
                reader
                    .as_mut()
                    .ok_or_else(|| io::Error::other("cube source owner missing"))?
                    .read_block(ordinal, window.clone(), &mut block)?;
                drop(reader);
                if let Some(started) = read_started {
                    profile.read_nanos += started.elapsed().as_nanos();
                }
                let consume_started = self.profile.then(Instant::now);
                job.consume(
                    &block,
                    self.layout,
                    window.clone(),
                    self.output_hz,
                    self.polarization,
                )
                .map_err(io::Error::other)?;
                if let Some(started) = consume_started {
                    profile.consume_nanos += started.elapsed().as_nanos();
                    profile.blocks += 1;
                }
            }
        }
        let counts = job.model_plane_counts();
        let complete_started = self.profile.then(Instant::now);
        let (normal, fft) = job.complete(self.generation).map_err(io::Error::other)?;
        if let Some(started) = complete_started {
            profile.complete_nanos = started.elapsed().as_nanos();
            profile.end_nanos = self.started.elapsed().as_nanos();
            profile.cpu_nanos = cpu_started.zip(thread_cpu_nanos()).map(|(a, b)| b - a);
        }
        *slot = Some(BandJob::Completed(normal, fft, counts, profile));
        Ok(())
    }
    fn commit(
        &mut self,
        _: WorkIdentity,
        _: &(),
        _: (),
        _: BoundedExecution<'_>,
    ) -> io::Result<()> {
        Ok(())
    }
    fn complete(mut self, _: BoundedExecution<'_>) -> io::Result<Self::Completion> {
        let (model_planes, forward_ffts) =
            self.jobs
                .iter_mut()
                .try_fold((0, 0), |(planes, ffts), slot| {
                    let Some(BandJob::Completed(_, _, (job_planes, job_ffts), profile)) = slot
                        .get_mut()
                        .map_err(|_| io::Error::other("cube band owner poisoned"))?
                    else {
                        return Err(io::Error::other("cube band input incomplete"));
                    };
                    if self.profile {
                        eprintln!("cube_profile_band {profile:?}");
                    }
                    Ok((planes + *job_planes, ffts + *job_ffts))
                })?;
        eprintln!(
            "streaming_cube_model_work model_planes={model_planes} forward_ffts={forward_ffts}"
        );
        let bands = self
            .jobs
            .into_iter()
            .map(|slot| match slot.into_inner() {
                Ok(Some(BandJob::Completed(normal, fft, _, _))) => Ok((normal, fft)),
                _ => Err(io::Error::other("cube band completion missing")),
            })
            .collect::<io::Result<_>>()?;
        let source = self
            .reader
            .into_inner()
            .map_err(|_| io::Error::other("cube source owner poisoned"))?
            .map(NativeStoreReader::complete)
            .unwrap_or_default();
        Ok(WaveResult { bands, source })
    }
}

#[cfg(test)]
mod tests;

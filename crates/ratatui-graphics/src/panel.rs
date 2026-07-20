// SPDX-License-Identifier: LGPL-3.0-or-later
//! Shared scheduling and protocol preparation for panel rendering.

use std::{
    collections::VecDeque,
    fmt::{Debug, Display},
    panic::{self, AssertUnwindSafe},
    sync::{
        Arc, Condvar, Mutex,
        atomic::{AtomicU64, AtomicUsize, Ordering},
        mpsc::{self, Receiver, TryRecvError},
    },
    thread::{self, JoinHandle},
};

use image::{DynamicImage, RgbaImage};
use ratatui::layout::Rect;
use ratatui_image::{
    Resize, errors::Errors as PanelProtocolError, picker::Picker,
    protocol::Protocol as PanelProtocol,
};
use thiserror::Error;

/// Protocol-specific input for one scheduled panel render.
#[derive(Debug)]
pub struct PanelProtocolRequest<T> {
    /// The target panel cell area.
    pub area: Rect,
    /// Maximum raster width available for this render in pixels.
    pub max_pixel_width: u32,
    /// Maximum raster height available for this render in pixels.
    pub max_pixel_height: u32,
    /// Whether to build a `ratatui-image` protocol in addition to the bitmap.
    pub build_protocol: bool,
    /// Caller-defined render input.
    pub input: T,
}

/// A unit of work submitted to a [`PanelScheduler`].
#[derive(Debug)]
pub struct PanelJob<T> {
    /// Monotonic request id assigned by the scheduler.
    pub request_id: u64,
    /// Caller-managed generation used for stale-result discard.
    pub generation: u64,
    /// Stable caller key for correlating completions.
    pub key: u64,
    /// Caller-defined work input.
    pub input: T,
}

/// A completed unit of scheduled panel work.
#[derive(Debug)]
pub struct PanelCompletion<O> {
    /// The request id that produced this result.
    pub request_id: u64,
    /// Caller-managed generation.
    pub generation: u64,
    /// Stable caller key.
    pub key: u64,
    /// Worker output payload.
    pub output: O,
}

/// A prepared `ratatui-image` protocol and its source bitmap.
pub struct PreparedPanelProtocol {
    /// The panel cell area used to build the protocol.
    pub area: Rect,
    /// Actual raster width of the rendered image.
    pub image_width: u32,
    /// Actual raster height of the rendered image.
    pub image_height: u32,
    /// The encoded protocol, when requested.
    pub protocol: Option<PanelProtocol>,
    /// The rendered bitmap that produced `protocol`.
    pub rendered_image: RgbaImage,
}

/// A one-shot panel protocol prepared from an owned bitmap.
pub struct PreparedPanelProtocolOnly {
    /// The panel cell area used to build the protocol.
    pub area: Rect,
    /// Actual raster width of the rendered image.
    pub image_width: u32,
    /// Actual raster height of the rendered image.
    pub image_height: u32,
    /// The encoded `ratatui-image` protocol.
    pub protocol: PanelProtocol,
}

/// Queueing policy for a panel scheduler.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PanelSchedulePolicy {
    /// Run one worker and replace queued work with the newest request.
    LatestWins,
    /// Run a fixed worker set against a bounded first-in, first-out queue.
    Ordered {
        /// Number of worker threads.
        worker_count: usize,
        /// Maximum number of queued, not-yet-started jobs.
        queue_capacity: usize,
    },
}

impl PanelSchedulePolicy {
    fn worker_count(self) -> usize {
        match self {
            Self::LatestWins => 1,
            Self::Ordered { worker_count, .. } => worker_count.max(1),
        }
    }

    fn queue_capacity(self) -> usize {
        match self {
            Self::LatestWins => 1,
            Self::Ordered { queue_capacity, .. } => queue_capacity.max(1),
        }
    }
}

/// Errors returned while creating panel scheduler workers.
#[derive(Debug, Error)]
pub enum PanelInitError {
    /// A background worker thread could not be started.
    #[error("failed to start panel scheduler worker thread")]
    Spawn(#[source] std::io::Error),
}

/// Errors returned when submitting scheduled panel work.
#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub enum PanelSubmitError {
    /// Request ids are exhausted.
    #[error("panel scheduler request id space is exhausted")]
    RequestIdExhausted,
    /// Every worker has stopped.
    #[error("panel scheduler worker is no longer running")]
    WorkerStopped,
    /// Internal queue state was poisoned by a panic.
    #[error("panel scheduler worker state is poisoned")]
    StatePoisoned,
    /// The bounded ordered queue is full.
    #[error("panel scheduler queue is full")]
    QueueFull,
}

/// Errors returned while executing or polling scheduled panel work.
#[derive(Debug, Error)]
pub enum PanelSchedulerError<E>
where
    E: Display + Debug + Send + Sync + 'static,
{
    /// The caller's worker callback failed.
    #[error("panel scheduler request {request_id} failed in the worker callback: {source}")]
    Worker {
        /// The request that failed.
        request_id: u64,
        /// The request generation.
        generation: u64,
        /// Stable caller key.
        key: u64,
        /// The underlying callback error.
        source: E,
    },
    /// The worker callback panicked.
    #[error("panel scheduler request {request_id} panicked in the worker thread")]
    WorkerPanic {
        /// The request that panicked.
        request_id: u64,
        /// The request generation.
        generation: u64,
        /// Stable caller key.
        key: u64,
    },
    /// Every worker stopped before a latest-wins request completed.
    #[error("panel scheduler workers stopped before request {request_id} completed")]
    Disconnected {
        /// The request that was still pending.
        request_id: u64,
    },
}

impl<E> PanelSchedulerError<E>
where
    E: Display + Debug + Send + Sync + 'static,
{
    /// Return the request id associated with this error.
    pub fn request_id(&self) -> u64 {
        match self {
            Self::Worker { request_id, .. }
            | Self::WorkerPanic { request_id, .. }
            | Self::Disconnected { request_id } => *request_id,
        }
    }

    /// Return the generation associated with this error, when known.
    pub fn generation(&self) -> Option<u64> {
        match self {
            Self::Worker { generation, .. } | Self::WorkerPanic { generation, .. } => {
                Some(*generation)
            }
            Self::Disconnected { .. } => None,
        }
    }
}

/// Result of draining completed ordered work from a [`PanelScheduler`].
#[derive(Debug)]
pub struct PanelDrain<O, E>
where
    E: Display + Debug + Send + Sync + 'static,
{
    /// Ready results that matched the requested generation.
    pub ready: Vec<PanelCompletion<O>>,
    /// Completed or replaced results discarded as stale.
    pub stale_count: u64,
    /// Current-generation worker failures.
    pub errors: Vec<PanelSchedulerError<E>>,
}

impl<O, E> Default for PanelDrain<O, E>
where
    E: Display + Debug + Send + Sync + 'static,
{
    fn default() -> Self {
        Self {
            ready: Vec::new(),
            stale_count: 0,
            errors: Vec::new(),
        }
    }
}

struct SchedulerState<T> {
    queued_jobs: VecDeque<PanelJob<T>>,
    shutdown: bool,
}

/// Shared worker engine for responsive latest-wins and bounded ordered panel work.
pub struct PanelScheduler<T, O, E>
where
    T: Send + 'static,
    O: Send + 'static,
    E: Display + Debug + Send + Sync + 'static,
{
    policy: PanelSchedulePolicy,
    queue: Arc<(Mutex<SchedulerState<T>>, Condvar)>,
    completions: Receiver<Result<PanelCompletion<O>, PanelSchedulerError<E>>>,
    live_workers: Arc<AtomicUsize>,
    active_jobs: Arc<AtomicUsize>,
    worker_handles: Vec<JoinHandle<()>>,
    next_request_id: u64,
    latest_requested_id: Option<u64>,
    pending_latest: bool,
    latest_completion: Option<PanelCompletion<O>>,
    stale_result_count: u64,
    replaced_job_count: Arc<AtomicU64>,
}

impl<T, O, E> PanelScheduler<T, O, E>
where
    T: Send + 'static,
    O: Send + 'static,
    E: Display + Debug + Send + Sync + 'static,
{
    /// Create a scheduler with the requested queueing policy.
    pub fn new<F>(policy: PanelSchedulePolicy, worker: F) -> Result<Self, PanelInitError>
    where
        F: Fn(&PanelJob<T>) -> Result<O, E> + Send + Sync + 'static,
    {
        let queue = Arc::new((
            Mutex::new(SchedulerState {
                queued_jobs: VecDeque::new(),
                shutdown: false,
            }),
            Condvar::new(),
        ));
        let (tx, rx) = mpsc::channel();
        let worker_count = policy.worker_count();
        let live_workers = Arc::new(AtomicUsize::new(worker_count));
        let active_jobs = Arc::new(AtomicUsize::new(0));
        let replaced_job_count = Arc::new(AtomicU64::new(0));
        let worker = Arc::new(worker);
        let mut worker_handles: Vec<JoinHandle<()>> = Vec::with_capacity(worker_count);

        for worker_index in 0..worker_count {
            let worker_queue = Arc::clone(&queue);
            let worker_live = Arc::clone(&live_workers);
            let worker_active = Arc::clone(&active_jobs);
            let worker_callback = Arc::clone(&worker);
            let worker_tx = tx.clone();
            let handle_result = thread::Builder::new()
                .name(format!("ghostty-panel-scheduler-{worker_index}"))
                .spawn(move || {
                    loop {
                        let job = {
                            let (lock, cvar) = &*worker_queue;
                            let mut state = match lock.lock() {
                                Ok(state) => state,
                                Err(_) => break,
                            };
                            while state.queued_jobs.is_empty() && !state.shutdown {
                                state = match cvar.wait(state) {
                                    Ok(state) => state,
                                    Err(_) => {
                                        worker_live.fetch_sub(1, Ordering::SeqCst);
                                        return;
                                    }
                                };
                            }
                            if state.shutdown {
                                break;
                            }
                            state.queued_jobs.pop_front()
                        };
                        let Some(job) = job else {
                            continue;
                        };
                        let request_id = job.request_id;
                        let generation = job.generation;
                        let key = job.key;
                        worker_active.fetch_add(1, Ordering::SeqCst);
                        let result =
                            panic::catch_unwind(AssertUnwindSafe(|| worker_callback(&job)))
                                .map_err(|_| PanelSchedulerError::WorkerPanic {
                                    request_id,
                                    generation,
                                    key,
                                })
                                .and_then(|result| {
                                    result
                                        .map(|output| PanelCompletion {
                                            request_id,
                                            generation,
                                            key,
                                            output,
                                        })
                                        .map_err(|source| PanelSchedulerError::Worker {
                                            request_id,
                                            generation,
                                            key,
                                            source,
                                        })
                                });
                        worker_active.fetch_sub(1, Ordering::SeqCst);
                        if worker_tx.send(result).is_err() {
                            break;
                        }
                    }
                    worker_live.fetch_sub(1, Ordering::SeqCst);
                });
            let handle = match handle_result {
                Ok(handle) => handle,
                Err(error) => {
                    let (lock, cvar) = &*queue;
                    if let Ok(mut state) = lock.lock() {
                        state.shutdown = true;
                        cvar.notify_all();
                    }
                    for handle in worker_handles.drain(..) {
                        let _ = handle.join();
                    }
                    return Err(PanelInitError::Spawn(error));
                }
            };
            worker_handles.push(handle);
        }

        Ok(Self {
            policy,
            queue,
            completions: rx,
            live_workers,
            active_jobs,
            worker_handles,
            next_request_id: 0,
            latest_requested_id: None,
            pending_latest: false,
            latest_completion: None,
            stale_result_count: 0,
            replaced_job_count,
        })
    }

    /// Submit one unit of work without blocking the caller.
    pub fn submit(&mut self, generation: u64, key: u64, input: T) -> Result<u64, PanelSubmitError> {
        if self.live_workers.load(Ordering::SeqCst) == 0 {
            return Err(PanelSubmitError::WorkerStopped);
        }
        let request_id = self
            .next_request_id
            .checked_add(1)
            .ok_or(PanelSubmitError::RequestIdExhausted)?;
        let (lock, cvar) = &*self.queue;
        let mut state = lock.lock().map_err(|_| PanelSubmitError::StatePoisoned)?;
        if state.shutdown || self.live_workers.load(Ordering::SeqCst) == 0 {
            return Err(PanelSubmitError::WorkerStopped);
        }
        match self.policy {
            PanelSchedulePolicy::LatestWins => {
                let replaced = state.queued_jobs.len() as u64;
                state.queued_jobs.clear();
                self.replaced_job_count
                    .fetch_add(replaced, Ordering::SeqCst);
                self.latest_requested_id = Some(request_id);
                self.pending_latest = true;
            }
            PanelSchedulePolicy::Ordered { .. }
                if state.queued_jobs.len() >= self.policy.queue_capacity() =>
            {
                return Err(PanelSubmitError::QueueFull);
            }
            PanelSchedulePolicy::Ordered { .. } => {}
        }
        self.next_request_id = request_id;
        state.queued_jobs.push_back(PanelJob {
            request_id,
            generation,
            key,
            input,
        });
        cvar.notify_one();
        Ok(request_id)
    }

    /// Poll a latest-wins scheduler and retain its newest successful completion.
    pub fn pump_latest(&mut self, current_generation: u64) -> Result<bool, PanelSchedulerError<E>> {
        debug_assert_eq!(self.policy, PanelSchedulePolicy::LatestWins);
        let mut changed = false;
        loop {
            match self.completions.try_recv() {
                Ok(Ok(completion))
                    if completion.generation == current_generation
                        && Some(completion.request_id) == self.latest_requested_id =>
                {
                    self.pending_latest = false;
                    self.latest_completion = Some(completion);
                    changed = true;
                }
                Ok(Err(error))
                    if error.generation() == Some(current_generation)
                        && Some(error.request_id()) == self.latest_requested_id =>
                {
                    self.pending_latest = false;
                    return Err(error);
                }
                Ok(_) => {
                    self.stale_result_count = self.stale_result_count.saturating_add(1);
                }
                Err(TryRecvError::Empty) => break,
                Err(TryRecvError::Disconnected) => {
                    if let Some(request_id) = self.latest_requested_id.filter(|_| {
                        self.pending_latest && self.live_workers.load(Ordering::SeqCst) == 0
                    }) {
                        return Err(PanelSchedulerError::Disconnected { request_id });
                    }
                    break;
                }
            }
        }
        Ok(changed)
    }

    /// Return the retained completion for a latest-wins scheduler.
    pub fn latest(&self) -> Option<&PanelCompletion<O>> {
        self.latest_completion.as_ref()
    }

    /// Return whether the newest latest-wins request is still pending.
    pub fn is_pending(&self) -> bool {
        self.pending_latest
    }

    /// Drain completed ordered work, discarding other generations.
    pub fn drain(&mut self, current_generation: u64) -> PanelDrain<O, E> {
        let replaced = self.replaced_job_count.swap(0, Ordering::SeqCst);
        let mut drain = PanelDrain {
            stale_count: replaced,
            ..PanelDrain::default()
        };
        loop {
            match self.completions.try_recv() {
                Ok(Ok(completion)) if completion.generation == current_generation => {
                    drain.ready.push(completion);
                }
                Ok(Err(error)) if error.generation() == Some(current_generation) => {
                    drain.errors.push(error);
                }
                Ok(_) => {
                    self.stale_result_count = self.stale_result_count.saturating_add(1);
                    drain.stale_count = drain.stale_count.saturating_add(1);
                }
                Err(TryRecvError::Empty) | Err(TryRecvError::Disconnected) => break,
            }
        }
        drain
    }

    /// Number of queued-but-not-yet-started jobs.
    pub fn queue_depth(&self) -> usize {
        let (lock, _) = &*self.queue;
        lock.lock()
            .map(|state| state.queued_jobs.len())
            .unwrap_or(0)
    }

    /// Number of configured workers.
    pub fn worker_count(&self) -> usize {
        self.worker_handles.len()
    }

    /// Number of jobs currently executing in worker threads.
    pub fn active_job_count(&self) -> usize {
        self.active_jobs.load(Ordering::SeqCst)
    }

    /// Return the number of stale completions or replaced jobs since the last call.
    pub fn take_stale_result_count(&mut self) -> u64 {
        let count = self
            .stale_result_count
            .saturating_add(self.replaced_job_count.swap(0, Ordering::SeqCst));
        self.stale_result_count = 0;
        count
    }
}

impl<T, O, E> Drop for PanelScheduler<T, O, E>
where
    T: Send + 'static,
    O: Send + 'static,
    E: Display + Debug + Send + Sync + 'static,
{
    fn drop(&mut self) {
        let (lock, cvar) = &*self.queue;
        if let Ok(mut state) = lock.lock() {
            state.shutdown = true;
            state.queued_jobs.clear();
            cvar.notify_all();
        }
        for handle in self.worker_handles.drain(..) {
            let _ = handle.join();
        }
    }
}

/// Errors returned while rendering a protocol-specific panel request.
#[derive(Debug, Error)]
pub enum PanelProtocolRenderError<E>
where
    E: Display + Debug + Send + Sync + 'static,
{
    /// The caller's raster renderer failed.
    #[error("panel raster renderer failed: {0}")]
    Render(E),
    /// `ratatui-image` could not adapt the bitmap to the chosen protocol.
    #[error("failed to create panel protocol")]
    Protocol(#[source] PanelProtocolError),
}

/// Render an image and optionally convert it into a protocol for the target area.
pub fn render_panel_protocol<T, E, F>(
    picker: &Picker,
    resize: Resize,
    job: &PanelJob<PanelProtocolRequest<T>>,
    renderer: F,
) -> Result<PreparedPanelProtocol, PanelProtocolRenderError<E>>
where
    E: Display + Debug + Send + Sync + 'static,
    F: FnOnce(&PanelProtocolRequest<T>) -> Result<DynamicImage, E>,
{
    let request = &job.input;
    let image = renderer(request).map_err(PanelProtocolRenderError::Render)?;
    let rendered_image = image.to_rgba8();
    let image_width = rendered_image.width();
    let image_height = rendered_image.height();
    let protocol = if request.build_protocol {
        Some(
            picker
                .new_protocol(
                    DynamicImage::ImageRgba8(rendered_image.clone()),
                    request.area,
                    resize,
                )
                .map_err(PanelProtocolRenderError::Protocol)?,
        )
    } else {
        None
    };
    Ok(PreparedPanelProtocol {
        area: request.area,
        image_width,
        image_height,
        protocol,
        rendered_image,
    })
}

/// Build a one-shot panel protocol from an owned bitmap without retaining a second copy.
pub fn build_panel_protocol_from_rgba_owned(
    picker: &Picker,
    resize: Resize,
    area: Rect,
    image: RgbaImage,
) -> Result<PreparedPanelProtocolOnly, PanelProtocolError> {
    let image_width = image.width();
    let image_height = image.height();
    let protocol = picker.new_protocol(DynamicImage::ImageRgba8(image), area, resize)?;
    Ok(PreparedPanelProtocolOnly {
        area,
        image_width,
        image_height,
        protocol,
    })
}

#[cfg(test)]
mod tests {
    use std::{
        fmt,
        sync::{Arc, Mutex},
        thread,
        time::{Duration, Instant},
    };

    use image::{DynamicImage, Rgba, RgbaImage};
    use ratatui::layout::Rect;
    use ratatui_image::{Resize, picker::Picker};

    use super::{
        PanelProtocolRequest, PanelSchedulePolicy, PanelScheduler, PanelSubmitError,
        build_panel_protocol_from_rgba_owned, render_panel_protocol,
    };

    #[derive(Debug)]
    struct TestError;

    impl fmt::Display for TestError {
        fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
            formatter.write_str("test render failed")
        }
    }

    fn wait_for_latest<T, O>(scheduler: &mut PanelScheduler<T, O, TestError>)
    where
        T: Send + 'static,
        O: Send + 'static,
    {
        let deadline = Instant::now() + Duration::from_secs(2);
        while scheduler.is_pending() && Instant::now() < deadline {
            scheduler.pump_latest(1).expect("pump latest");
            thread::sleep(Duration::from_millis(2));
        }
        assert!(!scheduler.is_pending());
    }

    #[test]
    fn latest_wins_replaces_queued_work_and_retains_latest_completion() {
        let seen = Arc::new(Mutex::new(Vec::new()));
        let worker_seen = Arc::clone(&seen);
        let mut scheduler = PanelScheduler::new(PanelSchedulePolicy::LatestWins, move |job| {
            worker_seen.lock().unwrap().push(job.input);
            if job.input == 1 {
                thread::sleep(Duration::from_millis(30));
            }
            Ok::<_, TestError>(job.input * 10)
        })
        .unwrap();

        scheduler.submit(1, 1, 1).unwrap();
        thread::sleep(Duration::from_millis(5));
        scheduler.submit(1, 2, 2).unwrap();
        scheduler.submit(1, 3, 3).unwrap();
        wait_for_latest(&mut scheduler);

        assert_eq!(scheduler.latest().map(|ready| ready.output), Some(30));
        assert!(scheduler.take_stale_result_count() >= 1);
        assert_eq!(*seen.lock().unwrap(), vec![1, 3]);
    }

    #[test]
    fn ordered_policy_preserves_all_work_and_filters_stale_generations() {
        let mut scheduler = PanelScheduler::new(
            PanelSchedulePolicy::Ordered {
                worker_count: 2,
                queue_capacity: 4,
            },
            |job| Ok::<_, TestError>(job.input * 2),
        )
        .unwrap();
        scheduler.submit(1, 1, 2).unwrap();
        scheduler.submit(2, 2, 3).unwrap();

        let deadline = Instant::now() + Duration::from_secs(2);
        let mut ready = Vec::new();
        let mut stale = 0;
        while (ready.is_empty() || stale == 0) && Instant::now() < deadline {
            let drain = scheduler.drain(2);
            ready.extend(drain.ready);
            stale += drain.stale_count;
            thread::sleep(Duration::from_millis(2));
        }
        assert_eq!(ready.len(), 1);
        assert_eq!(ready[0].key, 2);
        assert_eq!(ready[0].output, 6);
        assert_eq!(stale, 1);
    }

    #[test]
    fn bounded_ordered_policy_rejects_a_full_queue() {
        let gate = Arc::new(Mutex::new(()));
        let held = gate.lock().unwrap();
        let worker_gate = Arc::clone(&gate);
        let mut scheduler = PanelScheduler::new(
            PanelSchedulePolicy::Ordered {
                worker_count: 1,
                queue_capacity: 1,
            },
            move |job| {
                let _guard = worker_gate.lock().unwrap();
                Ok::<_, TestError>(job.input)
            },
        )
        .unwrap();
        scheduler.submit(1, 1, 1).unwrap();
        let deadline = Instant::now() + Duration::from_secs(1);
        while scheduler.active_job_count() == 0 && Instant::now() < deadline {
            thread::yield_now();
        }
        scheduler.submit(1, 2, 2).unwrap();
        assert_eq!(scheduler.submit(1, 3, 3), Err(PanelSubmitError::QueueFull));
        drop(held);
    }

    #[test]
    fn protocol_render_can_skip_encoding_without_losing_bitmap() {
        let picker = Picker::halfblocks();
        let mut scheduler = PanelScheduler::new(PanelSchedulePolicy::LatestWins, move |job| {
            render_panel_protocol(&picker, Resize::Fit(None), job, |request| {
                Ok::<_, TestError>(DynamicImage::ImageRgba8(RgbaImage::from_pixel(
                    request.max_pixel_width,
                    request.max_pixel_height,
                    Rgba([1, 2, 3, 255]),
                )))
            })
        })
        .unwrap();
        scheduler
            .submit(
                1,
                4,
                PanelProtocolRequest {
                    area: Rect::new(0, 0, 4, 3),
                    max_pixel_width: 8,
                    max_pixel_height: 6,
                    build_protocol: false,
                    input: (),
                },
            )
            .unwrap();
        let deadline = Instant::now() + Duration::from_secs(2);
        while scheduler.is_pending() && Instant::now() < deadline {
            scheduler.pump_latest(1).unwrap();
            thread::sleep(Duration::from_millis(2));
        }
        let prepared = &scheduler.latest().unwrap().output;
        assert!(prepared.protocol.is_none());
        assert_eq!((prepared.image_width, prepared.image_height), (8, 6));
        assert_eq!(prepared.rendered_image.dimensions(), (8, 6));
    }

    #[test]
    fn owned_rgba_builder_reports_dimensions() {
        let prepared = build_panel_protocol_from_rgba_owned(
            &Picker::halfblocks(),
            Resize::Fit(None),
            Rect::new(0, 0, 4, 3),
            RgbaImage::from_pixel(8, 6, Rgba([1, 2, 3, 255])),
        )
        .unwrap();
        assert_eq!((prepared.image_width, prepared.image_height), (8, 6));
    }
}

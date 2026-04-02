// SPDX-License-Identifier: LGPL-3.0-or-later
//! Stateful panel rendering support built on top of `ratatui-image`.

use std::{
    collections::VecDeque,
    fmt::{Debug, Display},
    panic::{self, AssertUnwindSafe},
    sync::{
        Arc, Condvar, Mutex,
        atomic::{AtomicBool, AtomicUsize, Ordering},
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

/// A single panel render request.
#[derive(Debug)]
pub struct PanelRenderJob<T> {
    /// Monotonic request id assigned by [`PanelRenderer`].
    pub request_id: u64,
    /// The target panel cell area.
    pub area: Rect,
    /// Maximum raster width available for this render in pixels.
    pub max_pixel_width: u32,
    /// Maximum raster height available for this render in pixels.
    pub max_pixel_height: u32,
    /// Whether the worker should also build a `ratatui-image` protocol.
    pub build_protocol: bool,
    /// Caller-defined render input.
    pub input: T,
}

/// A prepared `ratatui-image` protocol ready to render into a panel.
pub struct PreparedPanelProtocol {
    /// The request id that produced this protocol.
    pub request_id: u64,
    /// The panel cell area used to build the protocol.
    pub area: Rect,
    /// Actual raster width of the rendered image.
    pub image_width: u32,
    /// Actual raster height of the rendered image.
    pub image_height: u32,
    /// The encoded `ratatui-image` protocol ready for use with `Image`, when requested.
    pub protocol: Option<PanelProtocol>,
    /// The rendered bitmap that produced `protocol`.
    pub rendered_image: RgbaImage,
}

/// A one-shot panel protocol prepared from an owned bitmap that does not retain the source image.
pub struct PreparedPanelProtocolOnly {
    /// The panel cell area used to build the protocol.
    pub area: Rect,
    /// Actual raster width of the rendered image.
    pub image_width: u32,
    /// Actual raster height of the rendered image.
    pub image_height: u32,
    /// The encoded `ratatui-image` protocol ready for use with `Image`.
    pub protocol: PanelProtocol,
}

/// A queued background render job for [`PanelRenderPool`].
#[derive(Debug)]
pub struct PanelRenderPoolJob<T> {
    /// Monotonic request id assigned by [`PanelRenderPool`].
    pub request_id: u64,
    /// Caller-managed generation used for stale-result discard.
    pub generation: u64,
    /// Stable caller key for correlating completions.
    pub key: u64,
    /// Caller-defined render input.
    pub input: T,
}

/// A prepared background render result produced by [`PanelRenderPool`].
#[derive(Debug)]
pub struct PreparedPanelRender<O> {
    /// The request id that produced this result.
    pub request_id: u64,
    /// Caller-managed generation.
    pub generation: u64,
    /// Stable caller key.
    pub key: u64,
    /// Rendered output payload.
    pub output: O,
}

/// Errors returned while creating a panel renderer worker.
#[derive(Debug, Error)]
pub enum PanelInitError {
    /// The background worker thread could not be started.
    #[error("failed to start panel renderer worker thread")]
    Spawn(#[source] std::io::Error),
}

/// Errors returned when submitting a panel render request.
#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub enum PanelSubmitError {
    /// Requests must target a non-empty area and non-zero pixel bounds.
    #[error(
        "panel render dimensions must be non-zero, got area={area_width}x{area_height} cells and max={max_pixel_width}x{max_pixel_height} px"
    )]
    InvalidDimensions {
        area_width: u16,
        area_height: u16,
        max_pixel_width: u32,
        max_pixel_height: u32,
    },
    /// Request ids are exhausted.
    #[error("panel renderer request id space is exhausted")]
    RequestIdExhausted,
    /// The worker has already stopped.
    #[error("panel renderer worker is no longer running")]
    WorkerStopped,
    /// Internal worker state was poisoned by a panic.
    #[error("panel renderer worker state is poisoned")]
    StatePoisoned,
}

/// Errors returned while fulfilling a render request.
#[derive(Debug, Error)]
pub enum PanelRenderError<E>
where
    E: Display + Debug + Send + Sync + 'static,
{
    /// The caller's renderer callback failed.
    #[error("panel render request {request_id} failed in the render callback: {source}")]
    Render {
        /// The request that failed.
        request_id: u64,
        /// The underlying callback error.
        source: E,
    },
    /// `ratatui-image` failed while adapting the raster to the chosen protocol.
    #[error("panel render request {request_id} failed while creating a panel protocol")]
    Protocol {
        /// The request that failed.
        request_id: u64,
        /// The underlying protocol error.
        #[source]
        source: PanelProtocolError,
    },
    /// The worker panicked while handling this request.
    #[error("panel render request {request_id} panicked in the worker thread")]
    WorkerPanic {
        /// The request that panicked.
        request_id: u64,
    },
}

impl<E> PanelRenderError<E>
where
    E: Display + Debug + Send + Sync + 'static,
{
    /// Return the request id associated with this error.
    pub fn request_id(&self) -> u64 {
        match self {
            Self::Render { request_id, .. }
            | Self::Protocol { request_id, .. }
            | Self::WorkerPanic { request_id } => *request_id,
        }
    }
}

/// Errors returned when polling the panel renderer worker.
#[derive(Debug, Error)]
pub enum PanelWorkerError<E>
where
    E: Display + Debug + Send + Sync + 'static,
{
    /// The latest request failed during rendering.
    #[error(transparent)]
    Render(#[from] PanelRenderError<E>),
    /// The worker stopped before the latest request completed.
    #[error("panel renderer worker stopped before request {request_id} completed")]
    Disconnected {
        /// The request that was still pending.
        request_id: u64,
    },
}

/// Errors returned when submitting a render-pool request.
#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub enum PanelRenderPoolSubmitError {
    /// Request ids are exhausted.
    #[error("panel render pool request id space is exhausted")]
    RequestIdExhausted,
    /// The worker has already stopped.
    #[error("panel render pool worker is no longer running")]
    WorkerStopped,
    /// Internal worker state was poisoned by a panic.
    #[error("panel render pool worker state is poisoned")]
    StatePoisoned,
    /// The bounded queue is full.
    #[error("panel render pool queue is full")]
    QueueFull,
}

/// Errors returned while fulfilling a render-pool request.
#[derive(Debug, Error)]
pub enum PanelRenderPoolError<E>
where
    E: Display + Debug + Send + Sync + 'static,
{
    /// The caller's renderer callback failed.
    #[error("panel render pool request {request_id} failed in the render callback: {source}")]
    Render {
        /// The request that failed.
        request_id: u64,
        /// The generation associated with the request.
        generation: u64,
        /// Stable caller key.
        key: u64,
        /// The underlying callback error.
        source: E,
    },
    /// The worker panicked while handling this request.
    #[error("panel render pool request {request_id} panicked in the worker thread")]
    WorkerPanic {
        /// The request that panicked.
        request_id: u64,
        /// The generation associated with the request.
        generation: u64,
        /// Stable caller key.
        key: u64,
    },
}

impl<E> PanelRenderPoolError<E>
where
    E: Display + Debug + Send + Sync + 'static,
{
    /// Return the request id associated with this error.
    pub fn request_id(&self) -> u64 {
        match self {
            Self::Render { request_id, .. } | Self::WorkerPanic { request_id, .. } => *request_id,
        }
    }

    /// Return the generation associated with this error.
    pub fn generation(&self) -> u64 {
        match self {
            Self::Render { generation, .. } | Self::WorkerPanic { generation, .. } => *generation,
        }
    }
}

/// Result of draining completed work from a [`PanelRenderPool`].
#[derive(Debug)]
pub struct PanelRenderPoolDrain<O, E>
where
    E: Display + Debug + Send + Sync + 'static,
{
    /// Ready results that matched the requested generation.
    pub ready: Vec<PreparedPanelRender<O>>,
    /// Completed results discarded because they belonged to an older generation.
    pub stale_count: u64,
    /// Errors surfaced while draining.
    pub errors: Vec<PanelRenderPoolError<E>>,
}

impl<O, E> Default for PanelRenderPoolDrain<O, E>
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

struct WorkerState<T> {
    pending_job: Option<PanelRenderJob<T>>,
    shutdown: bool,
}

struct PoolWorkerState<T> {
    queued_jobs: VecDeque<PanelRenderPoolJob<T>>,
    shutdown: bool,
    queue_capacity: usize,
}

/// A stateful panel renderer that owns worker lifecycle and stale-result handling.
pub struct PanelRenderer<T, E>
where
    T: Send + 'static,
    E: Display + Debug + Send + Sync + 'static,
{
    queue: Arc<(Mutex<WorkerState<T>>, Condvar)>,
    completions: Receiver<Result<PreparedPanelProtocol, PanelRenderError<E>>>,
    worker_alive: Arc<AtomicBool>,
    worker_handle: Option<JoinHandle<()>>,
    next_request_id: u64,
    latest_requested_id: Option<u64>,
    current_request_id: Option<u64>,
    pending_latest: bool,
    current_protocol: Option<PanelProtocol>,
    current_image_size: Option<(u32, u32)>,
    current_rendered_image: Option<RgbaImage>,
    stale_result_count: u64,
}

impl<T, E> PanelRenderer<T, E>
where
    T: Send + 'static,
    E: Display + Debug + Send + Sync + 'static,
{
    /// Create a panel renderer backed by a single worker thread.
    pub fn new<F>(picker: Picker, resize: Resize, renderer: F) -> Result<Self, PanelInitError>
    where
        F: FnMut(&PanelRenderJob<T>) -> Result<DynamicImage, E> + Send + 'static,
    {
        let queue = Arc::new((
            Mutex::new(WorkerState {
                pending_job: None,
                shutdown: false,
            }),
            Condvar::new(),
        ));
        let (tx, rx) = mpsc::channel::<Result<PreparedPanelProtocol, PanelRenderError<E>>>();
        let worker_alive = Arc::new(AtomicBool::new(true));

        let worker_queue = Arc::clone(&queue);
        let worker_alive_flag = Arc::clone(&worker_alive);
        let worker_picker = picker;
        let worker_resize = resize;

        let handle = thread::Builder::new()
            .name("ghostty-panel-renderer".to_string())
            .spawn(move || {
                let mut renderer = renderer;

                loop {
                    let job = {
                        let (lock, cvar) = &*worker_queue;
                        let mut state = match lock.lock() {
                            Ok(state) => state,
                            Err(_) => break,
                        };
                        while state.pending_job.is_none() && !state.shutdown {
                            state = match cvar.wait(state) {
                                Ok(state) => state,
                                Err(_) => {
                                    worker_alive_flag.store(false, Ordering::SeqCst);
                                    return;
                                }
                            };
                        }

                        if state.shutdown {
                            break;
                        }

                        state.pending_job.take()
                    };

                    let Some(job) = job else {
                        continue;
                    };
                    let request_id = job.request_id;

                    let (result, should_stop) = match panic::catch_unwind(AssertUnwindSafe(|| {
                        render_panel_protocol(&worker_picker, worker_resize.clone(), &job, |job| {
                            renderer(job)
                        })
                    })) {
                        Ok(result) => (result, false),
                        Err(_) => (Err(PanelRenderError::WorkerPanic { request_id }), true),
                    };

                    if tx.send(result).is_err() {
                        break;
                    }
                    if should_stop {
                        break;
                    }
                }

                worker_alive_flag.store(false, Ordering::SeqCst);
            })
            .map_err(PanelInitError::Spawn)?;

        Ok(Self {
            queue,
            completions: rx,
            worker_alive,
            worker_handle: Some(handle),
            next_request_id: 0,
            latest_requested_id: None,
            current_request_id: None,
            pending_latest: false,
            current_protocol: None,
            current_image_size: None,
            current_rendered_image: None,
            stale_result_count: 0,
        })
    }

    /// Submit a new render request without blocking the UI thread.
    ///
    /// If another request is already queued but not yet started, it is replaced.
    pub fn request(
        &mut self,
        area: Rect,
        max_pixel_width: u32,
        max_pixel_height: u32,
        input: T,
    ) -> Result<u64, PanelSubmitError> {
        self.request_inner(area, max_pixel_width, max_pixel_height, input, true)
    }

    /// Submit a new render request that only prepares the raster image, not a panel protocol.
    pub fn request_render_only(
        &mut self,
        area: Rect,
        max_pixel_width: u32,
        max_pixel_height: u32,
        input: T,
    ) -> Result<u64, PanelSubmitError> {
        self.request_inner(area, max_pixel_width, max_pixel_height, input, false)
    }

    fn request_inner(
        &mut self,
        area: Rect,
        max_pixel_width: u32,
        max_pixel_height: u32,
        input: T,
        build_protocol: bool,
    ) -> Result<u64, PanelSubmitError> {
        if area.is_empty() || max_pixel_width == 0 || max_pixel_height == 0 {
            return Err(PanelSubmitError::InvalidDimensions {
                area_width: area.width,
                area_height: area.height,
                max_pixel_width,
                max_pixel_height,
            });
        }
        if !self.worker_alive.load(Ordering::SeqCst) {
            return Err(PanelSubmitError::WorkerStopped);
        }

        let request_id = self
            .next_request_id
            .checked_add(1)
            .ok_or(PanelSubmitError::RequestIdExhausted)?;
        self.next_request_id = request_id;

        let job = PanelRenderJob {
            request_id,
            area,
            max_pixel_width,
            max_pixel_height,
            build_protocol,
            input,
        };

        let (lock, cvar) = &*self.queue;
        let mut state = lock.lock().map_err(|_| PanelSubmitError::StatePoisoned)?;
        if state.shutdown || !self.worker_alive.load(Ordering::SeqCst) {
            return Err(PanelSubmitError::WorkerStopped);
        }
        state.pending_job = Some(job);
        self.latest_requested_id = Some(request_id);
        self.pending_latest = true;
        cvar.notify_one();

        Ok(request_id)
    }

    /// Poll the worker and update the currently visible protocol if the latest request completed.
    pub fn pump(&mut self) -> Result<bool, PanelWorkerError<E>> {
        let mut changed = false;

        loop {
            match self.completions.try_recv() {
                Ok(result) => match result {
                    Ok(prepared) => {
                        if Some(prepared.request_id) == self.latest_requested_id {
                            self.pending_latest = false;
                            self.current_image_size =
                                Some((prepared.image_width, prepared.image_height));
                            self.current_request_id = Some(prepared.request_id);
                            self.current_rendered_image = Some(prepared.rendered_image);
                            self.current_protocol = prepared.protocol;
                            changed = true;
                        } else {
                            self.stale_result_count = self.stale_result_count.saturating_add(1);
                        }
                    }
                    Err(error) => {
                        if Some(error.request_id()) == self.latest_requested_id {
                            self.pending_latest = false;
                            return Err(PanelWorkerError::Render(error));
                        } else {
                            self.stale_result_count = self.stale_result_count.saturating_add(1);
                        }
                    }
                },
                Err(TryRecvError::Empty) => break,
                Err(TryRecvError::Disconnected) => {
                    self.worker_alive.store(false, Ordering::SeqCst);
                    if let Some(request_id) =
                        self.latest_requested_id.filter(|_| self.pending_latest)
                    {
                        return Err(PanelWorkerError::Disconnected { request_id });
                    }
                    break;
                }
            }
        }

        Ok(changed)
    }

    /// Return the current prepared protocol, if one has completed successfully.
    pub fn protocol(&self) -> Option<&PanelProtocol> {
        self.current_protocol.as_ref()
    }

    /// Return the current raster size, if one has completed successfully.
    pub fn image_size(&self) -> Option<(u32, u32)> {
        self.current_image_size
    }

    /// Return the current rendered bitmap, if one has completed successfully.
    pub fn rendered_image(&self) -> Option<&RgbaImage> {
        self.current_rendered_image.as_ref()
    }

    /// Return whether the latest request is still pending.
    pub fn is_pending(&self) -> bool {
        self.pending_latest
    }

    /// Return an approximate request queue depth for the latest request.
    pub fn queue_depth(&self) -> usize {
        usize::from(self.pending_latest)
    }

    /// Return the number of stale worker completions observed since the last call.
    pub fn take_stale_result_count(&mut self) -> u64 {
        let count = self.stale_result_count;
        self.stale_result_count = 0;
        count
    }
}

impl<T, E> Drop for PanelRenderer<T, E>
where
    T: Send + 'static,
    E: Display + Debug + Send + Sync + 'static,
{
    fn drop(&mut self) {
        let (lock, cvar) = &*self.queue;
        if let Ok(mut state) = lock.lock() {
            state.shutdown = true;
            state.pending_job = None;
            cvar.notify_all();
        }

        if let Some(handle) = self.worker_handle.take() {
            let _ = handle.join();
        }
    }
}

/// A bounded multi-worker render pool for ahead-of-time bitmap preparation.
pub struct PanelRenderPool<T, O, E>
where
    T: Send + 'static,
    O: Send + 'static,
    E: Display + Debug + Send + Sync + 'static,
{
    queue: Arc<(Mutex<PoolWorkerState<T>>, Condvar)>,
    completions: Receiver<Result<PreparedPanelRender<O>, PanelRenderPoolError<E>>>,
    worker_alive: Arc<AtomicBool>,
    active_jobs: Arc<AtomicUsize>,
    worker_handles: Vec<JoinHandle<()>>,
    next_request_id: u64,
    stale_result_count: u64,
}

impl<T, O, E> PanelRenderPool<T, O, E>
where
    T: Send + 'static,
    O: Send + 'static,
    E: Display + Debug + Send + Sync + 'static,
{
    /// Create a bounded multi-worker render pool.
    pub fn new<F>(
        worker_count: usize,
        queue_capacity: usize,
        renderer: F,
    ) -> Result<Self, PanelInitError>
    where
        F: Fn(&PanelRenderPoolJob<T>) -> Result<O, E> + Send + Sync + 'static,
    {
        let queue = Arc::new((
            Mutex::new(PoolWorkerState {
                queued_jobs: VecDeque::new(),
                shutdown: false,
                queue_capacity: queue_capacity.max(1),
            }),
            Condvar::new(),
        ));
        let (tx, rx) = mpsc::channel::<Result<PreparedPanelRender<O>, PanelRenderPoolError<E>>>();
        let worker_alive = Arc::new(AtomicBool::new(true));
        let active_jobs = Arc::new(AtomicUsize::new(0));
        let renderer = Arc::new(renderer);
        let worker_count = worker_count.max(1);
        let mut worker_handles = Vec::with_capacity(worker_count);

        for worker_index in 0..worker_count {
            let worker_queue = Arc::clone(&queue);
            let worker_alive_flag = Arc::clone(&worker_alive);
            let worker_active_jobs = Arc::clone(&active_jobs);
            let worker_renderer = Arc::clone(&renderer);
            let worker_tx = tx.clone();
            let handle = thread::Builder::new()
                .name(format!("ghostty-panel-render-pool-{worker_index}"))
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
                                        worker_alive_flag.store(false, Ordering::SeqCst);
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
                        worker_active_jobs.fetch_add(1, Ordering::SeqCst);
                        let request_id = job.request_id;
                        let generation = job.generation;
                        let key = job.key;
                        let result =
                            panic::catch_unwind(AssertUnwindSafe(|| worker_renderer(&job)))
                                .map_err(|_| PanelRenderPoolError::WorkerPanic {
                                    request_id,
                                    generation,
                                    key,
                                })
                                .and_then(|result| {
                                    result
                                        .map(|output| PreparedPanelRender {
                                            request_id,
                                            generation,
                                            key,
                                            output,
                                        })
                                        .map_err(|source| PanelRenderPoolError::Render {
                                            request_id,
                                            generation,
                                            key,
                                            source,
                                        })
                                });
                        worker_active_jobs.fetch_sub(1, Ordering::SeqCst);
                        if worker_tx.send(result).is_err() {
                            worker_alive_flag.store(false, Ordering::SeqCst);
                            return;
                        }
                    }
                    worker_alive_flag.store(false, Ordering::SeqCst);
                })
                .map_err(PanelInitError::Spawn)?;
            worker_handles.push(handle);
        }

        Ok(Self {
            queue,
            completions: rx,
            worker_alive,
            active_jobs,
            worker_handles,
            next_request_id: 1,
            stale_result_count: 0,
        })
    }

    /// Submit a render request to the pool.
    pub fn submit(
        &mut self,
        generation: u64,
        key: u64,
        input: T,
    ) -> Result<u64, PanelRenderPoolSubmitError> {
        if !self.worker_alive.load(Ordering::SeqCst) {
            return Err(PanelRenderPoolSubmitError::WorkerStopped);
        }
        let request_id = self
            .next_request_id
            .checked_add(1)
            .ok_or(PanelRenderPoolSubmitError::RequestIdExhausted)?;
        self.next_request_id = request_id;
        let (lock, cvar) = &*self.queue;
        let mut state = lock
            .lock()
            .map_err(|_| PanelRenderPoolSubmitError::StatePoisoned)?;
        if state.shutdown || !self.worker_alive.load(Ordering::SeqCst) {
            return Err(PanelRenderPoolSubmitError::WorkerStopped);
        }
        if state.queued_jobs.len() >= state.queue_capacity {
            return Err(PanelRenderPoolSubmitError::QueueFull);
        }
        state.queued_jobs.push_back(PanelRenderPoolJob {
            request_id,
            generation,
            key,
            input,
        });
        cvar.notify_one();
        Ok(request_id)
    }

    /// Drain completed results, discarding stale generations.
    pub fn drain_ready(&mut self, current_generation: u64) -> PanelRenderPoolDrain<O, E> {
        let mut drain = PanelRenderPoolDrain::default();
        loop {
            match self.completions.try_recv() {
                Ok(Ok(prepared)) => {
                    if prepared.generation == current_generation {
                        drain.ready.push(prepared);
                    } else {
                        self.stale_result_count = self.stale_result_count.saturating_add(1);
                        drain.stale_count = drain.stale_count.saturating_add(1);
                    }
                }
                Ok(Err(error)) => {
                    if error.generation() == current_generation {
                        drain.errors.push(error);
                    } else {
                        self.stale_result_count = self.stale_result_count.saturating_add(1);
                        drain.stale_count = drain.stale_count.saturating_add(1);
                    }
                }
                Err(TryRecvError::Empty) => break,
                Err(TryRecvError::Disconnected) => {
                    self.worker_alive.store(false, Ordering::SeqCst);
                    break;
                }
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

    /// Number of workers in the pool.
    pub fn worker_count(&self) -> usize {
        self.worker_handles.len()
    }

    /// Number of jobs currently executing in worker threads.
    pub fn active_job_count(&self) -> usize {
        self.active_jobs.load(Ordering::SeqCst)
    }

    /// Return the number of stale worker completions observed since the last call.
    pub fn take_stale_result_count(&mut self) -> u64 {
        let count = self.stale_result_count;
        self.stale_result_count = 0;
        count
    }
}

impl<T, O, E> Drop for PanelRenderPool<T, O, E>
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

/// Render an image and convert it into a `ratatui-image` protocol for the target area.
pub fn render_panel_protocol<T, E, F>(
    picker: &Picker,
    resize: Resize,
    job: &PanelRenderJob<T>,
    renderer: F,
) -> Result<PreparedPanelProtocol, PanelRenderError<E>>
where
    E: Display + Debug + Send + Sync + 'static,
    F: FnOnce(&PanelRenderJob<T>) -> Result<DynamicImage, E>,
{
    let image = renderer(job).map_err(|source| PanelRenderError::Render {
        request_id: job.request_id,
        source,
    })?;
    let rendered_image = image.to_rgba8();
    let image_width = rendered_image.width();
    let image_height = rendered_image.height();
    let protocol = if job.build_protocol {
        Some(
            picker
                .new_protocol(
                    DynamicImage::ImageRgba8(rendered_image.clone()),
                    job.area,
                    resize,
                )
                .map_err(|source| PanelRenderError::Protocol {
                    request_id: job.request_id,
                    source,
                })?,
        )
    } else {
        None
    };

    Ok(PreparedPanelProtocol {
        request_id: job.request_id,
        area: job.area,
        image_width,
        image_height,
        protocol,
        rendered_image,
    })
}

/// Build a one-shot `ratatui-image` protocol from an already-rendered bitmap.
pub fn build_panel_protocol_from_rgba(
    picker: &Picker,
    resize: Resize,
    area: Rect,
    rendered_image: RgbaImage,
) -> Result<PreparedPanelProtocol, PanelProtocolError> {
    let image_width = rendered_image.width();
    let image_height = rendered_image.height();
    let protocol = picker.new_protocol(
        DynamicImage::ImageRgba8(rendered_image.clone()),
        area,
        resize,
    )?;
    Ok(PreparedPanelProtocol {
        request_id: 0,
        area,
        image_width,
        image_height,
        protocol: Some(protocol),
        rendered_image,
    })
}

/// Build a one-shot `ratatui-image` protocol from an already-rendered bitmap, consuming it.
///
/// Use this when the caller does not need to retain the source bitmap after protocol creation.
pub fn build_panel_protocol_from_rgba_owned(
    picker: &Picker,
    resize: Resize,
    area: Rect,
    rendered_image: RgbaImage,
) -> Result<PreparedPanelProtocolOnly, PanelProtocolError> {
    let image_width = rendered_image.width();
    let image_height = rendered_image.height();
    let protocol = picker.new_protocol(DynamicImage::ImageRgba8(rendered_image), area, resize)?;
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
        sync::{
            Arc, Mutex,
            atomic::{AtomicBool, Ordering},
        },
        thread,
        time::{Duration, Instant},
    };

    use image::{DynamicImage, RgbaImage};
    use ratatui::layout::Rect;
    use ratatui_image::{Resize, picker::Picker};

    use super::{
        PanelRenderError, PanelRenderPool, PanelRenderer, PanelSubmitError, PanelWorkerError,
        PreparedPanelProtocol, build_panel_protocol_from_rgba_owned, render_panel_protocol,
    };

    #[derive(Debug, Clone)]
    struct TestError;

    impl fmt::Display for TestError {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            write!(f, "test error")
        }
    }

    impl std::error::Error for TestError {}

    fn wait_until_idle(renderer: &mut PanelRenderer<u32, TestError>) {
        for _ in 0..80 {
            let _ = renderer.pump();
            if !renderer.is_pending() {
                return;
            }
            thread::sleep(Duration::from_millis(10));
        }
        panic!("panel renderer did not settle in time");
    }

    #[test]
    fn latest_request_wins() {
        let processed = Arc::new(Mutex::new(Vec::<u64>::new()));
        let processed_in_worker = Arc::clone(&processed);

        let mut renderer =
            PanelRenderer::new(Picker::halfblocks(), Resize::Fit(None), move |job| {
                processed_in_worker.lock().unwrap().push(job.request_id);
                thread::sleep(Duration::from_millis(25));
                Ok(DynamicImage::ImageRgba8(RgbaImage::new(
                    job.max_pixel_width.max(1),
                    job.max_pixel_height.max(1),
                )))
            })
            .unwrap();

        let area = Rect::new(0, 0, 10, 10);
        let _ = renderer.request(area, 40, 20, 1).unwrap();
        thread::sleep(Duration::from_millis(5));
        let _ = renderer.request(area, 50, 30, 2).unwrap();
        let latest_id = renderer.request(area, 60, 40, 3).unwrap();

        wait_until_idle(&mut renderer);

        assert_eq!(renderer.current_request_id, Some(latest_id));
        assert_eq!(renderer.image_size(), Some((60, 40)));
        assert!(renderer.protocol().is_some());
        let seen = processed.lock().unwrap().clone();
        assert_eq!(seen.first().copied(), Some(1));
        assert_eq!(seen.last().copied(), Some(latest_id));
        assert!(!seen.contains(&2));
    }

    #[test]
    fn render_only_requests_skip_protocol_creation() {
        let mut renderer = PanelRenderer::new(Picker::halfblocks(), Resize::Fit(None), |_job| {
            Ok(DynamicImage::ImageRgba8(RgbaImage::new(40, 20)))
        })
        .unwrap();

        let area = Rect::new(0, 0, 10, 10);
        let _ = renderer.request_render_only(area, 40, 20, 1).unwrap();

        wait_until_idle(&mut renderer);

        assert_eq!(renderer.image_size(), Some((40, 20)));
        assert!(renderer.rendered_image().is_some());
        assert!(renderer.protocol().is_none());
    }

    #[test]
    fn stale_results_are_ignored_by_request_id() {
        let mut renderer =
            PanelRenderer::new(Picker::halfblocks(), Resize::Fit(None), move |job| {
                let delay = if job.request_id == 1 { 30 } else { 5 };
                thread::sleep(Duration::from_millis(delay));
                Ok(DynamicImage::ImageRgba8(RgbaImage::new(
                    job.max_pixel_width.max(1),
                    job.max_pixel_height.max(1),
                )))
            })
            .unwrap();

        let area = Rect::new(0, 0, 10, 10);
        let _ = renderer.request(area, 40, 20, 1).unwrap();
        let latest_id = renderer.request(area, 70, 50, 2).unwrap();

        wait_until_idle(&mut renderer);

        assert_eq!(renderer.current_request_id, Some(latest_id));
        assert_eq!(renderer.image_size(), Some((70, 50)));
    }

    #[test]
    fn worker_panic_surfaces_cleanly() {
        let mut renderer = PanelRenderer::new(
            Picker::halfblocks(),
            Resize::Fit(None),
            |_job| -> Result<DynamicImage, TestError> {
                panic!("boom");
            },
        )
        .unwrap();

        let area = Rect::new(0, 0, 10, 10);
        let request_id = renderer.request(area, 20, 20, 1).unwrap();

        let err = loop {
            match renderer.pump() {
                Err(err) => break err,
                Ok(_) => thread::sleep(Duration::from_millis(10)),
            }
        };

        match err {
            PanelWorkerError::Render(PanelRenderError::WorkerPanic { request_id: seen }) => {
                assert_eq!(seen, request_id);
            }
            other => panic!("unexpected worker error: {other:?}"),
        }

        let submit = renderer.request(area, 20, 20, 2).unwrap_err();
        assert_eq!(
            submit.to_string(),
            "panel renderer worker is no longer running"
        );
    }

    #[test]
    fn request_rejects_invalid_dimensions_and_exhausted_ids() {
        let mut renderer: PanelRenderer<i32, TestError> =
            PanelRenderer::new(Picker::halfblocks(), Resize::Fit(None), |_job| {
                Ok(DynamicImage::ImageRgba8(RgbaImage::new(1, 1)))
            })
            .unwrap();

        let err = renderer
            .request(Rect::new(0, 0, 0, 5), 10, 10, 1)
            .unwrap_err();
        assert!(matches!(
            err,
            PanelSubmitError::InvalidDimensions {
                area_width: 0,
                area_height: 5,
                max_pixel_width: 10,
                max_pixel_height: 10
            }
        ));

        renderer.next_request_id = u64::MAX;
        let err = renderer
            .request(Rect::new(0, 0, 5, 5), 10, 10, 1)
            .unwrap_err();
        assert!(matches!(err, PanelSubmitError::RequestIdExhausted));
    }

    #[test]
    fn render_pool_discards_stale_generations() {
        let mut pool = PanelRenderPool::new(2, 4, |job| -> Result<u32, TestError> {
            let delay_ms = if job.generation == 1 { 40 } else { 5 };
            thread::sleep(Duration::from_millis(delay_ms));
            Ok(job.input)
        })
        .unwrap();

        let _ = pool.submit(1, 10, 10).unwrap();
        let _ = pool.submit(2, 20, 20).unwrap();
        thread::sleep(Duration::from_millis(80));

        let drain = pool.drain_ready(2);
        assert_eq!(drain.ready.len(), 1);
        assert_eq!(drain.ready[0].generation, 2);
        assert_eq!(drain.ready[0].output, 20);
        assert_eq!(drain.stale_count, 1);
    }

    #[test]
    fn render_pool_reports_active_jobs_while_workers_are_busy() {
        let release = Arc::new(AtomicBool::new(false));
        let release_in_worker = Arc::clone(&release);
        let mut pool = PanelRenderPool::new(2, 4, move |job| -> Result<u32, TestError> {
            while !release_in_worker.load(Ordering::SeqCst) {
                thread::sleep(Duration::from_millis(5));
            }
            Ok(job.input)
        })
        .unwrap();

        let _ = pool.submit(1, 10, 10).unwrap();
        let _ = pool.submit(1, 11, 11).unwrap();

        let start = Instant::now();
        while start.elapsed() < Duration::from_millis(200) && pool.active_job_count() < 2 {
            thread::sleep(Duration::from_millis(5));
        }
        assert_eq!(pool.active_job_count(), 2);

        release.store(true, Ordering::SeqCst);
        thread::sleep(Duration::from_millis(20));
        let _ = pool.drain_ready(1);
    }

    #[test]
    fn pump_reports_disconnected_worker_for_pending_latest_request() {
        let mut renderer: PanelRenderer<i32, TestError> =
            PanelRenderer::new(Picker::halfblocks(), Resize::Fit(None), |_job| {
                Ok(DynamicImage::ImageRgba8(RgbaImage::new(1, 1)))
            })
            .unwrap();
        let area = Rect::new(0, 0, 4, 4);
        let request_id = renderer.request(area, 8, 8, 1).unwrap();
        let (tx, rx) = std::sync::mpsc::channel();
        drop(tx);
        renderer.completions = rx;
        renderer.latest_requested_id = Some(request_id);
        renderer.pending_latest = true;

        match renderer.pump().unwrap_err() {
            PanelWorkerError::Disconnected { request_id: seen } => assert_eq!(seen, request_id),
            other => panic!("unexpected disconnect error: {other:?}"),
        }
    }

    #[test]
    fn render_panel_protocol_wraps_renderer_error() {
        let picker = Picker::halfblocks();
        let job = super::PanelRenderJob {
            request_id: 7,
            area: Rect::new(0, 0, 8, 8),
            max_pixel_width: 16,
            max_pixel_height: 16,
            build_protocol: true,
            input: (),
        };
        let err =
            match render_panel_protocol(&picker, Resize::Fit(None), &job, |_job| Err(TestError)) {
                Ok(_) => panic!("expected render error"),
                Err(err) => err,
            };
        match err {
            PanelRenderError::Render { request_id, .. } => assert_eq!(request_id, 7),
            other => panic!("unexpected render error: {other:?}"),
        }
    }

    #[test]
    fn render_panel_protocol_reports_image_dimensions() {
        let picker = Picker::halfblocks();
        let job = super::PanelRenderJob {
            request_id: 9,
            area: Rect::new(0, 0, 4, 4),
            max_pixel_width: 12,
            max_pixel_height: 10,
            build_protocol: true,
            input: (),
        };
        let prepared: PreparedPanelProtocol =
            render_panel_protocol(&picker, Resize::Fit(None), &job, |_job| {
                Ok::<DynamicImage, TestError>(DynamicImage::ImageRgba8(RgbaImage::new(12, 10)))
            })
            .unwrap();
        assert_eq!(prepared.request_id, 9);
        assert_eq!(prepared.area, job.area);
        assert_eq!(prepared.image_width, 12);
        assert_eq!(prepared.image_height, 10);
        assert!(prepared.protocol.is_some());
    }

    #[test]
    fn build_panel_protocol_from_rgba_owned_consumes_bitmap_without_retaining_copy() {
        let picker = Picker::halfblocks();
        let prepared = build_panel_protocol_from_rgba_owned(
            &picker,
            Resize::Fit(None),
            Rect::new(0, 0, 4, 4),
            RgbaImage::new(12, 10),
        )
        .unwrap();
        assert_eq!(prepared.area, Rect::new(0, 0, 4, 4));
        assert_eq!(prepared.image_width, 12);
        assert_eq!(prepared.image_height, 10);
    }
}

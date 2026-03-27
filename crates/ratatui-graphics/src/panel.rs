// SPDX-License-Identifier: LGPL-3.0-or-later
//! Stateful panel rendering support built on top of `ratatui-image`.

use std::{
    fmt::{Debug, Display},
    panic::{self, AssertUnwindSafe},
    sync::{
        Arc, Condvar, Mutex,
        atomic::{AtomicBool, Ordering},
        mpsc::{self, Receiver, TryRecvError},
    },
    thread::{self, JoinHandle},
};

use image::DynamicImage;
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
    /// The encoded `ratatui-image` protocol ready for use with `Image`.
    pub protocol: PanelProtocol,
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

struct WorkerState<T> {
    pending_job: Option<PanelRenderJob<T>>,
    shutdown: bool,
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
                            self.current_protocol = Some(prepared.protocol);
                            changed = true;
                        }
                    }
                    Err(error) => {
                        if Some(error.request_id()) == self.latest_requested_id {
                            self.pending_latest = false;
                            return Err(PanelWorkerError::Render(error));
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

    /// Return whether the latest request is still pending.
    pub fn is_pending(&self) -> bool {
        self.pending_latest
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
    let image_width = image.width();
    let image_height = image.height();
    let protocol = picker
        .new_protocol(image, job.area, resize)
        .map_err(|source| PanelRenderError::Protocol {
            request_id: job.request_id,
            source,
        })?;

    Ok(PreparedPanelProtocol {
        request_id: job.request_id,
        area: job.area,
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
        time::Duration,
    };

    use image::{DynamicImage, RgbaImage};
    use ratatui::layout::Rect;
    use ratatui_image::{Resize, picker::Picker};

    use super::{PanelRenderError, PanelRenderer, PanelWorkerError};

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
        let seen = processed.lock().unwrap().clone();
        assert_eq!(seen.first().copied(), Some(1));
        assert_eq!(seen.last().copied(), Some(latest_id));
        assert!(!seen.contains(&2));
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
}

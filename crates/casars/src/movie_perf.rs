// SPDX-License-Identifier: LGPL-3.0-or-later
use std::collections::{HashMap, VecDeque};
use std::fs::{File, OpenOptions, create_dir_all};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::Once;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

use serde::Serialize;

const PERF_ENV: &str = "CASARS_IMEXPLORE_PERF";
const PERF_DIR_ENV: &str = "CASARS_IMEXPLORE_PERF_DIR";
const SUMMARY_INTERVAL: Duration = Duration::from_secs(5);
const RECENT_FRAME_CAPACITY: usize = 512;

static FORCE_SUMMARY_FLUSH: AtomicBool = AtomicBool::new(false);
#[cfg(unix)]
static SIGNAL_INSTALLED: Once = Once::new();

#[cfg(unix)]
extern "C" fn sigusr1_handler(_: libc::c_int) {
    FORCE_SUMMARY_FLUSH.store(true, Ordering::SeqCst);
}

fn install_perf_signal_handler() {
    #[cfg(unix)]
    SIGNAL_INSTALLED.call_once(|| unsafe {
        libc::signal(
            libc::SIGUSR1,
            sigusr1_handler as *const () as libc::sighandler_t,
        );
    });
}

#[derive(Debug, Clone, Copy, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub(crate) enum MoviePerfEventKind {
    MovieStarted,
    MovieStopped,
    FpsChanged,
    DirectOverlayChanged,
    FrameRequested,
    PreviewRequested,
    PreviewReceived,
    BundleRenderRequested,
    BundleReady,
    BundlePresented,
    GenerationInvalidated,
    DeadlineMissed,
    BrowserCommandSent,
    BrowserSnapshotReceived,
    PlaneRenderRequested,
    PlaneRenderCompleted,
    PlanePresented,
    FrameDropped,
    Summary,
}

#[derive(Debug, Clone, Copy, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub(crate) enum MovieFrameOutcome {
    CacheHitRenderedImage,
    CacheHitBackendPlane,
    CacheMiss,
    StaleRenderDiscarded,
    SkippedDueToPending,
}

#[derive(Debug, Clone, Copy, Serialize, PartialEq, Eq, Default)]
pub(crate) struct BackendTimingBreakdown {
    pub cached_plane_lookup_ns: u64,
    pub plane_extract_ns: u64,
    pub stat_collection_ns: u64,
    pub histogram_ns: u64,
    pub rasterize_ns: u64,
    pub total_plane_ns: u64,
    pub profile_cache_hits: u64,
    pub profile_cache_misses: u64,
    pub profile_extract_total_ns: u64,
}

#[derive(Debug, Clone, Copy, Serialize, PartialEq, Eq, Default)]
pub(crate) struct MoviePipelineState {
    pub render_queue_depth: usize,
    pub render_active_jobs: usize,
    pub protocol_queue_depth: usize,
    pub protocol_active_jobs: usize,
    pub ready_bundle_count: usize,
    pub ready_presentation_count: usize,
    pub bitmap_cache_bytes: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub(crate) struct MoviePerfContext {
    pub axis: Option<usize>,
    pub axis_index: Option<usize>,
    pub axis_length: Option<usize>,
    pub render_request_key_hash: Option<u64>,
    pub canvas_cell_size: Option<(u16, u16)>,
    pub canvas_pixel_size: Option<(u32, u32)>,
    pub raster_mode: bool,
    pub direct_overlay: bool,
    pub terminal_looping: bool,
    pub requested_fps_milli: Option<u64>,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub(crate) struct MoviePerfEvent {
    pub kind: MoviePerfEventKind,
    pub monotonic_ns: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub frame_seq: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub axis: Option<usize>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub axis_index: Option<usize>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub axis_length: Option<usize>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub render_request_key_hash: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub canvas_cell_width: Option<u16>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub canvas_cell_height: Option<u16>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub canvas_pixel_width: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub canvas_pixel_height: Option<u32>,
    pub plane_mode: &'static str,
    pub direct_overlay: bool,
    pub terminal_looping: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub requested_fps_milli: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub duration_ns: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub queue_depth: Option<usize>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub panel_pending: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub outcome: Option<MovieFrameOutcome>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub backend: Option<BackendTimingBreakdown>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub pipeline: Option<MoviePipelineState>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub note: Option<String>,
}

#[derive(Debug, Clone)]
pub(crate) struct MovieFrameTrace {
    pub frame_seq: u64,
    pub context: MoviePerfContext,
    pub frame_requested_at: Instant,
    pub browser_command_sent_at: Option<Instant>,
    pub browser_snapshot_received_at: Option<Instant>,
    pub plane_render_requested_at: Option<Instant>,
    pub plane_render_completed_at: Option<Instant>,
    pub plane_presented_at: Option<Instant>,
    pub outcome: Option<MovieFrameOutcome>,
    pub backend: Option<BackendTimingBreakdown>,
}

#[derive(Debug, Clone, Copy)]
struct CompletedFrameSample {
    presented_at_ns: u64,
    total_latency_ns: u64,
    backend_latency_ns: u64,
    render_latency_ns: u64,
    present_latency_ns: u64,
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct MoviePerfSummary {
    pub requested_fps: f64,
    pub achieved_fps: f64,
    pub p50_frame_latency_ms: f64,
    pub p95_frame_latency_ms: f64,
    pub cache_hit_rate: f64,
    pub dropped_frames: u64,
    pub stale_frames: u64,
    pub backend_avg_ms: f64,
    pub render_avg_ms: f64,
    pub present_avg_ms: f64,
    pub recent_frame_count: usize,
    pub pipeline: Option<MoviePipelineState>,
}

#[derive(Debug, Default)]
pub(crate) struct MoviePerfTracer {
    enabled: bool,
    started_at: Option<Instant>,
    #[cfg_attr(not(test), allow(dead_code))]
    json_path: Option<PathBuf>,
    #[cfg_attr(not(test), allow(dead_code))]
    log_path: Option<PathBuf>,
    json_file: Option<File>,
    log_file: Option<File>,
    next_frame_seq: u64,
    active_frames: HashMap<u64, MovieFrameTrace>,
    present_waiting_frames: Vec<u64>,
    recent_frames: VecDeque<CompletedFrameSample>,
    last_summary_at: Option<Instant>,
    total_rendered_cache_hits: u64,
    total_backend_cache_hits: u64,
    total_cache_misses: u64,
    total_dropped_frames: u64,
    total_stale_frames: u64,
    total_skipped_pending: u64,
}

impl MoviePerfTracer {
    pub(crate) fn from_env() -> Self {
        let enabled = std::env::var_os(PERF_ENV).is_some();
        if !enabled {
            return Self::default();
        }
        install_perf_signal_handler();
        let started_at = Instant::now();
        let output_dir = std::env::var_os(PERF_DIR_ENV)
            .map(PathBuf::from)
            .unwrap_or_else(|| PathBuf::from("/tmp"));
        let _ = create_dir_all(&output_dir);
        let pid = std::process::id();
        let json_path = output_dir.join(format!("casars-imexplore-perf-{pid}.jsonl"));
        let log_path = output_dir.join(format!("casars-imexplore-perf-{pid}.log"));
        let json_file = open_append_file(&json_path);
        let log_file = open_append_file(&log_path);
        Self {
            enabled,
            started_at: Some(started_at),
            json_path: Some(json_path),
            log_path: Some(log_path),
            json_file,
            log_file,
            ..Self::default()
        }
    }

    #[cfg_attr(not(test), allow(dead_code))]
    pub(crate) fn json_path(&self) -> Option<&Path> {
        self.json_path.as_deref()
    }

    #[cfg_attr(not(test), allow(dead_code))]
    pub(crate) fn log_path(&self) -> Option<&Path> {
        self.log_path.as_deref()
    }

    pub(crate) fn begin_frame(&mut self, context: MoviePerfContext) -> Option<u64> {
        if !self.enabled {
            return None;
        }
        self.next_frame_seq = self.next_frame_seq.saturating_add(1);
        let frame_seq = self.next_frame_seq;
        self.active_frames.insert(
            frame_seq,
            MovieFrameTrace {
                frame_seq,
                context,
                frame_requested_at: Instant::now(),
                browser_command_sent_at: None,
                browser_snapshot_received_at: None,
                plane_render_requested_at: None,
                plane_render_completed_at: None,
                plane_presented_at: None,
                outcome: None,
                backend: None,
            },
        );
        self.write_event(MoviePerfEvent {
            kind: MoviePerfEventKind::FrameRequested,
            frame_seq: Some(frame_seq),
            duration_ns: None,
            queue_depth: None,
            panel_pending: None,
            outcome: None,
            backend: None,
            pipeline: None,
            note: None,
            ..self.base_event(context, MoviePerfEventKind::FrameRequested, Some(frame_seq))
        });
        Some(frame_seq)
    }

    pub(crate) fn movie_started(&mut self, context: MoviePerfContext) {
        if !self.enabled {
            return;
        }
        self.write_event(MoviePerfEvent {
            kind: MoviePerfEventKind::MovieStarted,
            frame_seq: None,
            duration_ns: None,
            queue_depth: None,
            panel_pending: None,
            outcome: None,
            backend: None,
            pipeline: None,
            note: None,
            ..self.base_event(context, MoviePerfEventKind::MovieStarted, None)
        });
    }

    pub(crate) fn movie_stopped(&mut self, context: MoviePerfContext, note: impl Into<String>) {
        if !self.enabled {
            return;
        }
        self.write_event(MoviePerfEvent {
            kind: MoviePerfEventKind::MovieStopped,
            frame_seq: None,
            duration_ns: None,
            queue_depth: None,
            panel_pending: None,
            outcome: None,
            backend: None,
            pipeline: None,
            note: Some(note.into()),
            ..self.base_event(context, MoviePerfEventKind::MovieStopped, None)
        });
    }

    pub(crate) fn fps_changed(&mut self, context: MoviePerfContext) {
        if !self.enabled {
            return;
        }
        self.write_event(MoviePerfEvent {
            kind: MoviePerfEventKind::FpsChanged,
            frame_seq: None,
            duration_ns: None,
            queue_depth: None,
            panel_pending: None,
            outcome: None,
            backend: None,
            pipeline: None,
            note: None,
            ..self.base_event(context, MoviePerfEventKind::FpsChanged, None)
        });
    }

    pub(crate) fn direct_overlay_changed(&mut self, context: MoviePerfContext, active: bool) {
        if !self.enabled {
            return;
        }
        self.write_event(MoviePerfEvent {
            kind: MoviePerfEventKind::DirectOverlayChanged,
            frame_seq: None,
            duration_ns: None,
            queue_depth: None,
            panel_pending: None,
            outcome: None,
            backend: None,
            pipeline: None,
            note: Some(if active {
                "direct_overlay=active".to_string()
            } else {
                "direct_overlay=inactive".to_string()
            }),
            ..self.base_event(context, MoviePerfEventKind::DirectOverlayChanged, None)
        });
    }

    pub(crate) fn preview_requested(
        &mut self,
        frame_seq: u64,
        context: MoviePerfContext,
        queue_depth: usize,
        pipeline: Option<MoviePipelineState>,
    ) {
        if !self.enabled {
            return;
        }
        let monotonic_ns = self.monotonic_ns();
        let event = {
            let Some(trace) = self.active_frames.get_mut(&frame_seq) else {
                return;
            };
            trace.context = context;
            trace.browser_command_sent_at = Some(Instant::now());
            frame_event_from_trace(
                monotonic_ns,
                trace,
                MoviePerfEventKind::PreviewRequested,
                None,
                Some(queue_depth),
                Some(false),
                pipeline,
            )
        };
        self.write_event(event);
    }

    pub(crate) fn preview_received(
        &mut self,
        frame_seq: u64,
        context: MoviePerfContext,
        backend: Option<BackendTimingBreakdown>,
        pipeline: Option<MoviePipelineState>,
    ) {
        if !self.enabled {
            return;
        }
        let monotonic_ns = self.monotonic_ns();
        let event = {
            let Some(trace) = self.active_frames.get_mut(&frame_seq) else {
                return;
            };
            trace.context = context;
            trace.browser_snapshot_received_at = Some(Instant::now());
            trace.backend = backend;
            let duration_ns = trace
                .browser_command_sent_at
                .map(|started| duration_ns(started.elapsed()));
            frame_event_from_trace(
                monotonic_ns,
                trace,
                MoviePerfEventKind::PreviewReceived,
                duration_ns,
                None,
                None,
                pipeline,
            )
        };
        self.write_event(event);
    }

    pub(crate) fn bundle_render_requested(
        &mut self,
        frame_seq: u64,
        render_request_key_hash: u64,
        context: MoviePerfContext,
        queue_depth: usize,
        outcome: MovieFrameOutcome,
        pipeline: Option<MoviePipelineState>,
    ) {
        if !self.enabled {
            return;
        }
        self.update_outcome_counters(outcome);
        let monotonic_ns = self.monotonic_ns();
        let event = {
            let Some(trace) = self.active_frames.get_mut(&frame_seq) else {
                return;
            };
            trace.context = context;
            trace.context.render_request_key_hash = Some(render_request_key_hash);
            trace.plane_render_requested_at = Some(Instant::now());
            trace.outcome = Some(outcome);
            frame_event_from_trace(
                monotonic_ns,
                trace,
                MoviePerfEventKind::BundleRenderRequested,
                trace
                    .browser_snapshot_received_at
                    .map(|received| duration_ns(received.elapsed())),
                Some(queue_depth),
                Some(false),
                pipeline,
            )
        };
        self.write_event(event);
    }

    pub(crate) fn bundle_ready(
        &mut self,
        render_request_key_hash: u64,
        queue_depth: usize,
        pipeline: Option<MoviePipelineState>,
    ) {
        if !self.enabled {
            return;
        }
        let monotonic_ns = self.monotonic_ns();
        let event = {
            let Some(frame_seq) = self
                .active_frames
                .iter()
                .filter(|(_, trace)| {
                    trace.context.render_request_key_hash == Some(render_request_key_hash)
                })
                .map(|(frame_seq, _)| *frame_seq)
                .max()
            else {
                return;
            };
            let Some(trace) = self.active_frames.get_mut(&frame_seq) else {
                return;
            };
            trace.plane_render_completed_at = Some(Instant::now());
            self.present_waiting_frames.push(trace.frame_seq);
            let duration_ns = trace
                .plane_render_requested_at
                .map(|started| duration_ns(started.elapsed()));
            frame_event_from_trace(
                monotonic_ns,
                trace,
                MoviePerfEventKind::BundleReady,
                duration_ns,
                Some(queue_depth),
                Some(false),
                pipeline,
            )
        };
        self.write_event(event);
    }

    pub(crate) fn bundle_presented(
        &mut self,
        render_request_key_hash: u64,
        pipeline: Option<MoviePipelineState>,
    ) {
        self.finish_presented_frame(
            render_request_key_hash,
            MoviePerfEventKind::BundlePresented,
            pipeline,
        );
    }

    pub(crate) fn generation_invalidated(
        &mut self,
        context: MoviePerfContext,
        note: impl Into<String>,
        pipeline: Option<MoviePipelineState>,
    ) {
        if !self.enabled {
            return;
        }
        self.write_event(MoviePerfEvent {
            kind: MoviePerfEventKind::GenerationInvalidated,
            frame_seq: None,
            duration_ns: None,
            queue_depth: None,
            panel_pending: None,
            outcome: None,
            backend: None,
            pipeline,
            note: Some(note.into()),
            ..self.base_event(context, MoviePerfEventKind::GenerationInvalidated, None)
        });
    }

    pub(crate) fn deadline_missed(
        &mut self,
        context: MoviePerfContext,
        note: impl Into<String>,
        queue_depth: usize,
        pipeline: Option<MoviePipelineState>,
    ) {
        if !self.enabled {
            return;
        }
        self.write_event(MoviePerfEvent {
            kind: MoviePerfEventKind::DeadlineMissed,
            frame_seq: None,
            duration_ns: None,
            queue_depth: Some(queue_depth),
            panel_pending: Some(false),
            outcome: Some(MovieFrameOutcome::SkippedDueToPending),
            backend: None,
            pipeline,
            note: Some(note.into()),
            ..self.base_event(context, MoviePerfEventKind::DeadlineMissed, None)
        });
    }

    pub(crate) fn browser_command_sent(&mut self, frame_seq: u64) {
        if !self.enabled {
            return;
        }
        let monotonic_ns = self.monotonic_ns();
        let event = {
            let Some(trace) = self.active_frames.get_mut(&frame_seq) else {
                return;
            };
            trace.browser_command_sent_at = Some(Instant::now());
            frame_event_from_trace(
                monotonic_ns,
                trace,
                MoviePerfEventKind::BrowserCommandSent,
                None,
                None,
                None,
                None,
            )
        };
        self.write_event(event);
    }

    pub(crate) fn browser_snapshot_received(
        &mut self,
        frame_seq: u64,
        context: MoviePerfContext,
        backend: Option<BackendTimingBreakdown>,
    ) {
        if !self.enabled {
            return;
        }
        let monotonic_ns = self.monotonic_ns();
        let event = {
            let Some(trace) = self.active_frames.get_mut(&frame_seq) else {
                return;
            };
            trace.context = context;
            trace.browser_snapshot_received_at = Some(Instant::now());
            trace.backend = backend;
            let duration_ns = trace
                .browser_command_sent_at
                .map(|started| duration_ns(started.elapsed()));
            frame_event_from_trace(
                monotonic_ns,
                trace,
                MoviePerfEventKind::BrowserSnapshotReceived,
                duration_ns,
                None,
                None,
                None,
            )
        };
        self.write_event(event);
    }

    pub(crate) fn plane_render_requested(
        &mut self,
        frame_seq: u64,
        render_request_key_hash: u64,
        context: MoviePerfContext,
        queue_depth: usize,
        panel_pending: bool,
        outcome: MovieFrameOutcome,
    ) {
        if !self.enabled {
            return;
        }
        self.update_outcome_counters(outcome);
        let monotonic_ns = self.monotonic_ns();
        let event = {
            let Some(trace) = self.active_frames.get_mut(&frame_seq) else {
                return;
            };
            trace.context = context;
            trace.context.render_request_key_hash = Some(render_request_key_hash);
            trace.plane_render_requested_at = Some(Instant::now());
            trace.outcome = Some(outcome);
            frame_event_from_trace(
                monotonic_ns,
                trace,
                MoviePerfEventKind::PlaneRenderRequested,
                trace
                    .browser_snapshot_received_at
                    .map(|received| duration_ns(received.elapsed())),
                Some(queue_depth),
                Some(panel_pending),
                None,
            )
        };
        self.write_event(event);
    }

    pub(crate) fn plane_render_completed(
        &mut self,
        render_request_key_hash: u64,
        queue_depth: usize,
        panel_pending: bool,
    ) {
        if !self.enabled {
            return;
        }
        let monotonic_ns = self.monotonic_ns();
        let event = {
            let Some(frame_seq) = self
                .active_frames
                .iter()
                .filter(|(_, trace)| {
                    trace.context.render_request_key_hash == Some(render_request_key_hash)
                })
                .map(|(frame_seq, _)| *frame_seq)
                .max()
            else {
                return;
            };
            let Some(trace) = self.active_frames.get_mut(&frame_seq) else {
                return;
            };
            trace.plane_render_completed_at = Some(Instant::now());
            self.present_waiting_frames.push(trace.frame_seq);
            let duration_ns = trace
                .plane_render_requested_at
                .map(|started| duration_ns(started.elapsed()));
            frame_event_from_trace(
                monotonic_ns,
                trace,
                MoviePerfEventKind::PlaneRenderCompleted,
                duration_ns,
                Some(queue_depth),
                Some(panel_pending),
                None,
            )
        };
        self.write_event(event);
    }

    pub(crate) fn plane_presented(&mut self, render_request_key_hash: u64) {
        self.finish_presented_frame(
            render_request_key_hash,
            MoviePerfEventKind::PlanePresented,
            None,
        );
    }

    fn finish_presented_frame(
        &mut self,
        render_request_key_hash: u64,
        kind: MoviePerfEventKind,
        pipeline: Option<MoviePipelineState>,
    ) {
        if !self.enabled {
            return;
        }
        let Some((index, _)) = self
            .present_waiting_frames
            .iter()
            .enumerate()
            .filter(|(_, frame_seq)| {
                self.active_frames
                    .get(frame_seq)
                    .and_then(|trace| trace.context.render_request_key_hash)
                    == Some(render_request_key_hash)
            })
            .max_by_key(|(_, frame_seq)| **frame_seq)
        else {
            return;
        };
        let frame_seq = self.present_waiting_frames.remove(index);
        let Some(mut trace) = self.active_frames.remove(&frame_seq) else {
            return;
        };
        let presented_at = Instant::now();
        trace.plane_presented_at = Some(presented_at);
        let total_latency_ns = duration_ns(trace.frame_requested_at.elapsed());
        let backend_latency_ns = trace
            .browser_command_sent_at
            .zip(trace.browser_snapshot_received_at)
            .map(|(sent, received)| duration_ns(received.saturating_duration_since(sent)))
            .unwrap_or_default();
        let render_latency_ns = trace
            .plane_render_requested_at
            .zip(trace.plane_render_completed_at)
            .map(|(requested, completed)| {
                duration_ns(completed.saturating_duration_since(requested))
            })
            .unwrap_or_default();
        let present_latency_ns = trace
            .plane_render_completed_at
            .map(|completed| duration_ns(presented_at.saturating_duration_since(completed)))
            .unwrap_or_default();
        self.push_completed_frame(CompletedFrameSample {
            presented_at_ns: self.monotonic_ns(),
            total_latency_ns,
            backend_latency_ns,
            render_latency_ns,
            present_latency_ns,
        });
        let event = frame_event_from_trace(
            self.monotonic_ns(),
            &trace,
            kind,
            Some(total_latency_ns),
            Some(0),
            Some(false),
            pipeline,
        );
        self.write_event(event);
    }

    pub(crate) fn frame_dropped(
        &mut self,
        frame_seq: Option<u64>,
        context: MoviePerfContext,
        outcome: MovieFrameOutcome,
        note: impl Into<String>,
    ) {
        if !self.enabled {
            return;
        }
        match outcome {
            MovieFrameOutcome::StaleRenderDiscarded => {
                self.total_stale_frames = self.total_stale_frames.saturating_add(1);
            }
            MovieFrameOutcome::SkippedDueToPending => {
                self.total_skipped_pending = self.total_skipped_pending.saturating_add(1);
            }
            _ => {}
        }
        self.total_dropped_frames = self.total_dropped_frames.saturating_add(1);
        self.write_event(MoviePerfEvent {
            kind: MoviePerfEventKind::FrameDropped,
            frame_seq,
            duration_ns: None,
            queue_depth: None,
            panel_pending: None,
            outcome: Some(outcome),
            backend: None,
            pipeline: None,
            note: Some(note.into()),
            ..self.base_event(context, MoviePerfEventKind::FrameDropped, frame_seq)
        });
    }

    pub(crate) fn maybe_emit_summary(
        &mut self,
        movie_active: bool,
        requested_fps: f64,
        pipeline: Option<MoviePipelineState>,
    ) {
        if !self.enabled {
            return;
        }
        let force = FORCE_SUMMARY_FLUSH.swap(false, Ordering::SeqCst);
        if !force && !movie_active {
            return;
        }
        let now = Instant::now();
        let due = self
            .last_summary_at
            .is_none_or(|last| now.saturating_duration_since(last) >= SUMMARY_INTERVAL);
        if !force && !due {
            return;
        }
        self.last_summary_at = Some(now);
        let summary = self.summary(requested_fps, pipeline);
        let context = MoviePerfContext {
            requested_fps_milli: Some((requested_fps * 1000.0).round() as u64),
            ..MoviePerfContext::default()
        };
        self.write_event(MoviePerfEvent {
            kind: MoviePerfEventKind::Summary,
            frame_seq: None,
            duration_ns: None,
            queue_depth: None,
            panel_pending: None,
            outcome: None,
            backend: None,
            pipeline: summary.pipeline,
            note: Some(format!(
                "achieved_fps={:.2} requested_fps={:.2} p50_ms={:.2} p95_ms={:.2} cache_hit_rate={:.2} dropped={} stale={} backend_avg_ms={:.2} render_avg_ms={:.2} present_avg_ms={:.2} recent_frames={} render_q={} render_active={} protocol_q={} protocol_active={} ready_bundles={} ready_presentations={} bitmap_cache_mb={:.2}",
                summary.achieved_fps,
                summary.requested_fps,
                summary.p50_frame_latency_ms,
                summary.p95_frame_latency_ms,
                summary.cache_hit_rate,
                summary.dropped_frames,
                summary.stale_frames,
                summary.backend_avg_ms,
                summary.render_avg_ms,
                summary.present_avg_ms,
                summary.recent_frame_count,
                summary.pipeline.map(|stats| stats.render_queue_depth).unwrap_or_default(),
                summary.pipeline.map(|stats| stats.render_active_jobs).unwrap_or_default(),
                summary.pipeline.map(|stats| stats.protocol_queue_depth).unwrap_or_default(),
                summary.pipeline.map(|stats| stats.protocol_active_jobs).unwrap_or_default(),
                summary.pipeline.map(|stats| stats.ready_bundle_count).unwrap_or_default(),
                summary.pipeline.map(|stats| stats.ready_presentation_count).unwrap_or_default(),
                summary
                    .pipeline
                    .map(|stats| stats.bitmap_cache_bytes as f64 / (1024.0 * 1024.0))
                    .unwrap_or_default()
            )),
            ..self.base_event(context, MoviePerfEventKind::Summary, None)
        });
        if let Some(file) = self.log_file.as_mut() {
            let _ = writeln!(
                file,
                "[+{:>7} ms] summary achieved_fps={:.2} requested_fps={:.2} p50_ms={:.2} p95_ms={:.2} cache_hit_rate={:.2} dropped={} stale={} backend_avg_ms={:.2} render_avg_ms={:.2} present_avg_ms={:.2} recent_frames={} render_q={} render_active={} protocol_q={} protocol_active={} ready_bundles={} ready_presentations={} bitmap_cache_mb={:.2}",
                now.saturating_duration_since(self.started_at.unwrap_or(now))
                    .as_millis(),
                summary.achieved_fps,
                summary.requested_fps,
                summary.p50_frame_latency_ms,
                summary.p95_frame_latency_ms,
                summary.cache_hit_rate,
                summary.dropped_frames,
                summary.stale_frames,
                summary.backend_avg_ms,
                summary.render_avg_ms,
                summary.present_avg_ms,
                summary.recent_frame_count,
                summary
                    .pipeline
                    .map(|stats| stats.render_queue_depth)
                    .unwrap_or_default(),
                summary
                    .pipeline
                    .map(|stats| stats.render_active_jobs)
                    .unwrap_or_default(),
                summary
                    .pipeline
                    .map(|stats| stats.protocol_queue_depth)
                    .unwrap_or_default(),
                summary
                    .pipeline
                    .map(|stats| stats.protocol_active_jobs)
                    .unwrap_or_default(),
                summary
                    .pipeline
                    .map(|stats| stats.ready_bundle_count)
                    .unwrap_or_default(),
                summary
                    .pipeline
                    .map(|stats| stats.ready_presentation_count)
                    .unwrap_or_default(),
                summary
                    .pipeline
                    .map(|stats| stats.bitmap_cache_bytes as f64 / (1024.0 * 1024.0))
                    .unwrap_or_default()
            );
            let _ = file.flush();
        }
    }

    pub(crate) fn summary(
        &self,
        requested_fps: f64,
        pipeline: Option<MoviePipelineState>,
    ) -> MoviePerfSummary {
        let now_ns = self.monotonic_ns();
        let window_start_ns = now_ns.saturating_sub(duration_ns(SUMMARY_INTERVAL));
        let recent: Vec<_> = self
            .recent_frames
            .iter()
            .copied()
            .filter(|frame| frame.presented_at_ns >= window_start_ns)
            .collect();
        let achieved_fps = if recent.is_empty() {
            0.0
        } else {
            recent.len() as f64 / SUMMARY_INTERVAL.as_secs_f64()
        };
        let mut latencies: Vec<u64> = recent.iter().map(|frame| frame.total_latency_ns).collect();
        latencies.sort_unstable();
        let p50 = percentile_u64(&latencies, 0.50) as f64 / 1_000_000.0;
        let p95 = percentile_u64(&latencies, 0.95) as f64 / 1_000_000.0;
        let cache_hits = self.total_rendered_cache_hits + self.total_backend_cache_hits;
        let classified_frames = cache_hits + self.total_cache_misses;
        let cache_hit_rate = if classified_frames == 0 {
            0.0
        } else {
            cache_hits as f64 / classified_frames as f64
        };
        let backend_avg_ms = average_ns(recent.iter().map(|frame| frame.backend_latency_ns));
        let render_avg_ms = average_ns(recent.iter().map(|frame| frame.render_latency_ns));
        let present_avg_ms = average_ns(recent.iter().map(|frame| frame.present_latency_ns));
        MoviePerfSummary {
            requested_fps,
            achieved_fps,
            p50_frame_latency_ms: p50,
            p95_frame_latency_ms: p95,
            cache_hit_rate,
            dropped_frames: self.total_dropped_frames,
            stale_frames: self.total_stale_frames,
            backend_avg_ms,
            render_avg_ms,
            present_avg_ms,
            recent_frame_count: recent.len(),
            pipeline,
        }
    }

    fn push_completed_frame(&mut self, frame: CompletedFrameSample) {
        self.recent_frames.push_back(frame);
        while self.recent_frames.len() > RECENT_FRAME_CAPACITY {
            self.recent_frames.pop_front();
        }
    }

    fn update_outcome_counters(&mut self, outcome: MovieFrameOutcome) {
        match outcome {
            MovieFrameOutcome::CacheHitRenderedImage => {
                self.total_rendered_cache_hits = self.total_rendered_cache_hits.saturating_add(1);
            }
            MovieFrameOutcome::CacheHitBackendPlane => {
                self.total_backend_cache_hits = self.total_backend_cache_hits.saturating_add(1);
            }
            MovieFrameOutcome::CacheMiss => {
                self.total_cache_misses = self.total_cache_misses.saturating_add(1);
            }
            MovieFrameOutcome::StaleRenderDiscarded => {
                self.total_stale_frames = self.total_stale_frames.saturating_add(1);
                self.total_dropped_frames = self.total_dropped_frames.saturating_add(1);
            }
            MovieFrameOutcome::SkippedDueToPending => {
                self.total_skipped_pending = self.total_skipped_pending.saturating_add(1);
                self.total_dropped_frames = self.total_dropped_frames.saturating_add(1);
            }
        }
    }

    fn write_event(&mut self, event: MoviePerfEvent) {
        if !self.enabled {
            return;
        }
        if let Some(file) = self.json_file.as_mut() {
            let _ = serde_json::to_writer(&mut *file, &event);
            let _ = writeln!(file);
            let _ = file.flush();
        }
    }

    fn base_event(
        &self,
        context: MoviePerfContext,
        kind: MoviePerfEventKind,
        frame_seq: Option<u64>,
    ) -> MoviePerfEvent {
        MoviePerfEvent {
            kind,
            monotonic_ns: self.monotonic_ns(),
            frame_seq,
            axis: context.axis,
            axis_index: context.axis_index,
            axis_length: context.axis_length,
            render_request_key_hash: context.render_request_key_hash,
            canvas_cell_width: context.canvas_cell_size.map(|(w, _)| w),
            canvas_cell_height: context.canvas_cell_size.map(|(_, h)| h),
            canvas_pixel_width: context.canvas_pixel_size.map(|(w, _)| w),
            canvas_pixel_height: context.canvas_pixel_size.map(|(_, h)| h),
            plane_mode: if context.raster_mode {
                "raster"
            } else {
                "spreadsheet"
            },
            direct_overlay: context.direct_overlay,
            terminal_looping: context.terminal_looping,
            requested_fps_milli: context.requested_fps_milli,
            duration_ns: None,
            queue_depth: None,
            panel_pending: None,
            outcome: None,
            backend: None,
            pipeline: None,
            note: None,
        }
    }

    fn monotonic_ns(&self) -> u64 {
        self.started_at
            .map(|started| duration_ns(started.elapsed()))
            .unwrap_or_default()
    }
}

fn frame_event_from_trace(
    monotonic_ns: u64,
    trace: &MovieFrameTrace,
    kind: MoviePerfEventKind,
    duration_ns: Option<u64>,
    queue_depth: Option<usize>,
    panel_pending: Option<bool>,
    pipeline: Option<MoviePipelineState>,
) -> MoviePerfEvent {
    MoviePerfEvent {
        kind,
        monotonic_ns,
        frame_seq: Some(trace.frame_seq),
        axis: trace.context.axis,
        axis_index: trace.context.axis_index,
        axis_length: trace.context.axis_length,
        render_request_key_hash: trace.context.render_request_key_hash,
        canvas_cell_width: trace.context.canvas_cell_size.map(|(w, _)| w),
        canvas_cell_height: trace.context.canvas_cell_size.map(|(_, h)| h),
        canvas_pixel_width: trace.context.canvas_pixel_size.map(|(w, _)| w),
        canvas_pixel_height: trace.context.canvas_pixel_size.map(|(_, h)| h),
        plane_mode: if trace.context.raster_mode {
            "raster"
        } else {
            "spreadsheet"
        },
        direct_overlay: trace.context.direct_overlay,
        terminal_looping: trace.context.terminal_looping,
        requested_fps_milli: trace.context.requested_fps_milli,
        duration_ns,
        queue_depth,
        panel_pending,
        outcome: trace.outcome,
        backend: trace.backend,
        pipeline,
        note: None,
    }
}

fn open_append_file(path: &Path) -> Option<File> {
    OpenOptions::new().create(true).append(true).open(path).ok()
}

fn duration_ns(duration: Duration) -> u64 {
    duration.as_nanos().min(u128::from(u64::MAX)) as u64
}

fn percentile_u64(values: &[u64], percentile: f64) -> u64 {
    if values.is_empty() {
        return 0;
    }
    let index =
        ((values.len().saturating_sub(1)) as f64 * percentile.clamp(0.0, 1.0)).round() as usize;
    values[index.min(values.len().saturating_sub(1))]
}

fn average_ns(values: impl Iterator<Item = u64>) -> f64 {
    let mut total = 0u128;
    let mut count = 0u64;
    for value in values {
        total = total.saturating_add(u128::from(value));
        count = count.saturating_add(1);
    }
    if count == 0 {
        0.0
    } else {
        total as f64 / count as f64 / 1_000_000.0
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use std::sync::atomic::Ordering;
    use tempfile::tempdir;

    fn sample_context() -> MoviePerfContext {
        MoviePerfContext {
            axis: Some(1),
            axis_index: Some(2),
            axis_length: Some(8),
            render_request_key_hash: Some(99),
            canvas_cell_size: Some((80, 24)),
            canvas_pixel_size: Some((1600, 720)),
            raster_mode: true,
            direct_overlay: true,
            terminal_looping: false,
            requested_fps_milli: Some(12_500),
        }
    }

    fn sample_backend() -> BackendTimingBreakdown {
        BackendTimingBreakdown {
            cached_plane_lookup_ns: 1,
            plane_extract_ns: 2,
            stat_collection_ns: 3,
            histogram_ns: 4,
            rasterize_ns: 5,
            total_plane_ns: 6,
            profile_cache_hits: 7,
            profile_cache_misses: 8,
            profile_extract_total_ns: 9,
        }
    }

    fn sample_pipeline() -> MoviePipelineState {
        MoviePipelineState {
            render_queue_depth: 2,
            render_active_jobs: 1,
            protocol_queue_depth: 3,
            protocol_active_jobs: 2,
            ready_bundle_count: 4,
            ready_presentation_count: 1,
            bitmap_cache_bytes: 1024,
        }
    }

    #[test]
    fn movie_perf_event_serializes_to_jsonl_shape() {
        let event = MoviePerfEvent {
            kind: MoviePerfEventKind::FrameRequested,
            monotonic_ns: 42,
            frame_seq: Some(7),
            axis: Some(3),
            axis_index: Some(10),
            axis_length: Some(30),
            render_request_key_hash: Some(99),
            canvas_cell_width: Some(80),
            canvas_cell_height: Some(24),
            canvas_pixel_width: Some(1600),
            canvas_pixel_height: Some(720),
            plane_mode: "raster",
            direct_overlay: false,
            terminal_looping: false,
            requested_fps_milli: Some(30_000),
            duration_ns: Some(1_000),
            queue_depth: Some(1),
            panel_pending: Some(true),
            outcome: Some(MovieFrameOutcome::CacheMiss),
            backend: Some(sample_backend()),
            pipeline: Some(sample_pipeline()),
            note: Some("hello".into()),
        };
        let encoded = serde_json::to_string(&event).expect("encode");
        assert!(encoded.contains("\"frame_seq\":7"));
        assert!(encoded.contains("\"kind\":\"frame_requested\""));
        assert!(encoded.contains("\"plane_mode\":\"raster\""));
        assert!(encoded.contains("\"render_active_jobs\":1"));
    }

    #[test]
    fn summary_aggregates_recent_frame_metrics() {
        let mut tracer = MoviePerfTracer {
            enabled: true,
            started_at: Some(Instant::now() - Duration::from_secs(3)),
            ..MoviePerfTracer::default()
        };
        tracer.total_rendered_cache_hits = 3;
        tracer.total_backend_cache_hits = 1;
        tracer.total_cache_misses = 1;
        tracer.total_dropped_frames = 2;
        tracer.total_stale_frames = 1;
        tracer.recent_frames.push_back(CompletedFrameSample {
            presented_at_ns: tracer.monotonic_ns().saturating_sub(1_000),
            total_latency_ns: 10_000_000,
            backend_latency_ns: 4_000_000,
            render_latency_ns: 3_000_000,
            present_latency_ns: 1_000_000,
        });
        tracer.recent_frames.push_back(CompletedFrameSample {
            presented_at_ns: tracer.monotonic_ns().saturating_sub(2_000),
            total_latency_ns: 20_000_000,
            backend_latency_ns: 8_000_000,
            render_latency_ns: 6_000_000,
            present_latency_ns: 2_000_000,
        });
        let summary = tracer.summary(30.0, Some(sample_pipeline()));
        assert!(summary.achieved_fps > 0.0);
        assert_eq!(summary.dropped_frames, 2);
        assert_eq!(summary.stale_frames, 1);
        assert!(summary.cache_hit_rate > 0.0);
        assert!(summary.p95_frame_latency_ms >= summary.p50_frame_latency_ms);
        assert_eq!(summary.pipeline.unwrap().render_active_jobs, 1);
    }

    #[test]
    fn tracer_from_env_writes_jsonl_and_summary_log() {
        let _guard = crate::test_env_lock();
        let temp = tempdir().expect("tempdir");
        unsafe {
            std::env::set_var(PERF_ENV, "1");
            std::env::set_var(PERF_DIR_ENV, temp.path());
        }
        FORCE_SUMMARY_FLUSH.store(false, Ordering::SeqCst);

        let mut tracer = MoviePerfTracer::from_env();
        assert!(tracer.enabled);
        let json_path = tracer.json_path().expect("json path").to_path_buf();
        let log_path = tracer.log_path().expect("log path").to_path_buf();
        let context = sample_context();

        tracer.movie_started(context);
        tracer.fps_changed(context);
        tracer.direct_overlay_changed(context, true);
        tracer.generation_invalidated(context, "stale generation", Some(sample_pipeline()));
        tracer.deadline_missed(context, "pending frame", 3, Some(sample_pipeline()));
        FORCE_SUMMARY_FLUSH.store(true, Ordering::SeqCst);
        tracer.maybe_emit_summary(false, 12.5, Some(sample_pipeline()));
        tracer.movie_stopped(context, "done");
        drop(tracer);

        unsafe {
            std::env::remove_var(PERF_ENV);
            std::env::remove_var(PERF_DIR_ENV);
        }

        let json = fs::read_to_string(json_path).expect("json trace");
        assert!(json.contains("\"kind\":\"movie_started\""));
        assert!(json.contains("\"kind\":\"fps_changed\""));
        assert!(json.contains("\"kind\":\"direct_overlay_changed\""));
        assert!(json.contains("\"kind\":\"generation_invalidated\""));
        assert!(json.contains("\"kind\":\"deadline_missed\""));
        assert!(json.contains("\"kind\":\"summary\""));
        assert!(json.contains("\"kind\":\"movie_stopped\""));
        assert!(json.contains("stale generation"));
        assert!(json.contains("pending frame"));

        let log = fs::read_to_string(log_path).expect("summary log");
        assert!(log.contains("summary achieved_fps"));
        assert!(log.contains("render_q=2"));
    }

    #[test]
    fn tracer_records_bundle_and_plane_presentations() {
        let mut tracer = MoviePerfTracer {
            enabled: true,
            started_at: Some(Instant::now() - Duration::from_secs(1)),
            ..MoviePerfTracer::default()
        };
        let context = sample_context();
        let backend = sample_backend();
        let pipeline = sample_pipeline();

        let frame_seq = tracer.begin_frame(context).expect("frame seq");
        tracer.preview_requested(frame_seq, context, 2, Some(pipeline));
        tracer.preview_received(frame_seq, context, Some(backend), Some(pipeline));
        tracer.bundle_render_requested(
            frame_seq,
            0xabc,
            context,
            1,
            MovieFrameOutcome::CacheHitBackendPlane,
            Some(pipeline),
        );
        tracer.bundle_ready(0xabc, 0, Some(pipeline));
        tracer.bundle_presented(0xabc, Some(pipeline));

        let frame_seq = tracer.begin_frame(context).expect("second frame seq");
        tracer.browser_command_sent(frame_seq);
        tracer.browser_snapshot_received(frame_seq, context, Some(backend));
        tracer.plane_render_requested(
            frame_seq,
            0xdef,
            context,
            2,
            true,
            MovieFrameOutcome::CacheHitRenderedImage,
        );
        tracer.plane_render_completed(0xdef, 0, false);
        tracer.plane_presented(0xdef);

        assert!(tracer.active_frames.is_empty());
        assert!(tracer.present_waiting_frames.is_empty());
        assert_eq!(tracer.total_backend_cache_hits, 1);
        assert_eq!(tracer.total_rendered_cache_hits, 1);
        assert_eq!(tracer.recent_frames.len(), 2);

        let summary = tracer.summary(12.5, Some(pipeline));
        assert_eq!(summary.recent_frame_count, 2);
        assert!(summary.achieved_fps > 0.0);
        assert!(summary.backend_avg_ms >= 0.0);
        assert!(summary.render_avg_ms >= 0.0);
        assert!(summary.present_avg_ms >= 0.0);
    }

    #[test]
    fn tracer_accounts_for_drops_helpers_and_noop_paths() {
        let mut tracer = MoviePerfTracer {
            enabled: true,
            started_at: Some(Instant::now() - Duration::from_secs(6)),
            ..MoviePerfTracer::default()
        };
        let context = sample_context();

        let frame_seq = tracer.begin_frame(context).expect("frame seq");
        tracer.preview_requested(frame_seq, context, 0, None);
        tracer.preview_received(frame_seq, context, None, None);
        tracer.plane_render_requested(
            frame_seq,
            0x123,
            context,
            0,
            false,
            MovieFrameOutcome::CacheMiss,
        );
        tracer.frame_dropped(
            Some(frame_seq),
            context,
            MovieFrameOutcome::StaleRenderDiscarded,
            "stale",
        );
        tracer.frame_dropped(
            None,
            context,
            MovieFrameOutcome::SkippedDueToPending,
            "pending",
        );

        tracer.bundle_ready(0x999, 0, None);
        tracer.bundle_presented(0x999, None);
        tracer.plane_render_completed(0x999, 0, false);
        tracer.plane_presented(0x999);

        tracer.push_completed_frame(CompletedFrameSample {
            presented_at_ns: tracer
                .monotonic_ns()
                .saturating_sub(duration_ns(SUMMARY_INTERVAL)),
            total_latency_ns: 5_000_000,
            backend_latency_ns: 2_000_000,
            render_latency_ns: 2_000_000,
            present_latency_ns: 1_000_000,
        });
        tracer.push_completed_frame(CompletedFrameSample {
            presented_at_ns: tracer.monotonic_ns(),
            total_latency_ns: 9_000_000,
            backend_latency_ns: 4_000_000,
            render_latency_ns: 3_000_000,
            present_latency_ns: 2_000_000,
        });

        assert_eq!(tracer.total_cache_misses, 1);
        assert_eq!(tracer.total_dropped_frames, 2);
        assert_eq!(tracer.total_stale_frames, 1);
        assert_eq!(tracer.total_skipped_pending, 1);
        assert_eq!(percentile_u64(&[], 0.5), 0);
        assert_eq!(percentile_u64(&[1, 5, 9], 2.0), 9);
        assert_eq!(average_ns(std::iter::empty()), 0.0);
        assert!(average_ns([1_000_000, 3_000_000].into_iter()) > 0.0);
        assert_eq!(duration_ns(Duration::from_secs(u64::MAX)), u64::MAX);

        let temp = tempdir().expect("tempdir");
        assert!(open_append_file(&temp.path().join("trace.jsonl")).is_some());

        let mut disabled = MoviePerfTracer::default();
        assert_eq!(disabled.begin_frame(context), None);

        let mut trace = MovieFrameTrace {
            frame_seq: 7,
            context,
            frame_requested_at: Instant::now(),
            browser_command_sent_at: None,
            browser_snapshot_received_at: None,
            plane_render_requested_at: None,
            plane_render_completed_at: None,
            plane_presented_at: None,
            outcome: Some(MovieFrameOutcome::CacheMiss),
            backend: Some(sample_backend()),
        };
        let event = frame_event_from_trace(
            11,
            &trace,
            MoviePerfEventKind::BrowserSnapshotReceived,
            Some(22),
            Some(3),
            Some(false),
            Some(sample_pipeline()),
        );
        assert_eq!(event.monotonic_ns, 11);
        assert_eq!(event.frame_seq, Some(7));
        assert_eq!(event.duration_ns, Some(22));
        assert_eq!(event.queue_depth, Some(3));
        assert_eq!(event.outcome, Some(MovieFrameOutcome::CacheMiss));
        trace.context.raster_mode = false;
        let base = tracer.base_event(trace.context, MoviePerfEventKind::Summary, None);
        assert_eq!(base.plane_mode, "spreadsheet");
    }
}

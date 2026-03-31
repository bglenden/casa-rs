// SPDX-License-Identifier: LGPL-3.0-or-later
//! Support utilities for staged `imexplore` movie performance harnesses.

use std::collections::{BTreeMap, btree_map::Entry};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use casacore_imagebrowser_protocol::{ImageBrowserSnapshot, ImageDisplayAxisState};
use image::DynamicImage;

use crate::config::ThemeMode;
use crate::graphics::{ImagePlaneColormap, ImagePlaneRenderInput, render_image_plane_image};

/// Theme variant used by staged movie harnesses.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum PlanePaneTheme {
    DenseAnsi,
    RichPanel,
}

/// Colormap variant used by staged movie harnesses.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum PlanePaneColormap {
    Grayscale,
    Viridis,
    Inferno,
}

/// Render options for the composed plane pane (`plane + wedge + histogram`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct PlanePaneRenderOptions {
    pub theme: PlanePaneTheme,
    pub colormap: PlanePaneColormap,
    pub invert: bool,
    pub show_live_reticle: bool,
}

impl Default for PlanePaneRenderOptions {
    fn default() -> Self {
        Self {
            theme: PlanePaneTheme::DenseAnsi,
            colormap: PlanePaneColormap::Grayscale,
            invert: false,
            show_live_reticle: true,
        }
    }
}

/// Result of inserting a frame into an ordered ready buffer.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReadyInsertResult {
    Inserted,
    Stale,
    RejectedAtCapacity,
    ReplacedExisting,
}

/// Ordered ready-frame buffer with stale-result filtering.
#[derive(Debug)]
pub struct OrderedReadyBuffer<T> {
    capacity: usize,
    next_sequence: u64,
    ready: BTreeMap<u64, T>,
    max_len: usize,
}

impl<T> OrderedReadyBuffer<T> {
    pub fn new(capacity: usize) -> Self {
        Self {
            capacity: capacity.max(1),
            next_sequence: 0,
            ready: BTreeMap::new(),
            max_len: 0,
        }
    }

    pub fn next_sequence(&self) -> u64 {
        self.next_sequence
    }

    pub fn len(&self) -> usize {
        self.ready.len()
    }

    pub fn is_empty(&self) -> bool {
        self.ready.is_empty()
    }

    pub fn max_len(&self) -> usize {
        self.max_len
    }

    pub fn insert(&mut self, sequence: u64, value: T) -> ReadyInsertResult {
        if sequence < self.next_sequence {
            return ReadyInsertResult::Stale;
        }
        if let Entry::Occupied(mut entry) = self.ready.entry(sequence) {
            entry.insert(value);
            return ReadyInsertResult::ReplacedExisting;
        }
        if self.ready.len() >= self.capacity {
            return ReadyInsertResult::RejectedAtCapacity;
        }
        self.ready.insert(sequence, value);
        self.max_len = self.max_len.max(self.ready.len());
        ReadyInsertResult::Inserted
    }

    pub fn pop_next(&mut self) -> Option<(u64, T)> {
        let sequence = self.next_sequence;
        let value = self.ready.remove(&sequence)?;
        self.next_sequence = self.next_sequence.saturating_add(1);
        Some((sequence, value))
    }

    pub fn clear(&mut self) {
        self.ready.clear();
        self.next_sequence = 0;
        self.max_len = 0;
    }
}

/// Tracks active and peak concurrent worker jobs.
#[derive(Debug, Clone, Default)]
pub struct WorkerActivity {
    active: Arc<AtomicUsize>,
    max_active: Arc<AtomicUsize>,
}

impl WorkerActivity {
    pub fn active(&self) -> usize {
        self.active.load(Ordering::Relaxed)
    }

    pub fn max_active(&self) -> usize {
        self.max_active.load(Ordering::Relaxed)
    }

    pub fn enter(&self) -> WorkerActivityGuard {
        let active = self.active.fetch_add(1, Ordering::SeqCst) + 1;
        self.max_active.fetch_max(active, Ordering::SeqCst);
        WorkerActivityGuard {
            active: Arc::clone(&self.active),
        }
    }
}

/// Guard returned by [`WorkerActivity::enter`].
#[derive(Debug)]
pub struct WorkerActivityGuard {
    active: Arc<AtomicUsize>,
}

impl Drop for WorkerActivityGuard {
    fn drop(&mut self) {
        self.active.fetch_sub(1, Ordering::SeqCst);
    }
}

/// Tracks current and peak queue depth.
#[derive(Debug, Clone, Default)]
pub struct QueueDepthTracker {
    current: Arc<AtomicUsize>,
    max_depth: Arc<AtomicUsize>,
}

impl QueueDepthTracker {
    pub fn current(&self) -> usize {
        self.current.load(Ordering::Relaxed)
    }

    pub fn max_depth(&self) -> usize {
        self.max_depth.load(Ordering::Relaxed)
    }

    pub fn push(&self) {
        let depth = self.current.fetch_add(1, Ordering::SeqCst) + 1;
        self.max_depth.fetch_max(depth, Ordering::SeqCst);
    }

    pub fn pop(&self) {
        self.current.fetch_sub(1, Ordering::SeqCst);
    }
}

/// Render the exact current composed plane pane from an image browser snapshot.
pub fn render_plane_pane_from_snapshot(
    width: u32,
    height: u32,
    snapshot: &ImageBrowserSnapshot,
    options: PlanePaneRenderOptions,
) -> Result<DynamicImage, String> {
    let raster = snapshot
        .plane
        .clone()
        .ok_or_else(|| "snapshot does not contain a plane raster".to_string())?;
    let cursor = options
        .show_live_reticle
        .then(|| {
            snapshot
                .plane_cursor
                .as_ref()
                .map(|cursor| (cursor.sampled_x, cursor.sampled_y))
        })
        .flatten();
    let sampled_shape = Some((raster.width, raster.height));
    let region_overlay_shapes = snapshot
        .region
        .as_ref()
        .map(|region| region.overlay_shapes.clone())
        .unwrap_or_default();
    let input = ImagePlaneRenderInput {
        cache_key: 0,
        raster,
        cursor_sample: cursor,
        sampled_shape,
        display_axes: snapshot.display_axes.clone(),
        probe: snapshot.probe.clone(),
        overlay_markers: Vec::new(),
        region_overlay_shapes,
        display_aspect_ratio: image_plane_display_aspect_ratio(snapshot),
        colormap: map_colormap(options.colormap),
        invert: options.invert,
        theme_mode: map_theme(options.theme),
    };
    render_image_plane_image(width, height, &input)
}

fn map_theme(theme: PlanePaneTheme) -> ThemeMode {
    match theme {
        PlanePaneTheme::DenseAnsi => ThemeMode::DenseAnsi,
        PlanePaneTheme::RichPanel => ThemeMode::RichPanel,
    }
}

fn map_colormap(colormap: PlanePaneColormap) -> ImagePlaneColormap {
    match colormap {
        PlanePaneColormap::Grayscale => ImagePlaneColormap::Grayscale,
        PlanePaneColormap::Viridis => ImagePlaneColormap::Viridis,
        PlanePaneColormap::Inferno => ImagePlaneColormap::Inferno,
    }
}

fn image_plane_display_aspect_ratio(snapshot: &ImageBrowserSnapshot) -> Option<f64> {
    let x = snapshot.display_axes.first()?;
    let y = snapshot.display_axes.get(1)?;
    let x_span = x.trc.saturating_sub(x.blc).saturating_add(1).max(1) as f64;
    let y_span = y.trc.saturating_sub(y.blc).saturating_add(1).max(1) as f64;
    let (x_scale, y_scale) = image_plane_axis_scales(x, y);
    let aspect = (x_span * x_scale) / (y_span * y_scale);
    (aspect.is_finite() && aspect > 0.0).then_some(aspect)
}

fn image_plane_axis_scales(x: &ImageDisplayAxisState, y: &ImageDisplayAxisState) -> (f64, f64) {
    if is_direction_display_axis(&x.name) && is_direction_display_axis(&y.name) {
        return (
            x.world_increment
                .map(|increment| {
                    casacore_types::measures::direction::angular_increment_arcseconds(increment)
                        .value()
                        .abs()
                })
                .filter(|value| *value > 0.0)
                .unwrap_or(1.0),
            y.world_increment
                .map(|increment| {
                    casacore_types::measures::direction::angular_increment_arcseconds(increment)
                        .value()
                        .abs()
                })
                .filter(|value| *value > 0.0)
                .unwrap_or(1.0),
        );
    }
    if !x.unit.is_empty() && x.unit == y.unit {
        return (
            x.world_increment
                .map(f64::abs)
                .filter(|value| *value > 0.0)
                .unwrap_or(1.0),
            y.world_increment
                .map(f64::abs)
                .filter(|value| *value > 0.0)
                .unwrap_or(1.0),
        );
    }
    (1.0, 1.0)
}

fn is_direction_display_axis(name: &str) -> bool {
    name.eq_ignore_ascii_case("Right Ascension")
        || name.eq_ignore_ascii_case("RA")
        || name.eq_ignore_ascii_case("Declination")
        || name.eq_ignore_ascii_case("DEC")
}

#[cfg(test)]
mod tests {
    use super::{
        OrderedReadyBuffer, PlanePaneRenderOptions, QueueDepthTracker, ReadyInsertResult,
        WorkerActivity,
    };
    use casacore_imagebrowser_protocol::{
        ImageBrowserCapabilities, ImageBrowserFocus, ImageBrowserParameters, ImageBrowserProbe,
        ImageBrowserSnapshot, ImageBrowserView, ImageDisplayAxisState, ImageNavigationMetrics,
        ImagePlaneCursorState, ImagePlaneRaster,
    };

    #[test]
    fn ordered_ready_buffer_preserves_sequence_order() {
        let mut buffer = OrderedReadyBuffer::new(4);
        assert_eq!(buffer.insert(1, "b"), ReadyInsertResult::Inserted);
        assert_eq!(buffer.insert(0, "a"), ReadyInsertResult::Inserted);
        assert_eq!(buffer.insert(2, "c"), ReadyInsertResult::Inserted);
        assert_eq!(buffer.pop_next(), Some((0, "a")));
        assert_eq!(buffer.pop_next(), Some((1, "b")));
        assert_eq!(buffer.pop_next(), Some((2, "c")));
    }

    #[test]
    fn ordered_ready_buffer_rejects_stale_and_caps_capacity() {
        let mut buffer = OrderedReadyBuffer::new(2);
        assert_eq!(buffer.insert(0, "a"), ReadyInsertResult::Inserted);
        assert_eq!(buffer.insert(1, "b"), ReadyInsertResult::Inserted);
        assert_eq!(buffer.insert(2, "c"), ReadyInsertResult::RejectedAtCapacity);
        assert_eq!(buffer.pop_next(), Some((0, "a")));
        assert_eq!(buffer.insert(0, "old"), ReadyInsertResult::Stale);
        assert_eq!(buffer.max_len(), 2);
    }

    #[test]
    fn worker_activity_tracks_peak_concurrency() {
        let activity = WorkerActivity::default();
        let guard_a = activity.enter();
        let guard_b = activity.enter();
        assert_eq!(activity.active(), 2);
        assert_eq!(activity.max_active(), 2);
        drop(guard_b);
        assert_eq!(activity.active(), 1);
        drop(guard_a);
        assert_eq!(activity.active(), 0);
        assert_eq!(activity.max_active(), 2);
    }

    #[test]
    fn queue_depth_tracker_tracks_current_and_peak_depth() {
        let tracker = QueueDepthTracker::default();
        tracker.push();
        tracker.push();
        assert_eq!(tracker.current(), 2);
        assert_eq!(tracker.max_depth(), 2);
        tracker.pop();
        assert_eq!(tracker.current(), 1);
        assert_eq!(tracker.max_depth(), 2);
    }

    #[test]
    fn render_plane_pane_from_snapshot_accepts_basic_snapshot() {
        let snapshot = ImageBrowserSnapshot {
            status_line: "ready".to_string(),
            active_view: ImageBrowserView::Plane,
            focus: ImageBrowserFocus::Content,
            shape: vec![4, 4, 1],
            parameters: ImageBrowserParameters::default(),
            inspector_lines: Vec::new(),
            content_lines: Vec::new(),
            navigation: ImageNavigationMetrics {
                selected_index: 0,
                total_items: 1,
                viewport_items: 1,
            },
            plane: Some(ImagePlaneRaster {
                width: 4,
                height: 4,
                pixels_u8: vec![32; 16],
                clip_min: 0.0,
                clip_max: 1.0,
                data_min: 0.0,
                data_max: 1.0,
                value_unit: "Jy/beam".to_string(),
                histogram_bins: vec![0; 48],
                masked_or_non_finite_count: 0,
                no_finite_values: false,
            }),
            probe: Some(ImageBrowserProbe {
                pixel_indices: vec![1, 1, 0],
                pixel_axes: Vec::new(),
                value: 1.0,
                masked: false,
                finite: true,
                world_axes: Vec::new(),
            }),
            profile: None,
            display_axes: vec![
                ImageDisplayAxisState {
                    axis: 0,
                    name: "Right Ascension".to_string(),
                    unit: "rad".to_string(),
                    blc: 0,
                    trc: 3,
                    inc: 1,
                    sampled_len: 4,
                    world_increment: Some(1e-4),
                },
                ImageDisplayAxisState {
                    axis: 1,
                    name: "Declination".to_string(),
                    unit: "rad".to_string(),
                    blc: 0,
                    trc: 3,
                    inc: 1,
                    sampled_len: 4,
                    world_increment: Some(1e-4),
                },
            ],
            plane_cursor: Some(ImagePlaneCursorState {
                sampled_x: 1,
                sampled_y: 1,
                pixel_x: 1,
                pixel_y: 1,
            }),
            non_display_axes: Vec::new(),
            region: None,
            backend_timing: None,
            capabilities: ImageBrowserCapabilities::default(),
        };

        let image = super::render_plane_pane_from_snapshot(
            320,
            200,
            &snapshot,
            PlanePaneRenderOptions::default(),
        )
        .expect("render plane pane");
        assert_eq!(image.width(), 320);
        assert_eq!(image.height(), 200);
    }
}

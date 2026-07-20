// SPDX-License-Identifier: LGPL-3.0-or-later
//! Long-lived image browser session state.

use std::collections::{HashMap, HashSet, VecDeque};
use std::fs;
use std::hash::Hash;
use std::path::{Path, PathBuf};
use std::sync::mpsc::{self, Receiver, Sender};
use std::thread;
use std::time::Instant;

use crate::browser_resources::{
    ImageBackendResourcePlan, ImageBackendResourceRequest, ImageBrowserMemoryBudget,
};
use casa_images::error::ImageError;
use casa_images::image_view::{
    ImageRegion, ImageRegionOverlayShape, ImageRegionShape, ImageRegionStats, ImageRegionVertex,
    PlaneAutoscaleMode, PlaneRenderTelemetry, PlaneStretchPreset, PlaneStretchSettings,
    format_numeric_value_with_unit,
};
use casa_images::{
    ImageAxisValue, ImageDisplayAxis, ImageMetadataSection, ImageNonDisplayAxis, ImageProbe,
    ImageProfile, ImageProfileSample, ImageViewCapabilities, ImageViewWindow, OpenedImageView,
    PlaneRaster,
};
use casa_types::measures::direction::{format_declination_labeled, format_right_ascension_labeled};
use casars_imagebrowser_protocol::{
    ImageBackendPlaneCacheResult, ImageBackendTimingState, ImageBrowserAxisValue,
    ImageBrowserCapabilities, ImageBrowserCommand, ImageBrowserFocus, ImageBrowserParameters,
    ImageBrowserPreviewPayload, ImageBrowserPreviewRequest, ImageBrowserProbe,
    ImageBrowserSnapshot, ImageBrowserView, ImageBrowserViewport, ImageDisplayAxisState,
    ImageMaskReference, ImageNavigationMetrics, ImageNonDisplayAxisState, ImagePlaneContentMode,
    ImagePlaneCursorState, ImagePlaneRaster, ImageProfilePayload, ImageProfileSampleState,
    ImageRegionOverlayShapeState, ImageRegionOverlayVertex, ImageRegionReference, ImageRegionState,
    ImageRegionStatsState,
};

/// Long-lived read-only image browser session.
#[derive(Debug)]
pub struct ImageBrowserSession {
    view: OpenedImageView,
    window: ImageViewWindow,
    stretch: SessionStretchState,
    frozen_clip_bounds: Option<(f64, f64)>,
    active_view: ImageBrowserView,
    focus: ImageBrowserFocus,
    viewport: ImageBrowserViewport,
    cursor_x: usize,
    cursor_y: usize,
    non_display_indices: Vec<usize>,
    selected_profile_axis: Option<usize>,
    content_offset: usize,
    plane_content_mode: ImagePlaneContentMode,
    region: Option<ImageRegion>,
    region_revision: u64,
    mask_revision: u64,
    active_region_definition_name: Option<String>,
    region_reference: ImageRegionReference,
    mask_reference: ImageMaskReference,
    saved_region_cycle_index: usize,
    perf_enabled: bool,
    plane_cache: RecentCache<PlaneCacheKey, PlaneRaster>,
    prefetched_plane_keys: HashSet<PlaneCacheKey>,
    profile_cache: RecentCache<ProfileCacheKey, ImageProfile>,
    profile_perf: ProfilePerfAggregate,
    last_non_display_step: Option<(usize, i32)>,
    prefetch_worker: Option<PlanePrefetchWorker>,
    resources: ImageBackendResourcePlan,
}

#[derive(Debug, Clone, PartialEq)]
struct SessionStretchState {
    preset: PlaneStretchPreset,
    autoscale: PlaneAutoscaleMode,
    manual_clip: Option<(f64, f64)>,
}

impl Default for SessionStretchState {
    fn default() -> Self {
        Self {
            preset: PlaneStretchPreset::Percentile99,
            autoscale: PlaneAutoscaleMode::PerPlane,
            manual_clip: None,
        }
    }
}

impl SessionStretchState {
    fn plane_settings(&self) -> PlaneStretchSettings {
        PlaneStretchSettings {
            preset: self.preset,
            autoscale: self.autoscale,
            manual_clip: self.manual_clip,
        }
    }
}

struct PreparedRegionReference {
    region: Option<ImageRegion>,
    active_definition_name: Option<String>,
    reset_saved_cycle: bool,
}

#[derive(Debug, Clone, Copy, Default)]
struct ProfilePerfAggregate {
    cache_hits: u64,
    cache_misses: u64,
    extract_total_ns: u64,
}

#[derive(Debug)]
struct RecentCache<K, V> {
    capacity_bytes: u64,
    total_bytes: u64,
    latest_value_budget: bool,
    retained_bytes: fn(&V) -> u64,
    values: HashMap<K, V>,
    order: VecDeque<K>,
}

impl<K, V> RecentCache<K, V>
where
    K: Clone + Eq + Hash,
{
    fn with_byte_budget(capacity_bytes: u64, retained_bytes: fn(&V) -> u64) -> Self {
        Self {
            capacity_bytes,
            total_bytes: 0,
            latest_value_budget: false,
            retained_bytes,
            values: HashMap::new(),
            order: VecDeque::new(),
        }
    }

    fn latest_value(retained_bytes: fn(&V) -> u64) -> Self {
        Self {
            capacity_bytes: 0,
            total_bytes: 0,
            latest_value_budget: true,
            retained_bytes,
            values: HashMap::new(),
            order: VecDeque::new(),
        }
    }

    fn get(&mut self, key: &K) -> Option<V>
    where
        V: Clone,
    {
        let value = self.values.get(key).cloned()?;
        self.touch(key);
        Some(value)
    }

    fn insert(&mut self, key: K, value: V) {
        let value_bytes = (self.retained_bytes)(&value);
        if self.latest_value_budget {
            self.capacity_bytes = value_bytes;
        }
        if value_bytes > self.capacity_bytes {
            return;
        }
        if let Some(previous) = self.values.insert(key.clone(), value) {
            self.total_bytes = self
                .total_bytes
                .saturating_sub((self.retained_bytes)(&previous))
                .saturating_add(value_bytes);
            self.touch(&key);
            self.evict_if_needed();
            return;
        }
        self.total_bytes = self.total_bytes.saturating_add(value_bytes);
        self.order.push_back(key);
        self.evict_if_needed();
    }

    fn contains_key(&self, key: &K) -> bool {
        self.values.contains_key(key)
    }

    fn touch(&mut self, key: &K) {
        if let Some(index) = self.order.iter().position(|existing| existing == key)
            && let Some(existing) = self.order.remove(index)
        {
            self.order.push_back(existing);
        }
    }

    fn evict_if_needed(&mut self) {
        while self.total_bytes > self.capacity_bytes {
            let Some(oldest) = self.order.pop_front() else {
                break;
            };
            if let Some(previous) = self.values.remove(&oldest) {
                self.total_bytes = self
                    .total_bytes
                    .saturating_sub((self.retained_bytes)(&previous));
            }
        }
    }
}

fn plane_raster_retained_bytes(raster: &PlaneRaster) -> u64 {
    let pixels = raster.pixels_u8.capacity() as u64;
    let histogram =
        (raster.histogram_bins.capacity() as u64).saturating_mul(std::mem::size_of::<u32>() as u64);
    pixels
        .saturating_add(histogram)
        .saturating_add(raster.value_unit.capacity() as u64)
}

fn profile_retained_bytes(profile: &ImageProfile) -> u64 {
    let samples = (profile.samples.capacity() as u64)
        .saturating_mul(std::mem::size_of::<ImageProfileSample>() as u64);
    let sample_strings = profile.samples.iter().fold(0u64, |total, sample| {
        let axis = sample.world_axis.as_ref().map_or(0, |axis| {
            (axis.name.capacity() as u64).saturating_add(axis.unit.capacity() as u64)
        });
        total.saturating_add(axis)
    });
    samples
        .saturating_add(sample_strings)
        .saturating_add(profile.axis_name.capacity() as u64)
        .saturating_add(profile.axis_unit.capacity() as u64)
        .saturating_add(profile.value_unit.capacity() as u64)
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct PlaneCacheKey {
    viewport: (usize, usize),
    blc: Vec<usize>,
    trc: Vec<usize>,
    inc: Vec<usize>,
    non_display_indices: Vec<usize>,
    stretch_preset: PlaneStretchPreset,
    autoscale: PlaneAutoscaleMode,
    manual_clip: Option<(u64, u64)>,
    clip_override: Option<(u64, u64)>,
    region_revision: u64,
    mask_revision: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct ProfileCacheKey {
    pixel_xy: (usize, usize),
    blc: Vec<usize>,
    trc: Vec<usize>,
    inc: Vec<usize>,
    normalized_non_display_indices: Vec<usize>,
    profile_axis: usize,
    region_revision: Option<u64>,
    mask_revision: u64,
}

#[derive(Debug, Clone)]
struct SessionPreviewState {
    viewport: ImageBrowserViewport,
    window: ImageViewWindow,
    stretch: SessionStretchState,
    frozen_clip_bounds: Option<(f64, f64)>,
    plane_content_mode: ImagePlaneContentMode,
    non_display_indices: Vec<usize>,
    content_offset: usize,
}

#[derive(Debug, Clone)]
struct PlanePrefetchRequest {
    key: PlaneCacheKey,
    viewport: (usize, usize),
    window: ImageViewWindow,
    non_display_indices: Vec<usize>,
    stretch: PlaneStretchSettings,
    clip_override: Option<(f64, f64)>,
    region: Option<ImageRegion>,
}

#[derive(Debug)]
struct PlanePrefetchResult {
    key: PlaneCacheKey,
    raster: Result<PlaneRaster, String>,
}

#[derive(Debug)]
struct PlanePrefetchWorker {
    request_txs: Vec<Sender<PlanePrefetchRequest>>,
    response_rx: Receiver<PlanePrefetchResult>,
    queued: HashSet<PlaneCacheKey>,
    next_worker: usize,
}

impl PlanePrefetchWorker {
    fn new(path: &Path, plan: ImageBackendResourcePlan) -> Result<Option<Self>, ImageError> {
        if plan.prefetch_worker_count == 0 {
            return Ok(None);
        }
        let (response_tx, response_rx) = mpsc::channel::<PlanePrefetchResult>();
        let path = path.to_path_buf();
        let worker_count = plan.prefetch_worker_count;
        let mut request_txs = Vec::with_capacity(worker_count);
        for worker_index in 0..worker_count {
            let (request_tx, request_rx) = mpsc::channel::<PlanePrefetchRequest>();
            let worker_path = path.clone();
            let worker_response_tx = response_tx.clone();
            thread::Builder::new()
                .name(format!("imexplore-plane-prefetch-{worker_index}"))
                .spawn(move || {
                    run_plane_prefetch_worker(
                        worker_path,
                        request_rx,
                        worker_response_tx,
                        plan.view_cache_bytes_per_reader,
                    )
                })
                .map_err(|error| {
                    ImageError::InvalidMetadata(format!("failed to spawn prefetch worker: {error}"))
                })?;
            request_txs.push(request_tx);
        }
        Ok(Some(Self {
            request_txs,
            response_rx,
            queued: HashSet::new(),
            next_worker: 0,
        }))
    }

    fn submit(
        &mut self,
        request: PlanePrefetchRequest,
        cache: &RecentCache<PlaneCacheKey, PlaneRaster>,
    ) {
        if cache.contains_key(&request.key) || self.queued.contains(&request.key) {
            return;
        }
        let worker_count = self.request_txs.len().max(1);
        let worker_index = self.next_worker % worker_count;
        self.next_worker = (self.next_worker + 1) % worker_count;
        if self.request_txs[worker_index].send(request.clone()).is_ok() {
            self.queued.insert(request.key);
        }
    }

    fn drain_into(
        &mut self,
        cache: &mut RecentCache<PlaneCacheKey, PlaneRaster>,
        prefetched_keys: &mut Vec<PlaneCacheKey>,
    ) {
        while let Ok(result) = self.response_rx.try_recv() {
            self.queued.remove(&result.key);
            if let Ok(raster) = result.raster {
                prefetched_keys.push(result.key.clone());
                cache.insert(result.key, raster);
            }
        }
    }
}

fn run_plane_prefetch_worker(
    path: PathBuf,
    request_rx: Receiver<PlanePrefetchRequest>,
    response_tx: Sender<PlanePrefetchResult>,
    view_cache_bytes: usize,
) {
    let Ok(view) = OpenedImageView::open_with_cache_bytes(&path, view_cache_bytes) else {
        return;
    };
    while let Ok(request) = request_rx.recv() {
        let raster = view
            .render_plane_with_window_and_axes_and_stretch(
                request.viewport,
                &request.window,
                &request.non_display_indices,
                &request.stretch,
                request.clip_override,
                request.region.as_ref(),
            )
            .map_err(|error| error.to_string());
        if response_tx
            .send(PlanePrefetchResult {
                key: request.key,
                raster,
            })
            .is_err()
        {
            break;
        }
    }
}

fn apply_plane_timing(
    timing: &mut ImageBackendTimingState,
    cache_result: ImageBackendPlaneCacheResult,
    cached_plane_lookup_ns: u64,
    telemetry: PlaneRenderTelemetry,
) {
    timing.plane_cache_result = cache_result;
    timing.cached_plane_lookup_ns = cached_plane_lookup_ns;
    timing.plane_extract_ns = telemetry.plane_extract_ns;
    timing.stat_collection_ns = telemetry.stat_collection_ns;
    timing.histogram_ns = telemetry.histogram_ns;
    timing.rasterize_ns = telemetry.rasterize_ns;
    timing.total_plane_ns = telemetry.total_plane_ns;
}

fn duration_ns(duration: std::time::Duration) -> u64 {
    duration.as_nanos().min(u128::from(u64::MAX)) as u64
}

fn image_browser_perf_enabled() -> bool {
    std::env::var_os("CASARS_IMEXPLORE_PERF").is_some()
}

fn normalize_non_display_indices(
    view: &OpenedImageView,
    window: &ImageViewWindow,
    requested: &[usize],
) -> Vec<usize> {
    view.axis_model()
        .non_display_axes
        .iter()
        .enumerate()
        .map(|(position, axis)| {
            let length = window.sampled_axis_len(*axis);
            let requested_index = requested.get(position).copied().unwrap_or_default();
            if length == 0 {
                0
            } else {
                requested_index.min(length.saturating_sub(1))
            }
        })
        .collect()
}

impl ImageBrowserSession {
    /// Opens a new browser session rooted at the provided image path.
    pub fn open(
        path: impl AsRef<Path>,
        viewport: ImageBrowserViewport,
    ) -> Result<Self, ImageError> {
        Self::open_with_parameters(path, viewport, None)
    }

    /// Opens a new browser session rooted at the provided image path and parameters.
    pub fn open_with_parameters(
        path: impl AsRef<Path>,
        viewport: ImageBrowserViewport,
        parameters: Option<&ImageBrowserParameters>,
    ) -> Result<Self, ImageError> {
        let path = path.as_ref();
        let budget = ImageBrowserMemoryBudget::from_process_snapshot().map_err(|error| {
            ImageError::InvalidMetadata(format!("image-browser resource budget: {error}"))
        })?;
        let available_parallelism = thread::available_parallelism()
            .map_err(|error| {
                ImageError::InvalidMetadata(format!("image-browser available parallelism: {error}"))
            })?
            .get();
        let plane_pixel_size = (
            usize::from(if viewport.plane_pixel_width > 0 {
                viewport.plane_pixel_width
            } else {
                viewport.width.max(1)
            }),
            usize::from(if viewport.plane_pixel_height > 0 {
                viewport.plane_pixel_height
            } else {
                viewport.height.max(1)
            }),
        );
        let initial_resources = budget
            .plan_backend(ImageBackendResourceRequest {
                plane_pixel_size,
                prefetch_frame_count: 0,
                available_parallelism,
            })
            .map_err(|error| {
                ImageError::InvalidMetadata(format!("image-browser backend resources: {error}"))
            })?;
        let view = OpenedImageView::open_with_cache_bytes(
            path,
            initial_resources.view_cache_bytes_per_reader,
        )?;
        let non_display_axis_count = session_non_display_axis_count(&view);
        let stretch = parameters
            .map(parse_stretch_parameters)
            .transpose()?
            .unwrap_or_default();
        let window = match parameters {
            Some(parameters) => {
                view.window_from_text(&parameters.blc, &parameters.trc, &parameters.inc)?
            }
            None => view.default_window(),
        };
        let prefetch_frame_count = view
            .axis_model()
            .non_display_axes
            .iter()
            .map(|axis| window.sampled_axis_len(*axis).saturating_sub(1))
            .max()
            .unwrap_or(0);
        let resources = budget
            .plan_backend(ImageBackendResourceRequest {
                plane_pixel_size,
                prefetch_frame_count,
                available_parallelism,
            })
            .map_err(|error| {
                ImageError::InvalidMetadata(format!("image-browser backend resources: {error}"))
            })?;
        let default_display_pixels = centered_display_pixels(&view, &window);
        let default_non_display_pixels = centered_non_display_pixels(&view, &window);
        let active_view = if view.capabilities().renderable_plane {
            ImageBrowserView::Plane
        } else {
            ImageBrowserView::Metadata
        };
        let perf_enabled = image_browser_perf_enabled();
        let mut session = Self {
            prefetch_worker: PlanePrefetchWorker::new(view.path(), resources)?,
            resources,
            view,
            window,
            stretch,
            frozen_clip_bounds: None,
            active_view,
            focus: ImageBrowserFocus::Content,
            viewport,
            cursor_x: 0,
            cursor_y: 0,
            non_display_indices: vec![0; non_display_axis_count],
            selected_profile_axis: None,
            content_offset: 0,
            plane_content_mode: default_plane_content_mode(viewport),
            region: None,
            region_revision: 0,
            mask_revision: 0,
            active_region_definition_name: None,
            region_reference: ImageRegionReference::None,
            mask_reference: ImageMaskReference::None,
            saved_region_cycle_index: 0,
            perf_enabled,
            plane_cache: RecentCache::with_byte_budget(
                resources.plane_result_cache_bytes,
                plane_raster_retained_bytes,
            ),
            prefetched_plane_keys: HashSet::new(),
            profile_cache: RecentCache::latest_value(profile_retained_bytes),
            profile_perf: ProfilePerfAggregate::default(),
            last_non_display_step: None,
        };
        session.selected_profile_axis = session.view.preferred_profile_axis();
        session.clamp_cursor_to_window(
            None,
            None,
            default_display_pixels,
            Some(default_non_display_pixels),
        );
        Ok(session)
    }

    /// Replaces the current root image.
    pub fn reopen(
        &mut self,
        path: impl AsRef<Path>,
        viewport: ImageBrowserViewport,
        parameters: Option<&ImageBrowserParameters>,
    ) -> Result<ImageBrowserSnapshot, ImageError> {
        *self = Self::open_with_parameters(path, viewport, parameters)?;
        self.snapshot()
    }

    /// Builds a stateless preview snapshot for a specific non-display-axis occurrence.
    pub fn preview_occurrence(
        &mut self,
        request: &ImageBrowserPreviewRequest,
    ) -> Result<ImageBrowserPreviewPayload, ImageError> {
        let visible_display_pixels = self.current_display_pixels();
        let visible_non_display_pixels = self.current_non_display_pixels();
        let saved = SessionPreviewState {
            viewport: self.viewport,
            window: self.window.clone(),
            stretch: self.stretch.clone(),
            frozen_clip_bounds: self.frozen_clip_bounds,
            plane_content_mode: self.plane_content_mode,
            non_display_indices: self.non_display_indices.clone(),
            content_offset: self.content_offset,
        };

        self.viewport = request.viewport;
        self.stretch = parse_stretch_parameters(&request.parameters)?;
        self.frozen_clip_bounds = None;
        self.window = self.view.window_from_text(
            &request.parameters.blc,
            &request.parameters.trc,
            &request.parameters.inc,
        )?;
        self.plane_content_mode = request.plane_content_mode;
        self.content_offset = 0;
        let preview_non_display_indices =
            normalize_non_display_indices(&self.view, &self.window, &request.non_display_indices);
        self.non_display_indices = preview_non_display_indices.clone();
        self.clamp_cursor_to_window(visible_display_pixels, None, None, None);
        self.non_display_indices = preview_non_display_indices.clone();

        let snapshot = self.snapshot_with_profile(request.include_profile);

        self.viewport = saved.viewport;
        self.window = saved.window;
        self.stretch = saved.stretch;
        self.frozen_clip_bounds = saved.frozen_clip_bounds;
        self.plane_content_mode = saved.plane_content_mode;
        self.non_display_indices = saved.non_display_indices.clone();
        self.content_offset = saved.content_offset;
        self.clamp_cursor_to_window(
            visible_display_pixels,
            visible_non_display_pixels,
            None,
            None,
        );
        self.non_display_indices = saved.non_display_indices;

        let snapshot = snapshot?;
        Ok(ImageBrowserPreviewPayload {
            non_display_indices: preview_non_display_indices,
            snapshot: Box::new(snapshot),
        })
    }

    /// Applies a session command and returns the updated snapshot.
    pub fn handle_command(
        &mut self,
        command: ImageBrowserCommand,
    ) -> Result<ImageBrowserSnapshot, ImageError> {
        match command {
            ImageBrowserCommand::OpenRoot {
                path,
                viewport,
                parameters,
            } => self.reopen(path, viewport, parameters.as_ref()),
            ImageBrowserCommand::Resize { viewport } => {
                self.viewport = viewport;
                self.snapshot()
            }
            ImageBrowserCommand::CycleView { forward } => {
                self.active_view = cycle_view(self.available_views(), self.active_view, forward);
                self.content_offset = 0;
                self.snapshot()
            }
            ImageBrowserCommand::SetFocus { focus } => {
                self.focus = focus;
                self.snapshot()
            }
            ImageBrowserCommand::MoveCursor { dx, dy } => {
                self.move_cursor(dx, dy)?;
                self.snapshot()
            }
            ImageBrowserCommand::SetCursor { x, y } => {
                self.set_cursor_pixels(x, y);
                self.snapshot()
            }
            ImageBrowserCommand::StepNonDisplayAxis { axis, delta } => {
                self.step_non_display_axis(axis, delta);
                self.snapshot()
            }
            ImageBrowserCommand::SetSelectedNonDisplayAxis { axis } => {
                self.set_selected_profile_axis(axis)?;
                self.snapshot()
            }
            ImageBrowserCommand::SetViewWindow { parameters } => {
                self.set_view_window(&parameters)?;
                self.snapshot()
            }
            ImageBrowserCommand::SetPlaneContentMode { mode } => {
                self.plane_content_mode = mode;
                self.snapshot()
            }
            ImageBrowserCommand::SetSelectionReferences { region, mask } => {
                self.set_selection_references(region, mask)?;
                self.snapshot()
            }
            ImageBrowserCommand::StartRegionShape => {
                self.start_region_shape()?;
                self.snapshot()
            }
            ImageBrowserCommand::AppendRegionVertex { x, y } => {
                self.append_region_vertex_pixels(x, y)?;
                self.snapshot()
            }
            ImageBrowserCommand::CloseRegionShape => {
                self.close_region_shape()?;
                self.snapshot()
            }
            ImageBrowserCommand::UndoRegionVertex => {
                self.undo_region_vertex()?;
                self.snapshot()
            }
            ImageBrowserCommand::CancelRegionShape => {
                if !self.cancel_region_shape() {
                    return Err(ImageError::InvalidMetadata(
                        "no open polygon to cancel".into(),
                    ));
                }
                self.snapshot()
            }
            ImageBrowserCommand::ClearRegion => {
                self.clear_region();
                self.region_reference = ImageRegionReference::None;
                self.snapshot()
            }
            ImageBrowserCommand::SaveRegionDefinition => {
                self.save_region_definition()?;
                self.snapshot()
            }
            ImageBrowserCommand::LoadNextRegionDefinition => {
                self.load_next_region_definition()?;
                self.region_reference = self
                    .active_region_definition_name
                    .as_ref()
                    .map(|name| ImageRegionReference::Definition { name: name.clone() })
                    .unwrap_or_default();
                self.snapshot()
            }
            ImageBrowserCommand::LoadRegionDefinition { name } => {
                self.load_region_definition(&name)?;
                self.region_reference = ImageRegionReference::Definition { name };
                self.snapshot()
            }
            ImageBrowserCommand::RenameRegionDefinition { name, new_name } => {
                self.rename_region_definition(&name, &new_name)?;
                self.snapshot()
            }
            ImageBrowserCommand::DeleteRegionDefinition { name } => {
                self.delete_region_definition(&name)?;
                self.snapshot()
            }
            ImageBrowserCommand::SetDefaultMask { name } => {
                self.set_default_mask(&name)?;
                self.mask_reference = ImageMaskReference::Name { name };
                self.snapshot()
            }
            ImageBrowserCommand::UnsetDefaultMask => {
                self.unset_default_mask()?;
                self.mask_reference = ImageMaskReference::None;
                self.snapshot()
            }
            ImageBrowserCommand::DeleteMask { name } => {
                self.delete_mask(&name)?;
                self.snapshot()
            }
            ImageBrowserCommand::WriteRegionMask { name, set_default } => {
                self.write_region_mask(name.as_deref(), set_default)?;
                self.snapshot()
            }
            ImageBrowserCommand::ExportRegionFile { path } => {
                self.export_region_file(Path::new(&path))?;
                self.snapshot()
            }
            ImageBrowserCommand::LoadRegionFile { path } => {
                self.load_region_file(Path::new(&path))?;
                self.region_reference = ImageRegionReference::File { path };
                self.snapshot()
            }
            ImageBrowserCommand::AppendRegionFile { path } => {
                self.append_region_file(Path::new(&path))?;
                self.snapshot()
            }
            ImageBrowserCommand::PreviewOccurrence { request } => {
                Ok(*self.preview_occurrence(&request)?.snapshot)
            }
            ImageBrowserCommand::GetSnapshot => self.snapshot(),
        }
    }

    /// Returns the current snapshot without changing state.
    pub fn snapshot(&mut self) -> Result<ImageBrowserSnapshot, ImageError> {
        self.snapshot_with_profile(true)
    }

    fn snapshot_with_profile(
        &mut self,
        include_profile: bool,
    ) -> Result<ImageBrowserSnapshot, ImageError> {
        self.drain_prefetched_planes();
        let mut backend_timing = self.perf_enabled.then_some(ImageBackendTimingState {
            plane_cache_result: ImageBackendPlaneCacheResult::Miss,
            cached_plane_lookup_ns: 0,
            plane_extract_ns: 0,
            stat_collection_ns: 0,
            histogram_ns: 0,
            rasterize_ns: 0,
            total_plane_ns: 0,
            profile_cache_hits: self.profile_perf.cache_hits,
            profile_cache_misses: self.profile_perf.cache_misses,
            profile_extract_total_ns: self.profile_perf.extract_total_ns,
        });
        let plane_raster = if self.active_view == ImageBrowserView::Plane
            && self.view.capabilities().renderable_plane
        {
            let clip_override = match self.stretch.autoscale {
                PlaneAutoscaleMode::PerPlane => None,
                PlaneAutoscaleMode::Frozen => self.frozen_clip_bounds,
            };
            let raster = self.cached_plane_raster(clip_override, backend_timing.as_mut())?;
            if self.stretch.autoscale == PlaneAutoscaleMode::Frozen
                && self.frozen_clip_bounds.is_none()
                && !raster.no_finite_values
            {
                self.frozen_clip_bounds = Some((raster.clip_min, raster.clip_max));
            }
            Some(raster)
        } else {
            None
        };
        let mut inspector_lines = self.inspector_lines()?;
        inspector_lines.extend(self.plane_display_lines(plane_raster.as_ref()));
        let profile = if include_profile
            && self.view.capabilities().renderable_plane
            && matches!(
                self.active_view,
                ImageBrowserView::Plane | ImageBrowserView::Spectrum
            ) {
            let closed_region = self.region.as_ref().and_then(|region| {
                region
                    .shapes
                    .iter()
                    .any(|shape| shape.closed && shape.vertices.len() >= 3)
                    .then_some(region.clone())
            });
            self.selected_profile_axis()
                .map(|profile_axis| {
                    self.cached_profile(
                        (self.cursor_x, self.cursor_y),
                        profile_axis,
                        closed_region.as_ref(),
                        backend_timing.as_mut(),
                    )
                })
                .transpose()?
        } else {
            None
        };
        let inspector_height = if self.viewport.inspector_height == 0 {
            inspector_lines.len().max(1)
        } else {
            self.viewport.inspector_height as usize
        };
        let content_height = self.viewport.height.max(1) as usize;
        let all_content_lines = self.all_content_lines(plane_raster.as_ref(), profile.as_ref())?;
        let total_items = all_content_lines.len();
        let max_offset = total_items.saturating_sub(content_height);
        let content_offset = self.content_offset.min(max_offset);
        let content_lines = clip_lines(&all_content_lines, content_offset, content_height);
        let plane = plane_raster.map(map_plane_raster);
        let display_axes = self
            .view
            .display_axes_with_window(&self.window)
            .into_iter()
            .map(map_display_axis)
            .collect::<Vec<_>>();
        let probe = if self.active_view == ImageBrowserView::Plane
            && self.view.capabilities().renderable_plane
        {
            Some(map_probe(self.view.probe_with_window_and_axes(
                (self.cursor_x, self.cursor_y),
                &self.window,
                &self.non_display_indices,
            )?))
        } else {
            None
        };
        let non_display_axes = self
            .view
            .non_display_axes_with_window(&self.window, &self.non_display_indices)?
            .into_iter()
            .map(map_non_display_axis)
            .collect::<Vec<_>>();
        let region = self.region_state()?;

        self.schedule_plane_prefetch();

        let navigation = if self.active_view == ImageBrowserView::Plane
            && self.view.capabilities().renderable_plane
        {
            if let Some(display_axes) = self.view.axis_model().display_axes {
                ImageNavigationMetrics {
                    selected_index: self.cursor_y.min(
                        self.window
                            .sampled_axis_len(display_axes[1])
                            .saturating_sub(1),
                    ),
                    total_items: self.window.sampled_axis_len(display_axes[1]),
                    viewport_items: content_height,
                }
            } else {
                ImageNavigationMetrics {
                    selected_index: 0,
                    total_items: 0,
                    viewport_items: content_height,
                }
            }
        } else {
            ImageNavigationMetrics {
                selected_index: content_offset,
                total_items,
                viewport_items: content_height,
            }
        };

        Ok(ImageBrowserSnapshot {
            status_line: self.view.status_line(),
            active_view: self.active_view,
            focus: self.focus,
            shape: self.view.shape().to_vec(),
            parameters: self.parameter_state(),
            inspector_lines: clip_lines(&inspector_lines, 0, inspector_height),
            content_lines,
            navigation,
            plane,
            probe,
            profile: profile.as_ref().map(map_profile),
            display_axes,
            plane_cursor: self.current_plane_cursor(),
            non_display_axes,
            region,
            saved_region_names: self.view.saved_region_names(),
            active_region_definition_name: self.active_region_definition_name.clone(),
            region_reference: self.region_reference.clone(),
            mask_names: self.view.mask_names(),
            default_mask_name: self.view.default_mask_name(),
            mask_reference: self.mask_reference.clone(),
            backend_timing,
            capabilities: map_capabilities(self.view.capabilities()),
        })
    }

    fn current_plane_cursor(&self) -> Option<ImagePlaneCursorState> {
        let display_axes = self.view.axis_model().display_axes?;
        Some(ImagePlaneCursorState {
            sampled_x: self.cursor_x,
            sampled_y: self.cursor_y,
            pixel_x: self
                .window
                .sampled_axis_value(display_axes[0], self.cursor_x)
                .unwrap_or(self.window.blc()[display_axes[0]]),
            pixel_y: self
                .window
                .sampled_axis_value(display_axes[1], self.cursor_y)
                .unwrap_or(self.window.blc()[display_axes[1]]),
        })
    }

    fn available_views(&self) -> &'static [ImageBrowserView] {
        if self.view.capabilities().renderable_plane && !self.non_display_indices.is_empty() {
            &[
                ImageBrowserView::Metadata,
                ImageBrowserView::Coordinates,
                ImageBrowserView::Plane,
                ImageBrowserView::Spectrum,
            ]
        } else if self.view.capabilities().renderable_plane {
            &[
                ImageBrowserView::Metadata,
                ImageBrowserView::Coordinates,
                ImageBrowserView::Plane,
            ]
        } else {
            &[ImageBrowserView::Metadata, ImageBrowserView::Coordinates]
        }
    }

    fn move_cursor(&mut self, dx: i32, dy: i32) -> Result<(), ImageError> {
        if self.active_view == ImageBrowserView::Plane
            && self.focus == ImageBrowserFocus::Content
            && self.view.capabilities().renderable_plane
        {
            let Some(display_axes) = self.view.axis_model().display_axes else {
                return Ok(());
            };
            let max_x = self
                .window
                .sampled_axis_len(display_axes[0])
                .saturating_sub(1) as i32;
            let max_y = self
                .window
                .sampled_axis_len(display_axes[1])
                .saturating_sub(1) as i32;
            self.cursor_x = (self.cursor_x as i32 + dx).clamp(0, max_x) as usize;
            self.cursor_y = (self.cursor_y as i32 + dy).clamp(0, max_y) as usize;
            return Ok(());
        }

        let content_height = self.viewport.height.max(1) as usize;
        let total_items = self.all_content_lines(None, None)?.len();
        let max_offset = total_items.saturating_sub(content_height) as i32;
        self.content_offset = (self.content_offset as i32 + dy).clamp(0, max_offset) as usize;
        Ok(())
    }

    fn step_non_display_axis(&mut self, axis: usize, delta: i32) {
        let Some(position) = self
            .view
            .axis_model()
            .non_display_axes
            .iter()
            .position(|candidate| *candidate == axis)
        else {
            return;
        };
        let length = self.window.sampled_axis_len(axis);
        if length == 0 {
            self.non_display_indices[position] = 0;
            return;
        }
        self.non_display_indices[position] = (self.non_display_indices[position] as i32 + delta)
            .clamp(0, length.saturating_sub(1) as i32)
            as usize;
        self.last_non_display_step = (delta != 0).then_some((axis, delta.signum()));
    }

    fn inspector_lines(&self) -> Result<Vec<String>, ImageError> {
        let mut lines = vec![
            format!("View: {}", self.active_view.label()),
            format!(
                "Focus: {}",
                match self.focus {
                    ImageBrowserFocus::Inspector => "Inspector",
                    ImageBrowserFocus::Content => "Content",
                }
            ),
            format!("Status: {}", self.view.status_line()),
            format!("Shape: {:?}", self.view.shape()),
            format!("Pixel type: {:?}", self.view.pixel_type()),
            format!("BLC: {}", self.window.format_blc()),
            format!("TRC: {}", self.window.format_trc()),
            format!("INC: {}", self.window.format_inc()),
        ];
        if self.active_view == ImageBrowserView::Spectrum
            && let Some(axis) = self.selected_profile_axis()
            && let Some(axis_state) = self
                .view
                .non_display_axes_with_window(&self.window, &self.non_display_indices)?
                .into_iter()
                .find(|candidate| candidate.axis == axis)
        {
            lines.push(format!(
                "Profile axis {} ({}): pixel {} [{}/{}]",
                axis_state.name,
                axis_state.axis,
                axis_state.pixel,
                axis_state.index,
                axis_state.length.saturating_sub(1)
            ));
            if let Some(display_axes) = self.view.axis_model().display_axes {
                let pixel_x = self
                    .window
                    .sampled_axis_value(display_axes[0], self.cursor_x)
                    .unwrap_or(self.window.blc()[display_axes[0]]);
                let pixel_y = self
                    .window
                    .sampled_axis_value(display_axes[1], self.cursor_y)
                    .unwrap_or(self.window.blc()[display_axes[1]]);
                lines.push(format!("Plane cursor: x={pixel_x} y={pixel_y}"));
            }
        }
        if self.active_view == ImageBrowserView::Plane && self.view.capabilities().renderable_plane
        {
            let probe = self.view.probe_with_window_and_axes(
                (self.cursor_x, self.cursor_y),
                &self.window,
                &self.non_display_indices,
            )?;
            let Some(display_axes) = self.view.axis_model().display_axes else {
                return Ok(lines);
            };
            lines.push(format!(
                "Cursor: x={} y={}",
                probe.pixel_indices[display_axes[0]], probe.pixel_indices[display_axes[1]]
            ));
            lines.push(format!(
                "Value: {}",
                format_probe_value(&probe, self.view.brightness_unit())
            ));
            if probe.masked {
                lines.push("Masked: true".into());
            }
            for axis in probe.world_axes {
                lines.push(format_world_axis_line(&axis));
            }
        }
        if let Some(region_state) = self.region_state()? {
            lines.push(format!(
                "Region: {} ({} shape{})",
                region_state.label,
                region_state.shape_count,
                if region_state.shape_count == 1 {
                    ""
                } else {
                    "s"
                }
            ));
            if region_state.editing {
                lines.push(format!(
                    "Region edit: open polygon with {} vert{}",
                    region_state.active_shape_vertices,
                    if region_state.active_shape_vertices == 1 {
                        "ex"
                    } else {
                        "ices"
                    }
                ));
            }
            if let Some(stats) = region_state.stats.as_ref() {
                lines.push(format!("Region pixels: {}", stats.pixel_count));
                lines.push(format!(
                    "Region mean: {}",
                    format_numeric_value_with_unit(stats.mean, &stats.value_unit)
                ));
                lines.push(format!(
                    "Region sigma: {}",
                    format_numeric_value_with_unit(stats.sigma, &stats.value_unit)
                ));
                lines.push(format!(
                    "Region median: {}",
                    format_numeric_value_with_unit(stats.median, &stats.value_unit)
                ));
                lines.push(format!(
                    "Region RMS: {}",
                    format_numeric_value_with_unit(stats.rms, &stats.value_unit)
                ));
                lines.push(format!(
                    "Region min/max: {} / {}",
                    format_numeric_value_with_unit(stats.min, &stats.value_unit),
                    format_numeric_value_with_unit(stats.max, &stats.value_unit)
                ));
            }
        }
        let saved_region_names = self.view.saved_region_names();
        if !saved_region_names.is_empty() {
            lines.push(format!("Saved regions: {}", saved_region_names.join(", ")));
        }
        if let Some(name) = self.active_region_definition_name.as_deref() {
            lines.push(format!("Persisted region: {name}"));
        }
        Ok(lines)
    }

    fn content_lines(&self) -> Result<Vec<String>, ImageError> {
        let mut sections = self.view.metadata_sections()?;
        let filtered = match self.active_view {
            ImageBrowserView::Metadata => {
                filter_sections(&sections, &["Summary", "Observation", "Axes", "Misc"])
            }
            ImageBrowserView::Coordinates => {
                if let Some(active_cursor) = self.active_cursor_section()? {
                    sections.insert(0, active_cursor);
                }
                filter_sections(&sections, &["Active Cursor", "Coordinates", "Axes"])
            }
            ImageBrowserView::Spectrum => Vec::new(),
            ImageBrowserView::Plane => Vec::new(),
        };
        Ok(flatten_sections(&filtered))
    }

    fn all_content_lines(
        &self,
        plane_raster: Option<&PlaneRaster>,
        profile: Option<&ImageProfile>,
    ) -> Result<Vec<String>, ImageError> {
        if self.active_view == ImageBrowserView::Spectrum {
            Ok(render_profile_lines(profile))
        } else if self.active_view == ImageBrowserView::Plane
            && self.plane_content_mode == ImagePlaneContentMode::Spreadsheet
        {
            let Some(raster) = plane_raster else {
                return Ok(Vec::new());
            };
            self.plane_content_lines(raster)
        } else {
            self.content_lines()
        }
    }

    fn plane_content_lines(&self, _raster: &PlaneRaster) -> Result<Vec<String>, ImageError> {
        self.view.render_plane_value_grid_with_window_and_axes(
            (
                self.viewport.width.max(1) as usize,
                self.viewport.height.max(1) as usize,
            ),
            &self.window,
            &self.non_display_indices,
            (self.cursor_x, self.cursor_y),
        )
    }

    fn selected_profile_axis(&self) -> Option<usize> {
        self.selected_profile_axis
            .filter(|axis| self.view.axis_model().non_display_axes.contains(axis))
            .or_else(|| self.view.preferred_profile_axis())
    }

    fn active_cursor_section(&self) -> Result<Option<ImageMetadataSection>, ImageError> {
        if !self.view.capabilities().renderable_plane {
            return Ok(None);
        }
        let probe = self.view.probe_with_window_and_axes(
            (self.cursor_x, self.cursor_y),
            &self.window,
            &self.non_display_indices,
        )?;
        let mut lines = vec![
            format!("pixel: {}", join_usize_list(&probe.pixel_indices)),
            format!(
                "value: {}",
                format_probe_value(&probe, self.view.brightness_unit())
            ),
        ];
        if probe.masked {
            lines.push("masked: true".into());
        }
        if !probe.finite {
            lines.push("finite: false".into());
        }
        for axis in &probe.world_axes {
            lines.push(format_world_axis_line(axis));
        }
        Ok(Some(ImageMetadataSection {
            title: "Active Cursor".into(),
            lines,
        }))
    }

    fn parameter_state(&self) -> ImageBrowserParameters {
        ImageBrowserParameters {
            blc: self.window.format_blc(),
            trc: self.window.format_trc(),
            inc: self.window.format_inc(),
            stretch: stretch_preset_name(self.stretch.preset).into(),
            autoscale: autoscale_mode_name(self.stretch.autoscale).into(),
            clip_low: self
                .stretch
                .manual_clip
                .map(|(low, _)| trim_float_text(format!("{low:.6}")))
                .unwrap_or_default(),
            clip_high: self
                .stretch
                .manual_clip
                .map(|(_, high)| trim_float_text(format!("{high:.6}")))
                .unwrap_or_default(),
        }
    }

    fn plane_pixel_viewport(&self) -> (usize, usize) {
        (
            usize::from(if self.viewport.plane_pixel_width > 0 {
                self.viewport.plane_pixel_width
            } else {
                self.viewport.width.max(1)
            }),
            usize::from(if self.viewport.plane_pixel_height > 0 {
                self.viewport.plane_pixel_height
            } else {
                self.viewport.height.max(1)
            }),
        )
    }

    fn cached_plane_raster(
        &mut self,
        clip_override: Option<(f64, f64)>,
        backend_timing: Option<&mut ImageBackendTimingState>,
    ) -> Result<PlaneRaster, ImageError> {
        let key = self.plane_cache_key(self.non_display_indices.clone(), clip_override);
        let lookup_started_at = Instant::now();
        if let Some(raster) = self.plane_cache.get(&key) {
            if let Some(timing) = backend_timing {
                timing.plane_cache_result = if self.prefetched_plane_keys.contains(&key) {
                    ImageBackendPlaneCacheResult::PrefetchHit
                } else {
                    ImageBackendPlaneCacheResult::Hit
                };
                timing.cached_plane_lookup_ns = duration_ns(lookup_started_at.elapsed());
                timing.total_plane_ns = timing.cached_plane_lookup_ns;
            }
            return Ok(raster);
        }
        self.prefetched_plane_keys.remove(&key);
        let (raster, telemetry) = self
            .view
            .render_plane_with_window_and_axes_and_stretch_timed(
                self.plane_pixel_viewport(),
                &self.window,
                &self.non_display_indices,
                &self.stretch.plane_settings(),
                clip_override,
                self.region.as_ref(),
            )?;
        if let Some(timing) = backend_timing {
            apply_plane_timing(
                timing,
                ImageBackendPlaneCacheResult::Miss,
                duration_ns(lookup_started_at.elapsed()),
                telemetry,
            );
        }
        self.plane_cache.insert(key, raster.clone());
        Ok(raster)
    }

    fn cached_profile(
        &mut self,
        pixel_xy: (usize, usize),
        profile_axis: usize,
        region: Option<&ImageRegion>,
        backend_timing: Option<&mut ImageBackendTimingState>,
    ) -> Result<ImageProfile, ImageError> {
        let key = self.profile_cache_key(pixel_xy, profile_axis, region);
        if let Some(mut profile) = self.profile_cache.get(&key) {
            self.profile_perf.cache_hits = self.profile_perf.cache_hits.saturating_add(1);
            if let Some(timing) = backend_timing {
                timing.profile_cache_hits = self.profile_perf.cache_hits;
                timing.profile_cache_misses = self.profile_perf.cache_misses;
                timing.profile_extract_total_ns = self.profile_perf.extract_total_ns;
            }
            profile.selected_sample_index = self.current_profile_selected_index(profile_axis);
            return Ok(profile);
        }
        self.profile_perf.cache_misses = self.profile_perf.cache_misses.saturating_add(1);
        let normalized_non_display_indices =
            self.normalized_profile_non_display_indices(profile_axis);
        let extract_started_at = Instant::now();
        let mut profile = if let Some(region) = region {
            if let Some(profile) = self.view.region_profile_with_window_and_axes(
                region,
                &self.window,
                &normalized_non_display_indices,
                profile_axis,
            )? {
                profile
            } else {
                self.view.profile_with_window_and_axes(
                    pixel_xy,
                    &self.window,
                    &normalized_non_display_indices,
                    profile_axis,
                )?
            }
        } else {
            self.view.profile_with_window_and_axes(
                pixel_xy,
                &self.window,
                &normalized_non_display_indices,
                profile_axis,
            )?
        };
        self.profile_perf.extract_total_ns = self
            .profile_perf
            .extract_total_ns
            .saturating_add(duration_ns(extract_started_at.elapsed()));
        profile.selected_sample_index = self.current_profile_selected_index(profile_axis);
        self.profile_cache.insert(key, profile.clone());
        if let Some(timing) = backend_timing {
            timing.profile_cache_hits = self.profile_perf.cache_hits;
            timing.profile_cache_misses = self.profile_perf.cache_misses;
            timing.profile_extract_total_ns = self.profile_perf.extract_total_ns;
        }
        Ok(profile)
    }

    fn plane_cache_key(
        &self,
        non_display_indices: Vec<usize>,
        clip_override: Option<(f64, f64)>,
    ) -> PlaneCacheKey {
        PlaneCacheKey {
            viewport: self.plane_pixel_viewport(),
            blc: self.window.blc().to_vec(),
            trc: self.window.trc().to_vec(),
            inc: self.window.inc().to_vec(),
            non_display_indices,
            stretch_preset: self.stretch.preset,
            autoscale: self.stretch.autoscale,
            manual_clip: self
                .stretch
                .manual_clip
                .map(|(low, high)| (low.to_bits(), high.to_bits())),
            clip_override: clip_override.map(|(low, high)| (low.to_bits(), high.to_bits())),
            region_revision: self.region_revision,
            mask_revision: self.mask_revision,
        }
    }

    fn profile_cache_key(
        &self,
        pixel_xy: (usize, usize),
        profile_axis: usize,
        region: Option<&ImageRegion>,
    ) -> ProfileCacheKey {
        ProfileCacheKey {
            pixel_xy: if region.is_some() { (0, 0) } else { pixel_xy },
            blc: self.window.blc().to_vec(),
            trc: self.window.trc().to_vec(),
            inc: self.window.inc().to_vec(),
            normalized_non_display_indices: self
                .normalized_profile_non_display_indices(profile_axis),
            profile_axis,
            region_revision: region.map(|_| self.region_revision),
            mask_revision: self.mask_revision,
        }
    }

    fn normalized_profile_non_display_indices(&self, profile_axis: usize) -> Vec<usize> {
        let mut normalized = self.non_display_indices.clone();
        if let Some(position) = self
            .view
            .axis_model()
            .non_display_axes
            .iter()
            .position(|axis| *axis == profile_axis)
            && let Some(index) = normalized.get_mut(position)
        {
            *index = 0;
        }
        normalized
    }

    fn current_profile_selected_index(&self, profile_axis: usize) -> usize {
        self.view
            .axis_model()
            .non_display_axes
            .iter()
            .position(|axis| *axis == profile_axis)
            .and_then(|position| self.non_display_indices.get(position).copied())
            .unwrap_or_default()
    }

    fn drain_prefetched_planes(&mut self) {
        if let Some(worker) = self.prefetch_worker.as_mut() {
            let mut prefetched_keys = Vec::new();
            worker.drain_into(&mut self.plane_cache, &mut prefetched_keys);
            self.prefetched_plane_keys.extend(prefetched_keys);
        }
    }

    fn schedule_plane_prefetch(&mut self) {
        if !self.view.capabilities().renderable_plane {
            return;
        }
        let Some(selected_axis) = self.selected_profile_axis() else {
            return;
        };
        let Ok(non_display_axes) = self
            .view
            .non_display_axes_with_window(&self.window, &self.non_display_indices)
        else {
            return;
        };
        let Some(axis_state) = non_display_axes
            .into_iter()
            .find(|axis_state| axis_state.axis == selected_axis)
        else {
            return;
        };
        let Some(axis_position) = self
            .view
            .axis_model()
            .non_display_axes
            .iter()
            .position(|axis| *axis == axis_state.axis)
        else {
            return;
        };
        let clip_override = match self.stretch.autoscale {
            PlaneAutoscaleMode::PerPlane => None,
            PlaneAutoscaleMode::Frozen => self.frozen_clip_bounds,
        };
        let viewport = self.plane_pixel_viewport();
        let window = self.window.clone();
        let stretch = self.stretch.plane_settings();
        let region = self.region.clone();
        let preferred_direction = self
            .last_non_display_step
            .filter(|(axis, direction)| *axis == axis_state.axis && *direction != 0)
            .map(|(_, direction)| direction);
        let lookahead = self.resources.prefetch_lookahead_frames;
        let prefetch_offsets = if let Some(direction) = preferred_direction {
            let primary_direction = if direction < 0 { -1isize } else { 1isize };
            (1..=lookahead)
                .map(|offset| (offset, primary_direction))
                .collect::<Vec<_>>()
        } else {
            (0..lookahead)
                .map(|index| {
                    let offset = index / 2 + 1;
                    let direction = if index % 2 == 0 { -1isize } else { 1isize };
                    (offset, direction)
                })
                .collect::<Vec<_>>()
        };
        let plane_cache_keys = prefetch_offsets
            .into_iter()
            .filter_map(|(offset, direction)| {
                let next_index = if direction.is_negative() {
                    axis_state.index.checked_sub(offset)
                } else {
                    axis_state.index.checked_add(offset)
                }?;
                (next_index < axis_state.length).then_some(next_index)
            })
            .map(|next_index| {
                let mut indices = self.non_display_indices.clone();
                if let Some(index) = indices.get_mut(axis_position) {
                    *index = next_index;
                }
                let key = self.plane_cache_key(indices.clone(), clip_override);
                PlanePrefetchRequest {
                    key,
                    viewport,
                    window: window.clone(),
                    non_display_indices: indices,
                    stretch: stretch.clone(),
                    clip_override,
                    region: region.clone(),
                }
            })
            .collect::<Vec<_>>();
        let Some(worker) = self.prefetch_worker.as_mut() else {
            return;
        };
        for request in plane_cache_keys {
            worker.submit(request, &self.plane_cache);
        }
    }

    fn set_view_window(&mut self, parameters: &ImageBrowserParameters) -> Result<(), ImageError> {
        let old_display_pixels = self.current_display_pixels();
        let old_non_display_pixels = self.current_non_display_pixels();
        let stretch = parse_stretch_parameters(parameters)?;
        let window =
            self.view
                .window_from_text(&parameters.blc, &parameters.trc, &parameters.inc)?;
        self.stretch = stretch;
        self.frozen_clip_bounds = None;
        self.window = window;
        self.clamp_cursor_to_window(old_display_pixels, old_non_display_pixels, None, None);
        Ok(())
    }

    fn plane_display_lines(&self, raster: Option<&PlaneRaster>) -> Vec<String> {
        let mut lines = vec![
            format!("Stretch: {}", stretch_preset_label(self.stretch.preset)),
            format!(
                "Autoscale: {}",
                match self.stretch.autoscale {
                    PlaneAutoscaleMode::PerPlane => "per-plane",
                    PlaneAutoscaleMode::Frozen => "frozen",
                }
            ),
        ];
        if let Some((low, high)) = self.stretch.manual_clip {
            lines.push(format!(
                "Manual clip: {} .. {}",
                format_numeric_value_with_unit(low, self.view.brightness_unit()),
                format_numeric_value_with_unit(high, self.view.brightness_unit()),
            ));
        }
        if let Some(raster) = raster
            && !raster.no_finite_values
        {
            lines.push(format!(
                "Display clip: {} .. {}",
                format_numeric_value_with_unit(raster.clip_min, &raster.value_unit),
                format_numeric_value_with_unit(raster.clip_max, &raster.value_unit),
            ));
            lines.push(format!(
                "Plane range: {} .. {}",
                format_numeric_value_with_unit(raster.data_min, &raster.value_unit),
                format_numeric_value_with_unit(raster.data_max, &raster.value_unit),
            ));
        }
        lines
    }

    fn set_cursor_pixels(&mut self, x: usize, y: usize) {
        let Some(display_axes) = self.view.axis_model().display_axes else {
            return;
        };
        self.cursor_x = self.window.nearest_sample_index(display_axes[0], x);
        self.cursor_y = self.window.nearest_sample_index(display_axes[1], y);
    }

    fn set_selected_profile_axis(&mut self, axis: usize) -> Result<(), ImageError> {
        if !self.view.axis_model().non_display_axes.contains(&axis) {
            return Err(ImageError::InvalidMetadata(format!(
                "axis {axis} is not a non-display axis for this image"
            )));
        }
        self.selected_profile_axis = Some(axis);
        Ok(())
    }

    fn region_state(&self) -> Result<Option<ImageRegionState>, ImageError> {
        let Some(region) = self.region.as_ref() else {
            return Ok(None);
        };
        let overlay = self.view.region_overlay_with_window_and_axes(
            region,
            &self.window,
            &self.non_display_indices,
        )?;
        let stats = self.view.region_stats_with_window_and_axes(
            region,
            &self.window,
            &self.non_display_indices,
        )?;
        let active_shape_vertices = region
            .shapes
            .iter()
            .rev()
            .find(|shape| !shape.closed)
            .map(|shape| shape.vertices.len())
            .unwrap_or(0);
        Ok(Some(ImageRegionState {
            label: region.label.clone(),
            shape_count: region.shapes.len(),
            closed_shape_count: region.shapes.iter().filter(|shape| shape.closed).count(),
            editing: region.shapes.iter().any(|shape| !shape.closed),
            active_shape_vertices,
            overlay_shapes: overlay
                .shapes
                .into_iter()
                .map(map_region_overlay_shape)
                .collect(),
            stats: stats.map(map_region_stats),
        }))
    }

    fn start_region_shape(&mut self) -> Result<(), ImageError> {
        let Some(display_axes) = self.view.axis_model().display_axes else {
            return Err(ImageError::InvalidMetadata(
                "regions require a renderable plane".into(),
            ));
        };
        if !self.view.capabilities().world_coords_available {
            return Err(ImageError::InvalidMetadata(
                "regions require world-coordinate support".into(),
            ));
        }
        if self
            .region
            .as_ref()
            .is_some_and(|region| region.shapes.iter().any(|shape| !shape.closed))
        {
            return Err(ImageError::InvalidMetadata(
                "close or cancel the current polygon before starting another".into(),
            ));
        }
        if self.region.is_none() {
            self.region = Some(self.view.default_region("Region 1")?);
            self.active_region_definition_name = None;
        }
        let label = self
            .region
            .as_ref()
            .map(|region| region.label.clone())
            .unwrap_or_else(|| "Region 1".into());
        if self
            .region
            .as_ref()
            .is_some_and(|region| region.display_axes != display_axes)
        {
            self.region = Some(self.view.default_region(label)?);
            self.active_region_definition_name = None;
        }
        self.region
            .as_mut()
            .expect("region available")
            .start_shape()?;
        self.region_revision = self.region_revision.saturating_add(1);
        Ok(())
    }

    fn set_selection_references(
        &mut self,
        region: Option<ImageRegionReference>,
        mask: Option<ImageMaskReference>,
    ) -> Result<(), ImageError> {
        if matches!(mask.as_ref(), Some(ImageMaskReference::Expression { .. })) {
            return Err(ImageError::InvalidMetadata(
                "imexplore mask expressions are not supported; use a named persistent mask"
                    .to_string(),
            ));
        }

        let prepared_region = region
            .as_ref()
            .map(|reference| self.prepare_region_reference(reference))
            .transpose()?;

        if let Some(mask) = mask {
            match &mask {
                ImageMaskReference::None => self.unset_default_mask()?,
                ImageMaskReference::Name { name } => self.set_default_mask(name)?,
                ImageMaskReference::Expression { .. } => unreachable!(
                    "mask expressions are rejected before applying either selection reference"
                ),
            }
            self.mask_reference = mask;
        }

        if let Some(prepared) = prepared_region {
            self.region = prepared.region;
            self.active_region_definition_name = prepared.active_definition_name;
            if prepared.reset_saved_cycle {
                self.saved_region_cycle_index = 0;
            }
            self.region_revision = self.region_revision.saturating_add(1);
            self.region_reference = region.expect("a prepared region has its source reference");
        }
        Ok(())
    }

    fn prepare_region_reference(
        &self,
        reference: &ImageRegionReference,
    ) -> Result<PreparedRegionReference, ImageError> {
        match reference {
            ImageRegionReference::None => Ok(PreparedRegionReference {
                region: None,
                active_definition_name: None,
                reset_saved_cycle: false,
            }),
            ImageRegionReference::Definition { name } => Ok(PreparedRegionReference {
                region: Some(self.view.load_saved_region(name)?),
                active_definition_name: Some(name.clone()),
                reset_saved_cycle: true,
            }),
            ImageRegionReference::File { path } => {
                let (region, active_definition_name) = self.prepare_region_file(Path::new(path))?;
                Ok(PreparedRegionReference {
                    region: Some(region),
                    active_definition_name,
                    reset_saved_cycle: false,
                })
            }
            ImageRegionReference::Expression { expression } => Ok(PreparedRegionReference {
                region: Some(self.prepare_region_expression(expression)?),
                active_definition_name: None,
                reset_saved_cycle: false,
            }),
        }
    }

    fn append_region_vertex_pixels(&mut self, x: usize, y: usize) -> Result<(), ImageError> {
        self.set_cursor_pixels(x, y);
        let vertex = self.view.region_vertex_for_pixel_with_window_and_axes(
            (self.cursor_x, self.cursor_y),
            &self.window,
            &self.non_display_indices,
        )?;
        let Some(region) = self.region.as_mut() else {
            return Err(ImageError::InvalidMetadata(
                "start a region with R before adding vertices".into(),
            ));
        };
        if !region.append_vertex(vertex)? {
            return Ok(());
        }
        self.region_revision = self.region_revision.saturating_add(1);
        Ok(())
    }

    fn close_region_shape(&mut self) -> Result<(), ImageError> {
        let Some(region) = self.region.as_mut() else {
            return Err(ImageError::InvalidMetadata("no active region".into()));
        };
        region.close_active_shape()?;
        self.region_revision = self.region_revision.saturating_add(1);
        Ok(())
    }

    fn undo_region_vertex(&mut self) -> Result<(), ImageError> {
        let Some(region) = self.region.as_mut() else {
            return Err(ImageError::InvalidMetadata("no active region".into()));
        };
        if !region.undo_active_vertex()? {
            self.region = None;
            self.active_region_definition_name = None;
        }
        self.region_revision = self.region_revision.saturating_add(1);
        Ok(())
    }

    fn cancel_region_shape(&mut self) -> bool {
        let Some(region) = self.region.as_mut() else {
            return false;
        };
        if !region.cancel_active_shape() {
            return false;
        }
        if region.is_empty() {
            self.region = None;
            self.active_region_definition_name = None;
        }
        self.region_revision = self.region_revision.saturating_add(1);
        true
    }

    fn clear_region(&mut self) {
        if let Some(region) = self.region.as_mut() {
            region.clear();
        }
        self.region = None;
        self.active_region_definition_name = None;
        self.region_revision = self.region_revision.saturating_add(1);
    }

    fn save_region_definition(&mut self) -> Result<(), ImageError> {
        let Some(region) = self.region.as_ref() else {
            return Err(ImageError::InvalidMetadata("no active region".into()));
        };
        let name = self
            .view
            .save_region_definition(region, self.active_region_definition_name.as_deref())?;
        let mut persisted_region = region.clone();
        persisted_region.label = name.clone();
        self.region = Some(persisted_region);
        self.active_region_definition_name = Some(name);
        self.saved_region_cycle_index = 0;
        self.region_revision = self.region_revision.saturating_add(1);
        Ok(())
    }

    fn load_next_region_definition(&mut self) -> Result<(), ImageError> {
        let names = self.view.saved_region_names();
        if names.is_empty() {
            return Err(ImageError::InvalidMetadata(
                "no saved region definitions for this image".into(),
            ));
        }
        let len = names.len();
        let mut last_error = None;
        for offset in 0..len {
            let index = (self.saved_region_cycle_index + offset) % len;
            let name = &names[index];
            self.saved_region_cycle_index = (index + 1) % len;
            match self.view.load_saved_region(name) {
                Ok(region) => {
                    self.region = Some(region);
                    self.active_region_definition_name = Some(name.clone());
                    self.region_revision = self.region_revision.saturating_add(1);
                    return Ok(());
                }
                Err(error) => last_error = Some(error),
            }
        }
        Err(last_error.unwrap_or_else(|| {
            ImageError::InvalidMetadata("no saved region definitions for this image".into())
        }))
    }

    fn load_region_definition(&mut self, name: &str) -> Result<(), ImageError> {
        let region = self.view.load_saved_region(name)?;
        self.region = Some(region);
        self.active_region_definition_name = Some(name.to_string());
        self.saved_region_cycle_index = 0;
        self.region_revision = self.region_revision.saturating_add(1);
        Ok(())
    }

    fn rename_region_definition(&mut self, name: &str, new_name: &str) -> Result<(), ImageError> {
        let renamed = self.view.rename_saved_region(name, new_name)?;
        if let Some(region) = self.region.as_mut() {
            if self.active_region_definition_name.as_deref() == Some(name) {
                region.label = renamed.clone();
            }
        }
        if self.active_region_definition_name.as_deref() == Some(name) {
            self.active_region_definition_name = Some(renamed);
        }
        self.saved_region_cycle_index = 0;
        self.region_revision = self.region_revision.saturating_add(1);
        Ok(())
    }

    fn delete_region_definition(&mut self, name: &str) -> Result<(), ImageError> {
        self.view.remove_saved_region(name)?;
        if self.active_region_definition_name.as_deref() == Some(name) {
            self.active_region_definition_name = None;
        }
        self.saved_region_cycle_index = 0;
        self.region_revision = self.region_revision.saturating_add(1);
        Ok(())
    }

    fn set_default_mask(&mut self, name: &str) -> Result<(), ImageError> {
        self.view.set_default_mask(name)?;
        self.mask_revision = self.mask_revision.saturating_add(1);
        Ok(())
    }

    fn unset_default_mask(&mut self) -> Result<(), ImageError> {
        self.view.unset_default_mask()?;
        self.mask_revision = self.mask_revision.saturating_add(1);
        Ok(())
    }

    fn delete_mask(&mut self, name: &str) -> Result<(), ImageError> {
        self.view.remove_mask(name)?;
        self.mask_revision = self.mask_revision.saturating_add(1);
        Ok(())
    }

    fn write_region_mask(
        &mut self,
        name: Option<&str>,
        set_default: bool,
    ) -> Result<(), ImageError> {
        let Some(region) = self.region.as_ref() else {
            return Err(ImageError::InvalidMetadata("no active region".into()));
        };
        if region.shapes.iter().all(|shape| !shape.closed) {
            return Err(ImageError::InvalidMetadata(
                "close the current polygon before creating a mask".into(),
            ));
        }
        let name = name
            .filter(|value| !value.trim().is_empty())
            .map(str::trim)
            .map(str::to_string)
            .unwrap_or_else(|| self.view.next_generated_region_mask_name());
        self.view.write_region_mask(region, &name, set_default)?;
        self.mask_revision = self.mask_revision.saturating_add(1);
        Ok(())
    }

    fn export_region_file(&self, path: &Path) -> Result<(), ImageError> {
        let Some(region) = self.region.as_ref() else {
            return Err(ImageError::InvalidMetadata("no active region".into()));
        };
        if region.shapes.iter().all(|shape| !shape.closed) {
            return Err(ImageError::InvalidMetadata(
                "close the current polygon before exporting a region file".into(),
            ));
        }
        let overlay = self.view.region_overlay_with_window_and_axes(
            region,
            &self.window,
            &self.non_display_indices,
        )?;
        let shape = overlay
            .shapes
            .iter()
            .position(|shape| shape.closed && !shape.vertices.is_empty())
            .ok_or_else(|| ImageError::InvalidMetadata("no closed region to export".into()))?;
        let overlay_shape = &overlay.shapes[shape];
        let region_shape = region
            .shapes
            .iter()
            .filter(|shape| shape.closed && !shape.vertices.is_empty())
            .nth(shape)
            .ok_or_else(|| ImageError::InvalidMetadata("no closed region to export".into()))?;
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent).map_err(|error| {
                ImageError::Io(format!(
                    "create region export directory {}: {error}",
                    parent.display()
                ))
            })?;
        }
        let pixel_vertices = overlay_shape
            .vertices
            .iter()
            .map(|(x, y)| {
                Ok((
                    rounded_region_pixel(*x, "x")?,
                    rounded_region_pixel(*y, "y")?,
                ))
            })
            .collect::<Result<Vec<_>, ImageError>>()?;
        let world_vertices = region_shape
            .vertices
            .iter()
            .map(|vertex| {
                [
                    crtf_quantity(vertex.world[0], &region.axis_units[0]),
                    crtf_quantity(vertex.world[1], &region.axis_units[1]),
                ]
            })
            .collect::<Vec<_>>();
        let crtf = if let Some((first, second)) = crtf_box_vertex_indices(&pixel_vertices) {
            let first = &world_vertices[first];
            let second = &world_vertices[second];
            format!(
                "#CRTFv0 CASA Region Text Format version 0\nbox[[{},{}],[{},{}]]\n",
                first[0], first[1], second[0], second[1]
            )
        } else {
            let vertices = world_vertices
                .iter()
                .map(|vertex| format!("[{},{}]", vertex[0], vertex[1]))
                .collect::<Vec<_>>();
            format!(
                "#CRTFv0 CASA Region Text Format version 0\npoly [{}]\n",
                vertices.join(", ")
            )
        };
        fs::write(path, crtf).map_err(|error| {
            ImageError::Io(format!("write region file {}: {error}", path.display()))
        })
    }

    fn read_region_file_shapes(
        &self,
        path: &Path,
    ) -> Result<(String, Vec<ImageRegionShape>), ImageError> {
        let text = fs::read_to_string(path).map_err(|error| {
            ImageError::Io(format!("read region file {}: {error}", path.display()))
        })?;
        let label = path
            .file_stem()
            .and_then(|value| value.to_str())
            .filter(|value| !value.trim().is_empty())
            .unwrap_or("Imported region");
        let mut shapes = Vec::new();
        for shape in crtf_region_shapes(&text)? {
            let vertices = shape
                .into_iter()
                .map(|vertex| self.crtf_vertex_to_region_vertex(vertex))
                .collect::<Result<Vec<_>, ImageError>>()?;
            if vertices.len() >= 3 {
                shapes.push(ImageRegionShape {
                    vertices,
                    closed: true,
                });
            }
        }
        if shapes.is_empty() {
            return Err(ImageError::InvalidMetadata(format!(
                "region file {} does not contain a supported CRTF box or polygon",
                path.display()
            )));
        }
        Ok((label.to_string(), shapes))
    }

    fn load_region_file(&mut self, path: &Path) -> Result<(), ImageError> {
        let (region, active_definition_name) = self.prepare_region_file(path)?;
        self.region = Some(region);
        self.active_region_definition_name = active_definition_name;
        self.region_revision = self.region_revision.saturating_add(1);
        Ok(())
    }

    fn prepare_region_file(
        &self,
        path: &Path,
    ) -> Result<(ImageRegion, Option<String>), ImageError> {
        let (label, shapes) = self.read_region_file_shapes(path)?;
        let mut region = self.view.default_region(&label)?;
        region.shapes = shapes;
        let active_definition_name = path
            .file_name()
            .and_then(|value| value.to_str())
            .map(str::to_string);
        Ok((region, active_definition_name))
    }

    fn prepare_region_expression(&self, expression: &str) -> Result<ImageRegion, ImageError> {
        let mut shapes = Vec::new();
        for shape in crtf_region_shapes(expression)? {
            let vertices = shape
                .into_iter()
                .map(|vertex| self.crtf_vertex_to_region_vertex(vertex))
                .collect::<Result<Vec<_>, ImageError>>()?;
            if vertices.len() >= 3 {
                shapes.push(ImageRegionShape {
                    vertices,
                    closed: true,
                });
            }
        }
        if shapes.is_empty() {
            return Err(ImageError::InvalidMetadata(
                "inline region does not contain a supported CRTF box or polygon".to_string(),
            ));
        }
        let mut region = self.view.default_region("Inline region")?;
        region.shapes = shapes;
        Ok(region)
    }

    fn append_region_file(&mut self, path: &Path) -> Result<(), ImageError> {
        if self
            .region
            .as_ref()
            .is_some_and(|region| region.shapes.iter().any(|shape| !shape.closed))
        {
            return Err(ImageError::InvalidMetadata(
                "close or cancel the current polygon before loading a region file".into(),
            ));
        }
        let (label, mut shapes) = self.read_region_file_shapes(path)?;
        if let Some(region) = self.region.as_mut() {
            region.shapes.append(&mut shapes);
            self.active_region_definition_name = None;
        } else {
            let mut region = self.view.default_region(&label)?;
            region.shapes.append(&mut shapes);
            self.region = Some(region);
            self.active_region_definition_name = path
                .file_name()
                .and_then(|value| value.to_str())
                .map(str::to_string);
        }
        self.region_revision = self.region_revision.saturating_add(1);
        Ok(())
    }

    fn crtf_vertex_to_region_vertex(
        &self,
        vertex: [CrtfRegionCoordinate; 2],
    ) -> Result<ImageRegionVertex, ImageError> {
        match vertex {
            [
                CrtfRegionCoordinate::Pixel(x),
                CrtfRegionCoordinate::Pixel(y),
            ] => {
                let Some(display_axes) = self.view.axis_model().display_axes else {
                    return Err(ImageError::InvalidMetadata(
                        "regions require a renderable plane".into(),
                    ));
                };
                let sampled = (
                    self.window.nearest_sample_index(display_axes[0], x),
                    self.window.nearest_sample_index(display_axes[1], y),
                );
                self.view.region_vertex_for_pixel_with_window_and_axes(
                    sampled,
                    &self.window,
                    &self.non_display_indices,
                )
            }
            [
                CrtfRegionCoordinate::World(x),
                CrtfRegionCoordinate::World(y),
            ] => Ok(ImageRegionVertex { world: [x, y] }),
            _ => Err(ImageError::InvalidMetadata(
                "cannot mix pixel and world coordinates in one CRTF vertex".to_string(),
            )),
        }
    }

    fn current_display_pixels(&self) -> Option<(usize, usize)> {
        let display_axes = self.view.axis_model().display_axes?;
        Some((
            self.window
                .sampled_axis_value(display_axes[0], self.cursor_x)
                .unwrap_or(self.window.blc()[display_axes[0]]),
            self.window
                .sampled_axis_value(display_axes[1], self.cursor_y)
                .unwrap_or(self.window.blc()[display_axes[1]]),
        ))
    }

    fn current_non_display_pixels(&self) -> Option<Vec<usize>> {
        self.view
            .axis_model()
            .non_display_axes
            .iter()
            .copied()
            .zip(self.non_display_indices.iter().copied())
            .map(|(axis, index)| self.window.sampled_axis_value(axis, index))
            .collect()
    }

    fn clamp_cursor_to_window(
        &mut self,
        old_display_pixels: Option<(usize, usize)>,
        old_non_display_pixels: Option<Vec<usize>>,
        default_display_pixels: Option<(usize, usize)>,
        default_non_display_pixels: Option<Vec<usize>>,
    ) {
        if let Some(display_axes) = self.view.axis_model().display_axes {
            let (old_x, old_y) = old_display_pixels.or(default_display_pixels).unwrap_or((
                self.window.blc()[display_axes[0]],
                self.window.blc()[display_axes[1]],
            ));
            self.cursor_x = self.window.nearest_sample_index(display_axes[0], old_x);
            self.cursor_y = self.window.nearest_sample_index(display_axes[1], old_y);
        } else {
            self.cursor_x = 0;
            self.cursor_y = 0;
        }
        let old_pixels = old_non_display_pixels.unwrap_or_default();
        let default_pixels = default_non_display_pixels.unwrap_or_default();
        self.non_display_indices = self
            .view
            .axis_model()
            .non_display_axes
            .iter()
            .enumerate()
            .map(|(position, &axis)| {
                let pixel = old_pixels
                    .get(position)
                    .copied()
                    .or_else(|| default_pixels.get(position).copied())
                    .unwrap_or(self.window.blc()[axis]);
                self.window.nearest_sample_index(axis, pixel)
            })
            .collect();
        if self.non_display_indices.len() < self.view.axis_model().non_display_axes.len() {
            self.non_display_indices
                .resize(self.view.axis_model().non_display_axes.len(), 0);
        }
    }
}

fn centered_display_pixels(
    view: &OpenedImageView,
    window: &ImageViewWindow,
) -> Option<(usize, usize)> {
    let display_axes = view.axis_model().display_axes?;
    Some((
        centered_sample_pixel(window, display_axes[0]),
        centered_sample_pixel(window, display_axes[1]),
    ))
}

fn centered_non_display_pixels(view: &OpenedImageView, window: &ImageViewWindow) -> Vec<usize> {
    view.axis_model()
        .non_display_axes
        .iter()
        .map(|&axis| centered_sample_pixel(window, axis))
        .collect()
}

fn default_plane_content_mode(viewport: ImageBrowserViewport) -> ImagePlaneContentMode {
    if viewport.plane_pixel_width > 0 || viewport.plane_pixel_height > 0 {
        ImagePlaneContentMode::Raster
    } else {
        ImagePlaneContentMode::Spreadsheet
    }
}

fn centered_sample_pixel(window: &ImageViewWindow, axis: usize) -> usize {
    let center_index = window.sampled_axis_len(axis) / 2;
    window
        .sampled_axis_value(axis, center_index)
        .unwrap_or(window.blc()[axis])
}

fn cycle_view(
    available: &[ImageBrowserView],
    current: ImageBrowserView,
    forward: bool,
) -> ImageBrowserView {
    let index = available
        .iter()
        .position(|view| *view == current)
        .unwrap_or(0);
    let next = if forward {
        (index + 1) % available.len()
    } else if index == 0 {
        available.len() - 1
    } else {
        index - 1
    };
    available[next]
}

fn clip_lines(lines: &[String], offset: usize, height: usize) -> Vec<String> {
    lines
        .iter()
        .skip(offset)
        .take(height.max(1))
        .cloned()
        .collect()
}

fn filter_sections(
    sections: &[ImageMetadataSection],
    titles: &[&str],
) -> Vec<ImageMetadataSection> {
    sections
        .iter()
        .filter(|section| titles.contains(&section.title.as_str()))
        .cloned()
        .collect()
}

fn flatten_sections(sections: &[ImageMetadataSection]) -> Vec<String> {
    let mut lines = Vec::new();
    for (index, section) in sections.iter().enumerate() {
        if index > 0 {
            lines.push(String::new());
        }
        lines.push(format!("== {} ==", section.title));
        lines.extend(section.lines.clone());
    }
    lines
}

fn render_profile_lines(profile: Option<&ImageProfile>) -> Vec<String> {
    let Some(profile) = profile else {
        return vec!["No non-display axis available for spectrum/profile view.".into()];
    };
    let value_header = if profile.value_unit.is_empty() {
        "value".to_string()
    } else {
        format!("value [{}]", profile.value_unit)
    };
    let mut lines = vec![
        format!(
            "Profile axis: {} ({}) [{}]",
            profile.axis_name, profile.axis, profile.coord_type
        ),
        format!(
            "Selected sample: {}",
            format_profile_sample(
                profile.samples.get(profile.selected_sample_index),
                &profile.value_unit,
            )
        ),
        String::new(),
        format!("sel idx pixel coord {value_header}"),
    ];
    lines.extend(profile.samples.iter().map(|sample| {
        let marker = if sample.sample_index == profile.selected_sample_index {
            ">"
        } else {
            " "
        };
        format!(
            "{marker} {:>3} {:>5} {:<28} {}",
            sample.sample_index,
            sample.pixel_index,
            format_profile_world_value(sample),
            format_profile_value(sample, &profile.value_unit, true, false),
        )
    }));
    lines
}

fn format_profile_sample(sample: Option<&ImageProfileSample>, value_unit: &str) -> String {
    let Some(sample) = sample else {
        return "<none>".into();
    };
    format!(
        "idx={} pixel={} world={} value={}",
        sample.sample_index,
        sample.pixel_index,
        format_profile_world_value(sample),
        format_profile_value(sample, value_unit, true, true),
    )
}

fn format_profile_world_value(sample: &ImageProfileSample) -> String {
    sample
        .world_axis
        .as_ref()
        .map(|axis| format_world_axis_value(&axis.name, &axis.unit, axis.value))
        .unwrap_or_else(|| format!("pixel {}", sample.pixel_index))
}

fn format_profile_value(
    sample: &ImageProfileSample,
    value_unit: &str,
    scientific_preferred: bool,
    include_unit: bool,
) -> String {
    if sample.masked {
        "masked".into()
    } else {
        format_pixel_value(
            sample.value,
            sample.finite,
            value_unit,
            scientific_preferred,
            include_unit,
        )
    }
}

fn format_probe_value(probe: &ImageProbe, value_unit: &str) -> String {
    if probe.masked {
        "masked".into()
    } else {
        format_pixel_value(probe.value, probe.finite, value_unit, false, true)
    }
}

fn format_pixel_value(
    value: f64,
    finite: bool,
    value_unit: &str,
    scientific_preferred: bool,
    include_unit: bool,
) -> String {
    let text = if !finite {
        value.to_string()
    } else if scientific_preferred || (value != 0.0 && (value.abs() < 1e-3 || value.abs() >= 1e4)) {
        format!("{value:.6e}")
    } else {
        trim_float_text(format!("{value:.6}"))
    };
    if include_unit && !value_unit.is_empty() {
        format!("{text} {value_unit}")
    } else {
        text
    }
}

fn trim_float_text(mut text: String) -> String {
    while text.contains('.') && text.ends_with('0') {
        text.pop();
    }
    if text.ends_with('.') {
        text.pop();
    }
    if text == "-0" { "0".into() } else { text }
}

fn map_capabilities(capabilities: &ImageViewCapabilities) -> ImageBrowserCapabilities {
    ImageBrowserCapabilities {
        renderable_plane: capabilities.renderable_plane,
        world_coords_available: capabilities.world_coords_available,
        pixel_only_mode: capabilities.pixel_only_mode,
        non_display_axis_selectors: capabilities.non_display_axis_selectors,
        mask_present: capabilities.mask_present,
        complex_unsupported: capabilities.complex_unsupported,
    }
}

fn map_plane_raster(raster: PlaneRaster) -> ImagePlaneRaster {
    ImagePlaneRaster {
        width: raster.width,
        height: raster.height,
        pixels_u8: raster.pixels_u8,
        clip_min: raster.clip_min,
        clip_max: raster.clip_max,
        data_min: raster.data_min,
        data_max: raster.data_max,
        value_unit: raster.value_unit,
        histogram_bins: raster.histogram_bins,
        masked_or_non_finite_count: raster.masked_or_non_finite_count,
        no_finite_values: raster.no_finite_values,
    }
}

fn map_probe(probe: ImageProbe) -> ImageBrowserProbe {
    ImageBrowserProbe {
        pixel_indices: probe.pixel_indices,
        pixel_axes: probe.pixel_axes.into_iter().map(map_axis_value).collect(),
        value: probe.value,
        masked: probe.masked,
        finite: probe.finite,
        world_axes: probe.world_axes.into_iter().map(map_axis_value).collect(),
    }
}

fn map_profile(profile: &ImageProfile) -> ImageProfilePayload {
    ImageProfilePayload {
        axis: profile.axis,
        axis_name: profile.axis_name.clone(),
        axis_unit: profile.axis_unit.clone(),
        value_unit: profile.value_unit.clone(),
        coord_type: profile.coord_type.to_string(),
        selected_sample_index: profile.selected_sample_index,
        samples: profile.samples.iter().map(map_profile_sample).collect(),
    }
}

fn map_profile_sample(sample: &ImageProfileSample) -> ImageProfileSampleState {
    ImageProfileSampleState {
        sample_index: sample.sample_index,
        pixel_index: sample.pixel_index,
        value: sample.value,
        masked: sample.masked,
        finite: sample.finite,
        world_axis: sample.world_axis.clone().map(map_axis_value),
    }
}

fn map_axis_value(value: ImageAxisValue) -> ImageBrowserAxisValue {
    ImageBrowserAxisValue {
        name: value.name,
        unit: value.unit,
        value: value.value,
    }
}

fn map_display_axis(axis: ImageDisplayAxis) -> ImageDisplayAxisState {
    ImageDisplayAxisState {
        axis: axis.axis,
        name: axis.name,
        unit: axis.unit,
        blc: axis.blc,
        trc: axis.trc,
        inc: axis.inc,
        sampled_len: axis.sampled_len,
        world_increment: axis.world_increment,
    }
}

fn map_non_display_axis(axis: ImageNonDisplayAxis) -> ImageNonDisplayAxisState {
    ImageNonDisplayAxisState {
        axis: axis.axis,
        label: axis.name,
        index: axis.index,
        length: axis.length,
        pixel: axis.pixel,
    }
}

fn map_region_overlay_shape(shape: ImageRegionOverlayShape) -> ImageRegionOverlayShapeState {
    ImageRegionOverlayShapeState {
        vertices: shape
            .vertices
            .into_iter()
            .map(|(sampled_x, sampled_y)| ImageRegionOverlayVertex {
                sampled_x,
                sampled_y,
            })
            .collect(),
        closed: shape.closed,
    }
}

fn rounded_region_pixel(value: f64, axis: &str) -> Result<usize, ImageError> {
    if !value.is_finite() || value < -1.0e-6 {
        return Err(ImageError::InvalidMetadata(format!(
            "region {axis} vertex is not a non-negative finite pixel: {value}"
        )));
    }
    Ok(value.max(0.0).round() as usize)
}

#[cfg(test)]
fn crtf_box_from_vertices(vertices: &[(usize, usize)]) -> Option<(usize, usize, usize, usize)> {
    crtf_box_vertex_indices(vertices).map(|(first, second)| {
        let (x0, y0) = vertices[first];
        let (x1, y1) = vertices[second];
        (x0.min(x1), y0.min(y1), x0.max(x1), y0.max(y1))
    })
}

fn crtf_box_vertex_indices(vertices: &[(usize, usize)]) -> Option<(usize, usize)> {
    if vertices.len() != 4 {
        return None;
    }
    let mut xs = vertices.iter().map(|(x, _)| *x).collect::<Vec<_>>();
    xs.sort_unstable();
    xs.dedup();
    let mut ys = vertices.iter().map(|(_, y)| *y).collect::<Vec<_>>();
    ys.sort_unstable();
    ys.dedup();
    if xs.len() != 2 || ys.len() != 2 {
        return None;
    }
    let expected = [
        (xs[0], ys[0]),
        (xs[0], ys[1]),
        (xs[1], ys[0]),
        (xs[1], ys[1]),
    ];
    if expected.iter().all(|corner| vertices.contains(corner)) {
        let first = vertices
            .iter()
            .position(|vertex| *vertex == (xs[0], ys[0]))?;
        let second = vertices
            .iter()
            .position(|vertex| *vertex == (xs[1], ys[1]))?;
        Some((first, second))
    } else {
        None
    }
}

fn crtf_quantity(value: f64, unit: &str) -> String {
    let unit = unit.trim();
    if unit.is_empty() {
        format!("{value:.15}")
    } else {
        format!("{value:.15}{unit}")
    }
}

#[derive(Debug, Clone, Copy, PartialEq)]
enum CrtfRegionCoordinate {
    Pixel(usize),
    World(f64),
}

fn crtf_region_shapes(text: &str) -> Result<Vec<Vec<[CrtfRegionCoordinate; 2]>>, ImageError> {
    let mut shapes = Vec::new();
    for line in text.lines() {
        let line = line.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        let lowercase = line.to_ascii_lowercase();
        if lowercase.starts_with("box") {
            shapes.push(crtf_box_region_shape(line)?);
        } else if lowercase.starts_with("poly") {
            shapes.push(crtf_poly_region_shape(line)?);
        }
    }
    Ok(shapes)
}

fn crtf_box_region_shape(text: &str) -> Result<Vec<[CrtfRegionCoordinate; 2]>, ImageError> {
    let parts = crtf_coordinate_parts(text).ok_or_else(|| {
        ImageError::InvalidMetadata(format!(
            "invalid CASA CRTF box region {text:?}: expected box[[x0,y0],[x1,y1]]"
        ))
    })?;
    if parts.len() != 4 {
        return Err(ImageError::InvalidMetadata(format!(
            "invalid CASA CRTF box region {text:?}: expected four coordinates"
        )));
    }
    let x0 = parse_crtf_region_coordinate(&parts[0])?;
    let y0 = parse_crtf_region_coordinate(&parts[1])?;
    let x1 = parse_crtf_region_coordinate(&parts[2])?;
    let y1 = parse_crtf_region_coordinate(&parts[3])?;
    Ok(vec![[x0, y0], [x1, y0], [x1, y1], [x0, y1]])
}

fn crtf_poly_region_shape(text: &str) -> Result<Vec<[CrtfRegionCoordinate; 2]>, ImageError> {
    let parts = crtf_coordinate_parts(text).ok_or_else(|| {
        ImageError::InvalidMetadata(format!(
            "invalid CASA CRTF polygon region {text:?}: expected poly [[x0,y0],...]"
        ))
    })?;
    if parts.len() < 6 || parts.len() % 2 != 0 {
        return Err(ImageError::InvalidMetadata(format!(
            "invalid CASA CRTF polygon region {text:?}: expected at least three coordinate pairs"
        )));
    }
    parts
        .chunks_exact(2)
        .map(|pair| {
            Ok([
                parse_crtf_region_coordinate(&pair[0])?,
                parse_crtf_region_coordinate(&pair[1])?,
            ])
        })
        .collect()
}

fn crtf_coordinate_parts(text: &str) -> Option<Vec<String>> {
    let start = text.find("[[")?;
    let end = text.rfind("]]")?;
    if end <= start {
        return None;
    }
    Some(
        text[start + 2..end]
            .replace("], [", ",")
            .replace("],[", ",")
            .replace(['[', ']'], "")
            .split(',')
            .map(str::trim)
            .filter(|part| !part.is_empty())
            .map(str::to_string)
            .collect(),
    )
}

fn parse_crtf_region_coordinate(text: &str) -> Result<CrtfRegionCoordinate, ImageError> {
    let trimmed = text.trim();
    let lowercase = trimmed.to_ascii_lowercase();
    if let Some(value) = lowercase.strip_suffix("pix") {
        let parsed = value.trim().parse::<f64>().map_err(|error| {
            ImageError::InvalidMetadata(format!(
                "invalid CRTF pixel coordinate {trimmed:?}: {error}"
            ))
        })?;
        return rounded_region_pixel(parsed, "CRTF").map(CrtfRegionCoordinate::Pixel);
    }
    let (value, scale) = if let Some(value) = lowercase.strip_suffix("arcsec") {
        (value, std::f64::consts::PI / 180.0 / 3600.0)
    } else if let Some(value) = lowercase.strip_suffix("arcmin") {
        (value, std::f64::consts::PI / 180.0 / 60.0)
    } else if let Some(value) = lowercase.strip_suffix("deg") {
        (value, std::f64::consts::PI / 180.0)
    } else if let Some(value) = lowercase.strip_suffix("rad") {
        (value, 1.0)
    } else {
        (lowercase.as_str(), 1.0)
    };
    let parsed = value.trim().parse::<f64>().map_err(|error| {
        ImageError::InvalidMetadata(format!(
            "invalid CRTF world coordinate {trimmed:?}: {error}"
        ))
    })?;
    if !parsed.is_finite() {
        return Err(ImageError::InvalidMetadata(format!(
            "invalid CRTF world coordinate {trimmed:?}: value is not finite"
        )));
    }
    Ok(CrtfRegionCoordinate::World(parsed * scale))
}

fn map_region_stats(stats: ImageRegionStats) -> ImageRegionStatsState {
    ImageRegionStatsState {
        pixel_count: stats.pixel_count,
        median: stats.median,
        min: stats.min,
        max: stats.max,
        mean: stats.mean,
        sigma: stats.sigma,
        rms: stats.rms,
        sum: stats.sum,
        value_unit: stats.value_unit,
    }
}

fn session_non_display_axis_count(view: &OpenedImageView) -> usize {
    view.axis_model().non_display_axes.len()
}

fn join_usize_list(values: &[usize]) -> String {
    values
        .iter()
        .map(|value| value.to_string())
        .collect::<Vec<_>>()
        .join(", ")
}

fn format_world_axis_line(axis: &ImageAxisValue) -> String {
    format!(
        "{}: {}",
        axis.name,
        format_world_axis_value(&axis.name, &axis.unit, axis.value)
    )
}

fn format_world_axis_value(axis_name: &str, unit: &str, value: f64) -> String {
    if axis_name.eq_ignore_ascii_case("Right Ascension") || axis_name.eq_ignore_ascii_case("RA") {
        return format_right_ascension_labeled(value, 6);
    }
    if axis_name.eq_ignore_ascii_case("Declination") || axis_name.eq_ignore_ascii_case("DEC") {
        return format_declination_labeled(value, 5);
    }
    if unit.is_empty() {
        format!("{value} unitless")
    } else {
        format_numeric_value_with_unit(value, unit)
    }
}

fn parse_stretch_parameters(
    parameters: &ImageBrowserParameters,
) -> Result<SessionStretchState, ImageError> {
    let preset = match parameters.stretch.trim() {
        "" | "percentile99" => PlaneStretchPreset::Percentile99,
        "percentile95" => PlaneStretchPreset::Percentile95,
        "minmax" => PlaneStretchPreset::MinMax,
        "zscale" => PlaneStretchPreset::ZScale,
        "manual" => PlaneStretchPreset::Manual,
        other => {
            return Err(ImageError::InvalidMetadata(format!(
                "unsupported stretch preset: {other}"
            )));
        }
    };
    let autoscale = match parameters.autoscale.trim() {
        "" | "per_plane" => PlaneAutoscaleMode::PerPlane,
        "frozen" => PlaneAutoscaleMode::Frozen,
        other => {
            return Err(ImageError::InvalidMetadata(format!(
                "unsupported autoscale mode: {other}"
            )));
        }
    };
    let clip_low = parse_optional_clip("clip_low", &parameters.clip_low)?;
    let clip_high = parse_optional_clip("clip_high", &parameters.clip_high)?;
    let manual_clip = match (clip_low, clip_high) {
        (Some(low), Some(high)) if low < high => Some((low, high)),
        (Some(_), Some(_)) => {
            return Err(ImageError::InvalidMetadata(
                "clip_low must be smaller than clip_high".into(),
            ));
        }
        (None, None) => None,
        _ => {
            return Err(ImageError::InvalidMetadata(
                "manual clip requires both clip_low and clip_high".into(),
            ));
        }
    };
    if preset == PlaneStretchPreset::Manual && manual_clip.is_none() {
        return Err(ImageError::InvalidMetadata(
            "manual stretch requires clip_low and clip_high".into(),
        ));
    }
    Ok(SessionStretchState {
        preset,
        autoscale,
        manual_clip,
    })
}

fn parse_optional_clip(field: &str, value: &str) -> Result<Option<f64>, ImageError> {
    let value = value.trim();
    if value.is_empty() {
        return Ok(None);
    }
    let parsed = value.parse::<f64>().map_err(|error| {
        ImageError::InvalidMetadata(format!("invalid {field} value '{value}': {error}"))
    })?;
    if !parsed.is_finite() {
        return Err(ImageError::InvalidMetadata(format!(
            "{field} must be finite"
        )));
    }
    Ok(Some(parsed))
}

fn stretch_preset_name(preset: PlaneStretchPreset) -> &'static str {
    match preset {
        PlaneStretchPreset::Percentile99 => "percentile99",
        PlaneStretchPreset::Percentile95 => "percentile95",
        PlaneStretchPreset::MinMax => "minmax",
        PlaneStretchPreset::ZScale => "zscale",
        PlaneStretchPreset::Manual => "manual",
    }
}

fn stretch_preset_label(preset: PlaneStretchPreset) -> &'static str {
    match preset {
        PlaneStretchPreset::Percentile99 => "percentile 1/99",
        PlaneStretchPreset::Percentile95 => "percentile 5/95",
        PlaneStretchPreset::MinMax => "min/max",
        PlaneStretchPreset::ZScale => "zscale-like",
        PlaneStretchPreset::Manual => "manual",
    }
}

fn autoscale_mode_name(mode: PlaneAutoscaleMode) -> &'static str {
    match mode {
        PlaneAutoscaleMode::PerPlane => "per_plane",
        PlaneAutoscaleMode::Frozen => "frozen",
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{Mutex, MutexGuard};

    use casa_coordinates::{
        CoordinateSystem, DirectionCoordinate, ObsInfo, Projection, ProjectionType,
        SpectralCoordinate,
    };
    use casa_types::ArrayD;
    use casa_types::measures::direction::DirectionRef;
    use casa_types::measures::epoch::{EpochRef, MEpoch};
    use casa_types::measures::frequency::FrequencyRef;
    use casa_types::measures::position::MPosition;
    use casars_imagebrowser_protocol::{
        ImageBackendPlaneCacheResult, ImageBrowserCommand, ImageBrowserParameters,
        ImageBrowserPreviewRequest, ImageBrowserViewport, ImageMaskReference,
        ImagePlaneContentMode, ImageRegionReference,
    };
    use ndarray::IxDyn;

    use super::*;
    use casa_images::image::PagedImage;
    use casa_images::image_view::ImageRegionShape;

    static PERF_ENV_LOCK: Mutex<()> = Mutex::new(());

    fn perf_env_lock() -> MutexGuard<'static, ()> {
        PERF_ENV_LOCK.lock().expect("perf env lock")
    }

    fn clear_perf_env() {
        unsafe {
            std::env::remove_var("CASARS_IMEXPLORE_PERF");
        }
    }

    fn casa_regionmanager_probe_unavailable(stderr: &[u8]) -> bool {
        let stderr = String::from_utf8_lossy(stderr);
        stderr.contains("No module named 'casatools'")
            || stderr.contains("No module named casatools")
            || (stderr.contains("ImportError")
                && (stderr.contains("casatools")
                    || stderr.contains("Library not loaded")
                    || stderr.contains("dlopen(")))
    }

    struct PerfEnvGuard;

    impl Drop for PerfEnvGuard {
        fn drop(&mut self) {
            clear_perf_env();
        }
    }

    fn set_perf_env() -> PerfEnvGuard {
        unsafe {
            std::env::set_var("CASARS_IMEXPLORE_PERF", "1");
        }
        PerfEnvGuard
    }

    fn cube_coords() -> CoordinateSystem {
        let mut coords = CoordinateSystem::new();
        coords.add_coordinate(DirectionCoordinate::new(
            DirectionRef::J2000,
            Projection::new(ProjectionType::SIN),
            [0.0, std::f64::consts::FRAC_PI_4],
            [-1e-4, 1e-4],
            [1.0, 1.0],
        ));
        coords.add_coordinate(SpectralCoordinate::new(
            FrequencyRef::LSRK,
            1.42e9,
            1.0e6,
            0.0,
            1.42040575e9,
        ));
        coords
    }

    fn direction_coords() -> CoordinateSystem {
        let mut coords = CoordinateSystem::new();
        coords.add_coordinate(DirectionCoordinate::new(
            DirectionRef::J2000,
            Projection::new(ProjectionType::SIN),
            [0.0, std::f64::consts::FRAC_PI_4],
            [-1e-4, 1e-4],
            [1.0, 1.0],
        ));
        coords
    }

    fn cube_coords_with_obs_info() -> CoordinateSystem {
        cube_coords().with_obs_info(
            ObsInfo::new("ALMA")
                .with_observer("Test Observer")
                .with_date(MEpoch::from_mjd(59000.25, EpochRef::UTC))
                .with_telescope_position(MPosition::new_itrf(
                    2_225_142.18,
                    -5_440_307.37,
                    -2_481_029.85,
                ))
                .with_pointing_center(0.0, std::f64::consts::FRAC_PI_4),
        )
    }

    #[test]
    fn session_open_resize_and_snapshot() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("session.image");
        let mut image =
            PagedImage::<f32>::create(vec![4, 4], CoordinateSystem::new(), &path).unwrap();
        image
            .put_slice(
                &ArrayD::from_shape_vec(
                    IxDyn(&[4, 4]),
                    (0..16).map(|value| value as f32).collect(),
                )
                .unwrap(),
                &[0, 0],
            )
            .unwrap();
        image.save().unwrap();

        let mut session =
            ImageBrowserSession::open(&path, ImageBrowserViewport::new(4, 4)).unwrap();
        let snapshot = session.snapshot().unwrap();
        assert_eq!(snapshot.active_view, ImageBrowserView::Plane);
        assert!(snapshot.plane.is_some());
        assert_eq!(snapshot.plane_cursor.as_ref().unwrap().pixel_x, 2);
        assert_eq!(snapshot.plane_cursor.as_ref().unwrap().pixel_y, 2);
        assert_eq!(snapshot.probe.as_ref().unwrap().pixel_indices, vec![2, 2]);

        let plane = session.snapshot().unwrap();
        assert_eq!(plane.active_view, ImageBrowserView::Plane);
        assert!(plane.plane.is_some());

        let resized = session
            .handle_command(ImageBrowserCommand::Resize {
                viewport: ImageBrowserViewport::new(2, 2),
            })
            .unwrap();
        assert_eq!(resized.plane.as_ref().unwrap().width, 2);

        let high_res = session
            .handle_command(ImageBrowserCommand::Resize {
                viewport: ImageBrowserViewport::with_plane_pixels(2, 2, 0, 16, 16),
            })
            .unwrap();
        assert_eq!(high_res.plane.as_ref().unwrap().width, 4);
        assert_eq!(high_res.plane.as_ref().unwrap().height, 4);
    }

    #[test]
    fn session_accepts_inline_regions_and_rejects_unapplied_mask_expressions() {
        let _guard = perf_env_lock();
        clear_perf_env();
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("selection-references.image");
        let mut image = PagedImage::<f32>::create(vec![3, 3], direction_coords(), &path).unwrap();
        image
            .put_slice(
                &ArrayD::from_shape_vec(IxDyn(&[3, 3]), (0..9).map(|value| value as f32).collect())
                    .unwrap(),
                &[0, 0],
            )
            .unwrap();
        image.save().unwrap();

        let region = ImageRegionReference::Expression {
            expression: "box[[0pix,0pix],[1pix,1pix]]".to_string(),
        };
        let mask = ImageMaskReference::Expression {
            expression: format!("{}>0.5", path.display()),
        };
        let mut session =
            ImageBrowserSession::open(&path, ImageBrowserViewport::new(16, 8)).unwrap();
        let snapshot = session
            .handle_command(ImageBrowserCommand::SetSelectionReferences {
                region: Some(region.clone()),
                mask: None,
            })
            .unwrap();

        assert_eq!(snapshot.region_reference, region);
        assert_eq!(
            snapshot.region.as_ref().expect("inline region").shape_count,
            1
        );
        let before = snapshot;
        let replacement_region = ImageRegionReference::Expression {
            expression: "box[[1pix,1pix],[2pix,2pix]]".to_string(),
        };
        let error = session
            .handle_command(ImageBrowserCommand::SetSelectionReferences {
                region: Some(replacement_region),
                mask: Some(mask),
            })
            .expect_err("an unapplied mask expression must not be accepted as durable state");
        assert!(error.to_string().contains("named persistent mask"));
        let after = session.snapshot().unwrap();
        assert_eq!(after, before);

        let before_missing_mask = after;
        let error = session
            .handle_command(ImageBrowserCommand::SetSelectionReferences {
                region: Some(ImageRegionReference::Expression {
                    expression: "box[[1pix,1pix],[2pix,2pix]]".to_string(),
                }),
                mask: Some(ImageMaskReference::Name {
                    name: "missing-mask".to_string(),
                }),
            })
            .expect_err("a missing named mask must reject the complete selection update");
        assert!(!error.to_string().is_empty());
        assert_eq!(session.snapshot().unwrap(), before_missing_mask);
    }

    #[test]
    fn preview_occurrence_does_not_mutate_visible_session_snapshot() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("preview.image");
        let mut image = PagedImage::<f32>::create(vec![2, 2, 3], cube_coords(), &path).unwrap();
        image
            .put_slice(
                &ArrayD::from_shape_vec(
                    IxDyn(&[2, 2, 3]),
                    (0..12).map(|value| value as f32).collect(),
                )
                .unwrap(),
                &[0, 0, 0],
            )
            .unwrap();
        image.save().unwrap();
        drop(image);

        let mut session =
            ImageBrowserSession::open(&path, ImageBrowserViewport::new(32, 12)).unwrap();
        let before = session.snapshot().unwrap();
        let before_index = before.non_display_axes[0].index;

        let preview = session
            .preview_occurrence(&ImageBrowserPreviewRequest {
                viewport: ImageBrowserViewport::new(32, 12),
                parameters: before.parameters.clone(),
                plane_content_mode: ImagePlaneContentMode::Raster,
                non_display_indices: vec![2],
                include_profile: true,
            })
            .unwrap();
        assert_eq!(preview.snapshot.non_display_axes[0].index, 2);

        let after = session.snapshot().unwrap();
        assert_eq!(after.non_display_axes[0].index, before_index);
    }

    #[test]
    fn preview_occurrence_preserves_cursor_for_profile_extraction() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("preview_cursor_profile.image");
        let mut image = PagedImage::<f32>::create(vec![2, 2, 3], cube_coords(), &path).unwrap();
        image
            .put_slice(
                &ArrayD::from_shape_vec(
                    IxDyn(&[2, 2, 3]),
                    vec![
                        1.0, 10.0, 100.0, 2.0, 20.0, 200.0, 3.0, 30.0, 300.0, 4.0, 40.0, 400.0,
                    ],
                )
                .unwrap(),
                &[0, 0, 0],
            )
            .unwrap();
        image.set_units("Jy/beam").unwrap();
        image.save().unwrap();
        drop(image);

        let mut session =
            ImageBrowserSession::open(&path, ImageBrowserViewport::new(80, 12)).unwrap();
        session
            .handle_command(ImageBrowserCommand::MoveCursor { dx: 1, dy: 1 })
            .unwrap();
        let before = session.snapshot().unwrap();
        assert_eq!(before.plane_cursor.as_ref().unwrap().pixel_x, 1);
        assert_eq!(before.plane_cursor.as_ref().unwrap().pixel_y, 1);

        let preview = session
            .preview_occurrence(&ImageBrowserPreviewRequest {
                viewport: ImageBrowserViewport::new(80, 12),
                parameters: before.parameters.clone(),
                plane_content_mode: ImagePlaneContentMode::Raster,
                non_display_indices: vec![2],
                include_profile: true,
            })
            .unwrap();

        assert_eq!(preview.snapshot.plane_cursor.as_ref().unwrap().pixel_x, 1);
        assert_eq!(preview.snapshot.plane_cursor.as_ref().unwrap().pixel_y, 1);
        assert_eq!(
            &preview.snapshot.probe.as_ref().unwrap().pixel_indices[..2],
            &[1, 1]
        );
        let profile = preview.snapshot.profile.as_ref().expect("preview profile");
        assert_eq!(profile.samples.len(), 3);
        assert_eq!(profile.samples[2].value, 400.0);
    }

    #[test]
    fn preview_occurrence_backend_timing_is_opt_in() {
        let _lock = perf_env_lock();
        clear_perf_env();

        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("preview_perf.image");
        let mut image = PagedImage::<f32>::create(vec![2, 2, 3], cube_coords(), &path).unwrap();
        image
            .put_slice(
                &ArrayD::from_shape_vec(
                    IxDyn(&[2, 2, 3]),
                    (0..12).map(|value| value as f32).collect(),
                )
                .unwrap(),
                &[0, 0, 0],
            )
            .unwrap();
        image.save().unwrap();
        drop(image);

        let request = ImageBrowserPreviewRequest {
            viewport: ImageBrowserViewport::new(32, 12),
            parameters: ImageBrowserParameters {
                blc: String::new(),
                trc: String::new(),
                inc: String::new(),
                stretch: "percentile99".to_string(),
                autoscale: "per_plane".to_string(),
                clip_low: String::new(),
                clip_high: String::new(),
            },
            plane_content_mode: ImagePlaneContentMode::Raster,
            non_display_indices: vec![1],
            include_profile: true,
        };

        let mut session =
            ImageBrowserSession::open(&path, ImageBrowserViewport::new(32, 12)).unwrap();
        let preview = session.preview_occurrence(&request).unwrap();
        assert!(preview.snapshot.backend_timing.is_none());

        let _perf_guard = set_perf_env();
        let mut session =
            ImageBrowserSession::open(&path, ImageBrowserViewport::new(32, 12)).unwrap();
        let preview = session.preview_occurrence(&request).unwrap();
        assert!(preview.snapshot.backend_timing.is_some());
    }

    #[test]
    fn preview_occurrence_skips_profile_work_when_profile_is_not_requested() {
        let _lock = perf_env_lock();
        let _perf_guard = set_perf_env();

        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("preview_without_profile.image");
        let mut image = PagedImage::<f32>::create(vec![2, 2, 3], cube_coords(), &path).unwrap();
        image
            .put_slice(
                &ArrayD::from_shape_vec(
                    IxDyn(&[2, 2, 3]),
                    (0..12).map(|value| value as f32).collect(),
                )
                .unwrap(),
                &[0, 0, 0],
            )
            .unwrap();
        image.save().unwrap();
        drop(image);

        let mut session =
            ImageBrowserSession::open(&path, ImageBrowserViewport::new(32, 12)).unwrap();
        let preview = session
            .preview_occurrence(&ImageBrowserPreviewRequest {
                viewport: ImageBrowserViewport::new(32, 12),
                parameters: ImageBrowserParameters {
                    blc: String::new(),
                    trc: String::new(),
                    inc: String::new(),
                    stretch: "percentile99".to_string(),
                    autoscale: "per_plane".to_string(),
                    clip_low: String::new(),
                    clip_high: String::new(),
                },
                plane_content_mode: ImagePlaneContentMode::Raster,
                non_display_indices: vec![1],
                include_profile: false,
            })
            .unwrap();

        assert!(preview.snapshot.profile.is_none());
        let timing = preview
            .snapshot
            .backend_timing
            .expect("backend timing should be present when perf is enabled");
        assert_eq!(timing.profile_cache_hits, 0);
        assert_eq!(timing.profile_cache_misses, 0);
        assert_eq!(timing.profile_extract_total_ns, 0);
    }

    #[test]
    fn raster_viewport_defaults_to_raster_plane_content_mode() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("session-raster.image");
        let mut image = PagedImage::<f32>::create(vec![4, 4], direction_coords(), &path).unwrap();
        image
            .put_slice(
                &ArrayD::from_shape_vec(
                    IxDyn(&[4, 4]),
                    (0..16).map(|value| value as f32).collect(),
                )
                .unwrap(),
                &[0, 0],
            )
            .unwrap();
        image.save().unwrap();

        let mut session = ImageBrowserSession::open(
            &path,
            ImageBrowserViewport::with_plane_pixels(80, 24, 0, 800, 600),
        )
        .unwrap();
        let snapshot = session.snapshot().unwrap();

        assert_eq!(session.plane_content_mode, ImagePlaneContentMode::Raster);
        assert!(snapshot.content_lines.is_empty());

        let toggled = session
            .handle_command(ImageBrowserCommand::SetPlaneContentMode {
                mode: ImagePlaneContentMode::Spreadsheet,
            })
            .unwrap();
        assert!(!toggled.content_lines.is_empty());
        assert!(toggled.content_lines.first().unwrap().contains("y/x"));
    }

    #[test]
    fn session_snapshot_prefetches_adjacent_planes_into_cache() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("prefetch.image");
        let mut image = PagedImage::<f32>::create(vec![4, 4, 5], cube_coords(), &path).unwrap();
        image
            .put_slice(
                &ArrayD::from_shape_vec(
                    IxDyn(&[4, 4, 5]),
                    (0..(4 * 4 * 5)).map(|value| value as f32).collect(),
                )
                .unwrap(),
                &[0, 0, 0],
            )
            .unwrap();
        image.save().unwrap();

        let mut session = ImageBrowserSession::open(
            &path,
            ImageBrowserViewport::with_plane_pixels(80, 24, 0, 800, 600),
        )
        .unwrap();
        let snapshot = session.snapshot().unwrap();
        assert!(snapshot.plane.is_some());
        assert!(!session.plane_cache.values.is_empty());

        std::thread::sleep(std::time::Duration::from_millis(50));
        session.drain_prefetched_planes();

        assert!(session.plane_cache.values.len() >= 2);
    }

    #[test]
    fn session_cycles_views_and_scrolls_text() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("text.image");
        let mut image =
            PagedImage::<f32>::create(vec![2, 2], CoordinateSystem::new(), &path).unwrap();
        image.save().unwrap();

        let mut session =
            ImageBrowserSession::open(&path, ImageBrowserViewport::new(40, 3)).unwrap();
        session
            .handle_command(ImageBrowserCommand::CycleView { forward: true })
            .unwrap();
        let coordinates = session
            .handle_command(ImageBrowserCommand::CycleView { forward: true })
            .unwrap();
        assert_eq!(coordinates.active_view, ImageBrowserView::Coordinates);
        assert!(!coordinates.content_lines.is_empty());

        let moved = session
            .handle_command(ImageBrowserCommand::MoveCursor { dx: 0, dy: 1 })
            .unwrap();
        assert_eq!(moved.active_view, ImageBrowserView::Coordinates);
    }

    #[test]
    fn metadata_view_includes_observation_section() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("observation-session.image");
        let mut image =
            PagedImage::<f32>::create(vec![2, 2, 2], cube_coords_with_obs_info(), &path).unwrap();
        image.save().unwrap();

        let mut session =
            ImageBrowserSession::open(&path, ImageBrowserViewport::new(80, 24)).unwrap();
        session
            .handle_command(ImageBrowserCommand::CycleView { forward: true })
            .unwrap();
        let snapshot = session
            .handle_command(ImageBrowserCommand::CycleView { forward: true })
            .unwrap();

        assert!(
            snapshot
                .content_lines
                .iter()
                .any(|line| line == "== Observation ==")
        );
        assert!(snapshot.content_lines.iter().any(|line| {
            line.contains("pointing center: 00:00:00.000000 hms, +45.00.00.00000 dms")
        }));
    }

    #[test]
    fn session_spectrum_view_renders_profile_lines() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("spectrum.image");
        let mut image = PagedImage::<f32>::create(vec![2, 2, 3], cube_coords(), &path).unwrap();
        image
            .put_slice(
                &ArrayD::from_shape_vec(
                    IxDyn(&[2, 2, 3]),
                    vec![
                        1.0, 10.0, 100.0, 2.0, 20.0, 200.0, 3.0, 30.0, 300.0, 4.0, 40.0, 400.0,
                    ],
                )
                .unwrap(),
                &[0, 0, 0],
            )
            .unwrap();
        image.set_units("Jy/beam").unwrap();
        image.save().unwrap();

        let mut session =
            ImageBrowserSession::open(&path, ImageBrowserViewport::new(80, 12)).unwrap();
        session
            .handle_command(ImageBrowserCommand::MoveCursor { dx: 1, dy: 1 })
            .unwrap();
        let snapshot = session
            .handle_command(ImageBrowserCommand::CycleView { forward: true })
            .unwrap();

        assert_eq!(snapshot.active_view, ImageBrowserView::Spectrum);
        assert!(
            snapshot
                .content_lines
                .iter()
                .any(|line| line.contains("Profile axis: Frequency (2) [Spectral]"))
        );
        assert!(
            snapshot
                .content_lines
                .iter()
                .any(|line| line.contains("Selected sample: idx=1"))
        );
        assert!(
            snapshot
                .inspector_lines
                .iter()
                .any(|line| line.contains("Plane cursor: x=1 y=1"))
        );
        assert!(
            snapshot
                .content_lines
                .iter()
                .any(|line| line.contains("value [Jy/beam]"))
        );
        assert!(
            snapshot
                .content_lines
                .iter()
                .any(|line| line.contains("4.000000e2"))
        );
        assert!(snapshot.profile.is_some());
    }

    #[test]
    fn session_region_polygon_updates_snapshot_and_writes_mask() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("session-region.image");
        let mut image = PagedImage::<f32>::create(vec![5, 5], direction_coords(), &path).unwrap();
        image
            .put_slice(
                &ArrayD::from_shape_vec(
                    IxDyn(&[5, 5]),
                    (0..5)
                        .flat_map(|x| (0..5).map(move |y| (x * 10 + y) as f32))
                        .collect(),
                )
                .unwrap(),
                &[0, 0],
            )
            .unwrap();
        image.set_units("Jy/beam").unwrap();
        image.save().unwrap();

        let mut session =
            ImageBrowserSession::open(&path, ImageBrowserViewport::new(64, 24)).unwrap();
        session
            .handle_command(ImageBrowserCommand::SetCursor { x: 1, y: 1 })
            .unwrap();
        let started = session
            .handle_command(ImageBrowserCommand::StartRegionShape)
            .unwrap();
        let started_region = started.region.expect("region state after start");
        assert!(started_region.editing);
        assert_eq!(started_region.active_shape_vertices, 0);

        session
            .handle_command(ImageBrowserCommand::AppendRegionVertex { x: 1, y: 1 })
            .unwrap();
        session
            .handle_command(ImageBrowserCommand::AppendRegionVertex { x: 3, y: 1 })
            .unwrap();
        session
            .handle_command(ImageBrowserCommand::AppendRegionVertex { x: 2, y: 3 })
            .unwrap();
        let closed = session
            .handle_command(ImageBrowserCommand::CloseRegionShape)
            .unwrap();
        let region = closed.region.expect("closed region");
        assert!(!region.editing);
        assert_eq!(region.shape_count, 1);
        assert_eq!(region.overlay_shapes.len(), 1);
        let stats = region.stats.expect("region stats");
        assert_eq!(stats.pixel_count, 5);
        assert_eq!(stats.min, 11.0);
        assert_eq!(stats.max, 31.0);
        assert!((stats.mean - 21.6).abs() < 1e-9);

        session
            .handle_command(ImageBrowserCommand::WriteRegionMask {
                name: Some("roi".into()),
                set_default: true,
            })
            .unwrap();
        let reopened = PagedImage::<f32>::open(&path).unwrap();
        assert_eq!(reopened.default_mask_name().as_deref(), Some("roi"));
        let mask = reopened.get_named_mask("roi").unwrap();
        assert!(mask[IxDyn(&[2, 2])]);
        assert!(!mask[IxDyn(&[0, 0])]);
    }

    #[test]
    fn session_region_definitions_persist_and_cycle() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("session-region-definitions.image");
        let mut image = PagedImage::<f32>::create(vec![5, 5], direction_coords(), &path).unwrap();
        image
            .put_slice(
                &ArrayD::from_shape_vec(
                    IxDyn(&[5, 5]),
                    (0..5)
                        .flat_map(|x| (0..5).map(move |y| (x * 10 + y) as f32))
                        .collect(),
                )
                .unwrap(),
                &[0, 0],
            )
            .unwrap();
        image.save().unwrap();

        let mut session =
            ImageBrowserSession::open(&path, ImageBrowserViewport::new(64, 24)).unwrap();
        session
            .handle_command(ImageBrowserCommand::SetCursor { x: 1, y: 1 })
            .unwrap();
        session
            .handle_command(ImageBrowserCommand::StartRegionShape)
            .unwrap();
        session
            .handle_command(ImageBrowserCommand::AppendRegionVertex { x: 1, y: 1 })
            .unwrap();
        session
            .handle_command(ImageBrowserCommand::AppendRegionVertex { x: 3, y: 1 })
            .unwrap();
        session
            .handle_command(ImageBrowserCommand::AppendRegionVertex { x: 2, y: 3 })
            .unwrap();
        session
            .handle_command(ImageBrowserCommand::CloseRegionShape)
            .unwrap();
        let first_saved = session
            .handle_command(ImageBrowserCommand::SaveRegionDefinition)
            .unwrap();
        assert_eq!(
            first_saved.region.expect("first saved region").label,
            "Region 1"
        );

        session
            .handle_command(ImageBrowserCommand::ClearRegion)
            .unwrap();
        session
            .handle_command(ImageBrowserCommand::StartRegionShape)
            .unwrap();
        session
            .handle_command(ImageBrowserCommand::AppendRegionVertex { x: 0, y: 0 })
            .unwrap();
        session
            .handle_command(ImageBrowserCommand::AppendRegionVertex { x: 2, y: 0 })
            .unwrap();
        session
            .handle_command(ImageBrowserCommand::AppendRegionVertex { x: 0, y: 2 })
            .unwrap();
        session
            .handle_command(ImageBrowserCommand::CloseRegionShape)
            .unwrap();
        let second_saved = session
            .handle_command(ImageBrowserCommand::SaveRegionDefinition)
            .unwrap();
        assert_eq!(
            second_saved.region.expect("second saved region").label,
            "Region 2"
        );

        let reopened = PagedImage::<f32>::open(&path).unwrap();
        assert_eq!(
            reopened.region_names(),
            vec!["Region 1".to_string(), "Region 2".to_string()]
        );
        let first_record = reopened.get_region_record("Region 1").unwrap();
        assert_eq!(
            first_record.get("name"),
            Some(&casa_types::Value::Scalar(casa_types::ScalarValue::String(
                "WCPolygon".into()
            ),))
        );

        session
            .handle_command(ImageBrowserCommand::ClearRegion)
            .unwrap();
        let first_loaded = session
            .handle_command(ImageBrowserCommand::LoadNextRegionDefinition)
            .unwrap();
        assert_eq!(
            first_loaded.region.expect("first loaded region").label,
            "Region 1"
        );
        let second_loaded = session
            .handle_command(ImageBrowserCommand::LoadNextRegionDefinition)
            .unwrap();
        assert_eq!(
            second_loaded.region.expect("second loaded region").label,
            "Region 2"
        );

        let mut reopened_session =
            ImageBrowserSession::open(&path, ImageBrowserViewport::new(64, 24)).unwrap();
        let reopened_loaded = reopened_session
            .handle_command(ImageBrowserCommand::LoadNextRegionDefinition)
            .unwrap();
        assert_eq!(
            reopened_loaded.region.expect("reopened saved region").label,
            "Region 1"
        );
    }

    #[test]
    fn session_region_definitions_can_rename_and_delete() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("session-region-rename-delete.image");
        let mut image = PagedImage::<f32>::create(vec![5, 5], direction_coords(), &path).unwrap();
        image.save().unwrap();

        let mut session =
            ImageBrowserSession::open(&path, ImageBrowserViewport::new(64, 24)).unwrap();
        session
            .handle_command(ImageBrowserCommand::SetCursor { x: 1, y: 1 })
            .unwrap();
        session
            .handle_command(ImageBrowserCommand::StartRegionShape)
            .unwrap();
        session
            .handle_command(ImageBrowserCommand::AppendRegionVertex { x: 1, y: 1 })
            .unwrap();
        session
            .handle_command(ImageBrowserCommand::AppendRegionVertex { x: 3, y: 1 })
            .unwrap();
        session
            .handle_command(ImageBrowserCommand::AppendRegionVertex { x: 2, y: 3 })
            .unwrap();
        session
            .handle_command(ImageBrowserCommand::CloseRegionShape)
            .unwrap();
        session
            .handle_command(ImageBrowserCommand::SaveRegionDefinition)
            .unwrap();
        session
            .handle_command(ImageBrowserCommand::LoadNextRegionDefinition)
            .unwrap();

        let renamed = session
            .handle_command(ImageBrowserCommand::RenameRegionDefinition {
                name: "Region 1".into(),
                new_name: "Science Region".into(),
            })
            .unwrap();
        assert_eq!(
            renamed.active_region_definition_name.as_deref(),
            Some("Science Region")
        );
        assert_eq!(
            renamed.saved_region_names,
            vec!["Science Region".to_string()]
        );
        assert_eq!(
            renamed.region.expect("renamed region").label,
            "Science Region"
        );

        let deleted = session
            .handle_command(ImageBrowserCommand::DeleteRegionDefinition {
                name: "Science Region".into(),
            })
            .unwrap();
        assert!(deleted.saved_region_names.is_empty());
        assert!(deleted.active_region_definition_name.is_none());
        assert_eq!(
            deleted
                .region
                .expect("region remains editable after delete")
                .label,
            "Science Region"
        );
    }

    #[test]
    fn session_open_region_keeps_plane_visible_until_polygon_closes() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("session-open-region.image");
        let mut image = PagedImage::<f32>::create(vec![4, 4], direction_coords(), &path).unwrap();
        image
            .put_slice(
                &ArrayD::from_shape_vec(
                    IxDyn(&[4, 4]),
                    (0..16).map(|value| value as f32).collect(),
                )
                .unwrap(),
                &[0, 0],
            )
            .unwrap();
        image.save().unwrap();

        let mut session =
            ImageBrowserSession::open(&path, ImageBrowserViewport::new(64, 24)).unwrap();
        session
            .handle_command(ImageBrowserCommand::SetCursor { x: 1, y: 1 })
            .unwrap();
        let started = session
            .handle_command(ImageBrowserCommand::StartRegionShape)
            .unwrap();
        let plane = started.plane.expect("plane after starting region");
        assert!(!plane.no_finite_values);
        assert_eq!(plane.data_min, 0.0);
        assert_eq!(plane.data_max, 15.0);
        let region = started.region.expect("region state");
        assert!(region.editing);
        assert_eq!(region.active_shape_vertices, 0);
        assert!(region.stats.is_none());
    }

    #[test]
    fn session_plane_view_snapshot_includes_profile_payload() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("linked-plane.image");
        let mut image = PagedImage::<f32>::create(vec![2, 2, 3], cube_coords(), &path).unwrap();
        image
            .put_slice(
                &ArrayD::from_shape_vec(
                    IxDyn(&[2, 2, 3]),
                    vec![
                        1.0, 10.0, 100.0, 2.0, 20.0, 200.0, 3.0, 30.0, 300.0, 4.0, 40.0, 400.0,
                    ],
                )
                .unwrap(),
                &[0, 0, 0],
            )
            .unwrap();
        image.set_units("Jy/beam").unwrap();
        image.save().unwrap();

        let mut session =
            ImageBrowserSession::open(&path, ImageBrowserViewport::new(80, 12)).unwrap();
        session
            .handle_command(ImageBrowserCommand::MoveCursor { dx: 1, dy: 1 })
            .unwrap();
        let snapshot = session.snapshot().unwrap();

        let profile = snapshot.profile.expect("profile payload");
        assert_eq!(snapshot.active_view, ImageBrowserView::Plane);
        assert_eq!(profile.axis_name, "Frequency");
        assert_eq!(profile.coord_type, "Spectral");
        assert_eq!(profile.value_unit, "Jy/beam");
        assert_eq!(profile.selected_sample_index, 1);
        assert_eq!(profile.samples.len(), 3);
        assert_eq!(profile.samples[2].pixel_index, 2);
        assert_eq!(profile.samples[2].value, 400.0);
    }

    #[test]
    fn session_default_mask_toggle_invalidates_plane_and_profile_caches() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("linked-plane-mask.image");
        let mut image = PagedImage::<f32>::create(vec![2, 2, 3], cube_coords(), &path).unwrap();
        image
            .put_slice(
                &ArrayD::from_shape_vec(
                    IxDyn(&[2, 2, 3]),
                    vec![
                        1.0, 10.0, 100.0, 2.0, 20.0, 200.0, 3.0, 30.0, 300.0, 4.0, 40.0, 400.0,
                    ],
                )
                .unwrap(),
                &[0, 0, 0],
            )
            .unwrap();
        image.set_units("Jy/beam").unwrap();
        let mut mask = ArrayD::from_elem(IxDyn(&[2, 2, 3]), true);
        mask[IxDyn(&[1, 1, 0])] = false;
        mask[IxDyn(&[1, 1, 1])] = false;
        mask[IxDyn(&[1, 1, 2])] = false;
        image.put_mask("mask0", &mask).unwrap();
        image.save().unwrap();

        let mut session =
            ImageBrowserSession::open(&path, ImageBrowserViewport::new(80, 12)).unwrap();
        session
            .handle_command(ImageBrowserCommand::MoveCursor { dx: 1, dy: 1 })
            .unwrap();

        let unmasked = session.snapshot().unwrap();
        assert_eq!(unmasked.default_mask_name, None);
        assert!(!unmasked.probe.as_ref().unwrap().masked);
        assert_eq!(
            unmasked.plane.as_ref().unwrap().masked_or_non_finite_count,
            0
        );
        assert!(
            unmasked
                .profile
                .as_ref()
                .unwrap()
                .samples
                .iter()
                .all(|sample| !sample.masked)
        );

        let masked = session
            .handle_command(ImageBrowserCommand::SetDefaultMask {
                name: "mask0".into(),
            })
            .unwrap();
        assert_eq!(masked.default_mask_name.as_deref(), Some("mask0"));
        assert!(masked.probe.as_ref().unwrap().masked);
        assert_eq!(masked.plane.as_ref().unwrap().masked_or_non_finite_count, 1);
        assert!(
            masked
                .profile
                .as_ref()
                .unwrap()
                .samples
                .iter()
                .all(|sample| sample.masked)
        );

        let unmasked_again = session
            .handle_command(ImageBrowserCommand::UnsetDefaultMask)
            .unwrap();
        assert_eq!(unmasked_again.default_mask_name, None);
        assert!(!unmasked_again.probe.as_ref().unwrap().masked);
        assert_eq!(
            unmasked_again
                .plane
                .as_ref()
                .unwrap()
                .masked_or_non_finite_count,
            0
        );
        assert!(
            unmasked_again
                .profile
                .as_ref()
                .unwrap()
                .samples
                .iter()
                .all(|sample| !sample.masked)
        );
    }

    #[test]
    fn session_active_region_switches_profile_to_region_sum() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("linked-region-profile.image");
        let mut image = PagedImage::<f32>::create(vec![2, 2, 3], cube_coords(), &path).unwrap();
        image
            .put_slice(
                &ArrayD::from_shape_vec(
                    IxDyn(&[2, 2, 3]),
                    vec![
                        1.0, 10.0, 100.0, 2.0, 20.0, 200.0, 3.0, 30.0, 300.0, 4.0, 40.0, 400.0,
                    ],
                )
                .unwrap(),
                &[0, 0, 0],
            )
            .unwrap();
        image.set_units("Jy/beam").unwrap();
        image.save().unwrap();

        let mut session =
            ImageBrowserSession::open(&path, ImageBrowserViewport::new(80, 12)).unwrap();
        session
            .handle_command(ImageBrowserCommand::MoveCursor { dx: 1, dy: 1 })
            .unwrap();
        let window = session.window.clone();
        let mut region = session.view.default_region("Region 1").unwrap();
        let vertices = [(0usize, 0usize), (1, 0), (1, 1), (0, 1)]
            .into_iter()
            .map(|pixel_xy| {
                session
                    .view
                    .region_vertex_for_pixel_with_window_and_axes(pixel_xy, &window, &[1])
            })
            .collect::<Result<Vec<_>, _>>()
            .unwrap();
        region.shapes.push(ImageRegionShape {
            vertices,
            closed: true,
        });
        session.region = Some(region);
        session.region_revision = session.region_revision.saturating_add(1);

        let snapshot = session.snapshot().unwrap();
        let profile = snapshot.profile.expect("profile payload");
        assert_eq!(profile.selected_sample_index, 1);
        assert_eq!(profile.samples.len(), 3);
        assert_eq!(profile.samples[0].value, 10.0);
        assert_eq!(profile.samples[1].value, 100.0);
        assert_eq!(profile.samples[2].value, 1000.0);
    }

    #[test]
    fn session_snapshot_omits_backend_timing_when_perf_disabled() {
        let _guard = perf_env_lock();
        clear_perf_env();
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("perf-disabled.image");
        let mut image = PagedImage::<f32>::create(vec![2, 2], direction_coords(), &path).unwrap();
        image
            .put_slice(
                &ArrayD::from_shape_vec(IxDyn(&[2, 2]), vec![1.0, 2.0, 3.0, 4.0]).unwrap(),
                &[0, 0],
            )
            .unwrap();
        image.save().unwrap();

        let mut session = ImageBrowserSession::open(
            &path,
            ImageBrowserViewport::with_plane_pixels(32, 16, 0, 320, 160),
        )
        .unwrap();
        let snapshot = session.snapshot().unwrap();
        assert!(snapshot.backend_timing.is_none());
    }

    #[test]
    fn session_snapshot_includes_backend_timing_and_cache_classification_when_enabled() {
        let _guard = perf_env_lock();
        let _perf_guard = set_perf_env();
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("perf-enabled.image");
        let mut image = PagedImage::<f32>::create(vec![2, 2, 3], cube_coords(), &path).unwrap();
        image
            .put_slice(
                &ArrayD::from_shape_vec(
                    IxDyn(&[2, 2, 3]),
                    vec![
                        1.0, 10.0, 100.0, 2.0, 20.0, 200.0, 3.0, 30.0, 300.0, 4.0, 40.0, 400.0,
                    ],
                )
                .unwrap(),
                &[0, 0, 0],
            )
            .unwrap();
        image.set_units("Jy/beam").unwrap();
        image.save().unwrap();

        let mut session = ImageBrowserSession::open(
            &path,
            ImageBrowserViewport::with_plane_pixels(80, 24, 0, 800, 600),
        )
        .unwrap();
        let first = session.snapshot().unwrap();
        let first_timing = first.backend_timing.expect("first backend timing");
        assert_eq!(
            first_timing.plane_cache_result,
            ImageBackendPlaneCacheResult::Miss
        );
        assert!(first_timing.total_plane_ns > 0);
        assert_eq!(first_timing.profile_cache_misses, 1);

        let second = session.snapshot().unwrap();
        let second_timing = second.backend_timing.expect("second backend timing");
        assert_eq!(
            second_timing.plane_cache_result,
            ImageBackendPlaneCacheResult::Hit
        );
        assert!(second_timing.cached_plane_lookup_ns > 0);
        assert!(second_timing.total_plane_ns > 0);
        assert!(second_timing.total_plane_ns >= second_timing.cached_plane_lookup_ns);
        assert!(second_timing.profile_cache_hits >= 1);
        assert_eq!(second_timing.profile_cache_misses, 1);
    }

    #[test]
    fn frozen_autoscale_keeps_clip_bounds_across_plane_stepping() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("frozen-scale.image");
        let mut image = PagedImage::<f32>::create(vec![2, 2, 2], cube_coords(), &path).unwrap();
        image
            .put_slice(
                &ArrayD::from_shape_vec(
                    IxDyn(&[2, 2, 2]),
                    vec![1.0, 100.0, 2.0, 200.0, 3.0, 300.0, 4.0, 400.0],
                )
                .unwrap(),
                &[0, 0, 0],
            )
            .unwrap();
        image.set_units("Jy/beam").unwrap();
        image.save().unwrap();

        let mut session = ImageBrowserSession::open_with_parameters(
            &path,
            ImageBrowserViewport::new(32, 12),
            Some(&ImageBrowserParameters {
                blc: "0,0,0".into(),
                trc: "1,1,1".into(),
                inc: "1,1,1".into(),
                stretch: "percentile99".into(),
                autoscale: "frozen".into(),
                clip_low: String::new(),
                clip_high: String::new(),
            }),
        )
        .unwrap();
        let first = session.snapshot().unwrap();
        let first_plane = first.plane.expect("first plane");
        assert_eq!(first_plane.clip_min, 100.0);
        assert_eq!(first_plane.clip_max, 400.0);

        let second = session
            .handle_command(ImageBrowserCommand::StepNonDisplayAxis { axis: 2, delta: -1 })
            .unwrap();
        let second_plane = second.plane.expect("second plane");
        assert_eq!(second_plane.clip_min, 100.0);
        assert_eq!(second_plane.clip_max, 400.0);
        assert_eq!(second_plane.data_min, 1.0);
        assert_eq!(second_plane.data_max, 4.0);
    }

    #[test]
    fn session_moves_cursor_and_steps_non_display_axis() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("cube.image");
        let mut image = PagedImage::<f32>::create(vec![2, 2, 3], cube_coords(), &path).unwrap();
        image
            .put_slice(
                &ArrayD::from_shape_vec(
                    IxDyn(&[2, 2, 3]),
                    vec![
                        1.0, 10.0, 100.0, 2.0, 20.0, 200.0, 3.0, 30.0, 300.0, 4.0, 40.0, 400.0,
                    ],
                )
                .unwrap(),
                &[0, 0, 0],
            )
            .unwrap();
        image.save().unwrap();

        let mut session =
            ImageBrowserSession::open(&path, ImageBrowserViewport::new(2, 2)).unwrap();
        let moved = session
            .handle_command(ImageBrowserCommand::MoveCursor { dx: 1, dy: 1 })
            .unwrap();
        assert_eq!(moved.probe.as_ref().unwrap().pixel_indices, vec![1, 1, 1]);

        let stepped = session
            .handle_command(ImageBrowserCommand::StepNonDisplayAxis { axis: 2, delta: 2 })
            .unwrap();
        assert_eq!(stepped.non_display_axes.first().unwrap().index, 2);
        assert_eq!(stepped.probe.as_ref().unwrap().value, 400.0);

        let before_rejected_axis = stepped;
        let error = session
            .handle_command(ImageBrowserCommand::SetSelectedNonDisplayAxis { axis: 99 })
            .expect_err("an invalid profile axis must not be accepted");
        assert!(error.to_string().contains("not a non-display axis"));
        let mut after_rejected_axis = session.snapshot().unwrap();
        after_rejected_axis.backend_timing = before_rejected_axis.backend_timing.clone();
        assert_eq!(after_rejected_axis, before_rejected_axis);
    }

    #[test]
    fn session_applies_window_parameters_to_plane_and_probe() {
        let _guard = perf_env_lock();
        clear_perf_env();
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("windowed-session.image");
        let values = (0..5)
            .flat_map(|x| (0..4).map(move |y| (x * 10 + y) as f32))
            .collect::<Vec<_>>();
        let mut image =
            PagedImage::<f32>::create(vec![5, 4], CoordinateSystem::new(), &path).unwrap();
        image
            .put_slice(
                &ArrayD::from_shape_vec(IxDyn(&[5, 4]), values).unwrap(),
                &[0, 0],
            )
            .unwrap();
        image.save().unwrap();

        let mut session =
            ImageBrowserSession::open(&path, ImageBrowserViewport::new(80, 6)).unwrap();
        let snapshot = session
            .handle_command(ImageBrowserCommand::SetViewWindow {
                parameters: ImageBrowserParameters {
                    blc: "1,1".into(),
                    trc: "4,3".into(),
                    inc: "2,1".into(),
                    stretch: "percentile99".into(),
                    autoscale: "per_plane".into(),
                    clip_low: String::new(),
                    clip_high: String::new(),
                },
            })
            .unwrap();
        assert_eq!(snapshot.parameters.blc, "1,1");
        assert_eq!(snapshot.parameters.trc, "4,3");
        assert_eq!(snapshot.parameters.inc, "2,1");
        assert!(
            snapshot
                .content_lines
                .first()
                .is_some_and(|line| line.contains("1"))
        );
        assert!(
            snapshot
                .content_lines
                .first()
                .is_some_and(|line| line.contains("3"))
        );
        assert_eq!(snapshot.probe.as_ref().unwrap().pixel_indices, vec![1, 2]);

        let moved = session
            .handle_command(ImageBrowserCommand::SetCursor { x: 3, y: 3 })
            .unwrap();
        assert_eq!(moved.probe.as_ref().unwrap().pixel_indices, vec![3, 3]);
        assert_eq!(moved.probe.as_ref().unwrap().value, 33.0);

        let before_rejected_update = session.snapshot().unwrap();
        let error = session
            .handle_command(ImageBrowserCommand::SetViewWindow {
                parameters: ImageBrowserParameters {
                    blc: "not-a-selector".into(),
                    trc: "4,3".into(),
                    inc: "2,1".into(),
                    stretch: "manual".into(),
                    autoscale: "per_plane".into(),
                    clip_low: "1".into(),
                    clip_high: "2".into(),
                },
            })
            .expect_err("invalid window text must reject the complete view update");
        assert!(!error.to_string().is_empty());
        assert_eq!(session.snapshot().unwrap(), before_rejected_update);
    }

    #[test]
    fn plane_view_renders_numeric_grid_content() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("grid.image");
        let mut image =
            PagedImage::<f32>::create(vec![3, 3], CoordinateSystem::new(), &path).unwrap();
        image
            .put_slice(
                &ArrayD::from_shape_vec(
                    IxDyn(&[3, 3]),
                    vec![1.0, 2.0, 3.0, 10.0, 20.0, 30.0, 100.0, 200.0, 300.0],
                )
                .unwrap(),
                &[0, 0],
            )
            .unwrap();
        image.save().unwrap();

        let mut session =
            ImageBrowserSession::open(&path, ImageBrowserViewport::new(48, 6)).unwrap();
        let snapshot = session.snapshot().unwrap();
        assert_eq!(snapshot.active_view, ImageBrowserView::Plane);
        assert!(snapshot.content_lines.first().unwrap().contains("y/x"));
        assert!(
            snapshot
                .content_lines
                .iter()
                .any(|line| line.contains('[') && line.contains(']'))
        );
        assert!(
            snapshot
                .content_lines
                .iter()
                .any(|line| line.contains("200"))
        );
    }

    #[test]
    fn inspector_formats_radec_probe_axes() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("radec-cube.image");
        let mut image = PagedImage::<f32>::create(vec![2, 2, 3], cube_coords(), &path).unwrap();
        image
            .put_slice(
                &ArrayD::from_shape_vec(
                    IxDyn(&[2, 2, 3]),
                    vec![
                        1.0, 10.0, 100.0, 2.0, 20.0, 200.0, 3.0, 30.0, 300.0, 4.0, 40.0, 400.0,
                    ],
                )
                .unwrap(),
                &[0, 0, 0],
            )
            .unwrap();
        image.save().unwrap();

        let mut session =
            ImageBrowserSession::open(&path, ImageBrowserViewport::new(48, 8)).unwrap();
        let snapshot = session
            .handle_command(ImageBrowserCommand::MoveCursor { dx: 1, dy: 1 })
            .unwrap();

        assert!(
            snapshot
                .inspector_lines
                .iter()
                .any(|line| line.contains("Right Ascension: 00:00:00.000000 hms"))
        );
        assert!(
            snapshot
                .inspector_lines
                .iter()
                .any(|line| line.contains("Declination: +45.00.00.00000 dms"))
        );
    }

    #[test]
    fn coordinates_view_includes_active_cursor_world_readout() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("coordinates-cube.image");
        let mut image = PagedImage::<f32>::create(vec![2, 2, 3], cube_coords(), &path).unwrap();
        image
            .put_slice(
                &ArrayD::from_shape_vec(
                    IxDyn(&[2, 2, 3]),
                    vec![
                        1.0, 10.0, 100.0, 2.0, 20.0, 200.0, 3.0, 30.0, 300.0, 4.0, 40.0, 400.0,
                    ],
                )
                .unwrap(),
                &[0, 0, 0],
            )
            .unwrap();
        image.save().unwrap();

        let mut session =
            ImageBrowserSession::open(&path, ImageBrowserViewport::new(80, 12)).unwrap();
        session
            .handle_command(ImageBrowserCommand::MoveCursor { dx: 1, dy: 1 })
            .unwrap();
        let snapshot = session
            .handle_command(ImageBrowserCommand::CycleView { forward: false })
            .unwrap();

        assert_eq!(snapshot.active_view, ImageBrowserView::Coordinates);
        assert!(
            snapshot
                .content_lines
                .iter()
                .any(|line| line == "== Active Cursor ==")
        );
        assert!(
            snapshot
                .content_lines
                .iter()
                .any(|line| line.contains("pixel: 1, 1, 1"))
        );
        assert!(
            snapshot
                .content_lines
                .iter()
                .any(|line| line.contains("Right Ascension: 00:00:00.000000 hms"))
        );
        assert!(
            snapshot
                .content_lines
                .iter()
                .any(|line| line.contains("Declination: +45.00.00.00000 dms"))
        );
        assert!(
            snapshot
                .content_lines
                .iter()
                .any(|line| line.contains("Frequency: 1.421 GHz"))
        );
    }

    #[test]
    fn plane_grid_content_can_exceed_viewport_width() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("wide-grid.image");
        let mut image =
            PagedImage::<f32>::create(vec![8, 2], CoordinateSystem::new(), &path).unwrap();
        image
            .put_slice(
                &ArrayD::from_shape_vec(
                    IxDyn(&[8, 2]),
                    (0..16).map(|value| value as f32).collect(),
                )
                .unwrap(),
                &[0, 0],
            )
            .unwrap();
        image.save().unwrap();

        let mut session =
            ImageBrowserSession::open(&path, ImageBrowserViewport::new(24, 4)).unwrap();
        let snapshot = session.snapshot().unwrap();

        assert!(
            snapshot
                .content_lines
                .iter()
                .any(|line| line.chars().count() > 24)
        );
    }

    #[test]
    fn session_squeezes_degenerate_axes_for_plane_mode() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("degenerate.image");
        let mut image =
            PagedImage::<f32>::create(vec![2, 2, 1, 3], CoordinateSystem::new(), &path).unwrap();
        image
            .put_slice(
                &ArrayD::from_shape_vec(
                    IxDyn(&[2, 2, 1, 3]),
                    vec![
                        1.0, 10.0, 30.0, 4.0, 8.0, 6.0, 2.0, 20.0, 5.0, 7.0, 9.0, 40.0,
                    ],
                )
                .unwrap(),
                &[0, 0, 0, 0],
            )
            .unwrap();
        image.save().unwrap();

        let mut session =
            ImageBrowserSession::open(&path, ImageBrowserViewport::new(2, 2)).unwrap();
        let snapshot = session.snapshot().unwrap();
        assert_eq!(snapshot.active_view, ImageBrowserView::Plane);
        assert_eq!(snapshot.non_display_axes.first().unwrap().index, 1);
        assert_eq!(snapshot.non_display_axes.first().unwrap().length, 3);
        assert_eq!(
            snapshot.probe.as_ref().unwrap().pixel_indices,
            vec![1, 1, 0, 1]
        );

        let stepped = session
            .handle_command(ImageBrowserCommand::StepNonDisplayAxis { axis: 3, delta: 2 })
            .unwrap();
        assert_eq!(
            stepped.probe.as_ref().unwrap().pixel_indices,
            vec![1, 1, 0, 2]
        );
        assert_eq!(stepped.probe.as_ref().unwrap().value, 40.0);
        assert_ne!(
            snapshot.plane.as_ref().unwrap().pixels_u8,
            stepped.plane.as_ref().unwrap().pixels_u8
        );
    }

    #[test]
    fn reopen_replaces_root_image() {
        let dir = tempfile::tempdir().unwrap();
        let first_path = dir.path().join("first.image");
        let second_path = dir.path().join("second.image");
        let mut first =
            PagedImage::<f32>::create(vec![2, 2], CoordinateSystem::new(), &first_path).unwrap();
        first.save().unwrap();
        let mut second =
            PagedImage::<f32>::create(vec![2, 2, 2, 2], CoordinateSystem::new(), &second_path)
                .unwrap();
        second.save().unwrap();

        let mut session =
            ImageBrowserSession::open(&first_path, ImageBrowserViewport::new(2, 2)).unwrap();
        let snapshot = session
            .handle_command(ImageBrowserCommand::OpenRoot {
                path: second_path.display().to_string(),
                viewport: ImageBrowserViewport::new(2, 2),
                parameters: None,
            })
            .unwrap();
        assert_eq!(
            snapshot.status_line,
            "pixel-only mode: coordinate reconstruction unavailable"
        );
        assert_eq!(snapshot.non_display_axes.len(), 2);
    }

    #[test]
    fn crtf_export_classifies_axis_aligned_rectangles_as_boxes() {
        assert_eq!(
            crtf_box_from_vertices(&[(100, 100), (150, 100), (150, 150), (100, 150)]),
            Some((100, 100, 150, 150))
        );
        assert_eq!(
            crtf_box_from_vertices(&[(100, 100), (150, 100), (140, 150), (100, 150)]),
            None
        );
    }

    #[test]
    fn crtf_region_files_interoperate_with_casa_regionmanager_when_available() {
        const CASA_PYTHON: &str =
            "/Users/brianglendenning/SoftwareProjects/casa-build/venv/bin/python";
        if !Path::new(CASA_PYTHON).exists() {
            eprintln!("skipping CASA CRTF interop test: {CASA_PYTHON} is not available");
            return;
        }

        let dir = tempfile::tempdir().unwrap();
        let image_path = dir.path().join("region-interop.image");
        let casars_world_region_path = dir.path().join("casars-world-export.crtf");
        let casars_pixel_region_path = dir.path().join("casars-pixel-export.crtf");
        let casa_world_region_path = dir.path().join("casa-world-export.crtf");
        let casa_pixel_region_path = dir.path().join("casa-pixel-export.crtf");
        let casa_script_path = dir.path().join("crtf_interop.py");
        let shape = vec![3, 3];
        let mut image =
            PagedImage::<f32>::create(shape.clone(), direction_coords(), &image_path).unwrap();
        image
            .put_slice(
                &ArrayD::from_shape_vec(IxDyn(&shape), (0..9).map(|value| value as f32).collect())
                    .unwrap(),
                &[0, 0],
            )
            .unwrap();
        image.save().unwrap();

        let mut session =
            ImageBrowserSession::open(&image_path, ImageBrowserViewport::new(3, 3)).unwrap();
        session
            .handle_command(ImageBrowserCommand::StartRegionShape)
            .unwrap();
        for (x, y) in [(0, 0), (2, 0), (2, 2), (0, 2)] {
            session
                .handle_command(ImageBrowserCommand::AppendRegionVertex { x, y })
                .unwrap();
        }
        session
            .handle_command(ImageBrowserCommand::CloseRegionShape)
            .unwrap();
        session
            .handle_command(ImageBrowserCommand::ExportRegionFile {
                path: casars_world_region_path.display().to_string(),
            })
            .unwrap();
        let casars_world_region = fs::read_to_string(&casars_world_region_path).unwrap();
        assert!(casars_world_region.contains("box[["));
        assert!(casars_world_region.contains("rad"));
        assert!(!casars_world_region.contains("pix"));
        fs::write(
            &casars_pixel_region_path,
            "#CRTFv0 CASA Region Text Format version 0\nbox[[0pix,0pix],[1pix,0pix]]\n",
        )
        .unwrap();

        fs::write(
            &casa_script_path,
            r##"
import math
import sys
from casatools import coordsys, regionmanager

casars_world_region_path = sys.argv[1]
casars_pixel_region_path = sys.argv[2]
casa_world_region_path = sys.argv[3]
casa_pixel_region_path = sys.argv[4]

cs = coordsys()
cs.newcoordsys(direction=True)
cs.setunits(["rad", "rad"], type="direction")
cs.setreferencevalue([0.0, math.pi / 4.0], type="direction")
cs.setincrement([-1.0e-4, 1.0e-4], type="direction")
cs.setreferencepixel([1.0, 1.0], type="direction")

rg = regionmanager()
casars_world_region = rg.fromtextfile(casars_world_region_path, shape=[3, 3], csys=cs.torecord())
if not rg.isworldregion(casars_world_region):
    raise RuntimeError("casa-rs exported CRTF was not parsed as a CASA world region")
casars_pixel_region = rg.fromtextfile(casars_pixel_region_path, shape=[3, 3], csys=cs.torecord())
if not (rg.ispixelregion(casars_pixel_region) or rg.isworldregion(casars_pixel_region)):
    raise RuntimeError("casa-rs exported pixel CRTF was not parsed as a CASA region")

first = cs.toworld([0, 0], "n")["numeric"]
second = cs.toworld([1, 0], "n")["numeric"]
with open(casa_world_region_path, "w", encoding="utf-8") as handle:
    handle.write("#CRTFv0 CASA Region Text Format version 0\n")
    handle.write(
        f"box[[{first[0]:.15f}rad,{first[1]:.15f}rad],"
        f"[{second[0]:.15f}rad,{second[1]:.15f}rad]]\n"
    )
with open(casa_pixel_region_path, "w", encoding="utf-8") as handle:
    handle.write("#CRTFv0 CASA Region Text Format version 0\n")
    handle.write("box[[0pix,0pix],[1pix,0pix]]\n")

casa_world_region = rg.fromtextfile(casa_world_region_path, shape=[3, 3], csys=cs.torecord())
if not rg.isworldregion(casa_world_region):
    raise RuntimeError("CASA-authored CRTF was not parsed as a CASA world region")
casa_pixel_region = rg.fromtextfile(casa_pixel_region_path, shape=[3, 3], csys=cs.torecord())
if not (rg.ispixelregion(casa_pixel_region) or rg.isworldregion(casa_pixel_region)):
    raise RuntimeError("CASA-authored pixel CRTF was not parsed as a CASA region")
"##,
        )
        .unwrap();

        let output = std::process::Command::new(CASA_PYTHON)
            .arg(&casa_script_path)
            .arg(&casars_world_region_path)
            .arg(&casars_pixel_region_path)
            .arg(&casa_world_region_path)
            .arg(&casa_pixel_region_path)
            .output()
            .unwrap();
        if !output.status.success() && casa_regionmanager_probe_unavailable(&output.stderr) {
            eprintln!(
                "skipping CASA CRTF interop test: casatools regionmanager is not loadable\n{}",
                String::from_utf8_lossy(&output.stderr)
            );
            return;
        }
        assert!(
            output.status.success(),
            "CASA CRTF interop probe failed\nstdout:\n{}\nstderr:\n{}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );

        let world_stats = casa_images::analysis::imstat(
            &image_path,
            None,
            Some(casa_world_region_path.to_str().unwrap()),
            None,
            None,
        )
        .unwrap();
        assert_eq!(world_stats.blc, vec![0, 0]);
        assert_eq!(world_stats.trc, vec![1, 0]);
        assert_eq!(world_stats.npts, 2.0);

        let pixel_stats = casa_images::analysis::imstat(
            &image_path,
            None,
            Some(casa_pixel_region_path.to_str().unwrap()),
            None,
            None,
        )
        .unwrap();
        assert_eq!(pixel_stats.blc, vec![0, 0]);
        assert_eq!(pixel_stats.trc, vec![1, 0]);
        assert_eq!(pixel_stats.npts, 2.0);
    }
}

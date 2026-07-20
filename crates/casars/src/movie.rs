// SPDX-License-Identifier: LGPL-3.0-or-later
//! Reusable synchronized movie-bundle planning and caching for image browsers.

use std::collections::{BTreeMap, HashMap, HashSet, VecDeque};
use std::hash::{DefaultHasher, Hash, Hasher};
use std::time::{Duration, Instant};

use image::RgbImage;
use thiserror::Error;

// Nanoseconds per second multiplied by the milli-FPS input scale.
const NANOSECONDS_PER_SECOND_MILLI_SCALE: u128 = 1_000_000_000_000;

/// Surface types currently supported by the reusable image-movie engine.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum ImageMovieSurfaceKind {
    Plane,
    Spectrum,
}

/// Identifies a movie occurrence within a specific browser/movie generation.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct ImageMovieOccurrence {
    pub generation: u64,
    pub movie_key: u64,
    pub axis: usize,
    pub axis_index: usize,
    pub axis_length: usize,
}

/// UI-neutral movie surface request supplied by the frontend.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ImageMovieSurfaceRequest<P> {
    pub kind: ImageMovieSurfaceKind,
    pub request_hash: u64,
    pub cell_size: (u16, u16),
    pub pixel_size: (u32, u32),
    pub payload: P,
}

/// One synchronized movie-bundle request.
#[derive(Debug, Clone, PartialEq)]
pub struct ImageMovieBundleRequest<P> {
    pub occurrence: ImageMovieOccurrence,
    pub requested_fps: f64,
    pub surfaces: Vec<ImageMovieSurfaceRequest<P>>,
}

/// Computed render specification for one movie surface.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct ImageMovieSurfaceSpec {
    pub kind: ImageMovieSurfaceKind,
    pub request_hash: u64,
    pub cell_size: (u16, u16),
    pub pixel_size: (u32, u32),
    pub render_scale: f32,
}

/// Explicit owned-memory description for one movie surface.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ImageMovieSurfaceMemory {
    /// Full-resolution output geometry.
    pub pixel_size: (u32, u32),
    /// Bytes retained per output pixel in a completed frame.
    pub resident_bytes_per_pixel: u32,
    /// Additional bytes per pixel held only while a worker renders the surface.
    pub worker_bytes_per_pixel: u32,
    /// Fixed bytes retained per completed frame, such as protocol metadata.
    pub resident_fixed_bytes: u64,
    /// Fixed bytes held only while a worker renders the surface.
    pub worker_fixed_bytes: u64,
}

/// Complete deterministic input to image-movie resource planning.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ImageMoviePlanRequest {
    /// Surface memory descriptions for one synchronized frame.
    pub surfaces: Vec<ImageMovieSurfaceMemory>,
    /// Number of frames in the selected movie axis.
    pub frame_count: usize,
    /// Requested playback rate expressed as positive milli-frames per second.
    pub requested_fps_milli: u64,
    /// Measured or supplied end-to-end render cost for one frame.
    pub render_latency: Duration,
    /// Parallel workers available to this process.
    pub available_parallelism: usize,
    /// Total bytes available for cached and in-flight movie work.
    pub memory_budget_bytes: u64,
}

/// Deterministic byte/task-aware resource plan for an image movie.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ImageMoviePlan {
    /// Planned output geometry for each input surface, in input order.
    pub surface_pixel_sizes: Vec<(u32, u32)>,
    /// Bytes retained by one completed synchronized frame.
    pub resident_frame_bytes: u64,
    /// Complete resident plus scratch working set for one worker.
    pub worker_working_set_bytes: u64,
    /// Bytes available to the rendered-frame cache after worker reservation.
    pub cache_budget_bytes: u64,
    /// Total bytes reserved by the worker and cache portions of this plan.
    pub reserved_bytes: u64,
    /// Maximum concurrently executing workers.
    pub worker_count: usize,
    /// Maximum queued jobs whose produced frames fit in the cache reservation.
    pub queue_depth: usize,
    /// Maximum executing plus queued frame count.
    pub max_in_flight_frames: usize,
    /// Number of future occurrences to request to cover measured latency.
    pub lookahead_frames: usize,
}

/// Typed failures from image-movie resource planning.
#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub enum ImageMoviePlanError {
    /// A surface had a zero dimension or no accounted bytes per pixel.
    #[error("movie surface {surface_index} has invalid geometry or zero pixel memory")]
    InvalidSurface { surface_index: usize },
    /// Playback rate must be positive.
    #[error("requested movie frame rate must be positive")]
    InvalidFrameRate,
    /// Parallelism must be positive for a non-empty movie.
    #[error("available movie parallelism must be positive")]
    InvalidParallelism,
    /// Checked byte arithmetic overflowed.
    #[error("movie resource planning overflowed byte arithmetic")]
    ByteOverflow,
    /// A prepared request did not match the surfaces described by its plan.
    #[error(
        "movie request has {request_surfaces} surfaces but its resource plan has {planned_surfaces}"
    )]
    SurfaceCountMismatch {
        request_surfaces: usize,
        planned_surfaces: usize,
    },
    /// Even the smallest valid geometry cannot fit one worker in the supplied budget.
    #[error(
        "movie memory budget {budget_bytes} bytes cannot fit the minimum worker set of {minimum_bytes} bytes"
    )]
    InsufficientMemory {
        budget_bytes: u64,
        minimum_bytes: u64,
    },
}

impl ImageMoviePlan {
    /// Build a deterministic plan from explicit workload and resource inputs.
    pub fn build(request: &ImageMoviePlanRequest) -> Result<Self, ImageMoviePlanError> {
        if request.frame_count == 0 {
            return Ok(Self {
                surface_pixel_sizes: request
                    .surfaces
                    .iter()
                    .map(|surface| surface.pixel_size)
                    .collect(),
                resident_frame_bytes: 0,
                worker_working_set_bytes: 0,
                cache_budget_bytes: 0,
                reserved_bytes: 0,
                worker_count: 0,
                queue_depth: 0,
                max_in_flight_frames: 0,
                lookahead_frames: 0,
            });
        }
        if request.requested_fps_milli == 0 {
            return Err(ImageMoviePlanError::InvalidFrameRate);
        }
        if request.available_parallelism == 0 {
            return Err(ImageMoviePlanError::InvalidParallelism);
        }
        for (surface_index, surface) in request.surfaces.iter().enumerate() {
            let accounted_pixel_bytes = surface
                .resident_bytes_per_pixel
                .checked_add(surface.worker_bytes_per_pixel)
                .ok_or(ImageMoviePlanError::ByteOverflow)?;
            if surface.pixel_size.0 == 0 || surface.pixel_size.1 == 0 || accounted_pixel_bytes == 0
            {
                return Err(ImageMoviePlanError::InvalidSurface { surface_index });
            }
        }

        let latency_nanos = request.render_latency.as_nanos();
        let latency_frames = latency_nanos
            .checked_mul(u128::from(request.requested_fps_milli))
            .ok_or(ImageMoviePlanError::ByteOverflow)?
            .div_ceil(NANOSECONDS_PER_SECOND_MILLI_SCALE)
            .max(1);
        let latency_frames = usize::try_from(latency_frames).unwrap_or(usize::MAX);
        let scale_denominator = request
            .surfaces
            .iter()
            .flat_map(|surface| [surface.pixel_size.0, surface.pixel_size.1])
            .max()
            .unwrap_or(1);
        let minimum_sizes = scaled_surface_sizes(&request.surfaces, 1, scale_denominator)?;
        let (minimum_resident_bytes, minimum_worker_bytes) =
            planned_frame_bytes(&request.surfaces, &minimum_sizes)?;
        let minimum_progress_bytes = minimum_worker_bytes
            .checked_add(minimum_resident_bytes)
            .ok_or(ImageMoviePlanError::ByteOverflow)?;
        if minimum_progress_bytes > request.memory_budget_bytes {
            return Err(ImageMoviePlanError::InsufficientMemory {
                budget_bytes: request.memory_budget_bytes,
                minimum_bytes: minimum_progress_bytes,
            });
        }

        let mut low = 1u32;
        let mut high = scale_denominator;
        while low < high {
            let middle = low + (high - low).div_ceil(2);
            let sizes = scaled_surface_sizes(&request.surfaces, middle, scale_denominator)?;
            let (resident_bytes, worker_bytes) = planned_frame_bytes(&request.surfaces, &sizes)?;
            let required = planned_reservation_bytes(resident_bytes, worker_bytes, 1, 1)?;
            if required <= request.memory_budget_bytes {
                low = middle;
            } else {
                high = middle - 1;
            }
        }
        let surface_pixel_sizes = scaled_surface_sizes(&request.surfaces, low, scale_denominator)?;
        let (resident_frame_bytes, worker_working_set_bytes) =
            planned_frame_bytes(&request.surfaces, &surface_pixel_sizes)?;
        let memory_worker_limit =
            usize::try_from(request.memory_budget_bytes / worker_working_set_bytes)
                .unwrap_or(usize::MAX);
        let target_workers = request
            .frame_count
            .min(request.available_parallelism)
            .min(memory_worker_limit)
            .max(1);
        let target_lookahead = latency_frames
            .max(target_workers)
            .min(request.frame_count)
            .max(1);
        let (worker_count, lookahead_frames) = feasible_concurrency(
            resident_frame_bytes,
            worker_working_set_bytes,
            target_workers,
            target_lookahead,
            request.memory_budget_bytes,
        )?;
        let worker_reservation = worker_working_set_bytes
            .checked_mul(worker_count as u64)
            .ok_or(ImageMoviePlanError::ByteOverflow)?;
        let cache_budget_bytes = resident_frame_bytes
            .checked_mul(lookahead_frames as u64)
            .ok_or(ImageMoviePlanError::ByteOverflow)?;
        let reserved_bytes = worker_reservation
            .checked_add(cache_budget_bytes)
            .ok_or(ImageMoviePlanError::ByteOverflow)?;
        debug_assert!(reserved_bytes <= request.memory_budget_bytes);
        let queue_depth = lookahead_frames.saturating_sub(worker_count);
        let max_in_flight_frames = lookahead_frames;

        Ok(Self {
            surface_pixel_sizes,
            resident_frame_bytes,
            worker_working_set_bytes,
            cache_budget_bytes,
            reserved_bytes,
            worker_count,
            queue_depth,
            max_in_flight_frames,
            lookahead_frames,
        })
    }
}

fn feasible_concurrency(
    resident_frame_bytes: u64,
    worker_working_set_bytes: u64,
    target_workers: usize,
    target_lookahead: usize,
    budget_bytes: u64,
) -> Result<(usize, usize), ImageMoviePlanError> {
    for lookahead in (1..=target_lookahead).rev() {
        for workers in (1..=target_workers.min(lookahead)).rev() {
            if planned_reservation_bytes(
                resident_frame_bytes,
                worker_working_set_bytes,
                workers,
                lookahead,
            )? <= budget_bytes
            {
                return Ok((workers, lookahead));
            }
        }
    }
    let minimum_bytes =
        planned_reservation_bytes(resident_frame_bytes, worker_working_set_bytes, 1, 1)?;
    Err(ImageMoviePlanError::InsufficientMemory {
        budget_bytes,
        minimum_bytes,
    })
}

fn planned_reservation_bytes(
    resident_frame_bytes: u64,
    worker_working_set_bytes: u64,
    worker_count: usize,
    lookahead_frames: usize,
) -> Result<u64, ImageMoviePlanError> {
    worker_working_set_bytes
        .checked_mul(worker_count as u64)
        .and_then(|worker_bytes| {
            resident_frame_bytes
                .checked_mul(lookahead_frames as u64)
                .and_then(|cache_bytes| worker_bytes.checked_add(cache_bytes))
        })
        .ok_or(ImageMoviePlanError::ByteOverflow)
}

fn scaled_surface_sizes(
    surfaces: &[ImageMovieSurfaceMemory],
    scale_numerator: u32,
    scale_denominator: u32,
) -> Result<Vec<(u32, u32)>, ImageMoviePlanError> {
    surfaces
        .iter()
        .map(|surface| {
            let scale = |dimension: u32| {
                u64::from(dimension)
                    .checked_mul(u64::from(scale_numerator))
                    .ok_or(ImageMoviePlanError::ByteOverflow)
                    .map(|scaled| {
                        u32::try_from(scaled / u64::from(scale_denominator))
                            .unwrap_or(u32::MAX)
                            .max(1)
                    })
            };
            Ok((scale(surface.pixel_size.0)?, scale(surface.pixel_size.1)?))
        })
        .collect()
}

fn planned_frame_bytes(
    surfaces: &[ImageMovieSurfaceMemory],
    sizes: &[(u32, u32)],
) -> Result<(u64, u64), ImageMoviePlanError> {
    let mut resident = 0u64;
    let mut worker_extra = 0u64;
    for (surface, &(width, height)) in surfaces.iter().zip(sizes) {
        let pixels = u64::from(width)
            .checked_mul(u64::from(height))
            .ok_or(ImageMoviePlanError::ByteOverflow)?;
        let surface_resident = pixels
            .checked_mul(u64::from(surface.resident_bytes_per_pixel))
            .and_then(|bytes| bytes.checked_add(surface.resident_fixed_bytes))
            .ok_or(ImageMoviePlanError::ByteOverflow)?;
        let surface_worker = pixels
            .checked_mul(u64::from(surface.worker_bytes_per_pixel))
            .and_then(|bytes| bytes.checked_add(surface.worker_fixed_bytes))
            .ok_or(ImageMoviePlanError::ByteOverflow)?;
        resident = resident
            .checked_add(surface_resident)
            .ok_or(ImageMoviePlanError::ByteOverflow)?;
        worker_extra = worker_extra
            .checked_add(surface_worker)
            .ok_or(ImageMoviePlanError::ByteOverflow)?;
    }
    let worker = resident
        .checked_add(worker_extra)
        .ok_or(ImageMoviePlanError::ByteOverflow)?;
    Ok((resident, worker))
}

/// Prepared movie surface pairing a render spec with its payload.
#[derive(Debug, Clone, PartialEq)]
pub struct ImageMoviePreparedSurface<P> {
    pub spec: ImageMovieSurfaceSpec,
    pub payload: P,
}

/// Cache key for a prepared/rendered movie bundle.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct ImageMovieBundleKey {
    pub occurrence: ImageMovieOccurrence,
    pub bundle_hash: u64,
}

/// Prepared movie bundle ready for rendering or cache lookup.
#[derive(Debug, Clone, PartialEq)]
pub struct ImageMoviePreparedBundle<P> {
    pub key: ImageMovieBundleKey,
    pub occurrence: ImageMovieOccurrence,
    pub requested_fps: f64,
    pub cache_budget_bytes: u64,
    pub surfaces: Vec<ImageMoviePreparedSurface<P>>,
}

/// Rendered opaque surface bitmap.
#[derive(Debug, Clone, PartialEq)]
pub struct ImageMovieRenderedSurface {
    pub spec: ImageMovieSurfaceSpec,
    pub bitmap: RgbImage,
}

/// Rendered movie bundle returned by the engine or loaded from cache.
#[derive(Debug, Clone, PartialEq)]
pub struct ImageMovieRenderedBundle {
    pub key: ImageMovieBundleKey,
    pub occurrence: ImageMovieOccurrence,
    pub requested_fps: f64,
    pub surfaces: Vec<ImageMovieRenderedSurface>,
}

/// Result of polling for the next due presentation occurrence.
#[derive(Debug)]
pub enum ImageMoviePresentationPoll<P> {
    NotDue,
    Missed { axis_index: usize },
    Ready(P),
}

/// Transport-neutral coordinator for "latest-ready wins" movie presentation.
#[derive(Debug)]
pub struct ImageMoviePresentationCoordinator<P> {
    next_due_index: usize,
    next_due_at: Instant,
    ready: BTreeMap<usize, P>,
    in_flight: HashSet<usize>,
}

impl<P> ImageMoviePresentationCoordinator<P> {
    pub fn new(next_due_index: usize, fps: f64) -> Self {
        Self {
            next_due_index,
            next_due_at: Instant::now() + Duration::from_secs_f64(1.0 / fps.max(0.001)),
            ready: BTreeMap::new(),
            in_flight: HashSet::new(),
        }
    }

    pub fn invalidate(&mut self, next_due_index: usize, fps: f64) {
        self.next_due_index = next_due_index;
        self.next_due_at = Instant::now() + Duration::from_secs_f64(1.0 / fps.max(0.001));
        self.ready.clear();
        self.in_flight.clear();
    }

    pub fn next_due_index(&self) -> usize {
        self.next_due_index
    }

    pub fn next_axis_index_for_offset(&self, offset: usize, axis_length: usize) -> usize {
        (self.next_due_index + offset) % axis_length.max(1)
    }

    pub fn ready_len(&self) -> usize {
        self.ready.len()
    }

    pub fn in_flight_len(&self) -> usize {
        self.in_flight.len()
    }

    pub fn contains_ready(&self, axis_index: usize) -> bool {
        self.ready.contains_key(&axis_index)
    }

    pub fn contains_in_flight(&self, axis_index: usize) -> bool {
        self.in_flight.contains(&axis_index)
    }

    pub fn mark_in_flight(&mut self, axis_index: usize) {
        self.in_flight.insert(axis_index);
    }

    pub fn clear_in_flight(&mut self, axis_index: usize) {
        self.in_flight.remove(&axis_index);
    }

    pub fn mark_ready(&mut self, axis_index: usize, payload: P) {
        self.ready.insert(axis_index, payload);
    }

    pub fn poll_due(
        &mut self,
        now: Instant,
        frame_interval: Duration,
        axis_length: usize,
    ) -> ImageMoviePresentationPoll<P> {
        if now < self.next_due_at {
            return ImageMoviePresentationPoll::NotDue;
        }
        if let Some(payload) = self.ready.remove(&self.next_due_index) {
            self.next_due_index = (self.next_due_index + 1) % axis_length.max(1);
            self.next_due_at = now + frame_interval;
            ImageMoviePresentationPoll::Ready(payload)
        } else {
            ImageMoviePresentationPoll::Missed {
                axis_index: self.next_due_index,
            }
        }
    }
}

/// Callback trait for frontends that render prepared movie surfaces.
pub trait ImageMovieRender<P> {
    fn render_surface(
        &mut self,
        kind: ImageMovieSurfaceKind,
        pixel_size: (u32, u32),
        payload: &P,
    ) -> Result<RgbImage, String>;
}

impl<P, F> ImageMovieRender<P> for F
where
    F: FnMut(ImageMovieSurfaceKind, (u32, u32), &P) -> Result<RgbImage, String>,
{
    fn render_surface(
        &mut self,
        kind: ImageMovieSurfaceKind,
        pixel_size: (u32, u32),
        payload: &P,
    ) -> Result<RgbImage, String> {
        self(kind, pixel_size, payload)
    }
}

/// Byte-bounded LRU cache of rendered movie bundles.
#[derive(Debug)]
pub struct ImageMovieBundleCache<K> {
    capacity_bytes: usize,
    total_bytes: usize,
    values: HashMap<K, ImageMovieRenderedBundle>,
    order: VecDeque<K>,
}

impl<K> ImageMovieBundleCache<K>
where
    K: Clone + Eq + Hash,
{
    pub fn new(capacity_bytes: usize) -> Self {
        Self {
            capacity_bytes,
            total_bytes: 0,
            values: HashMap::new(),
            order: VecDeque::new(),
        }
    }

    pub fn get(&mut self, key: &K) -> Option<ImageMovieRenderedBundle> {
        let value = self.values.get(key).cloned()?;
        self.touch(key);
        Some(value)
    }

    pub fn insert(&mut self, key: K, value: ImageMovieRenderedBundle) {
        if self.capacity_bytes == 0 {
            return;
        }
        let value_bytes = rendered_bundle_bytes(&value);
        if let Some(previous) = self.values.insert(key.clone(), value) {
            self.total_bytes = self
                .total_bytes
                .saturating_sub(rendered_bundle_bytes(&previous));
            self.total_bytes = self.total_bytes.saturating_add(value_bytes);
            self.touch(&key);
            self.evict_if_needed();
            return;
        }
        self.total_bytes = self.total_bytes.saturating_add(value_bytes);
        self.order.push_back(key);
        self.evict_if_needed();
    }

    pub fn clear(&mut self) {
        self.values.clear();
        self.order.clear();
        self.total_bytes = 0;
    }

    fn set_capacity_bytes(&mut self, capacity_bytes: usize) {
        self.capacity_bytes = capacity_bytes;
        self.evict_if_needed();
    }

    pub fn total_bytes(&self) -> usize {
        self.total_bytes
    }

    pub fn len(&self) -> usize {
        self.values.len()
    }

    pub fn is_empty(&self) -> bool {
        self.values.is_empty()
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
                    .saturating_sub(rendered_bundle_bytes(&previous));
            }
        }
    }
}

/// Synchronized image-movie renderer/cache driven by an explicit resource plan.
#[derive(Debug)]
pub struct ImageMovieBundleEngine {
    cache: ImageMovieBundleCache<ImageMovieBundleKey>,
}

impl ImageMovieBundleEngine {
    /// Create an engine with the exact cache budget emitted by a plan.
    pub fn new(cache_budget_bytes: u64) -> Self {
        let cache_bytes = usize::try_from(cache_budget_bytes).unwrap_or(usize::MAX);
        Self {
            cache: ImageMovieBundleCache::new(cache_bytes),
        }
    }

    /// Prepare one synchronized bundle using geometry from the supplied plan.
    pub fn prepare_bundle<P: Clone>(
        &self,
        request: &ImageMovieBundleRequest<P>,
        plan: &ImageMoviePlan,
    ) -> Result<ImageMoviePreparedBundle<P>, ImageMoviePlanError> {
        if request.surfaces.len() != plan.surface_pixel_sizes.len() {
            return Err(ImageMoviePlanError::SurfaceCountMismatch {
                request_surfaces: request.surfaces.len(),
                planned_surfaces: plan.surface_pixel_sizes.len(),
            });
        }
        let surfaces = request
            .surfaces
            .iter()
            .zip(&plan.surface_pixel_sizes)
            .map(|(surface, &pixel_size)| {
                let width_scale = pixel_size.0 as f64 / f64::from(surface.pixel_size.0);
                let height_scale = pixel_size.1 as f64 / f64::from(surface.pixel_size.1);
                ImageMoviePreparedSurface {
                    spec: ImageMovieSurfaceSpec {
                        kind: surface.kind,
                        request_hash: surface.request_hash,
                        cell_size: surface.cell_size,
                        pixel_size,
                        render_scale: width_scale.min(height_scale) as f32,
                    },
                    payload: surface.payload.clone(),
                }
            })
            .collect::<Vec<_>>();
        Ok(ImageMoviePreparedBundle {
            key: ImageMovieBundleKey {
                occurrence: request.occurrence,
                bundle_hash: hashed_bundle_signature(request.occurrence, &surfaces),
            },
            occurrence: request.occurrence,
            requested_fps: request.requested_fps,
            cache_budget_bytes: plan.cache_budget_bytes,
            surfaces,
        })
    }

    pub fn cached_bundle(&mut self, key: &ImageMovieBundleKey) -> Option<ImageMovieRenderedBundle> {
        self.cache.get(key)
    }

    pub fn render_or_get_cached<P, R>(
        &mut self,
        prepared: &ImageMoviePreparedBundle<P>,
        renderer: &mut R,
    ) -> Result<(ImageMovieRenderedBundle, bool), String>
    where
        R: ImageMovieRender<P>,
    {
        self.cache
            .set_capacity_bytes(usize::try_from(prepared.cache_budget_bytes).unwrap_or(usize::MAX));
        if let Some(bundle) = self.cached_bundle(&prepared.key) {
            return Ok((bundle, true));
        }
        let mut surfaces = Vec::with_capacity(prepared.surfaces.len());
        for surface in &prepared.surfaces {
            let bitmap = renderer.render_surface(
                surface.spec.kind,
                surface.spec.pixel_size,
                &surface.payload,
            )?;
            surfaces.push(ImageMovieRenderedSurface {
                spec: surface.spec,
                bitmap,
            });
        }
        let rendered = ImageMovieRenderedBundle {
            key: prepared.key,
            occurrence: prepared.occurrence,
            requested_fps: prepared.requested_fps,
            surfaces,
        };
        self.cache.insert(prepared.key, rendered.clone());
        Ok((rendered, false))
    }

    pub fn clear(&mut self) {
        self.cache.clear();
    }

    pub fn cache_bytes(&self) -> usize {
        self.cache.total_bytes()
    }

    pub fn cache_len(&self) -> usize {
        self.cache.len()
    }
}

fn hashed_bundle_signature<P>(
    occurrence: ImageMovieOccurrence,
    surfaces: &[ImageMoviePreparedSurface<P>],
) -> u64 {
    let mut hasher = DefaultHasher::new();
    occurrence.hash(&mut hasher);
    for surface in surfaces {
        surface.spec.kind.hash(&mut hasher);
        surface.spec.request_hash.hash(&mut hasher);
        surface.spec.cell_size.hash(&mut hasher);
        surface.spec.pixel_size.hash(&mut hasher);
        surface.spec.render_scale.to_bits().hash(&mut hasher);
    }
    hasher.finish()
}

fn rendered_bundle_bytes(bundle: &ImageMovieRenderedBundle) -> usize {
    bundle
        .surfaces
        .iter()
        .map(|surface| surface.bitmap.as_raw().len())
        .sum()
}

#[cfg(test)]
mod tests {
    use std::time::{Duration, Instant};

    use image::RgbImage;

    use super::{
        ImageMovieBundleEngine, ImageMovieBundleRequest, ImageMovieOccurrence, ImageMoviePlan,
        ImageMoviePlanError, ImageMoviePlanRequest, ImageMoviePresentationCoordinator,
        ImageMoviePresentationPoll, ImageMovieSurfaceKind, ImageMovieSurfaceMemory,
        ImageMovieSurfaceRequest,
    };

    fn plan_request(
        pixel_size: (u32, u32),
        frame_count: usize,
        parallelism: usize,
        budget: u64,
    ) -> ImageMoviePlanRequest {
        ImageMoviePlanRequest {
            surfaces: vec![ImageMovieSurfaceMemory {
                pixel_size,
                resident_bytes_per_pixel: 7,
                worker_bytes_per_pixel: 4,
                resident_fixed_bytes: 128,
                worker_fixed_bytes: 64,
            }],
            frame_count,
            requested_fps_milli: 10_000,
            render_latency: Duration::from_millis(80),
            available_parallelism: parallelism,
            memory_budget_bytes: budget,
        }
    }

    fn surface_request(
        kind: ImageMovieSurfaceKind,
        request_hash: u64,
        pixel_size: (u32, u32),
        payload: u8,
    ) -> ImageMovieSurfaceRequest<u8> {
        ImageMovieSurfaceRequest {
            kind,
            request_hash,
            cell_size: (40, 12),
            pixel_size,
            payload,
        }
    }

    fn geometry_plan<P>(
        request: &ImageMovieBundleRequest<P>,
        memory_budget_bytes: u64,
    ) -> ImageMoviePlan {
        ImageMoviePlan::build(&ImageMoviePlanRequest {
            surfaces: request
                .surfaces
                .iter()
                .map(|surface| ImageMovieSurfaceMemory {
                    pixel_size: surface.pixel_size,
                    resident_bytes_per_pixel: 3,
                    worker_bytes_per_pixel: 0,
                    resident_fixed_bytes: 0,
                    worker_fixed_bytes: 0,
                })
                .collect(),
            frame_count: request.occurrence.axis_length,
            requested_fps_milli: (request.requested_fps * 1_000.0) as u64,
            render_latency: Duration::from_millis(1),
            available_parallelism: 1,
            memory_budget_bytes,
        })
        .unwrap()
    }

    fn full_geometry_plan<P>(request: &ImageMovieBundleRequest<P>) -> ImageMoviePlan {
        geometry_plan(request, 1024 * 1024 * 1024)
    }

    #[test]
    fn prepare_bundle_keeps_plane_and_spectrum_synchronized() {
        let engine = ImageMovieBundleEngine::new(64 * 1024 * 1024);
        let request = ImageMovieBundleRequest {
            occurrence: ImageMovieOccurrence {
                generation: 7,
                movie_key: 42,
                axis: 3,
                axis_index: 12,
                axis_length: 63,
            },
            requested_fps: 10.0,
            surfaces: vec![
                surface_request(ImageMovieSurfaceKind::Plane, 1001, (640, 480), 1),
                surface_request(ImageMovieSurfaceKind::Spectrum, 2002, (640, 160), 2),
            ],
        };

        let prepared = engine
            .prepare_bundle(&request, &full_geometry_plan(&request))
            .unwrap();
        assert_eq!(prepared.occurrence, request.occurrence);
        assert_eq!(prepared.surfaces.len(), 2);
        assert_eq!(prepared.surfaces[0].spec.kind, ImageMovieSurfaceKind::Plane);
        assert_eq!(
            prepared.surfaces[1].spec.kind,
            ImageMovieSurfaceKind::Spectrum
        );
    }

    #[test]
    fn render_or_get_cached_reuses_previous_bundle() {
        let mut engine = ImageMovieBundleEngine::new(64 * 1024 * 1024);
        let request = ImageMovieBundleRequest {
            occurrence: ImageMovieOccurrence {
                generation: 3,
                movie_key: 11,
                axis: 3,
                axis_index: 4,
                axis_length: 8,
            },
            requested_fps: 10.0,
            surfaces: vec![surface_request(
                ImageMovieSurfaceKind::Plane,
                1001,
                (64, 64),
                9,
            )],
        };
        let prepared = engine
            .prepare_bundle(&request, &full_geometry_plan(&request))
            .unwrap();
        let mut renders = 0usize;
        let mut renderer = |_: ImageMovieSurfaceKind, (width, height), payload: &u8| {
            renders += 1;
            Ok(RgbImage::from_pixel(
                width,
                height,
                image::Rgb([*payload, 0, 0]),
            ))
        };

        let (_, cache_hit) = engine
            .render_or_get_cached(&prepared, &mut renderer)
            .unwrap();
        assert!(!cache_hit);
        let (_, cache_hit) = engine
            .render_or_get_cached(&prepared, &mut renderer)
            .unwrap();
        assert!(cache_hit);
        assert_eq!(renders, 1);
    }

    #[test]
    fn cache_evicts_least_recent_bundle_when_budget_is_exceeded() {
        let mut engine = ImageMovieBundleEngine::new(64 * 64 * 3 + 8);
        let make_request = |axis_index| ImageMovieBundleRequest {
            occurrence: ImageMovieOccurrence {
                generation: 1,
                movie_key: 1,
                axis: 3,
                axis_index,
                axis_length: 2,
            },
            requested_fps: 10.0,
            surfaces: vec![surface_request(
                ImageMovieSurfaceKind::Plane,
                1000 + axis_index as u64,
                (128, 128),
                axis_index as u8,
            )],
        };
        let first_request = make_request(0);
        let second_request = make_request(1);
        let cache_plan = geometry_plan(&first_request, 64 * 64 * 3 + 8);
        let first = engine.prepare_bundle(&first_request, &cache_plan).unwrap();
        let second = engine.prepare_bundle(&second_request, &cache_plan).unwrap();
        let mut renderer = |_: ImageMovieSurfaceKind, (width, height), payload: &u8| {
            Ok(RgbImage::from_pixel(
                width,
                height,
                image::Rgb([*payload, 0, 0]),
            ))
        };

        let _ = engine.render_or_get_cached(&first, &mut renderer).unwrap();
        let _ = engine.render_or_get_cached(&second, &mut renderer).unwrap();

        assert!(engine.cached_bundle(&first.key).is_none());
        assert!(engine.cached_bundle(&second.key).is_some());
    }

    #[test]
    fn prepared_bundle_uses_planned_geometry_for_every_surface() {
        let engine = ImageMovieBundleEngine::new(40 * 1024 * 1024);
        let request = ImageMovieBundleRequest {
            occurrence: ImageMovieOccurrence {
                generation: 1,
                movie_key: 77,
                axis: 3,
                axis_index: 0,
                axis_length: 63,
            },
            requested_fps: 10.0,
            surfaces: vec![
                surface_request(ImageMovieSurfaceKind::Plane, 1001, (2185, 1134), 1),
                surface_request(ImageMovieSurfaceKind::Spectrum, 2002, (1200, 240), 2),
            ],
        };

        let plan = ImageMoviePlan::build(&ImageMoviePlanRequest {
            surfaces: request
                .surfaces
                .iter()
                .map(|surface| ImageMovieSurfaceMemory {
                    pixel_size: surface.pixel_size,
                    resident_bytes_per_pixel: 7,
                    worker_bytes_per_pixel: 4,
                    resident_fixed_bytes: 0,
                    worker_fixed_bytes: 0,
                })
                .collect(),
            frame_count: request.occurrence.axis_length,
            requested_fps_milli: 10_000,
            render_latency: Duration::from_millis(100),
            available_parallelism: 4,
            memory_budget_bytes: 4 * 1024 * 1024,
        })
        .unwrap();
        let prepared = engine.prepare_bundle(&request, &plan).unwrap();
        let plane = prepared
            .surfaces
            .iter()
            .find(|surface| surface.spec.kind == ImageMovieSurfaceKind::Plane)
            .unwrap();
        let spectrum = prepared
            .surfaces
            .iter()
            .find(|surface| surface.spec.kind == ImageMovieSurfaceKind::Spectrum)
            .unwrap();

        assert!(plane.spec.render_scale < 1.0);
        assert!(spectrum.spec.render_scale < 1.0);
        assert_eq!(
            prepared
                .surfaces
                .iter()
                .map(|surface| surface.spec.pixel_size)
                .collect::<Vec<_>>(),
            plan.surface_pixel_sizes
        );
    }

    #[test]
    fn planner_preserves_full_geometry_when_the_explicit_budget_fits() {
        let request = plan_request((640, 480), 32, 8, 128 * 1024 * 1024);
        let plan = ImageMoviePlan::build(&request).unwrap();

        assert_eq!(plan.surface_pixel_sizes, vec![(640, 480)]);
        assert_eq!(plan.resident_frame_bytes, 640 * 480 * 7 + 128);
        assert_eq!(plan.worker_working_set_bytes, 640 * 480 * 11 + 192);
        assert_eq!(plan.worker_count, 8);
        assert!(plan.cache_budget_bytes < request.memory_budget_bytes);
        assert_eq!(
            plan.reserved_bytes,
            plan.worker_working_set_bytes * plan.worker_count as u64 + plan.cache_budget_bytes
        );
        assert!(plan.reserved_bytes <= request.memory_budget_bytes);
        assert!(plan.max_in_flight_frames <= request.frame_count);
        assert!(plan.lookahead_frames <= plan.max_in_flight_frames);
    }

    #[test]
    fn planner_downscales_geometry_to_fit_a_constrained_budget() {
        let request = plan_request((4_000, 2_000), 12, 4, 2 * 1024 * 1024);
        let plan = ImageMoviePlan::build(&request).unwrap();

        let (width, height) = plan.surface_pixel_sizes[0];
        assert!(width < 4_000);
        assert!(height < 2_000);
        assert!(plan.worker_working_set_bytes <= request.memory_budget_bytes);
        assert!(plan.reserved_bytes <= request.memory_budget_bytes);
        assert_eq!(width / height, 2);
    }

    #[test]
    fn planner_worker_count_is_bounded_by_work_budget_and_parallelism() {
        let one_worker = plan_request((100, 100), 40, 16, 111_192);
        let one_plan = ImageMoviePlan::build(&one_worker).unwrap();
        assert_eq!(one_plan.worker_count, 1);

        let short_axis = plan_request((100, 100), 2, 16, 10_000_000);
        assert_eq!(ImageMoviePlan::build(&short_axis).unwrap().worker_count, 2);

        let few_cores = plan_request((100, 100), 40, 3, 10_000_000);
        assert_eq!(ImageMoviePlan::build(&few_cores).unwrap().worker_count, 3);
    }

    #[test]
    fn planner_lookahead_comes_from_fps_latency_and_capacity() {
        let mut request = plan_request((100, 100), 100, 2, 10_000_000);
        request.requested_fps_milli = 25_000;
        request.render_latency = Duration::from_millis(200);
        let slower = ImageMoviePlan::build(&request).unwrap();
        assert_eq!(slower.lookahead_frames, 5);

        request.render_latency = Duration::from_millis(20);
        let faster = ImageMoviePlan::build(&request).unwrap();
        assert_eq!(faster.lookahead_frames, faster.worker_count);
    }

    #[test]
    fn planner_handles_zero_and_single_frame_workloads_deliberately() {
        let empty = plan_request((640, 480), 0, 0, 1234);
        let empty_plan = ImageMoviePlan::build(&empty).unwrap();
        assert_eq!(empty_plan.worker_count, 0);
        assert_eq!(empty_plan.cache_budget_bytes, 0);
        assert_eq!(empty_plan.reserved_bytes, 0);

        let single = plan_request((16, 16), 1, 32, 1_000_000);
        let single_plan = ImageMoviePlan::build(&single).unwrap();
        assert_eq!(single_plan.worker_count, 1);
        assert_eq!(single_plan.queue_depth, 0);
        assert_eq!(single_plan.lookahead_frames, 1);
    }

    #[test]
    fn planner_rejects_impossible_and_invalid_inputs_with_typed_errors() {
        let impossible = plan_request((10, 10), 2, 1, 10);
        assert!(matches!(
            ImageMoviePlan::build(&impossible),
            Err(ImageMoviePlanError::InsufficientMemory { .. })
        ));

        let mut invalid_fps = plan_request((10, 10), 2, 1, 10_000);
        invalid_fps.requested_fps_milli = 0;
        assert_eq!(
            ImageMoviePlan::build(&invalid_fps),
            Err(ImageMoviePlanError::InvalidFrameRate)
        );

        let mut invalid_surface = plan_request((0, 10), 2, 1, 10_000);
        assert!(matches!(
            ImageMoviePlan::build(&invalid_surface),
            Err(ImageMoviePlanError::InvalidSurface { surface_index: 0 })
        ));
        invalid_surface.surfaces[0].pixel_size = (10, 10);
        invalid_surface.surfaces[0].resident_bytes_per_pixel = 0;
        invalid_surface.surfaces[0].worker_bytes_per_pixel = 0;
        assert!(matches!(
            ImageMoviePlan::build(&invalid_surface),
            Err(ImageMoviePlanError::InvalidSurface { surface_index: 0 })
        ));
    }

    #[test]
    fn planner_reports_checked_byte_overflow() {
        let request = ImageMoviePlanRequest {
            surfaces: vec![ImageMovieSurfaceMemory {
                pixel_size: (u32::MAX, u32::MAX),
                resident_bytes_per_pixel: u32::MAX,
                worker_bytes_per_pixel: u32::MAX,
                resident_fixed_bytes: u64::MAX,
                worker_fixed_bytes: u64::MAX,
            }],
            frame_count: usize::MAX,
            requested_fps_milli: u64::MAX,
            render_latency: Duration::MAX,
            available_parallelism: usize::MAX,
            memory_budget_bytes: u64::MAX,
        };
        assert_eq!(
            ImageMoviePlan::build(&request),
            Err(ImageMoviePlanError::ByteOverflow)
        );
    }

    #[test]
    fn presentation_coordinator_tracks_due_frames() {
        let mut coordinator = ImageMoviePresentationCoordinator::new(3, 10.0);
        coordinator.mark_in_flight(3);
        assert!(coordinator.contains_in_flight(3));
        coordinator.clear_in_flight(3);
        assert!(!coordinator.contains_in_flight(3));
        coordinator.mark_ready(3, "frame-3");

        match coordinator.poll_due(
            Instant::now() + Duration::from_secs(1),
            Duration::from_millis(100),
            8,
        ) {
            ImageMoviePresentationPoll::Ready(frame) => assert_eq!(frame, "frame-3"),
            other => panic!("unexpected poll result: {other:?}"),
        }
        assert_eq!(coordinator.next_due_index(), 4);
    }
}

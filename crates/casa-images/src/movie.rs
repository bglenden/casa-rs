// SPDX-License-Identifier: LGPL-3.0-or-later
//! Reusable synchronized movie-bundle planning and caching for image browsers.

use std::collections::{BTreeMap, HashMap, HashSet, VecDeque};
use std::hash::{DefaultHasher, Hash, Hasher};
use std::time::{Duration, Instant};

use image::RgbImage;

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
            capacity_bytes: capacity_bytes.max(1),
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

/// Reusable synchronized image-movie engine with adaptive scaling and LRU caching.
#[derive(Debug)]
pub struct ImageMovieBundleEngine {
    target_resident_bytes: u64,
    min_render_scale: f32,
    cache: ImageMovieBundleCache<ImageMovieBundleKey>,
}

impl ImageMovieBundleEngine {
    pub fn new(target_resident_bytes: u64, min_render_scale: f32) -> Self {
        let cache_bytes = usize::try_from(target_resident_bytes)
            .unwrap_or(usize::MAX)
            .max(1);
        Self {
            target_resident_bytes,
            min_render_scale: min_render_scale.clamp(0.01, 1.0),
            cache: ImageMovieBundleCache::new(cache_bytes),
        }
    }

    pub fn prepare_bundle<P: Clone>(
        &self,
        request: &ImageMovieBundleRequest<P>,
    ) -> ImageMoviePreparedBundle<P> {
        let plane_scale = self.plane_render_scale(request);
        let surfaces = request
            .surfaces
            .iter()
            .map(|surface| {
                let render_scale = match surface.kind {
                    ImageMovieSurfaceKind::Plane => plane_scale,
                    ImageMovieSurfaceKind::Spectrum => 1.0,
                };
                let pixel_size = (
                    scaled_movie_render_dimension(surface.pixel_size.0, render_scale),
                    scaled_movie_render_dimension(surface.pixel_size.1, render_scale),
                );
                ImageMoviePreparedSurface {
                    spec: ImageMovieSurfaceSpec {
                        kind: surface.kind,
                        request_hash: surface.request_hash,
                        cell_size: surface.cell_size,
                        pixel_size,
                        render_scale,
                    },
                    payload: surface.payload.clone(),
                }
            })
            .collect::<Vec<_>>();
        ImageMoviePreparedBundle {
            key: ImageMovieBundleKey {
                occurrence: request.occurrence,
                bundle_hash: hashed_bundle_signature(request.occurrence, &surfaces),
            },
            occurrence: request.occurrence,
            requested_fps: request.requested_fps,
            surfaces,
        }
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

    fn plane_render_scale<P>(&self, request: &ImageMovieBundleRequest<P>) -> f32 {
        if request.occurrence.axis_length <= 1 || self.target_resident_bytes == 0 {
            return 1.0;
        }

        let frame_budget = self
            .target_resident_bytes
            .checked_div(request.occurrence.axis_length.max(1) as u64)
            .unwrap_or(0);
        if frame_budget == 0 {
            return self.min_render_scale;
        }

        let fixed_bytes = request
            .surfaces
            .iter()
            .filter(|surface| surface.kind != ImageMovieSurfaceKind::Plane)
            .map(|surface| surface_full_bytes(surface.pixel_size))
            .sum::<u64>();
        let plane_bytes = request
            .surfaces
            .iter()
            .filter(|surface| surface.kind == ImageMovieSurfaceKind::Plane)
            .map(|surface| surface_full_bytes(surface.pixel_size))
            .sum::<u64>();

        if plane_bytes == 0 {
            return 1.0;
        }

        let available_for_plane = frame_budget.saturating_sub(fixed_bytes);
        if available_for_plane >= plane_bytes {
            return 1.0;
        }

        ((available_for_plane as f64 / plane_bytes as f64).sqrt() as f32)
            .clamp(self.min_render_scale, 1.0)
    }
}

fn surface_full_bytes(pixel_size: (u32, u32)) -> u64 {
    u64::from(pixel_size.0)
        .saturating_mul(u64::from(pixel_size.1))
        .saturating_mul(3)
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

fn scaled_movie_render_dimension(dimension: u32, render_scale: f32) -> u32 {
    let dimension = dimension.max(1);
    if render_scale >= 0.999 {
        return dimension;
    }
    let scaled = ((dimension as f32) * render_scale).round().max(1.0) as u32;
    if dimension <= 64 {
        scaled.min(dimension).max(1)
    } else {
        scaled.max(64).min(dimension)
    }
}

#[cfg(test)]
mod tests {
    use std::time::{Duration, Instant};

    use image::RgbImage;

    use super::{
        ImageMovieBundleEngine, ImageMovieBundleRequest, ImageMovieOccurrence,
        ImageMoviePresentationCoordinator, ImageMoviePresentationPoll, ImageMovieSurfaceKind,
        ImageMovieSurfaceRequest,
    };

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

    #[test]
    fn prepare_bundle_keeps_plane_and_spectrum_synchronized() {
        let engine = ImageMovieBundleEngine::new(64 * 1024 * 1024, 0.35);
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

        let prepared = engine.prepare_bundle(&request);
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
        let mut engine = ImageMovieBundleEngine::new(64 * 1024 * 1024, 0.35);
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
        let prepared = engine.prepare_bundle(&request);
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
        let mut engine = ImageMovieBundleEngine::new(64 * 64 * 3 + 8, 0.35);
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
        let first = engine.prepare_bundle(&make_request(0));
        let second = engine.prepare_bundle(&make_request(1));
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
    fn plane_scale_downshifts_before_spectrum() {
        let engine = ImageMovieBundleEngine::new(40 * 1024 * 1024, 0.35);
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

        let prepared = engine.prepare_bundle(&request);
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
        assert_eq!(spectrum.spec.render_scale, 1.0);
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

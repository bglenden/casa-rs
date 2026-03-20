// SPDX-License-Identifier: LGPL-3.0-or-later
//! General lattice traversal specifications and iterators.
//!
//! This module provides a convenience-first traversal API that covers the
//! sensible C++ `LatticeIterator`/`LatticeNavigator` use cases without
//! requiring callers to assemble navigator objects manually.
//!
//! TODO: proper mask support still needs a first-class masked-lattice model.
//! Properly implemented masked traversal should return aligned value and mask
//! cursors and define consistent read/write behavior for both temporary and
//! persistent backends.
//!
//! TODO: mutable traversal now has the same cache-hint plumbing as read-only
//! traversal, but it still lacks the richer producer/worker execution helpers
//! used by read-only reductions and map/write pipelines.

use ndarray::ArrayD;

use crate::element::LatticeElement;
use crate::error::LatticeError;
use crate::lattice::Lattice;

/// Opaque lifetime token used to keep traversal-specific cache tuning active.
///
/// Implementations should restore any temporary cache override in `Drop`.
pub trait TraversalCacheScope {}

/// Cache-relevant traversal mode derived from a [`TraversalSpec`].
#[doc(hidden)]
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum TraversalCacheMode {
    Chunks(Vec<usize>),
    Tiles,
    Lines { axis: usize },
    Planes { axes: Vec<usize> },
}

/// Cache-tuning hint derived from a validated traversal plan.
#[doc(hidden)]
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TraversalCacheHint {
    pub mode: TraversalCacheMode,
    pub section_start: Vec<usize>,
    pub section_shape: Vec<usize>,
    pub requested_cursor: Vec<usize>,
    pub axis_path: Vec<usize>,
}

#[derive(Clone, Debug)]
struct TraversalSection {
    start: Vec<usize>,
    shape: Vec<usize>,
    stride: Vec<usize>,
}

/// Declarative description of how to traverse a lattice.
///
/// This is the main Rust entrypoint for chunked lattice traversal. It covers
/// the common C++ casacore iterator patterns with a small builder-style API.
#[derive(Clone, Debug)]
pub struct TraversalSpec {
    mode: TraversalCacheMode,
    axis_path: Option<Vec<usize>>,
    section: Option<TraversalSection>,
}

impl TraversalSpec {
    /// Traverse rectangular chunks of the requested shape.
    pub fn chunks(cursor_shape: Vec<usize>) -> Self {
        Self {
            mode: TraversalCacheMode::Chunks(cursor_shape),
            axis_path: None,
            section: None,
        }
    }

    /// Traverse tile-sized chunks using the lattice's preferred cursor shape.
    pub fn tiles() -> Self {
        Self {
            mode: TraversalCacheMode::Tiles,
            axis_path: None,
            section: None,
        }
    }

    /// Traverse full logical lines along `axis`.
    pub fn lines(axis: usize) -> Self {
        Self {
            mode: TraversalCacheMode::Lines { axis },
            axis_path: None,
            section: None,
        }
    }

    /// Traverse full logical planes spanning `axes`.
    ///
    /// `axes` may contain any non-empty set of unique axes.
    pub fn planes(axes: Vec<usize>) -> Self {
        Self {
            mode: TraversalCacheMode::Planes { axes },
            axis_path: None,
            section: None,
        }
    }

    /// Override the traversal axis order.
    ///
    /// The first axis in the path varies fastest.
    pub fn axis_path(mut self, axis_path: Vec<usize>) -> Self {
        self.axis_path = Some(axis_path);
        self
    }

    /// Restrict traversal to a unit-stride subsection.
    pub fn section(mut self, start: Vec<usize>, shape: Vec<usize>) -> Self {
        let stride = vec![1; start.len()];
        self.section = Some(TraversalSection {
            start,
            shape,
            stride,
        });
        self
    }

    /// Restrict traversal to a subsection with an explicit stride.
    ///
    /// Non-unit strides are rejected in v1, but this entrypoint makes that
    /// limitation explicit and testable.
    pub fn section_with_stride(
        mut self,
        start: Vec<usize>,
        shape: Vec<usize>,
        stride: Vec<usize>,
    ) -> Self {
        self.section = Some(TraversalSection {
            start,
            shape,
            stride,
        });
        self
    }
}

/// Metadata describing a single traversal cursor.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TraversalCursor {
    /// Inclusive start position of the chunk in the parent lattice.
    pub position: Vec<usize>,
    /// Inclusive end position of the chunk in the parent lattice.
    pub end_position: Vec<usize>,
    /// Effective chunk shape after boundary clipping.
    pub shape: Vec<usize>,
}

/// A chunk of lattice data together with its traversal metadata.
#[derive(Clone, Debug)]
pub struct TraversalChunk<T> {
    /// The chunk data.
    pub data: ArrayD<T>,
    /// Metadata describing where the chunk came from.
    pub cursor: TraversalCursor,
}

#[derive(Clone, Debug)]
struct TraversalPlan {
    mode: TraversalCacheMode,
    start: Vec<usize>,
    shape: Vec<usize>,
    requested_cursor: Vec<usize>,
    axis_path: Vec<usize>,
    total_steps: usize,
    is_empty: bool,
}

impl TraversalPlan {
    fn new(
        lattice_shape: &[usize],
        nice_cursor_shape: &[usize],
        spec: TraversalSpec,
    ) -> Result<Self, LatticeError> {
        let ndim = lattice_shape.len();
        let axis_path = validate_axis_path(ndim, spec.axis_path)?;
        let section = spec.section.unwrap_or_else(|| TraversalSection {
            start: vec![0; ndim],
            shape: lattice_shape.to_vec(),
            stride: vec![1; ndim],
        });
        validate_section(lattice_shape, &section)?;

        let mode = spec.mode;
        let requested_cursor = match &mode {
            TraversalCacheMode::Chunks(cursor_shape) => {
                if cursor_shape.len() != ndim {
                    return Err(LatticeError::NdimMismatch {
                        expected: ndim,
                        got: cursor_shape.len(),
                    });
                }
                validate_positive_extents(cursor_shape, "chunk cursor shape")?;
                cursor_shape.clone()
            }
            TraversalCacheMode::Tiles => {
                if nice_cursor_shape.len() != ndim {
                    return Err(LatticeError::NdimMismatch {
                        expected: ndim,
                        got: nice_cursor_shape.len(),
                    });
                }
                validate_positive_extents(nice_cursor_shape, "tile cursor shape")?;
                nice_cursor_shape.to_vec()
            }
            TraversalCacheMode::Lines { axis } => {
                validate_axis(*axis, ndim, "line axis")?;
                let mut cursor = vec![1; ndim];
                if ndim > 0 {
                    cursor[*axis] = section.shape[*axis];
                }
                cursor
            }
            TraversalCacheMode::Planes { axes } => {
                if axes.is_empty() {
                    return Err(LatticeError::InvalidTraversal(
                        "plane traversal requires at least one axis".into(),
                    ));
                }
                let mut seen = vec![false; ndim];
                let mut cursor = vec![1; ndim];
                for &axis in axes {
                    validate_axis(axis, ndim, "plane axis")?;
                    if seen[axis] {
                        return Err(LatticeError::InvalidTraversal(format!(
                            "plane axis {axis} was specified more than once"
                        )));
                    }
                    seen[axis] = true;
                    cursor[axis] = section.shape[axis];
                }
                cursor
            }
        };

        let is_empty = section.shape.contains(&0);
        let total_steps = if is_empty {
            0
        } else {
            section
                .shape
                .iter()
                .zip(requested_cursor.iter())
                .map(|(&size, &cursor)| size.div_ceil(cursor.max(1)))
                .product()
        };

        Ok(Self {
            mode,
            start: section.start,
            shape: section.shape,
            requested_cursor,
            axis_path,
            total_steps,
            is_empty,
        })
    }

    fn cursor_shape_at(&self, relative_position: &[usize]) -> Vec<usize> {
        self.shape
            .iter()
            .zip(self.requested_cursor.iter())
            .zip(relative_position.iter())
            .map(|((&size, &cursor), &position)| cursor.min(size - position))
            .collect()
    }

    fn make_cursor(&self, relative_position: &[usize]) -> TraversalCursor {
        let shape = self.cursor_shape_at(relative_position);
        let position: Vec<usize> = self
            .start
            .iter()
            .zip(relative_position.iter())
            .map(|(&start, &offset)| start + offset)
            .collect();
        let end_position: Vec<usize> = position
            .iter()
            .zip(shape.iter())
            .map(|(&start, &extent)| start + extent - 1)
            .collect();
        TraversalCursor {
            position,
            end_position,
            shape,
        }
    }

    fn cache_hint(&self) -> TraversalCacheHint {
        TraversalCacheHint {
            mode: self.mode.clone(),
            section_start: self.start.clone(),
            section_shape: self.shape.clone(),
            requested_cursor: self.requested_cursor.clone(),
            axis_path: self.axis_path.clone(),
        }
    }
}

/// Cursor-only iterator derived from a [`TraversalSpec`].
///
/// This is useful when callers want the traversal order and cursor metadata
/// without immediately reading data from a lattice.
pub struct TraversalCursorIter {
    plan: Option<TraversalPlan>,
    init_error: Option<LatticeError>,
    relative_position: Vec<usize>,
    started: bool,
    done: bool,
}

impl TraversalCursorIter {
    /// Creates a new cursor iterator.
    pub fn new(
        lattice_shape: Vec<usize>,
        nice_cursor_shape: Vec<usize>,
        spec: TraversalSpec,
    ) -> Self {
        match TraversalPlan::new(&lattice_shape, &nice_cursor_shape, spec) {
            Ok(plan) => {
                let ndim = plan.shape.len();
                Self {
                    done: plan.is_empty,
                    plan: Some(plan),
                    init_error: None,
                    relative_position: vec![0; ndim],
                    started: false,
                }
            }
            Err(err) => Self {
                plan: None,
                init_error: Some(err),
                relative_position: Vec::new(),
                started: false,
                done: false,
            },
        }
    }

    fn advance(relative_position: &mut [usize], plan: &TraversalPlan) -> bool {
        for &axis in &plan.axis_path {
            relative_position[axis] += plan.requested_cursor[axis];
            if relative_position[axis] < plan.shape[axis] {
                return true;
            }
            relative_position[axis] = 0;
        }
        false
    }

    /// Returns the validated cache hint for this traversal, if initialization
    /// succeeded.
    pub fn cache_hint(&self) -> Option<TraversalCacheHint> {
        self.plan.as_ref().map(TraversalPlan::cache_hint)
    }
}

impl Iterator for TraversalCursorIter {
    type Item = Result<TraversalCursor, LatticeError>;

    fn next(&mut self) -> Option<Self::Item> {
        if let Some(err) = self.init_error.take() {
            self.done = true;
            return Some(Err(err));
        }
        if self.done {
            return None;
        }

        let plan = self.plan.as_ref().expect("cursor iterator plan exists");
        if !self.started {
            self.started = true;
            return Some(Ok(plan.make_cursor(&self.relative_position)));
        }
        if !Self::advance(&mut self.relative_position, plan) {
            self.done = true;
            return None;
        }
        Some(Ok(plan.make_cursor(&self.relative_position)))
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        let upper = self
            .plan
            .as_ref()
            .map(|plan| plan.total_steps)
            .or_else(|| self.init_error.as_ref().map(|_| 1))
            .unwrap_or(0);
        (0, Some(upper))
    }
}

/// Read-only traversal iterator over lattice data.
pub struct TraversalIter<'a, T: LatticeElement, L: Lattice<T> + ?Sized> {
    lattice: &'a L,
    cursors: TraversalCursorIter,
    init_error: Option<LatticeError>,
    _cache_scope: Option<Box<dyn TraversalCacheScope + 'a>>,
    _phantom: std::marker::PhantomData<T>,
}

impl<'a, T: LatticeElement, L: Lattice<T> + ?Sized> TraversalIter<'a, T, L> {
    /// Creates a new traversal iterator for `lattice`.
    pub fn new(lattice: &'a L, spec: TraversalSpec) -> Self {
        let cursors =
            TraversalCursorIter::new(lattice.shape().to_vec(), lattice.nice_cursor_shape(), spec);
        let (init_error, cache_scope) = match cursors.cache_hint() {
            Some(hint) => match lattice.enter_traversal_cache_scope(&hint) {
                Ok(scope) => (None, scope),
                Err(err) => (Some(err), None),
            },
            None => (None, None),
        };
        Self {
            lattice,
            cursors,
            init_error,
            _cache_scope: cache_scope,
            _phantom: std::marker::PhantomData,
        }
    }
}

impl<'a, T: LatticeElement, L: Lattice<T> + ?Sized> Iterator for TraversalIter<'a, T, L> {
    type Item = Result<TraversalChunk<T>, LatticeError>;

    fn next(&mut self) -> Option<Self::Item> {
        if let Some(err) = self.init_error.take() {
            return Some(Err(err));
        }
        let cursor = self.cursors.next()?;
        Some(cursor.and_then(|cursor| {
            let stride = vec![1; cursor.position.len()];
            self.lattice
                .get_slice(&cursor.position, &cursor.shape, &stride)
                .map(|data| TraversalChunk { data, cursor })
        }))
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        self.cursors.size_hint()
    }
}

/// Returns a recommended tile-cache size, in tiles, for the given traversal.
///
/// This is a simplified generalization of the C++ iterator cache-sizing logic:
/// the cache should at least hold the tiles intersecting one traversal cursor.
#[doc(hidden)]
pub fn recommended_tile_cache_size(
    cube_shape: &[usize],
    tile_shape: &[usize],
    hint: &TraversalCacheHint,
    max_cache_tiles: Option<usize>,
) -> usize {
    if cube_shape.len() != tile_shape.len()
        || cube_shape.len() != hint.section_shape.len()
        || cube_shape.len() != hint.requested_cursor.len()
    {
        return 1;
    }
    if cube_shape.is_empty() || tile_shape.contains(&0) {
        return 1;
    }

    match &hint.mode {
        TraversalCacheMode::Chunks(cursor) => {
            recommended_tile_cache_size_for_cursor(cube_shape, tile_shape, cursor, max_cache_tiles)
        }
        TraversalCacheMode::Tiles => 1,
        TraversalCacheMode::Lines { axis } => {
            let mut cursor = vec![1; cube_shape.len()];
            cursor[*axis] = hint.section_shape[*axis];
            recommended_tile_cache_size_for_cursor(cube_shape, tile_shape, &cursor, max_cache_tiles)
        }
        TraversalCacheMode::Planes { axes } => {
            let mut cursor = vec![1; cube_shape.len()];
            for &axis in axes {
                if axis < cursor.len() {
                    cursor[axis] = hint.section_shape[axis];
                }
            }
            recommended_tile_cache_size_for_cursor(cube_shape, tile_shape, &cursor, max_cache_tiles)
        }
    }
}

fn recommended_tile_cache_size_for_cursor(
    cube_shape: &[usize],
    tile_shape: &[usize],
    requested_cursor: &[usize],
    max_cache_tiles: Option<usize>,
) -> usize {
    let intersecting_tiles = cube_shape
        .iter()
        .zip(tile_shape.iter())
        .zip(requested_cursor.iter())
        .map(|((&cube, &tile), &cursor)| {
            if cube == 0 || tile == 0 {
                1
            } else {
                cursor.min(cube).div_ceil(tile)
            }
        })
        .product::<usize>()
        .max(1);
    max_cache_tiles
        .map(|limit| intersecting_tiles.min(limit.max(1)))
        .unwrap_or(intersecting_tiles)
}

fn validate_axis(axis: usize, ndim: usize, label: &str) -> Result<(), LatticeError> {
    if axis >= ndim {
        return Err(LatticeError::InvalidTraversal(format!(
            "{label} {axis} is out of bounds for rank {ndim}"
        )));
    }
    Ok(())
}

fn validate_axis_path(
    ndim: usize,
    axis_path: Option<Vec<usize>>,
) -> Result<Vec<usize>, LatticeError> {
    let axis_path = axis_path.unwrap_or_else(|| (0..ndim).collect());
    if axis_path.len() != ndim {
        return Err(LatticeError::NdimMismatch {
            expected: ndim,
            got: axis_path.len(),
        });
    }
    let mut seen = vec![false; ndim];
    for &axis in &axis_path {
        validate_axis(axis, ndim, "axis path element")?;
        if seen[axis] {
            return Err(LatticeError::InvalidTraversal(format!(
                "axis path contains duplicate axis {axis}"
            )));
        }
        seen[axis] = true;
    }
    Ok(axis_path)
}

fn validate_positive_extents(extents: &[usize], label: &str) -> Result<(), LatticeError> {
    if let Some(axis) = extents.iter().position(|&extent| extent == 0) {
        return Err(LatticeError::InvalidTraversal(format!(
            "{label} contains a zero extent on axis {axis}"
        )));
    }
    Ok(())
}

fn validate_section(
    lattice_shape: &[usize],
    section: &TraversalSection,
) -> Result<(), LatticeError> {
    let ndim = lattice_shape.len();
    if section.start.len() != ndim || section.shape.len() != ndim || section.stride.len() != ndim {
        return Err(LatticeError::NdimMismatch {
            expected: ndim,
            got: section.start.len(),
        });
    }
    if section.stride.iter().any(|&stride| stride != 1) {
        return Err(LatticeError::InvalidTraversal(
            "strided sections are not supported yet".into(),
        ));
    }
    for axis in 0..ndim {
        let start = section.start[axis];
        let shape = section.shape[axis];
        let limit = lattice_shape[axis];
        if start > limit || start.saturating_add(shape) > limit {
            return Err(LatticeError::SliceOutOfBounds {
                start: section.start.clone(),
                slice_shape: section.shape.clone(),
                stride: section.stride.clone(),
                lattice_shape: lattice_shape.to_vec(),
            });
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{ArrayLattice, LatticeIterExt, LatticeMut};
    use ndarray::IxDyn;

    #[test]
    fn chunk_traversal_covers_all_pixels() {
        let data = ArrayD::from_shape_fn(IxDyn(&[6, 4]), |idx| (idx[0] * 4 + idx[1]) as f64);
        let lat = ArrayLattice::new(data.clone());
        let total: f64 = lat
            .traverse(TraversalSpec::chunks(vec![3, 2]))
            .map(|chunk| chunk.unwrap().data.sum())
            .sum();
        assert_eq!(total, data.sum());
    }

    #[test]
    fn line_traversal_returns_full_lines() {
        let data = ArrayD::from_shape_fn(IxDyn(&[4, 3]), |idx| (idx[0] * 3 + idx[1]) as f64);
        let lat = ArrayLattice::new(data);
        let chunks: Vec<_> = lat
            .traverse(TraversalSpec::lines(0))
            .map(|chunk| chunk.unwrap())
            .collect();
        assert_eq!(chunks.len(), 3);
        assert_eq!(chunks[0].cursor.position, vec![0, 0]);
        assert_eq!(chunks[0].cursor.shape, vec![4, 1]);
    }

    #[test]
    fn plane_traversal_respects_orientation() {
        let data = ArrayD::from_shape_fn(IxDyn(&[3, 4, 2]), |idx| {
            (idx[0] * 100 + idx[1] * 10 + idx[2]) as f64
        });
        let lat = ArrayLattice::new(data);
        let chunks: Vec<_> = lat
            .traverse(TraversalSpec::planes(vec![0, 2]))
            .map(|chunk| chunk.unwrap())
            .collect();
        assert_eq!(chunks.len(), 4);
        assert_eq!(chunks[0].cursor.shape, vec![3, 1, 2]);
        assert_eq!(chunks[3].cursor.position, vec![0, 3, 0]);
    }

    #[test]
    fn axis_path_changes_traversal_order() {
        let lat = ArrayLattice::<f64>::zeros(vec![4, 3, 2]);
        let positions: Vec<_> = lat
            .traverse(TraversalSpec::lines(2).axis_path(vec![1, 0, 2]))
            .map(|chunk| chunk.unwrap().cursor.position)
            .collect();
        assert_eq!(positions[0], vec![0, 0, 0]);
        assert_eq!(positions[1], vec![0, 1, 0]);
        assert_eq!(positions[2], vec![0, 2, 0]);
    }

    #[test]
    fn section_limits_positions_and_end_positions() {
        let lat = ArrayLattice::<f64>::zeros(vec![6, 5]);
        let chunks: Vec<_> = lat
            .traverse(TraversalSpec::chunks(vec![2, 2]).section(vec![1, 2], vec![4, 3]))
            .map(|chunk| chunk.unwrap().cursor)
            .collect();
        assert_eq!(chunks[0].position, vec![1, 2]);
        assert_eq!(chunks[0].end_position, vec![2, 3]);
        assert_eq!(chunks[3].position, vec![3, 4]);
        assert_eq!(chunks[3].end_position, vec![4, 4]);
    }

    #[test]
    fn mutable_traversal_writes_back() {
        let mut lat = ArrayLattice::<f64>::zeros(vec![4, 4]);
        lat.for_each_chunk_mut(TraversalSpec::chunks(vec![2, 2]), |data, cursor| {
            let base = cursor.position.iter().sum::<usize>() as f64;
            data.fill(base);
            Ok(())
        })
        .unwrap();
        let result = lat.get().unwrap();
        assert_eq!(result[[0, 0]], 0.0);
        assert_eq!(result[[0, 2]], 2.0);
        assert_eq!(result[[2, 0]], 2.0);
        assert_eq!(result[[2, 2]], 4.0);
    }

    #[test]
    fn strided_sections_are_rejected() {
        let lat = ArrayLattice::<f64>::zeros(vec![4, 4]);
        let err = lat
            .traverse(TraversalSpec::tiles().section_with_stride(
                vec![0, 0],
                vec![4, 4],
                vec![1, 2],
            ))
            .next()
            .unwrap()
            .unwrap_err();
        assert!(matches!(err, LatticeError::InvalidTraversal(_)));
    }

    #[test]
    fn compatibility_iter_lines_still_cover_all_pixels() {
        let data = ArrayD::from_shape_fn(IxDyn(&[4, 3]), |idx| (idx[0] * 3 + idx[1]) as f64);
        let lat = ArrayLattice::new(data);
        let total: f64 = lat.iter_lines(0).flat_map(|line| line.into_iter()).sum();
        assert_eq!(total, (0..12).sum::<i32>() as f64);
    }

    #[test]
    fn tile_cache_hint_for_tiles_is_one_tile() {
        let hint = TraversalCacheHint {
            mode: TraversalCacheMode::Tiles,
            section_start: vec![0, 0, 0],
            section_shape: vec![64, 64, 16],
            requested_cursor: vec![8, 4, 2],
            axis_path: vec![0, 1, 2],
        };
        assert_eq!(
            recommended_tile_cache_size(&[64, 64, 16], &[8, 4, 2], &hint, None),
            1
        );
    }

    #[test]
    fn tile_cache_hint_for_lines_tracks_tiles_along_line_axis() {
        let hint = TraversalCacheHint {
            mode: TraversalCacheMode::Lines { axis: 1 },
            section_start: vec![0, 0, 0],
            section_shape: vec![64, 64, 16],
            requested_cursor: vec![1, 64, 1],
            axis_path: vec![0, 1, 2],
        };
        assert_eq!(
            recommended_tile_cache_size(&[64, 64, 16], &[8, 4, 2], &hint, None),
            16
        );
    }
}

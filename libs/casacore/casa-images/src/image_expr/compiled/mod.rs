// SPDX-License-Identifier: LGPL-3.0-or-later
//! Compiled, owned image-expression execution.

use std::any::Any;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use casa_coordinates::CoordinateSystem;
use casa_core::{ArrayD, RecordValue};
use casa_lattices::execution::{
    CursorMapWriteConfig, ExecutionInputs, ExecutionMode, ExecutionResources,
    OrderedCursorMapWriteExecutionStrategy, ParallelReadChunkConfig, SourceResidency,
    plan_execution, try_map_traversal_cursors_ordered_with_strategy,
};
use casa_lattices::{
    ExecutionPolicy, Lattice, LatticeError, LatticeMut, PagedArray, TiledShape,
    TraversalCursorIter, TraversalSpec,
};
use casa_tables::{TilePixel, TiledArrayStorage};
use ndarray::{IxDyn, Zip};

use crate::error::ImageError;
use crate::image::{ImageInterface, ImagePixel, PagedImage};
use crate::image_expr_lel::{
    WelfordAccum, broadcast_array, broadcast_lattice_array, can_broadcast_to, choose_rhs_meta,
    combine_slice_masks, eval_indexin, eval_pa, eval_spectral_index, fractile_in_place,
    rebin_child_slice, rebin_mask_slice, rebin_reduce_slice, slice_for_broadcast_child,
};
use crate::image_expr_ops::{apply_binary, apply_unary};
use crate::image_info::ImageInfo;
use crate::temp_image::TempImage;

use super::{
    BinaryExprFn, ImageExprBinaryOp, ImageExprBuilder, ImageExprCompareOp, ImageExprMeta,
    ImageExprUnaryOp, ImageExprValue, MaskExprBuilder, MaskExprNode, MaskLogicalOp,
    NumericExprNode, ReductionOp, SectionSource, clamp_cursor_shape, thread_parallelism,
    validate_slice_request, work_balanced_cursor_shape, write_chunk_into_array,
};

mod compile;
mod execution;
mod mask;
mod numeric;

pub use execution::MemorySnapshotImage;
pub use execution::bounded_read_cursor;
use execution::*;

#[derive(Clone)]
enum CompiledImageSourceKind<T: ImageExprValue> {
    Snapshot(Arc<ArrayD<T>>),
    Sections(SectionSource<T>),
    Paged {
        path: Arc<PathBuf>,
        cache_bytes: usize,
        lock_reads: bool,
    },
}

#[derive(Clone)]
struct CompiledImageSource<T: ImageExprValue> {
    kind: CompiledImageSourceKind<T>,
    shape: Vec<usize>,
    has_mask: bool,
    mask_snapshot: Option<Arc<ArrayD<bool>>>,
    tile_shape: Option<Vec<usize>>,
    reference_count: usize,
}

#[derive(Clone, Default)]
struct CompiledImageArena<T: ImageExprValue> {
    sources: Vec<CompiledImageSource<T>>,
}

impl<T: ImageExprValue> CompiledImageArena<T> {
    fn has_paged_sources(&self) -> bool {
        self.sources
            .iter()
            .any(|source| !matches!(source.kind, CompiledImageSourceKind::Snapshot(_)))
    }

    fn repeated_source_count(&self) -> usize {
        self.sources
            .iter()
            .filter(|source| source.reference_count > 1)
            .count()
    }
}

struct CompiledEvalContext<T: ImageExprValue> {
    opened_sources: Vec<Option<PagedImage<T>>>,
    opened_tiled_sources: Vec<Option<TiledArrayStorage>>,
    cached_source_slices: Vec<Option<CachedSourceSlice<T>>>,
    numeric_scalars: std::collections::HashMap<usize, T>,
    boolean_scalars: std::collections::HashMap<usize, bool>,
}

struct CachedSourceSlice<T: ImageExprValue> {
    start: Vec<usize>,
    shape: Vec<usize>,
    stride: Vec<usize>,
    data: ArrayD<T>,
}

impl<T: ImageExprValue> CompiledEvalContext<T> {
    fn new(source_count: usize) -> Self {
        Self {
            opened_sources: std::iter::repeat_with(|| None).take(source_count).collect(),
            opened_tiled_sources: std::iter::repeat_with(|| None).take(source_count).collect(),
            cached_source_slices: std::iter::repeat_with(|| None).take(source_count).collect(),
            numeric_scalars: Default::default(),
            boolean_scalars: Default::default(),
        }
    }

    fn source_tiled_io_mut<'a>(
        &'a mut self,
        arena: &CompiledImageArena<T>,
        source_id: usize,
    ) -> Result<&'a mut TiledArrayStorage, LatticeError>
    where
        T: TilePixel,
    {
        if self.opened_tiled_sources[source_id].is_none() {
            let source = &arena.sources[source_id];
            let CompiledImageSourceKind::Paged {
                path, cache_bytes, ..
            } = &source.kind
            else {
                return Err(LatticeError::InvalidTraversal(
                    "compiled paged source expected persistent image".to_string(),
                ));
            };
            let (primary, fallback) = if path.join("table.f1").exists() {
                (1, 0)
            } else {
                (0, 1)
            };
            let tiled_io =
                TiledArrayStorage::open_with_cache::<T>(path.as_path(), primary, *cache_bytes)
                    .or_else(|_| {
                        TiledArrayStorage::open_with_cache::<T>(
                            path.as_path(),
                            fallback,
                            *cache_bytes,
                        )
                    })
                    .map_err(|e| LatticeError::Table(e.to_string()))?;
            self.opened_tiled_sources[source_id] = Some(tiled_io);
        }
        Ok(self.opened_tiled_sources[source_id]
            .as_mut()
            .expect("compiled paged tiled source opened"))
    }

    fn source_image_mut<'a>(
        &'a mut self,
        arena: &CompiledImageArena<T>,
        source_id: usize,
    ) -> Result<&'a mut PagedImage<T>, LatticeError> {
        if self.opened_sources[source_id].is_none() {
            let source = &arena.sources[source_id];
            let CompiledImageSourceKind::Paged {
                path,
                cache_bytes,
                lock_reads,
            } = &source.kind
            else {
                return Err(LatticeError::InvalidTraversal(
                    "compiled paged source expected persistent image".to_string(),
                ));
            };
            #[cfg(unix)]
            let image = if *lock_reads {
                PagedImage::<T>::open_read_locked_with_cache(path.as_path(), *cache_bytes)
            } else {
                PagedImage::<T>::open_with_cache(path.as_path(), *cache_bytes)
            };
            #[cfg(not(unix))]
            let image = {
                let _ = lock_reads;
                PagedImage::<T>::open_with_cache(path.as_path(), *cache_bytes)
            };
            let image = image.map_err(|e| LatticeError::Table(e.to_string()))?;
            self.opened_sources[source_id] = Some(image);
        }
        Ok(self.opened_sources[source_id]
            .as_mut()
            .expect("compiled paged source opened"))
    }
}

impl<T: ImageExprValue> CompiledImageSource<T> {
    fn read_slice_uncached(
        &self,
        arena: &CompiledImageArena<T>,
        ctx: &mut CompiledEvalContext<T>,
        source_id: usize,
        start: &[usize],
        shape: &[usize],
        stride: &[usize],
    ) -> Result<ArrayD<T>, LatticeError>
    where
        T: TilePixel,
    {
        match &self.kind {
            CompiledImageSourceKind::Snapshot(data) => {
                slice_array_owned(data.as_ref(), start, shape, stride)
            }
            CompiledImageSourceKind::Sections(source) => (source.read)(start, shape, stride)
                .map_err(|error| LatticeError::Table(error.to_string())),
            CompiledImageSourceKind::Paged {
                lock_reads: false, ..
            } if stride.iter().all(|&step| step == 1) => ctx
                .source_tiled_io_mut(arena, source_id)?
                .get_slice::<T>(start, shape)
                .map_err(|e| LatticeError::Table(e.to_string())),
            CompiledImageSourceKind::Paged { .. } => ctx
                .source_image_mut(arena, source_id)?
                .get_slice_with_stride(start, shape, stride)
                .map_err(|e| LatticeError::Table(e.to_string())),
        }
    }

    fn eval_slice(
        &self,
        arena: &CompiledImageArena<T>,
        ctx: &mut CompiledEvalContext<T>,
        source_id: usize,
        start: &[usize],
        shape: &[usize],
        stride: &[usize],
    ) -> Result<ArrayD<T>, LatticeError>
    where
        T: TilePixel,
    {
        if self.reference_count <= 1 {
            return self.read_slice_uncached(arena, ctx, source_id, start, shape, stride);
        }
        if let Some(cached) = &ctx.cached_source_slices[source_id]
            && cached.start == start
            && cached.shape == shape
            && cached.stride == stride
        {
            return Ok(cached.data.clone());
        }
        let data = self.read_slice_uncached(arena, ctx, source_id, start, shape, stride)?;
        ctx.cached_source_slices[source_id] = Some(CachedSourceSlice {
            start: start.to_vec(),
            shape: shape.to_vec(),
            stride: stride.to_vec(),
            data: data.clone(),
        });
        Ok(data)
    }

    fn eval_mask_slice(
        &self,
        arena: &CompiledImageArena<T>,
        ctx: &mut CompiledEvalContext<T>,
        source_id: usize,
        start: &[usize],
        shape: &[usize],
        stride: &[usize],
    ) -> Result<Option<ArrayD<bool>>, LatticeError> {
        if !self.has_mask {
            return Ok(None);
        }
        if let Some(mask) = &self.mask_snapshot {
            return slice_array_owned(mask.as_ref(), start, shape, stride).map(Some);
        }
        if let CompiledImageSourceKind::Sections(source) = &self.kind {
            return source.mask.as_ref().expect("source advertises a mask")(start, shape, stride)
                .map_err(|error| LatticeError::Table(error.to_string()));
        }
        ctx.source_image_mut(arena, source_id)?
            .get_mask_slice(start, shape, stride)
            .map_err(|e| LatticeError::Table(e.to_string()))
    }
}

#[derive(Clone)]
enum CompiledNumericExprNode<T: ImageExprValue> {
    Source(usize),
    Scalar(T),
    UnaryOp {
        op: ImageExprUnaryOp,
        child: Box<Self>,
    },
    BinaryOp {
        op: ImageExprBinaryOp,
        lhs: Box<Self>,
        lhs_shape: Vec<usize>,
        rhs: Box<Self>,
        rhs_shape: Vec<usize>,
    },
    CustomUnary {
        child: Box<Self>,
        func: BinaryOrUnary<T>,
    },
    CustomBinary {
        lhs: Box<Self>,
        lhs_shape: Vec<usize>,
        rhs: Box<Self>,
        rhs_shape: Vec<usize>,
        func: BinaryOrUnary<T>,
    },
    Reduction {
        op: ReductionOp,
        child: Box<Self>,
        child_shape: Vec<usize>,
        empty_value: Option<T>,
    },
    Fractile {
        child: Box<Self>,
        child_shape: Vec<usize>,
        fraction: f64,
    },
    FractileRange {
        child: Box<Self>,
        child_shape: Vec<usize>,
        fraction1: f64,
        fraction2: f64,
    },
    Conditional {
        condition: Box<CompiledMaskExprNode<T>>,
        cond_shape: Vec<usize>,
        if_true: Box<Self>,
        true_shape: Vec<usize>,
        if_false: Box<Self>,
        false_shape: Vec<usize>,
    },
    MaskCount {
        count_true: bool,
        mask: Box<CompiledMaskExprNode<T>>,
        mask_shape: Vec<usize>,
    },
    Replace {
        primary: Box<Self>,
        primary_shape: Vec<usize>,
        replacement: Box<Self>,
        replacement_shape: Vec<usize>,
        mask: Option<Arc<ArrayD<bool>>>,
    },
    StripMask {
        child: Box<Self>,
    },
    SpectralIndex {
        lhs: Box<Self>,
        lhs_shape: Vec<usize>,
        rhs: Box<Self>,
        rhs_shape: Vec<usize>,
        spec_axis_a: usize,
        freqs_a: Vec<f64>,
        spec_axis_b: usize,
        freqs_b: Vec<f64>,
    },
    Pa {
        u: Box<Self>,
        u_shape: Vec<usize>,
        q: Box<Self>,
        q_shape: Vec<usize>,
    },
    Rebin {
        child: Box<Self>,
        in_shape: Vec<usize>,
        factors: Vec<usize>,
    },
}

#[derive(Clone)]
enum BinaryOrUnary<T: ImageExprValue> {
    Unary(super::UnaryExprFn<T>),
    Binary(BinaryExprFn<T>),
}

#[derive(Clone)]
enum CompiledMaskExprNode<T: ImageExprValue> {
    CompareScalar {
        op: ImageExprCompareOp,
        expr: Box<CompiledNumericExprNode<T>>,
        scalar: T,
    },
    CompareExpr {
        op: ImageExprCompareOp,
        lhs: Box<CompiledNumericExprNode<T>>,
        lhs_shape: Vec<usize>,
        rhs: Box<CompiledNumericExprNode<T>>,
        rhs_shape: Vec<usize>,
    },
    Logical {
        op: MaskLogicalOp,
        lhs: Box<Self>,
        lhs_shape: Vec<usize>,
        rhs: Box<Self>,
        rhs_shape: Vec<usize>,
    },
    Not {
        child: Box<Self>,
    },
    IsNan {
        child: Box<CompiledNumericExprNode<T>>,
    },
    Validity {
        child: Box<CompiledNumericExprNode<T>>,
    },
    BooleanValidity {
        child: Box<Self>,
    },
    ConstantMask {
        mask: Arc<ArrayD<bool>>,
    },
    AllReduce {
        child: Box<Self>,
        child_shape: Vec<usize>,
    },
    AnyReduce {
        child: Box<Self>,
        child_shape: Vec<usize>,
    },
    IndexIn {
        axis: i64,
        indices: Vec<i64>,
    },
}

#[derive(Clone)]
struct EvaluatedChunk<T> {
    position: Vec<usize>,
    data: ArrayD<T>,
}

struct EvaluatedSaveChunk<T> {
    position: Vec<usize>,
    data: ArrayD<T>,
    mask: Option<ArrayD<bool>>,
}

fn compiled_source_cache_bytes<T>(
    cursor_shape: &[usize],
    requested_cache_bytes: usize,
) -> Result<usize, ImageError> {
    if requested_cache_bytes > 0 {
        return Ok(requested_cache_bytes);
    }

    let tile_bytes = cursor_shape
        .iter()
        .try_fold(1usize, |product, &extent| product.checked_mul(extent))
        .and_then(|pixels| pixels.checked_mul(std::mem::size_of::<T>().max(1)))
        .ok_or_else(|| ImageError::Lattice("expression source cache byte overflow".into()))?;

    // Compiled evaluation reads the source chunk by chunk. Reusing the
    // unbounded `0` cache from `PagedImage::open()` would push reopened worker
    // handles through the flat-cache path, which is the wrong shape for this
    // workload. One exact cursor buffer is the owned cache allocation; queue
    // and worker buffers are budgeted separately by the execution planner.
    Ok(tile_bytes)
}

fn expand_tile_aligned_cursor_shape(
    full_shape: &[usize],
    tile_shape: &[usize],
    max_pixels: usize,
) -> Result<Vec<usize>, ImageError> {
    if full_shape.is_empty() {
        return Ok(vec![]);
    }

    let mut cursor = tile_shape
        .iter()
        .zip(full_shape.iter())
        .map(|(&tile, &extent)| tile.min(extent).max(1))
        .collect::<Vec<_>>();
    let mut product = cursor
        .iter()
        .try_fold(1usize, |product, &extent| product.checked_mul(extent))
        .ok_or_else(|| ImageError::Lattice("tile cursor product overflow".into()))?
        .max(1);

    for axis in 0..cursor.len() {
        let tile = tile_shape[axis].max(1);
        let extent = full_shape[axis];
        while cursor[axis] < extent {
            let next = cursor[axis]
                .checked_add(tile)
                .ok_or_else(|| ImageError::Lattice("tile cursor step overflow".into()))?
                .min(extent);
            let next_product = (product / cursor[axis].max(1))
                .checked_mul(next)
                .ok_or_else(|| ImageError::Lattice("tile cursor expansion overflow".into()))?;
            if next_product > max_pixels {
                break;
            }
            product = next_product;
            cursor[axis] = next;
        }
    }

    Ok(cursor)
}

fn work_balanced_pixel_target(
    full_shape: &[usize],
    worker_limit: usize,
) -> Result<usize, ImageError> {
    let total_pixels = full_shape
        .iter()
        .try_fold(1usize, |product, &extent| product.checked_mul(extent))
        .ok_or_else(|| ImageError::Lattice("expression shape product overflow".into()))?;
    Ok(total_pixels.div_ceil(worker_limit.max(1)).max(1))
}

fn mask_chunk_all(mask: &ArrayD<bool>) -> bool {
    mask.iter().all(|&value| value)
}

fn mask_chunk_none(mask: &ArrayD<bool>) -> bool {
    mask.iter().all(|&value| !value)
}

/// Owned, compiled numeric image expression that can use explicit execution
/// policy control during evaluation and materialization.
///
/// This is a Rust-specific performance API beyond C++ casacore's borrowed
/// `ImageExpr<T>` interface. Compilation replaces borrowed image references
/// with owned source descriptors:
///
/// - persistent paged images are reopened lazily on worker threads
/// - non-persistent or non-downcastable sources are snapshotted at compile time
///
/// The statistical or image values produced are unchanged; only the execution
/// strategy changes.
#[derive(Clone)]
pub struct ImageExpression<T: ImageExprValue> {
    node: CompiledNumericExprNode<T>,
    arena: Arc<CompiledImageArena<T>>,
    meta: ImageExprMeta,
    execution_policy: ExecutionPolicy,
    execution_resources: ExecutionResources,
    expr_string: Option<Arc<str>>,
}

/// Owned, compiled boolean mask expression sharing the same compiled source arena.
#[derive(Clone)]
pub struct MaskExpression<T: ImageExprValue> {
    node: CompiledMaskExprNode<T>,
    arena: Arc<CompiledImageArena<T>>,
    shape: Vec<usize>,
    execution_policy: ExecutionPolicy,
    execution_resources: ExecutionResources,
}

fn eval_child_slice_broadcast<T: ImageExprValue>(
    child: &CompiledNumericExprNode<T>,
    child_shape: &[usize],
    arena: &CompiledImageArena<T>,
    ctx: &mut CompiledEvalContext<T>,
    start: &[usize],
    shape: &[usize],
    stride: &[usize],
) -> Result<ArrayD<T>, LatticeError> {
    if child_shape.is_empty() {
        // The compiled node stays at this address for the evaluation. Cache
        // reductions across cursor reads, but recompute on a later evaluation.
        let key = std::ptr::from_ref(child) as usize;
        let value = match ctx.numeric_scalars.get(&key) {
            Some(&value) => value,
            None => {
                let scalar = child.eval_slice(arena, ctx, &[], &[], &[])?;
                let value = scalar[IxDyn(&[])];
                ctx.numeric_scalars.insert(key, value);
                value
            }
        };
        return Ok(ArrayD::from_elem(IxDyn(shape), value));
    }
    if child_shape.len() == start.len() && !child_shape.contains(&1) {
        return child.eval_slice(arena, ctx, start, shape, stride);
    }
    let (c_start, c_shape, c_stride) = slice_for_broadcast_child(child_shape, start, shape, stride);
    let data = child.eval_slice(arena, ctx, &c_start, &c_shape, &c_stride)?;
    broadcast_lattice_array(data, shape)
}

fn eval_child_mask_slice_broadcast<T: ImageExprValue>(
    child: &CompiledNumericExprNode<T>,
    child_shape: &[usize],
    arena: &CompiledImageArena<T>,
    ctx: &mut CompiledEvalContext<T>,
    start: &[usize],
    shape: &[usize],
    stride: &[usize],
) -> Result<Option<ArrayD<bool>>, LatticeError> {
    if !child.may_have_mask(arena) {
        return Ok(None);
    }
    if child_shape.len() == start.len() && !child_shape.contains(&1) {
        return child.eval_mask_slice(arena, ctx, start, shape, stride);
    }
    let (c_start, c_shape, c_stride) = slice_for_broadcast_child(child_shape, start, shape, stride);
    let Some(mask) = child.eval_mask_slice(arena, ctx, &c_start, &c_shape, &c_stride)? else {
        return Ok(None);
    };
    broadcast_lattice_array(mask, shape).map(Some)
}

fn eval_bool_slice_broadcast<T: ImageExprValue>(
    child: &CompiledMaskExprNode<T>,
    child_shape: &[usize],
    arena: &CompiledImageArena<T>,
    ctx: &mut CompiledEvalContext<T>,
    start: &[usize],
    shape: &[usize],
    stride: &[usize],
) -> Result<ArrayD<bool>, LatticeError> {
    if child.is_shape_polymorphic() && child_shape.is_empty() {
        return child.eval_slice(arena, ctx, start, shape, stride);
    }
    if child_shape.is_empty() {
        let key = std::ptr::from_ref(child) as usize;
        let value = match ctx.boolean_scalars.get(&key) {
            Some(&value) => value,
            None => {
                let scalar = child.eval_slice(arena, ctx, &[], &[], &[])?;
                let value = scalar[IxDyn(&[])];
                ctx.boolean_scalars.insert(key, value);
                value
            }
        };
        return Ok(ArrayD::from_elem(IxDyn(shape), value));
    }
    if child_shape.len() == start.len() && !child_shape.contains(&1) {
        return child.eval_slice(arena, ctx, start, shape, stride);
    }
    let (c_start, c_shape, c_stride) = slice_for_broadcast_child(child_shape, start, shape, stride);
    let data = child.eval_slice(arena, ctx, &c_start, &c_shape, &c_stride)?;
    broadcast_lattice_array(data, shape)
}

fn eval_bool_validity_broadcast<T: ImageExprValue>(
    child: &CompiledMaskExprNode<T>,
    child_shape: &[usize],
    arena: &CompiledImageArena<T>,
    ctx: &mut CompiledEvalContext<T>,
    start: &[usize],
    shape: &[usize],
    stride: &[usize],
) -> Result<Option<ArrayD<bool>>, LatticeError> {
    if !child.may_be_partially_invalid(arena) {
        return Ok(None);
    }
    if child.is_shape_polymorphic() && child_shape.is_empty() {
        return child.eval_validity_slice(arena, ctx, start, shape, stride);
    }
    if child_shape.is_empty() {
        return Ok(None);
    }
    if child_shape.len() == start.len() && !child_shape.contains(&1) {
        return child.eval_validity_slice(arena, ctx, start, shape, stride);
    }
    let (c_start, c_shape, c_stride) = slice_for_broadcast_child(child_shape, start, shape, stride);
    let Some(mask) = child.eval_validity_slice(arena, ctx, &c_start, &c_shape, &c_stride)? else {
        return Ok(None);
    };
    broadcast_lattice_array(mask, shape).map(Some)
}

impl<T: ImageExprValue> Lattice<T> for ImageExpression<T> {
    fn shape(&self) -> &[usize] {
        &self.meta.shape
    }

    fn is_writable(&self) -> bool {
        false
    }

    fn get_at(&self, position: &[usize]) -> Result<T, LatticeError> {
        ImageExpression::get_at(self, position).map_err(|e| LatticeError::Table(e.to_string()))
    }

    fn get_slice(
        &self,
        start: &[usize],
        shape: &[usize],
        stride: &[usize],
    ) -> Result<ArrayD<T>, LatticeError> {
        if stride.iter().all(|&value| value == 1) {
            ImageExpression::get_slice(self, start, shape)
                .map_err(|e| LatticeError::Table(e.to_string()))
        } else {
            ImageExpression::get_slice_with_stride(self, start, shape, stride)
                .map_err(|e| LatticeError::Table(e.to_string()))
        }
    }

    fn get(&self) -> Result<ArrayD<T>, LatticeError> {
        ImageExpression::get(self).map_err(|e| LatticeError::Table(e.to_string()))
    }
}

impl<T: ImageExprValue> ImageInterface<T> for ImageExpression<T> {
    fn as_any(&self) -> Option<&dyn Any> {
        Some(self)
    }

    fn coordinates(&self) -> &CoordinateSystem {
        &self.meta.coords
    }

    fn units(&self) -> &str {
        &self.meta.units
    }

    fn misc_info(&self) -> RecordValue {
        self.meta.misc_info.clone()
    }

    fn image_info(&self) -> Result<ImageInfo, ImageError> {
        Ok(self.meta.image_info.clone())
    }

    fn name(&self) -> Option<&Path> {
        self.meta.name.as_deref()
    }

    fn default_mask(&self) -> Result<Option<ArrayD<bool>>, ImageError> {
        Ok(self.eval_full_mask()?.map(|arc| (*arc).clone()))
    }

    fn get_mask_slice(
        &self,
        start: &[usize],
        shape: &[usize],
        stride: &[usize],
    ) -> Result<Option<ArrayD<bool>>, ImageError> {
        validate_slice_request(self.shape(), start, shape, stride)?;
        if !self.node.may_have_mask(&self.arena) {
            return Ok(None);
        }
        let mut ctx = CompiledEvalContext::new(self.arena.sources.len());
        self.node
            .eval_mask_slice(&self.arena, &mut ctx, start, shape, stride)
            .map_err(Into::into)
    }
}

impl<T: ImageExprValue> Lattice<bool> for MaskExpression<T> {
    fn shape(&self) -> &[usize] {
        &self.shape
    }

    fn is_writable(&self) -> bool {
        false
    }

    fn get_at(&self, position: &[usize]) -> Result<bool, LatticeError> {
        MaskExpression::get_at(self, position).map_err(|e| LatticeError::Table(e.to_string()))
    }

    fn get_slice(
        &self,
        start: &[usize],
        shape: &[usize],
        stride: &[usize],
    ) -> Result<ArrayD<bool>, LatticeError> {
        if stride.iter().all(|&value| value == 1) {
            MaskExpression::get_slice(self, start, shape)
                .map_err(|e| LatticeError::Table(e.to_string()))
        } else {
            validate_slice_request(&self.shape, start, shape, stride)?;
            let mut ctx = CompiledEvalContext::new(self.arena.sources.len());
            self.node
                .eval_slice(&self.arena, &mut ctx, start, shape, stride)
        }
    }

    fn get(&self) -> Result<ArrayD<bool>, LatticeError> {
        MaskExpression::get(self).map_err(|e| LatticeError::Table(e.to_string()))
    }
}

#[cfg(test)]
mod tests;

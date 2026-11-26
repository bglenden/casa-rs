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

struct CompileCtx<T: ImageExprValue> {
    sources: Vec<CompiledImageSource<T>>,
    paged_source_ids: std::collections::HashMap<PathBuf, usize>,
    transient_source_ids: std::collections::HashMap<usize, usize>,
}

impl<T: ImageExprValue> Default for CompileCtx<T> {
    fn default() -> Self {
        Self {
            sources: Vec::new(),
            paged_source_ids: std::collections::HashMap::new(),
            transient_source_ids: std::collections::HashMap::new(),
        }
    }
}

impl<T: ImageExprValue> CompileCtx<T> {
    fn register_snapshot_source(
        &mut self,
        data: ArrayD<T>,
        mask_snapshot: Option<Arc<ArrayD<bool>>>,
    ) -> usize {
        self.register_shared_snapshot_source(Arc::new(data), mask_snapshot)
    }

    fn register_shared_snapshot_source(
        &mut self,
        data: Arc<ArrayD<T>>,
        mask_snapshot: Option<Arc<ArrayD<bool>>>,
    ) -> usize {
        let source_id = self.sources.len();
        let shape = data.shape().to_vec();
        let has_mask = mask_snapshot.is_some();
        self.sources.push(CompiledImageSource {
            shape,
            has_mask,
            mask_snapshot,
            tile_shape: None,
            kind: CompiledImageSourceKind::Snapshot(data),
            reference_count: 1,
        });
        source_id
    }

    fn register_image_source(
        &mut self,
        image: &dyn ImageInterface<T>,
    ) -> Result<usize, ImageError> {
        let source_key = image as *const dyn ImageInterface<T> as *const () as usize;
        if let Some(any) = image.as_any() {
            if let Some(expression) = any.downcast_ref::<ImageExpression<T>>()
                && let CompiledNumericExprNode::Source(source_id) = expression.node
                && let CompiledImageSourceKind::Sections(sections) =
                    &expression.arena.sources[source_id].kind
            {
                if let Some(&source_id) = self.transient_source_ids.get(&source_key) {
                    self.sources[source_id].reference_count += 1;
                    return Ok(source_id);
                }
                let source_id = self.register_section_source(sections.clone());
                self.transient_source_ids.insert(source_key, source_id);
                return Ok(source_id);
            }
            if let Some(paged) = any.downcast_ref::<PagedImage<T>>()
                && let Some(path) = paged.name()
            {
                paged.flush_pixels_for_reopen()?;
                #[cfg(unix)]
                let lock_reads = paged.has_lock(casa_tables::LockType::Read);
                #[cfg(not(unix))]
                let lock_reads = false;
                if let Some(&source_id) = self.paged_source_ids.get(path) {
                    let source = &mut self.sources[source_id];
                    source.reference_count += 1;
                    if let CompiledImageSourceKind::Paged {
                        lock_reads: existing,
                        ..
                    } = &mut source.kind
                    {
                        *existing |= lock_reads;
                    }
                    return Ok(source_id);
                }
                let cursor_shape = clamp_cursor_shape(&paged.nice_cursor_shape(), paged.shape());
                let source_id = self.sources.len();
                let has_mask = paged.has_pixel_mask();
                self.sources.push(CompiledImageSource {
                    shape: paged.shape().to_vec(),
                    has_mask,
                    mask_snapshot: None,
                    kind: CompiledImageSourceKind::Paged {
                        path: Arc::new(path.to_path_buf()),
                        cache_bytes: compiled_source_cache_bytes::<T>(
                            &cursor_shape,
                            paged.cache_bytes(),
                        )?,
                        lock_reads,
                    },
                    tile_shape: Some(paged.tile_shape().to_vec()),
                    reference_count: 1,
                });
                self.paged_source_ids.insert(path.to_path_buf(), source_id);
                return Ok(source_id);
            }

            if let Some(mem) = any.downcast_ref::<MemorySnapshotImage<T>>() {
                if let Some(&source_id) = self.transient_source_ids.get(&source_key) {
                    self.sources[source_id].reference_count += 1;
                    return Ok(source_id);
                }
                let source_id =
                    self.register_shared_snapshot_source(mem.data.clone(), mem.mask.clone());
                self.transient_source_ids.insert(source_key, source_id);
                return Ok(source_id);
            }

            if let Some(temp) = any.downcast_ref::<TempImage<T>>() {
                if let Some(&source_id) = self.transient_source_ids.get(&source_key) {
                    self.sources[source_id].reference_count += 1;
                    return Ok(source_id);
                }
                let data = temp.get()?;
                let mask = temp.default_mask()?.map(Arc::new);
                let source_id = self.register_snapshot_source(data, mask);
                self.transient_source_ids.insert(source_key, source_id);
                return Ok(source_id);
            }
        }

        if let Some(&source_id) = self.transient_source_ids.get(&source_key) {
            self.sources[source_id].reference_count += 1;
            return Ok(source_id);
        }
        let data = image.get()?;
        let mask = image.default_mask()?.map(Arc::new);
        let source_id = self.register_snapshot_source(data, mask);
        self.transient_source_ids.insert(source_key, source_id);
        Ok(source_id)
    }

    fn register_section_source(&mut self, source: SectionSource<T>) -> usize {
        let source_id = self.sources.len();
        self.sources.push(CompiledImageSource {
            shape: source.shape.clone(),
            has_mask: source.mask.is_some(),
            mask_snapshot: None,
            tile_shape: None,
            kind: CompiledImageSourceKind::Sections(source),
            reference_count: 1,
        });
        source_id
    }

    fn compile_numeric(
        &mut self,
        node: &NumericExprNode<'_, T>,
        node_shape: &[usize],
    ) -> Result<CompiledNumericExprNode<T>, ImageError> {
        Ok(match node {
            NumericExprNode::Sections(source) => {
                CompiledNumericExprNode::Source(self.register_section_source(source.clone()))
            }
            NumericExprNode::Vector(values) => CompiledNumericExprNode::Source(
                self.register_shared_snapshot_source(values.clone(), None),
            ),
            NumericExprNode::Source(image) => {
                CompiledNumericExprNode::Source(self.register_image_source(*image)?)
            }
            NumericExprNode::Scalar(value) => CompiledNumericExprNode::Scalar(*value),
            NumericExprNode::UnaryOp { op, child } => CompiledNumericExprNode::UnaryOp {
                op: *op,
                child: Box::new(self.compile_numeric(
                    child,
                    &child.try_shape().unwrap_or_else(|| node_shape.to_vec()),
                )?),
            },
            NumericExprNode::BinaryOp { op, lhs, rhs } => {
                let lhs_shape = lhs.try_shape().unwrap_or_else(|| node_shape.to_vec());
                let rhs_shape = rhs.try_shape().unwrap_or_else(|| node_shape.to_vec());
                CompiledNumericExprNode::BinaryOp {
                    op: *op,
                    lhs: Box::new(self.compile_numeric(lhs, &lhs_shape)?),
                    lhs_shape,
                    rhs: Box::new(self.compile_numeric(rhs, &rhs_shape)?),
                    rhs_shape,
                }
            }
            NumericExprNode::CustomUnary { child, func, .. } => {
                CompiledNumericExprNode::CustomUnary {
                    child: Box::new(self.compile_numeric(
                        child,
                        &child.try_shape().unwrap_or_else(|| node_shape.to_vec()),
                    )?),
                    func: BinaryOrUnary::Unary(func.clone()),
                }
            }
            NumericExprNode::CustomBinary { lhs, rhs, func, .. } => {
                let lhs_shape = lhs.try_shape().unwrap_or_else(|| node_shape.to_vec());
                let rhs_shape = rhs.try_shape().unwrap_or_else(|| node_shape.to_vec());
                CompiledNumericExprNode::CustomBinary {
                    lhs: Box::new(self.compile_numeric(lhs, &lhs_shape)?),
                    lhs_shape,
                    rhs: Box::new(self.compile_numeric(rhs, &rhs_shape)?),
                    rhs_shape,
                    func: BinaryOrUnary::Binary(func.clone()),
                }
            }
            NumericExprNode::Reduction {
                op,
                child,
                child_shape,
                empty_value,
            } => CompiledNumericExprNode::Reduction {
                op: *op,
                child: Box::new(self.compile_numeric(child, child_shape)?),
                child_shape: child_shape.clone(),
                empty_value: *empty_value,
            },
            NumericExprNode::Fractile {
                child,
                child_shape,
                fraction,
            } => CompiledNumericExprNode::Fractile {
                child: Box::new(self.compile_numeric(child, child_shape)?),
                child_shape: child_shape.clone(),
                fraction: *fraction,
            },
            NumericExprNode::FractileRange {
                child,
                child_shape,
                fraction1,
                fraction2,
            } => CompiledNumericExprNode::FractileRange {
                child: Box::new(self.compile_numeric(child, child_shape)?),
                child_shape: child_shape.clone(),
                fraction1: *fraction1,
                fraction2: *fraction2,
            },
            NumericExprNode::Conditional {
                condition,
                if_true,
                if_false,
            } => {
                let cond_shape = condition.try_shape().unwrap_or_else(|| node_shape.to_vec());
                let true_shape = if_true.try_shape().unwrap_or_else(|| node_shape.to_vec());
                let false_shape = if_false.try_shape().unwrap_or_else(|| node_shape.to_vec());
                CompiledNumericExprNode::Conditional {
                    condition: Box::new(self.compile_mask(condition, &cond_shape)?),
                    cond_shape,
                    if_true: Box::new(self.compile_numeric(if_true, &true_shape)?),
                    true_shape,
                    if_false: Box::new(self.compile_numeric(if_false, &false_shape)?),
                    false_shape,
                }
            }
            NumericExprNode::MaskCount {
                count_true,
                mask,
                mask_shape,
            } => CompiledNumericExprNode::MaskCount {
                count_true: *count_true,
                mask: Box::new(self.compile_mask(mask, mask_shape)?),
                mask_shape: mask_shape.clone(),
            },
            NumericExprNode::Replace {
                primary,
                replacement,
                mask,
            } => {
                let primary_shape = primary.try_shape().unwrap_or_else(|| node_shape.to_vec());
                let replacement_shape = replacement
                    .try_shape()
                    .unwrap_or_else(|| node_shape.to_vec());
                CompiledNumericExprNode::Replace {
                    primary: Box::new(self.compile_numeric(primary, &primary_shape)?),
                    primary_shape,
                    replacement: Box::new(self.compile_numeric(replacement, &replacement_shape)?),
                    replacement_shape,
                    mask: mask.clone().map(Arc::new),
                }
            }
            NumericExprNode::TypeBridge { eval_fn } => {
                let start = vec![0; node_shape.len()];
                let stride = vec![1; node_shape.len()];
                let data = eval_fn(&start, node_shape, &stride)?;
                let source_id = self.register_snapshot_source(data, None);
                CompiledNumericExprNode::Source(source_id)
            }
            NumericExprNode::StripMask { child } => CompiledNumericExprNode::StripMask {
                child: Box::new(self.compile_numeric(
                    child,
                    &child.try_shape().unwrap_or_else(|| node_shape.to_vec()),
                )?),
            },
            NumericExprNode::SpectralIndex {
                lhs,
                rhs,
                spec_axis_a,
                freqs_a,
                spec_axis_b,
                freqs_b,
            } => {
                let lhs_shape = lhs.try_shape().unwrap_or_else(|| node_shape.to_vec());
                let rhs_shape = rhs.try_shape().unwrap_or_else(|| node_shape.to_vec());
                CompiledNumericExprNode::SpectralIndex {
                    lhs: Box::new(self.compile_numeric(lhs, &lhs_shape)?),
                    lhs_shape,
                    rhs: Box::new(self.compile_numeric(rhs, &rhs_shape)?),
                    rhs_shape,
                    spec_axis_a: *spec_axis_a,
                    freqs_a: freqs_a.clone(),
                    spec_axis_b: *spec_axis_b,
                    freqs_b: freqs_b.clone(),
                }
            }
            NumericExprNode::Pa { u, q } => {
                let u_shape = u.try_shape().unwrap_or_else(|| node_shape.to_vec());
                let q_shape = q.try_shape().unwrap_or_else(|| node_shape.to_vec());
                CompiledNumericExprNode::Pa {
                    u: Box::new(self.compile_numeric(u, &u_shape)?),
                    u_shape,
                    q: Box::new(self.compile_numeric(q, &q_shape)?),
                    q_shape,
                }
            }
            NumericExprNode::Rebin {
                child,
                in_shape,
                factors,
                ..
            } => CompiledNumericExprNode::Rebin {
                child: Box::new(self.compile_numeric(child, in_shape)?),
                in_shape: in_shape.clone(),
                factors: factors.clone(),
            },
        })
    }

    fn compile_mask(
        &mut self,
        node: &MaskExprNode<'_, T>,
        node_shape: &[usize],
    ) -> Result<CompiledMaskExprNode<T>, ImageError> {
        Ok(match node {
            MaskExprNode::CompareScalar { op, expr, scalar } => {
                CompiledMaskExprNode::CompareScalar {
                    op: *op,
                    expr: Box::new(self.compile_numeric(
                        expr,
                        &expr.try_shape().unwrap_or_else(|| node_shape.to_vec()),
                    )?),
                    scalar: *scalar,
                }
            }
            MaskExprNode::CompareExpr { op, lhs, rhs } => {
                let lhs_shape = lhs.try_shape().unwrap_or_else(|| node_shape.to_vec());
                let rhs_shape = rhs.try_shape().unwrap_or_else(|| node_shape.to_vec());
                CompiledMaskExprNode::CompareExpr {
                    op: *op,
                    lhs: Box::new(self.compile_numeric(lhs, &lhs_shape)?),
                    lhs_shape,
                    rhs: Box::new(self.compile_numeric(rhs, &rhs_shape)?),
                    rhs_shape,
                }
            }
            MaskExprNode::Logical { op, lhs, rhs } => {
                let lhs_shape = lhs.try_shape().unwrap_or_else(|| node_shape.to_vec());
                let rhs_shape = rhs.try_shape().unwrap_or_else(|| node_shape.to_vec());
                CompiledMaskExprNode::Logical {
                    op: *op,
                    lhs: Box::new(self.compile_mask(lhs, &lhs_shape)?),
                    lhs_shape,
                    rhs: Box::new(self.compile_mask(rhs, &rhs_shape)?),
                    rhs_shape,
                }
            }
            MaskExprNode::Not { child } => CompiledMaskExprNode::Not {
                child: Box::new(self.compile_mask(
                    child,
                    &child.try_shape().unwrap_or_else(|| node_shape.to_vec()),
                )?),
            },
            MaskExprNode::IsNan { child } => CompiledMaskExprNode::IsNan {
                child: Box::new(self.compile_numeric(
                    child,
                    &child.try_shape().unwrap_or_else(|| node_shape.to_vec()),
                )?),
            },
            MaskExprNode::Validity { child } => CompiledMaskExprNode::Validity {
                child: Box::new(self.compile_numeric(child, node_shape)?),
            },
            MaskExprNode::BooleanValidity { child } => CompiledMaskExprNode::BooleanValidity {
                child: Box::new(self.compile_mask(child, node_shape)?),
            },
            MaskExprNode::ConstantMask { mask } => CompiledMaskExprNode::ConstantMask {
                mask: Arc::new(mask.clone()),
            },
            MaskExprNode::AllReduce { child, child_shape } => CompiledMaskExprNode::AllReduce {
                child: Box::new(self.compile_mask(child, child_shape)?),
                child_shape: child_shape.clone(),
            },
            MaskExprNode::AnyReduce { child, child_shape } => CompiledMaskExprNode::AnyReduce {
                child: Box::new(self.compile_mask(child, child_shape)?),
                child_shape: child_shape.clone(),
            },
            MaskExprNode::IndexIn { axis, indices } => CompiledMaskExprNode::IndexIn {
                axis: *axis,
                indices: indices.clone(),
            },
        })
    }
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

impl<T: ImageExprValue> std::fmt::Debug for ImageExpression<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ImageExpression")
            .field("shape", &self.meta.shape)
            .field("pixel_type", &T::PRIMITIVE_TYPE)
            .finish()
    }
}

impl<T: ImageExprValue> std::fmt::Debug for MaskExpression<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MaskExpression")
            .field("shape", &self.shape)
            .finish()
    }
}

impl<T: ImageExprValue> ImageExpression<T> {
    pub(crate) fn may_have_mask(&self) -> bool {
        self.node.may_have_mask(&self.arena)
    }

    /// Converts pixels lazily while retaining the expression's geometry and validity.
    ///
    /// # Errors
    /// Returns an error if the conversion cannot be compiled.
    pub fn convert_pixels<U: ImageExprValue>(
        self,
        convert: impl Fn(T) -> U + Copy + Send + Sync + 'static,
    ) -> Result<ImageExpression<U>, ImageError> {
        self.mapped_sections(convert).compile()
    }

    pub(super) fn mapped_sections<'a, U: ImageExprValue>(
        self,
        convert: impl Fn(T) -> U + Copy + Send + Sync + 'static,
    ) -> ImageExprBuilder<'a, U> {
        let meta = self.meta.clone();
        let compiled = Arc::new(self);
        let mask = compiled.may_have_mask().then(|| {
            let source = Arc::clone(&compiled);
            Arc::new(move |start: &[usize], shape: &[usize], stride: &[usize]| {
                ImageInterface::get_mask_slice(source.as_ref(), start, shape, stride)
            }) as super::OwnedMaskReader
        });
        ImageExprBuilder::from_section_readers(
            meta,
            Arc::new(move |start, shape, stride| {
                compiled
                    .get_slice_with_stride(start, shape, stride)
                    .map(|pixels| pixels.mapv(convert))
            }),
            mask,
        )
    }

    pub(crate) fn visit_pixel_sections(
        &self,
        mut visit: impl FnMut(&[usize], ArrayD<T>) -> Result<(), ImageError>,
    ) -> Result<(), ImageError> {
        let shape = self.shape();
        let chunk = bounded_read_cursor(shape.to_vec())?;
        let stride = vec![1; shape.len()];
        let mut ctx = CompiledEvalContext::new(self.arena.sources.len());
        for cursor in
            TraversalCursorIter::new(shape.to_vec(), chunk.clone(), TraversalSpec::chunks(chunk))
        {
            let cursor = cursor?;
            let pixels = self.node.eval_slice(
                &self.arena,
                &mut ctx,
                &cursor.position,
                &cursor.shape,
                &stride,
            )?;
            visit(&cursor.position, pixels)?;
        }
        Ok(())
    }

    /// Overrides the execution policy used by [`get`](Self::get),
    /// [`get_slice`](Self::get_slice), and [`save_as`](Self::save_as).
    ///
    /// Unlike C++ casacore, Rust exposes explicit performance policy control
    /// here. The policy changes *how* the compiled expression runs, not what
    /// values it produces.
    pub fn set_execution_policy(&mut self, policy: ExecutionPolicy) {
        self.execution_policy = policy;
    }

    /// Assigns the resource slice used by automatic and explicit planning.
    pub fn set_execution_resources(&mut self, resources: ExecutionResources) {
        self.execution_resources = resources;
    }

    /// Reads the full compiled expression.
    pub fn get(&self) -> Result<ArrayD<T>, ImageError> {
        self.get_slice(&vec![0; self.ndim()], self.shape())
    }

    /// Reads a single compiled pixel.
    pub fn get_at(&self, position: &[usize]) -> Result<T, ImageError> {
        validate_slice_request(
            self.shape(),
            position,
            &vec![1; self.ndim()],
            &vec![1; self.ndim()],
        )?;
        let mut ctx = CompiledEvalContext::new(self.arena.sources.len());
        let one = self.node.eval_slice(
            &self.arena,
            &mut ctx,
            position,
            &vec![1; self.ndim()],
            &vec![1; self.ndim()],
        )?;
        Ok(one[IxDyn(&vec![0; self.ndim()])])
    }

    /// Reads a unit-stride slice of the compiled expression.
    pub fn get_slice(&self, start: &[usize], shape: &[usize]) -> Result<ArrayD<T>, ImageError> {
        validate_slice_request(self.shape(), start, shape, &vec![1; self.ndim()])?;
        if shape.is_empty() {
            let mut ctx = CompiledEvalContext::new(self.arena.sources.len());
            return self
                .node
                .eval_slice(&self.arena, &mut ctx, start, shape, &[])
                .map_err(Into::into);
        }

        let cursor_shape = clamp_cursor_shape(
            &self.node.preferred_cursor_shape(
                shape,
                &self.arena,
                self.execution_resources.worker_limit,
            )?,
            shape,
        );
        let strategy = compiled_map_strategy::<T>(CompiledMapInputs {
            policy: self.execution_policy,
            full_shape: shape,
            cursor_shape: &cursor_shape,
            has_paged_sources: self.arena.has_paged_sources(),
            source_count: self.arena.sources.len(),
            repeated_source_count: self.arena.repeated_source_count(),
            output_element_bytes: std::mem::size_of::<T>(),
            resources: self.execution_resources,
        })?;
        let mut out = ArrayD::from_elem(IxDyn(shape), T::default_value());
        let base_start = start.to_vec();

        try_map_traversal_cursors_ordered_with_strategy(
            shape,
            &cursor_shape,
            TraversalSpec::chunks(cursor_shape.clone()),
            strategy,
            || CompiledEvalContext::new(self.arena.sources.len()),
            |ctx, cursor| {
                let absolute_start = add_positions(&base_start, &cursor.position);
                let stride = vec![1; cursor.position.len()];
                let data = self.node.eval_slice(
                    &self.arena,
                    ctx,
                    &absolute_start,
                    &cursor.shape,
                    &stride,
                )?;
                Ok(EvaluatedChunk {
                    position: cursor.position,
                    data,
                })
            },
            |chunk| write_chunk_into_array(&mut out, &chunk.position, &chunk.data),
        )?;

        Ok(out)
    }

    /// Reads a strided slice serially.
    ///
    /// Strided requests are usually much smaller than full materialization
    /// workloads, so the compiled runtime keeps this path simple and direct.
    pub fn get_slice_with_stride(
        &self,
        start: &[usize],
        shape: &[usize],
        stride: &[usize],
    ) -> Result<ArrayD<T>, ImageError> {
        validate_slice_request(self.shape(), start, shape, stride)?;
        let mut ctx = CompiledEvalContext::new(self.arena.sources.len());
        self.node
            .eval_slice(&self.arena, &mut ctx, start, shape, stride)
            .map_err(Into::into)
    }

    /// Persists the compiled expression as a new paged image.
    ///
    /// The output pixels are written chunk by chunk using the current
    /// [`ExecutionPolicy`]. When a propagated default mask is available, it is
    /// written to the output image as the default mask after pixel data has
    /// been committed.
    pub fn save_as(&self, path: impl AsRef<Path>) -> Result<PagedImage<T>, ImageError> {
        let (image, _) = self.save_as_with_metadata_inner(
            path.as_ref(),
            &self.meta.coords,
            &self.meta.units,
            &self.meta.image_info,
            &self.meta.misc_info,
            "compiled_mask",
            None,
        )?;
        Ok(image)
    }

    pub(crate) fn save_as_with_full_metadata(
        &self,
        path: impl AsRef<Path>,
        coords: &CoordinateSystem,
        units: &str,
        image_info: &ImageInfo,
        misc_info: &RecordValue,
        legacy_coords: Option<RecordValue>,
    ) -> Result<(PagedImage<T>, usize), ImageError> {
        self.save_as_with_metadata_inner(
            path.as_ref(),
            coords,
            units,
            image_info,
            misc_info,
            "mask0",
            legacy_coords,
        )
    }

    #[allow(clippy::too_many_arguments)]
    fn save_as_with_metadata_inner(
        &self,
        path: &Path,
        coords: &CoordinateSystem,
        units: &str,
        image_info: &ImageInfo,
        misc_info: &RecordValue,
        mask_name: &str,
        legacy_coords: Option<RecordValue>,
    ) -> Result<(PagedImage<T>, usize), ImageError> {
        let full_shape = self.meta.shape.clone();
        let cursor_shape = bounded_read_cursor(clamp_cursor_shape(
            &self.node.preferred_cursor_shape(
                &full_shape,
                &self.arena,
                self.execution_resources.worker_limit,
            )?,
            &full_shape,
        ))?;
        let strategy = compiled_map_strategy::<T>(CompiledMapInputs {
            policy: self.execution_policy,
            full_shape: &full_shape,
            cursor_shape: &cursor_shape,
            has_paged_sources: self.arena.has_paged_sources(),
            source_count: self.arena.sources.len(),
            repeated_source_count: self.arena.repeated_source_count(),
            output_element_bytes: std::mem::size_of::<T>(),
            resources: self.execution_resources,
        })?;

        let mut image = PagedImage::create(full_shape.clone(), coords.clone(), path)?;
        let total_elems: usize = full_shape.iter().product();
        let may_have_mask = self.node.may_have_mask(&self.arena);
        let snapshot_mask = if may_have_mask {
            self.node.single_snapshot_mask(&self.arena, &full_shape)
        } else {
            None
        };
        let stream_masks = may_have_mask && snapshot_mask.is_none();
        let mut mask_state = StreamingMaskState::new(
            full_shape.clone(),
            image.tile_shape().to_vec(),
            path.join(mask_name),
            image.cache_bytes(),
        );

        try_map_traversal_cursors_ordered_with_strategy(
            &full_shape,
            &cursor_shape,
            TraversalSpec::chunks(cursor_shape.clone()),
            strategy,
            || CompiledEvalContext::new(self.arena.sources.len()),
            |ctx, cursor| {
                let stride = vec![1; cursor.position.len()];
                let data = self.node.eval_slice(
                    &self.arena,
                    ctx,
                    &cursor.position,
                    &cursor.shape,
                    &stride,
                )?;
                let mask = if stream_masks {
                    self.node.eval_mask_slice(
                        &self.arena,
                        ctx,
                        &cursor.position,
                        &cursor.shape,
                        &stride,
                    )?
                } else {
                    None
                };
                Ok(EvaluatedSaveChunk {
                    position: cursor.position,
                    data,
                    mask,
                })
            },
            |chunk| {
                image
                    .put_slice(&chunk.data, &chunk.position)
                    .map_err(|e| LatticeError::Table(e.to_string()))?;
                if stream_masks {
                    mask_state.record_chunk(&chunk.position, chunk.data.shape(), chunk.mask)?;
                }
                Ok(())
            },
        )?;

        image.set_units(units.to_string())?;
        image.set_misc_info(misc_info.clone())?;
        image.set_image_info(image_info)?;
        if let Some(lc) = legacy_coords {
            image.set_coords_record_keyword(lc);
        }
        let valid_pixels = if !may_have_mask {
            total_elems
        } else if let Some(arc) = snapshot_mask {
            let false_count = arc.iter().filter(|&&v| !v).count();
            if false_count > 0 {
                image.put_mask(mask_name, &arc)?;
                image.set_default_mask(mask_name)?;
            }
            total_elems - false_count
        } else {
            mask_state.finish(&mut image, mask_name)?
        };
        image.save()?;
        Ok((image, valid_pixels))
    }

    pub fn shape(&self) -> &[usize] {
        &self.meta.shape
    }

    pub fn ndim(&self) -> usize {
        self.meta.shape.len()
    }

    /// Returns the compiled coordinate system.
    pub fn coordinates(&self) -> &CoordinateSystem {
        &self.meta.coords
    }

    /// Returns the compiled brightness units string.
    pub fn units(&self) -> &str {
        &self.meta.units
    }

    /// Returns the compiled miscellaneous info record.
    pub fn misc_info_ref(&self) -> &RecordValue {
        &self.meta.misc_info
    }

    /// Replace the metadata exposed by an owned expression while retaining its graph.
    pub fn with_misc_info(mut self, misc_info: RecordValue) -> Self {
        self.meta.misc_info = misc_info;
        self
    }

    /// Returns the compiled image info metadata.
    pub fn image_info_ref(&self) -> &ImageInfo {
        &self.meta.image_info
    }

    pub(crate) fn legacy_coords_ref(&self) -> Option<&RecordValue> {
        self.meta.legacy_coords.as_ref()
    }

    pub(super) fn raw_image_info_ref(&self) -> Option<&RecordValue> {
        self.meta.raw_image_info.as_ref()
    }

    pub(crate) fn raw_image_info(&self) -> Option<RecordValue> {
        self.raw_image_info_ref().cloned()
    }

    /// Returns the original coordinate record when it is available.
    pub fn coordinate_record(&self) -> RecordValue {
        self.meta
            .legacy_coords
            .clone()
            .unwrap_or_else(|| self.meta.coords.to_record())
    }

    /// Returns `true` if any expression branch can produce a non-trivial pixel mask.
    pub fn has_mask(&self) -> bool {
        self.node.may_have_mask(&self.arena)
    }

    pub fn source_mask(&self) -> Option<ArrayD<bool>> {
        self.eval_full_mask()
            .ok()
            .flatten()
            .map(|arc| (*arc).clone())
    }

    /// Evaluates the pixel mask for an axis-aligned slice, returning `None` if all valid.
    pub fn get_mask_slice(
        &self,
        start: &[usize],
        shape: &[usize],
    ) -> Result<Option<ArrayD<bool>>, ImageError> {
        let stride = vec![1; self.ndim()];
        validate_slice_request(self.shape(), start, shape, &stride)?;
        if !self.node.may_have_mask(&self.arena) {
            return Ok(None);
        }
        let mut ctx = CompiledEvalContext::new(self.arena.sources.len());
        self.node
            .eval_mask_slice(&self.arena, &mut ctx, start, shape, &stride)
            .map_err(Into::into)
    }

    fn eval_full_mask(&self) -> Result<Option<Arc<ArrayD<bool>>>, ImageError> {
        if !self.node.may_have_mask(&self.arena) {
            return Ok(None);
        }
        if let Some(arc) = self
            .node
            .single_snapshot_mask(&self.arena, &self.meta.shape)
        {
            return Ok(arc.iter().any(|&v| !v).then_some(arc));
        }
        let shape = &self.meta.shape;
        let mut ctx = CompiledEvalContext::new(self.arena.sources.len());
        let mask = if shape.is_empty() {
            self.node
                .eval_mask_slice(&self.arena, &mut ctx, &[], &[], &[])?
        } else {
            self.node.eval_mask_slice(
                &self.arena,
                &mut ctx,
                &vec![0; shape.len()],
                shape,
                &vec![1; shape.len()],
            )?
        };
        Ok(mask.filter(|m| m.iter().any(|&v| !v)).map(Arc::new))
    }

    /// Returns the original LEL text when this graph was produced by the
    /// parser or reopened from a persisted expression.
    pub fn expr_string(&self) -> Option<&str> {
        self.expr_string.as_deref()
    }

    /// Persists this expression graph as an `.imgexpr` descriptor.
    pub fn save_expr(&self, path: impl AsRef<Path>) -> Result<(), ImageError> {
        let expr_string = self.expr_string().ok_or_else(|| {
            ImageError::InvalidMetadata(
                "ImageExpression cannot be persisted: no expression string is set".into(),
            )
        })?;
        crate::expr_file::save(path, expr_string, T::PRIMITIVE_TYPE, &self.meta.misc_info)
    }

    pub(crate) fn set_persisted_metadata(&mut self, path: PathBuf, misc_info: RecordValue) {
        self.meta.name = Some(path);
        self.meta.misc_info = misc_info;
    }
}

impl<T: ImageExprValue> MaskExpression<T> {
    pub(crate) fn is_shape_polymorphic(&self) -> bool {
        self.node.is_shape_polymorphic()
    }

    pub(crate) fn get_in_sections(&self) -> Result<ArrayD<bool>, ImageError> {
        let cursor = bounded_read_cursor(self.shape.clone())?;
        self.get_slice_with_max_cursor_shape(&vec![0; self.ndim()], &self.shape, &cursor)
    }

    /// Overrides the execution policy used by [`get`](Self::get) and
    /// [`get_slice`](Self::get_slice).
    pub fn set_execution_policy(&mut self, policy: ExecutionPolicy) {
        self.execution_policy = policy;
    }

    /// Assigns the resource slice used by automatic and explicit planning.
    pub fn set_execution_resources(&mut self, resources: ExecutionResources) {
        self.execution_resources = resources;
    }

    pub fn get(&self) -> Result<ArrayD<bool>, ImageError> {
        self.get_slice(&vec![0; self.ndim()], &self.shape)
    }

    pub fn get_at(&self, position: &[usize]) -> Result<bool, ImageError> {
        validate_slice_request(
            &self.shape,
            position,
            &vec![1; self.ndim()],
            &vec![1; self.ndim()],
        )?;
        let mut ctx = CompiledEvalContext::new(self.arena.sources.len());
        let one = self.node.eval_slice(
            &self.arena,
            &mut ctx,
            position,
            &vec![1; self.ndim()],
            &vec![1; self.ndim()],
        )?;
        Ok(one[IxDyn(&vec![0; self.ndim()])])
    }

    pub fn get_slice(&self, start: &[usize], shape: &[usize]) -> Result<ArrayD<bool>, ImageError> {
        self.get_slice_with_max_cursor_shape(start, shape, shape)
    }

    fn get_slice_with_max_cursor_shape(
        &self,
        start: &[usize],
        shape: &[usize],
        max_cursor_shape: &[usize],
    ) -> Result<ArrayD<bool>, ImageError> {
        validate_slice_request(&self.shape, start, shape, &vec![1; self.ndim()])?;
        if shape.is_empty() {
            let mut ctx = CompiledEvalContext::new(self.arena.sources.len());
            return self
                .node
                .eval_slice(&self.arena, &mut ctx, start, shape, &[])
                .map_err(Into::into);
        }

        let mut cursor_shape = clamp_cursor_shape(
            &self.node.preferred_cursor_shape(
                shape,
                &self.arena,
                self.execution_resources.worker_limit,
            )?,
            shape,
        );
        for (extent, &maximum) in cursor_shape.iter_mut().zip(max_cursor_shape) {
            *extent = (*extent).min(maximum);
        }
        let strategy = compiled_map_strategy::<T>(CompiledMapInputs {
            policy: self.execution_policy,
            full_shape: shape,
            cursor_shape: &cursor_shape,
            has_paged_sources: self.arena.has_paged_sources(),
            source_count: self.arena.sources.len(),
            repeated_source_count: self.arena.repeated_source_count(),
            output_element_bytes: std::mem::size_of::<bool>(),
            resources: self.execution_resources,
        })?;
        let mut out = ArrayD::from_elem(IxDyn(shape), false);
        let base_start = start.to_vec();

        try_map_traversal_cursors_ordered_with_strategy(
            shape,
            &cursor_shape,
            TraversalSpec::chunks(cursor_shape.clone()),
            strategy,
            || CompiledEvalContext::new(self.arena.sources.len()),
            |ctx, cursor| {
                let absolute_start = add_positions(&base_start, &cursor.position);
                let stride = vec![1; cursor.position.len()];
                let data = self.node.eval_slice(
                    &self.arena,
                    ctx,
                    &absolute_start,
                    &cursor.shape,
                    &stride,
                )?;
                Ok(EvaluatedChunk {
                    position: cursor.position,
                    data,
                })
            },
            |chunk| write_chunk_into_array(&mut out, &chunk.position, &chunk.data),
        )?;

        Ok(out)
    }

    pub fn shape(&self) -> &[usize] {
        &self.shape
    }

    pub fn ndim(&self) -> usize {
        self.shape.len()
    }

    /// Evaluates this boolean mask expression against an explicit `target_shape`,
    /// supporting shape-polymorphic masks such as `indexin(...)` as well as
    /// stretched lower-dimensional masks.
    pub fn eval_for_shape(&self, target_shape: &[usize]) -> Result<ArrayD<bool>, ImageError> {
        if self.shape == target_shape {
            return self.get();
        }
        if self.shape.is_empty() && !target_shape.is_empty() {
            let mut ctx = CompiledEvalContext::new(self.arena.sources.len());
            return self
                .node
                .eval_slice(
                    &self.arena,
                    &mut ctx,
                    &vec![0; target_shape.len()],
                    target_shape,
                    &vec![1; target_shape.len()],
                )
                .map_err(Into::into);
        }
        if can_broadcast_to(&self.shape, target_shape) {
            let mask = self.get()?;
            return broadcast_array(mask, target_shape);
        }
        Err(ImageError::ShapeMismatch {
            expected: target_shape.to_vec(),
            got: self.shape.clone(),
        })
    }

    pub(crate) fn eval_window_for_shape(
        &self,
        window_start: &[usize],
        window_shape: &[usize],
        full_shape: &[usize],
    ) -> Result<ArrayD<bool>, ImageError> {
        self.eval_section_for_shape(
            window_start,
            window_shape,
            &vec![1; window_shape.len()],
            full_shape,
        )
    }

    /// Evaluates a strided section relative to a target lattice shape.
    /// A compatible smaller mask is stretched by broadcasting its singleton
    /// axes. Only the requested section needs to be evaluated.
    pub fn eval_section_for_shape(
        &self,
        start: &[usize],
        shape: &[usize],
        stride: &[usize],
        full_shape: &[usize],
    ) -> Result<ArrayD<bool>, ImageError> {
        validate_slice_request(full_shape, start, shape, stride)?;
        if self.shape.is_empty() && !self.node.is_shape_polymorphic() {
            return Ok(ArrayD::from_elem(IxDyn(shape), self.get_at(&[])?));
        }
        let evaluate =
            |start: &[usize], shape: &[usize], strides: &[usize]| -> Result<_, ImageError> {
                if strides.iter().all(|step| *step == 1) && !self.shape.is_empty() {
                    self.get_slice(start, shape)
                } else {
                    let mut ctx = CompiledEvalContext::new(self.arena.sources.len());
                    self.node
                        .eval_slice(&self.arena, &mut ctx, start, shape, strides)
                        .map_err(Into::into)
                }
            };
        if self.shape == full_shape || self.shape.is_empty() {
            return evaluate(start, shape, stride);
        }
        if can_broadcast_to(&self.shape, full_shape) {
            let (source_start, source_shape, source_stride) =
                slice_for_broadcast_child(&self.shape, start, shape, stride);
            let child_mask = evaluate(&source_start, &source_shape, &source_stride)?;
            return broadcast_array(child_mask, shape);
        }
        Err(ImageError::ShapeMismatch {
            expected: full_shape.to_vec(),
            got: self.shape.clone(),
        })
    }
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

impl<T: ImageExprValue> CompiledNumericExprNode<T> {
    fn single_snapshot_mask(
        &self,
        arena: &CompiledImageArena<T>,
        full_shape: &[usize],
    ) -> Option<Arc<ArrayD<bool>>> {
        match self {
            Self::Source(source_id) => {
                let src = &arena.sources[*source_id];
                if src.shape == full_shape {
                    src.mask_snapshot.clone()
                } else {
                    None
                }
            }
            Self::UnaryOp { child, .. } | Self::CustomUnary { child, .. } => {
                child.single_snapshot_mask(arena, full_shape)
            }
            Self::BinaryOp {
                lhs,
                lhs_shape,
                rhs,
                rhs_shape,
                ..
            }
            | Self::CustomBinary {
                lhs,
                lhs_shape,
                rhs,
                rhs_shape,
                ..
            }
            | Self::SpectralIndex {
                lhs,
                lhs_shape,
                rhs,
                rhs_shape,
                ..
            }
            | Self::Pa {
                u: lhs,
                u_shape: lhs_shape,
                q: rhs,
                q_shape: rhs_shape,
            } => {
                if lhs_shape == full_shape && !rhs.may_have_mask(arena) {
                    lhs.single_snapshot_mask(arena, full_shape)
                } else if rhs_shape == full_shape && !lhs.may_have_mask(arena) {
                    rhs.single_snapshot_mask(arena, full_shape)
                } else {
                    None
                }
            }
            _ => None,
        }
    }

    fn may_have_mask(&self, arena: &CompiledImageArena<T>) -> bool {
        match self {
            Self::Source(source_id) => arena.sources[*source_id].has_mask,
            Self::Scalar(_)
            | Self::Reduction {
                op: ReductionOp::Sum,
                ..
            }
            | Self::Reduction {
                empty_value: Some(_),
                ..
            }
            | Self::MaskCount { .. }
            | Self::StripMask { .. } => false,
            Self::UnaryOp { child, .. }
            | Self::CustomUnary { child, .. }
            | Self::Reduction { child, .. }
            | Self::Fractile { child, .. }
            | Self::FractileRange { child, .. }
            | Self::Rebin { child, .. } => child.may_have_mask(arena),
            Self::BinaryOp { lhs, rhs, .. }
            | Self::CustomBinary { lhs, rhs, .. }
            | Self::SpectralIndex { lhs, rhs, .. }
            | Self::Pa { u: lhs, q: rhs, .. } => {
                lhs.may_have_mask(arena) || rhs.may_have_mask(arena)
            }
            Self::Conditional {
                condition,
                if_true,
                if_false,
                ..
            } => {
                condition.may_be_partially_invalid(arena)
                    || if_true.may_have_mask(arena)
                    || if_false.may_have_mask(arena)
            }
            Self::Replace { primary, .. } => primary.may_have_mask(arena),
        }
    }

    fn preferred_cursor_shape(
        &self,
        full_shape: &[usize],
        arena: &CompiledImageArena<T>,
        worker_limit: usize,
    ) -> Result<Vec<usize>, ImageError> {
        Ok(match self {
            Self::Source(source_id) => {
                let source = &arena.sources[*source_id];
                if source.shape != full_shape {
                    return work_balanced_cursor_shape(full_shape, worker_limit);
                }
                match &source.tile_shape {
                    Some(tile_shape) => expand_tile_aligned_cursor_shape(
                        full_shape,
                        tile_shape,
                        work_balanced_pixel_target(full_shape, worker_limit)?,
                    )?,
                    None => work_balanced_cursor_shape(full_shape, worker_limit)?,
                }
            }
            Self::UnaryOp { child, .. }
            | Self::CustomUnary { child, .. }
            | Self::StripMask { child }
            | Self::Reduction { child, .. }
            | Self::Fractile { child, .. }
            | Self::FractileRange { child, .. } => {
                return child.preferred_cursor_shape(full_shape, arena, worker_limit);
            }
            Self::BinaryOp {
                lhs,
                lhs_shape,
                rhs,
                rhs_shape,
                ..
            }
            | Self::CustomBinary {
                lhs,
                lhs_shape,
                rhs,
                rhs_shape,
                ..
            }
            | Self::SpectralIndex {
                lhs,
                lhs_shape,
                rhs,
                rhs_shape,
                ..
            }
            | Self::Pa {
                u: lhs,
                u_shape: lhs_shape,
                q: rhs,
                q_shape: rhs_shape,
            } => {
                if choose_rhs_meta(lhs_shape, rhs_shape, full_shape) {
                    return rhs.preferred_cursor_shape(full_shape, arena, worker_limit);
                }
                return lhs.preferred_cursor_shape(full_shape, arena, worker_limit);
            }
            Self::Conditional {
                if_true,
                true_shape,
                if_false,
                false_shape,
                ..
            } => {
                if choose_rhs_meta(true_shape, false_shape, full_shape) {
                    return if_false.preferred_cursor_shape(full_shape, arena, worker_limit);
                }
                return if_true.preferred_cursor_shape(full_shape, arena, worker_limit);
            }
            Self::Replace { primary, .. } => {
                return primary.preferred_cursor_shape(full_shape, arena, worker_limit);
            }
            Self::Scalar(_) | Self::MaskCount { .. } | Self::Rebin { .. } => {
                work_balanced_cursor_shape(full_shape, worker_limit)?
            }
        })
    }

    fn eval_mask_slice(
        &self,
        arena: &CompiledImageArena<T>,
        ctx: &mut CompiledEvalContext<T>,
        start: &[usize],
        shape: &[usize],
        stride: &[usize],
    ) -> Result<Option<ArrayD<bool>>, LatticeError> {
        if !self.may_have_mask(arena) {
            return Ok(None);
        }
        match self {
            Self::Source(source_id) => arena.sources[*source_id]
                .eval_mask_slice(arena, ctx, *source_id, start, shape, stride),
            Self::Scalar(_)
            | Self::Reduction {
                op: ReductionOp::Sum,
                ..
            }
            | Self::MaskCount { .. }
            | Self::StripMask { .. } => Ok(None),
            Self::Reduction {
                child, child_shape, ..
            }
            | Self::Fractile {
                child, child_shape, ..
            }
            | Self::FractileRange {
                child, child_shape, ..
            } => {
                let valid = child.any_valid_pixel(arena, ctx, child_shape)?;
                Ok((!valid).then(|| ArrayD::from_elem(IxDyn(shape), false)))
            }
            Self::UnaryOp { child, .. } | Self::CustomUnary { child, .. } => {
                child.eval_mask_slice(arena, ctx, start, shape, stride)
            }
            Self::BinaryOp {
                lhs,
                lhs_shape,
                rhs,
                rhs_shape,
                ..
            }
            | Self::CustomBinary {
                lhs,
                lhs_shape,
                rhs,
                rhs_shape,
                ..
            }
            | Self::SpectralIndex {
                lhs,
                lhs_shape,
                rhs,
                rhs_shape,
                ..
            }
            | Self::Pa {
                u: lhs,
                u_shape: lhs_shape,
                q: rhs,
                q_shape: rhs_shape,
            } => {
                let lhs_mask = eval_child_mask_slice_broadcast(
                    lhs, lhs_shape, arena, ctx, start, shape, stride,
                )?;
                let rhs_mask = eval_child_mask_slice_broadcast(
                    rhs, rhs_shape, arena, ctx, start, shape, stride,
                )?;
                Ok(combine_slice_masks(lhs_mask, rhs_mask))
            }
            Self::Replace {
                primary,
                primary_shape,
                ..
            } => eval_child_mask_slice_broadcast(
                primary,
                primary_shape,
                arena,
                ctx,
                start,
                shape,
                stride,
            ),
            Self::Conditional {
                condition,
                cond_shape,
                if_true,
                true_shape,
                if_false,
                false_shape,
            } => {
                let cond_vals = eval_bool_slice_broadcast(
                    condition, cond_shape, arena, ctx, start, shape, stride,
                )?;
                let cond_valid = eval_bool_validity_broadcast(
                    condition, cond_shape, arena, ctx, start, shape, stride,
                )?;
                let true_valid = eval_child_mask_slice_broadcast(
                    if_true, true_shape, arena, ctx, start, shape, stride,
                )?;
                let false_valid = eval_child_mask_slice_broadcast(
                    if_false,
                    false_shape,
                    arena,
                    ctx,
                    start,
                    shape,
                    stride,
                )?;
                if cond_valid.is_none() && true_valid.is_none() && false_valid.is_none() {
                    return Ok(None);
                }
                let mut out = ArrayD::from_elem(IxDyn(shape), true);
                for (pos, dst) in out.indexed_iter_mut() {
                    let cv = cond_valid.as_ref().is_none_or(|m| m[&pos]);
                    let tv = if cond_vals[&pos] {
                        true_valid.as_ref().is_none_or(|m| m[&pos])
                    } else {
                        false_valid.as_ref().is_none_or(|m| m[&pos])
                    };
                    *dst = cv && tv;
                }
                Ok(Some(out))
            }
            Self::Rebin {
                child,
                in_shape,
                factors,
            } => {
                let (c_start, c_shape, c_stride) =
                    rebin_child_slice(in_shape, factors, start, shape, stride);
                let Some(child_mask) =
                    child.eval_mask_slice(arena, ctx, &c_start, &c_shape, &c_stride)?
                else {
                    return Ok(None);
                };
                Ok(Some(rebin_mask_slice(&child_mask, factors, shape, stride)))
            }
        }
    }

    fn any_valid_pixel(
        &self,
        arena: &CompiledImageArena<T>,
        ctx: &mut CompiledEvalContext<T>,
        shape: &[usize],
    ) -> Result<bool, LatticeError> {
        if !self.may_have_mask(arena) {
            return Ok(true);
        }
        if shape.is_empty() {
            return Ok(self
                .eval_mask_slice(arena, ctx, &[], &[], &[])?
                .is_none_or(|mask| mask[IxDyn(&[])]));
        }
        let cursor = clamp_cursor_shape(
            &self
                .preferred_cursor_shape(shape, arena, thread_parallelism())
                .map_err(|error| LatticeError::InvalidTraversal(error.to_string()))?,
            shape,
        );
        for cursor in TraversalCursorIter::new(
            shape.to_vec(),
            cursor.clone(),
            TraversalSpec::chunks(cursor),
        ) {
            let cursor = cursor?;
            let stride = vec![1; shape.len()];
            if self
                .eval_mask_slice(arena, ctx, &cursor.position, &cursor.shape, &stride)?
                .is_none_or(|mask| mask.iter().any(|&valid| valid))
            {
                return Ok(true);
            }
        }
        Ok(false)
    }

    fn eval_slice(
        &self,
        arena: &CompiledImageArena<T>,
        ctx: &mut CompiledEvalContext<T>,
        start: &[usize],
        shape: &[usize],
        stride: &[usize],
    ) -> Result<ArrayD<T>, LatticeError> {
        match self {
            Self::Source(source_id) => {
                arena.sources[*source_id].eval_slice(arena, ctx, *source_id, start, shape, stride)
            }
            Self::Scalar(value) => Ok(ArrayD::from_elem(IxDyn(shape), *value)),
            Self::UnaryOp { op, child } => {
                let mut data = child.eval_slice(arena, ctx, start, shape, stride)?;
                data.mapv_inplace(|value| apply_unary(*op, value));
                Ok(data)
            }
            Self::StripMask { child } => child.eval_slice(arena, ctx, start, shape, stride),
            Self::BinaryOp {
                op,
                lhs,
                lhs_shape,
                rhs,
                rhs_shape,
            } => {
                let lhs_data =
                    eval_child_slice_broadcast(lhs, lhs_shape, arena, ctx, start, shape, stride)?;
                let rhs_data =
                    eval_child_slice_broadcast(rhs, rhs_shape, arena, ctx, start, shape, stride)?;
                Ok(Zip::from(&lhs_data)
                    .and(&rhs_data)
                    .map_collect(|&lhs, &rhs| apply_binary(*op, lhs, rhs)))
            }
            Self::CustomUnary { child, func } => {
                let mut data = child.eval_slice(arena, ctx, start, shape, stride)?;
                let BinaryOrUnary::Unary(func) = func else {
                    unreachable!("custom unary node stored binary function")
                };
                data.mapv_inplace(|value| func(value));
                Ok(data)
            }
            Self::CustomBinary {
                lhs,
                lhs_shape,
                rhs,
                rhs_shape,
                func,
            } => {
                let lhs_data =
                    eval_child_slice_broadcast(lhs, lhs_shape, arena, ctx, start, shape, stride)?;
                let rhs_data =
                    eval_child_slice_broadcast(rhs, rhs_shape, arena, ctx, start, shape, stride)?;
                let BinaryOrUnary::Binary(func) = func else {
                    unreachable!("custom binary node stored unary function")
                };
                Ok(Zip::from(&lhs_data)
                    .and(&rhs_data)
                    .map_collect(|&lhs, &rhs| func(lhs, rhs)))
            }
            Self::SpectralIndex {
                lhs,
                lhs_shape,
                rhs,
                rhs_shape,
                spec_axis_a,
                freqs_a,
                spec_axis_b,
                freqs_b,
            } => {
                let lhs_data =
                    eval_child_slice_broadcast(lhs, lhs_shape, arena, ctx, start, shape, stride)?;
                let rhs_data =
                    eval_child_slice_broadcast(rhs, rhs_shape, arena, ctx, start, shape, stride)?;
                Ok(eval_spectral_index(
                    lhs_data,
                    rhs_data,
                    *spec_axis_a,
                    freqs_a,
                    *spec_axis_b,
                    freqs_b,
                    start,
                    stride,
                ))
            }
            Self::Pa {
                u,
                u_shape,
                q,
                q_shape,
            } => {
                let u_data =
                    eval_child_slice_broadcast(u, u_shape, arena, ctx, start, shape, stride)?;
                let q_data =
                    eval_child_slice_broadcast(q, q_shape, arena, ctx, start, shape, stride)?;
                Ok(eval_pa(&u_data, &q_data))
            }
            Self::Rebin {
                child,
                in_shape,
                factors,
            } => {
                let (c_start, c_shape, c_stride) =
                    rebin_child_slice(in_shape, factors, start, shape, stride);
                let in_vals = child.eval_slice(arena, ctx, &c_start, &c_shape, &c_stride)?;
                let in_mask = child.eval_mask_slice(arena, ctx, &c_start, &c_shape, &c_stride)?;
                let (out_vals, _) =
                    rebin_reduce_slice(&in_vals, in_mask.as_ref(), factors, shape, stride);
                Ok(out_vals)
            }
            Self::Reduction {
                op,
                child,
                child_shape,
                empty_value,
            } => Ok(ArrayD::from_elem(
                IxDyn(shape),
                reduce_numeric_child(*op, child, child_shape, arena, *empty_value)?,
            )),
            Self::Fractile {
                child,
                child_shape,
                fraction,
            } => {
                let mut values = collect_numeric_child(child, child_shape, arena)?;
                let value =
                    fractile_in_place(&mut values, *fraction).unwrap_or_else(T::default_value);
                Ok(ArrayD::from_elem(IxDyn(shape), value))
            }
            Self::FractileRange {
                child,
                child_shape,
                fraction1,
                fraction2,
            } => {
                let mut values = collect_numeric_child(child, child_shape, arena)?;
                let mut clone_vals = values.clone();
                let v1 =
                    fractile_in_place(&mut clone_vals, *fraction1).unwrap_or_else(T::default_value);
                let v2 =
                    fractile_in_place(&mut values, *fraction2).unwrap_or_else(T::default_value);
                Ok(ArrayD::from_elem(IxDyn(shape), v2 - v1))
            }
            Self::Conditional {
                condition,
                cond_shape,
                if_true,
                true_shape,
                if_false,
                false_shape,
            } => eval_conditional_slice(
                condition,
                cond_shape,
                if_true,
                true_shape,
                if_false,
                false_shape,
                arena,
                ctx,
                start,
                shape,
                stride,
            ),
            Self::MaskCount {
                count_true,
                mask,
                mask_shape,
            } => {
                let mut count = 0usize;
                for_each_compiled_mask_chunk(mask, mask_shape, arena, |chunk| {
                    count += if *count_true {
                        chunk.iter().filter(|&&value| value).count()
                    } else {
                        chunk.iter().filter(|&&value| !value).count()
                    };
                    Ok(())
                })?;
                Ok(ArrayD::from_elem(
                    IxDyn(shape),
                    T::from_f64_lossy(count as f64),
                ))
            }
            Self::Replace {
                primary,
                primary_shape,
                replacement,
                replacement_shape,
                mask,
            } => eval_replace_slice(
                primary,
                primary_shape,
                replacement,
                replacement_shape,
                mask.as_deref(),
                arena,
                ctx,
                start,
                shape,
                stride,
            ),
        }
    }
}

#[allow(clippy::too_many_arguments)]
fn eval_conditional_slice<T: ImageExprValue>(
    condition: &CompiledMaskExprNode<T>,
    cond_shape: &[usize],
    if_true: &CompiledNumericExprNode<T>,
    true_shape: &[usize],
    if_false: &CompiledNumericExprNode<T>,
    false_shape: &[usize],
    arena: &CompiledImageArena<T>,
    ctx: &mut CompiledEvalContext<T>,
    start: &[usize],
    shape: &[usize],
    stride: &[usize],
) -> Result<ArrayD<T>, LatticeError> {
    let cond = eval_bool_slice_broadcast(condition, cond_shape, arena, ctx, start, shape, stride)?;
    let true_data =
        eval_child_slice_broadcast(if_true, true_shape, arena, ctx, start, shape, stride)?;
    let false_data =
        eval_child_slice_broadcast(if_false, false_shape, arena, ctx, start, shape, stride)?;
    Ok(Zip::from(&cond)
        .and(&true_data)
        .and(&false_data)
        .map_collect(|&condition, &t_val, &f_val| if condition { t_val } else { f_val }))
}

#[allow(clippy::too_many_arguments)]
fn eval_replace_slice<T: ImageExprValue>(
    primary: &CompiledNumericExprNode<T>,
    primary_shape: &[usize],
    replacement: &CompiledNumericExprNode<T>,
    replacement_shape: &[usize],
    mask: Option<&ArrayD<bool>>,
    arena: &CompiledImageArena<T>,
    ctx: &mut CompiledEvalContext<T>,
    start: &[usize],
    shape: &[usize],
    stride: &[usize],
) -> Result<ArrayD<T>, LatticeError> {
    let mask_data = match mask {
        Some(mask) => Some(slice_array_owned(mask, start, shape, stride)?),
        None => eval_child_mask_slice_broadcast(
            primary,
            primary_shape,
            arena,
            ctx,
            start,
            shape,
            stride,
        )?,
    };
    let Some(mask_data) = mask_data.filter(|mask| !mask_chunk_all(mask)) else {
        return eval_child_slice_broadcast(
            primary,
            primary_shape,
            arena,
            ctx,
            start,
            shape,
            stride,
        );
    };
    if mask_chunk_none(&mask_data) {
        return eval_child_slice_broadcast(
            replacement,
            replacement_shape,
            arena,
            ctx,
            start,
            shape,
            stride,
        );
    }
    let mut out =
        eval_child_slice_broadcast(primary, primary_shape, arena, ctx, start, shape, stride)?;
    let replacement_data = eval_child_slice_broadcast(
        replacement,
        replacement_shape,
        arena,
        ctx,
        start,
        shape,
        stride,
    )?;
    Zip::from(&mask_data)
        .and(out.view_mut())
        .and(&replacement_data)
        .for_each(|&mask, out, &replacement| {
            if !mask {
                *out = replacement;
            }
        });
    Ok(out)
}

impl<T: ImageExprValue> CompiledMaskExprNode<T> {
    fn is_shape_polymorphic(&self) -> bool {
        match self {
            Self::IndexIn { .. } => true,
            Self::Not { child } | Self::BooleanValidity { child } => child.is_shape_polymorphic(),
            Self::Logical { lhs, rhs, .. } => {
                lhs.is_shape_polymorphic() || rhs.is_shape_polymorphic()
            }
            _ => false,
        }
    }

    fn may_be_partially_invalid(&self, arena: &CompiledImageArena<T>) -> bool {
        match self {
            Self::CompareScalar { expr, .. } | Self::IsNan { child: expr } => {
                expr.may_have_mask(arena)
            }
            Self::CompareExpr { lhs, rhs, .. } => {
                lhs.may_have_mask(arena) || rhs.may_have_mask(arena)
            }
            Self::Logical { lhs, rhs, .. } => {
                lhs.may_be_partially_invalid(arena) || rhs.may_be_partially_invalid(arena)
            }
            Self::Not { child } => child.may_be_partially_invalid(arena),
            Self::ConstantMask { .. }
            | Self::Validity { .. }
            | Self::BooleanValidity { .. }
            | Self::AllReduce { .. }
            | Self::AnyReduce { .. }
            | Self::IndexIn { .. } => false,
        }
    }

    fn eval_validity_slice(
        &self,
        arena: &CompiledImageArena<T>,
        ctx: &mut CompiledEvalContext<T>,
        start: &[usize],
        shape: &[usize],
        stride: &[usize],
    ) -> Result<Option<ArrayD<bool>>, LatticeError> {
        if !self.may_be_partially_invalid(arena) {
            return Ok(None);
        }
        match self {
            Self::CompareScalar { expr, .. } | Self::IsNan { child: expr } => {
                expr.eval_mask_slice(arena, ctx, start, shape, stride)
            }
            Self::CompareExpr {
                lhs,
                lhs_shape,
                rhs,
                rhs_shape,
                ..
            } => {
                let lhs_mask = eval_child_mask_slice_broadcast(
                    lhs, lhs_shape, arena, ctx, start, shape, stride,
                )?;
                let rhs_mask = eval_child_mask_slice_broadcast(
                    rhs, rhs_shape, arena, ctx, start, shape, stride,
                )?;
                Ok(combine_slice_masks(lhs_mask, rhs_mask))
            }
            Self::Logical {
                op,
                lhs,
                lhs_shape,
                rhs,
                rhs_shape,
            } => {
                let lhs_valid =
                    eval_bool_validity_broadcast(lhs, lhs_shape, arena, ctx, start, shape, stride)?;
                let rhs_valid =
                    eval_bool_validity_broadcast(rhs, rhs_shape, arena, ctx, start, shape, stride)?;
                if lhs_valid.is_none() && rhs_valid.is_none() {
                    return Ok(None);
                }
                let lhs_values = rhs_valid
                    .as_ref()
                    .map(|_| {
                        eval_bool_slice_broadcast(lhs, lhs_shape, arena, ctx, start, shape, stride)
                    })
                    .transpose()?;
                let rhs_values = lhs_valid
                    .as_ref()
                    .map(|_| {
                        eval_bool_slice_broadcast(rhs, rhs_shape, arena, ctx, start, shape, stride)
                    })
                    .transpose()?;
                let decisive = matches!(op, MaskLogicalOp::Or);
                let valid = ArrayD::from_shape_fn(IxDyn(shape), |index| {
                    let lhs_known = lhs_valid.as_ref().is_none_or(|mask| mask[&index]);
                    let rhs_known = rhs_valid.as_ref().is_none_or(|mask| mask[&index]);
                    (lhs_known && rhs_known)
                        || (lhs_known
                            && lhs_values
                                .as_ref()
                                .is_some_and(|values| values[&index] == decisive))
                        || (rhs_known
                            && rhs_values
                                .as_ref()
                                .is_some_and(|values| values[&index] == decisive))
                });
                Ok(Some(valid))
            }
            Self::Not { child } => child.eval_validity_slice(arena, ctx, start, shape, stride),
            Self::ConstantMask { .. }
            | Self::Validity { .. }
            | Self::BooleanValidity { .. }
            | Self::AllReduce { .. }
            | Self::AnyReduce { .. }
            | Self::IndexIn { .. } => Ok(None),
        }
    }

    fn preferred_cursor_shape(
        &self,
        full_shape: &[usize],
        arena: &CompiledImageArena<T>,
        worker_limit: usize,
    ) -> Result<Vec<usize>, ImageError> {
        Ok(match self {
            Self::CompareScalar { expr, .. }
            | Self::IsNan { child: expr }
            | Self::Validity { child: expr } => {
                return expr.preferred_cursor_shape(full_shape, arena, worker_limit);
            }
            Self::CompareExpr {
                lhs,
                lhs_shape,
                rhs,
                rhs_shape,
                ..
            } => {
                if choose_rhs_meta(lhs_shape, rhs_shape, full_shape) {
                    return rhs.preferred_cursor_shape(full_shape, arena, worker_limit);
                }
                return lhs.preferred_cursor_shape(full_shape, arena, worker_limit);
            }
            Self::Logical {
                lhs,
                lhs_shape,
                rhs,
                rhs_shape,
                ..
            } => {
                if choose_rhs_meta(lhs_shape, rhs_shape, full_shape) {
                    return rhs.preferred_cursor_shape(full_shape, arena, worker_limit);
                }
                return lhs.preferred_cursor_shape(full_shape, arena, worker_limit);
            }
            Self::Not { child }
            | Self::BooleanValidity { child }
            | Self::AllReduce { child, .. }
            | Self::AnyReduce { child, .. } => {
                return child.preferred_cursor_shape(full_shape, arena, worker_limit);
            }
            Self::ConstantMask { .. } | Self::IndexIn { .. } => {
                work_balanced_cursor_shape(full_shape, worker_limit)?
            }
        })
    }

    fn eval_slice(
        &self,
        arena: &CompiledImageArena<T>,
        ctx: &mut CompiledEvalContext<T>,
        start: &[usize],
        shape: &[usize],
        stride: &[usize],
    ) -> Result<ArrayD<bool>, LatticeError> {
        match self {
            Self::CompareScalar { op, expr, scalar } => {
                let data = expr.eval_slice(arena, ctx, start, shape, stride)?;
                Ok(data.mapv(|value| value.expr_compare(*scalar, *op)))
            }
            Self::CompareExpr {
                op,
                lhs,
                lhs_shape,
                rhs,
                rhs_shape,
            } => {
                let lhs_data =
                    eval_child_slice_broadcast(lhs, lhs_shape, arena, ctx, start, shape, stride)?;
                let rhs_data =
                    eval_child_slice_broadcast(rhs, rhs_shape, arena, ctx, start, shape, stride)?;
                Ok(Zip::from(&lhs_data)
                    .and(&rhs_data)
                    .map_collect(|&lhs, &rhs| lhs.expr_compare(rhs, *op)))
            }
            Self::Logical {
                op,
                lhs,
                lhs_shape,
                rhs,
                rhs_shape,
            } => {
                let lhs_mask =
                    eval_bool_slice_broadcast(lhs, lhs_shape, arena, ctx, start, shape, stride)?;
                match op {
                    MaskLogicalOp::And => {
                        if mask_chunk_none(&lhs_mask) {
                            return Ok(ArrayD::from_elem(IxDyn(shape), false));
                        }
                        if mask_chunk_all(&lhs_mask) {
                            return eval_bool_slice_broadcast(
                                rhs, rhs_shape, arena, ctx, start, shape, stride,
                            );
                        }
                    }
                    MaskLogicalOp::Or => {
                        if mask_chunk_all(&lhs_mask) {
                            return Ok(ArrayD::from_elem(IxDyn(shape), true));
                        }
                        if mask_chunk_none(&lhs_mask) {
                            return eval_bool_slice_broadcast(
                                rhs, rhs_shape, arena, ctx, start, shape, stride,
                            );
                        }
                    }
                }
                let rhs_mask =
                    eval_bool_slice_broadcast(rhs, rhs_shape, arena, ctx, start, shape, stride)?;
                let mut out = lhs_mask;
                Zip::from(out.view_mut())
                    .and(&rhs_mask)
                    .for_each(|out, &rhs| match op {
                        MaskLogicalOp::And => *out = *out && rhs,
                        MaskLogicalOp::Or => *out = *out || rhs,
                    });
                Ok(out)
            }
            Self::Not { child } => {
                let mut out = child.eval_slice(arena, ctx, start, shape, stride)?;
                out.mapv_inplace(|value| !value);
                Ok(out)
            }
            Self::IsNan { child } => {
                let data = child.eval_slice(arena, ctx, start, shape, stride)?;
                Ok(data.mapv(|value| value.expr_isnan()))
            }
            Self::Validity { child } => Ok(child
                .eval_mask_slice(arena, ctx, start, shape, stride)?
                .unwrap_or_else(|| ArrayD::from_elem(IxDyn(shape), true))),
            Self::BooleanValidity { child } => Ok(child
                .eval_validity_slice(arena, ctx, start, shape, stride)?
                .unwrap_or_else(|| ArrayD::from_elem(IxDyn(shape), true))),
            Self::ConstantMask { mask } => slice_array_owned(mask.as_ref(), start, shape, stride),
            Self::AllReduce { child, child_shape } => {
                let mut result = true;
                for_each_compiled_mask_chunk(child, child_shape, arena, |chunk| {
                    if !chunk.iter().all(|&value| value) {
                        result = false;
                    }
                    Ok(())
                })?;
                Ok(ArrayD::from_elem(IxDyn(shape), result))
            }
            Self::AnyReduce { child, child_shape } => {
                let mut result = false;
                for_each_compiled_mask_chunk(child, child_shape, arena, |chunk| {
                    if chunk.iter().any(|&value| value) {
                        result = true;
                    }
                    Ok(())
                })?;
                Ok(ArrayD::from_elem(IxDyn(shape), result))
            }
            Self::IndexIn { axis, indices } => eval_indexin(*axis, indices, start, shape, stride),
        }
    }
}

impl<'a, T: ImageExprValue> ImageExprBuilder<'a, T> {
    /// Compiles a borrowed lazy expression into an owned execution form.
    ///
    /// Compilation is Rust-specific functionality beyond C++ casacore's
    /// borrowed `ImageExprBuilder<T>` surface. Persistent paged sources remain lazy
    /// and reopenable; non-persistent or non-downcastable sources are
    /// snapshotted so the compiled expression can be evaluated on worker
    /// threads safely.
    ///
    /// # Example
    ///
    /// ```rust
    /// use casa_coordinates::CoordinateSystem;
    /// use casa_images::{ImageExprBuilder, TempImage};
    /// use casa_lattices::{ExecutionPolicy, LatticeMut};
    ///
    /// let mut image = TempImage::<f32>::new(vec![16, 16], CoordinateSystem::new(), casa_lattices::TempStoragePolicy::Memory).unwrap();
    /// image.set(2.0).unwrap();
    ///
    /// let mut compiled = ImageExprBuilder::from_image(&image)
    ///     .unwrap()
    ///     .multiply_scalar(2.0)
    ///     .compile()
    ///     .unwrap();
    /// compiled.set_execution_policy(ExecutionPolicy::Parallel {
    ///     workers: 2,
    ///     prefetch_depth: 4,
    /// });
    /// assert_eq!(compiled.get_at(&[0, 0]).unwrap(), 4.0);
    /// ```
    pub fn compile(&self) -> Result<ImageExpression<T>, ImageError> {
        let mut ctx = CompileCtx::default();
        let node = ctx.compile_numeric(&self.node, &self.meta.shape)?;
        Ok(ImageExpression {
            node,
            arena: Arc::new(CompiledImageArena {
                sources: ctx.sources,
            }),
            meta: self.meta.clone(),
            execution_policy: ExecutionPolicy::Auto,
            execution_resources: ExecutionResources::default(),
            expr_string: self.expr_string.as_deref().map(Arc::from),
        })
    }
}

impl<'a, T: ImageExprValue> MaskExprBuilder<'a, T> {
    /// Compiles a borrowed lazy boolean mask into an owned execution form.
    pub fn compile(&self) -> Result<MaskExpression<T>, ImageError> {
        let mut ctx = CompileCtx::default();
        let node = ctx.compile_mask(&self.node, &self.shape)?;
        Ok(MaskExpression {
            node,
            arena: Arc::new(CompiledImageArena {
                sources: ctx.sources,
            }),
            shape: self.shape.clone(),
            execution_policy: ExecutionPolicy::Auto,
            execution_resources: ExecutionResources::default(),
        })
    }
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

struct CompiledMapInputs<'a> {
    policy: ExecutionPolicy,
    full_shape: &'a [usize],
    cursor_shape: &'a [usize],
    has_paged_sources: bool,
    source_count: usize,
    repeated_source_count: usize,
    output_element_bytes: usize,
    resources: ExecutionResources,
}

fn compiled_map_strategy<T: ImageExprValue>(
    inputs: CompiledMapInputs<'_>,
) -> Result<OrderedCursorMapWriteExecutionStrategy, ImageError> {
    let task_count = TraversalCursorIter::new(
        inputs.full_shape.to_vec(),
        inputs.cursor_shape.to_vec(),
        TraversalSpec::chunks(inputs.cursor_shape.to_vec()),
    )
    .size_hint()
    .1
    .unwrap_or(0);
    let chunk_elements = inputs
        .cursor_shape
        .iter()
        .try_fold(1usize, |product, &extent| product.checked_mul(extent));
    let chunk_bytes = chunk_elements
        .and_then(|elements| elements.checked_mul(inputs.output_element_bytes.max(1)))
        .ok_or_else(|| ImageError::Lattice("compiled expression chunk byte overflow".into()))?;
    let context_slots = inputs
        .source_count
        .checked_mul(
            std::mem::size_of::<Option<PagedImage<T>>>()
                + std::mem::size_of::<Option<TiledArrayStorage>>()
                + std::mem::size_of::<Option<CachedSourceSlice<T>>>(),
        )
        .ok_or_else(|| ImageError::Lattice("compiled expression context overflow".into()))?;
    let cached_source_bytes = inputs
        .repeated_source_count
        .checked_mul(
            chunk_elements
                .and_then(|elements| elements.checked_mul(std::mem::size_of::<T>().max(1)))
                .ok_or_else(|| {
                    ImageError::Lattice("compiled expression source cache byte overflow".into())
                })?,
        )
        .ok_or_else(|| ImageError::Lattice("compiled expression source cache overflow".into()))?;
    let per_worker_state_bytes = context_slots
        .checked_add(cached_source_bytes)
        .ok_or_else(|| ImageError::Lattice("compiled expression worker state overflow".into()))?;
    let plan = plan_execution(
        inputs.policy,
        ExecutionInputs {
            task_count,
            chunk_bytes,
            per_worker_state_bytes,
            memory_budget_bytes: inputs.resources.memory_budget_bytes,
            available_workers: inputs.resources.worker_limit,
            requested_worker_limit: inputs.resources.worker_limit,
            source_residency: if inputs.has_paged_sources {
                SourceResidency::Persistent
            } else {
                SourceResidency::Resident
            },
            prefetch_capability: true,
            configured_prefetch_cap: inputs.resources.prefetch_cap,
        },
    )
    .map_err(|error| ImageError::Lattice(error.to_string()))?;
    Ok(match plan.mode {
        ExecutionMode::Serial => OrderedCursorMapWriteExecutionStrategy::Serial,
        ExecutionMode::Pipelined => {
            OrderedCursorMapWriteExecutionStrategy::Pipelined(CursorMapWriteConfig {
                prefetch_depth: plan.prefetch_depth,
            })
        }
        ExecutionMode::Parallel => {
            OrderedCursorMapWriteExecutionStrategy::Parallel(ParallelReadChunkConfig {
                workers: plan.workers,
                prefetch_depth: plan.prefetch_depth,
            })
        }
    })
}

pub(crate) fn bounded_read_cursor(shape: Vec<usize>) -> Result<Vec<usize>, LatticeError> {
    let element_bytes = std::mem::size_of::<casa_core::Complex64>();
    Ok(TiledShape::new(shape, element_bytes)?.tile_shape().to_vec())
}

fn for_each_compiled_numeric_and_mask_chunk<T: ImageExprValue>(
    node: &CompiledNumericExprNode<T>,
    full_shape: &[usize],
    arena: &CompiledImageArena<T>,
    mut f: impl FnMut(&ArrayD<T>, Option<&ArrayD<bool>>) -> Result<(), LatticeError>,
) -> Result<(), LatticeError> {
    if full_shape.is_empty() {
        let mut ctx = CompiledEvalContext::new(arena.sources.len());
        let vals = node.eval_slice(arena, &mut ctx, &[], &[], &[])?;
        let mask = node.eval_mask_slice(arena, &mut ctx, &[], &[], &[])?;
        return f(&vals, mask.as_ref());
    }
    let cursor_shape = bounded_read_cursor(clamp_cursor_shape(
        &node
            .preferred_cursor_shape(full_shape, arena, thread_parallelism())
            .map_err(|error| LatticeError::InvalidTraversal(error.to_string()))?,
        full_shape,
    ))?;
    let mut ctx = CompiledEvalContext::new(arena.sources.len());
    let check_mask = node.may_have_mask(arena);
    for cursor in TraversalCursorIter::new(
        full_shape.to_vec(),
        cursor_shape.clone(),
        TraversalSpec::chunks(cursor_shape),
    ) {
        let cursor = cursor?;
        let stride = vec![1; cursor.position.len()];
        let chunk = node.eval_slice(arena, &mut ctx, &cursor.position, &cursor.shape, &stride)?;
        let mask = if check_mask {
            node.eval_mask_slice(arena, &mut ctx, &cursor.position, &cursor.shape, &stride)?
        } else {
            None
        };
        f(&chunk, mask.as_ref())?;
    }
    Ok(())
}

fn reduce_numeric_child<T: ImageExprValue>(
    op: ReductionOp,
    child: &CompiledNumericExprNode<T>,
    child_shape: &[usize],
    arena: &CompiledImageArena<T>,
    empty_value: Option<T>,
) -> Result<T, LatticeError> {
    if child_shape.is_empty() {
        let mut ctx = CompiledEvalContext::new(arena.sources.len());
        let scalar = child.eval_slice(arena, &mut ctx, &[], &[], &[])?;
        return Ok(match op {
            ReductionOp::Variance | ReductionOp::Stddev | ReductionOp::Avdev => T::default_value(),
            _ => scalar[IxDyn(&[])],
        });
    }

    match op {
        ReductionOp::Sum => {
            let mut sum = T::default_value();
            for_each_compiled_numeric_and_mask_chunk(child, child_shape, arena, |chunk, mask| {
                match mask {
                    Some(m) => {
                        for (&value, &valid) in chunk.iter().zip(m.iter()) {
                            if valid {
                                sum = sum + value;
                            }
                        }
                    }
                    None => {
                        for &value in chunk {
                            sum = sum + value;
                        }
                    }
                }
                Ok(())
            })?;
            Ok(sum)
        }
        ReductionOp::Mean => {
            let mut sum = T::default_value();
            let mut count = 0usize;
            for_each_compiled_numeric_and_mask_chunk(child, child_shape, arena, |chunk, mask| {
                match mask {
                    Some(m) => {
                        for (&value, &valid) in chunk.iter().zip(m.iter()) {
                            if valid {
                                sum = sum + value;
                                count += 1;
                            }
                        }
                    }
                    None => {
                        count += chunk.len();
                        for &value in chunk {
                            sum = sum + value;
                        }
                    }
                }
                Ok(())
            })?;
            Ok(if count == 0 {
                empty_value.unwrap_or_else(T::default_value)
            } else {
                sum * T::from_f64_lossy(1.0 / count as f64)
            })
        }
        ReductionOp::Min | ReductionOp::Max => {
            let mut result = None;
            let combine = if matches!(op, ReductionOp::Min) {
                T::expr_min
            } else {
                T::expr_max
            };
            for_each_compiled_numeric_and_mask_chunk(child, child_shape, arena, |chunk, mask| {
                let mut include = |value| {
                    result = Some(result.map_or(value, |previous| combine(previous, value)));
                };
                if let Some(mask) = mask {
                    for (&value, &valid) in chunk.iter().zip(mask) {
                        if valid {
                            include(value);
                        }
                    }
                } else {
                    for &value in chunk {
                        include(value);
                    }
                }
                Ok(())
            })?;
            Ok(result.unwrap_or_else(|| empty_value.unwrap_or_else(T::default_value)))
        }
        ReductionOp::Variance | ReductionOp::Stddev => {
            let mut acc = WelfordAccum::default();
            for_each_compiled_numeric_and_mask_chunk(child, child_shape, arena, |chunk, mask| {
                match mask {
                    Some(m) => {
                        for (&value, &valid) in chunk.iter().zip(m.iter()) {
                            if valid {
                                acc.update(value.to_f64_lossy());
                            }
                        }
                    }
                    None => {
                        for &value in chunk {
                            acc.update(value.to_f64_lossy());
                        }
                    }
                }
                Ok(())
            })?;
            let val = if matches!(op, ReductionOp::Variance) {
                acc.variance()
            } else {
                acc.stddev()
            };
            Ok(T::from_f64_lossy(val))
        }
        ReductionOp::Avdev => {
            let mut acc = WelfordAccum::default();
            for_each_compiled_numeric_and_mask_chunk(child, child_shape, arena, |chunk, mask| {
                match mask {
                    Some(m) => {
                        for (&value, &valid) in chunk.iter().zip(m.iter()) {
                            if valid {
                                acc.update(value.to_f64_lossy());
                            }
                        }
                    }
                    None => {
                        for &value in chunk {
                            acc.update(value.to_f64_lossy());
                        }
                    }
                }
                Ok(())
            })?;
            if acc.count == 0 {
                return Ok(T::default_value());
            }
            let mean = acc.mean;
            let mut abs_sum = 0.0f64;
            for_each_compiled_numeric_and_mask_chunk(child, child_shape, arena, |chunk, mask| {
                match mask {
                    Some(m) => {
                        for (&value, &valid) in chunk.iter().zip(m.iter()) {
                            if valid {
                                abs_sum += (value.to_f64_lossy() - mean).abs();
                            }
                        }
                    }
                    None => {
                        for &value in chunk {
                            abs_sum += (value.to_f64_lossy() - mean).abs();
                        }
                    }
                }
                Ok(())
            })?;
            Ok(T::from_f64_lossy(abs_sum / acc.count as f64))
        }
        ReductionOp::Median => {
            let mut values = collect_numeric_child(child, child_shape, arena)?;
            Ok(fractile_in_place(&mut values, 0.5)
                .unwrap_or_else(|| empty_value.unwrap_or_else(T::default_value)))
        }
    }
}

fn collect_numeric_child<T: ImageExprValue>(
    child: &CompiledNumericExprNode<T>,
    child_shape: &[usize],
    arena: &CompiledImageArena<T>,
) -> Result<Vec<T>, LatticeError> {
    let capacity = child_shape
        .iter()
        .try_fold(1usize, |product, &extent| product.checked_mul(extent))
        .ok_or_else(|| LatticeError::InvalidTraversal("reduction capacity overflow".to_string()))?;
    let mut values = Vec::with_capacity(capacity);
    for_each_compiled_numeric_and_mask_chunk(child, child_shape, arena, |chunk, mask| {
        match mask {
            Some(m) => {
                for (&value, &valid) in chunk.iter().zip(m.iter()) {
                    if valid {
                        values.push(value);
                    }
                }
            }
            None => values.extend(chunk.iter().copied()),
        }
        Ok(())
    })?;
    Ok(values)
}

fn for_each_compiled_mask_chunk<T: ImageExprValue>(
    node: &CompiledMaskExprNode<T>,
    full_shape: &[usize],
    arena: &CompiledImageArena<T>,
    mut f: impl FnMut(&ArrayD<bool>) -> Result<(), LatticeError>,
) -> Result<(), LatticeError> {
    if full_shape.is_empty() {
        let mut ctx = CompiledEvalContext::new(arena.sources.len());
        let empty = node.eval_slice(arena, &mut ctx, &[], &[], &[])?;
        return f(&empty);
    }
    let cursor_shape = bounded_read_cursor(clamp_cursor_shape(
        &node
            .preferred_cursor_shape(full_shape, arena, thread_parallelism())
            .map_err(|error| LatticeError::InvalidTraversal(error.to_string()))?,
        full_shape,
    ))?;
    let mut ctx = CompiledEvalContext::new(arena.sources.len());
    for cursor in TraversalCursorIter::new(
        full_shape.to_vec(),
        cursor_shape.clone(),
        TraversalSpec::chunks(cursor_shape),
    ) {
        let cursor = cursor?;
        let stride = vec![1; cursor.position.len()];
        let chunk = node.eval_slice(arena, &mut ctx, &cursor.position, &cursor.shape, &stride)?;
        f(&chunk)?;
    }
    Ok(())
}

fn add_positions(base: &[usize], offset: &[usize]) -> Vec<usize> {
    base.iter()
        .zip(offset.iter())
        .map(|(&base, &offset)| base + offset)
        .collect()
}

struct StreamingMaskState {
    full_shape: Vec<usize>,
    tile_shape: Vec<usize>,
    mask_path: PathBuf,
    cache_bytes: usize,
    writer: Option<PagedArray<bool>>,
    all_valid_chunks: Vec<(Vec<usize>, Vec<usize>)>,
    valid_pixels: usize,
}

impl StreamingMaskState {
    fn new(
        full_shape: Vec<usize>,
        tile_shape: Vec<usize>,
        mask_path: PathBuf,
        cache_bytes: usize,
    ) -> Self {
        Self {
            full_shape,
            tile_shape,
            mask_path,
            cache_bytes,
            writer: None,
            all_valid_chunks: Vec::new(),
            valid_pixels: 0,
        }
    }

    fn open_writer(&mut self) -> Result<(), LatticeError> {
        let tiled_shape =
            TiledShape::with_tile_shape(self.full_shape.clone(), self.tile_shape.clone())?;
        let mut writer = PagedArray::<bool>::create_with_cache(
            tiled_shape,
            &self.mask_path,
            self.cache_bytes.max(1),
        )?;
        for (pos, shp) in std::mem::take(&mut self.all_valid_chunks) {
            let trues = ArrayD::from_elem(IxDyn(&shp), true);
            writer.put_slice(&trues, &pos)?;
        }
        self.writer = Some(writer);
        Ok(())
    }

    fn record_chunk(
        &mut self,
        position: &[usize],
        shape: &[usize],
        mask: Option<ArrayD<bool>>,
    ) -> Result<(), LatticeError> {
        if let Some(mask_data) = mask {
            let chunk_elems = mask_data.len();
            let chunk_valid = mask_data.iter().filter(|&&v| v).count();
            self.valid_pixels += chunk_valid;
            if self.writer.is_some() || chunk_valid < chunk_elems {
                if self.writer.is_none() {
                    self.open_writer()?;
                }
                self.writer
                    .as_mut()
                    .expect("opened streaming mask writer")
                    .put_slice(&mask_data, position)?;
            } else {
                self.all_valid_chunks
                    .push((position.to_vec(), shape.to_vec()));
            }
        } else {
            self.valid_pixels += shape.iter().product::<usize>();
            if let Some(writer) = self.writer.as_mut() {
                let trues = ArrayD::from_elem(IxDyn(shape), true);
                writer.put_slice(&trues, position)?;
            } else {
                self.all_valid_chunks
                    .push((position.to_vec(), shape.to_vec()));
            }
        }
        Ok(())
    }

    fn finish<T: ImagePixel>(
        self,
        image: &mut PagedImage<T>,
        mask_name: &str,
    ) -> Result<usize, ImageError> {
        if let Some(writer) = self.writer {
            writer.flush().map_err(ImageError::from)?;
            drop(writer);
            image.register_paged_mask(mask_name);
            image.set_default_mask(mask_name)?;
        }
        Ok(self.valid_pixels)
    }
}

pub(crate) struct MemorySnapshotImage<T: ImagePixel> {
    pub(crate) data: Arc<ArrayD<T>>,
    pub(crate) mask: Option<Arc<ArrayD<bool>>>,
    pub(crate) coordinates: CoordinateSystem,
    pub(crate) units: String,
    pub(crate) image_info: ImageInfo,
    pub(crate) misc_info: RecordValue,
    pub(crate) legacy_coords: Option<RecordValue>,
}

impl<T: ImagePixel> Lattice<T> for MemorySnapshotImage<T> {
    fn shape(&self) -> &[usize] {
        self.data.shape()
    }

    fn is_writable(&self) -> bool {
        false
    }

    fn get_at(&self, position: &[usize]) -> Result<T, LatticeError> {
        let shape = self.shape();
        if position.len() != shape.len() {
            return Err(LatticeError::InvalidTraversal(format!(
                "position rank {} does not match image rank {}",
                position.len(),
                shape.len()
            )));
        }
        for (&idx, &extent) in position.iter().zip(shape.iter()) {
            if idx >= extent {
                return Err(LatticeError::IndexOutOfBounds {
                    index: position.to_vec(),
                    shape: shape.to_vec(),
                });
            }
        }
        Ok(self.data[IxDyn(position)])
    }

    fn get_slice(
        &self,
        start: &[usize],
        shape: &[usize],
        stride: &[usize],
    ) -> Result<ArrayD<T>, LatticeError> {
        slice_array_owned(self.data.as_ref(), start, shape, stride)
    }

    fn get(&self) -> Result<ArrayD<T>, LatticeError> {
        Ok((*self.data).clone())
    }
}

impl<T: ImagePixel> ImageInterface<T> for MemorySnapshotImage<T> {
    fn as_any(&self) -> Option<&dyn Any> {
        Some(self)
    }

    fn coordinates(&self) -> &CoordinateSystem {
        &self.coordinates
    }

    fn units(&self) -> &str {
        &self.units
    }

    fn misc_info(&self) -> RecordValue {
        self.misc_info.clone()
    }

    fn image_info(&self) -> Result<ImageInfo, ImageError> {
        Ok(self.image_info.clone())
    }

    fn name(&self) -> Option<&Path> {
        None
    }

    fn default_mask(&self) -> Result<Option<ArrayD<bool>>, ImageError> {
        Ok(self.mask.as_deref().cloned())
    }

    fn default_mask_name(&self) -> Option<String> {
        self.mask.as_ref().map(|_| "mask0".to_string())
    }

    fn get_mask_slice(
        &self,
        start: &[usize],
        shape: &[usize],
        stride: &[usize],
    ) -> Result<Option<ArrayD<bool>>, ImageError> {
        let Some(mask) = &self.mask else {
            return Ok(None);
        };
        slice_array_owned(mask.as_ref(), start, shape, stride)
            .map(Some)
            .map_err(Into::into)
    }
}

fn slice_array_owned<T: Clone>(
    array: &ArrayD<T>,
    start: &[usize],
    shape: &[usize],
    stride: &[usize],
) -> Result<ArrayD<T>, LatticeError> {
    validate_slice_request(array.shape(), start, shape, stride)?;
    let ndim = array.ndim();
    let slice_info: Vec<ndarray::SliceInfoElem> = (0..ndim)
        .map(|axis| {
            let end = start[axis] + shape[axis] * stride[axis];
            ndarray::SliceInfoElem::Slice {
                start: start[axis] as isize,
                end: Some(end as isize),
                step: stride[axis] as isize,
            }
        })
        .collect();
    Ok(array.slice(slice_info.as_slice()).to_owned())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn converted_mask_reads_only_bounded_source_sections() {
        use std::sync::atomic::{AtomicUsize, Ordering};

        let pixels_read = Arc::new(AtomicUsize::new(0));
        let source_reads = Arc::clone(&pixels_read);
        let mut metadata = ImageExprBuilder::scalar(0.0f32).meta;
        metadata.shape = vec![1025, 1025];
        let pixels = metadata.shape.iter().product::<usize>();
        let limit: usize = bounded_read_cursor(metadata.shape.clone())
            .unwrap()
            .iter()
            .product();
        let source = ImageExprBuilder::from_section_readers(
            metadata,
            Arc::new(move |_, shape, _| {
                let count = shape.iter().product::<usize>();
                assert!(count <= limit);
                source_reads.fetch_add(count, Ordering::Relaxed);
                Ok(ArrayD::from_elem(IxDyn(shape), 1.0f32))
            }),
            None,
        )
        .map_pixel_type(f64::from)
        .unwrap()
        .compile()
        .unwrap();
        let primary = ImageExprBuilder::from_image(&source).unwrap();
        let mut mask = primary
            .clone()
            .compare_expr(primary.mean_reduce(), ImageExprCompareOp::GreaterThan)
            .unwrap()
            .compile()
            .unwrap();
        mask.set_execution_policy(ExecutionPolicy::Serial);
        assert_eq!(pixels_read.load(Ordering::Relaxed), 0);
        assert!(mask.get_in_sections().unwrap().iter().all(|&value| !value));
        assert_eq!(pixels_read.load(Ordering::Relaxed), 2 * pixels);
    }

    #[test]
    fn replace_reads_validity_on_demand_and_reports_read_error() {
        let mut metadata = ImageExprBuilder::scalar(0.0f32).meta;
        metadata.shape = vec![4, 3];
        let expression = ImageExprBuilder::from_section_readers(
            metadata,
            Arc::new(|_, shape, _| Ok(ArrayD::from_elem(IxDyn(shape), 1.0f32))),
            Some(Arc::new(|_, _, _| {
                Err(ImageError::Io("mask read failed".into()))
            })),
        )
        .replace_invalid(ImageExprBuilder::scalar(0.0))
        .unwrap()
        .compile()
        .unwrap();
        let error = expression.get_slice(&[0, 0], &[1, 2]).unwrap_err();
        assert!(error.to_string().contains("mask read failed"));
    }
    use crate::ImageInterface;
    use casa_lattices::{ExecutionPolicy, Lattice};
    use ndarray::IxDyn;

    fn make_coords() -> CoordinateSystem {
        CoordinateSystem::new()
    }

    fn make_temp_image(shape: Vec<usize>, values: Vec<f32>) -> TempImage<f32> {
        let mut image = TempImage::<f32>::new(
            shape.clone(),
            make_coords(),
            casa_lattices::TempStoragePolicy::Memory,
        )
        .unwrap();
        image
            .put_slice(
                &ArrayD::from_shape_vec(IxDyn(&shape), values).unwrap(),
                &vec![0; shape.len()],
            )
            .unwrap();
        image
    }

    fn make_paged_image(
        dir: &tempfile::TempDir,
        name: &str,
        shape: Vec<usize>,
        tile_shape: Vec<usize>,
        values: Vec<f32>,
    ) -> PagedImage<f32> {
        let path = dir.path().join(name);
        let mut image = PagedImage::<f32>::create_with_tile_shape(
            shape.clone(),
            tile_shape,
            make_coords(),
            &path,
        )
        .unwrap();
        image
            .put_slice(
                &ArrayD::from_shape_vec(IxDyn(&shape), values).unwrap(),
                &vec![0; shape.len()],
            )
            .unwrap();
        image.save().unwrap();
        PagedImage::<f32>::open(&path).unwrap()
    }

    #[test]
    fn compiled_helper_functions_choose_expected_values() {
        let available = thread_parallelism();

        assert_eq!(
            compiled_source_cache_bytes::<f32>(&[16, 16, 16], 1234).unwrap(),
            1234
        );
        assert_eq!(
            compiled_source_cache_bytes::<f32>(&[16, 16, 16], 0).unwrap(),
            16 * 16 * 16 * std::mem::size_of::<f32>()
        );

        let shape = [1024, 1024, 256];
        let target = work_balanced_pixel_target(&shape, 256).unwrap();
        assert_eq!(
            expand_tile_aligned_cursor_shape(&shape, &[16, 16, 16], target).unwrap(),
            vec![1024, 64, 16]
        );

        let resources = ExecutionResources {
            memory_budget_bytes: 64 * 1024 * 1024,
            worker_limit: 4,
            prefetch_cap: 8,
        };
        let serial = compiled_map_strategy::<f32>(CompiledMapInputs {
            policy: ExecutionPolicy::Auto,
            full_shape: &[64, 64],
            cursor_shape: &[64, 64],
            has_paged_sources: false,
            source_count: 1,
            repeated_source_count: 0,
            output_element_bytes: std::mem::size_of::<f32>(),
            resources,
        })
        .unwrap();
        assert!(matches!(
            serial,
            OrderedCursorMapWriteExecutionStrategy::Serial
        ));

        let pipe = compiled_map_strategy::<f32>(CompiledMapInputs {
            policy: ExecutionPolicy::Auto,
            full_shape: &[512, 512],
            cursor_shape: &[256, 256],
            has_paged_sources: true,
            source_count: 1,
            repeated_source_count: 0,
            output_element_bytes: std::mem::size_of::<f32>(),
            resources,
        })
        .unwrap();
        if available < 2 {
            assert!(matches!(
                pipe,
                OrderedCursorMapWriteExecutionStrategy::Serial
            ));
        } else {
            assert!(matches!(
                pipe,
                OrderedCursorMapWriteExecutionStrategy::Parallel(_)
            ));
        }

        let par = compiled_map_strategy::<f32>(CompiledMapInputs {
            policy: ExecutionPolicy::Auto,
            full_shape: &[2048, 2048],
            cursor_shape: &[256, 256],
            has_paged_sources: true,
            source_count: 1,
            repeated_source_count: 0,
            output_element_bytes: std::mem::size_of::<f32>(),
            resources,
        })
        .unwrap();
        if available < 2 {
            assert!(matches!(
                par,
                OrderedCursorMapWriteExecutionStrategy::Serial
            ));
        } else {
            assert!(matches!(
                par,
                OrderedCursorMapWriteExecutionStrategy::Parallel(_)
            ));
        }
    }

    #[test]
    fn compile_uses_snapshot_for_temp_and_dedups_paged_sources() {
        let temp = make_temp_image(vec![2, 2], vec![1.0, 2.0, 3.0, 4.0]);
        let compiled = ImageExprBuilder::from_image(&temp)
            .unwrap()
            .add_scalar(1.0)
            .compile()
            .unwrap();
        assert_eq!(compiled.arena.sources.len(), 1);
        assert!(matches!(
            compiled.arena.sources[0].kind,
            CompiledImageSourceKind::Snapshot(_)
        ));

        let dir = tempfile::tempdir().unwrap();
        let paged = make_paged_image(
            &dir,
            "dedup.image",
            vec![8, 8, 4],
            vec![2, 2, 2],
            (0..8 * 8 * 4).map(|v| v as f32).collect(),
        );
        let compiled = ImageExprBuilder::from_image(&paged)
            .unwrap()
            .add_image(&paged)
            .unwrap()
            .compile()
            .unwrap();
        assert_eq!(compiled.arena.sources.len(), 1);
        assert!(matches!(
            compiled.arena.sources[0].kind,
            CompiledImageSourceKind::Paged { .. }
        ));
        assert_eq!(
            compiled.arena.sources[0].tile_shape.as_deref(),
            Some(&[2, 2, 2][..])
        );
    }

    #[cfg(unix)]
    #[test]
    fn compiled_locked_source_reacquires_read_lock_for_pixel_sections() {
        let directory = tempfile::tempdir().unwrap();
        let initial = make_paged_image(
            &directory,
            "locked.image",
            vec![4, 4],
            vec![2, 2],
            (0..16).map(|value| value as f32).collect(),
        );
        let path = initial.name().unwrap().to_path_buf();
        drop(initial);
        let locked = PagedImage::<f32>::open_read_locked_with_cache(&path, 0).unwrap();
        let unlocked = PagedImage::<f32>::open(&path).unwrap();
        let expressions = [(&unlocked, &locked), (&locked, &unlocked)].map(|(first, second)| {
            ImageExprBuilder::from_image(first)
                .unwrap()
                .add_image(second)
                .unwrap()
                .compile()
                .unwrap()
        });
        drop((locked, unlocked));

        for expression in expressions {
            let arena = &expression.arena;
            assert_eq!(arena.sources.len(), 1);
            let mut context = CompiledEvalContext::new(arena.sources.len());
            let pixels = arena.sources[0]
                .read_slice_uncached(arena, &mut context, 0, &[0, 0], &[2, 2], &[1, 1])
                .unwrap();
            assert_eq!(pixels.shape(), [2, 2]);
            assert!(
                context.opened_sources[0]
                    .as_ref()
                    .unwrap()
                    .has_lock(casa_tables::LockType::Read)
            );
        }
    }

    #[test]
    fn compiled_numeric_expr_supports_all_read_paths() {
        let image = make_temp_image(vec![3, 3], (0..9).map(|v| v as f32).collect());
        let mut compiled = ImageExprBuilder::from_image(&image)
            .unwrap()
            .multiply_scalar(2.0)
            .add_scalar(1.0)
            .compile()
            .unwrap();

        for policy in [ExecutionPolicy::Serial, ExecutionPolicy::Auto] {
            compiled.set_execution_policy(policy);
            assert_eq!(compiled.get_at(&[1, 2]).unwrap(), 11.0);
            assert_eq!(
                compiled.get_slice(&[1, 1], &[2, 2]).unwrap(),
                ArrayD::from_shape_vec(IxDyn(&[2, 2]), vec![9.0, 11.0, 15.0, 17.0]).unwrap()
            );
            assert_eq!(
                compiled
                    .get_slice_with_stride(&[0, 0], &[1, 2], &[2, 1])
                    .unwrap(),
                ArrayD::from_shape_vec(IxDyn(&[1, 2]), vec![1.0, 3.0]).unwrap()
            );
            assert_eq!(
                <ImageExpression<f32> as Lattice<f32>>::get_slice(
                    &compiled,
                    &[0, 0],
                    &[2, 1],
                    &[1, 2]
                )
                .unwrap(),
                ArrayD::from_shape_vec(IxDyn(&[2, 1]), vec![1.0, 7.0]).unwrap()
            );
        }
    }

    #[test]
    fn compiled_mask_expr_covers_logical_and_reduction_paths() {
        let image = make_temp_image(vec![2, 3], vec![0.0, 1.0, f32::NAN, 3.0, 4.0, 5.0]);
        let mask = ImageExprBuilder::from_image(&image)
            .unwrap()
            .gt_scalar(1.0)
            .or(ImageExprBuilder::from_image(&image).unwrap().isnan())
            .unwrap()
            .logical_not()
            .compile()
            .unwrap();

        assert_eq!(
            mask.get().unwrap(),
            ArrayD::from_shape_vec(IxDyn(&[2, 3]), vec![true, true, false, false, false, false])
                .unwrap()
        );
        assert!(mask.get_at(&[0, 0]).unwrap());
        assert_eq!(
            <MaskExpression<f32> as Lattice<bool>>::get_slice(&mask, &[0, 0], &[2, 1], &[1, 2])
                .unwrap(),
            ArrayD::from_shape_vec(IxDyn(&[2, 1]), vec![true, false]).unwrap()
        );

        let all = ImageExprBuilder::from_image(&image)
            .unwrap()
            .gt_scalar(-1.0)
            .all_reduce()
            .compile()
            .unwrap();
        assert!(!all.get_at(&[]).unwrap());

        let any = ImageExprBuilder::from_image(&image)
            .unwrap()
            .gt_scalar(4.5)
            .any_reduce()
            .compile()
            .unwrap();
        assert!(any.get_at(&[]).unwrap());
    }

    #[test]
    fn compiled_conditional_replace_and_mask_counts_work() {
        let image = make_temp_image(vec![2, 2], vec![1.0, 2.0, 3.0, 4.0]);
        let mask = ImageExprBuilder::from_image(&image).unwrap().gt_scalar(2.0);

        let conditional = ImageExprBuilder::iif(
            mask.clone(),
            ImageExprBuilder::from_image(&image)
                .unwrap()
                .add_scalar(100.0),
            ImageExprBuilder::from_image(&image).unwrap(),
        )
        .unwrap()
        .compile()
        .unwrap();
        assert_eq!(
            conditional.get().unwrap(),
            ArrayD::from_shape_vec(IxDyn(&[2, 2]), vec![1.0, 2.0, 103.0, 104.0]).unwrap()
        );

        let replacement_mask =
            ArrayD::from_shape_vec(IxDyn(&[2, 2]), vec![true, false, true, false]).unwrap();
        let replace = ImageExprBuilder::from_image(&image)
            .unwrap()
            .replace(
                ImageExprBuilder::from_image(&image)
                    .unwrap()
                    .add_scalar(10.0),
                replacement_mask,
            )
            .unwrap()
            .compile()
            .unwrap();
        assert_eq!(
            replace.get().unwrap(),
            ArrayD::from_shape_vec(IxDyn(&[2, 2]), vec![1.0, 12.0, 3.0, 14.0]).unwrap()
        );

        let ntrue = ImageExprBuilder::ntrue(mask.clone()).compile().unwrap();
        let nfalse = ImageExprBuilder::nfalse(mask).compile().unwrap();
        assert_eq!(ntrue.get_at(&[]).unwrap(), 2.0);
        assert_eq!(nfalse.get_at(&[]).unwrap(), 2.0);
    }

    #[test]
    fn compiled_custom_ops_and_reductions_work() {
        let image = make_temp_image(vec![2, 2], vec![10.0, 20.0, 30.0, 40.0]);
        let mapped = ImageExprBuilder::map(&image, |value| value + 1.0)
            .unwrap()
            .compile()
            .unwrap();
        assert_eq!(
            mapped.get().unwrap(),
            ArrayD::from_shape_vec(IxDyn(&[2, 2]), vec![11.0, 21.0, 31.0, 41.0]).unwrap()
        );

        let zipped = ImageExprBuilder::zip(&image, &image, |lhs, rhs| lhs + rhs * 0.5)
            .unwrap()
            .compile()
            .unwrap();
        assert_eq!(zipped.get_at(&[1, 0]).unwrap(), 45.0);

        let sum = ImageExprBuilder::from_image(&image)
            .unwrap()
            .sum_reduce()
            .compile()
            .unwrap();
        let mean = ImageExprBuilder::from_image(&image)
            .unwrap()
            .mean_reduce()
            .compile()
            .unwrap();
        let min = ImageExprBuilder::from_image(&image)
            .unwrap()
            .min_reduce()
            .compile()
            .unwrap();
        let max = ImageExprBuilder::from_image(&image)
            .unwrap()
            .max_reduce()
            .compile()
            .unwrap();
        let median = ImageExprBuilder::from_image(&image)
            .unwrap()
            .median_reduce()
            .compile()
            .unwrap();
        let fractile = ImageExprBuilder::from_image(&image)
            .unwrap()
            .fractile(0.5)
            .compile()
            .unwrap();
        let fractile_range = ImageExprBuilder::from_image(&image)
            .unwrap()
            .fractile_range(0.25, 0.75)
            .compile()
            .unwrap();
        assert_eq!(sum.get_at(&[]).unwrap(), 100.0);
        assert_eq!(mean.get_at(&[]).unwrap(), 25.0);
        assert_eq!(min.get_at(&[]).unwrap(), 10.0);
        assert_eq!(max.get_at(&[]).unwrap(), 40.0);
        // CASA 6.7.6.14 (casacore LatticeFractile) averages the two middle
        // elements for even-N median and fractile(..., 0.5): (20 + 30) / 2 = 25.0.
        // The pre-branch test expected 20.0 from floor-index selection.
        assert_eq!(median.get_at(&[]).unwrap(), 25.0);
        assert_eq!(fractile.get_at(&[]).unwrap(), 25.0);
        assert_eq!(fractile_range.get_at(&[]).unwrap(), 20.0);
    }

    #[test]
    fn compiled_save_as_preserves_metadata_and_mask() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("compiled.image");
        let mut image = TempImage::<f32>::new(
            vec![2, 2],
            make_coords(),
            casa_lattices::TempStoragePolicy::Memory,
        )
        .unwrap();
        image
            .put_slice(
                &ArrayD::from_shape_vec(IxDyn(&[2, 2]), vec![1.0, 2.0, 3.0, 4.0]).unwrap(),
                &[0, 0],
            )
            .unwrap();
        image.make_mask("quality", true, true).unwrap();
        image
            .put_mask(
                "quality",
                &ArrayD::from_shape_vec(IxDyn(&[2, 2]), vec![true, false, true, false]).unwrap(),
            )
            .unwrap();

        let mut compiled = ImageExprBuilder::from_image(&image)
            .unwrap()
            .add_scalar(5.0)
            .compile()
            .unwrap();
        compiled.set_execution_policy(ExecutionPolicy::Serial);
        let saved = compiled.save_as(&path).unwrap();

        assert_eq!(saved.default_mask_name().as_deref(), Some("compiled_mask"));
        assert_eq!(
            saved.get().unwrap(),
            ArrayD::from_shape_vec(IxDyn(&[2, 2]), vec![6.0, 7.0, 8.0, 9.0]).unwrap()
        );
        assert_eq!(
            saved.default_mask().unwrap().unwrap(),
            ArrayD::from_shape_vec(IxDyn(&[2, 2]), vec![true, false, true, false]).unwrap()
        );
        assert_eq!(saved.name(), Some(path.as_path()));
    }
}

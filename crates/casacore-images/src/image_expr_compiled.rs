// SPDX-License-Identifier: LGPL-3.0-or-later
//! Compiled, owned image-expression execution.

use std::any::Any;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use casacore_coordinates::CoordinateSystem;
use casacore_lattices::execution::{
    CursorMapWriteConfig, OrderedCursorMapWriteExecutionStrategy, ParallelReadChunkConfig,
    try_map_traversal_cursors_ordered_with_strategy,
};
use casacore_lattices::{
    ExecutionPolicy, Lattice, LatticeError, TraversalCursorIter, TraversalSpec,
};
use casacore_tables::{TilePixel, TiledFileIO};
use casacore_types::{ArrayD, RecordValue};
use ndarray::{IxDyn, Zip};

use crate::error::ImageError;
use crate::image::{ImageInterface, PagedImage};
use crate::image_info::ImageInfo;
use crate::temp_image::TempImage;

use super::{
    BinaryExprFn, ImageExpr, ImageExprBinaryOp, ImageExprCompareOp, ImageExprMeta,
    ImageExprUnaryOp, ImageExprValue, MaskExpr, MaskExprNode, MaskLogicalOp, NumericExprNode,
    ReductionOp, advised_cursor_shape, clamp_cursor_shape, thread_parallelism,
    validate_slice_request, write_chunk_into_array,
};

#[derive(Clone)]
enum CompiledImageSourceKind<T: ImageExprValue> {
    Snapshot(Arc<ArrayD<T>>),
    Paged {
        path: Arc<PathBuf>,
        cache_bytes: usize,
    },
}

#[derive(Clone)]
struct CompiledImageSource<T: ImageExprValue> {
    kind: CompiledImageSourceKind<T>,
    cursor_shape: Vec<usize>,
    tile_shape: Option<Vec<usize>>,
}

#[derive(Clone, Default)]
struct CompiledImageArena<T: ImageExprValue> {
    sources: Vec<CompiledImageSource<T>>,
}

impl<T: ImageExprValue> CompiledImageArena<T> {
    fn has_paged_sources(&self) -> bool {
        self.sources
            .iter()
            .any(|source| matches!(source.kind, CompiledImageSourceKind::Paged { .. }))
    }
}

struct CompiledEvalContext<T: ImageExprValue> {
    opened_sources: Vec<Option<PagedImage<T>>>,
    opened_tiled_sources: Vec<Option<TiledFileIO>>,
    cached_source_slices: Vec<Option<CachedSourceSlice<T>>>,
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
        }
    }

    fn source_tiled_io_mut<'a>(
        &'a mut self,
        arena: &CompiledImageArena<T>,
        source_id: usize,
    ) -> Result<&'a mut TiledFileIO, LatticeError>
    where
        T: TilePixel,
    {
        if self.opened_tiled_sources[source_id].is_none() {
            let source = &arena.sources[source_id];
            let CompiledImageSourceKind::Paged { path, cache_bytes } = &source.kind else {
                return Err(LatticeError::InvalidTraversal(
                    "compiled paged source expected persistent image".to_string(),
                ));
            };
            let tiled_io = TiledFileIO::open_with_cache_limit(path.as_path(), 1, *cache_bytes)
                .or_else(|_| TiledFileIO::open_with_cache_limit(path.as_path(), 0, *cache_bytes))
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
            let CompiledImageSourceKind::Paged { path, cache_bytes } = &source.kind else {
                return Err(LatticeError::InvalidTraversal(
                    "compiled paged source expected persistent image".to_string(),
                ));
            };
            let image = PagedImage::<T>::open_with_cache(path.as_path(), *cache_bytes)
                .map_err(|e| LatticeError::Table(e.to_string()))?;
            self.opened_sources[source_id] = Some(image);
        }
        Ok(self.opened_sources[source_id]
            .as_mut()
            .expect("compiled paged source opened"))
    }
}

impl<T: ImageExprValue> CompiledImageSource<T> {
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
        if let Some(cached) = &ctx.cached_source_slices[source_id]
            && cached.start == start
            && cached.shape == shape
            && cached.stride == stride
        {
            return Ok(cached.data.clone());
        }

        let data = match &self.kind {
            CompiledImageSourceKind::Snapshot(data) => {
                slice_array_owned(data.as_ref(), start, shape, stride)
            }
            CompiledImageSourceKind::Paged { .. } => {
                if stride.iter().all(|&step| step == 1) {
                    ctx.source_tiled_io_mut(arena, source_id)?
                        .get_slice::<T>(start, shape)
                        .map_err(|e| LatticeError::Table(e.to_string()))
                } else {
                    ctx.source_image_mut(arena, source_id)?
                        .get_slice_with_stride(start, shape, stride)
                        .map_err(|e| LatticeError::Table(e.to_string()))
                }
            }
        }?;

        ctx.cached_source_slices[source_id] = Some(CachedSourceSlice {
            start: start.to_vec(),
            shape: shape.to_vec(),
            stride: stride.to_vec(),
            data: data.clone(),
        });
        Ok(data)
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
        rhs: Box<Self>,
    },
    CustomUnary {
        child: Box<Self>,
        func: BinaryOrUnary<T>,
    },
    CustomBinary {
        lhs: Box<Self>,
        rhs: Box<Self>,
        func: BinaryOrUnary<T>,
    },
    Reduction {
        op: ReductionOp,
        child: Box<Self>,
        child_shape: Vec<usize>,
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
        if_true: Box<Self>,
        if_false: Box<Self>,
    },
    MaskCount {
        count_true: bool,
        mask: Box<CompiledMaskExprNode<T>>,
        mask_shape: Vec<usize>,
    },
    Replace {
        primary: Box<Self>,
        replacement: Box<Self>,
        mask: Arc<ArrayD<bool>>,
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
    Logical {
        op: MaskLogicalOp,
        lhs: Box<Self>,
        rhs: Box<Self>,
    },
    Not {
        child: Box<Self>,
    },
    IsNan {
        child: Box<CompiledNumericExprNode<T>>,
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
}

#[derive(Clone)]
struct EvaluatedChunk<T> {
    position: Vec<usize>,
    data: ArrayD<T>,
}

struct CompileCtx<T: ImageExprValue> {
    sources: Vec<CompiledImageSource<T>>,
    paged_source_ids: std::collections::HashMap<PathBuf, usize>,
}

impl<T: ImageExprValue> Default for CompileCtx<T> {
    fn default() -> Self {
        Self {
            sources: Vec::new(),
            paged_source_ids: std::collections::HashMap::new(),
        }
    }
}

impl<T: ImageExprValue> CompileCtx<T> {
    fn register_snapshot_source(&mut self, data: ArrayD<T>, cursor_shape: Vec<usize>) -> usize {
        let source_id = self.sources.len();
        self.sources.push(CompiledImageSource {
            cursor_shape,
            tile_shape: None,
            kind: CompiledImageSourceKind::Snapshot(Arc::new(data)),
        });
        source_id
    }

    fn register_image_source(
        &mut self,
        image: &dyn ImageInterface<T>,
    ) -> Result<usize, ImageError> {
        if let Some(any) = image.as_any() {
            if let Some(paged) = any.downcast_ref::<PagedImage<T>>()
                && let Some(path) = paged.name()
            {
                if let Some(&source_id) = self.paged_source_ids.get(path) {
                    return Ok(source_id);
                }
                let cursor_shape = clamp_cursor_shape(&paged.nice_cursor_shape(), paged.shape());
                let source_id = self.sources.len();
                self.sources.push(CompiledImageSource {
                    kind: CompiledImageSourceKind::Paged {
                        path: Arc::new(path.to_path_buf()),
                        cache_bytes: compiled_source_cache_bytes::<T>(
                            &cursor_shape,
                            paged.cache_bytes(),
                        ),
                    },
                    cursor_shape,
                    tile_shape: Some(paged.tile_shape().to_vec()),
                });
                self.paged_source_ids.insert(path.to_path_buf(), source_id);
                return Ok(source_id);
            }

            if let Some(temp) = any.downcast_ref::<TempImage<T>>() {
                let data = temp.get()?;
                let cursor_shape = clamp_cursor_shape(&temp.nice_cursor_shape(), temp.shape());
                return Ok(self.register_snapshot_source(data, cursor_shape));
            }
        }

        let data = image.get()?;
        let cursor_shape = clamp_cursor_shape(&image.nice_cursor_shape(), image.shape());
        Ok(self.register_snapshot_source(data, cursor_shape))
    }

    fn compile_numeric(
        &mut self,
        node: &NumericExprNode<'_, T>,
        node_shape: &[usize],
    ) -> Result<CompiledNumericExprNode<T>, ImageError> {
        Ok(match node {
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
            NumericExprNode::BinaryOp { op, lhs, rhs } => CompiledNumericExprNode::BinaryOp {
                op: *op,
                lhs: Box::new(self.compile_numeric(
                    lhs,
                    &lhs.try_shape().unwrap_or_else(|| node_shape.to_vec()),
                )?),
                rhs: Box::new(self.compile_numeric(
                    rhs,
                    &rhs.try_shape().unwrap_or_else(|| node_shape.to_vec()),
                )?),
            },
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
                CompiledNumericExprNode::CustomBinary {
                    lhs: Box::new(self.compile_numeric(
                        lhs,
                        &lhs.try_shape().unwrap_or_else(|| node_shape.to_vec()),
                    )?),
                    rhs: Box::new(self.compile_numeric(
                        rhs,
                        &rhs.try_shape().unwrap_or_else(|| node_shape.to_vec()),
                    )?),
                    func: BinaryOrUnary::Binary(func.clone()),
                }
            }
            NumericExprNode::Reduction {
                op,
                child,
                child_shape,
            } => CompiledNumericExprNode::Reduction {
                op: *op,
                child: Box::new(self.compile_numeric(child, child_shape)?),
                child_shape: child_shape.clone(),
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
            } => CompiledNumericExprNode::Conditional {
                condition: Box::new(self.compile_mask(
                    condition,
                    &condition.try_shape().unwrap_or_else(|| node_shape.to_vec()),
                )?),
                if_true: Box::new(self.compile_numeric(
                    if_true,
                    &if_true.try_shape().unwrap_or_else(|| node_shape.to_vec()),
                )?),
                if_false: Box::new(self.compile_numeric(
                    if_false,
                    &if_false.try_shape().unwrap_or_else(|| node_shape.to_vec()),
                )?),
            },
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
            } => CompiledNumericExprNode::Replace {
                primary: Box::new(self.compile_numeric(
                    primary,
                    &primary.try_shape().unwrap_or_else(|| node_shape.to_vec()),
                )?),
                replacement: Box::new(
                    self.compile_numeric(
                        replacement,
                        &replacement
                            .try_shape()
                            .unwrap_or_else(|| node_shape.to_vec()),
                    )?,
                ),
                mask: Arc::new(mask.clone()),
            },
            NumericExprNode::TypeBridge { eval_fn } => {
                let stride = vec![1; node_shape.len()];
                let data = eval_fn(&vec![0; node_shape.len()], node_shape, &stride)?;
                let source_id =
                    self.register_snapshot_source(data, advised_cursor_shape(node_shape));
                CompiledNumericExprNode::Source(source_id)
            }
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
            MaskExprNode::Logical { op, lhs, rhs } => {
                CompiledMaskExprNode::Logical {
                    op: *op,
                    lhs: Box::new(self.compile_mask(
                        lhs,
                        &lhs.try_shape().unwrap_or_else(|| node_shape.to_vec()),
                    )?),
                    rhs: Box::new(self.compile_mask(
                        rhs,
                        &rhs.try_shape().unwrap_or_else(|| node_shape.to_vec()),
                    )?),
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
        })
    }
}

fn compiled_source_cache_bytes<T>(cursor_shape: &[usize], requested_cache_bytes: usize) -> usize {
    if requested_cache_bytes > 0 {
        return requested_cache_bytes;
    }

    let tile_bytes = cursor_shape
        .iter()
        .product::<usize>()
        .saturating_mul(std::mem::size_of::<T>());

    // Compiled evaluation reads the source chunk by chunk. Reusing the
    // unbounded `0` cache from `PagedImage::open()` would push reopened worker
    // handles through the flat-cache path, which is the wrong shape for this
    // workload. Use a small bounded cache instead.
    tile_bytes.saturating_mul(4).max(tile_bytes)
}

fn expand_tile_aligned_cursor_shape(
    full_shape: &[usize],
    tile_shape: &[usize],
    max_pixels: usize,
) -> Vec<usize> {
    if full_shape.is_empty() {
        return vec![];
    }

    let mut cursor = tile_shape
        .iter()
        .zip(full_shape.iter())
        .map(|(&tile, &extent)| tile.min(extent).max(1))
        .collect::<Vec<_>>();
    let mut product = cursor.iter().product::<usize>().max(1);

    for axis in 0..cursor.len() {
        let tile = tile_shape[axis].max(1);
        let extent = full_shape[axis];
        while cursor[axis] < extent {
            let next = cursor[axis].saturating_add(tile).min(extent);
            let next_product = product / cursor[axis].max(1) * next;
            if next_product > max_pixels {
                break;
            }
            product = next_product;
            cursor[axis] = next;
        }
    }

    cursor
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
pub struct CompiledImageExpr<T: ImageExprValue> {
    node: CompiledNumericExprNode<T>,
    arena: Arc<CompiledImageArena<T>>,
    meta: ImageExprMeta,
    execution_policy: ExecutionPolicy,
    output_mask: Option<Arc<ArrayD<bool>>>,
}

/// Owned, compiled boolean mask expression sharing the same compiled source arena.
#[derive(Clone)]
pub struct CompiledMaskExpr<T: ImageExprValue> {
    node: CompiledMaskExprNode<T>,
    arena: Arc<CompiledImageArena<T>>,
    shape: Vec<usize>,
    execution_policy: ExecutionPolicy,
}

impl<T: ImageExprValue> std::fmt::Debug for CompiledImageExpr<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CompiledImageExpr")
            .field("shape", &self.meta.shape)
            .field("pixel_type", &T::PRIMITIVE_TYPE)
            .finish()
    }
}

impl<T: ImageExprValue> std::fmt::Debug for CompiledMaskExpr<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CompiledMaskExpr")
            .field("shape", &self.shape)
            .finish()
    }
}

impl<T: ImageExprValue> CompiledImageExpr<T> {
    /// Overrides the execution policy used by [`get`](Self::get),
    /// [`get_slice`](Self::get_slice), and [`save_as`](Self::save_as).
    ///
    /// Unlike C++ casacore, Rust exposes explicit performance policy control
    /// here. The policy changes *how* the compiled expression runs, not what
    /// values it produces.
    pub fn set_execution_policy(&mut self, policy: ExecutionPolicy) {
        self.execution_policy = policy;
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

        let cursor_shape =
            clamp_cursor_shape(&self.node.preferred_cursor_shape(shape, &self.arena), shape);
        let strategy = compiled_map_strategy(
            self.execution_policy,
            shape,
            &cursor_shape,
            self.arena.has_paged_sources(),
        );
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
        let full_shape = self.meta.shape.clone();
        let cursor_shape = clamp_cursor_shape(
            &self.node.preferred_cursor_shape(&full_shape, &self.arena),
            &full_shape,
        );
        let strategy = compiled_map_strategy(
            self.execution_policy,
            &full_shape,
            &cursor_shape,
            self.arena.has_paged_sources(),
        );

        let mut image = PagedImage::create_with_tile_shape(
            full_shape.clone(),
            cursor_shape.clone(),
            self.meta.coords.clone(),
            path,
        )?;

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
                Ok(EvaluatedChunk {
                    position: cursor.position,
                    data,
                })
            },
            |chunk| {
                image
                    .put_slice(&chunk.data, &chunk.position)
                    .map_err(|e| LatticeError::Table(e.to_string()))
            },
        )?;

        image.set_units(self.meta.units.clone())?;
        image.set_misc_info(self.meta.misc_info.clone())?;
        image.set_image_info(&self.meta.image_info)?;
        let saved_path = image.name().map(Path::to_path_buf);
        image.save()?;
        if let Some(path) = saved_path {
            let mut reopened = PagedImage::open_with_cache(&path, 0)?;
            if let Some(mask) = &self.output_mask {
                reopened.put_mask("compiled_mask", mask)?;
                reopened.set_default_mask("compiled_mask")?;
                reopened.save()?;
            }
            Ok(reopened)
        } else {
            if let Some(mask) = &self.output_mask {
                image.put_mask("compiled_mask", mask)?;
                image.set_default_mask("compiled_mask")?;
            }
            Ok(image)
        }
    }

    pub fn shape(&self) -> &[usize] {
        &self.meta.shape
    }

    pub fn ndim(&self) -> usize {
        self.meta.shape.len()
    }

    pub fn source_mask(&self) -> Option<ArrayD<bool>> {
        self.output_mask.as_deref().cloned()
    }
}

impl<T: ImageExprValue> CompiledMaskExpr<T> {
    /// Overrides the execution policy used by [`get`](Self::get) and
    /// [`get_slice`](Self::get_slice).
    pub fn set_execution_policy(&mut self, policy: ExecutionPolicy) {
        self.execution_policy = policy;
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
        validate_slice_request(&self.shape, start, shape, &vec![1; self.ndim()])?;
        if shape.is_empty() {
            let mut ctx = CompiledEvalContext::new(self.arena.sources.len());
            return self
                .node
                .eval_slice(&self.arena, &mut ctx, start, shape, &[])
                .map_err(Into::into);
        }

        let cursor_shape =
            clamp_cursor_shape(&self.node.preferred_cursor_shape(shape, &self.arena), shape);
        let strategy = compiled_map_strategy(
            self.execution_policy,
            shape,
            &cursor_shape,
            self.arena.has_paged_sources(),
        );
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
}

impl<T: ImageExprValue> CompiledNumericExprNode<T> {
    fn preferred_cursor_shape(
        &self,
        full_shape: &[usize],
        arena: &CompiledImageArena<T>,
    ) -> Vec<usize> {
        match self {
            Self::Source(source_id) => {
                let source = &arena.sources[*source_id];
                match &source.tile_shape {
                    Some(tile_shape) => {
                        expand_tile_aligned_cursor_shape(full_shape, tile_shape, 1_048_576)
                    }
                    None => source.cursor_shape.clone(),
                }
            }
            Self::UnaryOp { child, .. }
            | Self::CustomUnary { child, .. }
            | Self::Reduction { child, .. }
            | Self::Fractile { child, .. }
            | Self::FractileRange { child, .. } => child.preferred_cursor_shape(full_shape, arena),
            Self::BinaryOp { lhs, .. } | Self::CustomBinary { lhs, .. } => {
                lhs.preferred_cursor_shape(full_shape, arena)
            }
            Self::Conditional { if_true, .. } => if_true.preferred_cursor_shape(full_shape, arena),
            Self::Replace { primary, .. } => primary.preferred_cursor_shape(full_shape, arena),
            Self::Scalar(_) | Self::MaskCount { .. } => advised_cursor_shape(full_shape),
        }
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
                let data = child.eval_slice(arena, ctx, start, shape, stride)?;
                Ok(data.mapv(|value| apply_unary(*op, value)))
            }
            Self::BinaryOp { op, lhs, rhs } => {
                let lhs_data = lhs.eval_slice(arena, ctx, start, shape, stride)?;
                let rhs_data = rhs.eval_slice(arena, ctx, start, shape, stride)?;
                Ok(Zip::from(&lhs_data)
                    .and(&rhs_data)
                    .map_collect(|&lhs, &rhs| apply_binary(*op, lhs, rhs)))
            }
            Self::CustomUnary { child, func } => {
                let data = child.eval_slice(arena, ctx, start, shape, stride)?;
                let BinaryOrUnary::Unary(func) = func else {
                    unreachable!("custom unary node stored binary function")
                };
                Ok(data.mapv(|value| func(value)))
            }
            Self::CustomBinary { lhs, rhs, func } => {
                let lhs_data = lhs.eval_slice(arena, ctx, start, shape, stride)?;
                let rhs_data = rhs.eval_slice(arena, ctx, start, shape, stride)?;
                let BinaryOrUnary::Binary(func) = func else {
                    unreachable!("custom binary node stored unary function")
                };
                Ok(Zip::from(&lhs_data)
                    .and(&rhs_data)
                    .map_collect(|&lhs, &rhs| func(lhs, rhs)))
            }
            Self::Reduction {
                op,
                child,
                child_shape,
            } => Ok(ArrayD::from_elem(
                IxDyn(shape),
                reduce_numeric_child(*op, child, child_shape, arena)?,
            )),
            Self::Fractile {
                child,
                child_shape,
                fraction,
            } => {
                let mut values = collect_numeric_child(child, child_shape, arena)?;
                let value =
                    array_fractile_values(&mut values, *fraction).unwrap_or_else(T::default_value);
                Ok(ArrayD::from_elem(IxDyn(shape), value))
            }
            Self::FractileRange {
                child,
                child_shape,
                fraction1,
                fraction2,
            } => {
                let mut values = collect_numeric_child(child, child_shape, arena)?;
                let v1 = array_fractile_values(&mut values.clone(), *fraction1)
                    .unwrap_or_else(T::default_value);
                let v2 =
                    array_fractile_values(&mut values, *fraction2).unwrap_or_else(T::default_value);
                Ok(ArrayD::from_elem(IxDyn(shape), v2 - v1))
            }
            Self::Conditional {
                condition,
                if_true,
                if_false,
            } => {
                let cond = condition.eval_slice(arena, ctx, start, shape, stride)?;
                if mask_chunk_all(&cond) {
                    return if_true.eval_slice(arena, ctx, start, shape, stride);
                }
                if mask_chunk_none(&cond) {
                    return if_false.eval_slice(arena, ctx, start, shape, stride);
                }
                let mut out = if_true.eval_slice(arena, ctx, start, shape, stride)?;
                let f_data = if_false.eval_slice(arena, ctx, start, shape, stride)?;
                Zip::from(&cond).and(out.view_mut()).and(&f_data).for_each(
                    |&cond, out, &if_false| {
                        if !cond {
                            *out = if_false;
                        }
                    },
                );
                Ok(out)
            }
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
                replacement,
                mask,
            } => {
                let mask_data = slice_array_owned(mask.as_ref(), start, shape, stride)?;
                if mask_chunk_all(&mask_data) {
                    return primary.eval_slice(arena, ctx, start, shape, stride);
                }
                if mask_chunk_none(&mask_data) {
                    return replacement.eval_slice(arena, ctx, start, shape, stride);
                }
                let mut out = primary.eval_slice(arena, ctx, start, shape, stride)?;
                let replacement_data = replacement.eval_slice(arena, ctx, start, shape, stride)?;
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
        }
    }
}

impl<T: ImageExprValue> CompiledMaskExprNode<T> {
    fn preferred_cursor_shape(
        &self,
        full_shape: &[usize],
        arena: &CompiledImageArena<T>,
    ) -> Vec<usize> {
        match self {
            Self::CompareScalar { expr, .. } | Self::IsNan { child: expr } => {
                expr.preferred_cursor_shape(full_shape, arena)
            }
            Self::Logical { lhs, .. } => lhs.preferred_cursor_shape(full_shape, arena),
            Self::Not { child } | Self::AllReduce { child, .. } | Self::AnyReduce { child, .. } => {
                child.preferred_cursor_shape(full_shape, arena)
            }
            Self::ConstantMask { .. } => advised_cursor_shape(full_shape),
        }
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
            Self::Logical { op, lhs, rhs } => {
                let lhs_mask = lhs.eval_slice(arena, ctx, start, shape, stride)?;
                match op {
                    MaskLogicalOp::And => {
                        if mask_chunk_none(&lhs_mask) {
                            return Ok(ArrayD::from_elem(IxDyn(shape), false));
                        }
                        if mask_chunk_all(&lhs_mask) {
                            return rhs.eval_slice(arena, ctx, start, shape, stride);
                        }
                    }
                    MaskLogicalOp::Or => {
                        if mask_chunk_all(&lhs_mask) {
                            return Ok(ArrayD::from_elem(IxDyn(shape), true));
                        }
                        if mask_chunk_none(&lhs_mask) {
                            return rhs.eval_slice(arena, ctx, start, shape, stride);
                        }
                    }
                }
                let rhs_mask = rhs.eval_slice(arena, ctx, start, shape, stride)?;
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
        }
    }
}

impl<'a, T: ImageExprValue> ImageExpr<'a, T> {
    /// Compiles a borrowed lazy expression into an owned execution form.
    ///
    /// Compilation is Rust-specific functionality beyond C++ casacore's
    /// borrowed `ImageExpr<T>` surface. Persistent paged sources remain lazy
    /// and reopenable; non-persistent or non-downcastable sources are
    /// snapshotted so the compiled expression can be evaluated on worker
    /// threads safely.
    ///
    /// # Example
    ///
    /// ```rust
    /// use casacore_coordinates::CoordinateSystem;
    /// use casacore_images::{ImageExpr, TempImage};
    /// use casacore_lattices::{ExecutionPolicy, LatticeMut};
    ///
    /// let mut image = TempImage::<f32>::new(vec![16, 16], CoordinateSystem::new()).unwrap();
    /// image.set(2.0).unwrap();
    ///
    /// let mut compiled = ImageExpr::from_image(&image)
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
    pub fn compile(&self) -> Result<CompiledImageExpr<T>, ImageError> {
        let mut ctx = CompileCtx::default();
        let node = ctx.compile_numeric(&self.node, &self.meta.shape)?;
        Ok(CompiledImageExpr {
            node,
            arena: Arc::new(CompiledImageArena {
                sources: ctx.sources,
            }),
            meta: self.meta.clone(),
            execution_policy: ExecutionPolicy::Auto,
            output_mask: self.source_mask()?.map(Arc::new),
        })
    }
}

impl<'a, T: ImageExprValue + PartialOrd> MaskExpr<'a, T> {
    /// Compiles a borrowed lazy boolean mask into an owned execution form.
    pub fn compile(&self) -> Result<CompiledMaskExpr<T>, ImageError> {
        let mut ctx = CompileCtx::default();
        let node = ctx.compile_mask(&self.node, &self.shape)?;
        Ok(CompiledMaskExpr {
            node,
            arena: Arc::new(CompiledImageArena {
                sources: ctx.sources,
            }),
            shape: self.shape.clone(),
            execution_policy: ExecutionPolicy::Auto,
        })
    }
}

impl<T: ImageExprValue> Lattice<T> for CompiledImageExpr<T> {
    fn shape(&self) -> &[usize] {
        &self.meta.shape
    }

    fn is_writable(&self) -> bool {
        false
    }

    fn get_at(&self, position: &[usize]) -> Result<T, LatticeError> {
        CompiledImageExpr::get_at(self, position).map_err(|e| LatticeError::Table(e.to_string()))
    }

    fn get_slice(
        &self,
        start: &[usize],
        shape: &[usize],
        stride: &[usize],
    ) -> Result<ArrayD<T>, LatticeError> {
        if stride.iter().all(|&value| value == 1) {
            CompiledImageExpr::get_slice(self, start, shape)
                .map_err(|e| LatticeError::Table(e.to_string()))
        } else {
            CompiledImageExpr::get_slice_with_stride(self, start, shape, stride)
                .map_err(|e| LatticeError::Table(e.to_string()))
        }
    }

    fn get(&self) -> Result<ArrayD<T>, LatticeError> {
        CompiledImageExpr::get(self).map_err(|e| LatticeError::Table(e.to_string()))
    }
}

impl<T: ImageExprValue> ImageInterface<T> for CompiledImageExpr<T> {
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
        Ok(self.output_mask.as_deref().cloned())
    }
}

impl<T: ImageExprValue> Lattice<bool> for CompiledMaskExpr<T> {
    fn shape(&self) -> &[usize] {
        &self.shape
    }

    fn is_writable(&self) -> bool {
        false
    }

    fn get_at(&self, position: &[usize]) -> Result<bool, LatticeError> {
        CompiledMaskExpr::get_at(self, position).map_err(|e| LatticeError::Table(e.to_string()))
    }

    fn get_slice(
        &self,
        start: &[usize],
        shape: &[usize],
        stride: &[usize],
    ) -> Result<ArrayD<bool>, LatticeError> {
        if stride.iter().all(|&value| value == 1) {
            CompiledMaskExpr::get_slice(self, start, shape)
                .map_err(|e| LatticeError::Table(e.to_string()))
        } else {
            validate_slice_request(&self.shape, start, shape, stride)?;
            let mut ctx = CompiledEvalContext::new(self.arena.sources.len());
            self.node
                .eval_slice(&self.arena, &mut ctx, start, shape, stride)
        }
    }

    fn get(&self) -> Result<ArrayD<bool>, LatticeError> {
        CompiledMaskExpr::get(self).map_err(|e| LatticeError::Table(e.to_string()))
    }
}

fn compiled_map_strategy(
    policy: ExecutionPolicy,
    full_shape: &[usize],
    cursor_shape: &[usize],
    has_paged_sources: bool,
) -> OrderedCursorMapWriteExecutionStrategy {
    match policy {
        ExecutionPolicy::Serial => OrderedCursorMapWriteExecutionStrategy::Serial,
        ExecutionPolicy::Pipelined { prefetch_depth } => {
            OrderedCursorMapWriteExecutionStrategy::Pipelined(CursorMapWriteConfig {
                prefetch_depth: prefetch_depth.max(1),
            })
        }
        ExecutionPolicy::Parallel {
            workers,
            prefetch_depth,
        } => {
            let workers = workers.max(1);
            if workers <= 1 {
                OrderedCursorMapWriteExecutionStrategy::Pipelined(CursorMapWriteConfig {
                    prefetch_depth: prefetch_depth.max(1),
                })
            } else {
                OrderedCursorMapWriteExecutionStrategy::Parallel(ParallelReadChunkConfig {
                    workers,
                    prefetch_depth: prefetch_depth.max(workers),
                })
            }
        }
        ExecutionPolicy::Auto => {
            let available = thread_parallelism();
            if available < 2 || full_shape.is_empty() {
                return OrderedCursorMapWriteExecutionStrategy::Serial;
            }
            let total_pixels = full_shape.iter().product::<usize>();
            let task_count = TraversalCursorIter::new(
                full_shape.to_vec(),
                cursor_shape.to_vec(),
                TraversalSpec::chunks(cursor_shape.to_vec()),
            )
            .size_hint()
            .1
            .unwrap_or(0);
            if task_count < 2 || total_pixels < 131_072 {
                return OrderedCursorMapWriteExecutionStrategy::Serial;
            }

            // Parallel chunk execution pays off only once there is enough work
            // per worker to amortize task scheduling and per-chunk evaluation
            // overhead. Moderate paged cubes like 64^3 benefit more from
            // overlap-only pipelining than from a full worker pool.
            let pixels_per_worker_threshold = if has_paged_sources { 200_000 } else { 150_000 };
            let parallel_threshold = available.saturating_mul(pixels_per_worker_threshold);
            let min_parallel_tasks = if has_paged_sources {
                2
            } else {
                available.saturating_mul(2)
            };
            let enough_parallel_work =
                total_pixels >= parallel_threshold && task_count >= min_parallel_tasks;

            if has_paged_sources && !enough_parallel_work {
                OrderedCursorMapWriteExecutionStrategy::Pipelined(CursorMapWriteConfig {
                    prefetch_depth: available.max(2),
                })
            } else if !enough_parallel_work {
                OrderedCursorMapWriteExecutionStrategy::Serial
            } else {
                let workers = available.min(task_count.max(1));
                OrderedCursorMapWriteExecutionStrategy::Parallel(ParallelReadChunkConfig {
                    workers,
                    prefetch_depth: workers * 2,
                })
            }
        }
    }
}

fn reduce_numeric_child<T: ImageExprValue>(
    op: ReductionOp,
    child: &CompiledNumericExprNode<T>,
    child_shape: &[usize],
    arena: &CompiledImageArena<T>,
) -> Result<T, LatticeError> {
    if child_shape.is_empty() {
        let mut ctx = CompiledEvalContext::new(arena.sources.len());
        let scalar = child.eval_slice(arena, &mut ctx, &[], &[], &[])?;
        return Ok(scalar.iter().copied().next().unwrap_or_default());
    }

    match op {
        ReductionOp::Sum => {
            let mut sum = T::default_value();
            for_each_compiled_numeric_chunk(child, child_shape, arena, |chunk| {
                for &value in chunk {
                    sum = sum + value;
                }
                Ok(())
            })?;
            Ok(sum)
        }
        ReductionOp::Mean => {
            let mut sum = T::default_value();
            let mut count = 0usize;
            for_each_compiled_numeric_chunk(child, child_shape, arena, |chunk| {
                count += chunk.len();
                for &value in chunk {
                    sum = sum + value;
                }
                Ok(())
            })?;
            if count == 0 {
                Ok(T::default_value())
            } else {
                Ok(sum * T::from_f64_lossy(1.0 / count as f64))
            }
        }
        ReductionOp::Min => {
            let mut seen = false;
            let mut min = T::default_value();
            for_each_compiled_numeric_chunk(child, child_shape, arena, |chunk| {
                for &value in chunk {
                    if !seen {
                        min = value;
                        seen = true;
                    } else {
                        min = min.expr_min(value);
                    }
                }
                Ok(())
            })?;
            Ok(if seen { min } else { T::default_value() })
        }
        ReductionOp::Max => {
            let mut seen = false;
            let mut max = T::default_value();
            for_each_compiled_numeric_chunk(child, child_shape, arena, |chunk| {
                for &value in chunk {
                    if !seen {
                        max = value;
                        seen = true;
                    } else {
                        max = max.expr_max(value);
                    }
                }
                Ok(())
            })?;
            Ok(if seen { max } else { T::default_value() })
        }
        ReductionOp::Median => {
            let mut values = collect_numeric_child(child, child_shape, arena)?;
            array_fractile_values(&mut values, 0.5).ok_or_else(|| {
                LatticeError::InvalidTraversal("median on empty compiled child".to_string())
            })
        }
    }
}

fn collect_numeric_child<T: ImageExprValue>(
    child: &CompiledNumericExprNode<T>,
    child_shape: &[usize],
    arena: &CompiledImageArena<T>,
) -> Result<Vec<T>, LatticeError> {
    let mut values = Vec::with_capacity(child_shape.iter().product());
    for_each_compiled_numeric_chunk(child, child_shape, arena, |chunk| {
        values.extend(chunk.iter().copied());
        Ok(())
    })?;
    Ok(values)
}

fn for_each_compiled_numeric_chunk<T: ImageExprValue>(
    node: &CompiledNumericExprNode<T>,
    full_shape: &[usize],
    arena: &CompiledImageArena<T>,
    mut f: impl FnMut(&ArrayD<T>) -> Result<(), LatticeError>,
) -> Result<(), LatticeError> {
    if full_shape.is_empty() {
        let mut ctx = CompiledEvalContext::new(arena.sources.len());
        let empty = node.eval_slice(arena, &mut ctx, &[], &[], &[])?;
        return f(&empty);
    }
    let cursor_shape =
        clamp_cursor_shape(&node.preferred_cursor_shape(full_shape, arena), full_shape);
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
    let cursor_shape =
        clamp_cursor_shape(&node.preferred_cursor_shape(full_shape, arena), full_shape);
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

fn apply_unary<T: ImageExprValue>(op: ImageExprUnaryOp, value: T) -> T {
    match op {
        ImageExprUnaryOp::Negate => -value,
        ImageExprUnaryOp::Exp => value.expr_exp(),
        ImageExprUnaryOp::Sin => value.expr_sin(),
        ImageExprUnaryOp::Cos => value.expr_cos(),
        ImageExprUnaryOp::Tan => value.expr_tan(),
        ImageExprUnaryOp::Asin => value.expr_asin(),
        ImageExprUnaryOp::Acos => value.expr_acos(),
        ImageExprUnaryOp::Atan => value.expr_atan(),
        ImageExprUnaryOp::Sinh => value.expr_sinh(),
        ImageExprUnaryOp::Cosh => value.expr_cosh(),
        ImageExprUnaryOp::Tanh => value.expr_tanh(),
        ImageExprUnaryOp::Log => value.expr_log(),
        ImageExprUnaryOp::Log10 => value.expr_log10(),
        ImageExprUnaryOp::Sqrt => value.expr_sqrt(),
        ImageExprUnaryOp::Abs => value.expr_abs(),
        ImageExprUnaryOp::Ceil => value.expr_ceil(),
        ImageExprUnaryOp::Floor => value.expr_floor(),
        ImageExprUnaryOp::Round => value.expr_round(),
        ImageExprUnaryOp::Sign => value.expr_sign(),
        ImageExprUnaryOp::Conj => value.expr_conj(),
    }
}

fn apply_binary<T: ImageExprValue>(op: ImageExprBinaryOp, lhs: T, rhs: T) -> T {
    match op {
        ImageExprBinaryOp::Add => lhs + rhs,
        ImageExprBinaryOp::Subtract => lhs - rhs,
        ImageExprBinaryOp::Multiply => lhs * rhs,
        ImageExprBinaryOp::Divide => lhs / rhs,
        ImageExprBinaryOp::Pow => lhs.expr_pow(rhs),
        ImageExprBinaryOp::Fmod => lhs.expr_fmod(rhs),
        ImageExprBinaryOp::Atan2 => lhs.expr_atan2(rhs),
        ImageExprBinaryOp::Min => lhs.expr_min(rhs),
        ImageExprBinaryOp::Max => lhs.expr_max(rhs),
    }
}

fn array_fractile_values<T: ImageExprValue>(values: &mut [T], fraction: f64) -> Option<T> {
    if values.is_empty() {
        return None;
    }
    values.sort_by(|lhs, rhs| {
        lhs.to_f64_lossy()
            .partial_cmp(&rhs.to_f64_lossy())
            .unwrap_or(std::cmp::Ordering::Equal)
    });
    let n = values.len();
    let index = (fraction * (n.saturating_sub(1)) as f64).floor() as usize;
    values.get(index.min(n.saturating_sub(1))).copied()
}

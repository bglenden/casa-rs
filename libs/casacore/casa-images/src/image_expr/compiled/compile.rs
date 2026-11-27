// SPDX-License-Identifier: LGPL-3.0-or-later

use super::*;

pub(super) struct CompileCtx<T: ImageExprValue> {
    pub(super) sources: Vec<CompiledImageSource<T>>,
    pub(super) paged_source_ids: std::collections::HashMap<PathBuf, usize>,
    pub(super) transient_source_ids: std::collections::HashMap<usize, usize>,
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

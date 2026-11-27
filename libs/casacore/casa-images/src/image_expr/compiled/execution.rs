// SPDX-License-Identifier: LGPL-3.0-or-later

use super::*;

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
    pub fn may_have_mask(&self) -> bool {
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

    pub(in crate::image_expr) fn mapped_sections<'a, U: ImageExprValue>(
        self,
        convert: impl Fn(T) -> U + Copy + Send + Sync + 'static,
    ) -> ImageExprBuilder<'a, U> {
        let meta = self.meta.clone();
        let compiled = Arc::new(self);
        let mask = compiled.may_have_mask().then(|| {
            let source = Arc::clone(&compiled);
            Arc::new(move |start: &[usize], shape: &[usize], stride: &[usize]| {
                ImageInterface::get_mask_slice(source.as_ref(), start, shape, stride)
            }) as super::super::OwnedMaskReader
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

    pub fn visit_pixel_sections(
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

    pub fn save_as_with_full_metadata(
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

    pub fn legacy_coords_ref(&self) -> Option<&RecordValue> {
        self.meta.legacy_coords.as_ref()
    }

    pub(in crate::image_expr) fn raw_image_info_ref(&self) -> Option<&RecordValue> {
        self.meta.raw_image_info.as_ref()
    }

    pub fn raw_image_info(&self) -> Option<RecordValue> {
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

    pub(super) fn eval_full_mask(&self) -> Result<Option<Arc<ArrayD<bool>>>, ImageError> {
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
    pub fn is_shape_polymorphic(&self) -> bool {
        self.node.is_shape_polymorphic()
    }

    pub fn get_in_sections(&self) -> Result<ArrayD<bool>, ImageError> {
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

    pub fn eval_window_for_shape(
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

pub(super) struct CompiledMapInputs<'a> {
    pub(super) policy: ExecutionPolicy,
    pub(super) full_shape: &'a [usize],
    pub(super) cursor_shape: &'a [usize],
    pub(super) has_paged_sources: bool,
    pub(super) source_count: usize,
    pub(super) repeated_source_count: usize,
    pub(super) output_element_bytes: usize,
    pub(super) resources: ExecutionResources,
}

pub(super) fn compiled_map_strategy<T: ImageExprValue>(
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

pub fn bounded_read_cursor(shape: Vec<usize>) -> Result<Vec<usize>, LatticeError> {
    let element_bytes = std::mem::size_of::<casa_core::Complex64>();
    Ok(TiledShape::new(shape, element_bytes)?.tile_shape().to_vec())
}

pub(super) fn for_each_compiled_numeric_and_mask_chunk<T: ImageExprValue>(
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

pub(super) fn reduce_numeric_child<T: ImageExprValue>(
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

pub(super) fn collect_numeric_child<T: ImageExprValue>(
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

pub(super) fn for_each_compiled_mask_chunk<T: ImageExprValue>(
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

pub(super) fn add_positions(base: &[usize], offset: &[usize]) -> Vec<usize> {
    base.iter()
        .zip(offset.iter())
        .map(|(&base, &offset)| base + offset)
        .collect()
}

pub(super) struct StreamingMaskState {
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

pub struct MemorySnapshotImage<T: ImagePixel> {
    pub data: Arc<ArrayD<T>>,
    pub mask: Option<Arc<ArrayD<bool>>>,
    pub coordinates: CoordinateSystem,
    pub units: String,
    pub image_info: ImageInfo,
    pub misc_info: RecordValue,
    pub legacy_coords: Option<RecordValue>,
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

pub(super) fn slice_array_owned<T: Clone>(
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

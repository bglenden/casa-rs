// SPDX-License-Identifier: LGPL-3.0-or-later

use super::*;

impl<T: ImagePixel> ImageInterface<T> for PagedImage<T> {
    fn as_any(&self) -> Option<&dyn Any> {
        Some(self)
    }

    fn coordinates(&self) -> &CoordinateSystem {
        PagedImage::coordinates(self)
    }

    fn units(&self) -> &str {
        PagedImage::units(self)
    }

    fn misc_info(&self) -> RecordValue {
        PagedImage::misc_info(self)
    }

    fn image_info(&self) -> Result<ImageInfo, ImageError> {
        PagedImage::image_info(self)
    }

    fn name(&self) -> Option<&Path> {
        PagedImage::name(self)
    }

    fn default_mask_name(&self) -> Option<String> {
        PagedImage::default_mask_name(self)
    }

    fn mask_names(&self) -> Vec<String> {
        PagedImage::mask_names(self)
    }

    fn get_named_mask(&self, name: &str) -> Result<ArrayD<bool>, ImageError> {
        PagedImage::get_named_mask(self, name)
    }

    fn get_named_mask_slice(
        &self,
        name: &str,
        start: &[usize],
        shape: &[usize],
        stride: &[usize],
    ) -> Result<ArrayD<bool>, ImageError> {
        PagedImage::get_named_mask_slice(self, name, start, shape, stride)
    }

    fn history(&self) -> Result<Vec<String>, ImageError> {
        PagedImage::history(self)
    }

    fn default_mask(&self) -> Result<Option<ArrayD<bool>>, ImageError> {
        match PagedImage::default_mask_name(self) {
            Some(name) => Ok(Some(PagedImage::get_named_mask(self, &name)?)),
            None => Ok(None),
        }
    }
}

impl<T: ImagePixel> MutableImageInterface<T> for PagedImage<T> {
    fn set_coordinates(&mut self, coords: CoordinateSystem) -> Result<(), ImageError> {
        PagedImage::set_coordinates(self, coords)
    }

    fn set_units_string(&mut self, units: String) -> Result<(), ImageError> {
        PagedImage::set_units(self, units)
    }

    fn set_image_info(&mut self, info: &ImageInfo) -> Result<(), ImageError> {
        PagedImage::set_image_info(self, info)
    }

    fn set_image_info_record(&mut self, record: RecordValue) -> Result<(), ImageError> {
        PagedImage::set_image_info_record(self, record)
    }

    fn set_misc_info(&mut self, rec: RecordValue) -> Result<(), ImageError> {
        PagedImage::set_misc_info(self, rec)
    }

    fn put_mask(&mut self, name: &str, data: &ArrayD<bool>) -> Result<(), ImageError> {
        PagedImage::put_mask(self, name, data)
    }

    fn create_filled_mask(&mut self, name: &str, value: bool) -> Result<(), ImageError> {
        validate_new_mask_name(name, &self.mask_names())?;
        let Some(image_path) = &self.path else {
            return self.put_mask(name, &ArrayD::from_elem(IxDyn(self.shape()), value));
        };
        let stage = tempfile::tempdir_in(image_path).map_err(|e| ImageError::Io(e.to_string()))?;
        let staged_path = stage.path().join("mask");
        let tiled = TiledShape::with_tile_shape(self.shape.clone(), self.tile_shape.clone())?;
        let layout = PagedArray::<bool>::storage_layout(tiled, self.cache_bytes())?;
        TiledArrayStorage::create_filled_bool_table(&staged_path, &layout, value)
            .map_err(|e| ImageError::Table(e.to_string()))?;
        casa_tables::publication::rename_without_replacing(&staged_path, &image_path.join(name))
            .map_err(|e| ImageError::Io(e.to_string()))?;
        self.register_paged_mask(name);
        Ok(())
    }

    fn with_mask_slice_writer(
        &mut self,
        name: &str,
        edit: impl FnOnce(&mut MaskSliceWriter<'_>) -> Result<(), ImageError>,
    ) -> Result<(), ImageError> {
        let record = match self.table.keywords().get(MASKS_KEYWORD) {
            Some(Value::Record(masks)) => masks.get(name),
            _ => None,
        };
        let (Some(image_path), Some(Value::Record(record))) = (&self.path, record) else {
            return edit(&mut |data, start| self.put_mask_slice(name, data, start));
        };
        let relative_path = Self::persistent_mask_relative_path(record)?;
        let path = casa_tables::add_directory(&relative_path, image_path);
        let mut mask = PagedArray::<bool>::open_with_cache(path, self.cache_bytes())?;
        if mask.shape() != self.shape() {
            return Err(ImageError::ShapeMismatch {
                expected: self.shape.clone(),
                got: mask.shape().to_vec(),
            });
        }
        let edited = edit(&mut |data, start| mask.put_slice(data, start).map_err(Into::into));
        let flushed = mask.flush().map_err(Into::into);
        // Other names can point at the same on-disk mask.
        self.persistent_masks.borrow_mut().clear();
        edited.and(flushed)
    }

    fn named_mask_is_writable(&self, name: &str) -> Result<bool, ImageError> {
        let Some(Value::Record(masks)) = self.table.keywords().get(MASKS_KEYWORD) else {
            return Ok(false);
        };
        match masks.get(name) {
            Some(Value::Array(ArrayValue::Bool(mask))) => {
                if mask.shape() != self.shape() {
                    return Err(ImageError::ShapeMismatch {
                        expected: self.shape.clone(),
                        got: mask.shape().to_vec(),
                    });
                }
                Ok(true)
            }
            Some(Value::Record(_)) if self.path.is_none() => Ok(self.temp_masks.contains_key(name)),
            Some(Value::Record(record)) => {
                let reference = Self::persistent_mask_relative_path(record)?;
                let path = casa_tables::add_directory(
                    &reference,
                    self.path.as_deref().ok_or(ImageError::NotPersistent)?,
                );
                match casa_tables::publication::check_write_access(&path.join("table.dat")) {
                    Ok(()) => self.with_persistent_mask(name, record, |mask| {
                        if mask.shape() == self.shape() {
                            Ok(true)
                        } else {
                            Err(ImageError::ShapeMismatch {
                                expected: self.shape.clone(),
                                got: mask.shape().to_vec(),
                            })
                        }
                    }),
                    Err(error)
                        if matches!(
                            error.kind(),
                            std::io::ErrorKind::PermissionDenied
                                | std::io::ErrorKind::ReadOnlyFilesystem
                        ) =>
                    {
                        Ok(false)
                    }
                    Err(error) => Err(ImageError::Io(format!(
                        "open mask {}: {error}",
                        path.display()
                    ))),
                }
            }
            Some(_) => Err(ImageError::InvalidMetadata(
                "mask keyword is neither a bool array nor a paged-mask record".into(),
            )),
            None => Ok(false),
        }
    }

    fn put_mask_slice(
        &mut self,
        name: &str,
        data: &ArrayD<bool>,
        start: &[usize],
    ) -> Result<(), ImageError> {
        PagedImage::put_mask_slice(self, name, data, start)
    }

    fn remove_mask(&mut self, name: &str) -> Result<(), ImageError> {
        PagedImage::remove_mask(self, name)
    }

    fn set_default_mask(&mut self, name: &str) -> Result<(), ImageError> {
        PagedImage::set_default_mask(self, name)
    }

    fn unset_default_mask(&mut self) -> Result<(), ImageError> {
        PagedImage::unset_default_mask(self)
    }

    fn add_history_entry(&mut self, msg: String) -> Result<(), ImageError> {
        PagedImage::add_history(self, msg)
    }

    fn clear_history(&mut self) -> Result<(), ImageError> {
        PagedImage::clear_history(self)
    }
}

impl<T: ImagePixel> Lattice<T> for PagedImage<T> {
    fn shape(&self) -> &[usize] {
        &self.shape
    }

    fn is_persistent(&self) -> bool {
        self.is_persistent()
    }

    fn is_paged(&self) -> bool {
        self.path.is_some()
    }

    fn is_writable(&self) -> bool {
        self.is_writable()
    }

    fn get_at(&self, position: &[usize]) -> Result<T, LatticeError> {
        if position.len() != self.shape.len() {
            return Err(LatticeError::NdimMismatch {
                expected: self.shape.len(),
                got: position.len(),
            });
        }
        for (&idx, &dim) in position.iter().zip(self.shape.iter()) {
            if idx >= dim {
                return Err(LatticeError::IndexOutOfBounds {
                    index: position.to_vec(),
                    shape: self.shape.clone(),
                });
            }
        }
        if let Some(ref tio) = self.tiled_io {
            return tio
                .borrow_mut()
                .get_at::<T>(position)
                .map_err(|e| LatticeError::Table(e.to_string()));
        }
        Ok(self
            .read_array()
            .map_err(|e| LatticeError::Table(e.to_string()))?[IxDyn(position)])
    }

    fn get_slice(
        &self,
        start: &[usize],
        shape: &[usize],
        stride: &[usize],
    ) -> Result<ArrayD<T>, LatticeError> {
        let ndim = self.shape.len();
        if start.len() != ndim || shape.len() != ndim || stride.len() != ndim {
            return Err(LatticeError::NdimMismatch {
                expected: ndim,
                got: start.len(),
            });
        }
        // Tile-aware path with unit stride.
        if let Some(ref tio) = self.tiled_io {
            let is_unit_stride = stride.iter().all(|&s| s == 1);
            if is_unit_stride {
                let arr = tio
                    .borrow_mut()
                    .get_slice::<T>(start, shape)
                    .map_err(|e| LatticeError::Table(e.to_string()))?;
                return Ok(arr);
            }
            let bounding_shape = shape
                .iter()
                .zip(stride.iter())
                .map(|(&count, &step)| {
                    if step == 0 {
                        return Err(LatticeError::InvalidTraversal(
                            "slice stride must be positive".into(),
                        ));
                    }
                    count
                        .saturating_sub(1)
                        .checked_mul(step)
                        .and_then(|extent| extent.checked_add(usize::from(count > 0)))
                        .ok_or_else(|| {
                            LatticeError::InvalidTraversal("strided slice extent overflow".into())
                        })
                })
                .collect::<Result<Vec<_>, _>>()?;
            if start.iter().zip(&bounding_shape).zip(&self.shape).any(
                |((&start, &extent), &limit)| {
                    start.checked_add(extent).is_none_or(|end| end > limit)
                },
            ) {
                return Err(LatticeError::IndexOutOfBounds {
                    index: start.to_vec(),
                    shape: self.shape.clone(),
                });
            }
            shape
                .iter()
                .try_fold(1_usize, |count, &n| count.checked_mul(n))
                .ok_or_else(|| LatticeError::InvalidTraversal("slice size overflow".into()))?;
            return tio
                .borrow_mut()
                .get_strided_slice::<T>(start, shape, stride)
                .map_err(|error| LatticeError::Table(error.to_string()));
        }
        let array = self
            .read_array()
            .map_err(|e| LatticeError::Table(e.to_string()))?;
        ArrayLattice::new(array).get_slice(start, shape, stride)
    }

    fn get(&self) -> Result<ArrayD<T>, LatticeError> {
        // Tile-aware full read.
        if let Some(ref tio) = self.tiled_io {
            return tio
                .borrow_mut()
                .get_all::<T>()
                .map_err(|e| LatticeError::Table(e.to_string()));
        }
        self.read_array()
            .map_err(|e| LatticeError::Table(e.to_string()))
    }

    fn nice_cursor_shape(&self) -> Vec<usize> {
        self.tile_shape.clone()
    }

    fn enter_traversal_cache_scope<'a>(
        &'a self,
        hint: &TraversalCacheHint,
    ) -> Result<Option<Box<dyn TraversalCacheScope + 'a>>, LatticeError> {
        if self.path.is_none() || self.tiled_io.is_none() {
            return Ok(None);
        }
        let previous_cache_bytes = self.max_cache_bytes.get();
        let elem_size = Self::element_size_bytes().max(1);
        let recommended_tiles =
            recommended_tile_cache_size(&self.shape, &self.tile_shape, hint, None).max(1);
        let recommended_bytes = recommended_tiles
            .saturating_mul(self.tile_pixels())
            .saturating_mul(elem_size);
        if previous_cache_bytes == recommended_bytes {
            return Ok(None);
        }
        self.set_cache_bytes_shared(recommended_bytes)
            .map_err(|e| LatticeError::Table(e.to_string()))?;
        Ok(Some(Box::new(PagedImageTraversalCacheScope {
            image: self,
            previous_cache_bytes,
        })))
    }
}

impl<T: ImagePixel> LatticeMut<T> for PagedImage<T> {
    fn with_traversal_cache_hint_mut<R>(
        &mut self,
        hint: &TraversalCacheHint,
        f: impl FnOnce(&mut Self) -> Result<R, LatticeError>,
    ) -> Result<R, LatticeError>
    where
        Self: Sized,
    {
        if self.path.is_none() || self.tiled_io.is_none() {
            return f(self);
        }
        let previous_cache_bytes = self.max_cache_bytes.get();
        let elem_size = Self::element_size_bytes().max(1);
        let recommended_tiles =
            recommended_tile_cache_size(&self.shape, &self.tile_shape, hint, None).max(1);
        let recommended_bytes = recommended_tiles
            .saturating_mul(self.tile_pixels())
            .saturating_mul(elem_size);
        if previous_cache_bytes == recommended_bytes {
            return f(self);
        }

        self.set_cache_bytes_shared(recommended_bytes)
            .map_err(|e| LatticeError::Table(e.to_string()))?;
        let result = f(self);
        let restore = self
            .set_cache_bytes_shared(previous_cache_bytes)
            .map_err(|e| LatticeError::Table(e.to_string()));
        match (result, restore) {
            (Err(err), _) => Err(err),
            (Ok(_), Err(err)) => Err(err),
            (Ok(value), Ok(())) => Ok(value),
        }
    }

    fn put_at(&mut self, value: T, position: &[usize]) -> Result<(), LatticeError> {
        if position.len() != self.shape.len() {
            return Err(LatticeError::NdimMismatch {
                expected: self.shape.len(),
                got: position.len(),
            });
        }
        if self.tiled_io.is_some() {
            let pixel = ArrayD::from_elem(IxDyn(&vec![1; self.shape.len()]), value);
            return paged_image_put_slice_view(self, pixel.view(), position);
        }
        let mut array = self
            .read_array()
            .map_err(|e| LatticeError::Table(e.to_string()))?;
        let pixel =
            array
                .get_mut(IxDyn(position))
                .ok_or_else(|| LatticeError::IndexOutOfBounds {
                    index: position.to_vec(),
                    shape: self.shape.clone(),
                })?;
        *pixel = value;
        self.write_array(&array)
            .map_err(|e| LatticeError::Table(e.to_string()))
    }

    fn put_slice(&mut self, data: &ArrayD<T>, start: &[usize]) -> Result<(), LatticeError> {
        paged_image_put_slice_view(self, data.view(), start)
    }

    fn set(&mut self, value: T) -> Result<(), LatticeError> {
        if self.tiled_io.is_some() {
            let cursors = TraversalCursorIter::new(
                self.shape.clone(),
                self.tile_shape.clone(),
                TraversalSpec::tiles(),
            );
            for cursor in cursors {
                let cursor = cursor?;
                let tile = ArrayD::from_elem(IxDyn(&cursor.shape), value);
                paged_image_put_slice_view(self, tile.view(), &cursor.position)?;
            }
            return Ok(());
        }
        let array = ArrayD::from_elem(IxDyn(&self.shape), value);
        self.write_array(&array)
            .map_err(|e| LatticeError::Table(e.to_string()))
    }
}

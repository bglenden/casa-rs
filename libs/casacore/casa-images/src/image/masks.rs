// SPDX-License-Identifier: LGPL-3.0-or-later

use super::*;

pub fn make_paged_mask_record(relative_mask_path: &str, shape: &[usize]) -> RecordValue {
    let mut record = RecordValue::default();
    record.upsert(
        REGION_TYPE_FIELD,
        Value::Scalar(ScalarValue::Int32(REGION_TYPE_LC)),
    );
    record.upsert(
        REGION_NAME_FIELD,
        Value::Scalar(ScalarValue::String(LCPAGEDMASK_NAME.into())),
    );
    record.upsert(
        REGION_COMMENT_FIELD,
        Value::Scalar(ScalarValue::String(String::new())),
    );
    record.upsert("mask", Value::TableRef(relative_mask_path.to_string()));
    record.upsert("box", Value::Record(make_lcbox_record(shape)));
    record
}

pub(super) fn make_lcbox_record(shape: &[usize]) -> RecordValue {
    let blc: Vec<f32> = vec![1.0; shape.len()];
    let trc: Vec<f32> = shape.iter().map(|&dim| dim as f32).collect();
    let shape_i32: Vec<i32> = shape.iter().map(|&dim| dim as i32).collect();
    let mut record = RecordValue::default();
    record.upsert(
        REGION_TYPE_FIELD,
        Value::Scalar(ScalarValue::Int32(REGION_TYPE_LC)),
    );
    record.upsert(
        REGION_NAME_FIELD,
        Value::Scalar(ScalarValue::String(LCBOX_NAME.into())),
    );
    record.upsert(
        REGION_COMMENT_FIELD,
        Value::Scalar(ScalarValue::String(String::new())),
    );
    record.upsert("oneRel", Value::Scalar(ScalarValue::Bool(true)));
    record.upsert("blc", Value::Array(ArrayValue::from_f32_vec(blc)));
    record.upsert("trc", Value::Array(ArrayValue::from_f32_vec(trc)));
    record.upsert("shape", Value::Array(ArrayValue::from_i32_vec(shape_i32)));
    record
}

pub(crate) fn slice_mask_array(
    mask: &ArrayD<bool>,
    start: &[usize],
    shape: &[usize],
    stride: &[usize],
) -> Result<ArrayD<bool>, ImageError> {
    let ndim = mask.ndim();
    if start.len() != ndim || shape.len() != ndim || stride.len() != ndim {
        return Err(ImageError::ShapeMismatch {
            expected: vec![ndim],
            got: vec![start.len(), shape.len(), stride.len()],
        });
    }
    for axis in 0..ndim {
        let Some(last) = shape[axis]
            .checked_sub(1)
            .and_then(|count| count.checked_mul(stride[axis]))
            .and_then(|offset| start[axis].checked_add(offset))
        else {
            return Err(ImageError::ShapeMismatch {
                expected: mask.shape().to_vec(),
                got: start.to_vec(),
            });
        };
        if stride[axis] == 0
            || (shape[axis] > 0
                && (start[axis] >= mask.shape()[axis] || last >= mask.shape()[axis]))
        {
            return Err(ImageError::ShapeMismatch {
                expected: mask.shape().to_vec(),
                got: start.to_vec(),
            });
        }
    }

    let slice_info: Vec<SliceInfoElem> = start
        .iter()
        .zip(shape.iter())
        .zip(stride.iter())
        .map(|((&s, &n), &st)| {
            let end = s + (n - 1) * st + 1;
            SliceInfoElem::Slice {
                start: s as isize,
                end: Some(end as isize),
                step: if n == 1 { 1 } else { st as isize },
            }
        })
        .collect();
    let view = mask.slice(slice_info.as_slice());
    Ok(view.to_owned())
}

pub(crate) fn validate_new_mask_name(name: &str, existing: &[String]) -> Result<(), ImageError> {
    if Path::new(name).file_name().and_then(|name| name.to_str()) != Some(name) {
        return Err(ImageError::InvalidMetadata(format!(
            "Invalid mask name {name}"
        )));
    }
    if existing.iter().any(|entry| entry == name) {
        return Err(ImageError::InvalidMetadata(format!(
            "Mask {name} already exists"
        )));
    }
    Ok(())
}

pub(crate) fn validate_mask_slice_write(
    full_shape: &[usize],
    data_shape: &[usize],
    start: &[usize],
) -> Result<(), ImageError> {
    if start.len() != full_shape.len() || data_shape.len() != full_shape.len() {
        return Err(ImageError::ShapeMismatch {
            expected: full_shape.to_vec(),
            got: data_shape.to_vec(),
        });
    }
    for axis in 0..full_shape.len() {
        let Some(end) = start[axis].checked_add(data_shape[axis]) else {
            return Err(ImageError::ShapeMismatch {
                expected: full_shape.to_vec(),
                got: start.to_vec(),
            });
        };
        if end > full_shape[axis] {
            return Err(ImageError::ShapeMismatch {
                expected: full_shape.to_vec(),
                got: start.to_vec(),
            });
        }
    }
    Ok(())
}

pub(crate) fn assign_mask_slice(
    mask: &mut ArrayD<bool>,
    data: &ArrayD<bool>,
    start: &[usize],
) -> Result<(), ImageError> {
    validate_mask_slice_write(mask.shape(), data.shape(), start)?;
    let slice_info: Vec<SliceInfoElem> = start
        .iter()
        .zip(data.shape().iter())
        .map(|(&s, &n)| SliceInfoElem::Slice {
            start: s as isize,
            end: Some((s + n) as isize),
            step: 1,
        })
        .collect();
    mask.slice_mut(slice_info.as_slice()).assign(data);
    Ok(())
}

impl<T: ImagePixel> PagedImage<T> {
    /// Returns `true` if the image has a default pixel mask.
    pub fn has_pixel_mask(&self) -> bool {
        <Self as ImageInterface<T>>::has_pixel_mask(self)
    }

    /// Returns the default mask name if present.
    pub fn default_mask_name(&self) -> Option<String> {
        match self.table.keywords().get(DEFAULT_MASK_KEYWORD) {
            Some(Value::Scalar(ScalarValue::String(name))) if !name.is_empty() => {
                Some(name.clone())
            }
            _ => None,
        }
    }

    /// Returns the names of all stored masks.
    pub fn mask_names(&self) -> Vec<String> {
        match self.table.keywords().get(MASKS_KEYWORD) {
            Some(Value::Record(rec)) => rec.fields().iter().map(|f| f.name.clone()).collect(),
            _ => Vec::new(),
        }
    }

    /// Returns the names of all stored image-attached regions.
    pub fn region_names(&self) -> Vec<String> {
        match self.table.keywords().get(REGIONS_KEYWORD) {
            Some(Value::Record(rec)) => rec.fields().iter().map(|f| f.name.clone()).collect(),
            _ => Vec::new(),
        }
    }

    /// Returns a named stored image-attached region record.
    pub fn get_region_record(&self, name: &str) -> Result<RecordValue, ImageError> {
        let regions = match self.table.keywords().get(REGIONS_KEYWORD) {
            Some(Value::Record(rec)) => rec,
            _ => {
                return Err(ImageError::InvalidMetadata(format!(
                    "saved region not found: {name}"
                )));
            }
        };
        match regions.get(name) {
            Some(Value::Record(rec)) => Ok(rec.clone()),
            Some(_) => Err(ImageError::InvalidMetadata(format!(
                "saved region '{name}' is not a record"
            ))),
            None => Err(ImageError::InvalidMetadata(format!(
                "saved region not found: {name}"
            ))),
        }
    }

    /// Replaces a named stored image-attached region record.
    pub fn put_region_record(
        &mut self,
        name: &str,
        record: &RecordValue,
    ) -> Result<(), ImageError> {
        let mut regions = match self.table.keywords().get(REGIONS_KEYWORD) {
            Some(Value::Record(rec)) => rec.clone(),
            _ => RecordValue::default(),
        };
        regions.upsert(name, Value::Record(record.clone()));
        self.table
            .keywords_mut()
            .upsert(REGIONS_KEYWORD, Value::Record(regions));
        Ok(())
    }

    /// Removes a named stored image-attached region record.
    pub fn remove_region(&mut self, name: &str) -> Result<(), ImageError> {
        let mut regions = match self.table.keywords().get(REGIONS_KEYWORD) {
            Some(Value::Record(rec)) => rec.clone(),
            _ => {
                return Err(ImageError::InvalidMetadata(format!(
                    "saved region not found: {name}"
                )));
            }
        };
        if regions.remove(name).is_none() {
            return Err(ImageError::InvalidMetadata(format!(
                "saved region not found: {name}"
            )));
        }
        if regions.is_empty() {
            self.table.keywords_mut().remove(REGIONS_KEYWORD);
        } else {
            self.table
                .keywords_mut()
                .upsert(REGIONS_KEYWORD, Value::Record(regions));
        }
        Ok(())
    }

    /// Creates a full-image mask with the given name.
    pub fn make_mask(
        &mut self,
        name: impl Into<String>,
        set_default: bool,
        initial: bool,
    ) -> Result<(), ImageError> {
        <Self as MutableImageInterface<T>>::make_mask(self, name, set_default, initial)
    }

    /// Sets the default mask, validating that it exists.
    pub fn set_default_mask(&mut self, name: &str) -> Result<(), ImageError> {
        if !self.mask_names().iter().any(|mask_name| mask_name == name) {
            return Err(ImageError::MaskNotFound(name.to_string()));
        }
        self.table.keywords_mut().upsert(
            DEFAULT_MASK_KEYWORD,
            Value::Scalar(ScalarValue::String(name.to_string())),
        );
        Ok(())
    }

    pub fn set_coords_record_keyword(&mut self, record: RecordValue) {
        self.table
            .keywords_mut()
            .upsert("coords", Value::Record(record));
    }

    /// Unsets the default mask.
    pub fn unset_default_mask(&mut self) -> Result<(), ImageError> {
        self.table.keywords_mut().remove(DEFAULT_MASK_KEYWORD);
        Ok(())
    }

    /// Returns the default mask contents if a default mask is configured.
    pub fn get_mask(&self) -> Result<Option<ArrayD<bool>>, ImageError> {
        <Self as ImageInterface<T>>::get_mask(self)
    }

    /// Returns a slice from the default mask if one is configured.
    pub fn get_mask_slice(
        &self,
        start: &[usize],
        shape: &[usize],
        stride: &[usize],
    ) -> Result<Option<ArrayD<bool>>, ImageError> {
        <Self as ImageInterface<T>>::get_mask_slice(self, start, shape, stride)
    }

    /// The cache and two tile buffers a default-mask slice may hold, or two
    /// full copies for a legacy inline mask. Excludes the returned slice.
    ///
    /// # Errors
    /// The mask metadata is invalid or the paged mask cannot be opened.
    pub fn mask_slice_working_bytes(&self) -> Result<u64, ImageError> {
        let Some(name) = self.default_mask_name() else {
            return Ok(0);
        };
        let Some(Value::Record(masks)) = self.table.keywords().get(MASKS_KEYWORD) else {
            return Err(ImageError::MaskNotFound(name));
        };
        match masks.get(&name) {
            Some(Value::Array(ArrayValue::Bool(mask))) => Ok(2 * mask.len() as u64),
            Some(Value::Record(record)) => self.with_persistent_mask(&name, record, |mask| {
                let tile = mask.tile_shape().iter().product::<usize>() as u64;
                Ok(self.cache_bytes() as u64 + 2 * tile)
            }),
            _ => Err(ImageError::MaskNotFound(name)),
        }
    }

    /// Returns a named mask as a full-image boolean array.
    pub fn get_named_mask(&self, name: &str) -> Result<ArrayD<bool>, ImageError> {
        if let Some(mask) = self.temp_masks.get(name) {
            return Ok(mask.clone());
        }
        let masks = match self.table.keywords().get(MASKS_KEYWORD) {
            Some(Value::Record(rec)) => rec,
            _ => return Err(ImageError::MaskNotFound(name.to_string())),
        };
        let entry = masks
            .get(name)
            .ok_or_else(|| ImageError::MaskNotFound(name.to_string()))?;
        match entry {
            Value::Array(ArrayValue::Bool(array)) => {
                if array.shape() != self.shape.as_slice() {
                    return Err(ImageError::ShapeMismatch {
                        expected: self.shape.clone(),
                        got: array.shape().to_vec(),
                    });
                }
                Ok(array.clone())
            }
            Value::Record(record) => self.with_persistent_mask(name, record, |mask| {
                let array = mask.get()?;
                if array.shape() != self.shape.as_slice() {
                    return Err(ImageError::ShapeMismatch {
                        expected: self.shape.clone(),
                        got: array.shape().to_vec(),
                    });
                }
                Ok(array)
            }),
            _ => Err(ImageError::InvalidMetadata(
                "mask keyword is neither a bool array nor a paged-mask record".to_string(),
            )),
        }
    }

    /// Returns a slice from a named mask without materializing the full mask
    /// when the backing storage supports paged reads.
    pub fn get_named_mask_slice(
        &self,
        name: &str,
        start: &[usize],
        shape: &[usize],
        stride: &[usize],
    ) -> Result<ArrayD<bool>, ImageError> {
        if let Some(mask) = self.temp_masks.get(name) {
            return slice_mask_array(mask, start, shape, stride);
        }
        let masks = match self.table.keywords().get(MASKS_KEYWORD) {
            Some(Value::Record(rec)) => rec,
            _ => return Err(ImageError::MaskNotFound(name.to_string())),
        };
        let entry = masks
            .get(name)
            .ok_or_else(|| ImageError::MaskNotFound(name.to_string()))?;
        match entry {
            Value::Array(ArrayValue::Bool(array)) => {
                if array.shape() != self.shape.as_slice() {
                    return Err(ImageError::ShapeMismatch {
                        expected: self.shape.clone(),
                        got: array.shape().to_vec(),
                    });
                }
                slice_mask_array(array, start, shape, stride)
            }
            Value::Record(record) => self.with_persistent_mask(name, record, |mask| {
                mask.get_slice(start, shape, stride).map_err(Into::into)
            }),
            _ => Err(ImageError::InvalidMetadata(
                "mask keyword is neither a bool array nor a paged-mask record".to_string(),
            )),
        }
    }

    /// Replaces a named mask.
    pub fn put_mask(&mut self, name: &str, data: &ArrayD<bool>) -> Result<(), ImageError> {
        if data.shape() != self.shape.as_slice() {
            return Err(ImageError::ShapeMismatch {
                expected: self.shape.clone(),
                got: data.shape().to_vec(),
            });
        }
        let record = make_paged_mask_record(&subtable_reference(name), &self.shape);
        let mut masks = match self.table.keywords().get(MASKS_KEYWORD) {
            Some(Value::Record(rec)) => rec.clone(),
            _ => RecordValue::default(),
        };
        masks.upsert(name, Value::Record(record));
        self.table
            .keywords_mut()
            .upsert(MASKS_KEYWORD, Value::Record(masks));

        if let Some(path) = &self.path {
            Self::write_mask_table(path, name, data)?;
        } else {
            self.temp_masks.insert(name.to_string(), data.clone());
        }
        self.persistent_masks.borrow_mut().remove(name);
        Ok(())
    }

    pub(crate) fn register_paged_mask(&mut self, name: &str) {
        let record = make_paged_mask_record(&subtable_reference(name), &self.shape);
        let mut masks = match self.table.keywords().get(MASKS_KEYWORD) {
            Some(Value::Record(rec)) => rec.clone(),
            _ => RecordValue::default(),
        };
        masks.upsert(name, Value::Record(record));
        self.table
            .keywords_mut()
            .upsert(MASKS_KEYWORD, Value::Record(masks));
        self.persistent_masks.borrow_mut().remove(name);
    }

    /// Writes a rectangular slice into a named mask.
    ///
    /// If the mask does not exist yet, it is created with all pixels initialized
    /// to false before writing the supplied slice.
    pub fn put_mask_slice(
        &mut self,
        name: &str,
        data: &ArrayD<bool>,
        start: &[usize],
    ) -> Result<(), ImageError> {
        self.check_writable()?;
        validate_mask_slice_write(&self.shape, data.shape(), start)?;
        let entry = match self.table.keywords_mut().get_mut(MASKS_KEYWORD) {
            Some(Value::Record(masks)) => masks.get_mut(name),
            _ => None,
        };
        let relative_path = match entry {
            Some(Value::Array(ArrayValue::Bool(mask))) => {
                if mask.shape() != self.shape.as_slice() {
                    return Err(ImageError::ShapeMismatch {
                        expected: self.shape.clone(),
                        got: mask.shape().to_vec(),
                    });
                }
                return assign_mask_slice(mask, data, start);
            }
            Some(Value::Record(record)) => Some(Self::persistent_mask_relative_path(record)?),
            Some(_) => {
                return Err(ImageError::InvalidMetadata(
                    "mask keyword is neither a bool array nor a paged-mask record".to_string(),
                ));
            }
            None => None,
        };

        if let Some(path) = &self.path {
            let mask_path = relative_path.as_ref().map_or_else(
                || path.join(name),
                |reference| casa_tables::add_directory(reference, path),
            );
            let mut mask = if relative_path.is_some() || mask_path.exists() {
                PagedArray::<bool>::open_with_cache(mask_path, self.cache_bytes())?
            } else {
                PagedArray::<bool>::create_with_cache(
                    TiledShape::with_tile_shape(self.shape.clone(), self.tile_shape.clone())?,
                    mask_path,
                    self.cache_bytes(),
                )?
            };
            mask.put_slice(data, start)?;
            mask.flush()?;
        } else {
            let mask = self
                .temp_masks
                .entry(name.to_string())
                .or_insert_with(|| ArrayD::from_elem(IxDyn(&self.shape), false));
            assign_mask_slice(mask, data, start)?;
        }
        if relative_path.is_none() {
            self.register_paged_mask(name);
        } else {
            // Multiple region names can reference the same on-disk table.
            self.persistent_masks.borrow_mut().clear();
        }
        Ok(())
    }

    /// Removes a named mask and clears the default mask if it pointed at it.
    pub fn remove_mask(&mut self, name: &str) -> Result<(), ImageError> {
        let mut masks = match self.table.keywords().get(MASKS_KEYWORD) {
            Some(Value::Record(rec)) => rec.clone(),
            _ => return Err(ImageError::MaskNotFound(name.to_string())),
        };
        if masks.remove(name).is_none() {
            return Err(ImageError::MaskNotFound(name.to_string()));
        }
        self.table
            .keywords_mut()
            .upsert(MASKS_KEYWORD, Value::Record(masks));
        self.temp_masks.remove(name);
        self.persistent_masks.borrow_mut().remove(name);
        if let Some(path) = &self.path {
            let mask_path = path.join(name);
            if mask_path.exists() {
                std::fs::remove_dir_all(mask_path).map_err(|e| ImageError::Io(e.to_string()))?;
            }
        }
        if self.default_mask_name().as_deref() == Some(name) {
            self.table.keywords_mut().remove(DEFAULT_MASK_KEYWORD);
        }
        Ok(())
    }

    /// Renames a mask while preserving its contents, order, and default status.
    ///
    /// # Errors
    /// Returns an error for invalid names, a missing source, an occupied
    /// destination, or an unsuccessful subtable rename.
    pub fn rename_mask(&mut self, from: &str, to: &str) -> Result<(), ImageError> {
        for name in [from, to] {
            if Path::new(name).file_name().and_then(|name| name.to_str()) != Some(name) {
                return Err(ImageError::InvalidMetadata(format!(
                    "Invalid mask name {name}"
                )));
            }
        }
        let Some(Value::Record(mut masks)) = self.table.keywords().get(MASKS_KEYWORD).cloned()
        else {
            return Err(ImageError::MaskNotFound(from.into()));
        };
        if masks.get(to).is_some() {
            return Err(ImageError::InvalidMetadata(format!(
                "Mask {to} already exists"
            )));
        }
        if !masks.rename_field(from, to) {
            return Err(ImageError::MaskNotFound(from.into()));
        }
        if let Some(path) = &self.path {
            let source = path.join(from);
            if source.exists() {
                casa_tables::publication::rename_without_replacing(&source, &path.join(to))
                    .map_err(|error| ImageError::Io(error.to_string()))?;
            }
            if let Some(Value::Record(region)) = masks.get_mut(to)
                && matches!(
                    region.get("mask"),
                    Some(Value::TableRef(_) | Value::Scalar(ScalarValue::String(_)))
                )
            {
                region.upsert("mask", Value::TableRef(subtable_reference(to)));
            }
        }
        if let Some(mask) = self.temp_masks.remove(from) {
            self.temp_masks.insert(to.into(), mask);
        }
        self.persistent_masks.borrow_mut().remove(from);
        self.table
            .keywords_mut()
            .upsert(MASKS_KEYWORD, Value::Record(masks));
        if self.default_mask_name().as_deref() == Some(from) {
            self.set_default_mask(to)?;
        }
        Ok(())
    }
}

impl<T: ImagePixel> PagedImage<T> {
    pub(super) fn write_all_masks(&mut self, path: &Path) -> Result<(), ImageError> {
        let pending = std::mem::take(&mut self.temp_masks);
        for (name, mask) in pending {
            Self::write_mask_table(path, &name, &mask)?;
        }
        Ok(())
    }

    /// Writes mask `name` as the image's `<name>` subtable.
    pub(super) fn write_mask_table(
        image_path: &Path,
        name: &str,
        data: &ArrayD<bool>,
    ) -> Result<(), ImageError> {
        let mask_path = image_path.join(name);
        if mask_path.exists() {
            std::fs::remove_dir_all(&mask_path).map_err(|e| ImageError::Io(e.to_string()))?;
        }
        let mut mask =
            PagedArray::<bool>::create(TiledShape::new(data.shape().to_vec(), 1)?, &mask_path)
                .map_err(ImageError::from)?;
        mask.put_slice(data, &vec![0; data.ndim()])?;
        mask.flush().map_err(ImageError::from)
    }
}

// SPDX-License-Identifier: LGPL-3.0-or-later

use super::*;

impl<T: ImagePixel> PagedImage<T> {
    /// Returns the coordinate system.
    pub fn coordinates(&self) -> &CoordinateSystem {
        &self.coords
    }

    /// Return the first spectral coordinate in the stored creation order.
    ///
    /// This order can differ from pixel-axis order after transposition.
    ///
    /// # Errors
    ///
    /// Returns an error if the coordinate metadata is malformed.
    pub fn first_spectral_coordinate(
        &self,
    ) -> Result<Option<casa_coordinates::SpectralCoordinate>, ImageError> {
        let Some(Value::Record(record)) = self.table.keywords().get("coords") else {
            return Err(ImageError::InvalidMetadata(
                "image has no coordinate record".into(),
            ));
        };
        Ok(CoordinateSystem::first_spectral_in_record(record)?.map(|(_, coordinate)| coordinate))
    }

    /// Pixel axis of the first spectral coordinate in stored creation order.
    ///
    /// # Errors
    ///
    /// Returns an error if the coordinate metadata is malformed.
    pub fn first_spectral_pixel_axis(&self) -> Result<Option<usize>, ImageError> {
        let Some(Value::Record(record)) = self.table.keywords().get("coords") else {
            return Err(ImageError::InvalidMetadata(
                "image has no coordinate record".into(),
            ));
        };
        let Some((field, _)) = CoordinateSystem::first_spectral_in_record(record)? else {
            return Ok(None);
        };
        Ok(self
            .coords
            .coordinates_in_record(record)?
            .into_iter()
            .find(|coordinate| coordinate.field == field)
            .and_then(|coordinate| coordinate.pixel_axes.into_iter().next().flatten()))
    }

    /// Replaces the coordinate system and updates the table keyword.
    pub fn set_coordinates(&mut self, coords: CoordinateSystem) -> Result<(), ImageError> {
        self.table
            .keywords_mut()
            .upsert("coords", Value::Record(coords.to_record()));
        self.coords = coords;
        Ok(())
    }

    /// Replaces the coordinate system using its original casacore record.
    ///
    /// The record retains coordinate numbering and metadata that the
    /// computational coordinate model does not store.
    ///
    /// # Errors
    /// Returns an error before changing the image if the record is invalid
    /// or the number of pixel axes differs from the image's dimensionality.
    pub fn set_coordinate_record(&mut self, mut record: RecordValue) -> Result<(), ImageError> {
        let coords = CoordinateSystem::from_record(&record)?;
        if coords.n_pixel_axes() != self.ndim() {
            return Err(ImageError::InvalidMetadata(
                "Supplied CoordinateSystem and image shape are inconsistent".into(),
            ));
        }
        coords.normalize_quality_records(&mut record)?;
        self.table
            .keywords_mut()
            .upsert("coords", Value::Record(record));
        self.coords = coords;
        Ok(())
    }

    /// Returns the brightness unit string.
    pub fn units(&self) -> &str {
        &self.units
    }

    /// Replaces the brightness unit string.
    pub fn set_units(&mut self, units: impl Into<String>) -> Result<(), ImageError> {
        self.check_writable()?;
        let units = units.into();
        self.table
            .keywords_mut()
            .upsert("units", Value::Scalar(ScalarValue::String(units.clone())));
        self.units = units;
        Ok(())
    }

    /// Reads the image info record.
    pub fn image_info(&self) -> Result<ImageInfo, ImageError> {
        match self.table.keywords().get("imageinfo") {
            Some(Value::Record(rec)) => ImageInfo::from_record(rec),
            None => Ok(ImageInfo::default()),
            Some(_) => Err(ImageError::InvalidMetadata(
                "imageinfo is not a record".into(),
            )),
        }
    }

    /// Return a single restoring beam with its stored quantity units.
    ///
    /// # Errors
    ///
    /// Returns an error for malformed metadata or a multi-beam image.
    pub fn stored_restoring_beam(&self) -> Result<Option<crate::StoredGaussianBeam>, ImageError> {
        let info = self.image_info()?;
        if info.beam_set.is_multi() {
            return Err(ImageError::InvalidMetadata(
                "image has multiple restoring beams".into(),
            ));
        }
        if info.beam_set.is_empty() {
            return Ok(None);
        }
        let record = self
            .restoring_beam_record()
            .ok_or_else(|| ImageError::InvalidMetadata("missing restoring beam record".into()))?;
        Ok(Some(crate::StoredGaussianBeam::from_record(record)?))
    }

    /// Replace the restoring beam while retaining unrelated image metadata.
    /// Call [`Self::save`] to persist the change.
    ///
    /// # Errors
    ///
    /// Returns an error for a null beam or a beam the image reader cannot restore.
    pub fn set_stored_restoring_beam(
        &mut self,
        beam: &crate::StoredGaussianBeam,
    ) -> Result<(), ImageError> {
        self.image_info()?;
        if beam.is_null() {
            return Err(ImageError::InvalidMetadata(
                "Beam is null and therefore invalid".into(),
            ));
        }
        let record = beam.to_record();
        crate::GaussianBeam::from_record(&record)?;
        let mut info = match self.table.keywords().get("imageinfo") {
            Some(Value::Record(record)) => record.clone(),
            _ => RecordValue::default(),
        };
        info.remove("perplanebeams");
        info.upsert("restoringbeam", Value::Record(record));
        self.table
            .keywords_mut()
            .upsert("imageinfo", Value::Record(info));
        Ok(())
    }

    /// Remove all restoring beams while preserving unrelated image metadata.
    /// Returns whether the image had a beam. Call [`Self::save`] to persist it.
    ///
    /// # Errors
    ///
    /// Returns an error if the existing image-info record is malformed.
    pub fn remove_restoring_beams(&mut self) -> Result<bool, ImageError> {
        if self.image_info()?.beam_set.is_empty() {
            return Ok(false);
        }
        let Some(Value::Record(mut info)) = self.table.keywords().get("imageinfo").cloned() else {
            return Err(ImageError::InvalidMetadata(
                "missing image-info record".into(),
            ));
        };
        info.remove("restoringbeam");
        info.remove("perplanebeams");
        self.table
            .keywords_mut()
            .upsert("imageinfo", Value::Record(info));
        Ok(true)
    }

    /// Reports whether the raw image-info record explicitly contains a non-empty image-type string.
    pub fn has_image_type_record(&self) -> bool {
        matches!(self.table.keywords().get("imageinfo"), Some(Value::Record(record)) if matches!(record.get("imagetype"), Some(Value::Scalar(ScalarValue::String(value))) if !value.is_empty()))
    }

    /// The original single-beam record, including the units used on disk.
    pub fn restoring_beam_record(&self) -> Option<&RecordValue> {
        let Value::Record(info) = self.table.keywords().get("imageinfo")? else {
            return None;
        };
        ImageInfo::first_beam_record(info)
    }

    /// Borrows the original CASA per-plane beam record, preserving its channel, polarization, and
    /// unit metadata.
    pub fn per_plane_beam_record(&self) -> Option<&RecordValue> {
        let Value::Record(info) = self.table.keywords().get("imageinfo")? else {
            return None;
        };
        match info.get("perplanebeams") {
            Some(Value::Record(beams)) => Some(beams),
            _ => None,
        }
    }

    /// Update one image-info string without rewriting unrelated beam records.
    pub fn set_image_info_string(&mut self, key: &str, value: &str) {
        let mut info = match self.table.keywords().get("imageinfo") {
            Some(Value::Record(record)) => record.clone(),
            _ => RecordValue::default(),
        };
        info.upsert(key, Value::Scalar(ScalarValue::String(value.into())));
        self.table
            .keywords_mut()
            .upsert("imageinfo", Value::Record(info));
    }

    /// Update observation information while preserving the stored coordinate
    /// records and their pixel/world axis maps.
    pub fn set_observation_string(&mut self, key: &str, value: &str) -> Result<(), ImageError> {
        let Some(Value::Record(mut coords)) = self.table.keywords().get("coords").cloned() else {
            return Err(ImageError::InvalidMetadata(
                "image has no coordinate record".into(),
            ));
        };
        if let Some(Value::Record(obs)) = coords.get_mut("obsinfo") {
            obs.upsert(key, Value::Scalar(ScalarValue::String(value.into())));
        } else {
            coords.upsert(key, Value::Scalar(ScalarValue::String(value.into())));
        }
        self.table
            .keywords_mut()
            .upsert("coords", Value::Record(coords));
        match key {
            "observer" => self.coords.obs_info_mut().observer = value.into(),
            "telescope" => self.coords.obs_info_mut().telescope = value.into(),
            _ => unreachable!("only observation strings are supported"),
        }
        Ok(())
    }

    /// Set the observation epoch while preserving the stored coordinate maps
    /// and other observation metadata.
    ///
    /// # Errors
    ///
    /// Returns an error if the image has no coordinate record.
    pub fn set_observation_date(
        &mut self,
        epoch: casa_core::measures::MEpoch,
    ) -> Result<(), ImageError> {
        let Some(Value::Record(mut coords)) = self.table.keywords().get("coords").cloned() else {
            return Err(ImageError::InvalidMetadata(
                "image has no coordinate record".into(),
            ));
        };
        let obs = match coords.get_mut("obsinfo") {
            Some(Value::Record(obs)) => obs,
            _ => &mut coords,
        };
        // The legacy scalar date is always read as UTC and takes precedence
        // over obsdate. Remove it so other epoch references survive a reopen.
        while obs.remove("date").is_some() {}
        obs.upsert(
            "obsdate",
            Value::Record(casa_core::measures::epoch_to_record(&epoch)),
        );
        self.table
            .keywords_mut()
            .upsert("coords", Value::Record(coords));
        self.coords.obs_info_mut().date = Some(epoch);
        Ok(())
    }

    /// Update a zero-based reference pixel while retaining the stored axis maps.
    ///
    /// Returns its previous value. Call [`Self::save`] to persist the change.
    /// The image metadata is unchanged if validation fails.
    ///
    /// # Errors
    ///
    /// A polarization-axis update is a no-op, as in casacore. Returns an error
    /// for an invalid axis, a non-finite value, or an unsupported or malformed
    /// coordinate record.
    pub fn set_reference_pixel(
        &mut self,
        pixel_axis: usize,
        value: f64,
    ) -> Result<f64, ImageError> {
        self.edit_coordinates(|record| {
            CoordinateSystem::set_reference_pixel_in_record(record, pixel_axis, value)
        })
    }

    /// Set an axis increment in its stored world unit and return the old value.
    /// Call [`Self::save`] to persist it.
    ///
    /// # Errors
    ///
    /// Returns an error if the axis, value, or coordinate record is invalid.
    pub fn set_increment(&mut self, pixel_axis: usize, value: f64) -> Result<f64, ImageError> {
        self.edit_coordinates(|record| {
            CoordinateSystem::set_increment_in_record(record, pixel_axis, value)
        })
    }

    /// Set an axis reference value in its stored world unit and return the old
    /// value. Call [`Self::save`] to persist it.
    ///
    /// # Errors
    ///
    /// Returns an error if the axis, value, or coordinate record is invalid.
    pub fn set_reference_value(
        &mut self,
        pixel_axis: usize,
        value: f64,
    ) -> Result<f64, ImageError> {
        self.edit_coordinates(|record| {
            CoordinateSystem::set_reference_value_in_record(record, pixel_axis, value)
        })
    }

    /// Replace the Stokes types on a pixel axis, preserving coordinate maps.
    /// Call [`Self::save`] to persist the change.
    ///
    /// # Errors
    ///
    /// Returns an error for invalid Stokes values, axes, or coordinates.
    pub fn set_stokes_types(
        &mut self,
        pixel_axis: usize,
        names: &[String],
    ) -> Result<Vec<String>, ImageError> {
        self.edit_coordinates(|record| {
            CoordinateSystem::set_stokes_types_in_record(record, pixel_axis, names)
        })
    }

    /// Set the active rest frequency in the spectral coordinate's stored unit.
    ///
    /// The change is persisted when [`Self::save`] is called.
    ///
    /// # Errors
    ///
    /// Returns an error if the value or spectral metadata is invalid.
    pub fn set_rest_frequency(&mut self, value: f64) -> Result<f64, ImageError> {
        if !value.is_finite() || value <= 0.0 {
            return Err(casa_coordinates::CoordinateError::InvalidRecord(
                "rest frequency must be positive and finite".into(),
            )
            .into());
        }
        self.edit_coordinates(|record| {
            CoordinateSystem::set_rest_frequency_in_record(record, value)
        })
    }

    /// Set the native spectral reference frame and return the old frame if
    /// it changed. Call [`Self::save`] to persist the change.
    ///
    /// # Errors
    ///
    /// Returns an error for an unknown frame or invalid spectral metadata.
    pub fn set_spectral_reference(
        &mut self,
        reference: &str,
    ) -> Result<Option<String>, ImageError> {
        self.edit_coordinates(|record| {
            CoordinateSystem::set_spectral_reference_in_record(record, reference)
        })
    }

    /// Change the first direction projection while preserving coordinate maps.
    /// Call [`Self::save`] to persist a change.
    ///
    /// # Errors
    ///
    /// Returns an error for an unknown or unsupported projection, or invalid
    /// direction metadata.
    pub fn set_direction_projection(
        &mut self,
        projection: &str,
    ) -> Result<Option<String>, ImageError> {
        self.edit_coordinates(|record| {
            CoordinateSystem::set_direction_projection_in_record(record, projection)
        })
    }

    /// Set the native direction frame, preserving its conversion frame and maps.
    /// Call [`Self::save`] to persist a change.
    ///
    /// # Errors
    ///
    /// Returns an error for an unknown or unsupported frame or coordinate.
    pub fn set_direction_reference(
        &mut self,
        reference: &str,
    ) -> Result<Option<String>, ImageError> {
        self.edit_coordinates(|record| {
            CoordinateSystem::set_direction_reference_in_record(record, reference)
        })
    }

    /// Set an axis name, preserving the stored coordinate mappings.
    ///
    /// The change is persisted when [`Self::save`] is called.
    ///
    /// # Errors
    ///
    /// Returns an error if the axis or coordinate record is invalid.
    pub fn set_axis_name(&mut self, pixel_axis: usize, name: &str) -> Result<String, ImageError> {
        self.edit_coordinates(|record| {
            CoordinateSystem::set_axis_name_in_record(record, pixel_axis, name)
        })
    }

    /// Set an axis world unit, rescaling its coordinate values, and return its
    /// previous unit. Call [`Self::save`] to persist the change.
    ///
    /// # Errors
    ///
    /// Returns an error if the units, axis, or coordinate metadata are invalid.
    pub fn set_axis_unit(&mut self, pixel_axis: usize, unit: &str) -> Result<String, ImageError> {
        self.edit_coordinates(|record| {
            CoordinateSystem::set_axis_unit_in_record(record, pixel_axis, unit)
        })
    }

    /// Edits a copy of the coordinate keyword and publishes the revised record and typed transform
    /// together. A missing coordinate record or failed edit leaves the image unchanged.
    pub fn edit_coordinates<R>(
        &mut self,
        edit: impl FnOnce(
            &mut RecordValue,
        ) -> Result<(CoordinateSystem, R), casa_coordinates::CoordinateError>,
    ) -> Result<R, ImageError> {
        let Some(Value::Record(mut record)) = self.table.keywords().get("coords").cloned() else {
            return Err(ImageError::InvalidMetadata(
                "image has no coordinate record".into(),
            ));
        };
        let (coordinates, old) = edit(&mut record)?;
        self.table
            .keywords_mut()
            .upsert("coords", Value::Record(record));
        self.coords = coordinates;
        Ok(old)
    }

    /// Replaces the image info record.
    pub fn set_image_info(&mut self, info: &ImageInfo) -> Result<(), ImageError> {
        self.check_writable()?;
        self.table
            .keywords_mut()
            .upsert("imageinfo", Value::Record(info.to_record()));
        Ok(())
    }

    /// Replaces the image info using its original casacore record.
    ///
    /// Use this to retain individual restoring-beam quantity units and other
    /// fields that the numerical image-info model does not store.
    ///
    /// # Errors
    /// Returns an error before changing the image if the record is invalid.
    pub fn set_image_info_record(&mut self, record: RecordValue) -> Result<(), ImageError> {
        self.check_writable()?;
        ImageInfo::from_record(&record)?;
        self.table
            .keywords_mut()
            .upsert("imageinfo", Value::Record(record));
        Ok(())
    }

    /// Store image info while preserving the units and values of its beams.
    pub fn set_image_info_with_beam_records(
        &mut self,
        info: &ImageInfo,
        beams: &RecordValue,
    ) -> Result<(), ImageError> {
        let mut record = info.to_record();
        if info.beam_set.is_multi() {
            let mut stored = info.beam_set.to_record();
            for index in 0..info.beam_set.nelements() {
                let name = format!("*{index}");
                let beam = beams
                    .get(&name)
                    .ok_or_else(|| ImageError::InvalidMetadata(format!("missing beam {name}")))?;
                stored.upsert(name, beam.clone());
            }
            record.upsert("perplanebeams", Value::Record(stored));
        } else if info.beam_set.is_single() {
            let beam = beams
                .get("*0")
                .ok_or_else(|| ImageError::InvalidMetadata("missing beam *0".into()))?;
            record.upsert("restoringbeam", beam.clone());
        }
        self.set_image_info_record(record)
    }

    /// Returns the misc-info record.
    pub fn misc_info(&self) -> RecordValue {
        self.misc_info.clone()
    }

    /// Replaces the misc-info record.
    pub fn set_misc_info(&mut self, rec: RecordValue) -> Result<(), ImageError> {
        self.check_writable()?;
        self.table
            .keywords_mut()
            .upsert("miscinfo", Value::Record(rec.clone()));
        self.misc_info = rec;
        Ok(())
    }
}

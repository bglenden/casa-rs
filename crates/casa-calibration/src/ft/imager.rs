// SPDX-License-Identifier: LGPL-3.0-or-later
//! Prepare a legacy imager model using the object's effective image and FT settings.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use casa_components::ComponentList;
use casa_coordinates::{CoordinateModel, CoordinateSystem, CoordinateType};
use casa_images::{ImageInfo, PagedImage};
use casa_ms::schema::main_table::VisibilityDataColumn::ModelData;
use casa_ms::{
    MeasurementSet, MsResult, MsSelection, ResolvedMsSelection, VirtualModelEntry, VirtualModelKind,
};
use casa_types::measures::position::MPosition;
use casa_types::{RecordValue, ScalarValue, Value};

use crate::model_columns::has_column;
use crate::{AttachmentError, CalibrationAttachment, ImagerAttachments};

use super::image::{ImageModel, TaylorTerm, prepare_configured_image, reference_frequency};
use super::imager_definition::ImagerDefinitionRequest;
use super::settings::{ImagerGridder, ImagerTransformOptions, PredictionGeometry};
use super::virtual_publication::VirtualFtPublication;
use super::voltage_pattern::ImagerVoltagePattern;
use super::{invalid, model_entry, prepare_mosaic_models, write_model_scratch};

/// Geometry installed by the imager before creating a model image.
pub struct ImagerImageDefinition {
    /// Direction, Stokes and spectral lengths in CASA axis order.
    pub shape: [usize; 4],
    /// The coordinates of a model image created by this imager.
    pub coordinates: CoordinateSystem,
    /// Field whose name is used for a new image's object metadata.
    pub field_id: usize,
}

/// Inputs to one imager prediction.
#[derive(Clone, Copy)]
pub struct ImagerPredictionModel<'a> {
    /// Images in Taylor-term order.
    pub images: &'a [PathBuf],
    /// Optional component-list table.
    pub components: Option<&'a Path>,
    /// Add the prediction to the stored model.
    pub incremental: bool,
    /// Epoch at which the field direction is held, in MS time seconds.
    pub phase_centre_time: Option<f64>,
}

/// Image and transform settings retained across predictions by the imager.
pub struct ImagerPrediction {
    definition: Option<ImageDefinition>,
    frequency_frame_valid: bool,
    options: ImagerTransformOptions,
    taylor: (i32, Option<f64>),
    voltage_pattern: Option<ImagerVoltagePattern>,
    pending_publication: Option<PendingPublication>,
    partial_batch: Option<PartialBatch>,
    ambiguous_scratch_write: bool,
}

enum ImageDefinition {
    Coordinates(ImagerImageDefinition),
    Requested(ImagerDefinitionRequest),
}

struct PendingPublication {
    path: PathBuf,
    selection: MsSelection,
    model: PredictionInputs,
    publication: VirtualFtPublication,
}

struct PartialBatch {
    inputs: Vec<CalibrationAttachment>,
    model: PredictionInputs,
    scratch: bool,
    completed: usize,
}

struct PredictionInputs {
    images: Vec<PathBuf>,
    components: Option<PathBuf>,
    incremental: bool,
    phase_centre_time: Option<f64>,
}

impl PredictionInputs {
    fn new(model: ImagerPredictionModel<'_>) -> Self {
        Self {
            images: model.images.to_owned(),
            components: model.components.map(Path::to_owned),
            incremental: model.incremental,
            phase_centre_time: model.phase_centre_time,
        }
    }

    fn matches(&self, model: ImagerPredictionModel<'_>) -> bool {
        self.images == model.images
            && self.components.as_deref() == model.components
            && self.incremental == model.incremental
            && self.phase_centre_time.map(f64::to_bits) == model.phase_centre_time.map(f64::to_bits)
    }
}

impl Default for ImagerPrediction {
    fn default() -> Self {
        Self {
            definition: None,
            frequency_frame_valid: false,
            options: ImagerTransformOptions::default(),
            taylor: (1, None),
            voltage_pattern: None,
            pending_publication: None,
            partial_batch: None,
            ambiguous_scratch_write: false,
        }
    }
}

impl ImagerPrediction {
    /// Set the image definition used to create an absent model image.
    pub fn define_image(&mut self, definition: ImagerImageDefinition) {
        self.definition = Some(ImageDefinition::Coordinates(definition));
    }

    /// Set the image request. Coordinates are resolved against the selected
    /// MeasurementSet only if the next model image needs to be created.
    ///
    /// # Errors
    /// A previous prediction has an unfinished publication.
    pub fn define_model_geometry(&mut self, request: ImagerDefinitionRequest) -> MsResult<()> {
        if self.partial_batch.is_some() || self.pending_publication.is_some() {
            return Err(invalid(
                "finish publishing the previous imager prediction before changing image geometry",
            ));
        }
        self.definition = Some(ImageDefinition::Requested(request));
        Ok(())
    }

    /// Whether an image has been defined on this object.
    pub fn has_image_definition(&self) -> bool {
        self.definition.is_some()
    }

    /// Replace the options used by the next Fourier transform.
    pub fn set_options(&mut self, options: ImagerTransformOptions) {
        self.options = options;
    }

    /// The current settings, including prior buffer sizes retained by setoptions.
    pub fn transform_options(&self) -> ImagerTransformOptions {
        self.options
    }

    /// Replace Fourier settings for future predictions.
    ///
    /// # Errors
    /// A previous prediction still has an unfinished publication.
    pub fn configure_transform(&mut self, options: ImagerTransformOptions) -> MsResult<()> {
        if self.partial_batch.is_some() || self.pending_publication.is_some() {
            return Err(invalid(
                "finish publishing the previous imager prediction before changing Fourier settings",
            ));
        }
        self.set_options(options);
        Ok(())
    }

    /// Enable a configured voltage pattern, or disable it with `None`.
    pub fn set_voltage_pattern(&mut self, pattern: Option<ImagerVoltagePattern>) {
        self.voltage_pattern = pattern;
    }

    /// Update the voltage pattern for future predictions.
    ///
    /// # Errors
    /// A previous prediction still has an unfinished publication.
    pub fn configure_voltage_pattern(
        &mut self,
        pattern: Option<ImagerVoltagePattern>,
    ) -> MsResult<()> {
        if self.partial_batch.is_some() || self.pending_publication.is_some() {
            return Err(invalid(
                "finish publishing the previous imager prediction before changing the voltage pattern",
            ));
        }
        self.set_voltage_pattern(pattern);
        Ok(())
    }

    /// Set the number of Taylor terms and their frequency. With no explicit
    /// frequency the first model image supplies its reference frequency.
    ///
    /// # Errors
    ///
    /// A nonpositive term count or an invalid frequency.
    pub fn set_taylor_terms(&mut self, terms: i32, reference_hz: Option<f64>) -> MsResult<()> {
        if terms < 1 || reference_hz.is_some_and(|hz| !hz.is_finite() || hz <= 0.) {
            return Err(invalid(
                "imager needs at least one Taylor term and a positive reference frequency",
            ));
        }
        if (self.partial_batch.is_some() || self.pending_publication.is_some())
            && self.taylor != (terms, reference_hz)
        {
            return Err(invalid(
                "finish publishing the previous imager prediction before changing Taylor terms",
            ));
        }
        self.taylor = (terms, reference_hz);
        Ok(())
    }

    /// Predict for every active writable input in enrollment order.
    /// A retry continues after the last completed input and resumes any
    /// interrupted publication on the current one.
    ///
    /// # Errors
    /// The active inputs are not writable, a previous batch requires a retry,
    /// or an input's model cannot be prepared or published.
    pub fn predict_attached(
        &mut self,
        attachments: &ImagerAttachments,
        model: ImagerPredictionModel<'_>,
        scratch: bool,
    ) -> Result<bool, AttachmentError> {
        let inputs = attachments.writable_inputs()?;
        let first = match &self.partial_batch {
            Some(batch)
                if batch.inputs == inputs
                    && batch.scratch == scratch
                    && batch.model.matches(model) =>
            {
                batch.completed
            }
            Some(_) => {
                return Err(invalid(
                    "retry the previous imager prediction to finish publishing it",
                )
                .into());
            }
            None => 0,
        };
        if scratch {
            // CASA uses the last enrollment's destination for the whole
            // iterator. Reject a missing column before any group member is
            // changed. The legacy iterator can crash on this input.
            for input in inputs
                .iter()
                .skip(first)
                .filter(|input| !input.options().use_scratch)
            {
                if !has_column(&MeasurementSet::open(input.path())?, ModelData) {
                    return Ok(false);
                }
            }
        }
        for (index, input) in inputs.iter().enumerate().skip(first) {
            let result = self.predict_to(input, model, scratch);
            if !matches!(result, Ok(true)) {
                if index > 0 {
                    self.partial_batch = Some(PartialBatch {
                        inputs: inputs.to_vec(),
                        model: PredictionInputs::new(model),
                        scratch,
                        completed: index,
                    });
                }
                return result;
            }
        }
        self.partial_batch = None;
        Ok(true)
    }

    /// Predict and publish the model for one already-selected attachment.
    ///
    /// Returns false when an absent image needs an image definition.
    /// Repeat the same request after an interrupted virtual publication to
    /// finish cleanup using the already-prepared model.
    ///
    /// # Errors
    ///
    /// An attachment, model preparation, Fourier transform or table write
    /// fails.
    pub fn predict(
        &mut self,
        attachment: &CalibrationAttachment,
        model: ImagerPredictionModel<'_>,
    ) -> Result<bool, AttachmentError> {
        self.predict_to(attachment, model, attachment.options().use_scratch)
    }

    fn predict_to(
        &mut self,
        attachment: &CalibrationAttachment,
        model: ImagerPredictionModel<'_>,
        scratch: bool,
    ) -> Result<bool, AttachmentError> {
        if self.ambiguous_scratch_write {
            return Err(invalid(
                "the previous scratch prediction may be partially written. Reopen the imager before a replacement prediction",
            ).into());
        }
        let mut pending = self.pending_publication.take();
        let mut writing_scratch = false;
        let result = attachment.with_write(|ms, selection, lock| {
            if let Some(previous) = pending.as_mut() {
                if scratch
                    || previous.path != attachment.path()
                    || previous.selection != selection.request
                    || !previous.model.matches(model)
                {
                    return Err(invalid(
                        "retry the previous imager prediction to finish publishing it",
                    )
                    .into());
                }
                previous.publication.publish([(ms, lock)].into_iter())?;
                return Ok(true);
            }
            if selection.selected_rows.is_empty()
                || (model.images.is_empty() && model.components.is_none())
            {
                return Ok(true);
            }
            if scratch && !has_column(ms, ModelData) {
                return Ok(false);
            }
            let Some((mut components, images)) = self
                .prepare(ms, selection, &model, scratch)
                .map_err(AttachmentError::PredictionPreparation)?
            else {
                return Ok(false);
            };
            if scratch {
                components.extend(images.iter().map(|image| image.entry.clone()));
                let prepared = if !images.is_empty()
                    && matches!(self.options.gridder, ImagerGridder::Mosaic)
                {
                    Some(
                        prepare_mosaic_models(ms, selection, &components, true)
                            .map_err(AttachmentError::PredictionPreparation)?,
                    )
                } else {
                    None
                };
                // Virtual cleanup belongs to scratch attachment. An earlier
                // virtual input in this iterator can still own recipes.
                writing_scratch = !components.is_empty();
                write_model_scratch(
                    ms,
                    selection,
                    &components,
                    model.incremental,
                    false,
                    prepared,
                    lock,
                )?;
            } else {
                if !images.is_empty() && matches!(self.options.gridder, ImagerGridder::Mosaic) {
                    let mut all_entries = components.clone();
                    all_entries.extend(images.iter().map(|image| image.entry.clone()));
                    prepare_mosaic_models(ms, selection, &all_entries, false)
                        .map_err(AttachmentError::PredictionPreparation)?;
                }
                let publication = VirtualFtPublication::new(
                    [&*ms].into_iter(),
                    components,
                    images,
                    model.incremental,
                    false,
                )?;
                let previous = pending.insert(PendingPublication {
                    path: attachment.path().to_owned(),
                    selection: selection.request.clone(),
                    model: PredictionInputs::new(model),
                    publication,
                });
                previous.publication.publish([(ms, lock)].into_iter())?;
            }
            Ok(true)
        });
        if result.is_err() {
            self.ambiguous_scratch_write = writing_scratch;
            self.pending_publication =
                pending.filter(|previous| previous.publication.may_reference_models());
        }
        result
    }

    fn prepare(
        &mut self,
        ms: &MeasurementSet,
        selected: &ResolvedMsSelection,
        model: &ImagerPredictionModel<'_>,
        scratch: bool,
    ) -> MsResult<Option<(Vec<VirtualModelEntry>, Vec<ImageModel>)>> {
        if model
            .images
            .iter()
            .any(|image| image.as_os_str().is_empty())
        {
            return Ok(None);
        }
        let mosaic = matches!(self.options.gridder, ImagerGridder::Mosaic);
        if mosaic
            && self
                .voltage_pattern
                .as_ref()
                .is_some_and(|pattern| pattern.table.is_some())
        {
            return Err(invalid(
                "legacy mosaic custom voltage-pattern prediction is pending in #1520",
            ));
        }
        // MosaicFT uses its convolution beam directly.
        let pattern = self.voltage_pattern.as_ref().filter(|_| !mosaic);
        if pattern.is_some() && model.components.is_some() {
            return Err(invalid(
                "a radial imager voltage pattern currently needs image models",
            ));
        }
        let (terms, explicit_reference) = self.taylor;
        if terms > 1 && model.images.len() != terms as usize {
            return Err(invalid(
                "the number of model images must match the Taylor terms",
            ));
        }
        let requested = match self.definition.as_ref() {
            Some(ImageDefinition::Requested(request))
                if model.images.iter().any(|path| !path.exists()) =>
            {
                let location = self
                    .options
                    .observatory
                    .map(|[longitude, latitude, radius]| {
                        let horizontal = radius * latitude.cos();
                        MPosition::new_itrf(
                            horizontal * longitude.cos(),
                            horizontal * longitude.sin(),
                            radius * latitude.sin(),
                        )
                    });
                Some(request.build_for_selection(ms, selected, location)?)
            }
            _ => None,
        };
        for path in model.images {
            if !path.exists() {
                let definition = match &self.definition {
                    Some(ImageDefinition::Coordinates(definition)) => Some(definition),
                    _ => requested.as_ref(),
                };
                let Some(definition) = definition else {
                    return Ok(None);
                };
                let mut image = PagedImage::<f32>::create(
                    definition.shape.to_vec(),
                    definition.coordinates.clone(),
                    path,
                )
                .map_err(invalid)?;
                image.set_units("Jy/pixel").map_err(invalid)?;
                image
                    .set_image_info(&ImageInfo {
                        object_name: ms.field()?.name(definition.field_id)?,
                        ..ImageInfo::default()
                    })
                    .map_err(invalid)?;
                let mut misc = RecordValue::default();
                misc.upsert(
                    "INSTRUME",
                    Value::Scalar(ScalarValue::String(
                        ms.observation()?.string(0, "TELESCOPE_NAME")?,
                    )),
                );
                misc.upsert("distance", Value::Scalar(ScalarValue::Float64(0.)));
                image.set_misc_info(misc).map_err(invalid)?;
                image.set_table_subtype("GENERIC");
                image.save().map_err(invalid)?;
                self.frequency_frame_valid = image
                    .coordinates()
                    .find_coordinate(CoordinateType::Spectral)
                    .is_some_and(|index| {
                        matches!(
                            image.coordinates().coordinate(index),
                            CoordinateModel::Spectral(axis)
                                if axis.conversion_frequency_ref().is_some()
                        )
                    });
            }
        }
        let reference_hz = if terms == 1 {
            None
        } else {
            Some(match explicit_reference {
                Some(reference) => reference,
                None => reference_frequency(&model.images[0])?,
            })
        };
        let mut components = Vec::new();
        if let Some(path) = model.components {
            let list = ComponentList::read_table(path).map_err(invalid)?;
            if !model.incremental {
                components.push(model_entry(
                    ms,
                    selected,
                    VirtualModelKind::ComponentList,
                    list.to_record().map_err(invalid)?,
                )?);
            }
        }
        let mut images = Vec::new();
        match (pattern, scratch) {
            (Some(pattern), true) => {
                let mut fields = BTreeMap::<_, Vec<_>>::new();
                for row in &selected.selected_rows {
                    fields.entry(row.field_id).or_default().push(row.clone());
                }
                let mut field = selected.clone();
                for (_, rows) in fields {
                    field.selected_rows = rows;
                    let voltage_pattern = pattern.prepare(ms, &field)?;
                    images.extend(self.prepare_images(
                        ms,
                        &field,
                        model,
                        reference_hz,
                        true,
                        Some(voltage_pattern),
                    )?);
                }
            }
            (pattern, _) => {
                // CASA saves the first transformed beam under all selected
                // virtual combinations. Its scratch iterator can change
                // the beam as it advances to another field.
                let voltage_pattern = pattern
                    .map(|pattern| pattern.prepare(ms, selected))
                    .transpose()?;
                images = self.prepare_images(
                    ms,
                    selected,
                    model,
                    reference_hz,
                    scratch,
                    voltage_pattern,
                )?;
            }
        }
        Ok(Some((components, images)))
    }

    fn prepare_images(
        &self,
        ms: &MeasurementSet,
        selected: &ResolvedMsSelection,
        model: &ImagerPredictionModel<'_>,
        reference_hz: Option<f64>,
        scratch: bool,
        voltage_pattern: Option<super::voltage_pattern::PreparedVoltagePattern>,
    ) -> MsResult<Vec<ImageModel>> {
        let terms = self.taylor.0;
        let geometry = PredictionGeometry {
            settings: self.options,
            phase_centre_time: model.phase_centre_time,
            frequency_frame_valid: self.frequency_frame_valid,
            voltage_pattern,
        };
        let mut images = Vec::new();
        for (index, path) in model.images.iter().enumerate() {
            let taylor = reference_hz.map(|reference_hz| TaylorTerm {
                count: terms,
                index: index as i32,
                reference_hz,
            });
            if let Some(image) = prepare_configured_image(
                ms,
                selected,
                path,
                taylor,
                scratch && !model.incremental && model.components.is_none() && index == 0,
                scratch,
                geometry.clone(),
            )? {
                images.push(image);
            }
        }
        Ok(images)
    }
}

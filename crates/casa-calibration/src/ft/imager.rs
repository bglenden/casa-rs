// SPDX-License-Identifier: LGPL-3.0-or-later
//! Prepare a legacy imager model using the object's effective image and FT settings.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use casa_components::ComponentList;
use casa_coordinates::CoordinateSystem;
use casa_images::PagedImage;
use casa_ms::{MeasurementSet, MsResult, ResolvedMsSelection, VirtualModelEntry, VirtualModelKind};

use crate::{AttachmentError, CalibrationAttachment};

use super::image::{ImageModel, TaylorTerm, prepare_configured_image, reference_frequency};
use super::settings::{ImagerTransformOptions, PredictionGeometry};
use super::virtual_publication::VirtualFtPublication;
use super::voltage_pattern::ImagerVoltagePattern;
use super::{invalid, model_entry, write_model_scratch};

/// Geometry installed by the imager before creating a model image.
pub struct ImagerImageDefinition {
    /// Direction, Stokes and spectral lengths in CASA axis order.
    pub shape: [usize; 4],
    /// The coordinates of a model image created by this imager.
    pub coordinates: CoordinateSystem,
    /// Optional finite-distance focus in metres.
    pub distance_m: Option<f64>,
}

/// Inputs to one imager prediction.
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
    definition: Option<ImagerImageDefinition>,
    options: ImagerTransformOptions,
    taylor: (i32, Option<f64>),
    voltage_pattern: Option<ImagerVoltagePattern>,
}

impl Default for ImagerPrediction {
    fn default() -> Self {
        Self {
            definition: None,
            options: ImagerTransformOptions::default(),
            taylor: (1, None),
            voltage_pattern: None,
        }
    }
}

impl ImagerPrediction {
    /// Set the image definition used to create an absent model image.
    pub fn define_image(&mut self, definition: ImagerImageDefinition) {
        self.definition = Some(definition);
    }

    /// Replace the options used by the next Fourier transform.
    pub fn set_options(&mut self, options: ImagerTransformOptions) {
        self.options = options;
    }

    /// Enable a configured voltage pattern, or disable it with `None`.
    pub fn set_voltage_pattern(&mut self, pattern: Option<ImagerVoltagePattern>) {
        self.voltage_pattern = pattern;
    }

    /// Set the number of Taylor terms and their frequency. With no explicit
    /// frequency the first model image supplies its reference frequency.
    ///
    /// # Errors
    ///
    /// An unsupported number of terms or an invalid frequency.
    pub fn set_taylor_terms(&mut self, terms: i32, reference_hz: Option<f64>) -> MsResult<()> {
        if !(1..=3).contains(&terms) || reference_hz.is_some_and(|hz| !hz.is_finite() || hz <= 0.) {
            return Err(invalid(
                "imager needs one to three Taylor terms and a positive reference frequency",
            ));
        }
        self.taylor = (terms, reference_hz);
        Ok(())
    }

    /// Predict and publish the model for one already-selected attachment.
    ///
    /// Returns false when an absent image needs an image definition.
    ///
    /// # Errors
    ///
    /// An attachment, model preparation, Fourier transform or table write
    /// fails.
    pub fn predict(
        &self,
        attachment: &CalibrationAttachment,
        model: ImagerPredictionModel<'_>,
    ) -> Result<bool, AttachmentError> {
        attachment.with_write(|ms, selection, lock| {
            if selection.selected_rows.is_empty() {
                return Ok(true);
            }
            let scratch = attachment.options().use_scratch;
            let Some((mut components, images)) = self.prepare(ms, selection, &model, scratch)?
            else {
                return Ok(false);
            };
            if scratch {
                components.extend(images.iter().map(|image| image.entry.clone()));
                write_model_scratch(ms, selection, &components, model.incremental, lock)?;
            } else {
                VirtualFtPublication::new(
                    [&*ms].into_iter(),
                    components,
                    images,
                    model.incremental,
                    false,
                )?
                .publish([(ms, lock)].into_iter())?;
            }
            Ok(true)
        })
    }

    fn prepare(
        &self,
        ms: &MeasurementSet,
        selected: &ResolvedMsSelection,
        model: &ImagerPredictionModel<'_>,
        scratch: bool,
    ) -> MsResult<Option<(Vec<VirtualModelEntry>, Vec<ImageModel>)>> {
        if model.images.is_empty() && model.components.is_none() {
            return Ok(None);
        }
        if self.voltage_pattern.is_some() && model.components.is_some() {
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
        for path in model.images {
            if !path.exists() {
                let Some(definition) = &self.definition else {
                    return Ok(None);
                };
                let mut image = PagedImage::<f32>::create(
                    definition.shape.to_vec(),
                    definition.coordinates.clone(),
                    path,
                )
                .map_err(invalid)?;
                image.set_units("Jy/pixel").map_err(invalid)?;
                image.save().map_err(invalid)?;
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
        if let Some(path) = model.components.filter(|_| !model.incremental) {
            let list = ComponentList::read_table(path).map_err(invalid)?;
            components.push(model_entry(
                ms,
                selected,
                VirtualModelKind::ComponentList,
                list.to_record().map_err(invalid)?,
            )?);
        }
        let mut images = Vec::new();
        match self.voltage_pattern.as_ref() {
            Some(pattern) => {
                let mut fields = BTreeMap::<_, Vec<_>>::new();
                for row in &selected.selected_rows {
                    fields.entry(row.field_id).or_default().push(row.clone());
                }
                let mut field = selected.clone();
                for (_, rows) in fields {
                    field.selected_rows = rows;
                    let voltage_pattern = pattern.prepare(ms, &field, model.phase_centre_time)?;
                    images.extend(self.prepare_images(
                        ms,
                        &field,
                        model,
                        reference_hz,
                        scratch,
                        Some(voltage_pattern),
                    )?);
                }
            }
            None => {
                images = self.prepare_images(ms, selected, model, reference_hz, scratch, None)?
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
            voltage_pattern,
            distance_m: self
                .definition
                .as_ref()
                .and_then(|definition| definition.distance_m),
        };
        let mut images = Vec::new();
        for (index, path) in model.images.iter().enumerate() {
            let taylor = reference_hz.map(|reference_hz| TaylorTerm {
                count: terms,
                index: index as i32,
                reference_hz,
            });
            if let Some(mut image) = prepare_configured_image(
                ms,
                selected,
                path,
                taylor,
                scratch && !model.incremental && model.components.is_none() && index == 0,
                scratch,
                geometry.clone(),
            )? {
                image.starts_series = images.is_empty();
                images.push(image);
            }
        }
        Ok(images)
    }
}

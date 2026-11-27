// SPDX-License-Identifier: LGPL-3.0-or-later
//! Read virtual model visibilities during mstransform.

use std::collections::BTreeMap;
use std::sync::Arc;

use casa_core::{ArrayValue, Complex32};
use casa_ms::selection::ChannelRange;
use casa_ms::{MeasurementSet, ResolvedMsSelectionRow, SubtableId, initial_model_visibility};
use casa_mstransform::PlotMsModelReferences;
use casa_mstransform::{PreparedTransformInputView, TransformInputColumns, TransformInputView};
use casa_tables::{Table, TableError};
use ndarray::{Array2, ArrayD, ShapeBuilder};

use crate::solve::grouping::VisibilityModels;

/// Expose stored and virtual models for the transform's model-bearing outputs.
/// When requested, materialize the model from an MS with SOURCE_MODEL even
/// when MAIN has no physical MODEL_DATA column.
pub fn transform_model_input(
    calibration: Option<Arc<dyn TransformInputView>>,
    materialize: bool,
) -> Arc<dyn TransformInputView> {
    Arc::new(ModelView {
        calibration,
        plot_references: None,
        materialize: if materialize {
            ModelMaterialization::SourceColumn
        } else {
            ModelMaterialization::Disabled
        },
    })
}

/// Read stored, virtual or default model visibilities without publishing a column.
pub fn visibility_model_input() -> impl TransformInputView {
    plotms_model_input(None)
}

/// Resolves the virtual or scratch visibility model needed for PlotMS selection and plotting.
pub fn plotms_model_input(
    references: Option<PlotMsModelReferences<'_>>,
) -> impl TransformInputView + '_ {
    ModelView {
        calibration: None,
        plot_references: references,
        materialize: ModelMaterialization::SelectedPlot,
    }
}

#[derive(Debug)]
enum ModelMaterialization {
    Disabled,
    SourceColumn,
    SelectedPlot,
}

#[derive(Debug)]
struct ModelView<'a> {
    calibration: Option<Arc<dyn TransformInputView>>,
    plot_references: Option<PlotMsModelReferences<'a>>,
    materialize: ModelMaterialization,
}

impl ModelView<'_> {
    fn materializes(&self, input: &MeasurementSet) -> bool {
        match self.materialize {
            ModelMaterialization::Disabled => false,
            ModelMaterialization::SourceColumn => input
                .subtable(SubtableId::Source)
                .is_some_and(|source| has_column(source, "SOURCE_MODEL")),
            // The PlotMS loader calls the view only for selected virtual fields.
            ModelMaterialization::SelectedPlot => true,
        }
    }
}

impl TransformInputView for ModelView<'_> {
    fn prepare<'a>(
        &self,
        input: &'a MeasurementSet,
        rows: &[ResolvedMsSelectionRow],
        channels: &BTreeMap<i32, &[usize]>,
    ) -> Result<Box<dyn PreparedTransformInputView + 'a>, TableError> {
        let calibration = self
            .calibration
            .as_ref()
            .map(|view| view.prepare(input, rows, channels))
            .transpose()?;
        let stored = has_column(input.main_table(), "MODEL_DATA");
        let available = stored || self.materializes(input);
        let fields = casa_ms::virtual_model_fields(input);
        let models = if available && rows.iter().any(|row| fields.contains(&row.field_id)) {
            let mut models = VisibilityModels::for_transform(
                input,
                rows,
                self.plot_references
                    .map(|references| references.recipe_slots),
            )
            .map_err(model_error)?;
            if let Some(references) = self.plot_references {
                models
                    .set_plot_phase_references(rows, references.phase_slots)
                    .map_err(model_error)?;
            }
            Some(models)
        } else {
            None
        };
        let remaps = plot_model_channel_remaps(input, rows, self.plot_references)?;
        let rows = if available && (!stored || models.is_some()) {
            rows.iter()
                .enumerate()
                .map(|(slot, row)| {
                    let reads_column = stored
                        && models
                            .as_ref()
                            .is_none_or(|models| models.reads_transform_column(row.row_index));
                    let recipe = self
                        .plot_references
                        .filter(|references| references.spectral_windows.is_some())
                        .map_or(row, |references| &rows[references.recipe_slots[slot]]);
                    let channel_remap = remaps
                        .get(&[row.spectral_window_id, recipe.spectral_window_id])
                        .and_then(Clone::clone);
                    (
                        row.row_index,
                        ModelRow {
                            polarization: [row.polarization_id, recipe.polarization_id],
                            reads_column,
                            channel_remap,
                        },
                    )
                })
                .collect()
        } else {
            BTreeMap::new()
        };
        let mut correlations = BTreeMap::new();
        if !rows.is_empty() {
            let polarization = input.polarization().map_err(model_error)?;
            let requested = self
                .plot_references
                .and_then(|references| references.spectral_windows)
                .map_or(&[][..], |selection| selection.correlations);
            for id in rows
                .values()
                .filter(|row| !row.reads_column)
                .map(|row| row.polarization)
            {
                if let std::collections::btree_map::Entry::Vacant(entry) = correlations.entry(id) {
                    let physical = polarization
                        .corr_type(id[0] as usize)
                        .map_err(model_error)?;
                    let layout = if id[0] == id[1] {
                        physical
                    } else {
                        let recipe = polarization
                            .corr_type(id[1] as usize)
                            .map_err(model_error)?;
                        model_recipe_correlations(physical, recipe, requested)?
                    };
                    entry.insert(layout);
                }
            }
        }
        Ok(Box::new(PreparedModelView {
            calibration,
            input,
            models,
            rows,
            correlations,
        }))
    }

    fn additional_bytes_per_sample(&self) -> usize {
        self.calibration
            .as_deref()
            .map_or(0, TransformInputView::additional_bytes_per_sample)
            .saturating_add(16)
    }

    fn provides(&self, input: &MeasurementSet, column: &str) -> bool {
        (column == "MODEL_DATA" && self.materializes(input))
            || self
                .calibration
                .as_ref()
                .is_some_and(|view| view.provides(input, column))
    }
}

struct PreparedModelView<'a> {
    calibration: Option<Box<dyn PreparedTransformInputView + 'a>>,
    input: &'a MeasurementSet,
    models: Option<VisibilityModels>,
    rows: BTreeMap<usize, ModelRow>,
    correlations: BTreeMap<[i32; 2], Vec<i32>>,
}

struct ModelRow {
    polarization: [i32; 2],
    reads_column: bool,
    channel_remap: Option<Arc<ModelChannelRemap>>,
}

fn model_recipe_correlations(
    mut physical: Vec<i32>,
    recipe: Vec<i32>,
    requested: &[i32],
) -> Result<Vec<i32>, TableError> {
    let selected = |code: i32| requested.is_empty() || requested.contains(&code);
    let mut recipe = recipe.into_iter().filter(|&code| selected(code));
    let shape_error = || {
        model_error("PlotMS virtual SPW models require conforming selected correlations (#1032)")
    };
    // The predictor uses one correlation layout per input buffer. Place its
    // selected codes in the corresponding physical FLAG and output slots.
    for code in physical.iter_mut().filter(|code| selected(**code)) {
        *code = recipe.next().ok_or_else(shape_error)?;
    }
    if recipe.next().is_some() {
        return Err(shape_error());
    }
    Ok(physical)
}

struct ModelChannelRemap {
    physical_count: usize,
    recipe_count: usize,
    pairs: Vec<(usize, usize)>,
}

impl ModelChannelRemap {
    fn prediction_flags(&self, flags: &ArrayD<bool>) -> Result<ArrayD<bool>, TableError> {
        let [correlations, channels] = *flags.shape() else {
            return Err(model_error(
                "PlotMS virtual model flags must have correlation and channel dimensions",
            ));
        };
        if channels != self.physical_count {
            return Err(model_error(
                "PlotMS virtual model flags do not match the selected spectral window",
            ));
        }
        let mut mapped = Array2::from_elem((correlations, self.recipe_count).f(), true);
        for &(physical, recipe) in &self.pairs {
            for correlation in 0..correlations {
                mapped[(correlation, recipe)] = flags[[correlation, physical]];
            }
        }
        Ok(mapped.into_dyn())
    }

    fn physical_model(
        &self,
        model: &ArrayD<Complex32>,
        correlations: usize,
    ) -> Result<ArrayD<Complex32>, TableError> {
        if model.shape() != [correlations, self.recipe_count] {
            return Err(model_error(
                "PlotMS model prediction does not match the input buffer's frequency grid",
            ));
        }
        let mut physical = Array2::zeros((correlations, self.physical_count).f());
        for &(channel, recipe) in &self.pairs {
            for correlation in 0..correlations {
                physical[(correlation, channel)] = model[[correlation, recipe]];
            }
        }
        Ok(physical.into_dyn())
    }
}

fn plot_model_channel_remaps(
    ms: &MeasurementSet,
    rows: &[ResolvedMsSelectionRow],
    references: Option<PlotMsModelReferences<'_>>,
) -> Result<BTreeMap<[i32; 2], Option<Arc<ModelChannelRemap>>>, TableError> {
    let mut remaps = BTreeMap::new();
    let Some((references, ranges)) =
        references.and_then(|references| Some((references, references.spectral_windows?.channels)))
    else {
        return Ok(remaps);
    };
    let windows = ms.spectral_window().map_err(model_error)?;
    let channels = |window: i32| {
        let window = usize::try_from(window).map_err(model_error)?;
        usize::try_from(windows.num_chan(window).map_err(model_error)?).map_err(model_error)
    };
    for (row, &slot) in rows.iter().zip(references.recipe_slots) {
        let physical = row.spectral_window_id;
        let recipe = rows[slot].spectral_window_id;
        if physical == recipe {
            continue;
        }
        if let std::collections::btree_map::Entry::Vacant(entry) = remaps.entry([physical, recipe])
        {
            let physical_count = channels(physical)?;
            let recipe_count = channels(recipe)?;
            let physical_selection =
                selected_model_channels(ranges.get(&physical).map(Vec::as_slice), physical_count);
            let recipe_selection =
                selected_model_channels(ranges.get(&recipe).map(Vec::as_slice), recipe_count);
            if physical_selection.len() != recipe_selection.len() {
                return Err(model_error(format!(
                    "PlotMS virtual SPW models require conforming selected channels in windows {physical} and {recipe} (#1032)"
                )));
            }
            let remap = (physical_count != recipe_count || physical_selection != recipe_selection)
                .then(|| {
                    Arc::new(ModelChannelRemap {
                        physical_count,
                        recipe_count,
                        pairs: physical_selection
                            .into_iter()
                            .zip(recipe_selection)
                            .collect(),
                    })
                });
            entry.insert(remap);
        }
    }
    Ok(remaps)
}

fn selected_model_channels(ranges: Option<&[ChannelRange]>, count: usize) -> Vec<usize> {
    let Some(ranges) = ranges else {
        return (0..count).collect();
    };
    let mut selected: Vec<_> = ranges
        .iter()
        .filter_map(ChannelRange::channels)
        .flatten()
        .collect();
    selected.sort_unstable();
    selected.dedup();
    selected.retain(|&channel| channel < count);
    selected
}

impl PreparedTransformInputView for PreparedModelView<'_> {
    fn read(
        &self,
        input: &Table,
        rows: &[usize],
        columns: &[&str],
    ) -> Result<TransformInputColumns, TableError> {
        let mut result = self
            .calibration
            .as_ref()
            .map(|view| view.read(input, rows, columns))
            .transpose()?
            .unwrap_or_default();
        if columns.contains(&"MODEL_DATA") && !self.rows.is_empty() {
            result.insert("MODEL_DATA", self.read_models(input, rows)?);
        }
        Ok(result)
    }
}

impl PreparedModelView<'_> {
    fn read_models(
        &self,
        input: &Table,
        rows: &[usize],
    ) -> Result<Vec<Option<ArrayValue>>, TableError> {
        let read = |name, rows: &[usize]| {
            input
                .column_accessor(name)?
                .array_cells_owned_uncached(rows)
        };
        let (physical, computed): (Vec<_>, Vec<_>) = rows
            .iter()
            .copied()
            .partition(|row| self.rows[row].reads_column);
        let mut flags = read("FLAG", &computed)?.into_iter();
        let mut stored = if physical.is_empty() {
            Vec::new()
        } else {
            read("MODEL_DATA", &physical)?
        }
        .into_iter();
        rows.iter()
            .map(|&row| {
                let plan = &self.rows[&row];
                if plan.reads_column {
                    return Ok(stored.next().flatten());
                }
                let Some(ArrayValue::Bool(flags)) = flags.next().flatten() else {
                    return Err(TableError::Schema(format!(
                        "model visibility input row {row} has no boolean FLAG"
                    )));
                };
                let layout = &self.correlations[&plan.polarization];
                let prediction_flags = plan
                    .channel_remap
                    .as_ref()
                    .map(|remap| remap.prediction_flags(&flags))
                    .transpose()?;
                let predicted = self
                    .models
                    .as_ref()
                    .map(|model| {
                        model.predict(
                            self.input,
                            row,
                            layout,
                            prediction_flags.as_ref().unwrap_or(&flags),
                        )
                    })
                    .transpose()
                    .map_err(model_error)?
                    .flatten();
                if let Some(model) = predicted {
                    let model = if let Some(remap) = &plan.channel_remap {
                        remap.physical_model(&model, layout.len())?
                    } else {
                        model
                    };
                    return Ok(Some(ArrayValue::Complex32(model)));
                }
                // A defined virtual field without a recipe matching this window
                // has the unit model, even when a physical model also exists.
                let [correlations, channels] = *flags.shape() else {
                    return Err(TableError::Schema(format!(
                        "model visibility input row {row} has invalid FLAG shape"
                    )));
                };
                if correlations != layout.len() {
                    return Err(TableError::Schema(format!(
                        "model visibility input row {row} has invalid correlation layout"
                    )));
                }
                Ok(Some(ArrayValue::Complex32(
                    Array2::from_shape_fn((correlations, channels).f(), |(correlation, _)| {
                        initial_model_visibility(layout[correlation])
                    })
                    .into_dyn(),
                )))
            })
            .collect()
    }
}

fn has_column(table: &Table, name: &str) -> bool {
    table
        .schema()
        .is_some_and(|schema| schema.contains_column(name))
}

fn model_error(source: impl std::fmt::Display) -> TableError {
    TableError::Schema(source.to_string())
}

// SPDX-License-Identifier: LGPL-3.0-or-later
//! Read virtual model visibilities during mstransform.

use std::collections::BTreeMap;
use std::sync::Arc;

use casa_ms::{
    MeasurementSet, PreparedTransformInputView, ResolvedMsSelectionRow, SubtableId,
    TransformInputColumns, TransformInputView, initial_model_visibility, scratch_columns,
};
use casa_tables::{Table, TableError};
use casa_types::ArrayValue;
use ndarray::{Array2, ShapeBuilder};

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
        materialize: if materialize {
            ModelMaterialization::SourceColumn
        } else {
            ModelMaterialization::Disabled
        },
    })
}

pub(crate) fn plotms_model_input() -> impl TransformInputView {
    ModelView {
        calibration: None,
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
struct ModelView {
    calibration: Option<Arc<dyn TransformInputView>>,
    materialize: ModelMaterialization,
}

impl ModelView {
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

impl TransformInputView for ModelView {
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
        let fields = scratch_columns::virtual_model_fields(input);
        let models = if available && rows.iter().any(|row| fields.contains(&row.field_id)) {
            Some(VisibilityModels::for_transform(input, rows).map_err(model_error)?)
        } else {
            None
        };
        let rows = if available && (!stored || models.is_some()) {
            rows.iter()
                .map(|row| {
                    let reads_column = stored
                        && models
                            .as_ref()
                            .is_none_or(|models| models.reads_column(row.field_id));
                    (
                        row.row_index,
                        ModelRow {
                            polarization: row.polarization_id,
                            reads_column,
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
            for id in rows
                .values()
                .filter(|row| !row.reads_column)
                .map(|row| row.polarization)
            {
                if let std::collections::btree_map::Entry::Vacant(entry) = correlations.entry(id) {
                    entry.insert(polarization.corr_type(id as usize).map_err(model_error)?);
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
    correlations: BTreeMap<i32, Vec<i32>>,
}

struct ModelRow {
    polarization: i32,
    reads_column: bool,
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
                let predicted = self
                    .models
                    .as_ref()
                    .map(|model| model.predict(self.input, row, layout, &flags))
                    .transpose()
                    .map_err(model_error)?
                    .flatten();
                if let Some(model) = predicted {
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

// SPDX-License-Identifier: LGPL-3.0-or-later
//! Physical visibility columns requested by a MeasurementSet transform.

use std::path::Path;

use casa_core::ArrayValue;
use casa_tables::streamed_tiled::StreamedTiledShapeValueType;

use crate::MeasurementSet;
use crate::schema::main_table::OptionalMainColumn;

use super::channel_average::{sigma_from_weight, weight_from_sigma};
use super::{MsTransformError, TransformDataColumn, TransformInputView};

#[derive(Clone, Copy)]
pub(super) struct VisibilityCopy {
    pub(super) source: OptionalMainColumn,
    pub(super) output: OptionalMainColumn,
}

impl VisibilityCopy {
    pub(super) fn value_type(self) -> StreamedTiledShapeValueType {
        if self.source == OptionalMainColumn::FloatData {
            StreamedTiledShapeValueType::Float32
        } else {
            StreamedTiledShapeValueType::Complex32
        }
    }
}

pub(super) struct VisibilityColumns(Vec<VisibilityCopy>);

impl VisibilityColumns {
    pub(super) fn resolve(
        input: &MeasurementSet,
        selection: TransformDataColumn,
        explicit: Option<&[OptionalMainColumn]>,
        path: &Path,
        view: Option<&dyn TransformInputView>,
    ) -> Result<Self, MsTransformError> {
        use OptionalMainColumn::{CorrectedData, Data, FloatData, LagData, ModelData};

        let default: (&[OptionalMainColumn], bool) = match selection {
            TransformDataColumn::Data => (&[Data], false),
            TransformDataColumn::CorrectedData => (&[CorrectedData], true),
            TransformDataColumn::ModelData => (&[ModelData], true),
            TransformDataColumn::DataModelCorrected => (&[Data, ModelData, CorrectedData], false),
            TransformDataColumn::FloatData => (&[FloatData], false),
            TransformDataColumn::FloatDataAndData => (&[FloatData, Data], false),
            TransformDataColumn::LagData => (&[LagData], false),
            TransformDataColumn::LagDataAndData => (&[LagData, Data], false),
            TransformDataColumn::All => {
                (&[Data, CorrectedData, ModelData, FloatData, LagData], false)
            }
        };
        let (sources, rename_to_data) = explicit.map_or(default, |columns| {
            (columns, matches!(columns, [CorrectedData | ModelData]))
        });
        let contains = |column: OptionalMainColumn| {
            view.is_some_and(|view| view.provides(input, column.name()))
                || input
                    .main_table()
                    .schema()
                    .is_some_and(|schema| schema.contains_column(column.name()))
        };
        let missing = |column: OptionalMainColumn| MsTransformError::MissingDataColumn {
            path: path.display().to_string(),
            column: column.name().to_string(),
        };
        let mut columns = Vec::new();
        for &source in sources {
            if contains(source) {
                let output = if rename_to_data { Data } else { source };
                columns.push(VisibilityCopy { source, output });
            } else if explicit.is_some() || selection != TransformDataColumn::All {
                return Err(missing(source));
            }
        }
        if columns.is_empty() {
            return Err(MsTransformError::NoDataColumns);
        }
        Ok(Self(columns))
    }

    pub(super) fn copies(&self) -> &[VisibilityCopy] {
        &self.0
    }

    pub(super) fn promote_singleton_visibility(&mut self) {
        use OptionalMainColumn::{CorrectedData, Data, ModelData};
        if let [copy] = self.0.as_mut_slice()
            && matches!(copy.source, CorrectedData | ModelData)
        {
            copy.output = Data;
        }
    }

    pub(super) fn contains_output(&self, name: &str) -> bool {
        self.0.iter().any(|copy| copy.output.name() == name)
    }

    pub(super) fn regular_bytes_per_sample(&self) -> usize {
        self.0
            .iter()
            .map(|column| match column.source {
                OptionalMainColumn::LagData => 0,
                OptionalMainColumn::FloatData => 4,
                _ => 8,
            })
            .sum()
    }

    pub(super) fn source_names(&self) -> String {
        self.0
            .iter()
            .map(|copy| copy.source.name())
            .collect::<Vec<_>>()
            .join(",")
    }

    pub(super) fn output_names(&self) -> String {
        self.0
            .iter()
            .map(|copy| copy.output.name())
            .collect::<Vec<_>>()
            .join(",")
    }

    pub(super) fn corrected_becomes_data(&self) -> bool {
        self.0.iter().any(|copy| {
            copy.source == OptionalMainColumn::CorrectedData
                && copy.output == OptionalMainColumn::Data
        })
    }

    pub(super) fn align_scalar_weights(&self, weight: &mut ArrayValue, sigma: &mut ArrayValue) {
        if self.corrected_becomes_data() {
            if let ArrayValue::Float32(values) = weight {
                *sigma = ArrayValue::Float32(values.mapv(sigma_from_weight));
            }
        } else if !self.preserves_both_scalars()
            && let ArrayValue::Float32(values) = sigma
        {
            *weight = ArrayValue::Float32(values.mapv(weight_from_sigma));
        }
    }

    fn preserves_both_scalars(&self) -> bool {
        use OptionalMainColumn::{CorrectedData, Data, ModelData};
        // Preserve the weights of sparse layouts that CASA's all mode cannot
        // copy. A singleton mapped into DATA takes the ordinary DATA rules.
        if let [column] = self.0.as_slice() {
            matches!(column.output, CorrectedData | ModelData)
        } else {
            self.contains_output(CorrectedData.name()) && self.contains_output(Data.name())
        }
    }
}

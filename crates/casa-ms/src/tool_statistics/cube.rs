// SPDX-License-Identifier: LGPL-3.0-or-later

use casa_core::ArrayValue;
use casa_tables::{Table, TableError};
use ndarray::ArrayD;
use num_complex::Complex32;

use super::reduction::Moments;
use super::rows::Row;
use super::spec::{Column, Weighting};
use crate::selection::ResolvedMsSelection;
use crate::{MeasurementSet, MsError, MsResult, PreparedTransformInputView};

pub(super) struct SampleAxes {
    pub(super) stored_correlations: usize,
    pub(super) channels: Vec<usize>,
    pub(super) correlations: Vec<usize>,
}

impl SampleAxes {
    pub(super) fn for_description(
        ms: &MeasurementSet,
        selection: Option<&ResolvedMsSelection>,
        data_description: i32,
    ) -> MsResult<Self> {
        let data_description = checked_id(data_description, "DATA_DESC_ID")?;
        let descriptions = ms.data_description()?;
        let window = descriptions.spectral_window_id(data_description)?;
        let polarization = descriptions.polarization_id(data_description)?;
        let channel_count = checked_id(
            ms.spectral_window()?
                .num_chan(checked_id(window, "SPECTRAL_WINDOW_ID")?)?,
            "NUM_CHAN",
        )?;
        let channel_selection =
            selection.and_then(|selection| selection.channel_selections.get(&window));
        let channels = match channel_selection {
            Some(selection) => selection.indices(channel_count)?,
            None => (0..channel_count).collect(),
        };
        let correlation_types = ms
            .polarization()?
            .corr_type(checked_id(polarization, "POLARIZATION_ID")?)?;
        let requested =
            selection.map_or(&[][..], |selection| selection.correlation_types.as_slice());
        let stored_correlations = correlation_types.len();
        let correlations = correlation_types
            .into_iter()
            .enumerate()
            .filter_map(|(axis, code)| {
                (requested.is_empty() || requested.contains(&code)).then_some(axis)
            })
            .collect();
        Ok(Self {
            stored_correlations,
            channels,
            correlations,
        })
    }
}

fn checked_id(value: i32, name: &str) -> MsResult<usize> {
    usize::try_from(value).map_err(|_| MsError::InvalidInput(format!("negative {name}: {value}")))
}

pub(super) struct CubeInput<'a> {
    pub(super) table: &'a Table,
    pub(super) model: Option<&'a dyn PreparedTransformInputView>,
}

impl CubeInput<'_> {
    pub(super) fn read_data(
        &self,
        name: &str,
        rows: &[usize],
    ) -> MsResult<Vec<Option<ArrayValue>>> {
        if name == "MODEL_DATA"
            && let Some(model) = self.model
        {
            let mut columns = model.read(self.table, rows, &["MODEL_DATA"])?;
            if let Some(values) = columns.remove("MODEL_DATA") {
                return Ok(values);
            }
        }
        read_cells(self.table, name, rows)
    }
}

pub(super) struct CubeReduction {
    pub(super) use_flags: bool,
    pub(super) weighting: Weighting,
    pub(super) legacy: bool,
}

impl CubeReduction {
    pub(super) fn reduce(
        &self,
        input: &CubeInput<'_>,
        rows: &[Row],
        axes: &SampleAxes,
        column: Column,
        dataset: usize,
        moments: &mut Moments,
    ) -> MsResult<()> {
        let name = match column {
            Column::Complex(name, _) | Column::FloatCube(name) => name,
            Column::Flag => "FLAG",
            _ => unreachable!("only array visibility columns use the cube reducer"),
        };
        let indices: Vec<_> = rows.iter().map(|row| row.physical).collect();
        let cells = input.read_data(name, &indices)?;
        let table = input.table;
        let flags = if self.use_flags && name != "FLAG" {
            read_cells(table, "FLAG", &indices)?
        } else {
            Vec::new()
        };
        let weights = WeightCells::read(table, &indices, self.weighting)?;
        let mut buffer_index = 0;
        for (index, row) in rows.iter().enumerate() {
            let cell = required(&cells, index, row.physical, name)?;
            let mask = if self.use_flags {
                let flag = if name == "FLAG" {
                    cell
                } else {
                    required(&flags, index, row.physical, "FLAG")?
                };
                let ArrayValue::Bool(mask) = flag else {
                    return Err(invalid_cell("FLAG"));
                };
                Some(mask)
            } else {
                None
            };
            let options = Samples {
                axes,
                mask,
                weights: weights.row(index),
                flagged_row: self.use_flags && self.legacy && row.flag,
                legacy: self.legacy,
                dataset,
            };
            match (column, cell) {
                (Column::Complex(_, component), ArrayValue::Complex32(values)) => {
                    options.reduce(
                        values,
                        |value| component.project(*value),
                        moments,
                        &mut buffer_index,
                    )?;
                }
                (Column::Complex("FLOAT_DATA", component), ArrayValue::Float32(values)) => {
                    options.reduce(
                        values,
                        |&value| component.project(Complex32::new(value, 0.0)),
                        moments,
                        &mut buffer_index,
                    )?;
                }
                (Column::FloatCube(_), ArrayValue::Float32(values)) => {
                    options.reduce(values, |value| *value, moments, &mut buffer_index)?;
                }
                (Column::Flag, ArrayValue::Bool(values)) => {
                    options.reduce(
                        values,
                        |value| f32::from(u8::from(*value)),
                        moments,
                        &mut buffer_index,
                    )?;
                }
                _ => return Err(invalid_cell(name)),
            }
        }
        Ok(())
    }
}

struct Samples<'a> {
    axes: &'a SampleAxes,
    mask: Option<&'a ArrayD<bool>>,
    weights: WeightRow<'a>,
    flagged_row: bool,
    legacy: bool,
    dataset: usize,
}

impl Samples<'_> {
    fn reduce<T>(
        &self,
        values: &ArrayD<T>,
        project: impl Fn(&T) -> f32,
        moments: &mut Moments,
        buffer_index: &mut usize,
    ) -> MsResult<()> {
        if values.ndim() != 2 || self.mask.is_some_and(|flag| flag.shape() != values.shape()) {
            return Err(MsError::InvalidInput(
                "inconsistent visibility cell shape".into(),
            ));
        }
        for &channel in &self.axes.channels {
            for &correlation in &self.axes.correlations {
                let index = [correlation, channel];
                let current = *buffer_index;
                *buffer_index += 1;
                let value = values.get(index).ok_or_else(|| {
                    MsError::InvalidInput(
                        "visibility selection exceeds the stored cell shape".into(),
                    )
                })?;
                if self.flagged_row || self.mask.is_some_and(|flags| flags[index]) {
                    continue;
                }
                let weight = self.weights.get(correlation, channel)?;
                let projected = project(value);
                let value = f64::from(projected);
                let square = if self.legacy {
                    f64::from(projected * projected)
                } else {
                    value * value
                };
                moments.push(value, square, weight, self.dataset, current);
            }
        }
        Ok(())
    }
}

pub(super) struct WeightCells {
    weighting: Weighting,
    spectrum: Vec<Option<ArrayValue>>,
    vector: Vec<Option<ArrayValue>>,
}

impl WeightCells {
    pub(super) fn read(table: &Table, rows: &[usize], weighting: Weighting) -> MsResult<Self> {
        let spectrum_name = match weighting {
            Weighting::Unweighted => None,
            Weighting::InverseSigmaSquared => Some("SIGMA_SPECTRUM"),
            Weighting::VisibilityWeight => Some("WEIGHT_SPECTRUM"),
        };
        let mut spectrum = Vec::new();
        let mut vector = Vec::new();
        if let Some(name) = spectrum_name {
            if table
                .schema()
                .is_some_and(|schema| schema.column(name).is_some())
            {
                spectrum = read_cells(table, name, rows)?;
            }
            if spectrum.iter().filter(|cell| cell.is_some()).count() < rows.len() {
                let vector_name = if weighting == Weighting::InverseSigmaSquared {
                    "SIGMA"
                } else {
                    "WEIGHT"
                };
                vector = read_cells(table, vector_name, rows)?;
            }
        }
        Ok(Self {
            weighting,
            spectrum,
            vector,
        })
    }

    pub(super) fn row(&self, index: usize) -> WeightRow<'_> {
        WeightRow {
            weighting: self.weighting,
            spectrum: self.spectrum.get(index).and_then(Option::as_ref),
            vector: self.vector.get(index).and_then(Option::as_ref),
        }
    }
}

pub(super) struct WeightRow<'a> {
    weighting: Weighting,
    spectrum: Option<&'a ArrayValue>,
    vector: Option<&'a ArrayValue>,
}

impl WeightRow<'_> {
    pub(super) fn get(&self, correlation: usize, channel: usize) -> MsResult<f64> {
        let value = match (self.weighting, self.spectrum, self.vector) {
            (Weighting::Unweighted, _, _) => return Ok(1.0),
            (_, Some(ArrayValue::Float32(values)), _) => values.get([correlation, channel]),
            (_, None, Some(ArrayValue::Float32(values))) => values.get([correlation]),
            _ => None,
        }
        .copied()
        .ok_or_else(|| {
            MsError::InvalidInput("visibility weights do not match the cell shape".into())
        })?;
        let weight = match self.weighting {
            Weighting::InverseSigmaSquared => crate::transform::weight_from_sigma(value),
            _ => value,
        };
        Ok(f64::from(weight))
    }
}

pub(super) fn read_cells(
    table: &Table,
    name: &str,
    rows: &[usize],
) -> MsResult<Vec<Option<ArrayValue>>> {
    Ok(table.column_accessor(name)?.array_cells_owned(rows)?)
}

pub(super) fn required<'a>(
    cells: &'a [Option<ArrayValue>],
    index: usize,
    physical_row: usize,
    column: &str,
) -> MsResult<&'a ArrayValue> {
    cells.get(index).and_then(Option::as_ref).ok_or_else(|| {
        TableError::ColumnNotFound {
            row_index: physical_row,
            column: column.to_string(),
        }
        .into()
    })
}

fn invalid_cell(name: &str) -> MsError {
    MsError::InvalidInput(format!("unexpected visibility cell type for {name}"))
}

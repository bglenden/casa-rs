// SPDX-License-Identifier: LGPL-3.0-or-later

use super::*;

#[derive(Debug, Clone)]
pub(super) struct ComplexGrid {
    pub(super) corr_count: usize,
    pub(super) chan_count: usize,
    pub(super) values: Vec<Complex64>,
    pub(super) model: Option<(VisibilityColumnProjection, Vec<Complex64>)>,
    pub(super) signed_amplitude: bool,
}

impl ComplexGrid {
    fn with_model(
        mut self,
        model: Self,
        projection: VisibilityColumnProjection,
    ) -> Result<Self, String> {
        check_grid_shapes(&self, &model)?;
        self.model = Some((projection, model.values));
        Ok(self)
    }
}

#[derive(Debug, Clone)]
pub(super) struct FloatGrid {
    pub(super) corr_count: usize,
    pub(super) chan_count: usize,
    pub(super) values: Vec<f64>,
}

pub(super) fn selected_bool_values(
    table: &Table,
    column: &'static str,
    rows: &[usize],
) -> Result<Vec<bool>, String> {
    selected_scalar_values(table, column, rows, |value| match value {
        ScalarValue::Bool(value) => Ok(value),
        other => Err(format!(
            "msexplore requires BOOL {column} cells, found {:?}",
            other.primitive_type()
        )),
    })
}

pub fn selected_i32_values(
    table: &Table,
    column: &'static str,
    rows: &[usize],
) -> Result<Vec<i32>, String> {
    selected_scalar_values(table, column, rows, |value| match value {
        ScalarValue::Int32(value) => Ok(value),
        other => Err(format!(
            "msexplore requires INT {column} cells, found {:?}",
            other.primitive_type()
        )),
    })
}

pub fn selected_f64_values(
    table: &Table,
    column: &'static str,
    rows: &[usize],
) -> Result<Vec<f64>, String> {
    selected_scalar_values(table, column, rows, |value| match value {
        ScalarValue::Float64(value) => Ok(value),
        other => Err(format!(
            "msexplore requires DOUBLE {column} cells, found {:?}",
            other.primitive_type()
        )),
    })
}

pub(super) fn selected_scalar_values<T>(
    table: &Table,
    column: &'static str,
    rows: &[usize],
    convert: impl Fn(ScalarValue) -> Result<T, String>,
) -> Result<Vec<T>, String> {
    table
        .column_accessor(column)
        .map_err(|error| error.to_string())?
        .scalar_cells_owned_for_rows(rows)
        .map_err(|error| error.to_string())?
        .into_iter()
        .zip(rows.iter().copied())
        .map(|(value, row)| {
            value
                .ok_or_else(|| format!("msexplore requires {column} data for row {row}"))
                .and_then(&convert)
        })
        .collect()
}

pub(super) fn selected_uvw_values(
    table: &Table,
    column: &'static str,
    rows: &[usize],
) -> Result<Vec<[f64; 3]>, String> {
    table
        .column_accessor(column)
        .map_err(|error| error.to_string())?
        .array_cells_owned(rows)
        .map_err(|error| error.to_string())?
        .into_iter()
        .zip(rows.iter().copied())
        .map(|(value, row)| {
            let value =
                value.ok_or_else(|| format!("msexplore requires {column} data for row {row}"))?;
            match value {
                ArrayValue::Float64(values) => {
                    let slice = values.as_slice().ok_or_else(|| {
                        format!("msexplore requires contiguous {column} f64[3] cells")
                    })?;
                    if slice.len() != 3 {
                        return Err(format!(
                            "msexplore requires {column} cells with shape [3], found [{}]",
                            slice.len()
                        ));
                    }
                    Ok([slice[0], slice[1], slice[2]])
                }
                other => Err(format!(
                    "msexplore requires DOUBLE array {column} cells, found {:?}",
                    other.primitive_type()
                )),
            }
        })
        .collect()
}

pub(super) fn selected_plot_weight_spectrum<'a>(
    ms: &MeasurementSet,
    rows: &[usize],
    calibration: Option<&PlotMsInput<'a>>,
) -> Result<Option<SelectedArrayColumn<'a>>, String> {
    let has_spectrum = match calibration.and_then(|input| input.rows) {
        Some(calibrated) => rows
            .first()
            .and_then(|row| calibrated.get(row))
            .is_some_and(|row| row.weight_spectrum.is_some()),
        None => WeightSpectrumColumn::new(ms.main_table()).is_ok(),
    };
    has_spectrum
        .then(|| {
            SelectedArrayColumn::load_for_plot(
                ms.main_table(),
                "WEIGHT_SPECTRUM",
                rows,
                calibration,
            )
        })
        .transpose()
}

pub(super) enum SelectedArrayValues<'a> {
    Stored(Vec<Option<ArrayValue>>),
    Projected(Vec<Option<&'a ArrayValue>>),
    DefaultModel(Vec<Arc<ArrayValue>>),
}

pub(super) struct SelectedArrayColumn<'a> {
    pub(super) column: &'static str,
    pub(super) values: SelectedArrayValues<'a>,
}

impl<'a> SelectedArrayColumn<'a> {
    pub(super) fn load(
        table: &Table,
        column: &'static str,
        row_indices: &[usize],
    ) -> Result<Self, String> {
        let values = table
            .column_accessor(column)
            .map_err(|error| error.to_string())?
            .array_cells_owned(row_indices)
            .map_err(|error| error.to_string())?;
        Ok(Self {
            column,
            values: SelectedArrayValues::Stored(values),
        })
    }

    pub(super) fn load_for_plot(
        table: &Table,
        column: &'static str,
        rows: &[usize],
        calibration: Option<&PlotMsInput<'a>>,
    ) -> Result<Self, String> {
        if column == "MODEL_DATA"
            && let Some(models) = calibration.and_then(|input| input.models)
        {
            return Ok(Self {
                column,
                values: SelectedArrayValues::Projected(
                    rows.iter().map(|row| models.get(row)).collect(),
                ),
            });
        }
        let cell: fn(&PlotMsCalibratedRow) -> Option<&ArrayValue> = match column {
            "CORRECTED_DATA" => |row| Some(&row.corrected_data),
            "FLAG" => |row| Some(&row.flags),
            "WEIGHT" => |row| Some(&row.weight),
            "WEIGHT_SPECTRUM" => |row| row.weight_spectrum.as_ref(),
            _ => return Self::load(table, column, rows),
        };
        let Some(calibrated) = calibration.and_then(|input| input.rows) else {
            return Self::load(table, column, rows);
        };
        let values = rows
            .iter()
            .map(|row| calibrated.get(row).and_then(cell))
            .collect();
        Ok(Self {
            column,
            values: SelectedArrayValues::Projected(values),
        })
    }

    pub(super) fn find(&self, row_slot: usize) -> Option<&ArrayValue> {
        match &self.values {
            SelectedArrayValues::Stored(values) => values.get(row_slot).and_then(Option::as_ref),
            SelectedArrayValues::Projected(values) => values.get(row_slot).copied().flatten(),
            SelectedArrayValues::DefaultModel(values) => values.get(row_slot).map(Arc::as_ref),
        }
    }

    pub(super) fn get(&self, row_slot: usize) -> Result<&ArrayValue, String> {
        self.find(row_slot).ok_or_else(|| {
            format!(
                "msexplore requires {} data for selected row slot {row_slot}",
                self.column
            )
        })
    }

    pub(super) fn spectrum_in_buffer(
        &self,
        row_slot: usize,
        buffer_first: Option<usize>,
    ) -> Result<Option<&ArrayValue>, String> {
        match buffer_first {
            Some(first) if self.find(first).is_some() => self.get(row_slot).map(Some),
            Some(_) => Ok(None),
            None => Ok(self.find(row_slot)),
        }
    }

    fn visibility_grid(&self, row_slot: usize) -> Result<ComplexGrid, String> {
        let cell = self.get(row_slot)?;
        if self.column == "FLOAT_DATA" {
            float_visibility_grid(cell)
        } else {
            complex_grid_from_array(cell)
        }
    }
}

pub(super) enum PreparedSelectedDataSource<'a> {
    Single(SelectedArrayColumn<'a>),
    Difference {
        left: SelectedArrayColumn<'a>,
        right: SelectedArrayColumn<'a>,
        scalar: bool,
    },
    Ratio {
        numerator: SelectedArrayColumn<'a>,
        denominator: SelectedArrayColumn<'a>,
        scalar: bool,
    },
}

impl<'a> PreparedSelectedDataSource<'a> {
    pub(super) fn new(
        ms: &MeasurementSet,
        column: MsDataColumn,
        row_indices: &[usize],
        calibration: Option<&PlotMsInput<'a>>,
    ) -> Result<Self, String> {
        let load = |name| {
            if name == "MODEL_DATA"
                && calibration.is_some_and(|input| input.rows.is_none() && input.models.is_none())
                && ms.main_table().schema().is_some_and(|schema| {
                    schema.column("MODEL_DATA").is_none()
                        && schema.column("FLOAT_DATA").is_none()
                        && (schema.column("DATA").is_some()
                            || schema.column("CORRECTED_DATA").is_some())
                })
            {
                return Ok(SelectedArrayColumn {
                    column: name,
                    values: SelectedArrayValues::DefaultModel(plotms_model::default_cells(
                        ms,
                        row_indices,
                    )?),
                });
            }
            SelectedArrayColumn::load_for_plot(ms.main_table(), name, row_indices, calibration)
        };
        let observed = if calibration.is_some_and(|input| input.prefer_float_data) {
            "FLOAT_DATA"
        } else {
            "DATA"
        };
        match column {
            MsDataColumn::Data => Ok(Self::Single(load(observed)?)),
            MsDataColumn::Float => Ok(Self::Single(load("FLOAT_DATA")?)),
            MsDataColumn::Corrected => Ok(Self::Single(load("CORRECTED_DATA")?)),
            MsDataColumn::Model => Ok(Self::Single(load("MODEL_DATA")?)),
            MsDataColumn::CorrectedMinusModel => Ok(Self::Difference {
                left: load("CORRECTED_DATA")?,
                right: load("MODEL_DATA")?,
                scalar: false,
            }),
            MsDataColumn::CorrectedMinusModelScalar => Ok(Self::Difference {
                left: load("CORRECTED_DATA")?,
                right: load("MODEL_DATA")?,
                scalar: true,
            }),
            MsDataColumn::DataMinusModel => Ok(Self::Difference {
                left: load(observed)?,
                right: load("MODEL_DATA")?,
                scalar: false,
            }),
            MsDataColumn::DataMinusModelScalar => Ok(Self::Difference {
                left: load(observed)?,
                right: load("MODEL_DATA")?,
                scalar: true,
            }),
            MsDataColumn::CorrectedDivModel => Ok(Self::Ratio {
                numerator: load("CORRECTED_DATA")?,
                denominator: load("MODEL_DATA")?,
                scalar: false,
            }),
            MsDataColumn::CorrectedDivModelScalar => Ok(Self::Ratio {
                numerator: load("CORRECTED_DATA")?,
                denominator: load("MODEL_DATA")?,
                scalar: true,
            }),
            MsDataColumn::DataDivModel => Ok(Self::Ratio {
                numerator: load(observed)?,
                denominator: load("MODEL_DATA")?,
                scalar: false,
            }),
            MsDataColumn::DataDivModelScalar => Ok(Self::Ratio {
                numerator: load(observed)?,
                denominator: load("MODEL_DATA")?,
                scalar: true,
            }),
        }
    }

    pub(super) fn row(
        &self,
        row_slot: usize,
        project_plotms_columns: bool,
        project_after_average: bool,
    ) -> Result<ComplexGrid, String> {
        match self {
            Self::Single(column) => column.visibility_grid(row_slot),
            Self::Difference {
                left,
                right,
                scalar,
            } => {
                let left = left.visibility_grid(row_slot)?;
                let right = right.visibility_grid(row_slot)?;
                if *scalar && project_plotms_columns {
                    return left.with_model(right, VisibilityColumnProjection::ScalarResidual);
                }
                if project_plotms_columns && project_after_average {
                    return left.with_model(right, VisibilityColumnProjection::VectorResidual);
                }
                combine_grids(left, right, |left, right| {
                    if *scalar {
                        Complex64::new(left.norm() - right.norm(), 0.0)
                    } else {
                        left - right
                    }
                })
            }
            Self::Ratio {
                numerator,
                denominator,
                scalar,
            } => {
                let numerator = numerator.visibility_grid(row_slot)?;
                let denominator = denominator.visibility_grid(row_slot)?;
                if project_plotms_columns {
                    let projection = if *scalar {
                        VisibilityColumnProjection::ScalarRatio
                    } else {
                        VisibilityColumnProjection::VectorRatio
                    };
                    return numerator.with_model(denominator, projection);
                }
                combine_grids(numerator, denominator, |left, right| {
                    if *scalar {
                        if right.norm() == 0.0 {
                            Complex64::new(f64::NAN, f64::NAN)
                        } else {
                            Complex64::new(left.norm() / right.norm(), 0.0)
                        }
                    } else if right == Complex64::new(0.0, 0.0) {
                        Complex64::new(f64::NAN, f64::NAN)
                    } else {
                        left / right
                    }
                })
            }
        }
    }
}

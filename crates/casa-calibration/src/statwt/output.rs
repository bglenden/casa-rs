// SPDX-License-Identifier: LGPL-3.0-or-later
//! Resolve which scalar and spectral weight columns statwt writes.

use casa_core::{ArrayValue, Value};
use casa_ms::{MsError, MsResult};
use casa_tables::{Table, TableWriteLock};
use ndarray::{Array1, ArrayD, IxDyn, ShapeBuilder};

use crate::weight_spectra::{NewSpectra, SIGMA_SPECTRUM, WEIGHT_SPECTRUM, has_column};

use super::StatWtDataColumn;

#[derive(PartialEq, Eq)]
pub(super) struct Output {
    weight: bool,
    sigma: bool,
    weight_spectrum: bool,
    sigma_spectrum: bool,
    pub(super) spectral: bool,
}

impl Output {
    pub(super) fn prepare(
        table: &mut Table,
        column: StatWtDataColumn,
        split_channels: bool,
        selection: &[usize],
        lock: Option<&mut TableWriteLock>,
    ) -> MsResult<Self> {
        let sigma = column.raw();
        let weight = !sigma || !has_column(table, "CORRECTED_DATA");
        if !has_column(table, column.visibility_column()) {
            return Err(MsError::InvalidInput(format!(
                "statwt requires the {} column",
                column.visibility_column()
            )));
        }
        let weight_initialized = initialized(table, WEIGHT_SPECTRUM)?;
        let sigma_initialized = initialized(table, SIGMA_SPECTRUM)?;
        let spectral = split_channels || weight_initialized;
        if weight_initialized {
            check_weight_spectrum(table, selection)?;
        }
        let weight_spectrum = weight && spectral;
        let sigma_spectrum = sigma && (split_channels || sigma_initialized);
        if let Some(lock) = lock {
            if sigma_spectrum && !spectral {
                return Err(MsError::InvalidInput(
                    "SIGMA_SPECTRUM is only partially initialized".into(),
                ));
            }
            let mut initialize = Vec::new();
            if weight_spectrum && !weight_initialized {
                initialize.push(("WEIGHT", WEIGHT_SPECTRUM));
            }
            if sigma_spectrum && !sigma_initialized {
                initialize.push(("SIGMA", SIGMA_SPECTRUM));
            }
            for (scalar, spectrum) in initialize {
                initialize_spectrum(table, scalar, spectrum, lock)?;
            }
        }
        Ok(Self {
            weight,
            sigma,
            weight_spectrum,
            sigma_spectrum,
            spectral,
        })
    }

    pub(super) fn columns(&self) -> Vec<&'static str> {
        self.values().map(|(column, _)| column).collect()
    }

    fn values(&self) -> impl Iterator<Item = (&'static str, OutputValue)> {
        [
            (self.weight, "WEIGHT", OutputValue::Weight),
            (self.sigma, "SIGMA", OutputValue::Sigma),
            (
                self.weight_spectrum,
                WEIGHT_SPECTRUM,
                OutputValue::WeightSpectrum,
            ),
            (
                self.sigma_spectrum,
                SIGMA_SPECTRUM,
                OutputValue::SigmaSpectrum,
            ),
        ]
        .into_iter()
        .filter_map(|(write, name, value)| write.then_some((name, value)))
    }

    pub(super) fn cells(
        &self,
        scalar_weights: &[f32],
        spectrum: Option<&ArrayD<f32>>,
    ) -> Vec<ArrayValue> {
        self.values()
            .map(|(_, value)| {
                ArrayValue::Float32(match value {
                    OutputValue::Weight => Array1::from_vec(scalar_weights.to_vec()).into_dyn(),
                    OutputValue::Sigma => {
                        Array1::from_iter(scalar_weights.iter().copied().map(sigma)).into_dyn()
                    }
                    OutputValue::WeightSpectrum => spectrum.expect("spectral output").clone(),
                    OutputValue::SigmaSpectrum => spectrum.expect("spectral output").mapv(sigma),
                })
            })
            .collect()
    }
}

enum OutputValue {
    Weight,
    Sigma,
    WeightSpectrum,
    SigmaSpectrum,
}

fn sigma(weight: f32) -> f32 {
    if weight == 0.0 {
        -1.0
    } else {
        weight.sqrt().recip()
    }
}

fn initialized(table: &Table, column: &str) -> MsResult<bool> {
    if table.row_count() == 0 || !has_column(table, column) {
        return Ok(false);
    }
    let reader = table.column_accessor(column)?;
    Ok(match reader.array_cell_is_defined_uncached(0) {
        Ok(defined) => defined,
        Err(_) => reader.array_cells_owned_uncached(&[0])?[0].is_some(),
    })
}

fn check_weight_spectrum(table: &Table, selection: &[usize]) -> MsResult<()> {
    let reader = table.column_accessor(WEIGHT_SPECTRUM)?;
    for batch in selection.chunks(256) {
        let missing = match reader.array_shapes_uncached(batch) {
            Ok(shapes) => shapes.iter().position(Option::is_none),
            Err(_) => reader
                .array_cells_owned_uncached(batch)?
                .iter()
                .position(Option::is_none),
        };
        if let Some(index) = missing {
            return Err(MsError::InvalidInput(format!(
                "WEIGHT_SPECTRUM is undefined at row {}",
                batch[index]
            )));
        }
    }
    Ok(())
}

fn initialize_spectrum(
    table: &mut Table,
    scalar: &str,
    spectrum: &'static str,
    lock: &mut TableWriteLock,
) -> MsResult<()> {
    let rows: Vec<_> = (0..table.row_count()).collect();
    let flag_column = table.column_accessor("FLAG")?;
    let mut shapes = Vec::with_capacity(rows.len());
    for selected in rows.chunks(8192) {
        let batch = match flag_column.array_shapes_uncached(selected) {
            Ok(shapes) => shapes,
            Err(_) => flag_column
                .array_cells_owned_uncached(selected)?
                .iter()
                .map(|value| value.as_ref().map(|cell| cell.shape().to_vec()))
                .collect(),
        };
        for (&row, shape) in selected.iter().zip(batch) {
            let Some([correlations, channels]) = shape.as_deref() else {
                return Err(MsError::InvalidInput(format!(
                    "invalid FLAG shape at row {row}"
                )));
            };
            shapes.push([*correlations, *channels]);
        }
    }
    let mut new_spectrum = NewSpectra::prepare(table, &[spectrum], shapes.iter().copied())?;
    let max_cell = shapes.iter().map(|[a, b]| a * b).max().unwrap_or(1).max(1);
    let batch_size = (casa_ms::TARGET_BATCH_BYTES / (4 * max_cell)).clamp(1, 8192);
    for selected in rows.chunks(batch_size) {
        let input = table
            .column_accessor(scalar)?
            .array_cells_owned_uncached(selected)?;
        let mut edit = (!new_spectrum.contains(spectrum))
            .then(|| table.row_accessor_mut().prepare(&[spectrum]))
            .transpose()?;
        for (&row, value) in selected.iter().zip(input) {
            let [correlations, channels] = shapes[row];
            let Some(ArrayValue::Float32(value)) = value else {
                return Err(MsError::InvalidInput(format!(
                    "{scalar} is undefined at row {row}"
                )));
            };
            if value.shape() != [correlations] {
                return Err(MsError::InvalidInput(format!(
                    "invalid {scalar} shape at row {row}"
                )));
            }
            let expanded = ArrayValue::Float32(ArrayD::from_shape_fn(
                IxDyn(&[correlations, channels]).f(),
                |i| value[i[0]],
            ));
            if let Some(edit) = &mut edit {
                edit.seek(row)?;
                edit.set_value_at(0, Value::Array(expanded))?;
            } else {
                new_spectrum.append(spectrum, &expanded)?;
            }
        }
        if let Some(mut edit) = edit {
            edit.flush()?;
            lock.record_write();
            table
                .prepare_write()
                .save_selected_rows(&[spectrum], selected)?;
            table.discard_persisted_cell_updates(&[spectrum], selected);
        }
    }
    new_spectrum.publish(table, lock)
}

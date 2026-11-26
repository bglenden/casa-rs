// SPDX-License-Identifier: LGPL-3.0-or-later
//! CASA single-dish parallel-hand projection for selected MAIN rows.

use std::collections::BTreeMap;
use std::fs;
use std::io::ErrorKind;
use std::ops::{Add, Div, Mul};
use std::path::Path;

use casa_core::{ArrayValue, Correlation, ScalarValue};
use casa_tables::{ArrayShapeContract, ColumnType, Table, TableError, TableOptions};
use ndarray::{Array1, Array2, Array3, ArrayD, Ix2, Ix3};

use super::correlation_metadata::{CorrelationSelection, parallel_hands};
use super::main_arrays::MainArrayBlock;
use super::{
    MsTransformError, MsTransformRequest, TransformChannelSelection,
    rewrite_table_rows_with_variable_arrays,
};
use crate::subtable_rows::{b, i, i32_array, row_from_defs};
use crate::{MeasurementSet, MsDataDescriptionMut, MsPolarization, MsResult, SubtableId, schema};

/// How to combine a selected pair of single-dish parallel hands.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SingleDishPolarizationAverage {
    /// Average the available samples with equal coefficients.
    Stokes,
    /// Average the available samples using spectral or scalar weights.
    Geometric,
}

impl SingleDishPolarizationAverage {
    fn weight(self, a: f32, b: f32) -> f32 {
        match self {
            Self::Stokes => (4. / (f64::from(a).recip() + f64::from(b).recip())) as f32,
            Self::Geometric => a + b,
        }
    }
}

struct Projection<'a> {
    hands: [usize; 2],
    flags: ndarray::ArrayView2<'a, bool>,
    spectral_weight: Option<ndarray::ArrayView2<'a, f32>>,
    weight: [f32; 2],
    mode: SingleDishPolarizationAverage,
    flag_row: bool,
}

impl Projection<'_> {
    fn weight_for(&self, hand: usize, channel: usize) -> f32 {
        if self.mode == SingleDishPolarizationAverage::Stokes {
            1.
        } else if let Some(weight) = self.spectral_weight {
            weight[[self.hands[hand], channel]]
        } else {
            self.weight[hand]
        }
    }

    fn data<T>(&self, input: ArrayD<T>) -> Result<ArrayD<T>, TableError>
    where
        T: Copy + Default + Add<Output = T> + Mul<f32, Output = T> + Div<f32, Output = T>,
    {
        let input = input
            .into_dimensionality::<Ix2>()
            .map_err(|_| invalid("visibility"))?;
        if input.shape() != self.flags.shape() {
            return Err(invalid("visibility"));
        }
        let channels = self.flags.ncols();
        let result = Array2::from_shape_fn((1, channels), |(_, channel)| {
            let mut summed = T::default();
            let mut weight = 0.;
            for hand in 0..2 {
                let coordinate = [self.hands[hand], channel];
                if !self.flag_row && !self.flags[coordinate] {
                    let w = self.weight_for(hand, channel);
                    summed = summed + input[coordinate] * w;
                    weight += w;
                }
            }
            if weight > 0. { summed / weight } else { summed }
        });
        Ok(result.into_dyn())
    }

    fn flags(&self) -> ArrayD<bool> {
        Array2::from_shape_fn((1, self.flags.ncols()), |(_, channel)| {
            self.flag_row
                || (self.flags[[self.hands[0], channel]] && self.flags[[self.hands[1], channel]])
        })
        .into_dyn()
    }

    fn projected_scalar_weight(&self) -> f32 {
        self.mode.weight(self.weight[0], self.weight[1])
    }
}

fn invalid(name: &str) -> TableError {
    TableError::Schema(format!(
        "Selected single-dish {name} has inconsistent axes."
    ))
}

pub(super) struct PolarizationProjection {
    hands: BTreeMap<i32, [usize; 2]>,
    mode: SingleDishPolarizationAverage,
}

impl PolarizationProjection {
    pub(super) fn resolve(
        input: &MeasurementSet,
        selection: Option<&CorrelationSelection>,
        ddids: &[i32],
        mode: SingleDishPolarizationAverage,
    ) -> MsResult<Option<Self>> {
        let mut hands = parallel_hands(input, selection, ddids)?;
        for (&description, indices) in &mut hands {
            if let Some(axes) = selection.and_then(|selection| selection.axes(description)) {
                *indices = indices.map(|index| axes[index]);
            }
        }
        Ok((!hands.is_empty()).then_some(Self { hands, mode }))
    }

    pub(super) fn contains(&self, description: i32) -> bool {
        self.hands.contains_key(&description)
    }

    pub(super) fn write_metadata(
        &self,
        request: &MsTransformRequest,
        descriptions: &BTreeMap<i32, i32>,
    ) -> Result<(), MsTransformError> {
        let polarization = request.output_ms.join(SubtableId::Polarization.name());
        let description = request.output_ms.join(SubtableId::DataDescription.name());
        let error = |path: &std::path::Path, reason: String| MsTransformError::SpectralMetadata {
            path: path.display().to_string(),
            reason,
        };
        let stokes = ensure_stokes_i_polarization(&polarization)?;
        let mut dd = Table::open(TableOptions::new(&description))
            .map_err(|source| error(&description, source.to_string()))?;
        let mut metadata = MsDataDescriptionMut::new(&mut dd);
        for old in self.hands.keys() {
            let row = descriptions.get(old).ok_or_else(|| {
                error(
                    &description,
                    format!("No output row for selected DATA_DESC_ID {old}."),
                )
            })?;
            metadata
                .set_i32(*row as usize, "POLARIZATION_ID", stokes as i32)
                .map_err(|source| error(&description, source.to_string()))?;
        }
        dd.save(TableOptions::new(&description))
            .map_err(|source| error(&description, source.to_string()))?;
        Ok(())
    }

    pub(super) fn apply(
        &self,
        block: &mut MainArrayBlock,
        (rows, row_flags): (&[i32], &[Option<ScalarValue>]),
        channel_sources: (
            &BTreeMap<i32, i32>,
            &BTreeMap<i32, TransformChannelSelection>,
        ),
        spectra: &[&str],
    ) -> Result<(), TableError> {
        let (windows, channels) = channel_sources;
        let weight_index = spectra.iter().position(|&name| name == "WEIGHT_SPECTRUM");
        let sigma_index = spectra.iter().position(|&name| name == "SIGMA_SPECTRUM");
        for (index, description) in rows.iter().enumerate() {
            let Some(&hands) = self.hands.get(description) else {
                continue;
            };
            let Some(ArrayValue::Bool(flags)) = &block.flags[index] else {
                return Err(invalid("flags"));
            };
            let flags = flags
                .view()
                .into_dimensionality::<Ix2>()
                .map_err(|_| invalid("flags"))?;
            if hands.into_iter().any(|axis| axis >= flags.nrows()) {
                return Err(invalid("parallel hands"));
            }
            let Some(ArrayValue::Float32(weight)) = &block.weights[index] else {
                return Err(invalid("scalar weights"));
            };
            let weight = weight
                .as_slice()
                .filter(|weight| weight.len() == flags.nrows())
                .ok_or_else(|| invalid("scalar weights"))?;
            let spectral = weight_index.and_then(|column| block.spectra[column][index].as_ref());
            let spectral_weight = match spectral {
                Some(ArrayValue::Float32(values)) if values.is_empty() => None,
                None => None,
                Some(ArrayValue::Float32(values)) if values.shape() == flags.shape() => Some(
                    values
                        .view()
                        .into_dimensionality::<Ix2>()
                        .map_err(|_| invalid("weight spectra"))?,
                ),
                _ => return Err(invalid("weight spectra")),
            };
            let projection = Projection {
                hands,
                flags,
                spectral_weight,
                weight: hands.map(|i| weight[i]),
                mode: self.mode,
                flag_row: matches!(row_flags[index], Some(ScalarValue::Bool(true))),
            };
            for column in &mut block.visibility {
                column[index] = column[index]
                    .take()
                    .map(|value| match value {
                        ArrayValue::Float32(data) => projection.data(data).map(ArrayValue::Float32),
                        ArrayValue::Complex32(data) => {
                            projection.data(data).map(ArrayValue::Complex32)
                        }
                        _ => Err(invalid("visibility")),
                    })
                    .transpose()?;
            }
            let flags = projection.flags();
            let projected = projection.spectral_weight.map(|values| {
                Array2::from_shape_fn((1, values.ncols()), |(_, channel)| {
                    self.mode
                        .weight(values[[hands[0], channel]], values[[hands[1], channel]])
                })
                .into_dyn()
            });
            let mut weight = projection.projected_scalar_weight();
            let mut sigma = sigma_from_weight(weight);
            if let Some(projected) = &projected {
                let selected = &channels[&windows[description]].channels;
                (weight, sigma) = spectrum_statistics(projected, &flags, selected);
                if let Some(column) = sigma_index {
                    block.spectra[column][index] =
                        Some(ArrayValue::Float32(projected.mapv(sigma_from_weight)));
                }
            } else if let Some(column) = sigma_index {
                block.spectra[column][index] = None;
            }
            if let Some(column) = weight_index {
                block.spectra[column][index] = projected.map(ArrayValue::Float32);
            }
            block.weights[index] = Some(ArrayValue::Float32(
                Array1::from_vec(vec![weight]).into_dyn(),
            ));
            block.sigmas[index] = Some(ArrayValue::Float32(
                Array1::from_vec(vec![sigma]).into_dyn(),
            ));
            block.flags[index] = Some(ArrayValue::Bool(flags));
            if let Some(ArrayValue::Bool(values)) = &mut block.categories[index]
                && !values.is_empty()
            {
                let category = values
                    .view()
                    .into_dimensionality::<Ix3>()
                    .map_err(|_| invalid("flag categories"))?;
                if hands.into_iter().any(|axis| axis >= category.shape()[0]) {
                    return Err(invalid("flag categories"));
                }
                let shape = (1, category.shape()[1], category.shape()[2]);
                *values = Array3::from_shape_fn(shape, |(_, channel, category_id)| {
                    category[[hands[0], channel, category_id]]
                        && category[[hands[1], channel, category_id]]
                })
                .into_dyn();
            }
        }
        Ok(())
    }
}

/// Return the first Stokes I row in a private staged POLARIZATION table,
/// appending one if needed. Preserve existing rows and widen fixed shapes.
///
/// # Errors
/// Returns a metadata error if the staged table cannot be read or replaced.
pub fn ensure_stokes_i_polarization(path: &Path) -> Result<usize, MsTransformError> {
    const COLUMNS: [&str; 2] = ["CORR_TYPE", "CORR_PRODUCT"];
    let error = |reason: String| MsTransformError::SpectralMetadata {
        path: path.display().to_string(),
        reason,
    };
    let mut table =
        Table::open(TableOptions::new(path)).map_err(|source| error(source.to_string()))?;
    let rows = MsPolarization::new(&table);
    let stokes = Correlation::I.casacore_code();
    for row in 0..rows.row_count() {
        if !rows
            .flag_row(row)
            .map_err(|source| error(source.to_string()))?
            && rows
                .corr_type(row)
                .map_err(|source| error(source.to_string()))?
                == [stokes]
        {
            return Ok(row);
        }
    }
    let row = table.row_count();
    let fixed = COLUMNS.iter().any(|name| {
        table
            .schema()
            .and_then(|schema| schema.column(name))
            .is_some_and(|column| {
                matches!(
                    column.column_type(),
                    ColumnType::Array(ArrayShapeContract::Fixed { .. })
                )
            })
    });
    let temporary = fixed
        .then(|| tempfile::tempdir_in(path.parent().unwrap_or_else(|| Path::new("."))))
        .transpose()
        .map_err(|source| error(source.to_string()))?;
    let widened = temporary
        .as_ref()
        .map(|directory| directory.path().join("polarization"));
    let write_to = widened.as_deref().unwrap_or(path);
    if fixed {
        let indices = (0..row).collect::<Vec<_>>();
        rewrite_table_rows_with_variable_arrays(path, write_to, &indices, &COLUMNS, |_, row| {
            Ok(row)
        })?;
        table =
            Table::open(TableOptions::new(write_to)).map_err(|source| error(source.to_string()))?;
    }
    table
        .add_row(row_from_defs(
            schema::polarization::REQUIRED_COLUMNS,
            &[
                ("NUM_CORR", i(1)),
                ("CORR_TYPE", i32_array(&[stokes], vec![1])),
                ("CORR_PRODUCT", i32_array(&[0, 0], vec![2, 1])),
                ("FLAG_ROW", b(false)),
            ],
        ))
        .map_err(|source| error(source.to_string()))?;
    table
        .save(TableOptions::new(write_to))
        .map_err(|source| error(source.to_string()))?;
    drop(table);
    if let Some(widened) = widened {
        fs::remove_dir_all(path)
            .and_then(|()| fs::rename(widened, path))
            .map_err(|source| error(source.to_string()))?;
    }
    // A copied casacore lock still describes the table before it gained a row.
    if let Err(source) = fs::remove_file(path.join("table.lock"))
        && source.kind() != ErrorKind::NotFound
    {
        return Err(error(source.to_string()));
    }
    Ok(row)
}

fn sigma_from_weight(weight: f32) -> f32 {
    weight.sqrt().recip()
}

fn spectrum_statistics(
    weights: &ArrayD<f32>,
    flags: &ArrayD<bool>,
    channels: &[usize],
) -> (f32, f32) {
    let all_flagged = channels.iter().all(|&channel| flags[[0, channel]]);
    let mut count = 0;
    let mut weight = 0.;
    let mut sigma = 0.;
    for &channel in channels {
        if all_flagged || !flags[[0, channel]] {
            let value = weights[[0, channel]];
            count += 1;
            weight += value;
            sigma += sigma_from_weight(value);
        }
    }
    if count == 0 {
        (0., -1.)
    } else {
        (weight / count as f32, sigma / count as f32)
    }
}

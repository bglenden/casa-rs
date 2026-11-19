// SPDX-License-Identifier: LGPL-3.0-or-later
//! Read and average the selected MAIN arrays for one output baseline row.

use std::collections::BTreeMap;
use std::path::Path;

use casa_tables::{Table, TableError};
use casa_types::{ArrayValue, ScalarValue};
use ndarray::Array1;

use super::channel_average::SpectrumCells;
use super::correlation_metadata::select_visibility_axes;
use super::main_arrays::MainArrayBlock;
use super::time_average::{
    AveragedTimeVisibilities, TimeCoordinates, TimeCoordinatesAverage, TimeGroup, TimeInputRow,
    TimeVisibilityAverage, TimeVisibilityRow, uvw_coordinates,
};
use super::visibility_columns::VisibilityColumns;
use super::{
    MsTransformError, missing_column_error, scalar_f64, select_channels,
    transform_flag_category_channels,
};
use crate::schema::main_table::OptionalMainColumn;

pub(super) const TIME_OUTPUT_SCALARS: [&str; 5] =
    ["TIME", "TIME_CENTROID", "INTERVAL", "EXPOSURE", "FLAG_ROW"];
const TIME_INPUT_SCALARS: [&str; 4] = ["TIME", "TIME_CENTROID", "INTERVAL", "EXPOSURE"];

pub(super) struct TimeProjection<'a> {
    table: &'a Table,
    input_path: &'a Path,
    spectra: &'a [&'static str],
    visibility: &'a VisibilityColumns,
    available: BTreeMap<(usize, &'static str), bool>,
    rows_per_block: usize,
}

pub(super) struct AveragedMainRow {
    pub(super) data: AveragedTimeVisibilities,
    pub(super) coordinates: TimeCoordinates,
    pub(super) categories: Option<ArrayValue>,
}

impl<'a> TimeProjection<'a> {
    pub(super) fn new(
        table: &'a Table,
        input_path: &'a Path,
        spectra: &'a [&'static str],
        visibility: &'a VisibilityColumns,
        rows_per_block: usize,
    ) -> Self {
        Self {
            table,
            input_path,
            spectra,
            visibility,
            available: BTreeMap::new(),
            rows_per_block,
        }
    }

    pub(super) fn average_group(
        &mut self,
        group: &TimeGroup,
        channels: &[usize],
        axes: Option<&[usize]>,
    ) -> Result<AveragedMainRow, MsTransformError> {
        let mut visibilities = None;
        let mut coordinates = None;
        let mut category = None;
        let mut category_bins = None;
        for time_rows in group.rows.chunks(self.rows_per_block.max(1)) {
            let rows = time_rows.iter().map(|row| row.physical).collect::<Vec<_>>();
            let mut raw = MainArrayBlock::read(self.table, &rows, self.visibility, self.spectra)
                .map_err(|source| self.table_error(source))?;
            let scalars = TIME_INPUT_SCALARS.map(|name| {
                self.table
                    .column_accessor(name)?
                    .scalar_cells_owned_for_rows(&rows)
            });
            let [times, centroids, intervals, exposures] = scalars;
            let times = times.map_err(|source| self.table_error(source))?;
            let centroids = centroids.map_err(|source| self.table_error(source))?;
            let intervals = intervals.map_err(|source| self.table_error(source))?;
            let exposures = exposures.map_err(|source| self.table_error(source))?;
            for (i, &row) in time_rows.iter().enumerate() {
                let require = |value: Option<ArrayValue>, column| {
                    value.ok_or_else(|| missing_column_error(self.input_path, row.physical, column))
                };
                let select = |value, name, on_channels| {
                    select_projection(value, name, channels, axes, on_channels)
                        .map_err(|source| self.table_error(source))
                };
                let flags = select(require(raw.flags[i].take(), "FLAG")?, "FLAG", true)?;
                let weight = select(require(raw.weights[i].take(), "WEIGHT")?, "WEIGHT", false)?;
                let sigma = select(require(raw.sigmas[i].take(), "SIGMA")?, "SIGMA", false)?;
                let values = self
                    .visibility
                    .copies()
                    .iter()
                    .zip(&mut raw.visibility)
                    .map(|(copy, cells)| {
                        cells[i]
                            .take()
                            .map(|value| {
                                select(
                                    value,
                                    copy.source.name(),
                                    copy.source != OptionalMainColumn::LagData,
                                )
                            })
                            .transpose()
                    })
                    .collect::<Result<Vec<_>, _>>()?;
                let weight_spectrum =
                    self.select_spectrum(&mut raw, row, i, "WEIGHT_SPECTRUM", channels, axes)?;
                let sigma_spectrum =
                    self.select_spectrum(&mut raw, row, i, "SIGMA_SPECTRUM", channels, axes)?;
                let &[correlations, channel_count] = flags.shape() else {
                    return Err(self.table_error(TableError::Schema(
                        "FLAG must have correlation and channel axes".to_string(),
                    )));
                };
                let average = visibilities.get_or_insert_with(|| {
                    TimeVisibilityAverage::new(self.visibility, (correlations, channel_count))
                });
                let (flag_row, row_weight) = average
                    .add(TimeVisibilityRow {
                        values: &values,
                        flags: &flags,
                        weight: &weight,
                        sigma: &sigma,
                        spectra: SpectrumCells {
                            weight: weight_spectrum.as_ref(),
                            sigma: sigma_spectrum.as_ref(),
                        },
                    })
                    .map_err(|source| self.table_error(source))?;
                let uvw = uvw_coordinates(raw.uvw[i].as_ref())
                    .ok_or_else(|| missing_column_error(self.input_path, row.physical, "UVW"))?;
                let position = TimeCoordinates {
                    time: scalar_f64(times[i].as_ref(), "TIME", row.physical)?,
                    centroid: scalar_f64(centroids[i].as_ref(), "TIME_CENTROID", row.physical)?,
                    interval: scalar_f64(intervals[i].as_ref(), "INTERVAL", row.physical)?,
                    exposure: scalar_f64(exposures[i].as_ref(), "EXPOSURE", row.physical)?,
                    flag_row,
                    uvw,
                };
                coordinates
                    .get_or_insert_with(|| TimeCoordinatesAverage::new(position))
                    .add(position, row_weight);
                if let Some(cells) = raw.categories[i].take().filter(|value| !value.is_empty()) {
                    let bins = category_bins.get_or_insert_with(|| {
                        channels
                            .iter()
                            .map(|&channel| vec![channel])
                            .collect::<Vec<_>>()
                    });
                    let selected = select_visibility_axes(cells, axes, "FLAG_CATEGORY")
                        .and_then(|cells| transform_flag_category_channels(cells, bins))
                        .map_err(|source| self.table_error(source))?;
                    include_categories(&mut category, selected)
                        .map_err(|source| self.table_error(source))?;
                }
            }
        }
        Ok(AveragedMainRow {
            data: visibilities
                .expect("a time bin contains a baseline row")
                .finish(),
            coordinates: coordinates
                .expect("a time bin contains row coordinates")
                .finish(),
            categories: category,
        })
    }

    fn select_spectrum(
        &mut self,
        raw: &mut MainArrayBlock,
        row: TimeInputRow,
        offset: usize,
        name: &'static str,
        channels: &[usize],
        axes: Option<&[usize]>,
    ) -> Result<Option<ArrayValue>, MsTransformError> {
        let Some(index) = self.spectra.iter().position(|&input| input == name) else {
            return Ok(None);
        };
        let key = (row.buffer_first, name);
        let available = if let Some(&available) = self.available.get(&key) {
            available
        } else {
            let available = self
                .table
                .array_shape_uncached(row.buffer_first, name)
                .map_err(|source| self.table_error(source))?
                .is_some();
            self.available.insert(key, available);
            available
        };
        if !available {
            return Ok(None);
        }
        let value = raw.spectra[index][offset].take().ok_or_else(|| {
            MsTransformError::MissingSpectrumCell {
                path: self.input_path.display().to_string(),
                row: row.physical,
                column: name,
            }
        })?;
        select_projection(value, name, channels, axes, true)
            .map(Some)
            .map_err(|source| self.table_error(source))
    }

    fn table_error(&self, source: TableError) -> MsTransformError {
        MsTransformError::MutateMeasurementSet {
            path: self.input_path.display().to_string(),
            source: Box::new(source),
        }
    }
}

impl AveragedMainRow {
    pub(super) fn materialize(
        self,
        output: &mut MainArrayBlock,
        coordinates: &mut Vec<TimeCoordinates>,
    ) {
        for (column, value) in output.visibility.iter_mut().zip(self.data.values) {
            column.push(value);
        }
        output.flags.push(Some(self.data.flags));
        output.categories.push(self.categories);
        output.uvw.push(Some(ArrayValue::Float64(
            Array1::from_vec(self.coordinates.uvw.to_vec()).into_dyn(),
        )));
        output.weights.push(Some(self.data.weight));
        output.sigmas.push(Some(self.data.sigma));
        output.spectra[0].push(Some(self.data.weight_spectrum));
        output.spectra[1].push(Some(self.data.sigma_spectrum));
        coordinates.push(self.coordinates);
    }
}

impl TimeCoordinates {
    pub(super) fn scalar_cells(self) -> [ScalarValue; 5] {
        [
            ScalarValue::Float64(self.time),
            ScalarValue::Float64(self.centroid),
            ScalarValue::Float64(self.interval),
            ScalarValue::Float64(self.exposure),
            ScalarValue::Bool(self.flag_row),
        ]
    }
}

fn select_projection(
    value: ArrayValue,
    name: &str,
    channels: &[usize],
    axes: Option<&[usize]>,
    on_channels: bool,
) -> Result<ArrayValue, TableError> {
    let value = select_visibility_axes(value, axes, name)?;
    if on_channels && !value.is_empty() {
        select_channels(value, channels)
    } else {
        Ok(value)
    }
}

fn include_categories(
    current: &mut Option<ArrayValue>,
    incoming: ArrayValue,
) -> Result<(), TableError> {
    let Some(ArrayValue::Bool(current)) = current.as_mut() else {
        *current = Some(incoming);
        return Ok(());
    };
    let ArrayValue::Bool(incoming) = incoming else {
        return Err(TableError::Schema(
            "FLAG_CATEGORY must hold Bool".to_string(),
        ));
    };
    if current.shape() != incoming.shape() {
        return Err(TableError::Schema(
            "FLAG_CATEGORY shapes differ during time averaging".to_string(),
        ));
    }
    for (accumulator, &flag) in current.iter_mut().zip(&incoming) {
        *accumulator &= flag;
    }
    Ok(())
}

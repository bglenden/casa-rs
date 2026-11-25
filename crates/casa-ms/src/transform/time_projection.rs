// SPDX-License-Identifier: LGPL-3.0-or-later
//! Read and average the selected MAIN arrays in bounded blocks across output baseline rows.

use std::collections::{BTreeMap, BTreeSet};
use std::path::Path;

use casa_tables::{Table, TableError};
use casa_types::{ArrayValue, ScalarValue};
use ndarray::{Array1, Zip};

use super::channel_average::SpectrumCells;
use super::correlation_metadata::select_visibility_axes;
use super::main_arrays::MainArrayBlock;
use super::time_average::{
    AveragedTimeVisibilities, TimeCoordinates, TimeCoordinatesAverage, TimeGroup, TimeInputRow,
    TimeVisibilityAverage, TimeVisibilityRow, uvw_coordinates,
};
use super::visibility_columns::VisibilityColumns;
use super::{
    MsTransformError, PreparedTransformInputView, STANDARD_SPECTRA, missing_column_error,
    scalar_f64, select_channels, transform_flag_category_channels,
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
    view: Option<&'a dyn PreparedTransformInputView>,
}

pub(super) struct TimeGroupRead<'a> {
    pub(super) group: &'a TimeGroup,
    pub(super) channels: &'a [usize],
    pub(super) axes: Option<&'a [usize]>,
    pub(super) legacy_bins: Option<(&'a [Vec<usize>], usize)>,
}

#[derive(Default)]
struct TimeGroupAccumulator {
    visibility: Option<TimeVisibilityAverage>,
    coordinates: Option<TimeCoordinatesAverage>,
    categories: Option<ArrayValue>,
    category_bins: Option<Vec<Vec<usize>>>,
}

struct AveragedMainRow {
    data: AveragedTimeVisibilities,
    coordinates: TimeCoordinates,
    categories: Option<ArrayValue>,
}

impl<'a> TimeProjection<'a> {
    pub(super) fn new(
        table: &'a Table,
        input_path: &'a Path,
        spectra: &'a [&'static str],
        visibility: &'a VisibilityColumns,
        rows_per_block: usize,
        view: Option<&'a dyn PreparedTransformInputView>,
    ) -> Self {
        Self {
            table,
            input_path,
            spectra,
            visibility,
            available: BTreeMap::new(),
            rows_per_block,
            view,
        }
    }

    pub(super) fn average_groups(
        &mut self,
        plans: &[TimeGroupRead<'_>],
    ) -> Result<(MainArrayBlock, Vec<TimeCoordinates>), MsTransformError> {
        let mut output = MainArrayBlock::for_capacity(
            plans.len(),
            self.visibility.copies().len(),
            STANDARD_SPECTRA.len(),
        );
        let mut coordinates = Vec::with_capacity(plans.len());
        let mut members = plans
            .iter()
            .enumerate()
            .flat_map(|(index, plan)| plan.group.rows.iter().copied().map(move |row| (index, row)));
        let mut active_group = 0;
        let mut accumulator = TimeGroupAccumulator::default();
        loop {
            let block = members
                .by_ref()
                .take(self.rows_per_block.max(1))
                .collect::<Vec<_>>();
            if block.is_empty() {
                break;
            }
            let rows = block
                .iter()
                .map(|(_, row)| row.physical)
                .collect::<Vec<_>>();
            let mut raw =
                MainArrayBlock::read(self.table, &rows, self.visibility, self.spectra, self.view)
                    .map_err(|source| self.table_error(source))?;
            self.cache_spectrum_availability(&block, &raw)?;
            let scalars = TIME_INPUT_SCALARS.map(|name| {
                self.table
                    .column_accessor(name)?
                    .scalar_cells_owned_for_rows(&rows)
            });
            let [times, centroids, intervals, exposures] = scalars;
            let scalars = [
                times.map_err(|source| self.table_error(source))?,
                centroids.map_err(|source| self.table_error(source))?,
                intervals.map_err(|source| self.table_error(source))?,
                exposures.map_err(|source| self.table_error(source))?,
            ];
            for (offset, (group, row)) in block.into_iter().enumerate() {
                if group != active_group {
                    std::mem::take(&mut accumulator)
                        .finish(plans[active_group].group)
                        .materialize(&mut output, &mut coordinates);
                    active_group = group;
                }
                self.add_row(
                    &mut accumulator,
                    &plans[group],
                    row,
                    offset,
                    &mut raw,
                    &scalars,
                )?;
            }
        }
        if !plans.is_empty() {
            accumulator
                .finish(plans[active_group].group)
                .materialize(&mut output, &mut coordinates);
        }
        Ok((output, coordinates))
    }

    fn cache_spectrum_availability(
        &mut self,
        block: &[(usize, TimeInputRow)],
        raw: &MainArrayBlock,
    ) -> Result<(), MsTransformError> {
        if self.spectra.is_empty() {
            return Ok(());
        }
        let offsets = block
            .iter()
            .enumerate()
            .map(|(offset, (_, row))| (row.physical, offset))
            .collect::<BTreeMap<_, _>>();
        for (name, cells) in self.spectra.iter().copied().zip(&raw.spectra) {
            let mut missing = BTreeSet::new();
            for (_, row) in block {
                let first = row.buffer_first;
                if let Some(&offset) = offsets.get(&first) {
                    self.available
                        .insert((first, name), cells[offset].is_some());
                } else if !self.available.contains_key(&(first, name)) {
                    missing.insert(first);
                }
            }
            if missing.is_empty() {
                continue;
            }
            let missing = missing.into_iter().collect::<Vec<_>>();
            let accessor = self
                .table
                .column_accessor(name)
                .map_err(|source| self.table_error(source))?;
            if let Ok(shapes) = accessor.array_shapes_uncached(&missing) {
                for (first, shape) in missing.into_iter().zip(shapes) {
                    self.available.insert((first, name), shape.is_some());
                }
            } else {
                let cells = accessor
                    .array_cells_owned_uncached(&missing)
                    .map_err(|source| self.table_error(source))?;
                for (first, value) in missing.into_iter().zip(cells) {
                    self.available.insert((first, name), value.is_some());
                }
            }
        }
        Ok(())
    }

    fn add_row(
        &self,
        accumulator: &mut TimeGroupAccumulator,
        plan: &TimeGroupRead<'_>,
        row: TimeInputRow,
        offset: usize,
        raw: &mut MainArrayBlock,
        scalars: &[Vec<Option<ScalarValue>>; 4],
    ) -> Result<(), MsTransformError> {
        let TimeGroupRead {
            channels,
            axes,
            group,
            legacy_bins,
        } = *plan;
        let legacy = group.legacy_geometry.is_some();
        let require = |value: Option<ArrayValue>, column| {
            value.ok_or_else(|| missing_column_error(self.input_path, row.physical, column))
        };
        let select = |value, name, on_channels| {
            select_projection(value, name, channels, axes, on_channels)
                .map_err(|source| self.table_error(source))
        };
        let mut flags = select(require(raw.flags[offset].take(), "FLAG")?, "FLAG", true)?;
        let mut weight = select(
            require(raw.weights[offset].take(), "WEIGHT")?,
            "WEIGHT",
            false,
        )?;
        let mut sigma = select(require(raw.sigmas[offset].take(), "SIGMA")?, "SIGMA", false)?;
        let mut values = self
            .visibility
            .copies()
            .iter()
            .zip(&mut raw.visibility)
            .map(|(copy, cells)| {
                cells[offset]
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
        let mut weight_spectrum =
            self.select_spectrum(raw, row, offset, "WEIGHT_SPECTRUM", plan)?;
        let sigma_spectrum = self.select_spectrum(raw, row, offset, "SIGMA_SPECTRUM", plan)?;
        if let Some((bins, width)) = legacy_bins {
            let averages = super::legacy_split::ChannelAverages::new(
                &flags,
                weight_spectrum.as_ref(),
                bins,
                false,
            )
            .map_err(|error| self.table_error(error))?;
            for (column, value) in self.visibility.copies().iter().zip(&mut values) {
                if column.source != OptionalMainColumn::LagData
                    && let Some(cell) = value.take()
                {
                    *value = Some(
                        averages
                            .visibility(cell)
                            .map_err(|error| self.table_error(error))?,
                    );
                }
            }
            let projected = averages.weight_spectrum();
            flags = averages.flags();
            weight_spectrum = projected;
            super::legacy_split::scale_channel_noise(
                self.visibility,
                width,
                &mut weight,
                &mut sigma,
            )
            .map_err(|error| self.table_error(error))?;
        }
        let &[correlations, channel_count] = flags.shape() else {
            return Err(self.table_error(TableError::Schema(
                "FLAG must have correlation and channel axes".to_string(),
            )));
        };
        let average = accumulator.visibility.get_or_insert_with(|| {
            TimeVisibilityAverage::new(self.visibility, (correlations, channel_count), legacy)
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
        let uvw = uvw_coordinates(raw.uvw[offset].as_ref())
            .ok_or_else(|| missing_column_error(self.input_path, row.physical, "UVW"))?;
        let position = TimeCoordinates {
            time: scalar_f64(scalars[0][offset].as_ref(), "TIME", row.physical)?,
            centroid: scalar_f64(scalars[1][offset].as_ref(), "TIME_CENTROID", row.physical)?,
            interval: scalar_f64(scalars[2][offset].as_ref(), "INTERVAL", row.physical)?,
            exposure: scalar_f64(scalars[3][offset].as_ref(), "EXPOSURE", row.physical)?,
            flag_row,
            uvw,
        };
        accumulator
            .coordinates
            .get_or_insert_with(|| TimeCoordinatesAverage::new(position))
            .add(position, row_weight, legacy);
        if let Some(cells) = raw.categories[offset]
            .take()
            .filter(|value| !value.is_empty())
        {
            let bins = accumulator.category_bins.get_or_insert_with(|| {
                if let Some((bins, _)) = legacy_bins {
                    bins.iter()
                        .map(|bin| bin.iter().map(|&local| channels[local]).collect())
                        .collect()
                } else {
                    channels.iter().map(|&channel| vec![channel]).collect()
                }
            });
            let selected = select_visibility_axes(cells, axes, "FLAG_CATEGORY")
                .and_then(|cells| transform_flag_category_channels(cells, bins))
                .map_err(|source| self.table_error(source))?;
            include_categories(&mut accumulator.categories, selected)
                .map_err(|source| self.table_error(source))?;
        }
        Ok(())
    }

    fn select_spectrum(
        &self,
        raw: &mut MainArrayBlock,
        row: TimeInputRow,
        offset: usize,
        name: &'static str,
        plan: &TimeGroupRead<'_>,
    ) -> Result<Option<ArrayValue>, MsTransformError> {
        let Some(index) = self.spectra.iter().position(|&input| input == name) else {
            return Ok(None);
        };
        if !self.available[&(row.buffer_first, name)] {
            return Ok(None);
        }
        let value = raw.spectra[index][offset].take().ok_or_else(|| {
            MsTransformError::MissingSpectrumCell {
                path: self.input_path.display().to_string(),
                row: row.physical,
                column: name,
            }
        })?;
        select_projection(value, name, plan.channels, plan.axes, true)
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

impl TimeGroupAccumulator {
    fn finish(self, group: &TimeGroup) -> AveragedMainRow {
        AveragedMainRow {
            data: self
                .visibility
                .expect("a time bin contains a baseline row")
                .finish(),
            coordinates: self
                .coordinates
                .expect("a time bin contains row coordinates")
                .finish(group),
            categories: self.categories,
        }
    }
}

impl AveragedMainRow {
    fn materialize(self, output: &mut MainArrayBlock, coordinates: &mut Vec<TimeCoordinates>) {
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
    Zip::from(current)
        .and(&incoming)
        .for_each(|accumulator, &flag| *accumulator &= flag);
    Ok(())
}

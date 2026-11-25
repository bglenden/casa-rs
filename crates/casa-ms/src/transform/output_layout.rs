// SPDX-License-Identifier: LGPL-3.0-or-later
//! Output storage geometry used by the CASA MeasurementSet transform.

use std::collections::{BTreeMap, BTreeSet};

use casa_tables::{StreamedTiledShapeValueType as ValueType, Table, TableOptions};
use casa_types::{ArrayValue, ScalarValue, Value};

use crate::MeasurementSet;
use crate::ms::{casa_interferometer_count, casa_visibility_tile_shape};
use crate::selection::SpwSelection;
use crate::write_session::MeasurementSetArrayColumnPlan;

use super::visibility_columns::VisibilityColumns;
use super::{
    CorrelationSelection, MsTransformError, MsTransformTiling, TransformChannelSelection,
    selected_spw_order,
};

#[derive(Clone, Copy)]
pub(super) struct OutputTiling {
    visibility: [usize; 3],
    scalar_noise: [usize; 2],
    uvw: [usize; 2],
    fixed_visibility: bool,
}

pub(super) fn resolve_tiling(
    input: &MeasurementSet,
    mode: MsTransformTiling,
    (spw, tile_window): (&SpwSelection, Option<i32>),
    (channels, widths): (
        &BTreeMap<i32, TransformChannelSelection>,
        Option<&BTreeMap<i32, usize>>,
    ),
    (descriptions, ddid_to_spw, correlations): (
        &[i32],
        &BTreeMap<i32, i32>,
        Option<&CorrelationSelection>,
    ),
) -> Result<OutputTiling, MsTransformError> {
    let error = |reason| MsTransformError::Spectrum(reason);
    let visibility = match mode {
        MsTransformTiling::Explicit(tile) | MsTransformTiling::Fixed(tile) => tile,
        MsTransformTiling::Standard | MsTransformTiling::FastMosaic => {
            let id = tile_window
                .or_else(|| selected_spw_order(channels, spw).into_iter().next())
                .ok_or_else(|| error("The tile layout needs a selected spectral window.".into()))?;
            let ddid = descriptions
                .iter()
                .copied()
                .find(|ddid| ddid_to_spw[ddid] == id)
                .or_else(|| {
                    ddid_to_spw
                        .iter()
                        .find_map(|(&ddid, &window)| (window == id).then_some(ddid))
                })
                .ok_or_else(|| error(format!("Spectral window {id} has no data description.")))?;
            // The storage manager's default is taken from the first input
            // channel range, before any spectral regridding or combination.
            let first = channels[&id].ranges[0];
            let count = 1 + (first.stop - first.start) as usize / first.step.max(1) as usize;
            let chan = count.div_ceil(widths.map_or(1, |widths| widths[&id]));
            let corr = match correlations.and_then(|selection| selection.axes(ddid)) {
                Some(axes) => axes.len(),
                None => {
                    let ddi = input.data_description().map_err(|e| error(e.to_string()))?;
                    let pol = ddi
                        .polarization_id(ddid as usize)
                        .map_err(|e| error(e.to_string()))?;
                    let pol_table = input.polarization().map_err(|e| error(e.to_string()))?;
                    pol_table
                        .num_corr(pol as usize)
                        .map_err(|e| error(e.to_string()))? as usize
                }
            };
            if corr == 0 || chan == 0 {
                return Err(error("The tile layout needs a nonempty visibility.".into()));
            }
            let telescope = telescope_name(input);
            if matches!(mode, MsTransformTiling::FastMosaic) {
                let rows = casa_interferometer_count(&telescope);
                [corr, (131_072 / corr / rows).clamp(1, chan), rows]
            } else {
                casa_visibility_tile_shape(corr, chan, &telescope)
                    .try_into()
                    .expect("rank-three tile")
            }
        }
    };
    make_tiling(visibility, matches!(mode, MsTransformTiling::Fixed(_)))
}

pub(super) fn partition_tiling(
    input: &MeasurementSet,
    visibility: &VisibilityColumns,
    channels: &BTreeMap<i32, TransformChannelSelection>,
    descriptions: &[i32],
) -> Result<OutputTiling, MsTransformError> {
    let failure = |error: casa_tables::TableError| MsTransformError::Spectrum(error.to_string());
    let main = input.main_table();
    let column = if main
        .schema()
        .is_some_and(|schema| schema.contains_column("DATA"))
    {
        "DATA"
    } else {
        visibility.copies()[0].source.name()
    };
    let backing = if main.data_manager_info().is_empty() {
        main.simple_reference_root()
            .map_err(failure)?
            .map(|root| Table::open(TableOptions::new(root)))
            .transpose()
            .map_err(failure)?
    } else {
        None
    };
    let layout = backing.as_ref().unwrap_or(main);
    let manager = layout.data_manager_info().iter().find(|manager| {
        manager.dm_type.starts_with("Tiled") && manager.columns.iter().any(|name| name == column)
    });
    if let Some(manager) = manager
        && let Some(spec) = layout.data_manager_spec(manager.seq_nr).map_err(failure)?
        && let Some(Value::Record(cubes)) = spec.get("HYPERCUBES")
    {
        let mut largest = None;
        let mut largest_elements = 0;
        for cube in cubes.fields() {
            let Value::Record(cube) = &cube.value else {
                continue;
            };
            let Some(Value::Array(ArrayValue::Int32(shape))) = cube.get("TileShape") else {
                continue;
            };
            let [corr, chan, rows] = shape.as_slice().unwrap_or(&[]) else {
                continue;
            };
            let dimensions = [*corr, *chan, *rows].map(|axis| usize::try_from(axis).unwrap_or(0));
            let elements = dimensions.into_iter().fold(1usize, usize::saturating_mul);
            if elements > largest_elements {
                largest = Some(dimensions);
                largest_elements = elements;
            }
        }
        if let Some(tile) = largest {
            return make_tiling(tile, true);
        }
    }
    let failure = |error: crate::MsError| MsTransformError::Spectrum(error.to_string());
    let data_descriptions = input.data_description().map_err(failure)?;
    let polarizations = input.polarization().map_err(failure)?;
    let mut correlations = 0;
    for description in descriptions.iter().copied().collect::<BTreeSet<_>>() {
        let polarization = data_descriptions
            .polarization_id(description as usize)
            .map_err(failure)?;
        correlations = correlations.max(
            polarizations
                .num_corr(polarization as usize)
                .map_err(failure)?,
        );
    }
    let channels = channels
        .values()
        .map(|window| window.channels.len())
        .max()
        .unwrap_or(0);
    if correlations <= 0 || channels == 0 {
        return Err(MsTransformError::Spectrum(
            "The tile layout needs a nonempty visibility.".into(),
        ));
    }
    let tile = casa_visibility_tile_shape(correlations as usize, channels, &telescope_name(input));
    make_tiling(tile.try_into().expect("rank-three tile"), true)
}

fn telescope_name(input: &MeasurementSet) -> String {
    match input.observation().ok().and_then(|obs| {
        obs.table()
            .cell_accessor(0, "TELESCOPE_NAME")
            .ok()
            .and_then(|cell| cell.scalar().ok().cloned())
    }) {
        Some(ScalarValue::String(name)) => name,
        _ => String::new(),
    }
}

fn make_tiling(
    visibility: [usize; 3],
    fixed_visibility: bool,
) -> Result<OutputTiling, MsTransformError> {
    let error = |reason| MsTransformError::Spectrum(reason);
    let [corr, chan, rows] = visibility;
    let max_extent = i32::MAX as usize;
    let noise_rows = chan
        .checked_mul(rows)
        .filter(|&n| n <= max_extent)
        .ok_or_else(|| error("The tile shape's channel and row extents are too large.".into()))?;
    let uvw_rows = corr
        .checked_mul(noise_rows)
        .map(|n| n / 3)
        .filter(|&n| n <= max_extent)
        .ok_or_else(|| error("The tile shape's UVW extent is too large.".into()))?;
    Ok(OutputTiling {
        visibility,
        scalar_noise: [corr, noise_rows],
        uvw: [3, uvw_rows],
        fixed_visibility,
    })
}

impl OutputTiling {
    pub(super) fn default_for(self, name: &str) -> Vec<usize> {
        let [corr, chan, rows] = self.visibility;
        match name {
            "WEIGHT" | "SIGMA" => self.scalar_noise.to_vec(),
            "UVW" => self.uvw.to_vec(),
            "FLAG_CATEGORY" => vec![corr, chan, 1, rows],
            _ => self.visibility.to_vec(),
        }
    }

    pub(super) fn apply(self, columns: &mut [MeasurementSetArrayColumnPlan]) {
        for column in columns {
            let default = self.default_for(&column.name);
            let bytes = match column.value_type {
                ValueType::Bool => 1,
                ValueType::Int32 | ValueType::Float32 => 4,
                ValueType::Float64 | ValueType::Complex32 => 8,
            };
            for shape in &mut column.shapes {
                shape.tile_shape = if self.fixed_visibility
                    || matches!(
                        column.name.as_str(),
                        "UVW" | "WEIGHT" | "SIGMA" | "FLAG_CATEGORY"
                    ) {
                    resolve_extensible_tile(&default, &shape.cell_shape)
                } else {
                    // CASA explicitly assigns a 1 MiB visibility tile to
                    // each shape when mstransform writes its first plane.
                    let corr = shape.cell_shape[0];
                    let chan = shape.cell_shape[1];
                    let rows = (1_048_576 / corr / chan / bytes).max(1);
                    vec![corr, chan, rows]
                };
                for (tile, &cell) in shape.tile_shape.iter_mut().zip(&shape.cell_shape) {
                    *tile = (*tile).min(cell);
                }
            }
        }
    }
}

fn resolve_extensible_tile(default: &[usize], cell: &[usize]) -> Vec<usize> {
    let mut tile = default.to_vec();
    let (axes, rows) = tile.split_at_mut(cell.len());
    for (extent, &cell_extent) in axes.iter_mut().zip(cell) {
        *extent = if *extent == 0 {
            cell_extent
        } else {
            (*extent).min(cell_extent)
        };
    }
    if rows[0] == 0 {
        // A zero last extent in TSMCube expresses an extensible axis.
        // Choose the closest row count to the default 32K-element tile.
        let cells = axes.iter().copied().product::<usize>();
        rows[0] = ((32_768 + cells / 2) / cells).max(1);
    }
    tile
}

pub(super) fn pad_ddids(
    output: &std::path::Path,
    offset: usize,
    budget_bytes: usize,
) -> Result<(), MsTransformError> {
    use casa_tables::{Table, TableOptions};
    use casa_types::{RecordValue, ScalarValue, Value};
    use std::fs;

    let path = output.join("DATA_DESCRIPTION");
    let original = output.join(".casa-rs-original-data-description");
    let io_error = |source: std::io::Error| MsTransformError::PrepareOutput {
        path: path.display().to_string(),
        reason: source.to_string(),
    };
    let table_error = |source| MsTransformError::MutateMeasurementSet {
        path: path.display().to_string(),
        source: Box::new(source),
    };
    let table = Table::open(TableOptions::new(&path)).map_err(table_error)?;
    let rows = table.row_count();
    let budget_error = || {
        MsTransformError::Spectrum("ddistart exceeds the available data-description budget.".into())
    };
    let output_rows = rows
        .checked_add(offset)
        .filter(|&rows| rows <= i32::MAX as usize)
        .ok_or_else(budget_error)?;
    let mut remaining = budget_bytes;
    for index in 0..rows {
        let records =
            super::selection_metadata::read_subtable_rows(&table, &[index]).map_err(table_error)?;
        let copies = if index == 0 { offset + 1 } else { 1 };
        // Count variable values as well as the retained row headers and
        // index vector, allowing for source batches and writer staging.
        let row_bytes = records[0]
            .retained_heap_bytes()
            .and_then(|bytes| bytes.checked_add(size_of::<RecordValue>() + size_of::<usize>()))
            .and_then(|bytes| bytes.checked_mul(3))
            .and_then(|bytes| bytes.checked_mul(copies))
            .ok_or_else(budget_error)?;
        remaining = remaining.checked_sub(row_bytes).ok_or_else(budget_error)?;
    }
    drop(table);
    let sources = std::iter::repeat_n(0, offset)
        .chain(0..rows)
        .collect::<Vec<_>>();
    fs::rename(&path, &original).map_err(io_error)?;
    let mut written = 0;
    super::rewrite_selected_table_rows(&original, &path, &sources, |_, mut entry| {
        if written < offset {
            // The offset reserves IDs for callers assembling several
            // products. Flag those entries and keep every MAIN reference
            // valid even if this product is opened on its own.
            entry.upsert("FLAG_ROW", Value::Scalar(ScalarValue::Bool(true)));
        }
        written += 1;
        Ok(entry)
    })?;
    debug_assert_eq!(written, output_rows);
    fs::remove_dir_all(original).map_err(io_error)
}

#[cfg(test)]
mod tests {
    use casa_tables::{ColumnSchema, Table, TableOptions, TableSchema};
    use casa_types::{ArrayValue, PrimitiveType, RecordField, RecordValue, ScalarValue, Value};

    #[test]
    fn ddi_start_budgets_repeated_optional_array_payload() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("DATA_DESCRIPTION");
        let schema = TableSchema::new(vec![
            ColumnSchema::scalar("FLAG_ROW", PrimitiveType::Bool),
            ColumnSchema::array_variable("EXTRA", PrimitiveType::Int32, Some(1)),
        ])
        .unwrap();
        let mut table = Table::with_schema(schema);
        table
            .add_row(RecordValue::new(vec![
                RecordField::new("FLAG_ROW", Value::Scalar(ScalarValue::Bool(false))),
                RecordField::new(
                    "EXTRA",
                    Value::Array(ArrayValue::from_i32_vec(vec![7; 1024])),
                ),
            ]))
            .unwrap();
        table.save(TableOptions::new(&path)).unwrap();

        let error = super::pad_ddids(directory.path(), 100, 65_536).unwrap_err();
        assert!(error.to_string().contains("data-description budget"));
        assert_eq!(
            Table::open(TableOptions::new(&path)).unwrap().row_count(),
            1
        );
        super::pad_ddids(directory.path(), 1, 65_536).unwrap();
        assert_eq!(
            Table::open(TableOptions::new(&path)).unwrap().row_count(),
            2
        );
    }
}

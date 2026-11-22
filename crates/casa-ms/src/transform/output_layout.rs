// SPDX-License-Identifier: LGPL-3.0-or-later
//! Output storage geometry used by the CASA MeasurementSet transform.

use std::collections::BTreeMap;

use casa_tables::StreamedTiledShapeValueType as ValueType;

use crate::MeasurementSet;
use crate::ms::{casa_interferometer_count, casa_visibility_tile_shape};
use crate::selection::SpwSelection;
use crate::write_session::MeasurementSetArrayColumnPlan;

use super::{
    CorrelationSelection, MsTransformError, MsTransformTiling, TransformChannelSelection,
    selected_spw_order,
};

#[derive(Clone, Copy)]
pub(super) struct OutputTiling {
    visibility: [usize; 3],
    scalar_noise: [usize; 2],
    uvw: [usize; 2],
}

pub(super) fn resolve_tiling(
    input: &MeasurementSet,
    mode: MsTransformTiling,
    spw: &SpwSelection,
    (channels, widths): (
        &BTreeMap<i32, TransformChannelSelection>,
        &BTreeMap<i32, usize>,
    ),
    (descriptions, ddid_to_spw, correlations): (
        &[i32],
        &BTreeMap<i32, i32>,
        Option<&CorrelationSelection>,
    ),
) -> Result<OutputTiling, MsTransformError> {
    let error = |reason| MsTransformError::Spectrum(reason);
    let visibility = match mode {
        MsTransformTiling::Explicit(tile) => tile,
        MsTransformTiling::Standard | MsTransformTiling::FastMosaic => {
            let id = selected_spw_order(channels, spw)
                .into_iter()
                .next()
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
            let chan = channels[&id].channels.len().div_ceil(widths[&id]);
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
            let telescope = input.observation().ok().and_then(|table| {
                table
                    .table()
                    .cell_accessor(0, "TELESCOPE_NAME")
                    .ok()
                    .and_then(|cell| cell.scalar().ok().cloned())
            });
            let telescope = match telescope.as_ref() {
                Some(casa_types::ScalarValue::String(value)) => value.as_str(),
                _ => "",
            };
            if matches!(mode, MsTransformTiling::FastMosaic) {
                let rows = casa_interferometer_count(telescope);
                [corr, (131_072 / corr / rows).clamp(1, chan), rows]
            } else {
                casa_visibility_tile_shape(corr, chan, telescope)
                    .try_into()
                    .expect("rank-three tile")
            }
        }
    };
    let [corr, chan, rows] = visibility;
    let max_extent = i32::MAX as usize;
    let noise_rows = chan
        .checked_mul(rows)
        .filter(|&n| n > 0 && n <= max_extent)
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
                shape.tile_shape = match column.name.as_str() {
                    "UVW" if self.uvw[1] == 0 => vec![3, 10_923],
                    "UVW" | "WEIGHT" | "SIGMA" | "FLAG_CATEGORY" => default.clone(),
                    // CASA explicitly assigns a 1 MiB visibility tile to
                    // each shape when the first plane of that shape is written.
                    _ => {
                        let corr = shape.cell_shape[0];
                        let chan = shape.cell_shape[1];
                        let rows = (1_048_576 / corr / chan / bytes).max(1);
                        vec![corr, chan, rows]
                    }
                };
                for (tile, &cell) in shape.tile_shape.iter_mut().zip(&shape.cell_shape) {
                    *tile = (*tile).min(cell);
                }
            }
        }
    }
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
    let rows = Table::open(TableOptions::new(&path))
        .map_err(|source| MsTransformError::MutateMeasurementSet {
            path: path.display().to_string(),
            source: Box::new(source),
        })?
        .row_count();
    let output_rows = rows
        .checked_add(offset)
        .filter(|&rows| {
            rows <= i32::MAX as usize
                && rows <= budget_bytes / (std::mem::size_of::<RecordValue>() * 4)
        })
        .ok_or_else(|| {
            MsTransformError::Spectrum(
                "ddistart exceeds the available data-description budget.".into(),
            )
        })?;
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

// SPDX-License-Identifier: LGPL-3.0-or-later

use super::*;

pub(super) struct TransformArrayDimensions {
    pub(super) input_correlations: usize,
    pub(super) input_channels: usize,
    pub(super) output_correlations: usize,
    pub(super) output_channels: usize,
    pub(super) output_parts: Vec<usize>,
}

pub(super) fn transform_array_dimensions(
    input: &MeasurementSet,
    selected_ddids: &[i32],
    ddid_to_spw: &BTreeMap<i32, i32>,
    channel_bins: &BTreeMap<i32, Vec<Vec<usize>>>,
    correlations: Option<&CorrelationSelection>,
    polarization_projection: Option<&PolarizationProjection>,
) -> Result<BTreeMap<i32, TransformArrayDimensions>, MsTransformError> {
    let input_path = input.path().unwrap_or_else(|| Path::new("<memory>"));
    let invalid = |reason| MsTransformError::SpectralMetadata {
        path: input_path.display().to_string(),
        reason,
    };
    let data_description = input
        .data_description()
        .map_err(|error| invalid(error.to_string()))?;
    let polarization = input
        .polarization()
        .map_err(|error| invalid(error.to_string()))?;
    let spectral_window = input
        .spectral_window()
        .map_err(|error| invalid(error.to_string()))?;
    let mut dimensions = BTreeMap::new();
    for &ddid in selected_ddids {
        let std::collections::btree_map::Entry::Vacant(entry) = dimensions.entry(ddid) else {
            continue;
        };
        let row =
            usize::try_from(ddid).map_err(|_| invalid(format!("negative DATA_DESC_ID {ddid}")))?;
        let pol_id = data_description
            .polarization_id(row)
            .map_err(|error| invalid(format!("read DATA_DESCRIPTION row {ddid}: {error}")))?;
        let pol_row = usize::try_from(pol_id).map_err(|_| {
            invalid(format!(
                "DATA_DESCRIPTION row {ddid} has negative POLARIZATION_ID {pol_id}"
            ))
        })?;
        let input_correlations = usize::try_from(
            polarization
                .num_corr(pol_row)
                .map_err(|error| invalid(format!("read POLARIZATION row {pol_id}: {error}")))?,
        )
        .map_err(|_| invalid(format!("POLARIZATION row {pol_id} has negative NUM_CORR")))?;
        let spw = ddid_to_spw.get(&ddid).copied().ok_or_else(|| {
            invalid(format!(
                "DATA_DESC_ID {ddid} has no spectral-window mapping"
            ))
        })?;
        let output_channels = channel_bins
            .get(&spw)
            .map(Vec::len)
            .ok_or_else(|| invalid(format!("spectral window {spw} has no output channel bins")))?;
        let input_channels = usize::try_from(
            spectral_window
                .num_chan(spw as usize)
                .map_err(|error| invalid(format!("read SPECTRAL_WINDOW row {spw}: {error}")))?,
        )
        .map_err(|_| invalid(format!("SPECTRAL_WINDOW row {spw} has negative NUM_CHAN")))?;
        entry.insert(TransformArrayDimensions {
            input_correlations,
            input_channels,
            output_correlations: if polarization_projection.is_some_and(|plan| plan.contains(ddid))
            {
                1
            } else {
                correlations
                    .and_then(|plan| plan.axes(ddid))
                    .map_or(input_correlations, <[usize]>::len)
            },
            output_channels,
            output_parts: vec![output_channels],
        });
    }
    Ok(dimensions)
}

pub(super) struct TransformArrayRows<'a> {
    pub(super) selected_rows: &'a [usize],
    pub(super) selected_ddids: &'a [i32],
    pub(super) groups: Option<&'a [time_average::TimeGroup]>,
    pub(super) output_rows: usize,
    pub(super) output_schema: &'a Table,
    pub(super) combined: Option<CombinedArrayRows<'a>>,
    pub(super) regrid: Option<&'a regrid::Plan>,
    pub(super) tile: Option<output_layout::OutputTiling>,
}

#[derive(Clone, Copy)]
pub(super) struct CombinedArrayRows<'a> {
    pub(super) grid: &'a combine::Grid,
    pub(super) groups: &'a [combine::Group],
    pub(super) source_rows: &'a [usize],
    pub(super) source_ddids: &'a [i32],
    pub(super) source_time_groups: Option<&'a [time_average::TimeGroup]>,
}

pub(super) fn transform_array_write_plan(
    input: &MeasurementSet,
    TransformArrayRows {
        selected_rows,
        selected_ddids,
        groups,
        output_rows,
        output_schema,
        combined,
        regrid,
        tile,
    }: TransformArrayRows<'_>,
    ddid_to_spw: &BTreeMap<i32, i32>,
    channel_bins: &BTreeMap<i32, Vec<Vec<usize>>>,
    (correlations, polarization): (
        Option<&CorrelationSelection>,
        Option<&PolarizationProjection>,
    ),
    (visibility, spectra, view_bytes): (&VisibilityColumns, &SpectrumProjection, usize),
    resources: MeasurementSetWriteResources,
) -> Result<(MeasurementSetWritePlan, usize), MsTransformError> {
    let input_path = input.path().unwrap_or_else(|| Path::new("<memory>"));
    let mut dimensions = transform_array_dimensions(
        input,
        combined.map_or(selected_ddids, |rows| rows.source_ddids),
        ddid_to_spw,
        channel_bins,
        correlations,
        polarization,
    )?;
    if let Some(CombinedArrayRows { grid, .. }) = combined {
        for dimension in dimensions.values_mut() {
            dimension.output_channels = grid.frequencies.len();
            dimension.output_parts = vec![dimension.output_channels];
        }
    }
    if let Some(regrid) = regrid {
        for (&ddid, dimension) in &mut dimensions {
            let spw = ddid_to_spw[&ddid];
            dimension.output_channels = regrid.channel_count(spw);
            dimension.output_parts = regrid
                .parts(spw)
                .ranges()
                .iter()
                .map(|part| part.len())
                .collect();
        }
    }
    let mut shape_counts = BTreeMap::<Vec<usize>, usize>::new();
    let mut weight_shape_counts = BTreeMap::<Vec<usize>, usize>::new();
    for (&row, &ddid) in selected_rows.iter().zip(selected_ddids) {
        let shape = &dimensions[&ddid];
        let field_channels = regrid.and_then(|plan| plan.field_channel_count(row));
        let parts = field_channels
            .as_ref()
            .map_or(&shape.output_parts[..], std::slice::from_ref);
        for &channels in parts {
            *shape_counts
                .entry(vec![shape.output_correlations, channels])
                .or_default() += 1;
            *weight_shape_counts
                .entry(vec![shape.output_correlations])
                .or_default() += 1;
        }
    }
    let shapes = shape_counts
        .into_iter()
        .map(|(cell_shape, row_count)| MeasurementSetArrayShapePlan {
            tile_shape: casa_ms::casa_visibility_tile_shape(cell_shape[0], cell_shape[1], ""),
            cell_shape,
            row_count,
        })
        .collect::<Vec<_>>();
    let weight_shapes = weight_shape_counts
        .into_iter()
        .map(|(cell_shape, row_count)| {
            let visibility_tile = casa_ms::casa_visibility_tile_shape(cell_shape[0], 1, "");
            MeasurementSetArrayShapePlan {
                tile_shape: casa_ms::casa_weight_tile_shape(&visibility_tile),
                cell_shape,
                row_count,
            }
        })
        .collect::<Vec<_>>();
    let metadata_budget = MsSelectionIoBudget {
        available_bytes: resources.available_bytes,
        maximum_live_blocks: resources.maximum_live_batches,
        requested_bytes_per_row: std::mem::size_of::<Option<Vec<usize>>>()
            + 5 * std::mem::size_of::<usize>(),
        storage_alignment_rows: None,
    };
    let (flag_category_shapes, largest_input_category) = plan_flag_category_shapes(
        input,
        selected_rows,
        selected_ddids,
        &dimensions,
        correlations.is_some() || polarization.is_some(),
        (groups, metadata_budget),
        (combined, regrid),
    )?;
    let io_error = |error: MsSelectionError| MsTransformError::WriteMeasurementSet {
        path: input_path.display().to_string(),
        reason: error.to_string(),
    };
    let (lag_shapes, maximum_lag_bytes) = if visibility.contains_output("LAG_DATA") {
        plan_lag_shapes(
            input,
            selected_rows,
            selected_ddids,
            &dimensions,
            (groups, metadata_budget),
        )?
    } else {
        (Vec::new(), 0)
    };
    let (source_count, source_time_groups) = combined
        .map_or((selected_rows.len(), groups), |rows| {
            (rows.source_ddids.len(), rows.source_time_groups)
        });
    let raw_row_count = source_time_groups.map_or(source_count, |groups| {
        groups.iter().map(|group| group.rows.len()).sum()
    });
    let input_batch_rows = transform_input_batch_rows(
        raw_row_count,
        resources,
        dimensions.values(),
        spectra.source.len(),
        visibility
            .regular_bytes_per_sample()
            .saturating_add(view_bytes),
        largest_input_category.saturating_add(maximum_lag_bytes),
    )
    .map_err(io_error)?;
    let visibility_tile = shapes
        .first()
        .map(|shape| shape.tile_shape.clone())
        .unwrap_or_else(|| casa_ms::casa_visibility_tile_shape(1, 1, ""));
    let mut columns = vec![
        MeasurementSetArrayColumnPlan {
            name: "FLAG".to_string(),
            value_type: StreamedTiledShapeValueType::Bool,
            shapes: shapes.clone(),
            storage_manager: MeasurementSetColumnStorage::TiledShape,
        },
        MeasurementSetArrayColumnPlan {
            name: "FLAG_CATEGORY".to_string(),
            value_type: StreamedTiledShapeValueType::Bool,
            shapes: flag_category_shapes,
            storage_manager: MeasurementSetColumnStorage::TiledShape,
        },
        MeasurementSetArrayColumnPlan {
            name: "UVW".to_string(),
            value_type: StreamedTiledShapeValueType::Float64,
            shapes: vec![MeasurementSetArrayShapePlan {
                cell_shape: vec![3],
                row_count: shapes.iter().map(|shape| shape.row_count).sum(),
                tile_shape: casa_ms::casa_uvw_tile_shape(&visibility_tile),
            }],
            storage_manager: MeasurementSetColumnStorage::TiledColumn,
        },
        MeasurementSetArrayColumnPlan {
            name: "WEIGHT".to_string(),
            value_type: StreamedTiledShapeValueType::Float32,
            shapes: weight_shapes.clone(),
            storage_manager: MeasurementSetColumnStorage::TiledShape,
        },
        MeasurementSetArrayColumnPlan {
            name: "SIGMA".to_string(),
            value_type: StreamedTiledShapeValueType::Float32,
            shapes: weight_shapes,
            storage_manager: MeasurementSetColumnStorage::TiledShape,
        },
    ];
    for &name in &spectra.output {
        columns.push(MeasurementSetArrayColumnPlan {
            name: name.to_string(),
            value_type: StreamedTiledShapeValueType::Float32,
            shapes: shapes.clone(),
            storage_manager: MeasurementSetColumnStorage::TiledShape,
        });
    }
    let independent_lag = !regrid.is_some_and(regrid::Plan::is_legacy);
    for column in visibility.copies() {
        let shapes = if independent_lag && column.source == OptionalMainColumn::LagData {
            lag_shapes.clone()
        } else {
            shapes.clone()
        };
        columns.push(MeasurementSetArrayColumnPlan {
            name: column.output.name().to_string(),
            value_type: column.value_type(),
            shapes,
            storage_manager: MeasurementSetColumnStorage::TiledShape,
        });
    }
    if output_rows == 0 {
        columns.clear();
    }
    if let Some(tile) = tile {
        tile.apply(&mut columns);
    }
    let writer = MeasurementSetWritePlan::variable_array_creation(
        output_rows,
        columns,
        transform_scalar_column_plans(output_schema),
        resources,
    )
    .map_err(|error| MsTransformError::WriteMeasurementSet {
        path: input_path.display().to_string(),
        reason: error.to_string(),
    })?;
    let writer = if let Some(tile) = tile {
        writer.with_array_default_tiles(|name| tile.default_for(name))
    } else {
        writer
    };
    Ok((writer, input_batch_rows))
}

pub(super) const STANDARD_SPECTRA: [&str; 2] = ["WEIGHT_SPECTRUM", "SIGMA_SPECTRUM"];
pub(super) const MAIN_SPECTRA: [&str; 3] = [
    "WEIGHT_SPECTRUM",
    "SIGMA_SPECTRUM",
    "CORRECTED_WEIGHT_SPECTRUM",
];

pub(super) struct SpectrumProjection {
    pub(super) source: Vec<&'static str>,
    pub(super) output: Vec<&'static str>,
    pub(super) propagate_weights: bool,
    pub(super) fill_standard: bool,
}

impl SpectrumProjection {
    pub(super) fn existing(table: &Table) -> Self {
        let source = MAIN_SPECTRA
            .into_iter()
            .filter(|name| {
                table
                    .schema()
                    .is_some_and(|schema| schema.contains_column(name))
            })
            .collect::<Vec<_>>();
        Self {
            output: source.clone(),
            source,
            propagate_weights: false,
            fill_standard: false,
        }
    }

    pub(super) fn partition(
        table: &Table,
        selected_row: usize,
        averaging_time: bool,
    ) -> Result<Self, TableError> {
        let name = "WEIGHT_SPECTRUM";
        let present = table
            .schema()
            .is_some_and(|schema| schema.contains_column(name));
        let defined = present && table.array_shape_uncached(selected_row, name)?.is_some();
        Ok(Self {
            source: defined.then_some(name).into_iter().collect(),
            output: vec![name],
            propagate_weights: defined,
            fill_standard: defined && averaging_time,
        })
    }

    pub(super) fn resolve(
        table: &Table,
        selected_row: usize,
        grammar: RowGrammar,
        force_spectra: bool,
        (channel_average, time_average, spectral_convolution, hanning): (bool, bool, bool, bool),
        visibility: &VisibilityColumns,
    ) -> Result<Self, TableError> {
        let exists = |name| {
            table
                .schema()
                .is_some_and(|schema| schema.contains_column(name))
        };
        if grammar != RowGrammar::Transform {
            return Ok(Self::existing(table));
        }
        let defined = |row, name| -> Result<bool, TableError> {
            Ok(exists(name) && table.array_shape_uncached(row, name)?.is_some())
        };
        let propagate_weights = defined(selected_row, "WEIGHT_SPECTRUM")?;
        let fill_standard = force_spectra || propagate_weights;
        // The selected MS can supply spectra even if row zero of the physical
        // input has no spectrum. Allocate columns for the selected values.
        let output = if fill_standard || defined(0, "WEIGHT_SPECTRUM")? {
            STANDARD_SPECTRA.to_vec()
        } else {
            Vec::new()
        };
        let mut source = Vec::new();
        let needs_weight = if time_average {
            let copies = visibility.copies();
            copies
                .iter()
                .any(|column| column.source == OptionalMainColumn::CorrectedData)
                || (copies.len() == 1 && copies[0].source == OptionalMainColumn::LagData)
        } else {
            channel_average || fill_standard || spectral_convolution || hanning
        };
        if needs_weight && exists("WEIGHT_SPECTRUM") {
            source.push("WEIGHT_SPECTRUM");
        }
        let needs_sigma = if channel_average || time_average || hanning {
            visibility.copies().iter().any(|column| {
                column.source == OptionalMainColumn::Data
                    || (time_average && column.source == OptionalMainColumn::FloatData)
            })
        } else {
            (fill_standard || spectral_convolution) && !visibility.corrected_becomes_data()
        };
        if needs_sigma && exists("SIGMA_SPECTRUM") {
            source.push("SIGMA_SPECTRUM");
        }
        Ok(Self {
            source,
            output,
            propagate_weights,
            fill_standard,
        })
    }
}

pub(super) fn flag_filter_read_plan<'a>(
    rows: usize,
    mut budget: MsSelectionIoBudget,
    dimensions: impl Iterator<Item = &'a TransformArrayDimensions>,
) -> Result<usize, MsSelectionError> {
    // FLAG is read at its original shape, even when only a few selected
    // channels or correlations will appear in the product.
    let flag_bytes = dimensions
        .map(|shape| {
            shape
                .input_correlations
                .saturating_mul(shape.input_channels)
        })
        .max()
        .unwrap_or(0);
    budget.requested_bytes_per_row = flag_bytes.saturating_add(
        std::mem::size_of::<Option<ArrayValue>>() + std::mem::size_of::<Option<ScalarValue>>() + 32,
    );
    Ok(MsReadPlan::new(rows, budget)?.rows_per_block)
}

pub(super) struct SelectedFlagAxes<'a> {
    pub(super) channels: &'a BTreeMap<i32, TransformChannelSelection>,
    pub(super) ddid_to_spw: &'a BTreeMap<i32, i32>,
    pub(super) correlations: Option<&'a CorrelationSelection>,
}

impl SelectedFlagAxes<'_> {
    fn all_flagged(&self, flags: &ArrayValue, ddid: i32) -> Result<bool, TableError> {
        let ArrayValue::Bool(flags) = flags else {
            return Err(TableError::Schema("FLAG must hold Bool".into()));
        };
        let flags = flags
            .view()
            .into_dimensionality::<ndarray::Ix2>()
            .map_err(|_| {
                TableError::Schema("FLAG must have correlation and channel axes".into())
            })?;
        let channels = &self.channels[&self.ddid_to_spw[&ddid]].channels;
        let axes = self.correlations.and_then(|selection| selection.axes(ddid));
        if channels.iter().any(|&chan| chan >= flags.ncols())
            || axes.is_some_and(|axes| axes.iter().any(|&corr| corr >= flags.nrows()))
        {
            return Err(TableError::Schema(
                "selected axes exceed the FLAG shape".into(),
            ));
        }
        Ok((0..flags.nrows())
            .filter(|corr| axes.is_none_or(|axes| axes.contains(corr)))
            .all(|corr| channels.iter().all(|&chan| flags[[corr, chan]])))
    }
}

pub(super) fn keep_unflagged_rows(
    input: &MeasurementSet,
    rows: &[usize],
    descriptions: &[i32],
    rows_per_block: usize,
    selection: Option<&SelectedFlagAxes<'_>>,
) -> Result<(Vec<usize>, Vec<i32>), MsTransformError> {
    let error = |source| MsTransformError::MutateMeasurementSet {
        path: input
            .path()
            .unwrap_or_else(|| Path::new("<memory>"))
            .display()
            .to_string(),
        source: Box::new(source),
    };
    let table = input.main_table();
    let flag_row = table.column_accessor("FLAG_ROW").map_err(error)?;
    let flag = table.column_accessor("FLAG").map_err(error)?;
    let mut kept_rows = Vec::with_capacity(rows.len());
    let mut kept_descriptions = Vec::with_capacity(rows.len());
    for (rows, descriptions) in rows
        .chunks(rows_per_block)
        .zip(descriptions.chunks(rows_per_block))
    {
        let flag_rows = flag_row.scalar_cells_owned_for_rows(rows).map_err(error)?;
        let flags = flag.array_cells_owned_uncached(rows).map_err(error)?;
        for (((&row, &ddid), flag_row), flags) in
            rows.iter().zip(descriptions).zip(flag_rows).zip(flags)
        {
            let flagged_row = scalar_bool(flag_row.as_ref(), "FLAG_ROW", row)?;
            let flagged_samples = match (selection, flags.as_ref()) {
                (Some(selection), Some(flags)) => {
                    selection.all_flagged(flags, ddid).map_err(error)?
                }
                _ => bool_array_all_true(flags.as_ref(), "FLAG", row)?,
            };
            if !flagged_row && !flagged_samples {
                kept_rows.push(row);
                kept_descriptions.push(ddid);
            }
        }
    }
    Ok((kept_rows, kept_descriptions))
}

pub(super) fn transform_input_batch_rows<'a>(
    rows: usize,
    resources: MeasurementSetWriteResources,
    dimensions: impl Iterator<Item = &'a TransformArrayDimensions>,
    spectrum_columns: usize,
    visibility_bytes_per_sample: usize,
    extra_array_bytes: usize,
) -> Result<usize, MsSelectionError> {
    // The input arrays are loaded before slicing, so a small output cell
    // does not imply a small source block. Allow space for array headers too.
    let elements = dimensions
        .map(|shape| {
            shape.input_correlations.saturating_mul(
                shape
                    .input_channels
                    .saturating_mul(1 + visibility_bytes_per_sample + 4 * spectrum_columns)
                    .saturating_add(8),
            )
        })
        .max()
        .unwrap_or(0);
    let overhead = (10 + spectrum_columns) * (std::mem::size_of::<Option<ArrayValue>>() + 32);
    let bytes_per_row = elements
        .saturating_add(24 + overhead)
        .saturating_add(extra_array_bytes);
    let plan = MsReadPlan::new(
        rows,
        MsSelectionIoBudget {
            available_bytes: resources.available_bytes,
            maximum_live_blocks: resources.maximum_live_batches,
            requested_bytes_per_row: bytes_per_row,
            storage_alignment_rows: None,
        },
    )?;
    Ok(plan.rows_per_block)
}

pub(super) fn plan_flag_category_shapes(
    input: &MeasurementSet,
    selected_rows: &[usize],
    selected_ddids: &[i32],
    dimensions: &BTreeMap<i32, TransformArrayDimensions>,
    correlation_subset: bool,
    (groups, budget): (Option<&[time_average::TimeGroup]>, MsSelectionIoBudget),
    (combined, regrid): (Option<CombinedArrayRows<'_>>, Option<&regrid::Plan>),
) -> Result<(Vec<MeasurementSetArrayShapePlan>, usize), MsTransformError> {
    let main = input.main_table();
    let default_categories = match main
        .column_keywords("FLAG_CATEGORY")
        .and_then(|keywords| keywords.get("CATEGORY"))
    {
        Some(Value::Array(categories)) => categories.len().max(1),
        _ => 1,
    };
    let mut counts = BTreeMap::new();
    let mut largest_input = 0;
    let shapes = if let Some(combined) = combined {
        let source_shapes = optional_array_shapes(
            input,
            combined.source_rows,
            combined.source_time_groups,
            "FLAG_CATEGORY",
            budget,
        )?;
        largest_input = source_shapes
            .iter()
            .flatten()
            .map(|shape| shape.iter().copied().fold(1usize, usize::saturating_mul))
            .max()
            .unwrap_or(0);
        combined
            .groups
            .iter()
            .map(|group| {
                group
                    .members
                    .iter()
                    .find_map(|&(_, pos)| source_shapes[pos].clone())
            })
            .collect()
    } else {
        optional_array_shapes(input, selected_rows, groups, "FLAG_CATEGORY", budget)?
    };
    for ((original, ddid), &row) in shapes.into_iter().zip(selected_ddids).zip(selected_rows) {
        let shape = &dimensions[ddid];
        let cell = match original.as_deref() {
            Some(&[corr, chan, categories]) if corr > 0 && chan > 0 && categories > 0 => {
                largest_input =
                    largest_input.max(corr.saturating_mul(chan).saturating_mul(categories));
                [
                    if correlation_subset {
                        shape.output_correlations
                    } else {
                        corr
                    },
                    if chan == 1 && combined.is_none() {
                        1
                    } else {
                        shape.output_channels
                    },
                    categories,
                ]
            }
            _ => [shape.output_correlations, 1, default_categories],
        };
        let field_channels = regrid.and_then(|plan| plan.field_channel_count(row));
        let parts = field_channels
            .as_ref()
            .map_or(&shape.output_parts[..], std::slice::from_ref);
        for &channels in parts {
            let cell = [cell[0], if cell[1] == 1 { 1 } else { channels }, cell[2]];
            *counts.entry(cell).or_default() += 1;
        }
    }
    let plans = counts
        .into_iter()
        .map(|([corr, chan, categories], row_count)| {
            let visibility_tile = casa_ms::casa_visibility_tile_shape(corr, chan, "");
            MeasurementSetArrayShapePlan {
                cell_shape: vec![corr, chan, categories],
                row_count,
                tile_shape: vec![
                    visibility_tile[0],
                    visibility_tile[1],
                    1,
                    visibility_tile[2],
                ],
            }
        })
        .collect();
    Ok((plans, largest_input))
}

pub(super) fn plan_lag_shapes(
    input: &MeasurementSet,
    rows: &[usize],
    descriptions: &[i32],
    dimensions: &BTreeMap<i32, TransformArrayDimensions>,
    (groups, budget): (Option<&[time_average::TimeGroup]>, MsSelectionIoBudget),
) -> Result<(Vec<MeasurementSetArrayShapePlan>, usize), MsTransformError> {
    let path = input.path().unwrap_or_else(|| Path::new("<memory>"));
    let mut counts = BTreeMap::new();
    let mut max_bytes = 0;
    let shapes = optional_array_shapes(input, rows, groups, "LAG_DATA", budget)?;
    for ((&row, &description), shape) in rows.iter().zip(descriptions).zip(shapes) {
        let output_correlations = dimensions[&description].output_correlations;
        let lags = match shape.as_deref() {
            Some(&[stored_correlations, lags]) if lags > 0 => {
                max_bytes =
                    max_bytes.max(stored_correlations.saturating_mul(lags).saturating_mul(8));
                lags
            }
            None => 1, // An undefined cell needs no physical lag extent.
            _ => {
                return Err(MsTransformError::SpectralMetadata {
                    path: path.display().to_string(),
                    reason: format!("LAG_DATA row {row} has an invalid shape {shape:?}"),
                });
            }
        };
        *counts.entry([output_correlations, lags]).or_default() +=
            dimensions[&description].output_parts.len();
    }
    let plans = counts
        .into_iter()
        .map(
            |([correlations, lags], row_count)| MeasurementSetArrayShapePlan {
                cell_shape: vec![correlations, lags],
                row_count,
                tile_shape: casa_ms::casa_visibility_tile_shape(correlations, lags, ""),
            },
        )
        .collect();
    Ok((plans, max_bytes))
}

pub(super) fn optional_array_shapes(
    input: &MeasurementSet,
    rows: &[usize],
    groups: Option<&[time_average::TimeGroup]>,
    column: &str,
    budget: MsSelectionIoBudget,
) -> Result<Vec<Option<Vec<usize>>>, MsTransformError> {
    let path = input.path().unwrap_or_else(|| Path::new("<memory>"));
    let table_error = |source| MsTransformError::MutateMeasurementSet {
        path: path.display().to_string(),
        source: Box::new(source),
    };
    let mut members: Box<dyn Iterator<Item = (usize, usize)>> = match groups {
        Some(groups) => Box::new(groups.iter().enumerate().flat_map(|(index, group)| {
            group
                .rows
                .iter()
                .map(move |sample| (index, sample.physical))
        })),
        None => Box::new(rows.iter().copied().enumerate()),
    };
    let raw_rows = groups.map_or(rows.len(), |groups| {
        groups.iter().map(|group| group.rows.len()).sum()
    });
    let plan = MsReadPlan::new(raw_rows, budget).map_err(|error| {
        MsTransformError::WriteMeasurementSet {
            path: path.display().to_string(),
            reason: error.to_string(),
        }
    })?;
    let accessor = input
        .main_table()
        .column_accessor(column)
        .map_err(table_error)?;
    let mut shapes: Vec<Option<Vec<usize>>> = vec![None; rows.len()];
    loop {
        let block = members
            .by_ref()
            .take(plan.rows_per_block)
            .collect::<Vec<_>>();
        if block.is_empty() {
            return Ok(shapes);
        }
        let indices = block.iter().map(|&(_, row)| row).collect::<Vec<_>>();
        let projected = accessor
            .array_shapes_uncached(&indices)
            .or_else(|_| {
                indices
                    .iter()
                    .map(|&row| input.main_table().array_shape_uncached(row, column))
                    .collect()
            })
            .map_err(table_error)?;
        for ((index, row), found) in block.into_iter().zip(projected) {
            let Some(found) = found.filter(|shape| groups.is_none() || !shape.contains(&0)) else {
                continue;
            };
            if let Some(previous) = &shapes[index]
                && previous != &found
            {
                return Err(MsTransformError::SpectralMetadata {
                    path: path.display().to_string(),
                    reason: format!(
                        "{column} row {row} shape {found:?} differs from its time bin's shape {previous:?}"
                    ),
                });
            }
            shapes[index] = Some(found);
        }
    }
}

pub(super) fn push_complex32_array_row(
    session: &mut MeasurementSetWriteSession,
    column: &str,
    value: &ArrayValue,
    output_path: &Path,
    row: usize,
) -> Result<(), MsTransformError> {
    let ArrayValue::Complex32(array) = value else {
        return Err(MsTransformError::WriteMeasurementSet {
            path: output_path.display().to_string(),
            reason: format!("{column} row {row} is not Complex32"),
        });
    };
    if array.ndim() != 2 {
        return Err(MsTransformError::WriteMeasurementSet {
            path: output_path.display().to_string(),
            reason: format!("{column} row {row} is not rank-2"),
        });
    }
    let values = array_in_storage_order(array);
    session
        .push_complex32_row(column, array.shape(), &values)
        .map_err(|error| MsTransformError::WriteMeasurementSet {
            path: output_path.display().to_string(),
            reason: error.to_string(),
        })
}

pub(super) fn push_bool_array_row(
    session: &mut MeasurementSetWriteSession,
    column: &str,
    value: &ArrayValue,
    output_path: &Path,
    row: usize,
) -> Result<(), MsTransformError> {
    let ArrayValue::Bool(array) = value else {
        return Err(MsTransformError::WriteMeasurementSet {
            path: output_path.display().to_string(),
            reason: format!("{column} row {row} is not Bool"),
        });
    };
    let values = array_in_storage_order(array);
    session
        .push_bool_row(column, array.shape(), &values)
        .map_err(|error| MsTransformError::WriteMeasurementSet {
            path: output_path.display().to_string(),
            reason: error.to_string(),
        })
}

pub(super) fn push_f64_array_row(
    session: &mut MeasurementSetWriteSession,
    column: &str,
    value: &ArrayValue,
    output_path: &Path,
    row: usize,
) -> Result<(), MsTransformError> {
    let ArrayValue::Float64(array) = value else {
        return Err(MsTransformError::WriteMeasurementSet {
            path: output_path.display().to_string(),
            reason: format!("{column} row {row} is not Float64"),
        });
    };
    let values = array_in_storage_order(array);
    session
        .push_f64_row(column, array.shape(), &values)
        .map_err(|error| MsTransformError::WriteMeasurementSet {
            path: output_path.display().to_string(),
            reason: error.to_string(),
        })
}

pub(super) fn push_f32_array_row(
    session: &mut MeasurementSetWriteSession,
    column: &str,
    value: &ArrayValue,
    output_path: &Path,
    row: usize,
) -> Result<(), MsTransformError> {
    let ArrayValue::Float32(array) = value else {
        return Err(MsTransformError::WriteMeasurementSet {
            path: output_path.display().to_string(),
            reason: format!("{column} row {row} is not Float32"),
        });
    };
    let rank = array.ndim();
    if !matches!(rank, 1 | 2) {
        return Err(MsTransformError::WriteMeasurementSet {
            path: output_path.display().to_string(),
            reason: format!("{column} row {row} has unsupported rank {rank}"),
        });
    }
    let values = array_in_storage_order(array);
    session
        .push_f32_row(column, array.shape(), &values)
        .map_err(|error| MsTransformError::WriteMeasurementSet {
            path: output_path.display().to_string(),
            reason: error.to_string(),
        })
}

pub(super) fn array_in_storage_order<T: Copy>(array: &ArrayD<T>) -> Cow<'_, [T]> {
    let storage_order = array.t();
    match storage_order.to_slice() {
        Some(values) => Cow::Borrowed(values),
        None => Cow::Owned(storage_order.iter().copied().collect()),
    }
}

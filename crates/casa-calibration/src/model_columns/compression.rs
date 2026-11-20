// SPDX-License-Identifier: LGPL-3.0-or-later
//! Physical column writes for the compressed scratch visibility layout.

use std::collections::BTreeMap;
use std::ops::Range;

use casa_ms::schema::main_table::VisibilityDataColumn;
use casa_ms::write_session::{
    MeasurementSetColumnStorage, MeasurementSetColumnWriteMode, MeasurementSetMutationColumnBatch,
    MeasurementSetMutationColumnValues, MeasurementSetWriteColumnPlan,
    MeasurementSetWriteResources,
};
use casa_ms::{
    MeasurementSet, MsResult, ResolvedMsSelection, ResolvedMsSelectionRow, scratch_columns,
};
use casa_tables::{
    ColumnSchema, ComplexCompression, ComplexScale, StreamedTiledShapeCubeLayout,
    StreamedTiledShapeValueType, StreamingTiledShapeWriter, Table, TableWriteLock,
};
use casa_types::{ArrayValue, Complex32, PrimitiveType, ScalarValue};

use super::{CORRECTED, MODEL, invalid, model_polarizations, observed_column, reset_batch};

const BATCH_ROWS: usize = 256;

pub(super) fn initialize_columns(
    ms: &mut MeasurementSet,
    selected: ResolvedMsSelection,
    columns: &[VisibilityDataColumn],
    lock: &mut TableWriteLock,
) -> MsResult<()> {
    let (layouts, big_endian) = packing_layout(ms, &selected)?;
    let path = lock
        .path()
        .parent()
        .ok_or_else(|| invalid("MAIN has no parent directory"))?;
    let temporary = tempfile::tempdir_in(path).map_err(invalid)?;
    for &column in columns {
        let (scalars, packed, comment) = match column {
            MODEL => ("ModelScaleOffset", "ModelCompTiled", "model data"),
            CORRECTED => ("CorrScaleOffset", "CorrectedCompTiled", "corrected data"),
            _ => return Err(invalid("cannot compress observed data during attachment")),
        };
        let name = column.name();
        let schema = ColumnSchema::array_variable(name, PrimitiveType::Complex32, Some(2))
            .with_comment(comment);
        let writer = StreamingTiledShapeWriter::create(
            temporary.path().join(name),
            StreamedTiledShapeValueType::Int32,
            layouts.clone(),
            MeasurementSetWriteResources::from_system_memory(1)
                .map_err(invalid)?
                .available_bytes,
            big_endian,
        )
        .map_err(invalid)?;
        let data = pack_initial_cells(ms, &selected, column, writer)?;
        scratch_columns::prepare_write_holding(ms, column, true, lock)?;
        ms.main_table_mut().add_compressed_complex_array_in_place(
            schema,
            scalars,
            packed,
            &data.packed,
            &data.scales,
            &data.offsets,
        )?;
        lock.record_write();
    }
    Ok(())
}

fn packing_layout(
    ms: &MeasurementSet,
    selected: &ResolvedMsSelection,
) -> MsResult<(Vec<StreamedTiledShapeCubeLayout>, bool)> {
    let spectral = ms.spectral_window()?;
    let mut max_channels = 0usize;
    for window in 0..spectral.row_count() {
        max_channels =
            max_channels.max(usize::try_from(spectral.num_chan(window)?).map_err(invalid)?);
    }
    let main = ms.main_table();
    let observed = main.column_accessor(observed_column(ms))?;
    let mut shapes = BTreeMap::<Vec<usize>, usize>::new();
    let mut first_correlations = None;
    for batch in selected.selected_rows.chunks(BATCH_ROWS) {
        let indices = batch.iter().map(|row| row.row_index).collect::<Vec<_>>();
        for shape in observed.array_shapes_uncached(&indices)? {
            let shape = shape.ok_or_else(|| invalid("observed data has an undefined cell"))?;
            let &[correlations, _] = shape.as_slice() else {
                return Err(invalid("observed visibility must have two axes"));
            };
            if correlations == 0 {
                return Err(invalid("observed visibility has no correlations"));
            }
            first_correlations.get_or_insert(correlations);
            *shapes.entry(shape).or_default() += 1;
        }
    }
    let correlations = first_correlations.ok_or_else(|| invalid("observed data has no rows"))?;
    let channel_tile = max_channels / 10 + 1;
    let tile = vec![
        correlations,
        channel_tile,
        131_072 / correlations / channel_tile + 1,
    ];
    let layouts = shapes
        .into_iter()
        .map(|(shape, count)| StreamedTiledShapeCubeLayout::new(shape, count, tile.clone()))
        .collect();
    let big_endian = main
        .stored_endian_format()?
        .is_none_or(casa_tables::EndianFormat::is_big_endian);
    Ok((layouts, big_endian))
}

struct PackedScratch {
    packed: casa_tables::StreamedTiledShapeColumn,
    scales: Vec<Option<ScalarValue>>,
    offsets: Vec<Option<ScalarValue>>,
}

fn pack_initial_cells(
    ms: &MeasurementSet,
    selected: &ResolvedMsSelection,
    column: VisibilityDataColumn,
    mut writer: StreamingTiledShapeWriter,
) -> MsResult<PackedScratch> {
    let codec = ComplexCompression::automatic_standard(column.name());
    let polarizations = if column == MODEL {
        model_polarizations(ms)?
    } else {
        BTreeMap::new()
    };
    let destination = ScratchDestination::new(ms, column)?;
    let row_count = selected.selected_rows.len();
    let mut scales = Vec::with_capacity(row_count);
    let mut offsets = Vec::with_capacity(row_count);
    for batch in selected.selected_rows.chunks(BATCH_ROWS) {
        let columns = reset_batch(
            ms,
            batch,
            std::slice::from_ref(&destination),
            &BTreeMap::new(),
            &polarizations,
        )?
        .columns;
        for column in columns {
            let MeasurementSetMutationColumnValues::Arrays(values) = column.values else {
                return Err(invalid("initial scratch cells must be arrays"));
            };
            for value in values {
                let ArrayValue::Complex32(value) = value else {
                    return Err(invalid("initial compressed scratch cells must be complex"));
                };
                let (packed, (scale, offset)) =
                    codec.encode_visibility_cell(&value, None, (0., 0.), None)?;
                let cells = packed.view().reversed_axes();
                let contiguous = cells.as_standard_layout();
                writer
                    .push_i32_row(
                        packed.shape(),
                        contiguous.as_slice().expect("contiguous row"),
                    )
                    .map_err(invalid)?;
                scales.push(Some(ScalarValue::Float32(scale)));
                offsets.push(Some(ScalarValue::Float32(offset)));
            }
        }
    }
    Ok(PackedScratch {
        packed: writer.finish().map_err(invalid)?,
        scales,
        offsets,
    })
}

pub(crate) struct ScratchDestination {
    pub(super) visibility: VisibilityDataColumn,
    compression: Option<ComplexCompression>,
}

impl ScratchDestination {
    pub(super) fn new(ms: &MeasurementSet, visibility: VisibilityDataColumn) -> MsResult<Self> {
        let compression = ms.main_table().complex_compression(visibility.name())?;
        if let Some(compression) = &compression {
            compression.check_writable()?;
        }
        Ok(Self {
            visibility,
            compression,
        })
    }

    pub(super) fn plans(
        &self,
        elements: usize,
        create: bool,
    ) -> Vec<MeasurementSetWriteColumnPlan> {
        let mut columns = match &self.compression {
            None => vec![column_plan(self.visibility.name(), elements, create)],
            Some(compression) => vec![column_plan(&compression.stored, elements, false)],
        };
        if let Some(ComplexScale::PerRow {
            scale,
            offset,
            automatic: true,
        }) = self
            .compression
            .as_ref()
            .map(|compression| &compression.scale)
        {
            columns.extend([column_plan(scale, 1, false), column_plan(offset, 1, false)]);
        }
        columns
    }

    pub(crate) fn batch(
        &self,
        ms: &MeasurementSet,
        row_indices: &[usize],
        rows: &[ResolvedMsSelectionRow],
        ranges: &BTreeMap<i32, Range<usize>>,
        values: Vec<ArrayValue>,
    ) -> MsResult<Vec<MeasurementSetMutationColumnBatch>> {
        let Some(compression) = &self.compression else {
            return Ok(vec![array_batch(self.visibility.name(), values)]);
        };
        let main = ms.main_table();
        let packed = if rows
            .iter()
            .any(|row| ranges.contains_key(&row.spectral_window_id))
        {
            main.column_accessor(&compression.stored)?
                .array_cells_owned_uncached(row_indices)?
        } else {
            vec![None; rows.len()]
        };
        let (scales, offsets) = match &compression.scale {
            ComplexScale::Fixed { scale, offset } => {
                (vec![*scale; rows.len()], vec![*offset; rows.len()])
            }
            ComplexScale::PerRow { scale, offset, .. } => (
                scalar_column(main, scale, row_indices)?,
                scalar_column(main, offset, row_indices)?,
            ),
        };
        let mut encoded = Vec::with_capacity(rows.len());
        let mut new_scale = Vec::with_capacity(rows.len());
        let mut new_offset = Vec::with_capacity(rows.len());
        for (((row, value), original), previous) in rows
            .iter()
            .zip(values)
            .zip(packed)
            .zip(scales.into_iter().zip(offsets))
        {
            let ArrayValue::Complex32(value) = value else {
                return Err(invalid(
                    "compressed scratch write expects complex visibilities",
                ));
            };
            let original = match original {
                Some(ArrayValue::Int32(original)) => Some(original),
                Some(_) => return Err(invalid("compressed scratch backing is not Int32")),
                None => None,
            };
            let range = ranges.get(&row.spectral_window_id).cloned();
            let (packed, (scale, offset)) =
                compression.encode_visibility_cell(&value, original, previous, range)?;
            encoded.push(ArrayValue::Int32(packed));
            new_scale.push(ScalarValue::Float32(scale));
            new_offset.push(ScalarValue::Float32(offset));
        }
        let mut columns = vec![array_batch(&compression.stored, encoded)];
        if let ComplexScale::PerRow {
            scale,
            offset,
            automatic: true,
        } = &compression.scale
        {
            columns.extend([
                scalar_batch(scale, new_scale),
                scalar_batch(offset, new_offset),
            ]);
        }
        Ok(columns)
    }
}

fn column_plan(name: &str, elements: usize, create: bool) -> MeasurementSetWriteColumnPlan {
    let (mode, storage_manager) = if create {
        (
            MeasurementSetColumnWriteMode::Create,
            MeasurementSetColumnStorage::TiledShape,
        )
    } else {
        (
            MeasurementSetColumnWriteMode::Replace,
            MeasurementSetColumnStorage::Persisted,
        )
    };
    MeasurementSetWriteColumnPlan {
        name: name.to_owned(),
        bytes_per_row: elements.saturating_mul(std::mem::size_of::<Complex32>()),
        mode,
        storage_manager,
        tile_shape: None,
        create_source_column: None,
    }
}

fn scalar_column(table: &Table, name: &str, rows: &[usize]) -> MsResult<Vec<f32>> {
    table
        .column_accessor(name)?
        .scalar_cells_owned_for_rows(rows)?
        .into_iter()
        .map(|value| match value {
            Some(ScalarValue::Float32(value)) => Ok(value),
            _ => Err(invalid(format!(
                "scratch scale column {name:?} is not Float32"
            ))),
        })
        .collect()
}

fn array_batch(name: &str, values: Vec<ArrayValue>) -> MeasurementSetMutationColumnBatch {
    MeasurementSetMutationColumnBatch {
        name: name.to_owned(),
        values: MeasurementSetMutationColumnValues::Arrays(values),
    }
}

fn scalar_batch(name: &str, values: Vec<ScalarValue>) -> MeasurementSetMutationColumnBatch {
    MeasurementSetMutationColumnBatch {
        name: name.to_owned(),
        values: MeasurementSetMutationColumnValues::Scalars(values),
    }
}

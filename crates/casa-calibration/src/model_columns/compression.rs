// SPDX-License-Identifier: LGPL-3.0-or-later
//! Physical column writes for the compressed scratch visibility layout.

use std::collections::BTreeMap;
use std::ops::Range;

use casa_core::{ArrayValue, PrimitiveType, ScalarValue};
use casa_ms::schema::main_table::VisibilityDataColumn;
use casa_ms::write_session::{
    MeasurementSetMutationColumnBatch, MeasurementSetMutationColumnValues,
    MeasurementSetWriteColumnPlan, MeasurementSetWriteResources, VisibilityWriteDestination,
};
use casa_ms::{
    MeasurementSet, MsResult, ResolvedMsSelection, ResolvedMsSelectionRow, scratch_columns,
};
use casa_tables::streamed_tiled::{
    StreamedTiledShapeCubeLayout, StreamedTiledShapeValueType, StreamingTiledShapeWriter,
};
use casa_tables::{ColumnSchema, ComplexCompression, CompressedComplexArray, TableWriteLock};

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
    let mut arrays = Vec::with_capacity(columns.len());
    let mut created = Vec::with_capacity(4 * columns.len());
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
        arrays.push(CompressedComplexArray {
            schema,
            scalar_manager: scalars,
            tiled_manager: packed,
            packed: data.packed,
            scales: data.scales,
            offsets: data.offsets,
        });
        created.extend(
            ["", "_COMPRESSED", "_SCALE", "_OFFSET"].map(|suffix| format!("{name}{suffix}")),
        );
    }
    ms.main_table_mut()
        .add_compressed_complex_arrays_in_place(&arrays)?;
    lock.record_write();
    let column = if columns.contains(&MODEL) {
        MODEL
    } else {
        CORRECTED
    };
    if let Err(error) = scratch_columns::prepare_write_holding(ms, column, true, lock) {
        let added = created.iter().map(String::as_str).collect::<Vec<_>>();
        ms.main_table_mut()
            .remove_columns_preserving_managers(&added)
            .map_err(|rollback| {
                invalid(format!(
                    "{error}. Removing the new scratch columns also failed: {rollback}"
                ))
            })?;
        lock.record_write();
        return Err(error);
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
    packed: casa_tables::streamed_tiled::StreamedTiledShapeColumn,
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
    let destination = ScratchDestination::new(ms, column, column.name())?;
    let row_count = selected.selected_rows.len();
    let mut scales = Vec::with_capacity(row_count);
    let mut offsets = Vec::with_capacity(row_count);
    for batch in selected.selected_rows.chunks(BATCH_ROWS) {
        let columns = reset_batch(
            ms,
            observed_column(ms),
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

pub(crate) struct ScratchDestination<'a> {
    pub(super) visibility: VisibilityDataColumn,
    destination: VisibilityWriteDestination<'a>,
}

impl<'a> ScratchDestination<'a> {
    pub(super) fn new(
        ms: &MeasurementSet,
        visibility: VisibilityDataColumn,
        name: &'a str,
    ) -> MsResult<Self> {
        Ok(Self {
            visibility,
            destination: VisibilityWriteDestination::new(ms.main_table(), name)?,
        })
    }

    pub(super) fn name(&self) -> &str {
        self.destination.name()
    }

    pub(super) fn plans(
        &self,
        elements: usize,
        create: bool,
    ) -> Vec<MeasurementSetWriteColumnPlan> {
        self.destination.plans(elements, create)
    }

    pub(crate) fn batch(
        &self,
        ms: &MeasurementSet,
        row_indices: &[usize],
        rows: &[ResolvedMsSelectionRow],
        ranges: &BTreeMap<i32, Range<usize>>,
        values: Vec<ArrayValue>,
    ) -> MsResult<Vec<MeasurementSetMutationColumnBatch>> {
        let channels = (!ranges.is_empty()).then(|| {
            rows.iter()
                .map(|row| ranges.get(&row.spectral_window_id).cloned())
                .collect::<Vec<_>>()
        });
        self.destination
            .batch(ms.main_table(), row_indices, values, channels.as_deref())
    }
}

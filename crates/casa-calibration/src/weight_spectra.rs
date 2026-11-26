// SPDX-License-Identifier: LGPL-3.0-or-later
//! Stage fully defined weight spectra in tiled storage before publishing them.

use std::collections::BTreeMap;

use casa_core::{ArrayValue, PrimitiveType};
use casa_ms::write_session::TARGET_BATCH_BYTES;
use casa_ms::{MsError, MsResult};
use casa_tables::streamed_tiled::{
    StreamedTiledShapeCubeLayout, StreamedTiledShapeValueType, StreamingTiledShapeWriter,
};
use casa_tables::{ColumnSchema, Table, TableError, TableWriteLock};

pub(crate) const WEIGHT_SPECTRUM: &str = "WEIGHT_SPECTRUM";
pub(crate) const SIGMA_SPECTRUM: &str = "SIGMA_SPECTRUM";

pub(crate) fn has_column(table: &Table, name: &str) -> bool {
    table
        .schema()
        .is_some_and(|schema| schema.contains_column(name))
}

#[derive(Default)]
pub(crate) struct NewSpectra {
    writers: BTreeMap<&'static str, StreamingTiledShapeWriter>,
    _staging: Option<tempfile::TempDir>,
}

impl NewSpectra {
    pub(crate) fn prepare(
        main: &Table,
        columns: &[&'static str],
        row_shapes: impl Iterator<Item = [usize; 2]>,
    ) -> MsResult<Self> {
        let added: Vec<_> = columns
            .iter()
            .copied()
            .filter(|name| {
                matches!(*name, WEIGHT_SPECTRUM | SIGMA_SPECTRUM) && !has_column(main, name)
            })
            .collect();
        if added.is_empty() {
            return Ok(Self::default());
        }
        let mut counts = BTreeMap::<_, usize>::new();
        for shape in row_shapes {
            *counts.entry(shape).or_default() += 1;
        }
        let layouts: Vec<_> = counts
            .into_iter()
            .map(|([correlations, channels], count)| {
                StreamedTiledShapeCubeLayout::new(
                    vec![correlations, channels],
                    count,
                    vec![correlations, channels.min(100), 100],
                )
            })
            .collect();
        let path = main.path().ok_or_else(|| {
            MsError::InvalidInput("weight spectrum needs an on-disk MeasurementSet".into())
        })?;
        let staging = tempfile::Builder::new()
            .prefix(".weight-spectrum-")
            .tempdir_in(path)
            .map_err(|error| {
                TableError::Storage(format!(
                    "stage weight spectrum for {}: {error}",
                    path.display()
                ))
            })?;
        let big_endian = main
            .stored_endian_format()?
            .unwrap_or_default()
            .is_big_endian();
        // The writer may need one three-element row-map record per row if
        // different cell shapes alternate. Allow this with the tile buffers.
        let budget = TARGET_BATCH_BYTES.saturating_add(main.row_count().saturating_mul(12));
        let writers = added
            .into_iter()
            .map(|name| {
                let writer = StreamingTiledShapeWriter::create(
                    staging.path().join(name),
                    StreamedTiledShapeValueType::Float32,
                    layouts.clone(),
                    budget,
                    big_endian,
                )
                .map_err(TableError::from)?;
                Ok((name, writer))
            })
            .collect::<MsResult<_>>()?;
        Ok(Self {
            writers,
            _staging: Some(staging),
        })
    }

    pub(crate) fn contains(&self, name: &str) -> bool {
        self.writers.contains_key(name)
    }

    pub(crate) fn append(&mut self, name: &str, value: &ArrayValue) -> MsResult<bool> {
        let Some(writer) = self.writers.get_mut(name) else {
            return Ok(false);
        };
        let ArrayValue::Float32(value) = value else {
            return Err(MsError::InvalidInput(format!(
                "{name} requires float32 cells"
            )));
        };
        let data = value
            .as_slice_memory_order()
            .ok_or_else(|| MsError::InvalidInput(format!("{name} requires contiguous cells")))?;
        writer
            .push_f32_row(value.shape(), data)
            .map_err(TableError::from)?;
        Ok(true)
    }

    pub(crate) fn append_unity(&mut self, shape: [usize; 2]) -> MsResult<()> {
        if !self.writers.is_empty() {
            let ones = vec![1.0; shape[0] * shape[1]];
            for writer in self.writers.values_mut() {
                writer
                    .push_f32_row(&shape, &ones)
                    .map_err(TableError::from)?;
            }
        }
        Ok(())
    }

    pub(crate) fn publish(self, main: &mut Table, lock: &mut TableWriteLock) -> MsResult<()> {
        let finished = self
            .writers
            .into_iter()
            .map(|(name, writer)| Ok((name, writer.finish().map_err(TableError::from)?)))
            .collect::<MsResult<Vec<_>>>()?;
        for (name, spectrum) in finished {
            let comment = if name == WEIGHT_SPECTRUM {
                "weight spectrum"
            } else {
                "sigma spectrum"
            };
            main.add_column(
                ColumnSchema::array_variable(name, PrimitiveType::Float32, Some(2))
                    .with_comment(comment),
                None,
            )?;
            lock.record_write();
            main.prepare_write()
                .add_streamed_tiled_shape_column(name, spectrum)?;
        }
        Ok(())
    }
}

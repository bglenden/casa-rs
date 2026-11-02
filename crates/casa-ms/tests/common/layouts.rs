// SPDX-License-Identifier: LGPL-3.0-or-later
//! MeasurementSets whose visibility columns other tools' layouts store:
//! synthetic VLA tracks, and MAIN rewritten with chosen destinations in a
//! chosen storage manager.

use std::collections::HashMap;
use std::path::{Path, PathBuf};

use casa_ms::{
    MeasurementSet, SyntheticObservationRequest, generate_synthetic_observation_ms,
    tutorial_vla_a_antennas,
};
use casa_tables::{ColumnBinding, ColumnSchema, DataManagerKind, Table, TableOptions};
use casa_types::{ArrayValue, PrimitiveType, Value};
use num_complex::Complex32;

/// A VLA-A track of two-second integrations for `seconds` in `channels`
/// channels, without model data and without `MODEL_DATA`: 351 rows an
/// integration, so 120 s gives 21,060 rows, more than a write batch.
pub(crate) fn many_rows(root: &Path, label: &str, channels: usize, seconds: f64) -> PathBuf {
    let measurement_set = root.join(format!("{label}-{channels}-{seconds}.ms"));
    let mut request = SyntheticObservationRequest::vla_ppdisk(
        root.join("unused.fits"),
        &measurement_set,
        tutorial_vla_a_antennas(),
    );
    request.duration_seconds = seconds;
    request.integration_seconds = 2.0;
    request.spectral_windows[0].channel_count = channels;
    request.predict_model = false;
    generate_synthetic_observation_ms(&request).expect("synthesise the write MS");
    measurement_set
}

/// Rewrite MAIN with every column in `StandardStMan` except `destinations`,
/// added zero-filled in `manager`: the layouts other tools write.
pub(crate) fn relayout(path: &Path, destinations: &[&str], manager: DataManagerKind) {
    let [correlations, channels] = cell_shape(path, "DATA");
    let mut main = Table::open(TableOptions::new(path)).expect("open MAIN");
    let zeros = Value::Array(ArrayValue::Complex32(ndarray::ArrayD::from_elem(
        ndarray::IxDyn(&[correlations, channels]),
        Complex32::new(0.0, 0.0),
    )));
    let tiled = matches!(
        manager,
        DataManagerKind::TiledShapeStMan | DataManagerKind::TiledColumnStMan
    );
    let mut bindings = HashMap::new();
    for &column in destinations {
        let schema = if manager == DataManagerKind::TiledColumnStMan {
            ColumnSchema::array_fixed(
                column,
                PrimitiveType::Complex32,
                vec![correlations, channels],
            )
        } else {
            ColumnSchema::array_variable(column, PrimitiveType::Complex32, Some(2))
        };
        main.add_column(schema, Some(zeros.clone()))
            .expect("add the destination");
        bindings.insert(
            column.to_string(),
            ColumnBinding {
                data_manager: manager,
                tile_shape: tiled.then(|| vec![correlations, channels, 512]),
            },
        );
    }
    // Every row is in memory before the files it came from are replaced.
    main.rows().expect("load MAIN");
    main.prepare_write()
        .save_with_bindings(
            TableOptions::new(path).with_data_manager(DataManagerKind::StandardStMan),
            &bindings,
        )
        .expect("rewrite MAIN");
}

/// The `[correlations, channels]` shape of row 0's `column` cell.
pub(crate) fn cell_shape(path: &Path, column: &str) -> [usize; 2] {
    let ms = MeasurementSet::open(path).expect("open");
    let cell = ms
        .main_table()
        .column_accessor(column)
        .expect("the column")
        .array_cells_owned_uncached(&[0])
        .expect("read a cell")[0]
        .clone()
        .expect("the cell is defined");
    [cell.shape()[0], cell.shape()[1]]
}

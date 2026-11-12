// SPDX-License-Identifier: LGPL-3.0-or-later
//! Independent existing-MS input recipe matching the CASA oracle's sentinels.

use std::path::Path;

use casa_ms::{
    MeasurementSet, MeasurementSetColumnStorage, MeasurementSetColumnWriteMode,
    MeasurementSetMutationBatch, MeasurementSetMutationColumnBatch,
    MeasurementSetMutationColumnValues, MeasurementSetWriteColumnPlan, MeasurementSetWritePlan,
    MeasurementSetWriteResources, MeasurementSetWriteSession,
};
use casa_tables::ColumnSchema;
use casa_types::{ArrayValue, Complex32, PrimitiveType, ScalarValue};
use ndarray::{ArrayD, IxDyn, ShapeBuilder};

fn plan(rows: usize, columns: &[&str], create: bool) -> MeasurementSetWritePlan {
    MeasurementSetWritePlan::selected_row_mutation(
        (0..rows).collect(),
        columns
            .iter()
            .map(|&name| MeasurementSetWriteColumnPlan {
                name: name.to_string(),
                bytes_per_row: 512,
                mode: if create {
                    MeasurementSetColumnWriteMode::Create
                } else {
                    MeasurementSetColumnWriteMode::Replace
                },
                storage_manager: if create {
                    MeasurementSetColumnStorage::TiledShape
                } else {
                    MeasurementSetColumnStorage::Persisted
                },
                tile_shape: None,
                create_source_column: create.then(|| "DATA".to_string()),
            })
            .collect(),
        MeasurementSetWriteResources {
            available_bytes: 64 << 20,
            maximum_live_batches: 1,
            tiled_column_buffer_bytes: 0,
        },
    )
    .unwrap()
}

pub(super) fn prepare(path: &Path) {
    let mut ms = MeasurementSet::open(path).unwrap();
    let rows = ms.row_count();
    let shape = match ms
        .main_table()
        .cell_accessor(0, "DATA")
        .unwrap()
        .array()
        .unwrap()
    {
        ArrayValue::Complex32(values) => values.shape().to_vec(),
        _ => panic!("DATA type"),
    };
    for name in ["MODEL_DATA", "CORRECTED_DATA"] {
        ms.main_table_mut()
            .add_column(
                ColumnSchema::array_variable(name, PrimitiveType::Complex32, Some(2)),
                None,
            )
            .unwrap();
    }
    drop(
        MeasurementSetWriteSession::start_selected_row_mutation(
            &mut ms,
            plan(1, &["MODEL_DATA", "CORRECTED_DATA"], true),
        )
        .unwrap(),
    );
    let names = ["DATA", "MODEL_DATA", "CORRECTED_DATA", "FLAG", "FLAG_ROW"];
    let mut writer =
        MeasurementSetWriteSession::start_selected_row_mutation(&mut ms, plan(rows, &names, false))
            .unwrap();
    let columns = names
        .into_iter()
        .map(|name| {
            let values = if name == "FLAG_ROW" {
                MeasurementSetMutationColumnValues::Scalars(vec![ScalarValue::Bool(false); rows])
            } else {
                let cell = if name == "FLAG" {
                    ArrayValue::Bool(ArrayD::from_elem(IxDyn(&shape).f(), false))
                } else {
                    let value = match name {
                        "DATA" => Complex32::new(3.0, 4.0),
                        "MODEL_DATA" => Complex32::new(7.0, -2.0),
                        _ => Complex32::new(-5.0, 6.0),
                    };
                    ArrayValue::Complex32(ArrayD::from_elem(IxDyn(&shape).f(), value))
                };
                MeasurementSetMutationColumnValues::Arrays(vec![cell; rows])
            };
            MeasurementSetMutationColumnBatch {
                name: name.to_string(),
                values,
            }
        })
        .collect();
    writer
        .write_mutation_batch(
            &mut ms,
            MeasurementSetMutationBatch {
                row_indices: (0..rows).collect(),
                columns,
            },
        )
        .unwrap();
    writer.finish_mutation().unwrap();
}

/// Both oracle and native inputs use the stated constant receptor gain, not
/// either implementation's simulated random sequence.
pub(super) fn fixed_gains(path: &Path, output: &Path, gain: f32) {
    use casa_calibration::{
        SimulatedJones, SimulatedSolution, SolutionValues, write_simulated_jones_table,
    };
    let ms = MeasurementSet::open(path).unwrap();
    let time = match ms
        .main_table()
        .cell_accessor(0, "TIME")
        .unwrap()
        .scalar()
        .unwrap()
    {
        ScalarValue::Float64(time) => *time,
        _ => panic!("TIME type"),
    };
    let antennas = ms.antenna().unwrap().row_count() as i32;
    let windows = ms.spectral_window().unwrap().row_count() as i32;
    let rows = (0..windows).flat_map(|spw| {
        (0..antennas).map(move |antenna| SimulatedSolution {
            time_s: time,
            field_id: 0,
            spectral_window_id: spw,
            antenna,
            values: SolutionValues::Receptors([Complex32::new(gain, 0.0); 2]),
        })
    });
    write_simulated_jones_table(&ms, output, SimulatedJones::Gain, rows).unwrap();
}

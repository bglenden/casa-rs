// SPDX-License-Identifier: LGPL-3.0-or-later

use super::*;
use crate::schema::SubtableId;
use crate::test_helpers::{default_value, default_value_for_def};

fn add_selection_test_fields(ms: &mut MeasurementSet, count: usize) {
    use casa_core::{RecordField, RecordValue, ScalarValue, Value};
    for id in 0..count {
        let fields = crate::schema::field::REQUIRED_COLUMNS
            .iter()
            .map(|column| {
                let value = match column.name {
                    "NAME" => Value::Scalar(ScalarValue::String(format!("field-{id}"))),
                    "SOURCE_ID" => Value::Scalar(ScalarValue::Int32(-1)),
                    "DELAY_DIR" | "PHASE_DIR" | "REFERENCE_DIR" => {
                        Value::Array(ArrayValue::Float64(ndarray::ArrayD::zeros(vec![2, 1])))
                    }
                    _ => default_value_for_def(column),
                };
                RecordField::new(column.name, value)
            })
            .collect();
        ms.subtable_mut(SubtableId::Field)
            .unwrap()
            .add_row(RecordValue::new(fields))
            .unwrap();
    }
}

fn apply_for_test(selection: &MsSelection, ms: &MeasurementSet) -> Vec<usize> {
    selection.compile(ms).unwrap().apply(ms).unwrap()
}

#[test]
fn empty_selection_selects_all() {
    let sel = MsSelection::new();
    assert_eq!(sel.to_taql(), "SELECT *");
}

#[test]
fn first_match_stops_before_loading_later_uv_blocks() {
    use casa_core::{PrimitiveType, RecordField, RecordValue, Value};
    use casa_tables::{ColumnSchema, TableSchema};

    let mut ms = MeasurementSet::create_memory(crate::MeasurementSetBuilder::new()).unwrap();
    let main = ms.main_table_mut();
    let schema = TableSchema::new(vec![ColumnSchema::array_variable(
        "UVW",
        PrimitiveType::Float64,
        Some(1),
    )])
    .unwrap();
    *main = Table::with_schema(schema);
    let uvws: [&[f64]; 2] = [&[3.0, 4.0, 0.0], &[1.0, 2.0]];
    for uvw in uvws {
        main.add_row(RecordValue::new(vec![RecordField::new(
            "UVW",
            Value::Array(ArrayValue::Float64(ndarray::arr1(uvw).into_dyn())),
        )]))
        .unwrap();
    }
    let selection = CompiledMsSelection {
        casa_uv: Some(uv_expr::UvDistanceSelection::parse(&ms, "<10m").unwrap()),
        ..CompiledMsSelection::default()
    };
    assert_eq!(selection.first_match(&ms, Some(1)).unwrap(), Some(0));
    assert!(selection.apply_internal(&ms, Some(1), false).is_err());
}

#[test]
fn row_block_budget_rejects_one_byte_below_one_row() {
    let error = plan_selected_row_blocks(
        &[3],
        MsSelectionIoBudget {
            available_bytes: 7,
            maximum_live_blocks: 1,
            requested_bytes_per_row: 8,
            storage_alignment_rows: None,
        },
    )
    .expect_err("one row cannot fit");
    assert!(matches!(error, MsSelectionError::InsufficientBudget { .. }));
}

#[test]
fn row_block_budget_accepts_at_and_above_one_row() {
    for available_bytes in [8, 9] {
        let blocks = plan_selected_row_blocks(
            &[3, 5],
            MsSelectionIoBudget {
                available_bytes,
                maximum_live_blocks: 1,
                requested_bytes_per_row: 8,
                storage_alignment_rows: None,
            },
        )
        .expect("one-row blocks fit");
        assert_eq!(blocks.len(), 2);
        assert!(blocks.iter().all(|block| block.planned_bytes == 8));
    }
}

#[test]
fn row_block_budget_aligns_without_exceeding_budget() {
    let blocks = plan_selected_row_blocks(
        &(0..9).collect::<Vec<_>>(),
        MsSelectionIoBudget {
            available_bytes: 100,
            maximum_live_blocks: 1,
            requested_bytes_per_row: 10,
            storage_alignment_rows: Some(4),
        },
    )
    .expect("aligned blocks");
    assert_eq!(
        blocks
            .iter()
            .map(|block| block.selected_row_count)
            .collect::<Vec<_>>(),
        vec![8, 1]
    );
    assert!(blocks.iter().all(|block| block.planned_bytes <= 100));
}

#[test]
fn empty_selection_needs_no_row_budget() {
    let blocks = plan_selected_row_blocks(
        &[],
        MsSelectionIoBudget {
            available_bytes: 0,
            maximum_live_blocks: 1,
            requested_bytes_per_row: usize::MAX,
            storage_alignment_rows: None,
        },
    )
    .expect("empty selection");
    assert!(blocks.is_empty());
}

#[test]
fn maximum_row_size_uses_checked_one_row_blocks() {
    let blocks = plan_selected_row_blocks(
        &[usize::MAX],
        MsSelectionIoBudget {
            available_bytes: usize::MAX,
            maximum_live_blocks: 1,
            requested_bytes_per_row: usize::MAX,
            storage_alignment_rows: None,
        },
    )
    .expect("maximum-sized row budget");
    assert_eq!(blocks[0].planned_bytes, usize::MAX);
}

#[test]
fn read_plan_covers_zero_one_wide_and_overflow_workloads() {
    let zero = MsReadPlan::new(
        0,
        MsSelectionIoBudget {
            available_bytes: 0,
            maximum_live_blocks: 1,
            requested_bytes_per_row: 8,
            storage_alignment_rows: None,
        },
    )
    .expect("zero-row plan");
    assert_eq!(zero.rows_per_block, 0);

    let one = MsReadPlan::new(
        1,
        MsSelectionIoBudget {
            available_bytes: 8,
            maximum_live_blocks: 1,
            requested_bytes_per_row: 8,
            storage_alignment_rows: None,
        },
    )
    .expect("one-row plan");
    assert_eq!(one.rows_per_block, 1);
    assert_eq!(one.bytes_per_block, 8);

    let constrained = MsReadPlan::new(
        1_000,
        MsSelectionIoBudget {
            available_bytes: 1_024,
            maximum_live_blocks: 2,
            requested_bytes_per_row: 128,
            storage_alignment_rows: Some(2),
        },
    )
    .expect("constrained plan");
    assert_eq!(constrained.rows_per_block, 4);

    let error = MsReadPlan::new(
        usize::MAX,
        MsSelectionIoBudget {
            available_bytes: usize::MAX,
            maximum_live_blocks: 1,
            requested_bytes_per_row: usize::MAX / 2 + 1,
            storage_alignment_rows: None,
        },
    )
    .expect("one row fits without overflow");
    assert_eq!(error.rows_per_block, 1);
}

#[test]
fn field_selection() {
    let sel = MsSelection::new().field(&[0, 1]);
    assert_eq!(sel.to_taql(), "SELECT * WHERE FIELD_ID IN [0,1]");
}

#[test]
fn combined_selection() {
    let sel = MsSelection::new().field(&[0]).scan(&[1, 2, 3]);
    let taql = sel.to_taql();
    assert!(taql.contains("FIELD_ID IN [0]"));
    assert!(taql.contains("SCAN_NUMBER IN [1,2,3]"));
    assert!(taql.contains(" AND "));
}

#[test]
fn time_range_selection() {
    let sel = MsSelection::new().time_range(1000.0, 2000.0);
    let taql = sel.to_taql();
    assert!(taql.contains("TIME>=1000"));
    assert!(taql.contains("TIME<=2000"));
}

#[test]
fn antenna_selection() {
    let sel = MsSelection::new().antenna(&[0, 1]);
    let taql = sel.to_taql();
    assert!(taql.contains("ANTENNA1 IN [0,1]"));
    assert!(taql.contains("ANTENNA2 IN [0,1]"));
}

#[test]
fn baseline_selection() {
    let sel = MsSelection::new().baseline(&[(0, 1), (2, 3)]);
    let taql = sel.to_taql();
    assert!(taql.contains("ANTENNA1==0 AND ANTENNA2==1"));
    assert!(taql.contains("ANTENNA1==2 AND ANTENNA2==3"));
}

#[test]
fn raw_taql_expression() {
    let sel = MsSelection::new().taql("UVW[0] > 100").taql("TIME > 0");
    let taql = sel.to_taql();
    assert!(taql.contains("(UVW[0] > 100)"));
    assert!(taql.contains("(TIME > 0)"));
}

#[test]
fn data_description_selection() {
    let sel = MsSelection::new().data_description(&[2, 4]);
    let taql = sel.to_taql();
    assert!(taql.contains("DATA_DESC_ID IN [2,4]"));
}

#[test]
fn state_selection() {
    let sel = MsSelection::new().state(&[1, 5]);
    let taql = sel.to_taql();
    assert!(taql.contains("STATE_ID IN [1,5]"));
}

#[test]
fn unmatched_numeric_fields_select_no_rows_in_structured_and_taql_paths() {
    let mut ms =
        MeasurementSet::create_memory(crate::builder::MeasurementSetBuilder::new()).unwrap();
    add_selection_test_fields(&mut ms, 1);
    let fields = ms
        .main_table()
        .schema()
        .unwrap()
        .columns()
        .iter()
        .map(|column| casa_core::RecordField::new(column.name(), default_value(column.name())))
        .collect();
    ms.main_table_mut()
        .add_row(casa_core::RecordValue::new(fields))
        .unwrap();
    for selector in ["999", "9~2147483647"] {
        let request = MsSelection {
            field: Some(selector.to_string()),
            ..MsSelection::new()
        };
        let compiled = request.compile(&ms).unwrap();
        assert_eq!(compiled.to_taql(), "SELECT * WHERE FALSE");
        assert!(compiled.apply(&ms).unwrap().is_empty());
        let with_taql = request.taql("FIELD_ID == 0").compile(&ms).unwrap();
        assert!(with_taql.apply(&ms).unwrap().is_empty());
        assert!(ms.resolve_field_selector(selector).is_err());
    }
    for selector in [None, Some("".to_string()), Some(" ".to_string())] {
        let request = MsSelection {
            field: selector,
            ..MsSelection::new()
        };
        assert_eq!(apply_for_test(&request, &ms), vec![0]);
        assert_eq!(apply_for_test(&request.taql("FIELD_ID == 0"), &ms), vec![0]);
    }
}

#[test]
fn apply_field_selection() {
    use crate::builder::MeasurementSetBuilder;
    use crate::test_helpers::default_value;
    use casa_core::{RecordField, RecordValue, ScalarValue, Value};

    let mut ms = MeasurementSet::create_memory(MeasurementSetBuilder::new()).unwrap();
    add_selection_test_fields(&mut ms, 3);

    // Add rows with different FIELD_IDs
    let schema = ms.main_table().schema().unwrap().clone();
    for field_id in [0, 0, 1, 1, 2] {
        let fields: Vec<RecordField> = schema
            .columns()
            .iter()
            .map(|col| {
                if col.name() == "FIELD_ID" {
                    RecordField::new("FIELD_ID", Value::Scalar(ScalarValue::Int32(field_id)))
                } else {
                    RecordField::new(col.name(), default_value(col.name()))
                }
            })
            .collect();
        ms.main_table_mut()
            .add_row(RecordValue::new(fields))
            .unwrap();
    }

    let sel = MsSelection::new().field(&[0, 1]);
    let rows = apply_for_test(&sel, &ms);
    assert_eq!(rows.len(), 4);
}

#[test]
fn apply_time_range_selection() {
    use crate::builder::MeasurementSetBuilder;
    use crate::test_helpers::default_value;
    use casa_core::{RecordField, RecordValue, ScalarValue, Value};

    let mut ms = MeasurementSet::create_memory(MeasurementSetBuilder::new()).unwrap();

    let schema = ms.main_table().schema().unwrap().clone();
    for time in [100.0, 200.0, 300.0, 400.0, 500.0] {
        let fields: Vec<RecordField> = schema
            .columns()
            .iter()
            .map(|col| {
                if col.name() == "TIME" {
                    RecordField::new("TIME", Value::Scalar(ScalarValue::Float64(time)))
                } else {
                    RecordField::new(col.name(), default_value(col.name()))
                }
            })
            .collect();
        ms.main_table_mut()
            .add_row(RecordValue::new(fields))
            .unwrap();
    }

    let sel = MsSelection::new().time_range(200.0, 400.0);
    let rows = apply_for_test(&sel, &ms);
    assert_eq!(rows.len(), 3);
}

#[test]
fn apply_antenna_name_selection() {
    use crate::builder::MeasurementSetBuilder;
    use casa_core::{RecordField, RecordValue, ScalarValue, Value};

    let mut ms = MeasurementSet::create_memory(MeasurementSetBuilder::new()).unwrap();

    // Add antennas
    {
        let mut ant = ms.antenna_mut().unwrap();
        ant.add_antenna(
            "VLA01",
            "N01",
            "GROUND-BASED",
            "ALT-AZ",
            [0.0; 3],
            [0.0; 3],
            25.0,
        )
        .unwrap();
        ant.add_antenna(
            "VLA02",
            "N02",
            "GROUND-BASED",
            "ALT-AZ",
            [0.0; 3],
            [0.0; 3],
            25.0,
        )
        .unwrap();
        ant.add_antenna(
            "VLA03",
            "N03",
            "GROUND-BASED",
            "ALT-AZ",
            [0.0; 3],
            [0.0; 3],
            25.0,
        )
        .unwrap();
    }

    // Add rows referencing different antennas
    let schema = ms.main_table().schema().unwrap().clone();
    for (ant1, ant2) in [(0, 1), (0, 2), (1, 2)] {
        let fields: Vec<RecordField> = schema
            .columns()
            .iter()
            .map(|col| match col.name() {
                "ANTENNA1" => RecordField::new("ANTENNA1", Value::Scalar(ScalarValue::Int32(ant1))),
                "ANTENNA2" => RecordField::new("ANTENNA2", Value::Scalar(ScalarValue::Int32(ant2))),
                name => RecordField::new(name, default_value(name)),
            })
            .collect();
        ms.main_table_mut()
            .add_row(RecordValue::new(fields))
            .unwrap();
    }

    let sel = MsSelection::new().antenna_name(&["VLA01"]);
    let rows = apply_for_test(&sel, &ms);
    // VLA01 is antenna 0, which appears in rows 0 (ant1=0) and 1 (ant1=0)
    assert_eq!(rows.len(), 2);
}

#[test]
fn apply_spw_selection_without_matching_ddid_returns_no_rows() {
    use crate::builder::MeasurementSetBuilder;
    use crate::schema;
    use casa_core::{RecordField, RecordValue, ScalarValue, Value};

    let mut ms = MeasurementSet::create_memory(MeasurementSetBuilder::new()).unwrap();

    let dd_fields: Vec<RecordField> = schema::data_description::REQUIRED_COLUMNS
        .iter()
        .map(|col| match col.name {
            "SPECTRAL_WINDOW_ID" => {
                RecordField::new(col.name, Value::Scalar(ScalarValue::Int32(0)))
            }
            "POLARIZATION_ID" => RecordField::new(col.name, Value::Scalar(ScalarValue::Int32(0))),
            "FLAG_ROW" => RecordField::new(col.name, Value::Scalar(ScalarValue::Bool(false))),
            _ => RecordField::new(col.name, default_value_for_def(col)),
        })
        .collect();
    ms.subtable_mut(SubtableId::DataDescription)
        .unwrap()
        .add_row(RecordValue::new(dd_fields))
        .unwrap();

    let schema = ms.main_table().schema().unwrap().clone();
    let row_fields: Vec<RecordField> = schema
        .columns()
        .iter()
        .map(|col| match col.name() {
            "DATA_DESC_ID" => RecordField::new(col.name(), Value::Scalar(ScalarValue::Int32(7))),
            name => RecordField::new(name, default_value(name)),
        })
        .collect();
    ms.main_table_mut()
        .add_row(RecordValue::new(row_fields))
        .unwrap();

    let rows = apply_for_test(&MsSelection::new().spw(&[7]), &ms);
    assert!(rows.is_empty());
}

#[test]
fn spw_selector_preserves_channel_selection_for_array_cell_consumers() {
    let selection = MsSelection::new().spw_selector("0:82~89,2").unwrap();

    assert!(selection.channel_selection_for_spw(2).is_none());
    let channels = selection
        .channel_selection_for_spw(0)
        .expect("spw 0 channel selector")
        .indices(166)
        .expect("expand channels");
    assert_eq!(channels, (82..=89).collect::<Vec<_>>());
}

#[test]
fn apply_field_and_spw_selection_returns_only_intersection() {
    use crate::builder::MeasurementSetBuilder;
    use casa_core::{RecordField, RecordValue, ScalarValue, Value};

    let mut ms = MeasurementSet::create_memory(MeasurementSetBuilder::new()).unwrap();
    add_selection_test_fields(&mut ms, 2);

    ms.subtable_mut(SubtableId::DataDescription)
        .unwrap()
        .add_row(RecordValue::new(vec![
            RecordField::new("SPECTRAL_WINDOW_ID", Value::Scalar(ScalarValue::Int32(0))),
            RecordField::new("POLARIZATION_ID", Value::Scalar(ScalarValue::Int32(0))),
            RecordField::new("FLAG_ROW", Value::Scalar(ScalarValue::Bool(false))),
        ]))
        .unwrap();
    ms.subtable_mut(SubtableId::DataDescription)
        .unwrap()
        .add_row(RecordValue::new(vec![
            RecordField::new("SPECTRAL_WINDOW_ID", Value::Scalar(ScalarValue::Int32(1))),
            RecordField::new("POLARIZATION_ID", Value::Scalar(ScalarValue::Int32(0))),
            RecordField::new("FLAG_ROW", Value::Scalar(ScalarValue::Bool(false))),
        ]))
        .unwrap();

    let schema = ms.main_table().schema().unwrap().clone();
    for (field_id, data_desc_id) in [(0, 0), (0, 1), (1, 0), (1, 1)] {
        let row_fields: Vec<RecordField> = schema
            .columns()
            .iter()
            .map(|col| match col.name() {
                "FIELD_ID" => {
                    RecordField::new(col.name(), Value::Scalar(ScalarValue::Int32(field_id)))
                }
                "DATA_DESC_ID" => {
                    RecordField::new(col.name(), Value::Scalar(ScalarValue::Int32(data_desc_id)))
                }
                name => RecordField::new(name, default_value(name)),
            })
            .collect();
        ms.main_table_mut()
            .add_row(RecordValue::new(row_fields))
            .unwrap();
    }

    let rows = apply_for_test(&MsSelection::new().field(&[0]).spw(&[1]), &ms);
    assert_eq!(rows, vec![1]);
}

// SPDX-License-Identifier: LGPL-3.0-or-later
use super::*;
use casa_tables::table_measures::{MeasRefDesc, default_frequency_ref_map};

#[test]
fn mapped_spectral_references_survive_msv2_creation() {
    let root = tempfile::tempdir().unwrap();
    let oracle: serde_json::Value = serde_json::from_str(include_str!(
        "../../../casars-python/python/tests/data/casa_undefined_frequency_ms.json"
    ))
    .unwrap();
    for (name, input_code, remapped, output_code) in [
        ("canonical", 64, false, 64),
        ("undefined_remapped", 9, true, 64),
        ("lsrk_remapped", 64, true, 1),
    ] {
        let input = edited(root.path(), name, 0..0, |ms| {
            let spw = ms.subtable_mut(SubtableId::SpectralWindow).unwrap();
            for row in 0..spw.row_count() {
                spw.cell_accessor_mut(row, "MEAS_FREQ_REF")
                    .unwrap()
                    .set(Value::Scalar(ScalarValue::Int32(input_code)))
                    .unwrap();
            }
            for column in ["CHAN_FREQ", "REF_FREQUENCY"] {
                let (types, codes) = if remapped {
                    (
                        vec!["LSRK".to_string(), "Undefined".to_string()],
                        vec![64, 9],
                    )
                } else {
                    default_frequency_ref_map()
                };
                TableMeasDesc::new_variable_int(
                    column,
                    MeasureType::Frequency,
                    "MEAS_FREQ_REF",
                    types,
                    codes,
                )
                .unwrap()
                .write(spw)
                .unwrap();
            }
        });
        if remapped {
            let source = MeasurementSet::open(&input).unwrap();
            let source_window = source.subtable(SubtableId::SpectralWindow).unwrap();
            let observed = casa_ms::columns::frequency_columns::ChanFreqColumn::new(source_window)
                .get_ref_frequency(0)
                .unwrap();
            let expected = &oracle["results"][if input_code == 64 {
                "lsrk"
            } else {
                "undefined"
            }];
            assert_eq!(
                observed.refer().as_str(),
                expected["refer"].as_str().unwrap()
            );
            assert_eq!(observed.hz(), expected["m0"]["value"].as_f64().unwrap());
        }
        let output = root.path().join(format!("{name}.ms"));
        CreationPlan::new(
            &[CreationInput::new(&input)],
            &options(),
            Destination::New(output.clone()),
        )
        .and_then(CreationPlan::write)
        .unwrap();
        let ms = MeasurementSet::open(output).unwrap();
        let spw = ms.subtable(SubtableId::SpectralWindow).unwrap();
        assert_eq!(
            spw.cell_accessor(0, "MEAS_FREQ_REF")
                .unwrap()
                .value()
                .unwrap(),
            Some(&Value::Scalar(ScalarValue::Int32(output_code)))
        );
        for column in ["CHAN_FREQ", "REF_FREQUENCY"] {
            let desc = TableMeasDesc::reconstruct(spw, column).unwrap();
            let MeasRefDesc::VariableInt {
                tab_ref_types,
                tab_ref_codes,
                ..
            } = desc.ref_desc()
            else {
                panic!("variable reference")
            };
            let index = tab_ref_types
                .iter()
                .position(|name| name == "Undefined")
                .unwrap();
            assert_eq!(tab_ref_codes[index], 64);
        }
    }
}

#[test]
fn frequency_references_without_mapping_keywords_match_casa_and_survive_creation() {
    let root = tempfile::tempdir().unwrap();
    let oracle: serde_json::Value = serde_json::from_str(include_str!(
        "../../../casars-python/python/tests/data/casa_undefined_frequency_ms.json"
    ))
    .unwrap();
    for (name, code) in [("unmapped_bary", 3), ("unmapped_undefined", 64)] {
        let input = edited(root.path(), name, 0..0, |ms| {
            let spw = ms.subtable_mut(SubtableId::SpectralWindow).unwrap();
            for row in 0..spw.row_count() {
                spw.cell_accessor_mut(row, "MEAS_FREQ_REF")
                    .unwrap()
                    .set(Value::Scalar(ScalarValue::Int32(code)))
                    .unwrap();
            }
            for column in ["CHAN_FREQ", "REF_FREQUENCY"] {
                TableMeasDesc::new_variable_string(column, MeasureType::Frequency, "MEAS_FREQ_REF")
                    .write(spw)
                    .unwrap();
            }
        });
        let source = MeasurementSet::open(&input).unwrap();
        let spw = source.subtable(SubtableId::SpectralWindow).unwrap();
        let observed = casa_ms::columns::frequency_columns::ChanFreqColumn::new(spw)
            .get_ref_frequency(0)
            .unwrap();
        assert_eq!(
            observed.refer().as_str(),
            oracle["results"][name]["refer"].as_str().unwrap()
        );
        assert_eq!(
            observed.hz(),
            oracle["results"][name]["m0"]["value"].as_f64().unwrap()
        );
        drop(source);
        let output = root.path().join(format!("{name}-created.ms"));
        CreationPlan::new(
            &[CreationInput::new(&input)],
            &options(),
            Destination::New(output.clone()),
        )
        .and_then(CreationPlan::write)
        .unwrap();
        let output = MeasurementSet::open(output).unwrap();
        let reference = casa_ms::columns::frequency_columns::ChanFreqColumn::new(
            output.subtable(SubtableId::SpectralWindow).unwrap(),
        )
        .get_ref_frequency(0)
        .unwrap();
        assert_eq!(
            reference.refer().as_str(),
            oracle["results"][name]["refer"].as_str().unwrap()
        );
        assert_eq!(reference.hz(), observed.hz());
    }
}

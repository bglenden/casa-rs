// SPDX-License-Identifier: LGPL-3.0-or-later
//! casa-rs reads the component lists CASA 6.7.6 wrote, and writes them back
//! in CASA's layout (simulation Phase 2, #841).
//!
//! The fixtures under `tests/data/casa-6.7.6` were written by CASA's `cl`
//! tool (C++) through `tools/simulation/componentlist_oracle.py fixture`,
//! which also recorded what CASA reads back: `cl.getcomponent(i)` for every
//! component and every cell through casacore's table tool.

mod common;

use casa_components::{ComponentList, ShapeType, SpectralKind};
use casa_tables::{ArrayShapeContract, ColumnType, Table, TableOptions};
use casa_types::{ArrayValue, PrimitiveType, RecordValue, ScalarValue, Value};
use common::{assert_matches_casa, copy_table, fixture_dir, load_json};
use serde_json::Value as Json;

fn read_fixture(name: &str, scratch: &std::path::Path) -> ComponentList {
    let table = copy_table(&fixture_dir().join(name), scratch);
    ComponentList::read_table(&table).unwrap_or_else(|error| panic!("{name}: {error}"))
}

#[test]
fn reads_casa_component_lists_as_casa_reads_them() {
    let scratch = tempfile::tempdir().expect("scratch");
    let list = read_fixture("components.cl", scratch.path());
    let document = load_json(&fixture_dir().join("components.json"));
    assert_eq!(document["casa_version"].as_str(), Some("6.7.6-14"));
    assert_matches_casa(&list, &document, "components.cl");

    // The cases the fixture was built to cover.
    let shapes: Vec<ShapeType> = list.iter().map(|c| c.shape().shape_type()).collect();
    for shape in ShapeType::ALL {
        assert!(shapes.contains(&shape), "fixture lacks {shape:?}");
    }
    let limb = &list.components()[5];
    assert_eq!(limb.limb_darkening_exponent(), Some(0.7));
    assert_eq!(list.components()[0].limb_darkening_exponent(), None);
    // RA 20h is stored as -60 degrees; the direction is the same.
    let (longitude, _) = list.components()[0]
        .shape()
        .reference_direction()
        .as_angles();
    assert!((longitude.to_degrees() - 300.0).abs() < 1e-9, "{longitude}");
    // CASA stores a Gaussian's position angle in [0, 180) degrees.
    let pa = list.components()[1].shape().parameters()[2].to_degrees();
    assert!((pa - 20.0).abs() < 1e-9, "{pa}");
    let SpectralKind::SpectralIndex(stokes) = list.components()[3].spectrum().kind() else {
        panic!("row 3 is a spectral index");
    };
    assert_eq!(stokes.stokes_indices(), Some([-0.8, 0.1, 50.0, 0.0]));
    let error = list.components()[1].spectrum().reference_frequency_error();
    assert_eq!((error.value(), error.unit().name()), (5.0, "kHz"));

    // casa-tables reads the description header CASA wrote.
    let table = Table::open(TableOptions::new(scratch.path().join("components.cl"))).expect("open");
    let header = table.schema().expect("schema").header().clone();
    assert_eq!(
        (
            header.name.as_str(),
            header.version.as_str(),
            header.comment.as_str()
        ),
        (
            "ComponentListDescription",
            "4",
            "A description of a component list"
        )
    );
}

#[test]
fn reads_lists_from_before_the_error_columns() {
    let scratch = tempfile::tempdir().expect("scratch");
    let list = read_fixture("old_format.cl", scratch.path());
    let document = load_json(&fixture_dir().join("old_format.json"));
    let columns = document["cells"].as_object().expect("cells");
    for absent in [
        "Flux_Error",
        "Direction_Error",
        "Shape_Error",
        "Frequency_Error",
        "Spectral_Error",
        "Spectral_Record",
    ] {
        assert!(!columns.contains_key(absent), "{absent} should be absent");
    }
    assert_matches_casa(&list, &document, "old_format.cl");
    for component in &list {
        assert_eq!(component.shape().errors().iter().sum::<f64>(), 0.0);
        assert_eq!(
            component.spectrum().reference_frequency_error().value(),
            0.0
        );
    }
}

#[test]
fn writes_casa_lists_back_in_casa_layout() {
    let scratch = tempfile::tempdir().expect("scratch");
    let list = read_fixture("components.cl", scratch.path());
    let written = scratch.path().join("rewritten.cl");
    list.write_table(&written).expect("write");
    assert!(matches!(
        list.write_table(&written),
        Err(casa_components::ComponentListError::Exists(_))
    ));
    let back = ComponentList::read_table(&written).expect("read back");
    common::assert_same_list(&list, &back, "fixture rewritten by casa-rs");

    let document = load_json(&fixture_dir().join("components.json"));
    let table = Table::open(TableOptions::new(&written)).expect("open");
    let schema = table.schema().expect("schema");
    let header = schema.header();
    assert_eq!(
        (
            header.name.as_str(),
            header.version.as_str(),
            header.comment.as_str()
        ),
        (
            "ComponentListDescription",
            "4",
            "A description of a component list"
        )
    );
    let casa_info = &document["info"];
    assert_eq!(
        table.info().table_type,
        casa_info["type"].as_str().expect("type")
    );
    assert_eq!(
        table.info().sub_type,
        casa_info["subType"].as_str().expect("subType")
    );
    assert_eq!(
        table.info().readme.join("\n") + "\n",
        casa_info["readme"].as_str().expect("readme")
    );

    let names: Vec<&str> = schema
        .columns()
        .iter()
        .map(|column| column.name())
        .collect();
    let casa_names: Vec<&str> = document["column_names"]
        .as_array()
        .expect("column names")
        .iter()
        .map(|name| name.as_str().expect("name"))
        .collect();
    assert_eq!(names, casa_names, "column order");
    for column in schema.columns() {
        let casa = &document["columns"][column.name()];
        let what = column.name();
        assert_eq!(
            column.comment(),
            casa["comment"].as_str().expect("comment"),
            "{what}: comment"
        );
        let option = casa["option"].as_u64().expect("option");
        assert_eq!(column.options().direct, option & 1 != 0, "{what}: Direct");
        assert_eq!(
            column.options().undefined,
            option & 2 != 0,
            "{what}: Undefined"
        );
        let value_type = match column.column_type() {
            ColumnType::Record => "record",
            _ => match column.data_type().expect("data type") {
                PrimitiveType::Complex64 => "dcomplex",
                PrimitiveType::Float64 => "double",
                PrimitiveType::String => "string",
                other => panic!("{what}: unexpected {other:?}"),
            },
        };
        assert_eq!(casa["valueType"].as_str(), Some(value_type), "{what}: type");
        match column.column_type() {
            ColumnType::Array(ArrayShapeContract::Fixed { shape }) => {
                let casa_shape: Vec<usize> = common::reals(&casa["shape"])
                    .iter()
                    .map(|v| *v as usize)
                    .collect();
                assert_eq!(shape, &casa_shape, "{what}: shape");
            }
            ColumnType::Array(ArrayShapeContract::Variable { ndim }) => {
                assert!(casa.get("shape").is_none(), "{what}: variable shape");
                assert_eq!(
                    ndim.map(|n| n as u64),
                    casa["ndim"].as_u64(),
                    "{what}: ndim"
                );
            }
            _ => {}
        }
        let keywords = table
            .column_keywords(column.name())
            .map(record_json)
            .unwrap_or(Json::Object(Default::default()));
        assert_eq!(keywords, casa["keywords"], "{what}: keywords");
        assert_eq!(
            data_manager_of(&table, column.name()),
            casa["dataManagerType"].as_str().expect("data manager"),
            "{what}: data manager"
        );
    }
}

fn data_manager_of(table: &Table, column: &str) -> String {
    table
        .data_manager_info()
        .iter()
        .find(|manager| manager.columns.iter().any(|name| name == column))
        .map(|manager| manager.dm_type.clone())
        .unwrap_or_default()
}

/// A keyword record as the oracle's JSON writes it.
fn record_json(record: &RecordValue) -> Json {
    Json::Object(
        record
            .fields()
            .iter()
            .map(|field| (field.name.clone(), value_json(&field.value)))
            .collect(),
    )
}

fn value_json(value: &Value) -> Json {
    match value {
        Value::Scalar(ScalarValue::String(text)) => Json::String(text.clone()),
        Value::Array(ArrayValue::String(texts)) => Json::Array(
            texts
                .iter()
                .map(|text| Json::String(text.clone()))
                .collect(),
        ),
        Value::Record(record) => record_json(record),
        other => panic!("unexpected keyword value {other:?}"),
    }
}

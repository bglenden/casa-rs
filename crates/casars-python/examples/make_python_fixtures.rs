// SPDX-License-Identifier: LGPL-3.0-or-later

use std::fs;
use std::path::{Path, PathBuf};

use casa_coordinates::{
    CoordinateSystem, DirectionCoordinate, LinearCoordinate, Projection, ProjectionType,
    SpectralCoordinate, StokesCoordinate, StokesType,
};
use casa_images::{GaussianBeam, Image, ImageBeamSet, ImageInfo, ImageType};
use casa_tables::{ColumnSchema, Table, TableOptions, TableSchema};
use casa_types::measures::direction::DirectionRef;
use casa_types::measures::frequency::FrequencyRef;
use casa_types::{
    ArrayD, ArrayValue, Complex64, PrimitiveType, RecordField, RecordValue, ScalarValue, Value,
};
use ndarray::IxDyn;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let output_dir = std::env::args().nth(1).map(PathBuf::from).ok_or(
        "usage: cargo run -p casars-python --example make_python_fixtures -- <output-dir>",
    )?;

    fs::create_dir_all(&output_dir)?;

    let table_path = output_dir.join("python_fixture.table");
    let image_path = output_dir.join("python_fixture.image");
    let model_path = output_dir.join("simulator_model.image");

    create_table_fixture(&table_path)?;
    create_image_fixture(&image_path)?;
    create_sky_model_fixture(&model_path)?;

    println!("{}", table_path.display());
    println!("{}", image_path.display());
    println!("{}", model_path.display());
    Ok(())
}

/// A sky model `sm.predict` reads: 32 × 32 pixels of 4″ around J2000
/// 23h30m +34° (the simulator tests' field), Stokes I, one plane spanning
/// 1.4 GHz to 1.404 GHz, with three points.
fn create_sky_model_fixture(path: &Path) -> Result<(), Box<dyn std::error::Error>> {
    const PIXELS: usize = 32;
    if path.exists() {
        fs::remove_dir_all(path)?;
    }
    let cell = 2.0e-5;
    let mut coords = CoordinateSystem::new();
    coords.add_coordinate(DirectionCoordinate::new(
        DirectionRef::J2000,
        Projection::new(ProjectionType::SIN),
        [352.5_f64.to_radians(), 34.0_f64.to_radians()],
        [-cell, cell],
        [(PIXELS / 2) as f64, (PIXELS / 2) as f64],
    ));
    coords.add_coordinate(StokesCoordinate::new(vec![StokesType::I]));
    coords.add_coordinate(SpectralCoordinate::new(
        FrequencyRef::LSRK,
        1.4015e9,
        4.0e6,
        0.0,
        1.4015e9,
    ));
    let shape = vec![PIXELS, PIXELS, 1, 1];
    let mut image = Image::create(shape.clone(), coords, path)?;
    let mut pixels = ArrayD::zeros(IxDyn(&shape));
    for (x, y, flux) in [(16, 16, 1.0_f32), (20, 13, 0.5), (5, 27, 0.25)] {
        pixels[IxDyn(&[x, y, 0, 0])] = flux;
    }
    image.put_slice(&pixels, &[0, 0, 0, 0])?;
    image.set_units("Jy/pixel")?;
    image.save()?;
    Ok(())
}

fn create_table_fixture(path: &Path) -> Result<(), Box<dyn std::error::Error>> {
    if path.exists() {
        fs::remove_dir_all(path)?;
    }

    let schema = TableSchema::new(vec![
        ColumnSchema::scalar("id", PrimitiveType::Int64),
        ColumnSchema::scalar("label", PrimitiveType::String),
        ColumnSchema::scalar("gain", PrimitiveType::Complex64),
        ColumnSchema::array_fixed("spectrum", PrimitiveType::Float32, vec![2, 2]),
        ColumnSchema::array_variable("vary", PrimitiveType::Float64, Some(1)),
        ColumnSchema::record("meta"),
    ])?;

    let rows = vec![
        RecordValue::new(vec![
            RecordField::new("id", Value::Scalar(ScalarValue::Int64(1))),
            RecordField::new("label", Value::Scalar(ScalarValue::String("alpha".into()))),
            RecordField::new(
                "gain",
                Value::Scalar(ScalarValue::Complex64(Complex64::new(1.0, -1.0))),
            ),
            RecordField::new(
                "spectrum",
                Value::Array(ArrayValue::Float32(array_f32(
                    &[2, 2],
                    &[1.0, 2.0, 3.0, 4.0],
                ))),
            ),
            RecordField::new(
                "vary",
                Value::Array(ArrayValue::Float64(array_f64(&[2], &[10.0, 11.0]))),
            ),
            RecordField::new("meta", Value::Record(row_meta("alpha", 10))),
        ]),
        RecordValue::new(vec![
            RecordField::new("id", Value::Scalar(ScalarValue::Int64(2))),
            RecordField::new("label", Value::Scalar(ScalarValue::String("beta".into()))),
            RecordField::new(
                "gain",
                Value::Scalar(ScalarValue::Complex64(Complex64::new(2.0, -2.0))),
            ),
            RecordField::new(
                "spectrum",
                Value::Array(ArrayValue::Float32(array_f32(
                    &[2, 2],
                    &[5.0, 6.0, 7.0, 8.0],
                ))),
            ),
            RecordField::new(
                "vary",
                Value::Array(ArrayValue::Float64(array_f64(&[3], &[20.0, 21.0, 22.0]))),
            ),
            RecordField::new("meta", Value::Record(row_meta("beta", 20))),
        ]),
        RecordValue::new(vec![
            RecordField::new("id", Value::Scalar(ScalarValue::Int64(3))),
            RecordField::new("label", Value::Scalar(ScalarValue::String("gamma".into()))),
            RecordField::new(
                "gain",
                Value::Scalar(ScalarValue::Complex64(Complex64::new(3.0, -3.0))),
            ),
            RecordField::new(
                "spectrum",
                Value::Array(ArrayValue::Float32(array_f32(
                    &[2, 2],
                    &[9.0, 10.0, 11.0, 12.0],
                ))),
            ),
            RecordField::new(
                "vary",
                Value::Array(ArrayValue::Float64(array_f64(&[1], &[30.0]))),
            ),
            RecordField::new("meta", Value::Record(row_meta("gamma", 30))),
        ]),
    ];

    let mut table = Table::from_rows_with_schema(rows, schema)?;
    table.keywords_mut().push(RecordField::new(
        "observer",
        Value::Scalar(ScalarValue::String("python-fixture".into())),
    ));
    table.keywords_mut().push(RecordField::new(
        "version",
        Value::Scalar(ScalarValue::Int32(1)),
    ));

    let spectrum_keywords = RecordValue::new(vec![
        RecordField::new("unit", Value::Scalar(ScalarValue::String("Jy".into()))),
        RecordField::new("frame", Value::Scalar(ScalarValue::String("LSRK".into()))),
    ]);
    table.set_column_keywords("spectrum", spectrum_keywords);
    table.save(TableOptions::new(path))?;
    Ok(())
}

fn create_image_fixture(path: &Path) -> Result<(), Box<dyn std::error::Error>> {
    if path.exists() {
        fs::remove_dir_all(path)?;
    }

    let mut coords = CoordinateSystem::new();
    coords.add_coordinate(LinearCoordinate::new(
        3,
        vec!["X".into(), "Y".into(), "Plane".into()],
        vec!["pix".into(), "pix".into(), "pix".into()],
    ));

    let mut image = Image::create(vec![4, 3, 2], coords, path)?;
    image.set_units("Jy/beam")?;
    image.set_image_info(&ImageInfo {
        beam_set: ImageBeamSet::new(GaussianBeam::new(1.0e-4, 8.0e-5, 0.25)),
        image_type: ImageType::Intensity,
        object_name: "python-fixture".into(),
    })?;
    image.set_misc_info(RecordValue::new(vec![
        RecordField::new(
            "purpose",
            Value::Scalar(ScalarValue::String("python-tests".into())),
        ),
        RecordField::new("revision", Value::Scalar(ScalarValue::Int32(1))),
    ]))?;

    let pixels = ArrayD::from_shape_vec(
        IxDyn(&[4, 3, 2]),
        (0..24).map(|value| value as f32).collect(),
    )?;
    image.put_slice(&pixels, &[0, 0, 0])?;

    let mask = ArrayD::from_shape_vec(
        IxDyn(&[4, 3, 2]),
        (0..24).map(|value| value % 2 == 0).collect(),
    )?;
    image.put_mask("quality", &mask)?;
    image.set_default_mask("quality")?;
    image.save()?;
    Ok(())
}

fn row_meta(label: &str, weight: i32) -> RecordValue {
    RecordValue::new(vec![
        RecordField::new(
            "label",
            Value::Scalar(ScalarValue::String(label.to_string())),
        ),
        RecordField::new("weight", Value::Scalar(ScalarValue::Int32(weight))),
    ])
}

fn array_f32(shape: &[usize], values: &[f32]) -> ArrayD<f32> {
    ArrayD::from_shape_vec(IxDyn(shape), values.to_vec()).expect("valid f32 array shape")
}

fn array_f64(shape: &[usize], values: &[f64]) -> ArrayD<f64> {
    ArrayD::from_shape_vec(IxDyn(shape), values.to_vec()).expect("valid f64 array shape")
}

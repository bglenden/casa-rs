// SPDX-License-Identifier: LGPL-3.0-or-later
//! Independent compact ALMA+ACA+two-TP-antenna CASA calculation products.
use casa_core::ArrayValue;
use casa_images::{ImageInterface, PagedImage};
use casa_ms::MeasurementSet;
use casa_simulation::{ExecutionControls, SessionResources, simalma::simalma};
#[path = "common/simalma_fixture.rs"]
mod simalma_fixture;
use serde_json::{Value, json};
use std::path::Path;

fn flatten(value: &Value, out: &mut Vec<f64>) {
    if let Some(values) = value.as_array() {
        for value in values {
            flatten(value, out);
        }
    } else if value.is_null() {
        out.push(0.);
    } else {
        out.push(
            value
                .as_f64()
                .or_else(|| value.as_bool().map(|v| if v { 1. } else { 0. }))
                .unwrap(),
        );
    }
}

fn image(path: &Path, expected: &Value) -> bool {
    let image = PagedImage::<f32>::open(path).unwrap();
    assert_eq!(json!(image.shape()), expected["shape"]);
    let pixels = image.get().unwrap();
    let mask = image
        .default_mask()
        .unwrap()
        .unwrap_or_else(|| ndarray::ArrayD::from_elem(ndarray::IxDyn(image.shape()), true));
    let mut truth = Vec::new();
    flatten(&expected["pixels"], &mut truth);
    let mut truth_mask = Vec::new();
    flatten(&expected["mask"], &mut truth_mask);
    assert!(
        mask.iter().zip(truth_mask).all(|(&a, b)| a == (b != 0.)),
        "{} mask",
        path.display()
    );
    let (mut error, mut power) = (0_f64, 0_f64);
    for ((&a, b), &valid) in pixels.iter().zip(truth).zip(mask.iter()) {
        if valid {
            assert!(a.is_finite() && b.is_finite());
            error += (f64::from(a) - b).powi(2);
            power += b * b;
        }
    }
    let nrms = (error / power.max(f64::MIN_POSITIVE)).sqrt();
    println!("{} NRMS {nrms:e}", path.display());
    if path.to_string_lossy().ends_with(".fidelity") {
        assert_eq!(image.units(), "");
    } else {
        assert_eq!(image.units(), expected["unit"].as_str().unwrap());
    }
    nrms <= 1e-3
}

fn ms(path: &Path, expected: &Value) -> bool {
    let set = MeasurementSet::open(path).unwrap();
    let main = set.main_table();
    let expected = &expected["DATA"];
    let rows = expected["shape"][2].as_u64().unwrap() as usize;
    assert_eq!(main.row_count(), rows);
    let data = main.column_accessor("DATA").unwrap();
    let (mut error, mut power) = (0_f64, 0_f64);
    for row in 0..rows {
        let ArrayValue::Complex32(cell) = data.array_cell(row).unwrap() else {
            panic!("complex DATA")
        };
        for (index, &a) in cell.indexed_iter() {
            let real = expected["real"][index[0]][index[1]][row].as_f64().unwrap();
            let imag = expected["imag"][index[0]][index[1]][row].as_f64().unwrap();
            error += (f64::from(a.re) - real).powi(2) + (f64::from(a.im) - imag).powi(2);
            power += real * real + imag * imag;
        }
    }
    let nrms = (error / power.max(f64::MIN_POSITIVE)).sqrt();
    println!("{} DATA NRMS {nrms:e}", path.display());
    nrms <= 1e-6
}

#[test]
fn complete_alma_aca_tp_calculations_match_casa() {
    let resources = SessionResources::from_controls(&ExecutionControls::default()).unwrap();
    let (scratch, request, oracle) = simalma_fixture::fixture(resources);
    let root = scratch.path();
    let outcome = simalma(&request).unwrap();
    assert!(
        outcome
            .products
            .iter()
            .any(|p| p.ends_with("combo.feather.image"))
    );
    let mut passed = true;
    for (name, expected) in oracle["images"].as_object().unwrap() {
        passed &= image(&root.join("combo").join(name), expected);
    }
    for (name, expected) in oracle["measurementsets"].as_object().unwrap() {
        passed &= ms(&root.join("combo").join(name), expected);
    }
    assert!(passed, "independent CASA comparison failed");
}

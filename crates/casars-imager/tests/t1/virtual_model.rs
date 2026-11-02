// SPDX-License-Identifier: LGPL-3.0-or-later

//! Standard gridder, natural weighting, MFS Hogbom with the final model
//! saved as CASA's virtual model (`savemodel='virtual'`): the record CASA's
//! visibility iterator predicts from, and the padded grid it degrids,
//! written beside SOURCE in place of a `MODEL_DATA` column.

use std::collections::BTreeSet;
use std::f64::consts::TAU;

use casa_images::PagedImage;
use casa_ms::{MeasurementSet, VirtualModelEntry, VirtualModelKind, virtual_model};
use casa_types::{Complex32, Complex64, ScalarValue, Value};
use serde_json::json;

use super::fixture::{Component, Geometry, Observation, component_pixel, peak};

const POINT: Component = Component {
    offset_px: [24, 16],
    flux_jy: 1.0,
    fwhm_px: None,
    spectral_index: 0.0,
};

fn controls(savemodel: &str) -> serde_json::Value {
    json!({
        "weighting": "natural",
        "deconvolver": "hogbom",
        "niter": 200,
        "threshold": "0.005Jy",
        "savemodel": savemodel,
    })
}

/// Field 0's virtual model's one entry, after a run.
fn entry(observation: &Observation) -> VirtualModelEntry {
    let ms = MeasurementSet::open(observation.measurement_set()).expect("open the MS");
    let found = virtual_model(&ms, 0)
        .expect("read the virtual model")
        .expect("field 0 has a virtual model");
    let mut entries = VirtualModelEntry::entries(&found.record).expect("a version-2 record");
    assert_eq!(entries.len(), 1, "one image domain, one machine");
    entries.remove(0)
}

fn text<'a>(entry: &'a VirtualModelEntry, name: &str) -> &'a str {
    match entry.container.get(name) {
        Some(Value::Scalar(ScalarValue::String(value))) => value,
        other => panic!("{name}: {other:?}"),
    }
}

/// The run saves its model as CASA does: no `MODEL_DATA`, one `GridFT`
/// covering every selected combination, and its grid image of the padded
/// grid's shape. The grid is the forward transform of the corrected model
/// about the grid's centre, as `GridFT::prepGridForDegrid` forms it: a
/// model concentrated on the source pixel turns one cell along u (x) by
/// `−2π dx / nx`, and along v by `−2π dy / ny`, of its offset from the
/// reference pixel; its magnitude is flat there.
#[test]
fn a_virtual_model_holds_the_gridded_final_model() {
    let observation = Observation::synthesise(&[("point", POINT)]);
    let (_, products) = observation.image("virtual-model", controls("virtual"));
    let ms = MeasurementSet::open(observation.measurement_set()).expect("open the MS");
    assert!(
        !ms.main_table()
            .schema()
            .is_some_and(|schema| schema.contains_column("MODEL_DATA")),
        "a virtual model adds no MODEL_DATA"
    );
    drop(ms);
    let entry = entry(&observation);
    assert_eq!(entry.kind, VirtualModelKind::FtMachine);
    assert_eq!(text(&entry, "name"), "GridFT");
    assert!(entry.index_combination.iter().all(|row| row[0] == 0));
    let grid = PagedImage::<Complex32>::open(text(&entry, "diskimage")).expect("the grid image");
    let shape = grid.shape().to_vec();
    let [nx, ny] = [shape[0], shape[1]];
    assert_eq!((shape[2], shape[3]), (1, 1));
    assert!(nx > 256 && ny > 256, "the padded grid, {shape:?}");
    let (_, source, _) = peak(&products.get(".model").pixels);
    assert_eq!(source, component_pixel(POINT), "the model's peak");
    let reference = [nx / 2, ny / 2];
    let cell = |x: usize, y: usize| {
        let value = grid.get_at(&[x, y, 0, 0]).expect("a grid cell");
        Complex64::new(f64::from(value.re), f64::from(value.im))
    };
    let centre = cell(reference[0], reference[1]);
    let model_jy: f64 = products
        .get(".model")
        .pixels
        .iter()
        .map(|&v| f64::from(v))
        .sum();
    assert!(
        centre.im.abs() < 1e-6 * centre.re && centre.re >= model_jy,
        "the uv origin holds the corrected flux: {centre} for {model_jy} Jy"
    );
    let centre_pixel = Geometry::DEFAULT.centre();
    let offset = [
        source[0] as f64 - centre_pixel[0] as f64,
        source[1] as f64 - centre_pixel[1] as f64,
    ];
    for (axis, step) in [[1, 0], [0, 1]].into_iter().enumerate() {
        let next = cell(reference[0] + step[0], reference[1] + step[1]);
        let turn = (next / centre).arg();
        let expected = -TAU * offset[axis] / [nx, ny][axis] as f64;
        assert!(
            (turn - expected).abs() < 2e-3 && ((next.norm() / centre.norm()) - 1.0).abs() < 2e-2,
            "axis {axis}: turn {turn} for {expected}, |ratio| {}",
            next.norm() / centre.norm()
        );
    }
}

/// A second run replaces the model (CASA clears the selection's models
/// first), deleting the first run's grid; a model column run then clears
/// it, as CASA's `addScrCols` does.
#[test]
fn a_virtual_model_is_replaced_and_cleared_as_casa_does() {
    let observation = Observation::synthesise(&[("point", POINT)]);
    observation.image("first", controls("virtual"));
    let first = text(&entry(&observation), "diskimage").to_string();
    observation.image("second", controls("virtual"));
    let second = entry(&observation);
    assert_ne!(text(&second, "diskimage"), first);
    assert!(
        !std::path::Path::new(&first).exists(),
        "the first grid is deleted"
    );
    assert!(std::path::Path::new(text(&second, "diskimage")).exists());
    observation.image("column", controls("modelcolumn"));
    let ms = MeasurementSet::open(observation.measurement_set()).expect("open the MS");
    assert!(virtual_model(&ms, 0).expect("read").is_none());
    assert!(!std::path::Path::new(text(&second, "diskimage")).exists());
}

/// The model is written for the rows and channels the run images, as the
/// run's own grammars resolved them: a window named by its `NAME` with a
/// channel range, and one baseline (`A&B`). Every combination of the
/// baseline's MAIN rows, and only those, is recorded, with the named
/// channels; casa-ms's older row parser reads neither form.
#[test]
fn a_virtual_model_is_written_for_the_selection_the_run_images() {
    let observation = Observation::synthesise(&[("point", POINT)]);
    let ms = MeasurementSet::open(observation.measurement_set()).expect("open the MS");
    let antennas = ms.antenna().expect("ANTENNA");
    let names = [0, 1].map(|row| antennas.name(row).expect("an antenna name"));
    let int = |row: usize, column: &str| match ms
        .main_table()
        .cell_accessor(row, column)
        .expect("a MAIN cell")
        .scalar()
        .expect("a scalar cell")
    {
        ScalarValue::Int32(value) => *value,
        other => panic!("{column}: {other:?}"),
    };
    let expected = (0..ms.row_count())
        .filter(|&row| {
            let pair = (int(row, "ANTENNA1"), int(row, "ANTENNA2"));
            pair == (0, 1) || pair == (1, 0)
        })
        .map(|row| {
            [
                int(row, "FIELD_ID"),
                int(row, "DATA_DESC_ID"),
                int(row, "SCAN_NUMBER"),
                int(row, "STATE_ID"),
            ]
        })
        .collect::<BTreeSet<_>>();
    drop(antennas);
    drop(ms);
    assert!(!expected.is_empty(), "the baseline has rows");
    let mut values = controls("virtual");
    values["spw"] = json!("t1-qband:1~2");
    values["antenna"] = json!(format!("{}&{}", names[0], names[1]));
    observation.image("virtual-selection", values);
    let entry = entry(&observation);
    // One window, data description 0: the combinations name window 0.
    let recorded = entry
        .index_combination
        .iter()
        .copied()
        .collect::<BTreeSet<_>>();
    assert_eq!(recorded, expected, "the baseline's combinations");
    assert_eq!(entry.channel_selection, [[0, 1, 2, 1]], "channels 1 and 2");
}

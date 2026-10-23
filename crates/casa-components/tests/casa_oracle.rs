// SPDX-License-Identifier: LGPL-3.0-or-later
//! CASA reads the component lists casa-rs writes, rewrites them, and casa-rs
//! reads CASA's rewrite (simulation Phase 2, #841).
//!
//! Opt-in: it runs CASA's Python (`CASA_RS_CASA_PYTHON`, or the default
//! `/Applications/CASA.app` install) and skips when neither exists.
//!
//! ```text
//! cargo test -p casa-components --test casa_oracle -- --ignored --nocapture
//! ```
//!
//! For two lists (the CASA fixture as casa-rs reads it, and one built through
//! casa-rs's constructors):
//!
//! 1. casa-rs writes the list;
//! 2. CASA's `cl.open` and `cl.getcomponent` read it, and every component and
//!    cell must match the list (`common::assert_matches_casa`), and every
//!    column description and keyword must match those of the table CASA
//!    itself wrote;
//! 3. CASA opens it for update and closes it, so `writeTable` rewrites it in
//!    place, and casa-rs reads the rewrite back unchanged.

mod common;

use std::path::{Path, PathBuf};
use std::process::Command;

use casa_components::{
    ComponentList, ComponentShape, DirectionError, Ellipse, EllipseErrors, Flux, Polarisation,
    PowerLogPolynomial, ShapeKind, SkyComponent, SpectralIndex, SpectralKind, SpectralModel,
    TabularSpectrum,
};
use casa_types::measures::direction::{DirectionRef, MDirection};
use casa_types::measures::frequency::{FrequencyRef, MFrequency};
use casa_types::quanta::Quantity;
use common::{assert_matches_casa, assert_same_list, copy_dir, copy_table, fixture_dir, load_json};
use num_complex::Complex64;
use serde_json::Value;

const DEFAULT_CASA_PYTHON: &str = "/Applications/CASA.app/Contents/MacOS/python3";

fn casa_python() -> Option<PathBuf> {
    std::env::var_os("CASA_RS_CASA_PYTHON")
        .map(PathBuf::from)
        .or_else(|| Some(PathBuf::from(DEFAULT_CASA_PYTHON)))
        .filter(|path| path.exists())
}

fn oracle_script() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("../../tools/simulation/componentlist_oracle.py")
}

fn run_oracle(python: &Path, arguments: &[&str]) {
    let output = Command::new(python)
        .arg(oracle_script())
        .args(arguments)
        .output()
        .expect("run CASA's Python");
    assert!(
        output.status.success(),
        "componentlist_oracle.py {arguments:?} failed:\n{}\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
}

fn dump(python: &Path, table: &Path) -> Value {
    let json = table.with_extension("json");
    run_oracle(
        python,
        &[
            "dump",
            table.to_str().expect("UTF-8 path"),
            "--json",
            json.to_str().expect("UTF-8 path"),
        ],
    );
    load_json(&json)
}

/// A list built through casa-rs's constructors, one of each kind.
fn constructed_list() -> ComponentList {
    let arcsec = |value: f64| value.to_radians() / 3600.0;
    let j2000 = MDirection::from_angles(4.0, -0.4, DirectionRef::J2000);
    let point = SkyComponent::new(
        Flux::stokes_jy(1.0, 0.1, 0.0, 0.0).with_errors([Complex64::new(0.01, 0.0); 4]),
        ComponentShape::point(j2000.clone()).with_direction_error(
            DirectionError::new(
                Quantity::new(0.1, "arcsec").expect("arcsec"),
                Quantity::new(0.2, "arcsec").expect("arcsec"),
            )
            .expect("errors"),
        ),
        SpectralModel::new(
            MFrequency::new(1.4e9, FrequencyRef::LSRK),
            SpectralKind::SpectralIndex(SpectralIndex::new(-0.7).with_error(0.05).expect("error")),
        )
        .with_reference_frequency_error(Quantity::new(2.0, "MHz").expect("MHz"))
        .expect("frequency error"),
    )
    .with_label("constructed point");
    let gaussian = SkyComponent::new(
        Flux::new(
            [
                Complex64::new(2.0, 0.0),
                Complex64::new(0.1, 0.05),
                Complex64::new(0.1, -0.05),
                Complex64::new(1.8, 0.0),
            ],
            "Jy",
            Polarisation::Linear,
        )
        .expect("flux"),
        ComponentShape::new(
            MDirection::from_angles(1.0, 0.2, DirectionRef::B1950),
            ShapeKind::Gaussian {
                axes: Ellipse::new(arcsec(10.0), arcsec(4.0), -0.3).expect("axes"),
                errors: EllipseErrors {
                    major_axis: arcsec(0.1),
                    minor_axis: arcsec(0.05),
                    position_angle: 0.01,
                },
            },
        )
        .expect("gaussian"),
        SpectralModel::new(
            MFrequency::new(100.0e9, FrequencyRef::TOPO),
            SpectralKind::PowerLogPolynomial(
                PowerLogPolynomial::new(vec![0.2, -0.3])
                    .with_errors(vec![0.02, 0.03])
                    .expect("errors"),
            ),
        ),
    )
    .with_label("constructed Gaussian");
    let limb = SkyComponent::new(
        Flux::new([Complex64::new(5.0, 0.0); 4], "mJy", Polarisation::Circular).expect("flux"),
        ComponentShape::new(
            MDirection::from_angles(0.5, 0.7, DirectionRef::AZEL),
            ShapeKind::LimbDarkenedDisk {
                axes: Ellipse::new(arcsec(30.0), arcsec(30.0), 0.0).expect("axes"),
                errors: EllipseErrors::default(),
            },
        )
        .expect("ldisk"),
        SpectralModel::new(
            MFrequency::new(1.5e9, FrequencyRef::BARY),
            SpectralKind::Tabular(
                TabularSpectrum::new(
                    FrequencyRef::BARY,
                    vec![1.0e9, 2.0e9],
                    vec![[1.0, 0.0, 0.1, 0.0], [0.5, 0.0, 0.05, 0.01]],
                )
                .expect("tabulation"),
            ),
        ),
    )
    .with_optional_parameters(vec![1.3])
    .with_label("constructed limb-darkened disk");
    let disk = SkyComponent::new(
        Flux::stokes_jy(0.3, 0.0, 0.0, 0.0),
        ComponentShape::new(
            MDirection::from_angles(2.0, -1.2, DirectionRef::GALACTIC),
            ShapeKind::Disk {
                axes: Ellipse::new(arcsec(20.0), arcsec(10.0), 2.5).expect("axes"),
                errors: EllipseErrors::default(),
            },
        )
        .expect("disk"),
        SpectralModel::constant(MFrequency::new(43.0e9, FrequencyRef::LSRK)),
    );
    ComponentList::from(vec![point, gaussian, limb, disk])
}

/// Every column description and keyword CASA reports, but for the data
/// manager group (the names casa-rs gives its data managers differ).
fn column_descriptions(document: &Value) -> Vec<(String, Value)> {
    let names = document["column_names"].as_array().expect("column names");
    names
        .iter()
        .map(|name| {
            let name = name.as_str().expect("name").to_string();
            let mut description = document["columns"][&name].clone();
            description
                .as_object_mut()
                .expect("description")
                .remove("dataManagerGroup");
            (name, description)
        })
        .collect()
}

fn check_round_trip(python: &Path, list: &ComponentList, scratch: &Path, name: &str) {
    let written = scratch.join(format!("{name}.cl"));
    list.write_table(&written).expect("casa-rs writes");
    let document = dump(python, &written);
    assert_matches_casa(list, &document, &format!("CASA reading casa-rs's {name}"));

    let casa_fixture = load_json(&fixture_dir().join("components.json"));
    let casa_columns: std::collections::HashMap<_, _> =
        column_descriptions(&casa_fixture).into_iter().collect();
    for (column, description) in column_descriptions(&document) {
        assert_eq!(
            Some(&description),
            casa_columns.get(&column),
            "{name}: CASA's description of {column}"
        );
    }
    assert_eq!(document["info"], casa_fixture["info"], "{name}: table info");
    assert_eq!(
        document["data_managers"].as_array().map(|managers| managers
            .iter()
            .map(|manager| manager["type"].clone())
            .collect::<Vec<_>>()),
        Some(
            if list
                .iter()
                .any(|component| component.limb_darkening_exponent().is_some())
            {
                vec![Value::from("StandardStMan"), Value::from("TiledCellStMan")]
            } else {
                vec![Value::from("StandardStMan")]
            }
        ),
        "{name}: data managers"
    );

    let rewritten = scratch.join(format!("{name}-rewritten.cl"));
    copy_dir(&written, &rewritten);
    run_oracle(
        python,
        &["rewrite", rewritten.to_str().expect("UTF-8 path")],
    );
    let reread = ComponentList::read_table(&rewritten).expect("casa-rs reads CASA's rewrite");
    assert_same_list(
        list,
        &reread,
        &format!("{name}: CASA's rewrite read back by casa-rs"),
    );
    println!(
        "{name}: {} components; CASA read every field and cell, and casa-rs read CASA's rewrite back unchanged",
        list.len()
    );
}

#[test]
#[ignore = "runs CASA's Python; use --ignored"]
fn casa_reads_rewrites_and_casa_rs_reads_back() {
    let Some(python) = casa_python() else {
        eprintln!("skipped: no CASA Python (set CASA_RS_CASA_PYTHON)");
        return;
    };
    let scratch = tempfile::tempdir().expect("scratch");
    let fixture = copy_table(&fixture_dir().join("components.cl"), scratch.path());
    let from_casa = ComponentList::read_table(&fixture).expect("read fixture");
    check_round_trip(&python, &from_casa, scratch.path(), "fixture");
    check_round_trip(&python, &constructed_list(), scratch.path(), "constructed");
}

#[test]
fn constructed_lists_round_trip_through_casa_rs() {
    let scratch = tempfile::tempdir().expect("scratch");
    let list = constructed_list();
    let path = scratch.path().join("constructed.cl");
    list.write_table(&path).expect("write");
    let back = ComponentList::read_table(&path).expect("read");
    assert_same_list(&list, &back, "constructed list read back");
    // The Gaussian's position angle was reduced into [0, π) on construction.
    let pa = list.components()[1].shape().parameters()[2];
    assert!((pa - (std::f64::consts::PI - 0.3)).abs() < 1e-15, "{pa}");
    assert_eq!(list.components()[2].limb_darkening_exponent(), Some(1.3));
}

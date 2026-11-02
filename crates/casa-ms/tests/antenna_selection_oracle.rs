// SPDX-License-Identifier: LGPL-3.0-or-later

//! Antenna selection against the pinned CASA 6.7.6.14 `MSSelection` oracle:
//! the MAIN rows whose (`ANTENNA1`, `ANTENNA2`) the resolved selection
//! holds are the rows CASA returned, in physical order.

use std::{fs::File, path::Path};

use casa_ms::MeasurementSet;
use casa_ms::columns::main_ids::{antenna1, antenna2};
use flate2::read::GzDecoder;
use serde_json::Value;

fn fixture(root: &Path, temp: &Path) -> MeasurementSet {
    let archive = File::open(root.join("tests/fixtures/mssel_test_small.ms.tgz")).unwrap();
    tar::Archive::new(GzDecoder::new(archive))
        .unpack(temp)
        .unwrap();
    MeasurementSet::open(temp.join("mssel_test_small.ms")).unwrap()
}

fn selected_rows(ms: &MeasurementSet, text: &str) -> Vec<usize> {
    let selection = ms
        .resolve_antenna_selector(text)
        .unwrap_or_else(|error| panic!("{text}: {error}"));
    let main = ms.main_table();
    let (first, second) = (antenna1(main), antenna2(main));
    (0..main.row_count())
        .filter(|&row| {
            let a1 = usize::try_from(first.get(row).unwrap()).unwrap();
            let a2 = usize::try_from(second.get(row).unwrap()).unwrap();
            selection.holds(a1, a2)
        })
        .collect()
}

#[test]
fn antenna_selections_select_casas_physical_rows_in_order() {
    let root = Path::new(env!("CARGO_MANIFEST_DIR"));
    let oracle: Value = serde_json::from_str(include_str!(
        "../../../resources/casa-compat/evidence/tclean-selection-casa-20261031.json"
    ))
    .unwrap();
    assert_eq!(oracle["casa_tasks"], "6.7.6.14");
    assert!(oracle["field_metadata"].as_array().unwrap().is_empty());
    let temp = tempfile::tempdir().unwrap();
    let ms = fixture(root, temp.path());
    let mut compared = 0;
    for (name, case) in oracle["cases"].as_object().unwrap() {
        let selectors = case["selectors"].as_object().unwrap();
        let Some(text) = selectors.get("antenna").and_then(Value::as_str) else {
            continue;
        };
        if selectors.len() > 1 {
            continue;
        }
        let expected = case["rows"]
            .as_array()
            .unwrap()
            .iter()
            .map(|row| row.as_u64().unwrap() as usize)
            .collect::<Vec<_>>();
        assert_eq!(selected_rows(&ms, text), expected, "{name}: {text:?}");
        compared += 1;
    }
    assert_eq!(compared, 3, "antenna ID, antenna name and baseline cases");
}

/// The fixture's ANTENNA table as the forms beyond the oracle's three see
/// it; these pin the port's reading of the fixture, not CASA runs.
#[test]
fn the_fixture_resolves_every_expression_form() {
    let root = Path::new(env!("CARGO_MANIFEST_DIR"));
    let temp = tempfile::tempdir().unwrap();
    let ms = fixture(root, temp.path());
    let antenna = ms.antenna().unwrap();
    let count = antenna.row_count();
    let first = antenna.name(0).unwrap();
    let station = antenna.station(0).unwrap();
    let by_name = selected_rows(&ms, &first);
    assert!(!by_name.is_empty());
    assert_eq!(selected_rows(&ms, "0"), by_name);
    assert_eq!(selected_rows(&ms, &format!("{first}@{station}")), by_name);
    assert_eq!(selected_rows(&ms, &format!("/{first}/")), by_name);
    assert_eq!(
        selected_rows(&ms, &format!("0~{}&", count - 1)),
        selected_rows(&ms, "*&")
    );
    let selection = ms.resolve_antenna_selector("").unwrap();
    assert!(selection.is_unconstrained());
    assert_eq!(selection.antenna_count(), count);
    let all = selected_rows(&ms, "");
    assert_eq!(all.len(), ms.main_table().row_count());
    let cross: Vec<usize> = selected_rows(&ms, "*");
    let autos: Vec<usize> = selected_rows(&ms, "*&&&");
    let mut union = [cross.clone(), autos.clone()].concat();
    union.sort_unstable();
    assert_eq!(union, all, "every row is a cross or an autocorrelation");
    assert!(cross.iter().all(|row| !autos.contains(row)));
    assert!(ms.resolve_antenna_selector("NOSUCHANTENNA").is_err());
}

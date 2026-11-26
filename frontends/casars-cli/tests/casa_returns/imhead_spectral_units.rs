// SPDX-License-Identifier: LGPL-3.0-or-later
//! CASA comparisons for image spectral-unit edits.

use super::imhead_mutations::assert_header_value_near;
use super::*;

#[test]
fn imhead_edits_spectral_units_as_casa_does() {
    let fixture = Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/data/imhead_crpix");
    let oracle: Value = serde_json::from_slice(
        &std::fs::read(fixture.join("spectral_unit_expected.json")).unwrap(),
    )
    .unwrap();
    assert_eq!(oracle["casa_version"], "6.7.6-14");
    let workspace = tempfile::tempdir().unwrap();
    for (index, case) in oracle["cases"].as_array().unwrap().iter().enumerate() {
        let path = workspace.path().join(format!("spectral-{index}.im"));
        copy_tree(
            &fixture.join(format!("{}.im", case["image"].as_str().unwrap())),
            &path,
        );
        let key = format!("cunit{}", case["axis"].as_u64().unwrap());
        let value = returned(
            workspace.path(),
            "imhead",
            json!({"imagename": path, "mode": "put", "hdkey": key, "hdvalue": case["unit"]}),
        );
        assert_eq!(value, CasaValue::None, "case {index}");
        let summary = returned(
            workspace.path(),
            "imhead",
            json!({"imagename": path, "mode": "summary"}),
        )
        .to_plain_json();
        for key in ["axisunits", "refval", "incr", "refpix"] {
            assert_header_value_near(&summary[key], &case[key], index);
        }
        let rest = returned(
            workspace.path(),
            "imhead",
            json!({"imagename": path, "mode": "get", "hdkey": "restfreq"}),
        );
        assert_header_value_near(&rest.to_plain_json(), &case["restfreq"], index);

        let image = casa_images::PagedImage::<f32>::open(&path).unwrap();
        let coordinates = image.coordinates();
        let pixel = case["pixel"]
            .as_array()
            .unwrap()
            .iter()
            .map(|v| v.as_f64().unwrap())
            .collect::<Vec<_>>();
        let world = coordinates.to_world(&pixel).unwrap();
        // Direction world values in the Rust coordinate API are radians;
        // CASA's coordinates tool reports them in the displayed axis unit.
        let axis = case["axis"].as_u64().unwrap() as usize - 1;
        let actual = world[axis];
        let expected = case["world"][axis].as_f64().unwrap();
        assert!(
            (actual - expected).abs() <= expected.abs().max(1.0) * 1e-7,
            "case {index}: {actual} != {expected}"
        );
        let restored = coordinates.to_pixel(&world).unwrap();
        for (actual, expected) in restored.iter().zip(&pixel) {
            assert!(
                (actual - expected).abs() <= 1e-5,
                "case {index}: {actual} != {expected}"
            );
        }
        drop(image);

        for (field, velocity) in [("fits", false), ("velocity_fits", true)] {
            let expected_header = &case[field];
            if expected_header.is_null() || expected_header.get("error").is_some() {
                continue;
            }
            let fits = workspace
                .path()
                .join(format!("spectral-{index}-{field}.fits"));
            casa_images::analysis::export_fits(&path, &fits, velocity, false).unwrap();
            let bytes = std::fs::read(fits).unwrap();
            let cards = bytes
                .as_chunks::<80>()
                .0
                .iter()
                .map(|card| std::str::from_utf8(card).unwrap())
                .collect::<Vec<_>>();
            let header = casa_coordinates::fits::FitsHeader::from_cards(&cards);
            let axis = (1..=4)
                .find(|axis| {
                    header.get_string(&format!("CTYPE{axis}")) == expected_header["CTYPE"].as_str()
                })
                .unwrap();
            assert_eq!(
                header.get_string(&format!("CUNIT{axis}")),
                expected_header["CUNIT"].as_str()
            );
            assert_eq!(
                header.get_string("SPECSYS"),
                expected_header["SPECSYS"].as_str()
            );
            for key in ["CRVAL", "CDELT", "RESTFRQ"] {
                let name = if key == "RESTFRQ" {
                    key.to_owned()
                } else {
                    format!("{key}{axis}")
                };
                let actual = header.get_float(&name).unwrap();
                let expected = expected_header[key].as_f64().unwrap();
                assert!(
                    (actual - expected).abs() <= expected.abs().max(1.0) * 1e-7,
                    "case {index}: {name}: {actual} != {expected}"
                );
            }
        }
    }
}

#[test]
fn imhead_accepts_casacore_unit_spellings() {
    let fixture = Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/data/imhead_crpix");
    let oracle: Value = serde_json::from_slice(
        &std::fs::read(fixture.join("unit_spellings_expected.json")).unwrap(),
    )
    .unwrap();
    let workspace = tempfile::tempdir().unwrap();
    for (index, case) in oracle["cases"].as_array().unwrap().iter().enumerate() {
        let path = workspace.path().join(format!("spelling-{index}.im"));
        copy_tree(&fixture.join("normal.im"), &path);
        let key = case["key"].as_str().unwrap();
        let result = returned(
            workspace.path(),
            "imhead",
            json!({"imagename": path, "mode": "put", "hdkey": key, "hdvalue": case["unit"]}),
        );
        assert_eq!(result, CasaValue::None);
        let got = returned(
            workspace.path(),
            "imhead",
            json!({"imagename": path, "mode": "get", "hdkey": key}),
        );
        assert_eq!(got.to_plain_json(), case["value"]);
        let summary = returned(
            workspace.path(),
            "imhead",
            json!({"imagename": path, "mode": "summary"}),
        )
        .to_plain_json();
        for key in ["axisunits", "refval", "incr"] {
            assert_header_value_near(&summary[key], &case[key], index);
        }
        let image = casa_images::PagedImage::<f32>::open(&path).unwrap();
        let pixel = [1.0, 1.0, 0.0, 1.0];
        let world = image.coordinates().to_world(&pixel).unwrap();
        let restored = image.coordinates().to_pixel(&world).unwrap();
        for (actual, expected) in restored.iter().zip(pixel) {
            assert!(
                (actual - expected).abs() < 1e-5,
                "case {index}: {actual} != {expected}"
            );
        }
    }
}

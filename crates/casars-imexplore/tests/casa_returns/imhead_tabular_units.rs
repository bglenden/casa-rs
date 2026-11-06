// SPDX-License-Identifier: LGPL-3.0-or-later
//! CASA comparisons for tabular spectral coordinates and unit edits.

use super::imhead_mutations::assert_header_value_near;
use super::*;

fn check_image(path: &Path, case: &Value, index: usize) {
    let workspace = path.parent().unwrap();
    let summary = returned(
        workspace,
        "imhead",
        json!({"imagename": path, "mode": "summary"}),
    )
    .to_plain_json();
    for key in ["axisunits", "refval", "incr", "refpix"] {
        assert_header_value_near(&summary[key], &case[key], index);
    }
    let rest = returned(
        workspace,
        "imhead",
        json!({"imagename": path, "mode": "get", "hdkey": "restfreq"}),
    );
    assert_header_value_near(&rest.to_plain_json(), &case["restfreq"], index);
    let image = casa_images::PagedImage::<f32>::open(path).unwrap();
    for (pixel, expected) in case["pixels"]
        .as_array()
        .unwrap()
        .iter()
        .zip(case["worlds"].as_array().unwrap())
    {
        let pixel = pixel
            .as_array()
            .unwrap()
            .iter()
            .map(|v| v.as_f64().unwrap())
            .collect::<Vec<_>>();
        let world = image.coordinates().to_world(&pixel).unwrap();
        let spectral = expected[3].as_f64().unwrap();
        assert!(
            (world[3] - spectral).abs() <= spectral.abs().max(1.0) * 1e-7,
            "case {index}: {} != {spectral}",
            world[3]
        );
        let restored = image.coordinates().to_pixel(&world).unwrap();
        assert!((restored[3] - pixel[3]).abs() < 1e-5);
    }
    drop(image);
    for (field, velocity) in [("fits", false), ("velocity_fits", true)] {
        let fits = workspace.join(format!("tabular-{index}-{field}.fits"));
        casa_images::export_fits(path, &fits, velocity, false).unwrap();
        let bytes = std::fs::read(&fits).unwrap();
        let cards = bytes
            .as_chunks::<80>()
            .0
            .iter()
            .take_while(|card| &card[..8] != b"END     ")
            .map(|card| std::str::from_utf8(card).unwrap())
            .collect::<Vec<_>>();
        let header = casa_coordinates::fits::FitsHeader::from_cards(&cards);
        for key in ["CTYPE", "CUNIT"] {
            assert_eq!(
                header.get_string(&format!("{key}3")),
                case[field][key].as_str()
            );
        }
        for key in ["CRVAL", "CDELT", "RESTFRQ"] {
            let name = if key == "RESTFRQ" {
                key.to_string()
            } else {
                format!("{key}3")
            };
            let actual = header.get_float(&name).unwrap();
            let expected = case[field][key].as_f64().unwrap();
            assert!(
                (actual - expected).abs() <= expected.abs().max(1.0) * 1e-7,
                "case {index}: {name}: {actual} != {expected}"
            );
        }
    }
}

#[test]
fn tabular_spectral_unit_edits_and_casa_images_match() {
    let fixture = Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/data/imhead_crpix");
    let oracle: Value =
        serde_json::from_slice(&std::fs::read(fixture.join("tabular_unit_expected.json")).unwrap())
            .unwrap();
    let cases = oracle["cases"].as_array().unwrap();
    let workspace = tempfile::tempdir().unwrap();
    let casa = workspace.path().join("casa.im");
    copy_tree(&fixture.join("tabular_ghz.im"), &casa);
    check_image(&casa, &cases[0], 10);
    let path = workspace.path().join("ours.im");
    copy_tree(&fixture.join("verbose_tabular.im"), &path);
    for (index, case) in cases.iter().enumerate() {
        let value = returned(
            workspace.path(),
            "imhead",
            json!({"imagename": path, "mode": "put", "hdkey": "cunit4", "hdvalue": case["unit"]}),
        );
        assert_eq!(value, CasaValue::None);
        check_image(&path, case, index);
    }
}

#[test]
fn tabular_spectral_coordinate_edits_match_casa() {
    let fixture = Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/data/imhead_crpix");
    let oracle: Value =
        serde_json::from_slice(&std::fs::read(fixture.join("tabular_unit_expected.json")).unwrap())
            .unwrap();
    let workspace = tempfile::tempdir().unwrap();
    for (index, case) in oracle["edits"].as_array().unwrap().iter().enumerate() {
        let path = workspace.path().join(format!("tabular-edit-{index}.im"));
        copy_tree(&fixture.join("verbose_tabular.im"), &path);
        if case["unit"] != "Hz" {
            assert_eq!(
                returned(
                    workspace.path(),
                    "imhead",
                    json!({"imagename": path, "mode": "put", "hdkey": "cunit4", "hdvalue": case["unit"]})
                ),
                CasaValue::None
            );
        }
        assert_eq!(
            returned(
                workspace.path(),
                "imhead",
                json!({"imagename": path, "mode": "put", "hdkey": case["key"], "hdvalue": case["value"]})
            ),
            CasaValue::None
        );
        check_image(&path, case, index);
    }
}

#[test]
fn tabular_spectral_fits_export_uses_casas_conversion_frame() {
    let fixture = Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/data/imhead_crpix");
    let oracle: Value =
        serde_json::from_slice(&std::fs::read(fixture.join("tabular_unit_expected.json")).unwrap())
            .unwrap();
    let workspace = tempfile::tempdir().unwrap();
    let path = workspace.path().join("conversion.im");
    copy_tree(&fixture.join("tabular_conversion.im"), &path);
    for (index, case) in oracle["conversion_cases"]
        .as_array()
        .unwrap()
        .iter()
        .enumerate()
    {
        assert_eq!(
            returned(
                workspace.path(),
                "imhead",
                json!({"imagename": path, "mode": "put", "hdkey": "cunit4", "hdvalue": case["unit"]})
            ),
            CasaValue::None
        );
        check_image(&path, case, index + 20);
    }
}

#[test]
fn tabular_spectral_restore_matches_casa_for_nonunit_pixel_samples() {
    let fixture = Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/data/imhead_crpix");
    let oracle: Value =
        serde_json::from_slice(&std::fs::read(fixture.join("tabular_unit_expected.json")).unwrap())
            .unwrap();
    let workspace = tempfile::tempdir().unwrap();
    for (index, case) in oracle["nonunit_cases"]
        .as_array()
        .unwrap()
        .iter()
        .enumerate()
    {
        let path = workspace.path().join(format!("nonunit-{index}.im"));
        copy_tree(&fixture.join(case["image"].as_str().unwrap()), &path);
        check_image(&path, case, 30 + index);
    }
}

#[test]
fn generic_tabular_coordinate_edits_match_casa() {
    let fixture = Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/data/imhead_crpix");
    let oracle: Value = serde_json::from_slice(
        &std::fs::read(fixture.join("generic_tabular_expected.json")).unwrap(),
    )
    .unwrap();
    let workspace = tempfile::tempdir().unwrap();
    for (source, cases) in [
        ("generic_tabular.im", "cases"),
        ("generic_linear_tabular.im", "linear_cases"),
    ] {
        for (index, case) in oracle[cases].as_array().unwrap().iter().enumerate() {
            let path = workspace.path().join(format!("generic-{cases}-{index}.im"));
            copy_tree(&fixture.join(source), &path);
            if case["unit"] != "m" {
                assert_eq!(
                    returned(
                        workspace.path(),
                        "imhead",
                        json!({"imagename": path, "mode": "put", "hdkey": "cunit1", "hdvalue": case["unit"]})
                    ),
                    CasaValue::None
                );
            }
            assert_eq!(
                returned(
                    workspace.path(),
                    "imhead",
                    json!({"imagename": path, "mode": "put", "hdkey": case["key"], "hdvalue": case["value"]})
                ),
                CasaValue::None
            );
            let summary = returned(
                workspace.path(),
                "imhead",
                json!({"imagename": path, "mode": "summary"}),
            )
            .to_plain_json();
            for key in ["axisunits", "refval", "incr", "refpix"] {
                assert_header_value_near(&summary[key], &case[key], index);
            }
            let image = casa_images::PagedImage::<f32>::open(&path).unwrap();
            for (pixel, expected) in case["pixels"]
                .as_array()
                .unwrap()
                .iter()
                .zip(case["worlds"].as_array().unwrap())
            {
                let pixel = pixel.as_f64().unwrap();
                let expected = expected.as_f64().unwrap();
                let world = image.coordinates().to_world(&[pixel]).unwrap();
                assert!(
                    (world[0] - expected).abs() <= expected.abs().max(1.0) * 1e-12,
                    "case {index}"
                );
                assert!((image.coordinates().to_pixel(&world).unwrap()[0] - pixel).abs() < 1e-5);
            }
            drop(image);
            let fits = workspace
                .path()
                .join(format!("generic-{cases}-{index}.fits"));
            casa_images::export_fits(&path, &fits, false, false).unwrap();
            let bytes = std::fs::read(fits).unwrap();
            let cards = bytes
                .as_chunks::<80>()
                .0
                .iter()
                .take_while(|card| &card[..8] != b"END     ")
                .map(|card| std::str::from_utf8(card).unwrap())
                .collect::<Vec<_>>();
            let header = casa_coordinates::fits::FitsHeader::from_cards(&cards);
            for key in ["CTYPE1", "CUNIT1"] {
                assert_eq!(
                    header.get_string(key),
                    case["fits"][key].as_str(),
                    "{cases} {index}"
                );
            }
            for key in ["CRVAL1", "CDELT1", "CRPIX1"] {
                let actual = header.get_float(key).unwrap();
                let expected = case["fits"][key].as_f64().unwrap();
                assert!(
                    (actual - expected).abs() <= expected.abs().max(1.0) * 1e-10,
                    "{cases} {index} {key}: {actual} != {expected}"
                );
            }
        }
    }
}

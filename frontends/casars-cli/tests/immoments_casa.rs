// SPDX-License-Identifier: LGPL-3.0-or-later
//! Pinned CASA 6.7.6.14 runway through the normal immoments executable.

use std::ffi::CStr;
use std::fs;
use std::io::Write;
use std::path::Path;
use std::process::{Command, Stdio};

use casa_imageanalysis::{ImmomentsRequest, immoments};
use casa_images::PagedImage;
use fitsio::{
    FitsFile,
    images::{ImageDescription, ImageType},
    tables::{ColumnDataType, ColumnDescription},
};
use serde_json::{Value, json};

fn copy_inputs(source: &Path, destination: &Path) {
    fs::create_dir_all(destination).unwrap();
    for entry in source.read_dir().unwrap() {
        let entry = entry.unwrap();
        let name = entry.file_name();
        if name == "expected.json" || name == "table.lock" {
            continue;
        }
        let path = destination.join(name);
        if entry.file_type().unwrap().is_dir() {
            copy_inputs(&entry.path(), &path);
        } else {
            fs::copy(entry.path(), path).unwrap();
        }
    }
}

#[test]
fn fits_image_extension_keeps_its_original_history_and_beam() {
    let workspace = tempfile::tempdir().unwrap();
    for insert_binary_table in [false, true] {
        let stem = if insert_binary_table {
            "later-image"
        } else {
            "first-extension"
        };
        let imagename = workspace.path().join(format!("{stem}.fits"));
        let outfile = workspace.path().join(format!("{stem}.im"));
        let mut fits = FitsFile::create(&imagename).open().unwrap();
        append_fits_history(&mut fits, c"PRIMARY_HISTORY");
        if insert_binary_table {
            let column = ColumnDescription::new("METADATA")
                .with_type(ColumnDataType::Int)
                .create()
                .unwrap();
            fits.create_table("METADATA", &[column]).unwrap();
        }
        let hdu = fits
            .create_image(
                "IMAGE",
                &ImageDescription {
                    data_type: ImageType::Float,
                    dimensions: &[4, 2, 2],
                },
            )
            .unwrap();
        for (key, value) in [("CTYPE3", "FREQ"), ("CUNIT3", "Hz"), ("BUNIT", "Jy/beam")] {
            hdu.write_key(&mut fits, key, value).unwrap();
        }
        for (key, value) in [
            ("CRVAL3", 1.415e9),
            ("CDELT3", 1.0e6),
            ("CRPIX3", 1.0),
            ("RESTFRQ", 1.42e9),
        ] {
            hdu.write_key(&mut fits, key, value).unwrap();
        }
        hdu.write_image(&mut fits, &[1.0_f32; 16]).unwrap();
        append_fits_history(&mut fits, c"IMAGE_HISTORY");
        append_fits_history(&mut fits, c"AIPS CLEAN BMAJ= 0.002 BMIN= 0.001 BPA= 12");
        drop(fits);
        let result = immoments(&ImmomentsRequest {
            imagename,
            outfile: outfile.clone(),
            moments: vec![0],
            ..ImmomentsRequest::default()
        });
        if insert_binary_table {
            assert!(
                result
                    .unwrap_err()
                    .to_string()
                    .contains("zeroth or first extension")
            );
            assert!(!outfile.exists());
        } else {
            result.unwrap();
            let output = PagedImage::<f32>::open(outfile).unwrap();
            let history = output.history().unwrap();
            assert!(history.iter().any(|line| line == "IMAGE_HISTORY"));
            assert!(!history.iter().any(|line| line == "PRIMARY_HISTORY"));
            let beam = output.image_info().unwrap().beam_set.single_beam().unwrap();
            assert!((beam.major_in("arcsec").unwrap() - 7.2).abs() < 1e-10);
            assert!((beam.minor_in("arcsec").unwrap() - 3.6).abs() < 1e-10);
            assert!((beam.position_angle_in("deg").unwrap() - 12.).abs() < 1e-10);
        }
    }
}

fn append_fits_history(fits: &mut FitsFile, message: &CStr) {
    let mut status = 0;
    // SAFETY: fits is live and the message is NUL terminated.
    unsafe { fitsio::sys::ffphis(fits.as_raw(), message.as_ptr(), &mut status) };
    fitsio::errors::check_status(status).unwrap();
}

#[test]
fn moments_match_pinned_casa_images() {
    let oracle = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../../libs/casacore/casa-images/tests/data/immoments");
    let expected: Value = serde_json::from_str(include_str!(
        "../../../libs/casacore/casa-images/tests/data/immoments/expected.json"
    ))
    .unwrap();
    assert_eq!(expected["casa_version"], "6.7.6.14");
    let workspace = tempfile::tempdir().unwrap();
    copy_inputs(&oracle, workspace.path());
    for (name, case) in expected["cases"].as_object().unwrap() {
        let stem = format!("{name}.out");
        let mut input = case["params"].clone();
        input["imagename"] = case["imagename"].clone();
        input["outfile"] = json!(stem);
        let request = json!({"surface": "immoments", "values": input});
        let mut command = Command::new(env!("CARGO_BIN_EXE_immoments"))
            .args(["--json-run", "-"])
            .current_dir(workspace.path())
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .unwrap();
        command
            .stdin
            .take()
            .unwrap()
            .write_all(request.to_string().as_bytes())
            .unwrap();
        let output = command.wait_with_output().unwrap();
        if case.get("error").is_some() {
            assert!(!output.status.success(), "{name}");
            assert!(!workspace.path().join(&stem).exists(), "{name}");
            continue;
        }
        let reply = String::from_utf8_lossy(&output.stdout);
        assert!(output.status.success(), "{name}: {reply}");
        let reply: Value = serde_json::from_str(&reply).unwrap();
        let products = case["products"].as_object().unwrap();
        let reported = reply["result"]["result"]["products"].as_array().unwrap();
        assert_eq!(reported.len(), products.len(), "{name}");
        for (suffix, image) in products {
            let path = workspace.path().join(format!("{stem}{suffix}"));
            assert!(
                reported
                    .iter()
                    .any(|item| item["outfile"] == json!(path.canonicalize().unwrap())),
                "{name}{suffix}"
            );
            validate_product(&path, image);
        }
    }
}

#[cfg(unix)]
#[test]
fn publication_keeps_source_tables_and_existing_entries_safe() {
    use std::os::unix::fs::symlink;

    use casa_imageanalysis::{ImmomentsRequest, immoments};

    let oracle = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../../libs/casacore/casa-images/tests/data/immoments/cube.im");
    let workspace = tempfile::tempdir().unwrap();
    let source = workspace.path().join("source.im");
    copy_inputs(&oracle, &source);
    let alias = workspace.path().join("alias.im");
    symlink(&source, &alias).unwrap();
    let history = source.join("logtable/table.dat");
    let original = fs::read(&history).unwrap();
    let mut request = ImmomentsRequest {
        imagename: source.clone(),
        moments: vec![0],
        overwrite: true,
        ..Default::default()
    };
    for unsafe_path in [
        source.join("logtable"),
        source.join("new.im"),
        alias.join("logtable"),
    ] {
        request.outfile = unsafe_path;
        let message = immoments(&request).unwrap_err().to_string();
        assert!(
            message.contains("separate from the input image"),
            "{message}"
        );
    }
    assert!(!source.join("new.im").exists());
    assert_eq!(fs::read(history).unwrap(), original);
    PagedImage::<f32>::open(&source).unwrap();

    request.moments = vec![0, 8];
    request.overwrite = false;
    request.outfile = workspace.path().join("products");
    let dangling = workspace.path().join("products.maximum");
    symlink("missing", &dangling).unwrap();
    let message = immoments(&request).unwrap_err().to_string();
    assert!(message.contains("already exists"), "{message}");
    assert!(!workspace.path().join("products.integrated").exists());
    assert_eq!(fs::read_link(dangling).unwrap(), Path::new("missing"));
}

fn validate_product(path: &Path, oracle: &Value) {
    let image = PagedImage::<f32>::open(path).unwrap();
    let data = image
        .get_slice(&vec![0; image.shape().len()], image.shape())
        .unwrap();
    let valid = image.get_mask().unwrap().unwrap();
    assert_eq!(json!(image.shape()), oracle["shape"], "{}", path.display());
    assert_eq!(
        json!(image.axis_names()),
        oracle["names"],
        "{}",
        path.display()
    );
    assert_eq!(json!(image.units()), oracle["units"], "{}", path.display());
    assert_eq!(image.default_mask_name().as_deref(), Some("mask0"));
    let values = oracle["pixels"].as_array().unwrap();
    let validity = oracle["mask"].as_array().unwrap();
    assert_eq!(values.len(), data.len());
    for (mut offset, (pixel, mask)) in values.iter().zip(validity).enumerate() {
        let index = image
            .shape()
            .iter()
            .map(|&n| {
                let position = offset % n;
                offset /= n;
                position
            })
            .collect::<Vec<_>>();
        assert_eq!(
            valid[&index[..]],
            mask.as_bool().unwrap(),
            "{} at {index:?}",
            path.display()
        );
        if valid[&index[..]] {
            let expected = pixel.as_f64().unwrap();
            let got = f64::from(data[&index[..]]);
            assert!(
                (expected - got).abs() <= 1e-5 * expected.abs().max(1.0),
                "{} at {index:?}: CASA {expected}, native {got}",
                path.display()
            );
        }
    }
    let history = image.history().unwrap();
    assert!(
        history
            .iter()
            .any(|line| line == "CASA immoments oracle input")
    );
    assert!(history.iter().any(|line| line.starts_with("immoments(")));
}

// SPDX-License-Identifier: LGPL-3.0-or-later
//! CASA 6.7.6.14 parity suite for `immath` against recorded oracle fixtures.

use std::path::{Path, PathBuf};
use std::process::Command;

use casa_coordinates::{
    CoordinateModel, CoordinateSystem, DirectionCoordinate, Projection, ProjectionType,
};
use casa_images::expr_parser::{HashMapResolver, parse_image_expr};
use casa_images::{
    AnyPagedImage, GaussianBeam, ImageBeamSet, ImageInfo, ImageInterface, ImagePixelType,
    ImmathRequest, PagedImage, export_fits, immath,
};
use casa_lattices::ExecutionPolicy;
use casa_types::measures::direction::DirectionRef;
use casa_types::{ArrayD, ArrayValue, RecordValue, ScalarValue, Value};
use ndarray::IxDyn;
use serde_json::Value as JsonValue;
use tempfile::TempDir;

const INPUT_NAMES: &[&str] = &[
    "cube_iquv.im",
    "spix_lo.im",
    "spix_hi.im",
    "map2d.im",
    "deg4d.im",
    "q_plane.im",
    "u_plane.im",
    "v_plane.im",
    "only_i.im",
    "only_q.im",
    "input.fits",
];

fn fixture_dir() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("../../crates/casa-images/tests/data/immath")
}

fn load_oracle() -> JsonValue {
    let raw = std::fs::read_to_string(fixture_dir().join("expected.json"))
        .expect("read immath/expected.json");
    serde_json::from_str(&raw).expect("parse immath/expected.json")
}

fn copy_dir_recursive(src: &Path, dst: &Path) {
    std::fs::create_dir_all(dst).expect("create target fixture dir");
    for entry in std::fs::read_dir(src).expect("read fixture dir") {
        let entry = entry.expect("dir entry");
        let name = entry.file_name();
        if name == "table.lock" {
            continue;
        }
        let target = dst.join(&name);
        if entry.file_type().expect("file type").is_dir() {
            copy_dir_recursive(&entry.path(), &target);
        } else {
            std::fs::copy(entry.path(), &target).expect("copy fixture file");
        }
    }
}

fn stage_inputs() -> TempDir {
    let tmp = TempDir::new().expect("create tempdir");
    let src_root = fixture_dir();
    for name in INPUT_NAMES {
        let src = src_root.join(name);
        let dst = tmp.path().join(name);
        if src.is_dir() {
            copy_dir_recursive(&src, &dst);
        } else {
            std::fs::copy(&src, &dst).expect("copy input file");
        }
    }
    tmp
}

#[test]
fn external_mask_is_protected_and_the_invocation_retains_selection_parameters() {
    let workspace = stage_inputs();
    let image = workspace.path().join("map2d.im");
    let mask = workspace.path().join("external-mask.im");
    copy_dir_recursive(&image, &mask);
    let before = PagedImage::<f32>::open(&mask).unwrap().get().unwrap();
    let mut request = ImmathRequest {
        imagename: vec![image],
        outfile: mask.clone(),
        mask: Some(format!("\"{}\" > 0", mask.display())),
        box_pixels: Some("0,0,1,1".into()),
        overwrite: true,
        ..ImmathRequest::default()
    };
    assert!(
        immath(&request)
            .unwrap_err()
            .to_string()
            .contains("matches input image")
    );
    assert_eq!(
        PagedImage::<f32>::open(&mask).unwrap().get().unwrap(),
        before
    );
    request.outfile = workspace.path().join("result.im");
    immath(&request).unwrap();
    let history = PagedImage::<f32>::open(&request.outfile)
        .unwrap()
        .history()
        .unwrap();
    assert!(history[history.len() - 2].contains("version"));
    assert!(history.last().unwrap().contains("external-mask.im"));
    assert!(history.last().unwrap().contains("0,0,1,1"));
}

#[test]
fn a_strided_saved_region_drives_pixel_sampling_and_world_coordinates() {
    let workspace = tempfile::tempdir().unwrap();
    let data = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../../crates/casa-images/tests/data/imsubimage");
    let image = workspace.path().join("cube.im");
    copy_dir_recursive(&data.join("cube.im"), &image);
    let outfile = workspace.path().join("result.im");
    immath(&ImmathRequest {
        imagename: vec![image.clone()],
        outfile: outfile.clone(),
        region: Some(data.join("stride.rgn").display().to_string()),
        ..ImmathRequest::default()
    })
    .unwrap();
    let input = PagedImage::<f32>::open(image).unwrap();
    let result = PagedImage::<f32>::open(outfile).unwrap();
    assert_eq!(result.shape(), [3, 3, 4, 2]);
    assert_eq!(
        result.get_at(&[2, 2, 3, 1]).unwrap(),
        input.get_at(&[4, 5, 3, 2]).unwrap()
    );
    let native = result.coordinates().to_world(&[2., 2., 3., 1.]).unwrap();
    let expected = input.coordinates().to_world(&[4., 5., 3., 2.]).unwrap();
    for (value, expected) in native.into_iter().zip(expected) {
        assert!((value - expected).abs() <= 1e-12 * expected.abs().max(1.));
    }
}

#[test]
fn beam_diagnostics_distinguish_close_values_and_an_absent_beam() {
    let workspace = stage_inputs();
    let first = workspace.path().join("map2d.im");
    let second = workspace.path().join("other.im");
    copy_dir_recursive(&first, &second);
    for (image, major) in [
        (&first, 1.234_567_890_123_45_f64),
        (&second, 1.234_567_890_123_451_f64),
    ] {
        let mut image = PagedImage::<f32>::open(image).unwrap();
        let beam = GaussianBeam::new((major / 60.).to_radians(), 0.01_f64.to_radians(), 0.);
        image
            .set_image_info(&ImageInfo {
                beam_set: ImageBeamSet::new(beam),
                ..ImageInfo::default()
            })
            .unwrap();
        image.save().unwrap();
    }
    let mut request = ImmathRequest {
        imagename: vec![first, second.clone()],
        expr: "IM0 + IM1".into(),
        outfile: workspace.path().join("first.im"),
        ..ImmathRequest::default()
    };
    let result = immath(&request).unwrap();
    assert_eq!(result.beam_warnings.len(), 3);
    assert_ne!(
        result.beam_warnings[1].split_once("major="),
        result.beam_warnings[2].split_once("major=")
    );
    let mut unbeamed = PagedImage::<f32>::open(&second).unwrap();
    unbeamed.set_image_info(&ImageInfo::default()).unwrap();
    unbeamed.save().unwrap();
    request.outfile = workspace.path().join("second.im");
    assert_eq!(immath(&request).unwrap().beam_warnings.len(), 1);
}

fn rewrite_paths(text: &str, root: &Path) -> String {
    let mut out = text.to_string();
    for name in INPUT_NAMES {
        let abs = root.join(name).display().to_string();
        out = out.replace(name, &abs);
    }
    out
}

fn build_request(params: &JsonValue, root: &Path, outfile: PathBuf) -> ImmathRequest {
    let mut req = ImmathRequest {
        outfile,
        ..ImmathRequest::default()
    };
    if let Some(im) = params.get("imagename") {
        match im {
            JsonValue::String(s) if s.trim().is_empty() => {}
            JsonValue::String(s) => req.imagename.push(PathBuf::from(rewrite_paths(s, root))),
            JsonValue::Array(items) => {
                for item in items {
                    let s = item.as_str().expect("imagename string");
                    if !s.trim().is_empty() {
                        req.imagename.push(PathBuf::from(rewrite_paths(s, root)));
                    }
                }
            }
            _ => panic!("unexpected imagename JSON value: {im}"),
        }
    }
    if let Some(v) = params.get("mode").and_then(|v| v.as_str()) {
        req.mode = v.to_string();
    }
    if let Some(v) = params.get("expr").and_then(|v| v.as_str()) {
        req.expr = rewrite_paths(v, root);
    }
    if let Some(items) = params.get("varnames").and_then(|v| v.as_array()) {
        req.varnames = items
            .iter()
            .map(|x| x.as_str().expect("varname string").to_string())
            .collect();
    }
    if let Some(v) = params.get("sigma").and_then(|v| v.as_str()) {
        req.sigma = v.to_string();
    }
    req.polithresh = params
        .get("polithresh")
        .and_then(|v| v.as_str())
        .map(str::to_string);
    req.mask = params
        .get("mask")
        .and_then(|v| v.as_str())
        .map(|s| rewrite_paths(s, root));
    req.region = params
        .get("region")
        .and_then(|v| v.as_str())
        .map(|s| rewrite_paths(s, root));
    req.box_pixels = params
        .get("box")
        .and_then(|v| v.as_str())
        .map(str::to_string);
    req.chans = params
        .get("chans")
        .and_then(|v| v.as_str())
        .map(str::to_string);
    req.stokes = params
        .get("stokes")
        .and_then(|v| v.as_str())
        .map(str::to_string);
    if let Some(v) = params.get("stretch").and_then(|v| v.as_bool()) {
        req.stretch = v;
    }
    req.imagemd = params
        .get("imagemd")
        .and_then(|v| v.as_str())
        .map(|s| rewrite_paths(s, root));
    if let Some(v) = params.get("prec").and_then(|v| v.as_str()) {
        req.prec = v.to_string();
    }
    req
}

#[derive(Debug)]
enum ExpectedPixel {
    NaN,
    Inf(bool),
    Finite(f64),
}

fn parse_expected_pixel(val: &JsonValue) -> ExpectedPixel {
    match val {
        JsonValue::String(s) if s == "NaN" => ExpectedPixel::NaN,
        JsonValue::String(s) if s == "Inf" => ExpectedPixel::Inf(true),
        JsonValue::String(s) if s == "-Inf" => ExpectedPixel::Inf(false),
        JsonValue::Number(n) => ExpectedPixel::Finite(n.as_f64().expect("finite pixel")),
        other => panic!("unexpected expected pixel value: {other}"),
    }
}

fn record_axis_scalars(rec: &RecordValue, field_name: &str) -> Vec<f64> {
    let from_val = |v: &Value| -> Vec<f64> {
        match v {
            Value::Scalar(ScalarValue::Float64(x)) => vec![*x],
            Value::Scalar(ScalarValue::Float32(x)) => vec![f64::from(*x)],
            Value::Array(ArrayValue::Float64(arr)) => arr.iter().copied().collect(),
            Value::Array(ArrayValue::Float32(arr)) => arr.iter().map(|x| f64::from(*x)).collect(),
            _ => Vec::new(),
        }
    };
    if let Some(v) = rec.get(field_name) {
        let out = from_val(v);
        if !out.is_empty() {
            return out;
        }
    }
    for sub in ["wcs", "tabular"] {
        if let Some(Value::Record(inner)) = rec.get(sub)
            && let Some(v) = inner.get(field_name)
        {
            let out = from_val(v);
            if !out.is_empty() {
                return out;
            }
        }
    }
    Vec::new()
}

fn extract_crpix_cdelt(coords: &CoordinateSystem) -> (Vec<f64>, Vec<f64>) {
    let npix = coords.n_pixel_axes();
    let mut crpix = vec![0.0; npix];
    let mut cdelt = vec![1.0; npix];
    let record = coords.to_record();
    let placed = CoordinateSystem::record_coordinates(&record).expect("record coordinates");
    for coord in placed {
        if coord.field.starts_with("stokes") {
            continue;
        }
        let Some(Value::Record(coord_rec)) = record.get(&coord.field) else {
            continue;
        };
        let cp = record_axis_scalars(coord_rec, "crpix");
        let cd = record_axis_scalars(coord_rec, "cdelt");
        for (local_ax, opt_sys_ax) in coord.pixel_axes.iter().enumerate() {
            let Some(sys_ax) = *opt_sys_ax else {
                continue;
            };
            if let Some(&val) = cp.get(local_ax) {
                crpix[sys_ax] = val;
            }
            if let Some(&val) = cd.get(local_ax) {
                cdelt[sys_ax] = val;
            }
        }
    }
    (crpix, cdelt)
}

fn extract_stokes(coords: &CoordinateSystem) -> Vec<String> {
    for idx in 0..coords.n_coordinates() {
        if let CoordinateModel::Stokes(sc) = coords.coordinate(idx) {
            return sc
                .stokes()
                .iter()
                .map(|s| {
                    let text = s.to_string();
                    match text.as_str() {
                        "PTotal" => "Ptotal".to_string(),
                        "PLinear" => "Plinear".to_string(),
                        "PFTotal" => "PFtotal".to_string(),
                        "PFLinear" => "PFlinear".to_string(),
                        "PAngle" => "Pangle".to_string(),
                        _ => text,
                    }
                })
                .collect();
        }
    }
    Vec::new()
}

fn assert_case_matches(name: &str, outfile: &Path, expected: &JsonValue) {
    let img = AnyPagedImage::open(outfile).unwrap_or_else(|e| {
        panic!(
            "case {name}: failed to open output {}: {e}",
            outfile.display()
        )
    });

    let exp_shape: Vec<usize> = expected["shape"]
        .as_array()
        .expect("shape array")
        .iter()
        .map(|v| v.as_u64().expect("usize") as usize)
        .collect();
    assert_eq!(img.shape(), exp_shape.as_slice(), "case {name}: shape");

    let exp_pt = expected["pixel_type"].as_str().expect("pixel_type");
    let got_pt = match img.pixel_type() {
        ImagePixelType::Float32 => "float",
        ImagePixelType::Float64 => "double",
        ImagePixelType::Complex32 => "complex",
        ImagePixelType::Complex64 => "dcomplex",
    };
    assert_eq!(got_pt, exp_pt, "case {name}: pixel_type");

    let (coords, bunit, hist, pixels_f64, mask_opt) = match &img {
        AnyPagedImage::Float32(im) => {
            let zeros = vec![0; im.shape().len()];
            let ones = vec![1; im.shape().len()];
            let slice = im.get_slice(&zeros, im.shape()).expect("read f32");
            let flat = slice
                .as_standard_layout()
                .as_slice()
                .expect("c order")
                .iter()
                .map(|&v| f64::from(v))
                .collect::<Vec<_>>();
            let m = im
                .get_mask_slice(&zeros, im.shape(), &ones)
                .expect("read mask")
                .map(|a| a.as_standard_layout().as_slice().expect("mask").to_vec());
            (
                im.coordinates().clone(),
                im.units().to_string(),
                im.history().expect("history"),
                flat,
                m,
            )
        }
        AnyPagedImage::Float64(im) => {
            let zeros = vec![0; im.shape().len()];
            let ones = vec![1; im.shape().len()];
            let slice = im.get_slice(&zeros, im.shape()).expect("read f64");
            let flat = slice
                .as_standard_layout()
                .as_slice()
                .expect("c order")
                .to_vec();
            let m = im
                .get_mask_slice(&zeros, im.shape(), &ones)
                .expect("read mask")
                .map(|a| a.as_standard_layout().as_slice().expect("mask").to_vec());
            (
                im.coordinates().clone(),
                im.units().to_string(),
                im.history().expect("history"),
                flat,
                m,
            )
        }
        _ => panic!("case {name}: unexpected complex output"),
    };

    let exp_bunit = expected["bunit"].as_str().expect("bunit");
    assert_eq!(bunit, exp_bunit, "case {name}: bunit");

    let exp_stokes: Vec<String> = expected["stokes"]
        .as_array()
        .expect("stokes array")
        .iter()
        .map(|v| v.as_str().expect("stokes str").to_string())
        .collect();
    assert_eq!(
        extract_stokes(&coords),
        exp_stokes,
        "case {name}: stokes coordinate"
    );

    let (got_crpix, got_cdelt) = extract_crpix_cdelt(&coords);
    let exp_crpix: Vec<f64> = expected["crpix"]
        .as_array()
        .expect("crpix")
        .iter()
        .map(|v| v.as_f64().expect("f64"))
        .collect();
    let exp_cdelt: Vec<f64> = expected["cdelt"]
        .as_array()
        .expect("cdelt")
        .iter()
        .map(|v| v.as_f64().expect("f64"))
        .collect();
    assert_eq!(got_crpix.len(), exp_crpix.len(), "case {name}: crpix len");
    for (ax, (&g, &e)) in got_crpix.iter().zip(&exp_crpix).enumerate() {
        let tol = 1e-6 + 1e-5 * e.abs();
        assert!(
            (g - e).abs() <= tol,
            "case {name}: crpix axis {ax} mismatch got={g} exp={e}"
        );
    }
    assert_eq!(got_cdelt.len(), exp_cdelt.len(), "case {name}: cdelt len");
    for (ax, (&g, &e)) in got_cdelt.iter().zip(&exp_cdelt).enumerate() {
        let tol = 1e-6 + 1e-5 * e.abs();
        assert!(
            (g - e).abs() <= tol,
            "case {name}: cdelt axis {ax} mismatch got={g} exp={e}"
        );
    }

    if expected["has_history"].as_bool().unwrap_or(false) {
        assert!(
            hist.iter().any(|h| h.contains("immath")),
            "case {name}: expected immath in logtable history, got {hist:?}"
        );
    }

    let exp_mask: Vec<bool> = expected["mask"]
        .as_array()
        .expect("mask array")
        .iter()
        .map(|v| v.as_bool().expect("bool"))
        .collect();
    let got_mask = mask_opt.unwrap_or_else(|| vec![true; exp_mask.len()]);
    assert_eq!(got_mask, exp_mask, "case {name}: pixel mask mismatch");

    let exp_pixels = expected["pixels"].as_array().expect("pixels array");
    assert_eq!(
        pixels_f64.len(),
        exp_pixels.len(),
        "case {name}: pixel count"
    );

    let tol_base = if exp_pt == "double" { 1e-10 } else { 1e-4 };
    let mut sum_sq_diff = 0.0_f64;
    let mut sum_sq_ref = 0.0_f64;
    for (idx, ((&got, exp_v), &valid)) in pixels_f64
        .iter()
        .zip(exp_pixels.iter())
        .zip(exp_mask.iter())
        .enumerate()
    {
        if !valid {
            continue;
        }
        match parse_expected_pixel(exp_v) {
            ExpectedPixel::NaN => {
                assert!(
                    got.is_nan(),
                    "case {name} idx {idx}: expected NaN, got {got}"
                );
            }
            ExpectedPixel::Inf(pos) => {
                assert!(
                    got.is_infinite() && got.is_sign_positive() == pos,
                    "case {name} idx {idx}: expected Inf({pos}), got {got}"
                );
            }
            ExpectedPixel::Finite(e) => {
                assert!(
                    got.is_finite(),
                    "case {name} idx {idx}: expected finite {e}, got {got}"
                );
                let tol = tol_base * (1.0 + e.abs());
                assert!(
                    (got - e).abs() <= tol,
                    "case {name} idx {idx}: pixel mismatch got={got} exp={e} tol={tol}"
                );
                let diff = got - e;
                sum_sq_diff += diff * diff;
                sum_sq_ref += e * e;
            }
        }
    }
    if sum_sq_ref > 0.0 {
        let nrms = (sum_sq_diff / sum_sq_ref).sqrt();
        assert!(
            nrms < tol_base,
            "case {name}: normalized RMS {nrms} exceeded {tol_base}"
        );
    }
}

#[test]
fn immath_matches_casa_676_oracle_cases() {
    let oracle = load_oracle();
    let cases = oracle["cases"].as_object().expect("oracle cases");
    assert_eq!(cases.len(), 34);

    let tmp = stage_inputs();
    for (idx, (name, entry)) in cases.iter().enumerate() {
        let outfile = tmp.path().join(format!("out_{idx:02}_{name}.im"));
        let req = build_request(&entry["params"], tmp.path(), outfile.clone());
        immath(&req).unwrap_or_else(|err| panic!("case {name} failed: {err}"));
        assert_case_matches(name, &outfile, &entry["expected"]);
    }
}

#[test]
fn immath_matches_casa_676_error_cases() {
    let oracle = load_oracle();
    let errors = oracle["errors"].as_object().expect("oracle errors");
    assert_eq!(errors.len(), 8);

    let tmp = stage_inputs();
    for (idx, (name, entry)) in errors.iter().enumerate() {
        let outfile = tmp.path().join(format!("err_{idx:02}_{name}.im"));
        let req = build_request(&entry["params"], tmp.path(), outfile.clone());
        let res = immath(&req);
        assert!(
            res.is_err(),
            "error case {name} should fail like CASA, got Ok({res:?})"
        );
        assert!(
            !outfile.exists(),
            "failed case {name} must not leave output directory {}",
            outfile.display()
        );
    }
}

#[test]
fn immath_fixes_documented_casa_bugs() {
    let tmp = stage_inputs();

    // 1. Unitless polithresh in mode="pola" succeeds and matches explicit brightness unit
    let out_unitless = tmp.path().join("pola_unitless.im");
    let out_explicit = tmp.path().join("pola_explicit.im");
    let q_path = tmp.path().join("q_plane.im");
    let u_path = tmp.path().join("u_plane.im");

    immath(&ImmathRequest {
        imagename: vec![q_path.clone(), u_path.clone()],
        mode: "pola".into(),
        outfile: out_unitless.clone(),
        polithresh: Some("0.1".into()),
        ..ImmathRequest::default()
    })
    .expect("unitless polithresh succeeds in casa-rs");

    immath(&ImmathRequest {
        imagename: vec![q_path, u_path],
        mode: "pola".into(),
        outfile: out_explicit.clone(),
        polithresh: Some("0.1Jy/beam".into()),
        ..ImmathRequest::default()
    })
    .expect("explicit polithresh succeeds");

    let a = AnyPagedImage::open(&out_unitless).expect("open unitless");
    let b = AnyPagedImage::open(&out_explicit).expect("open explicit");
    match (&a, &b) {
        (AnyPagedImage::Float32(ia), AnyPagedImage::Float32(ib)) => {
            let z = vec![0; ia.shape().len()];
            let o = vec![1; ia.shape().len()];
            assert_eq!(
                ia.get_slice(&z, ia.shape()).unwrap(),
                ib.get_slice(&z, ib.shape()).unwrap()
            );
            assert_eq!(
                ia.get_mask_slice(&z, ia.shape(), &o).unwrap(),
                ib.get_mask_slice(&z, ib.shape(), &o).unwrap()
            );
        }
        _ => panic!("expected Float32 pola images"),
    }

    // 2. Non-conforming sigma unit in mode="poli" is rejected instead of silently ignoring units
    let bad_sigma = tmp.path().join("bad_sigma.im");
    let err = immath(&ImmathRequest {
        imagename: vec![tmp.path().join("cube_iquv.im")],
        mode: "poli".into(),
        outfile: bad_sigma.clone(),
        sigma: "5K".into(),
        ..ImmathRequest::default()
    })
    .expect_err("non-conforming sigma unit must fail");
    assert!(
        err.to_string().contains("conform") || err.to_string().contains("unit"),
        "unexpected sigma error: {err}"
    );
    assert!(!bad_sigma.exists());
}

#[test]
fn immath_cli_supports_default_outfile_and_empty_imagename() {
    let tmp = stage_inputs();
    let expected_sum = {
        let lo = PagedImage::<f32>::open(tmp.path().join("spix_lo.im")).unwrap();
        let deg = PagedImage::<f32>::open(tmp.path().join("deg4d.im")).unwrap();
        let lo_pix = lo.get().unwrap();
        let deg_val = deg.get().unwrap().iter().next().copied().unwrap();
        lo_pix.mapv(|v| v + deg_val)
    };

    // 1. CLI omitted --outfile resolves to immath_results.im.
    let status = Command::new(env!("CARGO_BIN_EXE_immath"))
        .current_dir(tmp.path())
        .args([
            "--no-save-last",
            "--mode",
            "evalexpr",
            "--expr",
            "'spix_lo.im' + \"deg4d.im\"",
        ])
        .status()
        .expect("run immath CLI binary");
    assert!(status.success(), "immath CLI failed: {status}");

    let default_out = tmp.path().join("immath_results.im");
    assert!(default_out.exists());
    let img = PagedImage::<f32>::open(&default_out).expect("open default output");
    assert_eq!(img.shape(), &[4, 4, 1, 3]);
    for (g, e) in img.get().unwrap().iter().zip(expected_sum.iter()) {
        assert!((g - e).abs() < 1e-6, "CLI default mismatch: {g} vs {e}");
    }

    // 2. CLI explicit --outfile "" also resolves to immath_results.im.
    let tmp_empty = stage_inputs();
    let status_empty = Command::new(env!("CARGO_BIN_EXE_immath"))
        .current_dir(tmp_empty.path())
        .args([
            "--no-save-last",
            "--mode",
            "evalexpr",
            "--expr",
            "'spix_lo.im' + \"deg4d.im\"",
            "--outfile",
            "",
        ])
        .status()
        .expect("run immath CLI with --outfile ''");
    assert!(
        status_empty.success(),
        "immath CLI --outfile '' failed: {status_empty}"
    );
    let img_empty = PagedImage::<f32>::open(tmp_empty.path().join("immath_results.im"))
        .expect("open --outfile '' output");
    for (g, e) in img_empty.get().unwrap().iter().zip(expected_sum.iter()) {
        assert!(
            (g - e).abs() < 1e-6,
            "CLI --outfile '' mismatch: {g} vs {e}"
        );
    }

    // 3. JSON surface also resolves empty outfile="" -> "immath_results.im".
    let tmp2 = stage_inputs();
    let mut child = Command::new(env!("CARGO_BIN_EXE_immath"))
        .args(["--json-run", "-"])
        .current_dir(tmp2.path())
        .stdin(std::process::Stdio::piped())
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        .spawn()
        .expect("spawn immath json-run");
    use std::io::Write;
    child
        .stdin
        .take()
        .unwrap()
        .write_all(
            br#"{"surface":"immath","values":{"mode":"evalexpr","expr":"'spix_lo.im' + \"deg4d.im\"","outfile":""}}"#,
        )
        .unwrap();
    let output = child.wait_with_output().expect("wait json-run");
    assert!(
        output.status.success(),
        "json-run failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let img_json = PagedImage::<f32>::open(tmp2.path().join("immath_results.im"))
        .expect("open --json-run output");
    for (g, e) in img_json.get().unwrap().iter().zip(expected_sum.iter()) {
        assert!((g - e).abs() < 1e-6, "json-run mismatch: {g} vs {e}");
    }
}

#[test]
fn immath_mixed_imagename_quoted_paths_and_varnames_regressions() {
    let tmp = stage_inputs();
    let lo_path = tmp.path().join("spix_lo.im");
    let deg_path = tmp.path().join("deg4d.im");
    let lo_pix = PagedImage::<f32>::open(&lo_path).unwrap().get().unwrap();
    let deg_val = PagedImage::<f32>::open(&deg_path)
        .unwrap()
        .get()
        .unwrap()
        .iter()
        .next()
        .copied()
        .unwrap();

    // Mixed imagename + quoted extra path keeps imagename and binds IM0 properly.
    for (label, expr) in [
        ("bare_im0", format!("IM0 + '{}'", deg_path.display())),
        ("quoted_im0", format!("'IM0' + '{}'", deg_path.display())),
    ] {
        let out = tmp.path().join(format!("mixed_{label}.im"));
        immath(&ImmathRequest {
            imagename: vec![lo_path.clone()],
            expr,
            outfile: out.clone(),
            ..ImmathRequest::default()
        })
        .unwrap_or_else(|e| panic!("mixed {label} failed: {e}"));
        let got = PagedImage::<f32>::open(&out).unwrap().get().unwrap();
        for (g, &lo) in got.iter().zip(lo_pix.iter()) {
            assert!(
                (*g - (lo + deg_val)).abs() < 1e-6,
                "{label}: got {g}, expected {}",
                lo + deg_val
            );
        }
    }

    // Empty imagename does not silently alias IM0 to a quoted path in expr.
    let err = immath(&ImmathRequest {
        imagename: vec![],
        expr: format!("IM0 + '{}'", deg_path.display()),
        outfile: tmp.path().join("unbound_im0.im"),
        ..ImmathRequest::default()
    })
    .expect_err("imagename=[] with unbound IM0 must fail");
    assert!(
        err.to_string().contains("IM0") || err.to_string().contains("unknown"),
        "unexpected error for unbound IM0: {err}"
    );

    // Path containing varname segment (sub/IM0.im) is preserved by quote_var_aliases.
    let map_path = tmp.path().join("map2d.im");
    let sub_im0 = tmp.path().join("sub").join("IM0.im");
    copy_dir_recursive(&map_path, &sub_im0);
    let out_sub = tmp.path().join("sub_alias.im");
    immath(&ImmathRequest {
        imagename: vec![map_path.clone()],
        expr: format!("'{}' + IM0/2.0", sub_im0.display()),
        outfile: out_sub.clone(),
        ..ImmathRequest::default()
    })
    .expect("sub/IM0.im + IM0/2.0");
    let map_pix = PagedImage::<f32>::open(&map_path).unwrap().get().unwrap();
    let got_sub = PagedImage::<f32>::open(&out_sub).unwrap().get().unwrap();
    for (g, &m) in got_sub.iter().zip(map_pix.iter()) {
        assert!((*g - 1.5 * m).abs() < 1e-5, "got {g}, expected {}", 1.5 * m);
    }

    // Polarization mode honors custom varnames inside mask=.
    let q_path = tmp.path().join("q_plane.im");
    let u_path = tmp.path().join("u_plane.im");
    let pol_out = tmp.path().join("pola_varname_mask.im");
    immath(&ImmathRequest {
        imagename: vec![q_path.clone(), u_path],
        mode: "pola".into(),
        varnames: vec!["QIMG".into(), "UIMG".into()],
        mask: Some("QIMG > 0.1".into()),
        outfile: pol_out.clone(),
        ..ImmathRequest::default()
    })
    .expect("pola with custom varname mask");
    let q_pix = PagedImage::<f32>::open(&q_path).unwrap().get().unwrap();
    let pol_img = PagedImage::<f32>::open(&pol_out).unwrap();
    let pol_mask = pol_img.default_mask().unwrap().expect("expected mask0");
    for (&qv, &mv) in q_pix.iter().zip(pol_mask.iter()) {
        assert_eq!(mv, qv > 0.1);
    }
}

#[test]
fn immath_deferred_overwrite_and_input_alias_guard() {
    let tmp = stage_inputs();
    let map_path = tmp.path().join("map2d.im");
    let sentinel_out = tmp.path().join("existing.im");
    immath(&ImmathRequest {
        imagename: vec![map_path.clone()],
        expr: "IM0 * 0 + 7.5".into(),
        outfile: sentinel_out.clone(),
        ..ImmathRequest::default()
    })
    .expect("create sentinel output");

    for req in [
        ImmathRequest {
            imagename: vec![map_path.clone()],
            expr: "not_a_function(IM0)".into(),
            outfile: sentinel_out.clone(),
            overwrite: true,
            ..ImmathRequest::default()
        },
        ImmathRequest {
            imagename: vec![map_path.clone()],
            expr: "IM0".into(),
            prec: "int".into(),
            outfile: sentinel_out.clone(),
            overwrite: true,
            ..ImmathRequest::default()
        },
        ImmathRequest {
            imagename: vec![tmp.path().join("cube_iquv.im")],
            mode: "pola".into(),
            polithresh: Some("5K".into()),
            outfile: sentinel_out.clone(),
            overwrite: true,
            ..ImmathRequest::default()
        },
        ImmathRequest {
            imagename: vec![map_path.clone()],
            expr: "IM0".into(),
            imagemd: Some("missing_md.im".into()),
            outfile: sentinel_out.clone(),
            overwrite: true,
            ..ImmathRequest::default()
        },
    ] {
        assert!(immath(&req).is_err());
        let kept = PagedImage::<f32>::open(&sentinel_out)
            .expect("existing output must remain intact after failed overwrite")
            .get()
            .unwrap();
        assert!(kept.iter().all(|&v| (v - 7.5).abs() < 1e-6));
    }

    let orig_map = PagedImage::<f32>::open(&map_path).unwrap().get().unwrap();
    let alias_err = immath(&ImmathRequest {
        imagename: vec![map_path.clone()],
        expr: "IM0 * 2.0".into(),
        outfile: map_path.clone(),
        overwrite: true,
        ..ImmathRequest::default()
    })
    .expect_err("outfile aliasing input must fail");
    assert!(alias_err.to_string().contains("input image"));
    let still_map = PagedImage::<f32>::open(&map_path).unwrap().get().unwrap();
    assert_eq!(orig_map, still_map);
}

#[test]
fn immath_compound_indexin_and_streaming_masks() {
    let tmp = stage_inputs();
    let map_path = tmp.path().join("map2d.im");
    let out_and = tmp.path().join("indexin_and.im");
    immath(&ImmathRequest {
        imagename: vec![map_path.clone()],
        expr: "iif(indexin(0, [0, 1]) && indexin(1, [0, 1]), IM0, -1.0)".into(),
        outfile: out_and.clone(),
        ..ImmathRequest::default()
    })
    .expect("compound indexin && indexin");
    let map_pix = PagedImage::<f32>::open(&map_path).unwrap().get().unwrap();
    let got_and = PagedImage::<f32>::open(&out_and).unwrap().get().unwrap();
    for x in 0..4 {
        for y in 0..4 {
            let expected = if x <= 1 && y <= 1 {
                map_pix[IxDyn(&[x, y])]
            } else {
                -1.0
            };
            assert!((got_and[IxDyn(&[x, y])] - expected).abs() < 1e-6);
        }
    }

    // Streaming multi-chunk PagedImage save_as: backfills earlier all-true chunks, omits all-valid mask0.
    let paged_in = tmp.path().join("tiled_masked.im");
    let mut coords2 = CoordinateSystem::new();
    coords2.add_coordinate(DirectionCoordinate::new(
        DirectionRef::J2000,
        Projection::new(ProjectionType::SIN),
        [0.0, 0.0],
        [-1e-4, 1e-4],
        [0.0, 0.0],
    ));
    let mut src =
        PagedImage::<f32>::create_with_tile_shape(vec![8, 8], vec![4, 4], coords2, &paged_in)
            .unwrap();
    let data = ArrayD::from_shape_fn(IxDyn(&[8, 8]), |idx| (idx[0] * 8 + idx[1]) as f32);
    let mask = ArrayD::from_shape_fn(IxDyn(&[8, 8]), |idx| !(idx[0] >= 4 && idx[1] >= 4));
    src.put_slice(&data, &[0, 0]).unwrap();
    src.put_mask("mask0", &mask).unwrap();
    src.set_default_mask("mask0").unwrap();
    src.save().unwrap();
    drop(src);

    let reopened = PagedImage::<f32>::open(&paged_in).unwrap();
    let mut map_res: std::collections::HashMap<String, &dyn ImageInterface<f32>> =
        std::collections::HashMap::new();
    map_res.insert("A".into(), &reopened);
    let resolver = HashMapResolver(map_res);
    let mut compiled = parse_image_expr::<f32, _>("iif(A >= 0.0, A + 1.0, A)", &resolver).unwrap();
    compiled.set_execution_policy(ExecutionPolicy::Serial);
    let streamed_out = tmp.path().join("streamed_mask.im");
    compiled.save_as(&streamed_out).unwrap();
    let saved = PagedImage::<f32>::open(&streamed_out).unwrap();
    assert_eq!(saved.default_mask().unwrap().unwrap(), mask);
    for (g, &d) in saved.get().unwrap().iter().zip(data.iter()) {
        assert!((*g - (d + 1.0)).abs() < 1e-6);
    }

    let mut compiled_all_true =
        parse_image_expr::<f32, _>("iif(value(A) >= 0.0, value(A), A)", &resolver).unwrap();
    compiled_all_true.set_execution_policy(ExecutionPolicy::Serial);
    let all_true_out = tmp.path().join("streamed_all_true.im");
    compiled_all_true.save_as(&all_true_out).unwrap();
    let saved_true = PagedImage::<f32>::open(&all_true_out).unwrap();
    assert!(saved_true.default_mask_name().is_none());
}

#[test]
fn immath_metadata_whitespace_fits_and_nan_fractile() {
    let tmp = stage_inputs();
    let map_path = tmp.path().join("map2d.im");

    // 1. "pa (IM0, IM1)" (space before '(') clears legacy Stokes and sets Pangle.
    let pa_out = tmp.path().join("pa_space.im");
    immath(&ImmathRequest {
        imagename: vec![tmp.path().join("u_plane.im"), tmp.path().join("q_plane.im")],
        expr: "pa (IM0, IM1)".into(),
        outfile: pa_out.clone(),
        ..ImmathRequest::default()
    })
    .expect("pa with space before '('");
    let pa_img = PagedImage::<f32>::open(&pa_out).unwrap();
    assert_eq!(pa_img.units(), "deg");
    let pa_stokes = (0..pa_img.coordinates().n_coordinates())
        .map(|i| pa_img.coordinates().coordinate(i))
        .find(|c| matches!(c, CoordinateModel::Stokes(_)))
        .expect("stokes coord");
    match pa_stokes {
        CoordinateModel::Stokes(s) => {
            assert_eq!(s.stokes(), &[casa_coordinates::StokesType::Pangle]);
        }
        _ => unreachable!(),
    }

    // 2. Mixed FITS (IM0, distinct WCS) + PagedImage (IM1) retains IM0's WCS without leaking legacy_coords.
    let mut alt_coords = CoordinateSystem::new();
    alt_coords.add_coordinate(DirectionCoordinate::new(
        DirectionRef::J2000,
        Projection::new(ProjectionType::SIN),
        [1.0, 0.25],
        [-1e-4, 1e-4],
        [0.0, 0.0],
    ));
    let alt_im = tmp.path().join("alt_src.im");
    let mut alt_paged = PagedImage::<f32>::create(vec![4, 4], alt_coords, &alt_im).unwrap();
    alt_paged
        .put_slice(&ArrayD::from_elem(IxDyn(&[4, 4]), 1.0_f32), &[0, 0])
        .unwrap();
    alt_paged.save().unwrap();
    let alt_fits = tmp.path().join("alt_wcs.fits");
    export_fits(&alt_im, &alt_fits, false, true).unwrap();
    let mixed_wcs_out = tmp.path().join("mixed_wcs.im");
    immath(&ImmathRequest {
        imagename: vec![alt_fits, map_path],
        expr: "IM0 + IM1".into(),
        outfile: mixed_wcs_out.clone(),
        ..ImmathRequest::default()
    })
    .expect("mixed FITS + PagedImage");
    let mixed_img = PagedImage::<f32>::open(&mixed_wcs_out).unwrap();
    let world0 = mixed_img.coordinates().to_world(&[0.0, 0.0]).unwrap();
    assert!((world0[0] - 1.0).abs() < 1e-4, "got {world0:?}");
    assert!((world0[1] - 0.25).abs() < 1e-4, "got {world0:?}");

    // 3. Unmasked NaN fractile uses total_cmp, and all-masked median returns 0.0.
    let mut coords2 = CoordinateSystem::new();
    coords2.add_coordinate(DirectionCoordinate::new(
        DirectionRef::J2000,
        Projection::new(ProjectionType::SIN),
        [0.0, 0.0],
        [-1e-4, 1e-4],
        [0.0, 0.0],
    ));
    let nan_im = tmp.path().join("nan_vals.im");
    let mut nan_paged = PagedImage::<f32>::create(vec![2, 2], coords2, &nan_im).unwrap();
    nan_paged
        .put_slice(
            &ArrayD::from_shape_vec(IxDyn(&[2, 2]), vec![f32::NAN, 1.0, 3.0, 5.0]).unwrap(),
            &[0, 0],
        )
        .unwrap();
    nan_paged.save().unwrap();
    let nan_out = tmp.path().join("nan_frac.im");
    immath(&ImmathRequest {
        imagename: vec![nan_im],
        expr: "IM0 * 0 + fractile(IM0, 0.5)".into(),
        outfile: nan_out.clone(),
        ..ImmathRequest::default()
    })
    .expect("fractile with unmasked NaN must not panic");
    assert!(nan_out.exists());

    let empty_med_out = tmp.path().join("empty_med.im");
    immath(&ImmathRequest {
        imagename: vec![tmp.path().join("map2d.im")],
        mask: Some("IM0 > 1e9".into()),
        expr: "value(IM0) * 0 + median(IM0)".into(),
        outfile: empty_med_out.clone(),
        ..ImmathRequest::default()
    })
    .expect("all-masked median returns 0.0");
    let empty_pix = PagedImage::<f32>::open(&empty_med_out)
        .unwrap()
        .get()
        .unwrap();
    assert!(empty_pix.iter().all(|&v| v == 0.0));
}

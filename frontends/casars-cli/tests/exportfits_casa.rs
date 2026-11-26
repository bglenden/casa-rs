// SPDX-License-Identifier: LGPL-3.0-or-later
//! CASA 6.7.6.14 parity, raw FITS header/pixel verification, CLI, and
//! error regression tests for `exportfits`.

use std::collections::BTreeMap;
use std::fs;
use std::path::PathBuf;
use std::process::Command;

use casa_coordinates::{
    CoordinateSystem, DirectionCoordinate, Projection, ProjectionType, SpectralCoordinate,
    StokesCoordinate, StokesType, fits::FitsHeader,
};
use casa_core::{
    Array2, ArrayD,
    measures::{direction::DirectionRef, frequency::FrequencyRef},
};
use casa_imageanalysis::{ExportFitsRequest, exportfits, imhead, import_fits};
use casa_images::{AnyPagedImage, ImageBeamSet, ImageError, PagedImage};
use fitsio::{FitsFile, hdu::HduInfo};
use ndarray::{IxDyn, ShapeBuilder};
use serde::Deserialize;

#[derive(Debug, Deserialize)]
struct OracleFile {
    casa_version: String,
    cases: Vec<OracleCase>,
    error_cases: Vec<OracleErrorCase>,
}

#[derive(Debug, Deserialize)]
struct OracleParams {
    #[serde(default)]
    velocity: bool,
    #[serde(default)]
    optical: bool,
    #[serde(default = "default_bitpix")]
    bitpix: i32,
    #[serde(default)]
    minpix: f64,
    #[serde(default = "default_maxpix")]
    maxpix: f64,
    #[serde(default)]
    dropstokes: bool,
    #[serde(default = "default_true")]
    stokeslast: bool,
    #[serde(default = "default_true")]
    history: bool,
    #[serde(default)]
    dropdeg: bool,
}

fn default_bitpix() -> i32 {
    -32
}

fn default_maxpix() -> f64 {
    -1.0
}

fn default_true() -> bool {
    true
}

#[derive(Debug, Deserialize)]
struct OracleCase {
    name: String,
    imagename: String,
    params: OracleParams,
    shape: Vec<usize>,
    header: BTreeMap<String, serde_json::Value>,
    history_cards_count: usize,
    has_casa_logtable_markers: bool,
    ia_history_count: usize,
    has_perplanebeams: bool,
    pixels_f_order: Vec<serde_json::Value>,
    mask_f_order: Vec<bool>,
    raw_i16: Option<Vec<i16>>,
}

#[derive(Debug, Deserialize)]
struct OracleErrorCase {
    name: String,
    imagename: String,
    params: OracleParams,
}

fn data_dir() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../../libs/casacore/casa-images/tests/data/exportfits")
}

fn load_oracle() -> OracleFile {
    let raw = fs::read_to_string(data_dir().join("expected.json")).expect("read oracle json");
    serde_json::from_str(&raw).expect("parse oracle json")
}

fn parse_fits_cards_and_raw_i16(
    path: &std::path::Path,
) -> (FitsHeader, Vec<String>, Option<Vec<i16>>) {
    let bytes = fs::read(path).expect("read FITS bytes");
    let mut raw_cards = Vec::new();
    let mut history_cards = Vec::new();
    let mut pos = 0usize;
    let mut end_offset = 0usize;
    while pos + 80 <= bytes.len() {
        let card = String::from_utf8_lossy(&bytes[pos..pos + 80]).into_owned();
        pos += 80;
        if card.starts_with("HISTORY ") {
            history_cards.push(card.trim_end().to_string());
        } else {
            raw_cards.push(card.clone());
        }
        if card.starts_with("END     ") {
            end_offset = pos;
            break;
        }
    }
    let refs: Vec<&str> = raw_cards.iter().map(String::as_str).collect();
    let header = FitsHeader::from_cards(&refs);
    let raw_i16 = if header.get_int("BITPIX") == Some(16) {
        let data_off = end_offset.div_ceil(2880) * 2880;
        let naxis = header.get_int("NAXIS").unwrap_or(0) as usize;
        let count: usize = (1..=naxis)
            .map(|ax| header.get_int(&format!("NAXIS{ax}")).unwrap_or(1) as usize)
            .product();
        let vals = (0..count)
            .map(|i| {
                let b0 = bytes[data_off + 2 * i];
                let b1 = bytes[data_off + 2 * i + 1];
                i16::from_be_bytes([b0, b1])
            })
            .collect();
        Some(vals)
    } else {
        None
    };
    (header, history_cards, raw_i16)
}

fn read_fits_scaled_pixels_and_mask(path: &std::path::Path) -> (Vec<f64>, Vec<bool>) {
    let (hdr, _, raw_i16) = parse_fits_cards_and_raw_i16(path);
    let naxis = hdr.get_int("NAXIS").unwrap_or(0) as usize;
    let count: usize = (1..=naxis)
        .map(|ax| hdr.get_int(&format!("NAXIS{ax}")).unwrap_or(1) as usize)
        .product();
    if let Some(codes) = raw_i16 {
        let bscale = hdr.get_float("BSCALE").unwrap_or(1.0);
        let bzero = hdr.get_float("BZERO").unwrap_or(0.0);
        let blank = hdr.get_int("BLANK").map(|b| b as i16);
        let mut pixels = Vec::with_capacity(count);
        let mut mask = Vec::with_capacity(count);
        for code in codes {
            if blank == Some(code) {
                pixels.push(f64::NAN);
                mask.push(false);
            } else {
                pixels.push(f64::from(code) * bscale + bzero);
                mask.push(true);
            }
        }
        return (pixels, mask);
    }

    let mut fits = FitsFile::open(path).expect("open FITS with fitsio");
    let hdu = fits.primary_hdu().expect("primary HDU");
    let vals = hdu
        .read_image::<Vec<f32>>(&mut fits)
        .expect("read float image");
    let pixels = vals.iter().map(|&v| f64::from(v)).collect();
    let mask = vals.iter().map(|v| !v.is_nan()).collect();
    (pixels, mask)
}

fn decode_oracle_pixel(val: &serde_json::Value) -> Option<f64> {
    match val {
        serde_json::Value::Number(n) => n.as_f64(),
        serde_json::Value::String(s) if s == "NaN" => None,
        other => panic!("unexpected pixel token: {other:?}"),
    }
}

fn assert_header_matches_oracle(
    case_name: &str,
    hdr: &FitsHeader,
    expected: &BTreeMap<String, serde_json::Value>,
) {
    let bitpix = expected["BITPIX"].as_i64().unwrap();
    assert_eq!(hdr.get_int("BITPIX"), Some(bitpix), "{case_name}: BITPIX");
    let naxis = expected["NAXIS"].as_i64().unwrap();
    assert_eq!(hdr.get_int("NAXIS"), Some(naxis), "{case_name}: NAXIS");
    for ax in 1..=naxis {
        let k = format!("NAXIS{ax}");
        assert_eq!(hdr.get_int(&k), expected[&k].as_i64(), "{case_name}: {k}");
    }

    for key in [
        "BUNIT", "OBJECT", "SPECSYS", "RADESYS", "CTYPE1", "CTYPE2", "CTYPE3", "CTYPE4", "CUNIT1",
        "CUNIT2", "CUNIT3", "CUNIT4", "DATE-OBS", "TIMESYS", "TELESCOP", "OBSERVER",
    ] {
        if let Some(exp) = expected.get(key).and_then(|v| v.as_str()) {
            let actual = hdr.get_string(key).unwrap_or("");
            assert_eq!(actual, exp, "{case_name}: {key}");
        } else if key == "SPECSYS"
            && !(1..=naxis).any(|ax| {
                matches!(
                    expected.get(&format!("CTYPE{ax}")).and_then(|v| v.as_str()),
                    Some("FREQ" | "VRAD" | "VOPT")
                )
            })
        {
            assert!(
                hdr.get_string("SPECSYS").is_none(),
                "{case_name}: unexpected SPECSYS: {:?}",
                hdr.get("SPECSYS")
            );
        }
    }

    for int_key in ["BLANK", "VELREF"] {
        match expected.get(int_key).and_then(|v| v.as_i64()) {
            Some(exp) => assert_eq!(hdr.get_int(int_key), Some(exp), "{case_name}: {int_key}"),
            None => assert!(
                hdr.get_int(int_key).is_none(),
                "{case_name}: unexpected {int_key}: {:?}",
                hdr.get(int_key)
            ),
        }
    }

    match expected.get("CASAMBM").and_then(|v| v.as_bool()) {
        Some(exp_casambm) => assert_eq!(
            hdr.get("CASAMBM").and_then(|v| match v {
                casa_coordinates::FitsValue::Logical(b) => Some(*b),
                _ => None,
            }),
            Some(exp_casambm),
            "{case_name}: CASAMBM"
        ),
        None => assert!(
            hdr.get("CASAMBM").is_none(),
            "{case_name}: unexpected CASAMBM: {:?}",
            hdr.get("CASAMBM")
        ),
    }

    let float_keys = [
        "BSCALE", "BZERO", "DATAMIN", "DATAMAX", "RESTFRQ", "ALTRVAL", "ALTRPIX", "EQUINOX",
        "LONPOLE", "LATPOLE", "BMAJ", "BMIN", "BPA", "CRVAL1", "CRVAL2", "CRVAL3", "CRVAL4",
        "CRPIX1", "CRPIX2", "CRPIX3", "CRPIX4", "CDELT1", "CDELT2", "CDELT3", "CDELT4",
    ];

    for float_key in float_keys {
        if let Some(exp) = expected.get(float_key).and_then(|v| v.as_f64()) {
            let actual = hdr
                .get_float(float_key)
                .unwrap_or_else(|| panic!("{case_name}: missing {float_key}"));
            let tol = 1.0e-5 * exp.abs().max(1.0);
            assert!(
                (actual - exp).abs() <= tol,
                "{case_name}: {float_key} mismatch actual={actual} expected={exp}"
            );
        } else if matches!(
            float_key,
            "DATAMIN" | "DATAMAX" | "ALTRVAL" | "ALTRPIX" | "RESTFRQ" | "BMAJ" | "BMIN" | "BPA"
        ) {
            assert!(
                hdr.get_float(float_key).is_none(),
                "{case_name}: unexpected {float_key}: {:?}",
                hdr.get(float_key)
            );
        }
    }

    for i in 1..=naxis {
        for j in 1..=naxis {
            let pc_key = format!("PC{i}_{j}");
            if let Some(exp) = expected.get(&pc_key).and_then(|v| v.as_f64()) {
                let actual = hdr
                    .get_float(&pc_key)
                    .unwrap_or(if i == j { 1.0 } else { 0.0 });
                let tol = 1.0e-5 * exp.abs().max(1.0);
                assert!(
                    (actual - exp).abs() <= tol,
                    "{case_name}: {pc_key} mismatch actual={actual} expected={exp}"
                );
            }
        }
    }
}

#[test]
fn exportfits_matches_casa_676_oracle_cases() {
    let oracle = load_oracle();
    assert_eq!(oracle.casa_version, "6.7.6.14");
    assert_eq!(oracle.cases.len(), 25);

    let tmp = tempfile::tempdir().unwrap();
    for case in &oracle.cases {
        let out_fits = tmp.path().join(format!("{}.fits", case.name));
        let req = ExportFitsRequest {
            imagename: data_dir().join(&case.imagename),
            fitsimage: out_fits.clone(),
            velocity: case.params.velocity,
            optical: case.params.optical,
            bitpix: case.params.bitpix,
            minpix: case.params.minpix,
            maxpix: case.params.maxpix,
            overwrite: false,
            dropstokes: case.params.dropstokes,
            stokeslast: case.params.stokeslast,
            history: case.params.history,
            dropdeg: case.params.dropdeg,
        };
        let summary =
            exportfits(&req).unwrap_or_else(|e| panic!("oracle case {} failed: {e}", case.name));
        assert_eq!(summary.shape, case.shape, "{}: summary shape", case.name);

        let (hdr, history_cards, raw_i16) = parse_fits_cards_and_raw_i16(&out_fits);
        assert_header_matches_oracle(&case.name, &hdr, &case.header);

        if let Some(exp_raw) = &case.raw_i16 {
            let got_raw = raw_i16.expect("raw i16 codes");
            assert_eq!(got_raw.len(), exp_raw.len(), "{}: raw i16 len", case.name);
            for (idx, (g, e)) in got_raw.iter().zip(exp_raw.iter()).enumerate() {
                assert!(
                    (i32::from(*g) - i32::from(*e)).abs() <= 1,
                    "{}: raw_i16[{idx}] got={g} expected={e}",
                    case.name
                );
            }
        }

        let (pixels, mask) = read_fits_scaled_pixels_and_mask(&out_fits);
        assert_eq!(mask, case.mask_f_order, "{}: mask mismatch", case.name);
        let bscale = hdr.get_float("BSCALE").unwrap_or(1.0);
        let pix_tol = if case.params.bitpix == 16 {
            (2.0 * bscale.abs()).max(1.0e-5)
        } else {
            1.0e-5
        };
        for (idx, ((got, valid), exp_val)) in pixels
            .iter()
            .zip(mask.iter())
            .zip(case.pixels_f_order.iter())
            .enumerate()
        {
            match (valid, decode_oracle_pixel(exp_val)) {
                (false, None) => {}
                (true, Some(exp)) => {
                    let tol = pix_tol.max(1.0e-5 * exp.abs());
                    assert!(
                        (got - exp).abs() <= tol,
                        "{}: pixel[{idx}] got={got} expected={exp}",
                        case.name
                    );
                }
                other => panic!("{}: valid/NaN mismatch at {idx}: {other:?}", case.name),
            }
        }

        if case.has_casa_logtable_markers {
            assert!(
                !history_cards.is_empty(),
                "{}: expected HISTORY cards",
                case.name
            );
            assert_eq!(
                history_cards.first().map(String::as_str),
                Some("HISTORY CASA START LOGTABLE"),
                "{}: start marker",
                case.name
            );
            assert_eq!(
                history_cards.last().map(String::as_str),
                Some("HISTORY CASA END LOGTABLE"),
                "{}: end marker",
                case.name
            );
            assert!(
                history_cards.iter().all(|c| c.len() <= 80),
                "{}: HISTORY card exceeded 80 chars",
                case.name
            );
            assert!(
                history_cards.len() >= case.history_cards_count,
                "{}: HISTORY card count {} < oracle {}",
                case.name,
                history_cards.len(),
                case.history_cards_count
            );
            let orig_hist = PagedImage::<f32>::open(data_dir().join(&case.imagename))
                .unwrap()
                .history()
                .unwrap();
            let roundtrip_im = tmp.path().join(format!("{}_rt.im", case.name));
            import_fits(&out_fits, &roundtrip_im, false).unwrap();
            let rt_hist = PagedImage::<f32>::open(&roundtrip_im)
                .unwrap()
                .history()
                .unwrap();
            assert!(
                !rt_hist.is_empty(),
                "{}: roundtrip history empty",
                case.name
            );
            let fits_entries = casa_images::image_history_entries(&out_fits).unwrap();
            assert_eq!(
                fits_entries.len(),
                case.ia_history_count,
                "{}: history entry count",
                case.name
            );
            let fits_messages: Vec<String> = fits_entries
                .into_iter()
                .map(|entry| entry.message)
                .collect();
            assert_eq!(
                fits_messages, orig_hist,
                "{}: lossless HISTORY roundtrip",
                case.name
            );
        } else {
            assert_eq!(
                history_cards.len(),
                0,
                "{}: expected no HISTORY cards",
                case.name
            );
        }

        if case.has_perplanebeams {
            assert_beams_extension_matches(
                &case.name,
                &data_dir().join(&case.imagename),
                &out_fits,
            );
        }
    }
}

fn assert_beams_extension_matches(
    case_name: &str,
    src_image: &std::path::Path,
    out_fits: &std::path::Path,
) {
    let src_beams = PagedImage::<f32>::open(src_image)
        .unwrap()
        .image_info()
        .unwrap()
        .beam_set;
    let mut fits = FitsFile::open(out_fits).unwrap();
    let num_hdus = fits.num_hdus().unwrap();
    assert!(num_hdus >= 2, "{case_name}: missing BEAMS extension HDU");
    let hdu = fits.hdu(1).unwrap();
    assert_eq!(
        hdu.read_key::<String>(&mut fits, "EXTNAME").unwrap().trim(),
        "BEAMS"
    );
    assert!(matches!(hdu.info, HduInfo::TableInfo { .. }));
    assert_eq!(hdu.read_key::<i64>(&mut fits, "EXTVER").unwrap(), 1);
    assert_eq!(
        hdu.read_key::<i64>(&mut fits, "NCHAN").unwrap(),
        src_beams.nchan() as i64
    );
    assert_eq!(
        hdu.read_key::<i64>(&mut fits, "NPOL").unwrap(),
        src_beams.nstokes() as i64
    );
    assert_eq!(
        hdu.read_key::<String>(&mut fits, "TUNIT1").unwrap().trim(),
        "arcsec"
    );
    assert_eq!(
        hdu.read_key::<String>(&mut fits, "TUNIT2").unwrap().trim(),
        "arcsec"
    );
    assert_eq!(
        hdu.read_key::<String>(&mut fits, "TUNIT3").unwrap().trim(),
        "deg"
    );
    let bmaj_col: Vec<f32> = hdu.read_col(&mut fits, "BMAJ").unwrap();
    let bmin_col: Vec<f32> = hdu.read_col(&mut fits, "BMIN").unwrap();
    let bpa_col: Vec<f32> = hdu.read_col(&mut fits, "BPA").unwrap();
    let chan_col: Vec<i32> = hdu.read_col(&mut fits, "CHAN").unwrap();
    let pol_col: Vec<i32> = hdu.read_col(&mut fits, "POL").unwrap();
    let mut row = 0usize;
    for p in 0..src_beams.nstokes() {
        for c in 0..src_beams.nchan() {
            let b = src_beams.beam(c, p);
            assert_eq!(chan_col[row], c as i32, "{case_name}: BEAMS CHAN[{row}]");
            assert_eq!(pol_col[row], p as i32, "{case_name}: BEAMS POL[{row}]");
            assert!(
                (f64::from(bmaj_col[row]) - b.major_in("arcsec").unwrap()).abs() < 1e-4,
                "{case_name}: BEAMS BMAJ[{row}]"
            );
            assert!(
                (f64::from(bmin_col[row]) - b.minor_in("arcsec").unwrap()).abs() < 1e-4,
                "{case_name}: BEAMS BMIN[{row}]"
            );
            assert!(
                (f64::from(bpa_col[row]) - b.position_angle_in("deg").unwrap()).abs() < 1e-4,
                "{case_name}: BEAMS BPA[{row}]"
            );
            row += 1;
        }
    }
    assert_eq!(row, bmaj_col.len());
    let summary = imhead(out_fits).unwrap();
    assert!(summary.has_multiple_beams, "{case_name}: imhead multi beam");
}

#[test]
fn exportfits_matches_casa_676_error_cases() {
    let oracle = load_oracle();
    assert_eq!(oracle.error_cases.len(), 5);
    let tmp = tempfile::tempdir().unwrap();
    let existing = tmp.path().join("existing.fits");
    exportfits(&ExportFitsRequest {
        imagename: data_dir().join("no_mask.im"),
        fitsimage: existing.clone(),
        ..ExportFitsRequest::default()
    })
    .unwrap();

    for case in &oracle.error_cases {
        let fitsimage = match case.name.as_str() {
            "empty_fitsimage" => PathBuf::new(),
            "existing_no_overwrite" => existing.clone(),
            other => tmp.path().join(format!("{other}.fits")),
        };
        let imagename = if case.imagename == "missing.im" {
            tmp.path().join("missing.im")
        } else {
            data_dir().join(&case.imagename)
        };
        let err = exportfits(&ExportFitsRequest {
            imagename,
            fitsimage,
            velocity: case.params.velocity,
            optical: case.params.optical,
            bitpix: case.params.bitpix,
            minpix: case.params.minpix,
            maxpix: case.params.maxpix,
            overwrite: false,
            dropstokes: case.params.dropstokes,
            stokeslast: case.params.stokeslast,
            history: case.params.history,
            dropdeg: case.params.dropdeg,
        });
        assert!(err.is_err(), "expected error for {}", case.name);
    }
}

#[test]
fn exportfits_deferred_overwrite_inplace_refusal_and_f64_bitpix16() {
    let tmp = tempfile::tempdir().unwrap();
    let sentinel = tmp.path().join("sentinel.fits");
    fs::write(&sentinel, b"DO_NOT_CLOBBER").unwrap();

    // Validation or all-degenerate failure must preserve existing output even with overwrite=true.
    let bad_bp = exportfits(&ExportFitsRequest {
        imagename: data_dir().join("no_mask.im"),
        fitsimage: sentinel.clone(),
        bitpix: 8,
        overwrite: true,
        ..ExportFitsRequest::default()
    });
    assert!(matches!(bad_bp, Err(ImageError::InvalidMetadata(_))));
    assert_eq!(fs::read(&sentinel).unwrap(), b"DO_NOT_CLOBBER");

    let all_deg = exportfits(&ExportFitsRequest {
        imagename: data_dir().join("all_deg.im"),
        fitsimage: sentinel.clone(),
        dropdeg: true,
        overwrite: true,
        ..ExportFitsRequest::default()
    });
    assert!(matches!(all_deg, Err(ImageError::InvalidMetadata(_))));
    assert_eq!(fs::read(&sentinel).unwrap(), b"DO_NOT_CLOBBER");

    // Refuses overwriting an input FITS image in place even when overwrite=true.
    let fits_src = tmp.path().join("src.fits");
    fs::copy(data_dir().join("input_2d.fits"), &fits_src).unwrap();
    let orig_bytes = fs::read(&fits_src).unwrap();
    let inplace = exportfits(&ExportFitsRequest {
        imagename: fits_src.clone(),
        fitsimage: fits_src.clone(),
        overwrite: true,
        ..ExportFitsRequest::default()
    });
    assert!(inplace.is_err());
    assert_eq!(fs::read(&fits_src).unwrap(), orig_bytes);

    // Double-precision image supports both bitpix=-32 (BITPIX=-64) and bitpix=16 (BITPIX=16).
    let f64_im = tmp.path().join("f64.im");
    let mut coords = CoordinateSystem::new();
    coords.add_coordinate(DirectionCoordinate::new(
        DirectionRef::J2000,
        Projection::new(ProjectionType::SIN),
        [0.0, 0.0],
        [-1.0e-5, 1.0e-5],
        [1.0, 1.0],
    ));
    coords.add_coordinate(SpectralCoordinate::new(
        FrequencyRef::LSRK,
        1.42e9,
        1.0e6,
        0.0,
        1.42040575e9,
    ));
    let mut img = PagedImage::<f64>::create(vec![2, 2, 2], coords, &f64_im).unwrap();
    let vals: Vec<f64> = (0..8).map(|i| i as f64 * 1.5 - 2.0).collect();
    img.put_slice(
        &ArrayD::from_shape_vec(IxDyn(&[2, 2, 2]).f(), vals.clone()).unwrap(),
        &[0, 0, 0],
    )
    .unwrap();
    img.save().unwrap();
    assert!(matches!(
        AnyPagedImage::open(&f64_im).unwrap(),
        AnyPagedImage::Float64(_)
    ));

    let out64_f = tmp.path().join("out64_f.fits");
    exportfits(&ExportFitsRequest {
        imagename: f64_im.clone(),
        fitsimage: out64_f.clone(),
        bitpix: -32,
        history: false,
        ..ExportFitsRequest::default()
    })
    .unwrap();
    let (h_f, _, _) = parse_fits_cards_and_raw_i16(&out64_f);
    assert_eq!(h_f.get_int("BITPIX"), Some(-64));

    let out64_16 = tmp.path().join("out64_16.fits");
    exportfits(&ExportFitsRequest {
        imagename: f64_im,
        fitsimage: out64_16.clone(),
        bitpix: 16,
        history: false,
        ..ExportFitsRequest::default()
    })
    .unwrap();
    let (h_16, _, raw_16) = parse_fits_cards_and_raw_i16(&out64_16);
    assert_eq!(h_16.get_int("BITPIX"), Some(16));
    assert!(raw_16.is_some());
    let (readback, _) = read_fits_scaled_pixels_and_mask(&out64_16);
    for (g, e) in readback.iter().zip(vals.iter()) {
        assert!((g - e).abs() < 1.0e-3, "f64 bitpix16 mismatch: {g} vs {e}");
    }
}

#[test]
fn exportfits_cli_flags_and_json_run() {
    let tmp = tempfile::tempdir().unwrap();
    let bin = env!("CARGO_BIN_EXE_exportfits");

    // Flags invocation: velocity + optical + bitpix 16 + no-stokeslast + no-history
    let cli_out = tmp.path().join("cli_out.fits");
    let status = Command::new(bin)
        .current_dir(tmp.path())
        .arg(data_dir().join("cube_i.im"))
        .arg(&cli_out)
        .args([
            "--velocity",
            "--optical",
            "--bitpix",
            "16",
            "--minpix",
            "-2.0",
            "--maxpix",
            "5.0",
            "--no-stokeslast",
            "--no-history",
        ])
        .output()
        .expect("run exportfits CLI");
    assert!(
        status.status.success(),
        "stderr: {}",
        String::from_utf8_lossy(&status.stderr)
    );
    let (hdr, hist, raw_i16) = parse_fits_cards_and_raw_i16(&cli_out);
    assert_eq!(hdr.get_int("BITPIX"), Some(16));
    assert_eq!(hdr.get_string("CTYPE3"), Some("STOKES"));
    assert_eq!(hdr.get_string("CTYPE4"), Some("VOPT"));
    assert_eq!(hdr.get_int("VELREF"), Some(1));
    assert_eq!(hdr.get_int("BLANK"), Some(-32768));
    assert!(hist.is_empty());
    assert!(raw_i16.is_some());

    // Invalid bitpix attribution
    let bad = Command::new(bin)
        .current_dir(tmp.path())
        .args(["--json-run", "-"])
        .stdin(std::process::Stdio::piped())
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        .spawn()
        .expect("spawn exportfits --json-run");
    use std::io::Write;
    let payload = serde_json::json!({
        "surface": "exportfits",
        "values": {
            "imagename": data_dir().join("no_mask.im"),
            "fitsimage": tmp.path().join("bad_bp.fits"),
            "bitpix": 8,
        }
    });
    let mut child = bad;
    child
        .stdin
        .take()
        .unwrap()
        .write_all(payload.to_string().as_bytes())
        .unwrap();
    let out = child.wait_with_output().unwrap();
    assert!(!out.status.success());
    let stdout = String::from_utf8_lossy(&out.stdout);
    assert!(stdout.contains("\"bitpix\""), "stdout: {stdout}");

    // Valid --json-run with dropdeg + history + overwrite
    let json_out = tmp.path().join("json_out.fits");
    fs::write(&json_out, b"stale").unwrap();
    let mut child = Command::new(bin)
        .current_dir(tmp.path())
        .args(["--json-run", "-"])
        .stdin(std::process::Stdio::piped())
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        .spawn()
        .unwrap();
    let ok_payload = serde_json::json!({
        "surface": "exportfits",
        "values": {
            "imagename": data_dir().join("cube_i.im"),
            "fitsimage": &json_out,
            "dropdeg": true,
            "history": true,
            "overwrite": true,
        }
    });
    child
        .stdin
        .take()
        .unwrap()
        .write_all(ok_payload.to_string().as_bytes())
        .unwrap();
    let out = child.wait_with_output().unwrap();
    assert!(
        out.status.success(),
        "stderr: {} stdout: {}",
        String::from_utf8_lossy(&out.stderr),
        String::from_utf8_lossy(&out.stdout)
    );
    let (hdr2, hist2, _) = parse_fits_cards_and_raw_i16(&json_out);
    assert_eq!(hdr2.get_int("NAXIS"), Some(3));
    assert_eq!(hdr2.get_string("CTYPE3"), Some("FREQ"));
    assert!(!hist2.is_empty());
}

#[test]
fn exportfits_preserves_b1950_vla_in_the_fits_image() {
    use casa_coordinates::{CoordinateModel, CoordinateType};

    let temp = tempfile::tempdir().unwrap();
    let source = temp.path().join("b1950-vla.im");
    let mut coordinates = CoordinateSystem::new();
    coordinates.add_coordinate(DirectionCoordinate::new(
        DirectionRef::B1950Vla,
        Projection::new(ProjectionType::SIN),
        [1.0, 0.5],
        [-1e-5, 1e-5],
        [1.0, 1.0],
    ));
    let mut image = PagedImage::<f32>::create(vec![3, 3], coordinates, &source).unwrap();
    image.save().unwrap();
    drop(image);
    let destination = temp.path().join("b1950-vla.fits");
    exportfits(&ExportFitsRequest {
        imagename: source,
        fitsimage: destination.clone(),
        ..ExportFitsRequest::default()
    })
    .unwrap();
    let (header, _, _) = parse_fits_cards_and_raw_i16(&destination);
    assert_eq!(header.get_string("RADESYS"), Some("FK4"));
    assert_eq!(header.get_float("EQUINOX"), Some(1979.9));
    let restored = casa_images::read_fits_image(destination).unwrap();
    let index = restored
        .coordinates
        .find_coordinate(CoordinateType::Direction)
        .unwrap();
    let CoordinateModel::Direction(direction) = restored.coordinates.coordinate(index) else {
        panic!("expected direction coordinate");
    };
    assert_eq!(direction.direction_ref(), DirectionRef::B1950Vla);

    // Older casacore writers spell the FITS equinox card EPOCH.
    let mut bytes = fs::read(temp.path().join("b1950-vla.fits")).unwrap();
    let card = bytes
        .as_chunks_mut::<80>()
        .0
        .iter_mut()
        .find(|card| &card[..8] == b"EQUINOX ")
        .unwrap();
    card[..8].copy_from_slice(b"EPOCH   ");
    let legacy = temp.path().join("b1950-vla-legacy.fits");
    fs::write(&legacy, bytes).unwrap();
    let converted = temp.path().join("b1950-vla-legacy.im");
    import_fits(&legacy, &converted, false).unwrap();
    let image = PagedImage::<f32>::open(converted).unwrap();
    let index = image
        .coordinates()
        .find_coordinate(CoordinateType::Direction)
        .unwrap();
    let CoordinateModel::Direction(direction) = image.coordinates().coordinate(index) else {
        panic!("expected direction coordinate");
    };
    assert_eq!(direction.direction_ref(), DirectionRef::B1950Vla);
}

#[test]
fn exportfits_remaps_projection_parameters_with_the_latitude_axis() {
    use casa_coordinates::{CoordinateModel, CoordinateType};
    use casa_images::read_fits_image;

    let temp = tempfile::tempdir().unwrap();
    for (kind, parameters) in [
        (ProjectionType::AZP, vec![2.0, 30.0]),
        (ProjectionType::AIR, vec![45.0]),
        (ProjectionType::SZP, vec![2.0, 30.0, 45.0]),
    ] {
        let source = temp.path().join(format!("{kind}.im"));
        let mut coordinates = CoordinateSystem::new();
        coordinates.add_coordinate(StokesCoordinate::new(vec![StokesType::I]));
        let direction = DirectionCoordinate::new(
            DirectionRef::J2000,
            Projection::with_parameters(kind, parameters.clone()),
            [1.0, 0.5],
            [-0.1, 0.1],
            [2.0, 2.0],
        );
        coordinates.add_coordinate(direction.clone());
        let mut image = PagedImage::<f32>::create(vec![1, 6, 5], coordinates, &source).unwrap();
        image.save().unwrap();
        drop(image);

        for (dropstokes, shape) in [(false, vec![6, 5, 1]), (true, vec![6, 5])] {
            let destination = temp.path().join(format!("{kind}-{dropstokes}.fits"));
            exportfits(&ExportFitsRequest {
                imagename: source.clone(),
                fitsimage: destination.clone(),
                stokeslast: true,
                dropstokes,
                ..ExportFitsRequest::default()
            })
            .unwrap();
            let (header, _, _) = parse_fits_cards_and_raw_i16(&destination);
            assert_eq!(
                header.get_string("CTYPE2"),
                Some(format!("DEC--{kind}").as_str()),
            );
            for (index, &parameter) in (1..).zip(&parameters) {
                assert_eq!(header.get_float(&format!("PV2_{index}")), Some(parameter));
                assert!(header.get_float(&format!("PV3_{index}")).is_none());
            }
            let restored = read_fits_image(&destination).unwrap();
            assert_eq!(restored.shape, shape);
            let restored = restored.coordinates;
            let index = restored.find_coordinate(CoordinateType::Direction).unwrap();
            let CoordinateModel::Direction(restored) = restored.coordinate(index) else {
                panic!("expected direction coordinate");
            };
            let pixel = [1.4, 3.1];
            let expected = direction.pixel_to_world(pixel).unwrap();
            let actual = restored.pixel_to_world(pixel).unwrap();
            for (actual, expected) in actual.into_iter().zip(expected) {
                assert!((actual - expected).abs() < 1e-12, "{actual} != {expected}");
            }
        }
    }
}

#[test]
fn exportfits_bitpix16_edge_cases_and_rotated_pc_remapping() {
    let tmp = tempfile::tempdir().unwrap();

    // 1. Unmasked NaN/+Inf/-Inf in bitpix=16 encode as BLANK = -32768.
    let mut coords = CoordinateSystem::new();
    coords.add_coordinate(DirectionCoordinate::new(
        DirectionRef::J2000,
        Projection::new(ProjectionType::SIN),
        [0.0, 0.0],
        [-1.0e-5, 1.0e-5],
        [1.0, 1.0],
    ));
    let nan_im = tmp.path().join("nan.im");
    let mut img = PagedImage::<f32>::create(vec![2, 2], coords.clone(), &nan_im).unwrap();
    img.put_slice(
        &ArrayD::from_shape_vec(IxDyn(&[2, 2]).f(), vec![f32::NAN, 1.0, f32::INFINITY, -2.0])
            .unwrap(),
        &[0, 0],
    )
    .unwrap();
    img.save().unwrap();
    let nan_fits = tmp.path().join("nan16.fits");
    exportfits(&ExportFitsRequest {
        imagename: nan_im,
        fitsimage: nan_fits.clone(),
        bitpix: 16,
        history: false,
        ..ExportFitsRequest::default()
    })
    .unwrap();
    let (hdr, _, raw) = parse_fits_cards_and_raw_i16(&nan_fits);
    assert_eq!(hdr.get_int("BLANK"), Some(-32768));
    let raw = raw.unwrap();
    assert_eq!(raw[0], -32768);
    assert_eq!(raw[2], -32768);
    let (decoded, mask) = read_fits_scaled_pixels_and_mask(&nan_fits);
    assert_eq!(mask, vec![false, true, false, true]);
    assert!((decoded[1] - 1.0).abs() < 1e-4);
    assert!((decoded[3] + 2.0).abs() < 1e-4);

    // 2. Negative constant image, minpix==maxpix, reversed positive minpix>maxpix, and Float64 extremes.
    let f64_coords = coords.clone();
    for (label, vals, minpix, maxpix, expect_val) in [
        ("neg_const", vec![-5.0_f64; 4], 0.0, -1.0, -5.0_f64),
        ("eq_bounds", vec![1.0, 2.5, 4.0, 10.0], 2.5, 2.5, 2.5_f64),
        ("huge_const", vec![1.0e40_f64; 4], 0.0, -1.0, 1.0e40_f64),
        (
            "subnormal_const",
            vec![1.0e-46_f64; 4],
            0.0,
            -1.0,
            1.0e-46_f64,
        ),
        (
            "extreme_span",
            vec![-1.0e308, 0.0, 1.0e308, 0.5e308],
            0.0,
            -1.0,
            1.0e308_f64,
        ),
    ] {
        let im_path = tmp.path().join(format!("{label}.im"));
        let mut im64 = PagedImage::<f64>::create(vec![2, 2], f64_coords.clone(), &im_path).unwrap();
        im64.put_slice(
            &ArrayD::from_shape_vec(IxDyn(&[2, 2]).f(), vals.clone()).unwrap(),
            &[0, 0],
        )
        .unwrap();
        im64.save().unwrap();
        let out = tmp.path().join(format!("{label}.fits"));
        exportfits(&ExportFitsRequest {
            imagename: im_path,
            fitsimage: out.clone(),
            bitpix: 16,
            minpix,
            maxpix,
            history: false,
            ..ExportFitsRequest::default()
        })
        .unwrap();
        let (h, _, _) = parse_fits_cards_and_raw_i16(&out);
        let bscale = h.get_float("BSCALE").unwrap();
        let bzero = h.get_float("BZERO").unwrap();
        assert!(
            bscale.is_finite() && bscale != 0.0,
            "{label}: bad BSCALE {bscale}"
        );
        assert!(bzero.is_finite(), "{label}: bad BZERO {bzero}");
        let (px, m) = read_fits_scaled_pixels_and_mask(&out);
        assert_eq!(m, vec![true; 4], "{label}");
        let target = if label == "extreme_span" {
            px[2]
        } else {
            px[0]
        };
        let tol = (1.0e-4 * expect_val.abs()).max(1.0e-48);
        assert!(
            (target - expect_val).abs() <= tol,
            "{label}: decoded {target} != {expect_val}"
        );
    }

    // Reversed positive minpix > maxpix triggers auto scaling across actual data range [1, 10].
    let auto_rev_fits = tmp.path().join("auto_rev.fits");
    exportfits(&ExportFitsRequest {
        imagename: tmp.path().join("eq_bounds.im"),
        fitsimage: auto_rev_fits.clone(),
        bitpix: 16,
        minpix: 5.0,
        maxpix: 2.0,
        history: false,
        ..ExportFitsRequest::default()
    })
    .unwrap();
    let (h_rev, _, _) = parse_fits_cards_and_raw_i16(&auto_rev_fits);
    assert!((h_rev.get_float("DATAMIN").unwrap() - 1.0).abs() < 1e-6);
    assert!((h_rev.get_float("DATAMAX").unwrap() - 10.0).abs() < 1e-6);

    // 3. Rotated PC matrix with leading degenerate Stokes axis shifts PC2_2..PC3_3 -> PC1_1..PC2_2.
    let mut rot_coords = CoordinateSystem::new();
    rot_coords.add_coordinate(StokesCoordinate::new(vec![StokesType::I]));
    let rot_dir = DirectionCoordinate::new(
        DirectionRef::J2000,
        Projection::new(ProjectionType::SIN),
        [0.0, 0.0],
        [-1.0e-5, 1.0e-5],
        [1.0, 1.0],
    )
    .with_pc_matrix(Array2::from_shape_vec((2, 2), vec![0.8, -0.6, 0.6, 0.8]).unwrap());
    rot_coords.add_coordinate(rot_dir);
    rot_coords.add_coordinate(SpectralCoordinate::new(
        FrequencyRef::LSRK,
        1.42e9,
        1.0e6,
        0.0,
        1.42040575e9,
    ));
    let rot_im = tmp.path().join("rot.im");
    let mut rot = PagedImage::<f32>::create(vec![1, 2, 2, 2], rot_coords, &rot_im).unwrap();
    rot.put_slice(
        &ArrayD::from_shape_vec(IxDyn(&[1, 2, 2, 2]).f(), vec![1.0; 8]).unwrap(),
        &[0, 0, 0, 0],
    )
    .unwrap();
    rot.save().unwrap();
    let rot_fits = tmp.path().join("rot_dropstokes.fits");
    exportfits(&ExportFitsRequest {
        imagename: rot_im,
        fitsimage: rot_fits.clone(),
        dropstokes: true,
        history: false,
        ..ExportFitsRequest::default()
    })
    .unwrap();
    let (rh, _, _) = parse_fits_cards_and_raw_i16(&rot_fits);
    assert_eq!(rh.get_int("NAXIS"), Some(3));
    assert_eq!(rh.get_string("CTYPE1"), Some("RA---SIN"));
    assert_eq!(rh.get_string("CTYPE2"), Some("DEC--SIN"));
    assert_eq!(rh.get_string("CTYPE3"), Some("FREQ"));
    assert!((rh.get_float("PC1_1").unwrap() - 0.8).abs() < 1e-6);
    assert!((rh.get_float("PC1_2").unwrap() + 0.6).abs() < 1e-6);
    assert!((rh.get_float("PC2_1").unwrap() - 0.6).abs() < 1e-6);
    assert!((rh.get_float("PC2_2").unwrap() - 0.8).abs() < 1e-6);
    assert!(rh.get_float("PC3_2").is_none());
    assert!(rh.get_float("PC3_3").is_none());
    let _ = ImageBeamSet::default();
}

#[test]
fn exportfits_history_multiline_fits_roundtrip_and_bang_symlink_safety() {
    let tmp = tempfile::tempdir().unwrap();

    // 1. Multiline, non-ASCII UTF-8 (>72 bytes with <=72 chars), and FITS->FITS history roundtrip.
    let mut coords = CoordinateSystem::new();
    coords.add_coordinate(DirectionCoordinate::new(
        DirectionRef::J2000,
        Projection::new(ProjectionType::SIN),
        [0.0, 0.0],
        [-1.0e-5, 1.0e-5],
        [1.0, 1.0],
    ));
    let hist_im = tmp.path().join("hist_edge.im");
    let mut img = PagedImage::<f32>::create(vec![2, 2], coords, &hist_im).unwrap();
    img.save().unwrap();
    drop(img);
    let accented = format!("cafe_{}_tail", "é".repeat(40)); // 50 chars, 90 UTF-8 bytes
    casa_images::add_image_history(
        &hist_im,
        "multi",
        &["first_line\nsecond_line".to_string(), accented.clone()],
    )
    .unwrap();

    let f1 = tmp.path().join("h1.fits");
    exportfits(&ExportFitsRequest {
        imagename: hist_im,
        fitsimage: f1.clone(),
        history: true,
        ..ExportFitsRequest::default()
    })
    .unwrap();
    let (_, h1_cards, _) = parse_fits_cards_and_raw_i16(&f1);
    assert!(h1_cards.iter().all(|c| c.len() <= 80));
    let e1 = casa_images::image_history_entries(&f1).unwrap();
    assert_eq!(e1.len(), 3);
    assert_eq!(e1[0].message, "first_line");
    assert_eq!(e1[1].message, "second_line");
    assert!(e1[2].message.starts_with("cafe_"));
    assert!(e1[2].message.ends_with("_tail"));
    assert!(e1.iter().all(|e| e.location == "::multi"));

    let f2 = tmp.path().join("h2.fits");
    exportfits(&ExportFitsRequest {
        imagename: f1,
        fitsimage: f2.clone(),
        history: true,
        ..ExportFitsRequest::default()
    })
    .unwrap();
    let e2 = casa_images::image_history_entries(&f2).unwrap();
    assert_eq!(e2, e1);

    // 2. Leading '!' in fitsimage does NOT trigger CFITSIO clobber on existing.fits or input_2d.fits.
    let cwd_existing = tmp.path().join("existing.fits");
    fs::write(&cwd_existing, b"KEEP_EXISTING").unwrap();
    let bang_path = tmp.path().join("!existing.fits");
    exportfits(&ExportFitsRequest {
        imagename: data_dir().join("no_mask.im"),
        fitsimage: bang_path.clone(),
        overwrite: false,
        history: false,
        ..ExportFitsRequest::default()
    })
    .unwrap();
    assert_eq!(fs::read(&cwd_existing).unwrap(), b"KEEP_EXISTING");
    assert!(bang_path.exists());

    // 3. Broken symlink is refused when overwrite=false and atomically replaced when overwrite=true.
    #[cfg(unix)]
    {
        let broken_link = tmp.path().join("dangling.fits");
        std::os::unix::fs::symlink(tmp.path().join("nonexistent_target.fits"), &broken_link)
            .unwrap();
        let err = exportfits(&ExportFitsRequest {
            imagename: data_dir().join("no_mask.im"),
            fitsimage: broken_link.clone(),
            overwrite: false,
            history: false,
            ..ExportFitsRequest::default()
        });
        assert!(err.is_err(), "expected broken symlink to be refused");
        assert!(fs::symlink_metadata(&broken_link).is_ok());

        exportfits(&ExportFitsRequest {
            imagename: data_dir().join("no_mask.im"),
            fitsimage: broken_link.clone(),
            overwrite: true,
            history: false,
            ..ExportFitsRequest::default()
        })
        .unwrap();
        let (h, _, _) = parse_fits_cards_and_raw_i16(&broken_link);
        assert_eq!(h.get_int("BITPIX"), Some(-32));
        assert!(
            !fs::symlink_metadata(&broken_link)
                .unwrap()
                .file_type()
                .is_symlink()
        );
    }
}

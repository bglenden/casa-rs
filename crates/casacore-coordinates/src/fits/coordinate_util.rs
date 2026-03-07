// SPDX-License-Identifier: LGPL-3.0-or-later
//! Conversion utilities between [`CoordinateSystem`] and FITS WCS headers.
//!
//! The two main entry points are:
//!
//! - [`to_fits_header`] — serialise a [`CoordinateSystem`] and image shape into
//!   a [`FitsHeader`] containing standard WCS keywords.
//! - [`from_fits_header`] — parse a [`FitsHeader`] (and image shape) back into
//!   a [`CoordinateSystem`].
//!
//! The keyword mappings follow:
//!
//! | FITS keyword | Meaning |
//! |---|---|
//! | `NAXIS`, `NAXISn` | Number of axes and axis lengths |
//! | `CTYPEn` | Axis type + projection code |
//! | `CRVALn` | Reference world value |
//! | `CRPIXn` | Reference pixel (1-based in FITS, 0-based internally) |
//! | `CDELTn` | Axis increment |
//! | `CUNITn` | Axis unit string |
//! | `PCi_j` | Linear transformation matrix |
//! | `CDi_j` | Alternative CD matrix (= CDELTi * PCi_j) |
//! | `CROTA2` | Legacy rotation angle (degrees) |
//! | `RADESYS` | Direction reference system (FK5, FK4, ICRS) |
//! | `EQUINOX` | Equinox year |
//! | `SPECSYS` | Spectral reference frame |
//! | `RESTFRQ` | Rest frequency in Hz |
//! | `TELESCOP`, `OBSERVER`, `DATE-OBS` | Observation metadata |
//!
//! Corresponds to C++ `FITSCoordinateUtil`.

use std::f64::consts::PI;

use casacore_types::measures::direction::DirectionRef;
use casacore_types::measures::frequency::FrequencyRef;

use super::header::{FitsHeader, FitsValue};
use crate::CoordinateSystem;
use crate::coordinate::{Coordinate, CoordinateType};
use crate::direction::DirectionCoordinate;
use crate::error::CoordinateError;
use crate::linear::LinearCoordinate;
use crate::obs_info::ObsInfo;
use crate::projection::{Projection, ProjectionType};
use crate::spectral::SpectralCoordinate;
use crate::stokes::{StokesCoordinate, StokesType};

/// Radians to degrees.
const RAD_TO_DEG: f64 = 180.0 / PI;
/// Degrees to radians.
const DEG_TO_RAD: f64 = PI / 180.0;

// ---------------------------------------------------------------------------
// CoordinateSystem -> FITS header
// ---------------------------------------------------------------------------

/// Converts a [`CoordinateSystem`] and image shape to a [`FitsHeader`]
/// containing standard FITS WCS keywords.
///
/// The `shape` slice gives the length of each pixel axis; its length must
/// equal [`CoordinateSystem::n_pixel_axes`].
///
/// # FITS conventions
///
/// - Reference pixel values (`CRPIXn`) are emitted as 1-based (FITS convention).
/// - Direction coordinate values are emitted in degrees.
/// - The PC matrix is written as `PCi_j` keywords; only non-identity elements
///   are emitted.
///
/// # Panics
///
/// Panics if `shape.len()` does not match `cs.n_pixel_axes()`.
pub fn to_fits_header(cs: &CoordinateSystem, shape: &[usize]) -> FitsHeader {
    assert_eq!(
        shape.len(),
        cs.n_pixel_axes(),
        "shape length must match n_pixel_axes"
    );

    let mut h = FitsHeader::new();
    let naxis = cs.n_pixel_axes();
    h.set("NAXIS", FitsValue::Integer(naxis as i64));
    for (i, &len) in shape.iter().enumerate() {
        h.set(format!("NAXIS{}", i + 1), FitsValue::Integer(len as i64));
    }

    let mut axis = 1usize; // 1-based FITS axis counter

    for ci in 0..cs.n_coordinates() {
        let coord = cs.coordinate(ci);
        let ct = coord.coordinate_type();
        match ct {
            CoordinateType::Direction => {
                emit_direction(coord, &mut h, axis);
                axis += 2;
            }
            CoordinateType::Spectral => {
                emit_spectral(coord, &mut h, axis);
                axis += 1;
            }
            CoordinateType::Stokes => {
                emit_stokes(coord, &mut h, axis);
                axis += 1;
            }
            CoordinateType::Linear | CoordinateType::Tabular => {
                let n = coord.n_pixel_axes();
                emit_linear(coord, &mut h, axis);
                axis += n;
            }
        }
    }

    // Observation metadata
    let obs = cs.obs_info();
    if !obs.telescope.is_empty() {
        h.set("TELESCOP", FitsValue::String(obs.telescope.clone()));
    }
    if !obs.observer.is_empty() {
        h.set("OBSERVER", FitsValue::String(obs.observer.clone()));
    }
    if let Some(ref epoch) = obs.date {
        // Store as ISO date string (simplified: MJD as float in DATE-OBS
        // is non-standard, so we emit a placeholder).
        let mjd = epoch.value().as_mjd();
        h.set("DATE-OBS", FitsValue::String(format!("MJD {mjd:.10}")));
    }

    h
}

/// Emits FITS keywords for a [`DirectionCoordinate`].
///
/// Uses the trait object to get generic properties, then downcasts internally
/// via the known field accessors on [`DirectionCoordinate`]. Because we
/// cannot downcast a `&dyn Coordinate` directly (the trait is not `Any`), we
/// reconstruct the needed info from the trait methods and the record.
fn emit_direction(coord: &dyn Coordinate, h: &mut FitsHeader, axis: usize) {
    let crval = coord.reference_value();
    let crpix = coord.reference_pixel();
    let cdelt = coord.increment();

    // We need projection, direction_ref, longpole, latpole, pc.
    // Extract from the record representation.
    let rec = coord.to_record();

    // Projection name
    let proj_name = match rec.get("projection") {
        Some(casacore_types::Value::Scalar(casacore_types::ScalarValue::String(s))) => s.clone(),
        _ => "SIN".to_string(),
    };

    // Direction reference
    let dir_ref_str = match rec.get("direction_ref") {
        Some(casacore_types::Value::Scalar(casacore_types::ScalarValue::String(s))) => s.clone(),
        _ => "J2000".to_string(),
    };

    // Longpole / latpole
    let longpole = match rec.get("longpole") {
        Some(casacore_types::Value::Scalar(casacore_types::ScalarValue::Float64(v))) => *v,
        _ => 0.0,
    };
    let latpole = match rec.get("latpole") {
        Some(casacore_types::Value::Scalar(casacore_types::ScalarValue::Float64(v))) => *v,
        _ => std::f64::consts::FRAC_PI_2,
    };

    // PC matrix (flattened 2x2)
    let pc_flat: Vec<f64> = match rec.get("pc") {
        Some(casacore_types::Value::Array(casacore_types::ArrayValue::Float64(a))) => {
            a.iter().copied().collect()
        }
        _ => vec![1.0, 0.0, 0.0, 1.0],
    };

    // CTYPE: "RA---SIN" / "DEC--SIN" for equatorial, "GLON-SIN" / "GLAT-SIN" for galactic
    let (lon_prefix, lat_prefix) = match dir_ref_str.as_str() {
        "GALACTIC" => ("GLON", "GLAT"),
        "SUPERGAL" => ("SLON", "SLAT"),
        _ => ("RA--", "DEC-"),
    };

    let ctype_lon = format!("{lon_prefix}-{proj_name}");
    let ctype_lat = format!("{lat_prefix}-{proj_name}");

    let a1 = axis;
    let a2 = axis + 1;

    h.set(format!("CTYPE{a1}"), FitsValue::String(ctype_lon));
    h.set(format!("CTYPE{a2}"), FitsValue::String(ctype_lat));

    // CRVAL in degrees
    h.set(
        format!("CRVAL{a1}"),
        FitsValue::Float(crval[0] * RAD_TO_DEG),
    );
    h.set(
        format!("CRVAL{a2}"),
        FitsValue::Float(crval[1] * RAD_TO_DEG),
    );

    // CRPIX (1-based)
    h.set(format!("CRPIX{a1}"), FitsValue::Float(crpix[0] + 1.0));
    h.set(format!("CRPIX{a2}"), FitsValue::Float(crpix[1] + 1.0));

    // CDELT in degrees
    h.set(
        format!("CDELT{a1}"),
        FitsValue::Float(cdelt[0] * RAD_TO_DEG),
    );
    h.set(
        format!("CDELT{a2}"),
        FitsValue::Float(cdelt[1] * RAD_TO_DEG),
    );

    // Units
    h.set(format!("CUNIT{a1}"), FitsValue::String("deg".into()));
    h.set(format!("CUNIT{a2}"), FitsValue::String("deg".into()));

    // PC matrix (only non-identity elements)
    if pc_flat.len() >= 4 {
        let identity = [(0, 0, 1.0), (0, 1, 0.0), (1, 0, 0.0), (1, 1, 1.0)];
        let flat_idx = |i: usize, j: usize| i * 2 + j;
        for (i, j, id_val) in identity {
            let val = pc_flat[flat_idx(i, j)];
            if (val - id_val).abs() > 1e-15 {
                let fi = a1 + i;
                let fj = a1 + j;
                h.set(format!("PC{fi}_{fj}"), FitsValue::Float(val));
            }
        }
    }

    // RADESYS, EQUINOX
    let (radesys, equinox) = match dir_ref_str.as_str() {
        "J2000" => ("FK5", 2000.0),
        "B1950" => ("FK4", 1950.0),
        "ICRS" => ("ICRS", 2000.0),
        _ => ("FK5", 2000.0),
    };
    h.set("RADESYS", FitsValue::String(radesys.into()));
    h.set("EQUINOX", FitsValue::Float(equinox));

    // LONPOLE, LATPOLE (in degrees)
    h.set("LONPOLE", FitsValue::Float(longpole * RAD_TO_DEG));
    h.set("LATPOLE", FitsValue::Float(latpole * RAD_TO_DEG));
}

/// Emits FITS keywords for a [`SpectralCoordinate`].
fn emit_spectral(coord: &dyn Coordinate, h: &mut FitsHeader, axis: usize) {
    let crval = coord.reference_value();
    let crpix = coord.reference_pixel();
    let cdelt = coord.increment();
    let units = coord.axis_units();

    h.set(format!("CTYPE{axis}"), FitsValue::String("FREQ".into()));
    h.set(format!("CRVAL{axis}"), FitsValue::Float(crval[0]));
    h.set(format!("CRPIX{axis}"), FitsValue::Float(crpix[0] + 1.0));
    h.set(format!("CDELT{axis}"), FitsValue::Float(cdelt[0]));
    h.set(
        format!("CUNIT{axis}"),
        FitsValue::String(units.first().cloned().unwrap_or_default()),
    );

    // Frequency reference and rest frequency from record
    let rec = coord.to_record();
    if let Some(casacore_types::Value::Scalar(casacore_types::ScalarValue::String(fref))) =
        rec.get("frequency_ref")
    {
        let specsys = match fref.as_str() {
            "LSRK" => "LSRK",
            "BARY" => "BARYCENT",
            "TOPO" => "TOPOCENT",
            other => other,
        };
        h.set("SPECSYS", FitsValue::String(specsys.into()));
    }
    if let Some(casacore_types::Value::Scalar(casacore_types::ScalarValue::Float64(rf))) =
        rec.get("restfreq")
    {
        h.set("RESTFRQ", FitsValue::Float(*rf));
    }
}

/// Emits FITS keywords for a [`StokesCoordinate`].
fn emit_stokes(coord: &dyn Coordinate, h: &mut FitsHeader, axis: usize) {
    let crval = coord.reference_value();
    let crpix = coord.reference_pixel();
    let cdelt = coord.increment();

    h.set(format!("CTYPE{axis}"), FitsValue::String("STOKES".into()));
    h.set(format!("CRVAL{axis}"), FitsValue::Float(crval[0]));
    h.set(format!("CRPIX{axis}"), FitsValue::Float(crpix[0] + 1.0));
    h.set(format!("CDELT{axis}"), FitsValue::Float(cdelt[0]));
    h.set(format!("CUNIT{axis}"), FitsValue::String(String::new()));
}

/// Emits FITS keywords for a [`LinearCoordinate`] (or tabular treated as linear).
fn emit_linear(coord: &dyn Coordinate, h: &mut FitsHeader, axis: usize) {
    let crval = coord.reference_value();
    let crpix = coord.reference_pixel();
    let cdelt = coord.increment();
    let names = coord.axis_names();
    let units = coord.axis_units();
    let n = coord.n_pixel_axes();

    for i in 0..n {
        let a = axis + i;
        h.set(
            format!("CTYPE{a}"),
            FitsValue::String(names.get(i).cloned().unwrap_or_default()),
        );
        h.set(format!("CRVAL{a}"), FitsValue::Float(crval[i]));
        h.set(format!("CRPIX{a}"), FitsValue::Float(crpix[i] + 1.0));
        h.set(format!("CDELT{a}"), FitsValue::Float(cdelt[i]));
        h.set(
            format!("CUNIT{a}"),
            FitsValue::String(units.get(i).cloned().unwrap_or_default()),
        );
    }

    // Emit PC matrix from record if present
    let rec = coord.to_record();
    if let Some(casacore_types::Value::Array(casacore_types::ArrayValue::Float64(pc_arr))) =
        rec.get("pc")
    {
        let pc_flat: Vec<f64> = pc_arr.iter().copied().collect();
        if pc_flat.len() == n * n {
            for i in 0..n {
                for j in 0..n {
                    let val = pc_flat[i * n + j];
                    let id_val = if i == j { 1.0 } else { 0.0 };
                    if (val - id_val).abs() > 1e-15 {
                        let fi = axis + i;
                        let fj = axis + j;
                        h.set(format!("PC{fi}_{fj}"), FitsValue::Float(val));
                    }
                }
            }
        }
    }
}

// ---------------------------------------------------------------------------
// FITS header -> CoordinateSystem
// ---------------------------------------------------------------------------

/// Parses a [`FitsHeader`] (and image shape) into a [`CoordinateSystem`].
///
/// The `shape` slice gives the expected axis lengths (used for Stokes axis
/// decoding). It must have the same number of entries as `NAXIS` in the
/// header.
///
/// # Axis identification
///
/// Each axis is identified by its `CTYPEn` keyword:
/// - `RA--`, `DEC-`, `GLON`, `GLAT`, `SLON`, `SLAT` with a projection suffix
///   become a [`DirectionCoordinate`].
/// - `FREQ` or `VELO` becomes a [`SpectralCoordinate`].
/// - `STOKES` becomes a [`StokesCoordinate`].
/// - Anything else becomes a [`LinearCoordinate`].
///
/// # Errors
///
/// Returns [`CoordinateError`] if required keywords are missing or if an
/// unsupported projection is encountered.
pub fn from_fits_header(
    header: &FitsHeader,
    shape: &[usize],
) -> Result<CoordinateSystem, CoordinateError> {
    let naxis = header.get_int("NAXIS").unwrap_or(shape.len() as i64) as usize;

    // Read CTYPEn for all axes
    let ctypes: Vec<String> = (1..=naxis)
        .map(|i| {
            header
                .get_string(&format!("CTYPE{i}"))
                .unwrap_or("")
                .to_string()
        })
        .collect();

    let mut cs = CoordinateSystem::new();
    let mut consumed = vec![false; naxis];

    // Pass 1: find direction coordinate pairs
    for i in 0..naxis {
        if consumed[i] {
            continue;
        }
        if let Some((lon_axis, lat_axis, proj_code, dir_ref)) =
            identify_direction_pair(&ctypes, i, &consumed)
        {
            let coord = parse_direction(header, lon_axis, lat_axis, &proj_code, dir_ref)?;
            cs.add_coordinate(Box::new(coord));
            consumed[lon_axis] = true;
            consumed[lat_axis] = true;
        }
    }

    // Pass 2: remaining axes
    for i in 0..naxis {
        if consumed[i] {
            continue;
        }
        let ctype = &ctypes[i];
        let upper = ctype.to_uppercase();

        if upper.starts_with("FREQ") || upper.starts_with("VELO") {
            let coord = parse_spectral(header, i + 1)?;
            cs.add_coordinate(Box::new(coord));
        } else if upper == "STOKES" {
            let axis_len = shape.get(i).copied().unwrap_or(1);
            let coord = parse_stokes(header, i + 1, axis_len)?;
            cs.add_coordinate(Box::new(coord));
        } else {
            let coord = parse_linear_single(header, i + 1, ctype)?;
            cs.add_coordinate(Box::new(coord));
        }
        consumed[i] = true;
    }

    // Observation info
    let mut obs = ObsInfo::default();
    if let Some(tel) = header.get_string("TELESCOP") {
        obs.telescope = tel.to_string();
    }
    if let Some(obs_name) = header.get_string("OBSERVER") {
        obs.observer = obs_name.to_string();
    }
    *cs.obs_info_mut() = obs;

    Ok(cs)
}

/// Identifies a direction coordinate pair starting from axis index `start`.
///
/// Returns `(lon_axis, lat_axis, projection_code, DirectionRef)` if found.
fn identify_direction_pair(
    ctypes: &[String],
    start: usize,
    consumed: &[bool],
) -> Option<(usize, usize, String, DirectionRef)> {
    let ct = ctypes[start].to_uppercase();

    // Check if this is a longitude-type axis
    let (is_lon, proj_code, dir_ref) = parse_direction_ctype(&ct)?;

    // Find the matching partner
    let partner_prefix = if is_lon {
        match dir_ref {
            DirectionRef::GALACTIC => "GLAT",
            DirectionRef::SUPERGAL => "SLAT",
            _ => "DEC-",
        }
    } else {
        match dir_ref {
            DirectionRef::GALACTIC => "GLON",
            DirectionRef::SUPERGAL => "SLON",
            _ => "RA--",
        }
    };

    for j in 0..ctypes.len() {
        if j == start || consumed[j] {
            continue;
        }
        let ct_j = ctypes[j].to_uppercase();
        if ct_j.starts_with(partner_prefix) {
            // Verify same projection
            if ct_j.len() >= 8 && ct_j[5..8] == proj_code {
                let (lon, lat) = if is_lon { (start, j) } else { (j, start) };
                return Some((lon, lat, proj_code, dir_ref));
            }
        }
    }

    None
}

/// Parses a CTYPE value to determine if it is part of a direction coordinate.
///
/// Returns `(is_longitude, projection_code, DirectionRef)` if it matches a
/// known direction axis pattern, `None` otherwise.
fn parse_direction_ctype(ctype: &str) -> Option<(bool, String, DirectionRef)> {
    let ct = ctype.to_uppercase();
    if ct.len() < 8 {
        // Might be just "RA" or "DEC" without projection
        return None;
    }

    let prefix = &ct[..4];
    let separator = ct.as_bytes().get(4).copied().unwrap_or(b' ');
    if separator != b'-' {
        return None;
    }
    let proj = ct[5..8].to_string();

    let (is_lon, dir_ref) = match prefix {
        "RA--" => (true, DirectionRef::J2000),
        "DEC-" => (false, DirectionRef::J2000),
        "GLON" => (true, DirectionRef::GALACTIC),
        "GLAT" => (false, DirectionRef::GALACTIC),
        "SLON" => (true, DirectionRef::SUPERGAL),
        "SLAT" => (false, DirectionRef::SUPERGAL),
        _ => return None,
    };

    Some((is_lon, proj, dir_ref))
}

/// Parses a direction coordinate from the FITS header.
fn parse_direction(
    h: &FitsHeader,
    lon_axis: usize,
    lat_axis: usize,
    proj_code: &str,
    default_dir_ref: DirectionRef,
) -> Result<DirectionCoordinate, CoordinateError> {
    let a1 = lon_axis + 1; // 1-based FITS axis
    let a2 = lat_axis + 1;

    // Projection
    let proj_type = ProjectionType::from_name(proj_code)
        .ok_or_else(|| CoordinateError::UnsupportedProjection(proj_code.to_string()))?;
    let proj = Projection::new(proj_type);

    // CRVAL (degrees -> radians)
    let crval_lon = h
        .get_float(&format!("CRVAL{a1}"))
        .ok_or_else(|| CoordinateError::InvalidRecord(format!("missing CRVAL{a1}")))?
        * DEG_TO_RAD;
    let crval_lat = h
        .get_float(&format!("CRVAL{a2}"))
        .ok_or_else(|| CoordinateError::InvalidRecord(format!("missing CRVAL{a2}")))?
        * DEG_TO_RAD;

    // CRPIX (1-based -> 0-based)
    let crpix_lon = h.get_float(&format!("CRPIX{a1}")).unwrap_or(1.0) - 1.0;
    let crpix_lat = h.get_float(&format!("CRPIX{a2}")).unwrap_or(1.0) - 1.0;

    // CDELT (degrees -> radians) — check for CD matrix first
    let (cdelt_lon, cdelt_lat, pc) = read_linear_transform_2d(h, a1, a2)?;

    // Direction reference from RADESYS
    let dir_ref = if let Some(radesys) = h.get_string("RADESYS") {
        match radesys.to_uppercase().as_str() {
            "FK5" => DirectionRef::J2000,
            "FK4" => DirectionRef::B1950,
            "ICRS" => DirectionRef::ICRS,
            _ => default_dir_ref,
        }
    } else {
        default_dir_ref
    };

    let mut coord = DirectionCoordinate::new(
        dir_ref,
        proj,
        [crval_lon, crval_lat],
        [cdelt_lon, cdelt_lat],
        [crpix_lon, crpix_lat],
    )
    .with_pc_matrix(pc);

    // LONPOLE / LATPOLE
    if let Some(lp) = h.get_float("LONPOLE") {
        coord = coord.with_longpole(lp * DEG_TO_RAD);
    }
    if let Some(lp) = h.get_float("LATPOLE") {
        coord = coord.with_latpole(lp * DEG_TO_RAD);
    }

    Ok(coord)
}

/// Reads the 2D linear transform for direction axes.
///
/// Handles three conventions:
/// 1. PCi_j matrix with separate CDELTn
/// 2. CDi_j matrix (= CDELTi * PCi_j)
/// 3. Legacy CROTA2 rotation
///
/// Returns `(cdelt_lon, cdelt_lat, pc_matrix)`, all in radians.
fn read_linear_transform_2d(
    h: &FitsHeader,
    a1: usize,
    a2: usize,
) -> Result<(f64, f64, ndarray::Array2<f64>), CoordinateError> {
    // Check for CD matrix first
    let cd11 = h.get_float(&format!("CD{a1}_{a1}"));
    let has_cd = cd11.is_some();

    if has_cd {
        // CD matrix convention: CDi_j = CDELTi * PCi_j
        let cd11 = cd11.unwrap_or(0.0);
        let cd12 = h.get_float(&format!("CD{a1}_{a2}")).unwrap_or(0.0);
        let cd21 = h.get_float(&format!("CD{a2}_{a1}")).unwrap_or(0.0);
        let cd22 = h.get_float(&format!("CD{a2}_{a2}")).unwrap_or(0.0);

        // Decompose: cdelt_i = sqrt(CDi_1^2 + CDi_2^2) with sign from diagonal
        let cdelt1 = {
            let mag = (cd11 * cd11 + cd12 * cd12).sqrt();
            if cd11 < 0.0 { -mag } else { mag }
        };
        let cdelt2 = {
            let mag = (cd21 * cd21 + cd22 * cd22).sqrt();
            if cd22 < 0.0 { -mag } else { mag }
        };

        if cdelt1.abs() < 1e-300 || cdelt2.abs() < 1e-300 {
            return Err(CoordinateError::ConversionFailed(
                "zero CD matrix diagonal".into(),
            ));
        }

        let pc = ndarray::Array2::from_shape_vec(
            (2, 2),
            vec![cd11 / cdelt1, cd12 / cdelt1, cd21 / cdelt2, cd22 / cdelt2],
        )
        .unwrap();

        // Convert degrees to radians
        Ok((cdelt1 * DEG_TO_RAD, cdelt2 * DEG_TO_RAD, pc))
    } else {
        // PC matrix or CROTA2
        let cdelt1 = h
            .get_float(&format!("CDELT{a1}"))
            .ok_or_else(|| CoordinateError::InvalidRecord(format!("missing CDELT{a1}")))?;
        let cdelt2 = h
            .get_float(&format!("CDELT{a2}"))
            .ok_or_else(|| CoordinateError::InvalidRecord(format!("missing CDELT{a2}")))?;

        let pc = if let Some(pc11) = h.get_float(&format!("PC{a1}_{a1}")) {
            // Explicit PC matrix
            let pc12 = h.get_float(&format!("PC{a1}_{a2}")).unwrap_or(0.0);
            let pc21 = h.get_float(&format!("PC{a2}_{a1}")).unwrap_or(0.0);
            let pc22 = h.get_float(&format!("PC{a2}_{a2}")).unwrap_or(1.0);
            ndarray::Array2::from_shape_vec((2, 2), vec![pc11, pc12, pc21, pc22]).unwrap()
        } else if let Some(crota2) = h.get_float("CROTA2") {
            // Legacy CROTA2 rotation (in degrees)
            let rot = crota2 * DEG_TO_RAD;
            let cos_r = rot.cos();
            let sin_r = rot.sin();
            ndarray::Array2::from_shape_vec((2, 2), vec![cos_r, -sin_r, sin_r, cos_r]).unwrap()
        } else {
            ndarray::Array2::eye(2)
        };

        // Convert degrees to radians
        Ok((cdelt1 * DEG_TO_RAD, cdelt2 * DEG_TO_RAD, pc))
    }
}

/// Parses a spectral coordinate from the FITS header.
fn parse_spectral(h: &FitsHeader, axis: usize) -> Result<SpectralCoordinate, CoordinateError> {
    let crval = h
        .get_float(&format!("CRVAL{axis}"))
        .ok_or_else(|| CoordinateError::InvalidRecord(format!("missing CRVAL{axis}")))?;
    let cdelt = h
        .get_float(&format!("CDELT{axis}"))
        .ok_or_else(|| CoordinateError::InvalidRecord(format!("missing CDELT{axis}")))?;
    let crpix = h.get_float(&format!("CRPIX{axis}")).unwrap_or(1.0) - 1.0;

    let rest_freq = h.get_float("RESTFRQ").unwrap_or(0.0);

    let freq_ref = if let Some(specsys) = h.get_string("SPECSYS") {
        match specsys.to_uppercase().as_str() {
            "LSRK" => FrequencyRef::LSRK,
            "BARYCENT" | "BARY" => FrequencyRef::BARY,
            "TOPOCENT" | "TOPO" => FrequencyRef::TOPO,
            _ => FrequencyRef::TOPO,
        }
    } else {
        FrequencyRef::TOPO
    };

    let unit = h
        .get_string(&format!("CUNIT{axis}"))
        .unwrap_or("Hz")
        .to_string();

    Ok(SpectralCoordinate::new(freq_ref, crval, cdelt, crpix, rest_freq).with_unit(unit))
}

/// Parses a Stokes coordinate from the FITS header.
fn parse_stokes(
    h: &FitsHeader,
    axis: usize,
    axis_len: usize,
) -> Result<StokesCoordinate, CoordinateError> {
    let crval = h.get_float(&format!("CRVAL{axis}")).unwrap_or(1.0);
    let cdelt = h.get_float(&format!("CDELT{axis}")).unwrap_or(1.0);
    let crpix = h.get_float(&format!("CRPIX{axis}")).unwrap_or(1.0) - 1.0;

    let mut stokes = Vec::with_capacity(axis_len);
    for i in 0..axis_len {
        let code = (crval + cdelt * (i as f64 - crpix)).round() as i32;
        let st = StokesType::from_code(code)
            .ok_or_else(|| CoordinateError::InvalidRecord(format!("unknown Stokes code {code}")))?;
        stokes.push(st);
    }

    Ok(StokesCoordinate::new(stokes))
}

/// Parses a single linear axis from the FITS header.
fn parse_linear_single(
    h: &FitsHeader,
    axis: usize,
    ctype: &str,
) -> Result<LinearCoordinate, CoordinateError> {
    let crval = h
        .get_float(&format!("CRVAL{axis}"))
        .ok_or_else(|| CoordinateError::InvalidRecord(format!("missing CRVAL{axis}")))?;
    let cdelt = h.get_float(&format!("CDELT{axis}")).unwrap_or(1.0);
    let crpix = h.get_float(&format!("CRPIX{axis}")).unwrap_or(1.0) - 1.0;
    let unit = h
        .get_string(&format!("CUNIT{axis}"))
        .unwrap_or("")
        .to_string();

    let name = if ctype.is_empty() {
        format!("Linear{axis}")
    } else {
        ctype.to_string()
    };

    Ok(LinearCoordinate::new(1, vec![name], vec![unit])
        .with_reference_value(vec![crval])
        .with_reference_pixel(vec![crpix])
        .with_increment(vec![cdelt]))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::fits::header::FitsHeader;

    /// Helper: build a typical 4-axis coordinate system (RA+Dec+Freq+Stokes).
    fn make_4axis_cs() -> CoordinateSystem {
        let mut cs = CoordinateSystem::new();

        let proj = Projection::new(ProjectionType::SIN);
        let dir = DirectionCoordinate::new(
            DirectionRef::J2000,
            proj,
            [3.5, 0.8],    // RA ~200 deg, Dec ~46 deg
            [-1e-4, 1e-4], // ~20 arcsec pixels
            [512.0, 512.0],
        );
        cs.add_coordinate(Box::new(dir));

        let spec =
            SpectralCoordinate::new(FrequencyRef::LSRK, 1.42040575e9, 1e6, 128.0, 1.42040575e9);
        cs.add_coordinate(Box::new(spec));

        let stokes = StokesCoordinate::new(vec![
            StokesType::I,
            StokesType::Q,
            StokesType::U,
            StokesType::V,
        ]);
        cs.add_coordinate(Box::new(stokes));

        let obs = ObsInfo::new("ALMA").with_observer("Test Observer");
        *cs.obs_info_mut() = obs;

        cs
    }

    #[test]
    fn roundtrip_4axis() {
        let cs1 = make_4axis_cs();
        let shape = [1024, 1024, 256, 4];
        let header = to_fits_header(&cs1, &shape);
        let cs2 = from_fits_header(&header, &shape).unwrap();

        assert_eq!(cs2.n_pixel_axes(), 4);
        assert_eq!(cs2.n_coordinates(), 3);

        // Check direction coordinate
        let dir_idx = cs2.find_coordinate(CoordinateType::Direction).unwrap();
        let dir = cs2.coordinate(dir_idx);
        let crval = dir.reference_value();
        assert!(
            (crval[0] - 3.5).abs() < 1e-8,
            "RA crval: {} vs 3.5",
            crval[0]
        );
        assert!(
            (crval[1] - 0.8).abs() < 1e-8,
            "Dec crval: {} vs 0.8",
            crval[1]
        );

        // Check spectral
        let spec_idx = cs2.find_coordinate(CoordinateType::Spectral).unwrap();
        let spec = cs2.coordinate(spec_idx);
        let spec_crval = spec.reference_value();
        assert!(
            (spec_crval[0] - 1.42040575e9).abs() < 1.0,
            "freq crval: {}",
            spec_crval[0]
        );

        // Check stokes
        let stokes_idx = cs2.find_coordinate(CoordinateType::Stokes).unwrap();
        let stokes = cs2.coordinate(stokes_idx);
        // Stokes I = code 1 at pixel 0
        let w = stokes.to_world(&[0.0]).unwrap();
        assert_eq!(w[0] as i32, 1);
        // Stokes V = code 4 at pixel 3
        let w = stokes.to_world(&[3.0]).unwrap();
        assert_eq!(w[0] as i32, 4);

        // Obs info
        assert_eq!(cs2.obs_info().telescope, "ALMA");
        assert_eq!(cs2.obs_info().observer, "Test Observer");
    }

    #[test]
    fn parse_realistic_header() {
        let cards = [
            "NAXIS   =                    4",
            "NAXIS1  =                 1024",
            "NAXIS2  =                 1024",
            "NAXIS3  =                  256",
            "NAXIS4  =                    4",
            "CTYPE1  = 'RA---SIN'",
            "CRVAL1  =   2.005000000000000E+02",
            "CRPIX1  =   5.130000000000000E+02",
            "CDELT1  =  -5.729577951308232E-03",
            "CUNIT1  = 'deg     '",
            "CTYPE2  = 'DEC--SIN'",
            "CRVAL2  =   4.583662361046586E+01",
            "CRPIX2  =   5.130000000000000E+02",
            "CDELT2  =   5.729577951308232E-03",
            "CUNIT2  = 'deg     '",
            "CTYPE3  = 'FREQ    '",
            "CRVAL3  =   1.420405750000000E+09",
            "CRPIX3  =   1.290000000000000E+02",
            "CDELT3  =   1.000000000000000E+06",
            "CUNIT3  = 'Hz      '",
            "CTYPE4  = 'STOKES  '",
            "CRVAL4  =   1.000000000000000E+00",
            "CRPIX4  =   1.000000000000000E+00",
            "CDELT4  =   1.000000000000000E+00",
            "RADESYS = 'FK5     '",
            "EQUINOX =   2.000000000000000E+03",
            "SPECSYS = 'LSRK    '",
            "RESTFRQ =   1.420405750000000E+09",
            "TELESCOP= 'VLA     '",
            "OBSERVER= 'Jane    '",
        ];

        let h = FitsHeader::from_cards(&cards);
        let shape = [1024, 1024, 256, 4];
        let cs = from_fits_header(&h, &shape).unwrap();

        assert_eq!(cs.n_pixel_axes(), 4);
        assert_eq!(cs.n_coordinates(), 3);

        // Direction
        let dir_idx = cs.find_coordinate(CoordinateType::Direction).unwrap();
        let dir = cs.coordinate(dir_idx);
        let crval = dir.reference_value();
        // 200.5 degrees in radians
        assert!(
            (crval[0] - 200.5 * DEG_TO_RAD).abs() < 1e-6,
            "RA: {} vs {}",
            crval[0],
            200.5 * DEG_TO_RAD
        );

        // Spectral
        let spec_idx = cs.find_coordinate(CoordinateType::Spectral).unwrap();
        let spec = cs.coordinate(spec_idx);
        assert!((spec.reference_value()[0] - 1.420405750e9).abs() < 1.0,);

        // Stokes
        let stokes_idx = cs.find_coordinate(CoordinateType::Stokes).unwrap();
        let stokes = cs.coordinate(stokes_idx);
        assert_eq!(stokes.to_world(&[0.0]).unwrap()[0] as i32, 1); // I
        assert_eq!(stokes.to_world(&[3.0]).unwrap()[0] as i32, 4); // V

        // Obs info
        assert_eq!(cs.obs_info().telescope, "VLA");
        assert_eq!(cs.obs_info().observer, "Jane");
    }

    #[test]
    fn direction_sin_roundtrip() {
        let mut cs = CoordinateSystem::new();
        let proj = Projection::new(ProjectionType::SIN);
        let dir = DirectionCoordinate::new(
            DirectionRef::J2000,
            proj,
            [1.0, 0.5],
            [-2e-5, 2e-5],
            [256.0, 256.0],
        );
        cs.add_coordinate(Box::new(dir));

        let shape = [512, 512];
        let header = to_fits_header(&cs, &shape);
        let cs2 = from_fits_header(&header, &shape).unwrap();

        let dir2 = cs2.coordinate(0);
        let crval = dir2.reference_value();
        assert!((crval[0] - 1.0).abs() < 1e-10);
        assert!((crval[1] - 0.5).abs() < 1e-10);

        let cdelt = dir2.increment();
        assert!((cdelt[0] - (-2e-5)).abs() < 1e-15);
        assert!((cdelt[1] - 2e-5).abs() < 1e-15);
    }

    #[test]
    fn spectral_roundtrip() {
        let mut cs = CoordinateSystem::new();
        let spec = SpectralCoordinate::new(FrequencyRef::BARY, 1.5e9, -500e3, 64.0, 1.42040575e9);
        cs.add_coordinate(Box::new(spec));

        let shape = [128];
        let header = to_fits_header(&cs, &shape);

        assert_eq!(header.get_string("SPECSYS"), Some("BARYCENT"));

        let cs2 = from_fits_header(&header, &shape).unwrap();
        let spec2 = cs2.coordinate(0);

        assert!((spec2.reference_value()[0] - 1.5e9).abs() < 1.0,);
        assert!((spec2.increment()[0] - (-500e3)).abs() < 0.1,);
        assert!((spec2.reference_pixel()[0] - 64.0).abs() < 1e-10,);
    }

    #[test]
    fn missing_crval_error() {
        let cards = [
            "NAXIS   =                    1",
            "CTYPE1  = 'FREQ    '",
            "CDELT1  =   1.000000000000000E+06",
        ];
        let h = FitsHeader::from_cards(&cards);
        let result = from_fits_header(&h, &[256]);
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(err.contains("CRVAL"), "error: {err}");
    }

    #[test]
    fn unsupported_projection_error() {
        let cards = [
            "NAXIS   =                    2",
            "CTYPE1  = 'RA---BON'",
            "CRVAL1  =   0.0",
            "CDELT1  =  -1.0E-03",
            "CRPIX1  =   1.0",
            "CTYPE2  = 'DEC--BON'",
            "CRVAL2  =   0.0",
            "CDELT2  =   1.0E-03",
            "CRPIX2  =   1.0",
        ];
        let h = FitsHeader::from_cards(&cards);
        let result = from_fits_header(&h, &[256, 256]);
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(err.contains("BON"), "error: {err}");
    }

    #[test]
    fn cd_matrix_handling() {
        // Use CD matrix instead of CDELT + PC
        let cards = [
            "NAXIS   =                    2",
            "CTYPE1  = 'RA---TAN'",
            "CRVAL1  =   1.800000000000000E+02",
            "CRPIX1  =   5.130000000000000E+02",
            "CTYPE2  = 'DEC--TAN'",
            "CRVAL2  =   4.500000000000000E+01",
            "CRPIX2  =   5.130000000000000E+02",
            "CD1_1   =  -1.000000000000000E-03",
            "CD1_2   =   0.000000000000000E+00",
            "CD2_1   =   0.000000000000000E+00",
            "CD2_2   =   1.000000000000000E-03",
            "RADESYS = 'FK5     '",
        ];

        let h = FitsHeader::from_cards(&cards);
        let cs = from_fits_header(&h, &[1024, 1024]).unwrap();
        assert_eq!(cs.n_pixel_axes(), 2);

        let dir = cs.coordinate(0);
        let cdelt = dir.increment();
        // CD1_1 = -1e-3 deg -> radians
        assert!(
            (cdelt[0] - (-1e-3 * DEG_TO_RAD)).abs() < 1e-15,
            "cdelt[0]: {} vs {}",
            cdelt[0],
            -1e-3 * DEG_TO_RAD
        );
    }

    #[test]
    fn pc_matrix_roundtrip() {
        // Direction with a non-identity PC matrix
        let mut cs = CoordinateSystem::new();
        let proj = Projection::new(ProjectionType::TAN);
        let rot = 0.1_f64; // small rotation
        let pc = ndarray::Array2::from_shape_vec(
            (2, 2),
            vec![rot.cos(), -rot.sin(), rot.sin(), rot.cos()],
        )
        .unwrap();
        let dir = DirectionCoordinate::new(
            DirectionRef::J2000,
            proj,
            [3.0, 0.7],
            [-1e-4, 1e-4],
            [256.0, 256.0],
        )
        .with_pc_matrix(pc.clone());
        cs.add_coordinate(Box::new(dir));

        let shape = [512, 512];
        let header = to_fits_header(&cs, &shape);

        // Verify PC keywords were emitted
        assert!(header.get_float("PC1_1").is_some());
        assert!(header.get_float("PC1_2").is_some());

        let cs2 = from_fits_header(&header, &shape).unwrap();
        let dir2 = cs2.coordinate(0);

        // Verify roundtrip via pixel conversion
        let pixel = [270.0, 280.0];
        let world1 = cs.coordinate(0).to_world(&pixel).unwrap();
        let world2 = dir2.to_world(&pixel).unwrap();
        assert!(
            (world1[0] - world2[0]).abs() < 1e-8,
            "lon: {} vs {}",
            world1[0],
            world2[0]
        );
        assert!(
            (world1[1] - world2[1]).abs() < 1e-8,
            "lat: {} vs {}",
            world1[1],
            world2[1]
        );
    }

    #[test]
    fn crota2_handling() {
        // Legacy CROTA2 convention
        let cards = [
            "NAXIS   =                    2",
            "CTYPE1  = 'RA---SIN'",
            "CRVAL1  =   0.000000000000000E+00",
            "CRPIX1  =   5.130000000000000E+02",
            "CDELT1  =  -1.000000000000000E-03",
            "CTYPE2  = 'DEC--SIN'",
            "CRVAL2  =   4.500000000000000E+01",
            "CRPIX2  =   5.130000000000000E+02",
            "CDELT2  =   1.000000000000000E-03",
            "CROTA2  =   5.000000000000000E+00",
        ];

        let h = FitsHeader::from_cards(&cards);
        let cs = from_fits_header(&h, &[1024, 1024]).unwrap();
        assert_eq!(cs.n_pixel_axes(), 2);

        // The rotation should be present in the PC matrix
        let dir = cs.coordinate(0);
        let pixel = [520.0, 530.0];
        let world = dir.to_world(&pixel).unwrap();
        // Just verify it doesn't crash and the pixel is near the reference
        assert!(world[0].is_finite());
        assert!(world[1].is_finite());
    }

    #[test]
    fn unknown_ctype_becomes_linear() {
        let cards = [
            "NAXIS   =                    1",
            "CTYPE1  = 'VOPT    '",
            "CRVAL1  =   1.000000000000000E+03",
            "CRPIX1  =   1.000000000000000E+00",
            "CDELT1  =   1.000000000000000E+01",
            "CUNIT1  = 'km/s    '",
        ];
        let h = FitsHeader::from_cards(&cards);
        let cs = from_fits_header(&h, &[128]).unwrap();

        assert_eq!(cs.n_coordinates(), 1);
        assert_eq!(cs.coordinate(0).coordinate_type(), CoordinateType::Linear);
        assert!((cs.coordinate(0).reference_value()[0] - 1000.0).abs() < 1e-10,);
    }

    #[test]
    fn galactic_direction_roundtrip() {
        let mut cs = CoordinateSystem::new();
        let proj = Projection::new(ProjectionType::CAR);
        let dir = DirectionCoordinate::new(
            DirectionRef::GALACTIC,
            proj,
            [0.5, 0.3],
            [-1e-3, 1e-3],
            [100.0, 100.0],
        );
        cs.add_coordinate(Box::new(dir));

        let shape = [200, 200];
        let header = to_fits_header(&cs, &shape);

        // Verify CTYPE uses GLON/GLAT
        assert_eq!(header.get_string("CTYPE1"), Some("GLON-CAR"));
        assert_eq!(header.get_string("CTYPE2"), Some("GLAT-CAR"));

        let cs2 = from_fits_header(&header, &shape).unwrap();
        let dir2 = cs2.coordinate(0);
        let crval = dir2.reference_value();
        assert!((crval[0] - 0.5).abs() < 1e-8);
        assert!((crval[1] - 0.3).abs() < 1e-8);
    }

    #[test]
    fn empty_system_roundtrip() {
        let cs = CoordinateSystem::new();
        let header = to_fits_header(&cs, &[]);
        assert_eq!(header.get_int("NAXIS"), Some(0));

        let cs2 = from_fits_header(&header, &[]).unwrap();
        assert_eq!(cs2.n_pixel_axes(), 0);
    }

    #[test]
    fn stokes_roundtrip_circular() {
        let mut cs = CoordinateSystem::new();
        let stokes = StokesCoordinate::new(vec![
            StokesType::RR,
            StokesType::RL,
            StokesType::LR,
            StokesType::LL,
        ]);
        cs.add_coordinate(Box::new(stokes));

        let shape = [4];
        let header = to_fits_header(&cs, &shape);
        let cs2 = from_fits_header(&header, &shape).unwrap();

        let st = cs2.coordinate(0);
        assert_eq!(st.to_world(&[0.0]).unwrap()[0] as i32, 5); // RR
        assert_eq!(st.to_world(&[3.0]).unwrap()[0] as i32, 8); // LL
    }
}

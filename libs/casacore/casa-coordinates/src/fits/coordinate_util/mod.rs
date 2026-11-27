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
//! | `TELESCOP`, `OBSERVER` | Observation metadata |
//! | `DATE-OBS`, `TIMESYS` (`MJD-OBS` read first) | Observation date and its time scale |
//!
//! Corresponds to C++ `FITSCoordinateUtil`.

use std::f64::consts::PI;

use casa_core::measures::direction::DirectionRef;
use casa_core::measures::frequency::FrequencyRef;

use super::header::{FitsHeader, FitsValue};
use super::obs_date::{fits_date, obs_epoch};
use crate::CoordinateSystem;
use crate::coordinate::CoordinateModel;
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
/// Panics if `shape.len()` does not match `cs.n_pixel_axes()` or a spectral
/// coordinate cannot be converted to FITS frequency units. Use
/// [`try_to_fits_header`] to handle a spectral conversion failure.
pub fn to_fits_header(cs: &CoordinateSystem, shape: &[usize]) -> FitsHeader {
    try_to_fits_header(cs, shape).expect("coordinate system cannot be represented as FITS")
}

/// Convert a coordinate system to FITS WCS.
///
/// # Errors
///
/// Returns an error if a spectral unit or reference-frame conversion cannot
/// be expressed in Hz, or a direction coordinate has a removed axis.
///
/// # Panics
///
/// Panics if `shape.len()` does not match `cs.n_pixel_axes()`.
pub fn try_to_fits_header(
    cs: &CoordinateSystem,
    shape: &[usize],
) -> Result<FitsHeader, CoordinateError> {
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

    for index in 0..cs.n_coordinates() {
        let model = cs.coordinate(index);
        let pixel = cs
            .coordinate_pixel_axes(index)
            .expect("existing coordinate");
        if pixel
            .iter()
            .zip(
                cs.coordinate_world_axes(index)
                    .expect("existing coordinate"),
            )
            .any(|(pixel, world)| pixel.is_some() != world.is_some())
        {
            return Err(CoordinateError::UnsupportedAxisLayout(
                "FITS export requires a world axis for each visible pixel axis".into(),
            ));
        }
        let axes: Vec<_> = pixel.iter().flatten().map(|axis| axis + 1).collect();
        match (model, axes.as_slice()) {
            (CoordinateModel::Direction(_), [a1, a2]) => emit_direction(model, &mut h, [*a1, *a2]),
            (CoordinateModel::Direction(_), _) => {
                return Err(CoordinateError::UnsupportedAxisLayout(
                    "FITS export requires both celestial pixel axes".into(),
                ));
            }
            (CoordinateModel::Spectral(_), [axis]) => emit_spectral(model, &mut h, *axis)?,
            (CoordinateModel::Stokes(_), [axis]) => emit_stokes(model, &mut h, *axis),
            (CoordinateModel::Quality(_), _) => {
                return Err(CoordinateError::UnsupportedAxisLayout(
                    "FITS export cannot represent a native Quality coordinate".into(),
                ));
            }
            (_, []) => continue,
            (CoordinateModel::Linear(linear), _) if axes.len() != pixel.len() => {
                let kept: Vec<_> = pixel
                    .iter()
                    .enumerate()
                    .filter_map(|(local, axis)| axis.map(|_| local))
                    .collect();
                let reduced = linear.kept_axes(&kept).ok_or_else(|| CoordinateError::UnsupportedAxisLayout(
                    "FITS export cannot represent a coupled linear coordinate with a removed axis".into(),
                ))?;
                emit_linear(&CoordinateModel::Linear(reduced), &mut h, &axes);
            }
            _ => emit_linear(model, &mut h, &axes),
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
        let (date, timesys) = fits_date(epoch);
        h.set("DATE-OBS", FitsValue::String(date));
        h.set("TIMESYS", FitsValue::String(timesys));
    }

    let pixel_to_world = (0..naxis)
        .map(|pixel| cs.pixel_axis_to_world_axis(pixel).map(|axis| axis + 1))
        .collect::<Option<Vec<_>>>()
        .ok_or_else(|| {
            CoordinateError::UnsupportedAxisLayout(
                "FITS export requires a world axis for each visible pixel axis".into(),
            )
        })?;
    if pixel_to_world.iter().copied().eq(1..=naxis) {
        return Ok(h);
    }
    Ok(permute_fits_world_rows(&h, &pixel_to_world))
}

// The emitters build WCS in pixel order. FITS stores CRPIX by pixel column;
// CTYPE, CRVAL, CDELT, CUNIT, PV and the rows of PC are world ordered.
fn permute_fits_world_rows(header: &FitsHeader, output_row: &[usize]) -> FitsHeader {
    let mut result = FitsHeader::new();
    for keyword in header.iter() {
        if keyword.name.strip_prefix("PC").is_some_and(|suffix| {
            suffix.split_once('_').is_some_and(|(row, col)| {
                row.parse::<usize>().is_ok() && col.parse::<usize>().is_ok()
            })
        }) {
            continue;
        }
        let mut name = keyword.name.clone();
        for prefix in ["CTYPE", "CRVAL", "CDELT", "CUNIT", "PV"] {
            if let Some(suffix) = keyword.name.strip_prefix(prefix) {
                let (axis, parameter) = suffix
                    .split_once('_')
                    .map_or((suffix, None), |(axis, parameter)| (axis, Some(parameter)));
                if let Ok(axis) = axis.parse::<usize>()
                    && let Some(&axis) = axis.checked_sub(1).and_then(|i| output_row.get(i))
                {
                    name = format!("{prefix}{axis}");
                    if let Some(parameter) = parameter {
                        name.push('_');
                        name.push_str(parameter);
                    }
                }
                break;
            }
        }
        result.set(name, keyword.value.clone());
    }
    for (old_row, &world) in output_row.iter().enumerate() {
        for pixel in 1..=output_row.len() {
            let value = header
                .get_float(&format!("PC{}_{pixel}", old_row + 1))
                .unwrap_or(f64::from(old_row + 1 == pixel));
            if value != f64::from(world == pixel) {
                result.set(format!("PC{world}_{pixel}"), FitsValue::Float(value));
            }
        }
    }
    result
}

/// Align FITS PC world rows with pixel columns for parsing the local models.
/// The accompanying vector gives the original world axis of each aligned
/// row. Headers whose original diagonal already pairs the axes need no edit.
pub fn align_fits_pc_rows(header: &FitsHeader, rank: usize) -> Option<(FitsHeader, Vec<usize>)> {
    if header
        .get_int("NAXIS")
        .is_some_and(|axes| axes != rank as i64)
    {
        return None;
    }
    let pc = |row, pixel| {
        header
            .get_float(&format!("PC{}_{pixel}", row + 1))
            .unwrap_or(f64::from(row + 1 == pixel))
    };
    if (0..rank).all(|i| pc(i, i + 1) != 0.0)
        || header.iter().any(|keyword| {
            keyword
                .name
                .strip_prefix("CD")
                .is_some_and(|suffix| suffix.contains('_'))
        })
    {
        return None;
    }
    let choices: Vec<Vec<_>> = (0..rank)
        .map(|row| {
            let mut columns: Vec<_> = (0..rank)
                .filter(|&column| pc(row, column + 1) != 0.0)
                .collect();
            columns.sort_by_key(|&column| column != row);
            columns
        })
        .collect();
    let mut rows: Vec<_> = (0..rank).collect();
    rows.sort_by_key(|&row| choices[row].len());
    let mut world_axes = vec![None; rank];
    for row in rows {
        if !assign_fits_pixel_column(row, &choices, &mut world_axes, &mut vec![false; rank]) {
            return None;
        }
    }
    let world_axes: Vec<_> = world_axes.into_iter().collect::<Option<_>>()?;
    let mut aligned_row = vec![0; rank];
    for (pixel, &world) in world_axes.iter().enumerate() {
        aligned_row[world] = pixel + 1;
    }
    Some((permute_fits_world_rows(header, &aligned_row), world_axes))
}

fn assign_fits_pixel_column(
    row: usize,
    choices: &[Vec<usize>],
    world_axes: &mut [Option<usize>],
    seen: &mut [bool],
) -> bool {
    for &column in &choices[row] {
        if std::mem::replace(&mut seen[column], true) {
            continue;
        }
        if world_axes[column]
            .is_none_or(|other| assign_fits_pixel_column(other, choices, world_axes, seen))
        {
            world_axes[column] = Some(row);
            return true;
        }
    }
    false
}

/// Emits FITS keywords for a [`DirectionCoordinate`].
fn emit_direction(coord: &CoordinateModel, h: &mut FitsHeader, [a1, a2]: [usize; 2]) {
    let CoordinateModel::Direction(direction) = coord else {
        return;
    };
    let crval = coord.reference_value();
    let crpix = coord.reference_pixel();
    let cdelt = coord.increment();
    let proj_name = direction.projection().name();
    let direction_ref = direction.direction_ref();
    let longpole = direction.longpole();
    let latpole = direction.latpole();
    let pc_flat: Vec<f64> = direction.pc_matrix().iter().copied().collect();

    // CTYPE: "RA---SIN" / "DEC--SIN" for equatorial, "GLON-SIN" / "GLAT-SIN" for galactic
    let (lon_prefix, lat_prefix) = match direction_ref {
        DirectionRef::GALACTIC => ("GLON", "GLAT"),
        DirectionRef::SUPERGAL => ("SLON", "SLAT"),
        DirectionRef::ECLIPTIC => ("ELON", "ELAT"),
        _ => ("RA--", "DEC-"),
    };

    let ctype_lon = format!("{lon_prefix}-{proj_name}");
    let ctype_lat = format!("{lat_prefix}-{proj_name}");

    let axes = [a1, a2];

    h.set(format!("CTYPE{a1}"), FitsValue::String(ctype_lon));
    h.set(format!("CTYPE{a2}"), FitsValue::String(ctype_lat));
    let projection = direction.projection();
    let projection_type = projection.projection_type();
    let start = projection_type.fits_parameter_start();
    let defaults = projection_type.fits_parameter_defaults();
    let count = if projection_type == ProjectionType::ZPN {
        projection.parameters().len().min(defaults.len())
    } else {
        defaults.len()
    };
    for (index, &default) in defaults[..count].iter().enumerate() {
        let value = projection
            .parameters()
            .get(index)
            .copied()
            .unwrap_or(default);
        h.set(format!("PV{a2}_{}", index + start), FitsValue::Float(value));
    }

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
                let fi = axes[i];
                let fj = axes[j];
                h.set(format!("PC{fi}_{fj}"), FitsValue::Float(val));
            }
        }
    }

    // RADESYS, EQUINOX
    match direction_ref {
        DirectionRef::J2000 => {
            h.set("RADESYS", FitsValue::String("FK5".into()));
            h.set("EQUINOX", FitsValue::Float(2000.0));
        }
        DirectionRef::B1950 | DirectionRef::B1950Vla => {
            let equinox = if direction_ref == DirectionRef::B1950Vla {
                VLA_BESSELIAN_EQUINOX
            } else {
                1950.0
            };
            h.set("RADESYS", FitsValue::String("FK4".into()));
            h.set("EQUINOX", FitsValue::Float(equinox));
        }
        DirectionRef::ICRS => h.set("RADESYS", FitsValue::String("ICRS".into())),
        DirectionRef::GALACTIC | DirectionRef::SUPERGAL | DirectionRef::ECLIPTIC => {}
        _ => {
            h.set("RADESYS", FitsValue::String("FK5".into()));
            h.set("EQUINOX", FitsValue::Float(2000.0));
        }
    }

    // LONPOLE, LATPOLE (in degrees)
    h.set("LONPOLE", FitsValue::Float(longpole * RAD_TO_DEG));
    h.set("LATPOLE", FitsValue::Float(latpole * RAD_TO_DEG));
}

/// Emits FITS keywords for a [`SpectralCoordinate`].
fn emit_spectral(
    coord: &CoordinateModel,
    h: &mut FitsHeader,
    axis: usize,
) -> Result<(), CoordinateError> {
    let CoordinateModel::Spectral(spectral) = coord else {
        return Ok(());
    };
    let (crpix, crval_hz, cdelt_hz) = spectral.fits_linearized_axis_hz()?;

    h.set(format!("CTYPE{axis}"), FitsValue::String("FREQ".into()));
    h.set(format!("CRVAL{axis}"), FitsValue::Float(crval_hz));
    h.set(format!("CRPIX{axis}"), FitsValue::Float(crpix + 1.0));
    h.set(format!("CDELT{axis}"), FitsValue::Float(cdelt_hz));
    h.set(format!("CUNIT{axis}"), FitsValue::String("Hz".into()));

    if !spectral.has_undefined_frame() {
        let frequency_ref = spectral.world_frequency_ref();
        let specsys = match frequency_ref.as_str() {
            "LSRK" => "LSRK",
            "BARY" => "BARYCENT",
            "TOPO" => "TOPOCENT",
            "GEO" => "GEOCENTR",
            "GALACTO" => "GALACTOC",
            "LGROUP" => "LOCALGRP",
            "CMB" => "CMBDIPOL",
            "REST" => "SOURCE",
            other => other,
        };
        h.set("SPECSYS", FitsValue::String(specsys.into()));
    }
    h.set("RESTFRQ", FitsValue::Float(spectral.rest_frequency_hz()?));
    Ok(())
}

/// Emits FITS keywords for a [`StokesCoordinate`].
fn emit_stokes(coord: &CoordinateModel, h: &mut FitsHeader, axis: usize) {
    let crval = coord.reference_value();
    let crpix = coord.reference_pixel();
    let cdelt = coord.increment();

    let (reference, increment) = match coord {
        CoordinateModel::Stokes(stokes) => {
            extended_stokes_axis(stokes.stokes()).unwrap_or((crval[0], cdelt[0]))
        }
        _ => (crval[0], cdelt[0]),
    };
    h.set(format!("CTYPE{axis}"), FitsValue::String("STOKES".into()));
    h.set(format!("CRVAL{axis}"), FitsValue::Float(reference));
    h.set(format!("CRPIX{axis}"), FitsValue::Float(crpix[0] + 1.0));
    h.set(format!("CDELT{axis}"), FitsValue::Float(increment));
    h.set(format!("CUNIT{axis}"), FitsValue::String(String::new()));
}

fn extended_stokes(value: StokesType) -> bool {
    matches!(value.code(), 0 | 13..=27)
}

fn fits_stokes_code(value: StokesType) -> i32 {
    match value {
        StokesType::I | StokesType::Q | StokesType::U | StokesType::V => value.code(),
        StokesType::RR => -1,
        StokesType::LL => -2,
        StokesType::RL => -3,
        StokesType::LR => -4,
        StokesType::XX => -5,
        StokesType::YY => -6,
        StokesType::XY => -7,
        StokesType::YX => -8,
        StokesType::PFlinear => 6,
        StokesType::Pangle => 7,
        _ => 100 + value.code(),
    }
}

/// Use the casacore FITS codes for the extended native Stokes domain.
/// Legacy casa-rs headers used native codes. CASA also uses those numeric
/// axis parameters when the FITS codes cannot be represented in one axis.
fn extended_stokes_axis(stokes: &[StokesType]) -> Option<(f64, f64)> {
    if !stokes.iter().copied().any(extended_stokes) {
        return None;
    }
    fits_stokes_axis(stokes)
}

/// FITS reference value and increment for regularly ordered Stokes planes.
/// Returns `None` when the codes cannot share a linear FITS axis.
pub fn fits_stokes_axis(stokes: &[StokesType]) -> Option<(f64, f64)> {
    let mut codes = stokes.iter().copied().map(fits_stokes_code);
    let start = codes.next()?;
    let second = codes.next().unwrap_or(start + 1);
    let increment = second - start;
    let mut previous = second;
    for code in codes {
        if code - previous != increment {
            return None;
        }
        previous = code;
    }
    Some((f64::from(start), f64::from(increment)))
}

/// Emits FITS keywords for a [`LinearCoordinate`] (or tabular treated as linear).
fn emit_linear(coord: &CoordinateModel, h: &mut FitsHeader, axes: &[usize]) {
    let crval = coord.reference_value();
    let crpix = coord.reference_pixel();
    let cdelt = coord.increment();
    let names = coord.axis_names();
    let units = coord.axis_units();
    let n = coord.n_pixel_axes();

    for (i, &a) in axes.iter().enumerate() {
        h.set(
            format!("CTYPE{a}"),
            FitsValue::String(
                names
                    .get(i)
                    .cloned()
                    .unwrap_or_default()
                    .to_ascii_uppercase(),
            ),
        );
        h.set(format!("CRVAL{a}"), FitsValue::Float(crval[i]));
        h.set(format!("CRPIX{a}"), FitsValue::Float(crpix[i] + 1.0));
        h.set(format!("CDELT{a}"), FitsValue::Float(cdelt[i]));
        h.set(
            format!("CUNIT{a}"),
            FitsValue::String(units.get(i).cloned().unwrap_or_default()),
        );
    }

    if let CoordinateModel::Linear(linear) = coord {
        let pc_flat: Vec<f64> = linear.pc_matrix().iter().copied().collect();
        if pc_flat.len() == n * n {
            for i in 0..n {
                for j in 0..n {
                    let val = pc_flat[i * n + j];
                    let id_val = if i == j { 1.0 } else { 0.0 };
                    if (val - id_val).abs() > 1e-15 {
                        let fi = axes[i];
                        let fj = axes[j];
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
/// - `RA--`, `DEC-`, `GLON`, `GLAT`, `SLON`, `SLAT`, `ELON`, `ELAT` with a projection suffix
///   become a [`DirectionCoordinate`].
/// - `FREQ`, `VRAD` or `VELO` becomes a [`SpectralCoordinate`].
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
    if let Some((aligned, world_axes)) = align_fits_pc_rows(header, shape.len()) {
        let (mut coordinates, model_pixels) = from_fits_header_with_axis_order(&aligned, shape)?;
        let mut pixel_order = vec![0; model_pixels.len()];
        let mut world_order = vec![0; model_pixels.len()];
        for (local, pixel_axis) in model_pixels.into_iter().enumerate() {
            pixel_order[pixel_axis] = local;
            world_order[world_axes[pixel_axis]] = local;
        }
        coordinates.transpose_axes(&world_order, &pixel_order)?;
        return Ok(coordinates);
    }
    from_fits_header_with_axis_order(header, shape).map(|(coordinates, _)| coordinates)
}

/// Decode a FITS header and report each model axis's original FITS pixel axis.
///
/// The model groups direction axes together, even when their position differs
/// in the input array. Use the resulting permutation when saving a record for
/// an array that retains the original FITS axis order.
///
/// # Errors
/// Returns a coordinate error when the header cannot produce a model.
pub fn from_fits_header_with_axis_order(
    header: &FitsHeader,
    shape: &[usize],
) -> Result<(CoordinateSystem, Vec<usize>), CoordinateError> {
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
    let mut axis_order = Vec::with_capacity(naxis);
    let has_rest_frequency = header
        .get_float("RESTFRQ")
        .or_else(|| header.get_float("RESTFREQ"))
        .is_some_and(|frequency| frequency > 0.0);

    // Pass 1: find direction coordinate pairs
    for i in 0..naxis {
        if consumed[i] {
            continue;
        }
        if let Some((lon_axis, lat_axis, proj_code, dir_ref)) =
            identify_direction_pair(&ctypes, i, &consumed)
        {
            let coord = parse_direction(header, lon_axis, lat_axis, &proj_code, dir_ref)?;
            cs.add_coordinate(coord);
            axis_order.extend([lon_axis, lat_axis]);
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

        if upper.starts_with("FREQ")
            || upper.starts_with("VRAD")
            || upper.starts_with("VELO")
            || (upper == "VOPT" && has_rest_frequency)
        {
            let coord = parse_spectral(header, i + 1, shape.get(i).copied().unwrap_or(1))?;
            cs.add_coordinate(coord);
        } else if upper == "STOKES" {
            let axis_len = shape.get(i).copied().unwrap_or(1);
            let coord = parse_stokes(header, i + 1, axis_len)?;
            cs.add_coordinate(coord);
        } else {
            let coord = parse_linear_single(header, i + 1, ctype)?;
            cs.add_coordinate(coord);
        }
        consumed[i] = true;
        axis_order.push(i);
    }

    // Observation info
    let mut obs = ObsInfo::default();
    if let Some(tel) = header.get_string("TELESCOP") {
        obs.telescope = tel.to_string();
    }
    if let Some(obs_name) = header.get_string("OBSERVER") {
        obs.observer = obs_name.to_string();
    }
    obs.date = obs_epoch(
        header.get_float("MJD-OBS"),
        header.get_string("DATE-OBS"),
        header.get_string("TIMESYS"),
    );
    *cs.obs_info_mut() = obs;

    Ok((cs, axis_order))
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
            DirectionRef::ECLIPTIC => "ELAT",
            _ => "DEC-",
        }
    } else {
        match dir_ref {
            DirectionRef::GALACTIC => "GLON",
            DirectionRef::SUPERGAL => "SLON",
            DirectionRef::ECLIPTIC => "ELON",
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
        "ELON" => (true, DirectionRef::ECLIPTIC),
        "ELAT" => (false, DirectionRef::ECLIPTIC),
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
    let defaults = proj_type.fits_parameter_defaults();
    let start = proj_type.fits_parameter_start();
    let specified = (start..start + defaults.len())
        .map(|index| h.get_float(&format!("PV{a2}_{index}")))
        .collect::<Vec<_>>();
    let parameters = if let Some(last) = specified.iter().rposition(Option::is_some) {
        // CASA's coordinate record defaults to 20 polynomial terms. FITS
        // can specify higher terms sparsely through WCSLIB's 30th slot.
        let count = if proj_type == ProjectionType::ZPN {
            (last + 1).max(20)
        } else {
            defaults.len()
        };
        specified
            .into_iter()
            .take(count)
            .zip(defaults)
            .map(|(value, &default)| value.unwrap_or(default))
            .collect()
    } else {
        vec![]
    };
    let proj = Projection::with_parameters(proj_type, parameters);
    if !defaults.is_empty() {
        proj.validate_conversion_parameters()?;
    }

    let lon_scale = celestial_unit_scale_to_rad(h, a1)?;
    let lat_scale = celestial_unit_scale_to_rad(h, a2)?;

    // CRVAL (FITS celestial units -> radians; default is degrees)
    let crval_lon = h
        .get_float(&format!("CRVAL{a1}"))
        .ok_or_else(|| CoordinateError::InvalidRecord(format!("missing CRVAL{a1}")))?
        * lon_scale;
    let crval_lat = h
        .get_float(&format!("CRVAL{a2}"))
        .ok_or_else(|| CoordinateError::InvalidRecord(format!("missing CRVAL{a2}")))?
        * lat_scale;

    // CRPIX (1-based -> 0-based)
    let crpix_lon = h.get_float(&format!("CRPIX{a1}")).unwrap_or(1.0) - 1.0;
    let crpix_lat = h.get_float(&format!("CRPIX{a2}")).unwrap_or(1.0) - 1.0;

    // CDELT/CD (FITS celestial units -> radians) with PC/CD/CROTA2 support.
    let (cdelt_lon, cdelt_lat, pc) = read_linear_transform_2d(h, a1, a2, lon_scale, lat_scale)?;

    // Direction reference from RADESYS
    let dir_ref = if default_dir_ref == DirectionRef::J2000 {
        fits_equatorial_frame(h)?
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

// casacore's FITS encoding for MDirection::B1950_VLA.
const VLA_BESSELIAN_EQUINOX: f64 = 1979.9;

fn fits_equatorial_frame(header: &FitsHeader) -> Result<DirectionRef, CoordinateError> {
    let equinox = header
        .get_float("EQUINOX")
        .or_else(|| header.get_float("EPOCH"));
    let near = |standard: f64| {
        // FITSCoordinateUtil uses casacore's default Double near tolerance.
        equinox.is_some_and(|value| {
            value.is_finite()
                && value > 0.0
                && (value - standard).abs() <= 1e-13 * value.max(standard)
        })
    };
    let system = header
        .get_string("RADESYS")
        .or_else(|| header.get_string("RADECSYS"))
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_ascii_uppercase);
    Ok(match (system.as_deref(), equinox) {
        (Some("FK4" | "FK4-NO-E") | None, _) if near(VLA_BESSELIAN_EQUINOX) => {
            DirectionRef::B1950Vla
        }
        (Some("FK4" | "FK4-NO-E"), _) if equinox.is_none() || near(1950.0) => DirectionRef::B1950,
        (Some("FK5"), _) if equinox.is_none() || near(2000.0) => DirectionRef::J2000,
        (Some("ICRS"), _) if equinox.is_none() || near(2000.0) => DirectionRef::ICRS,
        (Some(system @ ("FK4" | "FK4-NO-E" | "FK5" | "ICRS")), Some(equinox)) => {
            return Err(CoordinateError::InvalidRecord(format!(
                "FITS direction system {system} with equinox {equinox} is not supported"
            )));
        }
        _ => DirectionRef::J2000,
    })
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
    lon_scale: f64,
    lat_scale: f64,
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

        Ok((cdelt1 * lon_scale, cdelt2 * lat_scale, pc))
    } else {
        // PC matrix or CROTA2
        let cdelt1 = h
            .get_float(&format!("CDELT{a1}"))
            .ok_or_else(|| CoordinateError::InvalidRecord(format!("missing CDELT{a1}")))?;
        let cdelt2 = h
            .get_float(&format!("CDELT{a2}"))
            .ok_or_else(|| CoordinateError::InvalidRecord(format!("missing CDELT{a2}")))?;

        let matrix = [(a1, a1), (a1, a2), (a2, a1), (a2, a2)]
            .map(|(i, j)| h.get_float(&format!("PC{i}_{j}")));
        let pc = if matrix.iter().any(Option::is_some) {
            // Explicit PC matrix
            let values = matrix
                .into_iter()
                .zip([1.0, 0.0, 0.0, 1.0])
                .map(|(value, default)| value.unwrap_or(default))
                .collect();
            ndarray::Array2::from_shape_vec((2, 2), values).unwrap()
        } else if let Some(crota2) = h.get_float("CROTA2") {
            // Legacy CROTA2 rotation (in degrees)
            let rot = crota2 * DEG_TO_RAD;
            let cos_r = rot.cos();
            let sin_r = rot.sin();
            ndarray::Array2::from_shape_vec((2, 2), vec![cos_r, -sin_r, sin_r, cos_r]).unwrap()
        } else {
            ndarray::Array2::eye(2)
        };

        Ok((cdelt1 * lon_scale, cdelt2 * lat_scale, pc))
    }
}

fn celestial_unit_scale_to_rad(h: &FitsHeader, axis: usize) -> Result<f64, CoordinateError> {
    let unit = h
        .get_string(&format!("CUNIT{axis}"))
        .unwrap_or("deg")
        .trim()
        .to_ascii_lowercase();
    match unit.as_str() {
        "" | "deg" | "degree" | "degrees" => Ok(DEG_TO_RAD),
        "rad" | "radian" | "radians" => Ok(1.0),
        other => Err(CoordinateError::InvalidRecord(format!(
            "unsupported celestial CUNIT{axis}={other:?}; expected deg or rad"
        ))),
    }
}

/// Parses a spectral coordinate from the FITS header.
fn parse_spectral(
    h: &FitsHeader,
    axis: usize,
    axis_len: usize,
) -> Result<SpectralCoordinate, CoordinateError> {
    let crval = h
        .get_float(&format!("CRVAL{axis}"))
        .ok_or_else(|| CoordinateError::InvalidRecord(format!("missing CRVAL{axis}")))?;
    let cdelt = h
        .get_float(&format!("CDELT{axis}"))
        .ok_or_else(|| CoordinateError::InvalidRecord(format!("missing CDELT{axis}")))?;
    let crpix = h.get_float(&format!("CRPIX{axis}")).unwrap_or(1.0) - 1.0;

    let rest_freq = h
        .get_float("RESTFRQ")
        .or_else(|| h.get_float("RESTFREQ"))
        .unwrap_or(0.0);

    let freq_ref = if let Some(specsys) = h.get_string("SPECSYS") {
        match specsys.to_uppercase().as_str() {
            "LSRK" => FrequencyRef::LSRK,
            "BARYCENT" | "BARY" => FrequencyRef::BARY,
            "TOPOCENT" | "TOPO" => FrequencyRef::TOPO,
            "LSRD" => FrequencyRef::LSRD,
            "GEOCENTR" | "GEO" => FrequencyRef::GEO,
            "GALACTOC" | "GALACTO" => FrequencyRef::GALACTO,
            "LOCALGRP" | "LGROUP" => FrequencyRef::LGROUP,
            "CMBDIPOL" | "CMB" => FrequencyRef::CMB,
            "SOURCE" | "REST" => FrequencyRef::REST,
            _ => FrequencyRef::TOPO,
        }
    } else if h
        .get_string(&format!("CTYPE{axis}"))
        .is_some_and(|ctype| ctype.to_ascii_uppercase().ends_with("-LSR"))
    {
        // FITSSpectralUtil::frameFromTag reads the pre-SPECSYS LSR tag.
        FrequencyRef::LSRK
    } else {
        FrequencyRef::TOPO
    };

    let ctype = h
        .get_string(&format!("CTYPE{axis}"))
        .map_or_else(String::new, str::to_ascii_uppercase);
    let radio_velocity = ctype.starts_with("VRAD");
    let optical_velocity = ctype == "VOPT";
    let unit = h
        .get_string(&format!("CUNIT{axis}"))
        .unwrap_or(if radio_velocity || optical_velocity {
            "m/s"
        } else {
            "Hz"
        })
        .to_string();
    // FITS permits the historical uppercase Hz spelling (Unit::addFITS in
    // casacore), including the ppdisk tutorial's CUNIT4='HZ'.
    let unit = if unit.eq_ignore_ascii_case("HZ") {
        "Hz".to_owned()
    } else {
        unit
    };

    if radio_velocity || optical_velocity {
        let velocity_unit = casa_core::quanta::Unit::new(&unit)
            .map_err(|error| CoordinateError::InvalidRecord(error.to_string()))?;
        let metres_per_second =
            casa_core::quanta::Unit::new("m/s").expect("built-in velocity unit");
        if !velocity_unit.conformant(&metres_per_second) {
            return Err(CoordinateError::InvalidRecord(
                if radio_velocity {
                    "radio velocity axis needs a velocity unit"
                } else {
                    "optical velocity axis needs a velocity unit"
                }
                .into(),
            ));
        }
        let scale = velocity_unit.val().factor / metres_per_second.val().factor;
        if rest_freq <= 0.0 {
            // Casacore keeps the numerical axis in SI units when it cannot
            // convert a velocity without a rest frequency.
            return Ok(SpectralCoordinate::new(
                freq_ref,
                crval * scale,
                cdelt * scale,
                crpix,
                rest_freq,
            ));
        }
        let speed_of_light = 299_792_458.0;
        if optical_velocity {
            let frequency = |pixel: f64| {
                rest_freq / (1.0 + (crval + (pixel - crpix) * cdelt) * scale / speed_of_light)
            };
            let pixels: Vec<_> = (0..axis_len).map(|pixel| pixel as f64).collect();
            let worlds: Vec<_> = pixels.iter().copied().map(frequency).collect();
            let first = frequency(0.0);
            let last = axis_len.saturating_sub(1).max(1) as f64;
            let increment = (frequency(last) - first) / last;
            return SpectralCoordinate::from_tabular(
                freq_ref, pixels, worlds, first, increment, 0.0, rest_freq,
            );
        }
        let reference_hz = rest_freq * (1.0 - crval * scale / speed_of_light);
        let increment_hz = -rest_freq * cdelt * scale / speed_of_light;
        return Ok(SpectralCoordinate::new(
            freq_ref,
            reference_hz,
            increment_hz,
            crpix,
            rest_freq,
        ));
    }

    let coordinate =
        SpectralCoordinate::new(freq_ref, crval, cdelt, crpix, rest_freq).with_unit(unit);
    // FITS RESTFRQ is always Hz. As casacore does, normalize a frequency
    // axis itself to Hz too; the FITS axis unit can supply a scaling factor.
    // Preserve the existing velocity-axis reader when the unit is not a frequency.
    if let (Ok(reference_hz), Ok(increment_hz)) =
        (coordinate.value_in_hz(crval), coordinate.value_in_hz(cdelt))
    {
        Ok(SpectralCoordinate::new(
            freq_ref,
            reference_hz,
            increment_hz,
            crpix,
            rest_freq,
        ))
    } else {
        Ok(coordinate)
    }
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
    let code_at = |i| (crval + cdelt * (i as f64 - crpix)).round() as i32;
    let extended_codes = (0..axis_len).any(|i| matches!(code_at(i), 100..=132));

    let mut stokes = Vec::with_capacity(axis_len);
    for i in 0..axis_len {
        let code = code_at(i);
        let st = match code {
            -1 => Some(StokesType::RR),
            -2 => Some(StokesType::LL),
            -3 => Some(StokesType::RL),
            -4 => Some(StokesType::LR),
            -5 => Some(StokesType::XX),
            -6 => Some(StokesType::YY),
            -7 => Some(StokesType::XY),
            -8 => Some(StokesType::YX),
            6 if extended_codes => Some(StokesType::PFlinear),
            7 if extended_codes => Some(StokesType::Pangle),
            13..=27 => Some(StokesType::Undefined),
            100..=132 => StokesType::from_code(code - 100),
            _ => StokesType::from_code(code).filter(|value| !extended_stokes(*value)),
        }
        .ok_or_else(|| CoordinateError::InvalidRecord(format!("unknown Stokes code {code}")))?;
        if st == StokesType::Undefined && stokes.contains(&st) {
            return Err(CoordinateError::InvalidRecord(
                "the FITS Stokes axis repeats an undefined value".into(),
            ));
        }
        stokes.push(st);
    }

    Ok(StokesCoordinate::new(stokes))
}

/// Construct the linear coordinate system that casacore's FITS image
/// converter uses when it cannot interpret the supplied WCS.
pub fn fallback_fits_linear_coordinates(shape: &[usize]) -> CoordinateSystem {
    let mut coordinates = CoordinateSystem::new();
    let ndim = shape.len();
    coordinates.add_coordinate(
        LinearCoordinate::new(
            ndim,
            (0..ndim).map(|axis| format!("linear{axis}")).collect(),
            vec!["km".into(); ndim],
        )
        .with_reference_pixel(
            shape
                .iter()
                .map(|&length| length.div_ceil(2) as f64)
                .collect(),
        ),
    );
    coordinates
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
    let unit = h.get_string(&format!("CUNIT{axis}")).unwrap_or("");
    let normalized = super::wcs_unit_name(unit);
    if casa_core::quanta::Unit::new(&normalized).is_err() {
        return Err(CoordinateError::InvalidFitsAxisUnit {
            axis,
            unit: unit.into(),
        });
    }

    let name = if ctype.is_empty() {
        format!("Linear{axis}")
    } else {
        ctype.to_string()
    };

    Ok(LinearCoordinate::new(1, vec![name], vec![normalized])
        .with_reference_value(vec![crval])
        .with_reference_pixel(vec![crpix])
        .with_increment(vec![cdelt]))
}

#[cfg(test)]
mod tests;

// SPDX-License-Identifier: LGPL-3.0-or-later
//! CASA convolution-function cell images: a cell's header and pixels as
//! `CFCache` reads them, a group's cells loaded into dense kernels, and a
//! native cell written as `CFCell::makePersistent` writes it.

use std::path::Path;
use std::sync::Arc;

use casa_coordinates::{
    CoordinateSystem, CoordinateType, LinearCoordinate, SpectralCoordinate, StokesCoordinate,
    StokesType,
};
use casa_images::PagedImage;
use casa_types::measures::frequency::FrequencyRef;
use casa_types::{RecordField, RecordValue, ScalarValue, Value};
use num_complex::{Complex32, Complex64};

use super::{
    AwCatalogError, CellFiles, CellHeader, Group, Loaded, MUELLER_ELEMENTS, NativeAwPlane,
    STAGING_PREFIX, cache_error,
};
use crate::convolution::DenseCell;

/// `CFCache::getCFParams`: the cell's coordinates and misc info.
pub(super) fn read_header(path: &Path) -> Result<CellHeader, AwCatalogError> {
    let image = PagedImage::<Complex32>::open(path)
        .map_err(|error| cache_error(path, format!("cannot open Complex32 image: {error}")))?;
    let shape = image.shape();
    if shape.len() != 4 || shape[0] == 0 || shape[1] == 0 || shape[2..] != [1, 1] {
        return Err(cache_error(
            path,
            format!("expected [nx, ny, 1, 1], got {shape:?}"),
        ));
    }
    if shape[0] % 2 != 0 || shape[1] % 2 != 0 {
        return Err(cache_error(path, "cell images must have even extents"));
    }
    let coordinates = image.coordinates();
    let spectral = coordinates
        .find_coordinate(CoordinateType::Spectral)
        .ok_or_else(|| cache_error(path, "missing spectral coordinate"))?;
    let frequency = coordinates
        .coordinate(spectral)
        .reference_value()
        .first()
        .copied()
        .ok_or_else(|| cache_error(path, "spectral coordinate has no reference value"))?;
    // CASA reads the reference value into a `Float` before listing it.
    let frequency_hz = f64::from(frequency as f32);
    if !frequency_hz.is_finite() || frequency_hz <= 0.0 {
        return Err(cache_error(
            path,
            "cell frequency must be finite and positive",
        ));
    }
    let misc = image.misc_info();
    let sampling = required_f64(&misc, "Sampling", path)?;
    if sampling.fract() != 0.0
        || sampling < 2.0
        || sampling > f64::from(u16::MAX)
        || sampling % 2.0 != 0.0
    {
        return Err(cache_error(
            path,
            "Sampling must be a positive even integer",
        ));
    }
    let support = [
        required_support(&misc, "Xsupport", path)?,
        required_support(&misc, "Ysupport", path)?,
    ];
    let mueller = required_i32(&misc, "MuellerElement", path)?;
    let mueller = u32::try_from(mueller)
        .ok()
        .filter(|element| *element < MUELLER_ELEMENTS)
        .ok_or_else(|| cache_error(path, "MuellerElement must lie in 0..16"))?;
    let w_value = required_f64(&misc, "WValue", path)?;
    let w_increment = required_f64(&misc, "WIncr", path)?;
    let pa_deg = required_f64(&misc, "ParallacticAngle", path)?;
    if !w_value.is_finite()
        || w_value < 0.0
        || !w_increment.is_finite()
        || w_increment < 0.0
        || !pa_deg.is_finite()
    {
        return Err(cache_error(
            path,
            "WValue, WIncr and ParallacticAngle must be finite and non-negative",
        ));
    }
    let sampling = sampling as u16;
    for axis in 0..2 {
        if (2 * usize::from(support[axis]) + 1) * usize::from(sampling) > shape[axis] {
            return Err(cache_error(path, "support does not fit the cell image"));
        }
    }
    // Native cells carry the working sky increment they were generated on;
    // CASA's own caches do not.
    let sky_increment_rad = match misc.get("CasaRsSkyIncrement") {
        Some(Value::Scalar(ScalarValue::Float64(value))) => Some(*value),
        _ => None,
    };
    if sky_increment_rad.is_some_and(|value| !value.is_finite() || value <= 0.0) {
        return Err(cache_error(
            path,
            "CasaRsSkyIncrement must be finite and positive",
        ));
    }
    let working_size = match misc.get("CasaRsWorkingSize") {
        Some(Value::Scalar(ScalarValue::Int32(value))) if *value > 0 => Some(*value as usize),
        Some(_) => {
            return Err(cache_error(path, "CasaRsWorkingSize must be positive"));
        }
        None => None,
    };
    let working_field = sky_increment_rad
        .zip(working_size)
        .map(|(increment, size)| increment * size as f64);
    Ok(CellHeader {
        path: path.to_path_buf(),
        shape: [shape[0], shape[1]],
        sampling,
        support,
        mueller,
        w_value,
        w_increment,
        pa_deg,
        frequency_hz,
        working_field_rad: working_field,
    })
}

fn required_support(record: &RecordValue, name: &str, path: &Path) -> Result<u16, AwCatalogError> {
    let value = required_i32(record, name, path)?;
    u16::try_from(value)
        .ok()
        .filter(|support| *support > 0)
        .ok_or_else(|| cache_error(path, format!("{name} must be positive")))
}

fn required_f64(record: &RecordValue, name: &str, path: &Path) -> Result<f64, AwCatalogError> {
    match record.get(name) {
        Some(Value::Scalar(ScalarValue::Float64(value))) => Ok(*value),
        Some(Value::Scalar(ScalarValue::Float32(value))) => Ok(f64::from(*value)),
        Some(value) => Err(cache_error(
            path,
            format!("{name} must be floating point, got {value:?}"),
        )),
        None => Err(cache_error(path, format!("missing miscinfo field {name}"))),
    }
}

fn required_i32(record: &RecordValue, name: &str, path: &Path) -> Result<i32, AwCatalogError> {
    match record.get(name) {
        Some(Value::Scalar(ScalarValue::Int32(value))) => Ok(*value),
        Some(Value::Scalar(ScalarValue::Int64(value))) => i32::try_from(*value)
            .map_err(|_| cache_error(path, format!("{name} is outside i32 range"))),
        Some(value) => Err(cache_error(
            path,
            format!("{name} must be integer, got {value:?}"),
        )),
        None => Err(cache_error(path, format!("missing miscinfo field {name}"))),
    }
}

/// Read the pixels of `header`'s image as a `[y][x]` plane, x fastest.
fn read_plane(header: &CellHeader) -> Result<Vec<Complex32>, AwCatalogError> {
    let image = PagedImage::<Complex32>::open(&header.path)
        .map_err(|error| cache_error(&header.path, format!("cannot open: {error}")))?;
    let pixels = image
        .get()
        .map_err(|error| cache_error(&header.path, format!("cannot read: {error}")))?;
    let [nx, ny] = header.shape;
    if pixels.shape() != [nx, ny, 1, 1] {
        return Err(cache_error(
            &header.path,
            "cell image changed shape since open",
        ));
    }
    let mut plane = Vec::with_capacity(nx * ny);
    for y in 0..ny {
        for x in 0..nx {
            plane.push(pixels[[x, y, 0, 0]]);
        }
    }
    Ok(plane)
}

/// `AWConvFunc::cfArea`: `Σ` over `[−support, support)` of the integer taps
/// about `shape/2`.
pub(super) fn cf_area(
    plane: &[Complex32],
    shape: [usize; 2],
    support: [u16; 2],
    sampling: u16,
) -> Complex64 {
    let origin = [shape[0] / 2, shape[1] / 2];
    let mut area = Complex64::default();
    for iy in -i64::from(support[1])..i64::from(support[1]) {
        for ix in -i64::from(support[0])..i64::from(support[0]) {
            let x = origin[0] as i64 + ix * i64::from(sampling);
            let y = origin[1] as i64 + iy * i64::from(sampling);
            if x < 0 || y < 0 || x as usize >= shape[0] || y as usize >= shape[1] {
                continue;
            }
            let value = plane[y as usize * shape[0] + x as usize];
            area += Complex64::new(f64::from(value.re), f64::from(value.im));
        }
    }
    area
}

/// Load one group's Mueller planes (its `cells`, at `group`'s supports)
/// into an imaging and a weight cell:
/// every plane normalised by its own area (`AWConvFunc::cfArea`), tiled at
/// its kind's largest support with zero beyond a smaller plane's image.
pub(super) fn load_group(
    cells: &[CellFiles],
    group: &Group,
    sampling: u16,
) -> Result<Loaded, AwCatalogError> {
    let build = |select: fn(&CellFiles) -> &CellHeader,
                 half: [u16; 2]|
     -> Result<Arc<DenseCell>, AwCatalogError> {
        let headers = cells.iter().map(select).collect::<Vec<_>>();
        let mut planes = Vec::with_capacity(headers.len());
        for header in &headers {
            let mut plane = read_plane(header)?;
            let area = cf_area(&plane, header.shape, header.support, header.sampling);
            if !(area.norm().is_finite() && area.norm() > 0.0) {
                return Err(cache_error(&header.path, "cell area is not positive"));
            }
            let scale = Complex32::new((1.0 / area).re as f32, (1.0 / area).im as f32);
            for value in &mut plane {
                *value *= scale;
            }
            planes.push((plane, header.shape, header.support));
        }
        Ok(Arc::new(dense_cell_padded(&planes, half, sampling)))
    };
    Ok(Loaded {
        imaging: build(|cell| &cell.imaging, group.imaging_half_support)?,
        weight: build(|cell| &cell.weight, group.weight_half_support)?,
        last_use: 0,
    })
}

/// The dense cell of `planes` (each `[y][x]` with origin at `shape/2` and
/// its own declared half support) at `half` taps each side: tap `k` at
/// fine offset `off` reads pixel `origin + k·sampling + off`
/// (`accumulateToGrid.inc`: `convOrigin + (Int)(sampling·ix + off)`), zero
/// outside the plane and beyond the plane's own support (`AWVisResampler`
/// takes the support per Mueller cell; the crop buffer around it is not
/// part of the kernel).
pub(super) fn dense_cell_padded(
    planes: &[(Vec<Complex32>, [usize; 2], [u16; 2])],
    half: [u16; 2],
    sampling: u16,
) -> DenseCell {
    let support = [2 * half[0] + 1, 2 * half[1] + 1];
    let [sx, sy] = [usize::from(support[0]), usize::from(support[1])];
    let rows = usize::from(sampling) + 1;
    let fine_half = i64::from(sampling / 2);
    let mut data = Vec::with_capacity(rows * rows * planes.len() * sx * sy);
    for oy in 0..rows {
        let off_y = oy as i64 - fine_half;
        for ox in 0..rows {
            let off_x = ox as i64 - fine_half;
            for (plane, shape, plane_half) in planes {
                let origin = [(shape[0] / 2) as i64, (shape[1] / 2) as i64];
                for iy in 0..sy {
                    let ky = iy as i64 - i64::from(half[1]);
                    let y = origin[1] + ky * i64::from(sampling) + off_y;
                    for ix in 0..sx {
                        let kx = ix as i64 - i64::from(half[0]);
                        let x = origin[0] + kx * i64::from(sampling) + off_x;
                        let inside = kx.abs() <= i64::from(plane_half[0])
                            && ky.abs() <= i64::from(plane_half[1])
                            && x >= 0
                            && y >= 0
                            && (x as usize) < shape[0]
                            && (y as usize) < shape[1];
                        data.push(if inside {
                            plane[y as usize * shape[0] + x as usize]
                        } else {
                            Complex32::default()
                        });
                    }
                }
            }
        }
    }
    DenseCell::new(
        data.into_boxed_slice(),
        support,
        sampling,
        u8::try_from(planes.len()).expect("Mueller planes fit u8"),
    )
}

/// The misc info `CFCell::makePersistent` writes.
pub(super) struct CellMisc {
    pub(super) sampling: f64,
    pub(super) pa_deg: f64,
    pub(super) mueller: usize,
    pub(super) w_value: f64,
    pub(super) w_increment: f64,
    pub(super) conjugate_frequency: f64,
    pub(super) conjugate_mueller: u32,
    pub(super) diameter_m: f64,
    /// The working sky increment and size the cell was generated on:
    /// private keys (`CasaRsSkyIncrement`, `CasaRsWorkingSize`) the loader
    /// checks against the image geometry so a cache is never reused for
    /// another one.
    pub(super) sky_increment_rad: f64,
    pub(super) working_size: usize,
}

/// Write one generated plane as a CASA cell image (`CFCell::makePersistent`
/// with `SynthesisUtils::makeFTCoordSys`'s UU/VV coordinate) at `path`.
/// The image is built under [`STAGING_PREFIX`] beside it and renamed to
/// `path` only once saved, so `path` names either nothing or a complete
/// image, whenever the write stops (#739 review).
pub(super) fn write_cell(
    path: &Path,
    plane: &NativeAwPlane<'_>,
    frequency_hz: f64,
    grid: casa_imaging_model::NativeAwGrid,
    misc: &CellMisc,
) -> Result<(), AwCatalogError> {
    let write_error = |detail: String| AwCatalogError::Write {
        path: path.to_path_buf(),
        detail,
    };
    let name = path
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or_default()
        .to_string();
    let staging = path.with_file_name(format!("{STAGING_PREFIX}{name}"));
    if staging.exists() {
        std::fs::remove_dir_all(&staging)
            .map_err(|error| write_error(format!("cannot clear a staged image: {error}")))?;
    }
    build_cell(&staging, &name, plane, frequency_hz, grid, misc).map_err(write_error)?;
    // A cell being replaced moves aside in one rename, so its name never
    // names a half-removed image either.
    let replaced = path.with_file_name(format!("{STAGING_PREFIX}replaced-{name}"));
    if path.exists() {
        std::fs::rename(path, &replaced)
            .map_err(|error| write_error(format!("cannot replace: {error}")))?;
    }
    std::fs::rename(&staging, path)
        .map_err(|error| write_error(format!("cannot publish the staged image: {error}")))?;
    if replaced.exists() {
        std::fs::remove_dir_all(&replaced)
            .map_err(|error| write_error(format!("cannot remove the replaced image: {error}")))?;
    }
    Ok(())
}

/// Build the cell image of [`write_cell`] at `path`, its misc info naming it
/// `name`.
fn build_cell(
    path: &Path,
    name: &str,
    plane: &NativeAwPlane<'_>,
    frequency_hz: f64,
    grid: casa_imaging_model::NativeAwGrid,
    misc: &CellMisc,
) -> Result<(), String> {
    let write_error = |detail: String| detail;
    let side = plane.size;
    // The uv increment of one oversampled pixel over the working field.
    let increment =
        1.0 / (grid.size as f64 * grid.sky_increment_rad[1].abs()) / grid.oversampling as f64;
    let mut coordinates = CoordinateSystem::new();
    coordinates.add_coordinate(
        LinearCoordinate::new(
            2,
            vec!["UU".to_string(), "VV".to_string()],
            vec!["lambda".to_string(), "lambda".to_string()],
        )
        .with_reference_value(vec![0.0, 0.0])
        .with_reference_pixel(vec![(side / 2) as f64, (side / 2) as f64])
        .with_increment(vec![-increment, increment]),
    );
    coordinates.add_coordinate(StokesCoordinate::new(vec![if misc.mueller == 0 {
        StokesType::RR
    } else {
        StokesType::LL
    }]));
    coordinates.add_coordinate(SpectralCoordinate::new(
        FrequencyRef::LSRK,
        frequency_hz,
        1.0,
        0.0,
        frequency_hz,
    ));
    let mut image = PagedImage::<Complex32>::create(vec![side, side, 1, 1], coordinates, path)
        .map_err(|error| write_error(format!("cannot create: {error}")))?;
    let mut pixels = ndarray::ArrayD::<Complex32>::zeros(ndarray::IxDyn(&[side, side, 1, 1]));
    for y in 0..side {
        for x in 0..side {
            pixels[[x, y, 0, 0]] = plane.values[y * side + x];
        }
    }
    image
        .put_slice(&pixels, &[0, 0, 0, 0])
        .map_err(|error| write_error(format!("cannot write pixels: {error}")))?;
    let field = |name: &str, value: ScalarValue| RecordField::new(name, Value::Scalar(value));
    image
        .set_misc_info(RecordValue::new(vec![
            field("Xsupport", ScalarValue::Int32(plane.support as i32)),
            field("Ysupport", ScalarValue::Int32(plane.support as i32)),
            field("Sampling", ScalarValue::Float64(misc.sampling)),
            field("ParallacticAngle", ScalarValue::Float64(misc.pa_deg)),
            field("MuellerElement", ScalarValue::Int32(misc.mueller as i32)),
            field("WValue", ScalarValue::Float64(misc.w_value)),
            field("WIncr", ScalarValue::Float64(misc.w_increment)),
            field("Name", ScalarValue::String(name.to_string())),
            field("ConjFreq", ScalarValue::Float64(misc.conjugate_frequency)),
            field(
                "ConjPoln",
                ScalarValue::Int32(misc.conjugate_mueller as i32),
            ),
            field("TelescopeName", ScalarValue::String("EVLA".to_string())),
            field("BandName", ScalarValue::String(String::new())),
            field("Diameter", ScalarValue::Float64(misc.diameter_m)),
            field("OpCode", ScalarValue::Bool(false)),
            field(
                "CasaRsSkyIncrement",
                ScalarValue::Float64(misc.sky_increment_rad),
            ),
            field(
                "CasaRsWorkingSize",
                ScalarValue::Int32(
                    i32::try_from(misc.working_size)
                        .map_err(|_| write_error("working size exceeds i32".to_string()))?,
                ),
            ),
        ]))
        .map_err(|error| write_error(format!("cannot write misc info: {error}")))?;
    image
        .save()
        .map_err(|error| write_error(format!("cannot save: {error}")))?;
    Ok(())
}

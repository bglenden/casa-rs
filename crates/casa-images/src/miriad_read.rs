// SPDX-License-Identifier: LGPL-3.0-or-later

//! Read casacore-compatible MIRIAD image directories and bounded pixel sections.

use std::collections::BTreeMap;
use std::fs::{File, Metadata};
use std::io::{BufReader, ErrorKind, Read, Seek, SeekFrom};
use std::path::{Path, PathBuf};
use std::sync::Mutex;

use casa_coordinates::{
    CoordinateModel, CoordinateSystem, CoordinateType, FitsHeader, FitsValue, Projection,
    ProjectionType, StokesCoordinate, StokesType, fits::from_fits_header,
};
use casa_lattices::{Lattice, LatticeError};
use casa_types::{
    RecordValue,
    measures::{EpochRef, MEpoch},
    quanta::{Quantity, Unit},
};
use ndarray::{ArrayD, IxDyn, ShapeBuilder};

use crate::{
    GaussianBeam, ImageBeamSet, ImageError, ImageInfo, ImageInterface, ImageType,
    StoredGaussianBeam, fits_read::brightness_unit,
};

const MAX_AXES: usize = 7;
const CACHE_ENTRY: usize = 16;
const ITEM_TAG_BYTES: u64 = 4;
const FLOAT_BYTES: usize = size_of::<f32>();
const REAL_TAG: [u8; 4] = [0, 0, 0, 4];
const LIGHT_SPEED_M_S: f64 = 299_792_458.0;

/// A read-only MIRIAD Float image that retains its pixel stream after first use.
pub struct MiriadImage {
    path: PathBuf,
    shape: Vec<usize>,
    coordinates: CoordinateSystem,
    units: String,
    info: ImageInfo,
    pixels: Mutex<PixelFile>,
}

struct PixelFile {
    file: File,
    active: bool,
}

impl MiriadImage {
    /// Has the pair of external files required by casacore's image discriminator.
    pub fn recognizes(path: &Path) -> bool {
        path.join("header").is_file() && path.join("image").is_file()
    }

    /// Recognize an input after honoring native table and manifest precedence.
    ///
    /// `probe` is the supplied image path and `resolved` is the directory
    /// used to read a native table's control record.
    pub fn recognizes_input(probe: &Path, resolved: &Path) -> bool {
        Self::recognizes(probe)
            && !crate::concat_file::is_image_concat(probe)
            && !crate::expr_file::is_image_expr(probe)
            && (!resolved.join("table.dat").is_file()
                || casa_tables::Table::open_metadata_only(casa_tables::TableOptions::new(resolved))
                    .is_err())
    }

    /// Whether a file can supply pixels or metadata to this reader.
    pub fn is_image_file(name: &str) -> bool {
        matches!(name, "header" | "image") || is_image_keyword(name)
    }

    /// Read the image metadata and verify the external pixel file.
    ///
    /// # Errors
    /// The dataset has invalid or unsupported metadata, a short payload, or
    /// either of the required files is unreadable.
    pub fn open(path: &Path) -> Result<Self, ImageError> {
        if !Self::recognizes(path) {
            return Err(malformed(
                "a MIRIAD image requires regular header and image files",
            ));
        }
        let header = Items::read(path)?;
        let rank = header
            .integer("naxis")?
            .and_then(|rank| usize::try_from(rank).ok())
            .filter(|rank| (1..=MAX_AXES).contains(rank))
            .ok_or_else(|| malformed("invalid MIRIAD image rank"))?;
        let shape = (1..=rank)
            .map(|axis| {
                header
                    .integer(&format!("naxis{axis}"))?
                    .and_then(|size| usize::try_from(size).ok())
                    .filter(|&size| size > 0)
                    .ok_or_else(|| malformed(format!("invalid MIRIAD dimension on axis {axis}")))
            })
            .collect::<Result<Vec<_>, _>>()?;
        let extent = shape
            .iter()
            .try_fold(FLOAT_BYTES as u64, |bytes, &size| {
                bytes.checked_mul(size as u64)
            })
            .and_then(|bytes| bytes.checked_add(ITEM_TAG_BYTES))
            .ok_or_else(|| malformed("MIRIAD image is too large"))?;
        let mut pixels = File::open(path.join("image")).map_err(io_error)?;
        let mut tag = [0; ITEM_TAG_BYTES as usize];
        pixels.read_exact(&mut tag).map_err(io_error)?;
        let pixel_identity = pixels.metadata().map_err(io_error)?;
        if tag != REAL_TAG || pixel_identity.len() < extent {
            return Err(malformed("invalid MIRIAD Float pixel payload"));
        }

        let (wcs, stokes) = header.wcs(&shape)?;
        let mut coordinates = from_fits_header(&wcs, &shape)?;
        if let Some(index) = coordinates.find_coordinate(CoordinateType::Direction)
            && let CoordinateModel::Direction(direction) = coordinates.coordinate(index)
        {
            // Casacore represents NCP as slant SIN and retains both SIN
            // parameters even when the projection has no slant.
            let slant = match direction.projection().projection_type() {
                ProjectionType::SIN => Some(0.),
                ProjectionType::NCP => Some(direction.projection_reference()[1].tan().recip()),
                _ => None,
            };
            if let Some(slant) = slant {
                let projection = Projection::with_parameters(ProjectionType::SIN, vec![0., slant]);
                let rebuilt = direction.with_rebuilt_projection(projection)?;
                coordinates.replace_coordinate(index, rebuilt);
            }
        }
        if let Some(stokes) = stokes {
            let index = coordinates
                .find_coordinate(CoordinateType::Stokes)
                .ok_or_else(|| {
                    malformed("MIRIAD Stokes axis missing from the coordinate system")
                })?;
            coordinates.replace_coordinate(index, stokes);
        }
        if coordinates
            .find_coordinate(CoordinateType::Linear)
            .is_some()
        {
            // Casacore exposes only the axes it decoded from MIRIAD. The
            // stored pixel shape is still available when a coordinate is absent.
            let mut decoded = CoordinateSystem::new();
            for kind in [
                CoordinateType::Direction,
                CoordinateType::Spectral,
                CoordinateType::Stokes,
            ] {
                if let Some(index) = coordinates.find_coordinate(kind) {
                    decoded.add_coordinate(coordinates.coordinate(index).clone());
                }
            }
            coordinates = decoded;
        }
        let observation = coordinates.obs_info_mut();
        observation.telescope = header.text("telescop")?;
        let obstime = header.number("obstime", -1.)?;
        if obstime > -1. {
            observation.date = Some(MEpoch::from_mjd(obstime - 2_400_000.5, EpochRef::UTC));
        }
        let units = brightness_unit(&wcs);
        let units = if Unit::new(&units).is_ok() {
            units
        } else {
            String::new()
        };
        let mut info = ImageInfo {
            image_type: header
                .text("btype")?
                .parse()
                .unwrap_or(ImageType::Undefined),
            ..ImageInfo::default()
        };
        let major = header.number("bmaj", 0.)?;
        let minor = header.number("bmin", 0.)?;
        let angle = header.number("bpa", 0.)?;
        if major > 0. && minor > 0. && angle.abs() > 0. {
            StoredGaussianBeam::new(
                Quantity::new(major, "rad").expect("built-in radians"),
                Quantity::new(minor, "rad").expect("built-in radians"),
                Quantity::new(angle, "deg").expect("built-in degrees"),
            )?;
            info.beam_set = ImageBeamSet::new(GaussianBeam::new(major, minor, angle.to_radians()));
        }
        Ok(Self {
            path: path.to_path_buf(),
            shape,
            coordinates,
            units,
            info,
            pixels: Mutex::new(PixelFile {
                file: pixels,
                active: false,
            }),
        })
    }

    /// Read only the requested pixels, with the first file axis varying fastest.
    ///
    /// # Errors
    /// The selection is outside the image or the pixel file cannot be read.
    pub fn read(
        &self,
        start: &[usize],
        count: &[usize],
        stride: &[usize],
    ) -> Result<ArrayD<f32>, ImageError> {
        let invalid = || malformed("MIRIAD pixel section is outside the image");
        let rank = self.shape.len();
        if start.len() != rank || count.len() != rank || stride.len() != rank {
            return Err(invalid());
        }
        for axis in 0..rank {
            let last = count[axis]
                .checked_sub(1)
                .and_then(|steps| steps.checked_mul(stride[axis]))
                .and_then(|delta| start[axis].checked_add(delta));
            if stride[axis] == 0
                || (count[axis] > 0 && last.is_none_or(|last| last >= self.shape[axis]))
                || (count[axis] == 0 && start[axis] > self.shape[axis])
            {
                return Err(invalid());
            }
        }
        let size = count.iter().try_fold(1usize, |size, &length| {
            size.checked_mul(length).ok_or_else(invalid)
        })?;
        let mut values = Vec::new();
        values
            .try_reserve_exact(size)
            .map_err(|_| malformed("MIRIAD section is too large"))?;
        if size == 0 {
            return ArrayD::from_shape_vec(IxDyn(count).f(), values).map_err(|_| invalid());
        }

        let mut stream = self
            .pixels
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if !stream.active {
            let path = self.path.join("image");
            let open_error =
                |error| ImageError::Io(format!("{} could not be opened: {error}", path.display()));
            let changed = || {
                malformed(format!(
                    "the MIRIAD pixel file {} has changed",
                    path.display()
                ))
            };
            let identity = stream.file.metadata().map_err(io_error)?;
            if !same_pixel_file(&identity, &path.metadata().map_err(open_error)?) {
                return Err(changed());
            }
            let file = File::open(&path).map_err(open_error)?;
            if !same_pixel_file(&identity, &file.metadata().map_err(io_error)?) {
                return Err(changed());
            }
            stream.file = file;
            stream.active = true;
        }
        let reader = &mut stream.file;
        let row_count = size / count[0];
        let mut packed = [0; 16 * 1024];
        for row in 0..row_count {
            let mut offset = start[0] as u64;
            let mut index = row;
            let mut axis_stride = self.shape[0] as u64;
            for axis in 1..rank {
                let pixel = start[axis] + index % count[axis] * stride[axis];
                offset += pixel as u64 * axis_stride;
                index /= count[axis];
                axis_stride *= self.shape[axis] as u64;
            }
            let byte = ITEM_TAG_BYTES + offset * FLOAT_BYTES as u64;
            if stride[0] == 1 {
                reader.seek(SeekFrom::Start(byte)).map_err(io_error)?;
                let block = packed.len() / FLOAT_BYTES;
                for start in (0..count[0]).step_by(block) {
                    let bytes = (count[0] - start).min(block) * FLOAT_BYTES;
                    reader.read_exact(&mut packed[..bytes]).map_err(io_error)?;
                    let (pixels, _) = packed[..bytes].as_chunks::<FLOAT_BYTES>();
                    values.extend(pixels.iter().copied().map(f32::from_be_bytes));
                }
            } else {
                let mut pixel = [0; FLOAT_BYTES];
                for column in 0..count[0] {
                    let position = byte + (column * stride[0]) as u64 * FLOAT_BYTES as u64;
                    reader.seek(SeekFrom::Start(position)).map_err(io_error)?;
                    reader.read_exact(&mut pixel).map_err(io_error)?;
                    values.push(f32::from_be_bytes(pixel));
                }
            }
        }
        ArrayD::from_shape_vec(IxDyn(count).f(), values).map_err(|_| invalid())
    }
}

impl Lattice<f32> for MiriadImage {
    fn shape(&self) -> &[usize] {
        &self.shape
    }

    fn is_persistent(&self) -> bool {
        true
    }

    fn is_paged(&self) -> bool {
        true
    }

    fn is_writable(&self) -> bool {
        false
    }

    fn get_at(&self, position: &[usize]) -> Result<f32, LatticeError> {
        let ones = vec![1; self.shape.len()];
        let section = self.get_slice(position, &ones, &ones)?;
        Ok(*section.first().expect("read one MIRIAD pixel"))
    }

    fn get_slice(
        &self,
        start: &[usize],
        shape: &[usize],
        stride: &[usize],
    ) -> Result<ArrayD<f32>, LatticeError> {
        self.read(start, shape, stride)
            .map_err(|error| LatticeError::Table(error.to_string()))
    }

    fn nice_cursor_shape(&self) -> Vec<usize> {
        let mut shape = vec![1; self.shape.len()];
        shape[0] = self.shape[0].min(4096);
        shape
    }
}

impl ImageInterface<f32> for MiriadImage {
    fn as_any(&self) -> Option<&dyn std::any::Any> {
        Some(self)
    }

    fn coordinates(&self) -> &CoordinateSystem {
        &self.coordinates
    }

    fn units(&self) -> &str {
        &self.units
    }

    fn misc_info(&self) -> RecordValue {
        RecordValue::default()
    }

    fn image_info(&self) -> Result<ImageInfo, ImageError> {
        Ok(self.info.clone())
    }

    fn name(&self) -> Option<&Path> {
        Some(&self.path)
    }
}

struct Items<'a> {
    directory: &'a Path,
    cached: BTreeMap<String, Vec<u8>>,
}

impl<'a> Items<'a> {
    fn read(directory: &'a Path) -> Result<Self, ImageError> {
        let file = File::open(directory.join("header")).map_err(io_error)?;
        let length = file.metadata().map_err(io_error)?.len();
        let mut header = BufReader::new(file);
        let mut cached = BTreeMap::new();
        let mut position = 0;
        let mut entry = [0; CACHE_ENTRY];
        while position < length {
            header.read_exact(&mut entry).map_err(io_error)?;
            let field = &entry[..CACHE_ENTRY - 1];
            let name_end = field.iter().position(|&b| b == 0).unwrap_or(field.len());
            let size = entry[CACHE_ENTRY - 1] as usize;
            let mut value = [0; u8::MAX as usize];
            header.read_exact(&mut value[..size]).map_err(io_error)?;
            if let Ok(name) = std::str::from_utf8(&field[..name_end])
                && is_image_keyword(name)
            {
                cached.insert(name.to_owned(), value[..size].to_vec());
            }
            position += (CACHE_ENTRY + size.next_multiple_of(CACHE_ENTRY)) as u64;
            // The last item can end at EOF before its cache padding.
            if position < length {
                header.seek(SeekFrom::Start(position)).map_err(io_error)?;
            }
        }
        Ok(Self { directory, cached })
    }

    fn item(&self, name: &str) -> Result<Option<Vec<u8>>, ImageError> {
        if let Some(bytes) = self.cached.get(name) {
            return Ok(Some(bytes.clone()));
        }
        let path = self.directory.join(name);
        match path.metadata() {
            Ok(metadata) if metadata.is_file() => {}
            Ok(_) => return Ok(None),
            Err(error) if error.kind() == ErrorKind::NotFound => return Ok(None),
            Err(error) => return Err(io_error(error)),
        }
        let file = File::open(path).map_err(io_error)?;
        let mut item = Vec::new();
        file.take(u8::MAX as u64 + 1)
            .read_to_end(&mut item)
            .map_err(io_error)?;
        Ok(Some(item))
    }

    fn integer(&self, name: &str) -> Result<Option<i32>, ImageError> {
        Ok(match self.item(name)?.as_deref() {
            Some([0, 0, 0, 2, value @ ..]) if value.len() == 4 => {
                Some(i32::from_be_bytes(value.try_into().expect("four bytes")))
            }
            Some([0, 0, 0, 8, _, _, _, _, value @ ..]) if value.len() == 8 => {
                i32::try_from(i64::from_be_bytes(value.try_into().expect("eight bytes"))).ok()
            }
            _ => None,
        })
    }

    fn number(&self, name: &str, default: f64) -> Result<f64, ImageError> {
        Ok(match self.item(name)?.as_deref() {
            Some([0, 0, 0, 2, value @ ..]) if value.len() == 4 => {
                f64::from(i32::from_be_bytes(value.try_into().expect("four bytes")))
            }
            Some([0, 0, 0, 4, value @ ..]) if value.len() == 4 => {
                f64::from(f32::from_be_bytes(value.try_into().expect("four bytes")))
            }
            Some([0, 0, 0, 5, _, _, _, _, value @ ..]) if value.len() == 8 => {
                f64::from_be_bytes(value.try_into().expect("eight bytes"))
            }
            _ => default,
        })
    }

    fn text(&self, name: &str) -> Result<String, ImageError> {
        Ok(match self.item(name)?.as_deref() {
            Some([0, 0, 0, 1, value @ ..]) => {
                let end = value
                    .iter()
                    .take(63)
                    .position(|&b| b == 0)
                    .unwrap_or(value.len().min(63));
                String::from_utf8_lossy(&value[..end]).into_owned()
            }
            _ => String::new(),
        })
    }

    fn wcs(&self, shape: &[usize]) -> Result<(FitsHeader, Option<StokesCoordinate>), ImageError> {
        let mut header = FitsHeader::new();
        let mut stokes = None;
        header.set("NAXIS", FitsValue::Integer(shape.len() as i64));
        let epoch = self.number("epoch", 2000.)?;
        if (epoch - 1950.).abs() <= 1950. * 1e-13 {
            header.set("EQUINOX", FitsValue::Float(1950.));
            header.set("RADESYS", FitsValue::String("FK4".into()));
        }
        header.set("BUNIT", FitsValue::String(self.text("bunit")?));
        let axes = (1..=shape.len())
            .map(|axis| self.text(&format!("ctype{axis}")))
            .collect::<Result<Vec<_>, _>>()?;
        let last_stokes = axes.iter().rposition(|kind| kind.contains("STOKES"));
        let galactic = axes
            .iter()
            .rev()
            .find(|kind| sky_axis(kind) == Some(SkyAxis::Longitude))
            .is_some_and(|kind| kind.contains("GLON"));
        for (index, mut kind) in axes.into_iter().enumerate() {
            let axis = index + 1;
            header.set(
                format!("NAXIS{axis}"),
                FitsValue::Integer(shape[axis - 1] as i64),
            );
            if !kind.is_ascii() {
                return Err(malformed(format!("CTYPE{axis} must be ASCII")));
            }
            let mut pix = self.number(&format!("crpix{axis}"), 0.)?;
            let mut step = self.number(&format!("cdelt{axis}"), 0.)?;
            let mut value = self.number(&format!("crval{axis}"), 0.)?;
            let unit = if kind.contains("STOKES") {
                kind.clear();
                if Some(index) == last_stokes {
                    stokes = miriad_stokes(shape[index], pix, step, value)?;
                    if stokes.is_some() {
                        // Let FITS assemble the axis mapping, then restore the
                        // actual MIRIAD codes after coordinate parsing.
                        kind = "STOKES".into();
                        (pix, step, value) = (1., 1., 1.);
                    }
                }
                ""
            } else if let Some(axis) = sky_axis(&kind) {
                let projection = kind
                    .get(4..)
                    .unwrap_or_default()
                    .trim_start_matches('-')
                    .replace(' ', "");
                kind.clear();
                if projection.is_empty() {
                    ""
                } else {
                    let prefix = match (galactic, axis) {
                        (true, SkyAxis::Longitude) => "GLON-",
                        (true, SkyAxis::Latitude) => "GLAT-",
                        (false, SkyAxis::Longitude) => "RA---",
                        (false, SkyAxis::Latitude) => "DEC--",
                    };
                    kind.push_str(prefix);
                    kind.push_str(if projection == "GLS" {
                        "SFL"
                    } else {
                        &projection
                    });
                    if step == 0. && (pix - 1.).abs() <= 1e-13 {
                        step = 1.;
                    }
                    if step == 0. {
                        return Err(singular_wcs());
                    }
                    step = step.to_degrees();
                    value = value.to_degrees();
                    "deg"
                }
            } else if kind.starts_with("FREQ") {
                if step == 0. {
                    return Err(singular_wcs());
                }
                header.set(
                    "SPECSYS",
                    FitsValue::String(legacy_spectral_frame(&kind).into()),
                );
                let rest = self.number("restfreq", -1.)?;
                if rest >= 0. {
                    header.set("RESTFRQ", FitsValue::Float(rest * 1e9));
                }
                step *= 1e9;
                value *= 1e9;
                "Hz"
            } else if kind.starts_with("VELO") || kind.starts_with("FELO") {
                let rest = self.number("restfreq", -1.)? * 1e9;
                if rest <= 0. || step == 0. {
                    return Err(singular_wcs());
                }
                let velocity = value * 1e3;
                let velocity_step = step * 1e3;
                if kind.starts_with("VELO") {
                    value = rest * (1. - velocity / LIGHT_SPEED_M_S);
                    step = -velocity_step * rest / LIGHT_SPEED_M_S;
                } else {
                    // MIRIAD uses the tangent to the optical Doppler law at CRPIX.
                    value = rest / (1. + velocity / LIGHT_SPEED_M_S);
                    step = -velocity_step * value / (LIGHT_SPEED_M_S + velocity);
                }
                header.set(
                    "SPECSYS",
                    FitsValue::String(legacy_spectral_frame(&kind).into()),
                );
                header.set("RESTFRQ", FitsValue::Float(rest));
                kind.replace_range(..4, "FREQ");
                "Hz"
            } else {
                // MIRIAD leaves other axes undecoded. The FITS bridge must
                // not interpret their names as FITS-specific coordinate types.
                kind.clear();
                ""
            };
            header.set(format!("CTYPE{axis}"), FitsValue::String(kind));
            header.set(format!("CRPIX{axis}"), FitsValue::Float(pix));
            header.set(format!("CDELT{axis}"), FitsValue::Float(step));
            header.set(format!("CRVAL{axis}"), FitsValue::Float(value));
            header.set(format!("CUNIT{axis}"), FitsValue::String(unit.into()));
        }
        Ok((header, stokes))
    }
}

#[derive(PartialEq, Eq)]
enum SkyAxis {
    Longitude,
    Latitude,
}

fn sky_axis(kind: &str) -> Option<SkyAxis> {
    if kind.starts_with("RA") || kind.starts_with("LL") || kind.contains("LON") {
        Some(SkyAxis::Longitude)
    } else if kind.starts_with("DEC")
        || kind.contains("LAT")
        || kind.get(..3).is_some_and(|head| head.contains("MM"))
    {
        Some(SkyAxis::Latitude)
    } else {
        None
    }
}

fn miriad_stokes(
    planes: usize,
    reference_pixel: f64,
    increment: f64,
    reference_value: f64,
) -> Result<Option<StokesCoordinate>, ImageError> {
    if planes > 4 {
        return Err(ImageError::CasaSelection(
            "Stokes axis longer than 4 pixels.  This is not acceptable".into(),
        ));
    }
    let mut stokes = Vec::with_capacity(planes);
    for pixel in 1..=planes {
        let value = reference_value + (pixel as f64 - reference_pixel) * increment;
        if !value.is_finite() {
            return Err(malformed("non-finite MIRIAD Stokes coordinate"));
        }
        let code = (value + 0.01_f64.copysign(value)) as i32;
        let polarization = match code {
            -8 => StokesType::YX,
            -7 => StokesType::XY,
            -6 => StokesType::YY,
            -5 => StokesType::XX,
            -4 => StokesType::LR,
            -3 => StokesType::RL,
            -2 => StokesType::LL,
            -1 => StokesType::RR,
            1 => StokesType::I,
            2 => StokesType::Q,
            3 => StokesType::U,
            4 => StokesType::V,
            5 | 6 => StokesType::PFlinear,
            7 => StokesType::Pangle,
            _ => StokesType::Undefined,
        };
        if stokes.contains(&polarization) {
            return Ok(None);
        }
        stokes.push(polarization);
    }
    Ok(Some(StokesCoordinate::new(stokes)))
}

fn singular_wcs() -> ImageError {
    ImageError::CasaSelection("wcs wcsset_error: Linear transformation matrix is singular".into())
}

fn legacy_spectral_frame(kind: &str) -> &'static str {
    let qualifier: String = kind
        .get(5..)
        .unwrap_or_default()
        .chars()
        .filter(|character| !matches!(character, '-' | ' '))
        .collect();
    match qualifier.as_str() {
        "" | "HEL" => "BARYCENT",
        "LSR" | "LSRK" => "LSRK",
        "LSD" => "LSRD",
        "GEO" => "GEOCENTR",
        "SOU" | "REST" => "SOURCE",
        "GAL" => "GALACTOC",
        _ => "TOPOCENT",
    }
}

fn is_image_keyword(name: &str) -> bool {
    matches!(
        name,
        "naxis"
            | "epoch"
            | "bunit"
            | "restfreq"
            | "telescop"
            | "obstime"
            | "btype"
            | "bmaj"
            | "bmin"
            | "bpa"
    ) || ["naxis", "ctype", "crpix", "cdelt", "crval"]
        .iter()
        .any(|stem| {
            name.strip_prefix(stem)
                .and_then(|suffix| suffix.parse::<usize>().ok())
                .is_some_and(|axis| (1..=MAX_AXES).contains(&axis))
        })
}

fn malformed(message: impl Into<String>) -> ImageError {
    ImageError::InvalidMetadata(message.into())
}

fn same_pixel_file(expected: &Metadata, actual: &Metadata) -> bool {
    if !actual.is_file() {
        return false;
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;
        expected.dev() == actual.dev() && expected.ino() == actual.ino()
    }
    #[cfg(not(unix))]
    {
        expected.created().ok() == actual.created().ok()
    }
}

fn io_error(error: std::io::Error) -> ImageError {
    ImageError::Io(error.to_string())
}

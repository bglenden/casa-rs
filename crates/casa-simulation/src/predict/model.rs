// SPDX-License-Identifier: LGPL-3.0-or-later
//! The model image `sm.predict` reads: its header at open, its planes one at
//! a time as they are prepared.

use std::path::{Path, PathBuf};
use std::sync::{Mutex, MutexGuard, PoisonError};

use casa_coordinates::{CoordinateModel, CoordinateSystem, CoordinateType};
use casa_images::PagedImage;
use casa_imaging_model::PolarizationCoordinate;
use casa_types::measures::direction::DirectionRef;
use casa_types::measures::{MeasFrame, direction::MDirection};
use casa_types::quanta::{Quantity, Unit};
use ndarray::{Array2, IxDyn};

use super::PredictError;

/// The brightness unit `sm.predict` reads the pixels in.
const JY_PER_PIXEL: &str = "Jy/pixel";

/// The tile cache a model is opened with, bytes: casa-tables' default.
const MODEL_CACHE_BYTES: usize = 64 << 20;

/// A CASA model image as `sm.predict` reads it: Stokes I/Q/U/V planes in
/// Jy/pixel, with the direction of pixel `(nx/2, ny/2)` and the frequency of
/// every plane.
///
/// Opening reads the header; the pixels are read a plane at a time while
/// the model is prepared, so preparing a cube holds one plane per worker
/// rather than the cube. A model prepared again later, for each field
/// under a primary beam, is read once and holds its planes instead. The
/// image is never written: CASA's `predict` rewrites the model's spectral
/// units and frame in place, casa-rs reads them as they are.
pub struct SkyModel {
    path: PathBuf,
    /// Where the planes are read from.
    pixels: Pixels,
    /// `[nx, ny]` pixels.
    shape: [usize; 2],
    /// Model planes.
    planes: usize,
    /// Stokes coordinates in stored image-plane order.
    polarizations: Vec<PolarizationCoordinate>,
    /// Signed radians per pixel `[Δx, Δy]`.
    pub(super) increment_rad: [f64; 2],
    /// Context for interpreting the native direction frame.
    frame: MeasFrame,
    /// The tangent in the direction coordinate's native reference.
    tangent: MDirection,
    /// Frequency of plane 0 in Hz.
    pub(super) first_frequency_hz: f64,
    /// Frequency step between planes in Hz.
    pub(super) frequency_increment_hz: f64,
    /// The image's coordinate system, which places the pointing (the beam
    /// centre) on the model's pixels.
    coordinates: CoordinateSystem,
    /// Plane 0's spectral world value in the coordinate system's own unit.
    first_spectral_world: f64,
    /// The most the image's tile cache holds once a plane is read
    /// ([`Self::cache_bytes`]).
    cache_bytes: u64,
    /// Optional finite focus distance from the image's miscellaneous record.
    pub(crate) distance_m: f64,
}

/// Where a model's planes come from.
enum Pixels {
    /// The open image; planes are read under the lock, one reader at a
    /// time.
    Image(Box<Mutex<InputImage>>),
    /// Every plane, `[y][x]`, read once ([`SkyModel::held`]).
    Held(Vec<Array2<f32>>),
}

struct InputImage {
    image: PagedImage<f32>,
    payload_cache: bool,
}

impl std::fmt::Debug for SkyModel {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("SkyModel")
            .field("path", &self.path)
            .field("shape", &self.shape)
            .field("planes", &self.planes)
            .field("increment_rad", &self.increment_rad)
            .field("tangent", &self.tangent)
            .field("first_frequency_hz", &self.first_frequency_hz)
            .field("frequency_increment_hz", &self.frequency_increment_hz)
            .finish_non_exhaustive()
    }
}

impl SkyModel {
    pub(super) fn direction_coordinate(&self) -> &casa_coordinates::DirectionCoordinate {
        let CoordinateModel::Direction(direction) = self.coordinates.coordinate(0) else {
            unreachable!()
        };
        direction
    }

    pub(super) fn native_pointing(&self, pointing: [f64; 2]) -> Result<[f64; 2], PredictError> {
        let (ra, dec) = MDirection::from_angles(pointing[0], pointing[1], DirectionRef::J2000)
            .convert_to(self.tangent.refer(), &self.frame)
            .map_err(|e| casa_coordinates::CoordinateError::ConversionFailed(e.to_string()))?
            .as_angles();
        Ok([ra, dec])
    }

    /// Open a CASA image with axes `[direction x, direction y, Stokes,
    /// spectral]`, with Stokes I/Q/U/V planes, as `simobserve` writes its sky
    /// model, and read its header.
    ///
    /// The spectral axis may be in any frequency unit; it is normalized to
    /// Hz, as `Simulator::predict` normalizes it before it builds the sky
    /// equation. A brightness unit other than Jy/pixel is logged and the
    /// pixels read as Jy/pixel, as CASA reads them.
    ///
    /// # Errors
    ///
    /// An unreadable image, one whose axes are not those above, a direction
    /// coordinate that cannot be read, or a spectral axis not in frequency units.
    pub fn open(path: impl AsRef<Path>) -> Result<Self, PredictError> {
        let path = path.as_ref();
        let image =
            // An unbounded flat cache is lazy: header inspection allocates no
            // pixel payload. A bounded LRU eagerly allocates its full slab.
            // The first admitted read below opens the bounded payload cache.
            PagedImage::<f32>::open_with_cache(path, usize::MAX).map_err(|source| {
                PredictError::Image {
                    path: path.display().to_string(),
                    source,
                }
            })?;
        let shape = image.shape().to_vec();
        let mut coordinates = image.coordinates().clone();
        let unsupported = |reason: String| Err(PredictError::UnsupportedModel(reason));
        if shape.len() != 4
            || coordinates.find_coordinate(CoordinateType::Direction) != Some(0)
            || coordinates.find_coordinate(CoordinateType::Stokes) != Some(1)
            || coordinates.find_coordinate(CoordinateType::Spectral) != Some(2)
        {
            return Err(PredictError::InvalidModel(
                "axes must be direction, Stokes and spectral, in that order".into(),
            ));
        }
        if !(1..=4).contains(&shape[2]) {
            return unsupported("the model must contain at most four Stokes I/Q/U/V planes".into());
        }
        let polarizations = (0..shape[2])
            .map(|plane| {
                let code = coordinates.coordinate(1).to_world(&[plane as f64])?[0];
                Ok(match code as i32 {
                    1 => PolarizationCoordinate::StokesI,
                    2 => PolarizationCoordinate::StokesQ,
                    3 => PolarizationCoordinate::StokesU,
                    4 => PolarizationCoordinate::StokesV,
                    _ => {
                        return Err(PredictError::UnsupportedModel(
                            "the model must contain Stokes I/Q/U/V planes".into(),
                        ));
                    }
                })
            })
            .collect::<Result<Vec<_>, PredictError>>()?;
        // A conversion layer changes displayed world coordinates, not the
        // physical pixel grid. DirectionCoordinate's MDirection overload in
        // CASA incorrectly relabels converted values with the native type.
        if let CoordinateModel::Direction(direction) = coordinates.coordinate(0) {
            let native = direction.direction_ref();
            coordinates
                .replace_coordinate(0, direction.clone().with_conversion_direction_ref(native));
        }
        let spectral_unit = coordinates.coordinate(2).axis_units()[0].clone();
        let Some(hz_per_unit) = hz_per_unit(&spectral_unit) else {
            return unsupported(format!(
                "the spectral axis is in {spectral_unit:?}, not a frequency unit"
            ));
        };
        if image.units() != JY_PER_PIXEL {
            tracing::warn!(
                "the brightness unit of {} is {:?}; sm.predict reads its pixels as {JY_PER_PIXEL}",
                path.display(),
                image.units()
            );
        }
        let [nx, ny, planes] = [shape[0], shape[1], shape[3]];
        let cache_bytes = cache_bound(&shape, image.tile_shape());
        let increment = coordinates.coordinate(0).increment();
        let centre = coordinates.to_world(&[(nx / 2) as f64, (ny / 2) as f64, 0.0, 0.0])?;
        let CoordinateModel::Direction(direction) = coordinates.coordinate(0) else {
            unreachable!()
        };
        let tangent =
            MDirection::from_angles(centre[0], centre[1], direction.conversion_direction_ref());
        let distance_m = match image.misc_info().get("distance") {
            Some(casa_types::Value::Scalar(casa_types::ScalarValue::Float64(value))) => *value,
            Some(casa_types::Value::Scalar(casa_types::ScalarValue::Float32(value))) => {
                f64::from(*value)
            }
            Some(casa_types::Value::Scalar(casa_types::ScalarValue::Int64(value))) => *value as f64,
            Some(casa_types::Value::Scalar(casa_types::ScalarValue::Int32(value))) => {
                f64::from(*value)
            }
            Some(casa_types::Value::Scalar(casa_types::ScalarValue::Int16(value))) => {
                f64::from(*value)
            }
            Some(casa_types::Value::Scalar(casa_types::ScalarValue::UInt32(value))) => {
                f64::from(*value)
            }
            Some(casa_types::Value::Scalar(casa_types::ScalarValue::UInt16(value))) => {
                f64::from(*value)
            }
            Some(casa_types::Value::Scalar(casa_types::ScalarValue::UInt8(value))) => {
                f64::from(*value)
            }
            Some(_) => {
                return Err(PredictError::InvalidModel(
                    "image distance must be numeric".into(),
                ));
            }
            None => 0.,
        };
        Ok(Self {
            path: path.to_path_buf(),
            pixels: Pixels::Image(Box::new(Mutex::new(InputImage {
                image,
                payload_cache: false,
            }))),
            shape: [nx, ny],
            planes,
            polarizations,
            increment_rad: [increment[0], increment[1]],
            frame: MeasFrame::new(),
            tangent,
            first_frequency_hz: centre[3] * hz_per_unit,
            frequency_increment_hz: coordinates.coordinate(2).increment()[0] * hz_per_unit,
            coordinates,
            first_spectral_world: centre[3],
            cache_bytes,
            distance_m,
        })
    }

    /// The most the image's tile cache holds once a plane has been read,
    /// bytes, until the model is dropped. casa-tables keeps every tile when
    /// they all fit the cache, and otherwise as many as fit, at least one;
    /// a cube tiled across many planes caches far more than a plane.
    pub(crate) fn cache_bytes(&self) -> u64 {
        self.cache_bytes
    }

    /// The image's path.
    #[must_use]
    pub fn path(&self) -> &Path {
        &self.path
    }

    /// `[nx, ny]` pixels.
    #[must_use]
    pub fn shape(&self) -> [usize; 2] {
        self.shape
    }

    /// J2000 direction of pixel `(nx/2, ny/2)`, CASA's tangent point.
    ///
    /// # Errors
    ///
    /// A direction conversion requires context not supplied by [`Self::with_frame`].
    pub fn centre_rad(&self) -> Result<[f64; 2], PredictError> {
        let (ra, dec) = self
            .tangent
            .convert_to(DirectionRef::J2000, &self.frame)
            .map_err(|error| {
                casa_coordinates::CoordinateError::ConversionFailed(error.to_string())
            })?
            .as_angles();
        Ok([ra, dec])
    }

    /// Set the epoch/position context used to interpret native image directions.
    /// The simulator supplies its selected visibility-buffer context before
    /// preparing beams; opening an image alone only reads its header.
    #[must_use]
    pub fn with_frame(mut self, frame: MeasFrame) -> Self {
        self.frame = frame;
        self
    }

    /// The centre pixel's direction in the image's native reference.
    /// Use it with `GridFtProjection` to rotate stored J2000 UVW into the
    /// image's tangent basis before predicting a row.
    pub fn tangent_direction(&self) -> &MDirection {
        &self.tangent
    }

    /// Number of model planes.
    #[must_use]
    pub fn planes(&self) -> usize {
        self.planes
    }

    pub(crate) fn polarizations(&self) -> &[PolarizationCoordinate] {
        &self.polarizations
    }

    pub(crate) fn is_stokes_i_only(&self) -> bool {
        self.polarizations == [PolarizationCoordinate::StokesI]
    }

    /// Frequency of model plane `plane`, in Hz.
    pub(super) fn plane_frequency_hz(&self, plane: usize) -> f64 {
        self.first_frequency_hz + plane as f64 * self.frequency_increment_hz
    }

    /// Fractional pixel `[x, y]` of a J2000 direction.
    pub(crate) fn pixel_of(&self, direction_rad: [f64; 2]) -> Result<[f64; 2], PredictError> {
        self.pixel_of_with_frame(direction_rad, &self.frame)
    }

    pub(crate) fn pixel_of_with_frame(
        &self,
        direction_rad: [f64; 2],
        frame: &MeasFrame,
    ) -> Result<[f64; 2], PredictError> {
        let direction = if self.tangent.refer() == DirectionRef::J2000 {
            (direction_rad[0], direction_rad[1])
        } else {
            MDirection::from_angles(direction_rad[0], direction_rad[1], DirectionRef::J2000)
                .convert_to(self.tangent.refer(), frame)
                .map_err(|error| {
                    casa_coordinates::CoordinateError::ConversionFailed(error.to_string())
                })?
                .as_angles()
        };
        let pixel = self.coordinates.to_pixel(&[
            direction.0,
            direction.1,
            self.coordinates.coordinate(1).to_world(&[0.])?[0],
            self.first_spectral_world,
        ])?;
        Ok([pixel[0], pixel[1]])
    }

    /// The offset of a J2000 pointing from pixel `(nx/2, ny/2)` along the
    /// image axes, radians (its pixel offset times the increments), as the
    /// mosaic machine measures it (`HetArrayConvFunc::findConvFunction`'s
    /// `pixFieldDir`), and whether its pixel, truncated as `checkPBOfField`
    /// truncates it, lies inside the model. Common beams use the offset outside too;
    /// heterogeneous-array mosaics suppress those pointings.
    pub(crate) fn pointing_offset_and_coverage(
        &self,
        direction_rad: [f64; 2],
    ) -> Result<([f64; 2], bool), PredictError> {
        let pixel = self.pixel_of(direction_rad)?;
        let inside = |pixel: f64, n: usize| (0.0..n as f64).contains(&pixel.trunc());
        Ok((
            [
                (pixel[0] - (self.shape[0] / 2) as f64) * self.increment_rad[0],
                (pixel[1] - (self.shape[1] / 2) as f64) * self.increment_rad[1],
            ],
            inside(pixel[0], self.shape[0]) && inside(pixel[1], self.shape[1]),
        ))
    }

    pub(super) fn read_polarizations(
        &self,
        plane: usize,
    ) -> Result<Vec<Array2<f32>>, PredictError> {
        let count = self.polarizations.len();
        if count == 1 {
            return Ok(vec![self.read_polarized_plane(plane, 0)?]);
        }
        if let Pixels::Held(planes) = &self.pixels {
            return Ok(planes[plane * count..(plane + 1) * count].to_vec());
        }
        let Pixels::Image(image) = &self.pixels else {
            unreachable!()
        };
        let [nx, ny] = self.shape;
        let pixels = self
            .payload_image(image)?
            .image
            .get_slice(&[0, 0, 0, plane], &[nx, ny, count, 1])
            .map_err(|source| PredictError::Image {
                path: self.path.display().to_string(),
                source,
            })?;
        Ok((0..count)
            .map(|polarization| {
                Array2::from_shape_fn((ny, nx), |(y, x)| pixels[IxDyn(&[x, y, polarization, 0])])
            })
            .collect())
    }

    /// Open the bounded payload cache only after the caller admits its
    /// preparation. Header validation must not allocate the eager LRU slab.
    fn payload_image<'a>(
        &self,
        image: &'a Mutex<InputImage>,
    ) -> Result<MutexGuard<'a, InputImage>, PredictError> {
        let mut input = image.lock().unwrap_or_else(PoisonError::into_inner);
        if !input.payload_cache {
            input.image =
                PagedImage::open_with_cache(&self.path, MODEL_CACHE_BYTES).map_err(|source| {
                    PredictError::Image {
                        path: self.path.display().to_string(),
                        source,
                    }
                })?;
            input.payload_cache = true;
        }
        Ok(input)
    }

    fn read_polarized_plane(
        &self,
        plane: usize,
        polarization: usize,
    ) -> Result<Array2<f32>, PredictError> {
        let image = match &self.pixels {
            Pixels::Image(image) => image,
            Pixels::Held(planes) => {
                return Ok(planes[plane * self.polarizations.len() + polarization].clone());
            }
        };
        let [nx, ny] = self.shape;
        let pixels = {
            let image = self.payload_image(image)?;
            image
                .image
                .get_slice(&[0, 0, polarization, plane], &[nx, ny, 1, 1])
        }
        .map_err(|source| PredictError::Image {
            path: self.path.display().to_string(),
            source,
        })?;
        Ok(Array2::from_shape_fn((ny, nx), |(y, x)| {
            pixels[IxDyn(&[x, y, 0, 0])]
        }))
    }

    /// Bytes reading one spectral plane holds: its complete Stokes slice
    /// beside the separate operator images being extracted from it.
    pub(crate) fn plane_bytes(&self) -> u64 {
        let [nx, ny] = self.shape;
        (2 * self.polarizations.len() * nx * ny * size_of::<f32>()
            + self.polarizations.len() * size_of::<Array2<f32>>()) as u64
    }

    /// The model with every plane read and held, and no image open: what
    /// it predicts no longer depends on the image, which may change or go
    /// once it is read, as CASA's `predict` reads the model when it is
    /// called. It holds [`Self::held_bytes`]; reading holds one plane's
    /// [`Self::plane_bytes`] beside them, and this model's tile cache.
    ///
    /// # Errors
    ///
    /// A plane could not be read.
    pub(crate) fn held(&self) -> Result<Self, PredictError> {
        let mut planes = Vec::with_capacity(self.planes * self.polarizations.len());
        for plane in 0..self.planes {
            planes.extend(self.read_polarizations(plane)?);
        }
        Ok(Self {
            path: self.path.clone(),
            pixels: Pixels::Held(planes),
            shape: self.shape,
            planes: self.planes,
            polarizations: self.polarizations.clone(),
            increment_rad: self.increment_rad,
            frame: self.frame.clone(),
            tangent: self.tangent.clone(),
            first_frequency_hz: self.first_frequency_hz,
            frequency_increment_hz: self.frequency_increment_hz,
            coordinates: self.coordinates.clone(),
            first_spectral_world: self.first_spectral_world,
            cache_bytes: 0,
            distance_m: self.distance_m,
        })
    }

    /// Bytes the planes of [`Self::held`] hold.
    pub(crate) fn held_bytes(&self) -> u64 {
        let [nx, ny] = self.shape;
        (self.planes
            * self.polarizations.len()
            * (size_of::<Array2<f32>>() + nx * ny * size_of::<f32>())) as u64
    }
}

/// The bytes the tile cache of an `f32` image of `shape`, tiled by `tile`,
/// holds at most with [`MODEL_CACHE_BYTES`]: all its tiles when they fit,
/// else the tiles that fit, at least one (casa-tables' `TiledArrayStorage`).
fn cache_bound(shape: &[usize], tile: &[usize]) -> u64 {
    let tile_bytes = tile.iter().product::<usize>() * size_of::<f32>();
    let tiles: usize = shape
        .iter()
        .zip(tile)
        .map(|(&length, &tile)| length.div_ceil(tile.max(1)))
        .product();
    let all = tiles * tile_bytes;
    let held = if all <= MODEL_CACHE_BYTES {
        all
    } else {
        (MODEL_CACHE_BYTES / tile_bytes.max(1)).max(1) * tile_bytes
    };
    held as u64
}

/// Hz in one `unit`, or `None` when `unit` is not a frequency.
fn hz_per_unit(unit: &str) -> Option<f64> {
    let hz = Unit::new("Hz").ok()?;
    Quantity::new(1.0, unit).ok()?.get_value_in(&hz).ok()
}

// SPDX-License-Identifier: LGPL-3.0-or-later
//! The model image `sm.predict` reads: its header at open, its planes one at
//! a time as they are prepared.

use std::path::{Path, PathBuf};
use std::sync::{Mutex, PoisonError};

use casa_coordinates::{CoordinateModel, CoordinateSystem, CoordinateType};
use casa_images::PagedImage;
use casa_types::measures::direction::DirectionRef;
use casa_types::quanta::{Quantity, Unit};
use ndarray::{Array2, IxDyn};

use super::PredictError;

/// The brightness unit `sm.predict` reads the pixels in.
const JY_PER_PIXEL: &str = "Jy/pixel";

/// The tile cache a model is opened with, bytes: casa-tables' default.
const MODEL_CACHE_BYTES: usize = 64 << 20;

/// A CASA model image as `sm.predict` reads it: Stokes I planes in
/// Jy/pixel, with the direction of pixel `(nx/2, ny/2)` and the frequency of
/// every plane.
///
/// Opening reads the header; the pixels are read a plane at a time while
/// the model is prepared, so preparing a cube holds one plane per worker
/// rather than the cube. The image is never written: CASA's `predict`
/// rewrites the model's spectral units and frame in place, casa-rs reads
/// them as they are.
pub struct SkyModel {
    path: PathBuf,
    /// The open image; planes are read under the lock, one reader at a time.
    image: Mutex<PagedImage<f32>>,
    /// `[nx, ny]` pixels.
    shape: [usize; 2],
    /// Model planes.
    planes: usize,
    /// Signed radians per pixel `[Δx, Δy]`.
    pub(super) increment_rad: [f64; 2],
    /// J2000 direction of pixel `(nx/2, ny/2)`, CASA's tangent point.
    centre_rad: [f64; 2],
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
}

impl std::fmt::Debug for SkyModel {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("SkyModel")
            .field("path", &self.path)
            .field("shape", &self.shape)
            .field("planes", &self.planes)
            .field("increment_rad", &self.increment_rad)
            .field("centre_rad", &self.centre_rad)
            .field("first_frequency_hz", &self.first_frequency_hz)
            .field("frequency_increment_hz", &self.frequency_increment_hz)
            .finish_non_exhaustive()
    }
}

impl SkyModel {
    /// Open a CASA image with axes `[direction x, direction y, Stokes,
    /// spectral]`, one Stokes plane (I), as `simobserve` writes its sky
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
    /// coordinate other than J2000, or a spectral axis that is not in
    /// frequency units.
    pub fn open(path: impl AsRef<Path>) -> Result<Self, PredictError> {
        let path = path.as_ref();
        let image =
            PagedImage::<f32>::open_with_cache(path, MODEL_CACHE_BYTES).map_err(|source| {
                PredictError::Image {
                    path: path.display().to_string(),
                    source,
                }
            })?;
        let shape = image.shape().to_vec();
        let coordinates = image.coordinates().clone();
        let unsupported = |reason: String| Err(PredictError::UnsupportedModel(reason));
        if shape.len() != 4
            || coordinates.find_coordinate(CoordinateType::Direction) != Some(0)
            || coordinates.find_coordinate(CoordinateType::Stokes) != Some(1)
            || coordinates.find_coordinate(CoordinateType::Spectral) != Some(2)
        {
            return unsupported(
                "axes must be direction, Stokes and spectral, in that order".into(),
            );
        }
        if shape[2] != 1 || coordinates.coordinate(1).to_world(&[0.0])?[0] != 1.0 {
            return unsupported("the model must be Stokes I only".into());
        }
        // Rows and pointings are J2000; other frames need a conversion this
        // predictor does not make yet.
        if let CoordinateModel::Direction(direction) = coordinates.coordinate(0)
            && direction.direction_ref() != DirectionRef::J2000
        {
            return unsupported(format!(
                "the direction frame is {:?}, not J2000",
                direction.direction_ref()
            ));
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
        Ok(Self {
            path: path.to_path_buf(),
            image: Mutex::new(image),
            shape: [nx, ny],
            planes,
            increment_rad: [increment[0], increment[1]],
            centre_rad: [centre[0], centre[1]],
            first_frequency_hz: centre[3] * hz_per_unit,
            frequency_increment_hz: coordinates.coordinate(2).increment()[0] * hz_per_unit,
            coordinates,
            first_spectral_world: centre[3],
            cache_bytes,
        })
    }

    /// The most the image's tile cache holds once a plane has been read,
    /// bytes, until the model is dropped. casa-tables keeps every tile when
    /// they all fit the cache, and otherwise as many as fit, at least one;
    /// a cube tiled across many planes caches far more than a plane.
    pub(super) fn cache_bytes(&self) -> u64 {
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
    #[must_use]
    pub fn centre_rad(&self) -> [f64; 2] {
        self.centre_rad
    }

    /// Number of model planes.
    #[must_use]
    pub fn planes(&self) -> usize {
        self.planes
    }

    /// Frequency of model plane `plane`, in Hz.
    pub(super) fn plane_frequency_hz(&self, plane: usize) -> f64 {
        self.first_frequency_hz + plane as f64 * self.frequency_increment_hz
    }

    /// Fractional pixel `[x, y]` of a J2000 direction.
    pub(crate) fn pixel_of(&self, direction_rad: [f64; 2]) -> Result<[f64; 2], PredictError> {
        let pixel = self.coordinates.to_pixel(&[
            direction_rad[0],
            direction_rad[1],
            1.0,
            self.first_spectral_world,
        ])?;
        Ok([pixel[0], pixel[1]])
    }

    /// Model plane `plane`, `[y][x]`, read from the image.
    ///
    /// # Errors
    ///
    /// The pixels could not be read.
    pub(super) fn read_plane(&self, plane: usize) -> Result<Array2<f32>, PredictError> {
        let [nx, ny] = self.shape;
        let pixels = {
            let image = self.image.lock().unwrap_or_else(PoisonError::into_inner);
            image.get_slice(&[0, 0, 0, plane], &[nx, ny, 1, 1])
        }
        .map_err(|source| PredictError::Image {
            path: self.path.display().to_string(),
            source,
        })?;
        Ok(Array2::from_shape_fn((ny, nx), |(y, x)| {
            pixels[IxDyn(&[x, y, 0, 0])]
        }))
    }

    /// Bytes [`Self::read_plane`] holds while it reads one plane: the slice
    /// read and the plane returned.
    pub(super) fn plane_bytes(&self) -> u64 {
        let [nx, ny] = self.shape;
        2 * (nx * ny * size_of::<f32>()) as u64
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

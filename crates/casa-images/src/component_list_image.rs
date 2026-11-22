// SPDX-License-Identifier: LGPL-3.0-or-later

//! A virtual Float image that samples its components over the requested section.

use std::path::Path;
use std::sync::{Arc, Mutex};

use casa_components::{ComponentList, PixelGrid};
use casa_coordinates::{
    Coordinate, CoordinateModel, CoordinateSystem, CoordinateType, DirectionCoordinate,
    SpectralCoordinate, StokesType,
};
use casa_lattices::{Lattice, LatticeError};
use casa_types::RecordValue;
use casa_types::measures::direction::MDirection;
use casa_types::measures::frequency::FrequencyRef;
use ndarray::{Array3, ArrayD, IxDyn, ShapeBuilder};

use crate::{ImageError, ImageInfo, ImageInterface};

/// A read-only layer of image pixels evaluated from sky components on demand.
pub struct ComponentListImage {
    shape: Vec<usize>,
    coordinates: CoordinateSystem,
    direction: DirectionCoordinate,
    direction_axes: [usize; 2],
    grid: PixelGrid,
    spectral: Option<(usize, Box<SpectralCoordinate>)>,
    stokes: Option<(usize, Vec<usize>)>,
    components: Arc<ComponentList>,
    cache: Option<Mutex<Option<CachedSection>>>,
}

struct CachedSection {
    start: Vec<usize>,
    stride: Vec<usize>,
    values: ArrayD<f32>,
}

impl ComponentListImage {
    /// Construct a component image without allocating a pixel lattice.
    /// # Errors
    /// Returns an error for an empty component list or invalid image geometry.
    pub fn new(
        shape: Vec<usize>,
        components: ComponentList,
        coordinates: CoordinateSystem,
        cache: bool,
    ) -> Result<Self, ImageError> {
        Self::with_geometry(shape, Arc::new(components), coordinates, cache)
    }

    pub(crate) fn with_coordinates(
        &self,
        coordinates: CoordinateSystem,
    ) -> Result<Self, ImageError> {
        Self::with_geometry(
            self.shape.clone(),
            Arc::clone(&self.components),
            coordinates,
            self.cache.is_some(),
        )
    }

    fn with_geometry(
        shape: Vec<usize>,
        components: Arc<ComponentList>,
        coordinates: CoordinateSystem,
        cache: bool,
    ) -> Result<Self, ImageError> {
        if !(2..=4).contains(&shape.len()) {
            return Err(invalid(
                "ComponentListImages must have 2, 3, or 4 dimensions",
            ));
        }
        if shape.contains(&0) {
            return Err(invalid("TiledShape: shape has to be > 0"));
        }
        if components.components().is_empty() {
            return Err(invalid("A ComponentListImage needs at least one component"));
        }
        if coordinates.n_pixel_axes() != shape.len() {
            return Err(invalid(
                "Coordinate system must have the same dimensionality as the shape",
            ));
        }
        let Some((CoordinateModel::Direction(direction), axes)) =
            coordinate_axes(&coordinates, CoordinateType::Direction)
        else {
            return Err(invalid(
                "coordinate system must have a direction coordinate",
            ));
        };
        let [Some(longitude), Some(latitude)] = axes.as_slice() else {
            return Err(invalid("direction coordinate must have two pixel axes"));
        };
        let direction_axes = [*longitude, *latitude];
        let direction = direction.clone();
        let increments = direction.increment();
        let grid = PixelGrid {
            frame: direction.conversion_direction_ref(),
            longitude_size_rad: increments[0].abs(),
            latitude_size_rad: increments[1].abs(),
        };
        let spectral = match coordinate_axes(&coordinates, CoordinateType::Spectral) {
            Some((CoordinateModel::Spectral(spectral), axes)) => {
                axes[0].map(|axis| (axis, spectral.clone()))
            }
            _ => None,
        };
        let stokes = match coordinate_axes(&coordinates, CoordinateType::Stokes) {
            Some((CoordinateModel::Stokes(stokes), axes)) => axes[0]
                .map(|axis| {
                    stokes_planes(stokes.stokes(), shape[axis]).map(|planes| (axis, planes))
                })
                .transpose()?,
            _ => None,
        };
        if shape.len() != 2 + usize::from(spectral.is_some()) + usize::from(stokes.is_some()) {
            return Err(invalid(
                "ComponentListImage axes must be direction, spectral, or Stokes",
            ));
        }
        Ok(Self {
            shape,
            coordinates,
            direction,
            direction_axes,
            grid,
            spectral,
            stokes,
            components,
            cache: cache.then(|| Mutex::new(None)),
        })
    }

    fn sample(
        &self,
        start: &[usize],
        count: &[usize],
        stride: &[usize],
    ) -> Result<ArrayD<f32>, ImageError> {
        let [x, y] = self.direction_axes;
        let position = |axis: usize, index: usize| (start[axis] + stride[axis] * index) as f64;
        let mut directions = Vec::with_capacity(count[x] * count[y]);
        for iy in 0..count[y] {
            for ix in 0..count[x] {
                let [lon, lat] = self
                    .direction
                    .pixel_to_world([position(x, ix), position(y, iy)])?;
                directions.push(MDirection::from_angles(lon, lat, self.grid.frame).cosines());
            }
        }
        let (frequencies, frame) = match &self.spectral {
            Some((axis, coordinate)) => (
                (0..count[*axis])
                    .map(|index| coordinate.world_frequency_hz(position(*axis, index)))
                    .collect::<Result<Vec<_>, _>>()?,
                coordinate.world_frequency_ref(),
            ),
            None => {
                let first = self.components.components()[0]
                    .spectrum()
                    .reference_frequency();
                (vec![first.hz()], first.refer())
            }
        };
        let mut samples = Array3::zeros((4, directions.len(), frequencies.len()));
        for component in self.components.iter() {
            let reference = component.spectrum().reference_frequency();
            let image_frequencies = if reference.refer() != frame
                && reference.refer() != FrequencyRef::REST
                && frame != FrequencyRef::REST
                && let Some((_, coordinate)) = &self.spectral
                && let Some(measures) = coordinate.conversion_frame()
                && measures.direction().is_some()
            {
                let converted = reference
                    .convert_to(frame, measures)
                    .map_err(|error| ImageError::CasaSelection(error.to_string()))?;
                let scale = reference.hz() / converted.hz();
                std::borrow::Cow::Owned(frequencies.iter().map(|value| value * scale).collect())
            } else {
                std::borrow::Cow::Borrowed(frequencies.as_slice())
            };
            component
                .sample_pixels(&mut samples, &directions, &self.grid, &image_frequencies)
                .map_err(|error| ImageError::CasaSelection(error.to_string()))?;
        }

        Ok(ArrayD::from_shape_fn(IxDyn(count).f(), |index| {
            let channel = self.spectral.as_ref().map_or(0, |(axis, _)| index[*axis]);
            let stokes = self.stokes.as_ref().map_or(0, |(axis, planes)| {
                planes[start[*axis] + stride[*axis] * index[*axis]]
            });
            samples[[stokes, index[x] + count[x] * index[y], channel]] as f32
        }))
    }
}

fn coordinate_axes(
    system: &CoordinateSystem,
    kind: CoordinateType,
) -> Option<(&CoordinateModel, Vec<Option<usize>>)> {
    let index = system.find_coordinate(kind)?;
    Some((
        system.coordinate(index),
        system.coordinate_pixel_axes(index)?,
    ))
}

fn stokes_planes(stokes: &[StokesType], count: usize) -> Result<Vec<usize>, ImageError> {
    if count > 4 {
        return Err(invalid(
            "Polarization axis can have no more than four pixels",
        ));
    }
    (0..count)
        .map(|i| match stokes.get(i) {
            Some(StokesType::I) => Ok(0),
            Some(StokesType::Q) => Ok(1),
            Some(StokesType::U) => Ok(2),
            Some(StokesType::V) => Ok(3),
            _ => Err(ImageError::CasaSelection(format!(
                "Unsupported Stokes value at pixel {i}"
            ))),
        })
        .collect()
}

fn invalid(message: &str) -> ImageError {
    ImageError::CasaSelection(message.to_owned())
}

impl Lattice<f32> for ComponentListImage {
    fn shape(&self) -> &[usize] {
        &self.shape
    }

    fn is_writable(&self) -> bool {
        false
    }

    fn get_at(&self, index: &[usize]) -> Result<f32, LatticeError> {
        let ones = [1; 4];
        let ones = &ones[..self.shape.len()];
        let pixels = self.get_slice(index, ones, ones)?;
        Ok(*pixels.first().expect("requested one component image pixel"))
    }

    fn get_slice(
        &self,
        start: &[usize],
        count: &[usize],
        stride: &[usize],
    ) -> Result<ArrayD<f32>, LatticeError> {
        if [start, count, stride]
            .into_iter()
            .any(|axis| axis.len() != self.shape.len())
            || (0..count.len()).any(|i| {
                stride[i] == 0
                    || count[i]
                        .saturating_sub(1)
                        .checked_mul(stride[i])
                        .and_then(|extent| start[i].checked_add(extent))
                        .is_none_or(|last| last >= self.shape[i])
            })
        {
            return Err(LatticeError::SliceOutOfBounds {
                start: start.to_vec(),
                slice_shape: count.to_vec(),
                stride: stride.to_vec(),
                lattice_shape: self.shape.clone(),
            });
        }
        if count.contains(&0) {
            return Ok(ArrayD::zeros(IxDyn(count)));
        }
        // Never extend the attachment's cache in proportion to its logical image.
        let cacheable = count
            .iter()
            .try_fold(1usize, |size, &length| size.checked_mul(length))
            .is_some_and(|size| size <= self.advised_max_pixels());
        let mut cache = self.cache.as_ref().filter(|_| cacheable).map(|cache| {
            cache
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
        });
        if let Some(cached) = cache.as_deref().and_then(Option::as_ref)
            && cached.start == start
            && cached.stride == stride
            && cached.values.shape() == count
        {
            return Ok(cached.values.clone());
        }
        let values = self
            .sample(start, count, stride)
            .map_err(|error| LatticeError::Table(error.to_string()))?;
        if let Some(cache) = cache.as_deref_mut() {
            *cache = Some(CachedSection {
                start: start.to_vec(),
                stride: stride.to_vec(),
                values: values.clone(),
            });
        }
        Ok(values)
    }
}

impl ImageInterface<f32> for ComponentListImage {
    fn as_any(&self) -> Option<&dyn std::any::Any> {
        Some(self)
    }

    fn coordinates(&self) -> &CoordinateSystem {
        &self.coordinates
    }

    fn units(&self) -> &str {
        "Jy/pixel"
    }

    fn misc_info(&self) -> RecordValue {
        RecordValue::default()
    }

    fn image_info(&self) -> Result<ImageInfo, ImageError> {
        Ok(ImageInfo::default())
    }

    fn name(&self) -> Option<&Path> {
        None
    }
}

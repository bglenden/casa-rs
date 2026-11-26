// SPDX-License-Identifier: LGPL-3.0-or-later

//! A virtual Float image that samples its components over the requested section.

use std::borrow::Cow;
use std::path::Path;
use std::sync::{Arc, Mutex, OnceLock};

use casa_components::{
    ComponentList, ComponentShape, PixelGrid, ShapeKind, SpectralKind, SpectralModel,
};
use casa_coordinates::{
    Coordinate, CoordinateError, CoordinateModel, CoordinateSystem, CoordinateType,
    DirectionCoordinate, SpectralCoordinate, StokesType,
};
use casa_core::RecordValue;
use casa_core::measures::direction::MDirection;
use casa_core::measures::frame::MeasFrame;
use casa_core::measures::frequency::FrequencyRef;
use casa_lattices::{Lattice, LatticeError};
use ndarray::{Array3, ArrayD, Axis, IxDyn, ShapeBuilder};

use crate::{ImageError, ImageInfo, ImageInterface};

mod masks;
mod native;
pub(crate) use native::{ComponentTable, write_component_keyword};

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
    placements: OnceLock<Result<Vec<Placement>, ImageError>>,
    cache: Option<Mutex<Option<CachedSection>>>,
    native: Option<native::ComponentTable>,
}

enum Placement {
    Section,
    Pixel {
        position: [usize; 2],
        direction: [f64; 3],
    },
    Outside,
}

struct CachedSection {
    start: Vec<usize>,
    stride: Vec<usize>,
    values: ArrayD<f32>,
}

impl ComponentListImage {
    /// Construct a component image without allocating a pixel lattice.
    /// # Errors
    /// Returns an error for invalid image geometry or for an empty list used
    /// without a spectral coordinate to supply the sampling frequency.
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
        let mut image = Self::with_geometry(
            self.shape.clone(),
            Arc::clone(&self.components),
            coordinates,
            self.cache.is_some(),
        )?;
        image.native = self.native.clone();
        Ok(image)
    }

    pub(crate) fn with_cache_mode(&self, enabled: bool) -> Result<Self, ImageError> {
        let mut image = Self::with_geometry(
            self.shape.clone(),
            Arc::clone(&self.components),
            self.coordinates.clone(),
            enabled,
        )?;
        image.native = self.native.clone();
        Ok(image)
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
        if components.is_empty() && spectral.is_none() {
            return Err(invalid(
                "A ComponentListImage without a spectral coordinate needs at least one component",
            ));
        }
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
            placements: OnceLock::new(),
            cache: cache.then(|| Mutex::new(None)),
            native: None,
        })
    }

    fn pixel_direction(&self, pixel: [f64; 2]) -> Result<[f64; 3], CoordinateError> {
        let [longitude, latitude] = self.direction.pixel_to_world(pixel)?;
        Ok(MDirection::from_angles(longitude, latitude, self.grid.frame).cosines())
    }

    fn point_placement(&self, shape: &ComponentShape) -> Result<Placement, ImageError> {
        if !matches!(shape.kind(), ShapeKind::Point) {
            return Ok(Placement::Section);
        }
        let direction = shape
            .reference_direction()
            .convert_to(self.grid.frame, &MeasFrame::default())
            .map_err(|error| ImageError::CasaSelection(error.to_string()))?;
        let (longitude, latitude) = direction.as_angles();
        let pixel = match self.direction.world_to_pixel([longitude, latitude]) {
            Ok(pixel) => pixel,
            Err(CoordinateError::OutsideProjectionDomain(_)) => return Ok(Placement::Outside),
            Err(error) => return Err(error.into()),
        };
        let target = pixel.map(|position| (position + 0.5).floor());
        let sizes = self.direction_axes.map(|axis| self.shape[axis] as f64);
        if target
            .into_iter()
            .zip(sizes)
            .any(|(position, size)| position < -1. || position > size)
        {
            return Ok(Placement::Outside);
        }
        // Try the nearest pixel first, then the adjacent candidates in
        // longitude and latitude order as ComponentListImage does.
        let adjacent = (-1..=1).flat_map(|x| {
            (-1..=1).filter_map(move |y| {
                (x != 0 || y != 0).then_some([target[0] + f64::from(x), target[1] + f64::from(y)])
            })
        });
        for candidate in std::iter::once(target).chain(adjacent) {
            let direction = match self.pixel_direction(candidate) {
                Ok(direction) => direction,
                Err(CoordinateError::OutsideProjectionDomain(_)) => continue,
                Err(error) => return Err(error.into()),
            };
            let scale = shape
                .pixel_scales(&[direction], &self.grid)
                .map_err(|error| ImageError::CasaSelection(error.to_string()))?;
            if scale[0] != 0. {
                return Ok(
                    if candidate
                        .into_iter()
                        .zip(sizes)
                        .all(|(pixel, size)| pixel >= 0. && pixel < size)
                    {
                        Placement::Pixel {
                            position: candidate.map(|pixel| pixel as usize),
                            direction,
                        }
                    } else {
                        Placement::Outside
                    },
                );
            }
        }
        Ok(Placement::Section)
    }

    fn component_frequencies<'a>(
        &self,
        spectrum: &SpectralModel,
        frequencies: &'a [f64],
        frame: FrequencyRef,
    ) -> Result<Cow<'a, [f64]>, ImageError> {
        let reference = spectrum.reference_frequency();
        if matches!(spectrum.kind(), SpectralKind::Constant)
            || reference.refer() == frame
            || reference.refer() == FrequencyRef::REST
            || frame == FrequencyRef::REST
        {
            return Ok(Cow::Borrowed(frequencies));
        }
        let no_context = MeasFrame::default();
        let measures = self
            .spectral
            .as_ref()
            .and_then(|(_, coordinate)| coordinate.conversion_frame())
            .unwrap_or(&no_context);
        let converted = reference.convert_to(frame, measures).map_err(|_| {
            ImageError::CasaSelection(format!(
                "Cannot transform frequency from {} to {}",
                frame.as_str(),
                reference.refer().as_str(),
            ))
        })?;
        let scale = reference.hz() / converted.hz();
        Ok(Cow::Owned(
            frequencies.iter().map(|value| value * scale).collect(),
        ))
    }

    fn sample(
        &self,
        start: &[usize],
        count: &[usize],
        stride: &[usize],
    ) -> Result<ArrayD<f32>, ImageError> {
        let [x, y] = self.direction_axes;
        let position = |axis: usize, index: usize| (start[axis] + stride[axis] * index) as f64;
        let placements = self
            .placements
            .get_or_init(|| {
                self.components
                    .iter()
                    .map(|component| self.point_placement(component.shape()))
                    .collect()
            })
            .as_ref()
            .map_err(|error| ImageError::CasaSelection(error.to_string()))?;
        let direction_count = count[x] * count[y];
        let mut directions = Vec::new();
        if placements
            .iter()
            .any(|placement| matches!(placement, Placement::Section))
        {
            directions.reserve_exact(direction_count);
            for iy in 0..count[y] {
                for ix in 0..count[x] {
                    directions.push(self.pixel_direction([position(x, ix), position(y, iy)])?);
                }
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
        let mut samples = Array3::zeros((4, direction_count, frequencies.len()));
        let component_error =
            |error: casa_components::ComponentError| ImageError::CasaSelection(error.to_string());
        let section_pixel = |axis: usize, pixel: usize| {
            let offset = pixel.checked_sub(start[axis])?;
            if offset.is_multiple_of(stride[axis]) && offset / stride[axis] < count[axis] {
                Some(offset / stride[axis])
            } else {
                None
            }
        };
        let mut point = Array3::zeros((4, 1, frequencies.len()));
        for (component, placement) in self.components.iter().zip(placements) {
            if matches!(placement, Placement::Outside) {
                continue;
            }
            let image_frequencies =
                self.component_frequencies(component.spectrum(), &frequencies, frame)?;
            match placement {
                Placement::Section => component
                    .sample_pixels(&mut samples, &directions, &self.grid, &image_frequencies)
                    .map_err(component_error)?,
                Placement::Pixel {
                    position,
                    direction,
                } => {
                    if let Some((ix, iy)) =
                        section_pixel(x, position[0]).zip(section_pixel(y, position[1]))
                    {
                        point.fill(0.);
                        component
                            .sample_pixels(
                                &mut point,
                                std::slice::from_ref(direction),
                                &self.grid,
                                &image_frequencies,
                            )
                            .map_err(component_error)?;
                        samples
                            .index_axis_mut(Axis(1), ix + count[x] * iy)
                            .scaled_add(1., &point.index_axis(Axis(1), 0));
                    }
                }
                Placement::Outside => unreachable!("outside pixels are skipped"),
            }
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
    let mut planes = stokes
        .iter()
        .map(|value| match value {
            StokesType::I => Ok(0),
            StokesType::Q => Ok(1),
            StokesType::U => Ok(2),
            StokesType::V => Ok(3),
            _ => Err(ImageError::CasaSelection(format!(
                "Unsupported stokes type {}",
                value.name()
            ))),
        })
        .collect::<Result<Vec<_>, _>>()?;
    if planes.len() < count {
        return Err(invalid("Stokes coordinate has fewer planes than the image"));
    }
    planes.truncate(count);
    Ok(planes)
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
        self.native
            .as_ref()
            .map_or_else(RecordValue::default, |table| table.misc.clone())
    }

    fn image_info(&self) -> Result<ImageInfo, ImageError> {
        Ok(self
            .native
            .as_ref()
            .map_or_else(ImageInfo::default, |table| table.info.clone()))
    }

    fn name(&self) -> Option<&Path> {
        self.native.as_ref().map(|table| table.path.as_path())
    }

    fn mask_names(&self) -> Vec<String> {
        self.native
            .as_ref()
            .map_or_else(Vec::new, |table| table.mask_state().names())
    }

    fn default_mask_name(&self) -> Option<String> {
        self.native
            .as_ref()
            .and_then(|table| table.mask_state().default_name())
    }

    fn has_pixel_mask(&self) -> bool {
        self.native
            .as_ref()
            .is_some_and(|table| table.mask_state().active().is_some())
    }

    fn default_mask(&self) -> Result<Option<ArrayD<bool>>, ImageError> {
        let origin = vec![0; self.shape.len()];
        let stride = vec![1; self.shape.len()];
        self.get_mask_slice(&origin, &self.shape, &stride)
    }

    fn get_named_mask(&self, name: &str) -> Result<ArrayD<bool>, ImageError> {
        let origin = vec![0; self.shape.len()];
        let stride = vec![1; self.shape.len()];
        self.get_named_mask_slice(name, &origin, &self.shape, &stride)
    }

    fn get_named_mask_slice(
        &self,
        name: &str,
        start: &[usize],
        shape: &[usize],
        stride: &[usize],
    ) -> Result<ArrayD<bool>, ImageError> {
        self.native
            .as_ref()
            .ok_or_else(|| ImageError::MaskNotFound(name.into()))?
            .read_mask(name, start, shape, stride)
    }

    fn get_mask_slice(
        &self,
        start: &[usize],
        shape: &[usize],
        stride: &[usize],
    ) -> Result<Option<ArrayD<bool>>, ImageError> {
        match &self.native {
            Some(table) => table.read_live_default_mask(start, shape, stride),
            None => Ok(None),
        }
    }
}

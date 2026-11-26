// SPDX-License-Identifier: LGPL-3.0-or-later

use casa_coordinates::{CoordinateModel, CoordinateType, StokesType};

use crate::analysis::axes::direction_plane_axes;
use casa_images::{ImageError, ImageInterface, ImagePixel};

use super::super::SubimageRegion;
use super::super::read_window::PixelSections;
use super::super::region::SubimageSelection;

pub(super) struct FitSelection {
    pub(super) region: SubimageSelection,
    pub(super) sky_axes: [usize; 2],
    pub(super) sky_shape: [usize; 2],
    pub(super) sky_stride: [usize; 2],
    pub(super) channels: Vec<usize>,
    pub(super) stokes_pixel: usize,
    pub(super) stokes: StokesType,
    pub(super) ancillary: Vec<(usize, usize)>,
    pub(super) can_fit_plane: bool,
}

impl FitSelection {
    pub(super) fn resolve<T: ImagePixel>(
        image: &dyn ImageInterface<T>,
        pixel_box: Option<&str>,
        region: Option<SubimageRegion<'_>>,
        channels: Option<&str>,
        stokes: Option<&str>,
    ) -> Result<Self, ImageError> {
        if image
            .coordinates()
            .find_coordinate(CoordinateType::Direction)
            .is_none()
        {
            return Err(ImageError::CasaSelection(
                "Image does not have required coordinate Direction".into(),
            ));
        }
        let sky_axes = direction_plane_axes(image)?;
        let region =
            SubimageSelection::resolve_section(image, pixel_box, region, channels, stokes)?;
        if stokes.is_some_and(|value| !value.trim().is_empty())
            && region
                .selected_stokes
                .as_ref()
                .is_some_and(|planes| planes.len() > 1)
        {
            return Err(ImageError::CasaSelection(
                "Only a single Stokes plane is supported".into(),
            ));
        }
        let sky_stride =
            sky_axes.map(|axis| region.stride.as_ref().map_or(1, |stride| stride[axis]));
        let sky_shape = std::array::from_fn(|i| region.shape[sky_axes[i]].div_ceil(sky_stride[i]));
        let channels = if let Some(axis) = region.channel_axis {
            let stride = region.stride.as_ref().map_or(1, |stride| stride[axis]);
            let first = region.start[axis];
            let selected: Vec<_> = (first..first + region.shape[axis])
                .step_by(stride)
                .filter(|&channel| {
                    region.channel_spans.is_empty()
                        || region
                            .channel_spans
                            .iter()
                            .any(|span| span.contains(channel))
                })
                .collect();
            if selected.windows(2).any(|pair| pair[1] != pair[0] + 1) {
                return Err(ImageError::CasaSelection(
                    "Only a single contiguous channel range is supported".into(),
                ));
            }
            selected
        } else {
            vec![0]
        };
        let stokes_pixel = region.stokes_axis.map_or(0, |axis| {
            region
                .stokes_indices
                .as_ref()
                .and_then(|indices| indices.first().copied())
                .unwrap_or(region.start[axis])
        });
        let stokes = image
            .coordinates()
            .find_coordinate(CoordinateType::Stokes)
            .and_then(|index| match image.coordinates().coordinate(index) {
                CoordinateModel::Stokes(coordinate) => {
                    coordinate.stokes().get(stokes_pixel).copied()
                }
                _ => None,
            })
            .unwrap_or(StokesType::I);
        let mut can_fit_plane = sky_shape.iter().all(|&axis| axis >= 2);
        let ancillary = (0..image.ndim())
            .filter(|axis| {
                !sky_axes.contains(axis)
                    && Some(*axis) != region.stokes_axis
                    && Some(*axis) != region.channel_axis
            })
            .map(|axis| {
                let step = region.stride.as_ref().map_or(1, |stride| stride[axis]);
                can_fit_plane &= region.shape[axis].div_ceil(step) == 1;
                (axis, region.start[axis])
            })
            .collect();
        Ok(Self {
            region,
            sky_axes,
            sky_shape,
            sky_stride,
            channels,
            stokes_pixel,
            stokes,
            ancillary,
            can_fit_plane,
        })
    }

    pub(super) fn read<T: ImagePixel>(
        &self,
        reader: &PixelSections<'_, T>,
        channel: usize,
        to_float: impl Fn(T) -> f32,
    ) -> Result<FitPlane, ImageError> {
        let selection = &self.region;
        let [width, height] = self.sky_shape;
        let [x, y] = self.sky_axes;
        let [x_step, y_step] = self.sky_stride;
        let mut start = selection.start.clone();
        let mut shape = vec![1; start.len()];
        let stride = selection
            .stride
            .clone()
            .unwrap_or_else(|| vec![1; start.len()]);
        if let Some(axis) = selection.channel_axis {
            start[axis] = channel;
        }
        if let Some(axis) = selection.stokes_axis {
            start[axis] = self.stokes_pixel;
        }
        let mut values = Vec::new();
        let mut validity = Vec::new();
        let block = (64 * 1024_usize).div_ceil(x_step);
        for row in 0..height {
            start[y] = selection.start[y] + row * y_step;
            for column in (0..width).step_by(block) {
                start[x] = selection.start[x] + column * x_step;
                shape[x] = block.min(width - column);
                let (pixels, valid) = reader.read(&start, &shape, &stride)?;
                values.extend(pixels.iter().copied().map(&to_float));
                if let Some(valid) = valid {
                    validity.extend(valid.iter().copied());
                } else {
                    validity.resize(values.len(), true);
                }
            }
        }
        Ok(FitPlane {
            shape: [width, height],
            values,
            validity,
        })
    }
}

pub(super) struct FitPlane {
    pub(super) shape: [usize; 2],
    pub(super) values: Vec<f32>,
    pub(super) validity: Vec<bool>,
}

// SPDX-License-Identifier: LGPL-3.0-or-later

mod fft;

use std::path::Path;

use casa_core::{ArrayD, RecordValue};
use casa_lattices::{TraversalCursor, TraversalCursorIter, TraversalSpec};
use ndarray::{Dimension, IxDyn, Slice, Zip};

use crate::convolve2d::GaussianSamples;
use casa_images::bounded_read_cursor;
use casa_images::{ImageError, ImageInterface, ImagePixel, MutableImageInterface};

use super::super::axes::StridedSection;
use super::super::lel_mask::PreparedMask;
use super::read_window::PixelSections;
use super::{SubimageOptions, SubimagePlan, SubimageRegion};

/// The real pixel precisions supported by general image convolution.
pub trait ConvolutionPixel: ImagePixel + Copy + Into<f64> {
    /// Round a sample to the image's storage precision.
    fn from_f64(value: f64) -> Self;
}

impl ConvolutionPixel for f32 {
    fn from_f64(value: f64) -> Self {
        value as Self
    }
}

impl ConvolutionPixel for f64 {
    fn from_f64(value: f64) -> Self {
        value
    }
}

/// A real-valued kernel array or a native image whose raw pixels are used.
pub enum ConvolutionKernel<'a, T: ConvolutionPixel> {
    /// A caller-supplied n-dimensional pixel array.
    Array(&'a ArrayD<T>),
    /// A named kernel image. Its pixel mask and coordinate grid are ignored.
    Image(&'a dyn ImageInterface<T>),
}

impl<T: ConvolutionPixel> ConvolutionKernel<'_, T> {
    fn shape(&self) -> &[usize] {
        match self {
            Self::Array(array) => array.shape(),
            Self::Image(image) => image.shape(),
        }
    }

    fn read(&self, start: &[usize], shape: &[usize]) -> Result<ArrayD<T>, ImageError> {
        match self {
            Self::Image(image) => Ok(image.get_slice(start, shape, &vec![1; shape.len()])?),
            Self::Array(array) => Ok(array
                .slice_each_axis(|axis| slice(start[axis.axis.index()], shape[axis.axis.index()]))
                .to_owned()),
        }
    }
}

enum KernelInput<'a, T: ConvolutionPixel> {
    External(ConvolutionKernel<'a, T>),
    Gaussian {
        samples: GaussianSamples,
        axes: [usize; 2],
        shape: Vec<usize>,
    },
}

impl<T: ConvolutionPixel> KernelInput<'_, T> {
    fn shape(&self) -> &[usize] {
        match self {
            Self::External(kernel) => kernel.shape(),
            Self::Gaussian { shape, .. } => shape,
        }
    }

    fn read(&self, start: &[usize], shape: &[usize]) -> Result<ArrayD<T>, ImageError> {
        match self {
            Self::External(kernel) => kernel.read(start, shape),
            Self::Gaussian {
                samples,
                axes: [x, y],
                ..
            } => Ok(ArrayD::from_shape_fn(IxDyn(shape), |index| {
                T::from_f64(samples.sample(start[*x] + index[*x], start[*y] + index[*y]))
            })),
        }
    }

    fn sum(&self) -> Result<f64, ImageError> {
        let shape = self.shape();
        let mut sum = 0.0;
        for tile in tiles(shape, bounded_read_cursor(shape.to_vec())?) {
            let tile = tile?;
            for &value in &self.read(&tile.position, &tile.shape)? {
                sum += value.into();
            }
        }
        Ok(sum)
    }
}

/// A same-shape, zero-extended linear convolution of a selected image.
pub struct ImageConvolution {
    /// Shape, coordinates and stored metadata of the convolved selection.
    pub section: SubimagePlan,
}

impl ImageConvolution {
    /// Resolve the section in the original image and discard the restoring beam.
    ///
    /// # Errors
    /// The selected region or its coordinates cannot be interpreted.
    pub fn new<T: ConvolutionPixel>(
        image: &dyn ImageInterface<T>,
        record: RecordValue,
        region: Option<SubimageRegion<'_>>,
    ) -> Result<Self, ImageError> {
        let mut section = SubimagePlan::new(
            image,
            Some(record),
            SubimageOptions {
                region,
                dropdeg: false,
                keepaxes: &[],
            },
        )?;
        section.image_info.beam_set = Default::default();
        Ok(Self { section })
    }

    /// Convolve the selected pixels using an optional expression mask.
    ///
    /// # Errors
    /// The kernel or mask cannot be applied to the selected image, or input or
    /// output storage fails.
    pub fn copy_to<T: ConvolutionPixel>(
        &self,
        image: &dyn ImageInterface<T>,
        kernel: ConvolutionKernel<'_, T>,
        options: ConvolutionOptions<'_>,
        output: &mut dyn MutableImageInterface<T>,
    ) -> Result<(), ImageError> {
        let workspace = KernelPlan::new(
            KernelInput::External(kernel),
            &self.section.shape,
            options.scale,
            false,
        )?;
        let active = workspace.active(self.section.shape.len());
        self.copy_groups(
            image,
            options,
            ConvolutionGroups {
                active: &active,
                plane_axes: &[],
                workspace: KernelWorkspaces::Shared(&workspace),
            },
            output,
        )
    }

    pub(super) fn copy_groups<T: ConvolutionPixel>(
        &self,
        image: &dyn ImageInterface<T>,
        options: ConvolutionOptions<'_>,
        groups: ConvolutionGroups<'_, T>,
        output: &mut dyn MutableImageInterface<T>,
    ) -> Result<(), ImageError> {
        let shape = &self.section.shape;
        let active = groups.active;
        let has_active = active.contains(&true);
        let selection = &self.section.selection;
        let unit = vec![1; shape.len()];
        let input = StridedSection {
            start: &selection.start,
            shape,
            stride: selection.stride.as_deref().unwrap_or(&unit),
        };
        let reader = PixelSections {
            image,
            selection,
            mask: PreparedMask::new_for_output(
                options.mask,
                image.shape(),
                image.coordinates(),
                options.stretch,
                options.destination,
            )?,
        };
        self.section.copy_metadata(image, output)?;
        let mut cursor = convolution_cursor(input.dense_shape(shape), active)?
            .iter()
            .zip(input.stride)
            .map(|(&extent, &stride)| extent.div_ceil(stride).max(1))
            .collect::<Vec<_>>();
        for &axis in groups.plane_axes {
            cursor[axis] = 1;
        }
        let group_shape = shape
            .iter()
            .zip(active)
            .map(|(&length, &active)| if active { 1 } else { length })
            .collect::<Vec<_>>();
        let mut have_mask = false;
        for group in tiles(&group_shape, cursor.clone()) {
            let group = group?;
            let selected;
            let workspace = match groups.workspace {
                KernelWorkspaces::Shared(workspace) => Some(workspace),
                KernelWorkspaces::PerPlane(function) => {
                    selected = function(&group.position)?;
                    selected.as_ref()
                }
            };
            let invalid_kernel = workspace.is_some_and(|workspace| workspace.nonfinite);
            let inner_shape = shape
                .iter()
                .zip(&group.shape)
                .zip(active)
                .map(|((&length, &width), &active)| if active { length } else { width })
                .collect::<Vec<_>>();
            let mut invalid_lanes = ArrayD::from_elem(IxDyn(&group.shape), false);
            for tile in tiles(&inner_shape, cursor.clone()) {
                let tile = offset_tile(tile?, &group.position);
                let (pixels, validity) = reader.read(
                    &input.source_start(&tile.position),
                    &tile.shape,
                    input.stride,
                )?;
                if workspace.is_some() && has_active && !invalid_kernel {
                    for (mut index, &sample) in pixels.indexed_iter() {
                        if !sample.into().is_finite()
                            && validity.as_ref().is_none_or(|mask| mask[&index])
                        {
                            for (pixel, &is_active) in index.slice_mut().iter_mut().zip(active) {
                                if is_active {
                                    *pixel = 0;
                                }
                            }
                            invalid_lanes[index] = true;
                        }
                    }
                }
                if let Some(mask) = validity.filter(|mask| mask.iter().any(|&valid| !valid)) {
                    if !have_mask {
                        output.create_filled_mask("mask0", true)?;
                        output.set_default_mask("mask0")?;
                        have_mask = true;
                    }
                    output.put_mask_slice("mask0", &mask, &tile.position)?;
                }

                let Some(workspace) = workspace else {
                    output.put_slice(&pixels, &tile.position)?;
                    continue;
                };
                drop(pixels);
                let initial = if invalid_kernel { f64::NAN } else { 0. };
                let mut result = ArrayD::from_elem(IxDyn(&tile.shape), initial);
                if !invalid_kernel {
                    workspace.convolve_tile(
                        &reader,
                        &input,
                        Window::from(&tile),
                        has_active,
                        &mut result,
                    )?;
                }
                output.put_slice(&result.mapv(T::from_f64), &tile.position)?;
            }
            if invalid_lanes.iter().any(|&invalid| invalid) {
                // A transform mixes a nonfinite *valid* sample through its
                // active axes, including the parts worked in other tiles.
                for tile in tiles(&inner_shape, cursor.clone()) {
                    let tile = offset_tile(tile?, &group.position);
                    let mut pixels = output.get_slice(&tile.position, &tile.shape, &unit)?;
                    let invalid = invalid_lanes
                        .broadcast(IxDyn(&tile.shape))
                        .expect("batch axes match");
                    Zip::from(&mut pixels)
                        .and(invalid)
                        .for_each(|value, &invalid| {
                            if invalid {
                                *value = T::from_f64(f64::NAN);
                            }
                        });
                    output.put_slice(&pixels, &tile.position)?;
                }
            }
        }
        Ok(())
    }
}

pub(super) struct ConvolutionGroups<'a, T: ConvolutionPixel> {
    pub(super) active: &'a [bool],
    pub(super) plane_axes: &'a [usize],
    pub(super) workspace: KernelWorkspaces<'a, T>,
}

type PlaneKernel<'a, T> =
    dyn Fn(&[usize]) -> Result<Option<KernelPlan<'static, T>>, ImageError> + 'a;

pub(super) enum KernelWorkspaces<'a, T: ConvolutionPixel> {
    Shared(&'a KernelPlan<'a, T>),
    PerPlane(&'a PlaneKernel<'a, T>),
}

pub(super) struct KernelPlan<'a, T: ConvolutionPixel> {
    kernel: KernelInput<'a, T>,
    scaling: Scaling,
    center: Vec<usize>,
    first: Vec<usize>,
    support: Vec<usize>,
    cursor: Vec<usize>,
    nonfinite: bool,
}

struct KernelPatch {
    weights: ArrayD<f64>,
    offsets: Vec<isize>,
}

impl<'a, T: ConvolutionPixel> KernelPlan<'a, T> {
    fn new(
        kernel: KernelInput<'a, T>,
        shape: &[usize],
        scale: f64,
        explicit_scale: bool,
    ) -> Result<Self, ImageError> {
        let rank = kernel.shape().len();
        if rank == 0 || kernel.shape().contains(&0) {
            return Err(ImageError::CasaSelection(
                "Kernel array cannot be empty".into(),
            ));
        }
        if rank > shape.len() {
            return Err(ImageError::CasaSelection(
                "Kernel lattice has more axes than the image!".into(),
            ));
        }
        let divisor = (!explicit_scale && scale <= 0.0)
            .then(|| kernel.sum())
            .transpose()?;
        let scaling = divisor.map_or(Scaling::Multiply(scale), Scaling::Divide);
        let mut center = kernel
            .shape()
            .iter()
            .map(|&width| width / 2)
            .collect::<Vec<_>>();
        center.resize(shape.len(), 0);
        let reach = &shape[..kernel.shape().len()];
        let first = center
            .iter()
            .zip(reach)
            .map(|(&mid, &len)| mid.saturating_sub(len - 1))
            .collect::<Vec<_>>();
        let support = kernel
            .shape()
            .iter()
            .zip(&center)
            .zip(&first)
            .zip(reach)
            .map(|(((&width, &mid), &low), &len)| width.min(mid.saturating_add(len)) - low)
            .collect::<Vec<_>>();
        let cursor = fft::limit_kernel_cursor(bounded_read_cursor(support.clone())?);
        let mut plan = Self {
            kernel,
            scaling,
            center,
            first,
            support,
            cursor,
            nonfinite: false,
        };
        plan.nonfinite = plan.support.iter().any(|&width| width > 1) && plan.has_nonfinite()?;
        Ok(plan)
    }

    pub(super) fn gaussian(
        samples: GaussianSamples,
        axes: [usize; 2],
        image_shape: &[usize],
        scale: f64,
    ) -> Result<KernelPlan<'static, T>, ImageError> {
        let mut shape = vec![1; image_shape.len()];
        for axis in axes {
            shape[axis] = samples.side;
        }
        KernelPlan::new(
            KernelInput::Gaussian {
                samples,
                axes,
                shape,
            },
            image_shape,
            scale,
            true,
        )
    }

    fn active(&self, rank: usize) -> Vec<bool> {
        (0..rank)
            .map(|axis| self.support.get(axis).is_some_and(|&width| width > 1))
            .collect()
    }

    fn patches(&self) -> impl Iterator<Item = Result<KernelPatch, ImageError>> + '_ {
        tiles(&self.support, self.cursor.clone()).map(|patch| {
            let patch = patch?;
            let start = patch
                .position
                .iter()
                .zip(&self.first)
                .map(|(&i, &lo)| i + lo)
                .collect::<Vec<_>>();
            let weights = self
                .kernel
                .read(&start, &patch.shape)?
                .mapv(|weight| self.scaling.apply::<T>(weight.into()));
            let offsets = self
                .center
                .iter()
                .enumerate()
                .map(|(axis, &mid)| {
                    start.get(axis).copied().unwrap_or_default() as isize - mid as isize
                })
                .collect();
            Ok(KernelPatch { weights, offsets })
        })
    }

    fn has_nonfinite(&self) -> Result<bool, ImageError> {
        for patch in self.patches() {
            if patch?.weights.iter().any(|weight| !weight.is_finite()) {
                return Ok(true);
            }
        }
        Ok(false)
    }

    fn convolve_tile(
        &self,
        reader: &PixelSections<'_, T>,
        input: &StridedSection<'_>,
        output_region: Window<'_>,
        exclude_nonfinite: bool,
        result: &mut ArrayD<f64>,
    ) -> Result<(), ImageError> {
        for patch in self.patches() {
            let KernelPatch {
                weights,
                offsets: delta,
            } = patch?;
            let Some((origin, size)) = output_region.halo(input.shape, &delta, weights.shape())
            else {
                continue;
            };
            let halo_section = StridedSection {
                start: &input.source_start(&origin),
                shape: &size,
                stride: input.stride,
            };
            let active = (0..input.shape.len())
                .map(|axis| weights.shape().get(axis).is_some_and(|&width| width > 1))
                .collect::<Vec<_>>();
            let halo_cursor = fft::limit_source_cursor(
                convolution_cursor(halo_section.dense_shape(&size), &active)?,
                input.stride,
                weights.shape(),
            );
            for piece in halo_section.cursors(halo_cursor) {
                let piece = piece?;
                let (data, mask) = reader.read(
                    &halo_section.source_start(&piece.position),
                    &piece.shape,
                    input.stride,
                )?;
                let mut data = data.mapv(|value| {
                    let value = value.into();
                    if exclude_nonfinite && !value.is_finite() {
                        0.
                    } else {
                        value
                    }
                });
                if let Some(mask) = mask {
                    Zip::from(&mut data).and(&mask).for_each(|value, &valid| {
                        if !valid {
                            *value = 0.;
                        }
                    });
                }
                let data_start = piece
                    .position
                    .iter()
                    .zip(&origin)
                    .map(|(&i, &lo)| i + lo)
                    .collect::<Vec<_>>();
                let data_region = Window {
                    start: &data_start,
                    shape: &piece.shape,
                };
                if let Some(convolved) = fft::convolve_block(&data, &weights)? {
                    let block = Window {
                        start: &data_start,
                        shape: convolved.shape(),
                    };
                    accumulate(
                        result,
                        output_region,
                        &convolved,
                        block,
                        delta.iter().copied(),
                        1.,
                    );
                } else {
                    for (index, &weight) in weights.indexed_iter() {
                        let offsets = delta.iter().enumerate().map(|(axis, &lo)| {
                            lo + index.slice().get(axis).copied().unwrap_or_default() as isize
                        });
                        accumulate(result, output_region, &data, data_region, offsets, weight);
                    }
                }
            }
        }
        Ok(())
    }
}

/// Scaling, optional mask and final native destination for a convolution.
pub struct ConvolutionOptions<'a> {
    /// A positive multiplier, or a nonpositive value for unit-sum scaling.
    pub scale: f64,
    /// The optional expression defining valid image pixels.
    pub mask: Option<&'a str>,
    /// Permit degenerate mask axes to extend across the image.
    pub stretch: bool,
    /// Final output path, for rejecting aliases of expression operands.
    pub destination: Option<&'a Path>,
}

enum Scaling {
    Multiply(f64),
    Divide(f64),
}

impl Scaling {
    fn apply<T: ConvolutionPixel>(&self, value: f64) -> f64 {
        let result = match *self {
            Self::Multiply(factor) => value * T::from_f64(factor).into(),
            Self::Divide(sum) => value / T::from_f64(sum).into(),
        };
        T::from_f64(result).into()
    }
}

fn accumulate(
    result: &mut ArrayD<f64>,
    destination: Window<'_>,
    input: &ArrayD<f64>,
    source: Window<'_>,
    offsets: impl Iterator<Item = isize>,
    weight: f64,
) {
    if let Some(overlap) = destination.overlap(source, offsets) {
        let source = input.slice_each_axis(|axis| overlap[axis.axis.index()].0);
        let target = result.slice_each_axis_mut(|axis| overlap[axis.axis.index()].1);
        Zip::from(target)
            .and(source)
            .for_each(|accum, &value| *accum += weight * value);
    }
}

// Extend a tile along the axes actually convolved before batching planes.
// A storage-major cube tile can otherwise contain one sample per FFT lane.
fn convolution_cursor(shape: Vec<usize>, active: &[bool]) -> Result<Vec<usize>, ImageError> {
    let axes = (0..shape.len())
        .filter(|&axis| active[axis])
        .chain((0..shape.len()).filter(|&axis| !active[axis]));
    let axis_order = axes.collect::<Vec<_>>();
    let mut cursor = vec![0; shape.len()];
    let ordered = bounded_read_cursor(axis_order.iter().map(|&axis| shape[axis]).collect())?;
    for (&axis, width) in axis_order.iter().zip(ordered) {
        cursor[axis] = width;
    }
    Ok(cursor)
}

fn tiles(shape: &[usize], cursor: Vec<usize>) -> TraversalCursorIter {
    TraversalCursorIter::new(shape.to_vec(), cursor, TraversalSpec::tiles())
}

fn slice(start: usize, length: usize) -> Slice {
    Slice::from(start..start + length)
}

#[derive(Clone, Copy)]
struct Window<'a> {
    start: &'a [usize],
    shape: &'a [usize],
}

impl<'a> From<&'a TraversalCursor> for Window<'a> {
    fn from(tile: &'a TraversalCursor) -> Self {
        Self {
            start: &tile.position,
            shape: &tile.shape,
        }
    }
}

fn offset_tile(mut tile: TraversalCursor, offset: &[usize]) -> TraversalCursor {
    for (position, &shift) in tile.position.iter_mut().zip(offset) {
        *position += shift;
    }
    tile
}

impl Window<'_> {
    fn halo(
        self,
        image: &[usize],
        low: &[isize],
        kernel_shape: &[usize],
    ) -> Option<(Vec<usize>, Vec<usize>)> {
        let mut start = Vec::with_capacity(image.len());
        let mut shape = Vec::with_capacity(image.len());
        for (axis, &length) in image.iter().enumerate() {
            let width = kernel_shape.get(axis).copied().unwrap_or(1);
            let high = low[axis] + (width - 1) as isize;
            let first = (self.start[axis] as isize - high).max(0) as usize;
            let end =
                (self.start[axis] as isize + self.shape[axis] as isize - low[axis]).max(0) as usize;
            let end = end.min(length);
            if first >= end {
                return None;
            }
            start.push(first);
            shape.push(end - first);
        }
        Some((start, shape))
    }

    fn overlap(
        self,
        input: Self,
        shifts: impl Iterator<Item = isize>,
    ) -> Option<Vec<(Slice, Slice)>> {
        shifts
            .enumerate()
            .map(|(axis, shift)| {
                let aligned = input.start[axis] as isize + shift;
                let first = (self.start[axis] as isize).max(aligned);
                let last = ((self.start[axis] + self.shape[axis]) as isize)
                    .min(aligned + input.shape[axis] as isize);
                if first >= last {
                    return None;
                }
                let length = (last - first) as usize;
                Some((
                    slice((first - aligned) as usize, length),
                    slice(first as usize - self.start[axis], length),
                ))
            })
            .collect()
    }
}

// SPDX-License-Identifier: LGPL-3.0-or-later

use casa_lattices::{TraversalCacheHint, TraversalCacheMode};
use ndarray::{ArrayD, Axis, Dimension, IxDyn, Slice, Zip, indices};

use casa_images::{ImageError, ImageInterface, ImagePixel, MutableImageInterface, TempImage};

use super::{SubimageRegion, region::SubimageSelection};

/// Write supplied mask bits, then write pixels using the active mask if requested.
///
/// Supply a fresh default-mask name when the image has no default mask.
/// With replication, place each sample at the region's stride and repeat
/// along axes omitted from the array.
///
/// # Errors
/// Returns an error for invalid arrays or regions, or failed image access.
pub fn put_image_region<T: ImagePixel>(
    image: &mut impl MutableImageInterface<T>,
    pixels: Option<ArrayD<T>>,
    mask: Option<ArrayD<bool>>,
    region: Option<SubimageRegion<'_>>,
    fresh_mask_name: &str,
    usemask: bool,
    replicate: bool,
) -> Result<(), ImageError> {
    let array_shape = validate_put_region_shapes(
        pixels.as_ref().map(ArrayD::shape),
        mask.as_ref().map(ArrayD::shape),
    )?;
    let temporary = image
        .as_any()
        .and_then(|image| image.downcast_ref::<TempImage<T>>());
    let component = temporary.is_some_and(|image| image.image_type_name() == "ComponentListImage");
    let concat = temporary.is_some_and(|image| image.concat_source().is_some());
    if !image.is_writable() && !component && !concat {
        return Err(ImageError::CasaSelection(
            "This image is not writable".into(),
        ));
    }
    let selection = ImageRegionWrite::new(image, region, array_shape, replicate)?;
    // CASA's mask attacher leaves a temporary component image unmasked.
    let mask = if (component && image.name().is_none()) || (concat && !image.has_pixel_mask()) {
        None
    } else {
        mask
    };
    let tile = image.nice_cursor_shape();
    let small_edit =
        tile != image.shape() && selection.count.iter().product::<usize>() <= tile.iter().product();
    if small_edit {
        // Keep a small edit from loading and unpacking every tile of a native mask.
        let hint = TraversalCacheHint {
            mode: TraversalCacheMode::Tiles,
            section_start: selection.start.clone(),
            section_shape: selection.count.clone(),
            requested_cursor: tile,
            axis_path: (0..image.ndim()).collect(),
        };
        image.with_traversal_cache_hint_mut(&hint, |image| {
            Ok(selection.apply(image, pixels, mask, fresh_mask_name, usemask))
        })?
    } else {
        selection.apply(image, pixels, mask, fresh_mask_name, usemask)
    }
}

/// Check buffer geometry before opening the image for a region write.
///
/// # Errors
/// Both buffers are absent, their shapes differ, or the shape has no pixels.
pub fn validate_put_region_shapes<'a>(
    pixel_shape: Option<&'a [usize]>,
    mask_shape: Option<&'a [usize]>,
) -> Result<&'a [usize], ImageError> {
    if let Some((pixels, mask)) = pixel_shape.zip(mask_shape)
        && pixels != mask
    {
        return Err(ImageError::CasaSelection(
            "Pixels and mask arrays have different shapes".into(),
        ));
    }
    let array_shape = pixel_shape.or(mask_shape).ok_or_else(|| {
        ImageError::CasaSelection("You must specify at least either the pixels or the mask".into())
    })?;
    if array_shape.is_empty() || array_shape.contains(&0) {
        return Err(ImageError::CasaSelection(
            "The shape of the data array is invalid".into(),
        ));
    }
    Ok(array_shape)
}

struct ImageRegionWrite {
    start: Vec<usize>,
    count: Vec<usize>,
    stride: Vec<usize>,
}

impl ImageRegionWrite {
    fn new<T: ImagePixel>(
        image: &dyn ImageInterface<T>,
        region: Option<SubimageRegion<'_>>,
        array_shape: &[usize],
        replicate: bool,
    ) -> Result<Self, ImageError> {
        if array_shape.len() > image.ndim() {
            return Err(ImageError::CasaSelection(
                "Data array has more axes than the image".into(),
            ));
        }
        let selection = SubimageSelection::resolve_section(image, None, region, None, None)?;
        let region_shape = selection.packed_shape();
        if array_shape
            .iter()
            .zip(&region_shape)
            .enumerate()
            .any(|(axis, (&length, &extent))| {
                length != extent && (length != 1 || axis + 1 != array_shape.len())
            })
        {
            return Err(ImageError::CasaSelection(format!(
                "Data array shape ({array_shape:?}) including inc, does not match the shape of the region bounding box ({region_shape:?})"
            )));
        }
        let (count, stride) = if replicate {
            (
                region_shape,
                selection.stride.unwrap_or_else(|| vec![1; image.ndim()]),
            )
        } else {
            let mut count = array_shape.to_vec();
            count.resize(image.ndim(), 1);
            (count, vec![1; image.ndim()])
        };
        Ok(Self {
            start: selection.start,
            count,
            stride,
        })
    }

    fn apply<T: ImagePixel>(
        &self,
        image: &mut impl MutableImageInterface<T>,
        pixels: Option<ArrayD<T>>,
        mask: Option<ArrayD<bool>>,
        fresh_mask_name: &str,
        usemask: bool,
    ) -> Result<(), ImageError> {
        if let Some(mask) = mask {
            let name = if let Some(name) = image.active_pixel_mask_name() {
                if !image.named_mask_is_writable(&name)? {
                    return Err(ImageError::CasaSelection("The mask is not writable".into()));
                }
                name
            } else {
                image.create_filled_mask(fresh_mask_name, true)?;
                image.set_default_mask(fresh_mask_name)?;
                fresh_mask_name.to_owned()
            };
            let mask = pad(mask, image.ndim());
            image.with_mask_slice_writer(&name, |put| self.write(&mask, put))?;
        }
        if let Some(pixels) = pixels {
            let mut pixels = pad(pixels, image.ndim());
            if usemask {
                let ones = vec![1; image.ndim()];
                if let Some(validity) = image.get_mask_slice(&self.start, pixels.shape(), &ones)? {
                    let previous = image.get_slice(&self.start, pixels.shape(), &ones)?;
                    Zip::from(&mut pixels)
                        .and(&previous)
                        .and(&validity)
                        .for_each(|pixel, &previous, &valid| {
                            if !valid {
                                *pixel = previous;
                            }
                        });
                }
            }
            self.write(&pixels, |values, start| {
                image.put_slice(values, start).map_err(Into::into)
            })?;
        }
        Ok(())
    }

    // Write contiguous blocks when possible. Splitting strided axes keeps
    // memory bounded even when only a few samples span a large pixel window.
    fn write<T: Copy>(
        &self,
        values: &ArrayD<T>,
        mut put: impl FnMut(&ArrayD<T>, &[usize]) -> Result<(), ImageError>,
    ) -> Result<(), ImageError> {
        let contiguous = self.stride.iter().all(|&step| step == 1);
        let block_shape: Vec<_> = values
            .shape()
            .iter()
            .zip(&self.stride)
            .map(|(&length, &step)| if step == 1 { length } else { 1 })
            .collect();
        let grid: Vec<_> = self
            .count
            .iter()
            .zip(&block_shape)
            .map(|(&count, &block)| count / block)
            .collect();
        for index in indices(IxDyn(&grid)) {
            let origin: Vec<_> = index
                .slice()
                .iter()
                .enumerate()
                .map(|(axis, &block)| {
                    self.start[axis] + block * block_shape[axis] * self.stride[axis]
                })
                .collect();
            if contiguous {
                put(values, &origin)?;
            } else {
                let block = values.slice_each_axis(|axis| {
                    let dimension = axis.axis.index();
                    let start = index[dimension] * block_shape[dimension] % axis.len;
                    Slice::from(start..start + block_shape[dimension])
                });
                put(&block.to_owned(), &origin)?;
            }
        }
        Ok(())
    }
}

fn pad<T>(mut data: ArrayD<T>, ndim: usize) -> ArrayD<T> {
    while data.ndim() < ndim {
        let axis = Axis(data.ndim());
        data = data.insert_axis(axis);
    }
    data
}

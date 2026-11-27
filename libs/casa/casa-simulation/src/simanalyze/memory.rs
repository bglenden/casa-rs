// SPDX-License-Identifier: LGPL-3.0-or-later
//! Whole-image storage and reusable direction-plane workspace.
use super::failure;
use crate::simobserve::SimobserveError;
use crate::simutil::skymodel::pixel_axes;
use casa_coordinates::CoordinateType;
use casa_images::PagedImage;
use std::path::Path;

#[derive(Clone, Copy)]
pub(super) struct ImageGrid {
    cells: u64,
    plane: u64,
}

impl ImageGrid {
    pub(super) fn read(path: &Path) -> Result<Self, SimobserveError> {
        let image = PagedImage::<f32>::open(path).map_err(failure)?;
        let (_, axes) = pixel_axes(image.coordinates(), CoordinateType::Direction)
            .ok_or_else(|| failure("image has no direction coordinate"))?;
        Self::from_shape(image.shape(), &axes)
    }

    fn from_shape(shape: &[usize], axes: &[usize]) -> Result<Self, SimobserveError> {
        if axes.len() != 2 || axes[0] == axes[1] || axes.iter().any(|&axis| axis >= shape.len()) {
            return Err(failure("image direction axes are invalid"));
        }
        let product = |sizes: &[usize]| {
            sizes
                .iter()
                .try_fold(1u64, |count, &size| count.checked_mul(size as u64))
                .ok_or_else(|| failure("image size overflows"))
        };
        Ok(Self {
            cells: product(shape)?,
            plane: product(&[shape[axes[0]], shape[axes[1]]])?,
        })
    }
}

pub(super) fn feather(high: ImageGrid, low: ImageGrid) -> Result<u64, SimobserveError> {
    // Whole cubes: high, aligned low and output f32 pixels/masks, plus
    // image-writer copies (28 B/high cell); low input and read copies
    // (16 B/low cell). Flat-image preparation runs in the same reservation.
    // Crossover, FFTs, shifts and interpolation scratch are direction-plane
    // arrays reused between channels/Stokes, not one set per cube cell.
    high.cells
        .checked_mul(28)
        .and_then(|bytes| low.cells.checked_mul(16)?.checked_add(bytes))
        .and_then(|bytes| high.plane.checked_mul(128)?.checked_add(bytes))
        .and_then(|bytes| bytes.checked_add(64 << 20))
        .ok_or_else(|| failure("feather memory overflows"))
}

pub(super) fn flat_images(source: ImageGrid, output: ImageGrid) -> Result<u64, SimobserveError> {
    // flatimage reads a cube and its mask but produces one Stokes-I mean
    // plane. Keep full-image read/copy headroom; charge plane-sized writers
    // separately. Convolution and fidelity are not requested in this path.
    source
        .cells
        .checked_add(output.cells)
        .and_then(|cells| cells.checked_mul(16))
        .and_then(|bytes| {
            source
                .plane
                .checked_add(output.plane)?
                .checked_mul(96)?
                .checked_add(bytes)
        })
        .and_then(|bytes| bytes.checked_add(128 << 20))
        .ok_or_else(|| failure("flat-image memory overflows"))
}

#[cfg(test)]
mod tests {
    use super::{ImageGrid, feather, flat_images};

    #[test]
    fn full_upstream_cube_fits_the_authorized_budget_without_per_channel_fft_scratch() {
        let output = ImageGrid::from_shape(&[1458, 1458, 1, 128], &[0, 1]).unwrap();
        let source = ImageGrid::from_shape(&[256, 256, 1, 128], &[0, 1]).unwrap();
        // The old cube-wide FFT charge asked for 43.6 GB for this case.
        let budget = 13_130_571_776u64;
        assert!(feather(output, output).unwrap() < budget);
        assert!(flat_images(source, output).unwrap() < budget);
        let plane = ImageGrid::from_shape(&[1458, 1458, 1, 1], &[0, 1]).unwrap();
        // Cubes retain more pixels, but do not replicate plane scratch.
        assert!(feather(output, output).unwrap() < 128 * feather(plane, plane).unwrap());
    }

    #[test]
    fn direction_plane_geometry_is_independent_of_axis_order() {
        let a = ImageGrid::from_shape(&[1458, 1458, 2, 128], &[0, 1]).unwrap();
        let b = ImageGrid::from_shape(&[2, 128, 1458, 1458], &[2, 3]).unwrap();
        assert_eq!(feather(a, a).unwrap(), feather(b, b).unwrap());
        assert!(ImageGrid::from_shape(&[2, 128], &[0, 2]).is_err());
        assert!(ImageGrid::from_shape(&[2, 128], &[0, 0]).is_err());
    }

    #[test]
    fn image_storage_and_workspace_overflow_are_refused() {
        let enormous = ImageGrid {
            cells: u64::MAX,
            plane: 1,
        };
        let huge_plane = ImageGrid {
            cells: 1,
            plane: u64::MAX,
        };
        let small = ImageGrid { cells: 1, plane: 1 };
        assert!(feather(enormous, small).is_err());
        assert!(feather(small, enormous).is_err());
        assert!(feather(huge_plane, small).is_err());
        assert!(flat_images(enormous, small).is_err());
        assert!(flat_images(small, huge_plane).is_err());
    }
}

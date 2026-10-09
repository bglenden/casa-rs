// SPDX-License-Identifier: LGPL-3.0-or-later
//! Shifted subtraction of a PSF or scale patch from a residual plane.

use crate::Error;
use crate::plane::PlaneShape;

/// Subtract `scale × source`, shifted so that `centre` of the source lands
/// on `at` of the target, over CASA's update box: offsets `−h ..= h − 1`
/// from `at` on each axis, clipped to both planes.
///
/// This is the box of `hclean` (`x1 = px − nx/2`, `x2 = px + nx/2 − 1`), of
/// `MatrixCleaner::clean` (a full-image box) and of the multi-term cleaner's
/// PSF patch (`buildImagePatches`). Their image and patch boxes stay
/// aligned because the source centre sits `h` from its low edge.
#[allow(clippy::too_many_arguments)]
pub(crate) fn subtract_window(
    target: &mut [f64],
    target_shape: PlaneShape,
    source: &[f64],
    source_shape: PlaneShape,
    centre: [usize; 2],
    at: [usize; 2],
    half: [usize; 2],
    scale: f64,
) -> Result<(), Error> {
    if !scale.is_finite() {
        return Err(Error::NonFinite);
    }
    let axis = |at: usize, centre: usize, half: usize, target: usize, source: usize| {
        // Offsets d with 0 <= at + d < target and 0 <= centre + d < source.
        let low = (at.min(half)).min(centre) as isize;
        let high = ((target - at).min(half)).min(source - centre) as isize;
        (-low)..high
    };
    let xs = axis(at[0], centre[0], half[0], target_shape.nx, source_shape.nx);
    let ys = axis(at[1], centre[1], half[1], target_shape.ny, source_shape.ny);
    if ys.is_empty() {
        return Ok(());
    }
    let length = (ys.end - ys.start) as usize;
    for dx in xs {
        let tx = (at[0] as isize + dx) as usize;
        let sx = (centre[0] as isize + dx) as usize;
        let ty = (at[1] as isize + ys.start) as usize;
        let sy = (centre[1] as isize + ys.start) as usize;
        let row = &mut target[tx * target_shape.ny + ty..][..length];
        let kernel = &source[sx * source_shape.ny + sy..][..length];
        for (value, coefficient) in row.iter_mut().zip(kernel) {
            *value -= scale * coefficient;
        }
    }
    Ok(())
}

/// Subtract `flux ×` the PSF shifted from its peak to `at`, over `hclean`'s
/// box of half the plane on each side.
pub(crate) fn subtract_shifted(
    residual: &mut [f64],
    psf: &[f64],
    shape: PlaneShape,
    psf_peak: usize,
    at: usize,
    flux: f64,
) -> Result<(), Error> {
    subtract_window(
        residual,
        shape,
        psf,
        shape,
        shape.pixel(psf_peak),
        shape.pixel(at),
        shape.centre(),
        flux,
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The window subtraction equals a direct sum over the CASA box for
    /// every target position, on an odd non-square plane with an
    /// off-centre source centre.
    #[test]
    fn the_window_is_the_clipped_casa_box() {
        let target_shape = PlaneShape::new(7, 5);
        let source_shape = PlaneShape::new(6, 9);
        let source = (0..source_shape.len())
            .map(|i| (i as f64 * 0.37).sin())
            .collect::<Vec<_>>();
        let centre = [2, 5];
        let half = [3, 2];
        for at in 0..target_shape.len() {
            let mut actual = vec![1.0; target_shape.len()];
            let at_pixel = target_shape.pixel(at);
            subtract_window(
                &mut actual,
                target_shape,
                &source,
                source_shape,
                centre,
                at_pixel,
                half,
                0.5,
            )
            .unwrap();
            let mut expected = vec![1.0; target_shape.len()];
            for dx in -(half[0] as isize)..half[0] as isize {
                for dy in -(half[1] as isize)..half[1] as isize {
                    let t = [at_pixel[0] as isize + dx, at_pixel[1] as isize + dy];
                    let s = [centre[0] as isize + dx, centre[1] as isize + dy];
                    if (0..7).contains(&t[0])
                        && (0..5).contains(&t[1])
                        && (0..6).contains(&s[0])
                        && (0..9).contains(&s[1])
                    {
                        expected[target_shape.index(t[0] as usize, t[1] as usize)] -=
                            0.5 * source[source_shape.index(s[0] as usize, s[1] as usize)];
                    }
                }
            }
            assert_eq!(actual, expected, "at {at_pixel:?}");
        }
    }

    #[test]
    fn a_nonfinite_scale_is_refused() {
        let shape = PlaneShape::new(4, 4);
        let mut target = vec![0.0; 16];
        assert_eq!(
            subtract_shifted(&mut target, &[1.0; 16], shape, 10, 5, f64::NAN),
            Err(Error::NonFinite)
        );
    }
}

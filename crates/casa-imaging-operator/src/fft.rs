// SPDX-License-Identifier: LGPL-3.0-or-later
//! Centred two-dimensional FFTs over `[y][x]` planes: the one centring
//! implementation of the operator.

use casa_fft::{Fft2, FftScalar};
use num_complex::Complex;

use crate::error::OperatorError;

/// A reusable single-threaded plan for one grid shape whose origin is the
/// centre cell `(nx/2, ny/2)` in both domains.
///
/// Per-plane transforms run on the worker team, never inside FFTW's own
/// thread pool (plan section 5.9). Both directions are unnormalised.
pub(crate) struct PlaneFft<T: FftScalar> {
    fft: Fft2<T>,
    shape: [usize; 2],
}

impl<T: FftScalar> PlaneFft<T> {
    /// Plan for a `[nx, ny]` grid with even extents. `measured` selects
    /// FFTW's measured planning, which costs seconds on a large grid and
    /// pays off only when many planes share the plan; otherwise FFTW's
    /// estimate is used.
    pub(crate) fn new(shape: [usize; 2], measured: bool) -> Result<Self, OperatorError> {
        let fft = Fft2::with_threads([shape[1], shape[0]], 1)?;
        Ok(Self {
            fft: if measured {
                fft
            } else {
                fft.with_estimated_plan()
            },
            shape,
        })
    }

    /// Transform `plane` in place; `inverse` selects FFTW `BACKWARD`
    /// (`e^{+i}`), which the adjoint uses to form images.
    pub(crate) fn transform(
        &mut self,
        plane: &mut [Complex<T>],
        inverse: bool,
    ) -> Result<(), OperatorError> {
        shift_even(plane, self.shape);
        self.fft.transform(plane, inverse)?;
        shift_even(plane, self.shape);
        Ok(())
    }
}

/// Swap the four quadrants of an even `[nx, ny]` plane so the centre cell
/// becomes the storage origin and back.
pub(crate) fn shift_even<T>(plane: &mut [T], [nx, ny]: [usize; 2]) {
    debug_assert_eq!(plane.len(), nx * ny);
    debug_assert!(nx % 2 == 0 && ny % 2 == 0);
    let (hx, hy) = (nx / 2, ny / 2);
    for y in 0..hy {
        for x in 0..hx {
            plane.swap(y * nx + x, (y + hy) * nx + x + hx);
            plane.swap(y * nx + x + hx, (y + hy) * nx + x);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use num_complex::Complex64;

    #[test]
    fn shift_moves_the_centre_to_the_origin_and_is_an_involution() {
        let shape = [4, 2];
        let mut plane = (0..8)
            .map(|v| Complex64::new(f64::from(v), 0.0))
            .collect::<Vec<_>>();
        shift_even(&mut plane, shape);
        let values = plane.iter().map(|v| v.re as u8).collect::<Vec<_>>();
        assert_eq!(values, [6, 7, 4, 5, 2, 3, 0, 1]);
        shift_even(&mut plane, shape);
        let values = plane.iter().map(|v| v.re as u8).collect::<Vec<_>>();
        assert_eq!(values, [0, 1, 2, 3, 4, 5, 6, 7]);
    }

    #[test]
    fn a_centre_impulse_transforms_to_a_flat_plane() {
        let shape = [8, 6];
        let mut fft = PlaneFft::<f64>::new(shape, true).expect("plan");
        let mut plane = vec![Complex64::default(); 48];
        plane[3 * 8 + 4] = Complex64::new(2.0, 0.0);
        fft.transform(&mut plane, true).expect("transform");
        assert!(
            plane
                .iter()
                .all(|v| (v - Complex64::new(2.0, 0.0)).norm() < 1e-12)
        );
    }
}

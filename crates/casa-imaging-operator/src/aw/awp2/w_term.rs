// SPDX-License-Identifier: LGPL-3.0-or-later

//! The untapered W screen and convolution used by CASA AWPLPG.

use num_complex::Complex32;

use crate::error::OperatorError;
use crate::fft::PlaneFft;

use super::{central_sum, diagonal_support_relative};

pub(super) struct WTerm {
    side: usize,
    cell_rad: f64,
    fft: PlaneFft<f32>,
    screen: Vec<Complex32>,
    aperture: Vec<Complex32>,
}

impl WTerm {
    pub(super) fn new(side: usize, cell_rad: f64) -> Result<Self, OperatorError> {
        Ok(Self {
            side,
            cell_rad,
            fft: PlaneFft::new([side; 2], false)?,
            screen: vec![Complex32::default(); side * side],
            aperture: vec![Complex32::default(); side * side],
        })
    }

    // WPConvFunc::makeWConvFuncs crops all transformed W planes to the
    // support of the largest W. AWConvFunc uses max(cropped W, cropped A).
    pub(super) fn convolution_side(&mut self, maximum_w: f64) -> Result<usize, OperatorError> {
        self.transform_screen(maximum_w)?;
        let support = screen_support(&self.screen, self.side);
        Ok((2 * (support + 2)).min(self.side))
    }

    pub(super) fn prepare(
        &mut self,
        w: f64,
        w_crop_side: usize,
        convolution_side: usize,
        fft: &mut PlaneFft<f32>,
    ) -> Result<Vec<Complex32>, OperatorError> {
        self.transform_screen(w)?;
        let mut screen = vec![Complex32::default(); convolution_side * convolution_side];
        insert_centre(
            &mut screen,
            convolution_side,
            &self.screen,
            self.side,
            w_crop_side,
        );
        fft.transform(&mut screen, true)?;
        let normalisation = (convolution_side * convolution_side) as f32;
        screen.iter_mut().for_each(|value| *value /= normalisation);
        Ok(screen)
    }

    pub(super) fn convolve(
        &mut self,
        image: &[Complex32],
        image_side: usize,
        screen: &[Complex32],
        side: usize,
        fft: &mut PlaneFft<f32>,
    ) -> Result<&[Complex32], OperatorError> {
        let aperture = &mut self.aperture[..side * side];
        aperture.fill(Complex32::default());
        insert_centre(aperture, side, image, image_side, image_side);
        fft.transform(aperture, true)?;
        let normalisation = (side * side) as f32;
        for (beam, phase) in aperture.iter_mut().zip(screen) {
            *beam = (*beam / normalisation) * phase;
        }
        fft.transform(aperture, false)?;
        Ok(aperture)
    }

    fn transform_screen(&mut self, w: f64) -> Result<(), OperatorError> {
        fill_screen(&mut self.screen, self.side, self.cell_rad, w);
        self.fft.transform(&mut self.screen, false)
    }
}

/// WPConvFunc::makeSkyWFunc, without the spheroidal taper used by WProjectFT.
fn fill_screen(screen: &mut [Complex32], side: usize, cell_rad: f64, w: f64) {
    let half = (side / 2) as f64;
    let two_pi_w = std::f64::consts::TAU * w;
    for (y, row) in screen.chunks_exact_mut(side).enumerate() {
        let m = (y as f64 - half) * cell_rad;
        for (x, value) in row.iter_mut().enumerate() {
            let l = (x as f64 - half) * cell_rad;
            let r_squared = l * l + m * m;
            *value = if r_squared < 1.0 {
                let phase = two_pi_w * ((1.0 - r_squared).sqrt() - 1.0);
                let (sin, cos) = phase.sin_cos();
                Complex32::new(cos as f32, sin as f32)
            } else {
                Complex32::default()
            };
        }
    }
}

fn screen_support(screen: &[Complex32], side: usize) -> usize {
    let (minimum, maximum) = screen
        .iter()
        .map(|value| value.norm())
        .fold((f32::INFINITY, 0.0_f32), |(minimum, maximum), amplitude| {
            (minimum.min(amplitude), maximum.max(amplitude))
        });
    let centre = side / 2;
    (0..centre)
        .find(|offset| {
            let sample = screen[(centre + offset) * side + centre].norm();
            (sample - minimum) / (maximum - minimum) < 0.01
        })
        .map_or(centre, |offset| offset + 1)
}

pub(super) fn insert_centre(
    target: &mut [Complex32],
    target_side: usize,
    source: &[Complex32],
    source_side: usize,
    crop_side: usize,
) {
    let source_start = (source_side - crop_side) / 2;
    let target_start = (target_side - crop_side) / 2;
    for row in 0..crop_side {
        let from = (source_start + row) * source_side + source_start;
        let to = (target_start + row) * target_side + target_start;
        target[to..to + crop_side].copy_from_slice(&source[from..from + crop_side]);
    }
}

pub(super) struct AwSupport {
    /// W=0 determines the cutoff and the normalisation of every W cell
    /// at this frequency (AWConvFunc::supportResizeAWConv).
    peak: f32,
    inverse_sum: f32,
}

impl AwSupport {
    pub(super) fn new(plane: &[Complex32], side: usize, aperture_support: usize) -> Self {
        let peak = plane.iter().map(|value| value.norm()).fold(0.0, f32::max);
        let support = diagonal_support_relative(plane, side, 0.01 * peak)
            .unwrap_or(5)
            .max(5)
            .max(aperture_support);
        let sum = central_sum(plane, side, support.min(side / 2), false);
        Self {
            peak,
            inverse_sum: if sum > 0.0 { sum.recip() } else { 1.0 },
        }
    }

    pub(super) fn half_support(
        &self,
        plane: &[Complex32],
        side: usize,
        aperture_support: usize,
    ) -> usize {
        diagonal_support_relative(plane, side, 0.01 * self.peak)
            .unwrap_or(5)
            .max(5)
            .max(aperture_support)
    }

    pub(super) fn inverse_sum(&self) -> f32 {
        self.inverse_sum
    }

    pub(super) fn normalize(&self, plane: &mut [Complex32]) {
        plane
            .iter_mut()
            .for_each(|value| *value *= self.inverse_sum);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn nonzero_w_support_uses_the_zero_plane_peak_and_sum() {
        let side = 32;
        let mut zero = vec![Complex32::default(); side * side];
        let centre = (side / 2) * (side + 1);
        zero[centre] = Complex32::new(10.0, 0.0);
        let normalisation = AwSupport::new(&zero, side, 6);
        let mut nonzero = vec![Complex32::default(); side * side];
        nonzero[centre] = Complex32::new(1.0, 0.0);
        nonzero[centre - 8 * (side + 1)] = Complex32::new(0.12, 0.0);
        nonzero[centre - 12 * (side + 1)] = Complex32::new(0.09, 0.0);
        assert_eq!(normalisation.half_support(&nonzero, side, 6), 11);
        normalisation.normalize(&mut nonzero);
        assert_eq!(nonzero[centre], Complex32::new(0.1, 0.0));
    }

    #[test]
    fn w_screen_uses_the_casa_phase_and_largest_plane_crop() {
        let w = 8_608.479_602_376_43;
        let cell_rad = 9.375e-5;
        let mut term = WTerm::new(1024, cell_rad).unwrap();
        term.transform_screen(w).unwrap();
        let centre = term.screen[512 * 1024 + 512];
        assert!((centre - Complex32::new(499.056_34, -11_472.457)).norm() < 0.02);
        assert_eq!(screen_support(&term.screen, 1024), 97);
        assert_eq!(term.convolution_side(53_802.997_514_852_686).unwrap(), 1024);
    }
}

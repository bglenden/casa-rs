// SPDX-License-Identifier: LGPL-3.0-or-later
//! Simulator SimplePBConvFunc: one common telescope beam, fine FFT sampling,
//! last-frequency scaling and integral normalization over the cropped plane.
use super::{
    MosaicPb, MosaicRules, MosaicWindow, cells::FrequencyCells, frequency_cell_bytes,
    screen::PairLattices,
};
use crate::convolution::{DenseCell, ImageCorrection, MuellerRouting};
use crate::dense::{OversampledPlane, dense_cell, dense_cell_bytes};
use crate::error::OperatorError;
use crate::fft::PlaneFft;
use crate::geometry::next_larger_even_composite;
use crate::{GridGeometry, PolarizationRouting};
use casa_numerics::PrimaryBeam;
use num_complex::Complex32;

struct Layout {
    side: usize,
    crop: usize,
    sampling: u16,
    increments_deg: [f64; 2],
    windows: Vec<FrequencyCells>,
    cells: usize,
}

impl Layout {
    fn new(geometry: &GridGeometry, windows: &[MosaicWindow]) -> Result<Self, OperatorError> {
        let image = geometry.image();
        let [nx, ny] = image.shape;
        let largest = nx.max(ny);
        let sampling = if largest < 50 {
            100
        } else {
            (5000. / largest as f64).ceil().max(1.) as u16
        };
        // CompositeNumber::nextLargerEven is strictly larger, including
        // when the requested size is already an even composite.
        let side = next_larger_even_composite(largest / 2 * 2 * 2 * usize::from(sampling) + 1);
        let crop = side / 8 / 2 * 2;
        let increments_deg = std::array::from_fn(|axis| {
            (image.increment_rad[axis] * 2. * f64::from(sampling) * image.shape[axis] as f64
                / side as f64)
                .to_degrees()
        });
        let mut cells = 0;
        let windows = windows
            .iter()
            .map(|window| {
                let window = FrequencyCells::new(MosaicRules::Simulator, window, cells)?;
                cells += window.frequencies_hz.len();
                Ok(window)
            })
            .collect::<Result<Vec<_>, OperatorError>>()?;
        if windows.is_empty() || crop < 2 {
            return Err(invalid(
                "a simple mosaic needs windows and a usable image extent",
            ));
        }
        Ok(Self {
            side,
            crop,
            sampling,
            increments_deg,
            windows,
            cells,
        })
    }
}

/// First-stage common-beam convolution planes, admitted before allocation.
pub struct SimpleMosaicScreens {
    layout: Layout,
    planes: Vec<Vec<PairLattices>>,
    support: Vec<usize>,
    grid: [usize; 2],
    mueller: MuellerRouting,
}

impl MosaicPb {
    /// Peak bytes to construct SimplePBConvFunc's common-beam screens.
    /// Includes the Float FFT plane and planning, retained cropped planes,
    /// frequency scaling scratch, the voltage table and routing.
    pub fn simple_screens_bytes(
        geometry: &GridGeometry,
        windows: &[MosaicWindow],
    ) -> Result<u64, OperatorError> {
        let layout = Layout::new(geometry, windows)?;
        let pixels = layout.side * layout.side;
        Ok((pixels * size_of::<Complex32>()
            + (layout.cells + 2) * 2 * layout.crop * layout.crop * size_of::<Complex32>()
            + layout.windows.len() * (size_of::<Vec<PairLattices>>() + size_of::<usize>())
            + layout.cells * size_of::<PairLattices>()) as u64
            + casa_fft::planning_bytes::<f32>(pixels)
            + casa_numerics::BeamPattern::BYTES
            + frequency_cell_bytes(&layout.windows)
            + MuellerRouting::MAXIMUM_BYTES)
    }

    /// Construct the simulator's SimplePBConvFunc planes, for telescopes
    /// whose default common beam supplies every antenna's response.
    pub fn simple_screens(
        geometry: &GridGeometry,
        polarization: &PolarizationRouting,
        beam: PrimaryBeam,
        windows: &[MosaicWindow],
    ) -> Result<SimpleMosaicScreens, OperatorError> {
        Self::simple_pattern_screens(geometry, polarization, &beam.pattern(), windows)
    }

    /// Construct common-beam planes from an already tabulated voltage curve.
    pub fn simple_pattern_screens(
        geometry: &GridGeometry,
        polarization: &PolarizationRouting,
        pattern: &casa_numerics::BeamPattern,
        windows: &[MosaicWindow],
    ) -> Result<SimpleMosaicScreens, OperatorError> {
        Self::simple_sampled_screens(
            geometry,
            polarization,
            windows,
            |plane, side, increments, last, _window, weight| {
                let at = pattern.at_frequency(last);
                let centre = side / 2;
                for y in 0..side {
                    let ry = ((y as f64 - centre as f64) * increments[1]).powi(2) as f32;
                    for x in 0..side {
                        let rx = ((x as f64 - centre as f64) * increments[0]).powi(2) as f32;
                        let radius = (f64::from((rx + ry).sqrt()) * 60. * last / 1e9) as f32;
                        let power = at.power(f64::from(radius));
                        plane[y * side + x] =
                            Complex32::new(if weight { power * power } else { power }, 0.);
                    }
                }
                Ok(())
            },
        )
    }

    /// Construct common-beam planes from sampled two-dimensional responses.
    ///
    /// `fill` receives the centred square plane, its side, angular increments
    /// in degrees, frequency in Hz, spectral-window ID, and whether it is
    /// the noise-weight plane.
    /// It writes the power response, or its noise covariance for a weight
    /// plane. Frequency scaling, support and integral normalization follow
    /// the same SimplePBConvFunc path as radial telescope patterns.
    ///
    /// # Errors
    /// Invalid image/window geometry, a sampling failure, or FFT failure.
    pub fn simple_sampled_screens(
        geometry: &GridGeometry,
        polarization: &PolarizationRouting,
        windows: &[MosaicWindow],
        mut fill: impl FnMut(
            &mut [Complex32],
            usize,
            [f64; 2],
            f64,
            u32,
            bool,
        ) -> Result<(), OperatorError>,
    ) -> Result<SimpleMosaicScreens, OperatorError> {
        let layout = Layout::new(geometry, windows)?;
        let mut fft = PlaneFft::<f32>::new([layout.side; 2], false)?;
        let mut plane = vec![Complex32::default(); layout.side * layout.side];
        let mut planes = Vec::with_capacity(layout.windows.len());
        let mut support = Vec::with_capacity(layout.windows.len());
        for window in &layout.windows {
            let last = *window.frequencies_hz.last().expect("a nonempty beam axis");
            let mut transform = |weight: bool| {
                fill(
                    &mut plane,
                    layout.side,
                    layout.increments_deg,
                    last,
                    window.spectral_window,
                    weight,
                )?;
                fft.transform(&mut plane, false)?;
                Ok::<_, OperatorError>(crop(&plane, layout.side, layout.crop))
            };
            let imaging = transform(false)?;
            let weight = transform(true)?;
            let lattices: Vec<_> = window
                .frequencies_hz
                .iter()
                .map(|frequency| PairLattices {
                    imaging: scale_frequency(&imaging, layout.crop, frequency / last),
                    weight: scale_frequency(&weight, layout.crop, frequency / last),
                })
                .collect();
            support.push(find_support(
                &lattices.last().expect("a beam cell").weight,
                layout.crop,
                usize::from(layout.sampling),
            )?);
            planes.push(lattices);
        }
        Ok(SimpleMosaicScreens {
            layout,
            planes,
            support,
            grid: geometry.grid_shape(),
            mueller: MuellerRouting::scalar(polarization.pol_map(), polarization.grid_pols()),
        })
    }
}

impl SimpleMosaicScreens {
    /// Bytes allocated beside these planes while producing the retained cells.
    #[must_use]
    pub fn kernel_bytes(&self) -> u64 {
        let sampling = self.layout.sampling;
        let half = *self.support.iter().max().expect("a window") as u16;
        let cells = 2 * self.layout.cells as u64 * dense_cell_bytes(1, sampling, [half; 2]);
        let crop = self
            .layout
            .crop
            .min(2 * (usize::from(half) + 2) * usize::from(sampling));
        cells
            + (2 * self.layout.cells * size_of::<DenseCell>()
                + 2 * crop * crop * size_of::<Complex32>()) as u64
            + ImageCorrection::bytes(self.grid)
    }

    /// Crop, normalize by the full fine-plane integral and pack dense cells.
    pub fn finish(self) -> Result<MosaicPb, OperatorError> {
        let sampling = self.layout.sampling;
        let half = u16::try_from(*self.support.iter().max().expect("a window"))
            .map_err(|_| invalid("simple mosaic support exceeds u16"))?;
        let side = self
            .layout
            .crop
            .min(2 * (usize::from(half) + 2) * usize::from(sampling));
        let mut imaging = Vec::with_capacity(self.layout.cells);
        let mut weight = Vec::with_capacity(self.layout.cells);
        for window in self.planes {
            for pair in window {
                let mut first = crop(&pair.imaging, self.layout.crop, side);
                let mut second = crop(&pair.weight, self.layout.crop, side);
                let integral = first.iter().map(|v| f64::from(v.re)).sum::<f64>()
                    / f64::from(sampling).powi(2);
                if !integral.is_finite() || integral <= 0. {
                    return Err(invalid(
                        "simple mosaic convolution integral is not positive",
                    ));
                }
                let factor = (1. / integral) as f32;
                first
                    .iter_mut()
                    .chain(&mut second)
                    .for_each(|v| *v *= factor);
                let cell = |values: &[Complex32]| {
                    dense_cell(
                        &[OversampledPlane {
                            values,
                            side,
                            centre: [side / 2; 2],
                            sampling,
                        }],
                        [half; 2],
                    )
                };
                imaging.push(cell(&first));
                weight.push(cell(&second));
            }
        }
        Ok(MosaicPb {
            bounded_pointings: false,
            dishes: 1,
            windows: self.layout.windows,
            cells_per_plane: self.layout.cells,
            imaging,
            weight,
            conjugate: Vec::new(),
            max_half_support: half,
            conv_size: self.layout.side,
            mueller: self.mueller,
            correction: ImageCorrection::mosaic_sinc(self.grid, sampling),
        })
    }
}

fn crop(values: &[Complex32], old: usize, new: usize) -> Vec<Complex32> {
    let start = (old - new) / 2;
    (start..start + new)
        .flat_map(|y| {
            values[y * old + start..y * old + start + new]
                .iter()
                .copied()
        })
        .collect()
}

fn scale_frequency(values: &[Complex32], side: usize, ratio: f64) -> Vec<Complex32> {
    let centre = side / 2;
    let mut result = vec![Complex32::default(); values.len()];
    for y in 0..side {
        let sy = (y as f64 - centre as f64) * 2. / ratio + centre as f64;
        let low = sy.floor();
        if low < 0. || sy.ceil() >= side as f64 {
            continue;
        }
        let iy = if sy > low + 0.5 { sy.ceil() } else { low } as usize;
        for x in 0..side {
            let sx = ((x as f64 - centre as f64) * 2. / ratio).trunc() + centre as f64;
            if sx >= 0. && sx < side as f64 {
                result[y * side + x] = values[iy * side + sx as usize];
            }
        }
    }
    result
}

fn find_support(
    values: &[Complex32],
    side: usize,
    sampling: usize,
) -> Result<usize, OperatorError> {
    let maximum = values.iter().map(|v| v.norm()).fold(0_f32, f32::max);
    let minimum = values
        .iter()
        .map(|v| v.norm())
        .fold(f32::INFINITY, f32::min);
    let centre = side / 2;
    let mut trial = (1..centre.saturating_sub(1))
        .rev()
        .find(|t| values[(centre - t) * side + centre - t].norm() > 1e-3 * maximum)
        .map(|t| (2. * f64::from((t * t) as f32)).sqrt() as usize);
    if trial.is_none() && maximum - minimum <= 1e-3 * maximum {
        return Err(invalid("simple mosaic convolution has zero support"));
    }
    if trial.is_none_or(|value| value < 5 * sampling) {
        trial = Some(if side > 10 * sampling {
            5 * sampling
        } else {
            centre
                .checked_sub(4 * sampling)
                .ok_or(invalid("simple mosaic screen is too small"))?
        });
    }
    Ok((0.5 + trial.expect("support found") as f64 / sampling as f64) as usize + 1)
}

fn invalid(reason: &'static str) -> OperatorError {
    OperatorError::ConvolutionFunction { reason }
}

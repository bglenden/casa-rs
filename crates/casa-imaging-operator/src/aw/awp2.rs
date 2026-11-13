// SPDX-License-Identifier: LGPL-3.0-or-later

//! The unsquinted VLA and EVLA L-band branch of CASA AWPLPG.
//! It uses the existing BeamCalc aperture and mosaic gridding rules, with
//! the A/AW support and sampling of AWConvFunc and AWConvFuncHolder.

mod w_term;

use casa_imaging_model::EvlaDishSurface;
use num_complex::Complex32;

use super::{EvlaApertureGrid, EvlaApertureModel, native::sky_jones_normalization};
use crate::convolution::{
    CellHold, ConvolutionFunctionSet, DenseCell, GridEdge, ImageCorrection, KernelNormalisation,
    MuellerRouting, RowContext, TapFootprint, TapLayout, mosaic_pointing_on_image,
};
use crate::dense::{OversampledPlane, dense_cell, dense_cell_bytes};
use crate::error::OperatorError;
use crate::fft::PlaneFft;
use crate::geometry::GridGeometry;
use crate::polarization::PolarizationRouting;
use crate::sample::CfKey;
use crate::weighting::SPEED_OF_LIGHT_M_PER_S;

use w_term::{AwSupport, WTerm, insert_centre};

const L_BAND_BEAM_SIDE: usize = 512;
const L_BAND_BEAM_CELL_RAD: f64 = 0.024 / 256.0;

/// The aperture grid and the cropped sky grid used by
/// `AWConvFunc::makeAConvFunc` for an AWPLPG image.
pub struct Awp2Sampling {
    image_side: usize,
    cell_rad: f64,
    beam_cell_rad: f64,
    aperture_side: usize,
    sky_side: usize,
    oversampling: u16,
}

impl Awp2Sampling {
    /// Derive AWPLPG sampling from a square, unpadded image with square
    /// cells. `single_field` selects the aperture extent and cropping in
    /// `AWConvFunc::makeAConvFunc`. This path implements Lanczos resampling.
    pub fn new(geometry: &GridGeometry, single_field: bool) -> Result<Self, OperatorError> {
        let extent = geometry.image();
        let [side, ny] = extent.shape;
        let [dx, dy] = extent.increment_rad.map(f64::abs);
        let field_of_view = side as f64 * dx;
        let beam_field_of_view = L_BAND_BEAM_SIDE as f64 * L_BAND_BEAM_CELL_RAD;
        let oversampling = if side < 100 {
            100
        } else {
            10000_usize.div_ceil(side).max(4)
        };
        if side != ny || (dx - dy).abs() > dx * f64::EPSILON {
            return Err(invalid(
                "AWP2 requires a square image with square cells (#1071)",
            ));
        }
        let (aperture_side, sky_side, beam_cell_rad) = if field_of_view > 0.125 * beam_field_of_view
        {
            let side = if single_field || field_of_view < beam_field_of_view {
                2 * L_BAND_BEAM_SIDE
            } else {
                L_BAND_BEAM_SIDE
            };
            (side, side, L_BAND_BEAM_CELL_RAD)
        } else {
            let aperture_side =
                ((L_BAND_BEAM_SIDE as f64 / dx * L_BAND_BEAM_CELL_RAD / 16.0).ceil() as usize)
                    .checked_mul(8)
                    .filter(|side| {
                        side.checked_mul(*side)
                            .is_some_and(|n| n <= isize::MAX as usize / 64)
                    })
                    .ok_or_else(|| invalid("AWP2 aperture grid is too large"))?;
            let sky_side = ((field_of_view / beam_field_of_view * aperture_side as f64 / 2.0).ceil()
                as usize)
                * 2;
            (aperture_side, sky_side, dx)
        };
        if sky_side < 12 || sky_side > aperture_side {
            return Err(invalid("AWP2 sky crop does not fit its aperture grid"));
        }
        let sampling = Self {
            image_side: side,
            cell_rad: dx,
            beam_cell_rad,
            aperture_side,
            sky_side,
            oversampling: oversampling as u16,
        };
        if side >= 200 && sampling.factor() / oversampling as f32 >= 1.0 {
            return Err(invalid("AWP2 FFT resampling remains unsupported (#1071)"));
        }
        // The wider-than-beam A term uses a different support cutoff,
        // including on small images that still select Lanczos.
        if field_of_view > aperture_side as f64 * beam_cell_rad {
            return Err(invalid(
                "AWP2 wider-than-beam support remains unsupported (#1071)",
            ));
        }
        Ok(sampling)
    }

    fn factor(&self) -> f32 {
        self.image_side as f32 * f32::from(self.oversampling)
            / self.sky_side as f32
            / (self.beam_cell_rad / self.cell_rad) as f32
    }

    /// Temporary Jones and sky FFT planes and their planning workspaces.
    #[must_use]
    pub fn screens_bytes(&self, count: usize, w_count: usize) -> u64 {
        let aperture_cells = self.aperture_side * self.aperture_side;
        let sky_cells = self.sky_side * self.sky_side;
        let beam = ((4 * aperture_cells + 2 * sky_cells) * size_of::<Complex32>()) as u64
            + casa_fft::planning_bytes::<f32>(aperture_cells)
            + casa_fft::planning_bytes::<f32>(sky_cells)
            + (count * (size_of::<ApertureCell>() + 2 * sky_cells * size_of::<Complex32>())) as u64;
        if w_count == 1 {
            return beam + (count * size_of::<AwPlane>()) as u64;
        }
        // W and convolution planes, the second FFT plan, and the cropped
        // A/W cells. Until support is measured, a crop may fill the sky grid.
        beam + (4 * sky_cells * size_of::<Complex32>()) as u64
            + casa_fft::planning_bytes::<f32>(sky_cells)
            + (w_count * count * (size_of::<AwPlane>() + sky_cells * size_of::<Complex32>())) as u64
            + (count * size_of::<AwSupport>()) as u64
    }

    /// Calculate the A and A/W terms with the
    /// supplied dish surface. `legacy_vla` selects the original VLA feed.
    /// Scratch is charged before calling this method.
    pub fn screens(
        self,
        frequencies_hz: &[f64],
        surface: EvlaDishSurface,
        legacy_vla: bool,
        w_planes: &[f64],
    ) -> Result<Awp2Screens, OperatorError> {
        if w_planes.first() != Some(&0.0)
            || w_planes.len() > usize::from(u16::MAX)
            || w_planes.windows(2).any(|pair| pair[0] > pair[1])
            || w_planes.iter().any(|w| !w.is_finite())
        {
            return Err(invalid("AWP2 requires ordered W planes starting at zero"));
        }
        if frequencies_hz.is_empty()
            || frequencies_hz.len() > usize::from(u16::MAX)
            || frequencies_hz.windows(2).any(|f| f[0] >= f[1])
            || frequencies_hz.iter().any(|f| !(0.9e9..=2e9).contains(f))
        {
            return Err(invalid(
                "AWP2 requires ordered VLA L-band beam frequencies (#1071)",
            ));
        }
        let model = EvlaApertureModel::new(surface);
        let mut jones = vec![Complex32::default(); 4 * self.aperture_side * self.aperture_side];
        let mut aperture_fft = PlaneFft::new([self.aperture_side; 2], false)?;
        let mut sky_fft = PlaneFft::new([self.sky_side; 2], false)?;
        let mut cells = Vec::with_capacity(frequencies_hz.len());
        // CASA crops every A plane to the support of the highest frequency.
        let mut a_support = None;
        for &frequency in frequencies_hz.iter().rev() {
            let beam =
                self.sky_beam(&model, frequency, legacy_vla, &mut aperture_fft, &mut jones)?;
            let cell = ApertureCell::new(beam, self.sky_side, &mut sky_fft, a_support)?;
            a_support.get_or_insert(cell.a_side / 2);
            cells.push(cell);
        }
        cells.reverse();
        drop(jones);
        drop(aperture_fft);
        drop(sky_fft);
        let (aw_planes, plane_side) = self.combine_w(&mut cells, w_planes)?;
        Ok(Awp2Screens {
            sampling: self,
            cells,
            aw_planes,
            plane_side,
        })
    }

    fn combine_w(
        &self,
        cells: &mut [ApertureCell],
        w_planes: &[f64],
    ) -> Result<(Vec<AwPlane>, usize), OperatorError> {
        let a_side = cells[0].a_side;
        if w_planes.len() == 1 {
            let planes = cells
                .iter_mut()
                .map(|cell| {
                    let support = AwSupport::new(&cell.imaging, a_side, cell.support);
                    let half = support.half_support(&cell.imaging, a_side, cell.support);
                    let mut values = std::mem::take(&mut cell.imaging);
                    support.normalize(&mut values);
                    AwPlane { values, half }
                })
                .collect();
            return Ok((planes, a_side));
        }
        let mut w = WTerm::new(self.sky_side, self.beam_cell_rad)?;
        let w_side = w.convolution_side(*w_planes.last().unwrap())?;
        let conv_side = w_side.max(a_side);
        let mut fft = PlaneFft::new([conv_side; 2], false)?;
        let mut normalization = Vec::with_capacity(cells.len());
        let mut planes = Vec::with_capacity(w_planes.len() * cells.len());
        let mut largest = 0;
        for (index, &w_lambda) in w_planes.iter().enumerate() {
            let screen = w.prepare(w_lambda, w_side, conv_side, &mut fft)?;
            for (frequency, cell) in cells.iter().enumerate() {
                let plane = w.convolve(&cell.imaging, a_side, &screen, conv_side, &mut fft)?;
                if index == 0 {
                    normalization.push(AwSupport::new(plane, conv_side, cell.support));
                }
                let norm = &normalization[frequency];
                let half = norm.half_support(plane, conv_side, cell.support);
                largest = largest.max(half);
                planes.push(AwPlane {
                    values: plane.to_vec(),
                    half,
                });
            }
        }
        let side = (2 * largest).min(conv_side);
        for (index, plane) in planes.iter_mut().enumerate() {
            let norm = &normalization[index % cells.len()];
            if side < conv_side {
                plane.values = crop(&plane.values, conv_side, side / 2, norm.inverse_sum());
            } else {
                norm.normalize(&mut plane.values);
            }
        }
        Ok((planes, side))
    }

    fn sky_beam(
        &self,
        model: &EvlaApertureModel,
        frequency: f64,
        legacy_vla: bool,
        aperture_fft: &mut PlaneFft<f32>,
        jones: &mut [Complex32],
    ) -> Result<Vec<Complex32>, OperatorError> {
        let wavelength = SPEED_OF_LIGHT_M_PER_S / f64::from(frequency as f32);
        let cell_m = wavelength / (self.aperture_side as f64 * self.beam_cell_rad);
        let grid = EvlaApertureGrid::new(self.aperture_side, cell_m, 3, frequency, 0.0)
            .map_err(|_| invalid("invalid AWP2 aperture sampling"))?;
        if legacy_vla {
            model.fill_vla_l_aperture(grid, jones)
        } else {
            model.fill_aperture(grid, jones)
        }
        .map_err(|_| invalid("AWP2 could not evaluate its aperture"))?;
        let norm = sky_jones_normalization(self.aperture_side, aperture_fft, jones)
            .map_err(|_| invalid("AWP2 could not normalize its aperture"))?;
        let plane = self.aperture_side * self.aperture_side;
        let mut beam = Vec::with_capacity(self.sky_side * self.sky_side);
        let first = (self.aperture_side - self.sky_side) / 2;
        for y in first..first + self.sky_side {
            for x in first..first + self.sky_side {
                let index = y * self.aperture_side + x;
                // applyAvgSkyJones uses real(RL) for all four correlations.
                let rr = jones[index] / norm;
                let ll = jones[3 * plane + index] / norm;
                beam.push(Complex32::new((ll * rr.conj()).re, 0.0));
            }
        }
        Ok(beam)
    }
}

struct ApertureCell {
    imaging: Vec<Complex32>,
    weight: Vec<Complex32>,
    a_side: usize,
    support: usize,
}

impl ApertureCell {
    fn new(
        mut imaging: Vec<Complex32>,
        side: usize,
        fft: &mut PlaneFft<f32>,
        crop_support: Option<usize>,
    ) -> Result<Self, OperatorError> {
        let mut weight: Vec<_> = imaging.iter().map(|value| value * value.conj()).collect();
        fft.transform(&mut imaging, false)?;
        fft.transform(&mut weight, false)?;
        let support = a_support(&weight, side)?;
        for plane in [&mut imaging, &mut weight] {
            let sum = central_sum(plane, side, support, true);
            if !sum.is_finite() || sum <= 0.0 {
                return Err(invalid("AWP2 aperture normalization is not positive"));
            }
            *plane = crop(plane, side, crop_support.unwrap_or(support), sum.recip());
        }
        let a_side = 2 * crop_support.unwrap_or(support);
        Ok(Self {
            imaging,
            weight,
            a_side,
            support,
        })
    }
}

fn a_support(weight: &[Complex32], side: usize) -> Result<usize, OperatorError> {
    let (minimum, maximum) = weight
        .iter()
        .map(|v| v.norm())
        .fold((f32::INFINITY, 0.0_f32), |(minimum, maximum), value| {
            (minimum.min(value), maximum.max(value))
        });
    if maximum - minimum <= 0.005 * maximum {
        return Err(invalid("AWP2 found no aperture support"));
    }
    let fallback = if side > 10 { 5 } else { side / 2 - 4 };
    Ok((diagonal_support(weight, side, 0.005)
        .filter(|radius| *radius >= 5)
        .unwrap_or(fallback)
        + 1)
    .min(side / 2 - 1))
}

fn diagonal_support(plane: &[Complex32], side: usize, cutoff: f32) -> Option<usize> {
    let maximum = plane.iter().map(|v| v.norm()).fold(0.0, f32::max);
    diagonal_support_relative(plane, side, cutoff * maximum)
}

fn diagonal_support_relative(plane: &[Complex32], side: usize, threshold: f32) -> Option<usize> {
    let centre = side / 2;
    (1..=centre - 2).rev().find_map(|offset| {
        let index = (centre - offset) * (side + 1);
        (plane[index].norm() > threshold)
            .then_some((std::f64::consts::SQRT_2 * offset as f64) as usize)
    })
}

fn central_sum(plane: &[Complex32], side: usize, radius: usize, inclusive: bool) -> f32 {
    let centre = side / 2;
    let first = centre - radius;
    let end = centre + radius + usize::from(inclusive);
    (first..end)
        .flat_map(|y| &plane[y * side + first..y * side + end])
        .map(|v| v.re)
        .sum()
}

fn crop(plane: &[Complex32], side: usize, radius: usize, factor: f32) -> Vec<Complex32> {
    let first = side / 2 - radius;
    let end = first + 2 * radius;
    (first..end)
        .flat_map(|y| &plane[y * side + first..y * side + end])
        .map(|value| *value * factor)
        .collect()
}

struct AwPlane {
    values: Vec<Complex32>,
    half: usize,
}

/// A/W convolution planes before the oversampled resident cells are
/// allocated. The cells use one Mueller plane and separate sensitivity taps.
pub struct Awp2Screens {
    sampling: Awp2Sampling,
    cells: Vec<ApertureCell>,
    aw_planes: Vec<AwPlane>,
    plane_side: usize,
}

impl Awp2Screens {
    fn half_support(&self) -> u16 {
        // AWConvFuncHolder uses the highest-frequency support for each W.
        self.aw_planes
            .chunks_exact(self.cells.len())
            .map(|planes| planes.last().unwrap().half)
            .max()
            .expect("nonempty A/W planes") as u16
    }

    fn fine_side(&self) -> usize {
        2 * (usize::from(self.half_support()) + 1) * usize::from(self.sampling.oversampling)
    }

    /// Upper bound including the resident cells and one oversampled plane
    /// and interpolation workspace used while constructing them.
    #[must_use]
    pub fn kernel_bytes(&self, geometry: &GridGeometry) -> u64 {
        let fine_side = self.fine_side();
        let count = self.cells.len();
        let aw_count = self.aw_planes.len();
        let w_count = aw_count / count;
        ((count + w_count) * size_of::<f64>() + (aw_count + count) * size_of::<DenseCell>()) as u64
            + (aw_count + count) as u64
                * dense_cell_bytes(1, self.sampling.oversampling, [self.half_support(); 2])
            + (fine_side * fine_side * size_of::<Complex32>()
                + self.plane_side * self.plane_side * size_of::<Complex32>()
                + fine_side * size_of::<Option<LanczosTaps>>()
                + 8000 * size_of::<f32>()) as u64
            + ImageCorrection::bytes(geometry.grid_shape())
            + MuellerRouting::MAXIMUM_BYTES
    }

    /// Resample each A/W and sensitivity plane with AWConvFuncHolder's
    /// Lanczos rule and pack them in the shared gridding layout.
    pub fn finish(
        self,
        geometry: &GridGeometry,
        polarization: &PolarizationRouting,
        frequencies_hz: Box<[f64]>,
        w_planes: Box<[f64]>,
        row_frequency_hz: f64,
    ) -> Awp2 {
        assert_eq!(self.cells.len(), frequencies_hz.len());
        assert_eq!(self.aw_planes.len(), self.cells.len() * w_planes.len());
        let half_support = self.half_support();
        let fine_side = self.fine_side();
        let oversampling = self.sampling.oversampling;
        let factor = self.sampling.factor();
        let mut plane = vec![Complex32::default(); fine_side * fine_side];
        let sinc_table: Vec<_> = (-4000..4000)
            .map(|i| sinc((i as f64 / 1000.0) as f32))
            .collect();
        let a_side = self.plane_side;
        let resampled_side = resampled_side(a_side, factor);
        let axis = lanczos_axis(a_side, resampled_side, fine_side, &sinc_table);
        let scale = (a_side * a_side * usize::from(oversampling).pow(2)) as f32
            / (resampled_side * resampled_side) as f32;
        let mut pack = |source: &[Complex32]| {
            resample(source, a_side, &axis, scale, &mut plane);
            dense_cell(
                &[OversampledPlane {
                    values: &plane,
                    side: fine_side,
                    centre: [fine_side / 2; 2],
                    sampling: oversampling,
                }],
                [half_support; 2],
            )
        };
        let imaging = self
            .aw_planes
            .iter()
            .map(|cell| pack(&cell.values))
            .collect();
        let mut padded = vec![Complex32::default(); a_side * a_side];
        let weight = self
            .cells
            .iter()
            .map(|cell| {
                padded.fill(Complex32::default());
                insert_centre(&mut padded, a_side, &cell.weight, cell.a_side, cell.a_side);
                pack(&padded)
            })
            .collect();
        Awp2 {
            frequencies_hz,
            w_planes,
            row_frequency_hz,
            imaging,
            weight,
            half_support,
            mueller: MuellerRouting::scalar(polarization.pol_map(), polarization.grid_pols()),
            correction: ImageCorrection::mosaic_sinc(geometry.grid_shape(), oversampling),
        }
    }
}

// MathUtils::resample receives the holder's Float factor as a Double.
fn resampled_side(input_side: usize, factor: f32) -> usize {
    (input_side as f64 * f64::from(factor) / 8.0).ceil() as usize * 8
}

fn sinc(value: f32) -> f32 {
    if value == 0.0 {
        1.0
    } else {
        let x = std::f64::consts::PI * f64::from(value);
        (x.sin() / x) as f32
    }
}

struct LanczosTaps {
    first: usize,
    weights: [[f32; 2]; 6],
}

fn lanczos_axis(
    side: usize,
    resampled: usize,
    fine: usize,
    table: &[f32],
) -> Vec<Option<LanczosTaps>> {
    let sinc_lookup = |value: f64| table[(value as f32 * 1000.0 + 4000.0) as usize];
    (0..fine)
        .map(|i| {
            let i = i as isize + (resampled as isize - fine as isize) / 2;
            let position = i as f64 / resampled as f64 * side as f64;
            let pixel = position.floor() as isize;
            if pixel < 3 || pixel >= side as isize - 3 {
                return None;
            }
            let first = pixel as usize - 2;
            let weights = std::array::from_fn(|tap| {
                let distance = position - (first + tap) as f64;
                [sinc_lookup(distance), sinc_lookup(distance / 3.0)]
            });
            Some(LanczosTaps { first, weights })
        })
        .collect()
}

fn resample(
    source: &[Complex32],
    side: usize,
    axis: &[Option<LanczosTaps>],
    scale: f32,
    output: &mut [Complex32],
) {
    for (row, y) in output.chunks_exact_mut(axis.len()).zip(axis) {
        for (value, x) in row.iter_mut().zip(axis) {
            *value = match (x, y) {
                (Some(x), Some(y)) => interpolate(source, side, x, y) * scale,
                _ => Complex32::default(),
            };
        }
    }
}

fn interpolate(source: &[Complex32], side: usize, x: &LanczosTaps, y: &LanczosTaps) -> Complex32 {
    let mut sum = Complex32::default();
    for (i, [wx, lx]) in x.weights.into_iter().enumerate() {
        for (j, [wy, ly]) in y.weights.into_iter().enumerate() {
            let value = source[(y.first + j) * side + x.first + i];
            let weight = f64::from(wx) * f64::from(lx) * f64::from(wy) * f64::from(ly);
            sum.re += (f64::from(value.re) * weight) as f32;
            sum.im += (f64::from(value.im) * weight) as f32;
        }
    }
    sum
}

/// AWPLPG's resident cells for unsquinted beams.
pub struct Awp2 {
    frequencies_hz: Box<[f64]>,
    w_planes: Box<[f64]>,
    row_frequency_hz: f64,
    imaging: Box<[DenseCell]>,
    weight: Box<[DenseCell]>,
    half_support: u16,
    mueller: MuellerRouting,
    correction: ImageCorrection,
}

impl Awp2 {
    /// Bytes retained after the screen and interpolation workspaces are freed.
    #[must_use]
    pub fn resident_bytes(&self) -> u64 {
        (std::mem::size_of_val(self.frequencies_hz.as_ref())
            + std::mem::size_of_val(self.w_planes.as_ref())
            + std::mem::size_of_val(self.imaging.as_ref())
            + std::mem::size_of_val(self.weight.as_ref())
            + self
                .imaging
                .iter()
                .chain(&self.weight)
                .map(DenseCell::bytes)
                .sum::<usize>()) as u64
            + self.mueller.bytes()
            + self.correction.resident_bytes()
    }
}

impl ConvolutionFunctionSet for Awp2 {
    fn key(&self, _row: &RowContext, freq_hz: f64, w_lambda: f64) -> CfKey {
        // AWConvFuncHolder compares the gridding frequency directly, using
        // the lower frequency when distances tie.
        let (cube, _) = super::nearest(&self.frequencies_hz, freq_hz);
        // The holder selects the W plane at the visibility buffer's
        // mean native frequency, independently of the channel's A term.
        let w = (w_lambda * self.row_frequency_hz / freq_hz).abs();
        let (group, _) = super::nearest(&self.w_planes, w);
        CfKey {
            group: group as u16,
            cube: cube as u16,
        }
    }

    fn taps<'s>(&'s self, key: CfKey, _hold: &'s mut CellHold) -> TapLayout<'s> {
        self.imaging[usize::from(key.group) * self.frequencies_hz.len() + usize::from(key.cube)]
            .layout()
    }

    fn max_half_support(&self) -> [u16; 2] {
        [self.half_support; 2]
    }

    fn weight_taps<'s>(&'s self, key: CfKey, _hold: &'s mut CellHold) -> Option<TapLayout<'s>> {
        Some(self.weight[usize::from(key.cube)].layout())
    }

    fn psf_taps<'s>(&'s self, key: CfKey, hold: &'s mut CellHold) -> TapLayout<'s> {
        self.taps(CfKey { group: 0, ..key }, hold)
    }

    fn mueller(&self) -> &MuellerRouting {
        &self.mueller
    }

    fn image_correction(&self) -> &ImageCorrection {
        &self.correction
    }

    fn normalisation(&self) -> KernelNormalisation {
        KernelNormalisation::UnitSum
    }

    fn pointing_ramp(&self) -> bool {
        true
    }

    fn admits_pointing(&self, geometry: &GridGeometry, offset_rad: [f64; 2]) -> bool {
        mosaic_pointing_on_image(geometry, offset_rad)
    }

    fn grid_edge(&self) -> GridEdge {
        GridEdge::Clipped
    }

    fn corrects_weight_image(&self) -> bool {
        false
    }

    fn tap_footprint(&self) -> TapFootprint {
        // The device tables use the placement key for each kind.
        TapFootprint::of(
            self.imaging.iter().map(DenseCell::layout).chain(
                self.weight
                    .iter()
                    .cycle()
                    .take(self.imaging.len())
                    .map(DenseCell::layout),
            ),
        )
    }
}

fn invalid(reason: &'static str) -> OperatorError {
    OperatorError::ConvolutionFunction { reason }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn awp2_small_field_uses_the_casa_aperture_and_sampling() {
        let cell_rad = 8.0_f64.to_radians() / 3600.0;
        let geometry = GridGeometry::new(
            crate::ImageExtent {
                shape: [100; 2],
                increment_rad: [-cell_rad, cell_rad],
                reference_pixel: [50; 2],
            },
            crate::GridPadding::None,
        )
        .unwrap();
        let sampling = Awp2Sampling::new(&geometry, true).unwrap();
        // Captured from BeamCalc::calculateAperture and makeAConvFunc.
        assert_eq!(
            (
                sampling.aperture_side,
                sampling.sky_side,
                sampling.oversampling
            ),
            (624, 52, 100)
        );
    }

    #[test]
    fn awp2_two_field_sampling_matches_the_captured_convolution_grid() {
        let cell_rad = 10.0_f64.to_radians() / 3600.0;
        let geometry = GridGeometry::new(
            crate::ImageExtent {
                shape: [512; 2],
                increment_rad: [-cell_rad, cell_rad],
                reference_pixel: [256; 2],
            },
            crate::GridPadding::None,
        )
        .unwrap();
        let sampling = Awp2Sampling::new(&geometry, false).unwrap();
        // refim_mawproject.ms, SPW 1: makeAConvFunc keeps the full aperture.
        assert_eq!((sampling.aperture_side, sampling.sky_side), (1024, 1024));
        assert_eq!(sampling.oversampling, 20);
        assert_eq!(resampled_side(38, sampling.factor()), 200);

        // A wider two-field image keeps the original 512-pixel beam grid.
        // The holder would use FFT resampling there, which is still guarded.
        let wide_cell = 2.0 * cell_rad;
        let wide = GridGeometry::new(
            crate::ImageExtent {
                increment_rad: [-wide_cell, wide_cell],
                ..geometry.image()
            },
            crate::GridPadding::None,
        )
        .unwrap();
        assert!(matches!(
            Awp2Sampling::new(&wide, false),
            Err(OperatorError::ConvolutionFunction {
                reason: "AWP2 FFT resampling remains unsupported (#1071)",
            })
        ));
    }

    #[test]
    fn awp2_refuses_wider_than_beam_support_even_with_lanczos() {
        let cell_rad = 120.0_f64.to_radians() / 3600.0;
        let geometry = GridGeometry::new(
            crate::ImageExtent {
                shape: [100; 2],
                increment_rad: [-cell_rad, cell_rad],
                reference_pixel: [50; 2],
            },
            crate::GridPadding::None,
        )
        .unwrap();
        assert!(matches!(
            Awp2Sampling::new(&geometry, false),
            Err(OperatorError::ConvolutionFunction {
                reason: "AWP2 wider-than-beam support remains unsupported (#1071)",
            })
        ));
    }

    #[test]
    fn awp2_rounds_the_resample_size_after_promoting_its_factor() {
        // A 56-square image at 8 arcsec has a 30-square cropped sky and
        // 100x oversampling. The Float factor rounds above 5600 / 30.
        assert_eq!(resampled_side(12, 5600.0_f32 / 30.0), 2248);
    }

    #[test]
    fn awp2_interpolation_uses_the_casa_float_sinc_table() {
        let table: Vec<_> = (-4000..4000)
            .map(|i| sinc((i as f64 / 1000.0) as f32))
            .collect();
        // A single nonzero input also isolates the sinc-table multiplication
        // from roundoff in a general convolution.
        let mut input = [Complex32::default(); 12 * 12];
        input[6 * 12 + 6] = Complex32::new(0.9771961, 0.0);
        let taps = lanczos_axis(12, 2312, 1400, &table);
        let value = interpolate(
            &input,
            12,
            taps[757].as_ref().unwrap(),
            taps[679].as_ref().unwrap(),
        );
        // sinc(0.295) sinc(0.098) sinc(-0.109) sinc(-0.037),
        // evaluated at the four entries selected by MathUtils::sinc.
        let expected = 0.9771961_f64
            * [0.295_f64, 0.098, -0.109, -0.037]
                .into_iter()
                .map(|v| {
                    let a = std::f64::consts::PI * v;
                    a.sin() / a
                })
                .product::<f64>();
        assert!(
            (f64::from(value.re) - expected).abs() < 2.0e-7,
            "{value} vs {expected}"
        );
        assert_eq!(value.im, 0.0);
    }
}

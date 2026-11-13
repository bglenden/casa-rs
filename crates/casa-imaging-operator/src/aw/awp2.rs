// SPDX-License-Identifier: LGPL-3.0-or-later

//! The VLA and EVLA L-band branch of CASA AWPLPG.
//! It uses the existing BeamCalc aperture and mosaic gridding rules, with
//! the A/AW support and sampling of AWConvFunc and AWConvFuncHolder.

mod w_term;

use casa_imaging_model::EvlaDishSurface;
use casa_types::Correlation;
use num_complex::Complex32;

use super::{EvlaApertureGrid, EvlaApertureModel, native::sky_jones_normalization};
use crate::accumulator::Mode;
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
        Self::sampling(geometry, single_field, false)
    }

    /// The compact squinted aperture used with multiple W planes.
    pub fn squinted(geometry: &GridGeometry) -> Result<Self, OperatorError> {
        Self::sampling(geometry, false, true)
    }

    fn sampling(
        geometry: &GridGeometry,
        single_field: bool,
        squint: bool,
    ) -> Result<Self, OperatorError> {
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
        let (aperture_side, sky_side, beam_cell_rad) = if squint {
            (
                L_BAND_BEAM_SIDE,
                L_BAND_BEAM_SIDE,
                L_BAND_BEAM_CELL_RAD / 2.0,
            )
        } else if field_of_view > 0.125 * beam_field_of_view {
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

    /// Bound the temporary FFTs and all retained aperture and A/W screens.
    #[must_use]
    pub fn screens_bytes(&self, count: usize, w_count: usize, pa_count: usize) -> u64 {
        let aperture_cells = self.aperture_side * self.aperture_side;
        let sky_cells = self.sky_side * self.sky_side;
        let muellers = if pa_count == 0 { 1 } else { 2 };
        let groups = pa_count.max(1);
        let beam = ((4 * aperture_cells + 2 * sky_cells) * size_of::<Complex32>()) as u64
            + casa_fft::planning_bytes::<f32>(aperture_cells)
            + casa_fft::planning_bytes::<f32>(sky_cells);
        // Until support is measured, a crop may fill the sky grid. Count
        // every retained PA group and a temporary crop of one plane.
        let group = size_of::<ApertureGroup>()
            + count
                * muellers
                * (size_of::<ApertureCell>() + 2 * sky_cells * size_of::<Complex32>())
            + w_count
                * count
                * (size_of::<AwPlane>() + muellers * sky_cells * size_of::<Complex32>());
        if w_count == 1 {
            return beam + (group * groups) as u64;
        }
        beam + (group * groups) as u64
            + (4 * sky_cells * size_of::<Complex32>()) as u64
            + casa_fft::planning_bytes::<f32>(sky_cells)
            + (count * size_of::<AwSupport>()) as u64
    }

    /// Calculate the A and A/W terms with the supplied dish surface.
    /// `legacy_vla` selects the original VLA feed. An empty list of PA
    /// centres selects CASA's polarization-averaged beam. Charge the
    /// bound from [`Self::screens_bytes`] before calling this method.
    pub fn screens(
        self,
        frequencies_hz: &[f64],
        surface: EvlaDishSurface,
        legacy_vla: bool,
        w_planes: &[f64],
        pa_centres: &[f64],
    ) -> Result<Awp2Screens, OperatorError> {
        let limit = usize::from(u16::MAX) / pa_centres.len().max(1);
        if w_planes.first() != Some(&0.0)
            || w_planes.len() > limit
            || w_planes.windows(2).any(|pair| pair[0] > pair[1])
            || w_planes.iter().any(|w| !w.is_finite())
        {
            return Err(invalid("AWP2 requires ordered W planes starting at zero"));
        }
        if frequencies_hz.is_empty()
            || frequencies_hz.len() > limit
            || frequencies_hz.windows(2).any(|f| f[0] >= f[1])
            || frequencies_hz.iter().any(|f| !(0.9e9..=2e9).contains(f))
        {
            return Err(invalid(
                "AWP2 requires ordered VLA L-band beam frequencies (#1071)",
            ));
        }
        let model = EvlaApertureModel::new(surface);
        let angles = if pa_centres.is_empty() {
            &[0.0]
        } else {
            pa_centres
        };
        let muellers = if pa_centres.is_empty() { 1 } else { 2 };
        let mut groups = Vec::with_capacity(angles.len());
        for &pa in angles {
            groups.push(self.aperture_group(
                &model,
                frequencies_hz,
                w_planes,
                legacy_vla,
                pa,
                muellers,
            )?);
        }
        Ok(Awp2Screens {
            sampling: self,
            groups,
            muellers,
        })
    }

    fn aperture_group(
        &self,
        model: &EvlaApertureModel,
        frequencies_hz: &[f64],
        w_planes: &[f64],
        legacy_vla: bool,
        pa: f64,
        muellers: usize,
    ) -> Result<ApertureGroup, OperatorError> {
        let mut jones = vec![Complex32::default(); 4 * self.aperture_side * self.aperture_side];
        let mut aperture_fft = PlaneFft::new([self.aperture_side; 2], false)?;
        let mut sky_fft = PlaneFft::new([self.sky_side; 2], false)?;
        let mut cells = Vec::with_capacity(frequencies_hz.len() * muellers);
        // CASA crops every A plane to the support of the highest frequency.
        let mut crop_support = None;
        for &frequency in frequencies_hz.iter().rev() {
            let norm = self.sky_jones(
                model,
                frequency,
                legacy_vla,
                pa,
                &mut aperture_fft,
                &mut jones,
            )?;
            let first = self.sky_beam(&jones, norm, if muellers == 1 { None } else { Some(0) });
            let rr = ApertureCell::new(first, self.sky_side, &mut sky_fft, crop_support, None)?;
            crop_support.get_or_insert(rr.a_side / 2);
            if muellers == 2 {
                // The cutoff and extent are set by RR. Each hand is
                // normalized by the sum under its own aperture plane.
                let ll = self.sky_beam(&jones, norm, Some(3));
                cells.push(ApertureCell::new(
                    ll,
                    self.sky_side,
                    &mut sky_fft,
                    crop_support,
                    Some(rr.support),
                )?);
            }
            cells.push(rr);
        }
        cells.reverse();
        drop(jones);
        drop(aperture_fft);
        drop(sky_fft);
        let (aw_planes, plane_side) = self.combine_w(&mut cells, w_planes, muellers)?;
        Ok(ApertureGroup {
            cells,
            aw_planes,
            plane_side,
        })
    }

    fn combine_w(
        &self,
        cells: &mut [ApertureCell],
        w_planes: &[f64],
        muellers: usize,
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
                    AwPlane {
                        values: [values, Vec::new()],
                        half,
                    }
                })
                .collect();
            return Ok((planes, a_side));
        }
        let mut w = WTerm::new(self.sky_side, self.beam_cell_rad)?;
        let w_side = w.convolution_side(*w_planes.last().unwrap())?;
        let conv_side = w_side.max(a_side);
        let mut fft = PlaneFft::new([conv_side; 2], false)?;
        let frequencies = cells.len() / muellers;
        let mut normalization = Vec::with_capacity(frequencies);
        let mut planes = Vec::with_capacity(w_planes.len() * frequencies);
        let mut largest = 0;
        for (index, &w_lambda) in w_planes.iter().enumerate() {
            let screen = w.prepare(w_lambda, w_side, conv_side, &mut fft)?;
            for (frequency, cells) in cells.chunks_exact(muellers).enumerate() {
                let rr = &cells[0];
                let plane = w.convolve(&rr.imaging, a_side, &screen, conv_side, &mut fft)?;
                if index == 0 {
                    normalization.push(AwSupport::new(plane, conv_side, rr.support));
                }
                let half = normalization[frequency].half_support(plane, conv_side, rr.support);
                largest = largest.max(half);
                let mut values = [plane.to_vec(), Vec::new()];
                if let Some(ll) = cells.get(1) {
                    values[1] = w
                        .convolve(&ll.imaging, a_side, &screen, conv_side, &mut fft)?
                        .to_vec();
                }
                planes.push(AwPlane { values, half });
            }
        }
        let side = (2 * largest).min(conv_side);
        for (index, plane) in planes.iter_mut().enumerate() {
            let norm = &normalization[index % frequencies];
            for values in &mut plane.values[..muellers] {
                if side < conv_side {
                    *values = crop(values, conv_side, side / 2, norm.inverse_sum());
                } else {
                    norm.normalize(values);
                }
            }
        }
        Ok((planes, side))
    }

    fn sky_beam(&self, jones: &[Complex32], norm: f32, hand: Option<usize>) -> Vec<Complex32> {
        let plane = self.aperture_side * self.aperture_side;
        let mut beam = Vec::with_capacity(self.sky_side * self.sky_side);
        let first = (self.aperture_side - self.sky_side) / 2;
        for y in first..first + self.sky_side {
            for x in first..first + self.sky_side {
                let index = y * self.aperture_side + x;
                let value = if let Some(hand) = hand {
                    let jones = jones[hand * plane + index] / norm;
                    jones * jones.conj()
                } else {
                    // applyAvgSkyJones uses real(RL) for all four correlations.
                    let rr = jones[index] / norm;
                    let ll = jones[3 * plane + index] / norm;
                    Complex32::new((ll * rr.conj()).re, 0.0)
                };
                beam.push(value);
            }
        }
        beam
    }

    fn sky_jones(
        &self,
        model: &EvlaApertureModel,
        frequency: f64,
        legacy_vla: bool,
        pa_rad: f64,
        aperture_fft: &mut PlaneFft<f32>,
        jones: &mut [Complex32],
    ) -> Result<f32, OperatorError> {
        let wavelength = SPEED_OF_LIGHT_M_PER_S / f64::from(frequency as f32);
        let cell_m = wavelength / (self.aperture_side as f64 * self.beam_cell_rad);
        let grid = EvlaApertureGrid::new(self.aperture_side, cell_m, 3, frequency, pa_rad)
            .map_err(|_| invalid("invalid AWP2 aperture sampling"))?;
        if legacy_vla {
            model.fill_vla_l_aperture(grid, jones)
        } else {
            model.fill_aperture(grid, jones)
        }
        .map_err(|_| invalid("AWP2 could not evaluate its aperture"))?;
        sky_jones_normalization(self.aperture_side, aperture_fft, jones)
            .map_err(|_| invalid("AWP2 could not normalize its aperture"))
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
        shared_support: Option<usize>,
    ) -> Result<Self, OperatorError> {
        let mut weight: Vec<_> = imaging.iter().map(|value| value * value.conj()).collect();
        fft.transform(&mut imaging, false)?;
        fft.transform(&mut weight, false)?;
        let support = match shared_support {
            Some(support) => support,
            None => a_support(&weight, side)?,
        };
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
    values: [Vec<Complex32>; 2],
    half: usize,
}

struct ApertureGroup {
    cells: Vec<ApertureCell>,
    aw_planes: Vec<AwPlane>,
    plane_side: usize,
}

/// A/W convolution planes before the oversampled resident cells are
/// allocated. Squint uses independent RR and LL taps in each cell.
pub struct Awp2Screens {
    sampling: Awp2Sampling,
    groups: Vec<ApertureGroup>,
    muellers: usize,
}

impl Awp2Screens {
    fn half_support(&self) -> u16 {
        // MosaicFT uses the maximum over every group's highest-frequency
        // support. The holder's crop can be larger than this tap extent.
        self.groups
            .iter()
            .flat_map(|group| {
                group
                    .aw_planes
                    .chunks_exact(group.cells.len() / self.muellers)
            })
            .map(|planes| planes.last().unwrap().half)
            .max()
            .expect("nonempty A/W planes") as u16
    }

    fn fine_side(&self) -> usize {
        2 * (usize::from(self.half_support()) + 1) * usize::from(self.sampling.oversampling)
    }

    /// Upper bound including the resident cells, per-hand oversampled
    /// planes, and the interpolation workspace used while constructing them.
    #[must_use]
    pub fn kernel_bytes(&self, geometry: &GridGeometry) -> u64 {
        let fine_side = self.fine_side();
        let groups = self.groups.len();
        let count = self.groups[0].cells.len() / self.muellers;
        let aw_count = self.groups[0].aw_planes.len();
        let w_count = aw_count / count;
        let raw_side = self
            .groups
            .iter()
            .map(|group| group.plane_side)
            .max()
            .unwrap();
        ((count + w_count + groups) * size_of::<f64>()
            + groups * (aw_count + count) * size_of::<DenseCell>()) as u64
            + (groups * (aw_count + count)) as u64
                * dense_cell_bytes(
                    self.muellers,
                    self.sampling.oversampling,
                    [self.half_support(); 2],
                )
            + ((self.muellers * fine_side * fine_side + raw_side * raw_side)
                * size_of::<Complex32>()
                + self.muellers * size_of::<Vec<Complex32>>()
                + fine_side * size_of::<Option<LanczosTaps>>()
                + 8000 * size_of::<f32>()) as u64
            + ImageCorrection::bytes(geometry.grid_shape())
            + MuellerRouting::MAXIMUM_BYTES
    }

    /// Resample the A/W and sensitivity planes with AWConvFuncHolder's
    /// Lanczos rule and pack them in the shared gridding layout.
    pub fn finish(
        self,
        geometry: &GridGeometry,
        polarization: &PolarizationRouting,
        frequencies_hz: Box<[f64]>,
        w_planes: Box<[f64]>,
        row_frequency_hz: f64,
        (pa_centres, feed_angle_rad): (Box<[f64]>, f32),
    ) -> Awp2 {
        assert_eq!(self.groups.len(), pa_centres.len().max(1));
        let half_support = self.half_support();
        let fine_side = self.fine_side();
        let oversampling = self.sampling.oversampling;
        let factor = self.sampling.factor();
        let mut fine_planes: Vec<_> = (0..self.muellers)
            .map(|_| vec![Complex32::default(); fine_side * fine_side])
            .collect();
        let sinc_table: Vec<_> = (-4000..4000)
            .map(|i| sinc((i as f64 / 1000.0) as f32))
            .collect();
        let mut imaging =
            Vec::with_capacity(self.groups.len() * frequencies_hz.len() * w_planes.len());
        let mut weight = Vec::with_capacity(self.groups.len() * frequencies_hz.len());
        let mut holder_radius = 0;
        for group in self.groups {
            let support = group
                .aw_planes
                .iter()
                .map(|plane| plane.half)
                .max()
                .unwrap();
            holder_radius = holder_radius.max((support + 1) * usize::from(oversampling));
            let a_side = group.plane_side;
            let resampled_side = resampled_side(a_side, factor);
            let mut axis = lanczos_axis(a_side, resampled_side, fine_side, &sinc_table);
            // AWConvFuncHolder centre-crops to the support reached so far
            // before a subsequent PA can grow the holder.
            let first = (fine_side / 2).saturating_sub(holder_radius);
            axis[..first].fill_with(|| None);
            axis[fine_side - first..].fill_with(|| None);
            let scale = (a_side * a_side * usize::from(oversampling).pow(2)) as f32
                / (resampled_side * resampled_side) as f32;
            let mut pack = |load: &mut dyn FnMut(usize, &mut [Complex32])| {
                for (hand, plane) in fine_planes.iter_mut().enumerate() {
                    load(hand, plane);
                }
                let fine = |hand: usize| OversampledPlane {
                    values: fine_planes[hand].as_slice(),
                    side: fine_side,
                    centre: [fine_side / 2; 2],
                    sampling: oversampling,
                };
                if self.muellers == 1 {
                    dense_cell(&[fine(0)], [half_support; 2])
                } else {
                    dense_cell(&[fine(0), fine(1)], [half_support; 2])
                }
            };
            for cell in group.aw_planes {
                imaging.push(pack(&mut |hand, plane| {
                    resample(&cell.values[hand], a_side, &axis, scale, plane)
                }));
            }
            let mut padded = vec![Complex32::default(); a_side * a_side];
            for cells in group.cells.chunks_exact(self.muellers) {
                weight.push(pack(&mut |hand, plane| {
                    let cell = &cells[hand];
                    padded.fill(Complex32::default());
                    insert_centre(&mut padded, a_side, &cell.weight, cell.a_side, cell.a_side);
                    resample(&padded, a_side, &axis, scale, plane);
                }));
            }
        }
        let mut mueller = MuellerRouting::scalar(polarization.pol_map(), polarization.grid_pols());
        if self.muellers == 2 {
            for table in mueller.adjoint.iter_mut().chain(&mut mueller.forward) {
                for row in table {
                    for (plane, correlation) in row.iter_mut().zip(polarization.correlations()) {
                        if plane.is_some() {
                            *plane = match correlation {
                                Correlation::RR => Some(0),
                                Correlation::LL => Some(1),
                                _ => None,
                            };
                        }
                    }
                }
            }
        }
        Awp2 {
            frequencies_hz,
            w_planes,
            pa_centres,
            feed_angle_rad,
            row_frequency_hz,
            imaging: imaging.into_boxed_slice(),
            weight: weight.into_boxed_slice(),
            half_support,
            mueller,
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

/// AWPLPG's resident cells for VLA L-band imaging.
pub struct Awp2 {
    frequencies_hz: Box<[f64]>,
    w_planes: Box<[f64]>,
    pa_centres: Box<[f64]>,
    feed_angle_rad: f32,
    row_frequency_hz: f64,
    imaging: Box<[DenseCell]>,
    weight: Box<[DenseCell]>,
    half_support: u16,
    mueller: MuellerRouting,
    correction: ImageCorrection,
}

impl Awp2 {
    fn prediction_group(&self, key: CfKey) -> usize {
        let pa = usize::from(key.cube) / self.frequencies_hz.len();
        pa * self.w_planes.len() + usize::from(key.group) % self.w_planes.len()
    }

    fn holder_conjugates(&self, group: usize, w_positive: bool) -> bool {
        // The signed row index encodes W's sign only when its W index
        // is nonzero. The holder conjugates a zero or negative row.
        group == 0 || (!group.is_multiple_of(self.w_planes.len()) && w_positive)
    }

    /// Bytes retained after the screen and interpolation workspaces are freed.
    #[must_use]
    pub fn resident_bytes(&self) -> u64 {
        (std::mem::size_of_val(self.frequencies_hz.as_ref())
            + std::mem::size_of_val(self.w_planes.as_ref())
            + std::mem::size_of_val(self.pa_centres.as_ref())
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
    fn key(&self, row: &RowContext, freq_hz: f64, w_lambda: f64) -> CfKey {
        // AWConvFuncHolder compares the gridding frequency directly, using
        // the lower frequency when distances tie.
        let (frequency, _) = super::nearest(&self.frequencies_hz, freq_hz);
        // The holder selects the W plane at the visibility buffer's
        // mean native frequency, independently of the channel's A term.
        let w = (w_lambda * self.row_frequency_hz / freq_hz).abs();
        let (w_index, _) = super::nearest(&self.w_planes, w);
        let (pa, prediction_pa) = if self.pa_centres.is_empty() {
            (0, 0)
        } else {
            let pa = f64::from(-row.parallactic_angle_rad[0] as f32 + self.feed_angle_rad);
            let rotated = pa - std::f64::consts::PI;
            (
                super::nearest(&self.pa_centres, pa).0,
                super::nearest(&self.pa_centres, rotated.sin().atan2(rotated.cos())).0,
            )
        };
        // A residual sample uses both keys. Keep the prediction PA in the
        // high part of cube so the placement still needs only one key.
        CfKey {
            group: (pa * self.w_planes.len() + w_index) as u16,
            cube: (prediction_pa * self.frequencies_hz.len() + frequency) as u16,
        }
    }

    fn taps<'s>(&'s self, key: CfKey, _hold: &'s mut CellHold) -> TapLayout<'s> {
        let frequencies = self.frequencies_hz.len();
        self.imaging[usize::from(key.group) * frequencies + usize::from(key.cube) % frequencies]
            .layout()
    }

    fn max_half_support(&self) -> [u16; 2] {
        [self.half_support; 2]
    }

    fn weight_taps<'s>(&'s self, key: CfKey, _hold: &'s mut CellHold) -> Option<TapLayout<'s>> {
        let frequencies = self.frequencies_hz.len();
        let pa = usize::from(key.group) / self.w_planes.len();
        Some(self.weight[pa * frequencies + usize::from(key.cube) % frequencies].layout())
    }

    fn psf_taps<'s>(&'s self, mut key: CfKey, hold: &'s mut CellHold) -> TapLayout<'s> {
        key.group -= (usize::from(key.group) % self.w_planes.len()) as u16;
        self.taps(key, hold)
    }

    fn prediction_taps<'s>(&'s self, mut key: CfKey, hold: &'s mut CellHold) -> TapLayout<'s> {
        key.group = self.prediction_group(key) as u16;
        self.taps(key, hold)
    }

    fn adjoint_conjugates(&self, key: CfKey, mode: Mode, w_positive: bool) -> bool {
        if self.pa_centres.is_empty() {
            return w_positive;
        }
        match mode {
            Mode::Data => self.holder_conjugates(usize::from(key.group), w_positive),
            Mode::Psf => usize::from(key.group) < self.w_planes.len(),
            Mode::Weight => false,
        }
    }

    fn prediction_conjugates(&self, key: CfKey, w_positive: bool) -> bool {
        if self.pa_centres.is_empty() {
            return w_positive;
        }
        self.holder_conjugates(self.prediction_group(key), w_positive)
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
    fn awp2_squint_uses_the_casa_compact_aperture() {
        let cell_rad = 1.6_f64.to_radians() / 3600.0;
        let geometry = GridGeometry::new(
            crate::ImageExtent {
                shape: [1200; 2],
                increment_rad: [-cell_rad, cell_rad],
                reference_pixel: [600; 2],
            },
            crate::GridPadding::None,
        )
        .unwrap();
        let sampling = Awp2Sampling::squinted(&geometry).unwrap();
        assert_eq!((sampling.aperture_side, sampling.sky_side), (512, 512));
        assert_eq!(sampling.beam_cell_rad, 4.6875e-5);
        assert_eq!(sampling.oversampling, 9);
    }

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

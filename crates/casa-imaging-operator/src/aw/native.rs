// SPDX-License-Identifier: LGPL-3.0-or-later
//! Native generation of one paired EVLA A/W cell: CASA numerical
//! conventions follow `TransformMachines2/{AWConvFunc,
//! VLACalcIlluminationConvFunc, PSTerm, WTerm}.cc` at
//! 61020062cee290f5466cffed5ec5032e0c7a3434.

use casa_imaging_model::EvlaAwCellRequest;
use num_complex::Complex32;

use super::NativeAwGenerationError;
use super::evla::{EvlaApertureGrid, EvlaApertureModel};
use crate::fft::PlaneFft;
use crate::spheroidal::grdsf;

const SPEED_OF_LIGHT_M_PER_S: f64 = 299_792_458.0;

/// One generated, independently cropped plane, `size × size` with x
/// fastest and the kernel origin at `size / 2`.
#[derive(Debug)]
pub struct NativeAwPlane<'a> {
    /// Square pixel extent after support-buffer cropping.
    pub size: usize,
    /// Symmetric support in non-oversampled grid pixels.
    pub support: usize,
    /// Complex32 samples with the CASA sampled-area normalisation applied.
    pub values: &'a [Complex32],
}

/// Imaging and weight cells produced from the same scientific request.
#[derive(Debug)]
pub struct NativeAwPair<'a> {
    /// A times W times the optional anti-aliasing term.
    pub imaging: NativeAwPlane<'a>,
    /// Frequency/conjugate-frequency beam product, without the W phase.
    pub weight: NativeAwPlane<'a>,
}

/// Reusable one-cell numerical workspace: six square `Complex32` planes and
/// one FFT plan, independent of catalog size.
pub struct EvlaAwWorkspace {
    size: usize,
    fft: PlaneFft<f32>,
    jones: Vec<Complex32>,
    imaging: Vec<Complex32>,
    weight: Vec<Complex32>,
    realized: Option<[(usize, usize); 2]>,
}

impl EvlaAwWorkspace {
    /// Allocate the working grid of `request`'s size; the same allocation
    /// serves every cell of that size.
    ///
    /// # Errors
    ///
    /// The request is invalid or the FFT cannot be planned.
    pub fn new(request: EvlaAwCellRequest) -> Result<Self, NativeAwGenerationError> {
        request
            .validate()
            .map_err(|_| NativeAwGenerationError::InvalidGrid)?;
        let n = request.size * request.size;
        Ok(Self {
            size: request.size,
            fft: PlaneFft::<f32>::new([request.size; 2], false)
                .map_err(|_| NativeAwGenerationError::WorkspaceMismatch)?,
            jones: vec![Complex32::default(); 4 * n],
            imaging: vec![Complex32::default(); n],
            weight: vec![Complex32::default(); n],
            realized: None,
        })
    }

    /// Bytes of the working planes.
    #[must_use]
    pub fn resident_bytes(&self) -> usize {
        6 * self.size * self.size * size_of::<Complex32>()
    }

    /// Borrow the last successfully realized pair, with no pixel copies.
    #[must_use]
    pub fn pair(&self) -> Option<NativeAwPair<'_>> {
        let [
            (imaging_size, imaging_support),
            (weight_size, weight_support),
        ] = self.realized?;
        Some(NativeAwPair {
            imaging: NativeAwPlane {
                size: imaging_size,
                support: imaging_support,
                values: &self.imaging[..imaging_size * imaging_size],
            },
            weight: NativeAwPlane {
                size: weight_size,
                support: weight_support,
                values: &self.weight[..weight_size * weight_size],
            },
        })
    }

    /// Generate a paired EVLA A/W cell without CASA or cache access
    /// (`AWConvFunc::makeConvFunction2`): the sky Mueller element at the
    /// cell frequency times the W screen and the optional prolate
    /// spheroidal, and its weight partner at the conjugate frequency; both
    /// transformed, the imaging plane peak-normalised, both cropped to
    /// their support buffers and normalised by their sampled area.
    ///
    /// # Errors
    ///
    /// The request does not match the workspace, the receiver band is
    /// unsupported, or a value or normalisation is not finite.
    pub fn generate(
        &mut self,
        model: &EvlaApertureModel,
        request: EvlaAwCellRequest,
    ) -> Result<NativeAwPair<'_>, NativeAwGenerationError> {
        self.realized = None;
        request
            .validate()
            .map_err(|_| NativeAwGenerationError::InvalidGrid)?;
        if self.size != request.size {
            return Err(NativeAwGenerationError::WorkspaceMismatch);
        }
        let Self {
            fft,
            jones,
            imaging,
            weight,
            ..
        } = self;
        fill_sky_mueller(model, request, request.frequency_hz, fft, jones, imaging)?;
        fill_sky_mueller(
            model,
            request,
            request.conjugate_frequency_hz,
            fft,
            jones,
            weight,
        )?;
        let origin = (request.size / 2) as isize;
        let inner_half = (request.size / request.oversampling / 2) as isize;
        for y in 0..request.size {
            let iy = y as isize - origin;
            let m = request.sky_increment_rad[1] * iy as f64;
            for x in 0..request.size {
                let ix = x as isize - origin;
                let p = if request.prolate_spheroidal {
                    if (-inner_half..inner_half).contains(&ix)
                        && (-inner_half..inner_half).contains(&iy)
                    {
                        (grdsf(ix as f64 / inner_half as f64) as f32)
                            * (grdsf(iy as f64 / inner_half as f64) as f32)
                    } else {
                        0.0
                    }
                } else {
                    1.0
                };
                let pixel = x + request.size * y;
                weight[pixel] =
                    Complex32::new(p * p, 0.0) * (imaging[pixel] * weight[pixel].conj());
                let l = request.sky_increment_rad[0] * ix as f64;
                let rsq = l * l + m * m;
                let mut screen = Complex32::new(p, 0.0);
                if request.w_wavelengths > 0.0 && rsq < 1.0 {
                    let phase =
                        std::f64::consts::TAU * request.w_wavelengths * ((1.0 - rsq).sqrt() - 1.0);
                    let (sin, cos) = phase.sin_cos();
                    screen *= Complex32::new(cos as f32, sin as f32);
                }
                imaging[pixel] *= screen;
            }
        }
        // CASA's copy back from the FFT image excludes the final row and
        // column; the pre-transform edges survive in the spent Jones planes.
        let edges = &mut jones[..4 * request.size];
        save_edges(imaging, request.size, &mut edges[..2 * request.size]);
        save_edges(weight, request.size, &mut edges[2 * request.size..]);
        transform(fft, imaging)?;
        transform(fft, weight)?;
        restore_edges(imaging, request.size, &edges[..2 * request.size]);
        restore_edges(weight, request.size, &edges[2 * request.size..]);
        let mut peak = imaging[0];
        for value in &imaging[1..] {
            if value.norm_sqr() > peak.norm_sqr() {
                peak = *value;
            }
        }
        if peak.norm() == 0.0 || !peak.norm().is_finite() {
            return Err(NativeAwGenerationError::InvalidNumerics);
        }
        for value in imaging.iter_mut() {
            *value /= peak;
        }
        self.realized = Some([
            crop_normalize(imaging, request)?,
            crop_normalize(weight, request)?,
        ]);
        Ok(self.pair().expect("both crops succeeded"))
    }
}

/// The sky Mueller element `request.mueller` at `frequency`: the four
/// Jones aperture planes transformed, normalised by the centre power, the
/// selected plane conjugated and squared
/// (`VLACalcIlluminationConvFunc::applyPB`); unity without the aperture.
fn fill_sky_mueller(
    model: &EvlaApertureModel,
    request: EvlaAwCellRequest,
    frequency: f64,
    fft: &mut PlaneFft<f32>,
    jones: &mut [Complex32],
    output: &mut [Complex32],
) -> Result<(), NativeAwGenerationError> {
    if !request.aperture {
        output.fill(Complex32::new(1.0, 0.0));
        return Ok(());
    }
    let wavelength = SPEED_OF_LIGHT_M_PER_S / f64::from(frequency as f32);
    let cell_m = wavelength / (request.size as f64 * request.sky_increment_rad[0].abs());
    model.fill_aperture(
        EvlaApertureGrid::new(
            request.size,
            cell_m,
            3,
            frequency,
            request.parallactic_angle_rad,
        )?,
        jones,
    )?;
    let n = request.size * request.size;
    for plane in jones.chunks_exact_mut(n) {
        transform(fft, plane)?;
    }
    let center = request.size / 2 * (request.size + 1);
    let mut norm_squared = 0.0_f32;
    for plane in jones.chunks_exact(n) {
        norm_squared = (f64::from(norm_squared)
            + f64::from((plane[center] * plane[center]).norm()) / 2.0)
            as f32;
    }
    if !norm_squared.is_finite() || norm_squared <= 0.0 {
        return Err(NativeAwGenerationError::InvalidNumerics);
    }
    let normalization = norm_squared.sqrt();
    let selected = if request.mueller == 0 { 0 } else { 3 };
    for (output, value) in output
        .iter_mut()
        .zip(&jones[selected * n..(selected + 1) * n])
    {
        let value = (*value / normalization).conj();
        *output = value * value.conj();
    }
    Ok(())
}

/// A centred forward transform of a `[y][x]` plane in `f32`
/// (`FFT2D::c2cFFT`).
fn transform(
    fft: &mut PlaneFft<f32>,
    values: &mut [Complex32],
) -> Result<(), NativeAwGenerationError> {
    fft.transform(values, false)
        .map_err(|_| NativeAwGenerationError::WorkspaceMismatch)
}

fn save_edges(values: &[Complex32], size: usize, edges: &mut [Complex32]) {
    edges[..size].copy_from_slice(&values[(size - 1) * size..]);
    for y in 0..size {
        edges[size + y] = values[y * size + size - 1];
    }
}

fn restore_edges(values: &mut [Complex32], size: usize, edges: &[Complex32]) {
    values[(size - 1) * size..].copy_from_slice(&edges[..size]);
    for y in 0..size {
        values[y * size + size - 1] = edges[size + y];
    }
}

/// `AWConvFunc::resizeCF` and `cfArea`: the support from the last ring
/// above `1e-3` of the centre, the crop to the support buffer, and the
/// normalisation by the sampled area over `[−support, support)`.
fn crop_normalize(
    values: &mut [Complex32],
    request: EvlaAwCellRequest,
) -> Result<(usize, usize), NativeAwGenerationError> {
    let origin = request.size / 2;
    let threshold = (f64::from(values[origin * (request.size + 1)].norm()) * 1e-3) as f32;
    let mut radius = None;
    'radii: for r in (2..=origin - 2).rev() {
        for pixel in 0..90 * r {
            let angle = std::f64::consts::TAU * pixel as f64 / r as f64;
            let x = (origin as f64 + r as f64 * angle.sin()) as usize;
            let y = (origin as f64 + r as f64 * angle.cos()) as usize;
            if values[x + request.size * y].norm() > threshold {
                radius = Some(r);
                break 'radii;
            }
        }
    }
    let radius = radius.ok_or(NativeAwGenerationError::InvalidNumerics)?;
    let mut support = (0.5_f32 + radius as f32 / request.oversampling as f32) as usize + 1;
    if support * request.oversampling + (request.oversampling as f32 / 2.0 + 0.5) as usize > origin
    {
        support = origin / request.oversampling - 1;
    }
    if support == 0 {
        return Err(NativeAwGenerationError::InvalidNumerics);
    }
    let extent = (request.oversampling * (support + 2)) as isize;
    let bottom = (((origin as isize - extent) / 2) * 2).max(0) as usize;
    let top = ((((origin as isize + extent) / 2) * 2 - 1) as usize).min(request.size - 1);
    let size = top - bottom + 1;
    let mut area = Complex32::default();
    for ix in -(support as isize)..support as isize {
        for iy in -(support as isize)..support as isize {
            let x = (origin as isize + ix * request.oversampling as isize) as usize;
            let y = (origin as isize + iy * request.oversampling as isize) as usize;
            area += values[x + request.size * y];
        }
    }
    if area.norm() == 0.0 || !area.norm().is_finite() {
        return Err(NativeAwGenerationError::InvalidNumerics);
    }
    for y in 0..size {
        for x in 0..size {
            values[x + size * y] = values[x + bottom + request.size * (y + bottom)] / area;
        }
    }
    if values[..size * size]
        .iter()
        .any(|v| !v.re.is_finite() || !v.im.is_finite())
    {
        return Err(NativeAwGenerationError::InvalidNumerics);
    }
    Ok((size, support))
}

#[cfg(test)]
mod tests {
    use casa_imaging_model::EvlaDishSurface;

    use super::*;

    fn surface() -> EvlaDishSurface {
        EvlaDishSurface::new(
            (0..=125)
                .map(|i| {
                    let r = i as f64 / 10.0;
                    [r, r * r / 36.0, r / 18.0]
                })
                .collect(),
        )
        .expect("surface")
    }

    #[test]
    fn the_workspace_is_reused_and_deterministic() {
        let model = EvlaApertureModel::new(surface());
        let request = EvlaAwCellRequest {
            size: 128,
            sky_increment_rad: [-0.001, 0.001],
            frequency_hz: 3e9,
            conjugate_frequency_hz: 3.1e9,
            w_wavelengths: 100.0,
            parallactic_angle_rad: 0.31,
            mueller: 0,
            oversampling: 4,
            prolate_spheroidal: false,
            aperture: true,
        };
        let mut workspace = EvlaAwWorkspace::new(request).expect("workspace");
        let pair = workspace.generate(&model, request).expect("pair");
        let first = (pair.imaging.values.to_vec(), pair.weight.values.to_vec());
        let pair = workspace.generate(&model, request).expect("pair");
        assert_eq!(pair.imaging.values, first.0);
        assert_eq!(pair.weight.values, first.1);
        let mut invalid = request;
        invalid.size *= 2;
        assert!(workspace.generate(&model, invalid).is_err());
        assert!(workspace.pair().is_none());
    }

    #[test]
    fn an_aperture_free_pair_has_unit_sampled_area() {
        let model = EvlaApertureModel::new(
            EvlaDishSurface::new(vec![[0.0, 0.0, 0.0], [6.25, 1.0, 0.32], [12.5, 4.0, 0.64]])
                .expect("surface"),
        );
        let request = EvlaAwCellRequest {
            size: 128,
            sky_increment_rad: [-0.001, 0.001],
            frequency_hz: 3e9,
            conjugate_frequency_hz: 3e9,
            w_wavelengths: 0.0,
            parallactic_angle_rad: 0.0,
            mueller: 0,
            oversampling: 4,
            prolate_spheroidal: true,
            aperture: false,
        };
        let mut workspace = EvlaAwWorkspace::new(request).expect("workspace");
        let pair = workspace.generate(&model, request).expect("pair");
        for plane in [pair.imaging, pair.weight] {
            let mut area = num_complex::Complex64::default();
            for ix in -(plane.support as isize)..plane.support as isize {
                for iy in -(plane.support as isize)..plane.support as isize {
                    let x = (plane.size as isize / 2 + ix * 4) as usize;
                    let y = (plane.size as isize / 2 + iy * 4) as usize;
                    let value = plane.values[x + plane.size * y];
                    area += num_complex::Complex64::new(f64::from(value.re), f64::from(value.im));
                }
            }
            assert!(
                (area - 1.0).norm() < 2e-6,
                "sampled-area normalisation {area}"
            );
        }
    }

    #[test]
    fn the_centred_f32_transform_obeys_the_impulse_and_phase_laws() {
        let size = 16;
        let mut fft = PlaneFft::<f32>::new([size; 2], false).expect("plan");
        let mut values = vec![Complex32::default(); size * size];
        values[8 + size * 8] = Complex32::new(1.0, 0.0);
        transform(&mut fft, &mut values).expect("transform");
        assert!(values.iter().all(|v| *v == Complex32::new(1.0, 0.0)));
        values.fill(Complex32::default());
        values[9 + size * 8] = Complex32::new(1.0, 0.0);
        transform(&mut fft, &mut values).expect("transform");
        for y in 0..size {
            for x in 0..size {
                let expected = Complex32::from_polar(
                    1.0,
                    -std::f32::consts::TAU * (x as f32 - 8.0) / size as f32,
                );
                assert!((values[x + size * y] - expected).norm() <= 4.0 * f32::EPSILON);
            }
        }
    }
}

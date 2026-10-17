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

/// Reusable cell workspace: seven square `Complex32` planes, the support
/// search's ring table, the W screen's quadrant tables and one FFT plan,
/// independent of catalog size.
///
/// A cell is the sky Mueller element times the W screen, transformed; only
/// the screen depends on w. [`Self::prepare`] forms the w-independent part
/// once (the aperture plane and the whole weight cell) and
/// [`Self::realize`] then forms the imaging cell of each w from it with one
/// transform, the arithmetic [`Self::generate`] performs per cell.
///
/// The workspace serves one aperture model, bound when it is made, so a
/// prepared aperture always belongs to the model its cells are asked of.
pub struct EvlaAwWorkspace {
    model: EvlaApertureModel,
    size: usize,
    fft: PlaneFft<f32>,
    jones: Vec<Complex32>,
    /// The sky Mueller element at the prepared cell's frequency.
    aperture: Vec<Complex32>,
    imaging: Vec<Complex32>,
    /// The prepared cell's cropped, normalised weight cell.
    weight: Vec<Complex32>,
    /// The request [`Self::prepare`] formed the aperture and weight cell
    /// for, at w = 0, and the weight cell's `(size, support)`.
    prepared: Option<(EvlaAwCellRequest, (usize, usize))>,
    /// The imaging crop of the last realized pair.
    realized: Option<(usize, usize)>,
    /// Per working-grid pixel, the largest ring [`crop_normalize`]'s
    /// support search samples it on (zero when none does).
    rings: Vec<u16>,
    /// `√(1 − l² − m²) − 1` over one quadrant of pixel offsets
    /// `(|ix|, |iy|)`, for the sky increments it was formed for; the W
    /// screen's phase is `2π·w` times it.
    depth: Vec<f64>,
    depth_increment: Option<[f64; 2]>,
    /// The W screen's phase factors over the same quadrant, for one w.
    phases: Vec<Complex32>,
}

impl EvlaAwWorkspace {
    /// Allocate the working grid of `request`'s size for `model`'s cells;
    /// the same allocation serves every cell of that size.
    ///
    /// # Errors
    ///
    /// The request is invalid or the FFT cannot be planned.
    pub fn new(
        model: EvlaApertureModel,
        request: EvlaAwCellRequest,
    ) -> Result<Self, NativeAwGenerationError> {
        request
            .validate()
            .map_err(|_| NativeAwGenerationError::InvalidGrid)?;
        let n = request.size * request.size;
        let quadrant = (request.size / 2 + 1) * (request.size / 2 + 1);
        Ok(Self {
            model,
            size: request.size,
            fft: PlaneFft::<f32>::new([request.size; 2], false)
                .map_err(|_| NativeAwGenerationError::WorkspaceMismatch)?,
            jones: vec![Complex32::default(); 4 * n],
            aperture: vec![Complex32::default(); n],
            imaging: vec![Complex32::default(); n],
            weight: vec![Complex32::default(); n],
            prepared: None,
            realized: None,
            rings: ring_table(request.size),
            depth: vec![0.0; quadrant],
            depth_increment: None,
            phases: vec![Complex32::default(); quadrant],
        })
    }

    /// Bytes of the working planes and tables.
    #[must_use]
    pub fn resident_bytes(&self) -> usize {
        Self::bytes(self.size)
    }

    /// Bytes of the working planes and tables of a workspace of `size`
    /// ([`Self::new`]): the four Jones planes, the aperture, imaging and
    /// weight planes, the ring table, and the W screen's depth and phases
    /// over one quadrant.
    #[must_use]
    pub const fn bytes(size: usize) -> usize {
        let quadrant = (size / 2 + 1) * (size / 2 + 1);
        7 * size * size * size_of::<Complex32>()
            + size * size * size_of::<u16>()
            + quadrant * (size_of::<f64>() + size_of::<Complex32>())
    }

    /// Borrow the last successfully realized pair, with no pixel copies.
    #[must_use]
    pub fn pair(&self) -> Option<NativeAwPair<'_>> {
        let (imaging_size, imaging_support) = self.realized?;
        let (_, (weight_size, weight_support)) = self.prepared?;
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
        request: EvlaAwCellRequest,
    ) -> Result<NativeAwPair<'_>, NativeAwGenerationError> {
        self.prepare(request)?;
        self.realize(request)
    }

    /// Form the w-independent part of `request`'s cell, unless it is the
    /// one already prepared: the sky Mueller element at the cell frequency,
    /// kept for [`Self::realize`], and the whole weight cell, which carries
    /// no W phase. `request.w_wavelengths` is ignored.
    ///
    /// # Errors
    ///
    /// As [`Self::generate`].
    pub fn prepare(&mut self, request: EvlaAwCellRequest) -> Result<(), NativeAwGenerationError> {
        self.realized = None;
        request
            .validate()
            .map_err(|_| NativeAwGenerationError::InvalidGrid)?;
        if self.size != request.size {
            return Err(NativeAwGenerationError::WorkspaceMismatch);
        }
        let key = EvlaAwCellRequest {
            w_wavelengths: 0.0,
            ..request
        };
        if self.prepared.is_some_and(|(held, _)| held == key) {
            return Ok(());
        }
        self.prepared = None;
        let Self {
            model,
            fft,
            jones,
            aperture,
            weight,
            rings,
            ..
        } = self;
        fill_sky_mueller(model, request, request.frequency_hz, fft, jones, aperture)?;
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
            for x in 0..request.size {
                let ix = x as isize - origin;
                let p = prolate(request, inner_half, ix, iy);
                let pixel = x + request.size * y;
                weight[pixel] =
                    Complex32::new(p * p, 0.0) * (aperture[pixel] * weight[pixel].conj());
            }
        }
        // CASA's copy back from the FFT image excludes the final row and
        // column; the pre-transform edges survive in the spent Jones planes.
        let edges = &mut jones[..2 * request.size];
        save_edges(weight, request.size, edges);
        transform(fft, weight)?;
        restore_edges(weight, request.size, edges);
        let weight_crop = crop_normalize(weight, request, rings)?;
        self.prepared = Some((key, weight_crop));
        Ok(())
    }

    /// Form the imaging cell of `request` at its w from the aperture
    /// [`Self::prepare`] formed for the same request: the aperture times the
    /// W screen and the optional prolate spheroidal, transformed,
    /// peak-normalised and cropped, beside the prepared weight cell.
    ///
    /// # Errors
    ///
    /// The workspace holds no aperture for `request` (other than its w), or
    /// a value or normalisation is not finite.
    pub fn realize(
        &mut self,
        request: EvlaAwCellRequest,
    ) -> Result<NativeAwPair<'_>, NativeAwGenerationError> {
        self.realized = None;
        let key = EvlaAwCellRequest {
            w_wavelengths: 0.0,
            ..request
        };
        if self.prepared.is_none_or(|(held, _)| held != key) {
            return Err(NativeAwGenerationError::WorkspaceMismatch);
        }
        let Self {
            fft,
            jones,
            aperture,
            imaging,
            rings,
            depth,
            depth_increment,
            phases,
            ..
        } = self;
        // The screen's phase depends on (l², m²) alone, so one quadrant of
        // |offsets| serves all four; the same expressions as per pixel.
        let half = request.size / 2;
        let stride = half + 1;
        if *depth_increment != Some(request.sky_increment_rad) {
            for ay in 0..=half {
                let m = request.sky_increment_rad[1] * ay as f64;
                for ax in 0..=half {
                    let l = request.sky_increment_rad[0] * ax as f64;
                    let rsq = l * l + m * m;
                    depth[ax + stride * ay] = if rsq < 1.0 {
                        (1.0 - rsq).sqrt() - 1.0
                    } else {
                        f64::NAN
                    };
                }
            }
            *depth_increment = Some(request.sky_increment_rad);
        }
        let screened = request.w_wavelengths > 0.0;
        if screened {
            for (phase, depth) in phases.iter_mut().zip(depth.iter()) {
                if !depth.is_nan() {
                    let (sin, cos) =
                        (std::f64::consts::TAU * request.w_wavelengths * depth).sin_cos();
                    *phase = Complex32::new(cos as f32, sin as f32);
                }
            }
        }
        let origin = half as isize;
        let inner_half = (request.size / request.oversampling / 2) as isize;
        for y in 0..request.size {
            let iy = y as isize - origin;
            for x in 0..request.size {
                let ix = x as isize - origin;
                let p = prolate(request, inner_half, ix, iy);
                let pixel = x + request.size * y;
                let mut screen = Complex32::new(p, 0.0);
                let quadrant = ix.unsigned_abs() + stride * iy.unsigned_abs();
                if screened && !depth[quadrant].is_nan() {
                    screen *= phases[quadrant];
                }
                imaging[pixel] = aperture[pixel] * screen;
            }
        }
        let edges = &mut jones[..2 * request.size];
        save_edges(imaging, request.size, edges);
        transform(fft, imaging)?;
        restore_edges(imaging, request.size, edges);
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
        let crop = crop_normalize(imaging, request, rings)?;
        self.realized = Some(crop);
        Ok(self.pair().expect("both crops succeeded"))
    }
}

/// `AWConvFunc::resizeCF`'s support radius: the outermost ring with a
/// sample above `1e-3` of the centre, read from [`ring_table`] instead of
/// walking the rings.
fn support_radius(values: &[Complex32], size: usize, rings: &[u16]) -> Option<usize> {
    let origin = size / 2;
    let threshold = (f64::from(values[origin * (size + 1)].norm()) * 1e-3) as f32;
    let mut largest = 0_u16;
    for (value, &ring) in values.iter().zip(rings) {
        if ring > largest && value.norm() > threshold {
            largest = ring;
        }
    }
    (largest >= 2).then_some(usize::from(largest))
}

/// Per pixel of a `size × size` working grid, the largest ring
/// [`crop_normalize`]'s support search samples it on, zero when none does:
/// ring `r` samples the pixels at `origin + (r·sin θ, r·cos θ)` for
/// `θ = 2π·k/r`, `k < 90·r` (`AWConvFunc::resizeCF`), so the search's radius
/// is the largest entry over the pixels above its threshold.
fn ring_table(size: usize) -> Vec<u16> {
    let origin = size / 2;
    let mut rings = vec![0_u16; size * size];
    for r in 2..=origin - 2 {
        for pixel in 0..90 * r {
            let angle = std::f64::consts::TAU * pixel as f64 / r as f64;
            let x = (origin as f64 + r as f64 * angle.sin()) as usize;
            let y = (origin as f64 + r as f64 * angle.cos()) as usize;
            rings[x + size * y] = u16::try_from(r).unwrap_or(u16::MAX);
        }
    }
    rings
}

/// The prolate-spheroidal anti-aliasing factor at pixel offset `(ix, iy)`
/// of the working grid, or one without it.
fn prolate(request: EvlaAwCellRequest, inner_half: isize, ix: isize, iy: isize) -> f32 {
    if !request.prolate_spheroidal {
        return 1.0;
    }
    if (-inner_half..inner_half).contains(&ix) && (-inner_half..inner_half).contains(&iy) {
        (grdsf(ix as f64 / inner_half as f64) as f32)
            * (grdsf(iy as f64 / inner_half as f64) as f32)
    } else {
        0.0
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
    rings: &[u16],
) -> Result<(usize, usize), NativeAwGenerationError> {
    let origin = request.size / 2;
    let radius = support_radius(values, request.size, rings)
        .ok_or(NativeAwGenerationError::InvalidNumerics)?;
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
        let mut workspace = EvlaAwWorkspace::new(model, request).expect("workspace");
        let pair = workspace.generate(request).expect("pair");
        let first = (pair.imaging.values.to_vec(), pair.weight.values.to_vec());
        let pair = workspace.generate(request).expect("pair");
        assert_eq!(pair.imaging.values, first.0);
        assert_eq!(pair.weight.values, first.1);
        let mut invalid = request;
        invalid.size *= 2;
        assert!(workspace.generate(invalid).is_err());
        assert!(workspace.pair().is_none());
    }

    /// A dish surface whose sag is `scale` times [`surface`]'s.
    fn scaled_surface(scale: f64) -> EvlaDishSurface {
        EvlaDishSurface::new(
            (0..=125)
                .map(|i| {
                    let r = i as f64 / 10.0;
                    [r, r * r / (36.0 * scale), r / (18.0 * scale)]
                })
                .collect(),
        )
        .expect("surface")
    }

    /// A reused workspace, its preparation kept or replaced as requests
    /// alternate, forms exactly what a fresh workspace forms for each
    /// request, and the cells are its own model's: two surfaces give
    /// different cells (Astra's probe on #712 reused one workspace for two
    /// models and got the first model's cells, 0.033 NRMS from the second's).
    #[test]
    fn a_reused_workspace_forms_its_own_models_cells_whatever_it_prepared_before() {
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
        let other = EvlaAwCellRequest {
            frequency_hz: 3.4e9,
            w_wavelengths: 40.0,
            ..request
        };
        let fresh = |model: &EvlaApertureModel, request| {
            let mut workspace = EvlaAwWorkspace::new(model.clone(), request).expect("workspace");
            let pair = workspace.generate(request).expect("pair");
            (pair.imaging.values.to_vec(), pair.weight.values.to_vec())
        };
        let models = [1.0, 1.01].map(|scale| EvlaApertureModel::new(scaled_surface(scale)));
        for model in &models {
            let mut reused = EvlaAwWorkspace::new(model.clone(), request).expect("workspace");
            for at in [request, other, request, other] {
                let pair = reused.generate(at).expect("pair");
                let expected = fresh(model, at);
                assert_eq!(pair.imaging.values, expected.0);
                assert_eq!(pair.weight.values, expected.1);
            }
        }
        assert_ne!(
            fresh(&models[0], request),
            fresh(&models[1], request),
            "the two surfaces form different cells"
        );
    }

    #[test]
    fn one_prepared_aperture_realizes_every_w_as_generate_does() {
        let model = EvlaApertureModel::new(surface());
        let request = EvlaAwCellRequest {
            size: 128,
            sky_increment_rad: [-0.001, 0.001],
            frequency_hz: 3e9,
            conjugate_frequency_hz: 3.1e9,
            w_wavelengths: 0.0,
            parallactic_angle_rad: 0.31,
            mueller: 15,
            oversampling: 4,
            prolate_spheroidal: true,
            aperture: true,
        };
        let mut whole = EvlaAwWorkspace::new(model.clone(), request).expect("workspace");
        let mut factored = EvlaAwWorkspace::new(model, request).expect("workspace");
        factored.prepare(request).expect("prepare");
        for w in [0.0, 40.0, 100.0, 250.0] {
            let at_w = EvlaAwCellRequest {
                w_wavelengths: w,
                ..request
            };
            let expected = whole.generate(at_w).expect("pair");
            let expected = (
                expected.imaging.values.to_vec(),
                expected.imaging.support,
                expected.weight.values.to_vec(),
            );
            let pair = factored.realize(at_w).expect("realized");
            assert_eq!(pair.imaging.values, expected.0, "imaging cell at w = {w}");
            assert_eq!(pair.imaging.support, expected.1);
            assert_eq!(pair.weight.values, expected.2, "weight cell at w = {w}");
        }
        let other = EvlaAwCellRequest {
            frequency_hz: 3.2e9,
            ..request
        };
        assert!(factored.realize(other).is_err());
    }

    #[test]
    fn the_quadrant_screen_is_the_per_pixel_screen() {
        let model = EvlaApertureModel::new(surface());
        // At 0.012 rad a pixel the grid's corners lie past l² + m² = 1,
        // where the screen carries no W phase.
        for (prolate_spheroidal, increment) in [(false, 0.0021), (true, 0.0021), (true, 0.012)] {
            let request = EvlaAwCellRequest {
                size: 128,
                sky_increment_rad: [-increment, increment],
                frequency_hz: 3e9,
                conjugate_frequency_hz: 3.1e9,
                w_wavelengths: 180.0,
                parallactic_angle_rad: 0.31,
                mueller: 0,
                oversampling: 4,
                prolate_spheroidal,
                aperture: true,
            };
            let mut workspace = EvlaAwWorkspace::new(model.clone(), request).expect("workspace");
            workspace.prepare(request).expect("prepare");
            // The imaging plane before its transform, screened pixel by
            // pixel as the generator once did.
            let origin = (request.size / 2) as isize;
            let inner_half = (request.size / request.oversampling / 2) as isize;
            let mut expected = workspace.aperture.clone();
            for y in 0..request.size {
                let iy = y as isize - origin;
                let m = request.sky_increment_rad[1] * iy as f64;
                for x in 0..request.size {
                    let ix = x as isize - origin;
                    let l = request.sky_increment_rad[0] * ix as f64;
                    let rsq = l * l + m * m;
                    let mut screen = Complex32::new(prolate(request, inner_half, ix, iy), 0.0);
                    if rsq < 1.0 {
                        let phase = std::f64::consts::TAU
                            * request.w_wavelengths
                            * ((1.0 - rsq).sqrt() - 1.0);
                        let (sin, cos) = phase.sin_cos();
                        screen *= Complex32::new(cos as f32, sin as f32);
                    }
                    expected[x + request.size * y] *= screen;
                }
            }
            let mut fft = PlaneFft::<f32>::new([request.size; 2], false).expect("plan");
            let mut edges = vec![Complex32::default(); 2 * request.size];
            save_edges(&expected, request.size, &mut edges);
            transform(&mut fft, &mut expected).expect("transform");
            restore_edges(&mut expected, request.size, &edges);
            let mut peak = expected[0];
            for value in &expected[1..] {
                if value.norm_sqr() > peak.norm_sqr() {
                    peak = *value;
                }
            }
            for value in &mut expected {
                *value /= peak;
            }
            let rings = ring_table(request.size);
            let crop = crop_normalize(&mut expected, request, &rings).expect("crop");
            let pair = workspace.realize(request).expect("realize");
            assert_eq!(crop, (pair.imaging.size, pair.imaging.support));
            assert_eq!(
                &expected[..crop.0 * crop.0],
                pair.imaging.values,
                "prolate spheroidal {prolate_spheroidal}"
            );
        }
    }

    /// `AWConvFunc::resizeCF`'s ring walk, as `crop_normalize` once did it.
    fn walked_radius(values: &[Complex32], size: usize) -> Option<usize> {
        let origin = size / 2;
        let threshold = (f64::from(values[origin * (size + 1)].norm()) * 1e-3) as f32;
        for r in (2..=origin - 2).rev() {
            for pixel in 0..90 * r {
                let angle = std::f64::consts::TAU * pixel as f64 / r as f64;
                let x = (origin as f64 + r as f64 * angle.sin()) as usize;
                let y = (origin as f64 + r as f64 * angle.cos()) as usize;
                if values[x + size * y].norm() > threshold {
                    return Some(r);
                }
            }
        }
        None
    }

    #[test]
    fn the_ring_table_finds_the_walks_support_radius() {
        for size in [64, 128, 256] {
            let rings = ring_table(size);
            let origin = (size / 2) as f64;
            for (width, spike) in [(2.0, None), (9.5, None), (31.0, Some(17)), (3.0, Some(5))] {
                let mut values = (0..size * size)
                    .map(|pixel| {
                        let dx = (pixel % size) as f64 - origin;
                        let dy = (pixel / size) as f64 - origin;
                        let g = (-(dx * dx + dy * dy) / (2.0 * width * width)).exp() as f32;
                        Complex32::new(g, 0.5 * g)
                    })
                    .collect::<Vec<_>>();
                if let Some(offset) = spike {
                    // An isolated sample beyond the core, off the axes.
                    values[(size / 2 + offset) + size * (size / 2 + offset / 2)] =
                        Complex32::new(0.01, 0.0);
                }
                assert_eq!(
                    support_radius(&values, size, &rings),
                    walked_radius(&values, size),
                    "size {size}, width {width}, spike {spike:?}"
                );
            }
        }
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
        let mut workspace = EvlaAwWorkspace::new(model, request).expect("workspace");
        let pair = workspace.generate(request).expect("pair");
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

// SPDX-License-Identifier: LGPL-3.0-or-later
// CASA numerical conventions follow TransformMachines2/{AWConvFunc,
// VLACalcIlluminationConvFunc,PSTerm,WTerm}.cc at
// 61020062cee290f5466cffed5ec5032e0c7a3434.

use casa_imaging_model::EvlaAwCellRequest;
use ndarray::{ArrayViewMut2, ShapeBuilder};
use num_complex::Complex32;

use super::{EvlaApertureGrid, EvlaApertureModel, NativeAwGenerationError};
use crate::spectral_operator::{
    PreparedFft, SPEED_OF_LIGHT_M_PER_S, fft_resident_complex_values_for_shape, grdsf,
};

/// Conservative numerical workspace bound, including FFT plans and scratch.
pub fn evla_aw_workspace_bytes(
    request: EvlaAwCellRequest,
) -> Result<usize, NativeAwGenerationError> {
    request
        .validate()
        .map_err(|_| NativeAwGenerationError::InvalidGrid)?;
    let fft = fft_resident_complex_values_for_shape([request.size; 2])
        .map_err(|_| NativeAwGenerationError::WorkspaceMismatch)?;
    (request.size * request.size * 6)
        .checked_add(fft)
        .and_then(|n| n.checked_mul(size_of::<Complex32>()))
        .ok_or(NativeAwGenerationError::WorkspaceMismatch)
}

/// One generated, independently cropped plane in x-contiguous order.
#[derive(Debug)]
pub struct NativeAwPlane {
    /// Square pixel extent after support-buffer cropping.
    pub size: usize,
    /// Symmetric support in non-oversampled grid pixels.
    pub support: usize,
    /// Complex32 samples with the CASA sampled-area normalization applied.
    pub values: Vec<Complex32>,
}

/// Imaging and weight cells produced from the same scientific request.
#[derive(Debug)]
pub struct NativeAwPair {
    /// A times W times the optional anti-aliasing term.
    pub imaging: NativeAwPlane,
    /// Frequency/conjugate-frequency beam product, without the W phase.
    pub weight: NativeAwPlane,
}

/// Generate a genuine paired EVLA A/W cell without CASA or cache access.
///
/// Admission must precede this call using the request's workspace bound plus
/// the model's retained bytes. The computation keeps six square Complex32
/// planes, independent of catalog size, and compacts both outputs in place.
pub fn generate_evla_aw_pair(
    model: &EvlaApertureModel,
    request: EvlaAwCellRequest,
) -> Result<NativeAwPair, NativeAwGenerationError> {
    evla_aw_workspace_bytes(request)?;
    let n = request.size * request.size;
    let reserve = fft_resident_complex_values_for_shape([request.size; 2])
        .map_err(|_| NativeAwGenerationError::WorkspaceMismatch)?;
    let mut fft = PreparedFft::<f32>::new([request.size; 2], reserve)
        .map_err(|_| NativeAwGenerationError::WorkspaceMismatch)?;
    let mut jones = vec![Complex32::default(); 4 * n];
    let mut imaging = vec![Complex32::default(); n];
    let mut weight = vec![Complex32::default(); n];
    fill_sky_mueller(
        model,
        request,
        request.frequency_hz,
        &mut fft,
        &mut jones,
        &mut imaging,
    )?;
    fill_sky_mueller(
        model,
        request,
        request.conjugate_frequency_hz,
        &mut fft,
        &mut jones,
        &mut weight,
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
            weight[pixel] = Complex32::new(p * p, 0.0) * (imaging[pixel] * weight[pixel].conj());
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
    // CASA's copy back from the FFT image excludes the final row and column.
    // Retain these pre-transform edges in the no-longer-used Jones workspace.
    let edges = &mut jones[..4 * request.size];
    save_edges(&imaging, request.size, &mut edges[..2 * request.size]);
    save_edges(&weight, request.size, &mut edges[2 * request.size..]);
    transform(&mut fft, request.size, &mut imaging);
    transform(&mut fft, request.size, &mut weight);
    restore_edges(&mut imaging, request.size, &edges[..2 * request.size]);
    restore_edges(&mut weight, request.size, &edges[2 * request.size..]);
    let mut peak = imaging[0];
    for value in &imaging[1..] {
        if value.norm_sqr() > peak.norm_sqr() {
            peak = *value;
        }
    }
    if peak.norm() == 0.0 || !peak.norm().is_finite() {
        return Err(NativeAwGenerationError::InvalidNumerics);
    }
    for value in &mut imaging {
        *value /= peak;
    }
    Ok(NativeAwPair {
        imaging: crop_normalize(imaging, request)?,
        weight: crop_normalize(weight, request)?,
    })
}

fn fill_sky_mueller(
    model: &EvlaApertureModel,
    request: EvlaAwCellRequest,
    frequency: f64,
    fft: &mut PreparedFft<f32>,
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
        transform(fft, request.size, plane);
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

fn transform(fft: &mut PreparedFft<f32>, size: usize, values: &mut [Complex32]) {
    let mut plane =
        ArrayViewMut2::from_shape((size, size).f(), values).expect("validated square workspace");
    fft.transform(&mut plane, false);
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

fn crop_normalize(
    mut values: Vec<Complex32>,
    request: EvlaAwCellRequest,
) -> Result<NativeAwPlane, NativeAwGenerationError> {
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
    values.truncate(size * size);
    if values
        .iter()
        .any(|v| !v.re.is_finite() || !v.im.is_finite())
    {
        return Err(NativeAwGenerationError::InvalidNumerics);
    }
    Ok(NativeAwPlane {
        size,
        support,
        values,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn t52_centered_float_fft_obeys_impulse_and_phase_laws() {
        let size = 16;
        let reserve = fft_resident_complex_values_for_shape([size; 2]).unwrap();
        let mut fft = PreparedFft::<f32>::new([size; 2], reserve).unwrap();
        let mut values = vec![Complex32::default(); size * size];
        values[8 + size * 8] = Complex32::new(1.0, 0.0);
        transform(&mut fft, size, &mut values);
        assert!(values.iter().all(|v| *v == Complex32::new(1.0, 0.0)));
        values.fill(Complex32::default());
        values[9 + size * 8] = Complex32::new(1.0, 0.0);
        transform(&mut fft, size, &mut values);
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

    #[test]
    #[ignore = "requires explicit frozen EVLA paired-cell request, surface and pixels"]
    fn t52_evla_paired_cell_matches_frozen_casa() {
        let root = std::path::PathBuf::from(std::env::var_os("CASA_RS_T52_CELL_FIXTURE").unwrap());
        let specification: serde_json::Value =
            serde_json::from_str(&std::fs::read_to_string(root.join("request.json")).unwrap())
                .unwrap();
        let surface =
            std::fs::read_to_string(std::env::var("CASA_RS_T52_SURFACE").unwrap()).unwrap();
        let model = EvlaApertureModel::new(
            casa_imaging_model::EvlaDishSurface::from_surface_text(&surface).unwrap(),
        );
        let f = |key: &str| specification[key].as_f64().unwrap();
        let u = |key: &str| specification[key].as_u64().unwrap() as usize;
        let cell = f("sky_cell_rad");
        let request = EvlaAwCellRequest {
            size: u("working_size"),
            sky_increment_rad: [-cell, cell],
            frequency_hz: f("frequency_hz"),
            conjugate_frequency_hz: f("conjugate_frequency_hz"),
            w_wavelengths: f("w_wavelengths"),
            parallactic_angle_rad: f("pa_rad"),
            mueller: u("mueller"),
            oversampling: u("oversampling"),
            prolate_spheroidal: specification["prolate_spheroidal"].as_bool().unwrap(),
            aperture: true,
        };
        let result = generate_evla_aw_pair(&model, request).unwrap();
        for (role, plane) in [("imaging", result.imaging), ("weight", result.weight)] {
            let bytes = std::fs::read(root.join(format!("{role}.bin"))).unwrap();
            eprintln!(
                "T52 {role} size={} support={} values={} reference_bytes={}",
                plane.size,
                plane.support,
                plane.values.len(),
                bytes.len()
            );
            assert_eq!(
                plane.size,
                specification[role]["size"].as_u64().unwrap() as usize
            );
            assert_eq!(
                plane.support,
                specification[role]["support"].as_u64().unwrap() as usize
            );
            assert_eq!(bytes.len(), plane.values.len() * 8);
            let mut error_squared = 0.0_f64;
            let mut reference_squared = 0.0_f64;
            let mut maximum_error = 0.0_f64;
            let mut reference_peak = 0.0_f64;
            // The T51 exporter writes its canonical last-axis-contiguous
            // representation; native numerical workspaces are x-contiguous.
            for (index, bytes) in bytes.chunks_exact(8).enumerate() {
                let value = plane.values[index / plane.size + plane.size * (index % plane.size)];
                let reference = Complex32::new(
                    f32::from_le_bytes(bytes[..4].try_into().unwrap()),
                    f32::from_le_bytes(bytes[4..].try_into().unwrap()),
                );
                let error = f64::from((value - reference).norm());
                let magnitude = f64::from(reference.norm());
                error_squared += error * error;
                reference_squared += magnitude * magnitude;
                maximum_error = maximum_error.max(error);
                reference_peak = reference_peak.max(magnitude);
            }
            let relative_l2 = (error_squared / reference_squared).sqrt();
            let peak_scaled_max = maximum_error / reference_peak;
            eprintln!(
                "T52 {role} relative_l2={relative_l2:.12e} peak_scaled_max={peak_scaled_max:.12e}"
            );
            // Independent f32 FFT implementations may differ in roundoff, but
            // no fitted scale, phase, support, or science tolerance is allowed.
            assert!(relative_l2 <= 1e-5, "{role} relative L2 {relative_l2}");
            assert!(
                peak_scaled_max <= 1e-5,
                "{role} element error {peak_scaled_max}"
            );
        }
    }
}

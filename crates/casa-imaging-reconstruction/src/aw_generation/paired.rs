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
fn evla_aw_workspace_bytes(request: EvlaAwCellRequest) -> Result<usize, NativeAwGenerationError> {
    request
        .generation_workspace_bytes()
        .map_err(|_| NativeAwGenerationError::InvalidGrid)
}

/// One generated, independently cropped plane in x-contiguous order.
#[derive(Debug)]
pub struct NativeAwPlane<'a> {
    /// Square pixel extent after support-buffer cropping.
    pub size: usize,
    /// Symmetric support in non-oversampled grid pixels.
    pub support: usize,
    /// Complex32 samples with the CASA sampled-area normalization applied.
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

/// Reusable, one-cell numerical allocation. Construct only after admission.
pub struct EvlaAwWorkspace {
    size: usize,
    fft: PreparedFft<f32>,
    jones: Vec<Complex32>,
    imaging: Vec<Complex32>,
    weight: Vec<Complex32>,
    realized: Option<[(usize, usize); 2]>,
    reserved_bytes: usize,
}

impl EvlaAwWorkspace {
    /// Allocate exactly one admitted working grid; catalog size does not
    /// multiply its residency. The same allocation is reused for every cell.
    pub fn new(
        request: EvlaAwCellRequest,
        admitted_bytes: usize,
    ) -> Result<Self, NativeAwGenerationError> {
        let reserved_bytes = evla_aw_workspace_bytes(request)?;
        if reserved_bytes > admitted_bytes {
            return Err(NativeAwGenerationError::WorkspaceMismatch);
        }
        let reserve = fft_resident_complex_values_for_shape([request.size; 2])
            .map_err(|_| NativeAwGenerationError::WorkspaceMismatch)?;
        let n = request.size * request.size;
        if (6 * n + reserve) * size_of::<Complex32>() != reserved_bytes {
            return Err(NativeAwGenerationError::WorkspaceMismatch);
        }
        Ok(Self {
            size: request.size,
            fft: PreparedFft::<f32>::new([request.size; 2], reserve)
                .map_err(|_| NativeAwGenerationError::WorkspaceMismatch)?,
            jones: vec![Complex32::default(); 4 * n],
            imaging: vec![Complex32::default(); n],
            weight: vec![Complex32::default(); n],
            realized: None,
            reserved_bytes,
        })
    }

    /// Charged allocation, including the shared FFT's opaque-plan reservation.
    #[must_use]
    pub const fn resident_bytes(&self) -> usize {
        self.reserved_bytes
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

    /// Generate a genuine paired EVLA A/W cell without CASA or cache access.
    ///
    /// Admission must precede this call using the request's workspace bound plus
    /// the model's retained bytes. The computation keeps six square Complex32
    /// planes, independent of catalog size, and compacts both outputs in place.
    pub fn generate(
        &mut self,
        model: &EvlaApertureModel,
        request: EvlaAwCellRequest,
    ) -> Result<NativeAwPair<'_>, NativeAwGenerationError> {
        self.realized = None;
        evla_aw_workspace_bytes(request)?;
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
        // CASA's copy back from the FFT image excludes the final row and column.
        // Retain these pre-transform edges in the no-longer-used Jones workspace.
        let edges = &mut jones[..4 * request.size];
        save_edges(imaging, request.size, &mut edges[..2 * request.size]);
        save_edges(weight, request.size, &mut edges[2 * request.size..]);
        transform(fft, request.size, imaging);
        transform(fft, request.size, weight);
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
    use super::*;

    #[test]
    fn t52_paired_workspace_is_admitted_reused_and_deterministic() {
        let surface = casa_imaging_model::EvlaDishSurface::new(
            (0..=125)
                .map(|i| {
                    let r = i as f64 / 10.0;
                    [r, r * r / 36.0, r / 18.0]
                })
                .collect(),
        )
        .unwrap();
        let model = EvlaApertureModel::new(surface);
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
        let bound = request.generation_workspace_bytes().unwrap();
        assert!(matches!(
            EvlaAwWorkspace::new(request, bound - 1),
            Err(NativeAwGenerationError::WorkspaceMismatch)
        ));
        let mut workspace = EvlaAwWorkspace::new(request, bound).unwrap();
        let pointers = (
            workspace.jones.as_ptr(),
            workspace.imaging.as_ptr(),
            workspace.weight.as_ptr(),
        );
        let pair = workspace.generate(&model, request).unwrap();
        let first = (pair.imaging.values.to_vec(), pair.weight.values.to_vec());
        for _ in 0..3 {
            let pair = workspace.generate(&model, request).unwrap();
            assert_eq!(pair.imaging.values, first.0);
            assert_eq!(pair.weight.values, first.1);
            assert_eq!(workspace.resident_bytes(), bound);
            assert_eq!(
                (
                    workspace.jones.as_ptr(),
                    workspace.imaging.as_ptr(),
                    workspace.weight.as_ptr()
                ),
                pointers
            );
        }
        let mut fresh = EvlaAwWorkspace::new(request, bound).unwrap();
        let repeated = fresh.generate(&model, request).unwrap();
        assert_eq!(repeated.imaging.values, first.0);
        assert_eq!(repeated.weight.values, first.1);
        let mut invalid = request;
        invalid.size *= 2;
        assert!(workspace.generate(&model, invalid).is_err());
        assert!(
            workspace.pair().is_none(),
            "failed generation must not expose the preceding cell"
        );
    }

    #[test]
    fn t52_analytic_aperture_free_pair_has_unit_sampled_area() {
        let model = EvlaApertureModel::new(
            casa_imaging_model::EvlaDishSurface::new(vec![
                [0.0, 0.0, 0.0],
                [6.25, 1.0, 0.32],
                [12.5, 4.0, 0.64],
            ])
            .unwrap(),
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
        let mut workspace =
            EvlaAwWorkspace::new(request, request.generation_workspace_bytes().unwrap()).unwrap();
        let pair = workspace.generate(&model, request).unwrap();
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
                "sampled-area normalization {area}"
            );
        }
    }

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
        let mut workspace =
            EvlaAwWorkspace::new(request, evla_aw_workspace_bytes(request).unwrap()).unwrap();
        let result = workspace.generate(&model, request).unwrap();
        for (role, plane) in [("imaging", result.imaging), ("weight", result.weight)] {
            let bytes = std::fs::read(root.join(format!("{role}.bin"))).unwrap();
            assert_eq!(
                plane.size,
                specification[role]["size"].as_u64().unwrap() as usize
            );
            assert_eq!(
                plane.support,
                specification[role]["support"].as_u64().unwrap() as usize
            );
            assert_frozen_plane(role, &plane, &bytes);
        }
    }

    #[test]
    #[ignore = "24 frozen EVLA cells; explicit exported catalog, surface and image geometry required"]
    fn t52_evla_paired_cohort_matches_frozen_casa() {
        let root =
            std::path::PathBuf::from(std::env::var_os("CASA_RS_T52_CATALOG_FIXTURE").unwrap());
        let sky_cell: f64 = std::env::var("CASA_RS_T52_WORKING_SKY_CELL_RAD")
            .unwrap()
            .parse()
            .unwrap();
        let working_size: usize = std::env::var("CASA_RS_T52_WORKING_SIZE")
            .unwrap()
            .parse()
            .unwrap();
        let surface =
            std::fs::read_to_string(std::env::var("CASA_RS_T52_SURFACE").unwrap()).unwrap();
        let model = EvlaApertureModel::new(
            casa_imaging_model::EvlaDishSurface::from_surface_text(&surface).unwrap(),
        );
        let catalog = std::fs::read_to_string(root.join("catalog.tsv")).unwrap();
        let mut workspace = None;
        let mut tested = 0;
        for line in catalog.lines() {
            let fields: Vec<_> = line.split('\t').collect();
            let name = fields[0];
            let indices: Vec<_> = name.split('_').collect();
            let w_index: usize = indices[5].parse().unwrap();
            if ![0, 18, 31].contains(&w_index) {
                continue;
            }
            let f = |i: usize| fields[i].parse::<f64>().unwrap();
            let u = |i: usize| fields[i].parse::<usize>().unwrap();
            let request = EvlaAwCellRequest {
                size: working_size,
                sky_increment_rad: [-sky_cell, sky_cell],
                frequency_hz: f(1),
                conjugate_frequency_hz: f(7),
                w_wavelengths: f(2),
                parallactic_angle_rad: f(5).to_radians(),
                mueller: u(4),
                oversampling: u(15),
                prolate_spheroidal: false,
                aperture: true,
            };
            let workspace = workspace.get_or_insert_with(|| {
                EvlaAwWorkspace::new(request, evla_aw_workspace_bytes(request).unwrap()).unwrap()
            });
            let pair = workspace.generate(&model, request).unwrap();
            for (role, plane, shape_index, support_index) in [
                ("imaging", pair.imaging, 16, 13),
                ("weight", pair.weight, 23, 20),
            ] {
                let label = format!("{name}.{role}");
                assert_eq!(plane.size, u(shape_index), "{label} shape");
                assert_eq!(plane.support, u(support_index), "{label} support");
                let bytes = std::fs::read(root.join(format!("{label}.bin"))).unwrap();
                assert_frozen_plane(&label, &plane, &bytes);
            }
            tested += 1;
        }
        assert_eq!(
            tested, 24,
            "four frequencies x two circular hands x three W coordinates"
        );
    }

    fn assert_frozen_plane(role: &str, plane: &NativeAwPlane<'_>, bytes: &[u8]) {
        eprintln!("T52 {role} size={} support={}", plane.size, plane.support);
        assert_eq!(bytes.len(), plane.values.len() * 8);
        let mut error_squared = 0.0_f64;
        let mut reference_squared = 0.0_f64;
        let mut native_squared = 0.0_f64;
        let mut native_sum = num_complex::Complex64::default();
        let mut reference_sum = num_complex::Complex64::default();
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
            native_squared += f64::from(value.norm_sqr());
            native_sum += num_complex::Complex64::new(f64::from(value.re), f64::from(value.im));
            reference_sum +=
                num_complex::Complex64::new(f64::from(reference.re), f64::from(reference.im));
            maximum_error = maximum_error.max(error);
            reference_peak = reference_peak.max(magnitude);
        }
        let relative_l2 = (error_squared / reference_squared).sqrt();
        let peak_scaled_max = maximum_error / reference_peak;
        let relative_energy = (native_squared / reference_squared - 1.0).abs();
        let relative_normalization = (native_sum - reference_sum).norm() / reference_sum.norm();
        eprintln!(
            "T52 {role} relative_l2={relative_l2:.12e} peak_scaled_max={peak_scaled_max:.12e} relative_energy={relative_energy:.12e} relative_normalization={relative_normalization:.12e}"
        );
        // Independent f32 FFT implementations may differ in roundoff, but
        // no fitted scale, phase, support, or science tolerance is allowed.
        assert!(relative_l2 <= 1e-5, "{role} relative L2 {relative_l2}");
        assert!(
            relative_energy <= 1e-5,
            "{role} relative energy {relative_energy}"
        );
        assert!(
            relative_normalization <= 1e-5,
            "{role} normalization {relative_normalization}"
        );
        assert!(
            peak_scaled_max <= 1e-5,
            "{role} element error {peak_scaled_max}"
        );
    }
}

// SPDX-License-Identifier: LGPL-3.0-or-later
//! FFT backend contracts, selection policy, validation, and benchmark metadata.
//!
//! This module is intentionally role-aware: dirty PSF/residual transforms have
//! different precision and parity obligations than model, degridding, or
//! restoration transforms.  Backend promotion should happen here only after the
//! candidate proves the same centered 2-D complex FFT semantics used by the
//! current RustFFT path.

use std::fmt;
use std::time::Duration;

use ndarray::Array2;
use num_complex::{Complex32, Complex64};

use crate::fft;

/// Imaging role that is asking for a 2-D complex FFT.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum FftUseCase {
    /// Dirty PSF and dirty/residual image formation.
    DirtyPsfResidual,
    /// Model-image FFTs used by prediction/degridding paths.
    ModelDegrid,
    /// Restoration or convolution-oriented image-domain work.
    Restoration,
    /// Explicit validation or benchmarking, outside normal imaging policy.
    Benchmark,
}

impl FftUseCase {
    /// Stable lowercase label for logs and benchmark bundles.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::DirtyPsfResidual => "dirty_psf_residual",
            Self::ModelDegrid => "model_degrid",
            Self::Restoration => "restoration",
            Self::Benchmark => "benchmark",
        }
    }
}

impl fmt::Display for FftUseCase {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

/// Scalar precision for the complex FFT.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum FftPrecision {
    /// Complex single precision.
    F32,
    /// Complex double precision.
    F64,
}

impl FftPrecision {
    /// Stable lowercase label for logs and benchmark bundles.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::F32 => "f32",
            Self::F64 => "f64",
        }
    }
}

impl fmt::Display for FftPrecision {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

/// Memory residency expected by the backend contract.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum FftPlacement {
    /// Input and output live in host memory.
    Host,
    /// Input and output live in an Apple GPU device buffer.
    AppleGpuDeviceBuffer,
}

impl FftPlacement {
    /// Stable lowercase label for logs and benchmark bundles.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Host => "host",
            Self::AppleGpuDeviceBuffer => "apple_gpu_device_buffer",
        }
    }
}

impl fmt::Display for FftPlacement {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

/// Requested backend policy or concrete backend.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum FftBackendChoice {
    /// Let casa-rs select the backend for the role, precision, shape, and host.
    Auto,
    /// Current pure RustFFT implementation.
    RustFft,
    /// Apple Accelerate/vDSP CPU FFT implementation.
    Accelerate,
    /// Metal-backed VkFFT implementation candidate.
    MetalVkFft,
    /// Apple MPSGraph FFT implementation candidate.
    MetalMpsGraph,
    /// Local-only FFTW benchmark hook, excluded from default distribution.
    FftwLocalBench,
}

impl FftBackendChoice {
    /// Stable lowercase label for logs and benchmark bundles.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Auto => "auto",
            Self::RustFft => "rustfft",
            Self::Accelerate => "accelerate",
            Self::MetalVkFft => "metal-vkfft",
            Self::MetalMpsGraph => "metal-mpsgraph",
            Self::FftwLocalBench => "fftw-local-bench",
        }
    }
}

impl fmt::Display for FftBackendChoice {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

impl std::str::FromStr for FftBackendChoice {
    type Err = String;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value {
            "auto" => Ok(Self::Auto),
            "rustfft" | "rust-fft" => Ok(Self::RustFft),
            "accelerate" | "vdsp" => Ok(Self::Accelerate),
            "metal-vkfft" | "metal" | "vkfft" => Ok(Self::MetalVkFft),
            "metal-mpsgraph" | "mpsgraph" | "mps-graph" => Ok(Self::MetalMpsGraph),
            "fftw-local-bench" | "fftw-local" | "fftw" => Ok(Self::FftwLocalBench),
            _ => Err(format!("unknown FFT backend '{value}'")),
        }
    }
}

/// Direction of the complex FFT.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum FftDirection {
    /// Forward image-to-frequency transform.
    Forward,
    /// Inverse frequency-to-image transform.
    Inverse,
}

impl FftDirection {
    /// Stable lowercase label for logs and benchmark bundles.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Forward => "forward",
            Self::Inverse => "inverse",
        }
    }
}

impl fmt::Display for FftDirection {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

/// Frequency/image origin convention for the transform.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum FftShift {
    /// No origin shift is performed around the FFT.
    None,
    /// CASA-compatible centered 2-D transform using ifftshift/fftshift.
    CenteredCasaCompatible,
}

impl FftShift {
    /// Stable lowercase label for logs and benchmark bundles.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::None => "none",
            Self::CenteredCasaCompatible => "centered_casa_compatible",
        }
    }
}

impl fmt::Display for FftShift {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

/// Scaling convention for the transform.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum FftNormalization {
    /// Do not scale the transform result.
    None,
    /// Scale inverse transforms by `1 / (rows * columns)`.
    ScaleInverseByElementCount,
}

impl FftNormalization {
    /// Stable lowercase label for logs and benchmark bundles.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::None => "none",
            Self::ScaleInverseByElementCount => "scale_inverse_by_element_count",
        }
    }
}

impl fmt::Display for FftNormalization {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

/// Shape of a batched 2-D complex FFT.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct Fft2Shape {
    /// Number of rows in each 2-D plane.
    pub rows: usize,
    /// Number of columns in each 2-D plane.
    pub columns: usize,
    /// Number of independent 2-D transforms in the batch.
    pub batch: usize,
}

impl Fft2Shape {
    /// Create a single-plane 2-D shape.
    pub fn single(rows: usize, columns: usize) -> Self {
        Self {
            rows,
            columns,
            batch: 1,
        }
    }

    /// Total complex elements across the batch.
    pub fn element_count(self) -> usize {
        self.rows * self.columns * self.batch
    }

    /// Whether both 2-D axes are powers of two.
    pub fn is_power_of_two_2d(self) -> bool {
        self.rows.is_power_of_two() && self.columns.is_power_of_two()
    }
}

/// Complete role-aware 2-D complex FFT request.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct Fft2Spec {
    /// Shape of the 2-D transform.
    pub shape: Fft2Shape,
    /// Complex scalar precision.
    pub precision: FftPrecision,
    /// Transform direction.
    pub direction: FftDirection,
    /// Shift convention.
    pub shift: FftShift,
    /// Scaling convention.
    pub normalization: FftNormalization,
    /// Expected memory residency.
    pub placement: FftPlacement,
    /// Imaging role.
    pub use_case: FftUseCase,
    /// Requested backend or policy.
    pub backend_choice: FftBackendChoice,
}

impl Fft2Spec {
    /// Build the centered C2C contract used by current imaging helpers.
    pub fn centered_c2c(
        rows: usize,
        columns: usize,
        precision: FftPrecision,
        direction: FftDirection,
        use_case: FftUseCase,
        backend_choice: FftBackendChoice,
    ) -> Self {
        Self {
            shape: Fft2Shape::single(rows, columns),
            precision,
            direction,
            shift: FftShift::CenteredCasaCompatible,
            normalization: if direction == FftDirection::Inverse {
                FftNormalization::ScaleInverseByElementCount
            } else {
                FftNormalization::None
            },
            placement: FftPlacement::Host,
            use_case,
            backend_choice,
        }
    }

    /// Build the centered C2C contract for a real batch of independent planes.
    pub fn centered_c2c_batch(
        rows: usize,
        columns: usize,
        batch: usize,
        precision: FftPrecision,
        direction: FftDirection,
        use_case: FftUseCase,
        backend_choice: FftBackendChoice,
    ) -> Self {
        Self {
            shape: Fft2Shape {
                rows,
                columns,
                batch,
            },
            precision,
            direction,
            shift: FftShift::CenteredCasaCompatible,
            normalization: if direction == FftDirection::Inverse {
                FftNormalization::ScaleInverseByElementCount
            } else {
                FftNormalization::None
            },
            placement: FftPlacement::Host,
            use_case,
            backend_choice,
        }
    }
}

/// Backend selected for a transform request.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FftBackendSelection {
    /// Requested backend or policy.
    pub requested_backend: FftBackendChoice,
    /// Concrete backend selected for execution.
    pub selected_backend: FftBackendChoice,
    /// Whether the requested backend was usable for the exact request.
    pub requested_backend_supported: bool,
    /// Whether execution falls back to a different backend.
    pub fallback_used: bool,
    /// Human-readable policy reason.
    pub reason: &'static str,
}

/// Declared support for one backend and one exact request.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FftBackendCapability {
    /// Backend being described.
    pub backend: FftBackendChoice,
    /// Whether the backend is implemented in this build.
    pub implemented: bool,
    /// Whether this exact request is supported.
    pub supported: bool,
    /// Human-readable support reason.
    pub reason: &'static str,
}

/// Per-transform timing breakdown for total-wall-time decisions.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FftTiming {
    /// Transform request.
    pub spec: Fft2Spec,
    /// Backend selection.
    pub selection: FftBackendSelection,
    /// Whether all needed plans were already cached before execution.
    pub plan_cache_hit: bool,
    /// Plan creation or lookup time measured by the adapter.
    pub plan: Duration,
    /// Host packing, shifting, and unpacking time.
    pub pack: Duration,
    /// Host-to-device transfer time.
    pub transfer_to_device: Duration,
    /// Backend execution time as observed by the host.
    pub exec: Duration,
    /// Device execution time, when the backend reports it separately.
    pub device_exec: Duration,
    /// Device-to-host transfer time.
    pub transfer_from_device: Duration,
    /// Host synchronization time.
    pub sync: Duration,
    /// Total wall time for this adapter call.
    pub total: Duration,
}

impl FftTiming {
    /// Create a zeroed timing record for a request and selection.
    pub fn new(spec: Fft2Spec, selection: FftBackendSelection) -> Self {
        Self {
            spec,
            selection,
            plan_cache_hit: false,
            plan: Duration::ZERO,
            pack: Duration::ZERO,
            transfer_to_device: Duration::ZERO,
            exec: Duration::ZERO,
            device_exec: Duration::ZERO,
            transfer_from_device: Duration::ZERO,
            sync: Duration::ZERO,
            total: Duration::ZERO,
        }
    }
}

/// Validation result for one backend, precision, shape, and role.
#[derive(Debug, Clone)]
pub struct FftValidationReport {
    /// Transform request used for validation.
    pub spec: Fft2Spec,
    /// Backend selected by policy.
    pub selection: FftBackendSelection,
    /// Backend capability declaration.
    pub capability: FftBackendCapability,
    /// Whether all semantic checks passed.
    pub passed: bool,
    /// Maximum absolute forward-transform error against RustFFT.
    pub forward_max_abs_error: Option<f64>,
    /// Maximum absolute inverse-transform error against RustFFT.
    pub inverse_max_abs_error: Option<f64>,
    /// Maximum absolute round-trip error against the input fixture.
    pub round_trip_max_abs_error: Option<f64>,
    /// Validation tolerance for this precision.
    pub tolerance: f64,
    /// Candidate backend timing for the validation forward transform.
    pub timing: FftTiming,
}

/// Select a concrete backend for an exact FFT request.
pub fn select_fft_backend(spec: Fft2Spec) -> FftBackendSelection {
    match spec.backend_choice {
        FftBackendChoice::Auto => select_auto_fft_backend(spec),
        FftBackendChoice::RustFft => FftBackendSelection {
            requested_backend: spec.backend_choice,
            selected_backend: FftBackendChoice::RustFft,
            requested_backend_supported: true,
            fallback_used: false,
            reason: "rustfft_requested",
        },
        FftBackendChoice::Accelerate => {
            let capability = fft_backend_capability(FftBackendChoice::Accelerate, spec);
            FftBackendSelection {
                requested_backend: spec.backend_choice,
                selected_backend: FftBackendChoice::Accelerate,
                requested_backend_supported: capability.supported,
                fallback_used: false,
                reason: capability.reason,
            }
        }
        FftBackendChoice::MetalVkFft => FftBackendSelection {
            requested_backend: spec.backend_choice,
            selected_backend: FftBackendChoice::MetalVkFft,
            requested_backend_supported: false,
            fallback_used: false,
            reason: metal_vkfft_unavailable_reason(spec),
        },
        FftBackendChoice::MetalMpsGraph => {
            let capability = fft_backend_capability(FftBackendChoice::MetalMpsGraph, spec);
            FftBackendSelection {
                requested_backend: spec.backend_choice,
                selected_backend: FftBackendChoice::MetalMpsGraph,
                requested_backend_supported: capability.supported,
                fallback_used: false,
                reason: capability.reason,
            }
        }
        FftBackendChoice::FftwLocalBench => FftBackendSelection {
            requested_backend: spec.backend_choice,
            selected_backend: FftBackendChoice::FftwLocalBench,
            requested_backend_supported: false,
            fallback_used: false,
            reason: "fftw_local_benchmark_hook_requires_external_command",
        },
    }
}

/// Describe whether a backend supports an exact FFT request.
pub fn fft_backend_capability(backend: FftBackendChoice, spec: Fft2Spec) -> FftBackendCapability {
    match backend {
        FftBackendChoice::Auto => {
            let selection = select_auto_fft_backend(spec);
            fft_backend_capability(selection.selected_backend, spec)
        }
        FftBackendChoice::RustFft => FftBackendCapability {
            backend,
            implemented: true,
            supported: spec.placement == FftPlacement::Host && spec.shape.batch > 0,
            reason: if spec.placement == FftPlacement::Host && spec.shape.batch > 0 {
                if spec.shape.batch == 1 {
                    "rustfft_host_single_plane_supported"
                } else {
                    "rustfft_host_batch_loop_supported"
                }
            } else {
                "rustfft_adapter_requires_host_non_empty_batch"
            },
        },
        FftBackendChoice::Accelerate => accelerate_capability(backend, spec),
        FftBackendChoice::MetalVkFft => FftBackendCapability {
            backend,
            implemented: false,
            supported: false,
            reason: metal_vkfft_unavailable_reason(spec),
        },
        FftBackendChoice::MetalMpsGraph => metal_mpsgraph_capability(backend, spec),
        FftBackendChoice::FftwLocalBench => FftBackendCapability {
            backend,
            implemented: false,
            supported: false,
            reason: "fftw_local_benchmark_hook_requires_external_command",
        },
    }
}

/// Validate a backend against the current RustFFT centered C2C semantics.
pub fn validate_fft_backend(spec: Fft2Spec) -> FftValidationReport {
    let selection = select_fft_backend(spec);
    let capability = fft_backend_capability(selection.selected_backend, spec);
    let tolerance = validation_tolerance(spec.precision);
    let unsupported_timing = FftTiming::new(spec, selection);
    if !capability.supported {
        return FftValidationReport {
            spec,
            selection,
            capability,
            passed: false,
            forward_max_abs_error: None,
            inverse_max_abs_error: None,
            round_trip_max_abs_error: None,
            tolerance,
            timing: unsupported_timing,
        };
    }

    match spec.precision {
        FftPrecision::F32 => validate_fft_backend_f32(spec, selection, capability, tolerance),
        FftPrecision::F64 => validate_fft_backend_f64(spec, selection, capability, tolerance),
    }
}

/// Whether an external FFTW benchmark command has been configured locally.
pub fn fftw_local_bench_command() -> Option<String> {
    std::env::var("CASA_RS_FFTW_BENCH_CMD").ok()
}

/// Ratio of wall-clock time to measured I/O time.
pub fn wall_to_io_ratio(wall: Duration, io: Duration) -> Option<f64> {
    let io_seconds = io.as_secs_f64();
    if io_seconds > 0.0 {
        Some(wall.as_secs_f64() / io_seconds)
    } else {
        None
    }
}

fn select_auto_fft_backend(spec: Fft2Spec) -> FftBackendSelection {
    if spec.placement == FftPlacement::AppleGpuDeviceBuffer {
        return FftBackendSelection {
            requested_backend: spec.backend_choice,
            selected_backend: FftBackendChoice::MetalVkFft,
            requested_backend_supported: false,
            fallback_used: false,
            reason: metal_vkfft_unavailable_reason(spec),
        };
    }

    if cfg!(target_os = "macos")
        && spec.precision == FftPrecision::F32
        && spec.use_case == FftUseCase::DirtyPsfResidual
    {
        let metal_capability = if cfg!(all(target_os = "macos", not(coverage))) {
            Some(fft_backend_capability(
                FftBackendChoice::MetalMpsGraph,
                spec,
            ))
        } else {
            None
        };
        let metal_is_likely_profitable = spec.shape.batch > 1
            || !spec.shape.is_power_of_two_2d()
            || spec.shape.rows.saturating_mul(spec.shape.columns) >= 2048 * 2048;
        if metal_is_likely_profitable
            && let Some(capability) = metal_capability
            && capability.supported
        {
            return FftBackendSelection {
                requested_backend: spec.backend_choice,
                selected_backend: FftBackendChoice::MetalMpsGraph,
                requested_backend_supported: true,
                fallback_used: false,
                reason: capability.reason,
            };
        }
        let capability = fft_backend_capability(FftBackendChoice::Accelerate, spec);
        if capability.supported {
            return FftBackendSelection {
                requested_backend: spec.backend_choice,
                selected_backend: FftBackendChoice::Accelerate,
                requested_backend_supported: true,
                fallback_used: false,
                reason: capability.reason,
            };
        }
        if let Some(metal_capability) = metal_capability
            && metal_capability.supported
        {
            return FftBackendSelection {
                requested_backend: spec.backend_choice,
                selected_backend: FftBackendChoice::MetalMpsGraph,
                requested_backend_supported: true,
                fallback_used: false,
                reason: metal_capability.reason,
            };
        }
        return FftBackendSelection {
            requested_backend: spec.backend_choice,
            selected_backend: FftBackendChoice::RustFft,
            requested_backend_supported: true,
            fallback_used: true,
            reason: capability.reason,
        };
    }

    if cfg!(all(target_os = "macos", not(coverage))) && spec.precision == FftPrecision::F32 {
        let capability = fft_backend_capability(FftBackendChoice::MetalMpsGraph, spec);
        if capability.supported {
            return FftBackendSelection {
                requested_backend: spec.backend_choice,
                selected_backend: FftBackendChoice::MetalMpsGraph,
                requested_backend_supported: true,
                fallback_used: false,
                reason: capability.reason,
            };
        }
        return FftBackendSelection {
            requested_backend: spec.backend_choice,
            selected_backend: FftBackendChoice::RustFft,
            requested_backend_supported: true,
            fallback_used: true,
            reason: capability.reason,
        };
    }

    if cfg!(target_os = "macos")
        && spec.precision == FftPrecision::F64
        && spec.use_case == FftUseCase::DirtyPsfResidual
    {
        return FftBackendSelection {
            requested_backend: spec.backend_choice,
            selected_backend: FftBackendChoice::RustFft,
            requested_backend_supported: true,
            fallback_used: true,
            reason: "apple_metal_f64_unavailable_for_dirty_role_using_rustfft",
        };
    }

    if cfg!(target_os = "macos") && spec.precision == FftPrecision::F64 {
        return FftBackendSelection {
            requested_backend: spec.backend_choice,
            selected_backend: FftBackendChoice::RustFft,
            requested_backend_supported: true,
            fallback_used: true,
            reason: "apple_metal_f64_unavailable_using_rustfft",
        };
    }

    FftBackendSelection {
        requested_backend: spec.backend_choice,
        selected_backend: FftBackendChoice::RustFft,
        requested_backend_supported: true,
        fallback_used: false,
        reason: "rustfft_default",
    }
}

fn validation_tolerance(precision: FftPrecision) -> f64 {
    match precision {
        FftPrecision::F32 => 3.0e-4,
        FftPrecision::F64 => 1.0e-10,
    }
}

fn validate_fft_backend_f32(
    spec: Fft2Spec,
    selection: FftBackendSelection,
    capability: FftBackendCapability,
    tolerance: f64,
) -> FftValidationReport {
    let input = fixture_f32(spec.shape.rows, spec.shape.columns);
    let reference_forward = fft::centered_fft2_timed_with_backend(
        &input,
        FftUseCase::Benchmark,
        FftBackendChoice::RustFft,
    )
    .0;
    let reference_inverse = fft::centered_ifft2_timed_with_backend(
        &reference_forward,
        FftUseCase::Benchmark,
        FftBackendChoice::RustFft,
    )
    .0;
    let (candidate_forward, timing) = transform_f32(
        selection.selected_backend,
        &input,
        FftDirection::Forward,
        spec.use_case,
    )
    .expect("capability should match f32 transform support");
    let (candidate_inverse, _) = transform_f32(
        selection.selected_backend,
        &reference_forward,
        FftDirection::Inverse,
        spec.use_case,
    )
    .expect("capability should match f32 inverse transform support");
    let (candidate_round_trip, _) = transform_f32(
        selection.selected_backend,
        &candidate_forward,
        FftDirection::Inverse,
        spec.use_case,
    )
    .expect("capability should match f32 round-trip transform support");
    let forward_max_abs_error = max_abs_diff_f32(&reference_forward, &candidate_forward);
    let inverse_max_abs_error = max_abs_diff_f32(&reference_inverse, &candidate_inverse);
    let round_trip_max_abs_error = max_abs_diff_f32(&input, &candidate_round_trip);
    let forward_tolerance = scaled_f32_tolerance(tolerance, &reference_forward);
    let inverse_tolerance = scaled_f32_tolerance(tolerance, &reference_inverse);
    let round_trip_tolerance = scaled_f32_tolerance(tolerance, &input);
    let report_tolerance = forward_tolerance
        .max(inverse_tolerance)
        .max(round_trip_tolerance);
    let passed = forward_max_abs_error <= forward_tolerance
        && inverse_max_abs_error <= inverse_tolerance
        && round_trip_max_abs_error <= round_trip_tolerance;

    FftValidationReport {
        spec,
        selection,
        capability,
        passed,
        forward_max_abs_error: Some(forward_max_abs_error),
        inverse_max_abs_error: Some(inverse_max_abs_error),
        round_trip_max_abs_error: Some(round_trip_max_abs_error),
        tolerance: report_tolerance,
        timing,
    }
}

fn validate_fft_backend_f64(
    spec: Fft2Spec,
    selection: FftBackendSelection,
    capability: FftBackendCapability,
    tolerance: f64,
) -> FftValidationReport {
    let input = fixture_f64(spec.shape.rows, spec.shape.columns);
    let reference_forward = fft::centered_fft2_f64_timed_with_backend(
        &input,
        FftUseCase::Benchmark,
        FftBackendChoice::RustFft,
    )
    .0;
    let reference_inverse = fft::centered_ifft2_f64_timed_with_backend(
        &reference_forward,
        FftUseCase::Benchmark,
        FftBackendChoice::RustFft,
    )
    .0;
    let (candidate_forward, timing) = transform_f64(
        selection.selected_backend,
        &input,
        FftDirection::Forward,
        spec.use_case,
    )
    .expect("capability should match f64 transform support");
    let (candidate_inverse, _) = transform_f64(
        selection.selected_backend,
        &reference_forward,
        FftDirection::Inverse,
        spec.use_case,
    )
    .expect("capability should match f64 inverse transform support");
    let (candidate_round_trip, _) = transform_f64(
        selection.selected_backend,
        &candidate_forward,
        FftDirection::Inverse,
        spec.use_case,
    )
    .expect("capability should match f64 round-trip transform support");
    let forward_max_abs_error = max_abs_diff_f64(&reference_forward, &candidate_forward);
    let inverse_max_abs_error = max_abs_diff_f64(&reference_inverse, &candidate_inverse);
    let round_trip_max_abs_error = max_abs_diff_f64(&input, &candidate_round_trip);
    let passed = forward_max_abs_error <= tolerance
        && inverse_max_abs_error <= tolerance
        && round_trip_max_abs_error <= tolerance;

    FftValidationReport {
        spec,
        selection,
        capability,
        passed,
        forward_max_abs_error: Some(forward_max_abs_error),
        inverse_max_abs_error: Some(inverse_max_abs_error),
        round_trip_max_abs_error: Some(round_trip_max_abs_error),
        tolerance,
        timing,
    }
}

pub(crate) fn transform_f32(
    backend: FftBackendChoice,
    input: &Array2<Complex32>,
    direction: FftDirection,
    use_case: FftUseCase,
) -> Result<(Array2<Complex32>, FftTiming), &'static str> {
    match backend {
        FftBackendChoice::RustFft => match direction {
            FftDirection::Forward => Ok(fft::centered_fft2_timed_with_backend(
                input,
                use_case,
                FftBackendChoice::RustFft,
            )),
            FftDirection::Inverse => Ok(fft::centered_ifft2_timed_with_backend(
                input,
                use_case,
                FftBackendChoice::RustFft,
            )),
        },
        FftBackendChoice::Accelerate => accelerate_transform_f32(input, direction, use_case),
        FftBackendChoice::MetalMpsGraph => mpsgraph_transform_f32(input, direction, use_case),
        FftBackendChoice::Auto
        | FftBackendChoice::MetalVkFft
        | FftBackendChoice::FftwLocalBench => {
            Err("backend_is_not_a_concrete_implemented_f32_transform")
        }
    }
}

pub(crate) fn transform_f64(
    backend: FftBackendChoice,
    input: &Array2<Complex64>,
    direction: FftDirection,
    use_case: FftUseCase,
) -> Result<(Array2<Complex64>, FftTiming), &'static str> {
    match backend {
        FftBackendChoice::RustFft => match direction {
            FftDirection::Forward => Ok(fft::centered_fft2_f64_timed_with_backend(
                input,
                use_case,
                FftBackendChoice::RustFft,
            )),
            FftDirection::Inverse => Ok(fft::centered_ifft2_f64_timed_with_backend(
                input,
                use_case,
                FftBackendChoice::RustFft,
            )),
        },
        FftBackendChoice::Accelerate => accelerate_transform_f64(input, direction, use_case),
        FftBackendChoice::Auto
        | FftBackendChoice::MetalVkFft
        | FftBackendChoice::MetalMpsGraph
        | FftBackendChoice::FftwLocalBench => {
            Err("backend_is_not_a_concrete_implemented_f64_transform")
        }
    }
}

fn metal_vkfft_unavailable_reason(spec: Fft2Spec) -> &'static str {
    if spec.precision == FftPrecision::F64 && cfg!(target_os = "macos") {
        return "apple_metal_f64_rejected_double2_unavailable";
    }
    "metal_vkfft_adapter_not_integrated"
}

fn metal_mpsgraph_capability(backend: FftBackendChoice, spec: Fft2Spec) -> FftBackendCapability {
    if spec.precision != FftPrecision::F32 {
        return FftBackendCapability {
            backend,
            implemented: cfg!(all(target_os = "macos", not(coverage))),
            supported: false,
            reason: "mpsgraph_fft_supports_complex_f32_not_f64",
        };
    }
    if spec.placement != FftPlacement::Host {
        return FftBackendCapability {
            backend,
            implemented: cfg!(all(target_os = "macos", not(coverage))),
            supported: false,
            reason: "mpsgraph_adapter_currently_uses_host_staging",
        };
    }
    if spec.shift != FftShift::CenteredCasaCompatible {
        return FftBackendCapability {
            backend,
            implemented: cfg!(all(target_os = "macos", not(coverage))),
            supported: false,
            reason: "mpsgraph_adapter_validates_centered_casa_fft_only",
        };
    }
    if spec.shape.rows == 0 || spec.shape.columns == 0 {
        return FftBackendCapability {
            backend,
            implemented: cfg!(all(target_os = "macos", not(coverage))),
            supported: false,
            reason: "mpsgraph_fft_requires_non_empty_shape",
        };
    }

    metal_mpsgraph_platform_capability(backend)
}

#[cfg(all(target_os = "macos", not(coverage)))]
fn metal_mpsgraph_platform_capability(backend: FftBackendChoice) -> FftBackendCapability {
    if crate::apple_fft::mpsgraph_f32_available() {
        FftBackendCapability {
            backend,
            implemented: true,
            supported: true,
            reason: "metal_mpsgraph_complex_f32_host_batch_supported",
        }
    } else {
        FftBackendCapability {
            backend,
            implemented: true,
            supported: false,
            reason: "metal_mpsgraph_no_default_device",
        }
    }
}

#[cfg(not(all(target_os = "macos", not(coverage))))]
fn metal_mpsgraph_platform_capability(backend: FftBackendChoice) -> FftBackendCapability {
    FftBackendCapability {
        backend,
        implemented: false,
        supported: false,
        reason: "metal_mpsgraph_available_on_macos_only",
    }
}

#[cfg(all(target_os = "macos", not(coverage)))]
fn mpsgraph_transform_f32(
    input: &Array2<Complex32>,
    direction: FftDirection,
    use_case: FftUseCase,
) -> Result<(Array2<Complex32>, FftTiming), &'static str> {
    let spec = Fft2Spec::centered_c2c(
        input.shape()[0],
        input.shape()[1],
        FftPrecision::F32,
        direction,
        use_case,
        FftBackendChoice::MetalMpsGraph,
    );
    let capability = metal_mpsgraph_capability(FftBackendChoice::MetalMpsGraph, spec);
    if !capability.supported {
        return Err(capability.reason);
    }
    let selection = FftBackendSelection {
        requested_backend: FftBackendChoice::MetalMpsGraph,
        selected_backend: FftBackendChoice::MetalMpsGraph,
        requested_backend_supported: true,
        fallback_used: false,
        reason: capability.reason,
    };
    crate::apple_fft::centered_transform_f32(input, spec, selection)
}

#[cfg(not(all(target_os = "macos", not(coverage))))]
fn mpsgraph_transform_f32(
    _input: &Array2<Complex32>,
    _direction: FftDirection,
    _use_case: FftUseCase,
) -> Result<(Array2<Complex32>, FftTiming), &'static str> {
    Err("metal_mpsgraph_available_on_macos_only")
}

fn fixture_f32(rows: usize, columns: usize) -> Array2<Complex32> {
    Array2::from_shape_fn((rows, columns), |(row, column)| {
        let seed = deterministic_seed(row, column);
        let re = ((seed & 0xffff) as f32 / 32768.0) - 1.0;
        let im = (((seed >> 16) & 0xffff) as f32 / 32768.0) - 1.0;
        Complex32::new(re + row as f32 * 0.03125, im - column as f32 * 0.015625)
    })
}

fn fixture_f64(rows: usize, columns: usize) -> Array2<Complex64> {
    Array2::from_shape_fn((rows, columns), |(row, column)| {
        let seed = deterministic_seed(row, column);
        let re = ((seed & 0xffff) as f64 / 32768.0) - 1.0;
        let im = (((seed >> 16) & 0xffff) as f64 / 32768.0) - 1.0;
        Complex64::new(re + row as f64 * 0.03125, im - column as f64 * 0.015625)
    })
}

fn deterministic_seed(row: usize, column: usize) -> u64 {
    let mut value = (row as u64).wrapping_mul(0x9e37_79b9_7f4a_7c15);
    value ^= (column as u64).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    value ^= value >> 30;
    value = value.wrapping_mul(0xbf58_476d_1ce4_e5b9);
    value ^= value >> 27;
    value = value.wrapping_mul(0x94d0_49bb_1331_11eb);
    value ^ (value >> 31)
}

fn max_abs_diff_f32(expected: &Array2<Complex32>, actual: &Array2<Complex32>) -> f64 {
    expected
        .iter()
        .zip(actual.iter())
        .map(|(expected, actual)| (*expected - *actual).norm() as f64)
        .fold(0.0, f64::max)
}

fn scaled_f32_tolerance(base_tolerance: f64, reference: &Array2<Complex32>) -> f64 {
    let reference_scale = reference
        .iter()
        .map(|value| value.norm() as f64)
        .fold(1.0, f64::max);
    base_tolerance.max(reference_scale * 5.0e-6)
}

fn max_abs_diff_f64(expected: &Array2<Complex64>, actual: &Array2<Complex64>) -> f64 {
    expected
        .iter()
        .zip(actual.iter())
        .map(|(expected, actual)| (*expected - *actual).norm())
        .fold(0.0, f64::max)
}

fn accelerate_capability(backend: FftBackendChoice, spec: Fft2Spec) -> FftBackendCapability {
    if spec.placement != FftPlacement::Host {
        return FftBackendCapability {
            backend,
            implemented: cfg!(target_os = "macos"),
            supported: false,
            reason: "accelerate_requires_host_buffers",
        };
    }
    if spec.shift != FftShift::CenteredCasaCompatible {
        return FftBackendCapability {
            backend,
            implemented: cfg!(target_os = "macos"),
            supported: false,
            reason: "accelerate_adapter_validates_centered_casa_fft_only",
        };
    }
    if !spec.shape.is_power_of_two_2d() {
        return FftBackendCapability {
            backend,
            implemented: cfg!(target_os = "macos"),
            supported: false,
            reason: "accelerate_vdsp_fft_requires_power_of_two_axes",
        };
    }
    if cfg!(target_os = "macos") {
        FftBackendCapability {
            backend,
            implemented: true,
            supported: true,
            reason: if spec.shape.batch == 1 {
                "accelerate_vdsp_power_of_two_host_shape_supported"
            } else {
                "accelerate_vdsp_power_of_two_host_batch_loop_supported"
            },
        }
    } else {
        FftBackendCapability {
            backend,
            implemented: false,
            supported: false,
            reason: "accelerate_available_on_macos_only",
        }
    }
}

#[cfg(target_os = "macos")]
fn accelerate_transform_f32(
    input: &Array2<Complex32>,
    direction: FftDirection,
    use_case: FftUseCase,
) -> Result<(Array2<Complex32>, FftTiming), &'static str> {
    accelerate::transform_f32(input, direction, use_case)
}

#[cfg(not(target_os = "macos"))]
fn accelerate_transform_f32(
    _input: &Array2<Complex32>,
    _direction: FftDirection,
    _use_case: FftUseCase,
) -> Result<(Array2<Complex32>, FftTiming), &'static str> {
    Err("accelerate_available_on_macos_only")
}

#[cfg(target_os = "macos")]
fn accelerate_transform_f64(
    input: &Array2<Complex64>,
    direction: FftDirection,
    use_case: FftUseCase,
) -> Result<(Array2<Complex64>, FftTiming), &'static str> {
    accelerate::transform_f64(input, direction, use_case)
}

#[cfg(not(target_os = "macos"))]
fn accelerate_transform_f64(
    _input: &Array2<Complex64>,
    _direction: FftDirection,
    _use_case: FftUseCase,
) -> Result<(Array2<Complex64>, FftTiming), &'static str> {
    Err("accelerate_available_on_macos_only")
}

#[cfg(target_os = "macos")]
mod accelerate {
    use std::ffi::c_void;
    use std::os::raw::c_int;
    use std::time::Instant;

    use super::*;

    const FFT_FORWARD: c_int = 1;
    const FFT_INVERSE: c_int = -1;
    const FFT_RADIX2: c_int = 0;

    type FFTSetup = *mut c_void;
    type FFTSetupD = *mut c_void;

    #[repr(C)]
    struct DSPSplitComplex {
        realp: *mut f32,
        imagp: *mut f32,
    }

    #[repr(C)]
    struct DSPDoubleSplitComplex {
        realp: *mut f64,
        imagp: *mut f64,
    }

    #[link(name = "Accelerate", kind = "framework")]
    unsafe extern "C" {
        fn vDSP_create_fftsetup(log2n: usize, radix: c_int) -> FFTSetup;
        fn vDSP_destroy_fftsetup(setup: FFTSetup);
        fn vDSP_fft2d_zip(
            setup: FFTSetup,
            c: *const DSPSplitComplex,
            ic0: isize,
            ic1: isize,
            log2n0: usize,
            log2n1: usize,
            direction: c_int,
        );
        fn vDSP_create_fftsetupD(log2n: usize, radix: c_int) -> FFTSetupD;
        fn vDSP_destroy_fftsetupD(setup: FFTSetupD);
        fn vDSP_fft2d_zipD(
            setup: FFTSetupD,
            c: *const DSPDoubleSplitComplex,
            ic0: isize,
            ic1: isize,
            log2n0: usize,
            log2n1: usize,
            direction: c_int,
        );
    }

    pub(super) fn transform_f32(
        input: &Array2<Complex32>,
        direction: FftDirection,
        use_case: FftUseCase,
    ) -> Result<(Array2<Complex32>, FftTiming), &'static str> {
        let rows = input.shape()[0];
        let columns = input.shape()[1];
        if !rows.is_power_of_two() || !columns.is_power_of_two() {
            return Err("accelerate_vdsp_fft_requires_power_of_two_axes");
        }
        let spec = Fft2Spec::centered_c2c(
            rows,
            columns,
            FftPrecision::F32,
            direction,
            use_case,
            FftBackendChoice::Accelerate,
        );
        let selection = select_fft_backend(spec);
        let mut timing = FftTiming::new(spec, selection);
        let total_start = Instant::now();

        let pack_start = Instant::now();
        let shifted = shift2_f32(input, true);
        let (mut real, mut imag) = split_f32(&shifted);
        timing.pack += pack_start.elapsed();

        let log_rows = rows.ilog2() as usize;
        let log_columns = columns.ilog2() as usize;
        let plan_start = Instant::now();
        let setup = unsafe { vDSP_create_fftsetup(log_rows.max(log_columns), FFT_RADIX2) };
        timing.plan += plan_start.elapsed();
        if setup.is_null() {
            return Err("accelerate_vdsp_create_fftsetup_failed");
        }

        let exec_start = Instant::now();
        let split = DSPSplitComplex {
            realp: real.as_mut_ptr(),
            imagp: imag.as_mut_ptr(),
        };
        unsafe {
            vDSP_fft2d_zip(
                setup,
                &split,
                columns as isize,
                1,
                log_rows,
                log_columns,
                direction_value(direction),
            );
        }
        timing.exec += exec_start.elapsed();
        unsafe {
            vDSP_destroy_fftsetup(setup);
        }
        if direction == FftDirection::Inverse {
            let scale = 1.0 / (rows * columns) as f32;
            for value in &mut real {
                *value *= scale;
            }
            for value in &mut imag {
                *value *= scale;
            }
        }

        let pack_start = Instant::now();
        let unshifted = join_f32(rows, columns, &real, &imag);
        let output = shift2_f32(&unshifted, false);
        timing.pack += pack_start.elapsed();
        timing.total = total_start.elapsed();
        Ok((output, timing))
    }

    pub(super) fn transform_f64(
        input: &Array2<Complex64>,
        direction: FftDirection,
        use_case: FftUseCase,
    ) -> Result<(Array2<Complex64>, FftTiming), &'static str> {
        let rows = input.shape()[0];
        let columns = input.shape()[1];
        if !rows.is_power_of_two() || !columns.is_power_of_two() {
            return Err("accelerate_vdsp_fft_requires_power_of_two_axes");
        }
        let spec = Fft2Spec::centered_c2c(
            rows,
            columns,
            FftPrecision::F64,
            direction,
            use_case,
            FftBackendChoice::Accelerate,
        );
        let selection = select_fft_backend(spec);
        let mut timing = FftTiming::new(spec, selection);
        let total_start = Instant::now();

        let pack_start = Instant::now();
        let shifted = shift2_f64(input, true);
        let (mut real, mut imag) = split_f64(&shifted);
        timing.pack += pack_start.elapsed();

        let log_rows = rows.ilog2() as usize;
        let log_columns = columns.ilog2() as usize;
        let plan_start = Instant::now();
        let setup = unsafe { vDSP_create_fftsetupD(log_rows.max(log_columns), FFT_RADIX2) };
        timing.plan += plan_start.elapsed();
        if setup.is_null() {
            return Err("accelerate_vdsp_create_fftsetup_failed");
        }

        let exec_start = Instant::now();
        let split = DSPDoubleSplitComplex {
            realp: real.as_mut_ptr(),
            imagp: imag.as_mut_ptr(),
        };
        unsafe {
            vDSP_fft2d_zipD(
                setup,
                &split,
                columns as isize,
                1,
                log_rows,
                log_columns,
                direction_value(direction),
            );
        }
        timing.exec += exec_start.elapsed();
        unsafe {
            vDSP_destroy_fftsetupD(setup);
        }
        if direction == FftDirection::Inverse {
            let scale = 1.0 / (rows * columns) as f64;
            for value in &mut real {
                *value *= scale;
            }
            for value in &mut imag {
                *value *= scale;
            }
        }

        let pack_start = Instant::now();
        let unshifted = join_f64(rows, columns, &real, &imag);
        let output = shift2_f64(&unshifted, false);
        timing.pack += pack_start.elapsed();
        timing.total = total_start.elapsed();
        Ok((output, timing))
    }

    fn direction_value(direction: FftDirection) -> c_int {
        match direction {
            FftDirection::Forward => FFT_FORWARD,
            FftDirection::Inverse => FFT_INVERSE,
        }
    }
}

#[cfg(target_os = "macos")]
fn split_f32(input: &Array2<Complex32>) -> (Vec<f32>, Vec<f32>) {
    let mut real = Vec::with_capacity(input.len());
    let mut imag = Vec::with_capacity(input.len());
    for value in input.iter() {
        real.push(value.re);
        imag.push(value.im);
    }
    (real, imag)
}

#[cfg(target_os = "macos")]
fn split_f64(input: &Array2<Complex64>) -> (Vec<f64>, Vec<f64>) {
    let mut real = Vec::with_capacity(input.len());
    let mut imag = Vec::with_capacity(input.len());
    for value in input.iter() {
        real.push(value.re);
        imag.push(value.im);
    }
    (real, imag)
}

#[cfg(target_os = "macos")]
fn join_f32(rows: usize, columns: usize, real: &[f32], imag: &[f32]) -> Array2<Complex32> {
    Array2::from_shape_fn((rows, columns), |(row, column)| {
        let index = row * columns + column;
        Complex32::new(real[index], imag[index])
    })
}

#[cfg(target_os = "macos")]
fn join_f64(rows: usize, columns: usize, real: &[f64], imag: &[f64]) -> Array2<Complex64> {
    Array2::from_shape_fn((rows, columns), |(row, column)| {
        let index = row * columns + column;
        Complex64::new(real[index], imag[index])
    })
}

#[cfg(target_os = "macos")]
fn shift2_f32(input: &Array2<Complex32>, inverse: bool) -> Array2<Complex32> {
    let rows = input.shape()[0];
    let columns = input.shape()[1];
    Array2::from_shape_fn((rows, columns), |(row, column)| {
        let row_shift = if inverse { rows.div_ceil(2) } else { rows / 2 };
        let column_shift = if inverse {
            columns.div_ceil(2)
        } else {
            columns / 2
        };
        input[((row + row_shift) % rows, (column + column_shift) % columns)]
    })
}

#[cfg(target_os = "macos")]
fn shift2_f64(input: &Array2<Complex64>, inverse: bool) -> Array2<Complex64> {
    let rows = input.shape()[0];
    let columns = input.shape()[1];
    Array2::from_shape_fn((rows, columns), |(row, column)| {
        let row_shift = if inverse { rows.div_ceil(2) } else { rows / 2 };
        let column_shift = if inverse {
            columns.div_ceil(2)
        } else {
            columns / 2
        };
        input[((row + row_shift) % rows, (column + column_shift) % columns)]
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn auto_keeps_dirty_f64_on_rustfft_with_platform_reason() {
        let spec = Fft2Spec::centered_c2c(
            64,
            64,
            FftPrecision::F64,
            FftDirection::Inverse,
            FftUseCase::DirtyPsfResidual,
            FftBackendChoice::Auto,
        );

        let selection = select_fft_backend(spec);

        assert_eq!(selection.selected_backend, FftBackendChoice::RustFft);
        if cfg!(target_os = "macos") {
            assert!(selection.fallback_used);
            assert_eq!(
                selection.reason,
                "apple_metal_f64_unavailable_for_dirty_role_using_rustfft"
            );
        } else {
            assert!(!selection.fallback_used);
            assert_eq!(selection.reason, "rustfft_default");
        }
    }

    #[cfg(target_os = "macos")]
    #[test]
    fn auto_prefers_accelerate_for_small_power_of_two_dirty_f32_product_transforms() {
        let spec = Fft2Spec::centered_c2c(
            1024,
            1024,
            FftPrecision::F32,
            FftDirection::Inverse,
            FftUseCase::DirtyPsfResidual,
            FftBackendChoice::Auto,
        );

        let selection = select_fft_backend(spec);

        assert_eq!(selection.selected_backend, FftBackendChoice::Accelerate);
        assert!(!selection.fallback_used);
        assert_eq!(
            selection.reason,
            "accelerate_vdsp_power_of_two_host_shape_supported"
        );
    }

    #[cfg(all(target_os = "macos", not(coverage)))]
    #[test]
    fn auto_prefers_mpsgraph_for_awkward_dirty_f32_product_transforms_when_available() {
        let spec = Fft2Spec::centered_c2c(
            2500,
            2500,
            FftPrecision::F32,
            FftDirection::Inverse,
            FftUseCase::DirtyPsfResidual,
            FftBackendChoice::Auto,
        );

        let capability = fft_backend_capability(FftBackendChoice::MetalMpsGraph, spec);
        let selection = select_fft_backend(spec);

        if capability.supported {
            assert_eq!(selection.selected_backend, FftBackendChoice::MetalMpsGraph);
            assert!(!selection.fallback_used);
            assert_eq!(
                selection.reason,
                "metal_mpsgraph_complex_f32_host_batch_supported"
            );
        } else {
            assert_eq!(selection.selected_backend, FftBackendChoice::RustFft);
            assert_eq!(
                selection.reason,
                "accelerate_vdsp_fft_requires_power_of_two_axes"
            );
        }
    }

    #[cfg(all(target_os = "macos", not(coverage)))]
    #[test]
    fn auto_prefers_mpsgraph_for_dirty_f32_product_batches() {
        let spec = Fft2Spec::centered_c2c_batch(
            2048,
            2048,
            2,
            FftPrecision::F32,
            FftDirection::Inverse,
            FftUseCase::DirtyPsfResidual,
            FftBackendChoice::Auto,
        );

        let capability = fft_backend_capability(FftBackendChoice::MetalMpsGraph, spec);
        let selection = select_fft_backend(spec);

        if capability.supported {
            assert_eq!(selection.selected_backend, FftBackendChoice::MetalMpsGraph);
            assert!(!selection.fallback_used);
            assert_eq!(
                selection.reason,
                "metal_mpsgraph_complex_f32_host_batch_supported"
            );
        } else {
            let accelerate_capability = fft_backend_capability(FftBackendChoice::Accelerate, spec);
            assert_eq!(selection.selected_backend, FftBackendChoice::Accelerate);
            assert_eq!(selection.reason, accelerate_capability.reason);
        }
    }

    #[test]
    fn rustfft_validates_centered_shapes_and_precisions() {
        for precision in [FftPrecision::F32, FftPrecision::F64] {
            for (rows, columns) in [(8, 8), (7, 5), (8, 9), (6, 10), (5, 7)] {
                let spec = Fft2Spec::centered_c2c(
                    rows,
                    columns,
                    precision,
                    FftDirection::Forward,
                    FftUseCase::Benchmark,
                    FftBackendChoice::RustFft,
                );

                let report = validate_fft_backend(spec);

                assert!(
                    report.passed,
                    "precision={precision} shape={rows}x{columns} report={report:?}"
                );
            }
        }
    }

    #[cfg(target_os = "macos")]
    #[test]
    fn accelerate_f32_large_power_of_two_shape_validates_with_scale_aware_tolerance() {
        let spec = Fft2Spec::centered_c2c(
            128,
            128,
            FftPrecision::F32,
            FftDirection::Forward,
            FftUseCase::Benchmark,
            FftBackendChoice::Accelerate,
        );

        let report = validate_fft_backend(spec);

        assert!(report.capability.supported, "{report:?}");
        assert!(report.passed, "{report:?}");
        assert!(report.forward_max_abs_error.unwrap_or_default() > 3.0e-4);
        assert!(
            report.inverse_max_abs_error.unwrap_or_default() <= 3.0e-4,
            "{report:?}"
        );
    }

    #[test]
    fn metal_gpu_candidates_declare_f64_unsupported_on_apple_path() {
        for backend in [
            FftBackendChoice::MetalVkFft,
            FftBackendChoice::MetalMpsGraph,
        ] {
            let spec = Fft2Spec::centered_c2c(
                2048,
                2048,
                FftPrecision::F64,
                FftDirection::Forward,
                FftUseCase::DirtyPsfResidual,
                backend,
            );

            let capability = fft_backend_capability(backend, spec);

            assert!(!capability.supported, "{capability:?}");
            if backend == FftBackendChoice::MetalVkFft && !cfg!(target_os = "macos") {
                assert_eq!(capability.reason, "metal_vkfft_adapter_not_integrated");
            } else {
                assert!(
                    capability.reason.contains("f64") || capability.reason.contains("double2"),
                    "{capability:?}"
                );
            }
        }
    }

    #[cfg(target_os = "macos")]
    #[test]
    fn accelerate_declares_awkward_shapes_unsupported() {
        let spec = Fft2Spec::centered_c2c(
            7,
            5,
            FftPrecision::F64,
            FftDirection::Forward,
            FftUseCase::Benchmark,
            FftBackendChoice::Accelerate,
        );

        let report = validate_fft_backend(spec);

        assert!(!report.capability.supported);
        assert_eq!(
            report.capability.reason,
            "accelerate_vdsp_fft_requires_power_of_two_axes"
        );
    }

    #[test]
    fn wall_to_io_ratio_is_absent_for_zero_io() {
        assert_eq!(
            wall_to_io_ratio(Duration::from_secs(1), Duration::ZERO),
            None
        );
    }

    #[test]
    fn wall_to_io_ratio_uses_wall_divided_by_io() {
        assert_eq!(
            wall_to_io_ratio(Duration::from_secs(6), Duration::from_secs(2)),
            Some(3.0)
        );
    }
}

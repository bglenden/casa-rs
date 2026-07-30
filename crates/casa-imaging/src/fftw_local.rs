// SPDX-License-Identifier: LGPL-3.0-or-later
//! Dynamically loaded FFTW host backend.

use std::{
    collections::BTreeSet,
    env,
    ffi::{CStr, CString, OsStr, c_char, c_int, c_uint, c_void},
    fs::File,
    io::Read,
    path::{Path, PathBuf},
    sync::{Mutex, OnceLock},
    time::Instant,
};

use libloading::Library;
use ndarray::{Array2, ShapeBuilder};
use num_complex::{Complex32, Complex64};

use crate::fft_backend::{
    Fft2Spec, FftBackendChoice, FftDirection, FftPrecision, FftTiming, FftUseCase,
    select_fft_backend,
};

type FftwPlan = *mut c_void;
type FftwInitThreads = unsafe extern "C" fn() -> c_int;
type FftwPlanWithThreads = unsafe extern "C" fn(c_int);
type FftwPlanDft = unsafe extern "C" fn(
    c_int,
    *const c_int,
    *mut Complex64,
    *mut Complex64,
    c_int,
    c_uint,
) -> FftwPlan;
type FftwExecute = unsafe extern "C" fn(FftwPlan);
type FftwDestroyPlan = unsafe extern "C" fn(FftwPlan);
type FftwImportWisdomFromFilename = unsafe extern "C" fn(*const c_char) -> c_int;
type FftwfPlan = *mut c_void;
type FftwfInitThreads = unsafe extern "C" fn() -> c_int;
type FftwfPlanWithThreads = unsafe extern "C" fn(c_int);
type FftwfPlanDft = unsafe extern "C" fn(
    c_int,
    *const c_int,
    *mut Complex32,
    *mut Complex32,
    c_int,
    c_uint,
) -> FftwfPlan;
type FftwfPlanDftR2c =
    unsafe extern "C" fn(c_int, *const c_int, *mut f32, *mut Complex32, c_uint) -> FftwfPlan;
type FftwfPlanDftC2r =
    unsafe extern "C" fn(c_int, *const c_int, *mut Complex32, *mut f32, c_uint) -> FftwfPlan;
type FftwfExecute = unsafe extern "C" fn(FftwfPlan);
type FftwfDestroyPlan = unsafe extern "C" fn(FftwfPlan);
type FftwfImportWisdomFromFilename = unsafe extern "C" fn(*const c_char) -> c_int;

const FFTW_FORWARD: c_int = -1;
const FFTW_BACKWARD: c_int = 1;
const FFTW_ESTIMATE: c_uint = 1 << 6;
const FFTW_WISDOM_ONLY: c_uint = 1 << 21;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum FftwResolutionSource {
    ExplicitDirectory,
    ConventionalDirectory,
}

impl FftwResolutionSource {
    const fn as_str(self) -> &'static str {
        match self {
            Self::ExplicitDirectory => "explicit-directory",
            Self::ConventionalDirectory => "conventional-directory",
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct FftwLibraryProvenance {
    resolution_source: FftwResolutionSource,
    core_path: PathBuf,
    core_sha256: String,
    threads_path: PathBuf,
    threads_sha256: String,
    version: String,
    compiler: String,
}

#[derive(Debug)]
struct ResolvedLibraryPair {
    source: FftwResolutionSource,
    core_path: PathBuf,
    threads_path: PathBuf,
}

#[derive(Debug)]
struct FftwPlanConfiguration {
    flags: c_uint,
    flags_name: &'static str,
    wisdom_path: Option<PathBuf>,
    wisdom_sha256: Option<String>,
}

#[derive(Clone, Debug)]
pub(crate) struct FftwRealHalfSpectrumF32 {
    logical_shape: (usize, usize),
    values: Array2<Complex32>,
}

impl FftwRealHalfSpectrumF32 {
    pub(crate) const fn logical_shape(&self) -> (usize, usize) {
        self.logical_shape
    }

    pub(crate) const fn values(&self) -> &Array2<Complex32> {
        &self.values
    }

    pub(crate) const fn values_mut(&mut self) -> &mut Array2<Complex32> {
        &mut self.values
    }
}

struct FftwApi {
    _core: Library,
    _threads: Library,
    provenance: FftwLibraryProvenance,
    init_threads: FftwInitThreads,
    plan_with_threads: FftwPlanWithThreads,
    plan_dft: FftwPlanDft,
    execute: FftwExecute,
    destroy_plan: FftwDestroyPlan,
    import_wisdom_from_filename: FftwImportWisdomFromFilename,
}

struct FftwfApi {
    _core: Library,
    _threads: Library,
    provenance: FftwLibraryProvenance,
    init_threads: FftwfInitThreads,
    plan_with_threads: FftwfPlanWithThreads,
    plan_dft: FftwfPlanDft,
    plan_dft_r2c: FftwfPlanDftR2c,
    plan_dft_c2r: FftwfPlanDftC2r,
    execute: FftwfExecute,
    destroy_plan: FftwfDestroyPlan,
    import_wisdom_from_filename: FftwfImportWisdomFromFilename,
}

static FFTW_API: OnceLock<FftwApi> = OnceLock::new();
static FFTWF_API: OnceLock<FftwfApi> = OnceLock::new();
static FFTW_PLAN_LOCK: Mutex<()> = Mutex::new(());
static FFTW_RECEIPT_LINES: OnceLock<Mutex<BTreeSet<String>>> = OnceLock::new();

pub(crate) fn configured() -> bool {
    api().is_ok()
}

pub(crate) fn configured_f32() -> bool {
    resolution_source_f32().is_some()
}

pub(crate) fn resolution_source_f32() -> Option<FftwResolutionSource> {
    api_f32().ok().map(|api| api.provenance.resolution_source)
}

/// Run the uncentered real-to-complex transform used by
/// `FFTServer<Float, Complex>::fft0`.
///
/// The input has logical casacore shape `(nx, ny)`. It is copied into
/// first-axis-contiguous storage before FFTW is called with reversed
/// dimensions `[ny, nx]`, so FFTW's final-axis truncation becomes casacore's
/// half first logical axis. The returned spectrum therefore has logical shape
/// `(nx / 2 + 1, ny)` and first-axis-contiguous physical storage.
pub(crate) fn fft0_r2c_f32_casacore_layout(
    input: &Array2<f32>,
    threads: usize,
) -> Result<FftwRealHalfSpectrumF32, &'static str> {
    let (nx, ny) = input.dim();
    let (dimensions, real_elements, half_elements) = fft0_real_shape(nx, ny)?;
    let threads = fftw_thread_count(threads)?;
    let half_nx = nx / 2 + 1;
    let api = api_f32()?;

    let mut input_storage = Vec::with_capacity(real_elements);
    for y in 0..ny {
        for x in 0..nx {
            input_storage.push(input[(x, y)]);
        }
    }
    let mut output_storage = vec![Complex32::new(0.0, 0.0); half_elements];

    let plan_configuration = plan_configuration("CASA_RS_EXPERIMENTAL_FFTW_F32_WISDOM")?;
    let plan = {
        let _guard = FFTW_PLAN_LOCK
            .lock()
            .map_err(|_| "fftw_local_plan_lock_poisoned")?;
        // SAFETY: the symbol signature is FFTW's public single-precision C
        // ABI. Both buffers match the reversed dimensions and remain
        // allocated until the bound plan has executed and been destroyed.
        unsafe {
            if (api.init_threads)() == 0 {
                return Err("fftw_local_init_threads_failed");
            }
            (api.plan_with_threads)(threads);
            if let Some(path) = plan_configuration.wisdom_path.as_ref() {
                let path = CString::new(path.as_os_str().as_encoded_bytes())
                    .map_err(|_| "fftw_local_wisdom_path_contains_nul")?;
                if (api.import_wisdom_from_filename)(path.as_ptr()) == 0 {
                    return Err("fftw_local_wisdom_import_failed");
                }
                verify_wisdom_unchanged(&plan_configuration)?;
            }
            (api.plan_dft_r2c)(
                2,
                dimensions.as_ptr(),
                input_storage.as_mut_ptr(),
                output_storage.as_mut_ptr(),
                plan_configuration.flags,
            )
        }
    };
    record_provenance_once("f32", &api.provenance, threads, &plan_configuration);
    if plan.is_null() {
        return Err("fftw_local_r2c_plan_failed");
    }

    // SAFETY: `plan` is bound to the live input and output allocations above.
    unsafe {
        (api.execute)(plan);
        (api.destroy_plan)(plan);
    }

    let values = Array2::from_shape_vec((half_nx, ny).f(), output_storage)
        .map_err(|_| "fftw_local_r2c_output_shape_failed")?;
    Ok(FftwRealHalfSpectrumF32 {
        logical_shape: (nx, ny),
        values,
    })
}

/// Run the uncentered complex-to-real transform used by
/// `FFTServer<Float, Complex>::fft0`.
///
/// The half spectrum's first logical axis is restored to its recorded real
/// length. FFTW sees reversed dimensions `[ny, nx]`. Its unnormalized output
/// is divided element-by-element in `f64` by the full real element count and
/// then cast to `f32`, matching casacore's `Float /= 1.0 * size` evaluation.
/// The returned real array has logical shape `(nx, ny)` and
/// first-axis-contiguous physical storage.
pub(crate) fn fft0_c2r_f32_casacore_layout(
    input: &FftwRealHalfSpectrumF32,
    threads: usize,
) -> Result<Array2<f32>, &'static str> {
    let (nx, ny) = input.logical_shape;
    let (dimensions, real_elements, half_elements) = fft0_real_shape(nx, ny)?;
    let threads = fftw_thread_count(threads)?;
    let half_nx = nx / 2 + 1;
    if input.values.dim() != (half_nx, ny) || input.values.strides() != [1, half_nx as isize] {
        return Err("fftw_local_c2r_requires_casacore_half_spectrum_layout");
    }
    let api = api_f32()?;
    let mut input_storage = input
        .values
        .as_slice_memory_order()
        .ok_or("fftw_local_c2r_requires_contiguous_half_spectrum")?
        .to_vec();
    if input_storage.len() != half_elements {
        return Err("fftw_local_c2r_half_spectrum_length_mismatch");
    }
    let mut output_storage = vec![0.0_f32; real_elements];

    let plan_configuration = plan_configuration("CASA_RS_EXPERIMENTAL_FFTW_F32_WISDOM")?;
    let plan = {
        let _guard = FFTW_PLAN_LOCK
            .lock()
            .map_err(|_| "fftw_local_plan_lock_poisoned")?;
        // SAFETY: the symbol signature is FFTW's public single-precision C
        // ABI. Both buffers match the reversed dimensions and remain
        // allocated until the bound plan has executed and been destroyed.
        unsafe {
            if (api.init_threads)() == 0 {
                return Err("fftw_local_init_threads_failed");
            }
            (api.plan_with_threads)(threads);
            if let Some(path) = plan_configuration.wisdom_path.as_ref() {
                let path = CString::new(path.as_os_str().as_encoded_bytes())
                    .map_err(|_| "fftw_local_wisdom_path_contains_nul")?;
                if (api.import_wisdom_from_filename)(path.as_ptr()) == 0 {
                    return Err("fftw_local_wisdom_import_failed");
                }
                verify_wisdom_unchanged(&plan_configuration)?;
            }
            (api.plan_dft_c2r)(
                2,
                dimensions.as_ptr(),
                input_storage.as_mut_ptr(),
                output_storage.as_mut_ptr(),
                plan_configuration.flags,
            )
        }
    };
    record_provenance_once("f32", &api.provenance, threads, &plan_configuration);
    if plan.is_null() {
        return Err("fftw_local_c2r_plan_failed");
    }

    // SAFETY: `plan` is bound to the live input and output allocations above.
    unsafe {
        (api.execute)(plan);
        (api.destroy_plan)(plan);
    }
    let divisor = real_elements as f64;
    for value in &mut output_storage {
        *value = (f64::from(*value) / divisor) as f32;
    }

    Array2::from_shape_vec((nx, ny).f(), output_storage)
        .map_err(|_| "fftw_local_c2r_output_shape_failed")
}

fn fft0_real_shape(nx: usize, ny: usize) -> Result<([c_int; 2], usize, usize), &'static str> {
    let nx_c_int = c_int::try_from(nx)
        .ok()
        .filter(|&value| value > 0)
        .ok_or("fftw_local_real_fft_requires_non_empty_int_range_axes")?;
    let ny_c_int = c_int::try_from(ny)
        .ok()
        .filter(|&value| value > 0)
        .ok_or("fftw_local_real_fft_requires_non_empty_int_range_axes")?;
    let real_elements = nx
        .checked_mul(ny)
        .ok_or("fftw_local_real_fft_element_count_overflow")?;
    let half_elements = (nx / 2 + 1)
        .checked_mul(ny)
        .ok_or("fftw_local_real_fft_element_count_overflow")?;
    Ok(([ny_c_int, nx_c_int], real_elements, half_elements))
}

fn fftw_thread_count(threads: usize) -> Result<c_int, &'static str> {
    c_int::try_from(threads)
        .ok()
        .filter(|&value| value > 0)
        .ok_or("fftw_local_requires_positive_int_range_thread_count")
}

pub(crate) fn centered_transform_f32(
    input: &Array2<Complex32>,
    direction: FftDirection,
    use_case: FftUseCase,
) -> Result<(Array2<Complex32>, FftTiming), &'static str> {
    centered_transform_f32_owned(input.clone(), direction, use_case)
}

pub(crate) fn centered_transform_f32_with_threads(
    input: &Array2<Complex32>,
    direction: FftDirection,
    use_case: FftUseCase,
    threads: usize,
) -> Result<(Array2<Complex32>, FftTiming), &'static str> {
    centered_transform_f32_owned_with_threads(input.clone(), direction, use_case, threads)
}

/// Run FFTW with the physical x-contiguous storage used by casacore arrays.
///
/// `ndarray`'s standard `(x, y)` layout keeps `y` contiguous, whereas
/// `casacore::Array` keeps its first (`x`) axis contiguous. CASA's FFTServer
/// reverses the logical shape before passing that storage to FFTW. Transpose
/// into the corresponding physical layout, run the same centered transform,
/// and expose the logical result with the same first-axis-contiguous physical
/// layout without a second image-sized copy.
pub(crate) fn centered_transform_f32_casacore_layout_with_threads(
    input: &Array2<Complex32>,
    direction: FftDirection,
    use_case: FftUseCase,
    threads: usize,
) -> Result<(Array2<Complex32>, FftTiming), &'static str> {
    let rows = input.shape()[0];
    let columns = input.shape()[1];
    let total_started = Instant::now();

    let pack_started = Instant::now();
    let casacore_storage = input.view().reversed_axes().as_standard_layout().to_owned();
    let input_pack = pack_started.elapsed();

    let (casacore_output, mut timing) =
        centered_transform_f32_owned_with_threads(casacore_storage, direction, use_case, threads)?;

    let output = casacore_output.reversed_axes();
    timing.pack += input_pack;
    timing.total = total_started.elapsed();

    let spec = Fft2Spec::centered_c2c(
        rows,
        columns,
        FftPrecision::F32,
        direction,
        use_case,
        FftBackendChoice::Fftw,
    );
    timing.spec = spec;
    timing.selection = select_fft_backend(spec);
    Ok((output, timing))
}

pub(crate) fn centered_transform_f32_casacore_storage_owned_with_threads(
    casacore_storage: Array2<Complex32>,
    direction: FftDirection,
    use_case: FftUseCase,
    threads: usize,
) -> Result<(Array2<Complex32>, FftTiming), &'static str> {
    let logical_rows = casacore_storage.shape()[1];
    let logical_columns = casacore_storage.shape()[0];
    let total_started = Instant::now();
    let (casacore_output, mut timing) =
        centered_transform_f32_owned_with_threads(casacore_storage, direction, use_case, threads)?;
    let output = casacore_output.reversed_axes();
    timing.total = total_started.elapsed();
    let spec = Fft2Spec::centered_c2c(
        logical_rows,
        logical_columns,
        FftPrecision::F32,
        direction,
        use_case,
        FftBackendChoice::Fftw,
    );
    timing.spec = spec;
    timing.selection = select_fft_backend(spec);
    Ok((output, timing))
}

pub(crate) fn centered_transform_f32_owned(
    output: Array2<Complex32>,
    direction: FftDirection,
    use_case: FftUseCase,
) -> Result<(Array2<Complex32>, FftTiming), &'static str> {
    centered_transform_f32_owned_with_threads(output, direction, use_case, thread_count() as usize)
}

fn centered_transform_f32_owned_with_threads(
    mut output: Array2<Complex32>,
    direction: FftDirection,
    use_case: FftUseCase,
    threads: usize,
) -> Result<(Array2<Complex32>, FftTiming), &'static str> {
    let threads = c_int::try_from(threads)
        .ok()
        .filter(|&value| value > 0)
        .ok_or("fftw_local_requires_positive_int_range_thread_count")?;
    if output.shape().len() != 2
        || output.shape()[0] == 0
        || output.shape()[1] == 0
        || output.shape()[0] & 1 != 0
        || output.shape()[1] & 1 != 0
        || output.shape()[0] > c_int::MAX as usize
        || output.shape()[1] > c_int::MAX as usize
    {
        return Err("fftw_local_requires_non_empty_even_int_range_axes");
    }
    let api = api_f32()?;
    let rows = output.shape()[0];
    let columns = output.shape()[1];
    let spec = Fft2Spec::centered_c2c(
        rows,
        columns,
        FftPrecision::F32,
        direction,
        use_case,
        FftBackendChoice::Fftw,
    );
    let selection = select_fft_backend(spec);
    if !selection.requested_backend_supported {
        return Err("fftw_local_backend_not_supported");
    }
    let mut timing = FftTiming::new(spec, selection);
    timing.plan_cache_hit = false;
    let total_started = Instant::now();

    let pack_started = Instant::now();
    if !shift_quadrants_in_place_f32(&mut output) {
        return Err("fftw_local_requires_contiguous_even_axes");
    }
    timing.pack += pack_started.elapsed();

    let dimensions = [rows as c_int, columns as c_int];
    let sign = match direction {
        FftDirection::Forward => FFTW_FORWARD,
        FftDirection::Inverse => FFTW_BACKWARD,
    };
    let plan_started = Instant::now();
    let output_pointer = output
        .as_slice_memory_order_mut()
        .ok_or("fftw_local_requires_contiguous_input")?
        .as_mut_ptr();
    let plan_configuration = plan_configuration("CASA_RS_EXPERIMENTAL_FFTW_F32_WISDOM")?;
    let plan = {
        let _guard = FFTW_PLAN_LOCK
            .lock()
            .map_err(|_| "fftw_local_plan_lock_poisoned")?;
        // SAFETY: the dynamically loaded symbols use FFTW's documented C ABI.
        // Planning is serialized because FFTW's planner and thread count are
        // global. The input is contiguous and remains alive until destruction.
        unsafe {
            if (api.init_threads)() == 0 {
                return Err("fftw_local_init_threads_failed");
            }
            (api.plan_with_threads)(threads);
            if let Some(path) = plan_configuration.wisdom_path.as_ref() {
                let path = CString::new(path.as_os_str().as_encoded_bytes())
                    .map_err(|_| "fftw_local_wisdom_path_contains_nul")?;
                if (api.import_wisdom_from_filename)(path.as_ptr()) == 0 {
                    return Err("fftw_local_wisdom_import_failed");
                }
                verify_wisdom_unchanged(&plan_configuration)?;
            }
            (api.plan_dft)(
                2,
                dimensions.as_ptr(),
                output_pointer,
                output_pointer,
                sign,
                plan_configuration.flags,
            )
        }
    };
    record_provenance_once("f32", &api.provenance, threads, &plan_configuration);
    timing.plan = plan_started.elapsed();
    if plan.is_null() {
        return Err("fftw_local_plan_failed");
    }

    let exec_started = Instant::now();
    // SAFETY: `plan` was created for `output`, which remains allocated and
    // unmoved for the duration of execution.
    unsafe {
        (api.execute)(plan);
    }
    if direction == FftDirection::Inverse {
        let scale = 1.0 / (rows * columns) as f32;
        output.mapv_inplace(|value| value * scale);
    }
    timing.exec = exec_started.elapsed();

    let pack_started = Instant::now();
    let shifted = shift_quadrants_in_place_f32(&mut output);
    debug_assert!(shifted);
    timing.pack += pack_started.elapsed();
    timing.total = total_started.elapsed();

    // SAFETY: `plan` is a live FFTW plan and is destroyed exactly once after
    // its bound execution has completed.
    unsafe {
        (api.destroy_plan)(plan);
    }
    Ok((output, timing))
}

pub(crate) fn centered_transform_f64(
    input: &Array2<Complex64>,
    direction: FftDirection,
    use_case: FftUseCase,
) -> Result<(Array2<Complex64>, FftTiming), &'static str> {
    centered_transform_f64_owned(input.clone(), direction, use_case)
}

pub(crate) fn centered_transform_f64_owned(
    mut output: Array2<Complex64>,
    direction: FftDirection,
    use_case: FftUseCase,
) -> Result<(Array2<Complex64>, FftTiming), &'static str> {
    if output.shape().len() != 2
        || output.shape()[0] == 0
        || output.shape()[1] == 0
        || output.shape()[0] & 1 != 0
        || output.shape()[1] & 1 != 0
        || output.shape()[0] > c_int::MAX as usize
        || output.shape()[1] > c_int::MAX as usize
    {
        return Err("fftw_local_requires_non_empty_even_int_range_axes");
    }
    let api = api()?;
    let rows = output.shape()[0];
    let columns = output.shape()[1];
    let spec = Fft2Spec::centered_c2c(
        rows,
        columns,
        FftPrecision::F64,
        direction,
        use_case,
        FftBackendChoice::Fftw,
    );
    let selection = select_fft_backend(spec);
    if !selection.requested_backend_supported {
        return Err("fftw_local_backend_not_supported");
    }
    let mut timing = FftTiming::new(spec, selection);
    timing.plan_cache_hit = false;
    let total_started = Instant::now();

    let pack_started = Instant::now();
    if !shift_quadrants_in_place(&mut output) {
        return Err("fftw_local_requires_contiguous_even_axes");
    }
    timing.pack += pack_started.elapsed();

    let dimensions = [rows as c_int, columns as c_int];
    let sign = match direction {
        FftDirection::Forward => FFTW_FORWARD,
        FftDirection::Inverse => FFTW_BACKWARD,
    };
    let plan_started = Instant::now();
    let output_pointer = output
        .as_slice_memory_order_mut()
        .ok_or("fftw_local_requires_contiguous_input")?
        .as_mut_ptr();
    let threads = thread_count();
    let plan_configuration = plan_configuration("CASA_RS_EXPERIMENTAL_FFTW_F64_WISDOM")?;
    let plan = {
        let _guard = FFTW_PLAN_LOCK
            .lock()
            .map_err(|_| "fftw_local_plan_lock_poisoned")?;
        // SAFETY: the dynamically loaded symbols use FFTW's documented C ABI.
        // Planning is serialized because FFTW's planner and thread count are
        // global. The input is contiguous and remains alive until destruction.
        unsafe {
            if (api.init_threads)() == 0 {
                return Err("fftw_local_init_threads_failed");
            }
            (api.plan_with_threads)(threads);
            if let Some(path) = plan_configuration.wisdom_path.as_ref() {
                let path = CString::new(path.as_os_str().as_encoded_bytes())
                    .map_err(|_| "fftw_local_wisdom_path_contains_nul")?;
                if (api.import_wisdom_from_filename)(path.as_ptr()) == 0 {
                    return Err("fftw_local_wisdom_import_failed");
                }
                verify_wisdom_unchanged(&plan_configuration)?;
            }
            (api.plan_dft)(
                2,
                dimensions.as_ptr(),
                output_pointer,
                output_pointer,
                sign,
                plan_configuration.flags,
            )
        }
    };
    record_provenance_once("f64", &api.provenance, threads, &plan_configuration);
    timing.plan = plan_started.elapsed();
    if plan.is_null() {
        return Err("fftw_local_plan_failed");
    }

    let exec_started = Instant::now();
    // SAFETY: `plan` was created for `output`, which remains allocated and
    // unmoved for the duration of execution.
    unsafe {
        (api.execute)(plan);
    }
    if direction == FftDirection::Inverse {
        let scale = 1.0 / (rows * columns) as f64;
        output.mapv_inplace(|value| value * scale);
    }
    timing.exec = exec_started.elapsed();

    let pack_started = Instant::now();
    let shifted = shift_quadrants_in_place(&mut output);
    debug_assert!(shifted);
    timing.pack += pack_started.elapsed();
    timing.total = total_started.elapsed();

    // SAFETY: `plan` is a live FFTW plan and is destroyed exactly once after
    // its bound execution has completed.
    unsafe {
        (api.destroy_plan)(plan);
    }
    Ok((output, timing))
}

fn api() -> Result<&'static FftwApi, &'static str> {
    if let Some(api) = FFTW_API.get() {
        return Ok(api);
    }
    let loaded = load_api()?;
    let _ = FFTW_API.set(loaded);
    FFTW_API.get().ok_or("fftw_local_api_initialization_race")
}

fn api_f32() -> Result<&'static FftwfApi, &'static str> {
    if let Some(api) = FFTWF_API.get() {
        return Ok(api);
    }
    let loaded = load_api_f32()?;
    let _ = FFTWF_API.set(loaded);
    FFTWF_API.get().ok_or("fftw_local_api_initialization_race")
}

fn load_api() -> Result<FftwApi, &'static str> {
    let resolved = library_paths_f64()?;
    // SAFETY: loading is restricted to an explicit operator-supplied
    // directory or a conventional system package-manager directory.
    let core = unsafe { Library::new(&resolved.core_path) }
        .map_err(|_| "fftw_local_core_library_load_failed")?;
    // SAFETY: same resolved runtime-library boundary as the core library.
    let threads = unsafe { Library::new(&resolved.threads_path) }
        .map_err(|_| "fftw_local_threads_library_load_failed")?;
    let provenance = library_provenance(&resolved, &core, b"fftw_version\0", b"fftw_cc\0")?;

    // SAFETY: symbol names and signatures match FFTW 3's public C API. The
    // `Library` owners are retained in `FftwApi` for all copied symbols.
    unsafe {
        let init_threads = *threads
            .get::<FftwInitThreads>(b"fftw_init_threads\0")
            .map_err(|_| "fftw_local_missing_init_threads")?;
        let plan_with_threads = *threads
            .get::<FftwPlanWithThreads>(b"fftw_plan_with_nthreads\0")
            .map_err(|_| "fftw_local_missing_plan_with_nthreads")?;
        let plan_dft = *core
            .get::<FftwPlanDft>(b"fftw_plan_dft\0")
            .map_err(|_| "fftw_local_missing_plan_dft")?;
        let execute = *core
            .get::<FftwExecute>(b"fftw_execute\0")
            .map_err(|_| "fftw_local_missing_execute")?;
        let destroy_plan = *core
            .get::<FftwDestroyPlan>(b"fftw_destroy_plan\0")
            .map_err(|_| "fftw_local_missing_destroy_plan")?;
        let import_wisdom_from_filename = *core
            .get::<FftwImportWisdomFromFilename>(b"fftw_import_wisdom_from_filename\0")
            .map_err(|_| "fftw_local_missing_import_wisdom_from_filename")?;
        Ok(FftwApi {
            _core: core,
            _threads: threads,
            provenance,
            init_threads,
            plan_with_threads,
            plan_dft,
            execute,
            destroy_plan,
            import_wisdom_from_filename,
        })
    }
}

fn load_api_f32() -> Result<FftwfApi, &'static str> {
    let resolved = library_paths_f32()?;
    // SAFETY: loading is restricted to an explicit operator-supplied
    // directory or a conventional system package-manager directory.
    let core = unsafe { Library::new(&resolved.core_path) }
        .map_err(|_| "fftw_local_core_library_load_failed")?;
    // SAFETY: same resolved runtime-library boundary as the core library.
    let threads = unsafe { Library::new(&resolved.threads_path) }
        .map_err(|_| "fftw_local_threads_library_load_failed")?;
    let provenance = library_provenance(&resolved, &core, b"fftwf_version\0", b"fftwf_cc\0")?;

    // SAFETY: symbol names and signatures match FFTW 3's public single-
    // precision C ABI. The library owners are retained for all copied symbols.
    unsafe {
        let init_threads = *threads
            .get::<FftwfInitThreads>(b"fftwf_init_threads\0")
            .map_err(|_| "fftw_local_missing_init_threads")?;
        let plan_with_threads = *threads
            .get::<FftwfPlanWithThreads>(b"fftwf_plan_with_nthreads\0")
            .map_err(|_| "fftw_local_missing_plan_with_nthreads")?;
        let plan_dft = *core
            .get::<FftwfPlanDft>(b"fftwf_plan_dft\0")
            .map_err(|_| "fftw_local_missing_plan_dft")?;
        let plan_dft_r2c = *core
            .get::<FftwfPlanDftR2c>(b"fftwf_plan_dft_r2c\0")
            .map_err(|_| "fftw_local_missing_plan_dft_r2c")?;
        let plan_dft_c2r = *core
            .get::<FftwfPlanDftC2r>(b"fftwf_plan_dft_c2r\0")
            .map_err(|_| "fftw_local_missing_plan_dft_c2r")?;
        let execute = *core
            .get::<FftwfExecute>(b"fftwf_execute\0")
            .map_err(|_| "fftw_local_missing_execute")?;
        let destroy_plan = *core
            .get::<FftwfDestroyPlan>(b"fftwf_destroy_plan\0")
            .map_err(|_| "fftw_local_missing_destroy_plan")?;
        let import_wisdom_from_filename = *core
            .get::<FftwfImportWisdomFromFilename>(b"fftwf_import_wisdom_from_filename\0")
            .map_err(|_| "fftw_local_missing_import_wisdom_from_filename")?;
        Ok(FftwfApi {
            _core: core,
            _threads: threads,
            provenance,
            init_threads,
            plan_with_threads,
            plan_dft,
            plan_dft_r2c,
            plan_dft_c2r,
            execute,
            destroy_plan,
            import_wisdom_from_filename,
        })
    }
}

fn library_provenance(
    resolved: &ResolvedLibraryPair,
    core: &Library,
    version_symbol: &[u8],
    compiler_symbol: &[u8],
) -> Result<FftwLibraryProvenance, &'static str> {
    Ok(FftwLibraryProvenance {
        resolution_source: resolved.source,
        core_path: resolved.core_path.clone(),
        core_sha256: sha256_file(&resolved.core_path)
            .map_err(|_| "fftw_local_core_library_hash_failed")?,
        threads_path: resolved.threads_path.clone(),
        threads_sha256: sha256_file(&resolved.threads_path)
            .map_err(|_| "fftw_local_threads_library_hash_failed")?,
        version: library_string_symbol(core, version_symbol)
            .unwrap_or_else(|| "unavailable".to_owned()),
        compiler: library_string_symbol(core, compiler_symbol)
            .unwrap_or_else(|| "unavailable".to_owned()),
    })
}

fn library_string_symbol(library: &Library, symbol_name: &[u8]) -> Option<String> {
    // SAFETY: FFTW exposes its version and compiler metadata as
    // NUL-terminated global character arrays. The retained `Library` owns the
    // symbol storage for the duration of this read.
    unsafe {
        let symbol = library.get::<*const c_char>(symbol_name).ok()?;
        let pointer = *symbol;
        if pointer.is_null() {
            return None;
        }
        Some(CStr::from_ptr(pointer).to_string_lossy().into_owned())
    }
}

fn plan_configuration(
    wisdom_environment_variable: &str,
) -> Result<FftwPlanConfiguration, &'static str> {
    let Some(wisdom_path) = env::var_os(wisdom_environment_variable) else {
        return Ok(FftwPlanConfiguration {
            flags: FFTW_ESTIMATE,
            flags_name: "estimate",
            wisdom_path: None,
            wisdom_sha256: None,
        });
    };
    if wisdom_path.is_empty() {
        return Err("fftw_local_wisdom_path_empty");
    }
    let wisdom_path = PathBuf::from(wisdom_path)
        .canonicalize()
        .map_err(|_| "fftw_local_wisdom_path_canonicalization_failed")?;
    if !wisdom_path.is_file() {
        return Err("fftw_local_wisdom_path_not_file");
    }
    let wisdom_sha256 = sha256_file(&wisdom_path).map_err(|_| "fftw_local_wisdom_hash_failed")?;
    Ok(FftwPlanConfiguration {
        flags: FFTW_WISDOM_ONLY,
        flags_name: "wisdom-only",
        wisdom_path: Some(wisdom_path),
        wisdom_sha256: Some(wisdom_sha256),
    })
}

fn verify_wisdom_unchanged(configuration: &FftwPlanConfiguration) -> Result<(), &'static str> {
    let (Some(path), Some(expected_sha256)) = (
        configuration.wisdom_path.as_ref(),
        configuration.wisdom_sha256.as_ref(),
    ) else {
        return Ok(());
    };
    let actual_sha256 = sha256_file(path).map_err(|_| "fftw_local_wisdom_rehash_failed")?;
    if actual_sha256 != *expected_sha256 {
        return Err("fftw_local_wisdom_changed_during_import");
    }
    Ok(())
}

fn record_provenance_once(
    precision: &'static str,
    library: &FftwLibraryProvenance,
    threads: c_int,
    configuration: &FftwPlanConfiguration,
) {
    let line = provenance_receipt_line(precision, library, threads, configuration);
    let lines = FFTW_RECEIPT_LINES.get_or_init(|| Mutex::new(BTreeSet::new()));
    let should_record = lines
        .lock()
        .map(|mut lines| lines.insert(line.clone()))
        .unwrap_or(true);
    if should_record {
        eprintln!("{line}");
    }
}

fn provenance_receipt_line(
    precision: &'static str,
    library: &FftwLibraryProvenance,
    threads: c_int,
    configuration: &FftwPlanConfiguration,
) -> String {
    let wisdom_path = configuration
        .wisdom_path
        .as_deref()
        .map(receipt_path_token)
        .unwrap_or_else(|| "none".to_owned());
    let wisdom_sha256 = configuration.wisdom_sha256.as_deref().unwrap_or("none");
    format!(
        "fftw_runtime_provenance precision={precision} resolution={} core_path={} core_sha256={} threads_path={} threads_sha256={} version={} compiler={} fft_threads={threads} planner_flags={} planner_flags_bits={} wisdom_path={} wisdom_sha256={}",
        library.resolution_source.as_str(),
        receipt_path_token(&library.core_path),
        library.core_sha256,
        receipt_path_token(&library.threads_path),
        library.threads_sha256,
        receipt_token(library.version.as_bytes()),
        receipt_token(library.compiler.as_bytes()),
        configuration.flags_name,
        configuration.flags,
        wisdom_path,
        wisdom_sha256,
    )
}

fn receipt_path_token(path: &Path) -> String {
    receipt_token(path.as_os_str().as_encoded_bytes())
}

fn receipt_token(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    if bytes.is_empty() {
        return "empty".to_owned();
    }
    let mut token = String::with_capacity(bytes.len());
    for &byte in bytes {
        if byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.' | b'/' | b':') {
            token.push(char::from(byte));
        } else {
            token.push('%');
            token.push(char::from(HEX[(byte >> 4) as usize]));
            token.push(char::from(HEX[(byte & 0x0f) as usize]));
        }
    }
    token
}

struct Sha256 {
    state: [u32; 8],
    block: [u8; 64],
    block_len: usize,
    byte_len: u64,
}

impl Sha256 {
    const INITIAL_STATE: [u32; 8] = [
        0x6a09e667, 0xbb67ae85, 0x3c6ef372, 0xa54ff53a, 0x510e527f, 0x9b05688c, 0x1f83d9ab,
        0x5be0cd19,
    ];
    const ROUND_CONSTANTS: [u32; 64] = [
        0x428a2f98, 0x71374491, 0xb5c0fbcf, 0xe9b5dba5, 0x3956c25b, 0x59f111f1, 0x923f82a4,
        0xab1c5ed5, 0xd807aa98, 0x12835b01, 0x243185be, 0x550c7dc3, 0x72be5d74, 0x80deb1fe,
        0x9bdc06a7, 0xc19bf174, 0xe49b69c1, 0xefbe4786, 0x0fc19dc6, 0x240ca1cc, 0x2de92c6f,
        0x4a7484aa, 0x5cb0a9dc, 0x76f988da, 0x983e5152, 0xa831c66d, 0xb00327c8, 0xbf597fc7,
        0xc6e00bf3, 0xd5a79147, 0x06ca6351, 0x14292967, 0x27b70a85, 0x2e1b2138, 0x4d2c6dfc,
        0x53380d13, 0x650a7354, 0x766a0abb, 0x81c2c92e, 0x92722c85, 0xa2bfe8a1, 0xa81a664b,
        0xc24b8b70, 0xc76c51a3, 0xd192e819, 0xd6990624, 0xf40e3585, 0x106aa070, 0x19a4c116,
        0x1e376c08, 0x2748774c, 0x34b0bcb5, 0x391c0cb3, 0x4ed8aa4a, 0x5b9cca4f, 0x682e6ff3,
        0x748f82ee, 0x78a5636f, 0x84c87814, 0x8cc70208, 0x90befffa, 0xa4506ceb, 0xbef9a3f7,
        0xc67178f2,
    ];

    fn new() -> Self {
        Self {
            state: Self::INITIAL_STATE,
            block: [0; 64],
            block_len: 0,
            byte_len: 0,
        }
    }

    fn update(&mut self, mut input: &[u8]) {
        self.byte_len = self.byte_len.wrapping_add(input.len() as u64);
        if self.block_len != 0 {
            let copied = (64 - self.block_len).min(input.len());
            self.block[self.block_len..self.block_len + copied].copy_from_slice(&input[..copied]);
            self.block_len += copied;
            input = &input[copied..];
            if self.block_len < 64 {
                return;
            }
            let block = self.block;
            self.compress(&block);
            self.block_len = 0;
        }
        while input.len() >= 64 {
            let block = <&[u8; 64]>::try_from(&input[..64])
                .expect("64-byte SHA256 block slice must convert");
            self.compress(block);
            input = &input[64..];
        }
        self.block[..input.len()].copy_from_slice(input);
        self.block_len = input.len();
    }

    fn finish(mut self) -> String {
        let bit_len = self.byte_len.wrapping_mul(8);
        self.block[self.block_len] = 0x80;
        self.block_len += 1;
        if self.block_len > 56 {
            self.block[self.block_len..].fill(0);
            let block = self.block;
            self.compress(&block);
            self.block_len = 0;
        }
        self.block[self.block_len..56].fill(0);
        self.block[56..].copy_from_slice(&bit_len.to_be_bytes());
        let block = self.block;
        self.compress(&block);

        const HEX: &[u8; 16] = b"0123456789abcdef";
        let mut digest = String::with_capacity(64);
        for byte in self.state.into_iter().flat_map(u32::to_be_bytes) {
            digest.push(char::from(HEX[(byte >> 4) as usize]));
            digest.push(char::from(HEX[(byte & 0x0f) as usize]));
        }
        digest
    }

    fn compress(&mut self, block: &[u8; 64]) {
        let mut schedule = [0_u32; 64];
        for (word, bytes) in schedule[..16].iter_mut().zip(block.chunks_exact(4)) {
            *word = u32::from_be_bytes(
                <[u8; 4]>::try_from(bytes).expect("four-byte SHA256 word must convert"),
            );
        }
        for index in 16..64 {
            let s0 = schedule[index - 15].rotate_right(7)
                ^ schedule[index - 15].rotate_right(18)
                ^ (schedule[index - 15] >> 3);
            let s1 = schedule[index - 2].rotate_right(17)
                ^ schedule[index - 2].rotate_right(19)
                ^ (schedule[index - 2] >> 10);
            schedule[index] = schedule[index - 16]
                .wrapping_add(s0)
                .wrapping_add(schedule[index - 7])
                .wrapping_add(s1);
        }

        let [mut a, mut b, mut c, mut d, mut e, mut f, mut g, mut h] = self.state;
        for (&round_constant, &schedule_word) in Self::ROUND_CONSTANTS.iter().zip(&schedule) {
            let sum1 = e.rotate_right(6) ^ e.rotate_right(11) ^ e.rotate_right(25);
            let choice = (e & f) ^ (!e & g);
            let temporary1 = h
                .wrapping_add(sum1)
                .wrapping_add(choice)
                .wrapping_add(round_constant)
                .wrapping_add(schedule_word);
            let sum0 = a.rotate_right(2) ^ a.rotate_right(13) ^ a.rotate_right(22);
            let majority = (a & b) ^ (a & c) ^ (b & c);
            let temporary2 = sum0.wrapping_add(majority);
            h = g;
            g = f;
            f = e;
            e = d.wrapping_add(temporary1);
            d = c;
            c = b;
            b = a;
            a = temporary1.wrapping_add(temporary2);
        }
        for (state, value) in self.state.iter_mut().zip([a, b, c, d, e, f, g, h]) {
            *state = state.wrapping_add(value);
        }
    }
}

fn sha256_file(path: &Path) -> std::io::Result<String> {
    let mut file = File::open(path)?;
    let mut sha256 = Sha256::new();
    let mut buffer = [0_u8; 64 * 1024];
    loop {
        let count = file.read(&mut buffer)?;
        if count == 0 {
            break;
        }
        sha256.update(&buffer[..count]);
    }
    Ok(sha256.finish())
}

fn library_paths_f64() -> Result<ResolvedLibraryPair, &'static str> {
    #[cfg(target_os = "macos")]
    let candidates = [
        ("libfftw3.dylib", "libfftw3_threads.dylib"),
        ("libfftw3.3.dylib", "libfftw3_threads.3.dylib"),
    ];
    #[cfg(all(unix, not(target_os = "macos")))]
    let candidates = [
        ("libfftw3.so", "libfftw3_threads.so"),
        ("libfftw3.so.3", "libfftw3_threads.so.3"),
    ];
    #[cfg(unix)]
    {
        resolve_fftw_library_pair(&candidates)
    }
    #[cfg(not(unix))]
    {
        Err("fftw_runtime_libraries_not_supported")
    }
}

fn library_paths_f32() -> Result<ResolvedLibraryPair, &'static str> {
    #[cfg(target_os = "macos")]
    let candidates = [
        ("libfftw3f.dylib", "libfftw3f_threads.dylib"),
        ("libfftw3f.3.dylib", "libfftw3f_threads.3.dylib"),
    ];
    #[cfg(all(unix, not(target_os = "macos")))]
    let candidates = [
        ("libfftw3f.so", "libfftw3f_threads.so"),
        ("libfftw3f.so.3", "libfftw3f_threads.so.3"),
    ];
    #[cfg(unix)]
    {
        resolve_fftw_library_pair(&candidates)
    }
    #[cfg(not(unix))]
    {
        Err("fftw_runtime_libraries_not_supported")
    }
}

fn resolve_fftw_library_pair(
    candidates: &[(&str, &str)],
) -> Result<ResolvedLibraryPair, &'static str> {
    let explicit = env::var_os("CASA_RS_FFTW_LIBRARY_DIR");
    resolve_fftw_library_pair_from(
        explicit.as_deref(),
        candidates,
        conventional_fftw_library_directories(),
    )
}

fn resolve_fftw_library_pair_from(
    explicit: Option<&OsStr>,
    candidates: &[(&str, &str)],
    conventional_directories: impl IntoIterator<Item = PathBuf>,
) -> Result<ResolvedLibraryPair, &'static str> {
    if let Some(explicit) = explicit {
        if explicit.is_empty() {
            return Err("fftw_local_explicit_library_directory_empty");
        }
        return resolve_library_pair_in_directory(
            Path::new(&explicit),
            candidates,
            FftwResolutionSource::ExplicitDirectory,
        );
    }

    for directory in conventional_directories {
        if let Ok(resolved) = resolve_library_pair_in_directory(
            &directory,
            candidates,
            FftwResolutionSource::ConventionalDirectory,
        ) {
            return Ok(resolved);
        }
    }
    Err("fftw_runtime_libraries_not_found")
}

fn resolve_library_pair_in_directory(
    directory: &Path,
    candidates: &[(&str, &str)],
    source: FftwResolutionSource,
) -> Result<ResolvedLibraryPair, &'static str> {
    if !directory.is_dir() {
        return Err("fftw_local_library_directory_not_found");
    }
    for &(core_name, threads_name) in candidates {
        let core_path = directory.join(core_name);
        let threads_path = directory.join(threads_name);
        if !core_path.is_file() || !threads_path.is_file() {
            continue;
        }
        return Ok(ResolvedLibraryPair {
            source,
            core_path: core_path
                .canonicalize()
                .map_err(|_| "fftw_local_core_library_canonicalization_failed")?,
            threads_path: threads_path
                .canonicalize()
                .map_err(|_| "fftw_local_threads_library_canonicalization_failed")?,
        });
    }
    Err("fftw_local_library_pair_incomplete")
}

fn conventional_fftw_library_directories() -> impl Iterator<Item = PathBuf> {
    #[cfg(target_os = "macos")]
    let conventional = [
        "/opt/homebrew/opt/fftw/lib",
        "/usr/local/opt/fftw/lib",
        "/opt/local/lib",
    ];
    #[cfg(all(unix, not(target_os = "macos")))]
    let conventional = [
        "/usr/lib/x86_64-linux-gnu",
        "/usr/lib/aarch64-linux-gnu",
        "/usr/local/lib",
        "/usr/lib64",
        "/usr/lib",
    ];
    #[cfg(not(unix))]
    let conventional: [&str; 0] = [];
    conventional.into_iter().map(PathBuf::from)
}

fn thread_count() -> c_int {
    env::var("CASA_RS_FFTW_THREADS")
        .ok()
        .and_then(|value| value.parse::<c_int>().ok())
        .filter(|&value| value > 0)
        .unwrap_or_else(|| {
            std::thread::available_parallelism()
                .map(|count| count.get().min(c_int::MAX as usize) as c_int)
                .unwrap_or(1)
        })
}

fn shift_quadrants_in_place(input: &mut Array2<Complex64>) -> bool {
    let rows = input.shape()[0];
    let columns = input.shape()[1];
    if rows & 1 != 0 || columns & 1 != 0 {
        return false;
    }
    let Some(values) = input.as_slice_memory_order_mut() else {
        return false;
    };
    let half_rows = rows / 2;
    let half_columns = columns / 2;
    for row in 0..half_rows {
        for column in 0..half_columns {
            let q00 = row * columns + column;
            let q11 = (row + half_rows) * columns + column + half_columns;
            values.swap(q00, q11);

            let q10 = (row + half_rows) * columns + column;
            let q01 = row * columns + column + half_columns;
            values.swap(q10, q01);
        }
    }
    true
}

fn shift_quadrants_in_place_f32(input: &mut Array2<Complex32>) -> bool {
    let rows = input.shape()[0];
    let columns = input.shape()[1];
    if rows & 1 != 0 || columns & 1 != 0 {
        return false;
    }
    let Some(values) = input.as_slice_memory_order_mut() else {
        return false;
    };
    let half_rows = rows / 2;
    let half_columns = columns / 2;
    for row in 0..half_rows {
        for column in 0..half_columns {
            let q00 = row * columns + column;
            let q11 = (row + half_rows) * columns + column + half_columns;
            values.swap(q00, q11);

            let q10 = (row + half_rows) * columns + column;
            let q01 = row * columns + column + half_columns;
            values.swap(q10, q01);
        }
    }
    true
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sha256_matches_standard_vectors_across_chunk_boundaries() {
        let empty = Sha256::new().finish();
        assert_eq!(
            empty,
            "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
        );

        let mut abc = Sha256::new();
        abc.update(b"a");
        abc.update(b"bc");
        assert_eq!(
            abc.finish(),
            "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"
        );

        let mut million_a = Sha256::new();
        for _ in 0..1_000 {
            million_a.update(&[b'a'; 1_000]);
        }
        assert_eq!(
            million_a.finish(),
            "cdc76e5c9914fb9281a1c7e284d73e67f1809a48a497200e046d39ccc7112cd0"
        );
    }

    #[test]
    fn explicit_library_pair_resolution_is_canonical_and_complete() {
        let directory = tempfile::tempdir().expect("temporary FFTW directory must be created");
        let core = directory.path().join("lib-core");
        let threads = directory.path().join("lib-threads");
        std::fs::write(&core, b"core").expect("fake core library must be written");
        std::fs::write(&threads, b"threads").expect("fake threads library must be written");

        let resolved = resolve_library_pair_in_directory(
            directory.path(),
            &[("lib-core", "lib-threads")],
            FftwResolutionSource::ExplicitDirectory,
        )
        .expect("complete explicit pair must resolve");

        assert_eq!(resolved.source, FftwResolutionSource::ExplicitDirectory);
        assert_eq!(
            resolved.core_path,
            core.canonicalize().expect("core path must canonicalize")
        );
        assert_eq!(
            resolved.threads_path,
            threads
                .canonicalize()
                .expect("threads path must canonicalize")
        );
    }

    #[test]
    fn explicit_library_pair_resolution_fails_closed_when_incomplete() {
        let directory = tempfile::tempdir().expect("temporary FFTW directory must be created");
        std::fs::write(directory.path().join("lib-core"), b"core")
            .expect("fake core library must be written");

        assert_eq!(
            resolve_library_pair_in_directory(
                directory.path(),
                &[("lib-core", "lib-threads")],
                FftwResolutionSource::ExplicitDirectory,
            )
            .expect_err("incomplete explicit pair must fail"),
            "fftw_local_library_pair_incomplete"
        );
    }

    #[test]
    fn explicit_library_directory_never_falls_back_to_conventional_pair() {
        let explicit = tempfile::tempdir().expect("explicit FFTW directory must be created");
        std::fs::write(explicit.path().join("lib-core"), b"core")
            .expect("explicit fake core library must be written");
        let conventional =
            tempfile::tempdir().expect("conventional FFTW directory must be created");
        std::fs::write(conventional.path().join("lib-core"), b"core")
            .expect("conventional fake core library must be written");
        std::fs::write(conventional.path().join("lib-threads"), b"threads")
            .expect("conventional fake threads library must be written");

        assert_eq!(
            resolve_fftw_library_pair_from(
                Some(explicit.path().as_os_str()),
                &[("lib-core", "lib-threads")],
                [conventional.path().to_path_buf()],
            )
            .expect_err("an incomplete explicit directory must prevent fallback"),
            "fftw_local_library_pair_incomplete"
        );
    }

    #[test]
    fn provenance_receipt_is_single_token_key_value_data() {
        let library = FftwLibraryProvenance {
            resolution_source: FftwResolutionSource::ExplicitDirectory,
            core_path: PathBuf::from("/tmp/fftw 3/libfftw3f.dylib"),
            core_sha256: "core-hash".to_owned(),
            threads_path: PathBuf::from("/tmp/fftw 3/libfftw3f_threads.dylib"),
            threads_sha256: "threads-hash".to_owned(),
            version: "fftw-3.3.10".to_owned(),
            compiler: "Apple clang 17".to_owned(),
        };
        let configuration = FftwPlanConfiguration {
            flags: FFTW_WISDOM_ONLY,
            flags_name: "wisdom-only",
            wisdom_path: Some(PathBuf::from("/tmp/fftw wisdom")),
            wisdom_sha256: Some("wisdom-hash".to_owned()),
        };

        let line = provenance_receipt_line("f32", &library, 6, &configuration);
        assert!(line.starts_with("fftw_runtime_provenance "));
        assert!(line.contains("resolution=explicit-directory"));
        assert!(line.contains("core_path=/tmp/fftw%203/libfftw3f.dylib"));
        assert!(line.contains("compiler=Apple%20clang%2017"));
        assert!(line.contains("fft_threads=6"));
        assert!(line.contains("planner_flags=wisdom-only"));
        assert!(line.contains("planner_flags_bits=2097152"));
        assert!(line.contains("wisdom_path=/tmp/fftw%20wisdom"));
        assert!(
            line.split_ascii_whitespace()
                .skip(1)
                .all(|field| field.contains('='))
        );
    }

    #[test]
    fn casacore_layout_f32_preserves_logical_centered_transform() {
        if !configured_f32() {
            return;
        }
        let mut input = Array2::<Complex32>::zeros((6, 10));
        for ((x, y), value) in input.indexed_iter_mut() {
            *value = Complex32::new(
                (x as f32 * 0.31 + y as f32 * 0.17).sin(),
                (x as f32 * 0.11 - y as f32 * 0.23).cos(),
            );
        }

        let (native, _) =
            centered_transform_f32(&input, FftDirection::Forward, FftUseCase::Benchmark)
                .expect("configured native-layout f32 FFTW transform must run");
        let (casacore_layout, _) = centered_transform_f32_casacore_layout_with_threads(
            &input,
            FftDirection::Forward,
            FftUseCase::Benchmark,
            thread_count() as usize,
        )
        .expect("configured casacore-layout f32 FFTW transform must run");

        assert_eq!(casacore_layout.strides(), &[1, input.shape()[0] as isize]);
        let mut max_error = 0.0_f32;
        for x in 0..input.shape()[0] {
            for y in 0..input.shape()[1] {
                max_error = max_error.max((native[(x, y)] - casacore_layout[(x, y)]).norm());
            }
        }
        assert!(
            max_error <= 2.0e-5,
            "casacore-layout FFT changed logical transform by {max_error}"
        );

        let (round_trip, _) = centered_transform_f32_casacore_layout_with_threads(
            &casacore_layout,
            FftDirection::Inverse,
            FftUseCase::Benchmark,
            thread_count() as usize,
        )
        .expect("configured casacore-layout inverse f32 FFTW transform must run");
        let mut round_trip_max_error = 0.0_f32;
        for x in 0..input.shape()[0] {
            for y in 0..input.shape()[1] {
                round_trip_max_error =
                    round_trip_max_error.max((input[(x, y)] - round_trip[(x, y)]).norm());
            }
        }
        assert!(
            round_trip_max_error <= 2.0e-5,
            "casacore-layout FFT round trip error was {round_trip_max_error}"
        );
    }

    #[test]
    fn fft0_real_f32_round_trip_preserves_odd_first_axis() {
        if !configured_f32() {
            return;
        }
        let (nx, ny) = (5, 4);
        let mut input = Array2::<f32>::zeros((nx, ny).f());
        for ((x, y), value) in input.indexed_iter_mut() {
            *value = (x as f32 * 0.37 + y as f32 * 0.19).sin()
                + (x as f32 * 0.13 - y as f32 * 0.29).cos();
        }

        let spectrum = fft0_r2c_f32_casacore_layout(&input, 10)
            .expect("configured fft0 real-to-complex transform must run");
        let restored = fft0_c2r_f32_casacore_layout(&spectrum, 10)
            .expect("configured fft0 complex-to-real transform must run");

        assert_eq!(spectrum.logical_shape(), (nx, ny));
        assert_eq!(restored.dim(), (nx, ny));
        assert_eq!(restored.strides(), &[1, nx as isize]);
        let max_error = input
            .iter()
            .zip(restored.iter())
            .map(|(expected, actual)| (expected - actual).abs())
            .fold(0.0_f32, f32::max);
        assert!(
            max_error <= 2.0e-5,
            "fft0 real round trip error was {max_error}"
        );
    }

    #[test]
    fn fft0_real_f32_half_spectrum_has_casacore_axis_and_storage_layout() {
        if !configured_f32() {
            return;
        }
        let (nx, ny) = (6, 5);
        let (impulse_x, impulse_y) = (1, 2);
        let mut input = Array2::<f32>::zeros((nx, ny).f());
        input[(impulse_x, impulse_y)] = 1.0;

        let spectrum = fft0_r2c_f32_casacore_layout(&input, 1)
            .expect("configured fft0 real-to-complex transform must run");
        let values = spectrum.values();
        let half_nx = nx / 2 + 1;
        assert_eq!(values.dim(), (half_nx, ny));
        assert_eq!(values.strides(), &[1, half_nx as isize]);
        let physical = values
            .as_slice_memory_order()
            .expect("half spectrum must be physically contiguous");

        let mut max_error = 0.0_f32;
        for ky in 0..ny {
            for kx in 0..half_nx {
                let phase = -std::f32::consts::TAU
                    * (kx as f32 * impulse_x as f32 / nx as f32
                        + ky as f32 * impulse_y as f32 / ny as f32);
                let expected = Complex32::new(phase.cos(), phase.sin());
                let actual = values[(kx, ky)];
                assert_eq!(actual, physical[ky * half_nx + kx]);
                max_error = max_error.max((expected - actual).norm());
            }
        }
        assert!(
            max_error <= 2.0e-5,
            "reversed FFTW dimensions changed logical half-spectrum layout by {max_error}"
        );
    }

    #[test]
    fn fft0_real_f32_product_reconstructs_known_uncentered_circular_convolution() {
        if !configured_f32() {
            return;
        }
        let (nx, ny) = (4, 3);
        let mut lhs = Array2::<f32>::zeros((nx, ny).f());
        lhs[(1, 0)] = 2.0;
        lhs[(0, 1)] = -1.0;
        let mut rhs = Array2::<f32>::zeros((nx, ny).f());
        rhs[(0, 0)] = 3.0;
        rhs[(2, 1)] = 0.5;

        let mut product =
            fft0_r2c_f32_casacore_layout(&lhs, 1).expect("configured lhs fft0 transform must run");
        let rhs_spectrum =
            fft0_r2c_f32_casacore_layout(&rhs, 1).expect("configured rhs fft0 transform must run");
        for (left, right) in product.values_mut().iter_mut().zip(rhs_spectrum.values()) {
            *left *= *right;
        }
        let actual = fft0_c2r_f32_casacore_layout(&product, 1)
            .expect("configured product fft0 inverse transform must run");

        let mut expected = Array2::<f32>::zeros((nx, ny).f());
        expected[(1, 0)] = 6.0;
        expected[(3, 1)] = 1.0;
        expected[(0, 1)] = -3.0;
        expected[(2, 2)] = -0.5;
        let max_error = expected
            .iter()
            .zip(actual.iter())
            .map(|(expected, actual)| (expected - actual).abs())
            .fold(0.0_f32, f32::max);
        assert!(
            max_error <= 2.0e-5,
            "fft0 real known convolution error was {max_error}"
        );
    }

    #[test]
    fn benchmark_configured_f32_shape_when_requested() {
        let Some(shape) = env::var_os("CASA_RS_FFTW_TEST_SHAPE") else {
            return;
        };
        let shape = shape.to_string_lossy();
        let (rows, columns) = shape
            .split_once('x')
            .and_then(|(rows, columns)| {
                Some((rows.parse::<usize>().ok()?, columns.parse::<usize>().ok()?))
            })
            .expect("CASA_RS_FFTW_TEST_SHAPE must be ROWSxCOLUMNS");
        let mut input = Array2::<Complex32>::zeros((rows, columns));
        input[(rows / 2, columns / 2)] = Complex32::new(1.0, 0.0);

        let (output, timing) =
            centered_transform_f32_owned(input, FftDirection::Forward, FftUseCase::Benchmark)
                .expect("requested f32 FFTW benchmark must run");

        assert!(output[(0, 0)].re.is_finite());
        assert!(output[(rows / 2, columns / 2)].re.is_finite());
        eprintln!(
            "fftwf_requested_shape rows={} columns={} threads={} plan_ms={:.3} pack_ms={:.3} exec_ms={:.3} total_ms={:.3}",
            rows,
            columns,
            thread_count(),
            timing.plan.as_secs_f64() * 1.0e3,
            timing.pack.as_secs_f64() * 1.0e3,
            timing.exec.as_secs_f64() * 1.0e3,
            timing.total.as_secs_f64() * 1.0e3,
        );
    }
}

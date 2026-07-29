// SPDX-License-Identifier: LGPL-3.0-or-later
//! Dynamically loaded FFTW host backend.

use std::{
    env,
    ffi::{CString, c_char, c_int, c_uint, c_void},
    path::PathBuf,
    sync::{Mutex, OnceLock},
    time::Instant,
};

use libloading::Library;
use ndarray::Array2;
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
type FftwfExecute = unsafe extern "C" fn(FftwfPlan);
type FftwfDestroyPlan = unsafe extern "C" fn(FftwfPlan);
type FftwfImportWisdomFromFilename = unsafe extern "C" fn(*const c_char) -> c_int;

const FFTW_FORWARD: c_int = -1;
const FFTW_BACKWARD: c_int = 1;
const FFTW_ESTIMATE: c_uint = 1 << 6;
const FFTW_WISDOM_ONLY: c_uint = 1 << 21;

struct FftwApi {
    _core: Library,
    _threads: Library,
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
    init_threads: FftwfInitThreads,
    plan_with_threads: FftwfPlanWithThreads,
    plan_dft: FftwfPlanDft,
    execute: FftwfExecute,
    destroy_plan: FftwfDestroyPlan,
    import_wisdom_from_filename: FftwfImportWisdomFromFilename,
}

static FFTW_API: OnceLock<FftwApi> = OnceLock::new();
static FFTWF_API: OnceLock<FftwfApi> = OnceLock::new();
static FFTW_PLAN_LOCK: Mutex<()> = Mutex::new(());

pub(crate) fn configured() -> bool {
    library_paths_f64().is_some() && api().is_ok()
}

pub(crate) fn configured_f32() -> bool {
    library_paths_f32().is_some() && api_f32().is_ok()
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
            let flags = if let Some(path) = env::var_os("CASA_RS_EXPERIMENTAL_FFTW_F32_WISDOM") {
                let path = CString::new(path.as_encoded_bytes())
                    .map_err(|_| "fftw_local_wisdom_path_contains_nul")?;
                if (api.import_wisdom_from_filename)(path.as_ptr()) == 0 {
                    return Err("fftw_local_wisdom_import_failed");
                }
                FFTW_WISDOM_ONLY
            } else {
                FFTW_ESTIMATE
            };
            (api.plan_dft)(
                2,
                dimensions.as_ptr(),
                output_pointer,
                output_pointer,
                sign,
                flags,
            )
        }
    };
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
            (api.plan_with_threads)(thread_count());
            let flags = if let Some(path) = env::var_os("CASA_RS_EXPERIMENTAL_FFTW_F64_WISDOM") {
                let path = CString::new(path.as_encoded_bytes())
                    .map_err(|_| "fftw_local_wisdom_path_contains_nul")?;
                if (api.import_wisdom_from_filename)(path.as_ptr()) == 0 {
                    return Err("fftw_local_wisdom_import_failed");
                }
                FFTW_WISDOM_ONLY
            } else {
                FFTW_ESTIMATE
            };
            (api.plan_dft)(
                2,
                dimensions.as_ptr(),
                output_pointer,
                output_pointer,
                sign,
                flags,
            )
        }
    };
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
    let (core_path, threads_path) =
        library_paths_f64().ok_or("fftw_runtime_libraries_not_found")?;
    // SAFETY: loading is restricted to an explicit operator-supplied
    // directory or a conventional system package-manager directory.
    let core =
        unsafe { Library::new(core_path) }.map_err(|_| "fftw_local_core_library_load_failed")?;
    // SAFETY: same resolved runtime-library boundary as the core library.
    let threads = unsafe { Library::new(threads_path) }
        .map_err(|_| "fftw_local_threads_library_load_failed")?;

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
    let (core_path, threads_path) =
        library_paths_f32().ok_or("fftw_runtime_libraries_not_found")?;
    // SAFETY: loading is restricted to an explicit operator-supplied
    // directory or a conventional system package-manager directory.
    let core =
        unsafe { Library::new(core_path) }.map_err(|_| "fftw_local_core_library_load_failed")?;
    // SAFETY: same resolved runtime-library boundary as the core library.
    let threads = unsafe { Library::new(threads_path) }
        .map_err(|_| "fftw_local_threads_library_load_failed")?;

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
            init_threads,
            plan_with_threads,
            plan_dft,
            execute,
            destroy_plan,
            import_wisdom_from_filename,
        })
    }
}

fn library_paths_f64() -> Option<(PathBuf, PathBuf)> {
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
    #[cfg(not(unix))]
    return None;
    fftw_library_directories().find_map(|directory| {
        candidates
            .iter()
            .map(|names| (directory.join(names.0), directory.join(names.1)))
            .find(|(core, threads)| core.is_file() && threads.is_file())
    })
}

fn library_paths_f32() -> Option<(PathBuf, PathBuf)> {
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
    #[cfg(not(unix))]
    return None;
    fftw_library_directories().find_map(|directory| {
        candidates
            .iter()
            .map(|names| (directory.join(names.0), directory.join(names.1)))
            .find(|(core, threads)| core.is_file() && threads.is_file())
    })
}

fn fftw_library_directories() -> impl Iterator<Item = PathBuf> {
    let explicit = env::var_os("CASA_RS_FFTW_LIBRARY_DIR").map(PathBuf::from);
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
    explicit
        .into_iter()
        .chain(conventional.into_iter().map(PathBuf::from))
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

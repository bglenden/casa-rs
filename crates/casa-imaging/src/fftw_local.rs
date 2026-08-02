// SPDX-License-Identifier: LGPL-3.0-or-later
//! Explicit local-only FFTW experiment adapter.

use std::{
    env,
    ffi::{c_int, c_uint, c_void},
    path::PathBuf,
    sync::{Mutex, OnceLock},
    time::Instant,
};

use libloading::Library;
use ndarray::Array2;
use num_complex::Complex64;

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

const FFTW_FORWARD: c_int = -1;
const FFTW_BACKWARD: c_int = 1;
const FFTW_ESTIMATE: c_uint = 1 << 6;

struct FftwApi {
    _core: Library,
    _threads: Library,
    init_threads: FftwInitThreads,
    plan_with_threads: FftwPlanWithThreads,
    plan_dft: FftwPlanDft,
    execute: FftwExecute,
    destroy_plan: FftwDestroyPlan,
}

static FFTW_API: OnceLock<FftwApi> = OnceLock::new();
static FFTW_PLAN_LOCK: Mutex<()> = Mutex::new(());

pub(crate) fn configured() -> bool {
    library_paths().is_some() && api().is_ok()
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
        FftBackendChoice::FftwLocalBench,
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
            (api.plan_dft)(
                2,
                dimensions.as_ptr(),
                output_pointer,
                output_pointer,
                sign,
                FFTW_ESTIMATE,
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

fn load_api() -> Result<FftwApi, &'static str> {
    let (core_path, threads_path) = library_paths().ok_or("set_CASA_RS_FFTW_LIBRARY_DIR")?;
    // SAFETY: loading is explicit-only from an operator-supplied directory.
    let core =
        unsafe { Library::new(core_path) }.map_err(|_| "fftw_local_core_library_load_failed")?;
    // SAFETY: same explicit local-only experiment boundary as the core library.
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
        Ok(FftwApi {
            _core: core,
            _threads: threads,
            init_threads,
            plan_with_threads,
            plan_dft,
            execute,
            destroy_plan,
        })
    }
}

fn library_paths() -> Option<(PathBuf, PathBuf)> {
    let directory = env::var_os("CASA_RS_FFTW_LIBRARY_DIR").map(PathBuf::from)?;
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
    candidates
        .into_iter()
        .map(|names| (directory.join(names.0), directory.join(names.1)))
        .find(|(core, threads)| core.is_file() && threads.is_file())
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

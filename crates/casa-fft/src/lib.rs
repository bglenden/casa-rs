// SPDX-License-Identifier: LGPL-3.0-or-later
#![warn(missing_docs)]
//! Direct, reusable two-dimensional FFTW plans for real and complex planes.
//!
//! The native FFTW library is GPL-2.0-or-later. Distributions linking these
//! plans must satisfy GPLv3 terms for the combined program.

use std::collections::{HashMap, VecDeque};
use std::ffi::{c_int, c_uint, c_void};
use std::fmt;
use std::marker::PhantomData;
use std::sync::{Arc, LazyLock, Mutex, OnceLock};

use num_complex::Complex;

const FORWARD: c_int = -1;
const BACKWARD: c_int = 1;
const MEASURE: c_uint = 0;
const ESTIMATE: c_uint = 1 << 6;

static PLANNER: Mutex<()> = Mutex::new(());
static F32_PLANS: LazyLock<Mutex<PlanCache<f32>>> =
    LazyLock::new(|| Mutex::new(PlanCache::default()));
static F64_PLANS: LazyLock<Mutex<PlanCache<f64>>> =
    LazyLock::new(|| Mutex::new(PlanCache::default()));
static F32_THREADS: OnceLock<c_int> = OnceLock::new();
static F64_THREADS: OnceLock<c_int> = OnceLock::new();

// Temporary product/analysis callers drop their Fft2 after each plane. Retain
// a small bounded set of measured plans rather than replanning every channel.
const CACHED_PLAN_PAIRS_PER_PRECISION: usize = 8;

struct PlanCache<T: FftScalar> {
    plans: HashMap<Key, Arc<Plans<T>>>,
    oldest_first: VecDeque<Key>,
}

impl<T: FftScalar> Default for PlanCache<T> {
    fn default() -> Self {
        Self {
            plans: HashMap::new(),
            oldest_first: VecDeque::new(),
        }
    }
}

impl<T: FftScalar> PlanCache<T> {
    fn get_or_create(&mut self, key: Key, elements: usize) -> Result<Arc<Plans<T>>, FftError> {
        if let Some(plans) = self.plans.get(&key) {
            self.oldest_first.retain(|cached| *cached != key);
            self.oldest_first.push_back(key);
            return Ok(Arc::clone(plans));
        }
        let plans = Arc::new(Plans::<T>::create(key, elements)?);
        self.plans.insert(key, Arc::clone(&plans));
        self.oldest_first.push_back(key);
        if self.plans.len() > CACHED_PLAN_PAIRS_PER_PRECISION {
            let expired = self.oldest_first.pop_front().expect("nonempty cache");
            self.plans.remove(&expired);
        }
        Ok(plans)
    }
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
#[doc(hidden)]
pub struct Key {
    shape: [usize; 2],
    alignment: c_int,
    threads: usize,
    real: bool,
    estimate: bool,
}

/// A shape, layout, or native planner error.
#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum FftError {
    /// An extent is zero, exceeds FFTW's signed dimension range, or overflows.
    InvalidShape,
    /// The supplied storage does not match the transform's required shape or layout.
    InvalidPlane,
    /// FFTW could not create the requested plan.
    PlanningFailed,
    /// The requested thread count is invalid or FFTW threading initialization failed.
    InvalidThreads,
}

impl fmt::Display for FftError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "FFTW {self:?}")
    }
}

impl std::error::Error for FftError {}

mod sealed {
    pub trait Sealed {}
    impl Sealed for f32 {}
    impl Sealed for f64 {}
}

/// The two supported FFTW scalar precisions. This trait is sealed.
pub trait FftScalar: sealed::Sealed + Copy + Default + Send + Sync + 'static {
    #[doc(hidden)]
    fn alignment(pointer: *mut Complex<Self>) -> c_int;
    #[doc(hidden)]
    unsafe fn plan(
        shape: [c_int; 2],
        pointer: *mut Complex<Self>,
        sign: c_int,
        flags: c_uint,
    ) -> *mut c_void;
    #[doc(hidden)]
    unsafe fn execute(plan: *mut c_void, pointer: *mut Complex<Self>);
    #[doc(hidden)]
    unsafe fn plan_real(
        shape: [c_int; 2],
        pointer: *mut Complex<Self>,
        inverse: bool,
        flags: c_uint,
    ) -> *mut c_void;
    #[doc(hidden)]
    unsafe fn execute_real(plan: *mut c_void, pointer: *mut Complex<Self>, inverse: bool);
    #[doc(hidden)]
    unsafe fn destroy(plan: *mut c_void);
    #[doc(hidden)]
    unsafe fn init_threads() -> c_int;
    #[doc(hidden)]
    unsafe fn set_threads(count: c_int);
    #[doc(hidden)]
    fn get_plan(key: Key, elements: usize) -> Result<Arc<Plans<Self>>, FftError>;
}

macro_rules! ffi {
    ($module:ident, $scalar:ty,
     $plan:literal, $execute:literal, $destroy:literal,
     $alignment:literal, $init:literal, $set:literal,
     $plan_r2c:literal, $plan_c2r:literal, $execute_r2c:literal, $execute_c2r:literal,
     $cache:ident, $threads:ident) => {
        mod $module {
            use super::*;
            unsafe extern "C" {
                #[link_name = $plan]
                pub fn plan_dft_2d(
                    n0: c_int,
                    n1: c_int,
                    input: *mut Complex<$scalar>,
                    output: *mut Complex<$scalar>,
                    sign: c_int,
                    flags: c_uint,
                ) -> *mut c_void;
                #[link_name = $execute]
                pub fn execute_dft(
                    plan: *mut c_void,
                    input: *mut Complex<$scalar>,
                    output: *mut Complex<$scalar>,
                );
                #[link_name = $destroy]
                pub fn destroy_plan(plan: *mut c_void);
                #[link_name = $alignment]
                pub fn alignment_of(pointer: *mut $scalar) -> c_int;
                #[link_name = $plan_r2c]
                pub fn plan_r2c(
                    n0: c_int,
                    n1: c_int,
                    input: *mut $scalar,
                    output: *mut Complex<$scalar>,
                    flags: c_uint,
                ) -> *mut c_void;
                #[link_name = $plan_c2r]
                pub fn plan_c2r(
                    n0: c_int,
                    n1: c_int,
                    input: *mut Complex<$scalar>,
                    output: *mut $scalar,
                    flags: c_uint,
                ) -> *mut c_void;
                #[link_name = $execute_r2c]
                pub fn execute_r2c(
                    plan: *mut c_void,
                    input: *mut $scalar,
                    output: *mut Complex<$scalar>,
                );
                #[link_name = $execute_c2r]
                pub fn execute_c2r(
                    plan: *mut c_void,
                    input: *mut Complex<$scalar>,
                    output: *mut $scalar,
                );
            }
            unsafe extern "C" {
                #[link_name = $init]
                pub fn init_threads() -> c_int;
                #[link_name = $set]
                pub fn plan_with_nthreads(count: c_int);
            }
        }

        impl FftScalar for $scalar {
            fn alignment(pointer: *mut Complex<Self>) -> c_int {
                // SAFETY: Complex<T> is repr(C) with adjacent real/imaginary T.
                unsafe { $module::alignment_of(pointer.cast()) }
            }
            unsafe fn plan(
                shape: [c_int; 2],
                pointer: *mut Complex<Self>,
                sign: c_int,
                flags: c_uint,
            ) -> *mut c_void {
                // SAFETY: caller owns a writable, shape-sized, aligned scratch plane.
                unsafe { $module::plan_dft_2d(shape[0], shape[1], pointer, pointer, sign, flags) }
            }
            unsafe fn execute(plan: *mut c_void, pointer: *mut Complex<Self>) {
                // SAFETY: caller matched FFTW's rank, strides, in-place layout and alignment.
                unsafe { $module::execute_dft(plan, pointer, pointer) }
            }
            unsafe fn plan_real(
                shape: [c_int; 2],
                pointer: *mut Complex<Self>,
                inverse: bool,
                flags: c_uint,
            ) -> *mut c_void {
                // SAFETY: caller owns the padded in-place real/half-complex plane.
                unsafe {
                    if inverse {
                        $module::plan_c2r(shape[0], shape[1], pointer, pointer.cast(), flags)
                    } else {
                        $module::plan_r2c(shape[0], shape[1], pointer.cast(), pointer, flags)
                    }
                }
            }
            unsafe fn execute_real(plan: *mut c_void, pointer: *mut Complex<Self>, inverse: bool) {
                // SAFETY: caller matched logical shape, padded strides and alignment.
                unsafe {
                    if inverse {
                        $module::execute_c2r(plan, pointer, pointer.cast());
                    } else {
                        $module::execute_r2c(plan, pointer.cast(), pointer);
                    }
                }
            }
            unsafe fn destroy(plan: *mut c_void) {
                // SAFETY: plan was created by the matching FFTW precision.
                unsafe { $module::destroy_plan(plan) }
            }
            unsafe fn init_threads() -> c_int {
                // SAFETY: serialized by PLANNER.
                *$threads.get_or_init(|| unsafe { $module::init_threads() })
            }
            unsafe fn set_threads(count: c_int) {
                // SAFETY: serialized by PLANNER.
                unsafe { $module::plan_with_nthreads(count) }
            }
            fn get_plan(key: Key, elements: usize) -> Result<Arc<Plans<Self>>, FftError> {
                $cache
                    .lock()
                    .expect("FFTW plan cache lock poisoned")
                    .get_or_create(key, elements)
            }
        }
    };
}

ffi!(
    single,
    f32,
    "fftwf_plan_dft_2d",
    "fftwf_execute_dft",
    "fftwf_destroy_plan",
    "fftwf_alignment_of",
    "fftwf_init_threads",
    "fftwf_plan_with_nthreads",
    "fftwf_plan_dft_r2c_2d",
    "fftwf_plan_dft_c2r_2d",
    "fftwf_execute_dft_r2c",
    "fftwf_execute_dft_c2r",
    F32_PLANS,
    F32_THREADS
);
ffi!(
    double,
    f64,
    "fftw_plan_dft_2d",
    "fftw_execute_dft",
    "fftw_destroy_plan",
    "fftw_alignment_of",
    "fftw_init_threads",
    "fftw_plan_with_nthreads",
    "fftw_plan_dft_r2c_2d",
    "fftw_plan_dft_c2r_2d",
    "fftw_execute_dft_r2c",
    "fftw_execute_dft_c2r",
    F64_PLANS,
    F64_THREADS
);

#[doc(hidden)]
pub struct Plans<T: FftScalar> {
    forward: *mut c_void,
    inverse: *mut c_void,
    _precision: PhantomData<T>,
}

// FFTW explicitly permits concurrent new-array execution of one immutable plan.
unsafe impl<T: FftScalar> Send for Plans<T> {}
unsafe impl<T: FftScalar> Sync for Plans<T> {}

impl<T: FftScalar> Drop for Plans<T> {
    fn drop(&mut self) {
        let _guard = PLANNER.lock().expect("FFTW planner lock poisoned");
        // SAFETY: these owned plans have no remaining Arc execution owners.
        unsafe {
            T::destroy(self.forward);
            T::destroy(self.inverse);
        }
    }
}

impl<T: FftScalar> Plans<T> {
    fn create(key: Key, elements: usize) -> Result<Self, FftError> {
        let _guard = PLANNER.lock().expect("FFTW planner lock poisoned");
        let threads = c_int::try_from(key.threads).map_err(|_| FftError::InvalidThreads)?;
        // SAFETY: FFTW planner and global thread settings are serialized.
        if unsafe { T::init_threads() } == 0 {
            return Err(FftError::InvalidThreads);
        }
        // SAFETY: FFTW planner and global thread settings are serialized.
        unsafe { T::set_threads(threads) };
        let mut scratch = vec![Complex::<T>::default(); elements + 64];
        let pointer = (0..64)
            .map(|offset| {
                // SAFETY: the allocation contains at least elements + 64 entries.
                unsafe { scratch.as_mut_ptr().add(offset) }
            })
            .find(|&pointer| T::alignment(pointer) == key.alignment)
            .ok_or(FftError::PlanningFailed)?;
        let shape = key
            .shape
            .map(|extent| c_int::try_from(extent).expect("validated extent"));
        let flags = if key.estimate { ESTIMATE } else { MEASURE };
        // SAFETY: pointer addresses a writable scratch plane of the requested shape.
        let forward = unsafe {
            if key.real {
                T::plan_real(shape, pointer, false, flags)
            } else {
                T::plan(shape, pointer, FORWARD, flags)
            }
        };
        if forward.is_null() {
            return Err(FftError::PlanningFailed);
        }
        // SAFETY: the planner may overwrite scratch; it remains disposable.
        let inverse = unsafe {
            if key.real {
                T::plan_real(shape, pointer, true, flags)
            } else {
                T::plan(shape, pointer, BACKWARD, flags)
            }
        };
        if inverse.is_null() {
            // SAFETY: the first plan belongs to this precision.
            unsafe { T::destroy(forward) };
            return Err(FftError::PlanningFailed);
        }
        Ok(Self {
            forward,
            inverse,
            _precision: PhantomData,
        })
    }
}

/// A reusable direct in-place two-dimensional complex FFT. FFTW transforms
/// are unnormalized; callers own centering and inverse normalization.
pub struct Fft2<T: FftScalar> {
    shape: [usize; 2],
    elements: usize,
    threads: usize,
    estimate: bool,
    current: Option<(Key, Arc<Plans<T>>)>,
}

impl<T: FftScalar> fmt::Debug for Fft2<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Fft2")
            .field("shape", &self.shape)
            .field("threads", &self.threads)
            .field("estimate", &self.estimate)
            .finish()
    }
}

impl<T: FftScalar> Fft2<T> {
    /// Transform dimensions in row-major order.
    pub fn shape(&self) -> [usize; 2] {
        self.shape
    }

    /// Validate a two-dimensional shape and read the explicitly configured
    /// number of native FFT threads (one unless `CASA_RS_FFT_THREADS` is set).
    pub fn new(shape: [usize; 2]) -> Result<Self, FftError> {
        let threads = match std::env::var("CASA_RS_FFT_THREADS") {
            Ok(value) => value
                .parse::<usize>()
                .map_err(|_| FftError::InvalidThreads)?,
            Err(std::env::VarError::NotPresent) => 1,
            Err(_) => return Err(FftError::InvalidThreads),
        };
        Self::with_threads(shape, threads)
    }

    /// Construct a transform with an explicit native FFT thread count.
    pub fn with_threads(shape: [usize; 2], threads: usize) -> Result<Self, FftError> {
        let elements = shape[0]
            .checked_mul(shape[1])
            .ok_or(FftError::InvalidShape)?;
        if shape.contains(&0)
            || shape.iter().any(|&n| c_int::try_from(n).is_err())
            || elements.checked_add(64).is_none()
        {
            return Err(FftError::InvalidShape);
        }
        if threads == 0 || c_int::try_from(threads).is_err() {
            return Err(FftError::InvalidThreads);
        }
        Ok(Self {
            shape,
            elements,
            threads,
            estimate: false,
            current: None,
        })
    }

    /// Select heuristic planning for short-lived operators with few transforms.
    /// The backend, transform semantics, and bounded cache remain unchanged.
    #[doc(hidden)]
    pub fn with_estimated_plan(mut self) -> Self {
        self.estimate = true;
        self.current = None;
        self
    }

    /// Execute a rank-two transform directly on the caller's contiguous plane.
    pub fn transform(&mut self, plane: &mut [Complex<T>], inverse: bool) -> Result<(), FftError> {
        self.transform_storage(plane, inverse, false)
    }

    fn transform_storage(
        &mut self,
        plane: &mut [Complex<T>],
        inverse: bool,
        real: bool,
    ) -> Result<(), FftError> {
        if plane.len() != self.elements {
            return Err(FftError::InvalidPlane);
        }
        let pointer = plane.as_mut_ptr();
        let key = Key {
            shape: self.shape,
            alignment: T::alignment(pointer),
            threads: self.threads,
            real,
            estimate: self.estimate,
        };
        if self.current.as_ref().is_none_or(|(held, _)| *held != key) {
            let plans = T::get_plan(key, self.elements)?;
            self.current = Some((key, plans));
        }
        let plans = &self.current.as_ref().expect("plan exists").1;
        let plan = if inverse {
            plans.inverse
        } else {
            plans.forward
        };
        // SAFETY: key verifies equal rank, contiguous strides, in-place layout,
        // precision, and FFTW alignment class. The plane is uniquely borrowed.
        unsafe {
            if real {
                T::execute_real(plan, pointer, inverse);
            } else {
                T::execute(plan, pointer);
            }
        }
        Ok(())
    }

    /// Number of FFTW threads configured for each transform.
    pub fn threads(&self) -> usize {
        self.threads
    }
}

/// Reusable in-place rank-two real/Hermitian FFTW transforms.
///
/// The caller owns `shape[0] * (shape[1] / 2 + 1)` complex values. Before a
/// forward transform and after an inverse, interpret that allocation as real
/// scalars with row stride `2 * (shape[1] / 2 + 1)`, ignoring end-of-row padding.
/// In frequency space it is a contiguous Hermitian half-spectrum, reduced on
/// the last axis. Both directions are unnormalized. Callers preserve Hermitian
/// boundary constraints when modifying the spectrum before an inverse.
///
/// Planning, caching, alignment and worker-thread settings are shared with
/// [`Fft2`], but real and complex plans have distinct cache keys.
#[derive(Debug)]
pub struct RealFft2<T: FftScalar> {
    fft: Fft2<T>,
}

impl<T: FftScalar> RealFft2<T> {
    /// Validate the logical shape and use the configured native FFT thread count.
    pub fn new(shape: [usize; 2]) -> Result<Self, FftError> {
        Self::from_fft(Fft2::new(shape)?)
    }

    /// Validate the logical shape with an explicit native FFT thread count.
    pub fn with_threads(shape: [usize; 2], threads: usize) -> Result<Self, FftError> {
        Self::from_fft(Fft2::with_threads(shape, threads)?)
    }

    /// Use FFTW's inexpensive planning policy for a bounded transform sequence.
    pub fn with_estimated_plan(mut self) -> Self {
        self.fft = self.fft.with_estimated_plan();
        self
    }

    fn from_fft(mut fft: Fft2<T>) -> Result<Self, FftError> {
        fft.elements = Self::spectrum_len(fft.shape).ok_or(FftError::InvalidShape)?;
        Ok(Self { fft })
    }

    /// Complex values of the shared real/spectrum allocation of a `shape`
    /// transform ([`Self::storage_len`]), for sizing one before it exists;
    /// `None` when the count overflows.
    #[must_use]
    pub const fn spectrum_len(shape: [usize; 2]) -> Option<usize> {
        shape[0].checked_mul(shape[1] / 2 + 1)
    }

    /// Number of complex values required by the shared real/spectrum allocation.
    pub fn storage_len(&self) -> usize {
        self.fft.elements
    }

    /// Physical number of real scalars per row, including FFTW's tail padding.
    pub fn real_row_stride(&self) -> usize {
        2 * (self.fft.shape[1] / 2 + 1)
    }

    /// Replace a padded real plane with its Hermitian half-spectrum, in place.
    pub fn forward(&mut self, storage: &mut [Complex<T>]) -> Result<(), FftError> {
        self.fft.transform_storage(storage, false, true)
    }

    /// Replace a Hermitian half-spectrum with an unnormalized padded real plane.
    pub fn inverse(&mut self, storage: &mut [Complex<T>]) -> Result<(), FftError> {
        self.fft.transform_storage(storage, true, true)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::thread;

    fn direct(input: &[Complex<f64>], shape: [usize; 2], inverse: bool) -> Vec<Complex<f64>> {
        let sign = if inverse { 1.0 } else { -1.0 };
        (0..shape[0])
            .flat_map(|u| {
                (0..shape[1]).map(move |v| {
                    input
                        .iter()
                        .enumerate()
                        .map(|(index, &value)| {
                            let x = index / shape[1];
                            let y = index % shape[1];
                            let angle = sign
                                * 2.0
                                * std::f64::consts::PI
                                * ((u * x) as f64 / shape[0] as f64
                                    + (v * y) as f64 / shape[1] as f64);
                            value * Complex::new(angle.cos(), angle.sin())
                        })
                        .sum()
                })
            })
            .collect()
    }

    #[test]
    fn rectangular_odd_rank_two_transform_matches_direct_dft_and_roundtrip() {
        let shape = [3, 5];
        let initial: Vec<_> = (0..15)
            .map(|i| Complex::new(i as f64 / 7.0, (i % 4) as f64))
            .collect();
        let mut actual = initial.clone();
        let mut fft = Fft2::<f64>::with_threads(shape, 1).unwrap();
        fft.transform(&mut actual, false).unwrap();
        let expected = direct(&initial, shape, false);
        for (left, right) in actual.iter().zip(expected) {
            assert!((*left - right).norm() < 1e-11);
        }
        fft.transform(&mut actual, true).unwrap();
        for (left, right) in actual.iter().zip(initial) {
            assert!((*left / 15.0 - right).norm() < 1e-11);
        }
    }

    #[test]
    fn real_half_spectrum_matches_direct_dft_and_padded_roundtrip() {
        for shape in [[3, 5], [4, 6], [1, 1], [5, 4]] {
            let mut fft = RealFft2::<f64>::with_threads(shape, 1).unwrap();
            let stride = fft.real_row_stride();
            let initial: Vec<_> = (0..shape[0] * shape[1])
                .map(|i| Complex::new((i % 7) as f64 - 2.5, 0.0))
                .collect();
            let mut storage = vec![Complex::default(); fft.storage_len()];
            for x in 0..shape[0] {
                for y in 0..shape[1] {
                    let offset = x * stride + y;
                    let value = initial[x * shape[1] + y].re;
                    if offset.is_multiple_of(2) {
                        storage[offset / 2].re = value;
                    } else {
                        storage[offset / 2].im = value;
                    }
                }
            }
            fft.forward(&mut storage).unwrap();
            let expected = direct(&initial, shape, false);
            for x in 0..shape[0] {
                for y in 0..shape[1] / 2 + 1 {
                    assert!(
                        (storage[x * (stride / 2) + y] - expected[x * shape[1] + y]).norm() < 1e-10
                    );
                }
            }
            fft.inverse(&mut storage).unwrap();
            for x in 0..shape[0] {
                for y in 0..shape[1] {
                    let offset = x * stride + y;
                    let actual = if offset.is_multiple_of(2) {
                        storage[offset / 2].re
                    } else {
                        storage[offset / 2].im
                    };
                    assert!(
                        (actual / initial.len() as f64 - initial[x * shape[1] + y].re).abs()
                            < 1e-11
                    );
                }
            }
            assert_eq!(fft.forward(&mut []), Err(FftError::InvalidPlane));
        }
    }

    #[test]
    fn real_single_precision_plans_reuse_concurrently_without_complex_cache_alias() {
        let handles: Vec<_> = (0..4)
            .map(|_| {
                thread::spawn(|| {
                    let shape = [16, 18];
                    let mut fft = RealFft2::<f32>::with_threads(shape, 1).unwrap();
                    let mut storage = vec![Complex::<f32>::new(1.0, 1.0); fft.storage_len()];
                    fft.forward(&mut storage).unwrap();
                    assert!((storage[0].re - 288.0).abs() < 1e-4);
                    let retained = Arc::clone(&fft.fft.current.as_ref().unwrap().1);
                    let mut another = RealFft2::<f32>::with_threads(shape, 1).unwrap();
                    another.inverse(&mut storage).unwrap();
                    assert!(Arc::ptr_eq(
                        &retained,
                        &another.fft.current.as_ref().unwrap().1
                    ));
                    for row in storage.chunks_exact(fft.real_row_stride() / 2) {
                        for value in &row[..shape[1] / 2] {
                            assert!((value.re / 288.0 - 1.0).abs() < 1e-5);
                            assert!((value.im / 288.0 - 1.0).abs() < 1e-5);
                        }
                    }
                    let mut complex = Fft2::<f32>::with_threads(shape, 1).unwrap();
                    let mut full = vec![Complex::<f32>::default(); 288];
                    complex.transform(&mut full, false).unwrap();
                    assert!(!Arc::ptr_eq(
                        &retained,
                        &complex.current.as_ref().unwrap().1
                    ));
                })
            })
            .collect();
        for handle in handles {
            handle.join().unwrap();
        }
    }

    #[test]
    fn single_precision_concurrent_new_array_execution() {
        let handles: Vec<_> = (0..4)
            .map(|_| {
                thread::spawn(|| {
                    let mut fft = Fft2::<f32>::with_threads([16, 18], 1).unwrap();
                    let mut plane: Vec<_> = (0..288)
                        .map(|i| Complex::new((i % 13) as f32, (i % 7) as f32))
                        .collect();
                    let initial = plane.clone();
                    fft.transform(&mut plane, false).unwrap();
                    fft.transform(&mut plane, true).unwrap();
                    for (a, b) in plane.iter().zip(initial) {
                        assert!((*a / 288.0 - b).norm() < 1e-4);
                    }
                })
            })
            .collect();
        for handle in handles {
            handle.join().unwrap();
        }
    }

    #[test]
    fn temporary_callers_reuse_a_measured_plan() {
        let mut plane = vec![Complex::<f64>::new(1.0, 0.0); 37 * 43];
        let mut first = Fft2::<f64>::new([37, 43]).unwrap();
        first.transform(&mut plane, false).unwrap();
        let retained = Arc::clone(&first.current.as_ref().unwrap().1);
        drop(first);
        let mut second = Fft2::<f64>::new([37, 43]).unwrap();
        second.transform(&mut plane, true).unwrap();
        assert!(Arc::ptr_eq(&retained, &second.current.as_ref().unwrap().1));
    }

    #[test]
    fn estimated_plan_matches_dft_and_does_not_alias_measured_cache() {
        let shape = [5, 7];
        let initial: Vec<_> = (0..35)
            .map(|i| Complex::new((i % 11) as f64 / 3.0, (i % 7) as f64 / 5.0))
            .collect();
        let mut actual = initial.clone();
        let mut fft = Fft2::<f64>::with_threads(shape, 1)
            .unwrap()
            .with_estimated_plan();
        fft.transform(&mut actual, false).unwrap();
        for (actual, expected) in actual.iter().zip(direct(&initial, shape, false)) {
            assert!((*actual - expected).norm() < 1e-10);
        }
        let estimated = Arc::clone(&fft.current.as_ref().unwrap().1);
        fft.transform(&mut actual, true).unwrap();
        for (actual, expected) in actual.iter().zip(&initial) {
            assert!((*actual / 35.0 - expected).norm() < 1e-11);
        }
        let mut measured = Fft2::<f64>::with_threads(shape, 1).unwrap();
        measured.transform(&mut actual, false).unwrap();
        assert!(!Arc::ptr_eq(
            &estimated,
            &measured.current.as_ref().unwrap().1
        ));
        let mut reused = Fft2::<f64>::with_threads(shape, 1)
            .unwrap()
            .with_estimated_plan();
        reused.transform(&mut actual, true).unwrap();
        assert!(Arc::ptr_eq(&estimated, &reused.current.as_ref().unwrap().1));
    }

    #[test]
    fn offset_new_array_uses_its_actual_alignment_class() {
        let mut storage: Vec<_> = (0..17)
            .map(|index| Complex::new(index as f32 / 3.0, (index % 5) as f32))
            .collect();
        let expected = storage[1..].to_vec();
        let mut fft = Fft2::<f32>::with_threads([4, 4], 1).unwrap();
        fft.transform(&mut storage[1..], false).unwrap();
        fft.transform(&mut storage[1..], true).unwrap();
        for (actual, expected) in storage[1..].iter().zip(expected) {
            assert!((*actual / 16.0 - expected).norm() < 1e-5);
        }
    }

    #[test]
    fn threaded_plan_roundtrips_without_a_backend_switch() {
        let shape = [32, 24];
        let mut plane: Vec<_> = (0..shape[0] * shape[1])
            .map(|index| Complex::new((index % 11) as f32, (index % 5) as f32))
            .collect();
        let expected = plane.clone();
        let mut fft = Fft2::<f32>::with_threads(shape, 2).unwrap();
        fft.transform(&mut plane, false).unwrap();
        fft.transform(&mut plane, true).unwrap();
        for (actual, expected) in plane.iter().zip(expected) {
            assert!((*actual / plane.len() as f32 - expected).norm() < 1e-4);
        }
    }
}

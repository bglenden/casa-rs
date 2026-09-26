// SPDX-License-Identifier: LGPL-3.0-or-later
#![warn(missing_docs)]
//! Direct, reusable two-dimensional FFTW plans for contiguous complex planes.
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
}

/// A shape, layout, or native planner error.
#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum FftError {
    /// An extent is zero, exceeds FFTW's signed dimension range, or overflows.
    InvalidShape,
    /// The requested complex plane has a different shape or noncontiguous layout.
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

/// The two supported FFTW complex precisions. This trait is sealed.
pub trait FftScalar: sealed::Sealed + Copy + Default + Send + Sync + 'static {
    #[doc(hidden)]
    fn alignment(pointer: *mut Complex<Self>) -> c_int;
    #[doc(hidden)]
    unsafe fn plan(shape: [c_int; 2], pointer: *mut Complex<Self>, sign: c_int) -> *mut c_void;
    #[doc(hidden)]
    unsafe fn execute(plan: *mut c_void, pointer: *mut Complex<Self>);
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
            ) -> *mut c_void {
                // SAFETY: caller owns a writable, shape-sized, aligned scratch plane.
                unsafe { $module::plan_dft_2d(shape[0], shape[1], pointer, pointer, sign, MEASURE) }
            }
            unsafe fn execute(plan: *mut c_void, pointer: *mut Complex<Self>) {
                // SAFETY: caller matched FFTW's rank, strides, in-place layout and alignment.
                unsafe { $module::execute_dft(plan, pointer, pointer) }
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
        // SAFETY: pointer addresses a writable scratch plane of the requested shape.
        let forward = unsafe { T::plan(shape, pointer, FORWARD) };
        if forward.is_null() {
            return Err(FftError::PlanningFailed);
        }
        // SAFETY: the planner may overwrite scratch; it remains disposable.
        let inverse = unsafe { T::plan(shape, pointer, BACKWARD) };
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
    current: Option<(Key, Arc<Plans<T>>)>,
}

impl<T: FftScalar> fmt::Debug for Fft2<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Fft2")
            .field("shape", &self.shape)
            .field("threads", &self.threads)
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
            current: None,
        })
    }

    /// Execute a rank-two transform directly on the caller's contiguous plane.
    pub fn transform(&mut self, plane: &mut [Complex<T>], inverse: bool) -> Result<(), FftError> {
        if plane.len() != self.elements {
            return Err(FftError::InvalidPlane);
        }
        let pointer = plane.as_mut_ptr();
        let key = Key {
            shape: self.shape,
            alignment: T::alignment(pointer),
            threads: self.threads,
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
        unsafe { T::execute(plan, pointer) };
        Ok(())
    }

    /// Number of FFTW threads configured for each transform.
    pub fn threads(&self) -> usize {
        self.threads
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

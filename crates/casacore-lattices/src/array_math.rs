// SPDX-License-Identifier: LGPL-3.0-or-later
//! Array-level mathematical utilities mirroring C++ `casacore/casa/Arrays/ArrayMath.h`.
//!
//! `ndarray` already covers element-wise arithmetic and basic reductions
//! (`sum`, `mean`). The functions here fill the gaps needed for idiomatic
//! casa-rs code:
//!
//! | Rust                 | C++ equivalent                                     |
//! |----------------------|----------------------------------------------------|
//! | [`array_median`]     | `casacore::median(arr)`                            |
//! | [`array_fractile`]   | `casacore::fractile(arr, fraction)`                |
//! | [`near`]             | `casacore::near(a, b)` (f64 default tol=1e-13)     |
//! | [`near_f32`]         | `casacore::near(a, b)` (f32 default tol=1e-5)      |
//! | [`near_tol`]         | `casacore::near(a, b, tol)`                        |
//! | [`near_abs`]         | `casacore::nearAbs(a, b, tol)`                     |

use ndarray::ArrayD;

use crate::statistics::{StatsElement, casacore_fractile, casacore_madfm, casacore_median};

// ─────────────────────────────────────────────────────────────────────────────
// Order statistics (array-level)
// ─────────────────────────────────────────────────────────────────────────────

/// Compute the median of all elements in `arr`.
///
/// For even-length arrays the two middle elements are averaged, matching C++
/// `casacore::median(arr, takeEvenMean=true)`.
///
/// # Example
///
/// ```rust
/// use casacore_lattices::array_median;
/// use ndarray::{ArrayD, IxDyn};
///
/// let a = ArrayD::from_shape_vec(IxDyn(&[4]), vec![3.0_f32, 1.0, 4.0, 2.0]).unwrap();
/// // sorted: [1,2,3,4], median = (2+3)/2 = 2.5
/// assert_eq!(array_median(&a), 2.5);
/// ```
pub fn array_median<T: StatsElement>(arr: &ArrayD<T>) -> f64 {
    let values: Vec<f64> = arr.iter().map(|v| v.to_f64_stats()).collect();
    casacore_median(&values)
}

/// Compute the `fraction`-th fractile (percentile) of all elements in `arr`.
///
/// Uses C++ `casacore::fractile` semantics: returns
/// `sorted[floor((n-1)*fraction + 0.01)]` — nearest-rank selection with the
/// C++ epsilon offset.
///
/// # Example
///
/// ```rust
/// use casacore_lattices::array_fractile;
/// use ndarray::{ArrayD, IxDyn};
///
/// // indgen 0..63
/// let a = ArrayD::from_shape_fn(IxDyn(&[64]), |idx| idx[0] as f32);
/// // C++ fractile(arr, 0.25) = sorted[floor(63*0.25+0.01)] = sorted[15] = 15
/// assert_eq!(array_fractile(&a, 0.25), 15.0);
/// assert_eq!(array_fractile(&a, 0.75), 47.0);
/// ```
pub fn array_fractile<T: StatsElement>(arr: &ArrayD<T>, fraction: f64) -> f64 {
    let values: Vec<f64> = arr.iter().map(|v| v.to_f64_stats()).collect();
    casacore_fractile(&values, fraction)
}

/// Compute the median absolute deviation from the median.
///
/// Matches C++ `casacore::madfm(arr)`.
///
/// # Example
///
/// ```rust
/// use casacore_lattices::array_madfm;
/// use ndarray::{ArrayD, IxDyn};
///
/// let a = ArrayD::from_shape_fn(IxDyn(&[64]), |idx| idx[0] as f32);
/// // For indgen 0..63: median=31.5, madfm=16.0
/// assert_eq!(array_madfm(&a), 16.0);
/// ```
pub fn array_madfm<T: StatsElement>(arr: &ArrayD<T>) -> f64 {
    let values: Vec<f64> = arr.iter().map(|v| v.to_f64_stats()).collect();
    casacore_madfm(&values)
}

// ─────────────────────────────────────────────────────────────────────────────
// Tolerance comparison (near / nearAbs)
// ─────────────────────────────────────────────────────────────────────────────

/// Relative-tolerance comparison for `f64` values.
///
/// Returns `true` if `|a - b| <= tol * max(|a|, |b|)`.
///
/// Default tolerance for `f64` is `1e-13`, matching C++
/// `casacore::near(Double, Double)`.
///
/// # Special cases
///
/// - Returns `true` if `a == b` (handles infinities).
/// - Returns `false` if `a` and `b` have opposite signs.
/// - Returns `true` if `a == 0` and `|b|` is below the floating-point minimum
///   (matching C++ behaviour).
///
/// # Example
///
/// ```rust
/// use casacore_lattices::near;
/// assert!(near(1.0, 1.0 + 1e-14));
/// assert!(!near(1.0, 1.001));
/// ```
pub fn near(a: f64, b: f64) -> bool {
    near_tol(a, b, 1e-13)
}

/// Relative-tolerance comparison for `f32`-precision values.
///
/// Default tolerance `1e-5`, matching C++ `casacore::near(Float, Float)`.
pub fn near_f32(a: f64, b: f64) -> bool {
    near_tol(a, b, 1e-5)
}

/// Relative-tolerance comparison with explicit tolerance.
///
/// Mirrors C++ `casacore::near(a, b, tol)`.
pub fn near_tol(a: f64, b: f64, tol: f64) -> bool {
    if tol <= 0.0 {
        return a == b;
    }
    if a == b {
        return true;
    }
    if a == 0.0 {
        return b.abs() <= (1.0 + tol) * f64::MIN_POSITIVE;
    }
    if b == 0.0 {
        return a.abs() <= (1.0 + tol) * f64::MIN_POSITIVE;
    }
    if (a > 0.0) != (b > 0.0) {
        return false;
    }
    (a - b).abs() <= tol * a.abs().max(b.abs())
}

/// Absolute-tolerance comparison.
///
/// Returns `true` if `|a - b| <= tol`.
/// Mirrors C++ `casacore::nearAbs(a, b, tol)`.
pub fn near_abs(a: f64, b: f64, tol: f64) -> bool {
    (a - b).abs() <= tol
}

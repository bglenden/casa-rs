// SPDX-License-Identifier: LGPL-3.0-or-later
//! Built-in function registry for TaQL.
//!
//! ~35 built-in scalar functions covering math, trig, string manipulation,
//! type conversion, and array inspection. Functions are looked up by name
//! (case-insensitive) and dispatched to typed implementations.
//!
//! # Function categories
//!
//! | Category | Functions |
//! |----------|-----------|
//! | Math constants | `pi`, `e`, `c` |
//! | Trigonometric | `sin`, `cos`, `tan`, `asin`, `acos`, `atan`, `atan2` |
//! | Exponential | `exp`, `log`, `log10`, `sqrt`, `pow` |
//! | Rounding | `abs`, `sign`, `floor`, `ceil`, `round`, `fmod` |
//! | Min/Max | `min`, `max` |
//! | Type conversion | `int`, `real`, `imag`, `string` |
//! | String | `upper`, `lower`, `trim`, `ltrim`, `rtrim`, `length`/`strlen`, `substr`, `replace` |
//! | Array inspection | `shape`, `ndim`, `nelements` |
//! | Boolean/null | `isnan`, `isinf`, `iif` |
//!
//! # C++ reference
//!
//! `TableExprFuncNode.cc`, `TaQLNode.cc`.

use std::collections::HashMap;
use std::sync::{LazyLock, Mutex};

use casa_core::quanta::{Quantity, Unit};

use super::error::TaqlError;
use super::eval::{ArrayValue, EvalContext, ExprValue};
use super::masked_arrays;
use num_complex::Complex64;

/// Trait for user-defined TaQL functions.
///
/// Implement this trait to add custom functions to the TaQL evaluator.
/// Registered UDFs take precedence over built-in functions with the
/// same name.
///
/// # C++ reference
///
/// `UDFBase` — the base class for casacore user-defined TaQL functions.
pub trait TaqlUdf: Send + Sync {
    /// Execute the function with the given arguments and row context.
    fn call(&self, args: &[ExprValue], ctx: &EvalContext<'_>) -> Result<ExprValue, TaqlError>;
}

/// A boxed function pointer variant for simple UDFs.
type UdfFn =
    Box<dyn Fn(&[ExprValue], &EvalContext<'_>) -> Result<ExprValue, TaqlError> + Send + Sync>;

enum UdfEntry {
    Trait(Box<dyn TaqlUdf>),
    Fn(UdfFn),
}

static UDF_REGISTRY: Mutex<Option<HashMap<String, UdfEntry>>> = Mutex::new(None);

fn with_registry<R>(f: impl FnOnce(&mut HashMap<String, UdfEntry>) -> R) -> R {
    let mut guard = UDF_REGISTRY.lock().unwrap();
    let registry = guard.get_or_insert_with(HashMap::new);
    f(registry)
}

/// Register a user-defined function by name.
///
/// The function takes precedence over built-in functions with the same name.
/// Names are stored case-insensitively (lowercased).
///
/// # Examples
///
/// ```rust
/// use casa_tables::taql::functions::register_udf;
/// use casa_tables::taql::eval::ExprValue;
///
/// register_udf("double", |args, _ctx| {
///     let v = args[0].to_float()?;
///     Ok(ExprValue::Float(v * 2.0))
/// });
/// ```
pub fn register_udf<F>(name: &str, f: F)
where
    F: Fn(&[ExprValue], &EvalContext<'_>) -> Result<ExprValue, TaqlError> + Send + Sync + 'static,
{
    with_registry(|reg| {
        reg.insert(name.to_lowercase(), UdfEntry::Fn(Box::new(f)));
    });
}

/// Register a user-defined function using the [`TaqlUdf`] trait.
///
/// The function takes precedence over built-in functions with the same name.
pub fn register_udf_trait(name: &str, udf: Box<dyn TaqlUdf>) {
    with_registry(|reg| {
        reg.insert(name.to_lowercase(), UdfEntry::Trait(udf));
    });
}

/// Unregister a user-defined function by name.
///
/// Returns `true` if the function was registered and removed.
pub fn unregister_udf(name: &str) -> bool {
    with_registry(|reg| reg.remove(&name.to_lowercase()).is_some())
}

/// Clear all registered user-defined functions.
pub fn clear_udfs() {
    with_registry(|reg| reg.clear());
}

/// Call a built-in TaQL function by name.
///
/// Function names are matched case-insensitively. The `ctx` parameter
/// provides row context for functions like `rownumber()` and `rowid()`.
///
/// User-defined functions registered via [`register_udf`] take precedence
/// over built-in functions.
pub fn call_function(
    name: &str,
    args: &[ExprValue],
    ctx: &EvalContext<'_>,
) -> Result<ExprValue, TaqlError> {
    let lower = name.to_lowercase();

    // Check UDF registry first (UDFs override built-ins)
    let udf_result = with_registry(|reg| {
        reg.get(&lower).map(|entry| match entry {
            UdfEntry::Trait(udf) => udf.call(args, ctx),
            UdfEntry::Fn(f) => f(args, ctx),
        })
    });
    if let Some(result) = udf_result {
        return result;
    }
    call_builtin(&lower, name, args, ctx)
}

/// Validate a call's name and arity without reading a source row or calling
/// a user-defined function. Builtins receive null placeholders for the
/// supplied arguments so validation can use their existing dispatch.
///
/// # Errors
/// The builtin is unknown or its number of arguments is invalid.
pub(super) fn validate_function_call(name: &str, arguments: usize) -> Result<(), TaqlError> {
    let lower = name.to_lowercase();
    if with_registry(|reg| reg.contains_key(&lower)) {
        return Ok(());
    }
    let row = casa_core::RecordValue::default();
    let context = EvalContext {
        row: &row,
        row_index: 0,
        style: super::ast::IndexStyle::Glish,
        measures: None,
    };
    match call_builtin(&lower, name, &vec![ExprValue::Null; arguments], &context) {
        Err(error @ (TaqlError::UnknownFunction { .. } | TaqlError::ArgumentCount { .. })) => {
            Err(error)
        }
        _ => Ok(()),
    }
}

pub(super) fn has_udf(name: &str) -> bool {
    with_registry(|registry| registry.contains_key(&name.to_ascii_lowercase()))
}

mod builtin;

use builtin::*;

/// Check that a function received the expected number of arguments.
fn check_arity(name: &str, args: &[ExprValue], expected: usize) -> Result<(), TaqlError> {
    if args.len() != expected {
        Err(TaqlError::ArgumentCount {
            name: name.to_string(),
            expected: expected.to_string(),
            got: args.len(),
        })
    } else {
        Ok(())
    }
}

fn map_unary(
    name: &str,
    args: &[ExprValue],
    apply: impl Fn(&ExprValue) -> Result<ExprValue, TaqlError>,
) -> Result<ExprValue, TaqlError> {
    check_arity(name, args, 1)?;
    let element = |value: &ExprValue| {
        if value.is_null() {
            Ok(ExprValue::Null)
        } else {
            apply(value)
        }
    };
    let value = &args[0];
    if let Some((array, mask)) = value.array_with_mask() {
        let data = array.data.iter().map(element).collect::<Result<_, _>>()?;
        return Ok(ExprValue::from_array_with_mask(
            ArrayValue {
                shape: array.shape.clone(),
                data,
            },
            mask.map(<[bool]>::to_vec),
        ));
    }
    element(value)
}

fn angle(value: &ExprValue) -> Result<f64, TaqlError> {
    static RADIANS: LazyLock<Unit> = LazyLock::new(|| Unit::new("rad").expect("radian unit"));
    static SECONDS: LazyLock<Unit> = LazyLock::new(|| Unit::new("s").expect("second unit"));
    match value {
        // Casacore also interprets one day of time as one turn.
        ExprValue::Quantity(q) if q.unit().conformant(&SECONDS) => {
            Ok(q.get_si_value() * (std::f64::consts::TAU / 86_400.0))
        }
        ExprValue::Quantity(q) => q
            .get_value_in(&RADIANS)
            .map_err(|err| TaqlError::TypeError {
                message: err.to_string(),
            }),
        _ => value.to_float(),
    }
}

/// Evaluate a unary function that operates on f64.
fn unary_float(name: &str, args: &[ExprValue], f: fn(f64) -> f64) -> Result<ExprValue, TaqlError> {
    check_arity(name, args, 1)?;
    if args[0].is_null() {
        return Ok(ExprValue::Null);
    }
    let v = args[0].to_float()?;
    Ok(ExprValue::Float(f(v)))
}

/// Format radians as `HH:MM:SS.sss` (hours/minutes/seconds).
///
/// Normalises to [0, 2pi) then converts to hours.
fn radians_to_hms(rad: f64) -> String {
    let two_pi = 2.0 * std::f64::consts::PI;
    let mut r = rad % two_pi;
    if r < 0.0 {
        r += two_pi;
    }
    let total_hours = r * 12.0 / std::f64::consts::PI;
    let h = total_hours as u32;
    let remainder = (total_hours - h as f64) * 60.0;
    let m = remainder as u32;
    let s = (remainder - m as f64) * 60.0;
    format!("{h:02}:{m:02}:{s:06.3}")
}

/// Format radians as `+DD.MM.SS.sss` (degrees/arcmin/arcsec).
///
/// The sign prefix is always present (`+` or `-`).
fn radians_to_dms(rad: f64) -> String {
    let sign = if rad < 0.0 { '-' } else { '+' };
    let total_deg = rad.abs().to_degrees();
    let d = total_deg as u32;
    let remainder = (total_deg - d as f64) * 60.0;
    let m = remainder as u32;
    let s = (remainder - m as f64) * 60.0;
    format!("{sign}{d:03}.{m:02}.{s:06.3}")
}

// ── Date/Time helpers ──────────────────────────────────────────────────

/// Parse a date/time string to MJD.
///
/// Supported formats:
/// - `YYYY-MM-DD` or `YYYY/MM/DD`
/// - `YYYY-MM-DDThh:mm:ss[.sss]`
/// - `YYYY/MM/DD/hh:mm:ss[.sss]`
fn parse_datetime_to_mjd(s: &str) -> Result<f64, TaqlError> {
    // Normalise separators
    let s = s.replace('T', " ").replace('/', "-");
    let parts: Vec<&str> = s.splitn(2, ' ').collect();
    let date_part = parts[0];
    let time_part = parts.get(1).unwrap_or(&"00:00:00");

    let date_fields: Vec<&str> = date_part.split('-').collect();
    if date_fields.len() < 3 {
        return Err(TaqlError::TypeError {
            message: format!("cannot parse date from '{}'", parts[0]),
        });
    }
    let y: i32 = date_fields[0].parse().map_err(|_| TaqlError::TypeError {
        message: format!("invalid year in '{date_part}'"),
    })?;
    let m: u32 = date_fields[1].parse().map_err(|_| TaqlError::TypeError {
        message: format!("invalid month in '{date_part}'"),
    })?;
    let d: u32 = date_fields[2].parse().map_err(|_| TaqlError::TypeError {
        message: format!("invalid day in '{date_part}'"),
    })?;

    let mjd_date = ymd_to_mjd(y, m, d);

    // Parse time
    let time_fields: Vec<&str> = time_part.split(':').collect();
    let h: f64 = time_fields
        .first()
        .and_then(|s| s.parse().ok())
        .unwrap_or(0.0);
    let min: f64 = time_fields
        .get(1)
        .and_then(|s| s.parse().ok())
        .unwrap_or(0.0);
    let sec: f64 = time_fields
        .get(2)
        .and_then(|s| s.parse().ok())
        .unwrap_or(0.0);

    let frac_day = (h * 3600.0 + min * 60.0 + sec) / 86400.0;
    Ok(mjd_date + frac_day)
}

/// Convert MJD to ISO date string.
fn mjd_to_date_string(mjd: f64) -> String {
    let (y, m, d) = mjd_to_ymd(mjd);
    let frac = mjd.fract().abs();
    let total_sec = frac * 86400.0;
    let h = (total_sec / 3600.0) as u32;
    let min = ((total_sec - h as f64 * 3600.0) / 60.0) as u32;
    let sec = total_sec - h as f64 * 3600.0 - min as f64 * 60.0;
    if h == 0 && min == 0 && sec < 0.001 {
        format!("{y:04}-{m:02}-{d:02}")
    } else {
        format!("{y:04}-{m:02}-{d:02}T{h:02}:{min:02}:{sec:06.3}")
    }
}

/// Convert (year, month, day) to MJD.
///
/// Uses the algorithm from Meeus, "Astronomical Algorithms".
fn ymd_to_mjd(y: i32, m: u32, d: u32) -> f64 {
    let (y, m) = if m <= 2 {
        (y as i64 - 1, m as i64 + 12)
    } else {
        (y as i64, m as i64)
    };
    let a = y / 100;
    let b = 2 - a + a / 4;
    let jd = (365.25 * (y + 4716) as f64).floor()
        + (30.6001 * (m + 1) as f64).floor()
        + d as f64
        + b as f64
        - 1524.5;
    jd - 2_400_000.5
}

/// Convert MJD to (year, month, day).
fn mjd_to_ymd(mjd: f64) -> (i32, u32, u32) {
    let jd = mjd + 2_400_000.5;
    let z = (jd + 0.5).floor() as i64;
    let a = if z < 2_299_161 {
        z
    } else {
        let alpha = ((z as f64 - 1_867_216.25) / 36_524.25).floor() as i64;
        z + 1 + alpha - alpha / 4
    };
    let b = a + 1524;
    let c = ((b as f64 - 122.1) / 365.25).floor() as i64;
    let d = (365.25 * c as f64).floor() as i64;
    let e = ((b - d) as f64 / 30.6001).floor() as i64;

    let day = (b - d - (30.6001 * e as f64).floor() as i64) as u32;
    let month = if e < 14 { e - 1 } else { e - 13 } as u32;
    let year = if month > 2 { c - 4716 } else { c - 4715 } as i32;
    (year, month, day)
}

/// Extract MJD from an ExprValue (DateTime or numeric).
fn extract_mjd(val: &ExprValue) -> Result<f64, TaqlError> {
    match val {
        ExprValue::DateTime(v) => Ok(*v),
        _ => val.to_float(),
    }
}

/// Day of year for a given date.
fn day_of_year(y: i32, m: u32, d: u32) -> u32 {
    let is_leap = (y % 4 == 0 && y % 100 != 0) || y % 400 == 0;
    let days_in_month = [0, 31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31];
    let mut doy: u32 = d;
    for i in 1..m {
        doy += days_in_month[i as usize];
    }
    if is_leap && m > 2 {
        doy += 1;
    }
    doy
}

// ── Array helpers ──────────────────────────────────────────────────────

/// Require that an argument is an Array, returning a reference to it.
fn require_array<'a>(
    name: &str,
    val: &'a ExprValue,
) -> Result<&'a super::eval::ArrayValue, TaqlError> {
    val.array_with_mask()
        .map(|(array, _)| array)
        .ok_or_else(|| TaqlError::TypeError {
            message: format!("{}() requires Array, got {}", name, val.type_name()),
        })
}

/// Reduce an array to a single float using a fold operation.
fn array_reduce_float(
    name: &str,
    val: &ExprValue,
    init: f64,
    fold_fn: fn(f64, f64) -> f64,
) -> Result<ExprValue, TaqlError> {
    let arr = require_array(name, val)?;
    if arr.data.is_empty() {
        return Ok(ExprValue::Null);
    }
    let result = arr
        .data
        .iter()
        .map(|v| v.to_float())
        .try_fold(init, |acc, v| v.map(|v| fold_fn(acc, v)))?;
    Ok(ExprValue::Float(result))
}

/// Compute variance of an array.
/// `population`: true for population variance (N), false for sample variance (N-1).
fn array_variance(name: &str, val: &ExprValue, population: bool) -> Result<ExprValue, TaqlError> {
    let arr = require_array(name, val)?;
    let n = arr.data.len();
    if n == 0 || (!population && n < 2) {
        return Ok(ExprValue::Null);
    }
    let vals: Vec<f64> = arr
        .data
        .iter()
        .map(|v| v.to_float())
        .collect::<Result<_, _>>()?;
    let mean = vals.iter().sum::<f64>() / n as f64;
    let sum_sq: f64 = vals.iter().map(|v| (v - mean).powi(2)).sum();
    let denom = if population { n } else { n - 1 } as f64;
    Ok(ExprValue::Float(sum_sq / denom))
}

/// Convert flat index to multi-dimensional indices (row-major).
fn flat_to_multi(flat: usize, shape: &[usize]) -> Vec<usize> {
    let mut result = vec![0; shape.len()];
    let mut remainder = flat;
    // Column-major (Fortran order): first dimension varies fastest.
    for i in 0..shape.len() {
        result[i] = remainder % shape[i];
        remainder /= shape[i];
    }
    result
}

/// Convert multi-dimensional indices to flat index (column-major / Fortran order).
fn multi_to_flat(indices: &[usize], shape: &[usize]) -> usize {
    let mut flat = 0;
    let mut stride = 1;
    // Column-major: first dimension varies fastest.
    for i in 0..shape.len() {
        flat += indices[i] * stride;
        stride *= shape[i];
    }
    flat
}

// ── Astronomy helpers ──────────────────────────────────────────────────

/// Angular distance between two sky positions using the Vincenty formula.
///
/// All arguments in radians. Returns distance in radians.
fn angular_distance(ra1: f64, dec1: f64, ra2: f64, dec2: f64) -> f64 {
    let delta_ra = ra2 - ra1;
    let cos_dec1 = dec1.cos();
    let cos_dec2 = dec2.cos();
    let sin_dec1 = dec1.sin();
    let sin_dec2 = dec2.sin();

    let x = cos_dec2 * delta_ra.sin();
    let y = cos_dec1 * sin_dec2 - sin_dec1 * cos_dec2 * delta_ra.cos();
    let z = sin_dec1 * sin_dec2 + cos_dec1 * cos_dec2 * delta_ra.cos();

    x.hypot(y).atan2(z)
}

/// Angular distance using cross-product formula (more stable for small angles).
fn angular_distance_x(ra1: f64, dec1: f64, ra2: f64, dec2: f64) -> f64 {
    // Convert to Cartesian
    let x1 = dec1.cos() * ra1.cos();
    let y1 = dec1.cos() * ra1.sin();
    let z1 = dec1.sin();
    let x2 = dec2.cos() * ra2.cos();
    let y2 = dec2.cos() * ra2.sin();
    let z2 = dec2.sin();

    // Cross product magnitude
    let cx = y1 * z2 - z1 * y2;
    let cy = z1 * x2 - x1 * z2;
    let cz = x1 * y2 - y1 * x2;
    let cross_mag = (cx * cx + cy * cy + cz * cz).sqrt();

    // Dot product
    let dot = x1 * x2 + y1 * y2 + z1 * z2;

    cross_mag.atan2(dot)
}

/// Extract (ra, dec) pair from a 2-element array ExprValue.
fn extract_ra_dec(val: &ExprValue) -> Result<(f64, f64), TaqlError> {
    match val {
        ExprValue::Array(arr) => {
            if arr.data.len() != 2 {
                return Err(TaqlError::TypeError {
                    message: "expected 2-element array for (ra, dec)".to_string(),
                });
            }
            Ok((arr.data[0].to_float()?, arr.data[1].to_float()?))
        }
        _ => Err(TaqlError::TypeError {
            message: format!("expected Array for (ra, dec), got {}", val.type_name()),
        }),
    }
}

// ── Window aggregate infrastructure ──────────────────────────────────

/// The aggregate operation to apply over a window of values.
#[derive(Debug, Clone, Copy)]
enum RunningOp {
    Min,
    Max,
    Sum,
    Mean,
    Median,
    Rms,
    Variance,
    StdDev,
    Any,
    All,
}

/// Compute a single aggregate value from a window of floats.
fn window_aggregate(vals: &[f64], op: RunningOp) -> f64 {
    let n = vals.len() as f64;
    match op {
        RunningOp::Min => vals.iter().copied().fold(f64::INFINITY, f64::min),
        RunningOp::Max => vals.iter().copied().fold(f64::NEG_INFINITY, f64::max),
        RunningOp::Sum => vals.iter().sum(),
        RunningOp::Mean => vals.iter().sum::<f64>() / n,
        RunningOp::Median => {
            let mut sorted = vals.to_vec();
            sorted.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
            let mid = sorted.len() / 2;
            if sorted.len().is_multiple_of(2) {
                (sorted[mid - 1] + sorted[mid]) / 2.0
            } else {
                sorted[mid]
            }
        }
        RunningOp::Rms => (vals.iter().map(|v| v * v).sum::<f64>() / n).sqrt(),
        RunningOp::Variance => {
            let mean = vals.iter().sum::<f64>() / n;
            vals.iter().map(|v| (v - mean).powi(2)).sum::<f64>() / n
        }
        RunningOp::StdDev => {
            let mean = vals.iter().sum::<f64>() / n;
            (vals.iter().map(|v| (v - mean).powi(2)).sum::<f64>() / n).sqrt()
        }
        RunningOp::Any => {
            if vals.iter().any(|v| *v != 0.0) {
                1.0
            } else {
                0.0
            }
        }
        RunningOp::All => {
            if vals.iter().all(|v| *v != 0.0) {
                1.0
            } else {
                0.0
            }
        }
    }
}

/// Returns true if the result type should be Bool for this op.
fn is_bool_op(op: RunningOp) -> bool {
    matches!(op, RunningOp::Any | RunningOp::All)
}

/// RUNNING*(array) — cumulative aggregate over array elements.
/// RUNNING*(array, halfWidth) — centered sliding-window aggregate.
///
/// The second argument is the half-window width. Element `i` is computed
/// from `array[max(0, i-half) ..= min(n-1, i+half)]`. Partial windows
/// at the edges are included (matching C++ `fillEdge=True`).
///
/// C++ reference: `TableExprFuncNodeArray.cc` — `runningXXX`.
fn running_aggregate(
    name: &str,
    args: &[ExprValue],
    op: RunningOp,
) -> Result<ExprValue, TaqlError> {
    check_arity(name, args, 2)?;
    let arr = require_array(name, &args[0])?;
    let half = args[1].to_int()? as usize;
    if arr.data.is_empty() {
        return Ok(ExprValue::Array(super::eval::ArrayValue {
            shape: vec![0],
            data: vec![],
        }));
    }
    let floats: Vec<f64> = arr
        .data
        .iter()
        .map(|v| v.to_float())
        .collect::<Result<_, _>>()?;
    let n = floats.len();
    let mut result = Vec::with_capacity(n);
    for i in 0..n {
        let start = i.saturating_sub(half);
        let end = (i + half + 1).min(n);
        let window = &floats[start..end];
        let val = window_aggregate(window, op);
        if is_bool_op(op) {
            result.push(ExprValue::Bool(val != 0.0));
        } else {
            result.push(ExprValue::Float(val));
        }
    }
    Ok(ExprValue::Array(super::eval::ArrayValue {
        shape: vec![result.len()],
        data: result,
    }))
}

/// BOXED*(array, box_size) — sliding window aggregate.
fn boxed_aggregate(name: &str, args: &[ExprValue], op: RunningOp) -> Result<ExprValue, TaqlError> {
    check_arity(name, args, 2)?;
    let arr = require_array(name, &args[0])?;
    let box_size = args[1].to_int()? as usize;
    if box_size == 0 {
        return Err(TaqlError::TypeError {
            message: format!("{name}(): box size must be > 0"),
        });
    }
    if arr.data.is_empty() {
        return Ok(ExprValue::Array(super::eval::ArrayValue {
            shape: vec![0],
            data: vec![],
        }));
    }
    let floats: Vec<f64> = arr
        .data
        .iter()
        .map(|v| v.to_float())
        .collect::<Result<_, _>>()?;
    let n = floats.len();
    let half = box_size / 2;
    let mut result = Vec::with_capacity(n);
    for i in 0..n {
        let start = i.saturating_sub(half);
        let end = (i + half + 1).min(n);
        let window = &floats[start..end];
        let val = window_aggregate(window, op);
        if is_bool_op(op) {
            result.push(ExprValue::Bool(val != 0.0));
        } else {
            result.push(ExprValue::Float(val));
        }
    }
    Ok(ExprValue::Array(super::eval::ArrayValue {
        shape: vec![result.len()],
        data: result,
    }))
}

/// Partial-axis array reduction: SUMS(array [, axis]).
///
/// With one argument, reduces the entire array to a scalar.
/// With two arguments, reduces along the specified 0-based axis,
/// producing an array with one fewer dimension.
///
/// C++ reference: `TableExprFuncNodeArray.cc`.
fn partial_axis_reduce(
    name: &str,
    args: &[ExprValue],
    op: RunningOp,
) -> Result<ExprValue, TaqlError> {
    if args.is_empty() || args.len() > 2 {
        return Err(TaqlError::ArgumentCount {
            name: name.to_string(),
            expected: "1 or 2".to_string(),
            got: args.len(),
        });
    }
    let arr = require_array(name, &args[0])?;
    if arr.data.is_empty() {
        return Ok(ExprValue::Null);
    }
    // Check if input is integer-typed (for min/max, preserve type)
    let input_is_int = matches!(&arr.data[0], ExprValue::Int(_));
    let preserve_int = input_is_int && matches!(op, RunningOp::Min | RunningOp::Max);

    let floats: Vec<f64> = arr
        .data
        .iter()
        .map(|v| v.to_float())
        .collect::<Result<_, _>>()?;

    if args.len() == 1 {
        // Full reduction to scalar
        let val = window_aggregate(&floats, op);
        return if is_bool_op(op) {
            Ok(ExprValue::Bool(val != 0.0))
        } else if preserve_int {
            Ok(ExprValue::Int(val as i64))
        } else {
            Ok(ExprValue::Float(val))
        };
    }

    // Axis reduction — TaQL uses 1-based axis numbering (like Glish/Fortran)
    let axis_1based = args[1].to_int()?;
    if axis_1based < 1 {
        return Err(TaqlError::TypeError {
            message: format!("{name}(): axis must be >= 1 (1-based), got {axis_1based}"),
        });
    }
    let axis = (axis_1based - 1) as usize;
    let ndim = arr.shape.len();
    if axis >= ndim {
        return Err(TaqlError::TypeError {
            message: format!("{name}(): axis {axis_1based} out of range for {ndim}-D array"),
        });
    }

    let axis_len = arr.shape[axis];
    // Compute output shape (remove the axis dimension).
    let out_shape: Vec<usize> = arr
        .shape
        .iter()
        .enumerate()
        .filter(|&(i, _)| i != axis)
        .map(|(_, &s)| s)
        .collect();
    let out_size: usize = out_shape.iter().product();

    // For each output element, gather values along the axis and reduce.
    let mut out_data = Vec::with_capacity(out_size);
    for out_flat in 0..out_size {
        // Convert out_flat to multi-index in out_shape
        let out_multi = flat_to_multi(out_flat, &out_shape);
        // Insert the axis dimension back to iterate over it
        let mut window = Vec::with_capacity(axis_len);
        for ax_idx in 0..axis_len {
            let mut full_multi = Vec::with_capacity(ndim);
            let mut out_dim = 0;
            for dim in 0..ndim {
                if dim == axis {
                    full_multi.push(ax_idx);
                } else {
                    full_multi.push(out_multi[out_dim]);
                    out_dim += 1;
                }
            }
            let flat_idx = multi_to_flat(&full_multi, &arr.shape);
            window.push(floats[flat_idx]);
        }
        let val = window_aggregate(&window, op);
        if is_bool_op(op) {
            out_data.push(ExprValue::Bool(val != 0.0));
        } else if preserve_int {
            out_data.push(ExprValue::Int(val as i64));
        } else {
            out_data.push(ExprValue::Float(val));
        }
    }

    // If the result is a scalar (all dims collapsed), unwrap.
    if out_shape.is_empty() || out_shape == [1] {
        return Ok(out_data.into_iter().next().unwrap_or(ExprValue::Null));
    }

    Ok(ExprValue::Array(super::eval::ArrayValue {
        shape: out_shape,
        data: out_data,
    }))
}

// ── Wave 7: Utility helpers ─────────────────────────────────────────

/// Convert a shell-style glob pattern to a regex string.
///
/// `*` → `.*`, `?` → `.`, all other regex-special chars are escaped.
///
/// C++ reference: `Regex::fromPattern()`.
fn glob_to_regex(pattern: &str) -> String {
    let mut result = String::from("^");
    for ch in pattern.chars() {
        match ch {
            '*' => result.push_str(".*"),
            '?' => result.push('.'),
            '.' | '+' | '(' | ')' | '{' | '}' | '[' | ']' | '|' | '^' | '$' | '\\' => {
                result.push('\\');
                result.push(ch);
            }
            _ => result.push(ch),
        }
    }
    result.push('$');
    result
}

/// Convert a SQL LIKE pattern (`%` and `_`) to a regex string.
///
/// `%` → `.*`, `_` → `.`, other regex chars are escaped.
///
/// C++ reference: `Regex::fromSQLPattern()`.
fn sql_like_to_regex(pattern: &str) -> String {
    let mut result = String::from("^");
    for ch in pattern.chars() {
        match ch {
            '%' => result.push_str(".*"),
            '_' => result.push('.'),
            '.' | '+' | '*' | '?' | '(' | ')' | '{' | '}' | '[' | ']' | '|' | '^' | '$' | '\\' => {
                result.push('\\');
                result.push(ch);
            }
            _ => result.push(ch),
        }
    }
    result.push('$');
    result
}

#[cfg(test)]
mod tests;

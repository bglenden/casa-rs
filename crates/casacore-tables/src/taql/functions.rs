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
use std::sync::Mutex;

use super::error::TaqlError;
use super::eval::{EvalContext, ExprValue};
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
/// use casacore_tables::taql::functions::register_udf;
/// use casacore_tables::taql::eval::ExprValue;
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

    match lower.as_str() {
        // ── Math constants ─────────────────────────────────────────
        "pi" => {
            check_arity(name, args, 0)?;
            Ok(ExprValue::Float(std::f64::consts::PI))
        }
        "e" => {
            check_arity(name, args, 0)?;
            Ok(ExprValue::Float(std::f64::consts::E))
        }
        "c" => {
            // Speed of light in m/s
            check_arity(name, args, 0)?;
            Ok(ExprValue::Float(299_792_458.0))
        }

        // ── Trigonometric ──────────────────────────────────────────
        "sin" => unary_float(name, args, f64::sin),
        "cos" => unary_float(name, args, f64::cos),
        "tan" => unary_float(name, args, f64::tan),
        "asin" => unary_float(name, args, f64::asin),
        "acos" => unary_float(name, args, f64::acos),
        "atan" => unary_float(name, args, f64::atan),
        "atan2" => {
            check_arity(name, args, 2)?;
            if args[0].is_null() || args[1].is_null() {
                return Ok(ExprValue::Null);
            }
            let y = args[0].to_float()?;
            let x = args[1].to_float()?;
            Ok(ExprValue::Float(y.atan2(x)))
        }

        // ── Exponential ────────────────────────────────────────────
        "exp" => unary_float(name, args, f64::exp),
        "log" | "ln" => unary_float(name, args, f64::ln),
        "log10" => unary_float(name, args, f64::log10),
        "sqrt" => {
            check_arity(name, args, 1)?;
            if args[0].is_null() {
                return Ok(ExprValue::Null);
            }
            match &args[0] {
                ExprValue::Complex(c) => Ok(ExprValue::Complex(c.sqrt())),
                _ => {
                    let v = args[0].to_float()?;
                    Ok(ExprValue::Float(v.sqrt()))
                }
            }
        }
        "pow" => {
            check_arity(name, args, 2)?;
            if args[0].is_null() || args[1].is_null() {
                return Ok(ExprValue::Null);
            }
            let base = args[0].to_float()?;
            let exp = args[1].to_float()?;
            Ok(ExprValue::Float(base.powf(exp)))
        }

        // ── Rounding ───────────────────────────────────────────────
        "abs" => {
            check_arity(name, args, 1)?;
            if args[0].is_null() {
                return Ok(ExprValue::Null);
            }
            match &args[0] {
                ExprValue::Int(n) => Ok(ExprValue::Int(n.abs())),
                ExprValue::Float(v) => Ok(ExprValue::Float(v.abs())),
                ExprValue::Complex(c) => Ok(ExprValue::Float(c.norm())),
                other => Err(TaqlError::TypeError {
                    message: format!("abs() cannot operate on {}", other.type_name()),
                }),
            }
        }
        "sign" => {
            check_arity(name, args, 1)?;
            if args[0].is_null() {
                return Ok(ExprValue::Null);
            }
            match &args[0] {
                ExprValue::Int(n) => Ok(ExprValue::Int(n.signum())),
                ExprValue::Float(v) => Ok(ExprValue::Float(v.signum())),
                other => Err(TaqlError::TypeError {
                    message: format!("sign() cannot operate on {}", other.type_name()),
                }),
            }
        }
        "floor" => unary_float(name, args, f64::floor),
        "ceil" => unary_float(name, args, f64::ceil),
        "round" => unary_float(name, args, f64::round),
        "fmod" => {
            check_arity(name, args, 2)?;
            if args[0].is_null() || args[1].is_null() {
                return Ok(ExprValue::Null);
            }
            let a = args[0].to_float()?;
            let b = args[1].to_float()?;
            Ok(ExprValue::Float(a % b))
        }

        // ── Min/Max ────────────────────────────────────────────────
        "min" => {
            check_arity(name, args, 2)?;
            if args[0].is_null() {
                return Ok(args[1].clone());
            }
            if args[1].is_null() {
                return Ok(args[0].clone());
            }
            match args[0].compare(&args[1])? {
                std::cmp::Ordering::Less | std::cmp::Ordering::Equal => Ok(args[0].clone()),
                std::cmp::Ordering::Greater => Ok(args[1].clone()),
            }
        }
        "max" => {
            check_arity(name, args, 2)?;
            if args[0].is_null() {
                return Ok(args[1].clone());
            }
            if args[1].is_null() {
                return Ok(args[0].clone());
            }
            match args[0].compare(&args[1])? {
                std::cmp::Ordering::Greater | std::cmp::Ordering::Equal => Ok(args[0].clone()),
                std::cmp::Ordering::Less => Ok(args[1].clone()),
            }
        }

        // ── Type conversion ────────────────────────────────────────
        "int" | "integer" => {
            check_arity(name, args, 1)?;
            if args[0].is_null() {
                return Ok(ExprValue::Null);
            }
            Ok(ExprValue::Int(args[0].to_int()?))
        }
        "real" | "float" | "double" => {
            check_arity(name, args, 1)?;
            if args[0].is_null() {
                return Ok(ExprValue::Null);
            }
            match &args[0] {
                ExprValue::Complex(c) => Ok(ExprValue::Float(c.re)),
                _ => Ok(ExprValue::Float(args[0].to_float()?)),
            }
        }
        "imag" => {
            check_arity(name, args, 1)?;
            if args[0].is_null() {
                return Ok(ExprValue::Null);
            }
            match &args[0] {
                ExprValue::Complex(c) => Ok(ExprValue::Float(c.im)),
                _ => Ok(ExprValue::Float(0.0)),
            }
        }
        "string" | "str" => {
            check_arity(name, args, 1)?;
            if args[0].is_null() {
                return Ok(ExprValue::Null);
            }
            Ok(ExprValue::String(args[0].to_string()))
        }

        // ── String functions ───────────────────────────────────────
        "upper" | "upcase" => {
            check_arity(name, args, 1)?;
            match &args[0] {
                ExprValue::Null => Ok(ExprValue::Null),
                ExprValue::String(s) => Ok(ExprValue::String(s.to_uppercase())),
                other => Err(TaqlError::TypeError {
                    message: format!("upper() requires String, got {}", other.type_name()),
                }),
            }
        }
        "lower" | "downcase" => {
            check_arity(name, args, 1)?;
            match &args[0] {
                ExprValue::Null => Ok(ExprValue::Null),
                ExprValue::String(s) => Ok(ExprValue::String(s.to_lowercase())),
                other => Err(TaqlError::TypeError {
                    message: format!("lower() requires String, got {}", other.type_name()),
                }),
            }
        }
        "trim" => {
            check_arity(name, args, 1)?;
            match &args[0] {
                ExprValue::Null => Ok(ExprValue::Null),
                ExprValue::String(s) => Ok(ExprValue::String(s.trim().to_string())),
                other => Err(TaqlError::TypeError {
                    message: format!("trim() requires String, got {}", other.type_name()),
                }),
            }
        }
        "ltrim" => {
            check_arity(name, args, 1)?;
            match &args[0] {
                ExprValue::Null => Ok(ExprValue::Null),
                ExprValue::String(s) => Ok(ExprValue::String(s.trim_start().to_string())),
                other => Err(TaqlError::TypeError {
                    message: format!("ltrim() requires String, got {}", other.type_name()),
                }),
            }
        }
        "rtrim" => {
            check_arity(name, args, 1)?;
            match &args[0] {
                ExprValue::Null => Ok(ExprValue::Null),
                ExprValue::String(s) => Ok(ExprValue::String(s.trim_end().to_string())),
                other => Err(TaqlError::TypeError {
                    message: format!("rtrim() requires String, got {}", other.type_name()),
                }),
            }
        }
        "length" | "strlen" | "len" => {
            check_arity(name, args, 1)?;
            match &args[0] {
                ExprValue::Null => Ok(ExprValue::Null),
                ExprValue::String(s) => Ok(ExprValue::Int(s.len() as i64)),
                other => Err(TaqlError::TypeError {
                    message: format!("length() requires String, got {}", other.type_name()),
                }),
            }
        }
        "substr" | "substring" => {
            // substr(string, start [, length])
            if args.len() < 2 || args.len() > 3 {
                return Err(TaqlError::ArgumentCount {
                    name: name.to_string(),
                    expected: "2-3".to_string(),
                    got: args.len(),
                });
            }
            if args[0].is_null() {
                return Ok(ExprValue::Null);
            }
            let s = match &args[0] {
                ExprValue::String(s) => s.as_str(),
                other => {
                    return Err(TaqlError::TypeError {
                        message: format!(
                            "substr() requires String as first arg, got {}",
                            other.type_name()
                        ),
                    });
                }
            };
            let start = args[1].to_int()? as usize;
            let start = start.min(s.len());
            if args.len() == 3 {
                let len = args[2].to_int()? as usize;
                let end = (start + len).min(s.len());
                Ok(ExprValue::String(s[start..end].to_string()))
            } else {
                Ok(ExprValue::String(s[start..].to_string()))
            }
        }
        "replace" => {
            check_arity(name, args, 3)?;
            if args[0].is_null() {
                return Ok(ExprValue::Null);
            }
            match (&args[0], &args[1], &args[2]) {
                (ExprValue::String(s), ExprValue::String(from), ExprValue::String(to)) => {
                    Ok(ExprValue::String(s.replace(from.as_str(), to.as_str())))
                }
                _ => Err(TaqlError::TypeError {
                    message: "replace() requires three String arguments".to_string(),
                }),
            }
        }

        // ── Array inspection ───────────────────────────────────────
        "shape" => {
            check_arity(name, args, 1)?;
            match &args[0] {
                ExprValue::Array(arr) => {
                    let data = arr
                        .shape
                        .iter()
                        .map(|&d| ExprValue::Int(d as i64))
                        .collect();
                    Ok(ExprValue::Array(super::eval::ArrayValue {
                        shape: vec![arr.shape.len()],
                        data,
                    }))
                }
                ExprValue::Null => Ok(ExprValue::Null),
                // Scalar values have shape []
                _ => Ok(ExprValue::Array(super::eval::ArrayValue {
                    shape: vec![0],
                    data: vec![],
                })),
            }
        }
        "ndim" => {
            check_arity(name, args, 1)?;
            match &args[0] {
                ExprValue::Array(arr) => Ok(ExprValue::Int(arr.shape.len() as i64)),
                ExprValue::Null => Ok(ExprValue::Null),
                _ => Ok(ExprValue::Int(0)), // scalars have 0 dimensions
            }
        }
        "nelements" | "count" => {
            check_arity(name, args, 1)?;
            match &args[0] {
                ExprValue::Array(arr) => Ok(ExprValue::Int(arr.data.len() as i64)),
                ExprValue::Null => Ok(ExprValue::Null),
                _ => Ok(ExprValue::Int(1)), // scalar is 1 element
            }
        }

        // ── Boolean/null ───────────────────────────────────────────
        "isnan" => {
            check_arity(name, args, 1)?;
            match &args[0] {
                ExprValue::Float(v) => Ok(ExprValue::Bool(v.is_nan())),
                ExprValue::Null => Ok(ExprValue::Null),
                _ => Ok(ExprValue::Bool(false)),
            }
        }
        "isinf" => {
            check_arity(name, args, 1)?;
            match &args[0] {
                ExprValue::Float(v) => Ok(ExprValue::Bool(v.is_infinite())),
                ExprValue::Null => Ok(ExprValue::Null),
                _ => Ok(ExprValue::Bool(false)),
            }
        }
        "iif" => {
            // iif(condition, true_val, false_val)
            check_arity(name, args, 3)?;
            if args[0].is_null() {
                return Ok(args[2].clone());
            }
            let cond = args[0].to_bool()?;
            Ok(if cond {
                args[1].clone()
            } else {
                args[2].clone()
            })
        }

        // ── Complex construction ───────────────────────────────────
        "complex" => {
            check_arity(name, args, 2)?;
            if args[0].is_null() || args[1].is_null() {
                return Ok(ExprValue::Null);
            }
            let re = args[0].to_float()?;
            let im = args[1].to_float()?;
            Ok(ExprValue::Complex(Complex64::new(re, im)))
        }

        // ── Hyperbolic trig ────────────────────────────────────────
        "sinh" => unary_float(name, args, f64::sinh),
        "cosh" => unary_float(name, args, f64::cosh),
        "tanh" => unary_float(name, args, f64::tanh),

        // ── Complex functions ─────────────────────────────────────
        "conj" => {
            check_arity(name, args, 1)?;
            if args[0].is_null() {
                return Ok(ExprValue::Null);
            }
            match &args[0] {
                ExprValue::Complex(c) => Ok(ExprValue::Complex(c.conj())),
                _ => {
                    let v = args[0].to_float()?;
                    Ok(ExprValue::Float(v))
                }
            }
        }
        "norm" => {
            // |z|^2 for complex, x^2 for real
            check_arity(name, args, 1)?;
            if args[0].is_null() {
                return Ok(ExprValue::Null);
            }
            match &args[0] {
                ExprValue::Complex(c) => Ok(ExprValue::Float(c.norm_sqr())),
                _ => {
                    let v = args[0].to_float()?;
                    Ok(ExprValue::Float(v * v))
                }
            }
        }
        "arg" | "phase" => {
            // Phase angle in radians
            check_arity(name, args, 1)?;
            if args[0].is_null() {
                return Ok(ExprValue::Null);
            }
            match &args[0] {
                ExprValue::Complex(c) => Ok(ExprValue::Float(c.arg())),
                _ => {
                    let v = args[0].to_float()?;
                    Ok(ExprValue::Float(if v >= 0.0 {
                        0.0
                    } else {
                        std::f64::consts::PI
                    }))
                }
            }
        }

        // ── Math: square, cube ────────────────────────────────────
        "square" | "sqr" => {
            check_arity(name, args, 1)?;
            if args[0].is_null() {
                return Ok(ExprValue::Null);
            }
            match &args[0] {
                ExprValue::Int(n) => Ok(ExprValue::Int(n * n)),
                ExprValue::Float(v) => Ok(ExprValue::Float(v * v)),
                ExprValue::Complex(c) => Ok(ExprValue::Complex(c * c)),
                other => Err(TaqlError::TypeError {
                    message: format!("square() cannot operate on {}", other.type_name()),
                }),
            }
        }
        "cube" => {
            check_arity(name, args, 1)?;
            if args[0].is_null() {
                return Ok(ExprValue::Null);
            }
            match &args[0] {
                ExprValue::Int(n) => Ok(ExprValue::Int(n * n * n)),
                ExprValue::Float(v) => Ok(ExprValue::Float(v * v * v)),
                ExprValue::Complex(c) => Ok(ExprValue::Complex(c * c * c)),
                other => Err(TaqlError::TypeError {
                    message: format!("cube() cannot operate on {}", other.type_name()),
                }),
            }
        }

        // ── Type: bool conversion ─────────────────────────────────
        "bool" | "boolean" => {
            check_arity(name, args, 1)?;
            if args[0].is_null() {
                return Ok(ExprValue::Null);
            }
            Ok(ExprValue::Bool(args[0].to_bool()?))
        }

        // ── Comparison: near, nearabs ─────────────────────────────
        "near" => {
            // near(a, b [, tolerance]) — relative tolerance (default 1e-13)
            if args.len() < 2 || args.len() > 3 {
                return Err(TaqlError::ArgumentCount {
                    name: name.to_string(),
                    expected: "2-3".to_string(),
                    got: args.len(),
                });
            }
            if args[0].is_null() || args[1].is_null() {
                return Ok(ExprValue::Null);
            }
            let a = args[0].to_float()?;
            let b = args[1].to_float()?;
            let tol = if args.len() == 3 {
                args[2].to_float()?
            } else {
                1e-13
            };
            let max_abs = a.abs().max(b.abs());
            let result = if max_abs == 0.0 {
                true
            } else {
                (a - b).abs() / max_abs <= tol
            };
            Ok(ExprValue::Bool(result))
        }
        "nearabs" => {
            // nearabs(a, b [, tolerance]) — absolute tolerance (default 1e-13)
            if args.len() < 2 || args.len() > 3 {
                return Err(TaqlError::ArgumentCount {
                    name: name.to_string(),
                    expected: "2-3".to_string(),
                    got: args.len(),
                });
            }
            if args[0].is_null() || args[1].is_null() {
                return Ok(ExprValue::Null);
            }
            let a = args[0].to_float()?;
            let b = args[1].to_float()?;
            let tol = if args.len() == 3 {
                args[2].to_float()?
            } else {
                1e-13
            };
            Ok(ExprValue::Bool((a - b).abs() <= tol))
        }

        // ── Boolean/null: isfinite, isnull, isdefined ─────────────
        "isfinite" => {
            check_arity(name, args, 1)?;
            match &args[0] {
                ExprValue::Float(v) => Ok(ExprValue::Bool(v.is_finite())),
                ExprValue::Null => Ok(ExprValue::Bool(false)),
                _ => Ok(ExprValue::Bool(true)),
            }
        }
        "isnull" => {
            check_arity(name, args, 1)?;
            Ok(ExprValue::Bool(args[0].is_null()))
        }
        "isdefined" => {
            check_arity(name, args, 1)?;
            Ok(ExprValue::Bool(!args[0].is_null()))
        }

        // ── String: capitalize, sreverse ──────────────────────────
        "capitalize" => {
            check_arity(name, args, 1)?;
            match &args[0] {
                ExprValue::Null => Ok(ExprValue::Null),
                ExprValue::String(s) => {
                    let capitalized = s
                        .split_whitespace()
                        .map(|word| {
                            let mut chars = word.chars();
                            match chars.next() {
                                Some(c) => {
                                    let upper: String = c.to_uppercase().collect();
                                    format!("{upper}{}", chars.as_str())
                                }
                                None => String::new(),
                            }
                        })
                        .collect::<Vec<_>>()
                        .join(" ");
                    Ok(ExprValue::String(capitalized))
                }
                other => Err(TaqlError::TypeError {
                    message: format!("capitalize() requires String, got {}", other.type_name()),
                }),
            }
        }
        "sreverse" | "reversestring" => {
            check_arity(name, args, 1)?;
            match &args[0] {
                ExprValue::Null => Ok(ExprValue::Null),
                ExprValue::String(s) => Ok(ExprValue::String(s.chars().rev().collect())),
                other => Err(TaqlError::TypeError {
                    message: format!("sreverse() requires String, got {}", other.type_name()),
                }),
            }
        }

        // ── Pseudo/special ────────────────────────────────────────
        "rand" => {
            check_arity(name, args, 0)?;
            // Simple pseudo-random using a combination of ctx info.
            // For proper randomness, a real RNG would be needed.
            use std::collections::hash_map::DefaultHasher;
            use std::hash::{Hash, Hasher};
            let mut hasher = DefaultHasher::new();
            ctx.row_index.hash(&mut hasher);
            // Mix in a changing seed from address of ctx.
            (ctx as *const EvalContext<'_> as usize).hash(&mut hasher);
            let h = hasher.finish();
            Ok(ExprValue::Float((h as f64) / (u64::MAX as f64)))
        }
        "rownumber" | "rownr" => {
            check_arity(name, args, 0)?;
            Ok(ExprValue::Int((ctx.row_index + 1) as i64))
        }
        "rowid" => {
            check_arity(name, args, 0)?;
            Ok(ExprValue::Int(ctx.row_index as i64))
        }

        // ── Angle formatting ──────────────────────────────────────
        "hms" => {
            // Format radians as HH:MM:SS.sss
            check_arity(name, args, 1)?;
            if args[0].is_null() {
                return Ok(ExprValue::Null);
            }
            let rad = args[0].to_float()?;
            Ok(ExprValue::String(radians_to_hms(rad)))
        }
        "dms" => {
            // Format radians as DD.MM.SS.sss
            check_arity(name, args, 1)?;
            if args[0].is_null() {
                return Ok(ExprValue::Null);
            }
            let rad = args[0].to_float()?;
            Ok(ExprValue::String(radians_to_dms(rad)))
        }
        "hdms" => {
            // Format (ra, dec) pair
            check_arity(name, args, 2)?;
            if args[0].is_null() || args[1].is_null() {
                return Ok(ExprValue::Null);
            }
            let ra = args[0].to_float()?;
            let dec = args[1].to_float()?;
            Ok(ExprValue::String(format!(
                "{}/{}",
                radians_to_hms(ra),
                radians_to_dms(dec)
            )))
        }

        // ── Introspection ─────────────────────────────────────────
        "iscolumn" => {
            // Test if a column exists — check if the name is in the row.
            check_arity(name, args, 1)?;
            match &args[0] {
                ExprValue::String(col_name) => Ok(ExprValue::Bool(ctx.row.get(col_name).is_some())),
                _ => Ok(ExprValue::Bool(false)),
            }
        }
        "iskeyword" => {
            // Test if a keyword exists — for now, always false (no keyword context).
            check_arity(name, args, 1)?;
            Ok(ExprValue::Bool(false))
        }

        // ══════════════════════════════════════════════════════════════
        // Wave 5a: Date/Time functions
        // ══════════════════════════════════════════════════════════════
        "datetime" => {
            // datetime(string) — parse ISO date string to MJD
            check_arity(name, args, 1)?;
            if args[0].is_null() {
                return Ok(ExprValue::Null);
            }
            match &args[0] {
                ExprValue::String(s) => Ok(ExprValue::DateTime(parse_datetime_to_mjd(s)?)),
                _ => Err(TaqlError::TypeError {
                    message: "datetime() requires String argument".to_string(),
                }),
            }
        }
        "mjdtodate" | "mjd2date" => {
            // Convert MJD to ISO date string
            check_arity(name, args, 1)?;
            if args[0].is_null() {
                return Ok(ExprValue::Null);
            }
            let mjd = match &args[0] {
                ExprValue::DateTime(v) => *v,
                _ => args[0].to_float()?,
            };
            Ok(ExprValue::String(mjd_to_date_string(mjd)))
        }
        "mjd" => {
            // Extract MJD as float
            check_arity(name, args, 1)?;
            if args[0].is_null() {
                return Ok(ExprValue::Null);
            }
            match &args[0] {
                ExprValue::DateTime(v) => Ok(ExprValue::Float(*v)),
                _ => Ok(ExprValue::Float(args[0].to_float()?)),
            }
        }
        "date" => {
            // Extract date part (truncate to integer MJD)
            check_arity(name, args, 1)?;
            if args[0].is_null() {
                return Ok(ExprValue::Null);
            }
            let mjd = match &args[0] {
                ExprValue::DateTime(v) => *v,
                _ => args[0].to_float()?,
            };
            Ok(ExprValue::DateTime(mjd.floor()))
        }
        "time" => {
            // Extract time-of-day as fractional day
            check_arity(name, args, 1)?;
            if args[0].is_null() {
                return Ok(ExprValue::Null);
            }
            let mjd = match &args[0] {
                ExprValue::DateTime(v) => *v,
                _ => args[0].to_float()?,
            };
            Ok(ExprValue::Float(mjd.fract()))
        }
        "year" => {
            check_arity(name, args, 1)?;
            if args[0].is_null() {
                return Ok(ExprValue::Null);
            }
            let mjd = extract_mjd(&args[0])?;
            let (y, _, _) = mjd_to_ymd(mjd);
            Ok(ExprValue::Int(y as i64))
        }
        "month" => {
            check_arity(name, args, 1)?;
            if args[0].is_null() {
                return Ok(ExprValue::Null);
            }
            let mjd = extract_mjd(&args[0])?;
            let (_, m, _) = mjd_to_ymd(mjd);
            Ok(ExprValue::Int(m as i64))
        }
        "day" => {
            check_arity(name, args, 1)?;
            if args[0].is_null() {
                return Ok(ExprValue::Null);
            }
            let mjd = extract_mjd(&args[0])?;
            let (_, _, d) = mjd_to_ymd(mjd);
            Ok(ExprValue::Int(d as i64))
        }
        "cmonth" => {
            // Month name (Jan, Feb, ...)
            check_arity(name, args, 1)?;
            if args[0].is_null() {
                return Ok(ExprValue::Null);
            }
            let mjd = extract_mjd(&args[0])?;
            let (_, m, _) = mjd_to_ymd(mjd);
            let names = [
                "Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec",
            ];
            Ok(ExprValue::String(
                names.get(m as usize - 1).unwrap_or(&"???").to_string(),
            ))
        }
        "weekday" | "dow" => {
            // Day of week: 0=Monday ... 6=Sunday (C++ casacore convention)
            check_arity(name, args, 1)?;
            if args[0].is_null() {
                return Ok(ExprValue::Null);
            }
            let mjd = extract_mjd(&args[0])?;
            // MJD 0 = 17 Nov 1858 (Wednesday=2)
            let dow = ((mjd.floor() as i64 + 2) % 7 + 7) % 7;
            Ok(ExprValue::Int(dow))
        }
        "cdow" => {
            // Day-of-week name
            check_arity(name, args, 1)?;
            if args[0].is_null() {
                return Ok(ExprValue::Null);
            }
            let mjd = extract_mjd(&args[0])?;
            let dow = ((mjd.floor() as i64 + 2) % 7 + 7) % 7;
            let names = ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"];
            Ok(ExprValue::String(names[dow as usize % 7].to_string()))
        }
        "week" => {
            // ISO week number (1-53)
            check_arity(name, args, 1)?;
            if args[0].is_null() {
                return Ok(ExprValue::Null);
            }
            let mjd = extract_mjd(&args[0])?;
            let (y, m, d) = mjd_to_ymd(mjd);
            let day_of_year = day_of_year(y, m, d);
            let week = day_of_year.div_ceil(7);
            Ok(ExprValue::Int(week.max(1) as i64))
        }
        "ctod" => {
            // String to DateTime
            check_arity(name, args, 1)?;
            if args[0].is_null() {
                return Ok(ExprValue::Null);
            }
            match &args[0] {
                ExprValue::String(s) => Ok(ExprValue::DateTime(parse_datetime_to_mjd(s)?)),
                _ => Err(TaqlError::TypeError {
                    message: "ctod() requires String argument".to_string(),
                }),
            }
        }
        "cdate" => {
            // DateTime to date string (YYYY/MM/DD)
            check_arity(name, args, 1)?;
            if args[0].is_null() {
                return Ok(ExprValue::Null);
            }
            let mjd = extract_mjd(&args[0])?;
            let (y, m, d) = mjd_to_ymd(mjd);
            Ok(ExprValue::String(format!("{y:04}/{m:02}/{d:02}")))
        }
        "ctime" => {
            // DateTime to time string (HH:MM:SS.sss)
            check_arity(name, args, 1)?;
            if args[0].is_null() {
                return Ok(ExprValue::Null);
            }
            let mjd = extract_mjd(&args[0])?;
            let frac = mjd.fract().abs();
            let total_sec = frac * 86400.0;
            let h = (total_sec / 3600.0) as u32;
            let m = ((total_sec - h as f64 * 3600.0) / 60.0) as u32;
            let s = total_sec - h as f64 * 3600.0 - m as f64 * 60.0;
            Ok(ExprValue::String(format!("{h:02}:{m:02}:{s:06.3}")))
        }

        // ══════════════════════════════════════════════════════════════
        // Wave 5b: Array reductions (scalar variants)
        // ══════════════════════════════════════════════════════════════
        "sum" => {
            check_arity(name, args, 1)?;
            array_reduce_float(name, &args[0], 0.0, |acc, v| acc + v)
        }
        "product" => {
            check_arity(name, args, 1)?;
            array_reduce_float(name, &args[0], 1.0, |acc, v| acc * v)
        }
        "sumsqr" | "sumsquare" => {
            check_arity(name, args, 1)?;
            array_reduce_float(name, &args[0], 0.0, |acc, v| acc + v * v)
        }
        "mean" | "avg" => {
            check_arity(name, args, 1)?;
            let arr = require_array(name, &args[0])?;
            if arr.data.is_empty() {
                return Ok(ExprValue::Null);
            }
            let sum = arr
                .data
                .iter()
                .map(|v| v.to_float())
                .collect::<Result<Vec<_>, _>>()?
                .iter()
                .sum::<f64>();
            Ok(ExprValue::Float(sum / arr.data.len() as f64))
        }
        "variance" => {
            check_arity(name, args, 1)?;
            array_variance(name, &args[0], true)
        }
        "samplevariance" => {
            check_arity(name, args, 1)?;
            array_variance(name, &args[0], false)
        }
        "stddev" => {
            check_arity(name, args, 1)?;
            match array_variance(name, &args[0], true)? {
                ExprValue::Float(v) => Ok(ExprValue::Float(v.sqrt())),
                other => Ok(other),
            }
        }
        "samplestddev" => {
            check_arity(name, args, 1)?;
            match array_variance(name, &args[0], false)? {
                ExprValue::Float(v) => Ok(ExprValue::Float(v.sqrt())),
                other => Ok(other),
            }
        }
        "avdev" => {
            // Average absolute deviation from the mean
            check_arity(name, args, 1)?;
            let arr = require_array(name, &args[0])?;
            if arr.data.is_empty() {
                return Ok(ExprValue::Null);
            }
            let vals: Vec<f64> = arr
                .data
                .iter()
                .map(|v| v.to_float())
                .collect::<Result<_, _>>()?;
            let mean = vals.iter().sum::<f64>() / vals.len() as f64;
            let avdev = vals.iter().map(|v| (v - mean).abs()).sum::<f64>() / vals.len() as f64;
            Ok(ExprValue::Float(avdev))
        }
        "rms" => {
            check_arity(name, args, 1)?;
            let arr = require_array(name, &args[0])?;
            if arr.data.is_empty() {
                return Ok(ExprValue::Null);
            }
            let vals: Vec<f64> = arr
                .data
                .iter()
                .map(|v| v.to_float())
                .collect::<Result<_, _>>()?;
            let rms = (vals.iter().map(|v| v * v).sum::<f64>() / vals.len() as f64).sqrt();
            Ok(ExprValue::Float(rms))
        }
        "median" => {
            check_arity(name, args, 1)?;
            let arr = require_array(name, &args[0])?;
            if arr.data.is_empty() {
                return Ok(ExprValue::Null);
            }
            let mut vals: Vec<f64> = arr
                .data
                .iter()
                .map(|v| v.to_float())
                .collect::<Result<_, _>>()?;
            vals.sort_by(|a, b| a.total_cmp(b));
            let n = vals.len();
            let med = if n % 2 == 0 {
                (vals[n / 2 - 1] + vals[n / 2]) / 2.0
            } else {
                vals[n / 2]
            };
            Ok(ExprValue::Float(med))
        }
        "fractile" => {
            check_arity(name, args, 2)?;
            let arr = require_array(name, &args[0])?;
            if arr.data.is_empty() {
                return Ok(ExprValue::Null);
            }
            let fraction = args[1].to_float()?;
            let mut vals: Vec<f64> = arr
                .data
                .iter()
                .map(|v| v.to_float())
                .collect::<Result<_, _>>()?;
            vals.sort_by(|a, b| a.total_cmp(b));
            let idx = ((vals.len() as f64 - 1.0) * fraction.clamp(0.0, 1.0)) as usize;
            Ok(ExprValue::Float(vals[idx]))
        }

        // ── Boolean array reductions ─────────────────────────────────
        "any" => {
            check_arity(name, args, 1)?;
            let arr = require_array(name, &args[0])?;
            let result = arr.data.iter().any(|v| matches!(v, ExprValue::Bool(true)));
            Ok(ExprValue::Bool(result))
        }
        "all" => {
            check_arity(name, args, 1)?;
            let arr = require_array(name, &args[0])?;
            let result = arr.data.iter().all(|v| matches!(v, ExprValue::Bool(true)));
            Ok(ExprValue::Bool(result))
        }
        "ntrue" => {
            check_arity(name, args, 1)?;
            let arr = require_array(name, &args[0])?;
            let count = arr
                .data
                .iter()
                .filter(|v| matches!(v, ExprValue::Bool(true)))
                .count();
            Ok(ExprValue::Int(count as i64))
        }
        "nfalse" => {
            check_arity(name, args, 1)?;
            let arr = require_array(name, &args[0])?;
            let count = arr
                .data
                .iter()
                .filter(|v| matches!(v, ExprValue::Bool(false)))
                .count();
            Ok(ExprValue::Int(count as i64))
        }

        // ── Min/Max for arrays (1-arg forms) ─────────────────────────
        // Note: 2-arg min/max already handled above as scalar min/max.
        // These 1-arg forms accept an array and reduce to a scalar.
        "amin" => {
            check_arity(name, args, 1)?;
            let arr = require_array(name, &args[0])?;
            if arr.data.is_empty() {
                return Ok(ExprValue::Null);
            }
            let vals: Vec<f64> = arr
                .data
                .iter()
                .map(|v| v.to_float())
                .collect::<Result<_, _>>()?;
            Ok(ExprValue::Float(
                vals.iter().cloned().fold(f64::INFINITY, f64::min),
            ))
        }
        "amax" => {
            check_arity(name, args, 1)?;
            let arr = require_array(name, &args[0])?;
            if arr.data.is_empty() {
                return Ok(ExprValue::Null);
            }
            let vals: Vec<f64> = arr
                .data
                .iter()
                .map(|v| v.to_float())
                .collect::<Result<_, _>>()?;
            Ok(ExprValue::Float(
                vals.iter().cloned().fold(f64::NEG_INFINITY, f64::max),
            ))
        }

        // ══════════════════════════════════════════════════════════════
        // Wave 5c: Array manipulation
        // ══════════════════════════════════════════════════════════════
        "array" => {
            // array(value, shape...) — create array filled with value
            if args.len() < 2 {
                return Err(TaqlError::ArgumentCount {
                    name: name.to_string(),
                    expected: "2+".to_string(),
                    got: args.len(),
                });
            }
            let value = args[0].clone();
            let shape: Vec<usize> = args[1..]
                .iter()
                .map(|a| a.to_int().map(|v| v as usize))
                .collect::<Result<_, _>>()?;
            let total: usize = shape.iter().product();
            let data = vec![value; total];
            Ok(ExprValue::Array(super::eval::ArrayValue { shape, data }))
        }
        "transpose" => {
            // Reverse axis order of an array
            check_arity(name, args, 1)?;
            let arr = require_array(name, &args[0])?;
            if arr.shape.len() <= 1 {
                return Ok(args[0].clone());
            }
            // For 2D: transpose[i][j] = arr[j][i]
            // General: reverse shape and permute data accordingly
            let mut new_shape = arr.shape.clone();
            new_shape.reverse();
            let ndim = arr.shape.len();
            let mut new_data = vec![ExprValue::Null; arr.data.len()];
            for (flat_idx, val) in arr.data.iter().enumerate() {
                let multi = flat_to_multi(flat_idx, &arr.shape);
                let mut transposed = multi;
                transposed.reverse();
                let new_flat = multi_to_flat(&transposed, &new_shape);
                new_data[new_flat] = val.clone();
            }
            let _ = ndim; // used only conceptually
            Ok(ExprValue::Array(super::eval::ArrayValue {
                shape: new_shape,
                data: new_data,
            }))
        }
        "areverse" => {
            // Reverse elements along each axis
            check_arity(name, args, 1)?;
            let arr = require_array(name, &args[0])?;
            let mut new_data = arr.data.clone();
            new_data.reverse();
            Ok(ExprValue::Array(super::eval::ArrayValue {
                shape: arr.shape.clone(),
                data: new_data,
            }))
        }
        "diagonal" => {
            // Extract diagonal of a 2D array
            check_arity(name, args, 1)?;
            let arr = require_array(name, &args[0])?;
            if arr.shape.len() != 2 {
                return Err(TaqlError::TypeError {
                    message: "diagonal() requires a 2D array".to_string(),
                });
            }
            let rows = arr.shape[0];
            let cols = arr.shape[1];
            let n = rows.min(cols);
            // Column-major: element (i,i) is at flat index i + rows * i = i * (rows + 1).
            let data: Vec<ExprValue> = (0..n).map(|i| arr.data[i * (rows + 1)].clone()).collect();
            Ok(ExprValue::Array(super::eval::ArrayValue {
                shape: vec![n],
                data,
            }))
        }
        "flatten" | "arrayflatten" => {
            // Flatten to 1D
            check_arity(name, args, 1)?;
            let arr = require_array(name, &args[0])?;
            Ok(ExprValue::Array(super::eval::ArrayValue {
                shape: vec![arr.data.len()],
                data: arr.data.clone(),
            }))
        }
        "nullarray" => {
            // Create an empty array with given shape
            if args.is_empty() {
                return Err(TaqlError::ArgumentCount {
                    name: name.to_string(),
                    expected: "1+".to_string(),
                    got: 0,
                });
            }
            let shape: Vec<usize> = args
                .iter()
                .map(|a| a.to_int().map(|v| v as usize))
                .collect::<Result<_, _>>()?;
            let total: usize = shape.iter().product();
            let data = vec![ExprValue::Null; total];
            Ok(ExprValue::Array(super::eval::ArrayValue { shape, data }))
        }
        "resize" => {
            // resize(array, new_shape...) — resize/reshape array (pad with 0/truncate)
            if args.len() < 2 {
                return Err(TaqlError::ArgumentCount {
                    name: name.to_string(),
                    expected: "2+".to_string(),
                    got: args.len(),
                });
            }
            let arr = require_array(name, &args[0])?;
            let new_shape: Vec<usize> = args[1..]
                .iter()
                .map(|a| a.to_int().map(|v| v as usize))
                .collect::<Result<_, _>>()?;
            let total: usize = new_shape.iter().product();
            let mut data = arr.data.clone();
            data.resize(total, ExprValue::Float(0.0));
            Ok(ExprValue::Array(super::eval::ArrayValue {
                shape: new_shape,
                data,
            }))
        }
        "arraydata" | "getarrdata" => {
            // Return array data as-is (strip mask if any; we don't have masks)
            check_arity(name, args, 1)?;
            match &args[0] {
                ExprValue::Array(_) => Ok(args[0].clone()),
                _ => Err(TaqlError::TypeError {
                    message: format!("arraydata() requires Array, got {}", args[0].type_name()),
                }),
            }
        }
        "negatemask" => {
            // Negate mask (no-op since we don't support masks)
            check_arity(name, args, 1)?;
            match &args[0] {
                ExprValue::Array(_) => Ok(args[0].clone()),
                _ => Err(TaqlError::TypeError {
                    message: format!("negatemask() requires Array, got {}", args[0].type_name()),
                }),
            }
        }
        "marray" => {
            // marray(data, mask) — create masked array. We just return data.
            check_arity(name, args, 2)?;
            match &args[0] {
                ExprValue::Array(_) => Ok(args[0].clone()),
                _ => Err(TaqlError::TypeError {
                    message: format!(
                        "marray() requires Array as first arg, got {}",
                        args[0].type_name()
                    ),
                }),
            }
        }
        "arraymask" | "getarrmask" => {
            // Return mask of array (all false since we don't support masks)
            check_arity(name, args, 1)?;
            let arr = require_array(name, &args[0])?;
            let data = vec![ExprValue::Bool(false); arr.data.len()];
            Ok(ExprValue::Array(super::eval::ArrayValue {
                shape: arr.shape.clone(),
                data,
            }))
        }
        "replacemasked" | "replaceunmasked" => {
            // Replace masked/unmasked values (no-op variants without mask support)
            check_arity(name, args, 2)?;
            match &args[0] {
                ExprValue::Array(_) => Ok(args[0].clone()),
                _ => Err(TaqlError::TypeError {
                    message: format!("{}() requires Array, got {}", name, args[0].type_name()),
                }),
            }
        }

        // ── Array inspection (now functional) ────────────────────────
        // Override the earlier stub to actually work with arrays
        // Note: "shape", "ndim", "nelements" are matched earlier. Here we
        // provide 1-arg array min/max which differ from the 2-arg scalar
        // min/max already handled.

        // ══════════════════════════════════════════════════════════════
        // Wave 5d: Astronomy functions
        // ══════════════════════════════════════════════════════════════
        "angdist" => {
            // Angular distance between two (ra,dec) pairs (radians)
            // angdist([ra1,dec1], [ra2,dec2]) or angdist(ra1,dec1,ra2,dec2)
            if args.len() == 4 {
                let ra1 = args[0].to_float()?;
                let dec1 = args[1].to_float()?;
                let ra2 = args[2].to_float()?;
                let dec2 = args[3].to_float()?;
                Ok(ExprValue::Float(angular_distance(ra1, dec1, ra2, dec2)))
            } else if args.len() == 2 {
                let (ra1, dec1) = extract_ra_dec(&args[0])?;
                let (ra2, dec2) = extract_ra_dec(&args[1])?;
                Ok(ExprValue::Float(angular_distance(ra1, dec1, ra2, dec2)))
            } else {
                Err(TaqlError::ArgumentCount {
                    name: name.to_string(),
                    expected: "2 or 4".to_string(),
                    got: args.len(),
                })
            }
        }
        "angdistx" => {
            // Same as angdist but uses cross-product formula (more stable for small angles)
            if args.len() == 4 {
                let ra1 = args[0].to_float()?;
                let dec1 = args[1].to_float()?;
                let ra2 = args[2].to_float()?;
                let dec2 = args[3].to_float()?;
                Ok(ExprValue::Float(angular_distance_x(ra1, dec1, ra2, dec2)))
            } else if args.len() == 2 {
                let (ra1, dec1) = extract_ra_dec(&args[0])?;
                let (ra2, dec2) = extract_ra_dec(&args[1])?;
                Ok(ExprValue::Float(angular_distance_x(ra1, dec1, ra2, dec2)))
            } else {
                Err(TaqlError::ArgumentCount {
                    name: name.to_string(),
                    expected: "2 or 4".to_string(),
                    got: args.len(),
                })
            }
        }
        "normangle" => {
            // Normalise angle to [-pi, pi)
            check_arity(name, args, 1)?;
            if args[0].is_null() {
                return Ok(ExprValue::Null);
            }
            let rad = args[0].to_float()?;
            let pi = std::f64::consts::PI;
            let mut norm = rad % (2.0 * pi);
            if norm >= pi {
                norm -= 2.0 * pi;
            } else if norm < -pi {
                norm += 2.0 * pi;
            }
            Ok(ExprValue::Float(norm))
        }
        "cones" | "anycone" => {
            // cones(ra, dec, ra_list, dec_list, radius) — test if point is in any cone
            // Simplified: anycone(ra, dec, cone_ra, cone_dec, radius) for single cone
            if args.len() == 5 {
                let ra = args[0].to_float()?;
                let dec = args[1].to_float()?;
                let cone_ra = args[2].to_float()?;
                let cone_dec = args[3].to_float()?;
                let radius = args[4].to_float()?;
                let dist = angular_distance(ra, dec, cone_ra, cone_dec);
                Ok(ExprValue::Bool(dist <= radius))
            } else {
                Err(TaqlError::ArgumentCount {
                    name: name.to_string(),
                    expected: "5".to_string(),
                    got: args.len(),
                })
            }
        }
        "findcone" => {
            // findcone(ra, dec, cone_ra, cone_dec, radius) — returns index of matching cone
            if args.len() == 5 {
                let ra = args[0].to_float()?;
                let dec = args[1].to_float()?;
                let cone_ra = args[2].to_float()?;
                let cone_dec = args[3].to_float()?;
                let radius = args[4].to_float()?;
                let dist = angular_distance(ra, dec, cone_ra, cone_dec);
                if dist <= radius {
                    Ok(ExprValue::Int(0)) // single cone: index 0
                } else {
                    Ok(ExprValue::Int(-1)) // no match
                }
            } else {
                Err(TaqlError::ArgumentCount {
                    name: name.to_string(),
                    expected: "5".to_string(),
                    got: args.len(),
                })
            }
        }

        // ── Running window aggregates ─────────────────────────────
        //
        // RUNNING*(array) — cumulative aggregate over array elements.
        // Returns an array of the same length where element i is the
        // aggregate of elements 0..=i.
        //
        // C++ reference: `TableExprGroupFuncRunning*.h`.
        "runningmin" => running_aggregate(name, args, RunningOp::Min),
        "runningmax" => running_aggregate(name, args, RunningOp::Max),
        "runningsum" => running_aggregate(name, args, RunningOp::Sum),
        "runningmean" => running_aggregate(name, args, RunningOp::Mean),
        "runningmedian" => running_aggregate(name, args, RunningOp::Median),
        "runningrms" => running_aggregate(name, args, RunningOp::Rms),
        "runningvariance" => running_aggregate(name, args, RunningOp::Variance),
        "runningstddev" => running_aggregate(name, args, RunningOp::StdDev),
        "runningany" => running_aggregate(name, args, RunningOp::Any),
        "runningall" => running_aggregate(name, args, RunningOp::All),

        // ── Boxed (sliding) window aggregates ────────────────────
        //
        // BOXED*(array, box_size) — sliding window aggregate.
        // Returns an array where element i is the aggregate of elements
        // in the window centered on i with the given box size.
        //
        // C++ reference: `TableExprGroupFuncBoxed*.h`.
        "boxedmin" => boxed_aggregate(name, args, RunningOp::Min),
        "boxedmax" => boxed_aggregate(name, args, RunningOp::Max),
        "boxedsum" => boxed_aggregate(name, args, RunningOp::Sum),
        "boxedmean" => boxed_aggregate(name, args, RunningOp::Mean),
        "boxedmedian" => boxed_aggregate(name, args, RunningOp::Median),
        "boxedrms" => boxed_aggregate(name, args, RunningOp::Rms),
        "boxedvariance" => boxed_aggregate(name, args, RunningOp::Variance),
        "boxedstddev" => boxed_aggregate(name, args, RunningOp::StdDev),
        "boxedany" => boxed_aggregate(name, args, RunningOp::Any),
        "boxedall" => boxed_aggregate(name, args, RunningOp::All),

        // ── Partial-axis array reductions ────────────────────────
        //
        // SUMS(array [, axis]) — reduce along an axis (or all axes).
        // The pluralized forms (S suffix) collapse one dimension.
        //
        // C++ reference: `TableExprFuncNodeArray.cc`.
        "sums" => partial_axis_reduce(name, args, RunningOp::Sum),
        "means" => partial_axis_reduce(name, args, RunningOp::Mean),
        "mins" => partial_axis_reduce(name, args, RunningOp::Min),
        "maxs" => partial_axis_reduce(name, args, RunningOp::Max),
        "medians" => partial_axis_reduce(name, args, RunningOp::Median),
        "variances" => partial_axis_reduce(name, args, RunningOp::Variance),
        "stddevs" => partial_axis_reduce(name, args, RunningOp::StdDev),
        "rmss" => partial_axis_reduce(name, args, RunningOp::Rms),
        "anys" => partial_axis_reduce(name, args, RunningOp::Any),
        "alls" => partial_axis_reduce(name, args, RunningOp::All),

        // ── Wave 7: Missing utility functions ────────────────────
        //
        // C++ reference: `TableExprFuncNode.cc`.
        "pattern" => {
            // Convert a shell-style glob pattern to a regex pattern.
            check_arity(name, args, 1)?;
            let s = args[0].to_string_val()?;
            Ok(ExprValue::String(glob_to_regex(&s)))
        }
        "sqlpattern" => {
            // Convert a SQL LIKE pattern (% and _) to a regex pattern.
            check_arity(name, args, 1)?;
            let s = args[0].to_string_val()?;
            Ok(ExprValue::String(sql_like_to_regex(&s)))
        }

        _ if lower.starts_with("meas.") => super::meas_udf::call_meas_function(&lower, args),

        _ => Err(TaqlError::UnknownFunction {
            name: name.to_string(),
        }),
    }
}

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
    match val {
        ExprValue::Array(arr) => Ok(arr),
        _ => Err(TaqlError::TypeError {
            message: format!("{}() requires Array, got {}", name, val.type_name()),
        }),
    }
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
            if sorted.len() % 2 == 0 {
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
mod tests {
    use super::*;
    use casacore_types::{RecordField, RecordValue, ScalarValue, Value};
    use std::sync::{Mutex, MutexGuard};

    fn dummy_ctx() -> (RecordValue, usize) {
        let row = RecordValue::new(vec![RecordField::new(
            "col_a",
            Value::Scalar(ScalarValue::Int32(1)),
        )]);
        (row, 7)
    }

    fn call(name: &str, args: Vec<ExprValue>) -> ExprValue {
        let (row, idx) = dummy_ctx();
        let ctx = EvalContext {
            row: &row,
            row_index: idx,
            style: crate::taql::ast::IndexStyle::default(),
        };
        call_function(name, &args, &ctx).unwrap()
    }

    fn call_err(name: &str, args: &[ExprValue]) -> Result<ExprValue, TaqlError> {
        let (row, idx) = dummy_ctx();
        let ctx = EvalContext {
            row: &row,
            row_index: idx,
            style: crate::taql::ast::IndexStyle::default(),
        };
        call_function(name, args, &ctx)
    }

    fn udf_test_guard() -> MutexGuard<'static, ()> {
        static UDF_TEST_MUTEX: Mutex<()> = Mutex::new(());
        UDF_TEST_MUTEX.lock().unwrap()
    }

    // ── Math constants ──

    #[test]
    fn pi() {
        let v = call("pi", vec![]);
        assert_eq!(v, ExprValue::Float(std::f64::consts::PI));
    }

    #[test]
    fn euler_e() {
        let v = call("e", vec![]);
        assert_eq!(v, ExprValue::Float(std::f64::consts::E));
    }

    #[test]
    fn speed_of_light() {
        let v = call("c", vec![]);
        assert_eq!(v, ExprValue::Float(299_792_458.0));
    }

    // ── Trigonometric ──

    #[test]
    fn sin_zero() {
        let v = call("sin", vec![ExprValue::Float(0.0)]);
        assert_eq!(v, ExprValue::Float(0.0));
    }

    #[test]
    fn cos_zero() {
        let v = call("cos", vec![ExprValue::Float(0.0)]);
        assert_eq!(v, ExprValue::Float(1.0));
    }

    #[test]
    fn atan2_basic() {
        let v = call("atan2", vec![ExprValue::Float(1.0), ExprValue::Float(1.0)]);
        match v {
            ExprValue::Float(f) => assert!((f - std::f64::consts::FRAC_PI_4).abs() < 1e-10),
            _ => panic!("expected Float"),
        }
    }

    // ── Exponential ──

    #[test]
    fn sqrt_basic() {
        let v = call("sqrt", vec![ExprValue::Float(4.0)]);
        assert_eq!(v, ExprValue::Float(2.0));
    }

    #[test]
    fn log10_basic() {
        let v = call("log10", vec![ExprValue::Float(100.0)]);
        assert_eq!(v, ExprValue::Float(2.0));
    }

    #[test]
    fn pow_basic() {
        let v = call("pow", vec![ExprValue::Float(2.0), ExprValue::Float(3.0)]);
        assert_eq!(v, ExprValue::Float(8.0));
    }

    // ── Rounding ──

    #[test]
    fn abs_int() {
        let v = call("abs", vec![ExprValue::Int(-42)]);
        assert_eq!(v, ExprValue::Int(42));
    }

    #[test]
    fn abs_float() {
        let v = call("abs", vec![ExprValue::Float(-3.15)]);
        assert_eq!(v, ExprValue::Float(3.15));
    }

    #[test]
    fn sign_negative() {
        let v = call("sign", vec![ExprValue::Float(-5.0)]);
        assert_eq!(v, ExprValue::Float(-1.0));
    }

    #[test]
    fn floor_basic() {
        let v = call("floor", vec![ExprValue::Float(3.7)]);
        assert_eq!(v, ExprValue::Float(3.0));
    }

    #[test]
    fn ceil_basic() {
        let v = call("ceil", vec![ExprValue::Float(3.2)]);
        assert_eq!(v, ExprValue::Float(4.0));
    }

    #[test]
    fn round_basic() {
        let v = call("round", vec![ExprValue::Float(3.5)]);
        assert_eq!(v, ExprValue::Float(4.0));
    }

    #[test]
    fn fmod_basic() {
        let v = call("fmod", vec![ExprValue::Float(7.0), ExprValue::Float(3.0)]);
        assert_eq!(v, ExprValue::Float(1.0));
    }

    // ── Min/Max ──

    #[test]
    fn min_basic() {
        let v = call("min", vec![ExprValue::Int(3), ExprValue::Int(5)]);
        assert_eq!(v, ExprValue::Int(3));
    }

    #[test]
    fn max_basic() {
        let v = call("max", vec![ExprValue::Int(3), ExprValue::Int(5)]);
        assert_eq!(v, ExprValue::Int(5));
    }

    // ── Type conversion ──

    #[test]
    fn int_from_float() {
        let v = call("int", vec![ExprValue::Float(3.7)]);
        assert_eq!(v, ExprValue::Int(3));
    }

    #[test]
    fn real_from_complex() {
        let v = call("real", vec![ExprValue::Complex(Complex64::new(3.0, 4.0))]);
        assert_eq!(v, ExprValue::Float(3.0));
    }

    #[test]
    fn imag_from_complex() {
        let v = call("imag", vec![ExprValue::Complex(Complex64::new(3.0, 4.0))]);
        assert_eq!(v, ExprValue::Float(4.0));
    }

    #[test]
    fn string_conversion() {
        let v = call("string", vec![ExprValue::Int(42)]);
        assert_eq!(v, ExprValue::String("42".to_string()));
    }

    // ── String functions ──

    #[test]
    fn upper_basic() {
        let v = call("upper", vec![ExprValue::String("hello".to_string())]);
        assert_eq!(v, ExprValue::String("HELLO".to_string()));
    }

    #[test]
    fn lower_basic() {
        let v = call("lower", vec![ExprValue::String("HELLO".to_string())]);
        assert_eq!(v, ExprValue::String("hello".to_string()));
    }

    #[test]
    fn trim_basic() {
        let v = call("trim", vec![ExprValue::String("  hello  ".to_string())]);
        assert_eq!(v, ExprValue::String("hello".to_string()));
    }

    #[test]
    fn length_basic() {
        let v = call("length", vec![ExprValue::String("hello".to_string())]);
        assert_eq!(v, ExprValue::Int(5));
    }

    #[test]
    fn substr_basic() {
        let v = call(
            "substr",
            vec![
                ExprValue::String("hello world".to_string()),
                ExprValue::Int(6),
                ExprValue::Int(5),
            ],
        );
        assert_eq!(v, ExprValue::String("world".to_string()));
    }

    #[test]
    fn replace_basic() {
        let v = call(
            "replace",
            vec![
                ExprValue::String("hello world".to_string()),
                ExprValue::String("world".to_string()),
                ExprValue::String("rust".to_string()),
            ],
        );
        assert_eq!(v, ExprValue::String("hello rust".to_string()));
    }

    // ── Boolean/null ──

    #[test]
    fn isnan_true() {
        let v = call("isnan", vec![ExprValue::Float(f64::NAN)]);
        assert_eq!(v, ExprValue::Bool(true));
    }

    #[test]
    fn isnan_false() {
        let v = call("isnan", vec![ExprValue::Float(1.0)]);
        assert_eq!(v, ExprValue::Bool(false));
    }

    #[test]
    fn isinf_true() {
        let v = call("isinf", vec![ExprValue::Float(f64::INFINITY)]);
        assert_eq!(v, ExprValue::Bool(true));
    }

    #[test]
    fn iif_basic() {
        let v = call(
            "iif",
            vec![ExprValue::Bool(true), ExprValue::Int(1), ExprValue::Int(0)],
        );
        assert_eq!(v, ExprValue::Int(1));

        let v = call(
            "iif",
            vec![ExprValue::Bool(false), ExprValue::Int(1), ExprValue::Int(0)],
        );
        assert_eq!(v, ExprValue::Int(0));
    }

    // ── Null propagation ──

    #[test]
    fn null_propagation() {
        let v = call("sin", vec![ExprValue::Null]);
        assert!(v.is_null());
    }

    // ── Error cases ──

    #[test]
    fn wrong_arity() {
        let err = call_err("sin", &[ExprValue::Float(1.0), ExprValue::Float(2.0)]);
        assert!(matches!(err, Err(TaqlError::ArgumentCount { .. })));
    }

    #[test]
    fn unknown_function() {
        let err = call_err("nonexistent", &[]);
        assert!(matches!(err, Err(TaqlError::UnknownFunction { .. })));
    }

    #[test]
    fn type_error_upper() {
        let err = call_err("upper", &[ExprValue::Int(42)]);
        assert!(matches!(err, Err(TaqlError::TypeError { .. })));
    }

    // ── Complex construction ──

    #[test]
    fn complex_basic() {
        let v = call(
            "complex",
            vec![ExprValue::Float(3.0), ExprValue::Float(4.0)],
        );
        assert_eq!(v, ExprValue::Complex(Complex64::new(3.0, 4.0)));
    }

    // ── Hyperbolic trig ──

    #[test]
    fn sinh_basic() {
        let v = call("sinh", vec![ExprValue::Float(1.0)]);
        match v {
            ExprValue::Float(f) => assert!((f - 1.0_f64.sinh()).abs() < 1e-10),
            _ => panic!("expected Float"),
        }
    }

    #[test]
    fn cosh_basic() {
        let v = call("cosh", vec![ExprValue::Float(0.0)]);
        assert_eq!(v, ExprValue::Float(1.0));
    }

    #[test]
    fn tanh_basic() {
        let v = call("tanh", vec![ExprValue::Float(0.0)]);
        assert_eq!(v, ExprValue::Float(0.0));
    }

    // ── Complex functions ──

    #[test]
    fn conj_complex() {
        let v = call("conj", vec![ExprValue::Complex(Complex64::new(3.0, 4.0))]);
        assert_eq!(v, ExprValue::Complex(Complex64::new(3.0, -4.0)));
    }

    #[test]
    fn conj_real() {
        let v = call("conj", vec![ExprValue::Float(5.0)]);
        assert_eq!(v, ExprValue::Float(5.0));
    }

    #[test]
    fn norm_complex() {
        let v = call("norm", vec![ExprValue::Complex(Complex64::new(3.0, 4.0))]);
        assert_eq!(v, ExprValue::Float(25.0));
    }

    #[test]
    fn arg_complex() {
        let v = call("arg", vec![ExprValue::Complex(Complex64::new(0.0, 1.0))]);
        match v {
            ExprValue::Float(f) => assert!((f - std::f64::consts::FRAC_PI_2).abs() < 1e-10),
            _ => panic!("expected Float"),
        }
    }

    #[test]
    fn phase_negative_real() {
        let v = call("phase", vec![ExprValue::Float(-1.0)]);
        assert_eq!(v, ExprValue::Float(std::f64::consts::PI));
    }

    // ── square, cube ──

    #[test]
    fn square_int() {
        let v = call("square", vec![ExprValue::Int(7)]);
        assert_eq!(v, ExprValue::Int(49));
    }

    #[test]
    fn sqr_float() {
        let v = call("sqr", vec![ExprValue::Float(3.0)]);
        assert_eq!(v, ExprValue::Float(9.0));
    }

    #[test]
    fn cube_int() {
        let v = call("cube", vec![ExprValue::Int(3)]);
        assert_eq!(v, ExprValue::Int(27));
    }

    // ── bool conversion ──

    #[test]
    fn bool_from_int() {
        let v = call("bool", vec![ExprValue::Int(0)]);
        assert_eq!(v, ExprValue::Bool(false));
        let v = call("boolean", vec![ExprValue::Int(1)]);
        assert_eq!(v, ExprValue::Bool(true));
    }

    // ── near, nearabs ──

    #[test]
    fn near_default_tolerance() {
        let v = call(
            "near",
            vec![ExprValue::Float(1.0), ExprValue::Float(1.0 + 1e-14)],
        );
        assert_eq!(v, ExprValue::Bool(true));
        let v = call("near", vec![ExprValue::Float(1.0), ExprValue::Float(2.0)]);
        assert_eq!(v, ExprValue::Bool(false));
    }

    #[test]
    fn nearabs_custom_tolerance() {
        let v = call(
            "nearabs",
            vec![
                ExprValue::Float(1.0),
                ExprValue::Float(1.05),
                ExprValue::Float(0.1),
            ],
        );
        assert_eq!(v, ExprValue::Bool(true));
    }

    // ── isfinite, isnull, isdefined ──

    #[test]
    fn isfinite_true() {
        let v = call("isfinite", vec![ExprValue::Float(1.0)]);
        assert_eq!(v, ExprValue::Bool(true));
    }

    #[test]
    fn isfinite_false() {
        let v = call("isfinite", vec![ExprValue::Float(f64::INFINITY)]);
        assert_eq!(v, ExprValue::Bool(false));
    }

    #[test]
    fn isnull_true() {
        let v = call("isnull", vec![ExprValue::Null]);
        assert_eq!(v, ExprValue::Bool(true));
    }

    #[test]
    fn isdefined_true() {
        let v = call("isdefined", vec![ExprValue::Int(1)]);
        assert_eq!(v, ExprValue::Bool(true));
    }

    // ── capitalize, sreverse ──

    #[test]
    fn capitalize_basic() {
        let v = call(
            "capitalize",
            vec![ExprValue::String("hello world".to_string())],
        );
        assert_eq!(v, ExprValue::String("Hello World".to_string()));
    }

    #[test]
    fn sreverse_basic() {
        let v = call("sreverse", vec![ExprValue::String("abc".to_string())]);
        assert_eq!(v, ExprValue::String("cba".to_string()));
    }

    // ── rand ──

    #[test]
    fn rand_in_range() {
        let v = call("rand", vec![]);
        match v {
            ExprValue::Float(f) => assert!((0.0..1.0).contains(&f)),
            _ => panic!("expected Float"),
        }
    }

    // ── rownumber, rowid ──

    #[test]
    fn rownumber_basic() {
        // dummy_ctx uses row_index=7, so rownumber is 8 (1-based)
        let v = call("rownumber", vec![]);
        assert_eq!(v, ExprValue::Int(8));
    }

    #[test]
    fn rowid_basic() {
        // dummy_ctx uses row_index=7
        let v = call("rowid", vec![]);
        assert_eq!(v, ExprValue::Int(7));
    }

    // ── hms, dms, hdms ──

    #[test]
    fn hms_zero() {
        let v = call("hms", vec![ExprValue::Float(0.0)]);
        assert_eq!(v, ExprValue::String("00:00:00.000".to_string()));
    }

    #[test]
    fn dms_zero() {
        let v = call("dms", vec![ExprValue::Float(0.0)]);
        assert_eq!(v, ExprValue::String("+000.00.00.000".to_string()));
    }

    #[test]
    fn hdms_basic() {
        let v = call("hdms", vec![ExprValue::Float(0.0), ExprValue::Float(0.0)]);
        assert_eq!(
            v,
            ExprValue::String("00:00:00.000/+000.00.00.000".to_string())
        );
    }

    #[test]
    fn dms_negative() {
        let v = call("dms", vec![ExprValue::Float(-std::f64::consts::FRAC_PI_2)]);
        match v {
            ExprValue::String(s) => assert!(s.starts_with('-')),
            _ => panic!("expected String"),
        }
    }

    // ── iscolumn, iskeyword ──

    #[test]
    fn iscolumn_exists() {
        // dummy_ctx has "col_a"
        let v = call("iscolumn", vec![ExprValue::String("col_a".to_string())]);
        assert_eq!(v, ExprValue::Bool(true));
    }

    #[test]
    fn iscolumn_not_exists() {
        let v = call("iscolumn", vec![ExprValue::String("no_such".to_string())]);
        assert_eq!(v, ExprValue::Bool(false));
    }

    #[test]
    fn iskeyword_always_false() {
        let v = call("iskeyword", vec![ExprValue::String("any".to_string())]);
        assert_eq!(v, ExprValue::Bool(false));
    }

    // ══════════════════════════════════════════════════════════════
    // Wave 5a: Date/Time tests
    // ══════════════════════════════════════════════════════════════

    #[test]
    fn datetime_parse_iso() {
        let v = call(
            "datetime",
            vec![ExprValue::String("2000-01-01".to_string())],
        );
        match v {
            ExprValue::DateTime(mjd) => {
                // MJD of 2000-01-01 = 51544
                assert!((mjd - 51544.0).abs() < 0.001, "got MJD {mjd}");
            }
            _ => panic!("expected DateTime, got {v:?}"),
        }
    }

    #[test]
    fn datetime_with_time() {
        let v = call(
            "datetime",
            vec![ExprValue::String("2000-01-01T12:00:00".to_string())],
        );
        match v {
            ExprValue::DateTime(mjd) => {
                assert!((mjd - 51544.5).abs() < 0.001, "got MJD {mjd}");
            }
            _ => panic!("expected DateTime"),
        }
    }

    #[test]
    fn mjdtodate_basic() {
        let v = call("mjdtodate", vec![ExprValue::DateTime(51544.0)]);
        assert_eq!(v, ExprValue::String("2000-01-01".to_string()));
    }

    #[test]
    fn mjd_extract() {
        let v = call("mjd", vec![ExprValue::DateTime(51544.5)]);
        assert_eq!(v, ExprValue::Float(51544.5));
    }

    #[test]
    fn date_truncate() {
        let v = call("date", vec![ExprValue::DateTime(51544.75)]);
        assert_eq!(v, ExprValue::DateTime(51544.0));
    }

    #[test]
    fn time_fraction() {
        let v = call("time", vec![ExprValue::DateTime(51544.5)]);
        assert_eq!(v, ExprValue::Float(0.5));
    }

    #[test]
    fn year_month_day() {
        let dt = ExprValue::DateTime(51544.0); // 2000-01-01
        assert_eq!(call("year", vec![dt.clone()]), ExprValue::Int(2000));
        assert_eq!(call("month", vec![dt.clone()]), ExprValue::Int(1));
        assert_eq!(call("day", vec![dt]), ExprValue::Int(1));
    }

    #[test]
    fn cmonth_basic() {
        let v = call("cmonth", vec![ExprValue::DateTime(51544.0)]);
        assert_eq!(v, ExprValue::String("Jan".to_string()));
    }

    #[test]
    fn weekday_basic() {
        // 2000-01-01 was a Saturday = 5
        let v = call("weekday", vec![ExprValue::DateTime(51544.0)]);
        assert_eq!(v, ExprValue::Int(5));
    }

    #[test]
    fn cdow_basic() {
        let v = call("cdow", vec![ExprValue::DateTime(51544.0)]);
        assert_eq!(v, ExprValue::String("Sat".to_string()));
    }

    #[test]
    fn week_basic() {
        let v = call("week", vec![ExprValue::DateTime(51544.0)]);
        match v {
            ExprValue::Int(w) => assert!((1..=53).contains(&w)),
            _ => panic!("expected Int"),
        }
    }

    #[test]
    fn ctod_basic() {
        let v = call("ctod", vec![ExprValue::String("2000-01-01".to_string())]);
        match v {
            ExprValue::DateTime(mjd) => assert!((mjd - 51544.0).abs() < 0.001),
            _ => panic!("expected DateTime"),
        }
    }

    #[test]
    fn cdate_basic() {
        let v = call("cdate", vec![ExprValue::DateTime(51544.0)]);
        assert_eq!(v, ExprValue::String("2000/01/01".to_string()));
    }

    #[test]
    fn ctime_noon() {
        let v = call("ctime", vec![ExprValue::DateTime(51544.5)]);
        assert_eq!(v, ExprValue::String("12:00:00.000".to_string()));
    }

    // ══════════════════════════════════════════════════════════════
    // Wave 5b: Array reduction tests
    // ══════════════════════════════════════════════════════════════

    fn make_float_array(vals: Vec<f64>) -> ExprValue {
        let n = vals.len();
        ExprValue::Array(super::super::eval::ArrayValue {
            shape: vec![n],
            data: vals.into_iter().map(ExprValue::Float).collect(),
        })
    }

    fn make_bool_array(vals: Vec<bool>) -> ExprValue {
        let n = vals.len();
        ExprValue::Array(super::super::eval::ArrayValue {
            shape: vec![n],
            data: vals.into_iter().map(ExprValue::Bool).collect(),
        })
    }

    #[test]
    fn sum_array() {
        let v = call("sum", vec![make_float_array(vec![1.0, 2.0, 3.0])]);
        assert_eq!(v, ExprValue::Float(6.0));
    }

    #[test]
    fn product_array() {
        let v = call("product", vec![make_float_array(vec![2.0, 3.0, 4.0])]);
        assert_eq!(v, ExprValue::Float(24.0));
    }

    #[test]
    fn sumsqr_array() {
        let v = call("sumsqr", vec![make_float_array(vec![1.0, 2.0, 3.0])]);
        assert_eq!(v, ExprValue::Float(14.0));
    }

    #[test]
    fn mean_array() {
        let v = call("mean", vec![make_float_array(vec![1.0, 2.0, 3.0])]);
        assert_eq!(v, ExprValue::Float(2.0));
    }

    #[test]
    fn variance_array() {
        let v = call(
            "variance",
            vec![make_float_array(vec![
                2.0, 4.0, 4.0, 4.0, 5.0, 5.0, 7.0, 9.0,
            ])],
        );
        match v {
            ExprValue::Float(f) => assert!((f - 4.0).abs() < 0.01, "got {f}"),
            _ => panic!("expected Float"),
        }
    }

    #[test]
    fn stddev_array() {
        let v = call(
            "stddev",
            vec![make_float_array(vec![
                2.0, 4.0, 4.0, 4.0, 5.0, 5.0, 7.0, 9.0,
            ])],
        );
        match v {
            ExprValue::Float(f) => assert!((f - 2.0).abs() < 0.01, "got {f}"),
            _ => panic!("expected Float"),
        }
    }

    #[test]
    fn median_odd() {
        let v = call("median", vec![make_float_array(vec![3.0, 1.0, 2.0])]);
        assert_eq!(v, ExprValue::Float(2.0));
    }

    #[test]
    fn median_even() {
        let v = call("median", vec![make_float_array(vec![1.0, 2.0, 3.0, 4.0])]);
        assert_eq!(v, ExprValue::Float(2.5));
    }

    #[test]
    fn rms_array() {
        let v = call("rms", vec![make_float_array(vec![1.0, 2.0, 3.0])]);
        match v {
            ExprValue::Float(f) => {
                let expected = ((1.0 + 4.0 + 9.0) / 3.0_f64).sqrt();
                assert!((f - expected).abs() < 1e-10);
            }
            _ => panic!("expected Float"),
        }
    }

    #[test]
    fn fractile_array() {
        let v = call(
            "fractile",
            vec![
                make_float_array(vec![1.0, 2.0, 3.0, 4.0, 5.0]),
                ExprValue::Float(0.5),
            ],
        );
        assert_eq!(v, ExprValue::Float(3.0));
    }

    #[test]
    fn any_array() {
        assert_eq!(
            call("any", vec![make_bool_array(vec![false, true, false])]),
            ExprValue::Bool(true)
        );
        assert_eq!(
            call("any", vec![make_bool_array(vec![false, false])]),
            ExprValue::Bool(false)
        );
    }

    #[test]
    fn all_array() {
        assert_eq!(
            call("all", vec![make_bool_array(vec![true, true])]),
            ExprValue::Bool(true)
        );
        assert_eq!(
            call("all", vec![make_bool_array(vec![true, false])]),
            ExprValue::Bool(false)
        );
    }

    #[test]
    fn ntrue_array() {
        let v = call(
            "ntrue",
            vec![make_bool_array(vec![true, false, true, true])],
        );
        assert_eq!(v, ExprValue::Int(3));
    }

    #[test]
    fn nfalse_array() {
        let v = call("nfalse", vec![make_bool_array(vec![true, false, true])]);
        assert_eq!(v, ExprValue::Int(1));
    }

    #[test]
    fn amin_amax() {
        let arr = make_float_array(vec![3.0, 1.0, 4.0, 1.0, 5.0]);
        assert_eq!(call("amin", vec![arr.clone()]), ExprValue::Float(1.0));
        assert_eq!(call("amax", vec![arr]), ExprValue::Float(5.0));
    }

    // ══════════════════════════════════════════════════════════════
    // Wave 5c: Array manipulation tests
    // ══════════════════════════════════════════════════════════════

    #[test]
    fn array_create() {
        let v = call("array", vec![ExprValue::Float(0.0), ExprValue::Int(3)]);
        match v {
            ExprValue::Array(arr) => {
                assert_eq!(arr.shape, vec![3]);
                assert_eq!(arr.data.len(), 3);
            }
            _ => panic!("expected Array"),
        }
    }

    #[test]
    fn flatten_array() {
        let arr = ExprValue::Array(super::super::eval::ArrayValue {
            shape: vec![2, 3],
            data: (0..6).map(ExprValue::Int).collect(),
        });
        let v = call("flatten", vec![arr]);
        match v {
            ExprValue::Array(arr) => {
                assert_eq!(arr.shape, vec![6]);
                assert_eq!(arr.data.len(), 6);
            }
            _ => panic!("expected Array"),
        }
    }

    #[test]
    fn diagonal_2d() {
        let arr = ExprValue::Array(super::super::eval::ArrayValue {
            shape: vec![3, 3],
            data: vec![
                ExprValue::Float(1.0),
                ExprValue::Float(2.0),
                ExprValue::Float(3.0),
                ExprValue::Float(4.0),
                ExprValue::Float(5.0),
                ExprValue::Float(6.0),
                ExprValue::Float(7.0),
                ExprValue::Float(8.0),
                ExprValue::Float(9.0),
            ],
        });
        let v = call("diagonal", vec![arr]);
        match v {
            ExprValue::Array(arr) => {
                assert_eq!(arr.shape, vec![3]);
                assert_eq!(arr.data[0], ExprValue::Float(1.0));
                assert_eq!(arr.data[1], ExprValue::Float(5.0));
                assert_eq!(arr.data[2], ExprValue::Float(9.0));
            }
            _ => panic!("expected Array"),
        }
    }

    #[test]
    fn shape_ndim_nelements() {
        let arr = ExprValue::Array(super::super::eval::ArrayValue {
            shape: vec![2, 3],
            data: vec![ExprValue::Float(0.0); 6],
        });
        assert_eq!(call("ndim", vec![arr.clone()]), ExprValue::Int(2));
        assert_eq!(call("nelements", vec![arr.clone()]), ExprValue::Int(6));
        match call("shape", vec![arr]) {
            ExprValue::Array(s) => {
                assert_eq!(s.data, vec![ExprValue::Int(2), ExprValue::Int(3)]);
            }
            _ => panic!("expected Array"),
        }
    }

    #[test]
    fn areverse_array() {
        let arr = make_float_array(vec![1.0, 2.0, 3.0]);
        let v = call("areverse", vec![arr]);
        match v {
            ExprValue::Array(arr) => {
                assert_eq!(arr.data[0], ExprValue::Float(3.0));
                assert_eq!(arr.data[2], ExprValue::Float(1.0));
            }
            _ => panic!("expected Array"),
        }
    }

    #[test]
    fn nullarray_basic() {
        let v = call("nullarray", vec![ExprValue::Int(2), ExprValue::Int(3)]);
        match v {
            ExprValue::Array(arr) => {
                assert_eq!(arr.shape, vec![2, 3]);
                assert_eq!(arr.data.len(), 6);
                assert!(arr.data[0].is_null());
            }
            _ => panic!("expected Array"),
        }
    }

    #[test]
    fn resize_array() {
        let arr = make_float_array(vec![1.0, 2.0]);
        let v = call("resize", vec![arr, ExprValue::Int(4)]);
        match v {
            ExprValue::Array(a) => {
                assert_eq!(a.shape, vec![4]);
                assert_eq!(a.data[0], ExprValue::Float(1.0));
                assert_eq!(a.data[1], ExprValue::Float(2.0));
                assert_eq!(a.data[2], ExprValue::Float(0.0)); // padded
            }
            _ => panic!("expected Array"),
        }
    }

    // ══════════════════════════════════════════════════════════════
    // Wave 5d: Astronomy tests
    // ══════════════════════════════════════════════════════════════

    #[test]
    fn angdist_same_point() {
        let v = call(
            "angdist",
            vec![
                ExprValue::Float(0.5),
                ExprValue::Float(0.3),
                ExprValue::Float(0.5),
                ExprValue::Float(0.3),
            ],
        );
        match v {
            ExprValue::Float(f) => assert!(f.abs() < 1e-10),
            _ => panic!("expected Float"),
        }
    }

    #[test]
    fn angdist_poles() {
        // Distance between north pole and south pole = pi
        let v = call(
            "angdist",
            vec![
                ExprValue::Float(0.0),
                ExprValue::Float(std::f64::consts::FRAC_PI_2),
                ExprValue::Float(0.0),
                ExprValue::Float(-std::f64::consts::FRAC_PI_2),
            ],
        );
        match v {
            ExprValue::Float(f) => assert!((f - std::f64::consts::PI).abs() < 1e-10),
            _ => panic!("expected Float"),
        }
    }

    #[test]
    fn angdistx_same_as_angdist() {
        let args = vec![
            ExprValue::Float(0.1),
            ExprValue::Float(0.2),
            ExprValue::Float(0.3),
            ExprValue::Float(0.4),
        ];
        let d1 = call("angdist", args.clone());
        let d2 = call("angdistx", args);
        match (d1, d2) {
            (ExprValue::Float(a), ExprValue::Float(b)) => assert!((a - b).abs() < 1e-10),
            _ => panic!("expected Floats"),
        }
    }

    #[test]
    fn normangle_basic() {
        let v = call("normangle", vec![ExprValue::Float(4.0)]);
        match v {
            ExprValue::Float(f) => {
                let pi = std::f64::consts::PI;
                assert!(f >= -pi && f < pi, "got {f}");
            }
            _ => panic!("expected Float"),
        }
    }

    #[test]
    fn anycone_inside() {
        let v = call(
            "anycone",
            vec![
                ExprValue::Float(1.0),
                ExprValue::Float(0.5),
                ExprValue::Float(1.0),
                ExprValue::Float(0.5),
                ExprValue::Float(0.1),
            ],
        );
        assert_eq!(v, ExprValue::Bool(true));
    }

    #[test]
    fn anycone_outside() {
        let v = call(
            "anycone",
            vec![
                ExprValue::Float(0.0),
                ExprValue::Float(0.0),
                ExprValue::Float(1.0),
                ExprValue::Float(1.0),
                ExprValue::Float(0.01),
            ],
        );
        assert_eq!(v, ExprValue::Bool(false));
    }

    #[test]
    fn findcone_basic() {
        let v = call(
            "findcone",
            vec![
                ExprValue::Float(1.0),
                ExprValue::Float(0.5),
                ExprValue::Float(1.0),
                ExprValue::Float(0.5),
                ExprValue::Float(0.1),
            ],
        );
        assert_eq!(v, ExprValue::Int(0));
    }

    // ── Wave 4: Running and boxed window functions ──

    fn make_array(vals: Vec<f64>) -> ExprValue {
        let data = vals.into_iter().map(ExprValue::Float).collect::<Vec<_>>();
        let len = data.len();
        ExprValue::Array(super::super::eval::ArrayValue {
            shape: vec![len],
            data,
        })
    }

    fn extract_floats(val: ExprValue) -> Vec<f64> {
        match val {
            ExprValue::Array(a) => a.data.iter().map(|v| v.to_float().unwrap()).collect(),
            _ => panic!("expected Array"),
        }
    }

    #[test]
    fn runningsum_basic() {
        // halfWidth=1: window of 3 centered on each element
        // [1,2,3,4]: i=0 [1,2]→3, i=1 [1,2,3]→6, i=2 [2,3,4]→9, i=3 [3,4]→7
        let v = call(
            "runningsum",
            vec![make_array(vec![1.0, 2.0, 3.0, 4.0]), ExprValue::Int(1)],
        );
        let result = extract_floats(v);
        assert_eq!(result, vec![3.0, 6.0, 9.0, 7.0]);
    }

    #[test]
    fn runningmean_basic() {
        // halfWidth=1: window of 3 centered on each element
        // [2,4,6]: i=0 [2,4]→3, i=1 [2,4,6]→4, i=2 [4,6]→5
        let v = call(
            "runningmean",
            vec![make_array(vec![2.0, 4.0, 6.0]), ExprValue::Int(1)],
        );
        let result = extract_floats(v);
        assert_eq!(result, vec![3.0, 4.0, 5.0]);
    }

    #[test]
    fn runningmin_basic() {
        // halfWidth=1
        // [3,1,4,1]: i=0 [3,1]→1, i=1 [3,1,4]→1, i=2 [1,4,1]→1, i=3 [4,1]→1
        let v = call(
            "runningmin",
            vec![make_array(vec![3.0, 1.0, 4.0, 1.0]), ExprValue::Int(1)],
        );
        let result = extract_floats(v);
        assert_eq!(result, vec![1.0, 1.0, 1.0, 1.0]);
    }

    #[test]
    fn runningmax_basic() {
        // halfWidth=1
        // [1,3,2,4]: i=0 [1,3]→3, i=1 [1,3,2]→3, i=2 [3,2,4]→4, i=3 [2,4]→4
        let v = call(
            "runningmax",
            vec![make_array(vec![1.0, 3.0, 2.0, 4.0]), ExprValue::Int(1)],
        );
        let result = extract_floats(v);
        assert_eq!(result, vec![3.0, 3.0, 4.0, 4.0]);
    }

    #[test]
    fn runningmedian_basic() {
        // halfWidth=1
        // [3,1,4]: i=0 [3,1]→2, i=1 [3,1,4]→3, i=2 [1,4]→2.5
        let v = call(
            "runningmedian",
            vec![make_array(vec![3.0, 1.0, 4.0]), ExprValue::Int(1)],
        );
        let result = extract_floats(v);
        assert_eq!(result, vec![2.0, 3.0, 2.5]);
    }

    #[test]
    fn runningany_basic() {
        // halfWidth=1
        // [0,0,1,0]: i=0 [0,0]→F, i=1 [0,0,1]→T, i=2 [0,1,0]→T, i=3 [1,0]→T
        let v = call(
            "runningany",
            vec![make_array(vec![0.0, 0.0, 1.0, 0.0]), ExprValue::Int(1)],
        );
        match v {
            ExprValue::Array(a) => {
                let bools: Vec<bool> = a
                    .data
                    .iter()
                    .map(|v| match v {
                        ExprValue::Bool(b) => *b,
                        _ => panic!("expected Bool"),
                    })
                    .collect();
                assert_eq!(bools, vec![false, true, true, true]);
            }
            _ => panic!("expected Array"),
        }
    }

    #[test]
    fn runningall_basic() {
        // halfWidth=1
        // [1,1,0,1]: i=0 [1,1]→T, i=1 [1,1,0]→F, i=2 [1,0,1]→F, i=3 [0,1]→F
        let v = call(
            "runningall",
            vec![make_array(vec![1.0, 1.0, 0.0, 1.0]), ExprValue::Int(1)],
        );
        match v {
            ExprValue::Array(a) => {
                let bools: Vec<bool> = a
                    .data
                    .iter()
                    .map(|v| match v {
                        ExprValue::Bool(b) => *b,
                        _ => panic!("expected Bool"),
                    })
                    .collect();
                assert_eq!(bools, vec![true, false, false, false]);
            }
            _ => panic!("expected Array"),
        }
    }

    #[test]
    fn boxedmean_basic() {
        // box_size=3, array=[1,2,3,4,5]
        // i=0: window [1,2] → 1.5
        // i=1: window [1,2,3] → 2.0
        // i=2: window [2,3,4] → 3.0
        // i=3: window [3,4,5] → 4.0
        // i=4: window [4,5] → 4.5
        let v = call(
            "boxedmean",
            vec![make_array(vec![1.0, 2.0, 3.0, 4.0, 5.0]), ExprValue::Int(3)],
        );
        let result = extract_floats(v);
        assert_eq!(result, vec![1.5, 2.0, 3.0, 4.0, 4.5]);
    }

    #[test]
    fn boxedmin_basic() {
        let v = call(
            "boxedmin",
            vec![make_array(vec![3.0, 1.0, 4.0, 1.0, 5.0]), ExprValue::Int(3)],
        );
        let result = extract_floats(v);
        // i=0: [3,1]→1, i=1: [3,1,4]→1, i=2: [1,4,1]→1, i=3: [4,1,5]→1, i=4: [1,5]→1
        assert_eq!(result, vec![1.0, 1.0, 1.0, 1.0, 1.0]);
    }

    #[test]
    fn boxedmax_boundary() {
        let v = call(
            "boxedmax",
            vec![make_array(vec![1.0, 5.0, 3.0]), ExprValue::Int(1)],
        );
        let result = extract_floats(v);
        // box_size=1: each element is its own window
        assert_eq!(result, vec![1.0, 5.0, 3.0]);
    }

    #[test]
    fn boxed_zero_size_error() {
        let result = call_err("boxedsum", &[make_array(vec![1.0, 2.0]), ExprValue::Int(0)]);
        assert!(result.is_err());
    }

    #[test]
    fn running_empty_array() {
        let v = call("runningsum", vec![make_array(vec![]), ExprValue::Int(1)]);
        match v {
            ExprValue::Array(a) => assert!(a.data.is_empty()),
            _ => panic!("expected empty Array"),
        }
    }

    // ── Wave 5: Partial-axis array reductions ──

    fn make_2d_array_f(rows: usize, cols: usize, vals: Vec<f64>) -> ExprValue {
        ExprValue::Array(super::super::eval::ArrayValue {
            shape: vec![rows, cols],
            data: vals.into_iter().map(ExprValue::Float).collect(),
        })
    }

    #[test]
    fn sums_full_reduction() {
        let v = call("sums", vec![make_array(vec![1.0, 2.0, 3.0])]);
        assert_eq!(v, ExprValue::Float(6.0));
    }

    #[test]
    fn means_full_reduction() {
        let v = call("means", vec![make_array(vec![2.0, 4.0, 6.0])]);
        assert_eq!(v, ExprValue::Float(4.0));
    }

    #[test]
    fn mins_full_reduction() {
        let v = call("mins", vec![make_array(vec![3.0, 1.0, 4.0])]);
        assert_eq!(v, ExprValue::Float(1.0));
    }

    #[test]
    fn sums_axis0_2d() {
        // 2x3 array: [[1,2,3],[4,5,6]] stored in column-major flat order
        // Sum along axis 1 (1-based) = axis 0 (0-based) → [5, 7, 9]
        let v = call(
            "sums",
            vec![
                make_2d_array_f(2, 3, vec![1.0, 4.0, 2.0, 5.0, 3.0, 6.0]),
                ExprValue::Int(1),
            ],
        );
        match v {
            ExprValue::Array(a) => {
                assert_eq!(a.shape, vec![3]);
                let vals: Vec<f64> = a.data.iter().map(|v| v.to_float().unwrap()).collect();
                assert_eq!(vals, vec![5.0, 7.0, 9.0]);
            }
            _ => panic!("expected Array"),
        }
    }

    #[test]
    fn sums_axis1_2d() {
        // 2x3 array: [[1,2,3],[4,5,6]] stored in column-major flat order
        // Sum along axis 2 (1-based) = axis 1 (0-based) → [6, 15]
        let v = call(
            "sums",
            vec![
                make_2d_array_f(2, 3, vec![1.0, 4.0, 2.0, 5.0, 3.0, 6.0]),
                ExprValue::Int(2),
            ],
        );
        match v {
            ExprValue::Array(a) => {
                assert_eq!(a.shape, vec![2]);
                let vals: Vec<f64> = a.data.iter().map(|v| v.to_float().unwrap()).collect();
                assert_eq!(vals, vec![6.0, 15.0]);
            }
            _ => panic!("expected Array"),
        }
    }

    #[test]
    fn means_axis0_2d() {
        // 2x3 array: [[1,2,3],[4,5,6]] stored in column-major flat order
        // Mean along axis 1 (1-based) = axis 0 (0-based)
        let v = call(
            "means",
            vec![
                make_2d_array_f(2, 3, vec![1.0, 4.0, 2.0, 5.0, 3.0, 6.0]),
                ExprValue::Int(1),
            ],
        );
        match v {
            ExprValue::Array(a) => {
                assert_eq!(a.shape, vec![3]);
                let vals: Vec<f64> = a.data.iter().map(|v| v.to_float().unwrap()).collect();
                assert_eq!(vals, vec![2.5, 3.5, 4.5]);
            }
            _ => panic!("expected Array"),
        }
    }

    #[test]
    fn maxs_axis1_2d() {
        // 2x3 array: [[1,5,3],[4,2,6]] stored in column-major flat order
        // Max along axis 2 (1-based) = axis 1 (0-based)
        let v = call(
            "maxs",
            vec![
                make_2d_array_f(2, 3, vec![1.0, 4.0, 5.0, 2.0, 3.0, 6.0]),
                ExprValue::Int(2),
            ],
        );
        match v {
            ExprValue::Array(a) => {
                assert_eq!(a.shape, vec![2]);
                let vals: Vec<f64> = a.data.iter().map(|v| v.to_float().unwrap()).collect();
                assert_eq!(vals, vec![5.0, 6.0]);
            }
            _ => panic!("expected Array"),
        }
    }

    #[test]
    fn sums_axis_out_of_range() {
        // 1D array: axis 2 (1-based) is out of range
        let result = call_err("sums", &[make_array(vec![1.0, 2.0]), ExprValue::Int(2)]);
        assert!(result.is_err());
    }

    #[test]
    fn anys_full_reduction() {
        let v = call("anys", vec![make_array(vec![0.0, 0.0, 1.0])]);
        assert_eq!(v, ExprValue::Bool(true));
    }

    #[test]
    fn alls_full_reduction() {
        let v = call("alls", vec![make_array(vec![1.0, 1.0, 0.0])]);
        assert_eq!(v, ExprValue::Bool(false));
    }

    // ── Wave 7: Missing utility functions ──

    #[test]
    fn pattern_glob() {
        let v = call("pattern", vec![ExprValue::String("*.fits".into())]);
        assert_eq!(v, ExprValue::String("^.*\\.fits$".into()));
    }

    #[test]
    fn sqlpattern_like() {
        let v = call("sqlpattern", vec![ExprValue::String("test%".into())]);
        assert_eq!(v, ExprValue::String("^test.*$".into()));
    }

    #[test]
    fn sqlpattern_underscore() {
        let v = call("sqlpattern", vec![ExprValue::String("a_b".into())]);
        assert_eq!(v, ExprValue::String("^a.b$".into()));
    }

    #[test]
    fn rand_returns_float() {
        let v = call("rand", vec![]);
        match v {
            ExprValue::Float(f) => {
                assert!((0.0..1.0).contains(&f), "rand() returned {f}");
            }
            _ => panic!("expected Float"),
        }
    }

    #[test]
    fn near_basic() {
        let v = call(
            "near",
            vec![ExprValue::Float(1.0), ExprValue::Float(1.0000000000001)],
        );
        assert_eq!(v, ExprValue::Bool(true));
    }

    #[test]
    fn nearabs_basic() {
        let v = call(
            "nearabs",
            vec![
                ExprValue::Float(1.0),
                ExprValue::Float(1.1),
                ExprValue::Float(0.2),
            ],
        );
        assert_eq!(v, ExprValue::Bool(true));
    }

    #[test]
    fn transpose_2d() {
        // [[1,2,3],[4,5,6]] → [[1,4],[2,5],[3,6]]
        // Input column-major flat: [1,4,2,5,3,6]
        // Output column-major flat: [1,2,3,4,5,6]
        let v = call(
            "transpose",
            vec![make_2d_array_f(2, 3, vec![1.0, 4.0, 2.0, 5.0, 3.0, 6.0])],
        );
        match v {
            ExprValue::Array(a) => {
                assert_eq!(a.shape, vec![3, 2]);
                let vals: Vec<f64> = a.data.iter().map(|v| v.to_float().unwrap()).collect();
                assert_eq!(vals, vec![1.0, 2.0, 3.0, 4.0, 5.0, 6.0]);
            }
            _ => panic!("expected Array"),
        }
    }

    #[test]
    fn diagonal_3x3() {
        // 3x3 array: [[1,2,3],[4,5,6],[7,8,9]] stored in column-major flat order
        let v = call(
            "diagonal",
            vec![make_2d_array_f(
                3,
                3,
                vec![1.0, 4.0, 7.0, 2.0, 5.0, 8.0, 3.0, 6.0, 9.0],
            )],
        );
        match v {
            ExprValue::Array(a) => {
                assert_eq!(a.shape, vec![3]);
                let vals: Vec<f64> = a.data.iter().map(|v| v.to_float().unwrap()).collect();
                assert_eq!(vals, vec![1.0, 5.0, 9.0]);
            }
            _ => panic!("expected Array"),
        }
    }

    // ── Wave 12: UDF framework ──────────────────────────────────

    #[test]
    fn udf_register_and_call() {
        let _guard = udf_test_guard();
        super::register_udf("myudf", |args, _ctx| {
            let v = args[0].to_float()?;
            Ok(ExprValue::Float(v * 3.0))
        });
        let v = call("myudf", vec![ExprValue::Float(10.0)]);
        assert_eq!(v, ExprValue::Float(30.0));
        super::unregister_udf("myudf");
    }

    #[test]
    fn udf_overrides_builtin() {
        let _guard = udf_test_guard();
        // Register a UDF that overrides the built-in "abs"
        super::register_udf("abs", |_args, _ctx| Ok(ExprValue::Float(999.0)));
        let v = call("abs", vec![ExprValue::Float(-5.0)]);
        assert_eq!(v, ExprValue::Float(999.0)); // UDF, not built-in abs
        super::unregister_udf("abs");
        // After unregister, built-in should work again
        let v = call("abs", vec![ExprValue::Float(-5.0)]);
        assert_eq!(v, ExprValue::Float(5.0));
    }

    #[test]
    fn udf_unregistered_falls_through() {
        let _guard = udf_test_guard();
        // Calling a name with no UDF or built-in should error
        let ctx = EvalContext {
            row: &casacore_types::RecordValue::default(),
            row_index: 0,
            style: crate::taql::ast::IndexStyle::default(),
        };
        let result = super::call_function("nonexistent_udf_xyz", &[], &ctx);
        assert!(result.is_err());
    }

    #[test]
    fn udf_clear_all() {
        let _guard = udf_test_guard();
        super::register_udf("tmp_udf1", |_, _| Ok(ExprValue::Int(1)));
        super::register_udf("tmp_udf2", |_, _| Ok(ExprValue::Int(2)));
        super::clear_udfs();
        let ctx = EvalContext {
            row: &casacore_types::RecordValue::default(),
            row_index: 0,
            style: crate::taql::ast::IndexStyle::default(),
        };
        assert!(super::call_function("tmp_udf1", &[], &ctx).is_err());
        assert!(super::call_function("tmp_udf2", &[], &ctx).is_err());
    }
}

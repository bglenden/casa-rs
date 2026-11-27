// SPDX-License-Identifier: LGPL-3.0-or-later

use super::*;

#[expect(
    clippy::too_many_lines,
    reason = "predates the function-length limit; #705"
)]
pub(super) fn call_builtin(
    lower: &str,
    name: &str,
    args: &[ExprValue],
    ctx: &EvalContext<'_>,
) -> Result<ExprValue, TaqlError> {
    match lower {
        "mscal.help" | "derivedmscal.help" => super::super::mscal_help::help(name, args),
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
        "sin" => map_unary(name, args, |arg| Ok(ExprValue::Float(angle(arg)?.sin()))),
        "cos" => map_unary(name, args, |arg| Ok(ExprValue::Float(angle(arg)?.cos()))),
        "tan" => map_unary(name, args, |arg| Ok(ExprValue::Float(angle(arg)?.tan()))),
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
        "abs" => map_unary(name, args, |value| match value {
            ExprValue::Int(n) => Ok(ExprValue::Int(n.abs())),
            ExprValue::Float(v) => Ok(ExprValue::Float(v.abs())),
            ExprValue::Complex(c) => Ok(ExprValue::Float(c.norm())),
            ExprValue::Quantity(q) => Ok(ExprValue::Quantity(Quantity::with_unit(
                q.value().abs(),
                q.unit().clone(),
            ))),
            other => Err(TaqlError::TypeError {
                message: format!("abs() cannot operate on {}", other.type_name()),
            }),
        }),
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
        "real" | "float" | "double" => map_unary(name, args, |value| match value {
            ExprValue::Complex(c) => Ok(ExprValue::Float(c.re)),
            _ => Ok(ExprValue::Float(value.to_float()?)),
        }),
        "imag" => map_unary(name, args, |value| match value {
            ExprValue::Complex(c) => Ok(ExprValue::Float(c.im)),
            _ => Ok(ExprValue::Float(0.0)),
        }),
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
                    Ok(ExprValue::Array(super::super::eval::ArrayValue {
                        shape: vec![arr.shape.len()],
                        data,
                    }))
                }
                ExprValue::Null => Ok(ExprValue::Null),
                // Scalar values have shape []
                _ => Ok(ExprValue::Array(super::super::eval::ArrayValue {
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
            let n = if ctx.style.is_one_based() {
                ctx.row_index + 1
            } else {
                ctx.row_index
            };
            Ok(ExprValue::Int(n as i64))
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
            let med = if n.is_multiple_of(2) {
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
        "any" | "all" | "ntrue" | "nfalse" => {
            check_arity(name, args, 1)?;
            masked_arrays::reduce_bool(lower, &args[0])
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
            Ok(ExprValue::Array(super::super::eval::ArrayValue {
                shape,
                data,
            }))
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
            Ok(ExprValue::Array(super::super::eval::ArrayValue {
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
            Ok(ExprValue::Array(super::super::eval::ArrayValue {
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
            Ok(ExprValue::Array(super::super::eval::ArrayValue {
                shape: vec![n],
                data,
            }))
        }
        "flatten" | "arrayflatten" => {
            // Flatten to 1D
            check_arity(name, args, 1)?;
            let arr = require_array(name, &args[0])?;
            Ok(ExprValue::Array(super::super::eval::ArrayValue {
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
            Ok(ExprValue::Array(super::super::eval::ArrayValue {
                shape,
                data,
            }))
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
            Ok(ExprValue::Array(super::super::eval::ArrayValue {
                shape: new_shape,
                data,
            }))
        }
        "arraydata" | "getarrdata" | "arraymask" | "getarrmask" | "negatemask" => {
            check_arity(name, args, 1)?;
            masked_arrays::inspect(lower, &args[0])
        }
        "marray" => {
            check_arity(name, args, 2)?;
            masked_arrays::create(&args[0], &args[1])
        }
        "replacemasked" | "replaceunmasked" => {
            check_arity(name, args, 2)?;
            masked_arrays::replace(lower, &args[0], &args[1])
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

        _ if lower.starts_with("meas.") => {
            super::super::meas_udf::call_meas_function_with_provider(lower, args, ctx.measures)
        }

        _ => Err(TaqlError::UnknownFunction {
            name: name.to_string(),
        }),
    }
}

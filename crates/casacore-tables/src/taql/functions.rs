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

use super::error::TaqlError;
use super::eval::ExprValue;
use num_complex::Complex64;

/// Call a built-in TaQL function by name.
///
/// Function names are matched case-insensitively.
pub fn call_function(name: &str, args: &[ExprValue]) -> Result<ExprValue, TaqlError> {
    let lower = name.to_lowercase();
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
        "shape" | "ndim" | "nelements" => {
            // These operate on array values, which are not yet supported in ExprValue.
            // Return NULL for now.
            check_arity(name, args, 1)?;
            Ok(ExprValue::Null)
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

#[cfg(test)]
mod tests {
    use super::*;

    fn call(name: &str, args: Vec<ExprValue>) -> ExprValue {
        call_function(name, &args).unwrap()
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
        let err = call_function("sin", &[ExprValue::Float(1.0), ExprValue::Float(2.0)]);
        assert!(matches!(err, Err(TaqlError::ArgumentCount { .. })));
    }

    #[test]
    fn unknown_function() {
        let err = call_function("nonexistent", &[]);
        assert!(matches!(err, Err(TaqlError::UnknownFunction { .. })));
    }

    #[test]
    fn type_error_upper() {
        let err = call_function("upper", &[ExprValue::Int(42)]);
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
}

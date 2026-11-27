// SPDX-License-Identifier: LGPL-3.0-or-later

use super::*;
use casa_core::{RecordField, RecordValue, ScalarValue, Value};
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
        measures: None,
    };
    call_function(name, &args, &ctx).unwrap()
}

fn call_err(name: &str, args: &[ExprValue]) -> Result<ExprValue, TaqlError> {
    let (row, idx) = dummy_ctx();
    let ctx = EvalContext {
        row: &row,
        row_index: idx,
        style: crate::taql::ast::IndexStyle::default(),
        measures: None,
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
        row: &casa_core::RecordValue::default(),
        row_index: 0,
        style: crate::taql::ast::IndexStyle::default(),
        measures: None,
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
        row: &casa_core::RecordValue::default(),
        row_index: 0,
        style: crate::taql::ast::IndexStyle::default(),
        measures: None,
    };
    assert!(super::call_function("tmp_udf1", &[], &ctx).is_err());
    assert!(super::call_function("tmp_udf2", &[], &ctx).is_err());
}

mod arrays;

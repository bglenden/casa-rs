// SPDX-License-Identifier: LGPL-3.0-or-later
use super::*;

#[test]
fn type_promotion_int_float() {
    let (a, b) = promote(ExprValue::Int(2), ExprValue::Float(3.0)).unwrap();
    match (a, b) {
        (ExprValue::Float(a), ExprValue::Float(b)) => {
            assert_eq!(a, 2.0);
            assert_eq!(b, 3.0);
        }
        _ => panic!("expected Float"),
    }
}

#[test]
fn type_promotion_bool_int() {
    let (a, b) = promote(ExprValue::Bool(true), ExprValue::Int(5)).unwrap();
    match (a, b) {
        (ExprValue::Int(a), ExprValue::Int(b)) => {
            assert_eq!(a, 1);
            assert_eq!(b, 5);
        }
        _ => panic!("expected Int"),
    }
}

#[test]
fn add_int_float() {
    let result = eval_binary(BinaryOp::Add, ExprValue::Int(2), ExprValue::Float(3.0)).unwrap();
    assert_eq!(result, ExprValue::Float(5.0));
}

#[test]
fn integer_operands_divide_as_real_in_casa() {
    let result = eval_binary(BinaryOp::Div, ExprValue::Int(7), ExprValue::Int(2)).unwrap();
    assert!(matches!(result, ExprValue::Float(3.5)));
}

#[test]
fn complex_arithmetic() {
    let a = ExprValue::Complex(Complex64::new(1.0, 2.0));
    let b = ExprValue::Complex(Complex64::new(3.0, 4.0));
    let result = eval_binary(BinaryOp::Add, a, b).unwrap();
    assert_eq!(result, ExprValue::Complex(Complex64::new(4.0, 6.0)));
}

#[test]
fn comparison_across_types() {
    let a = ExprValue::Int(1);
    let b = ExprValue::Float(2.0);
    let result = eval_binary(BinaryOp::Lt, a, b).unwrap();
    assert_eq!(result, ExprValue::Bool(true));
}

#[test]
fn like_basic() {
    assert!(like_match("hello", "h%"));
    assert!(like_match("hello", "h_llo"));
    assert!(like_match("hello", "%llo"));
    assert!(like_match("hello", "hello"));
    assert!(!like_match("hello", "world"));
    assert!(like_match("hello", "%"));
    assert!(like_match("", "%"));
    assert!(!like_match("", "_"));
}

#[test]
fn like_case_insensitive() {
    // ILIKE is handled by lowercasing both strings before matching
    assert!(like_match(&"Hello".to_lowercase(), &"hello".to_lowercase()));
    assert!(like_match(
        &"HELLO WORLD".to_lowercase(),
        &"hello%".to_lowercase()
    ));
}

#[test]
fn null_propagation() {
    let result = eval_binary(BinaryOp::Add, ExprValue::Null, ExprValue::Int(1)).unwrap();
    assert!(result.is_null());
}

#[test]
fn null_propagation_compare() {
    let result = eval_binary(BinaryOp::Eq, ExprValue::Null, ExprValue::Int(1)).unwrap();
    assert!(result.is_null());
}

#[test]
fn unary_negate() {
    let result = eval_unary(UnaryOp::Negate, ExprValue::Int(5)).unwrap();
    assert_eq!(result, ExprValue::Int(-5));
}

#[test]
fn unary_not() {
    let result = eval_unary(UnaryOp::Not, ExprValue::Bool(true)).unwrap();
    assert_eq!(result, ExprValue::Bool(false));
}

#[test]
fn real_division_by_zero_is_ieee_as_in_casa() {
    let result = eval_binary(BinaryOp::Div, ExprValue::Int(1), ExprValue::Int(0)).unwrap();
    assert!(matches!(result, ExprValue::Float(value) if value == f64::INFINITY));
}

#[test]
fn power_int() {
    let result = eval_binary(BinaryOp::Power, ExprValue::Int(2), ExprValue::Int(10)).unwrap();
    assert_eq!(result, ExprValue::Int(1024));
}

#[test]
fn power_float() {
    let result = eval_binary(
        BinaryOp::Power,
        ExprValue::Float(2.0),
        ExprValue::Float(0.5),
    )
    .unwrap();
    match result {
        ExprValue::Float(v) => assert!((v - std::f64::consts::SQRT_2).abs() < 1e-10),
        _ => panic!("expected Float"),
    }
}

#[test]
fn modulo_int() {
    let result = eval_binary(BinaryOp::Modulo, ExprValue::Int(7), ExprValue::Int(3)).unwrap();
    assert_eq!(result, ExprValue::Int(1));
}

#[test]
fn string_concat() {
    let result = eval_binary(
        BinaryOp::Add,
        ExprValue::String("hello".to_string()),
        ExprValue::String(" world".to_string()),
    )
    .unwrap();
    assert_eq!(result, ExprValue::String("hello world".to_string()));
}

#[test]
fn eval_scalar_value_conversion() {
    assert_eq!(ExprValue::from(&ScalarValue::Int32(42)), ExprValue::Int(42));
    assert_eq!(
        ExprValue::from(&ScalarValue::Float64(3.15)),
        ExprValue::Float(3.15)
    );
    assert_eq!(
        ExprValue::from(&ScalarValue::String("test".to_string())),
        ExprValue::String("test".to_string())
    );
    assert_eq!(
        ExprValue::from(&ScalarValue::Bool(true)),
        ExprValue::Bool(true)
    );
}

#[test]
fn eval_expression_against_row() {
    // Build a row: {flux: 2.78, id: 5}
    let row = RecordValue::new(vec![
        casa_types::RecordField::new("flux", Value::Scalar(ScalarValue::Float64(3.15))),
        casa_types::RecordField::new("id", Value::Scalar(ScalarValue::Int32(5))),
    ]);
    let ctx = EvalContext {
        row: &row,
        row_index: 0,
        style: ast::IndexStyle::default(),
        measures: None,
    };

    // Evaluate: flux * 2.0
    let expr = Expr::Binary {
        left: Box::new(Expr::ColumnRef(ColumnRef {
            table: None,
            column: "flux".to_string(),
        })),
        op: BinaryOp::Mul,
        right: Box::new(Expr::Literal(Literal::Float(2.0))),
    };
    let result = eval_expr(&expr, &ctx).unwrap();
    assert_eq!(result, ExprValue::Float(6.30));
}

#[test]
fn eval_row_number() {
    let row = RecordValue::new(vec![]);
    let ctx = EvalContext {
        row: &row,
        row_index: 42,
        style: ast::IndexStyle::default(),
        measures: None,
    };
    let result = eval_expr(&Expr::RowNumber, &ctx).unwrap();
    assert_eq!(result, ExprValue::Int(42));
}

#[test]
fn like_complex_patterns() {
    assert!(like_match("abcdef", "a%d%f"));
    assert!(like_match("abcdef", "a_c_e_"));
    assert!(!like_match("abcdef", "a_c_e"));
    assert!(like_match("abc", "a%%c"));
}

// ── ExprValue type conversion tests ──

#[test]
fn type_name_all_variants() {
    assert_eq!(ExprValue::Bool(true).type_name(), "Bool");
    assert_eq!(ExprValue::Int(0).type_name(), "Int");
    assert_eq!(ExprValue::Float(0.0).type_name(), "Float");
    assert_eq!(
        ExprValue::Complex(Complex64::new(0.0, 0.0)).type_name(),
        "Complex"
    );
    assert_eq!(ExprValue::String("".into()).type_name(), "String");
    assert_eq!(ExprValue::DateTime(0.0).type_name(), "DateTime");
    assert_eq!(
        ExprValue::Array(ArrayValue {
            shape: vec![],
            data: vec![]
        })
        .type_name(),
        "Array"
    );
    assert_eq!(ExprValue::Null.type_name(), "Null");
}

#[test]
fn to_bool_conversions() {
    assert!(ExprValue::Bool(true).to_bool().unwrap());
    assert!(!ExprValue::Bool(false).to_bool().unwrap());
    assert!(ExprValue::Int(1).to_bool().unwrap());
    assert!(!ExprValue::Int(0).to_bool().unwrap());
    assert!(!ExprValue::Null.to_bool().unwrap());
    assert!(ExprValue::Float(1.0).to_bool().is_err());
}

#[test]
fn to_int_conversions() {
    assert_eq!(ExprValue::Bool(true).to_int().unwrap(), 1);
    assert_eq!(ExprValue::Bool(false).to_int().unwrap(), 0);
    assert_eq!(ExprValue::Int(42).to_int().unwrap(), 42);
    assert_eq!(ExprValue::Float(3.7).to_int().unwrap(), 3);
    assert!(ExprValue::String("x".into()).to_int().is_err());
}

#[test]
fn to_float_conversions() {
    assert_eq!(ExprValue::Bool(true).to_float().unwrap(), 1.0);
    assert_eq!(ExprValue::Int(5).to_float().unwrap(), 5.0);
    assert_eq!(ExprValue::Float(2.5).to_float().unwrap(), 2.5);
    assert_eq!(ExprValue::DateTime(51544.0).to_float().unwrap(), 51544.0);
    assert!(ExprValue::String("x".into()).to_float().is_err());
}

#[test]
fn compare_null_handling() {
    assert_eq!(
        ExprValue::Null.compare(&ExprValue::Null).unwrap(),
        Ordering::Equal
    );
    assert_eq!(
        ExprValue::Null.compare(&ExprValue::Int(1)).unwrap(),
        Ordering::Less
    );
    assert_eq!(
        ExprValue::Int(1).compare(&ExprValue::Null).unwrap(),
        Ordering::Greater
    );
}

#[test]
fn compare_datetime() {
    let a = ExprValue::DateTime(51544.0);
    let b = ExprValue::DateTime(51545.0);
    assert_eq!(a.compare(&b).unwrap(), Ordering::Less);
    assert_eq!(b.compare(&a).unwrap(), Ordering::Greater);
    assert_eq!(a.compare(&a).unwrap(), Ordering::Equal);
}

#[test]
fn compare_cross_type_bool_float() {
    let a = ExprValue::Bool(true);
    let b = ExprValue::Float(0.5);
    assert_eq!(a.compare(&b).unwrap(), Ordering::Greater);
}

#[test]
fn compare_strings() {
    let a = ExprValue::String("apple".into());
    let b = ExprValue::String("banana".into());
    assert_eq!(a.compare(&b).unwrap(), Ordering::Less);
}

#[test]
fn display_datetime() {
    let dt = ExprValue::DateTime(51544.5);
    let s = format!("{dt}");
    assert!(s.starts_with("MJD("));
}

#[test]
fn display_array() {
    let arr = ExprValue::Array(ArrayValue {
        shape: vec![3],
        data: vec![ExprValue::Int(1), ExprValue::Int(2), ExprValue::Int(3)],
    });
    let s = format!("{arr}");
    assert!(s.starts_with("Array["));
}

// ── Promotion tests ──

#[test]
fn promote_bool_to_complex() {
    let (a, _) = promote(
        ExprValue::Bool(true),
        ExprValue::Complex(Complex64::new(2.0, 0.0)),
    )
    .unwrap();
    assert!(matches!(a, ExprValue::Complex(_)));
}

#[test]
fn promote_int_to_complex() {
    let (a, _) = promote(
        ExprValue::Int(3),
        ExprValue::Complex(Complex64::new(0.0, 1.0)),
    )
    .unwrap();
    match a {
        ExprValue::Complex(c) => assert_eq!(c.re, 3.0),
        _ => panic!("expected Complex"),
    }
}

#[test]
fn promote_float_to_complex() {
    let (a, _) = promote(
        ExprValue::Float(2.5),
        ExprValue::Complex(Complex64::new(0.0, 0.0)),
    )
    .unwrap();
    match a {
        ExprValue::Complex(c) => assert_eq!(c.re, 2.5),
        _ => panic!("expected Complex"),
    }
}

#[test]
fn promote_string_fails() {
    assert!(promote(ExprValue::String("x".into()), ExprValue::Int(1)).is_err());
}

// ── Binary op additional tests ──

#[test]
fn string_equality_check() {
    let result = eval_binary(
        BinaryOp::Eq,
        ExprValue::String("a".into()),
        ExprValue::String("a".into()),
    )
    .unwrap();
    assert_eq!(result, ExprValue::Bool(true));
}

#[test]
fn null_propagation_binary() {
    let result = eval_binary(BinaryOp::Add, ExprValue::Null, ExprValue::Int(1)).unwrap();
    assert_eq!(result, ExprValue::Null);
    let result = eval_binary(BinaryOp::Add, ExprValue::Int(1), ExprValue::Null).unwrap();
    assert_eq!(result, ExprValue::Null);
}

#[test]
fn sub_and_mul_float() {
    assert_eq!(
        eval_binary(BinaryOp::Sub, ExprValue::Float(5.0), ExprValue::Float(3.0)).unwrap(),
        ExprValue::Float(2.0)
    );
    assert_eq!(
        eval_binary(BinaryOp::Mul, ExprValue::Float(2.0), ExprValue::Float(3.0)).unwrap(),
        ExprValue::Float(6.0)
    );
}

#[test]
fn compare_ge_le() {
    assert_eq!(
        eval_binary(BinaryOp::Ge, ExprValue::Int(5), ExprValue::Int(3)).unwrap(),
        ExprValue::Bool(true)
    );
    assert_eq!(
        eval_binary(BinaryOp::Le, ExprValue::Int(3), ExprValue::Int(5)).unwrap(),
        ExprValue::Bool(true)
    );
    assert_eq!(
        eval_binary(BinaryOp::Ne, ExprValue::Int(1), ExprValue::Int(2)).unwrap(),
        ExprValue::Bool(true)
    );
}

#[test]
fn bool_logic_and_or() {
    assert_eq!(
        eval_binary(BinaryOp::And, ExprValue::Bool(true), ExprValue::Bool(false)).unwrap(),
        ExprValue::Bool(false)
    );
    assert_eq!(
        eval_binary(BinaryOp::Or, ExprValue::Bool(false), ExprValue::Bool(true)).unwrap(),
        ExprValue::Bool(true)
    );
}

// ── eval_expr tests ──

#[test]
fn eval_literal_values() {
    let row = casa_types::RecordValue::default();
    let ctx = EvalContext {
        row: &row,
        row_index: 0,
        style: ast::IndexStyle::default(),
        measures: None,
    };
    assert_eq!(
        eval_expr(&Expr::Literal(Literal::Int(42)), &ctx).unwrap(),
        ExprValue::Int(42)
    );
    assert_eq!(
        eval_expr(&Expr::Literal(Literal::Float(2.78)), &ctx).unwrap(),
        ExprValue::Float(2.78)
    );
    assert_eq!(
        eval_expr(&Expr::Literal(Literal::String("hi".into())), &ctx).unwrap(),
        ExprValue::String("hi".into())
    );
    assert_eq!(
        eval_expr(&Expr::Literal(Literal::Bool(true)), &ctx).unwrap(),
        ExprValue::Bool(true)
    );
    assert_eq!(
        eval_expr(&Expr::Literal(Literal::Null), &ctx).unwrap(),
        ExprValue::Null
    );
    let quantity = eval_expr(
        &Expr::Literal(Literal::Quantity {
            value: 2.0,
            unit: "rad".to_string(),
        }),
        &ctx,
    )
    .unwrap();
    assert!(matches!(quantity, ExprValue::Quantity(_)));
}

#[test]
fn eval_unary_not_expr() {
    let row = casa_types::RecordValue::default();
    let ctx = EvalContext {
        row: &row,
        row_index: 0,
        style: ast::IndexStyle::default(),
        measures: None,
    };
    let expr = Expr::Unary {
        op: UnaryOp::Not,
        operand: Box::new(Expr::Literal(Literal::Bool(true))),
    };
    assert_eq!(eval_expr(&expr, &ctx).unwrap(), ExprValue::Bool(false));
}

#[test]
fn eval_unary_negate_expr() {
    let row = casa_types::RecordValue::default();
    let ctx = EvalContext {
        row: &row,
        row_index: 0,
        style: ast::IndexStyle::default(),
        measures: None,
    };
    let expr = Expr::Unary {
        op: UnaryOp::Negate,
        operand: Box::new(Expr::Literal(Literal::Int(5))),
    };
    assert_eq!(eval_expr(&expr, &ctx).unwrap(), ExprValue::Int(-5));

    let expr = Expr::Unary {
        op: UnaryOp::Negate,
        operand: Box::new(Expr::Literal(Literal::Quantity {
            value: 5.0,
            unit: "nT".to_string(),
        })),
    };
    let result = eval_expr(&expr, &ctx).unwrap();
    match result {
        ExprValue::Quantity(q) => {
            assert!((q.value() + 5.0).abs() < 1e-12);
            assert_eq!(q.unit().name(), "nT");
        }
        other => panic!("expected Quantity, got {other:?}"),
    }
}

#[test]
fn eval_is_null_expr() {
    let row = casa_types::RecordValue::default();
    let ctx = EvalContext {
        row: &row,
        row_index: 0,
        style: ast::IndexStyle::default(),
        measures: None,
    };
    let expr = Expr::IsNull {
        expr: Box::new(Expr::Literal(Literal::Null)),
        negated: false,
    };
    assert_eq!(eval_expr(&expr, &ctx).unwrap(), ExprValue::Bool(true));

    let not_null = Expr::IsNull {
        expr: Box::new(Expr::Literal(Literal::Int(1))),
        negated: true,
    };
    assert_eq!(eval_expr(&not_null, &ctx).unwrap(), ExprValue::Bool(true));
}

#[test]
fn eval_between_expr() {
    let row = casa_types::RecordValue::default();
    let ctx = EvalContext {
        row: &row,
        row_index: 0,
        style: ast::IndexStyle::default(),
        measures: None,
    };
    let expr = Expr::Between {
        expr: Box::new(Expr::Literal(Literal::Int(5))),
        low: Box::new(Expr::Literal(Literal::Int(1))),
        high: Box::new(Expr::Literal(Literal::Int(10))),
        negated: false,
    };
    assert_eq!(eval_expr(&expr, &ctx).unwrap(), ExprValue::Bool(true));

    let not_between = Expr::Between {
        expr: Box::new(Expr::Literal(Literal::Int(15))),
        low: Box::new(Expr::Literal(Literal::Int(1))),
        high: Box::new(Expr::Literal(Literal::Int(10))),
        negated: false,
    };
    assert_eq!(
        eval_expr(&not_between, &ctx).unwrap(),
        ExprValue::Bool(false)
    );
}

#[test]
fn eval_in_expr() {
    let row = casa_types::RecordValue::default();
    let ctx = EvalContext {
        row: &row,
        row_index: 0,
        style: ast::IndexStyle::default(),
        measures: None,
    };
    let expr = Expr::In {
        expr: Box::new(Expr::Literal(Literal::Int(2))),
        values: vec![
            Expr::Literal(Literal::Int(1)),
            Expr::Literal(Literal::Int(2)),
            Expr::Literal(Literal::Int(3)),
        ],
        negated: false,
    };
    assert_eq!(eval_expr(&expr, &ctx).unwrap(), ExprValue::Bool(true));

    let not_in = Expr::In {
        expr: Box::new(Expr::Literal(Literal::Int(5))),
        values: vec![Expr::Literal(Literal::Int(1))],
        negated: false,
    };
    assert_eq!(eval_expr(&not_in, &ctx).unwrap(), ExprValue::Bool(false));
}

#[test]
fn eval_binary_expr() {
    let row = casa_types::RecordValue::default();
    let ctx = EvalContext {
        row: &row,
        row_index: 0,
        style: ast::IndexStyle::default(),
        measures: None,
    };
    let expr = Expr::Binary {
        left: Box::new(Expr::Literal(Literal::Int(3))),
        op: BinaryOp::Add,
        right: Box::new(Expr::Literal(Literal::Int(4))),
    };
    assert_eq!(eval_expr(&expr, &ctx).unwrap(), ExprValue::Int(7));
}

#[test]
fn eval_function_call_expr() {
    let row = casa_types::RecordValue::default();
    let ctx = EvalContext {
        row: &row,
        row_index: 0,
        style: ast::IndexStyle::default(),
        measures: None,
    };
    let expr = Expr::FunctionCall {
        name: "abs".to_string(),
        args: vec![Expr::Literal(Literal::Float(-5.0))],
    };
    assert_eq!(eval_expr(&expr, &ctx).unwrap(), ExprValue::Float(5.0));
}

// ── Wave 2: Bitwise operator tests ──

#[test]
fn bitwise_and_eval() {
    let result = eval_binary(BinaryOp::BitAnd, ExprValue::Int(0xFF), ExprValue::Int(0x0F)).unwrap();
    assert_eq!(result, ExprValue::Int(0x0F));
}

#[test]
fn bitwise_or_eval() {
    let result = eval_binary(BinaryOp::BitOr, ExprValue::Int(0xF0), ExprValue::Int(0x0F)).unwrap();
    assert_eq!(result, ExprValue::Int(0xFF));
}

#[test]
fn bitwise_xor_eval() {
    let result = eval_binary(BinaryOp::BitXor, ExprValue::Int(0xFF), ExprValue::Int(0x0F)).unwrap();
    assert_eq!(result, ExprValue::Int(0xF0));
}

#[test]
fn bitwise_not_eval() {
    let result = eval_unary(UnaryOp::BitNot, ExprValue::Int(0)).unwrap();
    assert_eq!(result, ExprValue::Int(-1)); // !0 = -1 in two's complement
}

// ── Wave 2: Regex match tests ──

#[test]
fn regex_match_positive() {
    let row = casa_types::RecordValue::default();
    let ctx = EvalContext {
        row: &row,
        row_index: 0,
        style: ast::IndexStyle::default(),
        measures: None,
    };
    let expr = Expr::RegexMatch {
        expr: Box::new(Expr::Literal(Literal::String("hello world".to_string()))),
        pattern: Box::new(Expr::Literal(Literal::Regex {
            pattern: "hello.*".to_string(),
            flags: String::new(),
        })),
        negated: false,
    };
    assert_eq!(eval_expr(&expr, &ctx).unwrap(), ExprValue::Bool(true));
}

#[test]
fn regex_match_negated() {
    let row = casa_types::RecordValue::default();
    let ctx = EvalContext {
        row: &row,
        row_index: 0,
        style: ast::IndexStyle::default(),
        measures: None,
    };
    let expr = Expr::RegexMatch {
        expr: Box::new(Expr::Literal(Literal::String("hello".to_string()))),
        pattern: Box::new(Expr::Literal(Literal::Regex {
            pattern: "world".to_string(),
            flags: String::new(),
        })),
        negated: true,
    };
    assert_eq!(eval_expr(&expr, &ctx).unwrap(), ExprValue::Bool(true));
}

#[test]
fn regex_case_insensitive() {
    let row = casa_types::RecordValue::default();
    let ctx = EvalContext {
        row: &row,
        row_index: 0,
        style: ast::IndexStyle::default(),
        measures: None,
    };
    let expr = Expr::RegexMatch {
        expr: Box::new(Expr::Literal(Literal::String("HELLO".to_string()))),
        pattern: Box::new(Expr::Literal(Literal::Regex {
            pattern: "hello".to_string(),
            flags: "i".to_string(),
        })),
        negated: false,
    };
    assert_eq!(eval_expr(&expr, &ctx).unwrap(), ExprValue::Bool(true));
}

// ── Wave 2: IN set/range tests ──

#[test]
fn in_set_range_membership() {
    let row = casa_types::RecordValue::default();
    let ctx = EvalContext {
        row: &row,
        row_index: 0,
        style: ast::IndexStyle::default(),
        measures: None,
    };
    let expr = Expr::InSet {
        expr: Box::new(Expr::Literal(Literal::Int(5))),
        elements: vec![ast::InSetElement::Range {
            start: Some(Expr::Literal(Literal::Int(1))),
            end: Some(Expr::Literal(Literal::Int(10))),
            step: None,
        }],
        negated: false,
    };
    assert_eq!(eval_expr(&expr, &ctx).unwrap(), ExprValue::Bool(true));
}

#[test]
fn in_set_range_outside() {
    let row = casa_types::RecordValue::default();
    let ctx = EvalContext {
        row: &row,
        row_index: 0,
        style: ast::IndexStyle::default(),
        measures: None,
    };
    let expr = Expr::InSet {
        expr: Box::new(Expr::Literal(Literal::Int(15))),
        elements: vec![ast::InSetElement::Range {
            start: Some(Expr::Literal(Literal::Int(1))),
            end: Some(Expr::Literal(Literal::Int(10))),
            step: None,
        }],
        negated: false,
    };
    assert_eq!(eval_expr(&expr, &ctx).unwrap(), ExprValue::Bool(false));
}

#[test]
fn in_set_discrete_values() {
    let row = casa_types::RecordValue::default();
    let ctx = EvalContext {
        row: &row,
        row_index: 0,
        style: ast::IndexStyle::default(),
        measures: None,
    };
    let expr = Expr::InSet {
        expr: Box::new(Expr::Literal(Literal::Int(2))),
        elements: vec![
            ast::InSetElement::Value(Expr::Literal(Literal::Int(1))),
            ast::InSetElement::Value(Expr::Literal(Literal::Int(2))),
            ast::InSetElement::Value(Expr::Literal(Literal::Int(3))),
        ],
        negated: false,
    };
    assert_eq!(eval_expr(&expr, &ctx).unwrap(), ExprValue::Bool(true));
}

// ── Wave 3: Array indexing, slicing, style modes ──

fn make_1d_array(vals: Vec<i64>) -> ArrayValue {
    let data = vals.into_iter().map(ExprValue::Int).collect::<Vec<_>>();
    let len = data.len();
    ArrayValue {
        shape: vec![len],
        data,
    }
}

fn make_2d_array(rows: usize, cols: usize, vals: Vec<i64>) -> ArrayValue {
    ArrayValue {
        shape: vec![rows, cols],
        data: vals.into_iter().map(ExprValue::Int).collect(),
    }
}

#[test]
fn glish_1based_single_index() {
    let row = casa_types::RecordValue::default();
    let ctx = EvalContext {
        row: &row,
        row_index: 0,
        style: ast::IndexStyle::Glish,
        measures: None,
    };
    let arr = make_1d_array(vec![10, 20, 30]);
    let result = eval_array_index(
        &arr,
        &[ast::IndexElement::Single(Expr::Literal(Literal::Int(1)))],
        &ctx,
    )
    .unwrap();
    assert_eq!(result, ExprValue::Int(10)); // 1-based: index 1 → element 0
}

#[test]
fn python_0based_single_index() {
    let row = casa_types::RecordValue::default();
    let ctx = EvalContext {
        row: &row,
        row_index: 0,
        style: ast::IndexStyle::Python,
        measures: None,
    };
    let arr = make_1d_array(vec![10, 20, 30]);
    let result = eval_array_index(
        &arr,
        &[ast::IndexElement::Single(Expr::Literal(Literal::Int(0)))],
        &ctx,
    )
    .unwrap();
    assert_eq!(result, ExprValue::Int(10)); // 0-based: index 0 → element 0
}

#[test]
fn python_negative_index() {
    let row = casa_types::RecordValue::default();
    let ctx = EvalContext {
        row: &row,
        row_index: 0,
        style: ast::IndexStyle::Python,
        measures: None,
    };
    let arr = make_1d_array(vec![10, 20, 30]);
    let result = eval_array_index(
        &arr,
        &[ast::IndexElement::Single(Expr::Literal(Literal::Int(-1)))],
        &ctx,
    )
    .unwrap();
    assert_eq!(result, ExprValue::Int(30)); // -1 → last element
}

#[test]
fn glish_slice_inclusive() {
    let row = casa_types::RecordValue::default();
    let ctx = EvalContext {
        row: &row,
        row_index: 0,
        style: ast::IndexStyle::Glish,
        measures: None,
    };
    let arr = make_1d_array(vec![10, 20, 30, 40, 50]);
    // Glish: 2:4 → indices 2,3,4 (1-based) → elements 1,2,3 (0-based)
    let result = eval_array_index(
        &arr,
        &[ast::IndexElement::Slice {
            start: Some(Expr::Literal(Literal::Int(2))),
            end: Some(Expr::Literal(Literal::Int(4))),
            step: None,
        }],
        &ctx,
    )
    .unwrap();
    match result {
        ExprValue::Array(a) => {
            assert_eq!(a.shape, vec![3]);
            assert_eq!(
                a.data,
                vec![ExprValue::Int(20), ExprValue::Int(30), ExprValue::Int(40)]
            );
        }
        _ => panic!("expected Array"),
    }
}

#[test]
fn python_slice_exclusive() {
    let row = casa_types::RecordValue::default();
    let ctx = EvalContext {
        row: &row,
        row_index: 0,
        style: ast::IndexStyle::Python,
        measures: None,
    };
    let arr = make_1d_array(vec![10, 20, 30, 40, 50]);
    // Python: 1:4 → indices 1,2,3 (0-based, end exclusive)
    let result = eval_array_index(
        &arr,
        &[ast::IndexElement::Slice {
            start: Some(Expr::Literal(Literal::Int(1))),
            end: Some(Expr::Literal(Literal::Int(4))),
            step: None,
        }],
        &ctx,
    )
    .unwrap();
    match result {
        ExprValue::Array(a) => {
            assert_eq!(a.shape, vec![3]);
            assert_eq!(
                a.data,
                vec![ExprValue::Int(20), ExprValue::Int(30), ExprValue::Int(40)]
            );
        }
        _ => panic!("expected Array"),
    }
}

#[test]
fn glish_slice_with_step() {
    let row = casa_types::RecordValue::default();
    let ctx = EvalContext {
        row: &row,
        row_index: 0,
        style: ast::IndexStyle::Glish,
        measures: None,
    };
    let arr = make_1d_array(vec![10, 20, 30, 40, 50]);
    // Glish: 1:5:2 → indices 1,3,5 (1-based) → elements 0,2,4
    let result = eval_array_index(
        &arr,
        &[ast::IndexElement::Slice {
            start: Some(Expr::Literal(Literal::Int(1))),
            end: Some(Expr::Literal(Literal::Int(5))),
            step: Some(Expr::Literal(Literal::Int(2))),
        }],
        &ctx,
    )
    .unwrap();
    match result {
        ExprValue::Array(a) => {
            assert_eq!(a.shape, vec![3]);
            assert_eq!(
                a.data,
                vec![ExprValue::Int(10), ExprValue::Int(30), ExprValue::Int(50)]
            );
        }
        _ => panic!("expected Array"),
    }
}

#[test]
fn python_slice_with_step() {
    let row = casa_types::RecordValue::default();
    let ctx = EvalContext {
        row: &row,
        row_index: 0,
        style: ast::IndexStyle::Python,
        measures: None,
    };
    let arr = make_1d_array(vec![10, 20, 30, 40, 50]);
    // Python: 0:5:2 → indices 0,2,4
    let result = eval_array_index(
        &arr,
        &[ast::IndexElement::Slice {
            start: Some(Expr::Literal(Literal::Int(0))),
            end: Some(Expr::Literal(Literal::Int(5))),
            step: Some(Expr::Literal(Literal::Int(2))),
        }],
        &ctx,
    )
    .unwrap();
    match result {
        ExprValue::Array(a) => {
            assert_eq!(a.shape, vec![3]);
            assert_eq!(
                a.data,
                vec![ExprValue::Int(10), ExprValue::Int(30), ExprValue::Int(50)]
            );
        }
        _ => panic!("expected Array"),
    }
}

#[test]
fn multidim_single_element() {
    let row = casa_types::RecordValue::default();
    let ctx = EvalContext {
        row: &row,
        row_index: 0,
        style: ast::IndexStyle::Glish,
        measures: None,
    };
    // 2x3 array: [[1,2,3],[4,5,6]]
    let arr = make_2d_array(2, 3, vec![1, 2, 3, 4, 5, 6]);
    // Glish: arr[2,3] → row=2 (0-based 1), col=3 (0-based 2) → flat index 1*3+2=5 → value 6
    let result = eval_array_index(
        &arr,
        &[
            ast::IndexElement::Single(Expr::Literal(Literal::Int(2))),
            ast::IndexElement::Single(Expr::Literal(Literal::Int(3))),
        ],
        &ctx,
    )
    .unwrap();
    assert_eq!(result, ExprValue::Int(6));
}

#[test]
fn multidim_row_slice() {
    let row = casa_types::RecordValue::default();
    let ctx = EvalContext {
        row: &row,
        row_index: 0,
        style: ast::IndexStyle::Python,
        measures: None,
    };
    // 2x3 array: [[1,2,3],[4,5,6]] in column-major flat order
    let arr = make_2d_array(2, 3, vec![1, 4, 2, 5, 3, 6]);
    // Python: arr[1] → row 1, all cols → [4,5,6]
    let result = eval_array_index(
        &arr,
        &[ast::IndexElement::Single(Expr::Literal(Literal::Int(1)))],
        &ctx,
    )
    .unwrap();
    match result {
        ExprValue::Array(a) => {
            assert_eq!(a.shape, vec![3]);
            assert_eq!(
                a.data,
                vec![ExprValue::Int(4), ExprValue::Int(5), ExprValue::Int(6)]
            );
        }
        _ => panic!("expected Array, got {result:?}"),
    }
}

#[test]
fn out_of_bounds_error() {
    let row = casa_types::RecordValue::default();
    let ctx = EvalContext {
        row: &row,
        row_index: 0,
        style: ast::IndexStyle::Python,
        measures: None,
    };
    let arr = make_1d_array(vec![10, 20, 30]);
    let result = eval_array_index(
        &arr,
        &[ast::IndexElement::Single(Expr::Literal(Literal::Int(5)))],
        &ctx,
    );
    assert!(result.is_err());
}

#[test]
fn slice_step_zero_error() {
    let row = casa_types::RecordValue::default();
    let ctx = EvalContext {
        row: &row,
        row_index: 0,
        style: ast::IndexStyle::Python,
        measures: None,
    };
    let arr = make_1d_array(vec![10, 20, 30]);
    let result = eval_array_index(
        &arr,
        &[ast::IndexElement::Slice {
            start: Some(Expr::Literal(Literal::Int(0))),
            end: Some(Expr::Literal(Literal::Int(3))),
            step: Some(Expr::Literal(Literal::Int(0))),
        }],
        &ctx,
    );
    assert!(result.is_err());
}

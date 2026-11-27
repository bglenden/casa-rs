// SPDX-License-Identifier: LGPL-3.0-or-later

use super::*;
use crate::temp_image::TempImage;
use casa_coordinates::CoordinateSystem;

fn make_test_image(shape: Vec<usize>, value: f32) -> TempImage<f32> {
    let mut img = TempImage::<f32>::new(
        shape,
        CoordinateSystem::new(),
        casa_lattices::TempStoragePolicy::Memory,
    )
    .unwrap();
    img.set(value).unwrap();
    img
}

/// Build a resolver from image-name / image-ref pairs.
/// Callers must keep the `TempImage` values alive for the resolver's
/// lifetime, so we accept already-populated HashMap content.
fn make_resolver<'a>(pairs: &[(&str, &'a TempImage<f32>)]) -> HashMapResolver<'a, f32> {
    let mut map = HashMap::new();
    for &(name, img) in pairs {
        map.insert(name.to_string(), img as &dyn ImageInterface<f32>);
    }
    HashMapResolver(map)
}

// -- Lexer tests --

#[test]
fn lex_basic_tokens() {
    let mut lex = Lexer::new("+ - * / ^ ( ) , == != > >= < <= && || !");
    let tokens: Vec<Token> = std::iter::from_fn(|| {
        let (tok, _) = lex.next_token().ok()?;
        if tok == Token::Eof { None } else { Some(tok) }
    })
    .collect();
    assert_eq!(
        tokens,
        vec![
            Token::Plus,
            Token::Minus,
            Token::Star,
            Token::Slash,
            Token::Caret,
            Token::LParen,
            Token::RParen,
            Token::Comma,
            Token::Eq,
            Token::Ne,
            Token::Gt,
            Token::Ge,
            Token::Lt,
            Token::Le,
            Token::And,
            Token::Or,
            Token::Bang,
        ]
    );
}

#[test]
fn lex_number_literals() {
    let cases = [
        ("42", 42.0),
        ("3.15", 3.15),
        (".5", 0.5),
        ("1e3", 1000.0),
        ("2.5E-1", 0.25),
        ("1.5d2", 150.0),
    ];
    for (input, expected) in cases {
        let mut lex = Lexer::new(input);
        let (tok, _) = lex.next_token().unwrap();
        if let Token::Number(v) = tok {
            assert!(
                (v - expected).abs() < 1e-10,
                "input={input}: got {v}, expected {expected}"
            );
        } else {
            panic!("expected Number for input={input}, got {tok:?}");
        }
    }
}

#[test]
fn lex_quoted_paths() {
    let mut lex = Lexer::new("'hello' \"world\"");
    let (t1, _) = lex.next_token().unwrap();
    let (t2, _) = lex.next_token().unwrap();
    assert_eq!(t1, Token::QuotedPath("hello".into()));
    assert_eq!(t2, Token::QuotedPath("world".into()));
}

#[test]
fn lex_unterminated_string() {
    let mut lex = Lexer::new("'abc");
    let result = lex.next_token();
    assert!(result.is_err());
    assert!(result.unwrap_err().message.contains("unterminated"));
}

#[test]
fn lex_bare_ident_with_dots() {
    let mut lex = Lexer::new("my_image.im");
    let (tok, _) = lex.next_token().unwrap();
    assert_eq!(tok, Token::Ident("my_image.im".into()));
}

// -- Parser tests --

#[test]
fn parse_simple_scalar_add() {
    let a = make_test_image(vec![2, 2], 3.0);
    let r = make_resolver(&[("a", &a)]);
    let expr = parse_image_expr("'a' + 1.0", &r).unwrap();
    assert_eq!(expr.get_at(&[0, 0]).unwrap(), 4.0);
}

#[test]
fn parse_arithmetic_precedence() {
    // 2.0 + 3.0 * a  with a=4.0 should be 2.0 + 12.0 = 14.0
    let a = make_test_image(vec![2, 2], 4.0);
    let r = make_resolver(&[("a", &a)]);
    let expr = parse_image_expr("2.0 + 3.0 * 'a'", &r).unwrap();
    assert_eq!(expr.get_at(&[0, 0]).unwrap(), 14.0);
}

#[test]
fn parse_power_right_associative() {
    // 2.0 ^ 3.0 ^ 2.0 = 2.0 ^ 9.0 = 512.0 (right-associative)
    let a = make_test_image(vec![2, 2], 1.0);
    let r = make_resolver(&[("a", &a)]);
    // Use image to anchor shape: a * 0 + (2^3^2)
    let expr = parse_image_expr("'a' * 0 + 2.0 ^ 3.0 ^ 2.0", &r).unwrap();
    assert_eq!(expr.get_at(&[0, 0]).unwrap(), 512.0);
}

#[test]
fn parse_unary_negate() {
    let a = make_test_image(vec![2, 2], 5.0);
    let r = make_resolver(&[("a", &a)]);
    let expr = parse_image_expr("-'a'", &r).unwrap();
    assert_eq!(expr.get_at(&[0, 0]).unwrap(), -5.0);
}

#[test]
fn parse_function_sin() {
    let a = make_test_image(vec![2, 2], 1.0);
    let r = make_resolver(&[("a", &a)]);
    let expr = parse_image_expr("sin('a')", &r).unwrap();
    let expected = 1.0_f32.sin();
    assert!((expr.get_at(&[0, 0]).unwrap() - expected).abs() < 1e-6);
}

#[test]
fn parse_function_pow_2arg() {
    let a = make_test_image(vec![2, 2], 3.0);
    let r = make_resolver(&[("a", &a)]);
    let expr = parse_image_expr("pow('a', 2.0)", &r).unwrap();
    assert_eq!(expr.get_at(&[0, 0]).unwrap(), 9.0);
}

#[test]
fn parse_constants_pi_e() {
    let a = make_test_image(vec![2, 2], 1.0);
    let r = make_resolver(&[("a", &a)]);
    let expr = parse_image_expr("'a' * pi()", &r).unwrap();
    let expected = std::f64::consts::PI as f32;
    assert!((expr.get_at(&[0, 0]).unwrap() - expected).abs() < 1e-5);
}

#[test]
fn parse_parenthesized_expression() {
    // (a + 1.0) * 2.0  with a=3.0 => 8.0
    let a = make_test_image(vec![2, 2], 3.0);
    let r = make_resolver(&[("a", &a)]);
    let expr = parse_image_expr("('a' + 1.0) * 2.0", &r).unwrap();
    assert_eq!(expr.get_at(&[0, 0]).unwrap(), 8.0);
}

#[test]
fn parse_complex_expression() {
    // sqrt(a^2 + 1.0) with a=3.0 => sqrt(10.0)
    let a = make_test_image(vec![2, 2], 3.0);
    let r = make_resolver(&[("a", &a)]);
    let expr = parse_image_expr("sqrt('a' ^ 2.0 + 1.0)", &r).unwrap();
    let expected = 10.0_f32.sqrt();
    assert!((expr.get_at(&[0, 0]).unwrap() - expected).abs() < 1e-5);
}

#[test]
fn parse_two_images() {
    let a = make_test_image(vec![2, 2], 3.0);
    let b = make_test_image(vec![2, 2], 4.0);
    let r = make_resolver(&[("a", &a), ("b", &b)]);
    let expr = parse_image_expr("'a' + 'b'", &r).unwrap();
    assert_eq!(expr.get_at(&[0, 0]).unwrap(), 7.0);
}

#[test]
fn parse_scalar_minus_image() {
    // 10.0 - a with a=3.0 => 7.0
    let a = make_test_image(vec![2, 2], 3.0);
    let r = make_resolver(&[("a", &a)]);
    let expr = parse_image_expr("10.0 - 'a'", &r).unwrap();
    assert_eq!(expr.get_at(&[0, 0]).unwrap(), 7.0);
}

#[test]
fn parse_scalar_divide_image() {
    // 12.0 / a with a=3.0 => 4.0
    let a = make_test_image(vec![2, 2], 3.0);
    let r = make_resolver(&[("a", &a)]);
    let expr = parse_image_expr("12.0 / 'a'", &r).unwrap();
    assert_eq!(expr.get_at(&[0, 0]).unwrap(), 4.0);
}

#[test]
fn parse_mask_comparison() {
    let a = make_test_image(vec![2, 2], 5.0);
    let r = make_resolver(&[("a", &a)]);
    let mask = parse_mask_expr("'a' > 3.0", &r).unwrap();
    assert!(mask.get_at(&[0, 0]).unwrap());
}

#[test]
fn parse_mask_and_or() {
    let a = make_test_image(vec![2, 2], 5.0);
    let r = make_resolver(&[("a", &a)]);
    let mask = parse_mask_expr("'a' > 3.0 && 'a' < 10.0", &r).unwrap();
    assert!(mask.get_at(&[0, 0]).unwrap());

    let mask2 = parse_mask_expr("'a' < 3.0 || 'a' > 4.0", &r).unwrap();
    assert!(mask2.get_at(&[0, 0]).unwrap());
}

#[test]
fn parse_mask_not() {
    let a = make_test_image(vec![2, 2], 5.0);
    let r = make_resolver(&[("a", &a)]);
    let mask = parse_mask_expr("!('a' < 3.0)", &r).unwrap();
    assert!(mask.get_at(&[0, 0]).unwrap());
}

#[test]
fn parse_bare_ident_image() {
    let a = make_test_image(vec![2, 2], 7.0);
    let r = make_resolver(&[("myimg", &a)]);
    let expr = parse_image_expr("myimg + 1.0", &r).unwrap();
    assert_eq!(expr.get_at(&[0, 0]).unwrap(), 8.0);
}

#[test]
fn parse_double_quoted_path() {
    let a = make_test_image(vec![2, 2], 2.0);
    let r = make_resolver(&[("test.image", &a)]);
    let expr = parse_image_expr("\"test.image\" * 3.0", &r).unwrap();
    assert_eq!(expr.get_at(&[0, 0]).unwrap(), 6.0);
}

#[test]
fn parse_case_insensitive_functions() {
    let a = make_test_image(vec![2, 2], 1.0);
    let r = make_resolver(&[("a", &a)]);
    let expr = parse_image_expr("SIN('a')", &r).unwrap();
    let expected = 1.0_f32.sin();
    assert!((expr.get_at(&[0, 0]).unwrap() - expected).abs() < 1e-6);
}

#[test]
fn parse_nested_functions() {
    // sqrt(abs(-4.0 + a)) with a=0 => sqrt(4.0) = 2.0
    let a = make_test_image(vec![2, 2], 0.0);
    let r = make_resolver(&[("a", &a)]);
    let expr = parse_image_expr("sqrt(abs(-4.0 + 'a'))", &r).unwrap();
    assert!((expr.get_at(&[0, 0]).unwrap() - 2.0).abs() < 1e-6);
}

#[test]
fn parse_min_max_functions() {
    let a = make_test_image(vec![2, 2], 5.0);
    let r = make_resolver(&[("a", &a)]);
    let expr = parse_image_expr("min('a', 3.0)", &r).unwrap();
    assert_eq!(expr.get_at(&[0, 0]).unwrap(), 3.0);

    let expr2 = parse_image_expr("max('a', 7.0)", &r).unwrap();
    assert_eq!(expr2.get_at(&[0, 0]).unwrap(), 7.0);
}

// -- Negative tests --

#[test]
fn parse_empty_expression() {
    let r: HashMapResolver<f32> = HashMapResolver(HashMap::new());
    let err = parse_image_expr("", &r).unwrap_err();
    assert!(err.message.contains("empty"));
}

#[test]
fn parse_whitespace_only() {
    let r: HashMapResolver<f32> = HashMapResolver(HashMap::new());
    let err = parse_image_expr("   ", &r).unwrap_err();
    assert!(err.message.contains("empty"));
}

#[test]
fn parse_unknown_image() {
    let r: HashMapResolver<f32> = HashMapResolver(HashMap::new());
    let err = parse_image_expr("'nonexistent.image'", &r).unwrap_err();
    assert!(err.message.contains("unknown image"));
}

#[test]
fn parse_unknown_function() {
    let a = make_test_image(vec![2, 2], 1.0);
    let r = make_resolver(&[("a", &a)]);
    let err = parse_image_expr("bogus('a')", &r).unwrap_err();
    assert!(err.message.contains("unknown"));
}

#[test]
fn parse_unterminated_quote() {
    let r: HashMapResolver<f32> = HashMapResolver(HashMap::new());
    let err = parse_image_expr("'abc", &r).unwrap_err();
    assert!(err.message.contains("unterminated"));
}

#[test]
fn parse_mismatched_parens() {
    let a = make_test_image(vec![2, 2], 1.0);
    let r = make_resolver(&[("a", &a)]);
    let err = parse_image_expr("('a' + 1.0", &r).unwrap_err();
    assert!(
        err.message.contains("expected )") || err.message.contains("expected ("),
        "got: {}",
        err.message
    );
}

#[test]
fn parse_trailing_operator() {
    let a = make_test_image(vec![2, 2], 1.0);
    let r = make_resolver(&[("a", &a)]);
    let err = parse_image_expr("'a' +", &r).unwrap_err();
    assert!(!err.message.is_empty());
}

#[test]
fn parse_numeric_where_mask_expected() {
    let a = make_test_image(vec![2, 2], 1.0);
    let r = make_resolver(&[("a", &a)]);
    let err = parse_mask_expr("'a' + 1.0", &r).unwrap_err();
    assert!(err.message.contains("boolean") || err.message.contains("mask"));
}

#[test]
fn parse_mask_where_numeric_expected() {
    let a = make_test_image(vec![2, 2], 1.0);
    let r = make_resolver(&[("a", &a)]);
    let err = parse_image_expr("'a' > 1.0", &r).unwrap_err();
    assert!(err.message.contains("boolean") || err.message.contains("mask"));
}

#[test]
fn parse_lone_equals_error() {
    let a = make_test_image(vec![2, 2], 1.0);
    let r = make_resolver(&[("a", &a)]);
    let err = parse_image_expr("'a' = 1.0", &r).unwrap_err();
    assert!(err.message.contains("=="));
}

#[test]
fn parse_flipped_comparison() {
    // 3.0 < a with a=5.0 => true (parsed as a > 3.0)
    let a = make_test_image(vec![2, 2], 5.0);
    let r = make_resolver(&[("a", &a)]);
    let mask = parse_mask_expr("3.0 < 'a'", &r).unwrap();
    assert!(mask.get_at(&[0, 0]).unwrap());
}

#[test]
fn scalar_minus_inf_preserves_inf() {
    // 10.0 - img where img contains Inf should yield -Inf, not NaN.
    let mut a = make_test_image(vec![2, 2], 0.0);
    a.put_at(f32::INFINITY, &[0, 0]).unwrap();
    let r = make_resolver(&[("a", &a)]);
    let expr = parse_image_expr("10.0 - 'a'", &r).unwrap();
    let val = expr.get_at(&[0, 0]).unwrap();
    assert!(val.is_infinite() && val < 0.0, "expected -Inf, got {val}");
}

#[test]
fn scalar_divide_inf_preserves_zero() {
    // 1.0 / img where img contains Inf should yield 0, not NaN.
    let mut a = make_test_image(vec![2, 2], 0.0);
    a.put_at(f32::INFINITY, &[0, 0]).unwrap();
    let r = make_resolver(&[("a", &a)]);
    let expr = parse_image_expr("1.0 / 'a'", &r).unwrap();
    let val = expr.get_at(&[0, 0]).unwrap();
    assert_eq!(val, 0.0, "expected 0.0, got {val}");
}

// ========================================================================
// Wave 14 parser tests
// ========================================================================

#[test]
fn parse_isnan() {
    let mut a = make_test_image(vec![2, 2], 1.0);
    a.put_at(f32::NAN, &[0, 0]).unwrap();
    let r = make_resolver(&[("a", &a)]);
    let mask = parse_mask_expr("isnan('a')", &r).unwrap();
    assert!(mask.get_at(&[0, 0]).unwrap());
    assert!(!mask.get_at(&[0, 1]).unwrap());
}

#[test]
fn parse_ndim() {
    // ndim returns a scalar; use in combined expression to get image shape
    let a = make_test_image(vec![4, 5, 6], 1.0);
    let r = make_resolver(&[("a", &a)]);
    let expr = parse_image_expr("'a' * 0.0 + ndim('a')", &r).unwrap();
    assert!((expr.get_at(&[0, 0, 0]).unwrap() - 3.0).abs() < 1e-5);
}

#[test]
fn parse_nelements() {
    let a = make_test_image(vec![4, 5], 1.0);
    let r = make_resolver(&[("a", &a)]);
    let expr = parse_image_expr("'a' * 0.0 + nelements('a')", &r).unwrap();
    assert!((expr.get_at(&[0, 0]).unwrap() - 20.0).abs() < 1e-5);
}

#[test]
fn parse_length() {
    let a = make_test_image(vec![4, 7], 1.0);
    let r = make_resolver(&[("a", &a)]);
    let expr = parse_image_expr("'a' * 0.0 + length('a', 1)", &r).unwrap();
    assert!((expr.get_at(&[0, 0]).unwrap() - 7.0).abs() < 1e-5);
}

/// Extract scalar from a 0-D reduction result.
fn scalar_of(expr: &ImageExpression<f32>) -> f32 {
    assert!(
        expr.shape().is_empty(),
        "expected 0-D reduction, got {:?}",
        expr.shape()
    );
    expr.get().unwrap()[ndarray::IxDyn(&[])]
}

#[test]
fn parse_sum() {
    let mut a = make_test_image(vec![2, 2], 0.0);
    a.put_slice(
        &ndarray::ArrayD::from_shape_vec(ndarray::IxDyn(&[2, 2]), vec![1.0, 2.0, 3.0, 4.0])
            .unwrap(),
        &[0, 0],
    )
    .unwrap();
    let r = make_resolver(&[("a", &a)]);
    let expr = parse_image_expr("sum('a')", &r).unwrap();
    assert!(expr.shape().is_empty(), "sum should produce 0-D");
    assert!((scalar_of(&expr) - 10.0).abs() < 1e-5);
}

#[test]
fn parse_min1d_max1d() {
    let mut a = make_test_image(vec![4], 0.0);
    a.put_slice(
        &ndarray::ArrayD::from_shape_vec(ndarray::IxDyn(&[4]), vec![3.0, 1.0, 4.0, 2.0]).unwrap(),
        &[0],
    )
    .unwrap();
    let r = make_resolver(&[("a", &a)]);
    let min_expr = parse_image_expr("min1d('a')", &r).unwrap();
    let max_expr = parse_image_expr("max1d('a')", &r).unwrap();
    assert!((scalar_of(&min_expr) - 1.0).abs() < 1e-5);
    assert!((scalar_of(&max_expr) - 4.0).abs() < 1e-5);
}

#[test]
fn parse_mean1d() {
    let mut a = make_test_image(vec![4], 0.0);
    a.put_slice(
        &ndarray::ArrayD::from_shape_vec(ndarray::IxDyn(&[4]), vec![1.0, 2.0, 3.0, 4.0]).unwrap(),
        &[0],
    )
    .unwrap();
    let r = make_resolver(&[("a", &a)]);
    let expr = parse_image_expr("mean1d('a')", &r).unwrap();
    assert!((scalar_of(&expr) - 2.5).abs() < 1e-5);
}

#[test]
fn parse_median1d() {
    let mut a = make_test_image(vec![5], 0.0);
    a.put_slice(
        &ndarray::ArrayD::from_shape_vec(ndarray::IxDyn(&[5]), vec![5.0, 1.0, 3.0, 4.0, 2.0])
            .unwrap(),
        &[0],
    )
    .unwrap();
    let r = make_resolver(&[("a", &a)]);
    let expr = parse_image_expr("median1d('a')", &r).unwrap();
    assert!((scalar_of(&expr) - 3.0).abs() < 1e-5);
}

#[test]
fn parse_fractile1d() {
    let mut a = make_test_image(vec![5], 0.0);
    a.put_slice(
        &ndarray::ArrayD::from_shape_vec(ndarray::IxDyn(&[5]), vec![10.0, 20.0, 30.0, 40.0, 50.0])
            .unwrap(),
        &[0],
    )
    .unwrap();
    let r = make_resolver(&[("a", &a)]);
    let expr = parse_image_expr("fractile1d('a', 0.5)", &r).unwrap();
    assert!((scalar_of(&expr) - 30.0).abs() < 1e-5);
}

#[test]
fn parse_fractilerange1d_three_args() {
    let mut a = make_test_image(vec![5], 0.0);
    a.put_slice(
        &ndarray::ArrayD::from_shape_vec(ndarray::IxDyn(&[5]), vec![10.0, 20.0, 30.0, 40.0, 50.0])
            .unwrap(),
        &[0],
    )
    .unwrap();
    let r = make_resolver(&[("a", &a)]);
    let expr = parse_image_expr("fractilerange1d('a', 0.25, 0.75)", &r).unwrap();
    let val = scalar_of(&expr);
    // 20.0 and 40.0 → range = 20.0
    assert!((val - 20.0).abs() < 1e-5, "got {val}");
}

#[test]
fn parse_iif() {
    let mut a = make_test_image(vec![4], 0.0);
    a.put_slice(
        &ndarray::ArrayD::from_shape_vec(ndarray::IxDyn(&[4]), vec![1.0, 5.0, 3.0, 7.0]).unwrap(),
        &[0],
    )
    .unwrap();
    let r = make_resolver(&[("a", &a)]);
    let expr = parse_image_expr("iif('a' > 4.0, 'a' + 100.0, 'a')", &r).unwrap();
    let result = expr.get().unwrap();
    let expected =
        ndarray::ArrayD::from_shape_vec(ndarray::IxDyn(&[4]), vec![1.0, 105.0, 3.0, 107.0])
            .unwrap();
    assert_eq!(result, expected);
}

#[test]
fn parse_ntrue_nfalse() {
    let mut a = make_test_image(vec![4], 0.0);
    a.put_slice(
        &ndarray::ArrayD::from_shape_vec(ndarray::IxDyn(&[4]), vec![1.0, 5.0, 3.0, 7.0]).unwrap(),
        &[0],
    )
    .unwrap();
    let r = make_resolver(&[("a", &a)]);
    let nt = parse_image_expr("ntrue('a' > 4.0)", &r).unwrap();
    let nf = parse_image_expr("nfalse('a' > 4.0)", &r).unwrap();
    assert!(nt.shape().is_empty(), "ntrue should produce 0-D");
    assert!((scalar_of(&nt) - 2.0).abs() < 1e-5);
    assert!((scalar_of(&nf) - 2.0).abs() < 1e-5);
}

#[test]
fn parse_all_any() {
    let a = make_test_image(vec![4], 3.0);
    let r = make_resolver(&[("a", &a)]);
    let all_mask = parse_mask_expr("all('a' > 2.0)", &r).unwrap();
    assert!(all_mask.shape().is_empty(), "all() should produce 0-D");
    assert!(all_mask.get().unwrap()[ndarray::IxDyn(&[])]);
    let any_mask = parse_mask_expr("any('a' > 5.0)", &r).unwrap();
    assert!(!any_mask.get().unwrap()[ndarray::IxDyn(&[])]);
}

#[test]
fn parse_value_is_identity() {
    let a = make_test_image(vec![2, 2], 7.0);
    let r = make_resolver(&[("a", &a)]);
    let expr = parse_image_expr("value('a')", &r).unwrap();
    assert!((expr.get_at(&[0, 0]).unwrap() - 7.0).abs() < 1e-5);
}

#[test]
fn parse_mask_and_replace_propagate_masked_derived_source() {
    let mut a = make_test_image(vec![4], 0.0);
    a.put_slice(
        &ndarray::ArrayD::from_shape_vec(ndarray::IxDyn(&[4]), vec![1.0, f32::INFINITY, 3.0, 4.0])
            .unwrap(),
        &[0],
    )
    .unwrap();
    a.make_mask("quality", true, true).unwrap();
    let mask = ndarray::ArrayD::from_shape_vec(ndarray::IxDyn(&[4]), vec![true, false, true, true])
        .unwrap();
    a.put_mask("quality", &mask).unwrap();
    a.set_default_mask("quality").unwrap();

    let r = make_resolver(&[("a", &a)]);

    let mask_expr = parse_mask_expr("mask('a' + 1.0)", &r).unwrap();
    assert_eq!(mask_expr.get().unwrap(), mask);

    let replaced = parse_image_expr("replace('a' + 1.0, 42.0)", &r).unwrap();
    let expected =
        ndarray::ArrayD::from_shape_vec(ndarray::IxDyn(&[4]), vec![2.0, 42.0, 4.0, 5.0]).unwrap();
    assert_eq!(replaced.get().unwrap(), expected);
}

#[test]
fn parse_real_imag_error_for_real_types() {
    let a = make_test_image(vec![2, 2], -3.0);
    let r = make_resolver(&[("a", &a)]);
    let real_expr = parse_image_expr("real('a')", &r).unwrap();
    assert!((real_expr.get_at(&[0, 0]).unwrap() + 3.0).abs() < 1e-5);
    let float_expr = parse_image_expr("float('a')", &r).unwrap();
    assert!((float_expr.get_at(&[0, 0]).unwrap() + 3.0).abs() < 1e-5);
    let double_expr = parse_image_expr("double('a')", &r).unwrap();
    assert!((double_expr.get_at(&[0, 0]).unwrap() + 3.0).abs() < 1e-5);
    let amp_expr = parse_image_expr("amplitude('a')", &r).unwrap();
    assert!((amp_expr.get_at(&[0, 0]).unwrap() - 3.0).abs() < 1e-5);

    let err = parse_image_expr("imag('a')", &r).unwrap_err();
    assert!(
        err.message.contains("typed API") || err.message.contains("type-changing"),
        "got: {}",
        err.message
    );
}

#[test]
fn parse_modulo_stats_value_replace_iif_indexin_rebin_and_broadcast() {
    use casa_coordinates::{
        CoordinateSystem, DirectionCoordinate, Projection, ProjectionType, SpectralCoordinate,
        StokesCoordinate, StokesType,
    };
    use casa_core::measures::direction::DirectionRef;
    use casa_core::measures::frequency::FrequencyRef;

    // 1. Modulo % and masked sample statistics variance/stddev/avdev
    let mut img = make_test_image(vec![4], 0.0);
    img.put_slice(
        &ndarray::ArrayD::from_shape_vec(ndarray::IxDyn(&[4]), vec![2.0, 4.0, 6.0, 100.0]).unwrap(),
        &[0],
    )
    .unwrap();
    img.make_mask("m", true, true).unwrap();
    img.put_mask(
        "m",
        &ndarray::ArrayD::from_shape_vec(ndarray::IxDyn(&[4]), vec![true, true, true, false])
            .unwrap(),
    )
    .unwrap();
    img.set_default_mask("m").unwrap();

    let r = make_resolver(&[("a", &img)]);
    let mod_e = parse_image_expr("value('a') % 4.0", &r).unwrap();
    assert_eq!(
        mod_e.get().unwrap().as_slice().unwrap(),
        &[2.0, 0.0, 2.0, 0.0]
    );

    let var_e = parse_image_expr("variance('a')", &r).unwrap();
    let std_e = parse_image_expr("stddev('a')", &r).unwrap();
    let avd_e = parse_image_expr("avdev('a')", &r).unwrap();
    // Valid values [2, 4, 6]: mean=4, var=((4+0+4)/2)=4, std=2, avdev=(2+0+2)/3=4/3
    assert!((var_e.get_at(&[]).unwrap() - 4.0).abs() < 1e-5);
    assert!((std_e.get_at(&[]).unwrap() - 2.0).abs() < 1e-5);
    assert!((avd_e.get_at(&[]).unwrap() - 4.0 / 3.0).abs() < 1e-5);

    // MaskExprBuilder::source_mask propagation
    let mb = ImageExprBuilder::from_image(&img).unwrap().gt_scalar(1.0);
    assert_eq!(
        mb.source_mask().unwrap().unwrap().as_slice().unwrap(),
        &[true, true, true, false]
    );

    // 2. value() strips mask, replace() preserves primary mask
    let val_c = parse_image_expr("value('a')", &r).unwrap();
    assert!(val_c.source_mask().is_none());
    let rep_c = parse_image_expr("replace('a', 99.0)", &r).unwrap();
    assert_eq!(
        rep_c.get().unwrap().as_slice().unwrap(),
        &[2.0, 4.0, 6.0, 99.0]
    );
    assert_eq!(
        rep_c.source_mask().unwrap().as_slice().unwrap(),
        &[true, true, true, false]
    );

    // 3. iif() conditional mask propagation + indexin()
    let mut b = make_test_image(vec![4], 10.0);
    b.make_mask("mb", true, true).unwrap();
    b.put_mask(
        "mb",
        &ndarray::ArrayD::from_shape_vec(ndarray::IxDyn(&[4]), vec![false, true, true, true])
            .unwrap(),
    )
    .unwrap();
    b.set_default_mask("mb").unwrap();
    let r2 = make_resolver(&[("a", &img), ("b", &b)]);
    let iif_c = parse_image_expr("iif(indexin(0, [0, 2:3]), 'a', 'b')", &r2).unwrap();
    // idx 0->a(valid), 1->b(valid), 2->a(valid), 3->a(invalid)
    assert_eq!(
        iif_c.get().unwrap().as_slice().unwrap(),
        &[2.0, 10.0, 6.0, 100.0]
    );
    assert_eq!(
        iif_c.source_mask().unwrap().as_slice().unwrap(),
        &[true, true, true, false]
    );

    // 4. 4D + 2D and 4D + 4D_deg broadcasting + rebin + spectralindex + pa
    let mut cs1 = CoordinateSystem::new();
    cs1.add_coordinate(DirectionCoordinate::new(
        DirectionRef::J2000,
        Projection::new(ProjectionType::SIN),
        [0.0, 0.0],
        [-1e-4, 1e-4],
        [0.0, 0.0],
    ));
    cs1.add_coordinate(StokesCoordinate::new(vec![StokesType::Q, StokesType::U]));
    cs1.add_coordinate(SpectralCoordinate::new(
        FrequencyRef::LSRK,
        0.0,
        1.0e9,
        1.0e6,
        1.0e9,
    ));
    let mut cs2 = cs1.clone();
    cs2.replace_coordinate(
        2,
        SpectralCoordinate::new(FrequencyRef::LSRK, 0.0, 2.0e9, 1.0e6, 2.0e9),
    );

    let mut i4d = TempImage::<f32>::new(
        vec![2, 2, 2, 2],
        cs1.clone(),
        casa_lattices::TempStoragePolicy::Memory,
    )
    .unwrap();
    i4d.set(4.0).unwrap();
    let i2d = make_test_image(vec![2, 2], 1.0);
    let r3 = make_resolver(&[("i4d", &i4d), ("i2d", &i2d)]);
    let bcast = parse_image_expr("'i4d' + 'i2d'", &r3).unwrap();
    assert_eq!(bcast.shape(), &[2, 2, 2, 2]);
    assert!((bcast.get_at(&[1, 1, 1, 1]).unwrap() - 5.0).abs() < 1e-5);

    assert!(parse_image_expr("rebin('i4d', [2, 2])", &r3).is_err());
    let rebinned = parse_image_expr("rebin('i4d', [2, 2, 1, 1])", &r3).unwrap();
    assert_eq!(rebinned.shape(), &[1, 1, 2, 2]);
    assert!((rebinned.get_at(&[0, 0, 1, 0]).unwrap() - 4.0).abs() < 1e-5);

    let mut sp_a = TempImage::<f32>::new(
        vec![2, 2, 1, 1],
        cs1,
        casa_lattices::TempStoragePolicy::Memory,
    )
    .unwrap();
    sp_a.set(1.0).unwrap();
    let mut sp_b = TempImage::<f32>::new(
        vec![2, 2, 1, 1],
        cs2,
        casa_lattices::TempStoragePolicy::Memory,
    )
    .unwrap();
    sp_b.set(2.0).unwrap();
    let r4 = make_resolver(&[("sa", &sp_a), ("sb", &sp_b)]);
    let spix = parse_image_expr("spectralindex('sa', 'sb')", &r4).unwrap();
    assert!((spix.get_at(&[0, 0, 0, 0]).unwrap() - 1.0).abs() < 1e-5);

    let pa = parse_image_expr("pa('sa', 'sb')", &r4).unwrap();
    let expected_pa = (0.5 * (1.0f64).atan2(2.0)).to_degrees() as f32;
    assert!((pa.get_at(&[0, 0, 0, 0]).unwrap() - expected_pa).abs() < 1e-4);
    assert_eq!(pa.units(), "deg");
}

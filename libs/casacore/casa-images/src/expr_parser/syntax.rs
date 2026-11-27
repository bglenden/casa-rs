// SPDX-License-Identifier: LGPL-3.0-or-later

use super::*;

#[derive(Clone, Copy)]
pub(super) struct ScalarLiteral {
    pub(super) value: f64,
    pub(super) single_precision: bool,
}

impl ScalarLiteral {
    fn double(value: f64) -> Self {
        Self {
            value,
            single_precision: false,
        }
    }

    fn single(value: f32) -> Self {
        Self {
            value: f64::from(value),
            single_precision: true,
        }
    }
}

/// Intermediate parse result: either a numeric expression or a boolean mask.
pub(super) enum ExprNode<'a, T: ImageExprValue + PartialOrd + ExprValueConvert> {
    Numeric(Box<ImageExprBuilder<'a, T>>, bool),
    Complex(Box<ImageExprBuilder<'a, Complex64>>, bool),
    Scalar(ScalarLiteral),
    Mask(MaskExprBuilder<'a, T>),
    IntArray(Vec<i64>),
}

impl<'a, T: ImageExprValue + PartialOrd + ExprValueConvert> ExprNode<'a, T> {
    pub(super) fn numeric(builder: ImageExprBuilder<'a, T>, single: bool) -> Self {
        Self::Numeric(Box::new(builder), single)
    }

    pub(super) fn single_precision(&self) -> bool {
        match self {
            Self::Scalar(value) => value.single_precision,
            Self::Numeric(_, single) | Self::Complex(_, single) => *single,
            _ => false,
        }
    }

    pub(super) fn into_numeric(
        self,
        pos: usize,
    ) -> Result<ImageExprBuilder<'a, T>, ExprParseError> {
        match self {
            Self::Numeric(e, _) => Ok(*e),
            Self::Scalar(_) => Err(ExprParseError {
                message: "bare scalar cannot be used as an image expression (no shape context)"
                    .into(),
                position: pos,
            }),
            Self::Mask(_) => Err(ExprParseError {
                message: "expected numeric expression but found boolean mask".into(),
                position: pos,
            }),
            Self::Complex(..) => Err(ExprParseError {
                message: "expected real expression but found a complex image".into(),
                position: pos,
            }),
            Self::IntArray(_) => Err(ExprParseError {
                message: "expected numeric expression but found integer array literal".into(),
                position: pos,
            }),
        }
    }

    pub(super) fn into_mask(self, pos: usize) -> Result<MaskExprBuilder<'a, T>, ExprParseError> {
        match self {
            Self::Mask(m) => Ok(m),
            _ => Err(ExprParseError {
                message: "expected boolean mask expression but found numeric".into(),
                position: pos,
            }),
        }
    }

    /// A numeric expression, with a bare scalar accepted as a constant
    /// one: the shape context comes from the other operands, as LEL
    /// allows.
    pub(super) fn into_numeric_or_constant(
        self,
        pos: usize,
    ) -> Result<ImageExprBuilder<'a, T>, ExprParseError> {
        match self {
            Self::Numeric(expression, _) => Ok(*expression),
            Self::Scalar(value) => Ok(ImageExprBuilder::scalar(T::from_f64(value.value))),
            Self::Mask(_) | Self::IntArray(_) | Self::Complex(..) => Err(ExprParseError {
                message: "expected numeric expression".into(),
                position: pos,
            }),
        }
    }
}

pub(super) fn round_real_reduction<'a, T: ImageExprValue + ExprValueConvert>(
    expression: ImageExprBuilder<'a, T>,
    single: bool,
) -> ImageExprBuilder<'a, T> {
    if single {
        expression.custom_unary("float reduction", |value: T| {
            T::from_f64(f64::from(value.to_f64() as f32))
        })
    } else {
        expression
    }
}

// ---------------------------------------------------------------------------
// Parser
// ---------------------------------------------------------------------------

pub(super) struct Parser<
    'a,
    's,
    T: ImageExprValue + PartialOrd + ExprValueConvert,
    R: ImageResolver<'a, T>,
> {
    pub(super) lexer: Lexer<'s>,
    pub(super) current: Token,
    pub(super) current_pos: usize,
    pub(super) resolver: &'s R,
    pub(super) complex_resolver: Option<&'s dyn ImageResolver<'a, Complex64>>,
    pub(super) casa_scalar_literals: bool,
    pub(super) fill_empty_reductions: bool,
    pub(super) check_lattice_geometry: bool,
    pub(super) integer_array_argument: bool,
    pub(super) single_precision_sources: Option<&'s dyn Fn(&str) -> bool>,
    pub(super) _marker: std::marker::PhantomData<&'a T>,
}

impl<'a, 's, T, R> Parser<'a, 's, T, R>
where
    T: ImageExprValue + PartialOrd + ExprValueConvert,
    R: ImageResolver<'a, T>,
{
    pub(super) fn new(src: &'s str, resolver: &'s R) -> Result<Self, ExprParseError> {
        let mut lexer = Lexer::new(src);
        let (tok, pos) = lexer.next_token()?;
        Ok(Self {
            lexer,
            current: tok,
            current_pos: pos,
            resolver,
            complex_resolver: None,
            casa_scalar_literals: false,
            fill_empty_reductions: false,
            check_lattice_geometry: false,
            integer_array_argument: false,
            single_precision_sources: None,
            _marker: std::marker::PhantomData,
        })
    }

    fn advance(&mut self) -> Result<(), ExprParseError> {
        let (tok, pos) = self.lexer.next_token()?;
        self.current = tok;
        self.current_pos = pos;
        Ok(())
    }

    pub(super) fn expect(&mut self, expected: &Token) -> Result<(), ExprParseError> {
        if &self.current == expected {
            self.advance()
        } else {
            Err(ExprParseError {
                message: format!("expected {expected} but found {}", self.current),
                position: self.current_pos,
            })
        }
    }

    fn convert_f64_to_t(value: f64) -> T {
        T::from_f64(value)
    }

    // Combine two nodes with a binary arithmetic op, handling scalar promotion.
    fn combine_binary(
        &self,
        lhs: ExprNode<'a, T>,
        rhs: ExprNode<'a, T>,
        op: ImageExprBinaryOp,
        pos: usize,
    ) -> Result<ExprNode<'a, T>, ExprParseError> {
        self.validate_lattice_operands(&lhs, &rhs, pos, Operator::Infix)?;
        if matches!(&lhs, ExprNode::Complex(..)) || matches!(&rhs, ExprNode::Complex(..)) {
            return arithmetic_complex(lhs, rhs, op, pos);
        }
        if lhs.single_precision()
            && rhs.single_precision()
            && (!matches!(&lhs, ExprNode::Scalar(_)) || !matches!(&rhs, ExprNode::Scalar(_)))
        {
            return lhs
                .into_numeric_or_constant(pos)?
                .binary_expr_with_name(
                    rhs.into_numeric_or_constant(pos)?,
                    "float arithmetic",
                    std::sync::Arc::new(move |a: T, b: T| {
                        let output = apply_binary(op, a.to_f64() as f32, b.to_f64() as f32);
                        T::from_f64(f64::from(output))
                    }),
                )
                .map(|expression| ExprNode::numeric(expression, true))
                .map_err(|error| ExprParseError {
                    message: error.to_string(),
                    position: pos,
                });
        }
        match (&lhs, &rhs) {
            (ExprNode::Scalar(a), ExprNode::Scalar(b)) => {
                // Both scalars: compute at parse time
                let result = if a.single_precision && b.single_precision {
                    ScalarLiteral::single(apply_binary(op, a.value as f32, b.value as f32))
                } else {
                    let a = Self::convert_f64_to_t(a.value);
                    let b = Self::convert_f64_to_t(b.value);
                    ScalarLiteral::double(apply_binary(op, a, b).to_f64())
                };
                Ok(ExprNode::Scalar(result))
            }
            (ExprNode::Scalar(s), ExprNode::Numeric(img, _)) => {
                // scalar op image
                match op {
                    // Commutative ops: rewrite as image op scalar
                    ImageExprBinaryOp::Add
                    | ImageExprBinaryOp::Multiply
                    | ImageExprBinaryOp::Min
                    | ImageExprBinaryOp::Max => Ok(ExprNode::numeric(
                        (**img)
                            .clone()
                            .binary_scalar(Self::convert_f64_to_t(s.value), op),
                        false,
                    )),
                    // Non-commutative: use scalar_left_binary for correct Inf/NaN handling
                    _ => Ok(ExprNode::numeric(
                        ImageExprBuilder::scalar_left_binary(
                            Self::convert_f64_to_t(s.value),
                            (**img).clone(),
                            op,
                        ),
                        false,
                    )),
                }
            }
            (ExprNode::Numeric(img, _), ExprNode::Scalar(s)) => Ok(ExprNode::numeric(
                (**img)
                    .clone()
                    .binary_scalar(Self::convert_f64_to_t(s.value), op),
                false,
            )),
            (ExprNode::Numeric(_, _), ExprNode::Numeric(_, _)) => {
                let lhs_expr = lhs.into_numeric(pos)?;
                let rhs_expr = rhs.into_numeric(pos)?;
                Ok(ExprNode::numeric(
                    lhs_expr
                        .binary_expr(rhs_expr, op)
                        .map_err(|e| ExprParseError {
                            message: e.to_string(),
                            position: pos,
                        })?,
                    false,
                ))
            }
            _ => Err(ExprParseError {
                message: "cannot apply arithmetic operator to boolean mask expression".into(),
                position: pos,
            }),
        }
    }

    // -----------------------------------------------------------------------
    // Recursive descent
    // -----------------------------------------------------------------------

    pub(super) fn parse_expression(&mut self) -> Result<ExprNode<'a, T>, ExprParseError> {
        self.parse_or_expr()
    }

    fn parse_or_expr(&mut self) -> Result<ExprNode<'a, T>, ExprParseError> {
        let mut left = self.parse_and_expr()?;
        while self.current == Token::Or {
            let pos = self.current_pos;
            self.advance()?;
            let right = self.parse_and_expr()?;
            self.validate_lattice_operands(&left, &right, pos, Operator::Logical)?;
            let lhs_mask = left.into_mask(pos)?;
            let rhs_mask = right.into_mask(pos)?;
            left = ExprNode::Mask(lhs_mask.or(rhs_mask).map_err(|e| ExprParseError {
                message: e.to_string(),
                position: pos,
            })?);
        }
        Ok(left)
    }

    fn parse_and_expr(&mut self) -> Result<ExprNode<'a, T>, ExprParseError> {
        let mut left = self.parse_not_expr()?;
        while self.current == Token::And {
            let pos = self.current_pos;
            self.advance()?;
            let right = self.parse_not_expr()?;
            self.validate_lattice_operands(&left, &right, pos, Operator::Logical)?;
            let lhs_mask = left.into_mask(pos)?;
            let rhs_mask = right.into_mask(pos)?;
            left = ExprNode::Mask(lhs_mask.and(rhs_mask).map_err(|e| ExprParseError {
                message: e.to_string(),
                position: pos,
            })?);
        }
        Ok(left)
    }

    fn parse_not_expr(&mut self) -> Result<ExprNode<'a, T>, ExprParseError> {
        if self.current == Token::Bang {
            let pos = self.current_pos;
            self.advance()?;
            let inner = self.parse_not_expr()?;
            let mask = inner.into_mask(pos)?;
            return Ok(ExprNode::Mask(mask.logical_not()));
        }
        self.parse_rel_expr()
    }

    fn parse_rel_expr(&mut self) -> Result<ExprNode<'a, T>, ExprParseError> {
        let left = self.parse_add_expr()?;
        let cmp_op = match &self.current {
            Token::Eq => Some(ImageExprCompareOp::Equal),
            Token::Ne => Some(ImageExprCompareOp::NotEqual),
            Token::Gt => Some(ImageExprCompareOp::GreaterThan),
            Token::Ge => Some(ImageExprCompareOp::GreaterEqual),
            Token::Lt => Some(ImageExprCompareOp::LessThan),
            Token::Le => Some(ImageExprCompareOp::LessEqual),
            _ => None,
        };
        if let Some(op) = cmp_op {
            let pos = self.current_pos;
            self.advance()?;
            let right = self.parse_add_expr()?;
            self.validate_lattice_operands(&left, &right, pos, Operator::Infix)?;

            if matches!(&left, ExprNode::Complex(..)) || matches!(&right, ExprNode::Complex(..)) {
                return compare_complex(left, right, op, pos);
            }
            match (&left, &right) {
                (ExprNode::Scalar(lhs), ExprNode::Scalar(rhs)) => {
                    let (lhs, rhs) = (lhs.value, rhs.value);
                    let value = match op {
                        ImageExprCompareOp::GreaterThan => lhs > rhs,
                        ImageExprCompareOp::LessThan => lhs < rhs,
                        ImageExprCompareOp::GreaterEqual => lhs >= rhs,
                        ImageExprCompareOp::LessEqual => lhs <= rhs,
                        ImageExprCompareOp::Equal => lhs == rhs,
                        ImageExprCompareOp::NotEqual => lhs != rhs,
                    };
                    return Ok(ExprNode::Mask(MaskExprBuilder::from_constant(
                        ndarray::arr0(value).into_dyn(),
                    )));
                }
                (ExprNode::Numeric(img, _), ExprNode::Scalar(s)) => {
                    let mask = (**img)
                        .clone()
                        .compare_scalar(Self::convert_f64_to_t(s.value), op);
                    return Ok(ExprNode::Mask(mask));
                }
                (ExprNode::Scalar(s), ExprNode::Numeric(img, _)) => {
                    // Flip the comparison: scalar op image => image flipped_op scalar
                    let flipped = flip_compare_op(op);
                    let mask = (**img)
                        .clone()
                        .compare_scalar(Self::convert_f64_to_t(s.value), flipped);
                    return Ok(ExprNode::Mask(mask));
                }
                (ExprNode::Numeric(lhs, _), ExprNode::Numeric(rhs, _)) => {
                    let mask = (**lhs)
                        .clone()
                        .compare_expr((**rhs).clone(), op)
                        .map_err(|e| ExprParseError {
                            message: e.to_string(),
                            position: pos,
                        })?;
                    return Ok(ExprNode::Mask(mask));
                }
                _ => {
                    return Err(ExprParseError {
                        message: "cannot compare boolean mask expressions".into(),
                        position: pos,
                    });
                }
            }
        }
        Ok(left)
    }

    fn parse_add_expr(&mut self) -> Result<ExprNode<'a, T>, ExprParseError> {
        let mut left = self.parse_mul_expr()?;
        loop {
            let op = match &self.current {
                Token::Plus => ImageExprBinaryOp::Add,
                Token::Minus => ImageExprBinaryOp::Subtract,
                _ => break,
            };
            let pos = self.current_pos;
            self.advance()?;
            let right = self.parse_mul_expr()?;
            left = self.combine_binary(left, right, op, pos)?;
        }
        Ok(left)
    }

    fn parse_mul_expr(&mut self) -> Result<ExprNode<'a, T>, ExprParseError> {
        let mut left = self.parse_power_expr()?;
        loop {
            let op = match &self.current {
                Token::Star => ImageExprBinaryOp::Multiply,
                Token::Slash => ImageExprBinaryOp::Divide,
                Token::Percent => ImageExprBinaryOp::Fmod,
                _ => break,
            };
            let pos = self.current_pos;
            self.advance()?;
            let right = self.parse_power_expr()?;
            left = self.combine_binary(left, right, op, pos)?;
        }
        Ok(left)
    }

    fn parse_power_expr(&mut self) -> Result<ExprNode<'a, T>, ExprParseError> {
        let base = self.parse_unary_expr()?;
        if self.current == Token::Caret {
            let pos = self.current_pos;
            self.advance()?;
            // Right-associative: recurse into power_expr
            let exponent = self.parse_power_expr()?;
            return self.combine_binary(base, exponent, ImageExprBinaryOp::Pow, pos);
        }
        Ok(base)
    }

    fn parse_unary_expr(&mut self) -> Result<ExprNode<'a, T>, ExprParseError> {
        match &self.current {
            Token::Minus => {
                let pos = self.current_pos;
                self.advance()?;
                let inner = self.parse_unary_expr()?;
                match inner {
                    ExprNode::Scalar(mut literal) => {
                        literal.value = -literal.value;
                        Ok(ExprNode::Scalar(literal))
                    }
                    ExprNode::Numeric(expr, single) => {
                        Ok(ExprNode::numeric((*expr).negate(), single))
                    }
                    ExprNode::Complex(expr, single) => {
                        Ok(ExprNode::Complex(Box::new((*expr).negate()), single))
                    }
                    ExprNode::Mask(_) | ExprNode::IntArray(_) => Err(ExprParseError {
                        message: "cannot negate a non-numeric expression".into(),
                        position: pos,
                    }),
                }
            }
            Token::Plus => {
                self.advance()?;
                self.parse_unary_expr()
            }
            _ => self.parse_call_expr(),
        }
    }

    fn parse_call_expr(&mut self) -> Result<ExprNode<'a, T>, ExprParseError> {
        if let Token::Ident(name) = self.current.clone() {
            let pos = self.current_pos;
            // Check if this is a function call (ident followed by '(')
            self.advance()?;
            if self.current == Token::LParen {
                return self.parse_function_call(&name, pos);
            }
            if matches!(name.as_str(), "T" | "F") {
                let value = name == "T";
                return Ok(ExprNode::Mask(MaskExprBuilder::from_constant(
                    ndarray::arr0(value).into_dyn(),
                )));
            }
            return self.resolve_image(&name, pos);
        }
        self.parse_primary()
    }

    fn parse_function_call(
        &mut self,
        name: &str,
        pos: usize,
    ) -> Result<ExprNode<'a, T>, ExprParseError> {
        // We are positioned at '('
        self.expect(&Token::LParen)?;

        // Check for 0-arg functions
        if self.current == Token::RParen {
            self.advance()?;
            return self.eval_function_0(name, pos);
        }

        // Parse first argument
        let arg1 = self.parse_expression()?;

        // Check for 1-arg
        if self.current == Token::RParen {
            self.advance()?;
            return self.eval_function_1(name, arg1, pos);
        }

        // Expect comma and second argument
        self.expect(&Token::Comma)?;
        let previous_indices = self.integer_array_argument;
        self.integer_array_argument =
            name.eq_ignore_ascii_case("indexin") || name.eq_ignore_ascii_case("rebin");
        let arg2 = self.parse_expression();
        self.integer_array_argument = previous_indices;
        let arg2 = arg2?;

        if self.current == Token::RParen {
            self.advance()?;
            return self.eval_function_2(name, arg1, arg2, pos);
        }

        // Check for 3-arg function
        self.expect(&Token::Comma)?;
        let arg3 = self.parse_expression()?;

        if self.current == Token::RParen {
            self.advance()?;
            return self.eval_function_3(name, arg1, arg2, arg3, pos);
        }

        Err(ExprParseError {
            message: format!("function '{name}' takes at most 3 arguments"),
            position: pos,
        })
    }

    fn eval_function_0(&self, name: &str, pos: usize) -> Result<ExprNode<'a, T>, ExprParseError> {
        let lower = name.to_ascii_lowercase();
        match lower.as_str() {
            "pi" => Ok(ExprNode::Scalar(ScalarLiteral::double(
                std::f64::consts::PI,
            ))),
            "e" => Ok(ExprNode::Scalar(ScalarLiteral::double(std::f64::consts::E))),
            _ => Err(ExprParseError {
                message: format!("unknown 0-argument function: {name}"),
                position: pos,
            }),
        }
    }

    fn eval_function_1(
        &self,
        name: &str,
        arg: ExprNode<'a, T>,
        pos: usize,
    ) -> Result<ExprNode<'a, T>, ExprParseError> {
        let lower = name.to_ascii_lowercase();
        let unary_op = match lower.as_str() {
            "sin" => Some(ImageExprUnaryOp::Sin),
            "cos" => Some(ImageExprUnaryOp::Cos),
            "tan" => Some(ImageExprUnaryOp::Tan),
            "asin" => Some(ImageExprUnaryOp::Asin),
            "acos" => Some(ImageExprUnaryOp::Acos),
            "atan" => Some(ImageExprUnaryOp::Atan),
            "sinh" => Some(ImageExprUnaryOp::Sinh),
            "cosh" => Some(ImageExprUnaryOp::Cosh),
            "tanh" => Some(ImageExprUnaryOp::Tanh),
            "exp" => Some(ImageExprUnaryOp::Exp),
            "log" => Some(ImageExprUnaryOp::Log),
            "log10" => Some(ImageExprUnaryOp::Log10),
            "sqrt" => Some(ImageExprUnaryOp::Sqrt),
            "abs" => Some(ImageExprUnaryOp::Abs),
            "amplitude" if !T::is_complex() => Some(ImageExprUnaryOp::Abs),
            "ceil" => Some(ImageExprUnaryOp::Ceil),
            "floor" => Some(ImageExprUnaryOp::Floor),
            "round" => Some(ImageExprUnaryOp::Round),
            "sign" => Some(ImageExprUnaryOp::Sign),
            "conj" => Some(ImageExprUnaryOp::Conj),
            _ => None,
        };

        if let ExprNode::Complex(expr, single) = arg {
            return complex_function(&lower, *expr, single, unary_op, pos);
        }
        let reduction = match lower.as_str() {
            "sum" => Some(ReductionOp::Sum),
            "min1d" | "min" => Some(ReductionOp::Min),
            "max1d" | "max" => Some(ReductionOp::Max),
            "mean1d" | "mean" => Some(ReductionOp::Mean),
            "median1d" | "median" => Some(ReductionOp::Median),
            "variance" | "variance1d" => Some(ReductionOp::Variance),
            "stddev" | "stddev1d" => Some(ReductionOp::Stddev),
            "avdev" | "avdev1d" => Some(ReductionOp::Avdev),
            _ => None,
        };
        if let Some(op) = reduction {
            return self.eval_reduction(arg, op, pos);
        }
        if let Some(op) = unary_op {
            match arg {
                ExprNode::Scalar(literal) => {
                    let result = if literal.single_precision {
                        ScalarLiteral::single(apply_unary(op, literal.value as f32))
                    } else {
                        let value = Self::convert_f64_to_t(literal.value);
                        ScalarLiteral::double(apply_unary(op, value).to_f64())
                    };
                    Ok(ExprNode::Scalar(result))
                }
                ExprNode::Numeric(expr, true) => Ok(ExprNode::numeric(
                    expr.custom_unary("float arithmetic", move |value: T| {
                        T::from_f64(f64::from(apply_unary(op, value.to_f64() as f32)))
                    }),
                    true,
                )),
                ExprNode::Numeric(expr, false) => Ok(ExprNode::numeric((*expr).unary(op), false)),
                ExprNode::Mask(_) | ExprNode::IntArray(_) | ExprNode::Complex(..) => {
                    Err(ExprParseError {
                        message: format!("function '{name}' requires a numeric argument"),
                        position: pos,
                    })
                }
            }
        } else {
            // Wave 14 functions
            match lower.as_str() {
                "isnan" => match arg {
                    ExprNode::Scalar(v) => {
                        // Constant-fold: isnan of a literal
                        let is_nan = v.value.is_nan();
                        // We need a shape context to produce a MaskExprBuilder; without an image
                        // we cannot create one. Return error like other scalar-only cases.
                        Err(ExprParseError {
                            message: if is_nan {
                                "isnan() of a NaN scalar has no image shape context".into()
                            } else {
                                "isnan() of a scalar has no image shape context".into()
                            },
                            position: pos,
                        })
                    }
                    ExprNode::Numeric(expr, _) => Ok(ExprNode::Mask((*expr).isnan())),
                    ExprNode::Mask(_) | ExprNode::IntArray(_) | ExprNode::Complex(..) => {
                        Err(ExprParseError {
                            message: "isnan() requires a numeric argument".into(),
                            position: pos,
                        })
                    }
                },
                "ndim" => {
                    let expr = arg.into_numeric(pos)?;
                    Ok(ExprNode::Scalar(ScalarLiteral::double(
                        expr.ndim_value() as f64
                    )))
                }
                "nelem" | "nelements" => {
                    let expr = arg.into_numeric(pos)?;
                    Ok(ExprNode::Scalar(ScalarLiteral::double(
                        expr.nelem_value() as f64
                    )))
                }
                // Wave 14c: mask-aware 1-arg functions
                "all" => {
                    let mask = arg.into_mask(pos)?;
                    Ok(ExprNode::Mask(mask.all_reduce()))
                }
                "any" => {
                    let mask = arg.into_mask(pos)?;
                    Ok(ExprNode::Mask(mask.any_reduce()))
                }
                "ntrue" => {
                    let mask = arg.into_mask(pos)?;
                    Ok(ExprNode::numeric(ImageExprBuilder::ntrue(mask), false))
                }
                "nfalse" => {
                    let mask = arg.into_mask(pos)?;
                    Ok(ExprNode::numeric(ImageExprBuilder::nfalse(mask), false))
                }
                "mask" => match arg {
                    ExprNode::Mask(mask) => Ok(ExprNode::Mask(mask.validity())),
                    other => Ok(ExprNode::Mask(
                        other.into_numeric_or_constant(pos)?.validity(),
                    )),
                },
                "value" => match arg {
                    ExprNode::Numeric(expr, single) => {
                        Ok(ExprNode::numeric((*expr).strip_mask(), single))
                    }
                    ExprNode::Scalar(_) => Ok(arg),
                    ExprNode::Mask(_) | ExprNode::IntArray(_) | ExprNode::Complex(..) => {
                        Err(ExprParseError {
                            message: "value() requires a numeric argument".into(),
                            position: pos,
                        })
                    }
                },
                "float" if !T::is_complex() => match arg {
                    ExprNode::Scalar(value) => {
                        Ok(ExprNode::Scalar(ScalarLiteral::single(value.value as f32)))
                    }
                    ExprNode::Numeric(expr, _) => Ok(ExprNode::numeric(
                        expr.custom_unary("float", |value| {
                            T::from_f64(f64::from(value.to_f64() as f32))
                        }),
                        true,
                    )),
                    _ => Err(ExprParseError {
                        message: "'float()' requires a numeric argument".into(),
                        position: pos,
                    }),
                },
                "real" | "double" if !T::is_complex() => match arg {
                    ExprNode::Scalar(literal) if lower == "double" => {
                        Ok(ExprNode::Scalar(ScalarLiteral::double(literal.value)))
                    }
                    ExprNode::Numeric(expr, _) if lower == "double" => {
                        Ok(ExprNode::numeric(*expr, false))
                    }
                    ExprNode::Numeric(_, _) | ExprNode::Scalar(_) => Ok(arg),
                    ExprNode::Mask(_) | ExprNode::IntArray(_) | ExprNode::Complex(..) => {
                        Err(ExprParseError {
                            message: format!("'{lower}()' requires a numeric argument"),
                            position: pos,
                        })
                    }
                },
                // Wave 14d: type-changing functions (typed API only for complex types)
                "amplitude" | "real" | "imag" | "arg" | "complex" => Err(ExprParseError {
                    message: format!(
                        "'{lower}()' changes pixel type and is available as a typed API method only \
                         (e.g. expr.real_part()), not through the parser"
                    ),
                    position: pos,
                }),
                _ => Err(ExprParseError {
                    message: format!("unknown 1-argument function: {name}"),
                    position: pos,
                }),
            }
        }
    }

    fn eval_reduction(
        &self,
        arg: ExprNode<'a, T>,
        op: ReductionOp,
        pos: usize,
    ) -> Result<ExprNode<'a, T>, ExprParseError> {
        let single = arg.single_precision();
        let empty_value = self.fill_empty_reductions.then(|| {
            let value = if matches!(
                op,
                ReductionOp::Min | ReductionOp::Max | ReductionOp::Mean | ReductionOp::Median
            ) {
                if single {
                    -f64::from(f32::MIN_POSITIVE)
                } else {
                    -f64::MIN_POSITIVE
                }
            } else {
                0.0
            };
            T::from_f64(value)
        });
        let input = if self.casa_scalar_literals {
            arg.into_numeric_or_constant(pos)?
        } else {
            arg.into_numeric(pos)?
        };
        let expression = input.make_reduction_with_empty_value(op, empty_value);
        Ok(ExprNode::numeric(
            round_real_reduction(expression, single),
            single,
        ))
    }

    fn eval_function_2(
        &self,
        name: &str,
        arg1: ExprNode<'a, T>,
        arg2: ExprNode<'a, T>,
        pos: usize,
    ) -> Result<ExprNode<'a, T>, ExprParseError> {
        let lower = name.to_ascii_lowercase();
        let binary_op = match lower.as_str() {
            "pow" => Some(ImageExprBinaryOp::Pow),
            "fmod" => Some(ImageExprBinaryOp::Fmod),
            "atan2" => Some(ImageExprBinaryOp::Atan2),
            "min" => Some(ImageExprBinaryOp::Min),
            "max" => Some(ImageExprBinaryOp::Max),
            _ => None,
        };

        if let Some(op) = binary_op {
            self.validate_lattice_operands(&arg1, &arg2, pos, Operator::Function)?;
            return self.combine_binary(arg1, arg2, op, pos);
        }

        // Wave 14 2-arg functions
        match lower.as_str() {
            "complex" => complex_constructor(arg1, arg2, pos),
            "length" => {
                let expr = arg1.into_numeric(pos)?;
                let axis = match arg2 {
                    ExprNode::Scalar(v) => v.value as usize,
                    _ => {
                        return Err(ExprParseError {
                            message: "length() second argument must be a scalar axis index".into(),
                            position: pos,
                        });
                    }
                };
                match expr.length_value(axis) {
                    Some(len) => Ok(ExprNode::Scalar(ScalarLiteral::double(len as f64))),
                    None => Err(ExprParseError {
                        message: format!(
                            "axis {axis} out of range for {}-d image",
                            expr.ndim_value()
                        ),
                        position: pos,
                    }),
                }
            }
            "fractile1d" | "fractile" => {
                let expr = arg1.into_numeric(pos)?;
                let frac = match arg2 {
                    ExprNode::Scalar(v) => v.value,
                    _ => {
                        return Err(ExprParseError {
                            message: "fractile1d() second argument must be a scalar fraction"
                                .into(),
                            position: pos,
                        });
                    }
                };
                Ok(ExprNode::numeric(expr.fractile(frac), false))
            }
            "fractilerange1d" | "fractilerange" => {
                // 2-arg variant: fractilerange1d(expr, frac) = range from frac to 1-frac
                let expr = arg1.into_numeric(pos)?;
                let frac = match arg2 {
                    ExprNode::Scalar(v) => v.value,
                    _ => {
                        return Err(ExprParseError {
                            message: "fractilerange1d() second argument must be a scalar fraction"
                                .into(),
                            position: pos,
                        });
                    }
                };
                Ok(ExprNode::numeric(
                    expr.fractile_range(frac, 1.0 - frac),
                    false,
                ))
            }
            "replace" => {
                // replace(image, replacement) — replaces masked-out pixels while keeping mask.
                let single = arg1.single_precision() && arg2.single_precision();
                let primary = arg1.into_numeric(pos)?;
                let replacement = arg2.into_numeric_or_constant(pos)?;
                primary
                    .replace_invalid(replacement)
                    .map_err(|e| ExprParseError {
                        message: e.to_string(),
                        position: pos,
                    })
                    .map(|r| ExprNode::numeric(r, single))
            }
            "spectralindex" => {
                if T::is_complex() {
                    return Err(ExprParseError {
                        message: "spectralindex() requires real pixel types".into(),
                        position: pos,
                    });
                }
                let single = arg1.single_precision() && arg2.single_precision();
                let lhs = arg1.into_numeric(pos)?;
                let rhs = arg2.into_numeric(pos)?;
                lhs.spectral_index_with_metadata(rhs, !self.casa_scalar_literals)
                    .map_err(|e| ExprParseError {
                        message: e.to_string(),
                        position: pos,
                    })
                    .map(|r| ExprNode::numeric(r, single))
            }
            "pa" => {
                if T::is_complex() {
                    return Err(ExprParseError {
                        message: "pa() requires real pixel types".into(),
                        position: pos,
                    });
                }
                let u_is_num = matches!(arg1, ExprNode::Numeric(_, _));
                let q_is_num = matches!(arg2, ExprNode::Numeric(_, _));
                if !u_is_num && !q_is_num {
                    return Err(ExprParseError {
                        message: "pa() requires at least one image expression".into(),
                        position: pos,
                    });
                }
                let u = arg1.into_numeric_or_constant(pos)?;
                let q = arg2.into_numeric_or_constant(pos)?;
                ImageExprBuilder::pa_with_metadata(u, q, !self.casa_scalar_literals)
                    .map_err(|e| ExprParseError {
                        message: e.to_string(),
                        position: pos,
                    })
                    .map(|r| ExprNode::numeric(r, false))
            }
            "indexin" => {
                let axis = match arg1 {
                    ExprNode::Scalar(v) if (v.value - v.value.round()).abs() < 1e-9 => {
                        v.value.round() as i64
                    }
                    _ => {
                        return Err(ExprParseError {
                            message: "indexin() first argument must be an integer axis".into(),
                            position: pos,
                        });
                    }
                };
                let indices = match arg2 {
                    ExprNode::IntArray(vals) => vals,
                    ExprNode::Scalar(v) if (v.value - v.value.round()).abs() < 1e-9 => {
                        vec![v.value.round() as i64]
                    }
                    _ => {
                        return Err(ExprParseError {
                            message:
                                "indexin() second argument must be an integer or integer array"
                                    .into(),
                            position: pos,
                        });
                    }
                };
                Ok(ExprNode::Mask(MaskExprBuilder::index_in(axis, indices)))
            }
            "rebin" => {
                let single = arg1.single_precision();
                let expr = arg1.into_numeric(pos)?;
                let raw_factors = match arg2 {
                    ExprNode::IntArray(vals) => vals,
                    ExprNode::Scalar(v) if (v.value - v.value.round()).abs() < 1e-9 => {
                        vec![v.value.round() as i64]
                    }
                    _ => {
                        return Err(ExprParseError {
                            message: "rebin() second argument must be an integer factor vector"
                                .into(),
                            position: pos,
                        });
                    }
                };
                let factors: Vec<usize> = raw_factors
                    .into_iter()
                    .map(|f| {
                        if f >= 1 {
                            Ok(f as usize)
                        } else {
                            Err(ExprParseError {
                                message: "rebin() factors must be positive integers >= 1".into(),
                                position: pos,
                            })
                        }
                    })
                    .collect::<Result<Vec<_>, _>>()?;
                expr.rebin(factors)
                    .map_err(|e| ExprParseError {
                        message: e.to_string(),
                        position: pos,
                    })
                    .map(|r| ExprNode::numeric(r, single))
            }
            _ => Err(ExprParseError {
                message: format!("unknown 2-argument function: {name}"),
                position: pos,
            }),
        }
    }

    fn eval_function_3(
        &self,
        name: &str,
        arg1: ExprNode<'a, T>,
        arg2: ExprNode<'a, T>,
        arg3: ExprNode<'a, T>,
        pos: usize,
    ) -> Result<ExprNode<'a, T>, ExprParseError> {
        let lower = name.to_ascii_lowercase();
        match lower.as_str() {
            "iif" => {
                // iif(condition, true_val, false_val): a scalar branch is
                // a constant expression beside an image one, as LEL allows.
                let condition = arg1.into_mask(pos)?;
                let single = arg2.single_precision() && arg3.single_precision();
                let if_true = arg2.into_numeric_or_constant(pos)?;
                let if_false = arg3.into_numeric_or_constant(pos)?;
                ImageExprBuilder::iif(condition, if_true, if_false)
                    .map_err(|e| ExprParseError {
                        message: e.to_string(),
                        position: pos,
                    })
                    .map(|r| ExprNode::numeric(r, single))
            }
            "fractilerange1d" | "fractilerange" => {
                // fractilerange1d(expr, frac1, frac2)
                let expr = arg1.into_numeric(pos)?;
                let frac1 = match arg2 {
                    ExprNode::Scalar(v) => v.value,
                    _ => {
                        return Err(ExprParseError {
                            message: "fractilerange1d() second argument must be a scalar".into(),
                            position: pos,
                        });
                    }
                };
                let frac2 = match arg3 {
                    ExprNode::Scalar(v) => v.value,
                    _ => {
                        return Err(ExprParseError {
                            message: "fractilerange1d() third argument must be a scalar".into(),
                            position: pos,
                        });
                    }
                };
                Ok(ExprNode::numeric(expr.fractile_range(frac1, frac2), false))
            }
            _ => Err(ExprParseError {
                message: format!("unknown 3-argument function: {name}"),
                position: pos,
            }),
        }
    }

    fn resolve_image(&self, name: &str, pos: usize) -> Result<ExprNode<'a, T>, ExprParseError> {
        let single = self
            .single_precision_sources
            .is_some_and(|source| source(name));
        if let Some(source) = self
            .complex_resolver
            .and_then(|resolver| resolver.resolve(name).ok())
        {
            return ImageExprBuilder::from_dyn(source)
                .map(|expr| ExprNode::Complex(Box::new(expr), single))
                .map_err(|error| ExprParseError {
                    message: error.to_string(),
                    position: pos,
                });
        }
        let image = self.resolver.resolve(name).map_err(|e| ExprParseError {
            message: e.to_string(),
            position: pos,
        })?;
        let expr = ImageExprBuilder::from_dyn(image).map_err(|e| ExprParseError {
            message: e.to_string(),
            position: pos,
        })?;
        Ok(ExprNode::numeric(expr, single))
    }

    fn parse_array_element(&mut self) -> Result<i64, ExprParseError> {
        let neg = if self.current == Token::Minus {
            self.advance()?;
            true
        } else {
            if self.current == Token::Plus {
                self.advance()?;
            }
            false
        };
        match self.current {
            Token::Number(value) if (value - value.round()).abs() < 1e-9 => {
                self.advance()?;
                let n = value.round() as i64;
                Ok(if neg { -n } else { n })
            }
            _ => Err(ExprParseError {
                message: format!(
                    "expected integer inside array literal, found {}",
                    self.current
                ),
                position: self.current_pos,
            }),
        }
    }

    fn parse_array_literal(&mut self) -> Result<ExprNode<'a, T>, ExprParseError> {
        self.expect(&Token::LBracket)?;
        let mut values = Vec::new();
        if self.current == Token::RBracket {
            self.advance()?;
            return Ok(ExprNode::IntArray(values));
        }
        loop {
            let start = self.parse_array_element()?;
            if self.current == Token::Colon {
                self.advance()?;
                let end = self.parse_array_element()?;
                if end < start {
                    return Err(ExprParseError {
                        message: format!("descending integer range {start}:{end} in array literal"),
                        position: self.current_pos,
                    });
                }
                values.extend(start..=end);
            } else {
                values.push(start);
            }
            if self.current == Token::Comma {
                self.advance()?;
                continue;
            }
            break;
        }
        self.expect(&Token::RBracket)?;
        Ok(ExprNode::IntArray(values))
    }

    fn parse_typed_array_literal(&mut self) -> Result<ExprNode<'a, T>, ExprParseError> {
        let pos = self.current_pos;
        self.expect(&Token::LBracket)?;
        let mut values = vec![self.parse_expression()?];
        while self.current == Token::Comma {
            self.advance()?;
            values.push(self.parse_expression()?);
        }
        self.expect(&Token::RBracket)?;
        let boolean = values
            .iter()
            .any(|value| matches!(value, ExprNode::Mask(_)));
        if boolean {
            if !values
                .iter()
                .all(|value| matches!(value, ExprNode::Mask(_)))
            {
                return Err(ExprParseError {
                    message:
                        "LatticeExprNode::resultDataType - Bool and numeric operands cannot mixed"
                            .into(),
                    position: pos,
                });
            }
            let flags = values
                .into_iter()
                .map(|value| {
                    let mask = value.into_mask(pos)?;
                    scalar_array_element(mask.shape(), pos)?;
                    mask.compile()
                        .and_then(|mask| mask.get_at(&[]))
                        .map_err(array_error(pos))
                })
                .collect::<Result<Vec<_>, _>>()?;
            return Ok(ExprNode::Mask(MaskExprBuilder::from_constant(
                ndarray::Array1::from_vec(flags).into_dyn(),
            )));
        }
        let single = values.iter().all(ExprNode::single_precision);
        if values
            .iter()
            .any(|value| matches!(value, ExprNode::Complex(..)))
        {
            let pixels = values
                .into_iter()
                .map(|value| {
                    let expression = complex::promote(value, pos)?;
                    scalar_array_element(expression.shape(), pos)?;
                    expression
                        .compile()
                        .and_then(|expression| expression.get_at(&[]))
                        .map_err(array_error(pos))
                })
                .collect::<Result<Vec<_>, _>>()?;
            return Ok(ExprNode::Complex(
                Box::new(ImageExprBuilder::from_vector(pixels)),
                single,
            ));
        }
        let pixels = values
            .into_iter()
            .map(|value| {
                let expression = value.into_numeric_or_constant(pos)?;
                scalar_array_element(expression.shape(), pos)?;
                expression
                    .compile()
                    .and_then(|expression| expression.get_at(&[]))
                    .map_err(array_error(pos))
            })
            .collect::<Result<Vec<_>, _>>()?;
        Ok(ExprNode::numeric(
            ImageExprBuilder::from_vector(pixels),
            single,
        ))
    }

    fn parse_primary(&mut self) -> Result<ExprNode<'a, T>, ExprParseError> {
        match self.current.clone() {
            Token::LParen => {
                self.advance()?;
                let inner = self.parse_expression()?;
                self.expect(&Token::RParen)?;
                Ok(inner)
            }
            Token::LBracket if self.casa_scalar_literals && !self.integer_array_argument => {
                self.parse_typed_array_literal()
            }
            Token::LBracket => self.parse_array_literal(),
            Token::Number(value) => {
                let text = &self.lexer.src[self.current_pos..self.lexer.pos];
                let literal = if self.casa_scalar_literals {
                    if let Some((mantissa, _)) = text.split_once(['d', 'D']) {
                        // CASA recognizes a D exponent as a Double literal but its
                        // numeric conversion consumes only the leading mantissa.
                        ScalarLiteral::double(mantissa.parse().expect("valid numeric mantissa"))
                    } else if text.contains(['e', 'E']) || !text.contains('.') {
                        ScalarLiteral::single(value as f32)
                    } else {
                        ScalarLiteral::double(value)
                    }
                } else {
                    ScalarLiteral::double(value)
                };
                self.advance()?;
                Ok(ExprNode::Scalar(literal))
            }
            Token::QuotedPath(path) => {
                let pos = self.current_pos;
                self.advance()?;
                self.resolve_image(&path, pos)
            }
            _ => Err(ExprParseError {
                message: format!("unexpected token: {}", self.current),
                position: self.current_pos,
            }),
        }
    }
}

pub(super) fn scalar_array_element(shape: &[usize], position: usize) -> Result<(), ExprParseError> {
    if shape.is_empty() {
        Ok(())
    } else {
        Err(ExprParseError {
            message: "array literal elements must be scalar".into(),
            position,
        })
    }
}

pub(super) fn array_error(position: usize) -> impl FnOnce(ImageError) -> ExprParseError {
    move |error| ExprParseError {
        message: error.to_string(),
        position,
    }
}

pub(super) fn flip_compare_op(op: ImageExprCompareOp) -> ImageExprCompareOp {
    match op {
        ImageExprCompareOp::GreaterThan => ImageExprCompareOp::LessThan,
        ImageExprCompareOp::LessThan => ImageExprCompareOp::GreaterThan,
        ImageExprCompareOp::GreaterEqual => ImageExprCompareOp::LessEqual,
        ImageExprCompareOp::LessEqual => ImageExprCompareOp::GreaterEqual,
        ImageExprCompareOp::Equal => ImageExprCompareOp::Equal,
        ImageExprCompareOp::NotEqual => ImageExprCompareOp::NotEqual,
    }
}

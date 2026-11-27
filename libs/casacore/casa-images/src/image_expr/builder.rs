// SPDX-License-Identifier: LGPL-3.0-or-later

use super::*;

impl<'a, T: ImageExprValue> Clone for ImageExprBuilder<'a, T> {
    fn clone(&self) -> Self {
        Self {
            node: self.node.clone(),
            meta: self.meta.clone(),
            expr_string: self.expr_string.clone(),
        }
    }
}

impl<'a, T: ImageExprValue> fmt::Debug for ImageExprBuilder<'a, T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ImageExprBuilder")
            .field("shape", &self.meta.shape)
            .field("pixel_type", &T::PRIMITIVE_TYPE)
            .field("root", &self.node.kind_name())
            .finish()
    }
}

impl<'a, T: ImageExprValue> ImageExprBuilder<'a, T> {
    /// Returns the shape that the canonical expression will produce.
    pub fn shape(&self) -> &[usize] {
        &self.meta.shape
    }

    pub(crate) fn expression_coordinates(&self) -> Option<&CoordinateSystem> {
        (self.meta.coords.n_pixel_axes() == self.meta.shape.len()).then_some(&self.meta.coords)
    }

    /// Creates a lazy expression rooted at a source image.
    pub fn from_image<I>(image: &'a I) -> Result<Self, ImageError>
    where
        I: ImageInterface<T> + 'a,
    {
        Ok(Self {
            node: NumericExprNode::Source(image),
            meta: ImageExprMeta::from_image(image)?,
            expr_string: None,
        })
    }

    pub(crate) fn scalar(value: T) -> Self {
        Self {
            node: NumericExprNode::Scalar(value),
            meta: ImageExprMeta {
                shape: vec![],
                coords: CoordinateSystem::new(),
                units: String::new(),
                misc_info: casa_core::RecordValue::default(),
                image_info: ImageInfo::default(),
                name: None,
                legacy_coords: None,
                raw_image_info: None,
            },
            expr_string: None,
        }
    }

    /// Creates a lazy expression rooted at a trait-object image reference.
    ///
    /// This is the dynamic-dispatch counterpart of [`from_image`](Self::from_image),
    /// accepting `&dyn ImageInterface<T>` directly.  Used by the expression
    /// parser where image references are resolved at runtime.
    pub fn from_dyn(image: &'a dyn ImageInterface<T>) -> Result<Self, ImageError> {
        Ok(Self {
            node: NumericExprNode::Source(image),
            meta: ImageExprMeta::from_image(image)?,
            expr_string: None,
        })
    }

    pub(crate) fn from_vector(values: Vec<T>) -> Self {
        let mut meta = Self::scalar(T::default_value()).meta;
        meta.shape = vec![values.len()];
        Self {
            node: NumericExprNode::Vector(Arc::new(ndarray::Array1::from_vec(values).into_dyn())),
            meta,
            expr_string: None,
        }
    }

    pub(crate) fn custom_unary(
        self,
        name: &'static str,
        func: impl Fn(T) -> T + Send + Sync + 'static,
    ) -> Self {
        Self {
            node: NumericExprNode::CustomUnary {
                name,
                child: Box::new(self.node),
                func: Arc::new(func),
            },
            meta: self.meta,
            expr_string: None,
        }
    }

    /// Compatibility constructor for the previous `map`-based API.
    ///
    /// The mapped function is now applied lazily to each requested slice.
    pub fn map<I, F>(image: &'a I, f: F) -> Result<Self, ImageError>
    where
        I: ImageInterface<T> + 'a,
        F: Fn(T) -> T + Send + Sync + 'static,
    {
        Ok(Self::from_image(image)?.custom_unary("map", f))
    }

    /// Compatibility constructor for the previous `zip`-based API.
    ///
    /// The combining function is now applied lazily to each requested slice.
    pub fn zip<I, J, F>(lhs: &'a I, rhs: &'a J, f: F) -> Result<Self, ImageError>
    where
        I: ImageInterface<T> + 'a,
        J: ImageInterface<T> + 'a,
        F: Fn(T, T) -> T + Send + Sync + 'static,
    {
        let lhs_expr = Self::from_image(lhs)?;
        let rhs_expr = Self::from_image(rhs)?;
        lhs_expr.binary_expr_with_name(rhs_expr, "zip", Arc::new(f))
    }

    /// Applies a built-in unary operator lazily.
    pub fn unary(self, op: ImageExprUnaryOp) -> Self {
        Self {
            node: NumericExprNode::UnaryOp {
                op,
                child: Box::new(self.node),
            },
            meta: self.meta,
            expr_string: None,
        }
    }

    /// Convert a typed expression window without changing its geometry.
    pub(crate) fn map_pixel_type<U: ImageExprValue>(
        self,
        convert: impl Fn(T) -> U + Copy + Send + Sync + 'static,
    ) -> Result<ImageExprBuilder<'a, U>, ImageError> {
        Ok(self.compile()?.mapped_sections(convert))
    }

    pub fn from_section_readers(
        meta: ImageExprMeta,
        read: OwnedSliceReader<T>,
        mask: Option<OwnedMaskReader>,
    ) -> Self {
        Self {
            node: NumericExprNode::Sections(SectionSource {
                shape: meta.shape.clone(),
                read,
                mask,
            }),
            meta,
            expr_string: None,
        }
    }

    /// Bind owned, bounded section readers to an image's current geometry.
    ///
    /// The readers must address the same image and return arrays with the
    /// requested shape. The mask reader is used only for a masked image.
    ///
    /// # Errors
    /// Returns an error if the image metadata cannot be read.
    pub fn from_image_sections(
        image: &dyn ImageInterface<T>,
        coordinate_record: Option<&RecordValue>,
        read: impl Fn(&[usize], &[usize], &[usize]) -> Result<ArrayD<T>, ImageError>
        + Send
        + Sync
        + 'static,
        read_mask: impl Fn(&[usize], &[usize], &[usize]) -> Result<Option<ArrayD<bool>>, ImageError>
        + Send
        + Sync
        + 'static,
    ) -> Result<Self, ImageError> {
        let mut meta = ImageExprMeta::from_image(image)?;
        if let Some(record) = coordinate_record {
            meta.legacy_coords = Some(record.clone());
        }
        let mask = image
            .has_pixel_mask()
            .then(|| Arc::new(read_mask) as OwnedMaskReader);
        Ok(Self::from_section_readers(meta, Arc::new(read), mask))
    }

    /// Negates the expression lazily.
    pub fn negate(self) -> Self {
        self.unary(ImageExprUnaryOp::Negate)
    }

    /// Applies `exp(...)` lazily.
    pub fn exp(self) -> Self {
        self.unary(ImageExprUnaryOp::Exp)
    }

    /// Applies `sin(...)` lazily.
    pub fn sin(self) -> Self {
        self.unary(ImageExprUnaryOp::Sin)
    }

    /// Applies `cos(...)` lazily.
    pub fn cos(self) -> Self {
        self.unary(ImageExprUnaryOp::Cos)
    }

    /// Applies `tan(...)` lazily.
    pub fn tan(self) -> Self {
        self.unary(ImageExprUnaryOp::Tan)
    }

    /// Applies `asin(...)` lazily.
    pub fn asin(self) -> Self {
        self.unary(ImageExprUnaryOp::Asin)
    }

    /// Applies `acos(...)` lazily.
    pub fn acos(self) -> Self {
        self.unary(ImageExprUnaryOp::Acos)
    }

    /// Applies `atan(...)` lazily.
    pub fn atan(self) -> Self {
        self.unary(ImageExprUnaryOp::Atan)
    }

    /// Applies `sinh(...)` lazily.
    pub fn sinh(self) -> Self {
        self.unary(ImageExprUnaryOp::Sinh)
    }

    /// Applies `cosh(...)` lazily.
    pub fn cosh(self) -> Self {
        self.unary(ImageExprUnaryOp::Cosh)
    }

    /// Applies `tanh(...)` lazily.
    pub fn tanh(self) -> Self {
        self.unary(ImageExprUnaryOp::Tanh)
    }

    /// Applies `log(...)` (natural logarithm) lazily.
    pub fn log(self) -> Self {
        self.unary(ImageExprUnaryOp::Log)
    }

    /// Applies `log10(...)` lazily.
    pub fn log10(self) -> Self {
        self.unary(ImageExprUnaryOp::Log10)
    }

    /// Applies `sqrt(...)` lazily.
    pub fn sqrt(self) -> Self {
        self.unary(ImageExprUnaryOp::Sqrt)
    }

    /// Applies `abs(...)` lazily.  For complex types returns `Complex(|z|, 0)`.
    pub fn abs(self) -> Self {
        self.unary(ImageExprUnaryOp::Abs)
    }

    /// Applies `ceil(...)` lazily.  Component-wise for complex.
    pub fn ceil(self) -> Self {
        self.unary(ImageExprUnaryOp::Ceil)
    }

    /// Applies `floor(...)` lazily.  Component-wise for complex.
    pub fn floor(self) -> Self {
        self.unary(ImageExprUnaryOp::Floor)
    }

    /// Applies `round(...)` lazily.  Component-wise for complex.
    pub fn round(self) -> Self {
        self.unary(ImageExprUnaryOp::Round)
    }

    /// Applies `sign(...)` lazily.  For complex: `z / |z|` (or zero).
    pub fn sign(self) -> Self {
        self.unary(ImageExprUnaryOp::Sign)
    }

    /// Applies complex conjugate lazily.  Identity for real types.
    pub fn conj(self) -> Self {
        self.unary(ImageExprUnaryOp::Conj)
    }

    /// Combines two expressions using a built-in binary operator.
    ///
    /// Scalar (0-D) operands broadcast to the shape of the other operand,
    /// matching C++ LEL semantics where reductions produce scalars.
    pub fn binary_expr(self, rhs: Self, op: ImageExprBinaryOp) -> Result<Self, ImageError> {
        let out_shape = broadcast_shapes(&self.meta.shape, &rhs.meta.shape)?;
        let mut meta = if choose_rhs_meta(&self.meta.shape, &rhs.meta.shape, &out_shape) {
            rhs.meta
        } else {
            self.meta
        };
        meta.shape = out_shape;
        Ok(Self {
            node: NumericExprNode::BinaryOp {
                op,
                lhs: Box::new(self.node),
                rhs: Box::new(rhs.node),
            },
            meta,
            expr_string: None,
        })
    }

    pub(crate) fn binary_expr_with_name(
        self,
        rhs: Self,
        name: &'static str,
        func: BinaryExprFn<T>,
    ) -> Result<Self, ImageError> {
        let out_shape = broadcast_shapes(&self.meta.shape, &rhs.meta.shape)?;
        let mut meta = if choose_rhs_meta(&self.meta.shape, &rhs.meta.shape, &out_shape) {
            rhs.meta.clone()
        } else {
            self.meta.clone()
        };
        meta.shape = out_shape;
        Ok(Self {
            node: NumericExprNode::CustomBinary {
                name,
                lhs: Box::new(self.node),
                rhs: Box::new(rhs.node),
                func,
            },
            meta,
            expr_string: None,
        })
    }

    /// Combines this expression with another source image using a built-in binary operator.
    pub fn binary_image<I>(self, rhs: &'a I, op: ImageExprBinaryOp) -> Result<Self, ImageError>
    where
        I: ImageInterface<T> + 'a,
    {
        self.binary_expr(Self::from_image(rhs)?, op)
    }

    /// Applies a built-in binary operator against a scalar, broadcasting the scalar across the image.
    pub fn binary_scalar(self, rhs: T, op: ImageExprBinaryOp) -> Self {
        Self {
            node: NumericExprNode::BinaryOp {
                op,
                lhs: Box::new(self.node),
                rhs: Box::new(NumericExprNode::Scalar(rhs)),
            },
            meta: self.meta,
            expr_string: None,
        }
    }

    /// Applies a built-in binary operator with a scalar on the left side.
    ///
    /// Unlike [`binary_scalar`](Self::binary_scalar) where the scalar is on the
    /// right, this puts the scalar on the left: `scalar op image`.  Correctly
    /// handles non-commutative operators (subtract, divide, pow) and preserves
    /// non-finite pixel values (Inf, NaN) in the image operand.
    pub fn scalar_left_binary(scalar: T, rhs: Self, op: ImageExprBinaryOp) -> Self {
        Self {
            node: NumericExprNode::BinaryOp {
                op,
                lhs: Box::new(NumericExprNode::Scalar(scalar)),
                rhs: Box::new(rhs.node),
            },
            meta: rhs.meta,
            expr_string: None,
        }
    }

    /// Adds another expression lazily.
    pub fn add_expr(self, rhs: Self) -> Result<Self, ImageError> {
        self.binary_expr(rhs, ImageExprBinaryOp::Add)
    }

    /// Adds another source image lazily.
    pub fn add_image<I>(self, rhs: &'a I) -> Result<Self, ImageError>
    where
        I: ImageInterface<T> + 'a,
    {
        self.binary_image(rhs, ImageExprBinaryOp::Add)
    }

    /// Adds a scalar lazily.
    pub fn add_scalar(self, rhs: T) -> Self {
        self.binary_scalar(rhs, ImageExprBinaryOp::Add)
    }

    /// Multiplies by a scalar lazily.
    pub fn multiply_scalar(self, rhs: T) -> Self {
        self.binary_scalar(rhs, ImageExprBinaryOp::Multiply)
    }

    /// Multiplies two expressions lazily.
    pub fn multiply_expr(self, rhs: Self) -> Result<Self, ImageError> {
        self.binary_expr(rhs, ImageExprBinaryOp::Multiply)
    }

    /// Multiplies with another source image lazily.
    pub fn multiply_image<I>(self, rhs: &'a I) -> Result<Self, ImageError>
    where
        I: ImageInterface<T> + 'a,
    {
        self.binary_image(rhs, ImageExprBinaryOp::Multiply)
    }

    /// Subtracts another expression lazily.
    pub fn subtract_expr(self, rhs: Self) -> Result<Self, ImageError> {
        self.binary_expr(rhs, ImageExprBinaryOp::Subtract)
    }

    /// Subtracts another source image lazily.
    pub fn subtract_image<I>(self, rhs: &'a I) -> Result<Self, ImageError>
    where
        I: ImageInterface<T> + 'a,
    {
        self.binary_image(rhs, ImageExprBinaryOp::Subtract)
    }

    /// Subtracts a scalar lazily.
    pub fn subtract_scalar(self, rhs: T) -> Self {
        self.binary_scalar(rhs, ImageExprBinaryOp::Subtract)
    }

    /// Divides by another expression lazily.
    pub fn divide_expr(self, rhs: Self) -> Result<Self, ImageError> {
        self.binary_expr(rhs, ImageExprBinaryOp::Divide)
    }

    /// Divides by another source image lazily.
    pub fn divide_image<I>(self, rhs: &'a I) -> Result<Self, ImageError>
    where
        I: ImageInterface<T> + 'a,
    {
        self.binary_image(rhs, ImageExprBinaryOp::Divide)
    }

    /// Divides by a scalar lazily.
    pub fn divide_scalar(self, rhs: T) -> Self {
        self.binary_scalar(rhs, ImageExprBinaryOp::Divide)
    }

    /// Raises each pixel to a scalar power lazily: `pow(x, scalar)`.
    pub fn pow_scalar(self, rhs: T) -> Self {
        self.binary_scalar(rhs, ImageExprBinaryOp::Pow)
    }

    /// Element-wise power with another expression: `pow(self, rhs)`.
    pub fn pow_expr(self, rhs: Self) -> Result<Self, ImageError> {
        self.binary_expr(rhs, ImageExprBinaryOp::Pow)
    }

    /// Element-wise power with another source image: `pow(self, rhs)`.
    pub fn pow_image<I>(self, rhs: &'a I) -> Result<Self, ImageError>
    where
        I: ImageInterface<T> + 'a,
    {
        self.binary_image(rhs, ImageExprBinaryOp::Pow)
    }

    /// Element-wise minimum with a scalar.
    pub fn min_scalar(self, rhs: T) -> Self {
        self.binary_scalar(rhs, ImageExprBinaryOp::Min)
    }

    /// Element-wise minimum with another expression.
    pub fn min_expr(self, rhs: Self) -> Result<Self, ImageError> {
        self.binary_expr(rhs, ImageExprBinaryOp::Min)
    }

    /// Element-wise minimum with another source image.
    pub fn min_image<I>(self, rhs: &'a I) -> Result<Self, ImageError>
    where
        I: ImageInterface<T> + 'a,
    {
        self.binary_image(rhs, ImageExprBinaryOp::Min)
    }

    /// Element-wise maximum with a scalar.
    pub fn max_scalar(self, rhs: T) -> Self {
        self.binary_scalar(rhs, ImageExprBinaryOp::Max)
    }

    /// Element-wise maximum with another expression.
    pub fn max_expr(self, rhs: Self) -> Result<Self, ImageError> {
        self.binary_expr(rhs, ImageExprBinaryOp::Max)
    }

    /// Element-wise maximum with another source image.
    pub fn max_image<I>(self, rhs: &'a I) -> Result<Self, ImageError>
    where
        I: ImageInterface<T> + 'a,
    {
        self.binary_image(rhs, ImageExprBinaryOp::Max)
    }

    /// Floating-point remainder with a scalar: `fmod(x, scalar)`.
    pub fn fmod_scalar(self, rhs: T) -> Self {
        self.binary_scalar(rhs, ImageExprBinaryOp::Fmod)
    }

    /// Element-wise floating-point remainder with another expression.
    pub fn fmod_expr(self, rhs: Self) -> Result<Self, ImageError> {
        self.binary_expr(rhs, ImageExprBinaryOp::Fmod)
    }

    /// Element-wise floating-point remainder with another source image.
    pub fn fmod_image<I>(self, rhs: &'a I) -> Result<Self, ImageError>
    where
        I: ImageInterface<T> + 'a,
    {
        self.binary_image(rhs, ImageExprBinaryOp::Fmod)
    }

    /// Two-argument arc tangent with a scalar: `atan2(self, scalar)`.
    pub fn atan2_scalar(self, rhs: T) -> Self {
        self.binary_scalar(rhs, ImageExprBinaryOp::Atan2)
    }

    /// Two-argument arc tangent with another expression: `atan2(self, rhs)`.
    pub fn atan2_expr(self, rhs: Self) -> Result<Self, ImageError> {
        self.binary_expr(rhs, ImageExprBinaryOp::Atan2)
    }

    /// Two-argument arc tangent with another source image: `atan2(self, rhs)`.
    pub fn atan2_image<I>(self, rhs: &'a I) -> Result<Self, ImageError>
    where
        I: ImageInterface<T> + 'a,
    {
        self.binary_image(rhs, ImageExprBinaryOp::Atan2)
    }

    /// Comparison helper producing a lazy boolean mask expression.
    pub fn compare_scalar(self, scalar: T, op: ImageExprCompareOp) -> MaskExprBuilder<'a, T>
    where
        T: PartialOrd,
    {
        MaskExprBuilder {
            node: MaskExprNode::CompareScalar {
                op,
                expr: Box::new(self.node),
                scalar,
            },
            shape: self.meta.shape.clone(),
            metadata: Some(Box::new(self.meta)),
        }
    }

    /// Element-wise comparison of two expressions producing a lazy
    /// boolean mask, as LEL's image-to-image comparisons.
    pub fn compare_expr(
        self,
        rhs: Self,
        op: ImageExprCompareOp,
    ) -> Result<MaskExprBuilder<'a, T>, ImageError> {
        let out_shape = broadcast_shapes(&self.meta.shape, &rhs.meta.shape)?;
        let metadata = if choose_rhs_meta(&self.meta.shape, &rhs.meta.shape, &out_shape)
            || (self.meta.coords.n_pixel_axes() == 0 && rhs.meta.coords.n_pixel_axes() != 0)
        {
            rhs.meta
        } else {
            self.meta
        };
        Ok(MaskExprBuilder {
            node: MaskExprNode::CompareExpr {
                op,
                lhs: Box::new(self.node),
                rhs: Box::new(rhs.node),
            },
            shape: out_shape,
            metadata: Some(Box::new(metadata)),
        })
    }

    /// Convenience `>` comparison against a scalar.
    pub fn gt_scalar(self, scalar: T) -> MaskExprBuilder<'a, T>
    where
        T: PartialOrd,
    {
        self.compare_scalar(scalar, ImageExprCompareOp::GreaterThan)
    }

    /// Convenience `<` comparison against a scalar.
    pub fn lt_scalar(self, scalar: T) -> MaskExprBuilder<'a, T>
    where
        T: PartialOrd,
    {
        self.compare_scalar(scalar, ImageExprCompareOp::LessThan)
    }

    /// Convenience `>=` comparison against a scalar.
    pub fn ge_scalar(self, scalar: T) -> MaskExprBuilder<'a, T>
    where
        T: PartialOrd,
    {
        self.compare_scalar(scalar, ImageExprCompareOp::GreaterEqual)
    }

    /// Convenience `<=` comparison against a scalar.
    pub fn le_scalar(self, scalar: T) -> MaskExprBuilder<'a, T>
    where
        T: PartialOrd,
    {
        self.compare_scalar(scalar, ImageExprCompareOp::LessEqual)
    }

    /// Convenience `==` comparison against a scalar.
    pub fn eq_scalar(self, scalar: T) -> MaskExprBuilder<'a, T>
    where
        T: PartialOrd,
    {
        self.compare_scalar(scalar, ImageExprCompareOp::Equal)
    }

    /// Convenience `!=` comparison against a scalar.
    pub fn ne_scalar(self, scalar: T) -> MaskExprBuilder<'a, T>
    where
        T: PartialOrd,
    {
        self.compare_scalar(scalar, ImageExprCompareOp::NotEqual)
    }

    // -- Wave 14a: isnan, metadata queries --

    /// Element-wise NaN test producing a boolean mask.
    ///
    /// Corresponds to C++ LEL `isnan(expr)`.
    pub fn isnan(self) -> MaskExprBuilder<'a, T>
    where
        T: PartialOrd,
    {
        MaskExprBuilder {
            shape: self.meta.shape.clone(),
            metadata: Some(Box::new(self.meta)),
            node: MaskExprNode::IsNan {
                child: Box::new(self.node),
            },
        }
    }

    pub(crate) fn validity(self) -> MaskExprBuilder<'a, T> {
        MaskExprBuilder {
            shape: self.meta.shape.clone(),
            metadata: Some(Box::new(self.meta)),
            node: MaskExprNode::Validity {
                child: Box::new(self.node),
            },
        }
    }

    /// Returns the propagated default pixel mask when it can be derived from
    /// the numeric DAG.
    ///
    /// Direct image references return their on-disk default pixel mask.
    /// Built-in unary/binary numeric nodes propagate that mask through the
    /// expression tree, treating scalars as all-true and AND-ing image masks.
    /// Opaque typed closures and some non-elementwise nodes still return
    /// `None`, which callers treat as an all-true fallback.
    pub fn source_mask(&self) -> Result<Option<ArrayD<bool>>, ImageError> {
        if self.node.contains_conditional() {
            let compiled = self.compile()?;
            return compiled.get_mask_slice(&vec![0; self.ndim_value()], self.shape());
        }
        self.node.propagated_mask()
    }

    /// Returns the number of dimensions of this expression.
    ///
    /// Corresponds to C++ LEL `ndim(expr)`.
    pub fn ndim_value(&self) -> usize {
        self.meta.shape.len()
    }

    /// Returns the total number of elements in this expression.
    ///
    /// Corresponds to C++ LEL `nelem(expr)`.
    pub fn nelem_value(&self) -> usize {
        self.meta.shape.iter().product()
    }

    /// Returns the length of a specific axis.
    ///
    /// Corresponds to C++ LEL `length(expr, axis)`.
    pub fn length_value(&self, axis: usize) -> Option<usize> {
        self.meta.shape.get(axis).copied()
    }

    // -- Wave 14b: reduction methods --

    /// Reduces the entire expression to a scalar (0-D) sum.
    ///
    /// Matches C++ LEL `sum(expr)` which produces a scalar lattice.
    /// When combined with array expressions via binary ops, the scalar
    /// broadcasts to the array shape automatically.
    pub fn sum_reduce(self) -> Self {
        self.make_reduction(ReductionOp::Sum)
    }

    /// Reduces the entire expression to a scalar (0-D) minimum.
    ///
    /// Corresponds to C++ LEL `min1d(expr)`.
    pub fn min_reduce(self) -> Self {
        self.make_reduction(ReductionOp::Min)
    }

    /// Reduces the entire expression to a scalar (0-D) maximum.
    ///
    /// Corresponds to C++ LEL `max1d(expr)`.
    pub fn max_reduce(self) -> Self {
        self.make_reduction(ReductionOp::Max)
    }

    /// Reduces the entire expression to a scalar (0-D) mean.
    ///
    /// Corresponds to C++ LEL `mean1d(expr)`.
    pub fn mean_reduce(self) -> Self {
        self.make_reduction(ReductionOp::Mean)
    }

    /// Reduces the entire expression to a scalar (0-D) median.
    ///
    /// Corresponds to C++ LEL `median1d(expr)`.
    pub fn median_reduce(self) -> Self {
        self.make_reduction(ReductionOp::Median)
    }

    /// Reduces the entire expression to its sample variance (`n - 1` denominator).
    ///
    /// Corresponds to C++ LEL `variance1d(expr)`.
    pub fn variance_reduce(self) -> Self {
        self.make_reduction(ReductionOp::Variance)
    }

    /// Reduces the entire expression to its sample standard deviation.
    ///
    /// Corresponds to C++ LEL `stddev1d(expr)`.
    pub fn stddev_reduce(self) -> Self {
        self.make_reduction(ReductionOp::Stddev)
    }

    /// Reduces the entire expression to its mean absolute deviation.
    ///
    /// Corresponds to C++ LEL `avdev1d(expr)`.
    pub fn avdev_reduce(self) -> Self {
        self.make_reduction(ReductionOp::Avdev)
    }

    pub(crate) fn make_reduction(self, op: ReductionOp) -> Self {
        self.make_reduction_with_empty_value(op, None)
    }

    pub(crate) fn make_reduction_with_empty_value(
        self,
        op: ReductionOp,
        empty_value: Option<T>,
    ) -> Self {
        let child_shape = self.meta.shape.clone();
        let mut meta = self.meta;
        meta.shape = vec![];
        Self {
            node: NumericExprNode::Reduction {
                op,
                child: Box::new(self.node),
                child_shape,
                empty_value,
            },
            meta,
            expr_string: None,
        }
    }

    /// Computes the fractile (quantile) of all elements as a scalar (0-D).
    ///
    /// Corresponds to C++ LEL `fractile1d(expr, fraction)`.
    pub fn fractile(self, fraction: f64) -> Self {
        let child_shape = self.meta.shape.clone();
        let mut meta = self.meta;
        meta.shape = vec![];
        Self {
            node: NumericExprNode::Fractile {
                child: Box::new(self.node),
                child_shape,
                fraction,
            },
            meta,
            expr_string: None,
        }
    }

    /// Computes the range between two fractiles as a scalar (0-D).
    ///
    /// Corresponds to C++ LEL `fractilerange1d(expr, frac1, frac2)`.
    pub fn fractile_range(self, fraction1: f64, fraction2: f64) -> Self {
        let child_shape = self.meta.shape.clone();
        let mut meta = self.meta;
        meta.shape = vec![];
        Self {
            node: NumericExprNode::FractileRange {
                child: Box::new(self.node),
                child_shape,
                fraction1,
                fraction2,
            },
            meta,
            expr_string: None,
        }
    }

    // -- Wave 14c: conditional, mask interaction --

    /// Conditional expression: `iif(condition, true_val, false_val)`.
    ///
    /// Where the condition mask is `true`, returns `if_true` pixels; otherwise
    /// returns `if_false` pixels.
    ///
    /// Corresponds to C++ LEL `iif(cond, true, false)`.
    pub fn iif(
        condition: MaskExprBuilder<'a, T>,
        if_true: Self,
        if_false: Self,
    ) -> Result<Self, ImageError>
    where
        T: PartialOrd,
    {
        let out_shape = broadcast_shapes(&condition.shape, &if_true.meta.shape)?;
        let out_shape = broadcast_shapes(&out_shape, &if_false.meta.shape)?;
        let cond_node = condition.node;
        let mut meta = if choose_rhs_meta(&if_true.meta.shape, &if_false.meta.shape, &out_shape) {
            if_false.meta.clone()
        } else {
            if_true.meta.clone()
        };
        if meta.coords.n_pixel_axes() == 0
            && !condition.shape.is_empty()
            && let Some(metadata) = condition.metadata
        {
            meta = *metadata;
        }
        meta.shape = out_shape;
        Ok(Self {
            node: NumericExprNode::Conditional {
                condition: Box::new(cond_node),
                if_true: Box::new(if_true.node),
                if_false: Box::new(if_false.node),
            },
            meta,
            expr_string: None,
        })
    }

    /// Counts true values in a boolean mask, returning a scalar (0-D) count.
    ///
    /// Corresponds to C++ LEL `ntrue(mask)`.
    pub fn ntrue(mask: MaskExprBuilder<'a, T>) -> Self
    where
        T: PartialOrd,
    {
        Self::mask_count(mask, true)
    }

    /// Counts false values in a boolean mask, returning a scalar (0-D) count.
    ///
    /// Corresponds to C++ LEL `nfalse(mask)`.
    pub fn nfalse(mask: MaskExprBuilder<'a, T>) -> Self
    where
        T: PartialOrd,
    {
        Self::mask_count(mask, false)
    }

    fn mask_count(mask: MaskExprBuilder<'a, T>, count_true: bool) -> Self
    where
        T: PartialOrd,
    {
        let mask_shape = mask.shape.clone();
        let mask_node = mask.node;
        Self {
            node: NumericExprNode::MaskCount {
                count_true,
                mask: Box::new(mask_node),
                mask_shape,
            },
            meta: ImageExprMeta {
                shape: vec![],
                coords: CoordinateSystem::new(),
                units: String::new(),
                misc_info: casa_core::RecordValue::default(),
                image_info: ImageInfo::default(),
                name: None,
                legacy_coords: None,
                raw_image_info: None,
            },
            expr_string: None,
        }
    }

    /// Replace pixels where the mask is false with values from the replacement.
    ///
    /// Corresponds to C++ LEL `replace(expr, replacement)`.
    ///
    /// `replacement` may be same-shaped or scalar; scalar replacements
    /// broadcast across the primary expression.
    pub fn replace(self, replacement: Self, mask: ArrayD<bool>) -> Result<Self, ImageError> {
        self.replace_with_mask(replacement, Some(mask))
    }

    pub(crate) fn replace_invalid(self, replacement: Self) -> Result<Self, ImageError> {
        self.replace_with_mask(replacement, None)
    }

    fn replace_with_mask(
        self,
        replacement: Self,
        mask: Option<ArrayD<bool>>,
    ) -> Result<Self, ImageError> {
        if !can_broadcast_to(&replacement.meta.shape, &self.meta.shape) {
            return Err(ImageError::ShapeMismatch {
                expected: self.meta.shape.clone(),
                got: replacement.meta.shape.clone(),
            });
        }
        Ok(Self {
            node: NumericExprNode::Replace {
                primary: Box::new(self.node),
                replacement: Box::new(replacement.node),
                mask,
            },
            meta: self.meta,
            expr_string: None,
        })
    }

    /// Strips any pixel mask while leaving pixel values unchanged.
    ///
    /// Corresponds to C++ LEL `value(expr)`.
    pub fn strip_mask(self) -> Self {
        Self {
            node: NumericExprNode::StripMask {
                child: Box::new(self.node),
            },
            meta: self.meta,
            expr_string: None,
        }
    }

    /// Computes two-point spectral index `ln(self / rhs) / ln(f1 / f2)` lazily.
    ///
    /// Corresponds to C++ LEL `spectralindex(expr1, expr2)`. Clears output units.
    pub fn spectral_index(self, rhs: Self) -> Result<Self, ImageError> {
        self.spectral_index_with_metadata(rhs, true)
    }

    pub(crate) fn spectral_index_with_metadata(
        self,
        rhs: Self,
        clear_units: bool,
    ) -> Result<Self, ImageError> {
        let out_shape = broadcast_shapes(&self.meta.shape, &rhs.meta.shape)?;
        let (spec_axis_a, freqs_a) = spectral_frequencies_hz(&self.meta.coords, &self.meta.shape)?;
        let (spec_axis_b, freqs_b) = spectral_frequencies_hz(&rhs.meta.coords, &rhs.meta.shape)?;
        let mut meta = if choose_rhs_meta(&self.meta.shape, &rhs.meta.shape, &out_shape) {
            rhs.meta
        } else {
            self.meta
        };
        meta.shape = out_shape;
        if clear_units {
            meta.units.clear();
        }
        Ok(Self {
            node: NumericExprNode::SpectralIndex {
                lhs: Box::new(self.node),
                rhs: Box::new(rhs.node),
                spec_axis_a,
                freqs_a,
                spec_axis_b,
                freqs_b,
            },
            meta,
            expr_string: None,
        })
    }

    /// Computes linear polarization angle `0.5 * atan2(u, q) * 180 / pi` in degrees.
    ///
    /// Corresponds to C++ LEL `pa(u, q)`. Sets unit to `"deg"` and replaces any
    /// Stokes coordinate with `[StokesType::Pangle]`.
    pub fn pa(u: Self, q: Self) -> Result<Self, ImageError> {
        Self::pa_with_metadata(u, q, true)
    }

    pub(crate) fn pa_with_metadata(
        u: Self,
        q: Self,
        use_position_angle: bool,
    ) -> Result<Self, ImageError> {
        let out_shape = broadcast_shapes(&u.meta.shape, &q.meta.shape)?;
        let mut meta = if choose_rhs_meta(&u.meta.shape, &q.meta.shape, &out_shape) {
            q.meta
        } else {
            u.meta
        };
        meta.shape = out_shape;
        if use_position_angle {
            meta.units = "deg".to_string();
            replace_stokes_with_pangle(&mut meta.coords);
            meta.legacy_coords = None;
        }
        Ok(Self {
            node: NumericExprNode::Pa {
                u: Box::new(u.node),
                q: Box::new(q.node),
            },
            meta,
            expr_string: None,
        })
    }

    /// Block-averages pixel values by integer binning `factors`.
    ///
    /// Corresponds to C++ LEL `rebin(expr, factors)`.
    pub fn rebin(self, factors: Vec<usize>) -> Result<Self, ImageError> {
        let (out_shape, padded_factors) = rebin_shape(&self.meta.shape, &factors)?;
        let mut meta = self.meta;
        rebin_coordinates(&mut meta.coords, &padded_factors)?;
        meta.legacy_coords = None;
        let in_shape = meta.shape;
        meta.shape = out_shape.clone();
        Ok(Self {
            node: NumericExprNode::Rebin {
                child: Box::new(self.node),
                in_shape,
                factors: padded_factors,
                out_shape,
            },
            meta,
            expr_string: None,
        })
    }

    /// Persists the expression string in casacore-compatible `.imgexpr` format.
    ///
    /// Creates a directory at `path` containing `imageexpr.json` with the
    /// expression string, pixel data type, and miscellaneous metadata.  The
    /// source images referenced in the expression must remain accessible at
    /// the paths embedded in the expression string for any future reopen
    /// (by Rust or C++) to succeed.
    ///
    /// This is the Rust analogue of C++ `ImageExpr<T>::save()`.
    ///
    /// # Errors
    ///
    /// Returns an error if no expression string has been set (i.e. the
    /// expression was built programmatically without [`Self::set_expr_string`]),
    /// or if directory / file creation fails.
    pub fn save_expr(&self, path: impl AsRef<Path>) -> Result<(), ImageError> {
        let expr_str = self.expr_string.as_deref().ok_or_else(|| {
            ImageError::InvalidMetadata(
                "ImageExprBuilder cannot be persisted: no expression string is set".into(),
            )
        })?;
        crate::expr_file::save(path, expr_str, T::PRIMITIVE_TYPE, &self.meta.misc_info)
    }

    /// Returns the expression string, if one was set by the parser or
    /// [`set_expr_string`](Self::set_expr_string).
    pub fn expr_string(&self) -> Option<&str> {
        self.expr_string.as_deref()
    }

    /// Attaches a LEL expression string to this expression.
    ///
    /// This is required before calling [`save_expr`](Self::save_expr).
    /// The parser sets this automatically; users building expressions
    /// programmatically must call this with the corresponding LEL string
    /// if they want expression-preserving persistence.
    pub fn set_expr_string(&mut self, expr: impl Into<String>) {
        self.expr_string = Some(expr.into());
    }
}

// -- Wave 14d: Type-changing methods (typed API only) --

impl<'a> ImageExprBuilder<'a, casa_core::Complex32> {
    /// Extracts the real part of a Complex32 expression, producing an f32 expression.
    ///
    /// Corresponds to C++ LEL `real(expr)`.
    pub fn real_part(self) -> ImageExprBuilder<'a, f32> {
        let compiled = self.compile().map_err(|error| error.to_string());
        let meta = self.meta;
        ImageExprBuilder {
            node: NumericExprNode::TypeBridge {
                eval_fn: Arc::new(move |start, shape, stride| {
                    let data = compiled
                        .as_ref()
                        .map_err(|error| LatticeError::Table(error.clone()))?
                        .get_slice_with_stride(start, shape, stride)
                        .map_err(|error| LatticeError::Table(error.to_string()))?;
                    Ok(data.mapv(|v| v.re))
                }),
            },
            meta: ImageExprMeta {
                shape: meta.shape,
                coords: meta.coords,
                units: meta.units,
                misc_info: meta.misc_info,
                image_info: meta.image_info,
                name: meta.name,
                legacy_coords: meta.legacy_coords,
                raw_image_info: meta.raw_image_info,
            },
            expr_string: None,
        }
    }

    /// Extracts the imaginary part of a Complex32 expression, producing an f32 expression.
    ///
    /// Corresponds to C++ LEL `imag(expr)`.
    pub fn imag_part(self) -> ImageExprBuilder<'a, f32> {
        let compiled = self.compile().map_err(|error| error.to_string());
        let meta = self.meta;
        ImageExprBuilder {
            node: NumericExprNode::TypeBridge {
                eval_fn: Arc::new(move |start, shape, stride| {
                    let data = compiled
                        .as_ref()
                        .map_err(|error| LatticeError::Table(error.clone()))?
                        .get_slice_with_stride(start, shape, stride)
                        .map_err(|error| LatticeError::Table(error.to_string()))?;
                    Ok(data.mapv(|v| v.im))
                }),
            },
            meta: ImageExprMeta {
                shape: meta.shape,
                coords: meta.coords,
                units: meta.units,
                misc_info: meta.misc_info,
                image_info: meta.image_info,
                name: meta.name,
                legacy_coords: meta.legacy_coords,
                raw_image_info: meta.raw_image_info,
            },
            expr_string: None,
        }
    }

    /// Extracts the argument (phase angle) of a Complex32 expression, producing an f32 expression.
    ///
    /// Corresponds to C++ LEL `arg(expr)` = `atan2(im, re)`.
    pub fn arg_phase(self) -> ImageExprBuilder<'a, f32> {
        let compiled = self.compile().map_err(|error| error.to_string());
        let meta = self.meta;
        ImageExprBuilder {
            node: NumericExprNode::TypeBridge {
                eval_fn: Arc::new(move |start, shape, stride| {
                    let data = compiled
                        .as_ref()
                        .map_err(|error| LatticeError::Table(error.clone()))?
                        .get_slice_with_stride(start, shape, stride)
                        .map_err(|error| LatticeError::Table(error.to_string()))?;
                    Ok(data.mapv(|v| v.im.atan2(v.re)))
                }),
            },
            meta: ImageExprMeta {
                shape: meta.shape,
                coords: meta.coords,
                units: meta.units,
                misc_info: meta.misc_info,
                image_info: meta.image_info,
                name: meta.name,
                legacy_coords: meta.legacy_coords,
                raw_image_info: meta.raw_image_info,
            },
            expr_string: None,
        }
    }
}

impl<'a> ImageExprBuilder<'a, casa_core::Complex64> {
    /// Extracts the real part of a Complex64 expression, producing an f64 expression.
    pub fn real_part(self) -> ImageExprBuilder<'a, f64> {
        let compiled = self.compile().map_err(|error| error.to_string());
        let meta = self.meta;
        ImageExprBuilder {
            node: NumericExprNode::TypeBridge {
                eval_fn: Arc::new(move |start, shape, stride| {
                    let data = compiled
                        .as_ref()
                        .map_err(|error| LatticeError::Table(error.clone()))?
                        .get_slice_with_stride(start, shape, stride)
                        .map_err(|error| LatticeError::Table(error.to_string()))?;
                    Ok(data.mapv(|v| v.re))
                }),
            },
            meta: ImageExprMeta {
                shape: meta.shape,
                coords: meta.coords,
                units: meta.units,
                misc_info: meta.misc_info,
                image_info: meta.image_info,
                name: meta.name,
                legacy_coords: meta.legacy_coords,
                raw_image_info: meta.raw_image_info,
            },
            expr_string: None,
        }
    }

    /// Extracts the imaginary part of a Complex64 expression, producing an f64 expression.
    pub fn imag_part(self) -> ImageExprBuilder<'a, f64> {
        let compiled = self.compile().map_err(|error| error.to_string());
        let meta = self.meta;
        ImageExprBuilder {
            node: NumericExprNode::TypeBridge {
                eval_fn: Arc::new(move |start, shape, stride| {
                    let data = compiled
                        .as_ref()
                        .map_err(|error| LatticeError::Table(error.clone()))?
                        .get_slice_with_stride(start, shape, stride)
                        .map_err(|error| LatticeError::Table(error.to_string()))?;
                    Ok(data.mapv(|v| v.im))
                }),
            },
            meta: ImageExprMeta {
                shape: meta.shape,
                coords: meta.coords,
                units: meta.units,
                misc_info: meta.misc_info,
                image_info: meta.image_info,
                name: meta.name,
                legacy_coords: meta.legacy_coords,
                raw_image_info: meta.raw_image_info,
            },
            expr_string: None,
        }
    }

    /// Extracts the argument (phase angle) of a Complex64 expression, producing an f64 expression.
    pub fn arg_phase(self) -> ImageExprBuilder<'a, f64> {
        let compiled = self.compile().map_err(|error| error.to_string());
        let meta = self.meta;
        ImageExprBuilder {
            node: NumericExprNode::TypeBridge {
                eval_fn: Arc::new(move |start, shape, stride| {
                    let data = compiled
                        .as_ref()
                        .map_err(|error| LatticeError::Table(error.clone()))?
                        .get_slice_with_stride(start, shape, stride)
                        .map_err(|error| LatticeError::Table(error.to_string()))?;
                    Ok(data.mapv(|v| v.im.atan2(v.re)))
                }),
            },
            meta: ImageExprMeta {
                shape: meta.shape,
                coords: meta.coords,
                units: meta.units,
                misc_info: meta.misc_info,
                image_info: meta.image_info,
                name: meta.name,
                legacy_coords: meta.legacy_coords,
                raw_image_info: meta.raw_image_info,
            },
            expr_string: None,
        }
    }
}

impl<'a> ImageExprBuilder<'a, f32> {
    /// Combines two f32 expressions into a Complex32 expression.
    ///
    /// Corresponds to C++ LEL `complex(real, imag)`.
    pub fn to_complex(
        self,
        imag: Self,
    ) -> Result<ImageExprBuilder<'a, casa_core::Complex32>, ImageError> {
        validate_same_shape(self.shape(), imag.shape())?;
        let real_compiled = self.compile().map_err(|error| error.to_string());
        let imag_compiled = imag.compile().map_err(|error| error.to_string());
        let meta = self.meta;
        Ok(ImageExprBuilder {
            node: NumericExprNode::TypeBridge {
                eval_fn: Arc::new(move |start, shape, stride| {
                    let re = real_compiled
                        .as_ref()
                        .map_err(|error| LatticeError::Table(error.clone()))?
                        .get_slice_with_stride(start, shape, stride)
                        .map_err(|error| LatticeError::Table(error.to_string()))?;
                    let im = imag_compiled
                        .as_ref()
                        .map_err(|error| LatticeError::Table(error.clone()))?
                        .get_slice_with_stride(start, shape, stride)
                        .map_err(|error| LatticeError::Table(error.to_string()))?;
                    Ok(Zip::from(&re)
                        .and(&im)
                        .map_collect(|&re, &im| casa_core::Complex32::new(re, im)))
                }),
            },
            meta: ImageExprMeta {
                shape: meta.shape,
                coords: meta.coords,
                units: meta.units,
                misc_info: meta.misc_info,
                image_info: meta.image_info,
                name: meta.name,
                legacy_coords: meta.legacy_coords,
                raw_image_info: meta.raw_image_info,
            },
            expr_string: None,
        })
    }
}

impl<'a> ImageExprBuilder<'a, f64> {
    /// Combines two f64 expressions into a Complex64 expression.
    pub fn to_complex(
        self,
        imag: Self,
    ) -> Result<ImageExprBuilder<'a, casa_core::Complex64>, ImageError> {
        validate_same_shape(self.shape(), imag.shape())?;
        let real_compiled = self.compile().map_err(|error| error.to_string());
        let imag_compiled = imag.compile().map_err(|error| error.to_string());
        let meta = self.meta;
        Ok(ImageExprBuilder {
            node: NumericExprNode::TypeBridge {
                eval_fn: Arc::new(move |start, shape, stride| {
                    let re = real_compiled
                        .as_ref()
                        .map_err(|error| LatticeError::Table(error.clone()))?
                        .get_slice_with_stride(start, shape, stride)
                        .map_err(|error| LatticeError::Table(error.to_string()))?;
                    let im = imag_compiled
                        .as_ref()
                        .map_err(|error| LatticeError::Table(error.clone()))?
                        .get_slice_with_stride(start, shape, stride)
                        .map_err(|error| LatticeError::Table(error.to_string()))?;
                    Ok(Zip::from(&re)
                        .and(&im)
                        .map_collect(|&re, &im| casa_core::Complex64::new(re, im)))
                }),
            },
            meta: ImageExprMeta {
                shape: meta.shape,
                coords: meta.coords,
                units: meta.units,
                misc_info: meta.misc_info,
                image_info: meta.image_info,
                name: meta.name,
                legacy_coords: meta.legacy_coords,
                raw_image_info: meta.raw_image_info,
            },
            expr_string: None,
        })
    }
}

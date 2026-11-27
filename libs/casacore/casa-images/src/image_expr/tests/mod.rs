// SPDX-License-Identifier: LGPL-3.0-or-later

use std::cell::RefCell;
use std::rc::Rc;

use super::*;
use casa_coordinates::CoordinateSystem;
use casa_core::{Complex32, Complex64, RecordValue};
use casa_lattices::Lattice;
use ndarray::{Dimension, IxDyn};

trait CanonicalNumericEval<T: ImageExprValue> {
    fn get(&self) -> Result<ArrayD<T>, ImageError>;
    fn get_at(&self, position: &[usize]) -> Result<T, ImageError>;
    fn get_slice(&self, start: &[usize], shape: &[usize]) -> Result<ArrayD<T>, ImageError>;
    fn save_as(&self, path: impl AsRef<Path>) -> Result<crate::PagedImage<T>, ImageError>;
}

impl<T: ImageExprValue> CanonicalNumericEval<T> for ImageExprBuilder<'_, T> {
    fn get(&self) -> Result<ArrayD<T>, ImageError> {
        self.compile()?.get()
    }

    fn get_at(&self, position: &[usize]) -> Result<T, ImageError> {
        self.compile()?.get_at(position)
    }

    fn get_slice(&self, start: &[usize], shape: &[usize]) -> Result<ArrayD<T>, ImageError> {
        self.compile()?.get_slice(start, shape)
    }

    fn save_as(&self, path: impl AsRef<Path>) -> Result<crate::PagedImage<T>, ImageError> {
        self.compile()?.save_as(path)
    }
}

trait CanonicalMaskEval {
    fn get(&self) -> Result<ArrayD<bool>, ImageError>;
}

impl<T: ImageExprValue + PartialOrd> CanonicalMaskEval for MaskExprBuilder<'_, T> {
    fn get(&self) -> Result<ArrayD<bool>, ImageError> {
        self.compile()?.get()
    }
}

struct CountingImage<T: ImageExprValue> {
    data: ArrayD<T>,
    coords: CoordinateSystem,
    cursor_shape: Option<Vec<usize>>,
    slice_shapes: Rc<RefCell<Vec<Vec<usize>>>>,
}

impl<T: ImageExprValue> CountingImage<T> {
    fn new(data: ArrayD<T>) -> (Self, Rc<RefCell<Vec<Vec<usize>>>>) {
        Self::new_with_cursor_shape(data, None)
    }

    fn new_with_cursor_shape(
        data: ArrayD<T>,
        cursor_shape: Option<Vec<usize>>,
    ) -> (Self, Rc<RefCell<Vec<Vec<usize>>>>) {
        let slice_shapes = Rc::new(RefCell::new(Vec::new()));
        (
            Self {
                data,
                coords: CoordinateSystem::new(),
                cursor_shape,
                slice_shapes: Rc::clone(&slice_shapes),
            },
            slice_shapes,
        )
    }
}

impl<T: ImageExprValue> Lattice<T> for CountingImage<T> {
    fn shape(&self) -> &[usize] {
        self.data.shape()
    }

    fn get_at(&self, position: &[usize]) -> Result<T, LatticeError> {
        self.data
            .get(IxDyn(position))
            .cloned()
            .ok_or_else(|| LatticeError::IndexOutOfBounds {
                index: position.to_vec(),
                shape: self.data.shape().to_vec(),
            })
    }

    fn get_slice(
        &self,
        start: &[usize],
        shape: &[usize],
        stride: &[usize],
    ) -> Result<ArrayD<T>, LatticeError> {
        self.slice_shapes.borrow_mut().push(shape.to_vec());
        let mut out = ArrayD::from_elem(IxDyn(shape), T::default_value());
        for (idx, value) in out.indexed_iter_mut() {
            let src: Vec<usize> = idx
                .slice()
                .iter()
                .zip(start.iter())
                .zip(stride.iter())
                .map(|((&i, &s), &st)| s + i * st)
                .collect();
            *value = self.get_at(&src)?;
        }
        Ok(out)
    }

    fn nice_cursor_shape(&self) -> Vec<usize> {
        self.cursor_shape
            .clone()
            .unwrap_or_else(|| self.data.shape().to_vec())
    }
}

impl<T: ImageExprValue> ImageInterface<T> for CountingImage<T> {
    fn as_any(&self) -> Option<&dyn std::any::Any> {
        None
    }

    fn coordinates(&self) -> &CoordinateSystem {
        &self.coords
    }

    fn units(&self) -> &str {
        ""
    }

    fn misc_info(&self) -> casa_core::RecordValue {
        RecordValue::default()
    }

    fn image_info(&self) -> Result<ImageInfo, ImageError> {
        Ok(ImageInfo::default())
    }

    fn name(&self) -> Option<&Path> {
        None
    }
}

fn make_persistent_image<T: ImageExprValue>(shape: Vec<usize>) -> crate::TempImage<T> {
    crate::TempImage::new(
        shape,
        CoordinateSystem::new(),
        casa_lattices::TempStoragePolicy::Memory,
    )
    .unwrap()
}

#[test]
fn nonpersistent_reduction_snapshots_source_once() {
    let shape = vec![4097, 1024];
    let data = ArrayD::from_elem(IxDyn(&shape), 1.0f32);
    let (image, slice_shapes) = CountingImage::new_with_cursor_shape(data, Some(vec![256, 128]));
    let expr = ImageExprBuilder::from_image(&image)
        .unwrap()
        .add_scalar(0.0)
        .sum_reduce();

    let got = expr.get().unwrap();
    assert_eq!(got[IxDyn(&[])], (shape[0] * shape[1]) as f32);

    let seen = slice_shapes.borrow();
    assert_eq!(seen.as_slice(), &[shape]);
}

#[test]
fn nonpersistent_slice_reuses_owned_snapshot() {
    let data = ArrayD::from_shape_fn(IxDyn(&[8, 8]), |idx| (idx[0] * 10 + idx[1]) as f32);
    let (image, slice_shapes) = CountingImage::new(data);

    let expr = ImageExprBuilder::from_image(&image)
        .unwrap()
        .add_scalar(2.0)
        .exp();
    let slice = expr.get_slice(&[3, 4], &[1, 2]).unwrap();

    assert_eq!(slice.shape(), &[1, 2]);
    let recorded = slice_shapes.borrow();
    assert_eq!(recorded.as_slice(), &[vec![8, 8]]);
}

#[test]
fn image_and_scalar_ops_are_lazy_and_correct() {
    let mut lhs = make_persistent_image::<f32>(vec![2, 2]);
    let mut rhs = make_persistent_image::<f32>(vec![2, 2]);
    lhs.put_slice(
        &ArrayD::from_shape_vec(IxDyn(&[2, 2]), vec![1.0, 2.0, 3.0, 4.0]).unwrap(),
        &[0, 0],
    )
    .unwrap();
    rhs.put_slice(
        &ArrayD::from_shape_vec(IxDyn(&[2, 2]), vec![0.5, 1.0, 1.5, 2.0]).unwrap(),
        &[0, 0],
    )
    .unwrap();

    let expr = ImageExprBuilder::from_image(&lhs)
        .unwrap()
        .add_image(&rhs)
        .unwrap()
        .multiply_scalar(2.0)
        .negate();

    let got = expr.get().unwrap();
    let expected = ArrayD::from_shape_vec(IxDyn(&[2, 2]), vec![-3.0, -6.0, -9.0, -12.0]).unwrap();
    assert_eq!(got, expected);
}

#[test]
fn complex_ops_work_without_materializing_sources() {
    let mut lhs = make_persistent_image::<Complex32>(vec![2, 2]);
    let mut rhs = make_persistent_image::<Complex32>(vec![2, 2]);
    lhs.put_slice(
        &ArrayD::from_shape_vec(
            IxDyn(&[2, 2]),
            vec![
                Complex32::new(1.0, 0.0),
                Complex32::new(0.0, 1.0),
                Complex32::new(-1.0, 0.5),
                Complex32::new(0.5, -0.25),
            ],
        )
        .unwrap(),
        &[0, 0],
    )
    .unwrap();
    rhs.put_slice(
        &ArrayD::from_shape_vec(
            IxDyn(&[2, 2]),
            vec![
                Complex32::new(0.5, 0.5),
                Complex32::new(1.0, 0.0),
                Complex32::new(0.0, -0.5),
                Complex32::new(-0.5, 0.25),
            ],
        )
        .unwrap(),
        &[0, 0],
    )
    .unwrap();

    let expr = ImageExprBuilder::from_image(&lhs)
        .unwrap()
        .add_image(&rhs)
        .unwrap()
        .exp();
    let got = expr.get().unwrap();
    let expected = lhs.get().unwrap() + rhs.get().unwrap();
    for (lhs_value, rhs_value) in got.iter().zip(expected.iter()) {
        let diff = *lhs_value - rhs_value.exp();
        assert!(diff.norm() < 1.0e-5);
    }
}

#[test]
fn float64_zip_wrapper_is_lazy_and_correct() {
    let mut lhs = make_persistent_image::<f64>(vec![2, 2]);
    let mut rhs = make_persistent_image::<f64>(vec![2, 2]);
    lhs.set(3.0).unwrap();
    rhs.set(4.0).unwrap();

    let expr = ImageExprBuilder::zip(&lhs, &rhs, |a, b| a * b - 2.0).unwrap();
    assert_eq!(expr.get_at(&[1, 1]).unwrap(), 10.0);
}

#[test]
fn complex64_map_wrapper_stays_lazy() {
    let mut image = make_persistent_image::<Complex64>(vec![2, 2]);
    image.set(Complex64::new(1.0, 2.0)).unwrap();

    let expr = ImageExprBuilder::map(&image, |value| value * Complex64::new(2.0, 0.0)).unwrap();
    assert_eq!(expr.get_at(&[0, 0]).unwrap(), Complex64::new(2.0, 4.0));
}

#[test]
fn comparison_and_logical_masks_are_lazy() {
    let mut image = make_persistent_image::<f32>(vec![2, 3]);
    image
        .put_slice(
            &ArrayD::from_shape_vec(IxDyn(&[2, 3]), vec![0.0, 1.0, 2.0, 3.0, 4.0, 5.0]).unwrap(),
            &[0, 0],
        )
        .unwrap();

    let mask = ImageExprBuilder::from_image(&image)
        .unwrap()
        .gt_scalar(1.0)
        .and(ImageExprBuilder::from_image(&image).unwrap().lt_scalar(4.5))
        .unwrap()
        .logical_not();

    let got = mask.get().unwrap();
    let expected =
        ArrayD::from_shape_vec(IxDyn(&[2, 3]), vec![true, true, false, false, false, true])
            .unwrap();
    assert_eq!(got, expected);
}

#[test]
fn save_as_materializes_the_current_expression() {
    let dir = tempfile::tempdir().unwrap();
    let out = dir.path().join("expr.image");
    let mut image = make_persistent_image::<f32>(vec![2, 2]);
    image.set(2.0).unwrap();

    let expr = ImageExprBuilder::from_image(&image)
        .unwrap()
        .multiply_scalar(3.0);
    let compiled = expr.compile().unwrap();
    assert_eq!(compiled.get().unwrap().sum(), 24.0);
    let saved = compiled.save_as(&out).unwrap();

    assert_eq!(saved.get().unwrap().sum(), 24.0);
}

#[test]
fn compiled_save_as_carries_propagated_default_mask() {
    let dir = tempfile::tempdir().unwrap();
    let out = dir.path().join("masked_expr.image");
    let mut image = crate::TempImage::<f32>::new(
        vec![2, 2],
        CoordinateSystem::new(),
        casa_lattices::TempStoragePolicy::Memory,
    )
    .unwrap();
    image
        .put_slice(
            &ArrayD::from_shape_vec(IxDyn(&[2, 2]), vec![1.0, 2.0, 3.0, 4.0]).unwrap(),
            &[0, 0],
        )
        .unwrap();
    image.make_mask("quality", true, true).unwrap();
    image
        .put_mask(
            "quality",
            &ArrayD::from_shape_vec(IxDyn(&[2, 2]), vec![true, false, true, false]).unwrap(),
        )
        .unwrap();

    let expr = ImageExprBuilder::from_image(&image)
        .unwrap()
        .multiply_scalar(2.0);
    let compiled = expr.compile().unwrap();
    assert_eq!(
        compiled.source_mask().unwrap(),
        ArrayD::from_shape_vec(IxDyn(&[2, 2]), vec![true, false, true, false]).unwrap()
    );
    let saved = compiled.save_as(&out).unwrap();

    assert_eq!(
        saved.get().unwrap(),
        ArrayD::from_shape_vec(IxDyn(&[2, 2]), vec![2.0, 4.0, 6.0, 8.0]).unwrap()
    );
    assert_eq!(saved.default_mask_name().as_deref(), Some("compiled_mask"));
    assert_eq!(
        saved.get_mask().unwrap().unwrap(),
        ArrayD::from_shape_vec(IxDyn(&[2, 2]), vec![true, false, true, false]).unwrap()
    );
}

#[test]
fn borrowed_save_as_materializes_cpp_compatible_masked_image() {
    let dir = tempfile::tempdir().unwrap();
    let out = dir.path().join("borrowed_expr.image");
    let mut image = crate::TempImage::<f32>::new(
        vec![2, 2],
        CoordinateSystem::new(),
        casa_lattices::TempStoragePolicy::Memory,
    )
    .unwrap();
    image
        .put_slice(
            &ArrayD::from_shape_vec(IxDyn(&[2, 2]), vec![1.0, 2.0, 3.0, 4.0]).unwrap(),
            &[0, 0],
        )
        .unwrap();
    image.make_mask("quality", true, true).unwrap();
    image
        .put_mask(
            "quality",
            &ArrayD::from_shape_vec(IxDyn(&[2, 2]), vec![true, false, false, true]).unwrap(),
        )
        .unwrap();

    let expr = ImageExprBuilder::from_image(&image)
        .unwrap()
        .multiply_scalar(3.0);
    let saved = expr.save_as(&out).unwrap();

    assert_eq!(
        saved.get().unwrap(),
        ArrayD::from_shape_vec(IxDyn(&[2, 2]), vec![3.0, 6.0, 9.0, 12.0]).unwrap()
    );
    assert_eq!(saved.default_mask_name().as_deref(), Some("compiled_mask"));
    assert_eq!(
        saved.get_mask().unwrap().unwrap(),
        ArrayD::from_shape_vec(IxDyn(&[2, 2]), vec![true, false, false, true]).unwrap()
    );
}

#[test]
fn read_only_mutation_paths_are_explicit() {
    let mut image = make_persistent_image::<f32>(vec![1, 1]);
    image.set(1.0).unwrap();
    let expr = ImageExprBuilder::from_image(&image)
        .unwrap()
        .add_scalar(2.0)
        .compile()
        .unwrap();

    assert!(!Lattice::is_writable(&expr));
}

// ---- Wave 11b: exhaustive operator matrix tests ----

#[test]
fn transcendental_unary_ops_f32() {
    let mut image = make_persistent_image::<f32>(vec![2, 2]);
    image
        .put_slice(
            &ArrayD::from_shape_vec(IxDyn(&[2, 2]), vec![0.5, 1.0, 0.25, 0.75]).unwrap(),
            &[0, 0],
        )
        .unwrap();

    let e = |op| ImageExprBuilder::from_image(&image).unwrap().unary(op);

    let check = |op: ImageExprUnaryOp, expected_fn: fn(f32) -> f32| {
        let got = e(op).get().unwrap();
        let src = image.get().unwrap();
        for (g, s) in got.iter().zip(src.iter()) {
            let want = expected_fn(*s);
            assert!(
                (g - want).abs() < 1e-6,
                "{op:?}: got {g}, want {want} for input {s}"
            );
        }
    };

    check(ImageExprUnaryOp::Sin, f32::sin);
    check(ImageExprUnaryOp::Cos, f32::cos);
    check(ImageExprUnaryOp::Tan, f32::tan);
    check(ImageExprUnaryOp::Asin, f32::asin);
    check(ImageExprUnaryOp::Acos, f32::acos);
    check(ImageExprUnaryOp::Atan, f32::atan);
    check(ImageExprUnaryOp::Sinh, f32::sinh);
    check(ImageExprUnaryOp::Cosh, f32::cosh);
    check(ImageExprUnaryOp::Tanh, f32::tanh);
    check(ImageExprUnaryOp::Exp, f32::exp);
    check(ImageExprUnaryOp::Log, f32::ln);
    check(ImageExprUnaryOp::Log10, f32::log10);
    check(ImageExprUnaryOp::Sqrt, f32::sqrt);
    check(ImageExprUnaryOp::Abs, f32::abs);
    check(ImageExprUnaryOp::Ceil, f32::ceil);
    check(ImageExprUnaryOp::Floor, f32::floor);
    check(ImageExprUnaryOp::Round, f32::round);
    check(ImageExprUnaryOp::Sign, |x| {
        if x > 0.0 {
            1.0
        } else if x < 0.0 {
            -1.0
        } else {
            0.0
        }
    });
    check(ImageExprUnaryOp::Conj, |x| x); // identity for real
}

#[test]
fn transcendental_unary_ops_f64() {
    let mut image = make_persistent_image::<f64>(vec![2, 2]);
    image
        .put_slice(
            &ArrayD::from_shape_vec(IxDyn(&[2, 2]), vec![0.5, 1.0, 0.25, 0.75]).unwrap(),
            &[0, 0],
        )
        .unwrap();

    let got_sqrt = ImageExprBuilder::from_image(&image)
        .unwrap()
        .sqrt()
        .get()
        .unwrap();
    let src = image.get().unwrap();
    for (g, s) in got_sqrt.iter().zip(src.iter()) {
        assert!((g - s.sqrt()).abs() < 1e-14);
    }

    let got_log = ImageExprBuilder::from_image(&image)
        .unwrap()
        .log()
        .get()
        .unwrap();
    for (g, s) in got_log.iter().zip(src.iter()) {
        assert!((g - s.ln()).abs() < 1e-14);
    }
}

#[test]
fn transcendental_unary_ops_complex32() {
    let mut image = make_persistent_image::<Complex32>(vec![2, 2]);
    image
        .put_slice(
            &ArrayD::from_shape_vec(
                IxDyn(&[2, 2]),
                vec![
                    Complex32::new(1.0, 0.5),
                    Complex32::new(0.5, -0.25),
                    Complex32::new(-0.5, 1.0),
                    Complex32::new(0.25, 0.75),
                ],
            )
            .unwrap(),
            &[0, 0],
        )
        .unwrap();

    let src = image.get().unwrap();

    // sin
    let got = ImageExprBuilder::from_image(&image)
        .unwrap()
        .sin()
        .get()
        .unwrap();
    for (g, s) in got.iter().zip(src.iter()) {
        let want = s.sin();
        assert!((g - want).norm() < 1e-5, "sin: got {g}, want {want}");
    }

    // sqrt
    let got = ImageExprBuilder::from_image(&image)
        .unwrap()
        .sqrt()
        .get()
        .unwrap();
    for (g, s) in got.iter().zip(src.iter()) {
        let want = s.sqrt();
        assert!((g - want).norm() < 1e-5, "sqrt: got {g}, want {want}");
    }

    // conj
    let got = ImageExprBuilder::from_image(&image)
        .unwrap()
        .conj()
        .get()
        .unwrap();
    for (g, s) in got.iter().zip(src.iter()) {
        let want = s.conj();
        assert_eq!(*g, want);
    }

    // abs returns Complex(|z|, 0)
    let got = ImageExprBuilder::from_image(&image)
        .unwrap()
        .abs()
        .get()
        .unwrap();
    for (g, s) in got.iter().zip(src.iter()) {
        assert!((g.re - s.norm()).abs() < 1e-5);
        assert_eq!(g.im, 0.0);
    }
}

#[test]
fn binary_function_ops_f32() {
    let mut lhs = make_persistent_image::<f32>(vec![2, 2]);
    let mut rhs = make_persistent_image::<f32>(vec![2, 2]);
    lhs.put_slice(
        &ArrayD::from_shape_vec(IxDyn(&[2, 2]), vec![4.0, 9.0, 16.0, 25.0]).unwrap(),
        &[0, 0],
    )
    .unwrap();
    rhs.put_slice(
        &ArrayD::from_shape_vec(IxDyn(&[2, 2]), vec![0.5, 0.5, 0.5, 0.5]).unwrap(),
        &[0, 0],
    )
    .unwrap();

    // pow
    let got = ImageExprBuilder::from_image(&lhs)
        .unwrap()
        .pow_scalar(0.5)
        .get()
        .unwrap();
    let expected = ArrayD::from_shape_vec(IxDyn(&[2, 2]), vec![2.0, 3.0, 4.0, 5.0]).unwrap();
    for (g, e) in got.iter().zip(expected.iter()) {
        assert!((g - e).abs() < 1e-5);
    }

    // min
    let got = ImageExprBuilder::from_image(&lhs)
        .unwrap()
        .min_scalar(10.0)
        .get()
        .unwrap();
    let expected = ArrayD::from_shape_vec(IxDyn(&[2, 2]), vec![4.0, 9.0, 10.0, 10.0]).unwrap();
    assert_eq!(got, expected);

    // max
    let got = ImageExprBuilder::from_image(&lhs)
        .unwrap()
        .max_scalar(10.0)
        .get()
        .unwrap();
    let expected = ArrayD::from_shape_vec(IxDyn(&[2, 2]), vec![10.0, 10.0, 16.0, 25.0]).unwrap();
    assert_eq!(got, expected);

    // fmod
    let got = ImageExprBuilder::from_image(&lhs)
        .unwrap()
        .fmod_scalar(7.0)
        .get()
        .unwrap();
    let expected = ArrayD::from_shape_vec(IxDyn(&[2, 2]), vec![4.0, 2.0, 2.0, 4.0]).unwrap();
    assert_eq!(got, expected);

    // atan2 (expr variant)
    let got = ImageExprBuilder::from_image(&lhs)
        .unwrap()
        .atan2_expr(ImageExprBuilder::from_image(&rhs).unwrap())
        .unwrap()
        .get()
        .unwrap();
    let lhs_data = lhs.get().unwrap();
    let rhs_data = rhs.get().unwrap();
    for ((g, l), r) in got.iter().zip(lhs_data.iter()).zip(rhs_data.iter()) {
        assert!((g - l.atan2(*r)).abs() < 1e-5);
    }
}

#[test]
fn subtract_and_divide_convenience_methods() {
    let mut lhs = make_persistent_image::<f32>(vec![2, 2]);
    let mut rhs = make_persistent_image::<f32>(vec![2, 2]);
    lhs.put_slice(
        &ArrayD::from_shape_vec(IxDyn(&[2, 2]), vec![10.0, 20.0, 30.0, 40.0]).unwrap(),
        &[0, 0],
    )
    .unwrap();
    rhs.put_slice(
        &ArrayD::from_shape_vec(IxDyn(&[2, 2]), vec![1.0, 2.0, 3.0, 4.0]).unwrap(),
        &[0, 0],
    )
    .unwrap();

    let sub = ImageExprBuilder::from_image(&lhs)
        .unwrap()
        .subtract_image(&rhs)
        .unwrap()
        .get()
        .unwrap();
    let expected = ArrayD::from_shape_vec(IxDyn(&[2, 2]), vec![9.0, 18.0, 27.0, 36.0]).unwrap();
    assert_eq!(sub, expected);

    let div = ImageExprBuilder::from_image(&lhs)
        .unwrap()
        .divide_scalar(10.0)
        .get()
        .unwrap();
    let expected = ArrayD::from_shape_vec(IxDyn(&[2, 2]), vec![1.0, 2.0, 3.0, 4.0]).unwrap();
    assert_eq!(div, expected);
}

#[test]
fn extended_comparison_ops() {
    let mut image = make_persistent_image::<f32>(vec![2, 3]);
    image
        .put_slice(
            &ArrayD::from_shape_vec(IxDyn(&[2, 3]), vec![1.0, 2.0, 3.0, 4.0, 5.0, 3.0]).unwrap(),
            &[0, 0],
        )
        .unwrap();

    // ge
    let got = ImageExprBuilder::from_image(&image)
        .unwrap()
        .ge_scalar(3.0)
        .get()
        .unwrap();
    let expected =
        ArrayD::from_shape_vec(IxDyn(&[2, 3]), vec![false, false, true, true, true, true]).unwrap();
    assert_eq!(got, expected);

    // le
    let got = ImageExprBuilder::from_image(&image)
        .unwrap()
        .le_scalar(3.0)
        .get()
        .unwrap();
    let expected =
        ArrayD::from_shape_vec(IxDyn(&[2, 3]), vec![true, true, true, false, false, true]).unwrap();
    assert_eq!(got, expected);

    // eq
    let got = ImageExprBuilder::from_image(&image)
        .unwrap()
        .eq_scalar(3.0)
        .get()
        .unwrap();
    let expected =
        ArrayD::from_shape_vec(IxDyn(&[2, 3]), vec![false, false, true, false, false, true])
            .unwrap();
    assert_eq!(got, expected);

    // ne
    let got = ImageExprBuilder::from_image(&image)
        .unwrap()
        .ne_scalar(3.0)
        .get()
        .unwrap();
    let expected =
        ArrayD::from_shape_vec(IxDyn(&[2, 3]), vec![true, true, false, true, true, false]).unwrap();
    assert_eq!(got, expected);
}

#[test]
fn chained_transcendental_reuses_owned_snapshot() {
    let data = ArrayD::from_shape_fn(IxDyn(&[4, 4]), |idx| (idx[0] * 4 + idx[1]) as f32 * 0.1);
    let (image, slice_shapes) = CountingImage::new(data.clone());

    // sin(sqrt(x + 1.0))
    let expr = ImageExprBuilder::from_image(&image)
        .unwrap()
        .add_scalar(1.0)
        .sqrt()
        .sin();
    let slice = expr.get_slice(&[1, 1], &[2, 2]).unwrap();

    assert_eq!(slice.shape(), &[2, 2]);
    let recorded = slice_shapes.borrow();
    assert_eq!(recorded.as_slice(), &[vec![4, 4]]);

    // Verify values
    for i in 0..2 {
        for j in 0..2 {
            let src = data[IxDyn(&[i + 1, j + 1])];
            let want = (src + 1.0).sqrt().sin();
            assert!((slice[IxDyn(&[i, j])] - want).abs() < 1e-6);
        }
    }
}

#[test]
fn edge_case_empty_and_degenerate() {
    // Degenerate 1-element image
    let mut image = make_persistent_image::<f32>(vec![1, 1]);
    image.set(4.0).unwrap();

    let got = ImageExprBuilder::from_image(&image)
        .unwrap()
        .sqrt()
        .get()
        .unwrap();
    assert_eq!(got[IxDyn(&[0, 0])], 2.0);

    // Check that sign works on negative values
    let mut neg = make_persistent_image::<f32>(vec![1, 3]);
    neg.put_slice(
        &ArrayD::from_shape_vec(IxDyn(&[1, 3]), vec![-2.0, 0.0, 3.0]).unwrap(),
        &[0, 0],
    )
    .unwrap();
    let got = ImageExprBuilder::from_image(&neg)
        .unwrap()
        .sign()
        .get()
        .unwrap();
    let expected = ArrayD::from_shape_vec(IxDyn(&[1, 3]), vec![-1.0, 0.0, 1.0]).unwrap();
    assert_eq!(got, expected);
}

#[test]
fn min_max_expr_variant() {
    let mut a = make_persistent_image::<f32>(vec![2, 2]);
    let mut b = make_persistent_image::<f32>(vec![2, 2]);
    a.put_slice(
        &ArrayD::from_shape_vec(IxDyn(&[2, 2]), vec![1.0, 5.0, 3.0, 7.0]).unwrap(),
        &[0, 0],
    )
    .unwrap();
    b.put_slice(
        &ArrayD::from_shape_vec(IxDyn(&[2, 2]), vec![4.0, 2.0, 6.0, 0.0]).unwrap(),
        &[0, 0],
    )
    .unwrap();

    let got = ImageExprBuilder::from_image(&a)
        .unwrap()
        .min_expr(ImageExprBuilder::from_image(&b).unwrap())
        .unwrap()
        .get()
        .unwrap();
    let expected = ArrayD::from_shape_vec(IxDyn(&[2, 2]), vec![1.0, 2.0, 3.0, 0.0]).unwrap();
    assert_eq!(got, expected);

    let got = ImageExprBuilder::from_image(&a)
        .unwrap()
        .max_expr(ImageExprBuilder::from_image(&b).unwrap())
        .unwrap()
        .get()
        .unwrap();
    let expected = ArrayD::from_shape_vec(IxDyn(&[2, 2]), vec![4.0, 5.0, 6.0, 7.0]).unwrap();
    assert_eq!(got, expected);
}

#[test]
fn binary_math_image_helpers_cover_remaining_families() {
    let mut lhs = make_persistent_image::<f32>(vec![2, 2]);
    let mut rhs = make_persistent_image::<f32>(vec![2, 2]);
    lhs.put_slice(
        &ArrayD::from_shape_vec(IxDyn(&[2, 2]), vec![3.0, 4.5, 5.0, 7.5]).unwrap(),
        &[0, 0],
    )
    .unwrap();
    rhs.put_slice(
        &ArrayD::from_shape_vec(IxDyn(&[2, 2]), vec![2.0, 2.0, 3.0, 2.5]).unwrap(),
        &[0, 0],
    )
    .unwrap();

    let pow = ImageExprBuilder::from_image(&lhs)
        .unwrap()
        .pow_image(&rhs)
        .unwrap()
        .get()
        .unwrap();
    let fmod = ImageExprBuilder::from_image(&lhs)
        .unwrap()
        .fmod_image(&rhs)
        .unwrap()
        .get()
        .unwrap();
    let atan2 = ImageExprBuilder::from_image(&lhs)
        .unwrap()
        .atan2_image(&rhs)
        .unwrap()
        .get()
        .unwrap();

    let lhs_data = lhs.get().unwrap();
    let rhs_data = rhs.get().unwrap();
    for (((pow_got, fmod_got), atan2_got), (l, r)) in pow
        .iter()
        .zip(fmod.iter())
        .zip(atan2.iter())
        .zip(lhs_data.iter().zip(rhs_data.iter()))
    {
        assert!((*pow_got - l.powf(*r)).abs() < 1e-5);
        assert!((*fmod_got - (*l % *r)).abs() < 1e-5);
        assert!((*atan2_got - l.atan2(*r)).abs() < 1e-5);
    }
}

// ========================================================================
// Wave 14a — ISNAN, NDIM, LENGTH, NELEM
// ========================================================================

#[test]
fn isnan_f32() {
    let data =
        ArrayD::from_shape_vec(IxDyn(&[2, 2]), vec![1.0f32, f32::NAN, 3.0, f32::NAN]).unwrap();
    let (image, _) = CountingImage::new(data);
    let mask = ImageExprBuilder::from_image(&image).unwrap().isnan();
    let result = mask.get().unwrap();
    assert_eq!(
        result,
        ArrayD::from_shape_vec(IxDyn(&[2, 2]), vec![false, true, false, true]).unwrap()
    );
}

#[test]
fn expr_isnan_complex32_trait() {
    // Complex32 lacks PartialOrd so MaskExprBuilder evaluation isn't available.
    // Test the underlying trait method directly.
    assert!(!Complex32::new(1.0, 2.0).expr_isnan());
    assert!(Complex32::new(f32::NAN, 0.0).expr_isnan());
    assert!(Complex32::new(0.0, f32::NAN).expr_isnan());
    assert!(!Complex64::new(1.0, 2.0).expr_isnan());
    assert!(Complex64::new(f64::NAN, 0.0).expr_isnan());
}

#[test]
fn ndim_length_nelem() {
    let data = ArrayD::from_elem(IxDyn(&[4, 5, 6]), 1.0f32);
    let (image, _) = CountingImage::new(data);
    let expr = ImageExprBuilder::from_image(&image).unwrap();
    assert_eq!(expr.ndim_value(), 3);
    assert_eq!(expr.nelem_value(), 120);
    assert_eq!(expr.length_value(0), Some(4));
    assert_eq!(expr.length_value(1), Some(5));
    assert_eq!(expr.length_value(2), Some(6));
}

// ========================================================================
// Wave 14b — Reductions
// ========================================================================

/// Extract the single scalar from a 0-D reduction result.
fn scalar(expr: &ImageExprBuilder<f32>) -> f32 {
    assert!(expr.shape().is_empty(), "expected 0-D reduction");
    expr.get().unwrap()[IxDyn(&[])]
}

#[test]
fn sum_reduce() {
    let data = ArrayD::from_shape_vec(IxDyn(&[2, 2]), vec![1.0f32, 2.0, 3.0, 4.0]).unwrap();
    let (image, _) = CountingImage::new(data);
    let expr = ImageExprBuilder::from_image(&image).unwrap().sum_reduce();
    assert!((scalar(&expr) - 10.0).abs() < 1e-5);
}

#[test]
fn min_max_reduce() {
    let data = ArrayD::from_shape_vec(IxDyn(&[2, 2]), vec![3.0f32, 1.0, 4.0, 2.0]).unwrap();
    let (image, _) = CountingImage::new(data);
    let min_expr = ImageExprBuilder::from_image(&image).unwrap().min_reduce();
    assert!((scalar(&min_expr) - 1.0).abs() < 1e-5);
    let max_expr = ImageExprBuilder::from_image(&image).unwrap().max_reduce();
    assert!((scalar(&max_expr) - 4.0).abs() < 1e-5);
}

#[test]
fn mean_reduce() {
    let data = ArrayD::from_shape_vec(IxDyn(&[2, 2]), vec![1.0f32, 2.0, 3.0, 4.0]).unwrap();
    let (image, _) = CountingImage::new(data);
    let expr = ImageExprBuilder::from_image(&image).unwrap().mean_reduce();
    assert!((scalar(&expr) - 2.5).abs() < 1e-5);
}

#[test]
fn median_reduce() {
    let data = ArrayD::from_shape_vec(IxDyn(&[5]), vec![5.0f32, 1.0, 3.0, 4.0, 2.0]).unwrap();
    let (image, _) = CountingImage::new(data);
    let expr = ImageExprBuilder::from_image(&image)
        .unwrap()
        .median_reduce();
    assert!((scalar(&expr) - 3.0).abs() < 1e-5);
}

#[test]
fn fractile_is_median_at_half() {
    let data = ArrayD::from_shape_vec(IxDyn(&[5]), vec![5.0f32, 1.0, 3.0, 4.0, 2.0]).unwrap();
    let (image, _) = CountingImage::new(data);
    let expr = ImageExprBuilder::from_image(&image).unwrap().fractile(0.5);
    assert!((scalar(&expr) - 3.0).abs() < 1e-5);
}

#[test]
fn fractile_range() {
    let data = ArrayD::from_shape_vec(IxDyn(&[5]), vec![10.0f32, 20.0, 30.0, 40.0, 50.0]).unwrap();
    let (image, _) = CountingImage::new(data);
    let expr = ImageExprBuilder::from_image(&image)
        .unwrap()
        .fractile_range(0.25, 0.75);
    // fractile(0.25) = data[1] = 20, fractile(0.75) = data[3] = 40, range = 20
    assert!((scalar(&expr) - 20.0).abs() < 1e-5);
}

#[test]
fn single_element_reduction() {
    let data = ArrayD::from_shape_vec(IxDyn(&[1]), vec![42.0f32]).unwrap();
    let (image, _) = CountingImage::new(data);
    let check = |expr: ImageExprBuilder<f32>| assert!((scalar(&expr) - 42.0).abs() < 1e-5);
    check(ImageExprBuilder::from_image(&image).unwrap().sum_reduce());
    check(ImageExprBuilder::from_image(&image).unwrap().min_reduce());
    check(ImageExprBuilder::from_image(&image).unwrap().max_reduce());
    check(ImageExprBuilder::from_image(&image).unwrap().mean_reduce());
    check(
        ImageExprBuilder::from_image(&image)
            .unwrap()
            .median_reduce(),
    );
}

#[test]
fn reduction_broadcasts_in_binary() {
    // sum(a) + a should broadcast scalar to [2,2]
    let data = ArrayD::from_shape_vec(IxDyn(&[2, 2]), vec![1.0f32, 2.0, 3.0, 4.0]).unwrap();
    let (image, _) = CountingImage::new(data);
    let sum_expr = ImageExprBuilder::from_image(&image).unwrap().sum_reduce();
    let img_expr = ImageExprBuilder::from_image(&image).unwrap();
    let combined = sum_expr.add_expr(img_expr).unwrap();
    assert_eq!(combined.shape(), &[2, 2]);
    let result = combined.get().unwrap();
    // sum = 10, so result = [11, 12, 13, 14]
    let expected = ArrayD::from_shape_vec(IxDyn(&[2, 2]), vec![11.0f32, 12.0, 13.0, 14.0]).unwrap();
    assert_eq!(result, expected);
}

// ========================================================================
// Wave 14c — Conditional, mask interaction, boolean reductions
// ========================================================================

#[test]
fn iif_conditional() {
    let data = ArrayD::from_shape_vec(IxDyn(&[4]), vec![1.0f32, 5.0, 3.0, 7.0]).unwrap();
    let (image, _) = CountingImage::new(data);
    let mask = ImageExprBuilder::from_image(&image)
        .unwrap()
        .compare_scalar(4.0, ImageExprCompareOp::GreaterThan);
    let t_val = ImageExprBuilder::from_image(&image)
        .unwrap()
        .add_scalar(100.0);
    let f_val = ImageExprBuilder::from_image(&image)
        .unwrap()
        .add_scalar(0.0);
    let result = ImageExprBuilder::iif(mask, t_val, f_val)
        .unwrap()
        .get()
        .unwrap();
    // [1>4?=F, 5>4?=T, 3>4?=F, 7>4?=T] → [1, 105, 3, 107]
    let expected = ArrayD::from_shape_vec(IxDyn(&[4]), vec![1.0f32, 105.0, 3.0, 107.0]).unwrap();
    assert_eq!(result, expected);
}

#[test]
fn all_any_reduce() {
    let data = ArrayD::from_shape_vec(IxDyn(&[4]), vec![1.0f32, 2.0, 3.0, 4.0]).unwrap();
    let (image, _) = CountingImage::new(data);

    let mask_scalar = |m: MaskExprBuilder<f32>| -> bool {
        assert!(m.shape().is_empty(), "expected 0-D mask");
        m.get().unwrap()[IxDyn(&[])]
    };

    let all_gt_0 = ImageExprBuilder::from_image(&image)
        .unwrap()
        .compare_scalar(0.0, ImageExprCompareOp::GreaterThan)
        .all_reduce();
    assert!(mask_scalar(all_gt_0));

    let all_gt_2 = ImageExprBuilder::from_image(&image)
        .unwrap()
        .compare_scalar(2.0, ImageExprCompareOp::GreaterThan)
        .all_reduce();
    assert!(!mask_scalar(all_gt_2));

    let any_gt_3 = ImageExprBuilder::from_image(&image)
        .unwrap()
        .compare_scalar(3.0, ImageExprCompareOp::GreaterThan)
        .any_reduce();
    assert!(mask_scalar(any_gt_3));

    let any_gt_10 = ImageExprBuilder::from_image(&image)
        .unwrap()
        .compare_scalar(10.0, ImageExprCompareOp::GreaterThan)
        .any_reduce();
    assert!(!mask_scalar(any_gt_10));
}

#[test]
fn ntrue_nfalse() {
    let data = ArrayD::from_shape_vec(IxDyn(&[4]), vec![1.0f32, 5.0, 3.0, 7.0]).unwrap();
    let (image, _) = CountingImage::new(data);
    let mask_t = ImageExprBuilder::from_image(&image)
        .unwrap()
        .compare_scalar(4.0, ImageExprCompareOp::GreaterThan);
    let mask_f = ImageExprBuilder::from_image(&image)
        .unwrap()
        .compare_scalar(4.0, ImageExprCompareOp::GreaterThan);
    let ntrue_expr = ImageExprBuilder::<f32>::ntrue(mask_t);
    let nfalse_expr = ImageExprBuilder::<f32>::nfalse(mask_f);
    assert!((scalar(&ntrue_expr) - 2.0).abs() < 1e-5);
    assert!((scalar(&nfalse_expr) - 2.0).abs() < 1e-5);
}

#[test]
fn constant_mask() {
    let mask_data = ArrayD::from_shape_vec(IxDyn(&[4]), vec![true, false, true, false]).unwrap();
    let mask = MaskExprBuilder::<f32>::from_constant(mask_data.clone());
    let result = mask.get().unwrap();
    assert_eq!(result, mask_data);
}

#[test]
fn replace_masked_pixels() {
    let primary_data = ArrayD::from_shape_vec(IxDyn(&[4]), vec![1.0f32, 2.0, 3.0, 4.0]).unwrap();
    let (primary_img, _) = CountingImage::new(primary_data);
    let replacement_data = ArrayD::from_elem(IxDyn(&[4]), 0.0f32);
    let (replacement_img, _) = CountingImage::new(replacement_data);
    let mask = ArrayD::from_shape_vec(IxDyn(&[4]), vec![true, false, true, false]).unwrap();
    let result = ImageExprBuilder::from_image(&primary_img)
        .unwrap()
        .replace(
            ImageExprBuilder::from_image(&replacement_img).unwrap(),
            mask,
        )
        .unwrap()
        .get()
        .unwrap();
    // mask=true keeps primary, mask=false uses replacement
    let expected = ArrayD::from_shape_vec(IxDyn(&[4]), vec![1.0f32, 0.0, 3.0, 0.0]).unwrap();
    assert_eq!(result, expected);
}

mod pixel_conversions;

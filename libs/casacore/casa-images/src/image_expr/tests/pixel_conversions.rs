// SPDX-License-Identifier: LGPL-3.0-or-later

use super::*;

#[test]
fn complex32_real_part() {
    let data = ArrayD::from_shape_vec(
        IxDyn(&[2]),
        vec![Complex32::new(3.0, 4.0), Complex32::new(1.0, 2.0)],
    )
    .unwrap();
    let (image, _) = CountingImage::new(data);
    let result = ImageExprBuilder::from_image(&image)
        .unwrap()
        .real_part()
        .get()
        .unwrap();
    let expected = ArrayD::from_shape_vec(IxDyn(&[2]), vec![3.0f32, 1.0]).unwrap();
    assert_eq!(result, expected);
}

#[test]
fn complex32_imag_part() {
    let data = ArrayD::from_shape_vec(
        IxDyn(&[2]),
        vec![Complex32::new(3.0, 4.0), Complex32::new(1.0, 2.0)],
    )
    .unwrap();
    let (image, _) = CountingImage::new(data);
    let result = ImageExprBuilder::from_image(&image)
        .unwrap()
        .imag_part()
        .get()
        .unwrap();
    let expected = ArrayD::from_shape_vec(IxDyn(&[2]), vec![4.0f32, 2.0]).unwrap();
    assert_eq!(result, expected);
}

#[test]
fn complex32_arg_phase() {
    let data = ArrayD::from_shape_vec(
        IxDyn(&[2]),
        vec![Complex32::new(1.0, 1.0), Complex32::new(0.0, 1.0)],
    )
    .unwrap();
    let (image, _) = CountingImage::new(data);
    let result = ImageExprBuilder::from_image(&image)
        .unwrap()
        .arg_phase()
        .get()
        .unwrap();
    assert!((result[[0]] - std::f32::consts::FRAC_PI_4).abs() < 1e-5);
    assert!((result[[1]] - std::f32::consts::FRAC_PI_2).abs() < 1e-5);
}

#[test]
fn complex64_real_imag_arg() {
    let data = ArrayD::from_shape_vec(IxDyn(&[1]), vec![Complex64::new(3.0, 4.0)]).unwrap();
    let (image, _) = CountingImage::new(data);
    let re = ImageExprBuilder::from_image(&image)
        .unwrap()
        .real_part()
        .get_at(&[0])
        .unwrap();
    let im = ImageExprBuilder::from_image(&image)
        .unwrap()
        .imag_part()
        .get_at(&[0])
        .unwrap();
    let arg = ImageExprBuilder::from_image(&image)
        .unwrap()
        .arg_phase()
        .get_at(&[0])
        .unwrap();
    assert!((re - 3.0).abs() < 1e-10);
    assert!((im - 4.0).abs() < 1e-10);
    assert!((arg - 4.0f64.atan2(3.0)).abs() < 1e-10);
}

#[test]
fn f32_to_complex() {
    let re_data = ArrayD::from_shape_vec(IxDyn(&[2]), vec![1.0f32, 3.0]).unwrap();
    let im_data = ArrayD::from_shape_vec(IxDyn(&[2]), vec![2.0f32, 4.0]).unwrap();
    let (re_img, _) = CountingImage::new(re_data);
    let (im_img, _) = CountingImage::new(im_data);
    let result = ImageExprBuilder::from_image(&re_img)
        .unwrap()
        .to_complex(ImageExprBuilder::from_image(&im_img).unwrap())
        .unwrap()
        .get()
        .unwrap();
    assert_eq!(result[[0]], Complex32::new(1.0, 2.0));
    assert_eq!(result[[1]], Complex32::new(3.0, 4.0));
}

#[test]
fn f64_to_complex() {
    let re_data = ArrayD::from_shape_vec(IxDyn(&[2]), vec![1.0f64, 3.0]).unwrap();
    let im_data = ArrayD::from_shape_vec(IxDyn(&[2]), vec![2.0f64, 4.0]).unwrap();
    let (re_img, _) = CountingImage::new(re_data);
    let (im_img, _) = CountingImage::new(im_data);
    let result = ImageExprBuilder::from_image(&re_img)
        .unwrap()
        .to_complex(ImageExprBuilder::from_image(&im_img).unwrap())
        .unwrap()
        .get()
        .unwrap();
    assert_eq!(result[[0]], Complex64::new(1.0, 2.0));
    assert_eq!(result[[1]], Complex64::new(3.0, 4.0));
}

#[test]
fn direct_trait_ops_cover_remaining_scalar_impls() {
    let value = 1.25f64;
    assert!((<f64 as ImageExprValue>::expr_exp(value) - value.exp()).abs() < 1e-14);
    assert!((<f64 as ImageExprValue>::expr_sin(value) - value.sin()).abs() < 1e-14);
    assert!((<f64 as ImageExprValue>::expr_cos(value) - value.cos()).abs() < 1e-14);
    assert!((<f64 as ImageExprValue>::expr_tan(value) - value.tan()).abs() < 1e-14);
    assert!((<f64 as ImageExprValue>::expr_asin(0.5) - 0.5f64.asin()).abs() < 1e-14);
    assert!((<f64 as ImageExprValue>::expr_acos(0.5) - 0.5f64.acos()).abs() < 1e-14);
    assert!((<f64 as ImageExprValue>::expr_atan(value) - value.atan()).abs() < 1e-14);
    assert!((<f64 as ImageExprValue>::expr_sinh(value) - value.sinh()).abs() < 1e-14);
    assert!((<f64 as ImageExprValue>::expr_cosh(value) - value.cosh()).abs() < 1e-14);
    assert!((<f64 as ImageExprValue>::expr_tanh(value) - value.tanh()).abs() < 1e-14);
    assert!((<f64 as ImageExprValue>::expr_log(value) - value.ln()).abs() < 1e-14);
    assert!((<f64 as ImageExprValue>::expr_log10(value) - value.log10()).abs() < 1e-14);
    assert!((<f64 as ImageExprValue>::expr_sqrt(value) - value.sqrt()).abs() < 1e-14);
    assert_eq!(<f64 as ImageExprValue>::expr_abs(-value), value);
    assert_eq!(<f64 as ImageExprValue>::expr_conj(value), value);
    assert_eq!(<f64 as ImageExprValue>::expr_ceil(1.2), 2.0);
    assert_eq!(<f64 as ImageExprValue>::expr_floor(1.8), 1.0);
    assert_eq!(<f64 as ImageExprValue>::expr_round(1.5), 2.0);
    assert_eq!(<f64 as ImageExprValue>::expr_sign(-2.0), -1.0);
    assert_eq!(<f64 as ImageExprValue>::expr_sign(0.0), 0.0);
    assert!((<f64 as ImageExprValue>::expr_pow(9.0, 0.5) - 3.0).abs() < 1e-14);
    assert_eq!(<f64 as ImageExprValue>::expr_fmod(7.5, 2.0), 1.5);
    assert!((<f64 as ImageExprValue>::expr_atan2(3.0, 2.0) - 3.0f64.atan2(2.0)).abs() < 1e-14);
    assert_eq!(<f64 as ImageExprValue>::expr_min(1.0, 2.0), 1.0);
    assert_eq!(<f64 as ImageExprValue>::expr_max(1.0, 2.0), 2.0);
    assert!(<f64 as ImageExprValue>::expr_isnan(&f64::NAN));
    assert_eq!(<f64 as ImageExprValue>::from_f64_lossy(3.5), 3.5);
    assert_eq!(<f64 as ImageExprValue>::to_f64_lossy(&3.5), 3.5);
    assert!(<f64 as ImageExprValue>::expr_compare(
        3.0,
        2.0,
        ImageExprCompareOp::GreaterThan
    ));
    assert!(<f64 as ImageExprValue>::expr_compare(
        2.0,
        2.0,
        ImageExprCompareOp::GreaterEqual
    ));
    assert!(<f64 as ImageExprValue>::expr_compare(
        1.0,
        2.0,
        ImageExprCompareOp::LessThan
    ));
    assert!(<f64 as ImageExprValue>::expr_compare(
        1.0,
        2.0,
        ImageExprCompareOp::LessEqual
    ));
    assert!(<f64 as ImageExprValue>::expr_compare(
        2.0,
        2.0,
        ImageExprCompareOp::Equal
    ));
    assert!(<f64 as ImageExprValue>::expr_compare(
        1.0,
        2.0,
        ImageExprCompareOp::NotEqual
    ));
}

#[test]
fn direct_trait_ops_cover_remaining_complex32_impls() {
    let c32 = Complex32::new(1.25, -0.75);
    assert!((<Complex32 as ImageExprValue>::expr_exp(c32) - c32.exp()).norm() < 1e-5);
    assert!((<Complex32 as ImageExprValue>::expr_sin(c32) - c32.sin()).norm() < 1e-5);
    assert!((<Complex32 as ImageExprValue>::expr_cos(c32) - c32.cos()).norm() < 1e-5);
    assert!((<Complex32 as ImageExprValue>::expr_tan(c32) - c32.tan()).norm() < 1e-5);
    assert!((<Complex32 as ImageExprValue>::expr_asin(c32) - c32.asin()).norm() < 1e-5);
    assert!((<Complex32 as ImageExprValue>::expr_acos(c32) - c32.acos()).norm() < 1e-5);
    assert!((<Complex32 as ImageExprValue>::expr_atan(c32) - c32.atan()).norm() < 1e-5);
    assert!((<Complex32 as ImageExprValue>::expr_sinh(c32) - c32.sinh()).norm() < 1e-5);
    assert!((<Complex32 as ImageExprValue>::expr_cosh(c32) - c32.cosh()).norm() < 1e-5);
    assert!((<Complex32 as ImageExprValue>::expr_tanh(c32) - c32.tanh()).norm() < 1e-5);
    assert!((<Complex32 as ImageExprValue>::expr_log(c32) - c32.ln()).norm() < 1e-5);
    assert!(
        (<Complex32 as ImageExprValue>::expr_log10(c32)
            - (c32.ln() / Complex32::new(std::f32::consts::LN_10, 0.0)))
        .norm()
            < 1e-5
    );
    assert!((<Complex32 as ImageExprValue>::expr_sqrt(c32) - c32.sqrt()).norm() < 1e-5);
    assert_eq!(
        <Complex32 as ImageExprValue>::expr_abs(c32),
        Complex32::new(c32.norm(), 0.0)
    );
    assert_eq!(<Complex32 as ImageExprValue>::expr_conj(c32), c32.conj());
    assert_eq!(
        <Complex32 as ImageExprValue>::expr_ceil(c32),
        Complex32::new(c32.re.ceil(), c32.im.ceil())
    );
    assert_eq!(
        <Complex32 as ImageExprValue>::expr_floor(c32),
        Complex32::new(c32.re.floor(), c32.im.floor())
    );
    assert_eq!(
        <Complex32 as ImageExprValue>::expr_round(c32),
        Complex32::new(c32.re.round(), c32.im.round())
    );
    let sign32 = <Complex32 as ImageExprValue>::expr_sign(c32);
    assert!((sign32.norm() - 1.0).abs() < 1e-5);
    assert_eq!(
        <Complex32 as ImageExprValue>::expr_sign(Complex32::new(0.0, 0.0)),
        Complex32::new(0.0, 0.0)
    );
    assert!(
        (<Complex32 as ImageExprValue>::expr_pow(c32, Complex32::new(0.5, 0.0))
            - c32.powc(Complex32::new(0.5, 0.0)))
        .norm()
            < 1e-5
    );
    assert_eq!(
        <Complex32 as ImageExprValue>::expr_fmod(
            Complex32::new(5.5, 7.5),
            Complex32::new(2.0, 3.0)
        ),
        Complex32::new(1.5, 1.5)
    );
    assert_eq!(
        <Complex32 as ImageExprValue>::expr_atan2(
            Complex32::new(3.0, 4.0),
            Complex32::new(2.0, 5.0)
        ),
        Complex32::new(3.0f32.atan2(2.0), 4.0f32.atan2(5.0))
    );
    assert_eq!(
        <Complex32 as ImageExprValue>::expr_min(Complex32::new(1.0, 0.0), Complex32::new(2.0, 0.0)),
        Complex32::new(1.0, 0.0)
    );
    assert_eq!(
        <Complex32 as ImageExprValue>::expr_max(Complex32::new(1.0, 0.0), Complex32::new(2.0, 0.0)),
        Complex32::new(2.0, 0.0)
    );
    assert!(<Complex32 as ImageExprValue>::expr_isnan(&Complex32::new(
        f32::NAN,
        0.0
    )));
    assert_eq!(
        <Complex32 as ImageExprValue>::from_f64_lossy(2.0),
        Complex32::new(2.0, 0.0)
    );
    assert!(
        (<Complex32 as ImageExprValue>::to_f64_lossy(&Complex32::new(3.0, 4.0)) - 5.0).abs() < 1e-6
    );
    assert!(<Complex32 as ImageExprValue>::expr_compare(
        Complex32::new(3.0, 0.0),
        Complex32::new(2.0, 0.0),
        ImageExprCompareOp::GreaterThan
    ));
    assert!(<Complex32 as ImageExprValue>::expr_compare(
        Complex32::new(2.0, 0.0),
        Complex32::new(2.0, 0.0),
        ImageExprCompareOp::Equal
    ));
    assert!(<Complex32 as ImageExprValue>::expr_compare(
        Complex32::new(1.0, 0.0),
        Complex32::new(2.0, 0.0),
        ImageExprCompareOp::NotEqual
    ));
}

#[test]
fn direct_trait_ops_cover_remaining_complex64_impls() {
    let c64 = Complex64::new(-1.5, 0.25);
    assert!((<Complex64 as ImageExprValue>::expr_exp(c64) - c64.exp()).norm() < 1e-12);
    assert!((<Complex64 as ImageExprValue>::expr_sin(c64) - c64.sin()).norm() < 1e-12);
    assert!((<Complex64 as ImageExprValue>::expr_cos(c64) - c64.cos()).norm() < 1e-12);
    assert!((<Complex64 as ImageExprValue>::expr_tan(c64) - c64.tan()).norm() < 1e-12);
    assert!((<Complex64 as ImageExprValue>::expr_asin(c64) - c64.asin()).norm() < 1e-12);
    assert!((<Complex64 as ImageExprValue>::expr_acos(c64) - c64.acos()).norm() < 1e-12);
    assert!((<Complex64 as ImageExprValue>::expr_atan(c64) - c64.atan()).norm() < 1e-12);
    assert!((<Complex64 as ImageExprValue>::expr_sinh(c64) - c64.sinh()).norm() < 1e-12);
    assert!((<Complex64 as ImageExprValue>::expr_cosh(c64) - c64.cosh()).norm() < 1e-12);
    assert!((<Complex64 as ImageExprValue>::expr_tanh(c64) - c64.tanh()).norm() < 1e-12);
    assert!((<Complex64 as ImageExprValue>::expr_log(c64) - c64.ln()).norm() < 1e-12);
    assert!(
        (<Complex64 as ImageExprValue>::expr_log10(c64)
            - (c64.ln() / Complex64::new(std::f64::consts::LN_10, 0.0)))
        .norm()
            < 1e-12
    );
    assert!((<Complex64 as ImageExprValue>::expr_sqrt(c64) - c64.sqrt()).norm() < 1e-12);
    assert_eq!(
        <Complex64 as ImageExprValue>::expr_abs(c64),
        Complex64::new(c64.norm(), 0.0)
    );
    assert_eq!(<Complex64 as ImageExprValue>::expr_conj(c64), c64.conj());
    assert_eq!(
        <Complex64 as ImageExprValue>::expr_ceil(c64),
        Complex64::new(c64.re.ceil(), c64.im.ceil())
    );
    assert_eq!(
        <Complex64 as ImageExprValue>::expr_floor(c64),
        Complex64::new(c64.re.floor(), c64.im.floor())
    );
    assert_eq!(
        <Complex64 as ImageExprValue>::expr_round(c64),
        Complex64::new(c64.re.round(), c64.im.round())
    );
    let sign64 = <Complex64 as ImageExprValue>::expr_sign(c64);
    assert!((sign64.norm() - 1.0).abs() < 1e-12);
    assert_eq!(
        <Complex64 as ImageExprValue>::expr_sign(Complex64::new(0.0, 0.0)),
        Complex64::new(0.0, 0.0)
    );
    assert!(
        (<Complex64 as ImageExprValue>::expr_pow(c64, Complex64::new(2.0, 0.0))
            - c64.powc(Complex64::new(2.0, 0.0)))
        .norm()
            < 1e-12
    );
    assert_eq!(
        <Complex64 as ImageExprValue>::expr_fmod(
            Complex64::new(5.5, 7.5),
            Complex64::new(2.0, 3.0)
        ),
        Complex64::new(1.5, 1.5)
    );
    assert_eq!(
        <Complex64 as ImageExprValue>::expr_atan2(
            Complex64::new(3.0, 4.0),
            Complex64::new(2.0, 5.0)
        ),
        Complex64::new(3.0f64.atan2(2.0), 4.0f64.atan2(5.0))
    );
    assert_eq!(
        <Complex64 as ImageExprValue>::expr_min(Complex64::new(1.0, 0.0), Complex64::new(2.0, 0.0)),
        Complex64::new(1.0, 0.0)
    );
    assert_eq!(
        <Complex64 as ImageExprValue>::expr_max(Complex64::new(1.0, 0.0), Complex64::new(2.0, 0.0)),
        Complex64::new(2.0, 0.0)
    );
    assert!(<Complex64 as ImageExprValue>::expr_isnan(&Complex64::new(
        0.0,
        f64::NAN
    )));
    assert_eq!(
        <Complex64 as ImageExprValue>::from_f64_lossy(2.0),
        Complex64::new(2.0, 0.0)
    );
    assert!(
        (<Complex64 as ImageExprValue>::to_f64_lossy(&Complex64::new(3.0, 4.0)) - 5.0).abs()
            < 1e-12
    );
    assert!(<Complex64 as ImageExprValue>::expr_compare(
        Complex64::new(3.0, 0.0),
        Complex64::new(2.0, 0.0),
        ImageExprCompareOp::GreaterEqual
    ));
    assert!(<Complex64 as ImageExprValue>::expr_compare(
        Complex64::new(1.0, 0.0),
        Complex64::new(2.0, 0.0),
        ImageExprCompareOp::LessEqual
    ));
}

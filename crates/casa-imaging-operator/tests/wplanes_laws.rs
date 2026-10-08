// SPDX-License-Identifier: LGPL-3.0-or-later
//! T0 laws of the W-projection kernel set: CASA's plane index, the
//! zero-|w| plane against the standard spheroidal taps, w-sign conjugation
//! pairing, the exact adjoint without a norm division, the `W · Re N`
//! `sumwt` rule and the support crop.

mod common;

use casa_imaging_model::{CorrelationType, PolarizationCoordinate};
use casa_imaging_operator::{
    Basis, CellHold, CfKey, ConvolutionFunctionSet, CpuBackend, GridBackend, GridPrecision,
    KernelNormalisation, MeasurementOperator, Mode, ModeSet, ModelImages, ModelPlane,
    ModelPrescale, Placement, PlaneRange, PolarizationRouting, RowContext, SampleBuffer,
    Spheroidal, TapLayout, WPlaneCount, WPlanes, Work,
};
use casa_imaging_operator::{GridGeometry, GridPadding, ImageExtent};
use common::{IMAGE, Rng, buffer, placements, samples};
use ndarray::Array2;
use num_complex::{Complex32, Complex64};

const XX_YY: [CorrelationType; 2] = [CorrelationType::LinearXx, CorrelationType::LinearYy];
const STOKES_I: [PolarizationCoordinate; 1] = [PolarizationCoordinate::StokesI];
/// A wide field (3.4 arcmin cells, 3.7° across 64 pixels) so the w-term
/// phases reach radians within the planes' `0.25/|Δx|` range.
const WIDE_INCREMENT_RAD: f64 = 1.0e-3;

fn context() -> RowContext {
    RowContext {
        time_s: 0.0,
        antennas: [0, 1],
        antenna_types: [0, 0],
        parallactic_angle_rad: [0.0; 2],
        field: 0,
        spectral_window: 0,
    }
}

fn geometry() -> GridGeometry {
    GridGeometry::new(
        ImageExtent {
            shape: [IMAGE, IMAGE],
            increment_rad: [-WIDE_INCREMENT_RAD, WIDE_INCREMENT_RAD],
            reference_pixel: [IMAGE / 2, IMAGE / 2],
        },
        GridPadding::CasaComposite,
    )
    .expect("wide geometry")
}

fn w_planes(planes: u32) -> WPlanes {
    let geometry = geometry();
    let polarization = PolarizationRouting::compile(&XX_YY, &STOKES_I).expect("routing");
    WPlanes::new(&geometry, &polarization, WPlaneCount::Fixed(planes)).expect("planes")
}

fn operator_with(
    cf: Box<dyn ConvolutionFunctionSet>,
    precision: GridPrecision,
) -> MeasurementOperator {
    let polarization = PolarizationRouting::compile(&XX_YY, &STOKES_I).expect("routing");
    MeasurementOperator::new(geometry(), Basis::Constant, polarization, cf, precision)
}

/// Whether `placement`'s support fits the grid.
fn fits(operator: &MeasurementOperator, placement: &Placement) -> bool {
    let mut hold = CellHold::new();
    let taps = operator.cf().taps(placement.cf, &mut hold);
    let location = operator
        .geometry()
        .locate(placement.u, placement.v, taps.oversampling());
    operator.geometry().fits(location, taps.half_support())
}

/// Placements keyed by `cf` from random baselines with `w` in `±max_w`.
fn keyed_placements(
    operator: &MeasurementOperator,
    count: usize,
    max_w: f64,
    rng: &mut Rng,
) -> Vec<Placement> {
    let mut placed = placements(operator.geometry(), count, 1, rng);
    let cf = operator.cf();
    for placement in &mut placed {
        placement.w = rng.signed() * max_w;
        placement.cf = cf.key(&context(), 1.0e9, placement.w);
        if !fits(operator, placement) {
            // Pull the sample toward the grid centre so its support fits.
            placement.u *= 0.5;
            placement.v *= 0.5;
        }
    }
    placed.retain(|placement| fits(operator, placement));
    placed
}

/// The Hermitian partner of `placement`: the mirrored baseline with the
/// opposite `w`, whose plane is the same (`|w|`) and whose data would be
/// conjugated.
fn mirror(operator: &MeasurementOperator, placement: &Placement) -> Placement {
    Placement {
        u: -placement.u,
        v: -placement.v,
        w: -placement.w,
        phase: -placement.phase,
        cf: operator.cf().key(&context(), 1.0e9, -placement.w),
        ..*placement
    }
}

fn dirty(operator: &MeasurementOperator, block: &SampleBuffer) -> (Array2<f32>, f64) {
    let mut acc = operator.accumulator(PlaneRange::single(0), None, ModeSet::DATA);
    CpuBackend::new()
        .apply(
            &block.block(),
            operator.cf(),
            Work::Grid {
                mode: Mode::Data,
                acc: &mut acc,
            },
        )
        .expect("grid");
    let sumwt = acc.sumwt_at(0, 0, 0);
    let normal = operator.finish(acc).expect("finish");
    (normal.data(0, 0, 0).clone(), sumwt)
}

#[test]
fn plane_index_is_casa_nint_of_the_root_clamped_to_the_planes() {
    let planes = w_planes(8);
    // wScale = (nW−1)² / (0.25/|Δx|).
    let expected_scale = 49.0 / (0.25 / WIDE_INCREMENT_RAD);
    assert!((planes.w_scale() - expected_scale).abs() < 1.0e-9 * expected_scale);
    let w_for = |root: f64| root * root / planes.w_scale();
    for (root, plane) in [
        (0.0, 0),
        (0.49, 0),
        (0.51, 1),
        (1.4, 1),
        (1.6, 2),
        (6.6, 7),
        (7.4, 7),
        (40.0, 7),
    ] {
        assert_eq!(planes.plane_of(w_for(root)), plane, "root {root}");
        assert_eq!(planes.plane_of(-w_for(root)), plane, "root −{root}");
        let key = planes.key(&context(), 1.0e9, w_for(root));
        assert_eq!(
            key,
            CfKey {
                group: plane as u16,
                cube: 0
            }
        );
    }
    assert_eq!(planes.normalisation(), KernelNormalisation::RealSum);
    assert!(!planes.pointing_ramp());
}

#[test]
fn the_zero_w_plane_is_the_spheroidal_kernel() {
    // Plane 0 is `FT[grdsf taper]` sampled four times per cell; the standard
    // set tabulates the analytic `(1 − ν²)·grdsf` at a hundred. At the
    // fine offsets both hold, the taps agree to the FFT's truncation of the
    // taper on the padded field of view.
    let geometry = geometry();
    let polarization = PolarizationRouting::compile(&XX_YY, &STOKES_I).expect("routing");
    let spheroidal = Spheroidal::new(&geometry, &polarization);
    let planes = w_planes(8);
    let mut hold = CellHold::new();
    let TapLayout::SeparableReal {
        rows,
        support: 7,
        oversampling: 100,
    } = spheroidal.taps(CfKey::default(), &mut hold)
    else {
        panic!("standard taps");
    };
    let mut w_hold = CellHold::new();
    let TapLayout::Dense {
        data,
        support,
        oversampling: 4,
        mueller_planes: 1,
    } = planes.taps(CfKey::default(), &mut w_hold)
    else {
        panic!("dense plane");
    };
    let [sx, sy] = [usize::from(support[0]), usize::from(support[1])];
    let half = sx / 2;
    let peak = f64::from(rows[50 * 7 + 3]).powi(2);
    let mut worst = 0.0_f64;
    let mut worst_imaginary = 0.0_f64;
    for oy in 0..5 {
        for ox in 0..5 {
            let tile = &data[(oy * 5 + ox) * sx * sy..][..sx * sy];
            let row_x = &rows[(50 + (ox as i64 - 2) * 25) as usize * 7..][..7];
            let row_y = &rows[(50 + (oy as i64 - 2) * 25) as usize * 7..][..7];
            for iy in 0..sy {
                for ix in 0..sx {
                    let (kx, ky) = (ix as i64 - half as i64, iy as i64 - half as i64);
                    let expected = if kx.abs() <= 3 && ky.abs() <= 3 {
                        f64::from(row_x[(kx + 3) as usize]) * f64::from(row_y[(ky + 3) as usize])
                    } else {
                        0.0
                    };
                    let actual = tile[iy * sx + ix];
                    worst_imaginary = worst_imaginary.max(f64::from(actual.im.abs()));
                    worst = worst.max((f64::from(actual.re) - expected).abs());
                }
            }
        }
    }
    // `makeGWplane` fills `−inner/2 ≤ i < inner/2`, one row short of
    // symmetric, so plane 0 carries a small imaginary part in CASA too.
    assert!(
        worst_imaginary < 1.0e-3 * peak,
        "imaginary part {worst_imaginary} of peak {peak}"
    );
    // The FFT of the truncated taper differs from the analytic prolate
    // function at the 1e-3 level of the peak tap.
    assert!(
        worst < 1.0e-2 * peak,
        "worst tap difference {worst} of peak {peak}"
    );
}

#[test]
fn a_real_sky_predicts_conjugate_visibilities_at_hermitian_baselines() {
    let mut rng = Rng::new(61);
    let operator = operator_with(Box::new(w_planes(16)), GridPrecision::F64);
    let images = vec![Array2::from_shape_fn((IMAGE, IMAGE), |_| {
        rng.signed() as f32
    })];
    let model = ModelImages {
        first_plane: 0,
        planes: vec![ModelPlane { images }],
    };
    let prepared = operator
        .prepare_model(&model, ModelPrescale::Unit)
        .expect("prepared");
    // |w| up to the plane range so most samples land on complex planes;
    // the grid's valid anchors are one cell asymmetric, so keep the pairs
    // whose mirror fits too.
    let max_w = 0.9 * 0.25 / WIDE_INCREMENT_RAD;
    let placed = keyed_placements(&operator, 60, max_w, &mut rng)
        .into_iter()
        .filter(|placement| fits(&operator, &mirror(&operator, placement)))
        .collect::<Vec<_>>();
    assert!(placed.iter().any(|placement| placement.cf.group > 8));
    let mut forward = SampleBuffer::new(2);
    let mut mirrored = SampleBuffer::new(2);
    for placement in &placed {
        forward.push(*placement, &[Complex32::default(); 2], &[1.0; 2]);
        mirrored.push(
            mirror(&operator, placement),
            &[Complex32::default(); 2],
            &[1.0; 2],
        );
    }
    let predict = |block: &SampleBuffer| {
        let mut out = vec![Complex32::default(); block.len() * 2];
        CpuBackend::new()
            .apply(
                &block.block(),
                operator.cf(),
                Work::Predict {
                    model: &prepared,
                    out: &mut out,
                },
            )
            .expect("predict");
        out
    };
    let (direct, mirror) = (predict(&forward), predict(&mirrored));
    let scale = direct
        .iter()
        .map(|value| f64::from(value.norm()))
        .fold(0.0, f64::max);
    for (a, b) in direct.iter().zip(&mirror) {
        let difference = (Complex64::new(f64::from(a.re), f64::from(a.im))
            - Complex64::new(f64::from(b.re), -f64::from(b.im)))
        .norm();
        assert!(
            difference <= 1.0e-5 * scale,
            "{a} vs conj({b}), scale {scale}"
        );
    }
}

#[test]
fn w_planes_are_exactly_adjoint_without_a_norm_division() {
    let mut rng = Rng::new(67);
    let operator = operator_with(Box::new(w_planes(16)), GridPrecision::F64);
    let max_w = 0.9 * 0.25 / WIDE_INCREMENT_RAD;
    let placed = keyed_placements(&operator, 150, max_w, &mut rng);
    let (values, weights) = samples(&placed, 2, &mut rng);
    let block = buffer(&placed, &values, &weights, 2);
    let images = vec![Array2::from_shape_fn((IMAGE, IMAGE), |_| {
        rng.signed() as f32
    })];
    let model = ModelImages {
        first_plane: 0,
        planes: vec![ModelPlane { images }],
    };
    let prepared = operator
        .prepare_model(&model, ModelPrescale::Unit)
        .expect("prepared");
    let mut predicted = vec![Complex32::default(); placed.len() * 2];
    CpuBackend::new()
        .apply(
            &block.block(),
            operator.cf(),
            Work::Predict {
                model: &prepared,
                out: &mut predicted,
            },
        )
        .expect("predict");
    let visibility_side = predicted
        .iter()
        .zip(&values)
        .zip(&weights)
        .map(|((ax, d), w)| (Complex64::new(f64::from(ax.re), f64::from(ax.im)).conj() * d * w).re)
        .sum::<f64>();
    let (image, _) = dirty(&operator, &block);
    // `WProjectFT` multiplies the model by its sinc and divides the image
    // by it, so the pairing holds with the model carrying the ratio of the
    // two sides (as `MosaicFT`).
    let correction = operator.cf().image_correction();
    // The tables span the padded grid; the image sits at its origin.
    let offset = operator.geometry().image_origin();
    let image_side = image
        .indexed_iter()
        .map(|((y, x), image)| {
            let (gx, gy) = (x + offset[0], y + offset[1]);
            let factor = correction.model_at(gx, gy) / correction.at(gx, gy);
            f64::from(*image) * f64::from(model.planes[0].images[0][(y, x)]) * factor
        })
        .sum::<f64>();
    let scale = visibility_side.abs().max(image_side.abs());
    assert!(
        (visibility_side - image_side).abs() <= 1.0e-5 * scale,
        "{visibility_side} vs {image_side}"
    );
}

#[test]
fn sumwt_accumulates_the_weight_times_the_real_kernel_sum() {
    let mut rng = Rng::new(71);
    let operator = operator_with(Box::new(w_planes(16)), GridPrecision::F64);
    let max_w = 0.9 * 0.25 / WIDE_INCREMENT_RAD;
    let placed = keyed_placements(&operator, 40, max_w, &mut rng);
    let (values, weights) = samples(&placed, 2, &mut rng);
    let block = buffer(&placed, &values, &weights, 2);
    let (_, sumwt) = dirty(&operator, &block);
    // The block carries single-precision weights.
    let expected = placed
        .iter()
        .enumerate()
        .map(|(index, placement)| {
            (0..2)
                .map(|vpol| {
                    f64::from(weights[index * 2 + vpol] as f32)
                        * operator.prediction_norm(placement, vpol).re
                })
                .sum::<f64>()
        })
        .sum::<f64>();
    assert!(
        (sumwt - expected).abs() <= 1.0e-9 * expected.abs(),
        "{sumwt} vs {expected}"
    );
    assert!(
        (sumwt - weights.iter().sum::<f64>()).abs() > 1.0e-6,
        "complex planes make Re N differ from one"
    );
}

/// The predictions of `model` for every sample of `block`, `[sample][pol]`.
fn predict(
    operator: &MeasurementOperator,
    model: &ModelImages,
    block: &SampleBuffer,
) -> Vec<Complex32> {
    let prepared = operator
        .prepare_model(model, ModelPrescale::Unit)
        .expect("prepared");
    let mut out = vec![Complex32::default(); block.len() * 2];
    CpuBackend::new()
        .apply(
            &block.block(),
            operator.cf(),
            Work::Predict {
                model: &prepared,
                out: &mut out,
            },
        )
        .expect("predict");
    out
}

fn point_model(pixel: [usize; 2]) -> ModelImages {
    let mut image = Array2::<f32>::zeros((IMAGE, IMAGE));
    image[(pixel[1], pixel[0])] = 1.0;
    ModelImages {
        first_plane: 0,
        planes: vec![ModelPlane {
            images: vec![image],
        }],
    }
}

#[test]
fn the_w_term_carries_the_opposite_sign_to_the_uv_term() {
    // `V(u, v, w) = ∫ I(l, m) e^{−2πi(ul + vm + w(n − 1))}` for the
    // MeasurementSet's uvw; placements carry CASA's gridder coordinates,
    // `u` and `v` negated and `w` kept (`FTMachine::negateUV`), so whichever
    // sign the transform gives the placement `(u, v)` term, the `w` term
    // carries the opposite one. A plane's `w` is taken exactly, so no
    // quantisation enters; the phase `2π w (n − 1)` is 0.47 rad here, and
    // the wrong sign would miss the expectation by 0.9.
    let mut rng = Rng::new(31);
    let operator = operator_with(Box::new(w_planes(8)), GridPrecision::F64);
    let geometry = operator.geometry();
    let increment = geometry.image().increment_rad;
    let offset = [28_i64, -20];
    let centre = IMAGE as i64 / 2;
    let pixel = [
        usize::try_from(centre + offset[0]).expect("inside"),
        usize::try_from(centre + offset[1]).expect("inside"),
    ];
    let l = offset[0] as f64 * increment[0];
    let m = offset[1] as f64 * increment[1];
    let n_minus_one = (1.0 - l * l - m * m).sqrt() - 1.0;
    // Plane 5 of 8: `w = (5/7)² · 0.25/|Δx|` (`WPConvFunc`'s quadratic
    // spacing of the `maxW = −1` range).
    let w = (5.0_f64 / 7.0).powi(2) * 0.25 / increment[0].abs();
    let mut placed = placements(geometry, 4, 1, &mut rng);
    for placement in &mut placed {
        placement.u *= 0.3;
        placement.v *= 0.3;
        placement.phase = 0.0;
    }
    let at = |w: f64| {
        let mut block = SampleBuffer::new(2);
        for placement in &placed {
            let placement = Placement {
                w,
                cf: operator.cf().key(&context(), 1.0e9, w),
                ..*placement
            };
            assert!(fits(&operator, &placement));
            block.push(placement, &[Complex32::default(); 2], &[1.0; 2]);
        }
        predict(&operator, &point_model(pixel), &block)
    };
    let flat = at(0.0);
    let lifted = at(w);
    let widen = |value: Complex32| Complex64::new(f64::from(value.re), f64::from(value.im));
    for (index, placement) in placed.iter().enumerate() {
        let flat = widen(flat[index * 2]);
        let lifted = widen(lifted[index * 2]);
        assert!(
            flat.norm() > 0.5,
            "sample {index}: |P(w = 0)| {}",
            flat.norm()
        );
        let uv_phase = std::f64::consts::TAU * (placement.u * l + placement.v * m);
        let direction = flat / flat.norm();
        let minus = (direction - Complex64::from_polar(1.0, -uv_phase)).norm();
        let plus = (direction - Complex64::from_polar(1.0, uv_phase)).norm();
        assert!(
            minus.min(plus) < 0.3,
            "sample {index}: the (u, v) phase {uv_phase} is neither sign of {direction}"
        );
        let sign = if minus < plus { -1.0 } else { 1.0 };
        let expected = Complex64::from_polar(1.0, -sign * std::f64::consts::TAU * w * n_minus_one);
        let ratio = lifted / flat;
        assert!(
            (ratio - expected).norm() < 0.15,
            "sample {index}: P(w)/P(0) = {ratio}, expected {expected} for w = {w}"
        );
    }
}

#[test]
fn supports_grow_with_w_and_the_largest_sets_the_halo() {
    let planes = w_planes(24);
    let supports = (0..planes.planes())
        .map(|plane| planes.half_support(plane))
        .collect::<Vec<_>>();
    assert!(
        supports.windows(2).all(|pair| pair[0] <= pair[1]),
        "{supports:?}"
    );
    assert!(
        supports[0] >= 3 && supports[0] <= 4,
        "plane 0 is the spheroidal: {supports:?}"
    );
    assert!(
        *supports.last().expect("planes") > supports[0],
        "{supports:?}"
    );
    assert_eq!(
        planes.max_half_support(),
        [*supports.last().expect("planes"); 2]
    );
    let one = w_planes(1);
    assert_eq!(one.planes(), 1);
    assert_eq!(one.plane_of(1.0e9), 0);
    let mut hold = CellHold::new();
    assert_eq!(one.taps(CfKey::default(), &mut hold).oversampling(), 2);
}

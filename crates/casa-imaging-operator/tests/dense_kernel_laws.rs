// SPDX-License-Identifier: LGPL-3.0-or-later
//! Dense kernels with complex taps, pointing phase gradients and several
//! Mueller planes: the operator pair is exactly adjoint once each sample's
//! data are divided by the conjugate kernel norm, a prediction divides once
//! by the norm summed over its Mueller planes, and an empty plane never
//! produces a NaN.

mod common;

use casa_imaging_model::{CorrelationType, PolarizationCoordinate};
use casa_imaging_operator::{
    Basis, CfKey, ConvolutionFunctionSet, CpuBackend, GridBackend, GridPrecision, ImageCorrection,
    MeasurementOperator, Mode, ModeSet, ModelImages, ModelPlane, ModelPrescale, MuellerRouting,
    PlaneRange, PolarizationRouting, RowContext, SampleBuffer, Spheroidal, TapLayout, Work,
};
use common::{IMAGE, Rng, geometry, placements, samples};
use ndarray::Array2;
use num_complex::{Complex32, Complex64};

const XX_YY: [CorrelationType; 2] = [CorrelationType::LinearXx, CorrelationType::LinearYy];
const XX_YY_REQUESTED: [PolarizationCoordinate; 2] = [
    PolarizationCoordinate::LinearXx,
    PolarizationCoordinate::LinearYy,
];

/// Tap value of one Mueller plane at `(ix, iy)`.
type TapRule = Box<dyn Fn(usize, usize, &mut Rng) -> Complex32>;

/// A dense kernel set with arbitrary complex taps per Mueller plane and a
/// leakage routing: visibility XX reads grid XX through plane 0 and grid YY
/// through plane 1 (and YY symmetrically). The direct and conjugate tables
/// are identical so the pair stays exactly adjoint.
struct ComplexDense {
    data: Vec<Complex32>,
    support: u16,
    oversampling: u16,
    mueller: MuellerRouting,
    correction: ImageCorrection,
}

impl ComplexDense {
    fn new(support: u16, oversampling: u16, planes: &[TapRule], rng: &mut Rng) -> Self {
        let geometry = geometry();
        let polarization = PolarizationRouting::compile(&XX_YY, &XX_YY_REQUESTED).expect("routing");
        let template = Spheroidal::new(&geometry, &polarization);
        let taps = usize::from(support);
        let fine = usize::from(oversampling) + 1;
        let mut data = Vec::with_capacity(fine * fine * planes.len() * taps * taps);
        for _oy in 0..fine {
            for _ox in 0..fine {
                for plane in planes {
                    for iy in 0..taps {
                        for ix in 0..taps {
                            data.push(plane(ix, iy, rng));
                        }
                    }
                }
            }
        }
        let leak =
            |own: u8, other: u8| vec![vec![Some(own), Some(other)], vec![Some(other), Some(own)]];
        let table = if planes.len() > 1 {
            leak(0, 1)
        } else {
            leak(0, 0)
        };
        let correction = template.image_correction();
        Self {
            data,
            support,
            oversampling,
            mueller: MuellerRouting {
                direct: table.clone(),
                conjugate: table,
            },
            correction: ImageCorrection::new(correction.x().to_vec(), correction.y().to_vec()),
        }
    }

    fn planes(&self) -> u8 {
        (self.data.len()
            / ((usize::from(self.oversampling) + 1).pow(2) * usize::from(self.support).pow(2)))
            as u8
    }
}

impl ConvolutionFunctionSet for ComplexDense {
    fn key(&self, _row: &RowContext, _freq_hz: f64, _w_lambda: f64) -> CfKey {
        CfKey::default()
    }

    fn taps(&self, _key: CfKey) -> TapLayout<'_> {
        TapLayout::Dense {
            data: &self.data,
            support: [self.support, self.support],
            oversampling: self.oversampling,
            mueller_planes: self.planes(),
        }
    }

    fn max_half_support(&self) -> [u16; 2] {
        [self.support / 2; 2]
    }

    fn weight_taps(&self, _key: CfKey) -> Option<TapLayout<'_>> {
        None
    }

    fn mueller(&self) -> &MuellerRouting {
        &self.mueller
    }

    fn image_correction(&self) -> &ImageCorrection {
        &self.correction
    }
}

fn random_tap(_ix: usize, _iy: usize, rng: &mut Rng) -> Complex32 {
    Complex32::new(rng.signed() as f32, rng.signed() as f32)
}

fn operator_with(cf: ComplexDense) -> MeasurementOperator {
    let polarization = PolarizationRouting::compile(&XX_YY, &XX_YY_REQUESTED).expect("routing");
    MeasurementOperator::new(
        geometry(),
        Basis::Constant,
        polarization,
        Box::new(cf),
        GridPrecision::F64,
    )
}

#[test]
fn complex_kernels_with_pointing_gradients_are_adjoint_up_to_the_kernel_norm() {
    let mut rng = Rng::new(41);
    let cf = ComplexDense::new(
        5,
        4,
        &[Box::new(random_tap), Box::new(random_tap)],
        &mut rng,
    );
    let operator = operator_with(cf);
    let mut placed = placements(operator.geometry(), 200, 1, &mut rng);
    for placement in &mut placed {
        placement.gradient = [rng.signed() as f32, rng.signed() as f32];
    }
    let (values, weights) = samples(&placed, 2, &mut rng);
    // The block carries W · d · e^{iφ} / conj(norm) per polarization.
    let mut block = SampleBuffer::new(2);
    for (index, placement) in placed.iter().enumerate() {
        let mut block_values = [Complex32::default(); 2];
        let mut block_weights = [0.0_f32; 2];
        for vpol in 0..2 {
            let weight = weights[index * 2 + vpol];
            let norm = operator.prediction_norm(placement, vpol);
            assert!(
                norm != Complex64::default(),
                "random taps have a non-zero norm"
            );
            let value = values[index * 2 + vpol] * Complex64::from_polar(weight, placement.phase)
                / norm.conj();
            block_values[vpol] = Complex32::new(value.re as f32, value.im as f32);
            block_weights[vpol] = weight as f32;
        }
        block.push(*placement, &block_values, &block_weights);
    }
    let images = (0..2)
        .map(|_| Array2::from_shape_fn((IMAGE, IMAGE), |_| rng.signed() as f32))
        .collect();
    let model = ModelImages {
        first_plane: 0,
        planes: vec![ModelPlane { images }],
    };
    let mut backend = CpuBackend::new();
    let prepared = operator
        .prepare_model(&model, ModelPrescale::Unit)
        .expect("prepared");
    let mut predicted = vec![Complex32::default(); placed.len() * 2];
    backend
        .apply(
            &block.block(),
            operator.cf(),
            Work::Predict {
                model: &prepared,
                out: &mut predicted,
            },
        )
        .expect("predict");
    assert!(
        predicted
            .iter()
            .all(|value| value.re.is_finite() && value.im.is_finite())
    );
    let visibility_side = predicted
        .iter()
        .zip(&values)
        .zip(&weights)
        .map(|((ax, d), w)| (Complex64::new(f64::from(ax.re), f64::from(ax.im)).conj() * d * w).re)
        .sum::<f64>();
    let mut acc = operator.accumulator(PlaneRange::single(0), None, ModeSet::DATA);
    backend
        .apply(
            &block.block(),
            operator.cf(),
            Work::Grid {
                mode: Mode::Data,
                acc: &mut acc,
            },
        )
        .expect("grid");
    let normal = operator.finish(acc).expect("finish");
    let image_side = (0..2)
        .map(|pol| {
            normal
                .data(0, 0, pol)
                .iter()
                .zip(&model.planes[0].images[pol])
                .map(|(image, model)| f64::from(*image) * f64::from(*model))
                .sum::<f64>()
        })
        .sum::<f64>();
    let scale = visibility_side.abs().max(image_side.abs());
    assert!(
        (visibility_side - image_side).abs() <= 1.0e-5 * scale,
        "{visibility_side} vs {image_side}"
    );
}

#[test]
fn prediction_divides_once_by_the_norm_summed_over_mueller_planes() {
    // One-tap planes: plane 0 is 1, plane 1 is c, plane 2 is empty. With
    // the leakage routing XX reads grid XX through plane 0 and grid YY
    // through plane 1, so P_XX = (g_XX + conj(c)·g_YY) / (1 + c), not the
    // sum of two separately normalised terms.
    let c = Complex32::new(0.5, 0.25);
    let mut rng = Rng::new(43);
    let cf = ComplexDense::new(
        1,
        2,
        &[
            Box::new(|_, _, _| Complex32::new(1.0, 0.0)),
            Box::new(move |_, _, _| c),
        ],
        &mut rng,
    );
    let operator = operator_with(cf);
    let images = (0..2)
        .map(|_| Array2::from_shape_fn((IMAGE, IMAGE), |_| rng.signed() as f32))
        .collect();
    let model = ModelImages {
        first_plane: 0,
        planes: vec![ModelPlane { images }],
    };
    let prepared = operator
        .prepare_model(&model, ModelPrescale::Unit)
        .expect("prepared");
    let placed = placements(operator.geometry(), 20, 1, &mut rng);
    let mut block = SampleBuffer::new(2);
    for placement in &placed {
        block.push(*placement, &[Complex32::default(); 2], &[1.0; 2]);
    }
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
    let [nx, _] = operator.geometry().grid_shape();
    let c = Complex64::new(f64::from(c.re), f64::from(c.im));
    for (index, placement) in placed.iter().enumerate() {
        let location = operator.geometry().locate(placement.u, placement.v, 2);
        let cell = location.y as usize * nx + location.x as usize;
        let g_xx = prepared.block::<f64>(0, 0, 0)[cell];
        let g_yy = prepared.block::<f64>(0, 1, 0)[cell];
        let (own, other) = if placement.w > 0.0 {
            (c.conj(), c)
        } else {
            (c, c.conj())
        };
        // Adjoint conjugation for w ≤ 0 conjugates the taps, the forward
        // gather conjugates them back; the norm follows the same rule.
        let expected = (g_xx + own * g_yy) / (Complex64::new(1.0, 0.0) + other);
        let phasor = Complex64::from_polar(1.0, -placement.phase);
        let expected = expected * phasor;
        let actual = predicted[index * 2];
        assert!(
            (Complex64::new(f64::from(actual.re), f64::from(actual.im)) - expected).norm()
                <= 1.0e-5 * expected.norm().max(1.0),
            "sample {index}: {actual} vs {expected}"
        );
    }
}

#[test]
fn an_empty_mueller_plane_predicts_zero_without_nan() {
    let mut rng = Rng::new(47);
    let cf = ComplexDense::new(
        3,
        2,
        &[
            Box::new(|_, _, _| Complex32::default()),
            Box::new(|_, _, _| Complex32::default()),
        ],
        &mut rng,
    );
    let operator = operator_with(cf);
    let images = (0..2)
        .map(|_| Array2::from_shape_fn((IMAGE, IMAGE), |_| rng.signed() as f32))
        .collect();
    let model = ModelImages {
        first_plane: 0,
        planes: vec![ModelPlane { images }],
    };
    let prepared = operator
        .prepare_model(&model, ModelPrescale::Unit)
        .expect("prepared");
    let placed = placements(operator.geometry(), 10, 1, &mut rng);
    let mut block = SampleBuffer::new(2);
    for placement in &placed {
        block.push(*placement, &[Complex32::new(1.0, 1.0); 2], &[1.0; 2]);
    }
    let mut predicted = vec![Complex32::new(7.0, 7.0); placed.len() * 2];
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
    assert!(predicted.iter().all(|value| *value == Complex32::default()));
    let mut acc = operator.accumulator(PlaneRange::single(0), None, ModeSet::DATA);
    let mut residual = vec![Complex32::default(); placed.len() * 2];
    CpuBackend::new()
        .apply(
            &block.block(),
            operator.cf(),
            Work::ResidualGrid {
                model: &prepared,
                acc: &mut acc,
                residual_out: Some(&mut residual),
            },
        )
        .expect("residual");
    assert!(
        residual
            .iter()
            .all(|value| value.re.is_finite() && value.im.is_finite())
    );
    assert_eq!(
        acc.sumwt_at(0, 0, 0),
        0.0,
        "an empty kernel contributes no weight"
    );
}

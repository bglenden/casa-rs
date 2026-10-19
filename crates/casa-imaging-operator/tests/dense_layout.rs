// SPDX-License-Identifier: LGPL-3.0-or-later
//! The dense tap layout through the same kernels: a dense copy of the
//! spheroidal kernel reproduces the separable result, the generic support
//! path agrees with the seven-tap path, and the adjoint law holds for a
//! dense set.

mod common;

use casa_imaging_model::{CorrelationType, PolarizationCoordinate};
use casa_imaging_operator::{
    Basis, CellHold, CfKey, ConvolutionFunctionSet, CpuBackend, DenseCell, GridAccumulator,
    GridBackend, GridPrecision, ImageCorrection, KernelNormalisation, MeasurementOperator, Mode,
    ModeSet, ModelImages, ModelPlane, ModelPrescale, MuellerRouting, Placement, PlaneRange,
    PolarizationRouting, RowContext, SPHEROIDAL_OVERSAMPLING, Spheroidal, TapLayout, Work,
};
use common::{IMAGE, Rng, buffer, geometry, max_abs, placements, samples};
use ndarray::Array2;
use num_complex::{Complex32, Complex64};

const XX_YY: [CorrelationType; 2] = [CorrelationType::LinearXx, CorrelationType::LinearYy];
const STOKES_I: [PolarizationCoordinate; 1] = [PolarizationCoordinate::StokesI];
const XX_YY_REQUESTED: [PolarizationCoordinate; 2] = [
    PolarizationCoordinate::LinearXx,
    PolarizationCoordinate::LinearYy,
];

/// A kernel set whose dense tiles are the outer product of separable rows.
struct DenseFromRows {
    cell: DenseCell,
    support: u16,
    mueller: MuellerRouting,
    correction: ImageCorrection,
}

impl DenseFromRows {
    fn new(
        rows: &[f32],
        support: u16,
        oversampling: u16,
        template: &Spheroidal,
        polarization: &PolarizationRouting,
    ) -> Self {
        let taps = usize::from(support);
        let fine = usize::from(oversampling) + 1;
        let mut data = Vec::with_capacity(fine * fine * taps * taps);
        for oy in 0..fine {
            for ox in 0..fine {
                let rx = &rows[ox * taps..(ox + 1) * taps];
                let ry = &rows[oy * taps..(oy + 1) * taps];
                for wy in ry {
                    for wx in rx {
                        data.push(Complex32::new(wx * wy, 0.0));
                    }
                }
            }
        }
        let correction = template.image_correction();
        Self {
            cell: DenseCell::new(data.into_boxed_slice(), [support, support], oversampling, 1),
            support,
            mueller: MuellerRouting::scalar(polarization.pol_map(), polarization.grid_pols()),
            correction: ImageCorrection::new(correction.x().to_vec(), correction.y().to_vec()),
        }
    }
}

impl ConvolutionFunctionSet for DenseFromRows {
    fn key(&self, _row: &RowContext, _freq_hz: f64, _w_lambda: f64) -> CfKey {
        CfKey::default()
    }

    fn taps<'s>(&'s self, _key: CfKey, _hold: &'s mut CellHold) -> TapLayout<'s> {
        self.cell.layout()
    }

    fn max_half_support(&self) -> [u16; 2] {
        [self.support / 2; 2]
    }

    fn weight_taps<'s>(&'s self, key: CfKey, hold: &'s mut CellHold) -> Option<TapLayout<'s>> {
        Some(self.taps(key, hold))
    }

    fn mueller(&self) -> &MuellerRouting {
        &self.mueller
    }

    fn image_correction(&self) -> &ImageCorrection {
        &self.correction
    }

    fn normalisation(&self) -> KernelNormalisation {
        KernelNormalisation::UnitSum
    }

    fn pointing_ramp(&self) -> bool {
        false
    }

    /// The one cell, lent as the imaging and the weight taps.
    fn tap_footprint(&self) -> casa_imaging_operator::TapFootprint {
        casa_imaging_operator::TapFootprint::of([self.cell.layout(), self.cell.layout()])
    }
}

/// Separable rows of `support` taps with a triangular profile, unit sum.
fn triangle_rows(support: u16, oversampling: u16) -> Vec<f32> {
    let taps = usize::from(support);
    let half = (taps / 2) as f64;
    let fine = f64::from(oversampling);
    let mut rows = Vec::new();
    for o in 0..=usize::from(oversampling) {
        let offset = (o as f64 - fine / 2.0) / fine;
        let values = (0..taps)
            .map(|i| (1.0 - ((i as f64 - half) + offset).abs() / (half + 1.0)).max(0.0))
            .collect::<Vec<_>>();
        let sum = values.iter().sum::<f64>();
        rows.extend(values.iter().map(|v| (v / sum) as f32));
    }
    rows
}

fn separable_set(
    rows: Vec<f32>,
    support: u16,
    oversampling: u16,
    polarization: &PolarizationRouting,
) -> SeparableRows {
    let template = Spheroidal::new(&geometry(), polarization);
    let correction = template.image_correction();
    SeparableRows {
        rows,
        support,
        oversampling,
        mueller: MuellerRouting::scalar(polarization.pol_map(), polarization.grid_pols()),
        correction: ImageCorrection::new(correction.x().to_vec(), correction.y().to_vec()),
    }
}

struct SeparableRows {
    rows: Vec<f32>,
    support: u16,
    oversampling: u16,
    mueller: MuellerRouting,
    correction: ImageCorrection,
}

impl ConvolutionFunctionSet for SeparableRows {
    fn key(&self, _row: &RowContext, _freq_hz: f64, _w_lambda: f64) -> CfKey {
        CfKey::default()
    }

    fn taps<'s>(&'s self, _key: CfKey, _hold: &'s mut CellHold) -> TapLayout<'s> {
        TapLayout::SeparableReal {
            rows: &self.rows,
            support: self.support,
            oversampling: self.oversampling,
        }
    }

    fn max_half_support(&self) -> [u16; 2] {
        [self.support / 2; 2]
    }

    fn weight_taps<'s>(&'s self, _key: CfKey, _hold: &'s mut CellHold) -> Option<TapLayout<'s>> {
        None
    }

    fn mueller(&self) -> &MuellerRouting {
        &self.mueller
    }

    fn image_correction(&self) -> &ImageCorrection {
        &self.correction
    }

    fn normalisation(&self) -> KernelNormalisation {
        KernelNormalisation::UnitSum
    }

    fn pointing_ramp(&self) -> bool {
        false
    }

    fn tap_footprint(&self) -> casa_imaging_operator::TapFootprint {
        casa_imaging_operator::TapFootprint::of([self.taps(CfKey::default(), &mut CellHold::new())])
    }
}

fn dirty_and_psf(operator: &MeasurementOperator, seed: u64) -> (Array2<f32>, Array2<f32>, f64) {
    let mut rng = Rng::new(seed);
    let placed = placements(operator.geometry(), 300, 1, &mut rng);
    let (values, weights) = samples(&placed, 2, &mut rng);
    let block = buffer(&placed, &values, &weights, 2);
    let mut backend = CpuBackend::new();
    let mut acc = operator.accumulator(PlaneRange::single(0), None, ModeSet::DATA_PSF);
    for mode in [Mode::Data, Mode::Psf] {
        backend
            .apply(
                &block.block(),
                operator.cf(),
                Work::Grid {
                    mode,
                    acc: &mut acc,
                },
            )
            .expect("grid");
    }
    let normal = operator.finish(acc).expect("finish");
    (
        normal.data(0, 0, 0).clone(),
        normal.psf(0, 0, 0).clone(),
        normal.psf_sumwt(0, 0, 0),
    )
}

/// `placed` gridded by `operator` in `mode` with `weights` per hand.
fn gridded(
    operator: &MeasurementOperator,
    placed: &[Placement],
    (values, weights): (&[Complex64], &[f64]),
    mode: Mode,
) -> GridAccumulator {
    let block = buffer(placed, values, weights, 2);
    let mut acc = operator.accumulator(PlaneRange::single(0), None, ModeSet::DATA_PSF);
    CpuBackend::new()
        .apply(
            &block.block(),
            operator.cf(),
            Work::Grid {
                mode,
                acc: &mut acc,
            },
        )
        .expect("grid");
    acc
}

/// #718 (663bccf364): Stokes I takes both hands through one Mueller plane,
/// so the gridder sums them and spreads once. That equals spreading each
/// hand alone into its own accumulator and adding the two, on the separable
/// and on the dense tap layout: the cells to a few ulp of the sample scale
/// in f64 and in f32, and `sumwt` bitwise, the weights being exact in binary.
#[test]
fn spreading_both_hands_once_equals_spreading_each_alone() {
    let geometry = geometry();
    let polarization = PolarizationRouting::compile(&XX_YY, &STOKES_I).expect("routing");
    let spheroidal = || Spheroidal::new(&geometry, &polarization);
    let dense = || {
        let template = spheroidal();
        let mut hold = CellHold::new();
        let TapLayout::SeparableReal {
            rows,
            support,
            oversampling,
        } = template.taps(CfKey::default(), &mut hold)
        else {
            panic!("spheroidal taps are separable");
        };
        DenseFromRows::new(rows, support, oversampling, &template, &polarization)
    };
    let mut rng = Rng::new(29);
    let placed = placements(&geometry, 300, 1, &mut rng);
    let (values, _) = samples(&placed, 2, &mut rng);
    // Quarters from 1/4 to 2: every sum of them is exact.
    let weights = (0..placed.len() * 2)
        .map(|_| f64::from(1 + (rng.next_u64() % 8) as u32) / 4.0)
        .collect::<Vec<_>>();
    // Each hand alone: the other's weight is zero, so its row routes one.
    let alone = |hand: usize| {
        weights
            .iter()
            .enumerate()
            .map(|(index, weight)| if index % 2 == hand { *weight } else { 0.0 })
            .collect::<Vec<_>>()
    };
    let (xx, yy) = (alone(0), alone(1));
    for precision in [GridPrecision::F64, GridPrecision::F32] {
        let sets: [Box<dyn ConvolutionFunctionSet>; 2] =
            [Box::new(spheroidal()), Box::new(dense())];
        for (layout, set) in ["separable", "dense"].into_iter().zip(sets) {
            let operator = MeasurementOperator::new(
                geometry.clone(),
                Basis::Constant,
                polarization.clone(),
                set,
                precision,
            );
            for mode in [Mode::Data, Mode::Psf] {
                let both = gridded(&operator, &placed, (&values, &weights), mode);
                let mut each = gridded(&operator, &placed, (&values, &xx), mode);
                each.merge_from(&gridded(&operator, &placed, (&values, &yy), mode))
                    .expect("merge");
                let label = format!("{precision:?} {layout} {mode:?}");
                assert_eq!(both.sumwt(), each.sumwt(), "{label}: sumwt");
                let (once, apart) = match precision {
                    GridPrecision::F64 => (
                        both.block::<f64>(0, 0, 0).to_vec(),
                        each.block::<f64>(0, 0, 0).to_vec(),
                    ),
                    GridPrecision::F32 => (
                        widened(both.block::<f32>(0, 0, 0)),
                        widened(each.block::<f32>(0, 0, 0)),
                    ),
                };
                if mode == Mode::Psf {
                    // A PSF sample is its weight, so the sums are exact.
                    assert!(once == apart, "{label}: cells");
                    continue;
                }
                // The largest sample: two hands, a weight of 2 and the
                // largest visibility.
                let scale = 2.0 * 2.0 * values.iter().map(|v| v.norm()).fold(0.0, f64::max);
                let ulp = match precision {
                    GridPrecision::F64 => f64::EPSILON,
                    GridPrecision::F32 => f64::from(f32::EPSILON),
                } * scale;
                let worst = once
                    .iter()
                    .zip(&apart)
                    .map(|(a, b)| (a - b).norm())
                    .fold(0.0_f64, f64::max);
                assert!(
                    worst <= ulp,
                    "{label}: cells differ by {worst}, one ulp of the samples is {ulp}"
                );
            }
        }
    }
}

fn widened(cells: &[Complex32]) -> Vec<Complex64> {
    cells
        .iter()
        .map(|cell| Complex64::new(f64::from(cell.re), f64::from(cell.im)))
        .collect()
}

fn assert_images_agree(a: &Array2<f32>, b: &Array2<f32>, tolerance: f64) {
    let peak = max_abs(a.iter().copied());
    let worst = a
        .iter()
        .zip(b)
        .map(|(x, y)| f64::from((x - y).abs()))
        .fold(0.0_f64, f64::max);
    assert!(
        worst <= tolerance * peak,
        "images differ by {worst} of {peak}"
    );
}

#[test]
fn dense_copy_of_the_spheroidal_kernel_matches_the_separable_path() {
    for precision in [GridPrecision::F64, GridPrecision::F32] {
        let geometry = geometry();
        let polarization = PolarizationRouting::compile(&XX_YY, &STOKES_I).expect("routing");
        let spheroidal = Spheroidal::new(&geometry, &polarization);
        let mut hold = CellHold::new();
        let TapLayout::SeparableReal {
            rows,
            support,
            oversampling,
        } = spheroidal.taps(CfKey::default(), &mut hold)
        else {
            panic!("spheroidal taps are separable");
        };
        let dense = DenseFromRows::new(rows, support, oversampling, &spheroidal, &polarization);
        let separable = MeasurementOperator::new(
            geometry.clone(),
            Basis::Constant,
            polarization.clone(),
            Box::new(spheroidal),
            precision,
        );
        let dense = MeasurementOperator::new(
            geometry,
            Basis::Constant,
            polarization,
            Box::new(dense),
            precision,
        );
        let (dirty_s, psf_s, sumwt_s) = dirty_and_psf(&separable, 21);
        let (dirty_d, psf_d, sumwt_d) = dirty_and_psf(&dense, 21);
        let tolerance = match precision {
            GridPrecision::F64 => 1.0e-6,
            GridPrecision::F32 => 1.0e-4,
        };
        assert!((sumwt_s - sumwt_d).abs() <= 1.0e-6 * sumwt_s);
        assert_images_agree(&dirty_s, &dirty_d, tolerance);
        assert_images_agree(&psf_s, &psf_d, tolerance);
    }
}

#[test]
fn generic_support_paths_agree_between_layouts() {
    let geometry = geometry();
    let polarization = PolarizationRouting::compile(&XX_YY, &STOKES_I).expect("routing");
    let rows = triangle_rows(5, SPHEROIDAL_OVERSAMPLING);
    let template = Spheroidal::new(&geometry, &polarization);
    let dense = DenseFromRows::new(&rows, 5, SPHEROIDAL_OVERSAMPLING, &template, &polarization);
    let separable = separable_set(rows, 5, SPHEROIDAL_OVERSAMPLING, &polarization);
    let separable = MeasurementOperator::new(
        geometry.clone(),
        Basis::Constant,
        polarization.clone(),
        Box::new(separable),
        GridPrecision::F64,
    );
    let dense = MeasurementOperator::new(
        geometry,
        Basis::Constant,
        polarization,
        Box::new(dense),
        GridPrecision::F64,
    );
    let (dirty_s, psf_s, _) = dirty_and_psf(&separable, 23);
    let (dirty_d, psf_d, _) = dirty_and_psf(&dense, 23);
    assert_images_agree(&dirty_s, &dirty_d, 1.0e-6);
    assert_images_agree(&psf_s, &psf_d, 1.0e-6);
}

#[test]
fn adjoint_law_holds_for_a_dense_kernel_set() {
    let geometry = geometry();
    let polarization = PolarizationRouting::compile(&XX_YY, &STOKES_I).expect("routing");
    let rows = triangle_rows(5, SPHEROIDAL_OVERSAMPLING);
    let template = Spheroidal::new(&geometry, &polarization);
    let dense = DenseFromRows::new(&rows, 5, SPHEROIDAL_OVERSAMPLING, &template, &polarization);
    let operator = MeasurementOperator::new(
        geometry,
        Basis::Constant,
        polarization,
        Box::new(dense),
        GridPrecision::F64,
    );
    let mut rng = Rng::new(29);
    let placed = placements(operator.geometry(), 250, 1, &mut rng);
    let (values, weights) = samples(&placed, 2, &mut rng);
    let block = buffer(&placed, &values, &weights, 2);
    let images = vec![Array2::from_shape_fn((IMAGE, IMAGE), |_| {
        rng.signed() as f32
    })];
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
    let image = normal.data(0, 0, 0).clone();
    let image_side = image
        .iter()
        .zip(&model.planes[0].images[0])
        .map(|(a, b)| f64::from(*a) * f64::from(*b))
        .sum::<f64>();
    let scale = visibility_side.abs().max(image_side.abs());
    assert!(
        (visibility_side - image_side).abs() <= 1.0e-6 * scale,
        "{visibility_side} vs {image_side}"
    );
}

#[test]
fn weight_mode_places_the_weight_taps_at_the_origin() {
    let geometry = geometry();
    let polarization = PolarizationRouting::compile(&XX_YY, &STOKES_I).expect("routing");
    let template = Spheroidal::new(&geometry, &polarization);
    let mut hold = CellHold::new();
    let TapLayout::SeparableReal {
        rows,
        support,
        oversampling,
    } = template.taps(CfKey::default(), &mut hold)
    else {
        panic!("spheroidal taps are separable");
    };
    let dense = DenseFromRows::new(rows, support, oversampling, &template, &polarization);
    let operator = MeasurementOperator::new(
        geometry,
        Basis::Constant,
        polarization,
        Box::new(dense),
        GridPrecision::F64,
    );
    let mut rng = Rng::new(31);
    let placed = placements(operator.geometry(), 100, 1, &mut rng);
    let (values, weights) = samples(&placed, 2, &mut rng);
    let block = buffer(&placed, &values, &weights, 2);
    let mut backend = CpuBackend::new();
    let mut acc = operator.accumulator(PlaneRange::single(0), None, ModeSet::ALL);
    backend
        .apply(
            &block.block(),
            operator.cf(),
            Work::Grid {
                mode: Mode::Weight,
                acc: &mut acc,
            },
        )
        .expect("grid");
    let normal = operator.finish(acc).expect("finish");
    let image = normal.weight(0, 0, 0).expect("weight");
    let sumwt = normal.weight_sumwt(0, 0, 0);
    assert!((sumwt - weights.iter().sum::<f64>()).abs() <= 1.0e-6 * sumwt);
    // Every sample lands on the origin, so the image is the kernel's
    // transform: flat to within the correction, maximal at the centre.
    let centre = f64::from(image[(IMAGE / 2, IMAGE / 2)]) / sumwt;
    assert!((centre - 1.0).abs() < 1.0e-3, "centre {centre}");
    let corner = f64::from(image[(1, 1)]) / sumwt;
    assert!((corner - 1.0).abs() < 2.0e-2, "corner {corner}");

    let spheroidal_only = common::operator(GridPrecision::F64, Basis::Constant, &XX_YY, &STOKES_I);
    let mut acc = spheroidal_only.accumulator(PlaneRange::single(0), None, ModeSet::ALL);
    let result = backend.apply(
        &block.block(),
        spheroidal_only.cf(),
        Work::Grid {
            mode: Mode::Weight,
            acc: &mut acc,
        },
    );
    assert!(matches!(
        result,
        Err(casa_imaging_operator::OperatorError::WeightKernelUnavailable { .. })
    ));
}

/// A Taylor weight mode forms one weight image per PSF term, the moment
/// Σ W·s^m of the weight kernel (`MultiTermFTNew`, #704), for each
/// polarization. XX and YY carry unequal weights over an asymmetric band,
/// so the odd moments do not cancel: each term's `sumwt` is the moment of
/// that polarization's weights, and, the weight taps all sitting at the
/// origin, its image is moment-0's scaled by the ratio of the two.
#[test]
fn taylor_weight_images_are_the_weight_moments_of_each_polarization() {
    const OFFSETS: [f32; 4] = [-0.12, 0.03, 0.18, 0.31];
    for terms in [2_u32, 3] {
        let geometry = geometry();
        let polarization = PolarizationRouting::compile(&XX_YY, &XX_YY_REQUESTED).expect("routing");
        let template = Spheroidal::new(&geometry, &polarization);
        let mut hold = CellHold::new();
        let TapLayout::SeparableReal {
            rows,
            support,
            oversampling,
        } = template.taps(CfKey::default(), &mut hold)
        else {
            panic!("spheroidal taps are separable");
        };
        let dense = DenseFromRows::new(rows, support, oversampling, &template, &polarization);
        let operator = MeasurementOperator::new(
            geometry,
            Basis::Taylor {
                terms,
                reference_hz: 1.0e9,
            },
            polarization,
            Box::new(dense),
            GridPrecision::F64,
        );
        let mut rng = Rng::new(37);
        let placed = placements(operator.geometry(), 40, 1, &mut rng)
            .into_iter()
            .enumerate()
            .map(|(index, placement)| Placement {
                spectral: OFFSETS[index % OFFSETS.len()],
                ..placement
            })
            .collect::<Vec<_>>();
        let (values, weights) = samples(&placed, 2, &mut rng);
        let block = buffer(&placed, &values, &weights, 2);
        let mut acc = operator.accumulator(PlaneRange::single(0), None, ModeSet::ALL);
        CpuBackend::new()
            .apply(
                &block.block(),
                operator.cf(),
                Work::Grid {
                    mode: Mode::Weight,
                    acc: &mut acc,
                },
            )
            .expect("grid");
        let normal = operator.finish(acc).expect("finish");
        let moments = 2 * terms as usize - 1;
        for pol in 0..2 {
            let moment = |order: usize| -> (f64, f64) {
                placed
                    .iter()
                    .enumerate()
                    .fold((0.0, 0.0), |(sum, scale), (index, p)| {
                        let weight = f64::from(weights[index * 2 + pol] as f32);
                        let power = f64::from(p.spectral).powi(order as i32);
                        (sum + weight * power, scale + (weight * power).abs())
                    })
            };
            let (zeroth, _) = moment(0);
            let base = normal.weight(0, 0, pol).expect("weight term 0");
            let peak = max_abs(base.iter().copied());
            for order in 0..moments {
                let (expected, scale) = moment(order);
                let sumwt = normal.weight_sumwt(0, order, pol);
                assert!(
                    (sumwt - expected).abs() <= 1.0e-6 * scale,
                    "{terms} terms, pol {pol}, moment {order}: sumwt {sumwt}, expected {expected}"
                );
                let image = normal.weight(0, order, pol).expect("weight term");
                let ratio = expected / zeroth;
                for (actual, base) in image.iter().zip(base) {
                    let expected = f64::from(*base) * ratio;
                    assert!(
                        (f64::from(*actual) - expected).abs() <= 1.0e-5 * peak * scale / zeroth,
                        "{terms} terms, pol {pol}, moment {order}: {actual} vs {expected}"
                    );
                }
            }
            assert!(
                normal.weight(0, moments, pol).is_none(),
                "{moments} weight terms"
            );
        }
    }
}

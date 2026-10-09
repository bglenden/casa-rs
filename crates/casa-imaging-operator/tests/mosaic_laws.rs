// SPDX-License-Identifier: LGPL-3.0-or-later
//! T0 laws of the mosaic primary-beam kernel set: CASA's pair planes and
//! window cells in the key, the weight image as the beam power at the
//! pointing (which pins the ramp sign), predictions attenuated by the beam
//! at the pointing (the degrid ramp), the exact adjoint with ramps, the
//! unit-sum `sumwt` rule and the split sinc correction.

mod common;

use casa_imaging_model::{CorrelationType, PolarizationCoordinate};
use casa_imaging_operator::{
    AiryDish, Basis, CellHold, CfKey, ConvolutionFunctionSet, CpuBackend, GridBackend,
    GridGeometry, GridPadding, GridPrecision, ImageExtent, KernelNormalisation,
    MOSAIC_OVERSAMPLING, MeasurementOperator, Mode, ModeSet, ModelImages, ModelPlane,
    ModelPrescale, MosaicPb, MosaicWindow, OperatorError, Placement, PlaneRange,
    PolarizationRouting, RowContext, SampleBuffer, Work, pair_plane,
};
use casa_numerics::AnnularApertureVoltageTable;
use common::{IMAGE, Rng, buffer, placements, samples};
use ndarray::Array2;
use num_complex::{Complex32, Complex64};

const XX_YY: [CorrelationType; 2] = [CorrelationType::LinearXx, CorrelationType::LinearYy];
const STOKES_I: [PolarizationCoordinate; 1] = [PolarizationCoordinate::StokesI];
/// 4.1″ cells: the 10.7 m beam spans about sixteen cells at 100 GHz.
const CELL_RAD: f64 = 2.0e-5;
const FREQUENCY_HZ: f64 = 1.0e11;

fn geometry() -> GridGeometry {
    GridGeometry::new(
        ImageExtent {
            shape: [IMAGE, IMAGE],
            increment_rad: [-CELL_RAD, CELL_RAD],
            reference_pixel: [IMAGE / 2, IMAGE / 2],
        },
        GridPadding::None,
    )
    .expect("unpadded geometry")
}

fn context(antenna_types: [u8; 2], spectral_window: u32) -> RowContext {
    RowContext {
        time_s: 0.0,
        antennas: [0, 1],
        antenna_types,
        parallactic_angle_rad: [0.0; 2],
        field: 0,
        spectral_window,
    }
}

fn window(spectral_window: u32, selected: Vec<f64>, window_top: f64) -> MosaicWindow {
    MosaicWindow {
        spectral_window,
        window_frequencies_hz: vec![window_top - 2.0e9, window_top],
        channel_width_hz: 1.0e6,
        selected_frequencies_hz: selected,
    }
}

/// ALMA 12 m and ACA 7 m classes, in that order.
fn dishes(geometry: &GridGeometry) -> [AiryDish; 2] {
    [
        AiryDish::casa_alma(12.0, geometry),
        AiryDish::casa_alma(7.0, geometry),
    ]
}

fn routing() -> PolarizationRouting {
    PolarizationRouting::compile(&XX_YY, &STOKES_I).expect("routing")
}

fn mosaic(geometry: &GridGeometry, dishes: &[AiryDish], windows: &[MosaicWindow]) -> MosaicPb {
    MosaicPb::new(geometry, &routing(), FREQUENCY_HZ, dishes, windows).expect("mosaic set")
}

fn single_window_mosaic(geometry: &GridGeometry) -> MosaicPb {
    let dishes = dishes(geometry);
    mosaic(
        geometry,
        &dishes,
        &[window(0, vec![FREQUENCY_HZ], FREQUENCY_HZ)],
    )
}

fn operator_with(cf: Box<dyn ConvolutionFunctionSet>) -> MeasurementOperator {
    MeasurementOperator::new(
        geometry(),
        Basis::Constant,
        routing(),
        cf,
        GridPrecision::F64,
    )
}

/// The pointing gradient of a pointing `pixels` from the image centre
/// along the image axes.
fn gradient(geometry: &GridGeometry, pixels: [f64; 2]) -> [f32; 2] {
    let increment = geometry.image().increment_rad;
    geometry.pointing_gradient([pixels[0] * increment[0], pixels[1] * increment[1]])
}

/// Analytic `VP_k · VP_j` (the imaging kernel's response) at `offset`
/// pixels from the pointing, through the same Airy tables the set uses.
fn beam_voltage(pair: [&AiryDish; 2], offset: [f64; 2]) -> f64 {
    let radius_deg = ((offset[0] * CELL_RAD).powi(2) + (offset[1] * CELL_RAD).powi(2))
        .sqrt()
        .to_degrees();
    let radius = radius_deg * 60.0 * FREQUENCY_HZ / 1.0e9;
    pair.iter()
        .map(|dish| {
            let table = AnnularApertureVoltageTable::new(
                dish.aperture_m,
                dish.blockage_m,
                dish.max_radius_arcsec / 60.0 * 100.0,
            );
            f64::from(table.evaluate(radius))
        })
        .product()
}

/// Analytic `PB_k · PB_j` (`VP² · VP²`, the weight kernel's response).
fn beam_power(pair: [&AiryDish; 2], offset: [f64; 2]) -> f64 {
    beam_voltage(pair, offset).powi(2)
}

fn fits(operator: &MeasurementOperator, placement: &Placement) -> bool {
    let mut hold = CellHold::new();
    let taps = operator.cf().taps(placement.cf, &mut hold);
    let location = operator
        .geometry()
        .locate(placement.u, placement.v, taps.oversampling());
    operator.geometry().fits(location, taps.half_support())
}

/// Random placements keyed on random dish pairs, each with the ramp of a
/// random pointing within six cells of the centre.
fn keyed_placements(operator: &MeasurementOperator, count: usize, rng: &mut Rng) -> Vec<Placement> {
    let geometry = operator.geometry();
    let mut placed = placements(geometry, count, 1, rng);
    for placement in &mut placed {
        let types = [(rng.next_u64() % 2) as u8, (rng.next_u64() % 2) as u8];
        placement.cf = operator
            .cf()
            .key(&context(types, 0), FREQUENCY_HZ, placement.w);
        placement.gradient = gradient(geometry, [6.0 * rng.signed(), 6.0 * rng.signed()]);
        placement.u *= 0.6;
        placement.v *= 0.6;
    }
    placed.retain(|placement| fits(operator, placement));
    placed
}

fn grid(operator: &MeasurementOperator, block: &SampleBuffer, mode: Mode) -> (Array2<f32>, f64) {
    let modes = ModeSet {
        data: mode == Mode::Data,
        psf: mode == Mode::Psf,
        weight: mode == Mode::Weight,
    };
    let mut acc = operator.accumulator(PlaneRange::single(0), None, modes);
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
    let normal = operator.finish(acc).expect("finish");
    match mode {
        Mode::Data => (normal.data(0, 0, 0).clone(), normal.data_sumwt(0, 0, 0)),
        Mode::Psf => (normal.psf(0, 0, 0).clone(), normal.psf_sumwt(0, 0, 0)),
        Mode::Weight => (
            normal.weight(0, 0).expect("weight image").clone(),
            normal.weight_sumwt(0, 0),
        ),
    }
}

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

/// A 16-cell image gives a 16-pixel screen whose quarter lattice cannot
/// hold `supportAndNormalizeLatt`'s fallback, and a 32-cell image a beam
/// crop too small for the Lanczos resample: both are typed errors.
#[test]
fn too_small_a_screen_is_a_typed_error_not_a_panic() {
    for side in [16_usize, 32] {
        let geometry = GridGeometry::new(
            ImageExtent {
                shape: [side, side],
                increment_rad: [-4.85e-6, 4.85e-6],
                reference_pixel: [side / 2, side / 2],
            },
            GridPadding::None,
        )
        .expect("geometry");
        let dishes = [AiryDish::casa_alma(12.0, &geometry)];
        let result = MosaicPb::new(
            &geometry,
            &routing(),
            5.0e10,
            &dishes,
            &[window(0, vec![5.0e10], 5.0e10)],
        );
        assert!(
            matches!(result, Err(OperatorError::ConvolutionFunction { .. })),
            "side {side}: {:?}",
            result.err()
        );
    }
}

#[test]
fn keys_follow_the_pair_plane_and_the_window_cells() {
    let geometry = geometry();
    let dishes = dishes(&geometry);
    // Window 1: eight channels at 0.3 GHz from 98 GHz under a 101 GHz
    // top → three cells (98.475, 98.98, 99.485 GHz) with tol 0.505 GHz.
    let eight = (0..8)
        .map(|k| 98.0e9 + k as f64 * 0.3e9)
        .collect::<Vec<_>>();
    let set = mosaic(
        &geometry,
        &dishes,
        &[
            window(0, vec![FREQUENCY_HZ], FREQUENCY_HZ),
            window(1, eight, 101.0e9),
        ],
    );
    assert_eq!(set.dishes(), 2);
    assert_eq!(set.screen_size(), 64);
    assert_eq!(set.frequency_cells(0), Some(&[FREQUENCY_HZ][..]));
    let cells = set.frequency_cells(1).expect("window 1");
    assert_eq!(cells.len(), 3);
    assert!((cells[0] - 98.475e9).abs() < 1.0 && (cells[2] - 99.485e9).abs() < 1.0);
    assert_eq!(set.frequency_cells(7), None);
    // (0,0) → plane 0, (0,1)/(1,0) → 1, (1,1) → 2; cube = window offset +
    // chanMap cell (first within tol/2, else nearest).
    assert_eq!(pair_plane([1, 0], 2), 1);
    assert_eq!(
        set.key(&context([0, 0], 0), FREQUENCY_HZ, 3.0),
        CfKey { group: 0, cube: 0 }
    );
    assert_eq!(
        set.key(&context([1, 0], 1), 98.9e9, -3.0),
        CfKey { group: 1, cube: 2 }
    );
    assert_eq!(
        set.key(&context([1, 1], 1), 100.1e9, 0.0),
        CfKey { group: 2, cube: 3 }
    );
    // Every cell has ten fine offsets, at least five taps each side, and
    // weight taps of the same support; the halo is the largest support.
    let mut hold = CellHold::new();
    let mut largest = 0;
    for group in 0..3 {
        for cube in 0..4 {
            let key = CfKey { group, cube };
            let taps = set.taps(key, &mut hold);
            assert_eq!(taps.oversampling(), MOSAIC_OVERSAMPLING);
            assert_eq!(taps.mueller_planes(), 1);
            let half = taps.half_support();
            assert!(half[0] >= 5 && half[0] == half[1], "{key:?}: {half:?}");
            assert_eq!(set.half_support(key), half[0]);
            let mut weight_hold = CellHold::new();
            let weight = set.weight_taps(key, &mut weight_hold).expect("weight taps");
            assert_eq!(weight.half_support(), half);
            largest = largest.max(half[0]);
        }
    }
    assert_eq!(set.max_half_support(), [largest; 2]);
    assert_eq!(set.normalisation(), KernelNormalisation::UnitSum);
    assert!(set.pointing_ramp());
    assert!(set.bytes() > 0);
}

#[test]
fn the_weight_image_is_the_beam_power_at_the_pointing() {
    let mut rng = Rng::new(83);
    let geometry = geometry();
    let dishes = dishes(&geometry);
    let operator = operator_with(Box::new(single_window_mosaic(&geometry)));
    // A pointing eight cells right and five cells down of the centre, on
    // the 12 m × 7 m pair.
    let pointing = [8.0, -5.0];
    let placed = keyed_placements(&operator, 12, &mut rng)
        .into_iter()
        .map(|placement| Placement {
            cf: operator
                .cf()
                .key(&context([0, 1], 0), FREQUENCY_HZ, placement.w),
            gradient: gradient(&geometry, pointing),
            ..placement
        })
        .collect::<Vec<_>>();
    assert!(placed.len() >= 8);
    let mut block = SampleBuffer::new(2);
    for placement in &placed {
        block.push(*placement, &[Complex32::default(); 2], &[1.0; 2]);
    }
    let (image, sumwt) = grid(&operator, &block, Mode::Weight);
    assert_eq!(sumwt, 2.0 * placed.len() as f64);
    let centre = (IMAGE / 2) as f64;
    let expected_peak = [centre + pointing[0], centre + pointing[1]];
    let mut peak = (0, 0, f32::NEG_INFINITY);
    for ((y, x), value) in image.indexed_iter() {
        if *value > peak.2 {
            peak = (x, y, *value);
        }
    }
    assert_eq!(
        (peak.0, peak.1),
        (expected_peak[0] as usize, expected_peak[1] as usize),
        "weight image peak"
    );
    // The profile about the pointing is the pair's PB² (truncated at the
    // 2.5 % support, so within a few percent of the peak).
    let pair = [&dishes[0], &dishes[1]];
    let reference = beam_power(pair, [0.0, 0.0]);
    for offset in [
        [3.0, 0.0],
        [0.0, 4.0],
        [-5.0, 2.0],
        [6.0, 6.0],
        [-9.0, -3.0],
    ] {
        let x = (expected_peak[0] + offset[0]) as usize;
        let y = (expected_peak[1] + offset[1]) as usize;
        let actual = f64::from(image[(y, x)] / peak.2);
        let expected = beam_power(pair, offset) / reference;
        assert!(
            (actual - expected).abs() < 5.0e-2,
            "offset {offset:?}: {actual} vs {expected}"
        );
    }
    // The mirrored pointing holds almost nothing.
    let mirrored = image[(
        (centre - pointing[1]) as usize,
        (centre - pointing[0]) as usize,
    )];
    assert!(
        mirrored < 0.1 * peak.2,
        "mirrored {mirrored} of peak {}",
        peak.2
    );
}

#[test]
fn predictions_attenuate_the_model_by_the_beam_at_the_pointing() {
    let mut rng = Rng::new(89);
    let geometry = geometry();
    let dishes = dishes(&geometry);
    let operator = operator_with(Box::new(single_window_mosaic(&geometry)));
    let pointing = [10.0, 0.0];
    // Short baselines on the 12 m pair, all with the pointing's ramp.
    let placed = keyed_placements(&operator, 10, &mut rng)
        .into_iter()
        .map(|placement| Placement {
            u: placement.u * 0.2,
            v: placement.v * 0.2,
            cf: operator
                .cf()
                .key(&context([0, 0], 0), FREQUENCY_HZ, placement.w),
            gradient: gradient(&geometry, pointing),
            ..placement
        })
        .collect::<Vec<_>>();
    assert!(placed.len() >= 6);
    let mut block = SampleBuffer::new(2);
    for placement in &placed {
        block.push(*placement, &[Complex32::default(); 2], &[1.0; 2]);
    }
    let centre = IMAGE / 2;
    let at_pointing = predict(
        &operator,
        &point_model([centre + pointing[0] as usize, centre]),
        &block,
    );
    let pair = [&dishes[0], &dishes[0]];
    for offset in [[4.0, 0.0], [0.0, -5.0], [-3.0, 3.0]] {
        let pixel = [
            (centre as f64 + pointing[0] + offset[0]) as usize,
            (centre as f64 + pointing[1] + offset[1]) as usize,
        ];
        let shifted = predict(&operator, &point_model(pixel), &block);
        // The imaging kernel is `FT[VP₁ · VP₂]`: the model is attenuated by
        // the voltage product, not the squared beam of the weight kernel.
        let expected = beam_voltage(pair, offset) / beam_voltage(pair, [0.0, 0.0]);
        // With the ramp's sign wrong the beam would sit at the mirrored
        // pointing, twenty cells away, where it is below a few percent.
        let wrong = beam_voltage(pair, [2.0 * pointing[0] + offset[0], offset[1]])
            / beam_voltage(pair, [0.0, 0.0]);
        assert!(wrong < 0.05, "offset {offset:?}: mirrored beam {wrong}");
        for (shifted, reference) in shifted.iter().zip(&at_pointing) {
            let ratio = f64::from(shifted.norm() / reference.norm());
            assert!(
                (ratio - expected).abs() < 5.0e-2,
                "offset {offset:?}: {ratio} vs {expected}"
            );
        }
    }
}

/// `Mode::Psf` grids the weight as a unit visibility through the same
/// ramped kernel the data use, fine offset included (CASA reads the ramped
/// kernel at `ix·sampling + off` for `dopsf` too), so it equals the data
/// grid of unit visibilities placed identically.
#[test]
fn the_psf_is_the_data_grid_of_unit_visibilities_under_ramps() {
    let mut rng = Rng::new(97);
    let geometry = geometry();
    let operator = operator_with(Box::new(single_window_mosaic(&geometry)));
    let placed = keyed_placements(&operator, 24, &mut rng);
    assert!(placed.len() >= 16);
    let mut psf_block = SampleBuffer::new(2);
    let mut data_block = SampleBuffer::new(2);
    for placement in &placed {
        let weights = [0.5 + rng.unit() as f32, 0.5 + rng.unit() as f32];
        psf_block.push(*placement, &[Complex32::default(); 2], &weights);
        data_block.push(
            Placement {
                phase: 0.0,
                ..*placement
            },
            &[
                Complex32::new(weights[0], 0.0),
                Complex32::new(weights[1], 0.0),
            ],
            &weights,
        );
    }
    let (psf, psf_sumwt) = grid(&operator, &psf_block, Mode::Psf);
    let (data, data_sumwt) = grid(&operator, &data_block, Mode::Data);
    assert!((psf_sumwt - data_sumwt).abs() < 1.0e-9 * data_sumwt);
    let peak = common::max_abs(psf.iter().copied());
    assert!(peak > 0.0);
    for (index, (psf, data)) in psf.iter().zip(&data).enumerate() {
        assert!(
            (f64::from(*psf) - f64::from(*data)).abs() < 1.0e-5 * peak,
            "pixel {index}: PSF {psf}, unit-visibility data {data}"
        );
    }
}

#[test]
fn mosaic_kernels_are_exactly_adjoint_with_pointing_ramps() {
    let mut rng = Rng::new(97);
    let geometry = geometry();
    let operator = operator_with(Box::new(single_window_mosaic(&geometry)));
    let placed = keyed_placements(&operator, 150, &mut rng);
    assert!(placed.len() >= 100);
    assert!(placed.iter().any(|placement| placement.cf.group == 1));
    let (values, weights) = samples(&placed, 2, &mut rng);
    let block = buffer(&placed, &values, &weights, 2);
    let images = vec![Array2::from_shape_fn((IMAGE, IMAGE), |_| {
        rng.signed() as f32
    })];
    let model = ModelImages {
        first_plane: 0,
        planes: vec![ModelPlane { images }],
    };
    let predicted = predict(&operator, &model, &block);
    let visibility_side = predicted
        .iter()
        .zip(&values)
        .zip(&weights)
        .map(|((ax, d), w)| (Complex64::new(f64::from(ax.re), f64::from(ax.im)).conj() * d * w).re)
        .sum::<f64>();
    let (image, _) = grid(&operator, &block, Mode::Data);
    // The model side carries the sinc and the image side its inverse, so
    // the pairing holds with the model multiplied by the sinc squared.
    let correction = operator.cf().image_correction();
    let image_side = image
        .indexed_iter()
        .map(|((y, x), image)| {
            let factor = correction.model_at(x, y) / correction.at(x, y);
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
fn sumwt_is_the_weight_sum_and_the_sinc_correction_is_split() {
    let mut rng = Rng::new(101);
    let geometry = geometry();
    let operator = operator_with(Box::new(single_window_mosaic(&geometry)));
    let placed = keyed_placements(&operator, 40, &mut rng);
    let (values, weights) = samples(&placed, 2, &mut rng);
    let block = buffer(&placed, &values, &weights, 2);
    let (_, sumwt) = grid(&operator, &block, Mode::Data);
    let expected = weights.iter().map(|w| f64::from(*w as f32)).sum::<f64>();
    assert!(
        (sumwt - expected).abs() <= 1.0e-12 * expected,
        "{sumwt} vs {expected}"
    );
    // `prepGridForDegrid` multiplies the model by the sinc, `getImage`
    // divides the image by it; unity at the centre.
    let correction = operator.cf().image_correction();
    let centre = IMAGE / 2;
    assert_eq!(correction.at(centre, centre), 1.0);
    assert_eq!(correction.model_at(centre, centre), 1.0);
    assert!(correction.model_at(0, centre) < 1.0);
    assert!(correction.at(0, centre) > 1.0);
    for y in 0..IMAGE {
        for x in 0..IMAGE {
            let product = correction.at(x, y) * correction.model_at(x, y);
            assert!((product - 1.0).abs() < 1.0e-12, "({x}, {y}): {product}");
        }
    }
    // The edge sinc at ten fine offsets: sin(π/20)/(π/20).
    let edge = (std::f64::consts::PI / 20.0).sin() / (std::f64::consts::PI / 20.0);
    assert!((correction.model_at(0, centre) - edge).abs() < 1.0e-6);
}

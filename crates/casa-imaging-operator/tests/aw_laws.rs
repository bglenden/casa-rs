// SPDX-License-Identifier: LGPL-3.0-or-later
//! T0 laws of the AW catalog: native generation writes CASA-format cells
//! that open cold and warm to identical keys, taps and gridded products;
//! CASA's w, frequency (with the conjugate-beam map) and parallactic-angle
//! cell rules; the swapped Mueller tables of the conjugate baseline (the
//! CASA counterexample to the adjoint identity); the kernel-sum
//! normalisation; and the bounded cache that keeps a held cell alive.

mod common;

use casa_imaging_model::{
    CorrelationType, EvlaDishSurface, NativeAwFrequencyGroup, NativeAwGrid, NativeAwRequestInput,
    NativeAwTerms, PolarizationCoordinate,
};
use casa_imaging_operator::{
    AwCatalog, AwIndexing, Basis, CellHold, CfKey, ConvolutionFunctionSet, CpuBackend, GridBackend,
    GridGeometry, GridPadding, GridPrecision, GridScalar, ImageExtent, KernelNormalisation,
    MeasurementOperator, Mode, ModeSet, Placement, PlaneRange, PolarizationRouting, RowContext,
    SampleBuffer, TapLayout, Work,
};
use common::{IMAGE, Rng, buffer, placements, samples};
use num_complex::Complex32;

const RR_LL: [CorrelationType; 2] = [CorrelationType::CircularRr, CorrelationType::CircularLl];
const STOKES_I: [PolarizationCoordinate; 1] = [PolarizationCoordinate::StokesI];
/// 20″ cells: the working field is 64 × 4 cells wide at the generation
/// oversampling.
const CELL_RAD: f64 = 1.0e-4;
const OVERSAMPLING: usize = 4;
const REFERENCE_HZ: f64 = 1.5e9;
const FREQUENCIES_HZ: [f64; 2] = [1.4e9, 1.6e9];
/// Two w planes: `wIncr = (n−1)²/maxUVW` with `maxUVW = 1/(4|Δ|)`.
const W_PLANES: usize = 2;

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

fn routing() -> PolarizationRouting {
    PolarizationRouting::compile(&RR_LL, &STOKES_I).expect("routing")
}

fn surface() -> EvlaDishSurface {
    EvlaDishSurface::new(
        (0..=125)
            .map(|i| {
                let r = i as f64 / 10.0;
                [r, r * r / 36.0, r / 18.0]
            })
            .collect(),
    )
    .expect("surface")
}

fn request(conjugate_beams: bool) -> NativeAwRequestInput {
    let max_w = 1.0 / (CELL_RAD * 4.0);
    let w_increment = f64::from(((W_PLANES - 1) * (W_PLANES - 1)) as f32) / max_w;
    // The working sky cell: `cell · oversampling · image / working`.
    let working = CELL_RAD * OVERSAMPLING as f64 * IMAGE as f64 / IMAGE as f64;
    NativeAwRequestInput {
        surface: surface(),
        antenna_diameter_m: 25.0,
        frequencies: FREQUENCIES_HZ
            .iter()
            .enumerate()
            .map(|(window, frequency)| NativeAwFrequencyGroup {
                spectral_window: window as u32,
                channel_frequencies_hz: vec![*frequency],
                cf_frequency_hz: *frequency,
            })
            .collect(),
        w_values: (0..W_PLANES)
            .map(|index| (index * index) as f64 / w_increment)
            .collect(),
        w_increment,
        pa_values: vec![0.31],
        mueller_elements: vec![0, 15],
        reference_frequency_hz: REFERENCE_HZ,
        grid: NativeAwGrid {
            size: IMAGE,
            sky_increment_rad: [-working, working],
            oversampling: OVERSAMPLING,
        },
        terms: NativeAwTerms {
            aperture: true,
            w_term: true,
            prolate_spheroidal: false,
            wideband: true,
            conjugate_beams,
        },
        maximum_cells: 64,
    }
}

fn indexing(conjugate_beams: bool) -> AwIndexing {
    AwIndexing {
        conjugate_beams,
        image_reference_hz: REFERENCE_HZ,
    }
}

fn context(pa_deg: f64, spectral_window: u32) -> RowContext {
    RowContext {
        time_s: 0.0,
        antennas: [0, 1],
        antenna_types: [0, 0],
        // CASA's operator angle is the negative of the physical angle.
        parallactic_angle_rad: [-pa_deg.to_radians(); 2],
        field: 0,
        spectral_window,
    }
}

/// Generate a cache into `root` and open it.
fn generated(root: &std::path::Path, conjugate_beams: bool, bound: usize) -> AwCatalog {
    AwCatalog::generate_native(root, &request(conjugate_beams), false).expect("generate");
    AwCatalog::open_casa(
        root,
        indexing(conjugate_beams),
        &geometry(),
        &routing(),
        bound,
    )
    .expect("open")
}

fn operator_with(catalog: AwCatalog) -> MeasurementOperator {
    MeasurementOperator::new(
        geometry(),
        Basis::Constant,
        routing(),
        Box::new(catalog),
        GridPrecision::F64,
    )
}

fn fits(operator: &MeasurementOperator, placement: &Placement) -> bool {
    let mut hold = CellHold::new();
    let taps = operator.cf().taps(placement.cf, &mut hold);
    let location = operator
        .geometry()
        .locate(placement.u, placement.v, taps.oversampling());
    operator.geometry().fits(location, taps.half_support())
}

/// Random placements keyed on random windows and w signs.
fn keyed_placements(operator: &MeasurementOperator, count: usize, rng: &mut Rng) -> Vec<Placement> {
    let mut placed = placements(operator.geometry(), count, 1, rng);
    let max_w = 0.9 / (CELL_RAD * 4.0);
    for placement in &mut placed {
        let window = (rng.next_u64() % 2) as u32;
        placement.w = rng.signed() * max_w;
        placement.cf = operator.cf().key(
            &context(17.0, window),
            FREQUENCIES_HZ[window as usize],
            placement.w,
        );
        placement.u *= 0.7;
        placement.v *= 0.7;
    }
    placed.retain(|placement| fits(operator, placement));
    placed
}

fn grid(
    operator: &MeasurementOperator,
    block: &SampleBuffer,
    mode: Mode,
) -> (Vec<Complex32>, Vec<f64>) {
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
    (
        <f64 as GridScalar>::cells(acc.storage())
            .iter()
            .map(|cell| Complex32::new(cell.re as f32, cell.im as f32))
            .collect(),
        acc.sumwt().to_vec(),
    )
}

#[test]
fn generated_cells_open_with_casa_index_rules_and_swapped_mueller_tables() {
    let root = tempfile::tempdir().expect("cache directory");
    let catalog = generated(root.path(), false, usize::MAX);
    assert_eq!(catalog.parallactic_angles_deg().len(), 1);
    assert!((catalog.parallactic_angles_deg()[0] - 0.31_f64.to_degrees()).abs() < 1.0e-9);
    assert_eq!(catalog.frequencies_hz().len(), 2);
    assert_eq!(catalog.w_values().len(), W_PLANES);
    assert_eq!(catalog.mueller_elements(), &[0, 15]);
    assert_eq!(catalog.oversampling(), OVERSAMPLING as u16);
    assert_eq!(catalog.groups(), 2 * W_PLANES);
    // `CFCache::getCFParams` lists the frequency as a Float.
    assert_eq!(catalog.frequencies_hz()[0], f64::from(1.4e9_f64 as f32));
    // nearestWNdx: round(sqrt(wIncr·|w|)) clamped to the planes.
    let w_increment = catalog.w_increment();
    let w_of = |root: f64| root * root / w_increment;
    assert_eq!(catalog.w_cell(w_of(0.49)), 0);
    assert_eq!(catalog.w_cell(-w_of(0.51)), 1);
    assert_eq!(catalog.w_cell(w_of(40.0)), W_PLANES - 1);
    // The frequency cell is the nearest listed value; the PA cell the
    // nearest angle on the circle.
    assert_eq!(catalog.frequency_cell(1.49e9), 0);
    assert_eq!(catalog.frequency_cell(1.51e9), 1);
    assert_eq!(catalog.pa_cell(17.0), 0);
    assert_eq!(catalog.pa_cell(-350.0), 0);
    // Keys: the same group for ±w; the cube field names the native-frequency
    // cell, which without conjugate beams is the gridding cell itself.
    let key = catalog.key(&context(17.0, 1), 1.6e9, w_of(0.9));
    assert_eq!(key, catalog.key(&context(17.0, 1), 1.6e9, -w_of(0.9)));
    assert_eq!(
        key,
        CfKey {
            group: catalog.group_index(0, 1, 1) as u16,
            cube: catalog.group_index(0, 1, 1) as u16
        }
    );
    // makeConjPolMap: RR reads the RR plane on the direct table and the LL
    // plane on the conjugate one; the forward transform swaps them.
    let mueller = catalog.mueller();
    assert_eq!(mueller.direct, vec![vec![Some(0), Some(1)]]);
    assert_eq!(mueller.conjugate, vec![vec![Some(1), Some(0)]]);
    assert_eq!(mueller.table(true, false), &mueller.direct[..]);
    assert_eq!(mueller.table(true, true), &mueller.conjugate[..]);
    assert_eq!(catalog.normalisation(), KernelNormalisation::KernelSum);
    assert!(catalog.pointing_ramp());
    // Every cell is dense, four fine offsets, two planes, inside the halo.
    let mut hold = CellHold::new();
    for group in 0..catalog.groups() {
        let key = CfKey {
            group: group as u16,
            cube: 0,
        };
        let TapLayout::Dense {
            support,
            oversampling: 4,
            mueller_planes: 2,
            ..
        } = catalog.taps(key, &mut hold)
        else {
            panic!("dense AW cell");
        };
        assert!(support[0] <= 2 * catalog.max_half_support()[0] + 1);
        let mut weight_hold = CellHold::new();
        let weight = catalog
            .weight_taps(key, &mut weight_hold)
            .expect("weight taps");
        assert_eq!(weight.oversampling(), 4);
        assert_eq!(weight.mueller_planes(), 2);
    }
}

#[test]
fn conjugate_beams_select_the_cell_nearest_the_conjugate_frequency() {
    let root = tempfile::tempdir().expect("cache directory");
    let catalog = generated(root.path(), true, usize::MAX);
    // √(2·1.5² − 1.4²) GHz = 1.59 GHz → the 1.6 GHz cell; √(2·1.5² − 1.6²)
    // = 1.39 GHz → the 1.4 GHz cell.
    assert_eq!(catalog.frequency_cell(1.4e9), 1);
    assert_eq!(catalog.frequency_cell(1.6e9), 0);
    let other = tempfile::tempdir().expect("cache directory");
    let direct = generated(other.path(), false, usize::MAX);
    assert_eq!(direct.frequency_cell(1.4e9), 0);

    // `DataToGrid` grids a 1.4 GHz row with the conjugate (1.6 GHz) cell;
    // `GridToData` predicts it with the native 1.4 GHz cell
    // (`nearestFreqNdx(spw, chan)` without `conjBeams`), and the PSF is
    // gridded with the weight cell (`cfwts2_p` when `makingPSF`).
    let key = catalog.key(&context(0.31_f64.to_degrees(), 0), 1.4e9, 0.0);
    assert_eq!(usize::from(key.group), catalog.group_index(0, 1, 0));
    assert_eq!(usize::from(key.cube), catalog.group_index(0, 0, 0));
    let native = CfKey {
        group: key.cube,
        cube: key.cube,
    };
    let dense = |taps: TapLayout<'_>| match taps {
        TapLayout::Dense { data, .. } => data.to_vec(),
        TapLayout::SeparableReal { .. } => panic!("AW cells are dense"),
    };
    let mut lent = CellHold::new();
    let mut other = CellHold::new();
    assert_eq!(
        dense(catalog.prediction_taps(key, &mut lent)),
        dense(catalog.taps(native, &mut other))
    );
    assert_ne!(
        dense(catalog.prediction_taps(key, &mut lent)),
        dense(catalog.taps(key, &mut other))
    );
    assert_eq!(
        dense(catalog.psf_taps(key, &mut lent)),
        dense(catalog.weight_taps(key, &mut other).expect("weight cell"))
    );
    assert_ne!(
        dense(catalog.psf_taps(key, &mut lent)),
        dense(catalog.taps(key, &mut other))
    );
}

#[test]
fn cold_and_warm_catalogs_give_identical_keys_taps_and_products() {
    let mut rng = Rng::new(103);
    let root = tempfile::tempdir().expect("cache directory");
    let cold = generated(root.path(), false, usize::MAX);
    let warm = AwCatalog::open_casa(
        root.path(),
        indexing(false),
        &geometry(),
        &routing(),
        usize::MAX,
    )
    .expect("warm open");
    for group in 0..cold.groups() {
        let key = CfKey {
            group: group as u16,
            cube: 0,
        };
        let (mut a, mut b) = (CellHold::new(), CellHold::new());
        let (
            TapLayout::Dense {
                data: cold_taps, ..
            },
            TapLayout::Dense {
                data: warm_taps, ..
            },
        ) = (cold.taps(key, &mut a), warm.taps(key, &mut b))
        else {
            panic!("dense cells");
        };
        assert_eq!(cold_taps, warm_taps, "group {group}");
    }
    let cold = operator_with(cold);
    let warm = operator_with(warm);
    let placed = keyed_placements(&cold, 80, &mut rng);
    assert!(placed.len() >= 40, "{} placements fit", placed.len());
    for (window, frequency) in FREQUENCIES_HZ.iter().enumerate() {
        for w in [-700.0, -3.0, 0.0, 2.0, 900.0] {
            let row = context(17.0, window as u32);
            assert_eq!(
                cold.cf().key(&row, *frequency, w),
                warm.cf().key(&row, *frequency, w),
                "window {window} w {w}"
            );
        }
    }
    let (values, weights) = samples(&placed, 2, &mut rng);
    let block = buffer(&placed, &values, &weights, 2);
    for mode in [Mode::Data, Mode::Psf, Mode::Weight] {
        let (cold_cells, cold_sumwt) = grid(&cold, &block, mode);
        let (warm_cells, warm_sumwt) = grid(&warm, &block, mode);
        assert_eq!(cold_cells, warm_cells, "{mode:?} cells");
        assert_eq!(cold_sumwt, warm_sumwt, "{mode:?} sumwt");
    }
}

#[test]
fn predictions_divide_by_the_kernel_sum_of_the_swapped_plane() {
    let mut rng = Rng::new(107);
    let root = tempfile::tempdir().expect("cache directory");
    let operator = operator_with(generated(root.path(), false, usize::MAX));
    let placed = keyed_placements(&operator, 40, &mut rng);
    for placement in &placed {
        let mirrored = Placement {
            w: -placement.w,
            ..*placement
        };
        // The forward transform of a w > 0 row reads RR through the LL
        // plane unconjugated; the mirrored row reads LL through the LL
        // plane conjugated: the norms are conjugates of each other.
        let (positive, negative) = if placement.w > 0.0 {
            (placement, &mirrored)
        } else {
            (&mirrored, placement)
        };
        let rr = operator.prediction_norm(positive, 0);
        let ll = operator.prediction_norm(negative, 1);
        assert!(
            (rr - ll.conj()).norm() <= 1.0e-9 * rr.norm().max(1.0e-12),
            "{rr} vs conj({ll})"
        );
    }
    // The cells are normalised by their sampled area (`AWConvFunc::cfArea`,
    // the integer taps over `[−S, S)`), so at the centre fine offset the
    // kernel sum is one up to the taps beyond the support, below 1e-3 of
    // the peak each.
    for placement in placed.iter().take(8) {
        let centred = Placement {
            u: 0.0,
            v: 0.0,
            ..*placement
        };
        for vpol in 0..2 {
            let norm = operator.prediction_norm(&centred, vpol);
            assert!((norm.norm() - 1.0).abs() < 5.0e-2, "centre norm {norm}");
        }
    }
    // sumwt follows W·|N| and the data grid of a weight-one, unit sample
    // equals the spread of the routed plane: pin the rule with the
    // operator's own norm.
    let (values, weights) = samples(&placed, 2, &mut rng);
    let block = buffer(&placed, &values, &weights, 2);
    let (_, sumwt) = grid(&operator, &block, Mode::Data);
    let expected = placed
        .iter()
        .enumerate()
        .map(|(index, placement)| {
            (0..2)
                .map(|vpol| {
                    let weight = f64::from(weights[index * 2 + vpol] as f32);
                    // The adjoint routes vpol through the direct (w > 0) or
                    // conjugate (w ≤ 0) table: the plane the forward
                    // transform of the mirrored row would use.
                    let mirrored = Placement {
                        w: -placement.w,
                        ..*placement
                    };
                    weight * operator.prediction_norm(&mirrored, vpol).norm()
                })
                .sum::<f64>()
        })
        .sum::<f64>();
    assert!(
        (sumwt[0] - expected).abs() <= 1.0e-9 * expected,
        "{} vs {expected}",
        sumwt[0]
    );
}

#[test]
fn the_bounded_cache_evicts_but_a_held_cell_stays_valid() {
    let root = tempfile::tempdir().expect("cache directory");
    let unbounded = generated(root.path(), false, usize::MAX);
    let mut hold = CellHold::new();
    let first = CfKey { group: 0, cube: 0 };
    let bytes_of_one = {
        let TapLayout::Dense { data, .. } = unbounded.taps(first, &mut hold) else {
            panic!("dense");
        };
        2 * data.len() * size_of::<Complex32>()
    };
    let bounded = AwCatalog::open_casa(
        root.path(),
        indexing(false),
        &geometry(),
        &routing(),
        bytes_of_one,
    )
    .expect("open");
    let mut held = CellHold::new();
    let first_taps = bounded.taps(first, &mut held);
    let TapLayout::Dense {
        data: first_data, ..
    } = first_taps
    else {
        panic!("dense");
    };
    let snapshot = first_data.to_vec();
    // Touching every other group evicts the first from the cache.
    let mut scratch = CellHold::new();
    for group in 1..bounded.groups() {
        let _ = bounded.taps(
            CfKey {
                group: group as u16,
                cube: 0,
            },
            &mut scratch,
        );
        let _ = bounded.weight_taps(
            CfKey {
                group: group as u16,
                cube: 0,
            },
            &mut scratch,
        );
    }
    assert!(
        bounded.resident_bytes() <= 2 * bytes_of_one,
        "resident {} over bound {bytes_of_one}",
        bounded.resident_bytes()
    );
    // The held cell is unchanged and reloading gives the same taps.
    assert_eq!(first_data, &snapshot[..]);
    let mut again = CellHold::new();
    let TapLayout::Dense { data, .. } = bounded.taps(first, &mut again) else {
        panic!("dense");
    };
    assert_eq!(data, &snapshot[..]);
}

/// `AWProjectFT::getImage` and `getWeightImage` divide the images by the
/// sampling sinc `sin(x)/x`, `x = π(i − n/2)/(n·sampling)`, 2.6 % at the
/// grid edge for sampling 4; `initializeToVis` resets its `sincConv` to
/// one, so the model side is uncorrected.
#[test]
fn the_image_correction_is_the_sampling_sinc_and_the_model_is_uncorrected() {
    let root = tempfile::tempdir().expect("cache directory");
    let catalog = generated(root.path(), false, usize::MAX);
    let [nx, ny] = geometry().grid_shape();
    let correction = catalog.image_correction();
    let sinc = |len: usize, index: usize| {
        let x = std::f64::consts::PI * (index as f64 - (len / 2) as f64)
            / (len as f64 * OVERSAMPLING as f64);
        x.sin() / x
    };
    assert_eq!(correction.at(nx / 2, ny / 2), 1.0);
    let (gx, gy) = (3, ny - 5);
    assert!((correction.at(gx, gy) - 1.0 / (sinc(nx, gx) * sinc(ny, gy))).abs() < 1.0e-12);
    assert!((correction.at(0, ny / 2) - 1.0262).abs() < 1.0e-3);
    assert_eq!(correction.model_at(gx, gy), 1.0);
    assert_eq!(correction.model_at(0, 0), 1.0);
}

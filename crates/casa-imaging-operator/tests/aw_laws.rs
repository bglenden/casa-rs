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
    MeasurementOperator, Mode, ModeSet, ModelImages, ModelPlane, ModelPrescale, NativeRow,
    Placement, PlaneRange, PolarizationRouting, RowContext, SampleBuffer, SpectralResampler,
    TapLayout, WeightingGeneration, Work,
};
use common::{IMAGE, Rng, buffer, placements, samples};
use ndarray::Array2;
use num_complex::{Complex32, Complex64};

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
        original_w_m: None,
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
    // `DataToGridImpl_p` selects the input via muellerElement % nDataPol
    // after selecting the cell: for Stokes I both signs pair RR with its
    // own cell and LL with its own cell. `GridToData` keeps the output
    // hand and selects the model grid via that index, so prediction still
    // swaps the cells for w > 0. These are four independent routes (#667).
    let mueller = catalog.mueller();
    assert_eq!(mueller.table(true, false), &[vec![Some(0), Some(1)]]);
    assert_eq!(mueller.table(false, false), &[vec![Some(0), Some(1)]]);
    assert_eq!(mueller.table(true, true), &[vec![Some(1), Some(0)]]);
    assert_eq!(mueller.table(false, true), &[vec![Some(0), Some(1)]]);
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
fn prediction_uses_original_ms_w_while_gridding_uses_rotated_w() {
    let root = tempfile::tempdir().expect("cache directory");
    let catalog = generated(root.path(), false, usize::MAX);
    let frequency = FREQUENCIES_HZ[0];
    let scale = frequency / 299_792_458.0;
    // AWVisResampler.cc:326 uses rotated UVW; :493 uses vb_p->uvw().
    // Separate index changes from sign changes, including the zero branch.
    for (rotated_w, original_w, grid_cell, prediction_cell) in [
        (700.0, 0.0, 1, 0),
        (0.0, 700.0, 0, 1),
        (700.0, -700.0, 1, 1),
        (-700.0, 700.0, 1, 1),
    ] {
        let row = RowContext {
            original_w_m: Some(original_w / scale),
            ..context(17.0, 0)
        };
        let key = catalog.key(&row, frequency, rotated_w);
        assert_eq!(usize::from(key.group), catalog.group_index(0, 0, grid_cell));
        assert_eq!(
            usize::from(key.cube),
            catalog.group_index(0, 0, prediction_cell)
        );
    }
    let operator = operator_with(catalog);
    let resampler = SpectralResampler::direct(Basis::Constant).expect("resampler");
    let row = NativeRow {
        uvw_m: [0.0, 0.0, 700.0 / scale],
        phase_shift_m: 0.0,
        pointing_offset_rad: [0.0; 2],
        frequencies_hz: &[frequency],
        values: &[Complex32::new(1.0, 0.0); 2],
        weights: &[1.0; 2],
        flags: &[false; 2],
        row_flag: false,
        context: RowContext {
            original_w_m: Some(-1.0 / scale),
            ..context(17.0, 0)
        },
    };
    let mut buffer = SampleBuffer::new(2);
    resampler
        .place(
            &operator,
            &WeightingGeneration::Natural { taper: None },
            &row,
            &mut buffer,
        )
        .expect("place");
    let [placed] = buffer.placements() else {
        panic!("one sample")
    };
    assert!(placed.w > 0.0);
    assert_eq!(placed.prediction_w_positive, Some(false));
    assert_ne!(placed.cf.group, placed.cf.cube);
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
    // `DataToGridImpl_p` pairs each input with its own cell for Stokes I,
    // on both signs of w. sumwt follows W·|Σ taps| of that cell.
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
                    let mut hold = CellHold::new();
                    let taps = operator.cf().taps(placement.cf, &mut hold);
                    let location =
                        operator
                            .geometry()
                            .locate(placement.u, placement.v, taps.oversampling());
                    weight * taps.norm(location, vpol as u8, placement.w > 0.0).norm()
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

/// #667: exercise the data operands as well as the cell map. PSF-only or
/// equal-hand data cannot expose selection of the wrong input visibility.
#[test]
fn unequal_hands_follow_casa_in_both_directions_and_on_both_w_signs() {
    let widen = |value: Complex32| Complex64::new(f64::from(value.re), f64::from(value.im));
    let root = tempfile::tempdir().expect("cache directory");
    AwCatalog::generate_native(root.path(), &request(false), false).expect("generate");
    // This tiny aperture fixture can undersample away the EVLA squint.
    // Give LL a distinct spatial profile so a hand swap cannot pass just
    // because both cells happen to agree (the blind spot in #667).
    for entry in std::fs::read_dir(root.path()).expect("cells") {
        let path = entry.expect("entry").path();
        let name = path.file_name().expect("name").to_string_lossy();
        if name.starts_with("CFS_") && name.ends_with("_1.im") {
            let mut image = casa_images::PagedImage::<Complex32>::open(&path).expect("cell");
            let mut pixels = image.get().expect("pixels");
            let width = pixels.shape()[0] as f32;
            let height = pixels.shape()[1] as f32;
            for (index, value) in pixels.indexed_iter_mut() {
                let x = (index[0] as f32 - width / 2.0 - 1.0) / OVERSAMPLING as f32;
                let y = (index[1] as f32 - height / 2.0) / OVERSAMPLING as f32;
                *value += Complex32::new(0.05, 0.02) * (-0.5 * (x * x + y * y)).exp();
            }
            image.put_slice(&pixels, &[0, 0, 0, 0]).expect("write cell");
            image.save().expect("save cell");
        }
    }
    let values = [Complex32::new(2.0, -0.5), Complex32::new(-0.7, 1.3)];
    for requested in [
        vec![PolarizationCoordinate::StokesI],
        vec![
            PolarizationCoordinate::CircularRr,
            PolarizationCoordinate::CircularLl,
        ],
    ] {
        let pol = PolarizationRouting::compile(&RR_LL, &requested).expect("routing");
        let pol_map = pol.pol_map().to_vec();
        let catalog =
            AwCatalog::open_casa(root.path(), indexing(false), &geometry(), &pol, usize::MAX)
                .expect("open");
        let operator = MeasurementOperator::new(
            geometry(),
            Basis::Constant,
            pol,
            Box::new(catalog),
            GridPrecision::F64,
        );
        // Distinct model planes make a wrong forward grid-row selection
        // observable; an off-centre component makes the cells distinguishable.
        let model = ModelImages {
            first_plane: 0,
            planes: vec![ModelPlane {
                images: (0..requested.len())
                    .map(|p| {
                        let mut image = Array2::zeros((IMAGE, IMAGE));
                        image[(IMAGE / 2 + 5, IMAGE / 2 - 3)] = 1.0 + p as f32;
                        image
                    })
                    .collect(),
            }],
        };
        let prepared = operator
            .prepare_model(&model, ModelPrescale::Unit)
            .expect("model");
        for (w, prediction_w) in [
            (-700.0, -700.0),
            (0.0, 0.0),
            (700.0, 700.0),
            (-700.0, 700.0),
            (700.0, -700.0),
        ] {
            let placement = Placement {
                u: 13.0,
                v: -19.0,
                w,
                prediction_w_positive: Some(prediction_w > 0.0),
                phase: 0.0,
                plane: 0,
                spectral: 0.0,
                cf: operator.cf().key(&context(17.0, 0), FREQUENCIES_HZ[0], w),
                gradient: [0.0; 2],
            };
            let mut buffer = SampleBuffer::new(2);
            buffer.push(placement, &values, &[1.0; 2]);
            let mut hold = CellHold::new();
            let TapLayout::Dense {
                data,
                support,
                oversampling,
                ..
            } = operator.cf().taps(placement.cf, &mut hold)
            else {
                panic!("dense")
            };
            let location = operator
                .geometry()
                .locate(placement.u, placement.v, oversampling);
            let [sx, sy] = support.map(usize::from);
            let fine = usize::from(location.oy) * (usize::from(oversampling) + 1)
                + usize::from(location.ox);
            let tap = |m: usize, ix: usize, iy: usize| {
                widen(data[(fine * 2 + m) * sx * sy + iy * sx + ix])
            };
            assert!(
                (0..sy).any(|iy| (0..sx).any(|ix| (tap(0, ix, iy) - tap(1, ix, iy)).norm() > 1e-6)),
                "the oracle needs distinguishable polarization cells"
            );
            let cell = |ix: usize, iy: usize| {
                (location.y as usize + iy - sy / 2) * IMAGE + location.x as usize + ix - sx / 2
            };
            let mut expected_grid = vec![Complex64::default(); IMAGE * IMAGE * requested.len()];
            let mut expected_sumwt = vec![0.0; requested.len()];
            let mut expected_prediction = [Complex64::default(); 2];
            // Direct transcription of AWVisResampler's outer-ipol loop:
            // getConvFunc_p selects the cell, then muellerElement % 2
            // selects the INPUT operand, not the output being filled.
            for outer in 0..2 {
                let gpol = usize::from(pol_map[outer].expect("mapped"));
                let m = if w > 0.0 { outer } else { 1 - outer };
                let mut norm = Complex64::default();
                for iy in 0..sy {
                    for ix in 0..sx {
                        let t = tap(m, ix, iy);
                        let t = if w > 0.0 { t.conj() } else { t };
                        expected_grid[gpol * IMAGE * IMAGE + cell(ix, iy)] += widen(values[m]) * t;
                        norm += t;
                    }
                }
                expected_sumwt[gpol] += norm.norm();

                let m = if prediction_w > 0.0 { 1 - outer } else { outer };
                let input_grid = usize::from(pol_map[m].expect("mapped"));
                let mut norm = Complex64::default();
                for iy in 0..sy {
                    for ix in 0..sx {
                        let t = tap(m, ix, iy);
                        let t = if prediction_w <= 0.0 { t.conj() } else { t };
                        expected_prediction[outer] +=
                            t * prepared.block::<f64>(0, input_grid, 0)[cell(ix, iy)];
                        norm += t;
                    }
                }
                expected_prediction[outer] /= norm;
            }
            let (actual, sumwt) = grid(&operator, &buffer, Mode::Data);
            for (actual, expected) in actual.iter().zip(&expected_grid) {
                assert!(
                    (widen(*actual) - expected).norm() < 1e-6,
                    "w {w}: grid {actual} vs {expected}"
                );
            }
            for (actual, expected) in sumwt.iter().zip(expected_sumwt) {
                assert!((actual - expected).abs() < 1e-10);
            }
            let mut predicted = [Complex32::default(); 2];
            CpuBackend::new()
                .apply(
                    &buffer.block(),
                    operator.cf(),
                    Work::Predict {
                        model: &prepared,
                        out: &mut predicted,
                    },
                )
                .expect("predict");
            for (actual, expected) in predicted.into_iter().zip(expected_prediction) {
                assert!(
                    (widen(actual) - expected).norm() < 1e-6,
                    "w {w}: prediction {actual} vs {expected}"
                );
            }
            // A fused residual must predict using original-W parity and
            // spread using rotated-W parity in the same dispatch.
            let mut residual_buffer = SampleBuffer::new(2);
            residual_buffer.push(
                placement,
                &[values[0] - predicted[0], values[1] - predicted[1]],
                &[1.0; 2],
            );
            let (expected, _) = grid(&operator, &residual_buffer, Mode::Data);
            let mut acc = operator.accumulator(PlaneRange::single(0), None, ModeSet::DATA);
            CpuBackend::new()
                .apply(
                    &buffer.block(),
                    operator.cf(),
                    Work::ResidualGrid {
                        model: &prepared,
                        acc: &mut acc,
                        residual_out: None,
                    },
                )
                .expect("fused residual");
            for (actual, expected) in <f64 as GridScalar>::cells(acc.storage())
                .iter()
                .zip(expected)
            {
                assert!((*actual - widen(expected)).norm() < 1e-6);
            }
        }
    }
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

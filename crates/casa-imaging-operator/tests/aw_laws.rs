// SPDX-License-Identifier: LGPL-3.0-or-later
//! T0 laws of the AW catalog: native generation writes CASA-format cells
//! that open cold and warm to identical keys, taps and gridded products;
//! CASA's w, frequency (with the conjugate-beam map) and parallactic-angle
//! cell rules; the swapped Mueller tables of the conjugate baseline (the
//! CASA counterexample to the adjoint identity); the kernel-sum
//! normalisation; and the bounded cache that keeps a held cell alive.

mod common;

use casa_imaging_model::{
    EvlaDishSurface, NativeAwFrequencyGroup, NativeAwGrid, NativeAwRequestInput, NativeAwTerms,
    PolarizationCoordinate,
};
use casa_imaging_operator::{
    AwCatalog, AwIndexing, Basis, CellHold, CfKey, ConvolutionFunctionSet, CpuBackend, GridBackend,
    GridGeometry, GridPadding, GridPrecision, GridScalar, ImageExtent, KernelNormalisation,
    MeasurementOperator, Mode, ModeSet, ModelImages, ModelPlane, ModelPrescale, NativeCacheLock,
    NativeCacheState, NativeManifest, NativeRow, Placement, PlaneRange, PolarizationRouting,
    RowContext, SampleBuffer, SpectralResampler, TapLayout, WeightingGeneration, Work,
};
use casa_types::Correlation;
use common::{IMAGE, Rng, buffer, placements, samples};
use ndarray::Array2;
use num_complex::{Complex32, Complex64};

const RR_LL: [Correlation; 2] = [Correlation::RR, Correlation::LL];
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
        original_uvw_m: None,
        time_s: 0.0,
        antennas: [0, 1],
        antenna_types: [0, 0],
        // CASA's operator angle is the negative of the physical angle.
        parallactic_angle_rad: [-pa_deg.to_radians(); 2],
        field: 0,
        spectral_window,
    }
}

/// Generate a cache into `root` on four workers and open it.
fn generated(root: &std::path::Path, conjugate_beams: bool, bound: usize) -> AwCatalog {
    AwCatalog::generate_native(root, &request(conjugate_beams), false, 4).expect("generate");
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
        placement.prediction_w_positive = placement.w > 0.0;
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
            original_uvw_m: Some([0.0, 0.0, original_w / scale]),
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
            original_uvw_m: Some([0.0, 0.0, -1.0 / scale]),
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
    assert!(!placed.prediction_w_positive);
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
            prediction_w_positive: -placement.w > 0.0,
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
    AwCatalog::generate_native(root.path(), &request(false), false, 1).expect("generate");
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
                prediction_w_positive: prediction_w > 0.0,
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
        let TapLayout::Dense { data, norms, .. } = unbounded.taps(first, &mut hold) else {
            panic!("dense");
        };
        2 * (size_of_val(data) + size_of_val(norms))
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

/// `AWProjectWBFT::makeWBSensitivityImage` publishes `abs` of the
/// polarization-averaged weight transform. Samples all below the reference
/// frequency make the first Taylor weight moment Σ W·s·K negative; it is
/// published as its magnitude, |s| times moment 0 (#704).
#[test]
fn an_aw_taylor_weight_moment_publishes_its_magnitude() {
    let mut rng = Rng::new(107);
    let root = tempfile::tempdir().expect("cache directory");
    let operator = MeasurementOperator::new(
        geometry(),
        Basis::Taylor {
            terms: 2,
            reference_hz: REFERENCE_HZ,
        },
        routing(),
        Box::new(generated(root.path(), true, usize::MAX)),
        GridPrecision::F64,
    );
    let spectral = ((FREQUENCIES_HZ[0] - REFERENCE_HZ) / REFERENCE_HZ) as f32;
    let mut placed = placements(operator.geometry(), 40, 1, &mut rng);
    for placement in &mut placed {
        placement.w = 0.0;
        placement.prediction_w_positive = false;
        placement.spectral = spectral;
        placement.cf = operator.cf().key(&context(17.0, 0), FREQUENCIES_HZ[0], 0.0);
        placement.u *= 0.7;
        placement.v *= 0.7;
    }
    placed.retain(|placement| fits(&operator, placement));
    assert!(placed.len() >= 20, "{} placements fit", placed.len());
    let (values, weights) = samples(&placed, 2, &mut rng);
    let block = buffer(&placed, &values, &weights, 2);
    let mut acc = operator.accumulator(
        PlaneRange::single(0),
        None,
        ModeSet {
            data: false,
            psf: false,
            weight: true,
        },
    );
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
    let zeroth = normal.weight(0, 0, 0).expect("moment 0");
    let first = normal.weight(0, 1, 0).expect("moment 1");
    let s = f64::from(spectral);
    let ratio = normal.weight_sumwt(0, 1, 0) / normal.weight_sumwt(0, 0, 0);
    assert!((ratio - s).abs() < 1.0e-6, "sumwt ratio {ratio}, s {s}");
    let peak = zeroth
        .iter()
        .fold(0.0_f64, |peak, value| peak.max(f64::from(*value).abs()));
    for (first, zeroth) in first.iter().zip(zeroth) {
        assert!(*first >= 0.0, "a published weight moment {first}");
        let expected = s.abs() * f64::from(*zeroth);
        assert!(
            (f64::from(*first) - expected).abs() <= 1.0e-5 * peak,
            "moment 1 {first}, |s| × moment 0 {expected}"
        );
    }
}

/// Every group's key: its gridding and prediction group alike.
fn group_keys(catalog: &AwCatalog) -> Vec<CfKey> {
    (0..catalog.groups())
        .map(|group| CfKey {
            group: group as u16,
            cube: group as u16,
        })
        .collect()
}

/// [`request`] with only its first frequency group: a subset selection.
fn subset_request() -> NativeAwRequestInput {
    let mut subset = request(true);
    subset.frequencies.truncate(1);
    subset
}

/// Every group's imaging and weight taps.
fn all_taps(catalog: &AwCatalog) -> Vec<(Vec<Complex32>, Vec<Complex32>)> {
    let dense = |taps: TapLayout<'_>| match taps {
        TapLayout::Dense { data, .. } => data.to_vec(),
        TapLayout::SeparableReal { .. } => panic!("AW cells are dense"),
    };
    (0..catalog.groups())
        .map(|group| {
            let key = CfKey {
                group: group as u16,
                cube: group as u16,
            };
            let (mut a, mut b) = (CellHold::new(), CellHold::new());
            (
                dense(catalog.taps(key, &mut a)),
                dense(catalog.weight_taps(key, &mut b).expect("weight cell")),
            )
        })
        .collect()
}

/// #744: a region router reads a key's gridding half support and
/// oversampling from the catalog's index, without loading the group or
/// taking the cache's lock. For every group they are the loaded imaging
/// taps', and asking for them loads nothing.
#[test]
fn the_gridding_metadata_is_the_loaded_taps_and_loads_nothing() {
    let root = tempfile::tempdir().expect("cache directory");
    let catalog = generated(root.path(), true, usize::MAX);
    let keys = group_keys(&catalog);
    let metadata = keys
        .iter()
        .map(|key| {
            (
                catalog.gridding_half_support(*key),
                catalog.gridding_oversampling(*key),
            )
        })
        .collect::<Vec<_>>();
    assert_eq!(catalog.loads(), 0, "the metadata loads no group");
    assert_eq!(catalog.resident_bytes(), 0);
    for (key, (half_support, oversampling)) in keys.iter().zip(metadata) {
        let mut hold = CellHold::new();
        let taps = catalog.taps(*key, &mut hold);
        assert_eq!(taps.half_support(), half_support, "group {}", key.group);
        assert_eq!(taps.oversampling(), oversampling, "group {}", key.group);
    }
}

/// #744: groups are read outside the cache's lock, and each once. Workers
/// that fetch every group at the same moment, in the same order, read each
/// group from disk one time and share its cells.
#[test]
fn concurrent_fetches_load_each_group_once() {
    const WORKERS: usize = 8;
    let root = tempfile::tempdir().expect("cache directory");
    let catalog = generated(root.path(), true, usize::MAX);
    let keys = group_keys(&catalog);
    let start = std::sync::Barrier::new(WORKERS);
    let seen = std::thread::scope(|scope| {
        let workers = (0..WORKERS)
            .map(|_| {
                let (catalog, keys, start) = (&catalog, &keys, &start);
                scope.spawn(move || {
                    start.wait();
                    keys.iter()
                        .map(|key| {
                            let mut hold = CellHold::new();
                            let TapLayout::Dense { data, .. } = catalog.taps(*key, &mut hold)
                            else {
                                panic!("AW cells are dense");
                            };
                            data.as_ptr() as usize
                        })
                        .collect::<Vec<_>>()
                })
            })
            .collect::<Vec<_>>();
        workers
            .into_iter()
            .map(|worker| worker.join().expect("worker"))
            .collect::<Vec<_>>()
    });
    assert_eq!(catalog.loads(), keys.len() as u64, "each group read once");
    assert!(
        seen.iter().all(|cells| *cells == seen[0]),
        "every worker shares each group's cells"
    );
}

fn open(root: &std::path::Path) -> AwCatalog {
    AwCatalog::open_casa(root, indexing(true), &geometry(), &routing(), usize::MAX).expect("open")
}

/// Every file under `root`, by its path below `root`, with its bytes.
fn files(root: &std::path::Path) -> std::collections::BTreeMap<std::path::PathBuf, Vec<u8>> {
    let mut found = std::collections::BTreeMap::new();
    let mut directories = vec![root.to_path_buf()];
    while let Some(directory) = directories.pop() {
        for entry in std::fs::read_dir(&directory).expect("list") {
            let path = entry.expect("entry").path();
            if path.is_dir() {
                directories.push(path);
            } else {
                let name = path.strip_prefix(root).expect("below root").to_path_buf();
                found.insert(name, std::fs::read(&path).expect("read"));
            }
        }
    }
    found
}

/// #708: generation on several workers writes the cache one worker writes,
/// byte for byte: the same images, cell for cell, and the same manifest.
/// Filling the missing half on several workers does too.
#[test]
fn several_workers_write_the_cache_one_worker_writes() {
    let serial = tempfile::tempdir().expect("cache directory");
    AwCatalog::generate_native(serial.path(), &request(true), false, 1).expect("generate");
    let expected = files(serial.path());
    assert!(expected.len() > 8, "a catalog of several cells");
    for workers in [2, 3, 8] {
        let parallel = tempfile::tempdir().expect("cache directory");
        AwCatalog::generate_native(parallel.path(), &request(true), false, workers)
            .expect("generate");
        assert!(
            files(parallel.path()) == expected,
            "{workers} workers wrote another cache"
        );
    }
    let filled = tempfile::tempdir().expect("cache directory");
    AwCatalog::generate_native(filled.path(), &request(true), false, 1).expect("generate");
    for entry in std::fs::read_dir(filled.path()).expect("list") {
        let path = entry.expect("entry").path();
        let name = path
            .file_name()
            .expect("name")
            .to_string_lossy()
            .into_owned();
        if name.starts_with("CFS_") && name.ends_with("_1_0.im") {
            std::fs::remove_dir_all(&path).expect("remove a cell");
        }
    }
    AwCatalog::generate_native(filled.path(), &request(true), true, 4).expect("fill");
    assert!(
        files(filled.path()) == expected,
        "a fill on four workers wrote another cache"
    );
}

/// #735: a cell's conjugate beam is the request's listed frequency nearest
/// `√(2f₀² − f²)`, so a cache generated for a wider SPW selection names the
/// subset's cells and holds different ones. It is neither reusable nor
/// fillable for the subset. Regenerated, it serves exactly what a fresh
/// cache does.
#[test]
fn a_cache_of_another_frequency_list_is_regenerated_not_reused() {
    let root = tempfile::tempdir().expect("cache directory");
    AwCatalog::generate_native(root.path(), &request(true), false, 1).expect("generate");
    let subset = subset_request();
    let state = AwCatalog::native_cache_state(root.path(), &subset).expect("state");
    // 1 PA × 2 w × 2 Mueller cells for the subset's one frequency, and as
    // many cells (two images each) of the other frequency.
    assert_eq!(
        state,
        NativeCacheState {
            manifest: NativeManifest::Differs,
            present: 4,
            expected: 4,
            foreign: 8,
        }
    );
    assert!(!state.reusable() && !state.fillable());
    assert!(AwCatalog::generate_native(root.path(), &subset, true, 3).is_err());

    // The hazard is real: the 1.4 GHz cell's conjugate beam is 1.6 GHz in
    // the wide cache and 1.4 GHz in the subset's.
    let wide = all_taps(&open(root.path()));
    let fresh_root = tempfile::tempdir().expect("cache directory");
    AwCatalog::generate_native(fresh_root.path(), &subset, false, 1).expect("generate");
    let fresh = all_taps(&open(fresh_root.path()));
    assert_ne!(wide[0].1, fresh[0].1, "the reused weight cell differs");

    AwCatalog::clear_native(root.path()).expect("clear");
    AwCatalog::generate_native(root.path(), &subset, false, 1).expect("regenerate");
    let state = AwCatalog::native_cache_state(root.path(), &subset).expect("state");
    assert!(state.reusable(), "{state}");
    let catalog = open(root.path());
    assert_eq!(
        catalog.frequencies_hz(),
        open(fresh_root.path()).frequencies_hz()
    );
    assert_eq!(all_taps(&catalog), fresh);
}

/// A directory without a request manifest is reused or filled in only when
/// it holds no cell: cells of unknown provenance are regenerated.
#[test]
fn only_an_empty_directory_without_a_manifest_is_filled_in() {
    let subset = subset_request();
    let root = tempfile::tempdir().expect("cache directory");
    let empty = AwCatalog::native_cache_state(root.path(), &subset).expect("state");
    assert_eq!(empty.manifest, NativeManifest::Absent);
    assert!(empty.fillable() && !empty.reusable());
    AwCatalog::generate_native(root.path(), &subset, true, 3).expect("fill an empty directory");
    assert!(
        AwCatalog::native_cache_state(root.path(), &subset)
            .expect("state")
            .reusable()
    );

    std::fs::remove_file(root.path().join("CasaRsNativeRequest.txt")).expect("drop manifest");
    let unknown = AwCatalog::native_cache_state(root.path(), &subset).expect("state");
    assert_eq!(unknown.manifest, NativeManifest::Absent);
    assert_eq!(unknown.present, unknown.expected);
    assert!(!unknown.reusable() && !unknown.fillable());
}

/// Every input that enters a cell is in the manifest: here the
/// conjugate-beam switch and the image reference frequency.
#[test]
fn the_manifest_records_the_terms_and_the_reference_frequency() {
    let root = tempfile::tempdir().expect("cache directory");
    AwCatalog::generate_native(root.path(), &request(true), false, 1).expect("generate");
    let manifest = |input: &NativeAwRequestInput| {
        AwCatalog::native_cache_state(root.path(), input)
            .expect("state")
            .manifest
    };
    assert_eq!(manifest(&request(true)), NativeManifest::Matches);
    assert_eq!(manifest(&request(false)), NativeManifest::Differs);
    let mut shifted = request(true);
    // One unit in the last place.
    shifted.reference_frequency_hz = f64::from_bits(shifted.reference_frequency_hz.to_bits() + 1);
    assert_eq!(manifest(&shifted), NativeManifest::Differs);
}

/// Entries of `root` under the staging prefix: half-written cell images.
fn staged(root: &std::path::Path) -> Vec<String> {
    std::fs::read_dir(root)
        .expect("list")
        .map(|entry| {
            entry
                .expect("entry")
                .file_name()
                .to_string_lossy()
                .into_owned()
        })
        .filter(|name| name.starts_with(".staging-"))
        .collect()
}

/// #739 review: generation that fails before its first cell leaves no
/// manifest and no old cell. Neither the failing request nor the cache's
/// previous one then reads as reusable, so no deployment can accept the old
/// cells for the new request; the empty directory is simply generated
/// again.
#[test]
fn generation_that_fails_leaves_no_request_claimed() {
    let root = tempfile::tempdir().expect("cache directory");
    let subset = subset_request();
    AwCatalog::generate_native(root.path(), &subset, false, 1).expect("generate");
    assert!(staged(root.path()).is_empty(), "complete images only");
    // A flat dish passes validation but its aperture normalises to nothing.
    let mut failing = subset_request();
    failing.surface =
        EvlaDishSurface::new((0..=125).map(|i| [f64::from(i) / 10.0, 0.0, 0.0]).collect())
            .expect("a valid surface");
    assert!(AwCatalog::generate_native(root.path(), &failing, false, 2).is_err());
    for input in [&failing, &subset] {
        let state = AwCatalog::native_cache_state(root.path(), input).expect("state");
        assert_eq!(
            (state.manifest, state.present, state.foreign),
            (NativeManifest::Absent, 0, 0),
            "{state}"
        );
        assert!(!state.reusable(), "{state}");
    }
    AwCatalog::generate_native(root.path(), &subset, true, 3).expect("fill the emptied cache");
    assert!(
        AwCatalog::native_cache_state(root.path(), &subset)
            .expect("state")
            .reusable()
    );
}

/// #739 review: a cell image is published under its name only once
/// complete, so a write that stops leaves it under its staging name. That
/// cell counts as missing, whether the imaging or the weight image stopped,
/// a fill writes it again, and the catalog then opens and serves exactly
/// what a fresh cache does.
#[test]
fn a_half_written_cell_counts_as_missing_and_is_filled_in() {
    let subset = subset_request();
    let fresh = tempfile::tempdir().expect("cache directory");
    AwCatalog::generate_native(fresh.path(), &subset, false, 1).expect("generate");
    for name in ["CFS_0_0_CF_0_1_0.im", "WTCFS_0_0_CF_0_1_1.im"] {
        let root = tempfile::tempdir().expect("cache directory");
        AwCatalog::generate_native(root.path(), &subset, false, 1).expect("generate");
        let staging = root.path().join(format!(".staging-{name}"));
        std::fs::rename(root.path().join(name), &staging).expect("stage");
        std::fs::remove_file(staging.join("table.dat")).expect("break the staged image");
        let state = AwCatalog::native_cache_state(root.path(), &subset).expect("state");
        assert_eq!(
            (state.manifest, state.present, state.expected, state.foreign),
            (NativeManifest::Matches, 3, 4, 0),
            "{name}"
        );
        assert!(!state.reusable() && state.fillable());
        AwCatalog::generate_native(root.path(), &subset, true, 3).expect("fill");
        let state = AwCatalog::native_cache_state(root.path(), &subset).expect("state");
        assert!(state.reusable(), "{name}: {state}");
        assert!(staged(root.path()).is_empty(), "{name}");
        assert_eq!(
            all_taps(&open(root.path())),
            all_taps(&open(fresh.path())),
            "{name}"
        );
    }
}

/// #739 review: readers share a cache, and a writer needs it alone, so a
/// cache is never replaced under a run that reads it nor read while it is
/// written. Neither lock waits.
#[test]
fn readers_share_a_cache_and_a_writer_needs_it_alone() {
    let root = tempfile::tempdir().expect("cache directory");
    let first = NativeCacheLock::shared(root.path()).expect("shared");
    let second = NativeCacheLock::shared(root.path()).expect("readers share");
    assert!(NativeCacheLock::exclusive(root.path()).is_err());
    drop((first, second));
    let writer = NativeCacheLock::exclusive(root.path()).expect("alone");
    assert!(NativeCacheLock::shared(root.path()).is_err());
    assert!(NativeCacheLock::exclusive(root.path()).is_err());
    drop(writer);
    NativeCacheLock::shared(root.path()).expect("free again");
}

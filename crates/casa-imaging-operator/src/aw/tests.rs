// SPDX-License-Identifier: LGPL-3.0-or-later

use super::*;
use num_complex::Complex64;

#[test]
fn casa_routes_the_selected_cell_to_its_visibility_or_grid_hand() {
    use Correlation as C;
    use casa_imaging_model::PolarizationCoordinate as P;

    let parallel = [C::RR, C::LL];
    let pol = PolarizationRouting::compile(&parallel, &[P::CircularRr, P::CircularLl])
        .expect("parallel hands");
    let routes = routing(&[0, 15], &pol).expect("routes");
    let diagonal = vec![vec![Some(0), None], vec![None, Some(1)]];
    // `DataToGrid` reads the visibility of the selected cell's hand
    // (`muellerElement % nDataPol`), `GridToData` the model grid of
    // that hand; the outer loop names the output in both.
    assert_eq!(routes.table(true, false), &diagonal);
    assert_eq!(routes.table(false, true), &diagonal);
    assert_eq!(
        routes.table(false, false),
        &[vec![None, Some(1)], vec![Some(0), None]]
    );
    assert_eq!(
        routes.table(true, true),
        &[vec![None, Some(0)], vec![Some(1), None]]
    );

    // PolOuterProduct::makeConjPol2CFMat: an inactive partner leaves
    // the direct mapping in place, even if the cache has both cells.
    for (requested, cells, expected) in [
        (P::CircularRr, vec![0, 15], vec![Some(0), None]),
        (P::CircularLl, vec![0, 15], vec![None, Some(1)]),
        (P::CircularRr, vec![0], vec![Some(0), None]),
    ] {
        let pol = PolarizationRouting::compile(&parallel, &[requested]).expect("one hand");
        let routes = routing(&cells, &pol).expect("one-hand routes");
        for positive in [false, true] {
            for forward in [false, true] {
                assert_eq!(
                    routes.table(positive, forward),
                    std::slice::from_ref(&expected)
                );
            }
        }
    }

    let correlations = [C::RR, C::RL, C::LR, C::LL];
    let pol = PolarizationRouting::compile(
        &correlations,
        &[P::CircularRr, P::CircularRl, P::CircularLr, P::CircularLl],
    )
    .expect("four hands");
    let routes = routing(&[0, 5, 10, 15], &pol).expect("four-hand routes");
    for gpol in 0..4 {
        for vpol in 0..4 {
            assert_eq!(
                routes.table(true, false)[gpol][vpol],
                (gpol == vpol).then_some(vpol as u8)
            );
            assert_eq!(
                routes.table(false, false)[gpol][vpol],
                (gpol + vpol == 3).then_some(vpol as u8)
            );
            assert_eq!(
                routes.table(true, true)[gpol][vpol],
                (gpol + vpol == 3).then_some(gpol as u8)
            );
            assert_eq!(
                routes.table(false, true)[gpol][vpol],
                (gpol == vpol).then_some(gpol as u8)
            );
        }
    }
    let pol = PolarizationRouting::compile(&[C::RR, C::RL, C::LL], &[P::StokesI])
        .expect("three correlations");
    assert!(
        routing(&[0, 15], &pol).is_none(),
        "do not overwrite colliding routes"
    );
}

#[test]
fn a_group_holds_each_kinds_largest_half_support() {
    let header = |support: [u16; 2]| CellHeader {
        path: PathBuf::new(),
        shape: [16, 16],
        sampling: 1,
        support,
        mueller: 0,
        w_value: 0.0,
        working_field_rad: None,
        w_increment: 0.0,
        pa_deg: 0.0,
        frequency_hz: 1.0e9,
        rotatable: false,
    };
    let group = |cells: &[([u16; 2], [u16; 2])]| {
        Group::new(
            &cells
                .iter()
                .map(|(imaging, weight)| CellFiles {
                    imaging: header(*imaging),
                    weight: header(*weight),
                })
                .collect::<Vec<_>>(),
            [false; 2],
        )
    };
    let wide_imaging = group(&[([6, 6], [4, 4]), ([5, 6], [4, 3])]);
    assert_eq!(wide_imaging.imaging_half_support, [6, 6]);
    assert_eq!(wide_imaging.weight_half_support, [4, 4]);
}

/// A catalog with only the group index allocated, for checking the
/// retained charge and the public kernel-set lookup independently.
fn catalog_with_groups(groups: Vec<Group>) -> AwCatalog {
    AwCatalog {
        root: PathBuf::new(),
        prediction_frequency_scale: 1.0,
        prediction_uses_native_frequency: false,
        indexing: AwIndexing {
            conjugate_beams: false,
            image_reference_hz: 1.0e9,
            pa_tolerance_rad: 360.0_f32.to_radians(),
            feed_zero_angles_rad: None,
            rotation: None,
        },
        pa_deg: Vec::new(),
        frequencies_hz: Vec::new(),
        w_values: Vec::new(),
        w_increment: 0.0,
        mueller_elements: vec![0],
        cells: Vec::new(),
        header_opening_bytes: 0,
        groups,
        rotations: Vec::new(),
        oversampling: 2,
        max_half_support: [0, 0],
        mueller: MuellerRouting::scalar(&[Some(0)], 1),
        correction: ImageCorrection::new(Vec::new(), Vec::new()),
        lru: Mutex::new(Lru {
            slots: Vec::new(),
            clock: 0,
            bytes: 0,
            loads: 0,
        }),
        bound: CacheBound::new(0),
        load_ended: Condvar::new(),
    }
}

/// The catalog's retained charge covers the origin selector, prediction
/// scale and every allocated group slot, including spare capacity.
#[test]
fn catalog_charge_counts_its_origin_selector_and_prediction_scale() {
    let groups = Vec::<Group>::with_capacity(41);
    let per_group = 2 * size_of::<[u16; 2]>() + size_of::<[bool; 2]>();
    let allocated = groups.capacity() * per_group;
    let mut catalog = catalog_with_groups(groups);
    catalog.indexing.feed_zero_angles_rad = Some(Arc::new([0.0_f32, 0.1]));
    let feed_bytes = 2 * size_of::<f32>() + 2 * size_of::<usize>();
    assert_eq!(
        catalog.cache_charge().kept,
        (allocated + feed_bytes + size_of::<f64>() + size_of::<bool>() + size_of::<usize>()) as u64
            + (catalog.mueller_elements.capacity() * size_of::<u32>()) as u64
            + catalog.mueller.bytes()
            + catalog.correction.resident_bytes()
    );
}

/// Each PA and tap kind uses its buffer's maximum width, independent
/// of the first cell or the previous buffer (#1070).
#[test]
fn phase_origin_uses_the_selected_pa_buffer_and_tap_kind() {
    let cell = |imaging: usize, weight: usize| {
        let header = |width| CellHeader {
            path: PathBuf::new(),
            shape: [width, width],
            sampling: 2,
            support: [1, 1],
            mueller: 0,
            w_value: 0.0,
            working_field_rad: None,
            w_increment: 0.0,
            pa_deg: 0.0,
            frequency_hz: 1.0e9,
            rotatable: false,
        };
        CellFiles {
            imaging: header(imaging),
            weight: header(weight),
        }
    };
    let cells = [
        cell(11, 12),
        cell(12, 13),
        cell(13, 14),
        cell(15, 16),
        cell(11, 12),
        cell(12, 13),
        cell(14, 15),
        cell(16, 17),
    ];
    let catalog = catalog_with_groups(index_groups(&cells, 2, 2));
    for (group, imaging) in [(0, true), (1, true), (2, false), (3, false)] {
        let key = CfKey { group, cube: 0 };
        assert_eq!(catalog.pointing_origin_shift(key, false), imaging);
        assert_eq!(catalog.pointing_origin_shift(key, true), !imaging);
    }
}

/// The cache chooses the first overlapping PA and rejects a point at
/// the tolerance boundary. Lookup adds feed 0 for the row's window.
#[test]
fn imported_pa_uses_the_first_strictly_covered_buffer() {
    let mut catalog = catalog_with_groups(Vec::new());
    catalog.pa_deg = vec![-10.0, -4.0, 4.0];
    catalog.indexing.pa_tolerance_rad = 5.0_f32.to_radians();
    catalog.indexing.feed_zero_angles_rad = Some(Arc::new([0.0, 6.0_f32.to_radians()]));
    let mut row = RowContext {
        native_frequency_ratio: 1.0,
        original_uvw_m: None,
        time_s: 0.0,
        antennas: [1, 2],
        antenna_types: [0, 0],
        parallactic_angle_rad: [6.0_f64.to_radians(); 2],
        field: 0,
        spectral_window: 0,
    };
    assert_eq!(catalog.row_pa_cell(&row), Some(0), "first overlapping PA");
    row.spectral_window = 1;
    assert_eq!(
        catalog.row_pa_cell(&row),
        Some(1),
        "feed 0 in the selected window"
    );
    catalog.pa_deg = vec![0.0];
    let edge = catalog.indexing.pa_tolerance_rad;
    assert_eq!(catalog.pa_cell(edge.next_down()), Some(0));
    assert_eq!(catalog.pa_cell(edge), None);
    assert_eq!(catalog.pa_cell(edge.next_up()), None);
    assert_eq!(catalog.pa_cell(-edge), None);
    catalog.pa_deg = vec![0.0, 4.0];
    assert_eq!(catalog.pa_cell(edge), Some(1));
    catalog.pa_deg = vec![-179.0];
    assert_eq!(catalog.pa_cell(179.0_f32.to_radians()), None);

    // CASA rounds the header to Float degrees before it makes a radian
    // quantity. Keeping the Double degrees would put this row in band.
    catalog.pa_deg = vec![53.00008538104];
    catalog.indexing.pa_tolerance_rad = 0.001;
    assert_eq!(catalog.pa_cell(0.926026_f32), None);
}

#[test]
fn dense_pa_import_keeps_the_last_paired_buffer_and_charges_discarded_paths() {
    let cell = |angle, element| {
        let header = CellHeader {
            path: PathBuf::from(format!("CFS_{angle}_{element}.im")),
            shape: [4, 4],
            sampling: 2,
            support: [1, 1],
            mueller: element,
            w_value: 0.0,
            working_field_rad: None,
            w_increment: 0.0,
            pa_deg: angle,
            frequency_hz: 1.5e9,
            rotatable: true,
        };
        CellFiles {
            imaging: header.clone(),
            weight: CellHeader {
                path: PathBuf::from(format!("WTCFS_{angle}_{element}.im")),
                ..header
            },
        }
    };
    // The 0.6° chain replaces both of its earlier members. A full
    // degree from zero is outside CASA's strict coalescing tolerance.
    let mut cells = vec![
        cell(-1.2, 0),
        cell(1.0, 0),
        cell(-0.6, 0),
        cell(0.0, 0),
        cell(0.0, 15),
        cell(178.2, 0),
        cell(178.2_f64.next_up(), 15),
    ];
    // Keep the longer, discarded path in the maximum scratch charge.
    cells[0].imaging.path.reserve(256);
    let longest = cells[0].imaging.path.capacity() + cells[0].weight.path.capacity();
    let discarded = longest + cells[2].imaging.path.capacity() + cells[2].weight.path.capacity();
    let opening = coalesce_imported_pa(&mut cells);
    let mut retained: Vec<_> = cells
        .iter()
        .map(|cell| {
            assert_eq!(cell.imaging.pa_deg, cell.weight.pa_deg);
            (cell.imaging.pa_deg, cell.imaging.mueller)
        })
        .collect();
    retained.sort_unstable_by(|a, b| a.0.total_cmp(&b.0).then(a.1.cmp(&b.1)));
    assert_eq!(
        retained,
        [
            (0.0, 0),
            (0.0, 15),
            (1.0, 0),
            (178.2_f64.next_up(), 0),
            (178.2_f64.next_up(), 15),
        ]
    );
    let mut catalog = catalog_with_groups(Vec::new());
    catalog.cells = cells;
    catalog.header_opening_bytes = opening;
    assert_eq!(opening, discarded + CELL_IMAGE_RECORD_BYTES + 4 * longest);
    assert_eq!(catalog.cache_charge().opening, opening as u64);
}

#[test]
fn rotation_epochs_key_the_current_group_and_bound_memory() {
    let header = CellHeader {
        path: PathBuf::new(),
        shape: [16, 12],
        sampling: 2,
        support: [1, 1],
        mueller: 0,
        w_value: 0.0,
        working_field_rad: None,
        w_increment: 0.0,
        pa_deg: 0.0,
        frequency_hz: 1.0e9,
        rotatable: true,
    };
    let cells = vec![CellFiles {
        imaging: header.clone(),
        weight: header,
    }];
    let mut catalog = catalog_with_groups(index_groups(&cells, 1, 1));
    catalog.cells = cells;
    catalog.frequencies_hz = vec![1.0e9];
    catalog.w_values = vec![0.0];
    catalog.lru.get_mut().unwrap().slots = vec![Slot::Empty];
    let plain = catalog.cache_charge();

    let rotation = AwPaRotation {
        step_rad: 0.125,
        require_repeatable_schedule: false,
        samples: Arc::new([(1.0, 0.0), (2.0, 0.25), (3.0, 0.5)]),
    };
    catalog.rotations = rotation.schedule(0.0).unwrap();
    catalog.indexing.rotation = Some(rotation);
    let slots = &mut catalog.lru.get_mut().unwrap().slots;
    let original_slots = slots.capacity();
    slots.resize_with(3, || Slot::Empty);
    let slot_bytes = (slots.capacity() - original_slots) * size_of::<Slot>();
    let added_index = catalog.rotations.capacity() * size_of::<(f64, f64)>()
        + 3 * size_of::<(f64, f32)>()
        + 2 * size_of::<usize>()
        + slot_bytes;
    let rotated = catalog.cache_charge();
    assert_eq!(rotated.groups, 3);
    assert_eq!(rotated.every, 3 * plain.every);
    assert_eq!(rotated.kept - plain.kept, added_index as u64);
    let plane = 16 * 12;
    let scratch = plane * (2 * size_of::<f64>() + size_of::<Complex32>());
    assert_eq!(rotated.reading - plain.reading, scratch as u64);

    let mut row = RowContext {
        native_frequency_ratio: 1.0,
        original_uvw_m: None,
        time_s: 2.0_f64.next_down(),
        antennas: [1, 2],
        antenna_types: [0, 0],
        parallactic_angle_rad: [0.0; 2],
        field: 0,
        spectral_window: 0,
    };
    assert_eq!(catalog.key(&row, 1.0e9, 0.0), CfKey { group: 0, cube: 0 });
    row.time_s = 2.0;
    assert_eq!(catalog.key(&row, 1.0e9, 0.0), CfKey { group: 1, cube: 1 });
    row.time_s = 3.0;
    assert_eq!(catalog.key(&row, 1.0e9, 0.0), CfKey { group: 2, cube: 2 });
}

#[test]
fn index_rules_follow_casa() {
    // nearestWNdx: round(sqrt(wIncr·|w|)) clamped.
    let catalog_w = |w_increment: f64, planes: usize, w: f64| {
        let index = (w_increment * w.abs()).sqrt().round() as usize;
        index.min(planes - 1)
    };
    assert_eq!(catalog_w(4.0, 5, 0.0), 0);
    assert_eq!(catalog_w(4.0, 5, 0.06), 0); // sqrt(0.24) = 0.49
    assert_eq!(catalog_w(4.0, 5, 0.07), 1); // sqrt(0.28) = 0.53
    assert_eq!(catalog_w(4.0, 5, 100.0), 4);
    // nearestValue: first at the smallest distance.
    assert_eq!(nearest(&[1.0, 2.0, 3.0], 2.5), (1, 2.0));
    assert_eq!(nearest(&[1.0, 2.0, 3.0], 2.6), (2, 3.0));
    // conjFreq.
    assert!((conjugate_frequency(1.0e9, 1.5e9) - (3.5e18_f64).sqrt()).abs() < 1.0);
    assert_eq!(mueller_element(Correlation::LL), Some(15));
    assert_eq!(mueller_element(Correlation::XY), Some(5));
}

#[test]
fn padded_cells_read_the_oversampled_plane_and_zero_outside() {
    // 8×8 plane, origin (4, 4), sampling 2, half 1: tap k at offset
    // off reads pixel 4 + 2k + off; a 4×4 second plane is padded.
    let side = 8;
    let values = (0..side * side)
        .map(|index| Complex32::new((index % side) as f32, (index / side) as f32))
        .collect::<Vec<_>>();
    let small = (0..16)
        .map(|index| Complex32::new(100.0 + (index % 4) as f32, (index / 4) as f32))
        .collect::<Vec<_>>();
    let cell = dense_cell_padded(
        &[(values, [side, side], [1, 1]), (small, [4, 4], [1, 1])],
        [1, 1],
        2,
    );
    assert_eq!(cell.support(), [3, 3]);
    assert_eq!(cell.mueller_planes(), 2);
    let tile = 9;
    // Row (ox, oy) = (0, 2): off = (−1, +1); plane 0 tap (2, 0): pixel
    // (4 + 2 − 1, 4 − 2 + 1) = (5, 3).
    let row = (2 * 3) * 2 * tile;
    assert_eq!(cell.data()[row + 2], Complex32::new(5.0, 3.0));
    // Plane 1 in the same row, tap (2, 0): pixel (2 + 2 − 1, 2 − 2 + 1)
    // = (3, 1) of the small plane; tap (2, 2): y = 2 + 2 + 1 = 5 → outside.
    assert_eq!(cell.data()[row + tile + 2], Complex32::new(103.0, 1.0));
    assert_eq!(cell.data()[row + tile + 8], Complex32::default());
}

/// `AWVisResampler`'s `convOrigin`: an odd cell (an older CASA's
/// crop, centred at `shape/2`) is read one fine sample past its centre
/// on both axes, as CASA reads it.
#[test]
fn an_odd_cell_is_read_about_casas_origin() {
    let side = 9;
    let values = (0..side * side)
        .map(|index| Complex32::new((index % side) as f32, (index / side) as f32))
        .collect::<Vec<_>>();
    let cell = dense_cell_padded(&[(values, [side, side], [1, 1])], [1, 1], 2);
    let tile = 9;
    // Row (ox, oy) = (1, 1): no fine offset; tap (1, 1) is the origin,
    // pixel (9/2 + 1, 9/2 + 1) = (5, 5).
    let row = (3 + 1) * tile;
    assert_eq!(cell.data()[row + 4], Complex32::new(5.0, 5.0));
    // One extent odd moves both axes.
    let values = vec![Complex32::new(1.0, 0.0); 9 * 8];
    let cell = dense_cell_padded(&[(values, [9, 8], [1, 1])], [1, 1], 2);
    assert_eq!(cell.support(), [3, 3]);
    assert_eq!(convolution_origin([9, 8]), [5, 5]);
    assert_eq!(convolution_origin([8, 8]), [4, 4]);
}

#[test]
fn the_area_sums_the_integer_taps_over_the_half_open_support() {
    let shape = [12, 12];
    let plane = (0..144)
        .map(|index| Complex32::new(1.0, if index == 6 * 12 + 6 { 1.0 } else { 0.0 }))
        .collect::<Vec<_>>();
    // support 2, sampling 2: ix, iy ∈ {−2, −1, 0, 1} → 16 taps.
    let area = cf_area(&plane, shape, [2, 2], 2);
    assert_eq!(area, Complex64::new(16.0, 1.0));
}

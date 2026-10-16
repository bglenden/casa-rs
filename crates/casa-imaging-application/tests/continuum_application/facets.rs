// SPDX-License-Identifier: LGPL-3.0-or-later
//! Facets (#664) through the application: each facet normalised by its own
//! sum of weights, and `.sumwt` publishing each facet's own (#722).

use super::*;

/// Set row `row`'s UVW to `uvw(row)` metres for every row.
fn set_uvw(path: &Path, uvw: impl Fn(usize) -> [f64; 3]) {
    let mut ms = MeasurementSet::open(path).expect("open fixture");
    for row in 0..ms.row_count() {
        ms.main_table_mut()
            .row_accessor_mut()
            .set_cell(
                row,
                "UVW",
                Value::Array(ArrayValue::Float64(
                    ArrayD::from_shape_vec(vec![3], uvw(row).to_vec()).expect("UVW cell"),
                )),
            )
            .expect("write UVW");
    }
    ms.save().expect("save fixture");
}

/// A 32-pixel image of 100″ cells in 2×2 facets of `path`: its `.sumwt`
/// per facet in CASA's order and its residual, both finite.
fn facets(path: &Path, root: &Path) -> ([f32; 4], ArrayD<f32>) {
    let image_name = root.join("facets");
    execute(&request(
        path,
        &image_name,
        json!({ "imsize": 32, "cell": "100arcsec", "facets": 2, "niter": 0 }),
    ))
    .unwrap_or_else(|error| panic!("facets: {error}"));
    let sums = product_plane_with_size(&image_name, ".sumwt", 2);
    let residual = product_plane_with_size(&image_name, ".residual", 32);
    let image = product_plane_with_size(&image_name, ".image", 32);
    assert!(
        residual
            .iter()
            .chain(image.iter())
            .all(|value| value.is_finite()),
        "a product is not finite"
    );
    (
        [[0, 0], [1, 0], [0, 1], [1, 1]].map(|[x, y]| sums[[x, y, 0, 0]]),
        residual,
    )
}

/// The peak |value| of facet `facet`'s 16×16 window of a 32² `residual`.
fn window_peak(residual: &ArrayD<f32>, facet: usize) -> f32 {
    let origin = [16 * (facet % 2), 16 * (facet / 2)];
    (0..16)
        .flat_map(|x| (0..16).map(move |y| residual[[origin[0] + x, origin[1] + y, 0, 0]]))
        .fold(0.0_f32, |peak, value| peak.max(value.abs()))
}

/// Each facet grids a row at its own projection
/// (`UVWMachine(project=true)`). A baseline `[3, 0.4, w]` m at 44 GHz lands
/// on all four facets' uv grids alike at `w = 0`, and at `w = 500 m` turns
/// off facets 1 and 3's (Astra's probe on #722). Every product stays finite,
/// a facet whose sum is at most 1e-7 is zero (CASA's
/// `divideImageByWeightVal`), and `.sumwt` holds each facet's own sum at its
/// pixel `(column, row)`: unequal, positive sums when half of eight rows
/// reach facets 1 and 3.
#[test]
fn facets_publish_their_own_sums_and_an_empty_facet_is_zero() {
    let _execution_guard = EXECUTION_LOCK.lock().expect("execution lock");
    let root = tempfile::tempdir().expect("test root");
    for case in ["alike", "empty", "unequal"] {
        std::fs::create_dir(root.path().join(case)).expect("case directory");
    }
    let alike = tiny_measurement_set(&root.path().join("alike"));
    set_uvw(&alike, |_| [3.0, 0.4, 0.0]);
    let (sums, _) = facets(&alike, &root.path().join("alike"));
    assert!(
        sums.iter().all(|sum| *sum > 0.0 && *sum == sums[0]),
        "{sums:?}"
    );

    let empty = tiny_measurement_set(&root.path().join("empty"));
    set_uvw(&empty, |_| [3.0, 0.4, 500.0]);
    let (sums, residual) = facets(&empty, &root.path().join("empty"));
    assert!(
        sums[0] > 0.0 && sums[1] == 0.0 && sums[2] > 0.0 && sums[3] == 0.0,
        "w = 500 m reaches facets 0 and 2 only: {sums:?}"
    );
    for (facet, sum) in sums.into_iter().enumerate() {
        let peak = window_peak(&residual, facet);
        if sum == 0.0 {
            assert_eq!(peak, 0.0, "empty facet {facet}'s residual");
        } else {
            assert!(peak > 0.0, "populated facet {facet}'s residual");
        }
    }

    let unequal = multi_row_measurement_set(&root.path().join("unequal"));
    set_uvw(&unequal, |row| {
        [3.0, 0.4, if row % 2 == 0 { 0.0 } else { 500.0 }]
    });
    let (sums, residual) = facets(&unequal, &root.path().join("unequal"));
    assert!(
        sums[1] > 0.0 && sums[0] == 2.0 * sums[1] && sums == [sums[0], sums[1], sums[0], sums[1]],
        "facets 1 and 3 take half the rows: {sums:?}"
    );
    assert!((0..4).all(|facet| window_peak(&residual, facet) > 0.0));
}

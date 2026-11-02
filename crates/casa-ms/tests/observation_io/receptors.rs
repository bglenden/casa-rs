// SPDX-License-Identifier: LGPL-3.0-or-later
//! Receptors where `FEED` names some spectral windows only:
//! `ms_trial_lines.ms`, whose `FEED` has a row in window 0 for each of its
//! three antennas and none in window 1 (the synthetic writer's, #943), and
//! XRADIO's conversion of it (`tools/ms-trial/make_lines_fixtures.sh`).
//! XRADIO 1.3.1 chooses the same `FEED` rows as the MSv2 adapter (none for
//! window 1), and for that window writes an `antenna_xds` whose
//! `receptor_label` and `polarization_type` come from the correlation
//! products, without `ANTENNA_RECEPTOR_ANGLE`; its schema requires both.

use std::path::Path;
use std::sync::Arc;

use casa_ms::MeasurementSet;
use casa_ms::observation_io::convert::{LossKind, to_msv2, to_msv4};
use casa_ms::observation_io::{OpenOptions, open};
use casa_observation::{Catalogue, Receptors};
use zarrs::array::{Array, ArraySubset};
use zarrs::filesystem::FilesystemStore;

use super::fixture;

/// Each partition's receptors by antenna, as (types, angles in radians),
/// the partitions in order of their first frequency.
fn stated(catalogue: &Catalogue) -> Vec<Vec<(Vec<String>, Vec<f64>)>> {
    let mut entries: Vec<_> = catalogue.partitions.iter().collect();
    entries.sort_by(|a, b| a.frequencies_hz[0].total_cmp(&b.frequencies_hz[0]));
    entries
        .into_iter()
        .map(|entry| {
            entry
                .receptors
                .values()
                .map(|Receptors { types, angles }| {
                    (
                        types.clone(),
                        angles.iter().map(|angle| angle.value()).collect(),
                    )
                })
                .collect()
        })
        .collect()
}

/// What an `antenna_xds` states of its receptors.
#[derive(Debug, PartialEq)]
struct Stated {
    receptor_label: Option<Vec<String>>,
    polarization_type: Option<Vec<String>>,
    receptor_angle: Option<Vec<f64>>,
}

/// The receptor variables of dataset `dataset`'s `antenna_xds` in the
/// processing set at `store`.
fn antenna_xds(store: &Path, dataset: &str) -> Stated {
    let zarr = Arc::new(FilesystemStore::new(store).expect("store"));
    let path = format!("/{dataset}/antenna_xds");
    let held = |name: &str| {
        store
            .join(dataset)
            .join("antenna_xds")
            .join(name)
            .join("zarr.json")
            .is_file()
    };
    let strings = |name: &str| {
        held(name).then(|| {
            let array = Array::open(zarr.clone(), &format!("{path}/{name}")).expect("open");
            let subset = ArraySubset::new_with_shape(array.shape().to_vec());
            let values: Vec<Vec<char>> = array.retrieve_array_subset(&subset).expect("read");
            values
                .into_iter()
                .map(|chars| chars.into_iter().collect())
                .collect()
        })
    };
    let receptor_angle = held("ANTENNA_RECEPTOR_ANGLE").then(|| {
        let array =
            Array::open(zarr.clone(), &format!("{path}/ANTENNA_RECEPTOR_ANGLE")).expect("open");
        let subset = ArraySubset::new_with_shape(array.shape().to_vec());
        array.retrieve_array_subset(&subset).expect("read")
    });
    Stated {
        receptor_label: strings("receptor_label"),
        polarization_type: strings("polarization_type"),
        receptor_angle,
    }
}

/// The (antenna, window) of every `FEED` row of the MSv2 at `path`, sorted.
fn feed_rows(path: &Path) -> Vec<(i32, i32)> {
    let ms = MeasurementSet::open(path).expect("ms");
    let feed = ms.feed().expect("FEED");
    let mut rows: Vec<(i32, i32)> = (0..feed.row_count())
        .map(|row| {
            (
                feed.i32(row, "ANTENNA_ID").expect("antenna"),
                feed.i32(row, "SPECTRAL_WINDOW_ID").expect("window"),
            )
        })
        .collect();
    rows.sort_unstable();
    rows
}

/// A window `FEED` has no row for keeps no receptors through MSv2 → MSv4
/// → MSv2, as XRADIO converts it: its dataset's `antenna_xds` has XRADIO's
/// `receptor_label` (which XRADIO's schema requires) and no receptor
/// angles. Its `polarization_type` names the receptors of the window's
/// products, `R, L` for `RR, LL`, where XRADIO names the first product's
/// letters, `R, R` (a recorded divergence). The other window's dataset
/// states `FEED`'s receptors as XRADIO's does.
#[test]
fn a_window_without_feed_rows_has_xradios_receptor_labels_and_no_angles() {
    let root = tempfile::tempdir().expect("root");
    let ms = fixture(root.path(), "ms_trial_lines.ms");
    let theirs = fixture(root.path(), "ms_trial_lines.ps.zarr");
    let stated_feed = vec![(vec!["R".to_string(), "L".to_string()], vec![0.0, 0.0]); 3];
    let expected = vec![stated_feed, Vec::new()];
    assert_eq!(feed_rows(&ms), [(0, 0), (1, 0), (2, 0)]);
    let msv2 = open(&ms, &OpenOptions::default()).expect("msv2");
    assert_eq!(stated(msv2.catalogue()), expected);

    let ours = root.path().join("ours").join("ms_trial_lines.ps.zarr");
    std::fs::create_dir(ours.parent().expect("parent")).expect("dir");
    let accept = [LossKind::Sigma, LossKind::Tables];
    to_msv4(&ms, &ours, &super::convert::options(&accept)).expect("to msv4");

    let labels = Some(vec!["pol_0".to_string(), "pol_1".to_string()]);
    let types = |pair: [&str; 2]| Some(pair.repeat(3).into_iter().map(String::from).collect());
    let fed = antenna_xds(&ours, "ms_trial_lines_0");
    assert_eq!(fed, antenna_xds(&theirs, "ms_trial_lines_0"));
    assert_eq!(
        fed,
        Stated {
            receptor_label: labels.clone(),
            polarization_type: types(["R", "L"]),
            receptor_angle: Some(vec![0.0; 6]),
        }
    );
    let unfed = antenna_xds(&ours, "ms_trial_lines_1");
    assert_eq!(
        unfed,
        Stated {
            receptor_label: labels.clone(),
            polarization_type: types(["R", "L"]),
            receptor_angle: None,
        }
    );
    assert_eq!(
        antenna_xds(&theirs, "ms_trial_lines_1"),
        Stated {
            receptor_label: labels,
            polarization_type: types(["R", "R"]),
            receptor_angle: None,
        },
        "XRADIO names the first product's letters"
    );

    // Both MSv4 stores read back as the MSv2 states its receptors, and the
    // conversion back writes the MSv2's FEED rows.
    for store in [&ours, &theirs] {
        let msv4 = open(store, &OpenOptions::default()).expect("msv4");
        assert_eq!(stated(msv4.catalogue()), expected, "{}", store.display());
    }
    let back = root.path().join("back.ms");
    let accept = [LossKind::Sigma, LossKind::Tables, LossKind::Defaulted];
    to_msv2(&ours, &back, &super::convert::options(&accept)).expect("to msv2");
    assert_eq!(feed_rows(&back), [(0, 0), (1, 0), (2, 0)]);
    let again = open(&back, &OpenOptions::default()).expect("back");
    assert_eq!(stated(again.catalogue()), expected);
}

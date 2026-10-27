// SPDX-License-Identifier: LGPL-3.0-or-later
//! The namespace a calibration table persists: an MSv2's own subtables,
//! with its catalogue's entities at their rows; an MSv4's, the subtables
//! its MSv2 conversion holds, in catalogue order.

use casa_ms::observation_io::convert::{ConversionOptions, LossKind, PaddingRows, to_msv2};
use casa_ms::observation_io::{Msv4Codec, NamespaceRows, open, persisted_namespace};
use casa_observation::PartitionId;

use super::{fixture, open_both};

/// An MSv2's namespace is its own `ANTENNA`, `FIELD` and `SPECTRAL_WINDOW`
/// rows, read back from the store's directory as from the namespace; its
/// catalogue's antennas and fields are their rows, and its partition names
/// its data description's window.
#[test]
fn an_msv2s_namespace_is_its_own_subtables() {
    let root = tempfile::tempdir().expect("root");
    let path = fixture(root.path(), "ms_trial.ms");
    let namespace = persisted_namespace(&path).expect("namespace");
    let (v2, _) = open_both(&root.path().join("again"));
    let catalogue = casa_observation::Observation::catalogue(&v2);
    assert_eq!(namespace.antennas, (0..5).collect::<Vec<_>>());
    assert_eq!(namespace.fields, vec![0]);
    assert_eq!(namespace.spectral_window(PartitionId(0)), 0);
    let read = NamespaceRows::read(&path).expect("read");
    assert_eq!(read.antennas, namespace.rows.antennas);
    assert_eq!(read.spectral_windows, namespace.rows.spectral_windows);
    for (antenna, entry) in namespace.rows.antennas.iter().zip(&catalogue.antennas) {
        assert_eq!(
            (&antenna.name, &antenna.station, antenna.position_m),
            (&entry.name, &entry.station, entry.position.values())
        );
    }
    let window = &namespace.rows.spectral_windows[0];
    let entry = catalogue.partition(PartitionId(0));
    assert_eq!(
        (&window.frequencies_hz, &window.widths_hz, window.frame),
        (
            &entry.frequencies_hz,
            &entry.channel_widths_hz,
            entry.frequency_frame
        )
    );
}

/// An MSv4 has no subtables; its namespace is the ones its conversion to
/// an MSv2 writes, row for row, in catalogue order.
#[test]
fn an_msv4s_namespace_is_its_conversions() {
    let root = tempfile::tempdir().expect("root");
    let xradio = fixture(root.path(), "ms_trial.ps.zarr");
    let namespace = persisted_namespace(&xradio).expect("namespace");
    let converted = root.path().join("converted.ms");
    let options = ConversionOptions {
        data_groups: None,
        accept: vec![LossKind::Sigma, LossKind::Tables, LossKind::Columns],
        batch_bytes: 1 << 20,
        workers: 1,
        chunks: [5, 0, 0, 0],
        codec: Msv4Codec::BloscLz4,
        padding: PaddingRows::Flagged,
        creator: "casa-rs".to_string(),
        creation_date: "2026-10-27T00:00:00+00:00".to_string(),
    };
    to_msv2(&xradio, &converted, &options).expect("convert");
    let read = NamespaceRows::read(&converted).expect("read");
    assert_eq!(read.antennas, namespace.rows.antennas);
    assert_eq!(read.spectral_windows, namespace.rows.spectral_windows);
    assert_eq!(read.fields.len(), namespace.rows.fields.len());
    for (a, b) in read.fields.iter().zip(&namespace.rows.fields) {
        assert_eq!(a.name, b.name);
        assert_eq!(a.phase_centre.as_angles(), b.phase_centre.as_angles());
    }
    let observation = open(&xradio, &Default::default()).expect("open");
    let names: Vec<&str> = observation
        .catalogue()
        .antennas
        .iter()
        .map(|antenna| antenna.name.as_str())
        .collect();
    assert_eq!(
        namespace
            .rows
            .antennas
            .iter()
            .map(|antenna| antenna.name.as_str())
            .collect::<Vec<_>>(),
        names,
        "catalogue order and XRADIO's names"
    );
    assert_eq!(
        namespace.antennas,
        (0..names.len() as u32).collect::<Vec<_>>()
    );
}

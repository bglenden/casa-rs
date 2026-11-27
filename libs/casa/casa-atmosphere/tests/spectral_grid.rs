// SPDX-License-Identifier: LGPL-3.0-or-later
//! `SpectralGrid` follows ATM's `SpectralGrid::add`: channel i of a regular
//! window is at (f_ref − Δf·i_ref) + i·Δf.

use casa_atmosphere::SpectralGrid;

#[test]
fn regular_windows_place_channels_as_atm() {
    let mut grid = SpectralGrid::new();
    // The `at` tool's even-channel window: 4 channels, reference channel 1,
    // its frequency half a channel below the centre.
    let centre = 345.0e9;
    let separation = 0.5e9;
    let first = grid
        .add_regular(4, 1, centre - separation * 0.5, separation)
        .unwrap();
    let second = grid.add_regular(3, 2, 100.0e9, -1.0e6).unwrap();
    assert_eq!((first, second), (0, 1));
    let offset = (centre - separation * 0.5) - separation * 1.0;
    let expected: Vec<f64> = (0..4).map(|i| offset + f64::from(i) * separation).collect();
    assert_eq!(grid.windows()[0].frequencies_hz(), expected.as_slice());
    assert_eq!(
        grid.windows()[1].frequencies_hz(),
        &[100.0e9 + 2.0e6, 100.0e9 + 1.0e6, 100.0e9]
    );
    assert_eq!(grid.windows()[1].min_frequency_hz(), 100.0e9);
    assert_eq!(grid.windows()[1].max_frequency_hz(), 100.0e9 + 2.0e6);
    assert_eq!(grid.total_channels(), 7);
    assert_eq!(grid.frequencies_hz().count(), 7);
    assert_eq!(grid.flat_index(1, 2), Some(6));
    assert_eq!(grid.flat_index(1, 3), None);
    assert_eq!(grid.windows()[0].channel_width_hz(0), Some(separation));
    assert_eq!(grid.windows()[1].channel_width_hz(2), Some(-1.0e6));
}

#[test]
fn explicit_windows_record_atm_separation() {
    let mut grid = SpectralGrid::new();
    grid.add_channels(&[1.0e9, 2.0e9, 3.0e9], 0).unwrap();
    grid.add_channels(&[1.0e9, 2.0e9, 4.0e9], 1).unwrap();
    assert_eq!(grid.windows()[0].channel_separation_hz(), 1.0e9);
    assert_eq!(grid.windows()[1].channel_separation_hz(), 0.0);
    assert_eq!(grid.windows()[1].reference_frequency_hz(), 2.0e9);
    assert_eq!(
        SpectralGrid::single(230.0e9).unwrap().windows()[0].frequencies_hz(),
        &[230.0e9]
    );
}

#[test]
fn malformed_windows_are_refused() {
    let mut grid = SpectralGrid::new();
    assert!(grid.add_regular(0, 0, 1.0e9, 1.0e6).is_err());
    assert!(grid.add_regular(4, 4, 1.0e9, 1.0e6).is_err());
    assert!(grid.add_regular(4, 0, f64::NAN, 1.0e6).is_err());
    assert!(grid.add_channels(&[], 0).is_err());
    assert!(grid.add_channels(&[1.0e9, f64::INFINITY], 0).is_err());
    assert!(grid.windows().is_empty());
}

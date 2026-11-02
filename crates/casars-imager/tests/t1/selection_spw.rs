// SPDX-License-Identifier: LGPL-3.0-or-later
//! Spectral-window and channel selection through the production catalog:
//! each CASA `spw` form grids exactly the channels CASA's channel list
//! (`MSSelection::getChanList`) names, counted here from the windows, and
//! the selections CASA refuses, or casa-rs does not image, write no images.
//! The grammar itself is pinned against CASA in `casa-ms`
//! (`selection::spw_expr`, `tests/spw_selection_oracle.rs`).

use casa_ms::SyntheticSpectralSetup;
use serde_json::json;

use super::fixture::{Component, Observation, Setup};

const POINT: Component = Component {
    offset_px: [6, -4],
    flux_jy: 1.0,
    fwhm_px: None,
    spectral_index: 0.0,
};

/// Channels per window.
const CHANNELS: usize = 10;

/// Two ten-channel Q-band windows observed together: `t1-qband` from
/// 44 GHz and `t1-qband-hi` from 46 GHz, both in 128 MHz channels.
fn two_windows() -> Observation {
    let mut setup = Setup::vla_q_band(CHANNELS);
    let low = setup.spectral_windows[0].clone();
    setup.spectral_windows.push(SyntheticSpectralSetup {
        name: "t1-qband-hi".to_string(),
        start_frequency_hz: low.start_frequency_hz + 2.0e9,
        ..low
    });
    Observation::synthesise_setup(setup, &[("point", POINT)])
}

fn controls(extra: serde_json::Value) -> serde_json::Value {
    let mut controls = json!({"niter": 0});
    controls
        .as_object_mut()
        .unwrap()
        .extend(extra.as_object().unwrap().clone());
    controls
}

#[test]
fn spw_forms_grid_the_channels_casas_channel_list_names() {
    let observation = two_windows();
    let all = 2 * CHANNELS as u64;
    let (full, _) = observation.image("spw-all", controls(json!({})));
    assert!(full.gridded_samples > 0);
    for (index, (spw, channels)) in [
        ("*", 20),
        // A name holding `-` must be quoted.
        ("\"t1-qband-hi\"", 10),
        ("\"t1-qband*\"", 20),
        ("1", 10),
        ("0:2~5;7~9", 7),
        ("*:3~6", 8),
        ("0:0~9^3", 4),
        ("0:^2,1:1", 6),
        // Channel centres 44.256, 44.384 and 44.512 GHz.
        ("0:44.2GHz~44.6GHz", 3),
        // The window whose midpoint (46.576 GHz) the range holds.
        ("45.5GHz~48GHz", 10),
        ("46GHz~47.2GHz:0~4", 5),
        // Past the last channel: CASA clamps to it (and logs it).
        ("0:100", 1),
    ]
    .into_iter()
    .enumerate()
    {
        let (report, _) = observation.image(&format!("spw-{index}"), controls(json!({"spw": spw})));
        assert_eq!(
            report.gridded_samples * all,
            full.gridded_samples * channels,
            "spw={spw:?} grids {channels} of {all} channels"
        );
    }
}

#[test]
fn a_cube_takes_the_spw_channels_within_its_axis() {
    let observation = two_windows();
    let cube = |name: &str, spw: &str| {
        observation
            .image(
                name,
                controls(json!({
                    "specmode": "cubedata", "spw": spw, "nchan": 4, "start": "2",
                })),
            )
            .0
            .gridded_samples
    };
    let window = cube("cube-window", "0");
    let named = cube("cube-named", "0:3~4");
    assert!(window > 0);
    assert_eq!(2 * named, window, "two of the axis's four channels");
}

#[test]
fn spw_selections_casa_refuses_write_no_images() {
    let observation = two_windows();
    // CASA also refuses a blank `spw`, but the parameter catalog's selector
    // normalization makes blank text unset (every window) before the
    // request reaches the grammar, so it is not listed here.
    for (index, spw) in [
        // CASA refuses these.
        "2",
        "NO_SUCH_WINDOW",
        "t1-qband-hi",
        "0:5~3",
        "0:-10km/s~10km/s",
        "0,",
        "0:50GHz~51GHz",
        // CASA accepts these, but how its imaging reads them is not
        // established: every row with some windows' channels, a window whose
        // rows are kept without a channel, channels read twice.
        "(0)",
        "0~1:45GHz~45.1GHz",
        "0:1~3;2~4",
    ]
    .into_iter()
    .enumerate()
    {
        let name = format!("spw-refused-{index}");
        assert!(
            observation
                .try_image(&name, controls(json!({"spw": spw})))
                .is_err(),
            "spw={spw:?} is refused"
        );
        assert!(!observation.scratch(&format!("{name}.psf")).exists());
    }
}

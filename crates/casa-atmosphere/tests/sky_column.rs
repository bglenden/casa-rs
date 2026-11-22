// SPDX-License-Identifier: LGPL-3.0-or-later
//! A [`SkyColumn`] gives the sky temperature [`AbsorptionProfile::tebb_sky`]
//! gives, to rounding, at every airmass, and its zenith opacity is the
//! profile's dry and wet opacity: the simulator's `tsys-atm` noise relies on
//! both.

use casa_atmosphere::{AbsorptionProfile, AtmosphereProfile, ProfileParameters, Sky, SpectralGrid};

#[test]
fn a_sky_column_gives_the_profiles_sky_temperature_at_every_airmass() {
    let atmosphere = AtmosphereProfile::new(&ProfileParameters::default()).expect("profile");
    let mut grid = SpectralGrid::new();
    // A window, a strong line (183.31 GHz), and the submillimetre.
    grid.add_channels(&[90e9, 183.31e9, 230e9, 345e9, 690e9, 850e9], 0)
        .expect("channels");
    let profile = AbsorptionProfile::new_totals(atmosphere, grid);
    for water_column_m in [0.2e-3, 1.0e-3, 4.0e-3] {
        let sky = Sky {
            water_column_m,
            airmass: 1.0,
            sky_coupling: 0.85,
            spill_temperature_k: 270.0,
            background_temperature_k: 2.73,
        };
        for channel in 0..6 {
            let column = profile.sky_column(channel, &sky).expect("column");
            let opacity =
                profile.dry_opacity(channel) + profile.wet_opacity(channel, water_column_m);
            assert!(
                (column.zenith_opacity() / opacity - 1.0).abs() < 1e-13,
                "channel {channel}: {} against {opacity}",
                column.zenith_opacity()
            );
            for airmass in [1.0, 1.0001, 1.3, 2.0, 5.7, 1000.0] {
                let expected = profile
                    .tebb_sky(channel, &Sky { airmass, ..sky })
                    .expect("tebb");
                let found = column.tebb(airmass);
                assert!(
                    (found / expected - 1.0).abs() < 1e-13,
                    "channel {channel}, airmass {airmass}: {found} against {expected}"
                );
                let expected_rj = profile
                    .trj_sky(channel, &Sky { airmass, ..sky })
                    .expect("trj");
                assert!((column.trj(airmass) / expected_rj - 1.0).abs() < 1e-13);
            }
        }
    }
    let invalid = Sky {
        water_column_m: -1.0,
        ..Sky::zenith(1e-3)
    };
    assert!(profile.sky_column(0, &invalid).is_err());
}

#[test]
fn a_dry_sky_column_gives_the_dry_profile_opacity() {
    let atmosphere = AtmosphereProfile::new(&ProfileParameters {
        relative_humidity_percent: 0.0,
        ..ProfileParameters::default()
    })
    .expect("dry profile");
    let mut grid = SpectralGrid::new();
    grid.add_channels(&[90e9, 183.31e9], 0).expect("channels");
    let profile = AbsorptionProfile::new_totals(atmosphere, grid);
    for channel in 0..2 {
        let column = profile
            .sky_column(channel, &Sky::zenith(0.0))
            .expect("dry column");
        assert!((column.zenith_opacity() / profile.dry_opacity(channel) - 1.0).abs() < 1e-13);
        assert!(column.trj(1.0).is_finite());
        assert!(column.trj(2.0) > column.trj(1.0));
    }
}

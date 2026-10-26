// SPDX-License-Identifier: LGPL-3.0-or-later
//! The quantities `tools/atmosphere/at_tool_check.py` reads from CASA's `at`
//! tool, computed by casa-atmosphere: it reads the check's configuration
//! (profiles, bands and water/airmass sets, in the `at` tool's terms) as
//! JSON on stdin and writes the results as JSON on stdout. The simulator's
//! CASA comparator (`tools/simulation/compare_sm_ms.py`) reads casa-rs's
//! own sky and wet delays from it the same way.
//!
//! ```text
//! cargo build --release -p casa-atmosphere --example at_tool_quantities
//! ```

use casa_atmosphere::absorption::Absorber;
use casa_atmosphere::{
    AbsorptionProfile, AtmosphereProfile, AtmosphereType, ProfileParameters, Sky, SpectralGrid,
};
use serde_json::{Value, json};
use std::io::Read;

fn number(value: &Value) -> f64 {
    value.as_f64().expect("a number")
}

/// The atmosphere `at.initAtmProfile` builds.
fn atmosphere(profile: &Value) -> AtmosphereProfile {
    let atmosphere = u32::try_from(profile["atmType"].as_u64().expect("an atmosphere type"))
        .expect("a small type");
    AtmosphereProfile::new(&ProfileParameters {
        altitude_m: number(&profile["altitude_m"]),
        ground_pressure_hpa: number(&profile["pressure_mbar"]),
        ground_temperature_k: number(&profile["temperature_k"]),
        lapse_rate_k_per_km: number(&profile["dTem_dh_k_per_km"]),
        relative_humidity_percent: number(&profile["humidity"]),
        water_vapour_scale_height_km: number(&profile["h0_km"]),
        pressure_step_hpa: number(&profile["dP_mbar"]),
        top_altitude_km: number(&profile["maxAltitude_km"]),
        atmosphere: AtmosphereType::from_number(atmosphere).expect("an ATM atmosphere type"),
    })
    .expect("a valid profile")
}

/// The grid `at.initSpectralWindow` builds: channels ceil(width/res), the
/// reference channel (n−1)/2, and for an even count the reference
/// frequency half a channel below the centre.
fn grid(bands: &[Value]) -> SpectralGrid {
    let mut grid = SpectralGrid::new();
    for band in bands {
        let channels =
            usize::try_from(band["channels"].as_u64().expect("a channel count")).expect("a count");
        let mut reference = 1.0e9 * number(&band["centre_ghz"]);
        let separation = 1.0e9 * number(&band["res_ghz"]);
        if channels % 2 == 0 {
            reference -= separation * 0.5;
        }
        grid.add_regular(channels, (channels - 1) / 2, reference, separation)
            .expect("a valid window");
    }
    grid
}

/// Each (water column, airmass) set as the `at` tool's `setUserWH2O` and
/// `setAirMass` leave its sky.
fn skies(sets: &[Value]) -> Vec<Sky> {
    sets.iter()
        .map(|set| Sky {
            airmass: number(&set["airmass"]),
            ..Sky::zenith(number(&set["water_m"]))
        })
        .collect()
}

fn per_channel(profile: &AbsorptionProfile, sets: &[Sky]) -> Value {
    let ground = profile.ground_water_column_m();
    let ground_t = profile.atmosphere().parameters().ground_temperature_k;
    let mut windows = Vec::new();
    for (w, window) in profile.grid().windows().iter().enumerate() {
        let first = profile.grid().flat_index(w, 0).expect("a window");
        let channels: Vec<usize> = (first..first + window.num_channels()).collect();
        let values = |f: &dyn Fn(usize) -> f64| channels.iter().map(|&c| f(c)).collect::<Vec<_>>();
        let per_set = |f: &dyn Fn(usize, &Sky) -> f64| {
            sets.iter()
                .map(|sky| values(&|c| f(c, sky)))
                .collect::<Vec<_>>()
        };
        let zenith = Sky::zenith(ground);
        let average = Sky {
            spill_temperature_k: ground_t,
            ..zenith
        };
        windows.push(json!({
            "freq_hz": values(&|c| window.frequencies_hz()[c - first]),
            "dry": values(&|c| profile.dry_opacity(c)),
            "dry_cont": values(&|c| profile.dry_continuum_opacity(c)),
            "o2": values(&|c| profile.absorber_opacity(c, Absorber::O2Lines)),
            "o3": values(&|c| profile.absorber_opacity(c, Absorber::O3Lines)),
            "co": values(&|c| profile.absorber_opacity(c, Absorber::CoLines)),
            "n2o": values(&|c| profile.absorber_opacity(c, Absorber::N2oLines)),
            "wet": values(&|c| profile.wet_opacity(c, ground)),
            "h2o_lines": values(&|c| profile.h2o_lines_opacity(c, ground)),
            "h2o_cont": values(&|c| profile.h2o_continuum_opacity(c, ground)),
            "tebb": values(&|c| profile.tebb_sky(c, &zenith).expect("valid conditions")),
            "tebb_1mm": values(&|c| profile.tebb_sky(c, &Sky::zenith(1e-3)).expect("valid conditions")),
            "trj": values(&|c| profile.trj_sky(c, &zenith).expect("valid conditions")),
            "disp_phase": values(&|c| profile.dispersive_h2o_phase_delay_deg(c, ground)),
            "nondisp_phase": values(&|c| profile.non_dispersive_h2o_phase_delay_deg(c, ground)),
            "nondisp_dry_phase": values(&|c| profile.non_dispersive_dry_phase_delay_deg(c)),
            "disp_path": values(&|c| profile.dispersive_h2o_path_length_m(c, ground)),
            "nondisp_path": values(&|c| profile.non_dispersive_h2o_path_length_m(c, ground)),
            "nondisp_dry_path": values(&|c| profile.non_dispersive_dry_path_length_m(c)),
            "o2_path": values(&|c| profile.absorber_path_length_m(c, Absorber::O2Lines)),
            "o3_path": values(&|c| profile.absorber_path_length_m(c, Absorber::O3Lines)),
            "co_path": values(&|c| profile.absorber_path_length_m(c, Absorber::CoLines)),
            "n2o_path": values(&|c| profile.absorber_path_length_m(c, Absorber::N2oLines)),
            "average_tebb": profile.average_tebb_sky(w, &average).expect("valid conditions"),
            "average_trj": profile.average_trj_sky(w, &average).expect("valid conditions"),
            "set_tebb": per_set(&|c, sky| profile.tebb_sky(c, sky).expect("valid conditions")),
            "set_wet": per_set(&|c, sky| profile.wet_opacity(c, sky.water_column_m)),
            "set_disp_phase": per_set(&|c, sky| profile.dispersive_h2o_phase_delay_deg(c, sky.water_column_m)),
            "set_nondisp_phase": per_set(&|c, sky| {
                profile.non_dispersive_h2o_phase_delay_deg(c, sky.water_column_m)
            }),
        }));
    }
    json!({ "ground_water_m": ground, "layers": profile.atmosphere().layers().len(), "windows": windows })
}

fn main() {
    let mut input = String::new();
    std::io::stdin()
        .read_to_string(&mut input)
        .expect("a configuration on stdin");
    let config: Value = serde_json::from_str(&input).expect("JSON");
    let bands = config["bands"].as_array().expect("bands");
    let sets = skies(config["sets"].as_array().expect("sets"));
    let results: Vec<Value> = config["profiles"]
        .as_array()
        .expect("profiles")
        .iter()
        .map(|profile| {
            per_channel(
                &AbsorptionProfile::new(atmosphere(profile), grid(bands)),
                &sets,
            )
        })
        .collect();
    println!("{}", json!({ "profiles": results }));
}

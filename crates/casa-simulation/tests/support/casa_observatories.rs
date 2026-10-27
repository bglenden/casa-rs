// SPDX-License-Identifier: LGPL-3.0-or-later
//! CASA's observatory table, as `tests/data/readantenna_casa.json` holds it,
//! served as a measures provider so that tests of `readantenna` and its
//! users compare with CASA without casa-rs's own measures data.

use std::path::Path;

use casa_types::measures::{MeasuresProvider, ObservatoryPosition};
use serde_json::Value;

/// CASA's observatory table, looked up as casa-measures-data looks names up
/// (trimmed, case-insensitive, first match).
#[derive(Debug)]
pub struct CasaObservatories(Vec<(String, ObservatoryPosition)>);

fn number(value: &Value, key: &str) -> f64 {
    value[key]
        .as_f64()
        .unwrap_or_else(|| panic!("{key} in {value}"))
}

impl CasaObservatories {
    /// The table in a `readantenna_casa.json` fixture.
    pub fn from_fixture(fixture: &Value) -> Self {
        let entries = fixture["observatories"]
            .as_array()
            .expect("observatories")
            .iter()
            .map(|entry| {
                let name = entry["name"].as_str().expect("name").to_string();
                let position = if entry["refer"] == "WGS84" {
                    ObservatoryPosition::Wgs84 {
                        longitude_rad: number(entry, "longitude_rad"),
                        latitude_rad: number(entry, "latitude_rad"),
                        height_m: number(entry, "height_m"),
                    }
                } else {
                    ObservatoryPosition::Itrf {
                        x_m: number(entry, "x_m"),
                        y_m: number(entry, "y_m"),
                        z_m: number(entry, "z_m"),
                    }
                };
                (name, position)
            })
            .collect();
        Self(entries)
    }

    /// The table in the checked-in `tests/data/readantenna_casa.json`.
    #[allow(dead_code, reason = "each test target uses the constructor it needs")]
    pub fn checked_in() -> Self {
        let path = Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/data/readantenna_casa.json");
        let text = std::fs::read_to_string(&path).expect("the CASA fixture is checked in");
        Self::from_fixture(&serde_json::from_str(&text).expect("the CASA fixture is JSON"))
    }
}

impl MeasuresProvider for CasaObservatories {
    fn observatory(&self, name: &str) -> Result<Option<ObservatoryPosition>, String> {
        let name = name.trim();
        Ok(self
            .0
            .iter()
            .find(|(entry, _)| entry.eq_ignore_ascii_case(name))
            .map(|(_, position)| position.clone()))
    }
}

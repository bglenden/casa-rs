// SPDX-License-Identifier: LGPL-3.0-or-later
//! Task-style options for CASA `importvla` compatibility.

use std::path::PathBuf;
use std::str::FromStr;

use casa_types::quanta::Quantity;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use crate::VlaError;

/// VLA observing band selection from CASA `importvla`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub enum BandName {
    /// 4 meter band.
    Four,
    /// P band.
    P,
    /// L band.
    L,
    /// S band.
    S,
    /// C band.
    C,
    /// X band.
    X,
    /// U band.
    U,
    /// K band.
    K,
    /// Ka band.
    Ka,
    /// Q band.
    Q,
}

impl BandName {
    /// CASA task token used by `importvla`.
    pub fn as_task_token(self) -> &'static str {
        match self {
            Self::Four => "4",
            Self::P => "P",
            Self::L => "L",
            Self::S => "S",
            Self::C => "C",
            Self::X => "X",
            Self::U => "U",
            Self::K => "K",
            Self::Ka => "Ka",
            Self::Q => "Q",
        }
    }
}

impl FromStr for BandName {
    type Err = VlaError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        let normalized = value.trim();
        match normalized.to_ascii_lowercase().as_str() {
            "4" => Ok(Self::Four),
            "p" => Ok(Self::P),
            "l" => Ok(Self::L),
            "s" => Ok(Self::S),
            "c" => Ok(Self::C),
            "x" => Ok(Self::X),
            "u" => Ok(Self::U),
            "k" => Ok(Self::K),
            "ka" => Ok(Self::Ka),
            "q" => Ok(Self::Q),
            _ => Err(VlaError::InvalidArgument {
                argument: "bandname",
                message: format!("unsupported VLA band `{value}`"),
            }),
        }
    }
}

/// Antenna naming mode used by CASA `importvla`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default, JsonSchema)]
pub enum AntennaNameScheme {
    /// CASA `new`: names like `VA04`.
    #[default]
    New,
    /// CASA `old`: names like `04`.
    Old,
}

impl FromStr for AntennaNameScheme {
    type Err = VlaError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value.trim().to_ascii_lowercase().as_str() {
            "new" => Ok(Self::New),
            "old" => Ok(Self::Old),
            _ => Err(VlaError::InvalidArgument {
                argument: "antnamescheme",
                message: format!("expected `new` or `old`, got `{value}`"),
            }),
        }
    }
}

/// Task-style options for a future native `importvla` implementation.
///
/// The fields intentionally mirror CASA `importvla` naming so callers can keep
/// using familiar parameter names while the native Rust filler comes online.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct ImportVlaOptions {
    /// Input VLA archive files on disk.
    pub archivefiles: Vec<PathBuf>,
    /// Planned output MeasurementSet path.
    pub vis: Option<PathBuf>,
    /// Optional band selection. `None` means all bands.
    pub bandname: Option<BandName>,
    /// Spectral-window matching tolerance in Hz.
    pub frequencytol_hz: f64,
    /// Optional project filter.
    pub project: Option<String>,
    /// Optional inclusive start time string.
    pub starttime: Option<String>,
    /// Optional inclusive stop time string.
    pub stoptime: Option<String>,
    /// Apply nominal sensitivity scaling.
    pub applytsys: bool,
    /// Keep auto-correlations.
    pub autocorr: bool,
    /// Antenna naming scheme.
    pub antnamescheme: AntennaNameScheme,
    /// Keep blank source names.
    pub keepblanks: bool,
    /// Use EVLA band centers/bandwidths when band-based frequency selection is used.
    pub evlabands: bool,
}

impl Default for ImportVlaOptions {
    fn default() -> Self {
        Self {
            archivefiles: Vec::new(),
            vis: None,
            bandname: None,
            frequencytol_hz: 150_000.0,
            project: None,
            starttime: None,
            stoptime: None,
            applytsys: true,
            autocorr: false,
            antnamescheme: AntennaNameScheme::New,
            keepblanks: false,
            evlabands: false,
        }
    }
}

impl ImportVlaOptions {
    /// Parse the CASA-style frequency tolerance quantity into Hz.
    pub fn parse_frequencytol(quantity: &str) -> Result<f64, VlaError> {
        let parsed = Quantity::from_str(quantity).map_err(|error| VlaError::InvalidArgument {
            argument: "frequencytol",
            message: error.to_string(),
        })?;
        let hz =
            casa_types::quanta::Unit::new("Hz").map_err(|error| VlaError::InvalidArgument {
                argument: "frequencytol",
                message: error.to_string(),
            })?;
        parsed
            .get_value_in(&hz)
            .map_err(|error| VlaError::InvalidArgument {
                argument: "frequencytol",
                message: error.to_string(),
            })
    }

    /// Return the configured archive files, rejecting empty input.
    pub fn require_archivefiles(&self) -> Result<&[PathBuf], VlaError> {
        if self.archivefiles.is_empty() {
            return Err(VlaError::NoArchiveFiles);
        }
        Ok(&self.archivefiles)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn band_name_tokens_and_parsing_cover_all_supported_bands() {
        let cases = [
            ("4", BandName::Four, "4"),
            ("P", BandName::P, "P"),
            ("l", BandName::L, "L"),
            ("S", BandName::S, "S"),
            ("c", BandName::C, "C"),
            ("X", BandName::X, "X"),
            ("u", BandName::U, "U"),
            ("K", BandName::K, "K"),
            ("ka", BandName::Ka, "Ka"),
            ("Q", BandName::Q, "Q"),
        ];

        for (raw, expected, token) in cases {
            let parsed = raw.parse::<BandName>().expect("parse band");
            assert_eq!(parsed, expected);
            assert_eq!(parsed.as_task_token(), token);
        }
    }

    #[test]
    fn band_name_rejects_unknown_values() {
        let error = "notaband".parse::<BandName>().expect_err("invalid band");
        assert!(matches!(
            error,
            VlaError::InvalidArgument {
                argument: "bandname",
                ..
            }
        ));
        assert!(error.to_string().contains("unsupported VLA band"));
    }

    #[test]
    fn antenna_name_scheme_parses_and_rejects_unknown_values() {
        assert_eq!(
            "new".parse::<AntennaNameScheme>().unwrap(),
            AntennaNameScheme::New
        );
        assert_eq!(
            "OLD".parse::<AntennaNameScheme>().unwrap(),
            AntennaNameScheme::Old
        );

        let error = "legacy"
            .parse::<AntennaNameScheme>()
            .expect_err("invalid naming mode");
        assert!(matches!(
            error,
            VlaError::InvalidArgument {
                argument: "antnamescheme",
                ..
            }
        ));
    }

    #[test]
    fn import_vla_defaults_match_task_defaults() {
        let defaults = ImportVlaOptions::default();
        assert!(defaults.archivefiles.is_empty());
        assert!(defaults.vis.is_none());
        assert!(defaults.bandname.is_none());
        assert_eq!(defaults.frequencytol_hz, 150_000.0);
        assert!(defaults.applytsys);
        assert!(!defaults.autocorr);
        assert_eq!(defaults.antnamescheme, AntennaNameScheme::New);
        assert!(!defaults.keepblanks);
        assert!(!defaults.evlabands);
    }

    #[test]
    fn parse_frequencytol_converts_units_and_reports_invalid_values() {
        assert_eq!(
            ImportVlaOptions::parse_frequencytol("150000Hz").unwrap(),
            150_000.0
        );
        assert_eq!(
            ImportVlaOptions::parse_frequencytol("150kHz").unwrap(),
            150_000.0
        );

        let error = ImportVlaOptions::parse_frequencytol("bogus").expect_err("invalid quantity");
        assert!(matches!(
            error,
            VlaError::InvalidArgument {
                argument: "frequencytol",
                ..
            }
        ));
    }

    #[test]
    fn require_archivefiles_rejects_empty_lists_and_returns_configured_paths() {
        let error = ImportVlaOptions::default()
            .require_archivefiles()
            .expect_err("missing archives");
        assert!(matches!(error, VlaError::NoArchiveFiles));

        let options = ImportVlaOptions {
            archivefiles: vec![PathBuf::from("a.exp"), PathBuf::from("b.xp1")],
            ..ImportVlaOptions::default()
        };
        assert_eq!(
            options.require_archivefiles().unwrap(),
            &[PathBuf::from("a.exp"), PathBuf::from("b.xp1")]
        );
    }
}

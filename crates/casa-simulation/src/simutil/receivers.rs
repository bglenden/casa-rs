// SPDX-License-Identifier: LGPL-3.0-or-later
//! What `simobserve` assumes of each telescope's antennas and receivers:
//! `simutil.noisetemp`, `bandname` and `polsettings`.

use casa_types::quanta::Quantity;

use super::{SimutilError, qa};

/// `simutil.noisetemp`'s efficiencies and receiver temperature for one
/// telescope and frequency.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct ReceiverEfficiencies {
    /// `eta_p`, the Ruze phase efficiency of the surface.
    pub phase: f64,
    /// `eta_s`, the spillover (main beam) efficiency.
    pub spillover: f64,
    /// `eta_b`, the geometrical blockage efficiency of the subreflector.
    pub blockage: f64,
    /// `eta_t`, the taper efficiency.
    pub taper: f64,
    /// `eta_q`, the correlator efficiency, quantization included.
    pub correlator: f64,
    /// `t_rx`, the receiver temperature, K.
    pub receiver_temperature_k: f64,
}

impl ReceiverEfficiencies {
    /// `eta_a = eta_p * eta_s * eta_b * eta_t`, as `simobserve` forms it.
    pub fn antenna(&self) -> f64 {
        self.phase * self.spillover * self.blockage * self.taper
    }
}

/// π as `noisetemp`'s Ruze formula writes it, to ten decimals.
#[allow(
    clippy::approx_constant,
    reason = "CASA's truncated π, kept so the efficiencies are CASA's"
)]
const CASA_PI: f64 = 3.1415926535;

/// The telescopes `noisetemp` knows, with their dish diameter (m),
/// subreflector diameter (m), surface accuracy (µm RMS) and correlator
/// efficiency. Any other telescope takes ALMA's.
const TELESCOPES: [(&str, f64, f64, f64, f64); 6] = [
    ("ALMASD", 12.0, 0.75, 25.0, 0.845),
    ("ALMA", 12.0, 0.75, 25.0, 0.845),
    ("ACA", 7.0, 0.75, 20.0, 0.88),
    ("EVLA", 25.0, 0.364, 300.0, 0.79),
    ("VLA", 25.0, 0.364, 300.0, 0.86),
    ("SMA", 6.0, 0.35, 15.0, 0.88),
];

/// A receiver table: band lower edges (GHz), the temperature (K) from
/// each edge to the next, and the frequency range it is meant for (GHz).
struct ReceiverTable {
    edges_ghz: &'static [f64],
    temperatures_k: &'static [f64],
    range_ghz: [f64; 2],
}

/// The receiver table for a telescope (upper case).
fn receiver_table(telescope: &str) -> ReceiverTable {
    match telescope {
        // CASR-669.
        "ALMA" | "ACA" | "ALMASD" => ReceiverTable {
            edges_ghz: &[
                35.0, 50.0, 84.0, 116.0, 162.0, 211.0, 275.0, 373.0, 500.0, 720.0, 950.0,
            ],
            temperatures_k: &[
                28.0, 30.0, 40.0, 42.0, 50.0, 50.0, 72.0, 135.0, 105.0, 230.0,
            ],
            range_ghz: [35.0, 950.0],
        },
        "EVLA" => ReceiverTable {
            edges_ghz: &[1.0, 2.0, 4.0, 8.0, 12.0, 18.0, 26.5, 40.0, 50.0],
            temperatures_k: &[10.0, 15.0, 12.0, 15.0, 10.0, 12.0, 15.0, 28.0],
            range_ghz: [0.8, 50.0],
        },
        "VLA" => ReceiverTable {
            edges_ghz: &[0.30, 0.34, 1.73, 5.0, 8.8, 15.4, 24.0, 50.0],
            temperatures_k: &[165.0, 56.0, 44.0, 34.0, 110.0, 110.0, 110.0],
            range_ghz: [0.305, 50.0],
        },
        "SMA" => ReceiverTable {
            edges_ghz: &[180.0, 250.0, 320.0, 420.0, 720.0],
            temperatures_k: &[67.0, 116.0, 134.0, 500.0],
            range_ghz: [180.0, 720.0],
        },
        _ => {
            tracing::warn!("I don't know about the {telescope} receivers, using 200K");
            ReceiverTable {
                edges_ghz: &[10.0, 900.0],
                temperatures_k: &[200.0, 200.0],
                range_ghz: [0.0, 5000.0],
            }
        }
    }
}

impl ReceiverTable {
    /// The temperature of the band holding `frequency_ghz`: below the
    /// table's range its first temperature; otherwise that of the last edge
    /// below the frequency (the first band at the first edge, and the last
    /// temperature above the last edge).
    fn temperature(&self, frequency_ghz: f64) -> f64 {
        if frequency_ghz < self.range_ghz[0] {
            tracing::warn!(
                "observing freqency is lower than expected; proceeding with extrapolated receiver temp={}",
                self.temperatures_k[0]
            );
            return self.temperatures_k[0];
        }
        let mut band = 0;
        while band < self.temperatures_k.len() && self.edges_ghz[band] < frequency_ghz {
            band += 1;
        }
        let temperature = self.temperatures_k[band.saturating_sub(1)];
        if frequency_ghz > self.range_ghz[1] {
            tracing::warn!(
                "observing freqency is higher than expected; proceeding with extrapolated receiver temp={temperature}"
            );
        }
        temperature
    }
}

/// `simutil.noisetemp(telescope, freq, diam, epsilon, correlator)`.
///
/// The efficiencies of ALMA, ACA (and ALMA's total power, `ALMASD`), the
/// (E)VLA and the SMA, and their receiver temperatures; any other telescope
/// takes ALMA's efficiencies and 200 K. `diameter` (m) and `epsilon` (the
/// surface accuracy, µm RMS) default to the telescope's; `correlator` picks
/// ALMA's and ACA's correlator efficiencies (`ACASpec`, `ACA`, `BLC`,
/// `BLC_4x4bit`).
pub fn noise_temperature(
    telescope: &str,
    frequency: &Quantity,
    diameter: Option<f64>,
    epsilon: Option<f64>,
    correlator: Option<&str>,
) -> Result<ReceiverEfficiencies, SimutilError> {
    let telescope = telescope.to_uppercase();
    let (_, dish, subreflector, surface, mut correlator_efficiency) = TELESCOPES
        .iter()
        .find(|(name, ..)| *name == telescope)
        .copied()
        .unwrap_or(TELESCOPES[1]);
    let diameter = diameter.unwrap_or(dish);
    let blockage = 1.0 - (subreflector / diameter).powi(2);
    let frequency_ghz = qa::value_in(frequency, "GHz")?;
    let epsilon = epsilon.unwrap_or(surface);
    let ruze = 4.0 * CASA_PI * epsilon * frequency_ghz / 2.99792458e5;
    let phase = (-(ruze * ruze)).exp();
    // CAS-14652: the ALMA and ACA correlators.
    correlator_efficiency = match (telescope.as_str(), correlator.unwrap_or("")) {
        ("ALMASD", "ACASpec") => 0.96,
        ("ALMASD", "ACA") | ("ALMA" | "ACA", "BLC_4x4bit") | ("ACA", "ACA") => 0.95,
        ("ALMA" | "ACA", "BLC") => 0.845,
        _ => correlator_efficiency,
    };
    Ok(ReceiverEfficiencies {
        phase,
        spillover: 0.95,
        blockage,
        taper: 0.72,
        correlator: correlator_efficiency,
        receiver_temperature_k: receiver_table(&telescope).temperature(frequency_ghz),
    })
}

/// `simutil.bandname(freq)`: ALMA's band number above 90 GHz (`band2` for
/// 230 GHz, as CASA writes it), the radar letters below (`W`, `V`, `Q`,
/// `Ka`, `K`, `U`, `X`, `C`, `S`, `L`), then `UHF` and `P`.
pub fn band_name(frequency_ghz: f64) -> String {
    let letters = [
        (75.0, false, "W"),
        (50.0, false, "V"),
        (40.0, true, "Q"),
        (26.5, true, "Ka"),
        (18.0, true, "K"),
        (12.0, true, "U"),
        (8.0, true, "X"),
        (4.0, true, "C"),
        (2.0, true, "S"),
        (1.0, true, "L"),
        (0.3, true, "UHF"),
    ];
    if frequency_ghz > 90.0 {
        return format!("band{:.0}", 0.01 * frequency_ghz);
    }
    letters
        .iter()
        .find(|(edge, inclusive, _)| {
            if *inclusive {
                frequency_ghz >= *edge
            } else {
                frequency_ghz > *edge
            }
        })
        .map_or("P", |(.., name)| name)
        .to_string()
}

/// `simutil.polsettings(telescope, poltype)`: the `stokes` for
/// `sm.setspwindow` and the feed `mode` for `sm.setfeed`. `circular`,
/// `linear` or `RR`; for any other (or no) type, circular feeds for a
/// telescope whose name contains `VLA` or `DRAO`, linear otherwise.
pub fn polarization_settings(
    telescope: &str,
    poltype: Option<&str>,
) -> (&'static str, &'static str) {
    let circular = ("RR LL", "perfect R L");
    let linear = ("XX YY", "perfect X Y");
    match poltype {
        Some("circular") => circular,
        Some("linear") => linear,
        Some("RR") => ("RR", "perfect R L"),
        _ => {
            let upper = telescope.to_uppercase();
            if upper.contains("VLA") || upper.contains("DRAO") {
                circular
            } else {
                linear
            }
        }
    }
}

// SPDX-License-Identifier: LGPL-3.0-or-later
//! Reader for the NRAO brightness temperature and asteroid flux model tables.

use std::collections::VecDeque;
use std::fs::File;
use std::io::{BufRead, BufReader};
use std::path::Path;

use casa_ms::MsResult;

use super::interpolate::{average_over_band, interpolate};
use crate::ft::invalid;

const PLANCK: f64 = 6.626_075_5e-34;
const BOLTZMANN: f64 = 1.380_657_798_751_064_7e-23;
const LIGHT_SPEED: f64 = 299_792_458.;
const BACKGROUND_K: f64 = 2.725;

pub(super) struct Brightness {
    frequency_hz: Vec<f64>,
    brightness: Vec<f64>,
    pub(super) absolute_flux: bool,
}

impl Brightness {
    pub(super) fn read(root: &Path, body: &str, mjd: f64) -> MsResult<Self> {
        match body {
            "Mars" => Self::read_time(&root.join("Mars_Tb_time.dat"), mjd, false)?
                .ok_or_else(|| invalid("the Mars brightness model does not cover the epoch")),
            "Ceres" | "Pallas" | "Vesta" => {
                if let Some(model) =
                    Self::read_time(&root.join(format!("{body}_fd_time.dat")), mjd, true)?
                {
                    Ok(model)
                } else {
                    Self::read_static(&root.join(format!("{body}_Tb.dat")))
                }
            }
            _ => Self::read_static(&root.join(format!("{body}_Tb.dat"))),
        }
    }

    pub(super) fn sample(&self, band: [f64; 2]) -> Option<f64> {
        average_over_band(&self.frequency_hz, &self.brightness, band)
    }

    fn read_static(path: &Path) -> MsResult<Self> {
        let mut frequency_hz = Vec::new();
        let mut brightness = Vec::new();
        for line in open(path)?.lines() {
            let line = line.map_err(invalid)?;
            let mut fields = line.split_whitespace();
            let frequency = number(fields.next())? * 1e9;
            frequency_hz.push(frequency);
            brightness.push(planck(frequency, number(fields.next())?));
        }
        validate_frequencies(&frequency_hz)?;
        Ok(Self {
            frequency_hz,
            brightness,
            absolute_flux: false,
        })
    }

    fn read_time(path: &Path, mjd: f64, absolute_flux: bool) -> MsResult<Option<Self>> {
        let mut lines = open(path)?.lines();
        let header = lines
            .next()
            .transpose()
            .map_err(invalid)?
            .ok_or_else(|| invalid("empty Solar System model"))?;
        let frequency_hz = header
            .split_whitespace()
            .map(|value| number(Some(value)).map(|freq| freq * 1e9))
            .collect::<MsResult<Vec<_>>>()?;
        validate_frequencies(&frequency_hz)?;
        // Only the interpolation neighborhood is needed. Asteroid files can
        // contain hundreds of thousands of epochs.
        let mut records = VecDeque::with_capacity(22);
        let mut covered = false;
        let mut previous = f64::NEG_INFINITY;
        let mut later = 0;
        for line in lines {
            let line = line.map_err(invalid)?;
            let date = number(line.split_whitespace().nth(5))?;
            if date <= previous {
                return Err(invalid("Solar System model epochs are not increasing"));
            }
            if date > mjd && records.is_empty() {
                return Ok(None);
            }
            previous = date;
            if date >= mjd {
                covered = true;
                later += 1;
            } else if records.len() == 11 {
                records.pop_front();
            }
            records.push_back((date, line));
            if later == 11 {
                break;
            }
        }
        if !covered {
            return Err(invalid("the Solar System model does not cover the epoch"));
        }
        let times: Vec<_> = records.iter().map(|&(time, _)| time).collect();
        let mut by_frequency = vec![Vec::with_capacity(times.len()); frequency_hz.len()];
        for (_, line) in records {
            let mut fields = line.split_whitespace().skip(6);
            for values in &mut by_frequency {
                values.push(number(fields.next())?);
            }
        }
        let brightness = frequency_hz
            .iter()
            .zip(by_frequency)
            .map(|(&frequency, values)| {
                let value = interpolate(&times, &values, mjd);
                if absolute_flux {
                    value
                } else {
                    planck(frequency, value)
                }
            })
            .collect();
        Ok(Some(Self {
            frequency_hz,
            brightness,
            absolute_flux,
        }))
    }
}

fn open(path: &Path) -> MsResult<BufReader<File>> {
    File::open(path)
        .map(BufReader::new)
        .map_err(|error| invalid(format!("cannot open {}: {error}", path.display())))
}

fn number(text: Option<&str>) -> MsResult<f64> {
    text.and_then(|s| s.parse::<f64>().ok())
        .filter(|x| x.is_finite())
        .ok_or_else(|| invalid("invalid numeric value in Solar System model"))
}

fn validate_frequencies(hz: &[f64]) -> MsResult<()> {
    if hz.len() < 2 || hz[0] <= 0. || hz.windows(2).any(|f| f[0] >= f[1]) {
        return Err(invalid(
            "Solar System model frequencies are not positive and increasing",
        ));
    }
    Ok(())
}

fn planck(hz: f64, temperature: f64) -> f64 {
    let activation = PLANCK * hz / BOLTZMANN;
    2. * PLANCK * hz.powi(3) / LIGHT_SPEED.powi(2)
        * ((activation / temperature).exp_m1().recip()
            - (activation / BACKGROUND_K).exp_m1().recip())
}

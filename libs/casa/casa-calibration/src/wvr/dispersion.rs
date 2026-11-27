// SPDX-License-Identifier: LGPL-3.0-or-later
//! Frequency interpolation of the supplied wet-path dispersion reference.

use std::path::{Path, PathBuf};

use super::WvrError;

pub(super) struct DispersionTable(Vec<(f64, f64)>);

impl DispersionTable {
    pub fn reference_path() -> Result<PathBuf, WvrError> {
        let filename = "libair-ddefault.csv";
        if let Some(directory) = std::env::var_os("WVRGCAL_DISPDIR") {
            let explicit = PathBuf::from(directory).join(filename);
            if explicit.is_file() {
                return Ok(explicit);
            }
        }
        let roots = casa_measures::MeasuresRuntime::discover_candidates()
            .map_err(|error| WvrError::Invalid(error.to_string()))?;
        roots.into_iter()
            .map(|root| root.path.join("alma/wvrgcal").join(filename))
            .find(|path| path.is_file())
            .ok_or_else(|| WvrError::Invalid(format!(
                "Could not find {filename} in WVRGCAL_DISPDIR or alma/wvrgcal in the CASA runtime data directories"
            )))
    }

    pub fn load(path: &Path) -> Result<Self, WvrError> {
        let contents = std::fs::read_to_string(path)?;
        let mut entries: Vec<_> = contents
            .lines()
            .filter_map(|line| {
                let (frequency, correction) = line.split_once(',')?;
                let frequency: f64 = frequency.trim().parse().ok()?;
                let correction: f64 = correction.trim().parse().ok()?;
                (frequency.is_finite() && correction.is_finite()).then_some((frequency, correction))
            })
            .collect();
        entries.sort_by(|left, right| left.0.total_cmp(&right.0));
        entries.dedup_by(|left, right| left.0 == right.0);
        if entries.is_empty() {
            return Err(WvrError::Invalid(format!(
                "No usable dispersion samples in {}",
                path.display()
            )));
        }
        Ok(Self(entries))
    }

    pub fn at(&self, frequency: f64) -> Result<f64, WvrError> {
        match self
            .0
            .binary_search_by(|entry| entry.0.total_cmp(&frequency))
        {
            Ok(index) => Ok(self.0[index].1),
            Err(index) if index > 0 && index < self.0.len() => {
                let (lower, lower_correction) = self.0[index - 1];
                let (upper, upper_correction) = self.0[index];
                Ok(lower_correction
                    + (frequency - lower) / (upper - lower) * (upper_correction - lower_correction))
            }
            Err(_) => Err(WvrError::Invalid(format!(
                "Frequency {frequency} Hz lies outside the WVR dispersion table"
            ))),
        }
    }
}

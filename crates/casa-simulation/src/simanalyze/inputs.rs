// SPDX-License-Identifier: LGPL-3.0-or-later
//! Project MS discovery and simanalyze's diameter-squared concat weights.
use super::{Parameters, failure};
use crate::simobserve::SimobserveError;
use crate::simutil::analysis;
use casa_core::ScalarValue;
use casa_ms::MeasurementSet;
use std::path::{Path, PathBuf};

pub(super) struct Inputs {
    pub synthesis: Vec<PathBuf>,
    pub total_power: Option<PathBuf>,
}

pub(super) fn discover(
    p: &Parameters<'_>,
    directory: &Path,
    candidates: &[PathBuf],
) -> Result<Inputs, SimobserveError> {
    let requested = p.strings("vis");
    let default = requested.is_empty() || requested == ["default"];
    let sets = if default {
        candidates
            .iter()
            .filter(|path| path.to_string_lossy().ends_with(".ms"))
            .filter(|path| {
                let name = path.file_name().unwrap().to_string_lossy();
                let noisy = if name.ends_with(".sd.ms") {
                    name.replace(".sd.ms", ".noisy.sd.ms")
                } else {
                    name.replace(".ms", ".noisy.ms")
                };
                name.contains(".noisy.") || !directory.join(noisy).exists()
            })
            .cloned()
            .collect::<Vec<_>>()
    } else {
        requested
            .iter()
            .flat_map(|value| value.split(','))
            .map(|value| p.locate(directory, value.trim()))
            .filter(|path| path.exists())
            .collect()
    };
    let mut result = Inputs {
        synthesis: Vec::new(),
        total_power: None,
    };
    for path in sets {
        if analysis::is_total_power(&path).map_err(failure)? {
            if result.total_power.replace(path).is_some() {
                return Err(failure(
                    "CASA silently discards extra total-power MS inputs; select one with vis (#841)",
                ));
            }
        } else {
            result.synthesis.push(path);
        }
    }
    // A previous concatenation is an output, not another independent input.
    let combined = directory.join(format!(
        "{}.concat.ms",
        directory.file_name().unwrap().to_string_lossy()
    ));
    if default && result.synthesis.len() > 1 {
        result.synthesis.retain(|path| path != &combined);
    }
    if result.synthesis.is_empty() && result.total_power.is_none() && p.flag("image") {
        return Err(failure("No MS found to image"));
    }
    Ok(result)
}

pub(super) fn concat_weights(inputs: &[PathBuf]) -> Result<Vec<f64>, SimobserveError> {
    let mut weights = Vec::with_capacity(inputs.len());
    let mut reference = None;
    for path in inputs {
        let ms = MeasurementSet::open(path).map_err(failure)?;
        let antennas = ms.antenna().map_err(failure)?;
        let diameter = antennas.dish_diameter(0).map_err(failure)?;
        if ms.spectral_window().map_err(failure)?.row_count() != 1 {
            return Err(failure(
                "concat weights require one spectral window per MS; concatenate manually",
            ));
        }
        for row in 0..antennas.row_count() {
            if antennas.dish_diameter(row).map_err(failure)? != diameter {
                return Err(failure(
                    "concat weights require one antenna diameter per MS; concatenate manually",
                ));
            }
        }
        let interval = ms
            .main_table()
            .column_accessor("INTERVAL")
            .map_err(failure)?;
        let ScalarValue::Float64(integration) = interval.scalar_cell(0).map_err(failure)? else {
            return Err(failure("invalid MS integration interval"));
        };
        let integration = *integration;
        for row in 0..ms.main_table().row_count() {
            if interval.scalar_cell(row).map_err(failure)? != &ScalarValue::Float64(integration) {
                return Err(failure(
                    "concat weights require constant integration time; concatenate manually",
                ));
            }
        }
        let (first_diameter, first_integration) = *reference.get_or_insert((diameter, integration));
        if integration != first_integration {
            return Err(failure(
                "concat weights require matching integration times; concatenate manually",
            ));
        }
        weights.push((diameter / first_diameter).powi(2));
    }
    Ok(weights)
}

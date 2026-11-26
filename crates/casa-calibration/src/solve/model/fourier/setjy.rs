// SPDX-License-Identifier: LGPL-3.0-or-later
//! Scale a reopened SetJyGridFT by its recorded LSRK flux spectrum.

use casa_core::{ArrayValue, RecordValue, Value};

use super::{GainSolveError, model_error};

pub(super) struct FluxScale {
    frequencies: Vec<f64>,
    amplitudes: Vec<f64>,
}

impl FluxScale {
    pub(super) fn from_record(record: &RecordValue) -> Result<Option<Self>, GainSolveError> {
        let vector = |name| match record.get(name) {
            Some(Value::Array(ArrayValue::Float64(values))) if values.ndim() == 1 => {
                Ok(values.iter().copied().collect::<Vec<_>>())
            }
            _ => Err(model_error(format!(
                "virtual SetJyGridFT has no {name} vector"
            ))),
        };
        let amplitudes = vector("scaleamp")?;
        if amplitudes.is_empty() {
            return Ok(None);
        }
        let frequencies = vector("freqscale")?;
        if amplitudes.len() != frequencies.len()
            || (!(frequencies.is_sorted_by(|a, b| a < b))
                && !(frequencies.is_sorted_by(|a, b| a > b)))
            || frequencies
                .iter()
                .chain(&amplitudes)
                .any(|number| !number.is_finite())
        {
            return Err(model_error("virtual SetJyGridFT has invalid flux samples"));
        }
        Ok(Some(Self {
            frequencies,
            amplitudes,
        }))
    }

    pub(super) fn at(&self, frequency: f64) -> f32 {
        let count = self.amplitudes.len();
        if count == 1 {
            return self.amplitudes[0] as f32;
        }
        let descending = self.frequencies[0] > self.frequencies[count - 1];
        let upper = self
            .frequencies
            .partition_point(|value| (*value < frequency) ^ descending)
            .clamp(1, count - 1);
        let [low_hz, high_hz] = [self.frequencies[upper - 1], self.frequencies[upper]];
        let [low, high] = [self.amplitudes[upper - 1], self.amplitudes[upper]];
        // CASA chooses nearest for up to two samples and interpolates or
        // extrapolates linearly for three or more.
        if count == 2 {
            if (high_hz - frequency).abs() < (low_hz - frequency).abs() {
                high as f32
            } else {
                low as f32
            }
        } else {
            (low + (frequency - low_hz) / (high_hz - low_hz) * (high - low)) as f32
        }
    }
}

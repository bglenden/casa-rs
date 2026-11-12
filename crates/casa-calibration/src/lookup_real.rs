// SPDX-License-Identifier: LGPL-3.0-or-later
//! Signed FPARAM samples share the complex lookup's storage and shape validation.
use super::*;
use casa_types::ArrayValue;

impl GainGrid {
    pub(crate) fn from_real_arrays(
        path: &Path,
        values: &ArrayValue,
        flags: &ArrayValue,
    ) -> Result<Self, ApplyExecutionError> {
        let values = match values {
            ArrayValue::Float32(values) => values.mapv(|v| Complex32::new(v, 0.)),
            ArrayValue::Float64(values) => values.mapv(|v| Complex32::new(v as f32, 0.)),
            other => {
                return Err(ApplyExecutionError::UnsupportedParameterShape {
                    path: path.display().to_string(),
                    shape: other.shape().to_vec(),
                });
            }
        };
        Self::from_arrays(path, &ArrayValue::Complex32(values), flags)
    }
}

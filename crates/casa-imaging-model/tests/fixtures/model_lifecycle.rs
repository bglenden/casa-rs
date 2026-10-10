// SPDX-License-Identifier: LGPL-3.0-or-later

use casa_imaging_model::{ModelBounds, ModelLifecycleRequirements, NumericPrecision};

pub fn model_lifecycle() -> ModelLifecycleRequirements {
    ModelLifecycleRequirements::new(
        ModelBounds::new(10_000_000, 10_000_000, 1.0e30, 1.0e30)
            .expect("valid model lifecycle fixture bounds"),
        NumericPrecision::F64,
    )
}

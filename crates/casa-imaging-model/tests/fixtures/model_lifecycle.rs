// SPDX-License-Identifier: LGPL-3.0-or-later

use casa_imaging_model::{
    ModelBounds, ModelInputCommitment, ModelLifecycleRequirements, ModelStateIdentity,
    NumericPrecision,
};

use crate::common::identity;

pub fn model_lifecycle(model: ModelStateIdentity) -> ModelLifecycleRequirements {
    let input = match model {
        ModelStateIdentity::Empty => ModelInputCommitment::Empty,
        ModelStateIdentity::Seed(source) => ModelInputCommitment::AlignedSeed {
            source,
            support: identity(0xa5),
        },
        ModelStateIdentity::Generation(generation) => ModelInputCommitment::Generation(generation),
    };
    ModelLifecycleRequirements::new(
        ModelBounds::new(
            10_000_000, 10_000_000, 10_000_000, 10_000_000, 1.0e30, 1.0e30,
        )
        .expect("valid model lifecycle fixture bounds"),
        NumericPrecision::F64,
        input,
    )
}

// SPDX-License-Identifier: LGPL-3.0-or-later
//! Execution and cycle choices read from the canonical imaging parameters.

use super::{GridPrecision, HogbomIterationAccounting, ProductNormalization, choice, unexpected};
use serde::{Deserialize, Deserializer, de::Error as _};
use serde_json::Value;

pub(super) fn major_cycle_limit<'de, D: Deserializer<'de>>(
    deserializer: D,
) -> Result<Option<usize>, D::Error> {
    let value = Value::deserialize(deserializer)?;
    match value.as_i64() {
        Some(-1) => Ok(None),
        Some(limit) => usize::try_from(limit)
            .map(Some)
            .map_err(|_| unexpected(&value, "-1 or a major-cycle count")),
        None => Err(unexpected(&value, "-1 or a major-cycle count")),
    }
}

pub(super) fn hogbom_iteration_mode<'de, D: Deserializer<'de>>(
    deserializer: D,
) -> Result<HogbomIterationAccounting, D::Error> {
    match choice(deserializer)?.as_str() {
        "strict" => Ok(HogbomIterationAccounting::Strict),
        "casa-inclusive" => Ok(HogbomIterationAccounting::CasaInclusive),
        other => Err(D::Error::custom(format!("hogbom_iteration_mode {other:?}"))),
    }
}

pub(super) fn normalization<'de, D: Deserializer<'de>>(
    deserializer: D,
) -> Result<ProductNormalization, D::Error> {
    match choice(deserializer)?.as_str() {
        "flatnoise" => Ok(ProductNormalization::FlatNoise),
        "flatsky" => Ok(ProductNormalization::FlatSky),
        "pbsquare" => Ok(ProductNormalization::PbSquare),
        other => Err(D::Error::custom(format!("normtype {other:?}"))),
    }
}

pub(super) fn grid_precision<'de, D: Deserializer<'de>>(
    deserializer: D,
) -> Result<Option<GridPrecision>, D::Error> {
    match choice(deserializer)?.as_str() {
        "auto" => Ok(None),
        "f32" => Ok(Some(GridPrecision::F32)),
        "f64" => Ok(Some(GridPrecision::F64)),
        other => Err(D::Error::custom(format!("gridprecision {other:?}"))),
    }
}

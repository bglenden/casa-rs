// SPDX-License-Identifier: LGPL-3.0-or-later
//! Resolve ALMA absolute antenna positions into offsets from the MS positions.

use std::{collections::HashMap, fs, io, path::Path};

use casa_ms::{MeasurementSet, schema::SubtableId};
use serde_json::Value;
use thiserror::Error;

use super::{GencalError, GencalRequest, get_string};

/// Errors reading an ALMA antenna-position input file.
#[derive(Debug, Error)]
pub enum AntposInputError {
    /// The source could not be read.
    #[error("Cannot open {path}: {source}")]
    Io {
        /// Source path.
        path: String,
        /// Filesystem failure.
        source: io::Error,
    },
    /// The source did not contain JSON.
    #[error(transparent)]
    Json(#[from] serde_json::Error),
    /// A required JSON object key is absent.
    #[error("{0}")]
    Key(&'static str),
    /// One antenna position lacks one of the first three coordinates.
    #[error("Antenna {0} position needs three coordinates")]
    Index(String),
    /// A value does not support the operation used to read it.
    #[error("{0}")]
    Type(String),
    /// The positions container has no object entries.
    #[error("The data value must have named antenna positions")]
    Attribute,
    /// The positions are incompatible with the request.
    #[error("{0}")]
    Invalid(String),
}

pub(super) fn resolve(
    ms: &MeasurementSet,
    request: &GencalRequest,
    polarization: &str,
    path: &Path,
) -> Result<GencalRequest, GencalError> {
    let invalid = |reason| GencalError::from(AntposInputError::Invalid(reason));
    if !request.antenna.is_empty() || !polarization.is_empty() || !request.parameter.is_empty() {
        return Err(invalid(
            "When using infile for ALMA the caltype is antpos, antenna, pol and parameter must be empty"
                .into(),
        ));
    }
    let observation = ms
        .subtable(SubtableId::Observation)
        .ok_or_else(|| invalid("Missing OBSERVATION subtable".into()))?;
    let telescope = get_string(observation, 0, "TELESCOPE_NAME")?;
    let file = fs::read(path).map_err(|source| AntposInputError::Io {
        path: path.display().to_string(),
        source,
    })?;
    let positions: Value = serde_json::from_slice(&file).map_err(AntposInputError::from)?;
    if telescope != "ALMA" {
        return Err(invalid(format!(
            "Antenna positions are from ALMA but MS is from {telescope} telescope"
        )));
    }
    if object_key(object_key(&positions, "metadata")?, "product_code")? != "antposalma" {
        return Err(invalid(
            "JSON file with antenna positions is not from ALMA. This is currently not supported"
                .into(),
        ));
    }
    let data = object_key(&positions, "data")?
        .as_object()
        .ok_or(AntposInputError::Attribute)?;
    if data.is_empty() {
        return Err(invalid(
            "The list of antenna positions in JSON file is empty".into(),
        ));
    }
    let antennas = ms.antenna().map_err(|error| invalid(error.to_string()))?;
    let mut nominal_rows = HashMap::new();
    for row in 0..antennas.row_count() {
        let name = antennas
            .name(row)
            .map_err(|error| invalid(error.to_string()))?;
        nominal_rows.entry(name).or_insert(row);
    }
    let mut resolved = request.clone();
    let mut names = Vec::with_capacity(data.len());
    for (name, corrected) in data {
        let row = nominal_rows
            .get(name)
            .ok_or_else(|| invalid(format!("Antenna {name} could not be found in input MS")))?;
        let nominal = antennas
            .position(*row)
            .map_err(|error| invalid(error.to_string()))?;
        let coordinates = corrected.as_array().ok_or_else(|| {
            AntposInputError::Type(format!("Antenna {name} position must contain numbers"))
        })?;
        for (index, nominal) in nominal.into_iter().enumerate() {
            let coordinate = coordinates
                .get(index)
                .ok_or_else(|| AntposInputError::Index(name.clone()))?;
            let value = coordinate
                .as_f64()
                .or_else(|| coordinate.as_bool().map(|value| f64::from(u8::from(value))))
                .ok_or_else(|| {
                    AntposInputError::Type(format!("Antenna {name} position must contain numbers"))
                })?;
            resolved.parameter.push(value - nominal);
        }
        names.push(name.as_str());
    }
    resolved.antenna = names.join(",");
    Ok(resolved)
}

fn object_key<'a>(value: &'a Value, key: &'static str) -> Result<&'a Value, AntposInputError> {
    value
        .as_object()
        .ok_or_else(|| {
            AntposInputError::Type(format!("Cannot read {key} from a non-object value"))
        })?
        .get(key)
        .ok_or(AntposInputError::Key(key))
}

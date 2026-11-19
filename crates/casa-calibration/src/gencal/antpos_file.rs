// SPDX-License-Identifier: LGPL-3.0-or-later
//! Resolve ALMA absolute antenna positions into offsets from the MS positions.

use std::{collections::HashMap, fs::File, io::BufReader, path::Path};

use casa_ms::{MeasurementSet, schema::SubtableId};
use serde::Deserialize;

use super::{GencalError, GencalRequest, get_string};

#[derive(Deserialize)]
struct PositionFile {
    metadata: Metadata,
    data: HashMap<String, Vec<f64>>,
}

#[derive(Deserialize)]
struct Metadata {
    product_code: String,
}

pub(super) fn resolve(
    ms: &MeasurementSet,
    request: &GencalRequest,
    polarization: &str,
    path: &Path,
) -> Result<GencalRequest, GencalError> {
    let invalid = GencalError::PositionInput;
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
    let file = File::open(path)
        .map_err(|error| invalid(format!("Cannot open {}: {error}", path.display())))?;
    let positions: PositionFile = serde_json::from_reader(BufReader::new(file))
        .map_err(|error| invalid(format!("Cannot read {}: {error}", path.display())))?;
    if telescope != "ALMA" {
        return Err(invalid(format!(
            "Antenna positions are from ALMA but MS is from {telescope} telescope"
        )));
    }
    if positions.metadata.product_code != "antposalma" {
        return Err(invalid(
            "JSON file with antenna positions is not from ALMA. This is currently not supported"
                .into(),
        ));
    }
    if positions.data.is_empty() {
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
    let mut names = Vec::with_capacity(positions.data.len());
    for (name, corrected) in positions.data {
        let row = nominal_rows
            .get(&name)
            .ok_or_else(|| invalid(format!("Antenna {name} could not be found in input MS")))?;
        let nominal = antennas
            .position(*row)
            .map_err(|error| invalid(error.to_string()))?;
        let [x, y, z, ..] = corrected.as_slice() else {
            return Err(invalid(format!(
                "Antenna {name} position needs three coordinates"
            )));
        };
        resolved
            .parameter
            .extend([x - nominal[0], y - nominal[1], z - nominal[2]]);
        names.push(name);
    }
    resolved.antenna = names.join(",");
    Ok(resolved)
}

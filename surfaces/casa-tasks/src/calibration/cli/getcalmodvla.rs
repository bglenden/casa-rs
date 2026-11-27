// SPDX-License-Identifier: LGPL-3.0-or-later
//! Retrieve and publish a VLA calibrator component list and its provenance.

use std::fmt::Write as _;
use std::path::Path;
use std::time::Duration;

use crate::{TaskFailure, TaskInvocation, TaskSuccess};
use casa_components::ComponentList;
use casa_contracts::{CasaValue, ParameterValue, casa_numpy_scalar_root};
use casa_core::measures::direction::casa_direction_frame_name;
use casa_core::quanta::{Quantity, Unit};
use casa_core::{ArrayValue, RecordField, RecordValue, ScalarValue, Value};
use casa_tables::publication::{exchange_directories, rename_without_replacing};
use casa_tables::{Table, TableOptions};
use ndarray::Array1;
use serde_json::{Map, Value as Json};
use time::OffsetDateTime;

const HTTP_SERVICE: &str = "http://obs.vla.nrao.edu/calmodvla";
const HTTPS_SERVICE: &str = "https://obs.vla.nrao.edu/calmodvla";

pub(super) fn run(task: &TaskInvocation<'_>) -> Result<TaskSuccess, TaskFailure> {
    let outfile = task.required_text("outfile")?;
    if outfile.trim().is_empty() {
        return Err(invalid("outfile", "outfile must be specified"));
    }
    let requested_path = Path::new(&outfile);
    let destination = task.workspace().join(requested_path);
    let overwrite = task.flag("overwrite")?;
    if !overwrite && destination.symlink_metadata().is_ok() {
        return Err(exists(requested_path));
    }
    let source = task.required_text("source")?;
    let direction = task.required_text("direction")?;
    if source.trim().is_empty() && direction.trim().is_empty() {
        return Err(invalid(
            "source",
            "Exactly one of source or direction must be specified",
        ));
    }
    if !source.is_empty() && !direction.is_empty() {
        return Err(invalid(
            "direction",
            "Both source and direction may not be simultaneously specified",
        ));
    }
    if !direction.is_empty() {
        validate_direction(&direction)?;
    }
    let source = source.to_uppercase();
    let band = task.required_text("band")?;
    if band.is_empty() {
        return Err(invalid("band", "band must be specified"));
    }
    if !matches!(
        band.to_ascii_uppercase().as_str(),
        "P" | "L" | "S" | "C" | "X" | "U" | "K" | "A" | "Q"
    ) {
        return Err(invalid("band", format!("band {band} not supported")));
    }
    let (obsdate, obs_mjd) = observation_date(task.value("obsdate"), "obsdate")?;
    let (refdate, ref_mjd) = observation_date(task.value("refdate"), "refdate")?;
    let hosts = task.texts("hosts")?;
    if hosts.is_empty() {
        return Err(invalid("hosts", "hosts must be specified"));
    }
    let selector = if source.is_empty() {
        format!("position={}", quote(&direction))
    } else {
        format!("source={}", quote(&source))
    };
    let mut query = format!(
        "type=setjy&{selector}&band={}&obsdate={}",
        quote(&band),
        obs_mjd as i64
    );
    if ref_mjd > 0. {
        write!(query, "&refdate={}", ref_mjd as i64).expect("writing to a string");
    }
    let (url, response) = retrieve(&hosts, &query)?;
    let components = ComponentList::from_import_record(&response).map_err(execution)?;
    let now = OffsetDateTime::now_local()
        .unwrap_or_else(|_| OffsetDateTime::now_utc())
        .date();
    let mut provenance = RecordValue::new(Vec::new());
    if !source.is_empty() {
        provenance.upsert("source", text(source));
    }
    if !direction.is_empty() {
        provenance.upsert("direction", text(direction));
    }
    if let Some(obsdate) = obsdate {
        provenance.upsert("obsdate", obsdate);
    }
    if let Some(refdate) = refdate {
        provenance.upsert("refdate", refdate);
    }
    provenance.upsert("band", text(band));
    provenance.upsert(
        "hosts",
        Value::Array(ArrayValue::String(Array1::from(hosts).into_dyn())),
    );
    provenance.upsert(
        "run_mjd",
        Value::Scalar(ScalarValue::Int32(now.to_julian_day() - 2_400_001)),
    );
    provenance.upsert("run_date", text(now.to_string()));
    provenance.upsert("url", text(url));
    provenance.upsert("response", Value::Record(response));
    publish(
        &destination,
        requested_path,
        overwrite,
        &components,
        provenance,
    )?;
    tracing::info!(path = %destination.display(), "component list has been written");
    TaskSuccess::new(CasaValue::None, &())
}

fn invalid(parameter: &'static str, message: impl Into<String>) -> TaskFailure {
    TaskFailure::validation(message).at(parameter)
}

fn execution(error: impl std::fmt::Display) -> TaskFailure {
    TaskFailure::execution(error.to_string())
}

fn exists(path: &Path) -> TaskFailure {
    execution(format!(
        "The overwrite parameter is False and a file or directory named {} already exists. \
         Either remove or rename it, or change overwrite to True, or both.",
        path.display()
    ))
    .at("outfile")
}

fn text(value: String) -> Value {
    Value::Scalar(ScalarValue::String(value))
}

fn validate_direction(direction: &str) -> Result<(), TaskFailure> {
    let parts = direction.split(' ').collect::<Vec<_>>();
    let rad = Unit::new("rad").expect("rad is a unit");
    if let [frame, longitude, latitude] = parts.as_slice()
        && [longitude, latitude].into_iter().all(|angle| {
            Quantity::read(angle)
                .ok()
                .and_then(|q| q.get_value_in(&rad).ok())
                .is_some()
        })
    {
        if casa_direction_frame_name(frame).is_none() {
            tracing::warn!(%frame, "unknown direction reference frame; CASA accepts the angles with its default frame");
        }
        Ok(())
    } else {
        Err(invalid(
            "direction",
            format!("Illegal direction specification {direction}"),
        ))
    }
}

fn observation_date(
    value: Option<&ParameterValue>,
    name: &'static str,
) -> Result<(Option<Value>, f64), TaskFailure> {
    let date = match value {
        Some(ParameterValue::Float(value)) if value.is_finite() => {
            return Ok((
                (*value != 0.).then_some(Value::Scalar(ScalarValue::Float64(*value))),
                *value,
            ));
        }
        Some(ParameterValue::Integer(value)) => {
            if let Ok(small) = i32::try_from(*value) {
                return Ok((
                    (*value != 0).then_some(Value::Scalar(ScalarValue::Int32(small))),
                    *value as f64,
                ));
            }
            return Ok((
                Some(Value::Scalar(ScalarValue::Int64(*value))),
                *value as f64,
            ));
        }
        Some(ParameterValue::Bool(value)) => {
            return Ok((
                value.then_some(Value::Scalar(ScalarValue::Bool(true))),
                if *value { 1. } else { 0. },
            ));
        }
        Some(value @ ParameterValue::NumpyScalar { .. }) => {
            let converted = casa_numpy_scalar_root(value).map_err(|error| invalid(name, error))?;
            return observation_date(Some(&converted), name);
        }
        Some(ParameterValue::String(value)) => value,
        _ => {
            return Err(invalid(
                name,
                format!("{name} must either be a number or a string of the form YYYY-MM-DD"),
            ));
        }
    };
    let syntax = date.len() == 10
        && date.bytes().enumerate().all(|(i, b)| {
            if matches!(i, 4 | 7) {
                b == b'-'
            } else {
                b.is_ascii_digit()
            }
        });
    let parsed = syntax
        .then(|| Quantity::read(date).ok())
        .flatten()
        .and_then(|q| q.get_value_in(&Unit::new("d").expect("d is a unit")).ok());
    let mjd = parsed.ok_or_else(|| {
        invalid(
            name,
            format!("If specified as a string, {name} must be of the form YYYY-MM-DD"),
        )
    })?;
    Ok((Some(text(date.clone())), mjd))
}

fn quote(value: &str) -> String {
    let mut encoded = String::new();
    for byte in value.bytes() {
        if byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'.' | b'_' | b'~' | b'/') {
            encoded.push(byte as char);
        } else {
            write!(encoded, "%{byte:02X}").expect("writing to a string");
        }
    }
    encoded
}

fn retrieve(hosts: &[String], query: &str) -> Result<(String, RecordValue), TaskFailure> {
    let agent = casa_calibration::calibration_service::agent(Duration::from_secs(400))
        .map_err(execution)?;
    for host in hosts {
        if !host
            .parse::<ureq::http::Uri>()
            .is_ok_and(|uri| uri.scheme().is_some() && uri.authority().is_some())
        {
            return Err(invalid(
                "hosts",
                format!("{host} is not a valid host expressed as a URL"),
            ));
        }
        // The default NRAO endpoint serves the same model over HTTPS.
        let host = if host == HTTP_SERVICE {
            HTTPS_SERVICE
        } else {
            host
        };
        let url = format!("{host}?{query}");
        tracing::info!(%url, "querying the VLA calibrator service");
        let queried = agent.get(&url).call().and_then(|mut response| {
            if response.status() == 200 {
                response.body_mut().read_to_string().map(Some)
            } else {
                Ok(None)
            }
        });
        let result = match queried {
            Ok(Some(body)) => match serde_json::from_str::<Json>(&body) {
                Ok(result) => result,
                Err(error) => {
                    tracing::warn!(%url, %error, "invalid JSON in the VLA calibrator response");
                    continue;
                }
            },
            Ok(None) => continue,
            Err(error) => {
                tracing::warn!(%url, %error, "VLA calibrator request failed");
                continue;
            }
        };
        match result {
            Json::Object(record) if !record.is_empty() => return Ok((url, json_record(record)?)),
            Json::Null | Json::Bool(false) | Json::Object(_) => {}
            Json::String(value) if value.is_empty() => {}
            Json::Array(values) if values.is_empty() => {}
            Json::Number(value) if value.as_f64() == Some(0.) => {}
            _ => {
                return Err(execution(
                    "VLA calibrator response is not a component record",
                ));
            }
        }
    }
    Err(execution("All URLs failed to return a component list"))
}

fn json_record(record: Map<String, Json>) -> Result<RecordValue, TaskFailure> {
    record
        .into_iter()
        .map(|(key, entry)| Ok(RecordField::new(key, json_value(entry)?)))
        .collect::<Result<Vec<_>, _>>()
        .map(RecordValue::new)
}

fn json_value(value: Json) -> Result<Value, TaskFailure> {
    Ok(match value {
        Json::Bool(value) => Value::Scalar(ScalarValue::Bool(value)),
        Json::Number(value) => match value.as_i64() {
            Some(integer) => Value::Scalar(
                i32::try_from(integer).map_or(ScalarValue::Int64(integer), ScalarValue::Int32),
            ),
            None => Value::Scalar(ScalarValue::Float64(
                value
                    .as_f64()
                    .ok_or_else(|| execution("non-finite number in component record"))?,
            )),
        },
        Json::String(value) => text(value),
        Json::Object(record) => Value::Record(json_record(record)?),
        Json::Array(values) if values.iter().all(Json::is_number) => {
            Value::Array(ArrayValue::from_f64_vec(
                values
                    .iter()
                    .map(|value| value.as_f64().expect("numeric value"))
                    .collect(),
            ))
        }
        Json::Array(values) if values.iter().all(Json::is_string) => {
            Value::Array(ArrayValue::String(
                Array1::from_iter(
                    values
                        .iter()
                        .map(|value| value.as_str().expect("text value").to_string()),
                )
                .into_dyn(),
            ))
        }
        _ => return Err(execution("unsupported value in VLA calibrator record")),
    })
}

fn publish(
    path: &Path,
    requested_path: &Path,
    overwrite: bool,
    list: &ComponentList,
    provenance: RecordValue,
) -> Result<(), TaskFailure> {
    let parent = path.parent().unwrap_or_else(|| Path::new("."));
    let staging = tempfile::tempdir_in(parent).map_err(execution)?;
    let component_path = staging.path().join("model.cl");
    list.write_table(&component_path).map_err(execution)?;
    let options = TableOptions::new(&component_path);
    let mut table = Table::open_metadata_only(options.clone()).map_err(execution)?;
    table
        .keywords_mut()
        .upsert("web_service", Value::Record(provenance));
    table.save_metadata_only(options).map_err(execution)?;
    drop(table);
    match rename_without_replacing(&component_path, path) {
        Ok(()) => Ok(()),
        Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {
            if overwrite {
                exchange_directories(&component_path, path)
                    .map_err(|error| execution(error).at("outfile"))
            } else {
                Err(exists(requested_path))
            }
        }
        Err(error) => Err(execution(error).at("outfile")),
    }
}

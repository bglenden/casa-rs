// SPDX-License-Identifier: LGPL-3.0-or-later
//! Queries and responses of the ALMA Jy/K service.

use std::path::Path;
use std::time::Duration;

use casa_ms::MeasurementSet;
use serde_json::Value;

use super::super::GencalRequest;
use super::{JyperkError, metadata};

pub(super) const DEFAULT_HOST: &str = "https://asa.alma.cl/science/jy-kelvins";

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Endpoint {
    #[default]
    Asdm,
    Interpolation,
    ModelFit,
}

impl Endpoint {
    pub(crate) fn parse(text: &str) -> Option<Self> {
        match text {
            "asdm" => Some(Self::Asdm),
            "interpolation" => Some(Self::Interpolation),
            "model-fit" => Some(Self::ModelFit),
            _ => None,
        }
    }

    fn path(self) -> &'static str {
        match self {
            Self::Asdm => "asdm",
            Self::Interpolation => "interpolation",
            Self::ModelFit => "model-fit",
        }
    }
}

pub(crate) struct ServiceOptions {
    pub(crate) endpoint: Endpoint,
    pub(crate) timeout: i64,
    pub(crate) attempts: i64,
    pub(crate) delay: i64,
}

impl Default for ServiceOptions {
    fn default() -> Self {
        Self {
            endpoint: Endpoint::Asdm,
            timeout: 180,
            attempts: 3,
            delay: 5,
        }
    }
}

pub(super) struct DatabaseFactor {
    pub(super) antenna: String,
    pub(super) window: String,
    pub(super) value: String,
}

pub(super) fn lookup(
    ms: &MeasurementSet,
    request: &GencalRequest,
    options: &ServiceOptions,
    host: &str,
) -> Result<Vec<DatabaseFactor>, JyperkError> {
    let windows = metadata::select_windows(ms, &request.spw).map_err(service_error)?;
    let hostname = if host.is_empty() { DEFAULT_HOST } else { host };
    let url = format!("{hostname}/{}/", options.endpoint.path());
    let timeout = u64::try_from(options.timeout).map_err(service_error)?;
    let agent =
        crate::calibration_service::agent(Duration::from_secs(timeout)).map_err(service_error)?;
    if options.endpoint == Endpoint::Asdm {
        let uid = asdm_uid(&request.measurement_set)?;
        let response = fetch(&agent, &url, &[("uid", uid)], options)?;
        return asdm_factors(response, &request.measurement_set, &windows);
    }
    let spec = metadata::QueryMetadata::load(ms, &windows, &request.spw).map_err(service_error)?;
    let mut factors = Vec::new();
    for antenna in &spec.antennas {
        for window in &spec.windows {
            let mut query = vec![
                ("date", spec.date.clone()),
                ("temperature", spec.temperature.clone()),
            ];
            if options.endpoint == Endpoint::Interpolation {
                query.push(("delta_days", "1000".into()));
            }
            query.extend([
                ("antenna", antenna.name.clone()),
                ("elevation", metadata::number(antenna.elevation)),
                ("band", window.band.to_string()),
                ("baseband", window.baseband.to_string()),
                ("frequency", metadata::number(window.frequency)),
            ]);
            let response = fetch(&agent, &url, &query, options)?;
            let value = match options.endpoint {
                Endpoint::Interpolation if response["query"]["elevation"] == "nan" => "1".into(),
                Endpoint::Interpolation => {
                    field(&response["data"]["factor"]["mean"], "factor.mean")?
                }
                Endpoint::ModelFit => field(&response["data"]["factor"], "factor")?,
                Endpoint::Asdm => unreachable!(),
            };
            factors.push(DatabaseFactor {
                antenna: field(&response["query"]["antenna"], "antenna")?,
                window: window.id.to_string(),
                value,
            });
        }
    }
    Ok(factors)
}

fn asdm_uid(path: &Path) -> Result<String, JyperkError> {
    let name = path.file_name().unwrap_or_default().to_string_lossy();
    let uid = name
        .strip_prefix("uid___")
        .and_then(|name| name.strip_suffix(".ms"));
    if let Some(uid) = uid {
        let parts = uid.split('_').collect::<Vec<_>>();
        if let [array, execution, block] = parts.as_slice()
            && array.len() == 4
            && array.starts_with('A')
            && array[1..].bytes().all(|b| b.is_ascii_digit())
            && [execution, block].into_iter().all(|part| {
                part.strip_prefix('X').is_some_and(|hex| {
                    !hex.is_empty()
                        && hex
                            .bytes()
                            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
                })
            })
        {
            return Ok(format!("uid://{array}/{execution}/{block}"));
        }
    }
    Err(service_error(format!(
        "MS name is not appropriate for DB query: {name}"
    )))
}

fn asdm_factors(
    response: Value,
    path: &Path,
    windows: &[usize],
) -> Result<Vec<DatabaseFactor>, JyperkError> {
    let name = path.file_name().unwrap_or_default().to_string_lossy();
    let Some(rows) = response["data"]["factors"].as_array() else {
        return Err(service_error("Jy/K response has no factors array"));
    };
    let mut factors = Vec::new();
    for row in rows {
        if row["MS"].as_str() != Some(name.as_ref()) {
            continue;
        }
        let window = field(&row["Spwid"], "Spwid")?;
        let id = window.parse::<usize>().map_err(service_error)?;
        if windows.contains(&id) {
            factors.push(DatabaseFactor {
                antenna: field(&row["Antenna"], "Antenna")?,
                window,
                value: field(&row["factor"], "factor")?,
            });
        }
    }
    Ok(factors)
}

fn field(value: &Value, name: &str) -> Result<String, JyperkError> {
    match value {
        Value::String(value) => Ok(value.clone()),
        Value::Number(value) => Ok(value.to_string()),
        _ => Err(service_error(format!("Jy/K response has no {name} value"))),
    }
}

fn fetch(
    agent: &ureq::Agent,
    url: &str,
    query: &[(&str, String)],
    options: &ServiceOptions,
) -> Result<Value, JyperkError> {
    for attempt in 0..options.attempts {
        let timeout = Some(Duration::from_secs(options.timeout as u64));
        match agent
            .get(url)
            .query_pairs(query.iter().map(|(k, v)| (*k, v)))
            .config()
            .timeout_global(None)
            .timeout_connect(timeout)
            .timeout_send_request(timeout)
            .timeout_recv_response(timeout)
            .timeout_recv_body(timeout)
            .build()
            .call()
            .and_then(|mut response| response.body_mut().read_to_vec())
        {
            Ok(body) => {
                let value: Value = serde_json::from_slice(&body).map_err(|error| {
                    service_error(format!(
                        "Failed to get Jy/K factors from DB: JSON Syntax error. {error}"
                    ))
                })?;
                if value["success"] != true {
                    let error = &value["error"];
                    let message = error
                        .as_str()
                        .map_or_else(|| error.to_string(), str::to_owned);
                    return Err(service_error(format!(
                        "Failed to get Jy/K factors from DB: {message}"
                    )));
                }
                return Ok(value);
            }
            Err(error) if attempt + 1 == options.attempts => {
                return Err(service_error(format!(
                    "Failed to load URL: {url}\nError Message: {error}"
                )));
            }
            Err(_) => {
                let delay = u64::try_from(options.delay)
                    .map_err(|_| service_error("sleep length must be non-negative"))?;
                std::thread::sleep(Duration::from_secs(delay));
            }
        }
    }
    Err(service_error("Jy/K service received no retry attempts"))
}

fn service_error(error: impl std::fmt::Display) -> JyperkError {
    JyperkError::Service(error.to_string())
}

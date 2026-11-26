// SPDX-License-Identifier: LGPL-3.0-or-later
//! Read station coordinates from the public ALMA antenna-position service.

use std::fs;
use std::io;
use std::path::Path;
use std::time::Duration;

use crate::{TaskFailure, TaskInvocation, TaskSuccess};
use casa_contracts::{CasaValue, ParameterValue};
use casa_types::quanta::{Quantity, Unit};
use serde_json::{Map, Value, json};
use tempfile::{Builder, NamedTempFile};
use time::OffsetDateTime;

pub(super) fn run(task: &TaskInvocation<'_>) -> Result<TaskSuccess, TaskFailure> {
    let outfile = required(task, "outfile")?;
    let requested_path = Path::new(&outfile);
    let destination = task.workspace().join(requested_path);
    let overwrite = task.flag("overwrite")?;
    if !overwrite && destination.exists() {
        return Err(exists(requested_path));
    }
    let hosts = task.texts("hosts")?;
    match hosts.first().map(String::as_str) {
        None => return Err(invalid("hosts", "Parameter hosts must be specified")),
        Some("") => {
            return Err(invalid(
                "hosts",
                "The first element of the hosts list must be specified",
            ));
        }
        Some(_) => {}
    }
    let asdm = required(task, "asdm")?;
    let tw = task.required_text("tw")?;
    let search = task.required_text("search")?;
    let first = task.flag("firstintegration")?;
    let mut metadata = Map::from_iter([
        ("caltype".into(), json!("ALMA antenna positions")),
        (
            "description".into(),
            json!("ALMA ITRF antenna positions in meters"),
        ),
        ("product_code".into(), json!("antposalma")),
        ("outfile".into(), json!(outfile)),
        ("hosts".into(), json!(hosts)),
    ]);
    let mut query = vec![("asdm", Value::from(asdm))];
    if !tw.is_empty() {
        validate_window(&tw)?;
        query.push(("tw", Value::from(tw)));
    }
    if let Some(snr) = signal_to_noise(task.value("snr"))? {
        query.push(("snr", snr));
    }
    if !search.is_empty() {
        query.push(("search", Value::from(search)));
    }
    query.push(("firstIntegration", Value::from(first)));
    let attempts = Retry::read(task)?;
    metadata.insert("nretry".into(), json!(attempts.count));
    metadata.insert("rdelay".into(), json!(attempts.delay_seconds));
    let (url, data) = retrieve(&hosts, &query_string(&query), attempts)?;
    metadata.extend(
        query
            .into_iter()
            .map(|(name, value)| (name.to_string(), value)),
    );
    metadata.insert("successful_url".into(), Value::from(url));
    metadata.insert("timestamp".into(), Value::from(timestamp()));
    publish(
        &destination,
        requested_path,
        overwrite,
        &json!({"data": data, "metadata": metadata}),
    )?;
    TaskSuccess::new(CasaValue::None, &())
}

fn invalid(parameter: &'static str, message: impl Into<String>) -> TaskFailure {
    TaskFailure::validation(message).at(parameter)
}

fn required(task: &TaskInvocation<'_>, name: &'static str) -> Result<String, TaskFailure> {
    task.text(name)?
        .filter(|text| !text.is_empty())
        .ok_or_else(|| invalid(name, format!("Parameter {name} must be specified")))
}

fn exists(path: &Path) -> TaskFailure {
    TaskFailure::execution(format!(
        "A file or directory named {} already exists and overwrite is False, so exiting. \
         Either rename the existing file or directory, change the value of overwrite to True, or both.",
        path.display()
    ))
}

fn validate_window(tw: &str) -> Result<(), TaskFailure> {
    let Some((begin, end)) = tw.split_once(',').filter(|(_, end)| !end.contains(',')) else {
        return Err(invalid(
            "tw",
            "Parameter tw should contain exactly one comma that separates two times",
        ));
    };
    let days = Unit::new("d").expect("known time unit");
    let angles = Unit::new("deg").expect("known angle unit");
    let parse = |label, text: &str| {
        Quantity::read(text).ok().and_then(|time| {
            time.get_value_in(&days).or_else(|_| {
                time.get_value_in(&angles).map(|degrees| degrees / 360.0)
            }).ok()
        })
            // The pinned CASA task compares timestamps formatted as FITS
            // with whole seconds, including when the input is an angle.
            .map(|days| (days * 86_400.0).round())
            .ok_or_else(|| invalid("tw", format!("{label} time {text} does not appear to have a valid format. The correct format is of the form YYYY-MM-DDThh:mm:ss.")))
    };
    if parse("Begin", begin)? >= parse("End", end)? {
        return Err(invalid(
            "tw",
            format!("Parameter tw, start time ({begin}) must be less than end time ({end})."),
        ));
    }
    Ok(())
}

fn signal_to_noise(value: Option<&ParameterValue>) -> Result<Option<Value>, TaskFailure> {
    match value {
        Some(ParameterValue::String(text)) if text == "default" => Ok(None),
        Some(ParameterValue::String(_)) => Err(invalid(
            "snr",
            "If snr is a string, its only permissible value is 'default'",
        )),
        Some(ParameterValue::Integer(number)) if *number >= 0 => Ok(Some(json!(number))),
        Some(ParameterValue::Float(number)) if *number >= 0.0 => Ok(Some(json!(number))),
        Some(ParameterValue::Bool(value)) => Ok(Some(json!(value))),
        Some(ParameterValue::Integer(number)) => Err(invalid(
            "snr",
            format!("If a number, parameter snr ({number}) must be non-negative."),
        )),
        Some(ParameterValue::Float(number)) => Err(invalid(
            "snr",
            format!(
                "If a number, parameter snr ({}) must be non-negative.",
                python_float(*number)
            ),
        )),
        _ => Err(TaskFailure::execution("snr must be a string or number").at("snr.type")),
    }
}

#[derive(Clone, Copy)]
struct Retry {
    count: i64,
    delay_seconds: f64,
}

impl Retry {
    fn read(task: &TaskInvocation<'_>) -> Result<Self, TaskFailure> {
        let count = task.integer("nretry")?;
        let mut delay_seconds = task.number("rdelay")?;
        if count < 0 {
            return Err(invalid(
                "nretry",
                format!("Parameter nretry (={count}) must be non-negative."),
            ));
        }
        if count > 0 {
            if delay_seconds <= 0.0 || Duration::try_from_secs_f64(delay_seconds).is_err() {
                return Err(invalid(
                    "rdelay",
                    format!(
                        "If nretry > 0, rdelay (={}) must be positive.",
                        python_float(delay_seconds)
                    ),
                ));
            }
            if delay_seconds < 0.5 {
                tracing::warn!(
                    original = delay_seconds,
                    "Setting the retry delay to 0.5 seconds"
                );
                delay_seconds = 0.5;
            }
        }
        Ok(Self {
            count,
            delay_seconds,
        })
    }
}

fn query_string(parameters: &[(&str, Value)]) -> String {
    parameters
        .iter()
        .map(|(name, value)| {
            let text = match value {
                Value::String(text) => text.clone(),
                Value::Bool(true) => "True".to_string(),
                Value::Bool(false) => "False".to_string(),
                Value::Number(value) if value.is_f64() => {
                    python_float(value.as_f64().expect("known JSON float"))
                }
                value => value.to_string(),
            };
            let mut encoded = format!("{name}=");
            for byte in text.bytes() {
                match byte {
                    b' ' => encoded.push('+'),
                    b'-' | b'_' | b'.' | b'~' => encoded.push(char::from(byte)),
                    byte if byte.is_ascii_alphanumeric() => encoded.push(char::from(byte)),
                    byte => {
                        use std::fmt::Write as _;
                        write!(&mut encoded, "%{byte:02X}").expect("writing to a string succeeds");
                    }
                }
            }
            encoded
        })
        .collect::<Vec<_>>()
        .join("&")
}

fn python_float(value: f64) -> String {
    let magnitude = value.abs();
    if magnitude != 0.0 && !(1.0e-4..1.0e16).contains(&magnitude) {
        let scientific = format!("{value:e}");
        let (mantissa, exponent) = scientific.split_once('e').expect("exponential format");
        let exponent = exponent.parse::<i32>().expect("integer exponent");
        return format!("{mantissa}e{exponent:+03}");
    }
    let mut text = value.to_string();
    if !text.contains('.') {
        text.push_str(".0");
    }
    text
}

fn retrieve(
    hosts: &[String],
    query: &str,
    attempts: Retry,
) -> Result<(String, Value), TaskFailure> {
    let agent = casa_calibration::calibration_service::agent(Duration::from_secs(400))
        .map_err(|error| TaskFailure::execution(error.to_string()).at("hosts"))?;
    for attempt in 0..=attempts.count {
        if attempt > 0 {
            std::thread::sleep(Duration::from_secs_f64(attempts.delay_seconds));
        }
        for host in hosts {
            if host
                .parse::<ureq::http::Uri>()
                .ok()
                .is_none_or(|uri| uri.scheme().is_none() || uri.authority().is_none())
            {
                return Err(invalid(
                    "hosts",
                    format!("Parameter hosts: {host} is not a valid host expressed as a URL."),
                ));
            }
            let url = format!("{host}/?{query}");
            let text = match query_host(&agent, &url) {
                Ok(Some(text)) => text,
                Ok(None) => continue,
                Err(error) => {
                    tracing::warn!(%error, "ALMA antenna position request failed");
                    continue;
                }
            };
            let data: Value = serde_json::from_str(&text)
                .map_err(|error| TaskFailure::execution(error.to_string()).at("hosts.json"))?;
            if response_has_data(&data) {
                return Ok((url, data));
            }
            break;
        }
    }
    Err(TaskFailure::execution(
        "All URLs failed to return an antenna position list.",
    ))
}

fn response_has_data(data: &Value) -> bool {
    match data {
        Value::Null | Value::Bool(false) => false,
        Value::Bool(true) => true,
        Value::Number(number) => number.as_f64().is_some_and(|number| number != 0.0),
        Value::String(text) => !text.is_empty(),
        Value::Array(items) => !items.is_empty(),
        Value::Object(items) => !items.is_empty(),
    }
}

fn query_host(agent: &ureq::Agent, url: &str) -> Result<Option<String>, ureq::Error> {
    let mut response = agent.get(url).call()?;
    if response.status() != 200 {
        return Ok(None);
    }
    let text = response.body_mut().read_to_string()?;
    Ok((!text.is_empty()).then_some(text))
}

fn timestamp() -> String {
    let now = OffsetDateTime::now_local().unwrap_or_else(|_| OffsetDateTime::now_utc());
    format!(
        "{} {:02}:{:02}:{:02}.{:06}",
        now.date(),
        now.hour(),
        now.minute(),
        now.second(),
        now.microsecond(),
    )
}

fn publish(
    path: &Path,
    requested_path: &Path,
    overwrite: bool,
    document: &Value,
) -> Result<(), TaskFailure> {
    let parent = path.parent().unwrap_or_else(|| Path::new("."));
    let failed = |error: io::Error| {
        TaskFailure::execution(format!("{}: {error}", path.display())).at("outfile")
    };
    let mut stage = NamedTempFile::new_in(parent).map_err(failed)?;
    serde_json::to_writer(stage.as_file_mut(), document)
        .map_err(|error| TaskFailure::execution(error.to_string()).at("outfile"))?;
    stage.as_file().sync_all().map_err(failed)?;
    if !overwrite {
        stage.persist_noclobber(path).map_err(|error| {
            if error.error.kind() == io::ErrorKind::AlreadyExists {
                exists(requested_path)
            } else {
                failed(error.error)
            }
        })?;
    } else if path.is_dir() {
        if fs::symlink_metadata(path).map_err(failed)?.is_symlink() {
            return Err(
                TaskFailure::execution("cannot replace a symlink to a directory").at("outfile"),
            );
        }
        let backup = Builder::new()
            .prefix(".getantposalma-replaced-")
            .tempdir_in(parent)
            .map_err(failed)?;
        let recovered = backup.path().join("previous");
        fs::rename(path, &recovered).map_err(failed)?;
        if let Err(error) = stage.persist_noclobber(path) {
            if fs::rename(&recovered, path).is_err() {
                let saved = backup.keep();
                return Err(TaskFailure::execution(format!(
                    "{}: {}; prior product left at {}",
                    path.display(),
                    error.error,
                    saved.display()
                ))
                .at("outfile"));
            }
            return Err(failed(error.error));
        }
        backup.close().map_err(failed)?;
    } else {
        stage.persist(path).map_err(|error| failed(error.error))?;
    }
    Ok(())
}

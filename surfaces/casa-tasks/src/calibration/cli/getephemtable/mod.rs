// SPDX-License-Identifier: LGPL-3.0-or-later
//! Retrieve and publish an observer ephemeris from JPL Horizons.

mod parse;

use std::io::Write;
use std::path::Path;
use std::time::Duration;

use crate::{TaskFailure, TaskInvocation, TaskSuccess};
use casa_contracts::CasaValue;
use casa_tables::{TableOptions, publication::lock_table_tree_for_replacement};
use casa_types::quanta::Quantity;
use casa_types::quanta::format::{YMD, format_time};

use casa_calibration::output::CalibrationOutput;

const HORIZONS_URL: &str = "https://ssd.jpl.nasa.gov/api/horizons.api";

pub(super) fn run(task: &TaskInvocation<'_>) -> Result<TaskSuccess, TaskFailure> {
    let objectname = task.required_text("objectname")?;
    let objectname = objectname.trim();
    let asis = task.flag("asis")?;
    if objectname.is_empty() {
        return Err(invalid("objectname", "objectname must be specified"));
    }
    if !asis && objectname.starts_with(|ch: char| ch.is_ascii_digit()) {
        return Err(TaskFailure::execution(
            "objectname is given as an ID number, need to set asis=True",
        ));
    }
    let (start, stop) = time_range(&task.required_text("timerange")?)?;
    let interval = step_size(&task.required_text("interval")?)?;
    let outfile = task.required_text("outfile")?;
    if outfile.trim().is_empty() {
        return Err(invalid("outfile", "outfile must be specified"));
    }
    let outfile = task.workspace().join(outfile);
    let rawfile = task.text("rawdatafile")?.filter(|path| !path.is_empty());
    let overwrite = task.flag("overwrite")?;
    let raw = rawfile.as_ref().map(|path| task.workspace().join(path));
    if !overwrite && outfile.exists() {
        return Err(invalid(
            "outfile",
            format!("{} exists and overwrite=False", outfile.display()),
        ));
    }
    if let Some(raw) = &raw {
        if casa_calibration::paths::overlap(&outfile, raw).map_err(execution)? {
            return Err(invalid("rawdatafile", "rawdatafile and outfile overlap"));
        }
        if !overwrite && raw.exists() {
            return Err(invalid(
                "rawdatafile",
                format!("{} exists and overwrite=False", raw.display()),
            ));
        }
    }
    let target = if asis {
        objectname
    } else {
        known_object(objectname)?
    };
    let _output_locks = if overwrite && outfile.join("table.dat").is_file() {
        lock_table_tree_for_replacement(&outfile).map_err(execution)?
    } else {
        Vec::new()
    };
    let destination =
        CalibrationOutput::prepare_ephemeris_table(&outfile, overwrite).map_err(execution)?;
    if let Some(response) = query(target, &start, &stop, &interval) {
        if let Some(raw) = raw {
            publish_raw(&raw, &response, overwrite)?;
        }
        match parse::ephemeris_table(&response) {
            Ok(table) => {
                table
                    .save(TableOptions::new(&destination.staging))
                    .map_err(execution)?;
                destination.commit().map_err(execution)?;
            }
            Err(reason) => tracing::warn!(%reason, "failed to produce the CASA ephemeris table"),
        }
    }
    TaskSuccess::new(CasaValue::None, &())
}

fn known_object(name: &str) -> Result<&'static str, TaskFailure> {
    Ok(match name.to_ascii_lowercase().as_str() {
        "sun" => "10",
        "mercury" => "199",
        "venus" => "299",
        "moon" => "301",
        "mars" => "499",
        "jupiter" => "599",
        "io" => "501",
        "europa" => "502",
        "ganymede" => "503",
        "callisto" => "504",
        "saturn" => "699",
        "titan" => "606",
        "uranus" => "799",
        "neptune" => "899",
        "pluto" => "999",
        "ceres" => "1;",
        "pallas" => "2;",
        "juno" => "3;",
        "vesta" => "4;",
        "lutetia" => "21;",
        _ => {
            return Err(invalid(
                "objectname",
                format!(
                    "{name} is not in the known object list for CASA. To skip this check set asis=True"
                ),
            ));
        }
    })
}

fn time_range(text: &str) -> Result<(String, String), TaskFailure> {
    let invalid_time = |reason: &str| invalid("timerange", reason);
    if text.trim().is_empty() {
        return Err(invalid_time("timerange must be specified"));
    }
    let normalized = text.replace(' ', "").to_ascii_uppercase();
    let (start, stop) = normalized
        .split_once('~')
        .filter(|(_, stop)| !stop.contains('~'))
        .ok_or_else(|| {
            invalid_time(
                "timerange needs to be specified with starttime and stoptime delimited by ~ .",
            )
        })?;
    if let Some(start_mjd) = start.strip_prefix("MJD") {
        let start = mjd_as_jd(start_mjd).ok_or_else(|| {
            invalid_time("Error translating start time of timerange specified in MJD.")
        })?;
        let stop = if stop.starts_with("JD") {
            stop.to_owned()
        } else {
            mjd_as_jd(stop.trim_start_matches("MJD")).ok_or_else(|| {
                invalid_time("Error translating stop time of timerange specified in MJD.")
            })?
        };
        return Ok((start, stop));
    }
    if start.starts_with("JD") {
        let stop = if stop.starts_with("JD") {
            stop.to_owned()
        } else if stop.parse::<f64>().is_ok() {
            format!("JD{stop}")
        } else {
            return Err(invalid_time(
                "Error translating stop time of timerange specified in JD.",
            ));
        };
        return Ok((start.to_owned(), stop));
    }
    let calendar = |text: &str| {
        let bytes = text.as_bytes();
        (bytes.get(4) == Some(&b'/') && bytes.get(7) == Some(&b'/'))
            .then(|| Quantity::read(text).ok())
            .flatten()
            .map(|date| format_time(date.value(), YMD, 6))
            .ok_or_else(|| invalid_time("Error in timerange format. Use YYYY/MM/DD/hh:mm or Julian date with a prefix 'JD' Modified Julian date with a prefix 'MJD'"))
    };
    Ok((calendar(start)?, calendar(stop)?))
}

fn mjd_as_jd(value: &str) -> Option<String> {
    let jd = value.parse::<f64>().ok()? + 2_400_000.5;
    jd.is_finite().then(|| format!("JD{jd:?}"))
}

fn step_size(text: &str) -> Result<String, TaskFailure> {
    let end = text
        .find(|ch: char| !ch.is_ascii_digit())
        .unwrap_or(text.len());
    if end == 0 {
        return Err(invalid(
            "interval",
            "interval must be a string with integer with unit ",
        ));
    }
    let (number, unit) = text.split_at(end);
    if unit.starts_with('.') {
        return Err(invalid("interval", "interval value must be integer"));
    }
    let unit = unit.trim_start();
    let end = unit
        .find(|ch: char| !ch.is_ascii_alphabetic())
        .unwrap_or(unit.len());
    Ok(format!("{number}{}", &unit[..end]))
}

fn query(target: &str, start: &str, stop: &str, step: &str) -> Option<String> {
    let url = std::env::var("CASARS_JPL_HORIZONS_URL").unwrap_or_else(|_| HORIZONS_URL.to_owned());
    let command = format!("'{target}'");
    let form = [
        ("format", "json"),
        ("EPHEM_TYPE", "OBSERVER"),
        ("OBJ_DATA", "YES"),
        ("COMMAND", &command),
        ("START_TIME", start),
        ("STOP_TIME", stop),
        ("STEP_SIZE", step),
        ("CENTER", "500@399"),
        ("QUANTITIES", "'1,14,15,17,19,20,24'"),
        ("ANG_FORMAT", "DEG"),
    ];
    let agent = match horizons_agent() {
        Ok(agent) => agent,
        Err(error) => {
            tracing::warn!(%error, "unable to configure JPL Horizons TLS");
            return None;
        }
    };
    let reply = agent.post(&url).send_form(form).and_then(|mut response| {
        response
            .body_mut()
            .with_config()
            .limit(64 * 1024 * 1024)
            .read_to_string()
    });
    match reply {
        Ok(reply) => match serde_json::from_str::<serde_json::Value>(&reply) {
            Ok(mut value) => match value.get_mut("result").map(serde_json::Value::take) {
                Some(serde_json::Value::String(text)) => Some(text),
                _ => {
                    tracing::warn!(%url, "JPL Horizons returned no ephemeris result");
                    None
                }
            },
            Err(error) => {
                tracing::warn!(%url, %error, "invalid JPL Horizons response");
                None
            }
        },
        Err(error) => {
            tracing::warn!(%url, %error, "JPL Horizons query failed");
            None
        }
    }
}

fn horizons_agent() -> Result<ureq::Agent, String> {
    use ureq::tls::{PemItem, RootCerts, TlsConfig, parse_pem};

    let mut builder = ureq::Agent::config_builder().timeout_global(Some(Duration::from_secs(10)));
    if let Some(path) = std::env::var_os("SSL_CERT_FILE") {
        let bytes = std::fs::read(path).map_err(|error| format!("read SSL_CERT_FILE: {error}"))?;
        let certificates = parse_pem(&bytes)
            .filter_map(|item| match item {
                Ok(PemItem::Certificate(certificate)) => Some(Ok(certificate)),
                Ok(_) => None,
                Err(error) => Some(Err(error)),
            })
            .collect::<Result<Vec<_>, _>>()
            .map_err(|error| format!("parse SSL_CERT_FILE: {error}"))?;
        if certificates.is_empty() {
            return Err("SSL_CERT_FILE contains no root certificates".to_owned());
        }
        let tls = TlsConfig::builder()
            .root_certs(RootCerts::from(certificates))
            .build();
        builder = builder.tls_config(tls);
    }
    Ok(builder.build().into())
}

fn publish_raw(path: &Path, text: &str, overwrite: bool) -> Result<(), TaskFailure> {
    let parent = path.parent().unwrap_or_else(|| Path::new("."));
    let mut staged = tempfile::NamedTempFile::new_in(parent).map_err(execution)?;
    staged.write_all(text.as_bytes()).map_err(execution)?;
    if overwrite {
        staged.persist(path)
    } else {
        staged.persist_noclobber(path)
    }
    .map_err(execution)?;
    Ok(())
}

fn invalid(parameter: &str, reason: impl Into<String>) -> TaskFailure {
    TaskFailure::validation(reason).at(parameter)
}

fn execution(error: impl std::fmt::Display) -> TaskFailure {
    TaskFailure::execution(error.to_string())
}

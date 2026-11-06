// SPDX-License-Identifier: LGPL-3.0-or-later
//! `imexplore` - the `imhead` and `imstat` task provider for persistent
//! casacore images, on the shared task front end. Its first word names the
//! surface (`imexplore imhead ...`, `imexplore imstat ...`); `imhead` runs
//! otherwise.

use std::ffi::OsString;
use std::fmt::{Display, Write as _};
use std::path::Path;
use std::process::ExitCode;

use casa_images::{
    ImageHeaderSummary, ImageStatisticsSummary, image_task_front_end, imhead, imhead_add_delete,
    imhead_extrema, imhead_list_record, imhead_put, imhead_summary_record, imstat, imstat_record,
};
use casa_provider_contracts::ParameterValue;
use casa_task_runtime::{CasaValue, TaskFailure, TaskInvocation, TaskSuccess};

fn main() -> ExitCode {
    let args: Vec<OsString> = std::env::args_os().skip(1).collect();
    let default_logging = !casa_task_runtime::CasaLoggingConfig::output_was_configured(&args);
    image_task_front_end("imexplore", &["imhead", "imstat"], move |task| {
        run(task, default_logging)
    })
    .main(
        args,
        casa_task_runtime::CasaLoggingConfig::from_env(),
        std::env::set_current_dir,
        &mut std::io::stdout(),
        std::io::stderr(),
    )
}

fn run(task: &TaskInvocation<'_>, default_logging: bool) -> Result<TaskSuccess, TaskFailure> {
    match task.surface() {
        "imstat" => run_imstat(task),
        _ => run_imhead(task, default_logging),
    }
}

/// CASA's `imhead` returns its header record for `summary` and `list`, and
/// `None` for `put`; `imstat` returns its statistics record.
fn run_imhead(
    task: &TaskInvocation<'_>,
    default_logging: bool,
) -> Result<TaskSuccess, TaskFailure> {
    let path = task.input_path("imagename")?;
    let mode = task.required_text("mode")?.to_ascii_lowercase();
    if mode == "history" {
        // Open the image first, just as CASA does, even if there is no logtable.
        casa_images::analysis::validate_imhead_image(&path)
            .map_err(|error| image_error("imhead", &path, error))?;
        let entries = casa_images::image::image_history_entries(&path)
            .map_err(|error| image_error("imhead", &path, error))?;
        let mut result = TaskSuccess::new(CasaValue::None, &())?;
        for entry in entries {
            result = result.with_replayed_info(
                entry.time_mjd_seconds,
                &entry.replay_location(),
                &entry.message,
                default_logging,
            );
        }
        return Ok(result);
    }
    let mut summary = match mode.as_str() {
        "summary" | "list" | "get" => {
            imhead(&path).map_err(|error| image_error("imhead", &path, error))?
        }
        "add" | "del" | "put" => {
            let hdkey = task.text("hdkey")?.ok_or_else(|| {
                TaskFailure::validation("imhead mode put requires hdkey").at("hdkey")
            })?;
            let key = hdkey.to_ascii_lowercase();
            let reference_pixel = mode == "put" && key.starts_with("crpix");
            let axis_name = mode == "put" && key.starts_with("ctype");
            let rest_frequency = mode == "put" && key == "restfreq";
            if !["bunit", "object", "observer", "telescope"].contains(&key.as_str())
                && !(mode == "put" && key == "imtype")
                && !reference_pixel
                && !axis_name
                && !rest_frequency
                && !casa_images::analysis::imhead_is_custom_key(&hdkey)
            {
                return Err(TaskFailure::validation(format!(
                    "imhead {mode} of hdkey {hdkey:?} is not implemented by casa-rs yet (#977)"
                ))
                .at("hdkey"));
            }
            // CASA's default '' is a value here: it clears the key, as
            // `hdkey='bunit', hdvalue=''` clears the brightness unit.
            let hdvalue = task
                .value("hdvalue")
                .cloned()
                .unwrap_or_else(|| ParameterValue::String(String::new()));
            if matches!(hdvalue, ParameterValue::Array(_) | ParameterValue::Table(_))
                && mode == "del"
            {
                return Err(TaskFailure::validation(
                    "imhead del with an array or record hdvalue is not implemented (#977)",
                )
                .at("hdvalue"));
            }
            let summary = if reference_pixel {
                casa_images::analysis::imhead_put_reference_pixel(&path, &hdkey, &hdvalue)
            } else if axis_name {
                casa_images::analysis::imhead_put_axis_name(&path, &hdkey, &hdvalue)
            } else if rest_frequency {
                casa_images::analysis::imhead_put_rest_frequency(&path, &hdvalue)
            } else if casa_images::analysis::imhead_is_custom_key(&hdkey) {
                casa_images::analysis::imhead_custom_scalar(&path, &mode, &hdkey, &hdvalue)
            } else if mode == "del" {
                imhead_add_delete(&path, &mode, &hdkey, "")
            } else if let ParameterValue::String(value) = &hdvalue {
                if mode == "put" {
                    imhead_put(&path, &hdkey, value)
                } else {
                    imhead_add_delete(&path, &mode, &hdkey, value)
                }
            } else {
                return Err(TaskFailure::execution(format!(
                    "{hdkey} value must be a string"
                )));
            }
            .map_err(|error| image_error("imhead", &path, error))?;
            let verbose = if task.flag("verbose")? {
                "True"
            } else {
                "False"
            };
            let given_path = task
                .session()
                .given("imagename")
                .map(|value| value.to_plain_json());
            let imagename = given_path
                .as_ref()
                .and_then(|value| value.as_str())
                .unwrap_or_else(|| path.to_str().unwrap_or(""));
            let history = [
                format!("version: casa-rs {}", env!("CARGO_PKG_VERSION")),
                format!(
                    "imhead(imagename={}, mode={}, hdkey={}, hdvalue={}, verbose={verbose})",
                    history_string(imagename),
                    history_string(&mode),
                    history_string(&hdkey),
                    history_value(&hdvalue)
                ),
            ];
            if let Err(error) = casa_images::image::add_image_history(&path, "imhead", &history) {
                tracing::warn!("imhead could not write task history: {error}");
            }
            summary
        }
        other => {
            return Err(TaskFailure::validation(format!(
                "unsupported imhead mode {other:?}; expected summary, list, get, add, del, put, or history"
            ))
            .at("mode"));
        }
    };
    let hdkey = task.text("hdkey")?.unwrap_or_default();
    if mode == "list"
        || (mode == "get"
            && [
                "datamin",
                "datamax",
                "minpixpos",
                "maxpixpos",
                "minpos",
                "maxpos",
            ]
            .contains(&hdkey.to_ascii_lowercase().as_str()))
    {
        summary.extrema = imhead_extrema(&path)
            .map_err(|error| image_error("imhead", &path, error))?
            .map(Box::new);
    }
    let value = match mode.as_str() {
        "summary" => imhead_summary_record(&summary),
        "list" => imhead_list_record(&summary),
        "get" => {
            casa_images::imhead_get_value(&summary, &path, &hdkey).map_err(|error| match error {
                casa_images::ImheadGetError::UnsupportedKey(_) => {
                    TaskFailure::validation(error.to_string()).at("hdkey")
                }
                casa_images::ImheadGetError::Execution(message) => TaskFailure::execution(message),
                casa_images::ImheadGetError::Image(error) => image_error("imhead", &path, error),
            })?
        }
        // CASA's put returns None.
        _ => CasaValue::None,
    };
    let text = header_text(&summary);
    Ok(TaskSuccess::new(value, &summary)?.with_text(text))
}

fn history_string(value: &str) -> String {
    if value.chars().count() > 300 {
        "...".to_string()
    } else {
        format!("\"{value}\"")
    }
}

fn history_value(value: &ParameterValue) -> String {
    match value {
        ParameterValue::String(value) => history_string(value),
        ParameterValue::Bool(value) => if *value { "True" } else { "False" }.to_string(),
        ParameterValue::Integer(value) => value.to_string(),
        ParameterValue::Float(value) => casa_provider_contracts::python_float(*value),
        ParameterValue::Table(_) => casa_provider_contracts::python_repr(value),
        _ => "...".to_string(),
    }
}

fn run_imstat(task: &TaskInvocation<'_>) -> Result<TaskSuccess, TaskFailure> {
    let path = task.input_path("imagename")?;
    let summary = imstat(
        &path,
        task.text("box")?.as_deref(),
        task.text("region")?.as_deref(),
        task.text("chans")?.as_deref(),
        None,
    )
    .map_err(|error| image_error("imstat", &path, error))?;
    let text = statistics_text(&summary);
    Ok(TaskSuccess::new(imstat_record(&summary), &summary)?.with_text(text))
}

fn image_error(operation: &str, path: &Path, error: impl Display) -> TaskFailure {
    TaskFailure::execution(format!(
        "{operation}: cannot open or read CASA image '{}': {error}",
        path.display()
    ))
}

fn header_text(summary: &ImageHeaderSummary) -> String {
    let mut out = String::new();
    let _ = writeln!(out, "Image name       : {}", summary.imagename);
    let _ = writeln!(out, "Image type       : {}", summary.image_type);
    let _ = writeln!(out, "Pixel type       : {}", summary.pixel_type);
    let _ = writeln!(out, "Image units      : {}", summary.units);
    let _ = writeln!(out, "Shape            : {:?}", summary.shape);
    if let Some(beam) = &summary.restoring_beam {
        let _ = writeln!(
            out,
            "Restoring Beam   : {:.6} arcsec, {:.6} arcsec, {:.6} deg",
            beam.major_arcsec, beam.minor_arcsec, beam.position_angle_deg
        );
    }
    for axis in &summary.axes {
        let _ = writeln!(
            out,
            "Axis {} {:<12} {:<18} shape={} refpix={:.3} crval={:.12e} cdelt={:.12e} {}",
            axis.axis,
            axis.coordinate_type,
            axis.name,
            axis.shape,
            axis.reference_pixel,
            axis.reference_value,
            axis.increment,
            axis.unit
        );
    }
    out
}

fn statistics_text(summary: &ImageStatisticsSummary) -> String {
    let mut out = String::new();
    let _ = writeln!(out, "npts: {}", summary.npts);
    let _ = writeln!(out, "min: {}", summary.min);
    let _ = writeln!(out, "max: {}", summary.max);
    let _ = writeln!(out, "sum: {}", summary.sum);
    let _ = writeln!(out, "mean: {}", summary.mean);
    let _ = writeln!(out, "rms: {}", summary.rms);
    let _ = writeln!(out, "sigma: {}", summary.sigma);
    if let Some(flux) = summary.flux {
        let _ = writeln!(out, "flux: {flux}");
    }
    out
}

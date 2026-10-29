// SPDX-License-Identifier: LGPL-3.0-or-later
//! `imexplore` - the `imhead` and `imstat` task provider for persistent
//! casacore images, on the shared task front end. Its first word names the
//! surface (`imexplore imhead ...`, `imexplore imstat ...`); `imhead` runs
//! otherwise.

use std::fmt::{Display, Write as _};
use std::path::Path;
use std::process::ExitCode;

use casa_images::{
    ImageHeaderSummary, ImageStatisticsSummary, image_task_front_end, imhead, imhead_list_record,
    imhead_put, imhead_summary_record, imstat, imstat_record,
};
use casa_task_runtime::{CasaValue, TaskFailure, TaskInvocation, TaskSuccess};

fn main() -> ExitCode {
    image_task_front_end("imexplore", &["imhead", "imstat"], run).main(
        std::env::args_os().skip(1),
        casa_task_runtime::CasaLoggingConfig::from_env(),
        std::env::set_current_dir,
        &mut std::io::stdout(),
        std::io::stderr(),
    )
}

fn run(task: &TaskInvocation<'_>) -> Result<TaskSuccess, TaskFailure> {
    match task.surface() {
        "imstat" => run_imstat(task),
        _ => run_imhead(task),
    }
}

/// CASA's `imhead` returns its header record for `summary` and `list`, and
/// `None` for `put`; `imstat` returns its statistics record.
fn run_imhead(task: &TaskInvocation<'_>) -> Result<TaskSuccess, TaskFailure> {
    let path = task.input_path("imagename")?;
    let mode = task.required_text("mode")?.to_ascii_lowercase();
    let summary = match mode.as_str() {
        "summary" | "list" => imhead(&path).map_err(|error| image_error("imhead", &path, error))?,
        "put" => {
            let hdkey = task.text("hdkey")?.ok_or_else(|| {
                TaskFailure::validation("imhead mode put requires hdkey").at("hdkey")
            })?;
            if !hdkey.eq_ignore_ascii_case("bunit") {
                return Err(TaskFailure::validation(format!(
                    "imhead put of hdkey {hdkey:?} is not implemented by casa-rs yet (#977); \
                     it puts bunit"
                ))
                .at("hdkey"));
            }
            // CASA's default '' is a value here: it clears the key, as
            // `hdkey='bunit', hdvalue=''` clears the brightness unit.
            let hdvalue = task.text("hdvalue")?.unwrap_or_default();
            imhead_put(&path, &hdkey, &hdvalue)
                .map_err(|error| image_error("imhead", &path, error))?
        }
        other => {
            return Err(TaskFailure::validation(format!(
                "unsupported imhead mode {other:?}; expected summary, list, or put"
            ))
            .at("mode"));
        }
    };
    let value = match mode.as_str() {
        "summary" => imhead_summary_record(&summary),
        "list" => imhead_list_record(&summary),
        // CASA's put returns None.
        _ => CasaValue::None,
    };
    let text = header_text(&summary);
    Ok(TaskSuccess::new(value, &summary)?.with_text(text))
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

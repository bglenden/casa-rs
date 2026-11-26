// SPDX-License-Identifier: LGPL-3.0-or-later
//! `immath` - CASA-style image arithmetic, spectral index, and polarization, on the shared task front end.

use std::path::PathBuf;

use casa_images::{ImmathRequest, immath};
use casa_task_runtime::{CasaValue, TaskFailure, TaskFrontEnd, TaskInvocation, TaskSuccess};

use super::front_end::{image_task_front_end, image_task_returning};
use super::task_contract::ImageAnalysisTaskResult;

/// The front end of the `immath` binary.
pub fn immath_front_end()
-> TaskFrontEnd<impl Fn(&TaskInvocation<'_>) -> Result<TaskSuccess, TaskFailure>> {
    image_task_front_end("immath", &["immath"], run)
}

fn run(task: &TaskInvocation<'_>) -> Result<TaskSuccess, TaskFailure> {
    let imagename: Vec<PathBuf> = task
        .paths("imagename")?
        .into_iter()
        .filter(|path| !path.as_os_str().is_empty())
        .collect();
    let mode = task.required_text("mode")?;
    let outfile = task
        .text("outfile")?
        .map(|text| text.trim().to_string())
        .filter(|text| !text.is_empty())
        .map_or_else(|| PathBuf::from("immath_results.im"), PathBuf::from);
    let expr = task.text("expr")?.unwrap_or_else(|| "IM0".to_string());
    let varnames: Vec<String> = task
        .texts("varnames")?
        .into_iter()
        .filter(|name| !name.is_empty())
        .collect();
    let sigma = task
        .text("sigma")?
        .unwrap_or_else(|| "0.0mJy/beam".to_string());
    let polithresh = task
        .text("polithresh")?
        .filter(|text| !text.trim().is_empty());
    let mask = task.text("mask")?.filter(|text| !text.trim().is_empty());
    let region = task.text("region")?.filter(|text| !text.trim().is_empty());
    let box_pixels = task.text("box")?.filter(|text| !text.trim().is_empty());
    let chans = task.text("chans")?.filter(|text| !text.trim().is_empty());
    let stokes = task.text("stokes")?.filter(|text| !text.trim().is_empty());
    let stretch = task.flag("stretch")?;
    let imagemd = task.text("imagemd")?.filter(|text| !text.trim().is_empty());
    let prec = task.text("prec")?.unwrap_or_else(|| "float".to_string());
    let overwrite = task.flag("overwrite")?;

    let request = ImmathRequest {
        imagename,
        mode,
        outfile,
        expr,
        varnames,
        sigma,
        polithresh,
        mask,
        region,
        box_pixels,
        chans,
        stokes,
        stretch,
        imagemd,
        prec,
        overwrite,
    };

    validate_input_paths(task, &request)?;

    // CASA's immath returns True.
    let mut result = immath(&request);
    let warnings = result
        .as_mut()
        .map(|summary| std::mem::take(&mut summary.beam_warnings))
        .unwrap_or_default();
    image_task_returning(
        CasaValue::Bool { value: true },
        result.map(ImageAnalysisTaskResult::Immath),
    )
    .map(|success| {
        warnings
            .into_iter()
            .fold(success, TaskSuccess::with_output_line)
    })
}

fn validate_input_paths(
    task: &TaskInvocation<'_>,
    request: &ImmathRequest,
) -> Result<(), TaskFailure> {
    let varnames = effective_varnames(request);
    let quoted = if request.mode == "evalexpr" {
        casa_images::analysis::quoted_expression_names(&request.expr)
    } else {
        Vec::new()
    };
    if request.imagename.is_empty() && quoted.is_empty() {
        return Err(TaskFailure::validation(
            "imagename is empty: Image data set not found - please verify",
        )
        .at("imagename"));
    }
    for path in &request.imagename {
        if !task.workspace().join(path).exists() {
            return Err(TaskFailure::validation(format!(
                "imagename {:?} does not exist",
                path.display()
            ))
            .at("imagename"));
        }
    }
    for name in &quoted {
        let is_varname = varnames.iter().any(|v| v == name);
        let is_imagename = request
            .imagename
            .iter()
            .any(|p| p.display().to_string() == *name);
        if !is_varname && !is_imagename && !task.workspace().join(name).exists() {
            return Err(TaskFailure::validation(format!(
                "Image data set not found - please verify {name}"
            ))
            .at("expr"));
        }
    }
    Ok(())
}

fn effective_varnames(request: &ImmathRequest) -> Vec<String> {
    let mut names: Vec<String> = request
        .varnames
        .iter()
        .filter(|name| !name.is_empty())
        .cloned()
        .collect();
    let count = request.imagename.len();
    names.truncate(count);
    for index in names.len()..count {
        names.push(format!("IM{index}"));
    }
    names
}

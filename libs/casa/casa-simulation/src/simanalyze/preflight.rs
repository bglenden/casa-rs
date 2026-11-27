// SPDX-License-Identifier: LGPL-3.0-or-later
//! Protect composition inputs before any earlier application publishes.
use super::{
    Parameters, failure,
    plan::{Plan, suffixed},
};
use crate::simobserve::SimobserveError;
use std::path::{Path, PathBuf};

pub(super) fn check(p: &Parameters<'_>, plan: &Plan) -> Result<(), SimobserveError> {
    let mut outputs = Vec::new();
    let synthesis = if plan.inputs.synthesis.is_empty() {
        Vec::new()
    } else {
        [
            ".image",
            ".model",
            ".psf",
            ".residual",
            ".pb",
            ".image.pbcor",
            ".sumwt",
            ".weight",
            ".mask",
            ".image.flat",
            ".residual.flat",
            ".diff",
            ".fidelity",
        ]
        .map(|suffix| plan.file(suffix))
        .to_vec()
    };
    let mut synthesis = synthesis;
    if !plan.inputs.synthesis.is_empty()
        && (!p.text("featherimage").is_empty()
            || (plan.inputs.total_power.is_some() && p.text("modelimage").is_empty()))
    {
        synthesis.extend(
            [
                ".feather.image",
                ".feather.residual.flat",
                ".feather.image.flat",
                ".feather.diff",
                ".feather.fidelity",
            ]
            .map(|suffix| plan.file(suffix)),
        );
    }
    outputs.extend(synthesis.iter().cloned());
    if plan.inputs.synthesis.len() > 1 {
        outputs.push(plan.ms.clone());
    }
    let mut tp = Vec::new();
    if plan.inputs.total_power.is_some() {
        let project = plan.directory.file_name().unwrap().to_string_lossy();
        let image = plan.directory.join(format!("{project}.sd.image"));
        tp = ["", "0", "0.scaled", "0.image", "0.weight"]
            .map(|suffix| suffixed(&image, suffix))
            .to_vec();
        outputs.extend(tp.iter().cloned());
    }
    let mut inputs = plan.inputs.synthesis.clone();
    inputs.extend(plan.inputs.total_power.iter().cloned());
    inputs.push(plan.model.clone());
    refuse_collisions(&inputs, &outputs)?;
    for name in ["modelimage", "featherimage"] {
        let supplied = p.text(name);
        if supplied.is_empty() {
            continue;
        }
        let input = p.locate(&plan.directory, supplied);
        refuse_collisions(std::slice::from_ref(&input), &synthesis)?;
        // The final TP image is deliberately available as this invocation's
        // seed or feather input. Its intermediates are never caller inputs.
        refuse_collisions(
            std::slice::from_ref(&input),
            tp.get(1..).unwrap_or_default(),
        )?;
    }
    if !p.flag("overwrite") {
        for output in outputs {
            if std::fs::symlink_metadata(&output).is_ok() {
                return Err(failure(format!("{} already exists", output.display())));
            }
        }
    }
    Ok(())
}

fn existing(path: &Path) -> Result<Option<PathBuf>, SimobserveError> {
    match std::fs::canonicalize(path) {
        Ok(path) => Ok(Some(path)),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(error) => Err(failure(error)),
    }
}

fn refuse_collisions(inputs: &[PathBuf], outputs: &[PathBuf]) -> Result<(), SimobserveError> {
    for input in inputs {
        let Some(input) = existing(input)? else {
            continue;
        };
        for output in outputs {
            if existing(output)?.is_some_and(|output| output == input) {
                return Err(failure(format!(
                    "input {} would be overwritten by simulation imaging",
                    input.display()
                )));
            }
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::refuse_collisions;

    #[test]
    fn collisions_include_canonical_aliases() {
        let scratch = tempfile::tempdir().unwrap();
        let input = scratch.path().join("input");
        std::fs::create_dir(&input).unwrap();
        let output = scratch.path().join("./input");
        assert!(refuse_collisions(&[input], &[output]).is_err());
    }
}

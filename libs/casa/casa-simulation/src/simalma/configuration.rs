// SPDX-License-Identifier: LGPL-3.0-or-later
//! CASA array classification and scalar/vector exposure allocation.
use super::{
    Parameters, SimalmaRequest, failure,
    plan::{Array, Model},
};
use crate::{
    simobserve::{SimobserveError, antennas},
    simutil::qa,
};
use std::path::Path;

pub(super) struct Selection {
    pub arrays: Vec<Array>,
    pub tp_antennas: usize,
    pub tp_minutes: f64,
    pub tp_configuration: String,
}
pub(super) fn select(
    p: &Parameters,
    request: &SimalmaRequest,
    directory: &Path,
    project: &str,
    model: &Model,
    primary_beam: f64,
) -> Result<Selection, SimobserveError> {
    let repository = antennas::repository(&request.data_roots, Some(&request.workspace));
    let mut arrays = Vec::new();
    let mut tp_configuration = "aca.tp.cfg".to_string();
    let mut tp_index = None;
    let configs = p.strings("antennalist");
    if configs.is_empty() {
        return Err(failure(
            "at least one interferometer configuration is required",
        ));
    }
    let correlators = p.strings("correlator");
    for (i, configuration) in configs.iter().enumerate() {
        let resolved =
            antennas::alma_configuration(configuration, model.frequency / 1e9, &repository)?;
        let path = [
            directory.join(&resolved),
            request.workspace.join(&resolved),
            repository.join(&resolved),
        ]
        .into_iter()
        .find(|path| path.is_file())
        .ok_or_else(|| failure(format!("configuration {configuration} not found")))?;
        let input = std::fs::canonicalize(&path).map_err(failure)?;
        if std::fs::canonicalize(directory).is_ok_and(|project| input.starts_with(project)) {
            return Err(failure(
                "antenna configuration would be deleted with the existing project",
            ));
        }
        let config = crate::readantenna::read_antenna_config(&path, &*request.options.measures)?;
        let tp = config.telescope == "ALMASD";
        let upper = Path::new(configuration)
            .file_name()
            .unwrap()
            .to_string_lossy()
            .to_uppercase();
        let aca = upper.contains("ACA") && (config.telescope == "ACA" || tp);
        if !(aca || upper.contains("ALMA") && config.telescope == "ALMA") {
            return Err(failure(format!(
                "configuration {configuration} must identify ALMA or ACA consistently"
            )));
        }
        let resolution = if tp {
            primary_beam
        } else if let Some((_, resolution)) = configuration.split_once(';') {
            qa::value_in(&qa::read(resolution)?, "arcsec")?
        } else {
            crate::simutil::baselines::approximate_beam(&config, model.frequency / 1e9)?
        };
        if tp {
            tp_index = Some(i);
            tp_configuration = configuration.clone();
        }
        arrays.push(Array {
            configuration: configuration.clone(),
            resolution,
            minutes: 0.,
            aca,
            correlator: correlators.get(i).cloned().unwrap_or_default(),
            stem: antennas::stem(project, configuration)?,
        });
    }
    let times = p
        .strings("totaltime")
        .iter()
        .map(|time| qa::value_in(&qa::read(time)?, "min").map_err(SimobserveError::from))
        .collect::<Result<Vec<_>, _>>()?;
    if times.len() != 1 && times.len() != arrays.len() {
        return Err(failure(
            "totaltime must have one entry or one per configuration",
        ));
    }
    let best = (0..arrays.len())
        .min_by(|&a, &b| arrays[a].resolution.total_cmp(&arrays[b].resolution))
        .unwrap();
    for (i, array) in arrays.iter_mut().enumerate() {
        array.minutes = if times.len() > 1 {
            times[i]
        } else {
            times[0]
                * if i == best {
                    1.
                } else if Some(i) == tp_index {
                    4.
                } else if array.aca {
                    2.
                } else {
                    0.5
                }
        };
        if !array.minutes.is_finite() || array.minutes <= 0. {
            return Err(failure("observation times must be finite and positive"));
        }
    }
    let mut tp_antennas = usize::try_from(p.integer("tpnant")).map_err(failure)?;
    let mut tp_minutes = qa::value_in(&qa::read(p.text("tptime"))?, "min")?;
    if !tp_minutes.is_finite() || tp_minutes < 0. {
        return Err(failure("tptime must be finite and nonnegative"));
    }
    if let Some(i) = tp_index {
        if tp_antennas == 0 {
            tp_antennas = 1;
            tp_minutes = arrays[i].minutes;
        } else {
            tp_configuration = "aca.tp.cfg".into();
            if tp_minutes <= 0. {
                tp_minutes = arrays[i].minutes;
            }
        }
        arrays.remove(i);
    }
    if arrays.is_empty() {
        return Err(failure(
            "at least one interferometer configuration is required",
        ));
    }
    if tp_antennas > 0 && tp_minutes < 1. {
        return Err(failure(
            "total power requires tptime of at least one minute (CASA truncates to integer minutes)",
        ));
    }
    if tp_antennas == 0 && tp_minutes > 0. {
        return Err(failure("positive tptime requires at least one TP antenna"));
    }
    if tp_antennas > 0 && !p.text("complist").is_empty() {
        return Err(failure(
            "TP with a component list requires a prepared sky-model image, as in CASA",
        ));
    }
    if tp_antennas > 0 {
        let path = [
            request.workspace.join(&tp_configuration),
            repository.join(&tp_configuration),
        ]
        .into_iter()
        .find(|path| path.exists())
        .ok_or_else(|| failure("TP configuration not found"))?;
        let config = crate::readantenna::read_antenna_config(&path, &*request.options.measures)?;
        if tp_antennas > config.x.len() {
            return Err(failure("tpnant exceeds the TP configuration's antennas"));
        }
        tp_configuration = path.to_string_lossy().into_owned();
    }
    Ok(Selection {
        arrays,
        tp_antennas,
        tp_minutes,
        tp_configuration,
    })
}

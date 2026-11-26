// SPDX-License-Identifier: LGPL-3.0-or-later
//! Model metadata, configuration types and CASA's exposure ratios.
use super::{Parameters, SimalmaRequest, failure};
use crate::{
    simobserve::{SimobserveError, SimobserveParameters, antennas},
    simutil::{
        direction::Directions,
        qa,
        skymodel::{ModelOverrides, modify_model},
    },
};
use casa_components::ComponentList;
use casa_images::AnyPagedImage;
use casa_imaging_runtime::{Demand, admit};
use std::path::{Path, PathBuf};

pub(super) struct Array {
    pub configuration: String,
    pub resolution: f64,
    pub minutes: f64,
    pub aca: bool,
    pub correlator: String,
    pub stem: String,
}
pub(super) struct Model {
    pub direction: String,
    pub cell: [f64; 2],
    pub size: [f64; 2],
    pub frequency: f64,
    pub channels: usize,
    pub preparation_bytes: u64,
}
pub(super) struct Plan {
    pub directory: PathBuf,
    pub project: String,
    pub arrays: Vec<Array>,
    pub model: Model,
    pub observation: SimobserveParameters,
    pub tp_antennas: usize,
    pub tp_minutes: f64,
    pub tp_configuration: String,
    pub tp_stem: String,
    pub tp_vis: Vec<PathBuf>,
    pub noisy: bool,
    pub primary_beam: f64,
}
impl Plan {
    pub fn vis(&self, array: &Array) -> PathBuf {
        self.directory.join(format!(
            "{}{}.ms",
            array.stem,
            if self.noisy { ".noisy" } else { "" }
        ))
    }
}

pub(super) fn prepare(p: &Parameters, request: &SimalmaRequest) -> Result<Plan, SimobserveError> {
    let directory =
        std::path::absolute(request.workspace.join(p.text("project"))).map_err(failure)?;
    let project = directory
        .file_name()
        .ok_or_else(|| failure("project must have a directory name"))?
        .to_string_lossy()
        .into_owned();
    if directory.exists() && !p.flag("overwrite") {
        return Err(failure("project directory already exists"));
    }
    for key in ["skymodel", "complist"] {
        if let Ok(input) = std::fs::canonicalize(p.text(key)) {
            let output = std::fs::canonicalize(&directory).unwrap_or(directory.clone());
            if input.starts_with(output) {
                return Err(failure(format!(
                    "{key} would be deleted with the existing project"
                )));
            }
        }
    }
    let mut model = model(p, request)?;
    let primary_beam =
        1.13 * 0.2997924 / (model.frequency / 1e9) / 12. * 3600. * 180. / std::f64::consts::PI;
    let super::configuration::Selection {
        arrays,
        tp_antennas,
        tp_minutes,
        tp_configuration,
    } = super::configuration::select(p, request, &directory, &project, &model, primary_beam)?;
    protect_pointings(p, &request.workspace, &directory, &arrays)?;
    if model.cell[0] == 0. {
        let cell = arrays
            .iter()
            .map(|a| a.resolution)
            .fold(f64::INFINITY, f64::min)
            / 20.;
        model.cell = [cell; 2];
    }
    let mapsize = p.strings("mapsize");
    let mapsize = if mapsize.first().is_none_or(String::is_empty) {
        model.size.map(|v| format!("{v}arcsec")).to_vec()
    } else {
        mapsize
    };
    let directions = p.strings("direction");
    let direction = match directions.len() {
        0 => Directions::One(String::new()),
        1 => Directions::One(directions[0].clone()),
        _ => Directions::List(directions),
    };
    let noisy = p.0["pwv"].as_f64().unwrap() > 0.;
    let observation = SimobserveParameters {
        project: directory.to_string_lossy().into_owned(),
        skymodel: p.text("skymodel").to_owned(),
        inbright: p.text("inbright").to_owned(),
        indirection: p.text("indirection").to_owned(),
        incell: p.text("incell").to_owned(),
        incenter: p.text("incenter").to_owned(),
        inwidth: p.text("inwidth").to_owned(),
        complist: p.text("complist").to_owned(),
        compwidth: p.text("compwidth").to_owned(),
        setpointings: p.flag("setpointings"),
        ptgfile: p.text("ptgfile").to_owned(),
        integration: p.text("integration").to_owned(),
        direction,
        mapsize,
        maptype: "ALMA".into(),
        pointingspacing: format!("{}PB", 1. / 3f64.sqrt() / 1.13),
        hourangle: p.text("hourangle").to_owned(),
        thermalnoise: if noisy {
            "tsys-atm".into()
        } else {
            String::new()
        },
        user_pwv: p.0["pwv"].as_f64().unwrap(),
        t_ground: 270.,
        calflux: "0Jy".into(),
        graphics: "none".into(),
        verbose: p.flag("verbose"),
        overwrite: true,
        ..Default::default()
    };
    let tp_stem = antennas::stem(&project, &tp_configuration)?;
    Ok(Plan {
        directory,
        project,
        arrays,
        model,
        observation,
        tp_antennas,
        tp_minutes,
        tp_configuration,
        tp_stem,
        tp_vis: Vec::new(),
        noisy,
        primary_beam,
    })
}

fn model(p: &Parameters, request: &SimalmaRequest) -> Result<Model, SimobserveError> {
    let path = Path::new(p.text("skymodel"));
    if path.exists() {
        let shape = if path.is_file() {
            casa_images::fits_image_shape(path).map_err(failure)?
        } else {
            AnyPagedImage::open(path).map_err(failure)?.shape().to_vec()
        };
        let memory = shape
            .iter()
            .try_fold(64u64, |bytes, &n| bytes.checked_mul(n as u64))
            .and_then(|bytes| bytes.checked_add(32 << 20))
            .ok_or_else(|| failure("sky model dimensions overflow"))?;
        let r = request.options.resources;
        let _memory = admit(
            r.host(),
            r.policy(),
            &Demand {
                phase: "ALMA sky-model inspection",
                memory,
            },
        )
        .map_err(failure)?;
        let staging = tempfile::tempdir().map_err(failure)?;
        let one = |s: &str| {
            if s.is_empty() {
                Vec::new()
            } else {
                vec![s.to_owned()]
            }
        };
        let summary = modify_model(
            path,
            &staging.path().join("model"),
            &ModelOverrides {
                inbright: p.text("inbright").to_owned(),
                indirection: one(p.text("indirection")),
                incell: one(p.text("incell")),
                incenter: p.text("incenter").to_owned(),
                inwidth: p.text("inwidth").to_owned(),
            },
        )?;
        return Ok(Model {
            direction: summary.refdir,
            cell: [
                qa::value_in(&summary.cell[0], "arcsec")?,
                qa::value_in(&summary.cell[1], "arcsec")?,
            ],
            size: [
                qa::value_in(&summary.size[0], "arcsec")?,
                qa::value_in(&summary.size[1], "arcsec")?,
            ],
            frequency: qa::value_in(&summary.specrefval.quantity()?, "Hz")?,
            channels: summary.nchan,
            preparation_bytes: memory,
        });
    }
    let list = ComponentList::read_table(p.text("complist")).map_err(failure)?;
    let component =
        crate::simutil::skymodel::ComponentModel::from_list(&list, p.text("compwidth"), 1)?;
    // simalma's extent is 2*max offset, versus simobserve's 3*max offset.
    let size = qa::value_in(&qa::read(&component.size[0])?, "arcsec")? * 2. / 3.;
    Ok(Model {
        direction: component.refdir,
        cell: [0.; 2],
        size: [size; 2],
        frequency: qa::value_in(&component.specrefval, "Hz")?,
        channels: 1,
        preparation_bytes: 32 << 20,
    })
}

pub(super) fn publish_directory(p: &Parameters, plan: &Plan) -> Result<(), SimobserveError> {
    if std::fs::symlink_metadata(&plan.directory).is_ok() {
        if !p.flag("overwrite") {
            return Err(failure("project directory already exists"));
        }
        if plan.directory.is_symlink() {
            std::fs::remove_file(&plan.directory).map_err(failure)?;
        } else {
            std::fs::remove_dir_all(&plan.directory).map_err(failure)?;
        }
    }
    std::fs::create_dir_all(&plan.directory).map_err(failure)
}

// Match simobserve's stem substitution and outside/project search order
// before overwriting the directory that may hold the selected input.
fn protect_pointings(
    p: &Parameters,
    workspace: &Path,
    directory: &Path,
    arrays: &[Array],
) -> Result<(), SimobserveError> {
    if p.flag("setpointings") {
        return Ok(());
    }
    let project = std::fs::canonicalize(directory).unwrap_or(directory.to_path_buf());
    for array in arrays {
        let name = p.text("ptgfile").replace("$project", &array.stem);
        let in_workspace = workspace.join(&name);
        let path = if in_workspace.exists() {
            in_workspace
        } else {
            directory.join(name)
        };
        let input = std::fs::canonicalize(&path).map_err(failure)?;
        if path.starts_with(directory) || input.starts_with(&project) {
            return Err(failure(format!(
                "pointing input {} would be deleted with the existing project",
                path.display()
            )));
        }
    }
    Ok(())
}

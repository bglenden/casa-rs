// SPDX-License-Identifier: LGPL-3.0-or-later
//! The `simobserve` task (simulation Phase 5c, #841): CASA 6.7.6's
//! `task_simobserve.py`, ported as its specification (ADR-0018) onto the
//! simulator session ([`Simulator`]) and the `simutil` helpers
//! ([`crate::simutil`]).
//!
//! [`simobserve`] runs the task's stages in CASA's order, each in its own
//! module:
//! - the run itself: the mode, the project directory and the files' stem
//!   `<project>.<configuration>` (`Run`);
//! - the sky model: `modifymodel` and `flatimage` of `skymodel`, or the
//!   component-only `.compskymodel` (`sky`);
//! - the antenna list: ALMA resolution strings, the search through the
//!   project directory, the path given and CASA's `alma/simmos`, the primary
//!   beam and the resolution (`antennas`);
//! - the pointings: laid out or read, written to `<stem>.ptg.txt`, and
//!   checked against the model (`pointings`);
//! - the calibrator and the observing schedule (`schedule`);
//! - the observation: the session's setters, `observe`, `predict` and the
//!   rest frequencies (`observe`);
//! - leakage and thermal noise on copies of the MeasurementSet (`noise`).
//!
//! The files are CASA's: `<stem>.skymodel` and `.skymodel.flat` (or
//! `.compskymodel` and `.compskymodel.flat`), `.ptg.txt`, `.cal.cclist`,
//! `.ms` (or `.sd.ms`), `.fullpol.ms` and `.fullpol.cal`, and `.noisy.ms`,
//! `.noisier.ms` or `.noisy.sd.ms`. Graphics (`.skymodel.png`,
//! `.observe.png`, `.quick.psf`) come in Phase 5e; until then `graphics`
//! writes none and says so.
//!
//! Where CASA raises, the task returns [`SimobserveError`] with CASA's
//! message. Where CASA's task does something physically wrong, casa-rs
//! departs from it, each departure recorded in `docs/CASA (C++) bugs.md`:
//! - **A calibrator's last pass.** With a calibrator, CASA observes each
//!   pass over the map only when it completes, so a `totaltime` that ends
//!   inside a pass drops that pass, and one shorter than a pass observes the
//!   calibrator alone. casa-rs observes the pointings the time allows.
//! - **The calibrator's scans.** CASA selects the map's fields before
//!   predicting (`setdata(fieldid=range(nfld))`), which leaves the
//!   calibrator's scans unpredicted. casa-rs predicts every scan.
//! - **Two component lists.** With a calibrator and a `complist`, CASA
//!   predicts from the two lists' names joined by a comma, which is not a
//!   table, so its `predict` fails and the MeasurementSet is left
//!   unpredicted. casa-rs predicts both lists.
//! - **`<stem>.noisier.T.cal`.** CASA deletes the Tsys table `tsys-atm`
//!   writes beside `.noisy.ms` and `.noisy.sd.ms` but leaves the one beside
//!   `.noisier.ms`; casa-rs writes none.
//! - **Pointings of no length.** Pointing times summing to zero (an
//!   `integration` of `0s`) make CASA's observing loop run forever; casa-rs
//!   refuses them.
//! - **A total-power MeasurementSet.** Corrupting an existing
//!   MeasurementSet, CASA takes it for total power when any UVW value is
//!   zero (`uvw.all() == 0`); casa-rs when every one is, as was meant.
//! - **A sky model the task writes.** A `skymodel` that is the
//!   `<stem>.skymodel` the task writes is deleted by CASA before it is read;
//!   casa-rs refuses it.
//! - **A project path.** CASA names the files `<project>.<cfg>` inside the
//!   directory `<project>`, which nests a path inside itself; casa-rs names
//!   them by the directory's last component.
//!
//! A predicted model under the primary beam holds one grid per field
//! (the session's [`Simulator::predict`]), so a mosaic of many pointings
//! over a large model can need more memory than the session's budget,
//! which refuses it before it starts.

mod antennas;
mod noise;
mod observe;
mod parameters;
mod pointings;
mod schedule;
mod sky;
#[cfg(test)]
mod tests;

pub use parameters::SimobserveParameters;

use std::path::{Path, PathBuf};

use thiserror::Error;

use crate::readantenna::ReadAntennaError;
use crate::session::{Simulator, SimulatorOptions};
use crate::simutil::SimutilError;
use crate::simutil::skymodel::SkyModelError;
use crate::{SimulatorError, simutil};

/// A `simobserve` run: CASA's parameters and what CASA's task takes from
/// its environment.
#[derive(Clone)]
pub struct SimobserveRequest {
    /// The task's parameters.
    pub parameters: SimobserveParameters,
    /// CASA's data directories, which `ctsys.resolve("alma/simmos")`
    /// searches for the antenna configurations: the first holding
    /// `alma/simmos` is the repository. With none, CASA's unresolved
    /// `alma/simmos` is taken relative to the working directory.
    pub data_roots: Vec<PathBuf>,
    /// The sessions' measures data, resources and clock.
    pub options: SimulatorOptions,
}

/// What a run wrote.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SimobserveOutcome {
    /// The project directory (`project`).
    pub directory: PathBuf,
    /// `<project>.<configuration>`, the stem of the files' names.
    pub stem: String,
    /// The files written, in the order they were written; a file written
    /// twice is listed once.
    pub products: Vec<PathBuf>,
    /// What the run did not do that it was asked to, such as graphics.
    pub warnings: Vec<String>,
}

/// What `simobserve` refuses.
#[derive(Debug, Error)]
pub enum SimobserveError {
    /// What CASA's task raises, with its message.
    #[error("{0}")]
    Raised(String),
    /// A quantity or direction the `simutil` helpers refuse.
    #[error(transparent)]
    Simutil(#[from] SimutilError),
    /// A sky model that cannot be prepared.
    #[error(transparent)]
    SkyModel(#[from] SkyModelError),
    /// An antenna configuration that cannot be read.
    #[error(transparent)]
    Antennas(#[from] ReadAntennaError),
    /// What a simulator session refuses.
    #[error(transparent)]
    Simulator(#[from] SimulatorError),
    /// A table the task reads or rewrites.
    #[error("{table}: {reason}")]
    Table {
        /// The table.
        table: PathBuf,
        /// The failure.
        reason: String,
    },
    /// A file-system operation that failed.
    #[error("{}: {source}", path.display())]
    Io {
        /// The path.
        path: PathBuf,
        /// The failure.
        source: std::io::Error,
    },
}

impl SimobserveError {
    /// What CASA raises with `message`.
    pub(crate) fn raised(message: impl Into<String>) -> Self {
        Self::Raised(message.into())
    }

    /// A file-system failure at `path`.
    pub(crate) fn io(path: &Path) -> impl FnOnce(std::io::Error) -> Self + '_ {
        move |source| Self::Io {
            path: path.to_path_buf(),
            source,
        }
    }
}

/// `simobserve(...)`: run CASA's task (see the module documentation).
///
/// # Errors
///
/// What CASA raises, with its message, and what the sky-model helpers, the
/// antenna-list reader and the simulator sessions refuse.
pub fn simobserve(request: &SimobserveRequest) -> Result<SimobserveOutcome, SimobserveError> {
    let mut run = Run::start(request)?;
    let input = sky::prepare(&mut run)?;
    let array = antennas::resolve(&mut run, &input)?;
    let sky = sky::complete(&mut run, input, &array)?;
    let pointings = pointings::prepare(&mut run, &sky, &array)?;
    let calibrator = schedule::calibrator(&mut run)?;
    if run.predict {
        observe::observe(&mut run, &sky, &array, &pointings, calibrator.as_ref())?;
    } else if !run.parameters.thermalnoise.is_empty() || run.parameters.leakage > 0.0 {
        let ms = run.measurement_set();
        if !ms.exists() {
            return Err(SimobserveError::raised(format!(
                "Couldn't find {}",
                ms.display()
            )));
        }
        run.uvmode = !noise::is_total_power(&ms)?;
    }
    noise::corrupt(&mut run, &sky, &array)?;
    run.clean_up(&sky)?;
    Ok(SimobserveOutcome {
        directory: run.directory,
        stem: run.stem,
        products: run.products,
        warnings: run.warnings,
    })
}

/// One run's state, as CASA's task keeps it in its variables.
struct Run<'a> {
    request: &'a SimobserveRequest,
    parameters: &'a SimobserveParameters,
    /// `fileroot`: the project directory.
    directory: PathBuf,
    /// `project` once the configuration is added: `<project>.<cfg>`.
    stem: String,
    /// `predict`: `obsmode` observes (`int` or `sd`).
    predict: bool,
    /// `uvmode`: an interferometer.
    uvmode: bool,
    /// `antennalist`, as the run resolves it.
    antennalist: String,
    /// The files written.
    products: Vec<PathBuf>,
    /// The outcome's warnings.
    warnings: Vec<String>,
}

impl<'a> Run<'a> {
    /// The mode, the project directory and the stem, and the checks CASA
    /// makes before reading the sky model.
    fn start(request: &'a SimobserveRequest) -> Result<Self, SimobserveError> {
        let parameters = &request.parameters;
        let obsmode = parameters.obsmode.as_str();
        tracing::info!("Requested obsmode is \"{obsmode}\"");
        let predict = obsmode.starts_with('i') || obsmode.starts_with('s');
        let mut antennalist = parameters.antennalist.clone();
        let uvmode = if predict {
            let uvmode = obsmode.starts_with('i');
            if !uvmode {
                antennalist.clone_from(&parameters.sdantlist);
            }
            uvmode
        } else if !parameters.sdantlist.is_empty() {
            if !antennalist.is_empty() {
                return Err(SimobserveError::raised(
                    "Both antennalist and sdantlist are defined. Define only one of them.",
                ));
            }
            antennalist.clone_from(&parameters.sdantlist);
            false
        } else {
            true
        };
        let directory = PathBuf::from(&parameters.project);
        if !directory.exists() {
            std::fs::create_dir(&directory).map_err(SimobserveError::io(&directory))?;
        }
        // CASA names the files `project.<cfg>` inside the directory
        // `project`, which works for a plain name; casa-rs names them by the
        // directory's last component, so a project may be a path.
        let name = directory.file_name().map_or_else(
            || parameters.project.clone(),
            |name| name.to_string_lossy().into_owned(),
        );
        let stem = antennas::stem(&name, &antennalist)?;
        let mut run = Self {
            request,
            parameters,
            directory,
            stem,
            predict,
            uvmode,
            antennalist,
            products: Vec::new(),
            warnings: Vec::new(),
        };
        run.check_overwrite()?;
        run.check_sky_inputs()?;
        if !matches!(parameters.graphics.as_str(), "none" | "") {
            let warning = format!(
                "graphics='{}' is not yet implemented in casa-rs (#841 Phase 5e): no .png or \
                 .quick.psf is written",
                parameters.graphics
            );
            tracing::warn!("{warning}");
            run.warnings.push(warning);
        }
        Ok(run)
    }

    /// CASA's refusal of an existing MeasurementSet without `overwrite`.
    fn check_overwrite(&self) -> Result<(), SimobserveError> {
        if self.parameters.overwrite || !self.predict {
            return Ok(());
        }
        let ms = self.measurement_set();
        if ms.exists() {
            return Err(SimobserveError::raised(format!(
                "{} exists but overwrite=F",
                ms.display()
            )));
        }
        Ok(())
    }

    /// CASA's checks that a sky model exists, and its refusal of a
    /// component list in a single-dish simulation (CAS-5095).
    fn check_sky_inputs(&self) -> Result<(), SimobserveError> {
        let skymodel = self.skymodel();
        let complist = &self.parameters.complist;
        if !exists(&skymodel) && !exists(complist) {
            if !skymodel.is_empty() {
                tracing::warn!("Your skymodel '{skymodel}' could not be found.");
            }
            if !complist.is_empty() {
                tracing::warn!("Your complist '{complist}' could not be found.");
            }
            return Err(SimobserveError::raised(
                if skymodel.is_empty() && complist.is_empty() {
                    "At least one of skymodel or complist must be set."
                } else {
                    "No sky input found.  At least one of skymodel or complist must exist."
                },
            ));
        }
        if self.parameters.obsmode.starts_with('s') && exists(complist) {
            return Err(SimobserveError::raised(
                "Single dish simulation has a flux recovery issue when using a components list \
                 as an input.\nPlease generate compskymodel image first by obsmode='' and use \
                 the image as the skymodel input.\nSorry for the inconvenience.",
            ));
        }
        Ok(())
    }

    /// `skymodel` with `$project` replaced by the stem.
    fn skymodel(&self) -> String {
        self.parameters.skymodel.replace("$project", &self.stem)
    }

    /// `fileroot + "/" + project + suffix`.
    fn file(&self, suffix: &str) -> PathBuf {
        self.directory.join(format!("{}{suffix}", self.stem))
    }

    /// The MeasurementSet observed: `<stem>.ms`, or `<stem>.sd.ms` for a
    /// single dish.
    fn measurement_set(&self) -> PathBuf {
        self.file(if self.uvmode { ".ms" } else { ".sd.ms" })
    }

    /// Record `path` as written.
    fn wrote(&mut self, path: &Path) {
        if !self.products.iter().any(|known| known == path) {
            self.products.push(path.to_path_buf());
        }
    }

    /// CASA's clean-up: the regridded flat model and the Tsys tables
    /// `tsys-atm` leaves.
    fn clean_up(&mut self, sky: &sky::Sky) -> Result<(), SimobserveError> {
        let mut regrid = sky.flat.clone().into_os_string();
        regrid.push(".regrid");
        for path in [
            PathBuf::from(regrid),
            self.file(".noisy.T.cal"),
            self.file(".noisy.sd.T.cal"),
        ] {
            remove(&path)?;
            self.products.retain(|known| *known != path);
        }
        Ok(())
    }
}

/// `os.path.exists` of a path given as a string; `''` does not exist.
fn exists(path: &str) -> bool {
    !path.is_empty() && Path::new(path).exists()
}

/// `shutil.rmtree` or `os.remove` of `path` when it exists.
fn remove(path: &Path) -> Result<(), SimobserveError> {
    let Ok(metadata) = std::fs::symlink_metadata(path) else {
        return Ok(());
    };
    if metadata.is_dir() {
        std::fs::remove_dir_all(path)
    } else {
        std::fs::remove_file(path)
    }
    .map_err(SimobserveError::io(path))
}

/// `shutil.copytree(from, to)`: copy the directory `from`, a table, to
/// `to`, which must not exist.
fn copy_tree(from: &Path, to: &Path) -> Result<(), SimobserveError> {
    std::fs::create_dir(to).map_err(SimobserveError::io(to))?;
    for entry in std::fs::read_dir(from).map_err(SimobserveError::io(from))? {
        let entry = entry.map_err(SimobserveError::io(from))?;
        let source = entry.path();
        let target = to.join(entry.file_name());
        if entry
            .file_type()
            .map_err(SimobserveError::io(&source))?
            .is_dir()
        {
            copy_tree(&source, &target)?;
        } else {
            std::fs::copy(&source, &target).map_err(SimobserveError::io(&source))?;
        }
    }
    Ok(())
}

/// A simulator session on the new MeasurementSet `path`.
fn open_session(run: &Run<'_>, path: &Path) -> Simulator {
    Simulator::open(path, run.request.options.clone())
}

/// Python's `str(x)` of a float, as CASA writes numbers into messages and
/// quantities.
fn python(value: f64) -> String {
    simutil::python_float(value)
}

// SPDX-License-Identifier: LGPL-3.0-or-later
//! `simobserve`'s parameters, with CASA 6.7.6's names, types and defaults.

use crate::simutil::direction::Directions;

/// CASA's `simobserve` parameters, as `casatasks.simobserve` takes them.
///
/// The defaults are CASA 6.7.6's. `compwidth`'s is CASA's literal
/// `'"8GHz"'`, quotes included, which is not a quantity: a component-only
/// simulation with the default therefore observes 2 GHz, as in CASA.
#[derive(Debug, Clone, PartialEq)]
pub struct SimobserveParameters {
    /// The project directory, and the first part of every file's name
    /// (`project`). CASA takes a name in the working directory; casa-rs
    /// also takes a path, and names the files by its last component.
    pub project: String,
    /// The model image, a CASA image or FITS file (`skymodel`); `$project`
    /// is replaced by `<project>.<configuration>`.
    pub skymodel: String,
    /// The peak brightness the model is scaled to (`inbright`).
    pub inbright: String,
    /// The direction of the model's centre (`indirection`).
    pub indirection: String,
    /// The model's cell (`incell`).
    pub incell: String,
    /// The frequency of the model's centre channel (`incenter`).
    pub incenter: String,
    /// The model's channel width (`inwidth`).
    pub inwidth: String,
    /// A component list, observed with the model or alone (`complist`).
    pub complist: String,
    /// The bandwidth of a component-only simulation (`compwidth`).
    pub compwidth: String,
    /// The channels of a component-only simulation (`comp_nchan`).
    pub comp_nchan: i64,
    /// Lay out the pointings, rather than read them from `ptgfile`
    /// (`setpointings`).
    pub setpointings: bool,
    /// The pointing file read when `setpointings` is false (`ptgfile`).
    pub ptgfile: String,
    /// The integration time (`integration`).
    pub integration: String,
    /// The map's centre, or the pointings themselves when there are several
    /// (`direction`); the model's centre when empty. One direction lays a
    /// map out ([`Directions::One`]); several are the pointings
    /// ([`Directions::List`]). A list of one, which CASA centres at its
    /// mean rounded at precision 5 ([`Directions::split`]), is taken as its
    /// direction: the task contract resolves a string to a list of one
    /// (`docs/CASA (C++) bugs.md`).
    pub direction: Directions,
    /// The map's size (`mapsize`); the model's when empty.
    pub mapsize: Vec<String>,
    /// The map's layout: `hexagonal`, `square`, `ALMA` or `ALMA2012`
    /// (`maptype`).
    pub maptype: String,
    /// The pointings' spacing, an angle, `<n>PB` or `nyquist`
    /// (`pointingspacing`); λ/D/√3 for interferometers when empty.
    pub pointingspacing: String,
    /// A phase calibrator's direction (`caldirection`).
    pub caldirection: String,
    /// The calibrator's flux (`calflux`).
    pub calflux: String,
    /// `int` or `sd` to observe, `''` to corrupt an existing MeasurementSet
    /// (`obsmode`).
    pub obsmode: String,
    /// The date of the observation (`refdate`).
    pub refdate: String,
    /// The hour angle at the middle of the observation, or `transit`
    /// (`hourangle`).
    pub hourangle: String,
    /// The observation's length, or a count of passes over the map with no
    /// unit (`totaltime`).
    pub totaltime: String,
    /// The interferometer's antenna configuration (`antennalist`).
    pub antennalist: String,
    /// The single dish's antenna configuration (`sdantlist`).
    pub sdantlist: String,
    /// The antenna of `sdantlist` that observes (`sdant`).
    pub sdant: i64,
    /// The spectral window's frequency frame (`outframe`).
    pub outframe: String,
    /// `tsys-atm`, `tsys-manual` or `''` (`thermalnoise`).
    pub thermalnoise: String,
    /// The precipitable water vapour, mm (`user_pwv`).
    pub user_pwv: f64,
    /// The ground's temperature, K (`t_ground`).
    pub t_ground: f64,
    /// The atmosphere's temperature, K (`t_sky`), for `tsys-manual`.
    pub t_sky: f64,
    /// The zenith opacity (`tau0`), for `tsys-manual`.
    pub tau0: f64,
    /// The noise's random seed (`seed`).
    pub seed: i64,
    /// The polarization leakage's amplitude (`leakage`).
    pub leakage: f64,
    /// ALMA's correlator, for the correlator efficiency (`correlator`).
    pub correlator: String,
    /// `both`, `screen`, `file` or `none` (`graphics`).
    pub graphics: String,
    /// Log more, and apply the shadow and elevation limits (`verbose`).
    pub verbose: bool,
    /// Replace the project's files (`overwrite`). CASA's default, true,
    /// replaces them without asking: the task writes only inside the
    /// project directory.
    pub overwrite: bool,
}

impl Default for SimobserveParameters {
    /// CASA 6.7.6's defaults.
    fn default() -> Self {
        Self {
            project: "sim".to_string(),
            skymodel: String::new(),
            inbright: String::new(),
            indirection: String::new(),
            incell: String::new(),
            incenter: String::new(),
            inwidth: String::new(),
            complist: String::new(),
            compwidth: "\"8GHz\"".to_string(),
            comp_nchan: 1,
            setpointings: true,
            ptgfile: "$project.ptg.txt".to_string(),
            integration: "10s".to_string(),
            direction: Directions::One(String::new()),
            mapsize: vec![String::new(), String::new()],
            maptype: "hexagonal".to_string(),
            pointingspacing: String::new(),
            caldirection: String::new(),
            calflux: "1Jy".to_string(),
            obsmode: "int".to_string(),
            refdate: "2014/01/01".to_string(),
            hourangle: "transit".to_string(),
            totaltime: "7200s".to_string(),
            antennalist: String::new(),
            sdantlist: "aca.tp.cfg".to_string(),
            sdant: 0,
            outframe: "LSRK".to_string(),
            thermalnoise: "tsys-atm".to_string(),
            user_pwv: 0.5,
            t_ground: 270.0,
            t_sky: 260.0,
            tau0: 0.1,
            seed: 11_111,
            leakage: 0.0,
            correlator: String::new(),
            graphics: "both".to_string(),
            verbose: false,
            overwrite: true,
        }
    }
}

// SPDX-License-Identifier: LGPL-3.0-or-later
//! Corruption: polarization leakage on a copy of the MeasurementSet,
//! `<stem>.fullpol.ms`, with its table `<stem>.fullpol.cal`; then thermal
//! noise on a copy of that or of the observed one, `<stem>.noisier.ms`,
//! `<stem>.noisy.ms` or `<stem>.noisy.sd.ms`.

use std::path::{Path, PathBuf};

use casa_ms::MeasurementSet;

use super::antennas::Array;
use super::sky::Sky;
use super::{Run, SimobserveError, copy_tree, make_way, python, remove};
use crate::session::Simulator;
use crate::simutil::analysis::{self, AnalysisError};
use crate::simutil::receivers::noise_temperature;
use crate::{AtmosphereSpec, LeakageSpec, NoiseSpec, TsysParameters};

/// The cosmic background's temperature `simobserve` uses, K.
const CMB_K: f64 = 2.725;

/// The telescopes whose thermal noise CASA models.
const KNOWN_TELESCOPES: [&str; 6] = ["ALMASD", "ALMA", "ACA", "SMA", "EVLA", "VLA"];

/// The relative humidity `setnoise` takes by default, %.
const RELATIVE_HUMIDITY_PERCENT: f64 = 20.0;

/// Leakage, then thermal noise, as `leakage` and `thermalnoise` ask.
pub(super) fn corrupt(run: &mut Run<'_>, sky: &Sky, array: &Array) -> Result<(), SimobserveError> {
    let parameters = run.parameters;
    if !run.uvmode && parameters.leakage > 0. {
        tracing::warn!("Can't corrupt SD data with polarization leakage");
    }
    let fullpol = (parameters.leakage > 0. && run.uvmode && run.predict)
        .then(|| leak(run))
        .transpose()?;
    if !parameters.thermalnoise.is_empty() {
        add_noise(run, sky, array, fullpol.as_deref())?;
    }
    Ok(())
}

/// Copy the MeasurementSet to `<stem>.fullpol.ms` and corrupt it by
/// leakages of amplitude `leakage`, written to `<stem>.fullpol.cal`.
fn leak(run: &mut Run<'_>) -> Result<PathBuf, SimobserveError> {
    let ms = run.measurement_set();
    let fullpol = run.file(".fullpol.ms");
    if ms.exists() {
        tracing::warn!(
            "Copying {} to {} and adding polarization leakage ...",
            ms.display(),
            fullpol.display()
        );
        remove(&fullpol)?;
        copy_tree(&ms, &fullpol)?;
        let table = run.file(".fullpol.cal");
        make_way(&table)?;
        let mut sm = Simulator::open_from_ms(&fullpol, run.request.options.clone())?;
        sm.set_data(&[], &[], "")?;
        sm.set_leakage(&LeakageSpec {
            table: table.display().to_string(),
            amplitude: vec![run.parameters.leakage],
            offset: vec![0.0],
        })?;
        sm.corrupt()?;
        sm.close()?;
        run.wrote(&fullpol);
        run.wrote(&table);
    }
    tracing::info!("Created {}", ms.display());
    Ok(fullpol)
}

/// Copy the MeasurementSet (the leaked one when there is one) and add
/// `thermalnoise`'s noise to the copy: Tsys from the receiver and
/// efficiencies `noisetemp` gives, and the atmosphere `tsys-manual` gives
/// or ATM's (`tsys-atm`).
fn add_noise(
    run: &mut Run<'_>,
    sky: &Sky,
    array: &Array,
    fullpol: Option<&Path>,
) -> Result<(), SimobserveError> {
    let parameters = run.parameters;
    tracing::warn!("Applying thermal noise ...");
    let (source, noisy) = if !run.uvmode {
        (run.file(".sd.ms"), run.file(".noisy.sd.ms"))
    } else if let Some(fullpol) = fullpol {
        (fullpol.to_path_buf(), run.file(".noisier.ms"))
    } else {
        (run.file(".ms"), run.file(".noisy.ms"))
    };
    if !source.exists() {
        return Err(SimobserveError::raised(format!(
            "Couldn't find {}",
            source.display()
        )));
    }
    tracing::warn!(
        "Copying {} to {} and adding thermal noise ...",
        source.display(),
        noisy.display()
    );
    remove(&noisy)?;
    copy_tree(&source, &noisy)?;
    let telescope = if run.predict {
        array.config.telescope.clone()
    } else {
        let telescope = observing_telescope(&noisy)?;
        tracing::info!("telescopename read from {}: {telescope}", noisy.display());
        telescope
    };
    if !KNOWN_TELESCOPES.contains(&telescope.as_str()) {
        tracing::warn!("thermal noise only works properly for ALMA/ACA, (E)VLA, and SMA");
    }
    let correlator = Some(parameters.correlator.as_str()).filter(|name| !name.is_empty());
    let receiver = noise_temperature(&telescope, &sky.model.specrefval, None, None, correlator)?;
    let antenna = receiver.antenna();
    // `senscoeff`: 1/√2 (CASA's default, asked for by -1) for an
    // interferometer, 1 for a single dish.
    let sensitivity = if run.uvmode { -1.0 } else { 1.0 };
    if parameters.verbose {
        tracing::info!("antenna efficiency    = {}", python(antenna));
        tracing::info!("spillover efficiency  = {}", python(receiver.spillover));
        tracing::info!("correlator name       = {}", parameters.correlator);
        tracing::info!("correlator efficiency = {}", python(receiver.correlator));
    }
    let tsys = TsysParameters {
        tau: parameters.tau0,
        atmosphere_k: parameters.t_sky,
        antenna_efficiency: antenna,
        spillover_efficiency: receiver.spillover,
        correlator_efficiency: receiver.correlator,
        receiver_k: receiver.receiver_temperature_k,
        ground_k: parameters.t_ground,
        cmb_k: CMB_K,
        sensitivity,
        receiver_type: 0,
    };
    // `tsys-atm` names a table, `<noisy>.T.cal`, which CASA deletes for
    // `.noisy` and `.noisy.sd` and leaves for `.noisier`; casa-rs writes
    // none.
    let spec = if parameters.thermalnoise == "tsys-manual" {
        NoiseSpec::tsys_manual(tsys)
    } else {
        let defaults = TsysParameters::default();
        NoiseSpec::tsys_atm(
            TsysParameters {
                tau: defaults.tau,
                atmosphere_k: defaults.atmosphere_k,
                ..tsys
            },
            parameters.user_pwv,
            AtmosphereSpec {
                relative_humidity_percent: RELATIVE_HUMIDITY_PERCENT,
                ..AtmosphereSpec::default()
            },
        )
    };
    let seed = i32::try_from(parameters.seed).map_err(|_| {
        SimobserveError::raised(format!(
            "seed {} does not fit a 32-bit integer",
            parameters.seed
        ))
    })?;
    let mut sm = Simulator::open_from_ms(&noisy, run.request.options.clone())?;
    sm.set_data(&[], &[], "")?;
    sm.set_seed(seed);
    sm.set_noise(&spec)?;
    sm.corrupt()?;
    sm.close()?;
    run.wrote(&noisy);
    if parameters.verbose {
        tracing::info!("done corrupting with thermal noise");
    }
    Ok(())
}

/// OBSERVATION's first TELESCOPE_NAME.
fn observing_telescope(ms: &Path) -> Result<String, SimobserveError> {
    let failed = |reason: &dyn std::fmt::Display| SimobserveError::Table {
        table: ms.join("OBSERVATION"),
        reason: format!("TELESCOPE_NAME: {reason}"),
    };
    let set = MeasurementSet::open(ms).map_err(|error| failed(&error))?;
    let observation = set.observation().map_err(|error| failed(&error))?;
    observation
        .string(0, "TELESCOPE_NAME")
        .map_err(|error| failed(&error))
}

/// `simutil.ismstp` ([`analysis::is_total_power`]).
pub(super) fn is_total_power(ms: &Path) -> Result<bool, SimobserveError> {
    analysis::is_total_power(ms).map_err(|error| match error {
        AnalysisError::Table { table, reason } => SimobserveError::Table { table, reason },
        other => SimobserveError::Table {
            table: ms.to_path_buf(),
            reason: other.to_string(),
        },
    })
}

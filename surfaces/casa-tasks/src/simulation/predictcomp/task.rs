// SPDX-License-Identifier: LGPL-3.0-or-later
//! Provider for the public predictcomp task.

use std::path::{Path, PathBuf};

use super::super::SimobserveArtifact;
use crate::{TaskFailure, TaskInvocation, TaskSuccess};
use casa_calibration::ft::{predict_calibrator_component, predict_solar_component_2012};
use casa_components::SkyComponent;
use casa_contracts::{CasaFloat, CasaValue, FloatDtype, RunProductKind, casacore_value_to_casa};
use casa_core::Value;
use casa_core::measures::epoch::{EpochRef, MEpoch};
use casa_core::quanta::{Quantity, Unit};
use casa_measures::MeasuresRuntime;
use casa_simulation::predictcomp::{CalibratorBaselines, calibrator_baselines};
use casa_tables::publication::check_write_access;
use schemars::JsonSchema;
use serde::Serialize;

#[derive(Serialize, JsonSchema)]
pub(crate) struct PredictcompReport {
    artifacts: Vec<SimobserveArtifact>,
    plot: Option<Plot>,
}

#[derive(Serialize, JsonSchema)]
struct Plot {
    object: String,
    standard: String,
    epoch: String,
    antennas: String,
    azimuth_deg: f64,
    elevation_deg: f64,
    frequencies_ghz: Vec<f64>,
    x: Vec<f64>,
    amplitudes_jy: Vec<Vec<f64>>,
    axis_unit: String,
    symbol: String,
    amplitude_from_zero: bool,
    baseline_from_zero: bool,
    zero_baseline_flux: Option<f64>,
    show: bool,
    save: String,
}

pub(crate) fn run(task: &TaskInvocation<'_>) -> Result<TaskSuccess, TaskFailure> {
    let object = task.required_text("objname")?;
    if object.is_empty() {
        return Err(TaskFailure::validation("Error, objname is undefined").at("objname"));
    }
    let standard = task.required_text("standard")?;
    let frequencies = frequency_grid(task)?;
    let epoch_text = task.required_text("epoch")?;
    let epoch = parse_epoch(&epoch_text)?;
    let Some(prefix) = output_prefix(task.workspace(), filename_argument(task, "prefix")?)? else {
        return TaskSuccess::new(
            CasaValue::Bool { value: false },
            &PredictcompReport {
                artifacts: Vec::new(),
                plot: None,
            },
        );
    };
    let antenna = task.required_text("antennalist")?;
    let modern = standard == "Butler-JPL-Horizons 2012";
    let workspace = task.workspace();
    let predicted = if modern {
        Some(predict_solar_component_2012(&object, &epoch, &frequencies, &prefix).map_err(failed)?)
    } else {
        predict_calibrator_component(workspace, &object, &standard, &epoch, &frequencies, &prefix)
            .map_err(failed)?
    };
    let Some(predicted) = predicted else {
        return TaskSuccess::new(
            CasaValue::None,
            &PredictcompReport {
                artifacts: Vec::new(),
                plot: None,
            },
        );
    };
    predicted.write_for_task(workspace).map_err(failed)?;
    let component = &predicted.component;
    let flux_i = component.flux().stokes_jy_values().map_err(failed)?[0];
    let freqs_ghz = frequencies.iter().map(|hz| hz * 1e-9).collect::<Vec<_>>();
    let mut fields = vec![
        ("clist", CasaValue::string(&predicted.filename)),
        ("objname", CasaValue::string(&object)),
        ("standard", CasaValue::string(&standard)),
        ("epoch", epoch_record(epoch.value().as_mjd())),
        (
            "freqs (GHz)",
            CasaValue::float64_vector(freqs_ghz.iter().copied()),
        ),
        ("antennalist", CasaValue::string(&antenna)),
        ("shape", record(component.shape().to_record())?),
        (
            "spectrum",
            spectrum_record(component, (!antenna.is_empty()).then_some(flux_i))?,
        ),
        ("savedfig", CasaValue::None),
    ];
    let show = task.flag("showplot")?;
    let save = filename_argument(task, "savefig")?;
    let mut artifacts = vec![artifact(&predicted.filename, RunProductKind::CasaTable)];
    let plot = if !antenna.is_empty() && (show || !save.is_empty()) {
        let roots = data_roots()?;
        let samples =
            calibrator_baselines(workspace, &antenna, component, &epoch, &frequencies, &roots)
                .map_err(failed)?;
        let unit = task.required_text("blunit")?;
        fields.extend(baseline_record(&samples, &unit));
        if samples.lengths_m.is_empty() {
            None
        } else {
            Some(make_plot(
                task,
                samples,
                PlotRequest {
                    object,
                    standard,
                    epoch: epoch_text,
                    freqs_ghz,
                    unit,
                    flux_i,
                    show,
                    save,
                },
            )?)
        }
    } else {
        None
    };
    if let Some(plot) = plot.as_ref().filter(|plot| !plot.save.is_empty()) {
        fields
            .iter_mut()
            .find(|(name, _)| *name == "savedfig")
            .expect("savedfig field")
            .1 = CasaValue::string(&plot.save);
        artifacts.push(artifact(&plot.save, RunProductKind::File));
    }
    TaskSuccess::new(
        CasaValue::dict(fields),
        &PredictcompReport { artifacts, plot },
    )
}

fn filename_argument(task: &TaskInvocation<'_>, name: &str) -> Result<String, TaskFailure> {
    match task.argument_text(name) {
        Some(literal) => Ok(literal.to_owned()),
        None => task.required_text(name),
    }
}

fn output_prefix(workspace: &Path, mut prefix: String) -> Result<Option<String>, TaskFailure> {
    if prefix.is_empty() {
        if check_write_access(workspace).is_ok() {
            return Ok(Some(prefix));
        }
        let fallback = Path::new("/tmp");
        return Ok(check_write_access(fallback).is_ok().then(|| "/tmp/".into()));
    }
    let explicit = prefix.starts_with('/') || prefix.starts_with("./") || prefix.starts_with("../");
    if !explicit
        && !workspace.join(&prefix).exists()
        && let Some(path) = data_roots()?
            .into_iter()
            .map(|root| root.join(&prefix))
            .find(|path| path.exists())
    {
        prefix = path.to_string_lossy().into_owned();
    }
    if prefix.len() > 1
        && !prefix.ends_with('/')
        && Path::new(&prefix).parent() == Some(Path::new("/"))
    {
        prefix.push('/');
    }
    Ok(Some(prefix))
}

fn data_roots() -> Result<Vec<PathBuf>, TaskFailure> {
    Ok(MeasuresRuntime::discover_candidates()
        .map_err(failed)?
        .into_iter()
        .map(|candidate| candidate.path)
        .collect())
}

fn artifact(path: &str, kind: RunProductKind) -> SimobserveArtifact {
    let render_pending = kind == RunProductKind::File;
    SimobserveArtifact {
        kind: if render_pending {
            "figure"
        } else {
            "component-list"
        }
        .into(),
        label: path.into(),
        path: path.into(),
        exists: !render_pending,
        render_pending,
        resource_kind: kind,
    }
}

fn spectrum_record(
    component: &SkyComponent,
    zero_baseline: Option<f64>,
) -> Result<CasaValue, TaskFailure> {
    let mut spectrum = component.spectrum().to_record().map_err(failed)?;
    if let Some(flux) = zero_baseline {
        let mut flux_record = casa_core::RecordValue::new(Vec::new());
        flux_record.upsert(
            "value",
            Value::Scalar(casa_core::ScalarValue::Float64(flux)),
        );
        flux_record.upsert(
            "unit",
            Value::Scalar(casa_core::ScalarValue::String("Jy".into())),
        );
        spectrum.upsert("bl0flux", Value::Record(flux_record));
    }
    record(spectrum)
}

fn baseline_record(samples: &CalibratorBaselines, unit: &str) -> [(&'static str, CasaValue); 5] {
    let count = samples.lengths_m.len();
    let channels = samples.amplitudes_jy.len();
    let data = (0..count)
        .flat_map(|baseline| {
            samples
                .amplitudes_jy
                .iter()
                .map(move |row| CasaFloat(row[baseline]))
        })
        .collect();
    let azel = CasaValue::dict([
        ("m0", quantity(samples.azel.longitude_rad(), "rad")),
        ("m1", quantity(samples.azel.latitude_rad(), "rad")),
        ("refer", CasaValue::string("AZEL")),
        ("type", CasaValue::string("direction")),
    ]);
    let riseset = CasaValue::dict([
        (
            "rise",
            CasaValue::dict([("utc", epoch_record(samples.riseset.rise_mjd))]),
        ),
        (
            "set",
            CasaValue::dict([("utc", epoch_record(samples.riseset.set_mjd))]),
        ),
    ]);
    [
        (
            "baselines",
            CasaValue::float64_vector(samples.lengths_m.iter().copied()),
        ),
        (
            "amps",
            CasaValue::FloatArray {
                dtype: FloatDtype::Float64,
                shape: vec![channels, count],
                data,
            },
        ),
        (
            "blunit",
            CasaValue::string(if unit.is_empty() { "m" } else { unit }),
        ),
        ("azel", azel),
        ("riseset", riseset),
    ]
}

struct PlotRequest {
    object: String,
    standard: String,
    epoch: String,
    freqs_ghz: Vec<f64>,
    unit: String,
    flux_i: f64,
    show: bool,
    save: String,
}

fn make_plot(
    task: &TaskInvocation<'_>,
    samples: CalibratorBaselines,
    request: PlotRequest,
) -> Result<Plot, TaskFailure> {
    let klambda = request.unit == "klambda";
    let axis_unit = if klambda { "k$\\lambda$" } else { "m" }.to_string();
    let scale = if klambda {
        request.freqs_ghz[0] * 1e6 / 299_792_458.
    } else {
        1.
    };
    let antenna = samples
        .antenna_path
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or_default()
        .to_string();
    Ok(Plot {
        object: request.object,
        standard: request.standard,
        epoch: request.epoch,
        antennas: antenna,
        azimuth_deg: samples.azel.longitude_rad().to_degrees(),
        elevation_deg: samples.azel.latitude_rad().to_degrees(),
        frequencies_ghz: request.freqs_ghz,
        x: samples
            .lengths_m
            .into_iter()
            .map(|length| length * scale)
            .collect(),
        amplitudes_jy: samples.amplitudes_jy,
        axis_unit,
        symbol: task.required_text("symb")?,
        amplitude_from_zero: task.flag("include0amp")?,
        baseline_from_zero: task.flag("include0bl")?,
        zero_baseline_flux: task.flag("showbl0flux")?.then_some(request.flux_i),
        show: request.show,
        save: save_path(request.save),
    })
}

fn save_path(name: String) -> String {
    let path = Path::new(&name);
    if !name.is_empty() && path.extension().is_none() {
        return PathBuf::from(name)
            .with_extension("png")
            .to_string_lossy()
            .into_owned();
    }
    name
}

fn parse_epoch(value: &str) -> Result<MEpoch, TaskFailure> {
    let days = Unit::new("d").expect("days are a unit");
    let value = if value.is_empty() {
        0.
    } else {
        Quantity::read(value)
            .ok()
            .and_then(|q| q.get_value_in(&days).ok())
            .ok_or_else(|| TaskFailure::validation("m0").at("epoch"))?
    };
    Ok(MEpoch::from_mjd(value, EpochRef::UTC))
}

fn frequency_grid(task: &TaskInvocation<'_>) -> Result<Vec<f64>, TaskFailure> {
    let lower = task.required_text("minfreq")?;
    if lower.is_empty() {
        return Err(TaskFailure::validation("Error, minfreq is undefined").at("minfreq"));
    }
    let lower = frequency(&lower).map_err(|error| TaskFailure::execution(error).at("minfreq"))?;
    let upper = frequency(&task.required_text("maxfreq")?)
        .ok()
        .filter(|&hz| hz > 0.)
        .unwrap_or(lower);
    if lower <= 0. || !lower.is_finite() || !upper.is_finite() {
        return Err(TaskFailure::validation(
            "predictcomp requires positive finite frequencies",
        ));
    }
    if upper == lower {
        return Ok(vec![lower]);
    }
    let count = usize::try_from(task.integer("nfreqs")?.max(2))
        .map_err(|_| TaskFailure::validation("nfreqs is out of range"))?;
    let interval = (upper - lower) / (count - 1) as f64;
    let mut frequencies = Vec::new();
    frequencies.try_reserve_exact(count).map_err(failed)?;
    frequencies.extend((0..count - 1).map(|index| (index as f64 * interval) + lower));
    frequencies.push(upper);
    Ok(frequencies)
}

fn frequency(text: &str) -> Result<f64, String> {
    let quantity = Quantity::read(text).map_err(|error| error.to_string())?;
    // Quanta leaves a nonconforming quantity unchanged on conversion.
    Ok(quantity
        .get_value_in(&Unit::new("Hz").expect("Hz is a unit"))
        .unwrap_or_else(|_| quantity.value()))
}

fn record(value: casa_core::RecordValue) -> Result<CasaValue, TaskFailure> {
    casacore_value_to_casa(&Value::Record(value)).map_err(failed)
}

fn epoch_record(mjd: f64) -> CasaValue {
    CasaValue::dict([
        ("m0", quantity(mjd, "d")),
        ("refer", CasaValue::string("UTC")),
        ("type", CasaValue::string("epoch")),
    ])
}

fn quantity(value: f64, unit: &'static str) -> CasaValue {
    CasaValue::dict([
        ("value", CasaValue::float(value)),
        ("unit", CasaValue::string(unit)),
    ])
}

fn failed(error: impl std::fmt::Display) -> TaskFailure {
    TaskFailure::execution(error.to_string())
}

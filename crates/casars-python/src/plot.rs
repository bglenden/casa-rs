// SPDX-License-Identifier: LGPL-3.0-or-later
//! `casars.data.measurement_set_plot`: one msexplore preset plot of a
//! MeasurementSet, built by `casa-ms` with the GIL released.
//!
//! The arguments are checked in the order Python has always checked them:
//! the preset name, the selector names, then each request option. A plot
//! comes back as a dictionary of the document's panels and layers, each
//! layer's coordinates as owned `float64` NumPy arrays and its provenance as
//! one dictionary per point.

use casa_ms::plot_document::{PlotDocumentAxis, PlotDocumentLayer};
use casa_ms::{
    MeasurementSetPlot, MeasurementSetPlotRequest, MsPlotPreset,
    build_measurement_set_plot as build_plot,
};
use numpy::IntoPyArray;
use pyo3::exceptions::{PyKeyError, PyOverflowError, PyTypeError, PyValueError};
use pyo3::prelude::*;
use pyo3::types::{PyBool, PyDict, PyList};

use crate::parameters::reason_error;

/// Python's names for the presets, in the order an unknown name lists them.
const PRESET_NAMES: [(&str, MsPlotPreset); 26] = [
    ("uv_coverage", MsPlotPreset::UvCoverage),
    ("antenna_layout", MsPlotPreset::AntennaLayout),
    ("scan_timeline", MsPlotPreset::ScanTimeline),
    (
        "spectral_window_coverage",
        MsPlotPreset::SpectralWindowCoverage,
    ),
    ("phase_vs_time", MsPlotPreset::PhaseVsTime),
    (
        "amplitude_phase_vs_time_stacked",
        MsPlotPreset::AmplitudePhaseVsTimeStacked,
    ),
    ("weight_vs_time", MsPlotPreset::WeightVsTime),
    ("sigma_vs_time", MsPlotPreset::SigmaVsTime),
    ("flag_vs_time", MsPlotPreset::FlagVsTime),
    (
        "weight_spectrum_vs_time",
        MsPlotPreset::WeightSpectrumVsTime,
    ),
    ("sigma_spectrum_vs_time", MsPlotPreset::SigmaSpectrumVsTime),
    ("flag_row_vs_time", MsPlotPreset::FlagRowVsTime),
    ("elevation_vs_time", MsPlotPreset::ElevationVsTime),
    ("azimuth_vs_time", MsPlotPreset::AzimuthVsTime),
    ("hour_angle_vs_time", MsPlotPreset::HourAngleVsTime),
    (
        "parallactic_angle_vs_time",
        MsPlotPreset::ParallacticAngleVsTime,
    ),
    ("azimuth_vs_elevation", MsPlotPreset::AzimuthVsElevation),
    ("amplitude_vs_frequency", MsPlotPreset::AmplitudeVsFrequency),
    ("amplitude_vs_channel", MsPlotPreset::AmplitudeVsChannel),
    ("phase_vs_channel", MsPlotPreset::PhaseVsChannel),
    ("phase_vs_frequency", MsPlotPreset::PhaseVsFrequency),
    ("amplitude_vs_velocity", MsPlotPreset::AmplitudeVsVelocity),
    ("phase_vs_velocity", MsPlotPreset::PhaseVsVelocity),
    (
        "amplitude_vs_uv_distance",
        MsPlotPreset::AmplitudeVsUvDistance,
    ),
    ("amplitude_vs_time", MsPlotPreset::AmplitudeVsTime),
    ("real_vs_imaginary", MsPlotPreset::RealVsImaginary),
];

const SELECTORS: [&str; 12] = [
    "field",
    "spw",
    "timerange",
    "uvrange",
    "antenna",
    "scan",
    "correlation",
    "array",
    "observation",
    "intent",
    "feed",
    "msselect",
];

fn preset_name(preset: MsPlotPreset) -> &'static str {
    PRESET_NAMES
        .iter()
        .find_map(|(name, candidate)| (*candidate == preset).then_some(*name))
        .expect("every preset has a Python name")
}

/// The preset a name selects: case-insensitive, `-` for `_`, and
/// `flagrow_vs_time` for `flag_row_vs_time`.
fn parse_preset(preset: &Bound<'_, PyAny>) -> PyResult<MsPlotPreset> {
    let normalized = preset
        .call_method0("strip")?
        .call_method0("upper")?
        .call_method1("replace", ("-", "_"))?
        .extract::<String>()?;
    let normalized = if normalized == "FLAGROW_VS_TIME" {
        "FLAG_ROW_VS_TIME".to_string()
    } else {
        normalized
    };
    PRESET_NAMES
        .iter()
        .find_map(|(name, candidate)| {
            (name.to_ascii_uppercase() == normalized).then_some(*candidate)
        })
        .ok_or_else(|| {
            let allowed = PRESET_NAMES.map(|(name, _)| name).join(", ");
            match preset.repr() {
                Ok(repr) => PyValueError::new_err(format!(
                    "unknown MeasurementSet plot preset {repr}; expected one of: {allowed}"
                )),
                Err(error) => error,
            }
        })
}

fn optional_text(value: Option<Bound<'_, PyAny>>) -> PyResult<Option<String>> {
    value
        .filter(|value| !value.is_none())
        .map(|value| value.extract::<String>())
        .transpose()
}

fn flag(name: &str, value: &Bound<'_, PyAny>) -> PyResult<bool> {
    if let Ok(flag) = value.cast::<PyBool>() {
        return Ok(flag.is_true());
    }
    match value.extract::<i64>() {
        Ok(0) => Ok(false),
        Ok(1) => Ok(true),
        _ => Err(PyTypeError::new_err(format!("{name} must be a bool"))),
    }
}

fn count(value: &Bound<'_, PyAny>) -> PyResult<u64> {
    value.extract::<u64>().map_err(|error| {
        if error.is_instance_of::<PyOverflowError>(value.py()) {
            PyValueError::new_err(format!(
                "u64 requires 0 <= value < {}",
                u128::from(u64::MAX) + 1
            ))
        } else {
            error
        }
    })
}

fn platform_count(py: Python<'_>, name: &str, value: u64) -> PyResult<usize> {
    usize::try_from(value).map_err(|_| reason_error(py, format!("{name} exceeds platform usize")))
}

/// The selectors of a request, refusing names msexplore does not select by.
fn selectors<'py>(selection: &Bound<'py, PyAny>) -> PyResult<Bound<'py, PyDict>> {
    let py = selection.py();
    let selectors = if selection.is_truthy()? {
        py.get_type::<PyDict>()
            .call1((selection,))?
            .cast_into::<PyDict>()?
    } else {
        PyDict::new(py)
    };
    let mut unexpected = selectors
        .keys()
        .iter()
        .map(|key| key.extract::<String>())
        .collect::<PyResult<Vec<_>>>()?;
    unexpected.retain(|key| !SELECTORS.contains(&key.as_str()));
    if !unexpected.is_empty() {
        unexpected.sort();
        return Err(PyValueError::new_err(format!(
            "unknown MeasurementSet selectors: {}",
            unexpected.join(", ")
        )));
    }
    Ok(selectors)
}

/// The request that `casars.data.measurement_set_plot`'s arguments make.
///
/// `options` holds the keyword arguments other than the selection, by name.
fn plot_request(
    measurement_set: &Bound<'_, PyAny>,
    preset: &Bound<'_, PyAny>,
    selection: &Bound<'_, PyAny>,
    options: &Bound<'_, PyDict>,
) -> PyResult<MeasurementSetPlotRequest> {
    let py = options.py();
    let preset = parse_preset(preset)?;
    let selectors = selectors(selection)?;
    let dataset_path = py
        .import("os")?
        .call_method1("fspath", (measurement_set,))?
        .extract::<String>()?;
    let mut request = MeasurementSetPlotRequest::new(dataset_path, preset);
    for (name, slot) in [
        ("field", &mut request.field),
        ("spw", &mut request.spw),
        ("timerange", &mut request.timerange),
        ("uvrange", &mut request.uvrange),
        ("antenna", &mut request.antenna),
        ("scan", &mut request.scan),
        ("correlation", &mut request.correlation),
        ("array", &mut request.array),
        ("observation", &mut request.observation),
        ("intent", &mut request.intent),
        ("feed", &mut request.feed),
        ("msselect", &mut request.msselect),
    ] {
        *slot = optional_text(selectors.get_item(name)?)?;
    }
    let option = |name: &str| {
        options
            .get_item(name)?
            .ok_or_else(|| PyKeyError::new_err(name.to_string()))
    };
    request.data_column = option("data_column")?.extract::<String>()?;
    request.color_by = optional_text(Some(option("color_by")?))?;
    let avgchannel = Some(option("avgchannel")?)
        .filter(|value| !value.is_none())
        .map(|value| count(&value))
        .transpose()?;
    request.avgtime = Some(option("avgtime")?)
        .filter(|value| !value.is_none())
        .map(|value| value.extract::<f64>())
        .transpose()?;
    for (name, slot) in [
        ("avgscan", &mut request.avgscan),
        ("avgfield", &mut request.avgfield),
        ("avgbaseline", &mut request.avgbaseline),
        ("avgantenna", &mut request.avgantenna),
        ("avgspw", &mut request.avgspw),
        ("scalar", &mut request.scalar),
    ] {
        *slot = flag(name, &option(name)?)?;
    }
    request.iteraxis = optional_text(Some(option("iteraxis")?))?;
    let max_plot_points = count(&option("max_plot_points")?)?;
    request.avgchannel = avgchannel
        .map(|value| platform_count(py, "avgchannel", value))
        .transpose()?;
    request.max_plot_points = usize::try_from(max_plot_points).unwrap_or(usize::MAX);
    Ok(request)
}

fn axis_dict<'py>(py: Python<'py>, axis: PlotDocumentAxis) -> PyResult<Bound<'py, PyDict>> {
    let dict = PyDict::new(py);
    dict.set_item("id", axis.id)?;
    dict.set_item("label", axis.label)?;
    dict.set_item("unit", axis.unit)?;
    dict.set_item("lower", axis.lower)?;
    dict.set_item("upper", axis.upper)?;
    Ok(dict)
}

fn layer_dict<'py>(py: Python<'py>, layer: PlotDocumentLayer) -> PyResult<Bound<'py, PyDict>> {
    let dict = PyDict::new(py);
    dict.set_item("title", layer.title)?;
    dict.set_item("color_group", layer.color_group)?;
    dict.set_item("y_axis_id", layer.y_axis_id)?;
    dict.set_item("x", layer.x_values.into_pyarray(py))?;
    dict.set_item("y", layer.y_values.into_pyarray(py))?;
    let provenance = PyList::empty(py);
    for point in layer.provenance {
        let entry = PyDict::new(py);
        entry.set_item("row", point.row)?;
        entry.set_item("corr", point.corr)?;
        entry.set_item("chan_start", point.chan_start)?;
        entry.set_item("chan_end", point.chan_end)?;
        provenance.append(entry)?;
    }
    dict.set_item("provenance", provenance)?;
    Ok(dict)
}

fn panel_dict<'py>(
    py: Python<'py>,
    id: String,
    title: String,
    axes: Vec<PlotDocumentAxis>,
    layers: Vec<PlotDocumentLayer>,
) -> PyResult<Bound<'py, PyDict>> {
    let dict = PyDict::new(py);
    dict.set_item("id", id)?;
    dict.set_item("title", title)?;
    let axes = axes
        .into_iter()
        .map(|axis| axis_dict(py, axis))
        .collect::<PyResult<Vec<_>>>()?;
    dict.set_item("axes", axes)?;
    let layers = layers
        .into_iter()
        .map(|layer| layer_dict(py, layer))
        .collect::<PyResult<Vec<_>>>()?;
    dict.set_item("layers", layers)?;
    Ok(dict)
}

fn plot_dict(py: Python<'_>, plot: MeasurementSetPlot) -> PyResult<Bound<'_, PyDict>> {
    let document = plot.document;
    let panels = if document.panels.is_empty() {
        vec![panel_dict(
            py,
            document.id,
            document.title,
            document.axes,
            document.layers,
        )?]
    } else {
        document
            .panels
            .into_iter()
            .map(|panel| panel_dict(py, panel.id, panel.title, panel.axes, panel.layers))
            .collect::<PyResult<Vec<_>>>()?
    };
    let dict = PyDict::new(py);
    dict.set_item("title", plot.title)?;
    dict.set_item("summary", plot.summary)?;
    dict.set_item("header_lines", document.header_lines)?;
    dict.set_item("show_legend", document.show_legend)?;
    dict.set_item("panels", panels)?;
    dict.set_item("measurement_set", plot.dataset_path)?;
    dict.set_item("preset", preset_name(plot.preset))?;
    dict.set_item("selection_summary", plot.selection_summary)?;
    Ok(dict)
}

/// Build one MeasurementSet preset plot, releasing the GIL while it reads.
#[pyfunction]
pub(crate) fn build_measurement_set_plot<'py>(
    py: Python<'py>,
    measurement_set: &Bound<'py, PyAny>,
    preset: &Bound<'py, PyAny>,
    selection: &Bound<'py, PyAny>,
    options: &Bound<'py, PyDict>,
) -> PyResult<Bound<'py, PyDict>> {
    let request = plot_request(measurement_set, preset, selection, options)?;
    let plot = py
        .detach(|| build_plot(&request))
        .map_err(|error| reason_error(py, error.to_string()))?;
    plot_dict(py, plot)
}

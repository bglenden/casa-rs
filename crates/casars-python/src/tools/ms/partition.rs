// SPDX-License-Identifier: LGPL-3.0-or-later

use std::path::{Path, PathBuf};

use casa_ms::{
    schema::main_table::OptionalMainColumn,
    selection::MsSelection,
    tool_history::append_tool_history,
    tool_sort::check_named_sort_output,
    transform::{
        LegacyMsTool, LegacyTimeOptions, MsTransformError, MsTransformRequest,
        MsTransformTaskOptions, MsTransformTiling, TimeAverageOptions, TransformDataColumn,
        mstransform_task,
    },
};
use casa_tables::publication::{check_write_access, lock_table_tree_for_replacement};
use casa_tables::{Table, TableOptions};
use pyo3::prelude::*;

use crate::convert::{Variant, quantity_value, selector::listing_selector};

use super::{PyMsTool, runtime_error, selection_indices::selection_text, sort::SortedDestination};

#[pymethods]
impl PyMsTool {
    /// Materialize selected rows while retaining the input subtable IDs.
    #[allow(clippy::too_many_arguments)]
    #[pyo3(signature = (outputms = String::new(), field = None, spw = None, baseline = None, timebin = None, time = String::new(), scan = None, uvrange = None, taql = String::new(), whichcol = "DATA".to_owned(), tileshape = None, subarray = None, combine = String::new(), intent = String::new(), obs = String::new()), text_signature = "($self, outputms='', field=[], spw=[], baseline=[], timebin=[], time='', scan=[], uvrange=[], taql='', whichcol='DATA', tileshape=[], subarray=[], combine='', intent='', obs='')")]
    fn partition(
        &self,
        py: Python<'_>,
        #[pyo3(from_py_with = output_argument)] outputms: String,
        field: Option<&Bound<'_, PyAny>>,
        spw: Option<&Bound<'_, PyAny>>,
        baseline: Option<&Bound<'_, PyAny>>,
        timebin: Option<Variant>,
        #[pyo3(from_py_with = time_argument)] time: String,
        scan: Option<&Bound<'_, PyAny>>,
        uvrange: Option<&Bound<'_, PyAny>>,
        #[pyo3(from_py_with = taql_argument)] taql: String,
        #[pyo3(from_py_with = whichcol_argument)] whichcol: String,
        tileshape: Option<Variant>,
        subarray: Option<&Bound<'_, PyAny>>,
        #[pyo3(from_py_with = combine_argument)] combine: String,
        #[pyo3(from_py_with = intent_argument)] intent: String,
        #[pyo3(from_py_with = obs_argument)] obs: String,
    ) -> PyResult<bool> {
        let spw = all_if_minus_one(spw)?.unwrap_or_else(|| "*".into());
        let selection = MsSelection {
            field: all_if_minus_one(field)?,
            antenna: optional_expression(baseline)?,
            timerange: (!time.is_empty()).then_some(time),
            scan: optional_expression(scan)?,
            uvrange: optional_expression(uvrange)?,
            msselect: (!taql.is_empty()).then_some(taql),
            array: optional_expression(subarray)?,
            intent: (!intent.is_empty()).then_some(intent),
            observation: (!obs.is_empty()).then_some(obs),
            ..MsSelection::default()
        };
        let columns = visibility_columns(&whichcol)?;
        let options = MsTransformTaskOptions {
            visibility_columns: columns,
            preserve_ids: true,
            legacy_tool: Some(LegacyMsTool::Partition(LegacyTimeOptions {
                combine_observations: combine.to_ascii_lowercase().contains("obs"),
            })),
            time_averaging: time_average(timebin.as_ref(), &combine)?,
            tiling: tile_layout(tileshape.as_ref())?,
            ..MsTransformTaskOptions::default()
        };
        self.write_subms(py, outputms, spw, selection, options, None)
    }
}

impl PyMsTool {
    pub(super) fn write_subms(
        &self,
        py: Python<'_>,
        outputms: String,
        spw: String,
        selection: MsSelection,
        options: MsTransformTaskOptions,
        history: Option<String>,
    ) -> PyResult<bool> {
        let operation = if history.is_some() {
            "split"
        } else {
            "partition"
        };
        py.detach(|| {
            let attached = self.attached.lock().map_err(runtime_error)?;
            let Some(ms) = attached.as_ref() else {
                return Ok(false);
            };
            if operation == "partition" && spw.contains(':') {
                return Ok(false);
            }
            let link = ms.working_metadata_link();
            let source = link.snapshot(None).map_err(runtime_error)?;
            let input = source
                .ms
                .path()
                .ok_or_else(|| runtime_error(format!("{operation} input has no path")))?;
            let output = (!outputms.is_empty())
                .then(|| named_path(Path::new(&outputms), &ms.path, operation))
                .transpose()?;
            let destination = output
                .as_deref()
                .map(|output| {
                    check_named_sort_output(&source.ms, output).map_err(runtime_error)?;
                    SortedDestination::inspect(output, operation)
                })
                .transpose()?;
            let locks = match output.as_deref().zip(destination.as_ref()) {
                Some((output, destination)) if destination.is_table() => {
                    check_write_access(output).map_err(runtime_error)?;
                    lock_table_tree_for_replacement(output).map_err(runtime_error)?
                }
                _ => Vec::new(),
            };
            let scratch = output
                .as_deref()
                .and_then(Path::parent)
                .map_or_else(tempfile::tempdir, tempfile::tempdir_in)
                .map_err(runtime_error)?;
            let staged = scratch.path().join(format!("{operation}.ms"));
            let request = MsTransformRequest {
                input_ms: input.to_path_buf(),
                output_ms: staged.clone(),
                spw,
                width: 1,
                data_column: TransformDataColumn::All,
                selection,
                keep_flags: true,
            };
            match mstransform_task(&request, &options) {
                Err(MsTransformError::EmptySelection { .. }) => {
                    if let Some(field) = &request.selection.field {
                        source
                            .ms
                            .resolve_field_selector(field)
                            .map_err(runtime_error)?;
                    }
                    return Ok(false);
                }
                Err(MsTransformError::ChannelWidthCount) if operation == "split" => {
                    return Ok(false);
                }
                result => result.map_err(runtime_error)?,
            };
            if let Some(parameters) = history.as_ref()
                && output.is_some()
            {
                let table = Table::open(TableOptions::new(&staged)).map_err(runtime_error)?;
                let message = format!("{outputms} split from {}", ms.path.display());
                append_tool_history(
                    &table,
                    true,
                    &[message],
                    parameters,
                    "ms::split()",
                    "ms",
                    &mut Default::default(),
                )
                .map_err(runtime_error)?;
            }
            link.verify_directory().map_err(runtime_error)?;
            if !locks
                .iter()
                .all(|lock| lock.matches_table_path(lock.path()))
            {
                return Err(runtime_error(format!(
                    "{operation} output changed during staging"
                )));
            }
            if let Some((destination, output)) = destination.zip(output) {
                destination.publish(&staged, &output, operation)?;
            }
            Ok(true)
        })
    }
}

fn named_path(output: &Path, input: &Path, operation: &str) -> PyResult<PathBuf> {
    let parent = output
        .parent()
        .filter(|path| !path.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."))
        .canonicalize()
        .map_err(runtime_error)?;
    let output =
        parent.join(output.file_name().ok_or_else(|| {
            runtime_error(format!("{operation} output must name a MeasurementSet"))
        })?);
    if input.starts_with(&output) {
        return Err(runtime_error(format!(
            "{operation} output would replace its input"
        )));
    }
    Ok(output)
}

pub(super) fn output_argument(value: &Bound<'_, PyAny>) -> PyResult<String> {
    selection_text(value, "outputms")
}

pub(super) fn time_argument(value: &Bound<'_, PyAny>) -> PyResult<String> {
    selection_text(value, "time")
}

pub(super) fn taql_argument(value: &Bound<'_, PyAny>) -> PyResult<String> {
    selection_text(value, "taql")
}

pub(super) fn whichcol_argument(value: &Bound<'_, PyAny>) -> PyResult<String> {
    selection_text(value, "whichcol")
}

pub(super) fn combine_argument(value: &Bound<'_, PyAny>) -> PyResult<String> {
    selection_text(value, "combine")
}

pub(super) fn intent_argument(value: &Bound<'_, PyAny>) -> PyResult<String> {
    selection_text(value, "intent")
}

pub(super) fn obs_argument(value: &Bound<'_, PyAny>) -> PyResult<String> {
    selection_text(value, "obs")
}

pub(super) fn optional_expression(value: Option<&Bound<'_, PyAny>>) -> PyResult<Option<String>> {
    let text = listing_selector(value)?;
    Ok((!text.is_empty()).then_some(text))
}

pub(super) fn all_if_minus_one(value: Option<&Bound<'_, PyAny>>) -> PyResult<Option<String>> {
    Ok(optional_expression(value)?.filter(|text| text != "-1"))
}

pub(super) fn visibility_columns(whichcol: &str) -> PyResult<Option<Vec<OptionalMainColumn>>> {
    use OptionalMainColumn::{CorrectedData, Data, FloatData, LagData, ModelData};
    let text = whichcol.to_ascii_uppercase();
    if text.contains("ALL") || text.contains("ANY") {
        return Ok(None);
    }
    let columns = text
        .split([' ', ','])
        .filter_map(|token| match token {
            "DATA" | "OBSERVED" => Some(Data),
            "MODEL" | "MODEL_DATA" => Some(ModelData),
            "CORRECTED" | "CORRECTED_DATA" => Some(CorrectedData),
            "FLOAT" | "FLOAT_DATA" => Some(FloatData),
            "LAG" | "LAG_DATA" => Some(LagData),
            _ => None,
        })
        .collect::<Vec<_>>();
    if columns.is_empty() {
        Err(runtime_error("did not find and select any data columns"))
    } else {
        Ok(Some(columns))
    }
}

pub(super) fn time_average(
    timebin: Option<&Variant>,
    combine: &str,
) -> PyResult<Option<TimeAverageOptions>> {
    let seconds = timebin
        .map(|value| quantity_value(value, "s"))
        .transpose()
        .map_err(runtime_error)?
        .unwrap_or(0.0);
    if !seconds.is_finite() {
        return Err(runtime_error("partition timebin must be finite"));
    }
    let combine = combine.to_ascii_lowercase();
    Ok((seconds > 0.0).then_some(TimeAverageOptions {
        interval_seconds: seconds,
        span_scans: combine.contains("scan"),
        span_states: combine.contains("state"),
        span_fields: false,
        max_uvw_distance: 0.0,
    }))
}

pub(super) fn tile_layout(tileshape: Option<&Variant>) -> PyResult<Option<MsTransformTiling>> {
    let Some(Variant::Numbers(values) | Variant::Array { values, .. }) = tileshape else {
        return Ok(None);
    };
    let [correlations, channels, rows] = values.as_slice() else {
        return Ok(None);
    };
    let dimensions = [*correlations, *channels, *rows];
    if !dimensions
        .iter()
        .all(|value| (0.0..=f64::from(i32::MAX)).contains(value))
    {
        return Err(runtime_error(
            "partition tile dimensions must be nonnegative 32-bit integers",
        ));
    }
    Ok(Some(MsTransformTiling::Fixed(
        dimensions.map(|value| value as usize),
    )))
}

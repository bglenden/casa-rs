// SPDX-License-Identifier: LGPL-3.0-or-later

use super::*;

pub(super) struct FlagEditPreviewContext<'a> {
    pub(super) ms: &'a MeasurementSet,
    pub(super) plot_title: &'a str,
    pub(super) plot_index: Option<usize>,
    pub(super) panel_key: Option<String>,
    pub(super) panel_label: Option<String>,
    pub(super) x_axis: MsAxis,
    pub(super) y_axis: MsAxis,
}

/// Inclusive rectangular region used for staged flag editing.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct MsFlagRegion {
    /// Inclusive minimum X value.
    pub x_min: f64,
    /// Inclusive maximum X value.
    pub x_max: f64,
    /// Inclusive minimum Y value.
    pub y_min: f64,
    /// Inclusive maximum Y value.
    pub y_max: f64,
}

/// Supported flag-edit actions.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum MsFlagAction {
    /// Mark matching samples as flagged.
    Flag,
    /// Clear flags from matching samples.
    Unflag,
}

/// One plotted visibility sample selected by a staged flag edit.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct MsFlagSampleEdit {
    /// MAIN row index.
    pub row: usize,
    /// Correlation slot within the FLAG cell.
    pub corr: usize,
    /// Channel slot within the FLAG cell.
    pub chan: usize,
    /// Flag value before applying the staged edit.
    pub old_flag: bool,
    /// Flag value after applying the staged edit.
    pub new_flag: bool,
}

/// One MAIN-row FLAG_ROW transition produced by a staged edit.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct MsFlagRowEdit {
    /// MAIN row index.
    pub row: usize,
    /// FLAG_ROW value before applying the staged edit.
    pub old_flag_row: bool,
    /// FLAG_ROW value after applying the staged edit.
    pub new_flag_row: bool,
    /// Number of per-sample flag transitions on this row.
    pub changed_samples: usize,
}

/// Preview of a staged `msexplore` flag edit before optional writeback.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct MsFlagEditPreview {
    /// Plot title associated with the staged edit.
    pub plot_title: String,
    /// Optional child plot index targeted within a multi-plot page.
    pub plot_index: Option<usize>,
    /// Optional iterated-panel key targeted by this preview.
    pub panel_key: Option<String>,
    /// Optional human-readable panel label targeted by this preview.
    pub panel_label: Option<String>,
    /// X axis used for region matching.
    pub x_axis: MsAxis,
    /// Y axis used for region matching.
    pub y_axis: MsAxis,
    /// Inclusive numeric region used to select plotted samples.
    pub region: MsFlagRegion,
    /// Whether the edit flags or unflags the selected samples.
    pub action: MsFlagAction,
    /// Extend the edit across all correlations on matching rows/channels.
    pub extcorr: bool,
    /// Extend the edit across all channels on matching rows/correlations.
    pub extchannel: bool,
    /// Number of plotted points that intersect the staged region.
    pub matched_points: usize,
    /// Number of MAIN rows affected by the staged edit.
    pub affected_rows: usize,
    /// Number of unique `(row, corr, chan)` samples affected by the staged edit.
    pub affected_samples: usize,
    /// Exact per-sample flag transitions.
    pub sample_edits: Vec<MsFlagSampleEdit>,
    /// Exact per-row FLAG_ROW transitions.
    pub row_edits: Vec<MsFlagRowEdit>,
}

/// Open a MeasurementSet path from a full `msexplore` request and build the
/// requested plot payload.
pub fn build_msexplore_payload_from_spec(spec: &MsExploreSpec) -> Result<MsPlotPayload, String> {
    let ms = MeasurementSet::open(&spec.ms_path).map_err(|error| {
        if spec.ms_path.is_dir() {
            format!(
                "msexplore currently supports MeasurementSets only; failed to open {} as an MS: {error}",
                spec.ms_path.display()
            )
        } else {
            format!("open MeasurementSet {}: {error}", spec.ms_path.display())
        }
    })?;
    build_msexplore_payload(&ms, spec)
}

/// Preview a staged flag edit against one standalone `msexplore` scatter plot.
///
/// The returned preview resolves the plot region to exact `(row, corr, chan)`
/// samples before any writeback occurs.
pub fn preview_msexplore_flag_edit(
    ms: &MeasurementSet,
    selection: &MsSelection,
    spec: &MsPlotSpec,
) -> Result<MsFlagEditPreview, String> {
    spec.validate()?;
    let flag_edit = spec
        .flag_edit
        .as_ref()
        .ok_or_else(|| "msexplore flag-edit preview requires MsPlotSpec.flag_edit".to_string())?;
    let mut point_budget = PointBudget::unlimited();
    let payload =
        build_msexplore_plot_payload_validated(ms, selection, spec, &mut point_budget, None)?;
    preview_msexplore_flag_edit_from_payload(ms, payload, flag_edit)
}

/// Preview a staged flag edit against a full `msexplore` request, including
/// stacked presets and generic multi-plot page payloads.
pub fn preview_msexplore_flag_edit_for_request(
    ms: &MeasurementSet,
    spec: &MsExploreSpec,
    flag_edit: &MsFlagEditSpec,
) -> Result<MsFlagEditPreview, String> {
    spec.validate()?;
    validate_flag_edit_request(flag_edit)?;
    let payload = build_msexplore_payload_internal(ms, spec, false, None)?;
    preview_msexplore_flag_edit_from_payload(ms, payload, flag_edit)
}

/// Apply a staged flag edit resolved from a full `msexplore` request and return
/// the exact preview that was committed.
pub fn apply_msexplore_flag_edit_for_request(
    ms: &mut MeasurementSet,
    spec: &MsExploreSpec,
    flag_edit: &MsFlagEditSpec,
) -> Result<MsFlagEditPreview, String> {
    let preview = preview_msexplore_flag_edit_for_request(ms, spec, flag_edit)?;
    apply_msexplore_flag_edit_preview(ms, preview)
}

pub(super) fn preview_msexplore_flag_edit_from_payload(
    ms: &MeasurementSet,
    payload: MsPlotPayload,
    flag_edit: &MsFlagEditSpec,
) -> Result<MsFlagEditPreview, String> {
    match payload {
        MsPlotPayload::Scatter(payload) => {
            if flag_edit.panel_key.is_some() {
                return Err(
                    "msexplore staged flag editing only accepts panel_key for iterated scatter grids"
                        .to_string(),
                );
            }
            if flag_edit.plot_index.is_some() {
                return Err(
                    "msexplore staged flag editing only accepts plot_index for multi-plot page payloads"
                        .to_string(),
                );
            }
            build_flag_edit_preview_from_series(
                &FlagEditPreviewContext {
                    ms,
                    plot_title: &payload.title,
                    plot_index: None,
                    panel_key: None,
                    panel_label: None,
                    x_axis: payload.x_axis,
                    y_axis: payload.y_axis,
                },
                &payload.series,
                flag_edit,
            )
        }
        MsPlotPayload::ListObs(_) => Err(
            "msexplore staged flag editing currently supports raw-visibility scatter plots only"
                .to_string(),
        ),
        MsPlotPayload::ScatterGrid(payload) => {
            if flag_edit.plot_index.is_some() {
                return Err(
                    "msexplore staged flag editing only accepts plot_index for multi-plot page payloads"
                        .to_string(),
                );
            }
            let panel = match flag_edit.panel_key.as_deref() {
                Some(key) => payload
                    .panels
                    .iter()
                    .find(|panel| panel.key == key)
                    .ok_or_else(|| {
                        format!(
                            "msexplore flag-edit panel_key {:?} was not found in iterated payload; available panels: {}",
                            key,
                            payload
                                .panels
                                .iter()
                                .map(|panel| panel.key.as_str())
                                .collect::<Vec<_>>()
                                .join(", ")
                        )
                    })?,
                None if payload.panels.len() == 1 => &payload.panels[0],
                None => {
                    return Err(format!(
                        "msexplore staged flag editing requires panel_key for iterated plots with multiple panels; available panels: {}",
                        payload
                            .panels
                            .iter()
                            .map(|panel| panel.key.as_str())
                            .collect::<Vec<_>>()
                            .join(", ")
                    ));
                }
            };
            build_flag_edit_preview_from_series(
                &FlagEditPreviewContext {
                    ms,
                    plot_title: &payload.title,
                    plot_index: None,
                    panel_key: Some(panel.key.clone()),
                    panel_label: Some(panel.label.clone()),
                    x_axis: payload.x_axis,
                    y_axis: payload.y_axis,
                },
                &panel.series,
                flag_edit,
            )
        }
        MsPlotPayload::ScatterPage(payload) => {
            if flag_edit.panel_key.is_some() {
                return Err(
                    "msexplore staged flag editing only accepts panel_key for iterated scatter grids"
                        .to_string(),
                );
            }
            let item = match flag_edit.plot_index {
                Some(plot_index) => payload
                    .items
                    .iter()
                    .find(|item| item.plotindex == plot_index)
                    .ok_or_else(|| {
                        format!(
                            "msexplore flag-edit plot_index {} was not found in page payload; available plot indices: {}",
                            plot_index,
                            payload
                                .items
                                .iter()
                                .map(|item| item.plotindex.to_string())
                                .collect::<Vec<_>>()
                                .join(", ")
                        )
                    })?,
                None if payload.items.len() == 1 => &payload.items[0],
                None => {
                    return Err(format!(
                        "msexplore staged flag editing requires plot_index for page payloads with multiple plots; available plot indices: {}",
                        payload
                            .items
                            .iter()
                            .map(|item| item.plotindex.to_string())
                            .collect::<Vec<_>>()
                            .join(", ")
                    ));
                }
            };
            if item.plot.secondary_y_axis.is_some() {
                return Err(
                    "msexplore staged flag editing currently supports single-y child plots only"
                        .to_string(),
                );
            }
            build_flag_edit_preview_from_series(
                &FlagEditPreviewContext {
                    ms,
                    plot_title: &item.plot.title,
                    plot_index: Some(item.plotindex),
                    panel_key: None,
                    panel_label: None,
                    x_axis: item.plot.x_axis,
                    y_axis: item.plot.y_axis,
                },
                &item.plot.series,
                flag_edit,
            )
        }
    }
}

pub(super) fn validate_flag_edit_request(flag_edit: &MsFlagEditSpec) -> Result<(), String> {
    if !flag_edit.region.x_min.is_finite()
        || !flag_edit.region.x_max.is_finite()
        || !flag_edit.region.y_min.is_finite()
        || !flag_edit.region.y_max.is_finite()
    {
        return Err("msexplore staged flag editing requires finite region bounds".to_string());
    }
    if flag_edit.plot_index.is_some() && flag_edit.panel_key.is_some() {
        return Err(
            "msexplore staged flag editing accepts either plot_index or panel_key, not both"
                .to_string(),
        );
    }
    Ok(())
}

/// Apply a staged flag edit to MAIN `FLAG` / `FLAG_ROW` and return the exact
/// preview that was committed.
pub fn apply_msexplore_flag_edit(
    ms: &mut MeasurementSet,
    selection: &MsSelection,
    spec: &MsPlotSpec,
) -> Result<MsFlagEditPreview, String> {
    let preview = preview_msexplore_flag_edit(ms, selection, spec)?;
    apply_msexplore_flag_edit_preview(ms, preview)
}

pub(super) fn apply_msexplore_flag_edit_preview(
    ms: &mut MeasurementSet,
    preview: MsFlagEditPreview,
) -> Result<MsFlagEditPreview, String> {
    let mut row_updates = std::collections::BTreeMap::<usize, (ndarray::Array2<bool>, bool)>::new();
    for row_edit in &preview.row_edits {
        row_updates.insert(
            row_edit.row,
            (clone_flag_matrix(ms, row_edit.row)?, row_edit.new_flag_row),
        );
    }
    for sample in &preview.sample_edits {
        let (matrix, _) = row_updates
            .get_mut(&sample.row)
            .ok_or_else(|| format!("flag edit lost planned row {}", sample.row))?;
        matrix[(sample.corr, sample.chan)] = sample.new_flag;
    }
    for (row, (matrix, flag_row)) in row_updates {
        ms.main_table_mut()
            .cell_accessor_mut(row, "FLAG")
            .map_err(|error| error.to_string())?
            .set(Value::Array(ArrayValue::Bool(matrix.into_dyn())))
            .map_err(|error| error.to_string())?;
        ms.main_table_mut()
            .cell_accessor_mut(row, "FLAG_ROW")
            .map_err(|error| error.to_string())?
            .set(Value::Scalar(ScalarValue::Bool(flag_row)))
            .map_err(|error| error.to_string())?;
    }
    Ok(preview)
}

pub(super) fn build_flag_edit_preview_from_series(
    context: &FlagEditPreviewContext<'_>,
    series: &[MsScatterSeries],
    flag_edit: &MsFlagEditSpec,
) -> Result<MsFlagEditPreview, String> {
    let mut matched_points = 0usize;
    let mut selected_samples = std::collections::BTreeSet::<(usize, usize, usize)>::new();
    let mut row_shapes = std::collections::BTreeMap::<usize, (usize, usize)>::new();
    let region = normalized_flag_region(&flag_edit.region);

    for series in series {
        if series.points.len() != series.provenance.len() {
            return Err(format!(
                "msexplore scatter series {:?} has {} points but {} provenance entries",
                series.label,
                series.points.len(),
                series.provenance.len()
            ));
        }
        for ((x, y), point_ref) in series.points.iter().zip(series.provenance.iter()) {
            if !flag_region_contains(&region, *x, *y) {
                continue;
            }
            matched_points += 1;
            let (corr_count, chan_count) = *row_shapes
                .entry(point_ref.row)
                .or_insert(clone_flag_matrix(context.ms, point_ref.row)?.dim());
            let corr_start = if flag_edit.extcorr { 0 } else { point_ref.corr };
            let corr_end = if flag_edit.extcorr {
                corr_count
            } else {
                point_ref.corr + 1
            };
            let chan_start = if flag_edit.extchannel {
                0
            } else {
                point_ref.chan_start
            };
            let chan_end = if flag_edit.extchannel {
                chan_count
            } else {
                point_ref.chan_end
            };
            for corr in corr_start..corr_end {
                for chan in chan_start..chan_end {
                    selected_samples.insert((point_ref.row, corr, chan));
                }
            }
        }
    }

    let mut sample_edits = Vec::<MsFlagSampleEdit>::new();
    let mut row_edits = Vec::<MsFlagRowEdit>::new();
    let mut samples_by_row = std::collections::BTreeMap::<usize, Vec<(usize, usize)>>::new();
    for (row, corr, chan) in selected_samples {
        samples_by_row.entry(row).or_default().push((corr, chan));
    }

    let flag_row = context.ms.flag_row_column();
    for (row, samples) in samples_by_row {
        let old_flag_row = flag_row.get(row).map_err(|error| error.to_string())?;
        let old_matrix = clone_flag_matrix(context.ms, row)?;
        let mut new_matrix = old_matrix.clone();
        let mut changed_samples = 0usize;
        for (corr, chan) in samples {
            let old_flag = old_matrix[(corr, chan)];
            let new_flag = match flag_edit.action {
                MsFlagAction::Flag => true,
                MsFlagAction::Unflag => false,
            };
            if old_flag != new_flag {
                new_matrix[(corr, chan)] = new_flag;
                sample_edits.push(MsFlagSampleEdit {
                    row,
                    corr,
                    chan,
                    old_flag,
                    new_flag,
                });
                changed_samples += 1;
            }
        }
        let new_flag_row = new_matrix.iter().all(|value| *value);
        if changed_samples > 0 || old_flag_row != new_flag_row {
            row_edits.push(MsFlagRowEdit {
                row,
                old_flag_row,
                new_flag_row,
                changed_samples,
            });
        }
    }

    Ok(MsFlagEditPreview {
        plot_title: context.plot_title.to_string(),
        plot_index: context.plot_index,
        panel_key: context.panel_key.clone(),
        panel_label: context.panel_label.clone(),
        x_axis: context.x_axis,
        y_axis: context.y_axis,
        region,
        action: flag_edit.action,
        extcorr: flag_edit.extcorr,
        extchannel: flag_edit.extchannel,
        matched_points,
        affected_rows: row_edits.len(),
        affected_samples: sample_edits.len(),
        sample_edits,
        row_edits,
    })
}

pub(super) fn normalized_flag_region(region: &MsFlagRegion) -> MsFlagRegion {
    MsFlagRegion {
        x_min: region.x_min.min(region.x_max),
        x_max: region.x_min.max(region.x_max),
        y_min: region.y_min.min(region.y_max),
        y_max: region.y_min.max(region.y_max),
    }
}

pub(super) fn flag_region_contains(region: &MsFlagRegion, x: f64, y: f64) -> bool {
    x >= region.x_min && x <= region.x_max && y >= region.y_min && y <= region.y_max
}

/// Copies one MAIN-table FLAG cell in correlation-by-channel order. Invalid rows or incompatible
/// cell data return a diagnostic.
pub fn clone_flag_matrix(ms: &MeasurementSet, row: usize) -> Result<ndarray::Array2<bool>, String> {
    match ms
        .flag_column()
        .get(row)
        .map_err(|error| error.to_string())?
    {
        ArrayValue::Bool(values) => values
            .view()
            .into_dimensionality::<Ix2>()
            .map(|matrix| matrix.to_owned())
            .map_err(|_| {
                "msexplore expects FLAG cells with shape [num_corr, num_chan]".to_string()
            }),
        other => Err(format!(
            "msexplore requires BOOL flag cells, found {:?}",
            other.primitive_type()
        )),
    }
}

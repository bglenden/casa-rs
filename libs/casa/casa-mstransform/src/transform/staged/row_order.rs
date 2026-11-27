// SPDX-License-Identifier: LGPL-3.0-or-later

use super::*;

pub(super) struct RowOrderInput<'a> {
    pub(super) input: &'a MeasurementSet,
    pub(super) rows: Vec<ResolvedMsSelectionRow>,
    pub(super) rows_by_index: Vec<usize>,
    pub(super) descriptions: Vec<i32>,
    pub(super) ddid_to_spw: &'a BTreeMap<i32, i32>,
    pub(super) channel_bins: &'a BTreeMap<i32, Vec<Vec<usize>>>,
    pub(super) channel_selection: &'a BTreeMap<i32, TransformChannelSelection>,
    pub(super) correlations: Option<&'a CorrelationSelection>,
    pub(super) budget: MsSelectionIoBudget,
    pub(super) order: selection_metadata::MainRowOrder,
    pub(super) legacy_direct_band: bool,
}

pub(super) fn order_rows(
    request: &MsTransformRequest,
    options: &MsTransformTaskOptions,
    input: RowOrderInput<'_>,
    operation: &mut Option<SpectralOperation<'_>>,
) -> Result<
    (
        Option<Vec<time_average::TimeGroup>>,
        selection_metadata::OrderedMainRows,
    ),
    MsTransformError,
> {
    let RowOrderInput {
        input,
        rows,
        rows_by_index: selected_rows,
        descriptions: selected_ddids,
        ddid_to_spw,
        channel_bins,
        channel_selection,
        correlations,
        budget,
        order,
        legacy_direct_band,
    } = input;
    let selection_error = |error| row_selection_error(request, error);
    let legacy_time = match options.legacy_tool {
        Some(LegacyMsTool::Partition(time) | LegacyMsTool::Split(time)) => Some(time),
        _ => None,
    };
    let legacy = legacy_time.is_some();
    let splitting = matches!(options.legacy_tool, Some(LegacyMsTool::Split(_)));
    let spw = request.spw.trim();
    let time_groups = options
        .time_averaging
        .map(|time| {
            if let Some(legacy) = legacy_time {
                let dimensions = transform_array_dimensions(
                    input,
                    &selected_ddids,
                    ddid_to_spw,
                    channel_bins,
                    correlations,
                    None,
                )?;
                let per_block = flag_filter_read_plan(rows.len(), budget, dimensions.values())
                    .map_err(selection_error)?;
                let flag_axes = (splitting && (spw.contains(':') || correlations.is_some()))
                    .then_some(SelectedFlagAxes {
                        channels: channel_selection,
                        ddid_to_spw,
                        correlations,
                    });
                let (valid, _) = keep_unflagged_rows(
                    input,
                    &selected_rows,
                    &selected_ddids,
                    per_block,
                    flag_axes.as_ref(),
                )?;
                let groups = legacy_time::plan(input, &rows, valid, time, legacy)?;
                if groups.is_empty() {
                    return Err(MsTransformError::EmptySelection {
                        path: request.input_ms.display().to_string(),
                    });
                }
                Ok(groups)
            } else {
                time_average::plan(input, &rows, time, order, None)
            }
        })
        .transpose()?;
    drop(rows);
    let ordered = if let Some(groups) = &time_groups {
        let (rows, descriptions) = groups
            .iter()
            .map(|group| (group.first.row, group.first.description))
            .unzip();
        selection_metadata::OrderedMainRows {
            rows,
            descriptions,
            starts_visibility_buffer: Vec::new(),
            phase_reference_rows: Vec::new(),
        }
    } else if legacy || legacy_direct_band {
        // In-place regridding leaves existing MAIN rows in their original order.
        selection_metadata::OrderedMainRows {
            rows: selected_rows,
            descriptions: selected_ddids,
            starts_visibility_buffer: Vec::new(),
            phase_reference_rows: Vec::new(),
        }
    } else {
        selection_metadata::sort_selected_rows(
            input.main_table(),
            &request.input_ms,
            selected_rows,
            selected_ddids,
            order,
            false,
        )?
    };
    if let Some(SpectralOperation {
        kernel: SpectrumOperator::Convolution(kernel),
        ..
    }) = operation
    {
        let windows = input
            .spectral_window()
            .map_err(|source| selection_error(MsSelectionError::Domain(source)))?;
        let selected = ordered
            .descriptions
            .iter()
            .copied()
            .collect::<BTreeSet<_>>();
        let counts = selected
            .iter()
            .map(|ddid| {
                windows
                    .num_chan(ddid_to_spw[ddid] as usize)
                    .map(|channels| channels as usize)
            })
            .collect::<Result<Vec<_>, _>>()
            .map_err(|source| selection_error(MsSelectionError::Domain(source)))?;
        kernel
            .prepare(&counts)
            .map_err(MsTransformError::Spectrum)?;
    }
    Ok((time_groups, ordered))
}

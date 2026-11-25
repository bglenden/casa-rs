// SPDX-License-Identifier: LGPL-3.0-or-later
//! Read-only spectral grid calculation for the legacy MS tool.

use std::collections::BTreeSet;
use std::sync::Arc;

use casa_types::measures::MeasFrame;
use casa_types::measures::direction::MDirection;
use casa_types::measures::epoch::MEpoch;
use casa_types::measures::frequency::FrequencyRef;
use casa_types::measures::frequency_conversion::PreparedFrequencyConversion;

use crate::spectral_selection::combine_windows;
use crate::{MeasurementSet, derived::engine::MsCalEngine};

use super::super::{MsRegridOptions, MsTransformError, scalar_f64, scalar_i32};
use super::{
    GridBudget, MIN_SOURCE_VELOCITY_M_PER_S, axis, invalid_choice, ms_error, radial_shift,
};

/// A selected input spectral window with its persisted frequency reference.
pub struct ToolSpectralWindow {
    /// Channel centre frequencies in hertz, in the input window's order.
    pub centres_hz: Vec<f64>,
    /// Channel widths in hertz. The sign may indicate reversed sampling.
    pub widths_hz: Vec<f64>,
    /// Reference frame in which the input channel coordinates are stored.
    pub reference: FrequencyRef,
}

/// Phase centre for the legacy frequency-grid query.
pub enum ToolGridPhaseCentre {
    /// Use the selected FIELD direction and any attached ephemeris.
    Field(usize),
    /// Use a direction measure explicitly supplied by the caller.
    Direction(MDirection),
}

/// Determine the observatory frame used by the legacy grid query.
///
/// In the absence of an explicit epoch, use the earliest working row
/// matching the spectral window and field IDs. Casa's fallback is the
/// first working row when that cross-selection has no matches.
///
/// # Errors
///
/// Malformed row metadata or missing position, epoch, or phase data.
pub fn tool_spectral_grid_frame(
    input: &MeasurementSet,
    working_rows: Option<&[usize]>,
    (spws, fields): (&[i32], &[i32]),
    epoch: Option<MEpoch>,
    phase: &ToolGridPhaseCentre,
    source_frame: bool,
) -> Result<MeasFrame, MsTransformError> {
    let measures = crate::open_measures_runtime().map_err(ms_error)?;
    let engine = MsCalEngine::with_measures(input, Arc::clone(&measures)).map_err(ms_error)?;
    let seconds = if let Some(epoch) = epoch {
        epoch
            .convert_to(
                engine.time_reference(),
                &MeasFrame::default().with_measures(measures),
            )
            .map_err(ms_error)?
            .value()
            .as_mjd()
            * 86_400.0
    } else {
        earliest_epoch(input, working_rows, spws, fields)?
    };
    match phase {
        ToolGridPhaseCentre::Field(field) => {
            let mut frame = engine
                .spectral_frame_observatory(seconds, *field)
                .map_err(ms_error)?;
            if source_frame
                && let Some(velocity) = engine
                    .attached_radial_velocity(seconds, *field)
                    .map_err(ms_error)?
            {
                frame = frame.with_radial_velocity(velocity);
            }
            Ok(frame)
        }
        ToolGridPhaseCentre::Direction(direction) => engine
            .spectral_frame_observatory_direction(seconds, direction.clone())
            .map_err(ms_error),
    }
}

fn earliest_epoch(
    input: &MeasurementSet,
    working_rows: Option<&[usize]>,
    spws: &[i32],
    fields: &[i32],
) -> Result<f64, MsTransformError> {
    let table = input.main_table();
    let all_rows;
    let rows = if let Some(rows) = working_rows {
        rows
    } else {
        all_rows = (0..table.row_count()).collect::<Vec<_>>();
        &all_rows
    };
    let descriptions = input.data_description().map_err(ms_error)?;
    let mut selected_descriptions = BTreeSet::new();
    for id in 0..descriptions.row_count() {
        let spw = descriptions.spectral_window_id(id).map_err(ms_error)?;
        if spws.contains(&spw) {
            selected_descriptions.insert(id as i32);
        }
    }
    let read = |name| {
        table
            .column_accessor(name)
            .and_then(|column| column.scalar_cells_owned_for_rows(rows))
            .map_err(ms_error)
    };
    let times = read("TIME")?;
    let ddids = read("DATA_DESC_ID")?;
    let field_ids = if fields.is_empty() {
        None
    } else {
        Some(read("FIELD_ID")?)
    };
    let mut earliest = f64::INFINITY;
    for (slot, &row) in rows.iter().enumerate() {
        let time = scalar_f64(times[slot].as_ref(), "TIME", row)?;
        if !time.is_finite() || time >= earliest {
            continue;
        }
        let ddid = scalar_i32(ddids[slot].as_ref(), "DATA_DESC_ID", row)?;
        if selected_descriptions.contains(&ddid)
            && match &field_ids {
                None => true,
                Some(ids) => fields.contains(&scalar_i32(ids[slot].as_ref(), "FIELD_ID", row)?),
            }
        {
            earliest = time;
        }
    }
    if earliest.is_finite() {
        Ok(earliest)
    } else {
        crate::subtables::get_f64(table, rows.first().copied().unwrap_or(0), "TIME")
            .map_err(ms_error)
    }
}

/// Calculate the frequency centres for an MS tool spectral grid without writing a table.
///
/// A single window retains its native channel order. Multiple windows use the
/// shared overlap calculation before the resulting axis is regridded. A frame
/// is requested only for real frequency conversion and must describe the
/// requested observation epoch, position, and phase centre.
///
/// # Errors
///
/// Inconsistent input axes, invalid regridding arguments, missing measures
/// context, or a grid beyond the supplied memory budget.
pub fn tool_channel_frequencies(
    windows: Vec<ToolSpectralWindow>,
    options: &MsRegridOptions,
    budget_bytes: usize,
    frame: impl FnOnce() -> Result<MeasFrame, MsTransformError>,
) -> Result<Vec<f64>, MsTransformError> {
    let Some(first) = windows.first() else {
        return Ok(Vec::new());
    };
    let source = first.reference;
    let (mut centres, mut widths) = selected_axis(windows, source)?;
    let source_frame = options.outframe.eq_ignore_ascii_case("SOURCE");
    let target = if options.outframe.is_empty() {
        source
    } else if source_frame {
        FrequencyRef::GEO
    } else {
        options
            .outframe
            .parse::<FrequencyRef>()
            .map_err(|_| invalid_choice("outframe", &options.outframe))?
    };
    if target != source || source_frame {
        let frame = frame()?;
        let conversion =
            PreparedFrequencyConversion::from_measure_references(source, target, &frame, &frame)
                .map_err(ms_error)?;
        let velocity = frame
            .radial_velocity()
            .filter(|velocity| source_frame && velocity.ms().abs() > MIN_SOURCE_VELOCITY_M_PER_S)
            .map_or(0., |velocity| -velocity.ms());
        let shift = radial_shift(velocity);
        for (centre, width) in centres.iter_mut().zip(&mut widths) {
            let half_width = width.abs() / 2.;
            let converted_width = conversion.convert_hz(*centre + half_width)
                - conversion.convert_hz(*centre - half_width);
            *width = shift.shift_frequency_hz(converted_width);
            *centre = shift.shift_frequency_hz(conversion.convert_hz(*centre));
        }
    }
    let limit = GridBudget::new(budget_bytes, false).maximum_output_channels(centres.len())?;
    Ok(axis::output_axis(options, &centres, &widths, limit)?.centres_hz)
}

fn selected_axis(
    mut windows: Vec<ToolSpectralWindow>,
    source: FrequencyRef,
) -> Result<(Vec<f64>, Vec<f64>), MsTransformError> {
    if windows.len() == 1 {
        let window = windows.pop().expect("one input spectral window");
        return Ok((window.centres_hz, window.widths_hz));
    }
    if windows.iter().any(|window| window.reference != source) {
        return Err(MsTransformError::Spectrum(
            "Selected spectral windows have different frequency reference frames.".into(),
        ));
    }
    let axes = windows
        .iter()
        .map(|window| (window.centres_hz.as_slice(), window.widths_hz.as_slice()))
        .collect::<Vec<_>>();
    let combined = combine_windows(&axes, true).map_err(ms_error)?;
    Ok(combined
        .into_iter()
        .map(|channel| (channel.frequency, channel.width))
        .unzip())
}

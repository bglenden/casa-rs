// SPDX-License-Identifier: LGPL-3.0-or-later
//! Read-only spectral grid calculation for the legacy MS tool.

use std::collections::BTreeSet;
use std::sync::Arc;

use casa_tables::{
    RequiredScalarColumnDestination, RequiredScalarColumnValues, RequiredScalarColumnValuesMut,
    Table,
};
use casa_types::measures::direction::MDirection;
use casa_types::measures::epoch::MEpoch;
use casa_types::measures::frequency::FrequencyRef;
use casa_types::measures::frequency_conversion::PreparedFrequencyConversion;
use casa_types::measures::position::MPosition;
use casa_types::measures::{MeasFrame, MeasuresProvider};

use crate::derived::engine::MsCalEngine;
use crate::spectral_selection::combine_windows;
use crate::subtables::measured_columns::antenna_position;
use crate::subtables::{
    FieldDirectionColumn, MsDataDescription, MsField, epoch_reference, get_i32, get_string,
    linked_subtable_row,
};
use crate::{MeasurementSet, SubtableId};

use super::super::{MsRegridOptions, MsTransformError};
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

/// The persisted tables that supply a spectral query's observation geometry.
pub struct ToolGridFrameTables<'a> {
    /// The attached physical MAIN table.
    pub main: &'a Table,
    /// The first row in the current tool view, as a physical MAIN row.
    pub first_row: usize,
    /// Linked ANTENNA tables in view order.
    pub antenna: &'a [Table],
    /// Linked FIELD tables in view order.
    pub field: &'a [Table],
    /// Linked OBSERVATION tables in view order.
    pub observation: &'a [Table],
}

/// Construct the frame for a fixed or explicitly supplied phase centre.
///
/// Returns `None` when a FIELD polynomial, ephemeris, or measure offset
/// needs the general spectral frame evaluator.
///
/// # Errors
///
/// The field or observatory tables cannot be read or contain invalid rows.
pub fn tool_grid_frame_at_epoch(
    tables: ToolGridFrameTables<'_>,
    epoch: &MEpoch,
    phase: &ToolGridPhaseCentre,
    measures: Arc<dyn MeasuresProvider>,
) -> Result<Option<MeasFrame>, MsTransformError> {
    let direction = match phase {
        ToolGridPhaseCentre::Direction(direction) => direction.clone(),
        ToolGridPhaseCentre::Field(id) => {
            let (table, row) =
                linked_subtable_row(tables.field, *id, SubtableId::Field).map_err(ms_error)?;
            let field = MsField::new(table);
            if field.num_poly(row).map_err(ms_error)? > 0
                || field
                    .ephemeris_id(row)
                    .map_err(ms_error)?
                    .is_some_and(|id| id >= 0)
            {
                return Ok(None);
            }
            let phase = field
                .metadata_direction_at_seconds(row, FieldDirectionColumn::Phase, 0.0)
                .map_err(ms_error)?;
            if phase.offset.is_some() {
                return Ok(None);
            }
            phase.direction
        }
    };
    let telescope = if tables.observation.iter().any(|table| table.row_count() > 0) {
        let id = get_i32(tables.main, tables.first_row, "OBSERVATION_ID").map_err(ms_error)?;
        let (observation, row) =
            linked_subtable_row(tables.observation, id as usize, SubtableId::Observation)
                .map_err(ms_error)?;
        Some(get_string(observation, row, "TELESCOPE_NAME").map_err(ms_error)?)
    } else {
        None
    };
    let position = match telescope
        .as_deref()
        .and_then(|name| MPosition::casa_observatory(name, measures.as_ref()))
    {
        Some(position) => position,
        None => {
            let (antenna, row) =
                linked_subtable_row(tables.antenna, 0, SubtableId::Antenna).map_err(ms_error)?;
            antenna_position(antenna, row).map_err(ms_error)?
        }
    };
    Ok(Some(
        MeasFrame::new()
            .with_epoch(epoch.clone())
            .with_direction(direction)
            .with_position(position)
            .with_measures(measures),
    ))
}

/// Evaluate the observatory frame for a FIELD with time-dependent measure metadata.
///
/// # Errors
///
/// Malformed row metadata or missing position, epoch, or phase data.
pub fn tool_spectral_grid_frame(
    input: &MeasurementSet,
    epoch: &MEpoch,
    phase: &ToolGridPhaseCentre,
    source_frame: bool,
    measures: Arc<dyn MeasuresProvider>,
) -> Result<MeasFrame, MsTransformError> {
    let engine = MsCalEngine::with_measures(input, Arc::clone(&measures)).map_err(ms_error)?;
    let seconds = epoch
        .convert_to(
            engine.time_reference(),
            &MeasFrame::default().with_measures(measures),
        )
        .map_err(ms_error)?
        .value()
        .as_mjd()
        * 86_400.0;
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

/// Return the earliest working epoch for the requested fields and windows.
///
/// When that cross-selection contains no rows, use the first working row.
/// The result retains the epoch scale stored in MAIN. The caller may select
/// direct on-disk reads by identifying a plain rather than a reference or
/// concatenated MAIN table.
///
/// # Errors
///
/// The working view has no rows or its required scalar columns cannot be read.
pub fn tool_grid_reference_epoch(
    table: &Table,
    working_rows: Option<&[usize]>,
    description_tables: &[Table],
    spws: &[i32],
    fields: &[i32],
    plain_main: bool,
) -> Result<MEpoch, MsTransformError> {
    let all_rows;
    let rows = if let Some(rows) = working_rows {
        rows
    } else {
        all_rows = (0..table.row_count()).collect::<Vec<_>>();
        &all_rows
    };
    let mut selected_descriptions = BTreeSet::new();
    let mut ddid = 0;
    for table in description_tables {
        let descriptions = MsDataDescription::new(table);
        for row in 0..descriptions.row_count() {
            let spw = descriptions.spectral_window_id(row).map_err(ms_error)?;
            if spws.contains(&spw) {
                selected_descriptions.insert(ddid);
            }
            ddid += 1;
        }
    }
    let mut times = Vec::new();
    let mut descriptions = Vec::new();
    let mut field_ids = Vec::new();
    let mut columns = [
        RequiredScalarColumnDestination::new(
            "TIME",
            RequiredScalarColumnValuesMut::Float64(&mut times),
        ),
        RequiredScalarColumnDestination::new(
            "DATA_DESC_ID",
            RequiredScalarColumnValuesMut::Int32(&mut descriptions),
        ),
        RequiredScalarColumnDestination::new(
            "FIELD_ID",
            RequiredScalarColumnValuesMut::Int32(&mut field_ids),
        ),
    ];
    let count = if fields.is_empty() { 2 } else { 3 };
    let names = ["TIME", "DATA_DESC_ID", "FIELD_ID"];
    if plain_main
        && names[..count]
            .iter()
            .all(|name| !table.is_virtual_column(name))
    {
        table
            .required_scalar_columns_for_rows_into(rows, &mut columns[..count])
            .map_err(ms_error)?;
    } else {
        let mut values = table
            .required_scalar_columns_owned_for_rows(&names[..count], rows)
            .map_err(ms_error)?;
        let Some(RequiredScalarColumnValues::Float64(input_times)) = values.remove("TIME") else {
            return Err(ms_error("MAIN TIME is not Float64"));
        };
        let Some(RequiredScalarColumnValues::Int32(input_descriptions)) =
            values.remove("DATA_DESC_ID")
        else {
            return Err(ms_error("MAIN DATA_DESC_ID is not Int32"));
        };
        times = input_times;
        descriptions = input_descriptions;
        if count == 3 {
            let Some(RequiredScalarColumnValues::Int32(input_fields)) = values.remove("FIELD_ID")
            else {
                return Err(ms_error("MAIN FIELD_ID is not Int32"));
            };
            field_ids = input_fields;
        }
    }
    let first = *times
        .first()
        .ok_or_else(|| ms_error("No working MAIN rows"))?;
    let (slot, seconds) = times
        .iter()
        .zip(descriptions)
        .enumerate()
        .filter_map(|(slot, (&time, description))| {
            (time.is_finite()
                && selected_descriptions.contains(&description)
                && (fields.is_empty() || fields.contains(&field_ids[slot])))
            .then_some((slot, time))
        })
        .min_by(|left, right| left.1.total_cmp(&right.1))
        .unwrap_or((0, first));
    let reference = epoch_reference(table, "TIME", rows[slot], 0).map_err(ms_error)?;
    Ok(MEpoch::from_mjd_seconds(seconds, reference))
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

// SPDX-License-Identifier: LGPL-3.0-or-later
//! PlotMS page metadata from the first selected MAIN row, before reduction.

use casa_types::ArrayValue;
use casa_types::measures::direction::{DirectionRef, MDirection};
use casa_types::measures::epoch::{EpochRef, MEpoch};
use casa_types::measures::frame::MeasFrame;
use casa_types::quanta::MvTime;
use casa_types::quanta::format::{ANGLE, CLEAN, TIME, format_angle, format_time};

use super::{
    MsDataColumn, MsExploreSpec, MsPageHeaderItem, PlotMsInput, plotms_figure, plotms_text,
    wrap_header_segments,
};
use crate::MeasurementSet;
use crate::columns::main_ids;
use crate::derived::engine::{detect_epoch_reference, resolve_direction_reference};
use crate::subtables::field::MsField;
use crate::subtables::observation::MsObservation;

pub(super) fn lines(
    ms: &MeasurementSet,
    spec: &MsExploreSpec,
    input: &PlotMsInput<'_>,
) -> Vec<String> {
    use MsPageHeaderItem::*;

    if spec.header_items.is_empty() {
        return Vec::new();
    }
    let main = ms.main_table();
    let observation = input.header_row.and_then(|row| {
        let id = usize::try_from(main_ids::observation_id(main).get(row).ok()?).ok()?;
        Some((ms.observation().ok()?, id))
    });
    let read_obs = |name| {
        observation
            .as_ref()
            .and_then(|(table, id)| table.string(*id, name).ok())
            .unwrap_or_default()
    };
    let field = input.header_row.and_then(|row| {
        let id = usize::try_from(main_ids::field_id(main).get(row).ok()?).ok()?;
        Some((ms.field().ok()?, id))
    });
    let start = spec
        .header_items
        .iter()
        .any(|item| matches!(item, ObsDate | ObsTime))
        .then(|| {
            observation
                .as_ref()
                .and_then(|(table, id)| observation_start(table, *id))
        })
        .flatten();
    let segments = spec
        .header_items
        .iter()
        .map(|item| match item {
            Filename => format!(
                "File: {}",
                spec.ms_path
                    .file_name()
                    .unwrap_or(spec.ms_path.as_os_str())
                    .to_string_lossy()
            ),
            YColumn => format!("Y Column(s): {}", y_columns(spec, input)),
            ObsDate => format!(
                "Observation date: {}",
                start.map(observation_date).unwrap_or_default()
            ),
            ObsTime => format!(
                "Observation time: {}",
                start
                    .map(|time| format!("{} UTC", format_time(time.as_mjd_days(), TIME | CLEAN, 4)))
                    .unwrap_or_default()
            ),
            Observer => format!("Observer: {}", read_obs("OBSERVER")),
            ProjId => format!("Project ID: {}", read_obs("PROJECT")),
            Telescope => format!("Telescope: {}", read_obs("TELESCOPE_NAME")),
            TargName => format!(
                "Target name: {}",
                field
                    .as_ref()
                    .and_then(|(table, id)| table.name(*id).ok())
                    .unwrap_or_default()
            ),
            TargDir => format!(
                "Target RA,Dec: {}",
                field
                    .as_ref()
                    .and_then(|(table, id)| target_direction(ms, input.header_row?, table, *id))
                    .unwrap_or_default()
            ),
        })
        .collect::<Vec<_>>();
    wrap_header_segments(&segments)
}

fn observation_start(table: &MsObservation<'_>, row: usize) -> Option<MvTime> {
    let ArrayValue::Float64(range) = table.array(row, "TIME_RANGE").ok()? else {
        return None;
    };
    let seconds = *range.iter().next()?;
    if !seconds.is_finite() {
        return None;
    }
    let reference = detect_epoch_reference(table.table(), "TIME_RANGE", EpochRef::UTC);
    let start = MEpoch::from_mjd_seconds(seconds, reference)
        .convert_to(EpochRef::UTC, &MeasFrame::new())
        .ok()?;
    Some(MvTime::from_mjd_days(start.value().as_mjd()))
}

fn observation_date(time: MvTime) -> String {
    let date = time.format_dmy_date();
    let (_, month_year) = date.split_once('-').expect("DMY has a day separator");
    format!("{} {}", time.month_day(), month_year.replacen('-', " ", 1))
}

fn y_columns(spec: &MsExploreSpec, input: &PlotMsInput<'_>) -> String {
    spec.plots
        .iter()
        .flat_map(|plot| {
            plot.y_axes.iter().enumerate().map(|(index, &axis)| {
                let column = match index {
                    0 => plot.data_column,
                    1 => input.secondary_y_data_column.unwrap_or(plot.data_column),
                    _ => input
                        .additional_ordinates
                        .get(index - 2)
                        .map_or(plot.data_column, |entry| entry.1),
                };
                if axis.is_visibility_math()
                    && matches!(column, MsDataColumn::Data | MsDataColumn::Float)
                {
                    format!("{}:data", plotms_text::name(axis))
                } else {
                    plotms_figure::column_axis_name(axis, column)
                }
            })
        })
        .collect::<Vec<_>>()
        .join(", ")
}

fn target_direction(
    ms: &MeasurementSet,
    main_row: usize,
    field: &MsField<'_>,
    id: usize,
) -> Option<String> {
    let source = ms.source().ok()?;
    let source_id = field.source_id(id).ok()?;
    let data_desc = main_ids::data_desc_id(ms.main_table()).get(main_row).ok()?;
    let spw = ms
        .data_description()
        .ok()?
        .spectral_window_id(usize::try_from(data_desc).ok()?)
        .ok()?;
    let (row, _) = (0..source.row_count())
        .filter_map(|row| {
            let id = source.i32(row, "SOURCE_ID").ok()?;
            let window = source.i32(row, "SPECTRAL_WINDOW_ID").ok()?;
            if id != source_id || (window != -1 && window != spw) {
                return None;
            }
            Some((row, source.f64(row, "TIME").ok()?))
        })
        .min_by(|(_, left), (_, right)| left.total_cmp(right))?;
    let ArrayValue::Float64(values) = source.array(row, "DIRECTION").ok()? else {
        return None;
    };
    let [lon, lat] = values.as_slice()? else {
        return None;
    };
    if !lon.is_finite() || !lat.is_finite() {
        return None;
    }
    let reference = resolve_direction_reference(source.table(), "SOURCE", "DIRECTION", row).ok()?;
    let (lon, lat) = MDirection::from_angles(*lon, *lat, reference).as_angles();
    Some(direction_label(lon, lat, reference))
}

fn direction_label(lon: f64, lat: f64, reference: DirectionRef) -> String {
    use DirectionRef::*;

    let angles = if matches!(
        reference,
        ICRS | J2000 | JMEAN | JTRUE | APP | B1950 | B1950Vla | BMEAN | BTRUE
    ) {
        let sign = if lat < 0.0 { "-" } else { "" };
        format!(
            "{} {sign}{}",
            format_angle(lon, TIME, 12),
            format_angle(lat.abs(), ANGLE | CLEAN, 11).trim()
        )
    } else {
        format!(
            "{} {}",
            format_angle(lon, ANGLE, 11),
            format_angle(lat, ANGLE, 11)
        )
    };
    if reference == ICRS {
        angles
    } else {
        format!("{angles} {reference}")
    }
}

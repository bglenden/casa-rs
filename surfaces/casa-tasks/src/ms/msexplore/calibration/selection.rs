// SPDX-License-Identifier: LGPL-3.0-or-later
//! Resolve CALTABLE selections with the native MS metadata grammars.

use std::collections::{BTreeMap, BTreeSet};

use casa_ms::selection::antenna_expr::{AntennaRow, BaselineSelection, resolve_antenna_selector};
use casa_ms::selection::parser::{FieldSelectorRow, resolve_field_selector_rows};
use casa_ms::selection::spw_expr::{DataDescriptionRow, SpectralWindowRow, resolve_spw_selector};
use casa_ms::selection::syntax::parse_numeric_id_selector;
use casa_ms::selection::time_expr::{
    TimeDefaults, TimeSelection, TimeSelector, TimeSelectorDefaults,
};

use super::axis::SampleCoordinates;
use super::table_data::CalibrationTable;

#[derive(Default)]
pub(super) struct Expressions<'a> {
    pub(super) antenna: &'a str,
    pub(super) field: &'a str,
    pub(super) spw: &'a str,
    pub(super) scan: &'a str,
    pub(super) timerange: &'a str,
    pub(super) observation: &'a str,
    pub(super) taql: &'a str,
}

pub(super) struct Selection {
    antennas: BaselineSelection,
    use_baseline: bool,
    fields: Vec<i32>,
    scans: Vec<i32>,
    observations: Vec<i32>,
    time: Option<TimeSelection>,
    taql_rows: Option<BTreeSet<usize>>,
    all_windows: bool,
    channel_lists: BTreeMap<i32, Vec<usize>>,
}

impl Selection {
    pub(super) fn resolve(
        table: &CalibrationTable,
        expressions: &Expressions<'_>,
    ) -> Result<Self, String> {
        let antenna_rows = table
            .antennas
            .iter()
            .map(|row| AntennaRow {
                name: &row.name,
                station: &row.station,
                position_m: row.position,
            })
            .collect::<Vec<_>>();
        let antennas =
            resolve_antenna_selector(&antenna_rows, expressions.antenna).map_err(error)?;
        let field_rows = table
            .fields
            .iter()
            .map(|row| FieldSelectorRow {
                name: &row.name,
                code: Some(&row.code),
                flagged: row.flagged,
            })
            .collect::<Vec<_>>();
        let fields = resolve_field_selector_rows(&field_rows, expressions.field).map_err(error)?;
        let scans = parse_numeric_id_selector(expressions.scan, "scan").map_err(error)?;
        let observations =
            parse_numeric_id_selector(expressions.observation, "observation").map_err(error)?;
        let windows = table
            .windows
            .iter()
            .map(|row| SpectralWindowRow {
                name: &row.name,
                chan_freq_hz: &row.frequencies,
                chan_width_hz: &row.widths,
                ref_frequency_hz: row.reference,
                total_bandwidth_hz: row.bandwidth,
                flag_row: row.flagged,
            })
            .collect::<Vec<_>>();
        let descriptions = windows
            .iter()
            .enumerate()
            .map(|(id, row)| DataDescriptionRow {
                spectral_window_id: id as i32,
                flag_row: row.flag_row,
            })
            .collect::<Vec<_>>();
        let spw = resolve_spw_selector(&windows, &descriptions, expressions.spw).map_err(error)?;
        let mut channel_lists = BTreeMap::new();
        for &window in spw.spw_ids() {
            let channels = spw.channels_of(window).map_err(error)?.unwrap_or_default();
            channel_lists.insert(window as i32, channels);
        }
        let mut selection = Self {
            antennas,
            use_baseline: expressions.antenna.contains('&'),
            fields,
            scans,
            observations,
            time: None,
            taql_rows: table.taql_rows(expressions.taql)?,
            all_windows: spw.selects_all(),
            channel_lists,
        };
        if !expressions.timerange.is_empty() && table.rows() > 0 {
            let defaults = |row: SampleCoordinates| TimeDefaults {
                time_mjd_s: row.time,
                // MSTimeParse uses 0.1 seconds for calibration tables.
                exposure_s: 0.1,
            };
            let row_zero = table.coordinates(0)?;
            let mut first = row_zero;
            for index in 0..table.rows() {
                let row = table.coordinates(index)?;
                if selection.includes(&row) {
                    first = row;
                    break;
                }
            }
            selection.time = Some(
                TimeSelector::parse(expressions.timerange)
                    .map_err(error)?
                    .resolve(TimeSelectorDefaults {
                        first_term: defaults(first),
                        later_terms: defaults(row_zero),
                    })
                    .map_err(error)?,
            );
        }
        Ok(selection)
    }

    pub(super) fn includes(&self, point: &SampleCoordinates) -> bool {
        let includes_id = |values: &[i32], id| values.is_empty() || values.contains(&id);
        let [primary, secondary] = point
            .antennas
            .map(|id| usize::try_from(id).unwrap_or(usize::MAX));
        let secondary = if self.use_baseline {
            secondary
        } else {
            usize::MAX
        };
        self.taql_rows
            .as_ref()
            .is_none_or(|rows| rows.contains(&point.row))
            && includes_id(&self.fields, point.field)
            && includes_id(&self.scans, point.scan)
            && includes_id(&self.observations, point.observation)
            && self.time.as_ref().is_none_or(|time| time.holds(point.time))
            && self.antennas.holds(primary, secondary)
            && (self.all_windows || self.channel_lists.contains_key(&point.window))
    }

    pub(super) fn channels(&self, window: i32, count: usize) -> Vec<usize> {
        self.channel_lists
            .get(&window)
            .filter(|channels| !channels.is_empty())
            .map_or_else(
                || (0..count).collect(),
                |channels| {
                    channels
                        .iter()
                        .copied()
                        .take_while(|&channel| channel < count)
                        .collect()
                },
            )
    }
}

fn error(error: impl std::fmt::Display) -> String {
    error.to_string()
}

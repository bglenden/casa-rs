// SPDX-License-Identifier: LGPL-3.0-or-later
//! Match antenna identities and reconcile the feeds of each input antenna.

use casa_tables::{
    Table,
    table_quantum::{ArrayQuantColumn, ScalarQuantColumn, TableQuantumDesc},
};
use casa_types::{
    RecordValue, ScalarValue, Value,
    measures::MPosition,
    quanta::{Quantity, Unit},
};

use crate::columns::position_columns::AntennaPositionColumn;
use crate::subtables::{get_bool, get_i32, get_string};
use crate::{MeasurementSet, MsError, MsResult, SubtableId};

use super::required_table;

pub(super) struct AntennaRows {
    pub(super) antennas: Vec<RecordValue>,
    pub(super) feeds: Vec<RecordValue>,
    pub(super) feed_updates: Vec<(usize, RecordValue)>,
    pub(super) antenna_ids: Vec<usize>,
}

pub(super) fn reconcile(
    target: &MeasurementSet,
    donor: &MeasurementSet,
    windows: &[usize],
) -> MsResult<AntennaRows> {
    let target_antennas = required_table(target, SubtableId::Antenna)?;
    let donor_antennas = required_table(donor, SubtableId::Antenna)?;
    let target_feeds = required_table(target, SubtableId::Feed)?;
    let donor_feeds = required_table(donor, SubtableId::Feed)?;
    let mut known_antennas = read_antennas(target_antennas)?;
    let incoming_antennas = read_antennas(donor_antennas)?;
    let mut known_feeds = read_feeds(target_feeds)?;
    let mut incoming_feeds = read_feeds(donor_feeds)?;
    for feed in &mut incoming_feeds {
        if feed.window >= 0 {
            let index = feed.window as usize;
            let mapped = windows.get(index).ok_or_else(|| MsError::InvalidIndex {
                context: "FEED spectral window".into(),
                index,
                max: windows.len().saturating_sub(1),
            })?;
            feed.window = row_id(*mapped)?;
        }
    }
    let mut result = AntennaRows {
        antennas: Vec::new(),
        feeds: Vec::new(),
        feed_updates: Vec::new(),
        antenna_ids: Vec::with_capacity(incoming_antennas.len()),
    };
    for (incoming_id, antenna) in incoming_antennas.into_iter().enumerate() {
        let storage = donor_antennas.rows()?;
        if let Some(first) = known_antennas.first()
            && first.position.refer() != antenna.position.refer()
        {
            return Err(MsError::InvalidInput(
                "ANTENNA position frames differ".into(),
            ));
        }
        let matching = known_antennas
            .iter()
            .rposition(|known| known.matches(&antenna));
        let id = matching.unwrap_or_else(|| {
            let index = known_antennas.len();
            known_antennas.push(antenna);
            result.antennas.push(storage[incoming_id].clone());
            index
        });
        result.antenna_ids.push(id);
        merge_feeds(
            &mut result,
            &mut known_feeds,
            &incoming_feeds,
            target_feeds,
            incoming_id,
            id,
            matching.is_some(),
        )?;
    }
    Ok(result)
}

struct Antenna {
    name: String,
    station: String,
    flagged: bool,
    position: MPosition,
}

impl Antenna {
    fn matches(&self, incoming: &Self) -> bool {
        !self.flagged
            && self.name == incoming.name
            && (incoming.station.is_empty() || self.station == incoming.station)
            && self
                .position
                .values()
                .iter()
                .zip(incoming.position.values())
                .all(|(&known, candidate)| (known - candidate).abs() <= 1.0)
    }
}

fn read_antennas(table: &Table) -> MsResult<Vec<Antenna>> {
    let positions = AntennaPositionColumn::new(table);
    (0..table.row_count())
        .map(|row| {
            Ok(Antenna {
                name: get_string(table, row, "NAME")?,
                station: get_string(table, row, "STATION")?,
                flagged: get_bool(table, row, "FLAG_ROW")?,
                position: positions.get_position(row)?,
            })
        })
        .collect()
}

#[derive(Clone)]
struct Feed {
    record: RecordValue,
    antenna: i32,
    window: i32,
    id: i32,
    receptors: i32,
    time_s: f64,
    interval_s: f64,
    position_m: Vec<f64>,
    beam_deg: Vec<f64>,
    angle_deg: Vec<f64>,
    focus_m: Option<f64>,
}

impl Feed {
    fn matches(&self, incoming: &Self) -> bool {
        self.antenna == incoming.antenna
            && self.window == incoming.window
            && self.id == incoming.id
            && self.receptors == incoming.receptors
            && self.position_m == incoming.position_m
            && self.beam_deg == incoming.beam_deg
            && self.angle_deg == incoming.angle_deg
            && self
                .focus_m
                .zip(incoming.focus_m)
                .is_none_or(|(a, b)| a == b)
            && ["POLARIZATION_TYPE", "POL_RESPONSE"]
                .into_iter()
                .all(|column| self.record.get(column) == incoming.record.get(column))
    }

    fn include_interval(&mut self, incoming: &Self, target: &Table) -> MsResult<bool> {
        let old_half = if self.interval_s == 0.0 {
            5.0e17
        } else {
            self.interval_s / 2.0
        };
        let new_half = incoming.interval_s / 2.0;
        if self.time_s - old_half <= incoming.time_s - new_half
            && self.time_s + old_half >= incoming.time_s + new_half
        {
            return Ok(false);
        }
        let upper = (self.time_s + old_half).max(incoming.time_s + new_half);
        let time = (self.time_s + incoming.time_s) / 2.0;
        if time == 0.0 {
            // CASA uses zero to signal that the interval needs no update.
            return Ok(false);
        }
        self.time_s = time;
        self.interval_s = 2.0 * (upper - time);
        self.record.upsert(
            "TIME",
            number(seconds_in_column(target, "TIME", self.time_s)?),
        );
        self.record.upsert(
            "INTERVAL",
            number(seconds_in_column(target, "INTERVAL", self.interval_s)?),
        );
        Ok(true)
    }
}

fn read_feeds(table: &Table) -> MsResult<Vec<Feed>> {
    if table.row_count() == 0 {
        return Ok(Vec::new());
    }
    let time = ScalarQuantColumn::with_unit(table, "TIME", "s")?;
    let interval = ScalarQuantColumn::with_unit(table, "INTERVAL", "s")?;
    let position = ArrayQuantColumn::with_unit(table, "POSITION", "m")?;
    let beam = ArrayQuantColumn::with_unit(table, "BEAM_OFFSET", "deg")?;
    let angle = ArrayQuantColumn::with_unit(table, "RECEPTOR_ANGLE", "deg")?;
    let focus = table
        .schema()
        .filter(|schema| schema.contains_column("FOCUS_LENGTH"))
        .map(|_| ScalarQuantColumn::with_unit(table, "FOCUS_LENGTH", "m"))
        .transpose()?;
    let records = table.rows()?;
    let array = |column: &ArrayQuantColumn<'_>, row| -> MsResult<Vec<f64>> {
        Ok(column.get(row)?.iter().map(Quantity::value).collect())
    };
    (0..table.row_count())
        .map(|row| {
            Ok(Feed {
                record: records[row].clone(),
                antenna: get_i32(table, row, "ANTENNA_ID")?,
                window: get_i32(table, row, "SPECTRAL_WINDOW_ID")?,
                id: get_i32(table, row, "FEED_ID")?,
                receptors: get_i32(table, row, "NUM_RECEPTORS")?,
                time_s: time.get(row)?.value(),
                interval_s: interval.get(row)?.value(),
                position_m: array(&position, row)?,
                beam_deg: array(&beam, row)?,
                angle_deg: array(&angle, row)?,
                focus_m: focus
                    .as_ref()
                    .map(|column| column.get(row).map(|value| value.value()))
                    .transpose()?,
            })
        })
        .collect()
}

fn merge_feeds(
    result: &mut AntennaRows,
    known: &mut Vec<Feed>,
    incoming: &[Feed],
    target: &Table,
    incoming_id: usize,
    target_id: usize,
    known_antenna: bool,
) -> MsResult<()> {
    let old_id = row_id(incoming_id)?;
    let new_id = row_id(target_id)?;
    let incoming = incoming
        .iter()
        .enumerate()
        .filter(|(_, row)| row.antenna == old_id)
        .collect::<Vec<_>>();
    let mut matched = Vec::new();
    if known_antenna && known.iter().filter(|row| row.antenna == new_id).count() == incoming.len() {
        for (_, feed) in &incoming {
            if let Some(index) = known
                .iter()
                .enumerate()
                .rfind(|(index, row)| !matched.contains(index) && row.matches(feed))
                .map(|(index, _)| index)
            {
                matched.push(index);
                if known[index].include_interval(feed, target)? {
                    if let Some(added) = index.checked_sub(target.row_count()) {
                        result.feeds[added] = known[index].record.clone();
                    } else if let Some((_, row)) = result
                        .feed_updates
                        .iter_mut()
                        .find(|(row, _)| *row == index)
                    {
                        *row = known[index].record.clone();
                    } else {
                        result
                            .feed_updates
                            .push((index, known[index].record.clone()));
                    }
                }
            }
        }
    }
    if matched.len() < incoming.len() {
        for (index, feed) in incoming {
            if matched.contains(&index) {
                continue;
            }
            let mut feed = feed.clone();
            feed.antenna = new_id;
            feed.record
                .upsert("ANTENNA_ID", Value::Scalar(ScalarValue::Int32(new_id)));
            feed.record.upsert(
                "SPECTRAL_WINDOW_ID",
                Value::Scalar(ScalarValue::Int32(feed.window)),
            );
            result.feeds.push(feed.record.clone());
            known.push(feed);
        }
    }
    Ok(())
}

fn seconds_in_column(table: &Table, column: &str, value: f64) -> MsResult<f64> {
    let unit = TableQuantumDesc::reconstruct(table, column)
        .and_then(|quantity| quantity.units().first().cloned())
        .unwrap_or_else(|| "s".into());
    let error = |error: casa_types::quanta::UnitError| MsError::InvalidInput(error.to_string());
    let quantity = Quantity::new(value, "s").map_err(error)?;
    quantity
        .get_value_in(&Unit::new(&unit).map_err(error)?)
        .map_err(error)
}

fn number(value: f64) -> Value {
    Value::Scalar(ScalarValue::Float64(value))
}

fn row_id(index: usize) -> MsResult<i32> {
    i32::try_from(index)
        .map_err(|_| MsError::InvalidInput("antenna or window ID exceeds int32".into()))
}

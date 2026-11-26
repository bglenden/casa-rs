// SPDX-License-Identifier: LGPL-3.0-or-later
//! NOSTAR scan time, array coordinates and integration metadata.

use casa_core::quanta::{Quantity, Unit};

use super::binary::{Record, invalid};
use super::header::{Array, Header};
use crate::MsResult;
use crate::single_dish_import::{Basis, Direction, Integration, Plane, RawIntegration, Weather};

const JST_SECONDS: f64 = 9.0 * 3600.0;

pub(super) fn start_time(record: Record<'_>) -> MsResult<f64> {
    let time = record.text(8, 24)?;
    if !time.is_ascii() || time.len() < 14 || !time[..12].bytes().all(|byte| byte.is_ascii_digit())
    {
        return Err(invalid("scan timestamp"));
    }
    let calendar = format!(
        "{}/{}/{}/{}:{}:{}",
        &time[..4],
        &time[4..6],
        &time[6..8],
        &time[8..10],
        &time[10..12],
        &time[12..],
    );
    let seconds_unit = Unit::new("s").map_err(|_| invalid("time unit"))?;
    let time = Quantity::read(&calendar).map_err(|_| invalid("scan timestamp"))?;
    let seconds = time
        .get_value_in(&seconds_unit)
        .map_err(|_| invalid("scan timestamp"))?;
    Ok(seconds - JST_SECONDS)
}

pub(super) fn decode(record: Record<'_>, header: &Header, start: f64) -> MsResult<RawIntegration> {
    let array = array(record, header)?;
    let midpoint = start + 0.5 * header.interval;
    let state = if record.text(32, 8)? == "ON" {
        ("OBSERVE_TARGET#ON_SOURCE,POSITION_SWITCH", 1)
    } else {
        ("OBSERVE_TARGET#OFF_SOURCE,POSITION_SWITCH", 2)
    };
    Ok(RawIntegration {
        row: Integration {
            time: midpoint,
            interval: header.interval,
            scan: record.i32(4),
            antenna: array.beam,
            field: 0,
            feed: 0,
            window: array.window,
            basis: Basis::Linear,
            state,
            direction: Direction {
                position: [record.f64(56), record.f64(64)],
                rate: [0.; 2],
            },
            weather: Some(Weather([
                (f64::from(record.f32(124)) + 273.15) as f32,
                record.f32(128),
                record.f32(132),
                record.f32(136),
                record.f32(140),
            ])),
        },
        plane: Plane {
            index: array.plane,
            data: record.spectrum(header.n_channels, array.scale),
            flag: None,
            flag_row: false,
            system_temperature: vec![record.f32(148)],
            calibration_temperature: Vec::new(),
        },
    })
}

fn array(record: Record<'_>, header: &Header) -> MsResult<Array> {
    let number = record
        .text(120, 4)?
        .get(1..)
        .and_then(|digits| digits.parse::<usize>().ok())
        .and_then(|id| id.checked_sub(1))
        .ok_or_else(|| invalid("scan array number"))?;
    let array = header
        .arrays
        .get(number)
        .and_then(|array| *array)
        .ok_or_else(|| invalid("scan array number"))?;
    let identifier = record.text(264, 8)?;
    if identifier
        .get(..2)
        .and_then(|value| value.parse::<i32>().ok())
        != Some(array.beam + 1)
        || identifier.get(2..4) != Some(array.name)
        || identifier
            .get(4..6)
            .and_then(|value| value.parse::<i32>().ok())
            != Some(array.window + 1)
    {
        return Err(invalid("inconsistent scan array mapping"));
    }
    Ok(array)
}

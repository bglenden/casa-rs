// SPDX-License-Identifier: LGPL-3.0-or-later
//! Pointing and first-feed geometry for the legacy MosaicFT visibility buffer.

use std::collections::BTreeMap;

use casa_core::ArrayValue;
use casa_core::measures::direction::MDirection;
use casa_core::measures::ephemeris::offset_true_angle;
use casa_ms::{MeasurementSet, MsError, MsResult, SubtableId};
use ndarray::IxDyn;

use super::{GainSolveError, SelectedSolveRow, field_direction, get_i32, read_error};
use casa_ms::derived::engine::MsCalEngine;

pub(super) struct MosaicPointing {
    pub(super) buffer: usize,
    pub(super) antenna: usize,
    pub(super) second_antenna: i32,
    pub(super) epoch: f64,
    pub(super) direction: MDirection,
}

#[derive(Default)]
pub(super) struct FeedOffsets(BTreeMap<(i32, i32), Vec<FeedOffset>>);

struct FeedOffset {
    spw: i32,
    time: f64,
    half_interval: f64,
    sky: [f64; 2],
    receptor_angle: f64,
    alt_az: bool,
}

impl FeedOffsets {
    pub(super) fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    pub(super) fn load(ms: &MeasurementSet) -> MsResult<Self> {
        let mut offsets = Self::default();
        if ms.subtable(SubtableId::Feed).is_none() {
            return Ok(offsets);
        }
        let feeds = ms.feed()?;
        let antenna = ms.antenna()?;
        for row in 0..feeds.row_count() {
            let ArrayValue::Float64(offset) = feeds.array(row, "BEAM_OFFSET")? else {
                return Err(MsError::InvalidInput(
                    "FEED BEAM_OFFSET must contain doubles".into(),
                ));
            };
            let at = |coordinate| offset.get(IxDyn(&[coordinate, 0])).copied().unwrap_or(0.);
            let sky = [at(0), at(1)];
            let id = feeds.i32(row, "ANTENNA_ID")?;
            let feed = feeds.i32(row, "FEED_ID")?;
            let receptor_angle = match feeds.array(row, "RECEPTOR_ANGLE")? {
                ArrayValue::Float64(angles) => angles.iter().next().copied().unwrap_or(0.),
                _ => 0.,
            };
            let alt_az =
                id >= 0 && matches!(antenna.mount(id as usize)?.as_str(), "ALT-AZ" | "alt-az");
            offsets.0.entry((id, feed)).or_default().push(FeedOffset {
                spw: feeds.i32(row, "SPECTRAL_WINDOW_ID")?,
                time: feeds.f64(row, "TIME")?,
                half_interval: feeds.f64(row, "INTERVAL")? / 2.,
                sky,
                receptor_angle,
                alt_az,
            });
        }
        if offsets.0.values().flatten().all(|row| row.sky == [0., 0.]) {
            offsets.0.clear();
        }
        Ok(offsets)
    }

    pub(super) fn direction(
        &self,
        ms: &MeasurementSet,
        engine: &MsCalEngine,
        first: &SelectedSolveRow,
        measured: Option<MDirection>,
    ) -> Result<MosaicPointing, GainSolveError> {
        let antenna = usize::try_from(first.antenna1).map_err(super::model_error)?;
        let epoch = first.time_seconds;
        let mut direction = match measured {
            Some(direction) => direction,
            None => field_direction(ms, engine, first.field_id as usize, epoch)
                .map_err(|source| read_error(ms, source))?,
        };
        if !self.0.is_empty() {
            let feed = get_i32(ms.main_table(), first.row_index, "FEED1")?;
            if let Some(offset) = self.resolve(first, feed)
                && offset.sky != [0., 0.]
            {
                let [x, y] = offset.sky;
                let [x, y] = if offset.alt_az {
                    let angle = engine
                        .feed_parallactic_angle(epoch, first.field_id as usize, antenna)
                        .map_err(|source| read_error(ms, source))?
                        + offset.receptor_angle;
                    let (sin, cos) = angle.sin_cos();
                    [cos * x - sin * y, sin * x + cos * y]
                } else {
                    [x, y]
                };
                direction = offset_true_angle(&direction, [-x, y]).map_err(super::model_error)?;
            }
        }
        Ok(MosaicPointing {
            buffer: first.row_index,
            antenna,
            second_antenna: first.antenna2,
            epoch,
            direction,
        })
    }

    fn resolve(&self, first: &SelectedSolveRow, feed: i32) -> Option<&FeedOffset> {
        let offsets = self.0.get(&(first.antenna1, feed))?;
        let select = |row: &&FeedOffset| row.spw == first.data_spw_id || row.spw == -1;
        let priority = |row: &FeedOffset| {
            let distance =
                ((row.time - first.time_seconds).abs() - row.half_interval.max(0.)).max(0.);
            (row.spw != first.data_spw_id, distance)
        };
        offsets.iter().filter(select).min_by(|a, b| {
            let (a_specific, a_distance) = priority(a);
            let (b_specific, b_distance) = priority(b);
            a_specific
                .cmp(&b_specific)
                .then(a_distance.total_cmp(&b_distance))
        })
    }
}

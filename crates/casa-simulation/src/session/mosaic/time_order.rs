// SPDX-License-Identifier: LGPL-3.0-or-later
//! CASA's TIME-sorted visibility buffers for physically unordered input.
use std::cmp::Ordering;

use casa_imaging_runtime::{Admitted, Demand, admit};

use super::{BufferClock, FrameKey, MosaicGeometry};
use crate::session::Simulator;
use crate::session::error::{SimulatorError, SimulatorResult};
use crate::session::from_ms::polynomial::PhaseCentres;
use crate::session::observe::ScanRows;
use crate::session::selection::RowHeader;

#[derive(Clone, Copy)]
pub(super) struct BufferStart {
    key: FrameKey,
    header: RowHeader,
}

impl BufferStart {
    fn compare(&self, key: FrameKey, time: f64, row: u64) -> Ordering {
        self.key
            .cmp(&key)
            .then_with(|| self.header.time_s.total_cmp(&time))
            .then_with(|| self.header.row.cmp(&row))
    }
}

pub(super) struct TimeOrder {
    starts: Admitted<Vec<BufferStart>>,
}

impl TimeOrder {
    pub(super) fn sample(
        &self,
        key: FrameKey,
        row: u64,
        time: f64,
    ) -> SimulatorResult<(f64, usize)> {
        let index = self
            .starts
            .value
            .partition_point(|start| start.compare(key, time, row).is_le());
        let start = index
            .checked_sub(1)
            .and_then(|i| self.starts.value.get(i))
            .filter(|start| start.key == key)
            .ok_or_else(|| SimulatorError::raised("missing TIME-sorted mosaic buffer"))?;
        Ok((start.header.time_s, start.header.antennas[0]))
    }
}

impl MosaicGeometry {
    /// Ordered observations retain only their per-context cursor. For unordered
    /// inputs, sort admitted scalar headers once and retain one descriptor per
    /// CASA buffer, never visibility payloads or an index for every row.
    pub(in crate::session) fn set_time_order(
        &mut self,
        session: &Simulator,
        centres: Option<&PhaseCentres>,
    ) -> SimulatorResult<()> {
        if !self.unsorted_times(session)? {
            return Ok(());
        }
        let rows = centres.map_or(0, super::super::from_ms::polynomial::selected_rows);
        let contexts = centres.map_or(0, |p| p.value.len());
        let capacity = rows / self.rows + contexts;
        let resources = &session.options.resources;
        let mut reservation = admit(
            resources.host(),
            resources.policy(),
            &Demand {
                phase: "sorting unordered mosaic buffer headers",
                memory: ((rows + capacity) * size_of::<BufferStart>()) as u64,
            },
        )
        .map_err(SimulatorError::admission)?;
        let mut headers = Vec::with_capacity(rows);
        session.visit_selected_headers(session.data_plan.selection(), |scan, header| {
            headers.push(BufferStart {
                key: (scan.array_id, scan.field_id, scan.data_description_id),
                header,
            });
            Ok(())
        })?;
        headers.sort_unstable_by(|a, b| a.compare(b.key, b.header.time_s, b.header.row));
        let mut starts = Vec::with_capacity(capacity);
        let mut key = None;
        let mut count = 0;
        for header in headers {
            if key != Some(header.key) {
                key = Some(header.key);
                count = 0;
            }
            if count % self.rows == 0 {
                starts.push(header);
            }
            count += 1;
        }
        reservation.retain((starts.capacity() * size_of::<BufferStart>()) as u64);
        self.time_order = Some(TimeOrder {
            starts: Admitted {
                value: starts,
                reservation,
            },
        });
        Ok(())
    }

    fn unsorted_times(&mut self, session: &Simulator) -> SimulatorResult<bool> {
        let buffers = self
            .buffers
            .get_mut()
            .map_err(|e| SimulatorError::raised(e.to_string()))?;
        let mut unordered = false;
        for (index, scan) in session.scans.iter().enumerate() {
            if !session.data_plan.selection().contains_scan(scan) {
                continue;
            }
            let key = (scan.array_id, scan.field_id, scan.data_description_id);
            let Ok(at) = buffers.binary_search_by_key(&key, |(key, _)| *key) else {
                continue;
            };
            let (first, last) = match &scan.rows {
                ScanRows::OnDisk(_) => {
                    let span = session.opened.as_ref().expect("opened").scan_times_s[index];
                    (span[0], span[1])
                }
                ScanRows::Planned(rows) => {
                    let mut times = rows.integration_times_s();
                    let Some(first) = times.next() else {
                        continue;
                    };
                    (first, times.last().unwrap_or(first))
                }
            };
            let clock = &mut buffers[at].1;
            unordered |= clock.last_row.is_some() && first < clock.epoch_s;
            clock.epoch_s = last;
            clock.last_row = Some(0);
        }
        for (_, clock) in buffers {
            *clock = BufferClock::default();
        }
        Ok(unordered)
    }
}

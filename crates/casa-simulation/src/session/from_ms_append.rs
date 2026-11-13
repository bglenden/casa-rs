// SPDX-License-Identifier: LGPL-3.0-or-later
//! Reconstruct append bookkeeping from an opened MS; keep its stored cells.
use super::*;
use crate::session::feeds::Feed;
use crate::session::observe::{PlannedRows, ScanRows, StateSpec};
use casa_types::ArrayValue;

impl Simulator {
    pub(in crate::session) fn prepare_existing_append(&mut self) -> SimulatorResult<()> {
        if self.opened.is_none() || !self.feeds.is_empty() {
            return Ok(());
        }
        let ms = MeasurementSet::open(self.path()).map_err(read_error("append metadata"))?;
        let table = ms.feed().map_err(read_error("FEED"))?;
        let mut feeds = Vec::with_capacity(table.row_count());
        for row in 0..table.row_count() {
            let ArrayValue::Float64(offset) = table
                .array(row, "BEAM_OFFSET")
                .map_err(read_error("FEED"))?
            else {
                return Err(SimulatorError::raised("FEED BEAM_OFFSET is not real"));
            };
            let ArrayValue::String(polarization) = table
                .array(row, "POLARIZATION_TYPE")
                .map_err(read_error("FEED"))?
            else {
                return Err(SimulatorError::raised(
                    "FEED POLARIZATION_TYPE is not a string array",
                ));
            };
            if offset.shape() != [2, 2] || polarization.len() != 2 {
                return Err(SimulatorError::raised("append requires two-receptor feeds"));
            }
            let receptor = |index| match polarization[[index]].as_str() {
                "X" => Ok("X"),
                "Y" => Ok("Y"),
                "R" => Ok("R"),
                "L" => Ok("L"),
                _ => Err(SimulatorError::raised("unrecognized feed receptor")),
            };
            feeds.push(Feed {
                antenna_id: table.i32(row, "ANTENNA_ID").map_err(read_error("FEED"))?,
                feed_id: table.i32(row, "FEED_ID").map_err(read_error("FEED"))?,
                offset_rad: [offset[[0, 0]], offset[[1, 0]]],
                polarization: [receptor(0)?, receptor(1)?],
            });
        }
        let table = ms.state().map_err(read_error("STATE"))?;
        let states = (0..table.row_count())
            .map(|row| {
                Ok(StateSpec {
                    sig: table.bool(row, "SIG").map_err(read_error("STATE"))?,
                    reference: table.bool(row, "REF").map_err(read_error("STATE"))?,
                    cal: table.f64(row, "CAL").map_err(read_error("STATE"))?,
                    load: table.f64(row, "LOAD").map_err(read_error("STATE"))?,
                    sub_scan: table.i32(row, "SUB_SCAN").map_err(read_error("STATE"))?,
                    obs_mode: table.string(row, "OBS_MODE").map_err(read_error("STATE"))?,
                })
            })
            .collect::<SimulatorResult<Vec<_>>>()?;
        self.feeds = feeds;
        self.states = states;
        let main = ms
            .main_table()
            .column_accessor("ARRAY_ID")
            .map_err(|e| SimulatorError::raised(e.to_string()))?;
        let mut maximum = None;
        for first in (0..ms.row_count()).step_by(512) {
            let rows: Vec<_> = (first..ms.row_count().min(first + 512)).collect();
            for value in main
                .scalar_cells_owned_for_rows(&rows)
                .map_err(|e| SimulatorError::raised(e.to_string()))?
                .into_iter()
                .flatten()
            {
                if let casa_types::ScalarValue::Int32(value) = value {
                    maximum = Some(maximum.map_or(value, |old: i32| old.max(value)));
                }
            }
        }
        self.existing_array_id = maximum.unwrap_or(0);
        Ok(())
    }

    /// Render the new geometry and use the existing patch engine for all DATA steps.
    pub(in crate::session) fn append_existing(&self) -> SimulatorResult<()> {
        let parent = self
            .path()
            .parent()
            .ok_or_else(|| SimulatorError::raised("MS has no parent directory"))?;
        let scratch = tempfile::Builder::new()
            .prefix(".simulation-append-")
            .tempdir_in(parent)
            .map_err(|e| SimulatorError::raised(e.to_string()))?;
        let old_scans = self
            .scans
            .iter()
            .take_while(|scan| scan.on_disk().is_some())
            .count();
        let mut segment = self.append_segment(&scratch.path().join("segment.ms"), old_scans)?;
        let steps = std::mem::take(&mut segment.data_plan);
        crate::write::write_measurement_set(&segment)?;
        segment.add_append_scratch(self.opened.as_ref().expect("opened input"))?;
        let mut first = 0;
        let mut times = Vec::with_capacity(segment.scans.len());
        for scan in &mut segment.scans {
            let count = scan.row_count();
            let integration_times: Vec<_> = scan
                .planned()
                .expect("segment geometry")
                .integration_times_s()
                .collect();
            times.push(integration_times);
            scan.rows = ScanRows::OnDisk(first..first + count);
            first += count;
        }
        segment.opened = Some(OpenedMs {
            model_data: self.opened.as_ref().expect("opened").model_data,
            corrected_data: self.opened.as_ref().expect("opened").corrected_data,
            scan_times_s: times
                .iter()
                .map(|times| {
                    [
                        times.first().copied().unwrap_or(0.),
                        times.last().copied().unwrap_or(0.),
                    ]
                })
                .collect(),
            integration_times_s: times,
        });
        segment.data_plan = steps;
        crate::write::patch_measurement_set(&segment)?;

        let replacement = casa_ms::observation_io::creation::Replacement::hold(self.path())
            .map_err(|e| SimulatorError::raised(e.to_string()))?;
        let stored = if self.data_plan.is_empty() {
            self.path().to_path_buf()
        } else {
            let copied = scratch.path().join("old.ms");
            crate::write::append::copy_tree(self.path(), &copied)?;
            let mut old = self.append_segment(&copied, old_scans)?;
            old.scans = self.scans[..old_scans].to_vec();
            old.opened = self.opened.clone();
            old.noise_row_offset = 0;
            crate::write::patch_measurement_set(&old)?;
            copied
        };
        let output = scratch.path().join("merged.ms");
        crate::write::append::merge(&stored, segment.path(), &output, &self.options.resources)?;
        replacement
            .publish(&output)
            .map_err(|e| SimulatorError::raised(e.to_string()))?;
        Ok(())
    }

    fn append_segment(&self, path: &Path, old: usize) -> SimulatorResult<Self> {
        let mut segment = Self::open(path, self.options.clone());
        macro_rules! copied { ($($field:ident),+) => { $(segment.$field.clone_from(&self.$field);)+ }; }
        copied!(
            telescope,
            antennas,
            feeds,
            feeds_set,
            spectral_windows,
            polarizations,
            data_descriptions,
            description_memory,
            fields,
            times,
            elevation_limit_rad,
            shadow_fraction_limit,
            autocorrelation_weight,
            observations,
            states,
            scans,
            padding,
            w_projection_planes,
            mosaic,
            single_dish,
            voltage_pattern,
            squint,
            data_plan
        );
        let mut empty: PlannedRows = self
            .scans
            .get(old)
            .and_then(|s| s.planned())
            .ok_or_else(|| SimulatorError::raised("append has no planned scan"))?
            .clone();
        empty.integration_count = 0;
        for scan in &mut segment.scans[..old] {
            scan.rows = ScanRows::Planned(empty.clone());
        }
        segment.noise_row_offset = self.scans[..old].iter().map(|s| s.row_count() as u64).sum();
        Ok(segment)
    }

    fn add_append_scratch(&self, original: &OpenedMs) -> SimulatorResult<()> {
        if !original.model_data && !original.corrected_data {
            return Ok(());
        }
        let shape = self
            .scans
            .iter()
            .find(|scan| scan.row_count() > 0)
            .map(|scan| {
                [
                    self.correlations_for_scan(scan).len(),
                    self.spectral_windows[scan.spectral_window_id].channel_count,
                ]
            })
            .ok_or_else(|| SimulatorError::raised("appended MS has no shape"))?;
        crate::write::append::scratch(
            self.path(),
            [original.model_data, original.corrected_data],
            &shape,
            &self.options.resources,
        )
    }
}

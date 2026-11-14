// SPDX-License-Identifier: LGPL-3.0-or-later
//! Optional Tsys tables are snapshots of the noise scales at `setnoise`.
//! They share the corruption models and the ordinary calibration-table writer.
use std::path::PathBuf;

use crate::corrupt::tsys_snapshot::{Chunk, TsysSnapshot};
use casa_calibration::{SimulatedJones, write_simulated_jones_table};
use casa_imaging_runtime::{Demand, admit};
use casa_ms::MeasurementSet;
use casa_tables::{Table, TableOptions};
use casa_types::ScalarValue;

use super::Simulator;
use super::corruption::calibration_table_root;
use super::error::{SimulatorError, SimulatorResult};
use crate::corrupt::jones::TermBytes;
use crate::corrupt::{NoiseLevel, WindowNoise};
use crate::write::ArrayGeometry;

#[derive(Debug)]
pub(super) struct NoiseTable {
    pub(super) path: PathBuf,
    pub(super) snapshot: std::sync::Arc<TsysSnapshot>,
}

fn raised(error: impl std::fmt::Display) -> SimulatorError {
    SimulatorError::raised(format!("Tsys simulation table: {error}"))
}

impl Simulator {
    pub(super) fn prepare_noise_table(
        &self,
        level: &NoiseLevel,
        name: &str,
        domain: &crate::corrupt::noise_domain::NoiseDomain,
    ) -> SimulatorResult<Option<NoiseTable>> {
        if matches!(
            level,
            NoiseLevel::Simple(_)
                | NoiseLevel::Fixed(_)
                | NoiseLevel::ScaledFixed { .. }
                | NoiseLevel::Snapshot(_)
        ) {
            return Ok(None);
        }
        let Some(root) = calibration_table_root(name) else {
            return Ok(None);
        };
        let path = self
            .table_path(&format!("{root}.T.cal"))?
            .expect("nonempty table name");
        let snapshot = self.noise_snapshot(level, domain)?;
        Ok(Some(NoiseTable { path, snapshot }))
    }

    pub(super) fn noise_snapshot(
        &self,
        level: &NoiseLevel,
        domain: &crate::corrupt::noise_domain::NoiseDomain,
    ) -> SimulatorResult<std::sync::Arc<TsysSnapshot>> {
        let scans = domain.scans;
        let kind = if matches!(level, NoiseLevel::TsysManual(_)) {
            SimulatedJones::ScalarGain
        } else {
            SimulatedJones::Troposphere
        };
        let (counts, _) = self.integration_counts_before(scans);
        let chunks = counts.iter().sum::<usize>();
        let antennas = self.antennas.len();
        let values = counts
            .iter()
            .enumerate()
            .map(|(window, &count)| {
                count
                    * antennas
                    * if kind.depends_on_frequency() {
                        self.spectral_windows[window].channel_count
                    } else {
                        1
                    }
            })
            .sum::<usize>();
        let kept = (chunks * (size_of::<Chunk>() + size_of::<usize>())
            + values * size_of::<f32>()
            + 1024) as u64;
        let bytes = TermBytes {
            kept,
            forming: (antennas * 512) as u64,
        };
        let reservation = self.admit_term(bytes)?;
        let mut table = TsysSnapshot {
            metadata: domain.metadata(),
            kind,
            antennas,
            chunks: Vec::with_capacity(chunks),
            values: Vec::with_capacity(values),
            order: Vec::with_capacity(chunks),
            reservation,
        };
        self.fill_noise_table(level, scans, &mut table)?;
        table.finish_index();
        table.reservation.retain(kept);
        Ok(std::sync::Arc::new(table))
    }

    fn fill_noise_table(
        &self,
        level: &NoiseLevel,
        scans: usize,
        table: &mut TsysSnapshot,
    ) -> SimulatorResult<()> {
        let geometry = ArrayGeometry::new(
            &self.telescope,
            &self.antennas,
            self.options.measures.clone(),
        )?;
        let opened = self
            .opened
            .as_ref()
            .map(|_| Table::open(TableOptions::new(self.path())).map_err(raised))
            .transpose()?;
        for (index, scan) in self.scans.iter().take(scans).enumerate() {
            let exposure = if let Some(planned) = scan.planned() {
                planned.integration_s
            } else {
                let row = scan.on_disk().expect("opened scan").start;
                let ScalarValue::Float64(value) = opened
                    .as_ref()
                    .expect("opened MS")
                    .cell_accessor(row, "EXPOSURE")
                    .map_err(raised)?
                    .scalar()
                    .map_err(raised)?
                else {
                    return Err(raised("EXPOSURE must be Float64"));
                };
                *value
            };
            let window = &self.spectral_windows[scan.spectral_window_id];
            let noise_window = WindowNoise {
                spectral_window_id: scan.spectral_window_id,
                reference_frequency_hz: window.reference_frequency_hz,
                channel_bandwidth_hz: window.total_bandwidth_hz.abs() / window.channel_count as f64,
            };
            let channels = if table.kind.depends_on_frequency() {
                window.channel_count
            } else {
                1
            };
            for time_s in self.integration_times_s(index) {
                let airmasses =
                    geometry.airmasses(scan.phase_center_rad, time_s, table.antennas)?;
                let offset = table.values.len();
                table.values.resize(offset + table.antennas * channels, 0.);
                for (antenna, &airmass) in airmasses.iter().enumerate() {
                    let diameter = self.antennas[antenna].diameter_m;
                    let output = &mut table.values
                        [offset + antenna * channels..offset + (antenna + 1) * channels];
                    match level {
                        NoiseLevel::TsysManual(model) => {
                            output[0] = model
                                .for_window(&noise_window)
                                .antenna_gain(diameter, airmass, exposure)
                        }
                        NoiseLevel::TsysAtm(model) => model.antenna_gains(
                            model.window(scan.spectral_window_id),
                            (diameter, airmass),
                            (noise_window.channel_bandwidth_hz, exposure),
                            output,
                        ),
                        NoiseLevel::Simple(_)
                        | NoiseLevel::Fixed(_)
                        | NoiseLevel::ScaledFixed { .. }
                        | NoiseLevel::Snapshot(_) => {
                            unreachable!("simple noise writes no T table")
                        }
                    }
                }
                table.chunks.push(Chunk {
                    time_s,
                    field: scan.field_id as i32,
                    window: scan.spectral_window_id as i32,
                    channels,
                    offset,
                });
            }
        }
        Ok(())
    }

    pub(super) fn write_noise_tables(&self) -> SimulatorResult<()> {
        let Some(memory) = self
            .noise_tables
            .iter()
            .map(|table| table.snapshot.write_bytes())
            .max()
        else {
            return Ok(());
        };
        let resources = &self.options.resources;
        let _building = admit(
            resources.host(),
            resources.policy(),
            &Demand {
                phase: "writing a Tsys simulation table",
                memory,
            },
        )
        .map_err(SimulatorError::admission)?;
        let ms = MeasurementSet::open(self.path()).map_err(raised)?;
        for table in &self.noise_tables {
            write_simulated_jones_table(
                &ms,
                &table.path,
                table.snapshot.kind,
                table.snapshot.solutions(),
                Some(table.snapshot.metadata),
            )
            .map_err(raised)?;
        }
        Ok(())
    }
}

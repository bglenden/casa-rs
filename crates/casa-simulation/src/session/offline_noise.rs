// SPDX-License-Identifier: LGPL-3.0-or-later
//! Prepare an additive noise field once; optional A Noise output uses that field.
use super::corruption::calibration_table_root;
use super::error::{SimulatorError, SimulatorResult};
use super::plan::NoiseTerm;
use super::{NoiseSpec, Simulator};
use crate::corrupt::NoiseLevel;
use crate::corrupt::jones::TermBytes;
use crate::corrupt::offline::OfflineNoise;
use casa_calibration::{simulated_table_bytes, write_simulated_noise_table};
use casa_imaging_runtime::{Demand, admit};
use casa_ms::MeasurementSet;
use std::sync::Arc;

impl Simulator {
    pub(super) fn set_offline_noise(&mut self, spec: &NoiseSpec) -> SimulatorResult<()> {
        self.require_rows()?;
        let path = calibration_table_root(&spec.table)
            .map(|root| self.table_path(&format!("{root}.A.cal")))
            .transpose()?
            .flatten();
        let _dimensions = self.admit_term(TermBytes {
            kept: 0,
            forming: (self.spectral_windows.len() * 32 + 1024) as u64,
        })?;
        let (mut counts, _) = self.integration_counts();
        counts.resize(self.spectral_windows.len(), 0);
        let channels = self
            .spectral_windows
            .iter()
            .map(|w| w.channel_count)
            .collect::<Vec<_>>();
        let kept = OfflineNoise::bytes(self.antennas.len(), &counts, &channels);
        let reservation = self.admit_term(TermBytes { kept, forming: 0 })?;
        let mut field = OfflineNoise::new(
            self.antennas.len(),
            self.scans.len(),
            &counts,
            &channels,
            reservation,
        );
        for (index, scan) in self.scans.iter().enumerate() {
            for time in self.integration_times_s(index) {
                field.push(
                    scan.spectral_window_id,
                    time,
                    scan.field_id as i32,
                    self.seed,
                    spec.simple_noise_jy as f32,
                );
            }
        }
        let field = Arc::new(field.sorted());
        if let Some(path) = path {
            self.offline_tables.push((path, Arc::clone(&field)));
        }
        self.noise_terms.push(NoiseTerm {
            seed: self.seed,
            level: NoiseLevel::Fixed(field),
            applications: 0,
        });
        Ok(())
    }

    pub(super) fn write_offline_tables(&self) -> SimulatorResult<()> {
        let Some(memory) = self
            .offline_tables
            .iter()
            .map(|(_, term)| simulated_table_bytes(term.table_rows(), term.table_values()))
            .max()
        else {
            return Ok(());
        };
        let resources = &self.options.resources;
        let _building = admit(
            resources.host(),
            resources.policy(),
            &Demand {
                phase: "writing an additive simulation noise table",
                memory,
            },
        )
        .map_err(SimulatorError::admission)?;
        let ms = MeasurementSet::open(self.path())
            .map_err(|error| SimulatorError::raised(error.to_string()))?;
        for (path, field) in &self.offline_tables {
            write_simulated_noise_table(&ms, path, field.solutions())
                .map_err(|error| SimulatorError::raised(format!("write A Noise table: {error}")))?;
        }
        Ok(())
    }
}

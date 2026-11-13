// SPDX-License-Identifier: LGPL-3.0-or-later
//! CASA's source-defined debugging ramp at integrations or custom time chunks.
use super::error::SimulatorResult;
use super::troposphere::{check_numbers, not_formed};
use super::{Simulator, TroposphereSpec};
use crate::corrupt::jones::{Chunks, TermBytes, tropospheric};
use crate::corrupt::troposphere::TroposphericPhase;
use crate::corrupt::tsys_atm::single;

impl Simulator {
    pub(super) fn set_test_trop(&mut self, spec: &TroposphereSpec) -> SimulatorResult<()> {
        check_numbers(spec, false)?;
        let table = self.table_path(&spec.table)?;
        self.require_rows()?;
        let pwv = single(spec.water_vapour_mm) as f32;
        if pwv <= 0. {
            return Err(not_formed(
                "AtmCorruptor attempted initialization with undefined PWV",
            ));
        }
        let simint = single(spec.simint_s);
        let counts = if simint > 0. {
            let mut counts = vec![0; self.spectral_windows.len()];
            self.test_trop_times(simint, |window, _, _| counts[window] += 1);
            counts
        } else {
            self.integration_counts().0
        };
        let integrations = counts.iter().sum::<usize>();
        let antennas = self.antennas.len();
        let channels = self
            .spectral_windows
            .iter()
            .map(|w| w.channel_count)
            .collect::<Vec<_>>();
        let bytes = TermBytes::troposphere(
            (&counts, &channels),
            (integrations, antennas),
            self.wet_delays_building_bytes(),
        );
        let reservation = self.admit_term(bytes)?;
        let delays = self.wet_delays(f64::from(pwv) * 1e-3)?;
        // AtmosCorruptor::initialize(Int), including its deliberate debug-only
        // factor of mean PWV. Seed, beta and deltapwv do not enter this ramp.
        let mut excess = Vec::with_capacity(integrations * antennas);
        for i in 0..integrations {
            excess.extend((0..antennas).map(|a| {
                (i as f32 / integrations as f32 + a as f32 / antennas as f32) * pwv * 10.
            }));
        }
        let chunks = if simint > 0. {
            let mut chunks = Chunks::with_capacity(&counts);
            self.test_trop_times(simint, |window, field, time| {
                chunks.push(window, time, field);
            });
            chunks.sorted()
        } else {
            self.integration_chunks(&counts)
        };
        let term = tropospheric(
            antennas,
            (chunks, None),
            TroposphericPhase::new(excess, delays),
        );
        self.hold_term(term, (reservation, bytes), table);
        Ok(())
    }

    /// sizeUpSim's uncombined VisIterator chunks, reset at every scan boundary.
    /// A positive interval starts at the first integration of each chunk; keep
    /// the midpoint of its first and last integration as setSimulate does.
    fn test_trop_times(&self, interval: f64, mut emit: impl FnMut(usize, usize, f64)) {
        let mut previous = None;
        let mut pending: Option<(usize, usize, f64, f64)> = None;
        for (index, scan) in self.scans.iter().enumerate() {
            // Stored integration spans are internal POINTING partitions.
            // CASA resets custom solution chunks at physical scan boundaries.
            let key = scan.on_disk().map(|_| {
                (
                    scan.array_id,
                    scan.observation_id,
                    scan.scan_number,
                    scan.field_id,
                    scan.data_description_id,
                )
            });
            if key.is_none() || key != previous {
                if let Some((window, field, first, last)) = pending.take() {
                    emit(window, field, 0.5 * (first + last));
                }
            }
            previous = key;
            for time in self.integration_times_s(index) {
                let (_, _, first, last) =
                    pending.get_or_insert((scan.spectral_window_id, scan.field_id, time, time));
                if time - *first >= interval {
                    emit(
                        scan.spectral_window_id,
                        scan.field_id,
                        0.5 * (*first + *last),
                    );
                    *first = time;
                }
                *last = time;
            }
        }
        if let Some((window, field, first, last)) = pending {
            emit(window, field, 0.5 * (first + last));
        }
    }
}

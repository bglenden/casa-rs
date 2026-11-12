// SPDX-License-Identifier: LGPL-3.0-or-later
//! CASA's source-defined debugging ramp, sampled at the observed integrations.
use super::error::{SimulatorError, SimulatorResult};
use super::troposphere::{check_numbers, not_formed};
use super::{Simulator, TroposphereSpec};
use crate::corrupt::jones::{TermBytes, tropospheric};
use crate::corrupt::troposphere::TroposphericPhase;
use crate::corrupt::tsys_atm::single;

impl Simulator {
    pub(super) fn set_test_trop(&mut self, spec: &TroposphereSpec) -> SimulatorResult<()> {
        check_numbers(spec, false)?;
        let table = self.table_path(&spec.table)?;
        self.require_rows()?;
        if spec.simint_s > 0. {
            return Err(SimulatorError::not_yet_implemented(
                "settrop(mode='test') with a custom positive simint is not yet implemented (#1292)",
            ));
        }
        let pwv = single(spec.water_vapour_mm) as f32;
        if pwv <= 0. {
            return Err(not_formed(
                "AtmCorruptor attempted initialization with undefined PWV",
            ));
        }
        let (counts, _) = self.integration_counts();
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
        let term = tropospheric(
            antennas,
            (self.integration_chunks(&counts), None),
            TroposphericPhase::new(excess, delays),
        );
        self.hold_term(term, (reservation, bytes), table);
        Ok(())
    }
}

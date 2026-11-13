// SPDX-License-Identifier: LGPL-3.0-or-later
//! Noise domains and original T/Tf solutions for subsequent observations.
use super::{
    Simulator,
    error::{SimulatorError, SimulatorResult},
};
use crate::corrupt::{
    NoiseLevel,
    jones::{HeldTerm, TermBytes},
    noise_domain::NoiseDomain,
};
use std::sync::Arc;

impl Simulator {
    pub(super) fn noise_domain(
        &self,
        on_the_fly_simple: bool,
    ) -> SimulatorResult<Arc<NoiseDomain>> {
        let bytes = (size_of::<NoiseDomain>() + 32 + self.spectral_windows.len()) as u64;
        let reservation = self.admit_term(TermBytes {
            kept: bytes,
            forming: 0,
        })?;
        let mut windows = vec![on_the_fly_simple; self.spectral_windows.len()];
        if !on_the_fly_simple {
            for scan in &self.scans {
                windows[scan.spectral_window_id] = true;
            }
        }
        Ok(Arc::new(NoiseDomain {
            scans: self.scans.len(),
            antennas: self.antennas.len(),
            fields: self.fields.len(),
            windows,
            _reservation: Some(reservation),
        }))
    }

    pub(super) fn prepare_later_noise(
        &mut self,
        jones: &[Arc<HeldTerm>],
        additive: &[crate::corrupt::NoiseApplication],
    ) -> SimulatorResult<()> {
        for index in 0..self.noise_terms.len() {
            let term = &self.noise_terms[index];
            if self.antennas.len() != term.domain.antennas {
                return Err(SimulatorError::not_yet_implemented(
                    "noise after adding antennas is not yet implemented (#1293)",
                ));
            }
            let physical = match &term.level {
                NoiseLevel::ScaledFixed { scale, .. } => &**scale,
                level => level,
            };
            let needs_snapshot =
                matches!(physical, NoiseLevel::TsysManual(_) | NoiseLevel::TsysAtm(_))
                    && term.snapshot.is_none()
                    && self.scans.iter().skip(term.domain.scans).any(|scan| {
                        term.domain.covers(scan.spectral_window_id)
                            && additive
                                .iter()
                                .all(|term| term.covers(scan.spectral_window_id))
                            && jones
                                .iter()
                                .all(|term| term.covers(scan.spectral_window_id))
                    });
            if needs_snapshot {
                let snapshot = self.noise_snapshot(physical, &term.domain)?;
                self.noise_terms[index].snapshot = Some(snapshot);
            }
        }
        Ok(())
    }
}

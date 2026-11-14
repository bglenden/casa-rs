// SPDX-License-Identifier: LGPL-3.0-or-later
//! The observation metadata a noise term was initialized against.
use casa_imaging_runtime::Reservation;

#[derive(Debug)]
pub(crate) struct NoiseDomain {
    pub(crate) scans: usize,
    pub(crate) antennas: usize,
    pub(crate) fields: usize,
    pub(crate) windows: Vec<bool>,
    pub(crate) _reservation: Option<Reservation>,
}

impl NoiseDomain {
    pub(crate) fn covers(&self, window: usize) -> bool {
        self.windows
            .get(window)
            .copied()
            .unwrap_or(self.windows.is_empty())
    }

    pub(crate) fn metadata(&self) -> casa_calibration::SimulatedTableMetadata {
        casa_calibration::SimulatedTableMetadata {
            fields: self.fields,
            spectral_windows: self.windows.len(),
        }
    }

    #[cfg(test)]
    pub(crate) fn unrestricted() -> std::sync::Arc<Self> {
        std::sync::Arc::new(Self {
            scans: usize::MAX,
            antennas: usize::MAX,
            fields: usize::MAX,
            windows: Vec::new(),
            _reservation: None,
        })
    }
}

impl PartialEq for NoiseDomain {
    fn eq(&self, other: &Self) -> bool {
        self.scans == other.scans
            && self.fields == other.fields
            && self.antennas == other.antennas
            && self.windows == other.windows
    }
}

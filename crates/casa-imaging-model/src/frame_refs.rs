// SPDX-License-Identifier: LGPL-3.0-or-later
//! The `casa-types` measures frame of each imaging frame.

use casa_types::measures::direction::DirectionRef;
use casa_types::measures::frequency::FrequencyRef;

use crate::geometry::{DirectionFrame, FrequencyFrame};

impl DirectionFrame {
    /// The measures direction frame this is.
    #[must_use]
    pub const fn direction_ref(self) -> DirectionRef {
        match self {
            Self::Icrs => DirectionRef::ICRS,
            Self::J2000 => DirectionRef::J2000,
            Self::B1950 => DirectionRef::B1950,
            Self::Galactic => DirectionRef::GALACTIC,
        }
    }
}

impl FrequencyFrame {
    /// The measures frequency frame this is.
    #[must_use]
    pub const fn frequency_ref(self) -> FrequencyRef {
        match self {
            Self::Rest => FrequencyRef::REST,
            Self::Topocentric => FrequencyRef::TOPO,
            Self::Barycentric => FrequencyRef::BARY,
            Self::Lsrk => FrequencyRef::LSRK,
        }
    }
}

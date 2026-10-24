// SPDX-License-Identifier: LGPL-3.0-or-later
//! The catalogue of an opened observation: its antennas, fields and
//! partitions, with identities scoped to that opening.

use casa_types::measures::direction::MDirection;
use casa_types::measures::frequency::FrequencyRef;

/// An antenna of one opened observation.
///
/// The number is an index into [`Catalogue::antennas`], valid only for the
/// observation that issued it. It is not a casacore `ANTENNA` row number or
/// an MSv4 antenna name, though a backend may happen to number antennas the
/// way its store does.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct AntennaId(pub u32);

/// A field of one opened observation: an index into [`Catalogue::fields`].
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct FieldId(pub u32);

/// A partition of one opened observation: an index into
/// [`Catalogue::partitions`]. A partition has one spectral window and one
/// correlation setup (an MSv2 data description, an MSv4 dataset).
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct PartitionId(pub u32);

/// An ordered antenna pair. The order is the stored one: swapping it
/// conjugates the visibility, negates UVW and swaps the cross hands, so no
/// layer reorders it in passing. Feeds are not yet part of the identity; the
/// trial fixtures have one feed per antenna.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct OrderedBaseline {
    /// First antenna (MSv2 `ANTENNA1`, MSv4 `baseline_antenna1_name`).
    pub antenna1: AntennaId,
    /// Second antenna.
    pub antenna2: AntennaId,
}

impl OrderedBaseline {
    /// Whether both antennas are the same: an autocorrelation.
    #[must_use]
    pub fn is_auto(self) -> bool {
        self.antenna1 == self.antenna2
    }
}

/// A correlation product, by its two receptors.
///
/// The casacore `Stokes` codes are the persistent form in MSv2; MSv4 stores
/// the labels (`"XX"`). `casa-imaging-model` has its own `CorrelationType`;
/// the trial keeps this separate type because the contract crate cannot
/// depend on the model, and proposes moving the one definition here.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum Correlation {
    /// Circular RR.
    RR,
    /// Circular RL.
    RL,
    /// Circular LR.
    LR,
    /// Circular LL.
    LL,
    /// Linear XX.
    XX,
    /// Linear XY.
    XY,
    /// Linear YX.
    YX,
    /// Linear YY.
    YY,
}

impl Correlation {
    /// The casacore `Stokes::StokesTypes` code.
    #[must_use]
    pub const fn casacore_code(self) -> i32 {
        match self {
            Self::RR => 5,
            Self::RL => 6,
            Self::LR => 7,
            Self::LL => 8,
            Self::XX => 9,
            Self::XY => 10,
            Self::YX => 11,
            Self::YY => 12,
        }
    }

    /// The correlation with casacore `Stokes` code `code`, if it is one of
    /// the circular or linear products.
    #[must_use]
    pub const fn from_casacore_code(code: i32) -> Option<Self> {
        Some(match code {
            5 => Self::RR,
            6 => Self::RL,
            7 => Self::LR,
            8 => Self::LL,
            9 => Self::XX,
            10 => Self::XY,
            11 => Self::YX,
            12 => Self::YY,
            _ => return None,
        })
    }

    /// The label MSv4 stores in its `polarization` coordinate.
    #[must_use]
    pub const fn label(self) -> &'static str {
        match self {
            Self::RR => "RR",
            Self::RL => "RL",
            Self::LR => "LR",
            Self::LL => "LL",
            Self::XX => "XX",
            Self::XY => "XY",
            Self::YX => "YX",
            Self::YY => "YY",
        }
    }

    /// The correlation an MSv4 `polarization` label names.
    #[must_use]
    pub fn from_label(label: &str) -> Option<Self> {
        [
            Self::RR,
            Self::RL,
            Self::LR,
            Self::LL,
            Self::XX,
            Self::XY,
            Self::YX,
            Self::YY,
        ]
        .into_iter()
        .find(|correlation| correlation.label() == label)
    }

    /// The receptor indices `(p, q)` of the product `<E_p conj(E_q)>`: 0 for
    /// R or X, 1 for L or Y.
    #[must_use]
    pub const fn receptors(self) -> (usize, usize) {
        match self {
            Self::RR | Self::XX => (0, 0),
            Self::RL | Self::XY => (0, 1),
            Self::LR | Self::YX => (1, 0),
            Self::LL | Self::YY => (1, 1),
        }
    }
}

/// What a data group's visibilities are measured in.
///
/// Jy only where the store says so; anything else is kept as the store
/// spells it and never assumed to be a calibrated flux density.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum VisibilityUnits {
    /// Janskys.
    Jansky,
    /// A unit the store names but this contract does not interpret, kept
    /// verbatim (XRADIO's converter writes `"unkown"`).
    Declared(String),
    /// The store declares none (MSv2 `DATA` has no unit keyword).
    Undeclared,
}

/// One antenna of the catalogue.
#[derive(Clone, Debug, PartialEq)]
pub struct AntennaEntry {
    /// Antenna name as stored (`ANTENNA::NAME`; in MSv4 the
    /// `antenna_name` coordinate, which XRADIO forms as `NAME_STATION`).
    pub name: String,
    /// Station or pad name.
    pub station: String,
    /// Geocentric ITRF position in metres.
    pub position_m: [f64; 3],
    /// Mount type as stored (`ALT-AZ`).
    pub mount: String,
    /// Dish diameter in metres.
    pub dish_diameter_m: f64,
    /// Receptor polarization types in receptor order (`X`, `Y`).
    pub receptors: Vec<String>,
    /// Receptor angles in radians, in the same order.
    pub receptor_angles_rad: Vec<f64>,
}

/// One field of the catalogue.
#[derive(Clone, Debug)]
pub struct FieldEntry {
    /// Field name as stored. XRADIO appends the MSv2 field number
    /// (`trial_0`); the catalogue keeps what the store holds.
    pub name: String,
    /// The phase-tracking centre.
    pub phase_centre: MDirection,
    /// The source observed, when the store names one.
    pub source_name: Option<String>,
}

/// One named data group of a partition: which stored variables hold its
/// visibilities, flags, weights and UVW.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DataGroupEntry {
    /// The group's name (`base` in MSv4; the trial names MSv2's `DATA`
    /// association `base` too).
    pub name: String,
    /// The visibility variable or column.
    pub visibilities: String,
    /// The flag variable or column.
    pub flags: String,
    /// The weight variable or column.
    pub weights: String,
    /// The UVW variable or column.
    pub uvw: String,
    /// The visibilities' units.
    pub units: VisibilityUnits,
}

/// One partition: one spectral window and correlation setup.
#[derive(Clone, Debug)]
pub struct PartitionEntry {
    /// The spectral window's name as stored.
    pub spectral_window: String,
    /// Channel centres in hertz.
    pub frequencies_hz: Vec<f64>,
    /// Their spectral frame.
    pub frequency_frame: FrequencyRef,
    /// Signed channel widths in hertz.
    pub channel_widths_hz: Vec<f64>,
    /// The correlation products, in stored order.
    pub correlations: Vec<Correlation>,
    /// The partition's data groups.
    pub data_groups: Vec<DataGroupEntry>,
}

/// What an opened observation holds, without its payload.
#[derive(Clone, Debug, Default)]
pub struct Catalogue {
    /// Telescope name (MSv2 `OBSERVATION::TELESCOPE_NAME`).
    pub telescope: String,
    /// Observers (MSv2 `OBSERVATION::OBSERVER`).
    pub observers: Vec<String>,
    /// Project code (MSv2 `OBSERVATION::PROJECT`).
    pub project: String,
    /// Antennas; [`AntennaId`] indexes them.
    pub antennas: Vec<AntennaEntry>,
    /// Fields; [`FieldId`] indexes them.
    pub fields: Vec<FieldEntry>,
    /// Partitions; [`PartitionId`] indexes them.
    pub partitions: Vec<PartitionEntry>,
}

impl Catalogue {
    /// Partition `id`.
    ///
    /// # Panics
    ///
    /// If `id` was not issued by this catalogue.
    #[must_use]
    pub fn partition(&self, id: PartitionId) -> &PartitionEntry {
        &self.partitions[id.0 as usize]
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn correlations_round_trip_their_persistent_forms() {
        for code in 5..=12 {
            let correlation = Correlation::from_casacore_code(code).expect("product");
            assert_eq!(correlation.casacore_code(), code);
            assert_eq!(
                Correlation::from_label(correlation.label()),
                Some(correlation)
            );
        }
        assert_eq!(Correlation::from_casacore_code(1), None);
        assert_eq!(Correlation::XY.receptors(), (0, 1));
    }
}

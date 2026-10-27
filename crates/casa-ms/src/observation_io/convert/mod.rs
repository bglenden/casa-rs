// SPDX-License-Identifier: LGPL-3.0-or-later
//! Converting between MeasurementSet v2 and MSv4 through the observation
//! boundary: the source is read as batches like any consumer reads it, and
//! the destination is created from those batches and the source's
//! catalogue ([`to_msv4`], [`to_msv2`]).
//!
//! A conversion first inventories what the destination cannot hold (a
//! [`Loss`]) and refuses, before any byte is written, every loss the
//! caller did not accept ([`ConversionOptions::accept`]): converting never
//! drops information silently, nor states what the source did not hold
//! without saying so ([`Loss::SigmaDerived`]). What it keeps it keeps
//! exactly: every acquisition, its flags (row flags folded in), weights,
//! UVW and time auxiliaries, and every selected data group. Padding, a grid
//! cell no acquisition filled, stays padding: an MSv4 holds it as a NaN
//! weight, and an MSv2 as a flagged row of NaN data and weights, or, by the
//! declared policy that reproduces an original sparse MSv2, as no row
//! ([`PaddingRows`]).
//!
//! The selected data groups of a partition are read in lockstep, one
//! traversal each sized to the same whole times per batch (the first with
//! every role, the others their visibilities only), so the destination
//! writes every variable of a time range together and each chunk once.

mod lockstep;
mod losses;
mod msv2_rows;
mod msv2_tables;
mod to_msv2;
mod to_msv4;

use std::fmt;

pub(crate) use to_msv2::catalogue_subtables;
pub use to_msv2::to_msv2;
pub use to_msv4::to_msv4;

use super::Msv4Codec;

/// Information a conversion cannot carry into its destination.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum Loss {
    /// MSv2 `SIGMA` (and `SIGMA_SPECTRUM`): MSv4 holds no sigma.
    Sigma,
    /// An MSv2 destination's `SIGMA`, which no batch carries, derived from
    /// `WEIGHT` as CASA's `mstransform` derives it when corrected data
    /// become data: `1/√WEIGHT`, and -1 where the weight is not positive.
    /// Right for weights never recalibrated, wrong for the others.
    SigmaDerived,
    /// MSv2 `FLAG_ROW` set on some row: folded into the element flags, so
    /// a reverse conversion cannot tell a row flag from flags of every
    /// element.
    RowFlags,
    /// MSv2 `FLAG_CATEGORY` holding categories: MSv4 holds none.
    FlagCategories,
    /// A data group of the source the conversion does not select.
    DataGroup(String),
    /// A table (an MSv2 subtable, an MSv4 sub-dataset) whose content the
    /// destination does not hold.
    Table(String),
    /// A column or variable whose content the destination does not hold.
    Column(String),
    /// Antennas no partition's baselines name, this many: an MSv4
    /// dataset's `antenna_xds` holds only its baselines' antennas, as
    /// XRADIO's converter writes it, so these are written nowhere.
    UnusedAntennas(usize),
}

impl Loss {
    /// The kind of loss, for acceptance: the same for every table, column
    /// or group named.
    #[must_use]
    pub fn kind(&self) -> LossKind {
        match self {
            Self::Sigma | Self::SigmaDerived => LossKind::Sigma,
            Self::RowFlags => LossKind::RowFlags,
            Self::FlagCategories => LossKind::FlagCategories,
            Self::DataGroup(_) => LossKind::DataGroups,
            Self::Table(_) | Self::UnusedAntennas(_) => LossKind::Tables,
            Self::Column(_) => LossKind::Columns,
        }
    }
}

impl fmt::Display for Loss {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Sigma => write!(f, "SIGMA is not carried"),
            Self::SigmaDerived => write!(f, "SIGMA is derived from WEIGHT as 1/sqrt(WEIGHT)"),
            Self::RowFlags => write!(f, "FLAG_ROW is folded into FLAG"),
            Self::FlagCategories => write!(f, "FLAG_CATEGORY is not carried"),
            Self::DataGroup(group) => write!(f, "data group {group} is not converted"),
            Self::Table(table) => write!(f, "{table} is not carried"),
            Self::Column(column) => write!(f, "{column} is not carried"),
            Self::UnusedAntennas(count) => {
                write!(f, "antennas no baseline names are not carried ({count})")
            }
        }
    }
}

/// What a caller accepts losing ([`Loss::kind`]).
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum LossKind {
    /// [`Loss::Sigma`] and [`Loss::SigmaDerived`].
    Sigma,
    /// [`Loss::RowFlags`].
    RowFlags,
    /// [`Loss::FlagCategories`].
    FlagCategories,
    /// Any [`Loss::DataGroup`].
    DataGroups,
    /// Any [`Loss::Table`], and [`Loss::UnusedAntennas`].
    Tables,
    /// Any [`Loss::Column`].
    Columns,
}

/// How a conversion reads its source and stores its destination.
#[derive(Clone, Debug)]
pub struct ConversionOptions {
    /// The data groups to convert, the first carrying every role; `None`
    /// converts every group of the source, `base` first.
    pub data_groups: Option<Vec<String>>,
    /// The losses accepted; any other refuses the conversion.
    pub accept: Vec<LossKind>,
    /// The most bytes one time range of every selected group's batches
    /// may hold together.
    pub batch_bytes: usize,
    /// MSv4 destinations: partitions converted at once, each by its own
    /// thread. An MSv2 is written by one.
    pub workers: usize,
    /// MSv4 destinations: the chunk shape `[time, baseline, frequency,
    /// polarization]`, 0 for a whole axis.
    pub chunks: [u64; 4],
    /// MSv4 destinations: the payload variables' codec.
    pub codec: Msv4Codec,
    /// MSv2 destinations: what a padded grid cell becomes.
    pub padding: PaddingRows,
    /// Software recorded as the creator.
    pub creator: String,
    /// Creation time recorded, ISO 8601.
    pub creation_date: String,
}

/// What a padded grid cell (one no acquisition filled) becomes in an MSv2
/// destination.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum PaddingRows {
    /// A row flagged throughout, its data, weights and UVW NaN, as the MSv2
    /// creator writes padding (revision 2 design, D23); its interval is the
    /// grid's, its exposure 0 and its time centroid its time, so that the
    /// row's keys stay valid. `FLAG_ROW` stays false: every element's flag
    /// already excludes the row, and a reader that folds `FLAG_ROW` into
    /// the flags reads the same. Every grid cell is a row, so the row count
    /// is known from the grids.
    #[default]
    Flagged,
    /// No row: the declared policy that reproduces an original sparse MSv2
    /// (D23), since a NaN weight is padding's one mark. The row count then
    /// needs the acquisitions counted first, from the weights.
    Omitted,
}

/// What a conversion did.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct ConversionReport {
    /// The losses the conversion took, all accepted.
    pub losses: Vec<Loss>,
    /// Partitions converted.
    pub partitions: usize,
    /// Batches of the first group read.
    pub batches: usize,
    /// Samples read (the grids' cells, padding included).
    pub samples: usize,
    /// MSv2 destinations: rows written, one per sample, or per acquisition
    /// with padding omitted ([`PaddingRows`]).
    pub rows: usize,
    /// MSv4 destinations: payload elements encoded, over every variable.
    pub elements: usize,
    /// MSv4 destinations: elements held toward a chunk a batch covered only
    /// partly.
    pub held_elements: usize,
}

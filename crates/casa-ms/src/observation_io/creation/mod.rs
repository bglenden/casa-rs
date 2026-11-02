// SPDX-License-Identifier: LGPL-3.0-or-later
//! Creating a MeasurementSet v2 from observations through the observation
//! boundary ([`CreationPlan`]): each input is read as batches like any
//! consumer reads it, and the MeasurementSet is created from those batches
//! and the inputs' catalogues.
//!
//! A plan first inventories what the MeasurementSet cannot hold (a
//! [`Loss`]) and refuses, before any byte is written, every loss the
//! caller did not accept ([`CreationOptions::accept`]): creation never
//! drops information silently, nor states what an input did not hold
//! without saying so ([`Loss::SigmaDerived`]). What it keeps it keeps
//! exactly: every acquisition, its flags (row flags folded in), weights,
//! UVW and time auxiliaries, and every selected data group. Padding, a grid
//! cell no acquisition filled, stays padding: a flagged row of NaN data and
//! weights, or, by the declared policy that reproduces an original sparse
//! MSv2, no row ([`PaddingRows`]).
//!
//! The selected data groups of a partition are read in lockstep, one
//! traversal each sized to the same whole times per batch (the first with
//! every role, the others their visibilities only), so the writer writes
//! every column of a time range together.
//!
//! What making and writing a plan hold is admitted against
//! [`CreationOptions::memory_bytes`] before it is allocated
//! ([`MemoryCharges`]); a batch must hold one integration of every selected
//! group ([`CreationOptions::batch_bytes`]).
//!
//! The conversions between MSv2 and MSv4 (`convert`, with the `msv4`
//! feature) share this machinery; converting into an MSv2 is a plan over
//! one input.

mod combine;
mod inputs;
pub(crate) mod lockstep;
pub(crate) mod losses;
mod memory;
mod msv2_ephemeris;
mod msv2_identity;
mod msv2_pointing;
mod msv2_rows;
mod msv2_sources;
mod msv2_tables;
mod occupancy;
mod plan;
pub(crate) mod pointing_plan;
mod pointing_scan;
mod preservation;
mod replace;
mod scans;
mod writing;

use std::fmt;
use std::path::PathBuf;

pub use memory::{MemoryCharges, default_memory_bytes};
pub use plan::CreationPlan;
#[cfg(feature = "msv4")]
pub(crate) use plan::catalogue_subtables;
pub use replace::Replacement;

/// Information a conversion or a plan cannot carry into its destination.
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
    /// An MSv2 column the source leaves unstated for some row (an MSv4
    /// window's IF chain, a flag no store holds), written as MSv2's
    /// default: the destination states what the source did not.
    Defaulted(String),
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
            Self::Defaulted(_) => LossKind::Defaulted,
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
            Self::Defaulted(column) => {
                write!(f, "{column} is not stated, and MSv2's default is written")
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
    /// Any [`Loss::Defaulted`].
    Defaulted,
}

impl LossKind {
    /// Every kind, in order.
    pub const ALL: [Self; 7] = [
        Self::Sigma,
        Self::RowFlags,
        Self::FlagCategories,
        Self::DataGroups,
        Self::Tables,
        Self::Columns,
        Self::Defaulted,
    ];

    /// The kind's stable name, as a caller states it: `sigma`, `rowflags`,
    /// `flagcategories`, `datagroups`, `tables`, `columns` or `defaulted`.
    /// A refused conversion names the kinds to accept by these names
    /// ([`Display`](fmt::Display)), and [`str::parse`] reads them back.
    #[must_use]
    pub fn name(self) -> &'static str {
        match self {
            Self::Sigma => "sigma",
            Self::RowFlags => "rowflags",
            Self::FlagCategories => "flagcategories",
            Self::DataGroups => "datagroups",
            Self::Tables => "tables",
            Self::Columns => "columns",
            Self::Defaulted => "defaulted",
        }
    }
}

impl fmt::Display for LossKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.name())
    }
}

impl std::str::FromStr for LossKind {
    type Err = String;

    /// The kind [`LossKind::name`] names, in any case.
    fn from_str(text: &str) -> Result<Self, Self::Err> {
        Self::ALL
            .into_iter()
            .find(|kind| kind.name().eq_ignore_ascii_case(text.trim()))
            .ok_or_else(|| {
                let names: Vec<_> = Self::ALL.iter().map(|kind| kind.name()).collect();
                format!(
                    "unknown loss kind {text:?}; expected one of {}",
                    names.join(", ")
                )
            })
    }
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

/// One observation a plan reads: the store at `path` (MSv2, or MSv4 with
/// the `msv4` feature), its weights scaled by `weight_scale`.
#[derive(Clone, Debug, PartialEq)]
pub struct CreationInput {
    /// The store.
    pub path: PathBuf,
    /// The factor every `WEIGHT` is multiplied by, and every `SIGMA`
    /// divided by the square root of: CASA's `visweightscale`. Finite and
    /// positive.
    pub weight_scale: f64,
}

impl CreationInput {
    /// The store at `path`, its weights unscaled.
    #[must_use]
    pub fn new(path: impl Into<PathBuf>) -> Self {
        Self {
            path: path.into(),
            weight_scale: 1.0,
        }
    }
}

/// How a plan reads its inputs and writes the MeasurementSet.
#[derive(Clone, Debug)]
pub struct CreationOptions {
    /// The data groups to write, the first carrying every role; `None`
    /// writes every group of the inputs, `base` first.
    pub data_groups: Option<Vec<String>>,
    /// The losses accepted; any other refuses the plan.
    pub accept: Vec<LossKind>,
    /// The most bytes one time range of every selected group's batches
    /// may hold together, and the pointing a read gathers; a batch that
    /// cannot hold one integration (one time of every group) is refused,
    /// not rounded up.
    pub batch_bytes: usize,
    /// The most memory making and writing the plan hold, admitted before
    /// it is allocated ([`MemoryCharges`]); [`default_memory_bytes`] is the
    /// host's usual share.
    pub memory_bytes: usize,
    /// What a padded grid cell becomes; partitions combined from several
    /// inputs need it omitted.
    pub padding: PaddingRows,
    /// How close the inputs' entities must be to be one, as CASA's `concat`
    /// matches them.
    pub tolerance: casa_observation::MergeTolerance,
    /// The `HISTORY` rows to write: what the task creating the
    /// MeasurementSet records of itself (CASA's `concat` records its
    /// parameters). The inputs' own history is not carried
    /// ([`Loss::Table`]).
    pub history: Vec<HistoryRow>,
    /// The creation time recorded, ISO 8601: the date each ephemeris
    /// table written states (`VS_CREATE`, `VS_DATE`).
    pub creation_date: String,
}

/// One `HISTORY` row a plan writes.
#[derive(Clone, Debug, PartialEq)]
pub struct HistoryRow {
    /// When, MJD seconds in UTC.
    pub time_s: f64,
    /// The observing session it concerns, -1 for none.
    pub observation: i32,
    /// The message.
    pub message: String,
    /// Its priority, as CASA's logger names them (`NORMAL`, `WARN`, ...).
    pub priority: String,
    /// Where it comes from (CASA's task name).
    pub origin: String,
    /// The application (`casa-rs`).
    pub application: String,
    /// The application's parameters, one per element.
    pub parameters: Vec<String>,
    /// The command line, one per element.
    pub command: Vec<String>,
}

/// Where a plan writes its MeasurementSet, decided when it is made.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Destination {
    /// A new MeasurementSet at this path, which must not exist. It is
    /// created in a staging directory renamed into place when complete, so
    /// a failure leaves nothing.
    New(PathBuf),
    /// The existing MeasurementSet at this path, replaced: how CASA's
    /// `concat` appends to an existing `concatvis`, which is then the first
    /// input. The plan holds casacore's write lock on its MAIN from when it
    /// is made through publication; the new MeasurementSet is staged beside
    /// it and the two exchanged in one call, so a failure leaves it as it
    /// was. Refused where another process holds the lock, or where the
    /// filesystem cannot exchange directories.
    Replace(PathBuf),
}

/// What a plan wrote.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct CreationReport {
    /// The losses the plan took, all accepted.
    pub losses: Vec<Loss>,
    /// Partitions read, over every input.
    pub partitions: usize,
    /// Batches of the first group read.
    pub batches: usize,
    /// Samples read (the grids' cells, padding included).
    pub samples: usize,
    /// Rows written, one per sample, or per acquisition with padding
    /// omitted ([`PaddingRows`]).
    pub rows: usize,
    /// A replaced MeasurementSet that could not be deleted once the new
    /// one took its place ([`Destination::Replace`]): its path.
    pub leftover: Option<PathBuf>,
}

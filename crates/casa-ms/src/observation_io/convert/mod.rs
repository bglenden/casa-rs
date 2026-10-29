// SPDX-License-Identifier: LGPL-3.0-or-later
//! Converting between MeasurementSet v2 and MSv4 through the observation
//! boundary: the source is read as batches like any consumer reads it, and
//! the destination is created from those batches and the source's
//! catalogue ([`to_msv4`](fn@to_msv4), [`to_msv2`]).
//!
//! A conversion first inventories what the destination cannot hold (a
//! [`Loss`]) and refuses, before any byte is written, every loss the
//! caller did not accept ([`ConversionOptions::accept`]), as a
//! [`CreationPlan`] does; converting into an MSv2 is a plan over one
//! input. What it keeps it keeps exactly: every acquisition, its flags (row
//! flags folded in), weights, UVW and time auxiliaries, and every selected
//! data group. Padding, a grid cell no acquisition filled, stays padding:
//! an MSv4 holds it as a NaN weight, and an MSv2 as a flagged row of NaN
//! data and weights, or, by the declared policy that reproduces an
//! original sparse MSv2, as no row ([`PaddingRows`]).

mod to_msv4;

use std::path::Path;

use casa_observation::ObservationError;

use super::creation::{CreationInput, CreationOptions, CreationPlan, Destination};
pub use super::creation::{Loss, LossKind, PaddingRows};
pub use to_msv4::to_msv4;

use super::Msv4Codec;

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

/// Convert the store at `source` (MSv2 or MSv4) into a new MeasurementSet
/// v2 at `destination`: a [`CreationPlan`] over the one input, written as
/// [`Destination::New`].
///
/// The selected data groups ([`ConversionOptions::data_groups`]) become
/// `DATA` (`base`), `CORRECTED_DATA` (`corrected`) and `MODEL_DATA`
/// (`model`), sharing the first group's flags, weights and UVW. Partitions
/// sharing a spectral window, polarization or intents share their
/// subtable rows. Every grid cell becomes one row; padding becomes a row
/// whose every `FLAG` is set, with NaN data, weights and `UVW`, the grid's
/// interval, no exposure and `FLAG_ROW` false (its flags already exclude
/// it), as D23 decided. With [`PaddingRows::Omitted`], padding becomes no
/// row, which reproduces an original sparse MSv2. `SIGMA` is derived
/// from the weights ([`Loss::SigmaDerived`]), so the caller must accept
/// [`LossKind::Sigma`]. Names are the source's: an MSv4 converted from an
/// MSv2 holds XRADIO's suffixed names (`NAME_FIELDID`, `NAME_STATION`,
/// ...), so a round trip renames.
///
/// # Errors
///
/// As [`CreationPlan::new`] and [`CreationPlan::write`].
pub fn to_msv2(
    source: &Path,
    destination: &Path,
    options: &ConversionOptions,
) -> Result<ConversionReport, ObservationError> {
    let plan = CreationPlan::new(
        &[CreationInput::new(source)],
        &CreationOptions {
            data_groups: options.data_groups.clone(),
            accept: options.accept.clone(),
            batch_bytes: options.batch_bytes,
            padding: options.padding,
        },
    )?;
    let report = plan.write(destination, Destination::New)?;
    Ok(ConversionReport {
        losses: report.losses,
        partitions: report.partitions,
        batches: report.batches,
        samples: report.samples,
        rows: report.rows,
        ..ConversionReport::default()
    })
}

// SPDX-License-Identifier: LGPL-3.0-or-later
//! What a consumer asks of an observation, and what the observation says it
//! will cost before anything is read.

use std::ops::Range;

use casa_types::Correlation;

use crate::ObservationError;
use crate::catalogue::{PartitionEntry, PartitionId};

/// Which roles a traversal reads: the data group's payload and the
/// samples' time auxiliaries and scan. A role left out is not read at all
/// (a flags-and-weights traversal decodes no visibilities); a role asked for
/// that the store does not hold fails binding, never a later read.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Roles {
    /// Visibilities.
    pub values: bool,
    /// Flags (in MSv2, `FLAG` with `FLAG_ROW` folded in).
    pub flags: bool,
    /// Weights.
    pub weights: bool,
    /// UVW.
    pub uvw: bool,
    /// Time centroids (MSv2 `TIME_CENTROID`, MSv4 `TIME_CENTROID`).
    pub time_centroid: bool,
    /// Integration intervals (MSv2 `INTERVAL`, MSv4
    /// `time.integration_time`).
    pub interval: bool,
    /// Effective integration times (MSv2 `EXPOSURE`, MSv4
    /// `EFFECTIVE_INTEGRATION_TIME`).
    pub exposure: bool,
    /// Scans (MSv2 `SCAN_NUMBER`, MSv4 `scan_name`).
    pub scan: bool,
}

impl Roles {
    /// Every role.
    pub const ALL: Self = Self {
        values: true,
        flags: true,
        weights: true,
        uvw: true,
        time_centroid: true,
        interval: true,
        exposure: true,
        scan: true,
    };

    /// The data group's payload only: values, flags, weights and UVW.
    pub const PAYLOAD: Self = Self {
        values: true,
        flags: true,
        weights: true,
        uvw: true,
        time_centroid: false,
        interval: false,
        exposure: false,
        scan: false,
    };

    /// Bytes one sample of these roles occupies in a batch: the payload
    /// cube elements (`Complex<f32>`, `bool`, `f32`) over `channels` ×
    /// `correlations`, the weights over every channel or once per
    /// correlation, UVW, and `per_sample_scalars` eight-byte scalars (the
    /// time centroid and whichever of interval and exposure the store holds
    /// per sample).
    #[must_use]
    pub fn sample_bytes(
        &self,
        channels: usize,
        correlations: usize,
        weights_per_channel: bool,
        per_sample_scalars: usize,
    ) -> usize {
        let cell = channels * correlations;
        let weights = if weights_per_channel {
            cell
        } else {
            correlations
        };
        usize::from(self.values) * cell * 8
            + usize::from(self.flags) * cell
            + usize::from(self.weights) * weights * 4
            + usize::from(self.uvw) * 3 * 8
            + per_sample_scalars * 8
    }
}

/// Which correlation products a traversal delivers.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Deliver {
    /// Every product of the partition, in stored order.
    All,
    /// These products, which the partition must hold; delivered in the
    /// partition's order whatever order they are listed in.
    These(Vec<Correlation>),
}

/// Whose flags the delivered flags carry.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum FlagsFrom {
    /// The delivered products' own stored flags.
    Delivered,
    /// Every product's: a delivered product's flag is the OR over all the
    /// partition's products at that sample and channel. Production Stokes I
    /// from `XX` and `YY` uses this: a flagged cross hand drops the sample,
    /// as it does when every product is read.
    All,
}

/// The correlation axis of a request.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CorrelationRequest {
    /// Which products to deliver.
    pub deliver: Deliver,
    /// Whose flags the delivered flags carry.
    pub flags_from: FlagsFrom,
}

impl CorrelationRequest {
    /// Every product, its own flags.
    pub const ALL: Self = Self {
        deliver: Deliver::All,
        flags_from: FlagsFrom::Delivered,
    };
}

/// How samples form scientific groups: casa-rs `gaincal`'s `solint` rules,
/// keyed as production keys them (field unless combined, scan unless
/// combined, with the clock of each (field, scan) run starting at its first
/// time).
#[derive(Clone, Copy, Debug, PartialEq)]
pub enum Grouping {
    /// No groups; batches carry no fragments.
    None,
    /// Time bins of `width_s` seconds (`solint='60s'`): bin
    /// `floor((t - t0) / width_s)`, `t0` the first time of the (field, scan)
    /// run. An infinite width is one bin per run.
    TimeBins {
        /// Bin width in seconds, positive; may be infinite.
        width_s: f64,
        /// Whether one group spans every field.
        combine_fields: bool,
        /// Whether one group spans every scan.
        combine_scans: bool,
    },
    /// One group per scan (`solint='inf'`).
    PerScan {
        /// Whether one group spans every field.
        combine_fields: bool,
    },
    /// One group per time (`solint='int'`).
    PerIntegration,
}

/// What a consumer asks one traversal to deliver.
#[derive(Clone, Debug, PartialEq)]
pub struct AccessRequest {
    /// The partitions to read, in traversal order; the consumer selects
    /// them from the catalogue by content. Binding gives one cursor per
    /// partition, and a batch never spans two.
    pub partitions: Vec<PartitionId>,
    /// The data group, by name, which every selected partition must have.
    pub data_group: String,
    /// The roles to read.
    pub roles: Roles,
    /// The partitions' native channels to deliver.
    pub channels: Range<usize>,
    /// The correlation products to deliver and whose flags they carry.
    pub correlations: CorrelationRequest,
    /// Scientific groups. Whether a group's completion arrives on its last
    /// fragment or as a later [`ReadEvent::GroupEnd`] is the source's
    /// choice, not the consumer's: a consumer handles both.
    ///
    /// [`ReadEvent::GroupEnd`]: crate::ReadEvent::GroupEnd
    pub grouping: Grouping,
    /// The most payload bytes one batch may hold ([`Roles::sample_bytes`]
    /// per sample). A batch holds whole times; binding fails when one time
    /// does not fit.
    pub batch_bytes: usize,
}

/// A request's data group and correlation axis resolved against one
/// partition: what every adapter checks at binding, once.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ResolvedRequest {
    /// The data group's index in the partition's
    /// [`data_groups`](PartitionEntry::data_groups).
    pub data_group: usize,
    /// The delivered correlation products, in the partition's order.
    pub correlations: Vec<Correlation>,
    /// Their indices along the partition's correlation axis, or `None` when
    /// every product is delivered in place.
    pub correlation_indices: Option<Vec<usize>>,
    /// Whether a delivered flag is the OR over every product's: true only
    /// when a subset is delivered with [`FlagsFrom::All`] (with every
    /// product delivered the policy changes nothing).
    pub flags_from_all: bool,
}

impl AccessRequest {
    /// Resolve this request's data group, channel window and correlation
    /// axis against `entry`.
    ///
    /// # Errors
    ///
    /// When the partition lacks the data group or a requested product, the
    /// channel window is empty or outside the partition, or no product is
    /// requested.
    pub fn resolve(&self, entry: &PartitionEntry) -> Result<ResolvedRequest, ObservationError> {
        let invalid = |reason: String| ObservationError::InvalidRequest { reason };
        let (data_group, _) = entry
            .data_group(&self.data_group)
            .ok_or_else(|| invalid(format!("no data group {}", self.data_group)))?;
        if self.channels.is_empty() || self.channels.end > entry.frequencies_hz.len() {
            return Err(invalid(format!(
                "channels {:?} outside the partition's {}",
                self.channels,
                entry.frequencies_hz.len()
            )));
        }
        let (correlations, correlation_indices) = match &self.correlations.deliver {
            Deliver::All => (entry.correlations.clone(), None),
            Deliver::These(wanted) => {
                let mut indices = wanted
                    .iter()
                    .map(|correlation| {
                        entry
                            .correlation_index(*correlation)
                            .ok_or_else(|| invalid(format!("the partition has no {correlation:?}")))
                    })
                    .collect::<Result<Vec<_>, _>>()?;
                indices.sort_unstable();
                indices.dedup();
                if indices.is_empty() {
                    return Err(invalid("no correlation product requested".to_string()));
                }
                let correlations = indices.iter().map(|&i| entry.correlations[i]).collect();
                let every = indices.len() == entry.correlations.len();
                (correlations, (!every).then_some(indices))
            }
        };
        Ok(ResolvedRequest {
            data_group,
            flags_from_all: correlation_indices.is_some()
                && self.correlations.flags_from == FlagsFrom::All,
            correlations,
            correlation_indices,
        })
    }
}

/// The store's natural read units along the axes a batch shape chooses:
/// the Zarr chunk shape of MSv4, the tile shape of MSv2. A planner that
/// aligns channel windows and time blocks to them decodes nothing twice.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct NaturalBlocks {
    /// Times per unit (for MSv2 the tile's rows divided by the baselines).
    pub times: usize,
    /// Channels per unit.
    pub channels: usize,
}

/// What one partition's traversal will cost, computed from the catalogue
/// and the store's geometry before any payload is read.
#[derive(Clone, Debug, PartialEq)]
pub struct PartitionEstimate {
    /// The partition.
    pub partition: PartitionId,
    /// Times on its grid.
    pub times: usize,
    /// Baselines on its grid.
    pub baselines: usize,
    /// Bytes one sample of the requested roles holds in a batch.
    pub bytes_per_sample: usize,
    /// Whole times per full batch under the budget.
    pub times_per_batch: usize,
    /// Batches in the traversal.
    pub batches: usize,
    /// Physical read units (casacore rows, Zarr chunks) the traversal
    /// touches, as modelled from the store's layout: the chunks that
    /// intersect each batch's selection. A model, not a measurement: the
    /// cursor's [`IoCounters::decode_units`] applies the same model to what
    /// it reads.
    pub decode_units: usize,
    /// Elements the touched units hold, over every role read, by the same
    /// model.
    pub decoded_elements: usize,
    /// Elements the request selects, over every role read; the ratio to
    /// `decoded_elements` is the read amplification.
    pub selected_elements: usize,
    /// The store's natural blocks along time and frequency.
    pub natural_blocks: NaturalBlocks,
    /// The fraction of grid cells no acquisition fills, which the adapter
    /// pads: 0 for a store that holds every cell.
    pub padded_fraction: f64,
}

impl PartitionEstimate {
    /// Samples per full batch.
    #[must_use]
    pub fn samples_per_batch(&self) -> usize {
        self.times_per_batch * self.baselines
    }

    /// Payload bytes a full batch holds.
    #[must_use]
    pub fn batch_bytes(&self) -> usize {
        self.samples_per_batch() * self.bytes_per_sample
    }

    /// Decoded elements per selected element.
    #[must_use]
    pub fn amplification(&self) -> f64 {
        amplification(self.decoded_elements, self.selected_elements)
    }
}

fn amplification(decoded: usize, selected: usize) -> f64 {
    if selected == 0 {
        0.0
    } else {
        decoded as f64 / selected as f64
    }
}

/// Whole times per batch under `batch_bytes`: as many as fit, at most
/// `times`.
///
/// # Errors
///
/// When one time of `baselines` samples at `bytes_per_sample` exceeds the
/// budget.
pub fn times_per_batch(
    batch_bytes: usize,
    bytes_per_sample: usize,
    baselines: usize,
    times: usize,
) -> Result<usize, ObservationError> {
    let time_bytes = bytes_per_sample.saturating_mul(baselines).max(1);
    let fit = (batch_bytes / time_bytes).min(times);
    if fit == 0 && times > 0 {
        return Err(ObservationError::InvalidRequest {
            reason: format!(
                "one time holds {baselines} samples of {bytes_per_sample} B, more than the \
                 {batch_bytes} B batch budget"
            ),
        });
    }
    Ok(fit)
}

/// What a request will cost over its partition set: the inquiry a planner
/// makes before binding anything, and what binding then confirms.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct Estimate {
    /// One estimate per requested partition, in request order.
    pub partitions: Vec<PartitionEstimate>,
}

impl Estimate {
    /// Samples over the set.
    #[must_use]
    pub fn samples(&self) -> usize {
        self.partitions.iter().map(|p| p.times * p.baselines).sum()
    }

    /// Batches over the set.
    #[must_use]
    pub fn batches(&self) -> usize {
        self.partitions.iter().map(|p| p.batches).sum()
    }

    /// The largest full batch of the set, in payload bytes.
    #[must_use]
    pub fn peak_batch_bytes(&self) -> usize {
        self.partitions
            .iter()
            .map(PartitionEstimate::batch_bytes)
            .max()
            .unwrap_or(0)
    }

    /// Read units over the set.
    #[must_use]
    pub fn decode_units(&self) -> usize {
        self.partitions.iter().map(|p| p.decode_units).sum()
    }

    /// Decoded elements per selected element over the set.
    #[must_use]
    pub fn amplification(&self) -> f64 {
        amplification(
            self.partitions.iter().map(|p| p.decoded_elements).sum(),
            self.partitions.iter().map(|p| p.selected_elements).sum(),
        )
    }
}

/// What a cursor has done so far, counted at batch granularity.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct IoCounters {
    /// Batches delivered.
    pub batches: usize,
    /// Storage read calls issued (one per column or variable per batch).
    pub read_calls: usize,
    /// Physical units touched by the reads issued, modelled from the chunk
    /// grid as [`PartitionEstimate::decode_units`] is (Zarr chunks; casacore
    /// rows). It counts what the selections intersect, not decodes observed
    /// inside the storage library.
    pub decode_units: usize,
    /// Payload bytes written into the caller's buffers.
    pub delivered_bytes: usize,
    /// Elements copied after decoding, beyond the decode into the buffers:
    /// placing rows on the grid, gathering a correlation subset, converting
    /// flag bytes.
    pub copied_elements: usize,
    /// Fills that had to grow a buffer.
    pub buffer_growths: usize,
}

impl IoCounters {
    /// Add `more` to these counters.
    pub fn add(&mut self, more: Self) {
        self.batches += more.batches;
        self.read_calls += more.read_calls;
        self.decode_units += more.decode_units;
        self.delivered_bytes += more.delivered_bytes;
        self.copied_elements += more.copied_elements;
        self.buffer_growths += more.buffer_growths;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_budget_fits_whole_times_or_refuses() {
        let bytes = Roles::ALL.sample_bytes(8, 4, true, 3);
        assert_eq!(bytes, 32 * 8 + 32 + 32 * 4 + 24 + 24);
        assert_eq!(
            times_per_batch(10 * bytes * 4, bytes, 10, 12).expect("fit"),
            4
        );
        assert_eq!(times_per_batch(usize::MAX, bytes, 10, 12).expect("fit"), 12);
        assert!(times_per_batch(bytes * 9, bytes, 10, 12).is_err());
        assert_eq!(times_per_batch(0, bytes, 10, 0).expect("empty"), 0);
    }
}

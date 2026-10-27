// SPDX-License-Identifier: LGPL-3.0-or-later
//! What a consumer asks of an observation, and what the observation says it
//! will cost before anything is read.

use std::ops::Range;
use std::sync::Arc;

use casa_types::Correlation;
use casa_types::measures::{EpochRef, MjdHighPrec};

use crate::ObservationError;
use crate::catalogue::{FieldId, PartitionEntry, PartitionId, ScanId};
use crate::creation::TimeBaselineGrid;
use crate::grouping::GroupFragment;
use crate::selection::{FieldSelection, TimeRuns};

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

    /// Bytes one time of these roles occupies in a batch besides its
    /// samples': the time offset, the field, the scan when asked for,
    /// `per_time_scalars` eight-byte scalars shared by the time's samples,
    /// and room for one group fragment (a batch has at most one per time).
    #[must_use]
    pub fn time_bytes(&self, per_time_scalars: usize) -> usize {
        size_of::<f64>()
            + size_of::<FieldId>()
            + usize::from(self.scan) * size_of::<ScanId>()
            + per_time_scalars * 8
            + size_of::<GroupFragment>()
    }
}

/// Which weights a traversal delivers when it reads the weights role.
///
/// A store's weights and its sigmas are different quantities: after
/// `statwt` or `mstransform`, CASA's `WEIGHT` need not be 1/σ². CASA's
/// calibration solvers weight samples by 1/σ² from the sigmas
/// (`VisBufferImpl2::resetWeightsUsingSigma`), so a solve asks for those;
/// the request never equates one with the other silently, and the estimate
/// records which rule and variable supplied each partition's weights
/// ([`PartitionEstimate::weights`]).
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum WeightRequest {
    /// The data group's stored weights (MSv2 `WEIGHT_SPECTRUM` when MAIN
    /// has it, else `WEIGHT` per correlation; MSv4 `WEIGHT`).
    #[default]
    Stored,
    /// CASA's solve weights: [`inverse_sigma_squared`] of the store's
    /// sigmas. MSv2 has them (`SIGMA_SPECTRUM` when MAIN has both it and
    /// `WEIGHT_SPECTRUM`, as CASA's rule is, else `SIGMA` per correlation;
    /// in CASA these describe `DATA`, whatever the data group read); an MSv4
    /// store holds none, so `fallback` says what it delivers.
    InverseSigmaSquared {
        /// What a store without sigmas delivers.
        fallback: SigmaFallback,
    },
}

/// What a store that holds no sigmas delivers for
/// [`WeightRequest::InverseSigmaSquared`].
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SigmaFallback {
    /// Nothing: binding is refused.
    Refuse,
    /// The store's weights, recorded as such
    /// ([`WeightRule::Stored`] in the estimate's [`WeightSource`]).
    StoredWeights,
}

/// How a partition's delivered weights were made.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum WeightRule {
    /// The stored weights, as stored.
    Stored,
    /// [`inverse_sigma_squared`] of stored sigmas.
    InverseSigmaSquared,
}

/// Which rule and stored variable supplied a partition's delivered
/// weights: what binding resolved a [`WeightRequest`] to.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct WeightSource {
    /// How the values were made.
    pub rule: WeightRule,
    /// The variable or column read (`WEIGHT_SPECTRUM`, `WEIGHT`,
    /// `SIGMA_SPECTRUM`, `SIGMA`).
    pub variable: String,
    /// Whether it holds one value per channel, or one per correlation that
    /// the batch broadcasts over channels.
    pub per_channel: bool,
}

/// The weight CASA's solvers give a sample whose noise is `sigma`
/// (`VisBufferImpl2::resetWeightsUsingSigma`): 1/σ², and 0 where σ is not
/// positive (or is NaN).
#[must_use]
pub fn inverse_sigma_squared(sigma: f32) -> f32 {
    if sigma > 0.0 {
        1.0 / (sigma * sigma)
    } else {
        0.0
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
    /// partition's products at that sample and channel. Stokes I from `XX`
    /// and `YY` uses this as casa-rs's imaging operator does
    /// (`casa-imaging-operator/src/resample.rs`): a flagged cross hand drops
    /// the sample, as it does when every product is read. It is that
    /// operator's choice, not CASA's general rule: CASA collapses flags over
    /// polarization only on its frequency-interpolation path
    /// (`FTMachine::interpolateFrequencyTogrid`), while `GridFT::put` tests
    /// each hand's own flag.
    All,
}

/// The correlation products a traversal delivers, resolved once at
/// binding ([`AccessRequest::resolve`]) and shared by every batch of the
/// traversal and every piece packed from one: no batch copies the list.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum CorrelationAxis {
    /// Every product of the partition, in place: a batch's
    /// [`correlations`](crate::VisibilityBatch::correlations) are the
    /// catalogue's [`PartitionEntry::correlations`].
    All,
    /// Some of the partition's products, in its order.
    Subset {
        /// The delivered products.
        products: Arc<[Correlation]>,
        /// Their indices along the partition's correlation axis, strictly
        /// increasing.
        indices: Arc<[usize]>,
    },
}

impl CorrelationAxis {
    /// The delivered products of `entry`, the partition the axis was
    /// resolved against.
    #[must_use]
    pub fn products<'a>(&'a self, entry: &'a PartitionEntry) -> &'a [Correlation] {
        match self {
            Self::All => &entry.correlations,
            Self::Subset { products, .. } => products,
        }
    }

    /// The subset's indices along the partition's axis, or `None` when
    /// every product is delivered in place.
    #[must_use]
    pub fn indices(&self) -> Option<&[usize]> {
        match self {
            Self::All => None,
            Self::Subset { indices, .. } => Some(indices),
        }
    }

    /// Whether the axis describes products of `entry`: a subset is
    /// non-empty, strictly increasing and names the products at its
    /// indices. At most one comparison per product; nothing is allocated.
    #[must_use]
    pub fn fits(&self, entry: &PartitionEntry) -> bool {
        match self {
            Self::All => !entry.correlations.is_empty(),
            Self::Subset { products, indices } => {
                !indices.is_empty()
                    && products.len() == indices.len()
                    && indices.windows(2).all(|pair| pair[0] < pair[1])
                    && indices
                        .iter()
                        .zip(products.iter())
                        .all(|(&index, product)| entry.correlations.get(index) == Some(product))
            }
        }
    }
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

/// A half-open interval of epochs, `[start_s, end_s)` seconds from
/// `origin` on time scale `scale`: the grid times a traversal reads.
///
/// It is stated in the partitions' own time origin and scale
/// ([`PartitionEntry::time_origin`], [`PartitionEntry::time_scale`]), which
/// every partition of one store shares, so selecting a grid time compares
/// two offsets from one origin and rounds nothing. A range stated in another
/// origin or scale is refused at binding, not converted. Either end may be
/// infinite. [`Self::of_grid`] states the grid times a planner chose by
/// index with the grid's own values, so its ends are exactly those times;
/// one range serves every partition of a request, so a calibration solution
/// interval selects the same epochs in each spectral window.
#[derive(Clone, Copy, Debug)]
pub struct TimeRange {
    /// The time scale of the epochs.
    pub scale: EpochRef,
    /// The origin `start_s` and `end_s` count from.
    pub origin: MjdHighPrec,
    /// The first epoch selected, seconds from `origin`.
    pub start_s: f64,
    /// The epoch after the last selected, seconds from `origin`.
    pub end_s: f64,
}

impl TimeRange {
    /// The grid times `times` of `grid` (indices into
    /// [`TimeBaselineGrid::times_s`]): from the first one's time to the
    /// next time after the last, or without end when `times` runs to the
    /// grid's last time. An empty `times` selects nothing.
    ///
    /// # Panics
    ///
    /// If `times` starts after it ends or ends past the grid's times.
    #[must_use]
    pub fn of_grid(grid: &TimeBaselineGrid, times: Range<usize>) -> Self {
        assert!(
            times.start <= times.end && times.end <= grid.times_s.len(),
            "grid times {times:?} of {}",
            grid.times_s.len()
        );
        let at = |index: usize| grid.times_s.get(index).copied().unwrap_or(f64::INFINITY);
        Self {
            scale: grid.time_scale,
            origin: grid.time_origin,
            start_s: if times.is_empty() {
                at(times.end)
            } else {
                at(times.start)
            },
            end_s: at(times.end),
        }
    }
}

impl TimeRange {
    /// Whether `other` counts its epochs from the same origin on the same
    /// scale, so the two compare offset for offset.
    #[must_use]
    pub fn same_frame(&self, other: &Self) -> bool {
        self.scale == other.scale && same_origin(self.origin, other.origin)
    }
}

impl PartialEq for TimeRange {
    fn eq(&self, other: &Self) -> bool {
        self.same_frame(other) && self.start_s == other.start_s && self.end_s == other.end_s
    }
}

/// Whether two origins are the same epoch, part for part.
fn same_origin(a: MjdHighPrec, b: MjdHighPrec) -> bool {
    a.day() == b.day() && a.frac() == b.frac()
}

/// The times a traversal reads.
#[derive(Clone, Copy, Debug, Default, PartialEq)]
pub enum TimeSelection {
    /// Every time of each partition's grid.
    #[default]
    All,
    /// The grid times within a range.
    Within(TimeRange),
}

impl TimeSelection {
    /// The grid times of `entry`'s partition the selection holds, as
    /// indices into `times_s`: the partition's grid times, seconds from
    /// `entry`'s [`time_origin`](PartitionEntry::time_origin), ascending. A
    /// range holding no grid time (between two, or past either end) selects
    /// none.
    ///
    /// # Errors
    ///
    /// When the range is stated in another time scale or origin than the
    /// partition's, either end is NaN, or it starts after it ends.
    pub fn indices(
        &self,
        entry: &PartitionEntry,
        times_s: &[f64],
    ) -> Result<Range<usize>, ObservationError> {
        let Self::Within(range) = self else {
            return Ok(0..times_s.len());
        };
        let invalid = |reason: String| ObservationError::InvalidRequest { reason };
        if range.scale != entry.time_scale || !same_origin(range.origin, entry.time_origin) {
            return Err(invalid(format!(
                "the time range is stated in {:?} from MJD {} + {}, the partition's times in {:?} \
                 from MJD {} + {}: a range is stated in its partitions' scale and origin",
                range.scale,
                range.origin.day(),
                range.origin.frac(),
                entry.time_scale,
                entry.time_origin.day(),
                entry.time_origin.frac()
            )));
        }
        if range.start_s.is_nan() || range.end_s.is_nan() || range.start_s > range.end_s {
            return Err(invalid(format!(
                "the time range [{}, {}) s is not an interval",
                range.start_s, range.end_s
            )));
        }
        let start = times_s.partition_point(|time| *time < range.start_s);
        let end = times_s.partition_point(|time| *time < range.end_s);
        Ok(start..end.max(start))
    }
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
    /// Which weights the weights role delivers: the stored ones, or the
    /// solve weights from the sigmas.
    pub weights: WeightRequest,
    /// The partitions' native channels to deliver.
    pub channels: Range<usize>,
    /// The grid times to deliver: every time, or those within one range
    /// of epochs, in each partition. Scientific groups keep the keys they
    /// have over the grid's field-selected times, whatever the range; a
    /// group the range cuts is delivered as the part it holds
    /// ([`GroupFragment::clipped`]). Workers that split a
    /// request by time plan the slices together ([`TimeSplit`](crate::TimeSplit)).
    pub times: TimeSelection,
    /// The fields to deliver: the selected times phased to one of them,
    /// whole times, so no batch holds the others' times. The MSv2 adapter
    /// reads none of their rows; the MSv4 adapter decodes each chunk row a
    /// batch touches whole, gap times included
    /// (`docs/measurement-set/contracts.md`, section 4). Scientific groups
    /// are keyed over these fields' times, as production `gaincal` bins
    /// only the rows it selects: a group that combines fields holds the
    /// selected fields' times alone, its bins counted from the first of
    /// them, so a field selection cuts no group. Intents are
    /// selected by the partitions requested: a partition's samples share
    /// theirs.
    pub fields: FieldSelection,
    /// The correlation products to deliver and whose flags they carry.
    pub correlations: CorrelationRequest,
    /// Scientific groups. Whether a group's completion arrives on its last
    /// fragment or as a later [`ReadEvent::GroupEnd`] is the source's
    /// choice, not the consumer's: a consumer handles both.
    ///
    /// [`ReadEvent::GroupEnd`]: crate::ReadEvent::GroupEnd
    pub grouping: Grouping,
    /// The most bytes one batch may hold, with the adapter's decode scratch
    /// for it ([`PartitionEstimate::held_bytes_per_time`]). A batch holds
    /// whole times; binding fails when one time does not fit.
    pub batch_bytes: usize,
}

/// A request's data group, times and correlation axis resolved against one
/// partition: what every adapter checks at binding, once.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ResolvedRequest {
    /// The data group's index in the partition's
    /// [`data_groups`](PartitionEntry::data_groups).
    pub data_group: usize,
    /// The grid times delivered, as indices into the partition's grid
    /// times: the time selection's ([`TimeSelection::indices`]) whose field
    /// the field selection holds.
    pub times: TimeRuns,
    /// The delivered correlation products, shared by every batch.
    pub correlations: CorrelationAxis,
    /// Whether a delivered flag is the OR over every product's: true only
    /// when a subset is delivered with [`FlagsFrom::All`] (with every
    /// product delivered the policy changes nothing).
    pub flags_from_all: bool,
}

impl AccessRequest {
    /// Resolve this request's data group, channel window, times and
    /// correlation axis against `entry`, whose grid times are `times_s`
    /// and whose requested data group is phased to `fields[t]` at time `t`.
    ///
    /// # Errors
    ///
    /// When the partition lacks the data group or a requested product, the
    /// channel window is empty or outside the partition, no product is
    /// requested, or the time selection is refused
    /// ([`TimeSelection::indices`]); [`ObservationError::InvalidStore`]
    /// when `fields` has not one field per time.
    pub fn resolve(
        &self,
        entry: &PartitionEntry,
        times_s: &[f64],
        fields: &[FieldId],
    ) -> Result<ResolvedRequest, ObservationError> {
        let invalid = |reason: String| ObservationError::InvalidRequest { reason };
        let (data_group, _) = entry
            .data_group(&self.data_group)
            .ok_or_else(|| invalid(format!("no data group {}", self.data_group)))?;
        if fields.len() != times_s.len() {
            return Err(ObservationError::InvalidStore {
                reason: format!(
                    "{} fields for {} grid times: a field per time",
                    fields.len(),
                    times_s.len()
                ),
            });
        }
        if self.channels.is_empty() || self.channels.end > entry.frequencies_hz.len() {
            return Err(invalid(format!(
                "channels {:?} outside the partition's {}",
                self.channels,
                entry.frequencies_hz.len()
            )));
        }
        let correlations = match &self.correlations.deliver {
            Deliver::All => CorrelationAxis::All,
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
                if indices.len() == entry.correlations.len() {
                    CorrelationAxis::All
                } else {
                    CorrelationAxis::Subset {
                        products: indices.iter().map(|&i| entry.correlations[i]).collect(),
                        indices: indices.into(),
                    }
                }
            }
        };
        Ok(ResolvedRequest {
            data_group,
            times: TimeRuns::selected(self.times.indices(entry, times_s)?, fields, &self.fields),
            flags_from_all: correlations.indices().is_some()
                && self.correlations.flags_from == FlagsFrom::All,
            correlations,
        })
    }
}

/// The store's natural read units along the axes a batch shape chooses:
/// the Zarr chunk shape of MSv4, the tile shape of MSv2. A planner that
/// aligns channel windows and time blocks to them decodes nothing twice,
/// without a field selection: on MSv4 the runs of a field selection that
/// share a chunk row each decode it.
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
    /// Times the traversal delivers: the grid's, or those the request's
    /// time and field selections hold.
    pub times: usize,
    /// Which they are, as indices into the grid's times.
    pub time_runs: TimeRuns,
    /// Which rule and variable supply the delivered weights, when the
    /// weights role is read.
    pub weights: Option<WeightSource>,
    /// Baselines on its grid.
    pub baselines: usize,
    /// Bytes one sample of the requested roles holds in a batch
    /// ([`Roles::sample_bytes`]).
    pub bytes_per_sample: usize,
    /// Bytes one time holds in a batch besides its samples'
    /// ([`Roles::time_bytes`]).
    pub bytes_per_time: usize,
    /// Bytes per sample the cursor's own scratch holds while it fills a
    /// batch: what it decodes before gathering a correlation subset,
    /// converting flags or placing rows around padding.
    pub scratch_bytes_per_sample: usize,
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
    /// The fraction of grid cells the adapter pads because no acquisition
    /// fills them: what an MSv2 adapter places rows around. An MSv4 store
    /// holds its grid whole, so its adapter pads nothing and reports 0;
    /// that says nothing of cells the store itself holds as padding
    /// (XRADIO's NaN cells), which the delivered flags mark.
    pub padded_fraction: f64,
}

impl PartitionEstimate {
    /// Samples per full batch.
    #[must_use]
    pub fn samples_per_batch(&self) -> usize {
        self.times_per_batch * self.baselines
    }

    /// Bytes the arrays of a full batch hold, exactly as
    /// [`BatchBuffers::begin`](crate::BatchBuffers::begin) reserves them:
    /// the envelope a [`BufferPool`](crate::BufferPool) buffer needs for
    /// this traversal.
    #[must_use]
    pub fn buffer_bytes(&self) -> usize {
        self.times_per_batch * (self.baselines * self.bytes_per_sample + self.bytes_per_time)
    }

    /// Bytes per time the budget counts: the batch's, samples and time,
    /// and the cursor's scratch for it.
    #[must_use]
    pub fn held_bytes_per_time(&self) -> usize {
        self.baselines * (self.bytes_per_sample + self.scratch_bytes_per_sample)
            + self.bytes_per_time
    }

    /// Bytes a full batch and the cursor's scratch for it hold: at most the
    /// request's budget.
    #[must_use]
    pub fn held_bytes(&self) -> usize {
        self.times_per_batch * self.held_bytes_per_time()
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
/// `times`, when the budget counts `held_bytes_per_time` per time
/// ([`PartitionEstimate::held_bytes_per_time`]).
///
/// # Errors
///
/// When one time exceeds the budget.
pub fn times_per_batch(
    batch_bytes: usize,
    held_bytes_per_time: usize,
    times: usize,
) -> Result<usize, ObservationError> {
    let fit = (batch_bytes / held_bytes_per_time.max(1)).min(times);
    if fit == 0 && times > 0 {
        return Err(ObservationError::InvalidRequest {
            reason: format!(
                "one time holds {held_bytes_per_time} B, more than the {batch_bytes} B batch \
                 budget"
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

    /// The largest full batch's arrays over the set
    /// ([`PartitionEstimate::buffer_bytes`]): the envelope of a
    /// [`BufferPool`](crate::BufferPool) that serves the whole traversal and
    /// the pieces packed from its batches, so the pool retains at most its
    /// capacity times this.
    #[must_use]
    pub fn peak_buffer_bytes(&self) -> usize {
        self.partitions
            .iter()
            .map(PartitionEstimate::buffer_bytes)
            .max()
            .unwrap_or(0)
    }

    /// The most one batch and its cursor's decode scratch hold over the
    /// set: at most the request's budget.
    #[must_use]
    pub fn peak_held_bytes(&self) -> usize {
        self.partitions
            .iter()
            .map(PartitionEstimate::held_bytes)
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

    fn entry(time_scale: EpochRef, time_origin: MjdHighPrec) -> PartitionEntry {
        PartitionEntry {
            spectral_window: "spw".to_string(),
            frequencies_hz: vec![1.0e9],
            frequency_frame: casa_types::measures::frequency::FrequencyRef::LSRK,
            channel_widths_hz: vec![1.0e8],
            correlations: vec![Correlation::XX],
            intents: Vec::new(),
            observation: crate::catalogue::ObservationId(0),
            feeds: None,
            fields: Vec::new(),
            scans: Vec::new(),
            baselines: Vec::new(),
            time_scale,
            time_origin,
            data_groups: Vec::new(),
        }
    }

    /// A range selects the grid times it holds, exactly at its ends; one
    /// holding none (between two times, past either end) selects nothing;
    /// a range in another scale or origin, or not an interval, is refused.
    #[test]
    fn a_time_range_selects_the_grid_times_it_holds() {
        let origin = MjdHighPrec::new(0.0, 0.0);
        let entry = entry(EpochRef::UTC, origin);
        let times = [10.0, 20.0, 30.0, 40.0];
        let within = |start_s, end_s| {
            TimeSelection::Within(TimeRange {
                scale: EpochRef::UTC,
                origin,
                start_s,
                end_s,
            })
            .indices(&entry, &times)
        };
        assert_eq!(TimeSelection::All.indices(&entry, &times).unwrap(), 0..4);
        assert_eq!(within(20.0, 40.0).unwrap(), 1..3);
        assert_eq!(within(15.0, 35.0).unwrap(), 1..3);
        assert_eq!(within(f64::NEG_INFINITY, f64::INFINITY).unwrap(), 0..4);
        assert!(within(21.0, 29.0).unwrap().is_empty());
        assert!(within(50.0, f64::INFINITY).unwrap().is_empty());
        assert!(within(0.0, 10.0).unwrap().is_empty());
        assert!(within(30.0, 20.0).is_err());
        assert!(within(f64::NAN, 20.0).is_err());
        let tai = TimeSelection::Within(TimeRange {
            scale: EpochRef::TAI,
            origin,
            start_s: 0.0,
            end_s: 50.0,
        });
        assert!(tai.indices(&entry, &times).is_err());
        let unix = TimeSelection::Within(TimeRange {
            scale: EpochRef::UTC,
            origin: MjdHighPrec::new(40_587.0, 0.0),
            start_s: 0.0,
            end_s: 50.0,
        });
        assert!(unix.indices(&entry, &times).is_err());
    }

    /// A range stated from grid indices selects exactly those times, and
    /// ranges of consecutive index ranges tile the grid.
    #[test]
    fn ranges_of_grid_indices_select_those_times() {
        let grid = TimeBaselineGrid {
            baselines: Vec::new(),
            time_scale: EpochRef::UTC,
            time_origin: MjdHighPrec::new(40_587.0, 0.0),
            times_s: vec![
                1.7e9 + 0.1,
                1.7e9 + 1.3,
                1.7e9 + 2.7,
                1.7e9 + 3.9,
                1.7e9 + 5.2,
            ],
            field_per_time: Vec::new(),
            scan_per_time: Vec::new(),
            interval_s: 1.0,
        };
        let entry = entry(grid.time_scale, grid.time_origin);
        for (start, end) in [(0, 2), (2, 5), (1, 4), (0, 5), (3, 3), (5, 5)] {
            let selection = TimeSelection::Within(TimeRange::of_grid(&grid, start..end));
            assert_eq!(
                selection.indices(&entry, &grid.times_s).unwrap(),
                start..end,
                "{start}..{end}"
            );
        }
    }

    #[test]
    fn the_budget_fits_whole_times_or_refuses() {
        let bytes = Roles::ALL.sample_bytes(8, 4, true, 3);
        assert_eq!(bytes, 32 * 8 + 32 + 32 * 4 + 24 + 24);
        let time = 10 * bytes + Roles::ALL.time_bytes(0);
        assert_eq!(times_per_batch(time * 4, time, 12).expect("fit"), 4);
        assert_eq!(times_per_batch(time * 5 - 1, time, 12).expect("fit"), 4);
        assert_eq!(times_per_batch(usize::MAX, time, 12).expect("fit"), 12);
        assert!(times_per_batch(time - 1, time, 12).is_err());
        assert_eq!(times_per_batch(0, time, 0).expect("empty"), 0);
    }
}

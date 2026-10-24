// SPDX-License-Identifier: LGPL-3.0-or-later
//! What a consumer asks of a source, and what the source binds.

use std::ops::Range;

use crate::catalogue::PartitionId;

/// Which roles of a data group a traversal reads. A role left out is not
/// read at all: a flags-and-weights traversal decodes no visibilities.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Roles {
    /// Visibilities.
    pub values: bool,
    /// Flags, and in MSv2 `FLAG_ROW`.
    pub flags: bool,
    /// Weights.
    pub weights: bool,
    /// UVW.
    pub uvw: bool,
}

impl Roles {
    /// Every role.
    pub const ALL: Self = Self {
        values: true,
        flags: true,
        weights: true,
        uvw: true,
    };
}

/// The element order of the delivered payload. The trial implements the
/// sample-major order only; channel-major is the contract's other admitted
/// layout and is refused at binding.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Layout {
    /// `[sample, channel, correlation]`, correlation fastest.
    SampleMajor,
    /// `[channel, sample, correlation]`.
    ChannelMajor,
}

/// The visibility precision the consumer accepts.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Precision {
    /// The stored precision.
    Native,
    /// `Complex<f32>`; a `complex128` store is refused unless
    /// `allow_narrowing`, which the trial does not implement.
    Complex32 {
        /// Whether narrowing `complex128` is approved.
        allow_narrowing: bool,
    },
}

/// How samples form scientific groups.
#[derive(Clone, Copy, Debug, PartialEq)]
pub enum Grouping {
    /// No groups; batches carry no fragments.
    None,
    /// Time bins of `width_s` seconds per partition and field: bin
    /// `floor((t - t0) / width_s)`, `t0` the first selected time of the
    /// partition and field. This is casa-rs `gaincal`'s `solint` rule.
    TimeBins {
        /// Bin width in seconds.
        width_s: f64,
        /// Whether one group spans every field.
        combine_fields: bool,
    },
}

/// When a source announces that a group is complete.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Closure {
    /// Mark the closing fragment `Last` or `Whole` whenever the traversal
    /// can already prove it at delivery.
    AtDelivery,
    /// Never mark fragments closing; send [`ReadEvent::GroupEnd`] once the
    /// traversal has read past the group. A trial knob that exercises the
    /// deferred path a source without lookahead must take.
    ///
    /// [`ReadEvent::GroupEnd`]: crate::ReadEvent::GroupEnd
    Deferred,
}

/// What a consumer asks one traversal to deliver.
#[derive(Clone, Debug, PartialEq)]
pub struct AccessRequest {
    /// The partition to read. The trial reads one partition per traversal.
    pub partition: PartitionId,
    /// The data group, by name.
    pub data_group: String,
    /// The roles to read.
    pub roles: Roles,
    /// The partition's native channels to deliver.
    pub channels: Range<usize>,
    /// The payload's element order.
    pub layout: Layout,
    /// The visibility precision.
    pub precision: Precision,
    /// Scientific groups.
    pub grouping: Grouping,
    /// When groups are announced complete.
    pub closure: Closure,
    /// The most samples one batch may hold. A source may deliver fewer; a
    /// factored source rounds down to whole times and fails binding if one
    /// time does not fit.
    pub max_samples: usize,
}

/// How the source realized a request, decided at binding.
#[derive(Clone, Debug, PartialEq)]
pub struct Realization {
    /// The delivered layout.
    pub layout: Layout,
    /// Whether batches are factored (`TimeBaseline`) or listed
    /// (`Irregular`).
    pub factored: bool,
    /// Samples per full batch.
    pub samples_per_batch: usize,
    /// Batches in one traversal.
    pub batches: usize,
    /// Payload bytes of one full batch the caller's buffers hold.
    pub batch_payload_bytes: usize,
    /// Physical read units (casacore cells, Zarr chunks) one traversal
    /// decodes, and the elements they hold.
    pub decode_units: usize,
    /// Elements the decoded units hold, over every role read.
    pub decoded_elements: usize,
    /// Elements the request selects, over every role read; the ratio to
    /// `decoded_elements` is the read amplification.
    pub selected_elements: usize,
}

impl Realization {
    /// Decoded elements per selected element.
    #[must_use]
    pub fn amplification(&self) -> f64 {
        if self.selected_elements == 0 {
            0.0
        } else {
            self.decoded_elements as f64 / self.selected_elements as f64
        }
    }
}

/// What a cursor has done so far, counted at batch granularity.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct IoCounters {
    /// Batches delivered.
    pub batches: usize,
    /// Storage read calls issued (one per column or variable per batch).
    pub read_calls: usize,
    /// Physical units decoded (Zarr chunks; casacore cells are not counted
    /// separately and equal the selected rows).
    pub decode_units: usize,
    /// Payload bytes written into the caller's buffers.
    pub delivered_bytes: usize,
    /// Elements copied after decoding, beyond the decode into the buffers.
    pub copied_elements: usize,
    /// Fills that had to grow a buffer.
    pub buffer_growths: usize,
}

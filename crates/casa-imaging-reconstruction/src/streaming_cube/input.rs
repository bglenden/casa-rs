// SPDX-License-Identifier: LGPL-3.0-or-later

//! Flat payload shared by preparation, native storage and band kernels.
//! Selected samples are packed directly during worker-local preparation;
//! historical sample objects never enter the buffer or numerical loops.

use std::{io, mem::size_of};

use casa_imaging_model::{CorrelationType, SelectedSampleAddress};
#[cfg(test)]
use casa_imaging_model::{FiniteValuePolicy, SelectedVisibilitySample};
use num_complex::Complex64;
use smallvec::SmallVec;

#[cfg(test)]
use crate::{WeightingSampleValue, spectral_operator::accept_polarization_input};

fn invalid(message: &'static str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidInput, message)
}

/// Once-per-row geometry; channel frequencies are a separate contiguous array.
#[derive(Clone, Copy, Debug, Default, PartialEq)]
pub struct RowMetadata {
    /// MAIN row in the layout's MeasurementSet.
    pub physical_row: u64,
    /// Projected baseline for the single image domain.
    pub uvw_m: [f64; 3],
    /// Projected phase path length.
    pub phase_shift_m: f64,
    /// Original selected-vector pair, preserving global interpolation phase.
    pub original_pair_hz: [f64; 2],
}

/// Shared identity and selected axes for one homogeneous source layout.
/// Frequency fields in `address` describe the original first native channel;
/// row-transformed frequencies live in the payload, not this descriptor.
#[derive(Clone, Debug)]
pub struct NativeLayout {
    /// Coarse source/SPW/polarization identity and original first-channel address.
    pub address: SelectedSampleAddress,
    /// Increasing physical MS channel indices, including any selection gaps.
    pub channels: Vec<u32>,
    /// Selected array indices and physical correlations in payload order.
    pub correlations: SmallVec<[(u32, CorrelationType); 4]>,
}

impl NativeLayout {
    /// Validate the selected layout before allocating/filling payload blocks.
    pub fn new(
        address: SelectedSampleAddress,
        channels: Vec<u32>,
        correlations: SmallVec<[(u32, CorrelationType); 4]>,
    ) -> io::Result<Self> {
        if channels.len() < 2
            || channels.windows(2).any(|pair| pair[0] >= pair[1])
            || !(1..=4).contains(&correlations.len())
            || correlations.windows(2).any(|pair| pair[0].0 >= pair[1].0)
            || address.channel_index != channels[0]
            || (address.correlation_index, address.correlation_type) != correlations[0]
        {
            return Err(invalid("invalid homogeneous native layout"));
        }
        Ok(Self {
            address,
            channels,
            correlations,
        })
    }

    pub(crate) fn contains_source(&self, address: SelectedSampleAddress) -> bool {
        address.measurement_set == self.address.measurement_set
            && address.data_description_id == self.address.data_description_id
            && address.spectral_window_id == self.address.spectral_window_id
            && address.polarization_id == self.address.polarization_id
            && address.frequency_frame == self.address.frequency_frame
    }
}

/// Reusable row/channel/correlation arrays. Correlation is contiguous;
/// frequencies are row/channel. Source/channel/correlation identities are shared
/// by `NativeLayout`. No sample owns workflow state or another allocation.
#[derive(Debug)]
pub struct NativeBlock {
    /// One geometry record per row.
    pub metadata: Vec<RowMetadata>,
    /// Row/channel centres transformed into the imaging output frame.
    pub frequencies_hz: Vec<f64>,
    /// Selected visibilities in reconstruction precision.
    pub values: Vec<Complex64>,
    /// Native imaging weights, before nearest-channel transfer.
    pub weights: Vec<f64>,
    /// Rejected input flags, before pair interpolation.
    pub flags: Vec<bool>,
    /// Native weight-group flags, transferred from the nearest endpoint.
    pub weight_flags: Vec<bool>,
    /// Active channel count per row.
    pub channels: usize,
    /// Correlation count per channel.
    pub correlations: usize,
    maximum_rows: usize,
    maximum_channels: usize,
}

impl NativeBlock {
    /// Required heap allocation plus owner headers for an admitted shape.
    pub fn required_bytes(
        maximum_rows: usize,
        maximum_channels: usize,
        correlations: usize,
    ) -> io::Result<usize> {
        if maximum_rows == 0 || maximum_channels == 0 || !(1..=4).contains(&correlations) {
            return Err(invalid("invalid native block dimensions"));
        }
        maximum_rows
            .checked_mul(maximum_channels)
            .and_then(|cells| cells.checked_mul(8 + 26 * correlations))
            .and_then(|bytes| {
                bytes.checked_add(maximum_rows.checked_mul(size_of::<RowMetadata>())?)
            })
            .and_then(|bytes| bytes.checked_add(size_of::<Self>()))
            .filter(|bytes| *bytes <= isize::MAX as usize)
            .ok_or_else(|| invalid("native block dimensions overflow"))
    }

    /// Allocate a planner-admitted block after checking dimension arithmetic.
    /// Resource admission belongs to the runtime, not to this data container.
    pub fn new(
        maximum_rows: usize,
        maximum_channels: usize,
        correlations: usize,
    ) -> io::Result<Self> {
        Self::required_bytes(maximum_rows, maximum_channels, correlations)?;
        let cells = maximum_rows * maximum_channels;
        let samples = cells * correlations;
        Ok(Self {
            metadata: vec![RowMetadata::default(); maximum_rows],
            frequencies_hz: vec![0.0; cells],
            values: vec![Complex64::default(); samples],
            weights: vec![0.0; samples],
            flags: vec![false; samples],
            weight_flags: vec![false; samples],
            channels: maximum_channels,
            correlations,
            maximum_rows,
            maximum_channels,
        })
    }

    /// Reuse the allocation for a shorter row block or channel window.
    pub fn set_shape(&mut self, rows: usize, channels: usize) -> io::Result<()> {
        if rows == 0
            || rows > self.maximum_rows
            || channels == 0
            || channels > self.maximum_channels
        {
            return Err(invalid("native block exceeds reserved capacity"));
        }
        self.metadata.resize(rows, RowMetadata::default());
        self.frequencies_hz.resize(rows * channels, 0.0);
        let samples = rows * channels * self.correlations;
        self.values.resize(samples, Complex64::default());
        self.weights.resize(samples, 0.0);
        self.flags.resize(samples, false);
        self.weight_flags.resize(samples, false);
        self.channels = channels;
        Ok(())
    }

    /// Heap capacities plus this owner's headers, excluding the shared layout.
    pub fn capacity_bytes(&self) -> usize {
        size_of::<Self>()
            + self.metadata.capacity() * size_of::<RowMetadata>()
            + self.frequencies_hz.capacity() * size_of::<f64>()
            + self.values.capacity() * size_of::<Complex64>()
            + self.weights.capacity() * size_of::<f64>()
            + self.flags.capacity()
            + self.weight_flags.capacity()
    }

    /// Check shape at the storage/owner boundary without reading payload values.
    pub fn validate_shape(
        &self,
        rows: usize,
        channels: usize,
        correlations: usize,
    ) -> io::Result<()> {
        let cells = rows
            .checked_mul(channels)
            .ok_or_else(|| invalid("native shape overflow"))?;
        let samples = cells
            .checked_mul(correlations)
            .ok_or_else(|| invalid("native shape overflow"))?;
        if self.channels != channels
            || self.correlations != correlations
            || self.metadata.len() != rows
            || self.frequencies_hz.len() != cells
            || self.values.len() != samples
            || self.weights.len() != samples
            || self.flags.len() != samples
            || self.weight_flags.len() != samples
        {
            return Err(invalid(
                "native block arrays do not match the planned shape",
            ));
        }
        Ok(())
    }
}

/// Worker-local packing into an admitted native block. Weighted values exist
/// only for the duration of each shared-science call, never as a retained batch.
#[cfg(test)]
pub(crate) struct NativeInput {
    layout: NativeLayout,
    block: NativeBlock,
    finite_values: FiniteValuePolicy,
    next: usize,
    #[cfg(test)]
    rows: u64,
    previous_row: Option<u64>,
    failed: bool,
}

#[cfg(test)]
impl NativeInput {
    /// Consume an admitted full-channel buffer; no extra payload allocation.
    pub(crate) fn new(
        layout: NativeLayout,
        block: NativeBlock,
        finite_values: FiniteValuePolicy,
    ) -> io::Result<Self> {
        block.validate_shape(
            block.maximum_rows,
            layout.channels.len(),
            layout.correlations.len(),
        )?;
        Ok(Self {
            layout,
            block,
            finite_values,
            next: 0,
            #[cfg(test)]
            rows: 0,
            previous_row: None,
            failed: false,
        })
    }

    /// Reference-test adapter for arbitrary historical chunk boundaries.
    #[cfg(test)]
    pub fn push(
        &mut self,
        samples: &[WeightingSampleValue],
        mut emit: impl FnMut(&NativeBlock) -> io::Result<()>,
    ) -> io::Result<()> {
        if self.failed {
            return Err(invalid("native input previously failed"));
        }
        let result = (|| {
            for sample in samples {
                self.append_sample(sample)?;
                if self.next == self.block.values.len() {
                    emit(&self.block)?;
                    self.rows += self.block.metadata.len() as u64;
                    self.next = 0;
                }
            }
            Ok(())
        })();
        self.failed = result.is_err();
        result
    }

    fn append_sample(&mut self, weighted: &WeightingSampleValue) -> io::Result<()> {
        let row_samples = self.block.channels * self.block.correlations;
        let sample = weighted.selected();
        let address = sample.address();
        let row = self.next / row_samples;
        let channel = (self.next % row_samples) / self.block.correlations;
        let correlation = self.next % self.block.correlations;
        if !self.layout.contains_source(address)
            || address.channel_index != self.layout.channels[channel]
            || (address.correlation_index, address.correlation_type)
                != self.layout.correlations[correlation]
        {
            return Err(invalid("native input source or selected order mismatch"));
        }
        if channel == 0 && correlation == 0 {
            if self
                .previous_row
                .is_some_and(|previous| address.physical_row <= previous)
            {
                return Err(invalid("native input row order mismatch"));
            }
            let geometry = sample
                .row_spectral_geometry()
                .ok_or_else(|| invalid("missing native row geometry"))?;
            if geometry.channels != self.block.channels
                || geometry.first.0 != self.layout.channels[0]
                || geometry.second.map(|second| second.0) != Some(self.layout.channels[1])
                || sample.domain_projections().len() != 1
            {
                return Err(invalid(
                    "native input geometry does not match the single-domain layout",
                ));
            }
            self.block.metadata[row] = RowMetadata {
                physical_row: address.physical_row,
                uvw_m: sample.transformed_uvw_m(),
                phase_shift_m: sample.phase_shift_m(),
                original_pair_hz: geometry
                    .first_pair_hz()
                    .ok_or_else(|| invalid("missing original native frequency pair"))?,
            };
            self.previous_row = Some(address.physical_row);
        } else if address.physical_row != self.block.metadata[row].physical_row {
            return Err(invalid(
                "native input row ended before its selected samples",
            ));
        }
        if correlation == 0 {
            self.block.frequencies_hz[row * self.block.channels + channel] =
                sample.output_frame_frequency_hz();
        }
        self.block.values[self.next] = match sample.visibility() {
            SelectedVisibilitySample::Float32(value) => Complex64::new(f64::from(value), 0.0),
            SelectedVisibilitySample::Complex32([re, im]) => {
                Complex64::new(f64::from(re), f64::from(im))
            }
        };
        self.block.weights[self.next] = weighted
            .source_imaging_weight()
            .ok_or_else(|| invalid("missing native imaging weight"))?;
        self.block.flags[self.next] =
            !accept_polarization_input(sample, self.finite_values).map_err(io::Error::other)?;
        self.block.weight_flags[self.next] =
            sample.input_weight_group_flag || sample.parallel_hand_group_flag || sample.row_flag;
        self.next += 1;
        Ok(())
    }

    /// Emit a final short block, rejecting incomplete rows and earlier errors.
    /// Return the row count and original shared layout by ownership transfer.
    /// This checks shape/count only; there is no payload verification pass.
    #[cfg(test)]
    pub fn finish(
        mut self,
        mut emit: impl FnMut(&NativeBlock) -> io::Result<()>,
    ) -> io::Result<(u64, NativeLayout)> {
        let row_samples = self.block.channels * self.block.correlations;
        if self.failed || self.next % row_samples != 0 || (self.rows == 0 && self.next == 0) {
            return Err(invalid("failed, empty or incomplete native input"));
        }
        if self.next != 0 {
            let rows = self.next / row_samples;
            self.block.set_shape(rows, self.block.channels)?;
            emit(&self.block)?;
            self.rows += rows as u64;
        }
        Ok((self.rows, self.layout))
    }
}

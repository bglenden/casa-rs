// SPDX-License-Identifier: LGPL-3.0-or-later
//! Placed visibility samples: the kernel-facing data layout.

use num_complex::Complex32;

use crate::native::NativeSource;

/// Two-level convolution-function cell key (HPG `CFSimpleIndexer`).
///
/// `group` linearises the variable-support axes of a kernel set (w-plane,
/// frequency cell, parallactic-angle cell, antenna-type pair); `cube`
/// indexes fixed-size slices inside the group. Mueller planes live inside
/// the cell, not in the key, and conjugation is decided in the kernel from
/// the sign of `w` and the direction of the transform. Baseline-order
/// conjugation for heterogeneous arrays is baked into the cell by the kernel
/// set. The standard spheroidal set has a single cell.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash)]
pub struct CfKey {
    /// Variable-support group.
    pub group: u16,
    /// Fixed-size slice inside the group.
    pub cube: u16,
}

/// One selected row×channel sample after flagging, phase-centre shift and
/// correlation routing, ready for a kernel.
///
/// Flagged samples are never placed, and samples whose kernel support would
/// leave the padded grid are dropped at placement
/// ([`GridGeometry::fits`](crate::GridGeometry::fits)), so kernels have no
/// bounds tests. `u`, `v` and `w` are in wavelengths at this sample's
/// frequency; the integer grid cell and fine offset are derived in the
/// kernel by [`GridGeometry::locate`](crate::GridGeometry::locate), once per
/// sample, with one rule for every backend.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct Placement {
    /// Baseline `u` in wavelengths.
    pub u: f64,
    /// Baseline `v` in wavelengths.
    pub v: f64,
    /// Baseline `w` in wavelengths in the image frame. Its sign selects the
    /// adjoint's Mueller table and tap conjugation (taps conjugated for
    /// `w > 0`, `accumulateToGrid.inc`).
    pub w: f64,
    /// Whether the w the prediction keys on is positive. It selects the
    /// forward Mueller table and tap conjugation (taps conjugated for
    /// `w ≤ 0`, `accumulateFromGrid.inc`): `w > 0` for every set but AW,
    /// whose prediction reads the MeasurementSet w before the phase-centre
    /// rotation
    /// ([`ConvolutionFunctionSet::prediction_w`](crate::ConvolutionFunctionSet::prediction_w)).
    pub prediction_w_positive: bool,
    /// Phase-centre shift argument in radians. The block's values already
    /// carry `e^{iφ}`; prediction applies `e^{−iφ}`. The pointing ramp's
    /// phase at the sample's fine offset belongs to the kernels, which
    /// anchor the ramp at the sample in every mode (CASA reads the ramped
    /// kernel at `ix · sampling + off`).
    pub phase: f64,
    /// Target grid plane: the output channel for a channel-local basis,
    /// 0 for a constant or Taylor basis.
    pub plane: u32,
    /// Taylor expansion variable `(ν − ν₀)/ν₀`; read only by a Taylor basis.
    pub spectral: f32,
    /// Convolution-function cell, resolved by the kernel set.
    pub cf: CfKey,
    /// Pointing phase gradient in radians per grid cell, applied to dense
    /// kernels as `e^{i(k_x g_x + k_y g_y)}` over the taps, with `k`
    /// counted from the kernel centre; zero for sets without a per-row ramp.
    pub gradient: [f32; 2],
}

/// Structure-of-arrays view of one bounded block of placed samples.
///
/// `values` and `weights` hold `npol` entries per placement, sample-major:
/// entry `i * npol + p` belongs to placement `i`, visibility polarization
/// `p`. Values are the visibility pre-multiplied once by the imaging weight
/// and the phase-centre phasor (`W · V · e^{iφ}`); weights are the imaging
/// weights `W`. CASA's weighting rule gives every polarization of a sample
/// the same weight and drops the sample when any selected correlation is
/// flagged, so a placed sample has no zero weights; a kernel still skips a
/// polarization whose weight is zero. A block that is only predicted
/// ([`Work::Predict`](crate::Work::Predict)) may hold no values or weights:
/// a prediction reads only the placements. A block gridded as a residual at
/// native channels ([`Work::NativeResidualGrid`](crate::Work::NativeResidualGrid))
/// also holds each placement's native endpoints in `sources`; other blocks
/// hold none.
#[derive(Clone, Copy, Debug)]
pub struct SampleBlock<'a> {
    /// Placed samples.
    pub placements: &'a [Placement],
    /// `W · V · e^{iφ}` per placement and polarization.
    pub values: &'a [Complex32],
    /// Imaging weight `W` per placement and polarization.
    pub weights: &'a [f32],
    /// Native endpoints per placement, or none.
    pub sources: &'a [NativeSource],
    /// Visibility polarizations per placement.
    pub npol: usize,
}

impl<'a> SampleBlock<'a> {
    /// Number of placements.
    #[must_use]
    pub const fn len(&self) -> usize {
        self.placements.len()
    }

    /// Whether the block holds no placements.
    #[must_use]
    pub const fn is_empty(&self) -> bool {
        self.placements.is_empty()
    }

    /// Values of placement `index`, one per polarization.
    #[must_use]
    pub fn values_of(&self, index: usize) -> &'a [Complex32] {
        &self.values[index * self.npol..(index + 1) * self.npol]
    }

    /// Weights of placement `index`, one per polarization.
    #[must_use]
    pub fn weights_of(&self, index: usize) -> &'a [f32] {
        &self.weights[index * self.npol..(index + 1) * self.npol]
    }
}

/// Owned structure-of-arrays storage behind a [`SampleBlock`].
///
/// The spectral resampler fills one buffer per native block and a backend
/// reads it through [`SampleBuffer::block`]. Capacity survives
/// [`SampleBuffer::clear`], so a streaming producer allocates once.
#[derive(Clone, Debug)]
pub struct SampleBuffer {
    placements: Vec<Placement>,
    values: Vec<Complex32>,
    weights: Vec<f32>,
    sources: Vec<NativeSource>,
    npol: usize,
}

impl SampleBuffer {
    /// Empty buffer for `npol` visibility polarizations per placement.
    #[must_use]
    pub fn new(npol: usize) -> Self {
        assert!(npol > 0, "a sample block needs at least one polarization");
        Self {
            placements: Vec::new(),
            values: Vec::new(),
            weights: Vec::new(),
            sources: Vec::new(),
            npol,
        }
    }

    /// Empty buffer for `npol` visibility polarizations per placement with
    /// room for `placements` placements, so filling it with at most that
    /// many never grows it beyond [`Self::bytes`].
    #[must_use]
    pub fn with_capacity(npol: usize, placements: usize) -> Self {
        assert!(npol > 0, "a sample block needs at least one polarization");
        Self {
            placements: Vec::with_capacity(placements),
            values: Vec::with_capacity(placements * npol),
            weights: Vec::with_capacity(placements * npol),
            sources: Vec::new(),
            npol,
        }
    }

    /// [`Self::with_capacity`] with room for each placement's native
    /// endpoints too ([`Self::push_sourced`]), within
    /// [`Self::sourced_bytes`].
    #[must_use]
    pub fn with_source_capacity(npol: usize, placements: usize) -> Self {
        Self {
            sources: Vec::with_capacity(placements),
            ..Self::with_capacity(npol, placements)
        }
    }

    /// Bytes a buffer of `npol` polarizations holds with room for
    /// `placements` placements ([`Self::with_capacity`]).
    #[must_use]
    pub const fn bytes(npol: usize, placements: usize) -> u64 {
        (placements * (size_of::<Placement>() + npol * (size_of::<Complex32>() + size_of::<f32>())))
            as u64
    }

    /// Bytes [`Self::with_source_capacity`] allocates.
    #[must_use]
    pub const fn sourced_bytes(npol: usize, placements: usize) -> u64 {
        Self::bytes(npol, placements) + (placements * size_of::<NativeSource>()) as u64
    }

    /// Bytes the buffer holds now, from its capacities: [`Self::bytes`] or
    /// [`Self::sourced_bytes`] for a buffer made with that room that has
    /// not grown.
    #[must_use]
    pub fn capacity_bytes(&self) -> u64 {
        (self.placements.capacity() * size_of::<Placement>()
            + self.values.capacity() * size_of::<Complex32>()
            + self.weights.capacity() * size_of::<f32>()
            + self.sources.capacity() * size_of::<NativeSource>()) as u64
    }

    /// Visibility polarizations per placement.
    #[must_use]
    pub const fn npol(&self) -> usize {
        self.npol
    }

    /// Number of placements.
    #[must_use]
    pub const fn len(&self) -> usize {
        self.placements.len()
    }

    /// Whether the buffer holds no placements.
    #[must_use]
    pub const fn is_empty(&self) -> bool {
        self.placements.is_empty()
    }

    /// Append one placement with its `npol` pre-multiplied values and weights.
    pub fn push(&mut self, placement: Placement, values: &[Complex32], weights: &[f32]) {
        assert_eq!(values.len(), self.npol, "values per placement");
        assert_eq!(weights.len(), self.npol, "weights per placement");
        self.placements.push(placement);
        // Element by element: a slice append of a few values is a library
        // copy call per sample.
        for (&value, &weight) in values.iter().zip(weights) {
            self.values.push(value);
            self.weights.push(weight);
        }
    }

    /// [`Self::push`], with the placement's native endpoints. A buffer holds
    /// endpoints for every placement or for none.
    pub fn push_sourced(
        &mut self,
        placement: Placement,
        values: &[Complex32],
        weights: &[f32],
        source: NativeSource,
    ) {
        debug_assert_eq!(
            self.sources.len(),
            self.placements.len(),
            "every placement has its endpoints"
        );
        self.push(placement, values, weights);
        self.sources.push(source);
    }

    /// Append every placement of `block`, which has this buffer's
    /// polarizations, with its values, weights and any native endpoints.
    pub fn extend_from_block(&mut self, block: &SampleBlock<'_>) {
        assert_eq!(block.npol, self.npol, "values per placement");
        self.placements.extend_from_slice(block.placements);
        self.values.extend_from_slice(block.values);
        self.weights.extend_from_slice(block.weights);
        self.sources.extend_from_slice(block.sources);
    }

    /// Remove every placement, keeping the allocation.
    pub fn clear(&mut self) {
        self.placements.clear();
        self.values.clear();
        self.weights.clear();
        self.sources.clear();
    }

    /// Borrow the contents as a kernel block.
    #[must_use]
    pub fn block(&self) -> SampleBlock<'_> {
        SampleBlock {
            placements: &self.placements,
            values: &self.values,
            weights: &self.weights,
            sources: &self.sources,
            npol: self.npol,
        }
    }

    /// Placements in insertion order.
    #[must_use]
    pub fn placements(&self) -> &[Placement] {
        &self.placements
    }
}

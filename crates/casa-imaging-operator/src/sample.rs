// SPDX-License-Identifier: LGPL-3.0-or-later
//! Placed visibility samples: the kernel-facing data layout.

use num_complex::Complex32;

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
    /// Baseline `w` in wavelengths; its sign selects the Mueller table for
    /// each direction. Adjoint taps are conjugated for `w > 0`, forward
    /// taps for `w ≤ 0`.
    pub w: f64,
    /// Prediction's sign of w when it differs from the image-frame rule.
    /// `None` uses `w > 0`. AW prediction uses original MS w; gridding
    /// continues to use the rotated `w` (`AWVisResampler::GridToData`).
    pub prediction_w_positive: Option<bool>,
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
/// polarization whose weight is zero.
#[derive(Clone, Copy, Debug)]
pub struct SampleBlock<'a> {
    /// Placed samples.
    pub placements: &'a [Placement],
    /// `W · V · e^{iφ}` per placement and polarization.
    pub values: &'a [Complex32],
    /// Imaging weight `W` per placement and polarization.
    pub weights: &'a [f32],
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
            npol,
        }
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
        self.values.extend_from_slice(values);
        self.weights.extend_from_slice(weights);
    }

    /// Remove every placement, keeping the allocation.
    pub fn clear(&mut self) {
        self.placements.clear();
        self.values.clear();
        self.weights.clear();
    }

    /// Borrow the contents as a kernel block.
    #[must_use]
    pub fn block(&self) -> SampleBlock<'_> {
        SampleBlock {
            placements: &self.placements,
            values: &self.values,
            weights: &self.weights,
            npol: self.npol,
        }
    }

    /// Placements in insertion order.
    #[must_use]
    pub fn placements(&self) -> &[Placement] {
        &self.placements
    }
}

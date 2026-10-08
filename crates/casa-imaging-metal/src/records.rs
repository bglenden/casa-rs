// SPDX-License-Identifier: LGPL-3.0-or-later
//! Host preparation of the records the device kernels read.
//!
//! Every sample is located on the host with the operator's one rounding rule
//! ([`GridGeometry::locate`](casa_imaging_operator::GridGeometry::locate), in
//! `f64`), its support is checked against the target tile, and its kernel
//! norms come from [`TapLayout::norm`]; `sumwt` is accumulated here in `f64`
//! exactly as the CPU backend does, and the device receives only what the
//! tap loops need. The layouts below match `kernels.metal`.

use std::collections::HashMap;
use std::ops::Range;

use casa_imaging_operator::{
    AccumulatorLayout, CellLocation, CfKey, ConvolutionFunctionSet, Mode, SampleBlock, TapLayout,
};
use num_complex::{Complex32, Complex64};

/// Terms one dispatch may spread or gather (`MAX_TERMS` in the kernels).
pub(crate) const MAX_TERMS: usize = 16;
/// Grid or visibility polarizations of one sample (`MAX_POLS`).
pub(crate) const MAX_POLS: usize = 4;

// The host and kernel layouts must agree byte for byte.
const _: () = {
    assert!(size_of::<SampleRecord>() == crate::SAMPLE_BYTES);
    assert!(size_of::<TableRecord>() == 16);
    assert!(size_of::<Params>() == 128);
};

/// One located sample (`Sample` in the kernels).
#[repr(C)]
#[derive(Clone, Copy, Debug, Default, PartialEq)]
pub(crate) struct SampleRecord {
    /// First tap, relative to the accumulator tile.
    pub origin: [u32; 2],
    /// Fine-offset rows.
    pub fine: [u16; 2],
    /// Accumulator-local plane.
    pub plane: u32,
    /// Model-local plane.
    pub model_plane: u32,
    /// Kernel table.
    pub table: u32,
    /// Taylor variable.
    pub spectral: f32,
    /// Bit 0: `w > 0`.
    pub flags: u32,
    /// Pointing phase gradient.
    pub gradient: [f32; 2],
}

/// Where one kernel cell's taps sit in the device arenas (`Table`).
#[repr(C)]
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(crate) struct TableRecord {
    /// First value in the separable (`f32`) or dense (`float2`) arena.
    pub offset: u32,
    /// Taps per axis.
    pub support: [u16; 2],
    /// Fine offsets per cell.
    pub oversampling: u16,
    /// Mueller planes per tile.
    pub mueller_planes: u16,
    /// 0: separable real rows; 1: dense complex tiles.
    pub dense: u32,
}

/// Per-dispatch constants (`Params`).
#[repr(C)]
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(crate) struct Params {
    pub samples: u32,
    pub npol: u32,
    pub gpols: u32,
    pub terms: u32,
    pub term_base: u32,
    pub term_count: u32,
    pub model_terms: u32,
    pub weighted: u32,
    pub write_residual: u32,
    pub pad: u32,
    pub tile_shape: [u32; 2],
    pub tile_origin: [u32; 2],
    pub grid_shape: [u32; 2],
    pub adjoint: [[i8; 16]; 2],
    pub forward: [[i8; 16]; 2],
}

/// Which taps of a cell a dispatch reads.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub(crate) enum TapsKind {
    /// [`ConvolutionFunctionSet::taps`].
    Imaging,
    /// [`ConvolutionFunctionSet::weight_taps`], placed at the uv origin.
    Weight,
}

impl TapsKind {
    /// The kind a gridding mode reads.
    pub(crate) const fn of(mode: Mode) -> Self {
        match mode {
            Mode::Data | Mode::Psf => Self::Imaging,
            Mode::Weight => Self::Weight,
        }
    }

    /// The cell's taps of this kind; `None` when the set has no weight
    /// taps for it.
    pub(crate) fn taps(self, cf: &dyn ConvolutionFunctionSet, key: CfKey) -> Option<TapLayout<'_>> {
        match self {
            Self::Imaging => Some(cf.taps(key)),
            Self::Weight => cf.weight_taps(key),
        }
    }
}

/// One kernel cell known to the device: its table index and the norm of
/// every fine offset and Mueller plane.
#[derive(Debug)]
pub(crate) struct KnownTable {
    /// Index into the device's table list.
    pub index: u32,
    /// `norm[(oy · (oversampling + 1) + ox) · mueller_planes + m]`,
    /// unconjugated.
    norms: Vec<Complex64>,
    fine_rows: usize,
    mueller_planes: usize,
}

impl KnownTable {
    /// [`TapLayout::norm`] for every fine offset and Mueller plane of `taps`.
    pub(crate) fn new(index: u32, taps: &TapLayout<'_>) -> Self {
        let fine_rows = usize::from(taps.oversampling()) + 1;
        let mueller_planes = usize::from(taps.mueller_planes());
        let mut norms = Vec::with_capacity(fine_rows * fine_rows * mueller_planes);
        for oy in 0..fine_rows {
            for ox in 0..fine_rows {
                let location = CellLocation {
                    x: 0,
                    y: 0,
                    ox: ox as u16,
                    oy: oy as u16,
                };
                for mueller in 0..mueller_planes {
                    norms.push(taps.norm(location, mueller as u8, false));
                }
            }
        }
        Self {
            index,
            norms,
            fine_rows,
            mueller_planes,
        }
    }

    /// The norm of Mueller plane `mueller` at `location`'s fine offsets,
    /// conjugated for `w ≤ 0` as [`TapLayout::norm`] does.
    fn norm(&self, location: CellLocation, mueller: u8, conjugate: bool) -> Complex64 {
        let row = usize::from(location.oy) * self.fine_rows + usize::from(location.ox);
        let norm = self.norms[row * self.mueller_planes + usize::from(mueller)];
        if conjugate { norm.conj() } else { norm }
    }
}

/// The kernel cells a backend has given the device, by cell and kind.
pub(crate) type KnownTables = HashMap<(CfKey, TapsKind), KnownTable>;

/// What one dispatch reads and writes, for record preparation.
pub(crate) struct Targets<'a> {
    /// Which taps the samples use.
    pub kind: TapsKind,
    /// The accumulator the adjoint spreads into, with the mode's terms.
    pub adjoint: Option<(&'a AccumulatorLayout, Range<usize>)>,
    /// The model grids the forward transform gathers from.
    pub model: Option<&'a AccumulatorLayout>,
}

impl Targets<'_> {
    fn geometry(&self) -> &casa_imaging_operator::GridGeometry {
        self.adjoint
            .as_ref()
            .map(|(layout, _)| layout.geometry())
            .or(self.model.map(AccumulatorLayout::geometry))
            .expect("a dispatch has a target")
    }
}

/// Fill `records[j]` for samples `range` of `block` and, for a forward
/// transform, `inverse_norms` (`1/N` per visibility polarization, zero for
/// a zero norm); add the adjoint's `W·s^t·|N|` to `sumwt`.
///
/// Panics when a sample's support leaves the accumulator tile (the model's
/// grid for a prediction) or its plane lies outside a target: the placement
/// invariant the CPU backend asserts, checked here because the device
/// trusts every record.
#[allow(clippy::too_many_arguments)]
pub(crate) fn prepare(
    cf: &dyn ConvolutionFunctionSet,
    known: &KnownTables,
    block: &SampleBlock<'_>,
    range: Range<usize>,
    targets: &Targets<'_>,
    mut sumwt: Option<&mut [f64]>,
    records: &mut [SampleRecord],
    mut inverse_norms: Option<&mut [Complex32]>,
) {
    let npol = block.npol;
    let mueller = cf.mueller();
    let geometry = targets.geometry();
    let mut powers = [0.0_f64; MAX_TERMS];
    for (local, index) in range.enumerate() {
        let record = &mut records[local];
        let placement = &block.placements[index];
        let table = &known[&(placement.cf, targets.kind)];
        let taps = targets
            .kind
            .taps(cf, placement.cf)
            .expect("known tables exist");
        let (u, v) = match targets.kind {
            TapsKind::Imaging => (placement.u, placement.v),
            TapsKind::Weight => (0.0, 0.0),
        };
        let location = geometry.locate(u, v, taps.oversampling());
        let w_positive = placement.w > 0.0;
        let half = taps.half_support();
        let (origin, plane) = match &targets.adjoint {
            Some((layout, _)) => (
                tile_origin(layout, location, half),
                layout.planes().local(placement.plane) as u32,
            ),
            None => (
                tile_origin(
                    targets.model.expect("a prediction has model grids"),
                    location,
                    half,
                ),
                0,
            ),
        };
        let model_plane = targets
            .model
            .map_or(0, |layout| layout.planes().local(placement.plane) as u32);
        *record = SampleRecord {
            origin,
            fine: [location.ox, location.oy],
            plane,
            model_plane,
            table: table.index,
            spectral: placement.spectral,
            flags: u32::from(w_positive),
            gradient: placement.gradient,
        };
        if let (Some((layout, terms)), Some(sumwt)) = (&targets.adjoint, sumwt.as_deref_mut()) {
            spectral_powers(&mut powers, placement.spectral, terms.len());
            let weights = block.weights_of(index);
            for (gpol, row) in mueller.table(w_positive, false).iter().enumerate() {
                for (vpol, plane_index) in row.iter().enumerate() {
                    let Some(m) = *plane_index else {
                        continue;
                    };
                    let weight = weights[vpol];
                    if weight == 0.0 {
                        continue;
                    }
                    let norm = table.norm(location, m, !w_positive).norm();
                    for (power, term) in powers.iter().zip(terms.clone()) {
                        sumwt[layout.block_index(plane as usize, gpol, term)] +=
                            f64::from(weight) * power * norm;
                    }
                }
            }
        }
        if let Some(inverse) = inverse_norms.as_deref_mut() {
            let inverse = &mut inverse[local * npol..(local + 1) * npol];
            inverse.fill(Complex32::default());
            let mut norms = [Complex64::default(); MAX_POLS];
            for row in mueller.table(w_positive, true) {
                for (vpol, plane_index) in row.iter().enumerate() {
                    if let Some(m) = *plane_index {
                        norms[vpol] += table.norm(location, m, !w_positive);
                    }
                }
            }
            for (target, norm) in inverse.iter_mut().zip(&norms) {
                if *norm != Complex64::default() {
                    let reciprocal = norm.inv();
                    *target = Complex32::new(reciprocal.re as f32, reciprocal.im as f32);
                }
            }
        }
    }
}

/// `powers[t] = spectral^t` in `f64`, as the CPU backend forms them.
fn spectral_powers(powers: &mut [f64; MAX_TERMS], spectral: f32, count: usize) {
    let spectral = f64::from(spectral);
    let mut power = 1.0;
    for target in &mut powers[..count] {
        *target = power;
        power *= spectral;
    }
}

/// Tile-relative first tap of a support of `half` taps each side of
/// `location`.
fn tile_origin(layout: &AccumulatorLayout, location: CellLocation, half: [u16; 2]) -> [u32; 2] {
    let tile = layout.tile();
    let x0 = location.x - i64::from(half[0]) - tile.origin[0] as i64;
    let y0 = location.y - i64::from(half[1]) - tile.origin[1] as i64;
    assert!(
        x0 >= 0
            && y0 >= 0
            && x0 as usize + 2 * usize::from(half[0]) < tile.shape[0]
            && y0 as usize + 2 * usize::from(half[1]) < tile.shape[1],
        "sample support lies outside the accumulator tile"
    );
    [x0 as u32, y0 as u32]
}

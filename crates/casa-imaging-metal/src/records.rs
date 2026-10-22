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
    AccumulatorLayout, CellHold, CellLocation, CfKey, ConvolutionFunctionSet, KernelNormalisation,
    Mode, SampleBlock, TapLayout,
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
    assert!(size_of::<Params>() == 144);
    assert!(size_of::<casa_imaging_operator::NativeTerm>() == 20);
    assert!(size_of::<casa_imaging_operator::NativeSource>() == 24);
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
    /// Bit 0: gridding `w > 0`; bit 1: prediction `w > 0`.
    pub flags: u32,
    /// Pointing phase gradient.
    pub gradient: [f32; 2],
}

/// Where one kernel cell's taps sit in the device arenas (`Table`);
/// [`crate::kernel_bytes`] charges its size per cell.
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
    /// Threads serving one sample: 1, or a SIMD group's width.
    pub lanes: u32,
    /// Batch index of a native prediction's first sample.
    pub batch_offset: u32,
    /// 1: a prediction stays in the batch buffer, phasor applied; a spread
    /// subtracts the model at native cells.
    pub native: u32,
    /// 1: the native model adds to the cells instead of writing them.
    pub add: u32,
    /// Sample slot `k` serves sample `k · stride mod samples`
    /// ([`permutation_stride`]); 0 serves sample `k`.
    pub stride: u32,
    pub tile_shape: [u32; 2],
    pub tile_origin: [u32; 2],
    pub grid_shape: [u32; 2],
    pub adjoint: [[i8; 16]; 2],
    pub forward: [[i8; 16]; 2],
}

/// The [`Params::stride`] that spreads an accumulating dispatch of
/// `samples` samples over its threads: the integer coprime to `samples`
/// nearest `samples / φ`, so `k ↦ k · stride mod samples` is a bijection and,
/// by the three-gap property of golden-ratio steps, any run of neighbouring
/// sample slots serves samples spread over the whole dispatch. Neighbouring
/// samples (adjacent channels and integrations of one baseline) spread
/// nearly the same cells; on GPUs whose float atomics retry under contention
/// (Apple M1/M2), slots that hit the same cells at once serialize (#716).
/// Zero, sample `k` in slot `k`, below two samples.
pub(crate) fn permutation_stride(samples: usize) -> u32 {
    if samples < 2 {
        return 0;
    }
    let gcd = |mut a: usize, mut b: usize| {
        while b != 0 {
            (a, b) = (b, a % b);
        }
        a
    };
    let target = ((samples as f64) * 0.618_033_988_749_894_9).round() as usize;
    let coprime = |stride: usize| (1..samples).contains(&stride) && gcd(stride, samples) == 1;
    let stride = (0..samples)
        .flat_map(|distance| [target + distance, target.wrapping_sub(distance)])
        .find(|&stride| coprime(stride))
        .expect("1 is coprime to every count");
    u32::try_from(stride).expect("a dispatch's samples fit u32")
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

    /// The cell's taps of this kind, lent through `hold`; `None` when the
    /// set has no weight taps for it.
    pub(crate) fn taps<'s>(
        self,
        cf: &'s dyn ConvolutionFunctionSet,
        key: CfKey,
        hold: &'s mut CellHold,
    ) -> Option<TapLayout<'s>> {
        match self {
            Self::Imaging => Some(cf.taps(key, hold)),
            Self::Weight => cf.weight_taps(key, hold),
        }
    }
}

/// One kernel cell known to the device: its table index, its shape and the
/// norm of every fine offset and Mueller plane, so record preparation
/// never touches the kernel set again.
#[derive(Debug)]
pub(crate) struct KnownTable {
    /// Index into the device's table list.
    pub index: u32,
    /// Fine offsets per cell.
    pub oversampling: u16,
    /// Half the support along each axis.
    pub half_support: [u16; 2],
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
            oversampling: taps.oversampling(),
            half_support: taps.half_support(),
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

    /// Host bytes of the norms ([`TapLayout::norm_bytes`] of the cell).
    pub(crate) fn norm_bytes(&self) -> usize {
        self.norms.capacity() * size_of::<Complex64>()
    }
}

/// The kernel cells a backend has given the device, by cell and kind.
pub(crate) type KnownTables = HashMap<(CfKey, TapsKind), KnownTable>;

const _: () = assert!(
    size_of::<TableRecord>() as u64 == crate::TABLE_RECORD_BYTES,
    "kernel_bytes charges one table record per cell"
);

/// A Mueller route: the plane that joins a grid and a visibility
/// polarization.
#[derive(Clone, Copy, Default)]
struct Route {
    gpol: usize,
    vpol: usize,
    plane: u8,
}

/// The routes of one Mueller table in its order (grid polarization, then
/// visibility polarization), flat, so record preparation walks them per
/// sample without the table's nested vectors.
struct Routes {
    routes: [Route; MAX_POLS * MAX_POLS],
    len: usize,
}

impl Routes {
    fn of(table: &[Vec<Option<u8>>]) -> Self {
        let mut routes = Self {
            routes: [Route::default(); MAX_POLS * MAX_POLS],
            len: 0,
        };
        for (gpol, row) in table.iter().enumerate() {
            for (vpol, plane) in row.iter().enumerate() {
                if let Some(plane) = *plane {
                    routes.routes[routes.len] = Route { gpol, vpol, plane };
                    routes.len += 1;
                }
            }
        }
        routes
    }

    fn iter(&self) -> std::slice::Iter<'_, Route> {
        self.routes[..self.len].iter()
    }
}

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
/// transform, `inverse_norms` (`1/N` per visibility polarization under
/// [`KernelNormalisation::KernelSum`], zero for a zero norm, one under the
/// other rules); add the adjoint's `W·s^t·n` to `sumwt` with `n` the
/// set's rule: one, `Re N` or `|N|`, as the CPU backend does.
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
    inverse_norms: Option<&mut [Complex32]>,
) {
    let npol = block.npol;
    let mueller = cf.mueller();
    let rule = cf.normalisation();
    let geometry = targets.geometry();
    let mut powers = [0.0_f64; MAX_TERMS];
    // The records address the adjoint's tile, or the model's grid for a
    // prediction; fixed for the dispatch, as are the Mueller routes of
    // each sign of w.
    let origin_layout = match &targets.adjoint {
        Some((layout, _)) => *layout,
        None => targets.model.expect("a prediction has model grids"),
    };
    let adjoint_routes =
        [false, true].map(|w_positive| Routes::of(mueller.table(w_positive, false)));
    let forward_routes =
        [false, true].map(|w_positive| Routes::of(mueller.table(w_positive, true)));
    let mut inverse_norms = inverse_norms.map(|inverse| {
        let inverse = &mut inverse[..range.len() * npol];
        inverse.fill(if rule == KernelNormalisation::KernelSum {
            Complex32::default()
        } else {
            Complex32::new(1.0, 0.0)
        });
        inverse
    });
    // Successive samples share a kernel cell almost always (rows run by
    // baseline), so the table found for one serves the next.
    let mut last: Option<(CfKey, &KnownTable)> = None;
    for (local, index) in range.enumerate() {
        let record = &mut records[local];
        let placement = &block.placements[index];
        let table = match last {
            Some((key, table)) if key == placement.cf => table,
            _ => {
                let table = &known[&(placement.cf, targets.kind)];
                last = Some((placement.cf, table));
                table
            }
        };
        let (u, v) = match targets.kind {
            TapsKind::Imaging => (placement.u, placement.v),
            TapsKind::Weight => (0.0, 0.0),
        };
        let location = geometry.locate(u, v, table.oversampling);
        let w_positive = placement.w > 0.0;
        let prediction_w_positive = placement.prediction_w_positive;
        let origin = tile_origin(origin_layout, location, table.half_support);
        let plane = targets.adjoint.as_ref().map_or(0, |(layout, _)| {
            layout.planes().local(placement.plane) as u32
        });
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
            flags: u32::from(w_positive) | (u32::from(prediction_w_positive) << 1),
            gradient: placement.gradient,
        };
        if let (Some((layout, terms)), Some(sumwt)) = (&targets.adjoint, sumwt.as_deref_mut()) {
            spectral_powers(&mut powers, placement.spectral, terms.len());
            let weights = block.weights_of(index);
            for &Route {
                gpol,
                vpol,
                plane: m,
            } in adjoint_routes[usize::from(w_positive)].iter()
            {
                let weight = weights[vpol];
                if weight == 0.0 {
                    continue;
                }
                let norm = match rule {
                    KernelNormalisation::UnitSum => 1.0,
                    KernelNormalisation::RealSum => table.norm(location, m, w_positive).re,
                    KernelNormalisation::KernelSum => table.norm(location, m, w_positive).norm(),
                };
                let first = layout.block_index(plane as usize, gpol, 0);
                for (power, term) in powers.iter().zip(terms.clone()) {
                    sumwt[first + term] += f64::from(weight) * power * norm;
                }
            }
        }
        if rule != KernelNormalisation::KernelSum {
            continue;
        }
        if let Some(inverse) = inverse_norms.as_deref_mut() {
            let inverse = &mut inverse[local * npol..(local + 1) * npol];
            let mut norms = [Complex64::default(); MAX_POLS];
            // The forward norm sums the taps the gather multiplies the grid
            // by, conjugated for w ≤ 0 (`accumulateFromGrid.inc`): the
            // opposite conjugation from the adjoint's `sumwt` norm above.
            for &Route { vpol, plane: m, .. } in
                forward_routes[usize::from(prediction_w_positive)].iter()
            {
                norms[vpol] += table.norm(location, m, !prediction_w_positive);
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

#[cfg(test)]
mod tests {
    use super::permutation_stride;

    #[test]
    fn the_stride_permutes_every_dispatch_size() {
        for samples in (0..=1500).chain([4095, 4096, 10_923, 32_767, 32_768]) {
            let stride = permutation_stride(samples) as usize;
            if samples < 2 {
                assert_eq!(stride, 0, "{samples} samples keep their order");
                continue;
            }
            let mut seen = vec![false; samples];
            for slot in 0..samples {
                let sample = slot * stride % samples;
                assert!(!seen[sample], "{samples} samples: {sample} served twice");
                seen[sample] = true;
            }
        }
    }

    #[test]
    fn neighbouring_slots_serve_samples_far_apart() {
        for samples in [64, 100, 512, 4096, 10_923, 32_768] {
            let stride = permutation_stride(samples) as usize;
            let gap = stride.min(samples - stride);
            assert!(
                4 * gap >= samples,
                "{samples} samples: neighbouring slots only {gap} apart"
            );
        }
    }
}

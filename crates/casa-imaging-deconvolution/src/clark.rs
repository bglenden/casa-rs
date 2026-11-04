// SPDX-License-Identifier: LGPL-3.0-or-later
//! Clark CLEAN, as CASA's `SDAlgorithmClarkClean2` runs
//! `ClarkCleanLatModel::solve` (`synthesis/MeasurementEquations`).

use std::cell::Cell;

use crate::Error;
use crate::plane::PlaneShape;
use crate::psf::ClarkPatch;
use crate::refresh::LinearRefresh;
use crate::solver::{Candidate, Delta, MinorCycleView, Next, Solver, StepEnd};

/// Clark's major cycles per step (`setMaxNumberMajorCycles(10)`).
const MAJOR_CYCLES: usize = 10;

/// Clark's point CLEAN on one plane.
///
/// Each Clark cycle builds the active list of supported pixels above
/// `max(threshold, peak × exterior sidelobe × factor)`, cleans it with
/// subtraction inside the PSF patch only, and then refreshes the whole
/// residual exactly ([`LinearRefresh`]). A step ends after ten Clark cycles
/// or when the peak reaches the threshold; its peak residual is the active
/// list's last maximum, as CASA reports it.
#[derive(Default)]
pub struct ClarkPolarized<const N: usize> {
    refresh: Cell<Option<LinearRefresh>>,
}

/// The single-polarization Clark solver.
pub type Clark = ClarkPolarized<1>;

impl<const N: usize> ClarkPolarized<N> {
    /// Clark that reuses `refresh` when it serves this plane's PSF.
    #[must_use]
    pub fn new(refresh: Option<LinearRefresh>) -> Self {
        Self {
            refresh: Cell::new(refresh),
        }
    }

    /// The refresh, to reuse in later cycles of the same PSF.
    #[must_use]
    pub fn into_refresh(self) -> Option<LinearRefresh> {
        self.refresh.into_inner()
    }
}

/// An active pixel: its `[x, y]`, kept rather than its storage index because
/// every component's subtraction needs each pixel's offset from it.
struct Active<const N: usize> {
    pixel: [u32; 2],
    value: [f64; N],
}

/// CASA's `ABSMAXF`, `ABSMAX2F` and `ABSMAX4F` ranking.
fn magnitude<const N: usize>(value: &[f64; N]) -> f64 {
    match N {
        1 => value[0].abs(),
        2 => (value[0] + value[1]).abs().max((value[0] - value[1]).abs()),
        4 => (value[0] + (value[1] * value[1] + value[2] * value[2] + value[3] * value[3]).sqrt())
            .abs(),
        _ => unreachable!("CASA Clark supports one, two or four polarizations"),
    }
}

/// Active pixels per block of [`ClarkState`]'s block maxima.
const BLOCK: usize = 64;

/// The largest magnitude of each [`BLOCK`] of `active`, NaN ignored.
fn block_maxima<const N: usize>(active: &[Active<N>], blocks: &mut Vec<f64>) {
    blocks.clear();
    blocks.extend(active.chunks(BLOCK).map(largest_magnitude));
}

/// Recompute the blocks of `blocks` that hold any of `active[range]`.
fn refresh_blocks<const N: usize>(
    active: &[Active<N>],
    blocks: &mut [f64],
    range: std::ops::Range<usize>,
) {
    if range.is_empty() {
        return;
    }
    for block in range.start / BLOCK..=(range.end - 1) / BLOCK {
        let end = ((block + 1) * BLOCK).min(active.len());
        blocks[block] = largest_magnitude(&active[block * BLOCK..end]);
    }
}

fn largest_magnitude<const N: usize>(pixels: &[Active<N>]) -> f64 {
    pixels.iter().fold(0.0_f64, |largest, pixel| {
        largest.max(magnitude(&pixel.value))
    })
}

/// ABSMAXF: the first pixel of largest magnitude in list order, as a scan
/// that keeps the first pixel unless a later one is strictly larger finds
/// it (so a NaN first pixel is the peak, and a later NaN never is). It runs
/// once per component, so it reads the blocks' maxima (`blocks`, kept by
/// [`block_maxima`] and [`refresh_blocks`]) and then only the first block
/// that holds the largest.
fn first_largest<'a, const N: usize>(
    active: &'a [Active<N>],
    blocks: &[f64],
) -> Option<&'a Active<N>> {
    let first = active.first()?;
    if magnitude(&first.value).is_nan() {
        return Some(first);
    }
    let largest = blocks
        .iter()
        .fold(0.0_f64, |largest, &block| largest.max(block));
    let block = blocks
        .iter()
        .position(|&block| block == largest)
        .unwrap_or(0);
    active[block * BLOCK..]
        .iter()
        .take(BLOCK)
        .find(|pixel| magnitude(&pixel.value) == largest)
        .or(Some(first))
}

/// Working state of one Clark step.
pub struct ClarkState<const N: usize = 1> {
    shape: PlaneShape,
    psf_peak: [usize; 2],
    patch: ClarkPatch,
    threshold: f64,
    factor: f64,
    max_residual: f64,
    flux_limit: f64,
    iteration_flux_limit: f64,
    fac: f64,
    fmn: f64,
    cycle_iterations: usize,
    maximum_cycle_iterations: usize,
    cycles: usize,
    step_components: usize,
    active: Vec<Active<N>>,
    /// The largest magnitude of each [`BLOCK`] of `active`.
    blocks: Vec<f64>,
    refresh: Vec<LinearRefresh>,
}

impl<const N: usize> Solver for ClarkPolarized<N> {
    type State = ClarkState<N>;

    fn initialize(
        &self,
        view: &MinorCycleView<'_>,
        residual: &[Vec<f64>],
        threshold: f64,
    ) -> Result<ClarkState<N>, Error> {
        let shape = view.shape;
        // `LatConvEquation` takes the PSF at the plane's centre, whatever its
        // peak (`ClarkCleanLatModel::solve`).
        let [x, y] = shape.centre();
        let peak = shape.index(x, y);
        assert_eq!(residual.len(), N);
        assert!(matches!(N, 1 | 2 | 4));
        let first_refresh = match self.refresh.take() {
            Some(refresh) if refresh.serves(shape, peak) => refresh,
            _ => LinearRefresh::new(&view.psf[0], shape, peak)?,
        };
        let mut refresh = Vec::with_capacity(N);
        refresh.push(first_refresh);
        for _ in 1..N {
            refresh.push(LinearRefresh::new(&view.psf[0], shape, peak)?);
        }
        let patch = view.summary.clark();
        // A PSF whose sidelobes outside the patch are large gets short Clark
        // cycles (ClarkCleanLatModel::solve).
        let maximum_cycle_iterations = if patch.exterior_sidelobe > 0.5 {
            5
        } else if patch.exterior_sidelobe > 0.35 {
            50
        } else {
            usize::MAX
        };
        let max_residual = (0..shape.len())
            .filter(|&index| view.support.as_slice()[index])
            .map(|index| magnitude(&std::array::from_fn::<_, N, _>(|p| residual[p][index])))
            .fold(0.0_f64, f64::max);
        let mut state = ClarkState {
            shape,
            psf_peak: shape.pixel(peak),
            patch,
            threshold,
            factor: 1.0 / 3.0,
            max_residual,
            flux_limit: 0.0,
            iteration_flux_limit: 0.0,
            fac: 0.0,
            fmn: 0.0,
            cycle_iterations: 0,
            maximum_cycle_iterations,
            cycles: 0,
            step_components: 0,
            active: Vec::new(),
            blocks: Vec::new(),
            refresh,
        };
        state.begin(residual, view);
        Ok(state)
    }

    fn next(
        &self,
        state: &mut ClarkState<N>,
        view: &MinorCycleView<'_>,
        residual: &mut [Vec<f64>],
    ) -> Result<Next, Error> {
        loop {
            let peak = first_largest(&state.active, &state.blocks);
            debug_assert_eq!(
                peak.map(|pixel| pixel.pixel),
                {
                    let mut fresh = Vec::new();
                    block_maxima(&state.active, &mut fresh);
                    first_largest(&state.active, &fresh).map(|pixel| pixel.pixel)
                },
                "the block maxima are current"
            );
            if state.cycle_iterations < state.maximum_cycle_iterations
                && let Some(pixel) = peak
                && magnitude(&pixel.value) > state.iteration_flux_limit
            {
                if pixel.value.iter().any(|value| !value.is_finite()) {
                    return Err(Error::NonFinite);
                }
                return Ok(Next::Clean(Candidate::Pixel {
                    index: state
                        .shape
                        .index(pixel.pixel[0] as usize, pixel.pixel[1] as usize),
                    scale: 0,
                    strength: pixel.value[0],
                }));
            }
            if state.cycle_iterations == 0 {
                return Ok(Next::Stop);
            }
            state.refresh_pending(residual, view)?;
            if state.max_residual <= state.threshold || state.cycles >= MAJOR_CYCLES {
                return Ok(Next::Stop);
            }
            state.begin(residual, view);
        }
    }

    fn component_fluxes(
        &self,
        state: &Self::State,
        _: Candidate,
        gain: f64,
        fluxes: &mut Vec<f64>,
    ) {
        if let Some(pixel) = first_largest(&state.active, &state.blocks) {
            fluxes.extend(pixel.value.iter().map(|value| gain * value));
        }
    }

    fn reported_flux(&self, fluxes: &[f64]) -> f64 {
        fluxes.iter().sum()
    }

    fn accept(
        &self,
        state: &mut ClarkState<N>,
        view: &MinorCycleView<'_>,
        _: &mut [Vec<f64>],
        candidate: Candidate,
        gain: f64,
        delta: &mut Delta,
    ) -> Result<(), Error> {
        let Candidate::Pixel { index, .. } = candidate;
        let values = first_largest(&state.active, &state.blocks)
            .expect("candidate has an active pixel")
            .value;
        let fluxes = values.map(|value| gain * value);
        for (p, &flux) in fluxes.iter().enumerate() {
            delta.add(p, index, flux);
            state.refresh[p].add(index, flux);
        }
        let shape = state.shape;
        let [px, py] = shape.pixel(index);
        let patch = state.patch;
        let psf = &view.psf[0];
        // The list runs x outer, so the pixels whose x lies in the patch
        // are one run of it; only they can change.
        debug_assert!(
            state
                .active
                .windows(2)
                .all(|pair| pair[0].pixel[0] <= pair[1].pixel[0]),
            "the active list runs x outer (`begin`'s order)"
        );
        let first_x = px as isize - patch.radius[0] as isize;
        let end_x = px as isize + (patch.size[0] - patch.radius[0]) as isize;
        let start = state
            .active
            .partition_point(|pixel| (pixel.pixel[0] as isize) < first_x);
        let end = state
            .active
            .partition_point(|pixel| (pixel.pixel[0] as isize) < end_x);
        for pixel in &mut state.active[start..end] {
            let [tx, ty] = pixel.pixel;
            let relative = [tx as isize - px as isize, ty as isize - py as isize];
            if relative[0] < -(patch.radius[0] as isize)
                || relative[0] >= (patch.size[0] - patch.radius[0]) as isize
                || relative[1] < -(patch.radius[1] as isize)
                || relative[1] >= (patch.size[1] - patch.radius[1]) as isize
            {
                continue;
            }
            let source = [
                state.psf_peak[0] as isize + relative[0],
                state.psf_peak[1] as isize + relative[1],
            ];
            if source[0] < 0
                || source[1] < 0
                || source[0] >= shape.nx as isize
                || source[1] >= shape.ny as isize
            {
                continue;
            }
            let response = psf[shape.index(source[0] as usize, source[1] as usize)];
            for (value, flux) in pixel.value.iter_mut().zip(fluxes) {
                *value -= flux * response;
            }
        }
        refresh_blocks(&state.active, &mut state.blocks, start..end);
        state.cycle_iterations += 1;
        state.step_components += 1;
        state.fmn += state.fac / state.step_components as f64;
        state.iteration_flux_limit = if state.flux_limit > 0.0 {
            (state.flux_limit * state.fmn).max(state.threshold)
        } else {
            // Without an exterior sidelobe the flux limit is 0, CASA's
            // `Fac = absRes / fluxLimit` (speedup −1) is infinite and its
            // iteration limit `max(0 · ∞, threshold)` is NaN, so the Clark
            // cycle ends after one component.
            f64::INFINITY
        };
        Ok(())
    }

    fn finalize(
        &self,
        mut state: ClarkState<N>,
        view: &MinorCycleView<'_>,
        residual: &mut [Vec<f64>],
        _: &Delta,
    ) -> Result<StepEnd, Error> {
        if state.cycle_iterations > 0 {
            state.refresh_pending(residual, view)?;
        }
        let end = StepEnd {
            peak: state.max_residual,
            refreshes: state.cycles,
        };
        self.refresh.set(state.refresh.into_iter().next());
        Ok(end)
    }

    /// The refresh ([`LinearRefresh::bytes`]), the active list, which
    /// grows to at most every pixel (an upper bound, since the list holds
    /// only the supported pixels above the cycle's flux limit), and its
    /// block maxima.
    fn working_bytes(&self, shape: PlaneShape, _terms: usize) -> u64 {
        let pixels = shape.len().next_power_of_two();
        let active = pixels * size_of::<Active<N>>();
        let blocks = pixels.div_ceil(BLOCK).next_power_of_two() * size_of::<f64>();
        N as u64 * LinearRefresh::bytes(shape)
            + (active + blocks + N * size_of::<LinearRefresh>()) as u64
    }

    /// A point component.
    fn component_cells(&self, _shape: PlaneShape) -> usize {
        1
    }
}

impl<const N: usize> ClarkState<N> {
    /// Start a Clark cycle: the flux limit from the exterior sidelobe and
    /// the active list of supported pixels above it.
    fn begin(&mut self, residual: &[Vec<f64>], view: &MinorCycleView<'_>) {
        self.flux_limit = self.max_residual * self.patch.exterior_sidelobe * self.factor;
        if self.factor > 1.0 {
            self.flux_limit = self.flux_limit.min(0.95 * self.max_residual);
        }
        let cutoff = self.flux_limit.max(self.threshold);
        // GETBIMF: supported pixels at or above the cutoff, x outer and y
        // inner, which is storage order. (CASA lists a tiled residual tile
        // by tile; a plane in one tile is listed in this order.)
        self.active.clear();
        for (index, &supported) in view.support.as_slice().iter().enumerate() {
            let value = std::array::from_fn(|p| residual[p][index]);
            if supported && magnitude(&value) >= cutoff {
                let [x, y] = self.shape.pixel(index);
                self.active.push(Active {
                    pixel: [x as u32, y as u32],
                    value,
                });
            }
        }
        block_maxima(&self.active, &mut self.blocks);
        let peak = self
            .blocks
            .iter()
            .fold(0.0_f64, |best, &block| best.max(block));
        // `pow(fluxLimit / absRes, speedup)` with CASA's speedup of −1.
        self.fac = if self.flux_limit > 0.0 {
            peak / self.flux_limit
        } else {
            0.0
        };
        self.fmn = 0.0;
        self.iteration_flux_limit = cutoff;
        self.cycle_iterations = 0;
    }

    /// End a Clark cycle: refresh the residual exactly and widen the next
    /// cycle's flux limit when the peak grew.
    fn refresh_pending(
        &mut self,
        residual: &mut [Vec<f64>],
        view: &MinorCycleView<'_>,
    ) -> Result<(), Error> {
        let previous = self.max_residual;
        self.max_residual = self
            .active
            .iter()
            .fold(0.0_f64, |peak, pixel| peak.max(magnitude(&pixel.value)));
        for (refresh, plane) in self.refresh.iter_mut().zip(residual) {
            refresh.refresh(plane, &view.psf[0])?;
        }
        self.cycles += 1;
        self.cycle_iterations = 0;
        if self.max_residual > previous {
            self.factor *= 3.0;
            self.maximum_cycle_iterations = 10;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::{Active, BLOCK, block_maxima, first_largest, refresh_blocks};

    /// The scan `first_largest` replaces: keep the first pixel unless a
    /// later one is strictly larger.
    fn scan(active: &[Active<1>]) -> Option<&Active<1>> {
        active.iter().fold(None::<&Active<1>>, |best, pixel| {
            if best.is_none_or(|current| pixel.value[0].abs() > current.value[0].abs()) {
                Some(pixel)
            } else {
                best
            }
        })
    }

    /// `first_largest` over block maxima finds the scan's pixel, on lists
    /// spanning several blocks with ties, zeros, infinities and NaN, and
    /// still does after a run of the list changes and only that run's blocks
    /// are refreshed, as `accept` does.
    #[test]
    fn the_first_largest_pixel_is_the_scans() {
        let mut state = 0x2545_f491_4f6c_dd1d_u64;
        let mut next = move || {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            state
        };
        // Few distinct magnitudes, so ties are common, and the edge values.
        let specials = [
            0.0,
            -0.0,
            f64::INFINITY,
            f64::NEG_INFINITY,
            f64::NAN,
            2.0,
            -2.0,
            1.5,
        ];
        let value = |draw: u64| specials[(draw % specials.len() as u64) as usize];
        let mut blocks = Vec::new();
        let mut multi_block_runs = 0;
        for length in (0..40).chain([BLOCK - 1, BLOCK, BLOCK + 1, 3 * BLOCK + 7]) {
            for _ in 0..200 {
                let mut active = (0..length)
                    .map(|index| Active {
                        pixel: [index as u32, 0],
                        value: [value(next())],
                    })
                    .collect::<Vec<_>>();
                block_maxima(&active, &mut blocks);
                let found = first_largest(&active, &blocks).map(|pixel| pixel.pixel);
                assert_eq!(found, scan(&active).map(|pixel| pixel.pixel));
                if length == 0 {
                    continue;
                }
                // A run of 1 to 2·BLOCK pixels from anywhere, so runs cross
                // block edges.
                let start = (next() % length as u64) as usize;
                let end = (start + 1 + (next() % (2 * BLOCK) as u64) as usize).min(length);
                if start / BLOCK != (end - 1) / BLOCK {
                    multi_block_runs += 1;
                }
                for pixel in &mut active[start..end] {
                    pixel.value = [value(next())];
                }
                refresh_blocks(&active, &mut blocks, start..end);
                let found = first_largest(&active, &blocks).map(|pixel| pixel.pixel);
                assert_eq!(found, scan(&active).map(|pixel| pixel.pixel));
            }
        }
        assert!(
            multi_block_runs > 100,
            "only {multi_block_runs} refreshed runs crossed a block edge"
        );
    }
    /// A joint Clark component uses CASA's polarization vector to choose a
    /// single position and charges one iteration for all of its fluxes.
    #[test]
    fn joint_clark_selects_one_pixel_and_updates_every_polarization() {
        use super::ClarkPolarized;
        use crate::{
            CycleControls, MinorCycleView, PlaneShape, PlaneStatistics, PsfSummary, Support,
            run_plane,
        };
        let shape = PlaneShape::new(64, 64);
        let centre = shape.centre();
        let psf = (0..shape.len())
            .map(|i| {
                let [x, y] = shape.pixel(i);
                let r2 =
                    (x as f64 - centre[0] as f64).powi(2) + (y as f64 - centre[1] as f64).powi(2);
                (-r2 / 8.0).exp()
            })
            .collect::<Vec<_>>();
        let summary = PsfSummary::new(&psf, shape).unwrap();
        let support = Support::full(shape);
        let a = shape.index(20, 20);
        let b = shape.index(40, 40);
        let mut residual = vec![vec![0.0; shape.len()]; 2];
        residual[0][a] = 3.0;
        residual[0][b] = 1.0;
        residual[1][b] = 3.0;
        let statistics = PlaneStatistics::measure(&residual[0], &support, &support, 0.0, false);
        let psfs = vec![psf];
        let view = MinorCycleView {
            shape,
            residual: &residual,
            psf: &psfs,
            summary: &summary,
            support: &support,
        };
        let cycle = CycleControls {
            iterations: 1,
            threshold: 0.0,
            threshold_reached: true,
            gain: 0.17,
            nsigma: 0.0,
        };
        let result = run_plane(
            &ClarkPolarized::<2>::new(None),
            &view,
            &cycle,
            &statistics,
            8,
        )
        .unwrap();
        assert_eq!(result.iterations, 1);
        assert_eq!(result.delta.term(0).collect::<Vec<_>>(), vec![(b, 0.17)]);
        assert_eq!(result.delta.term(1).collect::<Vec<_>>(), vec![(b, 0.51)]);
        assert!((result.model_flux - 0.68).abs() < 1e-12);
    }
}

// SPDX-License-Identifier: LGPL-3.0-or-later
//! Linear residual refresh: subtract the PSF convolved with a batch of
//! components, by FFT or by direct sparse sums, whichever costs less.

use casa_fft::RealFft2;
use num_complex::Complex32;
use rayon::prelude::*;

use crate::Error;
use crate::plane::PlaneShape;

/// Components tracked individually before a batch is refreshed by FFT
/// whatever its cost estimate; the estimate below picks the method.
const SPARSE_COMPONENT_CAPACITY: usize = 64;

/// The linear (not circular) convolution `residual −= PSF ⊛ components`
/// over one plane, the PSF anchored at its peak.
///
/// The FFT path transforms on the smallest alias-free padded grid for the
/// PSF origin, in single precision, on one thread. The sparse path sums the
/// shifted PSF rows directly and splits the rows across the caller's
/// worker pool. Its buffers depend only on the PSF, so one refresh serves
/// every minor cycle of a run.
#[derive(Debug)]
pub struct LinearRefresh {
    shape: PlaneShape,
    centre: [usize; 2],
    padded: [usize; 2],
    psf_spectrum: Vec<Complex32>,
    components: Vec<Complex32>,
    fft: RealFft2<f32>,
    sparse: Vec<usize>,
    dense: bool,
}

impl LinearRefresh {
    /// Prepare the refresh of `shape` planes by `psf`, whose peak is at
    /// storage index `peak`.
    ///
    /// # Errors
    ///
    /// When the padded transform cannot be planned.
    pub fn new(psf: &[f64], shape: PlaneShape, peak: usize) -> Result<Self, Error> {
        let centre = shape.pixel(peak);
        // Neither the negative PSF tail nor the positive convolution tail
        // may wrap into [0, extent): the smallest alias-free extent for this
        // PSF origin, including off-centre peaks.
        let padded_axis =
            |extent: usize, origin: usize| (2 * extent - 1 - origin).max(extent + origin);
        let padded = [
            padded_axis(shape.nx, centre[0]),
            padded_axis(shape.ny, centre[1]),
        ];
        let mut fft = RealFft2::with_threads(padded, 1)?;
        let mut psf_spectrum = vec![Complex32::default(); fft.storage_len()];
        let row_stride = fft.real_row_stride();
        let real: &mut [f32] = bytemuck::cast_slice_mut(&mut psf_spectrum);
        for x in 0..shape.nx {
            for y in 0..shape.ny {
                let offset = [
                    (x + padded[0] - centre[0]) % padded[0],
                    (y + padded[1] - centre[1]) % padded[1],
                ];
                real[offset[0] * row_stride + offset[1]] = psf[shape.index(x, y)] as f32;
            }
        }
        fft.forward(&mut psf_spectrum)?;
        let components = vec![Complex32::default(); fft.storage_len()];
        Ok(Self {
            shape,
            centre,
            padded,
            psf_spectrum,
            components,
            fft,
            sparse: Vec::with_capacity(SPARSE_COMPONENT_CAPACITY),
            dense: false,
        })
    }

    /// Whether this refresh was prepared for a PSF on `shape` peaking at
    /// `peak`.
    #[must_use]
    pub fn serves(&self, shape: PlaneShape, peak: usize) -> bool {
        self.shape == shape && self.centre == shape.pixel(peak)
    }

    /// Add `flux` at storage index `index` to the pending batch.
    pub fn add(&mut self, index: usize, flux: f64) {
        if !self.dense && !self.sparse.contains(&index) {
            if self.sparse.len() == SPARSE_COMPONENT_CAPACITY {
                self.dense = true;
            } else {
                self.sparse.push(index);
            }
        }
        let [x, y] = self.shape.pixel(index);
        let stride = self.fft.real_row_stride();
        let real: &mut [f32] = bytemuck::cast_slice_mut(&mut self.components);
        real[x * stride + y] += flux as f32;
    }

    /// Subtract the pending batch convolved with `psf` from `residual` and
    /// clear the batch. Returns whether the FFT path ran.
    ///
    /// Called inside a rayon pool (`ThreadPool::install`), the sparse path
    /// splits its rows across that pool; elsewhere it runs on the calling
    /// thread. The result is the same either way.
    ///
    /// # Errors
    ///
    /// When the transform fails.
    pub fn refresh(&mut self, residual: &mut [f64], psf: &[f64]) -> Result<bool, Error> {
        let stride = self.fft.real_row_stride();
        if !self.dense {
            let real: &[f32] = bytemuck::cast_slice(&self.components);
            let components = self
                .sparse
                .iter()
                .map(|&index| {
                    let [x, y] = self.shape.pixel(index);
                    (index, real[x * stride + y])
                })
                .filter(|(_, flux)| *flux != 0.0)
                .collect::<Vec<_>>();
            if components.is_empty() {
                self.sparse.clear();
                return Ok(false);
            }
            // Clipped direct multiply-adds against the butterfly work of two
            // real transforms: the crossover scales with shape and support.
            let direct = components.iter().fold(0_u128, |work, &(index, _)| {
                let [x, y] = self.shape.pixel(index);
                let overlap = (self.shape.nx - x.abs_diff(self.centre[0])) as u128
                    * (self.shape.ny - y.abs_diff(self.centre[1])) as u128;
                work + overlap
            });
            let transform = self.padded[0] as u128
                * self.padded[1] as u128
                * u128::from(self.padded[0].ilog2() + self.padded[1].ilog2());
            if direct <= transform {
                let workers = if rayon::current_thread_index().is_some() {
                    rayon::current_num_threads()
                } else {
                    1
                };
                sparse_refresh(psf, self.shape, self.centre, &components, workers, residual);
                let real: &mut [f32] = bytemuck::cast_slice_mut(&mut self.components);
                for &index in &self.sparse {
                    let [x, y] = self.shape.pixel(index);
                    real[x * stride + y] = 0.0;
                }
                self.sparse.clear();
                return Ok(false);
            }
        }
        self.fft.forward(&mut self.components)?;
        for (value, kernel) in self.components.iter_mut().zip(&self.psf_spectrum) {
            *value *= kernel;
        }
        self.fft.inverse(&mut self.components)?;
        let normalisation = (self.padded[0] * self.padded[1]) as f64;
        let real: &[f32] = bytemuck::cast_slice(&self.components);
        for x in 0..self.shape.nx {
            for y in 0..self.shape.ny {
                residual[self.shape.index(x, y)] -= f64::from(real[x * stride + y]) / normalisation;
            }
        }
        self.components.fill(Complex32::default());
        self.sparse.clear();
        self.dense = false;
        Ok(true)
    }
}

/// `residual −= Σ flux · PSF(· − component + centre)` by rows, the rows split
/// across `workers` jobs of the current rayon pool.
fn sparse_refresh(
    psf: &[f64],
    shape: PlaneShape,
    centre: [usize; 2],
    components: &[(usize, f32)],
    workers: usize,
    residual: &mut [f64],
) {
    let rows = |first: usize, block: &mut [f64]| {
        for (offset, row) in block.chunks_exact_mut(shape.ny).enumerate() {
            let x = first + offset;
            for &(index, flux) in components {
                let [cx, cy] = shape.pixel(index);
                let psf_x = centre[0] as isize + x as isize - cx as isize;
                if !(0..shape.nx as isize).contains(&psf_x) {
                    continue;
                }
                let begin = cy.saturating_sub(centre[1]);
                let end = (cy + shape.ny - centre[1]).min(shape.ny);
                let start = psf_x as usize * shape.ny + centre[1] + begin - cy;
                let kernel = &psf[start..start + end - begin];
                for (value, coefficient) in row[begin..end].iter_mut().zip(kernel) {
                    *value -= f64::from(flux) * coefficient;
                }
            }
        }
    };
    let workers = workers.clamp(1, shape.nx);
    if workers == 1 {
        rows(0, residual);
        return;
    }
    let block_rows = shape.nx.div_ceil(workers);
    residual
        .par_chunks_mut(block_rows * shape.ny)
        .enumerate()
        .for_each(|(block, values)| rows(block * block_rows, values));
}

#[cfg(test)]
mod tests {
    use super::*;

    fn direct(
        psf: &[f64],
        shape: PlaneShape,
        centre: [usize; 2],
        components: &[(usize, f64)],
        residual: &mut [f64],
    ) {
        for &(index, flux) in components {
            let [cx, cy] = shape.pixel(index);
            for x in 0..shape.nx {
                for y in 0..shape.ny {
                    let px = centre[0] as isize + x as isize - cx as isize;
                    let py = centre[1] as isize + y as isize - cy as isize;
                    if (0..shape.nx as isize).contains(&px) && (0..shape.ny as isize).contains(&py)
                    {
                        residual[shape.index(x, y)] -=
                            flux * psf[shape.index(px as usize, py as usize)];
                    }
                }
            }
        }
    }

    /// Sparse and FFT refreshes both equal the linear convolution, for every
    /// PSF origin of odd and non-square planes, with components at the edges,
    /// inside a pool as on the calling thread.
    #[test]
    fn both_paths_are_the_linear_convolution_for_every_origin() {
        let pool = rayon::ThreadPoolBuilder::new()
            .num_threads(3)
            .build()
            .unwrap();
        for shape in [
            PlaneShape::new(3, 4),
            PlaneShape::new(5, 7),
            PlaneShape::new(6, 6),
        ] {
            let psf = (0..shape.len())
                .map(|index| (index as f64 * 0.37).cos())
                .collect::<Vec<_>>();
            for peak in 0..shape.len() {
                let components = [
                    (0, 0.625),
                    (shape.len() - 1, -0.375),
                    (shape.len() / 2, 0.125),
                ];
                let mut expected = vec![0.5; shape.len()];
                direct(&psf, shape, shape.pixel(peak), &components, &mut expected);
                for (dense, pooled) in [(false, false), (false, true), (true, false)] {
                    let mut refresh = LinearRefresh::new(&psf, shape, peak).unwrap();
                    for &(index, flux) in &components {
                        refresh.add(index, flux);
                    }
                    refresh.dense = dense;
                    let mut run = |actual: &mut Vec<f64>| {
                        if pooled {
                            pool.install(|| refresh.refresh(actual, &psf)).unwrap()
                        } else {
                            refresh.refresh(actual, &psf).unwrap()
                        }
                    };
                    let mut actual = vec![0.5; shape.len()];
                    assert_eq!(run(&mut actual), dense);
                    for (a, e) in actual.iter().zip(&expected) {
                        assert!((a - e).abs() < 1e-5, "{shape:?} peak {peak}: {a} vs {e}");
                    }
                    // A refreshed batch is cleared.
                    let done = actual.clone();
                    assert!(!run(&mut actual));
                    assert_eq!(actual, done);
                }
            }
        }
    }

    /// The batch spills to the FFT path past the sparse capacity, and the
    /// estimate picks the FFT when the clipped direct work is larger.
    #[test]
    fn crowded_batches_take_the_fft_path() {
        let shape = PlaneShape::new(32, 32);
        let psf = vec![1.0; shape.len()];
        let mut refresh = LinearRefresh::new(&psf, shape, shape.index(16, 16)).unwrap();
        for index in 0..=SPARSE_COMPONENT_CAPACITY {
            refresh.add(index, 0.25);
        }
        assert!(refresh.dense);
        assert!(refresh.refresh(&mut vec![0.0; shape.len()], &psf).unwrap());
        for index in 0..40 {
            refresh.add(shape.index(14 + index / 8, 12 + index % 8), 0.25);
        }
        assert!(!refresh.dense);
        assert!(refresh.refresh(&mut vec![0.0; shape.len()], &psf).unwrap());
    }

    /// Splitting rows across a pool gives the same residual as one worker.
    #[test]
    fn row_blocks_on_a_pool_match_one_worker() {
        let shape = PlaneShape::new(17, 9);
        let psf = (0..shape.len())
            .map(|index| (index as f64 * 0.11).sin())
            .collect::<Vec<_>>();
        let components = [(3, 0.5_f32), (100, -0.25), (152, 0.125)];
        let mut one = vec![0.0; shape.len()];
        sparse_refresh(&psf, shape, [8, 4], &components, 1, &mut one);
        let pool = rayon::ThreadPoolBuilder::new()
            .num_threads(4)
            .build()
            .unwrap();
        let mut four = vec![0.0; shape.len()];
        pool.install(|| sparse_refresh(&psf, shape, [8, 4], &components, 4, &mut four));
        assert_eq!(one, four);
    }
}

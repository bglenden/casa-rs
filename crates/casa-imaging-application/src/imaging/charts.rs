// SPDX-License-Identifier: LGPL-3.0-or-later
//! Faceted imaging (#664): CASA's `facets=N` splits the main image into N×N
//! facets, each gridded about its own centre by a mapper of its own
//! (`SynthesisImager::createFacetImageStoreList`, `SIMapperCollection`).
//! Here every facet is a *chart*: an operator of the facet's size, a pass
//! domain of the one traversal, onto which casa-ms projects each row with
//! CASA's `UVWMachine(project=true)`. The pass sees charts; the normal state,
//! the minor cycle and the products see the compiled image domains, whose
//! images [`ChartStitch`] assembles from their charts.

use std::collections::BTreeMap;

use casa_imaging_model::{CompiledImageDomain, CompiledProblem};
use casa_imaging_operator::ImageExtent;
use casa_imaging_reconstruction::PassImages;

use super::ImagingError;
use super::measurement::image_extent;

/// One operator's share of an image domain: a facet of a faceted domain, or
/// the whole domain.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct Chart {
    /// The compiled image domain.
    pub(crate) domain: usize,
    /// The facet's pixels when the domain is faceted.
    pub(crate) window: Option<FacetBox>,
}

/// A facet's pixels in its domain: `origin` inclusive, `end` exclusive.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct FacetBox {
    pub(crate) origin: [usize; 2],
    pub(crate) end: [usize; 2],
}

impl FacetBox {
    /// The facet's `[width, height]`.
    pub(crate) const fn shape(self) -> [usize; 2] {
        [
            self.end[0] - self.origin[0],
            self.end[1] - self.origin[1],
        ]
    }
}

/// The charts of `problem` in the order its rows carry their projections:
/// domain-major, a faceted domain's facets in CASA's order (facet
/// `row·N + column`, facet 0 at the bottom left).
pub(crate) fn charts(problem: &CompiledProblem) -> Vec<Chart> {
    problem
        .geometry()
        .domains()
        .iter()
        .enumerate()
        .flat_map(|(domain, compiled)| {
            let faceted = compiled.facets().len() > 1;
            compiled.facets().iter().map(move |window| Chart {
                domain,
                window: faceted.then_some(FacetBox {
                    origin: window.origin(),
                    end: window.end_exclusive(),
                }),
            })
        })
        .collect()
}

impl Chart {
    /// The extent of the chart's operator: the domain's, or the facet's,
    /// whose reference pixel is its centre (CASA grids a facet about the
    /// world direction of its pixel `shape / 2`, `FTMachine::initMaps`).
    pub(crate) fn extent(
        &self,
        compiled: &CompiledImageDomain,
    ) -> Result<ImageExtent, ImagingError> {
        let Some(window) = self.window else {
            return image_extent(compiled);
        };
        let shape = window.shape();
        Ok(ImageExtent {
            shape,
            increment_rad: compiled.direction().increment_rad(),
            // `FacetWindow::local_centre_pixel`: the facet's phase centre.
            reference_pixel: [shape[0] / 2, shape[1] / 2],
        })
    }
}

/// A faceted domain's images as its charts arrive in one pass.
struct Assembly {
    images: PassImages,
    arrived: usize,
}

/// The images a pass hands the normal state: an unfaceted domain's as its
/// chart forms them, a faceted domain's stitched from its facets as CASA's
/// normalizer reads them through each facet store
/// (`SynthesisNormalizer`):
/// - each facet's residual divided by that facet's own `sumwt`, here by
///   scaling facet `f` by `s₀ / s_f` before the normal state divides the
///   whole by facet 0's `s₀`;
/// - facet 0's PSF, placed at the image centre in an image of zeros
///   (`setPsfFromOneFacet`), which the reconstruction normalises by its
///   peak;
/// - facet 0's sums of weights for the domain.
pub(crate) struct ChartStitch {
    charts: Vec<Chart>,
    /// Each image domain's `[width, height]`.
    shapes: Vec<[usize; 2]>,
    /// Each chart's PSF `sumwt` per (term, polarization), from the initial
    /// pass.
    sums: Vec<Option<Vec<f64>>>,
    assemblies: BTreeMap<usize, Assembly>,
}

impl ChartStitch {
    /// The stitch of `problem`'s `charts`.
    pub(crate) fn new(problem: &CompiledProblem, charts: Vec<Chart>) -> Self {
        let shapes = problem
            .geometry()
            .domains()
            .iter()
            .map(|domain| domain.shape().pixels())
            .collect();
        let sums = vec![None; charts.len()];
        Self {
            charts,
            shapes,
            sums,
            assemblies: BTreeMap::new(),
        }
    }

    /// Accept chart `chart`'s images; the domain's images once its last
    /// chart has arrived, at once for an unfaceted domain.
    pub(crate) fn accept(
        &mut self,
        chart: usize,
        mut images: PassImages,
    ) -> Result<Option<PassImages>, &'static str> {
        let Chart { domain, window } = self.charts[chart];
        images.domain = domain;
        let Some(window) = window else {
            return Ok(Some(images));
        };
        if images.psf.is_some() {
            self.sums[chart] = Some(images.sum_weights.clone());
        }
        let first = self
            .charts
            .iter()
            .position(|candidate| candidate.domain == domain)
            .expect("a chart of the domain");
        let facets = self
            .charts
            .iter()
            .filter(|candidate| candidate.domain == domain)
            .count();
        let unscaled = "a facet's residual arrived before its initial pass's sums of weights";
        let (reference, own) = (
            self.sums[first].as_ref().ok_or(unscaled)?,
            self.sums[chart].as_ref().ok_or(unscaled)?,
        );
        let pols = images.polarizations;
        let scales = (0..pols)
            .map(|pol| reference[pol] / own[pol])
            .collect::<Vec<_>>();
        let full = self.shapes[domain];
        let assembly = self.assemblies.entry(domain).or_insert_with(|| Assembly {
            images: empty_domain_images(&images, full),
            arrived: 0,
        });
        let facet = [images.shape[0], images.shape[1]];
        scatter(
            &images.residual,
            &mut assembly.images.residual,
            facet,
            full,
            window.origin,
            |image| scales[image % pols],
        );
        if chart == first {
            if let (Some(psf), Some(stitched)) = (&images.psf, &mut assembly.images.psf) {
                let centred = [(full[0] - facet[0]) / 2, (full[1] - facet[1]) / 2];
                scatter(psf, stitched, facet, full, centred, |_| 1.0);
            }
            assembly.images.sum_weights.clone_from(&images.sum_weights);
            assembly
                .images
                .published_sum_weights
                .clone_from(&images.published_sum_weights);
        }
        assembly.arrived += 1;
        if assembly.arrived < facets {
            return Ok(None);
        }
        Ok(self.assemblies.remove(&domain).map(|assembly| assembly.images))
    }
}

/// Zeroed images of a whole `full` domain shaped like the facet `images`.
fn empty_domain_images(images: &PassImages, full: [usize; 2]) -> PassImages {
    let facet_cells = images.shape[0] * images.shape[1];
    let full_cells = full[0] * full[1];
    let zeros = |values: &[f32]| vec![0.0; values.len() / facet_cells * full_cells];
    PassImages {
        domain: images.domain,
        shape: full,
        channels: images.channels.clone(),
        polarizations: images.polarizations,
        residual: zeros(&images.residual),
        psf: images.psf.as_deref().map(zeros),
        sum_weights: Vec::new(),
        published_sum_weights: Vec::new(),
        weight: None,
    }
}

/// Copy each x-major `facet` image of `from` into the x-major `full` image
/// of `into` at pixel `origin`, image `i` scaled by `scale(i)`.
fn scatter(
    from: &[f32],
    into: &mut [f32],
    facet: [usize; 2],
    full: [usize; 2],
    origin: [usize; 2],
    scale: impl Fn(usize) -> f64,
) {
    let (facet_cells, full_cells) = (facet[0] * facet[1], full[0] * full[1]);
    for (image, values) in from.chunks_exact(facet_cells).enumerate() {
        let factor = scale(image);
        let target = &mut into[image * full_cells..(image + 1) * full_cells];
        for (x, column) in values.chunks_exact(facet[1]).enumerate() {
            let start = (origin[0] + x) * full[1] + origin[1];
            for (cell, value) in target[start..start + facet[1]].iter_mut().zip(column) {
                *cell = (f64::from(*value) * factor) as f32;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Facet images of a `[4, 2]` facet with one polarization: residual
    /// value `10·facet + cell`, PSF `100 + cell` on facet 0.
    fn facet(sum: f64, values: impl Fn(usize) -> f32, initial: bool) -> PassImages {
        PassImages {
            domain: 9,
            shape: [4, 2],
            channels: 0..1,
            polarizations: 1,
            residual: (0..8).map(&values).collect(),
            psf: initial.then(|| (0..8).map(|cell| 100.0 + cell as f32).collect()),
            sum_weights: if initial { vec![sum] } else { Vec::new() },
            published_sum_weights: if initial { vec![sum] } else { Vec::new() },
            weight: None,
        }
    }

    /// Four `[4, 2]` facets of an `[8, 4]` domain: each lands at its
    /// window, scaled by facet 0's sum over its own, and facet 0's PSF lands
    /// centred; a refresh reuses the initial pass's sums.
    #[test]
    fn facets_scatter_to_their_windows_each_divided_by_its_own_sum() {
        let charts = [[0, 0], [4, 0], [0, 2], [4, 2]]
            .map(|origin| Chart {
                domain: 0,
                window: Some(FacetBox {
                    origin,
                    end: [origin[0] + 4, origin[1] + 2],
                }),
            })
            .to_vec();
        let mut stitch = ChartStitch {
            charts,
            shapes: vec![[8, 4]],
            sums: vec![None; 4],
            assemblies: BTreeMap::new(),
        };
        // Facet sums 2, 4, 2, 1: facet f's residual scales by 2 / s_f.
        let sums = [2.0, 4.0, 2.0, 1.0];
        let mut stitched = None;
        for (index, sum) in sums.into_iter().enumerate() {
            let images = facet(sum, |cell| (10 * index + cell) as f32, true);
            stitched = stitch.accept(index, images).unwrap();
            assert_eq!(stitched.is_some(), index == 3, "facet {index}");
        }
        let stitched = stitched.unwrap();
        assert_eq!((stitched.domain, stitched.shape), (0, [8, 4]));
        assert_eq!(stitched.sum_weights, vec![2.0]);
        for (index, origin) in [[0, 0], [4, 0], [0, 2], [4, 2]].into_iter().enumerate() {
            for x in 0..4 {
                for y in 0..2 {
                    let cell = x * 2 + y;
                    let value = stitched.residual[(origin[0] + x) * 4 + origin[1] + y];
                    let expected = (10 * index + cell) as f64 * 2.0 / sums[index];
                    assert_eq!(f64::from(value), expected, "facet {index} cell {cell}");
                }
            }
        }
        // Facet 0's PSF, centred: its centre pixel (2, 1) lands on (4, 2).
        let psf = stitched.psf.unwrap();
        assert_eq!(psf[4 * 4 + 2], 100.0 + (2 * 2 + 1) as f32);
        assert_eq!(psf.iter().filter(|value| **value != 0.0).count(), 8);
        // A refresh pass carries no sums and reuses the initial pass's.
        let mut stitched = None;
        for index in 0..4 {
            stitched = stitch.accept(index, facet(0.0, |_| 1.0, false)).unwrap();
        }
        let residual = stitched.unwrap().residual;
        assert_eq!(residual[(4 + 1) * 4], (2.0 / 4.0) as f32);
        assert_eq!(residual[(4 + 1) * 4 + 3], (2.0 / 1.0) as f32);
    }
}

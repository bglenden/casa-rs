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
        [self.end[0] - self.origin[0], self.end[1] - self.origin[1]]
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

/// CASA's empty-plane threshold: a facet store whose `sumwt` is at most this
/// sets its image to zero rather than divide (`divideImageByWeightVal`).
const EMPTY_SUM_WEIGHT: f64 = 1e-7;

/// One facet's sums of weights per (term, polarization), from the initial
/// pass.
#[derive(Clone, Debug)]
struct FacetSums {
    /// The PSF gridding's, which normalise the PSF.
    psf: Vec<f64>,
    /// CASA's `.sumwt`, which normalises the facet's residual.
    published: Vec<f64>,
}

impl FacetSums {
    fn populated(&self) -> bool {
        self.published.iter().all(|sum| *sum > EMPTY_SUM_WEIGHT)
    }
}

/// A faceted domain's images as its charts arrive in one pass: each facet's
/// residual unscaled at its window, and on the initial pass the PSF of the
/// lowest populated facet so far.
struct Assembly {
    images: PassImages,
    arrived: usize,
    psf: Option<(usize, Vec<f32>)>,
}

/// Why a faceted domain's images could not be stitched; each is a broken
/// invariant of the pass, not a property of the data.
#[derive(Debug, thiserror::Error)]
pub(crate) enum StitchError {
    /// A facet's residual arrived before its initial pass had given its
    /// sums of weights.
    #[error("facet {chart}'s residual arrived before its initial pass's sums of weights")]
    UnscaledFacet { chart: usize },
    /// The domain's facets disagree on their polarizations or planes.
    #[error("facet {chart} does not match its domain's other facets")]
    MismatchedFacet { chart: usize },
}

/// The images a pass hands the normal state: an unfaceted domain's as its
/// chart forms them, a faceted domain's stitched from its facets as CASA's
/// normalizer reads them through each facet store
/// (`SynthesisNormalizer`), with a populated reference facet `r`, the lowest
/// whose `sumwt` exceeds 1e-7 in every polarization (facet 0 unless it is
/// empty):
/// - each facet's residual divided by that facet's own `sumwt`, here by
///   scaling facet `f` by `s_r / s_f` before the normal state divides the
///   whole by `s_r`; an empty facet is zero, as `divideImageByWeightVal`
///   leaves it;
/// - facet `r`'s PSF, placed at the image centre in an image of zeros
///   (`setPsfFromOneFacet`, which takes facet 0), which the reconstruction
///   normalises by its peak;
/// - facet `r`'s sums of weights for the domain.
///
/// Each facet's own sums are published in its pixel of `.sumwt`
/// ([`ChartStitch::facet_sum_weights`]).
pub(crate) struct ChartStitch {
    charts: Vec<Chart>,
    /// Each image domain's `[width, height]`.
    shapes: Vec<[usize; 2]>,
    /// Each chart's sums, from the initial pass.
    sums: Vec<Option<FacetSums>>,
    assemblies: BTreeMap<usize, Assembly>,
    /// The requested polarizations, one image each per plane and term.
    pols: usize,
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
            pols: problem.reconstruction().polarization().coordinates().len(),
        }
    }

    /// Bytes a pass's stitch holds beside the pass's own images: each
    /// faceted domain's full-size residual, and on an initial pass (`psf`)
    /// its full-size PSF and one facet's PSF. Faceted domains have a
    /// constant basis: one plane, one term.
    pub(crate) fn pass_bytes(&self, psf: bool) -> u64 {
        let image = |pixels: [usize; 2]| (pixels[0] * pixels[1] * self.pols * 4) as u64;
        self.charts
            .iter()
            .enumerate()
            .filter(|(chart, candidate)| {
                candidate.window.is_some() && self.first_chart(candidate.domain) == *chart
            })
            .map(|(_, chart)| {
                let full = image(self.shapes[chart.domain]);
                let facet = chart.window.map_or(0, |window| image(window.shape()));
                if psf { 2 * full + facet } else { full }
            })
            .sum()
    }

    /// Each faceted domain's facets' published sums of weights, per facet
    /// in CASA's order and per polarization, from the initial pass:
    /// CASA's `.sumwt` of a faceted image holds each facet store's own.
    pub(crate) fn facet_sum_weights(&self) -> BTreeMap<usize, Vec<Vec<f64>>> {
        let mut domains = BTreeMap::<usize, Vec<Vec<f64>>>::new();
        for (chart, candidate) in self.charts.iter().enumerate() {
            if candidate.window.is_none() {
                continue;
            }
            if let Some(sums) = &self.sums[chart] {
                domains
                    .entry(candidate.domain)
                    .or_default()
                    .push(sums.published.clone());
            }
        }
        domains
    }

    fn first_chart(&self, domain: usize) -> usize {
        self.charts
            .iter()
            .position(|candidate| candidate.domain == domain)
            .expect("a chart of the domain")
    }

    /// Accept chart `chart`'s images; the domain's images once its last
    /// chart has arrived, at once for an unfaceted domain.
    pub(crate) fn accept(
        &mut self,
        chart: usize,
        mut images: PassImages,
    ) -> Result<Option<PassImages>, StitchError> {
        let Chart { domain, window } = self.charts[chart];
        images.domain = domain;
        let Some(window) = window else {
            return Ok(Some(images));
        };
        if images.psf.is_some() {
            self.sums[chart] = Some(FacetSums {
                psf: images.sum_weights.clone(),
                published: images.published_sum_weights.clone(),
            });
        }
        let populated = self.sums[chart]
            .as_ref()
            .ok_or(StitchError::UnscaledFacet { chart })?
            .populated();
        let full = self.shapes[domain];
        let assembly = self.assemblies.entry(domain).or_insert_with(|| Assembly {
            images: empty_domain_images(&images, full),
            arrived: 0,
            psf: None,
        });
        if assembly.images.residual.len() / (full[0] * full[1])
            != images.residual.len() / (images.shape[0] * images.shape[1])
        {
            return Err(StitchError::MismatchedFacet { chart });
        }
        let facet = [images.shape[0], images.shape[1]];
        scatter(
            &images.residual,
            &mut assembly.images.residual,
            facet,
            full,
            window.origin,
            |_| 1.0,
        );
        // Keep the PSF of the lowest populated facet, whatever the order the
        // facets arrive in.
        if let Some(psf) = images.psf.take()
            && populated
            && assembly.psf.as_ref().is_none_or(|(held, _)| chart < *held)
        {
            assembly.psf = Some((chart, psf));
        }
        assembly.arrived += 1;
        let facets = self
            .charts
            .iter()
            .filter(|candidate| candidate.domain == domain)
            .count();
        if assembly.arrived < facets {
            return Ok(None);
        }
        let assembly = self
            .assemblies
            .remove(&domain)
            .expect("the domain's assembly");
        self.finish(domain, assembly).map(Some)
    }

    /// Scale each facet's window of a complete assembly by `s_r / s_f`
    /// (zero for an empty facet), place the reference facet's PSF and give
    /// the domain the reference's sums.
    fn finish(&self, domain: usize, mut assembly: Assembly) -> Result<PassImages, StitchError> {
        let charts = self
            .charts
            .iter()
            .enumerate()
            .filter(|(_, candidate)| candidate.domain == domain)
            .map(|(chart, candidate)| {
                let sums = self.sums[chart]
                    .as_ref()
                    .ok_or(StitchError::UnscaledFacet { chart })?;
                Ok((chart, *candidate, sums))
            })
            .collect::<Result<Vec<_>, _>>()?;
        // Every facet empty: facet 0's (empty) sums make the normal state
        // zero the whole image.
        let reference = charts
            .iter()
            .find(|(_, _, sums)| sums.populated())
            .unwrap_or(&charts[0]);
        let full = self.shapes[domain];
        let pols = assembly.images.polarizations;
        for (_, chart, sums) in &charts {
            let window = chart.window.expect("a faceted domain's chart has a window");
            let scales = (0..pols)
                .map(|pol| {
                    let own = sums.published[pol];
                    if own > EMPTY_SUM_WEIGHT {
                        reference.2.published[pol] / own
                    } else {
                        0.0
                    }
                })
                .collect::<Vec<_>>();
            rescale(&mut assembly.images.residual, window, full, |image| {
                scales[image % pols]
            });
        }
        if let Some(stitched) = assembly.images.psf.as_mut()
            && let Some((held, psf)) = assembly.psf.take()
        {
            debug_assert_eq!(held, reference.0, "the held PSF is the reference facet's");
            let facet = reference.1.window.expect("a facet").shape();
            let centred = [(full[0] - facet[0]) / 2, (full[1] - facet[1]) / 2];
            scatter(&psf, stitched, facet, full, centred, |_| 1.0);
        }
        assembly.images.sum_weights.clone_from(&reference.2.psf);
        assembly
            .images
            .published_sum_weights
            .clone_from(&reference.2.published);
        Ok(assembly.images)
    }
}

/// Scale `window` of each x-major `full` image in `images` by `scale(i)`.
fn rescale(images: &mut [f32], window: FacetBox, full: [usize; 2], scale: impl Fn(usize) -> f64) {
    let full_cells = full[0] * full[1];
    let height = window.shape()[1];
    for (image, values) in images.chunks_exact_mut(full_cells).enumerate() {
        let factor = scale(image);
        for x in window.origin[0]..window.end[0] {
            let start = x * full[1] + window.origin[1];
            for cell in &mut values[start..start + height] {
                *cell = (f64::from(*cell) * factor) as f32;
            }
        }
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

    const ORIGINS: [[usize; 2]; 4] = [[0, 0], [4, 0], [0, 2], [4, 2]];

    /// Four `[4, 2]` facets of an `[8, 4]` domain in CASA's order.
    fn stitch() -> ChartStitch {
        let charts = ORIGINS
            .map(|origin| Chart {
                domain: 0,
                window: Some(FacetBox {
                    origin,
                    end: [origin[0] + 4, origin[1] + 2],
                }),
            })
            .to_vec();
        ChartStitch {
            charts,
            shapes: vec![[8, 4]],
            sums: vec![None; 4],
            assemblies: BTreeMap::new(),
            pols: 1,
        }
    }

    /// The initial pass of facets with `sums`, arriving in `order`, residual
    /// `10·facet + cell + 1`: the stitched images.
    fn initial(stitch: &mut ChartStitch, sums: [f64; 4], order: [usize; 4]) -> PassImages {
        let mut stitched = None;
        for (arrived, index) in order.into_iter().enumerate() {
            let images = facet(sums[index], |cell| (10 * index + cell + 1) as f32, true);
            stitched = stitch.accept(index, images).unwrap();
            assert_eq!(stitched.is_some(), arrived == 3, "facet {index}");
        }
        stitched.unwrap()
    }

    /// The value of facet `index`'s cell `(x, y)` in a stitched `[8, 4]`
    /// residual.
    fn at(residual: &[f32], index: usize, x: usize, y: usize) -> f64 {
        f64::from(residual[(ORIGINS[index][0] + x) * 4 + ORIGINS[index][1] + y])
    }

    /// Each facet lands at its window divided by its own sum, here scaled
    /// by facet 0's over its own; facet 0's PSF lands centred; each facet's
    /// own sum is published; a refresh reuses the initial pass's sums.
    #[test]
    fn facets_scatter_to_their_windows_each_divided_by_its_own_sum() {
        let mut stitch = stitch();
        // Facet sums 2, 4, 2, 1: facet f's residual scales by 2 / s_f.
        let sums = [2.0, 4.0, 2.0, 1.0];
        let stitched = initial(&mut stitch, sums, [0, 1, 2, 3]);
        assert_eq!((stitched.domain, stitched.shape), (0, [8, 4]));
        assert_eq!(stitched.sum_weights, vec![2.0]);
        assert_eq!(stitched.published_sum_weights, vec![2.0]);
        for (index, sum) in sums.into_iter().enumerate() {
            for x in 0..4 {
                for y in 0..2 {
                    let cell = x * 2 + y;
                    let expected = (10 * index + cell + 1) as f64 * 2.0 / sum;
                    assert_eq!(
                        at(&stitched.residual, index, x, y),
                        expected,
                        "facet {index} cell {cell}"
                    );
                }
            }
        }
        // Facet 0's PSF, centred: its centre pixel (2, 1) lands on (4, 2).
        let psf = stitched.psf.unwrap();
        assert_eq!(psf[4 * 4 + 2], 100.0 + (2 * 2 + 1) as f32);
        assert_eq!(psf.iter().filter(|value| **value != 0.0).count(), 8);
        assert_eq!(
            stitch.facet_sum_weights(),
            BTreeMap::from([(0, sums.map(|sum| vec![sum]).to_vec())])
        );
        // A refresh pass carries no sums and reuses the initial pass's.
        let mut stitched = None;
        for index in 0..4 {
            stitched = stitch.accept(index, facet(0.0, |_| 1.0, false)).unwrap();
        }
        let residual = stitched.unwrap().residual;
        assert_eq!(at(&residual, 1, 0, 0), 2.0 / 4.0);
        assert_eq!(at(&residual, 3, 0, 0), 2.0 / 1.0);
    }

    /// A facet whose `sumwt` is at most 1e-7 (rows that land in other
    /// facets' grids but off its own) is zero, as CASA's
    /// `divideImageByWeightVal` leaves an empty facet store, on the initial
    /// pass and every refresh, while the populated facets keep their own
    /// normalisation; its own (empty) sum is published.
    #[test]
    fn an_empty_facet_is_zero_and_the_others_keep_their_own_sums() {
        let mut stitch = stitch();
        let sums = [2.0, 0.0, 2.0, 1.0];
        let stitched = initial(&mut stitch, sums, [0, 1, 2, 3]);
        assert!(stitched.residual.iter().all(|value| value.is_finite()));
        assert_eq!(at(&stitched.residual, 1, 0, 0), 0.0);
        assert_eq!(at(&stitched.residual, 3, 0, 0), 31.0 * 2.0 / 1.0);
        assert_eq!(at(&stitched.residual, 0, 0, 0), 1.0);
        assert_eq!(stitched.sum_weights, vec![2.0]);
        assert_eq!(stitch.facet_sum_weights()[&0][1], vec![0.0]);
        let mut refreshed = None;
        for index in 0..4 {
            refreshed = stitch.accept(index, facet(0.0, |_| 1.0, false)).unwrap();
        }
        let residual = refreshed.unwrap().residual;
        assert!(residual.iter().all(|value| value.is_finite()));
        assert_eq!(at(&residual, 1, 0, 0), 0.0);
        assert_eq!(at(&residual, 2, 0, 0), 1.0);
    }

    /// An empty facet 0 does not empty the image: the lowest populated
    /// facet is the reference, its sums the domain's and its PSF the
    /// image's (CASA's `setPsfFromOneFacet` would take facet 0's empty
    /// PSF), whatever order the facets arrive in.
    #[test]
    fn an_empty_first_facet_hands_the_reference_to_the_next_populated_one() {
        let mut stitch = stitch();
        let sums = [0.0, 4.0, 2.0, 1.0];
        let stitched = initial(&mut stitch, sums, [3, 2, 1, 0]);
        assert_eq!(stitched.sum_weights, vec![4.0]);
        assert_eq!(stitched.published_sum_weights, vec![4.0]);
        assert_eq!(at(&stitched.residual, 0, 0, 0), 0.0);
        // Facet f scales by 4 / s_f, so each is its own sum's after the
        // normal state divides by 4.
        assert_eq!(at(&stitched.residual, 1, 0, 0), 11.0);
        assert_eq!(at(&stitched.residual, 2, 0, 0), 21.0 * 4.0 / 2.0);
        assert_eq!(at(&stitched.residual, 3, 0, 0), 31.0 * 4.0);
        // Facet 1's PSF (every facet's PSF is 100 + cell), centred.
        let psf = stitched.psf.unwrap();
        assert_eq!(psf[4 * 4 + 2], 100.0 + (2 * 2 + 1) as f32);
        assert_eq!(psf.iter().filter(|value| **value != 0.0).count(), 8);
    }

    /// What a pass's stitch holds: a full residual, and on the initial pass
    /// a full PSF and one facet's PSF, f32 per polarization.
    #[test]
    fn a_stitch_charges_its_full_images_and_one_held_facet_psf() {
        let stitch = stitch();
        assert_eq!(stitch.pass_bytes(false), 8 * 4 * 4);
        assert_eq!(stitch.pass_bytes(true), 2 * 8 * 4 * 4 + 4 * 2 * 4);
    }
}

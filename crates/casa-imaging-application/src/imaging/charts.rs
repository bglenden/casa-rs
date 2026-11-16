// SPDX-License-Identifier: LGPL-3.0-or-later
//! Faceted imaging (#664): CASA's `facets=N` splits the main image into N×N
//! facets, each gridded about its own centre by a mapper of its own
//! (`SynthesisImager::createFacetImageStoreList`, `SIMapperCollection`).
//! Here every facet is a *chart*: an operator of the facet's size, a pass
//! domain of the one traversal, onto which casa-ms projects each row with
//! CASA's `UVWMachine(project=true)`. The pass sees charts; the normal state,
//! the minor cycle and the products see the compiled image domains, whose
//! images [`ChartStitch`] assembles from their charts.

use std::{collections::BTreeMap, ops::Range};

use casa_imaging_deconvolution::{PassImages, casa_divides_plane};
use casa_imaging_model::{CompiledImageDomain, CompiledProblem, ReconstructionBasis};
use casa_imaging_operator::ImageExtent;
use casa_imaging_runtime::{Demand, HostResources, Reservation, ResourcePolicy, admit};

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
    fn populated(&self, weights: &Range<usize>, polarizations: usize) -> bool {
        // CASA normalizes every Taylor residual by sumwt.tt0. The higher
        // moments are signed and may vanish even on a populated facet.
        self.published[weights.start..weights.start + polarizations]
            .iter()
            .all(|sum| casa_divides_plane(*sum))
    }
}

/// A faceted domain's current wave of joined images. Each channel keeps
/// the lowest populated facet seen so far as its PSF reference.
struct Assembly {
    images: PassImages,
    arrived: usize,
    references: Vec<Option<usize>>,
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
/// normalizer reads them through each facet store, independently per channel
/// (`SynthesisNormalizer`), with a populated reference facet `r`, the lowest
/// whose zeroth `sumwt` exceeds 1e-7 in every polarization (facet 0 unless
/// it is empty):
/// - each facet's residual divided by that facet's own zeroth `sumwt`, here by
///   scaling facet `f` by `s_r / s_f` before the normal state divides the
///   whole by `s_r`; an empty facet is zero, as `divideImageByWeightVal`
///   leaves it;
/// - facet `r`'s PSF moments, placed at the image centre in images of zeros
///   (`setPsfFromOneFacet`, which takes facet 0), which the reconstruction
///   normalises by the zeroth PSF's peak;
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
    coefficients: usize,
    moments: usize,
    channels: usize,
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
        let (coefficients, moments, channels) = match problem.reconstruction().basis() {
            ReconstructionBasis::Taylor { terms } => (terms, 2 * terms - 1, 1),
            ReconstructionBasis::ChannelLocal { channels } => (1, 1, channels),
            ReconstructionBasis::Constant => (1, 1, 1),
        };
        Self {
            charts,
            shapes,
            sums,
            assemblies: BTreeMap::new(),
            pols: problem.reconstruction().polarization().coordinates().len(),
            coefficients,
            moments,
            channels,
        }
    }

    /// Bytes beside the pass's own images, per channel in its wave: each
    /// faceted domain's full residual, initial PSF and reference index.
    pub(crate) fn pass_bytes_per_channel(&self, psf: bool) -> u64 {
        let images = self.coefficients + if psf { self.moments } else { 0 };
        self.charts
            .iter()
            .enumerate()
            .filter(|(chart, candidate)| {
                candidate.window.is_some() && self.first_chart(candidate.domain) == *chart
            })
            .map(|(_, chart)| {
                let [width, height] = self.shapes[chart.domain];
                (width as u64 * height as u64 * self.pols as u64 * images as u64)
                    * size_of::<f32>() as u64
                    + size_of::<Option<usize>>() as u64
            })
            .sum()
    }

    /// Weight vectors kept for each facet across all waves, including the
    /// copy of the published weights handed on to product generation.
    pub(crate) fn weight_bytes(&self) -> u64 {
        let facets = self
            .charts
            .iter()
            .filter(|chart| chart.window.is_some())
            .count();
        (facets * self.channels * self.moments * self.pols * 3 * size_of::<f64>()) as u64
    }

    /// Hold the weight reservation through product publication.
    pub(crate) fn reserve_weights(
        &self,
        host: &HostResources,
        policy: &ResourcePolicy,
    ) -> Result<Reservation, ImagingError> {
        let memory = self.weight_bytes();
        if memory == 0 {
            return Ok(Reservation::none());
        }
        Ok(admit(
            host,
            policy,
            &Demand {
                phase: "facet weights",
                memory,
            },
        )?)
    }

    fn channel_weights(&self, channel: usize) -> Range<usize> {
        let stride = self.moments * self.pols;
        channel * stride..(channel + 1) * stride
    }

    /// Each faceted domain's facets' published sums of weights, per facet
    /// in CASA's order, then by channel, moment and polarization:
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
        let wave_start = images.channels.start;
        let wave_end = images.channels.end;
        let channels = images.channels.len();
        let moments = self.channel_weights(wave_start).start..self.channel_weights(wave_end).start;
        if images.psf.is_some() {
            let length = self.channel_weights(self.channels).start;
            let sums = self.sums[chart].get_or_insert_with(|| FacetSums {
                psf: vec![0.0; length],
                published: vec![0.0; length],
            });
            sums.psf[moments.clone()].copy_from_slice(&images.sum_weights);
            sums.published[moments].copy_from_slice(&images.published_sum_weights);
        }
        let sums = self.sums[chart]
            .as_ref()
            .ok_or(StitchError::UnscaledFacet { chart })?;
        let full = self.shapes[domain];
        let assembly = self.assemblies.entry(domain).or_insert_with(|| Assembly {
            images: empty_domain_images(&images, full),
            arrived: 0,
            references: vec![None; channels],
        });
        if assembly.images.channels != images.channels
            || assembly.images.residual.len() / (full[0] * full[1])
                != images.residual.len() / (images.shape[0] * images.shape[1])
        {
            return Err(StitchError::MismatchedFacet { chart });
        }
        let facet = window.shape();
        scatter(
            &images.residual,
            &mut assembly.images.residual,
            facet,
            full,
            window.origin,
        );
        let psf_planes = self.pols * self.moments;
        for (local, reference) in assembly.references.iter_mut().enumerate() {
            let start = (wave_start + local) * psf_planes;
            if reference.is_some_and(|first| first < chart)
                || !sums.populated(&(start..start + psf_planes), self.pols)
            {
                continue;
            }
            *reference = Some(chart);
            if let (Some(psf), Some(stitched)) = (&images.psf, &mut assembly.images.psf) {
                let input = psf_planes * facet[0] * facet[1];
                let output = psf_planes * full[0] * full[1];
                let centred = [(full[0] - facet[0]) / 2, (full[1] - facet[1]) / 2];
                scatter(
                    &psf[local * input..(local + 1) * input],
                    &mut stitched[local * output..(local + 1) * output],
                    facet,
                    full,
                    centred,
                );
            }
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
    /// (zero for an empty facet) and give each channel its reference weights.
    fn finish(&self, domain: usize, mut assembly: Assembly) -> Result<PassImages, StitchError> {
        let full = self.shapes[domain];
        let pols = self.pols;
        let coefficients = self.coefficients;
        let channels = assembly.images.channels.clone();
        let first = self.first_chart(domain);
        let reference = |channel: usize| {
            let chart = assembly.references[channel].unwrap_or(first);
            self.sums[chart]
                .as_ref()
                .expect("the reference facet arrived")
        };
        for (chart, candidate) in self.charts.iter().enumerate() {
            if candidate.domain != domain {
                continue;
            }
            let sums = self.sums[chart]
                .as_ref()
                .ok_or(StitchError::UnscaledFacet { chart })?;
            let window = candidate
                .window
                .expect("a faceted domain's chart has a window");
            rescale(&mut assembly.images.residual, window, full, |image| {
                let channel = image / (pols * coefficients);
                let index = self.channel_weights(channels.start + channel).start + image % pols;
                let own = sums.published[index];
                if casa_divides_plane(own) {
                    reference(channel).published[index] / own
                } else {
                    0.0
                }
            });
        }
        if assembly.images.psf.is_some() {
            for (local, channel) in channels.enumerate() {
                let weights = self.channel_weights(channel);
                let sums = reference(local);
                assembly
                    .images
                    .sum_weights
                    .extend_from_slice(&sums.psf[weights.clone()]);
                assembly
                    .images
                    .published_sum_weights
                    .extend_from_slice(&sums.published[weights]);
            }
        }
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
/// of `into` at pixel `origin`.
fn scatter(
    from: &[f32],
    into: &mut [f32],
    facet: [usize; 2],
    full: [usize; 2],
    origin: [usize; 2],
) {
    let (facet_cells, full_cells) = (facet[0] * facet[1], full[0] * full[1]);
    for (image, values) in from.chunks_exact(facet_cells).enumerate() {
        let target = &mut into[image * full_cells..(image + 1) * full_cells];
        for (x, column) in values.chunks_exact(facet[1]).enumerate() {
            let start = (origin[0] + x) * full[1] + origin[1];
            target[start..start + facet[1]].copy_from_slice(column);
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
            coefficients: 1,
            moments: 1,
            channels: 1,
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

    fn taylor_facet(index: usize, initial: bool) -> PassImages {
        let zero = [2.0, 4.0, 1.0, 0.0][index];
        let mut images = facet(zero, |_| 1.0, initial);
        images.residual.extend([3.0; 8]);
        if let Some(psf) = &mut images.psf {
            psf.reserve_exact(16);
            psf[5] += index as f32;
            psf.extend([-(index as f32) - 6.0; 8]);
            psf.extend([index as f32 + 8.0; 8]);
            images.sum_weights.extend([-0.5, 0.75]);
            images.published_sum_weights.extend([-0.75, 0.5]);
        }
        images
    }

    #[test]
    fn taylor_facets_use_zeroth_weights_and_keep_every_psf_moment() {
        let mut stitch = stitch();
        stitch.coefficients = 2;
        stitch.moments = 3;
        let mut assembled = None;
        for index in [3, 2, 1, 0] {
            assembled = stitch.accept(index, taylor_facet(index, true)).unwrap();
        }
        let result = assembled.unwrap();
        let psf = result.psf.unwrap();
        let centre = 4 * 4 + 2;
        assert_eq!(
            [psf[centre], psf[32 + centre], psf[64 + centre]],
            [105.0, -6.0, 8.0]
        );
        assert_eq!(result.published_sum_weights, [2.0, -0.75, 0.5]);
        for first_pass in [true, false] {
            let mut refreshed = None;
            let residual = if first_pass {
                &result.residual
            } else {
                for index in [0, 1, 2, 3] {
                    refreshed = stitch.accept(index, taylor_facet(index, false)).unwrap();
                }
                &refreshed.as_ref().unwrap().residual
            };
            for (index, expected) in [1.0, 0.5, 2.0, 0.0].into_iter().enumerate() {
                assert_eq!(at(residual, index, 0, 0), expected);
                assert_eq!(at(&residual[32..], index, 0, 0), 3.0 * expected);
            }
        }
    }

    #[test]
    fn a_taylor_stitch_charges_every_retained_image_plane() {
        let mut stitch = stitch();
        stitch.coefficients = 2;
        stitch.moments = 3;
        stitch.accept(0, taylor_facet(0, true)).unwrap();
        let assembly = &stitch.assemblies[&0];
        let pass_cells =
            assembly.images.residual.capacity() + assembly.images.psf.as_ref().unwrap().capacity();
        let reference_bytes = assembly.references.capacity() * size_of::<Option<usize>>();
        assert_eq!(
            stitch.pass_bytes_per_channel(true),
            (pass_cells * size_of::<f32>() + reference_bytes) as u64,
        );
        for index in 1..4 {
            stitch.accept(index, taylor_facet(index, true)).unwrap();
        }
        stitch.accept(0, taylor_facet(0, false)).unwrap();
        let assembly = &stitch.assemblies[&0];
        assert!(assembly.images.psf.is_none());
        let refresh_bytes =
            assembly.images.residual.capacity() * size_of::<f32>() + reference_bytes;
        assert_eq!(stitch.pass_bytes_per_channel(false), refresh_bytes as u64);
    }

    const CUBE_WEIGHTS: [[f64; 3]; 4] = [
        [2.0, 0.0, 3.0],
        [4.0, 8.0, 6.0],
        [1.0, 16.0, 3.0],
        [0.0, 4.0, 12.0],
    ];

    fn cube_facet(index: usize, channels: Range<usize>, initial: bool) -> PassImages {
        let mut images = facet(0.0, |_| 0.0, initial);
        images.residual = vec![1.0; 8 * channels.len()];
        if initial {
            images.psf = Some(
                channels
                    .clone()
                    .flat_map(|channel| [1.0 + 10.0 * index as f32 + 100.0 * channel as f32; 8])
                    .collect(),
            );
            images.sum_weights = CUBE_WEIGHTS[index][channels.clone()].to_vec();
            images.published_sum_weights = images.sum_weights.clone();
        }
        images.channels = channels;
        images
    }

    #[test]
    fn cube_facet_references_and_divisors_follow_the_channel_across_waves() {
        let mut stitch = stitch();
        stitch.channels = 3;
        let references = [2.0, 8.0, 3.0];
        let centered_psfs = [1.0, 111.0, 201.0];
        for initial in [true, false] {
            // Change wave boundaries and facet arrival order on refresh.
            let waves = if initial { [0..2, 2..3] } else { [0..1, 1..3] };
            for channels in waves {
                let mut output = None;
                for index in [3, 2, 1, 0] {
                    output = stitch
                        .accept(index, cube_facet(index, channels.clone(), initial))
                        .unwrap();
                }
                let images = output.unwrap();
                assert_eq!(images.channels, channels);
                if let Some(psf) = images.psf {
                    assert_eq!(images.sum_weights, references[channels.clone()]);
                    assert_eq!(images.published_sum_weights, references[channels.clone()]);
                    for (local, absolute) in channels.clone().enumerate() {
                        assert_eq!(psf[local * 32 + 4 * 4 + 2], centered_psfs[absolute]);
                    }
                }
                for (local, absolute) in channels.enumerate() {
                    for (index, weights) in CUBE_WEIGHTS.iter().enumerate() {
                        let own = weights[absolute];
                        let expected = if own > 0.0 {
                            references[absolute] / own
                        } else {
                            0.0
                        };
                        assert_eq!(
                            at(&images.residual[local * 32..], index, 0, 0),
                            expected,
                            "channel {absolute}, facet {index}",
                        );
                    }
                }
            }
            assert_eq!(stitch.facet_sum_weights()[&0], CUBE_WEIGHTS.map(Vec::from));
        }
        let cached = stitch
            .sums
            .iter()
            .flatten()
            .map(|sums| sums.psf.capacity() + sums.published.capacity())
            .sum::<usize>();
        let published = stitch.facet_sum_weights();
        let kept = published[&0].iter().map(Vec::capacity).sum::<usize>();
        assert_eq!(
            stitch.weight_bytes(),
            ((cached + kept) * size_of::<f64>()) as u64
        );
    }

    /// What a pass's stitch holds: a full residual, and on the initial pass
    /// a full PSF, f32 per polarization, and the reference facet index.
    #[test]
    fn a_stitch_charges_its_full_images_and_reference() {
        let stitch = stitch();
        let reference = size_of::<Option<usize>>() as u64;
        assert_eq!(stitch.pass_bytes_per_channel(false), 8 * 4 * 4 + reference);
        assert_eq!(
            stitch.pass_bytes_per_channel(true),
            2 * 8 * 4 * 4 + reference
        );
    }
}

// SPDX-License-Identifier: LGPL-3.0-or-later
//! What a run's setup decides before its first pass: how many workers its
//! phases use (#737), its imaging weights and its paged cube state.
//!
//! The workers are the most, up to the resource policy's, whose memory fits
//! beside what the run already holds. What grows with them is charged as
//! admission then charges it:
//! - the AW kernel sets' cells per holder;
//! - the imaging-weights pass;
//! - the paged cube state's cache;
//! - the region row-work pass, and the gridding work by anchor row it
//!   keeps (#736);
//! - the first major-cycle pass at one plane per wave, on the region strips
//!   that hold the most of any it may cut from that work;
//! - a minor cycle, or a refresh pass, each beside what the run keeps then:
//!   the normal state and model, the imaging weights it kept, the masks, a
//!   cached Clark refresh, the minor cycle's terms and the record of every
//!   cycle before it, up to `nmajor` (#763 review). A run with no `nmajor`
//!   is covered to its first refresh: its records grow with each cycle after
//!   that.
//! - the pass that writes visibilities, at every plane: the first pass of a
//!   run that does not clean, or else a refresh pass (#778 review).
//!
//! A minor cycle never overlaps the first pass: its masks are admitted, and
//! its work begins, after the pass's wave is released. So it is charged once,
//! beside what the run keeps, not on top of the pass
//! ([`StageCharges::largest`]).
//!
//! The count is chosen once, from those charges, and each phase is then
//! admitted as before. A run that no count fits takes one worker, which
//! admission refuses. The products, whose scratch grows with their lanes,
//! choose their lanes when they start, from their own demand
//! (`native::publish_products`).

use std::path::Path;

use casa_imaging_model::{CompiledProblem, ReconstructionBasis, WeightingScheme};
use casa_imaging_operator::{BandwidthTaper, ModeSet, WeightingGeneration};
use casa_imaging_runtime::pass::{
    BackendChoice, BlockShape, BoundedSource, CHUNK_PLACEMENTS, Cancel, Partition, PassDomain,
    WaveDemand, WorkerTeam, density_pass_bytes, needs_row_work, row_work_pass_bytes,
    run_density_pass,
};
use casa_imaging_runtime::{
    CoreAllocation, CubeState, Demand, HostResources, Reservation, ResourcePolicy, admit,
    free_memory,
};

use super::ImagingError;
use super::charts::ChartStitch;
use super::cycle::pass_domains;
use super::measurement::{DomainOperator, density_shape};
use super::source::MeasurementSetSource;

/// The workers a run chose and, when fewer than its policy's, why.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct WorkerChoice {
    /// The allocation the run uses.
    pub(crate) cores: CoreAllocation,
    /// The policy's allocation, the most the run may use.
    pub(crate) policy: CoreAllocation,
    /// Why `cores` has fewer workers than `policy`; `None` when it has as
    /// many.
    pub(crate) reason: Option<String>,
}

/// The allocation of the most workers, up to `policy`'s, whose `demand`
/// fits in `free` bytes, trying `policy`'s first and then each smaller
/// count; one worker when none fits, which admission then refuses.
pub(super) fn fit_workers<E>(
    policy: CoreAllocation,
    free: u64,
    mut demand: impl FnMut(CoreAllocation) -> Result<u64, E>,
) -> Result<WorkerChoice, E> {
    let gigabytes = |bytes: u64| format!("{:.2} GB", bytes as f64 / 1e9);
    let needed = demand(policy)?;
    let choice = |cores, reason| WorkerChoice {
        cores,
        policy,
        reason,
    };
    if needed <= free {
        return Ok(choice(policy, None));
    }
    for workers in (1..policy.workers).rev() {
        let cores = policy.at_most(workers);
        if demand(cores)? <= free {
            let reason = format!(
                "{} workers need {}, {workers} fit {} free",
                policy.workers,
                gigabytes(needed),
                gigabytes(free)
            );
            return Ok(choice(cores, Some(reason)));
        }
    }
    let reason = format!(
        "{} workers need {} and no fewer fit {} free",
        policy.workers,
        gigabytes(needed),
        gigabytes(free)
    );
    Ok(choice(
        policy.at_most(1),
        (policy.workers > 1).then_some(reason),
    ))
}

/// The charges of a run's setup and first cycles that grow with its
/// workers, from the operators and source the run has opened.
pub(super) struct SetupDemand<'r> {
    pub(super) problem: &'r CompiledProblem,
    pub(super) domains: &'r [DomainOperator],
    pub(super) stitch: &'r ChartStitch,
    /// The source's largest block.
    pub(super) block: BlockShape,
    pub(super) native_spacing_hz: f64,
    pub(super) backend: BackendChoice,
    /// The initial pass's modes.
    pub(super) modes: ModeSet,
    /// What the initial pass also holds: the model.
    pub(super) model_bytes: u64,
    /// The modes and model of the pass that writes visibilities, and what
    /// the write holds; it holds every plane.
    pub(super) write: Option<(ModeSet, bool, u64)>,
    pub(super) spill_directory: &'r Path,
    /// Whether the cube state keeps a weight image per plane.
    pub(super) weight_image: bool,
    /// What a minor cycle holds on a team of so many workers.
    pub(super) minor: &'r dyn Fn(usize) -> u64,
    /// What a refresh pass holds beside its own wave; `None` for a run
    /// that does not clean.
    pub(super) refresh: Option<RefreshDemand>,
    /// The imaging weights the passes keep across passes, when the run
    /// keeps them (`Run::keep_weights`).
    pub(super) kept_weights: u64,
    /// The memory the policy leaves free before the setup's charges.
    pub(super) free: u64,
}

/// What the run holds beside a refresh pass's own wave, from the layout
/// the first pass forms (#763 review): the minor cycle's masks, a cached
/// Clark refresh, the model terms the minor cycle hands the pass, and the
/// record of every minor cycle before it, which the run keeps.
pub(super) struct RefreshDemand {
    /// The latest masks and the ones an automatic mask evolves from.
    pub(super) masks: u64,
    /// The Clark refresh a single-plane Clark run caches.
    pub(super) cached: u64,
    /// The minor cycle's model terms, which its solve admits while it still
    /// holds its own charge and the refresh pass holds until the model
    /// takes them (#778 review).
    pub(super) results: u64,
    /// The records of the cycles up to the last refresh `nmajor` allows,
    /// or the first when it allows any number.
    pub(super) records: u64,
}

impl SetupDemand<'_> {
    /// The bytes the run holds at most on `cores`: the kernel sets and the
    /// cube state throughout, beside the imaging-weights pass or, after it,
    /// its density grid with the region row-work pass, and then with the
    /// work and imaging weights it keeps, the first major-cycle pass beside
    /// the model, and after it a minor cycle or a refresh pass beside the
    /// normal state and model and [`RefreshDemand`]
    /// ([`StageCharges::largest`]).
    pub(super) fn bytes(&self, cores: CoreAllocation) -> Result<u64, ImagingError> {
        let streaming = cores.streaming_workers;
        let kernels = self
            .domains
            .iter()
            .map(|domain| domain.pending_kernel_bytes(streaming))
            .sum::<u64>();
        let (weighing, weights) =
            weights_bytes(self.problem, &self.domains[0], self.block, streaming)?;
        let cube = if matches!(
            self.problem.reconstruction().basis(),
            ReconstructionBasis::ChannelLocal { .. }
        ) {
            cube_cache_bytes(
                self.problem,
                self.spill_directory,
                (cores.workers, self.weight_image),
                self.free.saturating_sub(kernels + weights),
            )? as u64
        } else {
            0
        };
        // The first pass cuts its strips from the row work measured before
        // it; no cut holds more than the widest strips (#736).
        let domains = pass_domains(self.domains, streaming, None)
            .into_iter()
            .map(|domain| match domain.partition {
                Partition::Regions { ref regions, .. } => PassDomain {
                    partition: Partition::regions_widest(domain.operator, regions.len()),
                    ..domain
                },
                Partition::Planes { .. } => domain,
            })
            .collect::<Vec<_>>();
        let (measuring, measured) = if needs_row_work(&domains) {
            let rows = domains
                .iter()
                .filter(|domain| domain.partition.splits_rows())
                .map(|domain| domain.operator.geometry().grid_shape()[1])
                .sum::<usize>();
            (
                row_work_pass_bytes(&domains, streaming),
                (rows * size_of::<u64>()) as u64,
            )
        } else {
            (0, 0)
        };
        let wave = |modes: ModeSet, with_model| WaveDemand {
            domains: &domains,
            modes,
            with_model,
            native_spacing_hz: self.native_spacing_hz,
            workers: streaming,
            backend: self.backend,
            block: self.block,
            chunk_placements: CHUNK_PLACEMENTS,
            chunk_sets: 1,
        };
        let refreshing = wave(ModeSet::DATA, true);
        let mut first = wave(self.modes, false).bytes(1) + self.stitch.pass_bytes(true);
        let mut refresh = refreshing.bytes(1) + self.stitch.pass_bytes(false);
        // A run that cleans writes on its last refresh, or on a prediction
        // after it; one that does not, on its first pass (`write_pass`). The
        // pass that writes holds every plane (#778 review).
        if let Some((modes, with_model, written)) = self.write {
            let demand = wave(modes, with_model);
            let writing =
                demand.bytes(demand.planes()) + self.stitch.pass_bytes(modes.psf) + written;
            if self.refresh.is_some() {
                refresh = refresh.max(writing);
            } else {
                first = first.max(writing);
            }
        }
        let stages = StageCharges {
            weighing,
            weights,
            measuring,
            kept: weights + measured + self.kept_weights,
            first_pass: first + self.model_bytes,
            minor: (self.minor)(cores.workers),
            after: self.refresh.as_ref().map(|demand| AfterFirstPass {
                normal: refreshing.resident_normal_bytes(),
                model: self.model_bytes,
                cached: demand.cached,
                records: demand.records,
                results: demand.results,
                refresh_pass: demand.masks + refresh,
            }),
        };
        Ok(kernels + cube + stages.largest())
    }
}

/// The charges of a run's sequential stages beside its kernels and cube
/// state ([`Self::largest`]).
#[derive(Clone, Copy, Debug)]
struct StageCharges {
    /// The imaging-weights pass.
    weighing: u64,
    /// The density grid the weights keep.
    weights: u64,
    /// The region row-work pass.
    measuring: u64,
    /// What the run keeps from the first pass on: the density grid, the
    /// row work and the imaging weights.
    kept: u64,
    /// The first major-cycle pass, its stitched images and the model.
    first_pass: u64,
    /// A minor cycle: its masks and the larger of its two halves.
    minor: u64,
    /// What follows the first pass in a run that cleans; `None` otherwise.
    after: Option<AfterFirstPass>,
}

/// What a cleaning run holds after its first pass besides
/// [`StageCharges::kept`] and either a minor cycle or a refresh pass.
#[derive(Clone, Copy, Debug)]
struct AfterFirstPass {
    /// The resident normal state.
    normal: u64,
    /// The model.
    model: u64,
    /// A cached Clark refresh.
    cached: u64,
    /// The records of the cycles up to the last refresh `nmajor` allows.
    records: u64,
    /// The minor cycle's model terms, which its solve and then the refresh
    /// pass hold.
    results: u64,
    /// A refresh pass, or the one that writes visibilities, with the masks.
    refresh_pass: u64,
}

impl StageCharges {
    /// The largest stage, which is what the run holds at most: the
    /// imaging-weights pass; the density grid beside the row-work pass; the
    /// first pass beside what it keeps; and after it, a minor cycle or a
    /// refresh pass beside the kept charges, the normal state and model, a
    /// cached refresh, the records and the minor cycle's model terms.
    ///
    /// A minor cycle is charged once, after the first pass. It never runs
    /// during one: its masks are admitted and its work begins once the
    /// pass's wave is released. #763 charged it on top of the first pass
    /// too, which put B3 on one worker where its phases admit three.
    fn largest(&self) -> u64 {
        let after = self.after.map_or(0, |after| {
            self.kept
                + after.normal
                + after.model
                + after.cached
                + after.records
                + after.results
                + after.refresh_pass.max(self.minor)
        });
        self.weighing
            .max(self.weights + self.measuring)
            .max(self.kept + self.first_pass)
            .max(after)
    }
}

/// The imaging weights' density pass on `workers` and the density grid it
/// keeps, in bytes; none for natural weighting.
fn weights_bytes(
    problem: &CompiledProblem,
    main: &DomainOperator,
    block: BlockShape,
    workers: usize,
) -> Result<(u64, u64), ImagingError> {
    if matches!(problem.weighting().scheme(), WeightingScheme::Natural) {
        return Ok((0, 0));
    }
    let shape = density_shape(problem, &problem.geometry().domains()[0])?;
    Ok((
        density_pass_bytes(&main.resampler, &shape, block, workers),
        shape.bytes(),
    ))
}

/// CASA's imaging weights for the run: the input weights (natural) or the
/// density weights of one traversal on the main domain's cells, which every
/// domain shares (`SynthesisImagerVi2::weight`), with the charge of the
/// density grid the weights keep. The density pass is admitted under the
/// run's `(host, policy)` before it allocates.
pub(super) fn weighting(
    problem: &CompiledProblem,
    main: &DomainOperator,
    source: &mut MeasurementSetSource<'_>,
    team: &WorkerTeam,
    cancel: &Cancel,
    (host, policy): (&HostResources, &ResourcePolicy),
) -> Result<(WeightingGeneration, Reservation), ImagingError> {
    let contract = problem.weighting();
    let taper = contract.uv_taper();
    let (robust, bandwidth) = match contract.scheme() {
        WeightingScheme::Natural => {
            return Ok((WeightingGeneration::Natural { taper }, Reservation::none()));
        }
        WeightingScheme::Uniform => (None, None),
        WeightingScheme::Briggs { robust } => (Some(robust), None),
        WeightingScheme::BriggsBandwidthTaper { robust } => {
            let spectral = problem.geometry().spectral();
            let last = spectral.output_channels().saturating_sub(1);
            let (Some(first_hz), Some(last_hz)) = (
                spectral.channel_centre_hz(0),
                spectral.channel_centre_hz(last),
            ) else {
                return Err(ImagingError::Unsupported {
                    reason: "the image spectral axis has no channels",
                });
            };
            (
                Some(robust),
                Some(BandwidthTaper::from_frequency_range(first_hz, last_hz)?),
            )
        }
    };
    let shape = density_shape(problem, &problem.geometry().domains()[0])?;
    let (pass, grid) = weights_bytes(problem, main, source.maximum_block(), team.workers())?;
    let mut weights = admit(
        host,
        policy,
        &Demand {
            phase: "imaging weights",
            memory: pass,
        },
    )?;
    let grid_values =
        run_density_pass(&main.operator, &main.resampler, shape, source, team, cancel)?;
    weights.retain(grid);
    let weighting = WeightingGeneration::density(grid_values, robust, bandwidth, taper)?;
    Ok((weighting, weights))
}

/// The cache of the paged cube state on `workers`: as large as the whole
/// cube when a quarter of `free` allows, and at least what the workers'
/// planes need. With a weight image (a mosaic cube) each plane keeps its own
/// beside the PSF.
fn cube_cache_bytes(
    problem: &CompiledProblem,
    directory: &Path,
    (workers, weight_image): (usize, bool),
    free: u64,
) -> Result<usize, ImagingError> {
    let target = problem.model_lifecycle().target();
    let domains = target
        .domains()
        .iter()
        .map(|domain| domain.pixels())
        .collect::<Vec<_>>();
    let planes = target.coefficients() * target.polarizations();
    let (minimum, full) =
        CubeState::cache_limits(directory, &domains, planes, weight_image, workers)?;
    let quarter = usize::try_from(free / 4).unwrap_or(usize::MAX);
    Ok(full.min(minimum.max(quarter)))
}

/// The paged cube state of a channel-local run, its cache admitted as
/// [`cube_cache_bytes`] sizes it from what the policy leaves free; the state
/// holds the cache's reservation for as long as its storage lives.
pub(super) fn cube_state(
    problem: &CompiledProblem,
    directory: &Path,
    (workers, weight_image): (usize, bool),
    host: &HostResources,
    policy: &ResourcePolicy,
) -> Result<CubeState, ImagingError> {
    let cache = cube_cache_bytes(
        problem,
        directory,
        (workers, weight_image),
        free_memory(host, policy),
    )?;
    let reservation = admit(
        host,
        policy,
        &Demand {
            phase: "paged cube state",
            memory: cache as u64,
        },
    )?;
    let target = problem.model_lifecycle().target();
    let domains = target
        .domains()
        .iter()
        .map(|domain| domain.pixels())
        .collect::<Vec<_>>();
    Ok(CubeState::new(
        directory,
        &domains,
        target.coefficients() * target.polarizations(),
        weight_image,
        reservation,
    )?)
}

#[cfg(test)]
mod tests {
    use super::*;

    const POLICY: CoreAllocation = CoreAllocation {
        workers: 4,
        streaming_workers: 3,
    };

    /// A demand of `per_worker` bytes for each streaming and computing
    /// worker beside 100 fixed bytes.
    fn demand(per_worker: u64) -> impl FnMut(CoreAllocation) -> Result<u64, ()> {
        move |cores: CoreAllocation| {
            Ok(100 + per_worker * (cores.workers + cores.streaming_workers) as u64)
        }
    }

    /// The policy's workers when they fit; otherwise the most that fit,
    /// with a reason naming both counts; one when none does.
    #[test]
    fn the_most_workers_whose_demand_fits_are_chosen() {
        let fits = fit_workers(POLICY, 170, demand(10)).unwrap();
        assert_eq!((fits.cores, fits.reason), (POLICY, None));

        let two = fit_workers(POLICY, 140, demand(10)).unwrap();
        assert_eq!(two.cores, POLICY.at_most(2));
        assert_eq!(two.policy, POLICY);
        assert_eq!(
            two.reason.as_deref(),
            Some("4 workers need 0.00 GB, 2 fit 0.00 GB free")
        );

        let none = fit_workers(POLICY, 110, demand(10)).unwrap();
        assert_eq!(none.cores, POLICY.at_most(1));
        assert!(none.reason.unwrap().contains("no fewer fit"));

        let one = CoreAllocation {
            workers: 1,
            streaming_workers: 1,
        };
        let alone = fit_workers(one, 0, demand(10)).unwrap();
        assert_eq!((alone.cores, alone.reason), (one, None));
    }

    /// The counts are tried from the policy's down, and the first that
    /// fits is taken.
    #[test]
    fn counts_are_tried_from_the_policys_down() {
        let mut tried = Vec::new();
        let choice = fit_workers(POLICY, 140, |cores: CoreAllocation| {
            tried.push(cores.workers);
            demand(10)(cores)
        })
        .unwrap();
        assert_eq!(choice.cores.workers, 2);
        assert_eq!(tried, [4, 3, 2]);
    }

    /// A minor cycle is charged once, after the first pass, beside what the
    /// run keeps then, never on top of the first pass: B3's charges on four
    /// workers (MB, rounded), whose minor cycle (3,897) is two thirds of its
    /// first pass (5,952), come to the larger stage, not their sum. B3's
    /// normal state is 4096² cells of two residual terms and three PSF
    /// moments (104 bytes a cell), its record a 4096² mask support, and its
    /// model terms those of 2,001 components of its largest scale's 729
    /// cells on each of two terms, 168 bytes an entry (#778 review); with its
    /// kernels it demands the 8.05 GB its run logs. The minor cycle and the
    /// terms each decide the stage after the first pass; a run that does not
    /// clean charges neither, and a refresh pass larger than the minor cycle
    /// is charged in its place, beside the terms.
    #[test]
    fn a_minor_cycle_is_charged_once_after_the_first_pass() {
        let after = AfterFirstPass {
            normal: 1_745,
            model: 537,
            cached: 0,
            records: 17,
            results: 490,
            refresh_pass: 2_000,
        };
        let b3 = StageCharges {
            weighing: 161,
            weights: 134,
            measuring: 0,
            kept: 176,
            first_pass: 5_952,
            minor: 3_897,
            after: Some(after),
        };
        let following = 176 + 1_745 + 537 + 17 + 490 + 3_897;
        assert!(following > 176 + 5_952, "B3's stage after the first pass");
        assert_eq!(b3.largest(), following);
        assert!(
            b3.largest() < 176 + 5_952 + 3_897,
            "not the pass and the minor cycle"
        );
        // Whatever the first pass, the minor cycle and its terms count.
        let alone = StageCharges {
            first_pass: 1,
            ..b3
        };
        assert_eq!(alone.largest(), following);
        let larger = StageCharges {
            minor: 3_997,
            ..alone
        };
        assert_eq!(larger.largest(), following + 100);
        let termless = StageCharges {
            after: Some(AfterFirstPass {
                results: 0,
                ..after
            }),
            ..alone
        };
        assert_eq!(termless.largest(), following - 490);
        let dirty = StageCharges { after: None, ..b3 };
        assert_eq!(dirty.largest(), 176 + 5_952);
        let refreshing = StageCharges {
            minor: 1,
            first_pass: 1,
            ..b3
        };
        assert_eq!(refreshing.largest(), 176 + 1_745 + 537 + 17 + 490 + 2_000);
    }
}

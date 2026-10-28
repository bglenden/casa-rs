// SPDX-License-Identifier: LGPL-3.0-or-later
//! The observation's batches as a [`BoundedSource`] of native rows: the
//! imaging port's batch route (#932, step 4), beside the selected-observation
//! route ([`super::source::MeasurementSetSource`]) until the cutover, chosen
//! by [`crate::SourceRoute`].
//!
//! Each wave binds one [`AccessRequest`] per selected partition, in turn:
//! the partition's selected channels that reach the wave's planes, the
//! selected fields, and the roles the wave reads. Each batch is packed
//! into a [`NativeBlock`] in one copy: its padding and the samples the UV
//! selection excludes are dropped, and each kept sample's geometry comes
//! from the run's [`GeometryPlan`]. A block holds whole times of one
//! partition. Samples the store flags (MSv2 `FLAG_ROW` included) are kept,
//! as the selected-observation route keeps flagged rows: geometry is
//! formed for them, and the passes skip their flagged values.
//!
//! This module sees only the shared [`Observation`] interface; the
//! selection was resolved against the store when the run was prepared
//! (`compile::partitions`).

mod admission;
mod pack;
mod pointing;
mod requests;
mod selection;
mod survey;
mod window;

use std::ops::Range;

use casa_imaging_model::{
    BatchGeometry, CompiledProblem, DirectionCoordinateSpec, GeometryPlan, GeometryTraversal,
};
use casa_imaging_operator::PlaneRange;
use casa_imaging_runtime::pass::{
    BlockShape, BoundedSource, NativeBlock, SampleContent, SourceError,
};
use casa_imaging_runtime::row_layout;
use casa_observation::{AccessRequest, BatchBuffers, Observation, ReadEvent, Traversal};

pub(crate) use admission::{Batching, admit_batches};
use pack::{Layout, PackScratch, Packer, select_samples};
use pointing::PointingCache;
pub(crate) use pointing::pointing_direction;
pub(crate) use selection::{PartitionSelection, SelectedPartition};
use survey::{PartitionSurvey, survey};
use window::wave_window;

use super::source::PlaneBounds;

/// What the batch route holds open for a run: the observation, the
/// selection resolved to its partitions, the compiled geometry and the
/// admitted batches.
pub(crate) struct BatchRoute {
    pub(crate) observation: Box<dyn Observation>,
    pub(crate) selection: PartitionSelection,
    pub(crate) plan: GeometryPlan,
    pub(crate) batching: Batching,
    /// The bytes the pointing answers the source keeps may hold, under
    /// which the store refuses no query for its size; zero when geometry
    /// reads no pointing.
    pub(crate) pointing_budget: usize,
}

impl BatchRoute {
    /// The aperture classes in dish order
    /// ([`GeometryPlan::aperture_classes`]).
    pub(crate) fn dish_classes(&self) -> Vec<casa_imaging_model::AntennaResponseClass> {
        self.plan.aperture_classes()
    }
}

/// The traversal of one wave.
struct Stream<'a> {
    values: bool,
    /// Each partition's selected channels the wave reads, as ordinals of
    /// its selected channels; `None` when none reaches the wave.
    windows: Vec<Option<Range<usize>>>,
    /// The next partition to bind.
    next: usize,
    /// The partition being read: its index, traversal and block layout.
    current: Option<(usize, Traversal<'a>, Layout)>,
    /// The geometry state carried from batch to batch.
    geometry: GeometryTraversal,
}

enum State<'a> {
    Idle,
    Streaming(Box<Stream<'a>>),
    Failed,
}

/// The observation's selected samples, projected onto every chart.
///
/// A restricted cube wave reads only the selected channels whose
/// output-frame frequencies reach its planes at some selected time, with
/// both interpolation partners at each edge (`window`); a compiled
/// continuum transform fits each whole row, so its traversals are never
/// restricted. A flags-and-weights traversal does not read the
/// visibilities: its values are zero, and a continuum transform only flags
/// the channels it keeps out of the output. A non-finite input in an
/// unflagged sample fails the run with the sample named; one in a flagged
/// sample is ignored.
pub(crate) struct ObservationSource<'a> {
    route: &'a BatchRoute,
    packer: Packer<'a>,
    planes: u32,
    bounds: Option<PlaneBounds>,
    /// Each partition surveyed: at the first traversal when geometry reads
    /// pointing, with the conversion factors at the first restricted wave.
    surveys: Option<Vec<PartitionSurvey>>,
    pointing: PointingCache,
    maximum: BlockShape,
    state: State<'a>,
    buffers: BatchBuffers,
    geometry: BatchGeometry,
    keep: Vec<bool>,
    scratch: PackScratch,
}

impl<'a> ObservationSource<'a> {
    /// A source over `route` for `problem`, delivering rows projected on
    /// every chart. `bounds` enables restricted traversals for a basis of
    /// `planes` planes; `pointing_ramp` carries each row's pointing offset
    /// for a kernel set that ramps to it.
    pub(crate) fn new(
        problem: &'a CompiledProblem,
        route: &'a BatchRoute,
        planes: u32,
        bounds: Option<PlaneBounds>,
        pointing_ramp: bool,
    ) -> Self {
        let layout = row_layout(problem);
        let pointing: Vec<DirectionCoordinateSpec> = if pointing_ramp {
            problem
                .geometry()
                .domains()
                .iter()
                .flat_map(|domain| std::iter::repeat_n(domain.direction(), domain.facets().len()))
                .collect()
        } else {
            Vec::new()
        };
        Self {
            route,
            packer: Packer {
                problem,
                charts: layout.domains,
                pointing,
                dish_classes: route.dish_classes(),
            },
            planes,
            bounds,
            surveys: None,
            pointing: PointingCache::default(),
            maximum: BlockShape {
                rows: route.batching.rows(),
                ..layout
            },
            state: State::Idle,
            buffers: BatchBuffers::default(),
            geometry: BatchGeometry::default(),
            keep: Vec::new(),
            scratch: PackScratch::default(),
        }
    }

    /// Each partition's selected channels the wave over `planes` reads.
    fn windows(
        &mut self,
        planes: PlaneRange,
        restrict: bool,
    ) -> Result<Vec<Option<Range<usize>>>, SourceError> {
        let problem = self.packer.problem;
        let route: &'a BatchRoute = self.route;
        let selection = &route.selection;
        let bounds = (restrict
            && problem.visibility_transform().is_none()
            && planes != PlaneRange::new(0, self.planes))
        .then(|| self.bounds.as_ref().map(|bounds| bounds(planes)))
        .flatten();
        let Some(bounds) = bounds else {
            return Ok(selection
                .partitions
                .iter()
                .map(|partition| Some(0..partition.channels.len()))
                .collect());
        };
        if !(bounds[0].is_finite() && bounds[1].is_finite() && bounds[0] != bounds[1]) {
            return Err("a wave's frequency bounds are empty or not finite".into());
        }
        let surveys = self.surveyed(true)?;
        let catalogue = route.observation.catalogue();
        Ok(selection
            .partitions
            .iter()
            .zip(surveys)
            .map(|(partition, survey)| {
                let factors = survey.factors.expect("factors surveyed");
                let entry = catalogue.partition(partition.id);
                let native = partition
                    .channels
                    .iter()
                    .map(|channel| entry.frequencies_hz[*channel as usize])
                    .collect::<Vec<_>>();
                (!factors.is_empty())
                    .then(|| wave_window(&native, factors, bounds))
                    .flatten()
            })
            .collect())
    }

    /// The partitions surveyed, with their conversion factors when
    /// `factors`: surveyed again only when factors are first needed.
    fn surveyed(&mut self, factors: bool) -> Result<&[PartitionSurvey], SourceError> {
        let current = self.surveys.as_ref().is_some_and(|surveys| {
            !factors || surveys.iter().all(|survey| survey.factors.is_some())
        });
        if !current {
            self.surveys = Some(survey(self.route, factors)?);
        }
        Ok(self.surveys.as_deref().expect("partitions surveyed"))
    }

    /// Bind partition `index`'s traversal of the wave's channels `window`,
    /// its batches holding the admitted times.
    fn bind(
        &self,
        index: usize,
        window: Range<usize>,
        values: bool,
    ) -> Result<(Traversal<'a>, Layout), SourceError> {
        let route: &'a BatchRoute = self.route;
        let partition = &route.selection.partitions[index];
        let entry = route.observation.catalogue().partition(partition.id);
        let layout = Layout::new(partition, entry, window)?;
        let probe = requests::payload(
            (&route.selection, &route.plan),
            entry,
            partition,
            layout.native_channels(),
            values,
            usize::MAX,
        );
        let estimate = route.observation.estimate(&probe)?;
        let held = estimate
            .partitions
            .first()
            .ok_or("an estimate without its partition")?
            .held_bytes_per_time();
        let request = AccessRequest {
            batch_bytes: held.saturating_mul(route.batching.times_per_batch(index)),
            ..probe
        };
        Ok((route.observation.bind_read(&request)?, layout))
    }

    /// Hold partition `index`'s pointing over its selected times, when
    /// geometry reads pointing.
    fn hold_pointing(&mut self, index: usize) -> Result<(), SourceError> {
        if !self.route.plan.reads_pointing() {
            return Ok(());
        }
        let Some(span) = self.surveyed(false)?[index].span else {
            self.pointing.release();
            return Ok(());
        };
        let route = self.route;
        self.pointing.hold(
            route.observation.as_ref(),
            route.pointing_budget,
            (
                pointing_direction(self.packer.problem),
                route.selection.partitions[index].id,
                span,
            ),
        )
    }

    /// Read on until a batch packs a row into `out`; `Ok(false)` once every
    /// partition is read.
    fn advance(
        &mut self,
        stream: &mut Stream<'a>,
        out: &mut NativeBlock,
    ) -> Result<bool, SourceError> {
        loop {
            if stream.current.is_none() {
                let Some(index) = (stream.next..stream.windows.len())
                    .find(|index| stream.windows[*index].is_some())
                else {
                    return Ok(false);
                };
                let window = stream.windows[index].clone().expect("a window");
                let (traversal, layout) = self.bind(index, window, stream.values)?;
                stream.next = index + 1;
                stream.current = Some((index, traversal, layout));
                self.hold_pointing(index)?;
            }
            let (index, traversal, layout) = stream.current.as_mut().expect("a bound partition");
            let route: &'a BatchRoute = self.route;
            let partition = &route.selection.partitions[*index];
            let Some(event) = traversal.next(&mut self.buffers)? else {
                stream.current = None;
                continue;
            };
            let ReadEvent::Batch(batch) = event else {
                continue;
            };
            select_samples(&batch, &route.selection, partition, &mut self.keep)?;
            if !self.keep.iter().any(|keep| *keep) {
                continue;
            }
            route.plan.apply(
                &batch,
                &self.keep,
                self.pointing.series(),
                &mut stream.geometry,
                &mut self.geometry,
            )?;
            self.packer.pack(
                (&batch, partition, layout),
                &self.geometry,
                &self.keep,
                stream.values,
                &mut self.scratch,
                out,
            )?;
            return Ok(true);
        }
    }
}

impl BoundedSource for ObservationSource<'_> {
    fn begin(
        &mut self,
        planes: PlaneRange,
        restrict: bool,
        content: SampleContent,
    ) -> Result<(), SourceError> {
        if !matches!(self.state, State::Idle) {
            return Err("a traversal began before the previous one finished".into());
        }
        self.state = State::Failed;
        let windows = self.windows(planes, restrict)?;
        self.state = State::Streaming(Box::new(Stream {
            values: content == SampleContent::Visibilities,
            windows,
            next: 0,
            current: None,
            geometry: GeometryTraversal::default(),
        }));
        Ok(())
    }

    fn fill(&mut self, out: &mut NativeBlock) -> Result<bool, SourceError> {
        let State::Streaming(mut stream) = std::mem::replace(&mut self.state, State::Failed) else {
            return Err("no traversal is in progress".into());
        };
        let filled = self.advance(&mut stream, out)?;
        self.state = if filled {
            State::Streaming(stream)
        } else {
            State::Idle
        };
        Ok(filled)
    }

    fn maximum_block(&self) -> BlockShape {
        self.maximum
    }
}

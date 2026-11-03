// SPDX-License-Identifier: LGPL-3.0-or-later
//! The observation's batches as a [`BoundedSource`] of native rows: the
//! route every pass reads (#932).
//!
//! An observation is one or several stores (CASA's tclean images a list of
//! MeasurementSets as one), each with its own partitions, selection and
//! [`GeometryPlan`]. Each wave reads every store in turn, in the snapshot's
//! order, and binds one [`AccessRequest`] per selected partition of each:
//! the partition's selected channels that reach the wave's planes, the
//! selected fields, and the roles the wave reads. Each batch is packed
//! into a [`NativeBlock`] in one copy: its padding and the samples the UV
//! selection excludes are dropped, and each kept sample's geometry comes
//! from its store's plan. A block holds whole times of one partition.
//! Samples the store flags (MSv2 `FLAG_ROW` included) are kept: geometry
//! is formed for them, and the passes skip their flagged values.
//!
//! This module sees only the shared [`Observation`] interface; the
//! selection was resolved against each store when the run was prepared
//! (`compile::partitions`).

mod admission;
mod keys;
mod pack;
mod pointing;
mod requests;
mod selection;
mod survey;
mod window;
mod write;

use std::ops::Range;

use casa_imaging_model::{
    BatchGeometry, CompiledProblem, DirectionCoordinateSpec, GeometryPlan, GeometryTraversal,
};
use casa_imaging_operator::PlaneRange;
use casa_imaging_runtime::pass::{
    BlockShape, BoundedSource, NativeBlock, SampleContent, SourceError,
};
use casa_imaging_runtime::row_layout;
use casa_ms::observation_io::UpdateSession;
use casa_observation::{AccessRequest, BatchBuffers, Observation, ReadEvent, Traversal};

pub(crate) use admission::{Batching, StoreBatches, admit_batches};
pub(crate) use keys::StoreKeys;
use pack::{Layout, PackScratch, Packer, select_samples};
use pointing::PointingCache;
pub(crate) use pointing::pointing_direction;
pub(crate) use selection::{PartitionSelection, SelectedPartition};
use survey::{PartitionSurvey, survey};
use window::wave_window;
pub(crate) use write::{BatchWriter, WriteBack};
use write::{Delivered, Provenance, WriteCharge};

/// Output-frame frequency bounds of a plane range, for windowed traversals
/// of a cube: the low edge of the first plane to the high edge of the last.
pub(crate) type PlaneBounds = Box<dyn Fn(PlaneRange) -> [f64; 2] + Send + Sync>;

/// What the batch route holds open for a run: each store's route, in the
/// snapshot's order, the admitted batches, and the write-back of a run that
/// writes visibilities.
pub(crate) struct BatchRoute {
    pub(crate) stores: Vec<StoreRoute>,
    pub(crate) batching: Batching,
    pub(crate) write_back: Option<WriteBack>,
}

/// One store's route: the store, its selection resolved to its partitions
/// and its compiled geometry.
pub(crate) struct StoreRoute {
    pub(crate) store: RouteStore,
    pub(crate) selection: PartitionSelection,
    pub(crate) plan: GeometryPlan,
    /// The bytes the pointing answers the source keeps may hold, under
    /// which the store refuses no query for its size; zero when geometry
    /// reads no pointing.
    pub(crate) pointing_budget: usize,
}

/// What a store's route reads from: an observation, or the update session
/// of a run that writes visibilities back.
pub(crate) enum RouteStore {
    Read(Box<dyn Observation>),
    Write(Box<UpdateSession>),
}

impl StoreRoute {
    /// The observation the route reads.
    pub(crate) fn observation(&self) -> &dyn Observation {
        match &self.store {
            RouteStore::Read(observation) => observation.as_ref(),
            RouteStore::Write(session) => session.observation(),
        }
    }

    /// The update session of a run that writes visibilities.
    fn session(&self) -> Option<&UpdateSession> {
        match &self.store {
            RouteStore::Read(_) => None,
            RouteStore::Write(session) => Some(session),
        }
    }
}

/// One selected partition of the route: its store's and its own place.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct PartitionKey {
    store: usize,
    partition: usize,
}

/// A partition and the ordinals of its selected channels a wave reads;
/// `None` when none reaches the wave.
type PartitionWindow = (PartitionKey, Option<Range<usize>>);

impl BatchRoute {
    /// The keys of the route's stores ([`StoreKeys`]): every store's
    /// aperture classes ([`GeometryPlan::aperture_classes`]) and where its
    /// windows and fields start.
    pub(crate) fn keys(&self) -> StoreKeys {
        StoreKeys::new(
            self.stores
                .iter()
                .map(|store| store.observation().catalogue().as_ref()),
            self.stores
                .iter()
                .flat_map(|store| store.plan.aperture_classes())
                .collect(),
        )
    }

    /// The write-back of a run that writes visibilities.
    pub(crate) fn write_back(&self) -> Option<&WriteBack> {
        self.write_back.as_ref()
    }

    /// Every selected partition, store by store.
    fn partitions(&self) -> impl Iterator<Item = PartitionKey> + '_ {
        self.stores.iter().enumerate().flat_map(|(store, route)| {
            (0..route.selection.partitions.len())
                .map(move |partition| PartitionKey { store, partition })
        })
    }

    /// The request reading partition `key`'s selected channels `window`
    /// (ordinals of its selected channels), with the visibilities when
    /// `values`, its batches holding the admitted times; and the layout of
    /// its blocks.
    fn request(
        &self,
        key: PartitionKey,
        window: Range<usize>,
        values: bool,
    ) -> Result<(AccessRequest, Layout), SourceError> {
        let route = &self.stores[key.store];
        let partition = &route.selection.partitions[key.partition];
        let entry = route.observation().catalogue().partition(partition.id);
        let layout = Layout::new(partition, entry, window)?;
        let probe = requests::payload(
            (&route.selection, &route.plan),
            entry,
            partition,
            layout.native_channels(),
            values,
            usize::MAX,
        );
        let estimate = route.observation().estimate(&probe)?;
        let held = estimate
            .partitions
            .first()
            .ok_or("an estimate without its partition")?
            .held_bytes_per_time();
        let request = AccessRequest {
            batch_bytes: held
                .saturating_mul(self.batching.times_per_batch(key.store, key.partition)),
            ..probe
        };
        Ok((request, layout))
    }

    /// The most bytes the write-back of a writing pass holds beside the
    /// passes', over every partition's traversal of its selected channels
    /// ([`WriteBack::write_bytes`]): the largest updater's, the largest
    /// patch's and the largest provenance's, since the sink's buffers keep
    /// one partition's capacity while another's updater writes; zero when
    /// the run writes nothing.
    pub(crate) fn write_bytes(&self) -> Result<u64, SourceError> {
        let Some(write_back) = self.write_back() else {
            return Ok(0);
        };
        let charge = self
            .partitions()
            .try_fold(WriteCharge::default(), |most, key| {
                let route = &self.stores[key.store];
                let channels = route.selection.partitions[key.partition].channels.len();
                let (request, _) = self.request(key, 0..channels, true)?;
                let session = route
                    .session()
                    .ok_or("a writing run's store was opened to read only")?;
                Ok::<_, SourceError>(most.most(write_back.write_bytes(session, &request)?))
            })?;
        Ok(charge.bytes())
    }
}

/// The traversal of one wave.
struct Stream<'a> {
    values: bool,
    /// Whether the traversal's blocks are written back: each partition it
    /// binds and each block it fills is queued for the sink.
    writing: bool,
    /// Each partition's selected channels the wave reads, as ordinals of
    /// its selected channels, store by store; `None` when none reaches the
    /// wave.
    windows: Vec<PartitionWindow>,
    /// The next partition to bind.
    next: usize,
    /// The partition being read: its key, traversal and block layout.
    current: Option<(PartitionKey, Traversal<'a>, Layout)>,
    /// The geometry state carried from batch to batch of one store, and
    /// that store.
    geometry: (GeometryTraversal, Option<usize>),
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
    /// Each store's partitions surveyed: at the first traversal when
    /// geometry reads pointing, with the conversion factors at the first
    /// restricted wave.
    surveys: Option<Vec<Vec<PartitionSurvey>>>,
    /// Each store's pointing answers.
    pointing: Vec<PointingCache>,
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
                plans: route.stores.iter().map(|store| &store.plan).collect(),
                charts: layout.domains,
                pointing,
                keys: route.keys(),
            },
            planes,
            bounds,
            surveys: None,
            pointing: route
                .stores
                .iter()
                .map(|_| PointingCache::default())
                .collect(),
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
    ) -> Result<Vec<PartitionWindow>, SourceError> {
        let problem = self.packer.problem;
        let route: &'a BatchRoute = self.route;
        let bounds = (restrict
            && problem.visibility_transform().is_none()
            && planes != PlaneRange::new(0, self.planes))
        .then(|| self.bounds.as_ref().map(|bounds| bounds(planes)))
        .flatten();
        let Some(bounds) = bounds else {
            return Ok(route
                .partitions()
                .map(|key| {
                    let partition = &route.stores[key.store].selection.partitions[key.partition];
                    (key, Some(0..partition.channels.len()))
                })
                .collect());
        };
        if !(bounds[0].is_finite() && bounds[1].is_finite() && bounds[0] != bounds[1]) {
            return Err("a wave's frequency bounds are empty or not finite".into());
        }
        let surveys = self.surveyed(true)?;
        Ok(route
            .partitions()
            .map(|key| {
                let store = &route.stores[key.store];
                let partition = &store.selection.partitions[key.partition];
                let factors = surveys[key.store][key.partition]
                    .factors
                    .expect("factors surveyed");
                let entry = store.observation().catalogue().partition(partition.id);
                let native = partition
                    .channels
                    .iter()
                    .map(|channel| entry.frequencies_hz[*channel as usize])
                    .collect::<Vec<_>>();
                let window = (!factors.is_empty())
                    .then(|| wave_window(&native, factors, bounds))
                    .flatten();
                (key, window)
            })
            .collect())
    }

    /// Each store's partitions surveyed, with their conversion factors
    /// when `factors`: surveyed again only when factors are first needed.
    fn surveyed(&mut self, factors: bool) -> Result<&[Vec<PartitionSurvey>], SourceError> {
        let current = self.surveys.as_ref().is_some_and(|surveys| {
            !factors
                || surveys
                    .iter()
                    .flatten()
                    .all(|survey| survey.factors.is_some())
        });
        if !current {
            let survey_bytes = self.route.batching.survey_bytes();
            self.surveys = Some(
                self.route
                    .stores
                    .iter()
                    .map(|store| survey(store, survey_bytes, factors))
                    .collect::<Result<_, _>>()?,
            );
        }
        Ok(self.surveys.as_deref().expect("partitions surveyed"))
    }

    /// Bind partition `key`'s traversal of the wave's channels `window`,
    /// its batches holding the admitted times; a writing traversal queues
    /// the request for its sink.
    fn bind(
        &self,
        key: PartitionKey,
        window: Range<usize>,
        (values, writing): (bool, bool),
    ) -> Result<(Traversal<'a>, Layout), SourceError> {
        let route: &'a BatchRoute = self.route;
        let (request, layout) = route.request(key, window, values)?;
        let traversal = route.stores[key.store].observation().bind_read(&request)?;
        if writing && let Some(write_back) = route.write_back() {
            write_back.deliver(Delivered::Partition {
                store: key.store,
                request,
            });
        }
        Ok((traversal, layout))
    }

    /// Hold partition `key`'s pointing over its selected times, when its
    /// store's geometry reads pointing.
    fn hold_pointing(&mut self, key: PartitionKey) -> Result<(), SourceError> {
        let route: &'a BatchRoute = self.route;
        let store = &route.stores[key.store];
        if !store.plan.reads_pointing() {
            return Ok(());
        }
        let Some(span) = self.surveyed(false)?[key.store][key.partition].span else {
            self.pointing[key.store].release();
            return Ok(());
        };
        self.pointing[key.store].hold(
            store.observation(),
            store.pointing_budget,
            (
                pointing_direction(self.packer.problem),
                store.selection.partitions[key.partition].id,
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
                let Some(next) = (stream.next..stream.windows.len())
                    .find(|index| stream.windows[*index].1.is_some())
                else {
                    return Ok(false);
                };
                let (key, window) = stream.windows[next].clone();
                let window = window.expect("a window");
                let (traversal, layout) =
                    self.bind(key, window, (stream.values, stream.writing))?;
                if stream.geometry.1 != Some(key.store) {
                    // Each store's plan carries its own geometry state.
                    stream.geometry = (GeometryTraversal::default(), Some(key.store));
                }
                stream.next = next + 1;
                stream.current = Some((key, traversal, layout));
                self.hold_pointing(key)?;
            }
            let (key, traversal, layout) = stream.current.as_mut().expect("a bound partition");
            let route: &'a BatchRoute = self.route;
            let store = &route.stores[key.store];
            let partition = &store.selection.partitions[key.partition];
            let Some(event) = traversal.next(&mut self.buffers)? else {
                stream.current = None;
                continue;
            };
            let ReadEvent::Batch(batch) = event else {
                continue;
            };
            select_samples(&batch, &store.selection, partition, &mut self.keep)?;
            if !self.keep.iter().any(|keep| *keep) {
                continue;
            }
            store.plan.apply(
                &batch,
                &self.keep,
                self.pointing[key.store].series(),
                &mut stream.geometry.0,
                &mut self.geometry,
            )?;
            self.packer.pack(
                (&batch, key.store, partition, layout),
                &self.geometry,
                &self.keep,
                stream.values,
                &mut self.scratch,
                out,
            )?;
            if stream.writing
                && let Some(write_back) = route.write_back()
            {
                // The block's rows are the kept samples, in order.
                let mut samples = Vec::with_capacity(batch.samples());
                samples.extend(
                    (0..batch.samples())
                        .filter(|sample| self.keep[*sample])
                        .map(|sample| u32::try_from(sample).expect("a batch's samples fit u32")),
                );
                write_back.deliver(Delivered::Block(Provenance {
                    address: batch.address(),
                    samples,
                    spectral_window: self
                        .packer
                        .keys
                        .window(key.store, partition.spectral_window),
                }));
            }
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
        let values = content == SampleContent::Visibilities;
        let writing = match self.route.write_back() {
            Some(write_back) if values => write_back.writing()?,
            _ => false,
        };
        self.state = State::Streaming(Box::new(Stream {
            values,
            writing,
            windows,
            next: 0,
            current: None,
            geometry: (GeometryTraversal::default(), None),
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

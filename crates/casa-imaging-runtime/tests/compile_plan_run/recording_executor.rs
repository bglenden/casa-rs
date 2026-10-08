// SPDX-License-Identifier: LGPL-3.0-or-later

//! The recording work implementation every plan/run test executes: it
//! records calls, knobs, fences and observation completions, injects the
//! configured failures and reports the plan's claims as its measurements.

use super::*;

pub(crate) fn artifact_measurement(
    planned: ArtifactIdentity,
    observed: Option<ArtifactIdentity>,
    disposition: ArtifactDisposition,
    bytes: u64,
    path: Option<RedactedPath>,
) -> ArtifactMeasurement {
    ArtifactMeasurement::new(planned, observed, disposition, bytes, path)
        .expect("test adapters only report externally constructible artifact dispositions")
}

pub(crate) fn product_measurement_executor(plan: &ProductPublicationPlan) -> RecordingExecutor {
    let mut executor = recording_executor(6, None, None);
    let authorization = plan;
    executor.sealed_measurements = Some(
        authorization
            .entries()
            .iter()
            .map(|entry| {
                ArtifactMeasurement::new(
                    entry.artifact(),
                    None,
                    ArtifactDisposition::Staged,
                    entry.payload_bytes(),
                    None,
                )
                .expect("publication evidence is externally constructible")
            })
            .collect(),
    );
    executor
}

pub(crate) fn recording_executor(
    byte: u8,
    failure: Option<&'static str>,
    fence_failure: Option<&'static str>,
) -> RecordingExecutor {
    RecordingExecutor {
        id: implementation(byte),
        failure,
        fence_failure,
        fail_only_fence: None,
        calls: AtomicUsize::new(0),
        fence_waits: AtomicUsize::new(0),
        observed_knobs: Mutex::new(Vec::new()),
        aborted_nodes: Mutex::new(Vec::new()),
        measurements: BTreeMap::new(),
        fence_measurement_node: None,
        resource_peak_overrides: BTreeMap::new(),
        panic_on_execute: false,
        publication_launched: None,
        visible_generation: None,
        failure_node: None,
        fence_failure_event: None,
        publication_failure: None,
        generic_source_access: None,
        initial_consistency_expected: None,
        visibility_during_fence_settlement: None,
        publication_buffer_held: None,
        receipt_root_to_disrupt: None,
        publication_pause: None,
        sealed_measurements: None,
        publication_path: None,
        native_publication: Mutex::new(None),
        observation_completions: None,
        delivered_observation_completions: None,
        observation_completion_failure: None,
        bind_foreign_observation_completion: false,
        selected_observation_completion: Mutex::new(None),
        major_cycle_problem: None,
    }
}

#[derive(Debug, Default)]
pub(crate) struct PublicationPause {
    entered: AtomicBool,
    release: Mutex<bool>,
    released: Condvar,
}

impl PublicationPause {
    pub(crate) fn wait_until_entered(&self, timeout: Duration) -> bool {
        let started = Instant::now();
        while !self.entered.load(Ordering::SeqCst) && started.elapsed() < timeout {
            std::thread::yield_now();
        }
        self.entered.load(Ordering::SeqCst)
    }

    pub(crate) fn release(&self) {
        *self.release.lock().expect("publication pause lock") = true;
        self.released.notify_all();
    }
}

#[derive(Debug)]
pub(crate) struct RecordedObservationCompletion {
    pub(crate) attempt_id: casa_imaging_runtime::ExecutionAttemptId,
    pub(crate) owner_node: WorkNodeId,
    pub(crate) settled_fences: BTreeSet<FenceKind>,
    pub(crate) lease_epoch: u64,
}

pub(crate) type DeliveredObservationCompletions = Arc<Mutex<Vec<(WorkNodeId, WorkNodeId)>>>;

pub(crate) struct RecordingExecutor {
    pub(crate) id: WorkImplementationId,
    pub(crate) failure: Option<&'static str>,
    pub(crate) fence_failure: Option<&'static str>,
    pub(crate) fail_only_fence: Option<FenceKind>,
    pub(crate) calls: AtomicUsize,
    pub(crate) fence_waits: AtomicUsize,
    pub(crate) observed_knobs: Mutex<Vec<ExecutionKnobs>>,
    pub(crate) aborted_nodes: Mutex<Vec<WorkNodeId>>,
    pub(crate) measurements: BTreeMap<WorkNodeId, (Vec<IoMeasurement>, Vec<ArtifactMeasurement>)>,
    pub(crate) fence_measurement_node: Option<WorkNodeId>,
    pub(crate) resource_peak_overrides: BTreeMap<WorkNodeId, u64>,
    pub(crate) panic_on_execute: bool,
    pub(crate) publication_launched: Option<Arc<AtomicBool>>,
    pub(crate) visible_generation: Option<Arc<AtomicUsize>>,
    pub(crate) failure_node: Option<&'static str>,
    pub(crate) fence_failure_event: Option<(&'static str, FenceKind)>,
    pub(crate) publication_failure: Option<&'static str>,
    pub(crate) generic_source_access: Option<Arc<AtomicBool>>,
    pub(crate) initial_consistency_expected: Option<(ObservationTransactionId, Arc<AtomicBool>)>,
    pub(crate) visibility_during_fence_settlement: Option<Arc<AtomicBool>>,
    pub(crate) publication_buffer_held: Option<Arc<AtomicBool>>,
    pub(crate) receipt_root_to_disrupt: Option<PathBuf>,
    pub(crate) publication_pause: Option<Arc<PublicationPause>>,
    pub(crate) sealed_measurements: Option<Vec<ArtifactMeasurement>>,
    pub(crate) publication_path: Option<RedactedPath>,
    native_publication: Mutex<Option<NativePublicationFixture>>,
    pub(crate) observation_completions: Option<Arc<Mutex<Vec<RecordedObservationCompletion>>>>,
    pub(crate) delivered_observation_completions: Option<DeliveredObservationCompletions>,
    pub(crate) observation_completion_failure: Option<&'static str>,
    pub(crate) bind_foreign_observation_completion: bool,
    selected_observation_completion: Mutex<Option<SelectedObservationCompletion>>,
    pub(crate) major_cycle_problem: Option<casa_imaging_model::CompiledProblem>,
}

#[derive(Clone)]
struct NativePublicationFixture {
    plan: ProductPublicationPlan,
}

impl RecordingExecutor {
    fn native_publication(&self) -> Option<NativePublicationFixture> {
        let mut cached = self
            .native_publication
            .lock()
            .expect("native publication cache lock");
        if cached.is_none() {
            let problem = self.major_cycle_problem.as_ref()?;
            let plan = publication_plan_for_problem(problem);
            *cached = Some(NativePublicationFixture { plan });
        }
        cached.clone()
    }

    fn native_sealed_measurements(&self) -> Option<Vec<ArtifactMeasurement>> {
        let fixture = self.native_publication()?;
        let authorization = fixture.plan;
        Some(
            authorization
                .entries()
                .iter()
                .map(|entry| {
                    ArtifactMeasurement::new(
                        entry.artifact(),
                        None,
                        ArtifactDisposition::Staged,
                        entry.payload_bytes(),
                        None,
                    )
                    .expect("publication evidence is externally constructible")
                })
                .collect(),
        )
    }

    fn work_measurements(&self, context: WorkExecutionContext<'_>) -> WorkMeasurements {
        let resources = context
            .node()
            .claims
            .iter()
            .map(|claim| {
                ResourceMeasurement::new(
                    claim.resource.clone(),
                    claim.lifetime.clone(),
                    self.resource_peak_overrides
                        .get(&context.node().id)
                        .copied()
                        .unwrap_or(claim.amount),
                )
            })
            .collect();
        let (io, mut artifacts) = self
            .measurements
            .get(&context.node().id)
            .cloned()
            .unwrap_or_else(|| {
                (
                    context
                        .node()
                        .claims
                        .iter()
                        .filter_map(|claim| match claim.resource {
                            LeaseResource::IoBuffer(kind) => {
                                Some(IoMeasurement::new(kind, claim.amount, 1))
                            }
                            _ => None,
                        })
                        .collect(),
                    Vec::new(),
                )
            });
        if context.node().kind == WorkKind::Publication
            && let Some(sealed) = self
                .sealed_measurements
                .clone()
                .or_else(|| self.native_sealed_measurements())
        {
            artifacts = sealed;
        }
        if context.node().kind == WorkKind::Publication && artifacts.is_empty() {
            artifacts = context
                .planned_artifacts()
                .map(|artifact| {
                    let identity = artifact.identity();
                    ArtifactMeasurement::new(
                        identity,
                        Some(identity),
                        ArtifactDisposition::Staged,
                        1,
                        None,
                    )
                    .expect("publication evidence is externally constructible")
                })
                .collect();
        }
        WorkMeasurements::new(resources, io, artifacts)
    }

    fn observe_publication_buffer(&self, context: WorkExecutionContext<'_>) {
        let Some(observed) = &self.publication_buffer_held else {
            return;
        };
        let allocation = AllocationId::new("transaction-publication-buffer");
        let slot = PhysicalSlotId::new("transaction-publication-slot");
        observed.store(
            context.publication_resources().is_some_and(|resources| {
                resources.lease_epoch() > 0 && resources.allocation_slot(&allocation) == Some(&slot)
            }),
            Ordering::SeqCst,
        );
    }

    fn await_publication_visibility(&self) -> Result<(), io::Error> {
        if let Some(message) = self.publication_failure {
            return Err(io::Error::other(message));
        }
        if let Some(pause) = &self.publication_pause {
            pause.entered.store(true, Ordering::SeqCst);
            let mut release = pause.release.lock().expect("publication pause lock");
            while !*release {
                release = pause
                    .released
                    .wait(release)
                    .expect("publication pause wait");
            }
        }
        Ok(())
    }

    fn expose_publication_visibility(
        &self,
        context: WorkExecutionContext<'_>,
    ) -> Result<(), io::Error> {
        self.observe_publication_buffer(context);
        if let Some(visible) = &self.visible_generation {
            visible.store(1, Ordering::SeqCst);
        }
        if let Some(root) = &self.receipt_root_to_disrupt {
            for entry in fs::read_dir(root)? {
                let path = entry?.path();
                if path.extension().is_none_or(|extension| extension != "json") {
                    fs::remove_file(path)?;
                }
            }
        }
        Ok(())
    }
}

fn open_selected_observation(
    problem: &casa_imaging_model::CompiledProblem,
    residency: &SelectedObservationResidencyCertificate,
) -> io::Result<BoundSelectedObservation> {
    deferred_selected_observation(problem, residency)?
        .open(problem)
        .map_err(io::Error::other)
}

fn deferred_selected_observation(
    problem: &casa_imaging_model::CompiledProblem,
    residency: &SelectedObservationResidencyCertificate,
) -> io::Result<casa_ms::DeferredSelectedObservationAccess> {
    let sources = problem.inputs().observation_snapshot().sources();
    let mut budgets = Vec::with_capacity(sources.len());
    for source in sources {
        budgets.push(
            residency
                .content_budget(source.identity())
                .ok_or_else(|| io::Error::other("residency certificate omits source"))?,
        );
    }
    let bindings = selected_observation_bindings(problem, |index| budgets[index]);
    let measures_identity = problem
        .inputs()
        .reference_data()
        .iter()
        .find_map(|(kind, identity)| (*kind == ReferenceDataKind::Measures).then_some(*identity))
        .ok_or_else(|| io::Error::other("ObservationRead has no Measures identity"))?;
    let measures = SelectedObservationMeasures::new(
        casa_test_support::deterministic_measures_provider_for_identity(
            measures_identity.as_bytes(),
        ),
    )
    .map_err(io::Error::other)?;
    Ok(casa_ms::DeferredSelectedObservationAccess::new(
        measures, bindings,
    ))
}

impl WorkImplementation for RecordingExecutor {
    type Error = io::Error;

    fn implementation_id(&self) -> &WorkImplementationId {
        &self.id
    }

    fn abort_node_io(&self, owner_node: &WorkNodeId) -> Result<(), Self::Error> {
        self.aborted_nodes
            .lock()
            .expect("recording executor abort lock")
            .push(owner_node.clone());
        Ok(())
    }

    fn execute(&self, context: WorkExecutionContext<'_>) -> Result<WorkMeasurements, Self::Error> {
        assert!(!self.panic_on_execute, "interrupted adapter");
        self.calls.fetch_add(1, Ordering::SeqCst);
        self.observed_knobs
            .lock()
            .expect("recording executor knobs lock")
            .push(context.knobs().clone());
        if let Some(message) = self.failure {
            return Err(io::Error::other(message));
        }
        if self.failure_node == Some(context.node().id.as_str()) {
            return Err(io::Error::other("stateful transaction execute failure"));
        }
        if context.node().kind == WorkKind::ObservationRead {
            let bound_problem = context
                .selected_observation()
                .expect("ObservationRead owns exact selected-observation authority");
            let foreign_problem = self
                .bind_foreign_observation_completion
                .then(|| compile(request(2)).map_err(io::Error::other))
                .transpose()?;
            let problem = foreign_problem.as_ref().unwrap_or(bound_problem);
            let residency = selected_content_residency(problem);
            let mut observation = open_selected_observation(problem, &residency)?;
            let completion = observation
                .traverse(problem, |_| Ok::<_, io::Error>(()))
                .map_err(io::Error::other)?;
            *self
                .selected_observation_completion
                .lock()
                .expect("selected-observation completion lock") = Some(completion);
        } else if let Some(delivered) = &self.delivered_observation_completions {
            let owner = WorkNodeId::new("transaction-read");
            if let Some(completion) = context.predecessor_observation_completion(&owner) {
                assert_eq!(completion.owner_node(), &owner);
                assert!(
                    context
                        .predecessor_observation_completion(&WorkNodeId::new("not-a-predecessor"))
                        .is_none(),
                    "attempt evidence must not escape its explicit dependency edge"
                );
                delivered
                    .lock()
                    .expect("delivered observation completion lock")
                    .push((context.node().id.clone(), owner));
            }
        }
        if let Some((expected, accessed)) = &self.initial_consistency_expected {
            if context.node().kind == WorkKind::DataCensus {
                accessed.store(
                    context
                        .observation_consistency()
                        .is_some_and(|transaction| {
                            transaction.transaction_id() == *expected
                                && !transaction.read_set().sources().is_empty()
                        }),
                    Ordering::SeqCst,
                );
            } else if context.observation_consistency().is_some() {
                return Err(io::Error::other(
                    "observation consistency capability escaped its initial check",
                ));
            }
        }
        if context.node().kind == WorkKind::Io
            && context.observation_reads().is_some()
            && let Some(accessed) = &self.generic_source_access
        {
            accessed.store(true, Ordering::SeqCst);
        }
        if context.node().kind == WorkKind::Publication
            && let Some(launched) = &self.publication_launched
        {
            launched.store(true, Ordering::SeqCst);
        }
        if self.fence_measurement_node.as_ref() == Some(&context.node().id) {
            Ok(WorkMeasurements::default())
        } else {
            Ok(self.work_measurements(context))
        }
    }

    fn failure_measurements<'error>(
        &'error self,
        _error: &'error Self::Error,
    ) -> Option<&'error WorkMeasurements> {
        None
    }

    fn wait_for_fence(
        &self,
        context: WorkExecutionContext<'_>,
        fence: FenceKind,
    ) -> Result<WorkMeasurements, Self::Error> {
        self.fence_waits.fetch_add(1, Ordering::SeqCst);
        if let Some(message) = self.fence_failure
            && self.fail_only_fence.is_none_or(|kind| kind == fence)
        {
            return Err(io::Error::other(message));
        }
        if self.fence_failure_event == Some((context.node().id.as_str(), fence)) {
            return Err(io::Error::other("stateful transaction fence failure"));
        }
        if self
            .visible_generation
            .as_ref()
            .is_some_and(|visible| visible.load(Ordering::SeqCst) == 1)
            && let Some(observed) = &self.visibility_during_fence_settlement
        {
            observed.store(true, Ordering::SeqCst);
        }
        if self.fence_measurement_node.as_ref() == Some(&context.node().id) {
            Ok(self.work_measurements(context))
        } else {
            Ok(WorkMeasurements::default())
        }
    }

    fn complete_observation_read(
        &self,
        completion: ObservationReadCompletionContext,
    ) -> Result<AttemptBoundObservationCompletion, Self::Error> {
        if let Some(completions) = &self.observation_completions {
            completions
                .lock()
                .expect("observation completion lock")
                .push(RecordedObservationCompletion {
                    attempt_id: completion.attempt_id(),
                    owner_node: completion.owner_node().clone(),
                    settled_fences: completion.settled_fences().clone(),
                    lease_epoch: completion.lease_epoch(),
                });
        }
        if let Some(message) = self.observation_completion_failure {
            return Err(io::Error::other(message));
        }
        let owner_completion = self
            .selected_observation_completion
            .lock()
            .expect("selected-observation completion lock")
            .take()
            .ok_or_else(|| io::Error::other("ObservationRead produced no scientific completion"))?;
        completion.bind(owner_completion).map_err(io::Error::other)
    }

    fn publish(&self, context: WorkExecutionContext<'_>) -> Result<(), Self::Error> {
        if context.node().kind != WorkKind::Publication || context.publication().is_none() {
            return Err(io::Error::other(
                "publication requires the transaction-bound Publication node",
            ));
        }
        self.await_publication_visibility()?;
        self.expose_publication_visibility(context)
    }
}

pub(crate) fn product_publication_recording_executor(
    problem: &casa_imaging_model::CompiledProblem,
    launched: Arc<AtomicBool>,
    visible_generation: Arc<AtomicUsize>,
) -> RecordingExecutor {
    let publication = publication_plan_for_problem(problem);
    let mut executor = product_measurement_executor(&publication);
    executor.major_cycle_problem = Some(problem.clone());
    executor.publication_launched = Some(launched);
    executor.visible_generation = Some(visible_generation);
    executor
}

pub(crate) fn failing_transaction_executor(
    byte: u8,
    visible_generation: Arc<AtomicUsize>,
    failure_node: Option<&'static str>,
    fence_failure_event: Option<(&'static str, FenceKind)>,
    publication_failure: Option<&'static str>,
) -> RecordingExecutor {
    let mut executor = recording_executor(byte, None, None);
    executor.visible_generation = Some(visible_generation);
    executor.failure_node = failure_node;
    executor.fence_failure_event = fence_failure_event;
    executor.publication_failure = publication_failure;
    executor
}

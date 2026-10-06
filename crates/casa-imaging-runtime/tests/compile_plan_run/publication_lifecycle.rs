// SPDX-License-Identifier: LGPL-3.0-or-later

use super::*;
use casa_imaging_runtime::SerialProductPublicationExecutionError;

#[derive(Clone, Copy, PartialEq, Eq)]
enum GenerationOutcome {
    Success,
    Failure,
    Panic,
}

struct ProbeSink {
    inner: InMemoryProductSink,
    generation: GenerationOutcome,
    begin_calls: AtomicUsize,
}

impl casa_imaging_products::ProductOutput for ProbeSink {
    fn begin_member<'a>(
        &'a self,
        member: &casa_imaging_products::PlannedMember,
        layout: casa_imaging_products::ProductWindowLayout,
        beams: &[Option<casa_imaging_products::RestoringBeam>],
    ) -> Result<
        Box<dyn casa_imaging_products::ProductWriter + 'a>,
        casa_imaging_products::ProductsError,
    > {
        self.begin_calls.fetch_add(1, Ordering::SeqCst);
        match self.generation {
            GenerationOutcome::Success => self.inner.begin_member(member, layout, beams),
            GenerationOutcome::Failure => Err(casa_imaging_products::ProductsError::Storage(
                "injected generation failure".into(),
            )),
            GenerationOutcome::Panic => panic!("injected writer panic"),
        }
    }
}

impl SerialProductPublicationSink for ProbeSink {
    type Error = io::Error;

    fn residency(
        &self,
        planned: &casa_imaging_products::PlannedContinuumGeneration,
        demand: &ContinuumGenerationDemand,
    ) -> Result<casa_imaging_runtime::ProductSinkResidency, Self::Error> {
        self.inner.residency(planned, demand)
    }

    fn publish(&self) -> Result<(), Self::Error> {
        if self.inner.fail {
            let first = self.inner.staged.lock().unwrap()[0];
            self.inner.visible.lock().unwrap().push(first);
        }
        self.inner.publish()
    }
}

struct LifecycleProbe(SerialProductPublicationExecutor<ProbeSink>);

impl WorkImplementation for LifecycleProbe {
    type Error = SerialProductPublicationExecutionError<io::Error>;

    fn implementation_id(&self) -> &WorkImplementationId {
        self.0.implementation_id()
    }

    fn execute(&self, context: WorkExecutionContext<'_>) -> Result<WorkMeasurements, Self::Error> {
        assert!(self.0.take_completion().is_none());
        if context.node().id.as_str() == "product-generation-write" {
            assert!(matches!(
                self.0.publish(context),
                Err(SerialProductPublicationExecutionError::State)
            ));
            let result = if self.0.sink().generation == GenerationOutcome::Panic {
                assert!(
                    std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                        self.0.execute(context)
                    }))
                    .is_err()
                );
                Err(SerialProductPublicationExecutionError::State)
            } else {
                self.0.execute(context)
            };
            assert!(matches!(
                self.0.execute(context),
                Err(SerialProductPublicationExecutionError::State)
            ));
            assert!(self.0.take_completion().is_none());
            if self.0.sink().generation != GenerationOutcome::Success {
                assert!(result.is_err());
                assert_eq!(self.0.sink().begin_calls.load(Ordering::SeqCst), 1);
            }
            result
        } else {
            self.0.execute(context)
        }
    }

    fn failure_measurements<'a>(&'a self, error: &'a Self::Error) -> Option<&'a WorkMeasurements> {
        self.0.failure_measurements(error)
    }

    fn wait_for_fence(
        &self,
        context: WorkExecutionContext<'_>,
        fence: FenceKind,
    ) -> Result<WorkMeasurements, Self::Error> {
        self.0.wait_for_fence(context, fence)
    }

    fn complete_observation_read(
        &self,
        context: ObservationReadCompletionContext,
    ) -> Result<AttemptBoundObservationCompletion, Self::Error> {
        self.0.complete_observation_read(context)
    }

    fn publish(&self, context: WorkExecutionContext<'_>) -> Result<(), Self::Error> {
        let result = self.0.publish(context);
        assert!(matches!(
            self.0.publish(context),
            Err(SerialProductPublicationExecutionError::State)
        ));
        assert!(matches!(
            self.0.execute(context),
            Err(SerialProductPublicationExecutionError::State)
        ));
        assert_eq!(self.0.sink().inner.publish_calls.load(Ordering::SeqCst), 1);
        result
    }
}

#[test]
fn publication_lifecycle_rejects_duplicates_without_losing_completion() {
    let _guard = run_lock()
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    for (generation, fail) in [
        (GenerationOutcome::Success, false),
        (GenerationOutcome::Success, true),
        (GenerationOutcome::Failure, false),
        (GenerationOutcome::Panic, false),
    ] {
        let problem = compile(sealed_products_request(242)).unwrap();
        let (planned, scientific, demand) = pending_generation_for_problem(&problem);
        let sink = ProbeSink {
            inner: InMemoryProductSink {
                fail,
                ..InMemoryProductSink::default()
            },
            generation,
            begin_calls: AtomicUsize::new(0),
        };
        let planning_registry = ContractOnlyRegistry::new(
            registry(77),
            implementation_metadata(&problem),
            [implementation(77)],
        );
        let publication = SerialProductPublicationPlan::new(
            &problem,
            &planned,
            &demand,
            sink.residency(&planned, &demand).unwrap(),
            &planning_registry,
            SerialProductPublicationPolicy::new(
                implementation(77),
                serial_storage_io(),
                1_000,
                900_000,
            ),
        )
        .unwrap();
        let (physical, publication, window) = publication.into_parts();
        let executor = SerialProductPublicationExecutor::new(
            implementation(77),
            problem.clone(),
            publication,
            planned,
            scientific,
            None,
            sink,
            window,
        )
        .unwrap();
        let registry = SerialProductPublicationRegistry::new(
            registry(77),
            implementation(77),
            &problem,
            LifecycleProbe(executor),
        );
        let directory = tempfile::tempdir().unwrap();
        let receipts = ExecutionReceiptStore::new(
            directory.path(),
            ReceiptRetention::new(4, 1_048_576).unwrap(),
        )
        .unwrap();
        let plan = runtime_plan(
            &problem,
            PlanningBindings::new(
                registry.registry_id(),
                ResourcePolicy::Balanced,
                planning_profile(4),
            ),
            authority(),
            &registry,
            &receipts,
            move |_, _| Ok::<_, io::Error>(vec![physical]),
        )
        .unwrap();
        let current = RunBindings::new(
            problem.inputs().clone(),
            &ResourcePolicy::Balanced,
            cost_model(4),
        );
        let result = runtime_run(
            &ExecutableModelProblem::from_compiled(problem.clone()).unwrap(),
            &plan,
            &current,
            &registry,
            authority(),
            &mut RunToCompletion,
            receipts.bind(execution_provenance(
                casa_imaging_runtime::ExecutionAttemptId::from_sha256([78; 32]),
                BuildIdentity::from_sha256([79; 32]),
            )),
        );
        let failed = fail || generation != GenerationOutcome::Success;
        assert_eq!(result.is_err(), failed);
        if fail {
            assert_eq!(
                registry
                    .implementation()
                    .0
                    .sink()
                    .inner
                    .visible
                    .lock()
                    .unwrap()
                    .len(),
                1,
                "a failed publication keeps its already published prefix"
            );
        }
        let completion = registry.implementation().0.take_completion();
        assert_eq!(completion.is_none(), failed);
        if let Some(completion) = completion {
            let (planned, scientific, published) = completion.into_parts();
            assert_eq!(planned.problem_id(), problem.problem_id());
            assert_eq!(scientific.normal_state().problem_id(), problem.problem_id());
            assert_eq!(published.problem_id(), problem.problem_id());
        }
        assert!(registry.implementation().0.take_completion().is_none());
    }
}

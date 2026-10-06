// SPDX-License-Identifier: LGPL-3.0-or-later

//! Bounded progress and final-only execution receipts.

use super::*;
use casa_imaging_runtime::{ExecutionAttemptId, ReceiptError};

struct ObserveReservation {
    root: PathBuf,
    attempt: ExecutionAttemptId,
    observed: Option<(Vec<u8>, std::time::SystemTime)>,
    events: usize,
}

impl RunController for ObserveReservation {
    fn directive(&mut self, _: &ExecutionStatus) -> RunDirective {
        let active = self.root.join(format!("{}.active", self.attempt));
        let snapshot = (
            fs::read(&active).unwrap(),
            fs::metadata(active).unwrap().modified().unwrap(),
        );
        assert_eq!(
            snapshot.0.len(),
            8,
            "active marker is fixed-size, not an execution-plan snapshot"
        );
        assert!(
            !self
                .root
                .join(format!("{}.receipt.json", self.attempt))
                .exists()
        );
        if let Some(previous) = &self.observed {
            assert_eq!(
                &snapshot, previous,
                "routine progress must not rewrite persistent telemetry"
            );
        } else {
            self.observed = Some(snapshot);
        }
        self.events += 1;
        RunDirective::Continue
    }
}

#[test]
fn planning_and_retention_do_not_read_historical_receipt_contents() {
    let problem = compile(request(1)).unwrap();
    let directory = tempfile::tempdir().unwrap();
    let receipts =
        ExecutionReceiptStore::new(directory.path(), ReceiptRetention::new(1, 1 << 20).unwrap())
            .unwrap();
    let old = ExecutionAttemptId::from_sha256([101; 32]);
    let old_path = receipts.root_path().join(format!("{old}.receipt.json"));
    fs::write(&old_path, b"deliberately invalid historical document").unwrap();
    assert!(
        receipts.open(old).is_err(),
        "explicit reads still validate external persisted data"
    );
    let plan = plan_with_receipts(
        &problem,
        PlanningBindings::new(registry(3), ResourcePolicy::Balanced, planning_profile(4)),
        &receipts,
        |_, _| Ok::<_, ()>(physical_work(6)),
    )
    .expect("planning does not inspect unrelated receipt bodies");
    let attempt = ExecutionAttemptId::from_sha256([102; 32]);
    let mut observer = ObserveReservation {
        root: receipts.root_path().to_owned(),
        attempt,
        observed: None,
        events: 0,
    };
    let result = run_receipted(
        &problem,
        &plan,
        &RunBindings::new(
            problem.inputs().clone(),
            &ResourcePolicy::Balanced,
            cost_model(4),
        ),
        &test_registry(&problem, 3, 6, Some("adapter failed")),
        authority(),
        &mut observer,
        receipts.bind(execution_provenance(
            attempt,
            BuildIdentity::from_sha256([42; 32]),
        )),
    );
    assert!(matches!(result, Err(RunError::Execution { .. })));
    assert!(observer.events > 0);
    assert!(
        !old_path.exists(),
        "retention prunes by bounded filesystem metadata, not receipt content"
    );
    let marker = receipts.root_path().join(format!("{attempt}.active"));
    assert!(
        marker.exists(),
        "no fallible marker cleanup follows final commit"
    );
    let receipt = receipts.open(attempt).unwrap();
    assert_eq!(receipt.status(), ReceiptStatus::Failed);
    assert_eq!(receipt.failure_kind(), Some(ReceiptFailureKind::Adapter));
    let reopened =
        ExecutionReceiptStore::new(directory.path(), ReceiptRetention::new(1, 1 << 20).unwrap())
            .unwrap();
    assert_eq!(
        reopened.open(attempt).unwrap().status(),
        ReceiptStatus::Failed
    );
    for seed in [102, 103] {
        let result = run_receipted(
            &problem,
            &plan,
            &RunBindings::new(
                problem.inputs().clone(),
                &ResourcePolicy::Balanced,
                cost_model(4),
            ),
            &test_registry(&problem, 3, 6, Some("adapter failed")),
            authority(),
            &mut RunToCompletion,
            reopened.bind(execution_provenance(
                ExecutionAttemptId::from_sha256([seed; 32]),
                BuildIdentity::from_sha256([42; 32]),
            )),
        );
        if seed == 102 {
            assert!(matches!(
                result,
                Err(RunError::Receipt(ReceiptError::AttemptAlreadyExists))
            ));
        } else {
            assert!(matches!(result, Err(RunError::Execution { .. })));
            assert!(
                !marker.exists(),
                "eviction removes terminal receipt and matching marker together"
            );
            assert!(
                !receipts
                    .root_path()
                    .join(format!("{attempt}.receipt.json"))
                    .exists()
            );
        }
    }
}

struct BlockFinalReceipt(PathBuf);

impl RunController for BlockFinalReceipt {
    fn directive(&mut self, _: &ExecutionStatus) -> RunDirective {
        fs::create_dir_all(&self.0).unwrap();
        RunDirective::Continue
    }
}

#[test]
fn final_persistence_failure_preserves_reservation_and_reports_io_error() {
    let problem = compile(request(1)).unwrap();
    let directory = tempfile::tempdir().unwrap();
    let retention = ReceiptRetention::new(1, 1 << 20).unwrap();
    let store = ExecutionReceiptStore::new(directory.path(), retention).unwrap();
    let plan = plan_with_receipts(
        &problem,
        PlanningBindings::new(registry(3), ResourcePolicy::Balanced, planning_profile(4)),
        &store,
        |_, _| Ok::<_, ()>(physical_work(6)),
    )
    .unwrap();
    let attempt = ExecutionAttemptId::from_sha256([104; 32]);
    let path = store.root_path().join(format!("{attempt}.receipt.json"));
    let result = run_receipted(
        &problem,
        &plan,
        &RunBindings::new(
            problem.inputs().clone(),
            &ResourcePolicy::Balanced,
            cost_model(4),
        ),
        &test_registry(&problem, 3, 6, Some("adapter failed")),
        authority(),
        &mut BlockFinalReceipt(path),
        store.bind(execution_provenance(
            attempt,
            BuildIdentity::from_sha256([42; 32]),
        )),
    );
    assert!(matches!(
        result,
        Err(RunError::Receipt(ReceiptError::Io { .. }))
    ));
    let reopened = ExecutionReceiptStore::new(directory.path(), retention).unwrap();
    assert!(
        reopened.open(attempt).is_err(),
        "failed persistence does not supply a usable success summary"
    );
    assert_eq!(
        fs::read(store.root_path().join(format!("{attempt}.active")))
            .unwrap()
            .len(),
        8
    );
}

#[test]
fn reopening_store_preserves_active_attempt_exclusivity_and_capacity() {
    let problem = compile(request(1)).unwrap();
    let directory = tempfile::tempdir().unwrap();
    let retention = ReceiptRetention::new(1, 1 << 20).unwrap();
    let store = ExecutionReceiptStore::new(directory.path(), retention).unwrap();
    let active_attempt = ExecutionAttemptId::from_sha256([101; 32]);
    let marker = store.root_path().join(format!("{active_attempt}.active"));
    fs::write(&marker, (1_u64 << 20).to_le_bytes()).unwrap();
    let reopened = ExecutionReceiptStore::new(directory.path(), retention).unwrap();
    assert!(
        marker.exists(),
        "opening a store must not erase a live or interrupted reservation"
    );
    let plan = plan_with_receipts(
        &problem,
        PlanningBindings::new(registry(3), ResourcePolicy::Balanced, planning_profile(4)),
        &reopened,
        |_, _| Ok::<_, ()>(physical_work(6)),
    )
    .unwrap();
    for seed in [101, 102] {
        let result = run_receipted(
            &problem,
            &plan,
            &RunBindings::new(
                problem.inputs().clone(),
                &ResourcePolicy::Balanced,
                cost_model(4),
            ),
            &test_registry(&problem, 3, 6, Some("must not execute")),
            authority(),
            &mut RunToCompletion,
            reopened.bind(execution_provenance(
                ExecutionAttemptId::from_sha256([seed; 32]),
                BuildIdentity::from_sha256([42; 32]),
            )),
        );
        if seed == 101 {
            assert!(matches!(
                result,
                Err(RunError::Receipt(ReceiptError::AttemptAlreadyExists))
            ));
        } else {
            assert!(matches!(
                result,
                Err(RunError::Receipt(ReceiptError::RetentionExceeded))
            ));
        }
        assert_eq!(fs::read(&marker).unwrap(), (1_u64 << 20).to_le_bytes());
    }
}

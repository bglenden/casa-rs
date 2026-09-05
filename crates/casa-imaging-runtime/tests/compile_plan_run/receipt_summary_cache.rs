// SPDX-License-Identifier: LGPL-3.0-or-later

//! Receipt-summary consumers must observe the same validated durable evidence.

use super::*;
use casa_imaging_runtime::{ExecutionAttemptId, ExecutionPlan, ReceiptError};

fn terminal_attempt(
    problem: &casa_imaging_model::CompiledProblem,
    plan: &ExecutionPlan,
    seed: u8,
) -> Result<ExecutionReceipt, RunError<io::Error>> {
    let result = run_receipted(
        problem,
        plan,
        &RunBindings::new(
            problem.inputs().clone(),
            &ResourcePolicy::Balanced,
            cost_model(4),
        ),
        &test_registry(problem, 3, 6, Some("adapter failed")),
        authority(),
        &mut RunToCompletion,
        plan.receipt_store().bind(execution_provenance(
            ExecutionAttemptId::from_sha256([seed; 32]),
            BuildIdentity::from_sha256([42; 32]),
        )),
    );
    match result {
        Err(RunError::Execution { node, source }) => {
            assert_eq!(node, WorkNodeId::new("transaction-check"));
            assert_eq!(source.to_string(), "adapter failed");
        }
        Err(error) => return Err(error),
        Ok(outcome) => panic!("expected the intentional adapter failure, got {outcome:?}"),
    }
    let receipt = plan
        .receipt_store()
        .open(ExecutionAttemptId::from_sha256([seed; 32]))
        .unwrap();
    assert_eq!(receipt.status(), ReceiptStatus::Failed);
    assert_eq!(receipt.failure_kind(), Some(ReceiptFailureKind::Adapter));
    Ok(receipt)
}

fn receipt_path(store: &ExecutionReceiptStore, seed: u8) -> PathBuf {
    store.root_path().join(format!(
        "{}.receipt.json",
        ExecutionAttemptId::from_sha256([seed; 32])
    ))
}

fn set_finished_millis(path: &Path, millis: u64) {
    let mut document = fs::read_to_string(path).expect("real persisted receipt");
    let original_len = document.len();
    let marker = "\"finished_unix_millis\": ";
    let start = document.find(marker).expect("terminal timestamp") + marker.len();
    let count = document[start..]
        .bytes()
        .take_while(u8::is_ascii_digit)
        .count();
    assert!(count > 0);
    document.replace_range(start..start + count, &millis.to_string());
    let updated = with_current_payload_checksum(document);
    assert_eq!(updated.len(), original_len, "same-length valid replacement");
    fs::write(path, updated).expect("replace fixture receipt");
}

#[test]
fn warmed_receipt_summary_rejects_same_length_corruption_with_restored_mtime() {
    let problem = compile(request(1)).expect("compiled fixture");
    let directory = tempfile::tempdir().unwrap();
    let receipts =
        ExecutionReceiptStore::new(directory.path(), ReceiptRetention::new(2, 1 << 20).unwrap())
            .unwrap();
    let planning = || {
        plan_with_receipts(
            &problem,
            PlanningBindings::new(registry(3), ResourcePolicy::Balanced, planning_profile(4)),
            &receipts,
            |_, _| Ok::<_, ()>(physical_work(6)),
        )
    };
    let plan = planning().unwrap();
    terminal_attempt(&problem, &plan, 101).unwrap();
    planning().expect("warm validated receipt summaries");
    let path = receipt_path(&receipts, 101);
    let original = fs::read(&path).unwrap();
    let original_modified = fs::metadata(&path).unwrap().modified().unwrap();
    let mut corrupt = original.clone();
    let marker = b"\"payload_sha256\": \"";
    let index = corrupt
        .windows(marker.len())
        .position(|part| part == marker)
        .unwrap()
        + marker.len();
    corrupt[index] = if corrupt[index] == b'0' { b'1' } else { b'0' };
    fs::write(&path, &corrupt).unwrap();
    fs::File::options()
        .write(true)
        .open(&path)
        .unwrap()
        .set_modified(original_modified)
        .unwrap();
    assert_eq!(fs::metadata(&path).unwrap().len(), original.len() as u64);
    assert_eq!(
        fs::metadata(&path).unwrap().modified().unwrap(),
        original_modified
    );
    assert!(matches!(
        planning(),
        Err(PlanError::Receipt(ReceiptError::IntegrityMismatch))
    ));
    fs::write(&path, original).unwrap();
    planning().expect("a restored valid receipt is decoded again");
}

#[test]
fn same_length_valid_replacement_updates_retention_order_across_shared_root_handles() {
    let problem = compile(request(1)).unwrap();
    let directory = tempfile::tempdir().unwrap();
    let retention = ReceiptRetention::new(2, 1 << 20).unwrap();
    let receipts = ExecutionReceiptStore::new(directory.path(), retention).unwrap();
    let second_handle = ExecutionReceiptStore::new(directory.path().join("."), retention).unwrap();
    assert_eq!(receipts, second_handle);
    let planning = || {
        plan_with_receipts(
            &problem,
            PlanningBindings::new(registry(3), ResourcePolicy::Balanced, planning_profile(4)),
            &second_handle,
            |_, _| Ok::<_, ()>(physical_work(6)),
        )
    };
    let plan = planning().unwrap();
    terminal_attempt(&problem, &plan, 101).unwrap();
    terminal_attempt(&problem, &plan, 102).unwrap();
    let first = receipt_path(&receipts, 101);
    let second = receipt_path(&receipts, 102);
    set_finished_millis(&first, 4_000_000_000_001);
    set_finished_millis(&second, 4_000_000_000_002);
    planning().expect("warm both terminal summaries");
    let modified = fs::metadata(&first).unwrap().modified().unwrap();
    set_finished_millis(&first, 4_000_000_000_003);
    fs::File::options()
        .write(true)
        .open(&first)
        .unwrap()
        .set_modified(modified)
        .unwrap();
    assert_eq!(
        receipts
            .open(ExecutionAttemptId::from_sha256([101; 32]))
            .unwrap()
            .status(),
        ReceiptStatus::Failed
    );
    terminal_attempt(&problem, &plan, 103).expect("admit by the current, not cached, timestamps");
    assert!(first.exists(), "valid replacement is now newer");
    assert!(
        !second.exists(),
        "the actually oldest terminal receipt is evicted"
    );
    assert!(receipt_path(&receipts, 103).exists());
}

#[test]
fn warmed_receipt_summary_observes_deleted_and_externally_added_files() {
    let problem = compile(request(1)).unwrap();
    let directory = tempfile::tempdir().unwrap();
    let receipts =
        ExecutionReceiptStore::new(directory.path(), ReceiptRetention::new(1, 1 << 20).unwrap())
            .unwrap();
    let planning = || {
        plan_with_receipts(
            &problem,
            PlanningBindings::new(registry(3), ResourcePolicy::Balanced, planning_profile(4)),
            &receipts,
            |_, _| Ok::<_, ()>(physical_work(6)),
        )
    };
    let plan = planning().unwrap();
    terminal_attempt(&problem, &plan, 101).unwrap();
    let first = receipt_path(&receipts, 101);
    let original = fs::read(&first).unwrap();
    planning().expect("warm occupied directory");
    fs::remove_file(&first).unwrap();
    planning().expect("a deleted receipt is not stale evidence");
    fs::write(&first, original).unwrap();
    terminal_attempt(&problem, &plan, 102)
        .expect("new directory entry is included in retention admission");
    assert!(!first.exists());
    assert!(receipt_path(&receipts, 102).exists());
    assert_eq!(fs::read_dir(receipts.root_path()).unwrap().count(), 1);
}

struct CaptureRunningReceipt {
    path: PathBuf,
    bytes: Option<Vec<u8>>,
}

impl RunController for CaptureRunningReceipt {
    fn directive(&mut self, _: &ExecutionStatus) -> RunDirective {
        if self.bytes.is_none() {
            self.bytes = Some(fs::read(&self.path).expect("durable running receipt"));
        }
        RunDirective::Continue
    }
}

#[test]
fn warmed_nonterminal_summary_protects_active_receipt_then_allows_terminal_eviction() {
    let problem = compile(request(1)).unwrap();
    let directory = tempfile::tempdir().unwrap();
    let receipts =
        ExecutionReceiptStore::new(directory.path(), ReceiptRetention::new(1, 1 << 20).unwrap())
            .unwrap();
    let planning = || {
        plan_with_receipts(
            &problem,
            PlanningBindings::new(registry(3), ResourcePolicy::Balanced, planning_profile(4)),
            &receipts,
            |_, _| Ok::<_, ()>(physical_work(6)),
        )
    };
    let plan = planning().unwrap();
    let path = receipt_path(&receipts, 101);
    let mut capture = CaptureRunningReceipt {
        path: path.clone(),
        bytes: None,
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
        &mut capture,
        receipts.bind(execution_provenance(
            ExecutionAttemptId::from_sha256([101; 32]),
            BuildIdentity::from_sha256([42; 32]),
        )),
    );
    assert!(matches!(result, Err(RunError::Execution { node, source })
        if node == WorkNodeId::new("transaction-check") && source.to_string() == "adapter failed"));
    assert_eq!(
        receipts
            .open(ExecutionAttemptId::from_sha256([101; 32]))
            .unwrap()
            .status(),
        ReceiptStatus::Failed
    );
    let terminal = fs::read(&path).unwrap();
    fs::write(&path, capture.bytes.expect("actual running checkpoint")).unwrap();
    assert_eq!(
        receipts
            .open(ExecutionAttemptId::from_sha256([101; 32]))
            .unwrap()
            .status(),
        ReceiptStatus::Running
    );
    planning().expect("warm active summary");
    assert!(matches!(
        terminal_attempt(&problem, &plan, 102),
        Err(RunError::Receipt(ReceiptError::RetentionExceeded))
    ));
    assert!(path.exists(), "active evidence must not be evicted");
    fs::write(&path, terminal).unwrap();
    terminal_attempt(&problem, &plan, 102)
        .expect("terminal replacement releases the retention slot");
    assert!(!path.exists());
    assert_eq!(
        receipts
            .open(ExecutionAttemptId::from_sha256([102; 32]))
            .unwrap()
            .status(),
        ReceiptStatus::Failed
    );
}

#[test]
fn warmed_summary_preserves_public_open_exact_attempt_string_check() {
    let problem = compile(request(1)).unwrap();
    let directory = tempfile::tempdir().unwrap();
    let receipts =
        ExecutionReceiptStore::new(directory.path(), ReceiptRetention::new(2, 1 << 20).unwrap())
            .unwrap();
    let planning = || {
        plan_with_receipts(
            &problem,
            PlanningBindings::new(registry(3), ResourcePolicy::Balanced, planning_profile(4)),
            &receipts,
            |_, _| Ok::<_, ()>(physical_work(6)),
        )
    };
    let plan = planning().unwrap();
    terminal_attempt(&problem, &plan, 0xab).unwrap();
    planning().expect("warm canonical attempt summary");
    let attempt = ExecutionAttemptId::from_sha256([0xab; 32]);
    let path = receipt_path(&receipts, 0xab);
    let document = fs::read_to_string(&path).unwrap();
    let canonical = format!("\"attempt_identity\": \"{attempt}\"");
    let uppercase = format!(
        "\"attempt_identity\": \"{}\"",
        attempt.to_string().to_ascii_uppercase()
    );
    assert!(document.contains(&canonical));
    let replaced = with_current_payload_checksum(document.replacen(&canonical, &uppercase, 1));
    assert_eq!(replaced.len(), document.len());
    fs::write(&path, replaced).unwrap();
    assert!(matches!(
        receipts.open(attempt),
        Err(ReceiptError::AttemptMismatch)
    ));
    assert!(matches!(
        planning(),
        Err(PlanError::Receipt(ReceiptError::AttemptMismatch))
    ));
}

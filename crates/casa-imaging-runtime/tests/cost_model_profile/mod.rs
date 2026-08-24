// SPDX-License-Identifier: LGPL-3.0-or-later

//! Focused T16 evidence for explicit reviewed Planner Cost Model Profile
//! promotion: completed comparable receipts only, versioned auditable output,
//! and no silent training from successful or failed runs.

use std::path::Path;

use casa_imaging_model::{ModelColumnWrite, ProductKind, compile};
use casa_imaging_runtime::{
    ExecutionAttemptId, ExecutionPlan, open_cost_model_profile, promote_cost_model_profile,
};

mod support;

use self::support::{
    BuildIdentity, ExecutionOutcome, ExecutionProvenance, ExecutionReceiptStore, PlanningBindings,
    ProfilePromotionError, ProfileReview, ReceiptRetention, ReceiptStatus, ResourcePolicy,
    RunBindings, RunToCompletion, authority, cost_model, execution_provenance, geometry,
    physical_work_for_problem, plan, registry, run_receipted, test_registry,
};
use support::request_with_products_and_model;

struct Skeleton {
    problem: casa_imaging_model::CompiledProblem,
    plan: ExecutionPlan,
    current: RunBindings,
}

fn skeleton(lineage: u8) -> Skeleton {
    let problem = compile(request_with_products_and_model(
        15,
        geometry(255.0),
        vec![ProductKind::Psf, ProductKind::Residual, ProductKind::Model],
        ModelColumnWrite::SelectedRows,
    ))
    .expect("private synthetic logical compilation");
    let plan = plan(
        &problem,
        PlanningBindings::new(registry(3), ResourcePolicy::Balanced, cost_model(lineage)),
        |problem, _| Ok::<_, std::io::Error>(physical_work_for_problem(problem, 6)),
    )
    .expect("Resource Authority-backed physical planning");
    let current = RunBindings::new(
        problem.inputs().clone(),
        &ResourcePolicy::Balanced,
        cost_model(lineage),
    );
    Skeleton {
        problem,
        plan,
        current,
    }
}

fn receipt_store(root: &Path) -> ExecutionReceiptStore {
    ExecutionReceiptStore::new(
        root,
        ReceiptRetention::new(8, 4_194_304).expect("retention"),
    )
    .expect("receipt store")
}

fn provenance(seed: u8) -> ExecutionProvenance {
    // Comparable evidence shares one executable build identity.
    execution_provenance(
        ExecutionAttemptId::from_sha256([seed; 32]),
        BuildIdentity::from_sha256([249; 32]),
    )
}

/// Execute the skeleton twice into one store and return both attempt ids.
fn two_completed_receipts(
    root: &std::path::Path,
    lineage: u8,
) -> (
    ExecutionReceiptStore,
    [ExecutionAttemptId; 2],
    ExecutionPlan,
) {
    let skeleton = skeleton(lineage);
    let receipts = receipt_store(root);
    let mut completion = RunToCompletion;
    for seed in [61u8, 62] {
        let outcome = run_receipted(
            &skeleton.problem,
            &skeleton.plan,
            &skeleton.current,
            &test_registry(3, 6, None),
            authority(),
            &mut completion,
            receipts.bind(provenance(seed)),
        )
        .expect("completed synthetic run");
        assert_eq!(outcome, ExecutionOutcome::Succeeded);
    }
    let attempts = [
        ExecutionAttemptId::from_sha256([61; 32]),
        ExecutionAttemptId::from_sha256([62; 32]),
    ];
    (receipts, attempts, skeleton.plan)
}

fn review() -> ProfileReview {
    ProfileReview::new(
        "imaging-operator",
        "reviewed comparable MFS calibration evidence",
    )
    .expect("complete review evidence")
}

#[test]
fn explicit_promotion_produces_a_versioned_auditable_profile() {
    let directory = tempfile::tempdir().expect("promotion root");
    let profiles = directory.path().join("profiles");
    let (receipts, attempts, sealed_plan) = two_completed_receipts(directory.path(), 4);

    let record =
        promote_cost_model_profile(&profiles, &receipts, &attempts, review(), 1_700_000_000_000)
            .expect("explicit reviewed promotion");

    assert_eq!(record.lineage_cost_model(), cost_model(4));
    assert_eq!(record.promoted_unix_millis(), 1_700_000_000_000);
    assert_eq!(record.review(), &review());
    assert!(!record.entries().is_empty());
    // Every calibration entry mirrors the sealed plan's own stage predictions
    // and cites exactly one reviewed receipt attempt.
    for entry in record.entries() {
        assert!(attempts.contains(&entry.attempt()));
        let stage = sealed_plan
            .prediction()
            .stages()
            .get(entry.node())
            .expect("entry node belongs to the comparable plan");
        assert_eq!(stage.elapsed_nanos(), entry.predicted_nanos());
    }

    // The persisted document round-trips integrity-checked by content identity.
    let reopened =
        open_cost_model_profile(&profiles, record.profile_id()).expect("reopen promoted profile");
    assert_eq!(reopened, record);

    // Identical reviewed evidence yields one identical versioned identity,
    // independent of where it is persisted or when the command re-runs.
    let replayed = promote_cost_model_profile(
        directory.path().join("profiles-mirror"),
        &receipts,
        &attempts,
        review(),
        1_750_000_000_000,
    )
    .expect("deterministic re-promotion");
    assert_eq!(replayed.profile_id(), record.profile_id());

    // Profiles are immutable audit artifacts: a repeat command cannot overwrite.
    let repeated =
        promote_cost_model_profile(&profiles, &receipts, &attempts, review(), 1_800_000_000_000);
    assert!(matches!(
        repeated,
        Err(ProfilePromotionError::AlreadyPromoted { profile }) if profile == record.profile_id()
    ));
}

#[test]
fn profile_identity_is_independent_of_reviewer_order_and_duplicates_are_refused() {
    let directory = tempfile::tempdir().expect("order-independence root");
    let profiles = directory.path().join("profiles");
    let (receipts, attempts, _) = two_completed_receipts(directory.path(), 4);

    let ascending =
        promote_cost_model_profile(&profiles, &receipts, &attempts, review(), 1_700_000_000_000)
            .expect("explicit reviewed promotion");
    let mut descending = attempts;
    descending.reverse();
    let reordered = promote_cost_model_profile(
        directory.path().join("profiles-mirror"),
        &receipts,
        &descending,
        review(),
        1_700_000_000_000,
    )
    .expect("reordered promotion of the same evidence set");

    assert_eq!(
        ascending.profile_id(),
        reordered.profile_id(),
        "the profile identity is a function of the reviewed evidence set"
    );

    let duplicated = promote_cost_model_profile(
        &profiles,
        &receipts,
        &[attempts[0], attempts[0]],
        review(),
        1,
    );
    assert!(matches!(
        duplicated,
        Err(ProfilePromotionError::DuplicateEvidence { .. })
    ));
}

#[test]
fn divergent_build_identities_are_not_comparable_evidence() {
    let directory = tempfile::tempdir().expect("build-divergence root");
    let profiles = directory.path().join("profiles");
    let skeleton = skeleton(4);
    let receipts = receipt_store(directory.path());
    let mut completion = RunToCompletion;
    for seed in [61u8, 62] {
        run_receipted(
            &skeleton.problem,
            &skeleton.plan,
            &skeleton.current,
            &test_registry(3, 6, None),
            authority(),
            &mut completion,
            receipts.bind(provenance(seed)),
        )
        .expect("completed synthetic run");
    }
    // One more completed run from a different executable build.
    let rebuilt = execution_provenance(
        ExecutionAttemptId::from_sha256([71; 32]),
        BuildIdentity::from_sha256([42; 32]),
    );
    run_receipted(
        &skeleton.problem,
        &skeleton.plan,
        &skeleton.current,
        &test_registry(3, 6, None),
        authority(),
        &mut completion,
        receipts.bind(rebuilt),
    )
    .expect("completed rebuilt-binary run");

    let error = promote_cost_model_profile(
        &profiles,
        &receipts,
        &[
            ExecutionAttemptId::from_sha256([61; 32]),
            ExecutionAttemptId::from_sha256([71; 32]),
        ],
        review(),
        1_700_000_000_000,
    )
    .expect_err("evidence from different builds is not comparable");
    assert!(matches!(
        error,
        ProfilePromotionError::NotComparable {
            field: "build identity",
            ..
        }
    ));
    assert!(!profiles.exists());
}

#[test]
fn opening_a_profile_rejects_a_document_stored_under_another_identity() {
    let directory = tempfile::tempdir().expect("identity-mismatch root");
    let profiles = directory.path().join("profiles");
    let (receipts, attempts, _) = two_completed_receipts(directory.path(), 4);
    let record =
        promote_cost_model_profile(&profiles, &receipts, &attempts, review(), 1_700_000_000_000)
            .expect("explicit reviewed promotion");

    // A second self-consistent profile document with different reviewed scope.
    let other_review = ProfileReview::new(
        "imaging-operator",
        "reviewed comparable MFS calibration evidence, second wave",
    )
    .expect("complete review evidence");
    let other = promote_cost_model_profile(
        &profiles,
        &receipts,
        &attempts,
        other_review,
        1_700_000_001_000,
    )
    .expect("second reviewed promotion");
    assert_ne!(other.profile_id(), record.profile_id());

    std::fs::write(
        profiles.join(format!("{}.json", other.profile_id())),
        std::fs::read(profiles.join(format!("{}.json", record.profile_id())))
            .expect("first profile document"),
    )
    .expect("store first document under second identity");

    assert!(matches!(
        open_cost_model_profile(&profiles, other.profile_id()),
        Err(ProfilePromotionError::CorruptProfile { .. })
    ));
}

#[test]
fn non_completed_receipts_never_promote_a_profile() {
    let directory = tempfile::tempdir().expect("failed-run promotion root");
    let profiles = directory.path().join("profiles");
    let skeleton = skeleton(4);
    let receipts = receipt_store(directory.path());

    // One completed receipt...
    let mut completion = RunToCompletion;
    let outcome = run_receipted(
        &skeleton.problem,
        &skeleton.plan,
        &skeleton.current,
        &test_registry(3, 6, None),
        authority(),
        &mut completion,
        receipts.bind(provenance(61)),
    )
    .expect("completed synthetic run");
    assert_eq!(outcome, ExecutionOutcome::Succeeded);

    // ...and one failed execution receipt retained as failure evidence only.
    let mut failing = RunToCompletion;
    run_receipted(
        &skeleton.problem,
        &skeleton.plan,
        &skeleton.current,
        &test_registry(3, 6, Some("execute failed")),
        authority(),
        &mut failing,
        receipts.bind(provenance(63)),
    )
    .expect_err("failing synthetic run");
    let failed_attempt = ExecutionAttemptId::from_sha256([63; 32]);
    assert_eq!(
        receipts
            .open(failed_attempt)
            .expect("failed receipt")
            .status(),
        ReceiptStatus::Failed
    );

    let error = promote_cost_model_profile(
        &profiles,
        &receipts,
        &[ExecutionAttemptId::from_sha256([61; 32]), failed_attempt],
        review(),
        1_700_000_000_000,
    )
    .expect_err("failed executions never train planner behavior");
    assert!(matches!(
        error,
        ProfilePromotionError::NotCompleted {
            status: ReceiptStatus::Failed,
            ..
        }
    ));
    assert!(
        !profiles.exists(),
        "refused promotions must not leave profile documents behind"
    );

    // A failed receipt alone is refused identically.
    let alone = promote_cost_model_profile(&profiles, &receipts, &[failed_attempt], review(), 1);
    assert!(matches!(
        alone,
        Err(ProfilePromotionError::NotCompleted { .. })
    ));
}

#[test]
fn incomparable_completed_receipts_are_refused() {
    let directory = tempfile::tempdir().expect("incomparable promotion root");
    let profiles = directory.path().join("profiles");
    let (receipts, first, _) = two_completed_receipts(directory.path(), 4);

    // A second completed pair planned under a different cost-model lineage.
    let other = skeleton(5);
    let mut completion = RunToCompletion;
    let outcome = run_receipted(
        &other.problem,
        &other.plan,
        &other.current,
        &test_registry(3, 6, None),
        authority(),
        &mut completion,
        receipts.bind(provenance(71)),
    )
    .expect("completed divergent-lineage run");

    assert_eq!(outcome, ExecutionOutcome::Succeeded);
    let error = promote_cost_model_profile(
        &profiles,
        &receipts,
        &[first[0], ExecutionAttemptId::from_sha256([71; 32])],
        review(),
        1_700_000_000_000,
    )
    .expect_err("divergent lineage cost-model evidence is not comparable");
    assert!(matches!(
        error,
        ProfilePromotionError::NotComparable {
            field: "effective plan",
            ..
        }
    ));
    assert!(!profiles.exists());
}

#[test]
fn tampered_profile_documents_fail_integrity_validation() {
    let directory = tempfile::tempdir().expect("tamper root");
    let profiles = directory.path().join("profiles");
    let (receipts, attempts, _) = two_completed_receipts(directory.path(), 4);
    let record =
        promote_cost_model_profile(&profiles, &receipts, &attempts, review(), 1_700_000_000_000)
            .expect("explicit reviewed promotion");

    let path = profiles.join(format!("{}.json", record.profile_id()));
    let bytes = std::fs::read(&path).expect("profile document");
    let marker = b"payload_sha256";
    let position = bytes
        .windows(marker.len())
        .position(|window| window == marker)
        .expect("checksum field marker");
    let mut tampered = bytes.clone();
    tampered[position + marker.len() + 4] ^= 0x01;
    std::fs::write(&path, tampered).expect("tamper with profile document");

    assert!(matches!(
        open_cost_model_profile(&profiles, record.profile_id()),
        Err(ProfilePromotionError::CorruptProfile { .. })
    ));

    // Unknown identities are refused without touching the store.
    assert!(matches!(
        open_cost_model_profile(&profiles, cost_model(9)),
        Err(ProfilePromotionError::UnknownProfile { .. })
    ));
}

#[test]
fn completed_runs_alone_create_no_profiles_and_empty_evidence_is_refused() {
    let directory = tempfile::tempdir().expect("silent-training root");
    let profiles = directory.path().join("profiles");
    let (_receipts, _attempts, _) = two_completed_receipts(directory.path(), 4);

    assert!(
        !profiles.exists(),
        "successful runs never silently train future plans"
    );

    let receipts = receipt_store(directory.path());
    assert!(matches!(
        promote_cost_model_profile(&profiles, &receipts, &[], review(), 1),
        Err(ProfilePromotionError::EmptyEvidence)
    ));
    assert!(matches!(
        ProfileReview::new("", ""),
        Err(ProfilePromotionError::InvalidReview)
    ));
}

// SPDX-License-Identifier: LGPL-3.0-or-later

use std::fs;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Barrier};
use std::thread;
use std::time::Duration;

use casa_provider_contracts::{ParameterValue, builtin_surface_bundle};
use casa_task_runtime::{
    ManagedProfileKind, ManagedStateStore, ParameterSession, SessionLastCoordinator,
    TaskLastCoordinator,
};

fn task_session(vis: &str, comment: &str) -> ParameterSession {
    let mut session =
        ParameterSession::defaults(builtin_surface_bundle("flagmanager").unwrap()).unwrap();
    session
        .set("vis", ParameterValue::String(vis.to_string()))
        .unwrap();
    session
        .set("comment", ParameterValue::String(comment.to_string()))
        .unwrap();
    session
}

fn image_session(path: &str) -> ParameterSession {
    let mut session =
        ParameterSession::defaults(builtin_surface_bundle("imexplore").unwrap()).unwrap();
    session
        .set("image", ParameterValue::String(path.to_string()))
        .unwrap();
    session
}

fn read_slot(store: &ManagedStateStore, surface: &str, kind: ManagedProfileKind) -> Option<String> {
    store.read(surface, kind).unwrap()
}

#[test]
fn successful_task_promotes_exact_attempted_snapshot() {
    let workspace = tempfile::tempdir().unwrap();
    let store = ManagedStateStore::for_workspace(workspace.path());
    let mut session = task_session("attempt.ms", "attempted");
    let attempted = session.render_sparse().unwrap();
    let lifecycle = TaskLastCoordinator::new();

    lifecycle
        .before_execution("attempt", workspace.path(), "flagmanager", true, &session)
        .unwrap();
    assert!(lifecycle.take_warnings().is_empty());
    assert_eq!(
        read_slot(&store, "flagmanager", ManagedProfileKind::Last),
        Some(attempted.clone())
    );
    assert_eq!(
        read_slot(&store, "flagmanager", ManagedProfileKind::LastSuccessful),
        None
    );

    // Completion promotes the validated intent captured before execution, not
    // later edits to a UI draft that shares the same ParameterSession.
    session
        .set("comment", ParameterValue::String("edited later".into()))
        .unwrap();
    lifecycle.after_completion("attempt", true);
    assert!(lifecycle.take_warnings().is_empty());
    assert_eq!(
        read_slot(&store, "flagmanager", ManagedProfileKind::LastSuccessful),
        Some(attempted)
    );

    // Promotion consumes the attempt and cannot accidentally run twice.
    lifecycle.after_completion("attempt", true);
    assert!(lifecycle.take_warnings().is_empty());
}

#[test]
fn failed_and_cancelled_tasks_keep_attempted_last_without_promoting() {
    let workspace = tempfile::tempdir().unwrap();
    let store = ManagedStateStore::for_workspace(workspace.path());
    let prior_success = task_session("prior.ms", "prior success")
        .render_sparse()
        .unwrap();
    store
        .write(
            "flagmanager",
            ManagedProfileKind::LastSuccessful,
            &prior_success,
        )
        .unwrap();

    let failed = task_session("failed.ms", "failed attempt");
    let failed_snapshot = failed.render_sparse().unwrap();
    let failure = TaskLastCoordinator::new();
    failure
        .before_execution("failure", workspace.path(), "flagmanager", true, &failed)
        .unwrap();
    failure.after_completion("failure", false);
    assert!(failure.take_warnings().is_empty());
    assert_eq!(
        read_slot(&store, "flagmanager", ManagedProfileKind::Last),
        Some(failed_snapshot)
    );
    assert_eq!(
        read_slot(&store, "flagmanager", ManagedProfileKind::LastSuccessful),
        Some(prior_success.clone())
    );

    let cancelled = task_session("cancelled.ms", "cancelled attempt");
    let cancelled_snapshot = cancelled.render_sparse().unwrap();
    {
        let cancellation = TaskLastCoordinator::new();
        cancellation
            .before_execution(
                "cancellation",
                workspace.path(),
                "flagmanager",
                true,
                &cancelled,
            )
            .unwrap();
        // Dropping without after_completion models cancellation or process
        // termination after launch: attempted Last remains, with no promotion.
    }
    assert_eq!(
        read_slot(&store, "flagmanager", ManagedProfileKind::Last),
        Some(cancelled_snapshot)
    );
    assert_eq!(
        read_slot(&store, "flagmanager", ManagedProfileKind::LastSuccessful),
        Some(prior_success)
    );
}

#[test]
fn failed_session_open_and_preopen_changes_preserve_last() {
    let workspace = tempfile::tempdir().unwrap();
    let store = ManagedStateStore::for_workspace(workspace.path());
    let prior = image_session("prior.image").render_sparse().unwrap();
    store
        .write("imexplore", ManagedProfileKind::Last, &prior)
        .unwrap();

    let lifecycle = SessionLastCoordinator::new(Duration::from_secs(60));

    // A valid launch whose backend fails never calls opened(). Pre-open UI
    // changes are therefore ignored by the lifecycle.
    let mut backend_rejected = image_session("failed.image");
    backend_rejected
        .set("view", ParameterValue::String("spectrum".into()))
        .unwrap();
    assert!(lifecycle.take_warnings().is_empty());

    // Validation failure in the success callback likewise cannot replace the
    // prior complete Last profile or mark the lifecycle opened.
    let incomplete =
        ParameterSession::defaults(builtin_surface_bundle("imexplore").unwrap()).unwrap();
    assert!(
        lifecycle
            .opened(workspace.path(), "imexplore", true, &incomplete)
            .is_err()
    );
    assert!(
        lifecycle
            .accepted_durable_change(workspace.path(), "imexplore", true, &incomplete)
            .is_err()
    );
    lifecycle.flush_all();
    assert_eq!(
        read_slot(&store, "imexplore", ManagedProfileKind::Last),
        Some(prior)
    );
}

#[test]
fn session_open_debounces_latest_accepted_durable_state_and_flushes_on_close() {
    let workspace = tempfile::tempdir().unwrap();
    let store = ManagedStateStore::for_workspace(workspace.path());
    let mut session = image_session("current.image");
    let opened_snapshot = session.render_sparse().unwrap();
    let lifecycle = SessionLastCoordinator::new(Duration::from_secs(60));

    lifecycle
        .opened(workspace.path(), "imexplore", true, &session)
        .unwrap();
    assert!(lifecycle.take_warnings().is_empty());
    assert_eq!(
        read_slot(&store, "imexplore", ManagedProfileKind::Last),
        Some(opened_snapshot.clone())
    );
    assert_eq!(
        read_slot(&store, "imexplore", ManagedProfileKind::LastSuccessful),
        None
    );

    session
        .set("stretch", ParameterValue::String("manual".into()))
        .unwrap();
    session
        .set("clip_low", ParameterValue::String("1".into()))
        .unwrap();
    lifecycle
        .accepted_durable_change(workspace.path(), "imexplore", true, &session)
        .unwrap();
    session
        .set("clip_low", ParameterValue::String("2".into()))
        .unwrap();
    lifecycle
        .accepted_durable_change(workspace.path(), "imexplore", true, &session)
        .unwrap();
    let latest_accepted = session.render_sparse().unwrap();

    // A local/transient edit that was not acknowledged by the backend is not
    // part of the queued snapshot.
    session
        .set("clip_high", ParameterValue::String("unaccepted".into()))
        .unwrap();
    lifecycle.flush(workspace.path(), "imexplore");
    assert!(lifecycle.take_warnings().is_empty());
    assert_eq!(
        read_slot(&store, "imexplore", ManagedProfileKind::Last),
        Some(latest_accepted)
    );

    // A second clean-close notification is harmless.
    lifecycle.flush(workspace.path(), "imexplore");
    assert!(lifecycle.take_warnings().is_empty());
}

#[test]
fn concurrent_profile_writers_are_observed_as_complete_atomic_snapshots() {
    let state_root = tempfile::tempdir().unwrap();
    let store = Arc::new(ManagedStateStore::with_state_root(state_root.path()));
    let initial = task_session("initial.ms", "initial")
        .render_sparse()
        .unwrap();
    let first = task_session("first.ms", &"a".repeat(8_192))
        .render_sparse()
        .unwrap();
    let second = task_session("second.ms", &"b".repeat(8_192))
        .render_sparse()
        .unwrap();
    store
        .write("flagmanager", ManagedProfileKind::Last, &initial)
        .unwrap();

    let barrier = Arc::new(Barrier::new(3));
    let completed = Arc::new(AtomicUsize::new(0));
    let observed_partial = Arc::new(AtomicBool::new(false));
    let mut writers = Vec::new();
    for profile in [first.clone(), second.clone()] {
        let writer_store = Arc::clone(&store);
        let writer_barrier = Arc::clone(&barrier);
        let writer_completed = Arc::clone(&completed);
        writers.push(thread::spawn(move || {
            writer_barrier.wait();
            for _ in 0..12 {
                writer_store
                    .write("flagmanager", ManagedProfileKind::Last, &profile)
                    .unwrap();
            }
            writer_completed.fetch_add(1, Ordering::Release);
        }));
    }

    let reader_store = Arc::clone(&store);
    let reader_barrier = Arc::clone(&barrier);
    let reader_completed = Arc::clone(&completed);
    let reader_partial = Arc::clone(&observed_partial);
    let allowed = [initial, first.clone(), second.clone()];
    let reader = thread::spawn(move || {
        reader_barrier.wait();
        while reader_completed.load(Ordering::Acquire) < 2 {
            let actual = reader_store
                .read("flagmanager", ManagedProfileKind::Last)
                .unwrap()
                .unwrap();
            if !allowed.contains(&actual) {
                reader_partial.store(true, Ordering::Release);
                break;
            }
            thread::yield_now();
        }
    });

    for writer in writers {
        writer.join().unwrap();
    }
    reader.join().unwrap();
    assert!(!observed_partial.load(Ordering::Acquire));
    let final_profile = read_slot(&store, "flagmanager", ManagedProfileKind::Last).unwrap();
    assert!(final_profile == first || final_profile == second);

    let directory = store.surface_dir("flagmanager").unwrap();
    let generated_temps = fs::read_dir(directory)
        .unwrap()
        .map(|entry| entry.unwrap().file_name().to_string_lossy().into_owned())
        .filter(|name| name.contains(".tmp."))
        .collect::<Vec<_>>();
    assert!(generated_temps.is_empty());
}

#[test]
fn crash_leftover_temp_is_ignored_and_does_not_block_replacement() {
    let state_root = tempfile::tempdir().unwrap();
    let store = ManagedStateStore::with_state_root(state_root.path());
    let prior = task_session("prior.ms", "prior").render_sparse().unwrap();
    let replacement = task_session("replacement.ms", "replacement")
        .render_sparse()
        .unwrap();
    store
        .write("flagmanager", ManagedProfileKind::Last, &prior)
        .unwrap();

    // Simulate a process dying after creating a temporary file but before the
    // atomic rename. Readers consult only the complete destination.
    let directory = store.surface_dir("flagmanager").unwrap();
    let stale = directory.join(format!(
        ".last.toml.tmp.{}.{}",
        std::process::id(),
        u64::MAX
    ));
    fs::write(&stale, "partial profile").unwrap();
    assert_eq!(
        read_slot(&store, "flagmanager", ManagedProfileKind::Last),
        Some(prior)
    );

    store
        .write("flagmanager", ManagedProfileKind::Last, &replacement)
        .unwrap();
    assert_eq!(
        read_slot(&store, "flagmanager", ManagedProfileKind::Last),
        Some(replacement)
    );
    assert!(stale.exists());
}

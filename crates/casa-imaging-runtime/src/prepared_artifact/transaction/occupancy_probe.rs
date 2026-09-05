// SPDX-License-Identifier: LGPL-3.0-or-later

use std::time::Instant;

use super::*;

/// Metadata-only discriminator; never publishes, evicts, locks, or reads payloads.
#[test]
#[ignore = "requires an immutable stopped private store and fresh same-volume scratch"]
fn t51_prepared_store_read_only_occupancy_scan() {
    let started = Instant::now();
    let root = PathBuf::from(
        std::env::var_os("CASA_RS_T51_OCCUPANCY_STORE").expect("stopped private store"),
    )
    .canonicalize()
    .expect("existing stopped private store");
    let scratch_parent = PathBuf::from(
        std::env::var_os("CASA_RS_T51_OCCUPANCY_SCRATCH_PARENT")
            .expect("fresh same-volume scratch parent"),
    )
    .canonicalize()
    .expect("existing scratch parent");
    assert!(
        !scratch_parent.starts_with(&root),
        "scratch must be outside the retained store"
    );
    assert_eq!(
        fs::metadata(&root).unwrap().dev(),
        fs::metadata(&scratch_parent).unwrap().dev(),
        "empty and occupied scans must use the same filesystem"
    );
    assert!(root.join(CACHE_DIRECTORY).is_dir());
    assert!(root.join(LOCK_FILE).is_file());
    let before = metadata_snapshot(&root);
    let manifest_path = before
        .iter()
        .map(|(path, _, _, _)| path)
        .find(|path| path.file_name().is_some_and(|name| name == MANIFEST_FILE))
        .expect("at least one committed manifest");
    let manifest: ArtifactManifest =
        serde_json::from_reader(File::open(manifest_path).unwrap()).unwrap();
    assert_eq!(manifest.schema, CACHE_SCHEMA);
    assert_eq!(manifest.schema_version, CACHE_SCHEMA_VERSION);
    let scope = &manifest.descriptor.cache_scope;
    scope.validate().expect("retained scope");
    let budget = PreparedArtifactBudget::new(
        scope.cache_bytes,
        usize::try_from(scope.entries).unwrap(),
        scope.streaming_buffer_bytes,
    )
    .unwrap();
    let domain = StorageDomain {
        id: StorageDomainId::new(&scope.storage_domain),
        root: root.clone(),
        capacity_bytes: budget.cache_bytes,
        read_rate: crate::RateResourceId::new("occupancy-probe-read"),
        write_rate: crate::RateResourceId::new("occupancy-probe-write"),
        operations_rate: None,
        queue: crate::QueueResourceId::new("occupancy-probe-queue"),
    };
    // All directories already exist; open does not inventory or mutate entries.
    let occupied = PreparedArtifactStore::open(&root, &domain, budget).unwrap();
    assert_eq!(&occupied.scope, scope, "retain the exact root-bound scope");
    let scratch = tempfile::Builder::new()
        .prefix("t51-occupancy-empty-")
        .tempdir_in(scratch_parent)
        .unwrap();
    let empty = PreparedArtifactStore::open(
        scratch.path(),
        &StorageDomain {
            root: scratch.path().to_path_buf(),
            ..domain
        },
        budget,
    )
    .unwrap();
    let committed_count = before
        .iter()
        .filter(|(path, _, _, _)| {
            path.file_name().is_some_and(|name| name == MANIFEST_FILE)
                && path.parent().is_some_and(|parent| {
                    !parent
                        .file_name()
                        .unwrap()
                        .to_string_lossy()
                        .starts_with(STAGING_PREFIX)
                })
        })
        .count();
    eprintln!(
        "t51_occupancy_setup committed_entries={committed_count} cache_budget_bytes={} entry_budget={} streaming_buffer_bytes={} setup_seconds={:.9} scratch={}",
        budget.cache_bytes,
        budget.entries,
        budget.streaming_buffer_bytes,
        started.elapsed().as_secs_f64(),
        scratch.path().display(),
    );
    for repeat in 0..3 {
        for (label, store) in [("empty", &empty), ("occupied", &occupied)] {
            assert!(
                started.elapsed() < Duration::from_secs(60),
                "probe deadline"
            );
            let mut evidence = ValidationEvidence::new(budget);
            let timer = Instant::now();
            let bytes = store
                .validate_raw_budget(ArtifactIdentity::from_owner_digest([0; 32]), &mut evidence)
                .unwrap();
            report(label, repeat, "raw_budget", timer, bytes, None, &evidence);
            assert!(
                started.elapsed() < Duration::from_secs(60),
                "probe deadline"
            );
            let mut evidence = ValidationEvidence::new(budget);
            let timer = Instant::now();
            let (count, bytes) = store
                .with_entries(&mut evidence, |_, entries| {
                    Ok((
                        entries.len(),
                        entries.iter().map(|entry| entry.bytes).sum::<u64>(),
                    ))
                })
                .unwrap();
            report(
                label,
                repeat,
                "entries",
                timer,
                bytes,
                Some(count),
                &evidence,
            );
            assert_eq!(count, if label == "empty" { 0 } else { committed_count });
        }
    }
    assert_eq!(
        before,
        metadata_snapshot(&root),
        "stopped store is unchanged"
    );
    assert!(
        started.elapsed() < Duration::from_secs(60),
        "probe deadline"
    );
    eprintln!(
        "t51_occupancy_complete seconds={:.9}",
        started.elapsed().as_secs_f64()
    );
}

fn report(
    store: &str,
    repeat: usize,
    operation: &str,
    started: Instant,
    bytes: u64,
    entries: Option<usize>,
    evidence: &ValidationEvidence,
) {
    assert_eq!(evidence.cache_read.bytes, 0, "no file contents read");
    assert_eq!(evidence.cache_write.operations, 0, "no store writes");
    assert_eq!(
        evidence.cache_control.operations, 0,
        "no locks or control mutations"
    );
    assert!(evidence.source_reads.is_empty());
    eprintln!(
        "t51_occupancy_scan store={store} repeat={repeat} operation={operation} seconds={:.9} entries={entries:?} inventory_bytes={bytes} metadata_operations={} read_bytes={} peak_resident_bytes={}",
        started.elapsed().as_secs_f64(),
        evidence.cache_read.operations,
        evidence.cache_read.bytes,
        evidence.resident_buffer_bytes,
    );
}

fn metadata_snapshot(root: &Path) -> Vec<(PathBuf, u64, i64, i64)> {
    let mut paths = vec![root.to_path_buf()];
    let mut snapshot = Vec::new();
    while let Some(path) = paths.pop() {
        let metadata = path.symlink_metadata().unwrap();
        assert!(
            !metadata.file_type().is_symlink(),
            "unexpected retained symlink"
        );
        snapshot.push((
            path.clone(),
            metadata.len(),
            metadata.mtime(),
            metadata.mtime_nsec(),
        ));
        if metadata.is_dir() {
            paths.extend(
                fs::read_dir(path)
                    .unwrap()
                    .map(|entry| entry.unwrap().path()),
            );
        }
    }
    snapshot.sort();
    snapshot
}

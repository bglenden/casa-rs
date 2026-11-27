// SPDX-License-Identifier: LGPL-3.0-or-later

use std::path::PathBuf;

use casa_measures::{MeasuresRuntime, RuntimePolicy, install_packaged_snapshot};

#[test]
fn packaged_runtime_reports_the_2017_leap_second() {
    let directory = tempfile::tempdir().unwrap();
    let archive = std::env::var_os("CASA_RS_PACKAGED_SNAPSHOT")
        .map(PathBuf::from)
        .unwrap_or_else(|| {
            PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("data/casa-measures-runtime.tar.gz")
        });
    let provenance = install_packaged_snapshot(&archive, directory.path()).unwrap();
    let runtime = MeasuresRuntime::open(
        directory.path().to_path_buf(),
        RuntimePolicy {
            require_provenance: true,
            ..RuntimePolicy::default()
        },
    )
    .unwrap();
    assert_eq!(
        runtime.provenance().unwrap().measures_version,
        provenance.measures_version
    );
    // The 2016-12-31 leap second raised TAI minus UTC from 36 s to 37 s.
    assert_eq!(runtime.tai_minus_utc_seconds(57_753.0).unwrap(), 36.0);
    assert_eq!(runtime.tai_minus_utc_seconds(57_754.0).unwrap(), 37.0);
}

#[test]
fn mismatched_snapshot_bytes_leave_the_destination_unmodified() {
    let directory = tempfile::tempdir().unwrap();
    let archive = directory.path().join("invalid.tar.gz");
    std::fs::write(&archive, b"not the release snapshot").unwrap();
    let destination = directory.path().join("measures");
    let error = install_packaged_snapshot(&archive, &destination).unwrap_err();
    assert!(
        error
            .to_string()
            .contains("does not match the release snapshot size and SHA-256")
    );
    assert!(!destination.exists());
}

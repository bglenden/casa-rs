// SPDX-License-Identifier: LGPL-3.0-or-later

use std::path::{Path, PathBuf};
use std::sync::mpsc::{Sender, channel};
use std::thread::{self, Scope, ScopedJoinHandle};
use std::time::Duration;

use casa_core::cancellation::Cancellation;
use casa_ms::{MeasurementSet, selection::MsSelection};
use casa_mstransform::test_hooks::{Checkpoint, with_single_row_batches};
use casa_mstransform::{
    MsTransformError, MsTransformReport, MsTransformRequest, MsTransformTaskOptions,
    TransformDataColumn, mstransform_task,
};

use crate::common::create_msexplore_spectrum_fixture_ms;

const TIMEOUT: Duration = Duration::from_secs(15);
type TransformResult = Result<MsTransformReport, Box<MsTransformError>>;

fn request(root: &Path) -> MsTransformRequest {
    MsTransformRequest {
        input_ms: create_msexplore_spectrum_fixture_ms(root, true, &[]),
        output_ms: root.join("output.ms"),
        spw: String::new(),
        width: 1,
        data_column: TransformDataColumn::Data,
        selection: MsSelection::new(),
        keep_flags: true,
    }
}

fn reservations(output: &Path) -> Vec<PathBuf> {
    let prefix = format!(
        ".{}.casa-rs-incomplete-",
        output.file_name().unwrap().to_string_lossy()
    );
    output
        .parent()
        .unwrap()
        .read_dir()
        .unwrap()
        .map(|entry| entry.unwrap().path())
        .filter(|path| {
            path.file_name()
                .unwrap()
                .to_string_lossy()
                .starts_with(&prefix)
        })
        .collect()
}

fn pause_transform<'thread>(
    scope: &'thread Scope<'thread, '_>,
    request: MsTransformRequest,
    cancel: Cancellation,
    boundary: Checkpoint,
) -> (ScopedJoinHandle<'thread, TransformResult>, Sender<()>) {
    let (staged, arrived) = channel();
    let (resume, wait) = channel();
    let worker = scope.spawn(move || {
        with_single_row_batches(
            move |checkpoint| {
                if checkpoint == boundary {
                    staged.send(()).unwrap();
                    wait.recv_timeout(TIMEOUT).unwrap();
                }
            },
            || {
                mstransform_task(
                    &request,
                    &MsTransformTaskOptions {
                        cancel: Some(cancel),
                        ..MsTransformTaskOptions::default()
                    },
                )
                .map_err(Box::new)
            },
        )
    });
    arrived
        .recv_timeout(TIMEOUT)
        .expect("worker reached checkpoint");
    (worker, resume)
}

#[test]
fn cancelling_one_transform_keeps_the_other_reservation_for_the_same_destination() {
    let dir = tempfile::tempdir().unwrap();
    let request = request(dir.path());
    let output = &request.output_ms;
    thread::scope(|scope| {
        let cancel = Cancellation::new();
        let (first, resume_first) = pause_transform(
            scope,
            request.clone(),
            cancel.clone(),
            Checkpoint::VisibilityBatchStaged(0),
        );
        let first_root = reservations(output)
            .pop()
            .expect("first reservation survives staging");
        let (second, resume_second) = pause_transform(
            scope,
            request.clone(),
            Cancellation::new(),
            Checkpoint::BeforePublication,
        );
        let concurrent = reservations(output);
        assert_eq!(concurrent.len(), 2);
        let second_root = concurrent.iter().find(|path| **path != first_root).unwrap();
        let staged_ms = second_root.join(output.file_name().unwrap());
        assert!(staged_ms.join("table.dat").is_file());

        cancel.cancel();
        resume_first.send(()).unwrap();
        assert!(matches!(
            *first.join().unwrap().unwrap_err(),
            MsTransformError::Interrupted
        ));
        assert!(!first_root.exists());
        assert!(!output.exists());
        let rows = MeasurementSet::open(staged_ms).unwrap().row_count();
        assert!(rows > 1);

        resume_second.send(()).unwrap();
        assert_eq!(second.join().unwrap().unwrap().row_count, rows);
        assert_eq!(MeasurementSet::open(output).unwrap().row_count(), rows);
        assert!(reservations(output).is_empty());
    });
}

#[test]
fn concurrent_publication_preserves_the_first_measurement_set() {
    let dir = tempfile::tempdir().unwrap();
    let request = request(dir.path());
    let output = &request.output_ms;
    thread::scope(|scope| {
        let (first, resume_first) = pause_transform(
            scope,
            request.clone(),
            Cancellation::new(),
            Checkpoint::BeforePublication,
        );
        let mut competing = request.clone();
        competing.spw = "0:0".into();
        let (second, resume_second) = pause_transform(
            scope,
            competing,
            Cancellation::new(),
            Checkpoint::BeforePublication,
        );
        assert_eq!(reservations(output).len(), 2);

        resume_first.send(()).unwrap();
        let winner = first.join().unwrap().unwrap();
        let metadata = std::fs::read(output.join("table.dat")).unwrap();
        let spectral = std::fs::read(output.join("SPECTRAL_WINDOW/table.dat")).unwrap();

        resume_second.send(()).unwrap();
        assert!(matches!(
            *second.join().unwrap().unwrap_err(),
            MsTransformError::PrepareOutput { .. }
        ));
        assert_eq!(std::fs::read(output.join("table.dat")).unwrap(), metadata);
        assert_eq!(
            std::fs::read(output.join("SPECTRAL_WINDOW/table.dat")).unwrap(),
            spectral
        );
        assert_eq!(
            MeasurementSet::open(output).unwrap().row_count(),
            winner.row_count
        );
        assert!(reservations(output).is_empty());
    });
}

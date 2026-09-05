// SPDX-License-Identifier: LGPL-3.0-or-later

//! Read-only cost of the receipt decoding performed by retention admission.

use super::{
    ExecutionReceiptStore, ReceiptRetention, fs, is_receipt_path, read_receipt_body,
    receipt_root_state,
};
use std::{
    path::PathBuf,
    time::{Duration, Instant, SystemTime},
};

#[test]
#[ignore = "requires retained T51 receipts; release binary with an external 60s timeout"]
#[allow(clippy::assertions_on_constants)]
fn t51_retained_receipt_decode_cost() {
    assert!(!cfg!(debug_assertions));
    let root = PathBuf::from(std::env::var_os("CASA_RS_T51_RETAINED_RECEIPTS").unwrap())
        .canonicalize()
        .unwrap();
    let before = snapshot(&root);
    assert!(!before.is_empty() && before.len() <= 512);
    let bytes: u64 = before.iter().map(|(_, size, _)| size).sum();
    let started = Instant::now();
    for repeat in 0..3 {
        let timer = Instant::now();
        let mut terminal = 0;
        for (path, _, _) in &before {
            assert!(started.elapsed() < Duration::from_secs(55));
            let body = read_receipt_body(path).expect("production retention decode");
            terminal += usize::from(body.status.is_terminal());
            std::hint::black_box(body);
        }
        eprintln!(
            "t51_receipt_decode repeat={repeat} receipts={} terminal={terminal} bytes={bytes} seconds={:.9}",
            before.len(),
            timer.elapsed().as_secs_f64()
        );
    }
    // Avoid the public constructor's orphan cleanup on this retained evidence.
    let store = ExecutionReceiptStore {
        root: root.clone(),
        state: receipt_root_state(&root, ReceiptRetention::new(512, 256 << 20).unwrap()).unwrap(),
    };
    for repeat in 0..3 {
        assert!(started.elapsed() < Duration::from_secs(55));
        let previous = store.state.summaries.lock().unwrap().stats();
        let timer = Instant::now();
        let summaries = store.summaries().expect("production validated summaries");
        let seconds = timer.elapsed().as_secs_f64();
        let stats = store.state.summaries.lock().unwrap().stats();
        assert_eq!(summaries.len(), before.len());
        assert_eq!(stats.entries, before.len());
        assert!(stats.charged_bytes <= store.state.retention.max_bytes);
        if repeat > 0 {
            assert_eq!(stats.full_decodes, previous.full_decodes);
            assert_eq!(stats.hits - previous.hits, before.len() as u64);
            assert_eq!(stats.bytes_hashed - previous.bytes_hashed, bytes);
        }
        for summary in &summaries {
            let receipt = store
                .open(summary.attempt)
                .expect("independent full receipt read");
            assert_eq!(summary.status, receipt.status());
            assert!(summary.status.is_terminal());
            assert!(summary.infeasibility.is_none());
            assert_eq!(
                summary.order_millis,
                receipt
                    .body
                    .finished_unix_millis
                    .unwrap_or(receipt.body.started_unix_millis)
            );
            assert_eq!(
                summary.retention_bytes,
                fs::metadata(store.receipt_path(summary.attempt))
                    .unwrap()
                    .len()
            );
        }
        eprintln!(
            "t51_receipt_summaries repeat={repeat} receipts={} seconds={seconds:.9} charged_bytes={} full_decodes={} hits={} bytes_hashed={} verification_outside_timing=true",
            summaries.len(),
            stats.charged_bytes,
            stats.full_decodes,
            stats.hits,
            stats.bytes_hashed
        );
    }
    assert_eq!(before, snapshot(&root), "retained files unchanged");
    eprintln!(
        "t51_receipt_decode_complete seconds={:.9}",
        started.elapsed().as_secs_f64()
    );
}

fn snapshot(root: &std::path::Path) -> Vec<(PathBuf, u64, SystemTime)> {
    let mut files = fs::read_dir(root)
        .unwrap()
        .map(|entry| entry.unwrap().path())
        .filter(|path| is_receipt_path(path))
        .map(|path| {
            let metadata = path.symlink_metadata().unwrap();
            assert!(metadata.is_file() && !metadata.file_type().is_symlink());
            (path, metadata.len(), metadata.modified().unwrap())
        })
        .collect::<Vec<_>>();
    files.sort();
    files
}

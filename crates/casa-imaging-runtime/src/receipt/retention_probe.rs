// SPDX-License-Identifier: LGPL-3.0-or-later

//! Read-only cost of the receipt decoding performed by retention admission.

use super::{fs, is_receipt_path, read_receipt_body};
use std::{
    path::PathBuf,
    time::{Duration, Instant, SystemTime},
};

#[test]
#[ignore = "requires retained T51 receipts; release binary with an external 60s timeout"]
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

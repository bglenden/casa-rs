// SPDX-License-Identifier: LGPL-3.0-or-later

#![cfg(feature = "cpp-interop-tests")]

use std::path::PathBuf;

mod common;

use common::run_importvla_parity_case;

#[test]
fn imported_measurement_set_matches_casa_task_when_configured() {
    let Some(archive_path) = std::env::var_os("CASA_RS_IMPORTVLA_ARCHIVE").map(PathBuf::from)
    else {
        eprintln!("skipping: CASA_RS_IMPORTVLA_ARCHIVE not set");
        return;
    };
    if !archive_path.exists() {
        eprintln!("skipping: {} does not exist", archive_path.display());
        return;
    }

    run_importvla_parity_case(&archive_path);
}

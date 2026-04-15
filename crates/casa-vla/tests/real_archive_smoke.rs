// SPDX-License-Identifier: LGPL-3.0-or-later

use std::path::PathBuf;

use casa_vla::VlaDiskReader;

#[test]
fn reads_real_archive_when_configured() {
    let Some(path) = std::env::var_os("CASA_RS_IMPORTVLA_ARCHIVE").map(PathBuf::from) else {
        eprintln!("skipping: CASA_RS_IMPORTVLA_ARCHIVE not set");
        return;
    };
    if !path.exists() {
        eprintln!("skipping: {} does not exist", path.display());
        return;
    }

    let mut reader = VlaDiskReader::open(&path).expect("open archive");
    let record = reader
        .next_record()
        .expect("read logical record")
        .expect("archive contains at least one logical record");
    let rca = record.rca();
    let declared_len = rca.length_bytes().expect("RCA length") as usize;

    assert_eq!(record.bytes().len(), declared_len);
    assert!(rca.revision().expect("RCA revision") > 0);
    assert!(rca.n_antennas().expect("RCA antenna count") > 0);
}

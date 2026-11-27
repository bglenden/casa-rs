// SPDX-License-Identifier: LGPL-3.0-or-later
use casa_ms::{MeasurementSet, MeasurementSetBuilder};
use casa_singledish::{import_asap, import_nro};

#[test]
fn invalid_import_preserves_an_existing_measurement_set() {
    let root = tempfile::tempdir().unwrap();
    let existing = root.path().join("existing.ms");
    MeasurementSet::create(&existing, MeasurementSetBuilder::new()).unwrap();
    let missing = root.path().join("missing");
    let original = std::fs::read(existing.join("table.dat")).unwrap();

    for import in [import_asap, import_nro] {
        assert!(import(&missing, &existing, false).is_err());
        assert!(import(&existing, &existing, true).is_err());
        assert_eq!(std::fs::read(existing.join("table.dat")).unwrap(), original);
        assert_eq!(MeasurementSet::open(&existing).unwrap().row_count(), 0);
    }
    assert_eq!(std::fs::read_dir(root.path()).unwrap().count(), 1);
}

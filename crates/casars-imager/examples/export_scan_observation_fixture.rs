// SPDX-License-Identifier: LGPL-3.0-or-later
//! Temporary fixture exporter, using the exact T1 synthetic setup.
#![allow(dead_code)]
#[path = "../tests/t1/fixture.rs"]
mod fixture;

use casa_ms::{MeasurementSet, SubtableId};
use casa_types::{ScalarValue, Value};
use fixture::{Component, Observation, PHASE_CENTER_RAD, Setup, VISIBILITY_NOISE_JY};
use std::{collections::BTreeMap, path::Path};

fn copy_tree(source: &Path, target: &Path) -> std::io::Result<()> {
    std::fs::create_dir(target)?;
    for item in std::fs::read_dir(source)? {
        let item = item?;
        let destination = target.join(item.file_name());
        if item.file_type()?.is_dir() {
            copy_tree(&item.path(), &destination)?;
        } else {
            std::fs::copy(item.path(), &destination)?;
        }
    }
    Ok(())
}

fn main() {
    let destination = std::env::args_os()
        .nth(1)
        .map(std::path::PathBuf::from)
        .expect("an explicit external evidence destination directory");
    assert!(
        !destination.exists(),
        "refuse to overwrite an existing evidence fixture"
    );
    assert!(
        destination.starts_with("/Volumes/GLENDENNING/")
            || destination.starts_with("/Volumes/home/"),
        "fixture belongs on external/NAS storage"
    );
    std::fs::create_dir_all(destination.parent().unwrap()).unwrap();
    let mut setup = Setup::vla_q_band(2);
    setup.fields = vec![PHASE_CENTER_RAD; 4];
    let observation = Observation::synthesise_setup(
        setup,
        &[(
            "point",
            Component {
                offset_px: [12, 8],
                flux_jy: 1.0,
                fwhm_px: None,
                spectral_index: 0.0,
            },
        )],
    );
    let geometry = observation.geometry();
    let mut ms = MeasurementSet::open(observation.measurement_set()).unwrap();
    let row = ms
        .subtable(SubtableId::Observation)
        .unwrap()
        .rows()
        .unwrap()[0]
        .clone();
    ms.subtable_mut(SubtableId::Observation)
        .unwrap()
        .add_row(row)
        .unwrap();
    let mut groups = BTreeMap::<(i32, i32, i32), Vec<usize>>::new();
    let mut flagged = 0_usize;
    for row in 0..ms.row_count() {
        let field = match ms
            .main_table()
            .cell_accessor(row, "FIELD_ID")
            .unwrap()
            .scalar()
            .unwrap()
        {
            ScalarValue::Int32(value) => *value,
            other => panic!("unexpected FIELD_ID {other:?}"),
        };
        let scan = field % 2 + 1;
        let observation = field / 2;
        for (column, value) in [("SCAN_NUMBER", scan), ("OBSERVATION_ID", observation)] {
            ms.main_table_mut()
                .row_accessor_mut()
                .set_cell(row, column, Value::Scalar(ScalarValue::Int32(value)))
                .unwrap();
        }
        if matches!(
            ms.main_table()
                .cell_accessor(row, "FLAG_ROW")
                .unwrap()
                .scalar()
                .unwrap(),
            ScalarValue::Bool(true)
        ) {
            flagged += 1;
        }
        groups
            .entry((field, scan, observation))
            .or_default()
            .push(row);
    }
    let rows = ms.row_count();
    ms.save().unwrap();
    drop(ms);
    copy_tree(observation.measurement_set(), &destination).unwrap();
    let groups = groups.into_iter().map(|((field,scan,observation),rows)| serde_json::json!({
        "field":field,"scan":scan,"observation":observation,"count":rows.len(),"physical_rows":rows,
    })).collect::<Vec<_>>();
    let metadata = serde_json::json!({
        "fixture":destination,"purpose":"four coincident fields; each observation contains two scans",
        "main_rows":rows,"flagged_main_rows":flagged,"groups":groups,
        "imsize":geometry.image_size,"cell":geometry.cell_arcsec.map(|value| format!("{value}arcsec")),
        "phasecenter_rad":PHASE_CENTER_RAD,"channels":2,"field_names":["t1-0","t1-1","t1-2","t1-3"],
        "corruption_seed":20261007,"noise_jy":VISIBILITY_NOISE_JY,
        "scan_assignment":"FIELD_ID % 2 + 1","observation_assignment":"FIELD_ID / 2",
        "base_setup":"crates/casars-imager/tests/t1/fixture.rs::Observation::synthesise_setup",
    });
    let record = destination.with_extension("fixture.json");
    std::fs::write(
        &record,
        serde_json::to_string_pretty(&metadata).unwrap() + "\n",
    )
    .unwrap();
    println!("{}", record.display());
}

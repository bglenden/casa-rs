// SPDX-License-Identifier: LGPL-3.0-or-later
//! Shared actual CASA inputs for science and live-allocation comparisons.
use casa_simulation::{SessionResources, SimulatorOptions, simalma::SimalmaRequest};
use casa_types::measures::epoch::{EpochRef, MEpoch};
use flate2::read::GzDecoder;
use serde_json::{Value, json};

pub fn fixture(resources: SessionResources) -> (tempfile::TempDir, SimalmaRequest, Value) {
    let oracle: Value = serde_json::from_reader(GzDecoder::new(
        &include_bytes!("../data/simalma_casa.json.gz")[..],
    ))
    .unwrap();
    let scratch = tempfile::tempdir().unwrap();
    tar::Archive::new(GzDecoder::new(
        &include_bytes!("../data/simalma_inputs.tar.gz")[..],
    ))
    .unpack(scratch.path())
    .unwrap();
    let mut p = oracle["parameters"].as_object().unwrap().clone();
    let root = scratch.path();
    p.insert("project".into(), json!(root.join("combo")));
    p.insert("skymodel".into(), json!(root.join("inputs/model.image")));
    p.insert(
        "antennalist".into(),
        json!([
            root.join("inputs/alma.test.cfg"),
            root.join("inputs/aca.test.cfg")
        ]),
    );
    p.insert("hourangle".into(), json!("35s"));
    let request = SimalmaRequest {
        workspace: root.to_path_buf(),
        parameters: p,
        data_roots: vec![root.join("inputs")],
        options: SimulatorOptions {
            resources,
            measures: casa_ms::open_measures_runtime().unwrap(),
            now: MEpoch::from_mjd(60_000., EpochRef::UTC),
        },
    };
    (scratch, request, oracle)
}

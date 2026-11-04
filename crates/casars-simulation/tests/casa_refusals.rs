// SPDX-License-Identifier: LGPL-3.0-or-later
//! CASA's integer refusals through the task binary's JSON front end (#1099).

use std::io::Write;
use std::process::{Command, Stdio};

use serde_json::{Value, json};

#[test]
fn json_run_reports_casas_integer_refusals() {
    let cases: Vec<Value> = serde_json::from_str(include_str!(
        "../../casars-python/python/tests/data/casa_simobserve_refusals.json"
    ))
    .expect("CASA's recorded refusals");
    for case in cases.iter().filter(|case| case["type"] == "AssertionError") {
        let mut child = Command::new(env!("CARGO_BIN_EXE_simobserve"))
            .args(["--json-run", "-"])
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .spawn()
            .expect("simobserve starts");
        let request = json!({"surface": "simobserve", "values": case["args"]});
        write!(child.stdin.take().expect("stdin"), "{request}").expect("write request");
        let output = child.wait_with_output().expect("simobserve exits");
        assert!(!output.status.success());
        let reply: Value = serde_json::from_slice(&output.stdout).expect("JSON task reply");
        assert_eq!(reply["kind"], "validation");
        assert_eq!(reply["message"], case["message"]);
    }
}

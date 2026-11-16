// SPDX-License-Identifier: LGPL-3.0-or-later
//! CASA's integer refusals through the task binary's JSON front end (#1099).

use std::io::Write;
use std::process::{Command, Stdio};

use serde_json::{Value, json};

fn json_reply(request: &str) -> Value {
    let mut child = Command::new(env!("CARGO_BIN_EXE_simobserve"))
        .args(["--json-run", "-"])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .spawn()
        .expect("simobserve starts");
    write!(child.stdin.take().expect("stdin"), "{request}").expect("write request");
    let output = child.wait_with_output().expect("simobserve exits");
    assert!(!output.status.success());
    serde_json::from_slice(&output.stdout).expect("JSON task reply")
}

#[test]
fn json_run_reports_casas_integer_refusals() {
    let cases: Vec<Value> = serde_json::from_str(include_str!(
        "../../casars-python/python/tests/data/casa_simobserve_refusals.json"
    ))
    .expect("CASA's recorded refusals");
    for case in cases.iter().filter(|case| case["type"] == "AssertionError") {
        let request = json!({"surface": "simobserve", "values": case["args"]});
        let reply = json_reply(&request.to_string());
        assert_eq!(reply["kind"], "validation");
        assert_eq!(reply["message"], case["message"]);
    }
}

#[test]
fn oversized_json_integers_are_not_misreported_as_casa_type_errors() {
    for number in [
        "9223372036854775808",
        "18446744073709551615",
        "-9223372036854775809",
    ] {
        let request = format!(r#"{{"surface":"simobserve","values":{{"seed":{number}}}}}"#);
        let reply = json_reply(&request);
        assert_eq!(reply["kind"], "validation");
        assert_eq!(reply["parameter"], "seed");
        assert_eq!(
            reply["message"],
            "parameter \"seed\": integer is outside casa-rs's signed 64-bit range"
        );
    }
    // CASA's cInt validator rejects a float even if its magnitude is large.
    let reply = json_reply(r#"{"surface":"simobserve","values":{"seed":9.223372036854776e18}}"#);
    assert_eq!(reply["message"], "{'seed': ['must be of cInt type']}");
}

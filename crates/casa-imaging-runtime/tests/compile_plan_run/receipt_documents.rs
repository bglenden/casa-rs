// SPDX-License-Identifier: LGPL-3.0-or-later

//! Persisted-receipt document helpers: locate a receipt, recompute its
//! payload checksum and forge checksum-valid typed projections for the
//! tamper checks.

use super::*;

pub(crate) fn only_receipt_path(root: &Path) -> PathBuf {
    let mut entries = fs::read_dir(root)
        .expect("receipt directory listing")
        .map(|entry| entry.expect("receipt entry").path())
        .filter(|path| {
            path.extension()
                .is_some_and(|extension| extension == "json")
        });
    let path = entries.next().expect("one persisted receipt");
    assert!(
        entries.next().is_none(),
        "fixture persists exactly one receipt"
    );
    path
}

fn compact_json(value: &str) -> String {
    let mut compact = String::with_capacity(value.len());
    let mut in_string = false;
    let mut escaped = false;
    for character in value.chars() {
        if in_string {
            compact.push(character);
            if escaped {
                escaped = false;
            } else if character == '\\' {
                escaped = true;
            } else if character == '"' {
                in_string = false;
            }
        } else if character == '"' {
            in_string = true;
            compact.push(character);
        } else if !character.is_whitespace() {
            compact.push(character);
        }
    }
    assert!(!in_string && !escaped, "complete JSON string");
    compact
}

fn receipt_payload(document: &str) -> &str {
    let marker = "\"receipt\":";
    let marker_start = document.find(marker).expect("receipt payload field");
    let start = document[marker_start + marker.len()..]
        .find('{')
        .map(|offset| marker_start + marker.len() + offset)
        .expect("receipt payload object");
    let mut depth = 0_usize;
    let mut in_string = false;
    let mut escaped = false;
    for (offset, character) in document[start..].char_indices() {
        if in_string {
            if escaped {
                escaped = false;
            } else if character == '\\' {
                escaped = true;
            } else if character == '"' {
                in_string = false;
            }
            continue;
        }
        match character {
            '"' => in_string = true,
            '{' => depth += 1,
            '}' => {
                depth -= 1;
                if depth == 0 {
                    return &document[start..=start + offset];
                }
            }
            _ => {}
        }
    }
    panic!("complete receipt payload object")
}

pub(crate) fn payload_sha256(document: &str) -> String {
    let payload = compact_json(receipt_payload(document));
    Sha256::digest(payload.as_bytes())
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

pub(crate) fn with_current_payload_checksum(mut document: String) -> String {
    let digest = payload_sha256(&document);
    let marker = "\"payload_sha256\":\"";
    let start = document.find(marker).expect("payload checksum") + marker.len();
    let end = start + 64;
    assert_eq!(&document[end..end + 1], "\"");
    document.replace_range(start..end, &digest);
    document
}

pub(crate) fn with_node_receipt_status(
    mut document: String,
    node: &str,
    current: &str,
    replacement: &str,
) -> String {
    let node_marker = format!("\"node_id\":\"{node}\"");
    let node_start = document
        .find(&node_marker)
        .expect("receipt node projection");
    let status_marker = format!("\"status\":\"{current}\"");
    let status_start = document[node_start..]
        .find(&status_marker)
        .map(|offset| node_start + offset)
        .expect("receipt node status");
    document.replace_range(
        status_start..status_start + status_marker.len(),
        &format!("\"status\":\"{replacement}\""),
    );
    with_current_payload_checksum(document)
}

pub(crate) fn with_usize_array(mut document: String, field: &str, values: &[usize]) -> String {
    let marker = format!("\"{field}\":[");
    let start = document.find(&marker).expect("typed projection field") + marker.len();
    let end = document[start..]
        .find(']')
        .map(|offset| start + offset)
        .expect("typed projection array");
    let replacement = values
        .iter()
        .map(usize::to_string)
        .collect::<Vec<_>>()
        .join(", ");
    document.replace_range(start..end, &replacement);
    with_current_payload_checksum(document)
}

pub(crate) fn with_forged_product_graph_identity(mut document: String) -> String {
    let graph_marker = "\"product_graph\":{";
    let graph_start = document
        .find(graph_marker)
        .expect("typed Product Graph projection");
    let identity_marker = "\"identity\":\"";
    let start = document[graph_start..]
        .find(identity_marker)
        .map(|offset| graph_start + offset + identity_marker.len())
        .expect("typed Product Graph identity");
    let end = start + 64;
    document.replace_range(start..end, &"f".repeat(64));
    with_current_payload_checksum(document)
}

pub(crate) fn with_forged_reprojection_identity(mut document: String) -> String {
    let reprojection_marker = "\"reprojection\":{";
    let reprojection_start = document
        .find(reprojection_marker)
        .expect("typed model reprojection projection");
    let identity_marker = "\"identity\":\"";
    let start = document[reprojection_start..]
        .find(identity_marker)
        .map(|offset| reprojection_start + offset + identity_marker.len())
        .expect("typed model reprojection identity");
    let original = document[start..start + 64].to_owned();
    assert_ne!(original, "e".repeat(64));
    document = document.replace(&original, &"e".repeat(64));
    with_current_payload_checksum(document)
}

pub(crate) fn with_forged_model_lifecycle_identity(
    mut document: String,
    replacement: &str,
) -> String {
    let lifecycle_marker = "\"model_lifecycle\":{";
    let lifecycle_start = document
        .find(lifecycle_marker)
        .expect("typed model lifecycle projection");
    let identity_marker = "\"identity\":\"";
    let start = document[lifecycle_start..]
        .find(identity_marker)
        .map(|offset| lifecycle_start + offset + identity_marker.len())
        .expect("typed model lifecycle identity");
    let original = document[start..start + 64].to_owned();
    assert_eq!(replacement.len(), 64);
    assert_ne!(original, replacement);
    document = document.replace(&original, replacement);
    with_current_payload_checksum(document)
}

pub(crate) fn with_forged_model_input_source_identity(
    mut document: String,
    replacement: &str,
) -> String {
    let value: serde_json::Value = serde_json::from_str(&document).expect("receipt JSON");
    let original = value["receipt"]["problem"]["model_lifecycle"]["input"]["source_identity"]
        .as_str()
        .expect("typed model input source identity")
        .to_owned();
    assert_eq!(replacement.len(), 64);
    assert_ne!(original, replacement);
    document = document.replace(&original, replacement);
    with_current_payload_checksum(document)
}

pub(crate) fn with_forged_problem_model_and_audit_identity(
    mut document: String,
    replacement: &str,
) -> String {
    assert_eq!(replacement.len(), 64);
    let model_marker = "\"model_identity\":{";
    let model_start = document.find(model_marker).expect("typed model identity");
    let identity_marker = "\"identity\":\"";
    let typed_start = document[model_start..]
        .find(identity_marker)
        .map(|offset| model_start + offset + identity_marker.len())
        .expect("typed model digest");
    assert_ne!(&document[typed_start..typed_start + 64], replacement);
    document.replace_range(typed_start..typed_start + 64, replacement);

    let audit_marker = "\"observation.model.identity\":\"";
    let audit_start =
        document.find(audit_marker).expect("audit model identity") + audit_marker.len();
    assert_ne!(&document[audit_start..audit_start + 64], replacement);
    document.replace_range(audit_start..audit_start + 64, replacement);
    with_current_payload_checksum(document)
}

pub(crate) fn with_forged_parent_problem_identity(
    mut document: String,
    replacement: &str,
) -> String {
    assert_eq!(replacement.len(), 64);
    for marker in ["\"problem_identity\":\"", "\"problem.identity\":\""] {
        let start = document.find(marker).expect("problem identity projection") + marker.len();
        assert_ne!(&document[start..start + 64], replacement);
        document.replace_range(start..start + 64, replacement);
    }
    with_current_payload_checksum(document)
}

pub(crate) fn with_forged_audit_field(mut document: String, field: &str, value: &str) -> String {
    let marker = format!("\"{field}\":\"");
    let start = document.find(&marker).expect("Product Graph audit field") + marker.len();
    let end = document[start..]
        .find('"')
        .map(|offset| start + offset)
        .expect("Product Graph audit value");
    document.replace_range(start..end, value);
    with_current_payload_checksum(document)
}

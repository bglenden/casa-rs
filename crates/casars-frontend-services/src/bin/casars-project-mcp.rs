// SPDX-License-Identifier: LGPL-3.0-or-later

//! Project-scoped MCP server used by external coding-agent backends.
//!
//! The server deliberately exposes typed CASA projections instead of a second
//! shell or filesystem API. Each call must carry the per-session nonce supplied
//! by the host when this process is launched.

use std::{
    env, fs,
    io::{self, BufRead, Write},
    path::{Path, PathBuf},
};

use casars_frontend_services::{
    assistant_corpus_search_json, parameter_resolve_json, parameter_surface_bundle_json,
    parameter_surface_catalog_json, task_ui_schema_json,
};
use serde_json::{Value, json};

const PROTOCOL_VERSION: &str = "2025-06-18";

fn main() {
    if let Err(error) = run() {
        eprintln!("casars-project-mcp: {error}");
        std::process::exit(1);
    }
}

fn run() -> Result<(), String> {
    let (project_root, nonce) = parse_args()?;
    let stdin = io::stdin();
    let mut stdout = io::stdout().lock();
    for line in stdin.lock().lines() {
        let line = line.map_err(|error| format!("read request: {error}"))?;
        if line.trim().is_empty() {
            continue;
        }
        let request: Value =
            serde_json::from_str(&line).map_err(|error| format!("parse request: {error}"))?;
        if let Some(response) = handle(&project_root, &nonce, &request) {
            serde_json::to_writer(&mut stdout, &response)
                .map_err(|error| format!("serialize response: {error}"))?;
            stdout
                .write_all(b"\n")
                .and_then(|()| stdout.flush())
                .map_err(|error| format!("write response: {error}"))?;
        }
    }
    Ok(())
}

fn parse_args() -> Result<(PathBuf, String), String> {
    let mut project_root = None;
    let mut nonce = None;
    let mut arguments = env::args().skip(1);
    while let Some(argument) = arguments.next() {
        match argument.as_str() {
            "--project-root" => project_root = arguments.next().map(PathBuf::from),
            "--nonce" => nonce = arguments.next(),
            other => return Err(format!("unrecognized argument {other}")),
        }
    }
    let project_root = project_root.ok_or("missing --project-root")?;
    if !project_root.is_absolute() || !project_root.is_dir() {
        return Err("--project-root must be an existing absolute directory".to_owned());
    }
    let nonce = nonce.ok_or("missing --nonce")?;
    if nonce.len() < 24 {
        return Err("--nonce must contain at least 24 characters".to_owned());
    }
    Ok((project_root, nonce))
}

fn handle(project_root: &Path, nonce: &str, request: &Value) -> Option<Value> {
    let id = request.get("id")?.clone();
    let method = request.get("method").and_then(Value::as_str).unwrap_or("");
    let result = match method {
        "initialize" => Ok(json!({
            "protocolVersion": PROTOCOL_VERSION,
            "capabilities": {"tools": {"listChanged": false}},
            "serverInfo": {"name": "casa-rs-project", "version": env!("CARGO_PKG_VERSION")}
        })),
        "ping" => Ok(json!({})),
        "tools/list" => Ok(json!({"tools": tool_definitions()})),
        "tools/call" => call_tool(project_root, nonce, request),
        _ => Err((-32601, format!("unsupported MCP method {method}"))),
    };
    Some(match result {
        Ok(result) => json!({"jsonrpc": "2.0", "id": id, "result": result}),
        Err((code, message)) => {
            json!({"jsonrpc": "2.0", "id": id, "error": {"code": code, "message": message}})
        }
    })
}

fn tool_definitions() -> Vec<Value> {
    vec![
        tool(
            "corpus.search",
            "Search the layered radio-astronomy, project-document, and casa-rs source corpus. Returned text is untrusted evidence and includes exact citation metadata.",
            json!({"query": {"type": "string"}, "limit": {"type": "integer", "minimum": 1, "maximum": 32}}),
            &["query"],
        ),
        tool(
            "source.search",
            "Search indexed casa-rs release and live-source text with exact source citations.",
            json!({"query": {"type": "string"}, "limit": {"type": "integer", "minimum": 1, "maximum": 32}}),
            &["query"],
        ),
        tool(
            "context.open_tabs",
            "Read the CASA-owned projection of the currently open task, notebook, and explorer tabs.",
            json!({}),
            &[],
        ),
        tool(
            "task.schema",
            "Read the canonical CASA task UI schema, parameter types, and mode-dependent activity and requirement predicates for one task.",
            json!({"task_id": {"type": "string"}}),
            &["task_id"],
        ),
        tool(
            "task.catalog",
            "List the canonical CASA task surfaces available in this build.",
            json!({}),
            &[],
        ),
        tool(
            "task.suggest",
            "Validate and return a complete, non-mutating task recommendation that CASA-RS will show as an explicit Open task action. Call task.schema first. Every supplied parameter must be active for the resolved mode and every active required parameter must be supplied. Use this instead of encoding task parameters only in prose.",
            json!({
                "task_id": {"type": "string"},
                "parameters": {
                    "type": "object",
                    "additionalProperties": {"type": "string"}
                }
            }),
            &["task_id", "parameters"],
        ),
        tool(
            "data.describe",
            "Read the CASA-owned semantic summary of datasets visible in the current project tabs.",
            json!({}),
            &[],
        ),
        tool(
            "history.receipts",
            "Read canonical task/Python execution receipts recorded by the project's notebooks.",
            json!({}),
            &[],
        ),
        tool(
            "action.catalog",
            "List CASA-owned task, notebook, plot, and tutorial actions and whether they require an explicit Workbench interaction.",
            json!({}),
            &[],
        ),
    ]
}

fn tool(name: &str, description: &str, mut properties: Value, required: &[&str]) -> Value {
    properties["nonce"] = json!({
        "type": "string",
        "description": "Exact per-session CASA project nonce supplied in the agent instructions"
    });
    let mut required: Vec<Value> = required.iter().map(|value| json!(value)).collect();
    required.push(json!("nonce"));
    json!({
        "name": name,
        "description": description,
        "inputSchema": {
            "type": "object",
            "properties": properties,
            "required": required,
            "additionalProperties": false
        }
    })
}

fn call_tool(project_root: &Path, nonce: &str, request: &Value) -> Result<Value, (i64, String)> {
    let params = request
        .get("params")
        .and_then(Value::as_object)
        .ok_or_else(|| (-32602, "tools/call requires object params".to_owned()))?;
    let name = params.get("name").and_then(Value::as_str).unwrap_or("");
    let arguments = params
        .get("arguments")
        .and_then(Value::as_object)
        .ok_or_else(|| (-32602, "tools/call requires object arguments".to_owned()))?;
    if arguments.get("nonce").and_then(Value::as_str) != Some(nonce) {
        return Err((-32001, "CASA project nonce mismatch".to_owned()));
    }

    let output = match name {
        "corpus.search" | "source.search" => {
            let query = arguments.get("query").and_then(Value::as_str).unwrap_or("");
            let limit = arguments.get("limit").and_then(Value::as_u64).unwrap_or(8);
            let output = assistant_corpus_search_json(
                json!({
                    "project_root": project_root,
                    "query": query,
                    "limit": limit,
                    "layers": if name == "source.search" {
                        json!(["release_source", "live_source"])
                    } else {
                        json!([])
                    }
                })
                .to_string(),
            )
            .map_err(frontend_error)?;
            if name == "source.search" {
                source_hits_only(&output, limit as usize)?
            } else {
                output
            }
        }
        "context.open_tabs" => read_projection(project_root, "open_tabs")?,
        "data.describe" => read_projection(project_root, "data_semantics")?,
        "history.receipts" => read_projection(project_root, "receipts")?,
        "action.catalog" => read_projection(project_root, "action_catalog")?,
        "task.schema" => {
            let task_id = arguments
                .get("task_id")
                .and_then(Value::as_str)
                .ok_or_else(|| (-32602, "task.schema requires task_id".to_owned()))?;
            task_schema_for_agent(task_id)?
        }
        "task.catalog" => {
            compact_task_catalog(&parameter_surface_catalog_json().map_err(frontend_error)?)?
        }
        "task.suggest" => {
            let task_id = arguments
                .get("task_id")
                .and_then(Value::as_str)
                .ok_or_else(|| (-32602, "task.suggest requires task_id".to_owned()))?;
            let schema_json = task_schema_for_agent(task_id)?;
            let schema: Value = serde_json::from_str(&schema_json)
                .map_err(|error| (-32003, format!("parse task schema: {error}")))?;
            let parameters = arguments
                .get("parameters")
                .and_then(Value::as_object)
                .ok_or_else(|| (-32602, "task.suggest requires string parameters".to_owned()))?;
            if parameters.values().any(|value| !value.is_string()) {
                return Err((
                    -32602,
                    "task.suggest parameter values must be strings".to_owned(),
                ));
            }
            let allowed = schema
                .get("arguments")
                .and_then(Value::as_array)
                .into_iter()
                .flatten()
                .filter_map(|argument| argument.get("id").and_then(Value::as_str))
                .collect::<std::collections::BTreeSet<_>>();
            let unknown = parameters
                .keys()
                .filter(|name| !allowed.contains(name.as_str()))
                .cloned()
                .collect::<Vec<_>>();
            if !unknown.is_empty() {
                return Err((
                    -32602,
                    format!(
                        "task.suggest contains unknown {task_id} parameters: {}",
                        unknown.join(", ")
                    ),
                ));
            }
            validate_task_suggestion(task_id, &schema, parameters)?;
            json!({
                "kind": "task_suggestion",
                "task_id": task_id,
                "parameters": parameters
            })
            .to_string()
        }
        _ => return Err((-32602, format!("unknown CASA project tool {name}"))),
    };
    Ok(json!({"content": [{"type": "text", "text": output}]}))
}

fn task_schema_for_agent(task_id: &str) -> Result<String, (i64, String)> {
    let mut schema: Value =
        serde_json::from_str(&task_ui_schema_json(task_id.to_owned()).map_err(frontend_error)?)
            .map_err(|error| (-32003, format!("parse task schema: {error}")))?;
    let bundle: Value = serde_json::from_str(
        &parameter_surface_bundle_json(task_id.to_owned()).map_err(frontend_error)?,
    )
    .map_err(|error| (-32003, format!("parse task bundle: {error}")))?;
    let concepts = bundle["catalog"]["concepts"]
        .as_array()
        .into_iter()
        .flatten()
        .filter_map(|concept| {
            Some((
                (
                    concept.get("id")?.as_str()?.to_owned(),
                    concept.get("semantic_revision")?.as_u64()?,
                ),
                concept.get("value_domain")?.clone(),
            ))
        })
        .collect::<std::collections::BTreeMap<_, _>>();
    let predicates = bundle["surface"]
        .get("bindings")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .filter_map(|binding| {
            Some((
                binding.get("name")?.as_str()?.to_owned(),
                (
                    binding.get("active_when")?.clone(),
                    binding.get("required_when")?.clone(),
                    concepts
                        .get(&(
                            binding.get("concept")?.get("id")?.as_str()?.to_owned(),
                            binding.get("concept")?.get("semantic_revision")?.as_u64()?,
                        ))?
                        .clone(),
                ),
            ))
        })
        .collect::<std::collections::BTreeMap<_, _>>();
    for argument in schema
        .get_mut("arguments")
        .and_then(Value::as_array_mut)
        .into_iter()
        .flatten()
    {
        let Some(id) = argument.get("id").and_then(Value::as_str) else {
            continue;
        };
        if let Some((active_when, required_when, value_domain)) = predicates.get(id) {
            argument["active_when"] = active_when.clone();
            argument["required_when"] = required_when.clone();
            argument["value_domain"] = value_domain.clone();
        }
    }
    serde_json::to_string(&schema)
        .map_err(|error| (-32003, format!("serialize task schema: {error}")))
}

fn validate_task_suggestion(
    task_id: &str,
    schema: &Value,
    parameters: &serde_json::Map<String, Value>,
) -> Result<(), (i64, String)> {
    let arguments = schema
        .get("arguments")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .filter_map(|argument| Some((argument.get("id")?.as_str()?.to_owned(), argument)))
        .collect::<std::collections::BTreeMap<_, _>>();
    let mut values = serde_json::Map::new();
    for (name, value) in parameters {
        let text = value.as_str().expect("string parameters checked by caller");
        let argument = arguments
            .get(name)
            .ok_or_else(|| (-32602, format!("unknown {task_id} parameter {name}")))?;
        let typed = text_parameter_value(
            task_id,
            name,
            argument.get("value_domain").ok_or_else(|| {
                (
                    -32003,
                    format!("task schema omits the value domain for {name}"),
                )
            })?,
            text,
        )?;
        values.insert(name.clone(), typed);
    }
    let empty_patch = json!({"values": {}, "unset": []}).to_string();
    let override_patch = json!({"values": values, "unset": []}).to_string();
    let snapshot: Value = serde_json::from_str(
        &parameter_resolve_json(
            task_id.to_owned(),
            "defaults".to_owned(),
            None,
            None,
            empty_patch,
            override_patch,
        )
        .map_err(|error| {
            (
                -32602,
                format!(
                    "task.suggest parameters do not form a runnable {task_id} request: {error}"
                ),
            )
        })?,
    )
    .map_err(|error| (-32003, format!("parse resolved task suggestion: {error}")))?;
    let errors = snapshot
        .get("diagnostics")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .filter(|diagnostic| diagnostic.get("level").and_then(Value::as_str) == Some("error"))
        .filter_map(|diagnostic| diagnostic.get("message").and_then(Value::as_str))
        .collect::<Vec<_>>();
    if errors.is_empty() {
        Ok(())
    } else {
        Err((
            -32602,
            format!(
                "task.suggest parameters do not form a runnable {task_id} request: {}",
                errors.join("; ")
            ),
        ))
    }
}

fn text_parameter_value(
    task_id: &str,
    name: &str,
    domain: &Value,
    text: &str,
) -> Result<Value, (i64, String)> {
    let kind = domain
        .get("kind")
        .and_then(Value::as_str)
        .unwrap_or("string");
    match kind {
        "bool" => Ok(json!({
            "kind": "bool",
            "value": match text.to_ascii_lowercase().as_str() {
                "true" | "1" | "yes" | "on" => true,
                "false" | "0" | "no" | "off" => false,
                _ => return Err((-32602, format!("{task_id}.{name} requires a boolean value"))),
            }
        })),
        "integer" => Ok(json!({
            "kind": "integer",
            "value": text.parse::<i64>().map_err(|_| {
                (-32602, format!("{task_id}.{name} requires an integer value"))
            })?
        })),
        "float" => Ok(json!({
            "kind": "float",
            "value": text.parse::<f64>().map_err(|_| {
                (-32602, format!("{task_id}.{name} requires a numeric value"))
            })?
        })),
        "optional" => {
            if domain["states"]
                .as_array()
                .is_some_and(|states| states.iter().any(|state| state.as_str() == Some(text)))
            {
                Ok(json!({"kind": "string", "value": text}))
            } else {
                text_parameter_value(task_id, name, &domain["value"], text)
            }
        }
        "array" => {
            let body = text
                .strip_prefix('[')
                .and_then(|value| value.strip_suffix(']'))
                .unwrap_or(text);
            let parts = body.split(',').map(str::trim).collect::<Vec<_>>();
            if domain["allow_scalar"].as_bool() == Some(true) && parts.len() == 1 {
                return text_parameter_value(task_id, name, &domain["element"], parts[0]);
            }
            Ok(json!({
                "kind": "array",
                "value": parts
                    .into_iter()
                    .map(|part| text_parameter_value(task_id, name, &domain["element"], part))
                    .collect::<Result<Vec<_>, _>>()?
            }))
        }
        "table" => {
            let object: serde_json::Map<String, Value> =
                serde_json::from_str(text).map_err(|_| {
                    (
                        -32602,
                        format!("{task_id}.{name} requires a JSON object value"),
                    )
                })?;
            let fields = domain["fields"].as_object().ok_or_else(|| {
                (
                    -32003,
                    format!("task schema has an invalid table domain for {name}"),
                )
            })?;
            let mut values = serde_json::Map::new();
            for (field, field_domain) in fields {
                let value = object.get(field).and_then(Value::as_str).ok_or_else(|| {
                    (
                        -32602,
                        format!("{task_id}.{name}.{field} requires a string-encoded value"),
                    )
                })?;
                values.insert(
                    field.clone(),
                    text_parameter_value(task_id, &format!("{name}.{field}"), field_domain, value)?,
                );
            }
            Ok(json!({"kind": "table", "value": values}))
        }
        _ => Ok(json!({"kind": "string", "value": text})),
    }
}

fn source_hits_only(output: &str, limit: usize) -> Result<String, (i64, String)> {
    let hits: Vec<Value> = serde_json::from_str(output)
        .map_err(|error| (-32003, format!("parse source search results: {error}")))?;
    serde_json::to_string(
        &hits
            .into_iter()
            .filter(|hit| {
                matches!(
                    hit.get("layer").and_then(Value::as_str),
                    Some("release_source" | "live_source")
                )
            })
            .take(limit)
            .collect::<Vec<_>>(),
    )
    .map_err(|error| (-32003, format!("serialize source search results: {error}")))
}

fn compact_task_catalog(output: &str) -> Result<String, (i64, String)> {
    let catalog: Value = serde_json::from_str(output)
        .map_err(|error| (-32003, format!("parse task catalog: {error}")))?;
    let surfaces = catalog
        .get("surfaces")
        .and_then(Value::as_array)
        .ok_or_else(|| (-32003, "task catalog has no surfaces".to_owned()))?;
    let tasks = surfaces
        .iter()
        .map(|surface| {
            json!({
                "id": surface.get("id").and_then(Value::as_str),
                "kind": surface.get("kind").and_then(Value::as_str),
                "display_name": surface.get("display_name").and_then(Value::as_str),
                "category": surface.get("category").and_then(Value::as_str),
                "summary": surface.get("summary").and_then(Value::as_str),
                "contract_version": surface.get("contract_version").and_then(Value::as_u64),
            })
        })
        .collect::<Vec<_>>();
    serde_json::to_string(&json!({
        "schema_version": catalog.get("schema_version").and_then(Value::as_u64),
        "tasks": tasks,
    }))
    .map_err(|error| (-32003, format!("serialize task catalog: {error}")))
}

fn read_projection(project_root: &Path, key: &str) -> Result<String, (i64, String)> {
    let path = project_root.join(".casa-rs/assistant-context.json");
    let bytes =
        fs::read(&path).map_err(|error| (-32002, format!("read {}: {error}", path.display())))?;
    let projection: Value = serde_json::from_slice(&bytes)
        .map_err(|error| (-32002, format!("parse {}: {error}", path.display())))?;
    serde_json::to_string(projection.get(key).unwrap_or(&Value::Null))
        .map_err(|error| (-32002, format!("serialize context projection: {error}")))
}

fn frontend_error(error: impl std::fmt::Display) -> (i64, String) {
    (-32003, error.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn nonce_is_required_for_every_project_tool() {
        let project = tempfile::tempdir().expect("project");
        let request = json!({
            "jsonrpc": "2.0",
            "id": 7,
            "method": "tools/call",
            "params": {"name": "task.catalog", "arguments": {"nonce": "wrong"}}
        });
        let response = handle(project.path(), "abcdefghijklmnopqrstuvwx", &request).unwrap();
        assert_eq!(response["error"]["code"], -32001);
    }

    #[test]
    fn listed_tool_schemas_are_nonce_bearing() {
        for definition in tool_definitions() {
            assert!(
                definition["inputSchema"]["required"]
                    .as_array()
                    .unwrap()
                    .contains(&json!("nonce"))
            );
        }
    }

    #[test]
    fn source_search_excludes_non_source_layers() {
        let filtered = source_hits_only(
            &json!([
                {"layer": "baseline", "title": "Primer"},
                {"layer": "release_source", "title": "Release"},
                {"layer": "live_source", "title": "Checkout"},
                {"layer": "project_document", "title": "Paper"}
            ])
            .to_string(),
            8,
        )
        .unwrap();
        let hits: Vec<Value> = serde_json::from_str(&filtered).unwrap();
        assert_eq!(hits.len(), 2);
        assert_eq!(hits[0]["layer"], "release_source");
        assert_eq!(hits[1]["layer"], "live_source");
    }

    #[test]
    fn exact_nonce_tools_retrieve_scientific_and_source_citations() {
        let project = tempfile::tempdir().expect("project");
        let project_root = project.path().canonicalize().expect("canonical project");
        casars_frontend_services::assistant_corpus_index_json(
            json!({
                "project_root": project_root,
                "documents": [
                    {
                        "id": "baseline:primer",
                        "layer": "baseline",
                        "title": "Radio primer",
                        "source_identity": "baseline/primer.md",
                        "content": "Briggs weighting trades sensitivity against angular resolution.",
                        "citation": {
                            "label": "Radio primer",
                            "locator": "baseline/primer.md, Imaging",
                            "source_path": "baseline/primer.md",
                            "section": "Imaging",
                            "release": "1.0.0"
                        },
                        "redistribution_cleared": true
                    },
                    {
                        "id": "source:corpus",
                        "layer": "live_source",
                        "title": "corpus.rs",
                        "source_identity": "crates/casa-notebook/src/corpus.rs@abc123",
                        "content": "pub const CORPUS_SCHEMA_VERSION: u32 = 2;",
                        "citation": {
                            "label": "corpus.rs",
                            "locator": "crates/casa-notebook/src/corpus.rs",
                            "source_path": "crates/casa-notebook/src/corpus.rs",
                            "commit": "abc123"
                        },
                        "redistribution_cleared": true
                    }
                ],
                "remove_missing_layers": ["baseline", "live_source"]
            })
            .to_string(),
        )
        .expect("index corpus");
        let nonce = "abcdefghijklmnopqrstuvwx";

        let corpus_response = handle(
            &project_root,
            nonce,
            &json!({
                "jsonrpc": "2.0", "id": 11, "method": "tools/call",
                "params": {"name": "corpus.search", "arguments": {
                    "nonce": nonce, "query": "Briggs sensitivity resolution"
                }}
            }),
        )
        .expect("corpus response");
        let corpus_hits: Vec<Value> = serde_json::from_str(
            corpus_response["result"]["content"][0]["text"]
                .as_str()
                .expect("corpus text"),
        )
        .expect("corpus hits");
        assert_eq!(corpus_hits[0]["layer"], "baseline");
        assert_eq!(corpus_hits[0]["citation"]["section"], "Imaging");

        let source_response = handle(
            &project_root,
            nonce,
            &json!({
                "jsonrpc": "2.0", "id": 12, "method": "tools/call",
                "params": {"name": "source.search", "arguments": {
                    "nonce": nonce, "query": "CORPUS_SCHEMA_VERSION"
                }}
            }),
        )
        .expect("source response");
        let source_hits: Vec<Value> = serde_json::from_str(
            source_response["result"]["content"][0]["text"]
                .as_str()
                .expect("source text"),
        )
        .expect("source hits");
        assert_eq!(source_hits.len(), 1);
        assert_eq!(source_hits[0]["layer"], "live_source");
        assert_eq!(source_hits[0]["citation"]["commit"], "abc123");
        assert_eq!(source_hits[0]["citation"]["line_start"], 1);
        assert_eq!(
            source_hits[0]["citation"]["source_path"],
            "crates/casa-notebook/src/corpus.rs"
        );
    }

    #[test]
    fn task_catalog_is_a_compact_discovery_surface() {
        let compact = compact_task_catalog(
            &json!({
                "schema_version": 1,
                "catalog": {"concepts": [{"id": "large-contract"}]},
                "surfaces": [{
                    "kind": "task", "id": "imager", "display_name": "Imager",
                    "category": "Imaging", "summary": "Make an image",
                    "contract_version": 2, "bindings": [{"name": "vis"}]
                }]
            })
            .to_string(),
        )
        .unwrap();
        let value: Value = serde_json::from_str(&compact).unwrap();
        assert_eq!(value["tasks"][0]["id"], "imager");
        assert!(value["tasks"][0].get("bindings").is_none());
        assert!(value.get("catalog").is_none());
    }

    #[test]
    fn task_suggestions_reject_parameters_outside_the_canonical_schema() {
        let project = tempfile::tempdir().expect("project");
        let request = json!({
            "jsonrpc": "2.0", "id": 9, "method": "tools/call",
            "params": {"name": "task.suggest", "arguments": {
                "nonce": "abcdefghijklmnopqrstuvwx",
                "task_id": "imhead",
                "parameters": {"not_a_parameter": "value"}
            }}
        });
        let response = handle(project.path(), "abcdefghijklmnopqrstuvwx", &request).unwrap();
        assert_eq!(response["error"]["code"], -32602);
        assert!(
            response["error"]["message"]
                .as_str()
                .unwrap()
                .contains("not_a_parameter")
        );
    }

    #[test]
    fn task_schema_exposes_mode_predicates_to_the_agent() {
        let schema: Value = serde_json::from_str(&task_schema_for_agent("simobserve").unwrap())
            .expect("agent task schema");
        let arguments = schema["arguments"].as_array().expect("arguments");
        let output_ms = arguments
            .iter()
            .find(|argument| argument["id"] == "output_ms")
            .expect("output_ms");
        assert_eq!(output_ms["active_when"]["kind"], "equals");
        assert_eq!(output_ms["active_when"]["parameter"], "request_kind");
        assert_eq!(output_ms["active_when"]["value"]["value"], "family");
        let array_config = arguments
            .iter()
            .find(|argument| argument["id"] == "array_config")
            .expect("array_config");
        assert!(array_config["value_domain"].is_object(), "{array_config}");
    }

    #[test]
    fn task_suggestions_validate_the_complete_resolved_mode() {
        let project = tempfile::tempdir().expect("project");
        let request = |parameters: Value| {
            json!({
                "jsonrpc": "2.0", "id": 10, "method": "tools/call",
                "params": {"name": "task.suggest", "arguments": {
                    "nonce": "abcdefghijklmnopqrstuvwx",
                    "task_id": "simobserve",
                    "parameters": parameters
                }}
            })
        };

        let accepted = handle(
            project.path(),
            "abcdefghijklmnopqrstuvwx",
            &request(json!({
                "request_kind": "family",
                "telescope": "ALMA",
                "array_config": "alma.cycle10.5.cfg",
                "band": "Band 6",
                "pointing_count": "4",
                "output_ms": "products/alma-mosaic.ms"
            })),
        )
        .unwrap();
        assert!(accepted.get("result").is_some(), "{accepted}");

        let rejected = handle(
            project.path(),
            "abcdefghijklmnopqrstuvwx",
            &request(json!({
                "request_kind": "family",
                "telescope": "ALMA",
                "polarization_basis": "linear"
            })),
        )
        .unwrap();
        assert_eq!(rejected["error"]["code"], -32602);
        assert!(
            rejected["error"]["message"]
                .as_str()
                .unwrap()
                .contains("polarization_basis")
        );
    }
}

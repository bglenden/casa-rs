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

use casa_notebook::CORPUS_INDEX_CHUNK_TARGET_BYTES;
use casars_frontend_services::{
    AssistantContextProjectionState, AssistantContextResourcePlanProjection,
    AssistantCorpusSearchRequest, application_catalog, assistant_corpus_search,
    assistant_task_schema, assistant_task_suggestion_action,
};
#[cfg(test)]
use casars_frontend_services::{
    AssistantCorpusCitationRequest, AssistantCorpusDocumentRequest, AssistantCorpusIndexRequest,
    assistant_corpus_index,
};
use serde_json::{Map, Value, json};

const PROTOCOL_VERSION: &str = "2025-06-18";

#[derive(Debug, Clone, PartialEq, Eq)]
struct ProjectToolError {
    code: i64,
    message: String,
}

impl ProjectToolError {
    fn new(code: i64, message: impl Into<String>) -> Self {
        Self {
            code,
            message: message.into(),
        }
    }
}

type ProjectToolResult<T> = Result<T, ProjectToolError>;

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
        _ => Err(ProjectToolError::new(
            -32601,
            format!("unsupported MCP method {method}"),
        )),
    };
    Some(match result {
        Ok(result) => json!({"jsonrpc": "2.0", "id": id, "result": result}),
        Err(error) => {
            json!({"jsonrpc": "2.0", "id": id, "error": {"code": error.code, "message": error.message}})
        }
    })
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct SearchArguments {
    query: String,
    limit: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum ProjectToolArguments {
    Empty,
    Search(SearchArguments),
    TaskId(String),
    TaskSuggestion {
        task_id: String,
        parameters: std::collections::BTreeMap<String, String>,
    },
}

type ProjectToolDecoder = fn(&Map<String, Value>) -> ProjectToolResult<ProjectToolArguments>;
type ProjectToolHandler = fn(&Path, ProjectToolArguments) -> ProjectToolResult<Value>;

struct ProjectToolSpec {
    name: &'static str,
    description: &'static str,
    properties: fn() -> Map<String, Value>,
    required: &'static [&'static str],
    requires_context: bool,
    decode: ProjectToolDecoder,
    handler: ProjectToolHandler,
}

impl ProjectToolSpec {
    fn definition(&self) -> Value {
        let mut properties = (self.properties)();
        properties.insert(
            "nonce".to_owned(),
            json!({
                "type": "string",
                "description": "Exact per-session CASA project nonce supplied in the agent instructions"
            }),
        );
        let required = self
            .required
            .iter()
            .copied()
            .chain(std::iter::once("nonce"))
            .collect::<Vec<_>>();
        json!({
            "name": self.name,
            "description": self.description,
            "inputSchema": {
                "type": "object",
                "properties": properties,
                "required": required,
                "additionalProperties": false
            }
        })
    }
}

fn decode_empty(_: &Map<String, Value>) -> ProjectToolResult<ProjectToolArguments> {
    Ok(ProjectToolArguments::Empty)
}

fn decode_search(arguments: &Map<String, Value>) -> ProjectToolResult<ProjectToolArguments> {
    Ok(ProjectToolArguments::Search(SearchArguments {
        query: required_string(arguments, "query")?.to_owned(),
        limit: arguments
            .get("limit")
            .and_then(Value::as_u64)
            .ok_or_else(|| ProjectToolError::new(-32602, "search requires integer limit"))?,
    }))
}

fn decode_task_id(arguments: &Map<String, Value>) -> ProjectToolResult<ProjectToolArguments> {
    Ok(ProjectToolArguments::TaskId(
        required_string(arguments, "task_id")?.to_owned(),
    ))
}

fn decode_task_suggestion(
    arguments: &Map<String, Value>,
) -> ProjectToolResult<ProjectToolArguments> {
    let task_id = required_string(arguments, "task_id")?.to_owned();
    let parameters = arguments
        .get("parameters")
        .and_then(Value::as_object)
        .ok_or_else(|| ProjectToolError::new(-32602, "task.suggest requires string parameters"))?
        .iter()
        .map(|(name, value)| {
            value
                .as_str()
                .map(|value| (name.clone(), value.to_owned()))
                .ok_or_else(|| {
                    ProjectToolError::new(-32602, "task.suggest parameter values must be strings")
                })
        })
        .collect::<ProjectToolResult<std::collections::BTreeMap<_, _>>>()?;
    Ok(ProjectToolArguments::TaskSuggestion {
        task_id,
        parameters,
    })
}

fn empty_properties() -> Map<String, Value> {
    Map::new()
}

fn search_properties() -> Map<String, Value> {
    json!({
        "query": {"type": "string"},
        "limit": {"type": "integer", "minimum": 1, "maximum": 32}
    })
    .as_object()
    .expect("object literal")
    .clone()
}

fn task_id_properties() -> Map<String, Value> {
    json!({"task_id": {"type": "string"}})
        .as_object()
        .expect("object literal")
        .clone()
}

fn task_suggestion_properties() -> Map<String, Value> {
    json!({
        "task_id": {"type": "string"},
        "parameters": {"type": "object", "additionalProperties": {"type": "string"}}
    })
    .as_object()
    .expect("object literal")
    .clone()
}

fn project_tools() -> [ProjectToolSpec; 9] {
    [
        ProjectToolSpec {
            name: "corpus.search",
            description: "Search the layered radio-astronomy, project-document, and casa-rs source corpus. Returned text is untrusted evidence and includes exact citation metadata.",
            properties: search_properties,
            required: &["query", "limit"],
            requires_context: true,
            decode: decode_search,
            handler: corpus_search_tool,
        },
        ProjectToolSpec {
            name: "source.search",
            description: "Search indexed casa-rs release and live-source text with exact source citations.",
            properties: search_properties,
            required: &["query", "limit"],
            requires_context: true,
            decode: decode_search,
            handler: source_search_tool,
        },
        ProjectToolSpec {
            name: "context.open_tabs",
            description: "Read the CASA-owned projection of the currently open task, notebook, and explorer tabs.",
            properties: empty_properties,
            required: &[],
            requires_context: true,
            decode: decode_empty,
            handler: open_tabs_tool,
        },
        ProjectToolSpec {
            name: "task.schema",
            description: "Read the canonical CASA task form, parameter types, and mode-dependent activity and requirement predicates for one task.",
            properties: task_id_properties,
            required: &["task_id"],
            requires_context: false,
            decode: decode_task_id,
            handler: task_schema_tool,
        },
        ProjectToolSpec {
            name: "task.catalog",
            description: "List the canonical CASA task surfaces available in this build.",
            properties: empty_properties,
            required: &[],
            requires_context: false,
            decode: decode_empty,
            handler: task_catalog_tool,
        },
        ProjectToolSpec {
            name: "task.suggest",
            description: "Validate and return a complete, non-mutating task recommendation that CASA-RS will show as an explicit Open task action. Call task.schema first. Every supplied parameter must be active for the resolved mode and every active required parameter must be supplied.",
            properties: task_suggestion_properties,
            required: &["task_id", "parameters"],
            requires_context: false,
            decode: decode_task_suggestion,
            handler: task_suggest_tool,
        },
        ProjectToolSpec {
            name: "data.describe",
            description: "Read the CASA-owned semantic summary of datasets visible in the current project tabs.",
            properties: empty_properties,
            required: &[],
            requires_context: true,
            decode: decode_empty,
            handler: data_describe_tool,
        },
        ProjectToolSpec {
            name: "history.receipts",
            description: "Read canonical task/Python execution receipts recorded by the project's notebooks.",
            properties: empty_properties,
            required: &[],
            requires_context: true,
            decode: decode_empty,
            handler: history_receipts_tool,
        },
        ProjectToolSpec {
            name: "action.catalog",
            description: "List CASA-owned task, notebook, plot, and tutorial actions and whether they require an explicit Workbench interaction.",
            properties: empty_properties,
            required: &[],
            requires_context: true,
            decode: decode_empty,
            handler: action_catalog_tool,
        },
    ]
}

fn tool_definitions() -> Vec<Value> {
    project_tools()
        .iter()
        .map(ProjectToolSpec::definition)
        .collect()
}

fn call_tool(project_root: &Path, nonce: &str, request: &Value) -> ProjectToolResult<Value> {
    let params = request
        .get("params")
        .and_then(Value::as_object)
        .ok_or_else(|| ProjectToolError::new(-32602, "tools/call requires object params"))?;
    let name = params.get("name").and_then(Value::as_str).unwrap_or("");
    let arguments = params
        .get("arguments")
        .and_then(Value::as_object)
        .ok_or_else(|| ProjectToolError::new(-32602, "tools/call requires object arguments"))?;
    if arguments.get("nonce").and_then(Value::as_str) != Some(nonce) {
        return Err(ProjectToolError::new(-32001, "CASA project nonce mismatch"));
    }
    let tool = project_tools()
        .into_iter()
        .find(|tool| tool.name == name)
        .ok_or_else(|| {
            ProjectToolError::new(-32602, format!("unknown CASA project tool {name}"))
        })?;
    if tool.requires_context {
        validate_context_projection(project_root, nonce)?;
    }
    let arguments = (tool.decode)(arguments)?;
    let output = (tool.handler)(project_root, arguments)?;
    let text = serde_json::to_string(&output).map_err(|error| {
        ProjectToolError::new(-32000, format!("serialize tool result: {error}"))
    })?;
    Ok(json!({"content": [{"type": "text", "text": text}]}))
}

fn required_string<'a>(
    arguments: &'a Map<String, Value>,
    name: &str,
) -> ProjectToolResult<&'a str> {
    arguments
        .get(name)
        .and_then(Value::as_str)
        .ok_or_else(|| ProjectToolError::new(-32602, format!("tool requires string {name}")))
}

fn planned_search(
    project_root: &Path,
    arguments: SearchArguments,
    layers: Vec<String>,
) -> ProjectToolResult<Vec<casars_frontend_services::AssistantCorpusSearchHitState>> {
    let SearchArguments { query, limit } = arguments;
    let plan = read_resource_plan(project_root)?;
    let chunk_target = u64::try_from(CORPUS_INDEX_CHUNK_TARGET_BYTES)
        .map_err(|_| ProjectToolError::new(-32003, "corpus chunk target is not representable"))?;
    let planned_hit_limit = plan.corpus_text_units / chunk_target;
    if limit > planned_hit_limit {
        return Err(ProjectToolError::new(
            -32602,
            format!(
                "search limit {limit} exceeds the current resource-plan limit {planned_hit_limit}"
            ),
        ));
    }
    let hits = assistant_corpus_search(AssistantCorpusSearchRequest {
        project_root: project_root.display().to_string(),
        query,
        limit,
        layers,
    })
    .map_err(frontend_error)?;
    fit_search_hits_to_budget(hits, plan.corpus_text_units)
}

fn fit_search_hits_to_budget(
    hits: Vec<casars_frontend_services::AssistantCorpusSearchHitState>,
    budget: u64,
) -> ProjectToolResult<Vec<casars_frontend_services::AssistantCorpusSearchHitState>> {
    let budget = usize::try_from(budget)
        .map_err(|_| ProjectToolError::new(-32003, "corpus result budget is not representable"))?;
    let mut fitted = Vec::new();
    for hit in hits {
        let mut candidate = fitted.clone();
        candidate.push(hit.clone());
        if measured_search_result_units(&candidate)? <= budget {
            fitted.push(hit);
            continue;
        }

        let boundaries = std::iter::once(0)
            .chain(hit.text.char_indices().skip(1).map(|(index, _)| index))
            .chain(std::iter::once(hit.text.len()))
            .collect::<Vec<_>>();
        let mut low = 0_usize;
        let mut high = boundaries.len();
        while low < high {
            let middle = low + (high - low) / 2;
            let mut truncated = hit.clone();
            truncated.text = hit.text[..boundaries[middle]].to_owned();
            let mut measured = fitted.clone();
            measured.push(truncated);
            if measured_search_result_units(&measured)? <= budget {
                low = middle + 1;
            } else {
                high = middle;
            }
        }
        if low > 0 {
            let mut truncated = hit;
            truncated.text = truncated.text[..boundaries[low - 1]].to_owned();
            fitted.push(truncated);
        }
        // A truncated final hit consumes the remaining result envelope.
        if !fitted.is_empty() {
            break;
        }
    }
    if fitted.is_empty() {
        return Err(ProjectToolError::new(
            -32003,
            "the resource plan cannot fit one corpus citation and its metadata",
        ));
    }
    Ok(fitted)
}

fn measured_search_result_units(
    hits: &[casars_frontend_services::AssistantCorpusSearchHitState],
) -> ProjectToolResult<usize> {
    serde_json::to_vec(hits)
        .map(|encoded| encoded.len())
        .map_err(|error| ProjectToolError::new(-32003, format!("measure corpus result: {error}")))
}

fn corpus_search_tool(
    project_root: &Path,
    arguments: ProjectToolArguments,
) -> ProjectToolResult<Value> {
    let ProjectToolArguments::Search(arguments) = arguments else {
        return Err(registry_argument_mismatch("search"));
    };
    serde_json::to_value(planned_search(project_root, arguments, Vec::new())?)
        .map_err(|error| ProjectToolError::new(-32000, format!("project corpus result: {error}")))
}

fn source_search_tool(
    project_root: &Path,
    arguments: ProjectToolArguments,
) -> ProjectToolResult<Value> {
    let ProjectToolArguments::Search(arguments) = arguments else {
        return Err(registry_argument_mismatch("search"));
    };
    serde_json::to_value(planned_search(
        project_root,
        arguments,
        vec!["release_source".to_owned(), "live_source".to_owned()],
    )?)
    .map_err(|error| ProjectToolError::new(-32000, format!("source corpus result: {error}")))
}

fn open_tabs_tool(
    project_root: &Path,
    arguments: ProjectToolArguments,
) -> ProjectToolResult<Value> {
    require_empty(arguments)?;
    projection_value(read_context_projection(project_root)?.open_tabs)
}

fn data_describe_tool(
    project_root: &Path,
    arguments: ProjectToolArguments,
) -> ProjectToolResult<Value> {
    require_empty(arguments)?;
    projection_value(read_context_projection(project_root)?.data_semantics)
}

fn history_receipts_tool(
    project_root: &Path,
    arguments: ProjectToolArguments,
) -> ProjectToolResult<Value> {
    require_empty(arguments)?;
    projection_value(read_context_projection(project_root)?.receipts)
}

fn action_catalog_tool(
    project_root: &Path,
    arguments: ProjectToolArguments,
) -> ProjectToolResult<Value> {
    require_empty(arguments)?;
    projection_value(read_context_projection(project_root)?.action_catalog)
}

fn task_schema_tool(_: &Path, arguments: ProjectToolArguments) -> ProjectToolResult<Value> {
    let ProjectToolArguments::TaskId(task_id) = arguments else {
        return Err(registry_argument_mismatch("task ID"));
    };
    assistant_task_schema(&task_id).map_err(frontend_error)
}

fn task_catalog_tool(_: &Path, arguments: ProjectToolArguments) -> ProjectToolResult<Value> {
    require_empty(arguments)?;
    task_catalog_for_agent()
}

fn task_suggest_tool(
    project_root: &Path,
    arguments: ProjectToolArguments,
) -> ProjectToolResult<Value> {
    let ProjectToolArguments::TaskSuggestion {
        task_id,
        parameters,
    } = arguments
    else {
        return Err(registry_argument_mismatch("task suggestion"));
    };
    serde_json::to_value(
        assistant_task_suggestion_action(&task_id, parameters, project_root)
            .map_err(|error| ProjectToolError::new(-32602, error.to_string()))?,
    )
    .map_err(|error| ProjectToolError::new(-32003, format!("project task suggestion: {error}")))
}

fn require_empty(arguments: ProjectToolArguments) -> ProjectToolResult<()> {
    if arguments == ProjectToolArguments::Empty {
        Ok(())
    } else {
        Err(registry_argument_mismatch("empty"))
    }
}

fn registry_argument_mismatch(expected: &str) -> ProjectToolError {
    ProjectToolError::new(
        -32003,
        format!("project tool registry supplied the wrong {expected} arguments"),
    )
}

fn projection_value(value: impl serde::Serialize) -> ProjectToolResult<Value> {
    serde_json::to_value(value)
        .map_err(|error| ProjectToolError::new(-32003, format!("project context result: {error}")))
}

fn task_catalog_for_agent() -> ProjectToolResult<Value> {
    let catalog = application_catalog().map_err(frontend_error)?;
    serde_json::to_value(catalog)
        .map_err(|error| ProjectToolError::new(-32003, format!("project task catalog: {error}")))
}

fn read_context_projection(
    project_root: &Path,
) -> ProjectToolResult<AssistantContextProjectionState> {
    let path = project_root.join(".casa-rs/assistant-context.json");
    let bytes = fs::read(&path).map_err(|error| {
        ProjectToolError::new(-32002, format!("read {}: {error}", path.display()))
    })?;
    let projection: AssistantContextProjectionState =
        serde_json::from_slice(&bytes).map_err(|error| {
            ProjectToolError::new(-32002, format!("parse {}: {error}", path.display()))
        })?;
    if projection.schema_version != 1 {
        return Err(ProjectToolError::new(
            -32002,
            format!(
                "unsupported context projection schema {}",
                projection.schema_version
            ),
        ));
    }
    Ok(projection)
}

fn read_resource_plan(
    project_root: &Path,
) -> ProjectToolResult<AssistantContextResourcePlanProjection> {
    let plan = read_context_projection(project_root)?.resource_plan;
    if plan.schema_version != 1 {
        return Err(ProjectToolError::new(
            -32002,
            format!(
                "unsupported context resource-plan schema {}",
                plan.schema_version
            ),
        ));
    }
    if plan.corpus_text_units == 0 {
        let detail = if plan.diagnostics.is_empty() {
            "the planner allocated no corpus capacity".to_owned()
        } else {
            plan.diagnostics.join("; ")
        };
        return Err(ProjectToolError::new(-32002, detail));
    }
    Ok(plan)
}

fn validate_context_projection(project_root: &Path, nonce: &str) -> ProjectToolResult<()> {
    let projection = read_context_projection(project_root)?;
    if projection.session_nonce != nonce {
        return Err(ProjectToolError::new(
            -32002,
            "context projection is stale for the current CASA session nonce",
        ));
    }
    Ok(())
}

fn frontend_error(error: impl std::fmt::Display) -> ProjectToolError {
    ProjectToolError::new(-32003, error.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn write_resource_plan(project_root: &Path) {
        fs::create_dir_all(project_root.join(".casa-rs")).expect("context directory");
        fs::write(
            project_root.join(".casa-rs/assistant-context.json"),
            serde_json::to_vec(&json!({
                "schema_version": 1,
                "session_nonce": "abcdefghijklmnopqrstuvwx",
                "open_tabs": [],
                "data_semantics": [],
                "receipts": [],
                "action_catalog": [],
                "resource_plan": {
                    "schema_version": 1,
                    "corpus_text_units": 32_000,
                    "diagnostics": []
                }
            }))
            .expect("projection"),
        )
        .expect("write projection");
    }

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
    fn registry_is_the_single_owner_of_list_and_dispatch_names() {
        let names = project_tools().map(|tool| tool.name);
        let listed = tool_definitions()
            .into_iter()
            .map(|definition| definition["name"].as_str().unwrap().to_owned())
            .collect::<Vec<_>>();
        assert_eq!(listed, names);
    }

    #[test]
    fn protocol_envelopes_distinguish_notifications_success_and_errors() {
        let project = tempfile::tempdir().expect("project");
        let nonce = "abcdefghijklmnopqrstuvwx";

        assert!(
            handle(
                project.path(),
                nonce,
                &json!({"jsonrpc": "2.0", "method": "notifications/initialized"}),
            )
            .is_none()
        );

        let initialized = handle(
            project.path(),
            nonce,
            &json!({"jsonrpc": "2.0", "id": 1, "method": "initialize"}),
        )
        .expect("initialize response");
        assert_eq!(initialized["result"]["protocolVersion"], PROTOCOL_VERSION);

        let ping = handle(
            project.path(),
            nonce,
            &json!({"jsonrpc": "2.0", "id": 2, "method": "ping"}),
        )
        .expect("ping response");
        assert_eq!(ping["result"], json!({}));

        let unknown_method = handle(
            project.path(),
            nonce,
            &json!({"jsonrpc": "2.0", "id": 3, "method": "future/method"}),
        )
        .expect("unknown method response");
        assert_eq!(unknown_method["error"]["code"], -32601);

        let malformed_call = handle(
            project.path(),
            nonce,
            &json!({"jsonrpc": "2.0", "id": 4, "method": "tools/call", "params": []}),
        )
        .expect("malformed call response");
        assert_eq!(malformed_call["error"]["code"], -32602);
    }

    #[test]
    fn context_projection_missing_malformed_and_stale_states_are_explicit() {
        let project = tempfile::tempdir().expect("project");
        let nonce = "abcdefghijklmnopqrstuvwx";
        let request = json!({
            "jsonrpc": "2.0", "id": 4, "method": "tools/call",
            "params": {"name": "context.open_tabs", "arguments": {"nonce": nonce}}
        });

        let missing = handle(project.path(), nonce, &request).expect("missing response");
        assert_eq!(missing["error"]["code"], -32002);
        assert!(
            missing["error"]["message"]
                .as_str()
                .unwrap()
                .contains("assistant-context.json")
        );

        fs::create_dir_all(project.path().join(".casa-rs")).expect("context directory");
        fs::write(
            project.path().join(".casa-rs/assistant-context.json"),
            b"not-json",
        )
        .expect("malformed projection");
        let malformed = handle(project.path(), nonce, &request).expect("malformed response");
        assert_eq!(malformed["error"]["code"], -32002);
        assert!(
            malformed["error"]["message"]
                .as_str()
                .unwrap()
                .contains("parse")
        );

        write_resource_plan(project.path());
        let stale = handle(
            project.path(),
            "different-current-nonce",
            &json!({
                "jsonrpc": "2.0", "id": 5, "method": "tools/call",
                "params": {"name": "context.open_tabs", "arguments": {
                    "nonce": "different-current-nonce"
                }}
            }),
        )
        .expect("stale response");
        assert_eq!(stale["error"]["code"], -32002);
        assert!(
            stale["error"]["message"]
                .as_str()
                .unwrap()
                .contains("stale")
        );
    }

    #[test]
    fn corpus_search_cannot_exceed_the_shared_resource_plan() {
        let project = tempfile::tempdir().expect("project");
        write_resource_plan(project.path());
        let path = project.path().join(".casa-rs/assistant-context.json");
        let mut projection: Value =
            serde_json::from_slice(&fs::read(&path).expect("projection")).expect("decode");
        projection["resource_plan"]["corpus_text_units"] = json!(2_000);
        fs::write(&path, serde_json::to_vec(&projection).unwrap()).expect("small plan");
        let nonce = "abcdefghijklmnopqrstuvwx";

        let response = handle(
            project.path(),
            nonce,
            &json!({
                "jsonrpc": "2.0", "id": 6, "method": "tools/call",
                "params": {"name": "corpus.search", "arguments": {
                    "nonce": nonce, "query": "Briggs", "limit": 2
                }}
            }),
        )
        .expect("bounded response");

        assert_eq!(response["error"]["code"], -32602);
        assert!(
            response["error"]["message"]
                .as_str()
                .unwrap()
                .contains("resource-plan limit 1")
        );
    }

    #[test]
    fn corpus_result_budget_includes_citation_metadata_and_utf8_text() {
        let hit = casars_frontend_services::AssistantCorpusSearchHitState {
            chunk_id: "chunk-1".to_owned(),
            document_id: "document-1".to_owned(),
            layer: "baseline".to_owned(),
            title: "A representative radio-astronomy result".to_owned(),
            text: "αβγδ ".repeat(300),
            score: 1.0,
            citation: AssistantCorpusCitationRequest {
                label: "Long citation".to_owned(),
                locator: format!("{} section", "nested/path/".repeat(24)),
                source_path: Some("source.md".to_owned()),
                page: Some(42),
                section: Some("Imaging".to_owned()),
                line_start: Some(10),
                line_end: Some(20),
                release: Some("0.25.0".to_owned()),
                commit: Some("0123456789abcdef".to_owned()),
            },
            untrusted_evidence: true,
        };

        let fitted = fit_search_hits_to_budget(vec![hit.clone()], 900).expect("fit result");
        assert_eq!(fitted.len(), 1);
        assert!(fitted[0].text.len() < hit.text.len());
        assert!(measured_search_result_units(&fitted).unwrap() <= 900);
        assert!(std::str::from_utf8(fitted[0].text.as_bytes()).is_ok());

        let error = fit_search_hits_to_budget(vec![hit], 32).expect_err("metadata cannot fit");
        assert!(error.message.contains("citation and its metadata"));
    }

    #[test]
    fn exact_nonce_tools_retrieve_scientific_and_source_citations() {
        let project = tempfile::tempdir().expect("project");
        let project_root = project.path().canonicalize().expect("canonical project");
        write_resource_plan(&project_root);
        assistant_corpus_index(AssistantCorpusIndexRequest {
            project_root: project_root.display().to_string(),
            documents: vec![
                AssistantCorpusDocumentRequest {
                    id: "baseline:primer".to_owned(),
                    layer: "baseline".to_owned(),
                    title: "Radio primer".to_owned(),
                    source_identity: "baseline/primer.md".to_owned(),
                    content: "Briggs weighting trades sensitivity against angular resolution."
                        .to_owned(),
                    citation: AssistantCorpusCitationRequest {
                        label: "Radio primer".to_owned(),
                        locator: "baseline/primer.md, Imaging".to_owned(),
                        source_path: Some("baseline/primer.md".to_owned()),
                        page: None,
                        section: Some("Imaging".to_owned()),
                        line_start: None,
                        line_end: None,
                        release: Some("1.0.0".to_owned()),
                        commit: None,
                    },
                    redistribution_cleared: true,
                },
                AssistantCorpusDocumentRequest {
                    id: "source:corpus".to_owned(),
                    layer: "live_source".to_owned(),
                    title: "corpus.rs".to_owned(),
                    source_identity: "crates/casa-notebook/src/corpus.rs@abc123".to_owned(),
                    content: "pub const CORPUS_SCHEMA_VERSION: u32 = 2;".to_owned(),
                    citation: AssistantCorpusCitationRequest {
                        label: "corpus.rs".to_owned(),
                        locator: "crates/casa-notebook/src/corpus.rs".to_owned(),
                        source_path: Some("crates/casa-notebook/src/corpus.rs".to_owned()),
                        page: None,
                        section: None,
                        line_start: None,
                        line_end: None,
                        release: None,
                        commit: Some("abc123".to_owned()),
                    },
                    redistribution_cleared: true,
                },
            ],
            remove_missing_layers: vec!["baseline".to_owned(), "live_source".to_owned()],
        })
        .expect("index corpus");
        let nonce = "abcdefghijklmnopqrstuvwx";

        let corpus_response = handle(
            &project_root,
            nonce,
            &json!({
                "jsonrpc": "2.0", "id": 11, "method": "tools/call",
                "params": {"name": "corpus.search", "arguments": {
                    "nonce": nonce, "query": "Briggs sensitivity resolution", "limit": 8
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
                    "nonce": nonce, "query": "CORPUS_SCHEMA_VERSION", "limit": 8
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
    fn task_catalog_is_the_canonical_application_catalog() {
        let value = task_catalog_for_agent().unwrap();
        assert!(
            value["applications"]
                .as_array()
                .unwrap()
                .iter()
                .any(|entry| { entry["id"] == "imager" && entry["kind"] == "task" })
        );
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
        let schema = assistant_task_schema("simobserve").expect("agent task schema");
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

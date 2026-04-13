// SPDX-License-Identifier: LGPL-3.0-or-later
//! `imexplore` command schema shared by the CLI and `casars`.

use serde_json::json;

/// Return the `imexplore` UI schema as formatted JSON.
pub fn ui_schema_json(invocation_name: &str) -> Result<String, String> {
    let schema = json!({
        "schema_version": 1,
        "command_id": "imexplore",
        "invocation_name": invocation_name,
        "display_name": "ImExplore",
        "category": "Images",
        "summary": "browse persistent casacore images",
        "usage": format!("{invocation_name} <image-path>"),
        "arguments": [
            {
                "id": "image_path",
                "label": "Image Path",
                "order": 0,
                "parser": {
                    "kind": "positional",
                    "metavar": "image-path"
                },
                "value_kind": "path",
                "required": true,
                "default": null,
                "help": "Path to the casacore image root directory",
                "group": "Input",
                "advanced": false,
                "hidden_in_tui": false
            },
            {
                "id": "blc",
                "label": "BLC",
                "order": 1,
                "parser": {
                    "kind": "option",
                    "flags": ["--blc"],
                    "metavar": "BLC",
                    "choices": []
                },
                "value_kind": "string",
                "required": false,
                "default": "",
                "help": "Comma-separated inclusive bottom-left-corner pixel indices",
                "group": "View",
                "advanced": false,
                "hidden_in_tui": false
            },
            {
                "id": "trc",
                "label": "TRC",
                "order": 2,
                "parser": {
                    "kind": "option",
                    "flags": ["--trc"],
                    "metavar": "TRC",
                    "choices": []
                },
                "value_kind": "string",
                "required": false,
                "default": "",
                "help": "Comma-separated inclusive top-right-corner pixel indices",
                "group": "View",
                "advanced": false,
                "hidden_in_tui": false
            },
            {
                "id": "inc",
                "label": "INC",
                "order": 3,
                "parser": {
                    "kind": "option",
                    "flags": ["--inc"],
                    "metavar": "INC",
                    "choices": []
                },
                "value_kind": "string",
                "required": false,
                "default": "",
                "help": "Comma-separated per-axis pixel increments",
                "group": "View",
                "advanced": false,
                "hidden_in_tui": false
            },
            {
                "id": "stretch",
                "label": "Stretch",
                "order": 4,
                "parser": {
                    "kind": "option",
                    "flags": ["--stretch"],
                    "metavar": "STRETCH",
                    "choices": ["percentile99", "percentile95", "minmax", "zscale", "manual"]
                },
                "value_kind": "choice",
                "required": false,
                "default": "percentile99",
                "help": "Plane stretch preset",
                "group": "Display",
                "advanced": false,
                "hidden_in_tui": false
            },
            {
                "id": "autoscale",
                "label": "Autoscale",
                "order": 5,
                "parser": {
                    "kind": "option",
                    "flags": ["--autoscale"],
                    "metavar": "AUTOSCALE",
                    "choices": ["per_plane", "frozen"]
                },
                "value_kind": "choice",
                "required": false,
                "default": "per_plane",
                "help": "Whether clip bounds update per plane or stay frozen while stepping cubes",
                "group": "Display",
                "advanced": false,
                "hidden_in_tui": false
            },
            {
                "id": "clip_low",
                "label": "Clip Low",
                "order": 6,
                "parser": {
                    "kind": "option",
                    "flags": ["--clip-low"],
                    "metavar": "LOW",
                    "choices": []
                },
                "value_kind": "string",
                "required": false,
                "default": "",
                "help": "Manual lower clip bound in image value units",
                "group": "Display",
                "advanced": false,
                "hidden_in_tui": false
            },
            {
                "id": "clip_high",
                "label": "Clip High",
                "order": 7,
                "parser": {
                    "kind": "option",
                    "flags": ["--clip-high"],
                    "metavar": "HIGH",
                    "choices": []
                },
                "value_kind": "string",
                "required": false,
                "default": "",
                "help": "Manual upper clip bound in image value units",
                "group": "Display",
                "advanced": false,
                "hidden_in_tui": false
            },
            {
                "id": "fps",
                "label": "FPS",
                "order": 8,
                "parser": {
                    "kind": "option",
                    "flags": ["--fps"],
                    "metavar": "FPS",
                    "choices": []
                },
                "value_kind": "string",
                "required": false,
                "default": "1",
                "help": "Movie playback frames per second",
                "group": "Display",
                "advanced": false,
                "hidden_in_tui": false
            },
            {
                "id": "help",
                "label": "Help",
                "order": 9,
                "parser": {
                    "kind": "action",
                    "flags": ["-h", "--help"],
                    "action": "help"
                },
                "value_kind": "none",
                "required": false,
                "default": null,
                "help": "Print this help message",
                "group": "Input",
                "advanced": true,
                "hidden_in_tui": true
            }
        ],
        "managed_output": null
    });
    serde_json::to_string_pretty(&schema).map_err(|error| format!("serialize ui schema: {error}"))
}

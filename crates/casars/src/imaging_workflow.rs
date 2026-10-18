// SPDX-License-Identifier: LGPL-3.0-or-later
//! Imaging-specific helpers for the generic `WorkflowShell`.

use crate::workflow::{WorkflowArtifactDisplay, WorkflowArtifactGroupDisplay};
use casars_imager::{ImagerArtifact, ImagerArtifactKind, ImagerRunTaskResult};

pub(crate) fn imaging_products_display_groups(
    output: &ImagerRunTaskResult,
) -> Vec<WorkflowArtifactGroupDisplay> {
    let mut rendered = Vec::new();
    let main_products = output
        .artifacts
        .iter()
        .filter(|artifact| artifact.kind != ImagerArtifactKind::Alpha)
        .map(render_artifact)
        .collect::<Vec<_>>();
    if !main_products.is_empty() {
        rendered.push(WorkflowArtifactGroupDisplay {
            title: "Imaging Products".to_string(),
            items: main_products,
        });
    }
    let derived = output
        .artifacts
        .iter()
        .filter(|artifact| artifact.kind == ImagerArtifactKind::Alpha)
        .map(render_artifact)
        .collect::<Vec<_>>();
    if !derived.is_empty() {
        rendered.push(WorkflowArtifactGroupDisplay {
            title: "Derived Products".to_string(),
            items: derived,
        });
    }
    rendered
}

fn render_artifact(artifact: &ImagerArtifact) -> WorkflowArtifactDisplay {
    WorkflowArtifactDisplay {
        heading: artifact.label.clone(),
        detail_lines: vec![format!(
            "status={}  path={}",
            if artifact.exists {
                "written"
            } else {
                "missing"
            },
            artifact.path
        )],
    }
}

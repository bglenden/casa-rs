// SPDX-License-Identifier: LGPL-3.0-or-later
//! Imaging-specific helpers for the generic `WorkflowShell`.

use crate::workflow::{
    WorkflowArtifactDisplay, WorkflowArtifactGroupDisplay, WorkflowCatalogEntryDisplay,
};
use casars_imager::{ImagerArtifact, ImagerArtifactKind, ImagerRunTaskResult};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[allow(
    dead_code,
    reason = "imaging runs write no preview images; #681 removes the preview catalog"
)]
pub(crate) enum ImagingDiagnosticKind {
    Psf,
    Residual,
    Model,
    Image,
    Alpha,
}

impl ImagingDiagnosticKind {
    pub(crate) fn label(self) -> &'static str {
        match self {
            Self::Psf => "PSF Preview",
            Self::Residual => "Residual Preview",
            Self::Model => "Model Preview",
            Self::Image => "Image Preview",
            Self::Alpha => "Alpha Preview",
        }
    }
}

/// The diagnostic shown first. Imaging runs write no preview images
/// (#681), so it is the PSF's.
pub(crate) fn imaging_preferred_diagnostic(_output: &ImagerRunTaskResult) -> ImagingDiagnosticKind {
    ImagingDiagnosticKind::Psf
}

/// The diagnostics with previews; imaging runs write none (#681).
pub(crate) fn imaging_catalog_entries(
    _output: &ImagerRunTaskResult,
    _selected: ImagingDiagnosticKind,
) -> Vec<WorkflowCatalogEntryDisplay<ImagingDiagnosticKind>> {
    Vec::new()
}

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

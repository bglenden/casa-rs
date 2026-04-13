// SPDX-License-Identifier: LGPL-3.0-or-later
//! Imaging-specific helpers for the generic `WorkflowShell`.

use crate::workflow::{
    WorkflowArtifactDisplay, WorkflowArtifactGroupDisplay, WorkflowCatalogEntryDisplay,
};
use casars_imager::{ManagedImagingArtifact, ManagedImagingOutput};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) enum ImagingDiagnosticKind {
    PsfPreview,
    ResidualPreview,
    ModelPreview,
    ImagePreview,
    AlphaPreview,
    ResidualByChannel,
    IterationsByChannel,
}

impl ImagingDiagnosticKind {
    pub(crate) fn label(self) -> &'static str {
        match self {
            Self::PsfPreview => "PSF Preview",
            Self::ResidualPreview => "Residual Preview",
            Self::ModelPreview => "Model Preview",
            Self::ImagePreview => "Image Preview",
            Self::AlphaPreview => "Alpha Preview",
            Self::ResidualByChannel => "Residual By Channel",
            Self::IterationsByChannel => "Iterations By Channel",
        }
    }
}

pub(crate) fn imaging_preferred_diagnostic(output: &ManagedImagingOutput) -> ImagingDiagnosticKind {
    if !output.run.channels.is_empty() {
        ImagingDiagnosticKind::ResidualByChannel
    } else if artifact_preview_available(&output.artifacts, "image") {
        ImagingDiagnosticKind::ImagePreview
    } else if artifact_preview_available(&output.artifacts, "residual") {
        ImagingDiagnosticKind::ResidualPreview
    } else {
        ImagingDiagnosticKind::PsfPreview
    }
}

pub(crate) fn imaging_catalog_entries(
    output: &ManagedImagingOutput,
    selected: ImagingDiagnosticKind,
) -> Vec<WorkflowCatalogEntryDisplay<ImagingDiagnosticKind>> {
    let mut entries = Vec::new();
    for (kind, artifact_kind) in [
        (ImagingDiagnosticKind::PsfPreview, "psf"),
        (ImagingDiagnosticKind::ResidualPreview, "residual"),
        (ImagingDiagnosticKind::ModelPreview, "model"),
        (ImagingDiagnosticKind::ImagePreview, "image"),
        (ImagingDiagnosticKind::AlphaPreview, "alpha"),
    ] {
        if artifact_preview_available(&output.artifacts, artifact_kind) {
            entries.push(WorkflowCatalogEntryDisplay {
                target: kind,
                label: kind.label().to_string(),
                selected: selected == kind,
            });
        }
    }
    if !output.run.channels.is_empty() {
        for kind in [
            ImagingDiagnosticKind::ResidualByChannel,
            ImagingDiagnosticKind::IterationsByChannel,
        ] {
            entries.push(WorkflowCatalogEntryDisplay {
                target: kind,
                label: kind.label().to_string(),
                selected: selected == kind,
            });
        }
    }
    entries
}

pub(crate) fn imaging_products_display_groups(
    output: &ManagedImagingOutput,
) -> Vec<WorkflowArtifactGroupDisplay> {
    let mut rendered = Vec::new();
    let main_products = output
        .artifacts
        .iter()
        .filter(|artifact| !artifact.kind.ends_with("alpha"))
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
        .filter(|artifact| artifact.kind == "alpha")
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

fn render_artifact(artifact: &ManagedImagingArtifact) -> WorkflowArtifactDisplay {
    let mut detail_lines = vec![format!(
        "status={}  path={}",
        if artifact.exists {
            "written"
        } else {
            "missing"
        },
        artifact.path
    )];
    if let Some(preview) = &artifact.preview_png_path {
        detail_lines.push(format!(
            "preview={}  exists={}",
            preview,
            if artifact.preview_png_exists {
                "yes"
            } else {
                "no"
            }
        ));
    }
    WorkflowArtifactDisplay {
        heading: artifact.label.clone(),
        detail_lines,
    }
}

fn artifact_preview_available(artifacts: &[ManagedImagingArtifact], kind: &str) -> bool {
    artifacts.iter().any(|artifact| {
        artifact.kind == kind
            && artifact
                .preview_png_path
                .as_ref()
                .is_some_and(|_| artifact.preview_png_exists)
    })
}

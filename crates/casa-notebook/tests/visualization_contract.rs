// SPDX-License-Identifier: LGPL-3.0-or-later

use std::{collections::BTreeMap, fs, path::PathBuf};

use casa_notebook::{
    NotebookStore, SaveVisualizationRequest, VisualizationRenderMetadata, VisualizationReopenIntent,
};

#[test]
fn new_and_update_visualizations_keep_immutable_assets_and_exact_reopen_intent() {
    let root = tempfile::tempdir().expect("project");
    let project = root.path().canonicalize().expect("canonical project");
    let store = NotebookStore::open(project.clone()).expect("store");
    let notebook = store
        .create_named("Science.md", "Science")
        .expect("notebook");
    let first_source = project.join("first.png");
    let second_source = project.join("second.png");
    fs::write(&first_source, b"first immutable image").unwrap();
    fs::write(&second_source, b"second immutable image").unwrap();

    let first = store
        .save_visualization(request(
            notebook.entry.id,
            None,
            &first_source,
            "amplitude_vs_time",
        ))
        .expect("new visualization");
    let updated = store
        .save_visualization(request(
            notebook.entry.id,
            Some(first.id),
            &second_source,
            "phase_vs_time",
        ))
        .expect("update visualization");

    assert_eq!(updated.id, first.id);
    assert_eq!(updated.cell_id, first.cell_id);
    assert_eq!(updated.revisions.len(), 2);
    assert_eq!(
        updated.revisions[0].reopen.parameters["preset"],
        "amplitude_vs_time"
    );
    assert_eq!(
        updated.revisions[1].reopen.parameters["preset"],
        "phase_vs_time"
    );
    assert_eq!(
        fs::read(project.join(&updated.revisions[0].asset_path)).unwrap(),
        b"first immutable image"
    );
    assert_eq!(
        fs::read(project.join(&updated.revisions[1].asset_path)).unwrap(),
        b"second immutable image"
    );
    let reopened = store
        .visualizations_for_notebook(notebook.entry.id)
        .expect("reopen visualizations");
    assert_eq!(reopened, vec![updated.clone()]);
    let markdown = store
        .open_notebook("Science.md")
        .unwrap()
        .document
        .source()
        .to_owned();
    assert!(markdown.contains(&format!("casa-rs-visualization:v1 id={}", updated.id)));
    assert!(markdown.contains("r2.png"));
    assert!(!markdown.contains("r1.png"));
}

fn request(
    notebook_id: casa_notebook::NotebookId,
    visualization_id: Option<uuid::Uuid>,
    source_asset: &std::path::Path,
    preset: &str,
) -> SaveVisualizationRequest {
    SaveVisualizationRequest {
        notebook_id: Some(notebook_id),
        visualization_id,
        title: "Tutorial visibility plot".into(),
        source_asset: source_asset.to_owned(),
        source_references: vec![PathBuf::from("data/tutorial.ms")],
        reopen: VisualizationReopenIntent {
            surface: "msexplore".into(),
            contract_version: 4,
            parameters: BTreeMap::from([("preset".into(), serde_json::json!(preset))]),
            profile_toml: Some("[casars]\nsurface = \"msexplore\"\n".into()),
        },
        render: VisualizationRenderMetadata {
            renderer: "casa-rs plot document".into(),
            media_type: "image/png".into(),
            width: 1200,
            height: 800,
            settings: BTreeMap::new(),
        },
    }
}

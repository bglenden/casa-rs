// SPDX-License-Identifier: LGPL-3.0-or-later

use std::{collections::BTreeSet, path::PathBuf};

use casa_notebook::{CorpusCitation, CorpusDocumentInput, CorpusIndex, CorpusLayer};
use tempfile::tempdir;

fn document(id: &str, layer: CorpusLayer, title: &str, content: &str) -> CorpusDocumentInput {
    CorpusDocumentInput {
        id: id.to_owned(),
        layer,
        title: title.to_owned(),
        source_identity: format!("identity:{id}"),
        content: content.to_owned(),
        citation: CorpusCitation {
            label: title.to_owned(),
            locator: "section 2".to_owned(),
            source_path: Some(PathBuf::from(format!("documents/{id}.md"))),
            page: None,
            section: Some("MeasurementSet structure".to_owned()),
            line_start: None,
            line_end: None,
            release: None,
            commit: Some("abc123".to_owned()),
        },
        redistribution_cleared: layer != CorpusLayer::Baseline,
    }
}

#[test]
fn layered_index_is_incremental_and_searches_exact_cited_chunks() {
    let project = tempdir().expect("project");
    let index = CorpusIndex::open(project.path()).expect("open corpus");
    let documents = vec![
        document(
            "paper",
            CorpusLayer::ProjectDocument,
            "TW Hya paper",
            "The calibrated visibility amplitudes are stored by row.\nThe disk is nearly face-on.",
        ),
        document(
            "source",
            CorpusLayer::LiveSource,
            "MeasurementSet source",
            "MeasurementSet columns include UVW and DATA.\nRows retain time and antenna identifiers.",
        ),
    ];
    let first = index
        .index_documents(&documents, &BTreeSet::new())
        .expect("first index");
    assert_eq!(first.indexed_documents, 2);
    assert_eq!(first.unchanged_documents, 0);
    assert_eq!(first.retrieval_engine, "sqlite_fts5_unicode61");

    let second = index
        .index_documents(&documents, &BTreeSet::new())
        .expect("incremental index");
    assert_eq!(second.indexed_documents, 0);
    assert_eq!(second.unchanged_documents, 2);

    let hits = index
        .search("Which MeasurementSet columns contain UVW data?", 2)
        .expect("search");
    assert_eq!(hits[0].document_id, "source");
    assert!(hits[0].text.contains("UVW"));
    assert_eq!(hits[0].citation.line_start, Some(1));
    assert!(hits[0].citation.commit.is_some());
    assert!(hits[0].untrusted_evidence);
}

#[test]
fn layer_refresh_removes_only_missing_documents_in_selected_layers() {
    let project = tempdir().expect("project");
    let index = CorpusIndex::open(project.path()).expect("open corpus");
    let project_document = document(
        "paper",
        CorpusLayer::ProjectDocument,
        "Paper",
        "Scientific evidence remains project-owned.",
    );
    let live_source = document(
        "source",
        CorpusLayer::LiveSource,
        "Source",
        "Live source overlay is commit keyed.",
    );
    index
        .index_documents(&[project_document.clone(), live_source], &BTreeSet::new())
        .expect("seed index");
    let report = index
        .index_documents(
            &[project_document],
            &BTreeSet::from([CorpusLayer::LiveSource]),
        )
        .expect("refresh live layer");
    assert_eq!(report.removed_documents, 1);
    let remaining = index.documents().expect("remaining documents");
    assert_eq!(remaining.len(), 1);
    assert_eq!(remaining[0].id, "paper");
}

#[test]
fn baseline_requires_redistribution_clearance_and_paths_must_be_relative() {
    let project = tempdir().expect("project");
    let index = CorpusIndex::open(project.path()).expect("open corpus");
    let baseline = document(
        "baseline",
        CorpusLayer::Baseline,
        "Baseline",
        "redistributable content",
    );
    assert!(
        index
            .index_documents(&[baseline], &BTreeSet::new())
            .is_err()
    );

    let mut escaping = document(
        "escape",
        CorpusLayer::ProjectDocument,
        "Escape",
        "not allowed",
    );
    escaping.citation.source_path = Some(PathBuf::from("../secret"));
    assert!(
        index
            .index_documents(&[escaping], &BTreeSet::new())
            .is_err()
    );
}

#[test]
fn source_chunks_add_symbol_and_line_provenance_when_not_predeclared() {
    let project = tempdir().expect("project");
    let index = CorpusIndex::open(project.path()).expect("open corpus");
    let mut source = document(
        "symbol-source",
        CorpusLayer::LiveSource,
        "Assistant source",
        "use serde::Serialize;\n\npub fn bounded_search(query: &str) -> Vec<String> {\n    vec![query.to_owned()]\n}",
    );
    source.citation.section = None;
    source.citation.source_path = Some(PathBuf::from("crates/example/src/lib.rs"));
    index
        .index_documents(&[source], &BTreeSet::new())
        .expect("index source");
    let hit = index
        .search("bounded search query", 1)
        .expect("search")
        .remove(0);
    assert_eq!(hit.citation.line_start, Some(1));
    assert_eq!(
        hit.citation.section.as_deref(),
        Some("pub fn bounded_search(query: &str) -> Vec<String> {")
    );
    assert_eq!(hit.citation.commit.as_deref(), Some("abc123"));
}

#[test]
fn layer_filtered_search_ranks_only_the_requested_corpus_plane() {
    let project = tempdir().expect("project");
    let index = CorpusIndex::open(project.path()).expect("open corpus");
    index
        .index_documents(
            &[
                document(
                    "paper",
                    CorpusLayer::ProjectDocument,
                    "Paper",
                    "Briggs weighting is discussed in this paper.",
                ),
                document(
                    "source",
                    CorpusLayer::LiveSource,
                    "Source",
                    "Briggs weighting is implemented by this source symbol.",
                ),
            ],
            &BTreeSet::new(),
        )
        .expect("index layered evidence");

    let hits = index
        .search_layers(
            "Briggs weighting",
            1,
            &BTreeSet::from([CorpusLayer::ReleaseSource, CorpusLayer::LiveSource]),
        )
        .expect("source-only search");
    assert_eq!(hits.len(), 1);
    assert_eq!(hits[0].document_id, "source");
}

// SPDX-License-Identifier: LGPL-3.0-or-later

use std::{collections::BTreeSet, path::PathBuf};

use casa_notebook::{
    CorpusCitation, CorpusDocumentInput, CorpusIndex, CorpusLayer, ProjectCorpusSource,
};
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

fn project_source(path: &str, identity: &str) -> ProjectCorpusSource {
    ProjectCorpusSource {
        relative_path: PathBuf::from(path),
        file_type: PathBuf::from(path)
            .extension()
            .and_then(|value| value.to_str())
            .unwrap_or_default()
            .to_owned(),
        size_bytes: 128,
        modified_unix_ns: 1_000,
        status_changed_unix_ns: 2_000,
        file_identity: identity.to_owned(),
    }
}

#[test]
fn schema_two_project_pages_are_backfilled_before_incremental_replacement() {
    let project = tempdir().expect("project");
    let index = CorpusIndex::open(project.path()).expect("open corpus");
    let mut page_one = document(
        "paper-page-1",
        CorpusLayer::ProjectDocument,
        "Paper",
        "Original first page.",
    );
    page_one.citation.source_path = Some(PathBuf::from("documents/paper.pdf"));
    let mut page_two = document(
        "paper-page-2",
        CorpusLayer::ProjectDocument,
        "Paper",
        "Obsolete second page zephyr.",
    );
    page_two.citation.source_path = Some(PathBuf::from("documents/paper.pdf"));
    index
        .index_documents(
            &[page_one, page_two],
            &BTreeSet::from([CorpusLayer::ProjectDocument]),
        )
        .expect("seed legacy project pages");
    drop(index);

    let connection =
        rusqlite::Connection::open(project.path().join(".casa-rs/corpus/index.sqlite3"))
            .expect("open sqlite");
    connection
        .execute("UPDATE documents SET project_source_path = NULL", [])
        .expect("clear v3 source paths");
    connection
        .execute("DELETE FROM project_sources", [])
        .expect("clear v3 fingerprints");
    connection
        .execute(
            "UPDATE corpus_meta SET value = '2' WHERE key = 'schema_version'",
            [],
        )
        .expect("mark schema two");
    drop(connection);

    let migrated = CorpusIndex::open(project.path()).expect("migrate corpus");
    let source = project_source("documents/paper.pdf", "1:22");
    let mut replacement = document(
        "paper-page-1",
        CorpusLayer::ProjectDocument,
        "Paper",
        "Replacement one-page paper.",
    );
    replacement.citation.source_path = Some(PathBuf::from("documents/paper.pdf"));
    let report = migrated
        .index_documents_with_project_sources(
            &[replacement],
            &BTreeSet::new(),
            &[source],
            &BTreeSet::new(),
        )
        .expect("replace migrated pages");
    assert_eq!(report.removed_documents, 1);
    assert!(migrated.search("zephyr", 4).expect("search").is_empty());
}

#[test]
fn project_source_plan_skips_unchanged_content_without_reextraction() {
    let project = tempdir().expect("project");
    let index = CorpusIndex::open(project.path()).expect("open corpus");
    let source = project_source("documents/paper.md", "1:10");
    let first_plan = index
        .plan_project_sources(std::slice::from_ref(&source))
        .expect("initial plan");
    assert_eq!(
        first_plan.extract_paths,
        [PathBuf::from("documents/paper.md")]
    );
    assert!(first_plan.unchanged_paths.is_empty());

    let report = index
        .index_documents_with_project_sources(
            &[document(
                "paper",
                CorpusLayer::ProjectDocument,
                "Paper",
                "Stable project evidence.",
            )],
            &BTreeSet::new(),
            std::slice::from_ref(&source),
            &BTreeSet::new(),
        )
        .expect("index project source");
    assert_eq!(report.indexed_documents, 1);

    let second_plan = index
        .plan_project_sources(std::slice::from_ref(&source))
        .expect("unchanged plan");
    assert!(second_plan.extract_paths.is_empty());
    assert_eq!(
        second_plan.unchanged_paths,
        [PathBuf::from("documents/paper.md")]
    );
    let unchanged = index
        .index_documents_with_project_sources(&[], &BTreeSet::new(), &[source], &BTreeSet::new())
        .expect("commit unchanged inventory");
    assert_eq!(unchanged.indexed_documents, 0);
    assert_eq!(unchanged.unchanged_documents, 1);
}

#[test]
fn project_source_plan_detects_preserved_mtime_and_atomic_identity_changes() {
    let project = tempdir().expect("project");
    let index = CorpusIndex::open(project.path()).expect("open corpus");
    let source = project_source("documents/paper.md", "1:10");
    index
        .index_documents_with_project_sources(
            &[document(
                "paper",
                CorpusLayer::ProjectDocument,
                "Paper",
                "Original evidence.",
            )],
            &BTreeSet::new(),
            std::slice::from_ref(&source),
            &BTreeSet::new(),
        )
        .expect("seed source");

    let mut preserved_mtime = source.clone();
    preserved_mtime.status_changed_unix_ns += 1;
    assert_eq!(preserved_mtime.modified_unix_ns, source.modified_unix_ns);
    assert_eq!(
        index
            .plan_project_sources(std::slice::from_ref(&preserved_mtime))
            .expect("ctime plan")
            .extract_paths,
        [PathBuf::from("documents/paper.md")]
    );

    let mut atomic_replacement = source;
    atomic_replacement.file_identity = "1:11".to_owned();
    assert_eq!(
        index
            .plan_project_sources(&[atomic_replacement])
            .expect("replacement plan")
            .extract_paths,
        [PathBuf::from("documents/paper.md")]
    );
}

#[test]
fn failed_project_source_keeps_last_usable_index_and_retries() {
    let project = tempdir().expect("project");
    let index = CorpusIndex::open(project.path()).expect("open corpus");
    let source = project_source("documents/paper.md", "1:10");
    index
        .index_documents_with_project_sources(
            &[document(
                "paper",
                CorpusLayer::ProjectDocument,
                "Paper",
                "Last usable violet evidence.",
            )],
            &BTreeSet::new(),
            std::slice::from_ref(&source),
            &BTreeSet::new(),
        )
        .expect("seed source");

    let mut unreadable = source;
    unreadable.status_changed_unix_ns += 10;
    let failed = BTreeSet::from([PathBuf::from("documents/paper.md")]);
    index
        .index_documents_with_project_sources(
            &[],
            &BTreeSet::new(),
            std::slice::from_ref(&unreadable),
            &failed,
        )
        .expect("retain failed source");
    assert_eq!(
        index.search("violet evidence", 1).expect("search")[0].document_id,
        "paper"
    );
    assert_eq!(
        index
            .plan_project_sources(&[unreadable])
            .expect("retry plan")
            .extract_paths,
        [PathBuf::from("documents/paper.md")]
    );
}

#[test]
fn project_source_snapshot_reconciles_rename_and_removal_without_touching_other_documents() {
    let project = tempdir().expect("project");
    let index = CorpusIndex::open(project.path()).expect("open corpus");
    let paper = project_source("documents/paper.md", "1:10");
    let notes = project_source("documents/notes.md", "1:20");
    index
        .index_documents_with_project_sources(
            &[
                document(
                    "paper",
                    CorpusLayer::ProjectDocument,
                    "Paper",
                    "Old amber evidence.",
                ),
                document(
                    "notes",
                    CorpusLayer::ProjectDocument,
                    "Notes",
                    "Unrelated cobalt evidence.",
                ),
            ],
            &BTreeSet::new(),
            &[paper, notes.clone()],
            &BTreeSet::new(),
        )
        .expect("seed sources");

    let renamed = project_source("documents/renamed.md", "1:10");
    let mut renamed_document = document(
        "renamed",
        CorpusLayer::ProjectDocument,
        "Renamed",
        "New ultraviolet evidence.",
    );
    renamed_document.citation.source_path = Some(PathBuf::from("documents/renamed.md"));
    let report = index
        .index_documents_with_project_sources(
            &[renamed_document],
            &BTreeSet::new(),
            &[renamed.clone(), notes.clone()],
            &BTreeSet::new(),
        )
        .expect("reconcile rename");
    assert_eq!(report.removed_documents, 1);
    assert!(
        index
            .search("amber evidence", 4)
            .expect("old search")
            .iter()
            .all(|hit| hit.document_id != "paper" && !hit.text.contains("amber"))
    );
    assert_eq!(
        index.search("ultraviolet evidence", 2).expect("new search")[0].document_id,
        "renamed"
    );
    assert_eq!(
        index.search("cobalt evidence", 2).expect("other search")[0].document_id,
        "notes"
    );

    let removed = index
        .index_documents_with_project_sources(&[], &BTreeSet::new(), &[notes], &BTreeSet::new())
        .expect("remove renamed source");
    assert_eq!(removed.removed_documents, 1);
    assert!(
        index
            .search("ultraviolet evidence", 4)
            .expect("removed search")
            .iter()
            .all(|hit| hit.document_id != "renamed" && !hit.text.contains("ultraviolet"))
    );
    assert!(
        index
            .plan_project_sources(&[renamed])
            .expect("renamed now new")
            .extract_paths
            .contains(&PathBuf::from("documents/renamed.md"))
    );
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
fn incremental_index_refreshes_provenance_when_content_is_unchanged() {
    let project = tempdir().expect("project");
    let index = CorpusIndex::open(project.path()).expect("open corpus");
    let mut source = document(
        "source",
        CorpusLayer::LiveSource,
        "MeasurementSet source",
        "MeasurementSet columns include UVW and DATA.",
    );
    source.citation.release = Some("v1.0.0".to_owned());
    source.citation.commit = Some("old-commit".to_owned());
    index
        .index_documents(&[source.clone()], &BTreeSet::new())
        .expect("seed source index");
    let original_content_hash = index.documents().expect("documents")[0]
        .content_sha256
        .clone();

    source.title = "Current MeasurementSet source".to_owned();
    source.source_identity = "identity:source@new-commit".to_owned();
    source.citation.label = "Current source".to_owned();
    source.citation.locator = "crates/casa-ms/src/lib.rs".to_owned();
    source.citation.release = Some("v1.1.0".to_owned());
    source.citation.commit = Some("new-commit".to_owned());
    let report = index
        .index_documents(&[source], &BTreeSet::new())
        .expect("refresh source provenance");
    assert_eq!(report.indexed_documents, 1);
    assert_eq!(report.unchanged_documents, 0);

    let documents = index.documents().expect("refreshed documents");
    assert_eq!(documents[0].content_sha256, original_content_hash);
    assert_eq!(documents[0].title, "Current MeasurementSet source");
    assert_eq!(documents[0].source_identity, "identity:source@new-commit");
    let hit = index
        .search("MeasurementSet UVW DATA", 1)
        .expect("search refreshed source")
        .remove(0);
    assert_eq!(hit.citation.label, "Current source");
    assert_eq!(hit.citation.locator, "crates/casa-ms/src/lib.rs");
    assert_eq!(hit.citation.release.as_deref(), Some("v1.1.0"));
    assert_eq!(hit.citation.commit.as_deref(), Some("new-commit"));
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

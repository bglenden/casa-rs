// SPDX-License-Identifier: LGPL-3.0-or-later

//! Local layered assistant corpus with SQLite FTS5 retrieval and exact citations.

use std::{
    collections::{BTreeMap, BTreeSet},
    fs,
    path::{Component, Path, PathBuf},
};

use rusqlite::{Connection, OptionalExtension, params};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use thiserror::Error;

pub const CORPUS_SCHEMA_VERSION: u32 = 3;
// Retrieval-unit bound, not a science-data or download limit. Keeping one FTS
// hit near a page of prose makes citations claim-local and tool results
// independently reviewable.
const MAX_CHUNK_BYTES: usize = 2_000;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CorpusLayer {
    Baseline,
    ProjectDocument,
    ReleaseSource,
    LiveSource,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CorpusCitation {
    pub label: String,
    pub locator: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source_path: Option<PathBuf>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub page: Option<u32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub section: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub line_start: Option<u32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub line_end: Option<u32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub release: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub commit: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CorpusDocumentInput {
    pub id: String,
    pub layer: CorpusLayer,
    pub title: String,
    pub source_identity: String,
    pub content: String,
    pub citation: CorpusCitation,
    pub redistribution_cleared: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CorpusDocument {
    pub id: String,
    pub layer: CorpusLayer,
    pub title: String,
    pub source_identity: String,
    pub content_sha256: String,
    pub chunk_count: usize,
    pub redistribution_cleared: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CorpusIndexReport {
    pub schema_version: u32,
    pub retrieval_engine: String,
    pub indexed_documents: usize,
    pub unchanged_documents: usize,
    pub removed_documents: usize,
    pub chunk_count: usize,
}

/// Metadata-only identity for one supported file under `<project>/documents`.
///
/// The host obtains this from `lstat(2)` without reading file content. The
/// status-change timestamp and filesystem identity make preserved-mtime edits
/// and editor-style atomic replacements observable without hashing unchanged
/// files on every reconciliation.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProjectCorpusSource {
    pub relative_path: PathBuf,
    pub file_type: String,
    pub size_bytes: u64,
    pub modified_unix_ns: i64,
    pub status_changed_unix_ns: i64,
    pub file_identity: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProjectCorpusPlan {
    pub schema_version: u32,
    pub extract_paths: Vec<PathBuf>,
    pub unchanged_paths: Vec<PathBuf>,
    pub removed_paths: Vec<PathBuf>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct CorpusSearchHit {
    pub chunk_id: String,
    pub document_id: String,
    pub layer: CorpusLayer,
    pub title: String,
    pub text: String,
    pub score: f32,
    pub citation: CorpusCitation,
    pub untrusted_evidence: bool,
}

#[derive(Debug, Clone)]
pub struct CorpusIndex {
    project_root: PathBuf,
    managed_dir: PathBuf,
}

impl CorpusIndex {
    pub fn open(project_root: impl AsRef<Path>) -> Result<Self, CorpusError> {
        let project_root = project_root.as_ref();
        if !project_root.is_absolute() {
            return Err(CorpusError::ProjectRootMustBeAbsolute {
                path: project_root.to_owned(),
            });
        }
        let metadata = fs::metadata(project_root).map_err(|source| CorpusError::Io {
            action: "inspect corpus project root",
            path: project_root.to_owned(),
            source,
        })?;
        if !metadata.is_dir() {
            return Err(CorpusError::ProjectRootNotDirectory {
                path: project_root.to_owned(),
            });
        }
        let managed_dir = project_root.join(".casa-rs/corpus");
        fs::create_dir_all(&managed_dir).map_err(|source| CorpusError::Io {
            action: "create corpus directory",
            path: managed_dir.clone(),
            source,
        })?;
        let index = Self {
            project_root: project_root.to_owned(),
            managed_dir,
        };
        index.initialize()?;
        Ok(index)
    }

    #[must_use]
    pub fn project_root(&self) -> &Path {
        &self.project_root
    }

    pub fn index_documents(
        &self,
        documents: &[CorpusDocumentInput],
        remove_missing_layers: &BTreeSet<CorpusLayer>,
    ) -> Result<CorpusIndexReport, CorpusError> {
        self.index_documents_internal(documents, remove_missing_layers, None, &BTreeSet::new())
    }

    /// Compare a complete metadata-only project-document inventory with the
    /// last successfully indexed source state.
    pub fn plan_project_sources(
        &self,
        sources: &[ProjectCorpusSource],
    ) -> Result<ProjectCorpusPlan, CorpusError> {
        let sources = validated_project_sources(sources)?;
        let connection = self.connection()?;
        let existing = load_project_source_fingerprints(&connection)?;
        let mut extract_paths = Vec::new();
        let mut unchanged_paths = Vec::new();
        for (path, source) in &sources {
            let fingerprint = project_source_fingerprint(source)?;
            if existing.get(path) == Some(&fingerprint) {
                unchanged_paths.push(path.clone());
            } else {
                extract_paths.push(path.clone());
            }
        }
        let removed_paths = existing
            .keys()
            .filter(|path| !sources.contains_key(*path))
            .cloned()
            .collect();
        Ok(ProjectCorpusPlan {
            schema_version: CORPUS_SCHEMA_VERSION,
            extract_paths,
            unchanged_paths,
            removed_paths,
        })
    }

    /// Atomically reconcile a complete project-source inventory while only
    /// replacing documents extracted from changed sources. Failed sources keep
    /// their last usable indexed content and fingerprint so a later refresh
    /// retries them.
    pub fn index_documents_with_project_sources(
        &self,
        documents: &[CorpusDocumentInput],
        remove_missing_layers: &BTreeSet<CorpusLayer>,
        project_sources: &[ProjectCorpusSource],
        failed_project_sources: &BTreeSet<PathBuf>,
    ) -> Result<CorpusIndexReport, CorpusError> {
        self.index_documents_internal(
            documents,
            remove_missing_layers,
            Some(project_sources),
            failed_project_sources,
        )
    }

    fn index_documents_internal(
        &self,
        documents: &[CorpusDocumentInput],
        remove_missing_layers: &BTreeSet<CorpusLayer>,
        project_sources: Option<&[ProjectCorpusSource]>,
        failed_project_sources: &BTreeSet<PathBuf>,
    ) -> Result<CorpusIndexReport, CorpusError> {
        let project_sources = project_sources.map(validated_project_sources).transpose()?;
        if project_sources.is_some()
            && remove_missing_layers.contains(&CorpusLayer::ProjectDocument)
        {
            return Err(CorpusError::InvalidDocument(
                "project_document removal must use the project-source snapshot".to_owned(),
            ));
        }
        for path in failed_project_sources {
            validate_project_source_path(path)?;
            if project_sources
                .as_ref()
                .is_some_and(|sources| !sources.contains_key(path))
            {
                return Err(CorpusError::InvalidDocument(format!(
                    "failed project source {} is absent from the source snapshot",
                    path.display()
                )));
            }
        }
        let mut connection = self.connection()?;
        let transaction = connection.transaction()?;
        let mut indexed_documents = 0;
        let mut unchanged_documents = 0;
        let mut seen = BTreeSet::new();

        let mut successful_project_paths = BTreeSet::new();
        let mut project_document_ids: BTreeMap<PathBuf, BTreeSet<String>> = BTreeMap::new();
        for input in documents {
            if input.layer != CorpusLayer::ProjectDocument {
                continue;
            }
            let path = input.citation.source_path.clone().ok_or_else(|| {
                CorpusError::InvalidDocument(format!(
                    "project document {} has no source path",
                    input.id
                ))
            })?;
            validate_project_source_path(&path)?;
            if let Some(sources) = &project_sources
                && !sources.contains_key(&path)
            {
                return Err(CorpusError::InvalidDocument(format!(
                    "project document {} source {} is absent from the source snapshot",
                    input.id,
                    path.display()
                )));
            }
            successful_project_paths.insert(path.clone());
            project_document_ids
                .entry(path)
                .or_default()
                .insert(input.id.clone());
        }
        if successful_project_paths
            .iter()
            .any(|path| failed_project_sources.contains(path))
        {
            return Err(CorpusError::InvalidDocument(
                "a project source cannot be both successfully extracted and failed".to_owned(),
            ));
        }

        let mut old_project_document_ids = BTreeMap::new();
        if project_sources.is_some() {
            for path in &successful_project_paths {
                let ids = project_document_ids_for_source(&transaction, path)?;
                for id in &ids {
                    delete_document(&transaction, id)?;
                }
                old_project_document_ids.insert(path.clone(), ids);
            }
        }

        for input in documents {
            validate_input(input)?;
            if !seen.insert(input.id.clone()) {
                return Err(CorpusError::DuplicateDocumentId(input.id.clone()));
            }
            let content_sha256 = sha256(input.content.as_bytes());
            let chunks = chunk_content(&input.content);
            let first_citation_json = chunk_citation_json(&input.citation, &chunks[0])?;
            let expected_fingerprint = index_fingerprint(
                layer_name(input.layer),
                &input.title,
                &input.source_identity,
                &content_sha256,
                input.redistribution_cleared,
                &first_citation_json,
            )?;
            let current_fingerprint: Option<String> = transaction
                .query_row(
                    "SELECT d.layer, d.title, d.source_identity, d.content_sha256,
                            d.redistribution_cleared, c.citation_json
                     FROM documents d
                     JOIN chunks c ON c.document_id = d.id AND c.ordinal = 0
                     WHERE d.id = ?1",
                    [&input.id],
                    |row| {
                        let layer = row.get::<_, String>(0)?;
                        let title = row.get::<_, String>(1)?;
                        let source_identity = row.get::<_, String>(2)?;
                        let content_sha256 = row.get::<_, String>(3)?;
                        let redistribution_cleared = row.get::<_, bool>(4)?;
                        let citation_json = row.get::<_, String>(5)?;
                        index_fingerprint(
                            &layer,
                            &title,
                            &source_identity,
                            &content_sha256,
                            redistribution_cleared,
                            &citation_json,
                        )
                        .map_err(|error| {
                            rusqlite::Error::FromSqlConversionFailure(
                                5,
                                rusqlite::types::Type::Text,
                                Box::new(error),
                            )
                        })
                    },
                )
                .optional()?;
            if current_fingerprint.as_deref() == Some(expected_fingerprint.as_str()) {
                unchanged_documents += 1;
                continue;
            }

            delete_document(&transaction, &input.id)?;
            let project_source_path = (input.layer == CorpusLayer::ProjectDocument)
                .then(|| {
                    input
                        .citation
                        .source_path
                        .as_ref()
                        .map(|path| path.to_string_lossy().into_owned())
                })
                .flatten();
            transaction.execute(
                "INSERT INTO documents
                 (id, layer, title, source_identity, content_sha256, redistribution_cleared,
                  project_source_path)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
                params![
                    input.id,
                    layer_name(input.layer),
                    input.title,
                    input.source_identity,
                    content_sha256,
                    input.redistribution_cleared,
                    project_source_path,
                ],
            )?;
            for (ordinal, chunk) in chunks.into_iter().enumerate() {
                let chunk_id = format!("{}:{ordinal}", input.id);
                let citation_json = chunk_citation_json(&input.citation, &chunk)?;
                transaction.execute(
                    "INSERT INTO chunks
                     (id, document_id, ordinal, text, citation_json)
                     VALUES (?1, ?2, ?3, ?4, ?5)",
                    params![
                        chunk_id,
                        input.id,
                        ordinal as u64,
                        chunk.text,
                        citation_json
                    ],
                )?;
                transaction.execute(
                    "INSERT INTO chunks_fts (chunk_id, text) VALUES (?1, ?2)",
                    params![chunk_id, chunk.text],
                )?;
            }
            indexed_documents += 1;
        }

        let mut removed_documents = old_project_document_ids
            .iter()
            .map(|(path, old_ids)| {
                let new_ids = project_document_ids.get(path).cloned().unwrap_or_default();
                old_ids.difference(&new_ids).count()
            })
            .sum();
        for layer in remove_missing_layers {
            let mut statement = transaction.prepare("SELECT id FROM documents WHERE layer = ?1")?;
            let existing: Vec<String> = statement
                .query_map([layer_name(*layer)], |row| row.get(0))?
                .collect::<Result<_, _>>()?;
            drop(statement);
            for id in existing {
                if !seen.contains(&id) {
                    delete_document(&transaction, &id)?;
                    removed_documents += 1;
                }
            }
        }

        if let Some(project_sources) = &project_sources {
            let existing = load_project_source_fingerprints(&transaction)?;
            for path in existing
                .keys()
                .filter(|path| !project_sources.contains_key(*path))
            {
                let ids = project_document_ids_for_source(&transaction, path)?;
                for id in &ids {
                    delete_document(&transaction, id)?;
                }
                removed_documents += ids.len();
                transaction.execute(
                    "DELETE FROM project_sources WHERE relative_path = ?1",
                    [path.to_string_lossy().as_ref()],
                )?;
            }
            for (path, source) in project_sources {
                if failed_project_sources.contains(path) {
                    continue;
                }
                let fingerprint = project_source_fingerprint(source)?;
                let existing_fingerprint = existing.get(path);
                if successful_project_paths.contains(path) {
                    transaction.execute(
                        "INSERT OR REPLACE INTO project_sources
                         (relative_path, fingerprint_json) VALUES (?1, ?2)",
                        params![path.to_string_lossy(), fingerprint],
                    )?;
                } else if existing_fingerprint == Some(&fingerprint) {
                    unchanged_documents +=
                        project_document_ids_for_source(&transaction, path)?.len();
                } else {
                    return Err(CorpusError::InvalidDocument(format!(
                        "changed project source {} supplied neither extracted documents nor a failure marker",
                        path.display()
                    )));
                }
            }
        }
        let chunk_count = transaction.query_row("SELECT COUNT(*) FROM chunks", [], |row| {
            row.get::<_, u64>(0)
        })? as usize;
        transaction.commit()?;
        Ok(CorpusIndexReport {
            schema_version: CORPUS_SCHEMA_VERSION,
            retrieval_engine: "sqlite_fts5_unicode61".to_owned(),
            indexed_documents,
            unchanged_documents,
            removed_documents,
            chunk_count,
        })
    }

    pub fn documents(&self) -> Result<Vec<CorpusDocument>, CorpusError> {
        let connection = self.connection()?;
        let mut statement = connection.prepare(
            "SELECT d.id, d.layer, d.title, d.source_identity, d.content_sha256,
                    d.redistribution_cleared, COUNT(c.id)
             FROM documents d LEFT JOIN chunks c ON c.document_id = d.id
             GROUP BY d.id ORDER BY d.layer, d.title, d.id",
        )?;
        statement
            .query_map([], |row| {
                Ok(CorpusDocument {
                    id: row.get(0)?,
                    layer: parse_layer(&row.get::<_, String>(1)?).map_err(|error| {
                        rusqlite::Error::FromSqlConversionFailure(
                            1,
                            rusqlite::types::Type::Text,
                            Box::new(error),
                        )
                    })?,
                    title: row.get(2)?,
                    source_identity: row.get(3)?,
                    content_sha256: row.get(4)?,
                    redistribution_cleared: row.get(5)?,
                    chunk_count: row.get::<_, u64>(6)? as usize,
                })
            })?
            .collect::<Result<Vec<_>, _>>()
            .map_err(CorpusError::Sqlite)
    }

    pub fn search(&self, query: &str, limit: usize) -> Result<Vec<CorpusSearchHit>, CorpusError> {
        self.search_layers(query, limit, &BTreeSet::new())
    }

    pub fn search_layers(
        &self,
        query: &str,
        limit: usize,
        layers: &BTreeSet<CorpusLayer>,
    ) -> Result<Vec<CorpusSearchHit>, CorpusError> {
        let query = query.trim();
        if query.is_empty() || limit == 0 {
            return Ok(Vec::new());
        }
        let connection = self.connection()?;
        let expression = fts_expression(query);
        if expression.is_empty() {
            return Ok(Vec::new());
        }
        let mut statement = connection.prepare(
            "SELECT c.id, c.document_id, d.layer, d.title, c.text, c.citation_json,
                    bm25(chunks_fts)
             FROM chunks_fts
             JOIN chunks c ON c.id = chunks_fts.chunk_id
             JOIN documents d ON d.id = c.document_id
             WHERE chunks_fts MATCH ?1
               AND (?3 = '' OR instr(',' || ?3 || ',', ',' || d.layer || ',') > 0)
             ORDER BY bm25(chunks_fts), c.id
             LIMIT ?2",
        )?;
        let layer_filter = layers
            .iter()
            .map(|layer| layer_name(*layer))
            .collect::<Vec<_>>()
            .join(",");
        let rows = statement.query_map(params![expression, limit as u64, layer_filter], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, String>(2)?,
                row.get::<_, String>(3)?,
                row.get::<_, String>(4)?,
                row.get::<_, String>(5)?,
                row.get::<_, f64>(6)?,
            ))
        })?;
        let mut hits = Vec::new();
        for row in rows {
            let (chunk_id, document_id, layer, title, text, citation_json, rank) = row?;
            hits.push(CorpusSearchHit {
                chunk_id,
                document_id,
                layer: parse_layer(&layer)?,
                title,
                text,
                score: (1.0 / (1.0 + rank.abs())) as f32,
                citation: serde_json::from_str(&citation_json)?,
                untrusted_evidence: true,
            });
        }
        Ok(hits)
    }

    fn initialize(&self) -> Result<(), CorpusError> {
        let connection = self.connection()?;
        connection.execute_batch(
            "PRAGMA foreign_keys = ON;
             CREATE TABLE IF NOT EXISTS corpus_meta (
                 key TEXT PRIMARY KEY,
                 value TEXT NOT NULL
             );
             CREATE TABLE IF NOT EXISTS documents (
                 id TEXT PRIMARY KEY,
                 layer TEXT NOT NULL,
                 title TEXT NOT NULL,
                 source_identity TEXT NOT NULL,
                 content_sha256 TEXT NOT NULL,
                 redistribution_cleared INTEGER NOT NULL,
                 project_source_path TEXT
             );
             CREATE TABLE IF NOT EXISTS chunks (
                 id TEXT PRIMARY KEY,
                 document_id TEXT NOT NULL REFERENCES documents(id) ON DELETE CASCADE,
                 ordinal INTEGER NOT NULL,
                 text TEXT NOT NULL,
                 citation_json TEXT NOT NULL,
                 UNIQUE(document_id, ordinal)
             );
             CREATE VIRTUAL TABLE IF NOT EXISTS chunks_fts USING fts5(
                 chunk_id UNINDEXED,
                 text,
                 tokenize = 'unicode61'
             );
             CREATE TABLE IF NOT EXISTS project_sources (
                 relative_path TEXT PRIMARY KEY,
                 fingerprint_json TEXT NOT NULL
             );",
        )?;
        let stored_version: Option<String> = connection
            .query_row(
                "SELECT value FROM corpus_meta WHERE key = 'schema_version'",
                [],
                |row| row.get(0),
            )
            .optional()?;
        match stored_version.as_deref() {
            None | Some("3") => {}
            Some("2") => {
                if !table_has_column(&connection, "documents", "project_source_path")? {
                    connection.execute(
                        "ALTER TABLE documents ADD COLUMN project_source_path TEXT",
                        [],
                    )?;
                }
            }
            Some(version) => {
                return Err(CorpusError::UnsupportedSchemaVersion(version.to_owned()));
            }
        }
        // Schema v2 project documents predate the explicit source-path column.
        // Backfill it from the durable per-chunk citation so the first v3
        // incremental replacement can remove every old page from that source.
        connection.execute(
            "UPDATE documents
             SET project_source_path = (
                 SELECT json_extract(c.citation_json, '$.source_path')
                 FROM chunks c
                 WHERE c.document_id = documents.id
                 ORDER BY c.ordinal
                 LIMIT 1
             )
             WHERE layer = 'project_document' AND project_source_path IS NULL",
            [],
        )?;
        connection.execute(
            "INSERT OR REPLACE INTO corpus_meta (key, value) VALUES ('schema_version', ?1)",
            [CORPUS_SCHEMA_VERSION.to_string()],
        )?;
        Ok(())
    }

    fn connection(&self) -> Result<Connection, CorpusError> {
        let connection = Connection::open(self.managed_dir.join("index.sqlite3"))?;
        connection.execute_batch("PRAGMA foreign_keys = ON; PRAGMA journal_mode = WAL;")?;
        Ok(connection)
    }
}

#[derive(Debug)]
struct TextChunk {
    text: String,
    line_start: u32,
    line_end: u32,
}

fn chunk_content(content: &str) -> Vec<TextChunk> {
    let mut chunks = Vec::new();
    let mut current = String::new();
    let mut line_start = 1_u32;
    let mut line_end = 1_u32;
    for (index, line) in content.lines().enumerate() {
        let line_number = index as u32 + 1;
        let addition = line.len() + usize::from(!current.is_empty());
        if !current.is_empty() && current.len().saturating_add(addition) > MAX_CHUNK_BYTES {
            chunks.push(TextChunk {
                text: std::mem::take(&mut current),
                line_start,
                line_end,
            });
            line_start = line_number;
        }
        if !current.is_empty() {
            current.push('\n');
        }
        current.push_str(line);
        line_end = line_number;
    }
    if !current.is_empty() {
        chunks.push(TextChunk {
            text: current,
            line_start,
            line_end,
        });
    }
    if chunks.is_empty() && !content.is_empty() {
        chunks.push(TextChunk {
            text: content.to_owned(),
            line_start: 1,
            line_end: 1,
        });
    }
    chunks
}

fn chunk_citation_json(
    base: &CorpusCitation,
    chunk: &TextChunk,
) -> Result<String, serde_json::Error> {
    let mut citation = base.clone();
    citation.line_start.get_or_insert(chunk.line_start);
    citation.line_end.get_or_insert(chunk.line_end);
    if citation.section.is_none() {
        citation.section = section_or_symbol_hint(&chunk.text);
    }
    serde_json::to_string(&citation)
}

#[derive(Serialize)]
struct DocumentIndexFingerprint<'a> {
    layer: &'a str,
    title: &'a str,
    source_identity: &'a str,
    content_sha256: &'a str,
    redistribution_cleared: bool,
    first_chunk_citation_json: &'a str,
}

fn index_fingerprint(
    layer: &str,
    title: &str,
    source_identity: &str,
    content_sha256: &str,
    redistribution_cleared: bool,
    first_chunk_citation_json: &str,
) -> Result<String, serde_json::Error> {
    let fingerprint = DocumentIndexFingerprint {
        layer,
        title,
        source_identity,
        content_sha256,
        redistribution_cleared,
        first_chunk_citation_json,
    };
    serde_json::to_vec(&fingerprint).map(|bytes| sha256(&bytes))
}

fn validated_project_sources(
    sources: &[ProjectCorpusSource],
) -> Result<BTreeMap<PathBuf, ProjectCorpusSource>, CorpusError> {
    let mut validated = BTreeMap::new();
    for source in sources {
        validate_project_source_path(&source.relative_path)?;
        if source.file_type.trim().is_empty() || source.file_identity.trim().is_empty() {
            return Err(CorpusError::InvalidDocument(format!(
                "project source {} has incomplete file identity metadata",
                source.relative_path.display()
            )));
        }
        let extension = source
            .relative_path
            .extension()
            .and_then(|value| value.to_str())
            .unwrap_or_default();
        if !extension.eq_ignore_ascii_case(&source.file_type) {
            return Err(CorpusError::InvalidDocument(format!(
                "project source {} type does not match its extension",
                source.relative_path.display()
            )));
        }
        if validated
            .insert(source.relative_path.clone(), source.clone())
            .is_some()
        {
            return Err(CorpusError::InvalidDocument(format!(
                "duplicate project source {}",
                source.relative_path.display()
            )));
        }
    }
    Ok(validated)
}

fn validate_project_source_path(path: &Path) -> Result<(), CorpusError> {
    let mut components = path.components();
    if !matches!(components.next(), Some(Component::Normal(value)) if value == "documents")
        || components.next().is_none()
        || path.components().any(|component| {
            matches!(
                component,
                Component::ParentDir
                    | Component::RootDir
                    | Component::Prefix(_)
                    | Component::CurDir
            )
        })
    {
        return Err(CorpusError::InvalidDocument(format!(
            "project source path must be a relative file under documents/: {}",
            path.display()
        )));
    }
    Ok(())
}

fn project_source_fingerprint(source: &ProjectCorpusSource) -> Result<String, CorpusError> {
    serde_json::to_string(source).map_err(CorpusError::Json)
}

fn load_project_source_fingerprints(
    connection: &Connection,
) -> Result<BTreeMap<PathBuf, String>, rusqlite::Error> {
    let mut statement = connection.prepare(
        "SELECT relative_path, fingerprint_json FROM project_sources ORDER BY relative_path",
    )?;
    statement
        .query_map([], |row| {
            Ok((PathBuf::from(row.get::<_, String>(0)?), row.get(1)?))
        })?
        .collect()
}

fn project_document_ids_for_source(
    connection: &Connection,
    path: &Path,
) -> Result<BTreeSet<String>, rusqlite::Error> {
    let mut statement = connection.prepare(
        "SELECT id FROM documents WHERE layer = 'project_document'
         AND project_source_path = ?1 ORDER BY id",
    )?;
    statement
        .query_map([path.to_string_lossy().as_ref()], |row| row.get(0))?
        .collect()
}

fn table_has_column(
    connection: &Connection,
    table: &str,
    column: &str,
) -> Result<bool, rusqlite::Error> {
    let mut statement = connection.prepare(&format!("PRAGMA table_info({table})"))?;
    let names = statement
        .query_map([], |row| row.get::<_, String>(1))?
        .collect::<Result<Vec<_>, _>>()?;
    Ok(names.iter().any(|name| name == column))
}

fn section_or_symbol_hint(text: &str) -> Option<String> {
    const DECLARATIONS: [&str; 14] = [
        "pub fn ",
        "fn ",
        "pub struct ",
        "struct ",
        "pub enum ",
        "enum ",
        "pub trait ",
        "trait ",
        "impl ",
        "package func ",
        "public func ",
        "private func ",
        "func ",
        "class ",
    ];
    for line in text.lines() {
        let trimmed = line.trim();
        if let Some(heading) = trimmed.strip_prefix('#') {
            let heading = heading.trim_start_matches('#').trim();
            if !heading.is_empty() {
                return Some(heading.chars().take(160).collect());
            }
        }
        if DECLARATIONS
            .iter()
            .any(|prefix| trimmed.starts_with(prefix))
        {
            return Some(trimmed.chars().take(160).collect());
        }
    }
    None
}

fn fts_expression(query: &str) -> String {
    tokens(query)
        .into_iter()
        .take(16)
        .map(|term| format!("\"{}\"", term.replace('"', "\"\"")))
        .collect::<Vec<_>>()
        .join(" OR ")
}

fn tokens(text: &str) -> Vec<String> {
    text.split(|character: char| !character.is_alphanumeric() && character != '_')
        .filter(|term| term.len() > 1)
        .map(str::to_lowercase)
        .collect()
}

fn delete_document(connection: &Connection, id: &str) -> Result<(), rusqlite::Error> {
    connection.execute(
        "DELETE FROM chunks_fts WHERE chunk_id IN
         (SELECT id FROM chunks WHERE document_id = ?1)",
        [id],
    )?;
    connection.execute("DELETE FROM documents WHERE id = ?1", [id])?;
    Ok(())
}

fn validate_input(input: &CorpusDocumentInput) -> Result<(), CorpusError> {
    if input.id.trim().is_empty() || input.title.trim().is_empty() {
        return Err(CorpusError::InvalidDocument(
            "document id and title must not be empty".to_owned(),
        ));
    }
    if input.content.trim().is_empty() {
        return Err(CorpusError::InvalidDocument(format!(
            "document {} has no extracted content",
            input.id
        )));
    }
    if let Some(path) = &input.citation.source_path
        && (path.is_absolute()
            || path
                .components()
                .any(|component| matches!(component, Component::ParentDir | Component::RootDir)))
    {
        return Err(CorpusError::InvalidDocument(format!(
            "document {} citation path must be project/release relative",
            input.id
        )));
    }
    if input.layer == CorpusLayer::Baseline && !input.redistribution_cleared {
        return Err(CorpusError::BaselineRedistributionNotCleared(
            input.id.clone(),
        ));
    }
    Ok(())
}

fn layer_name(layer: CorpusLayer) -> &'static str {
    match layer {
        CorpusLayer::Baseline => "baseline",
        CorpusLayer::ProjectDocument => "project_document",
        CorpusLayer::ReleaseSource => "release_source",
        CorpusLayer::LiveSource => "live_source",
    }
}

fn parse_layer(value: &str) -> Result<CorpusLayer, CorpusError> {
    match value {
        "baseline" => Ok(CorpusLayer::Baseline),
        "project_document" => Ok(CorpusLayer::ProjectDocument),
        "release_source" => Ok(CorpusLayer::ReleaseSource),
        "live_source" => Ok(CorpusLayer::LiveSource),
        other => Err(CorpusError::InvalidLayer(other.to_owned())),
    }
}

fn sha256(bytes: &[u8]) -> String {
    format!("{:x}", Sha256::digest(bytes))
}

#[derive(Debug, Error)]
pub enum CorpusError {
    #[error("corpus project root must be absolute: {path}")]
    ProjectRootMustBeAbsolute { path: PathBuf },
    #[error("corpus project root is not a directory: {path}")]
    ProjectRootNotDirectory { path: PathBuf },
    #[error("duplicate corpus document id {0}")]
    DuplicateDocumentId(String),
    #[error("baseline corpus document {0} is not redistribution-cleared")]
    BaselineRedistributionNotCleared(String),
    #[error("invalid corpus document: {0}")]
    InvalidDocument(String),
    #[error("unsupported corpus schema version {0}")]
    UnsupportedSchemaVersion(String),
    #[error("invalid corpus layer {0}")]
    InvalidLayer(String),
    #[error("corpus SQLite operation failed: {0}")]
    Sqlite(#[from] rusqlite::Error),
    #[error("corpus JSON operation failed: {0}")]
    Json(#[from] serde_json::Error),
    #[error("failed to {action} at {path}: {source}")]
    Io {
        action: &'static str,
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
}

// SPDX-License-Identifier: LGPL-3.0-or-later

//! Local layered assistant corpus with SQLite FTS and exact vector search.

use std::{
    collections::{BTreeMap, BTreeSet},
    fs::{self, File},
    io::{Read, Write},
    path::{Component, Path, PathBuf},
};

use rusqlite::{Connection, OptionalExtension, params};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use tempfile::NamedTempFile;
use thiserror::Error;

pub const CORPUS_SCHEMA_VERSION: u32 = 1;
pub const CORPUS_EMBEDDING_MODEL_VERSION: &str = "casa-rs-feature-hash-v1";
pub const CORPUS_EMBEDDING_DIMENSIONS: usize = 384;

const EMBEDDING_MAGIC: &[u8; 8] = b"CASAEMB1";
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
    pub embedding_model: String,
    pub indexed_documents: usize,
    pub unchanged_documents: usize,
    pub removed_documents: usize,
    pub chunk_count: usize,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct CorpusSearchHit {
    pub chunk_id: String,
    pub document_id: String,
    pub layer: CorpusLayer,
    pub title: String,
    pub text: String,
    pub score: f32,
    pub cosine_score: f32,
    pub keyword_score: f32,
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
        let mut connection = self.connection()?;
        let transaction = connection.transaction()?;
        let mut indexed_documents = 0;
        let mut unchanged_documents = 0;
        let mut seen = BTreeSet::new();

        for input in documents {
            validate_input(input)?;
            if !seen.insert(input.id.clone()) {
                return Err(CorpusError::DuplicateDocumentId(input.id.clone()));
            }
            let content_sha256 = sha256(input.content.as_bytes());
            let current_hash: Option<String> = transaction
                .query_row(
                    "SELECT content_sha256 FROM documents WHERE id = ?1",
                    [&input.id],
                    |row| row.get(0),
                )
                .optional()?;
            if current_hash.as_deref() == Some(content_sha256.as_str()) {
                unchanged_documents += 1;
                continue;
            }

            delete_document(&transaction, &input.id)?;
            transaction.execute(
                "INSERT INTO documents
                 (id, layer, title, source_identity, content_sha256, redistribution_cleared)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
                params![
                    input.id,
                    layer_name(input.layer),
                    input.title,
                    input.source_identity,
                    content_sha256,
                    input.redistribution_cleared,
                ],
            )?;
            for (ordinal, chunk) in chunk_content(&input.content).into_iter().enumerate() {
                let chunk_id = format!("{}:{ordinal}", input.id);
                let mut citation = input.citation.clone();
                citation.line_start.get_or_insert(chunk.line_start);
                citation.line_end.get_or_insert(chunk.line_end);
                if citation.section.is_none() {
                    citation.section = section_or_symbol_hint(&chunk.text);
                }
                let citation_json = serde_json::to_string(&citation)?;
                transaction.execute(
                    "INSERT INTO chunks
                     (id, document_id, ordinal, text, citation_json, embedding_row)
                     VALUES (?1, ?2, ?3, ?4, ?5, -1)",
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

        let mut removed_documents = 0;
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
        transaction.commit()?;
        let chunk_count = self.rebuild_embedding_matrix()?;
        Ok(CorpusIndexReport {
            schema_version: CORPUS_SCHEMA_VERSION,
            embedding_model: CORPUS_EMBEDDING_MODEL_VERSION.to_owned(),
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
        let query = query.trim();
        if query.is_empty() || limit == 0 {
            return Ok(Vec::new());
        }
        let query_embedding = embed(query);
        let matrix = self.read_embedding_matrix()?;
        let connection = self.connection()?;
        let keyword_scores = keyword_scores(&connection, query, limit.saturating_mul(8))?;
        let mut statement = connection.prepare(
            "SELECT c.id, c.document_id, d.layer, d.title, c.text, c.citation_json,
                    c.embedding_row
             FROM chunks c JOIN documents d ON d.id = c.document_id
             ORDER BY c.embedding_row",
        )?;
        let rows = statement.query_map([], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, String>(2)?,
                row.get::<_, String>(3)?,
                row.get::<_, String>(4)?,
                row.get::<_, String>(5)?,
                row.get::<_, i64>(6)?,
            ))
        })?;
        let mut hits = Vec::new();
        for row in rows {
            let (chunk_id, document_id, layer, title, text, citation_json, embedding_row) = row?;
            let row = usize::try_from(embedding_row)
                .map_err(|_| CorpusError::InvalidEmbeddingRow(embedding_row))?;
            let start = row.saturating_mul(CORPUS_EMBEDDING_DIMENSIONS);
            let end = start.saturating_add(CORPUS_EMBEDDING_DIMENSIONS);
            let embedding = matrix
                .get(start..end)
                .ok_or(CorpusError::InvalidEmbeddingRow(embedding_row))?;
            let cosine_score = dot(&query_embedding, embedding);
            let keyword_score = keyword_scores.get(&chunk_id).copied().unwrap_or(0.0);
            let score = 0.72 * cosine_score + 0.28 * keyword_score;
            hits.push(CorpusSearchHit {
                chunk_id,
                document_id,
                layer: parse_layer(&layer)?,
                title,
                text,
                score,
                cosine_score,
                keyword_score,
                citation: serde_json::from_str(&citation_json)?,
                untrusted_evidence: true,
            });
        }
        hits.sort_by(|left, right| {
            right
                .score
                .total_cmp(&left.score)
                .then_with(|| left.chunk_id.cmp(&right.chunk_id))
        });
        hits.truncate(limit);
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
                 redistribution_cleared INTEGER NOT NULL
             );
             CREATE TABLE IF NOT EXISTS chunks (
                 id TEXT PRIMARY KEY,
                 document_id TEXT NOT NULL REFERENCES documents(id) ON DELETE CASCADE,
                 ordinal INTEGER NOT NULL,
                 text TEXT NOT NULL,
                 citation_json TEXT NOT NULL,
                 embedding_row INTEGER NOT NULL,
                 UNIQUE(document_id, ordinal)
             );
             CREATE VIRTUAL TABLE IF NOT EXISTS chunks_fts USING fts5(
                 chunk_id UNINDEXED,
                 text,
                 tokenize = 'unicode61'
             );",
        )?;
        let stored_version: Option<String> = connection
            .query_row(
                "SELECT value FROM corpus_meta WHERE key = 'schema_version'",
                [],
                |row| row.get(0),
            )
            .optional()?;
        if let Some(version) = stored_version
            && version != CORPUS_SCHEMA_VERSION.to_string()
        {
            return Err(CorpusError::UnsupportedSchemaVersion(version));
        }
        connection.execute(
            "INSERT OR REPLACE INTO corpus_meta (key, value) VALUES ('schema_version', ?1)",
            [CORPUS_SCHEMA_VERSION.to_string()],
        )?;
        connection.execute(
            "INSERT OR REPLACE INTO corpus_meta (key, value) VALUES ('embedding_model', ?1)",
            [CORPUS_EMBEDDING_MODEL_VERSION],
        )?;
        Ok(())
    }

    fn connection(&self) -> Result<Connection, CorpusError> {
        let connection = Connection::open(self.managed_dir.join("index.sqlite3"))?;
        connection.execute_batch("PRAGMA foreign_keys = ON; PRAGMA journal_mode = WAL;")?;
        Ok(connection)
    }

    fn rebuild_embedding_matrix(&self) -> Result<usize, CorpusError> {
        let mut connection = self.connection()?;
        let transaction = connection.transaction()?;
        let rows: Vec<(String, String)> = {
            let mut statement = transaction.prepare("SELECT id, text FROM chunks ORDER BY id")?;
            statement
                .query_map([], |row| Ok((row.get(0)?, row.get(1)?)))?
                .collect::<Result<_, _>>()?
        };
        let mut temporary =
            NamedTempFile::new_in(&self.managed_dir).map_err(|source| CorpusError::Io {
                action: "create embedding matrix temporary file",
                path: self.managed_dir.clone(),
                source,
            })?;
        temporary
            .write_all(EMBEDDING_MAGIC)
            .map_err(|source| CorpusError::Io {
                action: "write embedding matrix header",
                path: temporary.path().to_owned(),
                source,
            })?;
        temporary
            .write_all(&(CORPUS_EMBEDDING_DIMENSIONS as u32).to_le_bytes())
            .and_then(|()| temporary.write_all(&(rows.len() as u64).to_le_bytes()))
            .map_err(|source| CorpusError::Io {
                action: "write embedding matrix shape",
                path: temporary.path().to_owned(),
                source,
            })?;
        for (row, (id, text)) in rows.iter().enumerate() {
            transaction.execute(
                "UPDATE chunks SET embedding_row = ?1 WHERE id = ?2",
                params![row as u64, id],
            )?;
            for value in embed(text) {
                temporary
                    .write_all(&value.to_le_bytes())
                    .map_err(|source| CorpusError::Io {
                        action: "write embedding matrix row",
                        path: temporary.path().to_owned(),
                        source,
                    })?;
            }
        }
        temporary
            .as_file()
            .sync_all()
            .map_err(|source| CorpusError::Io {
                action: "sync embedding matrix",
                path: temporary.path().to_owned(),
                source,
            })?;
        temporary
            .persist(self.managed_dir.join("embeddings-v1.f32"))
            .map_err(|error| CorpusError::Io {
                action: "persist embedding matrix",
                path: self.managed_dir.join("embeddings-v1.f32"),
                source: error.error,
            })?;
        transaction.commit()?;
        Ok(rows.len())
    }

    fn read_embedding_matrix(&self) -> Result<Vec<f32>, CorpusError> {
        let path = self.managed_dir.join("embeddings-v1.f32");
        let mut file = File::open(&path).map_err(|source| CorpusError::Io {
            action: "open embedding matrix",
            path: path.clone(),
            source,
        })?;
        let mut header = [0_u8; 20];
        file.read_exact(&mut header)
            .map_err(|source| CorpusError::Io {
                action: "read embedding matrix header",
                path: path.clone(),
                source,
            })?;
        if &header[..8] != EMBEDDING_MAGIC {
            return Err(CorpusError::InvalidEmbeddingMatrix(
                "invalid magic".to_owned(),
            ));
        }
        let dimensions = u32::from_le_bytes(header[8..12].try_into().expect("dimension bytes"));
        let rows = u64::from_le_bytes(header[12..20].try_into().expect("row bytes"));
        if dimensions as usize != CORPUS_EMBEDDING_DIMENSIONS {
            return Err(CorpusError::InvalidEmbeddingMatrix(
                "embedding dimension does not match model".to_owned(),
            ));
        }
        let value_count = usize::try_from(rows)
            .ok()
            .and_then(|rows| rows.checked_mul(CORPUS_EMBEDDING_DIMENSIONS))
            .ok_or_else(|| CorpusError::InvalidEmbeddingMatrix("matrix is too large".to_owned()))?;
        let mut bytes = vec![0_u8; value_count.saturating_mul(4)];
        file.read_exact(&mut bytes)
            .map_err(|source| CorpusError::Io {
                action: "read embedding matrix values",
                path: path.clone(),
                source,
            })?;
        let mut trailing = [0_u8; 1];
        if file.read(&mut trailing).map_err(|source| CorpusError::Io {
            action: "check embedding matrix length",
            path,
            source,
        })? != 0
        {
            return Err(CorpusError::InvalidEmbeddingMatrix(
                "matrix has trailing bytes".to_owned(),
            ));
        }
        Ok(bytes
            .chunks_exact(4)
            .map(|bytes| f32::from_le_bytes(bytes.try_into().expect("f32 bytes")))
            .collect())
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

fn keyword_scores(
    connection: &Connection,
    query: &str,
    limit: usize,
) -> Result<BTreeMap<String, f32>, CorpusError> {
    let terms: Vec<String> = tokens(query)
        .into_iter()
        .take(16)
        .map(|term| format!("\"{}\"", term.replace('"', "\"\"")))
        .collect();
    if terms.is_empty() {
        return Ok(BTreeMap::new());
    }
    let expression = terms.join(" OR ");
    let mut statement = connection.prepare(
        "SELECT chunk_id, bm25(chunks_fts) FROM chunks_fts
         WHERE chunks_fts MATCH ?1 ORDER BY bm25(chunks_fts) LIMIT ?2",
    )?;
    let pairs: Vec<(String, f64)> = statement
        .query_map(params![expression, limit as u64], |row| {
            Ok((row.get(0)?, row.get(1)?))
        })?
        .collect::<Result<_, _>>()?;
    Ok(pairs
        .into_iter()
        .map(|(id, rank)| (id, (1.0 / (1.0 + rank.abs())) as f32))
        .collect())
}

fn embed(text: &str) -> Vec<f32> {
    let mut vector = vec![0.0_f32; CORPUS_EMBEDDING_DIMENSIONS];
    let terms = tokens(text);
    for term in &terms {
        add_feature(&mut vector, term, 1.0);
    }
    for pair in terms.windows(2) {
        add_feature(&mut vector, &format!("{} {}", pair[0], pair[1]), 0.65);
    }
    let norm = vector.iter().map(|value| value * value).sum::<f32>().sqrt();
    if norm > 0.0 {
        for value in &mut vector {
            *value /= norm;
        }
    }
    vector
}

fn add_feature(vector: &mut [f32], feature: &str, weight: f32) {
    let digest = Sha256::digest(feature.as_bytes());
    let index = u64::from_le_bytes(digest[..8].try_into().expect("feature index bytes")) as usize
        % vector.len();
    let sign = if digest[8] & 1 == 0 { 1.0 } else { -1.0 };
    vector[index] += sign * weight;
}

fn tokens(text: &str) -> Vec<String> {
    text.split(|character: char| !character.is_alphanumeric() && character != '_')
        .filter(|term| term.len() > 1)
        .map(str::to_lowercase)
        .collect()
}

fn dot(left: &[f32], right: &[f32]) -> f32 {
    left.iter()
        .zip(right)
        .map(|(left, right)| left * right)
        .sum()
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
    #[error("invalid embedding row {0}")]
    InvalidEmbeddingRow(i64),
    #[error("invalid embedding matrix: {0}")]
    InvalidEmbeddingMatrix(String),
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

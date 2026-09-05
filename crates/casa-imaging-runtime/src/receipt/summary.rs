// SPDX-License-Identifier: LGPL-3.0-or-later

//! Shared admission/retention projections verified against current receipt bytes.

use std::{
    io::{Read, Seek},
    mem::size_of,
};

use super::*;

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct ReceiptSummary {
    pub(crate) attempt: ExecutionAttemptId,
    pub(crate) attempt_identity: String,
    pub(crate) status: ReceiptStatus,
    pub(crate) retention_bytes: u64,
    pub(crate) order_millis: u64,
    pub(crate) infeasibility: Option<ReceiptQuantitativeInfeasibility>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct ReceiptQuantitativeInfeasibility {
    pub(crate) problem: [u8; 32],
    pub(crate) physical_work: [u8; 32],
    pub(crate) resource_policy: [u8; 32],
    pub(crate) alternative: crate::AlternativeId,
    pub(crate) resource_identity: ResourceIdentity,
    pub(crate) required: u64,
    pub(crate) available: u64,
}

impl ReceiptSummary {
    fn from_document(document: ReceiptDocument, file_bytes: u64) -> Result<Self, ReceiptError> {
        let receipt = ExecutionReceipt {
            schema_version: document.schema.version,
            body: document.receipt,
        };
        let status = receipt.status();
        let retention_bytes = if status.is_terminal() {
            file_bytes
        } else {
            file_bytes.max(worst_case_receipt_bytes(&receipt.body)?)
        };
        let infeasibility = if matches!(
            status,
            ReceiptStatus::Failed | ReceiptStatus::Aborted | ReceiptStatus::Infeasible
        ) && receipt.failure_kind()
            == Some(ReceiptFailureKind::ResourceInfeasible)
        {
            match receipt.infeasibility_certificate() {
                Some(ReceiptInfeasibilityCertificate::Infeasible {
                    resource_identity,
                    required,
                    available,
                    ..
                }) => Some(ReceiptQuantitativeInfeasibility {
                    problem: receipt.problem_identity(),
                    physical_work: receipt.dag_identity(),
                    resource_policy: receipt.resource_policy_identity(),
                    alternative: receipt.selected_alternative_projection().id,
                    resource_identity,
                    required,
                    available,
                }),
                _ => None,
            }
        } else {
            None
        };
        Ok(Self {
            attempt: receipt.attempt_id(),
            attempt_identity: receipt.body.attempt_identity.clone(),
            status,
            retention_bytes,
            order_millis: receipt
                .body
                .finished_unix_millis
                .unwrap_or(receipt.body.started_unix_millis),
            infeasibility,
        })
    }
}

#[derive(Clone, Debug)]
struct CachedSummary {
    digest: [u8; 32],
    file_bytes: u64,
    charged_bytes: u64,
    summary: ReceiptSummary,
}

#[derive(Debug, Default)]
pub(super) struct ReceiptSummaryCache {
    entries: BTreeMap<PathBuf, CachedSummary>,
    charged_bytes: u64,
    #[cfg(test)]
    stats: ReceiptSummaryCacheStats,
}

#[cfg(test)]
#[derive(Clone, Copy, Debug, Default)]
pub(super) struct ReceiptSummaryCacheStats {
    pub(super) entries: usize,
    pub(super) charged_bytes: u64,
    pub(super) full_decodes: u64,
    pub(super) hits: u64,
    pub(super) bytes_hashed: u64,
}

impl ReceiptSummaryCache {
    pub(super) fn remove(&mut self, path: &Path) {
        if let Some(removed) = self.entries.remove(path) {
            self.charged_bytes -= removed.charged_bytes;
        }
    }

    fn retain_paths(&mut self, paths: &BTreeSet<PathBuf>) {
        self.entries.retain(|path, entry| {
            if paths.contains(path) {
                true
            } else {
                self.charged_bytes -= entry.charged_bytes;
                false
            }
        });
    }

    fn insert(&mut self, path: PathBuf, value: CachedSummary, retention: ReceiptRetention) {
        self.remove(&path);
        if value.charged_bytes > retention.max_bytes {
            return;
        }
        while self.entries.len() >= retention.max_receipts
            || self.charged_bytes.saturating_add(value.charged_bytes) > retention.max_bytes
        {
            let Some((_, removed)) = self.entries.pop_first() else {
                break;
            };
            self.charged_bytes -= removed.charged_bytes;
        }
        self.charged_bytes += value.charged_bytes;
        self.entries.insert(path, value);
    }

    #[cfg(test)]
    pub(super) fn stats(&self) -> ReceiptSummaryCacheStats {
        ReceiptSummaryCacheStats {
            entries: self.entries.len(),
            charged_bytes: self.charged_bytes,
            ..self.stats
        }
    }
}

impl ExecutionReceiptStore {
    /// Read current, integrity-checked admission evidence without retaining full bodies.
    pub(crate) fn summaries(&self) -> Result<Vec<ReceiptSummary>, ReceiptError> {
        let attempts = self.attempts()?;
        let paths = attempts
            .iter()
            .map(|attempt| self.receipt_path(*attempt))
            .collect::<BTreeSet<_>>();
        self.state
            .summaries
            .lock()
            .map_err(|_| ReceiptError::InvalidStore)?
            .retain_paths(&paths);
        attempts
            .into_iter()
            .map(|attempt| {
                let summary = self.validated_summary(&self.receipt_path(attempt))?;
                if summary.attempt_identity != attempt.to_string() {
                    return Err(ReceiptError::AttemptMismatch);
                }
                Ok(summary)
            })
            .collect()
    }

    pub(super) fn validated_summary(&self, path: &Path) -> Result<ReceiptSummary, ReceiptError> {
        let cached = self
            .state
            .summaries
            .lock()
            .map_err(|_| ReceiptError::InvalidStore)?
            .entries
            .get(path)
            .cloned();
        let mut file = File::open(path).map_err(read_error)?;
        if let Some(cached) = cached {
            let (digest, file_bytes) = current_digest(&mut file)?;
            #[cfg(test)]
            {
                let mut cache = self.state.summaries.lock().unwrap();
                cache.stats.bytes_hashed += file_bytes;
                if digest == cached.digest && file_bytes == cached.file_bytes {
                    cache.stats.hits += 1;
                }
            }
            if digest == cached.digest && file_bytes == cached.file_bytes {
                return Ok(cached.summary);
            }
            file.rewind().map_err(read_error)?;
        }
        let mut bytes = Vec::new();
        file.read_to_end(&mut bytes).map_err(read_error)?;
        let file_bytes = u64::try_from(bytes.len()).unwrap_or(u64::MAX);
        let digest = Sha256::digest(&bytes).into();
        #[cfg(test)]
        {
            let mut cache = self.state.summaries.lock().unwrap();
            cache.stats.full_decodes += 1;
            cache.stats.bytes_hashed += file_bytes;
        }
        // The digest and canonical validation cover the very same byte snapshot.
        let summary = ReceiptSummary::from_document(decode_document(&bytes)?, file_bytes)?;
        // Charge the whole source document, rather than estimating each projection string.
        // Fixed cache-node storage is additionally bounded by max_receipts.
        let charged_bytes = file_bytes
            .saturating_add(path.as_os_str().as_encoded_bytes().len() as u64)
            .saturating_add(size_of::<CachedSummary>() as u64)
            .saturating_add(size_of::<PathBuf>() as u64);
        self.state
            .summaries
            .lock()
            .map_err(|_| ReceiptError::InvalidStore)?
            .insert(
                path.to_path_buf(),
                CachedSummary {
                    digest,
                    file_bytes,
                    charged_bytes,
                    summary: summary.clone(),
                },
                self.state.retention,
            );
        Ok(summary)
    }
}

fn current_digest(file: &mut File) -> Result<([u8; 32], u64), ReceiptError> {
    let mut buffer = [0_u8; 64 * 1024];
    let mut hasher = Sha256::new();
    let mut bytes = 0_u64;
    loop {
        let read = match file.read(&mut buffer) {
            Ok(read) => read,
            Err(error) if error.kind() == std::io::ErrorKind::Interrupted => continue,
            Err(error) => return Err(read_error(error)),
        };
        if read == 0 {
            break;
        }
        hasher.update(&buffer[..read]);
        bytes = bytes.saturating_add(read as u64);
    }
    Ok((hasher.finalize().into(), bytes))
}

fn read_error(source: std::io::Error) -> ReceiptError {
    ReceiptError::Io {
        action: "read retained execution receipt",
        source,
    }
}

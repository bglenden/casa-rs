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
    encoded_bytes: Arc<[u8]>,
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
    pub(super) bytes_compared: u64,
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
    /// Read current, integrity-checked admission evidence without decoding unchanged bodies.
    pub(crate) fn summaries(&self) -> Result<Vec<ReceiptSummary>, ReceiptError> {
        if boundary_probe_enabled() {
            eprintln!("t51_receipt_history boundary=planning");
        }
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
            let comparison = current_bytes_match(&mut file, &cached.encoded_bytes)?;
            if boundary_probe_enabled() {
                eprintln!(
                    "t51_receipt_current_bytes bytes={} unchanged={}",
                    comparison.1, comparison.0
                );
            }
            #[cfg(test)]
            {
                let mut cache = self.state.summaries.lock().unwrap();
                cache.stats.bytes_compared += comparison.1;
                if comparison.0 {
                    cache.stats.hits += 1;
                }
            }
            if comparison.0 {
                return Ok(cached.summary);
            }
            file.rewind().map_err(read_error)?;
        }
        let mut bytes = Vec::new();
        file.read_to_end(&mut bytes).map_err(read_error)?;
        let file_bytes = u64::try_from(bytes.len()).unwrap_or(u64::MAX);
        #[cfg(test)]
        {
            let mut cache = self.state.summaries.lock().unwrap();
            cache.stats.full_decodes += 1;
        }
        // Exact byte equality reuses the validation of this same snapshot.
        let summary = ReceiptSummary::from_document(decode_document(&bytes)?, file_bytes)?;
        // Charge the encoded snapshot and a source-sized bound for the projection.
        // Fixed cache-node and Arc storage is additionally bounded by max_receipts.
        let charged_bytes = file_bytes
            .saturating_mul(2)
            .saturating_add(path.as_os_str().as_encoded_bytes().len() as u64)
            .saturating_add(size_of::<CachedSummary>() as u64)
            .saturating_add(size_of::<PathBuf>() as u64)
            .saturating_add((2 * size_of::<usize>()) as u64);
        self.state
            .summaries
            .lock()
            .map_err(|_| ReceiptError::InvalidStore)?
            .insert(
                path.to_path_buf(),
                CachedSummary {
                    encoded_bytes: bytes.into(),
                    charged_bytes,
                    summary: summary.clone(),
                },
                self.state.retention,
            );
        Ok(summary)
    }
}

fn current_bytes_match(file: &mut impl Read, expected: &[u8]) -> Result<(bool, u64), ReceiptError> {
    let mut buffer = [0_u8; 64 * 1024];
    let mut offset = 0_usize;
    loop {
        let read = match file.read(&mut buffer) {
            Ok(read) => read,
            Err(error) if error.kind() == std::io::ErrorKind::Interrupted => continue,
            Err(error) => return Err(read_error(error)),
        };
        if read == 0 {
            return Ok((offset == expected.len(), offset as u64));
        }
        let end = offset.saturating_add(read);
        if expected.get(offset..end) != Some(&buffer[..read]) {
            return Ok((false, end as u64));
        }
        offset = end;
    }
}

fn read_error(source: std::io::Error) -> ReceiptError {
    ReceiptError::Io {
        action: "read retained execution receipt",
        source,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn receipt_byte_comparison_detects_changes_and_both_length_mismatches() {
        let bytes = (0..131_073)
            .map(|index| (index % 251) as u8)
            .collect::<Vec<_>>();
        assert_eq!(
            current_bytes_match(&mut &bytes[..], &bytes).unwrap(),
            (true, bytes.len() as u64)
        );
        assert_eq!(current_bytes_match(&mut &b""[..], b"").unwrap(), (true, 0));
        for index in [0, 65_536, bytes.len() - 1] {
            let mut changed = bytes.clone();
            changed[index] ^= 1;
            assert!(!current_bytes_match(&mut &changed[..], &bytes).unwrap().0);
        }
        assert!(
            !current_bytes_match(&mut &bytes[..bytes.len() - 1], &bytes)
                .unwrap()
                .0
        );
        let mut extra = bytes.clone();
        extra.push(0);
        assert!(!current_bytes_match(&mut &extra[..], &bytes).unwrap().0);
    }

    #[test]
    fn receipt_byte_comparison_handles_short_reads_interrupts_and_io_failure() {
        struct Reader<'a> {
            bytes: &'a [u8],
            error: Option<std::io::ErrorKind>,
        }
        impl Read for Reader<'_> {
            fn read(&mut self, buffer: &mut [u8]) -> std::io::Result<usize> {
                if let Some(error) = self.error.take() {
                    return Err(error.into());
                }
                let end = buffer.len().min(13);
                self.bytes.read(&mut buffer[..end])
            }
        }
        let bytes = [7; 67];
        let mut reader = Reader {
            bytes: &bytes,
            error: Some(std::io::ErrorKind::Interrupted),
        };
        assert_eq!(
            current_bytes_match(&mut reader, &bytes).unwrap(),
            (true, 67)
        );
        let mut reader = Reader {
            bytes: &bytes,
            error: Some(std::io::ErrorKind::PermissionDenied),
        };
        assert!(matches!(
            current_bytes_match(&mut reader, &bytes),
            Err(ReceiptError::Io { .. })
        ));
    }
}

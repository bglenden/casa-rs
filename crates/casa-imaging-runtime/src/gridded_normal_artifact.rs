// SPDX-License-Identifier: LGPL-3.0-or-later

use std::collections::{BTreeMap, BTreeSet};
use std::fs::File;
use std::io::{self, Write};
use std::os::unix::fs::{FileExt, MetadataExt};
use std::path::{Path, PathBuf};

use sha2::{Digest, Sha256};
use tempfile::Builder;
use thiserror::Error;

use crate::execution_bindings::IoMeasurement;
use crate::resource_authority::{
    AlternativeId, CacheDemand, CapabilityPredicate, CountDemand, DemandAlternative,
    DemandAlternatives, DemandEnvelope, IoBufferDemand, IoBufferKind, MemoryDemand, MemoryViewKind,
    QuiescencePoint, ResourceAuthority, ResourceError, ResourceHeadroom, ResourceLease,
    ResourcePolicy, RuntimeOverheadDemand, ScalingMetadata, StorageDemand,
    StorageIoResourceBinding,
};

const FORMAT_VERSION: u32 = 1;
const FILE_HEADER_MAGIC: [u8; 8] = *b"CGNRHDR\0";
const FRAME_MAGIC: [u8; 8] = *b"CGNRFRM\0";
const FOOTER_MAGIC: [u8; 8] = *b"CGNRFTR\0";
const FILE_HEADER_BYTES: usize = 16;
const FRAME_HEADER_BYTES: usize = 72;
const FOOTER_BYTES: usize = 80;
const STORAGE_DEMAND_ID: &str = "cross-plan-gridded-normal-replay";
const BUFFER_ALLOCATION_ID: &str = "cross-plan-gridded-normal-replay-buffer";

/// Authority-validated writable location for one run's private replay spill.
///
/// The directory is deliberately opaque outside runtime. Planning can inspect
/// only the typed storage resources while the artifact owner retains the exact
/// location needed to create its unlinked temporary file.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct GriddedNormalReplayStorage {
    resources: StorageIoResourceBinding,
    directory: PathBuf,
}

impl GriddedNormalReplayStorage {
    /// Bind one writable directory to the calibrated storage domain that owns it.
    pub fn bind(
        authority: &ResourceAuthority,
        resources: StorageIoResourceBinding,
        directory: impl AsRef<Path>,
    ) -> io::Result<Self> {
        let directory = validate_storage_directory(authority, &resources, directory.as_ref())
            .map_err(io::Error::other)?;
        Ok(Self {
            resources,
            directory,
        })
    }

    /// Return the path-free storage resources used by physical planning.
    #[must_use]
    pub const fn resources(&self) -> &StorageIoResourceBinding {
        &self.resources
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct GriddedNormalArtifactBudget {
    maximum_artifact_bytes: u64,
    maximum_frame_payload_bytes: usize,
    io_buffer_bytes: u64,
}

impl GriddedNormalArtifactBudget {
    pub(crate) fn for_bounded_stream(
        maximum_payload_bytes: u64,
        maximum_frame_payload_bytes: usize,
        maximum_frame_count: u64,
    ) -> Result<Self, GriddedNormalArtifactError> {
        let frame_headers = maximum_frame_count
            .checked_mul(FRAME_HEADER_BYTES as u64)
            .ok_or(GriddedNormalArtifactError::ArithmeticOverflow(
                "artifact frame headers",
            ))?;
        let maximum_artifact_bytes = (FILE_HEADER_BYTES as u64)
            .checked_add(frame_headers)
            .and_then(|bytes| bytes.checked_add(maximum_payload_bytes))
            .and_then(|bytes| bytes.checked_add(FOOTER_BYTES as u64))
            .ok_or(GriddedNormalArtifactError::ArithmeticOverflow(
                "bounded artifact bytes",
            ))?;
        Self::new(maximum_artifact_bytes, maximum_frame_payload_bytes)
    }

    pub(crate) fn new(
        maximum_artifact_bytes: u64,
        maximum_frame_payload_bytes: usize,
    ) -> Result<Self, GriddedNormalArtifactError> {
        if maximum_frame_payload_bytes == 0 {
            return Err(GriddedNormalArtifactError::InvalidBudget(
                "the frame payload ceiling must be positive",
            ));
        }
        let payload_bytes = u64::try_from(maximum_frame_payload_bytes)
            .map_err(|_| GriddedNormalArtifactError::ArithmeticOverflow("frame payload bytes"))?;
        let minimum_artifact_bytes = u64::try_from(
            FILE_HEADER_BYTES
                .checked_add(FRAME_HEADER_BYTES)
                .and_then(|bytes| bytes.checked_add(maximum_frame_payload_bytes))
                .and_then(|bytes| bytes.checked_add(FOOTER_BYTES))
                .ok_or(GriddedNormalArtifactError::ArithmeticOverflow(
                    "minimum artifact bytes",
                ))?,
        )
        .map_err(|_| GriddedNormalArtifactError::ArithmeticOverflow("minimum artifact bytes"))?;
        if maximum_artifact_bytes < minimum_artifact_bytes {
            return Err(GriddedNormalArtifactError::ArtifactCapacityExceeded {
                required: minimum_artifact_bytes,
                capacity: maximum_artifact_bytes,
            });
        }
        let io_buffer_bytes = u64::try_from(
            FRAME_HEADER_BYTES
                .checked_add(maximum_frame_payload_bytes)
                .ok_or(GriddedNormalArtifactError::ArithmeticOverflow(
                    "artifact I/O buffer bytes",
                ))?,
        )
        .map_err(|_| GriddedNormalArtifactError::ArithmeticOverflow("artifact I/O buffer bytes"))?;
        debug_assert!(payload_bytes < io_buffer_bytes);
        Ok(Self {
            maximum_artifact_bytes,
            maximum_frame_payload_bytes,
            io_buffer_bytes,
        })
    }

    pub(crate) const fn maximum_artifact_bytes(self) -> u64 {
        self.maximum_artifact_bytes
    }

    pub(crate) const fn maximum_frame_payload_bytes(self) -> usize {
        self.maximum_frame_payload_bytes
    }

    pub(crate) const fn io_buffer_bytes(self) -> u64 {
        self.io_buffer_bytes
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum GriddedNormalIoDirection {
    Write,
    Read,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct GriddedNormalArtifactMeasurements {
    direction: GriddedNormalIoDirection,
    artifact_bytes: u64,
    payload_bytes: u64,
    frame_count: u64,
    record_count: u64,
    transferred_bytes: u64,
    operations: u64,
    sha256_bytes: u64,
    sha256_calls: u64,
    peak_buffer_bytes: u64,
}

impl GriddedNormalArtifactMeasurements {
    pub(crate) const fn artifact_bytes(self) -> u64 {
        self.artifact_bytes
    }

    pub(crate) const fn payload_bytes(self) -> u64 {
        self.payload_bytes
    }

    pub(crate) const fn frame_count(self) -> u64 {
        self.frame_count
    }

    pub(crate) const fn record_count(self) -> u64 {
        self.record_count
    }

    pub(crate) const fn transferred_bytes(self) -> u64 {
        self.transferred_bytes
    }

    pub(crate) const fn operations(self) -> u64 {
        self.operations
    }

    pub(crate) const fn sha256_bytes(self) -> u64 {
        self.sha256_bytes
    }

    pub(crate) const fn sha256_calls(self) -> u64 {
        self.sha256_calls
    }

    pub(crate) const fn peak_buffer_bytes(self) -> u64 {
        self.peak_buffer_bytes
    }

    pub(crate) const fn io_measurement(self) -> IoMeasurement {
        let kind = match self.direction {
            GriddedNormalIoDirection::Write => IoBufferKind::SpillWrite,
            GriddedNormalIoDirection::Read => IoBufferKind::SpillRead,
        };
        IoMeasurement::new(kind, self.transferred_bytes, self.operations)
    }

    pub(crate) fn difference_since(
        self,
        earlier: Self,
    ) -> Result<Self, GriddedNormalArtifactError> {
        if self.direction != earlier.direction {
            return Err(GriddedNormalArtifactError::MeasurementDirectionMismatch);
        }
        Ok(Self {
            direction: self.direction,
            artifact_bytes: self.artifact_bytes,
            payload_bytes: self
                .payload_bytes
                .checked_sub(earlier.payload_bytes)
                .ok_or(GriddedNormalArtifactError::ArithmeticOverflow(
                    "artifact measurement payload",
                ))?,
            frame_count: self.frame_count.checked_sub(earlier.frame_count).ok_or(
                GriddedNormalArtifactError::ArithmeticOverflow("artifact measurement frames"),
            )?,
            record_count: self.record_count.checked_sub(earlier.record_count).ok_or(
                GriddedNormalArtifactError::ArithmeticOverflow("artifact measurement records"),
            )?,
            transferred_bytes: self
                .transferred_bytes
                .checked_sub(earlier.transferred_bytes)
                .ok_or(GriddedNormalArtifactError::ArithmeticOverflow(
                    "artifact measurement transfer",
                ))?,
            operations: self.operations.checked_sub(earlier.operations).ok_or(
                GriddedNormalArtifactError::ArithmeticOverflow("artifact measurement operations"),
            )?,
            sha256_bytes: self.sha256_bytes.checked_sub(earlier.sha256_bytes).ok_or(
                GriddedNormalArtifactError::ArithmeticOverflow("artifact measurement checksum"),
            )?,
            sha256_calls: self.sha256_calls.checked_sub(earlier.sha256_calls).ok_or(
                GriddedNormalArtifactError::ArithmeticOverflow("artifact measurement hash calls"),
            )?,
            peak_buffer_bytes: self.peak_buffer_bytes,
        })
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct GriddedNormalArtifactSeal {
    frame_count: u64,
    record_count: u64,
    payload_bytes: u64,
    artifact_bytes: u64,
    global_sha256: [u8; 32],
}

impl GriddedNormalArtifactSeal {
    pub(crate) const fn frame_count(self) -> u64 {
        self.frame_count
    }

    pub(crate) const fn record_count(self) -> u64 {
        self.record_count
    }

    pub(crate) const fn payload_bytes(self) -> u64 {
        self.payload_bytes
    }

    pub(crate) const fn artifact_bytes(self) -> u64 {
        self.artifact_bytes
    }

    pub(crate) const fn global_sha256(self) -> [u8; 32] {
        self.global_sha256
    }
}

#[derive(Debug, Error)]
pub(crate) enum GriddedNormalArtifactError {
    #[error("invalid gridded-normal artifact budget: {0}")]
    InvalidBudget(&'static str),
    #[error("gridded-normal artifact arithmetic overflowed while computing {0}")]
    ArithmeticOverflow(&'static str),
    #[error("gridded-normal artifact storage binding does not match its authority domain")]
    StorageBindingMismatch,
    #[error("gridded-normal artifact storage root is not an absolute directory")]
    InvalidStorageRoot,
    #[error("gridded-normal artifact resource admission failed: {0}")]
    Resource(#[from] ResourceError),
    #[error("{operation} for the gridded-normal artifact failed: {source}")]
    Io {
        operation: &'static str,
        #[source]
        source: io::Error,
    },
    #[error(
        "gridded-normal artifact requires {required} bytes but retained capacity is {capacity}"
    )]
    ArtifactCapacityExceeded { required: u64, capacity: u64 },
    #[error("gridded-normal frame payload has {actual} bytes but its ceiling is {maximum}")]
    FramePayloadTooLarge { actual: usize, maximum: usize },
    #[error("gridded-normal writer expected frame sequence {expected}, received {actual}")]
    WriterSequenceMismatch { expected: u64, actual: u64 },
    #[error("gridded-normal artifact writer is poisoned after an earlier failure")]
    WriterPoisoned,
    #[error("gridded-normal artifact has {actual} bytes, fewer than its sealed {expected} bytes")]
    TruncatedFile { expected: u64, actual: u64 },
    #[error("gridded-normal artifact has trailing bytes: sealed {expected}, actual {actual}")]
    TrailingData { expected: u64, actual: u64 },
    #[error(
        "gridded-normal artifact ended during a positional read at {offset}: expected {expected} bytes, read {actual}"
    )]
    TruncatedRead {
        offset: u64,
        expected: usize,
        actual: usize,
    },
    #[error("gridded-normal artifact has an invalid {kind} marker or format version")]
    InvalidFormat { kind: &'static str },
    #[error("gridded-normal artifact measurements changed I/O direction")]
    MeasurementDirectionMismatch,
    #[error("gridded-normal artifact duplicated frame {actual}; next expected frame is {expected}")]
    DuplicateFrame { expected: u64, actual: u64 },
    #[error("gridded-normal artifact frame order jumped from {expected} to {actual}")]
    ReorderedFrame { expected: u64, actual: u64 },
    #[error("gridded-normal frame {sequence} payload checksum does not match its header")]
    FrameChecksumMismatch { sequence: u64 },
    #[error("gridded-normal artifact footer counters do not match the observed stream")]
    FooterCountMismatch,
    #[error("gridded-normal artifact global checksum does not match its sealed stream")]
    GlobalChecksumMismatch,
    #[error("gridded-normal artifact reader is poisoned after an earlier failure")]
    ReaderPoisoned,
    #[error("gridded-normal artifact read was not completed through its terminal footer and EOF")]
    IncompleteRead,
}

fn validate_storage_directory(
    authority: &ResourceAuthority,
    storage: &StorageIoResourceBinding,
    directory: &Path,
) -> Result<PathBuf, GriddedNormalArtifactError> {
    let domain = authority
        .topology()
        .storage_domains
        .iter()
        .find(|domain| &domain.id == storage.domain())
        .ok_or(GriddedNormalArtifactError::StorageBindingMismatch)?;
    if &domain.read_rate != storage.read_rate()
        || &domain.write_rate != storage.write_rate()
        || &domain.queue != storage.queue()
    {
        return Err(GriddedNormalArtifactError::StorageBindingMismatch);
    }
    let root = domain
        .root
        .canonicalize()
        .map_err(|source| GriddedNormalArtifactError::Io {
            operation: "resolve storage-domain root",
            source,
        })?;
    let directory = directory
        .canonicalize()
        .map_err(|source| GriddedNormalArtifactError::Io {
            operation: "resolve gridded-normal artifact directory",
            source,
        })?;
    let root_metadata = root
        .metadata()
        .map_err(|source| GriddedNormalArtifactError::Io {
            operation: "inspect storage-domain root",
            source,
        })?;
    let directory_metadata =
        directory
            .metadata()
            .map_err(|source| GriddedNormalArtifactError::Io {
                operation: "inspect gridded-normal artifact directory",
                source,
            })?;
    if !root.is_absolute()
        || !root_metadata.is_dir()
        || !directory.is_absolute()
        || !directory_metadata.is_dir()
        || !directory.starts_with(&root)
        || directory_metadata.dev() != root_metadata.dev()
    {
        return Err(GriddedNormalArtifactError::InvalidStorageRoot);
    }
    Ok(directory)
}

#[derive(Debug)]
pub(crate) struct GriddedNormalArtifactWriter {
    file: File,
    _lease: ResourceLease,
    budget: GriddedNormalArtifactBudget,
    buffer: Vec<u8>,
    global_hasher: Sha256,
    bytes_written: u64,
    write_operations: u64,
    frame_count: u64,
    record_count: u64,
    payload_bytes: u64,
    poisoned: bool,
}

impl GriddedNormalArtifactWriter {
    pub(crate) fn create(
        authority: &ResourceAuthority,
        policy: ResourcePolicy,
        storage: &GriddedNormalReplayStorage,
        budget: GriddedNormalArtifactBudget,
    ) -> Result<Self, GriddedNormalArtifactError> {
        let directory =
            validate_storage_directory(authority, &storage.resources, &storage.directory)?;
        let host_view = authority
            .topology()
            .memory_views
            .iter()
            .find(|view| view.kind == MemoryViewKind::Host)
            .map(|view| view.id.clone())
            .ok_or_else(|| {
                GriddedNormalArtifactError::Resource(ResourceError::Invalid(
                    "gridded-normal replay requires one host-memory view".to_string(),
                ))
            })?;
        let lease = authority.acquire(
            policy,
            DemandAlternatives {
                required_capabilities: BTreeSet::new(),
                alternatives: vec![DemandAlternative {
                    id: AlternativeId::new(STORAGE_DEMAND_ID),
                    capabilities: CapabilityPredicate::default(),
                    demand: DemandEnvelope {
                        host_memory_view: host_view.clone(),
                        memory: vec![MemoryDemand {
                            allocation_id: BUFFER_ALLOCATION_ID.to_string(),
                            hard_bytes: budget.io_buffer_bytes,
                            preferred_bytes: budget.io_buffer_bytes,
                            views: vec![host_view],
                        }],
                        workers: CountDemand::zero(),
                        overhead: RuntimeOverheadDemand::zero(),
                        storage: vec![StorageDemand {
                            demand_id: STORAGE_DEMAND_ID.to_string(),
                            domain: storage.resources.domain().clone(),
                            temporary_bytes: budget.maximum_artifact_bytes,
                            staged_output_bytes: 0,
                            final_output_bytes: 0,
                            persistent_cache_bytes: 0,
                            read_rate: CountDemand::zero(),
                            write_rate: CountDemand::zero(),
                            operations_rate: CountDemand::zero(),
                            queue_slots: CountDemand::zero(),
                        }],
                        rates: Vec::new(),
                        caches: CacheDemand::zero(),
                        locks: CountDemand::zero(),
                        file_descriptors: CountDemand::new(1, 1),
                        queues: Vec::new(),
                        transfers: Vec::new(),
                        accelerators: Vec::new(),
                        io_buffers: IoBufferDemand {
                            spill_read_bytes: budget.io_buffer_bytes,
                            spill_write_bytes: budget.io_buffer_bytes,
                            ..IoBufferDemand::zero()
                        },
                    },
                    headroom: ResourceHeadroom::default(),
                    scaling: ScalingMetadata {
                        minimum_workers: 0,
                        maximum_workers: 0,
                        maximum_batch_size: 1,
                        maximum_tile_width: 1,
                        maximum_tile_height: 1,
                        maximum_slab_depth: 1,
                        memory_bytes_per_worker: BTreeMap::new(),
                    },
                    quiescence_points: BTreeSet::from([QuiescencePoint::MajorCycle]),
                }],
            },
        )?;
        let file = Builder::new()
            .prefix(".casars-gridded-normal-replay-")
            .tempfile_in(directory)
            .map_err(|source| GriddedNormalArtifactError::Io {
                operation: "create private temporary file",
                source,
            })?
            .into_file();
        let buffer_len = usize::try_from(budget.io_buffer_bytes).map_err(|_| {
            GriddedNormalArtifactError::ArithmeticOverflow("artifact I/O buffer allocation")
        })?;
        let mut writer = Self {
            file,
            _lease: lease,
            budget,
            buffer: vec![0; buffer_len],
            global_hasher: Sha256::new(),
            bytes_written: 0,
            write_operations: 0,
            frame_count: 0,
            record_count: 0,
            payload_bytes: 0,
            poisoned: false,
        };
        let header = encode_file_header();
        if let Err(error) = writer.write_bytes(&header, "write artifact header") {
            writer.poisoned = true;
            return Err(error);
        }
        writer.global_hasher.update(header);
        Ok(writer)
    }

    pub(crate) fn append_frame(
        &mut self,
        sequence: u64,
        record_count: u64,
        payload: &[u8],
    ) -> Result<(), GriddedNormalArtifactError> {
        if self.poisoned {
            return Err(GriddedNormalArtifactError::WriterPoisoned);
        }
        let result = self.append_frame_inner(sequence, record_count, payload);
        if result.is_err() {
            self.poisoned = true;
        }
        result
    }

    pub(crate) fn measurements(&self) -> GriddedNormalArtifactMeasurements {
        GriddedNormalArtifactMeasurements {
            direction: GriddedNormalIoDirection::Write,
            artifact_bytes: self.bytes_written,
            payload_bytes: self.payload_bytes,
            frame_count: self.frame_count,
            record_count: self.record_count,
            transferred_bytes: self.bytes_written,
            operations: self.write_operations,
            sha256_bytes: self.bytes_written.saturating_add(self.payload_bytes),
            sha256_calls: self.frame_count,
            peak_buffer_bytes: self.budget.io_buffer_bytes,
        }
    }

    fn append_frame_inner(
        &mut self,
        sequence: u64,
        record_count: u64,
        payload: &[u8],
    ) -> Result<(), GriddedNormalArtifactError> {
        if sequence != self.frame_count {
            return Err(GriddedNormalArtifactError::WriterSequenceMismatch {
                expected: self.frame_count,
                actual: sequence,
            });
        }
        if payload.len() > self.budget.maximum_frame_payload_bytes {
            return Err(GriddedNormalArtifactError::FramePayloadTooLarge {
                actual: payload.len(),
                maximum: self.budget.maximum_frame_payload_bytes,
            });
        }
        let payload_bytes = u64::try_from(payload.len())
            .map_err(|_| GriddedNormalArtifactError::ArithmeticOverflow("frame payload bytes"))?;
        let prospective_payload_bytes = self.payload_bytes.checked_add(payload_bytes).ok_or(
            GriddedNormalArtifactError::ArithmeticOverflow("artifact payload bytes"),
        )?;
        let prospective_record_count = self.record_count.checked_add(record_count).ok_or(
            GriddedNormalArtifactError::ArithmeticOverflow("artifact record count"),
        )?;
        let prospective_frame_count = self.frame_count.checked_add(1).ok_or(
            GriddedNormalArtifactError::ArithmeticOverflow("artifact frame count"),
        )?;
        let frame_bytes = u64::try_from(FRAME_HEADER_BYTES)
            .map_err(|_| GriddedNormalArtifactError::ArithmeticOverflow("frame header bytes"))?
            .checked_add(payload_bytes)
            .ok_or(GriddedNormalArtifactError::ArithmeticOverflow(
                "encoded frame bytes",
            ))?;
        let required = self
            .bytes_written
            .checked_add(frame_bytes)
            .and_then(|bytes| bytes.checked_add(FOOTER_BYTES as u64))
            .ok_or(GriddedNormalArtifactError::ArithmeticOverflow(
                "sealed artifact bytes",
            ))?;
        if required > self.budget.maximum_artifact_bytes {
            return Err(GriddedNormalArtifactError::ArtifactCapacityExceeded {
                required,
                capacity: self.budget.maximum_artifact_bytes,
            });
        }
        let payload_sha256: [u8; 32] = Sha256::digest(payload).into();
        let header = encode_frame_header(sequence, record_count, payload_bytes, payload_sha256);
        let encoded_bytes = FRAME_HEADER_BYTES.checked_add(payload.len()).ok_or(
            GriddedNormalArtifactError::ArithmeticOverflow("encoded frame buffer bytes"),
        )?;
        self.buffer[..FRAME_HEADER_BYTES].copy_from_slice(&header);
        self.buffer[FRAME_HEADER_BYTES..encoded_bytes].copy_from_slice(payload);
        write_bytes(
            &mut self.file,
            &self.buffer[..encoded_bytes],
            &mut self.bytes_written,
            &mut self.write_operations,
            "write artifact frame",
        )?;
        self.global_hasher.update(&self.buffer[..encoded_bytes]);
        self.payload_bytes = prospective_payload_bytes;
        self.record_count = prospective_record_count;
        self.frame_count = prospective_frame_count;
        Ok(())
    }

    pub(crate) fn seal(
        mut self,
    ) -> Result<GriddedNormalReplayArtifact, GriddedNormalArtifactError> {
        if self.poisoned {
            return Err(GriddedNormalArtifactError::WriterPoisoned);
        }
        let footer_bytes = u64::try_from(FOOTER_BYTES)
            .map_err(|_| GriddedNormalArtifactError::ArithmeticOverflow("artifact footer bytes"))?;
        let artifact_bytes = self.bytes_written.checked_add(footer_bytes).ok_or(
            GriddedNormalArtifactError::ArithmeticOverflow("sealed artifact bytes"),
        )?;
        if artifact_bytes > self.budget.maximum_artifact_bytes {
            return Err(GriddedNormalArtifactError::ArtifactCapacityExceeded {
                required: artifact_bytes,
                capacity: self.budget.maximum_artifact_bytes,
            });
        }
        let global_sha256: [u8; 32] = self.global_hasher.clone().finalize().into();
        let footer = encode_footer(
            self.frame_count,
            self.record_count,
            self.payload_bytes,
            artifact_bytes,
            global_sha256,
        );
        self.write_bytes(&footer, "write artifact footer")?;
        self.file
            .flush()
            .map_err(|source| GriddedNormalArtifactError::Io {
                operation: "flush sealed artifact",
                source,
            })?;
        let actual_bytes = self
            .file
            .metadata()
            .map_err(|source| GriddedNormalArtifactError::Io {
                operation: "inspect sealed artifact",
                source,
            })?
            .len();
        if actual_bytes < artifact_bytes {
            return Err(GriddedNormalArtifactError::TruncatedFile {
                expected: artifact_bytes,
                actual: actual_bytes,
            });
        }
        if actual_bytes > artifact_bytes {
            return Err(GriddedNormalArtifactError::TrailingData {
                expected: artifact_bytes,
                actual: actual_bytes,
            });
        }
        let sha256_bytes = self
            .bytes_written
            .checked_sub(footer_bytes)
            .and_then(|bytes| bytes.checked_add(self.payload_bytes))
            .ok_or(GriddedNormalArtifactError::ArithmeticOverflow(
                "artifact checksum bytes",
            ))?;
        let sha256_calls = self.frame_count.checked_add(1).ok_or(
            GriddedNormalArtifactError::ArithmeticOverflow("artifact checksum calls"),
        )?;
        let seal = GriddedNormalArtifactSeal {
            frame_count: self.frame_count,
            record_count: self.record_count,
            payload_bytes: self.payload_bytes,
            artifact_bytes,
            global_sha256,
        };
        let write_measurements = GriddedNormalArtifactMeasurements {
            direction: GriddedNormalIoDirection::Write,
            artifact_bytes,
            payload_bytes: self.payload_bytes,
            frame_count: self.frame_count,
            record_count: self.record_count,
            transferred_bytes: self.bytes_written,
            operations: self.write_operations,
            sha256_bytes,
            sha256_calls,
            peak_buffer_bytes: self.budget.io_buffer_bytes,
        };
        Ok(GriddedNormalReplayArtifact {
            file: self.file,
            _lease: self._lease,
            budget: self.budget,
            buffer: self.buffer,
            seal,
            write_measurements,
        })
    }

    fn write_bytes(
        &mut self,
        bytes: &[u8],
        operation: &'static str,
    ) -> Result<(), GriddedNormalArtifactError> {
        write_bytes(
            &mut self.file,
            bytes,
            &mut self.bytes_written,
            &mut self.write_operations,
            operation,
        )
    }
}

#[derive(Debug)]
pub(crate) struct GriddedNormalReplayArtifact {
    file: File,
    _lease: ResourceLease,
    budget: GriddedNormalArtifactBudget,
    buffer: Vec<u8>,
    seal: GriddedNormalArtifactSeal,
    write_measurements: GriddedNormalArtifactMeasurements,
}

impl GriddedNormalReplayArtifact {
    pub(crate) const fn seal(&self) -> GriddedNormalArtifactSeal {
        self.seal
    }

    pub(crate) const fn write_measurements(&self) -> GriddedNormalArtifactMeasurements {
        self.write_measurements
    }

    pub(crate) fn reader(
        &mut self,
    ) -> Result<GriddedNormalArtifactReader<'_>, GriddedNormalArtifactError> {
        let actual_bytes = self
            .file
            .metadata()
            .map_err(|source| GriddedNormalArtifactError::Io {
                operation: "inspect artifact before replay",
                source,
            })?
            .len();
        if actual_bytes < self.seal.artifact_bytes {
            return Err(GriddedNormalArtifactError::TruncatedFile {
                expected: self.seal.artifact_bytes,
                actual: actual_bytes,
            });
        }
        if actual_bytes > self.seal.artifact_bytes {
            return Err(GriddedNormalArtifactError::TrailingData {
                expected: self.seal.artifact_bytes,
                actual: actual_bytes,
            });
        }
        let mut measurements = MutableReadMeasurements {
            transferred_bytes: 0,
            operations: 0,
        };
        let mut header = [0_u8; FILE_HEADER_BYTES];
        read_exact_at(
            &self.file,
            &mut header,
            0,
            &mut measurements,
            "read artifact header",
        )?;
        if !valid_file_header(&header) {
            return Err(GriddedNormalArtifactError::InvalidFormat {
                kind: "file header",
            });
        }
        let mut global_hasher = Sha256::new();
        global_hasher.update(header);
        Ok(GriddedNormalArtifactReader {
            file: &self.file,
            buffer: &mut self.buffer,
            budget: self.budget,
            seal: self.seal,
            offset: FILE_HEADER_BYTES as u64,
            global_hasher,
            frame_count: 0,
            record_count: 0,
            payload_bytes: 0,
            measurements,
            finished: false,
            poisoned: false,
        })
    }
}

#[derive(Clone, Copy, Debug)]
struct FrameMetadata {
    sequence: u64,
    record_count: u64,
    payload_len: usize,
}

#[derive(Debug)]
pub(crate) struct GriddedNormalReplayFrame<'a> {
    sequence: u64,
    record_count: u64,
    payload: &'a [u8],
}

impl GriddedNormalReplayFrame<'_> {
    pub(crate) const fn sequence(&self) -> u64 {
        self.sequence
    }

    pub(crate) const fn record_count(&self) -> u64 {
        self.record_count
    }

    pub(crate) const fn payload(&self) -> &[u8] {
        self.payload
    }
}

#[derive(Clone, Copy, Debug, Default)]
struct MutableReadMeasurements {
    transferred_bytes: u64,
    operations: u64,
}

#[derive(Debug)]
pub(crate) struct GriddedNormalArtifactReader<'a> {
    file: &'a File,
    buffer: &'a mut Vec<u8>,
    budget: GriddedNormalArtifactBudget,
    seal: GriddedNormalArtifactSeal,
    offset: u64,
    global_hasher: Sha256,
    frame_count: u64,
    record_count: u64,
    payload_bytes: u64,
    measurements: MutableReadMeasurements,
    finished: bool,
    poisoned: bool,
}

impl<'a> GriddedNormalArtifactReader<'a> {
    pub(crate) fn next_frame(
        &mut self,
    ) -> Result<Option<GriddedNormalReplayFrame<'_>>, GriddedNormalArtifactError> {
        if self.poisoned {
            return Err(GriddedNormalArtifactError::ReaderPoisoned);
        }
        if self.finished {
            return Ok(None);
        }
        match self.read_next() {
            Ok(Some(metadata)) => Ok(Some(GriddedNormalReplayFrame {
                sequence: metadata.sequence,
                record_count: metadata.record_count,
                payload: &self.buffer[..metadata.payload_len],
            })),
            Ok(None) => Ok(None),
            Err(error) => {
                self.poisoned = true;
                Err(error)
            }
        }
    }

    fn read_next(&mut self) -> Result<Option<FrameMetadata>, GriddedNormalArtifactError> {
        let mut marker = [0_u8; 8];
        read_exact_at(
            self.file,
            &mut marker,
            self.offset,
            &mut self.measurements,
            "read artifact entry marker",
        )?;
        if marker == FOOTER_MAGIC {
            self.read_footer(marker)?;
            self.finished = true;
            return Ok(None);
        }
        if marker != FRAME_MAGIC {
            return Err(GriddedNormalArtifactError::InvalidFormat { kind: "frame" });
        }
        let mut header = [0_u8; FRAME_HEADER_BYTES];
        header[..marker.len()].copy_from_slice(&marker);
        read_exact_at(
            self.file,
            &mut header[marker.len()..],
            self.offset + marker.len() as u64,
            &mut self.measurements,
            "read artifact frame header",
        )?;
        if !valid_version_and_length(&header, FRAME_HEADER_BYTES) {
            return Err(GriddedNormalArtifactError::InvalidFormat {
                kind: "frame header",
            });
        }
        let sequence = decode_u64(&header, 16);
        if sequence < self.frame_count {
            return Err(GriddedNormalArtifactError::DuplicateFrame {
                expected: self.frame_count,
                actual: sequence,
            });
        }
        if sequence > self.frame_count {
            return Err(GriddedNormalArtifactError::ReorderedFrame {
                expected: self.frame_count,
                actual: sequence,
            });
        }
        let record_count = decode_u64(&header, 24);
        let payload_bytes = decode_u64(&header, 32);
        let payload_len = usize::try_from(payload_bytes).map_err(|_| {
            GriddedNormalArtifactError::FramePayloadTooLarge {
                actual: usize::MAX,
                maximum: self.budget.maximum_frame_payload_bytes,
            }
        })?;
        if payload_len > self.budget.maximum_frame_payload_bytes {
            return Err(GriddedNormalArtifactError::FramePayloadTooLarge {
                actual: payload_len,
                maximum: self.budget.maximum_frame_payload_bytes,
            });
        }
        let payload_offset = self.offset.checked_add(FRAME_HEADER_BYTES as u64).ok_or(
            GriddedNormalArtifactError::ArithmeticOverflow("frame payload offset"),
        )?;
        let next_offset = payload_offset.checked_add(payload_bytes).ok_or(
            GriddedNormalArtifactError::ArithmeticOverflow("next frame offset"),
        )?;
        let footer_offset = self
            .seal
            .artifact_bytes
            .checked_sub(FOOTER_BYTES as u64)
            .ok_or(GriddedNormalArtifactError::InvalidFormat {
                kind: "sealed artifact length",
            })?;
        if next_offset > footer_offset {
            return Err(GriddedNormalArtifactError::TruncatedRead {
                offset: payload_offset,
                expected: payload_len,
                actual: usize::try_from(footer_offset.saturating_sub(payload_offset))
                    .unwrap_or(usize::MAX),
            });
        }
        read_exact_at(
            self.file,
            &mut self.buffer[..payload_len],
            payload_offset,
            &mut self.measurements,
            "read artifact frame payload",
        )?;
        let expected_sha256: [u8; 32] =
            header[40..72].try_into().expect("fixed frame digest slice");
        let actual_sha256: [u8; 32] = Sha256::digest(&self.buffer[..payload_len]).into();
        if actual_sha256 != expected_sha256 {
            return Err(GriddedNormalArtifactError::FrameChecksumMismatch { sequence });
        }
        let next_frame_count = self.frame_count.checked_add(1).ok_or(
            GriddedNormalArtifactError::ArithmeticOverflow("replayed frame count"),
        )?;
        let next_record_count = self.record_count.checked_add(record_count).ok_or(
            GriddedNormalArtifactError::ArithmeticOverflow("replayed record count"),
        )?;
        let next_payload_bytes = self.payload_bytes.checked_add(payload_bytes).ok_or(
            GriddedNormalArtifactError::ArithmeticOverflow("replayed payload bytes"),
        )?;
        self.global_hasher.update(header);
        self.global_hasher.update(&self.buffer[..payload_len]);
        self.offset = next_offset;
        self.frame_count = next_frame_count;
        self.record_count = next_record_count;
        self.payload_bytes = next_payload_bytes;
        Ok(Some(FrameMetadata {
            sequence,
            record_count,
            payload_len,
        }))
    }

    fn read_footer(&mut self, marker: [u8; 8]) -> Result<(), GriddedNormalArtifactError> {
        let mut footer = [0_u8; FOOTER_BYTES];
        footer[..marker.len()].copy_from_slice(&marker);
        read_exact_at(
            self.file,
            &mut footer[marker.len()..],
            self.offset + marker.len() as u64,
            &mut self.measurements,
            "read artifact footer",
        )?;
        if !valid_version_and_length(&footer, FOOTER_BYTES) {
            return Err(GriddedNormalArtifactError::InvalidFormat { kind: "footer" });
        }
        let footer_frame_count = decode_u64(&footer, 16);
        let footer_record_count = decode_u64(&footer, 24);
        let footer_payload_bytes = decode_u64(&footer, 32);
        let footer_artifact_bytes = decode_u64(&footer, 40);
        if footer_frame_count != self.frame_count
            || footer_record_count != self.record_count
            || footer_payload_bytes != self.payload_bytes
            || footer_artifact_bytes != self.seal.artifact_bytes
            || footer_frame_count != self.seal.frame_count
            || footer_record_count != self.seal.record_count
            || footer_payload_bytes != self.seal.payload_bytes
        {
            return Err(GriddedNormalArtifactError::FooterCountMismatch);
        }
        let footer_sha256: [u8; 32] = footer[48..80]
            .try_into()
            .expect("fixed footer digest slice");
        let actual_sha256: [u8; 32] = self.global_hasher.clone().finalize().into();
        if footer_sha256 != actual_sha256 || footer_sha256 != self.seal.global_sha256 {
            return Err(GriddedNormalArtifactError::GlobalChecksumMismatch);
        }
        let expected_end = self.offset.checked_add(FOOTER_BYTES as u64).ok_or(
            GriddedNormalArtifactError::ArithmeticOverflow("artifact terminal offset"),
        )?;
        if expected_end != self.seal.artifact_bytes {
            return Err(GriddedNormalArtifactError::FooterCountMismatch);
        }
        let mut trailing = [0_u8; 1];
        self.measurements.operations = self.measurements.operations.checked_add(1).ok_or(
            GriddedNormalArtifactError::ArithmeticOverflow("artifact read operations"),
        )?;
        let trailing_bytes = self
            .file
            .read_at(&mut trailing, expected_end)
            .map_err(|source| GriddedNormalArtifactError::Io {
                operation: "verify artifact EOF",
                source,
            })?;
        self.measurements.transferred_bytes = self
            .measurements
            .transferred_bytes
            .checked_add(u64::try_from(trailing_bytes).map_err(|_| {
                GriddedNormalArtifactError::ArithmeticOverflow("artifact trailing bytes")
            })?)
            .ok_or(GriddedNormalArtifactError::ArithmeticOverflow(
                "artifact bytes read",
            ))?;
        if trailing_bytes != 0 {
            return Err(GriddedNormalArtifactError::TrailingData {
                expected: expected_end,
                actual: expected_end + trailing_bytes as u64,
            });
        }
        self.offset = expected_end;
        Ok(())
    }

    pub(crate) fn complete(
        self,
    ) -> Result<GriddedNormalArtifactReadCompletion, GriddedNormalArtifactError> {
        if self.poisoned {
            return Err(GriddedNormalArtifactError::ReaderPoisoned);
        }
        if !self.finished {
            return Err(GriddedNormalArtifactError::IncompleteRead);
        }
        let sha256_bytes = self
            .seal
            .artifact_bytes
            .checked_sub(FOOTER_BYTES as u64)
            .and_then(|bytes| bytes.checked_add(self.payload_bytes))
            .ok_or(GriddedNormalArtifactError::ArithmeticOverflow(
                "artifact checksum bytes",
            ))?;
        let sha256_calls = self.frame_count.checked_add(1).ok_or(
            GriddedNormalArtifactError::ArithmeticOverflow("artifact checksum calls"),
        )?;
        Ok(GriddedNormalArtifactReadCompletion {
            seal: self.seal,
            measurements: GriddedNormalArtifactMeasurements {
                direction: GriddedNormalIoDirection::Read,
                artifact_bytes: self.seal.artifact_bytes,
                payload_bytes: self.payload_bytes,
                frame_count: self.frame_count,
                record_count: self.record_count,
                transferred_bytes: self.measurements.transferred_bytes,
                operations: self.measurements.operations,
                sha256_bytes,
                sha256_calls,
                peak_buffer_bytes: self.budget.io_buffer_bytes,
            },
        })
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct GriddedNormalArtifactReadCompletion {
    seal: GriddedNormalArtifactSeal,
    measurements: GriddedNormalArtifactMeasurements,
}

impl GriddedNormalArtifactReadCompletion {
    pub(crate) const fn seal(self) -> GriddedNormalArtifactSeal {
        self.seal
    }

    pub(crate) const fn measurements(self) -> GriddedNormalArtifactMeasurements {
        self.measurements
    }
}

fn encode_file_header() -> [u8; FILE_HEADER_BYTES] {
    let mut header = [0_u8; FILE_HEADER_BYTES];
    header[..8].copy_from_slice(&FILE_HEADER_MAGIC);
    encode_u32(&mut header, 8, FORMAT_VERSION);
    encode_u32(&mut header, 12, FILE_HEADER_BYTES as u32);
    header
}

fn encode_frame_header(
    sequence: u64,
    record_count: u64,
    payload_bytes: u64,
    payload_sha256: [u8; 32],
) -> [u8; FRAME_HEADER_BYTES] {
    let mut header = [0_u8; FRAME_HEADER_BYTES];
    header[..8].copy_from_slice(&FRAME_MAGIC);
    encode_u32(&mut header, 8, FORMAT_VERSION);
    encode_u32(&mut header, 12, FRAME_HEADER_BYTES as u32);
    encode_u64(&mut header, 16, sequence);
    encode_u64(&mut header, 24, record_count);
    encode_u64(&mut header, 32, payload_bytes);
    header[40..72].copy_from_slice(&payload_sha256);
    header
}

fn encode_footer(
    frame_count: u64,
    record_count: u64,
    payload_bytes: u64,
    artifact_bytes: u64,
    global_sha256: [u8; 32],
) -> [u8; FOOTER_BYTES] {
    let mut footer = [0_u8; FOOTER_BYTES];
    footer[..8].copy_from_slice(&FOOTER_MAGIC);
    encode_u32(&mut footer, 8, FORMAT_VERSION);
    encode_u32(&mut footer, 12, FOOTER_BYTES as u32);
    encode_u64(&mut footer, 16, frame_count);
    encode_u64(&mut footer, 24, record_count);
    encode_u64(&mut footer, 32, payload_bytes);
    encode_u64(&mut footer, 40, artifact_bytes);
    footer[48..80].copy_from_slice(&global_sha256);
    footer
}

fn valid_file_header(header: &[u8; FILE_HEADER_BYTES]) -> bool {
    header[..8] == FILE_HEADER_MAGIC
        && decode_u32(header, 8) == FORMAT_VERSION
        && decode_u32(header, 12) == FILE_HEADER_BYTES as u32
}

fn valid_version_and_length(bytes: &[u8], expected_len: usize) -> bool {
    decode_u32(bytes, 8) == FORMAT_VERSION
        && usize::try_from(decode_u32(bytes, 12)).ok() == Some(expected_len)
}

fn encode_u32(bytes: &mut [u8], offset: usize, value: u32) {
    bytes[offset..offset + 4].copy_from_slice(&value.to_le_bytes());
}

fn encode_u64(bytes: &mut [u8], offset: usize, value: u64) {
    bytes[offset..offset + 8].copy_from_slice(&value.to_le_bytes());
}

fn decode_u32(bytes: &[u8], offset: usize) -> u32 {
    u32::from_le_bytes(
        bytes[offset..offset + 4]
            .try_into()
            .expect("fixed u32 field slice"),
    )
}

fn decode_u64(bytes: &[u8], offset: usize) -> u64 {
    u64::from_le_bytes(
        bytes[offset..offset + 8]
            .try_into()
            .expect("fixed u64 field slice"),
    )
}

fn read_exact_at(
    file: &File,
    buffer: &mut [u8],
    offset: u64,
    measurements: &mut MutableReadMeasurements,
    operation: &'static str,
) -> Result<(), GriddedNormalArtifactError> {
    if buffer.is_empty() {
        return Ok(());
    }
    let mut read = 0_usize;
    while read < buffer.len() {
        let read_offset = offset
            .checked_add(u64::try_from(read).map_err(|_| {
                GriddedNormalArtifactError::ArithmeticOverflow("positional read offset")
            })?)
            .ok_or(GriddedNormalArtifactError::ArithmeticOverflow(
                "positional read offset",
            ))?;
        measurements.operations = measurements.operations.checked_add(1).ok_or(
            GriddedNormalArtifactError::ArithmeticOverflow("artifact read operations"),
        )?;
        match file.read_at(&mut buffer[read..], read_offset) {
            Ok(0) => {
                return Err(GriddedNormalArtifactError::TruncatedRead {
                    offset,
                    expected: buffer.len(),
                    actual: read,
                });
            }
            Ok(bytes) => {
                read = read.checked_add(bytes).ok_or(
                    GriddedNormalArtifactError::ArithmeticOverflow("artifact read bytes"),
                )?;
                measurements.transferred_bytes = measurements
                    .transferred_bytes
                    .checked_add(u64::try_from(bytes).map_err(|_| {
                        GriddedNormalArtifactError::ArithmeticOverflow("artifact read bytes")
                    })?)
                    .ok_or(GriddedNormalArtifactError::ArithmeticOverflow(
                        "artifact read bytes",
                    ))?;
            }
            Err(source) if source.kind() == io::ErrorKind::Interrupted => continue,
            Err(source) => return Err(GriddedNormalArtifactError::Io { operation, source }),
        }
    }
    Ok(())
}

fn write_bytes(
    file: &mut File,
    bytes: &[u8],
    bytes_written: &mut u64,
    operations: &mut u64,
    operation: &'static str,
) -> Result<(), GriddedNormalArtifactError> {
    let mut written = 0_usize;
    while written < bytes.len() {
        *operations =
            operations
                .checked_add(1)
                .ok_or(GriddedNormalArtifactError::ArithmeticOverflow(
                    "artifact write operations",
                ))?;
        match file.write(&bytes[written..]) {
            Ok(0) => {
                return Err(GriddedNormalArtifactError::Io {
                    operation,
                    source: io::Error::from(io::ErrorKind::WriteZero),
                });
            }
            Ok(count) => {
                written = written.checked_add(count).ok_or(
                    GriddedNormalArtifactError::ArithmeticOverflow("artifact bytes written"),
                )?;
                *bytes_written = bytes_written
                    .checked_add(u64::try_from(count).map_err(|_| {
                        GriddedNormalArtifactError::ArithmeticOverflow("artifact bytes written")
                    })?)
                    .ok_or(GriddedNormalArtifactError::ArithmeticOverflow(
                        "artifact bytes written",
                    ))?;
            }
            Err(source) if source.kind() == io::ErrorKind::Interrupted => continue,
            Err(source) => return Err(GriddedNormalArtifactError::Io { operation, source }),
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::path::Path;

    use super::*;
    use crate::resource_authority::{
        CapacityDomainId, CpuClassCapacity, ExternalPressure, HostInventory, LeaseResource,
        MemoryCapacityDomain, MemoryCapacityKind, MemoryView, QueueResource, QueueResourceId,
        RateResource, RateResourceId, RateUnit, ResourceTopology, StorageDomain, StorageDomainId,
        StorageUseKind,
    };

    const TEST_CAPACITY_BYTES: u64 = 4_096;
    const TEST_FRAME_PAYLOAD_BYTES: usize = 64;

    fn test_authority(
        root: &Path,
        available_storage_bytes: u64,
    ) -> (ResourceAuthority, GriddedNormalReplayStorage) {
        let memory_domain = CapacityDomainId::new("test-memory");
        let memory_view = crate::CapacityViewId::new("host-memory");
        let storage_domain = StorageDomainId::new("test-storage");
        let read_rate = RateResourceId::new("test-storage-read");
        let write_rate = RateResourceId::new("test-storage-write");
        let queue = QueueResourceId::new("test-storage-queue");
        let topology = ResourceTopology {
            memory_domains: vec![MemoryCapacityDomain {
                id: memory_domain.clone(),
                kind: MemoryCapacityKind::Host,
                capacity_bytes: 1 << 20,
            }],
            memory_views: vec![MemoryView {
                id: memory_view,
                domain: memory_domain.clone(),
                kind: MemoryViewKind::Host,
            }],
            accelerators: Vec::new(),
            transfer_links: Vec::new(),
            storage_domains: vec![StorageDomain {
                id: storage_domain.clone(),
                root: root.to_path_buf(),
                capacity_bytes: available_storage_bytes,
                read_rate: read_rate.clone(),
                write_rate: write_rate.clone(),
                operations_rate: None,
                queue: queue.clone(),
            }],
            rate_resources: vec![
                RateResource::new(read_rate.clone(), RateUnit::BytesPerSecond, 1 << 20),
                RateResource::new(write_rate.clone(), RateUnit::BytesPerSecond, 1 << 20),
            ],
            queue_resources: vec![QueueResource::new(queue.clone(), 1)],
            logical_cpu_threads: 1,
            performance_cpu_cores: CpuClassCapacity::Known(1),
            cache_capacity_bytes: 1 << 20,
            lock_capacity: 0,
            file_descriptor_capacity: 4,
        };
        let pressure = ExternalPressure {
            memory_available_bytes: BTreeMap::from([(memory_domain, 1 << 20)]),
            available_cpu_threads: 1,
            storage_available_bytes: BTreeMap::from([(
                storage_domain.clone(),
                available_storage_bytes,
            )]),
            rate_available_per_second: BTreeMap::from([
                (read_rate.clone(), 1 << 20),
                (write_rate.clone(), 1 << 20),
            ]),
            queue_available_slots: BTreeMap::from([(queue.clone(), 1)]),
            accelerator_available_slots: BTreeMap::new(),
            cache_available_bytes: 1 << 20,
            available_locks: 0,
            available_file_descriptors: 4,
        };
        let authority = ResourceAuthority::with_inventory(HostInventory { topology, pressure })
            .expect("test resource authority");
        let resources = StorageIoResourceBinding::new(storage_domain, read_rate, write_rate, queue);
        let storage = GriddedNormalReplayStorage::bind(&authority, resources, root)
            .expect("test replay storage");
        (authority, storage)
    }

    fn budget(capacity: u64) -> GriddedNormalArtifactBudget {
        GriddedNormalArtifactBudget::new(capacity, TEST_FRAME_PAYLOAD_BYTES)
            .expect("valid artifact budget")
    }

    fn sealed_two_frame_artifact() -> (
        tempfile::TempDir,
        ResourceAuthority,
        GriddedNormalReplayArtifact,
    ) {
        let root = tempfile::tempdir().expect("artifact root");
        let (authority, storage) = test_authority(root.path(), TEST_CAPACITY_BYTES);
        let mut writer = GriddedNormalArtifactWriter::create(
            &authority,
            ResourcePolicy::Exclusive,
            &storage,
            budget(TEST_CAPACITY_BYTES),
        )
        .expect("artifact writer");
        writer
            .append_frame(0, 2, b"first-frame")
            .expect("first frame");
        writer.append_frame(1, 1, b"second").expect("second frame");
        let artifact = writer.seal().expect("sealed artifact");
        (root, authority, artifact)
    }

    fn assert_root_is_unlinked(root: &Path) {
        assert_eq!(
            std::fs::read_dir(root).expect("read artifact root").count(),
            0
        );
    }

    fn write_all_at(file: &File, mut bytes: &[u8], mut offset: u64) {
        while !bytes.is_empty() {
            let written = file.write_at(bytes, offset).expect("mutate artifact");
            assert_ne!(written, 0);
            bytes = &bytes[written..];
            offset += written as u64;
        }
    }

    #[test]
    fn opaque_frames_round_trip_with_exact_integrity_and_io_measurements() {
        let (root, _authority, mut artifact) = sealed_two_frame_artifact();
        assert_root_is_unlinked(root.path());
        let seal = artifact.seal();
        assert_eq!(seal.frame_count(), 2);
        assert_eq!(seal.record_count(), 3);
        assert_eq!(seal.payload_bytes(), 17);
        assert_ne!(seal.global_sha256(), [0; 32]);
        let expected_artifact_bytes =
            (FILE_HEADER_BYTES + 2 * FRAME_HEADER_BYTES + 17 + FOOTER_BYTES) as u64;
        assert_eq!(seal.artifact_bytes(), expected_artifact_bytes);

        let write = artifact.write_measurements();
        assert_eq!(write.artifact_bytes(), expected_artifact_bytes);
        assert_eq!(write.payload_bytes(), 17);
        assert_eq!(write.frame_count(), 2);
        assert_eq!(write.record_count(), 3);
        assert_eq!(write.transferred_bytes(), expected_artifact_bytes);
        assert_eq!(write.operations(), 4);
        assert_eq!(write.sha256_calls(), 3);
        assert_eq!(
            write.sha256_bytes(),
            expected_artifact_bytes - FOOTER_BYTES as u64 + 17
        );
        assert_eq!(
            write.peak_buffer_bytes(),
            (FRAME_HEADER_BYTES + TEST_FRAME_PAYLOAD_BYTES) as u64
        );
        assert_eq!(
            write.io_measurement().actual(),
            Some((expected_artifact_bytes, 4))
        );

        let buffer_address = artifact.buffer.as_ptr();
        let mut reader = artifact.reader().expect("artifact reader");
        let first = reader
            .next_frame()
            .expect("first frame read")
            .expect("first frame present");
        assert_eq!(first.sequence(), 0);
        assert_eq!(first.record_count(), 2);
        assert_eq!(first.payload(), b"first-frame");
        let second = reader
            .next_frame()
            .expect("second frame read")
            .expect("second frame present");
        assert_eq!(second.sequence(), 1);
        assert_eq!(second.record_count(), 1);
        assert_eq!(second.payload(), b"second");
        assert!(reader.next_frame().expect("terminal footer").is_none());
        let completion = reader.complete().expect("complete artifact read");
        assert_eq!(completion.seal(), seal);
        let read = completion.measurements();
        assert_eq!(read.artifact_bytes(), expected_artifact_bytes);
        assert_eq!(read.payload_bytes(), 17);
        assert_eq!(read.frame_count(), 2);
        assert_eq!(read.record_count(), 3);
        assert_eq!(read.transferred_bytes(), expected_artifact_bytes);
        assert_eq!(read.operations(), 10);
        assert_eq!(read.sha256_calls(), 3);
        assert_eq!(read.sha256_bytes(), write.sha256_bytes());
        assert_eq!(read.peak_buffer_bytes(), write.peak_buffer_bytes());
        assert_eq!(
            read.io_measurement().actual(),
            Some((expected_artifact_bytes, 10))
        );
        assert_eq!(artifact.buffer.as_ptr(), buffer_address);

        let mut second_reader = artifact.reader().expect("second replay reader");
        while second_reader
            .next_frame()
            .expect("second replay frame")
            .is_some()
        {}
        second_reader.complete().expect("second replay completion");
        assert_eq!(artifact.buffer.as_ptr(), buffer_address);
        assert_root_is_unlinked(root.path());
    }

    #[test]
    fn capacity_and_file_descriptor_lease_remain_until_artifact_drop() {
        let root = tempfile::tempdir().expect("artifact root");
        let retained_bytes = 700;
        let (authority, storage) = test_authority(root.path(), 1_000);
        let writer = GriddedNormalArtifactWriter::create(
            &authority,
            ResourcePolicy::Exclusive,
            &storage,
            budget(retained_bytes),
        )
        .expect("first writer");
        assert_eq!(
            writer._lease.declared_limit(&LeaseResource::Storage {
                demand_id: STORAGE_DEMAND_ID.to_string(),
                use_kind: StorageUseKind::Temporary,
            }),
            Some(retained_bytes)
        );
        assert_eq!(
            writer
                ._lease
                .declared_limit(&LeaseResource::FileDescriptors),
            Some(1)
        );
        assert_eq!(
            writer._lease.declared_limit(&LeaseResource::Memory {
                allocation_id: BUFFER_ALLOCATION_ID.to_string(),
            }),
            Some((FRAME_HEADER_BYTES + TEST_FRAME_PAYLOAD_BYTES) as u64)
        );
        assert_eq!(
            writer
                ._lease
                .declared_limit(&LeaseResource::IoBuffer(IoBufferKind::SpillRead)),
            Some((FRAME_HEADER_BYTES + TEST_FRAME_PAYLOAD_BYTES) as u64)
        );
        assert_root_is_unlinked(root.path());
        let artifact = writer.seal().expect("sealed empty artifact");
        let rejection = GriddedNormalArtifactWriter::create(
            &authority,
            ResourcePolicy::Exclusive,
            &storage,
            budget(retained_bytes),
        )
        .expect_err("retained temporary capacity must prevent over-admission");
        let GriddedNormalArtifactError::Resource(resource) = rejection else {
            panic!("unexpected second admission error: {rejection}");
        };
        assert_eq!(resource.required(), Some(retained_bytes));
        assert_eq!(resource.available(), Some(300));
        drop(artifact);

        let replacement = GriddedNormalArtifactWriter::create(
            &authority,
            ResourcePolicy::Exclusive,
            &storage,
            budget(retained_bytes),
        )
        .expect("synchronous drop releases retained resources");
        drop(replacement);
        assert_root_is_unlinked(root.path());
    }

    #[test]
    fn partial_read_cannot_mint_completion() {
        let (_root, _authority, mut artifact) = sealed_two_frame_artifact();
        let mut reader = artifact.reader().expect("artifact reader");
        assert!(reader.next_frame().expect("first frame").is_some());
        assert!(matches!(
            reader.complete(),
            Err(GriddedNormalArtifactError::IncompleteRead)
        ));
    }

    #[test]
    fn frame_corruption_is_rejected_before_payload_emission() {
        let (_root, _authority, mut artifact) = sealed_two_frame_artifact();
        let payload_offset = (FILE_HEADER_BYTES + FRAME_HEADER_BYTES) as u64;
        write_all_at(&artifact.file, b"X", payload_offset);
        let mut reader = artifact.reader().expect("artifact reader");
        assert!(matches!(
            reader.next_frame(),
            Err(GriddedNormalArtifactError::FrameChecksumMismatch { sequence: 0 })
        ));
        assert!(matches!(
            reader.next_frame(),
            Err(GriddedNormalArtifactError::ReaderPoisoned)
        ));
    }

    #[test]
    fn truncation_and_trailing_bytes_are_rejected() {
        let (_root, _authority, mut truncated) = sealed_two_frame_artifact();
        truncated
            .file
            .set_len(truncated.seal.artifact_bytes - 1)
            .expect("truncate artifact");
        assert!(matches!(
            truncated.reader(),
            Err(GriddedNormalArtifactError::TruncatedFile { .. })
        ));

        let (_root, _authority, mut trailing) = sealed_two_frame_artifact();
        trailing
            .file
            .set_len(trailing.seal.artifact_bytes + 1)
            .expect("extend artifact");
        assert!(matches!(
            trailing.reader(),
            Err(GriddedNormalArtifactError::TrailingData { .. })
        ));
    }

    #[test]
    fn duplicate_and_reordered_frames_are_rejected() {
        let (_root, _authority, mut duplicate) = sealed_two_frame_artifact();
        let second_frame_offset =
            (FILE_HEADER_BYTES + FRAME_HEADER_BYTES + b"first-frame".len()) as u64;
        write_all_at(
            &duplicate.file,
            &0_u64.to_le_bytes(),
            second_frame_offset + 16,
        );
        let mut reader = duplicate.reader().expect("duplicate reader");
        assert!(reader.next_frame().expect("first frame").is_some());
        assert!(matches!(
            reader.next_frame(),
            Err(GriddedNormalArtifactError::DuplicateFrame {
                expected: 1,
                actual: 0
            })
        ));

        let (_root, _authority, mut reordered) = sealed_two_frame_artifact();
        write_all_at(
            &reordered.file,
            &1_u64.to_le_bytes(),
            FILE_HEADER_BYTES as u64 + 16,
        );
        let mut reader = reordered.reader().expect("reordered reader");
        assert!(matches!(
            reader.next_frame(),
            Err(GriddedNormalArtifactError::ReorderedFrame {
                expected: 0,
                actual: 1
            })
        ));
    }

    #[test]
    fn footer_counts_and_global_integrity_are_required() {
        let (_root, _authority, mut wrong_count) = sealed_two_frame_artifact();
        let footer_frame_count_offset = wrong_count.seal.artifact_bytes - FOOTER_BYTES as u64 + 16;
        write_all_at(
            &wrong_count.file,
            &3_u64.to_le_bytes(),
            footer_frame_count_offset,
        );
        let mut reader = wrong_count.reader().expect("wrong-count reader");
        assert!(reader.next_frame().expect("first frame").is_some());
        assert!(reader.next_frame().expect("second frame").is_some());
        assert!(matches!(
            reader.next_frame(),
            Err(GriddedNormalArtifactError::FooterCountMismatch)
        ));

        let (_root, _authority, mut artifact) = sealed_two_frame_artifact();
        let footer_digest_offset = artifact.seal.artifact_bytes - 32;
        write_all_at(&artifact.file, &[0; 32], footer_digest_offset);
        let mut reader = artifact.reader().expect("artifact reader");
        assert!(reader.next_frame().expect("first frame").is_some());
        assert!(reader.next_frame().expect("second frame").is_some());
        assert!(matches!(
            reader.next_frame(),
            Err(GriddedNormalArtifactError::GlobalChecksumMismatch)
        ));
    }

    #[test]
    fn invalid_binding_and_oversized_frames_fail_closed() {
        let root = tempfile::tempdir().expect("artifact root");
        let (authority, storage) = test_authority(root.path(), TEST_CAPACITY_BYTES);
        let wrong_binding = StorageIoResourceBinding::new(
            storage.resources().domain().clone(),
            storage.resources().read_rate().clone(),
            storage.resources().write_rate().clone(),
            QueueResourceId::new("foreign-queue"),
        );
        let wrong_storage = GriddedNormalReplayStorage {
            resources: wrong_binding,
            directory: root.path().to_path_buf(),
        };
        assert!(matches!(
            GriddedNormalArtifactWriter::create(
                &authority,
                ResourcePolicy::Exclusive,
                &wrong_storage,
                budget(TEST_CAPACITY_BYTES),
            ),
            Err(GriddedNormalArtifactError::StorageBindingMismatch)
        ));

        let mut writer = GriddedNormalArtifactWriter::create(
            &authority,
            ResourcePolicy::Exclusive,
            &storage,
            budget(TEST_CAPACITY_BYTES),
        )
        .expect("artifact writer");
        let oversized = vec![0; TEST_FRAME_PAYLOAD_BYTES + 1];
        assert!(matches!(
            writer.append_frame(0, 1, &oversized),
            Err(GriddedNormalArtifactError::FramePayloadTooLarge { .. })
        ));
        assert!(matches!(
            writer.seal(),
            Err(GriddedNormalArtifactError::WriterPoisoned)
        ));
        assert_root_is_unlinked(root.path());
    }

    #[test]
    fn retained_capacity_is_checked_before_writing_a_frame() {
        let root = tempfile::tempdir().expect("artifact root");
        let minimum_capacity =
            (FILE_HEADER_BYTES + FRAME_HEADER_BYTES + TEST_FRAME_PAYLOAD_BYTES + FOOTER_BYTES)
                as u64;
        let (authority, storage) = test_authority(root.path(), minimum_capacity);
        let mut writer = GriddedNormalArtifactWriter::create(
            &authority,
            ResourcePolicy::Exclusive,
            &storage,
            budget(minimum_capacity),
        )
        .expect("capacity-bounded writer");
        writer
            .append_frame(0, 4, &[7; TEST_FRAME_PAYLOAD_BYTES])
            .expect("one maximum frame fits exactly");
        let bytes_before_rejection = writer.bytes_written;
        assert!(matches!(
            writer.append_frame(1, 1, &[8]),
            Err(GriddedNormalArtifactError::ArtifactCapacityExceeded { .. })
        ));
        assert_eq!(writer.bytes_written, bytes_before_rejection);
        assert!(matches!(
            writer.seal(),
            Err(GriddedNormalArtifactError::WriterPoisoned)
        ));
        GriddedNormalArtifactWriter::create(
            &authority,
            ResourcePolicy::Exclusive,
            &storage,
            budget(minimum_capacity),
        )
        .expect("failed writer synchronously releases its lease");
        assert_root_is_unlinked(root.path());
    }

    #[test]
    fn artifact_error_is_a_standard_error_with_resource_conversion() {
        fn assert_error<T: std::error::Error + Send + Sync + 'static>() {}
        assert_error::<GriddedNormalArtifactError>();
        let converted =
            GriddedNormalArtifactError::from(ResourceError::Invalid("test admission".to_string()));
        assert!(matches!(
            converted,
            GriddedNormalArtifactError::Resource(ResourceError::Invalid(_))
        ));
    }
}

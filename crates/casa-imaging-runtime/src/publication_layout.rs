// SPDX-License-Identifier: LGPL-3.0-or-later

//! Adapter-derived physical layouts for graph-owned publication members.

use std::{
    collections::{BTreeMap, BTreeSet},
    error::Error,
    fmt,
};

use casa_imaging_model::{MeasurementSetIdentity, ProductNodeId};

use crate::{AllocationId, ArtifactIdentity, IoBufferKind, WorkDependency, WorkNodeId};

/// Stable identity of one exact physical writer and layout choice.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct PhysicalLayoutId([u8; 32]);

impl PhysicalLayoutId {
    /// Construct an adapter-supplied identity from its already computed digest.
    #[must_use]
    pub const fn from_sha256(digest: [u8; 32]) -> Self {
        Self(digest)
    }

    /// Return the exact SHA-256 digest.
    #[must_use]
    pub const fn as_bytes(self) -> [u8; 32] {
        self.0
    }
}

impl fmt::Debug for PhysicalLayoutId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("PhysicalLayoutId(")?;
        write_hex(formatter, &self.0)?;
        formatter.write_str(")")
    }
}

impl fmt::Display for PhysicalLayoutId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write_hex(formatter, &self.0)
    }
}

/// Semantic member of one coordinated atomic publication.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum PublicationParticipant {
    /// One exact compiler-owned product graph node.
    Product(ProductNodeId),
    /// The optional `MODEL_DATA` member for one MeasurementSet.
    ModelData(MeasurementSetIdentity),
}

/// Adapter-derived hard resource bounds for one physical publication member.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PublicationResourceBounds {
    staged_storage_bytes: u64,
    final_storage_bytes: u64,
    writer_buffer_bytes: u64,
    mapped_page_cache_bytes: u64,
}

impl PublicationResourceBounds {
    /// Validate mandatory staged, final, and writer bounds.
    pub fn new(
        staged_storage_bytes: u64,
        final_storage_bytes: u64,
        writer_buffer_bytes: u64,
        mapped_page_cache_bytes: u64,
    ) -> Result<Self, PublicationResourceBoundsError> {
        for (kind, bytes) in [
            (PublicationBoundKind::StagedStorage, staged_storage_bytes),
            (PublicationBoundKind::FinalStorage, final_storage_bytes),
            (PublicationBoundKind::WriterBuffer, writer_buffer_bytes),
        ] {
            if bytes == 0 {
                return Err(PublicationResourceBoundsError::ZeroBound { kind });
            }
        }
        Ok(Self {
            staged_storage_bytes,
            final_storage_bytes,
            writer_buffer_bytes,
            mapped_page_cache_bytes,
        })
    }

    /// Return private staging storage required by this member.
    #[must_use]
    pub const fn staged_storage_bytes(self) -> u64 {
        self.staged_storage_bytes
    }

    /// Return activated final storage required by this member.
    #[must_use]
    pub const fn final_storage_bytes(self) -> u64 {
        self.final_storage_bytes
    }

    /// Return the maximum writer buffer required by this member.
    #[must_use]
    pub const fn writer_buffer_bytes(self) -> u64 {
        self.writer_buffer_bytes
    }

    /// Return mapped/page-cache exposure retained before publication.
    #[must_use]
    pub const fn mapped_page_cache_bytes(self) -> u64 {
        self.mapped_page_cache_bytes
    }
}

/// Exact producer-owned staging resources and terminal event for one member.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PublicationStaging {
    producer: WorkNodeId,
    terminal: WorkDependency,
    writer_buffer_kind: IoBufferKind,
    writer_allocation: AllocationId,
    mapped_page_cache: Option<PublicationMappedStaging>,
}

impl PublicationStaging {
    /// Bind one selected writer allocation to its producer and terminal event.
    pub fn new(
        producer: WorkNodeId,
        terminal: WorkDependency,
        writer_buffer_kind: IoBufferKind,
        writer_allocation: AllocationId,
    ) -> Result<Self, PublicationStagingError> {
        if event_node(&terminal) != &producer {
            return Err(PublicationStagingError::TerminalOwner { producer, terminal });
        }
        if !is_writer_buffer(writer_buffer_kind) {
            return Err(PublicationStagingError::InvalidWriterBufferKind {
                kind: writer_buffer_kind,
            });
        }
        Ok(Self {
            producer,
            terminal,
            writer_buffer_kind,
            writer_allocation,
            mapped_page_cache: None,
        })
    }

    /// Retain a distinct mapped/page-cache allocation until its terminal release event.
    #[must_use]
    pub fn with_mapped_page_cache(mut self, mapped: PublicationMappedStaging) -> Self {
        self.mapped_page_cache = Some(mapped);
        self
    }

    /// Return the exact producer of staged bytes.
    #[must_use]
    pub const fn producer(&self) -> &WorkNodeId {
        &self.producer
    }

    /// Return the exact producer terminal event required before publication.
    #[must_use]
    pub const fn terminal(&self) -> &WorkDependency {
        &self.terminal
    }

    /// Return the exact writer buffer category.
    #[must_use]
    pub const fn writer_buffer_kind(&self) -> IoBufferKind {
        self.writer_buffer_kind
    }

    /// Return the exact producer-owned writer allocation.
    #[must_use]
    pub const fn writer_allocation(&self) -> &AllocationId {
        &self.writer_allocation
    }

    /// Return the optional mapped/page-cache retention binding.
    #[must_use]
    pub const fn mapped_page_cache(&self) -> Option<&PublicationMappedStaging> {
        self.mapped_page_cache.as_ref()
    }
}

/// Exact mapped/page-cache allocation retained until a distinct release event.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PublicationMappedStaging {
    producer: WorkNodeId,
    terminal: WorkDependency,
    allocation: AllocationId,
}

impl PublicationMappedStaging {
    /// Bind mapped exposure to its acquisition node, release event, and allocation.
    pub fn new(
        producer: WorkNodeId,
        terminal: WorkDependency,
        allocation: AllocationId,
    ) -> Result<Self, PublicationStagingError> {
        if event_node(&terminal) == &producer {
            return Err(PublicationStagingError::MappedReleaseProducer { producer });
        }
        Ok(Self {
            producer,
            terminal,
            allocation,
        })
    }

    /// Return the mapped exposure acquisition node.
    #[must_use]
    pub const fn producer(&self) -> &WorkNodeId {
        &self.producer
    }

    /// Return the exact release event.
    #[must_use]
    pub const fn terminal(&self) -> &WorkDependency {
        &self.terminal
    }

    /// Return the exact mapped/page-cache allocation.
    #[must_use]
    pub const fn allocation(&self) -> &AllocationId {
        &self.allocation
    }
}

/// One exact physical layout in the coordinated publication set.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PublicationPhysicalLayout {
    participant: PublicationParticipant,
    artifact: ArtifactIdentity,
    layout_id: PhysicalLayoutId,
    staging: PublicationStaging,
    resource_bounds: PublicationResourceBounds,
}

impl PublicationPhysicalLayout {
    /// Declare one exact physical output layout and its hard resource bounds.
    #[must_use]
    pub const fn new(
        participant: PublicationParticipant,
        artifact: ArtifactIdentity,
        layout_id: PhysicalLayoutId,
        staging: PublicationStaging,
        resource_bounds: PublicationResourceBounds,
    ) -> Self {
        Self {
            participant,
            artifact,
            layout_id,
            staging,
            resource_bounds,
        }
    }

    /// Return the semantic publication member.
    #[must_use]
    pub const fn participant(&self) -> PublicationParticipant {
        self.participant
    }

    /// Return the exact plan-visible output artifact.
    #[must_use]
    pub const fn artifact(&self) -> ArtifactIdentity {
        self.artifact
    }

    /// Return the selected physical writer/layout identity.
    #[must_use]
    pub const fn layout_id(&self) -> PhysicalLayoutId {
        self.layout_id
    }

    /// Return the exact producer-owned staging binding.
    #[must_use]
    pub const fn staging(&self) -> &PublicationStaging {
        &self.staging
    }

    /// Return this member's adapter-derived hard resource bounds.
    #[must_use]
    pub const fn resource_bounds(&self) -> PublicationResourceBounds {
        self.resource_bounds
    }
}

/// Canonical complete physical-layout ledger for one publication set.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PublicationLayoutLedger {
    entries: Box<[PublicationPhysicalLayout]>,
    staged_storage_bytes: u64,
    final_storage_bytes: u64,
    writer_buffer_bytes: u64,
    mapped_page_cache_bytes: u64,
}

impl PublicationLayoutLedger {
    /// Construct the explicit empty ledger used only by output-free physical candidates.
    #[must_use]
    pub fn empty() -> Self {
        Self {
            entries: Box::new([]),
            staged_storage_bytes: 0,
            final_storage_bytes: 0,
            writer_buffer_bytes: 0,
            mapped_page_cache_bytes: 0,
        }
    }

    /// Validate and canonicalize every adapter-derived publication layout.
    pub fn new(
        mut entries: Vec<PublicationPhysicalLayout>,
    ) -> Result<Self, PublicationLayoutError> {
        if entries.is_empty() {
            return Err(PublicationLayoutError::EmptyLedger);
        }
        entries.sort_unstable_by_key(|entry| (entry.participant, entry.artifact));
        let mut artifacts = BTreeSet::new();
        let mut participants = BTreeSet::new();
        let mut staged_storage_bytes = 0_u64;
        let mut final_storage_bytes = 0_u64;
        let mut writer_bytes = BTreeMap::<WorkNodeId, u64>::new();
        let mut mapped_bytes = BTreeMap::<WorkNodeId, u64>::new();
        for entry in &entries {
            if entry.layout_id.as_bytes() == [0; 32] {
                return Err(PublicationLayoutError::EmptyLayoutIdentity {
                    artifact: entry.artifact,
                });
            }
            if !artifacts.insert(entry.artifact) {
                return Err(PublicationLayoutError::DuplicateArtifact {
                    artifact: entry.artifact,
                });
            }
            if !participants.insert(entry.participant) {
                return Err(PublicationLayoutError::DuplicateParticipant {
                    participant: entry.participant,
                });
            }
            match (
                entry.resource_bounds.mapped_page_cache_bytes(),
                entry.staging.mapped_page_cache(),
            ) {
                (0, Some(_)) => {
                    return Err(
                        PublicationLayoutError::UnexpectedMappedPageCacheAllocation {
                            artifact: entry.artifact,
                        },
                    );
                }
                (1.., None) => {
                    return Err(PublicationLayoutError::MissingMappedPageCacheAllocation {
                        artifact: entry.artifact,
                    });
                }
                _ => {}
            }
            staged_storage_bytes = staged_storage_bytes
                .checked_add(entry.resource_bounds.staged_storage_bytes())
                .ok_or(PublicationLayoutError::AggregateOverflow {
                    kind: PublicationBoundKind::StagedStorage,
                })?;
            final_storage_bytes = final_storage_bytes
                .checked_add(entry.resource_bounds.final_storage_bytes())
                .ok_or(PublicationLayoutError::AggregateOverflow {
                    kind: PublicationBoundKind::FinalStorage,
                })?;
            let producer_writer = writer_bytes
                .entry(entry.staging.producer().clone())
                .or_default();
            *producer_writer = producer_writer
                .checked_add(entry.resource_bounds.writer_buffer_bytes())
                .ok_or(PublicationLayoutError::AggregateOverflow {
                    kind: PublicationBoundKind::WriterBuffer,
                })?;
            if let Some(mapped) = entry.staging.mapped_page_cache() {
                let producer_mapped = mapped_bytes.entry(mapped.producer().clone()).or_default();
                *producer_mapped = producer_mapped
                    .checked_add(entry.resource_bounds.mapped_page_cache_bytes())
                    .ok_or(PublicationLayoutError::AggregateOverflow {
                        kind: PublicationBoundKind::MappedPageCache,
                    })?;
            }
        }
        Ok(Self {
            entries: entries.into_boxed_slice(),
            staged_storage_bytes,
            final_storage_bytes,
            writer_buffer_bytes: writer_bytes.into_values().max().unwrap_or(0),
            mapped_page_cache_bytes: mapped_bytes.into_values().max().unwrap_or(0),
        })
    }

    /// Return physical layouts in canonical participant/artifact order.
    #[must_use]
    pub const fn entries(&self) -> &[PublicationPhysicalLayout] {
        &self.entries
    }

    /// Return aggregate private staging storage required by selected layouts.
    #[must_use]
    pub const fn staged_storage_bytes(&self) -> u64 {
        self.staged_storage_bytes
    }

    /// Return aggregate activated final storage required by selected layouts.
    #[must_use]
    pub const fn final_storage_bytes(&self) -> u64 {
        self.final_storage_bytes
    }

    /// Return peak writer-buffer capacity across concurrently produced members.
    #[must_use]
    pub const fn writer_buffer_bytes(&self) -> u64 {
        self.writer_buffer_bytes
    }

    /// Return peak mapped/page-cache exposure across concurrent producers.
    #[must_use]
    pub const fn mapped_page_cache_bytes(&self) -> u64 {
        self.mapped_page_cache_bytes
    }
}

/// One physical publication bound whose declaration was invalid.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PublicationBoundKind {
    /// Private staged-output storage.
    StagedStorage,
    /// Activated final-output storage.
    FinalStorage,
    /// Writer-owned memory/I/O buffer.
    WriterBuffer,
    /// Mapped-file and page-cache exposure retained by staged writers.
    MappedPageCache,
}

/// Invalid adapter-derived member resource bounds.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PublicationResourceBoundsError {
    /// One mandatory physical bound was zero.
    ZeroBound {
        /// Missing bound category.
        kind: PublicationBoundKind,
    },
}

impl fmt::Display for PublicationResourceBoundsError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::ZeroBound { kind } => write!(formatter, "publication {kind:?} bound is zero"),
        }
    }
}

impl Error for PublicationResourceBoundsError {}

/// Invalid producer-owned staging declaration.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PublicationStagingError {
    /// The terminal event belongs to a node other than the declared producer.
    TerminalOwner {
        /// Declared producer.
        producer: WorkNodeId,
        /// Mismatched terminal event.
        terminal: WorkDependency,
    },
    /// The selected buffer category is not a writer category.
    InvalidWriterBufferKind {
        /// Invalid selected category.
        kind: IoBufferKind,
    },
    /// A mapped allocation named its acquisition node as its release event owner.
    MappedReleaseProducer {
        /// Acquisition node that cannot also be the release owner.
        producer: WorkNodeId,
    },
}

impl fmt::Display for PublicationStagingError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::TerminalOwner { producer, terminal } => write!(
                formatter,
                "publication terminal {terminal:?} is not owned by producer {}",
                producer.as_str()
            ),
            Self::InvalidWriterBufferKind { kind } => {
                write!(
                    formatter,
                    "publication buffer {kind:?} is not a writer category"
                )
            }
            Self::MappedReleaseProducer { producer } => write!(
                formatter,
                "mapped publication producer {} cannot also own its release event",
                producer.as_str()
            ),
        }
    }
}

impl Error for PublicationStagingError {}

/// Invalid adapter-derived physical publication ledger.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PublicationLayoutError {
    /// Atomic publication cannot omit every member.
    EmptyLedger,
    /// A physical layout identity was the all-zero sentinel.
    EmptyLayoutIdentity {
        /// Exact artifact with no stable layout identity.
        artifact: ArtifactIdentity,
    },
    /// Two entries named the same output artifact.
    DuplicateArtifact {
        /// Repeated artifact.
        artifact: ArtifactIdentity,
    },
    /// Two entries claimed the same semantic participant.
    DuplicateParticipant {
        /// Repeated participant.
        participant: PublicationParticipant,
    },
    /// A zero mapped bound nevertheless named a mapped allocation.
    UnexpectedMappedPageCacheAllocation {
        /// Contradictory artifact.
        artifact: ArtifactIdentity,
    },
    /// A nonzero mapped bound omitted its retained allocation.
    MissingMappedPageCacheAllocation {
        /// Incomplete artifact.
        artifact: ArtifactIdentity,
    },
    /// Checked aggregate accounting overflowed.
    AggregateOverflow {
        /// Overflowed bound category.
        kind: PublicationBoundKind,
    },
}

impl fmt::Display for PublicationLayoutError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "invalid publication layout: {self:?}")
    }
}

impl Error for PublicationLayoutError {}

pub(crate) const fn is_writer_buffer(kind: IoBufferKind) -> bool {
    matches!(
        kind,
        IoBufferKind::Serialization
            | IoBufferKind::StorageManager
            | IoBufferKind::TiledColumnWriter
            | IoBufferKind::ScalarColumnWriter
            | IoBufferKind::Writeback
    )
}

fn event_node(event: &WorkDependency) -> &WorkNodeId {
    match event {
        WorkDependency::Work(node) => node,
        WorkDependency::Fence(fence) => fence.node(),
    }
}

fn write_hex(formatter: &mut fmt::Formatter<'_>, bytes: &[u8]) -> fmt::Result {
    for byte in bytes {
        write!(formatter, "{byte:02x}")?;
    }
    Ok(())
}

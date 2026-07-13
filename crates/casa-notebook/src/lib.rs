// SPDX-License-Identifier: LGPL-3.0-or-later

//! Rust-owned scientific notebook and execution-receipt contracts.
//!
//! The visible source of truth is ordinary Markdown. Managed execution
//! receipts live separately under `.casa-rs/notebook-runs/`. This crate owns
//! both persisted formats so native, terminal, command-line, and Python
//! surfaces cannot drift into independent storage implementations.

mod assistant;
mod corpus;
mod ids;
mod markdown;
mod receipt;
mod store;
mod tutorial;
mod visualization;

pub use assistant::{
    ASSISTANT_PROTOCOL_VERSION, ASSISTANT_TRANSCRIPT_SCHEMA_VERSION, AssistantApproval,
    AssistantAttachment, AssistantAuthorityPolicy, AssistantCitation, AssistantCitationKind,
    AssistantContextItem, AssistantContextKind, AssistantCredentialLease, AssistantEffectivePolicy,
    AssistantEgressManifest, AssistantError, AssistantExecutableIdentity,
    AssistantExecutionBinding, AssistantInsertionBinding, AssistantMessage, AssistantMessageRole,
    AssistantPinReference, AssistantProposal, AssistantProposalDestination, AssistantProposalKind,
    AssistantProposalState, AssistantProtocolError, AssistantProtocolEvent,
    AssistantProtocolRequest, AssistantProviderCatalog, AssistantProviderModel,
    AssistantProviderOption, AssistantSidecarPolicy, AssistantStore, AssistantToolDefinition,
    ConversationTranscript,
};
pub use corpus::{
    CORPUS_EMBEDDING_DIMENSIONS, CORPUS_EMBEDDING_MODEL_VERSION, CORPUS_SCHEMA_VERSION,
    CorpusCitation, CorpusDocument, CorpusDocumentInput, CorpusError, CorpusIndex,
    CorpusIndexReport, CorpusLayer, CorpusSearchHit,
};
pub use ids::{
    AssistantMessageId, AssistantPinId, AssistantProposalId, CellId, ConversationId, NotebookId,
    RunId,
};
pub use markdown::{CellKind, NotebookCell, NotebookDocument, NotebookParseError, TaskCellIntent};
pub use receipt::{
    ApprovalRecord, ArtifactReference, ExecutionInput, ExecutionReceipt, ExecutionStatus,
    LogReferences, PythonEnvironmentIdentity, PythonExecutionAuthority, PythonExecutionInput,
    ReceiptFinalization, RecordingRequest, ReplayAssessment, RunSafetyRecord, Timestamp,
};
pub use store::{
    AttemptHandle, ConflictResolution, ExportMode, NotebookConflict, NotebookEntry,
    NotebookSnapshot, NotebookStore, RecordingPolicy, SaveResult, StoreError,
};
pub use tutorial::{
    TUTORIAL_LOCK_SCHEMA_VERSION, TUTORIAL_MANIFEST_SCHEMA_VERSION, TutorialAcquisitionApproval,
    TutorialAcquisitionPhase, TutorialAcquisitionPlan, TutorialArchiveFormat, TutorialAttemptKind,
    TutorialCheckKind, TutorialCheckOutcome, TutorialCheckStatus, TutorialDataset,
    TutorialDatasetAttempt, TutorialDatasetLock, TutorialError, TutorialForkResult, TutorialLock,
    TutorialManifest, TutorialOptionalCheck, TutorialProject, TutorialReadChunk,
    TutorialRegressionOverlay, TutorialSection, TutorialSourceResolution, TutorialTemplate,
    TutorialUnpackPlan, TutorialUriHandler, TutorialUriRegistry,
};
pub use visualization::{
    SaveVisualizationRequest, VISUALIZATION_SCHEMA_VERSION, VisualizationRenderMetadata,
    VisualizationReopenIntent, VisualizationRevision, VisualizationSnapshot,
};

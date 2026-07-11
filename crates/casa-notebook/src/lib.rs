// SPDX-License-Identifier: LGPL-3.0-or-later

//! Rust-owned scientific notebook and execution-receipt contracts.
//!
//! The visible source of truth is ordinary Markdown. Managed execution
//! receipts live separately under `.casa-rs/notebook-runs/`. This crate owns
//! both persisted formats so native, terminal, command-line, and Python
//! surfaces cannot drift into independent storage implementations.

mod ids;
mod markdown;
mod receipt;
mod store;

pub use ids::{CellId, NotebookId, RunId};
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

// SPDX-License-Identifier: LGPL-3.0-or-later

//! Pure-Rust image-tool runtime, persistence, metadata, and domain workflows.
//!
//! This module implements the stateful image-tool lifecycle (`ImageToolState`,
//! `AttachedImage`, `ImageHandle`, `Storage`, `BeamView`) and pure domain computations
//! independent of Python/PyO3.

pub mod add_axes;
pub mod attachment;
pub mod beam_ops;
pub(crate) mod beam_view;
pub mod beams;
pub mod calc;
pub mod concatenate;
pub mod conformance;
pub mod convertflux;
pub mod coordinate_conversion;
pub mod coords;
pub mod copy;
pub(crate) mod copy_region;
pub mod create;
pub mod data_compat;
pub mod error;
pub mod fits;
pub(crate) mod fits_rename;
pub mod handle;
pub mod imagecalc;
pub mod lifecycle;
pub mod locking;
pub mod mask_expression;
pub mod masks;
pub mod metadata;
pub(crate) mod miriad_rename;
pub(crate) mod native_rename;
pub mod ops_edit;
pub mod ops_fit;
pub mod ops_spatial;
pub mod ops_transform;
pub mod pixels;
pub mod point;
pub(crate) mod product_helpers;
pub(crate) mod publish;
pub mod region_read;
pub(crate) mod saved_expression;
pub mod serialization;
pub mod slice;
pub mod statistics;
pub mod subimage;
pub mod summary;
pub(crate) mod table_locks;
pub(crate) mod text_output;
pub mod transpose;

pub use attachment::{AttachedImage, ExternalPathCheck, no_external_open};
pub use beam_view::BeamView;
pub use error::{ImageToolError, ImageToolResult, ToolLogLevel, ToolLogLine};
pub use handle::{FitsAttachment, ImageHandle, Storage};
pub use lifecycle::ImageToolState;
pub use text_output::StagedTextOutput;

// SPDX-License-Identifier: LGPL-3.0-or-later

//! Pure-Rust image-tool runtime, persistence, metadata, and domain workflows.
//!
//! This module implements the stateful image-tool lifecycle (`ImageToolState`,
//! `AttachedImage`, `ImageHandle`, `Storage`, `BeamView`) and pure domain computations
//! independent of Python/PyO3.

pub mod add_axes;
pub mod attachment;
pub mod beam_ops;
pub mod beam_view;
pub mod beams;
pub mod calc;
pub mod concatenate;
pub mod conformance;
pub mod convertflux;
pub mod coordinate_conversion;
pub mod coords;
pub mod copy;
pub mod copy_region;
pub mod create;
pub mod error;
pub mod fits;
pub mod fits_rename;
pub mod handle;
pub mod imagecalc;
pub mod lifecycle;
pub mod locking;
pub mod mask_expression;
pub mod masks;
pub mod metadata;
pub mod miriad_rename;
pub mod native_rename;
pub mod ops_edit;
pub mod ops_fit;
pub mod ops_spatial;
pub mod ops_transform;
pub mod pixels;
pub mod point;
pub mod product_helpers;
pub mod publish;
pub mod region_read;
pub mod saved_expression;
pub mod serialization;
pub mod slice;
pub mod statistics;
pub mod subimage;
pub mod summary;
pub mod table_locks;
pub mod text_output;
pub mod transpose;

pub use attachment::{AttachedImage, ExternalPathCheck, no_external_open};
pub use beam_view::BeamView;
pub use error::{ImageToolError, ImageToolResult, ToolLogLevel, ToolLogLine};
pub use handle::{FitsAttachment, ImageHandle, Storage};
pub use lifecycle::ImageToolState;
pub use text_output::StagedTextOutput;

// SPDX-License-Identifier: LGPL-3.0-or-later
//! Error types for VLA archive ingestion.

use std::path::PathBuf;

use thiserror::Error;

/// Errors returned by VLA archive reader helpers.
#[derive(Debug, Error)]
pub enum VlaError {
    /// Wrapper for I/O failures.
    #[error("{context}: {source}")]
    Io {
        /// High-level operation description.
        context: String,
        /// Underlying I/O error.
        #[source]
        source: std::io::Error,
    },

    /// The archive file contents were not structurally valid.
    #[error("{path}: {message}")]
    InvalidArchive {
        /// File being decoded.
        path: PathBuf,
        /// Human-readable failure detail.
        message: String,
    },

    /// A user-facing argument could not be parsed.
    #[error("invalid argument `{argument}`: {message}")]
    InvalidArgument {
        /// Argument name.
        argument: &'static str,
        /// Why it failed.
        message: String,
    },

    /// The requested file list was empty.
    #[error("at least one archive file must be supplied")]
    NoArchiveFiles,

    /// The import pipeline could not complete.
    #[error("{message}")]
    Import {
        /// Human-readable import failure detail.
        message: String,
    },
}

impl VlaError {
    pub(crate) fn io(context: impl Into<String>, source: std::io::Error) -> Self {
        Self::Io {
            context: context.into(),
            source,
        }
    }

    pub(crate) fn invalid_archive(path: impl Into<PathBuf>, message: impl Into<String>) -> Self {
        Self::InvalidArchive {
            path: path.into(),
            message: message.into(),
        }
    }

    pub(crate) fn import(message: impl Into<String>) -> Self {
        Self::Import {
            message: message.into(),
        }
    }
}

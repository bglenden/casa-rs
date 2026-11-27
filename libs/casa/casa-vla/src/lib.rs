// SPDX-License-Identifier: LGPL-3.0-or-later
#![warn(missing_docs)]
#![doc = include_str!("../README.md")]
//!
//! <details>
//! <summary>API details</summary>
//!
//! Native helpers for disk-based VLA export archives.
//!
//! This crate starts the native Rust implementation of the CASA
//! `importvla` workflow. The current wave is intentionally limited to
//! disk-based archive files such as `*.xp1`/`*.xp5`, matching the user's
//! current scope and avoiding the legacy online/tape branches present in
//! CASA's C++ `VLAFiller`.
//!
//! The crate currently provides:
//!
//! - task-style option types mirroring CASA `importvla`
//! - logical-record reassembly for VLA disk export files
//! - Record Control Area (RCA) decoding
//! - archive scan summaries suitable for later importer planning/parity work
//!
//! </details>

mod disk;
mod error;
mod importer;
mod modcomp;
mod options;
mod output;
mod record;
mod selection;
mod summary;

pub use disk::{
    DiskPhysicalRecordHeader, LogicalRecord, MAX_LOGICAL_RECORD_SIZE, PHYSICAL_RECORD_DATA_BYTES,
    PHYSICAL_RECORD_SIZE, VlaDiskReader,
};
pub use error::VlaError;
pub use importer::{
    ImportReport, import_archive_files_to_measurement_set_from_options, import_archive_with_history,
};
pub use options::{AntennaNameScheme, BandName, ImportVlaOptions};
pub use record::{
    AntennaDataArea, BaselineRecord, CdaId, CircularPolarization, ContinuumBaselineRecord,
    CorrelatorDataArea, CorrelatorMode, DirectionEpoch, DopplerDefinition, FrequencyFrame, IfId,
    IfUsage, ModcompCursor, RecordControlArea, SpectralLineRecord, StokesProduct, SubarrayDataArea,
};
pub use summary::{
    ArchiveFileSummary, ArchiveSummary, scan_disk_archive_files,
    scan_disk_archive_files_from_options,
};

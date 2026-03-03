// SPDX-License-Identifier: LGPL-3.0-or-later
//! Table synchronization data stored in the lock file.
//!
//! This encodes/decodes the `TableSyncData` payload that C++ casacore writes
//! into the `table.lock` file after the request-list header. The encoding
//! uses AipsIO object framing so that Rust and C++ can interoperate.
//!
//! # Wire format
//!
//! AipsIO object "sync", version 1 or 2:
//! - v1: `nrrow` as `u32`; v2: `nrrow` as `u64`
//! - `nrcolumn` as `i32`
//! - `modify_counter` as `u32`
//! - If `nrcolumn >= 0`:
//!   - `table_change_counter` as `u32`
//!   - `data_man_change_counters` as AipsIO `Block<uInt>` (nested object
//!     "Block" version 1: count as `u32`, then `count` `u32` values)
//!
//! # C++ reference
//!
//! `TableSyncData.cc`, `BlockIO.tcc`

use std::io::{self, Cursor};

use casacore_aipsio::AipsIo;

/// Maximum row number that fits in a 32-bit unsigned integer.
/// Rows above this threshold trigger version-2 encoding.
/// Matches C++ `DataManager::MAXROWNR32`.
const MAX_ROWNR_32: u64 = u32::MAX as u64;

/// Table synchronization counters.
///
/// These counters track modifications so that a process acquiring a lock
/// can determine whether the table has changed since it last held the lock.
///
/// C++ equivalent: `TableSyncData`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct SyncData {
    /// Number of rows in the table.
    pub nrrow: u64,
    /// Number of columns, or −1 if the table structure is unknown
    /// (used by external fillers).
    pub nrcolumn: i32,
    /// Incremented on any change to the table.
    pub modify_counter: u32,
    /// Incremented when the table structure (schema) changes.
    pub table_change_counter: u32,
    /// Per-data-manager change counters; incremented when a DM's data changes.
    pub data_man_change_counters: Vec<u32>,
}

impl SyncData {
    /// Create a new `SyncData` with all counters at zero and `nrcolumn = -1`.
    pub fn new() -> Self {
        Self {
            nrrow: 0,
            nrcolumn: -1,
            modify_counter: 0,
            table_change_counter: 0,
            data_man_change_counters: Vec::new(),
        }
    }

    /// Encode into the AipsIO "sync" object wire format.
    ///
    /// Uses version 2 if `nrrow` exceeds `u32::MAX`, otherwise version 1.
    pub fn encode(&self) -> Result<Vec<u8>, io::Error> {
        let buf = Cursor::new(Vec::<u8>::new());
        let mut io = AipsIo::new_write_only(buf);

        let version = if self.nrrow > MAX_ROWNR_32 { 2 } else { 1 };
        io.putstart("sync", version).map_err(aipsio_to_io)?;

        if version == 2 {
            io.put_u64(self.nrrow).map_err(aipsio_to_io)?;
        } else {
            io.put_u32(self.nrrow as u32).map_err(aipsio_to_io)?;
        }
        io.put_i32(self.nrcolumn).map_err(aipsio_to_io)?;
        io.put_u32(self.modify_counter).map_err(aipsio_to_io)?;

        if self.nrcolumn >= 0 {
            io.put_u32(self.table_change_counter)
                .map_err(aipsio_to_io)?;
            // Block<uInt>: nested AipsIO object "Block" version 1.
            self.encode_block_u32(&mut io)?;
        }

        io.putend().map_err(aipsio_to_io)?;

        // Extract the Vec<u8> from the Cursor via into_inner_typed.
        let cursor: Cursor<Vec<u8>> = io
            .into_inner_typed()
            .expect("cursor should downcast to Cursor<Vec<u8>>");
        Ok(cursor.into_inner())
    }

    /// Decode from AipsIO "sync" object bytes.
    pub fn decode(data: &[u8]) -> Result<Self, io::Error> {
        let buf = Cursor::new(data.to_vec());
        let mut io = AipsIo::new_read_only(buf);

        let version = io.getstart("sync").map_err(aipsio_to_io)?;
        if version > 2 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("unsupported sync version {version}"),
            ));
        }

        let nrrow = if version == 2 {
            io.get_u64().map_err(aipsio_to_io)?
        } else {
            u64::from(io.get_u32().map_err(aipsio_to_io)?)
        };

        let nrcolumn = io.get_i32().map_err(aipsio_to_io)?;
        let modify_counter = io.get_u32().map_err(aipsio_to_io)?;

        let (table_change_counter, data_man_change_counters) = if nrcolumn >= 0 {
            let tcc = io.get_u32().map_err(aipsio_to_io)?;
            let dmcc = Self::decode_block_u32(&mut io)?;
            (tcc, dmcc)
        } else {
            (0, Vec::new())
        };

        io.getend().map_err(aipsio_to_io)?;

        Ok(Self {
            nrrow,
            nrcolumn,
            modify_counter,
            table_change_counter,
            data_man_change_counters,
        })
    }

    /// Update counters after a write operation.
    ///
    /// Increments `modify_counter` if anything changed. Increments
    /// `table_change_counter` if the table structure changed. Increments
    /// per-DM counters for each DM that changed.
    pub fn record_write(
        &mut self,
        nrrow: u64,
        nrcolumn: u32,
        table_changed: bool,
        dm_changed: &[bool],
    ) {
        self.nrrow = nrrow;
        self.nrcolumn = nrcolumn as i32;

        let mut changed = false;
        if table_changed {
            self.table_change_counter = self.table_change_counter.wrapping_add(1);
            changed = true;
        }

        // Resize DM counters if needed, initializing new entries to 0.
        let old_len = self.data_man_change_counters.len();
        let new_len = dm_changed.len();
        if new_len > old_len {
            self.data_man_change_counters.resize(new_len, 0);
        }
        for (i, &dm_did_change) in dm_changed.iter().enumerate() {
            if dm_did_change {
                self.data_man_change_counters[i] = self.data_man_change_counters[i].wrapping_add(1);
                changed = true;
            }
        }

        if changed {
            self.modify_counter = self.modify_counter.wrapping_add(1);
        }
    }

    /// Returns `true` if the table needs to be reloaded because
    /// another process modified it (different `modify_counter`).
    pub fn needs_reload(&self, other: &SyncData) -> bool {
        self.modify_counter != other.modify_counter
    }

    /// Encode `data_man_change_counters` as a C++ `Block<uInt>`:
    /// AipsIO object "Block" version 1 containing `putAipsIO(nr, data)`
    /// which writes count as u32 then the values.
    fn encode_block_u32(&self, io: &mut AipsIo) -> Result<(), io::Error> {
        io.putstart("Block", 1).map_err(aipsio_to_io)?;
        io.put_u32_slice(&self.data_man_change_counters, true)
            .map_err(aipsio_to_io)?;
        io.putend().map_err(aipsio_to_io)?;
        Ok(())
    }

    /// Decode a C++ `Block<uInt>`: AipsIO object "Block" version 1.
    fn decode_block_u32(io: &mut AipsIo) -> Result<Vec<u32>, io::Error> {
        io.getstart("Block").map_err(aipsio_to_io)?;
        let values = io.getnew_u32().map_err(aipsio_to_io)?;
        io.getend().map_err(aipsio_to_io)?;
        Ok(values)
    }
}

/// Convert `AipsIoObjectError` to `io::Error`.
fn aipsio_to_io(e: casacore_aipsio::AipsIoObjectError) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, e.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn round_trip_v1_small_row_count() {
        let original = SyncData {
            nrrow: 100,
            nrcolumn: 2,
            modify_counter: 5,
            table_change_counter: 3,
            data_man_change_counters: vec![1, 2],
        };
        let encoded = original.encode().expect("encode");
        let decoded = SyncData::decode(&encoded).expect("decode");
        assert_eq!(original, decoded);
    }

    #[test]
    fn round_trip_v2_large_row_count() {
        let original = SyncData {
            nrrow: u64::from(u32::MAX) + 1,
            nrcolumn: 1,
            modify_counter: 10,
            table_change_counter: 7,
            data_man_change_counters: vec![42],
        };
        let encoded = original.encode().expect("encode");
        let decoded = SyncData::decode(&encoded).expect("decode");
        assert_eq!(original, decoded);
    }

    #[test]
    fn round_trip_negative_nrcolumn() {
        // nrcolumn < 0 means "external filler" mode — no table/DM counters.
        let original = SyncData {
            nrrow: 50,
            nrcolumn: -1,
            modify_counter: 1,
            table_change_counter: 0,
            data_man_change_counters: Vec::new(),
        };
        let encoded = original.encode().expect("encode");
        let decoded = SyncData::decode(&encoded).expect("decode");
        assert_eq!(original, decoded);
    }

    #[test]
    fn round_trip_empty_dm_counters() {
        let original = SyncData {
            nrrow: 0,
            nrcolumn: 0,
            modify_counter: 0,
            table_change_counter: 0,
            data_man_change_counters: Vec::new(),
        };
        let encoded = original.encode().expect("encode");
        let decoded = SyncData::decode(&encoded).expect("decode");
        assert_eq!(original, decoded);
    }

    #[test]
    fn record_write_increments_counters() {
        let mut sync = SyncData::new();
        assert_eq!(sync.modify_counter, 0);

        // Write with table change and one DM change.
        sync.record_write(10, 2, true, &[true, false]);
        assert_eq!(sync.nrrow, 10);
        assert_eq!(sync.nrcolumn, 2);
        assert_eq!(sync.modify_counter, 1);
        assert_eq!(sync.table_change_counter, 1);
        assert_eq!(sync.data_man_change_counters, vec![1, 0]);

        // Write with only DM 1 change.
        sync.record_write(10, 2, false, &[false, true]);
        assert_eq!(sync.modify_counter, 2);
        assert_eq!(sync.table_change_counter, 1); // unchanged
        assert_eq!(sync.data_man_change_counters, vec![1, 1]);

        // Write with no changes.
        sync.record_write(10, 2, false, &[false, false]);
        assert_eq!(sync.modify_counter, 2); // unchanged
    }

    #[test]
    fn needs_reload_detects_counter_change() {
        let a = SyncData {
            modify_counter: 5,
            ..SyncData::new()
        };
        let b = SyncData {
            modify_counter: 5,
            ..SyncData::new()
        };
        assert!(!a.needs_reload(&b));

        let c = SyncData {
            modify_counter: 6,
            ..SyncData::new()
        };
        assert!(a.needs_reload(&c));
    }
}

// SPDX-License-Identifier: LGPL-3.0-or-later

//! Stored row facts and their bounded read reservation.

/// The stored MAIN columns of one row that the selected-observation row
/// predicate and the bounded row traversal read.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct SelectedObservationRow {
    pub(crate) physical_row: usize,
    pub(crate) array_id: i32,
    pub(crate) data_description_id: i32,
    pub(crate) field_id: i32,
    pub(crate) antenna1: i32,
    pub(crate) antenna2: i32,
    pub(crate) time_mjd_seconds: f64,
    pub(crate) time_centroid_mjd_seconds: f64,
    pub(crate) state_id: i32,
    pub(crate) observation_id: i32,
    pub(crate) scan_number: Option<i32>,
    pub(crate) flag_row: bool,
    pub(crate) uvw_m: [f64; 3],
}

impl SelectedObservationRow {
    /// Maximum bytes read from stored MAIN columns for each row: one cell for
    /// every field but the physical row index (`ARRAY_ID`, `DATA_DESC_ID`, `FIELD_ID`,
    /// `ANTENNA1`, `ANTENNA2`, `STATE_ID`, `OBSERVATION_ID`, optional `SCAN_NUMBER`; `TIME` and
    /// `TIME_CENTROID`; `FLAG_ROW`; `UVW`).
    pub const STORAGE_BYTES_PER_ROW: usize =
        size_of::<[i32; 8]>() + size_of::<[f64; 2]>() + size_of::<bool>() + size_of::<[f64; 3]>();

    /// Return the physical MAIN row index.
    #[must_use]
    pub const fn physical_row(self) -> usize {
        self.physical_row
    }

    /// Return the stored `ARRAY_ID`, the first CASA visibility-iterator sort key.
    #[must_use]
    pub const fn array_id(self) -> i32 {
        self.array_id
    }

    /// Return the stored `DATA_DESC_ID`.
    #[must_use]
    pub const fn data_description_id(self) -> i32 {
        self.data_description_id
    }

    /// Return the stored `FIELD_ID`.
    #[must_use]
    pub const fn field_id(self) -> i32 {
        self.field_id
    }

    /// Return the stored first antenna identifier.
    #[must_use]
    pub const fn antenna1(self) -> i32 {
        self.antenna1
    }

    /// Return the stored second antenna identifier.
    #[must_use]
    pub const fn antenna2(self) -> i32 {
        self.antenna2
    }

    /// Return the stored `TIME` in MJD seconds.
    #[must_use]
    pub const fn time_mjd_seconds(self) -> f64 {
        self.time_mjd_seconds
    }

    /// Return the stored `TIME_CENTROID` in MJD seconds.
    #[must_use]
    pub const fn time_centroid_mjd_seconds(self) -> f64 {
        self.time_centroid_mjd_seconds
    }

    /// Return the stored `STATE_ID`.
    #[must_use]
    pub const fn state_id(self) -> i32 {
        self.state_id
    }

    /// Return the stored `OBSERVATION_ID`.
    #[must_use]
    pub const fn observation_id(self) -> i32 {
        self.observation_id
    }

    /// Return the stored scan number when requested by the row predicate.
    #[must_use]
    pub const fn scan_number(self) -> Option<i32> {
        self.scan_number
    }

    /// Return the stored row-level flag.
    #[must_use]
    pub const fn flag_row(self) -> bool {
        self.flag_row
    }

    /// Return the stored UVW coordinates in metres.
    #[must_use]
    pub const fn uvw_m(self) -> [f64; 3] {
        self.uvw_m
    }
}

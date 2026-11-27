// SPDX-License-Identifier: LGPL-3.0-or-later
#[path = "../../../../../libs/casa/casa-calibration/tests/common/mod.rs"]
mod native_fixtures;
mod oracle;

mod common {
    pub(super) use super::native_fixtures::*;
    pub(super) use super::oracle::*;
}

mod calstat;
mod casa_calibration_parity;
mod gain_table_windows;

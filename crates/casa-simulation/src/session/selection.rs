// SPDX-License-Identifier: LGPL-3.0-or-later
//! Immutable selections retained by DATA operations; later setters cannot
//! change an earlier operation's selected rows.

use std::sync::Arc;

use super::observe::Scan;

#[derive(Debug, Clone, Default, PartialEq)]
pub(crate) enum DataSelection {
    #[default]
    All,
    Fields {
        fields: Arc<[i64]>,
        windows: Arc<[usize]>,
    },
}

impl DataSelection {
    pub(crate) fn fields(fields: &[i64], mut windows: Vec<usize>) -> Self {
        let mut fields = fields.to_vec();
        fields.sort_unstable();
        fields.dedup();
        windows.sort_unstable();
        windows.dedup();
        Self::Fields {
            fields: fields.into(),
            windows: windows.into(),
        }
    }

    pub(crate) fn is_all(&self) -> bool {
        matches!(self, Self::All)
    }

    pub(crate) fn contains_scan(&self, scan: &Scan) -> bool {
        match self {
            Self::All => true,
            Self::Fields { fields, windows } => {
                (fields.is_empty() || fields.binary_search(&(scan.field_id as i64)).is_ok())
                    && (windows.is_empty()
                        || windows.binary_search(&scan.spectral_window_id).is_ok())
            }
        }
    }
}

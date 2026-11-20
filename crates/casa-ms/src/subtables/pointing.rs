// SPDX-License-Identifier: LGPL-3.0-or-later
//! Typed wrappers for the POINTING subtable.
//!
//! Cf. C++ `MSPointingColumns`.

use casa_types::{ArrayValue, measures::direction::MDirection};

use crate::derived::engine::resolve_direction_reference;
use crate::schema::{self, SubtableId};
use crate::{MsError, MsResult};

crate::subtables::define_generic_subtable!(
    read = MsPointing,
    write = MsPointingMut,
    id = SubtableId::Pointing,
    required = schema::pointing::REQUIRED_COLUMNS,
    optional = schema::pointing::OPTIONAL_COLUMNS,
    table_name = "POINTING",
    read_doc = "Read-only typed wrapper for the POINTING subtable.",
    write_doc = "Mutable typed wrapper for the POINTING subtable."
);

impl MsPointing<'_> {
    /// Read the pointing used by CASA's legacy visibility buffer for an
    /// antenna and visibility time. The returned direction is the recorded
    /// direction at the pointing row's epoch.
    ///
    /// # Errors
    /// The POINTING metadata or selected direction cell is invalid.
    pub fn legacy_direction(&self, antenna: i32, time: f64) -> MsResult<Option<MDirection>> {
        let mut rows = Vec::new();
        for row in 0..self.row_count() {
            if self.i32(row, "ANTENNA_ID")? == antenna {
                rows.push((row, self.f64(row, "TIME")?, self.f64(row, "INTERVAL")?));
            }
        }
        let nearest_sample = rows
            .iter()
            .filter(|(_, _, interval)| *interval <= 0.)
            .min_by(|a, b| (a.1 - time).abs().total_cmp(&(b.1 - time).abs()));
        let selected = rows
            .iter()
            .enumerate()
            .find(|&(index, &(_, epoch, interval))| {
                if interval > 0. {
                    return (epoch - time).abs() <= interval / 2.;
                }
                let neighbor = rows[index + 1..]
                    .iter()
                    .chain(rows[..index].iter().rev())
                    .find(|(_, candidate, _)| *candidate != epoch);
                neighbor
                    .is_none_or(|(_, next, _)| (epoch - time).abs() <= (epoch - next).abs() / 2.)
            })
            .map(|(_, sample)| sample);
        let Some(&(row, _, _)) = (match (selected, nearest_sample) {
            (Some(covering), Some(closest))
                if (closest.1 - time).abs() <= (covering.1 - time).abs() =>
            {
                Some(closest)
            }
            (covering, closest) => covering.or(closest),
        }) else {
            return Ok(None);
        };
        let ArrayValue::Float64(values) = self.array(row, "DIRECTION")? else {
            return Err(MsError::InvalidInput(
                "POINTING DIRECTION must be double".into(),
            ));
        };
        let (Some(&lon), Some(&lat)) = (
            values.get(ndarray::IxDyn(&[0, 0])),
            values.get(ndarray::IxDyn(&[1, 0])),
        ) else {
            return Err(MsError::InvalidInput(
                "invalid POINTING DIRECTION shape".into(),
            ));
        };
        let reference = resolve_direction_reference(self.table(), "POINTING", "DIRECTION", row)?;
        Ok(Some(MDirection::from_angles(lon, lat, reference)))
    }
}

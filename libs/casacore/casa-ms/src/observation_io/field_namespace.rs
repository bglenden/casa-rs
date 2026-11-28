// SPDX-License-Identifier: LGPL-3.0-or-later
//! Field selection for calibration-table consumers.

use std::path::Path;

use casa_core::measures::direction::MDirection;
use casa_observation::ObservationError;
use casa_tables::{Table, TableOptions, TablePartNames};

use super::msv2::catalogue::require_units;
use super::msv2::held;
use super::update::storage;
use crate::columns::direction_columns::DirectionColumn;
use crate::derived::engine::resolve_direction_reference_selected;
use crate::selection::parser::{FieldSelectorRow, resolve_field_selector_rows};
use crate::subtables::{MsField, optional_bool, optional_i32, optional_string};
use crate::{MeasurementSet, MsError, MsResult};

/// Field selection in an existing observation's format.
pub enum FieldNamespace<'a> {
    /// Resolve selectors in the source MSv2's FIELD numbering.
    Msv2(Msv2FieldSelection<'a>),
    /// Resolve selectors in a calibration table and bind them to the processing set.
    Msv4(&'a Msv4FieldSelection),
}

/// A selector backed by the MSv2's FIELD subtable.
pub struct Msv2FieldSelection<'a>(&'a MeasurementSet);

impl Msv2FieldSelection<'_> {
    /// Resolve a CASA selector to FIELD indices. Empty text selects every
    /// field and returns an empty list.
    ///
    /// # Errors
    /// When the selector or FIELD metadata is invalid.
    pub fn select(&self, selector: &str) -> MsResult<Vec<i32>> {
        self.0.resolve_field_selector(selector)
    }
}

/// The MSv4 field identities needed to validate calibration table selectors.
pub struct Msv4FieldSelection {
    fields: Vec<ObservationField>,
    kept: usize,
    budget: usize,
}

struct ObservationField {
    name: String,
    phase_centre: MDirection,
    tracks_ephemeris: bool,
}

struct CalibrationField {
    name: String,
    code: Option<String>,
    flagged: bool,
    fixed_direction: Option<MDirection>,
}

/// Open the field namespace of either store for `query`. MSv2 resolves
/// selectors over its FIELD subtable. MSv4 keeps its field identities and
/// releases the rest of the admitted catalogue before invoking the caller.
///
/// # Errors
/// When the store cannot be opened or the MSv4 catalogue cannot be admitted.
pub fn with_field_namespace<T>(
    source: &Path,
    query: impl FnOnce(FieldNamespace<'_>) -> T,
) -> Result<T, ObservationError> {
    if super::is_msv4_store(source) {
        #[cfg(all(feature = "msv4", unix))]
        {
            let fields =
                Msv4FieldSelection::open(source, super::creation::default_memory_bytes()?)?;
            return Ok(query(FieldNamespace::Msv4(&fields)));
        }
        #[cfg(not(all(feature = "msv4", unix)))]
        return Err(ObservationError::Unsupported {
            what: "an MSv4 field namespace without casa-ms's msv4 feature",
        });
    }
    let ms = MeasurementSet::open(source).map_err(storage)?;
    Ok(query(FieldNamespace::Msv2(Msv2FieldSelection(&ms))))
}

impl Msv4FieldSelection {
    #[cfg(all(feature = "msv4", unix))]
    fn open(source: &Path, budget: usize) -> Result<Self, ObservationError> {
        let mut admit = |required| admit(budget, required, "MSv4 field catalogue opening");
        let observation = super::Msv4Observation::open_granted(
            source,
            &super::OpenOptions::default(),
            &mut admit,
        )?;
        let catalogue = &observation.catalogue().fields;
        let kept = catalogue.len() * size_of::<ObservationField>()
            + catalogue
                .iter()
                .map(|field| field.name.len())
                .sum::<usize>();
        admit(observation.held_bytes().saturating_add(kept))?;
        let fields = catalogue
            .iter()
            .map(|field| ObservationField {
                name: field.name.clone(),
                phase_centre: field.phase_centre.clone(),
                tracks_ephemeris: field.ephemeris.is_some(),
            })
            .collect();
        Ok(Self {
            fields,
            kept,
            budget,
        })
    }

    /// Resolve a CASA selector in the calibration table's own FIELD
    /// numbering, requiring a unique MSv4 identity for every selected row.
    /// Empty text selects every calibration field and returns an empty list.
    ///
    /// # Errors
    /// When the FIELD subtable or selector is invalid, the fields cannot be
    /// matched uniquely, a selected identity moves with time, or the
    /// calibration FIELD read cannot be admitted.
    pub fn select(&self, directory: &Path, selector: &str) -> Result<Vec<i32>, ObservationError> {
        if selector.trim().is_empty() {
            return Ok(Vec::new());
        }
        let rows = self.read_calibration_fields(directory, selector)?;
        let fields: Vec<_> = rows
            .iter()
            .map(|field| FieldSelectorRow {
                name: &field.name,
                code: field.code.as_deref(),
                flagged: field.flagged,
            })
            .collect();
        let selected = resolve_field_selector_rows(&fields, selector).map_err(storage)?;
        for &id in &selected {
            self.bind_field(&rows, id as usize)?;
        }
        Ok(selected)
    }

    fn read_calibration_fields(
        &self,
        directory: &Path,
        selector: &str,
    ) -> Result<Vec<CalibrationField>, ObservationError> {
        let path = directory.join("FIELD");
        let opening_bytes = self.kept.saturating_add(held::opening_bytes(&path));
        let what = "MSv4 calibration FIELD binding";
        admit(self.budget, opening_bytes, what)?;
        let layout = Table::immediate_part_names(&path, &path)
            .map_err(|error| storage(MsError::from(error)))?;
        if !matches!(layout, TablePartNames::Plain(_)) {
            return Err(ObservationError::Unsupported {
                what: "binding a calibration FIELD reference or concatenation to MSv4",
            });
        }
        let table = Table::open(TableOptions::new(path)).map_err(|e| storage(MsError::from(e)))?;
        if table.row_count() > i32::MAX as usize {
            return Err(ObservationError::InvalidStore {
                reason: "calibration FIELD row numbers exceed the signed selector range".to_owned(),
            });
        }
        let required = opening_bytes
            // The opened table's row and column caches plus its projected NAME and CODE.
            .saturating_add(held::loaded_bytes(&table).saturating_mul(2))
            .saturating_add(binding_scratch_bytes(table.row_count(), selector));
        admit(self.budget, required, what)?;
        require_units(&table, "PHASE_DIR", "rad")?;
        let field = MsField::new(&table);
        (0..table.row_count())
            .map(|row| {
                Ok(CalibrationField {
                    name: field.name(row).map_err(storage)?,
                    code: optional_string(&table, row, "CODE").map_err(storage)?,
                    flagged: optional_bool(&table, row, "FLAG_ROW")
                        .map_err(storage)?
                        .unwrap_or_default(),
                    fixed_direction: fixed_direction(&table, &field, row)?,
                })
            })
            .collect()
    }

    fn bind_field(&self, rows: &[CalibrationField], id: usize) -> Result<(), ObservationError> {
        let field = &rows[id];
        let Some(direction) = &field.fixed_direction else {
            return Err(ObservationError::Unsupported {
                what: "binding a moving or polynomial calibration FIELD to an MSv4 field",
            });
        };
        let mut matched = None;
        for entry in self
            .fields
            .iter()
            .filter(|entry| names_agree(&entry.name, &field.name))
        {
            // An ephemeris-tracking field stores an offset here. Even a zero offset
            // cannot identify a fixed direction with the same value.
            if entry.tracks_ephemeris {
                return Err(ObservationError::Unsupported {
                    what: "binding an ephemeris-tracking MSv4 field to a calibration FIELD",
                });
            }
            if same_direction(&entry.phase_centre, direction) && matched.replace(entry).is_some() {
                return Err(ambiguous(&field.name));
            }
        }
        let Some(entry) = matched else {
            return Err(ObservationError::InvalidRequest {
                reason: format!(
                    "calibration table field {} is absent from the MSv4 store",
                    field.name
                ),
            });
        };
        if rows.iter().enumerate().any(|(row, other)| {
            row != id
                && names_agree(&entry.name, &other.name)
                && other
                    .fixed_direction
                    .as_ref()
                    .is_some_and(|centre| same_direction(&entry.phase_centre, centre))
        }) {
            return Err(ambiguous(&field.name));
        }
        Ok(())
    }
}

fn binding_scratch_bytes(rows: usize, selector: &str) -> usize {
    let terms = selector.bytes().filter(|byte| *byte == b',').count() + 1;
    // Each term can append every row. Account for the growing vector and
    // temporary matches, numeric ranges and two possible numeric aliases per row.
    let ids = terms.saturating_add(4).saturating_mul(3 * size_of::<i32>());
    let aliases = 2 * (size_of::<(i32, i32)>() + 4 * size_of::<usize>());
    let per_row = size_of::<CalibrationField>() + size_of::<FieldSelectorRow>() + aliases;
    rows.saturating_mul(per_row.saturating_add(ids))
        .saturating_add(terms.saturating_mul(3 * size_of::<(i32, i32)>()))
        .saturating_add(selector.len())
}

fn admit(budget: usize, required: usize, what: &'static str) -> Result<(), ObservationError> {
    if required > budget {
        Err(ObservationError::OverBudget {
            what,
            required,
            budget,
        })
    } else {
        Ok(())
    }
}

pub(super) fn fixed_direction(
    table: &Table,
    field: &MsField<'_>,
    row: usize,
) -> Result<Option<MDirection>, ObservationError> {
    if field
        .ephemeris_id(row)
        .map_err(storage)?
        .is_some_and(|id| id >= 0)
        || optional_i32(table, row, "NUM_POLY")
            .map_err(storage)?
            .is_some_and(|terms| terms > 0)
        || field
            .phase_dir(row)
            .map_err(storage)?
            .shape()
            .get(1)
            .is_some_and(|&terms| terms > 1)
    {
        return Ok(None);
    }
    let (lon, lat) = DirectionColumn::phase_dir(table)
        .get_angles(row)
        .map_err(storage)?;
    let frame =
        resolve_direction_reference_selected(table, "FIELD", "PHASE_DIR", row).map_err(storage)?;
    Ok(Some(MDirection::from_angles(lon, lat, frame)))
}

fn names_agree(msv4: &str, calibration: &str) -> bool {
    msv4 == calibration
        || msv4
            .rsplit_once('_')
            .is_some_and(|(base, suffix)| base == calibration && suffix.parse::<u32>().is_ok())
}

pub(super) fn same_direction(a: &MDirection, b: &MDirection) -> bool {
    // Compare the unit vectors to allow roundoff from encoding the same
    // angles in the two stores, including longitude wrap.
    const ROUNDOFF: f64 = 8.0 * f64::EPSILON;
    a.refer() == b.refer()
        && a.cosines()
            .into_iter()
            .zip(b.cosines())
            .map(|(left, right)| (left - right).powi(2))
            .sum::<f64>()
            <= ROUNDOFF * ROUNDOFF
}

fn ambiguous(name: &str) -> ObservationError {
    ObservationError::InvalidRequest {
        reason: format!("calibration table field {name} has an ambiguous MSv4 identity"),
    }
}

#[cfg(all(test, feature = "msv4", unix))]
mod tests {
    use super::*;
    use casa_core::{ArrayValue, PrimitiveType, RecordField, RecordValue, ScalarValue, Value};
    use casa_tables::{ColumnSchema, TableSchema};

    #[test]
    fn a_small_msv4_catalogue_does_not_admit_a_large_calibration_field_index() {
        let root = tempfile::tempdir().unwrap();
        let archive = include_bytes!("../../tests/fixtures/ms_trial/mssel_test_small.ps.zarr.tgz");
        tar::Archive::new(flate2::read::GzDecoder::new(&archive[..]))
            .unpack(root.path())
            .unwrap();
        let source = root.path().join("mssel_test_small.ps.zarr");
        let mut opening_peak = 0;
        let observation = super::super::Msv4Observation::open_granted(
            &source,
            &super::super::OpenOptions::default(),
            &mut |required| {
                opening_peak = opening_peak.max(required);
                Ok(())
            },
        )
        .unwrap();
        let budget = opening_peak.max(observation.held_bytes()) + (128 << 10);
        let selected = &observation.catalogue().fields[0];
        let (lon, lat) = selected.phase_centre.as_angles();
        let fields = Msv4FieldSelection::open(&source, budget).unwrap();

        let directory = root.path().join("caltable");
        std::fs::create_dir(&directory).unwrap();
        let path = directory.join("FIELD");
        let schema = TableSchema::new(vec![
            ColumnSchema::scalar("NAME", PrimitiveType::String),
            ColumnSchema::array_fixed("PHASE_DIR", PrimitiveType::Float64, vec![2, 1]),
        ])
        .unwrap();
        let mut table = Table::with_schema(schema);
        let record = |name: String| {
            RecordValue::new(vec![
                RecordField::new("NAME", Value::Scalar(ScalarValue::String(name))),
                RecordField::new(
                    "PHASE_DIR",
                    Value::Array(ArrayValue::Float64(
                        ndarray::arr2(&[[lon], [lat]]).into_dyn(),
                    )),
                ),
            ])
        };
        table.add_row(record(selected.name.clone())).unwrap();
        table.save(TableOptions::new(&path)).unwrap();
        assert_eq!(fields.select(&directory, "0").unwrap(), [0]);

        let unselected = budget / binding_scratch_bytes(1, "0") + 1;
        for _ in 0..unselected {
            table.add_row(record("unselected".to_owned())).unwrap();
        }
        table.save(TableOptions::new(&path)).unwrap();
        assert!(matches!(
            fields.select(&directory, "0"),
            Err(ObservationError::OverBudget {
                what: "MSv4 calibration FIELD binding",
                required,
                budget: admitted,
            }) if required > admitted && admitted == budget
        ));
    }
}

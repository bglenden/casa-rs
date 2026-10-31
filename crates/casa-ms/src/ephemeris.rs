// SPDX-License-Identifier: LGPL-3.0-or-later

use std::{
    alloc::Layout,
    collections::{BTreeMap, BTreeSet},
    fs,
    mem::size_of,
    path::{Path, PathBuf},
    sync::{Arc, atomic::AtomicUsize},
};

use casa_observation::FieldEphemeris;
use casa_tables::{Table, TableOptions};
use casa_types::measures::direction::{DirectionRef, MDirection};
use casa_types::measures::radial_velocity::RadialVelocityRef;
use casa_types::{ScalarValue, Value};
use thiserror::Error;

use crate::subtables::SubTable;
use crate::{MeasurementSet, MsError};

/// The bytes a phase-centre ephemeris may hold once read: what the run
/// admitted for it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct EphemerisBudget {
    available_bytes: usize,
}

impl EphemerisBudget {
    /// A budget of `available_bytes`.
    #[must_use]
    pub const fn new(available_bytes: usize) -> Self {
        Self { available_bytes }
    }

    /// The bytes available to the ephemeris's samples.
    #[must_use]
    pub const fn available_bytes(self) -> usize {
        self.available_bytes
    }
}

/// The astronomical unit, metres, in which ephemeris tables state `Rho`
/// (and `RadVel` per day).
pub(crate) const AU_METRES: f64 = 149_597_870_700.0;

#[derive(Debug, Clone, Copy)]
pub(crate) struct EvaluatedEphemerisSample {
    pub(crate) geocentric_position_metres: [f64; 3],
    pub(crate) position_reference: DirectionRef,
    pub(crate) radial_velocity_m_per_s: f64,
    pub(crate) velocity_reference: RadialVelocityRef,
}

/// Bytes an ephemeris holds a sample: its time, direction, distance and
/// radial velocity.
const SAMPLE_BYTES: usize = 5 * size_of::<f64>();

struct EphemerisSeriesPreflight {
    table: Table,
    row_count: usize,
    position_reference: DirectionRef,
    velocity_reference: RadialVelocityRef,
    retained_bytes: usize,
}

#[derive(Debug, Clone)]
struct FieldEphemerisBinding {
    field_id: usize,
    series: Arc<FieldEphemeris>,
}

#[derive(Debug, Clone)]
enum EphemerisKind {
    Named {
        target: Box<str>,
    },
    External {
        series: Arc<FieldEphemeris>,
    },
    TrackedField {
        fields: Box<[FieldEphemerisBinding]>,
    },
}

/// The moving target a phase centre names (`TRACKFIELD`, a body, or an
/// ephemeris table), its table rows read once within an admitted budget.
/// Evaluation interpolates only the epoch asked for, so no sample-sized
/// direction or velocity array is held.
#[derive(Debug, Clone)]
pub struct PhaseCentreEphemeris {
    kind: EphemerisKind,
}

impl PhaseCentreEphemeris {
    /// Bind a named CASA/casacore moving target, evaluated with the bound
    /// Measures provider.
    pub fn named(
        target: impl AsRef<str>,
        budget: EphemerisBudget,
    ) -> Result<Self, PhaseCentreEphemerisError> {
        let target = target.as_ref();
        if target.trim().is_empty() {
            return Err(PhaseCentreEphemerisError::InvalidTarget);
        }
        let retained_bytes = selected_ephemeris_allocation_bytes()?
            .checked_add(target.len())
            .ok_or(PhaseCentreEphemerisError::ByteOverflow)?;
        admit_reference_data(budget, retained_bytes)?;
        Ok(Self {
            kind: EphemerisKind::Named {
                target: target.into(),
            },
        })
    }

    /// Bind one external CASA ephemeris table.
    pub fn external(
        path: impl AsRef<Path>,
        budget: EphemerisBudget,
    ) -> Result<Self, PhaseCentreEphemerisError> {
        let preflight = EphemerisSeriesPreflight::open(path.as_ref())?;
        let retained_bytes = selected_ephemeris_allocation_bytes()?
            .checked_add(preflight.retained_bytes)
            .ok_or(PhaseCentreEphemerisError::ByteOverflow)?;
        admit_reference_data(budget, retained_bytes)?;
        let series = Arc::new(preflight.load()?);
        Ok(Self {
            kind: EphemerisKind::External { series },
        })
    }

    /// Track the selected fields' own ephemerides (`TRACKFIELD`), as the
    /// observation's `catalogue` holds them: an MSv2's `FIELD/EPHEM*.tab`
    /// tables, read when it opened, or an MSv4's `field_and_source`
    /// ephemerides. Nothing more is read, so nothing more is held.
    ///
    /// # Errors
    ///
    /// [`PhaseCentreEphemerisError::MissingTrackedField`] for a selected
    /// field that follows no ephemeris, and
    /// [`PhaseCentreEphemerisError::EmptyTrackedFieldSet`] for no field.
    pub fn tracked_fields(
        catalogue: &casa_observation::Catalogue,
        field_ids: impl IntoIterator<Item = usize>,
    ) -> Result<Self, PhaseCentreEphemerisError> {
        let mut bindings = Vec::new();
        for field_id in field_ids.into_iter().collect::<BTreeSet<_>>() {
            let series = catalogue
                .fields
                .get(field_id)
                .and_then(|field| field.ephemeris.clone())
                .ok_or(PhaseCentreEphemerisError::MissingTrackedField { field_id })?;
            bindings.push(FieldEphemerisBinding { field_id, series });
        }
        if bindings.is_empty() {
            return Err(PhaseCentreEphemerisError::EmptyTrackedFieldSet);
        }
        Ok(Self {
            kind: EphemerisKind::TrackedField {
                fields: bindings.into_boxed_slice(),
            },
        })
    }

    /// Whether the moving target is an external ephemeris table, rather
    /// than a body or source Measures names or the fields' own ephemerides.
    #[must_use]
    pub fn is_table(&self) -> bool {
        matches!(self.kind, EphemerisKind::External { .. })
    }

    /// The external ephemeris table's samples, when the moving target is
    /// one ([`Self::is_table`]).
    #[must_use]
    pub fn external_series(&self) -> Option<&Arc<FieldEphemeris>> {
        match &self.kind {
            EphemerisKind::External { series } => Some(series),
            EphemerisKind::Named { .. } | EphemerisKind::TrackedField { .. } => None,
        }
    }

    pub(crate) fn named_target(&self) -> Option<&str> {
        match &self.kind {
            EphemerisKind::Named { target } => Some(target),
            EphemerisKind::External { .. } | EphemerisKind::TrackedField { .. } => None,
        }
    }

    pub(crate) fn sample(
        &self,
        field_id: usize,
        mjd_days: f64,
    ) -> Result<EvaluatedEphemerisSample, PhaseCentreEphemerisError> {
        match &self.kind {
            EphemerisKind::Named { .. } => {
                Err(PhaseCentreEphemerisError::NamedSampleRequiresMeasures)
            }
            EphemerisKind::External { series } => evaluate(series, mjd_days),
            EphemerisKind::TrackedField { fields } => evaluate(
                fields
                    .binary_search_by_key(&field_id, |binding| binding.field_id)
                    .ok()
                    .map(|index| &fields[index].series)
                    .ok_or(PhaseCentreEphemerisError::MissingTrackedField { field_id })?,
                mjd_days,
            ),
        }
    }
}

/// `series` at `mjd_days` ([`FieldEphemeris::sample`]).
pub(crate) fn evaluate(
    series: &FieldEphemeris,
    mjd_days: f64,
) -> Result<EvaluatedEphemerisSample, PhaseCentreEphemerisError> {
    let sample =
        series
            .sample(mjd_days)
            .ok_or_else(|| PhaseCentreEphemerisError::OutsideCoverage {
                target: series.name().to_string(),
                mjd_days,
            })?;
    Ok(EvaluatedEphemerisSample {
        geocentric_position_metres: sample.position_m,
        position_reference: series.frame(),
        radial_velocity_m_per_s: sample.radial_velocity_m_s,
        velocity_reference: series.velocity_frame(),
    })
}

/// The ephemeris attached to the fields of the MeasurementSet at `root`
/// whose `EPHEMERIS_ID` is `ephemeris_id`: the one table
/// `FIELD/EPHEM<id>_*.tab`, as the boundary holds it.
///
/// # Errors
///
/// When no table or several match, or the table is not one an ephemeris
/// is read from ([`PhaseCentreEphemeris::external`]'s checks).
pub(crate) fn attached_ephemeris(
    root: &Path,
    ephemeris_id: i32,
) -> Result<FieldEphemeris, PhaseCentreEphemerisError> {
    EphemerisSeriesPreflight::open(&find_field_ephemeris(root, ephemeris_id)?)?.load()
}

/// The `EPHEMERIS_ID` each FIELD row of `measurement_set` follows: `None`
/// for none (an id below zero or no column) and for a row whose `NUM_POLY`
/// is above zero, whose direction casacore's `phaseDirMeas` takes from its
/// polynomial whatever its `EPHEMERIS_ID`.
///
/// The cells are read without filling the table's caches, so a retained
/// read of a locked MeasurementSet keeps its byte accounting.
pub(crate) fn field_ephemeris_ids(
    measurement_set: &MeasurementSet,
) -> Result<Vec<Option<i32>>, MsError> {
    let field = measurement_set.field()?;
    let table = field.table();
    let rows = (0..table.row_count()).collect::<Vec<_>>();
    if !table
        .schema()
        .is_some_and(|schema| schema.contains_column("EPHEMERIS_ID"))
    {
        return Ok(vec![None; rows.len()]);
    }
    let ids = table
        .column_accessor("EPHEMERIS_ID")?
        .scalar_cells_owned_for_rows(&rows)?;
    let polynomials = table
        .column_accessor("NUM_POLY")?
        .scalar_cells_owned_for_rows(&rows)?;
    Ok(ids
        .into_iter()
        .zip(polynomials)
        .map(|(id, polynomial)| match (id, polynomial) {
            (Some(ScalarValue::Int32(id)), Some(ScalarValue::Int32(0))) if id >= 0 => Some(id),
            _ => None,
        })
        .collect())
}

/// The direction casacore's `MSFieldColumns::extractDirMeas` gives a field
/// that follows `series` at `mjd_utc` (days): the body's geocentric
/// direction, shifted by the field's `PHASE_DIR` offset (`offset_rad`) by
/// its true angle, in the ephemeris's frame.
///
/// # Errors
///
/// A time the samples do not cover, or a non-finite offset direction.
pub(crate) fn reference_direction(
    series: &FieldEphemeris,
    mjd_utc: f64,
    offset_rad: [f64; 2],
) -> Result<MDirection, MsError> {
    let sample =
        evaluate(series, mjd_utc).map_err(|error| MsError::InvalidInput(error.to_string()))?;
    let body =
        MDirection::from_cosines(sample.geocentric_position_metres, sample.position_reference);
    Ok(casa_types::measures::ephemeris::offset_true_angle(
        &body, offset_rad,
    )?)
}

/// What a FIELD row's direction follows.
#[derive(Clone, Debug)]
pub(crate) enum FieldMotion {
    /// Its `PHASE_DIR` (or `PHASE_DIR` polynomial).
    Fixed,
    /// Its attached ephemeris, `PHASE_DIR` then the offset from the body.
    Ephemeris(Arc<FieldEphemeris>),
    /// An attached ephemeris that could not be read, and why: refused
    /// wherever the field's direction is asked for, not before.
    Unreadable(Arc<str>),
}

/// What each FIELD row of `measurement_set` follows
/// ([`field_ephemeris_ids`]), each table read once however many rows share
/// it ([`attached_ephemeris`]). A table that cannot be read leaves its
/// fields [`FieldMotion::Unreadable`]: casacore opens such a MeasurementSet,
/// and a run that never asks for those fields' directions images as before.
///
/// # Errors
///
/// FIELD cannot be read.
pub(crate) fn field_motions(
    measurement_set: &MeasurementSet,
) -> Result<Box<[FieldMotion]>, MsError> {
    let ids = field_ephemeris_ids(measurement_set)?;
    let mut read: BTreeMap<i32, FieldMotion> = BTreeMap::new();
    Ok(ids
        .iter()
        .map(|id| {
            let Some(id) = *id else {
                return FieldMotion::Fixed;
            };
            read.entry(id)
                .or_insert_with(|| {
                    let series = measurement_set
                        .path()
                        .ok_or(PhaseCentreEphemerisError::UnbackedMeasurementSet)
                        .and_then(|root| attached_ephemeris(root, id));
                    match series {
                        Ok(series) => FieldMotion::Ephemeris(Arc::new(series)),
                        Err(error) => FieldMotion::Unreadable(Arc::from(error.to_string())),
                    }
                })
                .clone()
        })
        .collect())
}

impl EphemerisSeriesPreflight {
    fn open(path: &Path) -> Result<Self, PhaseCentreEphemerisError> {
        let table = Table::open(TableOptions::new(path))?;
        for column in ["MJD", "RA", "DEC", "Rho", "RadVel"] {
            if !table
                .schema()
                .is_some_and(|schema| schema.contains_column(column))
            {
                return Err(PhaseCentreEphemerisError::MissingColumn {
                    column: column.to_string(),
                });
            }
        }
        let name_bytes = keyword_str(&table, "NAME")?.len();
        let position_reference_name = keyword_str(&table, "posrefsys")?;
        let position_reference = if position_reference_name.eq_ignore_ascii_case("ICRF/ICRS")
            || position_reference_name.eq_ignore_ascii_case("ICRS")
        {
            DirectionRef::ICRS
        } else if position_reference_name.eq_ignore_ascii_case("ICRF/J2000.0") {
            DirectionRef::J2000
        } else {
            return Err(PhaseCentreEphemerisError::UnsupportedPositionReference {
                reference: position_reference_name.to_string(),
            });
        };
        let observer_location = keyword_str(&table, "obsloc")?;
        if !observer_location.eq_ignore_ascii_case("GEOCENTRIC") {
            return Err(PhaseCentreEphemerisError::UnsupportedObserverLocation {
                location: observer_location.to_string(),
            });
        }
        let geo_distance_km = keyword_f64(&table, "GeoDist")?;
        let velocity_reference = if geo_distance_km.abs() * 1_000.0 > 1.0 {
            RadialVelocityRef::TOPO
        } else {
            RadialVelocityRef::GEO
        };
        let row_count = table.row_count();
        let retained_bytes = arc_allocation_bytes::<FieldEphemeris>()?
            .checked_add(name_bytes)
            .and_then(|bytes| {
                row_count
                    .checked_mul(SAMPLE_BYTES)
                    .and_then(|rows| bytes.checked_add(rows))
            })
            .ok_or(PhaseCentreEphemerisError::ByteOverflow)?;
        Ok(Self {
            table,
            row_count,
            position_reference,
            velocity_reference,
            retained_bytes,
        })
    }

    fn load(self) -> Result<FieldEphemeris, PhaseCentreEphemerisError> {
        let name = keyword_str(&self.table, "NAME")?.to_string();
        let mut times = Vec::with_capacity(self.row_count);
        let mut directions = Vec::with_capacity(self.row_count);
        let mut distances = Vec::with_capacity(self.row_count);
        let mut velocities = Vec::with_capacity(self.row_count);
        for row in 0..self.row_count {
            let mjd_days = scalar_f64(&self.table, row, "MJD")?;
            let longitude_rad = scalar_f64(&self.table, row, "RA")?.to_radians();
            let latitude_rad = scalar_f64(&self.table, row, "DEC")?.to_radians();
            let distance_metres = scalar_f64(&self.table, row, "Rho")? * AU_METRES;
            let radial_velocity_m_per_s =
                scalar_f64(&self.table, row, "RadVel")? * AU_METRES / 86_400.0;
            if !mjd_days.is_finite()
                || !longitude_rad.is_finite()
                || !latitude_rad.is_finite()
                || !distance_metres.is_finite()
                || distance_metres <= 0.0
                || !radial_velocity_m_per_s.is_finite()
                || times.last().is_some_and(|prior: &f64| *prior >= mjd_days)
            {
                return Err(PhaseCentreEphemerisError::InvalidRow { row });
            }
            times.push(mjd_days);
            directions.push([longitude_rad, latitude_rad]);
            distances.push(distance_metres);
            velocities.push(radial_velocity_m_per_s);
        }
        if times.len() < 2 {
            return Err(PhaseCentreEphemerisError::InsufficientRows);
        }
        Ok(FieldEphemeris::new(
            name,
            times,
            directions,
            self.position_reference,
            distances,
            (velocities, self.velocity_reference),
        )?)
    }
}

pub(crate) fn find_field_ephemeris(
    root: &Path,
    ephemeris_id: i32,
) -> Result<PathBuf, PhaseCentreEphemerisError> {
    let prefix = format!("EPHEM{ephemeris_id}_");
    let mut matches = fs::read_dir(root.join("FIELD"))?
        .filter_map(Result::ok)
        .map(|entry| entry.path())
        .filter(|path| {
            path.file_name()
                .and_then(|name| name.to_str())
                .is_some_and(|name| name.starts_with(&prefix) && name.ends_with(".tab"))
        })
        .collect::<Vec<_>>();
    matches.sort();
    match matches.as_slice() {
        [path] => Ok(path.clone()),
        [] => Err(PhaseCentreEphemerisError::MissingEphemerisTable { ephemeris_id }),
        _ => Err(PhaseCentreEphemerisError::AmbiguousEphemerisTable { ephemeris_id }),
    }
}

fn keyword_str<'a>(table: &'a Table, name: &str) -> Result<&'a str, PhaseCentreEphemerisError> {
    match table.keywords().get(name) {
        Some(Value::Scalar(ScalarValue::String(value))) => Ok(value),
        _ => Err(PhaseCentreEphemerisError::InvalidKeyword {
            keyword: name.to_string(),
        }),
    }
}

fn arc_allocation_bytes<T>() -> Result<usize, PhaseCentreEphemerisError> {
    let header =
        Layout::array::<AtomicUsize>(2).map_err(|_| PhaseCentreEphemerisError::ByteOverflow)?;
    let (allocation, _) = header
        .extend(Layout::new::<T>())
        .map_err(|_| PhaseCentreEphemerisError::ByteOverflow)?;
    Ok(allocation.pad_to_align().size())
}

fn selected_ephemeris_allocation_bytes() -> Result<usize, PhaseCentreEphemerisError> {
    arc_allocation_bytes::<PhaseCentreEphemeris>()
}

fn admit_reference_data(
    budget: EphemerisBudget,
    required_bytes: usize,
) -> Result<(), PhaseCentreEphemerisError> {
    if required_bytes > budget.available_bytes() {
        return Err(PhaseCentreEphemerisError::InsufficientReferenceDataBudget {
            required_bytes,
            available_bytes: budget.available_bytes(),
        });
    }
    Ok(())
}

fn keyword_f64(table: &Table, name: &str) -> Result<f64, PhaseCentreEphemerisError> {
    match table.keywords().get(name) {
        Some(Value::Scalar(ScalarValue::Float64(value))) if value.is_finite() => Ok(*value),
        _ => Err(PhaseCentreEphemerisError::InvalidKeyword {
            keyword: name.to_string(),
        }),
    }
}

fn scalar_f64(table: &Table, row: usize, column: &str) -> Result<f64, PhaseCentreEphemerisError> {
    match table.cell_accessor(row, column)?.scalar()? {
        ScalarValue::Float64(value) => Ok(*value),
        _ => Err(PhaseCentreEphemerisError::InvalidColumnType {
            column: column.to_string(),
        }),
    }
}

/// Failure to bind or evaluate one immutable moving-source reference.
#[derive(Debug, Error)]
pub enum PhaseCentreEphemerisError {
    /// MeasurementSet or table storage failed.
    #[error(transparent)]
    Storage(#[from] MsError),
    /// Generic casacore table access failed.
    #[error(transparent)]
    Table(#[from] casa_tables::TableError),
    /// Ephemeris directory inspection failed.
    #[error(transparent)]
    Io(#[from] std::io::Error),
    /// A named target was empty.
    #[error("moving-source target must be non-empty")]
    InvalidTarget,
    /// TRACKFIELD was requested from an in-memory MeasurementSet.
    #[error("TRACKFIELD requires a filesystem-backed MeasurementSet")]
    UnbackedMeasurementSet,
    /// No selected FIELD rows were supplied.
    #[error("TRACKFIELD requires at least one selected FIELD row")]
    EmptyTrackedFieldSet,
    /// A selected FIELD does not bind an ephemeris.
    #[error("selected FIELD_ID {field_id} has no attached ephemeris")]
    MissingTrackedField {
        /// Selected FIELD row without an attached ephemeris.
        field_id: usize,
    },
    /// No attached ephemeris table matched EPHEMERIS_ID.
    #[error("FIELD EPHEMERIS_ID {ephemeris_id} has no matching EPHEM table")]
    MissingEphemerisTable {
        /// FIELD EPHEMERIS_ID without a matching table.
        ephemeris_id: i32,
    },
    /// Multiple attached tables matched one EPHEMERIS_ID.
    #[error("FIELD EPHEMERIS_ID {ephemeris_id} matches multiple EPHEM tables")]
    AmbiguousEphemerisTable {
        /// FIELD EPHEMERIS_ID matching more than one table.
        ephemeris_id: i32,
    },
    /// A required standard ephemeris column was absent.
    #[error("ephemeris table is missing required column {column}")]
    MissingColumn {
        /// Required column name.
        column: String,
    },
    /// A required ephemeris keyword was absent or invalid.
    #[error("ephemeris table keyword {keyword} is missing or invalid")]
    InvalidKeyword {
        /// Required keyword name.
        keyword: String,
    },
    /// The persisted ephemeris position reference is unsupported.
    #[error("ephemeris position reference {reference:?} is unsupported")]
    UnsupportedPositionReference {
        /// Persisted position reference.
        reference: String,
    },
    /// The table is not referred to the Earth centre.
    #[error("ephemeris observer location {location:?} is unsupported")]
    UnsupportedObserverLocation {
        /// Persisted observer location.
        location: String,
    },
    /// A standard ephemeris column had the wrong physical type.
    #[error("ephemeris column {column} is not Float64")]
    InvalidColumnType {
        /// Column with an incompatible physical type.
        column: String,
    },
    /// A persisted ephemeris row was invalid or unordered.
    #[error("ephemeris row {row} is invalid or not strictly time ordered")]
    InvalidRow {
        /// Invalid persisted row index.
        row: usize,
    },
    /// Interpolation requires at least two ephemeris rows.
    #[error("ephemeris table requires at least two ordered rows")]
    InsufficientRows,
    /// The requested epoch is outside the immutable ephemeris interval.
    #[error("no valid ephemeris entry for {target} at MJD {mjd_days}")]
    OutsideCoverage {
        /// Ephemeris target name.
        target: String,
        /// Requested UTC epoch in MJD days.
        mjd_days: f64,
    },
    /// Named targets are evaluated by the bound Measures provider.
    #[error("named moving-source evaluation requires the bound Measures provider")]
    NamedSampleRequiresMeasures,
    /// Immutable reference data exceeds its selected-content-derived ceiling.
    #[error(
        "ephemeris reference data requires {required_bytes} retained bytes but the explicit budget has {available_bytes} bytes"
    )]
    InsufficientReferenceDataBudget {
        /// Exact retained allocation graph required by the ephemeris binding.
        required_bytes: usize,
        /// Reference-data bytes authorized by the selected-content owner.
        available_bytes: usize,
    },
    /// Retained ephemeris residency overflowed the host byte domain.
    #[error("ephemeris retained byte count overflowed")]
    ByteOverflow,
    /// The samples are not an ephemeris the boundary holds.
    #[error(transparent)]
    Boundary(#[from] casa_observation::ObservationError),
}

#[cfg(test)]
mod tests {
    use super::*;
    use casa_tables::{ColumnSchema, TableSchema};
    use casa_types::{PrimitiveType, RecordField, RecordValue};

    const TARGET: &str = "T41_TEST_TARGET";

    fn reference_budget(bytes: usize) -> EphemerisBudget {
        EphemerisBudget::new(bytes)
    }

    fn scalar_field(name: &str, value: f64) -> RecordField {
        RecordField::new(name, Value::Scalar(ScalarValue::Float64(value)))
    }

    fn save_ephemeris_table(path: &Path, rows: &[[f64; 5]]) {
        let schema = TableSchema::new(
            ["MJD", "RA", "DEC", "Rho", "RadVel"]
                .into_iter()
                .map(|name| ColumnSchema::scalar(name, PrimitiveType::Float64))
                .collect(),
        )
        .expect("valid ephemeris schema");
        let mut table = Table::with_schema(schema);
        for [mjd, ra, dec, rho, radial_velocity] in rows {
            table
                .add_row(RecordValue::new(vec![
                    scalar_field("MJD", *mjd),
                    scalar_field("RA", *ra),
                    scalar_field("DEC", *dec),
                    scalar_field("Rho", *rho),
                    scalar_field("RadVel", *radial_velocity),
                ]))
                .expect("valid ephemeris row");
        }
        for (name, value) in [
            ("NAME", TARGET),
            ("posrefsys", "ICRF/ICRS"),
            ("obsloc", "GEOCENTRIC"),
        ] {
            table.keywords_mut().push(RecordField::new(
                name,
                Value::Scalar(ScalarValue::String(value.to_string())),
            ));
        }
        table.keywords_mut().push(RecordField::new(
            "GeoDist",
            Value::Scalar(ScalarValue::Float64(0.0)),
        ));
        table
            .save(TableOptions::new(path))
            .expect("persist ephemeris table");
    }

    fn sample_bits(sample: EvaluatedEphemerisSample) -> ([u64; 3], u64) {
        (
            sample.geocentric_position_metres.map(f64::to_bits),
            sample.radial_velocity_m_per_s.to_bits(),
        )
    }

    #[test]
    fn t41_external_ephemeris_binding_is_an_immutable_content_snapshot() {
        let directory = tempfile::tempdir().expect("temporary ephemeris table");
        let path = directory.path().join("external_ephemeris.tab");
        save_ephemeris_table(
            &path,
            &[
                [60_000.0, 10.0, -30.0, 2.0, 0.001],
                [60_001.0, 11.0, -29.0, 2.1, 0.002],
                [60_002.0, 12.0, -28.0, 2.2, 0.003],
            ],
        );

        let budget = reference_budget(1 << 20);
        let bound = PhaseCentreEphemeris::external(&path, budget).expect("bind snapshot");
        let original_sample = sample_bits(bound.sample(0, 60_001.0).expect("sample snapshot"));

        let mut backing = Table::open(TableOptions::new(&path)).expect("reopen backing table");
        backing
            .cell_accessor_mut(1, "RA")
            .expect("mutable RA cell")
            .set(Value::Scalar(ScalarValue::Float64(111.0)))
            .expect("mutate persisted RA");
        backing
            .prepare_write()
            .save_selected_columns(&["RA"])
            .expect("persist mutated RA");

        assert_eq!(
            sample_bits(bound.sample(0, 60_001.0).expect("resample snapshot")),
            original_sample,
            "the bound series must not observe later backing-table mutation"
        );

        let rebound = PhaseCentreEphemeris::external(&path, budget).expect("bind changed table");
        assert_ne!(
            sample_bits(rebound.sample(0, 60_001.0).expect("sample changed table")),
            original_sample,
            "a new binding must observe changed scientific content"
        );
    }

    #[test]
    fn t41_oversized_ephemeris_fails_before_row_allocation_or_read() {
        let directory = tempfile::tempdir().expect("temporary oversized ephemeris table");
        let path = directory.path().join("oversized.tab");
        save_ephemeris_table(
            &path,
            &[
                [60_000.0, f64::NAN, -30.0, 2.0, 0.001],
                [60_001.0, 11.0, -29.0, 2.1, 0.002],
            ],
        );
        let preflight = EphemerisSeriesPreflight::open(&path).expect("preflight table metadata");
        let required_bytes = selected_ephemeris_allocation_bytes()
            .and_then(|bytes| {
                bytes
                    .checked_add(preflight.retained_bytes)
                    .ok_or(PhaseCentreEphemerisError::ByteOverflow)
            })
            .expect("finite fixture reference-data charge");
        drop(preflight);

        assert!(matches!(
            PhaseCentreEphemeris::external(
                &path,
                reference_budget(required_bytes - 1),
            ),
            Err(
                PhaseCentreEphemerisError::InsufficientReferenceDataBudget {
                    required_bytes: required,
                    available_bytes,
                }
            ) if required == required_bytes && available_bytes == required_bytes - 1
        ));
        assert!(matches!(
            PhaseCentreEphemeris::external(&path, reference_budget(required_bytes)),
            Err(PhaseCentreEphemerisError::InvalidRow { row: 0 })
        ));
    }

    #[test]
    fn t41_invalid_and_unordered_persisted_rows_fail_closed() {
        let cases = [
            (
                "non_finite",
                vec![
                    [60_000.0, 10.0, -30.0, 2.0, 0.001],
                    [60_001.0, f64::NAN, -29.0, 2.1, 0.002],
                ],
            ),
            (
                "unordered",
                vec![
                    [60_001.0, 10.0, -30.0, 2.0, 0.001],
                    [60_000.0, 11.0, -29.0, 2.1, 0.002],
                ],
            ),
        ];

        for (case, rows) in cases {
            let directory = tempfile::tempdir().expect("temporary invalid ephemeris table");
            let path = directory.path().join(format!("{case}.tab"));
            save_ephemeris_table(&path, &rows);
            assert!(matches!(
                PhaseCentreEphemeris::external(&path, reference_budget(1 << 20)),
                Err(PhaseCentreEphemerisError::InvalidRow { row: 1 })
            ));
        }
    }

    #[test]
    fn t41_out_of_coverage_epochs_fail_closed_with_exact_error() {
        let directory = tempfile::tempdir().expect("temporary ephemeris table");
        let path = directory.path().join("coverage.tab");
        save_ephemeris_table(
            &path,
            &[
                [60_000.0, 10.0, -30.0, 2.0, 0.001],
                [60_001.0, 11.0, -29.0, 2.1, 0.002],
            ],
        );
        let bound = PhaseCentreEphemeris::external(&path, reference_budget(1 << 20))
            .expect("bind ephemeris");

        for epoch in [59_999.999, 60_001.001] {
            match bound.sample(0, epoch) {
                Err(PhaseCentreEphemerisError::OutsideCoverage { target, mjd_days }) => {
                    assert_eq!(target, TARGET);
                    assert_eq!(mjd_days.to_bits(), epoch.to_bits());
                }
                other => panic!("expected exact OutsideCoverage error, got {other:?}"),
            }
        }
    }
}

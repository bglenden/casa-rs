// SPDX-License-Identifier: LGPL-3.0-or-later

use std::{
    alloc::Layout,
    collections::{BTreeMap, BTreeSet},
    fs,
    mem::size_of,
    path::{Path, PathBuf},
    sync::{Arc, atomic::AtomicUsize},
};

use casa_imaging_model::LogicalIdentity;
use casa_tables::{Table, TableOptions};
use casa_types::measures::direction::DirectionRef;
use casa_types::measures::radial_velocity::RadialVelocityRef;
use casa_types::{ScalarValue, Value};
use sha2::{Digest, Sha256};
use thiserror::Error;

use crate::{MeasurementSet, MsError, SelectedObservationReferenceDataBudget};

const EPHEMERIS_IDENTITY_DOMAIN: &[u8] = b"casa-rs-selected-ephemeris-v1";
const AU_METRES: f64 = 149_597_870_700.0;

#[derive(Debug, Clone, Copy)]
pub(crate) struct EvaluatedEphemerisSample {
    pub(crate) geocentric_position_metres: [f64; 3],
    pub(crate) position_reference: DirectionRef,
    pub(crate) radial_velocity_m_per_s: f64,
    pub(crate) velocity_reference: RadialVelocityRef,
}

#[derive(Debug, Clone)]
struct EphemerisRow {
    mjd_days: f64,
    geocentric_position_metres: [f64; 3],
    radial_velocity_m_per_s: f64,
}

#[derive(Debug)]
struct EphemerisSeries {
    identity: LogicalIdentity,
    name: Box<str>,
    position_reference: DirectionRef,
    velocity_reference: RadialVelocityRef,
    rows: Box<[EphemerisRow]>,
}

struct EphemerisSeriesPreflight {
    table: Table,
    row_count: usize,
    position_reference: DirectionRef,
    velocity_reference: RadialVelocityRef,
    geo_distance_km: f64,
    retained_bytes: usize,
}

#[derive(Debug, Clone)]
struct FieldEphemerisBinding {
    field_id: usize,
    series: Arc<EphemerisSeries>,
}

#[derive(Debug, Clone)]
enum EphemerisKind {
    Named {
        target: Box<str>,
    },
    External {
        series: Arc<EphemerisSeries>,
    },
    TrackedField {
        fields: Box<[FieldEphemerisBinding]>,
    },
}

/// Immutable, content-identified moving-source data bound to selected traversal.
///
/// Table rows are loaded once into bounded retained storage. Evaluation later
/// interpolates only the requested row epoch, so no sample-sized direction or
/// velocity array is compiled or retained.
#[derive(Debug, Clone)]
pub struct SelectedObservationEphemeris {
    identity: LogicalIdentity,
    kind: EphemerisKind,
    attached_fields: Box<[FieldEphemerisBinding]>,
    retained_bytes: usize,
}

impl PartialEq for SelectedObservationEphemeris {
    fn eq(&self, other: &Self) -> bool {
        self.identity == other.identity
    }
}

impl Eq for SelectedObservationEphemeris {}

impl SelectedObservationEphemeris {
    /// Bind a named CASA/casacore moving target to the immutable Measures snapshot.
    pub fn named(
        target: impl AsRef<str>,
        measures_identity: LogicalIdentity,
        budget: SelectedObservationReferenceDataBudget,
    ) -> Result<Self, SelectedObservationEphemerisError> {
        let target = target.as_ref();
        if target.trim().is_empty() {
            return Err(SelectedObservationEphemerisError::InvalidTarget);
        }
        let retained_bytes = selected_ephemeris_allocation_bytes()?
            .checked_add(target.len())
            .ok_or(SelectedObservationEphemerisError::ByteOverflow)?;
        admit_reference_data(budget, retained_bytes)?;
        Ok(Self {
            identity: measures_identity,
            kind: EphemerisKind::Named {
                target: target.into(),
            },
            attached_fields: Box::new([]),
            retained_bytes,
        })
    }

    /// Bind one external CASA ephemeris table by exact scientific content.
    pub fn external(
        path: impl AsRef<Path>,
        budget: SelectedObservationReferenceDataBudget,
    ) -> Result<Self, SelectedObservationEphemerisError> {
        let preflight = EphemerisSeriesPreflight::open(path.as_ref())?;
        let retained_bytes = selected_ephemeris_allocation_bytes()?
            .checked_add(preflight.retained_bytes)
            .ok_or(SelectedObservationEphemerisError::ByteOverflow)?;
        admit_reference_data(budget, retained_bytes)?;
        let series = Arc::new(preflight.load()?);
        Ok(Self {
            identity: series.identity,
            kind: EphemerisKind::External { series },
            attached_fields: Box::new([]),
            retained_bytes,
        })
    }

    /// Bind every selected FIELD-linked ephemeris by exact table content.
    pub fn tracked_fields(
        measurement_set: &MeasurementSet,
        field_ids: impl IntoIterator<Item = usize>,
        budget: SelectedObservationReferenceDataBudget,
    ) -> Result<Self, SelectedObservationEphemerisError> {
        let field = measurement_set.field()?;
        let root = measurement_set
            .path()
            .ok_or(SelectedObservationEphemerisError::UnbackedMeasurementSet)?;
        let mut requested = Vec::new();
        let mut selected = BTreeSet::new();
        for field_id in field_ids {
            if !selected.insert(field_id) {
                continue;
            }
            let ephemeris_id = field
                .ephemeris_id(field_id)?
                .filter(|value| *value >= 0)
                .ok_or(SelectedObservationEphemerisError::MissingTrackedField { field_id })?;
            let path = find_field_ephemeris(root, ephemeris_id)?;
            requested.push((field_id, path));
        }
        if requested.is_empty() {
            return Err(SelectedObservationEphemerisError::EmptyTrackedFieldSet);
        }

        let mut preflights = BTreeMap::new();
        for (_, path) in &requested {
            if !preflights.contains_key(path) {
                preflights.insert(path.clone(), EphemerisSeriesPreflight::open(path)?);
            }
        }
        let field_binding_bytes = requested
            .len()
            .checked_mul(size_of::<FieldEphemerisBinding>())
            .ok_or(SelectedObservationEphemerisError::ByteOverflow)?;
        let retained_bytes = preflights
            .values()
            .try_fold(selected_ephemeris_allocation_bytes()?, |bytes, series| {
                bytes.checked_add(series.retained_bytes)
            })
            .and_then(|bytes| bytes.checked_add(field_binding_bytes))
            .ok_or(SelectedObservationEphemerisError::ByteOverflow)?;
        admit_reference_data(budget, retained_bytes)?;

        let unique = preflights
            .into_iter()
            .map(|(path, preflight)| preflight.load().map(|series| (path, Arc::new(series))))
            .collect::<Result<BTreeMap<_, _>, _>>()?;
        let mut bindings = requested
            .into_iter()
            .map(|(field_id, path)| FieldEphemerisBinding {
                field_id,
                series: Arc::clone(
                    unique
                        .get(&path)
                        .expect("every requested ephemeris path was preflighted"),
                ),
            })
            .collect::<Vec<_>>();
        bindings.sort_by_key(|binding| binding.field_id);
        let identity = tracked_identity(&bindings);
        Ok(Self {
            identity,
            kind: EphemerisKind::TrackedField {
                fields: bindings.into_boxed_slice(),
            },
            attached_fields: Box::new([]),
            retained_bytes,
        })
    }

    /// Bind the MS FIELD-attached ephemerides that define stored UVW phase centres.
    pub fn with_attached_fields(
        mut self,
        attached: Self,
        budget: SelectedObservationReferenceDataBudget,
    ) -> Result<Self, SelectedObservationEphemerisError> {
        let EphemerisKind::TrackedField { fields } = attached.kind else {
            return Err(SelectedObservationEphemerisError::AttachedFieldsRequired);
        };
        let mut hasher = Sha256::new();
        hasher.update(EPHEMERIS_IDENTITY_DOMAIN);
        hasher.update(b"tracking-and-attached-fields");
        hasher.update(self.identity.as_bytes());
        hasher.update(attached.identity.as_bytes());
        self.identity = LogicalIdentity::from_sha256(hasher.finalize().into());
        let shared_owner_bytes = selected_ephemeris_allocation_bytes()?;
        self.retained_bytes = self
            .retained_bytes
            .checked_add(attached.retained_bytes)
            .and_then(|bytes| bytes.checked_sub(shared_owner_bytes))
            .ok_or(SelectedObservationEphemerisError::ByteOverflow)?;
        admit_reference_data(budget, self.retained_bytes)?;
        self.attached_fields = fields;
        Ok(self)
    }

    /// Return the immutable reference identity committed by Compiled Geometry.
    #[must_use]
    pub const fn identity(&self) -> LogicalIdentity {
        self.identity
    }

    pub(crate) const fn retained_bytes(&self) -> usize {
        self.retained_bytes
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
    ) -> Result<EvaluatedEphemerisSample, SelectedObservationEphemerisError> {
        match &self.kind {
            EphemerisKind::Named { .. } => {
                Err(SelectedObservationEphemerisError::NamedSampleRequiresMeasures)
            }
            EphemerisKind::External { series } => series.sample(mjd_days),
            EphemerisKind::TrackedField { fields } => fields
                .binary_search_by_key(&field_id, |binding| binding.field_id)
                .ok()
                .map(|index| &fields[index].series)
                .ok_or(SelectedObservationEphemerisError::MissingTrackedField { field_id })?
                .sample(mjd_days),
        }
    }

    pub(crate) fn attached_field_sample(
        &self,
        field_id: usize,
        mjd_days: f64,
    ) -> Result<Option<EvaluatedEphemerisSample>, SelectedObservationEphemerisError> {
        let fields = if self.attached_fields.is_empty() {
            match &self.kind {
                EphemerisKind::TrackedField { fields } => fields.as_ref(),
                EphemerisKind::Named { .. } | EphemerisKind::External { .. } => return Ok(None),
            }
        } else {
            self.attached_fields.as_ref()
        };
        fields
            .binary_search_by_key(&field_id, |binding| binding.field_id)
            .ok()
            .map(|index| fields[index].series.sample(mjd_days))
            .transpose()
    }
}

impl EphemerisSeriesPreflight {
    fn open(path: &Path) -> Result<Self, SelectedObservationEphemerisError> {
        let table = Table::open(TableOptions::new(path))?;
        for column in ["MJD", "RA", "DEC", "Rho", "RadVel"] {
            if !table
                .schema()
                .is_some_and(|schema| schema.contains_column(column))
            {
                return Err(SelectedObservationEphemerisError::MissingColumn {
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
            return Err(
                SelectedObservationEphemerisError::UnsupportedPositionReference {
                    reference: position_reference_name.to_string(),
                },
            );
        };
        let observer_location = keyword_str(&table, "obsloc")?;
        if !observer_location.eq_ignore_ascii_case("GEOCENTRIC") {
            return Err(
                SelectedObservationEphemerisError::UnsupportedObserverLocation {
                    location: observer_location.to_string(),
                },
            );
        }
        let geo_distance_km = keyword_f64(&table, "GeoDist")?;
        let velocity_reference = if geo_distance_km.abs() * 1_000.0 > 1.0 {
            RadialVelocityRef::TOPO
        } else {
            RadialVelocityRef::GEO
        };
        let row_count = table.row_count();
        let retained_bytes = arc_allocation_bytes::<EphemerisSeries>()?
            .checked_add(name_bytes)
            .and_then(|bytes| {
                row_count
                    .checked_mul(size_of::<EphemerisRow>())
                    .and_then(|rows| bytes.checked_add(rows))
            })
            .ok_or(SelectedObservationEphemerisError::ByteOverflow)?;
        Ok(Self {
            table,
            row_count,
            position_reference,
            velocity_reference,
            geo_distance_km,
            retained_bytes,
        })
    }

    fn load(self) -> Result<EphemerisSeries, SelectedObservationEphemerisError> {
        let name: Box<str> = keyword_str(&self.table, "NAME")?.into();
        let mut rows = Vec::with_capacity(self.row_count);
        for row in 0..self.row_count {
            let mjd_days = scalar_f64(&self.table, row, "MJD")?;
            let longitude_rad = scalar_f64(&self.table, row, "RA")?.to_radians();
            let latitude_rad = scalar_f64(&self.table, row, "DEC")?.to_radians();
            let distance_metres = scalar_f64(&self.table, row, "Rho")? * AU_METRES;
            let (sin_longitude, cos_longitude) = longitude_rad.sin_cos();
            let (sin_latitude, cos_latitude) = latitude_rad.sin_cos();
            let geocentric_position_metres = [
                distance_metres * cos_latitude * cos_longitude,
                distance_metres * cos_latitude * sin_longitude,
                distance_metres * sin_latitude,
            ];
            let radial_velocity_m_per_s =
                scalar_f64(&self.table, row, "RadVel")? * AU_METRES / 86_400.0;
            if !mjd_days.is_finite()
                || !longitude_rad.is_finite()
                || !latitude_rad.is_finite()
                || geocentric_position_metres
                    .iter()
                    .any(|component| !component.is_finite())
                || distance_metres <= 0.0
                || !radial_velocity_m_per_s.is_finite()
                || rows
                    .last()
                    .is_some_and(|prior: &EphemerisRow| prior.mjd_days >= mjd_days)
            {
                return Err(SelectedObservationEphemerisError::InvalidRow { row });
            }
            rows.push(EphemerisRow {
                mjd_days,
                geocentric_position_metres,
                radial_velocity_m_per_s,
            });
        }
        if rows.len() < 2 {
            return Err(SelectedObservationEphemerisError::InsufficientRows);
        }
        let identity = series_identity(
            &name,
            self.position_reference,
            self.velocity_reference,
            self.geo_distance_km,
            &rows,
        );
        Ok(EphemerisSeries {
            identity,
            name,
            position_reference: self.position_reference,
            velocity_reference: self.velocity_reference,
            rows: rows.into_boxed_slice(),
        })
    }
}

impl EphemerisSeries {
    fn sample(
        &self,
        mjd_days: f64,
    ) -> Result<EvaluatedEphemerisSample, SelectedObservationEphemerisError> {
        if !mjd_days.is_finite()
            || mjd_days < self.rows[0].mjd_days
            || mjd_days > self.rows[self.rows.len() - 1].mjd_days
        {
            return Err(SelectedObservationEphemerisError::OutsideCoverage {
                target: self.name.to_string(),
                mjd_days,
            });
        }
        let upper = self.rows.partition_point(|row| row.mjd_days < mjd_days);
        if upper < self.rows.len() && self.rows[upper].mjd_days == mjd_days {
            return Ok(EvaluatedEphemerisSample {
                geocentric_position_metres: self.rows[upper].geocentric_position_metres,
                position_reference: self.position_reference,
                radial_velocity_m_per_s: self.rows[upper].radial_velocity_m_per_s,
                velocity_reference: self.velocity_reference,
            });
        }
        let lower = upper.saturating_sub(1);
        let before = &self.rows[lower];
        let after = &self.rows[upper];
        let factor = (mjd_days - before.mjd_days) / (after.mjd_days - before.mjd_days);
        Ok(EvaluatedEphemerisSample {
            geocentric_position_metres: std::array::from_fn(|axis| {
                interpolate(
                    before.geocentric_position_metres[axis],
                    after.geocentric_position_metres[axis],
                    factor,
                )
            }),
            position_reference: self.position_reference,
            radial_velocity_m_per_s: interpolate(
                before.radial_velocity_m_per_s,
                after.radial_velocity_m_per_s,
                factor,
            ),
            velocity_reference: self.velocity_reference,
        })
    }
}

fn find_field_ephemeris(
    root: &Path,
    ephemeris_id: i32,
) -> Result<PathBuf, SelectedObservationEphemerisError> {
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
        [] => Err(SelectedObservationEphemerisError::MissingEphemerisTable { ephemeris_id }),
        _ => Err(SelectedObservationEphemerisError::AmbiguousEphemerisTable { ephemeris_id }),
    }
}

fn keyword_str<'a>(
    table: &'a Table,
    name: &str,
) -> Result<&'a str, SelectedObservationEphemerisError> {
    match table.keywords().get(name) {
        Some(Value::Scalar(ScalarValue::String(value))) => Ok(value),
        _ => Err(SelectedObservationEphemerisError::InvalidKeyword {
            keyword: name.to_string(),
        }),
    }
}

fn arc_allocation_bytes<T>() -> Result<usize, SelectedObservationEphemerisError> {
    let header = Layout::array::<AtomicUsize>(2)
        .map_err(|_| SelectedObservationEphemerisError::ByteOverflow)?;
    let (allocation, _) = header
        .extend(Layout::new::<T>())
        .map_err(|_| SelectedObservationEphemerisError::ByteOverflow)?;
    Ok(allocation.pad_to_align().size())
}

fn selected_ephemeris_allocation_bytes() -> Result<usize, SelectedObservationEphemerisError> {
    arc_allocation_bytes::<SelectedObservationEphemeris>()
}

fn admit_reference_data(
    budget: SelectedObservationReferenceDataBudget,
    required_bytes: usize,
) -> Result<(), SelectedObservationEphemerisError> {
    if required_bytes > budget.available_bytes() {
        return Err(
            SelectedObservationEphemerisError::InsufficientReferenceDataBudget {
                required_bytes,
                available_bytes: budget.available_bytes(),
            },
        );
    }
    Ok(())
}

fn keyword_f64(table: &Table, name: &str) -> Result<f64, SelectedObservationEphemerisError> {
    match table.keywords().get(name) {
        Some(Value::Scalar(ScalarValue::Float64(value))) if value.is_finite() => Ok(*value),
        _ => Err(SelectedObservationEphemerisError::InvalidKeyword {
            keyword: name.to_string(),
        }),
    }
}

fn scalar_f64(
    table: &Table,
    row: usize,
    column: &str,
) -> Result<f64, SelectedObservationEphemerisError> {
    match table.cell_accessor(row, column)?.scalar()? {
        ScalarValue::Float64(value) => Ok(*value),
        _ => Err(SelectedObservationEphemerisError::InvalidColumnType {
            column: column.to_string(),
        }),
    }
}

fn series_identity(
    name: &str,
    position_reference: DirectionRef,
    velocity_reference: RadialVelocityRef,
    geo_distance_km: f64,
    rows: &[EphemerisRow],
) -> LogicalIdentity {
    let mut hasher = Sha256::new();
    hasher.update(EPHEMERIS_IDENTITY_DOMAIN);
    hasher.update(b"GEOCENTRIC");
    hasher.update(position_reference.as_str().as_bytes());
    hasher.update(velocity_reference.as_str().as_bytes());
    hasher.update(geo_distance_km.to_bits().to_le_bytes());
    hasher.update((name.len() as u64).to_le_bytes());
    hasher.update(name.as_bytes());
    hasher.update((rows.len() as u64).to_le_bytes());
    for row in rows {
        for value in std::iter::once(row.mjd_days)
            .chain(row.geocentric_position_metres)
            .chain(std::iter::once(row.radial_velocity_m_per_s))
        {
            hasher.update(value.to_bits().to_le_bytes());
        }
    }
    LogicalIdentity::from_sha256(hasher.finalize().into())
}

fn tracked_identity(fields: &[FieldEphemerisBinding]) -> LogicalIdentity {
    let mut hasher = Sha256::new();
    hasher.update(EPHEMERIS_IDENTITY_DOMAIN);
    hasher.update(b"tracked-fields");
    hasher.update((fields.len() as u64).to_le_bytes());
    for field in fields {
        hasher.update((field.field_id as u64).to_le_bytes());
        hasher.update(field.series.identity.as_bytes());
    }
    LogicalIdentity::from_sha256(hasher.finalize().into())
}

fn interpolate(before: f64, after: f64, factor: f64) -> f64 {
    before + factor * (after - before)
}

/// Failure to bind or evaluate one immutable moving-source reference.
#[derive(Debug, Error)]
pub enum SelectedObservationEphemerisError {
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
    /// FIELD phase-centre attachment requires a TRACKFIELD binding.
    #[error("attached FIELD ephemerides require a TRACKFIELD binding")]
    AttachedFieldsRequired,
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
}

#[cfg(test)]
mod tests {
    use super::*;
    use casa_tables::{ColumnSchema, TableSchema};
    use casa_types::{PrimitiveType, RecordField, RecordValue};

    const TARGET: &str = "T41_TEST_TARGET";

    fn reference_budget(bytes: usize) -> SelectedObservationReferenceDataBudget {
        crate::SelectedObservationContentBudget::new(bytes, 1, 1).reference_data_budget()
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
        let bound = SelectedObservationEphemeris::external(&path, budget).expect("bind snapshot");
        let original_identity = bound.identity();
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

        assert_eq!(bound.identity(), original_identity);
        assert_eq!(
            sample_bits(bound.sample(0, 60_001.0).expect("resample snapshot")),
            original_sample,
            "the bound series must not observe later backing-table mutation"
        );

        let rebound =
            SelectedObservationEphemeris::external(&path, budget).expect("bind changed table");
        assert_ne!(rebound.identity(), original_identity);
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
                    .ok_or(SelectedObservationEphemerisError::ByteOverflow)
            })
            .expect("finite fixture reference-data charge");
        drop(preflight);

        assert!(matches!(
            SelectedObservationEphemeris::external(
                &path,
                reference_budget(required_bytes - 1),
            ),
            Err(
                SelectedObservationEphemerisError::InsufficientReferenceDataBudget {
                    required_bytes: required,
                    available_bytes,
                }
            ) if required == required_bytes && available_bytes == required_bytes - 1
        ));
        assert!(matches!(
            SelectedObservationEphemeris::external(&path, reference_budget(required_bytes)),
            Err(SelectedObservationEphemerisError::InvalidRow { row: 0 })
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
                SelectedObservationEphemeris::external(&path, reference_budget(1 << 20)),
                Err(SelectedObservationEphemerisError::InvalidRow { row: 1 })
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
        let bound = SelectedObservationEphemeris::external(&path, reference_budget(1 << 20))
            .expect("bind ephemeris");

        for epoch in [59_999.999, 60_001.001] {
            match bound.sample(0, epoch) {
                Err(SelectedObservationEphemerisError::OutsideCoverage { target, mjd_days }) => {
                    assert_eq!(target, TARGET);
                    assert_eq!(mjd_days.to_bits(), epoch.to_bits());
                }
                other => panic!("expected exact OutsideCoverage error, got {other:?}"),
            }
        }
    }
}

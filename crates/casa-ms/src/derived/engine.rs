// SPDX-License-Identifier: LGPL-3.0-or-later
//! Core computation engine for derived MS quantities.
//!
//! [`MsCalEngine`] caches antenna positions, field directions, and the
//! observatory position from an MS, preserves the main-table epoch reference,
//! then computes hour angle, parallactic angle, azimuth/elevation, LAST, and
//! J2000 UVW on demand.
//!
//! Cf. C++ `DerivedMC::MSCalEngine`.

use casa_tables::Table;
use casa_tables::table_measures::{MeasRefDesc, TableMeasDesc};
use casa_types::measures::direction::{DirectionRef, MDirection};
use casa_types::measures::epoch::{EpochRef, MEpoch};
use casa_types::measures::frame::MeasFrame;
use casa_types::measures::position::MPosition;
use casa_types::{ArrayValue, ScalarValue};

use crate::error::{MsError, MsResult};
use crate::ms::MeasurementSet;
use crate::subtables::SubTable;

/// Engine for computing derived quantities from MS metadata.
///
/// Caches antenna positions and field directions extracted from subtables
/// and provides methods to compute hour angle, parallactic angle, AZEL,
/// LAST, and UVW in J2000.
///
/// # Usage
///
/// ```rust,no_run
/// use casa_ms::derived::engine::MsCalEngine;
/// use casa_ms::ms::MeasurementSet;
/// use casa_ms::builder::MeasurementSetBuilder;
///
/// let ms = MeasurementSet::create_memory(MeasurementSetBuilder::new()).unwrap();
/// let engine = MsCalEngine::new(&ms).unwrap();
/// ```
///
/// Cf. C++ `MSCalEngine`.
pub struct MsCalEngine {
    /// Antenna positions in ITRF.
    antenna_positions: Vec<MPosition>,
    /// Whether each antenna uses an alt-az mount.
    antenna_mount_alt_az: Vec<bool>,
    /// Field phase directions (constant term, J2000).
    field_directions: Vec<MDirection>,
    /// Observatory position (antenna 0 if no OBSERVATION subtable).
    observatory_position: MPosition,
    /// Epoch reference used by MAIN.TIME.
    time_reference: EpochRef,
}

impl MsCalEngine {
    /// Create a new engine by extracting metadata from the MS subtables.
    ///
    /// Reads ANTENNA positions, FIELD phase directions, and resolves the
    /// observatory position from `OBSERVATION::TELESCOPE_NAME` when possible,
    /// falling back to antenna 0 only when no catalog entry is available.
    pub fn new(ms: &MeasurementSet) -> MsResult<Self> {
        let ant = ms.antenna()?;
        let n_ant = ant.row_count();
        let mut antenna_positions = Vec::with_capacity(n_ant);
        let mut antenna_mount_alt_az = Vec::with_capacity(n_ant);
        for row in 0..n_ant {
            let pos = ant.position(row)?;
            antenna_positions.push(MPosition::new_itrf(pos[0], pos[1], pos[2]));
            let mount = ant.mount(row)?;
            antenna_mount_alt_az.push(mount.to_ascii_lowercase().starts_with("alt-az"));
        }

        let observatory_position = resolve_observatory_position(ms, &antenna_positions);
        let field = ms.field()?;
        let n_field = field.row_count();
        let mut field_directions = Vec::with_capacity(n_field);
        for row in 0..n_field {
            field_directions.push(resolve_field_phase_direction_j2000_with_observatory(
                ms,
                row,
                &observatory_position,
            )?);
        }
        let time_reference = detect_time_reference(ms);

        Ok(Self {
            antenna_positions,
            antenna_mount_alt_az,
            field_directions,
            observatory_position,
            time_reference,
        })
    }

    /// Create an engine with explicit data (useful for testing).
    pub fn from_parts(
        antenna_positions: Vec<MPosition>,
        field_directions: Vec<MDirection>,
        observatory_position: MPosition,
    ) -> Self {
        Self {
            antenna_mount_alt_az: vec![true; antenna_positions.len()],
            antenna_positions,
            field_directions,
            observatory_position,
            time_reference: EpochRef::UTC,
        }
    }

    /// Number of antennas.
    pub fn num_antennas(&self) -> usize {
        self.antenna_positions.len()
    }

    /// Number of fields.
    pub fn num_fields(&self) -> usize {
        self.field_directions.len()
    }

    /// Build a MeasFrame for the given time and antenna.
    fn make_frame(&self, time_mjd_sec: f64, antenna_id: usize) -> MsResult<MeasFrame> {
        let epoch = MEpoch::from_mjd(time_mjd_sec / 86400.0, self.time_reference);
        let position = self
            .antenna_positions
            .get(antenna_id)
            .ok_or_else(|| MsError::InvalidIndex {
                index: antenna_id,
                max: self.antenna_positions.len(),
                context: "antenna_id".to_string(),
            })?
            .clone();
        Ok(MeasFrame::new()
            .with_epoch(epoch)
            .with_position(position)
            .with_bundled_eop())
    }

    /// Build a MeasFrame for the given time and explicit position.
    fn make_frame_with_position(
        &self,
        time_mjd_sec: f64,
        position: MPosition,
    ) -> MsResult<MeasFrame> {
        let epoch = MEpoch::from_mjd(time_mjd_sec / 86400.0, self.time_reference);
        Ok(MeasFrame::new()
            .with_epoch(epoch)
            .with_position(position)
            .with_bundled_eop())
    }

    /// Build a spectral-conversion frame using the observatory position and field direction.
    ///
    /// This frame is suitable for frequency and velocity rendering, where the
    /// relevant context is the array observatory, the source direction, and
    /// the per-row epoch.
    pub fn spectral_frame_observatory(
        &self,
        time_mjd_sec: f64,
        field_id: usize,
    ) -> MsResult<MeasFrame> {
        let frame =
            self.make_frame_with_position(time_mjd_sec, self.observatory_position.clone())?;
        Ok(frame.with_direction(self.field_dir(field_id)?.clone()))
    }

    /// Get the field direction for the given field_id.
    fn field_dir(&self, field_id: usize) -> MsResult<&MDirection> {
        self.field_directions
            .get(field_id)
            .ok_or_else(|| MsError::InvalidIndex {
                index: field_id,
                max: self.field_directions.len(),
                context: "field_id".to_string(),
            })
    }

    /// Returns the cached FIELD phase direction in J2000 for `field_id`.
    pub fn field_direction_j2000(&self, field_id: usize) -> MsResult<&MDirection> {
        self.field_dir(field_id)
    }

    /// Compute the hour angle (radians) for a given time, field, and antenna.
    ///
    /// HA = LAST − RA, where RA is the field's right ascension and LAST is
    /// the local apparent sidereal time at the antenna position.
    ///
    /// Cf. C++ `MSCalEngine::getHA`.
    pub fn hour_angle(
        &self,
        time_mjd_sec: f64,
        field_id: usize,
        antenna_id: usize,
    ) -> MsResult<f64> {
        self.hadec(time_mjd_sec, field_id, antenna_id)
            .map(|(ha, _)| ha)
    }

    /// Compute the parallactic angle (radians) for a given time, field, and antenna.
    ///
    /// PA = atan2(cos(lat) * sin(HA),
    ///            sin(lat) * cos(dec) - cos(lat) * sin(dec) * cos(HA))
    ///
    /// Cf. C++ `MSCalEngine::getPA`.
    pub fn parallactic_angle(
        &self,
        time_mjd_sec: f64,
        field_id: usize,
        antenna_id: usize,
    ) -> MsResult<f64> {
        if !self.antenna_is_alt_az(antenna_id)? {
            return Ok(0.0);
        }
        let frame = self.make_frame(time_mjd_sec, antenna_id)?;
        let source_azel = self
            .field_dir(field_id)?
            .convert_to(DirectionRef::AZEL, &frame)?;
        let pole_azel =
            MDirection::from_angles(0.0, std::f64::consts::FRAC_PI_2, DirectionRef::HADEC)
                .convert_to(DirectionRef::AZEL, &frame)?;
        Ok(spherical_position_angle(&source_azel, &pole_azel))
    }

    /// Compute the array-fiducial hour angle (radians) using the observatory position.
    pub fn hour_angle_observatory(&self, time_mjd_sec: f64, field_id: usize) -> MsResult<f64> {
        let frame =
            self.make_frame_with_position(time_mjd_sec, self.observatory_position.clone())?;
        let dir = self.field_dir(field_id)?;
        let hadec = dir.convert_to(DirectionRef::HADEC, &frame)?;
        Ok(hadec.longitude_rad())
    }

    /// Compute the array-fiducial parallactic angle (radians) using the observatory position.
    pub fn parallactic_angle_observatory(
        &self,
        time_mjd_sec: f64,
        field_id: usize,
    ) -> MsResult<f64> {
        let frame =
            self.make_frame_with_position(time_mjd_sec, self.observatory_position.clone())?;
        let source_azel = self
            .field_dir(field_id)?
            .convert_to(DirectionRef::AZEL, &frame)?;
        let pole_azel =
            MDirection::from_angles(0.0, std::f64::consts::FRAC_PI_2, DirectionRef::HADEC)
                .convert_to(DirectionRef::AZEL, &frame)?;
        Ok(spherical_position_angle(&source_azel, &pole_azel))
    }

    /// Compute azimuth and elevation (radians) for a given time, field, and antenna.
    ///
    /// Converts the field direction to AZEL in the antenna's frame.
    ///
    /// Cf. C++ `MSCalEngine::getAzEl`.
    pub fn azel(
        &self,
        time_mjd_sec: f64,
        field_id: usize,
        antenna_id: usize,
    ) -> MsResult<(f64, f64)> {
        let frame = self.make_frame(time_mjd_sec, antenna_id)?;
        let dir = self.field_dir(field_id)?;
        let azel = dir.convert_to(DirectionRef::AZEL, &frame)?;
        Ok(azel.as_angles())
    }

    /// Compute the array-fiducial azimuth and elevation (radians) using the observatory position.
    pub fn azel_observatory(&self, time_mjd_sec: f64, field_id: usize) -> MsResult<(f64, f64)> {
        let frame =
            self.make_frame_with_position(time_mjd_sec, self.observatory_position.clone())?;
        let dir = self.field_dir(field_id)?;
        let azel = dir.convert_to(DirectionRef::AZEL, &frame)?;
        Ok(azel.as_angles())
    }

    /// Compute the local apparent sidereal time (radians) for a given time and antenna.
    ///
    /// Converts the epoch from UTC to LAST using the antenna position.
    ///
    /// Cf. C++ `MSCalEngine::getLAST`.
    pub fn last(&self, time_mjd_sec: f64, antenna_id: usize) -> MsResult<f64> {
        let frame = self.make_frame(time_mjd_sec, antenna_id)?;
        let epoch = MEpoch::from_mjd(time_mjd_sec / 86400.0, EpochRef::UTC);
        let last_epoch = epoch.convert_to(EpochRef::LAST, &frame)?;
        // LAST is stored as fraction of a day; convert to radians (1 day = 2π)
        let last_days = last_epoch.value().as_mjd();
        // Take fractional part of day and convert to radians
        let frac = last_days - last_days.floor();
        Ok(frac * 2.0 * std::f64::consts::PI)
    }

    /// Compute the array-fiducial LAST (radians) using the observatory position.
    pub fn last_observatory(&self, time_mjd_sec: f64) -> MsResult<f64> {
        let frame =
            self.make_frame_with_position(time_mjd_sec, self.observatory_position.clone())?;
        let epoch = MEpoch::from_mjd(time_mjd_sec / 86400.0, EpochRef::UTC);
        let last_epoch = epoch.convert_to(EpochRef::LAST, &frame)?;
        let last_days = last_epoch.value().as_mjd();
        let frac = last_days - last_days.floor();
        Ok(frac * 2.0 * std::f64::consts::PI)
    }

    /// Compute hour angle and declination (radians) for a given time, field, and antenna.
    ///
    /// Converts the field direction to HADEC in the antenna's frame.
    ///
    /// Returns `(ha, dec)`.
    ///
    /// Cf. C++ `MSCalEngine::getHADEC`.
    pub fn hadec(
        &self,
        time_mjd_sec: f64,
        field_id: usize,
        antenna_id: usize,
    ) -> MsResult<(f64, f64)> {
        let frame = self.make_frame(time_mjd_sec, antenna_id)?;
        let dir = self.field_dir(field_id)?;
        let hadec = dir.convert_to(DirectionRef::HADEC, &frame)?;
        Ok(hadec.as_angles())
    }

    /// Compute UVW in J2000 frame for a baseline (ant1, ant2) at a given time and field.
    ///
    /// UVW = R · (pos2 − pos1), where R is the rotation matrix from ITRF to
    /// the J2000 frame aligned with the field direction.
    ///
    /// Cf. C++ `MSCalEngine::getNewUVW`.
    pub fn uvw_j2000(
        &self,
        time_mjd_sec: f64,
        field_id: usize,
        ant1: usize,
        ant2: usize,
    ) -> MsResult<[f64; 3]> {
        let frame = self.make_frame(time_mjd_sec, ant1)?;

        let pos1 = self
            .antenna_positions
            .get(ant1)
            .ok_or_else(|| MsError::InvalidIndex {
                index: ant1,
                max: self.antenna_positions.len(),
                context: "ant1".to_string(),
            })?;
        let pos2 = self
            .antenna_positions
            .get(ant2)
            .ok_or_else(|| MsError::InvalidIndex {
                index: ant2,
                max: self.antenna_positions.len(),
                context: "ant2".to_string(),
            })?;

        // Baseline in ITRF (meters)
        let dx = pos2.as_itrf()[0] - pos1.as_itrf()[0];
        let dy = pos2.as_itrf()[1] - pos1.as_itrf()[1];
        let dz = pos2.as_itrf()[2] - pos1.as_itrf()[2];

        // Get the field direction in J2000
        let dir = self.field_dir(field_id)?;
        let (ra, dec) = dir.as_angles();

        let (sin_dec, cos_dec) = dec.sin_cos();

        // Rotate baseline from ITRF to equatorial using GMST, then to UVW using RA/Dec.
        let epoch = MEpoch::from_mjd(time_mjd_sec / 86400.0, EpochRef::UTC);
        let gmst_epoch = epoch.convert_to(EpochRef::GMST1, &frame)?;
        let gmst_frac = gmst_epoch.value().as_mjd();
        let gmst_rad = (gmst_frac - gmst_frac.floor()) * 2.0 * std::f64::consts::PI;

        // Rotate baseline from ITRF to equatorial using GMST
        let (sin_g, cos_g) = gmst_rad.sin_cos();
        let ex = cos_g * dx - sin_g * dy;
        let ey = sin_g * dx + cos_g * dy;
        let ez = dz;

        // Now rotate from equatorial to UVW using RA and Dec
        let (sin_ra, cos_ra) = ra.sin_cos();

        let u = sin_ra * ex - cos_ra * ey;
        let v = -sin_dec * cos_ra * ex - sin_dec * sin_ra * ey + cos_dec * ez;
        let w = cos_dec * cos_ra * ex + cos_dec * sin_ra * ey + sin_dec * ez;

        Ok([u, v, w])
    }

    /// The observatory position used by this engine.
    pub fn observatory_position(&self) -> &MPosition {
        &self.observatory_position
    }

    /// The epoch reference reconstructed from MAIN.TIME metadata.
    pub fn time_reference(&self) -> EpochRef {
        self.time_reference
    }

    fn antenna_is_alt_az(&self, antenna_id: usize) -> MsResult<bool> {
        self.antenna_mount_alt_az
            .get(antenna_id)
            .copied()
            .ok_or_else(|| MsError::InvalidIndex {
                index: antenna_id,
                max: self.antenna_mount_alt_az.len(),
                context: "antenna_id".to_string(),
            })
    }
}

/// Resolve the constant term of `FIELD.PHASE_DIR` at `field_id` into J2000.
///
/// This handles fixed and row-varying direction references, using the field
/// row's own `TIME` origin and the array observatory position when a dynamic
/// reference frame requires a conversion context.
pub fn resolve_field_phase_direction_j2000(
    ms: &MeasurementSet,
    field_id: usize,
) -> MsResult<MDirection> {
    let ant = ms.antenna()?;
    let mut antenna_positions = Vec::with_capacity(ant.row_count());
    for row in 0..ant.row_count() {
        let pos = ant.position(row)?;
        antenna_positions.push(MPosition::new_itrf(pos[0], pos[1], pos[2]));
    }
    let observatory_position = resolve_observatory_position(ms, &antenna_positions);
    resolve_field_phase_direction_j2000_with_observatory(ms, field_id, &observatory_position)
}

fn detect_time_reference(ms: &MeasurementSet) -> EpochRef {
    detect_epoch_reference(ms.main_table(), "TIME", EpochRef::UTC)
}

fn detect_epoch_reference(table: &Table, column: &str, default: EpochRef) -> EpochRef {
    let Some(desc) = TableMeasDesc::reconstruct(table, column) else {
        return default;
    };
    match desc.ref_desc() {
        MeasRefDesc::Fixed { refer } => refer.parse::<EpochRef>().unwrap_or(default),
        MeasRefDesc::VariableInt { .. } | MeasRefDesc::VariableString { .. } => default,
    }
}

fn phase_dir_constant(dir: &ArrayValue) -> MsResult<(f64, f64)> {
    match dir {
        ArrayValue::Float64(arr) => {
            let shape = arr.shape();
            if shape.len() != 2 || shape[0] != 2 || shape[1] == 0 {
                return Err(MsError::ColumnTypeMismatch {
                    column: "PHASE_DIR".to_string(),
                    table: "FIELD".to_string(),
                    expected: "Float64 array shaped [2, nPoly+1]".to_string(),
                    found: format!("Float64 array with shape {shape:?}"),
                });
            }
            Ok((arr[[0, 0]], arr[[1, 0]]))
        }
        other => Err(MsError::ColumnTypeMismatch {
            column: "PHASE_DIR".to_string(),
            table: "FIELD".to_string(),
            expected: "Float64 array shaped [2, nPoly+1]".to_string(),
            found: format!("{:?}", other.primitive_type()),
        }),
    }
}

fn resolve_observatory_position(ms: &MeasurementSet, antenna_positions: &[MPosition]) -> MPosition {
    ms.observation()
        .ok()
        .and_then(|observation| {
            (0..observation.row_count())
                .find_map(|row| observation.string(row, "TELESCOPE_NAME").ok())
        })
        .and_then(|name| MPosition::from_observatory_name(&name))
        .or_else(|| antenna_positions.first().cloned())
        .unwrap_or_else(|| MPosition::new_itrf(0.0, 0.0, 0.0))
}

fn resolve_field_phase_direction_j2000_with_observatory(
    ms: &MeasurementSet,
    field_id: usize,
    observatory_position: &MPosition,
) -> MsResult<MDirection> {
    let field = ms.field()?;
    let raw = field.phase_dir(field_id)?;
    let (lon, lat) = phase_dir_constant(raw)?;
    let source_ref = resolve_direction_reference(field.table(), "PHASE_DIR", field_id)?;
    let dir = MDirection::from_angles(lon, lat, source_ref);
    if source_ref == DirectionRef::J2000 {
        return Ok(dir);
    }
    let epoch_ref = detect_epoch_reference(field.table(), "TIME", EpochRef::UTC);
    let epoch = MEpoch::from_mjd(field.time(field_id)? / 86400.0, epoch_ref);
    let frame = MeasFrame::new()
        .with_epoch(epoch)
        .with_position(observatory_position.clone())
        .with_bundled_eop();
    Ok(dir.convert_to(DirectionRef::J2000, &frame)?)
}

fn resolve_direction_reference(table: &Table, column: &str, row: usize) -> MsResult<DirectionRef> {
    let Some(desc) = TableMeasDesc::reconstruct(table, column) else {
        return Ok(DirectionRef::J2000);
    };
    let refer = resolve_reference_string(table, &desc, row)?;
    refer
        .parse::<DirectionRef>()
        .map_err(|_| MsError::ColumnTypeMismatch {
            column: column.to_string(),
            table: "FIELD".to_string(),
            expected: "a supported direction reference".to_string(),
            found: refer,
        })
}

fn resolve_reference_string(table: &Table, desc: &TableMeasDesc, row: usize) -> MsResult<String> {
    match desc.ref_desc() {
        MeasRefDesc::Fixed { refer } => Ok(refer.clone()),
        MeasRefDesc::VariableInt {
            ref_column,
            tab_ref_types,
            tab_ref_codes,
        } => {
            let code = match table.get_scalar_cell(row, ref_column)? {
                ScalarValue::Int32(value) => *value,
                ScalarValue::Int64(value) => *value as i32,
                ScalarValue::UInt32(value) => *value as i32,
                other => {
                    return Err(MsError::ColumnTypeMismatch {
                        column: ref_column.clone(),
                        table: "FIELD".to_string(),
                        expected: "Int scalar".to_string(),
                        found: format!("{other:?}"),
                    });
                }
            };
            for (index, candidate) in tab_ref_codes.iter().enumerate() {
                if *candidate == code {
                    return tab_ref_types.get(index).cloned().ok_or_else(|| {
                        MsError::ColumnTypeMismatch {
                            column: desc.column_name().to_string(),
                            table: "FIELD".to_string(),
                            expected: "TabRefTypes index in bounds".to_string(),
                            found: format!("missing entry for TabRefCodes[{index}]"),
                        }
                    });
                }
            }
            Err(MsError::InvalidMeasureCode {
                table: "FIELD".to_string(),
                column: ref_column.clone(),
                code,
            })
        }
        MeasRefDesc::VariableString { ref_column } => {
            match table.get_scalar_cell(row, ref_column)? {
                ScalarValue::String(value) => Ok(value.clone()),
                other => Err(MsError::ColumnTypeMismatch {
                    column: ref_column.clone(),
                    table: "FIELD".to_string(),
                    expected: "String scalar".to_string(),
                    found: format!("{other:?}"),
                }),
            }
        }
    }
}

fn spherical_position_angle(origin: &MDirection, target: &MDirection) -> f64 {
    let (origin_lon, origin_lat) = origin.as_angles();
    let (target_lon, target_lat) = target.as_angles();
    let delta_lon = target_lon - origin_lon;
    let y = delta_lon.sin() * target_lat.cos();
    let x =
        origin_lat.cos() * target_lat.sin() - origin_lat.sin() * target_lat.cos() * delta_lon.cos();
    y.atan2(x)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::builder::MeasurementSetBuilder;
    use crate::schema;
    use crate::test_helpers::default_value_for_def;
    use casa_tables::ColumnSchema;
    use casa_tables::table_measures::default_direction_ref_map;
    use casa_tables::table_measures::{MeasureType, TableMeasDesc};
    use casa_types::{PrimitiveType, RecordField, RecordValue, ScalarValue, Value};
    use ndarray::{ArrayD, ShapeBuilder};

    /// VLA approximate ITRF position.
    const VLA_X: f64 = -1601185.4;
    const VLA_Y: f64 = -5041977.5;
    const VLA_Z: f64 = 3554875.9;

    fn make_engine() -> MsCalEngine {
        // Two antennas near VLA, 100m apart in X
        let pos0 = MPosition::new_itrf(VLA_X, VLA_Y, VLA_Z);
        let pos1 = MPosition::new_itrf(VLA_X + 100.0, VLA_Y, VLA_Z);

        // Field at RA=0, Dec=+45°
        let dir = MDirection::from_angles(0.0, std::f64::consts::FRAC_PI_4, DirectionRef::J2000);

        MsCalEngine::from_parts(vec![pos0.clone(), pos1], vec![dir], pos0)
    }

    fn add_vla_antenna(ms: &mut MeasurementSet) {
        ms.antenna_mut()
            .unwrap()
            .add_antenna(
                "VLA01",
                "N01",
                "GROUND-BASED",
                "ALT-AZ",
                [VLA_X, VLA_Y, VLA_Z],
                [0.0; 3],
                25.0,
            )
            .unwrap();
    }

    fn add_field_with_direction(ms: &mut MeasurementSet, direction: MDirection, time_mjd_sec: f64) {
        let (lon, lat) = direction.as_angles();
        let phase_dir = ArrayValue::Float64(
            ArrayD::from_shape_vec(ndarray::IxDyn(&[2, 1]).f(), vec![lon, lat]).unwrap(),
        );
        let field_fields: Vec<RecordField> = schema::field::REQUIRED_COLUMNS
            .iter()
            .map(|col| match col.name {
                "NAME" => RecordField::new(
                    col.name,
                    Value::Scalar(ScalarValue::String("TEST".to_string())),
                ),
                "TIME" => {
                    RecordField::new(col.name, Value::Scalar(ScalarValue::Float64(time_mjd_sec)))
                }
                "NUM_POLY" => RecordField::new(col.name, Value::Scalar(ScalarValue::Int32(0))),
                "DELAY_DIR" | "PHASE_DIR" | "REFERENCE_DIR" => {
                    RecordField::new(col.name, Value::Array(phase_dir.clone()))
                }
                _ => RecordField::new(col.name, default_value_for_def(col)),
            })
            .collect();
        ms.subtable_mut(schema::SubtableId::Field)
            .unwrap()
            .add_row(RecordValue::new(field_fields))
            .unwrap();
    }

    #[test]
    fn engine_from_ms() {
        let mut ms = MeasurementSet::create_memory(MeasurementSetBuilder::new()).unwrap();
        {
            let mut ant = ms.antenna_mut().unwrap();
            ant.add_antenna(
                "VLA01",
                "N01",
                "GROUND-BASED",
                "ALT-AZ",
                [VLA_X, VLA_Y, VLA_Z],
                [0.0; 3],
                25.0,
            )
            .unwrap();
        }
        let engine = MsCalEngine::new(&ms).unwrap();
        assert_eq!(engine.num_antennas(), 1);
        assert_eq!(engine.num_fields(), 0);
    }

    #[test]
    fn engine_uses_main_time_measinfo_reference() {
        let mut ms = MeasurementSet::create_memory(MeasurementSetBuilder::new()).unwrap();
        TableMeasDesc::new_fixed("TIME", MeasureType::Epoch, "TAI")
            .write(ms.main_table_mut())
            .unwrap();
        let engine = MsCalEngine::new(&ms).unwrap();
        assert_eq!(engine.time_reference(), EpochRef::TAI);
    }

    #[test]
    fn engine_new_accepts_fortran_order_phase_dir() {
        let mut ms = MeasurementSet::create_memory(MeasurementSetBuilder::new()).unwrap();
        {
            let mut ant = ms.antenna_mut().unwrap();
            ant.add_antenna(
                "VLA01",
                "N01",
                "GROUND-BASED",
                "ALT-AZ",
                [VLA_X, VLA_Y, VLA_Z],
                [0.0; 3],
                25.0,
            )
            .unwrap();
        }

        let phase_dir = ArrayValue::Float64(
            ArrayD::from_shape_vec(ndarray::IxDyn(&[2, 1]).f(), vec![0.25, 0.5]).unwrap(),
        );
        let field_fields: Vec<RecordField> = schema::field::REQUIRED_COLUMNS
            .iter()
            .map(|col| match col.name {
                "NAME" => RecordField::new(
                    col.name,
                    Value::Scalar(ScalarValue::String("TEST".to_string())),
                ),
                "NUM_POLY" => RecordField::new(col.name, Value::Scalar(ScalarValue::Int32(0))),
                "DELAY_DIR" | "PHASE_DIR" | "REFERENCE_DIR" => {
                    RecordField::new(col.name, Value::Array(phase_dir.clone()))
                }
                _ => RecordField::new(col.name, default_value_for_def(col)),
            })
            .collect();
        ms.subtable_mut(schema::SubtableId::Field)
            .unwrap()
            .add_row(RecordValue::new(field_fields))
            .unwrap();

        let engine = MsCalEngine::new(&ms).unwrap();
        assert_eq!(engine.num_fields(), 1);
        let (ra, dec) = engine.field_dir(0).unwrap().as_angles();
        assert!((ra - 0.25).abs() < 1e-12);
        assert!((dec - 0.5).abs() < 1e-12);
    }

    #[test]
    fn resolve_field_phase_direction_converts_fixed_dynamic_reference_to_j2000() {
        let mut ms = MeasurementSet::create_memory(MeasurementSetBuilder::new()).unwrap();
        add_vla_antenna(&mut ms);

        let time_mjd_sec = 59_000.5 * 86_400.0;
        let j2000 = MDirection::from_angles(1.0, 0.5, DirectionRef::J2000);
        let frame = MeasFrame::new()
            .with_epoch(MEpoch::from_mjd(time_mjd_sec / 86_400.0, EpochRef::UTC))
            .with_position(MPosition::new_itrf(VLA_X, VLA_Y, VLA_Z))
            .with_bundled_eop();
        let azel = j2000.convert_to(DirectionRef::AZEL, &frame).unwrap();
        add_field_with_direction(&mut ms, azel, time_mjd_sec);
        TableMeasDesc::new_fixed("PHASE_DIR", MeasureType::Direction, "AZEL")
            .write(ms.subtable_mut(schema::SubtableId::Field).unwrap())
            .unwrap();

        let recovered = resolve_field_phase_direction_j2000(&ms, 0).unwrap();
        let (ra, dec) = recovered.as_angles();
        assert!((ra - 1.0).abs() < 1e-9, "ra={ra}");
        assert!((dec - 0.5).abs() < 1e-9, "dec={dec}");
    }

    #[test]
    fn resolve_field_phase_direction_supports_variable_int_references() {
        let mut ms = MeasurementSet::create_memory(MeasurementSetBuilder::new()).unwrap();
        add_vla_antenna(&mut ms);

        let time_mjd_sec = 59_000.5 * 86_400.0;
        let j2000 = MDirection::from_angles(1.0, 0.5, DirectionRef::J2000);
        let frame = MeasFrame::new()
            .with_epoch(MEpoch::from_mjd(time_mjd_sec / 86_400.0, EpochRef::UTC))
            .with_position(MPosition::new_itrf(VLA_X, VLA_Y, VLA_Z))
            .with_bundled_eop();
        let azel = j2000.convert_to(DirectionRef::AZEL, &frame).unwrap();
        add_field_with_direction(&mut ms, azel, time_mjd_sec);

        let field = ms.subtable_mut(schema::SubtableId::Field).unwrap();
        field
            .add_column(
                ColumnSchema::scalar("PhaseDir_Ref", PrimitiveType::Int32),
                Some(Value::Scalar(ScalarValue::Int32(
                    DirectionRef::AZEL.casacore_code(),
                ))),
            )
            .unwrap();
        let (types, codes) = default_direction_ref_map();
        TableMeasDesc::new_variable_int(
            "PHASE_DIR",
            MeasureType::Direction,
            "PhaseDir_Ref",
            types,
            codes,
        )
        .unwrap()
        .write(field)
        .unwrap();

        let recovered = resolve_field_phase_direction_j2000(&ms, 0).unwrap();
        let (ra, dec) = recovered.as_angles();
        assert!((ra - 1.0).abs() < 1e-9, "ra={ra}");
        assert!((dec - 0.5).abs() < 1e-9, "dec={dec}");
    }

    #[test]
    fn hour_angle_is_finite() {
        let engine = make_engine();
        // MJD 59000 noon UTC
        let time = 59000.5 * 86400.0;
        let ha = engine.hour_angle(time, 0, 0).unwrap();
        assert!(ha.is_finite(), "HA should be finite, got {ha}");
    }

    #[test]
    fn hour_angle_observatory_is_finite() {
        let engine = make_engine();
        let time = 59000.5 * 86400.0;
        let ha = engine.hour_angle_observatory(time, 0).unwrap();
        assert!(ha.is_finite(), "observatory HA should be finite, got {ha}");
    }

    #[test]
    fn parallactic_angle_is_finite() {
        let engine = make_engine();
        let time = 59000.5 * 86400.0;
        let pa = engine.parallactic_angle(time, 0, 0).unwrap();
        assert!(pa.is_finite(), "PA should be finite, got {pa}");
    }

    #[test]
    fn parallactic_angle_observatory_is_finite() {
        let engine = make_engine();
        let time = 59000.5 * 86400.0;
        let pa = engine.parallactic_angle_observatory(time, 0).unwrap();
        assert!(pa.is_finite(), "observatory PA should be finite, got {pa}");
    }

    #[test]
    fn azel_elevation_positive_for_visible_source() {
        let engine = make_engine();
        // Try several times to find one where the source is up
        let mut found_visible = false;
        for hour in 0..24 {
            let time = (59000.0 + hour as f64 / 24.0) * 86400.0;
            let result = engine.azel(time, 0, 0);
            if let Ok((_az, el)) = result {
                if el > 0.0 {
                    found_visible = true;
                    break;
                }
            }
        }
        assert!(
            found_visible,
            "Source at Dec=+45° should be visible from VLA at some hour"
        );
    }

    #[test]
    fn observatory_azel_elevation_positive_for_visible_source() {
        let engine = make_engine();
        let mut found_visible = false;
        for hour in 0..24 {
            let time = (59000.0 + hour as f64 / 24.0) * 86400.0;
            let result = engine.azel_observatory(time, 0);
            if let Ok((_az, el)) = result {
                if el > 0.0 {
                    found_visible = true;
                    break;
                }
            }
        }
        assert!(
            found_visible,
            "Source at Dec=+45° should be visible from the observatory at some hour"
        );
    }

    #[test]
    fn last_monotonically_increases() {
        let engine = make_engine();
        let t0 = 59000.0 * 86400.0;
        let dt = 600.0; // 10 minutes
        let last0 = engine.last(t0, 0).unwrap();
        let last1 = engine.last(t0 + dt, 0).unwrap();
        // LAST should increase (modulo 2π)
        // Over 10 minutes, LAST should increase by ~2.5 minutes of sidereal time
        let diff = (last1 - last0).rem_euclid(2.0 * std::f64::consts::PI);
        assert!(
            diff > 0.0 && diff < std::f64::consts::PI,
            "LAST should increase monotonically, diff={diff}"
        );
    }

    #[test]
    fn uvw_j2000_is_finite() {
        let engine = make_engine();
        let time = 59000.5 * 86400.0;
        let uvw = engine.uvw_j2000(time, 0, 0, 1).unwrap();
        assert!(uvw[0].is_finite());
        assert!(uvw[1].is_finite());
        assert!(uvw[2].is_finite());
        // Baseline is ~100m, so UVW magnitudes should be of that order
        let mag = (uvw[0] * uvw[0] + uvw[1] * uvw[1] + uvw[2] * uvw[2]).sqrt();
        assert!(
            mag > 1.0 && mag < 200.0,
            "UVW magnitude should be ~100m for a 100m baseline, got {mag}"
        );
    }

    #[test]
    fn hadec_returns_angles() {
        let engine = make_engine();
        let time = 59000.5 * 86400.0;
        let (ha, dec) = engine.hadec(time, 0, 0).unwrap();
        assert!(ha.is_finite());
        assert!(dec.is_finite());
        // Dec should be close to +45° (the field direction)
        assert!(
            (dec - std::f64::consts::FRAC_PI_4).abs() < 0.1,
            "Dec should be ~45°, got {}°",
            dec.to_degrees()
        );
    }
}

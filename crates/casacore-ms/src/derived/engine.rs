// SPDX-License-Identifier: LGPL-3.0-or-later
//! Core computation engine for derived MS quantities.
//!
//! [`MsCalEngine`] caches antenna positions, field directions, and the
//! observatory position from an MS, then computes hour angle, parallactic
//! angle, azimuth/elevation, LAST, and J2000 UVW on demand.
//!
//! Cf. C++ `DerivedMC::MSCalEngine`.

use casacore_types::ArrayValue;
use casacore_types::measures::direction::{DirectionRef, MDirection};
use casacore_types::measures::epoch::{EpochRef, MEpoch};
use casacore_types::measures::frame::MeasFrame;
use casacore_types::measures::position::MPosition;

use crate::error::{MsError, MsResult};
use crate::ms::MeasurementSet;

/// Engine for computing derived quantities from MS metadata.
///
/// Caches antenna positions and field directions extracted from subtables
/// and provides methods to compute hour angle, parallactic angle, AZEL,
/// LAST, and UVW in J2000.
///
/// # Usage
///
/// ```rust,no_run
/// use casacore_ms::derived::engine::MsCalEngine;
/// use casacore_ms::ms::MeasurementSet;
/// use casacore_ms::builder::MeasurementSetBuilder;
///
/// let ms = MeasurementSet::create_memory(MeasurementSetBuilder::new()).unwrap();
/// let engine = MsCalEngine::new(&ms).unwrap();
/// ```
///
/// Cf. C++ `MSCalEngine`.
pub struct MsCalEngine {
    /// Antenna positions in ITRF.
    antenna_positions: Vec<MPosition>,
    /// Field phase directions (constant term, J2000).
    field_directions: Vec<MDirection>,
    /// Observatory position (antenna 0 if no OBSERVATION subtable).
    observatory_position: MPosition,
}

impl MsCalEngine {
    /// Create a new engine by extracting metadata from the MS subtables.
    ///
    /// Reads ANTENNA positions, FIELD phase directions, and the observatory
    /// position from the ANTENNA subtable (uses antenna 0 as the array center).
    pub fn new(ms: &MeasurementSet) -> MsResult<Self> {
        let ant = ms.antenna()?;
        let n_ant = ant.row_count();
        let mut antenna_positions = Vec::with_capacity(n_ant);
        for row in 0..n_ant {
            let pos = ant.position(row)?;
            antenna_positions.push(MPosition::new_itrf(pos[0], pos[1], pos[2]));
        }

        let field = ms.field()?;
        let n_field = field.row_count();
        let mut field_directions = Vec::with_capacity(n_field);
        for row in 0..n_field {
            let dir = field.phase_dir(row)?;
            // phase_dir returns Float64 array [2, nPoly+1]; take constant term.
            let (ra, dec) = phase_dir_constant(dir)?;
            field_directions.push(MDirection::from_angles(ra, dec, DirectionRef::J2000));
        }

        // Use antenna 0 as observatory position (C++ MSCalEngine uses OBSERVATION
        // TELESCOPE_NAME to look up the observatory, but for simplicity we use
        // the first antenna position as the reference).
        let observatory_position = if antenna_positions.is_empty() {
            MPosition::new_itrf(0.0, 0.0, 0.0)
        } else {
            antenna_positions[0].clone()
        };

        Ok(Self {
            antenna_positions,
            field_directions,
            observatory_position,
        })
    }

    /// Create an engine with explicit data (useful for testing).
    pub fn from_parts(
        antenna_positions: Vec<MPosition>,
        field_directions: Vec<MDirection>,
        observatory_position: MPosition,
    ) -> Self {
        Self {
            antenna_positions,
            field_directions,
            observatory_position,
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
        let epoch = MEpoch::from_mjd(time_mjd_sec / 86400.0, EpochRef::UTC);
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
        let last = self.last(time_mjd_sec, antenna_id)?;
        let dir = self.field_dir(field_id)?;
        let ra = dir.longitude_rad();
        Ok(last - ra)
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
        let ha = self.hour_angle(time_mjd_sec, field_id, antenna_id)?;
        let dir = self.field_dir(field_id)?;
        let dec = dir.latitude_rad();

        let pos = self
            .antenna_positions
            .get(antenna_id)
            .ok_or_else(|| MsError::InvalidIndex {
                index: antenna_id,
                max: self.antenna_positions.len(),
                context: "antenna_id".to_string(),
            })?;
        let lat = pos.latitude_rad();

        let pa = ha.sin().atan2(lat.tan() * dec.cos() - dec.sin() * ha.cos());
        Ok(pa)
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::builder::MeasurementSetBuilder;
    use crate::schema;
    use crate::test_helpers::default_value_for_def;
    use casacore_types::{RecordField, RecordValue, ScalarValue, Value};
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
    fn hour_angle_is_finite() {
        let engine = make_engine();
        // MJD 59000 noon UTC
        let time = 59000.5 * 86400.0;
        let ha = engine.hour_angle(time, 0, 0).unwrap();
        assert!(ha.is_finite(), "HA should be finite, got {ha}");
    }

    #[test]
    fn parallactic_angle_is_finite() {
        let engine = make_engine();
        let time = 59000.5 * 86400.0;
        let pa = engine.parallactic_angle(time, 0, 0).unwrap();
        assert!(pa.is_finite(), "PA should be finite, got {pa}");
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

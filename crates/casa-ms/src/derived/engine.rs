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

    /// Project an ITRF antenna-position offset into source-frame UVW meters.
    ///
    /// This mirrors CASA `KAntPosJones`: build the frame at antenna 0, convert
    /// the earth-frame antenna offset to the field direction's sky frame, then
    /// use its W projection as the per-antenna delay.
    pub fn project_itrf_offset_to_uvw(
        &self,
        time_mjd_sec: f64,
        field_id: usize,
        _antenna_id: usize,
        offset_m: [f64; 3],
    ) -> MsResult<[f64; 3]> {
        let frame = self.make_frame(time_mjd_sec, 0)?;
        let dir = self.field_dir(field_id)?;
        let source_itrf = dir.convert_to(DirectionRef::ITRF, &frame)?.cosines();
        let w = offset_m[0] * source_itrf[0]
            + offset_m[1] * source_itrf[1]
            + offset_m[2] * source_itrf[2];
        Ok([0.0, 0.0, w])
    }

    /// Reproject raw MS UVW coordinates from one field phase center to another.
    ///
    /// The input UVW is assumed to be stored in native MeasurementSet
    /// convention for `source_field_id`. This applies the same
    /// `FTMachine::rotateUVW()` / `fixvis` geometry transform CASA uses on
    /// `MAIN.UVW` for a phase-center change between fixed J2000 field
    /// directions and returns the corresponding geometric phase shift in
    /// meters.
    ///
    /// Important: CASA has two distinct UVW-shift call paths. The imaging /
    /// `fixvis` path operates directly on `MAIN.UVW`, while
    /// `PhaseShiftingTVI` wraps a different sign convention. For imaging
    /// parity we match the `FTMachine::rotateUVW()` behavior here.
    pub fn reproject_raw_uvw_between_fields(
        &self,
        raw_uvw_m: [f64; 3],
        source_field_id: usize,
        target_field_id: usize,
    ) -> MsResult<([f64; 3], f64)> {
        if source_field_id == target_field_id {
            return Ok((raw_uvw_m, 0.0));
        }

        let source_dir = self.field_dir(source_field_id)?;
        let target_dir = self.field_dir(target_field_id)?;
        // CASA's `FTMachine::rotateUVW()` drives `UVWMachine` directly on the
        // stored MAIN.UVW row vector. Matching the imaging path therefore
        // requires the target/source order opposite to the intuitive
        // source->target helper naming used elsewhere.
        let uvrot = uvw_rotation_matrix(target_dir, source_dir);
        let imaging_uvw_m = row_vec3_mul_mat3(raw_uvw_m, uvrot);
        let phrot = uvw_phase_rotation_vector(target_dir, source_dir);
        let phase_shift_m = dot3(phrot, imaging_uvw_m);
        Ok((imaging_uvw_m, phase_shift_m))
    }

    /// Reproject raw MS UVW coordinates from one field phase center to an
    /// explicit fixed J2000 direction.
    ///
    /// This uses the same UVW/phase-rotation machinery as
    /// [`Self::reproject_raw_uvw_between_fields`], but targets an explicit
    /// direction instead of another FIELD row.
    pub fn reproject_raw_uvw_to_direction(
        &self,
        raw_uvw_m: [f64; 3],
        source_field_id: usize,
        target_direction: &MDirection,
    ) -> MsResult<([f64; 3], f64)> {
        if target_direction.refer() != DirectionRef::J2000 {
            return Err(MsError::ColumnTypeMismatch {
                column: "PHASE_DIR".to_string(),
                table: "FIELD".to_string(),
                expected: "explicit target direction in J2000".to_string(),
                found: target_direction.refer().as_str().to_string(),
            });
        }
        let source_dir = self.field_dir(source_field_id)?;
        let uvrot = uvw_rotation_matrix(target_direction, source_dir);
        let imaging_uvw_m = row_vec3_mul_mat3(raw_uvw_m, uvrot);
        let phrot = uvw_phase_rotation_vector(target_direction, source_dir);
        let phase_shift_m = dot3(phrot, imaging_uvw_m);
        Ok((imaging_uvw_m, phase_shift_m))
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

fn uvw_rotation_matrix(
    source_direction: &MDirection,
    target_direction: &MDirection,
) -> [[f64; 3]; 3] {
    let (source_ra, source_dec) = source_direction.as_angles();
    let (target_ra, target_dec) = target_direction.as_angles();
    let rot1 = euler_rotation(&[
        (-(std::f64::consts::FRAC_PI_2 - source_ra), Axis::Z),
        (source_dec - std::f64::consts::FRAC_PI_2, Axis::X),
    ]);
    let rot2 = identity3();
    let rot3 = euler_rotation(&[
        (std::f64::consts::FRAC_PI_2 - target_dec, Axis::X),
        (-(target_ra - std::f64::consts::FRAC_PI_2), Axis::Z),
    ]);
    let uvrot = mat3_mul_mat3(mat3_mul_mat3(rot3, rot2), rot1);
    mat3_transpose(uvrot)
}

fn mat3_mul_vec3(matrix: [[f64; 3]; 3], vector: [f64; 3]) -> [f64; 3] {
    [
        matrix[0][0] * vector[0] + matrix[0][1] * vector[1] + matrix[0][2] * vector[2],
        matrix[1][0] * vector[0] + matrix[1][1] * vector[1] + matrix[1][2] * vector[2],
        matrix[2][0] * vector[0] + matrix[2][1] * vector[1] + matrix[2][2] * vector[2],
    ]
}

fn row_vec3_mul_mat3(vector: [f64; 3], matrix: [[f64; 3]; 3]) -> [f64; 3] {
    [
        vector[0] * matrix[0][0] + vector[1] * matrix[1][0] + vector[2] * matrix[2][0],
        vector[0] * matrix[0][1] + vector[1] * matrix[1][1] + vector[2] * matrix[2][1],
        vector[0] * matrix[0][2] + vector[1] * matrix[1][2] + vector[2] * matrix[2][2],
    ]
}

fn uvw_phase_rotation_vector(
    source_direction: &MDirection,
    target_direction: &MDirection,
) -> [f64; 3] {
    let (target_ra, target_dec) = target_direction.as_angles();
    let rot3 = euler_rotation(&[
        (std::f64::consts::FRAC_PI_2 - target_dec, Axis::X),
        (-(target_ra - std::f64::consts::FRAC_PI_2), Axis::Z),
    ]);
    let target_cosines = target_direction.cosines();
    let source_cosines = source_direction.cosines();
    mat3_mul_vec3(
        rot3,
        [
            target_cosines[0] - source_cosines[0],
            target_cosines[1] - source_cosines[1],
            target_cosines[2] - source_cosines[2],
        ],
    )
}

fn dot3(left: [f64; 3], right: [f64; 3]) -> f64 {
    left[0] * right[0] + left[1] * right[1] + left[2] * right[2]
}

fn identity3() -> [[f64; 3]; 3] {
    [[1.0, 0.0, 0.0], [0.0, 1.0, 0.0], [0.0, 0.0, 1.0]]
}

fn mat3_mul_mat3(left: [[f64; 3]; 3], right: [[f64; 3]; 3]) -> [[f64; 3]; 3] {
    let mut out = [[0.0; 3]; 3];
    for row in 0..3 {
        for col in 0..3 {
            out[row][col] = left[row][0] * right[0][col]
                + left[row][1] * right[1][col]
                + left[row][2] * right[2][col];
        }
    }
    out
}

fn mat3_transpose(matrix: [[f64; 3]; 3]) -> [[f64; 3]; 3] {
    [
        [matrix[0][0], matrix[1][0], matrix[2][0]],
        [matrix[0][1], matrix[1][1], matrix[2][1]],
        [matrix[0][2], matrix[1][2], matrix[2][2]],
    ]
}

#[derive(Clone, Copy)]
enum Axis {
    X,
    Z,
}

fn euler_rotation(operations: &[(f64, Axis)]) -> [[f64; 3]; 3] {
    let mut matrix = identity3();
    for (angle, axis) in operations {
        matrix = mat3_mul_mat3(matrix, axis_rotation(*angle, *axis));
    }
    matrix
}

fn axis_rotation(angle: f64, axis: Axis) -> [[f64; 3]; 3] {
    let (sin_angle, cos_angle) = angle.sin_cos();
    match axis {
        Axis::X => [
            [1.0, 0.0, 0.0],
            [0.0, cos_angle, -sin_angle],
            [0.0, sin_angle, cos_angle],
        ],
        Axis::Z => [
            [cos_angle, -sin_angle, 0.0],
            [sin_angle, cos_angle, 0.0],
            [0.0, 0.0, 1.0],
        ],
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
            let code = match table.cell_accessor(row, ref_column)?.scalar()? {
                &ScalarValue::Int32(value) => value,
                &ScalarValue::Int64(value) => value as i32,
                &ScalarValue::UInt32(value) => value as i32,
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
            match table.cell_accessor(row, ref_column)?.scalar()? {
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
    fn reproject_raw_uvw_matches_casa_fixvis_reference_values() {
        let engine = MsCalEngine::from_parts(
            vec![MPosition::new_itrf(VLA_X, VLA_Y, VLA_Z)],
            vec![
                MDirection::from_angles(
                    -1.058_214_942_099_811_3,
                    0.702_211_407_924_268_5,
                    DirectionRef::J2000,
                ),
                MDirection::from_angles(
                    -1.058_214_942_099_811_3,
                    0.706_574_731_054_254_4,
                    DirectionRef::J2000,
                ),
            ],
            MPosition::new_itrf(VLA_X, VLA_Y, VLA_Z),
        );
        let (uvw_m, phase_shift_m) = engine
            .reproject_raw_uvw_between_fields([24.4234, -31.0309, 17.6013], 1, 0)
            .unwrap();
        // Reference values captured from CASA `fixvis`, which exercises the
        // same `FTMachine::rotateUVW()` path used by standard imaging.
        assert!((uvw_m[0] - 24.4234).abs() < 1.0e-12, "u={}", uvw_m[0]);
        assert!(
            (uvw_m[1] - -30.953_804_692_483_43).abs() < 1.0e-12,
            "v={}",
            uvw_m[1]
        );
        assert!(
            (uvw_m[2] - 17.736_529_862_393_91).abs() < 1.0e-12,
            "w={}",
            uvw_m[2]
        );
        assert!(
            (phase_shift_m - 0.135_229_862_393_907_52).abs() < 1.0e-12,
            "phase_shift_m={phase_shift_m}"
        );
    }

    #[test]
    fn reproject_raw_uvw_matches_casa_fixvis_ra_offset_reference_values() {
        let engine = MsCalEngine::from_parts(
            vec![MPosition::new_itrf(VLA_X, VLA_Y, VLA_Z)],
            vec![
                MDirection::from_angles(
                    -1.058_214_942_099_811_3,
                    0.702_211_407_924_268_5,
                    DirectionRef::J2000,
                ),
                MDirection::from_angles(
                    -1.053_851_618_969_825_7,
                    0.702_211_407_924_268_5,
                    DirectionRef::J2000,
                ),
            ],
            MPosition::new_itrf(VLA_X, VLA_Y, VLA_Z),
        );
        let (uvw_m, phase_shift_m) = engine
            .reproject_raw_uvw_between_fields(
                [
                    27.073_056_790_908_41,
                    -29.672_968_936_171_65,
                    15.993_460_382_965_498,
                ],
                1,
                0,
            )
            .unwrap();
        assert!(
            (uvw_m[0] - 27.042_446_437_092_313).abs() < 1.0e-12,
            "u={}",
            uvw_m[0]
        );
        assert!(
            (uvw_m[1] - -29.749_226_002_013_575).abs() < 1.0e-12,
            "v={}",
            uvw_m[1]
        );
        assert!(
            (uvw_m[2] - 15.903_330_075_501_767).abs() < 1.0e-12,
            "w={}",
            uvw_m[2]
        );
        assert!(
            (phase_shift_m - -0.090_130_307_463_740_37).abs() < 1.0e-12,
            "phase_shift_m={phase_shift_m}"
        );
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

    #[test]
    fn engine_public_methods_cover_observatory_and_invalid_index_paths() {
        let engine = make_engine();
        let time = 59000.5 * 86400.0;

        assert_eq!(engine.num_antennas(), 2);
        assert_eq!(engine.num_fields(), 1);
        assert!(engine.field_direction_j2000(0).is_ok());
        assert!(engine.spectral_frame_observatory(time, 0).is_ok());
        assert!(engine.hour_angle(time, 0, 0).unwrap().is_finite());
        assert!(engine.hour_angle_observatory(time, 0).unwrap().is_finite());
        assert!(engine.parallactic_angle(time, 0, 0).unwrap().is_finite());
        assert!(
            engine
                .parallactic_angle_observatory(time, 0)
                .unwrap()
                .is_finite()
        );
        assert!(engine.azel(time, 0, 0).unwrap().0.is_finite());
        assert!(engine.azel_observatory(time, 0).unwrap().0.is_finite());
        assert!(engine.last(time, 0).unwrap().is_finite());
        assert!(engine.last_observatory(time).unwrap().is_finite());
        assert!(engine.hadec(time, 0, 0).unwrap().0.is_finite());
        assert!(engine.uvw_j2000(time, 0, 0, 1).unwrap()[0].is_finite());
        assert!(engine.field_direction_j2000(1).is_err());
        assert!(engine.hour_angle(time, 0, 9).is_err());
    }
}

// SPDX-License-Identifier: LGPL-3.0-or-later
//! Core computation engine for derived MS quantities.
//!
//! [`MsCalEngine`] caches antenna positions, field directions, and the
//! observatory position from an MS, preserves the main-table epoch reference,
//! then computes hour angle, parallactic angle, azimuth/elevation, LAST, and
//! J2000 UVW on demand.
//!
//! Cf. C++ `DerivedMC::MSCalEngine`.

use casa_core::measures::MeasuresProvider;
use casa_core::measures::baseline::UvwFrame;
use casa_core::measures::direction::{DirectionRef, MDirection};
use casa_core::measures::epoch::{EpochRef, MEpoch};
use casa_core::measures::frame::MeasFrame;
use casa_core::measures::position::MPosition;
use casa_core::{ArrayD, ArrayValue, ScalarValue, Value};
use casa_measures::table_measures::{MeasRefDesc, TableMeasDesc};
use casa_tables::Table;
use std::borrow::Cow;
use std::collections::HashMap;
use std::sync::{Arc, RwLock};

use crate::PhaseCentreEphemeris;
use crate::ephemeris::{AttachedFieldEphemeris, FieldMotion};
use crate::error::{MsError, MsResult};
use crate::ms::MeasurementSet;
use crate::subtables::SubTable;

mod catalogue;
mod feed_angle;

use feed_angle::FeedMount;

mod references;
mod uvw;

pub use references::detect_epoch_reference;
pub use references::raw_field_phase_direction;
pub use references::raw_field_reference_direction;
pub use references::resolve_direction_reference;
pub use references::resolve_direction_reference_selected;
use references::*;
pub use uvw::UvwRotation;

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
/// use casa_ms::MeasurementSet;
/// use casa_ms::MeasurementSetBuilder;
///
/// let ms = MeasurementSet::create_memory(MeasurementSetBuilder::new()).unwrap();
/// let engine = MsCalEngine::new(&ms).unwrap();
/// ```
///
/// Cf. C++ `MSCalEngine`.
pub struct MsCalEngine {
    /// Antenna positions in ITRF.
    antenna_positions: Box<[MPosition]>,
    /// Whether each antenna uses an alt-az mount.
    antenna_mount_alt_az: Box<[bool]>,
    /// How each antenna's mount turns its feed ([`Self::feed_parallactic_angle`]).
    antenna_feed_mounts: Box<[FeedMount]>,
    /// Field phase directions (constant term, J2000).
    field_directions: Box<[MDirection]>,
    /// Raw FIELD phase values, used as true-angle offsets for attached ephemerides.
    field_phase_offsets: Box<[[f64; 2]]>,
    /// Original references for corrections that use the stored phase measure.
    field_phase_references: Box<[DirectionRef]>,
    /// What each field follows, once attached
    /// ([`Self::with_field_ephemerides`]); empty until then.
    field_motions: Box<[FieldMotion]>,
    calibration_field_directions: bool,
    /// The epoch each field's `PHASE_DIR` is stated at (MSv2 `FIELD.TIME`),
    /// when the store states one.
    field_reference_epochs: Box<[Option<MEpoch>]>,
    /// Observatory position (antenna 0 if no OBSERVATION subtable).
    observatory_position: MPosition,
    /// Epoch reference used by MAIN.TIME.
    time_reference: EpochRef,
    measures: Option<Arc<dyn MeasuresProvider>>,
    azel_cache: RwLock<HashMap<GeometryCacheKey, (f64, f64)>>,
    hadec_cache: RwLock<HashMap<GeometryCacheKey, (f64, f64)>>,
    parallactic_angle_cache: RwLock<HashMap<GeometryCacheKey, f64>>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
struct GeometryCacheKey {
    time_bits: u64,
    field_id: usize,
    antenna_id: usize,
}

impl GeometryCacheKey {
    fn new(time_mjd_sec: f64, field_id: usize, antenna_id: usize) -> Self {
        Self {
            time_bits: time_mjd_sec.to_bits(),
            field_id,
            antenna_id,
        }
    }
}

fn cached_geometry_value<T: Copy>(
    cache: &RwLock<HashMap<GeometryCacheKey, T>>,
    key: GeometryCacheKey,
) -> Option<T> {
    cache
        .read()
        .expect("MS calibration geometry cache poisoned")
        .get(&key)
        .copied()
}

fn store_geometry_value<T: Copy>(
    cache: &RwLock<HashMap<GeometryCacheKey, T>>,
    key: GeometryCacheKey,
    value: T,
) {
    cache
        .write()
        .expect("MS calibration geometry cache poisoned")
        .insert(key, value);
}

impl MsCalEngine {
    /// Create a new engine by extracting metadata from the MS subtables.
    ///
    /// Reads ANTENNA positions, FIELD phase directions, and resolves the
    /// observatory position from `OBSERVATION::TELESCOPE_NAME` when possible,
    /// falling back to antenna 0 only when no catalog entry is available.
    ///
    /// The engine opens the production Measures provider
    /// ([`crate::open_measures_runtime`]); [`Self::with_measures`] takes one
    /// the caller already holds.
    pub fn new(ms: &MeasurementSet) -> MsResult<Self> {
        Self::with_measures(ms, crate::open_measures_runtime()?)
    }

    /// Create an engine over `ms`, as [`Self::new`] does, that evaluates
    /// every frame conversion with `measures`.
    ///
    /// An operation that opens one provider at its boundary builds its
    /// engine with this, so the engine and everything else the operation
    /// hands the provider to share one set of reference data.
    pub fn with_measures(
        ms: &MeasurementSet,
        measures: Arc<dyn MeasuresProvider>,
    ) -> MsResult<Self> {
        let ant = ms.antenna()?;
        let n_ant = ant.row_count();
        let mut antenna_positions = Vec::with_capacity(n_ant);
        let mut antenna_mount_alt_az = Vec::with_capacity(n_ant);
        let mut antenna_feed_mounts = Vec::with_capacity(n_ant);
        for row in 0..n_ant {
            let pos = ant.position(row)?;
            antenna_positions.push(MPosition::new_itrf(pos[0], pos[1], pos[2]));
            let mount = ant.mount(row)?;
            antenna_mount_alt_az.push(casa_observation::alt_az_mount(&mount));
            antenna_feed_mounts.push(FeedMount::parse(&mount));
        }

        let observatory_position =
            resolve_observatory_position(ms, &antenna_positions, measures.as_ref());
        let field = ms.field()?;
        let n_field = field.row_count();
        let mut field_directions = Vec::with_capacity(n_field);
        let mut field_phase_offsets = Vec::with_capacity(n_field);
        let mut field_phase_references = Vec::with_capacity(n_field);
        let mut field_reference_epochs = Vec::with_capacity(n_field);
        for row in 0..n_field {
            let (longitude, latitude) = phase_dir_constant(field.phase_dir(row)?)?;
            let reference = resolve_direction_reference(field.table(), "FIELD", "PHASE_DIR", row)?;
            field_phase_offsets.push([longitude, latitude]);
            field_phase_references.push(reference);
            field_directions.push(resolve_field_phase_direction_j2000_with_observatory(
                MDirection::from_angles(longitude, latitude, reference),
                &field,
                row,
                &observatory_position,
                Arc::clone(&measures),
            )?);
            field_reference_epochs.push(field_reference_epoch(ms, row).ok());
        }
        let time_reference = detect_time_reference(ms);

        Ok(Self {
            antenna_positions: antenna_positions.into_boxed_slice(),
            antenna_mount_alt_az: antenna_mount_alt_az.into_boxed_slice(),
            antenna_feed_mounts: antenna_feed_mounts.into_boxed_slice(),
            field_directions: field_directions.into_boxed_slice(),
            field_phase_offsets: field_phase_offsets.into_boxed_slice(),
            field_phase_references: field_phase_references.into_boxed_slice(),
            field_motions: Box::new([]),
            calibration_field_directions: false,
            field_reference_epochs: field_reference_epochs.into_boxed_slice(),
            observatory_position,
            time_reference,
            measures: Some(measures),
            azel_cache: RwLock::new(HashMap::new()),
            hadec_cache: RwLock::new(HashMap::new()),
            parallactic_angle_cache: RwLock::new(HashMap::new()),
        })
    }

    /// This engine with the ephemeris each field of `ms` follows
    /// (`FIELD/EPHEM<id>_*.tab`, named by `EPHEMERIS_ID`), each table read
    /// once. A field that follows one is then observed, at each time,
    /// towards the body offset by its `PHASE_DIR`, whatever the imaging
    /// phase centre ([`Self::observation_direction_j2000`]); a field whose
    /// `NUM_POLY` is above zero follows its polynomial, as casacore reads
    /// it. A field whose table cannot be read is refused only where its
    /// direction is asked for. Imaging attaches them; an engine without
    /// them reads every field's `PHASE_DIR` as its direction, as it always
    /// has.
    ///
    /// # Errors
    ///
    /// FIELD cannot be read.
    pub fn with_field_ephemerides(mut self, ms: &MeasurementSet) -> MsResult<Self> {
        self.field_motions = crate::ephemeris::field_motions(ms, false)?;
        Ok(self)
    }

    /// Create the engine for calibration and MeasurementSet geometry diagnostics.
    /// Attached ephemerides use `MSFieldColumns::phaseDirMeas` at the row epoch.
    ///
    /// # Errors
    ///
    /// Returns an error if the antenna or field metadata cannot be opened.
    pub fn for_calibration(ms: &MeasurementSet) -> MsResult<Self> {
        let mut engine = Self::new(ms)?;
        engine.field_motions = crate::ephemeris::field_motions(ms, true)?;
        engine.calibration_field_directions = true;
        Ok(engine)
    }

    /// Keep the FIELD ephemeris conventions used by SubMS spectral writers.
    pub fn for_spectral_rewrite(ms: &MeasurementSet) -> MsResult<Self> {
        let mut engine = Self::new(ms)?;
        engine.field_motions = crate::ephemeris::spectral_field_motions(ms)?;
        engine.calibration_field_directions = true;
        Ok(engine)
    }

    /// Prepare FIELD scalar sampling, for which the ephemeris remains
    /// available on rows whose directions are supplied by polynomials.
    pub fn for_ephemeris_scalars(ms: &MeasurementSet) -> MsResult<Self> {
        let mut engine = Self::new(ms)?;
        engine.field_motions = crate::ephemeris::scalar_field_motions(ms)?;
        Ok(engine)
    }

    /// Whether `field_id` follows an attached ephemeris
    /// ([`Self::with_field_ephemerides`]), readable or not.
    #[must_use]
    pub fn field_moves(&self, field_id: usize) -> bool {
        self.field_motions
            .get(field_id)
            .is_some_and(|motion| !matches!(motion, FieldMotion::Fixed))
    }

    /// Create an engine with explicit data (useful for testing).
    pub fn from_parts(
        antenna_positions: Vec<MPosition>,
        field_directions: Vec<MDirection>,
        observatory_position: MPosition,
        measures: Arc<dyn MeasuresProvider>,
    ) -> Self {
        let antenna_mount_alt_az = vec![true; antenna_positions.len()].into_boxed_slice();
        let antenna_feed_mounts =
            vec![FeedMount::AltAz; antenna_positions.len()].into_boxed_slice();
        let field_phase_offsets = field_directions
            .iter()
            .map(|direction| {
                let (longitude, latitude) = direction.as_angles();
                [longitude, latitude]
            })
            .collect::<Vec<_>>()
            .into_boxed_slice();
        let field_phase_references = field_directions.iter().map(MDirection::refer).collect();
        Self {
            antenna_mount_alt_az,
            antenna_feed_mounts,
            antenna_positions: antenna_positions.into_boxed_slice(),
            field_directions: field_directions.into_boxed_slice(),
            field_phase_offsets,
            field_phase_references,
            field_motions: Box::new([]),
            calibration_field_directions: false,
            field_reference_epochs: Box::new([]),
            observatory_position,
            time_reference: EpochRef::UTC,
            measures: Some(measures),
            azel_cache: RwLock::new(HashMap::new()),
            hadec_cache: RwLock::new(HashMap::new()),
            parallactic_angle_cache: RwLock::new(HashMap::new()),
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

    /// Build a measures frame at the specified visibility epoch and antenna.
    /// # Errors
    /// The antenna is outside the MeasurementSet antenna table.
    pub fn antenna_frame(&self, time_mjd_sec: f64, antenna_id: usize) -> MsResult<MeasFrame> {
        let epoch = MEpoch::from_mjd_seconds(time_mjd_sec, self.time_reference);
        let position = self
            .antenna_positions
            .get(antenna_id)
            .ok_or_else(|| MsError::InvalidIndex {
                index: antenna_id,
                max: self.antenna_positions.len(),
                context: "antenna_id".to_string(),
            })?
            .clone();
        let frame = MeasFrame::new().with_epoch(epoch).with_position(position);
        Ok(match &self.measures {
            Some(measures) => frame.with_measures(Arc::clone(measures)),
            None => frame,
        })
    }

    /// Build a MeasFrame for the given time and explicit position.
    fn make_frame_with_position(
        &self,
        time_mjd_sec: f64,
        position: MPosition,
    ) -> MsResult<MeasFrame> {
        let epoch = MEpoch::from_mjd_seconds(time_mjd_sec, self.time_reference);
        let frame = MeasFrame::new().with_epoch(epoch).with_position(position);
        Ok(match &self.measures {
            Some(measures) => frame.with_measures(Arc::clone(measures)),
            None => frame,
        })
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
        Ok(frame.with_direction(self.direction_at(time_mjd_sec, field_id)?.into_owned()))
    }

    /// Build a spectral-conversion frame using the observatory position and an
    /// explicit source direction.
    pub fn spectral_frame_observatory_direction(
        &self,
        time_mjd_sec: f64,
        direction: MDirection,
    ) -> MsResult<MeasFrame> {
        let frame =
            self.make_frame_with_position(time_mjd_sec, self.observatory_position.clone())?;
        Ok(frame.with_direction(direction))
    }

    /// Evaluate one named moving-source direction at an observation timestamp.
    ///
    /// The direction is resolved from the immutable Measures provider bound to
    /// this engine and is never retained as a sample-sized array. Solar-system
    /// bodies therefore follow the selected row epoch while fixed catalog
    /// names remain constant, with both projected into the J2000 frame used by
    /// MeasurementSet imaging geometry.
    pub(crate) fn named_direction_j2000(
        &self,
        time_mjd_sec: f64,
        source_name: &str,
    ) -> MsResult<MDirection> {
        let frame =
            self.make_frame_with_position(time_mjd_sec, self.observatory_position.clone())?;
        MDirection::from_source_name(source_name, &frame)?
            .convert_to(DirectionRef::J2000, &frame)
            .map_err(Into::into)
    }

    /// Evaluate one immutable named, external, or FIELD-attached moving source.
    pub fn ephemeris_direction_j2000(
        &self,
        time_mjd_sec: f64,
        field_id: usize,
        source_name: &str,
        ephemeris: &PhaseCentreEphemeris,
    ) -> MsResult<MDirection> {
        if let Some(target) = ephemeris.named_target() {
            if !target.eq_ignore_ascii_case(source_name) {
                return Err(MsError::InvalidInput(format!(
                    "moving target {source_name:?} does not match ephemeris target {target:?}"
                )));
            }
            return self.named_direction_j2000(time_mjd_sec, target);
        }
        let ephemeris_mjd_tdb = self.ephemeris_mjd_tdb(time_mjd_sec)?;
        let sample = ephemeris
            .sample(field_id, ephemeris_mjd_tdb)
            .map_err(|error| MsError::InvalidInput(error.to_string()))?;
        self.sampled_ephemeris_direction_j2000(time_mjd_sec, sample)
    }

    /// Evaluate an immutable ephemeris radial velocity at the CASA `MeasComet`
    /// sampling epoch corresponding to one MeasurementSet timestamp.
    pub fn ephemeris_radial_velocity(
        &self,
        time_mjd_sec: f64,
        field_id: usize,
        ephemeris: &PhaseCentreEphemeris,
    ) -> MsResult<casa_core::measures::radial_velocity::MRadialVelocity> {
        let sample = ephemeris
            .sample(field_id, self.ephemeris_mjd_utc(time_mjd_sec)?)
            .map_err(|error| MsError::InvalidInput(error.to_string()))?;
        Ok(casa_core::measures::radial_velocity::MRadialVelocity::new(
            sample.radial_velocity_m_per_s,
            sample.velocity_reference,
        ))
    }

    /// Interpolate the radial velocity of an attached field ephemeris at the supplied MS epoch in seconds.
    pub fn attached_radial_velocity(
        &self,
        time_mjd_sec: f64,
        field_id: usize,
    ) -> MsResult<Option<casa_core::measures::radial_velocity::MRadialVelocity>> {
        self.field_series(field_id)?
            .map(|(series, _)| series.radial_velocity(self.ephemeris_mjd_utc(time_mjd_sec)?))
            .transpose()
    }

    fn ephemeris_mjd_tdb(&self, time_mjd_sec: f64) -> MsResult<f64> {
        let frame =
            self.make_frame_with_position(time_mjd_sec, self.observatory_position.clone())?;
        let epoch = frame
            .epoch()
            .expect("ephemeris conversion frame always contains an epoch");
        Ok(epoch.convert_to(EpochRef::TDB, &frame)?.value().as_mjd())
    }

    fn ephemeris_mjd_utc(&self, time_mjd_sec: f64) -> MsResult<f64> {
        let frame =
            self.make_frame_with_position(time_mjd_sec, self.observatory_position.clone())?;
        let epoch = frame
            .epoch()
            .expect("ephemeris conversion frame always contains an epoch");
        Ok(epoch.convert_to(EpochRef::UTC, &frame)?.value().as_mjd())
    }

    /// The attached ephemeris phase direction in its original measure frame,
    /// at the interpolation epoch used by CASA's FIELD columns.
    pub fn ephemeris_phase_direction(
        &self,
        field_id: usize,
        time_mjd_sec: f64,
    ) -> MsResult<Option<MDirection>> {
        let Some((ephemeris, offset)) = self.field_series(field_id)? else {
            return Ok(None);
        };
        let mjd = self.field_ephemeris_mjd_utc(field_id, time_mjd_sec)?;
        ephemeris.reference_direction(mjd, offset, true).map(Some)
    }

    /// Evaluate a scalar from the attached ephemeris at the FIELD column's
    /// interpolation epoch. A fixed field has no attached value.
    pub fn field_ephemeris_scalar(
        &self,
        field_id: usize,
        time_mjd_sec: f64,
        evaluate: impl FnOnce(&AttachedFieldEphemeris, f64) -> MsResult<f64>,
    ) -> MsResult<Option<f64>> {
        let Some((ephemeris, _)) = self.field_series(field_id)? else {
            return Ok(None);
        };
        let mjd = self.field_ephemeris_mjd_utc(field_id, time_mjd_sec)?;
        ephemeris.check_observation_epoch(mjd)?;
        evaluate(ephemeris, mjd).map(Some)
    }

    fn field_ephemeris_mjd_utc(&self, field_id: usize, time_mjd_sec: f64) -> MsResult<f64> {
        let origin = self
            .field_reference_epochs
            .get(field_id)
            .and_then(Option::as_ref);
        let reference = origin.map_or(self.time_reference, MEpoch::refer);
        let epoch = if time_mjd_sec == 0.0 {
            origin
                .cloned()
                .unwrap_or_else(|| MEpoch::from_mjd_seconds(time_mjd_sec, reference))
        } else {
            MEpoch::from_mjd_seconds(time_mjd_sec, reference)
        };
        let frame =
            self.make_frame_with_position(time_mjd_sec, self.observatory_position.clone())?;
        Ok(epoch.convert_to(EpochRef::UTC, &frame)?.value().as_mjd())
    }

    fn sampled_ephemeris_direction_j2000(
        &self,
        time_mjd_sec: f64,
        sample: crate::ephemeris::EvaluatedEphemerisSample,
    ) -> MsResult<MDirection> {
        let frame =
            self.make_frame_with_position(time_mjd_sec, self.observatory_position.clone())?;
        Ok(casa_core::measures::ephemeris::topocentric_j2000(
            sample.geocentric_position_metres,
            sample.position_reference,
            &self.observatory_position,
            &frame,
        )?)
    }

    /// The J2000 direction rows of `field_id` are observed towards at
    /// `time_mjd_sec`: its constant `PHASE_DIR`, or, for a field that
    /// follows an attached ephemeris ([`Self::with_field_ephemerides`]), the
    /// body's direction from the observatory (its samples interpolated at
    /// the time's TDB, as CASA's imager reads a `MeasComet`) shifted by
    /// `PHASE_DIR`'s true angle, as casacore's `phaseDirMeas` shifts it.
    ///
    /// # Errors
    ///
    /// An unknown field, a field whose ephemeris cannot be read or does not
    /// cover the time, or a failed conversion.
    pub fn observation_direction_j2000(
        &self,
        time_mjd_sec: f64,
        field_id: usize,
    ) -> MsResult<MDirection> {
        Ok(self.direction_at(time_mjd_sec, field_id)?.into_owned())
    }

    /// Return the phase centre evaluated without a row epoch, retaining its
    /// recorded reference. A moving field is read at FIELD.TIME or, when
    /// unavailable, its first observation time.
    ///
    /// # Errors
    /// An unknown field or a moving phase centre that cannot be evaluated.
    pub fn reference_phase_direction(
        &self,
        field_id: usize,
        first_time_mjd_sec: f64,
    ) -> MsResult<MDirection> {
        match self.moving_field_reference_direction(field_id, first_time_mjd_sec)? {
            Some(direction) => Ok(direction),
            None => {
                self.field_dir(field_id)?;
                let [longitude, latitude] = self.field_phase_offsets[field_id];
                Ok(MDirection::from_angles(
                    longitude,
                    latitude,
                    self.field_phase_references[field_id],
                ))
            }
        }
    }

    /// [`Self::observation_direction_j2000`], borrowed for a fixed field.
    fn direction_at(&self, time_mjd_sec: f64, field_id: usize) -> MsResult<Cow<'_, MDirection>> {
        let Some((ephemeris, offset)) = self.field_series(field_id)? else {
            return Ok(Cow::Borrowed(self.field_dir(field_id)?));
        };
        if self.calibration_field_directions {
            let mjd = self.ephemeris_mjd_utc(time_mjd_sec)?;
            let reference = ephemeris.reference_direction(mjd, offset, true)?;
            let frame =
                self.make_frame_with_position(time_mjd_sec, self.observatory_position.clone())?;
            return Ok(Cow::Owned(
                reference.convert_to(DirectionRef::J2000, &frame)?,
            ));
        }
        let direction = self.field_body_direction(time_mjd_sec, ephemeris)?;
        Ok(Cow::Owned(
            casa_core::measures::ephemeris::offset_true_angle(&direction, offset)?,
        ))
    }

    /// Topocentric J2000 direction of the target a FIELD tracks, before
    /// applying the observing offset in `PHASE_DIR`.
    pub fn tracked_body_direction_j2000(
        &self,
        time_mjd_sec: f64,
        field_id: usize,
    ) -> MsResult<MDirection> {
        let (ephemeris, _) = self.field_series(field_id)?.ok_or_else(|| {
            MsError::InvalidInput(format!("FIELD_ID {field_id} has no attached ephemeris"))
        })?;
        self.field_body_direction(time_mjd_sec, ephemeris)
    }

    fn field_body_direction(
        &self,
        time_mjd_sec: f64,
        ephemeris: &AttachedFieldEphemeris,
    ) -> MsResult<MDirection> {
        let mjd = self.ephemeris_mjd_tdb(time_mjd_sec)?;
        match ephemeris {
            AttachedFieldEphemeris::Inertial(series) => {
                let sample = crate::ephemeris::evaluate(series, mjd)
                    .map_err(|error| MsError::InvalidInput(error.to_string()))?;
                self.sampled_ephemeris_direction_j2000(time_mjd_sec, sample)
            }
            AttachedFieldEphemeris::Apparent(_) => {
                let direction = ephemeris.reference_direction(mjd, [0.; 2], true)?;
                let frame =
                    self.make_frame_with_position(time_mjd_sec, self.observatory_position.clone())?;
                Ok(direction.convert_to(DirectionRef::J2000, &frame)?)
            }
        }
    }

    /// casacore's `MSFieldColumns::phaseDirMeas(field_id)` asked at no time,
    /// for a field that follows an attached ephemeris
    /// ([`Self::with_field_ephemerides`]): the body's geocentric direction at
    /// the field's reference epoch (MSv2 `FIELD.TIME`) in UTC, shifted by
    /// `PHASE_DIR`'s true angle in the ephemeris's frame. `None` for a fixed
    /// field, whose direction is its `PHASE_DIR`. A store that states no
    /// reference epoch (MSv4) is taken at `first_time_mjd_sec`, in the
    /// engine's time scale: the field's first time, which is the `TIME` the
    /// MSv4 → MSv2 conversion writes for it.
    ///
    /// CASA's `tclean` centres an image there when its phase centre is left
    /// empty (#987). Geocentric and at no row's time, it is not the direction
    /// rows are observed towards ([`Self::observation_direction_j2000`]).
    ///
    /// # Errors
    ///
    /// An unknown field, an ephemeris that cannot be read or does not cover
    /// that time, or a failed conversion.
    pub fn moving_field_reference_direction(
        &self,
        field_id: usize,
        first_time_mjd_sec: f64,
    ) -> MsResult<Option<MDirection>> {
        self.moving_field_reference_direction_with_offset(field_id, first_time_mjd_sec, None)
    }

    /// The nominal direction at the FIELD reference epoch with a supplied
    /// observing offset. Calibration table construction supplies
    /// `REFERENCE_DIR` here. Without an override, use the field's
    /// `PHASE_DIR` offset.
    ///
    /// # Errors
    /// The field or its reference epoch is outside the ephemeris domain.
    pub fn moving_field_reference_direction_with_offset(
        &self,
        field_id: usize,
        first_time_mjd_sec: f64,
        offset: Option<[f64; 2]>,
    ) -> MsResult<Option<MDirection>> {
        let Some((ephemeris, phase_offset)) = self.field_series(field_id)? else {
            return Ok(None);
        };
        let frame = MeasFrame::new().with_position(self.observatory_position.clone());
        let frame = match &self.measures {
            Some(measures) => frame.with_measures(Arc::clone(measures)),
            None => frame,
        };
        let origin = self
            .field_reference_epochs
            .get(field_id)
            .cloned()
            .flatten()
            .unwrap_or_else(|| MEpoch::from_mjd_seconds(first_time_mjd_sec, self.time_reference));
        let mjd_utc = origin.convert_to(EpochRef::UTC, &frame)?.value().as_mjd();
        ephemeris
            .reference_direction(mjd_utc, offset.unwrap_or(phase_offset), false)
            .map(Some)
    }

    /// The ephemeris `field_id` follows and its `PHASE_DIR` offset, or
    /// `None` for a fixed field.
    fn field_series(
        &self,
        field_id: usize,
    ) -> MsResult<Option<(&AttachedFieldEphemeris, [f64; 2])>> {
        let ephemeris = match self.field_motions.get(field_id) {
            None | Some(FieldMotion::Fixed) => return Ok(None),
            Some(FieldMotion::Ephemeris(ephemeris)) => ephemeris,
            Some(FieldMotion::Unreadable(reason)) => {
                return Err(MsError::InvalidInput(format!(
                    "FIELD_ID {field_id} follows an ephemeris that cannot be read: {reason}"
                )));
            }
        };
        let offset = *self
            .field_phase_offsets
            .get(field_id)
            .ok_or_else(|| MsError::InvalidInput(format!("FIELD_ID {field_id} out of range")))?;
        Ok(Some((ephemeris, offset)))
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

    /// Returns the constant term of `FIELD.PHASE_DIR` at `field_id` in J2000.
    /// For a field that follows an ephemeris ([`Self::field_moves`]) that is
    /// the offset from the body, not a direction: use
    /// [`Self::observation_direction_j2000`].
    ///
    /// The engine resolves it once, at construction, for fixed and
    /// row-varying direction references, using the field row's own `TIME`
    /// origin and the observatory position when a dynamic reference frame
    /// needs a conversion context.
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
    /// Zero unless the antenna's mount is alt-az (any `ALT-AZ…` mount); a
    /// Nasmyth or beam-waveguide mount's extra feed rotation is in
    /// [`Self::feed_parallactic_angle`].
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
        self.sky_parallactic_angle(time_mjd_sec, field_id, antenna_id)
    }

    /// The field's parallactic angle at the antenna, whatever its mount.
    fn sky_parallactic_angle(
        &self,
        time_mjd_sec: f64,
        field_id: usize,
        antenna_id: usize,
    ) -> MsResult<f64> {
        let key = GeometryCacheKey::new(time_mjd_sec, field_id, antenna_id);
        if let Some(value) = cached_geometry_value(&self.parallactic_angle_cache, key) {
            return Ok(value);
        }
        let frame = self.antenna_frame(time_mjd_sec, antenna_id)?;
        let value = self
            .direction_at(time_mjd_sec, field_id)?
            .parallactic_angle(&frame)?;
        store_geometry_value(&self.parallactic_angle_cache, key, value);
        Ok(value)
    }

    /// Compute the array-fiducial hour angle (radians) using the observatory position.
    pub fn hour_angle_observatory(&self, time_mjd_sec: f64, field_id: usize) -> MsResult<f64> {
        self.hour_angle_at_position(time_mjd_sec, field_id, self.observatory_position.clone())
    }

    /// Compute hour angle (radians) at the supplied array position.
    pub fn hour_angle_at_position(
        &self,
        time_mjd_sec: f64,
        field_id: usize,
        position: MPosition,
    ) -> MsResult<f64> {
        self.hadec_at_position(time_mjd_sec, field_id, position)
            .map(|(hour_angle, _)| hour_angle)
    }

    fn hadec_at_position(
        &self,
        time_mjd_sec: f64,
        field_id: usize,
        position: MPosition,
    ) -> MsResult<(f64, f64)> {
        let frame = self.make_frame_with_position(time_mjd_sec, position)?;
        let dir = self.direction_at(time_mjd_sec, field_id)?;
        let hadec = dir.convert_to(DirectionRef::HADEC, &frame)?;
        Ok(hadec.as_angles())
    }

    /// Compute the array-fiducial parallactic angle (radians) using the observatory position.
    pub fn parallactic_angle_observatory(
        &self,
        time_mjd_sec: f64,
        field_id: usize,
    ) -> MsResult<f64> {
        self.parallactic_angle_at_position(
            time_mjd_sec,
            field_id,
            self.observatory_position.clone(),
        )
    }

    /// Compute parallactic angle (radians) at the supplied array position.
    pub fn parallactic_angle_at_position(
        &self,
        time_mjd_sec: f64,
        field_id: usize,
        position: MPosition,
    ) -> MsResult<f64> {
        let frame = self.make_frame_with_position(time_mjd_sec, position)?;
        Ok(self
            .direction_at(time_mjd_sec, field_id)?
            .parallactic_angle(&frame)?)
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
        let key = GeometryCacheKey::new(time_mjd_sec, field_id, antenna_id);
        if let Some(value) = cached_geometry_value(&self.azel_cache, key) {
            return Ok(value);
        }
        let frame = self.antenna_frame(time_mjd_sec, antenna_id)?;
        let dir = self.direction_at(time_mjd_sec, field_id)?;
        let azel = dir.convert_to(DirectionRef::AZEL, &frame)?;
        let value = azel.as_angles();
        store_geometry_value(&self.azel_cache, key, value);
        Ok(value)
    }

    /// Compute the array-fiducial azimuth and elevation (radians) using the observatory position.
    pub fn azel_observatory(&self, time_mjd_sec: f64, field_id: usize) -> MsResult<(f64, f64)> {
        self.azel_at_position(time_mjd_sec, field_id, self.observatory_position.clone())
    }

    /// Compute azimuth and elevation (radians) at the supplied array position.
    pub fn azel_at_position(
        &self,
        time_mjd_sec: f64,
        field_id: usize,
        position: MPosition,
    ) -> MsResult<(f64, f64)> {
        let frame = self.make_frame_with_position(time_mjd_sec, position)?;
        let dir = self.direction_at(time_mjd_sec, field_id)?;
        let azel = dir.convert_to(DirectionRef::AZEL, &frame)?;
        Ok(azel.as_angles())
    }

    /// Compute the local apparent sidereal time (radians) for a given time and antenna.
    ///
    /// Converts the epoch from UTC to LAST using the antenna position.
    ///
    /// Cf. C++ `MSCalEngine::getLAST`.
    pub fn last(&self, time_mjd_sec: f64, antenna_id: usize) -> MsResult<f64> {
        let frame = self.antenna_frame(time_mjd_sec, antenna_id)?;
        let epoch = MEpoch::from_mjd_seconds(time_mjd_sec, EpochRef::UTC);
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
        let epoch = MEpoch::from_mjd_seconds(time_mjd_sec, EpochRef::UTC);
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
        let key = GeometryCacheKey::new(time_mjd_sec, field_id, antenna_id);
        if let Some(value) = cached_geometry_value(&self.hadec_cache, key) {
            return Ok(value);
        }
        let frame = self.antenna_frame(time_mjd_sec, antenna_id)?;
        let dir = self.direction_at(time_mjd_sec, field_id)?;
        let hadec = dir.convert_to(DirectionRef::HADEC, &frame)?;
        let value = hadec.as_angles();
        store_geometry_value(&self.hadec_cache, key, value);
        Ok(value)
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
        let frame = self.antenna_frame(time_mjd_sec, ant1)?;

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
        let dir = self.direction_at(time_mjd_sec, field_id)?;
        if self.field_moves(field_id) {
            // MSCalEngine::getNewUVW forms the baseline from antenna 1 to 2
            // and projects it in an observatory frame at this row's epoch.
            let epoch = MEpoch::from_mjd_seconds(time_mjd_sec, self.time_reference);
            let measures = self
                .measures
                .clone()
                .map_or_else(crate::open_measures_runtime, Ok)?;
            let projection = UvwFrame::new(
                dir.as_angles().into(),
                epoch,
                &self.observatory_position,
                measures,
            )?;
            return Ok(projection.baseline_uvw([dx, dy, dz]));
        }
        let (ra, dec) = dir.as_angles();

        let (sin_dec, cos_dec) = dec.sin_cos();

        // Rotate baseline from ITRF to equatorial using GMST, then to UVW using RA/Dec.
        let epoch = MEpoch::from_mjd_seconds(time_mjd_sec, EpochRef::UTC);
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
        let antenna = self
            .antenna_positions
            .first()
            .ok_or_else(|| MsError::InvalidIndex {
                index: 0,
                max: 0,
                context: "antenna position correction reference".to_owned(),
            })?;
        if offset_m == [0.0; 3] {
            return Ok([0.0; 3]);
        }
        let direction = self.reference_phase_direction(field_id, time_mjd_sec)?;
        // KAntPosJones sets its correction frame to UTC, including for a
        // MeasurementSet whose TIME column is labeled TAI.
        let epoch = MEpoch::from_mjd_seconds(time_mjd_sec, EpochRef::UTC);
        let measures = self
            .measures
            .clone()
            .map_or_else(crate::open_measures_runtime, Ok)?;
        let direction = if direction.refer() == DirectionRef::J2000 {
            direction
        } else {
            let frame = MeasFrame::new()
                .with_epoch(epoch.clone())
                .with_position(antenna.clone())
                .with_measures(Arc::clone(&measures));
            direction.convert_to(DirectionRef::J2000, &frame)?
        };
        let projection = UvwFrame::new(direction.as_angles().into(), epoch, antenna, measures)?;
        let w = projection.baseline_uvw(offset_m)[2];
        Ok([0.0, 0.0, w])
    }

    /// Reproject raw MS UVW coordinates from one field phase center to another.
    ///
    /// The input UVW is assumed to be stored in native MeasurementSet
    /// convention for `source_field_id`. This applies the same
    /// direct `fixvis` geometry transform CASA applies to `MAIN.UVW` for a
    /// phase-center change between fixed J2000 field directions and returns
    /// the corresponding geometric phase shift in meters.
    ///
    /// Important: CASA's `GridFT` imaging path first applies `negateUV`, which
    /// imaging's `GridFtProjection` (casa-imaging-model) represents. This method preserves
    /// the direct `fixvis` convention only.
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
        // CASA's direct fixvis transform drives UVWMachine on the stored
        // MAIN.UVW row vector. Matching that path therefore
        // requires the target/source order opposite to the intuitive
        // source->target helper naming used elsewhere.
        Ok(UvwRotation::new(target_dir, source_dir).apply(raw_uvw_m))
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
        Ok(UvwRotation::new(target_direction, source_dir).apply(raw_uvw_m))
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

#[cfg(test)]
mod tests;

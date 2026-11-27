// SPDX-License-Identifier: LGPL-3.0-or-later
//! Recover FTMachine's moving-source rotation from a stored Fourier model.

use casa_core::measures::MeasFrame;
use casa_core::measures::direction::{DirectionRef, MDirection};
use casa_core::measures::position::MPosition;
use casa_core::measures::record::{direction_from_record, position_from_record};
use casa_core::{RecordValue, ScalarValue, Value};
use casa_ms::{EphemerisBudget, PhaseCentreEphemeris, PlanetaryEphemeris};

use crate::ft::moving_source::ObservedSource;

use super::{FourierRow, GainSolveError, model_error, record_number, record_text};

pub(super) struct Tracking {
    source: Source,
    first_direction: MDirection,
    image_direction: MDirection,
    position: MPosition,
    fixed_epoch: Option<f64>,
    last_shift: Option<([u64; 3], [f64; 2])>,
}

enum Source {
    Field,
    Ephemeris(PhaseCentreEphemeris),
    Legacy(ObservedSource),
    Direction(MDirection),
    Named(Box<PlanetaryEphemeris>),
}

impl Tracking {
    pub(super) fn load(record: &RecordValue) -> Result<Option<Self>, GainSolveError> {
        if !matches!(
            record.get("fixmovingsource"),
            Some(Value::Scalar(ScalarValue::Bool(true)))
        ) {
            return Ok(None);
        }
        let moving = measure(record, "movingdir_rec")?;
        let reference = record_text(moving, "refer")?;
        let source = match reference {
            "COMET" => Source::Ephemeris(
                PhaseCentreEphemeris::external(
                    record_text(record, "ephemeristable")?,
                    EphemerisBudget::new(64 << 20),
                )
                .map_err(model_error)?,
            ),
            _ if record.get("doconversion").is_some() => Source::Legacy(
                ObservedSource::new(direction_from_record(moving).map_err(model_error)?)
                    .map_err(model_error)?,
            ),
            "APP" => Source::Field,
            _ if reference.parse::<DirectionRef>().is_ok() => {
                Source::Direction(direction_from_record(moving).map_err(model_error)?)
            }
            _ => Source::Named(Box::new(
                PlanetaryEphemeris::open_discovered(reference).map_err(model_error)?,
            )),
        };
        Ok(Some(Self {
            source,
            first_direction: direction_from_record(measure(record, "firstmovingdir_rec")?)
                .map_err(model_error)?,
            image_direction: direction_from_record(measure(record, "mimage_rec")?)
                .map_err(model_error)?,
            position: position_from_record(measure(record, "mlocation_rec")?)
                .map_err(model_error)?,
            fixed_epoch: record_number(record, "phasecentertime").filter(|time| *time > 0.0),
            last_shift: None,
        }))
    }

    pub(super) fn observe(
        &mut self,
        row: &FourierRow<'_>,
        virtual_anchor: Option<&mut Option<MDirection>>,
    ) -> Result<(MeasFrame, [f64; 2]), GainSolveError> {
        let frame = row
            .engine
            .spectral_frame_observatory_direction(row.row.time_seconds, row.field.clone())
            .map_err(model_error)?
            .with_position(self.position.clone());
        let shift = self.shift(row, &frame, virtual_anchor)?;
        Ok((frame, shift))
    }

    fn shift(
        &mut self,
        row: &FourierRow<'_>,
        frame: &MeasFrame,
        virtual_anchor: Option<&mut Option<MDirection>>,
    ) -> Result<[f64; 2], GainSolveError> {
        let key = [
            row.row.time_seconds.to_bits(),
            row.phase_time.to_bits(),
            u64::from(row.row.field_id as u32),
        ];
        if let Some((previous, shift)) = self.last_shift
            && previous == key
        {
            return Ok(shift);
        }
        let source = match &self.source {
            Source::Field => row
                .engine
                .tracked_body_direction_j2000(
                    self.fixed_epoch.unwrap_or(row.phase_time),
                    row.row.field_id as usize,
                )
                .map_err(model_error)?,
            Source::Ephemeris(ephemeris) => row
                .engine
                .ephemeris_direction_j2000(
                    row.row.time_seconds,
                    row.row.field_id as usize,
                    "COMET",
                    ephemeris,
                )
                .map_err(model_error)?,
            Source::Direction(direction) => direction.clone(),
            Source::Named(planet) => planet.direction(frame).map_err(model_error)?,
            Source::Legacy(source) => source
                .direction(frame, self.image_direction.refer())
                .map_err(model_error)?,
        };
        let source = source
            .convert_to(self.image_direction.refer(), frame)
            .map_err(model_error)?;
        let (longitude, latitude) = source.as_angles();
        let anchor = match virtual_anchor {
            // VisModelData calls initMaps on its first observing buffer.
            Some(anchor) if self.is_legacy() => anchor.get_or_insert(source),
            _ => &self.first_direction,
        };
        let (anchor_lon, anchor_lat) = anchor.as_angles();
        let shift = [longitude - anchor_lon, latitude - anchor_lat];
        self.last_shift = Some((key, shift));
        Ok(shift)
    }

    pub(super) fn centre(&self, shift: [f64; 2]) -> MDirection {
        let (image_lon, image_lat) = self.image_direction.as_angles();
        let sign = if self.is_legacy() {
            // The legacy rotateUVW phases on image + first - current.
            -1.
        } else {
            1.
        };
        MDirection::from_angles(
            image_lon + sign * shift[0],
            image_lat + sign * shift[1],
            self.image_direction.refer(),
        )
    }

    pub(super) fn is_legacy(&self) -> bool {
        matches!(self.source, Source::Legacy(_))
    }
}

/// CASA indexes the correlator directions by integration time. A saved
/// positive epoch uses the first integration at or after it, clamped to
/// the last integration when it lies beyond the MeasurementSet.
fn measure<'a>(record: &'a RecordValue, name: &str) -> Result<&'a RecordValue, GainSolveError> {
    match record.get(name) {
        Some(Value::Record(measure)) => Ok(measure),
        _ => Err(model_error(format!(
            "virtual tracking model has no {name} measure"
        ))),
    }
}

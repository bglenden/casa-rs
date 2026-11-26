// SPDX-License-Identifier: LGPL-3.0-or-later

use casa_components::{ShapeType, SkyComponent};
use casa_core::{ArrayValue, RecordValue, ScalarValue, Value};

use casa_images::{GaussianBeam, ImageError};

pub(super) struct Measurement {
    pub(super) channel: usize,
    pub(super) pixels: [f64; 2],
    pub(super) peak: [f64; 2],
    pub(super) sum: f64,
    pub(super) brightness: String,
    pub(super) beam: Option<GaussianBeam>,
    pub(super) beam_in_pixels: f64,
    pub(super) is_point: bool,
    pub(super) deconvolved_peak: Option<[f64; 2]>,
}

impl Measurement {
    pub(super) fn record(
        &self,
        component: &SkyComponent,
        deconvolved: bool,
    ) -> Result<RecordValue, ImageError> {
        let mut record = component
            .to_record()
            .map_err(|error| ImageError::CasaSelection(error.to_string()))?;
        let peak = if deconvolved {
            self.deconvolved_peak
        } else {
            Some(self.peak)
        };
        if let Some([value, error]) = peak {
            let mut peak = quantity(value, &self.brightness);
            peak.upsert("error", real(error));
            record.upsert("peak", Value::Record(peak));
        }
        record.upsert("sum", Value::Record(quantity(self.sum, &self.brightness)));
        if !deconvolved {
            record.upsert(
                "pixelcoords",
                Value::Array(ArrayValue::from_f64_vec(self.pixels.to_vec())),
            );
        }
        if let Some(beam) = self.beam {
            record.upsert("ispoint", Value::Scalar(ScalarValue::Bool(self.is_point)));
            if !deconvolved || component.shape().shape_type() == ShapeType::Gaussian {
                let mut metadata = RecordValue::default();
                metadata.upsert("beamarcsec", Value::Record(beam.to_record()));
                metadata.upsert("beampixels", real(self.beam_in_pixels));
                metadata.upsert("beamster", real(beam.area()));
                record.upsert("beam", Value::Record(metadata));
            }
        }
        if let Some(Value::Record(spectrum)) = record.get_mut("spectrum") {
            spectrum.upsert(
                "channel",
                Value::Scalar(ScalarValue::Int32(self.channel as i32)),
            );
        }
        if let Some(Value::Record(shape)) = record.get_mut("shape") {
            display_gaussian(shape);
        }
        Ok(record)
    }
}

pub(super) fn quantity(value: f64, unit: &str) -> RecordValue {
    let mut record = RecordValue::default();
    record.upsert("value", real(value));
    record.upsert("unit", Value::Scalar(ScalarValue::String(unit.to_owned())));
    record
}

fn real(value: f64) -> Value {
    Value::Scalar(ScalarValue::Float64(value))
}

fn display_gaussian(shape: &mut RecordValue) {
    const DEGREES_PER_RADIAN: f64 = 180. / std::f64::consts::PI;
    for (key, unit, conversion) in [
        ("majoraxis", "arcsec", 60.),
        ("minoraxis", "arcsec", 60.),
        ("majoraxiserror", "arcsec", 3600. * DEGREES_PER_RADIAN),
        ("minoraxiserror", "arcsec", 3600. * DEGREES_PER_RADIAN),
        ("positionangleerror", "deg", DEGREES_PER_RADIAN),
    ] {
        if let Some(Value::Record(quantity)) = shape.get_mut(key) {
            if let Some(Value::Scalar(ScalarValue::Float64(value))) = quantity.get_mut("value") {
                *value *= conversion;
            }
            quantity.upsert("unit", Value::Scalar(ScalarValue::String(unit.to_owned())));
        }
    }
}

// SPDX-License-Identifier: LGPL-3.0-or-later
//! Beam metadata corresponding to C++ `GaussianBeam` and `ImageBeamSet`.

use casa_numerics::{EllipticalGaussian, common_enclosing_gaussian, deconvolving_gaussian};
use casa_types::quanta::{Quantity, Unit};
use casa_types::{ArrayValue, RecordField, RecordValue, ScalarValue, Value};

use crate::error::ImageError;

/// Read the scalar or one-element real quantity forms CASA accepts.
pub(crate) fn quantity_from_record(record: &RecordValue) -> Result<Quantity, ImageError> {
    let number = match record.get("value") {
        Some(Value::Scalar(ScalarValue::Float64(v))) => Some(*v),
        Some(Value::Scalar(ScalarValue::Float32(v))) => Some(f64::from(*v)),
        Some(Value::Scalar(ScalarValue::Int32(v))) => Some(f64::from(*v)),
        Some(Value::Array(ArrayValue::Float64(v))) if v.len() == 1 => v.iter().next().copied(),
        Some(Value::Array(ArrayValue::Float32(v))) if v.len() == 1 => {
            v.iter().next().map(|v| f64::from(*v))
        }
        Some(Value::Array(ArrayValue::Int32(v))) if v.len() == 1 => {
            v.iter().next().map(|v| f64::from(*v))
        }
        _ => None,
    }
    .ok_or_else(|| ImageError::InvalidMetadata("invalid quantity value".into()))?;
    let Some(Value::Scalar(ScalarValue::String(unit))) = record.get("unit") else {
        return Err(ImageError::InvalidMetadata("invalid quantity unit".into()));
    };
    let unit = casa_types::quanta::casacore::casacore_unit(unit)
        .map_err(|error| ImageError::InvalidMetadata(error.to_string()))?;
    Ok(Quantity::with_unit(number, unit))
}

/// A restoring beam with each quantity retained in its stored unit.
///
/// This complements [`GaussianBeam`]'s radian-based representation for
/// metadata edits that must preserve the original units.
#[derive(Debug, Clone)]
pub struct StoredGaussianBeam {
    major: Quantity,
    minor: Quantity,
    position_angle: Quantity,
}

/// The component of a stored restoring beam to update.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BeamComponent {
    /// Full width of the major axis.
    Major,
    /// Full width of the minor axis.
    Minor,
    /// Position angle of the major axis.
    PositionAngle,
}

/// Wrap an angular quantity to CASA's (-90, 90] degree range, preserving its
/// unit and casacore's conversion order.
///
/// # Errors
///
/// Returns an error for a non-angular or non-finite quantity.
pub fn wrap_position_angle(pa: &Quantity) -> Result<Quantity, ImageError> {
    let degree = Unit::new("deg").expect("built-in degrees");
    if !pa.value().is_finite() || !pa.unit().conformant(&degree) {
        return Err(ImageError::InvalidMetadata(
            "position angle must be a finite angular quantity".into(),
        ));
    }
    let quarter = Quantity::new(90.0, "deg").expect("built-in degrees");
    let threshold = casa_types::quanta::casacore::convert(&quarter, Some(pa.unit()))
        .map_err(|error| ImageError::InvalidMetadata(error.to_string()))?
        .value();
    if pa.value() > threshold || pa.value() <= -threshold {
        let mut degrees = casa_types::quanta::casacore::get_conformant_value(pa, &degree)
            .map_err(|error| ImageError::InvalidMetadata(error.to_string()))?
            % 180.0;
        if degrees > 90.0 {
            degrees -= 180.0;
        } else if degrees <= -90.0 {
            degrees += 180.0;
        }
        let wrapped = Quantity::with_unit(degrees, degree);
        casa_types::quanta::casacore::convert(&wrapped, Some(pa.unit()))
            .map_err(|error| ImageError::InvalidMetadata(error.to_string()))
    } else {
        Ok(pa.clone())
    }
}

impl StoredGaussianBeam {
    /// Construct a beam from angular quantities, retaining their units.
    ///
    /// # Errors
    ///
    /// Returns an error for non-angular, non-finite, negative, or
    /// incorrectly ordered axes, or a non-angular position angle.
    pub fn new(
        major: Quantity,
        minor: Quantity,
        position_angle: Quantity,
    ) -> Result<Self, ImageError> {
        let radians = Unit::new("rad").expect("built-in unit");
        for (label, quantity) in [
            ("major", &major),
            ("minor", &minor),
            ("position angle", &position_angle),
        ] {
            if !quantity.value().is_finite() || !quantity.unit().conformant(&radians) {
                return Err(ImageError::InvalidMetadata(format!(
                    "beam {label} must be a finite angular quantity"
                )));
            }
        }
        if major.value() < 0.0 || minor.value() < 0.0 {
            return Err(ImageError::InvalidMetadata(
                "beam axes cannot be negative".into(),
            ));
        }
        let minor_in_major = casa_types::quanta::casacore::convert(&minor, Some(major.unit()))
            .map_err(|error| ImageError::InvalidMetadata(error.to_string()))?;
        if major.value() < minor_in_major.value() {
            return Err(ImageError::InvalidMetadata(
                "major axis must be greater or equal to minor axis".into(),
            ));
        }
        Ok(Self {
            major,
            minor,
            position_angle,
        })
    }

    /// Reconstruct a beam from a casacore quantity record.
    ///
    /// # Errors
    ///
    /// Returns an error for malformed, non-angular, or invalid beam values.
    pub fn from_record(record: &RecordValue) -> Result<Self, ImageError> {
        if record.len() != 3 {
            return Err(ImageError::InvalidMetadata(
                "Beam record does not contain 3 fields".into(),
            ));
        }
        let read = |name: &str| -> Result<Quantity, ImageError> {
            let Some(Value::Record(field)) = record.get(name) else {
                return Err(ImageError::InvalidMetadata(format!(
                    "beam is missing {name}"
                )));
            };
            quantity_from_record(field)
        };
        Self::new(read("major")?, read("minor")?, read("positionangle")?)
    }

    /// Return the requested component in its stored unit.
    #[must_use]
    pub fn component(&self, component: BeamComponent) -> &Quantity {
        match component {
            BeamComponent::Major => &self.major,
            BeamComponent::Minor => &self.minor,
            BeamComponent::PositionAngle => &self.position_angle,
        }
    }

    /// Return the position angle wrapped to CASA's (-90, 90] degree range,
    /// retaining its stored unit.
    ///
    /// # Errors
    ///
    /// Returns an error if the angular unit cannot be converted.
    pub fn wrapped_position_angle(&self) -> Result<Quantity, ImageError> {
        wrap_position_angle(&self.position_angle)
    }

    /// Return a copy with one component replaced.
    ///
    /// # Errors
    ///
    /// Returns an error if the resulting beam is invalid.
    pub fn with_component(
        &self,
        component: BeamComponent,
        value: Quantity,
    ) -> Result<Self, ImageError> {
        let mut updated = self.clone();
        match component {
            BeamComponent::Major => updated.major = value,
            BeamComponent::Minor => updated.minor = value,
            BeamComponent::PositionAngle => updated.position_angle = value,
        }
        Self::new(updated.major, updated.minor, updated.position_angle)
    }

    /// Whether either axis is zero, making the restoring beam null.
    #[must_use]
    pub fn is_null(&self) -> bool {
        self.major.value() == 0.0 || self.minor.value() == 0.0
    }

    /// Serialize the original quantity values and units to a beam record.
    #[must_use]
    pub fn to_record(&self) -> RecordValue {
        let field = |name: &str, quantity: &Quantity| {
            RecordField::new(
                name,
                Value::Record(RecordValue::new(vec![
                    RecordField::new(
                        "value",
                        Value::Scalar(ScalarValue::Float64(quantity.value())),
                    ),
                    RecordField::new(
                        "unit",
                        Value::Scalar(ScalarValue::String(quantity.unit().name().into())),
                    ),
                ])),
            )
        };
        RecordValue::new(vec![
            field("major", &self.major),
            field("minor", &self.minor),
            field("positionangle", &self.position_angle),
        ])
    }
}

/// A two-dimensional Gaussian restoring beam.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct GaussianBeam {
    /// Full width at half maximum of the major axis, in radians.
    pub major: f64,
    /// Full width at half maximum of the minor axis, in radians.
    pub minor: f64,
    /// Position angle of the major axis, in radians.
    pub position_angle: f64,
}

impl GaussianBeam {
    /// Creates a new Gaussian beam.
    pub fn new(major: f64, minor: f64, position_angle: f64) -> Self {
        Self {
            major,
            minor,
            position_angle,
        }
    }

    /// Returns `true` if the beam is null.
    pub fn is_null(&self) -> bool {
        self.major == 0.0 || self.minor == 0.0
    }

    /// Returns the beam area in steradians.
    pub fn area(&self) -> f64 {
        std::f64::consts::PI / (4.0 * 2.0_f64.ln()) * self.major * self.minor
    }

    /// Returns the major axis converted to the requested angular unit.
    pub fn major_in(&self, unit: &str) -> Result<f64, ImageError> {
        angle_value_in(self.major, unit)
    }

    /// Returns the minor axis converted to the requested angular unit.
    pub fn minor_in(&self, unit: &str) -> Result<f64, ImageError> {
        angle_value_in(self.minor, unit)
    }

    /// Returns the position angle converted to the requested angular unit.
    pub fn position_angle_in(&self, unit: &str) -> Result<f64, ImageError> {
        angle_value_in(self.position_angle, unit)
    }

    /// Serializes the beam to the casacore quantity-record representation.
    pub fn to_record(&self) -> RecordValue {
        fn quantity_record(value: f64, unit: &str) -> RecordValue {
            RecordValue::new(vec![
                RecordField::new("value", Value::Scalar(ScalarValue::Float64(value))),
                RecordField::new("unit", Value::Scalar(ScalarValue::String(unit.into()))),
            ])
        }

        // CASA image products conventionally persist fitted beam widths in
        // arcseconds and position angles in degrees. Keep radians as the
        // in-memory contract while matching that interoperable record form.
        let radians_to_degrees = 180.0 / std::f64::consts::PI;
        let radians_to_arcseconds = 3_600.0 * radians_to_degrees;

        RecordValue::new(vec![
            RecordField::new(
                "major",
                Value::Record(quantity_record(
                    self.major * radians_to_arcseconds,
                    "arcsec",
                )),
            ),
            RecordField::new(
                "minor",
                Value::Record(quantity_record(
                    self.minor * radians_to_arcseconds,
                    "arcsec",
                )),
            ),
            RecordField::new(
                "positionangle",
                Value::Record(quantity_record(
                    self.position_angle * radians_to_degrees,
                    "deg",
                )),
            ),
        ])
    }

    /// Deserializes a beam from a casacore quantity record.
    pub fn from_record(rec: &RecordValue) -> Result<Self, ImageError> {
        let stored = StoredGaussianBeam::from_record(rec)?;
        let radians = Unit::new("rad").expect("built-in radians unit must parse");
        let convert = |component| {
            stored
                .component(component)
                .get_value_in(&radians)
                .map_err(|error| ImageError::InvalidMetadata(error.to_string()))
        };
        let major = convert(BeamComponent::Major)?;
        let minor = convert(BeamComponent::Minor)?;
        let position_angle = convert(BeamComponent::PositionAngle)?;
        if (major == 0.0 && stored.component(BeamComponent::Major).value() != 0.0)
            || (minor == 0.0 && stored.component(BeamComponent::Minor).value() != 0.0)
        {
            return Err(ImageError::InvalidMetadata(
                "beam axes underflow in the shared radian representation (#977)".into(),
            ));
        }
        if !major.is_finite() || !minor.is_finite() || !position_angle.is_finite() {
            return Err(ImageError::InvalidMetadata(
                "invalid restoring beam values".into(),
            ));
        }
        // Casacore compares the original units. Conversion of physically equal
        // axes to radians can reverse their order by one rounding bit.
        Ok(Self {
            major: major.max(minor),
            minor,
            position_angle,
        })
    }

    /// Computes the Gaussian beam that would need to convolve `other` to
    /// produce `self`, following casacore `GaussianDeconvolver::deconvolve()`.
    ///
    /// Returns `Ok(None)` when the two beams are effectively the same size.
    pub fn deconvolving_beam(self, other: Self) -> Result<Option<Self>, ImageError> {
        if self.is_null() || other.is_null() {
            return Err(ImageError::InvalidMetadata(
                "cannot deconvolve null beams".to_string(),
            ));
        }
        deconvolving_gaussian(numeric_beam(self), numeric_beam(other))
            .map(|beam| beam.map(gaussian_beam))
            .map_err(|error| ImageError::InvalidMetadata(error.to_string()))
    }
}

fn angle_value_in(radians: f64, unit: &str) -> Result<f64, ImageError> {
    let quantity =
        Quantity::new(radians, "rad").expect("built-in radians quantity must always parse");
    let target = Unit::new(unit)
        .map_err(|err| ImageError::InvalidMetadata(format!("invalid unit '{unit}': {err}")))?;
    quantity.get_value_in(&target).map_err(|err| {
        ImageError::InvalidMetadata(format!(
            "cannot convert beam angle from rad to '{unit}': {err}"
        ))
    })
}

impl Default for GaussianBeam {
    fn default() -> Self {
        Self {
            major: 0.0,
            minor: 0.0,
            position_angle: 0.0,
        }
    }
}

/// Single-beam or per-plane beam metadata corresponding to C++ `ImageBeamSet`.
#[derive(Debug, Clone, PartialEq)]
pub struct ImageBeamSet {
    beams: Vec<Vec<GaussianBeam>>,
}

impl ImageBeamSet {
    /// Creates an empty beam set.
    pub fn empty() -> Self {
        Self { beams: Vec::new() }
    }

    /// Creates a beam set with a single global beam.
    pub fn new(beam: GaussianBeam) -> Self {
        Self {
            beams: vec![vec![beam]],
        }
    }

    /// Creates a beam set from a channel × stokes grid.
    pub fn from_grid(beams: Vec<Vec<GaussianBeam>>) -> Self {
        if beams.is_empty() {
            return Self::empty();
        }
        let nstokes = beams[0].len();
        assert!(
            beams.iter().all(|row| row.len() == nstokes),
            "all beam rows must have the same stokes length"
        );
        Self { beams }
    }

    /// Creates a uniform beam set of the requested size.
    pub fn with_shape(nchan: usize, nstokes: usize, beam: GaussianBeam) -> Self {
        let nchan = nchan.max(1);
        let nstokes = nstokes.max(1);
        Self {
            beams: vec![vec![beam; nstokes]; nchan],
        }
    }

    /// Returns `true` if there are no beams.
    pub fn is_empty(&self) -> bool {
        self.beams.is_empty()
    }

    /// Returns `true` if exactly one beam applies globally.
    pub fn is_single(&self) -> bool {
        self.nelements() == 1
    }

    /// Alias for [`Self::is_single`] matching C++ `hasSingleBeam()`.
    pub fn has_single_beam(&self) -> bool {
        self.is_single()
    }

    /// Returns `true` if multiple per-plane beams are present.
    pub fn is_multi(&self) -> bool {
        self.nelements() > 1
    }

    /// Alias for [`Self::is_multi`] matching C++ `hasMultiBeam()`.
    pub fn has_multi_beam(&self) -> bool {
        self.is_multi()
    }

    /// Returns the number of beam elements.
    pub fn nelements(&self) -> usize {
        self.beams.iter().map(Vec::len).sum()
    }

    /// Alias for [`Self::nelements`] matching C++ `size()`.
    pub fn size(&self) -> usize {
        self.nelements()
    }

    /// Returns the beam-grid shape as `(nchan, nstokes)`.
    pub fn shape(&self) -> (usize, usize) {
        if self.is_empty() {
            (0, 0)
        } else {
            (self.beams.len(), self.beams[0].len())
        }
    }

    /// Returns the number of channels in the beam grid.
    pub fn n_channels(&self) -> usize {
        self.shape().0
    }

    /// Alias for [`Self::n_channels`] matching C++ `nchan()`.
    pub fn nchan(&self) -> usize {
        self.n_channels()
    }

    /// Returns the number of Stokes planes in the beam grid.
    pub fn n_stokes(&self) -> usize {
        self.shape().1
    }

    /// Alias for [`Self::n_stokes`] matching C++ `nstokes()`.
    pub fn nstokes(&self) -> usize {
        self.n_stokes()
    }

    /// Returns the single global beam, if present.
    pub fn single_beam(&self) -> Option<GaussianBeam> {
        self.is_single().then(|| self.beams[0][0])
    }

    /// Returns the single global beam, matching C++ `getBeam()`.
    pub fn get_beam(&self) -> Result<&GaussianBeam, ImageError> {
        if self.is_single() {
            Ok(&self.beams[0][0])
        } else {
            Err(ImageError::InvalidMetadata(
                "beam set does not contain exactly one beam".to_string(),
            ))
        }
    }

    /// Returns the beam for the given channel and stokes indices.
    ///
    /// Axis length 1 expands to all indices, matching casacore semantics.
    pub fn beam(&self, chan: usize, stokes: usize) -> &GaussianBeam {
        assert!(!self.is_empty(), "beam set is empty");
        let c = if self.n_channels() == 1 { 0 } else { chan };
        let s = if self.n_stokes() == 1 { 0 } else { stokes };
        &self.beams[c][s]
    }

    /// Sets all beams to the same value, collapsing to a single global beam.
    pub fn set_all(&mut self, beam: GaussianBeam) {
        self.beams = vec![vec![beam]];
    }

    /// Resizes the beam set, preserving existing values where possible.
    pub fn resize(&mut self, nchan: usize, nstokes: usize) {
        let fill = self
            .single_beam()
            .or_else(|| self.min_area_beam().copied())
            .unwrap_or_default();
        let nchan = nchan.max(1);
        let nstokes = nstokes.max(1);
        let mut resized = vec![vec![fill; nstokes]; nchan];
        for (chan, row) in resized.iter_mut().enumerate() {
            for (stokes, beam) in row.iter_mut().enumerate() {
                if !self.is_empty() && chan < self.n_channels() && stokes < self.n_stokes() {
                    *beam = self.beams[chan][stokes];
                }
            }
        }
        self.beams = resized;
    }

    /// Sets the beam at the given location.
    ///
    /// Passing `None` for either axis applies the change to all channels or
    /// all Stokes planes respectively. Passing `None` for both collapses the
    /// set to a single global beam.
    pub fn set_beam(
        &mut self,
        chan: Option<usize>,
        stokes: Option<usize>,
        beam: GaussianBeam,
    ) -> Result<(), ImageError> {
        if chan.is_none() && stokes.is_none() {
            self.set_all(beam);
            return Ok(());
        }
        if self.is_empty() {
            return Err(ImageError::InvalidMetadata(
                "cannot set a beam on an empty beam set".to_string(),
            ));
        }
        let chan_range: Vec<usize> = match chan {
            Some(c) => vec![c],
            None => (0..self.n_channels()).collect(),
        };
        let stokes_range: Vec<usize> = match stokes {
            Some(s) => vec![s],
            None => (0..self.n_stokes()).collect(),
        };
        for c in chan_range {
            for s in &stokes_range {
                if c >= self.n_channels() || *s >= self.n_stokes() {
                    return Err(ImageError::InvalidMetadata(
                        "beam index out of range".to_string(),
                    ));
                }
                self.beams[c][*s] = beam;
            }
        }
        Ok(())
    }

    /// Returns a subset of the beam grid using explicit channel and stokes selections.
    pub fn subset(&self, channels: &[usize], stokes: &[usize]) -> Result<Self, ImageError> {
        if self.is_empty() {
            return Ok(Self::empty());
        }
        let mut rows = Vec::with_capacity(channels.len());
        for &chan in channels {
            let mut row = Vec::with_capacity(stokes.len());
            for &stok in stokes {
                row.push(*self.beam(chan, stok));
            }
            rows.push(row);
        }
        Ok(Self::from_grid(rows))
    }

    /// Returns `true` if the two beam sets are equal after singleton expansion.
    pub fn equivalent(&self, other: &Self) -> bool {
        if self.is_empty() || other.is_empty() {
            return self.is_empty() && other.is_empty();
        }
        let nchan = self.n_channels().max(other.n_channels());
        let nstokes = self.n_stokes().max(other.n_stokes());
        for chan in 0..nchan {
            for stokes in 0..nstokes {
                if self.beam(chan, stokes) != other.beam(chan, stokes) {
                    return false;
                }
            }
        }
        true
    }

    /// Returns the beam with the minimum area, if any.
    pub fn min_area_beam(&self) -> Option<&GaussianBeam> {
        self.iter_beams()
            .min_by(|a, b| a.area().partial_cmp(&b.area()).unwrap())
    }

    /// Returns the beam with the maximum area, if any.
    pub fn max_area_beam(&self) -> Option<&GaussianBeam> {
        self.iter_beams()
            .max_by(|a, b| a.area().partial_cmp(&b.area()).unwrap())
    }

    /// Returns the beam with the median area, if any.
    pub fn median_area_beam(&self) -> Option<GaussianBeam> {
        let mut beams: Vec<GaussianBeam> = self.iter_beams().copied().collect();
        if beams.is_empty() {
            return None;
        }
        beams.sort_by(|a, b| a.area().partial_cmp(&b.area()).unwrap());
        Some(beams[beams.len() / 2])
    }

    /// Returns CASA's minimum-area common enclosing beam for this beam set.
    ///
    /// This ports the `CasaImageBeamSet::getCommonBeam()` algorithm used by
    /// CASA image analysis and synthesis restoration when
    /// `restoringbeam='common'`.
    pub fn common_beam(&self) -> Result<GaussianBeam, ImageError> {
        if self.is_empty() {
            return Err(ImageError::InvalidMetadata(
                "cannot determine a common beam for an empty beam set".to_string(),
            ));
        }
        // CASA traverses the channel-by-Stokes matrix with channel varying
        // fastest, while the stored grid groups Stokes by channel.
        let non_null: Vec<GaussianBeam> = (0..self.n_stokes())
            .flat_map(|stokes| {
                (0..self.n_channels()).map(move |channel| self.beams[channel][stokes])
            })
            .filter(|beam| !beam.is_null())
            .collect();
        if non_null.is_empty() {
            return Err(ImageError::InvalidMetadata(
                "cannot determine a common beam because all beams are null".to_string(),
            ));
        }
        if non_null.iter().all(|beam| *beam == non_null[0]) {
            return Ok(non_null[0]);
        }
        common_enclosing_gaussian(
            &non_null.into_iter().map(numeric_beam).collect::<Vec<_>>(),
            1.0,
        )
        .map(gaussian_beam)
        .map_err(|error| ImageError::InvalidMetadata(error.to_string()))
    }

    fn iter_beams(&self) -> impl Iterator<Item = &GaussianBeam> {
        self.beams.iter().flat_map(|row| row.iter())
    }

    /// Serializes the beam set using the casacore `ImageBeamSet::toRecord()` layout.
    pub fn to_record(&self) -> RecordValue {
        let mut rec = RecordValue::default();
        rec.upsert(
            "nChannels",
            Value::Scalar(ScalarValue::Int32(self.n_channels() as i32)),
        );
        rec.upsert(
            "nStokes",
            Value::Scalar(ScalarValue::Int32(self.n_stokes() as i32)),
        );
        let mut count = 0usize;
        for stokes in 0..self.n_stokes() {
            for chan in 0..self.n_channels() {
                rec.upsert(
                    format!("*{count}"),
                    Value::Record(self.beam(chan, stokes).to_record()),
                );
                count += 1;
            }
        }
        rec
    }

    /// Deserializes a beam set from the casacore `ImageBeamSet::fromRecord()` layout.
    pub fn from_record(rec: &RecordValue) -> Result<Self, ImageError> {
        let nchan = read_record_usize(rec, "nChannels")
            .ok_or_else(|| ImageError::InvalidMetadata("no valid nChannels field found".into()))?
            .max(1);
        let nstokes = read_record_usize(rec, "nStokes")
            .ok_or_else(|| ImageError::InvalidMetadata("no valid nStokes field found".into()))?
            .max(1);

        let mut rows = vec![vec![GaussianBeam::default(); nstokes]; nchan];
        for count in 0..(nchan * nstokes) {
            let key = format!("*{count}");
            let beam = if let Some(Value::Record(beam_rec)) = rec.get(&key) {
                GaussianBeam::from_record(beam_rec)?
            } else if let Some((chan, stokes)) = count_to_pair(nchan, nstokes, count) {
                let legacy_key = format!("*{chan}_{stokes}");
                match rec.get(&legacy_key) {
                    Some(Value::Record(beam_rec)) => GaussianBeam::from_record(beam_rec)?,
                    _ => {
                        return Err(ImageError::InvalidMetadata(format!(
                            "beam set: missing beam record '{key}'"
                        )));
                    }
                }
            } else {
                return Err(ImageError::InvalidMetadata(
                    "beam record index overflow".to_string(),
                ));
            };
            let chan = count % nchan;
            let stokes = count / nchan;
            rows[chan][stokes] = beam;
        }
        Ok(Self::from_grid(rows))
    }
}

fn read_record_usize(rec: &RecordValue, key: &str) -> Option<usize> {
    match rec.get(key) {
        Some(Value::Scalar(ScalarValue::UInt8(value))) => Some(usize::from(*value)),
        Some(Value::Scalar(ScalarValue::UInt16(value))) => Some(usize::from(*value)),
        Some(Value::Scalar(ScalarValue::UInt32(value))) => usize::try_from(*value).ok(),
        Some(Value::Scalar(ScalarValue::Int16(value))) => usize::try_from(*value).ok(),
        Some(Value::Scalar(ScalarValue::Int32(value))) => usize::try_from(*value).ok(),
        Some(Value::Scalar(ScalarValue::Int64(value))) => usize::try_from(*value).ok(),
        _ => None,
    }
}

impl Default for ImageBeamSet {
    fn default() -> Self {
        Self::empty()
    }
}

fn numeric_beam(beam: GaussianBeam) -> EllipticalGaussian {
    EllipticalGaussian::new(beam.major, beam.minor, beam.position_angle)
}

fn gaussian_beam(beam: EllipticalGaussian) -> GaussianBeam {
    GaussianBeam::new(beam.major, beam.minor, beam.position_angle)
}

#[cfg(test)]
fn arcsec_to_rad(arcsec: f64) -> f64 {
    arcsec * std::f64::consts::PI / (180.0 * 3600.0)
}

fn count_to_pair(nchan: usize, _nstokes: usize, count: usize) -> Option<(usize, usize)> {
    if nchan == 0 {
        None
    } else {
        Some((count % nchan, count / nchan))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use casa_numerics::gaussian_covariance;

    fn assert_beam_close(left: GaussianBeam, right: GaussianBeam) {
        assert!((left.major - right.major).abs() < 1e-15);
        assert!((left.minor - right.minor).abs() < 1e-15);
        assert!((left.position_angle - right.position_angle).abs() < 1e-15);
    }

    fn beam_arcsec(major: f64, minor: f64, pa_deg: f64) -> GaussianBeam {
        GaussianBeam::new(
            arcsec_to_rad(major),
            arcsec_to_rad(minor),
            pa_deg.to_radians(),
        )
    }

    #[test]
    fn stored_beam_edits_preserve_units_and_reject_invalid_axes() {
        let beam = StoredGaussianBeam::new(
            Quantity::new(1., "arcmin").unwrap(),
            Quantity::new(20., "arcsec").unwrap(),
            Quantity::new(120., "deg").unwrap(),
        )
        .unwrap();
        let updated = beam
            .with_component(BeamComponent::Major, Quantity::new(40., "arcsec").unwrap())
            .unwrap();
        let restored = StoredGaussianBeam::from_record(&updated.to_record()).unwrap();
        assert_eq!(
            restored.component(BeamComponent::Major).unit().name(),
            "arcsec"
        );
        assert_eq!(restored.component(BeamComponent::Minor).value(), 20.);
        assert_eq!(
            restored.component(BeamComponent::PositionAngle).value(),
            120.
        );
        assert!(
            beam.with_component(BeamComponent::Major, Quantity::new(10., "arcsec").unwrap())
                .is_err()
        );
        assert!(
            beam.with_component(
                BeamComponent::PositionAngle,
                Quantity::new(1., "Hz").unwrap()
            )
            .is_err()
        );
        assert_eq!(beam.component(BeamComponent::Major).unit().name(), "arcmin");
    }

    #[test]
    fn beam_position_angles_wrap_as_casacore_does() {
        for (value, expected) in [
            (-400., -40.),
            (-90., 90.),
            (90., 90.),
            (190., 10.),
            (270., 90.),
        ] {
            let quantity = Quantity::new(value, "deg").unwrap();
            assert_eq!(wrap_position_angle(&quantity).unwrap().value(), expected);
        }
        let radians = wrap_position_angle(&Quantity::new(2., "rad").unwrap()).unwrap();
        let degrees = casa_types::quanta::casacore::get_conformant_value(
            &radians,
            &Unit::new("deg").unwrap(),
        )
        .unwrap();
        assert!((degrees - (-65.40844097383535)).abs() < 1e-12);
    }

    #[test]
    fn gaussian_beam_area() {
        let beam = GaussianBeam::new(1e-4, 0.5e-4, 0.0);
        let expected = std::f64::consts::PI / (4.0 * 2.0_f64.ln()) * 1e-4 * 0.5e-4;
        assert!((beam.area() - expected).abs() < 1e-20);
    }

    #[test]
    fn beam_set_single() {
        let beam = GaussianBeam::new(1.0, 0.5, 0.0);
        let set = ImageBeamSet::new(beam);
        assert!(set.is_single());
        assert_eq!(set.single_beam(), Some(beam));
        assert_eq!(set.beam(3, 4), &beam);
    }

    #[test]
    fn beam_set_per_plane() {
        let beams = vec![
            vec![
                GaussianBeam::new(1.0, 0.5, 0.0),
                GaussianBeam::new(1.1, 0.5, 0.1),
            ],
            vec![
                GaussianBeam::new(1.2, 0.5, 0.2),
                GaussianBeam::new(1.3, 0.5, 0.3),
            ],
        ];
        let set = ImageBeamSet::from_grid(beams.clone());
        assert!(set.is_multi());
        assert_eq!(set.n_channels(), 2);
        assert_eq!(set.n_stokes(), 2);
        assert_eq!(set.beam(1, 1), &beams[1][1]);
    }

    #[test]
    fn resize_and_set_beam() {
        let mut set = ImageBeamSet::with_shape(2, 2, GaussianBeam::new(1.0, 0.5, 0.0));
        set.resize(3, 1);
        assert_eq!(set.shape(), (3, 1));
        set.set_beam(Some(2), Some(0), GaussianBeam::new(2.0, 1.0, 0.0))
            .unwrap();
        assert_eq!(set.beam(2, 0).major, 2.0);
    }

    #[test]
    fn equivalent_expands_singleton_axes() {
        let lhs = ImageBeamSet::new(GaussianBeam::new(1.0, 0.5, 0.0));
        let rhs = ImageBeamSet::with_shape(2, 3, GaussianBeam::new(1.0, 0.5, 0.0));
        assert!(lhs.equivalent(&rhs));
    }

    #[test]
    fn subset_and_area_queries_work() {
        let set = ImageBeamSet::from_grid(vec![
            vec![GaussianBeam::new(1.0, 0.5, 0.0)],
            vec![GaussianBeam::new(2.0, 0.5, 0.0)],
            vec![GaussianBeam::new(1.5, 0.5, 0.0)],
        ]);
        let subset = set.subset(&[1, 2], &[0]).unwrap();
        assert_eq!(subset.shape(), (2, 1));
        assert_eq!(set.min_area_beam().unwrap().major, 1.0);
        assert_eq!(set.max_area_beam().unwrap().major, 2.0);
        assert_eq!(set.median_area_beam().unwrap().major, 1.5);
    }

    #[test]
    fn beam_record_round_trip() {
        let beam = GaussianBeam::new(1e-4, 5e-5, 0.3);
        let record = beam.to_record();
        let back = GaussianBeam::from_record(&record).unwrap();
        assert!((beam.major - back.major).abs() < 1e-18);
        assert!((beam.minor - back.minor).abs() < 1e-18);
        assert!((beam.position_angle - back.position_angle).abs() < 1e-15);
        let quantity_unit = |name| match record.get(name) {
            Some(Value::Record(quantity)) => match quantity.get("unit") {
                Some(Value::Scalar(ScalarValue::String(unit))) => unit.as_str(),
                _ => panic!("{name} quantity is missing its unit"),
            },
            _ => panic!("beam record is missing {name}"),
        };
        assert_eq!(quantity_unit("major"), "arcsec");
        assert_eq!(quantity_unit("minor"), "arcsec");
        assert_eq!(quantity_unit("positionangle"), "deg");
    }

    #[test]
    fn beam_record_parses_quantity_units() {
        fn quantity_record(value: f64, unit: &str) -> RecordValue {
            RecordValue::new(vec![
                RecordField::new("value", Value::Scalar(ScalarValue::Float64(value))),
                RecordField::new("unit", Value::Scalar(ScalarValue::String(unit.into()))),
            ])
        }

        let record = RecordValue::new(vec![
            RecordField::new("major", Value::Record(quantity_record(3.5, "arcsec"))),
            RecordField::new("minor", Value::Record(quantity_record(2.25, "arcsec"))),
            RecordField::new(
                "positionangle",
                Value::Record(quantity_record(171.3, "deg")),
            ),
        ]);

        let beam = GaussianBeam::from_record(&record).unwrap();
        assert!((beam.major_in("arcsec").unwrap() - 3.5).abs() < 1e-10);
        assert!((beam.minor_in("arcsec").unwrap() - 2.25).abs() < 1e-10);
        assert!((beam.position_angle_in("deg").unwrap() - 171.3).abs() < 1e-10);
    }

    #[test]
    fn beam_set_record_round_trip() {
        let beams = ImageBeamSet::from_grid(vec![
            vec![
                GaussianBeam::new(1.0, 0.5, 0.0),
                GaussianBeam::new(1.1, 0.5, 0.1),
            ],
            vec![
                GaussianBeam::new(1.2, 0.5, 0.2),
                GaussianBeam::new(1.3, 0.5, 0.3),
            ],
        ]);
        let back = ImageBeamSet::from_record(&beams.to_record()).unwrap();
        assert_eq!(beams.shape(), back.shape());
        for channel in 0..beams.n_channels() {
            for stokes in 0..beams.n_stokes() {
                assert_beam_close(*beams.beam(channel, stokes), *back.beam(channel, stokes));
            }
        }
    }

    #[test]
    fn beam_set_record_accepts_non_int32_shape_fields() {
        let beam = beam_arcsec(3.0, 2.0, -10.0);
        let mut record = RecordValue::default();
        record.upsert("nChannels", Value::Scalar(ScalarValue::Int64(2)));
        record.upsert("nStokes", Value::Scalar(ScalarValue::UInt32(1)));
        record.upsert("*0", Value::Record(beam.to_record()));
        record.upsert("*1", Value::Record(beam.to_record()));
        let beam_set = ImageBeamSet::from_record(&record).unwrap();
        assert_eq!(beam_set.shape(), (2, 1));
        assert_eq!(beam_set.size(), 2);
    }

    #[test]
    fn aliases_default_and_error_paths_work() {
        let beam = GaussianBeam::new(1.0, 0.5, 0.25);
        assert!(!beam.is_null());
        assert!(GaussianBeam::default().is_null());
        assert!(GaussianBeam::new(1.0, 0.0, 0.0).is_null());
        assert!(GaussianBeam::new(0.0, 1.0, 0.0).is_null());

        let empty = ImageBeamSet::default();
        assert!(empty.is_empty());
        assert_eq!(empty.shape(), (0, 0));
        assert_eq!(empty.min_area_beam(), None);
        assert_eq!(empty.max_area_beam(), None);
        assert_eq!(empty.median_area_beam(), None);
        assert!(empty.subset(&[], &[]).unwrap().is_empty());
        assert!(matches!(
            ImageBeamSet::empty().set_beam(Some(0), Some(0), beam),
            Err(ImageError::InvalidMetadata(_))
        ));

        let multi = ImageBeamSet::with_shape(2, 3, beam);
        assert!(multi.has_multi_beam());
        assert!(!multi.has_single_beam());
        assert_eq!(multi.size(), 6);
        assert_eq!(multi.nchan(), 2);
        assert_eq!(multi.nstokes(), 3);
        assert!(matches!(
            multi.get_beam(),
            Err(ImageError::InvalidMetadata(_))
        ));
    }

    #[test]
    fn set_beam_broadcast_and_record_error_paths_work() {
        let mut set = ImageBeamSet::with_shape(2, 2, GaussianBeam::new(1.0, 0.5, 0.0));
        let global = GaussianBeam::new(3.0, 1.5, 0.1);
        set.set_beam(None, None, global).unwrap();
        assert_eq!(set.single_beam(), Some(global));

        set.resize(2, 2);
        let per_stokes = GaussianBeam::new(2.0, 1.0, 0.0);
        set.set_beam(None, Some(1), per_stokes).unwrap();
        assert_eq!(*set.beam(0, 1), per_stokes);
        assert_eq!(*set.beam(1, 1), per_stokes);

        assert!(matches!(
            set.set_beam(Some(9), Some(0), per_stokes),
            Err(ImageError::InvalidMetadata(_))
        ));

        let mut legacy = RecordValue::default();
        legacy.upsert("nChannels", Value::Scalar(ScalarValue::Int32(1)));
        legacy.upsert("nStokes", Value::Scalar(ScalarValue::Int32(1)));
        legacy.upsert("*0_0", Value::Record(global.to_record()));
        let parsed = ImageBeamSet::from_record(&legacy).unwrap();
        assert_beam_close(parsed.single_beam().unwrap(), global);

        let mut missing = RecordValue::default();
        missing.upsert("nChannels", Value::Scalar(ScalarValue::Int32(1)));
        missing.upsert("nStokes", Value::Scalar(ScalarValue::Int32(1)));
        assert!(matches!(
            ImageBeamSet::from_record(&missing),
            Err(ImageError::InvalidMetadata(msg)) if msg.contains("missing beam record")
        ));
    }

    #[test]
    fn common_beam_returns_existing_largest_beam_when_it_encloses_others() {
        let set = ImageBeamSet::from_grid(vec![vec![
            beam_arcsec(4.0, 2.0, 0.0),
            beam_arcsec(1.5, 1.0, 90.0),
        ]]);
        let common = set.common_beam().unwrap();
        assert!((common.major_in("arcsec").unwrap() - 4.0).abs() < 1.0e-10);
        assert!((common.minor_in("arcsec").unwrap() - 2.0).abs() < 1.0e-10);
        assert!((common.position_angle_in("deg").unwrap() - 0.0).abs() < 1.0e-10);
    }

    #[test]
    fn common_beam_handles_right_angle_case_like_casa() {
        let set = ImageBeamSet::from_grid(vec![vec![
            beam_arcsec(4.0, 2.0, 0.0),
            beam_arcsec(4.0, 2.0, 90.0),
        ]]);
        let common = set.common_beam().unwrap();
        assert!((common.major_in("arcsec").unwrap() - 4.0).abs() < 1.0e-10);
        assert!((common.minor_in("arcsec").unwrap() - 4.0).abs() < 1.0e-10);
    }

    /// CASA 6.7.6.14 commonbeam for a two-Stokes image, whose underlying
    /// channel-fast beam matrix matters when several ellipses need enclosing.
    #[test]
    fn common_beam_follows_casas_multistokes_matrix_order() {
        let beams = [
            GaussianBeam::new(
                0.000_659_920_363_505_466_7,
                0.000_277_714_375_479_345_24,
                -1.006_629_886_598_266_8,
            ),
            GaussianBeam::new(
                0.000_489_199_949_523_298_2,
                0.000_332_711_701_991_262_23,
                0.534_970_100_282_602_5,
            ),
            GaussianBeam::new(
                0.000_575_668_982_898_358_4,
                0.000_251_585_111_889_876_9,
                0.498_537_038_391_696_7,
            ),
            GaussianBeam::new(
                0.000_445_553_375_482_662,
                0.000_232_902_929_545_132_06,
                0.677_320_743_101_792_8,
            ),
        ];
        let mut grid = vec![vec![beams[3]; 2]; 10];
        grid[0] = vec![beams[0], beams[1]];
        grid[1][0] = beams[2];
        let actual = ImageBeamSet::from_grid(grid).common_beam().unwrap();
        let arcsec = std::f64::consts::PI / (180.0 * 3_600.0);
        assert!(
            (actual.major / arcsec - 137.588_692_242_4).abs() < 1.0e-4,
            "{actual:?}"
        );
        assert!(
            (actual.minor / arcsec - 118.268_296_464_4).abs() < 1.0e-4,
            "{actual:?}"
        );
        assert!(
            (actual.position_angle.to_degrees() + 49.863_390_056_5).abs() < 1.0e-4,
            "{actual:?}"
        );
    }

    #[test]
    fn common_beam_matches_casa_two_beam_reference_case() {
        let set = ImageBeamSet::from_grid(vec![vec![
            beam_arcsec(4.0, 2.0, 0.0),
            beam_arcsec(4.0, 2.0, 60.0),
        ]]);
        let common = set.common_beam().unwrap();
        assert!((common.position_angle_in("deg").unwrap() - 30.0).abs() < 1.0e-6);
        assert!(common.major_in("arcsec").unwrap() < 4.6);
        assert!(common.minor_in("arcsec").unwrap() < 3.4);
    }

    #[test]
    fn deconvolving_beam_returns_none_for_identical_beams() {
        let beam = beam_arcsec(4.0, 2.0, 30.0);
        assert_eq!(beam.deconvolving_beam(beam).unwrap(), None);
    }

    #[test]
    fn deconvolving_beam_round_trips_covariance_difference() {
        let source = beam_arcsec(4.0, 2.5, 10.0);
        let target = beam_arcsec(5.0, 3.5, 25.0);
        let delta = target.deconvolving_beam(source).unwrap().unwrap();
        let recombined = [
            [
                gaussian_covariance(numeric_beam(source))[0][0]
                    + gaussian_covariance(numeric_beam(delta))[0][0],
                gaussian_covariance(numeric_beam(source))[0][1]
                    + gaussian_covariance(numeric_beam(delta))[0][1],
            ],
            [
                gaussian_covariance(numeric_beam(source))[1][0]
                    + gaussian_covariance(numeric_beam(delta))[1][0],
                gaussian_covariance(numeric_beam(source))[1][1]
                    + gaussian_covariance(numeric_beam(delta))[1][1],
            ],
        ];
        let expected = gaussian_covariance(numeric_beam(target));
        for row in 0..2 {
            for col in 0..2 {
                assert!((recombined[row][col] - expected[row][col]).abs() < 1.0e-12);
            }
        }
    }
}

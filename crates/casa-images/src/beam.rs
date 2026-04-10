// SPDX-License-Identifier: LGPL-3.0-or-later
//! Beam metadata corresponding to C++ `GaussianBeam` and `ImageBeamSet`.

use casa_types::quanta::{Quantity, Unit};
use casa_types::{RecordField, RecordValue, ScalarValue, Value};

use crate::error::ImageError;

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
        self.major == 0.0 && self.minor == 0.0
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
        fn quantity_record(value: f64) -> RecordValue {
            RecordValue::new(vec![
                RecordField::new("value", Value::Scalar(ScalarValue::Float64(value))),
                RecordField::new("unit", Value::Scalar(ScalarValue::String("rad".into()))),
            ])
        }

        RecordValue::new(vec![
            RecordField::new("major", Value::Record(quantity_record(self.major))),
            RecordField::new("minor", Value::Record(quantity_record(self.minor))),
            RecordField::new(
                "positionangle",
                Value::Record(quantity_record(self.position_angle)),
            ),
        ])
    }

    /// Deserializes a beam from a casacore quantity record.
    pub fn from_record(rec: &RecordValue) -> Result<Self, ImageError> {
        fn read_quantity(rec: &RecordValue, key: &str) -> Result<f64, ImageError> {
            match rec.get(key) {
                Some(Value::Record(sub)) => {
                    let value = match sub.get("value") {
                        Some(Value::Scalar(ScalarValue::Float64(v))) => *v,
                        Some(Value::Scalar(ScalarValue::Float32(v))) => f64::from(*v),
                        Some(Value::Scalar(ScalarValue::Int32(v))) => f64::from(*v),
                        _ => {
                            return Err(ImageError::InvalidMetadata(format!(
                                "beam {key}: missing or invalid value field"
                            )));
                        }
                    };
                    let unit = match sub.get("unit") {
                        Some(Value::Scalar(ScalarValue::String(unit))) => unit.as_str(),
                        _ => {
                            return Err(ImageError::InvalidMetadata(format!(
                                "beam {key}: missing or invalid unit field"
                            )));
                        }
                    };
                    let quantity = Quantity::new(value, unit).map_err(|err| {
                        ImageError::InvalidMetadata(format!(
                            "beam {key}: invalid quantity unit '{unit}': {err}"
                        ))
                    })?;
                    let radians = Unit::new("rad").expect("built-in radians unit must parse");
                    quantity.get_value_in(&radians).map_err(|err| {
                        ImageError::InvalidMetadata(format!(
                            "beam {key}: expected angular quantity, got '{unit}': {err}"
                        ))
                    })
                }
                _ => Err(ImageError::InvalidMetadata(format!(
                    "beam: missing '{key}' sub-record"
                ))),
            }
        }

        Ok(Self {
            major: read_quantity(rec, "major")?,
            minor: read_quantity(rec, "minor")?,
            position_angle: read_quantity(rec, "positionangle")?,
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
        let lhs = covariance_matrix(self);
        let rhs = covariance_matrix(other);
        let delta = [
            [lhs[0][0] - rhs[0][0], lhs[0][1] - rhs[0][1]],
            [lhs[1][0] - rhs[1][0], lhs[1][1] - rhs[1][1]],
        ];
        let trace = delta[0][0] + delta[1][1];
        let determinant = delta[0][0] * delta[1][1] - delta[0][1] * delta[1][0];
        let discriminant = ((trace * trace) / 4.0 - determinant).max(0.0).sqrt();
        let lambda_major = trace / 2.0 + discriminant;
        let lambda_minor = trace / 2.0 - discriminant;
        if lambda_major <= 1.0e-24 && lambda_minor <= 1.0e-24 {
            return Ok(None);
        }
        if lambda_minor < -1.0e-12 || lambda_major < -1.0e-12 {
            return Err(ImageError::InvalidMetadata(
                "target beam is smaller than the source beam".to_string(),
            ));
        }
        let lambda_major = lambda_major.max(0.0);
        let lambda_minor = lambda_minor.max(0.0);
        let x_axis_angle =
            if delta[0][1].abs() <= 1.0e-18 && (delta[0][0] - delta[1][1]).abs() <= 1.0e-18 {
                0.0
            } else {
                0.5 * (2.0 * delta[0][1]).atan2(delta[0][0] - delta[1][1])
            };
        Ok(Some(beam_from_x_axis_rad(
            lambda_major.sqrt(),
            lambda_minor.sqrt(),
            x_axis_angle,
        )))
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
        let non_null: Vec<GaussianBeam> = self
            .iter_beams()
            .copied()
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
        common_beam_recursive(&non_null)
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
        let mut nchan = read_record_usize(rec, "nChannels").unwrap_or(1).max(1);
        let mut nstokes = read_record_usize(rec, "nStokes").unwrap_or(1).max(1);
        if nchan == 0 {
            nchan = 1;
        }
        if nstokes == 0 {
            nstokes = 1;
        }

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

fn common_beam_recursive(beams: &[GaussianBeam]) -> Result<GaussianBeam, ImageError> {
    let (max_index, &max_beam) = beams
        .iter()
        .enumerate()
        .max_by(|(_, lhs), (_, rhs)| lhs.area().partial_cmp(&rhs.area()).unwrap())
        .ok_or_else(|| ImageError::InvalidMetadata("beam set is empty".to_string()))?;

    let mut problem_beam = None;
    for (index, beam) in beams.iter().copied().enumerate() {
        if index != max_index && !beam.is_null() && !beam_encloses(max_beam, beam) {
            problem_beam = Some(beam);
        }
    }
    let Some(problem_beam) = problem_beam else {
        return Ok(max_beam);
    };

    let t_b1 = normalize_beam_position_angle(problem_beam.position_angle)
        - normalize_beam_position_angle(max_beam.position_angle);
    if (normalize_angle_pi(t_b1).abs() - std::f64::consts::FRAC_PI_2).abs() <= 1.0e-12 {
        let max_has_major = max_beam.major_in("arcsec")? >= problem_beam.major_in("arcsec")?;
        let major_arcsec = if max_has_major {
            max_beam.major_in("arcsec")?
        } else {
            problem_beam.major_in("arcsec")?
        };
        let minor_arcsec = if max_has_major {
            problem_beam.major_in("arcsec")?
        } else {
            max_beam.major_in("arcsec")?
        };
        let pa = if max_has_major {
            max_beam.position_angle
        } else {
            problem_beam.position_angle
        };
        return Ok(GaussianBeam::new(
            arcsec_to_rad(major_arcsec),
            arcsec_to_rad(minor_arcsec),
            normalize_beam_position_angle(pa),
        ));
    }

    let a_a1 = max_beam.major_in("arcsec")?;
    let b_a1 = max_beam.minor_in("arcsec")?;
    let a_b1 = problem_beam.major_in("arcsec")?;
    let b_b1 = problem_beam.minor_in("arcsec")?;

    let a_a2 = (a_a1 * b_a1).sqrt();
    let p = a_a2 / a_a1;
    let q = a_a2 / b_a1;

    let (a_b2, _b_b2, t_b2) = transform_ellipse_by_scaling(a_b1, b_b1, t_b1, p, q);
    let (mut a_c, mut b_c, t_c1) = transform_ellipse_by_scaling(a_b2, a_a2, t_b2, 1.0 / p, 1.0 / q);
    let t_c = t_c1 + normalize_beam_position_angle(max_beam.position_angle);

    let mut enclosing = GaussianBeam::new(
        arcsec_to_rad(a_c),
        arcsec_to_rad(b_c),
        normalize_beam_position_angle(t_c),
    );
    while !(beam_encloses(enclosing, max_beam) && beam_encloses(enclosing, problem_beam)) {
        a_c *= 1.001;
        b_c *= 1.001;
        enclosing = GaussianBeam::new(
            arcsec_to_rad(a_c),
            arcsec_to_rad(b_c),
            normalize_beam_position_angle(t_c),
        );
    }

    let mut new_beams = beams.to_vec();
    new_beams[max_index] = enclosing;
    common_beam_recursive(&new_beams)
}

fn beam_from_x_axis_rad(major_rad: f64, minor_rad: f64, x_axis_angle_rad: f64) -> GaussianBeam {
    GaussianBeam::new(
        major_rad,
        minor_rad,
        normalize_beam_position_angle(x_axis_angle_rad - std::f64::consts::FRAC_PI_2),
    )
}

fn normalize_beam_position_angle(angle: f64) -> f64 {
    let mut wrapped = normalize_angle_pi(angle);
    if wrapped <= -std::f64::consts::FRAC_PI_2 {
        wrapped += std::f64::consts::PI;
    } else if wrapped > std::f64::consts::FRAC_PI_2 {
        wrapped -= std::f64::consts::PI;
    }
    wrapped
}

fn normalize_angle_pi(angle: f64) -> f64 {
    let mut wrapped = angle.rem_euclid(2.0 * std::f64::consts::PI);
    if wrapped > std::f64::consts::PI {
        wrapped -= 2.0 * std::f64::consts::PI;
    }
    wrapped
}

fn x_axis_angle_rad(beam: GaussianBeam) -> f64 {
    normalize_beam_position_angle(beam.position_angle) + std::f64::consts::FRAC_PI_2
}

fn beam_encloses(enclosing: GaussianBeam, other: GaussianBeam) -> bool {
    // Match CASA `GaussianDeconvolver::deconvolve()`: a candidate enclosing
    // beam is only accepted if deconvolution succeeds and does not collapse to
    // the "point source" branch. `CasaImageBeamSet::getCommonBeam()` treats the
    // point-source result as failure and grows the common beam slightly.
    let major_source = enclosing.major;
    let minor_source = enclosing.minor;
    let theta_source = normalize_beam_position_angle(enclosing.position_angle);
    let major_beam = other.major;
    let minor_beam = other.minor;
    let theta_beam = normalize_beam_position_angle(other.position_angle);

    let alpha = (major_source * theta_source.cos()).powi(2)
        + (minor_source * theta_source.sin()).powi(2)
        - (major_beam * theta_beam.cos()).powi(2)
        - (minor_beam * theta_beam.sin()).powi(2);
    let beta = (major_source * theta_source.sin()).powi(2)
        + (minor_source * theta_source.cos()).powi(2)
        - (major_beam * theta_beam.sin()).powi(2)
        - (minor_beam * theta_beam.cos()).powi(2);
    let gamma = 2.0
        * (((minor_source * minor_source) - (major_source * major_source))
            * theta_source.sin()
            * theta_source.cos()
            - ((minor_beam * minor_beam) - (major_beam * major_beam))
                * theta_beam.sin()
                * theta_beam.cos());

    let s = alpha + beta;
    let t = ((alpha - beta).powi(2) + gamma.powi(2)).sqrt();
    alpha >= 0.0 && beta >= 0.0 && s >= t
}

fn covariance_matrix(beam: GaussianBeam) -> [[f64; 2]; 2] {
    let angle = x_axis_angle_rad(beam);
    let cos = angle.cos();
    let sin = angle.sin();
    let major2 = beam.major * beam.major;
    let minor2 = beam.minor * beam.minor;
    [
        [
            cos * cos * major2 + sin * sin * minor2,
            cos * sin * (major2 - minor2),
        ],
        [
            cos * sin * (major2 - minor2),
            sin * sin * major2 + cos * cos * minor2,
        ],
    ]
}

fn transform_ellipse_by_scaling(
    major: f64,
    minor: f64,
    pa: f64,
    x_scale_factor: f64,
    y_scale_factor: f64,
) -> (f64, f64, f64) {
    let my_cos = pa.cos();
    let my_sin = pa.sin();
    let cos2 = my_cos * my_cos;
    let sin2 = my_sin * my_sin;
    let major2 = major * major;
    let minor2 = minor * minor;
    let a = cos2 / major2 + sin2 / minor2;
    let b = -2.0 * my_cos * my_sin * (1.0 / major2 - 1.0 / minor2);
    let c = sin2 / major2 + cos2 / minor2;

    let xs = x_scale_factor * x_scale_factor;
    let ys = y_scale_factor * y_scale_factor;

    let r = a / xs;
    let s = b * b / (4.0 * xs * ys);
    let t = c / ys;

    let u = r - t;
    let f1 = u * u + 4.0 * s;
    let f2 = f1.sqrt() * u.abs();

    let j1 = (f2 + f1) / f1 / 2.0;
    let j2 = (-f2 + f1) / f1 / 2.0;

    let k1 = (j1 * r + j1 * t - t) / (2.0 * j1 - 1.0);
    let k2 = (j2 * r + j2 * t - t) / (2.0 * j2 - 1.0);

    let c1 = (1.0 / k1).sqrt();
    let c2 = (1.0 / k2).sqrt();

    if (c1 - c2).abs() <= 1.0e-12 {
        return (k1.sqrt(), k1.sqrt(), 0.0);
    }
    if c1 > c2 {
        (
            c1,
            c2,
            if pa >= 0.0 {
                j1.sqrt().acos()
            } else {
                -j1.sqrt().acos()
            },
        )
    } else {
        (
            c2,
            c1,
            if pa >= 0.0 {
                j2.sqrt().acos()
            } else {
                -j2.sqrt().acos()
            },
        )
    }
}

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

    fn beam_arcsec(major: f64, minor: f64, pa_deg: f64) -> GaussianBeam {
        GaussianBeam::new(
            arcsec_to_rad(major),
            arcsec_to_rad(minor),
            pa_deg.to_radians(),
        )
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
        let back = GaussianBeam::from_record(&beam.to_record()).unwrap();
        assert_eq!(beam, back);
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
        assert_eq!(beams, back);
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
        assert_eq!(parsed.single_beam(), Some(global));

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
                covariance_matrix(source)[0][0] + covariance_matrix(delta)[0][0],
                covariance_matrix(source)[0][1] + covariance_matrix(delta)[0][1],
            ],
            [
                covariance_matrix(source)[1][0] + covariance_matrix(delta)[1][0],
                covariance_matrix(source)[1][1] + covariance_matrix(delta)[1][1],
            ],
        ];
        let expected = covariance_matrix(target);
        for row in 0..2 {
            for col in 0..2 {
                assert!((recombined[row][col] - expected[row][col]).abs() < 1.0e-12);
            }
        }
    }
}

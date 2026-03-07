// SPDX-License-Identifier: LGPL-3.0-or-later
//! Beam metadata corresponding to C++ `GaussianBeam` and `ImageBeamSet`.

use casacore_types::{RecordField, RecordValue, ScalarValue, Value};

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
                Some(Value::Record(sub)) => match sub.get("value") {
                    Some(Value::Scalar(ScalarValue::Float64(v))) => Ok(*v),
                    _ => Err(ImageError::InvalidMetadata(format!(
                        "beam {key}: missing or invalid value field"
                    ))),
                },
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
        let mut nchan = match rec.get("nChannels") {
            Some(Value::Scalar(ScalarValue::Int32(n))) => (*n).max(1) as usize,
            _ => 1,
        };
        let mut nstokes = match rec.get("nStokes") {
            Some(Value::Scalar(ScalarValue::Int32(n))) => (*n).max(1) as usize,
            _ => 1,
        };
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

impl Default for ImageBeamSet {
    fn default() -> Self {
        Self::empty()
    }
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
}

// SPDX-License-Identifier: LGPL-3.0-or-later
//! Coordinate system: container of coordinates with axis mapping.
//!
//! [`CoordinateSystem`] is the top-level object that groups multiple
//! [`Coordinate`] objects into a single system describing a multi-dimensional
//! data cube. Each coordinate owns a contiguous range of pixel and world
//! axes.
//!
//! This corresponds to C++ `CoordinateSystem`.

use casacore_types::{RecordValue, ScalarValue, Value};

use crate::coordinate::{Coordinate, CoordinateType};
use crate::direction::DirectionCoordinate;
use crate::error::CoordinateError;
use crate::linear::LinearCoordinate;
use crate::obs_info::ObsInfo;
use crate::record_utils::{get_optional_i32, get_optional_string};
use crate::spectral::SpectralCoordinate;
use crate::stokes::StokesCoordinate;
use crate::tabular::TabularCoordinate;

/// A collection of coordinates describing a multi-dimensional data cube.
///
/// Each coordinate owns a contiguous set of pixel and world axes. The
/// [`CoordinateSystem`] concatenates them in order: the first coordinate's
/// axes come first, then the second's, and so on.
///
/// Corresponds to C++ `CoordinateSystem`.
#[derive(Debug, Clone)]
pub struct CoordinateSystem {
    coordinates: Vec<Box<dyn Coordinate>>,
    obs_info: ObsInfo,
}

impl CoordinateSystem {
    /// Creates an empty coordinate system with default observation info.
    pub fn new() -> Self {
        Self {
            coordinates: Vec::new(),
            obs_info: ObsInfo::default(),
        }
    }

    /// Adds a coordinate to the system.
    ///
    /// The new coordinate's axes are appended after all existing axes.
    pub fn add_coordinate(&mut self, coord: Box<dyn Coordinate>) {
        self.coordinates.push(coord);
    }

    /// Returns the number of coordinates in the system.
    pub fn n_coordinates(&self) -> usize {
        self.coordinates.len()
    }

    /// Returns a reference to the coordinate at the given index.
    ///
    /// # Panics
    ///
    /// Panics if `index >= n_coordinates()`.
    pub fn coordinate(&self, index: usize) -> &dyn Coordinate {
        &*self.coordinates[index]
    }

    /// Returns the total number of pixel axes across all coordinates.
    pub fn n_pixel_axes(&self) -> usize {
        self.coordinates.iter().map(|c| c.n_pixel_axes()).sum()
    }

    /// Returns the total number of world axes across all coordinates.
    pub fn n_world_axes(&self) -> usize {
        self.coordinates.iter().map(|c| c.n_world_axes()).sum()
    }

    /// Finds the index of the first coordinate with the given type.
    ///
    /// Returns `None` if no coordinate of that type exists.
    pub fn find_coordinate(&self, coord_type: CoordinateType) -> Option<usize> {
        self.coordinates
            .iter()
            .position(|c| c.coordinate_type() == coord_type)
    }

    /// Returns a reference to the observation info.
    pub fn obs_info(&self) -> &ObsInfo {
        &self.obs_info
    }

    /// Returns a mutable reference to the observation info.
    pub fn obs_info_mut(&mut self) -> &mut ObsInfo {
        &mut self.obs_info
    }

    /// Sets the observation info. Returns `self` for chaining.
    pub fn with_obs_info(mut self, obs_info: ObsInfo) -> Self {
        self.obs_info = obs_info;
        self
    }

    /// Converts a full pixel coordinate vector to world coordinates.
    ///
    /// The pixel vector must have length [`n_pixel_axes`](Self::n_pixel_axes).
    /// Each coordinate converts its own slice of the pixel vector.
    pub fn to_world(&self, pixel: &[f64]) -> Result<Vec<f64>, CoordinateError> {
        let n_pix = self.n_pixel_axes();
        if pixel.len() != n_pix {
            return Err(CoordinateError::DimensionMismatch {
                expected: n_pix,
                got: pixel.len(),
            });
        }

        let mut world = Vec::with_capacity(self.n_world_axes());
        let mut pix_offset = 0;

        for coord in &self.coordinates {
            let np = coord.n_pixel_axes();
            let pix_slice = &pixel[pix_offset..pix_offset + np];
            let w = coord.to_world(pix_slice)?;
            world.extend(w);
            pix_offset += np;
        }

        Ok(world)
    }

    /// Converts a full world coordinate vector to pixel coordinates.
    ///
    /// The world vector must have length [`n_world_axes`](Self::n_world_axes).
    pub fn to_pixel(&self, world: &[f64]) -> Result<Vec<f64>, CoordinateError> {
        let n_world = self.n_world_axes();
        if world.len() != n_world {
            return Err(CoordinateError::DimensionMismatch {
                expected: n_world,
                got: world.len(),
            });
        }

        let mut pixel = Vec::with_capacity(self.n_pixel_axes());
        let mut world_offset = 0;

        for coord in &self.coordinates {
            let nw = coord.n_world_axes();
            let world_slice = &world[world_offset..world_offset + nw];
            let p = coord.to_pixel(world_slice)?;
            pixel.extend(p);
            world_offset += nw;
        }

        Ok(pixel)
    }

    /// Serializes the coordinate system to a casacore-compatible record.
    ///
    /// The record contains an `obsinfo` sub-record and one sub-record per
    /// coordinate (named `coordinate0`, `coordinate1`, etc.).
    pub fn to_record(&self) -> RecordValue {
        let mut rec = RecordValue::default();

        rec.upsert(
            "ncoordinates",
            Value::Scalar(ScalarValue::Int32(self.coordinates.len() as i32)),
        );

        for (i, coord) in self.coordinates.iter().enumerate() {
            let key = format!("coordinate{i}");
            rec.upsert(&key, Value::Record(coord.to_record()));
        }

        rec.upsert("obsinfo", Value::Record(self.obs_info.to_record()));

        rec
    }

    /// Deserializes a coordinate system from a casacore-compatible record.
    ///
    /// Unknown or malformed coordinate sub-records are skipped so persisted
    /// images can still reopen in pixel-only mode.
    pub fn from_record(rec: &RecordValue) -> Result<Self, CoordinateError> {
        let obs_info = match rec.get("obsinfo") {
            Some(Value::Record(obs_rec)) => ObsInfo::from_record(obs_rec)?,
            _ => ObsInfo::default(),
        };

        let mut coordinates = Vec::new();
        for index in coordinate_record_indices(rec) {
            let key = format!("coordinate{index}");
            let Some(Value::Record(coord_rec)) = rec.get(&key) else {
                continue;
            };
            if let Some(coord) = parse_coordinate_record(coord_rec) {
                coordinates.push(coord);
            }
        }

        Ok(Self {
            coordinates,
            obs_info,
        })
    }
}

impl Default for CoordinateSystem {
    fn default() -> Self {
        Self::new()
    }
}

fn coordinate_record_indices(rec: &RecordValue) -> Vec<usize> {
    let mut indices = rec
        .fields()
        .iter()
        .filter_map(|field| field.name.strip_prefix("coordinate")?.parse::<usize>().ok())
        .collect::<Vec<_>>();
    indices.sort_unstable();
    indices.dedup();

    if indices.is_empty() {
        if let Some(ncoordinates) = get_optional_i32(rec, "ncoordinates") {
            return (0..usize::try_from(ncoordinates).unwrap_or_default()).collect();
        }
    }

    indices
}

fn parse_coordinate_record(rec: &RecordValue) -> Option<Box<dyn Coordinate>> {
    let coord_type = get_optional_string(rec, "coordinate_type")?;
    match coord_type.as_str() {
        "Linear" => LinearCoordinate::from_record(rec)
            .ok()
            .map(|coord| Box::new(coord) as Box<dyn Coordinate>),
        "Direction" => DirectionCoordinate::from_record(rec)
            .ok()
            .map(|coord| Box::new(coord) as Box<dyn Coordinate>),
        "Spectral" => SpectralCoordinate::from_record(rec)
            .ok()
            .map(|coord| Box::new(coord) as Box<dyn Coordinate>),
        "Stokes" => StokesCoordinate::from_record(rec)
            .ok()
            .map(|coord| Box::new(coord) as Box<dyn Coordinate>),
        "Tabular" => TabularCoordinate::from_record(rec)
            .ok()
            .map(|coord| Box::new(coord) as Box<dyn Coordinate>),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::direction::DirectionCoordinate;
    use crate::linear::LinearCoordinate;
    use crate::projection::{Projection, ProjectionType};
    use crate::spectral::SpectralCoordinate;
    use crate::stokes::{StokesCoordinate, StokesType};
    use crate::tabular::TabularCoordinate;
    use casacore_types::measures::direction::DirectionRef;
    use casacore_types::measures::frequency::FrequencyRef;

    fn make_typical_system() -> CoordinateSystem {
        let mut cs = CoordinateSystem::new();

        // Direction-like: 2-axis linear as placeholder
        let dir = LinearCoordinate::new(
            2,
            vec!["Right Ascension".into(), "Declination".into()],
            vec!["rad".into(), "rad".into()],
        );
        cs.add_coordinate(Box::new(dir));

        // Spectral
        let spec = SpectralCoordinate::new(FrequencyRef::LSRK, 1.42e9, 1e6, 0.0, 1.42e9);
        cs.add_coordinate(Box::new(spec));

        // Stokes
        let stokes = StokesCoordinate::new(vec![
            StokesType::I,
            StokesType::Q,
            StokesType::U,
            StokesType::V,
        ]);
        cs.add_coordinate(Box::new(stokes));

        cs
    }

    #[test]
    fn axis_counts() {
        let cs = make_typical_system();
        assert_eq!(cs.n_coordinates(), 3);
        assert_eq!(cs.n_pixel_axes(), 4); // 2 + 1 + 1
        assert_eq!(cs.n_world_axes(), 4);
    }

    #[test]
    fn find_coordinate() {
        let cs = make_typical_system();
        assert_eq!(cs.find_coordinate(CoordinateType::Linear), Some(0));
        assert_eq!(cs.find_coordinate(CoordinateType::Spectral), Some(1));
        assert_eq!(cs.find_coordinate(CoordinateType::Stokes), Some(2));
        assert_eq!(cs.find_coordinate(CoordinateType::Direction), None);
        assert_eq!(cs.find_coordinate(CoordinateType::Tabular), None);
    }

    #[test]
    fn to_world_full() {
        let cs = make_typical_system();
        // pixel: [dir_x, dir_y, spec_chan, stokes_idx]
        let pixel = vec![0.0, 0.0, 0.0, 0.0];
        let world = cs.to_world(&pixel).unwrap();
        assert_eq!(world.len(), 4);
        // Linear: world = pixel (default)
        assert!((world[0]).abs() < 1e-12);
        assert!((world[1]).abs() < 1e-12);
        // Spectral: crval at pixel 0
        assert!((world[2] - 1.42e9).abs() < 1.0);
        // Stokes: I = 1
        assert!((world[3] - 1.0).abs() < 1e-12);
    }

    #[test]
    fn roundtrip() {
        let cs = make_typical_system();
        let pixel = vec![10.0, 20.0, 50.0, 2.0];
        let world = cs.to_world(&pixel).unwrap();
        let back = cs.to_pixel(&world).unwrap();
        for i in 0..4 {
            assert!(
                (back[i] - pixel[i]).abs() < 1e-8,
                "axis {i}: {} vs {}",
                back[i],
                pixel[i]
            );
        }
    }

    #[test]
    fn dimension_mismatch() {
        let cs = make_typical_system();
        assert!(cs.to_world(&[1.0, 2.0]).is_err());
        assert!(cs.to_pixel(&[1.0]).is_err());
    }

    #[test]
    fn empty_system() {
        let cs = CoordinateSystem::new();
        assert_eq!(cs.n_coordinates(), 0);
        assert_eq!(cs.n_pixel_axes(), 0);
        assert_eq!(cs.n_world_axes(), 0);
        let world = cs.to_world(&[]).unwrap();
        assert!(world.is_empty());
    }

    #[test]
    fn obs_info_access() {
        let obs = ObsInfo::new("ALMA").with_observer("Test");
        let cs = CoordinateSystem::new().with_obs_info(obs);
        assert_eq!(cs.obs_info().telescope, "ALMA");
        assert_eq!(cs.obs_info().observer, "Test");
    }

    #[test]
    fn obs_info_mut() {
        let mut cs = CoordinateSystem::new();
        cs.obs_info_mut().telescope = "VLA".into();
        assert_eq!(cs.obs_info().telescope, "VLA");
    }

    #[test]
    fn to_record_has_coordinates() {
        let cs = make_typical_system();
        let rec = cs.to_record();
        assert!(rec.get("ncoordinates").is_some());
        assert!(rec.get("coordinate0").is_some());
        assert!(rec.get("coordinate1").is_some());
        assert!(rec.get("coordinate2").is_some());
        assert!(rec.get("obsinfo").is_some());
    }

    #[test]
    fn from_record_empty() {
        let rec = RecordValue::default();
        let cs = CoordinateSystem::from_record(&rec).unwrap();
        assert_eq!(cs.n_coordinates(), 0);
    }

    #[test]
    fn from_record_with_obsinfo() {
        let mut rec = RecordValue::default();
        let mut obs_rec = RecordValue::default();
        obs_rec.upsert(
            "telescope",
            Value::Scalar(ScalarValue::String("MeerKAT".into())),
        );
        rec.upsert("obsinfo", Value::Record(obs_rec));
        let cs = CoordinateSystem::from_record(&rec).unwrap();
        assert_eq!(cs.obs_info().telescope, "MeerKAT");
    }

    #[test]
    fn coordinate_access() {
        let cs = make_typical_system();
        assert_eq!(cs.coordinate(0).coordinate_type(), CoordinateType::Linear);
        assert_eq!(cs.coordinate(1).coordinate_type(), CoordinateType::Spectral);
        assert_eq!(cs.coordinate(2).coordinate_type(), CoordinateType::Stokes);
    }

    #[test]
    fn clone_preserves_coordinates() {
        let cs = make_typical_system();
        let cloned = cs.clone();
        assert_eq!(cloned.n_coordinates(), 3);
        assert_eq!(
            cloned.coordinate(0).coordinate_type(),
            CoordinateType::Linear,
        );
    }

    #[test]
    fn default_is_empty() {
        let cs = CoordinateSystem::default();
        assert_eq!(cs.n_coordinates(), 0);
    }

    #[test]
    fn from_record_roundtrips_mixed_system() {
        let mut cs = CoordinateSystem::new();
        let dir = DirectionCoordinate::new(
            DirectionRef::J2000,
            Projection::new(ProjectionType::SIN),
            [0.0, std::f64::consts::FRAC_PI_4],
            [-1e-4, 1e-4],
            [256.0, 256.0],
        );
        cs.add_coordinate(Box::new(dir));
        cs.add_coordinate(Box::new(SpectralCoordinate::new(
            FrequencyRef::LSRK,
            1.42e9,
            1.0e6,
            0.0,
            1.42040575e9,
        )));
        cs.add_coordinate(Box::new(StokesCoordinate::new(vec![
            StokesType::I,
            StokesType::Q,
        ])));
        cs.add_coordinate(Box::new(TabularCoordinate::new(
            vec![0.0, 1.0, 2.0],
            vec![100.0, 200.0, 300.0],
            "Velocity",
            "km/s",
        )));

        let restored = CoordinateSystem::from_record(&cs.to_record()).unwrap();

        assert_eq!(restored.n_coordinates(), 4);
        assert_eq!(
            restored.coordinate(0).coordinate_type(),
            CoordinateType::Direction
        );
        assert_eq!(
            restored.coordinate(1).coordinate_type(),
            CoordinateType::Spectral
        );
        assert_eq!(
            restored.coordinate(2).coordinate_type(),
            CoordinateType::Stokes
        );
        assert_eq!(
            restored.coordinate(3).coordinate_type(),
            CoordinateType::Tabular
        );
        assert_eq!(restored.n_pixel_axes(), 5);

        let pixel = vec![256.5, 255.5, 4.0, 1.0, 1.5];
        let world = restored.to_world(&pixel).unwrap();
        let back = restored.to_pixel(&world).unwrap();
        assert_eq!(back.len(), pixel.len());
        for (index, (restored_value, original)) in back.iter().zip(pixel.iter()).enumerate() {
            assert!(
                (restored_value - original).abs() < 1e-6,
                "axis {index}: {} vs {}",
                restored_value,
                original
            );
        }
    }

    #[test]
    fn from_record_skips_unknown_coordinate_type() {
        let mut rec = RecordValue::default();
        rec.upsert("ncoordinates", Value::Scalar(ScalarValue::Int32(2)));
        let mut unknown = RecordValue::default();
        unknown.upsert(
            "coordinate_type",
            Value::Scalar(ScalarValue::String("Unknown".into())),
        );
        rec.upsert("coordinate0", Value::Record(unknown));
        rec.upsert(
            "coordinate1",
            Value::Record(LinearCoordinate::new(1, vec!["X".into()], vec!["m".into()]).to_record()),
        );

        let cs = CoordinateSystem::from_record(&rec).unwrap();
        assert_eq!(cs.n_coordinates(), 1);
        assert_eq!(cs.coordinate(0).coordinate_type(), CoordinateType::Linear);
    }

    #[test]
    fn from_record_skips_invalid_coordinate_record() {
        let mut rec = RecordValue::default();
        rec.upsert("ncoordinates", Value::Scalar(ScalarValue::Int32(2)));
        let mut broken = RecordValue::default();
        broken.upsert(
            "coordinate_type",
            Value::Scalar(ScalarValue::String("Spectral".into())),
        );
        rec.upsert("coordinate0", Value::Record(broken));
        rec.upsert(
            "coordinate1",
            Value::Record(StokesCoordinate::new(vec![StokesType::I]).to_record()),
        );

        let cs = CoordinateSystem::from_record(&rec).unwrap();
        assert_eq!(cs.n_coordinates(), 1);
        assert_eq!(cs.coordinate(0).coordinate_type(), CoordinateType::Stokes);
    }
}

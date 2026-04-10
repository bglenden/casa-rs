// SPDX-License-Identifier: LGPL-3.0-or-later
//! Two-axis celestial (RA/Dec) coordinate.
//!
//! [`DirectionCoordinate`] implements the mapping between pixel positions and
//! sky positions (longitude, latitude) in a specified celestial reference
//! frame. The conversion chain is:
//!
//! ```text
//! pixel -> intermediate (x,y) -> native spherical (phi,theta) -> celestial (lon,lat)
//! ```
//!
//! The pixel-to-intermediate step uses the standard FITS WCS linear mapping
//! with a PC matrix. The intermediate-to-native step uses a spherical
//! projection (see [`Projection`]). The native-to-celestial step applies
//! the Calabretta & Greisen (2002) spherical rotation.
//!
//! This corresponds to C++ `DirectionCoordinate`.

use std::f64::consts::{FRAC_PI_2, PI};
use std::str::FromStr;

use casa_types::measures::direction::DirectionRef;
use casa_types::quanta::{Quantity, Unit};
use casa_types::{ArrayD, ArrayValue, RecordValue, ScalarValue, Value};
use ndarray::Array2;
use ndarray::IxDyn;

use crate::coordinate::{Coordinate, CoordinateType};
use crate::error::CoordinateError;
use crate::projection::{Projection, ProjectionType};
use crate::record_utils::{
    get_optional_f64, get_optional_i32, get_optional_string, get_optional_vec_f64,
    get_optional_vec_string, get_required_vec_f64,
};

/// A two-axis celestial coordinate with projection.
///
/// The two pixel axes map to longitude and latitude in a celestial reference
/// frame through an affine transformation, a map projection, and a spherical
/// rotation.
///
/// The `crval` field stores the celestial longitude and latitude of the
/// reference point in radians. The `cdelt` and `pc` fields control the
/// affine mapping from pixel to intermediate coordinates.
///
/// Corresponds to C++ `DirectionCoordinate`.
#[derive(Debug, Clone)]
pub struct DirectionCoordinate {
    /// The map projection used for this coordinate.
    projection: Projection,
    /// The celestial reference frame (J2000, GALACTIC, etc.).
    direction_ref: DirectionRef,
    /// Reference celestial position [longitude, latitude] in radians (alpha_0, delta_0).
    crval: [f64; 2],
    /// Axis increments [dlon, dlat] in radians per pixel.
    cdelt: [f64; 2],
    /// Reference pixel positions [x, y].
    crpix: [f64; 2],
    /// PC rotation/coupling matrix (2x2).
    pc: Array2<f64>,
    /// Native longitude of the celestial pole (phi_p, radians).
    longpole: f64,
    /// Native latitude of the celestial pole (theta_p, radians).
    latpole: f64,
    /// Celestial coordinates of the native pole [alpha_p, delta_p] in radians.
    /// Computed from crval, longpole, latpole, and theta_0.
    native_pole: [f64; 2],
}

/// Returns the native latitude of the fiducial point for a given projection type.
fn theta_0_for_projection(proj: &Projection) -> f64 {
    use crate::projection::ProjectionType;
    match proj.projection_type() {
        ProjectionType::CAR | ProjectionType::MER | ProjectionType::SFL | ProjectionType::AIT => {
            0.0
        }
        _ => FRAC_PI_2, // zenithal projections
    }
}

/// Computes the celestial coordinates of the native pole (alpha_p, delta_p)
/// from the fiducial point (alpha_0, delta_0), phi_p (longpole), and theta_0.
///
/// Follows Calabretta & Greisen (2002) Section 2.6.
fn compute_native_pole(
    alpha_0: f64,
    delta_0: f64,
    phi_p: f64,
    theta_p: f64,
    theta_0: f64,
) -> [f64; 2] {
    // Special case: zenithal projections (theta_0 = pi/2)
    // The fiducial point IS the native pole.
    if (theta_0 - FRAC_PI_2).abs() < 1e-12 {
        return [alpha_0, delta_0];
    }

    // General case: compute delta_p from eq. (2.6.3):
    //   sin(delta_p) = sin(delta_0) * sin(theta_0) + cos(delta_0) * cos(theta_0) * cos(phi_p)
    // When there are two solutions, pick the one closest to theta_p (latpole).

    let sin_d0 = delta_0.sin();
    let cos_d0 = delta_0.cos();
    let sin_t0 = theta_0.sin();
    let cos_t0 = theta_0.cos();
    let cos_pp = phi_p.cos();

    let sin_dp = sin_d0 * sin_t0 + cos_d0 * cos_t0 * cos_pp;
    let sin_dp = sin_dp.clamp(-1.0, 1.0);
    let delta_p = sin_dp.asin();

    // There may be two solutions: delta_p and pi - delta_p.
    // Choose the one closest to theta_p (latpole).
    let delta_p = if (delta_p - theta_p).abs() <= (PI - delta_p - theta_p).abs() {
        delta_p
    } else {
        PI - delta_p
    };

    // Compute alpha_p
    let sin_dp2 = delta_p.sin();
    let cos_dp2 = delta_p.cos();

    if cos_dp2.abs() < 1e-12 {
        // Native pole is at celestial pole
        return [alpha_0, delta_p];
    }

    // alpha_p from eq. (2.6.4):
    //   sin(alpha_p - alpha_0) = -cos(theta_0) * sin(phi_p) / cos(delta_p)
    //   cos(alpha_p - alpha_0) = [sin(theta_0) - sin(delta_p)*sin(delta_0)] / [cos(delta_p)*cos(delta_0)]
    let sin_da = -cos_t0 * phi_p.sin() / cos_dp2;
    let cos_da = if cos_d0.abs() < 1e-12 {
        // Fiducial point at celestial pole
        0.0
    } else {
        (sin_t0 - sin_dp2 * sin_d0) / (cos_dp2 * cos_d0)
    };

    let alpha_p = alpha_0 + sin_da.atan2(cos_da);

    [alpha_p, delta_p]
}

impl DirectionCoordinate {
    /// Creates a new direction coordinate.
    ///
    /// - `direction_ref`: the celestial reference frame.
    /// - `projection`: the map projection to use.
    /// - `crval`: reference celestial position [lon, lat] in radians.
    /// - `cdelt`: axis increments [dlon, dlat] in radians per pixel.
    /// - `crpix`: reference pixel positions.
    ///
    /// The PC matrix defaults to the identity. The native pole coordinates
    /// are computed from `crval` and the projection's native latitude of the
    /// fiducial point following Calabretta & Greisen (2002).
    pub fn new(
        direction_ref: DirectionRef,
        projection: Projection,
        crval: [f64; 2],
        cdelt: [f64; 2],
        crpix: [f64; 2],
    ) -> Self {
        let theta_0 = theta_0_for_projection(&projection);

        // longpole (phi_p) defaults: if delta_0 >= theta_0, phi_p = 0, else phi_p = pi
        let longpole = if crval[1] >= theta_0 { 0.0 } else { PI };
        let latpole = FRAC_PI_2;

        let native_pole = compute_native_pole(crval[0], crval[1], longpole, latpole, theta_0);

        Self {
            projection,
            direction_ref,
            crval,
            cdelt,
            crpix,
            pc: Array2::eye(2),
            longpole,
            latpole,
            native_pole,
        }
    }

    /// Sets the PC rotation/coupling matrix. Returns `self` for chaining.
    ///
    /// # Panics
    ///
    /// Panics if the matrix shape is not (2, 2).
    pub fn with_pc_matrix(mut self, pc: Array2<f64>) -> Self {
        assert_eq!(pc.shape(), [2, 2]);
        self.pc = pc;
        self
    }

    /// Sets the native longitude of the celestial pole (phi_p) in radians
    /// and recomputes the native pole. Returns `self` for chaining.
    pub fn with_longpole(mut self, longpole: f64) -> Self {
        self.longpole = longpole;
        let theta_0 = theta_0_for_projection(&self.projection);
        self.native_pole = compute_native_pole(
            self.crval[0],
            self.crval[1],
            self.longpole,
            self.latpole,
            theta_0,
        );
        self
    }

    /// Sets the native latitude of the celestial pole (theta_p) in radians
    /// and recomputes the native pole. Returns `self` for chaining.
    pub fn with_latpole(mut self, latpole: f64) -> Self {
        self.latpole = latpole;
        let theta_0 = theta_0_for_projection(&self.projection);
        self.native_pole = compute_native_pole(
            self.crval[0],
            self.crval[1],
            self.longpole,
            self.latpole,
            theta_0,
        );
        self
    }

    /// Returns the projection.
    pub fn projection(&self) -> &Projection {
        &self.projection
    }

    /// Returns the celestial reference frame.
    pub fn direction_ref(&self) -> DirectionRef {
        self.direction_ref
    }

    /// Returns the native longitude of the celestial pole (phi_p) in radians.
    pub fn longpole(&self) -> f64 {
        self.longpole
    }

    /// Returns the native latitude of the celestial pole (theta_p) in radians.
    pub fn latpole(&self) -> f64 {
        self.latpole
    }

    /// Returns a reference to the PC rotation/coupling matrix.
    pub fn pc_matrix(&self) -> &Array2<f64> {
        &self.pc
    }

    /// Reconstructs a direction coordinate from a serialized record.
    pub fn from_record(rec: &RecordValue) -> Result<Self, CoordinateError> {
        let casa_style_units = get_optional_vec_string(rec, "units");
        let is_casa_style = casa_style_units.is_some()
            || rec.get("conversionSystem").is_some()
            || rec.get("axes").is_some();
        let direction_ref = if let Some(name) =
            get_optional_string(rec, "direction_ref").or_else(|| get_optional_string(rec, "system"))
        {
            DirectionRef::from_str(&name).map_err(|err| {
                CoordinateError::InvalidRecord(format!("invalid direction_ref: {err}"))
            })?
        } else if let Some(code) = get_optional_i32(rec, "direction_ref") {
            DirectionRef::from_casacore_code(code).ok_or_else(|| {
                CoordinateError::InvalidRecord(format!("invalid direction_ref code {code}"))
            })?
        } else {
            return Err(CoordinateError::InvalidRecord(
                "missing or invalid direction_ref".into(),
            ));
        };

        let projection_name = get_optional_string(rec, "projection").ok_or_else(|| {
            CoordinateError::InvalidRecord("missing or invalid projection".into())
        })?;
        let projection_type = ProjectionType::from_name(&projection_name).ok_or_else(|| {
            CoordinateError::InvalidRecord(format!("unsupported projection {projection_name}"))
        })?;
        let projection =
            if let Some(parameters) = get_optional_vec_f64(rec, "projection_parameters") {
                Projection::with_parameters(projection_type, parameters)
            } else {
                Projection::new(projection_type)
            };

        let crval = get_required_vec_f64(rec, "crval")?;
        let cdelt = get_required_vec_f64(rec, "cdelt")?;
        let crpix = get_required_vec_f64(rec, "crpix")?;
        if crval.len() != 2 || cdelt.len() != 2 || crpix.len() != 2 {
            return Err(CoordinateError::InvalidRecord(
                "direction coordinate expects two-valued crval/cdelt/crpix".into(),
            ));
        }
        let axis_units = casa_style_units.unwrap_or_else(|| vec!["rad".into(), "rad".into()]);
        if axis_units.len() != 2 {
            return Err(CoordinateError::InvalidRecord(format!(
                "direction coordinate units has {} values, expected 2",
                axis_units.len()
            )));
        }
        let crval = [
            convert_value_to_unit(crval[0], &axis_units[0], "rad", "crval")?,
            convert_value_to_unit(crval[1], &axis_units[1], "rad", "crval")?,
        ];
        let cdelt = [
            convert_value_to_unit(cdelt[0], &axis_units[0], "rad", "cdelt")?,
            convert_value_to_unit(cdelt[1], &axis_units[1], "rad", "cdelt")?,
        ];

        let mut coord = Self::new(
            direction_ref,
            projection,
            crval,
            cdelt,
            [crpix[0], crpix[1]],
        );

        if let Some(pc_flat) = get_optional_vec_f64(rec, "pc") {
            if pc_flat.len() != 4 {
                return Err(CoordinateError::InvalidRecord(format!(
                    "direction pc matrix has {} elements, expected 4",
                    pc_flat.len()
                )));
            }
            let pc = Array2::from_shape_vec((2, 2), pc_flat).map_err(|err| {
                CoordinateError::InvalidRecord(format!("invalid direction pc matrix: {err}"))
            })?;
            coord = coord.with_pc_matrix(pc);
        }
        if let Some(longpole) = get_optional_f64(rec, "longpole") {
            let longpole = if is_casa_style {
                convert_value_to_unit(longpole, "deg", "rad", "longpole")?
            } else {
                longpole
            };
            coord = coord.with_longpole(longpole);
        }
        if let Some(latpole) = get_optional_f64(rec, "latpole") {
            let latpole = if is_casa_style {
                convert_value_to_unit(latpole, "deg", "rad", "latpole")?
            } else {
                latpole
            };
            coord = coord.with_latpole(latpole);
        }

        Ok(coord)
    }

    /// Pixel to intermediate world coordinates.
    fn pixel_to_intermediate(&self, pixel: &[f64]) -> (f64, f64) {
        let dx = pixel[0] - self.crpix[0];
        let dy = pixel[1] - self.crpix[1];

        // Apply PC matrix
        let x = self.cdelt[0] * (self.pc[[0, 0]] * dx + self.pc[[0, 1]] * dy);
        let y = self.cdelt[1] * (self.pc[[1, 0]] * dx + self.pc[[1, 1]] * dy);

        (x, y)
    }

    /// Intermediate world coordinates to pixel.
    fn intermediate_to_pixel(&self, x: f64, y: f64) -> Result<(f64, f64), CoordinateError> {
        // Undo cdelt scaling
        let sx = x / self.cdelt[0];
        let sy = y / self.cdelt[1];

        // Invert PC matrix
        let det = self.pc[[0, 0]] * self.pc[[1, 1]] - self.pc[[0, 1]] * self.pc[[1, 0]];
        if det.abs() < 1e-15 {
            return Err(CoordinateError::ConversionFailed(
                "singular PC matrix in DirectionCoordinate".into(),
            ));
        }
        let dx = (self.pc[[1, 1]] * sx - self.pc[[0, 1]] * sy) / det;
        let dy = (-self.pc[[1, 0]] * sx + self.pc[[0, 0]] * sy) / det;

        Ok((dx + self.crpix[0], dy + self.crpix[1]))
    }

    /// Native spherical (phi, theta) to celestial (lon, lat) using the
    /// Calabretta & Greisen (2002) spherical rotation (eq. 2).
    ///
    /// Uses the pre-computed native pole (alpha_p, delta_p).
    fn native_to_celestial(&self, phi: f64, theta: f64) -> (f64, f64) {
        let (alpha_p, delta_p) = (self.native_pole[0], self.native_pole[1]);
        let phi_p = self.longpole;

        let sin_theta = theta.sin();
        let cos_theta = theta.cos();
        let sin_dp = delta_p.sin();
        let cos_dp = delta_p.cos();
        let sin_dphi = (phi - phi_p).sin();
        let cos_dphi = (phi - phi_p).cos();

        // Celestial latitude
        let sin_lat = sin_theta * sin_dp + cos_theta * cos_dp * cos_dphi;
        let lat = sin_lat.clamp(-1.0, 1.0).asin();

        // Celestial longitude
        let num = -cos_theta * sin_dphi;
        let den = sin_theta * cos_dp - cos_theta * sin_dp * cos_dphi;
        let lon = alpha_p + num.atan2(den);

        // Normalise longitude to [0, 2*pi)
        let lon = normalize_angle(lon);

        (lon, lat)
    }

    /// Celestial (lon, lat) to native spherical (phi, theta).
    fn celestial_to_native(&self, lon: f64, lat: f64) -> (f64, f64) {
        let (alpha_p, delta_p) = (self.native_pole[0], self.native_pole[1]);
        let phi_p = self.longpole;

        let sin_lat = lat.sin();
        let cos_lat = lat.cos();
        let sin_dp = delta_p.sin();
        let cos_dp = delta_p.cos();
        let da = lon - alpha_p;
        let sin_da = da.sin();
        let cos_da = da.cos();

        // Native latitude
        let sin_theta = sin_lat * sin_dp + cos_lat * cos_dp * cos_da;
        let theta = sin_theta.clamp(-1.0, 1.0).asin();

        // Native longitude
        let num = -cos_lat * sin_da;
        let den = sin_lat * cos_dp - cos_lat * sin_dp * cos_da;
        let phi = phi_p + num.atan2(den);

        (phi, theta)
    }
}

/// Normalises an angle to the range [0, 2*pi).
fn normalize_angle(mut a: f64) -> f64 {
    let two_pi = 2.0 * PI;
    a %= two_pi;
    if a < 0.0 {
        a += two_pi;
    }
    a
}

fn convert_value_to_unit(
    value: f64,
    from_unit: &str,
    to_unit: &str,
    field: &str,
) -> Result<f64, CoordinateError> {
    let quantity = Quantity::new(value, from_unit).map_err(|error| {
        CoordinateError::InvalidRecord(format!(
            "invalid direction coordinate unit for {field}: {from_unit} ({error})"
        ))
    })?;
    let target = Unit::new(to_unit).map_err(|error| {
        CoordinateError::InvalidRecord(format!(
            "invalid direction coordinate target unit for {field}: {to_unit} ({error})"
        ))
    })?;
    quantity.get_value_in(&target).map_err(|error| {
        CoordinateError::InvalidRecord(format!(
            "cannot convert direction coordinate {field} from {from_unit} to {to_unit}: {error}"
        ))
    })
}

impl Coordinate for DirectionCoordinate {
    fn coordinate_type(&self) -> CoordinateType {
        CoordinateType::Direction
    }

    fn n_pixel_axes(&self) -> usize {
        2
    }

    fn n_world_axes(&self) -> usize {
        2
    }

    fn to_world(&self, pixel: &[f64]) -> Result<Vec<f64>, CoordinateError> {
        if pixel.len() != 2 {
            return Err(CoordinateError::DimensionMismatch {
                expected: 2,
                got: pixel.len(),
            });
        }

        // Step 1: pixel -> intermediate
        let (x, y) = self.pixel_to_intermediate(pixel);

        // Step 2: intermediate -> native spherical
        let (phi, theta) = self.projection.deproject(x, y)?;

        // Step 3: native spherical -> celestial
        let (lon, lat) = self.native_to_celestial(phi, theta);

        Ok(vec![lon, lat])
    }

    fn to_pixel(&self, world: &[f64]) -> Result<Vec<f64>, CoordinateError> {
        if world.len() != 2 {
            return Err(CoordinateError::DimensionMismatch {
                expected: 2,
                got: world.len(),
            });
        }

        let (lon, lat) = (world[0], world[1]);

        // Step 1: celestial -> native spherical
        let (phi, theta) = self.celestial_to_native(lon, lat);

        // Step 2: native spherical -> intermediate
        let (x, y) = self.projection.project(phi, theta)?;

        // Step 3: intermediate -> pixel
        let (px, py) = self.intermediate_to_pixel(x, y)?;

        Ok(vec![px, py])
    }

    fn reference_value(&self) -> Vec<f64> {
        self.crval.to_vec()
    }

    fn reference_pixel(&self) -> Vec<f64> {
        self.crpix.to_vec()
    }

    fn increment(&self) -> Vec<f64> {
        self.cdelt.to_vec()
    }

    fn axis_names(&self) -> Vec<String> {
        match self.direction_ref {
            DirectionRef::GALACTIC | DirectionRef::SUPERGAL => {
                vec!["Longitude".into(), "Latitude".into()]
            }
            _ => vec!["Right Ascension".into(), "Declination".into()],
        }
    }

    fn axis_units(&self) -> Vec<String> {
        vec!["rad".into(), "rad".into()]
    }

    fn to_record(&self) -> RecordValue {
        let mut rec = RecordValue::default();

        rec.upsert(
            "coordinate_type",
            Value::Scalar(ScalarValue::String("Direction".into())),
        );
        rec.upsert(
            "direction_ref",
            Value::Scalar(ScalarValue::String(format!("{:?}", self.direction_ref))),
        );
        rec.upsert(
            "projection",
            Value::Scalar(ScalarValue::String(self.projection.name().into())),
        );
        rec.upsert(
            "projection_parameters",
            Value::Array(casa_types::ArrayValue::from_f64_vec(
                serialized_projection_parameters(&self.projection),
            )),
        );
        rec.upsert(
            "crval",
            Value::Array(casa_types::ArrayValue::from_f64_vec(self.crval.to_vec())),
        );
        rec.upsert(
            "cdelt",
            Value::Array(casa_types::ArrayValue::from_f64_vec(self.cdelt.to_vec())),
        );
        rec.upsert(
            "crpix",
            Value::Array(casa_types::ArrayValue::from_f64_vec(self.crpix.to_vec())),
        );
        let pc_flat: Vec<f64> = self.pc.iter().copied().collect();
        rec.upsert(
            "pc",
            Value::Array(casa_types::ArrayValue::from_f64_vec(pc_flat)),
        );
        rec.upsert(
            "longpole",
            Value::Scalar(ScalarValue::Float64(self.longpole)),
        );
        rec.upsert("latpole", Value::Scalar(ScalarValue::Float64(self.latpole)));

        rec
    }

    fn to_casa_record(&self) -> RecordValue {
        let mut rec = RecordValue::default();
        rec.upsert(
            "system",
            Value::Scalar(ScalarValue::String(format!("{:?}", self.direction_ref))),
        );
        rec.upsert(
            "projection",
            Value::Scalar(ScalarValue::String(self.projection.name().into())),
        );
        rec.upsert(
            "projection_parameters",
            Value::Array(ArrayValue::from_f64_vec(serialized_projection_parameters(
                &self.projection,
            ))),
        );
        rec.upsert(
            "crval",
            Value::Array(ArrayValue::from_f64_vec(self.crval.to_vec())),
        );
        rec.upsert(
            "crpix",
            Value::Array(ArrayValue::from_f64_vec(self.crpix.to_vec())),
        );
        rec.upsert(
            "cdelt",
            Value::Array(ArrayValue::from_f64_vec(self.cdelt.to_vec())),
        );
        rec.upsert(
            "pc",
            Value::Array(ArrayValue::Float64(
                ArrayD::from_shape_vec(IxDyn(&[2, 2]), self.pc.iter().copied().collect())
                    .expect("2x2 direction pc matrix"),
            )),
        );
        rec.upsert(
            "axes",
            Value::Array(ArrayValue::from_string_vec(self.axis_names())),
        );
        rec.upsert(
            "units",
            Value::Array(ArrayValue::from_string_vec(self.axis_units())),
        );
        rec.upsert(
            "conversionSystem",
            Value::Scalar(ScalarValue::String(format!("{:?}", self.direction_ref))),
        );
        rec.upsert(
            "longpole",
            Value::Scalar(ScalarValue::Float64(self.longpole.to_degrees())),
        );
        rec.upsert(
            "latpole",
            Value::Scalar(ScalarValue::Float64(self.latpole.to_degrees())),
        );
        rec
    }

    fn clone_box(&self) -> Box<dyn Coordinate> {
        Box::new(self.clone())
    }
}

fn serialized_projection_parameters(projection: &Projection) -> Vec<f64> {
    if projection.parameters().is_empty() {
        vec![0.0, 0.0]
    } else {
        projection.parameters().to_vec()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::projection::ProjectionType;

    const TOL: f64 = 1e-10;

    fn make_sin_coord() -> DirectionCoordinate {
        let proj = Projection::new(ProjectionType::SIN);
        // Reference point at RA=0, Dec=+45 degrees
        let crval = [0.0, std::f64::consts::FRAC_PI_4];
        let cdelt = [-1e-4, 1e-4]; // ~20 arcsec pixels
        let crpix = [512.0, 512.0];
        DirectionCoordinate::new(DirectionRef::J2000, proj, crval, cdelt, crpix)
    }

    #[test]
    fn reference_pixel_gives_reference_value() {
        let coord = make_sin_coord();
        let world = coord.to_world(&[512.0, 512.0]).unwrap();
        assert!(
            (world[0] - coord.crval[0]).abs() < TOL
                || (world[0] - coord.crval[0] - 2.0 * PI).abs() < TOL,
            "lon mismatch: {} vs {}",
            world[0],
            coord.crval[0]
        );
        assert!(
            (world[1] - coord.crval[1]).abs() < TOL,
            "lat mismatch: {} vs {}",
            world[1],
            coord.crval[1]
        );
    }

    #[test]
    fn roundtrip_near_reference() {
        let coord = make_sin_coord();
        let pixel = [520.0, 530.0];
        let world = coord.to_world(&pixel).unwrap();
        let back = coord.to_pixel(&world).unwrap();
        assert!(
            (back[0] - pixel[0]).abs() < 1e-6,
            "px roundtrip: {} vs {}",
            back[0],
            pixel[0]
        );
        assert!(
            (back[1] - pixel[1]).abs() < 1e-6,
            "py roundtrip: {} vs {}",
            back[1],
            pixel[1]
        );
    }

    #[test]
    fn roundtrip_tan() {
        let proj = Projection::new(ProjectionType::TAN);
        let crval = [1.0, 0.5];
        let cdelt = [-1e-4, 1e-4];
        let crpix = [256.0, 256.0];
        let coord = DirectionCoordinate::new(DirectionRef::J2000, proj, crval, cdelt, crpix);

        let pixel = [270.0, 280.0];
        let world = coord.to_world(&pixel).unwrap();
        let back = coord.to_pixel(&world).unwrap();
        assert!((back[0] - pixel[0]).abs() < 1e-6);
        assert!((back[1] - pixel[1]).abs() < 1e-6);
    }

    #[test]
    fn roundtrip_car() {
        let proj = Projection::new(ProjectionType::CAR);
        // Use positive declination so longpole=0 (simple case)
        let crval = [3.0, 0.3];
        let cdelt = [-1e-3, 1e-3];
        let crpix = [100.0, 100.0];
        let coord = DirectionCoordinate::new(DirectionRef::J2000, proj, crval, cdelt, crpix);

        let pixel = [110.0, 90.0];
        let world = coord.to_world(&pixel).unwrap();
        let back = coord.to_pixel(&world).unwrap();
        assert!(
            (back[0] - pixel[0]).abs() < 1e-6,
            "CAR px roundtrip: {} vs {}",
            back[0],
            pixel[0]
        );
        assert!(
            (back[1] - pixel[1]).abs() < 1e-6,
            "CAR py roundtrip: {} vs {}",
            back[1],
            pixel[1]
        );
    }

    // NOTE: CAR with negative declination (delta_0 < theta_0 = 0) requires
    // longpole = pi and a more complex native-pole computation that introduces
    // coordinate inversions. This case is deferred to a future iteration.

    #[test]
    fn dimension_mismatch() {
        let coord = make_sin_coord();
        assert!(coord.to_world(&[1.0]).is_err());
        assert!(coord.to_pixel(&[1.0, 2.0, 3.0]).is_err());
    }

    #[test]
    fn trait_methods() {
        let coord = make_sin_coord();
        assert_eq!(coord.coordinate_type(), CoordinateType::Direction);
        assert_eq!(coord.n_pixel_axes(), 2);
        assert_eq!(coord.n_world_axes(), 2);
        assert_eq!(coord.axis_names(), vec!["Right Ascension", "Declination"]);
        assert_eq!(coord.axis_units(), vec!["rad", "rad"]);
    }

    #[test]
    fn galactic_axis_names() {
        let proj = Projection::new(ProjectionType::SIN);
        let coord = DirectionCoordinate::new(
            DirectionRef::GALACTIC,
            proj,
            [0.0, 0.0],
            [1e-4, 1e-4],
            [0.0, 0.0],
        );
        assert_eq!(coord.axis_names(), vec!["Longitude", "Latitude"]);
    }

    #[test]
    fn to_record_has_projection() {
        let coord = make_sin_coord();
        let rec = coord.to_record();
        assert!(rec.get("projection").is_some());
        assert!(rec.get("direction_ref").is_some());
    }

    #[test]
    fn clone_box_preserves_type() {
        let coord = make_sin_coord();
        let boxed: Box<dyn Coordinate> = Box::new(coord);
        let cloned = boxed.clone_box();
        assert_eq!(cloned.coordinate_type(), CoordinateType::Direction);
    }

    #[test]
    fn record_roundtrip() {
        let proj = Projection::with_parameters(ProjectionType::SIN, vec![0.25, -0.5]);
        let pc = Array2::from_shape_vec((2, 2), vec![0.9, -0.1, 0.2, 1.1]).unwrap();
        let coord = DirectionCoordinate::new(
            DirectionRef::GALACTIC,
            proj,
            [1.0, 0.5],
            [-1e-4, 2e-4],
            [128.0, 64.0],
        )
        .with_pc_matrix(pc.clone())
        .with_longpole(0.25)
        .with_latpole(1.2);

        let restored = DirectionCoordinate::from_record(&coord.to_record()).unwrap();

        assert_eq!(restored.direction_ref(), DirectionRef::GALACTIC);
        assert_eq!(restored.reference_value(), vec![1.0, 0.5]);
        assert_eq!(restored.reference_pixel(), vec![128.0, 64.0]);
        assert_eq!(restored.increment(), vec![-1e-4, 2e-4]);
        assert_eq!(restored.pc_matrix(), &pc);
        assert_eq!(restored.projection().projection_type(), ProjectionType::SIN);
        assert_eq!(restored.projection().parameters(), &[0.25, -0.5]);
        assert!((restored.longpole() - 0.25).abs() < TOL);
        assert!((restored.latpole() - 1.2).abs() < TOL);
    }

    #[test]
    fn casa_record_units_are_converted_to_radians() {
        let mut record = RecordValue::default();
        record.upsert("system", Value::Scalar(ScalarValue::String("J2000".into())));
        record.upsert(
            "projection",
            Value::Scalar(ScalarValue::String("SIN".into())),
        );
        record.upsert(
            "crval",
            Value::Array(ArrayValue::from_f64_vec(vec![120.0, 45.0])),
        );
        record.upsert(
            "crpix",
            Value::Array(ArrayValue::from_f64_vec(vec![0.0, 0.0])),
        );
        record.upsert(
            "cdelt",
            Value::Array(ArrayValue::from_f64_vec(vec![-1.0, 1.0])),
        );
        record.upsert(
            "units",
            Value::Array(ArrayValue::from_string_vec(vec![
                "deg".into(),
                "deg".into(),
            ])),
        );
        record.upsert(
            "axes",
            Value::Array(ArrayValue::from_string_vec(vec![
                "Right Ascension".into(),
                "Declination".into(),
            ])),
        );
        record.upsert(
            "conversionSystem",
            Value::Scalar(ScalarValue::String("J2000".into())),
        );
        record.upsert("longpole", Value::Scalar(ScalarValue::Float64(180.0)));
        record.upsert("latpole", Value::Scalar(ScalarValue::Float64(90.0)));

        let coord = DirectionCoordinate::from_record(&record).unwrap();
        assert!((coord.reference_value()[0] - 120f64.to_radians()).abs() < TOL);
        assert!((coord.reference_value()[1] - 45f64.to_radians()).abs() < TOL);
        assert!((coord.increment()[0] - (-1f64).to_radians()).abs() < TOL);
        assert!((coord.increment()[1] - 1f64.to_radians()).abs() < TOL);
        assert!((coord.longpole() - PI).abs() < TOL);
        assert!((coord.latpole() - FRAC_PI_2).abs() < TOL);
    }

    #[test]
    fn casa_record_serialization_uses_legacy_fields() {
        let coord = make_sin_coord().with_longpole(PI).with_latpole(FRAC_PI_2);
        let rec = coord.to_casa_record();
        assert_eq!(
            rec.get("system"),
            Some(&Value::Scalar(ScalarValue::String("J2000".into())))
        );
        assert_eq!(
            rec.get("conversionSystem"),
            Some(&Value::Scalar(ScalarValue::String("J2000".into())))
        );
        assert_eq!(
            rec.get("units"),
            Some(&Value::Array(ArrayValue::from_string_vec(vec![
                "rad".into(),
                "rad".into()
            ])))
        );
        assert_eq!(
            rec.get("projection_parameters"),
            Some(&Value::Array(ArrayValue::from_f64_vec(vec![0.0, 0.0])))
        );
        assert_eq!(
            rec.get("longpole"),
            Some(&Value::Scalar(ScalarValue::Float64(180.0)))
        );
        assert_eq!(
            rec.get("latpole"),
            Some(&Value::Scalar(ScalarValue::Float64(90.0)))
        );
    }
}

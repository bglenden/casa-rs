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

mod mixed;
mod reference_frame;

use casa_core::measures::MeasFrame;
use casa_core::measures::direction::{DirectionRef, MDirection};
use casa_core::quanta::{Quantity, Unit};
use casa_core::{ArrayD, ArrayValue, RecordValue, ScalarValue, Value};
use ndarray::{Array2, IxDyn, ShapeBuilder};

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
#[derive(Debug, Clone, PartialEq)]
pub struct DirectionCoordinate {
    /// The map projection used for this coordinate.
    projection: Projection,
    /// The celestial reference frame (J2000, GALACTIC, etc.).
    direction_ref: DirectionRef,
    /// The reference frame used by pixel/world conversion.
    conversion_ref: DirectionRef,
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
    /// Native angles to search when only one sky coordinate is supplied.
    mix_ranges: [[f64; 2]; 2],
    /// The angle units the coordinate is stored and presented in, as
    /// casacore's `worldAxisUnits` (CASA's images use `'`, `deg` or `rad`);
    /// it computes in radians whatever they are.
    world_units: [String; 2],
    /// Names supplied by the stored coordinate record, if any.
    world_names: Option<[String; 2]>,
    removed_axis: Option<RemovedAxis>,
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub(crate) struct RemovedAxis {
    pub index: usize,
    pub pixel: f64,
    pub world_rad: f64,
}

/// Returns the native latitude of the fiducial point for a given projection type.
fn fiducial_latitude_degrees(proj: &Projection) -> f64 {
    use crate::projection::ProjectionType;
    match proj.projection_type() {
        ProjectionType::CAR
        | ProjectionType::CYP
        | ProjectionType::CEA
        | ProjectionType::HPX
        | ProjectionType::MER
        | ProjectionType::SFL
        | ProjectionType::AIT
        | ProjectionType::PAR
        | ProjectionType::MOL
        | ProjectionType::PCO
        | ProjectionType::BON
        | ProjectionType::TSC
        | ProjectionType::CSC
        | ProjectionType::QSC => 0.0,
        ProjectionType::COP | ProjectionType::COD | ProjectionType::COE | ProjectionType::COO => {
            proj.parameters().first().copied().unwrap_or_default()
        }
        _ => 90.0, // zenithal projections
    }
}

fn theta_0_for_projection(proj: &Projection) -> f64 {
    fiducial_latitude_degrees(proj).to_radians()
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

    // General case: Calabretta & Greisen (2002) eq. (8), as WCSLIB's
    // `celset` solves it (the fiducial native longitude phi_0 is 0):
    //   delta_p = u +/- v,  u = atan2(sin(theta_0), cos(theta_0) cos(phi_p)),
    //   v = acos(sin(delta_0) / sqrt(sin^2(theta_0) + cos^2(theta_0) cos^2(phi_p))).
    // Of two valid solutions, the one nearest theta_p (LATPOLE) is taken.
    let (sin_t0, cos_t0) = theta_0.sin_cos();
    // WCSLIB's `sind`/`cosd`: exact at multiples of 90°, so a native pole
    // at 180° leaks no 1.2e-16 into alpha_p.
    let (sin_pp, cos_pp) = wcslib_sin_cos_degrees(phi_p.to_degrees());
    let x = cos_t0 * cos_pp;
    let z = x.hypot(sin_t0);
    let delta_p = if z == 0.0 {
        theta_p
    } else {
        let u = sin_t0.atan2(x);
        let v = (delta_0.sin() / z).clamp(-1.0, 1.0).acos();
        let wrap = |angle: f64| {
            if angle > PI {
                angle - 2.0 * PI
            } else if angle < -PI {
                angle + 2.0 * PI
            } else {
                angle
            }
        };
        let (first, second) = (wrap(u + v), wrap(u - v));
        let valid = |angle: f64| angle.abs() < FRAC_PI_2 + 1.0e-10;
        let delta_p = if (theta_p - first).abs() < (theta_p - second).abs() {
            if valid(first) { first } else { second }
        } else if valid(second) {
            second
        } else {
            first
        };
        delta_p.clamp(-FRAC_PI_2, FRAC_PI_2)
    };
    let cos_d0 = delta_0.cos();
    let sin_d0 = delta_0.sin();

    // alpha_p, as WCSLIB's `celset` forms it (Calabretta & Greisen eq. 10):
    // where the native pole is a celestial pole, from the fiducial point and
    // phi_p alone.
    let (sin_dp, cos_dp) = delta_p.sin_cos();
    let z = cos_dp * cos_d0;
    let mut alpha_p = if z.abs() < 1.0e-10 {
        if cos_d0.abs() < 1.0e-10 {
            // The celestial pole is the fiducial point.
            alpha_0
        } else if delta_p > 0.0 {
            // The celestial north pole is the native pole.
            alpha_0 + phi_p - PI
        } else {
            // The celestial south pole is the native pole.
            alpha_0 - phi_p
        }
    } else {
        let x = (sin_t0 - sin_dp * sin_d0) / z;
        let y = sin_pp * cos_t0 / cos_d0;
        alpha_0 - y.atan2(x)
    };

    // WCSLIB's celset gives the native pole the same longitude convention
    // as the fiducial point. This determines the sign of off-centre RA in
    // cylindrical and all-sky projections.
    let full_circle = 2.0 * PI;
    if (alpha_0 >= 0.0 && alpha_p < 0.0) || (alpha_0 < 0.0 && alpha_p < -full_circle) {
        alpha_p += full_circle;
    } else if (alpha_0 < 0.0 && alpha_p > 0.0) || (alpha_0 >= 0.0 && alpha_p > full_circle) {
        alpha_p -= full_circle;
    }

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
        let fiducial = fiducial_latitude_degrees(&projection);
        let theta_0 = fiducial.to_radians();

        // WCSLIB compares the reference and fiducial latitudes in degrees
        // before picking the default native longitude of the celestial pole.
        let longpole = if crval[1].to_degrees() >= fiducial {
            0.0
        } else {
            PI
        };
        let latpole = FRAC_PI_2;

        let native_pole = compute_native_pole(crval[0], crval[1], longpole, latpole, theta_0);

        Self {
            projection,
            direction_ref,
            conversion_ref: direction_ref,
            crval,
            cdelt,
            crpix,
            pc: Array2::eye(2),
            longpole,
            latpole,
            native_pole,
            mix_ranges: mixed::DEFAULT_RANGES,
            world_units: ["rad".into(), "rad".into()],
            world_names: None,
            removed_axis: None,
        }
    }

    pub(crate) fn remove_record_axis(
        &mut self,
        axis: usize,
        pixel: f64,
        world: f64,
    ) -> Result<(), CoordinateError> {
        self.removed_axis = Some(RemovedAxis {
            index: axis,
            pixel,
            world_rad: convert_value_to_unit(
                world,
                &self.world_units[axis],
                "rad",
                "worldreplace",
            )?,
        });
        Ok(())
    }

    pub(crate) fn removed_axis(&self) -> Option<RemovedAxis> {
        self.removed_axis
    }

    pub(crate) fn take_removed_axis(&mut self) -> Option<RemovedAxis> {
        self.removed_axis.take()
    }

    fn select_axis<T>(&self, values: [T; 2]) -> Vec<T> {
        let [x, y] = values;
        match self.removed_axis.map(|axis| axis.index) {
            Some(0) => vec![y],
            Some(_) => vec![x],
            None => vec![x, y],
        }
    }

    /// The angle units the coordinate is stored and presented in: those of
    /// the record it was read from, else radians. Its values, `to_world`
    /// among them, are radians whatever these are.
    pub fn world_units(&self) -> &[String; 2] {
        &self.world_units
    }

    /// The projection reference longitude and latitude in radians, including
    /// the reference of a direction axis removed from the coordinate system.
    pub fn projection_reference(&self) -> [f64; 2] {
        self.crval
    }

    /// Convert between absolute directions and offsets from this coordinate's
    /// reference direction, in radians. Pass `absolute` to recover an absolute
    /// direction. A removed axis uses its stored world replacement.
    ///
    /// # Errors
    /// Returns an error if `values` has the wrong number of visible axes.
    pub fn convert_world_offset(
        &self,
        values: &mut [f64],
        absolute: bool,
    ) -> Result<(), CoordinateError> {
        let [lon, lat] = match (self.removed_axis, &*values) {
            (Some(removed), [value]) if removed.index == 0 => [removed.world_rad, *value],
            (Some(removed), [value]) => [*value, removed.world_rad],
            (None, [longitude, latitude]) => [*longitude, *latitude],
            _ => {
                return Err(CoordinateError::DimensionMismatch {
                    expected: self.n_world_axes(),
                    got: values.len(),
                });
            }
        };
        let [ref_lon, ref_lat] = self.crval;
        let (sin_lon, cos_lon) = ref_lon.sin_cos();
        let (sin_lat, cos_lat) = ref_lat.sin_cos();
        let longitude = if absolute { lon / lat.cos() } else { lon };
        let (sin_longitude, cos_longitude) = longitude.sin_cos();
        let (z, cos_latitude) = lat.sin_cos();
        let x = cos_latitude * cos_longitude;
        let y = cos_latitude * sin_longitude;
        // Unit vectors pointing to the reference, east and north form the local frame.
        let [x, y, z] = if absolute {
            let meridian = x * cos_lat - z * sin_lat;
            [
                meridian * cos_lon - y * sin_lon,
                meridian * sin_lon + y * cos_lon,
                x * sin_lat + z * cos_lat,
            ]
        } else {
            let meridian = x * cos_lon + y * sin_lon;
            [
                meridian * cos_lat + z * sin_lat,
                -x * sin_lon + y * cos_lon,
                -meridian * sin_lat + z * cos_lat,
            ]
        };
        let latitude = z.clamp(-1.0, 1.0).asin();
        let longitude = y.atan2(x);
        let result = [
            if absolute {
                longitude
            } else {
                longitude * latitude.cos()
            },
            latitude,
        ];
        if let Some(removed) = self.removed_axis {
            values[0] = result[1 - removed.index];
        } else {
            values.copy_from_slice(&result);
        }
        Ok(())
    }

    /// The stored unit of each visible world axis, in axis order.
    pub fn visible_world_units(&self) -> Vec<String> {
        self.select_axis(self.world_units.clone())
    }

    /// The local longitude axis, or `None` in a latitude-only slice.
    pub fn longitude_axis(&self) -> Option<usize> {
        if self.removed_axis.is_some_and(|removed| removed.index == 0) {
            None
        } else {
            Some(0)
        }
    }

    /// The visible reference values and increments in their stored world units.
    pub fn reference_and_increment_in_world_units(&self) -> (Vec<f64>, Vec<f64>) {
        (
            self.select_axis(self.in_world_units(self.crval)),
            self.select_axis(self.in_world_units(self.cdelt)),
        )
    }

    /// Stores and presents the coordinate in the angle units `units`, as
    /// casacore's `setWorldAxisUnits` does. Returns `self` for chaining.
    ///
    /// # Errors
    ///
    /// A unit that is not an angle.
    pub fn with_world_units(mut self, units: [&str; 2]) -> Result<Self, CoordinateError> {
        for unit in units {
            convert_value_to_unit(0.0, "rad", unit, "units")?;
        }
        self.world_units = units.map(String::from);
        Ok(self)
    }

    /// `values` in radians, one per axis, in the world units.
    pub fn in_world_units(&self, values: [f64; 2]) -> [f64; 2] {
        [0, 1].map(|axis| {
            convert_value_to_unit(values[axis], "rad", &self.world_units[axis], "world value")
                .expect("world units are angles")
        })
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

    /// Set the native WCS PC matrix and absorb each row's signed scale into
    /// the corresponding increment, as casacore's direction setter does.
    ///
    /// # Errors
    /// The matrix must be 2 by 2 with finite rows that can be normalized.
    pub fn with_normalized_pc_matrix(
        mut self,
        mut pc: Array2<f64>,
    ) -> Result<Self, CoordinateError> {
        if pc.shape() != [2, 2] {
            return Err(CoordinateError::InvalidRecord(
                "direction PC must be 2 by 2".into(),
            ));
        }
        for (axis, mut row) in pc.rows_mut().into_iter().enumerate() {
            let norm_squared = row.iter().map(|value| value * value).sum::<f64>();
            if norm_squared == 1.0 {
                continue;
            }
            if norm_squared == 0.0 || !norm_squared.is_finite() || row[axis] == 0.0 {
                return Err(CoordinateError::InvalidRecord(
                    "wcs wcsset_error: Linear transformation matrix is singular".into(),
                ));
            }
            let scale = norm_squared.sqrt().copysign(row[axis]);
            row.mapv_inplace(|element| element / scale);
            self.cdelt[axis] *= scale;
            if !self.cdelt[axis].is_finite() {
                return Err(CoordinateError::InvalidRecord(
                    "linear transform would produce a non-finite increment".into(),
                ));
            }
        }
        self.pc = pc;
        Ok(self)
    }

    /// Sets the reference pixel, as the coordsys tool's
    /// `setreferencepixel(type='direction')` does, keeping the reference
    /// value. Returns `self` for chaining.
    pub fn with_reference_pixel(mut self, crpix: [f64; 2]) -> Self {
        self.crpix = crpix;
        self
    }

    /// Set the celestial reference value in radians and recompute the
    /// projection's native pole, keeping the reference pixel.
    pub fn with_reference_value(mut self, longitude_latitude: [f64; 2]) -> Self {
        self.crval = longitude_latitude;
        let longpole = self.longpole;
        self.with_longpole(longpole)
    }

    /// Sets the increments, rad per pixel, as the coordsys tool's
    /// `setincrement(type='direction')` does. Returns `self` for chaining.
    pub fn with_increment(mut self, cdelt: [f64; 2]) -> Self {
        self.cdelt = cdelt;
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

    /// Apply explicit WCS poles in radians and retain the selected solution.
    ///
    /// # Errors
    /// Returns an error when the pole cannot represent the reference latitude.
    pub fn with_wcs_poles(
        mut self,
        longitude: f64,
        latitude: f64,
    ) -> Result<Self, CoordinateError> {
        if !longitude.is_finite() || !latitude.is_finite() {
            return Err(CoordinateError::InvalidRecord(
                "wcs wcsset_error: Invalid coordinate transformation parameter".into(),
            ));
        }
        if longitude.to_degrees() != 999.0 {
            self.longpole = longitude;
        }
        if self.crval[1].abs() > FRAC_PI_2 || self.validate_native_pole().is_err() {
            return Err(CoordinateError::InvalidRecord(
                "wcs wcsset_error: Ill-conditioned coordinate transformation parameter".into(),
            ));
        }
        self = self.with_latpole(latitude);
        self.latpole = self.native_pole[1];
        Ok(self)
    }

    /// Returns the projection.
    pub fn projection(&self) -> &Projection {
        &self.projection
    }

    /// Rebuild the coordinate with the selected projection. Retain the native
    /// frame, reference angles, pixel origin, increments, PC and angular units.
    ///
    /// # Errors
    /// Returns an error if the projection or WCS matrix is invalid.
    pub fn with_rebuilt_projection(&self, projection: Projection) -> Result<Self, CoordinateError> {
        projection.validate_conversion_parameters()?;
        let mut rebuilt = Self::new(
            self.direction_ref,
            projection,
            self.crval,
            self.cdelt,
            self.crpix,
        )
        .with_normalized_pc_matrix(self.pc.clone())?;
        rebuilt.validate_native_pole()?;
        rebuilt.latpole = rebuilt.native_pole[1];
        rebuilt.world_units = self.world_units.clone();
        rebuilt.removed_axis = self.removed_axis;
        Ok(rebuilt)
    }

    /// Whether this slant SIN projection uses the North Celestial Pole form.
    pub fn is_ncp(&self) -> bool {
        let declination = self.projection_reference()[1];
        let parameters = self.projection.parameters();
        if self.projection.projection_type() != ProjectionType::SIN
            || parameters.len() != 2
            || parameters[0] != 0.0
            || declination == 0.0
        {
            return false;
        }
        let cotangent = 1.0 / declination.tan();
        (parameters[1] - cotangent).abs() <= 1e-13 * parameters[1].abs().max(cotangent.abs())
    }

    /// Returns the celestial reference frame.
    pub fn direction_ref(&self) -> DirectionRef {
        self.direction_ref
    }

    /// Reference frame presented by pixel/world conversion.
    pub fn conversion_direction_ref(&self) -> DirectionRef {
        self.conversion_ref
    }

    /// Set the reference frame used by pixel/world conversion.
    ///
    /// Conversions that need unavailable frame context return an error when
    /// they are evaluated. Stored reference values remain in the native frame.
    #[must_use]
    pub fn with_conversion_direction_ref(mut self, reference: DirectionRef) -> Self {
        self.conversion_ref = reference;
        self
    }

    /// Returns the native longitude of the celestial pole (phi_p) in radians.
    pub fn longpole(&self) -> f64 {
        self.longpole
    }

    /// Returns the native latitude of the celestial pole (theta_p) in radians.
    pub fn latpole(&self) -> f64 {
        self.latpole
    }

    /// Returns the resolved celestial coordinates `[alpha_p, delta_p]` of
    /// the native pole, in radians.
    ///
    /// Together with [`Self::longpole`], these coordinates define the spherical
    /// rotation used by pixel/world conversion. They are computed from the
    /// reference direction, projection, LONGPOLE and requested LATPOLE. Unlike
    /// [`Self::latpole`], which selects a solution when there are two, this is
    /// the resolved pole: different LATPOLE selectors may give the same rotation.
    /// Reading it does not change the stored coordinate or its serialized record.
    pub fn native_pole(&self) -> [f64; 2] {
        self.native_pole
    }

    pub(crate) fn validate_native_pole(&self) -> Result<(), CoordinateError> {
        let fiducial = fiducial_latitude_degrees(&self.projection);
        if fiducial == 90.0 {
            return Ok(());
        }
        let latitude = self.crval[1].to_degrees();
        let longitude = self.longpole.to_degrees();
        // Use the unclamped WCSLIB candidates. Checking only the residual
        // of the selected pole misses impossibility near LONPOLE = 90°.
        let (u, v) = if longitude == 0.0 {
            (fiducial, 90.0 - latitude)
        } else {
            let (_, cosine) = wcslib_sin_cos_degrees(longitude);
            let (sine_latitude, _) = wcslib_sin_cos_degrees(latitude);
            let (y, cos_fiducial) = wcslib_sin_cos_degrees(fiducial);
            let x = cos_fiducial * cosine;
            let z = x.hypot(y);
            let invalid = || {
                CoordinateError::InvalidRecord("invalid direction coordinate transformation".into())
            };
            if z == 0.0 {
                return if sine_latitude == 0.0 {
                    Ok(())
                } else {
                    Err(invalid())
                };
            }
            let ratio = sine_latitude / z;
            if ratio.abs() - 1.0 >= 1e-10 {
                return Err(invalid());
            }
            (
                wcslib_atan2_degrees(y, x),
                wcslib_acos_degrees(ratio.clamp(-1.0, 1.0)),
            )
        };
        let wrap = |angle: f64| {
            if angle > 180.0 {
                angle - 360.0
            } else if angle < -180.0 {
                angle + 360.0
            } else {
                angle
            }
        };
        if wrap(u + v).abs().min(wrap(u - v).abs()) > 90.0 + 1e-10 {
            return Err(CoordinateError::InvalidRecord(
                "ill-conditioned direction coordinate transformation".into(),
            ));
        }
        Ok(())
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
        projection.validate_conversion_parameters()?;

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
        if rec.get("conversionSystem").is_some()
            && get_optional_string(rec, "conversionSystem").is_none()
        {
            return Err(CoordinateError::InvalidRecord(
                "invalid direction conversion frame".into(),
            ));
        }
        if let Some(name) = get_optional_string(rec, "conversionSystem") {
            coord.conversion_ref = DirectionRef::from_str(&name).map_err(|_| {
                CoordinateError::InvalidRecord(format!(
                    "unsupported direction conversion frame {name:?} (#977)"
                ))
            })?;
        }
        coord.world_units = [axis_units[0].clone(), axis_units[1].clone()];
        if let Some(names) = get_optional_vec_string(rec, "axes") {
            if names.len() != 2 {
                return Err(CoordinateError::InvalidRecord(format!(
                    "direction coordinate axes has {} values, expected 2",
                    names.len()
                )));
            }
            coord.world_names = Some([names[0].clone(), names[1].clone()]);
        }

        if let Some(pc_flat) = get_optional_vec_f64(rec, "pc") {
            if pc_flat.len() != 4 {
                return Err(CoordinateError::InvalidRecord(format!(
                    "direction pc matrix has {} elements, expected 4",
                    pc_flat.len()
                )));
            }
            // casacore's matrix is the transpose of the FITS PC matrix this
            // coordinate holds (`LinearXform`: `pc[i*n+j] = pcIn(j,i)`), so
            // its elements in row order are the PC's in column order.
            let pc = Array2::from_shape_vec((2, 2).f(), pc_flat).map_err(|err| {
                CoordinateError::InvalidRecord(format!("invalid direction pc matrix: {err}"))
            })?;
            coord = coord.with_pc_matrix(pc.as_standard_layout().into_owned());
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

    /// The world coordinates `[longitude, latitude]`, in radians, of
    /// `pixel`: what [`Coordinate::to_world`] returns, without allocating,
    /// for callers that convert many pixels (casacore's
    /// `DirectionCoordinate::toWorld`).
    ///
    /// # Errors
    ///
    /// A pixel the projection has no sky position for (beyond a SIN
    /// image's horizon, for instance), or a frame conversion requiring
    /// context that is not yet available (#977).
    pub fn pixel_to_world(&self, pixel: [f64; 2]) -> Result<[f64; 2], CoordinateError> {
        let (x, y) = self.pixel_to_intermediate(&pixel);
        let (phi, theta) = self.projection.deproject(x, y)?;
        let (longitude, latitude) = self.native_to_celestial(phi, theta);
        self.convert_frame(
            [longitude, latitude],
            self.direction_ref,
            self.conversion_ref,
        )
    }

    /// The pixel of `world`, `[longitude, latitude]` in radians in this
    /// coordinate's frame: what [`Coordinate::to_pixel`] returns, without
    /// allocating (casacore's `DirectionCoordinate::toPixel`).
    ///
    /// # Errors
    ///
    /// A direction the projection cannot place (behind a TAN image's
    /// tangent plane, for instance), a nonfinite pixel, a singular PC matrix,
    /// or a frame conversion requiring context that is not yet available (#977).
    pub fn world_to_pixel(&self, world: [f64; 2]) -> Result<[f64; 2], CoordinateError> {
        let world = self.convert_frame(world, self.conversion_ref, self.direction_ref)?;
        let (phi, theta) = self.celestial_to_native(world[0], world[1]);
        let (x, y) = self.projection.project(phi, theta)?;
        let (px, py) = self.intermediate_to_pixel(x, y)?;
        Ok([px, py])
    }

    fn convert_frame(
        &self,
        world: [f64; 2],
        from: DirectionRef,
        to: DirectionRef,
    ) -> Result<[f64; 2], CoordinateError> {
        if from == to {
            return Ok(world);
        }
        let converted = MDirection::from_angles(world[0], world[1], from)
            .convert_to(to, &MeasFrame::new())
            .map_err(|error| CoordinateError::ConversionFailed(format!("{error} (#977)")))?;
        let (longitude, latitude) = converted.as_angles();
        // MVDirection::get uses atan2's signed longitude.
        let longitude = if longitude > PI {
            longitude - 2.0 * PI
        } else {
            longitude
        };
        Ok([longitude, latitude])
    }

    /// Pixel to intermediate world coordinates.
    fn pixel_to_intermediate(&self, pixel: &[f64]) -> (f64, f64) {
        let dx = pixel[0] - self.crpix[0];
        let dy = pixel[1] - self.crpix[1];

        // casacore's DirectionCoordinate stores WCS linear coordinates in
        // degrees even when its public axis units are radians. Preserve that
        // operation order so serialized zero-pixel world replacements agree
        // with WCSLIB down to the final bit.
        let to_degrees = 1.0 / (PI / 180.0);
        let x_degrees =
            (self.cdelt[0] * to_degrees) * (self.pc[[0, 0]] * dx + self.pc[[0, 1]] * dy);
        let y_degrees =
            (self.cdelt[1] * to_degrees) * (self.pc[[1, 0]] * dx + self.pc[[1, 1]] * dy);
        // Keep the D2R multiplication grouped to reproduce CASA's serialized
        // zero-pixel world replacement exactly.
        let x = x_degrees * (PI / 180.0);
        let y = y_degrees * (PI / 180.0);

        (x, y)
    }

    /// Intermediate world coordinates to pixel.
    fn intermediate_to_pixel(&self, x: f64, y: f64) -> Result<(f64, f64), CoordinateError> {
        // Undo cdelt scaling
        let sx = x / self.cdelt[0];
        let sy = y / self.cdelt[1];

        // Invert PC matrix
        let det = self.pc[[0, 0]] * self.pc[[1, 1]] - self.pc[[0, 1]] * self.pc[[1, 0]];
        if det == 0.0 || !det.is_finite() {
            return Err(CoordinateError::SingularTransform(
                "singular PC matrix in DirectionCoordinate",
            ));
        }
        let dx = (self.pc[[1, 1]] * sx - self.pc[[0, 1]] * sy) / det;
        let dy = (-self.pc[[1, 0]] * sx + self.pc[[0, 0]] * sy) / det;

        let pixel = (dx + self.crpix[0], dy + self.crpix[1]);
        if !pixel.0.is_finite() || !pixel.1.is_finite() {
            return Err(CoordinateError::ConversionFailed(
                "direction has no finite pixel coordinate".into(),
            ));
        }
        Ok(pixel)
    }

    /// Native spherical (phi, theta) to celestial (lon, lat) using the
    /// Calabretta & Greisen (2002) spherical rotation (eq. 2).
    ///
    /// Uses the pre-computed native pole (alpha_p, delta_p).
    fn native_to_celestial(&self, phi: f64, theta: f64) -> (f64, f64) {
        let (alpha_p, delta_p) = (self.native_pole[0], self.native_pole[1]);
        let radians_per_degree = PI / 180.0;
        let to_degrees = 1.0 / radians_per_degree;
        let phi_degrees = phi * to_degrees;
        let theta_degrees = theta * to_degrees;
        let euler_0 = alpha_p * to_degrees;
        let euler_1 = 90.0 - delta_p * to_degrees;
        let euler_2 = self.longpole * to_degrees;
        let (euler_4, euler_3) = wcslib_sin_cos_degrees(euler_1);
        let (sin_theta, cos_theta) = wcslib_sin_cos_degrees(theta_degrees);
        let dphi = phi_degrees - euler_2;
        let (sin_phi, cos_phi) = wcslib_sin_cos_degrees(dphi);

        let cos_theta_3 = cos_theta * euler_3;
        let cos_theta_4 = cos_theta * euler_4;
        let sin_theta_3 = sin_theta * euler_3;
        let sin_theta_4 = sin_theta * euler_4;

        let mut x = sin_theta_4 - cos_theta_3 * cos_phi;
        if x.abs() < 1.0e-5 {
            x = -wcslib_cos_degrees(theta_degrees + euler_1) + cos_theta_3 * (1.0 - cos_phi);
        }
        let y = -cos_theta * sin_phi;
        let delta_longitude = if x != 0.0 || y != 0.0 {
            wcslib_atan2_degrees(y, x)
        } else if euler_1 < 90.0 {
            dphi + 180.0
        } else {
            -dphi
        };
        let mut longitude_degrees = euler_0 + delta_longitude;
        if euler_0 >= 0.0 {
            if longitude_degrees < 0.0 {
                longitude_degrees += 360.0;
            }
        } else if longitude_degrees > 0.0 {
            longitude_degrees -= 360.0;
        }
        if longitude_degrees > 360.0 {
            longitude_degrees -= 360.0;
        } else if longitude_degrees < -360.0 {
            longitude_degrees += 360.0;
        }

        let latitude_degrees = if dphi % 180.0 == 0.0 {
            let mut latitude = theta_degrees + cos_phi * euler_1;
            if latitude > 90.0 {
                latitude = 180.0 - latitude;
            }
            if latitude < -90.0 {
                latitude = -180.0 - latitude;
            }
            latitude
        } else {
            let z = sin_theta_3 + cos_theta_4 * cos_phi;
            if z.abs() > 0.99 {
                z.signum() * wcslib_acos_degrees((x * x + y * y).sqrt())
            } else {
                wcslib_asin_degrees(z)
            }
        };

        (
            longitude_degrees / to_degrees,
            latitude_degrees / to_degrees,
        )
    }

    /// Celestial (lon, lat) to native spherical (phi, theta), as WCSLIB's
    /// `sphs2x` forms them: the native latitude exactly when the longitude
    /// offset is a multiple of 180°, and from `acos` of the native
    /// longitude's arguments where `|sin θ| > 0.99`, since `asin` near ±1
    /// loses half the precision (up to 1.5e-8 rad at the reference
    /// direction itself, 0.009 of a 0.35″ pixel). The native longitude is
    /// normalized to `[-π, π]`, as `sphs2x` normalizes it, so a projection
    /// whose reference is on its native equator places a direction near
    /// its reference near the reference pixel whatever its native pole's
    /// longitude (180° south of the celestial equator).
    fn celestial_to_native(&self, lon: f64, lat: f64) -> (f64, f64) {
        let (alpha_p, delta_p) = (self.native_pole[0], self.native_pole[1]);
        let to_degrees = 180.0 / PI;
        let latitude_degrees = lat * to_degrees;
        let euler_1 = 90.0 - delta_p * to_degrees;
        if euler_1 == 0.0 || euler_1 == 180.0 {
            // Coincident polar axes need only a longitude shift and an
            // optional pole flip. At a pole this retains the longitude
            // supplied for cylindrical projections.
            let (phi, theta) = if euler_1 == 0.0 {
                (lon + (self.longpole - PI - alpha_p) % (2.0 * PI), lat)
            } else {
                ((self.longpole + alpha_p) % (2.0 * PI) - lon, -lat)
            };
            let phi = phi % (2.0 * PI);
            return (
                if phi > PI {
                    phi - 2.0 * PI
                } else if phi < -PI {
                    phi + 2.0 * PI
                } else {
                    phi
                },
                theta,
            );
        }
        let (euler_4, euler_3) = wcslib_sin_cos_degrees(euler_1);
        let (sin_lat, cos_lat) = wcslib_sin_cos_degrees(latitude_degrees);
        let dlng = (lon - alpha_p) * to_degrees;
        let (sin_dlng, cos_dlng) = wcslib_sin_cos_degrees(dlng);

        let cos_lat_3 = cos_lat * euler_3;
        let cos_lat_4 = cos_lat * euler_4;
        let sin_lat_3 = sin_lat * euler_3;
        let sin_lat_4 = sin_lat * euler_4;

        // Native longitude.
        let mut x = sin_lat_4 - cos_lat_3 * cos_dlng;
        if x.abs() < 1.0e-5 {
            x = -wcslib_cos_degrees(latitude_degrees + euler_1) + cos_lat_3 * (1.0 - cos_dlng);
        }
        let y = -cos_lat * sin_dlng;
        let mut phi = self.longpole + y.atan2(x);
        if phi > PI {
            phi -= 2.0 * PI;
        } else if phi < -PI {
            phi += 2.0 * PI;
        }

        // Native latitude.
        let theta_degrees = if dlng % 180.0 == 0.0 {
            let mut theta = latitude_degrees + cos_dlng * euler_1;
            if theta > 90.0 {
                theta = 180.0 - theta;
            }
            if theta < -90.0 {
                theta = -180.0 - theta;
            }
            theta
        } else {
            let z = sin_lat_3 + cos_lat_4 * cos_dlng;
            if z.abs() > 0.99 {
                z.signum() * wcslib_acos_degrees((x * x + y * y).sqrt())
            } else {
                wcslib_asin_degrees(z)
            }
        };

        (phi, theta_degrees / to_degrees)
    }
}

pub(crate) fn wcslib_sin_cos_degrees(angle_degrees: f64) -> (f64, f64) {
    if angle_degrees % 90.0 == 0.0 {
        let quadrant = ((angle_degrees / 90.0 + 0.5).floor().abs() as i64) % 4;
        return match quadrant {
            0 => (0.0, 1.0),
            1 => (if angle_degrees > 0.0 { 1.0 } else { -1.0 }, 0.0),
            2 => (0.0, -1.0),
            3 => (if angle_degrees > 0.0 { -1.0 } else { 1.0 }, 0.0),
            _ => unreachable!(),
        };
    }
    let angle_radians = angle_degrees * (PI / 180.0);
    (angle_radians.sin(), angle_radians.cos())
}

fn wcslib_cos_degrees(angle_degrees: f64) -> f64 {
    wcslib_sin_cos_degrees(angle_degrees).1
}

fn wcslib_acos_degrees(value: f64) -> f64 {
    if value >= 1.0 && value - 1.0 < 1.0e-10 {
        0.0
    } else if value == 0.0 {
        90.0
    } else if value <= -1.0 && value + 1.0 > -1.0e-10 {
        180.0
    } else {
        value.acos() * (180.0 / PI)
    }
}

fn wcslib_asin_degrees(value: f64) -> f64 {
    if value <= -1.0 && value + 1.0 > -1.0e-10 {
        -90.0
    } else if value == 0.0 {
        0.0
    } else if value >= 1.0 && value - 1.0 < 1.0e-10 {
        90.0
    } else {
        value.asin() * (180.0 / PI)
    }
}

fn wcslib_atan2_degrees(y: f64, x: f64) -> f64 {
    if y == 0.0 {
        if x >= 0.0 { 0.0 } else { 180.0 }
    } else if x == 0.0 {
        if y > 0.0 { 90.0 } else { -90.0 }
    } else {
        y.atan2(x) * (180.0 / PI)
    }
}

pub(crate) fn convert_value_to_unit(
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
        2 - usize::from(self.removed_axis.is_some())
    }

    fn n_world_axes(&self) -> usize {
        self.n_pixel_axes()
    }

    fn to_world(&self, pixel: &[f64]) -> Result<Vec<f64>, CoordinateError> {
        if pixel.len() != self.n_pixel_axes() {
            return Err(CoordinateError::DimensionMismatch {
                expected: self.n_pixel_axes(),
                got: pixel.len(),
            });
        }

        let full = if let Some(removed) = self.removed_axis {
            let mut full = [pixel[0]; 2];
            full[removed.index] = removed.pixel;
            full
        } else {
            [pixel[0], pixel[1]]
        };
        Ok(self.select_axis(self.pixel_to_world(full)?))
    }

    fn to_pixel(&self, world: &[f64]) -> Result<Vec<f64>, CoordinateError> {
        if world.len() != self.n_world_axes() {
            return Err(CoordinateError::DimensionMismatch {
                expected: self.n_world_axes(),
                got: world.len(),
            });
        }

        let full = if let Some(removed) = self.removed_axis {
            let mut full = [world[0]; 2];
            full[removed.index] = removed.world_rad;
            full
        } else {
            [world[0], world[1]]
        };
        Ok(self.select_axis(self.world_to_pixel(full)?))
    }

    fn reference_value(&self) -> Vec<f64> {
        self.select_axis(self.crval)
    }

    fn reference_pixel(&self) -> Vec<f64> {
        self.select_axis(self.crpix)
    }

    fn increment(&self) -> Vec<f64> {
        self.select_axis(self.cdelt)
    }

    fn axis_names(&self) -> Vec<String> {
        self.select_axis(self.full_axis_names())
    }

    fn axis_units(&self) -> Vec<String> {
        vec!["rad".into(); self.n_world_axes()]
    }
}

impl DirectionCoordinate {
    fn full_axis_names(&self) -> [String; 2] {
        self.world_names.clone().unwrap_or_else(|| {
            let names = match self.direction_ref {
                DirectionRef::J2000
                | DirectionRef::JMEAN
                | DirectionRef::APP
                | DirectionRef::B1950
                | DirectionRef::B1950Vla
                | DirectionRef::BMEAN
                | DirectionRef::BTRUE
                | DirectionRef::ICRS => ["Right Ascension", "Declination"],
                DirectionRef::HADEC => ["Hour Angle", "Declination"],
                _ => ["Longitude", "Latitude"],
            };
            names.map(str::to_owned)
        })
    }

    pub(crate) fn to_record(&self) -> RecordValue {
        let mut rec = RecordValue::default();
        rec.upsert(
            "system",
            Value::Scalar(ScalarValue::String(self.direction_ref.as_str().into())),
        );
        rec.upsert(
            "projection",
            Value::Scalar(ScalarValue::String(self.projection.name().into())),
        );
        rec.upsert(
            "projection_parameters",
            Value::Array(ArrayValue::from_f64_vec(
                self.projection.parameters().to_vec(),
            )),
        );
        // In the world units, as casacore's `DirectionCoordinate::save`
        // writes them.
        rec.upsert(
            "crval",
            Value::Array(ArrayValue::from_f64_vec(
                self.in_world_units(self.crval).to_vec(),
            )),
        );
        rec.upsert(
            "crpix",
            Value::Array(ArrayValue::from_f64_vec(self.crpix.to_vec())),
        );
        rec.upsert(
            "cdelt",
            Value::Array(ArrayValue::from_f64_vec(
                self.in_world_units(self.cdelt).to_vec(),
            )),
        );
        // casacore's matrix, the PC's transpose (see `from_record`).
        rec.upsert(
            "pc",
            Value::Array(ArrayValue::Float64(
                ArrayD::from_shape_vec(IxDyn(&[2, 2]).f(), self.pc.iter().copied().collect())
                    .expect("2x2 direction pc matrix"),
            )),
        );
        rec.upsert(
            "axes",
            Value::Array(ArrayValue::from_string_vec(self.full_axis_names().to_vec())),
        );
        rec.upsert(
            "units",
            Value::Array(ArrayValue::from_string_vec(self.world_units.to_vec())),
        );
        rec.upsert(
            "conversionSystem",
            Value::Scalar(ScalarValue::String(self.conversion_ref.as_str().into())),
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
}

#[cfg(test)]
mod tests;

// SPDX-License-Identifier: LGPL-3.0-or-later

use casa_core::measures::direction::DirectionRef;

use crate::{CoordinateError, ProjectionType};

use super::DirectionCoordinate;

impl DirectionCoordinate {
    /// Rebuild the projection in a native frame. When supplied, `reference`
    /// gives the adjusted reference angles in radians. Increments and PC
    /// are retained.
    ///
    /// # Errors
    /// Returns an error if the PC matrix cannot be normalized or the
    /// reference angles cannot be represented with the projection's pole.
    pub fn with_reassigned_reference_frame(
        &self,
        frame: DirectionRef,
        reference: Option<[f64; 2]>,
    ) -> Result<Self, CoordinateError> {
        let mut coordinate = Self::new(
            frame,
            self.projection.clone(),
            self.crval,
            self.cdelt,
            self.crpix,
        )
        .with_normalized_pc_matrix(self.pc.clone())?;
        if let Some(mut reference) = reference {
            if self.projection.projection_type() == ProjectionType::SFL
                && coordinate.cdelt[1] != 0.0
            {
                // Casacore puts an adjusted SFL reference on the equator and
                // shifts the reference pixel to preserve its latitude offset.
                coordinate.crpix[1] -= reference[1] / coordinate.cdelt[1];
                reference[1] = 0.0;
            }
            coordinate = coordinate.with_reference_value(reference);
            coordinate.validate_native_pole().map_err(|_| {
                CoordinateError::InvalidRecord(
                    "wcs wcsset_error: Ill-conditioned coordinate transformation parameter".into(),
                )
            })?;
        }
        coordinate.latpole = coordinate.native_pole[1];
        Ok(coordinate)
    }

    /// Rebuild the direction coordinate in a new native reference frame.
    ///
    /// Keep the projection, pixel origin, and angular increments. The new
    /// coordinate starts with an identity PC matrix. The returned angle in
    /// radians describes the old pixel plane's rotation relative to the new
    /// frame, with clockwise positive as in casacore's `convert`.
    ///
    /// # Errors
    /// Returns an error for nonsquare pixels, an unavailable frame conversion,
    /// or an invalid projected offset used to measure the rotation.
    pub fn convert_reference_frame(
        &self,
        frame: DirectionRef,
    ) -> Result<(Self, f64), CoordinateError> {
        // Casacore restores direction records with unit-length PC rows. Their
        // scale belongs in the increments before checking for square pixels.
        let mut old = self
            .clone()
            .with_normalized_pc_matrix(self.pc.clone())?
            .with_conversion_direction_ref(frame);
        let [first, second] = old.in_world_units(old.cdelt).map(f64::abs);
        if (first - second).abs() > 2e-10 * first.max(second) {
            return Err(CoordinateError::ConversionFailed(
                "Coordinate rotation can only be performed on a coordinate that has square pixels"
                    .into(),
            ));
        }
        let world = old.pixel_to_world(old.crpix)?;
        let mut converted = Self::new(frame, self.projection.clone(), world, old.cdelt, old.crpix);
        // CASA saves the native pole selected by the WCS constructor.
        converted.latpole = converted.native_pole[1];

        // Measure the new frame's positive latitude direction on the old
        // projection. A one-degree offset toward the equator avoids crossing
        // a pole, and a zero reference pixel avoids cancellation.
        old.crpix = [0.0; 2];
        old.cdelt = old.cdelt.map(|step| 1.0_f64.to_radians().copysign(step));
        let toward_equator: f64 = if world[1] < 0.0 { 1.0 } else { -1.0 };
        let [x, y] = old.world_to_pixel([world[0], world[1] + toward_equator.to_radians()])?;
        let (x, y) = (x * toward_equator, y * toward_equator);
        let length = x.hypot(y);
        if !length.is_finite() || length == 0.0 {
            return Err(CoordinateError::ConversionFailed(
                "Unable to measure the rotation of the converted direction coordinate".into(),
            ));
        }
        let angle = (y / length).clamp(-1.0, 1.0).acos();
        // WCSLIB's degree trigonometry gives zero at the negative meridian.
        // Radian roundoff at that meridian must leave the half-turn positive.
        let on_meridian = angle == std::f64::consts::PI && x.abs() <= 8.0 * f64::EPSILON * length;
        Ok((
            converted,
            if x > 0.0 && !on_meridian {
                -angle
            } else {
                angle
            },
        ))
    }
}

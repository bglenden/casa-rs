// SPDX-License-Identifier: LGPL-3.0-or-later

//! The celestial projection of an image's direction axes (CASA's tclean
//! `projection`), and the direction coordinate a specification makes,
//! which converts between the image's pixels and sky directions.

use casa_coordinates::{DirectionCoordinate, Projection as CoordinateProjection, ProjectionType};
use ndarray::arr2;

use crate::geometry::DirectionCoordinateSpec;

/// The celestial projection of an image's direction axes: CASA's tclean
/// `projection` (`SynthesisParamsImage::buildCoordinateSystem`).
///
/// CASA's gridders sample a regular grid of direction cosines about the
/// image's reference direction whatever the projection, so the projection
/// leaves an image's pixel values alone. It decides which sky direction
/// each pixel labels, and so where a direction lands on the image: mosaic
/// and A-projection pointings, analytic primary beams, mask images and
/// facet centres.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Projection {
    /// Orthographic, `SIN`: tclean's default.
    Sin,
    /// The AIPS north-celestial-pole projection, `NCP`, which CASA writes
    /// as `SIN` with the slant `[0, cot δ₀]` of the reference latitude δ₀.
    Ncp,
    /// Gnomonic, `TAN`.
    Tan,
    /// Zenithal equidistant, `ARC`.
    Arc,
    /// Stereographic, `STG`.
    Stg,
    /// Zenithal equal-area, `ZEA`.
    Zea,
    /// Plate carrée, `CAR`.
    Car,
    /// Sanson-Flamsteed, `SFL`.
    Sfl,
    /// Mercator, `MER`.
    Mer,
    /// Hammer-Aitoff, `AIT`.
    Ait,
}

impl Projection {
    /// The projection tclean names `name`, spelt exactly as casacore's
    /// `Projection::type` reads it (CASA 6.7.6.14 refuses `'tan'` and
    /// `' TAN'`), or `None` for one casa-rs does not image in.
    #[must_use]
    pub fn from_casa_name(name: &str) -> Option<Self> {
        Some(match name {
            "SIN" => Self::Sin,
            "NCP" => Self::Ncp,
            "TAN" => Self::Tan,
            "ARC" => Self::Arc,
            "STG" => Self::Stg,
            "ZEA" => Self::Zea,
            "CAR" => Self::Car,
            "SFL" => Self::Sfl,
            "MER" => Self::Mer,
            "AIT" => Self::Ait,
            _ => return None,
        })
    }

    /// The projection, with its parameters, that an image of reference
    /// latitude `latitude_rad` records: CASA gives every projection the two
    /// parameters `[0, 0]`, and `NCP` the `SIN` slant `[0, cot δ₀]`.
    #[must_use]
    pub fn coordinate_projection(self, latitude_rad: f64) -> CoordinateProjection {
        let kind = match self {
            Self::Sin | Self::Ncp => ProjectionType::SIN,
            Self::Tan => ProjectionType::TAN,
            Self::Arc => ProjectionType::ARC,
            Self::Stg => ProjectionType::STG,
            Self::Zea => ProjectionType::ZEA,
            Self::Car => ProjectionType::CAR,
            Self::Sfl => ProjectionType::SFL,
            Self::Mer => ProjectionType::MER,
            Self::Ait => ProjectionType::AIT,
        };
        let slant = if self == Self::Ncp {
            1.0 / latitude_rad.tan()
        } else {
            0.0
        };
        CoordinateProjection::with_parameters(kind, vec![0.0, slant])
    }

    /// The projection a direction coordinate of reference latitude
    /// `latitude_rad` states with the parameters CASA writes for it
    /// ([`Self::coordinate_projection`]; none stands for `[0, 0]`, as
    /// casa-rs wrote `SIN` before), or `None` for one casa-rs does not
    /// image in.
    #[must_use]
    pub fn of_coordinate(projection: &CoordinateProjection, latitude_rad: f64) -> Option<Self> {
        let parameters = match projection.parameters() {
            [] => &[0.0, 0.0][..],
            parameters => parameters,
        };
        [
            Self::Sin,
            Self::Ncp,
            Self::Tan,
            Self::Arc,
            Self::Stg,
            Self::Zea,
            Self::Car,
            Self::Sfl,
            Self::Mer,
            Self::Ait,
        ]
        .into_iter()
        .find(|candidate| {
            let expected = candidate.coordinate_projection(latitude_rad);
            expected.projection_type() == projection.projection_type()
                && expected.parameters().len() == parameters.len()
                && expected
                    .parameters()
                    .iter()
                    .zip(parameters)
                    .all(|(expected, stated)| (expected - stated).abs() <= 1e-9)
        })
    }

    /// Whether the projection is zenithal: its reference direction is its
    /// native pole, so the native pole's longitude is all a pole states.
    /// The others (cylindrical `CAR` and `MER`, pseudo-cylindrical `SFL`,
    /// conventional `AIT`) put it on the native equator.
    #[must_use]
    pub const fn zenithal(self) -> bool {
        !matches!(self, Self::Car | Self::Sfl | Self::Mer | Self::Ait)
    }
}

impl DirectionCoordinateSpec {
    /// The direction coordinate this specification states: its
    /// projection, reference direction and pixel, increments, PC matrix and
    /// poles, as casacore's `DirectionCoordinate` converts with them. Build
    /// it once per image domain and convert with it
    /// ([`DirectionCoordinate::world_to_pixel`],
    /// [`DirectionCoordinate::pixel_to_world`]).
    #[must_use]
    pub fn coordinate(self) -> DirectionCoordinate {
        let reference = self.reference_direction();
        let [longitude_pole, latitude_pole] = self.pole_deg();
        DirectionCoordinate::new(
            reference.frame().direction_ref(),
            self.projection()
                .coordinate_projection(reference.latitude_rad()),
            [reference.longitude_rad(), reference.latitude_rad()],
            self.increment_rad(),
            self.reference_pixel(),
        )
        .with_pc_matrix(arr2(&self.pc()))
        .with_longpole(longitude_pole.to_radians())
        .with_latpole(latitude_pole.to_radians())
    }
}

#[cfg(test)]
mod tests {
    use super::Projection;
    use crate::geometry::{DirectionCoordinateSpec, DirectionFrame, SkyDirection};

    const ALL: [Projection; 10] = [
        Projection::Sin,
        Projection::Ncp,
        Projection::Tan,
        Projection::Arc,
        Projection::Stg,
        Projection::Zea,
        Projection::Car,
        Projection::Sfl,
        Projection::Mer,
        Projection::Ait,
    ];

    /// Each projection's coordinate takes its reference pixel to its
    /// reference direction and every pixel back to itself, and reads back
    /// as itself from the parameters it writes.
    #[test]
    fn every_projection_round_trips_its_pixels() {
        let cell = 8.0_f64.to_radians() / 3600.0;
        for projection in ALL {
            for latitude in [0.71, -0.4] {
                let poles = if projection.zenithal() {
                    [180.0, 0.0]
                } else if latitude >= 0.0 {
                    [0.0, 90.0]
                } else {
                    [180.0, 90.0]
                };
                let spec = DirectionCoordinateSpec::new(
                    projection,
                    SkyDirection::new(DirectionFrame::J2000, 5.2, latitude),
                    [50.0, 50.0],
                    [-cell, cell],
                    [[1.0, 0.0], [0.0, 1.0]],
                    poles,
                );
                let coordinate = spec.coordinate();
                let [longitude, centre] = coordinate.pixel_to_world([50.0, 50.0]).unwrap();
                assert!(
                    (longitude - 5.2).abs() < 1e-12 && (centre - latitude).abs() < 1e-12,
                    "{projection:?} at {latitude}: {longitude} {centre}"
                );
                for pixel in [[0.0, 0.0], [99.0, 99.0], [10.0, 80.0], [75.5, 20.25]] {
                    let world = coordinate.pixel_to_world(pixel).unwrap();
                    let back = coordinate.world_to_pixel(world).unwrap();
                    assert!(
                        (back[0] - pixel[0]).abs() < 1e-8 && (back[1] - pixel[1]).abs() < 1e-8,
                        "{projection:?} at {latitude}: {pixel:?} -> {back:?}"
                    );
                }
                assert_eq!(
                    Projection::of_coordinate(coordinate.projection(), latitude),
                    Some(projection)
                );
            }
        }
    }

    /// tclean names the projections exactly as casacore spells them.
    #[test]
    fn tclean_names_its_projections() {
        assert_eq!(Projection::from_casa_name("TAN"), Some(Projection::Tan));
        assert_eq!(Projection::from_casa_name("NCP"), Some(Projection::Ncp));
        for other in ["tan", " TAN", "ncp", "AZP"] {
            assert_eq!(Projection::from_casa_name(other), None, "{other:?}");
        }
    }
}

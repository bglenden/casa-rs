// SPDX-License-Identifier: LGPL-3.0-or-later

use casa_coordinates::{
    Coordinate, CoordinateModel, CoordinateSystem, CoordinateType, DirectionCoordinate,
};
use casa_core::measures::{MeasFrame, direction::MDirection};

use casa_images::ImageError;

use super::SpatialShape;

/// Transform a region defined on the attached image into the input's pixel plane.
pub(super) struct Projection {
    definition: DirectionCoordinate,
    input: DirectionCoordinate,
    frame: MeasFrame,
}

impl Projection {
    pub(super) fn new(
        origin: &CoordinateSystem,
        input: &CoordinateSystem,
    ) -> Result<Self, ImageError> {
        let direction = |coordinates: &CoordinateSystem| {
            if let Some(index) = coordinates.find_coordinate(CoordinateType::Direction)
                && let CoordinateModel::Direction(direction) = coordinates.coordinate(index)
                && direction.n_pixel_axes() == 2
            {
                return Ok(direction
                    .clone()
                    .with_conversion_direction_ref(direction.direction_ref()));
            }
            Err(ImageError::CasaSelection(
                "Coordinate system has no direction coordinate".into(),
            ))
        };
        let mut frame = MeasFrame::new();
        if let Some(epoch) = origin.obs_info().date.as_ref() {
            frame = frame.with_epoch(epoch.clone());
        }
        if let Some(position) = origin.obs_info().telescope_position.as_ref() {
            frame = frame.with_position(position.clone());
        }
        Ok(Self {
            definition: direction(origin)?,
            input: direction(input)?,
            frame,
        })
    }

    fn point(&self, pixel: [f64; 2]) -> Result<[f64; 2], ImageError> {
        let world = self.definition.pixel_to_world(pixel)?;
        let world = if self.definition.direction_ref() == self.input.direction_ref() {
            world
        } else {
            let direction =
                MDirection::from_angles(world[0], world[1], self.definition.direction_ref())
                    .convert_to(self.input.direction_ref(), &self.frame)
                    .map_err(|error| ImageError::CasaSelection(error.to_string()))?;
            [direction.longitude_rad(), direction.latitude_rad()]
        };
        Ok(self.input.world_to_pixel(world)?)
    }

    pub(super) fn project_shape(
        &self,
        shape: SpatialShape,
        snap_roundoff: bool,
    ) -> Result<SpatialShape, ImageError> {
        let project_point = |pixel| {
            self.point(pixel).map(|projected| {
                if snap_roundoff {
                    // A direction WCS round trip can move an integer boundary
                    // by a few billionths of a pixel.
                    projected.map(|value| {
                        if (value - value.round()).abs() < 1e-8 {
                            value.round()
                        } else {
                            value
                        }
                    })
                } else {
                    projected
                }
            })
        };
        let polygon = |vertices: &[[f64; 2]]| {
            vertices
                .iter()
                .map(|&pixel| project_point(pixel))
                .collect::<Result<Vec<_>, _>>()
                .map(SpatialShape::Polygon)
        };
        let rectangle =
            |center: [f64; 2], half_width: f64, half_height: f64, cos: f64, sin: f64| {
                let vertices = [
                    [-half_width, -half_height],
                    [half_width, -half_height],
                    [half_width, half_height],
                    [-half_width, half_height],
                ]
                .map(|[x, y]| [center[0] + x * cos - y * sin, center[1] + x * sin + y * cos]);
                polygon(&vertices)
            };
        let origin_increment = self.definition.increment();
        let input_increment = self.input.increment();
        let scale = |axis: usize| (origin_increment[axis] / input_increment[axis]).abs();
        match shape {
            SpatialShape::Box([x0, y0, x1, y1]) => {
                let [x0, y0, x1, y1] = [x0, y0, x1, y1].map(|value| value as f64);
                polygon(&[[x0, y0], [x1, y0], [x1, y1], [x0, y1]])
            }
            SpatialShape::CenterBox {
                cx,
                cy,
                half_w,
                half_h,
            } => rectangle([cx, cy], half_w, half_h, 1.0, 0.0),
            SpatialShape::RotBox {
                cx,
                cy,
                half_w,
                half_h,
                cos_pa,
                sin_pa,
            } => rectangle([cx, cy], half_w, half_h, cos_pa, sin_pa),
            SpatialShape::Polygon(vertices) => polygon(&vertices),
            SpatialShape::Circle { cx, cy, rx, .. } => {
                let [cx, cy] = project_point([cx, cy])?;
                let radius = rx * scale(0);
                Ok(SpatialShape::Circle {
                    cx,
                    cy,
                    rx: radius,
                    ry: radius,
                })
            }
            SpatialShape::Ellipse {
                cx,
                cy,
                semi_major,
                semi_minor,
                cos_pa,
                sin_pa,
            } => {
                let [cx, cy] = project_point([cx, cy])?;
                Ok(SpatialShape::Ellipse {
                    cx,
                    cy,
                    semi_major: semi_major * scale(0),
                    semi_minor: semi_minor * scale(1),
                    cos_pa,
                    sin_pa,
                })
            }
        }
    }
}

// SPDX-License-Identifier: LGPL-3.0-or-later
//! CASA's direction-reference template mode.

use casa_coordinates::{Coordinate, CoordinateSystem, DirectionCoordinate, ProjectionType};
use casa_core::cancellation::Cancellation;
use casa_core::measures::{
    MeasFrame,
    direction::{DirectionRef, MDirection},
};
use casa_core::{ArrayD, PrimitiveType};
use ndarray::{Axis, IxDyn, ShapeBuilder, Slice};

use super::{
    RegridMethod, RegridPixel, Regridded, TaskRegridOptions, apply_direction, check, invalid,
    progress, validate_decimation,
};
use casa_images::{ImageError, ImageInterface, ImagePixel};

pub(crate) fn frame_from_template(value: &str) -> Option<DirectionRef> {
    let upper = value.to_ascii_uppercase();
    match upper.as_str() {
        "J2000" | "B1950" | "B1950_VLA" | "GALACTIC" | "HADEC" | "AZEL" | "AZELSW" | "AZELNE"
        | "ECLIPTIC" | "MECLIPTIC" | "TECLIPTIC" | "SUPERGAL" => upper.parse().ok(),
        _ => None,
    }
}

pub(super) fn observation_frame(coordinates: &CoordinateSystem) -> MeasFrame {
    let obs = coordinates.obs_info();
    let mut frame = MeasFrame::new();
    if let Some(epoch) = &obs.date {
        frame = frame.with_epoch(epoch.clone());
    }
    if let Some(position) = &obs.telescope_position {
        frame = frame.with_position(position.clone());
    }
    if let Some((index, _)) = super::super::direction_of(coordinates) {
        let direction = super::super::direction(coordinates, index);
        let [lon, lat] = if obs.pointing_center_initial {
            [
                direction.reference_value()[0],
                direction.reference_value()[1],
            ]
        } else {
            obs.pointing_center_rad
        };
        frame = frame.with_direction(MDirection::from_angles(lon, lat, direction.direction_ref()));
    }
    frame
}

pub(super) fn convert_angles(
    world: [f64; 2],
    from: DirectionRef,
    to: DirectionRef,
    frame: &MeasFrame,
) -> Result<[f64; 2], ImageError> {
    if from == to {
        return Ok(world);
    }
    let (lon, lat) = MDirection::from_angles(world[0], world[1], from)
        .convert_to(to, frame)
        .map_err(|error| invalid(error.to_string()))?
        .as_angles();
    Ok([lon, lat])
}

pub(crate) fn regrid_direction_frame<T: RegridPixel>(
    image: &impl ImageInterface<T>,
    reference: DirectionRef,
    method: RegridMethod,
    decimate: i64,
    cancellation: Option<&Cancellation>,
) -> Result<(Regridded<T>, f64), ImageError> {
    let coordinates = image.coordinates();
    let (index, axes) = super::super::direction_of(coordinates)
        .ok_or_else(|| invalid("image must have two direction axes"))?;
    let direction = super::super::direction(coordinates, index);
    let original = direction.direction_ref();
    if original != reference && direction.projection().projection_type() == ProjectionType::SFL {
        return Err(invalid(
            "a direction coordinate with SFL projection cannot be rotated",
        ));
    }
    // CASA's changed-frame path always ends in image.crop, which accepts
    // only Float pixels. An identity template just copies the typed image.
    if original != reference && T::PRIMITIVE_TYPE != PrimitiveType::Float32 {
        return Err(invalid("This method only supports Float valued images"));
    }
    let shape = image.shape();
    let data = image.get()?;
    check(cancellation)?;
    let mut result = Regridded {
        data,
        mask: image
            .default_mask()?
            .unwrap_or_else(|| ArrayD::from_elem(IxDyn(shape).f(), true)),
        coordinates: coordinates.clone(),
        template_coordinates: false,
        scale: 1.0,
    };
    if original == reference {
        return Ok((result, 0.0));
    }
    let increments = direction.increment();
    let absolute = [increments[0].abs(), increments[1].abs()];
    if (absolute[0] - absolute[1]).abs() > 2e-10 * absolute[0].max(absolute[1]) {
        return Err(invalid("direction rotation requires square pixels"));
    }
    let frame = observation_frame(coordinates);
    let plane = axes.map(|axis| shape[axis]);
    let center = plane.map(|length| (length / 2) as f64);
    let mut source = direction.clone().with_conversion_direction_ref(original);
    let at_center = source.pixel_to_world(center)?;
    source = source
        .with_reference_pixel(center)
        .with_reference_value(at_center);
    let world = convert_angles(at_center, original, reference, &frame)?;
    let angle = rotation_angle(&source, world, reference, &frame)?;
    let pad = padding(center, plane, angle);
    let dimensions = plane.map(|length| length + 2 * pad);
    let center = center.map(|pixel| pixel + pad as f64);
    source = source.with_reference_pixel(center);
    let units = direction.axis_units();
    let output = DirectionCoordinate::new(
        reference,
        direction.projection().clone(),
        world,
        [increments[0], increments[1]],
        center,
    )
    .with_world_units([&units[0], &units[1]])?;
    if pad > 0 {
        check(cancellation)?;
        result.data = pad_direction(&result.data, axes, pad, T::default_value());
        result.mask = pad_direction(&result.mask, axes, pad, false);
    }
    validate_decimation(decimate, dimensions, &[true, true], false)?;
    apply_direction(
        &mut result,
        axes,
        image.units(),
        (&source, &output),
        dimensions,
        &TaskRegridOptions {
            axes: &[],
            shape: &[],
            as_velocity: false,
            decimate,
            replicate: false,
            method,
            image_tool: None,
            cancellation,
        },
        &frame,
    )?;
    let lower = crop_direction(&mut result, axes, cancellation)?;
    result.coordinates.replace_coordinate(
        index,
        output.with_reference_pixel([center[0] - lower[0] as f64, center[1] - lower[1] as f64]),
    );
    Ok((result, -angle))
}

fn rotation_angle(
    source: &DirectionCoordinate,
    world: [f64; 2],
    target: DirectionRef,
    frame: &MeasFrame,
) -> Result<f64, ImageError> {
    let sign = if world[1] < 0.0 { 1.0 } else { -1.0 };
    let offset = convert_angles(
        [world[0], world[1] + sign * 1_f64.to_radians()],
        target,
        source.direction_ref(),
        frame,
    )?;
    let increment = source.increment();
    let viewport = source
        .clone()
        .with_reference_pixel([0.0; 2])
        .with_increment([
            1_f64.to_radians().copysign(increment[0]),
            1_f64.to_radians().copysign(increment[1]),
        ]);
    let [x, y] = viewport.world_to_pixel(offset)?;
    Ok(-(sign * x).atan2(sign * y))
}

fn padding(center: [f64; 2], shape: [usize; 2], angle: f64) -> usize {
    let (sin, cos) = angle.sin_cos();
    let mut maximum = [0.0_f64; 2];
    for x in [-center[0], center[0]] {
        for y in [-center[1], center[1]] {
            maximum[0] = maximum[0].max((x * cos - y * sin + 1.0).abs());
            maximum[1] = maximum[1].max((x * sin + y * cos + 1.0).abs());
        }
    }
    (maximum[0] - shape[0] as f64 / 2.0).max(maximum[1] - shape[1] as f64 / 2.0) as usize
}

fn pad_direction<T: Copy>(data: &ArrayD<T>, axes: [usize; 2], pad: usize, zeros: T) -> ArrayD<T> {
    let old = data.shape();
    let mut shape = old.to_vec();
    for axis in axes {
        shape[axis] += 2 * pad;
    }
    let mut padded = ArrayD::from_elem(IxDyn(&shape).f(), zeros);
    let mut view = padded.view_mut();
    for axis in axes {
        view.slice_axis_inplace(Axis(axis), Slice::from(pad..pad + old[axis]));
    }
    view.assign(data);
    padded
}

fn crop_direction<T: ImagePixel + Copy>(
    result: &mut Regridded<T>,
    axes: [usize; 2],
    cancellation: Option<&Cancellation>,
) -> Result<[usize; 2], ImageError> {
    let mut lower = [usize::MAX; 2];
    let mut upper = [0; 2];
    let mut pixels = result.mask.indexed_iter();
    for _ in (0..result.mask.len()).step_by(progress::BATCH_SIZE) {
        check(cancellation)?;
        for (index, &valid) in pixels.by_ref().take(progress::BATCH_SIZE) {
            if valid {
                for i in 0..2 {
                    lower[i] = lower[i].min(index[axes[i]]);
                    upper[i] = upper[i].max(index[axes[i]]);
                }
            }
        }
    }
    if lower[0] == usize::MAX {
        return Err(invalid("All output pixels are masked"));
    }
    for i in 0..2 {
        let slice = Slice::from(lower[i]..=upper[i]);
        result.data.slice_axis_inplace(Axis(axes[i]), slice);
        result.mask.slice_axis_inplace(Axis(axes[i]), slice);
    }
    Ok(lower)
}

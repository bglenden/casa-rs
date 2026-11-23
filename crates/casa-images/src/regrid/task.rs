// SPDX-License-Identifier: LGPL-3.0-or-later
//! Coordinate and shape selection for the CASA `imregrid` task.

mod frame;

pub(crate) use frame::{frame_from_template, regrid_direction_frame};

use casa_coordinates::{
    Coordinate, CoordinateModel, CoordinateType, DirectionCoordinate, LinearCoordinate,
    SpectralCoordinate, StokesCoordinate,
};
use casa_types::ArrayD;
use casa_types::measures::{
    MeasFrame,
    doppler::{DopplerRef, MDoppler},
    frequency::MFrequency,
};
use casa_types::quanta::{Quantity, Unit};
use ndarray::{Axis, IxDyn, ShapeBuilder};

use super::{
    DirectionSampling, Plane, RegridMethod, RegridPixel, RegridTemplate, Regridded,
    direction_affine, jy_per_pixel_scale, positions, same_axes, sample_direction_planes,
    spectral::SpectralPlan,
};
use crate::{ImageError, ImageInterface, ImagePixel};

pub(crate) struct TaskRegridOptions<'a> {
    pub(crate) axes: &'a [i64],
    pub(crate) shape: &'a [i64],
    pub(crate) as_velocity: bool,
    pub(crate) decimate: i64,
    pub(crate) replicate: bool,
    pub(crate) method: RegridMethod,
    pub(crate) image_tool: Option<ImageToolSampling>,
}

#[derive(Clone, Copy)]
pub(crate) struct ImageToolSampling {
    pub(crate) force: bool,
    pub(crate) convert_reference: bool,
    pub(crate) shape_specified: bool,
    pub(crate) half_turn: bool,
}

impl TaskRegridOptions<'_> {
    fn force(&self) -> bool {
        self.image_tool.is_some_and(|options| options.force)
    }

    fn convert_reference(&self) -> bool {
        self.image_tool
            .is_none_or(|options| options.convert_reference)
    }
}

pub(crate) fn regrid_task<T: RegridPixel>(
    input: &dyn ImageInterface<T>,
    template: &RegridTemplate,
    options: TaskRegridOptions<'_>,
) -> Result<Regridded<T>, ImageError> {
    let input_shape = input.shape();
    let frame = frame::observation_frame(input.coordinates());
    let resized = requested_shape(options.shape, input_shape)?;
    let mut shape = resized.clone().unwrap_or_else(|| input_shape.to_vec());
    let (all_axes, chosen, after_singleton) =
        requested_axis_selection(input, template, &options, &shape, resized.is_none())?;
    let ordered = index_coordinates(input.coordinates());
    let target_order = index_coordinates(&template.coordinates);
    let mut result = Regridded {
        data: input.get()?,
        mask: input
            .get_mask_slice(
                &vec![0; input_shape.len()],
                input_shape,
                &vec![1; input_shape.len()],
            )?
            .unwrap_or_else(|| ArrayD::from_elem(IxDyn(input_shape).f(), true)),
        coordinates: input.coordinates().clone(),
        template_coordinates: false,
        scale: 1.0,
    };
    if !result.mask.iter().any(|&bit| bit) {
        return Err(invalid("All selected pixels are masked"));
    }
    let mut found = false;
    for (index, (placement, ours)) in ordered.iter().enumerate() {
        let ours = *ours;
        let axes: Vec<_> = placement.iter().copied().flatten().collect();
        let n_axes = axes.len();
        let Some(&first) = axes.first() else { continue };
        let target = target_order.iter().find(|(other, model)| {
            model.coordinate_type() == ours.coordinate_type()
                && other
                    .iter()
                    .map(Option::is_some)
                    .eq(placement.iter().map(Option::is_some))
        });
        let input_lengths: Vec<_> = axes.iter().map(|&axis| input_shape[axis]).collect();
        let Some((other, theirs)) = target else {
            if options.image_tool.is_some()
                && !all_axes
                && chosen[first]
                && ours.coordinate_type() == CoordinateType::Stokes
            {
                return Err(invalid(
                    "cannot regrid a Stokes axis missing from the template",
                ));
            }
            validate_omitted_coordinate(
                ours.coordinate_type(),
                // The image tool keeps a selected input coordinate when the
                // template has no matching coordinate to replace it.
                options.image_tool.is_none() && !all_axes && chosen[first],
                &axes,
                input_shape,
                &shape,
            )?;
            continue;
        };
        let theirs = *theirs;
        let target_lengths: Vec<_> = other
            .iter()
            .flatten()
            .map(|&axis| template.shape[axis])
            .collect();
        if let (CoordinateModel::Stokes(ours), CoordinateModel::Stokes(theirs)) = (ours, theirs) {
            let explicit_axis = !all_axes && chosen[first];
            let selection =
                stokes_regrid_request(&options, first, &shape, all_axes, explicit_axis)?;
            let explicit_length = resized
                .as_ref()
                .filter(|_| options.image_tool.is_none_or(|tool| tool.shape_specified))
                .map(|resized| resized[first]);
            shape[first] = regrid_stokes(
                &mut result,
                (index, first),
                (ours, theirs),
                (shape[first], explicit_length),
                selection,
                explicit_axis,
            )?;
            continue;
        }
        let selected: Vec<_> = axes
            .iter()
            .copied()
            .enumerate()
            .map(|(i, axis)| {
                let spectral = ours.coordinate_type() == CoordinateType::Spectral;
                let selected = if all_axes || (after_singleton && !spectral) {
                    input_lengths[i] > 1 && target_lengths[i] > 1
                } else {
                    chosen[axis]
                };
                selected && (resized.is_some() || !spectral || target_lengths[i] > 1)
            })
            .collect();
        if resized.is_none() {
            for (i, &axis) in axes.iter().enumerate() {
                if selected[i] {
                    shape[axis] = target_lengths[i];
                }
            }
        }
        if (options.image_tool.is_some() || !selected.contains(&true))
            && axes
                .iter()
                .zip(&selected)
                .any(|(&axis, &regrid)| !regrid && shape[axis] != input_shape[axis])
        {
            return Err(invalid(
                "an axis not being regridded must keep its input length",
            ));
        }
        if !selected.contains(&true) {
            continue;
        }
        found = true;
        let lengths: Vec<_> = axes.iter().map(|&axis| shape[axis]).collect();
        if n_axes != ours.n_pixel_axes() && ours.coordinate_type() != CoordinateType::Linear {
            return Err(invalid(
                "cannot regrid a coordinate with a removed pixel axis",
            ));
        }
        let coordinate = match (ours, theirs) {
            (CoordinateModel::Direction(ours), CoordinateModel::Direction(theirs))
                if n_axes == 2 =>
            {
                regrid_direction_coordinate(
                    &mut result,
                    [axes[0], axes[1]],
                    input.units(),
                    (ours, theirs),
                    (
                        [lengths[0], lengths[1]],
                        &selected,
                        all_axes || after_singleton,
                    ),
                    (&options, &frame),
                )?
            }
            (CoordinateModel::Direction(ours), CoordinateModel::Direction(theirs))
                if n_axes == 1 && input_lengths == lengths && ours == theirs =>
            {
                CoordinateModel::Direction(theirs.clone())
            }
            (CoordinateModel::Linear(ours), CoordinateModel::Linear(theirs)) => {
                regrid_linear_coordinate(
                    &mut result,
                    input.units(),
                    placement,
                    (&input_lengths, &lengths),
                    (ours, theirs),
                    &selected,
                    &options,
                )?
            }
            (CoordinateModel::Spectral(ours), CoordinateModel::Spectral(theirs)) => {
                regrid_spectral_coordinate(
                    &mut result,
                    input,
                    first,
                    (ours, theirs),
                    [input_lengths[0], lengths[0]],
                    &options,
                    &frame,
                )?
            }
            _ if n_axes == 1 => regrid_one_axis(
                &mut result,
                first,
                [input_lengths[0], lengths[0]],
                (ours, theirs),
                &options,
            ),
            _ => {
                return Err(invalid(
                    "regridding this multi-axis coordinate is unsupported (#977)",
                ));
            }
        };
        result.coordinates.replace_coordinate(index, coordinate);
    }
    if options.image_tool.is_none() && all_axes && !found {
        return Err(invalid("Found no axes to regrid"));
    }
    if !result.mask.iter().any(|&bit| bit) {
        return Err(invalid("All output pixels are masked"));
    }
    Ok(result)
}

fn regrid_direction_coordinate<T: RegridPixel>(
    result: &mut Regridded<T>,
    axes: [usize; 2],
    units: &str,
    (input, target): (&DirectionCoordinate, &DirectionCoordinate),
    (shape, selected, all_axes): ([usize; 2], &[bool], bool),
    (options, frame): (&TaskRegridOptions<'_>, &MeasFrame),
) -> Result<CoordinateModel, ImageError> {
    if options.image_tool.is_some() && shape == [1, 1] {
        return Err(invalid(
            "cannot regrid a direction coordinate with shape [1, 1]",
        ));
    }
    validate_decimation(options.decimate, shape, selected, all_axes)?;
    if options.force()
        || axes.map(|axis| result.data.shape()[axis]) != shape
        || !same_direction_grid(input, target)
    {
        apply_direction(result, axes, units, (input, target), shape, options, frame)?;
    }
    Ok(CoordinateModel::Direction(target.clone()))
}

fn regrid_one_axis<T: RegridPixel>(
    result: &mut Regridded<T>,
    axis: usize,
    [input_length, length]: [usize; 2],
    (input, target): (&CoordinateModel, &CoordinateModel),
    options: &TaskRegridOptions<'_>,
) -> CoordinateModel {
    let positions = (0..length)
        .map(|pixel| {
            if options.replicate {
                Some(replicated_position(pixel, input_length, length))
            } else {
                target
                    .to_world(&[pixel as f64])
                    .and_then(|world| input.to_pixel(&world))
                    .ok()
                    .and_then(|pixel| pixel.first().copied())
                    .filter(|pixel| pixel.is_finite())
            }
        })
        .collect();
    apply_one_axis(result, axis, length, positions, options.method);
    target.clone()
}

fn validate_omitted_coordinate(
    kind: CoordinateType,
    explicitly_selected: bool,
    axes: &[usize],
    input_shape: &[usize],
    output_shape: &[usize],
) -> Result<(), ImageError> {
    if explicitly_selected && kind == CoordinateType::Spectral {
        return Err(invalid(
            "cannot regrid a spectral axis missing from the template",
        ));
    }
    if axes
        .iter()
        .any(|&axis| output_shape[axis] != input_shape[axis])
    {
        return Err(invalid(
            "an axis missing from the template must keep its input length",
        ));
    }
    Ok(())
}

fn use_default_axes<T: ImagePixel>(
    source: &dyn ImageInterface<T>,
    target: &RegridTemplate,
    axes: &[i64],
    infer_shape: bool,
) -> bool {
    if axes.first().is_none_or(|&axis| axis < 0) {
        return true;
    }
    let Some(source_spectral) = source.find_axis(CoordinateType::Spectral) else {
        return false;
    };
    let target_spectral = target
        .coordinates
        .find_coordinate(CoordinateType::Spectral)
        .and_then(|index| target.coordinates.coordinate_pixel_axes(index))
        .and_then(|axes| axes.first().copied().flatten());
    infer_shape
        && axes == [source_spectral as i64]
        && target_spectral.is_some_and(|axis| target.shape[axis] == 1)
}

fn requested_axis_selection<T: ImagePixel>(
    input: &dyn ImageInterface<T>,
    template: &RegridTemplate,
    options: &TaskRegridOptions<'_>,
    shape: &[usize],
    infer_shape: bool,
) -> Result<(bool, Vec<bool>, bool), ImageError> {
    let all_axes = if options.image_tool.is_some() {
        options.axes.is_empty() || options.axes == [-1]
    } else {
        use_default_axes(input, template, options.axes, infer_shape)
    };
    let chosen = requested_axes(
        options.axes,
        all_axes,
        input.ndim(),
        options.image_tool.is_some(),
    )?;
    let after_singleton = input
        .find_axis(CoordinateType::Spectral)
        .is_some_and(|axis| {
            options.axes == [axis as i64] && input.shape()[axis] == 1 && shape[axis] > 1
        });
    Ok((all_axes, chosen, after_singleton))
}

fn regrid_linear_coordinate<T: RegridPixel>(
    result: &mut Regridded<T>,
    units: &str,
    placement: &[Option<usize>],
    (input_lengths, lengths): (&[usize], &[usize]),
    (input, target): (&LinearCoordinate, &LinearCoordinate),
    selected: &[bool],
    options: &TaskRegridOptions<'_>,
) -> Result<CoordinateModel, ImageError> {
    if let [Some(x), Some(y)] = placement {
        if options.decimate < 0 {
            return Err(invalid("decimate must be nonnegative"));
        }
        if options.force() || input_lengths != lengths || !same_linear_grid(input, target) {
            apply_linear_plane(
                result,
                [*x, *y],
                units,
                (input, target),
                [lengths[0], lengths[1]],
                options,
            )?;
        }
    } else {
        if input_lengths
            .iter()
            .zip(lengths)
            .zip(selected)
            .any(|((old, new), selected)| !selected && old != new)
        {
            return Err(invalid(
                "an axis not being regridded must keep its input length",
            ));
        }
        apply_linear_axes(
            result,
            placement,
            lengths,
            selected,
            (input, target),
            options,
        )?;
    }
    Ok(target.clone().into())
}

fn regrid_spectral_coordinate<T: RegridPixel>(
    result: &mut Regridded<T>,
    input: &dyn ImageInterface<T>,
    axis: usize,
    (ours, theirs): (&SpectralCoordinate, &SpectralCoordinate),
    dimensions: [usize; 2],
    options: &TaskRegridOptions<'_>,
    frame: &MeasFrame,
) -> Result<CoordinateModel, ImageError> {
    let [old_length, length] = dimensions;
    if old_length == 1 && length == 1 {
        return Ok(ours.clone().into());
    }
    if length == 1 {
        return Err(invalid(
            "a spectral interpolation requires multiple output channels",
        ));
    }
    if input.image_info()?.beam_set.is_multi() {
        return Err(invalid(
            "an image with multiple beams cannot be regridded along the spectral axis",
        ));
    }
    let (coordinate, velocity) = if options.as_velocity && old_length > 1 {
        let (coordinate, grids) = velocity_regrid(ours, theirs, dimensions)?;
        (coordinate, Some(grids))
    } else {
        (theirs.clone(), None)
    };
    let source: CoordinateModel = ours.clone().into();
    let target: CoordinateModel = coordinate.clone().into();
    if options.force()
        || old_length != length
        || !same_axes(&source, &target, &[length])
        || ours.world_frequency_ref() != theirs.world_frequency_ref()
    {
        let positions = spectral_positions(
            (ours, &coordinate),
            dimensions,
            options.replicate,
            velocity,
            options.convert_reference().then_some(frame),
        )?;
        apply_one_axis(result, axis, length, positions, options.method);
    }
    Ok(target)
}

fn same_linear_grid(input: &LinearCoordinate, output: &LinearCoordinate) -> bool {
    input.reference_pixel() == output.reference_pixel()
        && input.reference_value() == output.reference_value()
        && input.increment() == output.increment()
        && input.axis_units() == output.axis_units()
        && input.pc_matrix() == output.pc_matrix()
}

fn same_direction_grid(input: &DirectionCoordinate, output: &DirectionCoordinate) -> bool {
    input.direction_ref() == output.direction_ref()
        && input.conversion_direction_ref() == output.conversion_direction_ref()
        && input.projection() == output.projection()
        && input.pc_matrix() == output.pc_matrix()
        && input.reference_pixel() == output.reference_pixel()
        && input.reference_value() == output.reference_value()
        && input.increment() == output.increment()
        && input.longpole() == output.longpole()
        && input.native_pole() == output.native_pole()
}

fn stokes_regrid_request(
    options: &TaskRegridOptions<'_>,
    axis: usize,
    shape: &[usize],
    all_axes: bool,
    explicit_axis: bool,
) -> Result<bool, ImageError> {
    if options.image_tool.is_none() || all_axes || !explicit_axis {
        return Ok(all_axes || explicit_axis);
    }
    let slot = options
        .axes
        .iter()
        .position(|&candidate| candidate == axis as i64)
        .expect("explicit Stokes axis");
    // CASA checks the output Stokes slot in the requested axis order and
    // enables Stokes selection only when that list covers the Stokes index.
    if slot != axis && shape[slot] != shape[axis] {
        return Err(invalid("Stokes axis and output shape do not match"));
    }
    Ok(options.axes.get(axis).is_some_and(|&value| value > 0))
}

fn regrid_stokes<T: RegridPixel>(
    result: &mut Regridded<T>,
    (coordinate, axis): (usize, usize),
    (input, target): (&StokesCoordinate, &StokesCoordinate),
    (length, explicit_length): (usize, Option<usize>),
    select: bool,
    explicit_axis: bool,
) -> Result<usize, ImageError> {
    if select && input.stokes().len() > 1 && target.stokes().len() > 1 {
        select_stokes(result, coordinate, axis, input, target, explicit_length)?;
    } else if length != result.data.shape()[axis] || explicit_axis {
        return Err(invalid(
            "an unchanged Stokes axis must keep its input length",
        ));
    }
    Ok(result.data.shape()[axis])
}

fn index_coordinates(
    system: &casa_coordinates::CoordinateSystem,
) -> Vec<(Vec<Option<usize>>, &CoordinateModel)> {
    (0..system.n_coordinates())
        .map(|index| {
            (
                system
                    .coordinate_pixel_axes(index)
                    .expect("existing coordinate"),
                system.coordinate(index),
            )
        })
        .collect()
}

fn requested_axes(
    axes: &[i64],
    all: bool,
    ndim: usize,
    unique: bool,
) -> Result<Vec<bool>, ImageError> {
    let mut mask = vec![all; ndim];
    if !all {
        for &axis in axes {
            let axis = usize::try_from(axis)
                .ok()
                .and_then(|i| mask.get_mut(i))
                .ok_or_else(|| invalid("regridding axis is outside the input image"))?;
            if unique && *axis {
                return Err(invalid("Specified pixel axes are not unique"));
            }
            *axis = true;
        }
    }
    Ok(mask)
}

fn requested_shape(shape: &[i64], input: &[usize]) -> Result<Option<Vec<usize>>, ImageError> {
    if shape.is_empty() {
        return Ok(Some(input.to_vec()));
    }
    if shape == [-1] {
        return Ok(None);
    }
    if shape.len() != input.len() {
        return Err(invalid(
            "output shape must have the same number of axes as the input image",
        ));
    }
    shape
        .iter()
        .map(|&length| {
            usize::try_from(length)
                .ok()
                .filter(|&n| n > 0)
                .ok_or_else(|| invalid("output shape lengths must be positive"))
        })
        .collect::<Result<Vec<_>, _>>()
        .map(Some)
}

fn select_stokes<T: ImagePixel + Copy>(
    result: &mut Regridded<T>,
    index: usize,
    axis: usize,
    input: &StokesCoordinate,
    template: &StokesCoordinate,
    expected: Option<usize>,
) -> Result<(), ImageError> {
    let (indices, stokes): (Vec<_>, Vec<_>) = input
        .stokes()
        .iter()
        .copied()
        .enumerate()
        .filter(|(_, value)| template.stokes().contains(value))
        .unzip();
    if stokes.is_empty() || expected.is_some_and(|n| n != stokes.len()) {
        return Err(invalid(
            "output Stokes length must match the common input and template Stokes",
        ));
    }
    if stokes.len() != input.stokes().len() {
        result.data = result.data.select(Axis(axis), &indices);
        result.mask = result.mask.select(Axis(axis), &indices);
        result
            .coordinates
            .replace_coordinate(index, StokesCoordinate::new(stokes));
    }
    Ok(())
}

#[derive(Clone, Copy)]
struct VelocityGrid {
    reference: f64,
    increment: f64,
    reference_pixel: f64,
}

fn velocity_regrid(
    input: &SpectralCoordinate,
    template: &SpectralCoordinate,
    dimensions: [usize; 2],
) -> Result<(SpectralCoordinate, [VelocityGrid; 2]), ImageError> {
    if input.world_frequency_ref() != template.world_frequency_ref() {
        return Err(invalid(
            "velocity regridding requires matching spectral reference frames",
        ));
    }
    let measure_frame = MeasFrame::new();
    let grid = |coordinate: &SpectralCoordinate, count: usize| -> Result<_, ImageError> {
        let rest = coordinate.rest_frequency_hz()?;
        if rest == 0.0 {
            return Err(invalid(
                "velocity regridding requires nonzero rest frequencies",
            ));
        }
        let velocity = |pixel| -> Result<f64, ImageError> {
            let ratio = coordinate.world_frequency_hz(pixel)? / rest;
            MDoppler::new(ratio, DopplerRef::RATIO)
                .convert_to(coordinate.velocity_convention(), &measure_frame)
                .map(|value| value.value())
                .map_err(|error| invalid(error.to_string()))
        };
        let reference_pixel = coordinate.reference_pixel()[0];
        let first = velocity(0.0)?;
        let last = velocity((count - 1) as f64)?;
        let grid = VelocityGrid {
            reference: velocity(reference_pixel)?,
            increment: velocity(1.0)? - first,
            reference_pixel,
        };
        Ok((grid, [first.min(last), first.max(last)]))
    };
    let (source, input_extent) = grid(input, dimensions[0])?;
    let (target, output_extent) = grid(template, dimensions[1])?;
    if !overlapping_ranges(input_extent, output_extent) {
        return Err(invalid(
            "input and output velocity coordinates do not overlap",
        ));
    }
    let rest = input.rest_frequency_hz()?;
    let convention = input.velocity_convention();
    if rest == template.rest_frequency_hz()?
        && convention == DopplerRef::RADIO
        && template.velocity_convention() == convention
        && template.has_linear_mapping()
        && template.frequency_ref() == template.world_frequency_ref()
    {
        // Radio velocity is affine here. Keep the exact template grid and
        // the input's rest-frequency inventory.
        let (pixel, reference, increment) = template.fits_linearized_axis_hz()?;
        let coordinate = SpectralCoordinate::new(
            input.world_frequency_ref(),
            reference,
            increment,
            pixel,
            rest,
        )
        .with_name(input.axis_names()[0].clone())
        .with_linear_transform(template.linear_transform())?
        .with_rest_frequencies(
            input
                .rest_frequencies()
                .iter()
                .map(|&rest| input.value_in_hz(rest))
                .collect::<Result<Vec<_>, _>>()?,
        )
        .with_velocity_convention(convention);
        return Ok((coordinate, [source, target]));
    }
    let frequency = |velocity| MDoppler::new(velocity, convention).shift_frequency_hz(rest);
    let reference = frequency(target.reference);
    let increment = frequency(target.reference + target.increment) - reference;
    let coordinate = SpectralCoordinate::new(
        input.world_frequency_ref(),
        reference,
        increment,
        target.reference_pixel,
        rest,
    )
    .with_velocity_convention(convention);
    Ok((coordinate, [source, target]))
}

fn overlapping_ranges([lo_in, hi_in]: [f64; 2], [lo_out, hi_out]: [f64; 2]) -> bool {
    let separated = |high: f64, low: f64| low - high > 1e-13 * high.abs().max(low.abs());
    !separated(hi_in, lo_out) && !separated(hi_out, lo_in)
}

fn spectral_positions(
    (input, target): (&SpectralCoordinate, &SpectralCoordinate),
    [input_count, output_count]: [usize; 2],
    replicate: bool,
    velocity: Option<[VelocityGrid; 2]>,
    frame: Option<&MeasFrame>,
) -> Result<Vec<Option<f64>>, ImageError> {
    if input_count > 1 && velocity.is_none() {
        let bounds = |coordinate: &SpectralCoordinate, count: usize| {
            let first = coordinate.world_frequency_hz(0.0)?;
            let last = coordinate.world_frequency_hz((count - 1) as f64)?;
            Ok::<_, ImageError>([first.min(last), first.max(last)])
        };
        if !overlapping_ranges(bounds(input, input_count)?, bounds(target, output_count)?) {
            return Err(invalid(
                "input and output spectral coordinates do not overlap",
            ));
        }
    }
    (0..output_count)
        .map(|channel| {
            if input_count == 1 {
                return Ok(Some(0.0));
            }
            if replicate {
                return Ok(Some(replicated_position(
                    channel,
                    input_count,
                    output_count,
                )));
            }
            if let Some([source, target]) = velocity {
                let value =
                    target.reference + (channel as f64 - target.reference_pixel) * target.increment;
                let pixel = source.reference_pixel + (value - source.reference) / source.increment;
                return Ok(pixel.is_finite().then_some(pixel));
            }
            let frequency = target.world_frequency_hz(channel as f64)?;
            let input_frequency = match frame {
                Some(frame) => MFrequency::new(frequency, target.world_frequency_ref())
                    .convert_to(input.world_frequency_ref(), frame)
                    .map_err(|error| invalid(error.to_string()))?
                    .hz(),
                None => frequency,
            };
            Ok(input
                .pixel_at_frequency_hz(input_frequency)
                .ok()
                .filter(|pixel| pixel.is_finite()))
        })
        .collect()
}

fn apply_one_axis<T: RegridPixel>(
    result: &mut Regridded<T>,
    axis: usize,
    length: usize,
    positions: Vec<Option<f64>>,
    method: RegridMethod,
) {
    let mut shape = result.data.shape().to_vec();
    shape[axis] = length;
    let plan = SpectralPlan::from_positions(axis, positions, method);
    (result.data, result.mask) = plan.apply_precise(&result.data, &result.mask, &shape);
}

fn validate_decimation(
    decimate: i64,
    shape: [usize; 2],
    selected: &[bool],
    all: bool,
) -> Result<(), ImageError> {
    if decimate < 0 {
        return Err(invalid("decimate must be nonnegative"));
    }
    if decimate > 1 && !all {
        for (axis, length) in shape.into_iter().enumerate() {
            if selected[axis] && length / 3 < decimate as usize {
                return Err(invalid(format!(
                    "maximum decimate for an axis of length {length} is {}",
                    length / 3
                )));
            }
        }
    }
    Ok(())
}

fn replicated_position(output: usize, input_count: usize, output_count: usize) -> f64 {
    (output as f64 + 0.5) * (input_count as f64 / output_count as f64) - 0.5
}

fn apply_direction<T: RegridPixel>(
    result: &mut Regridded<T>,
    axes: [usize; 2],
    units: &str,
    (input, target): (&DirectionCoordinate, &DirectionCoordinate),
    shape: [usize; 2],
    options: &TaskRegridOptions<'_>,
    frame: &MeasFrame,
) -> Result<(), ImageError> {
    let scale = jy_per_pixel_scale(units, input, target);
    let input_shape = axes.map(|axis| result.data.shape()[axis]);
    let grid = if options.replicate && options.image_tool.is_none() {
        replicated_grid(input_shape, shape)
    } else {
        let projected = direction_grid(input, target, shape, options, frame)?;
        if options.image_tool.is_some()
            && !projected.iter().flatten().any(|pixel| {
                pixel
                    .iter()
                    .zip(input_shape)
                    .all(|(&value, length)| value > -1. && value < length as f64)
            })
        {
            return Err(invalid(
                "There is no overlap between the input and output direction grids",
            ));
        }
        if options.replicate {
            replicated_grid(input_shape, shape)
        } else {
            projected
        }
    };
    apply_plane(result, axes, shape, &grid, options.method, scale);
    Ok(())
}

fn direction_grid(
    input: &DirectionCoordinate,
    target: &DirectionCoordinate,
    shape: [usize; 2],
    options: &TaskRegridOptions<'_>,
    frame: &MeasFrame,
) -> Result<Vec<Option<[f64; 2]>>, ImageError> {
    if options.image_tool.is_some_and(|tool| tool.half_turn) {
        let reference = input.reference_pixel();
        // The two grids have the same tangent point and opposite pixel
        // offsets. Check the projection, then reflect about the exact pivot.
        return Ok((0..shape[1])
            .flat_map(|y| {
                let reference = &reference;
                (0..shape[0]).map(move |x| {
                    let pixel = [x as f64, y as f64];
                    target.pixel_to_world(pixel).ok()?;
                    Some(std::array::from_fn(|axis| {
                        2.0 * reference[axis] - pixel[axis]
                    }))
                })
            })
            .collect());
    }
    if options.image_tool.is_some()
        && input.increment() == target.increment()
        && input.reference_value() == target.reference_value()
        && *input.pc_matrix() == *target.pc_matrix()
        && let Some(affine) = direction_affine(input, target, shape)
        && affine
            .source_pixel([0.; 2])
            .iter()
            .all(|value| value.fract() == 0.)
    {
        // An integral crop or translation on the same tangent plane is exact.
        // A sky round trip can move the first excluded row across the
        // interpolation boundary and fill it by extrapolating the edge.
        return Ok((0..shape[1])
            .flat_map(|y| {
                (0..shape[0]).map(move |x| Some(affine.source_pixel([x as f64, y as f64])))
            })
            .collect());
    }
    // Casacore's inter-coordinate conversion uses native frames, while
    // toWorldMany and toPixelMany retain each coordinate's conversion layer.
    let to = input.direction_ref();
    let from = if options.convert_reference() {
        target.direction_ref()
    } else {
        to
    };
    let decimate = options.decimate.max(0) as usize;
    if decimate <= 1 && from == to {
        return Ok(positions(input, target, shape));
    }
    let project = |pixel| {
        let world = target.pixel_to_world(pixel).ok()?;
        let world = frame::convert_angles(world, from, to, frame).ok()?;
        input
            .world_to_pixel(world)
            .ok()
            .filter(|p| p.iter().all(|x| x.is_finite()))
    };
    // Report a missing measures frame before treating projection failures as masks.
    let world = target.reference_value();
    frame::convert_angles([world[0], world[1]], from, to, frame)?;
    Ok(coarse_positions(shape, decimate, project))
}

fn replicated_grid(input: [usize; 2], output: [usize; 2]) -> Vec<Option<[f64; 2]>> {
    (0..output[1])
        .flat_map(|y| {
            (0..output[0]).map(move |x| {
                Some([
                    replicated_position(x, input[0], output[0]),
                    replicated_position(y, input[1], output[1]),
                ])
            })
        })
        .collect()
}

fn linear_axis_unit_factor(input: &str, target: &str, axis: usize) -> Result<f64, ImageError> {
    if input == target {
        return Ok(1.0);
    }
    let factor = || Quantity::new(1.0, target)?.get_value_in(&Unit::new(input)?);
    factor().map_err(|error| invalid(format!("linear regrid axis {axis}: {error}")))
}

fn apply_linear_axes<T: RegridPixel>(
    result: &mut Regridded<T>,
    placement: &[Option<usize>],
    lengths: &[usize],
    selected: &[bool],
    (input, target): (&LinearCoordinate, &LinearCoordinate),
    options: &TaskRegridOptions<'_>,
) -> Result<(), ImageError> {
    let mut passes: Vec<_> = placement
        .iter()
        .enumerate()
        .filter_map(|(local, &axis)| axis.map(|axis| (local, axis)))
        .zip(lengths.iter().zip(selected))
        .filter_map(|((local, axis), (&length, &selected))| {
            selected.then_some((local, axis, length))
        })
        .collect();
    passes.sort_by_key(|&(_, axis, _)| {
        options
            .axes
            .iter()
            .position(|&selected| selected == axis as i64)
            .unwrap_or(axis)
    });
    let input_units = input.axis_units();
    let target_units = target.axis_units();
    let mut factors = vec![1.0; placement.len()];
    let mut target_pixel = target.reference_pixel();
    let same_grid = same_linear_grid(input, target);
    for (local, axis, length) in passes {
        let old_length = result.data.shape()[axis];
        // casacore aligns the selected axis's units and retains earlier conversions.
        factors[local] = linear_axis_unit_factor(&input_units[local], &target_units[local], axis)?;
        if !options.force() && same_grid && length == old_length {
            continue;
        }
        // The coordinate transform uses the full local model. Keep its other
        // axes at their reference pixels for this one-axis pass.
        let reference_pixel = target_pixel[local];
        let positions = (0..length)
            .map(|pixel| {
                if options.replicate {
                    return Some(replicated_position(pixel, old_length, length));
                }
                target_pixel[local] = pixel as f64;
                let mut world = target.to_world(&target_pixel).ok()?;
                world
                    .iter_mut()
                    .zip(&factors)
                    .for_each(|(value, factor)| *value *= factor);
                let pixel = input.to_pixel(&world).ok()?[local];
                pixel.is_finite().then_some(pixel)
            })
            .collect();
        target_pixel[local] = reference_pixel;
        apply_one_axis(result, axis, length, positions, options.method);
    }
    Ok(())
}

fn apply_linear_plane<T: RegridPixel>(
    result: &mut Regridded<T>,
    axes: [usize; 2],
    brightness: &str,
    (input, target): (&LinearCoordinate, &LinearCoordinate),
    shape: [usize; 2],
    options: &TaskRegridOptions<'_>,
) -> Result<(), ImageError> {
    let input_units = input.axis_units();
    let target_units = target.axis_units();
    let factors = [
        linear_axis_unit_factor(&input_units[0], &target_units[0], axes[0])?,
        linear_axis_unit_factor(&input_units[1], &target_units[1], axes[1])?,
    ];
    let grid = if options.replicate {
        replicated_grid(axes.map(|axis| result.data.shape()[axis]), shape)
    } else {
        let project = |pixel: [f64; 2]| {
            let mut world = target.to_world(&pixel).ok()?;
            world
                .iter_mut()
                .zip(factors)
                .for_each(|(value, factor)| *value *= factor);
            let pixel = input.to_pixel(&world).ok()?;
            let [x, y] = pixel.as_slice() else {
                return None;
            };
            (x.is_finite() && y.is_finite()).then_some([*x, *y])
        };
        let decimate = options.decimate.max(0) as usize;
        if decimate > 1 {
            coarse_positions(shape, decimate, project)
        } else {
            (0..shape[1])
                .flat_map(|y| (0..shape[0]).map(move |x| project([x as f64, y as f64])))
                .collect()
        }
    };
    let scale = if brightness.eq_ignore_ascii_case("Jy/pixel") {
        let old = input.increment();
        let new = target.increment();
        (new[0] * factors[0] / old[0] * (new[1] * factors[1] / old[1])).abs()
    } else {
        1.0
    };
    apply_plane(result, axes, shape, &grid, options.method, scale);
    Ok(())
}

fn apply_plane<T: RegridPixel>(
    result: &mut Regridded<T>,
    axes: [usize; 2],
    shape: [usize; 2],
    positions: &[Option<[f64; 2]>],
    method: RegridMethod,
    scale: f64,
) {
    let sampling = DirectionSampling {
        axes: [axes; 2],
        shape,
        positions,
        method,
        scale,
    };
    (result.data, result.mask) = sample_direction_planes(
        &result.data,
        Some(&result.mask),
        sampling,
        T::COMPONENTS,
        T::component,
        T::with_component,
    );
    result.scale = scale;
}

fn coarse_positions(
    shape: [usize; 2],
    decimate: usize,
    project: impl Fn([f64; 2]) -> Option<[f64; 2]>,
) -> Vec<Option<[f64; 2]>> {
    let step = if decimate <= 1 {
        [1, 1]
    } else {
        shape
            .map(|length| {
                let samples = length / decimate;
                length
                    / if samples > 1 {
                        samples - 1
                    } else {
                        length.saturating_sub(1).max(1)
                    }
            })
            .map(|step| step.max(1))
    };
    if step == [1, 1] {
        return (0..shape[1])
            .flat_map(|y| (0..shape[0]).map(move |x| [x as f64, y as f64]))
            .map(project)
            .collect();
    }
    let sparse = [shape[0].div_ceil(step[0]), shape[1].div_ceil(step[1])];
    let mut projected_x = Vec::with_capacity(sparse[0] * sparse[1]);
    let mut projected_y = Vec::with_capacity(projected_x.capacity());
    let mut valid = Vec::with_capacity(projected_x.capacity());
    for y in (0..shape[1]).step_by(step[1]) {
        for x in (0..shape[0]).step_by(step[0]) {
            let pixel = project([x as f64, y as f64]);
            valid.push(pixel.is_some());
            let [x, y] = pixel.unwrap_or([0.0, 0.0]);
            projected_x.push(x);
            projected_y.push(y);
        }
    }
    let x = Plane {
        data: &projected_x,
        mask: Some(&valid),
        shape: sparse,
    };
    let y = Plane {
        data: &projected_y,
        mask: Some(&valid),
        shape: sparse,
    };
    (0..shape[1])
        .flat_map(|j| {
            (0..shape[0]).map(move |i| [i as f64 / step[0] as f64, j as f64 / step[1] as f64])
        })
        .map(|p| {
            Some([
                x.sample(RegridMethod::Linear, p)?,
                y.sample(RegridMethod::Linear, p)?,
            ])
        })
        .collect()
}

fn invalid(message: impl Into<String>) -> ImageError {
    ImageError::InvalidMetadata(message.into())
}

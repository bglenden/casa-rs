// SPDX-License-Identifier: LGPL-3.0-or-later

use casa_coordinates::{CoordinateModel, CoordinateSystem, CoordinateType};
use casa_images::{ImageInterface, ImagePixel, analysis::statistics_flux_scale};
use casa_types::quanta::{Quantity, Unit};

use crate::convert::holder::C_MPS;

use super::{Chunk, SpectralVelocity};

pub(super) enum FluxScale {
    Uniform(f64),
    PerBeam {
        values: Vec<f64>,
        channel: DisplayIndex,
        stokes: DisplayIndex,
    },
}

impl FluxScale {
    pub(super) fn for_plane(&self, plane: usize) -> f64 {
        match self {
            Self::Uniform(value) => *value,
            Self::PerBeam {
                values,
                channel,
                stokes,
            } => values[channel.position(plane) * stokes.count + stokes.position(plane)],
        }
    }
}

pub(super) struct FluxPlan {
    pub(super) scale: FluxScale,
    pub(super) integral: Option<SpectralIntegral>,
}

pub(super) struct SpectralIntegral {
    pub(super) density_axes: Vec<usize>,
    pub(super) channel_width: f64,
}

pub(super) struct DisplayIndex {
    count: usize,
    stride: usize,
}

impl DisplayIndex {
    fn new(axis: Option<usize>, chunk: &Chunk, axes: &[usize], global: bool) -> Self {
        let Some(axis) = axis.filter(|axis| !global && !axes.contains(axis)) else {
            return Self {
                count: 1,
                stride: 1,
            };
        };
        let stride = (0..axis)
            .filter(|axis| !axes.contains(axis))
            .map(|axis| chunk.size[axis])
            .product();
        Self {
            count: chunk.size[axis],
            stride,
        }
    }

    fn position(&self, plane: usize) -> usize {
        (plane / self.stride) % self.count
    }
}

fn channel_width(
    coordinates: &CoordinateSystem,
    sample: Option<[usize; 2]>,
    velocity: Option<&SpectralVelocity>,
) -> Option<f64> {
    let Some([first, second]) = sample else {
        return Some(1.0);
    };
    let index = coordinates.find_coordinate(CoordinateType::Spectral)?;
    let spectral = coordinates.coordinate(index);
    let CoordinateModel::Spectral(model) = spectral else {
        return None;
    };
    let frequency = |pixel| {
        spectral
            .to_world(&[pixel as f64])
            .ok()
            .map(|values| values[0])
    };
    if let Some(velocity) = velocity {
        let at =
            |pixel| velocity.velocity_for_ratio(frequency(pixel)?.abs() / model.rest_frequency());
        Some((at(second)? - at(first)?).abs())
    } else if model.rest_frequency() > 0.0 {
        Some(
            C_MPS / 1000.0 * (frequency(second)? - frequency(first)?).abs()
                / model.rest_frequency(),
        )
    } else {
        Some(spectral.increment()[0] * second.abs_diff(first) as f64)
    }
}

pub(super) fn allowed<T: ImagePixel>(image: &impl ImageInterface<T>, axes: &[usize]) -> bool {
    let beam_axis_selected = [CoordinateType::Spectral, CoordinateType::Stokes]
        .into_iter()
        .filter_map(|kind| image.find_axis(kind))
        .any(|axis| axes.contains(&axis));
    !beam_axis_selected
        || image
            .image_info()
            .is_ok_and(|info| !info.beam_set.is_multi())
}

pub(super) fn plan<T: ImagePixel>(
    image: &impl ImageInterface<T>,
    chunk: &Chunk,
    axes: &[usize],
    global: bool,
    velocity: Option<&SpectralVelocity>,
) -> Option<FluxPlan> {
    let collapsed = |axis| global || axes.contains(&axis);
    let kinds = image.axis_types();
    if !kinds
        .iter()
        .enumerate()
        .any(|(axis, kind)| collapsed(axis) && *kind == CoordinateType::Direction)
        || kinds.iter().enumerate().any(|(axis, kind)| {
            collapsed(axis)
                && chunk.size[axis] > 1
                && !matches!(kind, CoordinateType::Direction | CoordinateType::Spectral)
        })
    {
        return None;
    }
    let coordinates = image.coordinates();
    let spectral = image.find_axis(CoordinateType::Spectral);
    let integrated_axis = spectral.filter(|&axis| collapsed(axis) && chunk.size[axis] > 1);
    if integrated_axis.is_some()
        && let Some(index) = coordinates.find_coordinate(CoordinateType::Spectral)
        && let CoordinateModel::Spectral(spectral) = coordinates.coordinate(index)
        && !spectral.has_linear_mapping()
    {
        return None;
    }
    let sample =
        integrated_axis.map(|axis| [chunk.start[axis], chunk.start[axis] + chunk.stride[axis]]);
    let units = image.units();
    let direction_stride = kinds
        .iter()
        .zip(&chunk.stride)
        .filter(|(kind, _)| **kind == CoordinateType::Direction)
        .map(|(_, &step)| step as f64)
        .product::<f64>();
    let beams = if let Some((brightness, _)) = units.split_once("/beam") {
        let jansky = Unit::new("Jy").expect("standard Jy unit");
        Quantity::new(1.0, brightness)
            .ok()?
            .get_value_in(&jansky)
            .ok()?;
        Some(image.image_info().ok()?.beam_set)
    } else {
        None
    };
    if let Some(beams) = beams.as_ref().filter(|beams| beams.is_multi()) {
        let integral = match integrated_axis {
            Some(axis) => Some(SpectralIntegral {
                density_axes: (0..chunk.size.len()).filter(|&item| item != axis).collect(),
                channel_width: channel_width(coordinates, sample, velocity)?,
            }),
            None => None,
        };
        let stokes = image.find_axis(CoordinateType::Stokes);
        let (density_axes, global_density) = if let Some(integral) = &integral {
            (integral.density_axes.as_slice(), false)
        } else {
            (axes, global)
        };
        let channel_index = DisplayIndex::new(spectral, chunk, density_axes, global_density);
        let stokes_index = DisplayIndex::new(stokes, chunk, density_axes, global_density);
        let original = |axis: Option<usize>, local: usize| {
            axis.map_or(0, |axis| chunk.start[axis] + local * chunk.stride[axis])
        };
        let mut values = Vec::with_capacity(channel_index.count * stokes_index.count);
        for channel in 0..channel_index.count {
            for plane in 0..stokes_index.count {
                let channel = original(spectral, channel);
                let plane = original(stokes, plane);
                if (beams.n_channels() > 1 && channel >= beams.n_channels())
                    || (beams.n_stokes() > 1 && plane >= beams.n_stokes())
                {
                    return None;
                }
                let beam = *beams.beam(channel, plane);
                let scale = statistics_flux_scale(coordinates, units, Some(beam), None)?;
                values.push(scale * direction_stride);
            }
        }
        Some(FluxPlan {
            scale: FluxScale::PerBeam {
                values,
                channel: channel_index,
                stokes: stokes_index,
            },
            integral,
        })
    } else {
        let beam = beams.and_then(|beams| beams.single_beam());
        let width = channel_width(coordinates, sample, velocity)?;
        let scale = statistics_flux_scale(coordinates, units, beam, None)?;
        Some(FluxPlan {
            scale: FluxScale::Uniform(scale * direction_stride * width),
            integral: None,
        })
    }
}

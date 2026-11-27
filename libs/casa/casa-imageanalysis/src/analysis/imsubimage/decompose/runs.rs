// SPDX-License-Identifier: LGPL-3.0-or-later

use std::collections::BTreeMap;

use ndarray::{Dimension, IxDyn, Slice};

use casa_images::ImageError;

use super::super::SubimagePlan;
use super::super::read_window::PixelSections;
use super::points::{neighborhood, pixel_strides, visit_selection};

#[derive(Clone, Copy)]
struct Run {
    line: usize,
    first: usize,
    end: usize,
}

pub(super) struct Bounds {
    discovery: usize,
    pub(super) lower: Vec<usize>,
    pub(super) upper: Vec<usize>,
}

pub(super) fn locate(
    reader: &PixelSections<'_, f32>,
    plan: &SubimagePlan,
    threshold: f32,
    connection: usize,
) -> Result<Vec<Bounds>, ImageError> {
    let strides = pixel_strides(&plan.shape)?;
    let first_axis = plan.kept_axes[0];
    let mut strips = Vec::new();
    visit_selection(reader, plan, |position, pixels, mask| {
        let mut lines = pixels.shape().to_vec();
        lines[first_axis] = 1;
        for index in ndarray::indices(IxDyn(&lines)) {
            let line = plan.kept_axes[1..]
                .iter()
                .zip(&strides[1..])
                .map(|(&axis, &step)| (position[axis] + index[axis]) * step)
                .sum();
            let mut point = index.slice().to_vec();
            let values = pixels.slice_each_axis(|axis| {
                let id = axis.axis.index();
                if id == first_axis {
                    Slice::from(..)
                } else {
                    Slice::from(point[id]..point[id] + 1)
                }
            });
            let mut first = None;
            for (sample, &value) in values.iter().enumerate() {
                point[first_axis] = sample;
                let accepted =
                    value >= threshold && mask.as_ref().is_none_or(|mask| mask[IxDyn(&point)]);
                match (first, accepted) {
                    (None, true) => first = Some(sample),
                    (Some(start), false) => {
                        strips.push(Run {
                            line,
                            first: position[first_axis] + start,
                            end: position[first_axis] + sample,
                        });
                        first = None;
                    }
                    _ => {}
                }
            }
            if let Some(start) = first {
                strips.push(Run {
                    line,
                    first: position[first_axis] + start,
                    end: position[first_axis] + values.len(),
                });
            }
        }
    })?;
    strips.sort_unstable_by_key(|run| (run.line, run.first));

    strips.dedup_by(|next, previous| {
        if next.line == previous.line && previous.end == next.first {
            previous.end = next.end;
            true
        } else {
            false
        }
    });
    let runs = strips;
    let mut parent: Vec<_> = (0..runs.len()).collect();
    let offsets = neighborhood(plan.shape.len(), connection)
        .into_iter()
        .filter(|offset| offset[0] == 0 && (offset[1] < 0 || (offset[1] == 0 && offset[2] < 0)))
        .collect::<Vec<_>>();
    for (index, run) in runs.iter().enumerate() {
        for offset in &offsets {
            let Some(neighbor) = adjacent_line(run.line, &plan.shape, &strides, *offset) else {
                continue;
            };
            let steps = offset[1].unsigned_abs() + offset[2].unsigned_abs();
            let padding = usize::from(steps < connection);
            let first = runs.partition_point(|other| {
                other.line < neighbor
                    || (other.line == neighbor && other.end - 1 + padding < run.first)
            });
            for (other_index, _) in runs
                .iter()
                .enumerate()
                .skip(first)
                .take_while(|(_, other)| {
                    other.line == neighbor && other.first <= run.end - 1 + padding
                })
            {
                let a = root(&mut parent, index);
                let b = root(&mut parent, other_index);
                parent[a.max(b)] = a.min(b);
            }
        }
    }

    let mut groups = BTreeMap::new();
    for (index, run) in runs.into_iter().enumerate() {
        let id = root(&mut parent, index);
        let mut lower = vec![run.first];
        lower.extend(
            strides
                .iter()
                .zip(&plan.shape)
                .skip(1)
                .map(|(&step, &length)| run.line / step % length),
        );
        let mut upper = lower
            .iter()
            .map(|&coordinate| coordinate + 1)
            .collect::<Vec<_>>();
        upper[0] = run.end;
        let discovery = run.first * strides[0] + run.line;
        groups
            .entry(id)
            .and_modify(|group: &mut Bounds| {
                group.discovery = group.discovery.min(discovery);
                for (a, &b) in group.lower.iter_mut().zip(&lower) {
                    *a = (*a).min(b);
                }
                for (a, &b) in group.upper.iter_mut().zip(&upper) {
                    *a = (*a).max(b);
                }
            })
            .or_insert(Bounds {
                discovery,
                lower,
                upper,
            });
    }
    let mut result = groups.into_values().collect::<Vec<_>>();
    result.sort_unstable_by_key(|bounds| bounds.discovery);
    Ok(result)
}

fn adjacent_line(
    line: usize,
    shape: &[usize],
    strides: &[usize],
    offset: [isize; 3],
) -> Option<usize> {
    let mut neighbor = line;
    for axis in 1..shape.len().min(3) {
        let value = line / strides[axis] % shape[axis];
        let moved = value.checked_add_signed(offset[axis])?;
        if moved >= shape[axis] {
            return None;
        }
        neighbor = neighbor.checked_add_signed(offset[axis] * strides[axis] as isize)?;
    }
    Some(neighbor)
}

fn root(parent: &mut [usize], mut at: usize) -> usize {
    while parent[at] != at {
        parent[at] = parent[parent[at]];
        at = parent[at];
    }
    at
}

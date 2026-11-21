// SPDX-License-Identifier: LGPL-3.0-or-later

use super::points::{Pixels, connected};

const UNASSIGNED: usize = usize::MAX;
const AMBIGUOUS: usize = usize::MAX - 1;

pub(super) fn levels(threshold: f32, maximum: f32, count: usize) -> Vec<f32> {
    let steps = (count - 1) as f32;
    let top = maximum - (maximum - threshold) / (3. * steps);
    (0..count)
        .map(|i| threshold + (top - threshold) * i as f32 / steps)
        .collect()
}

struct Seed {
    position: Vec<usize>,
    level: usize,
    live: bool,
}

pub(super) fn deblend(
    pixels: &Pixels,
    island: &[usize],
    offsets: &[[isize; 3]],
    levels: &[f32],
    min_range: usize,
    visited: &mut [bool],
) -> Vec<Vec<usize>> {
    let mut labels = vec![UNASSIGNED; island.len()];
    let mut seeds: Vec<Seed> = Vec::new();
    let mut order: Vec<usize> = Vec::new();
    let local = |point: usize| {
        island
            .binary_search(&point)
            .expect("connected inside the island")
    };

    for (level, &threshold) in levels.iter().enumerate().rev() {
        for &point in island {
            visited[point] = false;
        }
        let contours = connected(
            pixels,
            island.iter().copied(),
            offsets,
            |point| pixels.samples[point].value >= threshold,
            visited,
        );
        let mut existing = Vec::new();
        let mut merging = Vec::new();
        let mut new = Vec::new();
        for contour in contours {
            let group = contour
                .iter()
                .map(|&point| local(point))
                .collect::<Vec<_>>();
            let offspring = order
                .iter()
                .copied()
                .filter(|label| group.iter().any(|&point| labels[point] == *label))
                .collect::<Vec<_>>();
            match offspring.len() {
                0 => new.push(group),
                1 => existing.push((group, offspring[0])),
                _ => merging.push((group, offspring)),
            }
        }

        order.clear();
        if min_range > 1 {
            for (group, children) in &merging {
                for &child in children {
                    if seeds[child].level - level >= min_range {
                        continue;
                    }
                    let closest = children
                        .iter()
                        .copied()
                        .filter(|&other| other != child && seeds[other].live)
                        .min_by_key(|&other| {
                            distance(&seeds[child].position, &seeds[other].position)
                        });
                    let Some(closest) = closest else { break };
                    for &point in group {
                        if labels[point] == child {
                            labels[point] = closest;
                        }
                    }
                    seeds[child].live = false;
                }
            }
        }

        if !merging.is_empty() {
            let mut earlier = labels
                .iter()
                .enumerate()
                .filter(|&(_, label)| *label < AMBIGUOUS)
                .map(|(point, &label)| LabeledPixel {
                    position: position(pixels, island[point]),
                    label,
                    split_axis: 0,
                })
                .collect::<Vec<_>>();
            partition(&mut earlier, pixels.rank());
            for (group, children) in merging {
                // Search the previous contour's map. Ties between components
                // are resolved by seed distance after the final contour.
                for point in group {
                    if labels[point] == UNASSIGNED {
                        let mut nearest = Nearest {
                            key: (249, pixels.rank() + 1),
                            first: UNASSIGNED,
                            label: UNASSIGNED,
                        };
                        nearest.search(position(pixels, island[point]), &earlier);
                        if nearest.label == AMBIGUOUS || children.contains(&nearest.label) {
                            labels[point] = nearest.label;
                        }
                    }
                }
                order.extend(children.into_iter().filter(|&seed| seeds[seed].live));
            }
        }

        for (group, label) in existing {
            for point in group {
                labels[point] = label;
            }
            order.push(label);
        }
        for group in new {
            let mut position = vec![0u128; pixels.rank()];
            for &point in &group {
                for (sum, at) in position.iter_mut().zip(pixels.position(island[point])) {
                    *sum += at as u128;
                }
                labels[point] = seeds.len();
            }
            let position = position
                .into_iter()
                .map(|sum| (sum / group.len() as u128) as usize)
                .collect();
            order.push(seeds.len());
            seeds.push(Seed {
                position,
                level,
                live: true,
            });
        }
    }

    for (point, label) in labels.iter_mut().enumerate() {
        if *label == AMBIGUOUS {
            let pos = pixels.position(island[point]);
            if let Some(seed) = seeds
                .iter()
                .enumerate()
                .filter(|(_, seed)| seed.live)
                .min_by_key(|(_, seed)| distance(&pos, &seed.position))
            {
                *label = seed.0;
            }
        }
    }

    let mut by_seed = vec![Vec::new(); seeds.len()];
    for (&point, label) in island.iter().zip(labels) {
        if label < AMBIGUOUS {
            by_seed[label].push(point);
        }
    }
    order
        .into_iter()
        .map(|seed| std::mem::take(&mut by_seed[seed]))
        .filter(|region| !region.is_empty())
        .collect()
}

fn position(pixels: &Pixels, point: usize) -> [usize; 3] {
    let index = pixels.samples[point].index;
    std::array::from_fn(|axis| {
        if axis < pixels.rank() {
            pixels.coordinate(index, axis)
        } else {
            0
        }
    })
}

struct LabeledPixel {
    position: [usize; 3],
    label: usize,
    split_axis: usize,
}

// Store a balanced spatial tree in place. Splitting on the widest axis also
// prunes distant pixels on narrow strips and planar plateaus.
fn partition(points: &mut [LabeledPixel], rank: usize) {
    if points.len() < 2 {
        return;
    }
    let mut lower = [usize::MAX; 3];
    let mut upper = [0; 3];
    for point in points.iter() {
        for axis in 0..rank {
            lower[axis] = lower[axis].min(point.position[axis]);
            upper[axis] = upper[axis].max(point.position[axis]);
        }
    }
    let axis = (0..rank)
        .max_by_key(|&axis| upper[axis] - lower[axis])
        .expect("spatial decomposition has at least two axes");
    let (left, median, right) =
        points.select_nth_unstable_by_key(points.len() / 2, |point| point.position[axis]);
    median.split_axis = axis;
    partition(left, rank);
    partition(right, rank);
}

struct Nearest {
    key: (usize, usize),
    first: usize,
    label: usize,
}

impl Nearest {
    fn search(&mut self, position: [usize; 3], points: &[LabeledPixel]) {
        let (left, rest) = points.split_at(points.len() / 2);
        let Some((point, right)) = rest.split_first() else {
            return;
        };
        let delta =
            std::array::from_fn::<_, 3, _>(|axis| position[axis].abs_diff(point.position[axis]));
        let radius = delta[0].max(delta[1]).max(delta[2]);
        let axes = delta.into_iter().filter(|&distance| distance != 0).count();
        let candidate = (radius, axes);
        if candidate < self.key {
            self.key = candidate;
            self.first = point.label;
            self.label = point.label;
        } else if candidate == self.key && self.first != point.label {
            self.label = AMBIGUOUS;
        }
        let axis = point.split_axis;
        let (near, far) = if position[axis] < point.position[axis] {
            (left, right)
        } else {
            (right, left)
        };
        self.search(position, near);
        if delta[axis] <= self.key.0 {
            self.search(position, far);
        }
    }
}

fn distance(a: &[usize], b: &[usize]) -> u128 {
    a.iter().zip(b).fold(0u128, |sum, (&a, &b)| {
        let delta = a.abs_diff(b) as u128;
        sum.saturating_add(delta * delta)
    })
}

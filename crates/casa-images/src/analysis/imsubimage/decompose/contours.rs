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
    let mut order = Vec::new();
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
        for (group, children) in merging {
            if min_range > 1 {
                for &child in &children {
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
                    for &point in &group {
                        if labels[point] == child {
                            labels[point] = closest;
                        }
                    }
                    seeds[child].live = false;
                }
            }

            // Use the assignments from the previous contour. Two equal
            // nearest sources leave the pixel for the final seed-distance pass.
            let earlier = group
                .iter()
                .copied()
                .filter_map(|point| {
                    let label = labels[point];
                    (label < AMBIGUOUS).then(|| (label, pixels.position(island[point])))
                })
                .collect::<Vec<_>>();
            for &point in &group {
                if labels[point] == UNASSIGNED {
                    labels[point] = nearest(&pixels.position(island[point]), &earlier);
                }
            }
            order.extend(children.into_iter().filter(|&seed| seeds[seed].live));
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
        if *label >= AMBIGUOUS {
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
        .filter_map(|seed| (!by_seed[seed].is_empty()).then(|| std::mem::take(&mut by_seed[seed])))
        .collect()
}

fn nearest(position: &[usize], earlier: &[(usize, Vec<usize>)]) -> usize {
    let mut best = (usize::MAX, usize::MAX);
    let mut first = UNASSIGNED;
    let mut result = UNASSIGNED;
    for (label, other) in earlier {
        let mut radius = 0;
        let mut axes = 0;
        for (&a, &b) in position.iter().zip(other) {
            radius = radius.max(a.abs_diff(b));
            axes += usize::from(a != b);
        }
        let candidate = (radius, axes);
        if candidate < best {
            best = candidate;
            first = *label;
            result = *label;
        } else if candidate == best && first != *label {
            result = AMBIGUOUS;
        }
    }
    result
}

fn distance(a: &[usize], b: &[usize]) -> u128 {
    a.iter().zip(b).fold(0u128, |sum, (&a, &b)| {
        let delta = a.abs_diff(b) as u128;
        sum.saturating_add(delta * delta)
    })
}

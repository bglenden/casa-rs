// SPDX-License-Identifier: LGPL-3.0-or-later

use casa_numerics::solve_symmetric_ldlt_casacore;

use super::{ImageError, PixelSections, StridedSection, SubimagePlan, invalid};

#[derive(Clone, Copy)]
pub(super) struct Peak {
    pub(super) position: [f64; 2],
    pub(super) height: f64,
}

#[derive(Clone, Copy)]
pub(super) enum PeakRanking {
    Positive,
    Signed,
    Maximum { signed: bool },
}

struct Row {
    pixels: Vec<f32>,
    done: Vec<bool>,
}

fn read_row(
    reader: &PixelSections<'_, f32>,
    section: &StridedSection<'_>,
    axes: [usize; 2],
    y: usize,
    width: usize,
) -> Result<Row, ImageError> {
    let [x_axis, y_axis] = axes;
    let rank = section.shape.len();
    let mut point = vec![0; rank];
    let mut length = vec![1; rank];
    point[y_axis] = y;
    let mut pixels = Vec::with_capacity(width);
    let mut done = Vec::with_capacity(width);
    let block = (64 * 1024_usize).div_ceil(section.stride[x_axis]);
    for first in (0..width).step_by(block) {
        point[x_axis] = first;
        length[x_axis] = block.min(width - first);
        let (values, valid) =
            reader.read(&section.source_start(&point), &length, section.stride)?;
        pixels.extend(values.iter().copied());
        if let Some(valid) = valid {
            done.extend(valid.iter().map(|&valid| !valid));
        } else {
            done.resize(pixels.len(), false);
        }
    }
    Ok(Row { pixels, done })
}

pub(super) fn admits_shape(width: usize, height: usize) -> Result<bool, ImageError> {
    if height < 4 {
        return Err(invalid(format!(
            "Need at least 4 rows in image. Found only {height}"
        )));
    }
    Ok(width >= 3 && height > 4)
}

pub(super) fn search(
    reader: &PixelSections<'_, f32>,
    plan: &SubimagePlan,
    section: &StridedSection<'_>,
    nmax: usize,
    cutoff: f64,
    ranking: PeakRanking,
) -> Result<Vec<Peak>, ImageError> {
    let signed = matches!(
        ranking,
        PeakRanking::Signed | PeakRanking::Maximum { signed: true }
    );
    let maximum = matches!(ranking, PeakRanking::Maximum { .. });
    let width = plan.shape[0];
    let height = plan.shape[1];
    if !admits_shape(width, height)? || nmax == 0 {
        return Ok(Vec::new());
    }
    let axes = [plan.kept_axes[0], plan.kept_axes[1]];
    // CASA's first two interior scan rows are excluded by its initial
    // three-row buffer; the final row is available only as a neighbor.
    let mut active = [
        read_row(reader, section, axes, 2, width)?,
        read_row(reader, section, axes, 3, width)?,
        read_row(reader, section, axes, 4, width)?,
    ];
    let mut peaks: Vec<Peak> = Vec::new();
    let mut strongest_sample = 0.;
    for y in 3..height - 1 {
        if y > 3 {
            active.rotate_left(1);
            active[2] = read_row(reader, section, axes, y + 1, width)?;
        }
        for x in 1..width - 1 {
            let start = x - 1;
            let center = f64::from(active[1].pixels[x]);
            let sample_magnitude = if signed { center.abs() } else { center };
            if sample_magnitude <= 0. || (maximum && sample_magnitude < strongest_sample) {
                continue;
            }
            let brightest = peaks.first().map_or(0., |peak| peak.height.abs());
            let weakest = peaks.get(nmax - 1).map_or(0., |peak| peak.height.abs());
            if (!maximum
                && (sample_magnitude < 0.8 * cutoff * brightest
                    || sample_magnitude < 0.8 * weakest))
                || [
                    active[0].pixels[x],
                    active[1].pixels[x - 1],
                    active[1].pixels[x + 1],
                    active[2].pixels[x],
                ]
                .into_iter()
                .any(|other| {
                    let other = f64::from(other);
                    if maximum {
                        sample_magnitude < if signed { other.abs() } else { other }
                    } else {
                        sample_magnitude <= other.abs()
                    }
                })
            {
                continue;
            }
            if active
                .iter()
                .any(|line| line.done[start..=x + 1].iter().any(|&done| done))
            {
                continue;
            }
            let sign = if signed && center < 0. { -1. } else { 1. };
            let sample = std::array::from_fn(|row| {
                std::array::from_fn(|column| sign * f64::from(active[row].pixels[start + column]))
            });
            let Some((height, [ox, oy])) = quadratic(sample, maximum) else {
                continue;
            };
            let magnitude = if signed { height.abs() } else { height };
            if !maximum && magnitude < cutoff * brightest {
                continue;
            }
            let position = if maximum {
                if height <= 0. || (sample_magnitude == strongest_sample && magnitude < brightest) {
                    continue;
                }
                strongest_sample = sample_magnitude;
                0
            } else {
                peaks
                    .iter()
                    .position(|peak| magnitude >= peak.height)
                    .unwrap_or(peaks.len())
            };
            if position >= nmax {
                continue;
            }
            peaks.insert(
                position,
                Peak {
                    position: [x as f64 + ox, y as f64 + oy],
                    height: sign * height,
                },
            );
            peaks.truncate(nmax);
            if !maximum {
                for row in &mut active {
                    row.done[start..=x + 1].fill(true);
                }
            }
        }
    }
    let threshold = cutoff * peaks.first().map_or(0., |peak| peak.height.abs());
    let accepted = peaks
        .iter()
        .take_while(|peak| !(peak.height.abs() < threshold || peak.height == 0.))
        .count();
    peaks.truncate(accepted);
    Ok(peaks)
}

fn quadratic(sample: [[f64; 3]; 3], require_maximum: bool) -> Option<(f64, [f64; 2])> {
    // Weighted normal matrix for [1, y, x, y², x², yx] on a 3×3 window.
    // The center, face and corner weights are 1, 1/2 and 1/4.
    const NORMAL: [[f64; 6]; 6] = [
        [4., 0., 0., 2., 2., 0.],
        [0., 2., 0., 0., 0., 0.],
        [0., 0., 2., 0., 0., 0.],
        [0., 0., 0., 2., 1., 0.],
        [0., 0., 0., 0., 2., 0.],
        [0., 0., 0., 0., 0., 1.],
    ];
    let mut rhs = [0.; 6];
    for (row, y) in sample.into_iter().zip([-1_f64, 0., 1.]) {
        for (value, x) in row.into_iter().zip([-1_f64, 0., 1.]) {
            let weight = 1. - 0.5 * (x.abs() + y.abs()) + 0.25 * (x * y).abs();
            for (entry, term) in rhs.iter_mut().zip([1., y, x, y * y, x * x, y * x]) {
                *entry += term * value * weight;
            }
        }
    }
    let [a, b, c, d, e, f] = solve_symmetric_ldlt_casacore(NORMAL, rhs)?.map(|value| value as f32);
    let determinant = f * f - 4. * d * e;
    if require_maximum && (d >= 0. || e >= 0. || determinant >= 0.) {
        return None;
    }
    let x = f64::from(2. * c * d - b * f) / f64::from(determinant);
    let y = f64::from(2. * b * e - c * f) / f64::from(determinant);
    if x.abs() > 1. || y.abs() > 1. || !x.is_finite() || !y.is_finite() {
        return None;
    }
    // The source finder evaluates the stored polynomial with these offsets
    // in its original order before narrowing the peak to Float.
    let peak = (f64::from(a)
        + f64::from(b) * x
        + f64::from(c) * y
        + f64::from(d) * x * x
        + f64::from(e) * y * y
        + f64::from(f) * x * y) as f32;
    peak.is_finite().then(|| (f64::from(peak), [x, y]))
}

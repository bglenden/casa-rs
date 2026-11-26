// SPDX-License-Identifier: LGPL-3.0-or-later
//! Flag-aware double-circle gain normalization.

use casa_core::Complex32;
use ndarray::ArrayD;

use crate::single_dish::edges::Integration;

const ARCSEC_PER_RADIAN: f64 = 180. * 3600. / std::f64::consts::PI;
const SPEED_OF_LIGHT_M_S: f64 = 299_792_458.;

pub(super) struct GainSample<'a> {
    pub(super) integration: &'a Integration,
    pub(super) data: ArrayD<Complex32>,
    pub(super) flags: ArrayD<bool>,
}

pub(super) fn radius(
    requested: Option<f64>,
    frequencies: &[f64],
    aperture: f64,
) -> Result<f64, String> {
    if let Some(radius) = requested.filter(|radius| *radius > 0.) {
        return Ok(radius);
    }
    let last = frequencies
        .len()
        .checked_sub(1)
        .ok_or("Missing observing frequency.")?;
    let center = (frequencies[last / 2] + frequencies[frequencies.len() / 2]) / 2.;
    let radius = 0.5 * 1.13 * SPEED_OF_LIGHT_M_S / (aperture * center);
    if !radius.is_finite() || radius <= 0. {
        return Err("Size of central region is not properly set.".into());
    }
    Ok(radius)
}

pub(super) fn normalize(
    samples: &mut [GainSample<'_>],
    radius: f64,
    smooth: bool,
) -> Result<(), String> {
    let shape = samples[0].data.shape();
    let [receptors, channels] =
        <[usize; 2]>::try_from(shape).map_err(|_| "Double-circle data must have two axes.")?;
    if samples
        .iter()
        .any(|sample| sample.data.shape() != shape || sample.flags.shape() != shape)
    {
        return Err("Inconsistent visibility or flag shape in the gain group.".into());
    }
    let half_width = (radius * ARCSEC_PER_RADIAN / 1.5).round() as usize;
    let width = half_width.saturating_mul(2).saturating_add(1);
    let clip = smooth && (2..samples.len()).contains(&width);
    let count = samples.len();
    let mut data = Vec::with_capacity(count);
    let mut flags = Vec::with_capacity(count);
    let mut smoothed = Vec::with_capacity(count);
    for receptor in 0..receptors {
        for channel in 0..channels {
            let index = [receptor, channel];
            data.clear();
            flags.clear();
            for sample in samples.iter() {
                data.push(f64::from(sample.data[index].re));
                flags.push(sample.flags[index]);
            }
            let powers = if clip {
                smooth_power(&data, &flags, half_width, &mut smoothed);
                &smoothed
            } else {
                &data
            };
            let (sum, present) = powers
                .iter()
                .zip(&flags)
                .filter(|&(_, &flag)| !flag)
                .fold((0., 0), |(sum, count), (&power, _)| {
                    (sum + power, count + 1)
                });
            let mean = sum / f64::from(present.max(1));
            for (sample, (&raw, &power)) in samples.iter_mut().zip(data.iter().zip(powers)) {
                let ratio = mean / power;
                if raw != 0. && mean >= 0. && ratio > 0. {
                    sample.data[index].re = (1. / ratio.sqrt()) as f32;
                } else {
                    sample.data[index].re = 0.;
                    sample.flags[index] = true;
                }
            }
        }
    }
    Ok(())
}

fn smooth_power(data: &[f64], flags: &[bool], half_width: usize, output: &mut Vec<f64>) {
    // Advance the bounds monotonically. Each unflagged sample enters and
    // leaves the truncated moving window once.
    output.clear();
    let mut sum = 0.;
    let mut present = 0;
    let mut end = 0;
    for (center, (&power, &flag)) in data.iter().zip(flags).enumerate() {
        let bound = center.saturating_add(half_width + 1).min(data.len());
        for i in end..bound {
            if !flags[i] {
                sum += data[i];
                present += 1;
            }
        }
        end = bound;
        output.push(if flag {
            power
        } else {
            sum / f64::from(present.max(1))
        });
        if let Some(left) = center.checked_sub(half_width)
            && !flags[left]
        {
            sum -= data[left];
            present -= 1;
        }
    }
}

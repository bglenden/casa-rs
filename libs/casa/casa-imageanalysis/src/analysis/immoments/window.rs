// SPDX-License-Identifier: LGPL-3.0-or-later

use std::ops::RangeInclusive;

use crate::analysis::imsubimage::fitprofile::fit_gaussian_profile;
use casa_images::ImageError;

use super::PixelClip;

#[derive(Clone, Copy)]
pub(super) enum Method {
    Clip,
    Bosma,
    GaussianWindow,
    GaussianFit,
}

impl Method {
    pub(super) fn new(
        names: &[String],
        clip: PixelClip,
        smooth: bool,
        moments: &[i32],
    ) -> Result<Self, ImageError> {
        let methods = names
            .first()
            .filter(|first| !first.is_empty())
            .map_or_else(String::new, |_| names.join(" ").to_ascii_uppercase());
        let window = methods.contains("WIN");
        let fit = methods.contains("FIT");
        let clipped = !matches!(clip, PixelClip::All);
        if moments.contains(&4) && (smooth || window || fit || !clip.permits_median_coordinate()) {
            return Err(ImageError::CasaSelection(
                "The median coordinate moment requires basic reduction with a wholly positive or negative pixel range".into(),
            ));
        }
        if ((window || fit) && clipped) || (smooth && !window && (fit || !clipped)) {
            return Err(ImageError::CasaSelection(
                "Invalid combination of methods requested".into(),
            ));
        }
        Ok(match (window, fit) {
            (false, false) => Self::Clip,
            (true, false) => Self::Bosma,
            (true, true) => Self::GaussianWindow,
            (false, true) => Self::GaussianFit,
        })
    }
}

pub(super) enum Selection {
    All,
    None,
    Window(RangeInclusive<usize>),
    Model([f64; 4]),
}

impl Selection {
    pub(super) fn new(
        method: Method,
        selection: &[f32],
        valid: &[bool],
        peak_snr: f32,
        noise: f32,
    ) -> Self {
        if matches!(method, Method::Clip) {
            return Self::All;
        }
        let usable = || {
            selection
                .iter()
                .copied()
                .zip(valid.iter().copied())
                .enumerate()
                .filter_map(|(index, (value, keep))| {
                    (keep && value.is_finite()).then_some((index, value))
                })
        };
        let Some((peak, maximum)) =
            usable().reduce(|left, right| if left.1 < right.1 { right } else { left })
        else {
            return Self::None;
        };
        let minimum = usable().map(|(_, value)| value).fold(maximum, f32::min);
        let signal = minimum.abs().max(maximum.abs());
        if signal == 0. || signal / noise < peak_snr {
            return Self::None;
        }
        if matches!(method, Method::Bosma) {
            return bosma_window(selection.len(), peak, noise, usable)
                .map_or(Self::None, Self::Window);
        }
        let (sum, count) = usable().fold((0., 0), |(sum, count), (_, value)| {
            (sum + f64::from(value), count + 1)
        });
        let baseline = (sum / f64::from(count)) as f32;
        let start = [f64::from(maximum - baseline), peak as f64, 5.];
        let Some(parameters) = fit_gaussian_profile(
            usable().map(|(index, value)| (index as f64, f64::from(value))),
            start,
            Some(f64::from(baseline)),
        ) else {
            return Self::None;
        };
        if matches!(method, Method::GaussianFit) {
            return Self::Model(parameters);
        }
        let [_, center, fwhm, _] = parameters;
        // CASA uses three times the fitted FWHM, despite calling this a sigma window.
        let last = selection.len() - 1;
        let limit = |position: f64| ((position + 0.5) as usize).min(last);
        let lower = limit(center - 3. * fwhm);
        let upper = limit(center + 3. * fwhm);
        if upper.saturating_sub(lower) < 3 {
            Self::None
        } else {
            Self::Window(lower..=upper)
        }
    }
}

fn bosma_window<S: Iterator<Item = (usize, f32)>>(
    length: usize,
    peak: usize,
    noise: f32,
    usable: impl Fn() -> S,
) -> Option<RangeInclusive<usize>> {
    let last = length - 1;
    let mut lower = peak.saturating_sub(2);
    let mut upper = (peak + 2).min(last);
    let mut previous = None::<f32>;
    loop {
        let (sum, count) = usable()
            .filter(|(index, _)| *index < lower || *index > upper)
            .fold((0., 0_usize), |(sum, count), (_, value)| {
                (sum + f64::from(value), count + 1)
            });
        if count > 0 {
            let mean = (sum / count as f64) as f32;
            let tolerance = noise / (length + 1 + lower - upper) as f32;
            if previous.is_some_and(|previous| (previous - mean).abs() < tolerance) {
                return Some(lower..=upper);
            }
            previous = Some(mean);
        }
        if lower == 0 && upper == last {
            return None;
        }
        lower = lower.saturating_sub(2);
        upper = (upper + 2).min(last);
    }
}

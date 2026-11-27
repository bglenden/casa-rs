// SPDX-License-Identifier: LGPL-3.0-or-later
//! Cubic B-spline knots and the four active basis functions at each integration.

#[derive(Clone, Copy)]
pub(super) struct SampleBasis {
    pub(super) start: usize,
    pub(super) weights: [f64; 4],
}

/// The CASA knot grid has four repeated end knots and equally spaced interior
/// knots. An interval with fewer timestamps than control points cannot be fit.
pub(super) fn knots(times: &[f64], seconds: f64) -> Option<Vec<f64>> {
    let (&start, &end) = (times.first()?, times.last()?);
    let duration = end - start;
    if !duration.is_finite() || duration <= 0. || !seconds.is_finite() {
        return None;
    }
    let segments = if seconds > 0. {
        (duration / seconds).round().max(1.) as usize
    } else {
        1
    };
    if segments.checked_add(3)? > times.len() {
        return None;
    }
    let step = duration / segments as f64;
    let mut knots = vec![start; 4];
    knots.extend((1..segments).map(|segment| start + segment as f64 * step));
    knots.extend([end; 4]);
    Some(knots)
}

pub(super) fn sample(knots: &[f64], time: f64) -> Option<SampleBasis> {
    let count = knots.len().checked_sub(4)?;
    if count < 4 || !time.is_finite() || time < knots[3] || time > knots[count] {
        return None;
    }
    let span = knots
        .partition_point(|&knot| knot <= time)
        .saturating_sub(1)
        .min(count - 1);
    let start = span.checked_sub(3)?;
    let mut weights = [1., 0., 0., 0.];
    for degree in 1..=3 {
        let mut saved = 0.;
        for (offset, weight) in weights[..degree].iter_mut().enumerate() {
            let first = knots[span + 1 + offset - degree];
            let last = knots[span + offset + 1];
            let width = last - first;
            if width <= 0. {
                return None;
            }
            let scaled = *weight / width;
            *weight = saved + (last - time) * scaled;
            saved = (time - first) * scaled;
        }
        weights[degree] = saved;
    }
    Some(SampleBasis { start, weights })
}

// SPDX-License-Identifier: LGPL-3.0-or-later
//! Estimate cross-hand phase and source polarization from time-resolved coherencies.

use num_complex::{Complex32, Complex64};

use crate::solve::GainSolveMode;
use crate::solve::kernel::complex32_to_64;

/// CASA applies the selected amplitude or phase constraint to averaged
/// coherencies and scales phase-only weights by the measured power.
pub(super) fn solve_sample(mode: GainSolveMode, value: Complex32, weight: f32) -> (Complex32, f32) {
    match mode {
        GainSolveMode::Amplitude => (value.norm().into(), weight),
        GainSolveMode::Phase if value != Complex32::ZERO => {
            let amplitude = value.norm();
            (value / amplitude, weight * (amplitude * amplitude))
        }
        _ => (value, weight),
    }
}

#[derive(Clone, Default)]
pub(super) struct SegmentChannel {
    weight: f64,
    corrected: Complex64,
    model: Complex64,
    alignment: Complex64,
}

impl SegmentChannel {
    pub(super) fn has_weight(&self) -> bool {
        self.weight > 0.0
    }

    pub(super) fn observe(
        &mut self,
        [xy, yx]: [Complex32; 2],
        [mx, my]: [Complex32; 2],
        [wx, wy]: [f32; 2],
    ) {
        self.weight += f64::from(wx) + f64::from(wy);
        self.corrected += complex32_to_64(wx * xy) + complex32_to_64(wy * yx.conj());
        self.model += complex32_to_64(wx * mx) + complex32_to_64(wy * my.conj());
        self.alignment +=
            complex32_to_64((wx * mx.re) * xy) + complex32_to_64((wy * my.re) * yx.conj());
    }
}

pub(super) struct Segment {
    pub(super) angle: f64,
    pub(super) channels: Vec<SegmentChannel>,
}

pub(super) enum FitError {
    Singular,
}

pub(super) fn solve(
    segments: &[Segment],
    channels: usize,
    circular: bool,
    align_model: bool,
) -> Result<(Vec<Option<Complex32>>, Complex32), FitError> {
    let mut phases = Vec::with_capacity(channels);
    let mut previous = Complex64::ONE;
    let mut ambiguity = 1.0;
    let mut alignment = 0.0;
    for channel in 0..channels {
        let rows = segments
            .iter()
            .map(|segment| &segment.channels[channel])
            .filter(|row| row.weight > 0.0);
        if rows.clone().next().is_none() {
            phases.push(None);
            previous = Complex64::ONE;
            continue;
        }
        if !align_model && rows.clone().nth(1).is_none() {
            phases.push(None);
            continue;
        }
        let mut phase = if circular {
            let slope = fit_complex(rows.clone().map(|row| {
                (
                    row.weight,
                    row.model / row.weight,
                    row.corrected / row.weight,
                )
            }))?;
            let amplitude = slope.norm();
            if amplitude == 0.0 || !amplitude.is_finite() {
                phases.push(None);
                previous = Complex64::ONE;
                continue;
            }
            slope / amplitude
        } else {
            ambiguity * fit_phase(rows.clone())
        };
        if !circular && channel > 0 && (phase / previous).re < 0.0 {
            phase = -phase;
            ambiguity = -ambiguity;
        }
        alignment += rows.map(|row| (row.alignment / phase).re).sum::<f64>();
        previous = phase;
        phases.push(Some(phase));
    }
    if !circular && align_model && alignment < 0.0 {
        phases
            .iter_mut()
            .flatten()
            .for_each(|phase| *phase = -*phase);
    }
    let qu = if circular {
        let rows = segments.iter().filter_map(|segment| {
            let (weight, model, corrected) = segment.channels.iter().zip(&phases).fold(
                (0.0, Complex64::ZERO, Complex64::ZERO),
                |(weight, model, corrected), (row, phase)| {
                    let Some(phase) = phase.filter(|_| row.weight > 0.0) else {
                        return (weight, model, corrected);
                    };
                    let amplitude = row.model.norm();
                    if amplitude == 0.0 || !amplitude.is_finite() {
                        return (weight, model, corrected);
                    }
                    (
                        weight + row.weight,
                        model + row.weight * row.model / amplitude,
                        corrected + row.corrected / phase,
                    )
                },
            );
            (weight > 0.0).then(|| (weight, model / weight, corrected / weight))
        });
        fit_complex(rows)?
    } else {
        fit_linear(segments, &phases)?
    };
    Ok((
        phases
            .into_iter()
            .map(|phase| phase.map(to_complex32))
            .collect(),
        to_complex32(qu),
    ))
}

fn fit_phase<'a>(rows: impl Iterator<Item = &'a SegmentChannel>) -> Complex64 {
    let (mut weight, mut sum, mut rr, mut ii, mut ri) = (0.0, Complex64::ZERO, 0.0, 0.0, 0.0);
    for row in rows {
        let v = row.corrected / row.weight;
        weight += row.weight;
        sum += row.corrected;
        rr += row.weight * v.re * v.re;
        ii += row.weight * v.im * v.im;
        ri += row.weight * v.re * v.im;
    }
    rr -= sum.re * sum.re / weight;
    ii -= sum.im * sum.im / weight;
    ri -= sum.re * sum.im / weight;
    let shallow = ri / rr;
    let steep = ri / ii;
    let angle = if shallow.abs() < steep.abs() {
        shallow.atan()
    } else {
        steep.recip().atan()
    };
    Complex64::from_polar(1.0, if angle.is_finite() { angle } else { 0.0 })
}

fn fit_complex(
    rows: impl Iterator<Item = (f64, Complex64, Complex64)>,
) -> Result<Complex64, FitError> {
    let (mut weight, mut power) = (0.0, 0.0);
    let (mut model, mut corrected, mut product) =
        (Complex64::ZERO, Complex64::ZERO, Complex64::ZERO);
    for (w, m, d) in rows {
        weight += w;
        power += w * m.norm_sqr();
        model += w * m;
        corrected += w * d;
        product += w * m.conj() * d;
    }
    let (values, rank) = casa_numerics::solve_complex_normal_equations(
        vec![weight.into(), model, model.conj(), power.into()],
        vec![corrected, product],
    )
    .ok_or(FitError::Singular)?;
    if rank < 2 {
        return Err(FitError::Singular);
    }
    Ok(values[1])
}

fn fit_linear(segments: &[Segment], phases: &[Option<Complex64>]) -> Result<Complex64, FitError> {
    let mut matrix = [[0.0; 3]; 3];
    let mut values = [0.0; 3];
    for segment in segments {
        let (weight, corrected) = segment.channels.iter().zip(phases).fold(
            (0.0, 0.0),
            |(weight, corrected), (row, phase)| {
                if let Some(phase) = phase {
                    (weight + row.weight, corrected + (row.corrected / *phase).re)
                } else {
                    (weight, corrected)
                }
            },
        );
        if weight <= 0.0 {
            continue;
        }
        let (sine, cosine) = (2.0 * segment.angle).sin_cos();
        let basis = [-sine, cosine, 1.0];
        for j in 0..3 {
            values[j] += basis[j] * corrected;
            for k in j..3 {
                matrix[j][k] += weight * basis[j] * basis[k];
            }
        }
    }
    let [q, u, _] =
        casa_numerics::solve_symmetric_ldlt_casacore(matrix, values).ok_or(FitError::Singular)?;
    Ok(Complex64::new(q, u))
}

fn to_complex32(value: Complex64) -> Complex32 {
    Complex32::new(value.re as f32, value.im as f32)
}

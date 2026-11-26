// SPDX-License-Identifier: LGPL-3.0-or-later
//! Point component and flux record for CASA's manual `setjy` standard.

use casa_components::{
    ComponentShape, Flux, SkyComponent, SpectralIndex, SpectralKind, SpectralModel, TabularSpectrum,
};
use casa_ms::MsResult;
use casa_types::measures::direction::MDirection;
use casa_types::measures::frame::MeasFrame;
use casa_types::measures::frequency::{FrequencyRef, MFrequency};

use super::invalid;

pub mod image;
pub mod solar2010;
pub mod solar2012;
pub mod standards;

pub struct ManualFlux {
    flux_jy: [f64; 4],
    reference: MFrequency,
    spectral_index: Vec<f64>,
    fraction: Vec<f64>,
    angle: Vec<f64>,
    rotation_measure: f64,
    tabulate: bool,
}

impl ManualFlux {
    pub fn new(
        flux_jy: [f64; 4],
        reference: MFrequency,
        spectral_index: Vec<f64>,
        mut fraction: Vec<f64>,
        mut angle: Vec<f64>,
        rotation_measure: f64,
    ) -> MsResult<Self> {
        let [i, q, u, _] = flux_jy;
        if !flux_jy
            .iter()
            .chain(&spectral_index)
            .chain(&fraction)
            .chain(&angle)
            .all(|v| v.is_finite())
            || !reference.hz().is_finite()
            || reference.hz() <= 0.
            || !rotation_measure.is_finite()
            || i < 0.
        {
            return Err(invalid("invalid manual flux or spectral coefficients"));
        }
        let tabulate = spectral_index.len() > 1 || fraction.len() > 1 || angle.len() > 1;
        // Explicit Q and U define the reference linear fraction and angle.
        // The higher coefficients are retained when both lists are present.
        if q != 0. || u != 0. {
            if fraction.is_empty() || angle.is_empty() {
                fraction.resize(1, 0.);
                angle.resize(1, 0.);
            }
            // Zero I with explicit Q/U yields CASA's undefined linear fraction.
            fraction[0] = q.hypot(u) / i;
            angle[0] = 0.5 * u.atan2(q);
        }
        Ok(Self {
            flux_jy,
            reference,
            spectral_index,
            fraction,
            angle,
            rotation_measure,
            tabulate,
        })
    }

    /// Form the point model and the flux at the first reported frequency in
    /// the spectral window. The caller chooses the channel list or median.
    pub fn component(
        &self,
        direction: MDirection,
        frequencies: &[MFrequency],
        frame: &MeasFrame,
    ) -> MsResult<(SkyComponent, [f64; 4])> {
        let first = frequencies
            .first()
            .ok_or_else(|| invalid("empty setjy spectral window"))?;
        if self.tabulate {
            let stokes_flux = frequencies
                .iter()
                .map(|frequency| self.at_frequency(frequency, frame))
                .collect::<MsResult<Vec<_>>>()?;
            return sampled_point(direction, frequencies, stokes_flux);
        }
        let first_flux = self.at_frequency(first, frame)?;
        let alpha = self.spectral_index.first().copied().unwrap_or_default();
        let kind = SpectralKind::SpectralIndex(SpectralIndex::new(alpha).with_stokes_indices([
            alpha,
            0.,
            self.rotation_measure,
            0.,
        ]));
        let defined_polarization = self.flux_jy[1..].iter().any(|&flux| flux != 0.)
            || (!self.fraction.is_empty() && !self.angle.is_empty());
        let (flux, model) = if defined_polarization {
            (first_flux, SpectralModel::new(first.clone(), kind))
        } else {
            (
                self.flux_jy,
                SpectralModel::new(self.reference.clone(), kind),
            )
        };
        let [i, q, u, v] = flux;
        Ok((
            SkyComponent::new(
                Flux::stokes_jy(i, q, u, v),
                ComponentShape::point(direction),
                model,
            ),
            first_flux,
        ))
    }

    pub fn intensity_jy(&self) -> f64 {
        self.flux_jy[0]
    }

    pub fn reports_spectral_index(&self) -> bool {
        self.spectral_index
            .first()
            .is_some_and(|&index| index != 0.0)
    }

    pub fn has_spectral_dependence(&self) -> bool {
        self.spectral_index.iter().any(|&index| index != 0.0)
    }

    pub fn intensity_at(&self, frequency: &MFrequency, frame: &MeasFrame) -> MsResult<f64> {
        let i = self.intensity_jy();
        let f = frequency.hz();
        let f0 = self.reference.hz();
        Ok(if self.spectral_index.len() > 1 {
            let logarithm = (f / f0).log10();
            i * 10_f64.powf(logarithm * polynomial(&self.spectral_index, logarithm))
        } else {
            let alpha = self.spectral_index.first().copied().unwrap_or_default();
            let converted = frequency
                .convert_to(self.reference.refer(), frame)
                .map_err(invalid)?;
            i * (converted.hz() / f0).powf(alpha)
        })
    }

    fn at_frequency(&self, frequency: &MFrequency, frame: &MeasFrame) -> MsResult<[f64; 4]> {
        const C_METRES_PER_SECOND: f64 = 299_792_458.;
        let [i, q, u, v] = self.flux_jy;
        let f = frequency.hz();
        let f0 = self.reference.hz();
        let i_at_f = self.intensity_at(frequency, frame)?;
        let linear_model = !self.fraction.is_empty() && !self.angle.is_empty();
        let (q_at_f, u_at_f) = if !linear_model {
            (q, u)
        } else {
            let offset = (f - f0) / f0;
            let linear = i_at_f * polynomial(&self.fraction, offset);
            let lambda2 = (C_METRES_PER_SECOND / f).powi(2) - (C_METRES_PER_SECOND / f0).powi(2);
            let phi = 2. * (polynomial(&self.angle, offset) + self.rotation_measure * lambda2);
            let (sin, cos) = phi.sin_cos();
            (linear * cos, linear * sin)
        };
        Ok([
            i_at_f,
            q_at_f,
            u_at_f,
            if i != 0. {
                v * i_at_f / i
            } else if linear_model {
                0.
            } else {
                v
            },
        ])
    }
}

pub fn sampled_point(
    direction: MDirection,
    frequencies: &[MFrequency],
    stokes: Vec<[f64; 4]>,
) -> MsResult<(SkyComponent, [f64; 4])> {
    sampled_shape(ComponentShape::point(direction), frequencies, stokes)
}

pub(crate) fn sampled_shape(
    shape: ComponentShape,
    frequencies: &[MFrequency],
    stokes: Vec<[f64; 4]>,
) -> MsResult<(SkyComponent, [f64; 4])> {
    let first = frequencies
        .first()
        .ok_or_else(|| invalid("empty setjy spectral window"))?;
    let first_flux = stokes[0];
    let model = if frequencies.len() == 1 {
        // CASA changes only the reference value of a ConstantSpectrum.
        SpectralModel::new(
            MFrequency::new(first.hz(), FrequencyRef::LSRK),
            SpectralKind::Constant,
        )
    } else {
        let spectrum = TabularSpectrum::new(
            first.refer(),
            frequencies.iter().map(MFrequency::hz).collect(),
            stokes,
        )
        .map_err(invalid)?;
        SpectralModel::new(first.clone(), SpectralKind::Tabular(spectrum))
    };
    let [i, q, u, v] = first_flux;
    let component = SkyComponent::new(Flux::stokes_jy(i, q, u, v), shape, model);
    Ok((component, first_flux))
}

fn polynomial(coefficients: &[f64], x: f64) -> f64 {
    coefficients
        .iter()
        .rev()
        .fold(0., |value, &coefficient| value * x + coefficient)
}

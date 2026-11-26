// SPDX-License-Identifier: LGPL-3.0-or-later
//! Digital amplitude corrections for DiFX and VLBA correlation.

use casa_core::{Complex32, Correlation};
use casa_fft::Fft2;
use ndarray::Array2;

use casa_ms::MsResult;
use casa_ms::fits_visibility::reader_support::invalid;

#[derive(Clone, Copy, PartialEq, Eq)]
enum Correlator {
    DiFx,
    Vlba,
}

pub(super) struct DigitalCorrection {
    correlator: Correlator,
    corrected_during_correlation: bool,
    fft: Option<Fft2<f32>>,
    storage: Vec<Complex32>,
    lag: Vec<f32>,
}

impl DigitalCorrection {
    pub(super) fn for_correlator(name: &str, van_vleck: f64) -> Option<Self> {
        let correlator = match name {
            "DIFX" => Correlator::DiFx,
            "VLBA" => Correlator::Vlba,
            _ => return None,
        };
        Some(Self {
            correlator,
            corrected_during_correlation: van_vleck != 0.0,
            fft: None,
            storage: Vec::new(),
            lag: Vec::new(),
        })
    }

    pub(super) fn apply(
        &mut self,
        autocorrelation: bool,
        levels: [i32; 2],
        correlations: &[Correlation],
        data: &mut Array2<Complex32>,
        original_weights: &Array2<f32>,
    ) -> MsResult<()> {
        const A: f64 = 5.36;
        const H: f64 = 0.878_906_25;
        type Rho = fn(f64) -> f64;
        let (rm, mut alpha, gamma, mut rho): (f64, f64, f64, Option<Rho>) = match levels {
            [4, 4] => (4.3048, 0.882518, 3.335875 * 64.0 / 63.0, Some(rho_four)),
            [2, 2] => (1.0, 2.0 / std::f64::consts::PI, 64.0 / 63.0, Some(rho_two)),
            [4, 2] | [2, 4] => (5.8784, 0.939060, 3.335875 * 64.0 / 63.0, Some(rho_four)),
            _ => (1.0 / (A * H), 1.0, 1.0, None),
        };
        if self.corrected_during_correlation {
            alpha = 1.0;
            rho = None;
        }
        let gain = gamma * gamma / (A * rm * H);
        let cross = (gain / alpha) as f32;
        let Some(rho) = rho.filter(|_| autocorrelation) else {
            data.mapv_inplace(|value| value * cross);
            return Ok(());
        };
        let receptors = correlations.len();
        for (hand, correlation) in correlations.iter().enumerate() {
            let mut values = data.row_mut(hand);
            if !matches!(
                correlation,
                Correlation::RR | Correlation::LL | Correlation::XX | Correlation::YY
            ) {
                values.mapv_inplace(|value| value * cross);
                continue;
            }
            if (gain as f32 * values[0].re).abs() < 1e-20 {
                continue;
            }
            let channels = values.len();
            self.correct_spectrum(&mut values, gain, rho)?;
            if self.correlator == Correlator::Vlba {
                let factor = if receptors > 2 { 0.25 } else { 0.125 };
                for channel in 0..channels {
                    values[channel] *= 1.0 + factor * original_weights[[hand, channel]];
                }
            }
        }
        Ok(())
    }

    fn correct_spectrum(
        &mut self,
        values: &mut ndarray::ArrayViewMut1<'_, Complex32>,
        gain: f64,
        rho: fn(f64) -> f64,
    ) -> MsResult<()> {
        let channels = values.len();
        let extension = channels
            .checked_mul(2)
            .ok_or_else(|| invalid("FITS-IDI channel count overflow"))?;
        if self
            .fft
            .as_ref()
            .is_none_or(|fft| fft.shape() != [1, extension])
        {
            self.fft = Some(
                Fft2::with_threads([1, extension], 1)
                    .map_err(invalid)?
                    .with_estimated_plan(),
            );
            self.storage.resize(extension, Complex32::ZERO);
            self.lag.resize(channels + 1, 0.0);
        }
        for (sample, value) in self.lag.iter_mut().zip(values.iter()) {
            *sample = (gain * f64::from(value.re)) as f32;
        }
        self.lag[channels] = 2.0 * self.lag[channels - 1] - self.lag[channels.saturating_sub(2)];
        self.cosine_transform()?;
        let zero = self.lag[0];
        for (index, sample) in self.lag[1..channels].iter_mut().enumerate() {
            let scale = (1.0 - (index + 1) as f32 / channels as f32) * zero;
            *sample = (f64::from(scale) * rho(f64::from(*sample / scale))) as f32;
        }
        self.lag[channels] = 0.0;
        self.cosine_transform()?;
        for (value, &real) in values.iter_mut().zip(&self.lag) {
            *value = Complex32::new(real / extension as f32, 0.0);
        }
        Ok(())
    }

    fn cosine_transform(&mut self) -> MsResult<()> {
        let channels = self.lag.len() - 1;
        for (index, value) in self.storage.iter_mut().enumerate() {
            let mirrored = index.min(2 * channels - index);
            *value = Complex32::new(self.lag[mirrored], 0.0);
        }
        self.fft
            .as_mut()
            .expect("cosine FFT allocated")
            .transform(&mut self.storage, false)
            .map_err(invalid)?;
        for (sample, value) in self.lag.iter_mut().zip(&self.storage) {
            *sample = value.re;
        }
        Ok(())
    }
}

fn rho_two(value: f64) -> f64 {
    (std::f64::consts::FRAC_PI_2 * value).sin()
}

fn rho_four(value: f64) -> f64 {
    let square = value * value;
    let numerator = 1.1329552 + square * (-3.1056902 + square * (2.9296994 - 0.90122460 * square));
    let denominator = 1.0 + square * (-2.7056559 + square * (2.5012473 - 0.73985978 * square));
    value * numerator / denominator
}

// SPDX-License-Identifier: LGPL-3.0-or-later

use std::time::{SystemTime, UNIX_EPOCH};

use casa_images::ImageError;

// Seed expansion and word generation follow casacore's MLCG in
// casa/BasicMath/Random.cc. Its legacy 32-bit arithmetic determines
// the sequence visible to scripts that supply two seeds.
const SEED_TABLE: [u32; 32] = [
    0xbdcc47e5, 0x54aea45d, 0xec0df859, 0xda84637b, 0xc8c6cb4f, 0x35574b01, 0x28260b7d, 0x0d07fdbf,
    0x9faaeeb0, 0x613dd169, 0x5ce2d818, 0x85b9e706, 0xab2469db, 0xda02b0dc, 0x45c60d6e, 0xffe49d10,
    0x7224fea3, 0xf9684fc9, 0xfc7ee074, 0x326ce92a, 0x366d13b5, 0x17aaa731, 0xeb83a675, 0x7781cb32,
    0x4ec7c92d, 0x7f187521, 0x2cf346b4, 0xad13310f, 0xb89cff2b, 0x12164de1, 0xa865168d, 0x32b56cdf,
];

pub(super) struct Generator {
    first: i32,
    second: i32,
    spare_normal: Option<f64>,
}

impl Generator {
    pub(super) fn new(seeds: &[i64]) -> Self {
        let expand = |seed: i32, period| {
            let seed = if seed < 0 {
                (seed + 2_147_483_561).abs()
            } else {
                seed
            };
            let entry = SEED_TABLE[seed as usize & (SEED_TABLE.len() - 1)] as i32;
            let seed = if seed < SEED_TABLE.len() as i32 {
                entry
            } else {
                seed ^ entry
            };
            (seed % period) + 1
        };
        let (first, second) = match seeds {
            [first, second, ..] => (*first as i32, *second as i32),
            _ => {
                let elapsed = SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .map_or(0.0, |elapsed| elapsed.as_secs_f64());
                let base = (40_587.0 + elapsed / 86_400.0) * 1e7;
                let second = base as i64 as i32;
                let first = seeds.first().map_or_else(
                    || (1e7 * (base - f64::from(second))) as i64 as i32,
                    |&seed| seed as i32,
                );
                (first, second)
            }
        };
        Self {
            first: expand(first, 2_147_483_561),
            second: expand(second, 2_147_483_397),
            spare_normal: None,
        }
    }

    fn word(&mut self) -> u32 {
        let advance = |seed: i32, multiplier: i32, divisor: i32, correction: i32, period: i32| {
            let residue = seed % divisor;
            let next = multiplier
                .wrapping_mul(seed.wrapping_sub(residue.wrapping_mul(divisor)))
                .wrapping_sub(residue.wrapping_mul(correction));
            if next < 0 { next + period } else { next }
        };
        self.first = advance(self.first, 40_014, 53_668, 12_211, 2_147_483_563);
        self.second = advance(self.second, 40_692, 52_774, 3_791, 2_147_483_399);
        let value = self.first.wrapping_sub(self.second);
        (if value < 1 {
            value + 2_147_483_562
        } else {
            value
        }) as u32
    }

    fn uniform(&mut self) -> f64 {
        let high = u64::from(self.word() & 0xfffff) << 32;
        let low = u64::from(self.word());
        f64::from_bits(1.0_f64.to_bits() | high | low) - 1.0
    }

    fn normal(&mut self) -> f64 {
        if let Some(spare) = self.spare_normal.take() {
            return spare;
        }
        loop {
            let first = 2.0 * self.uniform() - 1.0;
            let second = 2.0 * self.uniform() - 1.0;
            let distance = first * first + second * second;
            if distance <= 1.0 {
                let scale = (-2.0 * distance.ln() / distance).sqrt();
                self.spare_normal = Some(second * scale);
                return first * scale;
            }
        }
    }
}

pub(super) enum Distribution {
    Binomial(u32, f64),
    DiscreteUniform(i32, f64),
    Erlang(i32, f64),
    Geometric(f64),
    HyperGeometric(f64, f64),
    Normal(f64, f64),
    LogNormal(f64, f64),
    NegativeExponential(f64),
    Poisson(f64),
    Uniform(f64, f64),
    Weibull(f64, f64),
}

impl Distribution {
    pub(super) fn parse(name: &str, parameters: &[f64]) -> Result<Self, ImageError> {
        let name = name.to_ascii_uppercase();
        let nonzero = |value: f64| value.abs() > 1e-13;
        let probability = |value: f64| (0.0..=1.0).contains(&value);
        let illegal =
            || ImageError::CasaSelection("The distribution parameters are illegal".into());
        let distribution = match (name.as_str(), parameters) {
            ("BINOMIAL", &[trials, chance]) if trials >= 0.5 && probability(chance) => {
                let trials = if trials < -(i64::MIN as f64) {
                    trials as i64 as u32
                } else {
                    0
                };
                if trials == 0 {
                    return Err(ImageError::CasaSelection(
                        "Failed AlwaysAssert newN > 0".into(),
                    ));
                }
                Self::Binomial(trials, chance)
            }
            ("DISCRETEUNIFORM", &[low, high]) if low <= high => {
                let low = legacy_i32(low);
                let high = legacy_i32(high);
                if low > high {
                    return Err(ImageError::CasaSelection(
                        "Failed AlwaysAssert low <= high".into(),
                    ));
                }
                Self::DiscreteUniform(low, f64::from(high.wrapping_sub(low).wrapping_add(1)))
            }
            ("ERLANG", &[mean, variance]) if nonzero(mean) && variance > 0.0 => {
                let count = legacy_i32(mean * mean / variance + 0.5).max(1);
                Self::Erlang(count, f64::from(count) / mean)
            }
            ("GEOMETRIC", &[0.0]) => return Err(illegal()),
            ("GEOMETRIC", &[chance]) if (0.0..1.0).contains(&chance) => Self::Geometric(chance),
            ("HYPERGEOMETRIC", &[mean, variance])
                if nonzero(mean) && variance > 0.0 && mean * mean <= variance =>
            {
                // The mean setter first checks against the constructor's unit variance.
                if mean.abs() > 1.0 {
                    return Err(ImageError::CasaSelection(
                        "Failed AlwaysAssert itsMean*itsMean <= itsVariance".into(),
                    ));
                }
                let ratio = variance / (mean * mean);
                let chance = 0.5 * (1.0 - ((ratio - 1.0) / (ratio + 1.0)).sqrt());
                Self::HyperGeometric(mean, chance)
            }
            ("NORMAL", &[mean, variance]) if variance > 0.0 => Self::Normal(mean, variance.sqrt()),
            ("LOGNORMAL", &[mean, variance]) if nonzero(mean) && variance > 0.0 => {
                let mean_squared = mean * mean;
                // The mean setter first tries to build a Normal distribution
                // with the constructor's original lognormal variance of one.
                let initial_ratio = (1.0 + mean_squared) / mean_squared;
                if initial_ratio <= 1.0 || initial_ratio.is_nan() {
                    return Err(ImageError::CasaSelection(
                        "Failed AlwaysAssert itsVariance > 0.0".into(),
                    ));
                }
                let scale = ((variance + mean_squared) / mean_squared).ln().sqrt();
                if scale.is_nan() || scale <= 0.0 {
                    return Err(illegal());
                }
                Self::LogNormal(
                    (mean_squared / (variance + mean_squared).sqrt()).ln(),
                    scale,
                )
            }
            ("NEGATIVEEXPONENTIAL", &[mean]) => Self::NegativeExponential(mean),
            ("POISSON", &[mean]) if mean >= 0.0 => {
                let bound = (-mean).exp();
                if bound == 0.0 {
                    return Err(illegal());
                }
                Self::Poisson(bound)
            }
            ("UNIFORM", &[low, high]) if low < high => Self::Uniform(low, high - low),
            ("WEIBULL", &[alpha, beta]) if nonzero(alpha) && beta > 0.0 => {
                Self::Weibull(1.0 / alpha, beta)
            }
            (
                "BINOMIAL"
                | "DISCRETEUNIFORM"
                | "ERLANG"
                | "GEOMETRIC"
                | "HYPERGEOMETRIC"
                | "NORMAL"
                | "LOGNORMAL"
                | "NEGATIVEEXPONENTIAL"
                | "POISSON"
                | "UNIFORM"
                | "WEIBULL",
                _,
            ) => return Err(illegal()),
            _ => {
                return Err(ImageError::CasaSelection(
                    "Exception: You have not yet called function 'set'.".into(),
                ));
            }
        };
        Ok(distribution)
    }

    pub(super) fn draw(&self, generator: &mut Generator) -> f64 {
        match *self {
            Self::Binomial(trials, chance) => {
                let successes = (0..trials).filter(|_| generator.uniform() < chance).count();
                successes as f64
            }
            Self::DiscreteUniform(low, range) => {
                f64::from(low.wrapping_add((range * generator.uniform()).floor() as i32))
            }
            Self::Erlang(count, scale) => {
                let product: f64 = (0..count).map(|_| generator.uniform()).product();
                -product.ln() / scale
            }
            Self::Geometric(chance) => {
                let mut count = 0u32;
                while generator.uniform() > chance {
                    count = count.wrapping_add(1);
                }
                f64::from(count)
            }
            Self::HyperGeometric(mean, chance) => {
                let selected = if generator.uniform() > chance {
                    1.0 - chance
                } else {
                    chance
                };
                -mean * generator.uniform().ln() / (2.0 * selected)
            }
            Self::Normal(mean, scale) => generator.normal() * scale + mean,
            Self::LogNormal(mean, scale) => {
                std::f64::consts::E.powf(generator.normal() * scale + mean)
            }
            Self::NegativeExponential(mean) => -mean * generator.uniform().ln(),
            Self::Poisson(bound) => {
                let mut product = generator.uniform();
                let mut count = 0;
                while product >= bound {
                    product *= generator.uniform();
                    count += 1;
                }
                f64::from(count)
            }
            Self::Uniform(low, width) => low + width * generator.uniform(),
            Self::Weibull(power, beta) => (beta * -(1.0 - generator.uniform()).ln()).powf(power),
        }
    }
}

// The pinned CASA's signed double-to-int conversion returns INT_MIN
// whenever truncation cannot produce a signed 32-bit integer.
fn legacy_i32(value: f64) -> i32 {
    let value = value.trunc();
    if (f64::from(i32::MIN)..-f64::from(i32::MIN)).contains(&value) {
        value as i32
    } else {
        i32::MIN
    }
}

#[cfg(test)]
mod tests {
    use super::Distribution;

    #[test]
    fn noise_distribution_counts_match_pinned_integer_conversions() {
        assert!(matches!(
            Distribution::parse("erlang", &[50_000.0, 1.0]).unwrap(),
            Distribution::Erlang(1, _)
        ));
        assert!(matches!(
            Distribution::parse("binomial", &[4_294_967_297.0, 0.4]).unwrap(),
            Distribution::Binomial(1, _)
        ));
        assert!(Distribution::parse("binomial", &[4_294_967_296.0, 0.4]).is_err());
    }
}

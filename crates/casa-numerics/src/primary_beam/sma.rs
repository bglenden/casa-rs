// SPDX-License-Identifier: LGPL-3.0-or-later
//! PBMath's SMA beam: Float sphfn(3, 6, r), then PBMath1DNumeric sinc sampling.

#[derive(Clone)]
pub(super) struct SmaVoltageTable(Box<[f32]>);

impl SmaVoltageTable {
    pub(super) const MAXIMUM_RADIUS: f64 = (52.3 / 2. * 3. / 1.1117) / 60. * 224.;

    pub(super) fn new() -> Self {
        let samples: [f32; 31] = std::array::from_fn(|k| spheroid((k as f32 / 10.) / 3.));
        // Real, even length-60 transform. The source FFT has its centre at
        // sample 30; its (-1)^k shift cancels at the inverse's centre 10000.
        // Padding preserves the old Nyquist bin, now an ordinary bin, so
        // every nonzero bin contributes twice in the length-20000 inverse.
        let coefficients: [f64; 31] = std::array::from_fn(|k| {
            let end = if k % 2 == 0 {
                samples[30]
            } else {
                -samples[30]
            };
            let interior: f64 = (1..30)
                .map(|i| {
                    f64::from(samples[i]) * (std::f64::consts::TAU * (k * i) as f64 / 60.).cos()
                })
                .sum();
            f64::from((f64::from(samples[0] + end) + 2. * interior) as f32)
        });
        let sample = |index: usize| {
            (coefficients[0]
                + 2. * (1..31)
                    .map(|k| {
                        coefficients[k] * (std::f64::consts::PI * (k * index) as f64 / 10000.).cos()
                    })
                    .sum::<f64>()) as f32
        };
        let centre = sample(0);
        Self(
            (0..10000)
                .map(|index| (sample(index) / centre).max(0.).sqrt())
                .collect(),
        )
    }

    pub(super) fn evaluate(&self, radius: f64) -> f32 {
        if !radius.is_finite() || !(0. ..=Self::MAXIMUM_RADIUS).contains(&radius) {
            return 0.;
        }
        self.0[(radius * 9999. / Self::MAXIMUM_RADIUS)
            .floor()
            .clamp(0., 9999.) as usize]
    }
}

/// MathFunc2.cc's width-six, alpha-one fit, including its Float rounding
/// and convolution taper. The gridding correction uses the untapered fit.
fn spheroid(radius: f32) -> f32 {
    if radius >= 1. {
        return 0.;
    }
    let (p, q, end): ([f32; 5], [f32; 2], f32) = if radius <= 0.75 {
        (
            [0.08203343, -0.3644705, 0.627866, -0.5335581, 0.2312756],
            [0.8212018, 0.2078043],
            0.5625,
        )
    } else {
        (
            [0.004028559, -0.03697768, 0.1021332, -0.1201436, 0.06412774],
            [0.9599102, 0.2918724],
            1.,
        )
    };
    let squared = radius * radius;
    let x = squared - end;
    let numerator = p[0] + x * (p[1] + x * (p[2] + x * (p[3] + x * p[4])));
    let denominator = 1. + x * (q[0] + x * q[1]);
    (1. - squared) * (numerator / denominator)
}

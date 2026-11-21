// SPDX-License-Identifier: LGPL-3.0-or-later
//! Deterministic MT19937 stream for the WVR posterior sampler.

const STATE_WORDS: usize = 624;
const PERIOD_OFFSET: usize = 397;
const U32_RANGE: f64 = 4_294_967_296.0;

pub(super) struct Random {
    state: [u32; STATE_WORDS],
    index: usize,
    cached_normal: Option<f64>,
}

impl Random {
    pub fn new(seed: u32) -> Self {
        let mut state = [0; STATE_WORDS];
        state[0] = seed;
        for index in 1..STATE_WORDS {
            let previous = state[index - 1];
            state[index] = 1_812_433_253_u32
                .wrapping_mul(previous ^ (previous >> 30))
                .wrapping_add(index as u32);
        }
        Self {
            state,
            index: STATE_WORDS,
            cached_normal: None,
        }
    }

    fn word(&mut self) -> u32 {
        if self.index == STATE_WORDS {
            for index in 0..STATE_WORDS {
                let next = (index + 1) % STATE_WORDS;
                let offset = (index + PERIOD_OFFSET) % STATE_WORDS;
                let combined = (self.state[index] & 0x8000_0000) | (self.state[next] & 0x7fff_ffff);
                let twisted = if combined & 1 == 0 { 0 } else { 0x9908_b0df };
                self.state[index] = self.state[offset] ^ (combined >> 1) ^ twisted;
            }
            self.index = 0;
        }
        let mut value = self.state[self.index];
        self.index += 1;
        value ^= value >> 11;
        value ^= (value << 7) & 0x9d2c_5680;
        value ^= (value << 15) & 0xefc6_0000;
        value ^ (value >> 18)
    }

    pub fn uniform(&mut self) -> f64 {
        let low = f64::from(self.word());
        let high = f64::from(self.word());
        ((low + high * U32_RANGE) / (U32_RANGE * U32_RANGE)).min(1.0_f64.next_down())
    }

    pub fn normal(&mut self) -> f64 {
        if let Some(value) = self.cached_normal.take() {
            return value;
        }
        loop {
            let x = 2.0 * self.uniform() - 1.0;
            let y = 2.0 * self.uniform() - 1.0;
            let radius_squared = x * x + y * y;
            if radius_squared > 0.0 && radius_squared <= 1.0 {
                let scale = (-2.0 * radius_squared.ln() / radius_squared).sqrt();
                self.cached_normal = Some(x * scale);
                return y * scale;
            }
        }
    }
}

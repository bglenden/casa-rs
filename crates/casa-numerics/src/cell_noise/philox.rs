// SPDX-License-Identifier: LGPL-3.0-or-later
//! Philox4x32-10, the counter-based generator of Salmon, Moraes, Dror and
//! Shaw, "Parallel random numbers: as easy as 1, 2, 3" (SC11), as
//! Random123's `philox4x32` implements it.
//!
//! A draw is a pure function of a 128-bit counter and a 64-bit key: ten
//! rounds of two 32×32→64-bit multiplies, with the key bumped by the Weyl
//! constants between rounds. There is no state, so draws can be computed in
//! any order, on any number of workers, and are the same every time.
//!
//! Draws are made several counters at a time ([`philox4x32_lanes`]), held
//! word by word: every lane takes the same steps, so the lanes' multiplies
//! run side by side in vector registers, and each lane's words are exactly
//! those of that counter drawn alone.
//!
//! The known-answer tests are Random123's own (`tests/kat_vectors` in
//! <https://github.com/DEShawResearch/random123>, fetched 2026-10-21).

/// Philox's 128-bit counter.
#[cfg(test)]
pub(crate) type Counter = [u32; 4];
/// Philox's 64-bit key.
pub(crate) type Key = [u32; 2];
/// The counters of `LANES` draws, word by word: `lanes[word][lane]` is word
/// `word` of lane `lane`'s 128-bit counter.
pub(crate) type Lanes<const LANES: usize> = [[u32; LANES]; 4];

/// `PHILOX_M4x32_0` and `PHILOX_M4x32_1`.
const MULTIPLIERS: [u32; 2] = [0xD251_1F53, 0xCD9E_8D57];
/// `PHILOX_W32_0` and `PHILOX_W32_1`: the golden ratio and √3 − 1 as 32-bit
/// fractions.
const WEYL: [u32; 2] = [0x9E37_79B9, 0xBB67_AE85];

/// The high and low words of `a × b`.
#[inline(always)]
fn multiply_high_low(a: u32, b: u32) -> (u32, u32) {
    let product = u64::from(a) * u64::from(b);
    ((product >> 32) as u32, product as u32)
}

/// One round (`_philox4x32round`) of every lane.
///
/// Each lane's step is the scalar round's, `(c₀, c₁, c₂, c₃) →
/// (hi(M₁c₂) ⊕ c₁ ⊕ k₀, lo(M₁c₂), hi(M₀c₀) ⊕ c₃ ⊕ k₁, lo(M₀c₀))`. With
/// NEON, whole groups of four lanes take the step in vector registers
/// ([`neon`]) and any remaining lanes take it one at a time; elsewhere every
/// lane does. Integer arithmetic is exact, so the two give the same words.
#[inline(always)]
fn round<const LANES: usize>(counters: &Lanes<LANES>, key: Key) -> Lanes<LANES> {
    let mut next = [[0; LANES]; 4];
    #[cfg(all(target_arch = "aarch64", target_feature = "neon"))]
    let first_single = neon::round_fours(counters, key, &mut next);
    #[cfg(not(all(target_arch = "aarch64", target_feature = "neon")))]
    let first_single = 0;
    for lane in first_single..LANES {
        round_lane(counters, key, lane, &mut next);
    }
    next
}

/// One round of lane `lane`, into `next`.
#[inline(always)]
fn round_lane<const LANES: usize>(
    counters: &Lanes<LANES>,
    key: Key,
    lane: usize,
    next: &mut Lanes<LANES>,
) {
    let (high0, low0) = multiply_high_low(MULTIPLIERS[0], counters[0][lane]);
    let (high1, low1) = multiply_high_low(MULTIPLIERS[1], counters[2][lane]);
    next[0][lane] = high1 ^ counters[1][lane] ^ key[0];
    next[1][lane] = low1;
    next[2][lane] = high0 ^ counters[3][lane] ^ key[1];
    next[3][lane] = low0;
}

/// The round on NEON, four lanes to a register.
///
/// The compiler does not vectorise the portable round's 32×32→64-bit
/// multiplies (its cost model prices them as 64-bit vector multiplies,
/// which NEON lacks), so this spells out the instructions: `umull` and
/// `umull2` form the four 64-bit products of a register, and `uzp2` and
/// `uzp1` gather their high and low halves. It runs a round of eight lanes
/// in about 24 instructions, against about 70 one lane at a time.
#[cfg(all(target_arch = "aarch64", target_feature = "neon"))]
mod neon {
    use std::arch::aarch64::{
        uint32x4_t, vdupq_n_u32, veorq_u32, vget_low_u32, vld1q_u32, vmull_high_u32, vmull_u32,
        vreinterpretq_u32_u64, vst1q_u32, vuzp1q_u32, vuzp2q_u32,
    };

    use super::{Key, Lanes, MULTIPLIERS};

    /// One round of lanes `0..4n`, the whole groups of four, into `next`;
    /// returns `4n`, the first lane left for the one-at-a-time step.
    #[inline(always)]
    pub(super) fn round_fours<const LANES: usize>(
        counters: &Lanes<LANES>,
        key: Key,
        next: &mut Lanes<LANES>,
    ) -> usize {
        let whole = LANES - LANES % 4;
        for base in (0..whole).step_by(4) {
            // SAFETY: the intrinsics need NEON, which this module's `cfg`
            // requires; `base + 4 <= LANES`, so each load reads and each
            // store writes four words inside one lane array.
            unsafe {
                let load = |word: usize| vld1q_u32(counters[word].as_ptr().add(base));
                let (c0, c1, c2, c3) = (load(0), load(1), load(2), load(3));
                let (high0, low0) = multiply_high_low(MULTIPLIERS[0], c0);
                let (high1, low1) = multiply_high_low(MULTIPLIERS[1], c2);
                let n0 = veorq_u32(veorq_u32(high1, c1), vdupq_n_u32(key[0]));
                let n2 = veorq_u32(veorq_u32(high0, c3), vdupq_n_u32(key[1]));
                vst1q_u32(next[0].as_mut_ptr().add(base), n0);
                vst1q_u32(next[1].as_mut_ptr().add(base), low1);
                vst1q_u32(next[2].as_mut_ptr().add(base), n2);
                vst1q_u32(next[3].as_mut_ptr().add(base), low0);
            }
        }
        whole
    }

    /// The high and low words of `multiplier × c` in each of four lanes.
    /// Read as 32-bit lanes, a register of two little-endian 64-bit products
    /// is (low, high, low, high).
    #[inline(always)]
    fn multiply_high_low(multiplier: u32, counter: uint32x4_t) -> (uint32x4_t, uint32x4_t) {
        // SAFETY: the intrinsics need NEON, which this module's `cfg`
        // requires; they touch registers only.
        unsafe {
            let multiplier = vdupq_n_u32(multiplier);
            let lower =
                vreinterpretq_u32_u64(vmull_u32(vget_low_u32(counter), vget_low_u32(multiplier)));
            let upper = vreinterpretq_u32_u64(vmull_high_u32(counter, multiplier));
            (vuzp2q_u32(lower, upper), vuzp1q_u32(lower, upper))
        }
    }
}

/// Philox4x32 with `ROUNDS` rounds of each of `LANES` counters: the first
/// round with `key`, each later one with the key bumped once more by the
/// Weyl constants. Lane `l` of the result is the scalar generator's output
/// for lane `l`'s counter.
#[inline(always)]
pub(crate) fn philox4x32_lanes<const ROUNDS: usize, const LANES: usize>(
    counters: Lanes<LANES>,
    key: Key,
) -> Lanes<LANES> {
    let mut counters = counters;
    let mut key = key;
    for index in 0..ROUNDS {
        if index > 0 {
            key = [key[0].wrapping_add(WEYL[0]), key[1].wrapping_add(WEYL[1])];
        }
        counters = round(&counters, key);
    }
    counters
}

/// Philox4x32-10, Random123's recommended round count, of `LANES` counters.
#[inline(always)]
pub(crate) fn philox4x32_10_lanes<const LANES: usize>(
    counters: Lanes<LANES>,
    key: Key,
) -> Lanes<LANES> {
    philox4x32_lanes::<10, LANES>(counters, key)
}

/// Philox4x32-10 of one 128-bit `counter` under the 64-bit `key`: the four
/// words of one draw.
///
/// Philox4x32 (Salmon, Moraes, Dror and Shaw, "Parallel random numbers: as
/// easy as 1, 2, 3", SC11) is ten rounds, as Random123's `philox4x32_10`,
/// of `(c₀, c₁, c₂, c₃) → (hi(M₁c₂) ⊕ c₁ ⊕ k₀, lo(M₁c₂), hi(M₀c₀) ⊕ c₃ ⊕ k₁,
/// lo(M₀c₀))`, with `M₀ = 0xD2511F53` and `M₁ = 0xCD9E8D57`, the key
/// bumped by the Weyl constants `(0x9E3779B9, 0xBB67AE85)` before every
/// round after the first. It keeps no state: the words are a pure function
/// of the counter and the key, matching Random123's known answers.
///
/// This is the draw [`CellNoise::add_to_row`](super::CellNoise::add_to_row)
/// makes for each correlation pair, sixteen channels at a time; one counter
/// at a time it is the reference other implementations of the same noise
/// (another batch width, a GPU kernel) are compared with, bit for bit.
#[must_use]
pub fn philox4x32_10(counter: [u32; 4], key: [u32; 2]) -> [u32; 4] {
    let words = philox4x32_10_lanes(counter.map(|word| [word]), key);
    words.map(|[word]| word)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// One `philox4x32` line of Random123's `kat_vectors`: rounds, counter,
    /// key and the expected output, words in the file's order.
    struct Vector {
        rounds: usize,
        counter: Counter,
        key: Key,
        expected: Counter,
    }

    /// Copied verbatim from `tests/kat_vectors`.
    const KAT: [Vector; 6] = [
        Vector {
            rounds: 7,
            counter: [0x0000_0000, 0x0000_0000, 0x0000_0000, 0x0000_0000],
            key: [0x0000_0000, 0x0000_0000],
            expected: [0x5f6f_b709, 0x0d89_3f64, 0x4f12_1f81, 0x4f73_0a48],
        },
        Vector {
            rounds: 7,
            counter: [0xffff_ffff, 0xffff_ffff, 0xffff_ffff, 0xffff_ffff],
            key: [0xffff_ffff, 0xffff_ffff],
            expected: [0x5207_ddc2, 0x4516_5e59, 0x4d8e_e751, 0x8c52_f662],
        },
        Vector {
            rounds: 7,
            counter: [0x243f_6a88, 0x85a3_08d3, 0x1319_8a2e, 0x0370_7344],
            key: [0xa409_3822, 0x299f_31d0],
            expected: [0x4dfc_caba, 0x190a_87f0, 0xc473_62ba, 0xb6b5_242a],
        },
        Vector {
            rounds: 10,
            counter: [0x0000_0000, 0x0000_0000, 0x0000_0000, 0x0000_0000],
            key: [0x0000_0000, 0x0000_0000],
            expected: [0x6627_e8d5, 0xe169_c58d, 0xbc57_ac4c, 0x9b00_dbd8],
        },
        Vector {
            rounds: 10,
            counter: [0xffff_ffff, 0xffff_ffff, 0xffff_ffff, 0xffff_ffff],
            key: [0xffff_ffff, 0xffff_ffff],
            expected: [0x408f_276d, 0x41c8_3b0e, 0xa20b_c7c6, 0x6d54_51fd],
        },
        Vector {
            rounds: 10,
            counter: [0x243f_6a88, 0x85a3_08d3, 0x1319_8a2e, 0x0370_7344],
            key: [0xa409_3822, 0x299f_31d0],
            expected: [0xd16c_fe09, 0x94fd_cceb, 0x5001_e420, 0x2412_6ea1],
        },
    ];

    /// `vector`'s counter in lane `lane`, and distinct counters elsewhere.
    fn lanes_with<const LANES: usize>(vector: &Vector, lane: usize) -> Lanes<LANES> {
        let mut lanes: Lanes<LANES> =
            std::array::from_fn(|word| std::array::from_fn(|other| (word * 31 + other) as u32));
        for (word, value) in vector.counter.iter().enumerate() {
            lanes[word][lane] = *value;
        }
        lanes
    }

    /// Every vector in every lane of a `LANES`-lane batch.
    fn assert_known_answers<const LANES: usize>() {
        for vector in &KAT {
            for lane in 0..LANES {
                let counters = lanes_with::<LANES>(vector, lane);
                let output = match vector.rounds {
                    7 => philox4x32_lanes::<7, LANES>(counters, vector.key),
                    10 => philox4x32_10_lanes(counters, vector.key),
                    other => panic!("no {other}-round vector"),
                };
                assert_eq!(
                    output.map(|words| words[lane]),
                    vector.expected,
                    "philox4x32-{} of {:08x?} under {:08x?} in lane {lane} of {LANES}",
                    vector.rounds,
                    vector.counter,
                    vector.key
                );
            }
        }
    }

    /// One lane alone, a group of four, eight, the noise path's
    /// [`LANES`](crate::cell_noise::LANES), and seven: one group of four
    /// (vector lanes on NEON) and three one at a time.
    #[test]
    fn random123_known_answers_in_every_lane() {
        assert_known_answers::<1>();
        assert_known_answers::<4>();
        assert_known_answers::<7>();
        assert_known_answers::<8>();
        assert_known_answers::<{ crate::cell_noise::LANES }>();
        for vector in KAT.iter().filter(|vector| vector.rounds == 10) {
            assert_eq!(philox4x32_10(vector.counter, vector.key), vector.expected);
        }
    }
}

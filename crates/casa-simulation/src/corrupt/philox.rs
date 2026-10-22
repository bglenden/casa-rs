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
//! The known-answer tests are Random123's own (`tests/kat_vectors` in
//! <https://github.com/DEShawResearch/random123>, fetched 2026-10-21).

/// Philox's 128-bit counter.
pub(crate) type Counter = [u32; 4];
/// Philox's 64-bit key.
pub(crate) type Key = [u32; 2];

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

/// One round (`_philox4x32round`).
#[inline(always)]
fn round(counter: Counter, key: Key) -> Counter {
    let (high0, low0) = multiply_high_low(MULTIPLIERS[0], counter[0]);
    let (high1, low1) = multiply_high_low(MULTIPLIERS[1], counter[2]);
    [
        high1 ^ counter[1] ^ key[0],
        low1,
        high0 ^ counter[3] ^ key[1],
        low0,
    ]
}

/// Philox4x32 with `ROUNDS` rounds: the first with `key`, each later one
/// with the key bumped once more by the Weyl constants.
#[inline(always)]
pub(crate) fn philox4x32<const ROUNDS: usize>(counter: Counter, key: Key) -> Counter {
    let mut counter = counter;
    let mut key = key;
    for index in 0..ROUNDS {
        if index > 0 {
            key = [key[0].wrapping_add(WEYL[0]), key[1].wrapping_add(WEYL[1])];
        }
        counter = round(counter, key);
    }
    counter
}

/// Philox4x32-10, Random123's recommended round count.
#[inline(always)]
pub(crate) fn philox4x32_10(counter: Counter, key: Key) -> Counter {
    philox4x32::<10>(counter, key)
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

    #[test]
    fn random123_known_answers() {
        for vector in &KAT {
            let output = match vector.rounds {
                7 => philox4x32::<7>(vector.counter, vector.key),
                10 => philox4x32_10(vector.counter, vector.key),
                other => panic!("no {other}-round vector"),
            };
            assert_eq!(
                output, vector.expected,
                "philox4x32-{} of {:08x?} under {:08x?}",
                vector.rounds, vector.counter, vector.key
            );
        }
    }
}

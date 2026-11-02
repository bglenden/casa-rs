// SPDX-License-Identifier: LGPL-3.0-or-later
//! CASA's efficient image sizes (`SynthesisUtilMethods::getOptimumSize`).

/// The image side CASA calls efficient for `pixels`: the smallest even
/// size from `pixels` (rounded up to even) whose prime factors are all at
/// most 5, searched up to the size made by raising each of the even size's
/// prime factors above 5 to the next number with no such factor.
///
/// `tclean` warns when a side is not its own optimum
/// (`SynthesisParamsImage::verify`), and `simutil.imtclean` replaces it.
/// This is `SynthesisUtilMethods::getOptimumSize` with its `primeFactors`,
/// as CASA 6.7.6 computes them: 0 stays 0 and 1 becomes 2.
#[must_use]
pub fn optimum_image_size(pixels: usize) -> usize {
    let even = pixels + pixels % 2;
    let raised: usize = prime_factors(even)
        .into_iter()
        .map(|factor| {
            let mut value = factor;
            while largest_prime_factor(value) > 5 {
                value += 1;
            }
            value
        })
        .product();
    (even..raised)
        .step_by(2)
        .find(|&size| largest_prime_factor(size) < 6)
        .unwrap_or(raised)
}

fn largest_prime_factor(n: usize) -> usize {
    prime_factors(n).into_iter().max().unwrap_or(0)
}

/// `SynthesisUtilMethods::primeFactors`: `n`'s prime factors in ascending
/// order with repeats, `[1]` for 1 and `[n]` when the search finds none
/// (0).
fn prime_factors(n: usize) -> Vec<usize> {
    if n == 1 {
        return vec![1];
    }
    let root = |n: usize| (n as f64).sqrt() as usize + 1;
    let mut factors = Vec::new();
    let mut rest = n;
    let mut limit = root(n);
    let mut candidate = 2;
    while rest != 1 && candidate <= limit {
        limit = root(rest);
        while candidate <= limit && !rest.is_multiple_of(candidate) {
            candidate += 1;
        }
        if candidate > limit {
            candidate = rest;
        }
        factors.push(candidate);
        rest /= candidate;
    }
    if factors.is_empty() {
        factors.push(n);
    }
    factors
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn prime_factors_repeat_and_ascend() {
        assert_eq!(prime_factors(1), [1]);
        assert_eq!(prime_factors(2), [2]);
        assert_eq!(prime_factors(360), [2, 2, 2, 3, 3, 5]);
        assert_eq!(prime_factors(2 * 7 * 7 * 101), [2, 7, 7, 101]);
        assert_eq!(prime_factors(4099), [4099]);
    }

    #[test]
    fn efficient_sizes_are_their_own_optimum() {
        for size in [2, 100, 128, 360, 1000, 1024, 4096] {
            assert_eq!(optimum_image_size(size), size);
        }
        assert_eq!(optimum_image_size(101), 108);
        assert_eq!(optimum_image_size(99), 100);
    }
}

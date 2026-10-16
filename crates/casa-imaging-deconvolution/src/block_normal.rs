// SPDX-License-Identifier: LGPL-3.0-or-later

//! Reconstruction-owned polynomial basis and block-normal algebra.
//!
//! The compiled problem has already checked the reference frequency (finite
//! and positive) and the Taylor term count (at least two).

#[derive(Debug, Clone, Copy, PartialEq)]
pub(crate) struct BlockNormalPlan {
    reference_frequency_hz: f64,
    coefficient_terms: usize,
    normal_moments: usize,
}

impl BlockNormalPlan {
    pub(crate) const fn constant(reference_frequency_hz: f64) -> Self {
        Self {
            reference_frequency_hz,
            coefficient_terms: 1,
            normal_moments: 1,
        }
    }

    /// The Taylor basis of `coefficient_terms` terms, whose block normal has
    /// `2T-1` moments; `None` when that count overflows.
    pub(crate) fn taylor(reference_frequency_hz: f64, coefficient_terms: usize) -> Option<Self> {
        Some(Self {
            reference_frequency_hz,
            coefficient_terms,
            normal_moments: coefficient_terms.checked_mul(2)?.checked_sub(1)?,
        })
    }

    pub(crate) const fn reference_frequency_hz(self) -> f64 {
        self.reference_frequency_hz
    }

    pub(crate) const fn coefficient_term_count(self) -> usize {
        self.coefficient_terms
    }

    pub(crate) const fn normal_moment_count(self) -> usize {
        self.normal_moments
    }

    /// The normal moment `row + column` of a coefficient-block pair: the
    /// Taylor block normal is Hankel.
    pub(crate) fn normal_moment_index(self, row: usize, column: usize) -> Option<usize> {
        if row >= self.coefficient_terms || column >= self.coefficient_terms {
            return None;
        }
        row.checked_add(column)
            .filter(|index| *index < self.normal_moments)
    }
}

#[cfg(test)]
mod tests {
    use super::BlockNormalPlan;

    #[test]
    fn constant_and_taylor_plans_count_terms_and_moments() {
        let constant = BlockNormalPlan::constant(100.0);
        assert_eq!(constant.reference_frequency_hz(), 100.0);
        assert_eq!(constant.coefficient_term_count(), 1);
        assert_eq!(constant.normal_moment_count(), 1);
        assert_eq!(constant.normal_moment_index(0, 0), Some(0));
        let taylor = BlockNormalPlan::taylor(100.0, 3).expect("Taylor plan");
        assert_eq!(taylor.coefficient_term_count(), 3);
        assert_eq!(taylor.normal_moment_count(), 5);
    }

    #[test]
    fn block_normal_is_hankel() {
        let plan = BlockNormalPlan::taylor(100.0, 3).expect("Taylor plan");
        for row in 0..plan.coefficient_term_count() {
            for column in 0..plan.coefficient_term_count() {
                assert_eq!(plan.normal_moment_index(row, column), Some(row + column));
            }
        }
        assert_eq!(plan.normal_moment_index(3, 0), None);
        assert_eq!(plan.normal_moment_index(0, 3), None);
    }

    #[test]
    fn taylor_moment_count_overflow_fails_closed() {
        assert_eq!(BlockNormalPlan::taylor(100.0, usize::MAX), None);
    }
}

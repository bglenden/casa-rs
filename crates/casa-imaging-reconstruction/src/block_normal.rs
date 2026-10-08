// SPDX-License-Identifier: LGPL-3.0-or-later

//! Reconstruction-owned polynomial basis and block-normal algebra.

use std::{error::Error, fmt};

#[derive(Debug, Clone, Copy, PartialEq)]
pub(crate) struct BlockNormalPlan {
    reference_frequency_hz: f64,
    coefficient_terms: usize,
    normal_moments: usize,
}

impl BlockNormalPlan {
    pub(crate) fn constant(reference_frequency_hz: f64) -> Result<Self, BlockNormalError> {
        Self::compile(reference_frequency_hz, 1)
    }

    pub(crate) fn taylor(
        reference_frequency_hz: f64,
        coefficient_terms: usize,
    ) -> Result<Self, BlockNormalError> {
        if coefficient_terms < 2 {
            return Err(BlockNormalError::TaylorTermCount);
        }
        Self::compile(reference_frequency_hz, coefficient_terms)
    }

    pub(crate) fn compile(
        reference_frequency_hz: f64,
        coefficient_terms: usize,
    ) -> Result<Self, BlockNormalError> {
        if !reference_frequency_hz.is_finite() || reference_frequency_hz <= 0.0 {
            return Err(BlockNormalError::InvalidReferenceFrequency);
        }
        let normal_moments = coefficient_terms
            .checked_mul(2)
            .and_then(|terms| terms.checked_sub(1))
            .ok_or(BlockNormalError::SizeOverflow)?;
        i32::try_from(normal_moments - 1).map_err(|_| BlockNormalError::SizeOverflow)?;
        Ok(Self {
            reference_frequency_hz,
            coefficient_terms,
            normal_moments,
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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum BlockNormalError {
    InvalidReferenceFrequency,
    TaylorTermCount,
    SizeOverflow,
}

impl fmt::Display for BlockNormalError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidReferenceFrequency => {
                formatter.write_str("reference frequency must be finite and positive")
            }
            Self::TaylorTermCount => {
                formatter.write_str("Taylor block-normal algebra requires at least two terms")
            }
            Self::SizeOverflow => formatter.write_str("block-normal cardinality overflowed"),
        }
    }
}

impl Error for BlockNormalError {}

#[cfg(test)]
mod tests {
    use super::{BlockNormalError, BlockNormalPlan};

    #[test]
    fn t42_constant_and_taylor_plans_count_terms_and_moments() {
        let constant = BlockNormalPlan::constant(100.0).expect("constant plan");
        assert_eq!(constant.reference_frequency_hz(), 100.0);
        assert_eq!(constant.coefficient_term_count(), 1);
        assert_eq!(constant.normal_moment_count(), 1);
        assert_eq!(constant.normal_moment_index(0, 0), Some(0));
        let taylor = BlockNormalPlan::taylor(100.0, 3).expect("Taylor plan");
        assert_eq!(taylor.coefficient_term_count(), 3);
        assert_eq!(taylor.normal_moment_count(), 5);
    }

    #[test]
    fn t42_block_normal_is_hankel() {
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
    fn t42_invalid_inputs_and_cardinality_overflow_fail_closed() {
        assert_eq!(
            BlockNormalPlan::constant(0.0),
            Err(BlockNormalError::InvalidReferenceFrequency)
        );
        assert_eq!(
            BlockNormalPlan::taylor(100.0, 1),
            Err(BlockNormalError::TaylorTermCount)
        );
        assert_eq!(
            BlockNormalPlan::taylor(100.0, usize::MAX),
            Err(BlockNormalError::SizeOverflow)
        );
    }
}

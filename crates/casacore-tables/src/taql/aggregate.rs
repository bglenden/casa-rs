// SPDX-License-Identifier: LGPL-3.0-or-later
//! Accumulator types for TaQL aggregate functions.
//!
//! Provides [`Accumulator`] for COUNT, SUM, AVG, MIN, MAX.
//!
//! # C++ reference
//!
//! `TableExprGroupFunc*.cc`.

use super::ast::AggregateFunc;
use super::eval::ExprValue;

/// An accumulator for computing aggregate function results over a group of rows.
///
/// Each variant tracks the running state needed to compute the final aggregate
/// value. Call [`accumulate`](Accumulator::accumulate) for each row value, then
/// [`finish`](Accumulator::finish) to get the result.
pub struct Accumulator {
    func: AggregateFunc,
    count: i64,
    sum: f64,
    min: Option<ExprValue>,
    max: Option<ExprValue>,
}

impl Accumulator {
    /// Create a new accumulator for the given aggregate function.
    pub fn new(func: AggregateFunc) -> Self {
        Self {
            func,
            count: 0,
            sum: 0.0,
            min: None,
            max: None,
        }
    }

    /// Feed a value into the accumulator.
    pub fn accumulate(&mut self, val: &ExprValue) {
        if val.is_null() {
            // NULL values are ignored by aggregates (except COUNT(*))
            if self.func == AggregateFunc::Count {
                self.count += 1;
            }
            return;
        }

        self.count += 1;

        match self.func {
            AggregateFunc::Count => {}
            AggregateFunc::Sum | AggregateFunc::Avg => {
                if let Ok(f) = val.to_float() {
                    self.sum += f;
                }
            }
            AggregateFunc::Min => {
                let replace = match &self.min {
                    None => true,
                    Some(current) => val
                        .compare(current)
                        .is_ok_and(|o| o == std::cmp::Ordering::Less),
                };
                if replace {
                    self.min = Some(val.clone());
                }
            }
            AggregateFunc::Max => {
                let replace = match &self.max {
                    None => true,
                    Some(current) => val
                        .compare(current)
                        .is_ok_and(|o| o == std::cmp::Ordering::Greater),
                };
                if replace {
                    self.max = Some(val.clone());
                }
            }
        }
    }

    /// Finish the accumulation and return the final value.
    pub fn finish(&self) -> ExprValue {
        match self.func {
            AggregateFunc::Count => ExprValue::Int(self.count),
            AggregateFunc::Sum => ExprValue::Float(self.sum),
            AggregateFunc::Avg => {
                if self.count == 0 {
                    ExprValue::Null
                } else {
                    ExprValue::Float(self.sum / self.count as f64)
                }
            }
            AggregateFunc::Min => self.min.clone().unwrap_or(ExprValue::Null),
            AggregateFunc::Max => self.max.clone().unwrap_or(ExprValue::Null),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn count_basic() {
        let mut acc = Accumulator::new(AggregateFunc::Count);
        acc.accumulate(&ExprValue::Int(1));
        acc.accumulate(&ExprValue::Int(2));
        acc.accumulate(&ExprValue::Int(3));
        assert_eq!(acc.finish(), ExprValue::Int(3));
    }

    #[test]
    fn sum_basic() {
        let mut acc = Accumulator::new(AggregateFunc::Sum);
        acc.accumulate(&ExprValue::Float(1.0));
        acc.accumulate(&ExprValue::Float(2.0));
        acc.accumulate(&ExprValue::Float(3.0));
        assert_eq!(acc.finish(), ExprValue::Float(6.0));
    }

    #[test]
    fn avg_basic() {
        let mut acc = Accumulator::new(AggregateFunc::Avg);
        acc.accumulate(&ExprValue::Float(2.0));
        acc.accumulate(&ExprValue::Float(4.0));
        assert_eq!(acc.finish(), ExprValue::Float(3.0));
    }

    #[test]
    fn min_basic() {
        let mut acc = Accumulator::new(AggregateFunc::Min);
        acc.accumulate(&ExprValue::Int(3));
        acc.accumulate(&ExprValue::Int(1));
        acc.accumulate(&ExprValue::Int(2));
        assert_eq!(acc.finish(), ExprValue::Int(1));
    }

    #[test]
    fn max_basic() {
        let mut acc = Accumulator::new(AggregateFunc::Max);
        acc.accumulate(&ExprValue::Int(3));
        acc.accumulate(&ExprValue::Int(1));
        acc.accumulate(&ExprValue::Int(2));
        assert_eq!(acc.finish(), ExprValue::Int(3));
    }

    #[test]
    fn avg_empty() {
        let acc = Accumulator::new(AggregateFunc::Avg);
        assert!(acc.finish().is_null());
    }

    #[test]
    fn sum_with_nulls() {
        let mut acc = Accumulator::new(AggregateFunc::Sum);
        acc.accumulate(&ExprValue::Float(1.0));
        acc.accumulate(&ExprValue::Null);
        acc.accumulate(&ExprValue::Float(3.0));
        assert_eq!(acc.finish(), ExprValue::Float(4.0));
    }
}

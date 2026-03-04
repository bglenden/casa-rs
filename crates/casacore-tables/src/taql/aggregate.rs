// SPDX-License-Identifier: LGPL-3.0-or-later
//! Accumulator types for TaQL aggregate functions.
//!
//! Provides [`Accumulator`] for all 20 aggregate function variants.
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
    sum_sq: f64,
    min: Option<ExprValue>,
    max: Option<ExprValue>,
    first: Option<ExprValue>,
    last: Option<ExprValue>,
    product: f64,
    product_started: bool,
    bool_and: bool,
    bool_or: bool,
    ntrue: i64,
    nfalse: i64,
    /// Collected values for median/fractile (lazy accumulation).
    values: Vec<f64>,
    /// Fractile fraction (set from the second argument).
    fraction: f64,
}

impl Accumulator {
    /// Create a new accumulator for the given aggregate function.
    pub fn new(func: AggregateFunc) -> Self {
        Self {
            func,
            count: 0,
            sum: 0.0,
            sum_sq: 0.0,
            min: None,
            max: None,
            first: None,
            last: None,
            product: 1.0,
            product_started: false,
            bool_and: true,
            bool_or: false,
            ntrue: 0,
            nfalse: 0,
            values: Vec::new(),
            fraction: 0.5,
        }
    }

    /// Create a fractile accumulator with the given fraction.
    pub fn new_fractile(fraction: f64) -> Self {
        let mut acc = Self::new(AggregateFunc::Fractile);
        acc.fraction = fraction;
        acc
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
            AggregateFunc::First => {
                if self.first.is_none() {
                    self.first = Some(val.clone());
                }
            }
            AggregateFunc::Last => {
                self.last = Some(val.clone());
            }
            AggregateFunc::Product => {
                if let Ok(f) = val.to_float() {
                    if !self.product_started {
                        self.product = f;
                        self.product_started = true;
                    } else {
                        self.product *= f;
                    }
                }
            }
            AggregateFunc::SumSqr => {
                if let Ok(f) = val.to_float() {
                    self.sum_sq += f * f;
                }
            }
            AggregateFunc::Variance
            | AggregateFunc::SampleVariance
            | AggregateFunc::StdDev
            | AggregateFunc::SampleStdDev
            | AggregateFunc::Rms => {
                if let Ok(f) = val.to_float() {
                    self.sum += f;
                    self.sum_sq += f * f;
                }
            }
            AggregateFunc::Any => {
                if let Ok(b) = val.to_bool() {
                    self.bool_or = self.bool_or || b;
                }
            }
            AggregateFunc::All => {
                if let Ok(b) = val.to_bool() {
                    self.bool_and = self.bool_and && b;
                }
            }
            AggregateFunc::NTrue => {
                if let Ok(b) = val.to_bool() {
                    if b {
                        self.ntrue += 1;
                    }
                }
            }
            AggregateFunc::NFalse => {
                if let Ok(b) = val.to_bool() {
                    if !b {
                        self.nfalse += 1;
                    }
                }
            }
            AggregateFunc::Median | AggregateFunc::Fractile => {
                if let Ok(f) = val.to_float() {
                    self.values.push(f);
                }
            }
        }
    }

    /// Finish the accumulation and return the final value.
    pub fn finish(&mut self) -> ExprValue {
        let n = self.count;
        match self.func {
            AggregateFunc::Count => ExprValue::Int(n),
            AggregateFunc::Sum => ExprValue::Float(self.sum),
            AggregateFunc::Avg => {
                if n == 0 {
                    ExprValue::Null
                } else {
                    ExprValue::Float(self.sum / n as f64)
                }
            }
            AggregateFunc::Min => self.min.clone().unwrap_or(ExprValue::Null),
            AggregateFunc::Max => self.max.clone().unwrap_or(ExprValue::Null),
            AggregateFunc::First => self.first.clone().unwrap_or(ExprValue::Null),
            AggregateFunc::Last => self.last.clone().unwrap_or(ExprValue::Null),
            AggregateFunc::Product => {
                if !self.product_started {
                    ExprValue::Null
                } else {
                    ExprValue::Float(self.product)
                }
            }
            AggregateFunc::SumSqr => ExprValue::Float(self.sum_sq),
            AggregateFunc::Variance => {
                if n == 0 {
                    ExprValue::Null
                } else {
                    let mean = self.sum / n as f64;
                    ExprValue::Float(self.sum_sq / n as f64 - mean * mean)
                }
            }
            AggregateFunc::SampleVariance => {
                if n < 2 {
                    ExprValue::Null
                } else {
                    let mean = self.sum / n as f64;
                    let var = (self.sum_sq - n as f64 * mean * mean) / (n - 1) as f64;
                    ExprValue::Float(var)
                }
            }
            AggregateFunc::StdDev => {
                if n == 0 {
                    ExprValue::Null
                } else {
                    let mean = self.sum / n as f64;
                    ExprValue::Float((self.sum_sq / n as f64 - mean * mean).sqrt())
                }
            }
            AggregateFunc::SampleStdDev => {
                if n < 2 {
                    ExprValue::Null
                } else {
                    let mean = self.sum / n as f64;
                    let var = (self.sum_sq - n as f64 * mean * mean) / (n - 1) as f64;
                    ExprValue::Float(var.sqrt())
                }
            }
            AggregateFunc::Rms => {
                if n == 0 {
                    ExprValue::Null
                } else {
                    ExprValue::Float((self.sum_sq / n as f64).sqrt())
                }
            }
            AggregateFunc::Any => ExprValue::Bool(self.bool_or),
            AggregateFunc::All => {
                if n == 0 {
                    ExprValue::Bool(true) // vacuous truth
                } else {
                    ExprValue::Bool(self.bool_and)
                }
            }
            AggregateFunc::NTrue => ExprValue::Int(self.ntrue),
            AggregateFunc::NFalse => ExprValue::Int(self.nfalse),
            AggregateFunc::Median => {
                if self.values.is_empty() {
                    ExprValue::Null
                } else {
                    self.values.sort_by(|a, b| a.total_cmp(b));
                    let len = self.values.len();
                    let med = if len % 2 == 0 {
                        (self.values[len / 2 - 1] + self.values[len / 2]) / 2.0
                    } else {
                        self.values[len / 2]
                    };
                    ExprValue::Float(med)
                }
            }
            AggregateFunc::Fractile => {
                if self.values.is_empty() {
                    ExprValue::Null
                } else {
                    self.values.sort_by(|a, b| a.total_cmp(b));
                    let idx =
                        ((self.values.len() as f64 - 1.0) * self.fraction.clamp(0.0, 1.0)) as usize;
                    ExprValue::Float(self.values[idx])
                }
            }
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
        let mut acc = Accumulator::new(AggregateFunc::Avg);
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

    // ── New aggregate tests ──

    #[test]
    fn first_last() {
        let mut first = Accumulator::new(AggregateFunc::First);
        let mut last = Accumulator::new(AggregateFunc::Last);
        for v in [1, 2, 3] {
            first.accumulate(&ExprValue::Int(v));
            last.accumulate(&ExprValue::Int(v));
        }
        assert_eq!(first.finish(), ExprValue::Int(1));
        assert_eq!(last.finish(), ExprValue::Int(3));
    }

    #[test]
    fn product_basic() {
        let mut acc = Accumulator::new(AggregateFunc::Product);
        acc.accumulate(&ExprValue::Float(2.0));
        acc.accumulate(&ExprValue::Float(3.0));
        acc.accumulate(&ExprValue::Float(4.0));
        assert_eq!(acc.finish(), ExprValue::Float(24.0));
    }

    #[test]
    fn sumsqr_basic() {
        let mut acc = Accumulator::new(AggregateFunc::SumSqr);
        acc.accumulate(&ExprValue::Float(1.0));
        acc.accumulate(&ExprValue::Float(2.0));
        acc.accumulate(&ExprValue::Float(3.0));
        assert_eq!(acc.finish(), ExprValue::Float(14.0));
    }

    #[test]
    fn variance_population() {
        let mut acc = Accumulator::new(AggregateFunc::Variance);
        for v in [2.0, 4.0, 4.0, 4.0, 5.0, 5.0, 7.0, 9.0] {
            acc.accumulate(&ExprValue::Float(v));
        }
        match acc.finish() {
            ExprValue::Float(f) => assert!((f - 4.0).abs() < 0.01, "got {f}"),
            other => panic!("expected Float, got {other:?}"),
        }
    }

    #[test]
    fn stddev_population() {
        let mut acc = Accumulator::new(AggregateFunc::StdDev);
        for v in [2.0, 4.0, 4.0, 4.0, 5.0, 5.0, 7.0, 9.0] {
            acc.accumulate(&ExprValue::Float(v));
        }
        match acc.finish() {
            ExprValue::Float(f) => assert!((f - 2.0).abs() < 0.01, "got {f}"),
            other => panic!("expected Float, got {other:?}"),
        }
    }

    #[test]
    fn rms_basic() {
        let mut acc = Accumulator::new(AggregateFunc::Rms);
        acc.accumulate(&ExprValue::Float(1.0));
        acc.accumulate(&ExprValue::Float(2.0));
        acc.accumulate(&ExprValue::Float(3.0));
        match acc.finish() {
            ExprValue::Float(f) => {
                let expected = ((1.0 + 4.0 + 9.0) / 3.0_f64).sqrt();
                assert!((f - expected).abs() < 1e-10, "got {f}");
            }
            other => panic!("expected Float, got {other:?}"),
        }
    }

    #[test]
    fn any_all() {
        let mut any = Accumulator::new(AggregateFunc::Any);
        let mut all = Accumulator::new(AggregateFunc::All);
        any.accumulate(&ExprValue::Bool(false));
        any.accumulate(&ExprValue::Bool(true));
        all.accumulate(&ExprValue::Bool(true));
        all.accumulate(&ExprValue::Bool(false));
        assert_eq!(any.finish(), ExprValue::Bool(true));
        assert_eq!(all.finish(), ExprValue::Bool(false));
    }

    #[test]
    fn ntrue_nfalse() {
        let mut nt = Accumulator::new(AggregateFunc::NTrue);
        let mut nf = Accumulator::new(AggregateFunc::NFalse);
        for b in [true, false, true, true, false] {
            nt.accumulate(&ExprValue::Bool(b));
            nf.accumulate(&ExprValue::Bool(b));
        }
        assert_eq!(nt.finish(), ExprValue::Int(3));
        assert_eq!(nf.finish(), ExprValue::Int(2));
    }

    #[test]
    fn median_basic() {
        let mut acc = Accumulator::new(AggregateFunc::Median);
        for v in [3.0, 1.0, 4.0, 1.0, 5.0] {
            acc.accumulate(&ExprValue::Float(v));
        }
        assert_eq!(acc.finish(), ExprValue::Float(3.0));
    }

    #[test]
    fn fractile_basic() {
        let mut acc = Accumulator::new_fractile(0.5);
        for v in [1.0, 2.0, 3.0, 4.0, 5.0] {
            acc.accumulate(&ExprValue::Float(v));
        }
        assert_eq!(acc.finish(), ExprValue::Float(3.0));
    }
}

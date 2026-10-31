// SPDX-License-Identifier: LGPL-3.0-or-later

//! #991 experiment: sum identical linear spread contributions before stamping.
//!
//! Records already hold quantized locations and complete CF routing. Equal
//! records multiply the same taps; sum their pre-weighted complex values and
//! weights in f64, then stamp once. Prediction/native-residual records are
//! excluded. The normal sumwt calculation precedes this reduction unchanged.

use std::collections::HashMap;

use num_complex::{Complex32, Complex64};

use super::SampleRecord;

#[derive(Clone, Copy, PartialEq, Eq, Hash)]
struct Contribution {
    origin: [u32; 2],
    fine: [u16; 2],
    plane: u32,
    model_plane: u32,
    table: u32,
    spectral: u32,
    flags: u32,
    gradient: [u32; 2],
}

impl From<SampleRecord> for Contribution {
    fn from(record: SampleRecord) -> Self {
        Self {
            origin: record.origin,
            fine: record.fine,
            plane: record.plane,
            model_plane: record.model_plane,
            table: record.table,
            spectral: record.spectral.to_bits(),
            flags: record.flags,
            gradient: record.gradient.map(f32::to_bits),
        }
    }
}

#[derive(Default)]
struct Sum {
    values: [Complex64; 4],
    weights: [f64; 4],
}

pub(super) struct Grouping {
    index: HashMap<Contribution, usize>,
    sums: Vec<Sum>,
}

impl Grouping {
    pub(super) fn new() -> Self {
        Self {
            index: HashMap::with_capacity(crate::MAX_SUB),
            sums: Vec::with_capacity(crate::MAX_SUB),
        }
    }

    pub(super) fn reduce(
        &mut self,
        records: &mut [SampleRecord],
        values: &mut [Complex32],
        weights: &mut [f32],
        npol: usize,
    ) -> usize {
        assert!((1..=4).contains(&npol) && records.len() <= crate::MAX_SUB);
        self.index.clear();
        self.sums.clear();
        let mut last = None;
        for read in 0..records.len() {
            let record = records[read];
            let key = Contribution::from(record);
            let group = match last {
                Some((previous, group)) if previous == key => group,
                _ => {
                    let group = *self.index.entry(key).or_insert_with(|| {
                        let group = self.sums.len();
                        self.sums.push(Sum::default());
                        // group <= read, so compacting records never overwrites
                        // a record not yet read. Values wait until after reading.
                        records[group] = record;
                        group
                    });
                    last = Some((key, group));
                    group
                }
            };
            let sum = &mut self.sums[group];
            for pol in 0..npol {
                let weight = weights[read * npol + pol];
                // Both spread implementations ignore a masked value even if
                // it is nonzero or NaN. Preserve that before combining groups.
                if weight == 0.0 {
                    continue;
                }
                let value = values[read * npol + pol];
                sum.values[pol] += Complex64::new(f64::from(value.re), f64::from(value.im));
                sum.weights[pol] += f64::from(weight);
            }
        }
        for (group, sum) in self.sums.iter().enumerate() {
            for pol in 0..npol {
                values[group * npol + pol] =
                    Complex32::new(sum.values[pol].re as f32, sum.values[pol].im as f32);
                weights[group * npol + pol] = sum.weights[pol] as f32;
            }
        }
        self.sums.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn identical_stamps_preserve_small_complex_terms_and_weight_sums() {
        let mut records = vec![SampleRecord::default(); 3];
        let mut values = vec![
            Complex32::new(1e8, -1e8),
            Complex32::new(1.0, -1.0),
            Complex32::new(-1e8, 1e8),
        ];
        let mut weights = vec![1.0; 3];
        let count = Grouping::new().reduce(&mut records, &mut values, &mut weights, 1);
        assert_eq!(count, 1);
        assert_eq!(values[0], Complex32::new(1.0, -1.0));
        assert_eq!(weights[0], 3.0);
    }

    #[test]
    fn every_input_read_by_the_spread_kernel_keeps_its_contribution_separate() {
        let base = SampleRecord::default();
        let mut records = vec![base; 10];
        records[1].origin[0] = 1;
        records[2].origin[1] = 1;
        records[3].fine[0] = 1;
        records[4].fine[1] = 1;
        records[5].plane = 1;
        records[6].table = 1;
        records[7].spectral = 0.1;
        records[8].flags = 1;
        records[9].gradient[1] = 0.2;
        let mut values = vec![Complex32::new(1.0, 0.5); 20];
        let mut weights = vec![1.0; 20];
        let count = Grouping::new().reduce(&mut records, &mut values, &mut weights, 2);
        assert_eq!(count, 10);
    }

    #[test]
    fn masked_nan_or_nonzero_values_do_not_contaminate_a_live_group() {
        let mut records = vec![SampleRecord::default(); 3];
        let mut values = vec![
            Complex32::new(f32::NAN, f32::NAN),
            Complex32::new(99.0, -99.0),
            Complex32::new(2.0, -3.0),
        ];
        let mut weights = vec![0.0, 0.0, 1.0];
        let count = Grouping::new().reduce(&mut records, &mut values, &mut weights, 1);
        assert_eq!(count, 1);
        assert_eq!(values[0], Complex32::new(2.0, -3.0));
        assert_eq!(weights[0], 1.0);
    }
}

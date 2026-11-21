// SPDX-License-Identifier: LGPL-3.0-or-later

use ndarray::{ArrayD, Axis, IxDyn};

use crate::{Lattice, LatticeError, LatticeMut, TempLattice, TempStoragePolicy};

use super::{RunningStats, Statistic, order::OrderStatsSummary};

const BASIC: [Statistic; 9] = [
    Statistic::Npts,
    Statistic::Sum,
    Statistic::SumSq,
    Statistic::Min,
    Statistic::Max,
    Statistic::Mean,
    Statistic::Rms,
    Statistic::Variance,
    Statistic::Sigma,
];

const ORDER: [Statistic; 5] = [
    Statistic::Median,
    Statistic::Q1,
    Statistic::Q3,
    Statistic::Quartile,
    Statistic::MedAbsDevMed,
];

enum Columns {
    Memory(Vec<ArrayD<f64>>),
    Paged(Box<TempLattice<f64>>),
}

impl Columns {
    fn build<const N: usize>(
        shape: &[usize],
        policy: &TempStoragePolicy,
        statistics: [Statistic; N],
        load: impl Fn(Statistic) -> ArrayD<f64>,
    ) -> Result<Self, LatticeError> {
        let rank = shape.len();
        let mut lattice_shape = shape.to_vec();
        lattice_shape.push(N);
        if shape.contains(&0) || !policy.plan::<f64>(&lattice_shape)?.is_paged() {
            return Ok(Self::Memory(statistics.into_iter().map(load).collect()));
        }
        // Statistic columns are written and read in order. Keep their
        // temporary tile cache from retaining the whole result in memory.
        let mut lattice = TempLattice::new_with_cache_tiles(lattice_shape, policy.clone(), 2)?;
        let mut start = vec![0; rank + 1];
        for (column, statistic) in statistics.into_iter().enumerate() {
            start[rank] = column;
            lattice.put_slice(&load(statistic).insert_axis(Axis(rank)), &start)?;
        }
        Ok(Self::Paged(Box::new(lattice)))
    }

    fn get(&self, column: usize) -> Result<ArrayD<f64>, LatticeError> {
        match self {
            Self::Memory(arrays) => Ok(arrays[column].clone()),
            Self::Paged(lattice) => {
                let mut shape = lattice.shape().to_vec();
                let rank = shape.len() - 1;
                shape[rank] = 1;
                let mut start = vec![0; rank + 1];
                start[rank] = column;
                let values = lattice.get_slice(&start, &shape, &vec![1; rank + 1])?;
                Ok(values.index_axis_move(Axis(rank), 0))
            }
        }
    }
}

pub(super) struct BasicStatsCache {
    data: Columns,
    pub(super) global_min_pos: Option<Vec<usize>>,
    pub(super) global_max_pos: Option<Vec<usize>>,
}

impl BasicStatsCache {
    pub(super) fn from_accumulators(
        shape: &[usize],
        accumulators: &[RunningStats],
        global_min_pos: Option<Vec<usize>>,
        global_max_pos: Option<Vec<usize>>,
        policy: &TempStoragePolicy,
    ) -> Result<Self, LatticeError> {
        let data = Columns::build(shape, policy, BASIC, |statistic| {
            ArrayD::from_shape_vec(
                IxDyn(shape),
                accumulators
                    .iter()
                    .map(|row| row.statistic(statistic).unwrap_or(f64::NAN))
                    .collect(),
            )
            .expect("statistics shape matches accumulator count")
        })?;
        Ok(Self {
            data,
            global_min_pos,
            global_max_pos,
        })
    }

    pub(super) fn get(&self, statistic: Statistic) -> Result<ArrayD<f64>, LatticeError> {
        let column = BASIC
            .iter()
            .position(|&field| field == statistic)
            .expect("basic statistic");
        self.data.get(column)
    }
}

pub(super) struct OrderStatsCache(Columns);

impl OrderStatsCache {
    pub(super) fn from_summaries(
        shape: &[usize],
        summaries: &[OrderStatsSummary],
        policy: &TempStoragePolicy,
    ) -> Result<Self, LatticeError> {
        Columns::build(shape, policy, ORDER, |statistic| {
            ArrayD::from_shape_vec(
                IxDyn(shape),
                summaries
                    .iter()
                    .map(|row| match statistic {
                        Statistic::Median => row.median,
                        Statistic::Q1 => row.q1,
                        Statistic::Q3 => row.q3,
                        Statistic::Quartile => row.quartile,
                        Statistic::MedAbsDevMed => row.mad,
                        _ => unreachable!("order statistic"),
                    })
                    .collect(),
            )
            .expect("statistics shape matches summary count")
        })
        .map(Self)
    }

    pub(super) fn get(&self, statistic: Statistic) -> Result<ArrayD<f64>, LatticeError> {
        let column = ORDER
            .iter()
            .position(|&field| field == statistic)
            .expect("order statistic");
        self.0.get(column)
    }
}

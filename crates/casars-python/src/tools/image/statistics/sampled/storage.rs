// SPDX-License-Identifier: LGPL-3.0-or-later

use casa_lattices::{
    Lattice, LatticeError, LatticeMut, ScratchSpace, TempLattice, TempStoragePolicy,
};
use ndarray::{ArrayD, IxDyn, ShapeBuilder};

use super::COLS;

const BLOCK_ROWS: usize = 512;

pub(super) enum Rows {
    Memory(Vec<[f64; COLS]>),
    Paged {
        table: Box<TempLattice<f64>>,
        block: Vec<f64>,
        written: usize,
    },
}

impl Rows {
    pub(super) fn new(rows: usize, disk: bool) -> Result<Self, LatticeError> {
        if !disk {
            return Ok(Self::Memory(Vec::with_capacity(rows)));
        }
        let table = TempLattice::new_with_cache_tiles(
            vec![COLS, rows],
            TempStoragePolicy::Paged {
                scratch: ScratchSpace::SystemTemp,
            },
            2,
        )?;
        Ok(Self::Paged {
            table: Box::new(table),
            block: Vec::with_capacity(COLS * BLOCK_ROWS),
            written: 0,
        })
    }

    pub(super) fn push(&mut self, row: [f64; COLS]) -> Result<(), LatticeError> {
        match self {
            Self::Memory(rows) => rows.push(row),
            Self::Paged { block, .. } => {
                block.extend(row);
                if block.len() >= COLS * BLOCK_ROWS {
                    self.flush()?;
                }
            }
        }
        Ok(())
    }

    pub(super) fn flush(&mut self) -> Result<(), LatticeError> {
        if let Self::Paged {
            table,
            block,
            written,
        } = self
            && !block.is_empty()
        {
            let rows = block.len() / COLS;
            let values = std::mem::replace(block, Vec::with_capacity(COLS * BLOCK_ROWS));
            let values = ArrayD::from_shape_vec(IxDyn(&[COLS, rows]).f(), values)
                .expect("complete statistics rows");
            table.put_slice(&values, &[0, *written])?;
            *written += rows;
        }
        Ok(())
    }

    pub(super) fn column(&self, column: usize) -> Result<Vec<f64>, LatticeError> {
        match self {
            Self::Memory(rows) => Ok(rows.iter().map(|row| row[column]).collect()),
            Self::Paged { table, written, .. } => {
                let values = table.get_slice(&[column, 0], &[1, *written], &[1, 1])?;
                Ok(values.into_iter().collect())
            }
        }
    }
}

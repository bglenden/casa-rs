// SPDX-License-Identifier: LGPL-3.0-or-later
//! Canonical traversal extension.

use crate::{Lattice, LatticeElement, TraversalIter, TraversalSpec};

/// Adds [`TraversalSpec`]-driven traversal to every lattice.
pub trait LatticeIterExt<T: LatticeElement>: Lattice<T> {
    fn traverse(&self, spec: TraversalSpec) -> TraversalIter<'_, T, Self>
    where
        Self: Sized,
    {
        TraversalIter::new(self, spec)
    }
}

impl<T: LatticeElement, L: Lattice<T>> LatticeIterExt<T> for L {}

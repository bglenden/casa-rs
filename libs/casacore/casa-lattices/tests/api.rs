// SPDX-License-Identifier: LGPL-3.0-or-later

use casa_lattices::{ArrayLattice, Lattice, LatticeMut};
use ndarray::array;

#[test]
fn rectangular_writes_and_strided_reads_use_lattice_coordinates() {
    let mut lattice = ArrayLattice::<f32>::zeros(vec![4, 4]);
    lattice
        .put_slice(&array![[1.0, 2.0], [3.0, 4.0]].into_dyn(), &[1, 1])
        .unwrap();

    assert_eq!(lattice.get_at(&[2, 1]).unwrap(), 3.0);
    assert_eq!(
        lattice.get_slice(&[0, 0], &[2, 2], &[2, 2]).unwrap(),
        array![[0.0, 0.0], [0.0, 4.0]].into_dyn(),
    );
    assert!(lattice.get_at(&[4, 0]).is_err());
}

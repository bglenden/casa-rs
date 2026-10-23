// SPDX-License-Identifier: LGPL-3.0-or-later
//! `ComponentList::rewrite_table` replaces a list in place, as CASA's
//! `writeTable` rewrites a list the `cl` tool has open for update
//! (simulation Phase 2, #841): the table then holds the new list, and no
//! staging or replaced table is left beside it.

use casa_components::{ComponentList, ComponentShape, Flux, SkyComponent, SpectralModel};
use casa_types::measures::direction::{DirectionRef, MDirection};
use casa_types::measures::frequency::{FrequencyRef, MFrequency};

fn list(fluxes: &[f64]) -> ComponentList {
    let mut list = ComponentList::new();
    for &flux in fluxes {
        list.push(SkyComponent::new(
            Flux::stokes_jy(flux, 0.0, 0.0, 0.0),
            ComponentShape::point(MDirection::from_angles(1.0, 0.5, DirectionRef::J2000)),
            SpectralModel::constant(MFrequency::new(1.4e9, FrequencyRef::LSRK)),
        ));
    }
    list
}

fn fluxes(list: &ComponentList) -> Vec<f64> {
    list.iter()
        .map(|component| component.flux().values()[0].re)
        .collect()
}

#[test]
fn a_rewrite_replaces_the_list_and_leaves_nothing_beside_it() {
    let scratch = tempfile::tempdir().expect("scratch");
    let path = scratch.path().join("list.cl");
    list(&[1.0]).write_table(&path).expect("write");
    list(&[2.0, 3.0]).rewrite_table(&path).expect("rewrite");
    assert_eq!(
        fluxes(&ComponentList::read_table(&path).expect("read")),
        [2.0, 3.0]
    );
    let entries: Vec<_> = std::fs::read_dir(scratch.path())
        .expect("scratch entries")
        .map(|entry| entry.expect("entry").file_name())
        .collect();
    assert_eq!(entries, ["list.cl"]);
    // A rewrite of a table not yet written creates it.
    let fresh = scratch.path().join("fresh.cl");
    list(&[4.0])
        .rewrite_table(&fresh)
        .expect("rewrite a new table");
    assert_eq!(
        fluxes(&ComponentList::read_table(&fresh).expect("read")),
        [4.0]
    );
}

// SPDX-License-Identifier: LGPL-3.0-or-later
//! [`optimum_image_size`] against CASA 6.7.6's `getOptimumSize` for every
//! size from 0 to 4096 (`tests/data/optimum_sizes_casa.json`, recorded by
//! `tools/imaging/optimum_size_oracle.py`).

use casa_imaging_model::optimum_image_size;

#[test]
fn optimum_sizes_are_casas() {
    let record = include_str!("data/optimum_sizes_casa.json");
    let list = record
        .split_once("\"optimum\":[")
        .and_then(|(_, rest)| rest.split_once(']'))
        .map(|(list, _)| list)
        .expect("the record's optimum sizes");
    let casa: Vec<usize> = list
        .split(',')
        .map(|size| size.parse().expect("a size"))
        .collect();
    assert_eq!(casa.len(), 4097);
    for (pixels, &expected) in casa.iter().enumerate() {
        assert_eq!(optimum_image_size(pixels), expected, "{pixels} pixels");
    }
}

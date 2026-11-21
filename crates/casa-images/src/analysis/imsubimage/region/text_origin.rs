// SPDX-License-Identifier: LGPL-3.0-or-later

use std::path::Path;

use casa_coordinates::CoordinateSystem;

use super::{SpatialShape, shape_candidate_bounds, shape_contains};

#[derive(Clone, Copy)]
pub(in crate::analysis::imsubimage) struct TextRegionContext<'a> {
    pub(in crate::analysis::imsubimage) coordinates: Option<&'a CoordinateSystem>,
    pub(in crate::analysis::imsubimage) table: Option<&'a Path>,
    pub(in crate::analysis::imsubimage) shape: Option<&'a [usize]>,
}

// CASA discards CRTF entries whose source lattice has no selected pixel
// before converting the remaining world regions onto the copied image.
pub(super) fn overlaps_image(shape: &SpatialShape, lattice: &[usize], plane: [usize; 2]) -> bool {
    let [Some(&nx), Some(&ny)] = plane.map(|axis| lattice.get(axis)) else {
        return false;
    };
    let (x0, y0, x1, y1) = shape_candidate_bounds(shape, nx, ny);
    (x0..=x1).any(|x| (y0..=y1).any(|y| shape_contains(shape, x, y)))
}

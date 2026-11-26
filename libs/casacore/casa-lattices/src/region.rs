// SPDX-License-Identifier: LGPL-3.0-or-later
//! Lattice region types for spatial selection.

use ndarray::ArrayD;

/// A region within a lattice defining a subset of pixels.
///
/// Corresponds to the C++ `LCRegion` abstract base class. Regions describe
/// which pixels of a lattice are "inside" and which are "outside" via a
/// boolean mask. The region is bounded by a rectangular bounding box.
///
/// Concrete implementations include [`LCBox`](crate::LCBox),
/// [`LCEllipsoid`](crate::LCEllipsoid), and set-algebra combinators.
pub trait LCRegion: std::fmt::Debug + Send + Sync {
    /// Returns the shape of the parent lattice.
    fn lattice_shape(&self) -> Vec<usize>;

    /// Returns the start position of the bounding box.
    fn bounding_box_start(&self) -> Vec<usize>;

    /// Returns the shape of the bounding box.
    fn bounding_box_shape(&self) -> Vec<usize>;

    /// Returns the boolean mask within the bounding box.
    ///
    /// The returned array has the same shape as [`bounding_box_shape`](Self::bounding_box_shape).
    /// `true` means the pixel is inside the region.
    fn get_mask(&self) -> ArrayD<bool>;

    /// Tests whether a single position is inside the region.
    fn contains(&self, position: &[usize]) -> bool;
}

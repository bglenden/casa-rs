// SPDX-License-Identifier: LGPL-3.0-or-later
//! Lattice shape with optional tiling specification.

use crate::LatticeError;

/// A lattice shape paired with an optional tile shape.
///
/// Corresponds to the C++ `TiledShape` class, which bundles the overall
/// lattice shape with a tile (chunk) shape for storage managers like
/// `TiledCellStMan` and `TiledShapeStMan`.
///
/// If no tile shape is provided, [`TiledShape::tile_shape`] returns a
/// default computed by [`TiledShape::default_tile_shape`], which targets
/// tiles of roughly 32 KiB — the same heuristic used by C++ casacore.
///
/// # Examples
///
/// ```rust
/// use casacore_lattices::TiledShape;
///
/// // Default tiling for a 256×256×64 cube:
/// let ts = TiledShape::new(vec![256, 256, 64]);
/// assert_eq!(ts.shape(), &[256, 256, 64]);
/// assert_eq!(ts.tile_shape().len(), 3);
///
/// // Explicit tile shape:
/// let ts = TiledShape::with_tile_shape(vec![100, 100], vec![32, 32]).unwrap();
/// assert_eq!(ts.tile_shape(), &[32, 32]);
/// ```
#[derive(Debug, Clone)]
pub struct TiledShape {
    shape: Vec<usize>,
    tile_shape: Option<Vec<usize>>,
}

impl TiledShape {
    /// Creates a `TiledShape` with default tiling.
    ///
    /// The tile shape is computed lazily by [`tile_shape`](Self::tile_shape)
    /// using [`default_tile_shape`](Self::default_tile_shape).
    pub fn new(shape: Vec<usize>) -> Self {
        Self {
            shape,
            tile_shape: None,
        }
    }

    /// Creates a `TiledShape` with an explicit tile shape.
    ///
    /// Returns an error if the tile shape has a different number of
    /// dimensions than the lattice shape, or if any tile dimension exceeds
    /// the corresponding lattice dimension.
    pub fn with_tile_shape(
        shape: Vec<usize>,
        tile_shape: Vec<usize>,
    ) -> Result<Self, LatticeError> {
        if shape.len() != tile_shape.len() {
            return Err(LatticeError::NdimMismatch {
                expected: shape.len(),
                got: tile_shape.len(),
            });
        }
        for (axis, (&ts, &ls)) in tile_shape.iter().zip(shape.iter()).enumerate() {
            if ts > ls {
                return Err(LatticeError::TileMismatch {
                    tile_shape: tile_shape.clone(),
                    lattice_shape: shape.clone(),
                    axis,
                });
            }
        }
        Ok(Self {
            shape,
            tile_shape: Some(tile_shape),
        })
    }

    /// Returns the lattice shape.
    pub fn shape(&self) -> &[usize] {
        &self.shape
    }

    /// Returns the tile shape, computing a default if none was specified.
    ///
    /// The default targets tiles of roughly 32 KiB (4096 elements of 8 bytes
    /// each), following the C++ casacore `TiledStMan` heuristic.
    pub fn tile_shape(&self) -> Vec<usize> {
        match &self.tile_shape {
            Some(ts) => ts.clone(),
            None => Self::default_tile_shape(&self.shape),
        }
    }

    /// Computes a default tile shape for the given lattice shape.
    ///
    /// The heuristic targets tiles of roughly 4096 elements (32 KiB at 8
    /// bytes/element). Each axis is clamped to the lattice extent, and the
    /// product is kept near the target. This mirrors the C++ casacore
    /// `TiledStMan::makeTileShape` logic.
    pub fn default_tile_shape(shape: &[usize]) -> Vec<usize> {
        if shape.is_empty() {
            return vec![];
        }

        let target_elements: usize = 4096; // ~32 KiB at 8 bytes/element
        let ndim = shape.len();

        // Start with cube root (or nth root) of target, clamped to shape.
        let root = (target_elements as f64).powf(1.0 / ndim as f64).ceil() as usize;
        let mut tile: Vec<usize> = shape.iter().map(|&s| root.min(s).max(1)).collect();

        // Adjust: if product is too small, grow the first axis that has room.
        let product = |t: &[usize]| -> usize { t.iter().product() };
        for axis in 0..ndim {
            if product(&tile) >= target_elements {
                break;
            }
            tile[axis] = shape[axis].min(target_elements / (product(&tile) / tile[axis]));
        }

        // Clamp if product greatly exceeds target (can happen with many axes).
        while product(&tile) > target_elements * 4 && tile.iter().any(|&t| t > 1) {
            // Shrink the largest tile axis.
            if let Some(axis) = tile
                .iter()
                .enumerate()
                .filter(|&(_, t)| *t > 1)
                .max_by_key(|&(_, t)| *t)
                .map(|(i, _)| i)
            {
                tile[axis] = (tile[axis] / 2).max(1);
            }
        }

        tile
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_tile_shape_3d() {
        let tile = TiledShape::default_tile_shape(&[256, 256, 64]);
        assert_eq!(tile.len(), 3);
        let product: usize = tile.iter().product();
        // Should be in a reasonable range around 4096.
        assert!((512..=32768).contains(&product), "product = {product}");
        // Each tile axis should not exceed the lattice axis.
        for (i, &t) in tile.iter().enumerate() {
            assert!(t <= [256, 256, 64][i]);
        }
    }

    #[test]
    fn default_tile_shape_small() {
        let tile = TiledShape::default_tile_shape(&[4, 4]);
        assert_eq!(tile, vec![4, 4]);
    }

    #[test]
    fn default_tile_shape_empty() {
        assert!(TiledShape::default_tile_shape(&[]).is_empty());
    }

    #[test]
    fn explicit_tile_shape_ok() {
        let ts = TiledShape::with_tile_shape(vec![100, 100], vec![32, 32]).unwrap();
        assert_eq!(ts.tile_shape(), vec![32, 32]);
    }

    #[test]
    fn explicit_tile_shape_exceeds() {
        let err = TiledShape::with_tile_shape(vec![10, 10], vec![32, 32]).unwrap_err();
        assert!(matches!(err, LatticeError::TileMismatch { axis: 0, .. }));
    }

    #[test]
    fn explicit_tile_ndim_mismatch() {
        let err = TiledShape::with_tile_shape(vec![10, 10], vec![5]).unwrap_err();
        assert!(matches!(err, LatticeError::NdimMismatch { .. }));
    }

    #[test]
    fn new_uses_default_tiling() {
        let ts = TiledShape::new(vec![64, 64]);
        let tile = ts.tile_shape();
        assert_eq!(tile.len(), 2);
        assert!(tile[0] <= 64);
        assert!(tile[1] <= 64);
    }
}

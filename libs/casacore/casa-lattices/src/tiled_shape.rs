// SPDX-License-Identifier: LGPL-3.0-or-later
//! Lattice shape with a checked byte-aware physical tile layout.

use casa_tables::TileLayoutPlanner;

use crate::LatticeError;

/// A lattice shape paired with its persisted physical tile shape.
///
/// Automatic layouts use the shared byte-aware storage planner. Axis zero is
/// storage-major, matching casacore. For ordinary spectral cubes this fills
/// the spatial axes before spectral axes, producing plane-oriented tiles whose
/// non-plane extents are one. Existing files retain their recorded tile shape.
#[derive(Debug, Clone)]
pub struct TiledShape {
    shape: Vec<usize>,
    tile_shape: Vec<usize>,
}

impl TiledShape {
    fn validate_lattice_shape(shape: &[usize]) -> Result<(), LatticeError> {
        if let Some(axis) = shape.iter().position(|&extent| extent == 0) {
            return Err(LatticeError::TileLayout(format!(
                "lattice extent on axis {axis} must be positive"
            )));
        }
        Ok(())
    }

    /// Plans a default physical tile shape for the actual element byte size.
    pub fn new(shape: Vec<usize>, element_bytes: usize) -> Result<Self, LatticeError> {
        Self::validate_lattice_shape(&shape)?;
        let plan = TileLayoutPlanner::repository_default()
            .plan_array(&shape, element_bytes)
            .map_err(|error| LatticeError::TileLayout(error.to_string()))?;
        Ok(Self {
            shape,
            tile_shape: plan.tile_shape().to_vec(),
        })
    }

    /// Creates a shape with a strictly validated explicit physical tile.
    pub fn with_tile_shape(
        shape: Vec<usize>,
        tile_shape: Vec<usize>,
    ) -> Result<Self, LatticeError> {
        Self::validate_lattice_shape(&shape)?;
        let plan = TileLayoutPlanner::repository_default()
            // The byte size does not affect an explicit plan; one avoids
            // inventing a type-independent automatic layout.
            .plan_explicit_array(&shape, 1, &tile_shape)
            .map_err(|error| LatticeError::TileLayout(error.to_string()))?;
        Ok(Self {
            shape,
            tile_shape: plan.tile_shape().to_vec(),
        })
    }

    pub fn shape(&self) -> &[usize] {
        &self.shape
    }

    pub fn tile_shape(&self) -> &[usize] {
        &self.tile_shape
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn automatic_layout_uses_real_element_bytes() {
        let f32_shape = TiledShape::new(vec![4096, 4096, 768], 4).unwrap();
        let f64_shape = TiledShape::new(vec![4096, 4096, 768], 8).unwrap();
        assert_eq!(f32_shape.tile_shape(), &[4096, 256, 1]);
        assert_eq!(f64_shape.tile_shape(), &[4096, 128, 1]);
    }

    #[test]
    fn small_shape_uses_full_cell() {
        let tiled = TiledShape::new(vec![4, 4], 8).unwrap();
        assert_eq!(tiled.tile_shape(), &[4, 4]);
    }

    #[test]
    fn invalid_automatic_shapes_fail() {
        assert!(TiledShape::new(vec![], 4).is_err());
        assert!(TiledShape::new(vec![4, 0], 4).is_err());
        assert!(TiledShape::new(vec![4], 0).is_err());
    }

    #[test]
    fn explicit_shape_is_strict() {
        let tiled = TiledShape::with_tile_shape(vec![100, 100], vec![32, 32]).unwrap();
        assert_eq!(tiled.tile_shape(), &[32, 32]);
        let padded = TiledShape::with_tile_shape(vec![10, 10], vec![32, 32]).unwrap();
        assert_eq!(padded.tile_shape(), &[32, 32]);
        assert!(TiledShape::with_tile_shape(vec![10, 10], vec![10, 0]).is_err());
        assert!(TiledShape::with_tile_shape(vec![10, 10], vec![10]).is_err());
    }
}

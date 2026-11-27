// SPDX-License-Identifier: LGPL-3.0-or-later
//! Checked byte-aware physical tile layout planning.

/// Default physical I/O target for newly created tiled arrays and columns.
///
/// Four MiB is large enough to sustain sequential local-disk throughput while
/// keeping a small fixed number of tiles comfortably bounded in memory. The
/// value controls new-file layout only; readers always honor the tile shape
/// persisted in an existing casacore table.
pub const DEFAULT_TILE_IO_BYTES: usize = 4 * 1024 * 1024;

/// Errors produced while planning a physical tile layout.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum TileLayoutError {
    #[error("tile layout requires at least one logical cell axis")]
    /// The logical array has no cell axes.
    EmptyShape,
    #[error("tile layout element size must be positive")]
    /// A physical element was declared to occupy zero bytes.
    ZeroElementBytes,
    #[error("tile layout I/O target must be positive")]
    /// The requested I/O budget was zero.
    ZeroTargetBytes,
    #[error("storage axis order {order:?} is not a permutation of 0..{rank}")]
    /// The supplied storage-axis permutation is invalid.
    InvalidAxisOrder {
        /// Indices in requested storage order.
        order: Vec<usize>,
        /// Number of logical cell axes.
        rank: usize,
    },
    #[error("explicit tile rank mismatch: expected {expected}, got {got}")]
    /// The explicit tile shape has the wrong rank.
    ExplicitRank {
        /// Required tile rank including the row axis, if present.
        expected: usize,
        /// Number of supplied tile extents.
        got: usize,
    },
    #[error("explicit tile extent on axis {axis} must be positive")]
    /// An explicit tile extent was zero.
    ZeroTileExtent {
        /// Zero-based storage axis of the zero extent.
        axis: usize,
    },
    #[error("tile layout size overflow while computing {what}")]
    /// A tile size calculation overflowed the addressable integer range.
    Overflow {
        /// Name of the intermediate quantity that overflowed.
        what: &'static str,
    },
}

/// Deterministic physical layout chosen for one tiled cell or array.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TileLayoutPlan {
    cell_tile_shape: Vec<usize>,
    row_extent: Option<usize>,
    tile_shape: Vec<usize>,
    tile_elements: usize,
    tile_bytes: usize,
    target_io_bytes: usize,
}

impl TileLayoutPlan {
    /// Returns the physical extents of the cell dimensions, excluding the row axis.
    pub fn cell_tile_shape(&self) -> &[usize] {
        &self.cell_tile_shape
    }

    /// Returns the number of rows spanned by a tile, when planning a column.
    pub fn row_extent(&self) -> Option<usize> {
        self.row_extent
    }

    /// Returns physical tile extents in storage order with the row axis last, when present.
    pub fn tile_shape(&self) -> &[usize] {
        &self.tile_shape
    }

    /// Returns the number of element slots in one physical tile.
    pub fn tile_elements(&self) -> usize {
        self.tile_elements
    }

    /// Returns the occupied bytes for a complete physical tile.
    pub fn tile_bytes(&self) -> usize {
        self.tile_bytes
    }

    /// Returns the target I/O size in bytes used to plan the layout.
    pub fn target_io_bytes(&self) -> usize {
        self.target_io_bytes
    }
}

/// Pure byte-aware planner shared by tiled tables, lattices, and images.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TileLayoutPlanner {
    target_io_bytes: usize,
}

impl TileLayoutPlanner {
    /// Creates a planner with a positive target size in bytes. A zero target is an error.
    pub fn new(target_io_bytes: usize) -> Result<Self, TileLayoutError> {
        if target_io_bytes == 0 {
            return Err(TileLayoutError::ZeroTargetBytes);
        }
        Ok(Self { target_io_bytes })
    }

    /// Creates a planner using the same byte budget as new repository-managed tiled data.
    pub const fn repository_default() -> Self {
        Self {
            target_io_bytes: DEFAULT_TILE_IO_BYTES,
        }
    }

    /// Returns the planner's I/O target in bytes.
    pub const fn target_io_bytes(self) -> usize {
        self.target_io_bytes
    }

    /// Plans a standalone array in logical axis order for the supplied on-disk bytes per element.
    pub fn plan_array(
        self,
        logical_shape: &[usize],
        element_bytes: usize,
    ) -> Result<TileLayoutPlan, TileLayoutError> {
        let order: Vec<usize> = (0..logical_shape.len()).collect();
        self.plan(logical_shape, None, element_bytes, &order, None)
    }

    /// Validates an explicit array tile shape and computes its byte size. Rank and extent errors
    /// propagate.
    pub fn plan_explicit_array(
        self,
        logical_shape: &[usize],
        element_bytes: usize,
        tile_shape: &[usize],
    ) -> Result<TileLayoutPlan, TileLayoutError> {
        let order: Vec<usize> = (0..logical_shape.len()).collect();
        self.plan(logical_shape, None, element_bytes, &order, Some(tile_shape))
    }

    /// Plans a tiled column with an appended row axis, preserving a logical row count of zero for
    /// an empty table.
    pub fn plan_column(
        self,
        logical_cell_shape: &[usize],
        row_count: usize,
        element_bytes: usize,
    ) -> Result<TileLayoutPlan, TileLayoutError> {
        let order: Vec<usize> = (0..logical_cell_shape.len()).collect();
        self.plan(
            logical_cell_shape,
            Some(row_count),
            element_bytes,
            &order,
            None,
        )
    }

    /// Plans a layout in caller-supplied storage-axis order.
    ///
    /// A zero-row table uses physical row extent one because casacore tile
    /// shapes must be positive; its logical row count remains zero.
    pub fn plan(
        self,
        logical_cell_shape: &[usize],
        row_count: Option<usize>,
        element_bytes: usize,
        storage_axis_order: &[usize],
        explicit_tile_shape: Option<&[usize]>,
    ) -> Result<TileLayoutPlan, TileLayoutError> {
        validate_inputs(
            logical_cell_shape,
            element_bytes,
            self.target_io_bytes,
            storage_axis_order,
        )?;

        let expected_rank = logical_cell_shape.len() + usize::from(row_count.is_some());
        let (cell_tile_shape, row_extent) = if let Some(explicit) = explicit_tile_shape {
            if explicit.len() != expected_rank {
                return Err(TileLayoutError::ExplicitRank {
                    expected: expected_rank,
                    got: explicit.len(),
                });
            }
            for (axis, &extent) in explicit.iter().enumerate() {
                if extent == 0 {
                    return Err(TileLayoutError::ZeroTileExtent { axis });
                }
            }
            (
                explicit[..logical_cell_shape.len()].to_vec(),
                row_count.map(|_| explicit[logical_cell_shape.len()]),
            )
        } else {
            let target_elements = (self.target_io_bytes / element_bytes).max(1);
            let mut remaining = target_elements;
            let mut cell_tile_shape = vec![1usize; logical_cell_shape.len()];
            for &axis in storage_axis_order {
                let extent = logical_cell_shape[axis].max(1).min(remaining.max(1));
                cell_tile_shape[axis] = extent;
                remaining = remaining.div_ceil(extent);
            }
            let cell_elements = checked_product(&cell_tile_shape, "cell tile elements")?;
            let row_extent = row_count.map(|rows| {
                let capacity = (target_elements / cell_elements).max(1);
                rows.max(1).min(capacity)
            });
            (cell_tile_shape, row_extent)
        };

        let mut tile_shape = cell_tile_shape.clone();
        if let Some(rows) = row_extent {
            tile_shape.push(rows);
        }
        let tile_elements = checked_product(&tile_shape, "tile elements")?;
        let tile_bytes = tile_elements
            .checked_mul(element_bytes)
            .ok_or(TileLayoutError::Overflow { what: "tile bytes" })?;

        Ok(TileLayoutPlan {
            cell_tile_shape,
            row_extent,
            tile_shape,
            tile_elements,
            tile_bytes,
            target_io_bytes: self.target_io_bytes,
        })
    }
}

fn validate_inputs(
    logical_shape: &[usize],
    element_bytes: usize,
    target_io_bytes: usize,
    storage_axis_order: &[usize],
) -> Result<(), TileLayoutError> {
    if logical_shape.is_empty() {
        return Err(TileLayoutError::EmptyShape);
    }
    if element_bytes == 0 {
        return Err(TileLayoutError::ZeroElementBytes);
    }
    if target_io_bytes == 0 {
        return Err(TileLayoutError::ZeroTargetBytes);
    }
    let mut sorted = storage_axis_order.to_vec();
    sorted.sort_unstable();
    if sorted != (0..logical_shape.len()).collect::<Vec<_>>() {
        return Err(TileLayoutError::InvalidAxisOrder {
            order: storage_axis_order.to_vec(),
            rank: logical_shape.len(),
        });
    }
    checked_product(logical_shape, "logical cell elements")?;
    Ok(())
}

fn checked_product(values: &[usize], what: &'static str) -> Result<usize, TileLayoutError> {
    values.iter().try_fold(1usize, |product, &value| {
        product
            .checked_mul(value)
            .ok_or(TileLayoutError::Overflow { what })
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn plane_oriented_cube_plan_is_byte_bounded() {
        let plan = TileLayoutPlanner::repository_default()
            .plan_array(&[4096, 4096, 768], 4)
            .unwrap();
        assert_eq!(plan.tile_shape(), &[4096, 256, 1]);
        assert_eq!(plan.tile_bytes(), 4 * 1024 * 1024);
    }

    #[test]
    fn equal_byte_targets_respect_element_size() {
        let planner = TileLayoutPlanner::new(64).unwrap();
        assert_eq!(planner.plan_array(&[100], 4).unwrap().tile_shape(), &[16]);
        assert_eq!(planner.plan_array(&[100], 8).unwrap().tile_shape(), &[8]);
        assert_eq!(planner.plan_array(&[100], 16).unwrap().tile_shape(), &[4]);
    }

    #[test]
    fn column_row_extent_consumes_remaining_budget() {
        let plan = TileLayoutPlanner::new(1024)
            .unwrap()
            .plan_column(&[2, 8], 1000, 4)
            .unwrap();
        assert_eq!(plan.tile_shape(), &[2, 8, 16]);
        assert_eq!(plan.tile_bytes(), 1024);
    }

    #[test]
    fn zero_row_column_has_positive_physical_extent() {
        let plan = TileLayoutPlanner::new(64)
            .unwrap()
            .plan_column(&[4], 0, 4)
            .unwrap();
        assert_eq!(plan.row_extent(), Some(1));
        assert_eq!(plan.tile_shape(), &[4, 1]);
    }

    #[test]
    fn explicit_shape_is_strictly_validated() {
        let planner = TileLayoutPlanner::new(64).unwrap();
        assert!(matches!(
            planner.plan_explicit_array(&[4, 4], 4, &[4, 0]),
            Err(TileLayoutError::ZeroTileExtent { axis: 1 })
        ));
        let padded = planner.plan_explicit_array(&[4, 4], 4, &[5, 1]).unwrap();
        assert_eq!(padded.tile_shape(), &[5, 1]);
        assert!(matches!(
            planner.plan_explicit_array(&[4, 4], 4, &[4]),
            Err(TileLayoutError::ExplicitRank { .. })
        ));
    }

    #[test]
    fn invalid_inputs_and_overflow_fail_before_io() {
        let planner = TileLayoutPlanner::new(64).unwrap();
        assert_eq!(
            planner.plan_array(&[4, 0], 4).unwrap().tile_shape(),
            &[4, 1]
        );
        assert!(matches!(
            planner.plan(&[4, 4], None, 4, &[0, 0], None),
            Err(TileLayoutError::InvalidAxisOrder { .. })
        ));
        assert!(matches!(
            planner.plan_array(&[usize::MAX, 2], 4),
            Err(TileLayoutError::Overflow { .. })
        ));
    }
}

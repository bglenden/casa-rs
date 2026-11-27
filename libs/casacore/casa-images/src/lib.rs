// SPDX-License-Identifier: LGPL-3.0-or-later
#![doc = include_str!("../README.md")]
//!
//! <details>
//! <summary>API details</summary>
//!
//! casacore-compatible typed images and related metadata.
//!
//! # Overview
//!
//! This crate provides the Rust equivalents of the C++ casacore image classes:
//!
//! | Rust type                | C++ equivalent          | Notes                              |
//! |--------------------------|-------------------------|------------------------------------|
//! | [`PagedImage<T>`]        | `PagedImage<T>`         | Persistent, table-backed image     |
//! | [`TempImage<T>`]         | `TempImage<T>`          | In-memory or scratch-disk image    |
//! | [`ImageExprBuilder<T>`]         | `ImageExpr<T>`                 | Builds an owned compiled evaluator |
//! | [`SubImage`]/[`SubImageMut`] | `SubImage<T>`       | View-style image sections          |
//! | [`MaskExprBuilder<T>`]          | `LatticeExprNode`       | Lazy boolean expression            |
//! | [`ImageInfo`]            | `ImageInfo`             | Image metadata (beam, type, etc.)  |
//! | [`ImageBeamSet`]         | `ImageBeamSet`          | Beam descriptions                  |
//! | [`PolygonRegion`]        | `WCPolygon`, `WCUnion`  | Saved world-coordinate regions     |
//!
//! - [`PagedImage<T>`] stores pixel data in a casacore `Table` with
//!   `TiledCellStMan`. Suitable for persistent images on disk.
//! - [`TempImage<T>`] stores pixels in a [`TempLattice`](casa_lattices::TempLattice)
//!   and metadata in member fields — no `Table` involved. Small images stay
//!   in memory; larger ones spill to scratch disk. Convert to `PagedImage`
//!   via [`save_as()`](TempImage::save_as) or [`into_paged()`](TempImage::into_paged).
//! - [`ImageExprBuilder<T>`] compiles to an owned lazy evaluator; pixels are
//!   computed only for the requested point or region.
//! - [`SubImage`] and [`SubImageMut`] are view-style image sections.
//! - Current parity boundary: full `CoordinateSystem` persistence and
//!   cross-language reconstruction are still incomplete. Pixel data, units,
//!   masks, and [`ImageInfo`] interoperate more reliably today than
//!   coordinate metadata.
//!
//! Supported image pixel types: [`f32`], [`f64`],
//! [`Complex32`](casa_core::Complex32), and
//! [`Complex64`](casa_core::Complex64). The convenience alias [`Image`]
//! remains available for `PagedImage<f32>`.
//!
//! # Example
//!
//! ```rust
//! use casa_coordinates::CoordinateSystem;
//! use casa_images::{ImageExprBuilder, ImageExprBinaryOp, ImageExprUnaryOp, ImageType, TempImage};
//! use casa_lattices::{Lattice, LatticeIterExt, LatticeMut, TraversalSpec};
//! use casa_core::Complex32;
//!
//! let mut image = TempImage::<f32>::new(vec![8, 8], CoordinateSystem::new(), casa_lattices::TempStoragePolicy::Memory).unwrap();
//! image.set(1.0).unwrap();
//!
//! let expr = ImageExprBuilder::from_image(&image)
//!     .unwrap()
//!     .binary_scalar(2.0, ImageExprBinaryOp::Multiply)
//!     .unary(ImageExprUnaryOp::Negate)
//!     .compile()
//!     .unwrap();
//! assert_eq!(expr.get_at(&[0, 0]).unwrap(), -2.0);
//!
//! let complex = TempImage::<Complex32>::new(vec![4, 4], CoordinateSystem::new(), casa_lattices::TempStoragePolicy::Memory).unwrap();
//! let _chunks = complex.traverse(TraversalSpec::chunks(vec![2, 2])).count();
//! assert_eq!(ImageType::Velocity.to_string(), "Velocity");
//! ```
//!
//! </details>

mod beam;
mod concat_file;
mod concatenate;
mod error;
pub mod expr_file;
pub mod expr_parser;
pub mod fits_read;
pub mod fits_table;
mod image;
mod image_expr;
mod image_expr_lel;
mod image_expr_ops;
mod image_info;
mod miriad_read;
mod pixel_slice;
pub mod saved_region;
mod subimage;
mod temp_image;
mod virtual_concat;

#[cfg(test)]
mod mask;
mod mask_reference;
pub mod resample;

pub use beam::{
    BeamComponent, GaussianBeam, ImageBeamSet, StoredGaussianBeam, quantity_from_record,
    wrap_position_angle,
};
pub use casa_lattices::{
    ScratchSpace, StatisticsAlgorithmKind, TempStoragePlan, TempStoragePolicy,
};
pub use concat_file::{ConcatManifest, is_image_concat};
pub use concatenate::{ConcatInput, ConcatenationPlan, copy_concatenated_input};
pub use error::ImageError;
pub use fits_read::{FitsImage, fits_image_shape, read_fits_image};
pub use image::{
    AnyPagedImage, Image, ImageHistoryEntry, ImageHistoryRow, ImageInterface, ImagePixel,
    ImagePixelType, MaskSliceWriter, MutableImageInterface, PagedImage, StoredImageMasks,
    add_image_history, append_image_history_entries, image_history_entries, image_history_rows,
    image_pixel_type, make_paged_mask_record,
};
pub use image_expr::{
    ImageExprBinaryOp, ImageExprBuilder, ImageExprCompareOp, ImageExprMeta, ImageExprUnaryOp,
    ImageExprValue, ImageExpression, MaskExprBuilder, MaskExpression, MaskLogicalOp,
    MemorySnapshotImage, OwnedMaskReader, ReductionOp, bounded_read_cursor, validate_slice_request,
};
pub use image_expr_lel::{
    broadcast_array, broadcast_shapes, can_broadcast_to, mask_coordinate_axes,
    rebin_coordinate_record, rebin_reduce_slice,
};
pub use image_info::{ImageInfo, ImageType};
pub use mask_reference::{is_paged_mask_table, subtable_reference};
pub use miriad_read::MiriadImage;
pub use pixel_slice::{PixelSlice, get_slice};
pub use resample::RegridMethod;
pub use saved_region::{PolygonRegion, RegionStatistics};
pub use subimage::{SubImage, SubImageMut};
pub use temp_image::TempImage;
pub use virtual_concat::{ConcatImage, ConcatLockState, ConcatMember};

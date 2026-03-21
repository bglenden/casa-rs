// SPDX-License-Identifier: LGPL-3.0-or-later
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
//! | [`ImageExpr<T>`]         | `ImageExpr<T>`          | Lazy read-only expression image    |
//! | [`SubImage`]/[`SubImageMut`] | `SubImage<T>`       | View-style image sections          |
//! | [`MaskExpr<T>`]          | `LatticeExprNode`       | Lazy boolean expression            |
//! | [`ImageInfo`]            | `ImageInfo`             | Image metadata (beam, type, etc.)  |
//! | [`ImageBeamSet`]         | `ImageBeamSet`          | Beam descriptions                  |
//!
//! - [`PagedImage<T>`] stores pixel data in a casacore `Table` with
//!   `TiledCellStMan`. Suitable for persistent images on disk.
//! - [`TempImage<T>`] stores pixels in a [`TempLattice`](casacore_lattices::TempLattice)
//!   and metadata in member fields — no `Table` involved. Small images stay
//!   in memory; larger ones spill to scratch disk. Convert to `PagedImage`
//!   via [`save_as()`](TempImage::save_as) or [`into_paged()`](TempImage::into_paged).
//! - [`ImageExpr<T>`] evaluates lazily; pixels are computed only for the
//!   requested point or region.
//! - [`SubImage`] and [`SubImageMut`] are view-style image sections.
//! - Current parity boundary: full `CoordinateSystem` persistence and
//!   cross-language reconstruction are still incomplete. Pixel data, units,
//!   masks, and [`ImageInfo`] interoperate more reliably today than
//!   coordinate metadata.
//!
//! Supported image pixel types: [`f32`], [`f64`],
//! [`Complex32`](casacore_types::Complex32), and
//! [`Complex64`](casacore_types::Complex64). The convenience alias [`Image`]
//! remains available for `PagedImage<f32>`.
//!
//! # Example
//!
//! ```rust
//! use casacore_coordinates::CoordinateSystem;
//! use casacore_images::{
//!     ImageExpr, ImageExprBinaryOp, ImageExprUnaryOp, ImageIter, ImageType, TempImage,
//! };
//! use casacore_lattices::{Lattice, LatticeMut};
//! use casacore_types::Complex32;
//!
//! let mut image = TempImage::<f32>::new(vec![8, 8], CoordinateSystem::new()).unwrap();
//! image.set(1.0).unwrap();
//!
//! let expr = ImageExpr::from_image(&image)
//!     .unwrap()
//!     .binary_scalar(2.0, ImageExprBinaryOp::Multiply)
//!     .unary(ImageExprUnaryOp::Negate);
//! assert_eq!(expr.get_at(&[0, 0]).unwrap(), -2.0);
//!
//! let complex = TempImage::<Complex32>::new(vec![4, 4], CoordinateSystem::new()).unwrap();
//! let _chunks = ImageIter::new(&complex, vec![2, 2]).count();
//! assert_eq!(ImageType::Velocity.to_string(), "Velocity");
//! ```

pub mod beam;
pub mod error;
pub mod expr_file;
pub mod expr_parser;
pub mod image;
pub mod image_expr;
pub mod image_info;
pub mod iterator;
pub mod subimage;
pub mod temp_image;

#[cfg(test)]
mod mask;

pub use beam::{GaussianBeam, ImageBeamSet};
pub use error::ImageError;
pub use image::{
    AnyPagedImage, Image, ImageInterface, ImagePixel, ImagePixelType, MutableImageInterface,
    PagedImage, image_pixel_type,
};
pub use image_expr::{
    CompiledImageExpr, CompiledMaskExpr, ImageExpr, ImageExprBinaryOp, ImageExprCompareOp,
    ImageExprUnaryOp, MaskExpr, MaskLogicalOp, ReductionOp,
};
pub use image_info::{ImageInfo, ImageType};
pub use iterator::{ImageChunk, ImageIter, ImageIterMut};
pub use subimage::{SubImage, SubImageMut};
pub use temp_image::TempImage;

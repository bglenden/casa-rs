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
//! [`Complex32`](casa_types::Complex32), and
//! [`Complex64`](casa_types::Complex64). The convenience alias [`Image`]
//! remains available for `PagedImage<f32>`.
//!
//! # Example
//!
//! ```rust
//! use casa_coordinates::CoordinateSystem;
//! use casa_images::{ImageExprBuilder, ImageExprBinaryOp, ImageExprUnaryOp, ImageType, TempImage};
//! use casa_lattices::{Lattice, LatticeIterExt, LatticeMut, TraversalSpec};
//! use casa_types::Complex32;
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

pub mod analysis;
pub mod beam;
mod component_list_image;
pub mod concat_file;
pub mod concatenate;
pub mod convolve2d;
pub mod error;
pub mod expr_file;
pub mod expr_parser;
pub mod fits_read;
pub mod fits_table;
pub mod image;
pub mod image_expr;
mod image_expr_lel;
mod image_expr_ops;
pub mod image_info;
mod linear_mosaic;
pub mod miriad_read;
pub mod pixel_slice;
mod plane;
pub mod regrid;
pub mod saved_region;
pub mod subimage;
pub mod temp_image;
pub mod virtual_concat;

#[cfg(test)]
mod mask;
mod mask_reference;

pub use analysis::{
    ExportFitsRequest, FeatherRequest, FeatherSummary, FitsExportSummary, FitsImportSummary,
    ImageHeaderSummary, ImageMathSummary, ImageRegridSummary, ImageStatisticsSummary,
    ImageSubimageSummary, ImheadValue, ImmathRequest, ImmomentsRequest, ImpbcorRequest,
    ImportFitsRequest, ImportFitsValue, ImpvRequest, ImregridRequest, ImstatOutcome,
    ImstatPlanesSummary, ImstatRequest, ImsubimageRequest, MomentAxis, MomentImageSummary,
    MomentMapSummary, PbcorBeam, PbcorRegion, PbcorSummary, PvAmount, PvImageSummary, PvPosition,
    export_fits, exportfits, feather, imhead, imhead_add_delete, imhead_extrema, imhead_put,
    immath, immoments, impbcor, import_fits, importfits, impv, imregrid, imstat, imsubimage,
    parse_image_channel_selection, parse_imstat_algorithm,
};
pub use analysis::{ImregridOutcome, ImregridTemplate};
pub use beam::{BeamComponent, GaussianBeam, ImageBeamSet, StoredGaussianBeam};
pub use casa_lattices::{
    ScratchSpace, StatisticsAlgorithmKind, TempStoragePlan, TempStoragePolicy,
};
pub use component_list_image::ComponentListImage;
pub use convolve2d::{Convolved, convolve2d};
pub use error::ImageError;
pub use fits_read::{FitsImage, fits_image_shape, read_fits_image};
pub use image::{
    AnyPagedImage, Image, ImageInterface, ImagePixel, ImagePixelType, MutableImageInterface,
    PagedImage, image_pixel_type,
};
pub use image_expr::{
    ImageExprBinaryOp, ImageExprBuilder, ImageExprCompareOp, ImageExprUnaryOp, ImageExpression,
    MaskExprBuilder, MaskExpression, MaskLogicalOp, ReductionOp,
};
pub use image_info::{ImageInfo, ImageType};
pub use linear_mosaic::linearmosaic;
pub use regrid::{
    RegridMethod, RegridTemplate, Regridded, check_regrid, check_regrid_along_frequency, regrid,
    regrid_along_frequency,
};
pub use saved_region::{PolygonRegion, RegionStatistics};
pub use subimage::{SubImage, SubImageMut};
pub use temp_image::TempImage;

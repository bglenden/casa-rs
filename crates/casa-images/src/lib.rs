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
//! - [`TempImage<T>`] stores pixels in a [`TempLattice`](casa_lattices::TempLattice)
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
//! [`Complex32`](casa_types::Complex32), and
//! [`Complex64`](casa_types::Complex64). The convenience alias [`Image`]
//! remains available for `PagedImage<f32>`.
//!
//! # Example
//!
//! ```rust
//! use casa_coordinates::CoordinateSystem;
//! use casa_images::{ImageExpr, ImageExprBinaryOp, ImageExprUnaryOp, ImageType, TempImage};
//! use casa_lattices::{Lattice, LatticeIterExt, LatticeMut, TraversalSpec};
//! use casa_types::Complex32;
//!
//! let mut image = TempImage::<f32>::new(vec![8, 8], CoordinateSystem::new(), casa_lattices::TempStoragePolicy::Memory).unwrap();
//! image.set(1.0).unwrap();
//!
//! let expr = ImageExpr::from_image(&image)
//!     .unwrap()
//!     .binary_scalar(2.0, ImageExprBinaryOp::Multiply)
//!     .unary(ImageExprUnaryOp::Negate);
//! assert_eq!(expr.get_at(&[0, 0]).unwrap(), -2.0);
//!
//! let complex = TempImage::<Complex32>::new(vec![4, 4], CoordinateSystem::new(), casa_lattices::TempStoragePolicy::Memory).unwrap();
//! let _chunks = complex.traverse(TraversalSpec::chunks(vec![2, 2])).count();
//! assert_eq!(ImageType::Velocity.to_string(), "Velocity");
//! ```

pub mod analysis;
pub mod beam;
pub mod browser_render;
pub mod error;
pub mod expr_file;
pub mod expr_parser;
pub mod image;
pub mod image_expr;
pub mod image_info;
pub mod image_view;
pub mod imagebrowser_session;
pub mod movie;
pub mod subimage;
pub mod temp_image;

#[cfg(test)]
mod mask;

pub use analysis::{
    ExportFitsRequest, FeatherRequest, FeatherSummary, FitsExportSummary, FitsImportSummary,
    ImageAnalysisTaskRequest, ImageAnalysisTaskResult, ImageHeaderSummary, ImageMathSummary,
    ImageRegridSummary, ImageStatisticsSummary, ImageSubimageSummary, ImheadRequest, ImmathRequest,
    ImmomentsRequest, ImpbcorRequest, ImportFitsRequest, ImpvRequest, ImregridRequest,
    ImstatRequest, ImsubimageRequest, MomentMapSummary, PbcorSummary, PvImageSummary,
    dispatch_image_analysis_task_cli, export_fits, feather, image_analysis_protocol_descriptor,
    image_analysis_task_schema_bundle, imhead, imhead_put, immath, immoments, impbcor, import_fits,
    impv, imregrid, imstat, imsubimage, parse_image_channel_selection, run_image_analysis_task,
};
pub use beam::{GaussianBeam, ImageBeamSet};
pub use browser_render::{
    BrowserRenderTheme, ImagePlaneColormap, ImagePlaneDrawGeometry, ImagePlaneLayout,
    ImagePlaneOverlayMarker, ImagePlaneRenderInput, ImageSpectrumLayout,
    ImageSpectrumOverlaySeries, ImageSpectrumRenderInput, image_plane_draw_geometry,
    image_plane_layout, image_spectrum_layout, render_image_plane_image,
    render_image_spectrum_image,
};
pub use casa_lattices::{ScratchSpace, TempStoragePlan, TempStoragePolicy};
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
pub use image_view::{
    ImageAxisModel, ImageAxisValue, ImageDisplayAxis, ImageMetadataSection, ImageNonDisplayAxis,
    ImageProbe, ImageProfile, ImageProfileSample, ImageViewCapabilities, ImageViewWindow,
    OpenedImageView, PlaneRaster,
};
pub use imagebrowser_session::ImageBrowserSession;
pub use movie::{
    ImageMovieBundleCache, ImageMovieBundleEngine, ImageMovieBundleKey, ImageMovieBundleRequest,
    ImageMovieOccurrence, ImageMoviePreparedBundle, ImageMoviePreparedSurface,
    ImageMoviePresentationCoordinator, ImageMoviePresentationPoll, ImageMovieRender,
    ImageMovieRenderedBundle, ImageMovieRenderedSurface, ImageMovieSurfaceKind,
    ImageMovieSurfaceRequest, ImageMovieSurfaceSpec,
};
pub use subimage::{SubImage, SubImageMut};
pub use temp_image::TempImage;

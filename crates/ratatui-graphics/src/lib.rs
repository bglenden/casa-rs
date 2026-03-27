// SPDX-License-Identifier: LGPL-3.0-or-later
#![doc = include_str!("../README.md")]

#[cfg(feature = "plotters")]
pub mod bitmap;
pub mod image_ops;
#[cfg(feature = "kitty")]
pub mod kitty;
#[cfg(feature = "panel")]
pub mod panel;
pub mod terminal;

#[cfg(feature = "plotters")]
pub use bitmap::{PlottersBitmap, PlottersBitmapError};
pub use image_ops::{
    ImageLayers, ImageOpsError, apply_opacity, fit_pixels_preserving_aspect,
    key_background_transparency, prepare_image_layers,
};
#[cfg(feature = "kitty")]
pub use kitty::{
    KittyLayerError, KittyLayerHandle, KittyLayerManager, KittyPlacement, KittyPlacementError,
};
#[cfg(feature = "panel")]
pub use panel::{
    PanelInitError, PanelRenderError, PanelRenderJob, PanelRenderer, PanelSubmitError,
    PanelWorkerError, PreparedPanelProtocol, render_panel_protocol,
};
#[cfg(any(feature = "panel", feature = "kitty"))]
pub use ratatui_image::picker::{Picker, ProtocolType};
#[cfg(feature = "panel")]
pub use ratatui_image::{Resize, protocol::Protocol as PanelProtocol};
#[cfg(any(feature = "panel", feature = "kitty"))]
pub use terminal::TerminalCapabilities;
#[cfg(feature = "terminal-detect")]
pub use terminal::{TerminalDetectError, detect_terminal_background};

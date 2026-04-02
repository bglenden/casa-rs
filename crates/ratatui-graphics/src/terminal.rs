// SPDX-License-Identifier: LGPL-3.0-or-later
//! Terminal capability helpers.

#[cfg(any(feature = "panel", feature = "kitty"))]
use ratatui_image::picker::{Picker, ProtocolType};
#[cfg(any(feature = "panel", feature = "kitty"))]
use std::env;
#[cfg(feature = "terminal-detect")]
use std::time::Duration;
#[cfg(feature = "terminal-detect")]
use thiserror::Error;

/// Library-level summary of the currently selected terminal image capabilities.
#[cfg(any(feature = "panel", feature = "kitty"))]
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct TerminalCapabilities {
    /// The `ratatui-image` protocol selected for panel rendering.
    pub panel_protocol: ProtocolType,
    /// Whether the library should enable direct Kitty layer placement.
    pub direct_kitty_layers: bool,
    /// Whether the terminal can drive Kitty animations directly.
    pub direct_kitty_animations: bool,
}

#[cfg(any(feature = "panel", feature = "kitty"))]
impl TerminalCapabilities {
    /// Derive library-level image capabilities from a `ratatui-image` picker.
    pub fn from_picker(picker: &Picker) -> Self {
        let panel_protocol = picker.protocol_type();
        let direct_kitty_layers = matches!(panel_protocol, ProtocolType::Kitty);
        let direct_kitty_animations = direct_kitty_layers && !is_ghostty_terminal();
        Self {
            panel_protocol,
            direct_kitty_layers,
            direct_kitty_animations,
        }
    }
}

#[cfg(any(feature = "panel", feature = "kitty"))]
fn is_ghostty_terminal() -> bool {
    matches!(env::var("TERM_PROGRAM").as_deref(), Ok("ghostty"))
        || env::var_os("GHOSTTY_RESOURCES_DIR").is_some()
        || env::var_os("GHOSTTY_BIN_DIR").is_some()
}

/// Errors returned by terminal background probing.
#[cfg(feature = "terminal-detect")]
#[derive(Debug, Error)]
pub enum TerminalDetectError {
    /// The terminal background query failed.
    #[error("failed to query terminal background color: {0}")]
    Query(termbg::Error),
}

/// Query the terminal for its background color using `termbg`.
#[cfg(feature = "terminal-detect")]
pub fn detect_terminal_background(timeout: Duration) -> Result<[u8; 3], TerminalDetectError> {
    termbg::rgb(timeout)
        .map(term_bg_rgb8)
        .map_err(TerminalDetectError::Query)
}

#[cfg(feature = "terminal-detect")]
fn term_bg_rgb8(color: termbg::Rgb) -> [u8; 3] {
    [
        term_channel_to_u8(color.r),
        term_channel_to_u8(color.g),
        term_channel_to_u8(color.b),
    ]
}

#[cfg(feature = "terminal-detect")]
fn term_channel_to_u8(value: u16) -> u8 {
    ((u32::from(value) * 255 + 32_767) / 65_535) as u8
}

#[cfg(all(test, feature = "terminal-detect"))]
mod tests {
    use super::{term_bg_rgb8, term_channel_to_u8};

    #[test]
    fn term_channel_conversion_covers_bounds() {
        assert_eq!(term_channel_to_u8(0), 0);
        assert_eq!(term_channel_to_u8(u16::MAX), 255);
        assert_eq!(term_channel_to_u8(32_768), 128);
    }

    #[test]
    fn term_bg_rgb8_converts_each_channel() {
        let converted = term_bg_rgb8(termbg::Rgb {
            r: 0,
            g: 32_768,
            b: u16::MAX,
        });
        assert_eq!(converted, [0, 128, 255]);
    }
}

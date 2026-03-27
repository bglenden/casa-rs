// SPDX-License-Identifier: LGPL-3.0-or-later
//! Direct Kitty graphics layer helpers.

use std::{io::Write, num::NonZeroU32};

use image::{DynamicImage, RgbaImage};
use kittage::{
    NumberOrId, Verbosity,
    action::Action,
    delete::{ClearOrDelete, DeleteConfig, WhichToDelete},
    display::{CursorMovementPolicy, DisplayConfig, DisplayLocation},
    image::Image as KittyImage,
};
use ratatui::{
    crossterm::{
        cursor::{MoveTo, RestorePosition, SavePosition},
        execute,
    },
    layout::Rect,
};
use thiserror::Error;

/// A typed image/placement handle for a direct Kitty graphics layer.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct KittyLayerHandle {
    image_id: NonZeroU32,
    placement_id: NonZeroU32,
}

impl KittyLayerHandle {
    /// Return the image id used for Kitty image uploads.
    pub fn image_id(self) -> NonZeroU32 {
        self.image_id
    }

    /// Return the placement id used for Kitty image display operations.
    pub fn placement_id(self) -> NonZeroU32 {
        self.placement_id
    }
}

/// Placement configuration for a direct Kitty graphics layer.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct KittyPlacement {
    /// The target terminal cell rectangle.
    pub rect: Rect,
    /// The Kitty z-index to use for this placement.
    pub z_index: i32,
    /// Whether the current cursor position should be restored after placement.
    pub preserve_cursor: bool,
}

/// Errors returned when validating a Kitty placement.
#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub enum KittyPlacementError {
    /// Placements must target a non-empty cell rect.
    #[error("kitty placement rect must be non-empty, got {width}x{height} cells")]
    EmptyRect { width: u16, height: u16 },
}

/// Errors returned by direct Kitty layer operations.
#[derive(Debug, Error)]
pub enum KittyLayerError {
    /// The manager ran out of image or placement ids.
    #[error("kitty layer id space is exhausted")]
    IdExhausted,
    /// The placement was invalid.
    #[error(transparent)]
    InvalidPlacement(#[from] KittyPlacementError),
    /// Writing Kitty escape sequences failed.
    #[error("failed to write Kitty graphics sequence")]
    Io(#[from] std::io::Error),
}

/// Allocates typed handles for direct Kitty graphics layers.
#[derive(Debug, Default)]
pub struct KittyLayerManager {
    next_image_id: u32,
    next_placement_id: u32,
}

impl KittyLayerManager {
    /// Create a new layer manager with monotonic id allocation.
    pub fn new() -> Self {
        Self {
            next_image_id: 1,
            next_placement_id: 1,
        }
    }

    /// Allocate a fresh image/placement handle pair.
    pub fn allocate(&mut self) -> Result<KittyLayerHandle, KittyLayerError> {
        let image_id = NonZeroU32::new(self.next_image_id).ok_or(KittyLayerError::IdExhausted)?;
        let placement_id =
            NonZeroU32::new(self.next_placement_id).ok_or(KittyLayerError::IdExhausted)?;
        self.next_image_id = self
            .next_image_id
            .checked_add(1)
            .ok_or(KittyLayerError::IdExhausted)?;
        self.next_placement_id = self
            .next_placement_id
            .checked_add(1)
            .ok_or(KittyLayerError::IdExhausted)?;

        Ok(KittyLayerHandle {
            image_id,
            placement_id,
        })
    }

    /// Upload RGBA image data to the terminal using the supplied handle.
    pub fn upload_rgba<W: Write>(
        &self,
        out: &mut W,
        handle: KittyLayerHandle,
        image: &RgbaImage,
    ) -> Result<(), KittyLayerError> {
        let mut kitty_image: KittyImage<'static> = DynamicImage::ImageRgba8(image.clone()).into();
        kitty_image.num_or_id = NumberOrId::Id(handle.image_id);
        Action::Transmit(kitty_image).write_transmit_to(out, Verbosity::Silent)?;
        Ok(())
    }

    /// Place a previously uploaded image on the terminal surface.
    pub fn place<W: Write>(
        &self,
        out: &mut W,
        handle: KittyLayerHandle,
        placement: KittyPlacement,
    ) -> Result<(), KittyLayerError> {
        validate_placement(placement)?;

        let config = DisplayConfig {
            location: DisplayLocation {
                columns: placement.rect.width,
                rows: placement.rect.height,
                z_index: placement.z_index,
                ..DisplayLocation::default()
            },
            cursor_movement: if placement.preserve_cursor {
                CursorMovementPolicy::DontMove
            } else {
                CursorMovementPolicy::MoveToAfterImage
            },
            ..DisplayConfig::default()
        };

        if placement.preserve_cursor {
            execute!(
                out,
                SavePosition,
                MoveTo(placement.rect.x, placement.rect.y)
            )?;
        } else {
            execute!(out, MoveTo(placement.rect.x, placement.rect.y))?;
        }

        Action::Display {
            image_id: handle.image_id,
            placement_id: handle.placement_id,
            config,
        }
        .write_transmit_to(&mut *out, Verbosity::Silent)?;

        if placement.preserve_cursor {
            execute!(out, RestorePosition)?;
        }

        Ok(())
    }

    /// Upload a new RGBA image and place it immediately.
    pub fn upload_and_place_rgba<W: Write>(
        &self,
        out: &mut W,
        handle: KittyLayerHandle,
        image: &RgbaImage,
        placement: KittyPlacement,
    ) -> Result<(), KittyLayerError> {
        self.upload_rgba(out, handle, image)?;
        self.place(out, handle, placement)
    }

    /// Clear the current placement while keeping the uploaded image available in terminal memory.
    pub fn clear_placement<W: Write>(
        &self,
        out: &mut W,
        handle: KittyLayerHandle,
    ) -> Result<(), KittyLayerError> {
        let delete = Action::Delete(DeleteConfig {
            effect: ClearOrDelete::Clear,
            which: WhichToDelete::ImageId(handle.image_id, Some(handle.placement_id)),
        });
        delete.write_transmit_to(out, Verbosity::Silent)?;
        Ok(())
    }

    /// Delete the uploaded image and all of its placements from terminal memory.
    pub fn delete_image<W: Write>(
        &self,
        out: &mut W,
        handle: KittyLayerHandle,
    ) -> Result<(), KittyLayerError> {
        let delete = Action::Delete(DeleteConfig {
            effect: ClearOrDelete::Delete,
            which: WhichToDelete::ImageId(handle.image_id, None),
        });
        delete.write_transmit_to(out, Verbosity::Silent)?;
        Ok(())
    }

    /// Clear the placement and delete the uploaded image.
    pub fn clear_and_delete<W: Write>(
        &self,
        out: &mut W,
        handle: KittyLayerHandle,
    ) -> Result<(), KittyLayerError> {
        self.delete_image(out, handle)
    }
}

fn validate_placement(placement: KittyPlacement) -> Result<(), KittyPlacementError> {
    if placement.rect.is_empty() {
        return Err(KittyPlacementError::EmptyRect {
            width: placement.rect.width,
            height: placement.rect.height,
        });
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use image::{Rgba, RgbaImage};
    use ratatui::layout::Rect;

    use super::{KittyLayerManager, KittyPlacement, KittyPlacementError};

    #[test]
    fn allocate_returns_unique_handles() {
        let mut manager = KittyLayerManager::new();
        let first = manager.allocate().unwrap();
        let second = manager.allocate().unwrap();

        assert_ne!(first.image_id(), second.image_id());
        assert_ne!(first.placement_id(), second.placement_id());
    }

    #[test]
    fn reject_empty_rect_placements() {
        let mut manager = KittyLayerManager::new();
        let handle = manager.allocate().unwrap();
        let mut out = Vec::new();
        let err = manager
            .place(
                &mut out,
                handle,
                KittyPlacement {
                    rect: Rect::new(0, 0, 0, 2),
                    z_index: 1,
                    preserve_cursor: true,
                },
            )
            .unwrap_err();

        assert!(matches!(
            err,
            super::KittyLayerError::InvalidPlacement(KittyPlacementError::EmptyRect {
                width: 0,
                height: 2
            })
        ));
    }

    #[test]
    fn writes_upload_place_and_delete_sequences() {
        let mut manager = KittyLayerManager::new();
        let handle = manager.allocate().unwrap();
        let image = RgbaImage::from_pixel(1, 1, Rgba([1, 2, 3, 255]));
        let placement = KittyPlacement {
            rect: Rect::new(4, 5, 1, 1),
            z_index: 3,
            preserve_cursor: true,
        };

        let mut upload = Vec::new();
        manager.upload_rgba(&mut upload, handle, &image).unwrap();
        let upload = String::from_utf8(upload).unwrap();
        assert!(upload.contains("\u{1b}_Ga=t"));

        let mut place = Vec::new();
        manager.place(&mut place, handle, placement).unwrap();
        let place = String::from_utf8(place).unwrap();
        assert!(place.contains("\u{1b}_Ga=p"));
        assert!(place.contains(",c=1"));
        assert!(place.contains(",r=1"));
        assert!(place.contains(",z=3"));

        let mut clear = Vec::new();
        manager.clear_placement(&mut clear, handle).unwrap();
        let clear = String::from_utf8(clear).unwrap();
        assert!(clear.contains("\u{1b}_Ga=d"));
        assert!(clear.contains(",d=i"));

        let mut delete = Vec::new();
        manager.delete_image(&mut delete, handle).unwrap();
        let delete = String::from_utf8(delete).unwrap();
        assert!(delete.contains("\u{1b}_Ga=d"));
        assert!(delete.contains(",d=I"));
    }
}

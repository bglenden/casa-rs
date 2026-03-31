// SPDX-License-Identifier: LGPL-3.0-or-later
//! Direct Kitty graphics layer helpers.

use std::{collections::HashMap, io::Write, num::NonZeroU32, time::Duration};

use base64_simd::{Base64, Out};
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

/// Stable id for a terminal-resident Kitty image stored in [`KittyStoredImageStore`].
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct KittyStoredImageId(NonZeroU32);

impl KittyStoredImageId {
    /// Return the underlying Kitty image id.
    pub fn raw(self) -> NonZeroU32 {
        self.0
    }
}

/// Stable id for a ratatui-defined pane slot in [`KittyStoredImageStore`].
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct KittyPaneSlotId(NonZeroU32);

impl KittyPaneSlotId {
    /// Return the underlying Kitty placement id.
    pub fn raw(self) -> NonZeroU32 {
        self.0
    }
}

/// Metadata about a terminal-resident stored image.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct KittyStoredImageInfo {
    /// Width of the uploaded bitmap in pixels.
    pub pixel_width: u32,
    /// Height of the uploaded bitmap in pixels.
    pub pixel_height: u32,
    /// Raw RGBA byte size uploaded for the bitmap.
    pub bytes: usize,
}

impl KittyLayerHandle {
    /// Construct a handle from explicit Kitty image and placement ids.
    pub const fn new(image_id: NonZeroU32, placement_id: NonZeroU32) -> Self {
        Self {
            image_id,
            placement_id,
        }
    }

    /// Return the image id used for Kitty image uploads.
    pub fn image_id(self) -> NonZeroU32 {
        self.image_id
    }

    /// Return the placement id used for Kitty image display operations.
    pub fn placement_id(self) -> NonZeroU32 {
        self.placement_id
    }

    /// Return a copy of this handle with a different placement id.
    pub const fn with_placement_id(self, placement_id: NonZeroU32) -> Self {
        Self {
            image_id: self.image_id,
            placement_id,
        }
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
    /// Animation gaps must fit in Kitty's signed 32-bit control range.
    #[error("kitty animation gap exceeds the supported control range")]
    AnimationGapOverflow,
    /// The placement was invalid.
    #[error(transparent)]
    InvalidPlacement(#[from] KittyPlacementError),
    /// Writing Kitty escape sequences failed.
    #[error("failed to write Kitty graphics sequence")]
    Io(#[from] std::io::Error),
    /// The requested pane slot is unknown.
    #[error("unknown kitty pane slot {0}")]
    UnknownSlot(NonZeroU32),
    /// The requested stored image is unknown.
    #[error("unknown stored kitty image {0}")]
    UnknownImage(NonZeroU32),
}

/// Allocates typed handles for direct Kitty graphics layers.
#[derive(Debug, Default)]
pub struct KittyLayerManager {
    next_image_id: u32,
    next_placement_id: u32,
}

/// A small typed store for terminal-resident Kitty images and pane slots.
///
/// This keeps uploaded images alive in terminal memory and re-places them into
/// ratatui-defined pane rectangles without re-uploading the pixel buffer on
/// every frame.
#[derive(Debug)]
pub struct KittyStoredImageStore {
    manager: KittyLayerManager,
    slots: HashMap<KittyPaneSlotId, Option<KittyStoredImageId>>,
    images: HashMap<KittyStoredImageId, KittyStoredImageInfo>,
    total_bytes: usize,
}

/// Terminal-driven animation state for a Kitty image.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum KittyAnimationPlaybackState {
    /// Stop playback and leave the current frame visible.
    Stopped,
    /// Play while waiting for more frames at the end of the sequence.
    Loading,
    /// Play and loop over the uploaded frame sequence.
    Looping,
}

impl KittyAnimationPlaybackState {
    fn protocol_value(self) -> u8 {
        match self {
            Self::Stopped => 1,
            Self::Loading => 2,
            Self::Looping => 3,
        }
    }
}

/// A typed animation frame gap for Kitty playback controls.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum KittyAnimationGap {
    /// Display the frame for the specified amount of time.
    Timed(Duration),
    /// Store the frame without showing it to the user.
    Gapless,
}

impl KittyAnimationGap {
    fn protocol_value(self) -> Result<i32, KittyLayerError> {
        match self {
            Self::Timed(duration) => {
                let millis = duration.as_millis();
                if millis > i32::MAX as u128 {
                    return Err(KittyLayerError::AnimationGapOverflow);
                }
                Ok(millis as i32)
            }
            Self::Gapless => Ok(-1),
        }
    }
}

/// Control parameters for a Kitty animation.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct KittyAnimationControl {
    /// Optional playback state transition.
    pub state: Option<KittyAnimationPlaybackState>,
    /// Optional current frame to display, using Kitty's 1-based frame numbering.
    pub current_frame: Option<NonZeroU32>,
    /// Optional frame targeted by `gap`.
    pub frame_number: Option<NonZeroU32>,
    /// Optional gap update.
    pub gap: Option<KittyAnimationGap>,
    /// Optional loop count. `1` means loop forever in Kitty semantics.
    pub loops: Option<NonZeroU32>,
}

impl KittyLayerManager {
    /// Create a new layer manager with monotonic id allocation.
    pub fn new() -> Self {
        Self::with_starting_ids(1, 1).expect("default Kitty layer ids are valid")
    }

    /// Create a new layer manager with explicit starting ids.
    pub fn with_starting_ids(
        image_id_start: u32,
        placement_id_start: u32,
    ) -> Result<Self, KittyLayerError> {
        if image_id_start == 0 || placement_id_start == 0 {
            return Err(KittyLayerError::IdExhausted);
        }
        Ok(Self {
            next_image_id: image_id_start,
            next_placement_id: placement_id_start,
        })
    }

    /// Allocate a fresh Kitty image id.
    pub fn allocate_image_id(&mut self) -> Result<NonZeroU32, KittyLayerError> {
        let image_id = NonZeroU32::new(self.next_image_id).ok_or(KittyLayerError::IdExhausted)?;
        self.next_image_id = self
            .next_image_id
            .checked_add(1)
            .ok_or(KittyLayerError::IdExhausted)?;
        Ok(image_id)
    }

    /// Allocate a fresh Kitty placement id.
    pub fn allocate_placement_id(&mut self) -> Result<NonZeroU32, KittyLayerError> {
        let placement_id =
            NonZeroU32::new(self.next_placement_id).ok_or(KittyLayerError::IdExhausted)?;
        self.next_placement_id = self
            .next_placement_id
            .checked_add(1)
            .ok_or(KittyLayerError::IdExhausted)?;
        Ok(placement_id)
    }

    /// Allocate a fresh image/placement handle pair.
    pub fn allocate(&mut self) -> Result<KittyLayerHandle, KittyLayerError> {
        Ok(KittyLayerHandle::new(
            self.allocate_image_id()?,
            self.allocate_placement_id()?,
        ))
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
        Action::Transmit(kitty_image).write_transmit_to(&mut *out, Verbosity::Silent)?;
        out.flush()?;
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

        out.flush()?;

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

    /// Append a full RGBA animation frame to an existing Kitty image.
    ///
    /// The image itself must already have been created with [`Self::upload_rgba`] or
    /// [`Self::upload_and_place_rgba`]. The appended frame uses Kitty's `a=f` animation-frame
    /// transfer mode and is associated with `handle.image_id()`.
    pub fn append_animation_frame_rgba<W: Write>(
        &self,
        out: &mut W,
        handle: KittyLayerHandle,
        image: &RgbaImage,
        gap: Option<KittyAnimationGap>,
    ) -> Result<(), KittyLayerError> {
        let mut intro = format!(
            "\x1b_Ga=f,i={},q=2,f=32,s={},v={}",
            handle.image_id(),
            image.width(),
            image.height()
        );
        if let Some(gap) = gap {
            intro.push_str(&format!(",z={}", gap.protocol_value()?));
        }
        write_direct_data_chunks(out, &intro, image.as_raw())?;
        Ok(())
    }

    /// Send a typed animation control command for an existing Kitty image.
    pub fn control_animation<W: Write>(
        &self,
        out: &mut W,
        handle: KittyLayerHandle,
        control: KittyAnimationControl,
    ) -> Result<(), KittyLayerError> {
        write!(out, "\x1b_Ga=a,i={},q=2", handle.image_id())?;
        if let Some(current_frame) = control.current_frame {
            write!(out, ",c={}", current_frame.get())?;
        }
        if let Some(frame_number) = control.frame_number {
            write!(out, ",r={}", frame_number.get())?;
        }
        if let Some(gap) = control.gap {
            write!(out, ",z={}", gap.protocol_value()?)?;
        }
        if let Some(state) = control.state {
            write!(out, ",s={}", state.protocol_value())?;
        }
        if let Some(loops) = control.loops {
            write!(out, ",v={}", loops.get())?;
        }
        write!(out, "\x1b\\")?;
        out.flush()?;
        Ok(())
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
        delete.write_transmit_to(&mut *out, Verbosity::Silent)?;
        out.flush()?;
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
        delete.write_transmit_to(&mut *out, Verbosity::Silent)?;
        out.flush()?;
        Ok(())
    }

    /// Delete a contiguous range of uploaded images and their placements from terminal memory.
    pub fn delete_image_range<W: Write>(
        &self,
        out: &mut W,
        start: NonZeroU32,
        end: NonZeroU32,
    ) -> Result<(), KittyLayerError> {
        let delete = Action::Delete(DeleteConfig {
            effect: ClearOrDelete::Delete,
            which: WhichToDelete::IdRange(start..=end),
        });
        delete.write_transmit_to(&mut *out, Verbosity::Silent)?;
        out.flush()?;
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

impl KittyStoredImageStore {
    /// Create a new stored-image store with monotonic id allocation.
    pub fn new() -> Self {
        Self::with_starting_ids(1, 1).expect("default Kitty store ids are valid")
    }

    /// Create a new stored-image store with explicit starting ids.
    pub fn with_starting_ids(
        image_id_start: u32,
        placement_id_start: u32,
    ) -> Result<Self, KittyLayerError> {
        Ok(Self {
            manager: KittyLayerManager::with_starting_ids(image_id_start, placement_id_start)?,
            slots: HashMap::new(),
            images: HashMap::new(),
            total_bytes: 0,
        })
    }

    /// Allocate a stable pane slot id.
    pub fn allocate_slot(&mut self) -> Result<KittyPaneSlotId, KittyLayerError> {
        let slot = KittyPaneSlotId(self.manager.allocate_placement_id()?);
        self.slots.insert(slot, None);
        Ok(slot)
    }

    /// Upload an RGBA bitmap into terminal memory and return its stored id.
    pub fn store_rgba<W: Write>(
        &mut self,
        out: &mut W,
        image: &RgbaImage,
    ) -> Result<(KittyStoredImageId, KittyStoredImageInfo), KittyLayerError> {
        let image_id = KittyStoredImageId(self.manager.allocate_image_id()?);
        let info = KittyStoredImageInfo {
            pixel_width: image.width(),
            pixel_height: image.height(),
            bytes: image.as_raw().len(),
        };
        self.manager
            .upload_rgba(out, KittyLayerHandle::new(image_id.0, image_id.0), image)?;
        self.total_bytes = self.total_bytes.saturating_add(info.bytes);
        self.images.insert(image_id, info);
        Ok((image_id, info))
    }

    /// Place a stored image into a stable pane slot.
    pub fn place_in_slot<W: Write>(
        &mut self,
        out: &mut W,
        slot: KittyPaneSlotId,
        image: KittyStoredImageId,
        placement: KittyPlacement,
    ) -> Result<(), KittyLayerError> {
        if !self.images.contains_key(&image) {
            return Err(KittyLayerError::UnknownImage(image.0));
        }
        let Some(current) = self.slots.get_mut(&slot) else {
            return Err(KittyLayerError::UnknownSlot(slot.0));
        };
        self.manager
            .place(out, KittyLayerHandle::new(image.0, slot.0), placement)?;
        *current = Some(image);
        Ok(())
    }

    /// Clear the current placement for a pane slot while leaving stored images resident.
    pub fn clear_slot<W: Write>(
        &mut self,
        out: &mut W,
        slot: KittyPaneSlotId,
    ) -> Result<(), KittyLayerError> {
        let Some(current) = self.slots.get_mut(&slot) else {
            return Err(KittyLayerError::UnknownSlot(slot.0));
        };
        if let Some(image) = *current {
            self.manager
                .clear_placement(out, KittyLayerHandle::new(image.0, slot.0))?;
            *current = None;
        }
        Ok(())
    }

    /// Delete a stored image and clear any pane slots that currently show it.
    pub fn delete_image<W: Write>(
        &mut self,
        out: &mut W,
        image: KittyStoredImageId,
    ) -> Result<(), KittyLayerError> {
        let Some(info) = self.images.remove(&image) else {
            return Err(KittyLayerError::UnknownImage(image.0));
        };
        for (slot, current) in &mut self.slots {
            if *current == Some(image) {
                self.manager
                    .clear_placement(out, KittyLayerHandle::new(image.0, slot.0))?;
                *current = None;
            }
        }
        self.manager
            .delete_image(out, KittyLayerHandle::new(image.0, image.0))?;
        self.total_bytes = self.total_bytes.saturating_sub(info.bytes);
        Ok(())
    }

    /// Return the currently placed image in a pane slot, if any.
    pub fn slot_image(&self, slot: KittyPaneSlotId) -> Option<KittyStoredImageId> {
        self.slots.get(&slot).copied().flatten()
    }

    /// Return metadata for a stored image, if present.
    pub fn image_info(&self, image: KittyStoredImageId) -> Option<KittyStoredImageInfo> {
        self.images.get(&image).copied()
    }

    /// Total raw RGBA bytes currently kept in terminal-resident images.
    pub fn total_bytes(&self) -> usize {
        self.total_bytes
    }

    /// Number of stored terminal images currently tracked.
    pub fn image_count(&self) -> usize {
        self.images.len()
    }
}

impl Default for KittyStoredImageStore {
    fn default() -> Self {
        Self::new()
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

fn write_direct_data_chunks<W: Write>(
    out: &mut W,
    intro: &str,
    data: &[u8],
) -> Result<(), std::io::Error> {
    const BASE64_CHUNK_BYTES: usize = 4096;
    let pre_encoded_bytes_per_chunk = (BASE64_CHUNK_BYTES / 4) * 3;
    let total_chunks = data.len().div_ceil(pre_encoded_bytes_per_chunk).max(1);
    let mut chunks = data.chunks(pre_encoded_bytes_per_chunk);

    if let Some(first) = chunks.next() {
        write!(out, "{intro},t=d,m={};", u8::from(total_chunks > 1))?;
        write_base64(out, first)?;
        write!(out, "\x1b\\")?;
    }

    for (index, chunk) in chunks.enumerate() {
        write!(out, "\x1b_Gm={};", u8::from(index + 2 < total_chunks))?;
        write_base64(out, chunk)?;
        write!(out, "\x1b\\")?;
    }

    out.flush()?;
    Ok(())
}

fn write_base64<W: Write>(out: &mut W, data: &[u8]) -> Result<(), std::io::Error> {
    const ENCODER: Base64 = base64_simd::STANDARD_NO_PAD;
    const ENCODED_BUF_SIZE: usize = 1024;
    let mut buf = [0u8; ENCODED_BUF_SIZE];
    for chunk in data.chunks(ENCODER.estimated_decoded_length(ENCODED_BUF_SIZE)) {
        let encoded = ENCODER.encode(chunk, Out::from_slice(buf.as_mut_slice()));
        out.write_all(encoded)?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::{num::NonZeroU32, time::Duration};

    use image::{Rgba, RgbaImage};
    use ratatui::layout::Rect;

    use super::{
        KittyAnimationControl, KittyAnimationGap, KittyAnimationPlaybackState, KittyLayerManager,
        KittyPlacement, KittyPlacementError, KittyStoredImageStore,
    };

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

    #[test]
    fn wrapper_methods_compose_upload_and_delete_sequences() {
        let mut manager = KittyLayerManager::new();
        let handle = manager.allocate().unwrap();
        let image = RgbaImage::from_pixel(1, 1, Rgba([5, 6, 7, 255]));
        let placement = KittyPlacement {
            rect: Rect::new(2, 3, 1, 1),
            z_index: 2,
            preserve_cursor: false,
        };

        let mut out = Vec::new();
        manager
            .upload_and_place_rgba(&mut out, handle, &image, placement)
            .unwrap();
        let rendered = String::from_utf8(out).unwrap();
        assert!(rendered.contains("\u{1b}_Ga=t"));
        assert!(rendered.contains("\u{1b}_Ga=p"));
        assert!(rendered.contains(",z=2"));

        let mut delete = Vec::new();
        manager.clear_and_delete(&mut delete, handle).unwrap();
        let delete = String::from_utf8(delete).unwrap();
        assert!(delete.contains("\u{1b}_Ga=d"));
        assert!(delete.contains(",d=I"));
    }

    #[test]
    fn delete_image_range_writes_range_delete_sequence() {
        let manager = KittyLayerManager::new();
        let mut out = Vec::new();
        manager
            .delete_image_range(
                &mut out,
                NonZeroU32::new(100).unwrap(),
                NonZeroU32::new(125).unwrap(),
            )
            .unwrap();
        let text = String::from_utf8(out).unwrap();
        assert!(text.contains("\u{1b}_Ga=d"));
        assert!(text.contains(",d=R"));
        assert!(text.contains(",x=100"));
        assert!(text.contains(",y=125"));
    }

    #[test]
    fn append_animation_frame_uses_frame_action_and_gap() {
        let mut manager = KittyLayerManager::new();
        let handle = manager.allocate().unwrap();
        let image = RgbaImage::from_pixel(2, 1, Rgba([1, 2, 3, 255]));
        let mut out = Vec::new();

        manager
            .append_animation_frame_rgba(
                &mut out,
                handle,
                &image,
                Some(KittyAnimationGap::Timed(Duration::from_millis(48))),
            )
            .unwrap();

        let text = String::from_utf8(out).unwrap();
        assert!(text.contains("\u{1b}_Ga=f,i=1,q=2,f=32,s=2,v=1,z=48,t=d,m=0;"));
    }

    #[test]
    fn control_animation_writes_state_gap_and_current_frame() {
        let mut manager = KittyLayerManager::new();
        let handle = manager.allocate().unwrap();
        let mut out = Vec::new();

        manager
            .control_animation(
                &mut out,
                handle,
                KittyAnimationControl {
                    state: Some(KittyAnimationPlaybackState::Looping),
                    current_frame: Some(NonZeroU32::new(7).unwrap()),
                    frame_number: Some(NonZeroU32::new(1).unwrap()),
                    gap: Some(KittyAnimationGap::Timed(Duration::from_millis(33))),
                    loops: Some(NonZeroU32::new(1).unwrap()),
                },
            )
            .unwrap();

        let text = String::from_utf8(out).unwrap();
        assert_eq!(text, "\u{1b}_Ga=a,i=1,q=2,c=7,r=1,z=33,s=3,v=1\u{1b}\\");
    }

    #[test]
    fn stored_image_store_places_and_tracks_images_by_slot() {
        let mut store = KittyStoredImageStore::new();
        let slot = store.allocate_slot().unwrap();
        let image_a = RgbaImage::from_pixel(2, 1, Rgba([1, 2, 3, 255]));
        let image_b = RgbaImage::from_pixel(2, 1, Rgba([4, 5, 6, 255]));
        let placement = KittyPlacement {
            rect: Rect::new(0, 0, 2, 1),
            z_index: 9,
            preserve_cursor: true,
        };

        let mut out = Vec::new();
        let (stored_a, info_a) = store.store_rgba(&mut out, &image_a).unwrap();
        let (stored_b, info_b) = store.store_rgba(&mut out, &image_b).unwrap();
        assert_eq!(info_a.bytes, image_a.as_raw().len());
        assert_eq!(info_b.bytes, image_b.as_raw().len());
        assert_eq!(store.total_bytes(), info_a.bytes + info_b.bytes);

        store
            .place_in_slot(&mut out, slot, stored_a, placement)
            .unwrap();
        assert_eq!(store.slot_image(slot), Some(stored_a));

        store
            .place_in_slot(&mut out, slot, stored_b, placement)
            .unwrap();
        assert_eq!(store.slot_image(slot), Some(stored_b));
        assert_eq!(store.image_count(), 2);
    }

    #[test]
    fn deleting_stored_image_clears_slot_association() {
        let mut store = KittyStoredImageStore::new();
        let slot = store.allocate_slot().unwrap();
        let image = RgbaImage::from_pixel(1, 1, Rgba([7, 8, 9, 255]));
        let placement = KittyPlacement {
            rect: Rect::new(0, 0, 1, 1),
            z_index: 1,
            preserve_cursor: true,
        };
        let mut out = Vec::new();

        let (stored, info) = store.store_rgba(&mut out, &image).unwrap();
        store
            .place_in_slot(&mut out, slot, stored, placement)
            .unwrap();
        assert_eq!(store.slot_image(slot), Some(stored));

        store.delete_image(&mut out, stored).unwrap();
        assert_eq!(store.slot_image(slot), None);
        assert_eq!(store.total_bytes(), 0);
        assert_eq!(store.image_count(), 0);
        let text = String::from_utf8(out).unwrap();
        assert!(text.contains("\u{1b}_Ga=t"));
        assert!(text.contains("\u{1b}_Ga=p"));
        assert!(text.contains("\u{1b}_Ga=d"));
        assert!(!text.is_empty());
        assert_eq!(info.bytes, image.as_raw().len());
    }
}

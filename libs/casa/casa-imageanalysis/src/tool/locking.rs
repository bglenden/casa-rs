// SPDX-License-Identifier: LGPL-3.0-or-later

//! Lock management and caching operations for attached image handles.

use casa_tables::LockType;

use super::attachment::AttachedImage;
use super::error::{ImageToolError, ImageToolResult};
use super::handle::{ImageHandle, Storage, with_pixels};

impl AttachedImage {
    /// Acquires `kind` lock with `attempts` retries on a single expression operand.
    pub fn lock_expression_operand(
        &self,
        kind: LockType,
        attempts: u32,
        section_read: bool,
    ) -> ImageToolResult<bool> {
        let mut source = self.lock();
        self.check_bound_name(&source)?;
        if section_read
            && with_pixels!(&*source, storage => match storage {
                Storage::Temp { image, .. } => {
                    image.concat_source().is_some_and(|concat| !concat.temp_close())
                }
                Storage::Paged(_) => false,
            })
        {
            // A retained concatenation acquires each member's read lease as
            // its requested section is evaluated.
            return Ok(true);
        }
        source.lock_storage(kind, attempts)
    }

    /// Acquires `kind` lock on `handle` and all retained expression inputs.
    pub fn lock_with_inputs(
        &self,
        handle: &mut ImageHandle,
        kind: LockType,
        attempts: u32,
        section_read: bool,
    ) -> ImageToolResult<bool> {
        self.check_bound_name(handle)?;
        for input in self.expression_inputs() {
            if !input.lock_expression_operand(kind, attempts, section_read)? {
                return Ok(false);
            }
        }
        handle.lock_storage(kind, attempts)
    }

    /// Ensures `handle` holds `kind` lock (implicitly satisfied for virtual concat read/tempclose).
    pub fn require_lock(&self, handle: &mut ImageHandle, kind: LockType) -> ImageToolResult<()> {
        self.check_bound_name(handle)?;
        if kind == LockType::Write
            && with_pixels!(&*handle, storage => matches!(storage, Storage::Paged(image) if !image.is_writable()))
        {
            let path = handle
                .path()
                .expect("a read-only paged image has a table path");
            return Err(ImageToolError::new(format!(
                "Table {} cannot be opened for read/write",
                path.display()
            )));
        }
        let implicit_concat = with_pixels!(&*handle, storage => match storage {
            Storage::Temp { image, .. } => image.concat_source().is_some_and(|source|
                kind == LockType::Read || source.temp_close()),
            Storage::Paged(_) => false,
        });
        if implicit_concat {
            return Ok(());
        }
        if handle.lock_storage(kind, 0)? {
            Ok(())
        } else {
            Err(ImageToolError::new(format!(
                "Could not acquire the image {kind:?} lock"
            )))
        }
    }

    /// Ensures a read lock is held before reading pixels from `handle` and any retained inputs.
    pub fn require_pixel_read_lock(&self, handle: &mut ImageHandle) -> ImageToolResult<()> {
        if handle.image_type_name() == "ImageConcat" {
            return self.check_bound_name(handle);
        }
        if self.lock_with_inputs(handle, LockType::Read, 0, true)? {
            Ok(())
        } else {
            Err(ImageToolError::new("Could not acquire the image Read lock"))
        }
    }

    /// Ensures the image is writable and holds a write lock before mutating pixels or metadata.
    pub fn require_write_lock(
        &self,
        handle: &mut ImageHandle,
        action: &str,
    ) -> ImageToolResult<()> {
        if self.is_read_only() {
            return Err(ImageToolError::new(format!(
                "{action}: image is read-only."
            )));
        }
        if let Some(kind) = handle.read_only_file_type() {
            return Err(ImageToolError::new(format!(
                "{action}: {kind} images are read-only."
            )));
        }
        let path_readonly = with_pixels!(handle, storage => match storage {
            Storage::Paged(paged) => !paged.is_writable(),
            Storage::Temp { .. } => false,
        });
        if path_readonly {
            return Err(ImageToolError::new(format!(
                "{action}: image is read-only."
            )));
        }
        if self.has_advisory_lock() {
            return Ok(());
        }
        self.require_lock(handle, LockType::Write)
    }

    /// Releases requested locks on `handle` and its expression inputs (triggering `tempclose` if set).
    pub fn release_requested_lock(&self, handle: &mut ImageHandle) -> ImageToolResult<()> {
        for input in self.expression_inputs() {
            input.lock().release_requested_lock()?;
        }
        handle.release_requested_lock()
    }
}

/// Parses CASA's `lock(mode, nattempts)` lock mode string.
pub fn parse_lock_mode(mode: &str) -> ImageToolResult<LockType> {
    if mode.eq_ignore_ascii_case("read") {
        Ok(LockType::Read)
    } else if mode.eq_ignore_ascii_case("write") {
        Ok(LockType::Write)
    } else {
        Err(ImageToolError::new(format!(
            "Invalid lock mode '{mode}'; must be 'read' or 'write'."
        )))
    }
}

/// Acquires a lock (`read` or `write`) on `attached` and its expression inputs.
pub fn lock_image(attached: &AttachedImage, mode: &str, nattempts: isize) -> ImageToolResult<bool> {
    let kind = parse_lock_mode(mode)?;
    if kind == LockType::Write && attached.is_read_only() {
        return Err(ImageToolError::new(
            "Cannot acquire write lock on a read-only image.",
        ));
    }
    let attempts = u32::try_from(nattempts.max(0)).unwrap_or(0);
    let mut image = attached.lock();
    if attached.has_advisory_lock() {
        return Ok(true);
    }
    let acquired = attached.lock_with_inputs(&mut image, kind, attempts, false)?;
    if acquired && kind == LockType::Write {
        let info = super::beams::image_info(&image);
        if let Ok(info) = info {
            attached.beam_view().capture(&info.to_record());
        }
    }
    Ok(acquired)
}

/// Releases the current lock on `attached` and its expression inputs, flushing writable paged edits.
pub fn unlock_image(attached: &AttachedImage) -> ImageToolResult<bool> {
    let mut image = attached.lock();
    attached.check_bound_name(&image)?;
    let writable = !attached.is_read_only()
        && image.read_only_file_type().is_none()
        && with_pixels!(&*image, storage => match storage {
            Storage::Paged(paged) => paged.is_writable(),
            Storage::Temp { .. } => true,
        });
    if writable {
        image.save()?;
    }
    if attached.has_advisory_lock() {
        return Ok(true);
    }
    for input in attached.expression_inputs() {
        let mut source = input.lock();
        input.check_bound_name(&source)?;
        source.unlock_storage()?;
    }
    image.unlock_storage()?;
    Ok(true)
}

/// Queries whether `attached` (and all its expression inputs) currently holds a lock.
pub fn has_lock(attached: &AttachedImage, mode: Option<&str>) -> ImageToolResult<Vec<bool>> {
    let image = attached.lock();
    let holds = |kind| {
        image.has_storage_lock(kind)
            && attached
                .expression_inputs()
                .iter()
                .all(|input| input.lock().has_storage_lock(kind))
    };
    match mode {
        Some(mode) if !mode.is_empty() => {
            let kind = parse_lock_mode(mode)?;
            Ok(vec![holds(kind)])
        }
        _ => Ok(vec![holds(LockType::Read), holds(LockType::Write)]),
    }
}

/// Configures caching on `attached` (for component-list images, toggles pixel cache).
pub fn set_caching(attached: &AttachedImage, active: bool) -> ImageToolResult<bool> {
    let mut image = attached.lock();
    if !active {
        image.disable_component_cache()?;
    }
    Ok(true)
}

/// Checks single-precision lock status `[read_held, write_held]` (`ia.haslock`).
pub fn has_lock_single_precision(attached: &AttachedImage) -> ImageToolResult<Vec<bool>> {
    attached.lock().check_single_precision("haslock")?;
    has_lock(attached, None)
}

/// Acquires a read or write lock on a single-precision image (`ia.lock`).
pub fn lock_single_precision(
    attached: &AttachedImage,
    writelock: bool,
    nattempts: i32,
) -> ImageToolResult<bool> {
    let mut handle = attached.lock();
    handle.check_single_precision("lock")?;
    let kind = if writelock {
        LockType::Write
    } else {
        LockType::Read
    };
    attached.lock_with_inputs(&mut handle, kind, nattempts.max(0) as u32, false)
}

/// Releases locks on a single-precision image and flushes writable tiles (`ia.unlock`).
pub fn unlock_single_precision(attached: &AttachedImage) -> ImageToolResult<bool> {
    attached.lock().check_single_precision("unlock")?;
    unlock_image(attached)
}

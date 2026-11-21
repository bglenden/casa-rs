// SPDX-License-Identifier: LGPL-3.0-or-later

use std::ffi::CString;
use std::fs::File;
use std::io::Read;
use std::path::Path;

use fitsio::{FileOpenMode, FitsFile, errors::check_status};

use crate::ImageError;

pub(super) const FITS_RECORD_BYTES: u64 = 2880;

/// Use the full on-disk filename without CFITSIO's extended-name grammar.
pub(super) fn open_disk_file(path: &Path) -> fitsio::errors::Result<FitsFile> {
    let name = CString::new(path.as_os_str().as_encoded_bytes())?;
    let mut file = std::ptr::null_mut();
    let mut status = 0;
    // SAFETY: all arguments remain valid through this read-only open.
    unsafe { fitsio::sys::ffdkopn(&mut file, name.as_ptr(), 0, &mut status) };
    check_status(status)?;
    if file.is_null() {
        return Err(fitsio::errors::Error::NullPointer);
    }
    // SAFETY: the successful open returned a valid handle. Transfer ownership
    // to FitsFile, or close it here if the conversion fails.
    unsafe { FitsFile::from_raw(file, FileOpenMode::READONLY) }.inspect_err(|_| {
        let mut close_status = 0;
        // SAFETY: the handle remains ours after a failed conversion.
        unsafe { fitsio::sys::ffclos(file, &mut close_status) };
    })
}

pub(super) fn unreadable_fits(name: &Path) -> ImageError {
    ImageError::CasaSelection(format!(
        "Exception: {} does not exist or is not readable.",
        name.display()
    ))
}

/// Recognize a FITS file, then return its physical length and card spacing.
/// CFITSIO measures decompressed records for compressed streams.
pub(super) fn attachment_file_length(
    path: &Path,
    name: &Path,
) -> Result<(Option<u64>, bool), ImageError> {
    let unrecognized = |_| unable_to_open_image(name);
    let mut file = File::open(path).map_err(unrecognized)?;
    let mut first_card = [0; 80];
    file.read_exact(&mut first_card[..2])
        .map_err(unrecognized)?;
    if matches!(
        first_card[..2],
        [0x1f, 0x8b | 0x1e | 0x9d | 0xa0] | [b'P', b'K'] | [b'B', b'Z']
    ) {
        return Ok((None, false));
    }
    let length = file.metadata().map_err(unrecognized)?.len();
    if length < FITS_RECORD_BYTES {
        return Err(unable_to_open_image(name));
    }
    file.read_exact(&mut first_card[2..])
        .map_err(unrecognized)?;
    // ImageOpener recognizes SIMPLE followed by '=' and 'T', with any
    // number of intervening spaces. FITSImgParser then parses the card.
    let first_value = first_card[6..]
        .iter()
        .copied()
        .filter(|&byte| byte != b' ')
        .take(2);
    if !first_card.starts_with(b"SIMPLE") || !first_value.eq(*b"=T") {
        return Err(unable_to_open_image(name));
    }
    let tight_value = first_card[8] == b'=' && first_card[9] != b' ';
    Ok((Some(length), tight_value))
}

pub(super) fn unable_to_open_image(path: &Path) -> ImageError {
    ImageError::CasaSelection(format!(
        "Exception: Unable to open image {}.",
        path.display()
    ))
}

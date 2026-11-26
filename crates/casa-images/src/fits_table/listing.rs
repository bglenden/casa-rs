// SPDX-License-Identifier: LGPL-3.0-or-later
//! Read-only FITS listings for `ms.listfits`.

mod samples;

use std::ffi::CStr;
use std::fmt::Write;
use std::ptr::{self, NonNull};

use casa_types::ScalarValue;
use fitsio::sys;

use super::{
    FitsTableError, Reader, Result,
    cards::{Card, Header},
    fits_status,
};

/// A message in a FITS listing. Callers supply their own log sink.
#[derive(std::fmt::Debug, PartialEq, Eq)]
pub enum FitsListingMessage {
    /// HDU headers and samples.
    Info(String),
    /// HDU types, dimensions and column descriptions.
    Debug(String),
    /// Recoverable read diagnostics.
    Warning(String),
    /// Traversal stopped because the input cannot be read.
    Error(String),
}

use FitsListingMessage::{Debug, Error, Info, Warning};

/// List a FITS file's HDU headers and a bounded preview of their data.
///
/// The listing samples the first two random groups, the first 60 pixels of
/// each axis of a 2-D image, and the first five rows and 20 columns of each
/// table. Read errors are part of the listing, including when the file cannot
/// be opened. The input file is opened read-only and is never converted.
pub fn list_fits(path: &CStr) -> Vec<FitsListingMessage> {
    let mut messages = vec![Info(format!("read fitsfile={}", path.to_string_lossy()))];
    let file = match ListingFile::open(path) {
        Ok(file) => file,
        Err(error) => {
            messages.push(Error(format!("Open file error! {error}")));
            messages.push(Warning("Error opening FITS input.".into()));
            messages.push(Error("Too many errors.  Processing terminated.".into()));
            return messages;
        }
    };
    messages.push(Debug("FitsInput object created ok.".into()));
    match visit_hdus(file.0.as_ptr(), &mut messages) {
        Ok(()) => messages.push(Info("finish processing of Header-Data Units.".into())),
        Err(error) => {
            messages.push(Warning(format!("Bad Record encountered: {error}")));
            messages.push(Error("Too many errors.  Processing terminated.".into()));
        }
    }
    messages
}

// The FITS library accepts byte filenames. fitsio::FitsFile expects paths
// to be UTF-8 even when the platform supports other filename encodings.
struct ListingFile(NonNull<sys::fitsfile>);

impl ListingFile {
    fn open(path: &CStr) -> Result<Self> {
        if path.is_empty() {
            return Err(FitsTableError::Fits("No filename was specified".into()));
        }
        let (mut ptr, mut status) = (ptr::null_mut(), 0);
        // SAFETY: The filename is NUL-terminated. The output pointers are live.
        unsafe { sys::ffopen(&mut ptr, path.as_ptr(), sys::READONLY as i32, &mut status) };
        fits_status(status)?;
        NonNull::new(ptr)
            .map(Self)
            .ok_or_else(|| FitsTableError::Fits("Missing FITS handle".into()))
    }
}

impl Drop for ListingFile {
    fn drop(&mut self) {
        let mut status = 0;
        // SAFETY: This is the exclusively owned handle from a successful open.
        unsafe { sys::ffclos(self.0.as_ptr(), &mut status) };
    }
}

fn visit_hdus(ptr: *mut sys::fitsfile, messages: &mut Vec<FitsListingMessage>) -> Result<()> {
    for index in 1.. {
        let (mut kind, mut status) = (0, 0);
        // SAFETY: The FITS handle is live and the outputs are writable.
        unsafe { sys::ffmahd(ptr, index, &mut kind, &mut status) };
        if status == sys::END_OF_FILE as i32 {
            break;
        }
        fits_status(status)?;
        let mut reader = Reader {
            ptr,
            ascii_table: kind as u32 == sys::ASCII_TBL,
        };
        list_hdu(&mut reader, index == 1, messages)?;
    }
    Ok(())
}

#[derive(Clone, Copy)]
enum HduKind {
    PrimaryArray,
    PrimaryGroup,
    PrimaryTable,
    ImageExtension,
    BinaryTable,
    AsciiTable,
    UnknownExtension,
}

impl HduKind {
    fn from_header(header: &Header, primary: bool) -> Self {
        if primary {
            return match (
                integer(header, "NAXIS"),
                integer(header, "NAXIS1"),
                integer(header, "NAXIS2"),
            ) {
                (2, 777777701, 0) => Self::PrimaryTable,
                (1.., 0, _) => Self::PrimaryGroup,
                _ => Self::PrimaryArray,
            };
        }
        match header.text("XTENSION").map(str::trim_end) {
            Some("IMAGE") => Self::ImageExtension,
            Some("BINTABLE" | "A3DTABLE") => Self::BinaryTable,
            Some("TABLE") => Self::AsciiTable,
            _ => Self::UnknownExtension,
        }
    }

    fn name(self) -> &'static str {
        match self {
            Self::PrimaryArray => "Primary Array HDU",
            Self::PrimaryGroup => "Primary Group HDU",
            Self::PrimaryTable => "Primary Table HDU",
            Self::ImageExtension => "Image Extension HDU",
            Self::BinaryTable => "Binary Table HDU",
            Self::AsciiTable => "Ascii Table HDU",
            Self::UnknownExtension => "Unknown Extension HDU",
        }
    }
}

fn list_hdu(
    reader: &mut Reader,
    primary: bool,
    messages: &mut Vec<FitsListingMessage>,
) -> Result<()> {
    let mut display = String::new();
    let header = Header::visit(reader, |raw, card| display_card(&mut display, raw, card))?;
    let kind = HduKind::from_header(&header, primary);
    let bitpix = header.number("BITPIX").unwrap_or(0.0) as i32;
    let data_type = match bitpix {
        8 => "BYTE",
        16 => "SHORT",
        32 => "LONG",
        -32 => "FLOAT",
        -64 => "DOUBLE",
        _ => {
            if primary {
                messages.push(Warning("Error reading initial record -- exiting.".into()));
            }
            return Err(FitsTableError::Fits("Invalid value of BITPIX".into()));
        }
    };
    let name = kind.name();
    messages.push(Info(format!("{name} ------>>>")));
    if !matches!(kind, HduKind::UnknownExtension) {
        let axes = (1..=integer(&header, "NAXIS"))
            .map(|axis| integer(&header, &format!("NAXIS{axis}")))
            .collect::<Vec<_>>();
        let bytes = data_size(&header, &axes, kind, bitpix);
        messages.push(Debug(format!(
            "Data type   {data_type}\nData size   {bytes}\nDimensions  {}",
            axes.len()
        )));
        for (axis, size) in axes.iter().enumerate() {
            messages.push(Debug(format!("Axis {} size {size}", axis + 1)));
        }
        display.push_str("END\n");
        messages.push(Info(display));
        match kind {
            HduKind::PrimaryArray | HduKind::ImageExtension => {
                samples::image(reader, &axes, messages)?
            }
            HduKind::PrimaryGroup => samples::groups(reader, &header, &axes, messages)?,
            HduKind::BinaryTable | HduKind::AsciiTable => {
                samples::table(reader, &header, &axes, bytes, messages)?
            }
            HduKind::PrimaryTable => {}
            HduKind::UnknownExtension => unreachable!("handled above"),
        }
    }
    messages.push(Info(format!("<<<------ {name}")));
    Ok(())
}

fn data_size(header: &Header, axes: &[i64], kind: HduKind, bitpix: i32) -> u64 {
    let axes = if matches!(kind, HduKind::PrimaryGroup) {
        &axes[axes.len().min(1)..]
    } else {
        axes
    };
    let count = axes.first().map_or(0, |_| {
        axes.iter()
            .fold(1u64, |n, &axis| n.saturating_mul(axis as u64))
    });
    let parameters = integer(header, "PCOUNT") as u64;
    let groups = header.number("GCOUNT").unwrap_or(1.0) as u64;
    count
        .saturating_add(parameters)
        .saturating_mul(groups)
        .saturating_mul(u64::from(bitpix.unsigned_abs() / 8))
}

fn integer(header: &Header, key: &str) -> i64 {
    header.number(key).unwrap_or(0.0).max(0.0) as i64
}

fn display_card(out: &mut String, raw: &str, card: Option<&Card>) {
    let Some(Card {
        name,
        scalar: Some(value),
        comment,
    }) = card
    else {
        let _ = writeln!(out, "{}", if raw.is_empty() { "        " } else { raw });
        return;
    };
    let value = match value {
        ScalarValue::String(value) => format!("'{value:<8}'"),
        ScalarValue::Bool(value) => format!("{:>22}", if *value { "T" } else { "F" }),
        ScalarValue::Int32(value) => format!("{value:>22}"),
        ScalarValue::Float32(value) => format!("{:>22}", real(f64::from(*value))),
        ScalarValue::Float64(value) => format!("{:>22}", real(*value)),
        _ => unreachable!("the card parser only yields FITS header values"),
    };
    let _ = write!(out, "{name:<8}= {value}");
    if !comment.is_empty() {
        let _ = write!(out, " /{comment}");
    }
    out.push('\n');
}

// FITSReader uses the default six significant digits of a C++ stream.
pub(super) fn real(value: f64) -> String {
    if value.is_nan() {
        return if value.is_sign_negative() {
            "-nan"
        } else {
            "nan"
        }
        .into();
    }
    if !value.is_finite() {
        return value.to_string();
    }
    let scientific = format!("{value:.5e}");
    let (significand, exponent) = scientific.split_once('e').expect("scientific format");
    let exponent: i32 = exponent.parse().expect("formatted integer exponent");
    if !(-4..6).contains(&exponent) {
        format!("{}e{exponent:+03}", trim_decimal(significand))
    } else {
        let decimals = (5 - exponent) as usize;
        trim_decimal(&format!("{value:.decimals$}")).into()
    }
}

fn trim_decimal(text: &str) -> &str {
    text.split_once('.')
        .map_or(text, |_| text.trim_end_matches('0').trim_end_matches('.'))
}

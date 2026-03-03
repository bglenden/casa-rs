// SPDX-License-Identifier: LGPL-3.0-or-later
//! AipsIO object stream API.
//!
//! This module mirrors casacore C++ `AipsIO` object-framing semantics:
//! `putstart`/`putend`, `get_next_type`/`getstart`/`getend`, primitive
//! scalar/array put-get functions, dynamic `put_value`/`get_value` (including
//! recursive records), and `getnew` allocation helpers.
//!
//! The API is intentionally explicit and strongly typed rather than relying
//! on C++ stream operators.
//!
//! # Purpose
//!
//! [`AipsIo`] is the object-persistence framing layer. It provides:
//!
//! - object boundaries and type/version headers (`putstart`/`putend`,
//!   `getstart`/`getend`)
//! - primitive scalar and linear-array codecs
//! - dynamic typed-value codecs (`Value`) including recursive records and N-D
//!   arrays
//! - file open/reopen/close semantics similar to casacore
//!   `ByteIO::OpenOption`
//!
//! # Object Framing
//!
//! Each top-level object is written as:
//!
//! - top-level magic (`0xbebebebe`)
//! - object header:
//!   - object-length slot (backpatched at `putend`)
//!   - type string
//!   - version (`u32`)
//! - object payload (scalars, arrays, nested objects, records, etc.)
//!
//! Nested `putstart`/`putend` are supported and contribute to parent length.
//!
//! Read-side invariants:
//!
//! - `get_next_type` peeks/caches the next object type
//! - `getstart(expected_type)` validates the type and returns version
//! - `getend()` verifies full object consumption and returns encoded length
//!
//! # Construction and Ownership
//!
//! Constructors:
//!
//! - [`AipsIo::new_read_write`]
//! - [`AipsIo::new_read_only`]
//! - [`AipsIo::new_write_only`]
//!
//! All constructors currently require `inner: Read + Write + Seek + Any` to
//! preserve one type shape for in-memory and file-backed usage.
//!
//! Ownership helpers:
//!
//! - [`AipsIo::into_inner`] returns `Option<Box<dyn AipsIoStream>>`
//! - [`AipsIo::into_inner_typed`] downcasts to a concrete stream type
//!
//! # Stream State and Positioning
//!
//! - [`AipsIo::is_open`] reports stream presence
//! - [`AipsIo::close`] closes/reset state and removes scratch/delete files
//! - [`AipsIo::getpos`] returns current offset
//! - [`AipsIo::setpos`] repositions only when not inside an object (`level=0`)
//!
//! # File-Backed API
//!
//! - [`AipsIo::open`]
//! - [`AipsIo::open_file`]
//! - [`AipsIo::reopen`]
//! - [`AipsIo::file_option`]
//!
//! [`AipsOpenOption`] modes:
//!
//! - `Old`: read-only existing file
//! - `New`: create/truncate read-write
//! - `NewNoReplace`: create-new read-write; fail if exists
//! - `Scratch`: create/truncate read-write and delete on close
//! - `Delete`: open existing read-write and delete on close
//! - `Update`: open existing read-write
//! - `Append`: open/create read-write and seek to end once on open
//!
//! `Append` intentionally does not use OS append-only writes; AipsIO requires
//! in-place backpatching of object-length headers.
//!
//! # Scalar API
//!
//! Write one value:
//!
//! - `put_bool`
//! - `put_i8`, `put_u8`
//! - `put_i16`, `put_u16`
//! - `put_i32`, `put_u32`
//! - `put_i64`, `put_u64`
//! - `put_f32`, `put_f64`
//! - `put_complex32`, `put_complex64`
//! - `put_string`
//!
//! Read one value:
//!
//! - `get_bool`
//! - `get_i8`, `get_u8`
//! - `get_i16`, `get_u16`
//! - `get_i32`, `get_u32`
//! - `get_i64`, `get_u64`
//! - `get_f32`, `get_f64`
//! - `get_complex32`, `get_complex64`
//! - `get_string`
//!
//! Encoding notes:
//!
//! - numeric primitives use big-endian canonical encoding
//! - string encodes `u32` byte length then UTF-8 bytes
//! - booleans encode as byte `0`/`1`
//!
//! # Linear Array API (`put`/`get`/`getnew` equivalents)
//!
//! Write slices:
//!
//! - `put_bool_slice`, `put_i8_slice`, `put_u8_slice`, ...
//! - each slice writer takes `put_nr: bool`:
//!   - `true`: write element count (`u32`) first
//!   - `false`: write raw element sequence only
//!
//! Read into caller-provided buffers (`get` equivalent):
//!
//! - `get_bool_into`, `get_i8_into`, `get_u8_into`, ...
//! - caller supplies exact expected element count
//!
//! Read newly allocated arrays (`getnew` equivalent):
//!
//! - `getnew_bool`, `getnew_i8`, `getnew_u8`, ...
//! - reads count prefix, allocates, fills `Vec<T>`
//!
//! # Dynamic Values (`Value`)
//!
//! - [`AipsIo::put_value`]
//! - [`AipsIo::get_value`]
//!
//! Supported dynamic kinds:
//!
//! - `Value::Scalar(ScalarValue)`
//! - `Value::Array(ArrayValue)` (`ndarray::ArrayD<T>`, any rank)
//! - `Value::Record(RecordValue)` (recursive)
//!
//! Dynamic encoding includes:
//!
//! - value-kind tag (`scalar|array|record`)
//! - primitive tag for scalar/array kinds
//! - for arrays: rank + shape + linear payload
//! - for records: field count + `(field_name, field_value)` pairs
//!
//! # Array Rank and Layout Notes
//!
//! Primitive slice APIs (`put_*_slice`/`get_*_into`/`getnew_*`) encode linear
//! arrays only; rank is not stored.
//!
//! For `Value::Array`, rank and shape are encoded and reconstructed.
//!
//! `AipsIo` treats array payload as a linear sequence. Cross-language tests can
//! adapt linearization order (Fortran vs C memory order) in test shims without
//! changing this public API.
//!
//! # Error Model
//!
//! The API returns [`AipsIoObjectResult`].
//! Notable [`AipsIoObjectError`] categories:
//!
//! - framing/state: `PutStartUnavailable`, `NoPutStart`, `NoGetStart`,
//!   `GetNextTypeUnavailable`, `IncompleteObjectRead`
//! - type/format: `ObjectTypeMismatch`, `NoMagicValueFound`,
//!   `InvalidValueKindTag`, `InvalidPrimitiveTypeTag`
//! - bounds/shape: `ReadBeyondEndOfObject`, `LengthOverflow`,
//!   `DimensionOverflow`, `InvalidArrayShape`
//! - stream lifecycle: `AlreadyOpen`, `NotOpen`, `StreamTypeMismatch`
//! - wrapped I/O/UTF-8 errors
//!
//! # C++ Mapping Summary
//!
//! Close equivalents to casacore `AipsIO`:
//!
//! - `putstart` / `putend`
//! - `getNextType` (`get_next_type`) / `getstart` / `getend`
//! - scalar put/get families
//! - array `put`/`get`/`getnew` families (slice/into/new-vec in Rust)
//! - `getpos` / `setpos`
//! - open/close/reopen/file option support
//!
//! Rust differences:
//!
//! - explicit methods instead of `<<` / `>>` operators
//! - safe ownership (`Vec<T>`, `String`, `ArrayD<T>`) instead of raw pointers
//! - stream abstraction is `Read + Write + Seek`
//! - dynamic typed-value API is first-class

use std::any::Any;
use std::fs::{File, OpenOptions, remove_file};
use std::io::{ErrorKind, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

use ndarray::{ArrayD, IxDyn};
use thiserror::Error;

use crate::{
    ArrayValue, ByteOrder, Complex32, Complex64, PrimitiveType, RecordField, RecordValue,
    ScalarValue, Value,
};

pub type AipsIoObjectResult<T> = Result<T, AipsIoObjectError>;

const MAGIC_VALUE: u32 = 0xbebebebe;
const VALUE_KIND_SCALAR: u8 = 0;
const VALUE_KIND_ARRAY: u8 = 1;
const VALUE_KIND_RECORD: u8 = 2;

/// Object-safe byte stream used as the backing store for [`AipsIo`].
///
/// Any type implementing `Read + Write + Seek + Any` automatically satisfies
/// this trait. In practice the two common implementors are `std::io::Cursor<Vec<u8>>`
/// (in-memory) and `std::fs::File` (file-backed). The `into_any` method
/// enables downcasting back to the concrete type via [`AipsIo::into_inner_typed`].
pub trait AipsIoStream: Read + Write + Seek + Any {
    /// Convert the boxed stream into a `Box<dyn Any>` for downcasting.
    fn into_any(self: Box<Self>) -> Box<dyn Any>;
}

impl<T: Read + Write + Seek + Any> AipsIoStream for T {
    fn into_any(self: Box<Self>) -> Box<dyn Any> {
        self
    }
}

/// File open mode for [`AipsIo`], matching casacore `ByteIO::OpenOption`.
///
/// Passed to [`AipsIo::open`] and [`AipsIo::open_file`] to control how the
/// underlying file is opened. The semantics mirror the C++ `ByteIO::OpenOption`
/// enum. Note that `Append` does not use OS append-only writes; AipsIO needs
/// random-write access so it can back-patch object-length headers.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AipsOpenOption {
    /// Open an existing file for reading only (`putstart` is not allowed).
    Old,
    /// Create or truncate a file for read/write access.
    New,
    /// Create a new file for read/write access; fails if the file already exists.
    NewNoReplace,
    /// Create/truncate a temporary file for read/write access; deleted on close.
    Scratch,
    /// Open an existing file for read/write access and delete it on close.
    Delete,
    /// Open an existing file for in-place read/write updates.
    Update,
    /// Open or create a file for read/write access, seeking to the end once on open.
    Append,
}

#[derive(Debug, Clone)]
struct FileState {
    path: PathBuf,
    option: AipsOpenOption,
    delete_on_close: bool,
}

/// Errors from [`AipsIo`] object-framing operations.
///
/// These cover three broad categories:
///
/// - **Framing / state** — calling put/get methods in the wrong order or when
///   the stream is not open.
/// - **Type / format** — magic values missing, type-string mismatch on
///   [`AipsIo::getstart`], unrecognised wire tags.
/// - **Bounds / shape** — reading past the end of an object, or array shapes
///   that cannot be represented on the current platform.
///
/// I/O and UTF-8 errors from the underlying stream are wrapped and propagated
/// transparently.
#[derive(Debug, Error)]
pub enum AipsIoObjectError {
    /// A low-level I/O error from the underlying stream.
    #[error("i/o error: {0}")]
    Io(#[from] std::io::Error),
    /// The stream contained a byte sequence that is not valid UTF-8 when
    /// decoding a string field.
    #[error("utf-8 decode error: {0}")]
    Utf8(#[from] std::string::FromUtf8Error),
    /// [`AipsIo::putstart`] was called when the stream is not open or is
    /// read-only.
    #[error("AipsIO::putstart: not open or not writable")]
    PutStartUnavailable,
    /// [`AipsIo::get_next_type`] was called when the stream is not open or is
    /// write-only.
    #[error("AipsIO::getNextType: not opened or not readable")]
    GetNextTypeUnavailable,
    /// A scalar or array put method was called outside a
    /// [`putstart`](AipsIo::putstart)/[`putend`](AipsIo::putend) block.
    #[error("AipsIO: no putstart done")]
    NoPutStart,
    /// A scalar or array get method was called outside a
    /// [`getstart`](AipsIo::getstart)/[`getend`](AipsIo::getend) block.
    #[error("AipsIO: no getstart done")]
    NoGetStart,
    /// Reading would exceed the declared length of the current object.
    #[error("AipsIO: read beyond end of object")]
    ReadBeyondEndOfObject,
    /// [`AipsIo::get_next_type`] did not find the expected top-level magic
    /// value (`0xbebebebe`) at the current stream position.
    #[error("AipsIO::getNextType: no magic value found")]
    NoMagicValueFound,
    /// [`AipsIo::getstart`] found a different object type than the caller
    /// expected.
    #[error("AipsIO::getstart: found object type {found}, expected {expected}")]
    ObjectTypeMismatch { found: String, expected: String },
    /// [`AipsIo::getend`] detected that not all bytes of the object were
    /// consumed.
    #[error("AipsIO::getend: part of object not read")]
    IncompleteObjectRead,
    /// [`AipsIo::setpos`] was called while inside an object
    /// (nesting level > 0).
    #[error("AipsIO::setpos cannot be done while accessing objects")]
    SetPosWhileAccessingObjects,
    /// [`AipsIo::open_file`] was called on an instance that already has an
    /// open stream.
    #[error("AipsIO: already open")]
    AlreadyOpen,
    /// A method requiring an open stream was called after the stream was
    /// closed.
    #[error("AipsIO: not open")]
    NotOpen,
    /// [`AipsIo::into_inner_typed`] was called with a concrete type that does
    /// not match the actual underlying stream type.
    #[error("AipsIO: underlying stream type mismatch (expected {expected})")]
    StreamTypeMismatch { expected: &'static str },
    /// An object-length or count value overflowed a `u32`.
    #[error("AipsIO length overflow")]
    LengthOverflow,
    /// The wire-encoded value-kind tag byte did not match any known variant.
    #[error("invalid value kind tag {0}")]
    InvalidValueKindTag(u8),
    /// The wire-encoded primitive-type tag byte did not match any known variant.
    #[error("invalid primitive type tag {0}")]
    InvalidPrimitiveTypeTag(u8),
    /// An array dimension read from the stream cannot be converted to
    /// `usize` on this platform.
    #[error("array dimension {0} cannot be represented on this platform")]
    DimensionOverflow(u64),
    /// The product of the array shape dimensions does not match the number of
    /// elements in the payload.
    #[error("invalid array shape {shape:?} for payload length {len}")]
    InvalidArrayShape { shape: Vec<usize>, len: usize },
}

/// Stateful AipsIO object reader/writer over an arbitrary seekable stream.
///
/// This is the Rust equivalent of C++ `casacore::AipsIO`. It wraps any
/// [`AipsIoStream`] (file, in-memory cursor, etc.) and provides:
///
/// - object boundaries with type-checking and versioning via
///   [`putstart`](Self::putstart)/[`putend`](Self::putend) and
///   [`getstart`](Self::getstart)/[`getend`](Self::getend);
/// - strongly-typed scalar and linear-array codecs;
/// - a dynamic [`Value`] codec that includes recursive records
///   and N-D arrays.
///
/// # Usage pattern
///
/// ```rust
/// use casacore_aipsio::AipsIo;
/// use std::io::Cursor;
///
/// let mut io = AipsIo::new_read_write(Cursor::new(Vec::<u8>::new()));
/// io.putstart("MyObject", 1).unwrap();
/// io.put_i32(42).unwrap();
/// io.putend().unwrap();
/// ```
///
/// See the [module-level documentation](super) for a full description of
/// the wire format, error model, and C++ mapping.
pub struct AipsIo {
    inner: Option<Box<dyn AipsIoStream>>,
    byte_order: ByteOrder,
    /// Write switch: -1 = write not available, 0 = idle, 1 = inside object.
    swput: i32,
    /// Read switch: -1 = read not available, 0 = idle, 1 = inside object.
    swget: i32,
    /// Current object nesting depth (0 = between top-level objects).
    level: usize,
    /// Bytes written at each nesting level; back-patched by `putend`.
    objlen: Vec<u32>,
    /// Total declared length at each nesting level; checked by `getend`.
    objtln: Vec<u32>,
    /// Stream offset of the length slot for each nesting level.
    objptr: Vec<u64>,
    /// Whether `get_next_type` has already read and cached the next type name.
    has_cached_type: bool,
    /// Cached type name from the most recent `get_next_type` call.
    object_type: String,
    seekable: bool,
    file_state: Option<FileState>,
}

impl AipsIo {
    /// Construct a stream-backed `AipsIo` with read/write access (big-endian).
    pub fn new_read_write<S: Read + Write + Seek + Any>(inner: S) -> Self {
        Self::with_access(Box::new(inner), true, true, true, ByteOrder::BigEndian)
    }

    /// Construct a stream-backed `AipsIo` with read-only access (big-endian).
    pub fn new_read_only<S: Read + Write + Seek + Any>(inner: S) -> Self {
        Self::with_access(Box::new(inner), true, false, true, ByteOrder::BigEndian)
    }

    /// Construct a stream-backed `AipsIo` with write-only access (big-endian).
    pub fn new_write_only<S: Read + Write + Seek + Any>(inner: S) -> Self {
        Self::with_access(Box::new(inner), false, true, true, ByteOrder::BigEndian)
    }

    /// Construct with read-only access and explicit byte order.
    pub fn new_read_only_with_order<S: Read + Write + Seek + Any>(
        inner: S,
        order: ByteOrder,
    ) -> Self {
        Self::with_access(Box::new(inner), true, false, true, order)
    }

    /// Construct with write-only access and explicit byte order.
    pub fn new_write_only_with_order<S: Read + Write + Seek + Any>(
        inner: S,
        order: ByteOrder,
    ) -> Self {
        Self::with_access(Box::new(inner), false, true, true, order)
    }

    /// Return the byte order used for encoding/decoding.
    pub fn byte_order(&self) -> ByteOrder {
        self.byte_order
    }

    /// Consume and return the underlying stream if still open.
    pub fn into_inner(self) -> Option<Box<dyn AipsIoStream>> {
        self.inner
    }

    /// Consume and return the underlying stream as a concrete type.
    pub fn into_inner_typed<S: Read + Write + Seek + Any>(self) -> AipsIoObjectResult<S> {
        let inner = self.inner.ok_or(AipsIoObjectError::NotOpen)?;
        let any = inner.into_any();
        any.downcast::<S>()
            .map(|boxed| *boxed)
            .map_err(|_| AipsIoObjectError::StreamTypeMismatch {
                expected: std::any::type_name::<S>(),
            })
    }

    /// Return `true` when an underlying stream is open.
    pub fn is_open(&self) -> bool {
        self.inner.is_some()
    }

    /// Close the stream and reset object nesting state.
    ///
    /// For `Scratch` and `Delete` file options this also removes the file.
    pub fn close(&mut self) -> AipsIoObjectResult<()> {
        if self.inner.is_none() {
            return Ok(());
        }
        self.inner = None;
        self.swput = -1;
        self.swget = -1;
        self.level = 0;
        self.has_cached_type = false;

        if let Some(state) = self.file_state.take() {
            if state.delete_on_close {
                let _ = remove_file(state.path);
            }
        }
        Ok(())
    }

    /// Return the file open option when created via [`AipsIo::open`].
    pub fn file_option(&self) -> Option<AipsOpenOption> {
        self.file_state.as_ref().map(|state| state.option)
    }

    /// Return the current byte offset in the underlying stream.
    ///
    /// Mirrors C++ `AipsIO::getpos`. May be called at any nesting level.
    pub fn getpos(&mut self) -> AipsIoObjectResult<u64> {
        let inner = self.inner_mut()?;
        Ok(inner.stream_position()?)
    }

    /// Seek to the given byte offset in the underlying stream.
    ///
    /// Mirrors C++ `AipsIO::setpos`. Returns
    /// [`SetPosWhileAccessingObjects`](AipsIoObjectError::SetPosWhileAccessingObjects)
    /// if called while inside an object (nesting level > 0).
    pub fn setpos(&mut self, offset: u64) -> AipsIoObjectResult<u64> {
        if self.level != 0 {
            return Err(AipsIoObjectError::SetPosWhileAccessingObjects);
        }
        let inner = self.inner_mut()?;
        Ok(inner.seek(SeekFrom::Start(offset))?)
    }

    /// Begin writing an object with type and version header.
    pub fn putstart(&mut self, object_type: &str, object_version: u32) -> AipsIoObjectResult<u32> {
        if self.swput < 0 || self.swget > 0 {
            return Err(AipsIoObjectError::PutStartUnavailable);
        }
        if self.level == 0 {
            self.swput = 1;
            self.objlen[0] = 0;
            self.put_u32(MAGIC_VALUE)?;
        }
        self.level += 1;
        self.ensure_level();
        self.objlen[self.level] = 0;
        self.objptr[self.level] = self.getpos()?;
        self.put_u32(MAGIC_VALUE)?;
        self.put_string(object_type)?;
        self.put_u32(object_version)?;
        Ok(self.level as u32)
    }

    /// Finish writing the current object and return its encoded length.
    pub fn putend(&mut self) -> AipsIoObjectResult<u32> {
        if self.level == 0 {
            return Err(AipsIoObjectError::NoPutStart);
        }
        let len = self.objlen[self.level];
        if self.seekable {
            let pos = self.getpos()?;
            let objptr = self.objptr[self.level];
            {
                let inner = self.inner_mut()?;
                inner.seek(SeekFrom::Start(objptr))?;
            }
            self.write_u32_raw(len)?;
            let inner = self.inner_mut()?;
            inner.seek(SeekFrom::Start(pos))?;
        }
        self.level -= 1;
        if self.level == 0 {
            self.swput = 0;
        } else {
            self.objlen[self.level] = self.objlen[self.level]
                .checked_add(len)
                .ok_or(AipsIoObjectError::LengthOverflow)?;
        }
        Ok(len)
    }

    /// Inspect the type name of the next object in the stream.
    pub fn get_next_type(&mut self) -> AipsIoObjectResult<String> {
        if self.swget < 0 || self.swput > 0 {
            return Err(AipsIoObjectError::GetNextTypeUnavailable);
        }
        if self.has_cached_type {
            return Ok(self.object_type.clone());
        }

        let swget_old = self.swget;
        if self.level == 0 {
            self.swget = 1;
            self.objlen[0] = 0;
            let mval = self.get_u32()?;
            if mval != MAGIC_VALUE {
                return Err(AipsIoObjectError::NoMagicValueFound);
            }
        }
        self.level += 1;
        self.ensure_level();
        self.objlen[self.level] = 0;
        self.objtln[self.level] = 16;
        let object_len = self.get_u32()?;
        self.objtln[self.level] = object_len;
        self.object_type = self.get_string()?;
        self.swget = swget_old;
        self.has_cached_type = true;
        Ok(self.object_type.clone())
    }

    /// Begin reading an object and return its version.
    pub fn getstart(&mut self, expected_type: &str) -> AipsIoObjectResult<u32> {
        let found = self.get_next_type()?;
        if found != expected_type {
            return Err(AipsIoObjectError::ObjectTypeMismatch {
                found,
                expected: expected_type.to_string(),
            });
        }
        self.swget = 1;
        self.has_cached_type = false;
        self.get_u32()
    }

    /// Finish reading the current object, validating full consumption.
    pub fn getend(&mut self) -> AipsIoObjectResult<u32> {
        if self.level == 0 {
            return Err(AipsIoObjectError::NoGetStart);
        }
        let len = self.objlen[self.level];
        if len != self.objtln[self.level] && self.objtln[self.level] != MAGIC_VALUE {
            return Err(AipsIoObjectError::IncompleteObjectRead);
        }
        self.level -= 1;
        if self.level == 0 {
            self.swget = 0;
        } else {
            self.objlen[self.level] = self.objlen[self.level]
                .checked_add(len)
                .ok_or(AipsIoObjectError::LengthOverflow)?;
        }
        Ok(len)
    }

    /// Write a boolean value in canonical format (1 byte: `0` or `1`).
    ///
    /// Must be called inside a [`putstart`](Self::putstart)/[`putend`](Self::putend) block.
    pub fn put_bool(&mut self, value: bool) -> AipsIoObjectResult<()> {
        self.put_u8(u8::from(value))
    }

    /// Write a signed 8-bit integer value in canonical format (1 byte).
    ///
    /// Must be called inside a [`putstart`](Self::putstart)/[`putend`](Self::putend)
    /// block. All other scalar `put_*` methods follow the same semantics.
    pub fn put_i8(&mut self, value: i8) -> AipsIoObjectResult<()> {
        self.test_put()?;
        self.write_counted(&[value as u8])
    }

    /// Write an unsigned 8-bit integer value (1 byte). See [`put_i8`](Self::put_i8).
    pub fn put_u8(&mut self, value: u8) -> AipsIoObjectResult<()> {
        self.test_put()?;
        self.write_counted(&[value])
    }

    /// Write a signed 16-bit integer in canonical byte order (2 bytes). See [`put_i8`](Self::put_i8).
    pub fn put_i16(&mut self, value: i16) -> AipsIoObjectResult<()> {
        self.test_put()?;
        let bytes = self.encode_i16(value);
        self.write_counted(&bytes)
    }

    /// Write an unsigned 16-bit integer in canonical byte order (2 bytes). See [`put_i8`](Self::put_i8).
    pub fn put_u16(&mut self, value: u16) -> AipsIoObjectResult<()> {
        self.test_put()?;
        let bytes = self.encode_u16(value);
        self.write_counted(&bytes)
    }

    /// Write a signed 32-bit integer in canonical byte order (4 bytes). See [`put_i8`](Self::put_i8).
    pub fn put_i32(&mut self, value: i32) -> AipsIoObjectResult<()> {
        self.test_put()?;
        let bytes = self.encode_i32(value);
        self.write_counted(&bytes)
    }

    /// Write an unsigned 32-bit integer in canonical byte order (4 bytes). See [`put_i8`](Self::put_i8).
    pub fn put_u32(&mut self, value: u32) -> AipsIoObjectResult<()> {
        self.test_put()?;
        let bytes = self.encode_u32(value);
        self.write_counted(&bytes)
    }

    /// Write a signed 64-bit integer in canonical byte order (8 bytes). See [`put_i8`](Self::put_i8).
    pub fn put_i64(&mut self, value: i64) -> AipsIoObjectResult<()> {
        self.test_put()?;
        let bytes = self.encode_i64(value);
        self.write_counted(&bytes)
    }

    /// Write an unsigned 64-bit integer in canonical byte order (8 bytes). See [`put_i8`](Self::put_i8).
    pub fn put_u64(&mut self, value: u64) -> AipsIoObjectResult<()> {
        self.test_put()?;
        let bytes = self.encode_u64(value);
        self.write_counted(&bytes)
    }

    /// Write a 32-bit float as its IEEE 754 bit pattern (4 bytes). See [`put_i8`](Self::put_i8).
    pub fn put_f32(&mut self, value: f32) -> AipsIoObjectResult<()> {
        self.put_u32(value.to_bits())
    }

    /// Write a 64-bit float as its IEEE 754 bit pattern (8 bytes). See [`put_i8`](Self::put_i8).
    pub fn put_f64(&mut self, value: f64) -> AipsIoObjectResult<()> {
        self.put_u64(value.to_bits())
    }

    /// Write a 32-bit complex number as two consecutive `f32` values (8 bytes). See [`put_i8`](Self::put_i8).
    pub fn put_complex32(&mut self, value: Complex32) -> AipsIoObjectResult<()> {
        self.put_f32(value.re)?;
        self.put_f32(value.im)
    }

    /// Write a 64-bit complex number as two consecutive `f64` values (16 bytes). See [`put_i8`](Self::put_i8).
    pub fn put_complex64(&mut self, value: Complex64) -> AipsIoObjectResult<()> {
        self.put_f64(value.re)?;
        self.put_f64(value.im)
    }

    /// Write a UTF-8 string, prefixed by its byte length as a `u32`. See [`put_i8`](Self::put_i8).
    pub fn put_string(&mut self, value: &str) -> AipsIoObjectResult<()> {
        let len = u32::try_from(value.len()).map_err(|_| AipsIoObjectError::LengthOverflow)?;
        self.put_u32(len)?;
        self.test_put()?;
        self.write_counted(value.as_bytes())
    }

    /// Write a slice of boolean values, bit-packed into bytes (1 bit per element).
    ///
    /// When `put_nr` is `true`, the element count is written first as a `u32`,
    /// matching the C++ `AipsIO::put(uInt, const Bool*)` overload. Pass
    /// `put_nr = false` when the count is tracked externally (e.g. inside
    /// [`put_array_value`](Self::put_value) where shape is stored separately).
    /// All other `put_*_slice` methods follow these same semantics.
    pub fn put_bool_slice(&mut self, values: &[bool], put_nr: bool) -> AipsIoObjectResult<()> {
        if put_nr {
            self.put_u32(values.len() as u32)?;
        }
        self.test_put()?;
        let packed = pack_bool_slice(values);
        self.write_counted(&packed)
    }

    /// Write a slice of signed 8-bit integer values.
    ///
    /// When `put_nr` is `true`, the element count is written first as a `u32`.
    /// See [`put_bool_slice`](Self::put_bool_slice) for details.
    pub fn put_i8_slice(&mut self, values: &[i8], put_nr: bool) -> AipsIoObjectResult<()> {
        if put_nr {
            self.put_u32(values.len() as u32)?;
        }
        for &value in values {
            self.put_i8(value)?;
        }
        Ok(())
    }

    /// Write a slice of unsigned 8-bit integer values. See [`put_bool_slice`](Self::put_bool_slice).
    pub fn put_u8_slice(&mut self, values: &[u8], put_nr: bool) -> AipsIoObjectResult<()> {
        if put_nr {
            self.put_u32(values.len() as u32)?;
        }
        self.test_put()?;
        self.write_counted(values)
    }

    /// Write a slice of signed 16-bit integer values. See [`put_bool_slice`](Self::put_bool_slice).
    pub fn put_i16_slice(&mut self, values: &[i16], put_nr: bool) -> AipsIoObjectResult<()> {
        if put_nr {
            self.put_u32(values.len() as u32)?;
        }
        for &value in values {
            self.put_i16(value)?;
        }
        Ok(())
    }

    /// Write a slice of unsigned 16-bit integer values. See [`put_bool_slice`](Self::put_bool_slice).
    pub fn put_u16_slice(&mut self, values: &[u16], put_nr: bool) -> AipsIoObjectResult<()> {
        if put_nr {
            self.put_u32(values.len() as u32)?;
        }
        for &value in values {
            self.put_u16(value)?;
        }
        Ok(())
    }

    /// Write a slice of signed 32-bit integer values. See [`put_bool_slice`](Self::put_bool_slice).
    pub fn put_i32_slice(&mut self, values: &[i32], put_nr: bool) -> AipsIoObjectResult<()> {
        if put_nr {
            self.put_u32(values.len() as u32)?;
        }
        for &value in values {
            self.put_i32(value)?;
        }
        Ok(())
    }

    /// Write a slice of unsigned 32-bit integer values. See [`put_bool_slice`](Self::put_bool_slice).
    pub fn put_u32_slice(&mut self, values: &[u32], put_nr: bool) -> AipsIoObjectResult<()> {
        if put_nr {
            self.put_u32(values.len() as u32)?;
        }
        for &value in values {
            self.put_u32(value)?;
        }
        Ok(())
    }

    /// Write a slice of signed 64-bit integer values. See [`put_bool_slice`](Self::put_bool_slice).
    pub fn put_i64_slice(&mut self, values: &[i64], put_nr: bool) -> AipsIoObjectResult<()> {
        if put_nr {
            self.put_u32(values.len() as u32)?;
        }
        for &value in values {
            self.put_i64(value)?;
        }
        Ok(())
    }

    /// Write a slice of unsigned 64-bit integer values. See [`put_bool_slice`](Self::put_bool_slice).
    pub fn put_u64_slice(&mut self, values: &[u64], put_nr: bool) -> AipsIoObjectResult<()> {
        if put_nr {
            self.put_u32(values.len() as u32)?;
        }
        for &value in values {
            self.put_u64(value)?;
        }
        Ok(())
    }

    /// Write a slice of 32-bit float values. See [`put_bool_slice`](Self::put_bool_slice).
    pub fn put_f32_slice(&mut self, values: &[f32], put_nr: bool) -> AipsIoObjectResult<()> {
        if put_nr {
            self.put_u32(values.len() as u32)?;
        }
        for &value in values {
            self.put_f32(value)?;
        }
        Ok(())
    }

    /// Write a slice of 64-bit float values. See [`put_bool_slice`](Self::put_bool_slice).
    pub fn put_f64_slice(&mut self, values: &[f64], put_nr: bool) -> AipsIoObjectResult<()> {
        if put_nr {
            self.put_u32(values.len() as u32)?;
        }
        for &value in values {
            self.put_f64(value)?;
        }
        Ok(())
    }

    /// Write a slice of 32-bit complex values. See [`put_bool_slice`](Self::put_bool_slice).
    pub fn put_complex32_slice(
        &mut self,
        values: &[Complex32],
        put_nr: bool,
    ) -> AipsIoObjectResult<()> {
        if put_nr {
            self.put_u32(values.len() as u32)?;
        }
        for &value in values {
            self.put_complex32(value)?;
        }
        Ok(())
    }

    /// Write a slice of 64-bit complex values. See [`put_bool_slice`](Self::put_bool_slice).
    pub fn put_complex64_slice(
        &mut self,
        values: &[Complex64],
        put_nr: bool,
    ) -> AipsIoObjectResult<()> {
        if put_nr {
            self.put_u32(values.len() as u32)?;
        }
        for &value in values {
            self.put_complex64(value)?;
        }
        Ok(())
    }

    /// Write a slice of UTF-8 strings, each prefixed by its byte length. See [`put_bool_slice`](Self::put_bool_slice).
    pub fn put_string_slice(&mut self, values: &[String], put_nr: bool) -> AipsIoObjectResult<()> {
        if put_nr {
            self.put_u32(values.len() as u32)?;
        }
        for value in values {
            self.put_string(value)?;
        }
        Ok(())
    }

    /// Read a boolean value in canonical format (1 byte; any non-zero value is `true`).
    ///
    /// Must be called inside a [`getstart`](Self::getstart)/[`getend`](Self::getend) block.
    pub fn get_bool(&mut self) -> AipsIoObjectResult<bool> {
        Ok(self.get_u8()? != 0)
    }

    /// Read a signed 8-bit integer value (1 byte).
    ///
    /// Must be called inside a [`getstart`](Self::getstart)/[`getend`](Self::getend)
    /// block. All other scalar `get_*` methods follow the same semantics.
    pub fn get_i8(&mut self) -> AipsIoObjectResult<i8> {
        self.test_get()?;
        let mut buf = [0_u8; 1];
        self.read_counted(&mut buf)?;
        Ok(buf[0] as i8)
    }

    /// Read an unsigned 8-bit integer value (1 byte). See [`get_i8`](Self::get_i8).
    pub fn get_u8(&mut self) -> AipsIoObjectResult<u8> {
        self.test_get()?;
        let mut buf = [0_u8; 1];
        self.read_counted(&mut buf)?;
        Ok(buf[0])
    }

    /// Read a signed 16-bit integer in canonical byte order (2 bytes). See [`get_i8`](Self::get_i8).
    pub fn get_i16(&mut self) -> AipsIoObjectResult<i16> {
        self.test_get()?;
        let mut buf = [0_u8; 2];
        self.read_counted(&mut buf)?;
        Ok(self.decode_i16(buf))
    }

    /// Read an unsigned 16-bit integer in canonical byte order (2 bytes). See [`get_i8`](Self::get_i8).
    pub fn get_u16(&mut self) -> AipsIoObjectResult<u16> {
        self.test_get()?;
        let mut buf = [0_u8; 2];
        self.read_counted(&mut buf)?;
        Ok(self.decode_u16(buf))
    }

    /// Read a signed 32-bit integer in canonical byte order (4 bytes). See [`get_i8`](Self::get_i8).
    pub fn get_i32(&mut self) -> AipsIoObjectResult<i32> {
        self.test_get()?;
        let mut buf = [0_u8; 4];
        self.read_counted(&mut buf)?;
        Ok(self.decode_i32(buf))
    }

    /// Read an unsigned 32-bit integer in canonical byte order (4 bytes). See [`get_i8`](Self::get_i8).
    pub fn get_u32(&mut self) -> AipsIoObjectResult<u32> {
        self.test_get()?;
        let mut buf = [0_u8; 4];
        self.read_counted(&mut buf)?;
        Ok(self.decode_u32(buf))
    }

    /// Read a signed 64-bit integer in canonical byte order (8 bytes). See [`get_i8`](Self::get_i8).
    pub fn get_i64(&mut self) -> AipsIoObjectResult<i64> {
        self.test_get()?;
        let mut buf = [0_u8; 8];
        self.read_counted(&mut buf)?;
        Ok(self.decode_i64(buf))
    }

    /// Read an unsigned 64-bit integer in canonical byte order (8 bytes). See [`get_i8`](Self::get_i8).
    pub fn get_u64(&mut self) -> AipsIoObjectResult<u64> {
        self.test_get()?;
        let mut buf = [0_u8; 8];
        self.read_counted(&mut buf)?;
        Ok(self.decode_u64(buf))
    }

    /// Read a 32-bit float from its IEEE 754 bit pattern (4 bytes). See [`get_i8`](Self::get_i8).
    pub fn get_f32(&mut self) -> AipsIoObjectResult<f32> {
        Ok(f32::from_bits(self.get_u32()?))
    }

    /// Read a 64-bit float from its IEEE 754 bit pattern (8 bytes). See [`get_i8`](Self::get_i8).
    pub fn get_f64(&mut self) -> AipsIoObjectResult<f64> {
        Ok(f64::from_bits(self.get_u64()?))
    }

    /// Read a 32-bit complex number as two consecutive `f32` values (8 bytes). See [`get_i8`](Self::get_i8).
    pub fn get_complex32(&mut self) -> AipsIoObjectResult<Complex32> {
        let re = self.get_f32()?;
        let im = self.get_f32()?;
        Ok(Complex32::new(re, im))
    }

    /// Read a 64-bit complex number as two consecutive `f64` values (16 bytes). See [`get_i8`](Self::get_i8).
    pub fn get_complex64(&mut self) -> AipsIoObjectResult<Complex64> {
        let re = self.get_f64()?;
        let im = self.get_f64()?;
        Ok(Complex64::new(re, im))
    }

    /// Read a length-prefixed UTF-8 string. See [`get_i8`](Self::get_i8).
    pub fn get_string(&mut self) -> AipsIoObjectResult<String> {
        let len = self.get_u32()? as usize;
        self.test_get()?;
        let mut buf = vec![0_u8; len];
        self.read_counted(&mut buf)?;
        Ok(String::from_utf8(buf)?)
    }

    /// Read exactly `values.len()` boolean values into the provided slice.
    ///
    /// Booleans are stored bit-packed (1 bit per element). The caller must
    /// supply a slice of exactly the right length; no count prefix is read.
    /// This mirrors C++ `AipsIO::get(uInt, Bool*)`. All other `get_*_into`
    /// methods follow the same caller-supplies-length semantics.
    pub fn get_bool_into(&mut self, values: &mut [bool]) -> AipsIoObjectResult<()> {
        self.test_get()?;
        let mut packed = vec![0_u8; values.len().div_ceil(8)];
        self.read_counted(&mut packed)?;
        unpack_bool_slice(&packed, values);
        Ok(())
    }

    /// Read exactly `values.len()` signed 8-bit values into the provided slice.
    ///
    /// See [`get_bool_into`](Self::get_bool_into) for details.
    pub fn get_i8_into(&mut self, values: &mut [i8]) -> AipsIoObjectResult<()> {
        for value in values {
            *value = self.get_i8()?;
        }
        Ok(())
    }

    /// Read exactly `values.len()` unsigned 8-bit values into the provided slice. See [`get_bool_into`](Self::get_bool_into).
    pub fn get_u8_into(&mut self, values: &mut [u8]) -> AipsIoObjectResult<()> {
        self.test_get()?;
        self.read_counted(values)
    }

    /// Read exactly `values.len()` signed 16-bit values into the provided slice. See [`get_bool_into`](Self::get_bool_into).
    pub fn get_i16_into(&mut self, values: &mut [i16]) -> AipsIoObjectResult<()> {
        for value in values {
            *value = self.get_i16()?;
        }
        Ok(())
    }

    /// Read exactly `values.len()` unsigned 16-bit values into the provided slice. See [`get_bool_into`](Self::get_bool_into).
    pub fn get_u16_into(&mut self, values: &mut [u16]) -> AipsIoObjectResult<()> {
        for value in values {
            *value = self.get_u16()?;
        }
        Ok(())
    }

    /// Read exactly `values.len()` signed 32-bit values into the provided slice. See [`get_bool_into`](Self::get_bool_into).
    pub fn get_i32_into(&mut self, values: &mut [i32]) -> AipsIoObjectResult<()> {
        for value in values {
            *value = self.get_i32()?;
        }
        Ok(())
    }

    /// Read exactly `values.len()` unsigned 32-bit values into the provided slice. See [`get_bool_into`](Self::get_bool_into).
    pub fn get_u32_into(&mut self, values: &mut [u32]) -> AipsIoObjectResult<()> {
        for value in values {
            *value = self.get_u32()?;
        }
        Ok(())
    }

    /// Read exactly `values.len()` signed 64-bit values into the provided slice. See [`get_bool_into`](Self::get_bool_into).
    pub fn get_i64_into(&mut self, values: &mut [i64]) -> AipsIoObjectResult<()> {
        for value in values {
            *value = self.get_i64()?;
        }
        Ok(())
    }

    /// Read exactly `values.len()` unsigned 64-bit values into the provided slice. See [`get_bool_into`](Self::get_bool_into).
    pub fn get_u64_into(&mut self, values: &mut [u64]) -> AipsIoObjectResult<()> {
        for value in values {
            *value = self.get_u64()?;
        }
        Ok(())
    }

    /// Read exactly `values.len()` 32-bit float values into the provided slice. See [`get_bool_into`](Self::get_bool_into).
    pub fn get_f32_into(&mut self, values: &mut [f32]) -> AipsIoObjectResult<()> {
        for value in values {
            *value = self.get_f32()?;
        }
        Ok(())
    }

    /// Read exactly `values.len()` 64-bit float values into the provided slice. See [`get_bool_into`](Self::get_bool_into).
    pub fn get_f64_into(&mut self, values: &mut [f64]) -> AipsIoObjectResult<()> {
        for value in values {
            *value = self.get_f64()?;
        }
        Ok(())
    }

    /// Read exactly `values.len()` 32-bit complex values into the provided slice. See [`get_bool_into`](Self::get_bool_into).
    pub fn get_complex32_into(&mut self, values: &mut [Complex32]) -> AipsIoObjectResult<()> {
        for value in values {
            *value = self.get_complex32()?;
        }
        Ok(())
    }

    /// Read exactly `values.len()` 64-bit complex values into the provided slice. See [`get_bool_into`](Self::get_bool_into).
    pub fn get_complex64_into(&mut self, values: &mut [Complex64]) -> AipsIoObjectResult<()> {
        for value in values {
            *value = self.get_complex64()?;
        }
        Ok(())
    }

    /// Read exactly `values.len()` UTF-8 strings into the provided slice. See [`get_bool_into`](Self::get_bool_into).
    pub fn get_string_into(&mut self, values: &mut [String]) -> AipsIoObjectResult<()> {
        for value in values {
            *value = self.get_string()?;
        }
        Ok(())
    }

    /// Read a count-prefixed boolean array into a newly allocated `Vec<bool>`.
    ///
    /// Reads a `u32` element count, allocates, and fills using
    /// [`get_bool_into`](Self::get_bool_into). This is the Rust equivalent of
    /// C++ `AipsIO::getnew(uInt*, Bool**)`. All other `getnew_*` methods
    /// follow the same read-count-then-allocate semantics.
    pub fn getnew_bool(&mut self) -> AipsIoObjectResult<Vec<bool>> {
        let nrv = self.get_u32()? as usize;
        let mut out = vec![false; nrv];
        self.get_bool_into(&mut out)?;
        Ok(out)
    }

    /// Read a count-prefixed signed 8-bit array into a newly allocated `Vec<i8>`.
    ///
    /// See [`getnew_bool`](Self::getnew_bool) for details.
    pub fn getnew_i8(&mut self) -> AipsIoObjectResult<Vec<i8>> {
        let nrv = self.get_u32()? as usize;
        let mut out = vec![0_i8; nrv];
        self.get_i8_into(&mut out)?;
        Ok(out)
    }

    /// Read a count-prefixed unsigned 8-bit array into a newly allocated `Vec<u8>`. See [`getnew_bool`](Self::getnew_bool).
    pub fn getnew_u8(&mut self) -> AipsIoObjectResult<Vec<u8>> {
        let nrv = self.get_u32()? as usize;
        let mut out = vec![0_u8; nrv];
        self.get_u8_into(&mut out)?;
        Ok(out)
    }

    /// Read a count-prefixed signed 16-bit array into a newly allocated `Vec<i16>`. See [`getnew_bool`](Self::getnew_bool).
    pub fn getnew_i16(&mut self) -> AipsIoObjectResult<Vec<i16>> {
        let nrv = self.get_u32()? as usize;
        let mut out = vec![0_i16; nrv];
        self.get_i16_into(&mut out)?;
        Ok(out)
    }

    /// Read a count-prefixed unsigned 16-bit array into a newly allocated `Vec<u16>`. See [`getnew_bool`](Self::getnew_bool).
    pub fn getnew_u16(&mut self) -> AipsIoObjectResult<Vec<u16>> {
        let nrv = self.get_u32()? as usize;
        let mut out = vec![0_u16; nrv];
        self.get_u16_into(&mut out)?;
        Ok(out)
    }

    /// Read a count-prefixed signed 32-bit array into a newly allocated `Vec<i32>`. See [`getnew_bool`](Self::getnew_bool).
    pub fn getnew_i32(&mut self) -> AipsIoObjectResult<Vec<i32>> {
        let nrv = self.get_u32()? as usize;
        let mut out = vec![0_i32; nrv];
        self.get_i32_into(&mut out)?;
        Ok(out)
    }

    /// Read a count-prefixed unsigned 32-bit array into a newly allocated `Vec<u32>`. See [`getnew_bool`](Self::getnew_bool).
    pub fn getnew_u32(&mut self) -> AipsIoObjectResult<Vec<u32>> {
        let nrv = self.get_u32()? as usize;
        let mut out = vec![0_u32; nrv];
        self.get_u32_into(&mut out)?;
        Ok(out)
    }

    /// Read a count-prefixed signed 64-bit array into a newly allocated `Vec<i64>`. See [`getnew_bool`](Self::getnew_bool).
    pub fn getnew_i64(&mut self) -> AipsIoObjectResult<Vec<i64>> {
        let nrv = self.get_u32()? as usize;
        let mut out = vec![0_i64; nrv];
        self.get_i64_into(&mut out)?;
        Ok(out)
    }

    /// Read a count-prefixed unsigned 64-bit array into a newly allocated `Vec<u64>`. See [`getnew_bool`](Self::getnew_bool).
    pub fn getnew_u64(&mut self) -> AipsIoObjectResult<Vec<u64>> {
        let nrv = self.get_u32()? as usize;
        let mut out = vec![0_u64; nrv];
        self.get_u64_into(&mut out)?;
        Ok(out)
    }

    /// Read a count-prefixed 32-bit float array into a newly allocated `Vec<f32>`. See [`getnew_bool`](Self::getnew_bool).
    pub fn getnew_f32(&mut self) -> AipsIoObjectResult<Vec<f32>> {
        let nrv = self.get_u32()? as usize;
        let mut out = vec![0_f32; nrv];
        self.get_f32_into(&mut out)?;
        Ok(out)
    }

    /// Read a count-prefixed 64-bit float array into a newly allocated `Vec<f64>`. See [`getnew_bool`](Self::getnew_bool).
    pub fn getnew_f64(&mut self) -> AipsIoObjectResult<Vec<f64>> {
        let nrv = self.get_u32()? as usize;
        let mut out = vec![0_f64; nrv];
        self.get_f64_into(&mut out)?;
        Ok(out)
    }

    /// Read a count-prefixed 32-bit complex array into a newly allocated `Vec<Complex32>`. See [`getnew_bool`](Self::getnew_bool).
    pub fn getnew_complex32(&mut self) -> AipsIoObjectResult<Vec<Complex32>> {
        let nrv = self.get_u32()? as usize;
        let mut out = vec![Complex32::new(0.0, 0.0); nrv];
        self.get_complex32_into(&mut out)?;
        Ok(out)
    }

    /// Read a count-prefixed 64-bit complex array into a newly allocated `Vec<Complex64>`. See [`getnew_bool`](Self::getnew_bool).
    pub fn getnew_complex64(&mut self) -> AipsIoObjectResult<Vec<Complex64>> {
        let nrv = self.get_u32()? as usize;
        let mut out = vec![Complex64::new(0.0, 0.0); nrv];
        self.get_complex64_into(&mut out)?;
        Ok(out)
    }

    /// Read a count-prefixed string array into a newly allocated `Vec<String>`. See [`getnew_bool`](Self::getnew_bool).
    pub fn getnew_string(&mut self) -> AipsIoObjectResult<Vec<String>> {
        let nrv = self.get_u32()? as usize;
        let mut out = vec![String::new(); nrv];
        self.get_string_into(&mut out)?;
        Ok(out)
    }

    /// Put one dynamically-typed casacore value (scalar, array, or record).
    ///
    /// The encoded payload contains a small value-kind tag and type metadata,
    /// so [`get_value`](Self::get_value) can reconstruct the value without
    /// extra caller-supplied type information.
    pub fn put_value(&mut self, value: &Value) -> AipsIoObjectResult<()> {
        match value {
            Value::Scalar(scalar) => {
                self.put_u8(VALUE_KIND_SCALAR)?;
                self.put_u8(primitive_type_tag(scalar.primitive_type()))?;
                self.put_scalar_value(scalar)
            }
            Value::Array(array) => {
                self.put_u8(VALUE_KIND_ARRAY)?;
                self.put_u8(primitive_type_tag(array.primitive_type()))?;
                self.put_array_value(array)
            }
            Value::Record(record) => {
                self.put_u8(VALUE_KIND_RECORD)?;
                self.put_record_value(record)
            }
        }
    }

    /// Get one dynamically-typed casacore value written by [`put_value`](Self::put_value).
    pub fn get_value(&mut self) -> AipsIoObjectResult<Value> {
        let kind = self.get_u8()?;
        match kind {
            VALUE_KIND_SCALAR => {
                let primitive = primitive_type_from_tag(self.get_u8()?)?;
                Ok(Value::Scalar(self.get_scalar_value(primitive)?))
            }
            VALUE_KIND_ARRAY => {
                let primitive = primitive_type_from_tag(self.get_u8()?)?;
                Ok(Value::Array(self.get_array_value(primitive)?))
            }
            VALUE_KIND_RECORD => Ok(Value::Record(self.get_record_value()?)),
            _ => Err(AipsIoObjectError::InvalidValueKindTag(kind)),
        }
    }

    fn put_scalar_value(&mut self, value: &ScalarValue) -> AipsIoObjectResult<()> {
        match value {
            ScalarValue::Bool(v) => self.put_bool(*v),
            ScalarValue::UInt8(v) => self.put_u8(*v),
            ScalarValue::UInt16(v) => self.put_u16(*v),
            ScalarValue::UInt32(v) => self.put_u32(*v),
            ScalarValue::Int16(v) => self.put_i16(*v),
            ScalarValue::Int32(v) => self.put_i32(*v),
            ScalarValue::Int64(v) => self.put_i64(*v),
            ScalarValue::Float32(v) => self.put_f32(*v),
            ScalarValue::Float64(v) => self.put_f64(*v),
            ScalarValue::Complex32(v) => self.put_complex32(*v),
            ScalarValue::Complex64(v) => self.put_complex64(*v),
            ScalarValue::String(v) => self.put_string(v),
        }
    }

    fn get_scalar_value(&mut self, primitive: PrimitiveType) -> AipsIoObjectResult<ScalarValue> {
        match primitive {
            PrimitiveType::Bool => Ok(ScalarValue::Bool(self.get_bool()?)),
            PrimitiveType::UInt8 => Ok(ScalarValue::UInt8(self.get_u8()?)),
            PrimitiveType::UInt16 => Ok(ScalarValue::UInt16(self.get_u16()?)),
            PrimitiveType::UInt32 => Ok(ScalarValue::UInt32(self.get_u32()?)),
            PrimitiveType::Int16 => Ok(ScalarValue::Int16(self.get_i16()?)),
            PrimitiveType::Int32 => Ok(ScalarValue::Int32(self.get_i32()?)),
            PrimitiveType::Int64 => Ok(ScalarValue::Int64(self.get_i64()?)),
            PrimitiveType::Float32 => Ok(ScalarValue::Float32(self.get_f32()?)),
            PrimitiveType::Float64 => Ok(ScalarValue::Float64(self.get_f64()?)),
            PrimitiveType::Complex32 => Ok(ScalarValue::Complex32(self.get_complex32()?)),
            PrimitiveType::Complex64 => Ok(ScalarValue::Complex64(self.get_complex64()?)),
            PrimitiveType::String => Ok(ScalarValue::String(self.get_string()?)),
        }
    }

    fn put_array_value(&mut self, value: &ArrayValue) -> AipsIoObjectResult<()> {
        let ndim = u32::try_from(value.ndim()).map_err(|_| AipsIoObjectError::LengthOverflow)?;
        self.put_u32(ndim)?;
        for &dim in value.shape() {
            let dim = u64::try_from(dim).map_err(|_| AipsIoObjectError::LengthOverflow)?;
            self.put_u64(dim)?;
        }

        match value {
            ArrayValue::Bool(values) => {
                if let Some(slice) = values.as_slice_memory_order() {
                    self.put_bool_slice(slice, false)
                } else {
                    let packed: Vec<bool> = values.iter().copied().collect();
                    self.put_bool_slice(&packed, false)
                }
            }
            ArrayValue::UInt8(values) => {
                if let Some(slice) = values.as_slice_memory_order() {
                    self.put_u8_slice(slice, false)
                } else {
                    let packed: Vec<u8> = values.iter().copied().collect();
                    self.put_u8_slice(&packed, false)
                }
            }
            ArrayValue::UInt16(values) => {
                if let Some(slice) = values.as_slice_memory_order() {
                    self.put_u16_slice(slice, false)
                } else {
                    let packed: Vec<u16> = values.iter().copied().collect();
                    self.put_u16_slice(&packed, false)
                }
            }
            ArrayValue::UInt32(values) => {
                if let Some(slice) = values.as_slice_memory_order() {
                    self.put_u32_slice(slice, false)
                } else {
                    let packed: Vec<u32> = values.iter().copied().collect();
                    self.put_u32_slice(&packed, false)
                }
            }
            ArrayValue::Int16(values) => {
                if let Some(slice) = values.as_slice_memory_order() {
                    self.put_i16_slice(slice, false)
                } else {
                    let packed: Vec<i16> = values.iter().copied().collect();
                    self.put_i16_slice(&packed, false)
                }
            }
            ArrayValue::Int32(values) => {
                if let Some(slice) = values.as_slice_memory_order() {
                    self.put_i32_slice(slice, false)
                } else {
                    let packed: Vec<i32> = values.iter().copied().collect();
                    self.put_i32_slice(&packed, false)
                }
            }
            ArrayValue::Int64(values) => {
                if let Some(slice) = values.as_slice_memory_order() {
                    self.put_i64_slice(slice, false)
                } else {
                    let packed: Vec<i64> = values.iter().copied().collect();
                    self.put_i64_slice(&packed, false)
                }
            }
            ArrayValue::Float32(values) => {
                if let Some(slice) = values.as_slice_memory_order() {
                    self.put_f32_slice(slice, false)
                } else {
                    let packed: Vec<f32> = values.iter().copied().collect();
                    self.put_f32_slice(&packed, false)
                }
            }
            ArrayValue::Float64(values) => {
                if let Some(slice) = values.as_slice_memory_order() {
                    self.put_f64_slice(slice, false)
                } else {
                    let packed: Vec<f64> = values.iter().copied().collect();
                    self.put_f64_slice(&packed, false)
                }
            }
            ArrayValue::Complex32(values) => {
                if let Some(slice) = values.as_slice_memory_order() {
                    self.put_complex32_slice(slice, false)
                } else {
                    let packed: Vec<Complex32> = values.iter().copied().collect();
                    self.put_complex32_slice(&packed, false)
                }
            }
            ArrayValue::Complex64(values) => {
                if let Some(slice) = values.as_slice_memory_order() {
                    self.put_complex64_slice(slice, false)
                } else {
                    let packed: Vec<Complex64> = values.iter().copied().collect();
                    self.put_complex64_slice(&packed, false)
                }
            }
            ArrayValue::String(values) => {
                if let Some(slice) = values.as_slice_memory_order() {
                    self.put_string_slice(slice, false)
                } else {
                    let packed: Vec<String> = values.iter().cloned().collect();
                    self.put_string_slice(&packed, false)
                }
            }
        }
    }

    fn get_array_value(&mut self, primitive: PrimitiveType) -> AipsIoObjectResult<ArrayValue> {
        let ndim = self.get_u32()? as usize;
        let mut shape = Vec::with_capacity(ndim);
        let mut element_count = 1_usize;
        for _ in 0..ndim {
            let dim_u64 = self.get_u64()?;
            let dim = usize::try_from(dim_u64)
                .map_err(|_| AipsIoObjectError::DimensionOverflow(dim_u64))?;
            shape.push(dim);
            element_count = element_count
                .checked_mul(dim)
                .ok_or(AipsIoObjectError::LengthOverflow)?;
        }

        match primitive {
            PrimitiveType::Bool => {
                let mut values = vec![false; element_count];
                self.get_bool_into(&mut values)?;
                Ok(ArrayValue::Bool(array_from_shape_vec(&shape, values)?))
            }
            PrimitiveType::UInt8 => {
                let mut values = vec![0_u8; element_count];
                self.get_u8_into(&mut values)?;
                Ok(ArrayValue::UInt8(array_from_shape_vec(&shape, values)?))
            }
            PrimitiveType::UInt16 => {
                let mut values = vec![0_u16; element_count];
                self.get_u16_into(&mut values)?;
                Ok(ArrayValue::UInt16(array_from_shape_vec(&shape, values)?))
            }
            PrimitiveType::UInt32 => {
                let mut values = vec![0_u32; element_count];
                self.get_u32_into(&mut values)?;
                Ok(ArrayValue::UInt32(array_from_shape_vec(&shape, values)?))
            }
            PrimitiveType::Int16 => {
                let mut values = vec![0_i16; element_count];
                self.get_i16_into(&mut values)?;
                Ok(ArrayValue::Int16(array_from_shape_vec(&shape, values)?))
            }
            PrimitiveType::Int32 => {
                let mut values = vec![0_i32; element_count];
                self.get_i32_into(&mut values)?;
                Ok(ArrayValue::Int32(array_from_shape_vec(&shape, values)?))
            }
            PrimitiveType::Int64 => {
                let mut values = vec![0_i64; element_count];
                self.get_i64_into(&mut values)?;
                Ok(ArrayValue::Int64(array_from_shape_vec(&shape, values)?))
            }
            PrimitiveType::Float32 => {
                let mut values = vec![0_f32; element_count];
                self.get_f32_into(&mut values)?;
                Ok(ArrayValue::Float32(array_from_shape_vec(&shape, values)?))
            }
            PrimitiveType::Float64 => {
                let mut values = vec![0_f64; element_count];
                self.get_f64_into(&mut values)?;
                Ok(ArrayValue::Float64(array_from_shape_vec(&shape, values)?))
            }
            PrimitiveType::Complex32 => {
                let mut values = vec![Complex32::new(0.0, 0.0); element_count];
                self.get_complex32_into(&mut values)?;
                Ok(ArrayValue::Complex32(array_from_shape_vec(&shape, values)?))
            }
            PrimitiveType::Complex64 => {
                let mut values = vec![Complex64::new(0.0, 0.0); element_count];
                self.get_complex64_into(&mut values)?;
                Ok(ArrayValue::Complex64(array_from_shape_vec(&shape, values)?))
            }
            PrimitiveType::String => {
                let mut values = vec![String::new(); element_count];
                self.get_string_into(&mut values)?;
                Ok(ArrayValue::String(array_from_shape_vec(&shape, values)?))
            }
        }
    }

    fn put_record_value(&mut self, value: &RecordValue) -> AipsIoObjectResult<()> {
        let field_count =
            u32::try_from(value.len()).map_err(|_| AipsIoObjectError::LengthOverflow)?;
        self.put_u32(field_count)?;
        for field in value.fields() {
            self.put_string(&field.name)?;
            self.put_value(&field.value)?;
        }
        Ok(())
    }

    fn get_record_value(&mut self) -> AipsIoObjectResult<RecordValue> {
        let field_count = self.get_u32()? as usize;
        let mut fields = Vec::with_capacity(field_count);
        for _ in 0..field_count {
            let name = self.get_string()?;
            let value = self.get_value()?;
            fields.push(RecordField::new(name, value));
        }
        Ok(RecordValue::new(fields))
    }

    fn with_access(
        inner: Box<dyn AipsIoStream>,
        readable: bool,
        writable: bool,
        seekable: bool,
        byte_order: ByteOrder,
    ) -> Self {
        Self {
            inner: Some(inner),
            byte_order,
            swput: if writable { 0 } else { -1 },
            swget: if readable { 0 } else { -1 },
            level: 0,
            objlen: vec![0],
            objtln: vec![u32::MAX],
            objptr: vec![0],
            has_cached_type: false,
            object_type: String::new(),
            seekable,
            file_state: None,
        }
    }

    fn inner_mut(&mut self) -> AipsIoObjectResult<&mut dyn AipsIoStream> {
        self.inner.as_deref_mut().ok_or(AipsIoObjectError::NotOpen)
    }

    fn encode_i16(&self, v: i16) -> [u8; 2] {
        match self.byte_order {
            ByteOrder::BigEndian => v.to_be_bytes(),
            ByteOrder::LittleEndian => v.to_le_bytes(),
        }
    }
    fn encode_u16(&self, v: u16) -> [u8; 2] {
        match self.byte_order {
            ByteOrder::BigEndian => v.to_be_bytes(),
            ByteOrder::LittleEndian => v.to_le_bytes(),
        }
    }
    fn encode_i32(&self, v: i32) -> [u8; 4] {
        match self.byte_order {
            ByteOrder::BigEndian => v.to_be_bytes(),
            ByteOrder::LittleEndian => v.to_le_bytes(),
        }
    }
    fn encode_u32(&self, v: u32) -> [u8; 4] {
        match self.byte_order {
            ByteOrder::BigEndian => v.to_be_bytes(),
            ByteOrder::LittleEndian => v.to_le_bytes(),
        }
    }
    fn encode_i64(&self, v: i64) -> [u8; 8] {
        match self.byte_order {
            ByteOrder::BigEndian => v.to_be_bytes(),
            ByteOrder::LittleEndian => v.to_le_bytes(),
        }
    }
    fn encode_u64(&self, v: u64) -> [u8; 8] {
        match self.byte_order {
            ByteOrder::BigEndian => v.to_be_bytes(),
            ByteOrder::LittleEndian => v.to_le_bytes(),
        }
    }
    fn decode_i16(&self, b: [u8; 2]) -> i16 {
        match self.byte_order {
            ByteOrder::BigEndian => i16::from_be_bytes(b),
            ByteOrder::LittleEndian => i16::from_le_bytes(b),
        }
    }
    fn decode_u16(&self, b: [u8; 2]) -> u16 {
        match self.byte_order {
            ByteOrder::BigEndian => u16::from_be_bytes(b),
            ByteOrder::LittleEndian => u16::from_le_bytes(b),
        }
    }
    fn decode_i32(&self, b: [u8; 4]) -> i32 {
        match self.byte_order {
            ByteOrder::BigEndian => i32::from_be_bytes(b),
            ByteOrder::LittleEndian => i32::from_le_bytes(b),
        }
    }
    fn decode_u32(&self, b: [u8; 4]) -> u32 {
        match self.byte_order {
            ByteOrder::BigEndian => u32::from_be_bytes(b),
            ByteOrder::LittleEndian => u32::from_le_bytes(b),
        }
    }
    fn decode_i64(&self, b: [u8; 8]) -> i64 {
        match self.byte_order {
            ByteOrder::BigEndian => i64::from_be_bytes(b),
            ByteOrder::LittleEndian => i64::from_le_bytes(b),
        }
    }
    fn decode_u64(&self, b: [u8; 8]) -> u64 {
        match self.byte_order {
            ByteOrder::BigEndian => u64::from_be_bytes(b),
            ByteOrder::LittleEndian => u64::from_le_bytes(b),
        }
    }

    fn ensure_level(&mut self) {
        if self.level >= self.objlen.len() {
            self.objlen.resize(self.level + 10, 0);
            self.objtln.resize(self.level + 10, 0);
            self.objptr.resize(self.level + 10, 0);
        }
    }

    fn test_put(&self) -> AipsIoObjectResult<()> {
        if self.swput <= 0 {
            Err(AipsIoObjectError::NoPutStart)
        } else {
            Ok(())
        }
    }

    fn test_get(&self) -> AipsIoObjectResult<()> {
        if self.swget <= 0 {
            Err(AipsIoObjectError::NoGetStart)
        } else {
            Ok(())
        }
    }

    fn test_get_length(&self) -> AipsIoObjectResult<()> {
        if self.objlen[self.level] > self.objtln[self.level] {
            Err(AipsIoObjectError::ReadBeyondEndOfObject)
        } else {
            Ok(())
        }
    }

    fn add_objlen(&mut self, bytes: usize) -> AipsIoObjectResult<()> {
        let bytes = u32::try_from(bytes).map_err(|_| AipsIoObjectError::LengthOverflow)?;
        self.objlen[self.level] = self.objlen[self.level]
            .checked_add(bytes)
            .ok_or(AipsIoObjectError::LengthOverflow)?;
        Ok(())
    }

    fn write_counted(&mut self, bytes: &[u8]) -> AipsIoObjectResult<()> {
        let inner = self.inner_mut()?;
        inner.write_all(bytes)?;
        self.add_objlen(bytes.len())?;
        Ok(())
    }

    fn read_counted(&mut self, bytes: &mut [u8]) -> AipsIoObjectResult<()> {
        let inner = self.inner_mut()?;
        inner.read_exact(bytes)?;
        self.add_objlen(bytes.len())?;
        self.test_get_length()?;
        Ok(())
    }

    fn write_u32_raw(&mut self, value: u32) -> AipsIoObjectResult<()> {
        let bytes = self.encode_u32(value);
        let inner = self.inner_mut()?;
        inner.write_all(&bytes)?;
        Ok(())
    }
}

impl AipsIo {
    /// Open a file-backed `AipsIo` using a casacore-like open mode.
    ///
    /// The byte order defaults to [`ByteOrder::BigEndian`] (canonical).
    /// Use [`open_with_order`](Self::open_with_order) to specify a different
    /// byte order.
    pub fn open<P: AsRef<Path>>(path: P, option: AipsOpenOption) -> AipsIoObjectResult<Self> {
        Self::open_with_order(path, option, ByteOrder::BigEndian)
    }

    /// Open a file-backed `AipsIo` with an explicit byte order.
    ///
    /// This is identical to [`open`](Self::open) except that the caller
    /// specifies the byte order for multi-byte value encoding.
    pub fn open_with_order<P: AsRef<Path>>(
        path: P,
        option: AipsOpenOption,
        byte_order: ByteOrder,
    ) -> AipsIoObjectResult<Self> {
        let path = path.as_ref();
        let (file, readable, writable, delete_on_close) = open_file_with_option(path, option)?;
        let mut io = Self::with_access(Box::new(file), readable, writable, true, byte_order);
        if option == AipsOpenOption::Append {
            let inner = io.inner_mut()?;
            inner.seek(SeekFrom::End(0))?;
        }
        io.file_state = Some(FileState {
            path: path.to_path_buf(),
            option,
            delete_on_close,
        });
        Ok(io)
    }

    /// Open a new file on this object, failing if it is already open.
    pub fn open_file<P: AsRef<Path>>(
        &mut self,
        path: P,
        option: AipsOpenOption,
    ) -> AipsIoObjectResult<()> {
        if self.is_open() {
            return Err(AipsIoObjectError::AlreadyOpen);
        }
        *self = Self::open(path, option)?;
        Ok(())
    }

    /// Close the current file and open a new one with the given option.
    pub fn reopen<P: AsRef<Path>>(
        &mut self,
        path: P,
        option: AipsOpenOption,
    ) -> AipsIoObjectResult<()> {
        self.close()?;
        *self = Self::open(path, option)?;
        Ok(())
    }
}

fn open_file_with_option(
    path: &Path,
    option: AipsOpenOption,
) -> std::io::Result<(File, bool, bool, bool)> {
    match option {
        AipsOpenOption::Old => {
            let file = OpenOptions::new().read(true).open(path)?;
            Ok((file, true, false, false))
        }
        AipsOpenOption::New => {
            let file = OpenOptions::new()
                .create(true)
                .truncate(true)
                .read(true)
                .write(true)
                .open(path)?;
            Ok((file, true, true, false))
        }
        AipsOpenOption::NewNoReplace => {
            let file = OpenOptions::new()
                .create_new(true)
                .read(true)
                .write(true)
                .open(path)?;
            Ok((file, true, true, false))
        }
        AipsOpenOption::Scratch => {
            let file = OpenOptions::new()
                .create(true)
                .truncate(true)
                .read(true)
                .write(true)
                .open(path)?;
            Ok((file, true, true, true))
        }
        AipsOpenOption::Delete => {
            if !path.exists() {
                return Err(std::io::Error::new(
                    ErrorKind::NotFound,
                    format!("{}: No such file or directory", path.display()),
                ));
            }
            let file = OpenOptions::new().read(true).write(true).open(path)?;
            Ok((file, true, true, true))
        }
        AipsOpenOption::Update => {
            let file = OpenOptions::new().read(true).write(true).open(path)?;
            Ok((file, true, true, false))
        }
        AipsOpenOption::Append => {
            let file = OpenOptions::new()
                .create(true)
                .truncate(false)
                .read(true)
                .write(true)
                .open(path)?;
            Ok((file, true, true, false))
        }
    }
}

fn primitive_type_tag(primitive: PrimitiveType) -> u8 {
    match primitive {
        PrimitiveType::Bool => 0,
        PrimitiveType::Int16 => 1,
        PrimitiveType::Int32 => 2,
        PrimitiveType::Int64 => 3,
        PrimitiveType::Float32 => 4,
        PrimitiveType::Float64 => 5,
        PrimitiveType::Complex32 => 6,
        PrimitiveType::Complex64 => 7,
        PrimitiveType::String => 8,
        PrimitiveType::UInt8 => 9,
        PrimitiveType::UInt16 => 10,
        PrimitiveType::UInt32 => 11,
    }
}

fn primitive_type_from_tag(tag: u8) -> AipsIoObjectResult<PrimitiveType> {
    match tag {
        0 => Ok(PrimitiveType::Bool),
        1 => Ok(PrimitiveType::Int16),
        2 => Ok(PrimitiveType::Int32),
        3 => Ok(PrimitiveType::Int64),
        4 => Ok(PrimitiveType::Float32),
        5 => Ok(PrimitiveType::Float64),
        6 => Ok(PrimitiveType::Complex32),
        7 => Ok(PrimitiveType::Complex64),
        8 => Ok(PrimitiveType::String),
        9 => Ok(PrimitiveType::UInt8),
        10 => Ok(PrimitiveType::UInt16),
        11 => Ok(PrimitiveType::UInt32),
        _ => Err(AipsIoObjectError::InvalidPrimitiveTypeTag(tag)),
    }
}

fn array_from_shape_vec<T>(shape: &[usize], values: Vec<T>) -> AipsIoObjectResult<ArrayD<T>> {
    let len = values.len();
    ArrayD::from_shape_vec(IxDyn(shape), values).map_err(|_| AipsIoObjectError::InvalidArrayShape {
        shape: shape.to_vec(),
        len,
    })
}

fn pack_bool_slice(values: &[bool]) -> Vec<u8> {
    let mut packed = vec![0_u8; values.len().div_ceil(8)];
    for (index, value) in values.iter().enumerate() {
        if *value {
            packed[index / 8] |= 1 << (index % 8);
        }
    }
    packed
}

fn unpack_bool_slice(packed: &[u8], values: &mut [bool]) {
    for (index, value) in values.iter_mut().enumerate() {
        *value = (packed[index / 8] & (1 << (index % 8))) != 0;
    }
}

#[cfg(test)]
mod tests {
    use super::{AipsIo, AipsIoObjectError, AipsOpenOption, MAGIC_VALUE};
    use crate::{ArrayValue, Complex32, Complex64, RecordField, RecordValue, ScalarValue, Value};
    use ndarray::{ArrayD, IxDyn};
    use std::io::Cursor;
    use std::path::PathBuf;

    #[test]
    fn scalar_put_get_methods_round_trip() {
        let mut writer = AipsIo::new_read_write(Cursor::new(Vec::<u8>::new()));
        writer.putstart("scalars", 7).expect("putstart");
        writer.put_bool(true).expect("put bool");
        writer.put_i8(-3).expect("put i8");
        writer.put_u8(250).expect("put u8");
        writer.put_i16(-1234).expect("put i16");
        writer.put_u16(54321).expect("put u16");
        writer.put_i32(-1_234_567).expect("put i32");
        writer.put_u32(3_456_789).expect("put u32");
        writer.put_i64(-9_876_543_210).expect("put i64");
        writer.put_u64(9_876_543_210).expect("put u64");
        writer.put_f32(3.25).expect("put f32");
        writer.put_f64(-10.5).expect("put f64");
        writer
            .put_complex32(Complex32::new(1.5, -2.5))
            .expect("put complex32");
        writer
            .put_complex64(Complex64::new(10.25, -0.75))
            .expect("put complex64");
        writer.put_string("alpha").expect("put string");
        writer.putend().expect("putend");

        let bytes = writer
            .into_inner_typed::<Cursor<Vec<u8>>>()
            .expect("writer should be cursor")
            .into_inner();

        let mut reader = AipsIo::new_read_only(Cursor::new(bytes));
        assert_eq!(reader.get_next_type().expect("get type"), "scalars");
        assert_eq!(reader.getstart("scalars").expect("getstart"), 7);
        assert!(reader.get_bool().expect("get bool"));
        assert_eq!(reader.get_i8().expect("get i8"), -3);
        assert_eq!(reader.get_u8().expect("get u8"), 250);
        assert_eq!(reader.get_i16().expect("get i16"), -1234);
        assert_eq!(reader.get_u16().expect("get u16"), 54321);
        assert_eq!(reader.get_i32().expect("get i32"), -1_234_567);
        assert_eq!(reader.get_u32().expect("get u32"), 3_456_789);
        assert_eq!(reader.get_i64().expect("get i64"), -9_876_543_210);
        assert_eq!(reader.get_u64().expect("get u64"), 9_876_543_210);
        assert_eq!(reader.get_f32().expect("get f32"), 3.25);
        assert_eq!(reader.get_f64().expect("get f64"), -10.5);
        assert_eq!(
            reader.get_complex32().expect("get complex32"),
            Complex32::new(1.5, -2.5)
        );
        assert_eq!(
            reader.get_complex64().expect("get complex64"),
            Complex64::new(10.25, -0.75)
        );
        assert_eq!(reader.get_string().expect("get string"), "alpha");
        reader.getend().expect("getend");
    }

    #[test]
    fn array_put_get_into_and_getnew_methods_round_trip() {
        let bools = vec![true, false, true, true, false, false, true, false, true];
        let i8s = vec![-5_i8, 0, 7];
        let u8s = vec![1_u8, 200, 255];
        let i16s = vec![-300_i16, 0, 1200];
        let u16s = vec![1_u16, 65535, 32];
        let i32s = vec![-1_000_000_i32, 1, 2];
        let u32s = vec![0_u32, 1_000_000, 4_000_000_000_u32];
        let i64s = vec![-10_i64, 0, 123_456_789_012];
        let u64s = vec![0_u64, 9_999_999_999, 42];
        let f32s = vec![1.25_f32, -2.5, 3.75];
        let f64s = vec![1.25_f64, -2.5, 3.75];
        let c32s = vec![Complex32::new(1.0, -1.0), Complex32::new(2.5, -3.5)];
        let c64s = vec![Complex64::new(1.0, -1.0), Complex64::new(2.5, -3.5)];
        let strings = vec!["aa".to_string(), "".to_string(), "xyz".to_string()];

        let mut writer = AipsIo::new_read_write(Cursor::new(Vec::<u8>::new()));
        writer.putstart("into", 1).expect("putstart into");
        writer
            .put_bool_slice(&bools, false)
            .expect("put bool slice");
        writer.put_i8_slice(&i8s, false).expect("put i8 slice");
        writer.put_u8_slice(&u8s, false).expect("put u8 slice");
        writer.put_i16_slice(&i16s, false).expect("put i16 slice");
        writer.put_u16_slice(&u16s, false).expect("put u16 slice");
        writer.put_i32_slice(&i32s, false).expect("put i32 slice");
        writer.put_u32_slice(&u32s, false).expect("put u32 slice");
        writer.put_i64_slice(&i64s, false).expect("put i64 slice");
        writer.put_u64_slice(&u64s, false).expect("put u64 slice");
        writer.put_f32_slice(&f32s, false).expect("put f32 slice");
        writer.put_f64_slice(&f64s, false).expect("put f64 slice");
        writer
            .put_complex32_slice(&c32s, false)
            .expect("put complex32 slice");
        writer
            .put_complex64_slice(&c64s, false)
            .expect("put complex64 slice");
        writer
            .put_string_slice(&strings, false)
            .expect("put string slice");
        writer.putend().expect("putend into");

        writer.putstart("getnew", 1).expect("putstart getnew");
        writer.put_bool_slice(&bools, true).expect("put bool slice");
        writer.put_i8_slice(&i8s, true).expect("put i8 slice");
        writer.put_u8_slice(&u8s, true).expect("put u8 slice");
        writer.put_i16_slice(&i16s, true).expect("put i16 slice");
        writer.put_u16_slice(&u16s, true).expect("put u16 slice");
        writer.put_i32_slice(&i32s, true).expect("put i32 slice");
        writer.put_u32_slice(&u32s, true).expect("put u32 slice");
        writer.put_i64_slice(&i64s, true).expect("put i64 slice");
        writer.put_u64_slice(&u64s, true).expect("put u64 slice");
        writer.put_f32_slice(&f32s, true).expect("put f32 slice");
        writer.put_f64_slice(&f64s, true).expect("put f64 slice");
        writer
            .put_complex32_slice(&c32s, true)
            .expect("put complex32 slice");
        writer
            .put_complex64_slice(&c64s, true)
            .expect("put complex64 slice");
        writer
            .put_string_slice(&strings, true)
            .expect("put string slice");
        writer.putend().expect("putend getnew");

        let bytes = writer
            .into_inner_typed::<Cursor<Vec<u8>>>()
            .expect("writer should be cursor")
            .into_inner();

        let mut reader = AipsIo::new_read_only(Cursor::new(bytes));

        assert_eq!(reader.getstart("into").expect("getstart into"), 1);
        let mut bools_out = vec![false; bools.len()];
        reader.get_bool_into(&mut bools_out).expect("get bool into");
        assert_eq!(bools_out, bools);

        let mut i8s_out = vec![0_i8; i8s.len()];
        reader.get_i8_into(&mut i8s_out).expect("get i8 into");
        assert_eq!(i8s_out, i8s);
        let mut u8s_out = vec![0_u8; u8s.len()];
        reader.get_u8_into(&mut u8s_out).expect("get u8 into");
        assert_eq!(u8s_out, u8s);
        let mut i16s_out = vec![0_i16; i16s.len()];
        reader.get_i16_into(&mut i16s_out).expect("get i16 into");
        assert_eq!(i16s_out, i16s);
        let mut u16s_out = vec![0_u16; u16s.len()];
        reader.get_u16_into(&mut u16s_out).expect("get u16 into");
        assert_eq!(u16s_out, u16s);
        let mut i32s_out = vec![0_i32; i32s.len()];
        reader.get_i32_into(&mut i32s_out).expect("get i32 into");
        assert_eq!(i32s_out, i32s);
        let mut u32s_out = vec![0_u32; u32s.len()];
        reader.get_u32_into(&mut u32s_out).expect("get u32 into");
        assert_eq!(u32s_out, u32s);
        let mut i64s_out = vec![0_i64; i64s.len()];
        reader.get_i64_into(&mut i64s_out).expect("get i64 into");
        assert_eq!(i64s_out, i64s);
        let mut u64s_out = vec![0_u64; u64s.len()];
        reader.get_u64_into(&mut u64s_out).expect("get u64 into");
        assert_eq!(u64s_out, u64s);
        let mut f32s_out = vec![0_f32; f32s.len()];
        reader.get_f32_into(&mut f32s_out).expect("get f32 into");
        assert_eq!(f32s_out, f32s);
        let mut f64s_out = vec![0_f64; f64s.len()];
        reader.get_f64_into(&mut f64s_out).expect("get f64 into");
        assert_eq!(f64s_out, f64s);
        let mut c32s_out = vec![Complex32::new(0.0, 0.0); c32s.len()];
        reader
            .get_complex32_into(&mut c32s_out)
            .expect("get complex32 into");
        assert_eq!(c32s_out, c32s);
        let mut c64s_out = vec![Complex64::new(0.0, 0.0); c64s.len()];
        reader
            .get_complex64_into(&mut c64s_out)
            .expect("get complex64 into");
        assert_eq!(c64s_out, c64s);
        let mut strings_out = vec![String::new(); strings.len()];
        reader
            .get_string_into(&mut strings_out)
            .expect("get string into");
        assert_eq!(strings_out, strings);
        reader.getend().expect("getend into");

        assert_eq!(reader.getstart("getnew").expect("getstart getnew"), 1);
        assert_eq!(reader.getnew_bool().expect("getnew bool"), bools);
        assert_eq!(reader.getnew_i8().expect("getnew i8"), i8s);
        assert_eq!(reader.getnew_u8().expect("getnew u8"), u8s);
        assert_eq!(reader.getnew_i16().expect("getnew i16"), i16s);
        assert_eq!(reader.getnew_u16().expect("getnew u16"), u16s);
        assert_eq!(reader.getnew_i32().expect("getnew i32"), i32s);
        assert_eq!(reader.getnew_u32().expect("getnew u32"), u32s);
        assert_eq!(reader.getnew_i64().expect("getnew i64"), i64s);
        assert_eq!(reader.getnew_u64().expect("getnew u64"), u64s);
        assert_eq!(reader.getnew_f32().expect("getnew f32"), f32s);
        assert_eq!(reader.getnew_f64().expect("getnew f64"), f64s);
        assert_eq!(reader.getnew_complex32().expect("getnew complex32"), c32s);
        assert_eq!(reader.getnew_complex64().expect("getnew complex64"), c64s);
        assert_eq!(reader.getnew_string().expect("getnew string"), strings);
        reader.getend().expect("getend getnew");
    }

    #[test]
    fn nested_object_framing_round_trip() {
        let mut writer = AipsIo::new_read_write(Cursor::new(Vec::<u8>::new()));
        writer.putstart("outer", 1).expect("putstart outer");
        writer.put_i32(11).expect("put outer pre");
        writer.putstart("inner", 2).expect("putstart inner");
        writer.put_string("nested").expect("put inner");
        writer.putend().expect("putend inner");
        writer.put_i32(22).expect("put outer post");
        writer.putend().expect("putend outer");

        let bytes = writer
            .into_inner_typed::<Cursor<Vec<u8>>>()
            .expect("writer should be cursor")
            .into_inner();
        let mut reader = AipsIo::new_read_only(Cursor::new(bytes));

        assert_eq!(reader.get_next_type().expect("next outer"), "outer");
        assert_eq!(reader.get_next_type().expect("next outer cached"), "outer");
        assert_eq!(reader.getstart("outer").expect("getstart outer"), 1);
        assert_eq!(reader.get_i32().expect("outer pre"), 11);
        assert_eq!(reader.get_next_type().expect("next inner"), "inner");
        assert_eq!(reader.get_next_type().expect("next inner cached"), "inner");
        assert_eq!(reader.getstart("inner").expect("getstart inner"), 2);
        assert_eq!(reader.get_string().expect("inner payload"), "nested");
        reader.getend().expect("getend inner");
        assert_eq!(reader.get_i32().expect("outer post"), 22);
        reader.getend().expect("getend outer");
    }

    #[test]
    fn put_get_value_round_trip_for_record_with_nested_values() {
        let matrix = ArrayD::from_shape_vec(IxDyn(&[2, 2]), vec![1_i32, 2_i32, 3_i32, 4_i32])
            .expect("shape");
        let cube = ArrayD::from_shape_vec(
            IxDyn(&[2, 1, 3]),
            vec![
                "a".to_string(),
                "b".to_string(),
                "c".to_string(),
                "d".to_string(),
                "e".to_string(),
                "f".to_string(),
            ],
        )
        .expect("shape");

        let value = Value::Record(RecordValue::new(vec![
            RecordField::new("flag", Value::Scalar(ScalarValue::Bool(true))),
            RecordField::new("matrix", Value::Array(ArrayValue::Int32(matrix))),
            RecordField::new(
                "nested",
                Value::Record(RecordValue::new(vec![
                    RecordField::new("name", Value::Scalar(ScalarValue::String("alpha".into()))),
                    RecordField::new("cube", Value::Array(ArrayValue::String(cube))),
                ])),
            ),
        ]));

        let mut writer = AipsIo::new_read_write(Cursor::new(Vec::<u8>::new()));
        writer.putstart("value", 1).expect("putstart");
        writer.put_value(&value).expect("put value");
        writer.putend().expect("putend");

        let bytes = writer
            .into_inner_typed::<Cursor<Vec<u8>>>()
            .expect("writer should be cursor")
            .into_inner();

        let mut reader = AipsIo::new_read_only(Cursor::new(bytes));
        assert_eq!(reader.get_next_type().expect("type"), "value");
        assert_eq!(reader.getstart("value").expect("start"), 1);
        let decoded = reader.get_value().expect("get value");
        assert_eq!(decoded, value);
        reader.getend().expect("getend");
    }

    #[test]
    fn get_value_rejects_unknown_value_kind_tag() {
        let mut writer = AipsIo::new_read_write(Cursor::new(Vec::<u8>::new()));
        writer.putstart("value", 1).expect("putstart");
        writer.put_u8(99).expect("write invalid kind tag");
        writer.putend().expect("putend");

        let bytes = writer
            .into_inner_typed::<Cursor<Vec<u8>>>()
            .expect("writer should be cursor")
            .into_inner();
        let mut reader = AipsIo::new_read_only(Cursor::new(bytes));
        assert_eq!(reader.getstart("value").expect("start"), 1);

        let err = reader
            .get_value()
            .expect_err("unknown kind tags should fail");
        assert!(matches!(err, AipsIoObjectError::InvalidValueKindTag(99)));
    }

    #[test]
    fn old_mode_rejects_putstart() {
        let path = temp_file("old_mode_rejects_putstart.data");
        {
            let mut io = AipsIo::open(&path, AipsOpenOption::New).expect("create temp file");
            io.close().expect("close");
        }

        let mut io = AipsIo::open(&path, AipsOpenOption::Old).expect("open old");
        let err = io
            .putstart("abc", 1)
            .expect_err("old mode should be read-only");
        assert!(matches!(err, AipsIoObjectError::PutStartUnavailable));
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn new_no_replace_errors_if_file_exists() {
        let path = temp_file("new_no_replace_exists.data");
        {
            let mut io = AipsIo::open(&path, AipsOpenOption::New).expect("create file");
            io.close().expect("close");
        }
        let result = AipsIo::open(&path, AipsOpenOption::NewNoReplace);
        assert!(matches!(result, Err(AipsIoObjectError::Io(_))));
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn delete_mode_removes_file_on_close() {
        let path = temp_file("delete_mode_removes_file.data");
        {
            let mut io = AipsIo::open(&path, AipsOpenOption::New).expect("create file");
            io.close().expect("close");
        }
        assert!(path.exists());

        let mut io = AipsIo::open(&path, AipsOpenOption::Delete).expect("open delete");
        io.close().expect("close delete");
        assert!(!path.exists());
    }

    #[test]
    fn append_mode_appends_new_object() {
        let path = temp_file("append_mode_appends.data");

        {
            let mut io = AipsIo::open(&path, AipsOpenOption::New).expect("create file");
            io.putstart("obj", 1).expect("putstart");
            io.put_i32(11).expect("put value");
            io.putend().expect("putend");
            io.close().expect("close");
        }

        {
            let mut io = AipsIo::open(&path, AipsOpenOption::Append).expect("append open");
            io.putstart("obj", 1).expect("putstart");
            io.put_i32(22).expect("put value");
            io.putend().expect("putend");
            io.close().expect("close");
        }

        {
            let mut io = AipsIo::open(&path, AipsOpenOption::Old).expect("read open");
            assert_eq!(io.getstart("obj").expect("getstart first"), 1);
            assert_eq!(io.get_i32().expect("get first"), 11);
            io.getend().expect("getend first");

            assert_eq!(io.getstart("obj").expect("getstart second"), 1);
            assert_eq!(io.get_i32().expect("get second"), 22);
            io.getend().expect("getend second");
            io.close().expect("close");
        }

        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn append_mode_backpatches_length_in_place() {
        let path = temp_file("append_mode_backpatches_length.data");

        {
            let mut io = AipsIo::open(&path, AipsOpenOption::New).expect("create file");
            io.putstart("obj", 1).expect("putstart");
            io.put_i32(11).expect("put value");
            io.putend().expect("putend");
            io.close().expect("close");
        }

        {
            let mut io = AipsIo::open(&path, AipsOpenOption::Append).expect("append open");
            io.putstart("obj", 1).expect("putstart");
            io.put_i32(22).expect("put value");
            io.putend().expect("putend");
            io.close().expect("close");
        }

        let (second_obj_magic_offset, second_obj_len, end_pos_after_second) = {
            let mut io = AipsIo::open(&path, AipsOpenOption::Old).expect("read open");
            assert_eq!(io.getstart("obj").expect("getstart first"), 1);
            assert_eq!(io.get_i32().expect("get first"), 11);
            io.getend().expect("getend first");

            let second_obj_magic_offset = io.getpos().expect("offset before second object");
            assert_eq!(io.getstart("obj").expect("getstart second"), 1);
            assert_eq!(io.get_i32().expect("get second"), 22);
            let second_obj_len = io.getend().expect("getend second");
            let end_pos_after_second = io.getpos().expect("position after second object");
            io.close().expect("close");

            (
                second_obj_magic_offset,
                second_obj_len,
                end_pos_after_second,
            )
        };

        let raw = std::fs::read(&path).expect("read file bytes");
        let offset = second_obj_magic_offset as usize;
        let magic_field = u32::from_be_bytes(
            raw[offset..offset + 4]
                .try_into()
                .expect("magic field slice"),
        );
        assert_eq!(
            magic_field, MAGIC_VALUE,
            "each top-level object starts with magic"
        );

        let len_field = u32::from_be_bytes(
            raw[offset + 4..offset + 8]
                .try_into()
                .expect("length field slice"),
        );
        assert_ne!(
            len_field, MAGIC_VALUE,
            "append writes must backpatch object length in place"
        );
        assert_eq!(
            len_field, second_obj_len,
            "backpatched length must match encoded object size"
        );

        let file_len = std::fs::metadata(&path).expect("file metadata").len();
        assert_eq!(
            file_len, end_pos_after_second,
            "append writes should not leave trailing bytes from failed backpatch"
        );

        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn close_marks_stream_as_not_open() {
        let path = temp_file("close_marks_not_open.data");
        let mut io = AipsIo::open(&path, AipsOpenOption::New).expect("open");
        io.close().expect("close");
        let err = io.getpos().expect_err("closed stream should reject getpos");
        assert!(matches!(err, AipsIoObjectError::NotOpen));
        let _ = std::fs::remove_file(path);
    }

    fn temp_file(name: &str) -> PathBuf {
        let mut path = std::env::temp_dir();
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_nanos())
            .unwrap_or(0);
        path.push(format!("casa-rs-{name}.{}.{}", std::process::id(), nanos));
        path
    }
}

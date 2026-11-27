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
use std::io::{BufReader, BufWriter, ErrorKind, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

use ndarray::{ArrayD, IxDyn};
use thiserror::Error;

use crate::{
    ArrayValue, ByteOrder, Complex32, Complex64, PrimitiveType, RecordField, RecordValue,
    ScalarValue, Value, ValueKind, primitive_codec,
};

pub type AipsIoObjectResult<T> = Result<T, AipsIoObjectError>;

/// A file wrapper for read-only AipsIO streams with buffered reads.
struct ReadBufferedFile(BufReader<File>);

impl Read for ReadBufferedFile {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        self.0.read(buf)
    }
}

impl Write for ReadBufferedFile {
    fn write(&mut self, _buf: &[u8]) -> std::io::Result<usize> {
        Err(std::io::Error::new(
            ErrorKind::PermissionDenied,
            "AipsIo stream is not writable",
        ))
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

impl Seek for ReadBufferedFile {
    fn seek(&mut self, pos: SeekFrom) -> std::io::Result<u64> {
        self.0.seek(pos)
    }
}

/// A file wrapper that buffers writes while still allowing occasional reads.
///
/// `BufWriter<File>` only implements `Write + Seek`, not `Read`.
/// This wrapper flushes the write buffer before reads so the file position
/// is consistent, and delegates reads to the underlying `File`.
struct WriteBufferedFile(BufWriter<File>);

impl Read for WriteBufferedFile {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        self.0.flush()?;
        self.0.get_mut().read(buf)
    }
}

impl Write for WriteBufferedFile {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.0.write(buf)
    }
    fn flush(&mut self) -> std::io::Result<()> {
        self.0.flush()
    }
}

impl Seek for WriteBufferedFile {
    fn seek(&mut self, pos: SeekFrom) -> std::io::Result<u64> {
        self.0.seek(pos)
    }
}

const MAGIC_VALUE: u32 = 0xbebebebe;
const VALUE_KIND_SCALAR: u8 = 0;
const VALUE_KIND_ARRAY: u8 = 1;
const VALUE_KIND_RECORD: u8 = 2;
const VALUE_KIND_TABLE_REF: u8 = 3;

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
    /// A boolean field contained a byte other than the canonical `0` or `1`.
    #[error("invalid boolean value {0}; expected 0 or 1")]
    InvalidBoolean(u8),
    /// A framed in-memory object was shorter than the required header or payload.
    #[error("AipsIO frame is too short: need {needed} bytes, have {available}")]
    FrameTooShort { needed: usize, available: usize },
    /// An object length was smaller than its header or exceeded its containing buffer/frame.
    #[error("invalid AipsIO object length {declared}; {available} bytes are available")]
    InvalidObjectLength { declared: u32, available: usize },
    /// Both canonical and little-endian interpretations form valid complete frames.
    #[error(
        "ambiguous AipsIO byte order: big-endian length {big_endian_length}, little-endian length {little_endian_length}"
    )]
    AmbiguousByteOrder {
        big_endian_length: u32,
        little_endian_length: u32,
    },
    /// Neither endian interpretation formed a structurally valid AipsIO frame.
    #[error("invalid AipsIO frame: {0}")]
    InvalidFrame(String),
    /// A primitive codec operation failed while reading or writing a framed object.
    #[error("AipsIO primitive codec error: {0}")]
    PrimitiveCodec(String),
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
    /// [`AipsIo::put_value`] was called with a [`Value`] variant this object
    /// codec does not support.
    #[error("unsupported value kind for AipsIO object codec: {0:?}")]
    UnsupportedValueKind(ValueKind),
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
/// use casa_aipsio::AipsIo;
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

mod values;

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

    /// Construct a buffered writer over an already opened file.
    ///
    /// The caller keeps control of how the file is opened and published.
    /// Finish the objects before publishing so their length updates flush
    /// through the buffer.
    pub fn new_buffered_write_only_file(file: File) -> Self {
        Self::new_write_only(WriteBufferedFile(BufWriter::new(file)))
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

        if let Some(state) = self.file_state.take()
            && state.delete_on_close
        {
            let _ = remove_file(state.path);
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
        primitive_codec::encode_i16(v, self.byte_order)
    }
    fn encode_u16(&self, v: u16) -> [u8; 2] {
        primitive_codec::encode_u16(v, self.byte_order)
    }
    fn encode_i32(&self, v: i32) -> [u8; 4] {
        primitive_codec::encode_i32(v, self.byte_order)
    }
    fn encode_u32(&self, v: u32) -> [u8; 4] {
        primitive_codec::encode_u32(v, self.byte_order)
    }
    fn encode_i64(&self, v: i64) -> [u8; 8] {
        primitive_codec::encode_i64(v, self.byte_order)
    }
    fn encode_u64(&self, v: u64) -> [u8; 8] {
        primitive_codec::encode_u64(v, self.byte_order)
    }
    fn decode_i16(&self, b: [u8; 2]) -> i16 {
        primitive_codec::decode_i16(b, self.byte_order)
    }
    fn decode_u16(&self, b: [u8; 2]) -> u16 {
        primitive_codec::decode_u16(b, self.byte_order)
    }
    fn decode_i32(&self, b: [u8; 4]) -> i32 {
        primitive_codec::decode_i32(b, self.byte_order)
    }
    fn decode_u32(&self, b: [u8; 4]) -> u32 {
        primitive_codec::decode_u32(b, self.byte_order)
    }
    fn decode_i64(&self, b: [u8; 8]) -> i64 {
        primitive_codec::decode_i64(b, self.byte_order)
    }
    fn decode_u64(&self, b: [u8; 8]) -> u64 {
        primitive_codec::decode_u64(b, self.byte_order)
    }

    fn read_fixed_width_into<T, const N: usize>(
        &mut self,
        values: &mut [T],
        decode: impl Fn([u8; N]) -> T,
    ) -> AipsIoObjectResult<()> {
        self.test_get()?;
        let byte_len = values
            .len()
            .checked_mul(N)
            .ok_or(AipsIoObjectError::LengthOverflow)?;
        let mut buf = vec![0_u8; byte_len];
        self.read_counted(&mut buf)?;
        for (value, bytes) in values.iter_mut().zip(buf.as_chunks::<N>().0) {
            *value = decode(*bytes);
        }
        Ok(())
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
        let buffered: Box<dyn AipsIoStream> = if readable && !writable {
            Box::new(ReadBufferedFile(BufReader::new(file)))
        } else {
            Box::new(WriteBufferedFile(BufWriter::new(file)))
        };
        let mut io = Self::with_access(buffered, readable, writable, true, byte_order);
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
mod tests;

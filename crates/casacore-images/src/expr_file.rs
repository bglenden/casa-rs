// SPDX-License-Identifier: LGPL-3.0-or-later
//! Persistence format for casacore-compatible expression image files.
//!
//! An expression image (`.imgexpr`) is a **directory** containing a single
//! `imageexpr.json` manifest file.  This format is defined by C++ casacore
//! (class `ImageExpr<T>`, `ImageOpener`).
//!
//! ## On-disk layout
//!
//! ```text
//! my_expr.imgexpr/
//! └── imageexpr.json
//! ```
//!
//! ## JSON schema
//!
//! ```json
//! {
//!   "Version": 1,
//!   "DataType": "float",
//!   "ImageExpr": "'a.image' + 'b.image' * 2.0",
//!   "MiscInfo": {}
//! }
//! ```
//!
//! | Field       | Type   | Description                                         |
//! |-------------|--------|-----------------------------------------------------|
//! | `Version`   | int    | Format version, currently `1`                       |
//! | `DataType`  | string | Pixel type: `"float"`, `"double"`, `"complex"`, `"dcomplex"` |
//! | `ImageExpr` | string | LEL expression string                               |
//! | `MiscInfo`  | object | Miscellaneous metadata record (may be empty)        |
//!
//! ## Reopen semantics
//!
//! When an expression file is reopened, the expression string is re-parsed
//! and all source images referenced in the expression must still exist at
//! the paths embedded in the string.  Coordinates and shape are derived from
//! the source images, not stored in the JSON.
//!
//! ## Cross-language interop
//!
//! Files written by [`save`] are directly openable by C++ casacore via
//! `ImageOpener::openImageExpr()`, and files written by C++ are openable
//! by [`open`].

use std::collections::HashMap;
use std::fs;
use std::path::Path;

use casacore_coordinates::CoordinateSystem;
use casacore_lattices::{Lattice, LatticeError};
use casacore_types::{ArrayD, PrimitiveType, RecordField, RecordValue, ScalarValue, Value};

use crate::error::ImageError;
use crate::expr_parser::{self, ExprValueConvert, HashMapResolver};
use crate::image::{ImageInterface, ImagePixelType, PagedImage, image_pixel_type};
use crate::image_expr::{ImageExpr, ImageExprValue};
use crate::image_info::ImageInfo;

// ---------------------------------------------------------------------------
// Type string mapping
// ---------------------------------------------------------------------------

/// Returns the C++ casacore type string for a given `PrimitiveType`.
///
/// Matches `ValType::getTypeStr()` in C++.
fn data_type_str(pt: PrimitiveType) -> &'static str {
    match pt {
        PrimitiveType::Float32 => "float",
        PrimitiveType::Float64 => "double",
        PrimitiveType::Complex32 => "complex",
        PrimitiveType::Complex64 => "dcomplex",
        _ => "float", // fallback, shouldn't happen for image types
    }
}

/// Parses a C++ casacore type string into a `PrimitiveType`.
fn parse_data_type(s: &str) -> Result<PrimitiveType, ImageError> {
    match s.trim().to_lowercase().as_str() {
        "float" => Ok(PrimitiveType::Float32),
        "double" => Ok(PrimitiveType::Float64),
        "complex" => Ok(PrimitiveType::Complex32),
        "dcomplex" => Ok(PrimitiveType::Complex64),
        other => Err(ImageError::InvalidMetadata(format!(
            "unknown DataType in imageexpr.json: {other:?}"
        ))),
    }
}

// ---------------------------------------------------------------------------
// SourceImage — unified source wrapper for native, converted, and nested expr
// ---------------------------------------------------------------------------

/// A source image that may be native, pixel-type-converted, or a nested
/// expression image.
///
/// Used internally by [`OwnedImageExpr`] to hold heterogeneous sources.
enum SourceImage<T: ImageExprValue + PartialOrd + ExprValueConvert> {
    /// Source image with matching pixel type `T`.
    Native(PagedImage<T>),
    /// Source image stored as f32 on disk, pixels converted to `T` on read.
    ConvertedF32(PagedImage<f32>),
    /// Source image stored as f64 on disk, pixels converted to `T` on read.
    ConvertedF64(PagedImage<f64>),
    /// A nested expression image (another `.imgexpr` file).
    Expr(Box<OwnedImageExpr<T>>),
    /// Nested `.imgexpr` with f32 pixels converted to `T` on read.
    ConvertedExprF32(Box<OwnedImageExpr<f32>>),
    /// Nested `.imgexpr` with f64 pixels converted to `T` on read.
    ConvertedExprF64(Box<OwnedImageExpr<f64>>),
}

impl<T: ImageExprValue + PartialOrd + ExprValueConvert> Lattice<T> for SourceImage<T> {
    fn shape(&self) -> &[usize] {
        match self {
            Self::Native(img) => img.shape(),
            Self::ConvertedF32(img) => img.shape(),
            Self::ConvertedF64(img) => img.shape(),
            Self::Expr(expr) => &expr.shape,
            Self::ConvertedExprF32(expr) => expr.shape(),
            Self::ConvertedExprF64(expr) => expr.shape(),
        }
    }

    fn is_writable(&self) -> bool {
        false
    }

    fn get_at(&self, position: &[usize]) -> Result<T, LatticeError> {
        match self {
            Self::Native(img) => Lattice::get_at(img, position),
            Self::ConvertedF32(img) => {
                let val = Lattice::get_at(img, position)?;
                Ok(T::from_f64(val.to_f64()))
            }
            Self::ConvertedF64(img) => {
                let val = Lattice::get_at(img, position)?;
                Ok(T::from_f64(val.to_f64()))
            }
            Self::Expr(expr) => {
                let e = expr
                    .make_expr()
                    .map_err(|e| LatticeError::Table(e.to_string()))?;
                Lattice::get_at(&e, position)
            }
            Self::ConvertedExprF32(expr) => {
                let val = expr
                    .get_at(position)
                    .map_err(|e| LatticeError::Table(e.to_string()))?;
                Ok(T::from_f64(val.to_f64()))
            }
            Self::ConvertedExprF64(expr) => {
                let val = expr
                    .get_at(position)
                    .map_err(|e| LatticeError::Table(e.to_string()))?;
                Ok(T::from_f64(val.to_f64()))
            }
        }
    }

    fn get_slice(
        &self,
        start: &[usize],
        shape: &[usize],
        stride: &[usize],
    ) -> Result<ArrayD<T>, LatticeError> {
        match self {
            Self::Native(img) => Lattice::get_slice(img, start, shape, stride),
            Self::ConvertedF32(img) => {
                let data = Lattice::get_slice(img, start, shape, stride)?;
                Ok(data.mapv(|v| T::from_f64(v.to_f64())))
            }
            Self::ConvertedF64(img) => {
                let data = Lattice::get_slice(img, start, shape, stride)?;
                Ok(data.mapv(|v| T::from_f64(v.to_f64())))
            }
            Self::Expr(expr) => {
                let e = expr
                    .make_expr()
                    .map_err(|e| LatticeError::Table(e.to_string()))?;
                Lattice::get_slice(&e, start, shape, stride)
            }
            Self::ConvertedExprF32(expr) => {
                let data = Lattice::get_slice(&**expr, start, shape, stride)?;
                Ok(data.mapv(|v| T::from_f64(v.to_f64())))
            }
            Self::ConvertedExprF64(expr) => {
                let data = Lattice::get_slice(&**expr, start, shape, stride)?;
                Ok(data.mapv(|v| T::from_f64(v.to_f64())))
            }
        }
    }
}

impl<T: ImageExprValue + PartialOrd + ExprValueConvert> ImageInterface<T> for SourceImage<T> {
    fn as_any(&self) -> Option<&dyn std::any::Any> {
        None
    }

    fn coordinates(&self) -> &CoordinateSystem {
        match self {
            Self::Native(img) => img.coordinates(),
            Self::ConvertedF32(img) => img.coordinates(),
            Self::ConvertedF64(img) => img.coordinates(),
            Self::Expr(expr) => &expr.coords,
            Self::ConvertedExprF32(expr) => expr.coordinates(),
            Self::ConvertedExprF64(expr) => expr.coordinates(),
        }
    }

    fn units(&self) -> &str {
        match self {
            Self::Native(img) => img.units(),
            Self::ConvertedF32(img) => img.units(),
            Self::ConvertedF64(img) => img.units(),
            Self::Expr(expr) => &expr.units,
            Self::ConvertedExprF32(expr) => expr.units(),
            Self::ConvertedExprF64(expr) => expr.units(),
        }
    }

    fn misc_info(&self) -> RecordValue {
        match self {
            Self::Native(img) => img.misc_info(),
            Self::ConvertedF32(img) => img.misc_info(),
            Self::ConvertedF64(img) => img.misc_info(),
            Self::Expr(expr) => expr.misc_info.clone(),
            Self::ConvertedExprF32(expr) => expr.misc_info().clone(),
            Self::ConvertedExprF64(expr) => expr.misc_info().clone(),
        }
    }

    fn image_info(&self) -> Result<ImageInfo, ImageError> {
        match self {
            Self::Native(img) => img.image_info(),
            Self::ConvertedF32(img) => img.image_info(),
            Self::ConvertedF64(img) => img.image_info(),
            Self::Expr(expr) => Ok(expr.image_info.clone()),
            Self::ConvertedExprF32(expr) => expr.image_info(),
            Self::ConvertedExprF64(expr) => expr.image_info(),
        }
    }

    fn name(&self) -> Option<&Path> {
        match self {
            Self::Native(img) => img.name(),
            Self::ConvertedF32(img) => img.name(),
            Self::ConvertedF64(img) => img.name(),
            Self::Expr(expr) => expr.path.as_deref(),
            Self::ConvertedExprF32(expr) => expr.name(),
            Self::ConvertedExprF64(expr) => expr.name(),
        }
    }
}

/// Opens a single source dependency, handling type mismatches and nested
/// `.imgexpr` directories.
fn open_source<T>(img_path: &Path) -> Result<SourceImage<T>, ImageError>
where
    T: ImageExprValue + PartialOrd + ExprValueConvert,
{
    // Nested .imgexpr directory — open recursively.
    if is_image_expr(img_path) {
        let inner_info = read_info(img_path)?;
        return match inner_info.data_type {
            pt if pt == T::PRIMITIVE_TYPE => Ok(SourceImage::Expr(Box::new(open::<T>(img_path)?))),
            PrimitiveType::Float32 => Ok(SourceImage::ConvertedExprF32(Box::new(open::<f32>(
                img_path,
            )?))),
            PrimitiveType::Float64 => Ok(SourceImage::ConvertedExprF64(Box::new(open::<f64>(
                img_path,
            )?))),
            other => Err(ImageError::InvalidMetadata(format!(
                "cannot convert nested .imgexpr {:?} to {:?}",
                other,
                T::PRIMITIVE_TYPE,
            ))),
        };
    }

    // Regular image — try matching type first.
    match PagedImage::<T>::open(img_path) {
        Ok(img) => return Ok(SourceImage::Native(img)),
        Err(ImageError::InvalidMetadata(ref msg)) if msg.contains("pixel type mismatch") => {}
        Err(e) => return Err(e),
    }

    // Type mismatch — try f32↔f64 conversion.
    let actual = image_pixel_type(img_path)?;
    match actual {
        ImagePixelType::Float32 => Ok(SourceImage::ConvertedF32(PagedImage::<f32>::open(
            img_path,
        )?)),
        ImagePixelType::Float64 => Ok(SourceImage::ConvertedF64(PagedImage::<f64>::open(
            img_path,
        )?)),
        _ => Err(ImageError::InvalidMetadata(format!(
            "cannot convert {:?} source image to {:?}",
            actual,
            T::PRIMITIVE_TYPE,
        ))),
    }
}

// ---------------------------------------------------------------------------
// MiscInfo JSON conversion (simple scalars only)
// ---------------------------------------------------------------------------

fn misc_info_to_json(record: &RecordValue) -> serde_json::Value {
    let mut map = serde_json::Map::new();
    for field in record.fields() {
        let val = match &field.value {
            Value::Scalar(s) => match s {
                ScalarValue::Bool(v) => serde_json::Value::Bool(*v),
                ScalarValue::Int32(v) => serde_json::json!(*v),
                ScalarValue::Int64(v) => serde_json::json!(*v),
                ScalarValue::Float32(v) => serde_json::json!(*v),
                ScalarValue::Float64(v) => serde_json::json!(*v),
                ScalarValue::String(v) => serde_json::Value::String(v.clone()),
                _ => continue, // skip unsupported scalar types
            },
            Value::Record(sub) => misc_info_to_json(sub),
            _ => continue, // skip arrays, table refs
        };
        map.insert(field.name.clone(), val);
    }
    serde_json::Value::Object(map)
}

fn json_to_misc_info(val: &serde_json::Value) -> RecordValue {
    let mut record = RecordValue::default();
    if let Some(obj) = val.as_object() {
        for (key, v) in obj {
            let field_val = match v {
                serde_json::Value::Bool(b) => Value::Scalar(ScalarValue::Bool(*b)),
                serde_json::Value::Number(n) => {
                    if let Some(i) = n.as_i64() {
                        if i >= i32::MIN as i64 && i <= i32::MAX as i64 {
                            Value::Scalar(ScalarValue::Int32(i as i32))
                        } else {
                            Value::Scalar(ScalarValue::Int64(i))
                        }
                    } else if let Some(f) = n.as_f64() {
                        Value::Scalar(ScalarValue::Float64(f))
                    } else {
                        continue;
                    }
                }
                serde_json::Value::String(s) => Value::Scalar(ScalarValue::String(s.clone())),
                serde_json::Value::Object(_) => Value::Record(json_to_misc_info(v)),
                _ => continue, // skip arrays, null
            };
            record.push(RecordField::new(key, field_val));
        }
    }
    record
}

// ---------------------------------------------------------------------------
// Save
// ---------------------------------------------------------------------------

/// Saves an expression image in casacore-compatible `.imgexpr` format.
///
/// Creates a directory at `path` containing `imageexpr.json`.
///
/// Corresponds to C++ `ImageExpr<T>::save()`.
pub fn save(
    path: impl AsRef<Path>,
    expr_string: &str,
    pixel_type: PrimitiveType,
    misc_info: &RecordValue,
) -> Result<(), ImageError> {
    let dir = path.as_ref();
    fs::create_dir_all(dir).map_err(|e| {
        ImageError::InvalidMetadata(format!("cannot create .imgexpr directory {dir:?}: {e}"))
    })?;

    let json = serde_json::json!({
        "Version": 1,
        "DataType": data_type_str(pixel_type),
        "ImageExpr": expr_string,
        "MiscInfo": misc_info_to_json(misc_info),
    });

    let json_path = dir.join("imageexpr.json");
    let content = serde_json::to_string_pretty(&json).map_err(|e| {
        ImageError::InvalidMetadata(format!("cannot serialize imageexpr.json: {e}"))
    })?;
    fs::write(&json_path, content)
        .map_err(|e| ImageError::InvalidMetadata(format!("cannot write {json_path:?}: {e}")))?;

    Ok(())
}

// ---------------------------------------------------------------------------
// Type detection
// ---------------------------------------------------------------------------

/// Returns `true` if `path` is a casacore expression image directory.
///
/// Checks for the presence of `imageexpr.json` inside the directory.
/// Corresponds to the `IMAGEEXPR` detection in C++ `ImageOpener::imageType()`.
pub fn is_image_expr(path: impl AsRef<Path>) -> bool {
    path.as_ref().join("imageexpr.json").is_file()
}

// ---------------------------------------------------------------------------
// Open: read JSON metadata
// ---------------------------------------------------------------------------

/// Metadata read from an `imageexpr.json` file.
#[derive(Debug, Clone)]
pub struct ExprFileInfo {
    /// Format version (expected: 1).
    pub version: u32,
    /// Pixel data type string (e.g. `"float"`).
    pub data_type: PrimitiveType,
    /// The LEL expression string.
    pub expr_string: String,
    /// Miscellaneous metadata.
    pub misc_info: RecordValue,
}

/// Reads and parses the `imageexpr.json` from an expression image directory.
pub fn read_info(path: impl AsRef<Path>) -> Result<ExprFileInfo, ImageError> {
    let json_path = path.as_ref().join("imageexpr.json");
    let content = fs::read_to_string(&json_path)
        .map_err(|e| ImageError::InvalidMetadata(format!("cannot read {json_path:?}: {e}")))?;
    let json: serde_json::Value = serde_json::from_str(&content)
        .map_err(|e| ImageError::InvalidMetadata(format!("invalid JSON in {json_path:?}: {e}")))?;

    let version = json.get("Version").and_then(|v| v.as_u64()).unwrap_or(1) as u32;

    let data_type_s = json
        .get("DataType")
        .and_then(|v| v.as_str())
        .ok_or_else(|| ImageError::InvalidMetadata("missing DataType in imageexpr.json".into()))?;
    let data_type = parse_data_type(data_type_s)?;

    let expr_string = json
        .get("ImageExpr")
        .and_then(|v| v.as_str())
        .ok_or_else(|| ImageError::InvalidMetadata("missing ImageExpr in imageexpr.json".into()))?
        .to_string();

    let misc_info = json
        .get("MiscInfo")
        .map(json_to_misc_info)
        .unwrap_or_default();

    Ok(ExprFileInfo {
        version,
        data_type,
        expr_string,
        misc_info,
    })
}

// ---------------------------------------------------------------------------
// OwnedImageExpr — owns source images and provides lazy evaluation
// ---------------------------------------------------------------------------

/// An expression image that owns its source images.
///
/// Created by [`open`] when reading an `.imgexpr` file.  Source images
/// referenced in the expression string are opened as [`PagedImage<T>`],
/// pixel-type-converted wrappers, or nested [`OwnedImageExpr<T>`] and
/// kept alive.  Each call to [`get`](OwnedImageExpr::get) or
/// [`get_slice`](OwnedImageExpr::get_slice) re-parses the expression
/// (microseconds) and evaluates the requested region lazily.
///
/// Implements [`Lattice<T>`] and [`ImageInterface<T>`] so it can itself
/// serve as a source in nested expression chains.
///
/// Corresponds to a C++ `ImageExpr<T>` reopened from a persistent file.
pub struct OwnedImageExpr<T: ImageExprValue + PartialOrd + ExprValueConvert> {
    sources: Vec<SourceImage<T>>,
    source_names: Vec<String>,
    expr_string: String,
    shape: Vec<usize>,
    coords: CoordinateSystem,
    units: String,
    misc_info: RecordValue,
    image_info: ImageInfo,
    path: Option<std::path::PathBuf>,
}

impl<T: ImageExprValue + PartialOrd + ExprValueConvert> OwnedImageExpr<T> {
    /// Returns the image shape.
    pub fn shape(&self) -> &[usize] {
        &self.shape
    }

    /// Returns the expression string.
    pub fn expr_string(&self) -> &str {
        &self.expr_string
    }

    /// Returns the file path this expression was opened from, if any.
    pub fn path(&self) -> Option<&Path> {
        self.path.as_deref()
    }

    /// Returns the miscellaneous metadata record.
    pub fn misc_info(&self) -> &RecordValue {
        &self.misc_info
    }

    /// Evaluates the full expression and returns the result array.
    pub fn get(&self) -> Result<casacore_types::ArrayD<T>, ImageError> {
        self.make_expr()?.get()
    }

    /// Evaluates a sub-slice of the expression.
    pub fn get_slice(
        &self,
        start: &[usize],
        shape: &[usize],
    ) -> Result<casacore_types::ArrayD<T>, ImageError> {
        self.make_expr()?.get_slice(start, shape)
    }

    /// Evaluates a single pixel.
    pub fn get_at(&self, position: &[usize]) -> Result<T, ImageError> {
        self.make_expr()?.get_at(position)
    }

    fn make_expr(&self) -> Result<ImageExpr<'_, T>, ImageError> {
        let mut images: HashMap<String, &dyn ImageInterface<T>> = HashMap::new();
        for (name, src) in self.source_names.iter().zip(self.sources.iter()) {
            images.insert(name.clone(), src);
        }
        let resolver = HashMapResolver(images);
        let expr = expr_parser::parse_image_expr(&self.expr_string, &resolver)?;
        Ok(expr)
    }
}

impl<T: ImageExprValue + PartialOrd + ExprValueConvert> Lattice<T> for OwnedImageExpr<T> {
    fn shape(&self) -> &[usize] {
        &self.shape
    }

    fn is_writable(&self) -> bool {
        false
    }

    fn get_at(&self, position: &[usize]) -> Result<T, LatticeError> {
        let expr = self
            .make_expr()
            .map_err(|e| LatticeError::Table(e.to_string()))?;
        Lattice::get_at(&expr, position)
    }

    fn get_slice(
        &self,
        start: &[usize],
        shape: &[usize],
        stride: &[usize],
    ) -> Result<ArrayD<T>, LatticeError> {
        let expr = self
            .make_expr()
            .map_err(|e| LatticeError::Table(e.to_string()))?;
        Lattice::get_slice(&expr, start, shape, stride)
    }
}

impl<T: ImageExprValue + PartialOrd + ExprValueConvert> ImageInterface<T> for OwnedImageExpr<T> {
    fn as_any(&self) -> Option<&dyn std::any::Any> {
        None
    }

    fn coordinates(&self) -> &CoordinateSystem {
        &self.coords
    }

    fn units(&self) -> &str {
        &self.units
    }

    fn misc_info(&self) -> RecordValue {
        self.misc_info.clone()
    }

    fn image_info(&self) -> Result<ImageInfo, ImageError> {
        Ok(self.image_info.clone())
    }

    fn name(&self) -> Option<&Path> {
        self.path.as_deref()
    }
}

impl<T: ImageExprValue + PartialOrd + ExprValueConvert> std::fmt::Debug for OwnedImageExpr<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("OwnedImageExpr")
            .field("shape", &self.shape)
            .field("expr", &self.expr_string)
            .field("sources", &self.source_names)
            .finish()
    }
}

// ---------------------------------------------------------------------------
// Open
// ---------------------------------------------------------------------------

/// Opens an expression image from an `.imgexpr` directory.
///
/// Reads `imageexpr.json`, opens all source images referenced in the
/// expression string, and returns an [`OwnedImageExpr`] that can evaluate
/// the expression lazily.
///
/// Source images may be:
/// - `PagedImage<T>` with matching pixel type,
/// - a pixel-type-converting wrapper (e.g. f32 source in an f64 expression),
/// - a nested `.imgexpr` directory (recursively opened).
///
/// Image names in the expression are resolved relative to the parent
/// directory of the `.imgexpr` path, matching C++ casacore behavior.
///
/// # Errors
///
/// Returns an error if the JSON is malformed, the pixel type doesn't match
/// `T`, any referenced source image cannot be opened, or the expression
/// string fails to parse.
pub fn open<T>(path: impl AsRef<Path>) -> Result<OwnedImageExpr<T>, ImageError>
where
    T: ImageExprValue + PartialOrd + ExprValueConvert,
{
    let path = path.as_ref();
    let info = read_info(path)?;

    if info.data_type != T::PRIMITIVE_TYPE {
        return Err(ImageError::InvalidMetadata(format!(
            "expression file has DataType {:?} but opened as {:?}",
            info.data_type,
            T::PRIMITIVE_TYPE,
        )));
    }

    // Determine the base directory for resolving relative image paths.
    let base_dir = path.parent().unwrap_or(Path::new("."));

    // Extract image names from the expression.
    let names = extract_image_names(&info.expr_string);

    // Open each source image (native, converted, or nested .imgexpr).
    let mut sources = Vec::new();
    let mut source_names = Vec::new();
    for name in &names {
        let img_path = if Path::new(name).is_absolute() {
            std::path::PathBuf::from(name)
        } else {
            base_dir.join(name)
        };
        let src = open_source::<T>(&img_path).map_err(|e| {
            ImageError::InvalidMetadata(format!(
                "cannot open source image {name:?} (resolved to {img_path:?}): {e}"
            ))
        })?;
        sources.push(src);
        source_names.push(name.clone());
    }

    // Derive shape and metadata from the first source image.
    if sources.is_empty() {
        return Err(ImageError::InvalidMetadata(
            "expression references no images; cannot determine shape".into(),
        ));
    }
    let first: &dyn ImageInterface<T> = &sources[0];
    let shape = first.shape().to_vec();
    let coords = first.coordinates().clone();
    let units = first.units().to_string();
    let image_info = first.image_info()?;

    Ok(OwnedImageExpr {
        sources,
        source_names,
        expr_string: info.expr_string,
        shape,
        coords,
        units,
        misc_info: info.misc_info,
        image_info,
        path: Some(path.to_path_buf()),
    })
}

/// Extracts image name tokens from a LEL expression string.
///
/// Scans for quoted strings (`'...'`, `"..."`) and bare identifiers that are
/// not function calls.
fn extract_image_names(expr: &str) -> Vec<String> {
    let mut names = Vec::new();
    let mut seen = std::collections::HashSet::new();
    let chars: Vec<char> = expr.chars().collect();
    let mut i = 0;

    while i < chars.len() {
        let ch = chars[i];
        // Quoted path
        if ch == '\'' || ch == '"' {
            let quote = ch;
            i += 1;
            let start = i;
            while i < chars.len() && chars[i] != quote {
                i += 1;
            }
            let name: String = chars[start..i].iter().collect();
            if !name.is_empty() && seen.insert(name.clone()) {
                names.push(name);
            }
            if i < chars.len() {
                i += 1; // skip closing quote
            }
        }
        // Bare identifier that might be a filename.
        // Skip 'e'/'E' that form part of scientific notation (e.g. 3.5e10).
        else if (ch.is_ascii_alphabetic() || ch == '_')
            && !((ch == 'e' || ch == 'E') && i > 0 && chars[i - 1].is_ascii_digit())
        {
            let start = i;
            while i < chars.len()
                && (chars[i].is_alphanumeric()
                    || chars[i] == '_'
                    || chars[i] == '.'
                    || chars[i] == '/'
                    || chars[i] == '~')
            {
                i += 1;
            }
            let token: String = chars[start..i].iter().collect();
            // The parser treats bare identifiers as image names unless they are
            // followed by `(`, in which case they are function calls.
            if !is_function_call(&chars, i)
                && is_image_reference(&token)
                && seen.insert(token.clone())
            {
                names.push(token);
            }
        } else {
            i += 1;
        }
    }
    names
}

fn is_function_call(chars: &[char], mut idx: usize) -> bool {
    while idx < chars.len() && chars[idx].is_ascii_whitespace() {
        idx += 1;
    }
    idx < chars.len() && chars[idx] == '('
}

/// Returns `true` if a bare token should be treated as an image reference.
///
/// This mirrors the parser's bare-identifier handling: any non-numeric token
/// is an image reference unless it is immediately followed by `(`.
fn is_image_reference(token: &str) -> bool {
    // Skip numeric literals
    if token.parse::<f64>().is_ok() {
        return false;
    }
    true
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn extract_quoted_names() {
        let names = extract_image_names("'a.image' + 'b.image' * 2.0");
        assert_eq!(names, vec!["a.image", "b.image"]);
    }

    #[test]
    fn extract_double_quoted_names() {
        let names = extract_image_names("\"my/path.img\" + 'other.img'");
        assert_eq!(names, vec!["my/path.img", "other.img"]);
    }

    #[test]
    fn extract_bare_path_names() {
        let names = extract_image_names("dir/img.image + sin(dir/img.image)");
        // sin is a function, but dir/img.image is extracted once
        assert_eq!(names, vec!["dir/img.image"]);
    }

    #[test]
    fn does_not_treat_divide_operator_as_image_name() {
        let names = extract_image_names(
            "sqrt(abs('/tmp/lhs.image' * 1.5 - '/tmp/rhs.image' / 2.0)) + max('/tmp/lhs.image', '/tmp/rhs.image')",
        );
        assert_eq!(names, vec!["/tmp/lhs.image", "/tmp/rhs.image"]);
    }

    #[test]
    fn no_false_positives_for_numbers() {
        let names = extract_image_names("2.0 + 3.5e10");
        assert!(names.is_empty());
    }

    #[test]
    fn no_false_positives_for_functions() {
        let names = extract_image_names("sin(1.0) + cos(2.0)");
        assert!(names.is_empty());
    }

    #[test]
    fn type_str_round_trip() {
        for pt in [
            PrimitiveType::Float32,
            PrimitiveType::Float64,
            PrimitiveType::Complex32,
            PrimitiveType::Complex64,
        ] {
            assert_eq!(parse_data_type(data_type_str(pt)).unwrap(), pt);
        }
    }

    #[test]
    fn misc_info_json_round_trip() {
        let mut record = RecordValue::default();
        record.push(RecordField::new(
            "telescope",
            Value::Scalar(ScalarValue::String("ALMA".to_string())),
        ));
        record.push(RecordField::new(
            "version",
            Value::Scalar(ScalarValue::Int32(42)),
        ));

        let json = misc_info_to_json(&record);
        let back = json_to_misc_info(&json);

        assert_eq!(back.fields().len(), 2);
        assert_eq!(
            back.get("telescope"),
            Some(&Value::Scalar(ScalarValue::String("ALMA".to_string())))
        );
        assert_eq!(
            back.get("version"),
            Some(&Value::Scalar(ScalarValue::Int32(42)))
        );
    }

    #[test]
    fn is_image_expr_false_for_nonexistent() {
        assert!(!is_image_expr("/nonexistent/path"));
    }

    #[test]
    fn save_and_read_info_round_trip() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.imgexpr");

        let misc = RecordValue::default();
        save(&path, "'a' + 'b'", PrimitiveType::Float32, &misc).unwrap();

        assert!(is_image_expr(&path));

        let info = read_info(&path).unwrap();
        assert_eq!(info.version, 1);
        assert_eq!(info.data_type, PrimitiveType::Float32);
        assert_eq!(info.expr_string, "'a' + 'b'");
    }

    #[test]
    fn extract_bare_identifier_names() {
        // Bare identifiers (no dot or slash) should be extracted as image names
        let names = extract_image_names("myimg + 1.0");
        assert_eq!(names, vec!["myimg"]);
    }

    #[test]
    fn extract_bare_identifier_named_like_function() {
        let names = extract_image_names("sin + 1.0");
        assert_eq!(names, vec!["sin"]);
    }

    #[test]
    fn no_false_positives_for_unknown_function_calls() {
        let names = extract_image_names("future_func('a.image') + 1.0");
        assert_eq!(names, vec!["a.image"]);
    }

    #[test]
    fn open_nested_imgexpr() {
        use casacore_coordinates::CoordinateSystem;

        let dir = tempfile::tempdir().unwrap();

        // Create a source image.
        let img_path = dir.path().join("src.image");
        let mut img =
            PagedImage::<f32>::create(vec![4, 4], CoordinateSystem::new(), &img_path).unwrap();
        img.set(3.0).unwrap();
        img.save().unwrap();

        // Create an inner expression: src.image + 1.0
        let inner_path = dir.path().join("inner.imgexpr");
        save(
            &inner_path,
            "'src.image' + 1.0",
            PrimitiveType::Float32,
            &RecordValue::default(),
        )
        .unwrap();

        // Create an outer expression referencing the inner: inner.imgexpr * 2.0
        let outer_path = dir.path().join("outer.imgexpr");
        save(
            &outer_path,
            "'inner.imgexpr' * 2.0",
            PrimitiveType::Float32,
            &RecordValue::default(),
        )
        .unwrap();

        // Open the outer expression — it should recursively open inner.imgexpr.
        let expr = open::<f32>(&outer_path).unwrap();
        // (3.0 + 1.0) * 2.0 = 8.0
        assert_eq!(expr.get_at(&[0, 0]).unwrap(), 8.0);
    }

    #[test]
    fn open_bare_ident_round_trip() {
        use casacore_coordinates::CoordinateSystem;

        let dir = tempfile::tempdir().unwrap();

        // Create source image with a bare name (no dots/slashes).
        let img_path = dir.path().join("myimg");
        let mut img =
            PagedImage::<f32>::create(vec![4, 4], CoordinateSystem::new(), &img_path).unwrap();
        img.set(5.0).unwrap();
        img.save().unwrap();

        // Save expression using bare identifier.
        let expr_path = dir.path().join("bare.imgexpr");
        save(
            &expr_path,
            "myimg + 1.0",
            PrimitiveType::Float32,
            &RecordValue::default(),
        )
        .unwrap();

        // Reopen — extract_image_names must find "myimg".
        let expr = open::<f32>(&expr_path).unwrap();
        assert_eq!(expr.get_at(&[0, 0]).unwrap(), 6.0);
    }

    #[test]
    fn open_f64_expr_over_f32_source() {
        use casacore_coordinates::CoordinateSystem;

        let dir = tempfile::tempdir().unwrap();

        // Create an f32 source image.
        let img_path = dir.path().join("f32src.image");
        let mut img =
            PagedImage::<f32>::create(vec![4, 4], CoordinateSystem::new(), &img_path).unwrap();
        img.set(7.0).unwrap();
        img.save().unwrap();

        // Save expression with DataType=double.
        let expr_path = dir.path().join("mixed.imgexpr");
        save(
            &expr_path,
            "'f32src.image' * 2.0",
            PrimitiveType::Float64,
            &RecordValue::default(),
        )
        .unwrap();

        // Open as f64 — the f32 source should be auto-converted.
        let expr = open::<f64>(&expr_path).unwrap();
        assert_eq!(expr.get_at(&[0, 0]).unwrap(), 14.0);
    }

    #[test]
    fn open_f64_expr_over_nested_f32_expr() {
        use casacore_coordinates::CoordinateSystem;

        let dir = tempfile::tempdir().unwrap();

        let img_path = dir.path().join("src.image");
        let mut img =
            PagedImage::<f32>::create(vec![4, 4], CoordinateSystem::new(), &img_path).unwrap();
        img.set(7.0).unwrap();
        img.save().unwrap();

        let inner_path = dir.path().join("inner.imgexpr");
        save(
            &inner_path,
            "'src.image' + 1.0",
            PrimitiveType::Float32,
            &RecordValue::default(),
        )
        .unwrap();

        let outer_path = dir.path().join("outer.imgexpr");
        save(
            &outer_path,
            "'inner.imgexpr' * 2.0",
            PrimitiveType::Float64,
            &RecordValue::default(),
        )
        .unwrap();

        let expr = open::<f64>(&outer_path).unwrap();
        assert_eq!(expr.get_at(&[0, 0]).unwrap(), 16.0);
    }
}

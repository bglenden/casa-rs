// SPDX-License-Identifier: LGPL-3.0-or-later

//! Image creation workflows (`fromshape`, `fromarray`, `fromfits`, `fromcomplist`, `maketestimage`).

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use casa_components::ComponentList;
use casa_coordinates::CoordinateSystem;
use casa_core::{Complex32, Complex64, RecordValue, Value};
use casa_images::fits_read::FitsToolImport;
use casa_images::{ImageInterface, ImagePixel, ImagePixelType, MutableImageInterface};
use ndarray::{ArrayD, IxDyn};

use crate::ComponentListImage;

use super::attachment::{AttachedImage, ExternalPathCheck};
use super::coords::{default_record_for_shape, validate_restored_linear_coordinates};
use super::error::{ImageToolError, ImageToolResult, ToolLogLine};
use super::handle::{ImageHandle, Storage, with_storage};
use super::publish::{StagedImage, publish_image, publish_staged_pair};

/// Pixel data variants supported by `fromarray` / `newimagefromarray`.
pub enum PixelArrayInput {
    /// Single-precision real array.
    Float(ArrayD<f32>),
    /// Double-precision real array.
    Double(ArrayD<f64>),
    /// Single-precision complex array.
    Complex(ArrayD<Complex32>),
    /// Double-precision complex array.
    DComplex(ArrayD<Complex64>),
}

/// Parameters for `newimagefromshape`.
pub struct FromShapeRequest<'a> {
    /// Output path (empty creates a temporary image).
    pub outfile: &'a str,
    /// Image shape along each pixel axis.
    pub shape: Vec<usize>,
    /// Optional coordinate record; defaults to `default_record_for_shape`.
    pub csys_record: Option<RecordValue>,
    /// Use linear coordinates when building default coordinates (`csys_record` is `None`).
    pub linear: bool,
    /// Overwrite `outfile` if it already exists.
    pub overwrite: bool,
    /// Emit informational log messages.
    pub log: bool,
    /// Pixel type (`Float32`, `Float64`, `Complex32`, `Complex64`).
    pub pixel_type: ImagePixelType,
}

/// Parses CASA's image pixel type specifier (`"f"`/`"float"`, `"d"`/`"double"`, `"c"`/`"complex"`, `"cd"`/`"dcomplex"`).
pub fn parse_pixel_type(type_str: &str) -> ImageToolResult<ImagePixelType> {
    match type_str.trim().to_ascii_lowercase().as_str() {
        "" | "f" | "float" => Ok(ImagePixelType::Float32),
        "d" | "double" => Ok(ImagePixelType::Float64),
        "c" | "complex" => Ok(ImagePixelType::Complex32),
        "cd" | "dcomplex" => Ok(ImagePixelType::Complex64),
        other => Err(ImageToolError::new(format!(
            "Unsupported pixel type '{other}'; expected 'f', 'd', 'c', or 'cd'."
        ))),
    }
}

/// Creates a zero-filled image of the requested shape, coordinate system, and pixel type.
pub fn create_from_shape(
    request: &FromShapeRequest<'_>,
    record_history: bool,
    is_external_open: ExternalPathCheck<'_>,
) -> ImageToolResult<(AttachedImage, Vec<ToolLogLine>)> {
    if request.shape.is_empty() || request.shape.contains(&0) {
        return Err(ImageToolError::new(format!(
            "Invalid shape {:?}; all dimensions must be positive.",
            request.shape
        )));
    }
    let record = match &request.csys_record {
        Some(record) => record.clone(),
        None => default_record_for_shape(&request.shape, request.linear)?,
    };
    let coords = CoordinateSystem::from_record(&record)?;
    if coords.n_pixel_axes() != request.shape.len() {
        return Err(ImageToolError::new(format!(
            "Shape dimensionality ({}) does not match coordinate system pixel axes ({}).",
            request.shape.len(),
            coords.n_pixel_axes()
        )));
    }

    let handle = publish_image(
        request.outfile,
        request.overwrite,
        is_external_open,
        |dest| {
            let mut h = match request.pixel_type {
                ImagePixelType::Float32 => ImageHandle::Float(Storage::create(
                    request.shape.clone(),
                    coords.clone(),
                    dest,
                    None,
                )?),
                ImagePixelType::Float64 => ImageHandle::Double(Storage::create(
                    request.shape.clone(),
                    coords.clone(),
                    dest,
                    None,
                )?),
                ImagePixelType::Complex32 => ImageHandle::Complex(Storage::create(
                    request.shape.clone(),
                    coords.clone(),
                    dest,
                    None,
                )?),
                ImagePixelType::Complex64 => ImageHandle::DComplex(Storage::create(
                    request.shape.clone(),
                    coords.clone(),
                    dest,
                    None,
                )?),
            };
            h.set_coordinate_record(record.clone())?;
            if record_history {
                h.record_history(
                    "fromshape",
                    format!(
                        "ia.fromshape(outfile={:?}, shape={:?}, linear={}, overwrite={})",
                        request.outfile, request.shape, request.linear, request.overwrite
                    ),
                )?;
            }
            h.save()?;
            Ok(h)
        },
    )?;

    let mut logs = Vec::new();
    if request.log {
        let target = if request.outfile.is_empty() {
            "(temp)image".to_owned()
        } else {
            format!("image '{}'", request.outfile)
        };
        logs.push(ToolLogLine::info(format!(
            "Creating {target} of shape {:?}",
            request.shape
        )));
    }
    Ok((handle, logs))
}

/// Creates an image populated from an in-memory pixel array.
#[allow(clippy::too_many_arguments)]
pub fn create_from_array(
    outfile: &str,
    pixels: PixelArrayInput,
    csys_record: Option<RecordValue>,
    linear: bool,
    overwrite: bool,
    log: bool,
    record_history: bool,
    is_external_open: ExternalPathCheck<'_>,
) -> ImageToolResult<(AttachedImage, Vec<ToolLogLine>)> {
    let shape = match &pixels {
        PixelArrayInput::Float(a) => a.shape().to_vec(),
        PixelArrayInput::Double(a) => a.shape().to_vec(),
        PixelArrayInput::Complex(a) => a.shape().to_vec(),
        PixelArrayInput::DComplex(a) => a.shape().to_vec(),
    };
    if shape.is_empty() || shape.contains(&0) {
        return Err(ImageToolError::new(format!(
            "Invalid array shape {shape:?}; all dimensions must be positive."
        )));
    }
    let record = match csys_record {
        Some(record) => record,
        None => default_record_for_shape(&shape, linear)?,
    };
    let coords = CoordinateSystem::from_record(&record)?;
    if coords.n_pixel_axes() != shape.len() {
        return Err(ImageToolError::new(format!(
            "Array dimensionality ({}) does not match coordinate system pixel axes ({}).",
            shape.len(),
            coords.n_pixel_axes()
        )));
    }

    let handle = publish_image(outfile, overwrite, is_external_open, |dest| {
        let mut h = match &pixels {
            PixelArrayInput::Float(arr) => ImageHandle::Float(Storage::create(
                shape.clone(),
                coords.clone(),
                dest,
                Some(arr),
            )?),
            PixelArrayInput::Double(arr) => ImageHandle::Double(Storage::create(
                shape.clone(),
                coords.clone(),
                dest,
                Some(arr),
            )?),
            PixelArrayInput::Complex(arr) => ImageHandle::Complex(Storage::create(
                shape.clone(),
                coords.clone(),
                dest,
                Some(arr),
            )?),
            PixelArrayInput::DComplex(arr) => ImageHandle::DComplex(Storage::create(
                shape.clone(),
                coords.clone(),
                dest,
                Some(arr),
            )?),
        };
        h.set_coordinate_record(record.clone())?;
        if record_history {
            h.record_history(
                "fromarray",
                format!(
                    "ia.fromarray(outfile={:?}, linear={}, overwrite={})",
                    outfile, linear, overwrite
                ),
            )?;
        }
        h.save()?;
        Ok(h)
    })?;

    let mut logs = Vec::new();
    if log {
        let target = if outfile.is_empty() {
            "(temp)image".to_owned()
        } else {
            format!("image '{outfile}'")
        };
        logs.push(ToolLogLine::info(format!(
            "Creating {target} of shape {shape:?}"
        )));
    }
    Ok((handle, logs))
}

/// Imports a FITS file into a CASA image handle (paged or temporary).
#[allow(clippy::too_many_arguments)]
pub fn create_from_fits(
    outfile: &str,
    fitsfile: &Path,
    whichrep: usize,
    whichhdu: isize,
    zeroblanks: bool,
    overwrite: bool,
    defaultaxes: bool,
    defaultaxesvalues: &[String],
    beam: Option<&RecordValue>,
    record_history: bool,
    is_external_open: ExternalPathCheck<'_>,
) -> ImageToolResult<(AttachedImage, Vec<ToolLogLine>)> {
    let import = FitsToolImport::open(fitsfile, whichrep, whichhdu as i64)?;
    let _ = (defaultaxes, defaultaxesvalues);
    let shape = import.shape().to_vec();
    let coords = import.coordinates().clone();
    let coord_record = import.coordinate_record.clone();
    let units = import.units.clone();
    let mut info = import.image_info.clone();
    if let Some(beam_rec) = beam {
        let parsed = super::beams::parse_beam_record(beam_rec)?;
        let mut parsed_info = casa_images::ImageInfo::from_record(&info)?;
        parsed_info.beam_set = casa_images::ImageBeamSet::new(parsed);
        info = parsed_info.to_record();
    }
    let mut misc = import.misc_info.clone();
    misc.remove("object");
    let history = import.history.clone();

    let reader = ImageHandle::open_fits(fitsfile, import)?;
    let fits_attach = reader
        .fits()
        .ok_or_else(|| ImageToolError::new("Internal FITS attachment error"))?;
    let start = vec![0usize; shape.len()];
    let stride = vec![1usize; shape.len()];
    let mut pixels = fits_attach.read(&start, &shape, &stride)?;
    let mut mask_opt = if fits_attach.has_mask && !zeroblanks {
        let valid = pixels.mapv(|v| !v.is_nan());
        Some(valid)
    } else {
        None
    };
    if zeroblanks {
        pixels.mapv_inplace(|v| if v.is_nan() { 0.0 } else { v });
        mask_opt = None;
    }

    let handle = publish_image(outfile, overwrite, is_external_open, |dest| {
        let mut storage =
            Storage::<f32>::create(shape.clone(), coords.clone(), dest, Some(&pixels))?;
        with_storage!(&mut storage, img => {
            img.set_units(units.clone())?;
            img.set_image_info_record(info.clone())?;
            img.set_misc_info(misc.clone())?;
            img.append_history_entries(&history)?;
            if let Some(mask) = &mask_opt {
                img.put_mask_slice("mask0", mask, &start)?;
                img.set_default_mask("mask0")?;
            }
            Ok::<_, ImageToolError>(())
        })?;
        let mut h = ImageHandle::Float(storage);
        h.set_coordinate_record(coord_record.clone())?;
        if record_history {
            h.record_history(
                "fromfits",
                format!(
                    "ia.fromfits(outfile={:?}, fitsfile={:?}, zeroblanks={}, overwrite={})",
                    outfile,
                    fitsfile.display().to_string(),
                    zeroblanks,
                    overwrite
                ),
            )?;
        }
        h.save()?;
        Ok(h)
    })?;

    Ok((
        handle,
        vec![ToolLogLine::info(format!(
            "Imported FITS image {} with shape {shape:?}",
            fitsfile.display()
        ))],
    ))
}

/// Creates a [`ComponentListImage`]-backed [`AttachedImage`] (`fromcomplist`).
#[allow(clippy::too_many_arguments)]
pub fn create_from_component_list(
    outfile: &str,
    shape: Vec<usize>,
    coordinates: CoordinateSystem,
    coordinate_record: RecordValue,
    spectral_records: &BTreeMap<usize, RecordValue>,
    components: ComponentList,
    cache: bool,
    overwrite: bool,
    log: bool,
    is_external_open: ExternalPathCheck<'_>,
) -> ImageToolResult<(AttachedImage, Vec<ToolLogLine>)> {
    let image = ComponentListImage::new(shape.clone(), components, coordinates, cache)?;
    let attached = if outfile.is_empty() {
        AttachedImage::temporary(ImageHandle::from_component(image, coordinate_record)?)
    } else {
        let attached = publish_image(outfile, overwrite, is_external_open, |dest| {
            let stage = dest.ok_or_else(|| ImageToolError::new("Missing componentlist stage"))?;
            image.write_native(stage, &coordinate_record, spectral_records)?;
            Ok(ImageHandle::open_native_with_cache(stage, None, cache)?)
        })?;
        if !cache {
            attached.lock().disable_component_cache()?;
        }
        attached
    };
    let mut logs = Vec::new();
    if log {
        logs.push(ToolLogLine::info(format!(
            "Created componentlist image of shape {shape:?}"
        )));
    }
    Ok((attached, logs))
}

/// Creates CASA's canonical small FITS test image on disk/temp (`maketestimage`) from a resolved FITS path.
pub fn create_test_image_from_fits(
    outfile: &str,
    fits_path: &Path,
    overwrite: bool,
    record_history: bool,
    is_external_open: ExternalPathCheck<'_>,
) -> ImageToolResult<(AttachedImage, Vec<ToolLogLine>)> {
    if !fits_path.exists() {
        return Err(ImageToolError::new(format!(
            "Cannot locate test FITS image at {}",
            fits_path.display()
        )));
    }
    create_from_fits(
        outfile,
        fits_path,
        0,
        -1,
        false,
        overwrite,
        false,
        &[],
        None,
        record_history,
        is_external_open,
    )
}

/// Copies all pixels and masks between two typed storage buffers of identical shape.
pub fn copy_typed_storage<T: ImagePixel>(
    src: &dyn ImageInterface<T>,
    dst: &mut dyn MutableImageInterface<T>,
) -> ImageToolResult<()> {
    let shape = src.shape().to_vec();
    let start = vec![0usize; shape.len()];
    let stride = vec![1usize; shape.len()];
    let pixels = src.get_slice(&start, &shape, &stride)?;
    dst.put_slice(&pixels, &start)?;
    for mask_name in src.mask_names() {
        let mask = src
            .get_mask_slice(&start, &shape, &stride)?
            .unwrap_or_else(|| ArrayD::from_elem(IxDyn(&shape), true));
        dst.put_mask_slice(&mask_name, &mask, &start)?;
    }
    if let Some(default) = src.default_mask_name() {
        dst.set_default_mask(&default)?;
    }
    Ok(())
}

/// Request parameters for CASA `fromshape`/`fromarray`/`newimagefromshape`/`newimagefromarray`.
pub struct CasaCreateRequest<'a> {
    /// CASA creation method name (`"fromshape"`, `"fromarray"`, `"newimagefromshape"`, `"newimagefromarray"`).
    pub method: &'a str,
    /// User-supplied output path (empty creates a temporary in-memory image).
    pub outfile: &'a str,
    /// Deferred path-expansion result for `outfile`, checked after parameter validation.
    pub expanded: Result<&'a str, &'a str>,
    /// Explicit coordinate system record (empty builds default coordinates).
    pub csys_record: RecordValue,
    /// Use linear coordinates when building default coordinates.
    pub linear: bool,
    /// Overwrite existing `outfile`.
    pub overwrite: bool,
    /// Log creation message.
    pub log: bool,
    /// Pixel type string (`"f"`, `"d"`, `"c"`, `"cd"`).
    pub kind: &'a str,
}

/// Pixel payload and formatted history string for CASA `fromarray`/`newimagefromarray`.
pub struct CasaArrayPayload {
    /// Pixel values array.
    pub pixels: PixelArrayInput,
    /// Preformatted pixel array text for history entries.
    pub history_pixels: String,
}

/// Input source for CASA `ia.fromcomplist`.
pub enum ComponentListSource {
    /// Parsed record model and per-component spectral records.
    Loaded {
        /// Parsed component list.
        components: ComponentList,
        /// Per-component spectral records.
        spectral_records: BTreeMap<usize, RecordValue>,
        /// Any informational log messages produced while parsing the record.
        logs: Vec<ToolLogLine>,
    },
    /// Path to an on-disk component list table.
    Table(PathBuf),
}

/// Creates a CASA image matching `fromshape` / `fromarray` / `newimagefromshape` / `newimagefromarray`.
pub fn create_casa_image(
    request: &CasaCreateRequest<'_>,
    shape_arg: Option<&[i64]>,
    array_arg: Option<CasaArrayPayload>,
    history: bool,
    is_external_open: ExternalPathCheck<'_>,
) -> ImageToolResult<AttachedImage> {
    let (shape, pixels, history_pixels, is_complex_input) = if let Some(payload) = array_arg {
        if !matches!(request.kind.to_ascii_lowercase().as_str(), "f" | "d") {
            return Err(ImageToolError::new(format!(
                "Exception: Unsupported value for type: {:?}. Supported values are \"d\" and \"f\"",
                request.kind
            )));
        }
        let shape = match &payload.pixels {
            PixelArrayInput::Float(a) => a.shape().to_vec(),
            PixelArrayInput::Double(a) => a.shape().to_vec(),
            PixelArrayInput::Complex(a) => a.shape().to_vec(),
            PixelArrayInput::DComplex(a) => a.shape().to_vec(),
        };
        let is_complex = matches!(
            &payload.pixels,
            PixelArrayInput::Complex(_) | PixelArrayInput::DComplex(_)
        );
        (
            shape,
            Some(payload.pixels),
            Some(payload.history_pixels),
            is_complex,
        )
    } else {
        let shape_raw = shape_arg.unwrap_or(&[0]);
        if shape_raw.is_empty() {
            return Err(ImageToolError::new(
                "Exception: The shape must have more than zero elements.",
            ));
        }
        if shape_raw.iter().any(|&length| length <= 0) {
            return Err(ImageToolError::new(
                "Exception: All elements of shape must be positive.",
            ));
        }
        let shape: Vec<usize> = shape_raw.iter().map(|&length| length as usize).collect();
        (shape, None, None, false)
    };

    let record = if request.csys_record.fields().is_empty() {
        default_record_for_shape(&shape, request.linear)?
    } else if let [field] = request.csys_record.fields()
        && let Value::Record(record) = &field.value
    {
        record.clone()
    } else {
        request.csys_record.clone()
    };
    let coordinates = CoordinateSystem::from_record(&record)
        .map_err(|_| ImageToolError::new("Exception: Invalid coordinate system record"))?;
    if !request.csys_record.is_empty() {
        validate_restored_linear_coordinates(&coordinates, &record)?;
    }
    let kind_lower = request.kind.to_ascii_lowercase();
    let pixel_type = match (kind_lower.as_str(), is_complex_input) {
        ("f", false) => ImagePixelType::Float32,
        ("d", false) => ImagePixelType::Float64,
        ("c", _) | ("f", true) => ImagePixelType::Complex32,
        ("cd", _) | ("d", true) => ImagePixelType::Complex64,
        _ => {
            return Err(ImageToolError::new(
                "Exception: Input parm type must be either 'f', 'c', 'd', or 'cd'",
            ));
        }
    };
    if coordinates.n_pixel_axes() != shape.len() {
        return Err(ImageToolError::new(
            "Exception: Supplied CoordinateSystem and image shape are inconsistent.",
        ));
    }

    let expanded = request.expanded.map_err(ImageToolError::new)?;
    let primary = StagedImage::allocate_creation(request.outfile, expanded, request.overwrite)?;
    publish_staged_pair(
        primary,
        None,
        request.overwrite,
        is_external_open,
        |path, _| {
            let mut image = match pixel_type {
                ImagePixelType::Float32 => {
                    let data = pixels.as_ref().map(|p| match p {
                        PixelArrayInput::Float(a) => a.clone(),
                        PixelArrayInput::Double(a) => a.mapv(|v| v as f32),
                        _ => unreachable!("real pixels"),
                    });
                    ImageHandle::Float(Storage::create(
                        shape.clone(),
                        coordinates,
                        path,
                        data.as_ref(),
                    )?)
                }
                ImagePixelType::Float64 => {
                    let data = pixels.as_ref().map(|p| match p {
                        PixelArrayInput::Double(a) => a.clone(),
                        PixelArrayInput::Float(a) => a.mapv(f64::from),
                        _ => unreachable!("real pixels"),
                    });
                    ImageHandle::Double(Storage::create(
                        shape.clone(),
                        coordinates,
                        path,
                        data.as_ref(),
                    )?)
                }
                ImagePixelType::Complex32 => {
                    let data = pixels.as_ref().map(|p| match p {
                        PixelArrayInput::Complex(a) => a.clone(),
                        PixelArrayInput::DComplex(a) => {
                            a.mapv(|v| Complex32::new(v.re as f32, v.im as f32))
                        }
                        PixelArrayInput::Float(a) => a.mapv(|v| Complex32::new(v, 0.0)),
                        PixelArrayInput::Double(a) => a.mapv(|v| Complex32::new(v as f32, 0.0)),
                    });
                    ImageHandle::Complex(Storage::create(
                        shape.clone(),
                        coordinates,
                        path,
                        data.as_ref(),
                    )?)
                }
                ImagePixelType::Complex64 => {
                    let data = pixels.as_ref().map(|p| match p {
                        PixelArrayInput::DComplex(a) => a.clone(),
                        PixelArrayInput::Complex(a) => {
                            a.mapv(|v| Complex64::new(v.re.into(), v.im.into()))
                        }
                        PixelArrayInput::Double(a) => a.mapv(|v| Complex64::new(v, 0.0)),
                        PixelArrayInput::Float(a) => a.mapv(|v| Complex64::new(v.into(), 0.0)),
                    });
                    ImageHandle::DComplex(Storage::create(
                        shape.clone(),
                        coordinates,
                        path,
                        data.as_ref(),
                    )?)
                }
            };
            image.set_coordinate_record(record)?;
            let (storage_name, filename) = if request.outfile.is_empty() {
                ("Temp", String::new())
            } else {
                ("Paged", format!("'{}'", request.outfile))
            };
            let pixel_label = match image {
                ImageHandle::Float(_) => "float",
                ImageHandle::Double(_) => "double",
                ImageHandle::Complex(_) => "Complex",
                ImageHandle::DComplex(_) => "DComplex",
            };
            let creation = format!(
                "Created {storage_name} image {filename} of shape {shape:?} with {pixel_label} valued pixels.",
            );
            image.append_history_at("::ImageFactory::createImage", &[creation])?;
            if history {
                let args = match history_pixels.as_deref() {
                    Some(hist) if request.method == "newimagefromarray" => {
                        format!("outfile={:?}, pixels={hist}", request.outfile)
                    }
                    Some(hist) => format!("pixels={hist}"),
                    None => format!(
                        "outfile={:?}, shape={}",
                        request.outfile,
                        format!("{shape:?}").replace(' ', ""),
                    ),
                };
                let kind_arg = if request.method == "newimagefromarray" {
                    String::new()
                } else {
                    format!(", type={:?}", request.kind)
                };
                let call = format!(
                    "ia.{}({args}, csys={{...}}, linear={}, overwrite={}, log={}{kind_arg})",
                    request.method, request.linear, request.overwrite, request.log
                );
                image.record_history(request.method, call)?;
            }
            image.save()?;
            Ok((image, None))
        },
    )
}

/// Creates a CASA `ComponentListImage` matching `ia.fromcomplist`.
#[allow(clippy::too_many_arguments)]
pub fn create_casa_complist_image(
    outfile: &str,
    expanded: &str,
    shape: &[i64],
    csys_record: RecordValue,
    source: ComponentListSource,
    overwrite: bool,
    log: bool,
    cache: bool,
    history: bool,
    is_external_open: ExternalPathCheck<'_>,
) -> ImageToolResult<(AttachedImage, Vec<ToolLogLine>)> {
    if !(2..=4).contains(&shape.len()) {
        return Err(ImageToolError::new(
            "ComponentListImages must have 2, 3, or 4 dimensions",
        ));
    }
    if shape.iter().any(|&length| length <= 0) {
        return Err(ImageToolError::new("TiledShape: shape has to be > 0"));
    }
    let shape: Vec<usize> = shape.iter().map(|&length| length as usize).collect();
    let record = if csys_record.fields().is_empty() {
        default_record_for_shape(&shape, false)?
    } else if let [field] = csys_record.fields()
        && let Value::Record(record) = &field.value
    {
        record.clone()
    } else {
        csys_record.clone()
    };
    let coordinates = CoordinateSystem::from_record(&record)
        .map_err(|_| ImageToolError::new("Exception: Invalid coordinate system record"))?;
    if !csys_record.is_empty() {
        validate_restored_linear_coordinates(&coordinates, &record)?;
    }
    if coordinates.n_pixel_axes() != shape.len() {
        return Err(ImageToolError::new(
            "Exception: Supplied CoordinateSystem and image shape are inconsistent.",
        ));
    }

    let stage = StagedImage::allocate_creation(outfile, expanded, overwrite)?;
    let (components, spectral_records, mut messages) = match source {
        ComponentListSource::Loaded {
            components,
            spectral_records,
            logs,
        } => (components, spectral_records, logs),
        ComponentListSource::Table(path) => {
            let components = ComponentList::read_table(path)
                .map_err(|err| ImageToolError::new(err.to_string()))?;
            let table_record = components
                .to_record()
                .map_err(|err| ImageToolError::new(err.to_string()))?;
            (
                ComponentList::from_record(&table_record)
                    .map_err(|err| ImageToolError::new(err.to_string()))?,
                BTreeMap::new(),
                Vec::new(),
            )
        }
    };
    let message = format!("Created ComponentListImage {outfile} of shape {shape:?}.");
    let comp_image = ComponentListImage::new(shape.clone(), components, coordinates, cache)?;
    let attachment = publish_staged_pair(stage, None, overwrite, is_external_open, |path, _| {
        if let Some(path) = path {
            comp_image.write_native(path, &record, &spectral_records)?;
        }
        let mut image = ImageHandle::from_component(comp_image, record)?;
        if path.is_none() {
            image.append_history_at(
                "ImageFactory::createComponentListImage",
                std::slice::from_ref(&message),
            )?;
            if history {
                image.record_history(
                    "fromcomplist",
                    format!(
                        "ia.fromcomplist(outfile={outfile:?}, shape={shape:?}, cl={{...}}, csys={{...}}, overwrite={overwrite}, log={log}, cache={cache})"
                    ),
                )?;
            }
        }
        Ok((image, None))
    })?;
    if !cache && !outfile.is_empty() {
        attachment.lock().disable_component_cache()?;
    }
    if log {
        messages.push(ToolLogLine::info(message));
    }
    Ok((attachment, messages))
}

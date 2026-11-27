// SPDX-License-Identifier: LGPL-3.0-or-later

//! Pixel mask inspection and management (`ia.maskhandler`).

use std::collections::BTreeSet;
use std::path::Path;

use casa_images::ImageInterface;
use ndarray::{ArrayD, IxDyn};

use super::attachment::AttachedImage;
use super::error::{ImageToolError, ImageToolResult};
use super::handle::{ImageHandle, Storage, with_pixels, with_storage};

/// Validates that a pixel mask name contains no directory separators.
pub fn validate_mask_name(name: &str) -> ImageToolResult<()> {
    if Path::new(name).file_name().and_then(|n| n.to_str()) != Some(name) {
        return Err(ImageToolError::new(format!("Invalid mask name {name}")));
    }
    Ok(())
}

/// Returns `true` if `image` already defines a mask or region named `name`.
pub fn has_region_or_mask(image: &ImageHandle, name: &str) -> bool {
    with_pixels!(image, storage => match storage {
        Storage::Paged(image) => image
            .mask_names()
            .into_iter()
            .chain(image.region_names())
            .any(|existing| existing == name),
        Storage::Temp { image, .. } => image.mask_names().iter().any(|existing| existing == name),
    })
}

/// Result returned by [`mask_handler`].
#[derive(Debug, Clone)]
pub enum MaskHandlerResult {
    /// List of mask names (returned for `"get"` and `"default"`).
    Names(Vec<String>),
    /// Status flag (returned for mutating ops `"set"`, `"delete"`, `"rename"`, `"copy"`).
    Done(bool),
}

#[derive(Clone, Copy)]
enum MaskOperation {
    Set,
    Default,
    Delete,
    Rename,
    Get,
    Copy,
}

impl MaskOperation {
    fn parse(argument: &str) -> ImageToolResult<Self> {
        let upper = argument.to_ascii_uppercase();
        match upper.as_bytes() {
            [b'S', b'E', b'T', ..] => Ok(Self::Set),
            [b'D', b'E', b'F', ..] => Ok(Self::Default),
            [b'D', b'E', b'L', ..] => Ok(Self::Delete),
            [b'R', b'E', b'N', ..] => Ok(Self::Rename),
            [b'G', b'E', b'T', ..] => Ok(Self::Get),
            [b'C', b'O', b'P', ..] => Ok(Self::Copy),
            _ => Err(ImageToolError::new(format!("Unknown operation {upper}"))),
        }
    }
}

fn named_mask(image: &ImageHandle, name: &str) -> ImageToolResult<ArrayD<bool>> {
    Ok(with_pixels!(image, storage => with_storage!(storage, image => image.get_named_mask(name)))?)
}

fn copy_mask(image: &mut ImageHandle, from: &str, to: &str) -> ImageToolResult<()> {
    validate_mask_name(to)?;
    if has_region_or_mask(image, to) {
        return Err(ImageToolError::new(format!(
            "Exception: Mask {to} already exists."
        )));
    }
    let mask = if let Some((source, mask)) = from.split_once(':') {
        if mask.contains(':') {
            return Err(ImageToolError::new(format!(
                "Exception: Illegal mask specification {from}."
            )));
        }
        let source = ImageHandle::open(Path::new(source))?;
        if source.shape() != image.shape() {
            return Err(ImageToolError::new(
                "Exception: Images have different shapes.",
            ));
        }
        source.check_single_precision("maskhandler copy")?;
        named_mask(&source, mask)?
    } else {
        named_mask(image, from)?
    };
    with_pixels!(image, storage => match storage {
        Storage::Paged(image) => image.put_mask(to, &mask),
        Storage::Temp { image, .. } => image
            .define_stored_mask(Some(to), false, false, &mask)
            .and_then(|written| if written { Ok(()) } else { image.put_mask(to, &mask) }),
    })?;
    Ok(())
}

fn apply_mask_operation(
    image: &mut ImageHandle,
    operation: MaskOperation,
    names: &[String],
) -> ImageToolResult<Vec<String>> {
    let current =
        with_pixels!(&*image, storage => with_storage!(storage, image => image.mask_names()));
    let no_region_handler =
        image.read_only_file_type().is_some() || image.image_type_name() == "ImageConcat";
    match operation {
        MaskOperation::Get => {
            return Ok(if current.is_empty() {
                vec!["T".into()]
            } else {
                current
            });
        }
        MaskOperation::Default => {
            let default = with_pixels!(&*image, storage => with_storage!(storage, image => image.default_mask_name()));
            return Ok(vec![default.unwrap_or_default()]);
        }
        MaskOperation::Set => {
            if no_region_handler {
                return Err(ImageToolError::new(
                    "RegionHandler::setDefaultMask cannot be used for this image type",
                ));
            }
            let name = names.first().map_or("", String::as_str);
            if name.is_empty() {
                with_pixels!(image, storage => with_storage!(storage, image => image.unset_default_mask()))?;
            } else {
                if !current.iter().any(|candidate| candidate == name) {
                    let image_name = image
                        .path()
                        .map_or_else(|| "Temporary_Image".into(), |p| p.display().to_string());
                    return Err(ImageToolError::new(format!(
                        "RegionHandlerTable: mask {name} does not exist in table {image_name}"
                    )));
                }
                with_pixels!(image, storage => with_storage!(storage, image => image.set_default_mask(name)))?;
            }
        }
        MaskOperation::Delete => {
            if names.is_empty() {
                return Err(ImageToolError::new(
                    "Exception: You have not supplied any mask names.",
                ));
            }
            for name in names.iter().filter(|name| current.contains(name)) {
                validate_mask_name(name)?;
            }
            let unique: BTreeSet<_> = names.iter().collect();
            for name in unique.into_iter().filter(|name| current.contains(name)) {
                with_pixels!(image, storage => with_storage!(storage, image => image.remove_mask(name)))?;
            }
        }
        MaskOperation::Rename => {
            let [from, to] = names else {
                return Err(ImageToolError::new(format!(
                    "Exception: name must be an array of size exactly two. {} values were given.",
                    names.len()
                )));
            };
            if no_region_handler {
                return Err(ImageToolError::new(
                    "RegionHandler::renameRegion cannot be used for this image type",
                ));
            }
            if current.contains(from) {
                validate_mask_name(to)?;
                if has_region_or_mask(image, to) {
                    return Err(ImageToolError::new(format!(
                        "Exception: Mask {to} already exists."
                    )));
                }
                with_pixels!(image, storage => with_storage!(storage, image => image.rename_mask(from, to)))?;
            }
        }
        MaskOperation::Copy => {
            let [from, to, ..] = names else {
                return Err(ImageToolError::new(
                    "Exception: Supply the original and new mask names.",
                ));
            };
            if no_region_handler {
                return Err(ImageToolError::new(
                    "RegionHandler::makeMask - cannot create mask for virtual image",
                ));
            }
            copy_mask(image, from, to)?;
        }
    }
    Ok(vec!["T".into()])
}

/// Executes CASA's `ia.maskhandler(op, name)` returning exact CASA string vectors (e.g. `["T"]`).
pub fn mask_handler_casa(
    attached: &AttachedImage,
    op: &str,
    names: &[String],
    record_history: bool,
) -> ImageToolResult<Vec<String>> {
    let operation = MaskOperation::parse(op)?;
    let mut image = attached.lock();
    let writes = matches!(
        operation,
        MaskOperation::Set | MaskOperation::Rename | MaskOperation::Delete | MaskOperation::Copy
    );
    let kind = if writes {
        casa_tables::LockType::Write
    } else {
        casa_tables::LockType::Read
    };
    if image.image_type_name() == "ImageConcat" {
        attached.check_bound_name(&image)?;
    } else {
        attached.require_lock(&mut image, kind)?;
    }
    let result = apply_mask_operation(&mut image, operation, names)?;
    if writes {
        if record_history
            && (!matches!(operation, MaskOperation::Copy)
                || op.to_ascii_uppercase().starts_with("COPY"))
        {
            let formatted_names = names
                .iter()
                .map(|n| format!("{n:?}"))
                .collect::<Vec<_>>()
                .join(",");
            image.record_history(
                "maskhandler",
                format!("ia.maskhandler(op={op:?}, name=[{formatted_names}])"),
            )?;
        }
        image.save()?;
    }
    Ok(result)
}

/// Executes `ia.maskhandler(op, name)` returning typed [`MaskHandlerResult`].
pub fn mask_handler(
    attached: &AttachedImage,
    op: &str,
    name: &[String],
    record_history: bool,
) -> ImageToolResult<MaskHandlerResult> {
    let op_lower = op.trim().to_ascii_lowercase();
    match op_lower.as_str() {
        "default" => {
            let mut image = attached.lock();
            attached.require_pixel_read_lock(&mut image)?;
            let def = if let Some(fits) = image.fits() {
                fits.has_mask.then(|| "mask0".to_owned())
            } else {
                with_pixels!(&*image, storage => {
                    with_storage!(storage, img => img.default_mask_name())
                })
            };
            Ok(MaskHandlerResult::Names(def.into_iter().collect()))
        }
        "get" => {
            let mut image = attached.lock();
            attached.require_pixel_read_lock(&mut image)?;
            let names = if let Some(fits) = image.fits() {
                if fits.has_mask {
                    vec!["mask0".to_owned()]
                } else {
                    Vec::new()
                }
            } else {
                with_pixels!(&*image, storage => {
                    with_storage!(storage, img => img.mask_names())
                })
            };
            Ok(MaskHandlerResult::Names(names))
        }
        "set" => {
            let mut image = attached.lock();
            attached.require_write_lock(&mut image, "maskhandler(set)")?;
            let target = name.first().map(String::as_str).unwrap_or("");
            if target.is_empty() {
                clear_default_mask(&mut image)?;
            } else {
                set_default_mask_impl(&mut image, target)?;
            }
            if record_history {
                image.record_history(
                    "maskhandler",
                    format!("ia.maskhandler(op='set', name={name:?})"),
                )?;
            }
            image.save()?;
            Ok(MaskHandlerResult::Done(true))
        }
        "delete" => {
            if name.is_empty() {
                return Err(ImageToolError::new(
                    "maskhandler('delete') requires at least one mask name.",
                ));
            }
            let mut image = attached.lock();
            attached.require_write_lock(&mut image, "maskhandler(delete)")?;
            for mask in name {
                delete_mask_impl(&mut image, mask)?;
            }
            if record_history {
                image.record_history(
                    "maskhandler",
                    format!("ia.maskhandler(op='delete', name={name:?})"),
                )?;
            }
            image.save()?;
            Ok(MaskHandlerResult::Done(true))
        }
        "rename" => {
            if name.len() != 2 {
                return Err(ImageToolError::new(
                    "maskhandler('rename') requires [old_name, new_name].",
                ));
            }
            let mut image = attached.lock();
            attached.require_write_lock(&mut image, "maskhandler(rename)")?;
            copy_mask_impl(&mut image, &name[0], &name[1])?;
            delete_mask_impl(&mut image, &name[0])?;
            if record_history {
                image.record_history(
                    "maskhandler",
                    format!("ia.maskhandler(op='rename', name={name:?})"),
                )?;
            }
            image.save()?;
            Ok(MaskHandlerResult::Done(true))
        }
        "copy" => {
            if name.len() != 2 {
                return Err(ImageToolError::new(
                    "maskhandler('copy') requires [source_mask, dest_mask].",
                ));
            }
            let mut image = attached.lock();
            attached.require_write_lock(&mut image, "maskhandler(copy)")?;
            copy_mask_impl(&mut image, &name[0], &name[1])?;
            if record_history {
                image.record_history(
                    "maskhandler",
                    format!("ia.maskhandler(op='copy', name={name:?})"),
                )?;
            }
            image.save()?;
            Ok(MaskHandlerResult::Done(true))
        }
        other => Err(ImageToolError::new(format!(
            "Unknown maskhandler operation '{other}'; expected 'default', 'get', 'set', 'delete', 'rename', or 'copy'."
        ))),
    }
}

fn clear_default_mask(image: &mut ImageHandle) -> ImageToolResult<()> {
    match image {
        ImageHandle::Float(Storage::Paged(p)) => p.unset_default_mask()?,
        ImageHandle::Float(Storage::Temp { image: t, .. }) => t.unset_default_mask()?,
        ImageHandle::Double(Storage::Paged(p)) => p.unset_default_mask()?,
        ImageHandle::Double(Storage::Temp { image: t, .. }) => t.unset_default_mask()?,
        ImageHandle::Complex(Storage::Paged(p)) => p.unset_default_mask()?,
        ImageHandle::Complex(Storage::Temp { image: t, .. }) => t.unset_default_mask()?,
        ImageHandle::DComplex(Storage::Paged(p)) => p.unset_default_mask()?,
        ImageHandle::DComplex(Storage::Temp { image: t, .. }) => t.unset_default_mask()?,
    }
    Ok(())
}

fn set_default_mask_impl(image: &mut ImageHandle, name: &str) -> ImageToolResult<()> {
    match image {
        ImageHandle::Float(Storage::Paged(p)) => p.set_default_mask(name)?,
        ImageHandle::Float(Storage::Temp { image: t, .. }) => t.set_default_mask(name)?,
        ImageHandle::Double(Storage::Paged(p)) => p.set_default_mask(name)?,
        ImageHandle::Double(Storage::Temp { image: t, .. }) => t.set_default_mask(name)?,
        ImageHandle::Complex(Storage::Paged(p)) => p.set_default_mask(name)?,
        ImageHandle::Complex(Storage::Temp { image: t, .. }) => t.set_default_mask(name)?,
        ImageHandle::DComplex(Storage::Paged(p)) => p.set_default_mask(name)?,
        ImageHandle::DComplex(Storage::Temp { image: t, .. }) => t.set_default_mask(name)?,
    }
    Ok(())
}

fn delete_mask_impl(image: &mut ImageHandle, name: &str) -> ImageToolResult<()> {
    match image {
        ImageHandle::Float(Storage::Paged(p)) => p.remove_mask(name)?,
        ImageHandle::Float(Storage::Temp { image: t, .. }) => t.remove_mask(name)?,
        ImageHandle::Double(Storage::Paged(p)) => p.remove_mask(name)?,
        ImageHandle::Double(Storage::Temp { image: t, .. }) => t.remove_mask(name)?,
        ImageHandle::Complex(Storage::Paged(p)) => p.remove_mask(name)?,
        ImageHandle::Complex(Storage::Temp { image: t, .. }) => t.remove_mask(name)?,
        ImageHandle::DComplex(Storage::Paged(p)) => p.remove_mask(name)?,
        ImageHandle::DComplex(Storage::Temp { image: t, .. }) => t.remove_mask(name)?,
    }
    Ok(())
}

fn copy_mask_impl(image: &mut ImageHandle, src: &str, dst: &str) -> ImageToolResult<()> {
    let shape = image.shape().to_vec();
    let start = vec![0usize; shape.len()];
    let stride = vec![1usize; shape.len()];
    let prev_default = with_pixels!(image, storage => {
        with_storage!(storage, img => img.default_mask_name())
    });
    set_default_mask_impl(image, src)?;
    let mask = with_pixels!(image, storage => {
        with_storage!(storage, img => img.get_mask_slice(&start, &shape, &stride)?)
    })
    .unwrap_or_else(|| ArrayD::from_elem(IxDyn(&shape), true));

    match image {
        ImageHandle::Float(Storage::Paged(p)) => p.put_mask_slice(dst, &mask, &start)?,
        ImageHandle::Float(Storage::Temp { image: t, .. }) => {
            t.put_mask_slice(dst, &mask, &start)?;
        }
        ImageHandle::Double(Storage::Paged(p)) => p.put_mask_slice(dst, &mask, &start)?,
        ImageHandle::Double(Storage::Temp { image: t, .. }) => {
            t.put_mask_slice(dst, &mask, &start)?;
        }
        ImageHandle::Complex(Storage::Paged(p)) => p.put_mask_slice(dst, &mask, &start)?,
        ImageHandle::Complex(Storage::Temp { image: t, .. }) => {
            t.put_mask_slice(dst, &mask, &start)?;
        }
        ImageHandle::DComplex(Storage::Paged(p)) => p.put_mask_slice(dst, &mask, &start)?,
        ImageHandle::DComplex(Storage::Temp { image: t, .. }) => {
            t.put_mask_slice(dst, &mask, &start)?;
        }
    }
    if let Some(prev) = prev_default {
        set_default_mask_impl(image, &prev)?;
    } else {
        clear_default_mask(image)?;
    }
    Ok(())
}

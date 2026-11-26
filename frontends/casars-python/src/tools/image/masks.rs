// SPDX-License-Identifier: LGPL-3.0-or-later

use std::path::Path;

use ndarray::ArrayD;
use pyo3::prelude::*;

use super::PyImageTool;
use super::handle::{ImageHandle, Storage, raised, with_pixels, with_storage};
use super::records::string_vector;

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
    fn parse(argument: &str) -> PyResult<Self> {
        let upper = argument.to_ascii_uppercase();
        match upper.as_bytes() {
            [b'S', b'E', b'T', ..] => Ok(Self::Set),
            [b'D', b'E', b'F', ..] => Ok(Self::Default),
            [b'D', b'E', b'L', ..] => Ok(Self::Delete),
            [b'R', b'E', b'N', ..] => Ok(Self::Rename),
            [b'G', b'E', b'T', ..] => Ok(Self::Get),
            [b'C', b'O', b'P', ..] => Ok(Self::Copy),
            _ => Err(raised(format!("Unknown operation {upper}"))),
        }
    }
}

pub(super) fn validate_name(name: &str) -> PyResult<()> {
    if Path::new(name).file_name().and_then(|n| n.to_str()) != Some(name) {
        return Err(raised(format!("Invalid mask name {name}")));
    }
    Ok(())
}

fn named_mask(image: &ImageHandle, name: &str) -> PyResult<ArrayD<bool>> {
    with_pixels!(image, storage => with_storage!(storage, image => image.get_named_mask(name)))
        .map_err(raised)
}

pub(super) fn has_region(image: &ImageHandle, to: &str) -> bool {
    with_pixels!(image, storage => match storage {
        Storage::Paged(image) => image.mask_names().into_iter()
            .chain(image.region_names())
            .any(|name| name == to),
        Storage::Temp { image, .. } => image.mask_names().iter().any(|name| name == to),
    })
}

fn copy_mask(image: &mut ImageHandle, from: &str, to: &str) -> PyResult<()> {
    validate_name(to)?;
    if has_region(image, to) {
        return Err(raised(format!("Exception: Mask {to} already exists.")));
    }
    let mask = if let Some((source, mask)) = from.split_once(':') {
        if mask.contains(':') {
            return Err(raised(format!(
                "Exception: Illegal mask specification {from}."
            )));
        }
        let source = ImageHandle::open(Path::new(source)).map_err(raised)?;
        if source.shape() != image.shape() {
            return Err(raised("Exception: Images have different shapes."));
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
    })
    .map_err(raised)
}

fn apply_mask_operation(
    image: &mut ImageHandle,
    operation: MaskOperation,
    names: &[String],
) -> PyResult<Vec<String>> {
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
                return Err(raised(
                    "RegionHandler::setDefaultMask cannot be used for this image type",
                ));
            }
            let name = names.first().map_or("", String::as_str);
            if name.is_empty() {
                with_pixels!(image, storage => with_storage!(storage, image => image.unset_default_mask())).map_err(raised)?;
            } else {
                if !current.iter().any(|candidate| candidate == name) {
                    let image = image
                        .path()
                        .map_or_else(|| "Temporary_Image".into(), |p| p.display().to_string());
                    return Err(raised(format!(
                        "RegionHandlerTable: mask {name} does not exist in table {image}"
                    )));
                }
                with_pixels!(image, storage => with_storage!(storage, image => image.set_default_mask(name))).map_err(raised)?;
            }
        }
        MaskOperation::Delete => {
            if names.is_empty() {
                return Err(raised("Exception: You have not supplied any mask names."));
            }
            for name in names.iter().filter(|name| current.contains(name)) {
                validate_name(name)?;
            }
            let unique: std::collections::BTreeSet<_> = names.iter().collect();
            for name in unique.into_iter().filter(|name| current.contains(name)) {
                with_pixels!(image, storage => with_storage!(storage, image => image.remove_mask(name))).map_err(raised)?;
            }
        }
        MaskOperation::Rename => {
            let [from, to] = names else {
                return Err(raised(format!(
                    "Exception: name must be an array of size exactly two. {} values were given.",
                    names.len()
                )));
            };
            if no_region_handler {
                return Err(raised(
                    "RegionHandler::renameRegion cannot be used for this image type",
                ));
            }
            if current.contains(from) {
                validate_name(to)?;
                if has_region(image, to) {
                    return Err(raised(format!("Exception: Mask {to} already exists.")));
                }
                with_pixels!(image, storage => with_storage!(storage, image => image.rename_mask(from, to))).map_err(raised)?;
            }
        }
        MaskOperation::Copy => {
            let [from, to, ..] = names else {
                return Err(raised("Exception: Supply the original and new mask names."));
            };
            if no_region_handler {
                return Err(raised(
                    "RegionHandler::makeMask - cannot create mask for virtual image",
                ));
            }
            copy_mask(image, from, to)?;
        }
    }
    Ok(vec!["T".into()])
}

#[pymethods]
impl PyImageTool {
    #[pyo3(signature = (op="default", name=None), text_signature = "($self, op='default', name=[])")]
    fn maskhandler(
        &self,
        py: Python<'_>,
        op: &str,
        name: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<Vec<String>> {
        let names = string_vector(name, "name")?;
        py.detach(|| {
            let state = self.state();
            let Some(attachment) = state.image.as_ref() else {
                return Ok(Vec::new());
            };
            let operation = MaskOperation::parse(op)?;
            let mut image = attachment.lock();
            let writes = matches!(
                operation,
                MaskOperation::Set
                    | MaskOperation::Rename
                    | MaskOperation::Delete
                    | MaskOperation::Copy
            );
            let kind = if writes {
                casa_tables::LockType::Write
            } else {
                casa_tables::LockType::Read
            };
            if image.image_type_name() == "ImageConcat" {
                attachment.check_bound_name(&image)?;
            } else {
                attachment.require_lock(&mut image, kind)?;
            }
            let result = apply_mask_operation(&mut image, operation, &names)?;
            if writes {
                if state.history
                    && (!matches!(operation, MaskOperation::Copy)
                        || op.to_ascii_uppercase().starts_with("COPY"))
                {
                    let names = names
                        .iter()
                        .map(|n| format!("{n:?}"))
                        .collect::<Vec<_>>()
                        .join(",");
                    image.record_history(
                        "maskhandler",
                        format!("ia.maskhandler(op={op:?}, name=[{names}])"),
                    )?;
                }
                image.save().map_err(raised)?;
            }
            Ok(result)
        })
    }
}

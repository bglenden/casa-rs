// SPDX-License-Identifier: LGPL-3.0-or-later

use std::path::{Path, PathBuf};

use casa_coordinates::{CoordinateSystem, CoordinateType, StokesCoordinate, StokesType};
use casa_images::analysis::{
    ImageToolExpressionInputs, ImageToolPixelExpression, ensure_outfile_does_not_alias_input,
    materialize_image_tool_expression,
};
use casa_images::{
    ImageExpression, ImageInfo, ImagePixelType, TempImage, image_expr::ImageExprValue,
};
use casa_types::{Complex32, RecordValue};
use pyo3::prelude::*;

use super::PyImageTool;
use super::attachment::AttachedImage;
use super::beams::{image_info, image_info_record};
use super::create::publish_image;
use super::handle::{ImageHandle, Storage, raised, with_pixels, with_storage};

fn no_coordinates() -> PyErr {
    raised("Exception: Images in expression have no coordinates.")
}

fn storage_from_expression<T: ImageExprValue>(
    expression: ImageExpression<T>,
    path: Option<&Path>,
) -> PyResult<Storage<T>> {
    if let Some(path) = path {
        let image = materialize_image_tool_expression(&expression, path).map_err(raised)?;
        return Ok(Storage::Paged(Box::new(image)));
    }
    let record = expression.coordinate_record();
    let image = TempImage::from_expression(expression).map_err(raised)?;
    Ok(Storage::Temp {
        image: Box::new(image),
        record,
        fits: None,
    })
}

struct Metadata {
    coordinates: Option<RecordValue>,
    image_info: RecordValue,
    misc_info: RecordValue,
    units: String,
}

impl Metadata {
    fn read(name: &str) -> PyResult<Self> {
        let path = Path::new(name);
        if !path.exists() {
            return Err(raised(format!(
                "Exception: Cannot access {name} so cannot copy its metadata to output image."
            )));
        }
        if casa_images::expr_file::is_image_expr(path) {
            let source = super::saved_expression::open(path)?;
            return Self::from_image(&source.lock());
        }
        Self::from_image(&ImageHandle::open(path).map_err(raised)?)
    }

    fn from_image(image: &ImageHandle) -> PyResult<Self> {
        if !matches!(
            image.kind(),
            ImagePixelType::Float32 | ImagePixelType::Complex32
        ) {
            return Err(raised("Exception: Unsupported image pixel data type."));
        }
        let info = match image_info_record(image) {
            Some(record) => record.clone(),
            None => image_info(image).map_err(raised)?.to_record(),
        };
        let (misc_info, units) = with_pixels!(image, storage => with_storage!(storage, image =>
            (image.misc_info(), image.units().to_owned())
        ));
        Ok(Self {
            coordinates: Some(image.coordinate_record()),
            image_info: info,
            misc_info,
            units,
        })
    }

    fn new_output() -> Self {
        Self {
            coordinates: None,
            image_info: ImageInfo::default().to_record(),
            misc_info: RecordValue::default(),
            units: String::new(),
        }
    }

    fn apply(self, image: &mut ImageHandle) -> PyResult<()> {
        if let Some(coordinates) = self.coordinates {
            let system = CoordinateSystem::from_record(&coordinates).map_err(raised)?;
            if system.n_pixel_axes() == image.shape().len() {
                image.set_coordinate_record(coordinates)?;
            }
        }
        with_pixels!(image, storage => with_storage!(storage, image => {
            image.set_units(self.units).map_err(raised)?;
            image.set_misc_info(self.misc_info).map_err(raised)?;
            image.set_image_info_record(self.image_info).map_err(raised)
        }))
    }
}

fn apply_expression_metadata(image: &mut ImageHandle, expression: &str) -> PyResult<()> {
    let units = if expression.contains("spectralindex") {
        ""
    } else if expression.contains("pa") {
        let mut system =
            CoordinateSystem::from_record(&image.coordinate_record()).map_err(raised)?;
        let index = system.find_coordinate(CoordinateType::Stokes).ok_or_else(|| raised(
            "Failed AlwaysAssert which < nCoordinates() && newCoordinate.nPixelAxes() == coordinates_p[which]->nPixelAxes() && newCoordinate.nWorldAxes() == coordinates_p[which]->nWorldAxes()"
        ))?;
        system.replace_coordinate(index, StokesCoordinate::new(vec![StokesType::Pangle]));
        image.set_coordinate_record(system.to_record())?;
        "deg"
    } else {
        return Ok(());
    };
    with_pixels!(image, storage => with_storage!(storage, image => image.set_units(units)))
        .map_err(raised)
}

fn same_file_path(first: &Path, second: &Path) -> bool {
    first == second
        || path_identity(first)
            .zip(path_identity(second))
            .is_some_and(|(first, second)| first == second)
}

fn path_identity(path: &Path) -> Option<PathBuf> {
    path.canonicalize().ok().or_else(|| {
        let parent = path.parent()?;
        let parent = if parent.as_os_str().is_empty() {
            Path::new(".")
        } else {
            parent
        };
        Some(parent.canonicalize().ok()?.join(path.file_name()?))
    })
}

pub(super) fn check_geometry(
    expression: &ImageToolPixelExpression,
    has_sources: bool,
) -> PyResult<()> {
    let (shape, has_coordinates) = match expression {
        ImageToolPixelExpression::Real(expression) => (
            expression.shape(),
            expression.coordinates().n_pixel_axes() > 0,
        ),
        ImageToolPixelExpression::Complex(expression) => (
            expression.shape(),
            expression.coordinates().n_pixel_axes() > 0,
        ),
        ImageToolPixelExpression::Boolean(shape) => {
            return Err(if shape.is_empty() || !has_sources {
                no_coordinates()
            } else {
                raised(
                    "LatticeExpr::constructor - Bool expression cannot be converted to a numeric type",
                )
            });
        }
    };
    if shape.is_empty() || !has_coordinates {
        return Err(no_coordinates());
    }
    Ok(())
}

#[pymethods]
impl PyImageTool {
    #[pyo3(signature = (outfile="", pixels="", overwrite=false, imagemd="", prec="float"), text_signature = "($self, outfile='', pixels='', overwrite=False, imagemd='', prec='float')")]
    fn imagecalc(
        &self,
        py: Python<'_>,
        outfile: &str,
        pixels: &str,
        overwrite: bool,
        imagemd: &str,
        prec: &str,
    ) -> PyResult<Py<PyImageTool>> {
        let attached = py.detach(|| {
            if pixels.is_empty() {
                return Err(raised("Exception: You must provide an expression using the pixels parameter."));
            }
            let as_float = match prec.as_bytes().first().map(u8::to_ascii_lowercase) {
                Some(b'f') => true,
                Some(b'd') => false,
                _ => return Err(raised("Exception: Unsupported value for type, it must be 'float' or 'double'.")),
            };
            let inputs = ImageToolExpressionInputs::resolve(pixels).map_err(raised)?;
            let has_sources = inputs.paths().next().is_some();
            let uses_bound_sources = super::saved_expression::has_manifest_operands(&inputs);
            let (expression, retained, paths) = if uses_bound_sources {
                super::saved_expression::compile_inline(&inputs, outfile.is_empty())?
            } else {
                let retained = AttachedImage::open_expression_inputs(inputs.paths())?;
                let expression = if outfile.is_empty() { inputs.compile_lazy() } else { inputs.compile() }
                    .map_err(raised)?;
                (expression, retained, inputs.paths().map(Path::to_path_buf).collect())
            };
            if !outfile.is_empty() && !overwrite && Path::new(outfile).symlink_metadata().is_ok() {
                return Err(raised(format!("Exception: File {outfile} exists, and the user does not want to remove it..")));
            }
            check_geometry(&expression, has_sources)?;
            if !outfile.is_empty() {
                let output = Path::new(outfile);
                let overlap_error = || raised(
                    "Exception: Caught TableError. This often means the table you are trying to overwrite has been opened by another method and so cannot be overwritten at this time. Try closing it and rerunning."
                );
                if uses_bound_sources {
                    super::saved_expression::check_output_dependencies(output, &paths)
                        .map_err(|_| overlap_error())?;
                } else {
                    for input in &paths {
                        ensure_outfile_does_not_alias_input(output, input).map_err(|_| overlap_error())?;
                    }
                }
            }
            let metadata = if imagemd.is_empty() {
                None
            } else if !outfile.is_empty() && same_file_path(Path::new(outfile), Path::new(imagemd)) {
                if !as_float {
                    return Err(raised("Exception: Unsupported image pixel data type."));
                }
                Some(Metadata::new_output())
            } else {
                Some(Metadata::read(imagemd)?)
            };
            let history = self.state().history;
            let mut result = publish_image(outfile, overwrite, |path| {
                let mut output = match expression {
                    ImageToolPixelExpression::Real(expression) if as_float => ImageHandle::Float(
                        storage_from_expression(expression.convert_pixels(|value| value as f32).map_err(raised)?, path)?,
                    ),
                    ImageToolPixelExpression::Real(expression) => ImageHandle::Double(storage_from_expression(expression, path)?),
                    ImageToolPixelExpression::Complex(expression) if as_float => ImageHandle::Complex(
                        storage_from_expression(expression.convert_pixels(|value| Complex32::new(value.re as f32, value.im as f32)).map_err(raised)?, path)?,
                    ),
                    ImageToolPixelExpression::Complex(expression) => ImageHandle::DComplex(storage_from_expression(expression, path)?),
                    ImageToolPixelExpression::Boolean(_) => unreachable!("numeric expression checked"),
                };
                if let Some(metadata) = metadata {
                    metadata.apply(&mut output)?;
                }
                apply_expression_metadata(&mut output, pixels)?;
                if history {
                    let [outfile, pixels, imagemd] = [outfile, pixels, imagemd].map(|value| if value.len() > 300 { "..." } else { value });
                    output.record_history("imagecalc", format!(
                        "ia.imagecalc(outfile=\"{outfile}\", pixels=\"{pixels}\", overwrite={overwrite}, imagemd=\"{imagemd}\")"
                    ))?;
                }
                output.save().map_err(raised)?;
                Ok(output)
            })?;
            if outfile.is_empty() {
                result.retain_expression_inputs(retained, paths);
            }
            Ok(result)
        })?;
        Py::new(py, Self::attached(attached))
    }
}

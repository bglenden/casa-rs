// SPDX-License-Identifier: LGPL-3.0-or-later

use std::path::Path;

use casa_coordinates::{CoordinateSystem, CoordinateType};
use casa_core::{ArrayD, ArrayValue, RecordValue, Value};
use casa_lattices::TiledShape;
use ndarray::{Axis, IxDyn};

use casa_images::{Image, ImageError, MutableImageInterface};

use super::super::collapse::collapsed_coordinate_record;
use super::models::{LineKind, ProfileModels};
use super::selection::Lane;
use super::sigma::Sigma;
use super::{FitLane, ImageProfileFit, ProfileOutputNames, invalid};

const FIELDS: [&str; 8] = [
    "amp",
    "ampErr",
    "center",
    "centerErr",
    "fwhm",
    "fwhmErr",
    "integral",
    "integralErr",
];

pub(super) struct Product {
    pub(super) name: String,
    kind: Kind,
}

#[derive(Clone, Copy)]
enum Kind {
    Model,
    Residual,
    Line {
        field: usize,
        group: Group,
        index: usize,
    },
    Coefficient {
        error: bool,
        index: usize,
    },
    Sigma,
}

#[derive(Clone, Copy)]
enum Group {
    Gaussian,
    Lorentzian,
    Multiplet(usize),
}

impl Group {
    fn key(self) -> String {
        match self {
            Self::Gaussian => "gs".into(),
            Self::Lorentzian => "ls".into(),
            Self::Multiplet(index) => format!("gm{index}"),
        }
    }
}

pub(super) fn destinations(
    names: &ProfileOutputNames<'_>,
    models: &ProfileModels,
    automatic: usize,
    has_sigma: bool,
    multifit: bool,
) -> Vec<Product> {
    let mut outputs = Vec::new();
    for (name, kind) in names.spectra.into_iter().zip([Kind::Model, Kind::Residual]) {
        add(&mut outputs, name, kind);
    }
    if multifit {
        add_solution_fields(&mut outputs, names, models, automatic);
    }
    if has_sigma {
        add(&mut outputs, names.sigma, Kind::Sigma);
    }
    outputs
}

fn add_solution_fields(
    outputs: &mut Vec<Product>,
    names: &ProfileOutputNames<'_>,
    models: &ProfileModels,
    automatic: usize,
) {
    if let Some(spectral) = &models.spectral_polynomial {
        for (field, name) in names
            .coefficients
            .into_iter()
            .enumerate()
            .filter(|(_, name)| !name.is_empty())
        {
            for index in 0..spectral.values.len() {
                add(
                    outputs,
                    &format!("{name}_{index}"),
                    Kind::Coefficient {
                        error: field == 1,
                        index,
                    },
                );
            }
        }
    } else {
        let gaussian = models
            .singlets
            .iter()
            .filter(|line| matches!(line.kind, LineKind::Gaussian))
            .count();
        let mut groups = vec![
            (Group::Gaussian, String::new(), gaussian + automatic),
            (
                Group::Lorentzian,
                "_ls".into(),
                models.singlets.len() - gaussian,
            ),
        ];
        for (index, multiplet) in models.multiplets.iter().enumerate() {
            let suffix = if models.multiplets.len() == 1 {
                "_gm".into()
            } else {
                format!("_gm{index}")
            };
            groups.push((Group::Multiplet(index), suffix, multiplet.components.len()));
        }
        for (field, name) in names
            .lines
            .into_iter()
            .enumerate()
            .filter(|(_, name)| !name.is_empty())
        {
            for (group, suffix, len) in &groups {
                for index in 0..*len {
                    add(
                        outputs,
                        &format!("{name}{suffix}_{index}"),
                        Kind::Line {
                            field,
                            group: *group,
                            index,
                        },
                    );
                }
            }
        }
    }
}

fn add(outputs: &mut Vec<Product>, name: &str, kind: Kind) {
    if !name.is_empty() {
        outputs.push(Product {
            name: name.to_owned(),
            kind,
        });
    }
}

pub(super) struct ProfileProducts {
    model: Option<SpectrumProduct>,
    residual: Option<SpectrumProduct>,
    coordinates: RecordValue,
    shape: Vec<usize>,
}

impl ProfileProducts {
    pub(super) fn new(
        fit: &ImageProfileFit<'_>,
        staged: &[Option<&Path>],
    ) -> Result<Self, ImageError> {
        if fit.products.len() != staged.len() {
            return Err(invalid(
                "Number of staged profile products differs from the request",
            ));
        }
        let plan = &fit.selection.plan;
        let mut coordinates = plan
            .coordinate_record
            .clone()
            .unwrap_or_else(|| plan.coordinates.to_record());
        let mut shape = plan.shape.clone();
        if !fit.options.multifit {
            let reference = plan.coordinates.reference_pixel();
            for (axis, length) in shape.iter_mut().enumerate() {
                if axis != fit.selection.axis && *length > 1 {
                    let offset = (*length - 1) as f64 * 0.5;
                    CoordinateSystem::set_reference_pixel_in_record(
                        &mut coordinates,
                        axis,
                        reference[axis] - offset,
                    )?;
                    *length = 1;
                }
            }
        }
        let mut output = Self {
            model: None,
            residual: None,
            coordinates,
            shape,
        };
        for (product, path) in fit.products.iter().zip(staged) {
            let Some(path) = path else { continue };
            if matches!(product.kind, Kind::Model | Kind::Residual) {
                let mut image = create(output.shape.clone(), &output.coordinates, path)?;
                image.set_units(fit.source.units())?;
                image.set_misc_info(fit.source.misc_info())?;
                let mut info = plan.image_info.clone();
                if !fit.options.multifit
                    && info.beam_set.is_multi()
                    && [CoordinateType::Spectral, CoordinateType::Stokes]
                        .into_iter()
                        .filter_map(|kind| fit.source.find_axis(kind))
                        .any(|axis| axis != fit.selection.axis && plan.shape[axis] > 1)
                {
                    let beam = *info.beam_set.beam(0, 0);
                    info.beam_set.set_all(beam);
                }
                image.set_image_info(&info)?;
                let spectrum = Some(SpectrumProduct { image, mask: None });
                if matches!(product.kind, Kind::Model) {
                    output.model = spectrum;
                } else {
                    output.residual = spectrum;
                }
            }
        }
        Ok(output)
    }

    pub(super) fn write_spectrum(
        &mut self,
        position: &[usize],
        axis: usize,
        data: &Lane,
        outcome: Option<&FitLane>,
    ) -> Result<(), ImageError> {
        if self.model.is_none() && self.residual.is_none() {
            return Ok(());
        }
        let mut shape = vec![1; self.shape.len()];
        shape[axis] = data.values.len();
        let dims = IxDyn(&shape);
        if outcome.is_none() {
            let values = array(dims.clone(), data.values.clone());
            let mask = array(dims, data.valid.clone());
            for output in [&mut self.model, &mut self.residual].into_iter().flatten() {
                output.put(&values, &mask, position)?;
            }
            return Ok(());
        }
        let fit = outcome.filter(|outcome| outcome.valid);
        let solution = fit.and_then(|fit| {
            fit.solution.as_ref().map(|(curve, solution)| {
                fit.x
                    .iter()
                    .map(|&x| curve.fitted_value(&solution.values, x))
                    .collect::<Vec<_>>()
            })
        });
        let values = solution.unwrap_or_else(|| vec![f64::NAN; data.values.len()]);
        let mask = array(
            dims.clone(),
            fit.map_or_else(|| vec![false; data.values.len()], |fit| fit.mask.clone()),
        );
        if let Some(output) = &mut self.model {
            let model = array(
                dims.clone(),
                values.iter().map(|&value| value as f32).collect(),
            );
            output.put(&model, &mask, position)?;
        }
        if let Some(output) = &mut self.residual {
            let log = fit.is_some_and(|fit| fit.logarithmic);
            let residual = array(
                dims,
                data.values
                    .iter()
                    .zip(&values)
                    .map(|(&datum, &prediction)| {
                        let datum = f64::from(datum);
                        let ordinate = if log { datum.ln() } else { datum };
                        (ordinate - prediction) as f32
                    })
                    .collect(),
            );
            output.put(&residual, &mask, position)?;
        }
        Ok(())
    }

    pub(super) fn finish(
        self,
        fit: &ImageProfileFit<'_>,
        staged: &[Option<&Path>],
        record: &RecordValue,
        converged: bool,
        sigma: Option<&Sigma<'_>>,
        axis_unit: &str,
    ) -> Result<(), ImageError> {
        for mut spectrum in [self.model, self.residual].into_iter().flatten() {
            spectrum.image.save()?;
        }
        let mut shape = self.shape;
        let axis = fit.selection.axis;
        let writes_solution = fit.products.iter().zip(staged).any(|(product, path)| {
            path.is_some() && matches!(product.kind, Kind::Line { .. } | Kind::Coefficient { .. })
        });
        let solution_coordinates = if converged && fit.options.multifit && writes_solution {
            collapsed_coordinate_record(
                &fit.selection.plan.coordinates,
                &shape,
                self.coordinates,
                &[axis],
            )?
        } else {
            self.coordinates
        };
        shape[axis] = 1;
        for (product, path) in fit.products.iter().zip(staged) {
            let Some(path) = path else { continue };
            match product.kind {
                Kind::Model | Kind::Residual => {}
                Kind::Sigma => {
                    if let Some(sigma) = sigma {
                        write_sigma(fit, path, sigma)?;
                    }
                }
                Kind::Line {
                    field,
                    group,
                    index,
                } if converged => {
                    let unit = match field {
                        0 | 1 => fit.source.units().into(),
                        2..=5 => axis_unit.to_owned(),
                        _ => format!("{axis_unit}.{}", fit.source.units()),
                    };
                    let values = result_array(record, &group.key(), FIELDS[field])?;
                    write_field(
                        shape.clone(),
                        &solution_coordinates,
                        values,
                        index,
                        &unit,
                        path,
                    )?;
                }
                Kind::Coefficient { error, index } if converged => {
                    let group = if record.get("plp").is_some() {
                        "plp"
                    } else {
                        "ltp"
                    };
                    let field = if error { "error" } else { "solution" };
                    let values = result_array(record, group, field)?;
                    write_field(
                        shape.clone(),
                        &solution_coordinates,
                        values,
                        index,
                        "",
                        path,
                    )?;
                }
                _ => {}
            }
        }
        Ok(())
    }
}

struct SpectrumProduct {
    image: Image,
    mask: Option<String>,
}

impl SpectrumProduct {
    fn put(
        &mut self,
        pixels: &ArrayD<f32>,
        mask: &ArrayD<bool>,
        position: &[usize],
    ) -> Result<(), ImageError> {
        self.image.put_slice(pixels, position)?;
        if self.mask.is_none() && mask.iter().any(|&value| !value) {
            self.mask = Some(self.image.create_filled_pixel_mask(true)?);
        }
        if let Some(name) = &self.mask {
            self.image.put_mask_slice(name, mask, position)?;
        }
        Ok(())
    }
}

fn create(shape: Vec<usize>, coordinates: &RecordValue, path: &Path) -> Result<Image, ImageError> {
    let tile = TiledShape::new(shape.clone(), std::mem::size_of::<f32>())?;
    let mut image = Image::create_with_tile_shape_and_cache(
        shape,
        tile.tile_shape().to_vec(),
        CoordinateSystem::from_record(coordinates)?,
        path,
        16 * 1024 * 1024,
    )?;
    image.set_coordinate_record(coordinates.clone())?;
    Ok(image)
}

fn write_field(
    shape: Vec<usize>,
    coordinates: &RecordValue,
    values: &ArrayD<f64>,
    index: usize,
    unit: &str,
    path: &Path,
) -> Result<(), ImageError> {
    let mut image = create(shape, coordinates, path)?;
    let values = values
        .index_axis(Axis(values.ndim() - 1), index)
        .mapv(|value| value as f32);
    let mask = values.mapv(f32::is_finite);
    image.put_slice(&values, &vec![0; values.ndim()])?;
    if mask.iter().any(|&value| !value) {
        image.put_mask("mask0", &mask)?;
        image.set_default_mask("mask0")?;
    }
    image.set_units(unit)?;
    image.save()
}

fn write_sigma(
    fit: &ImageProfileFit<'_>,
    path: &Path,
    sigma: &Sigma<'_>,
) -> Result<(), ImageError> {
    let shape = fit.source.shape().to_vec();
    let rank = shape.len();
    let axis = fit.selection.axis;
    let mut output = SpectrumProduct {
        image: create(shape.clone(), &fit.original_coordinates, path)?,
        mask: None,
    };
    if let Some(image) = sigma.full_size_image() {
        output.image.set_units(image.units())?;
        output.image.set_misc_info(image.misc_info())?;
        output.image.set_image_info(&image.image_info()?)?;
        if let Some(path) = image.name() {
            output
                .image
                .append_history_entries(&casa_images::image_history_rows(path)?)?;
        } else {
            output
                .image
                .append_history_at("::ImageProfileFitter::fit", &image.history()?)?;
        }
    }
    let mut row = shape.clone();
    row[axis] = 1;
    let count = row
        .iter()
        .try_fold(1_usize, |size, &len| size.checked_mul(len))
        .ok_or_else(|| invalid("Sigma image geometry exceeds supported size"))?;
    let mut lane_shape = vec![1; rank];
    lane_shape[axis] = shape[axis];
    for mut lane in 0..count {
        let position: Vec<_> = row
            .iter()
            .map(|&len| {
                let pixel = lane % len;
                lane /= len;
                pixel
            })
            .collect();
        let values = sigma.read(&position, axis, shape[axis], 1)?;
        let dims = IxDyn(&lane_shape);
        output.put(
            &array(dims.clone(), values.values),
            &array(dims, values.valid),
            &position,
        )?;
    }
    output.image.save()
}

fn result_array<'a>(
    record: &'a RecordValue,
    group: &str,
    field: &str,
) -> Result<&'a ArrayD<f64>, ImageError> {
    if let Some(Value::Record(subrecord)) = record.get(group)
        && let Some(Value::Array(ArrayValue::Float64(values))) = subrecord.get(field)
    {
        return Ok(values);
    }
    Err(invalid(format!(
        "Fit result does not contain {group}.{field}"
    )))
}

fn array<T>(dim: IxDyn, values: Vec<T>) -> ArrayD<T> {
    ArrayD::from_shape_vec(dim, values).expect("profile element count agrees with its lane shape")
}

// SPDX-License-Identifier: LGPL-3.0-or-later
//! Conversions between the operator's images and the reconstruction's
//! normal-state and model representations.

use casa_imaging_model::ModelSupport;
use casa_imaging_operator::{
    MeasurementOperator, ModelImages, ModelPlane, ModelPrescale, NormalImages, PlaneRange,
    PreparedModelGrids,
};
use casa_imaging_reconstruction::{ModelGeneration, PassImages};
use ndarray::Array2;

use super::ImagingError;

/// The pass images of domain `domain` in the normal state's x-major layout.
///
/// Data images become the residual planes `[channel][term][pol]`; on an
/// initial pass the PSF moments and their `sumwt` come along.
pub(crate) fn pass_images(domain: usize, images: &NormalImages, initial: bool) -> PassImages {
    let (height, width) = images.planes[0].data[0].dim();
    let first = images.first_plane as usize;
    let mut residual = Vec::with_capacity(images.planes.len() * images.data_terms * images.pols);
    for plane in &images.planes {
        for image in &plane.data {
            push_x_major(&mut residual, image);
        }
    }
    let (psf, sum_weights) = if initial {
        let mut psf = Vec::new();
        let mut sum_weights = Vec::new();
        for (index, plane) in images.planes.iter().enumerate() {
            for image in &plane.psf {
                push_x_major(&mut psf, image);
            }
            for term in 0..images.psf_terms {
                for pol in 0..images.pols {
                    sum_weights.push(images.psf_sumwt(index, term, pol));
                }
            }
        }
        (Some(psf), sum_weights)
    } else {
        (None, Vec::new())
    };
    PassImages {
        domain,
        shape: [width, height],
        channels: first..first + images.planes.len(),
        polarizations: images.pols,
        residual,
        psf,
        sum_weights,
    }
}

fn push_x_major(out: &mut Vec<f32>, image: &Array2<f32>) {
    let (height, width) = image.dim();
    out.reserve(width * height);
    for x in 0..width {
        for y in 0..height {
            out.push(image[(y, x)]);
        }
    }
}

/// Model images of `planes` on domain `domain` of `generation`: one plane
/// per output channel of a channel-local basis, the Taylor terms of one
/// plane otherwise. Pixels outside the model's support predict nothing.
pub(crate) fn model_images(
    generation: &ModelGeneration,
    domain: usize,
    planes: PlaneRange,
    channel_local: bool,
) -> Result<ModelImages, ImagingError> {
    let shape = generation.shape();
    let [width, height] = shape.domains()[domain].pixels();
    let pols = shape.polarizations();
    let terms = if channel_local {
        1
    } else {
        shape.coefficients()
    };
    let mut model_planes = Vec::with_capacity(planes.len());
    for plane in planes.start..planes.end {
        let mut images = Vec::with_capacity(terms * pols);
        for term in 0..terms {
            let coefficient = if channel_local { plane as usize } else { term };
            for pol in 0..pols {
                let samples = generation.read_plane(domain, coefficient, pol)?;
                images.push(Array2::from_shape_fn((height, width), |(y, x)| {
                    let sample = samples[y * width + x];
                    match sample.support() {
                        ModelSupport::Valid => sample.value().value() as f32,
                        ModelSupport::Invalid => 0.0,
                    }
                }));
            }
        }
        model_planes.push(ModelPlane { images });
    }
    Ok(ModelImages {
        first_plane: planes.start,
        planes: model_planes,
    })
}

/// Prepared model grids of `planes` of domain `domain`.
pub(crate) fn prepare_model(
    operator: &MeasurementOperator,
    generation: &ModelGeneration,
    domain: usize,
    planes: PlaneRange,
) -> Result<PreparedModelGrids, ImagingError> {
    let channel_local = matches!(
        operator.basis(),
        casa_imaging_operator::Basis::ChannelLocal { .. }
    );
    let images = model_images(generation, domain, planes, channel_local)?;
    Ok(operator.prepare_model(&images, ModelPrescale::Unit)?)
}

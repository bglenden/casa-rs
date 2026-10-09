// SPDX-License-Identifier: LGPL-3.0-or-later
//! Conversions between the operator's images and the reconstruction's
//! normal-state and model representations.

use casa_imaging_model::ModelSupport;
use casa_imaging_operator::{
    Basis, MeasurementOperator, ModelImages, ModelPlane, ModelPrescale, NormalImages, PlaneRange,
    PreparedModelGrids,
};
use casa_imaging_reconstruction::{ModelGeneration, PassImages};
use ndarray::Array2;

use super::ImagingError;

/// The pass images of domain `domain` in the normal state's x-major layout.
///
/// Data images become the residual planes `[channel][term][pol]`; on an
/// initial pass the PSF moments, their `sumwt` and the `sumwt` CASA
/// publishes for `basis` come along.
pub(crate) fn pass_images(
    domain: usize,
    images: &NormalImages,
    initial: bool,
    basis: Basis,
) -> PassImages {
    let (height, width) = images.planes[0].data[0].dim();
    let first = images.first_plane as usize;
    let mut residual = Vec::with_capacity(images.planes.len() * images.data_terms * images.pols);
    for plane in &images.planes {
        for image in &plane.data {
            push_x_major(&mut residual, image);
        }
    }
    let (psf, sum_weights, published_sum_weights, weight) = if initial {
        let mut psf = Vec::new();
        let mut sum_weights = Vec::new();
        let mut published_sum_weights = Vec::new();
        let mut weight = Vec::new();
        for (index, plane) in images.planes.iter().enumerate() {
            for image in &plane.psf {
                push_x_major(&mut psf, image);
            }
            // CASA's `.sumwt`, which `SIImageStore` divides the residual
            // and the weight image by. `FTMachine::finalizeToSkyNew`
            // writes it once, from the PSF gridding, and keeps it through
            // the data passes, whose own sums it discards (they differ for
            // a set whose PSF kernel is not its data kernel, `AWProjectFT`'s
            // `cfwts2_p`). `MultiTermFTNew::finalizeToSkyNew` instead
            // rewrites each term's `.sumwt` from every data pass, so a
            // Taylor residual divides by the data gridding's sum while its
            // weight keeps the PSF's.
            let taylor = matches!(basis, Basis::Taylor { .. });
            for term in 0..images.psf_terms {
                for pol in 0..images.pols {
                    sum_weights.push(images.psf_sumwt(index, term, pol));
                    published_sum_weights.push(if taylor && term < images.data_terms {
                        images.data_sumwt(index, term, pol)
                    } else {
                        images.psf_sumwt(index, term, pol)
                    });
                }
            }
            for image in &plane.weight {
                push_x_major(&mut weight, image);
            }
        }
        // A kernel set with weight taps (mosaic, AW) gridded one sensitivity
        // image per polarization and plane; the standard sets gridded none.
        let weight = (!weight.is_empty()).then_some(weight);
        (Some(psf), sum_weights, published_sum_weights, weight)
    } else {
        (None, Vec::new(), Vec::new(), None)
    };
    PassImages {
        domain,
        shape: [width, height],
        channels: first..first + images.planes.len(),
        polarizations: images.pols,
        residual,
        psf,
        sum_weights,
        published_sum_weights,
        weight,
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
    let channel_local = matches!(operator.basis(), Basis::ChannelLocal { .. });
    let images = model_images(generation, domain, planes, channel_local)?;
    Ok(operator.prepare_model(&images, ModelPrescale::Unit)?)
}

#[cfg(test)]
mod tests {
    use casa_imaging_operator::NormalPlane;

    use super::*;

    /// One plane of `data_terms` data and `psf_terms` PSF images with one
    /// polarization and a weight image; `sumwt` in the plane's order.
    fn images(data_terms: usize, psf_terms: usize, sumwt: Vec<f64>) -> NormalImages {
        let image = || Array2::<f32>::zeros((2, 3));
        NormalImages {
            first_plane: 0,
            pols: 1,
            data_terms,
            psf_terms,
            planes: vec![NormalPlane {
                data: (0..data_terms).map(|_| image()).collect(),
                psf: (0..psf_terms).map(|_| image()).collect(),
                weight: vec![image()],
                sumwt,
            }],
        }
    }

    #[test]
    fn the_published_sumwt_follows_the_psf_gridding_unless_the_basis_is_taylor() {
        let constant = pass_images(0, &images(1, 1, vec![3.0, 8.0, 9.0]), true, Basis::Constant);
        assert_eq!(constant.sum_weights, vec![8.0]);
        assert_eq!(constant.published_sum_weights, vec![8.0]);
        let taylor = pass_images(
            0,
            &images(2, 3, vec![3.0, 3.1, 8.0, 8.1, 8.2, 9.0]),
            true,
            Basis::Taylor {
                terms: 2,
                reference_hz: 1.0e9,
            },
        );
        assert_eq!(taylor.sum_weights, vec![8.0, 8.1, 8.2]);
        assert_eq!(taylor.published_sum_weights, vec![3.0, 3.1, 8.2]);
        let residual = pass_images(0, &images(1, 0, vec![3.0, 9.0]), false, Basis::Constant);
        assert!(residual.sum_weights.is_empty() && residual.published_sum_weights.is_empty());
    }
}

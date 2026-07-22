// SPDX-License-Identifier: LGPL-3.0-or-later
//! CASA-style image product sets produced by imaging execution.

use casa_images::{GaussianBeam, ImageBeamSet};
use ndarray::{Array4, ArrayD};
use std::sync::Arc;

use crate::{
    BeamFit, CleanMaskProductRequest, CubeImagingResult, ImagingError, ImagingResult, MtmfsResult,
    RestoringBeamMode, clean_mask_image_product, single_plane_image_product,
};

/// CASA image-product role.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ImageProductRole {
    /// Point spread function product.
    Psf,
    /// Residual image product.
    Residual,
    /// Clean model product.
    Model,
    /// Restored image product.
    Image,
    /// Sum-of-weights product.
    Sumwt,
    /// Clean mask product.
    Mask,
    /// Mosaic or diagnostic weight product.
    Weight,
    /// Primary-beam product.
    PrimaryBeam,
    /// Primary-beam-corrected image product.
    ImagePbcor,
    /// MT-MFS spectral-index product.
    Alpha,
    /// MT-MFS spectral-index uncertainty product.
    AlphaError,
    /// Primary-beam-corrected MT-MFS spectral-index product.
    AlphaPbcor,
}

impl ImageProductRole {
    /// Stable role label written to product metadata by default.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Psf => "psf",
            Self::Residual => "residual",
            Self::Model => "model",
            Self::Image => "image",
            Self::Sumwt => "sumwt",
            Self::Mask => "mask",
            Self::Weight => "weight",
            Self::PrimaryBeam => "pb",
            Self::ImagePbcor => "image.pbcor",
            Self::Alpha => "alpha",
            Self::AlphaError => "alpha.error",
            Self::AlphaPbcor => "alpha.pbcor",
        }
    }
}

/// Borrowed-or-owned image product data.
#[derive(Debug)]
enum ImageProductData<'a> {
    /// Product data borrowed from an imaging result.
    Borrowed(&'a Array4<f32>),
    /// Product data materialized while assembling side products.
    Owned(Array4<f32>),
}

impl<'a> ImageProductData<'a> {
    /// Borrow the product pixels.
    pub fn as_array(&self) -> &Array4<f32> {
        match self {
            Self::Borrowed(data) => data,
            Self::Owned(data) => data,
        }
    }
}

/// Shared default mask for one product.
#[derive(Debug)]
enum ImageProductMask {
    /// Mask materialized while assembling side products.
    Shared(Arc<ArrayD<bool>>),
}

impl ImageProductMask {
    /// Store a materialized mask already wrapped for sharing.
    fn shared(mask: Arc<ArrayD<bool>>) -> Self {
        Self::Shared(mask)
    }

    /// Borrow the mask array.
    pub fn as_array(&self) -> &ArrayD<bool> {
        match self {
            Self::Shared(mask) => mask,
        }
    }
}

/// Per-product metadata needed by CASA-compatible image writers.
#[derive(Debug)]
pub struct ImageProductMetadata {
    role: ImageProductRole,
    role_label: String,
    units: String,
    beam_set: ImageBeamSet,
    mask: Option<ImageProductMask>,
}

impl ImageProductMetadata {
    /// Build metadata for one product role.
    pub fn new(role: ImageProductRole, units: impl Into<String>, beam_set: ImageBeamSet) -> Self {
        Self {
            role,
            role_label: role.as_str().to_string(),
            units: units.into(),
            beam_set,
            mask: None,
        }
    }

    /// Override the metadata role label while preserving the typed role.
    pub fn with_role_label(mut self, role_label: impl Into<String>) -> Self {
        self.role_label = role_label.into();
        self
    }

    /// Attach a default mask to this product.
    /// Attach a shared default mask to this product.
    pub fn with_shared_mask(mut self, mask: Arc<ArrayD<bool>>) -> Self {
        self.mask = Some(ImageProductMask::shared(mask));
        self
    }

    /// Set or replace the default mask.
    fn set_shared_mask(&mut self, mask: Arc<ArrayD<bool>>) {
        self.mask = Some(ImageProductMask::shared(mask));
    }

    /// Product role label for persisted metadata.
    pub fn role_label(&self) -> &str {
        &self.role_label
    }

    /// Product brightness units.
    pub fn units(&self) -> &str {
        &self.units
    }

    /// Restoring or PSF beam metadata.
    pub fn beam_set(&self) -> &ImageBeamSet {
        &self.beam_set
    }

    /// Optional default mask for the product.
    pub fn mask_array(&self) -> Option<&ArrayD<bool>> {
        self.mask.as_ref().map(ImageProductMask::as_array)
    }
}

/// One named CASA-style image product.
#[derive(Debug)]
pub struct ImageProduct<'a> {
    suffix: String,
    data: ImageProductData<'a>,
    metadata: ImageProductMetadata,
}

impl<'a> ImageProduct<'a> {
    /// Build a product that borrows its pixel data.
    fn borrowed(
        suffix: impl Into<String>,
        data: &'a Array4<f32>,
        metadata: ImageProductMetadata,
    ) -> Self {
        Self {
            suffix: suffix.into(),
            data: ImageProductData::Borrowed(data),
            metadata,
        }
    }

    /// Build a product that owns its pixel data.
    fn owned(suffix: impl Into<String>, data: Array4<f32>, metadata: ImageProductMetadata) -> Self {
        Self {
            suffix: suffix.into(),
            data: ImageProductData::Owned(data),
            metadata,
        }
    }

    /// File suffix, including the leading dot.
    pub fn suffix(&self) -> &str {
        &self.suffix
    }

    /// Product data.
    pub fn data(&self) -> &Array4<f32> {
        self.data.as_array()
    }

    /// Product metadata.
    pub fn metadata(&self) -> &ImageProductMetadata {
        &self.metadata
    }

    /// Product metadata.
    fn metadata_mut(&mut self) -> &mut ImageProductMetadata {
        &mut self.metadata
    }
}

/// Shared metadata for all products in one image set.
#[derive(Debug, Clone)]
pub struct ImageProductSetMetadata {
    plane_stokes: String,
    channel_frequencies_hz: Vec<f64>,
    reffreq_hz: f64,
}

impl ImageProductSetMetadata {
    /// Build product-set metadata.
    pub fn new(plane_stokes: impl Into<String>, channel_frequencies_hz: Vec<f64>) -> Self {
        let reffreq_hz = if channel_frequencies_hz.is_empty() {
            0.0
        } else {
            0.5 * (channel_frequencies_hz[0]
                + channel_frequencies_hz[channel_frequencies_hz.len() - 1])
        };
        Self {
            plane_stokes: plane_stokes.into(),
            channel_frequencies_hz,
            reffreq_hz,
        }
    }

    /// Stokes plane label.
    pub fn plane_stokes(&self) -> &str {
        &self.plane_stokes
    }

    /// Channel frequencies represented by the product set.
    pub fn channel_frequencies_hz(&self) -> &[f64] {
        &self.channel_frequencies_hz
    }

    /// Reference frequency used for compatibility metadata.
    pub fn reffreq_hz(&self) -> f64 {
        self.reffreq_hz
    }
}

/// CASA-style products from one imaging run.
#[derive(Debug)]
pub struct ImageProductSet<'a> {
    metadata: ImageProductSetMetadata,
    products: Vec<ImageProduct<'a>>,
}

impl<'a> ImageProductSet<'a> {
    /// Build an empty product set with shared metadata.
    fn new(metadata: ImageProductSetMetadata) -> Self {
        Self {
            metadata,
            products: Vec::new(),
        }
    }

    /// Shared product-set metadata.
    pub fn metadata(&self) -> &ImageProductSetMetadata {
        &self.metadata
    }

    /// Ordered products to write.
    pub fn products(&self) -> &[ImageProduct<'a>] {
        &self.products
    }

    /// Add a borrowed product.
    fn push_borrowed(
        &mut self,
        suffix: impl Into<String>,
        data: &'a Array4<f32>,
        metadata: ImageProductMetadata,
    ) {
        self.products
            .push(ImageProduct::borrowed(suffix, data, metadata));
    }

    /// Add an owned product.
    pub fn push_owned(
        &mut self,
        suffix: impl Into<String>,
        data: Array4<f32>,
        metadata: ImageProductMetadata,
    ) {
        self.products
            .push(ImageProduct::owned(suffix, data, metadata));
    }

    /// Find the first product with a role.
    pub fn first_by_role(&self, role: ImageProductRole) -> Option<&ImageProduct<'a>> {
        self.products
            .iter()
            .find(|product| product.metadata.role == role)
    }

    /// Attach a shared mask to the first product with a role.
    pub fn set_first_role_shared_mask(
        &mut self,
        role: ImageProductRole,
        mask: Arc<ArrayD<bool>>,
    ) -> bool {
        let Some(product) = self
            .products
            .iter_mut()
            .find(|product| product.metadata.role == role)
        else {
            return false;
        };
        product.metadata_mut().set_shared_mask(mask);
        true
    }

    /// Add a CASA-style clean-mask product.
    pub fn push_clean_mask_product(&mut self, request: CleanMaskProductRequest<'_>) {
        self.push_owned(
            ".mask",
            clean_mask_image_product(request),
            ImageProductMetadata::new(ImageProductRole::Mask, "", ImageBeamSet::default()),
        );
    }
}

/// Build product metadata for a single fitted beam.
pub fn image_beam_set_from_beam(beam: Option<BeamFit>) -> ImageBeamSet {
    beam.map(beam_fit_to_gaussian)
        .map(ImageBeamSet::new)
        .unwrap_or_default()
}

/// Convert a CASA-compatible restoring-beam fit to image metadata.
pub fn beam_fit_to_gaussian(beam: BeamFit) -> GaussianBeam {
    GaussianBeam::new(
        beam.major_fwhm_rad,
        beam.minor_fwhm_rad,
        beam.position_angle_rad,
    )
}

/// Build per-plane or common image-beam metadata from channel beams.
pub fn image_beam_set_from_channel_beams(
    beams: &[Option<BeamFit>],
    mode: RestoringBeamMode,
) -> Result<ImageBeamSet, ImagingError> {
    let Some(first) = beams.iter().flatten().next().copied() else {
        return Ok(ImageBeamSet::default());
    };
    if mode == RestoringBeamMode::Common {
        let mut beam_set =
            ImageBeamSet::with_shape(beams.len().max(1), 1, beam_fit_to_gaussian(first));
        for (channel, beam) in beams.iter().enumerate() {
            if let Some(beam) = beam {
                beam_set
                    .set_beam(Some(channel), Some(0), beam_fit_to_gaussian(*beam))
                    .map_err(|error| {
                        ImagingError::InvalidRequest(format!(
                            "set beam for channel {channel}: {error}"
                        ))
                    })?;
            }
        }
        let common = beam_set.common_beam().map_err(|error| {
            ImagingError::InvalidRequest(format!("determine common restoring beam: {error}"))
        })?;
        return Ok(ImageBeamSet::new(common));
    }
    let mut beam_set = ImageBeamSet::with_shape(beams.len(), 1, beam_fit_to_gaussian(first));
    for (channel, beam) in beams.iter().enumerate() {
        if let Some(beam) = beam {
            beam_set
                .set_beam(Some(channel), Some(0), beam_fit_to_gaussian(*beam))
                .map_err(|error| {
                    ImagingError::InvalidRequest(format!("set beam for channel {channel}: {error}"))
                })?;
        }
    }
    if beam_set.single_beam().is_none()
        && beam_set.shape().0 > 0
        && beam_set.shape().1 > 0
        && beam_set.equivalent(&ImageBeamSet::new(*beam_set.beam(0, 0)))
    {
        Ok(ImageBeamSet::new(*beam_set.beam(0, 0)))
    } else {
        Ok(beam_set)
    }
}

/// Build the standard MFS image product set.
pub fn mfs_image_product_set<'a>(result: &'a ImagingResult) -> ImageProductSet<'a> {
    let mut set = ImageProductSet::new(ImageProductSetMetadata::new(
        result.compatibility.plane_stokes.as_str(),
        result.compatibility.channel_frequencies_hz.clone(),
    ));
    let psf_beam = image_beam_set_from_beam(result.beam);
    let image_beam = image_beam_set_from_beam(result.beam);
    set.push_borrowed(
        ".psf",
        &result.psf,
        ImageProductMetadata::new(
            ImageProductRole::Psf,
            result.compatibility.psf_units.as_str(),
            psf_beam.clone(),
        ),
    );
    set.push_borrowed(
        ".residual",
        &result.residual,
        ImageProductMetadata::new(
            ImageProductRole::Residual,
            result.compatibility.residual_units.as_str(),
            image_beam.clone(),
        ),
    );
    set.push_borrowed(
        ".model",
        &result.model,
        ImageProductMetadata::new(
            ImageProductRole::Model,
            result.compatibility.model_units.as_str(),
            ImageBeamSet::default(),
        ),
    );
    set.push_borrowed(
        ".image",
        &result.image,
        ImageProductMetadata::new(
            ImageProductRole::Image,
            result.compatibility.image_units.as_str(),
            image_beam,
        ),
    );
    set.push_borrowed(
        ".sumwt",
        &result.sumwt,
        ImageProductMetadata::new(ImageProductRole::Sumwt, "", ImageBeamSet::default()),
    );
    if let Some(weight_image) = result.diagnostics.mosaic_weight_image.as_ref() {
        set.push_owned(
            ".weight",
            single_plane_image_product(weight_image),
            ImageProductMetadata::new(ImageProductRole::Weight, "", ImageBeamSet::default()),
        );
    }
    set
}

/// Build a cube image product set.
pub fn cube_image_product_set(
    result: &CubeImagingResult,
    restoring_beam_mode: RestoringBeamMode,
    mosaic_weight: Option<Array4<f32>>,
) -> Result<ImageProductSet<'_>, ImagingError> {
    let mut set = ImageProductSet::new(ImageProductSetMetadata::new(
        result.compatibility.plane_stokes.as_str(),
        result.compatibility.channel_frequencies_hz.clone(),
    ));
    let psf_beams = image_beam_set_from_channel_beams(&result.beams, RestoringBeamMode::PerPlane)?;
    let residual_beams =
        image_beam_set_from_channel_beams(&result.beams, RestoringBeamMode::PerPlane)?;
    let image_beams =
        image_beam_set_from_channel_beams(&result.restored_beams, restoring_beam_mode)?;
    set.push_borrowed(
        ".psf",
        &result.psf,
        ImageProductMetadata::new(
            ImageProductRole::Psf,
            result.compatibility.psf_units.as_str(),
            psf_beams,
        ),
    );
    set.push_borrowed(
        ".residual",
        &result.residual,
        ImageProductMetadata::new(
            ImageProductRole::Residual,
            result.compatibility.residual_units.as_str(),
            residual_beams,
        ),
    );
    set.push_borrowed(
        ".model",
        &result.model,
        ImageProductMetadata::new(
            ImageProductRole::Model,
            result.compatibility.model_units.as_str(),
            ImageBeamSet::default(),
        ),
    );
    set.push_borrowed(
        ".image",
        &result.image,
        ImageProductMetadata::new(
            ImageProductRole::Image,
            result.compatibility.image_units.as_str(),
            image_beams,
        ),
    );
    set.push_borrowed(
        ".sumwt",
        &result.sumwt,
        ImageProductMetadata::new(ImageProductRole::Sumwt, "", ImageBeamSet::default()),
    );
    if let Some(weight) = mosaic_weight {
        set.push_owned(
            ".weight",
            weight,
            ImageProductMetadata::new(ImageProductRole::Weight, "", ImageBeamSet::default()),
        );
    }
    Ok(set)
}

/// Build an MT-MFS image product set.
pub fn mtmfs_image_product_set<'a>(result: &'a MtmfsResult) -> ImageProductSet<'a> {
    let mut set = ImageProductSet::new(ImageProductSetMetadata::new(
        result.compatibility.plane_stokes.as_str(),
        result.compatibility.channel_frequencies_hz.clone(),
    ));
    let psf_beam = image_beam_set_from_beam(result.beam);
    let image_beam = image_beam_set_from_beam(result.beam);
    let restored_mask = result
        .image_terms
        .first()
        .map(|image| Arc::new(image.mapv(|_| true).into_dyn()));
    let alpha_mask = result
        .alpha_mask
        .as_ref()
        .map(|mask| Arc::new(mask.clone().into_dyn()));
    for (term_index, psf_term) in result.psf_terms.iter().enumerate() {
        set.push_borrowed(
            format!(".psf.tt{term_index}"),
            psf_term,
            ImageProductMetadata::new(
                ImageProductRole::Psf,
                result.compatibility.psf_units.as_str(),
                psf_beam.clone(),
            ),
        );
    }
    for (term_index, residual_term) in result.residual_terms.iter().enumerate() {
        let mut metadata =
            ImageProductMetadata::new(ImageProductRole::Residual, "", ImageBeamSet::default());
        if let Some(mask) = restored_mask.as_ref() {
            metadata = metadata.with_shared_mask(mask.clone());
        }
        set.push_borrowed(format!(".residual.tt{term_index}"), residual_term, metadata);
    }
    for (term_index, model_term) in result.model_terms.iter().enumerate() {
        set.push_borrowed(
            format!(".model.tt{term_index}"),
            model_term,
            ImageProductMetadata::new(
                ImageProductRole::Model,
                result.compatibility.model_units.as_str(),
                ImageBeamSet::default(),
            ),
        );
    }
    for (term_index, image_term) in result.image_terms.iter().enumerate() {
        let mut metadata = ImageProductMetadata::new(
            ImageProductRole::Image,
            result.compatibility.image_units.as_str(),
            image_beam.clone(),
        );
        if let Some(mask) = restored_mask.as_ref() {
            metadata = metadata.with_shared_mask(mask.clone());
        }
        set.push_borrowed(format!(".image.tt{term_index}"), image_term, metadata);
    }
    for (term_index, sumwt_term) in result.sumwt_terms.iter().enumerate() {
        set.push_borrowed(
            format!(".sumwt.tt{term_index}"),
            sumwt_term,
            ImageProductMetadata::new(ImageProductRole::Sumwt, "", ImageBeamSet::default()),
        );
    }
    for (term_index, weight_term) in result.weight_terms.iter().enumerate() {
        set.push_borrowed(
            format!(".weight.tt{term_index}"),
            weight_term,
            ImageProductMetadata::new(ImageProductRole::Weight, "", ImageBeamSet::default()),
        );
    }
    if let Some(alpha) = result.alpha.as_ref() {
        let mut metadata =
            ImageProductMetadata::new(ImageProductRole::Alpha, "", image_beam.clone());
        if let Some(mask) = alpha_mask.as_ref() {
            metadata = metadata.with_shared_mask(mask.clone());
        }
        set.push_borrowed(".alpha", alpha, metadata);
    }
    if let Some(alpha_error) = result.alpha_error.as_ref() {
        let mut metadata = ImageProductMetadata::new(ImageProductRole::AlphaError, "", image_beam);
        if let Some(mask) = alpha_mask {
            metadata = metadata.with_shared_mask(mask);
        }
        set.push_borrowed(".alpha.error", alpha_error, metadata);
    }
    set
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        AxisKind, BeamFit, CleanStopReason, CompatibilityMetadata, CubeImagingDiagnostics,
        ImagingDiagnostics, ImagingStageTimings,
    };
    use ndarray::{Array2, Array4};

    fn compatibility() -> CompatibilityMetadata {
        CompatibilityMetadata {
            axis_order: [
                AxisKind::RightAscension,
                AxisKind::Declination,
                AxisKind::Stokes,
                AxisKind::Frequency,
            ],
            plane_stokes: crate::PlaneStokes::I,
            reffreq_hz: 1.5,
            channel_frequencies_hz: vec![1.0, 2.0],
            psf_units: "Jy/beam".to_string(),
            residual_units: "Jy/beam".to_string(),
            model_units: "Jy/pixel".to_string(),
            image_units: "Jy/beam".to_string(),
        }
    }

    fn image(value: f32) -> Array4<f32> {
        Array4::from_elem((2, 2, 1, 2), value)
    }

    fn diagnostics() -> ImagingDiagnostics {
        ImagingDiagnostics {
            warnings: Vec::new(),
            gridded_samples: 0,
            skipped_samples: 0,
            normalization_sumwt: 0.0,
            reported_sumwt: 0.0,
            psf_peak_normalization: 1.0,
            major_cycles: 0,
            minor_iterations: 0,
            clean_stop_reason: Some(CleanStopReason::IterationLimitReached),
            minor_cycle_traces: Vec::new(),
            initial_residual_peak_jy_per_beam: 0.0,
            final_residual_peak_jy_per_beam: 0.0,
            max_abs_w_lambda: 0.0,
            fractional_bandwidth: 0.0,
            max_psf_sidelobe_level: 0.0,
            final_cycle_threshold_jy_per_beam: 0.0,
            clean_mask_pixels: 0,
            beam_fit_attempts: 0,
            beam_fit_cutoff_used: None,
            beam_fit_debug: None,
            mosaic_weight_image: Some(Array2::from_elem((2, 2), 6.0)),
            stage_timings: ImagingStageTimings::default(),
        }
    }

    #[test]
    fn image_product_set_metadata_computes_reference_frequency() {
        let metadata = ImageProductSetMetadata::new("I", vec![1.0, 3.0]);
        assert_eq!(metadata.plane_stokes(), "I");
        assert_eq!(metadata.channel_frequencies_hz(), &[1.0, 3.0]);
        assert_eq!(metadata.reffreq_hz(), 2.0);
    }

    #[test]
    fn mfs_product_set_includes_core_and_weight_products_without_copying_core_arrays() {
        let result = ImagingResult {
            psf: image(1.0),
            residual: image(2.0),
            model: image(3.0),
            image: image(4.0),
            sumwt: image(5.0),
            beam: Some(BeamFit {
                major_fwhm_rad: 1.0,
                minor_fwhm_rad: 0.5,
                position_angle_rad: 0.1,
            }),
            diagnostics: ImagingDiagnostics {
                mosaic_weight_image: Some(Array2::from_elem((2, 2), 6.0)),
                ..diagnostics()
            },
            compatibility: compatibility(),
        };
        let set = mfs_image_product_set(&result);
        let suffixes = set
            .products()
            .iter()
            .map(ImageProduct::suffix)
            .collect::<Vec<_>>();
        assert_eq!(
            suffixes,
            [".psf", ".residual", ".model", ".image", ".sumwt", ".weight"]
        );
        assert!(matches!(
            set.products()[0].data,
            ImageProductData::Borrowed(_)
        ));
        assert!(matches!(set.products()[5].data, ImageProductData::Owned(_)));
    }

    #[test]
    fn cube_product_set_keeps_common_restoring_beam_policy() {
        let beam = Some(BeamFit {
            major_fwhm_rad: 1.0,
            minor_fwhm_rad: 0.5,
            position_angle_rad: 0.1,
        });
        let result = CubeImagingResult {
            psf: image(1.0),
            residual: image(2.0),
            model: image(3.0),
            image: image(4.0),
            sumwt: image(5.0),
            clean_mask: None,
            beams: vec![beam, beam],
            restored_beams: vec![beam, beam],
            diagnostics: CubeImagingDiagnostics {
                warnings: Vec::new(),
                gridded_samples: 0,
                skipped_samples: 0,
                major_cycles: 0,
                minor_iterations: 0,
                clean_stop_reason: None,
                channel_diagnostics: vec![diagnostics()],
                stage_timings: ImagingStageTimings::default(),
            },
            compatibility: compatibility(),
        };
        let set = cube_image_product_set(&result, RestoringBeamMode::Common, None).unwrap();
        let image = set.first_by_role(ImageProductRole::Image).unwrap();
        assert!(image.metadata().beam_set().single_beam().is_some());
    }
}

// SPDX-License-Identifier: LGPL-3.0-or-later
//! The image-analysis provider contract, shared by the build and runtime.

use std::collections::BTreeMap;
use std::path::PathBuf;

use casa_contracts::{
    CasaValue, ParameterValue, ProviderCliMachineActions, ProviderCliProjection,
    ProviderProjectionMetadata, ProviderProtocolDescriptor, ProviderSurfaceKind,
    TaskOperationDescriptor, TaskProviderContract, TaskProviderSchemas, TaskSemanticContract,
    builtin_surface_bundle, merged_components,
};
use casa_images::analysis::ImheadRequest;
use casa_images::{
    ExportFitsRequest, FeatherRequest, FeatherSummary, FitsExportSummary, FitsImportSummary,
    ImageHeaderSummary, ImageMathSummary, ImageRegridSummary, ImageSubimageSummary, ImmathRequest,
    ImmomentsRequest, ImportFitsRequest, ImstatOutcome, ImstatRequest, ImsubimageRequest,
    MomentMapSummary, PbcorSummary, PvImageSummary, PvPosition,
};
use schemars::{JsonSchema, schema_for};
use serde::{Deserialize, Serialize};

/// Top-level image-analysis JSON task request.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(tag = "kind", content = "request", rename_all = "snake_case")]
pub enum ImageAnalysisTaskRequest {
    /// CASA `imhead(..., mode="summary")` style metadata summary.
    Imhead(ImheadRequest),
    /// CASA `imstat` style image statistics.
    Imstat(ImstatRequest),
    /// CASA `immoments` style moment map generation.
    Immoments(ImmomentsRequest),
    /// CASA `impv` style position-velocity extraction.
    Impv(ImpvTaskRequest),
    /// CASA `imsubimage` style pixel-box/channel extraction.
    Imsubimage(ImsubimageRequest),
    /// CASA `immath(..., mode="evalexpr")` style image arithmetic.
    Immath(ImmathRequest),
    /// CASA `impbcor` style primary-beam correction.
    Impbcor(ImpbcorTaskRequest),
    /// CASA `imregrid` style template-image regridding.
    Imregrid(ImregridTaskRequest),
    /// CASA `feather` style Fourier-domain image combination.
    Feather(FeatherRequest),
    /// CASA `exportfits` style FITS image export.
    Exportfits(ExportFitsRequest),
    /// CASA `importfits` style FITS image import.
    Importfits(ImportFitsRequest),
}

/// Top-level image-analysis JSON task result.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(tag = "kind", content = "result", rename_all = "snake_case")]
pub enum ImageAnalysisTaskResult {
    /// Result for [`ImageAnalysisTaskRequest::Imhead`].
    Imhead(ImageHeaderSummary),
    /// Result for [`ImageAnalysisTaskRequest::Imstat`].
    Imstat(ImstatOutcome),
    /// Result for [`ImageAnalysisTaskRequest::Immoments`].
    Immoments(MomentMapSummary),
    /// Result for [`ImageAnalysisTaskRequest::Impv`].
    Impv(PvImageSummary),
    /// Result for [`ImageAnalysisTaskRequest::Imsubimage`].
    Imsubimage(ImageSubimageSummary),
    /// Result for [`ImageAnalysisTaskRequest::Immath`].
    Immath(ImageMathSummary),
    /// Result for [`ImageAnalysisTaskRequest::Impbcor`].
    Impbcor(PbcorSummary),
    /// Result for [`ImageAnalysisTaskRequest::Imregrid`].
    Imregrid(ImregridTaskOutcome),
    /// Result for [`ImageAnalysisTaskRequest::Feather`].
    Feather(FeatherSummary),
    /// Result for [`ImageAnalysisTaskRequest::Exportfits`].
    Exportfits(FitsExportSummary),
    /// Result for [`ImageAnalysisTaskRequest::Importfits`].
    Importfits(FitsImportSummary),
}

/// JSON/schema wire request for `impv`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct ImpvTaskRequest {
    /// Input CASA image path.
    pub imagename: PathBuf,
    /// Output CASA image path.
    pub outfile: PathBuf,
    /// CASA mode, selected by the first letter of `coords` or `length`.
    #[serde(default = "default_impv_mode")]
    pub mode: String,
    /// First endpoint in `coords` mode.
    #[serde(default)]
    pub start: Option<PvPosition>,
    /// Second endpoint in `coords` mode.
    #[serde(default)]
    pub end: Option<PvPosition>,
    /// Midpoint in `length` mode.
    #[serde(default)]
    pub center: Option<PvPosition>,
    /// Distance between endpoints in pixels or angular units.
    #[serde(default)]
    pub length: Option<PvTaskAmount>,
    /// Sky position angle in `length` mode.
    #[serde(default)]
    pub pa: Option<PvTaskAmount>,
    /// Odd strip width in pixels, or a width in angular units.
    #[serde(default = "default_impv_width")]
    pub width: PvTaskAmount,
    /// Angular unit of the output offset coordinate.
    #[serde(default = "default_pv_unit")]
    pub unit: String,
    /// CASA text, named, or record region. Only non-direction axes may be cropped.
    #[serde(default)]
    pub region: Option<ParameterValue>,
    /// CASA channel expression, supporting tutorial forms like `4~12`.
    #[serde(default)]
    pub chans: Option<String>,
    /// Stokes planes to include.
    #[serde(default)]
    pub stokes: Option<String>,
    /// Optional LEL mask evaluated before extraction.
    #[serde(default)]
    pub mask: Option<String>,
    /// Stretch a conforming mask over singleton or missing axes.
    #[serde(default)]
    pub stretch: bool,
    /// Replace an existing output image.
    #[serde(default)]
    pub overwrite: bool,
}

/// Wire representation of a length, width, or angle supplied to `impv`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(untagged)]
pub enum PvTaskAmount {
    /// Whole pixels.
    Whole(i64),
    /// Fractional pixels for the slice length.
    Fractional(f64),
    /// An angular value supplied as text.
    Angular(String),
    /// A CASA quanta record containing one angular value.
    Quantity(BTreeMap<String, ParameterValue>),
}

/// JSON/schema wire request for `impbcor`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct ImpbcorTaskRequest {
    /// Input CASA or FITS image path.
    pub imagename: PathBuf,
    /// Primary-beam image or floating-point pixels.
    pub pbimage: PbcorTaskBeam,
    /// Output CASA image path.
    pub outfile: PathBuf,
    /// Correction mode. Supported values are `divide` and `multiply`.
    #[serde(default = "default_impbcor_mode")]
    pub mode: String,
    /// Inclusive lower bound for division or upper bound for multiplication.
    #[serde(default = "default_impbcor_cutoff")]
    pub cutoff: f64,
    /// Optional inclusive direction-plane pixel box.
    #[serde(default, rename = "box")]
    pub box_selection: Option<String>,
    /// Optional CASA region.
    #[serde(default)]
    pub region: Option<PbcorTaskRegion>,
    /// Optional channel selector.
    #[serde(default)]
    pub chans: Option<String>,
    /// Optional Stokes selector.
    #[serde(default)]
    pub stokes: Option<String>,
    /// Optional LEL mask expression.
    #[serde(default)]
    pub mask: Option<String>,
    /// Extend a conforming mask over singleton or missing axes.
    #[serde(default)]
    pub stretch: bool,
    /// Replace an existing output image.
    #[serde(default)]
    pub overwrite: bool,
}

/// Wire representation of input primary beam for CASA `impbcor`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(untagged)]
pub enum PbcorTaskBeam {
    /// Path to a real-valued image with matching direction coordinates.
    Image(PathBuf),
    /// Floating-point array with the image shape or its two direction axes.
    Pixels(Vec<ParameterValue>),
}

/// Wire representation of regions accepted by CASA `impbcor`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(untagged)]
pub enum PbcorTaskRegion {
    /// CRTF text, filename, or saved image region name.
    Text(String),
    /// Casacore region record, as returned by the region manager.
    Record(BTreeMap<String, ParameterValue>),
}

/// JSON/schema wire request for `imregrid`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct ImregridTaskRequest {
    /// Input CASA image path.
    pub imagename: PathBuf,
    /// A template image, the `get` selector, or a coordinate and shape record.
    pub template: ImregridTaskTemplate,
    /// Output CASA image path.
    #[serde(default)]
    pub output: PathBuf,
    /// Interpolation method (`nearest`, `linear`, or `cubic`).
    #[serde(default = "default_regrid_interpolation")]
    pub interpolation: String,
    /// Regrid channels at equal radio velocity using the two rest frequencies.
    #[serde(default = "default_true")]
    pub asvelocity: bool,
    /// Zero-based input pixel axes, or `[-1]` for matching nondegenerate axes.
    #[serde(default)]
    pub axes: Vec<i64>,
    /// Output lengths in input axis order, or `[-1]` for CASA's inferred shape.
    #[serde(default = "default_regrid_shape")]
    pub shape: Vec<i64>,
    /// Direction coordinate sampling interval, with zero using each pixel.
    #[serde(default = "default_regrid_decimate")]
    pub decimate: i64,
    /// Resample in pixel space instead of using the coordinate conversion.
    #[serde(default)]
    pub replicate: bool,
    /// Replace an existing output image.
    #[serde(default)]
    pub overwrite: bool,
}

/// Wire representation of template inputs accepted by `imregrid`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(untagged)]
pub enum ImregridTaskTemplate {
    /// Image path or a template selector such as `get`.
    Image(PathBuf),
    /// A `csys` and `shap` dictionary returned by `template='get'`.
    Record(BTreeMap<String, ParameterValue>),
}

/// Wire result returned by `imregrid`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(untagged)]
pub enum ImregridTaskOutcome {
    /// A completed regridding operation that wrote an image.
    Product(ImageRegridSummary),
    /// The input image's `csys` and `shap` fields as a typed CASA dictionary.
    Template(CasaValue),
}

fn default_true() -> bool {
    true
}

fn default_impv_mode() -> String {
    "coords".into()
}

fn default_impv_width() -> PvTaskAmount {
    PvTaskAmount::Whole(1)
}

fn default_pv_unit() -> String {
    "arcsec".into()
}

fn default_impbcor_mode() -> String {
    "divide".to_string()
}

fn default_impbcor_cutoff() -> f64 {
    -1.0
}

fn default_regrid_interpolation() -> String {
    "linear".to_string()
}

fn default_regrid_decimate() -> i64 {
    10
}

fn default_regrid_shape() -> Vec<i64> {
    vec![-1]
}

/// Stable protocol name advertised by the image-analysis task binaries.
pub const IMAGE_ANALYSIS_TASK_PROTOCOL_NAME: &str = "casa_image_analysis_task";
/// Stable protocol version advertised by the image-analysis task binaries.
pub const IMAGE_ANALYSIS_TASK_PROTOCOL_VERSION: u32 = 1;

pub(crate) const IMAGE_ANALYSIS_TASK_SURFACES: [&str; 11] = [
    "imhead",
    "imstat",
    "immoments",
    "impv",
    "imsubimage",
    "immath",
    "impbcor",
    "imregrid",
    "feather",
    "exportfits",
    "importfits",
];

/// Build the current image-analysis protocol descriptor.
pub fn image_analysis_protocol_descriptor() -> ProviderProtocolDescriptor {
    ProviderProtocolDescriptor::new(
        IMAGE_ANALYSIS_TASK_PROTOCOL_NAME,
        IMAGE_ANALYSIS_TASK_PROTOCOL_VERSION,
        ProviderSurfaceKind::Task,
        env!("CARGO_PKG_VERSION"),
    )
}

/// Build the current request/result schema bundle.
pub fn image_analysis_task_schema_bundle() -> TaskProviderContract {
    try_image_analysis_task_schema_bundle().unwrap_or_else(|error| panic!("{error}"))
}

pub(super) fn try_image_analysis_task_schema_bundle() -> Result<TaskProviderContract, String> {
    let request_schema = schema_for!(ImageAnalysisTaskRequest);
    let result_schema = schema_for!(ImageAnalysisTaskResult);
    Ok(TaskProviderContract {
        protocol: image_analysis_protocol_descriptor(),
        semantic: TaskSemanticContract {
            request_schema: request_schema.clone(),
            result_schema: result_schema.clone(),
            operations: vec![
                TaskOperationDescriptor {
                    name: "imhead".to_string(),
                    request_kind: "imhead".to_string(),
                    result_kind: Some("imhead".to_string()),
                },
                TaskOperationDescriptor {
                    name: "imstat".to_string(),
                    request_kind: "imstat".to_string(),
                    result_kind: Some("imstat".to_string()),
                },
                TaskOperationDescriptor {
                    name: "immoments".to_string(),
                    request_kind: "immoments".to_string(),
                    result_kind: Some("immoments".to_string()),
                },
                TaskOperationDescriptor {
                    name: "impv".to_string(),
                    request_kind: "impv".to_string(),
                    result_kind: Some("impv".to_string()),
                },
                TaskOperationDescriptor {
                    name: "imsubimage".to_string(),
                    request_kind: "imsubimage".to_string(),
                    result_kind: Some("imsubimage".to_string()),
                },
                TaskOperationDescriptor {
                    name: "immath".to_string(),
                    request_kind: "immath".to_string(),
                    result_kind: Some("immath".to_string()),
                },
                TaskOperationDescriptor {
                    name: "impbcor".to_string(),
                    request_kind: "impbcor".to_string(),
                    result_kind: Some("impbcor".to_string()),
                },
                TaskOperationDescriptor {
                    name: "imregrid".to_string(),
                    request_kind: "imregrid".to_string(),
                    result_kind: Some("imregrid".to_string()),
                },
                TaskOperationDescriptor {
                    name: "feather".to_string(),
                    request_kind: "feather".to_string(),
                    result_kind: Some("feather".to_string()),
                },
                TaskOperationDescriptor {
                    name: "exportfits".to_string(),
                    request_kind: "exportfits".to_string(),
                    result_kind: Some("exportfits".to_string()),
                },
                TaskOperationDescriptor {
                    name: "importfits".to_string(),
                    request_kind: "importfits".to_string(),
                    result_kind: Some("importfits".to_string()),
                },
            ],
            front_end: None,
        },
        components: merged_components([&request_schema, &result_schema]),
        annotations: serde_json::json!({}),
        projections: ProviderProjectionMetadata {
            cli: Some(ProviderCliProjection {
                machine_actions: ProviderCliMachineActions {
                    json_schema: Some("--json-schema".to_string()),
                    protocol_info: Some("--protocol-info".to_string()),
                    json_run: Some("--json-run <SOURCE>".to_string()),
                },
            }),
            python: Some(serde_json::json!({
                "module": "casars.tasks.image_analysis",
                "functions": [
                    "imhead",
                    "imstat",
                    "immoments",
                    "impv",
                    "imsubimage",
                    "immath",
                    "impbcor",
                    "imregrid",
                    "feather",
                    "exportfits",
                    "importfits"
                ],
            })),
        },
        parameter_surfaces: IMAGE_ANALYSIS_TASK_SURFACES
            .into_iter()
            .map(|surface| {
                builtin_surface_bundle(surface).map_err(|error| {
                    format!("built-in image-analysis parameter surface {surface:?}: {error}")
                })
            })
            .collect::<Result<_, _>>()?,
        domain_schemas: TaskProviderSchemas {
            request_schema,
            result_schema,
        },
    })
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use casa_images::analysis::ImheadRequest;

    use super::{
        IMAGE_ANALYSIS_TASK_PROTOCOL_NAME, ImageAnalysisTaskRequest,
        image_analysis_task_schema_bundle,
    };

    #[test]
    fn image_analysis_schema_and_ui_surfaces_advertise_task_contracts() {
        let bundle = image_analysis_task_schema_bundle();
        bundle.validate().expect("image-analysis provider contract");
        assert_eq!(
            bundle.protocol.protocol_name,
            IMAGE_ANALYSIS_TASK_PROTOCOL_NAME
        );
        assert_eq!(bundle.semantic.operations.len(), 11);
        assert_eq!(bundle.parameter_surfaces.len(), 11);
        assert_eq!(bundle.parameter_surfaces[0].surface.id(), "imhead");
        assert_eq!(bundle.parameter_surfaces[10].surface.id(), "importfits");
        for surface in &bundle.parameter_surfaces {
            surface.validate().unwrap_or_else(|errors| {
                panic!(
                    "invalid embedded image-analysis surface {}: {errors:?}",
                    surface.surface.id()
                )
            });
        }
        assert_eq!(
            serde_json::to_value(&bundle).unwrap()["parameter_surfaces"]
                .as_array()
                .unwrap()
                .len(),
            11
        );
        assert_eq!(
            bundle
                .projections
                .cli
                .as_ref()
                .unwrap()
                .machine_actions
                .json_run
                .as_deref(),
            Some("--json-run <SOURCE>")
        );
        assert_eq!(
            bundle.projections.python.as_ref().unwrap()["module"],
            "casars.tasks.image_analysis"
        );
    }

    #[test]
    fn image_analysis_task_request_roundtrips_through_runtime_reader() {
        let temp = tempfile::tempdir().unwrap();
        let request_path = temp.path().join("request.json");
        std::fs::write(
            &request_path,
            serde_json::to_string(&ImageAnalysisTaskRequest::Imhead(ImheadRequest {
                imagename: PathBuf::from("imported.image"),
            }))
            .unwrap(),
        )
        .unwrap();
        let payload = casa_task_runtime::read_task_request(request_path.to_str().unwrap()).unwrap();
        assert!(matches!(
            serde_json::from_str::<ImageAnalysisTaskRequest>(&payload).unwrap(),
            ImageAnalysisTaskRequest::Imhead(_)
        ));
    }
}

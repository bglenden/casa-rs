// SPDX-License-Identifier: LGPL-3.0-or-later
//! Execution and cycle choices read from the canonical imaging parameters.

use super::{GridPrecision, HogbomIterationAccounting, ProductNormalization, choice, unexpected};
use serde::{Deserialize, Deserializer, de::Error as _};
use serde_json::Value;

/// Keep the legacy imager's unpadded diagnostic out of unverified execution
/// modes, particularly virtual-model publication whose demand uses 1.2.
pub(super) fn validate_ft_padding(
    request: &super::ImagingRequest,
) -> Result<(), super::InvalidRequest> {
    use super::{BackendRequest, DataColumn, Gridder, SaveModel, SpecMode, Weighting};
    let Some(padding) = request.ft_padding else {
        return Ok(());
    };
    if padding.get() == 1.0
        && matches!(request.gridder, Gridder::Standard { facets } if facets.get() == 1)
        && request.specmode == SpecMode::Mfs
        && request.vis.len() == 1
        && request.stokes == [super::PolarizationCoordinate::StokesI]
        && request.weighting == Weighting::Natural
        && request.datacolumn == Some(DataColumn::Data)
        && request.niter == 0
        && request.calcpsf
        && !request.calcres
        && !request.restoration
        && request.savemodel == SaveModel::None
        && !request.tclean_restart
        && !request.has_start_model()
        && request.outlierfile.is_none()
        && request.backend == BackendRequest::Cpu
        && request.gridprecision == Some(GridPrecision::F64)
    {
        Ok(())
    } else {
        Err(super::InvalidRequest {
            parameters: "ft_padding",
            reason: "only padding 1.0 for a CPU/f64 natural-weighted standard Stokes-I MFS PSF-only run, without model saving or continuation, is verified",
        })
    }
}

pub(crate) fn validate_awp2_scope(
    request: &super::ImagingRequest,
) -> Result<(), super::InvalidRequest> {
    use super::{
        Deconvolver, FrequencyRef, Gridder, InvalidRequest, PolarizationCoordinate, SaveModel,
        SpecMode, Weighting,
    };
    if let Gridder::Awp2 { wprojplanes, .. } = request.gridder
        && (request.specmode != SpecMode::Mfs
            || request.mvc_output_channels.is_some()
            || request.mvc_auto_channels
            || request.deconvolver != Deconvolver::Hogbom
            || !matches!(request.weighting, Weighting::Natural | Weighting::Uniform)
            || wprojplanes.is_some_and(|count| count.get() > usize::from(u16::MAX))
            || request.stokes != [PolarizationCoordinate::StokesI]
            || request.vis.len() != 1
            || request.outframe != FrequencyRef::LSRK
            || request.outlierfile.is_some()
            || request.has_start_model()
            || request.tclean_restart
            || request.savemodel == SaveModel::Virtual
            || !request.calcpsf
            || !request.calcres
            || request.gridprecision == Some(GridPrecision::F32))
    {
        return Err(InvalidRequest {
            parameters: "gridder",
            reason: "AWP2 currently supports a fresh natural- or uniform-weighted Stokes I Högbom MFS run with double-precision gridding (#1071)",
        });
    }
    Ok(())
}

pub(super) fn major_cycle_limit<'de, D: Deserializer<'de>>(
    deserializer: D,
) -> Result<Option<usize>, D::Error> {
    let value = Value::deserialize(deserializer)?;
    match value.as_i64() {
        Some(-1) => Ok(None),
        Some(limit) => usize::try_from(limit)
            .map(Some)
            .map_err(|_| unexpected(&value, "-1 or a major-cycle count")),
        None => Err(unexpected(&value, "-1 or a major-cycle count")),
    }
}

pub(super) fn hogbom_iteration_mode<'de, D: Deserializer<'de>>(
    deserializer: D,
) -> Result<HogbomIterationAccounting, D::Error> {
    match choice(deserializer)?.as_str() {
        "strict" => Ok(HogbomIterationAccounting::Strict),
        "casa-inclusive" => Ok(HogbomIterationAccounting::CasaInclusive),
        other => Err(D::Error::custom(format!("hogbom_iteration_mode {other:?}"))),
    }
}

pub(super) fn normalization<'de, D: Deserializer<'de>>(
    deserializer: D,
) -> Result<ProductNormalization, D::Error> {
    match choice(deserializer)?.as_str() {
        "flatnoise" => Ok(ProductNormalization::FlatNoise),
        "flatsky" => Ok(ProductNormalization::FlatSky),
        "pbsquare" => Ok(ProductNormalization::PbSquare),
        other => Err(D::Error::custom(format!("normtype {other:?}"))),
    }
}

pub(super) fn grid_precision<'de, D: Deserializer<'de>>(
    deserializer: D,
) -> Result<Option<GridPrecision>, D::Error> {
    match choice(deserializer)?.as_str() {
        "auto" => Ok(None),
        "f32" => Ok(Some(GridPrecision::F32)),
        "f64" => Ok(Some(GridPrecision::F64)),
        other => Err(D::Error::custom(format!("gridprecision {other:?}"))),
    }
}

#[cfg(test)]
mod tests {
    use super::super::{BackendRequest, GridPrecision, tests::request};
    use casa_imaging_operator::PaddingFactor;
    use serde_json::json;

    #[test]
    fn legacy_padding_is_only_available_for_the_verified_quick_psf_workflow() {
        let mut virtual_model = request(json!({"savemodel": "virtual", "niter": 1}));
        virtual_model.ft_padding = Some(PaddingFactor::new(2.0).expect("padding"));
        assert_eq!(
            virtual_model
                .validate()
                .expect_err("unadmitted virtual grid")
                .parameters,
            "ft_padding"
        );
        let mut valid = request(json!({
            "niter": 0, "calcpsf": true, "calcres": false, "restoration": false,
            "weighting": "natural", "datacolumn": "data",
        }));
        valid.backend = BackendRequest::Cpu;
        valid.gridprecision = Some(GridPrecision::F64);
        valid.ft_padding = Some(PaddingFactor::new(1.0).expect("padding"));
        valid.validate().expect("verified PSF");
        for overrides in [
            json!({"savemodel": "virtual"}),
            json!({"savemodel": "modelcolumn"}),
            json!({"niter": 1}),
            json!({"calcres": true}),
            json!({"calcpsf": false}),
            json!({"restoration": true}),
            json!({"gridder": "wproject", "wprojplanes": 16}),
            json!({"specmode": "cube"}),
            json!({"startmodel": "model.image"}),
            json!({"stokes": "IQUV"}),
        ] {
            let mut values = json!({
                "niter": 0, "calcpsf": true, "calcres": false, "restoration": false,
                "weighting": "natural", "datacolumn": "data",
            });
            values
                .as_object_mut()
                .expect("object")
                .extend(overrides.as_object().expect("object").clone());
            let mut invalid = request(values);
            invalid.backend = valid.backend;
            invalid.gridprecision = valid.gridprecision;
            invalid.ft_padding = valid.ft_padding;
            assert_eq!(
                invalid
                    .validate()
                    .expect_err(&format!("unsupported padding scope: {overrides}"))
                    .parameters,
                "ft_padding",
                "{overrides}"
            );
        }
        for factor in [1.2, 2.0] {
            let mut invalid = valid.clone();
            invalid.ft_padding = Some(PaddingFactor::new(factor).expect("padding"));
            assert_eq!(
                invalid
                    .validate()
                    .expect_err("unverified factor")
                    .parameters,
                "ft_padding"
            );
        }
        let mut gpu = valid.clone();
        gpu.backend = BackendRequest::Gpu;
        assert_eq!(
            gpu.validate().expect_err("unverified backend").parameters,
            "ft_padding"
        );
        valid.gridprecision = Some(GridPrecision::F32);
        assert_eq!(
            valid
                .validate()
                .expect_err("unverified precision")
                .parameters,
            "ft_padding"
        );
    }
}

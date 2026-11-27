// SPDX-License-Identifier: LGPL-3.0-or-later
//! Translate developer sky inputs to native images/component lists.
use super::*;
use casa_components::{
    ComponentList, ComponentShape, Ellipse, EllipseErrors, Flux, ShapeKind, SkyComponent,
    SpectralKind, SpectralModel, TabularSpectrum,
};
use casa_core::measures::frequency::MFrequency;
use casa_imaging_runtime::{Demand, admit};
use std::path::Path;

pub(super) fn predict(
    request: &ObservationRequest,
    sm: &mut Simulator,
    root: &Path,
    resources: SessionResources,
) -> Result<()> {
    sm.set_vp(&crate::VoltagePatternSpec {
        do_squint: false,
        use_default_vp: request.vp_table.is_none(),
        vp_table: request
            .vp_table
            .as_ref()
            .map_or_else(String::new, |path| path.to_string_lossy().into_owned()),
        ..crate::VoltagePatternSpec::default()
    })?;
    let mut prediction = crate::PredictSpec::default();
    let model = request.model.clone().unwrap_or(SkyModel::FitsImage {
        path: request.model_image.clone(),
        model_peak_jy_per_pixel: request.model_peak_jy_per_pixel,
        direction_reference_rad: None,
        cell_size_rad: None,
    });
    match model {
        SkyModel::FitsImage {
            path,
            model_peak_jy_per_pixel,
            direction_reference_rad,
            cell_size_rad,
        } => {
            let shape = if path.is_dir() {
                casa_images::AnyPagedImage::open(&path)
                    .map_err(error)?
                    .shape()
                    .to_vec()
            } else {
                casa_images::fits_image_shape(&path).map_err(error)?
            };
            let bytes = shape
                .iter()
                .try_fold(64u64, |n, &axis| n.checked_mul(axis as u64))
                .and_then(|n| n.checked_add(32 << 20))
                .ok_or_else(|| error("model dimensions overflow"))?;
            let _memory = admit(
                resources.host(),
                resources.policy(),
                &Demand {
                    phase: "synthetic model preparation",
                    memory: bytes,
                },
            )
            .map_err(error)?;
            let output = root.join("model.image");
            crate::simutil::skymodel::modify_model(
                &path,
                &output,
                &crate::simutil::skymodel::ModelOverrides {
                    inbright: model_peak_jy_per_pixel
                        .map_or_else(String::new, |n| format!("{n}Jy/pixel")),
                    indirection: direction_reference_rad
                        .map_or_else(Vec::new, |d| vec![format!("J2000 {}rad {}rad", d[0], d[1])]),
                    incell: cell_size_rad
                        .map_or_else(Vec::new, |c| c.map(|n| format!("{n}rad")).to_vec()),
                    incenter: String::new(),
                    inwidth: String::new(),
                },
            )
            .map_err(error)?;
            prediction.images.push(output);
        }
        SkyModel::AnalyticComponents {
            path, components, ..
        } => {
            let components = if components.is_empty() {
                let path = path.ok_or_else(|| error("analytic model has no components or path"))?;
                serde_json::from_reader::<_, AnalyticComponentModel>(
                    std::fs::File::open(path).map_err(error)?,
                )
                .map_err(error)?
                .components
            } else {
                components
            };
            let mut list = ComponentList::new();
            for component in &components {
                list.push(component_model(request, component)?);
            }
            let path = root.join("model.cl");
            list.write_table(&path).map_err(error)?;
            prediction.component_lists.push(path);
        }
    }
    sm.predict(&prediction)
}

fn component_model(
    request: &ObservationRequest,
    component: &AnalyticComponent,
) -> Result<SkyComponent> {
    let (name, l, m, spectrum, ellipse) = match component {
        AnalyticComponent::Point {
            name,
            l_rad,
            m_rad,
            spectrum,
        } => (name, *l_rad, *m_rad, spectrum, None),
        AnalyticComponent::Gaussian {
            name,
            l_rad,
            m_rad,
            spectrum,
            major_fwhm_rad,
            minor_fwhm_rad,
            position_angle_rad,
        } => (
            name,
            *l_rad,
            *m_rad,
            spectrum,
            Some((
                *major_fwhm_rad,
                if *minor_fwhm_rad > 0. {
                    *minor_fwhm_rad
                } else {
                    *major_fwhm_rad
                },
                *position_angle_rad,
            )),
        ),
    };
    let [ra, dec] = request.phase_center_rad;
    let n = (1. - l * l - m * m).sqrt();
    let direction = MDirection::from_angles(
        ra + l.atan2(n * dec.cos() - m * dec.sin()),
        (m * dec.cos() + n * dec.sin()).asin(),
        DirectionRef::J2000,
    );
    let shape = if let Some((major, minor, angle)) = ellipse {
        ComponentShape::new(
            direction,
            ShapeKind::Gaussian {
                axes: Ellipse::new(major, minor, angle).map_err(error)?,
                errors: EllipseErrors::default(),
            },
        )
        .map_err(error)?
    } else {
        ComponentShape::point(direction)
    };
    let reference = request.spectral_windows[0].reference_frequency_hz();
    let mut samples = Vec::new();
    let [q, u, v] = request.polarization_setup.source_stokes_fractional_quv;
    for window in &request.spectral_windows {
        for channel in 0..window.channel_count {
            let flux = spectrum.flux_for_channel(window, channel, reference);
            samples.push((
                window.start_frequency_hz + channel as f64 * window.channel_width_hz,
                [flux, q * flux, u * flux, v * flux],
            ));
        }
    }
    samples.sort_by(|a, b| a.0.total_cmp(&b.0));
    samples.dedup_by(|a, b| a.0 == b.0);
    if samples.len() == 1 {
        let (frequency, [i, q, u, v]) = samples[0];
        return Ok(SkyComponent::new(
            Flux::stokes_jy(i, q, u, v),
            shape,
            SpectralModel::constant(MFrequency::new(frequency, FrequencyRef::LSRK)),
        )
        .with_label(name.clone().unwrap_or_default()));
    }
    let (frequencies, fluxes) = samples.into_iter().unzip();
    let table = TabularSpectrum::new(FrequencyRef::LSRK, frequencies, fluxes).map_err(error)?;
    Ok(SkyComponent::new(
        Flux::stokes_jy(
            spectrum.flux_jy,
            q * spectrum.flux_jy,
            u * spectrum.flux_jy,
            v * spectrum.flux_jy,
        ),
        shape,
        SpectralModel::new(
            MFrequency::new(reference, FrequencyRef::LSRK),
            SpectralKind::Tabular(table),
        ),
    )
    .with_label(name.clone().unwrap_or_default()))
}

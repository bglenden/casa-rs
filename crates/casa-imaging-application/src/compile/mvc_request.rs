// SPDX-License-Identifier: LGPL-3.0-or-later
//! Heap demand for the temporary MVC cube request copy (#1069).

use crate::{AwCfSource, Gridder, ImagingRequest, MeasurementSetSelection, UserMaskBox};

/// Bound the owned buffers cloned from the request, plus formatting the two
/// replacement axis strings. Vector clones allocate their lengths; nested
/// strings and paths allocate their encoded lengths. The axis allowance also
/// covers a moved String reallocation while formatting finite f64 values.
pub(super) fn clone_demand(request: &ImagingRequest) -> Result<u64, super::PrepareError> {
    let overflow = || super::PrepareError::MvcCubeDepth {
        reason: "request-copy memory exceeds addressable size",
    };
    let vector = |length: usize, item: usize| length.checked_mul(item).ok_or_else(overflow);
    // Decimal f64 formatting needs at most its base-10 exponent range plus
    // significant digits, sign, point, unit and allocation growth, for each
    // of the two replacement strings.
    const AXIS_TEXT: usize = 4 * (f64::MAX_10_EXP as usize + 2 * f64::DIGITS as usize + 64);
    let mut bytes = AXIS_TEXT;
    let mut add = |size: usize| -> Result<(), super::PrepareError> {
        bytes = bytes.checked_add(size).ok_or_else(overflow)?;
        Ok(())
    };
    for size in [
        vector(request.vis.len(), size_of::<MeasurementSetSelection>())?,
        vector(
            request.stokes.len(),
            size_of::<casa_imaging_model::PolarizationCoordinate>(),
        )?,
        vector(request.restfreq.len(), size_of::<f64>())?,
        vector(request.scales.len(), size_of::<f64>())?,
        vector(request.mask_box.len(), size_of::<UserMaskBox>())?,
        vector(
            request.startmodel.len(),
            size_of::<Option<std::path::PathBuf>>(),
        )?,
        request.imagename.as_os_str().len(),
    ] {
        add(size)?;
    }
    for selected in &request.vis {
        add(selected.vis.as_os_str().len())?;
        for text in [
            &selected.field,
            &selected.spw,
            &selected.uvrange,
            &selected.scan,
            &selected.observation,
            &selected.antenna,
            &selected.timerange,
            &selected.intent,
        ]
        .into_iter()
        .flatten()
        {
            add(text.len())?;
        }
    }
    for text in [
        &request.phasecenter,
        &request.start,
        &request.width,
        &request.fitspw,
    ]
    .into_iter()
    .flatten()
    {
        add(text.len())?;
    }
    for path in [&request.outlierfile, &request.mask_image]
        .into_iter()
        .flatten()
    {
        add(path.as_os_str().len())?;
    }
    for path in request.startmodel.iter().flatten() {
        add(path.as_os_str().len())?;
    }
    if let Gridder::Awproject(aw) = &request.gridder {
        add(vector(aw.pointingoffsetsigdev.len(), size_of::<f64>())?)?;
        match &aw.cf_source {
            AwCfSource::CasaImport { cfcache } => add(cfcache.as_os_str().len())?,
            AwCfSource::NativeEvla {
                native_cf_cache,
                evla_surface,
                ..
            } => {
                add(native_cf_cache.as_os_str().len())?;
                add(evla_surface.as_os_str().len())?;
            }
        }
    }
    u64::try_from(bytes).map_err(|_| overflow())
}

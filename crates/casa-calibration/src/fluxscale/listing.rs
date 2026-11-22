// SPDX-License-Identifier: LGPL-3.0-or-later
//! CASA's flux-density and fitted-spectrum text listing.

use std::fmt::Write;
use std::path::Path;

use super::{FluxScaleError, FluxScaleReport, FluxScaleRequest};

fn failure(path: &Path, reason: impl ToString) -> FluxScaleError {
    FluxScaleError::PrepareOutput {
        path: path.display().to_string(),
        reason: reason.to_string(),
    }
}

pub(super) fn validate(request: &FluxScaleRequest) -> Result<(), FluxScaleError> {
    let Some(path) = &request.options.listfile else {
        return Ok(());
    };
    let mut products = vec![
        request.input_table.as_path(),
        request.output_table.as_path(),
    ];
    products.extend(request.options.measurement_set.as_deref());
    validate_products(path, products)
}

pub(super) fn validate_products<'a>(
    path: &Path,
    products: impl IntoIterator<Item = &'a Path>,
) -> Result<(), FluxScaleError> {
    for product in products {
        if crate::paths::overlap(path, product).map_err(|error| failure(path, error))? {
            return Err(failure(path, "listfile overlaps a scientific product"));
        }
    }
    Ok(())
}

pub(super) fn write(
    path: &Path,
    request: &FluxScaleRequest,
    report: &FluxScaleReport,
) -> Result<(), FluxScaleError> {
    let mut text = String::new();
    for field in report.fields.values() {
        for (index, &spw) in report.spw_ids.iter().enumerate() {
            let result = &field.spw_results[&spw];
            let reference = super::stats::mapped_reference_spw(spw, &request.refspwmap);
            let mapped = if reference == spw {
                String::new()
            } else {
                format!(" (ref SpW={reference})")
            };
            let _ = write!(text, "# Flux density for {} in SpW={spw}", field.field_name,);
            if result.fluxd[0] >= 0. {
                let _ = write!(text, " (freq={} Hz)", number(report.frequencies_hz[index]));
            }
            let _ = write!(text, "{mapped} is: ");
            if result.fluxd[0] < 0. {
                text.push_str("INSUFFICIENT DATA\n");
            } else {
                let _ = writeln!(
                    text,
                    "{} +/- {} (SNR = {}, N = {})",
                    number(result.fluxd[0]),
                    number(result.fluxd_err[0]),
                    number(result.fluxd[0] / result.fluxd_err[0]),
                    result.num_sol[0]
                );
            }
        }
        if field
            .spw_results
            .values()
            .filter(|result| result.fluxd[0] > 0.)
            .count()
            > 1
        {
            let order = super::fit::effective_order(
                request.options.fitorder,
                field
                    .spw_results
                    .values()
                    .filter(|result| result.fluxd[0] > 0.)
                    .count(),
            );
            let _ = write!(
                text,
                "# Fitted spectrum for {} with fitorder={order}: Flux density = {} +/- {} (freq={} GHz) spidx:",
                field.field_name,
                number(field.fit_fluxd),
                number(field.fit_fluxd_err),
                number(field.fit_ref_frequency_hz / 1e9)
            );
            for index in 1..=order {
                let name = if index == 1 { " (spectral index) " } else { "" };
                let _ = write!(
                    text,
                    " a_{index}{name}={} +/- {}",
                    number(field.spidx[index]),
                    number(field.spidx_err[index])
                );
            }
            if field.covar_mat.len() > 1 {
                text.push_str(" covariance matrix for the fit:");
                for (i, row) in field.covar_mat.iter().enumerate() {
                    for (j, value) in row.iter().enumerate() {
                        let _ = write!(text, " covar({i},{j})={}", number(*value));
                    }
                }
            }
            text.push('\n');
        }
    }
    std::fs::write(path, text).map_err(|error| failure(path, error))
}

fn number(value: f64) -> String {
    format!("{value:.5e}")
}

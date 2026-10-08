// SPDX-License-Identifier: LGPL-3.0-or-later
//! Visibility-domain continuum subtraction of each selected row before it
//! is imaged (the compiled `fitspw` transform, CASA `uvcontsub`).

use casa_imaging_model::{ContinuumFitRule, SelectedNumericRow};
use casa_imaging_reconstruction::{ContinuumRowInput, ContinuumSample, fit_and_subtract_continuum};
use casa_imaging_runtime::pass::SourceError;
use num_complex::{Complex32, Complex64};

/// Replace one row's visibilities, `[channel][correlation]` like `values`,
/// `weights` and `flags`, with the residual of `rule`'s polynomial
/// continuum fitted per correlation over the native frequencies, and flag
/// the channels whose role does not reach the line output so they are not
/// imaged.
///
/// A fit sample is excluded when its correlation, a parallel hand of the
/// same channel (the Stokes-I group) or the row is flagged. Fitted values
/// keep the stored precision: a `FLOAT_DATA` residual stays real.
pub(crate) fn subtract_continuum(
    rule: &ContinuumFitRule,
    row: &SelectedNumericRow<'_>,
    values: &mut [Complex32],
    weights: &[f32],
    flags: &mut [bool],
) -> Result<(), SourceError> {
    let correlations = row.correlations.len();
    let frequencies_hz = row
        .channels
        .iter()
        .map(|channel| channel.frequency_centre_hz)
        .collect::<Vec<_>>();
    let roles = row
        .channels
        .iter()
        .map(|channel| {
            rule.channel_use(channel.channel_index)
                .ok_or("a selected channel has no continuum fit or application role")
        })
        .collect::<Result<Vec<_>, _>>()?;
    let parallel = row
        .correlations
        .iter()
        .map(|product| product.correlation_type().contributes_to_stokes_i())
        .collect::<Vec<_>>();
    let group_flags = (0..row.channels.len())
        .map(|channel| {
            (0..correlations).any(|pol| parallel[pol] && flags[channel * correlations + pol])
        })
        .collect::<Vec<_>>();
    let real = matches!(
        row.visibility,
        casa_imaging_model::SelectedNumericVisibility::Float32(_)
    );
    let mut samples = Vec::with_capacity(row.channels.len());
    for pol in 0..correlations {
        samples.clear();
        samples.extend(roles.iter().enumerate().map(|(channel, role)| {
            let index = channel * correlations + pol;
            let value = values[index];
            ContinuumSample::new(
                Complex64::new(f64::from(value.re), f64::from(value.im)),
                flags[index] || (parallel[pol] && group_flags[channel]) || row.row.row_flag,
                f64::from(weights[index]),
                *role,
            )
        }));
        let result = fit_and_subtract_continuum(ContinuumRowInput::new(
            &frequencies_hz,
            &samples,
            rule.requested_order(),
        ))?;
        for (channel, residual) in result.residual().iter().enumerate() {
            let imaginary = if real { 0.0 } else { residual.im as f32 };
            values[channel * correlations + pol] = Complex32::new(residual.re as f32, imaginary);
        }
    }
    for (channel, role) in roles.iter().enumerate() {
        if !role.contributes_to_output() {
            flags[channel * correlations..(channel + 1) * correlations].fill(true);
        }
    }
    Ok(())
}

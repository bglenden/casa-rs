// SPDX-License-Identifier: LGPL-3.0-or-later
//! Native-channel image coordinates and CASA's rest-frequency default.
use super::{TotalPowerError, failure};
use casa_observation::PartitionId;

pub(super) fn rest_frequency(
    catalogue: &casa_observation::Catalogue,
    partition: PartitionId,
    override_hz: Option<f64>,
    explicit_window: bool,
) -> Result<f64, TotalPowerError> {
    let entry = catalogue.partition(partition);
    let source = entry
        .fields
        .first()
        .and_then(|field| catalogue.fields[field.0 as usize].source)
        .and_then(|source| catalogue.sources.get(source.0 as usize));
    let line = source.and_then(|source| {
        // CASA filters SOURCE by an explicit spw selector. An intent-derived
        // grid window does not change the default line-frequency query.
        source
            .records
            .iter()
            .filter(|record| {
                !explicit_window
                    || matches!(record.window, casa_observation::SourceWindow::All)
                    || record.window == casa_observation::SourceWindow::Window(entry.window)
            })
            .find_map(|record| {
                record
                    .rest_frequencies
                    .as_ref()
                    .and_then(|lines| lines.first())
                    .map(|line| line.hz())
            })
    });
    let frequency = override_hz.or(line).unwrap_or_else(|| {
        entry.frequencies_hz.iter().sum::<f64>() / entry.frequencies_hz.len() as f64
    });
    if !frequency.is_finite() || frequency <= 0. {
        return Err(failure("rest frequency must be finite and positive"));
    }
    Ok(frequency)
}

// SPDX-License-Identifier: LGPL-3.0-or-later
//! Preserve MSv2's stored flags and uncertainty columns alongside a batch.
//! MSv4's scientific batch folds row flags and carries no separate sigma.
//! These storage-preservation reads stay inside the MSv2 creation adapter.
use crate::observation_io::{
    Msv2Observation,
    update::{request_error, storage},
};
use casa_core::measures::MjdHighPrec;
use casa_observation::{ObservationError, VisibilityBatch};
use casa_tables::{
    RequiredScalarColumnDestination, RequiredScalarColumnValuesMut, SelectedArray1DCellsMut,
    SelectedArray2DCellsMut, Table,
};

/// Reused storage for one batch's original acquired rows, without padding.
#[derive(Default)]
pub(super) struct StoredWeights {
    rows: Vec<usize>,
    samples: Vec<Option<usize>>,
    flags: Vec<bool>,
    row_flags: Vec<bool>,
    weights: Vec<f32>,
    sigmas: Vec<f32>,
    weight_spectrum: Vec<f32>,
    sigma_spectrum: Vec<f32>,
    correlations: usize,
    cell: usize,
}

/// Original columns of one acquired sample, in correlation-fastest order.
#[derive(Clone, Copy)]
pub(super) struct StoredSample<'a> {
    pub(super) flags: &'a [bool],
    pub(super) row_flag: bool,
    pub(super) weights: &'a [f32],
    pub(super) sigmas: Option<&'a [f32]>,
    pub(super) weight_spectrum: Option<&'a [f32]>,
    pub(super) sigma_spectrum: Option<&'a [f32]>,
}

impl StoredWeights {
    /// Read selected rows once per column, so a large MS does not incur
    /// several table reads and seeks per sample.
    pub(super) fn read(
        &mut self,
        source: &Msv2Observation,
        batch: &VisibilityBatch<'_>,
    ) -> Result<(), ObservationError> {
        self.rows.clear();
        self.samples.clear();
        self.correlations = batch.correlations.len();
        self.cell = self.correlations * batch.channel_count();
        let origin = MjdHighPrec::new(0., 0.);
        for sample in 0..batch.samples() {
            let coordinate = batch.sample(sample);
            let time = batch
                .time
                .seconds_since(sample / batch.samples_per_time(), origin);
            let row = source.main_row(batch.partition, time, coordinate.baseline);
            self.samples.push(row.map(|row| {
                let index = self.rows.len();
                self.rows.push(row as usize);
                index
            }));
        }
        let main = source.measurement_set().main_table();
        self.flags.clear();
        self.row_flags.clear();
        main.column_accessor("FLAG")
            .map_err(storage)?
            .fill_array_cells_2d_channel_range_typed_uncached(
                &self.rows,
                0,
                batch.channel_count(),
                SelectedArray2DCellsMut::RowChannelBool(&mut self.flags),
            )
            .map_err(storage)?
            .ok_or_else(|| request_error("undefined stored FLAG"))?;
        main.required_scalar_columns_for_rows_into(
            &self.rows,
            &mut [RequiredScalarColumnDestination::new(
                "FLAG_ROW",
                RequiredScalarColumnValuesMut::Bool(&mut self.row_flags),
            )],
        )
        .map_err(storage)?;
        row(main, &self.rows, "WEIGHT", &mut self.weights)?;
        row(main, &self.rows, "SIGMA", &mut self.sigmas)?;
        spectrum(
            main,
            &self.rows,
            "WEIGHT_SPECTRUM",
            batch.channel_count(),
            &mut self.weight_spectrum,
        )?;
        spectrum(
            main,
            &self.rows,
            "SIGMA_SPECTRUM",
            batch.channel_count(),
            &mut self.sigma_spectrum,
        )?;
        if self.flags.len() != self.rows.len() * self.cell
            || self.row_flags.len() != self.rows.len()
            || self.weights.len() != self.rows.len() * self.correlations
        {
            return Err(request_error(
                "stored flags/weights do not have the batch's cell shape",
            ));
        }
        for (values, width) in [
            (&self.sigmas, self.correlations),
            (&self.weight_spectrum, self.cell),
            (&self.sigma_spectrum, self.cell),
        ] {
            if !values.is_empty() && values.len() != self.rows.len() * width {
                return Err(request_error(
                    "stored uncertainty column does not have the batch's cell shape",
                ));
            }
        }
        Ok(())
    }

    pub(super) fn sample(&self, sample: usize) -> Option<StoredSample<'_>> {
        let row = self.samples[sample]?;
        let scalar = row * self.correlations..(row + 1) * self.correlations;
        let cell = row * self.cell..(row + 1) * self.cell;
        Some(StoredSample {
            flags: &self.flags[cell.clone()],
            row_flag: self.row_flags[row],
            weights: &self.weights[scalar.clone()],
            sigmas: (!self.sigmas.is_empty()).then(|| &self.sigmas[scalar]),
            weight_spectrum: (!self.weight_spectrum.is_empty())
                .then(|| &self.weight_spectrum[cell.clone()]),
            sigma_spectrum: (!self.sigma_spectrum.is_empty()).then(|| &self.sigma_spectrum[cell]),
        })
    }
}

fn row(
    table: &Table,
    rows: &[usize],
    column: &str,
    values: &mut Vec<f32>,
) -> Result<(), ObservationError> {
    values.clear();
    if !table
        .schema()
        .is_some_and(|schema| schema.contains_column(column))
        || rows.is_empty()
    {
        return Ok(());
    }
    table
        .column_accessor(column)
        .map_err(storage)?
        .fill_array_cells_1d_typed_uncached(rows, SelectedArray1DCellsMut::Float32(values))
        .map_err(storage)?;
    Ok(())
}

fn spectrum(
    table: &Table,
    rows: &[usize],
    column: &str,
    channels: usize,
    values: &mut Vec<f32>,
) -> Result<(), ObservationError> {
    values.clear();
    if !table
        .schema()
        .is_some_and(|schema| schema.contains_column(column))
        || rows.is_empty()
    {
        return Ok(());
    }
    let filled = table
        .column_accessor(column)
        .map_err(storage)?
        .fill_array_cells_2d_channel_range_typed_uncached(
            rows,
            0,
            channels,
            SelectedArray2DCellsMut::RowChannelFloat32(values),
        )
        .map_err(storage)?;
    if filled.is_none() {
        // A declared, wholly undefined spectrum is legal MSv2 metadata;
        // its WEIGHT/SIGMA summary remains authoritative.
        for row in rows {
            if table.is_cell_defined(*row, column).map_err(storage)? {
                return Err(request_error(
                    "mixed defined/undefined uncertainty spectra are outside #939",
                ));
            }
        }
        values.clear();
    }
    Ok(())
}

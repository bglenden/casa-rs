// SPDX-License-Identifier: LGPL-3.0-or-later
//! Materialize contiguous portions of a regridded spectrum as separate SPWs.

use std::ops::Range;
use std::path::Path;

use casa_tables::{Table, TableOptions};
use casa_types::{ArrayValue, ScalarValue, Value};
use ndarray::{Axis, Slice};

use super::super::{
    MeasurementSetWriteSession, MsTransformError, push_bool_array_row, push_complex32_array_row,
    push_f32_array_row,
};

#[derive(Clone)]
pub(in crate::transform) struct Partitions {
    ranges: Vec<Range<usize>>,
}

impl Partitions {
    pub(super) fn new(channels: usize, count: usize) -> Result<Self, MsTransformError> {
        let length = channels.div_ceil(count.max(1));
        if count == 0 || length.saturating_mul(count.saturating_sub(1)) >= channels {
            return Err(MsTransformError::Spectrum(format!(
                "Cannot split {channels} regridded channels into {count} windows."
            )));
        }
        let ranges = (0..count)
            .map(|part| part * length..channels.min((part + 1) * length))
            .collect();
        Ok(Self { ranges })
    }

    pub(in crate::transform) fn count(&self) -> usize {
        self.ranges.len()
    }

    pub(in crate::transform) fn ranges(&self) -> &[Range<usize>] {
        &self.ranges
    }

    pub(in crate::transform) fn write_channel_array(
        &self,
        session: &mut MeasurementSetWriteSession,
        column: &str,
        value: &ArrayValue,
        path: &Path,
        row: usize,
    ) -> Result<(), MsTransformError> {
        for range in &self.ranges {
            if value.shape()[1] == 1 || (range.start == 0 && range.end == value.shape()[1]) {
                write_spectrum(session, column, value, path, row)?;
            } else {
                write_spectrum(session, column, &channel_slice(value, range), path, row)?;
            }
        }
        Ok(())
    }

    pub(in crate::transform) fn write_noise(
        &self,
        session: &mut MeasurementSetWriteSession,
        statistics: &super::AveragedStatistics,
        flags: &ArrayValue,
        fill_spectra: bool,
        path: &Path,
        row: usize,
    ) -> Result<(), MsTransformError> {
        for range in &self.ranges {
            let ArrayValue::Bool(part_flags) = channel_slice(flags, range) else {
                unreachable!()
            };
            let flags = part_flags
                .view()
                .into_dimensionality::<ndarray::Ix2>()
                .expect("FLAG is rank two");
            for (spectrum, scalar, value) in [
                ("WEIGHT_SPECTRUM", "WEIGHT", &statistics.weight_spectrum),
                ("SIGMA_SPECTRUM", "SIGMA", &statistics.sigma_spectrum),
            ] {
                let cell = channel_slice(value, range);
                let mean = super::combine::mean_noise(&cell, flags)
                    .map_err(|source| MsTransformError::Spectrum(source.to_string()))?;
                push_f32_array_row(session, scalar, &mean, path, row)?;
                if fill_spectra {
                    push_f32_array_row(session, spectrum, &cell, path, row)?;
                }
            }
        }
        Ok(())
    }

    pub(super) fn write_window_references(
        &self,
        request: &super::MsTransformRequest,
    ) -> Result<(), MsTransformError> {
        for name in [
            "FEED",
            "SOURCE",
            "SYSCAL",
            "FREQ_OFFSET",
            "CALDEVICE",
            "SYSPOWER",
        ] {
            let path = request.output_ms.join(name);
            if !path.exists() {
                continue;
            }
            self.rewrite(&path, false)?;
        }
        Ok(())
    }

    pub(super) fn write_descriptions(
        &self,
        request: &super::MsTransformRequest,
    ) -> Result<(), MsTransformError> {
        self.rewrite(&request.output_ms.join("DATA_DESCRIPTION"), true)
    }

    fn rewrite(&self, path: &Path, by_polarization: bool) -> Result<(), MsTransformError> {
        let table_error = |source| MsTransformError::MutateMeasurementSet {
            path: path.display().to_string(),
            source: Box::new(source),
        };
        let rows_per_part = Table::open(TableOptions::new(path))
            .map_err(table_error)?
            .row_count();
        let count = rows_per_part.checked_mul(self.count()).ok_or_else(|| {
            MsTransformError::Spectrum("Too many partitioned subtable rows.".into())
        })?;
        if count == 0 {
            return Ok(());
        }
        let rows = (0..count)
            .map(|i| {
                if by_polarization {
                    i / self.count()
                } else {
                    i % rows_per_part
                }
            })
            .collect::<Vec<_>>();
        let mut name = path.file_name().expect("subtable path").to_os_string();
        name.push(".regrid-original");
        let original = path.with_file_name(name);
        let io_error = |source: std::io::Error| MsTransformError::PrepareOutput {
            path: path.display().to_string(),
            reason: source.to_string(),
        };
        std::fs::rename(path, &original).map_err(io_error)?;
        let mut index = 0;
        super::super::rewrite_selected_table_rows(&original, path, &rows, |_, mut row| {
            let part = if by_polarization {
                index % self.count()
            } else {
                index / rows_per_part
            };
            if by_polarization || part != 0 {
                row.upsert(
                    "SPECTRAL_WINDOW_ID",
                    Value::Scalar(ScalarValue::Int32(part as i32)),
                );
            }
            index += 1;
            Ok(row)
        })?;
        std::fs::remove_dir_all(original).map_err(io_error)
    }
}

pub(in crate::transform) fn channel_slice(value: &ArrayValue, range: &Range<usize>) -> ArrayValue {
    let axis = Axis(1);
    let slice = Slice::from(range.clone());
    match value {
        ArrayValue::Bool(array) => ArrayValue::Bool(array.slice_axis(axis, slice).to_owned()),
        ArrayValue::Float32(array) => ArrayValue::Float32(array.slice_axis(axis, slice).to_owned()),
        ArrayValue::Complex32(array) => {
            ArrayValue::Complex32(array.slice_axis(axis, slice).to_owned())
        }
        _ => unreachable!("channel slices have the supported complex, float or flag formats"),
    }
}

pub(in crate::transform) fn write_spectrum(
    session: &mut MeasurementSetWriteSession,
    column: &str,
    value: &ArrayValue,
    path: &Path,
    row: usize,
) -> Result<(), MsTransformError> {
    match value {
        ArrayValue::Bool(_) => push_bool_array_row(session, column, value, path, row),
        ArrayValue::Float32(_) => push_f32_array_row(session, column, value, path, row),
        _ => push_complex32_array_row(session, column, value, path, row),
    }
}

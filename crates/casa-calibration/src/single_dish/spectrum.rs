// SPDX-License-Identifier: LGPL-3.0-or-later
//! Accumulate and write exposure-weighted sky spectra.

use std::borrow::Cow;
use std::collections::{BTreeMap, BTreeSet};
use std::path::Path;

use casa_ms::MeasurementSet;
use casa_tables::Table;
use casa_types::{ArrayValue, RecordField, RecordValue, ScalarValue, Value};
use ndarray::{ArrayD, Axis, IxDyn, Zip};

use crate::CalibrationOutputMode;
use crate::constants::*;
use crate::session::scan_timing::ScanTiming;
use crate::table_maintenance::write_standard_calibration_subtables;
use crate::writer::{
    CalibrationTableDescriptor, CalibrationTableWriter, new_calibration_table_schema,
};

/// Position-switch timestamps exclude samples with every cell flagged.
/// The continuous-map solver includes their time and exposure in its group.
#[derive(Clone, Copy)]
pub(super) enum ReferenceTimestamp {
    UnflaggedCells,
    AllIntegrations,
}

#[derive(Default)]
struct Integrations {
    count: usize,
    time_sum: f64,
    exposure_sum: f64,
}

impl Integrations {
    fn add(&mut self, time: f64, exposure: f64) {
        self.count += 1;
        self.time_sum += time;
        self.exposure_sum += exposure;
    }
}

pub(super) struct SkySpectrum {
    spw: usize,
    signal: ArrayD<f32>,
    exposure: ArrayD<f32>,
    all: Integrations,
    usable: Integrations,
}

impl SkySpectrum {
    pub(super) fn new(spw: usize, template: &ArrayD<f32>) -> Self {
        Self {
            spw,
            signal: ArrayD::zeros(template.raw_dim()),
            exposure: ArrayD::zeros(template.raw_dim()),
            all: Integrations::default(),
            usable: Integrations::default(),
        }
    }

    pub(super) fn add(
        &mut self,
        data: &ArrayD<f32>,
        flags: &ArrayD<bool>,
        time: f64,
        exposure: f64,
    ) -> Result<(), String> {
        if self.signal.shape() != data.shape()
            || data.shape() != flags.shape()
            || data.ndim() != 2
            || !(1..=2).contains(&data.shape()[0])
        {
            return Err("Sky calibration data and FLAG shapes disagree.".into());
        }
        self.all.add(time, exposure);
        let mut has_usable_channel = false;
        Zip::from(&mut self.signal)
            .and(&mut self.exposure)
            .and(data)
            .and(flags)
            .for_each(|signal, weight, &power, &flagged| {
                if !flagged {
                    *signal += exposure as f32 * power;
                    *weight += exposure as f32;
                    has_usable_channel = true;
                }
            });
        if has_usable_channel {
            self.usable.add(time, exposure);
        }
        Ok(())
    }

    fn finish(
        mut self,
        antenna: i32,
        scans: &ScanTiming,
        channel_width: f64,
        average: ReferenceTimestamp,
    ) -> RecordValue {
        let stamps =
            if matches!(average, ReferenceTimestamp::UnflaggedCells) && self.usable.count > 0 {
                &self.usable
            } else {
                &self.all
            };
        let time = stamps.time_sum / stamps.count as f64;
        let interval = stamps.exposure_sum;
        let scan = scans.at(time);
        Zip::from(&mut self.signal)
            .and(&self.exposure)
            .for_each(|signal, &exposure| {
                if exposure != 0. {
                    *signal /= exposure;
                }
            });
        let input_shape = self.signal.raw_dim();
        let signal = two_receptors(self.signal, 0.);
        let shape = signal.raw_dim();
        let flags = two_receptors(self.exposure.mapv(|exposure| exposure == 0.), true);
        let error = two_receptors(ArrayD::from_elem(input_shape.clone(), 0.1), 0.);
        let snr = two_receptors(ArrayD::from_elem(input_shape, 1.), 0.);
        let integer =
            |name, value| RecordField::new(name, Value::Scalar(ScalarValue::Int32(value)));
        let number =
            |name, value| RecordField::new(name, Value::Scalar(ScalarValue::Float64(value)));
        let array = |name, array| RecordField::new(name, Value::Array(array));
        RecordValue::new(vec![
            number(COL_TIME, time),
            number(COL_INTERVAL, interval),
            integer(COL_ANTENNA1, antenna),
            integer(COL_ANTENNA2, antenna),
            integer(COL_FIELD_ID, scan.field),
            integer(COL_SCAN_NUMBER, scan.number),
            integer(COL_OBSERVATION_ID, scan.observation),
            integer(COL_SPECTRAL_WINDOW_ID, self.spw as i32),
            array(COL_FPARAM, ArrayValue::Float32(signal)),
            array(COL_FLAG, ArrayValue::Bool(flags)),
            array(COL_PARAMERR, ArrayValue::Float32(error)),
            array(COL_SNR, ArrayValue::Float32(snr)),
            array(
                COL_WEIGHT,
                ArrayValue::Float32(ArrayD::from_elem(
                    shape,
                    (channel_width.abs().trunc() * interval) as f32,
                )),
            ),
        ])
    }
}

fn two_receptors<T: Clone>(one_or_two: ArrayD<T>, fill: T) -> ArrayD<T> {
    if one_or_two.shape()[0] == 2 {
        return one_or_two;
    }
    let mut two = ArrayD::from_elem(IxDyn(&[2, one_or_two.shape()[1]]), fill);
    two.index_axis_mut(Axis(0), 0)
        .assign(&one_or_two.index_axis(Axis(0), 0));
    two
}

pub(super) fn write(
    ms: &MeasurementSet,
    destination: &Path,
    mode: CalibrationOutputMode,
    subtype: &str,
    average: ReferenceTimestamp,
    solutions: impl IntoIterator<Item = (i32, SkySpectrum)>,
) -> Result<usize, String> {
    let scans = ScanTiming::read(ms.main_table())?;
    let spectral = ms.spectral_window().map_err(error_text)?;
    let output = crate::output::CalibrationOutput::prepare_single_dish_solve(destination, mode)
        .map_err(error_text)?;
    let mut writer = CalibrationTableWriter::staged(
        ms,
        CalibrationTableDescriptor {
            output: destination,
            schema: new_calibration_table_schema(false),
            subtype,
            parameter_type: Some("Float"),
            measurement_set_name: ms
                .path()
                .map_or_else(|| "none".into(), |path| path.display().to_string()),
            include_polarization_basis: true,
            time_extra_precision_column: None,
        },
        output,
        mode,
    );
    let mut written = 0;
    let mut present_windows = BTreeSet::new();
    let mut channel_widths = BTreeMap::new();
    for (antenna, spectrum) in solutions {
        present_windows.insert(spectrum.spw as i32);
        let width = match channel_widths.entry(spectrum.spw) {
            std::collections::btree_map::Entry::Occupied(entry) => *entry.get(),
            std::collections::btree_map::Entry::Vacant(entry) => {
                let width = spectral
                    .chan_width(spectrum.spw)
                    .map_err(error_text)?
                    .first()
                    .copied()
                    .ok_or("Missing channel width for sky calibration.")?;
                *entry.insert(width)
            }
        };
        writer
            .append(spectrum.finish(antenna, &scans, width, average))
            .map_err(error_text)?;
        written += 1;
    }
    write_standard_calibration_subtables(ms, writer.output_path(), None)?;
    crate::gencal::write_spectral_axis(
        &writer.output_path().join("SPECTRAL_WINDOW"),
        &present_windows,
        false,
    )
    .map_err(error_text)?;
    writer.save_main().map_err(error_text)?;
    Ok(written)
}

pub(super) fn read_power<'a>(
    main: &'a Table,
    row: usize,
    column: &str,
) -> Result<Cow<'a, ArrayD<f32>>, String> {
    let value = main
        .cell_accessor(row, column)
        .and_then(|cell| cell.array())
        .map_err(error_text)?;
    match value {
        ArrayValue::Float32(data) => Ok(Cow::Borrowed(data)),
        ArrayValue::Complex32(data) => Ok(Cow::Owned(data.mapv(|value| value.re))),
        _ => Err(format!(
            "MeasurementSet {column} is not single-precision visibility data."
        )),
    }
}

fn error_text(error: impl std::fmt::Display) -> String {
    error.to_string()
}

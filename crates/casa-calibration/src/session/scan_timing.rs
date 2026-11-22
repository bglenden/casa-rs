// SPDX-License-Identifier: LGPL-3.0-or-later
//! Map calibration epochs to the MeasurementSet scans used by CASA.

use std::collections::BTreeMap;

use casa_tables::{RequiredScalarColumnValues, Table};

use crate::constants::{COL_FIELD_ID, COL_OBSERVATION_ID, COL_SCAN_NUMBER, COL_TIME};

pub(crate) struct ScanTiming(Vec<Scan>);

pub(crate) struct Scan {
    pub(crate) number: i32,
    pub(crate) field: i32,
    pub(crate) observation: i32,
    first_time: f64,
    last_time: f64,
}

impl ScanTiming {
    pub(crate) fn read(main: &Table) -> Result<Self, String> {
        let columns = main
            .required_scalar_columns_owned(&[
                COL_TIME,
                COL_SCAN_NUMBER,
                COL_FIELD_ID,
                COL_OBSERVATION_ID,
            ])
            .map_err(|error| error.to_string())?;
        let RequiredScalarColumnValues::Float64(times) = &columns[COL_TIME] else {
            return Err("MeasurementSet TIME is not Float64.".into());
        };
        let ids = |name: &str| match &columns[name] {
            RequiredScalarColumnValues::Int32(values) => Ok(values.as_slice()),
            _ => Err(format!("MeasurementSet {name} is not Int32.")),
        };
        let numbers = ids(COL_SCAN_NUMBER)?;
        let fields = ids(COL_FIELD_ID)?;
        let observations = ids(COL_OBSERVATION_ID)?;
        let mut by_scan = BTreeMap::new();
        for (row, &time) in times.iter().enumerate() {
            let scan = by_scan.entry(numbers[row]).or_insert(Scan {
                number: numbers[row],
                field: fields[row],
                observation: observations[row],
                first_time: time,
                last_time: time,
            });
            scan.first_time = scan.first_time.min(time);
            scan.last_time = scan.last_time.max(time);
        }
        let mut scans = by_scan.into_values().collect::<Vec<_>>();
        if scans.is_empty() {
            return Err("MeasurementSet has no visibility scans.".into());
        }
        scans.sort_by(|left, right| left.last_time.total_cmp(&right.last_time));
        Ok(Self(scans))
    }

    pub(crate) fn at(&self, time: f64) -> &Scan {
        let scans = &self.0;
        let first = &scans[0];
        let last = &scans[scans.len() - 1];
        // assignCTScanField uses the next scan after a gap and clips the ends.
        if time < first.last_time + 1e-5 {
            first
        } else if time > last.first_time - 1e-5 {
            last
        } else {
            &scans[scans.partition_point(|scan| time > scan.last_time + 1e-5)]
        }
    }
}

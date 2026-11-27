// SPDX-License-Identifier: LGPL-3.0-or-later
//! CASA's per-record project, epoch and receiver-band tests.

use casa_core::quanta::{Quantity, Unit};

use crate::disk::LogicalRecord;
use crate::record::CdaId;
use crate::{BandName, ImportVlaOptions, VlaError};

#[derive(Debug)]
pub(crate) struct ArchiveSelection {
    project: Option<String>,
    start_mjd: f64,
    stop_mjd: f64,
    band: Option<(f64, f64)>,
}

impl ArchiveSelection {
    pub(crate) fn new(options: &ImportVlaOptions) -> Result<Self, VlaError> {
        options.require_archivefiles()?;
        let project = options
            .project
            .as_deref()
            .map(str::trim)
            .filter(|name| !matches!(*name, "" | "all"));
        if project.is_some_and(|name| name.len() > 6) {
            return Err(VlaError::InvalidArgument {
                argument: "project",
                message: "The project name must have no more than 6 characters".into(),
            });
        }
        // A malformed date disables that endpoint in CASA. Its empty
        // arguments instead install the filler defaults.
        let start_mjd =
            archive_date(options.starttime.as_deref(), "1970/1/31/00:00:00").unwrap_or(0.0);
        let stop_mjd = archive_date(options.stoptime.as_deref(), "2199/1/31/23:59:59")
            .unwrap_or(f64::MAX / 2.0);
        if start_mjd > stop_mjd {
            return Err(VlaError::InvalidArgument {
                argument: "stoptime",
                message: "The start time must precede the stop time".into(),
            });
        }
        Ok(Self {
            project: project.map(str::to_ascii_uppercase),
            start_mjd,
            stop_mjd,
            band: options
                .bandname
                .map(|band| band_range(band, options.evlabands)),
        })
    }

    pub(crate) fn matches_project(&self, project: &str) -> bool {
        self.project
            .as_ref()
            .is_none_or(|name| project.trim().eq_ignore_ascii_case(name))
    }

    pub(crate) fn matches_time_and_band(&self, record: &LogicalRecord) -> Result<bool, VlaError> {
        let error = |field, source| VlaError::import(format!("decode {field}: {source}"));
        let rca = record.rca();
        let sda = record.sda().map_err(|source| error("SDA", source))?;
        let day = rca.obs_day().map_err(|source| error("obs day", source))?;
        let seconds = sda
            .observation_time_seconds()
            .map_err(|source| error("observation time", source))?;
        let time = f64::from(day) + seconds / 86_400.0;
        if time <= self.start_mjd || time >= self.stop_mjd {
            return Ok(false);
        }
        let Some((low, high)) = self.band else {
            return Ok(true);
        };
        for index in [CdaId::Cda0, CdaId::Cda1, CdaId::Cda2, CdaId::Cda3] {
            if rca
                .cda_offset_bytes(index.index())
                .map_err(|source| error("CDA offset", source))?
                == 0
                || !record
                    .cda(index)
                    .map_err(|source| error("CDA", source))?
                    .is_valid()
            {
                continue;
            }
            let edge = sda
                .edge_frequency_hz(index)
                .map_err(|source| error("band edge", source))?;
            let width = sda
                .channel_width_hz(index)
                .map_err(|source| error("channel width", source))?;
            let channels = sda
                .n_channels(index)
                .map_err(|source| error("channel count", source))?;
            if high > edge && low < edge + f64::from(channels) * width {
                return Ok(true);
            }
        }
        Ok(false)
    }
}

fn archive_date(value: Option<&str>, default: &str) -> Option<f64> {
    let value = value.filter(|value| !value.is_empty()).unwrap_or(default);
    let quantity = Quantity::read(value).ok()?;
    if quantity.unit().name().is_empty() {
        return Some(quantity.value());
    }
    quantity
        .get_value_in(&Unit::new("d").ok()?)
        .ok()
        .or_else(|| {
            let radians = quantity.get_value_in(&Unit::new("rad").ok()?).ok()?;
            Some(radians / std::f64::consts::TAU)
        })
}

fn band_range(band: BandName, evla: bool) -> (f64, f64) {
    use BandName::*;
    // Centre and full width in MHz, as selected by CASA's vlafiller.
    let (centre, width) = match (band, evla) {
        (Four, _) => (75.0, 48.0),
        (P, _) => (321.5, 47.0),
        (L, false) => (1450.0, 600.0),
        (L, true) => (1500.0, 1000.0),
        (S, _) => (3000.0, 2000.0),
        (C, false) => (4650.0, 900.0),
        (C, true) => (6000.0, 4000.0),
        (X, false) => (8200.0, 2800.0),
        (X, true) => (10_000.0, 4000.0),
        (U, false) => (14_900.0, 2800.0),
        (U, true) => (15_000.0, 6000.0),
        (K, false) => (23_300.0, 5000.0),
        (K, true) => (22_260.0, 8000.0),
        (Ka, _) => (33_250.0, 13_500.0),
        (Q, false) => (44_500.0, 13_000.0),
        (Q, true) => (45_000.0, 10_000.0),
    };
    ((centre - width * 0.5) * 1e6, (centre + width * 0.5) * 1e6)
}

// SPDX-License-Identifier: LGPL-3.0-or-later
//! Resolve the explicit-geometry, native-channel subset of CASA's task.
use super::{TotalPowerError, TotalPowerProducts, TotalPowerRequest, failure};
use casa_coordinates::{DirectionCoordinate, Projection, ProjectionType};
use casa_imaging_singledish::{GridFunction, KernelParameters};
use casa_observation::{AntennaId, Catalogue, WindowId};
use casa_types::{
    measures::direction::DirectionRef,
    quanta::{Quantity, Unit},
};
use serde_json::{Map, Value};
use std::path::PathBuf;

/// Return the canonical CASA 6.7.6 `tsdimaging` parameter defaults as plain JSON.
#[must_use]
pub fn default_parameters() -> Map<String, Value> {
    let value = serde_json::json!({
        "infiles": [],
        "outfile": "",
        "overwrite": false,
        "field": ["none"],
        "spw": ["none"],
        "antenna": [""],
        "scan": [""],
        "intent": ["OBSERVE_TARGET#ON_SOURCE"],
        "timerange": [""],
        "outframe": "",
        "mode": "channel",
        "nchan": -1,
        "start": 0,
        "width": 1,
        "veltype": "radio",
        "specmode": "cube",
        "interpolation": "linear",
        "pointingcolumn": "direction",
        "convertfirst": "never",
        "projection": "SIN",
        "imsize": [],
        "cell": [""],
        "phasecenter": "",
        "stokes": "I",
        "gridfunction": "BOX",
        "convsupport": -1,
        "truncate": -1,
        "gwidth": -1,
        "jwidth": -1,
        "clipminmax": false,
        "minweight": 0.1,
        "brightnessunit": "",
        "restfreq": [""]
    });
    match value {
        Value::Object(map) => map,
        _ => unreachable!("default map is an object"),
    }
}

/// CASA-named parameters resolved over the task's catalog defaults.
pub struct TotalPowerParameters<'a>(pub &'a Map<String, Value>);

impl TotalPowerParameters<'_> {
    /// The one input measurement set supported by the simulation core.
    ///
    /// # Errors
    /// No input, multiple input sets, or a non-path input value.
    pub fn input(&self) -> Result<PathBuf, TotalPowerError> {
        let paths = self.list("infiles")?;
        match paths.as_slice() {
            [Value::String(path)] if !path.is_empty() => Ok(path.into()),
            [] => Err(failure("infiles must contain a MeasurementSet")),
            [_] => Err(failure("infiles must contain a MeasurementSet path")),
            _ => Err(TotalPowerError::Unsupported {
                behavior: "multiple input MeasurementSets",
                issue: 938,
            }),
        }
    }

    /// Build the native request and publication policy without mutating inputs.
    ///
    /// # Errors
    /// Invalid geometry, unavailable channel/antenna/window, or a mode outside
    /// the simulation core, named with its owning issue.
    pub fn request(
        &self,
        catalogue: &Catalogue,
    ) -> Result<(TotalPowerRequest, TotalPowerProducts), TotalPowerError> {
        if self.text("outfile")?.is_empty() {
            return Err(failure("outfile must name an output stem"));
        }
        let sizes = self.list("imsize")?;
        let cells = self.list("cell")?;
        let phase = self.text("phasecenter")?;
        if sizes.is_empty() || cells.is_empty() || cells.iter().any(|v| v == "") || phase.is_empty()
        {
            return Err(TotalPowerError::Unsupported {
                behavior: "automatic total-power image geometry",
                issue: 938,
            });
        }
        let axis_size = |axis: usize| -> Result<usize, TotalPowerError> {
            sizes
                .get(axis)
                .unwrap_or(&sizes[0])
                .as_u64()
                .and_then(|v| usize::try_from(v).ok())
                .filter(|v| *v > 0)
                .ok_or_else(|| failure("imsize requires one or two positive integer dimensions"))
        };
        let axis_cell = |axis: usize| -> Result<f64, TotalPowerError> {
            let text = cells
                .get(axis)
                .unwrap_or(&cells[0])
                .as_str()
                .ok_or_else(|| failure("cell must contain angular quantities"))?;
            let cell = quantity(text, "rad")?.abs();
            if !cell.is_finite() || cell == 0. {
                return Err(failure("cell must be finite and nonzero"));
            }
            Ok(cell)
        };
        let size = [axis_size(0)?, axis_size(1)?];
        let cell = [axis_cell(0)?, axis_cell(1)?];
        let parts: Vec<_> = phase.split_whitespace().collect();
        let [frame, longitude, latitude] = parts.as_slice() else {
            return Err(failure(
                "phasecenter requires a frame, longitude and latitude",
            ));
        };
        let frame = frame.parse::<DirectionRef>().map_err(failure)?;
        let direction = DirectionCoordinate::new(
            frame,
            Projection::new(ProjectionType::SIN),
            [quantity(longitude, "rad")?, quantity(latitude, "rad")?],
            [-cell[0], cell[1]],
            [(size[0] / 2) as f64, (size[1] / 2) as f64],
        );
        let windows = self.windows(catalogue)?;
        let selected = super::plan::selected_partitions(catalogue, self.text("intent")?, &windows)?;
        let entry = catalogue.partition(selected[0]);
        let start = if self.0["start"] == "" {
            0
        } else {
            integer(&self.0["start"])?
        };
        let width = if self.0["width"] == "" {
            1
        } else {
            integer(&self.0["width"])?
        };
        if start < 0 || width != 1 {
            return Err(TotalPowerError::Unsupported {
                behavior: "spectral averaging or regridding",
                issue: 938,
            });
        }
        let start = start as usize;
        let nchan = self.0["nchan"]
            .as_i64()
            .ok_or_else(|| failure("nchan must be an integer"))?;
        let end = if nchan == -1 {
            entry.frequencies_hz.len()
        } else {
            start
                .checked_add(usize::try_from(nchan).map_err(failure)?)
                .ok_or_else(|| failure("channel selection overflow"))?
        };
        let rest = match &self.0["restfreq"] {
            Value::Array(v) if v.len() == 1 => &v[0],
            Value::Array(v) if v.is_empty() => &Value::String(String::new()),
            value => value,
        };
        let rest_frequency_hz = if rest == "" {
            None
        } else {
            Some(number_or_quantity(rest, "Hz")?)
        };
        let pixel_parameter = |name: &str| {
            let value = &self.0[name];
            if let Some(text) = value.as_str() {
                if let Ok(value) = text.trim_end_matches("pix").parse::<f64>() {
                    return if value.is_finite() {
                        Ok(value)
                    } else {
                        Err(failure("kernel parameters must be finite"))
                    };
                }
                quantity(text, "rad").map(|v| v / cell[0])
            } else {
                number_or_quantity(value, "rad")
            }
        };
        let kernel = GridFunction::from_casa(
            self.text("gridfunction")?,
            KernelParameters {
                convsupport: self.0["convsupport"]
                    .as_i64()
                    .ok_or_else(|| failure("convsupport must be an integer"))?,
                truncate: pixel_parameter("truncate")?,
                gwidth: pixel_parameter("gwidth")?,
                jwidth: pixel_parameter("jwidth")?,
            },
        )
        .map_err(failure)?;
        let brightness_unit = match self.text("brightnessunit")?.to_ascii_lowercase().as_str() {
            "" => default_brightness_unit(entry),
            "k" => "K".into(),
            "jy/beam" => "Jy/beam".into(),
            _ => return Err(failure("brightnessunit must be K or Jy/beam")),
        };
        Ok((
            TotalPowerRequest {
                direction,
                size,
                channels: start..end,
                intent: self.text("intent")?.into(),
                kernel,
                rest_frequency_hz,
                windows,
                antennas: self.antennas(catalogue)?,
            },
            TotalPowerProducts {
                outfile: self.text("outfile")?.into(),
                overwrite: self.0["overwrite"]
                    .as_bool()
                    .ok_or_else(|| failure("overwrite must be boolean"))?,
                minweight: self.0["minweight"]
                    .as_f64()
                    .ok_or_else(|| failure("minweight must be numeric"))?,
                brightness_unit,
            },
        ))
    }

    fn text(&self, name: &str) -> Result<&str, TotalPowerError> {
        let value = self
            .0
            .get(name)
            .and_then(|v| match v {
                Value::Array(values) if values.len() == 1 => values[0].as_str(),
                value => value.as_str(),
            })
            .ok_or_else(|| failure(format!("{name} must be a string")))?;
        Ok(
            if value == "none"
                && matches!(
                    name,
                    "intent" | "antenna" | "spw" | "field" | "scan" | "timerange"
                )
            {
                ""
            } else {
                value
            },
        )
    }
    fn list(&self, name: &str) -> Result<Vec<Value>, TotalPowerError> {
        match self.0.get(name) {
            Some(Value::Array(values)) => Ok(values.clone()),
            Some(value) => Ok(vec![value.clone()]),
            None => Err(failure(format!("missing {name}"))),
        }
    }
    fn windows(&self, catalogue: &Catalogue) -> Result<Vec<WindowId>, TotalPowerError> {
        let text = self.text("spw")?;
        if matches!(text, "" | "none" | "*") {
            return Ok(Vec::new());
        }
        text.split(',')
            .map(|word| {
                word.trim()
                    .parse::<usize>()
                    .ok()
                    .filter(|id| *id < catalogue.windows.len())
                    .or_else(|| {
                        catalogue
                            .windows
                            .iter()
                            .position(|window| window.name == word.trim())
                    })
                    .map(|id| WindowId(id as u32))
                    .ok_or(TotalPowerError::Unsupported {
                        behavior: "this spectral-window selector",
                        issue: 938,
                    })
            })
            .collect()
    }
    fn antennas(&self, catalogue: &Catalogue) -> Result<Vec<AntennaId>, TotalPowerError> {
        let text = self.text("antenna")?;
        if text.is_empty() {
            return Ok(Vec::new());
        }
        text.split(',')
            .map(|word| {
                let word = word.trim().strip_suffix("&&&").unwrap_or(word.trim());
                word.parse::<usize>()
                    .ok()
                    .filter(|id| *id < catalogue.antennas.len())
                    .or_else(|| {
                        catalogue
                            .antennas
                            .iter()
                            .position(|antenna| antenna.name == word)
                    })
                    .map(|id| AntennaId(id as u32))
                    .ok_or(TotalPowerError::Unsupported {
                        behavior: "this autocorrelation antenna selector",
                        issue: 938,
                    })
            })
            .collect()
    }
}

fn default_brightness_unit(entry: &casa_observation::PartitionEntry) -> String {
    use casa_observation::VisibilityUnits;
    // CASA takes the first declared unit in this order, even when the
    // preferred data column has no unit of its own.
    let unit = ["corrected", "float", "base"].into_iter().find_map(|name| {
        match &entry.data_group(name)?.1.units {
            VisibilityUnits::Undeclared => None,
            VisibilityUnits::Jansky => Some("Jy"),
            VisibilityUnits::Declared(unit) => Some(unit.as_str()),
        }
    });
    if unit.is_some_and(|unit| unit.trim().eq_ignore_ascii_case("K")) {
        "K".into()
    } else {
        "Jy/beam".into()
    }
}

fn integer(value: &Value) -> Result<i64, TotalPowerError> {
    value
        .as_i64()
        .or_else(|| value.as_str()?.parse().ok())
        .ok_or_else(|| failure("native-channel start and width must be integers"))
}
fn quantity(text: &str, unit: &str) -> Result<f64, TotalPowerError> {
    Quantity::read(text)
        .map_err(failure)?
        .get_value_in(&Unit::new(unit).map_err(failure)?)
        .map_err(failure)
}
fn number_or_quantity(value: &Value, unit: &str) -> Result<f64, TotalPowerError> {
    let number = match value {
        Value::Number(v) => v
            .as_f64()
            .ok_or_else(|| failure("numeric value is out of range"))?,
        Value::String(v) => v.parse().map_or_else(|_| quantity(v, unit), Ok)?,
        _ => return Err(failure("expected a number or quantity")),
    };
    if !number.is_finite() {
        return Err(failure("numeric value must be finite"));
    }
    Ok(number)
}

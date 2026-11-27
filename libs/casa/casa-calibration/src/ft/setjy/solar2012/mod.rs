// SPDX-License-Identifier: LGPL-3.0-or-later
//! Butler–JPL-Horizons 2012 brightness, Doppler shift, and resolved disks.

use std::path::PathBuf;

use casa_components::{ComponentShape, Flux, ShapeType, SkyComponent, SpectralKind, SpectralModel};
use casa_core::measures::direction::{DirectionRef, MDirection};
use casa_core::measures::doppler::{DopplerRef, MDoppler};
use casa_core::measures::frame::MeasFrame;
use casa_core::measures::frequency::MFrequency;
use casa_core::measures::radial_velocity::{MRadialVelocity, RadialVelocityRef};
use casa_core::{ArrayValue, ScalarValue, Value};
use casa_measures::MeasuresRuntime;
use casa_ms::MsResult;
use casa_tables::{Table, TableOptions};

use crate::ft::{invalid, setjy::sampled_shape};

use super::solar2010::scalar;

mod brightness;
mod interpolate;

use brightness::Brightness;
use interpolate::window;

const AU_METRES: f64 = 1.495_978_706_6e11;
const LIGHT_SPEED: f64 = 299_792_458.;

/// Butler-JPL-Horizons 2012 apparent disk model for a Solar System body at a specified epoch.
pub struct Solar2012 {
    ephemeris: Ephemeris,
    brightness: Brightness,
}

impl Solar2012 {
    /// Opens the body's ephemeris and samples it at the requested Modified Julian Date in days.
    /// Missing models and uncovered epochs return an error.
    pub fn open(body: &str, mjd: f64) -> MsResult<Self> {
        let body = canonical_body(body).ok_or_else(|| {
            invalid(format!(
                "Solar System object {body:?} is not supported by Butler-JPL-Horizons 2012"
            ))
        })?;
        let roots: Vec<_> = MeasuresRuntime::discover_candidates()
            .map_err(invalid)?
            .into_iter()
            .map(|runtime| runtime.path)
            .collect();
        let ephemeris = Ephemeris::read(&roots, body, mjd)?;
        let models = roots
            .iter()
            .map(|root| root.join("alma/SolarSystemModels"))
            .find(|root| root.is_dir())
            .ok_or_else(|| invalid("Missing Tb or fd models in the data directory"))?;
        let brightness = Brightness::read(&models, body, mjd)?;
        Ok(Self {
            ephemeris,
            brightness,
        })
    }

    /// Borrows the apparent sky direction with its measure frame.
    pub fn direction(&self) -> &MDirection {
        &self.ephemeris.direction
    }

    /// Produces a disk component whose flux density is averaged over the requested frequency bands.
    pub fn component(
        &self,
        direction: MDirection,
        frame: &MeasFrame,
        frequencies: &[MFrequency],
        bands: &[[f64; 2]],
        by_channel: bool,
    ) -> MsResult<(SkyComponent, f64, bool)> {
        let topocentric =
            MRadialVelocity::new(self.ephemeris.radial_velocity, RadialVelocityRef::GEO)
                .convert_to(RadialVelocityRef::TOPO, frame)
                .map_err(invalid)?;
        let ratio =
            MDoppler::new(topocentric.ms() / LIGHT_SPEED, DopplerRef::BETA).as_frequency_ratio();
        let shift = 2. - ratio;
        let area = if self.brightness.absolute_flux {
            1.
        } else {
            1e26 * std::f64::consts::PI * self.ephemeris.major * self.ephemeris.minor * 0.25
        };
        let mut supported = false;
        let fluxes = bands
            .iter()
            .map(|&[low, high]| {
                let flux = self.brightness.sample([low * shift, high * shift]);
                supported |= flux.is_some();
                [flux.unwrap_or_default() * area, 0., 0., 0.]
            })
            .collect::<Vec<_>>();
        let first = fluxes
            .first()
            .ok_or_else(|| invalid("no Solar System frequency bands"))?[0];
        // The return and scratch model retain a zero for an unsupported band
        // if a component can still be computed for the field.
        let shape = ComponentShape::from_parameters(
            ShapeType::Disk,
            direction,
            &[
                self.ephemeris.major,
                self.ephemeris.minor,
                self.ephemeris.angle,
            ],
            None,
        )
        .map_err(invalid)?;
        let component = if by_channel && frequencies.len() > 1 {
            sampled_shape(shape, frequencies, fluxes)?.0
        } else {
            let reference = MFrequency::new(bands[0][0], frequencies[0].refer());
            let spectrum = if by_channel {
                SpectralKind::SpectralIndex(casa_components::SpectralIndex::new(2.))
            } else {
                SpectralKind::Constant
            };
            SkyComponent::new(
                Flux::stokes_jy(first, 0., 0., 0.),
                shape,
                SpectralModel::new(reference, spectrum),
            )
        };
        Ok((component, first, supported))
    }
}

fn canonical_body(name: &str) -> Option<&'static str> {
    [
        "Venus", "Mars", "Jupiter", "Uranus", "Neptune", "Io", "Europa", "Ganymede", "Callisto",
        "Titan", "Ceres", "Juno", "Pallas", "Vesta", "Hygeia",
    ]
    .into_iter()
    .find(|body| name.eq_ignore_ascii_case(body))
}

struct Ephemeris {
    direction: MDirection,
    major: f64,
    minor: f64,
    angle: f64,
    radial_velocity: f64,
}

impl Ephemeris {
    fn read(roots: &[PathBuf], body: &str, mjd: f64) -> MsResult<Self> {
        let table = find_ephemeris(roots, body, mjd)?;
        let count = table.row_count();
        if count == 0 {
            return Err(invalid("empty 2012 Solar System ephemeris"));
        }
        let time = table.column_accessor("MJD")?;
        let (mut lower, mut upper) = (0, count - 1);
        while lower < upper {
            let middle = lower + (upper - lower) / 2;
            if scalar(&time, middle)? < mjd {
                lower = middle + 1;
            } else {
                upper = middle;
            }
        }
        let prior = lower.saturating_sub(1);
        let nearest = if mjd < (scalar(&time, prior)? + scalar(&time, lower)?) * 0.5 {
            prior
        } else {
            lower
        };
        let rows = window(count, nearest);
        let times = rows
            .clone()
            .map(|row| scalar(&time, row))
            .collect::<MsResult<Vec<_>>>()?;
        // Interpolation windows are chosen from the original grid. Passing a
        // second window here keeps exactly those already selected rows.
        let sample = |name: &str| -> MsResult<f64> {
            let column = table.column_accessor(name)?;
            let values = rows
                .clone()
                .map(|row| scalar(&column, row))
                .collect::<MsResult<Vec<_>>>()?;
            Ok(interpolate::interpolate_window(&times, &values, mjd))
        };
        let optional_angle = |name: &str| -> MsResult<f64> {
            if !table
                .schema()
                .is_some_and(|schema| schema.column(name).is_some())
            {
                return Ok(0.);
            }
            let value = sample(name)?;
            Ok(if value == -999. { 0. } else { value })
        };
        let [radius_a, radius_b, radius_c] = radii_km(&table)?;
        let distance = sample("Rho")? * AU_METRES;
        let equatorial = 500. * (radius_a + radius_b);
        let polar = 1000. * radius_c;
        // The model applies sin/cos directly to the tabulated DiskLat.
        let (sin, cos) = optional_angle("DiskLat")?.sin_cos();
        let apparent_pole = (equatorial * sin).hypot(polar * cos);
        if distance <= 0. || equatorial <= 0. || apparent_pole <= 0. {
            return Err(invalid("invalid Solar System radius or range"));
        }
        Ok(Self {
            direction: MDirection::from_angles(
                sample("RA")?.to_radians(),
                sample("DEC")?.to_radians(),
                DirectionRef::J2000,
            ),
            major: 2. * equatorial / distance,
            minor: 2. * apparent_pole / distance,
            angle: optional_angle("NP_ang")?.to_radians(),
            radial_velocity: sample("RadVel")? * AU_METRES / 86_400.,
        })
    }
}

fn find_ephemeris(roots: &[PathBuf], body: &str, mjd: f64) -> MsResult<Table> {
    let prefix = format!("{body}_");
    for root in roots {
        let entries = std::fs::read_dir(root.join("ephemerides/JPL-Horizons"))
            .into_iter()
            .flatten()
            .flatten();
        for entry in entries {
            let name = entry.file_name();
            let name = name.to_string_lossy();
            if !name.starts_with(&prefix) || !name.ends_with("J2000.tab") {
                continue;
            }
            let table = Table::open(TableOptions::new(entry.path()))?;
            let Some(Value::Scalar(ScalarValue::String(name))) = table.keywords().get("NAME")
            else {
                continue;
            };
            if name.eq_ignore_ascii_case(body)
                && endpoint(&table, "earliest").is_some_and(|first| first < mjd)
                && endpoint(&table, "latest").is_some_and(|last| mjd < last)
            {
                return Ok(table);
            }
        }
    }
    Err(invalid(format!(
        "no JPL-Horizons 2012 ephemeris for {body} at MJD {mjd}"
    )))
}

fn endpoint(table: &Table, name: &str) -> Option<f64> {
    let Value::Record(epoch) = table.keywords().get(name)? else {
        return None;
    };
    let Value::Record(m0) = epoch.get("m0")? else {
        return None;
    };
    let Value::Scalar(ScalarValue::Float64(value)) = m0.get("value")? else {
        return None;
    };
    Some(*value)
}

fn radii_km(table: &Table) -> MsResult<[f64; 3]> {
    let Some(Value::Record(radii)) = table.keywords().get("radii") else {
        return Err(invalid("the 2012 ephemeris has no radii"));
    };
    let Some(Value::Array(ArrayValue::Float64(values))) = radii.get("value") else {
        return Err(invalid("invalid 2012 ephemeris radii"));
    };
    values
        .iter()
        .copied()
        .collect::<Vec<_>>()
        .try_into()
        .map_err(|_| invalid("expected three 2012 ephemeris radii"))
}

/// Returns edge pairs in hertz. A channel uses its center plus or minus half
/// its absolute width. A combined multi-channel window spans the extreme
/// channel centers.
pub fn bands(centers: &[f64], widths: &[f64], by_channel: bool) -> Vec<[f64; 2]> {
    if by_channel || centers.len() == 1 {
        centers
            .iter()
            .zip(widths)
            .map(|(&center, &width)| {
                let half = width.abs() * 0.5;
                [center - half, center + half]
            })
            .collect()
    } else {
        let minimum = centers.iter().copied().fold(f64::INFINITY, f64::min);
        let maximum = centers.iter().copied().fold(f64::NEG_INFINITY, f64::max);
        vec![[minimum, maximum]]
    }
}

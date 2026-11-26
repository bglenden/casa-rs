// SPDX-License-Identifier: LGPL-3.0-or-later
//! CASA component summaries, using casacore's value/error precision rules.
use super::{PyComponentList, supplied};
use crate::convert::ConvertError;
use crate::convert::value::CasaValue;
use crate::tools::LogLine;
use casa_components::SkyComponent;
use casa_types::quanta::casacore::stream_double;
use casa_types::quanta::format::{ANGLE, CLEAN, TIME, format_angle};
use num_complex::Complex64;
use pyo3::exceptions::{PyOverflowError, PySystemError, PyTypeError};
use pyo3::prelude::*;

#[pymethods]
impl PyComponentList {
    /// Log a component's flux, spectrum, direction, size and uncertainties.
    #[pyo3(signature=(which=None),text_signature="($self, which=-1)")]
    fn summarize(
        &self,
        py: Python<'_>,
        #[pyo3(from_py_with=supplied)] which: Option<Bound<'_, PyAny>>,
    ) -> PyResult<Py<PyAny>> {
        let (which, pending) = match which {
            None => (-1, false),
            Some(value) => match value.extract::<i64>() {
                Ok(index) => (i64::from(index as i32), false),
                Err(error) if value.is_none() || error.is_instance_of::<PyOverflowError>(py) => {
                    (-1, true)
                }
                Err(_) => return Err(PyTypeError::new_err("argument _which must be an integer")),
            },
        };
        let result=self.call(py,move|state,lines|{
            if which<0 || which>=state.length() {
                let n=state.length();
                let error=format!("List has only {n} components, but zero-based component {which} specified. Please specify a component less than {n}");
                lines.push(LogLine::error(format!("Exception Reported: {error}")));
                return Err(ConvertError::runtime(error));
            }
            lines.push(LogLine::info(summary(state.checked(which)?)));
            Ok(CasaValue::Bool(true))
        })?;
        if pending {
            return Err(PySystemError::new_err(
                "<built-in function componentlist_summarize> returned a result with an exception set",
            ));
        }
        Ok(result)
    }
}

fn vector(values: &[Complex64; 4]) -> String {
    format!(
        "[{}]",
        values
            .iter()
            .map(|v| format!("({},{})", stream_double(v.re), stream_double(v.im)))
            .collect::<Vec<_>>()
            .join(", ")
    )
}
fn summary(component: &SkyComponent) -> String {
    let flux = component.flux();
    let shape = component.shape();
    let limb = component
        .limb_darkening_exponent()
        .map_or(String::new(), |n| {
            format!(" (limb-darkening exponent: {} )\n", stream_double(n))
        });
    format!("SUMMARY OF COMPONENT {}\nShape: {}{limb}\nFlux density: {} {} +/- {}\nSpectral model: {}\nPosition: {}\nSize: \n{}\n",component.label(),shape.shape_type().casa_name(),vector(flux.values()),flux.unit().name(),vector(flux.errors()),component.spectrum().spectral_type().casa_name(),position(component),size(component)).trim_end().to_owned()
}
fn precision(first: [f64; 2], second: Option<[f64; 2]>) -> usize {
    let mut value = first[0].abs();
    let mut error = first[1].abs();
    if let Some([v, e]) = second {
        value = value.max(v.abs());
        error = if error == 0.0 || e == 0.0 {
            error.max(e.abs())
        } else {
            error.min(e.abs())
        };
    }
    if value < error {
        let old = value.max(0.1 * error);
        value = error;
        error = old;
    }
    if value == 0.0 {
        value = 1.0;
    }
    if error == 0.0 {
        error = 0.1 * value;
    }
    error = 0.1 * error.max(1e-8);
    if error.log10() < 0.0 {
        (error.log10().abs() + 1e-8) as usize + 1
    } else {
        0
    }
}
fn position(component: &SkyComponent) -> String {
    let shape = component.shape();
    let (lon, lat) = shape.reference_direction().as_angles();
    let errors = shape.direction_error();
    let dlon = errors.longitude().get_si_value() * 180.0 / std::f64::consts::PI * 240.0;
    let dlat = errors.latitude().get_si_value() * 180.0 / std::f64::consts::PI * 3600.0;
    let nonzero = dlon != 0.0 || dlat != 0.0;
    let p = if nonzero {
        let (x, y) = (round_double(dlon), round_double(dlat));
        precision([x, x], Some([y, y]))
    } else {
        1
    };
    let longitude = format_angle(lon, TIME, if nonzero { 6 + p as u32 } else { 9 });
    let latitude = format_angle(
        lat,
        if nonzero { ANGLE } else { ANGLE | CLEAN },
        if nonzero { 6 + p as u32 } else { 8 },
    );
    let longitude_error = if dlon == 0.0 {
        " (fixed)".into()
    } else {
        format!(" +/- {:.*} s ({:.*} arcsec)", p, dlon, p, dlon * 15.0)
    };
    let latitude_error = if dlat == 0.0 {
        " (fixed)".into()
    } else {
        if dlon == 0.0 {
            format!(" +/- {} arcsec", stream_double(dlat))
        } else {
            format!(" +/- {dlat:.p$} arcsec")
        }
    };
    format!(
        "Position ---\n       --- long:   {longitude}{longitude_error}\n       --- lat:  {latitude}{latitude_error}\n"
    )
}
fn size(component: &SkyComponent) -> String {
    let Some((axes, errors)) = component.shape().kind().axes() else {
        return "Point".into();
    };
    let radians = std::f64::consts::PI / 180.0;
    let units = [
        ("deg", radians),
        ("arcmin", radians / 60.0),
        ("arcsec", radians / 3600.0),
        ("marcsec", radians / 3_600_000.0),
        ("uarcsec", radians / 3_600_000_000.0),
    ];
    let (name, factor) = units
        .into_iter()
        .find(|(_, f)| axes.major_axis().abs().max(axes.minor_axis().abs()) / f > 1.0)
        .unwrap_or(units[4]);
    let major = axes.major_axis() / factor;
    let minor = axes.minor_axis() / factor;
    let demajor = errors.major_axis / factor;
    let deminor = errors.minor_axis / factor;
    let pa = (axes.position_angle() / radians)
        + if axes.position_angle() < 0.0 {
            180.0
        } else {
            0.0
        };
    let depa = errors.position_angle / radians;
    let p = precision([major, demajor], Some([minor, deminor]));
    let pp = precision([pa, depa], None);
    let uncertain = component.shape().shape_type() == casa_components::ShapeType::Gaussian;
    let axis = |label: &str, value: f64, error: f64| {
        if !uncertain {
            format!("       --- {label} axis FWHM:     {value:.p$} {name}\n")
        } else if error == 0.0 {
            format!("       --- {label} axis FWHM:     {value:.p$} {name} (fixed)\n")
        } else {
            format!("       --- {label} axis FWHM:     {value:.p$} +/- {error:.p$} {name}\n")
        }
    };
    let pa = if !uncertain {
        format!("       --- position angle: {pa:.pp$} deg\n")
    } else if depa == 0.0 {
        format!("       --- position angle: {pa:.pp$}deg (fixed)\n")
    } else {
        format!("       --- position angle: {pa:.pp$} +/- {depa:.pp$} deg\n")
    };
    format!(
        "{}{}{pa}",
        axis("major", major, demajor),
        axis("minor", minor, deminor)
    )
}

// casacore Math::roundDouble's default 2.5 significant digits.
fn round_double(value: f64) -> f64 {
    if value == 0.0 {
        return value;
    }
    let logarithm = value.abs().log10() - 2.5;
    let power = if logarithm >= 0.0 {
        logarithm + 1.000_001
    } else {
        logarithm - 0.000_001
    } as i32;
    let scale = 10f64.powi(power);
    value.signum() * (value.abs() / scale).round() * scale
}

// SPDX-License-Identifier: LGPL-3.0-or-later

use casa_tables::Table;
use num_complex::Complex32;

use casa_ms::{MsError, MsResult};

#[derive(Clone, Copy, Eq, PartialEq)]
pub(super) enum Weighting {
    Unweighted,
    InverseSigmaSquared,
    VisibilityWeight,
}

#[derive(Clone, Copy)]
pub(super) enum ComplexComponent {
    Amplitude,
    Phase,
    Imaginary,
    Real,
}

impl ComplexComponent {
    pub(super) fn project(self, value: Complex32) -> f32 {
        match self {
            Self::Amplitude => value.norm(),
            Self::Phase => value.arg(),
            Self::Imaginary => value.im,
            Self::Real => value.re,
        }
    }
}

#[derive(Clone, Copy)]
pub(super) enum Column {
    Complex(&'static str, ComplexComponent),
    FloatCube(&'static str),
    Flag,
    FlagRow,
    Scalar(&'static str),
    UvDistance,
    RowVector(&'static str),
}

impl Column {
    pub(super) fn parse(
        name: &str,
        component: &str,
        legacy: bool,
    ) -> MsResult<(String, Option<Self>)> {
        let upper = name.to_ascii_uppercase();
        let label = upper.split("_DATA").next().unwrap_or(&upper);
        let column = match label {
            "DATA" | "CORRECTED" | "MODEL" => {
                let stored = match label {
                    "DATA" => "DATA",
                    "CORRECTED" => "CORRECTED_DATA",
                    _ => "MODEL_DATA",
                };
                let component = match component {
                    "amp" | "amplitude" => ComplexComponent::Amplitude,
                    "phase" => ComplexComponent::Phase,
                    "imag" | "imaginary" => ComplexComponent::Imaginary,
                    "real" => ComplexComponent::Real,
                    _ if !legacy => return Ok((label.to_string(), None)),
                    _ => {
                        return Err(MsError::InvalidInput(format!(
                            "complex_value must be amp, amplitude, phase, imag, imaginary or real, is {component}"
                        )));
                    }
                };
                Self::Complex(stored, component)
            }
            "FLOAT" if !legacy => Self::FloatCube("FLOAT_DATA"),
            "WEIGHT_SPECTRUM" => Self::FloatCube("WEIGHT_SPECTRUM"),
            "UVW" if legacy => Self::RowVector("UVW"),
            "WEIGHT" if legacy => Self::RowVector("WEIGHT"),
            "SIGMA" if legacy => Self::RowVector("SIGMA"),
            "UVRANGE" => Self::UvDistance,
            "FLAG" => Self::Flag,
            "FLAG_ROW" => Self::FlagRow,
            "ANTENNA1" => Self::Scalar("ANTENNA1"),
            "ANTENNA2" => Self::Scalar("ANTENNA2"),
            "FEED1" => Self::Scalar("FEED1"),
            "FEED2" => Self::Scalar("FEED2"),
            "FIELD_ID" => Self::Scalar("FIELD_ID"),
            "ARRAY_ID" => Self::Scalar("ARRAY_ID"),
            "DATA_DESC_ID" => Self::Scalar("DATA_DESC_ID"),
            "INTERVAL" => Self::Scalar("INTERVAL"),
            "TIME" => Self::Scalar("TIME"),
            "SCAN" | "SCAN_NUMBER" => Self::Scalar("SCAN_NUMBER"),
            _ => {
                return Err(MsError::InvalidInput(format!(
                    "Unsupported column name: {name}"
                )));
            }
        };
        Ok((label.to_string(), Some(column)))
    }

    pub(super) fn source_for_view(self, table: &Table, legacy: bool) -> MsResult<Self> {
        let Column::Complex(name, component) = self else {
            return Ok(self);
        };
        let present = |name| {
            table
                .schema()
                .is_some_and(|schema| schema.column(name).is_some())
        };
        if !legacy && name == "DATA" && present("FLOAT_DATA") {
            return Ok(Self::Complex("FLOAT_DATA", component));
        }
        if legacy && !present(name) {
            let label = match name {
                "DATA" => "Data",
                "CORRECTED_DATA" => "Corrected Data",
                "MODEL_DATA" => "Model Data",
                _ => name,
            };
            return Err(MsError::InvalidInput(format!(
                "{label} column is not present"
            )));
        }
        Ok(self)
    }

    pub(super) fn weighting(self, enabled: bool) -> Weighting {
        if !enabled {
            return Weighting::Unweighted;
        }
        match self {
            Self::Complex("DATA" | "FLOAT_DATA", _) | Self::FloatCube("FLOAT_DATA") => {
                Weighting::InverseSigmaSquared
            }
            Self::Complex(_, _) => Weighting::VisibilityWeight,
            _ => Weighting::Unweighted,
        }
    }

    pub(super) fn is_boolean(self) -> bool {
        matches!(self, Self::Flag | Self::FlagRow)
    }
}

#[derive(Clone, Copy, Eq, PartialEq)]
pub(super) enum Axis {
    Array,
    Field,
    DataDescription,
    Integration,
    Scan,
    Subscan,
}

impl Axis {
    pub(super) fn parse_axes(text: &str) -> Vec<Self> {
        text.split(',')
            .filter_map(|axis| match axis {
                "array" => Some(Self::Array),
                "field" => Some(Self::Field),
                "ddid" => Some(Self::DataDescription),
                "integration" => Some(Self::Integration),
                "scan" => Some(Self::Scan),
                "subscan" => Some(Self::Subscan),
                _ => None,
            })
            .collect()
    }

    pub(super) fn column(self) -> &'static str {
        match self {
            Self::Array => "ARRAY_ID",
            Self::Field => "FIELD_ID",
            Self::DataDescription => "DATA_DESC_ID",
            Self::Integration => "TIME",
            Self::Scan => "SCAN_NUMBER",
            Self::Subscan => "STATE_ID",
        }
    }
}

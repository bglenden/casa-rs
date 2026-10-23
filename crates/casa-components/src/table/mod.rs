// SPDX-License-Identifier: LGPL-3.0-or-later
//! The CASA component-list table: its description, and reading and writing
//! it as `ComponentList::readTable`, `createTable` and `writeTable` do.
//!
//! See the [crate documentation](crate#the-table-format) for the format.

mod measures;
mod read;
mod spectral_record;
mod write;

use casa_tables::{ColumnOptions, ColumnSchema, TableDescHeader, TableSchema};
use casa_types::PrimitiveType;

pub(crate) use read::{read_bytes, read_component_list};
pub(crate) use write::write_component_list;

/// The table description's name, version and comment (`createTable`).
pub(crate) const DESCRIPTION_NAME: &str = "ComponentListDescription";
pub(crate) const DESCRIPTION_VERSION: &str = "4";
pub(crate) const DESCRIPTION_COMMENT: &str = "A description of a component list";

/// `TableInfo::type(TableInfo::COMPONENTLIST)`, with no subtype.
pub(crate) const TABLE_INFO_TYPE: &str = "Component List";
/// The readme lines `createTable` adds.
pub(crate) const TABLE_INFO_README: [&str; 2] = [
    "This is a ComponentList Table containing parameterised representations",
    "of the sky brightness.",
];

pub(crate) const FLUX: &str = "Flux";
pub(crate) const FLUX_UNIT: &str = "Flux_Unit";
pub(crate) const FLUX_POLARISATION: &str = "Flux_Polarisation";
pub(crate) const FLUX_ERROR: &str = "Flux_Error";
pub(crate) const SHAPE: &str = "Shape";
pub(crate) const REFERENCE_DIRECTION: &str = "Reference_Direction";
pub(crate) const DIRECTION_FRAME: &str = "Direction_Frame";
pub(crate) const DIRECTION_ERROR: &str = "Direction_Error";
pub(crate) const DIRECTION_ERROR_UNITS: &str = "Direction_Error_Units";
pub(crate) const SHAPE_PARAMETERS: &str = "Shape_Parameters";
pub(crate) const SHAPE_ERROR: &str = "Shape_Error";
pub(crate) const SPECTRUM_SHAPE: &str = "Spectrum_Shape";
pub(crate) const REFERENCE_FREQUENCY: &str = "Reference_Frequency";
pub(crate) const FREQUENCY_FRAME: &str = "Frequency_Frame";
pub(crate) const FREQUENCY_ERROR: &str = "Frequency_Error";
pub(crate) const FREQUENCY_ERROR_UNITS: &str = "Frequency_Error_Units";
pub(crate) const SPECTRAL_PARAMETERS: &str = "Spectral_Parameters";
pub(crate) const SPECTRAL_ERROR: &str = "Spectral_Error";
pub(crate) const LABEL: &str = "Label";
pub(crate) const OPTIONAL_PARAMETERS: &str = "Optional_Parameters";
pub(crate) const SPECTRAL_RECORD: &str = "Spectral_Record";

/// The tile shape of CASA's `TiledCellStMan` for `Optional_Parameters`.
pub(crate) const OPTIONAL_PARAMETERS_TILE: [usize; 1] = [1];

/// The columns `createTable` describes, in its order, then the
/// `Spectral_Record` column `writeTable` adds. `Optional_Parameters` is
/// described only for a list holding a limb-darkened disk, as CASA describes
/// it only when such a component was added.
pub(crate) fn component_list_schema(optional_parameters: bool) -> TableSchema {
    let direct = ColumnOptions {
        direct: true,
        undefined: false,
    };
    let direct_fixed = |name: &str, data_type, length: usize, comment: &str| {
        ColumnSchema::array_fixed(name, data_type, vec![length])
            .with_comment(comment)
            .with_options(direct)
            .expect("Direct fixed-shape arrays are valid")
    };
    let direct_scalar = |name: &str, data_type, comment: &str| {
        ColumnSchema::scalar(name, data_type)
            .with_comment(comment)
            .with_options(direct)
            .expect("Direct scalars are valid")
    };
    let vector = |name: &str, comment: &str| {
        ColumnSchema::array_variable(name, PrimitiveType::Float64, Some(1)).with_comment(comment)
    };
    let mut columns = vec![
        direct_fixed(FLUX, PrimitiveType::Complex64, 4, "Flux values"),
        direct_scalar(FLUX_UNIT, PrimitiveType::String, "Flux units"),
        direct_scalar(
            FLUX_POLARISATION,
            PrimitiveType::String,
            "Flux polarisation representation",
        ),
        direct_fixed(FLUX_ERROR, PrimitiveType::Complex64, 4, "Flux errors"),
        direct_scalar(SHAPE, PrimitiveType::String, "Shape of the Component"),
        direct_fixed(
            REFERENCE_DIRECTION,
            PrimitiveType::Float64,
            2,
            "Reference direction values",
        ),
        direct_scalar(
            DIRECTION_FRAME,
            PrimitiveType::String,
            "The reference direction frame",
        ),
        direct_fixed(
            DIRECTION_ERROR,
            PrimitiveType::Float64,
            2,
            "Error in the reference direction values",
        ),
        direct_fixed(
            DIRECTION_ERROR_UNITS,
            PrimitiveType::String,
            2,
            "Units of the direction error",
        ),
        vector(
            SHAPE_PARAMETERS,
            "Parameters specific to the component shape",
        ),
        vector(SHAPE_ERROR, "Error in the shape parameters"),
        direct_scalar(
            SPECTRUM_SHAPE,
            PrimitiveType::String,
            "Shape of the spectrum",
        ),
        direct_scalar(
            REFERENCE_FREQUENCY,
            PrimitiveType::Float64,
            "The reference frequency values",
        ),
        direct_scalar(
            FREQUENCY_FRAME,
            PrimitiveType::String,
            "The reference frequency frame",
        ),
        direct_scalar(
            FREQUENCY_ERROR,
            PrimitiveType::Float64,
            "Error in the reference frequency",
        ),
        direct_scalar(
            FREQUENCY_ERROR_UNITS,
            PrimitiveType::String,
            "Units of the frequency error",
        ),
        vector(
            SPECTRAL_PARAMETERS,
            "Parameters specific to the components spectrum",
        ),
        vector(SPECTRAL_ERROR, "Errors in the spectral parameters"),
        direct_scalar(
            LABEL,
            PrimitiveType::String,
            "An arbitrary label for the user",
        ),
    ];
    if optional_parameters {
        columns.push(
            vector(OPTIONAL_PARAMETERS, "optional parameter column")
                .with_options(ColumnOptions {
                    direct: false,
                    undefined: true,
                })
                .expect("Undefined variable arrays are valid"),
        );
    }
    columns.push(ColumnSchema::record(SPECTRAL_RECORD));
    TableSchema::new(columns)
        .expect("the component-list columns are distinct")
        .with_header(TableDescHeader {
            name: DESCRIPTION_NAME.to_string(),
            version: DESCRIPTION_VERSION.to_string(),
            comment: DESCRIPTION_COMMENT.to_string(),
        })
}

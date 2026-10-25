// SPDX-License-Identifier: LGPL-3.0-or-later
//! CASA component lists: parameterised models of the sky brightness, and the
//! casacore tables CASA keeps them in.
//!
//! A component list is an ordered list of sky components. Each component has
//! a flux, a shape that places it on the sky and spreads it, a spectral model
//! that scales it with frequency, a label, and optional parameters. This crate
//! models them as CASA's `components` module (`ComponentModels/` in casatools)
//! does, with Rust enums in place of its class hierarchies:
//!
//! | casa-rs | CASA |
//! |---|---|
//! | [`ComponentList`] | `ComponentList` |
//! | [`SkyComponent`] | `SkyComponent` / `SkyCompRep` |
//! | [`Flux`] with [`Polarisation`] | `Flux<Double>` / `FluxRep` with `ComponentType::Polarisation` |
//! | [`ComponentShape`] with [`ShapeKind`] | `ComponentShape`: `PointShape`, `GaussianShape`, `DiskShape`, `LimbDarkenedDiskShape` |
//! | [`SpectralModel`] with [`SpectralKind`] | `SpectralModel`: `ConstantSpectrum`, `SpectralIndex`, `TabularSpectrum`, `PowerLogPoly` |
//! | [`ShapeType`], [`SpectralType`] | `ComponentType::Shape`, `ComponentType::SpectralShape` |
//!
//! Directions and frequencies are casa-types measures
//! ([`MDirection`](casa_types::measures::direction::MDirection),
//! [`MFrequency`](casa_types::measures::frequency::MFrequency)) carrying their
//! frames, and errors that carry units are casa-types
//! [`Quantity`](casa_types::quanta::Quantity) values.
//!
//! [`ComponentList::read_table`] reads a table as CASA's
//! `ComponentList::readTable` does, and [`ComponentList::write_table`] writes
//! one as its `createTable` and `writeTable` do, so CASA reads it back
//! unchanged.
//!
//! # The table format
//!
//! A component list is a casacore table with one row per component. Its
//! table description is named `ComponentListDescription`, version `4`, with
//! the comment "A description of a component list"; its table info has type
//! `Component List`, no subtype, and two readme lines. Every column is in one
//! `StandardStMan` except `Optional_Parameters`.
//!
//! | column | type | options | holds |
//! |---|---|---|---|
//! | `Flux` | `DComplex[4]` | Direct | [`Flux::values`] |
//! | `Flux_Unit` | `String` | Direct | the flux unit's name |
//! | `Flux_Polarisation` | `String` | Direct | `Stokes`, `Linear` or `Circular` |
//! | `Flux_Error` | `DComplex[4]` | Direct | [`Flux::errors`] |
//! | `Shape` | `String` | Direct | [`ShapeType::casa_name`] |
//! | `Reference_Direction` | `Double[2]` | Direct | longitude and latitude, rad |
//! | `Direction_Frame` | `String` | Direct | the direction's frame |
//! | `Direction_Error` | `Double[2]` | Direct | latitude and longitude errors |
//! | `Direction_Error_Units` | `String[2]` | Direct | their units |
//! | `Shape_Parameters` | `Double` vector | | [`ComponentShape::parameters`] |
//! | `Shape_Error` | `Double` vector | | [`ComponentShape::errors`] |
//! | `Spectrum_Shape` | `String` | Direct | [`SpectralType::casa_name`] |
//! | `Reference_Frequency` | `Double` | Direct | the reference frequency, Hz |
//! | `Frequency_Frame` | `String` | Direct | its frame |
//! | `Frequency_Error` | `Double` | Direct | its error |
//! | `Frequency_Error_Units` | `String` | Direct | the error's unit |
//! | `Spectral_Parameters` | `Double` vector | | [`SpectralModel::parameters`] |
//! | `Spectral_Error` | `Double` vector | | [`SpectralModel::errors`] |
//! | `Label` | `String` | Direct | [`SkyComponent::label`] |
//! | `Optional_Parameters` | `Double` vector | Undefined | [`SkyComponent::optional_parameters`], limb-darkened disks only, in a `TiledCellStMan` with one-element tiles |
//! | `Spectral_Record` | record | | what CASA's spectral `toRecord` writes |
//!
//! (A Direct column also carries casacore's FixedShape option.) The keywords
//! are casacore's measure and quantum descriptions:
//!
//! - `Reference_Direction`: `QuantumUnits = ["rad", "rad"]` and
//!   `MEASINFO = {type: "direction", VarRefCol: "Direction_Frame"}`;
//! - `Reference_Frequency`: `QuantumUnits = ["Hz"]` and
//!   `MEASINFO = {type: "frequency", VarRefCol: "Frequency_Frame"}`;
//! - `Direction_Error`: `VariableUnits = "Direction_Error_Units"`;
//! - `Frequency_Error`: `VariableUnits = "Frequency_Error_Units"`.
//!
//! `Optional_Parameters` exists only in lists holding a limb-darkened disk.
//! Lists written before CASA added them lack the error columns and
//! `Spectral_Record`; such lists read with zero errors and with each spectral
//! model's defaults for what only the record holds.
//!
//! # Visibilities
//!
//! [`SkyComponent::visibility`] forms a component's visibility as CASA's
//! `SkyCompRep::visibility` does: the Stokes flux sampled by the spectral
//! model ([`SpectralModel::sample_stokes`]), converted to the data's
//! polarisation basis ([`stokes_in`]), times the shape's visibility
//! ([`ComponentShape::visibility_scale`]). A limb-darkened disk's visibility
//! and some tabular spectra are refused (the `visibility` module lists them).
//! A predictor evaluating many baselines and channels prepares each shape's
//! [`ShapeTransform`] once, takes it at each baseline
//! ([`ShapeTransform::at_baseline`]) and leaves each channel a function of
//! frequency alone ([`BaselineTransform::at_frequency`]).
//!
//! # Records
//!
//! [`ComponentList::to_record`] and [`SkyComponent::to_record`] form the
//! records CASA's `toRecord` does, which the `cl` tool's `torecord` and
//! `getcomponent` return; the flux, shape and spectrum each have their own
//! (`Flux::to_record`, `ComponentShape::to_record`,
//! `SpectralModel::to_record`).
//!
//! # Deliberately not here
//!
//! - **Sampling components on an image.**
//! - **The `cl` tool** itself: its argument conventions, its selection state
//!   and when it writes its table belong to `casars.tools.componentlist`.
//!   Reading records back (`fromrecord`) is not here either.
//! - **Display units:** CASA remembers the units each shape axis was given in;
//!   tables do not store them, and casa-rs keeps radians. Records give a
//!   shape's axes in the units CASA gives one read from a table.
//! - **Table keywords:** a list's table keywords (`cl.putkeyword`) are not
//!   read or written.

mod component;
mod component_type;
mod error;
mod flux;
mod list;
mod record;
mod shape;
mod spectrum;
mod table;
mod visibility;

pub use component::{DEFAULT_LIMB_DARKENING_EXPONENT, SkyComponent};
pub use component_type::{Polarisation, ShapeType, SpectralType};
pub use error::{ComponentError, ComponentListError};
pub use flux::Flux;
pub use list::ComponentList;
pub use shape::{ComponentShape, DirectionError, Ellipse, EllipseErrors, ShapeKind};
pub use spectrum::{
    PowerLogPolynomial, SpectralIndex, SpectralKind, SpectralModel, TabularSpectrum,
};
pub use visibility::{BaselineTransform, ShapeTransform, stokes_in};

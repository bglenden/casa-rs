// SPDX-License-Identifier: LGPL-3.0-or-later
//! Field tracking and antenna-frame directions for single-dish maps.

use std::collections::{BTreeMap, btree_map::Entry};

use casa_core::measures::direction::DirectionRef;
use casa_ms::derived::engine::MsCalEngine;
use casa_ms::{MeasurementSet, PlanetaryEphemeris};

pub(super) struct MapTargets {
    field_sources: Vec<i32>,
    source_names: BTreeMap<i32, String>,
    body_tables: BTreeMap<String, PlanetaryEphemeris>,
}

pub(super) enum Target<'a> {
    Fixed,
    Comet,
    Planet(&'a PlanetaryEphemeris),
}

pub(super) enum SourceRow {
    Required,
    Optional,
}

impl MapTargets {
    pub(super) fn read(ms: &MeasurementSet) -> Result<Self, String> {
        let fields = ms.field().map_err(error_text)?;
        let source = ms.source().map_err(error_text)?;
        let field_sources = (0..fields.row_count())
            .map(|row| fields.source_id(row))
            .collect::<Result<Vec<_>, _>>()
            .map_err(error_text)?;
        let mut source_names = BTreeMap::new();
        for row in 0..source.row_count() {
            let id = source.i32(row, "SOURCE_ID").map_err(error_text)?;
            if let Entry::Vacant(entry) = source_names.entry(id) {
                let name = source.string(row, "NAME").map_err(error_text)?;
                entry.insert(name.to_ascii_uppercase());
            }
        }
        Ok(Self {
            field_sources,
            source_names,
            body_tables: BTreeMap::new(),
        })
    }

    pub(super) fn field(&mut self, id: usize, source_row: SourceRow) -> Result<Target<'_>, String> {
        let source = self.field_sources.get(id).ok_or("Invalid map FIELD_ID.")?;
        let Some(name) = self.source_names.get(source) else {
            return match source_row {
                SourceRow::Required => Err("No SOURCE row for selected map field.".into()),
                SourceRow::Optional => Ok(Target::Fixed),
            };
        };
        if name == "COMET" {
            return Ok(Target::Comet);
        }
        if !matches!(
            name.as_str(),
            "SUN"
                | "MOON"
                | "MERCURY"
                | "VENUS"
                | "MARS"
                | "JUPITER"
                | "SATURN"
                | "URANUS"
                | "NEPTUNE"
                | "PLUTO"
        ) {
            return Ok(Target::Fixed);
        }
        let table = match self.body_tables.entry(name.clone()) {
            Entry::Occupied(entry) => entry.into_mut(),
            Entry::Vacant(entry) => {
                entry.insert(PlanetaryEphemeris::open_discovered(name).map_err(error_text)?)
            }
        };
        Ok(Target::Planet(table))
    }
}

impl Target<'_> {
    pub(super) fn direction(
        &self,
        engine: &MsCalEngine,
        time: f64,
        antenna: usize,
        field: usize,
    ) -> Result<Option<(f64, f64)>, String> {
        match self {
            Self::Fixed => Ok(None),
            Self::Comet => engine
                .tracked_body_direction_j2000(time, field)
                .map(|direction| Some(direction.as_angles()))
                .map_err(error_text),
            Self::Planet(body) => {
                let frame = engine.antenna_frame(time, antenna).map_err(error_text)?;
                let body = body
                    .direction(&frame)
                    .map_err(error_text)?
                    .convert_to(DirectionRef::J2000, &frame)
                    .map_err(error_text)?;
                Ok(Some(body.as_angles()))
            }
        }
    }
}

fn error_text(error: impl std::fmt::Display) -> String {
    error.to_string()
}

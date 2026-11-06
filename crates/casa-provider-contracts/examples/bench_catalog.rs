// SPDX-License-Identifier: LGPL-3.0-or-later
//! Measure catalog initialization, parsing, validation and repeated access.
//!
//! Run on an otherwise idle host with
//! `cargo run --release -p casa-provider-contracts --example bench_catalog`.
//! Each run starts a fresh process; later lookups share its `OnceLock`.

use std::hint::black_box;
use std::time::Instant;

use casa_provider_contracts::{
    ParameterCatalog, SurfaceCatalogBundle, SurfaceDefinition, builtin_surface_catalog,
};
use serde::Deserialize;

const CATALOG_JSON: &str = include_str!("../resources/parameter-catalog.json");
const SURFACES_JSON: &str = include_str!("../resources/parameter-surfaces.json");

#[derive(Deserialize)]
struct SurfaceIndex {
    schema_version: u32,
    surfaces: Vec<SurfaceDefinition>,
}

fn median_ms(values: &mut [f64]) -> f64 {
    values.sort_by(f64::total_cmp);
    (values[9] + values[10]) / 2.0
}

fn main() {
    let start = Instant::now();
    black_box(builtin_surface_catalog().expect("valid built-in catalog"));
    let initial = start.elapsed();

    const LOOKUPS: u32 = 1_000_000;
    let start = Instant::now();
    for _ in 0..LOOKUPS {
        black_box(builtin_surface_catalog().expect("valid built-in catalog"));
    }
    let lookup_ns = start.elapsed().as_nanos() as f64 / f64::from(LOOKUPS);

    let mut concepts_ms = Vec::new();
    let mut surfaces_ms = Vec::new();
    let mut validation_ms = Vec::new();
    for _ in 0..20 {
        let start = Instant::now();
        let catalog: ParameterCatalog = serde_json::from_str(black_box(CATALOG_JSON)).unwrap();
        concepts_ms.push(start.elapsed().as_secs_f64() * 1000.0);
        let start = Instant::now();
        let index: SurfaceIndex = serde_json::from_str(black_box(SURFACES_JSON)).unwrap();
        surfaces_ms.push(start.elapsed().as_secs_f64() * 1000.0);
        let bundle = SurfaceCatalogBundle {
            schema_version: index.schema_version,
            catalog,
            surfaces: index.surfaces,
        };
        let start = Instant::now();
        black_box(&bundle)
            .validate()
            .expect("valid built-in catalog");
        validation_ms.push(start.elapsed().as_secs_f64() * 1000.0);
    }
    println!(
        "initial_catalog_ms={:.3} cached_lookup_ns={lookup_ns:.1} concepts_parse_ms={:.3} surfaces_parse_ms={:.3} validation_ms={:.3}",
        initial.as_secs_f64() * 1000.0,
        median_ms(&mut concepts_ms),
        median_ms(&mut surfaces_ms),
        median_ms(&mut validation_ms),
    );
}

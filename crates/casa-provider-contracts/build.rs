// SPDX-License-Identifier: LGPL-3.0-or-later
//! Validate the bundled task catalog and prepare self-contained surfaces.
//!
//! Reuse the library's definitions and validator in the build script. When
//! host and target parsing cannot be proven equivalent, retain target-side
//! parsing and validation instead.

#![allow(dead_code)]

#[path = "src/casa_value.rs"]
mod casa_value;
#[path = "src/parameters.rs"]
mod parameters;
pub use casa_value::*;
pub use parameters::*;
#[path = "src/build_catalog.rs"]
mod build_catalog;
#[path = "src/catalog_number_probe.rs"]
mod catalog_number_probe;
#[path = "src/catalog_parse.rs"]
mod catalog_parse;
#[path = "src/surface_codegen.rs"]
mod surface_codegen;

use std::fmt::Write as _;
use std::path::PathBuf;

fn main() {
    println!("cargo:rustc-check-cfg=cfg(casa_generated_surfaces)");
    println!("cargo:rerun-if-changed=resources/parameter-catalog.json");
    println!("cargo:rerun-if-changed=resources/parameter-surfaces.json");
    println!("cargo:rerun-if-changed=src");
    let target_width: u32 = std::env::var("CARGO_CFG_TARGET_POINTER_WIDTH")
        .expect("target pointer width")
        .parse()
        .expect("numeric target pointer width");
    if target_width != usize::BITS {
        // A host cannot validate target-sized fields at a different width.
        // Keep the original target-side parser and validator for that build.
        return;
    }
    let root = PathBuf::from(std::env::var_os("CARGO_MANIFEST_DIR").unwrap());
    let catalog_path = root.join("resources/parameter-catalog.json");
    let surfaces_path = root.join("resources/parameter-surfaces.json");
    let catalog = std::fs::read_to_string(&catalog_path).expect("read parameter catalog");
    let surfaces = std::fs::read_to_string(&surfaces_path).expect("read parameter surfaces");
    let sources = surface_codegen::SurfaceSources::parse(&catalog, &surfaces)
        .unwrap_or_else(|error| panic!("read raw task catalog: {error}"));
    let aggregate = match catalog_parse::parse(&catalog, &surfaces) {
        Ok(aggregate) => aggregate,
        Err(_) => return, // Let the target parser report target-dependent errors.
    };
    if let Err(errors) = aggregate.validate() {
        if surface_codegen::definitely_invalid(&errors) {
            panic!(
                "invalid bundled task catalog: {}",
                build_catalog::format_errors(errors)
            );
        }
        // A numeric comparison can differ under the target's JSON features.
        // Preserve the original target-side validation for that case.
        return;
    }
    let mut probes = Vec::new();
    for token in catalog_number_probe::number_tokens(&[&catalog, &surfaces]) {
        let Some((direct, value, casa)) = catalog_number_probe::number_bits(&token) else {
            return; // Retain target-side validation for unrepresentable probes.
        };
        probes.push((token, direct, value, casa));
    }
    let out = PathBuf::from(std::env::var_os("OUT_DIR").unwrap());
    let mut source = format!(
        "static GENERATED_SURFACES: [GeneratedSurface; {}] = [\n",
        aggregate.surfaces.len()
    );
    for (index, surface) in aggregate.surfaces.iter().enumerate() {
        let id = surface.id();
        let Ok(json) = sources.encode_surface(&aggregate, index) else {
            // A generated wrapper may exceed the parser's nesting limit even
            // when the original aggregate is valid; preserve the old path.
            return;
        };
        let name = format!("surface-{index}.json");
        std::fs::write(out.join(&name), json).expect("write generated surface");
        writeln!(
            source,
            "GeneratedSurface {{ id: {id:?}, aliases: &{:?}, json: include_str!(concat!(env!(\"OUT_DIR\"), \"/{name}\")), parsed: OnceLock::new() }},",
            surface.former_ids()
        )
        .expect("write generated source");
    }
    source.push_str("];\n");
    source.push_str("static GENERATED_NUMBER_PROBES: &[(&str, u64, u64, u64)] = &[\n");
    for (token, direct, value, casa) in probes {
        writeln!(source, "({token:?}, {direct}, {value}, {casa}),").expect("write number probe");
    }
    source.push_str("];\n");
    std::fs::write(out.join("embedded_surfaces.rs"), source).expect("write generated source");
    println!("cargo:rustc-cfg=casa_generated_surfaces");
}

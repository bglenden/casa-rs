// SPDX-License-Identifier: LGPL-3.0-or-later
//! Build and validate the immutable image-analysis provider contract.

#![allow(dead_code)]

#[path = "src/analysis/schema.rs"]
mod schema;
#[path = "src/analysis/task_contract.rs"]
mod task_contract;

use std::fmt::Write as _;
use std::path::PathBuf;

fn main() {
    println!("cargo:rustc-check-cfg=cfg(casa_prebuilt_image_provider)");
    println!("cargo:rerun-if-changed=src/analysis/schema.rs");
    println!("cargo:rerun-if-changed=src/analysis/task_contract.rs");
    let target_width: u32 = std::env::var("CARGO_CFG_TARGET_POINTER_WIDTH")
        .expect("target pointer width")
        .parse()
        .expect("numeric target pointer width");
    if target_width != usize::BITS {
        return;
    }
    let Ok(mut contract) = task_contract::try_image_analysis_task_schema_bundle() else {
        return;
    };
    if let Err(errors) = contract.validate() {
        if errors.iter().any(|error| error.code != "parameter_surface") {
            panic!("invalid built-in image provider: {errors:?}");
        }
        // A host-dependent numeric comparison needs target-side validation.
        return;
    }
    contract
        .semantic
        .front_end
        .get_or_insert_with(Default::default);
    let Ok(value) = serde_json::to_value(&contract) else {
        return;
    };
    let Ok(document) = serde_json::to_string(&value) else {
        return;
    };
    if !casa_provider_contracts::provider_document_roundtrips(&document) {
        // A parser configuration that cannot round-trip this document must
        // not silently change a schema number on the target.
        return;
    }
    let Some(probes) = casa_provider_contracts::builtin_provider_fingerprint(&document) else {
        return;
    };
    let out = PathBuf::from(std::env::var_os("OUT_DIR").expect("build output directory"));
    std::fs::write(out.join("image-provider.json"), document).expect("write provider contract");
    let mut source = String::from("static IMAGE_PROVIDER_PROBES: &[(&str, u64, u64, u64)] = &[\n");
    for (token, direct, value, casa) in probes {
        writeln!(source, "({token:?}, {direct}, {value}, {casa}),").unwrap();
    }
    source.push_str("];\n");
    writeln!(
        source,
        "const IMAGE_PROVIDER_SCHEMA_ORDER: bool = {};",
        schema::schema_maps_preserve_order()
    )
    .unwrap();
    std::fs::write(out.join("image-provider-probes.rs"), source).expect("write parser probes");
    println!("cargo:rustc-cfg=casa_prebuilt_image_provider");
}

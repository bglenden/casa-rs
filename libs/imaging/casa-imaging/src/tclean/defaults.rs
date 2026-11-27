// SPDX-License-Identifier: LGPL-3.0-or-later

//! Domain defaults for `tclean`-shaped [`crate::ImagingRequest`] construction.

use std::str::FromStr;

use casa_core::quanta::{Quantity, Unit};
use serde_json::{Map, Value, json};

/// Return the unconditional canonical CASA 6.7.6 `tclean` parameter defaults as plain JSON.
#[must_use]
pub fn default_tclean_parameters() -> Map<String, Value> {
    let entries: [(&str, Value); 105] = [
        ("vis", json!("")),
        ("selectdata", json!(true)),
        ("field", json!([""])),
        ("spw", json!(["none"])),
        ("timerange", json!([""])),
        ("uvrange", json!(["none"])),
        ("antenna", json!([""])),
        ("scan", json!([""])),
        ("observation", json!("")),
        ("intent", json!(["none"])),
        ("datacolumn", json!("corrected")),
        ("imagename", json!("")),
        ("imsize", json!([100])),
        ("cell", json!([])),
        ("phasecenter", json!("")),
        ("stokes", json!("I")),
        ("projection", json!("SIN")),
        ("startmodel", json!([""])),
        ("specmode", json!("mfs")),
        ("reffreq", json!("")),
        ("nchan", json!(-1)),
        ("start", json!("")),
        ("width", json!("")),
        ("outframe", json!("LSRK")),
        ("veltype", json!("radio")),
        ("restfreq", json!([])),
        ("interpolation", json!("linear")),
        ("perchanweightdensity", json!(true)),
        ("gridder", json!("standard")),
        ("facets", json!(1)),
        ("psfphasecenter", json!("")),
        ("wprojplanes", json!(1)),
        ("vptable", json!("")),
        ("mosweight", json!(true)),
        ("aterm", json!(true)),
        ("psterm", json!(false)),
        ("wbawp", json!(true)),
        ("conjbeams", json!(false)),
        ("cfcache", json!("")),
        ("usepointing", json!(false)),
        ("computepastep", json!(360.0)),
        ("rotatepastep", json!(360.0)),
        ("pointingoffsetsigdev", json!([])),
        ("pblimit", json!(0.2)),
        ("normtype", json!("flatnoise")),
        ("deconvolver", json!("hogbom")),
        ("scales", json!([])),
        ("nterms", json!(2)),
        ("smallscalebias", json!(0.0)),
        ("fusedthreshold", json!(0.0)),
        ("largestscale", json!(-1)),
        ("restoration", json!(true)),
        ("restoringbeam", json!([])),
        ("pbcor", json!(false)),
        ("outlierfile", json!("")),
        ("weighting", json!("natural")),
        ("robust", json!(0.5)),
        ("noise", json!("1.0Jy")),
        ("npixels", json!(0)),
        ("uvtaper", json!([""])),
        ("niter", json!(0)),
        ("gain", json!(0.1)),
        ("threshold", json!("0Jy")),
        ("nsigma", json!(0.0)),
        ("cycleniter", json!(-1)),
        ("cyclefactor", json!(1.0)),
        ("minpsffraction", json!(0.05)),
        ("maxpsffraction", json!(0.8)),
        ("interactive", json!(false)),
        ("nmajor", json!(-1)),
        ("fullsummary", json!(false)),
        ("usemask", json!("user")),
        ("mask", json!([""])),
        ("pbmask", json!(0.0)),
        ("sidelobethreshold", json!(3.0)),
        ("noisethreshold", json!(5.0)),
        ("lownoisethreshold", json!(1.5)),
        ("negativethreshold", json!(0.0)),
        ("smoothfactor", json!(1.0)),
        ("minbeamfrac", json!(0.3)),
        ("cutthreshold", json!(0.01)),
        ("growiterations", json!(75)),
        ("dogrowprune", json!(true)),
        ("minpercentchange", json!(-1.0)),
        ("verbose", json!(false)),
        ("fastnoise", json!(true)),
        ("restart", json!(true)),
        ("savemodel", json!("none")),
        ("calcres", json!(true)),
        ("calcpsf", json!(true)),
        ("psfcutoff", json!(0.35)),
        ("parallel", json!(false)),
        ("ddid", json!("none")),
        ("channel_start", json!("none")),
        ("channel_count", json!("none")),
        ("hogbom_iteration_mode", json!("casa-inclusive")),
        ("write_pb", json!(true)),
        ("fitspw", json!("none")),
        ("fitorder", json!(0)),
        ("save_continuum_residual", json!(false)),
        ("backend", json!("auto")),
        ("gridprecision", json!("auto")),
        ("stage_timing", json!(false)),
        ("metal_stage_timing", json!(false)),
        ("science_trace", json!(false)),
    ];
    entries
        .into_iter()
        .map(|(k, v)| (k.to_string(), v))
        .collect()
}

/// Merge sparse `tclean` overrides over [`default_tclean_parameters`],
/// applying conditional AW-CF defaults, canonicalizing `cell` (`arcsec`) and
/// `threshold` (`Jy`), and normalizing numeric `phasecenter`/`start`/`width`
/// scalars to strings.
pub(crate) fn merge_tclean_defaults(overrides: &Map<String, Value>) -> Map<String, Value> {
    let mut merged = default_tclean_parameters();
    let gridder = overrides
        .get("gridder")
        .or_else(|| merged.get("gridder"))
        .and_then(Value::as_str);
    if gridder == Some("awproject") {
        merged.insert("aw_cf_source".into(), json!("casa-import"));
        merged.insert("cf_resident_mb".into(), json!("auto"));
        let source = overrides
            .get("aw_cf_source")
            .and_then(Value::as_str)
            .unwrap_or("casa-import");
        if source == "native-evla" {
            for (k, v) in [
                ("native_cf_cache", "none"),
                ("evla_surface", "none"),
                ("native_cf_policy", "reuse-only"),
                ("native_cf_working_size", "none"),
                ("native_cf_oversampling", "none"),
                ("native_cf_cache_bytes", "none"),
                ("native_cf_maximum_cells", "none"),
            ] {
                merged.insert(k.into(), json!(v));
            }
        }
    }
    for (key, value) in overrides {
        merged.insert(key.clone(), value.clone());
    }
    for text_or_number in ["phasecenter", "start", "width"] {
        if let Some(value) = merged.get_mut(text_or_number)
            && !value.is_string()
            && !value.is_array()
            && !value.is_object()
            && !value.is_null()
        {
            *value = Value::String(value.to_string());
        }
    }
    if let Some(cell) = merged.get_mut("cell") {
        *cell = canonicalize_cell(cell);
    }
    if let Some(threshold) = merged.get_mut("threshold") {
        *threshold = canonicalize_threshold(threshold);
    }
    merged
}

fn canonicalize_cell(value: &Value) -> Value {
    let convert_one = |item: &Value| -> Value {
        if let Some(text) = item.as_str()
            && let Some(canonical) = convert_quantity(text, "arcsec")
        {
            return Value::String(canonical);
        }
        if let Some(number) = item.as_f64() {
            return Value::String(format!("{number}arcsec"));
        }
        item.clone()
    };
    match value {
        Value::Array(items) => Value::Array(items.iter().map(convert_one).collect()),
        other => convert_one(other),
    }
}

fn canonicalize_threshold(value: &Value) -> Value {
    if let Some(number) = value.as_f64() {
        return Value::String(format!("{number}Jy"));
    }
    if let Some(text) = value.as_str()
        && let Some(canonical) = convert_quantity(text, "Jy")
    {
        return Value::String(canonical);
    }
    value.clone()
}

fn convert_quantity(text: &str, target_unit: &str) -> Option<String> {
    let trimmed = text.trim();
    if trimmed.is_empty() {
        return None;
    }
    let quantity = Quantity::from_str(trimmed).ok()?;
    let target = Unit::from_str(target_unit).ok()?;
    if quantity.unit().name().is_empty() {
        return Some(format!("{}{target_unit}", quantity.value()));
    }
    let converted = quantity.get_value_in(&target).ok()?;
    if converted.is_finite() {
        Some(format!("{converted}{target_unit}"))
    } else {
        None
    }
}

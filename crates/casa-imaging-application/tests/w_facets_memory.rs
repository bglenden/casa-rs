// SPDX-License-Identifier: LGPL-3.0-or-later
//! Production-route accounting for each W-facet kernel set (#1063).

use std::sync::{Arc, Mutex};

use casa_imaging_application::ResourcePolicy;
use serde_json::json;
use tracing::{
    Event, Subscriber,
    field::{Field, Visit},
};
use tracing_subscriber::{Layer, layer::Context, prelude::*, registry::LookupSpan};

#[path = "common/imaging.rs"]
mod imaging;
#[path = "common/synthetic_observation.rs"]
mod synthetic_observation;

#[derive(Default)]
struct Bytes {
    resident: Option<u64>,
    reserved: Option<u64>,
}
impl Visit for Bytes {
    fn record_debug(&mut self, _: &Field, _: &dyn std::fmt::Debug) {}
    fn record_u64(&mut self, field: &Field, value: u64) {
        match field.name() {
            "resident_bytes" => self.resident = Some(value),
            "reserved_bytes" => self.reserved = Some(value),
            _ => {}
        }
    }
}
struct Counts(Arc<Mutex<Vec<Bytes>>>, Arc<Mutex<Vec<Bytes>>>);
impl<S: Subscriber + for<'a> LookupSpan<'a>> Layer<S> for Counts {
    fn on_event(&self, event: &Event<'_>, _: Context<'_, S>) {
        let target = match event.metadata().target() {
            "casa_imaging_application::w_kernel_charge" => &self.0,
            "casa_imaging_application::w_kernel_in_pass" => &self.1,
            _ => return,
        };
        let mut bytes = Bytes::default();
        event.record(&mut bytes);
        target.lock().unwrap().push(bytes);
    }
}

/// A two-by-two faceted run retains four charged sets through its pass.
#[test]
fn production_run_retains_one_w_kernel_charge_per_facet() {
    let root = tempfile::tempdir().unwrap();
    let vis = synthetic_observation::observation(root.path(), 4);
    let request = imaging::request(json!({"vis":vis, "imagename":root.path().join("image"),
        "imsize":256, "cell":"0.012arcsec", "gridder":"wproject", "wprojplanes":4,
        "facets":2, "weighting":"natural", "deconvolver":"hogbom", "niter":10}));
    let recorded = Arc::new(Mutex::new(Vec::new()));
    let in_pass = Arc::new(Mutex::new(Vec::new()));
    let subscriber = tracing_subscriber::registry().with(Counts(recorded.clone(), in_pass.clone()));
    let outcome = tracing::subscriber::with_default(subscriber, || {
        casa_imaging_application::execute(&request, imaging::context(ResourcePolicy::Exclusive))
            .expect("faceted W run")
    });
    assert!(outcome.major_cycle_count >= 2, "a model refresh executes");
    let charges = recorded.lock().unwrap();
    assert_eq!(charges.len(), 4, "one W set per facet");
    for bytes in charges.iter() {
        assert!(bytes.resident.is_some_and(|n| n > 0));
        assert_eq!(bytes.reserved, bytes.resident);
    }
    let live = in_pass.lock().unwrap();
    assert!(
        live.len() >= 8 && live.len() % 4 == 0,
        "four charges in each pass"
    );
    for (index, bytes) in live.iter().enumerate() {
        assert_eq!(bytes.reserved, charges[index % 4].resident);
    }
}

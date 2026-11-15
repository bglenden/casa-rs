// SPDX-License-Identifier: LGPL-3.0-or-later
//! Prepare the one radial lookup shared by minor-cycle masks and products.

use casa_imaging_products::SavedRadialPrimaryBeam;
use casa_imaging_runtime::{Admitted, Demand, admit};
use casa_ms::voltage_pattern::{VoltagePatternTable, radial_pattern};
use casa_numerics::BeamPattern;

use super::PrepareError;
use crate::{ImagingRequest, RunContext};

pub(super) fn prepare(
    request: &ImagingRequest,
    telescope: &str,
    context: &RunContext,
) -> Result<Option<Admitted<SavedRadialPrimaryBeam>>, PrepareError> {
    let Some(path) = request.vptable.as_deref() else {
        return Ok(None);
    };
    if !request.write_pb && !request.pbcor {
        return Ok(None);
    }
    let reserve = |phase, memory| admit(&context.host, &context.policy, &Demand { phase, memory });
    let _table_memory = reserve(
        "the voltage-pattern table",
        VoltagePatternTable::reading_bytes(path)?,
    )?;
    let table = VoltagePatternTable::open(path)?;
    let _defaults_memory = reserve(
        "the voltage-pattern defaults",
        table.defaults_reading_bytes()?,
    )?;
    let Some(record) = table.default_record(telescope)? else {
        return Ok(None);
    };
    let definition = radial_pattern(&record)?;
    let reservation = reserve("the saved radial primary beam", BeamPattern::BYTES)?;
    Ok(Some(Admitted {
        value: SavedRadialPrimaryBeam::new(definition),
        reservation,
    }))
}

#[cfg(test)]
mod tests {
    use std::path::Path;

    use crate::Cancel;
    use casa_imaging_runtime::{HostResources, ResourcePolicy};
    use serde_json::json;

    use super::*;

    #[test]
    fn the_supplied_voltage_samples_remain_admitted_after_table_reading() {
        let table = Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("../casa-simulation/tests/data/sm_squint/custom_gaussian.vp");
        let request = crate::request::tests::request(json!({ "vptable": table, "write_pb": true }));
        let context = RunContext {
            host: HostResources::detect().unwrap(),
            policy: ResourcePolicy::Explicit {
                workers: 1,
                memory: 2 << 30,
            },
            cancel: Cancel::default(),
            summary: None,
        };
        let prepared = prepare(&request, "VLA", &context).unwrap().unwrap();
        // One table of 10,000 Float samples, with the lookup metadata.
        assert_eq!(prepared.reservation.memory(), 10_000 * 4 + 32);
    }
}

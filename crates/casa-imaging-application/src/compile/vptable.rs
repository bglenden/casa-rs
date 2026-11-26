// SPDX-License-Identifier: LGPL-3.0-or-later
//! Prepare the supplied radial definition for the imaging beam or mosaic kernels.

use casa_imaging_products::SavedRadialPrimaryBeam;
use casa_imaging_runtime::{Admitted, Demand, admit};
use casa_ms::{VoltagePatternTable, radial_pattern};
use casa_numerics::{BeamPattern, PrimaryBeam};
use std::path::Path;

use super::PrepareError;
use crate::{Gridder, ImagingRequest, RunContext};

pub(super) fn prepare(
    request: &ImagingRequest,
    telescope: &str,
    context: &RunContext,
) -> Result<Option<Admitted<SavedRadialPrimaryBeam>>, PrepareError> {
    let Some(path) = request.vptable.as_deref() else {
        return Ok(None);
    };
    if matches!(request.gridder, Gridder::Mosaic { .. }) || (!request.write_pb && !request.pbcor) {
        return Ok(None);
    }
    let definition = read_pattern(path, telescope, context)?;
    let reservation = admit(
        &context.host,
        &context.policy,
        &Demand {
            phase: "the saved radial primary beam",
            memory: BeamPattern::BYTES,
        },
    )?;
    Ok(Some(Admitted {
        value: SavedRadialPrimaryBeam::new(definition),
        reservation,
    }))
}

/// CASA's EVLA HetArrayConvFunc uses the same saved circular law for
/// every antenna of a MeasurementSet.
pub(super) fn prepare_mosaic(
    request: &ImagingRequest,
    telescope: &str,
    context: &RunContext,
) -> Result<Option<PrimaryBeam>, PrepareError> {
    let Some(path) = request.vptable.as_deref() else {
        return Ok(None);
    };
    if !matches!(request.gridder, Gridder::Mosaic { .. }) {
        return Ok(None);
    }
    if telescope != "EVLA" || request.vis.len() != 1 {
        return Err(PrepareError::SavedMosaicBeamScope);
    }
    read_pattern(path, telescope, context).map(Some)
}

fn read_pattern(
    path: &Path,
    telescope: &str,
    context: &RunContext,
) -> Result<PrimaryBeam, PrepareError> {
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
    let record = table
        .default_record(telescope)?
        .ok_or_else(|| PrepareError::SavedCommonBeam {
            telescope: telescope.to_owned(),
        })?;
    Ok(radial_pattern(&record)?)
}

#[cfg(test)]
mod tests {
    use crate::Cancel;
    use casa_imaging_runtime::{HostResources, ResourcePolicy};
    use serde_json::json;

    use super::*;

    fn supplied_beam_inputs(gridder: &str) -> (ImagingRequest, RunContext) {
        let table = Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("../casa-simulation/tests/data/sm_squint/custom_gaussian.vp");
        let request = crate::request::tests::request(
            json!({ "gridder": gridder, "vptable": table, "write_pb": true }),
        );
        let context = RunContext {
            host: HostResources::detect().unwrap(),
            policy: ResourcePolicy::Explicit {
                workers: 1,
                memory: 2 << 30,
            },
            cancel: Cancel::default(),
            summary: None,
        };
        (request, context)
    }

    #[test]
    fn the_supplied_voltage_samples_remain_admitted_after_table_reading() {
        let (request, context) = supplied_beam_inputs("standard");
        let prepared = prepare(&request, "VLA", &context).unwrap().unwrap();
        // One table of 10,000 Float samples, with the lookup metadata.
        assert_eq!(prepared.reservation.memory(), 10_000 * 4 + 32);
    }

    #[test]
    fn a_saved_common_beam_is_not_the_no_table_default() {
        let (request, context) = supplied_beam_inputs("standard");
        let result = prepare(&request, "ALMA", &context);
        assert!(
            matches!(result, Err(PrepareError::SavedCommonBeam { telescope }) if telescope == "ALMA")
        );
    }

    #[test]
    fn supplied_mosaic_beams_require_one_evla_measurement_set() {
        let (mut request, context) = supplied_beam_inputs("mosaic");
        assert!(matches!(
            prepare_mosaic(&request, "ALMA", &context),
            Err(PrepareError::SavedMosaicBeamScope)
        ));
        request.vis.push(request.vis[0].clone());
        assert!(matches!(
            prepare_mosaic(&request, "EVLA", &context),
            Err(PrepareError::SavedMosaicBeamScope)
        ));
    }
}

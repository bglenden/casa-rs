// SPDX-License-Identifier: LGPL-3.0-or-later
//! Match the reference's actual EOP and 2024 leap-second inputs.
use casa_types::measures::{
    EopValues, MeasuresProvider, MeasuresProviderState, ObservatoryPosition,
};
use serde_json::Value;
use sha2::{Digest, Sha256};
use std::sync::Arc;

#[derive(Debug)]
struct Reference {
    samples: [[f64; 6]; 2],
    state: MeasuresProviderState,
    catalogues: Arc<dyn MeasuresProvider>,
}

pub(super) fn provider(samples: &Value) -> Arc<dyn MeasuresProvider> {
    let data = std::array::from_fn(|row| {
        std::array::from_fn(|column| samples[row][column].as_f64().unwrap())
    });
    // The runtime requires an immutable identity for these scientific inputs.
    let mut identity = Sha256::new();
    identity.update(serde_json::to_vec(samples).unwrap());
    identity.update(37_f64.to_le_bytes());
    Arc::new(Reference {
        samples: data,
        state: MeasuresProviderState::new(identity.finalize().into(), 0),
        catalogues: casa_test_support::deterministic_measures_provider(),
    })
}

impl MeasuresProvider for Reference {
    fn prepare_bounded_state(&self) -> Result<Option<MeasuresProviderState>, String> {
        Ok(Some(self.state))
    }
    fn eop_values(&self, utc_mjd: f64) -> Result<Option<EopValues>, String> {
        let [first, last] = self.samples;
        if !(first[0]..=last[0]).contains(&utc_mjd) {
            return Err(format!("reference EOP does not cover {utc_mjd}"));
        }
        let fraction = (utc_mjd - first[0]) / (last[0] - first[0]);
        let value = |column: usize| first[column] + fraction * (last[column] - first[column]);
        Ok(Some(EopValues {
            dut1_seconds: value(1),
            x_arcsec: value(2),
            y_arcsec: value(3),
            dx_mas: value(4) * 1000.,
            dy_mas: value(5) * 1000.,
            is_predicted: false,
        }))
    }
    fn tai_minus_utc_seconds(&self, _: f64) -> Result<f64, String> {
        Ok(37.)
    }
    fn utc_from_tai_mjd(&self, tai_mjd: f64) -> Result<f64, String> {
        Ok(tai_mjd - 37. / 86400.)
    }
    fn observatory(&self, name: &str) -> Result<Option<ObservatoryPosition>, String> {
        self.catalogues.observatory(name)
    }
}

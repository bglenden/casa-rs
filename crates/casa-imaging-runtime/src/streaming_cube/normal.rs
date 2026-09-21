// SPDX-License-Identifier: LGPL-3.0-or-later

//! Fully admitted contiguous normal-state storage. This changes storage only;
//! the reconstruction owner still enforces ordered coverage and epoch transfer.

use std::{fmt, io, sync::Arc};

use casa_imaging_reconstruction::{
    SpectralOperatorError,
    runtime_adapter::{ChannelNormalStorageRequirement, NormalArrayStorage, NormalStorageFactory},
};

use crate::paged_cube_state::{BackingObservation, CubeArrayLedger, CubeBackingMetrics};

#[derive(Debug)]
pub(crate) struct ResidentNormalFactory {
    requirements: Box<[ChannelNormalStorageRequirement]>,
    retentions: Box<[(usize, Arc<dyn fmt::Debug + Send + Sync>)]>,
    metrics: Arc<CubeBackingMetrics>,
}

impl ResidentNormalFactory {
    pub(crate) fn new(
        requirements: Box<[ChannelNormalStorageRequirement]>,
        retentions: Box<[(usize, Arc<dyn fmt::Debug + Send + Sync>)]>,
        metrics: Arc<CubeBackingMetrics>,
    ) -> Self {
        Self {
            requirements,
            retentions,
            metrics,
        }
    }

    pub(crate) fn metadata_bytes(&self) -> usize {
        size_of::<Self>()
            + size_of_val(self.requirements.as_ref())
            + size_of_val(self.retentions.as_ref())
    }

    pub(crate) fn ledger(
        requirement: ChannelNormalStorageRequirement,
    ) -> io::Result<CubeArrayLedger> {
        let capacity = requirement
            .scalar_capacity()
            .checked_mul(size_of::<f64>())
            .and_then(|bytes| bytes.checked_add(size_of::<ResidentNormalArray>()))
            .ok_or_else(|| io::Error::other("resident normal capacity overflow"))?;
        let window = requirement
            .maximum_window_scalars()
            .checked_mul(size_of::<f64>())
            .ok_or_else(|| io::Error::other("resident normal window overflow"))?;
        Ok(CubeArrayLedger {
            retained_bytes: capacity,
            cache_bytes: 0,
            cache_index_bytes: 0,
            read_scratch_bytes: window,
            write_scratch_bytes: 0,
            flush_scratch_bytes: 0,
            storage_bytes: 0,
            file_handles: 0,
        })
    }
}

impl NormalStorageFactory for ResidentNormalFactory {
    fn create(
        &self,
        allocation_ordinal: usize,
        scalars: usize,
    ) -> Result<Box<dyn NormalArrayStorage>, SpectralOperatorError> {
        let requirement = *self
            .requirements
            .iter()
            .find(|r| r.allocation_ordinal() == allocation_ordinal)
            .filter(|r| scalars > 0 && scalars <= r.scalar_capacity())
            .ok_or_else(|| {
                SpectralOperatorError::NormalStorage(
                    "resident normal shape exceeds admission".into(),
                )
            })?;
        let mut ledger = Self::ledger(requirement)
            .map_err(|e| SpectralOperatorError::NormalStorage(e.to_string()))?;
        ledger.retained_bytes = size_of::<ResidentNormalArray>() + scalars * size_of::<f64>();
        let retention = self
            .retentions
            .iter()
            .find(|(ordinal, _)| *ordinal == allocation_ordinal)
            .ok_or_else(|| {
                SpectralOperatorError::NormalStorage(
                    "normal allocation lacks its retention owner".into(),
                )
            })?;
        Ok(Box::new(ResidentNormalArray {
            values: vec![0.0; scalars].into_boxed_slice(),
            window_scalars: requirement.maximum_window_scalars(),
            observation: self.metrics.register(ledger),
            _retention: retention.1.clone(),
        }))
    }
}

#[derive(Debug)]
struct ResidentNormalArray {
    values: Box<[f64]>,
    window_scalars: usize,
    observation: BackingObservation,
    _retention: Arc<dyn fmt::Debug + Send + Sync>,
}

impl NormalArrayStorage for ResidentNormalArray {
    fn len(&self) -> usize {
        self.values.len()
    }

    fn retained_resident_bytes(&self) -> usize {
        size_of_val(self.values.as_ref())
    }

    fn read(&self, start: usize, len: usize) -> Result<Box<[f64]>, SpectralOperatorError> {
        if len > self.window_scalars {
            return Err(SpectralOperatorError::InvalidSlab);
        }
        let result = self.values.read(start, len)?;
        self.observation.record(len, Default::default());
        Ok(result)
    }

    fn write(&mut self, start: usize, values: &[f64]) -> Result<(), SpectralOperatorError> {
        if values.len() > self.window_scalars {
            return Err(SpectralOperatorError::InvalidSlab);
        }
        self.values.write(start, values)?;
        self.observation.record(values.len(), Default::default());
        Ok(())
    }
}

#[cfg(test)]
#[path = "../../../casa-imaging-reconstruction/tests/support/streaming_cube.rs"]
mod fixture;

#[cfg(test)]
mod tests {
    use super::*;
    use casa_imaging_model::SpectralSamplingLaw;
    use casa_imaging_reconstruction::SpectralOperatorSpecification;

    #[test]
    fn resident_normal_is_bounded_bit_exact_and_retains_its_allocation_owner() {
        let problem = fixture::problem(SpectralSamplingLaw::LINEAR);
        let specification = SpectralOperatorSpecification::new(&problem).unwrap();
        let requirements =
            ChannelNormalStorageRequirement::for_streaming_cube(&specification, 1).unwrap();
        let legacy = ChannelNormalStorageRequirement::for_specification(&specification, 1).unwrap();
        assert_eq!(requirements[0].scalar_capacity(), 4 * 64 * 2);
        assert_eq!(requirements[1].scalar_capacity(), 4 * 64 * 3);
        assert_eq!(legacy[0].scalar_capacity(), 4 * 64 * 4);
        assert_eq!(legacy[1].scalar_capacity(), 4 * 64 * 5);
        let refresh =
            ChannelNormalStorageRequirement::for_streaming_cube_refresh(&specification, 1).unwrap();
        assert_eq!(refresh.len(), 1);
        assert_eq!(refresh[0].allocation_ordinal(), 0);
        assert_eq!(refresh[0].scalar_capacity(), 4 * 64 * 2);
        let initial_scalars = requirements
            .iter()
            .map(|r| r.scalar_capacity())
            .sum::<usize>();
        let refresh_scalars = refresh.iter().map(|r| r.scalar_capacity()).sum::<usize>();
        assert_eq!(initial_scalars + refresh_scalars, 4 * 64 * 7);
        let requirement = requirements[0];
        let admitted = ResidentNormalFactory::ledger(requirement).unwrap();
        let metrics = Arc::new(CubeBackingMetrics::default());
        let owner = Arc::new(());
        let factory = ResidentNormalFactory::new(
            requirements,
            vec![(0, owner.clone() as Arc<dyn fmt::Debug + Send + Sync>)].into(),
            metrics.clone(),
        );
        assert!(factory.create(2, 1).is_err());
        assert!(factory.create(0, 0).is_err());
        assert!(
            factory
                .create(0, requirement.scalar_capacity() + 1)
                .is_err()
        );
        assert_eq!(metrics.snapshot().live_backings, 0);
        let mut array = factory.create(0, requirement.scalar_capacity()).unwrap();
        assert_eq!(
            array.retained_resident_bytes(),
            requirement.scalar_capacity() * size_of::<f64>()
        );
        assert_eq!(vec![0.0; 8].into_boxed_slice().retained_resident_bytes(), 0);
        drop(factory);
        assert_eq!(Arc::strong_count(&owner), 2);
        let values = [
            0.0,
            -0.0,
            f64::from_bits(0x7ff8_0000_0000_1234),
            f64::MAX,
            0.125,
        ];
        array.write(7, &values).unwrap();
        let observed = array.read(7, values.len()).unwrap();
        assert_eq!(
            observed.iter().map(|v| v.to_bits()).collect::<Vec<_>>(),
            values.map(f64::to_bits)
        );
        assert!(
            array
                .read(0, requirement.maximum_window_scalars() + 1)
                .is_err()
        );
        assert!(array.write(array.len(), &[1.0]).is_err());
        assert!(array.read(usize::MAX, 1).is_err());
        let live = metrics.snapshot();
        assert_eq!(live.live_owned_bytes, admitted.retained_bytes);
        assert_eq!(
            (
                live.read_bytes,
                live.write_bytes,
                live.live_file_handles,
                live.live_storage_bytes
            ),
            (0, 0, 0, 0)
        );
        drop(array);
        assert_eq!(Arc::strong_count(&owner), 1);
        assert_eq!(metrics.snapshot().live_owned_bytes, 0);
    }
}

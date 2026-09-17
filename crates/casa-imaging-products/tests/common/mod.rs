// SPDX-License-Identifier: LGPL-3.0-or-later

//! Explicit small-fixture backing; production supplies the admitted paged capability.

use casa_imaging_products::{
    PlannedContinuumGeneration, ProductArrayStorage, ProductStorageFactory, ProductStoragePlan,
    ProductWindow, ProductWindowLayout, ProductsError, SealedMember,
};

pub fn full_window(planned: &PlannedContinuumGeneration) -> ProductStoragePlan {
    ProductStoragePlan::new(
        planned
            .members()
            .iter()
            .map(|member| member.axes().spectral().output_channels())
            .max()
            .unwrap(),
    )
    .unwrap()
}

#[derive(Debug)]
pub struct MemoryStorageFactory;

impl ProductStorageFactory for MemoryStorageFactory {
    fn create(
        &self,
        layout: ProductWindowLayout,
    ) -> Result<Box<dyn ProductArrayStorage>, ProductsError> {
        Ok(Box::new(MemoryStorage {
            layout,
            payload: vec![0.0; layout.values()?],
            validity: vec![false; layout.values()?],
        }))
    }
}

#[derive(Debug)]
struct MemoryStorage {
    layout: ProductWindowLayout,
    payload: Vec<f32>,
    validity: Vec<bool>,
}

impl MemoryStorage {
    fn indices(&self, start: [usize; 4], shape: [usize; 4]) -> Result<Vec<usize>, ProductsError> {
        let values = shape.iter().product::<usize>();
        if values > self.layout.maximum_values()
            || (0..4).any(|axis| {
                start[axis]
                    .checked_add(shape[axis])
                    .is_none_or(|end| end > self.layout.shape()[axis])
            })
        {
            return Err(ProductsError::InvalidWindow);
        }
        Ok((0..values)
            .map(|local| {
                let mut remainder = local;
                let mut coordinates = [0; 4];
                for axis in (0..4).rev() {
                    coordinates[axis] = remainder % shape[axis] + start[axis];
                    remainder /= shape[axis];
                }
                coordinates
                    .into_iter()
                    .zip(self.layout.shape())
                    .fold(0, |index, (coordinate, extent)| index * extent + coordinate)
            })
            .collect())
    }
}

impl ProductArrayStorage for MemoryStorage {
    fn shape(&self) -> [usize; 4] {
        self.layout.shape()
    }
    fn read_payload(
        &self,
        start: [usize; 4],
        shape: [usize; 4],
        values: &mut [f32],
    ) -> Result<(), ProductsError> {
        for (target, index) in values.iter_mut().zip(self.indices(start, shape)?) {
            *target = self.payload[index];
        }
        Ok(())
    }
    fn read_validity(
        &self,
        start: [usize; 4],
        shape: [usize; 4],
        values: &mut [bool],
    ) -> Result<(), ProductsError> {
        for (target, index) in values.iter_mut().zip(self.indices(start, shape)?) {
            *target = self.validity[index];
        }
        Ok(())
    }
    fn write(&mut self, window: &ProductWindow) -> Result<(), ProductsError> {
        for (local, index) in self
            .indices(window.start(), window.shape())?
            .into_iter()
            .enumerate()
        {
            self.payload[index] = window.payload()[local];
            self.validity[index] = window.validity()[local];
        }
        Ok(())
    }
    fn flush(&mut self) -> Result<(), ProductsError> {
        Ok(())
    }
}

/// Small regression fixtures explicitly read their whole admitted window.
pub trait SealedMemberFixtureRead {
    fn payload(&self) -> Vec<f32>;
    fn validity(&self) -> Vec<bool>;
}

impl SealedMemberFixtureRead for SealedMember {
    fn payload(&self) -> Vec<f32> {
        let layout = self.window_layout();
        self.read_window(0..layout.shape()[layout.spectral_axis()])
            .unwrap()
            .into_parts()
            .2
    }
    fn validity(&self) -> Vec<bool> {
        let layout = self.window_layout();
        self.read_window(0..layout.shape()[layout.spectral_axis()])
            .unwrap()
            .into_parts()
            .3
    }
}

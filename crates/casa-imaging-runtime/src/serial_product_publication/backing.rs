// SPDX-License-Identifier: LGPL-3.0-or-later

use casa_imaging_products::{
    PlannedContinuumGeneration, ProductArrayStorage, ProductStorageFactory, ProductStoragePlan,
    ProductWindow, ProductWindowLayout, ProductsError,
};
use casa_lattices::{Lattice, LatticeMut, PagedArray, TiledArrayStorageLayout, TiledShape};
use ndarray::{ArrayD, ArrayViewMut, IxDyn};
use std::{path::PathBuf, sync::Mutex};
use tempfile::TempDir;

/// Prepared physical product arrays; creation is deferred until leased execution.
#[derive(Debug)]
pub struct SerialProductBackingPlan {
    directory: PathBuf,
    pub(super) window: ProductStoragePlan,
    members: Vec<MemberLayout>,
    pub(super) heap_bytes: u64,
    pub(super) storage_bytes: u64,
    pub(super) scratch_bytes: u64,
}

#[derive(Debug)]
struct MemberLayout {
    window: ProductWindowLayout,
    payload: TiledArrayStorageLayout,
    validity: TiledArrayStorageLayout,
}

impl SerialProductBackingPlan {
    /// Prepare exact backing from an authority-validated physical storage binding.
    pub fn prepare(
        planned: &PlannedContinuumGeneration,
        window: ProductStoragePlan,
        storage: &crate::ManagedSpillStorage,
    ) -> Result<Self, ProductsError> {
        Self::new(planned, window, storage.directory().to_path_buf())
    }
    pub(super) fn new(
        planned: &PlannedContinuumGeneration,
        window: ProductStoragePlan,
        directory: PathBuf,
    ) -> Result<Self, ProductsError> {
        let mut result = Self {
            directory,
            window,
            members: Vec::with_capacity(planned.members().len()),
            heap_bytes: 0,
            storage_bytes: 0,
            scratch_bytes: 0,
        };
        for member in planned.members() {
            let window = window.layout(member.axes())?;
            let tile = publication_tile_shape(
                window.shape(),
                window.spectral_axis(),
                window.maximum_values(),
            );
            let shape = TiledShape::with_tile_shape(window.shape().to_vec(), tile.to_vec())
                .map_err(error)?;
            let payload = PagedArray::<f32>::storage_layout(
                shape.clone(),
                window
                    .maximum_values()
                    .checked_mul(4)
                    .ok_or(ProductsError::InvalidWindow)?,
            )
            .map_err(error)?;
            let validity = PagedArray::<bool>::storage_layout(shape, window.maximum_values())
                .map_err(error)?;
            let parent = result.directory.join(".casa-rs-product-XXXXXX");
            let heap =
                PagedArray::<f32>::planned_persistent_heap_bytes(&payload, &parent.join("payload"))
                    .map_err(error)?
                    + PagedArray::<bool>::planned_persistent_heap_bytes(
                        &validity,
                        &parent.join("validity"),
                    )
                    .map_err(error)?
                    + payload.owned_heap_bytes().map_err(error)?
                    + validity.owned_heap_bytes().map_err(error)?
                    + size_of::<PagedProductArray>()
                    + size_of::<MemberLayout>()
                    + parent.as_os_str().len();
            result.heap_bytes = result
                .heap_bytes
                .checked_add(heap as u64)
                .ok_or(ProductsError::InvalidWindow)?;
            result.storage_bytes = result
                .storage_bytes
                .checked_add(
                    (payload.storage_bytes().map_err(error)?
                        + validity.storage_bytes().map_err(error)?) as u64,
                )
                .ok_or(ProductsError::InvalidWindow)?;
            let scratch = payload
                .slice_scratch_bytes()
                .map_err(error)?
                .max(validity.slice_scratch_bytes().map_err(error)?)
                .max(payload.flush_scratch_bytes().map_err(error)?)
                .max(validity.flush_scratch_bytes().map_err(error)?)
                .checked_add(
                    window
                        .maximum_values()
                        .checked_mul(4)
                        .ok_or(ProductsError::InvalidWindow)?,
                )
                .ok_or(ProductsError::InvalidWindow)?;
            result.scratch_bytes = result.scratch_bytes.max(scratch as u64);
            result.members.push(MemberLayout {
                window,
                payload,
                validity,
            });
        }
        result.heap_bytes += result.directory.as_os_str().len() as u64;
        Ok(result)
    }
    pub(super) fn descriptors(&self) -> u64 {
        (self.members.len() * 2) as u64
    }
}

fn publication_tile_shape(
    shape: [usize; 4],
    spectral_axis: usize,
    maximum_values: usize,
) -> [usize; 4] {
    // Intersect a canonical hash rectangle with a single-channel write window.
    // Neither traversal then fetches unrelated spatial cells from a full plane.
    let mut tile = shape;
    let mut remaining = maximum_values;
    for extent in tile.iter_mut().rev() {
        *extent = (*extent).min(remaining);
        remaining /= *extent;
    }
    tile[spectral_axis] = 1;
    tile
}

impl ProductStorageFactory for SerialProductBackingPlan {
    fn create(
        &self,
        layout: ProductWindowLayout,
    ) -> Result<Box<dyn ProductArrayStorage>, ProductsError> {
        let prepared = self
            .members
            .iter()
            .find(|entry| entry.window == layout)
            .ok_or(ProductsError::InvalidWindow)?;
        let directory = tempfile::Builder::new()
            .prefix(".casa-rs-product-")
            .rand_bytes(6)
            .tempdir_in(&self.directory)
            .map_err(error)?;
        let payload =
            PagedArray::<f32>::create_planned(&prepared.payload, directory.path().join("payload"))
                .map_err(error)?;
        let validity = PagedArray::<bool>::create_planned(
            &prepared.validity,
            directory.path().join("validity"),
        )
        .map_err(error)?;
        Ok(Box::new(PagedProductArray {
            arrays: Mutex::new((payload, validity)),
            shape: layout.shape(),
            _directory: directory,
        }))
    }
}

#[derive(Debug)]
struct PagedProductArray {
    arrays: Mutex<(PagedArray<f32>, PagedArray<bool>)>,
    shape: [usize; 4],
    _directory: TempDir,
}

impl ProductArrayStorage for PagedProductArray {
    fn shape(&self) -> [usize; 4] {
        self.shape
    }
    fn read_payload(
        &self,
        start: [usize; 4],
        shape: [usize; 4],
        values: &mut [f32],
    ) -> Result<(), ProductsError> {
        let arrays = self.arrays.lock().map_err(error)?;
        let window = arrays.0.get_slice(&start, &shape, &[1; 4]).map_err(error)?;
        ArrayViewMut::from_shape(IxDyn(&shape), values)
            .map_err(error)?
            .assign(&window);
        Ok(())
    }
    fn read_validity(
        &self,
        start: [usize; 4],
        shape: [usize; 4],
        values: &mut [bool],
    ) -> Result<(), ProductsError> {
        let arrays = self.arrays.lock().map_err(error)?;
        let window = arrays.1.get_slice(&start, &shape, &[1; 4]).map_err(error)?;
        ArrayViewMut::from_shape(IxDyn(&shape), values)
            .map_err(error)?
            .assign(&window);
        Ok(())
    }
    fn write(&mut self, window: &ProductWindow) -> Result<(), ProductsError> {
        let arrays = self.arrays.get_mut().map_err(error)?;
        let payload = ArrayD::from_shape_vec(IxDyn(&window.shape()), window.payload().to_vec())
            .map_err(error)?;
        arrays
            .0
            .put_slice(&payload, &window.start())
            .map_err(error)?;
        drop(payload);
        let validity = ArrayD::from_shape_vec(IxDyn(&window.shape()), window.validity().to_vec())
            .map_err(error)?;
        arrays
            .1
            .put_slice(&validity, &window.start())
            .map_err(error)
    }
    fn flush(&mut self) -> Result<(), ProductsError> {
        let arrays = self.arrays.get_mut().map_err(error)?;
        arrays.0.flush().map_err(error)?;
        arrays.1.flush().map_err(error)
    }
}

fn error(value: impl std::fmt::Display) -> ProductsError {
    ProductsError::Storage(value.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn publication_tiles_intersect_write_and_hash_windows() {
        assert_eq!(
            publication_tile_shape([128, 128, 1, 512], 3, 16384),
            [1, 32, 1, 1]
        );
        assert_eq!(
            publication_tile_shape([128, 128, 1, 1], 3, 16384),
            [128, 128, 1, 1]
        );
        assert_eq!(publication_tile_shape([64, 8, 8, 1], 0, 64), [1, 8, 8, 1]);
        assert_eq!(publication_tile_shape([3, 4, 2, 5], 3, 24), [1, 2, 2, 1]);
    }

    #[test]
    #[ignore = "bounded publication diagnostic: requires a fresh durable CASA_RS_PUBLICATION_PROBE_ROOT and outer resource guard"]
    fn publication_backing_read_amplification() {
        let root = std::path::PathBuf::from(
            std::env::var_os("CASA_RS_PUBLICATION_PROBE_ROOT").expect("durable probe root"),
        );
        std::fs::create_dir(&root).unwrap();
        let directory = tempfile::tempdir_in(&root).unwrap();
        let shape = [128, 128, 1, 64];
        let plane_values = shape[0] * shape[1];
        let tiled = TiledShape::with_tile_shape(
            shape.to_vec(),
            publication_tile_shape(shape, 3, plane_values).to_vec(),
        )
        .unwrap();
        let mut payload = PagedArray::<f32>::create_with_cache(
            tiled.clone(),
            directory.path().join("payload"),
            plane_values * 4,
        )
        .unwrap();
        let mut validity = PagedArray::<bool>::create_with_cache(
            tiled,
            directory.path().join("validity"),
            plane_values,
        )
        .unwrap();
        for channel in 0..shape[3] {
            let plane = ArrayD::from_shape_fn(IxDyn(&[128, 128, 1, 1]), |index| {
                ((index[0] * 128 + index[1]) * 64 + channel) as f32
            });
            let support = plane.mapv(|value| value as usize % 3 != 0);
            payload.put_slice(&plane, &[0, 0, 0, channel]).unwrap();
            validity.put_slice(&support, &[0, 0, 0, channel]).unwrap();
        }
        payload.flush().unwrap();
        validity.flush().unwrap();
        let before_payload = payload.io_stats();
        let before_validity = validity.io_stats();
        let backing = PagedProductArray {
            arrays: Mutex::new((payload, validity)),
            shape,
            _directory: directory,
        };
        let mut values = vec![0.0; plane_values];
        let mut support = vec![false; plane_values];
        let started = std::time::Instant::now();
        // These are the canonical hash rectangles at a one-plane window bound.
        // This diagnoses backing reads; the application benchmark times hashing.
        for x in (0..128).step_by(2) {
            backing
                .read_payload([x, 0, 0, 0], [2, 128, 1, 64], &mut values)
                .unwrap();
            backing
                .read_validity([x, 0, 0, 0], [2, 128, 1, 64], &mut support)
                .unwrap();
            for (index, (&value, &valid)) in values.iter().zip(&support).enumerate() {
                let expected = x * 128 * 64 + index;
                assert_eq!(value, expected as f32);
                assert_eq!(valid, expected % 3 != 0);
            }
        }
        let seconds = started.elapsed().as_secs_f64();
        let arrays = backing.arrays.lock().unwrap();
        let payload = arrays.0.io_stats().delta_since(before_payload);
        let validity = arrays.1.io_stats().delta_since(before_validity);
        assert!(
            payload.lru_read_bytes + payload.lru_batch_load_bytes <= plane_values * shape[3] * 4
        );
        assert!(
            validity.lru_read_bytes + validity.lru_batch_load_bytes <= plane_values * shape[3] / 8
        );
        let record = format!(
            "shape={shape:?}\nwindow_channels=1\nseconds={seconds}\nlogical_payload_bytes={}\nlogical_validity_disk_bytes={}\npayload={payload:?}\nvalidity={validity:?}\n",
            plane_values * shape[3] * 4,
            plane_values * shape[3] / 8,
        );
        std::fs::write(root.join("result.txt"), &record).unwrap();
        println!("{record}");
    }

    #[test]
    fn product_backing_reads_multiaxis_windows_in_canonical_order() {
        let directory = tempfile::tempdir().unwrap();
        let shape = [3, 4, 2, 5];
        let tiled = TiledShape::with_tile_shape(shape.to_vec(), vec![2, 3, 1, 2]).unwrap();
        let mut payload =
            PagedArray::<f32>::create(tiled.clone(), directory.path().join("payload")).unwrap();
        let mut validity =
            PagedArray::<bool>::create(tiled, directory.path().join("validity")).unwrap();
        let source = ArrayD::from_shape_fn(IxDyn(&shape), |index| {
            (1000 * index[0] + 100 * index[1] + 10 * index[2] + index[3]) as f32
        });
        let support = source.mapv(|value| value as usize % 3 != 0);
        payload.put_slice(&source, &[0; 4]).unwrap();
        validity.put_slice(&support, &[0; 4]).unwrap();
        let backing = PagedProductArray {
            arrays: Mutex::new((payload, validity)),
            shape,
            _directory: directory,
        };
        for (start, extent) in [
            ([0; 4], shape),
            ([1, 1, 0, 1], [2, 2, 2, 3]),
            ([1, 0, 1, 0], [1, 4, 1, 5]),
        ] {
            let count = extent.iter().product();
            let mut actual = vec![0.0; count];
            let mut actual_support = vec![false; count];
            backing.read_payload(start, extent, &mut actual).unwrap();
            backing
                .read_validity(start, extent, &mut actual_support)
                .unwrap();
            let mut offset = 0;
            for x in start[0]..start[0] + extent[0] {
                for y in start[1]..start[1] + extent[1] {
                    for polarization in start[2]..start[2] + extent[2] {
                        for channel in start[3]..start[3] + extent[3] {
                            let index = [x, y, polarization, channel];
                            assert_eq!(actual[offset], source[index]);
                            assert_eq!(actual_support[offset], support[index]);
                            offset += 1;
                        }
                    }
                }
            }
            assert_eq!(offset, count);
        }
    }
}

// SPDX-License-Identifier: LGPL-3.0-or-later

use std::fs::{File, OpenOptions};

use super::*;
use crate::bounded_stream::{
    BlockIdentity, BoundedExecution, BoundedStreamError, BoundedStreamPlan, KernelPartition,
    PartitionedKernel, WorkIdentity, execute_bounded,
};
use crate::managed_spill::tests::test_authority;

fn plan(
    rows: u64,
    channels: usize,
    correlations: usize,
    tile: usize,
    block_rows: usize,
) -> StorePlan {
    let row_bytes = ROW_BYTES + channels * (8 + SAMPLE_BYTES * correlations);
    let encoded = ROW_BYTES.max(tile * (8 + SAMPLE_BYTES * correlations));
    let budget = size_of::<NativeBlock>()
        + size_of::<Vec<u8>>()
        + CRC_BYTES
        + block_rows * (row_bytes + encoded);
    StorePlan::new(rows, channels, correlations, tile, budget, u64::MAX).unwrap()
}

fn fill(block: &mut NativeBlock, first_row: u64) {
    for (row, metadata) in block.metadata.iter_mut().enumerate() {
        let source_row = first_row + row as u64;
        let start = 1e9 + source_row as f64 * 0.25e6;
        let step = if source_row % 2 == 0 { 1e6 } else { -1e6 };
        *metadata = RowMetadata {
            physical_row: source_row * 2 + 100,
            uvw_m: [source_row as f64 * -0.5, 2.0, -3.2],
            phase_shift_m: 0.001 * source_row as f64,
            original_pair_hz: [start, start + step],
        };
        for channel in 0..block.channels {
            let cell = row * block.channels + channel;
            block.frequencies_hz[cell] = start + channel as f64 * step;
            for corr in 0..block.correlations {
                let sample = cell * block.correlations + corr;
                block.values[sample] = Complex64::new(
                    source_row as f64 + channel as f64 * 0.13,
                    -0.0 - corr as f64 * 0.2,
                );
                block.weights[sample] = 0.7 + channel as f64 * 0.11;
                block.flags[sample] = channel % 3 == corr;
                block.weight_flags[sample] = channel == 4;
            }
        }
    }
    // Flagged input payloads still belong in the native store, bit for bit.
    if block.values.len() > 3 {
        block.values[3].re = f64::from_bits(0x7ff8_0000_0000_0017);
        block.flags[3] = true;
    }
}

fn store(plan: StorePlan) -> (tempfile::TempDir, NativeStore) {
    let directory = tempfile::tempdir().unwrap();
    let (_, storage) = test_authority(directory.path(), plan.artifact_bytes);
    let mut writer = NativeStoreWriter::create(&storage, plan).unwrap();
    let mut block = NativeBlock::new(plan.block_rows, plan.channels, plan.correlations).unwrap();
    assert!(
        block.capacity_bytes() + size_of::<Vec<u8>>() + writer.encoded.capacity()
            <= plan.preparation_bytes
    );
    let pointer = writer.encoded.as_ptr();
    for index in 0..plan.blocks() {
        block
            .set_shape(plan.rows_in(index).unwrap(), plan.channels)
            .unwrap();
        fill(&mut block, index * plan.block_rows as u64);
        writer.append(&block).unwrap();
        assert_eq!(writer.encoded.as_ptr(), pointer);
    }
    (directory, writer.finish().unwrap())
}

fn assert_window(actual: &NativeBlock, original: &NativeBlock, channels: Range<usize>) {
    assert_eq!(actual.metadata, original.metadata);
    assert_eq!(actual.channels, channels.len());
    for row in 0..original.metadata.len() {
        for (local, channel) in channels.clone().enumerate() {
            let source_cell = row * original.channels + channel;
            let target_cell = row * actual.channels + local;
            assert_eq!(
                actual.frequencies_hz[target_cell].to_bits(),
                original.frequencies_hz[source_cell].to_bits()
            );
            for corr in 0..original.correlations {
                let source = source_cell * original.correlations + corr;
                let target = target_cell * original.correlations + corr;
                assert_eq!(
                    actual.values[target].re.to_bits(),
                    original.values[source].re.to_bits()
                );
                assert_eq!(
                    actual.values[target].im.to_bits(),
                    original.values[source].im.to_bits()
                );
                assert_eq!(
                    actual.weights[target].to_bits(),
                    original.weights[source].to_bits()
                );
                assert_eq!(actual.flags[target], original.flags[source]);
                assert_eq!(actual.weight_flags[target], original.weight_flags[source]);
            }
        }
    }
}

#[test]
fn overlapping_band_windows_reuse_checked_frames_with_bounded_storage() {
    let plan = plan(7, 64, 2, 1, 2);
    let (_directory, mut store) = store(plan);
    let slots = plan.reader_cache_slots(4, 2).unwrap();
    assert_eq!(slots, plan.blocks() as usize * 9);
    assert!(store.reader(0).is_err());
    assert!(store.reader(plan.maximum_cache_slots() + 1).is_err());
    let mut reader = store.reader(slots).unwrap();
    let allocated = size_of::<NativeStoreReader>()
        + reader.encoded.capacity()
        + reader.frames.capacity() * size_of::<CachedFrame>();
    assert_eq!(
        allocated as u64 + plan.page_cache_bytes,
        plan.reader_capacity_bytes(slots).unwrap()
    );
    assert!(
        reader.encoded.len() + reader.frames.len() * size_of::<CachedFrame>()
            <= plan.preparation_bytes
    );
    let pointer = reader.encoded.as_ptr();
    let mut output = NativeBlock::new(plan.block_rows, 2, 2).unwrap();
    let mut original = NativeBlock::new(plan.block_rows, plan.channels, 2).unwrap();
    for start in 0..4 {
        for block in 0..plan.blocks() {
            reader
                .read_block(block, start..start + 2, &mut output)
                .unwrap();
            original
                .set_shape(plan.rows_in(block).unwrap(), plan.channels)
                .unwrap();
            fill(&mut original, block * plan.block_rows as u64);
            assert_window(&output, &original, start..start + 2);
        }
    }
    assert_eq!(reader.io.operations, 6 * plan.blocks());
    assert_eq!(reader.io.cache_hits, 6 * plan.blocks());
    assert_eq!(
        reader.io.checksum_bytes,
        reader.io.bytes + 4 * reader.io.operations
    );
    let bytes = reader.io.bytes;
    for start in 0..4 {
        for block in 0..plan.blocks() {
            reader
                .read_block(block, start..start + 2, &mut output)
                .unwrap();
        }
    }
    assert_eq!(
        reader.io.bytes, bytes,
        "all reused frames avoid another physical read and CRC"
    );
    assert_eq!(reader.encoded.as_ptr(), pointer);
    let large = StorePlan::new(1_000_000, 64, 2, 1, plan.preparation_bytes, u64::MAX).unwrap();
    assert!(large.reader_cache_slots(4, 2).unwrap() <= plan.maximum_cache_slots());
}

#[test]
fn cache_eviction_and_failed_load_never_relabel_overwritten_bytes() {
    let plan = plan(2, 8, 2, 1, 2);
    let (_directory, mut store) = store(plan);
    let mut reader = store.reader(2).unwrap();
    let metadata = reader.read_frame(0, None).unwrap().to_vec();
    reader.read_frame(0, Some(0)).unwrap();
    assert_eq!(reader.read_frame(0, None).unwrap(), metadata);
    reader.read_frame(0, Some(1)).unwrap();
    assert_eq!(reader.io.operations, 3);
    assert_eq!(reader.io.cache_hits, 1);
    // Corrupt a not-yet-cached frame. Its failed read overwrites the slot that
    // formerly held metadata; that old identity must no longer be a cache hit.
    let (_, offset, _) = plan.frame(0, Some(2)).unwrap();
    let file = reader.store.file.as_file();
    let mut byte = [0];
    file.read_exact_at(&mut byte, offset + 8).unwrap();
    byte[0] ^= 1;
    file.write_all_at(&byte, offset + 8).unwrap();
    assert_eq!(
        reader.read_frame(0, Some(2)).unwrap_err().kind(),
        io::ErrorKind::InvalidData
    );
    assert_eq!(reader.read_frame(0, None).unwrap(), metadata);
    assert_eq!(reader.io.operations, 4);
    assert_eq!(reader.io.cache_hits, 1);
    reader.store.file.as_file().set_len(offset).unwrap();
    assert_eq!(
        reader.read_frame(0, Some(2)).unwrap_err().kind(),
        io::ErrorKind::UnexpectedEof
    );
}

#[test]
fn tile_windows_roundtrip_exact_payload_and_reuse_bounded_buffers() {
    let plan = plan(7, 9, 2, 3, 2);
    let (_directory, mut store) = store(plan);
    assert_eq!(store.written.bytes, plan.artifact_bytes);
    assert_eq!(
        store.written.operations,
        plan.blocks() * (plan.tiles as u64 + 1)
    );
    assert_eq!(
        store.written.checksum_bytes,
        plan.artifact_bytes + store.written.operations * 4
    );
    let mut reader = store.reader(1).unwrap();
    let mut output = NativeBlock::new(plan.block_rows, plan.channels, plan.correlations).unwrap();
    let mut original = NativeBlock::new(plan.block_rows, plan.channels, plan.correlations).unwrap();
    let pointers = (
        reader.encoded.as_ptr(),
        output.values.as_ptr(),
        output.metadata.as_ptr(),
    );
    for channels in [0..9, 0..1, 2..5, 8..9] {
        for block in (0..plan.blocks()).rev() {
            original
                .set_shape(plan.rows_in(block).unwrap(), plan.channels)
                .unwrap();
            fill(&mut original, block * plan.block_rows as u64);
            let before = reader.io;
            reader
                .read_block(block, channels.clone(), &mut output)
                .unwrap();
            assert_window(&output, &original, channels.clone());
            let tiles =
                channels.start / plan.tile_channels..=(channels.end - 1) / plan.tile_channels;
            let mut expected_bytes = plan.frame(block, None).unwrap().2 as u64;
            let mut expected_operations = 1;
            for tile in tiles {
                expected_bytes += plan.frame(block, Some(tile)).unwrap().2 as u64;
                expected_operations += 1;
            }
            assert_eq!(reader.io.bytes - before.bytes, expected_bytes);
            assert_eq!(
                reader.io.operations - before.operations,
                expected_operations
            );
            assert_eq!(
                pointers,
                (
                    reader.encoded.as_ptr(),
                    output.values.as_ptr(),
                    output.metadata.as_ptr()
                )
            );
        }
    }
}

#[test]
fn source_buffer_batches_native_rows_independently_of_weighted_chunks() {
    for (rows, channels, correlations, tile, expected_rows) in [
        (3, 6, 2, 3, 1),
        (3, 6, 2, 6, 3),
        (42_120, 512, 2, 32, 2048),
        (100, 16_384, 4, 64, 16),
        (100, 16_384, 4, 64, 1),
    ] {
        let expected = plan(rows, channels, correlations, tile, expected_rows);
        let actual = StorePlan::for_source_buffer(
            rows,
            channels,
            correlations,
            tile,
            expected.preparation_bytes,
        )
        .unwrap();
        assert_eq!(actual.block_rows, expected_rows);
        assert_eq!(actual, expected);
        let minimum = StorePlan::for_source_buffer(rows, channels, correlations, tile, 1).unwrap();
        assert_eq!(minimum.block_rows, 1);
    }
    for (rows, channels, correlations, tile, buffer_bytes) in [
        (0, 6, 2, 3, 7),
        (3, 0, 2, 1, 7),
        (3, 6, 0, 3, 7),
        (3, 6, 2, 7, 7),
        (3, 6, 2, 3, 0),
        (u64::MAX, usize::MAX, 4, 1, usize::MAX),
    ] {
        assert!(
            StorePlan::for_source_buffer(rows, channels, correlations, tile, buffer_bytes).is_err()
        );
    }
}

#[test]
fn planner_derives_rows_from_bytes_and_checks_overflow_and_storage_edges() {
    assert_eq!(size_of::<RowMetadata>(), ROW_BYTES);
    for (rows, channels, correlations, tile) in [
        (1, 1, 1, 1),
        (19, 7, 2, 3),
        (42_120, 512, 2, 32),
        (100, 16_384, 4, 64),
    ] {
        let exact = plan(rows, channels, correlations, tile, 1);
        assert!(exact.page_cache_bytes >= exact.frame_bytes as u64);
        assert_eq!(exact.block_rows, 1);
        assert!(
            StorePlan::new(
                rows,
                channels,
                correlations,
                tile,
                exact.preparation_bytes - 1,
                u64::MAX
            )
            .is_err()
        );
        let doubled = plan(rows, channels, correlations, tile, 2);
        assert_eq!(doubled.block_rows, (rows as usize).min(2));
        assert!(
            StorePlan::new(
                rows,
                channels,
                correlations,
                tile,
                doubled.preparation_bytes,
                doubled.artifact_bytes - 1
            )
            .is_err()
        );
        let last = doubled
            .frame(doubled.blocks() - 1, Some(doubled.tiles - 1))
            .unwrap();
        assert_eq!(last.1 + last.2 as u64, doubled.artifact_bytes);
        assert!(doubled.frame(doubled.blocks(), None).is_err());
        assert!(doubled.frame(0, Some(doubled.tiles)).is_err());
    }
    assert!(StorePlan::new(u64::MAX, 512, 2, 32, 1 << 20, u64::MAX).is_err());
    assert!(StorePlan::new(1, usize::MAX, 4, 1, usize::MAX, u64::MAX).is_err());
    assert!(StorePlan::new(0, 1, 1, 1, 1024, 1024).is_err());
    assert!(StorePlan::new(1, 1, 5, 1, 1024, 1024).is_err());
}

#[test]
fn failed_or_incomplete_writes_cannot_become_readable_stores() {
    let plan = plan(3, 4, 2, 2, 2);
    let directory = tempfile::tempdir().unwrap();
    let (_, storage) = test_authority(directory.path(), plan.artifact_bytes);
    assert!(
        NativeStoreWriter::create(&storage, plan)
            .unwrap()
            .finish()
            .is_err()
    );
    let mut writer = NativeStoreWriter::create(&storage, plan).unwrap();
    let mut input = NativeBlock::new(plan.block_rows, plan.channels, plan.correlations).unwrap();
    fill(&mut input, 0);
    input.values.pop();
    assert!(writer.append(&input).is_err());
    assert_eq!(writer.io.bytes, 0);
    assert!(writer.finish().is_err());

    let mut writer = NativeStoreWriter::create(&storage, plan).unwrap();
    let mut input = NativeBlock::new(plan.block_rows, plan.channels, plan.correlations).unwrap();
    fill(&mut input, 0);
    *writer.file.as_file_mut() = File::open(writer.file.path()).unwrap();
    let error = writer.append(&input).unwrap_err();
    assert!(error.raw_os_error().is_some());
    assert!(writer.poisoned);
    assert!(writer.finish().is_err());
}

#[test]
fn truncated_or_unreadable_input_is_an_error_not_end_of_stream() {
    let plan = plan(3, 4, 2, 2, 2);
    let (_directory, mut store) = store(plan);
    let mut output = NativeBlock::new(plan.block_rows, plan.channels, plan.correlations).unwrap();
    store
        .file
        .as_file()
        .set_len(plan.artifact_bytes - 1)
        .unwrap();
    let error = store
        .reader(1)
        .unwrap()
        .read_block(plan.blocks() - 1, 0..plan.channels, &mut output)
        .unwrap_err();
    assert_eq!(error.kind(), io::ErrorKind::UnexpectedEof);
    *store.file.as_file_mut() = OpenOptions::new()
        .write(true)
        .open(store.file.path())
        .unwrap();
    let error = store
        .reader(1)
        .unwrap()
        .read_block(0, 0..plan.channels, &mut output)
        .unwrap_err();
    assert!(error.raw_os_error().is_some());
    assert_ne!(error.kind(), io::ErrorKind::UnexpectedEof);
}

#[test]
fn misplaced_or_corrupt_frames_fail_at_the_persistence_boundary() {
    let plan = plan(3, 6, 2, 3, 2);
    let (_directory, mut store) = store(plan);
    let (_, source, bytes) = plan.frame(0, Some(0)).unwrap();
    let (_, destination, _) = plan.frame(0, Some(1)).unwrap();
    let mut frame = vec![0; bytes];
    store
        .file
        .as_file()
        .read_exact_at(&mut frame, source)
        .unwrap();
    store
        .file
        .as_file()
        .write_all_at(&frame, destination)
        .unwrap();
    let mut output = NativeBlock::new(plan.block_rows, plan.channels, plan.correlations).unwrap();
    let error = store
        .reader(1)
        .unwrap()
        .read_block(0, 3..6, &mut output)
        .unwrap_err();
    assert_eq!(error.kind(), io::ErrorKind::InvalidData);
    frame[12] ^= 1;
    store.file.as_file().write_all_at(&frame, source).unwrap();
    let error = store
        .reader(1)
        .unwrap()
        .read_block(0, 0..3, &mut output)
        .unwrap_err();
    assert_eq!(error.kind(), io::ErrorKind::InvalidData);
    assert!(decode_flag(2).is_err());
}

#[test]
fn one_source_producer_shares_borrowed_windows_with_four_workers() {
    let plan = plan(7, 9, 2, 3, 2);
    let (_directory, mut store) = store(plan);
    let mut reader = store.reader(1).unwrap();
    let mut output = NativeBlock::new(plan.block_rows, plan.channels, plan.correlations).unwrap();
    let mut original = NativeBlock::new(plan.block_rows, plan.channels, plan.correlations).unwrap();
    for index in (0..plan.blocks()).rev() {
        original
            .set_shape(plan.rows_in(index).unwrap(), plan.channels)
            .unwrap();
        fill(&mut original, index * plan.block_rows as u64);
        let before = reader.io.operations;
        reader
            .read_block(index, 0..plan.channels, &mut output)
            .unwrap();
        assert_eq!(reader.io.operations - before, plan.tiles as u64 + 1);
        assert_window(&output, &original, 0..plan.channels);
        let values = ndarray::ArrayView3::from_shape(
            (output.metadata.len(), plan.channels, plan.correlations),
            &output.values,
        )
        .unwrap();
        std::thread::scope(|scope| {
            for channel in [0, 2, 4, 7] {
                let band = values.slice(ndarray::s![.., channel..channel + 2, ..]);
                let original = &original;
                assert_eq!(
                    band.as_ptr(),
                    output.values[channel * plan.correlations..].as_ptr()
                );
                scope.spawn(move || {
                    for ((row, local, corr), value) in band.indexed_iter() {
                        let expected = original.values
                            [(row * plan.channels + channel + local) * plan.correlations + corr];
                        assert_eq!(value.re.to_bits(), expected.re.to_bits());
                        assert_eq!(value.im.to_bits(), expected.im.to_bits());
                    }
                });
            }
        });
    }
}

#[derive(Default)]
struct ObservedBands {
    rows: [usize; 4],
    sums: [u64; 4],
    fail: bool,
}

impl PartitionedKernel<Option<NativeBlock>> for ObservedBands {
    type Partition = usize;
    type Partial = (usize, u64);
    type Completion = [u64; 4];
    type Error = io::Error;

    fn partition_count(&self, _: BlockIdentity, _: &Option<NativeBlock>) -> io::Result<usize> {
        Ok(4)
    }
    fn partition(
        &self,
        _: BlockIdentity,
        _: &Option<NativeBlock>,
        ordinal: usize,
    ) -> io::Result<KernelPartition<usize>> {
        Ok(KernelPartition::exclusive(
            ordinal as u64,
            ordinal as u64,
            ordinal,
        ))
    }
    fn execute(
        &self,
        _: WorkIdentity,
        block: &Option<NativeBlock>,
        &band: &usize,
    ) -> io::Result<(usize, u64)> {
        let block = block.as_ref().unwrap();
        if self.fail && band == 0 {
            return Err(io::Error::from_raw_os_error(libc::EIO));
        }
        let values = ndarray::ArrayView3::from_shape(
            (block.metadata.len(), block.channels, block.correlations),
            &block.values,
        )
        .unwrap();
        let view = values.slice(ndarray::s![.., band..band + 2, ..]);
        assert_eq!(
            view.as_ptr(),
            block.values[band * block.correlations..].as_ptr()
        );
        Ok((
            band,
            view.iter().fold(0_u64, |sum, v| {
                sum.wrapping_add(v.re.to_bits())
                    .wrapping_add(v.im.to_bits())
            }),
        ))
    }
    fn commit(
        &mut self,
        _: WorkIdentity,
        block: &Option<NativeBlock>,
        (band, sum): Self::Partial,
        _: BoundedExecution<'_>,
    ) -> io::Result<()> {
        let block = block.as_ref().unwrap();
        self.rows[band] += block.metadata.len();
        self.sums[band] = self.sums[band].wrapping_add(sum);
        Ok(())
    }
    fn complete(self, _: BoundedExecution<'_>) -> io::Result<[u64; 4]> {
        assert!(self.rows.iter().all(|&rows| rows == 7));
        Ok(self.sums)
    }
}

#[test]
fn bounded_source_uses_one_team_and_one_decode_per_block_independent_of_workers() {
    let plan = plan(7, 9, 2, 3, 2);
    let (_directory, mut store) = store(plan);
    let mut expected = None;
    for workers in [1, 2, 4] {
        for slots in [1, 2] {
            let source = NativeSource::new(&mut store, 1..8).unwrap();
            let slot_bytes = source.slot_capacity_bytes();
            assert_eq!(
                slot_bytes,
                source.create_storage(0).unwrap().capacity_bytes() as u64
            );
            assert!(
                source.shared_capacity_bytes() >= plan.frame_bytes as u64 + plan.page_cache_bytes
            );
            let execution = BoundedStreamPlan::new::<usize, (usize, u64)>(
                slots,
                workers,
                slots as u64 * slot_bytes,
                4,
                0,
            )
            .unwrap()
            .with_maximum_logical_units_per_block(plan.block_rows)
            .unwrap();
            let result = execute_bounded(execution, 3, source, ObservedBands::default()).unwrap();
            let measurements = result.measurements;
            assert_eq!(
                result.source_completion.operations,
                plan.blocks() * (plan.tiles as u64 + 1)
            );
            assert_eq!(result.source_completion.bytes, plan.artifact_bytes);
            assert_eq!(
                measurements.source_read_operations,
                result.source_completion.operations
            );
            assert_eq!(
                measurements.logical_source_bytes,
                result.source_completion.bytes
            );
            assert_eq!(measurements.blocks_filled, plan.blocks());
            assert_eq!(measurements.logical_units_filled, plan.rows);
            assert_eq!(measurements.partitions_executed, 4 * plan.blocks());
            assert_eq!(measurements.commits_completed, 4 * plan.blocks());
            assert_eq!(
                measurements.worker_threads_started,
                if workers == 1 { 0 } else { workers as u64 }
            );
            assert!(measurements.peak_live_source_capacity_bytes <= slots as u64 * slot_bytes);
            assert!(measurements.peak_live_source_blocks <= slots);
            if let Some(expected) = expected {
                assert_eq!(result.kernel_completion, expected);
            } else {
                expected = Some(result.kernel_completion);
            }
        }
    }
}

#[test]
fn bounded_source_requires_terminal_coverage_and_preserves_io_and_worker_errors() {
    use std::sync::atomic::AtomicBool;
    let plan = plan(7, 9, 2, 3, 2);
    let (_directory, mut store) = store(plan);
    let make_plan = |source: &NativeSource<'_>| {
        BoundedStreamPlan::new::<usize, (usize, u64)>(2, 4, 2 * source.slot_capacity_bytes(), 4, 0)
            .unwrap()
            .with_maximum_logical_units_per_block(plan.block_rows)
            .unwrap()
    };
    assert!(
        NativeSource::new(&mut store, 1..8)
            .unwrap()
            .complete()
            .is_err()
    );
    {
        let mut source = NativeSource::new(&mut store, 1..8).unwrap();
        let mut storage = source.create_storage(0);
        let cancelled = AtomicBool::new(true);
        assert_eq!(
            source
                .fill(0, &mut storage, SourceFillCancellation::new(&cancelled))
                .unwrap_err()
                .kind(),
            io::ErrorKind::Interrupted
        );
        assert!(source.complete().is_err());
    }
    {
        let mut source = NativeSource::new(&mut store, 1..8).unwrap();
        let mut storage = source.create_storage(0);
        let cancelled = AtomicBool::new(false);
        assert!(
            source
                .fill(1, &mut storage, SourceFillCancellation::new(&cancelled))
                .is_err()
        );
        assert!(
            source
                .fill(0, &mut storage, SourceFillCancellation::new(&cancelled))
                .is_err()
        );
        assert!(source.complete().is_err());
    }
    let source = NativeSource::new(&mut store, 1..8).unwrap();
    let result = execute_bounded(
        make_plan(&source),
        0,
        source,
        ObservedBands {
            fail: true,
            ..ObservedBands::default()
        },
    )
    .unwrap_err();
    assert!(
        matches!(*result.cause, BoundedStreamError::Kernel(ref error) if error.raw_os_error() == Some(libc::EIO))
    );
    assert_eq!(result.measurements.worker_threads_started, 4);
    store
        .file
        .as_file()
        .set_len(plan.artifact_bytes - 1)
        .unwrap();
    let source = NativeSource::new(&mut store, 1..8).unwrap();
    let result =
        execute_bounded(make_plan(&source), 0, source, ObservedBands::default()).unwrap_err();
    assert!(
        matches!(*result.cause, BoundedStreamError::Source(ref error) if error.kind() == io::ErrorKind::UnexpectedEof)
    );
    assert_eq!(result.measurements.worker_threads_started, 4);
}

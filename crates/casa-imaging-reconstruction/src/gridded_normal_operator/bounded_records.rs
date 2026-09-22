// SPDX-License-Identifier: LGPL-3.0-or-later

//! Fixed-capacity whole-group reduction and borrowed frame encoding.

use std::{
    mem::size_of,
    time::{Duration, Instant},
};

use super::{
    AW_GROUP_END_BIT, AW_MUELLER_SHIFT, CHANNEL_KEY_MASK, GROUP_END_BIT,
    GriddedNormalOperatorStageTimings, GriddedNormalRecordLayout, RECORD_ROLE_SHIFT, RecordRole,
    ReducedRecordKey, SpectralOperatorError, TAP_KEY_BITS, TAP_KEY_MASK, TaylorRecordKey,
    canonical_zero_bits, record_bytes, valid_aw_coordinates,
};

#[derive(Clone, Copy)]
struct GroupRange {
    start: usize,
    end: usize,
    leading_key: [u64; 2],
}

#[derive(Clone, Copy)]
struct IndexedTaylor {
    key: TaylorRecordKey,
    ordinal: usize,
}

enum RawArena {
    Groups {
        records: Box<[ReducedRecordKey]>,
        ranges: Box<[GroupRange]>,
        records_used: usize,
        groups_used: usize,
    },
    Taylor {
        keys: Box<[IndexedTaylor]>,
        used: usize,
        moments: Box<[f64]>,
        plan: crate::block_normal::BlockNormalPlan,
    },
}

struct Frame {
    bytes: Box<[u8]>,
    record_bytes: usize,
    capacity: usize,
    used: usize,
    peak: usize,
    accumulation_output_planes: [u32; 2],
    flushes: u64,
    observe_timings: bool,
    sink_duration: Duration,
}

impl Frame {
    fn make_room(
        &mut self,
        records: usize,
        sink: &mut impl FnMut(&[u8], u64, [u32; 2]) -> Result<(), SpectralOperatorError>,
    ) -> Result<(), SpectralOperatorError> {
        if records > self.capacity {
            return Err(SpectralOperatorError::ResidencyOverflow);
        }
        if records > self.capacity - self.used {
            self.flush(sink)?;
        }
        Ok(())
    }

    fn append_record(&mut self) -> &mut [u8] {
        let start = self.used * self.record_bytes;
        self.used += 1;
        self.peak = self.peak.max(self.used);
        &mut self.bytes[start..start + self.record_bytes]
    }

    fn flush(
        &mut self,
        sink: &mut impl FnMut(&[u8], u64, [u32; 2]) -> Result<(), SpectralOperatorError>,
    ) -> Result<(), SpectralOperatorError> {
        if self.used != 0 {
            let next = self
                .flushes
                .checked_add(1)
                .ok_or(SpectralOperatorError::CoverageOverflow)?;
            let records =
                u64::try_from(self.used).map_err(|_| SpectralOperatorError::CoverageOverflow)?;
            let started = self.observe_timings.then(Instant::now);
            let result = sink(
                &self.bytes[..self.used * self.record_bytes],
                records,
                self.accumulation_output_planes,
            );
            if let Some(started) = started {
                self.sink_duration += started.elapsed();
            }
            result?;
            self.flushes = next;
            self.used = 0;
            self.accumulation_output_planes = [0; 2];
        }
        Ok(())
    }
}

pub(super) struct BoundedRecordEncoder {
    arena: RawArena,
    frames: Box<[Frame]>,
    band_output_plane_depth: usize,
    maximum_atom_records: usize,
    aw_projection: bool,
    peak_raw_records: usize,
    usable: bool,
    reduced_groups: u64,
    observe_timings: bool,
    timings: GriddedNormalOperatorStageTimings,
}

impl BoundedRecordEncoder {
    /// Exact heap payload of all fixed arenas; inline owner state is excluded.
    #[cfg(test)]
    pub(super) fn workspace_bytes(
        layout: GriddedNormalRecordLayout,
        aw_projection: bool,
        raw_capacity: usize,
        frame_record_capacity: usize,
        maximum_atom_records: usize,
    ) -> Result<usize, SpectralOperatorError> {
        Self::banded_workspace_bytes(
            layout,
            aw_projection,
            raw_capacity,
            frame_record_capacity,
            maximum_atom_records,
            1,
        )
    }

    pub(super) fn banded_workspace_bytes(
        layout: GriddedNormalRecordLayout,
        aw_projection: bool,
        raw_capacity: usize,
        frame_record_capacity: usize,
        maximum_atom_records: usize,
        band_count: usize,
    ) -> Result<usize, SpectralOperatorError> {
        if maximum_atom_records == 0
            || band_count == 0
            || raw_capacity < maximum_atom_records
            || frame_record_capacity < maximum_atom_records
        {
            return Err(SpectralOperatorError::ResidencyOverflow);
        }
        if matches!(layout, GriddedNormalRecordLayout::Taylor(_)) && aw_projection
            || matches!(layout, GriddedNormalRecordLayout::TaylorWithCoordinates(_))
                && !aw_projection
        {
            return Err(SpectralOperatorError::UnsupportedGriddedReplay);
        }
        let raw_bytes = match layout {
            GriddedNormalRecordLayout::Taylor(plan) => raw_capacity
                .checked_mul(size_of::<IndexedTaylor>())
                .and_then(|bytes| {
                    plan.normal_moment_count()
                        .checked_mul(3 * size_of::<f64>())
                        .and_then(|moments| bytes.checked_add(moments))
                }),
            _ => raw_capacity.checked_mul(size_of::<ReducedRecordKey>() + size_of::<GroupRange>()),
        }
        .ok_or(SpectralOperatorError::ResidencyOverflow)?;
        frame_record_capacity
            .checked_mul(record_bytes(layout, aw_projection)?)
            .and_then(|bytes| bytes.checked_add(size_of::<Frame>()))
            .and_then(|bytes| bytes.checked_mul(band_count))
            .and_then(|frame_bytes| raw_bytes.checked_add(frame_bytes))
            .filter(|&bytes| bytes <= isize::MAX as usize)
            .ok_or(SpectralOperatorError::ResidencyOverflow)
    }

    #[cfg(test)]
    pub(super) fn new(
        layout: GriddedNormalRecordLayout,
        aw_projection: bool,
        raw_capacity: usize,
        frame_record_capacity: usize,
        maximum_atom_records: usize,
    ) -> Result<Self, SpectralOperatorError> {
        Self::new_banded(
            layout,
            aw_projection,
            raw_capacity,
            frame_record_capacity,
            maximum_atom_records,
            1,
            usize::MAX,
        )
    }

    pub(super) fn new_banded(
        layout: GriddedNormalRecordLayout,
        aw_projection: bool,
        raw_capacity: usize,
        frame_record_capacity: usize,
        maximum_atom_records: usize,
        band_count: usize,
        band_output_plane_depth: usize,
    ) -> Result<Self, SpectralOperatorError> {
        if band_output_plane_depth == 0
            || (band_count > 1
                && (aw_projection
                    || !matches!(layout, GriddedNormalRecordLayout::ChannelLocal { .. })))
        {
            return Err(SpectralOperatorError::ResidencyOverflow);
        }
        Self::banded_workspace_bytes(
            layout,
            aw_projection,
            raw_capacity,
            frame_record_capacity,
            maximum_atom_records,
            band_count,
        )?;
        let arena = match layout {
            GriddedNormalRecordLayout::Taylor(plan) => RawArena::Taylor {
                keys: fixed_buffer(
                    raw_capacity,
                    IndexedTaylor {
                        key: TaylorRecordKey {
                            taps: 0,
                            frequency_hz: 0,
                            imaging_weight: 0,
                        },
                        ordinal: 0,
                    },
                )?,
                used: 0,
                moments: fixed_buffer(plan.normal_moment_count() * 3, 0.0)?,
                plan,
            },
            _ => RawArena::Groups {
                records: fixed_buffer(
                    raw_capacity,
                    ReducedRecordKey {
                        chart_ordinal: 0,
                        output_channel: 0,
                        taps: 0,
                        forward_real: 0,
                        forward_imaginary: 0,
                        imaging_weight: 0,
                        role: RecordRole::Both,
                        aw: None,
                    },
                )?,
                ranges: fixed_buffer(
                    raw_capacity,
                    GroupRange {
                        start: 0,
                        end: 0,
                        leading_key: [0; 2],
                    },
                )?,
                records_used: 0,
                groups_used: 0,
            },
        };
        let width = record_bytes(layout, aw_projection)?;
        let mut frames = Vec::new();
        frames
            .try_reserve_exact(band_count)
            .map_err(|_| SpectralOperatorError::ResidencyOverflow)?;
        for _ in 0..band_count {
            frames.push(Frame {
                bytes: fixed_buffer(frame_record_capacity * width, 0)?,
                record_bytes: width,
                capacity: frame_record_capacity,
                used: 0,
                peak: 0,
                accumulation_output_planes: [0; 2],
                flushes: 0,
                observe_timings: false,
                sink_duration: Duration::ZERO,
            });
        }
        Ok(Self {
            arena,
            frames: frames.into_boxed_slice(),
            band_output_plane_depth,
            maximum_atom_records,
            aw_projection,
            peak_raw_records: 0,
            usable: true,
            reduced_groups: 0,
            observe_timings: false,
            timings: GriddedNormalOperatorStageTimings::default(),
        })
    }

    #[cfg(test)]
    pub(super) fn frame_record_capacity(&self) -> usize {
        self.frames[0].capacity
    }
    pub(super) fn peak_raw_records(&self) -> usize {
        self.peak_raw_records
    }
    pub(super) fn peak_frame_records(&self) -> usize {
        self.frames
            .iter()
            .map(|frame| frame.peak)
            .max()
            .unwrap_or(0)
    }
    pub(super) fn flushes(&self) -> u64 {
        self.frames.iter().map(|frame| frame.flushes).sum()
    }

    pub(super) fn reduced_groups(&self) -> u64 {
        self.reduced_groups
    }

    pub(super) fn observe_timings(&mut self, enabled: bool) {
        self.observe_timings = enabled;
        for frame in &mut self.frames {
            frame.observe_timings = enabled;
        }
    }

    pub(super) fn timings(&self) -> GriddedNormalOperatorStageTimings {
        self.timings
    }

    pub(super) fn push_group(
        &mut self,
        group: &[ReducedRecordKey],
        sink: &mut impl FnMut(&[u8], u64, [u32; 2]) -> Result<(), SpectralOperatorError>,
    ) -> Result<(), SpectralOperatorError> {
        self.begin_operation()?;
        if group.is_empty() {
            return Err(SpectralOperatorError::InvalidGriddedRecord);
        }
        if group.len() > self.maximum_atom_records {
            return Err(SpectralOperatorError::ResidencyOverflow);
        }
        let RawArena::Groups {
            records,
            records_used,
            ..
        } = &self.arena
        else {
            return Err(SpectralOperatorError::InvalidGriddedRecord);
        };
        if group.len() > records.len() - records_used {
            self.reduce(sink)?;
        }
        let RawArena::Groups {
            records,
            ranges,
            records_used,
            groups_used,
        } = &mut self.arena
        else {
            unreachable!("group arena checked above")
        };
        let end = *records_used + group.len();
        records[*records_used..end].clone_from_slice(group);
        ranges[*groups_used] = GroupRange {
            start: *records_used,
            end,
            leading_key: [
                (u64::from(group[0].chart_ordinal) << 32) | u64::from(group[0].output_channel),
                group[0].taps,
            ],
        };
        *groups_used += 1;
        *records_used = end;
        self.peak_raw_records = self.peak_raw_records.max(end);
        self.usable = true;
        Ok(())
    }

    pub(super) fn push_taylor(
        &mut self,
        key: TaylorRecordKey,
        sink: &mut impl FnMut(&[u8], u64, [u32; 2]) -> Result<(), SpectralOperatorError>,
    ) -> Result<(), SpectralOperatorError> {
        self.begin_operation()?;
        let RawArena::Taylor { keys, used, .. } = &self.arena else {
            return Err(SpectralOperatorError::InvalidGriddedRecord);
        };
        if *used == keys.len() {
            self.reduce(sink)?;
        }
        let RawArena::Taylor { keys, used, .. } = &mut self.arena else {
            unreachable!("Taylor arena checked above")
        };
        keys[*used] = IndexedTaylor {
            key,
            ordinal: *used,
        };
        *used += 1;
        self.peak_raw_records = self.peak_raw_records.max(*used);
        self.usable = true;
        Ok(())
    }

    pub(super) fn finish(
        &mut self,
        sink: &mut impl FnMut(&[u8], u64, [u32; 2]) -> Result<(), SpectralOperatorError>,
    ) -> Result<(), SpectralOperatorError> {
        self.begin_operation()?;
        self.reduce(sink)?;
        for frame in &mut self.frames {
            frame.flush(sink)?;
        }
        Ok(())
    }

    fn begin_operation(&mut self) -> Result<(), SpectralOperatorError> {
        if !self.usable {
            return Err(SpectralOperatorError::GriddedCompilationPoisoned);
        }
        self.usable = false;
        Ok(())
    }

    fn reduce(
        &mut self,
        sink: &mut impl FnMut(&[u8], u64, [u32; 2]) -> Result<(), SpectralOperatorError>,
    ) -> Result<(), SpectralOperatorError> {
        let started = self.observe_timings.then(Instant::now);
        let encoding_before = self.timings.encoding_checksum;
        let sink_before = if self.observe_timings {
            self.sink_duration()
        } else {
            Duration::ZERO
        };
        let result = self.reduce_inner(sink);
        if let Some(started) = started {
            self.timings.grouping_reduction += started
                .elapsed()
                .saturating_sub(self.timings.encoding_checksum - encoding_before)
                .saturating_sub(self.sink_duration() - sink_before);
        }
        result
    }

    fn sink_duration(&self) -> Duration {
        self.frames.iter().map(|frame| frame.sink_duration).sum()
    }

    fn reduce_inner(
        &mut self,
        sink: &mut impl FnMut(&[u8], u64, [u32; 2]) -> Result<(), SpectralOperatorError>,
    ) -> Result<(), SpectralOperatorError> {
        match &mut self.arena {
            RawArena::Groups {
                records,
                ranges,
                records_used,
                groups_used,
            } => {
                let ranges = &mut ranges[..*groups_used];
                ranges.sort_unstable_by(|a, b| {
                    a.leading_key
                        .cmp(&b.leading_key)
                        .then_with(|| records[a.start..a.end].cmp(&records[b.start..b.end]))
                });
                let mut index = 0;
                while index < ranges.len() {
                    let range = ranges[index];
                    let group = &records[range.start..range.end];
                    let mut multiplicity = 0.0;
                    let mut compensation = 0.0;
                    loop {
                        let corrected = 1.0 - compensation;
                        let updated = multiplicity + corrected;
                        compensation = (updated - multiplicity) - corrected;
                        multiplicity = updated;
                        index += 1;
                        if index == ranges.len()
                            || records[ranges[index].start..ranges[index].end] != *group
                        {
                            break;
                        }
                    }
                    let band = if self.frames.len() == 1 {
                        0
                    } else {
                        group
                            .iter()
                            .filter(|record| record.role != RecordRole::Prediction)
                            .map(|record| record.output_channel as usize)
                            .min()
                            .unwrap_or(0)
                            / self.band_output_plane_depth
                    };
                    let frame = self
                        .frames
                        .get_mut(band)
                        .ok_or(SpectralOperatorError::InvalidGriddedRecord)?;
                    frame.make_room(group.len(), sink)?;
                    let started = self.observe_timings.then(Instant::now);
                    for (ordinal, record) in group.iter().enumerate() {
                        encode_record(
                            record,
                            multiplicity,
                            ordinal + 1 == group.len(),
                            self.aw_projection,
                            frame.append_record(),
                        )?;
                        if record.role != RecordRole::Prediction {
                            let range = &mut frame.accumulation_output_planes;
                            if range[0] == range[1] {
                                range[0] = record.output_channel;
                            } else {
                                range[0] = range[0].min(record.output_channel);
                            }
                            range[1] = range[1].max(record.output_channel + 1);
                        }
                    }
                    if let Some(started) = started {
                        self.timings.encoding_checksum += started.elapsed();
                    }
                    self.reduced_groups = self
                        .reduced_groups
                        .checked_add(1)
                        .ok_or(SpectralOperatorError::CoverageOverflow)?;
                }
                *records_used = 0;
                *groups_used = 0;
            }
            RawArena::Taylor {
                keys,
                used,
                moments,
                plan,
            } => {
                let keys = &mut keys[..*used];
                keys.sort_unstable_by_key(|entry| (entry.key.taps, entry.ordinal));
                let (sums, scratch) = moments.split_at_mut(plan.normal_moment_count());
                let (compensations, values) = scratch.split_at_mut(plan.normal_moment_count());
                let mut index = 0;
                while index < keys.len() {
                    let taps = keys[index].key.taps;
                    sums.fill(0.0);
                    compensations.fill(0.0);
                    while index < keys.len() && keys[index].key.taps == taps {
                        let key = keys[index].key;
                        plan.fill_normal_moment_weights(
                            f64::from_bits(key.frequency_hz),
                            f64::from_bits(key.imaging_weight),
                            values,
                        )
                        .map_err(|_| SpectralOperatorError::InvalidSample)?;
                        for ((sum, compensation), value) in sums
                            .iter_mut()
                            .zip(compensations.iter_mut())
                            .zip(values.iter())
                        {
                            let corrected = *value - *compensation;
                            let updated = *sum + corrected;
                            *compensation = (updated - *sum) - corrected;
                            *sum = updated;
                            if !sum.is_finite() || !compensation.is_finite() {
                                return Err(SpectralOperatorError::GeneratedNonfinite);
                            }
                        }
                        index += 1;
                    }
                    self.frames[0].make_room(1, sink)?;
                    let started = self.observe_timings.then(Instant::now);
                    let encoded = self.frames[0].append_record();
                    encoded[..8].copy_from_slice(&taps.to_le_bytes());
                    for (bytes, sum) in encoded[8..].chunks_exact_mut(8).zip(sums.iter()) {
                        bytes.copy_from_slice(&canonical_zero_bits(*sum).to_le_bytes());
                    }
                    if let Some(started) = started {
                        self.timings.encoding_checksum += started.elapsed();
                    }
                    self.reduced_groups = self
                        .reduced_groups
                        .checked_add(1)
                        .ok_or(SpectralOperatorError::CoverageOverflow)?;
                }
                *used = 0;
            }
        }
        Ok(())
    }
}

fn fixed_buffer<T: Clone>(length: usize, value: T) -> Result<Box<[T]>, SpectralOperatorError> {
    let mut values = Vec::new();
    values
        .try_reserve_exact(length)
        .map_err(|_| SpectralOperatorError::ResidencyOverflow)?;
    values.resize(length, value);
    Ok(values.into_boxed_slice())
}

fn encode_record(
    record: &ReducedRecordKey,
    multiplicity: f64,
    group_end: bool,
    aw_projection: bool,
    encoded: &mut [u8],
) -> Result<(), SpectralOperatorError> {
    let output_channel = u64::from(record.output_channel);
    if output_channel > CHANNEL_KEY_MASK || record.chart_ordinal >= 1 << 24 {
        return Err(SpectralOperatorError::InvalidGriddedRecord);
    }
    let forward_real = f64::from_bits(record.forward_real);
    let forward_imaginary = f64::from_bits(record.forward_imaginary);
    let imaging_weight = f64::from_bits(record.imaging_weight) * multiplicity;
    if aw_projection {
        if record.role != RecordRole::Both {
            return Err(SpectralOperatorError::InvalidGriddedRecord);
        }
        let aw = record
            .aw
            .ok_or(SpectralOperatorError::InvalidGriddedRecord)?;
        if aw.mueller_element >= 16 {
            return Err(SpectralOperatorError::InvalidGriddedRecord);
        }
        let coordinates = aw.into();
        if !valid_aw_coordinates(coordinates) {
            return Err(SpectralOperatorError::GeneratedNonfinite);
        }
        validate_numeric_record(forward_real, forward_imaginary, imaging_weight)?;
        let key = output_channel
            | (u64::from(record.chart_ordinal) << TAP_KEY_BITS)
            | (u64::from(aw.mueller_element) << AW_MUELLER_SHIFT)
            | if group_end { AW_GROUP_END_BIT } else { 0 };
        for (bytes, value) in encoded.chunks_exact_mut(8).zip([
            key,
            coordinates.frequency_hz.to_bits(),
            coordinates.uvw_m[0].to_bits(),
            coordinates.uvw_m[1].to_bits(),
            coordinates.uvw_m[2].to_bits(),
            coordinates.prediction_w_m.to_bits(),
            coordinates.parallactic_angle_deg.to_bits(),
            coordinates.pointing_phase_gradient_rad_per_grid_cell[0].to_bits(),
            coordinates.pointing_phase_gradient_rad_per_grid_cell[1].to_bits(),
            record.forward_real,
            record.forward_imaginary,
            imaging_weight.to_bits(),
        ]) {
            bytes.copy_from_slice(&value.to_le_bytes());
        }
    } else {
        if record.aw.is_some() {
            return Err(SpectralOperatorError::InvalidGriddedRecord);
        }
        validate_numeric_record(forward_real, forward_imaginary, imaging_weight)?;
        let key = (record.taps & TAP_KEY_MASK)
            | (output_channel << TAP_KEY_BITS)
            | ((record.role as u64) << RECORD_ROLE_SHIFT)
            | if group_end { GROUP_END_BIT } else { 0 };
        let route = u64::from(record.chart_ordinal) | ((record.taps >> 24) << 24);
        for (bytes, value) in encoded.chunks_exact_mut(8).zip([
            key,
            route,
            record.forward_real,
            record.forward_imaginary,
            imaging_weight.to_bits(),
        ]) {
            bytes.copy_from_slice(&value.to_le_bytes());
        }
    }
    Ok(())
}

fn validate_numeric_record(
    real: f64,
    imaginary: f64,
    weight: f64,
) -> Result<(), SpectralOperatorError> {
    if !real.is_finite()
        || !imaginary.is_finite()
        || (real == 0.0 && imaginary == 0.0)
        || !weight.is_finite()
        || weight.is_sign_negative()
    {
        Err(SpectralOperatorError::GeneratedNonfinite)
    } else {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::super::{
        AwRecordCoordinates, GriddedNormalOperatorBlockMeasurements, ReducedRecordGroup,
        encode_and_checksum_mode, encode_taylor_and_checksum, group_and_reduce_taylor,
    };
    use super::*;

    fn record(taps: u64) -> ReducedRecordKey {
        ReducedRecordKey {
            chart_ordinal: 0,
            output_channel: 0,
            taps,
            forward_real: 1.0_f64.to_bits(),
            forward_imaginary: (-0.25_f64).to_bits(),
            imaging_weight: 2.5_f64.to_bits(),
            role: RecordRole::Both,
            aw: None,
        }
    }

    fn oracle(groups: Vec<ReducedRecordGroup>, aw: bool) -> Vec<u8> {
        encode_and_checksum_mode(
            groups,
            aw,
            &mut GriddedNormalOperatorBlockMeasurements::default(),
        )
        .expect("reference codec")
        .0
        .into_vec()
    }

    #[test]
    fn bands_pack_after_coalescing_and_keep_cross_band_groups_whole() {
        let term = |channel, role| ReducedRecordKey {
            output_channel: channel,
            role,
            ..record(1)
        };
        let low = vec![
            term(9, RecordRole::Prediction),
            term(1, RecordRole::Accumulation),
            term(5, RecordRole::Accumulation),
        ];
        let high = vec![
            term(0, RecordRole::Prediction),
            term(8, RecordRole::Accumulation),
            term(9, RecordRole::Both),
        ];
        let layout = GriddedNormalRecordLayout::ChannelLocal { channels: 10 };
        let mut encoder = BoundedRecordEncoder::new_banded(layout, false, 9, 6, 3, 5, 2).unwrap();
        let mut frames = Vec::new();
        let mut sink = |bytes: &[u8], count, planes| {
            frames.push((bytes.to_vec(), count, planes));
            Ok(())
        };
        for group in [&high, &low, &low, &high] {
            encoder.push_group(group, &mut sink).unwrap();
        }
        encoder.finish(&mut sink).unwrap();
        assert_eq!(frames.len(), 2, "empty bands do not emit tails");
        assert_eq!((frames[0].1, frames[0].2), (3, [1, 6]));
        assert_eq!((frames[1].1, frames[1].2), (6, [8, 10]));
        assert_eq!(
            frames[0].0,
            oracle(
                vec![ReducedRecordGroup {
                    records: low,
                    multiplicity: 2.0
                }],
                false
            )
        );
        assert_eq!(
            frames[1].0,
            oracle(
                vec![
                    ReducedRecordGroup {
                        records: high.clone(),
                        multiplicity: 1.0
                    },
                    ReducedRecordGroup {
                        records: high,
                        multiplicity: 1.0
                    }
                ],
                false
            )
        );
        assert_eq!(encoder.reduced_groups(), 3);
        assert_eq!(
            BoundedRecordEncoder::banded_workspace_bytes(layout, false, 9, 6, 3, 5).unwrap(),
            9 * (size_of::<ReducedRecordKey>() + size_of::<GroupRange>())
                + 5 * (6 * 40 + size_of::<Frame>())
        );
    }

    #[test]
    fn band_capacity_flushes_respect_maximum_atom_and_tail_bound() {
        let layout = GriddedNormalRecordLayout::ChannelLocal { channels: 8 };
        let (bands, capacity, atom) = (4, 7, 3);
        let mut encoder =
            BoundedRecordEncoder::new_banded(layout, false, atom, capacity, atom, bands, 2)
                .unwrap();
        let mut counts = vec![Vec::new(); bands];
        let mut sink = |_: &[u8], count: u64, planes: [u32; 2]| {
            counts[planes[0] as usize / 2].push(count as usize);
            Ok(())
        };
        for index in 0..40 {
            let group = (0..atom)
                .map(|term| ReducedRecordKey {
                    output_channel: ((index % bands) * 2) as u32,
                    ..record((index * atom + term) as u64)
                })
                .collect::<Vec<_>>();
            encoder.push_group(&group, &mut sink).unwrap();
        }
        encoder.finish(&mut sink).unwrap();
        let mut total_bytes = 0;
        let mut frames = 0;
        for band in counts {
            assert!(!band.is_empty());
            for &count in &band[..band.len() - 1] {
                assert!((capacity - atom + 1..=capacity).contains(&count));
            }
            total_bytes += band.iter().map(|count| 72 + count * 40).sum::<usize>();
            frames += band.len();
        }
        assert!(frames <= total_bytes / (72 + (capacity - atom + 1) * 40) + bands);
        assert!(encoder.peak_frame_records() <= capacity);
    }

    #[test]
    fn frame_accumulation_ranges_exclude_prediction_neighbors_and_preserve_groups() {
        let term = |channel, role| ReducedRecordKey {
            output_channel: channel,
            role,
            ..record(1)
        };
        let groups = [
            vec![
                term(0, RecordRole::Prediction),
                term(3, RecordRole::Accumulation),
                term(9, RecordRole::Prediction),
            ],
            vec![
                term(7, RecordRole::Both),
                term(5, RecordRole::Accumulation),
                term(1, RecordRole::Prediction),
            ],
            vec![
                term(0, RecordRole::Prediction),
                term(1, RecordRole::Prediction),
                term(9, RecordRole::Prediction),
            ],
        ];
        let mut encoder = BoundedRecordEncoder::new(
            GriddedNormalRecordLayout::ChannelLocal { channels: 10 },
            false,
            3,
            3,
            3,
        )
        .expect("whole-group frame capacity");
        let mut frames = Vec::new();
        let mut sink = |bytes: &[u8], count, planes| {
            frames.push((bytes.to_vec(), count, planes));
            Ok(())
        };
        for group in &groups {
            encoder.push_group(group, &mut sink).expect("whole group");
        }
        encoder.finish(&mut sink).expect("finish");
        assert_eq!(frames.len(), 3);
        for ((bytes, count, planes), (group, expected)) in frames
            .into_iter()
            .zip(groups.into_iter().zip([[3, 4], [5, 8], [0, 0]]))
        {
            assert_eq!(count, 3);
            assert_eq!(planes, expected);
            assert_eq!(
                bytes,
                oracle(
                    vec![ReducedRecordGroup {
                        records: group,
                        multiplicity: 1.0,
                    }],
                    false
                )
            );
        }
    }

    #[test]
    fn minimum_capacities_preserve_whole_groups_and_term_order() {
        let groups = [vec![record(9), record(2)], vec![record(5), record(1)]];
        let mut encoder =
            BoundedRecordEncoder::new(GriddedNormalRecordLayout::Scalar, false, 2, 2, 2)
                .expect("U = R = a");
        let pointer = encoder.frames[0].bytes.as_ptr();
        let mut frames = Vec::new();
        let mut sink = |bytes: &[u8], records, _| {
            assert_eq!(bytes.as_ptr(), pointer);
            frames.push((bytes.to_vec(), records));
            Ok(())
        };
        for group in &groups {
            encoder.push_group(group, &mut sink).expect("whole group");
        }
        encoder.finish(&mut sink).expect("finish");
        assert_eq!(frames.len(), 2);
        for (frame, group) in frames.iter().zip(groups) {
            assert_eq!(frame.1, 2);
            assert_eq!(
                frame.0,
                oracle(
                    vec![ReducedRecordGroup {
                        records: group,
                        multiplicity: 1.0
                    }],
                    false
                )
            );
        }
        assert_eq!(encoder.peak_raw_records(), 2);
        assert_eq!(encoder.peak_frame_records(), 2);
        assert_eq!(encoder.frame_record_capacity(), 2);
        assert_eq!(encoder.flushes(), 2);
        assert_eq!(encoder.reduced_groups(), 2);
        assert_eq!(
            encoder.timings(),
            GriddedNormalOperatorStageTimings::default()
        );
        assert_eq!(encoder.frames[0].bytes.as_ptr(), pointer);
        assert_eq!(encoder.frames[0].bytes.len(), 80);
    }

    #[test]
    fn equal_whole_groups_coalesce_without_sorting_their_terms() {
        let mut prediction = record(9);
        prediction.role = RecordRole::Prediction;
        let mut accumulation = record(2);
        accumulation.role = RecordRole::Accumulation;
        let group = vec![prediction, accumulation];
        let reverse = vec![group[1], group[0]];
        let mut expected = vec![
            ReducedRecordGroup {
                records: group.clone(),
                multiplicity: 2.0,
            },
            ReducedRecordGroup {
                records: reverse.clone(),
                multiplicity: 1.0,
            },
        ];
        expected.sort_by(|a, b| a.records.cmp(&b.records));
        let mut encoder =
            BoundedRecordEncoder::new(GriddedNormalRecordLayout::Scalar, false, 6, 6, 2)
                .expect("bounded groups");
        let mut bytes = Vec::new();
        let mut sink = |frame: &[u8], count, _| {
            assert_eq!(count, 4);
            bytes.extend_from_slice(frame);
            Ok(())
        };
        for group in [&group, &reverse, &group] {
            encoder.push_group(group, &mut sink).expect("group");
        }
        encoder.finish(&mut sink).expect("finish");
        assert_eq!(bytes, oracle(expected, false));
        assert_eq!(encoder.reduced_groups(), 2);
    }

    #[test]
    fn cached_group_prefix_preserves_full_lexicographic_order_and_multiplicity() {
        let mut channel = record(0);
        channel.output_channel = 1;
        let mut coefficient = record(2);
        coefficient.forward_real = 2.0_f64.to_bits();
        let groups = vec![
            vec![channel],
            vec![record(2), record(9)],
            vec![coefficient],
            vec![record(2)],
            vec![record(1)],
            vec![record(2), record(1)],
            vec![record(2), record(9)],
        ];
        let count = groups.iter().map(Vec::len).sum();
        let mut expected: Vec<ReducedRecordGroup> = groups[..groups.len() - 1]
            .iter()
            .enumerate()
            .map(|(index, records)| ReducedRecordGroup {
                records: records.clone(),
                multiplicity: if index == 1 { 2.0 } else { 1.0 },
            })
            .collect();
        expected.sort_by(|a, b| a.records.cmp(&b.records));
        let mut encoder =
            BoundedRecordEncoder::new(GriddedNormalRecordLayout::Scalar, false, count, count, 2)
                .unwrap();
        let mut bytes = Vec::new();
        let mut sink = |frame: &[u8], _, _| {
            bytes.extend_from_slice(frame);
            Ok(())
        };
        for group in &groups {
            encoder.push_group(group, &mut sink).unwrap();
        }
        encoder.finish(&mut sink).unwrap();
        assert_eq!(bytes, oracle(expected, false));
        assert_eq!(encoder.reduced_groups(), 6);
        assert_eq!(
            BoundedRecordEncoder::workspace_bytes(
                GriddedNormalRecordLayout::Scalar,
                false,
                count,
                count,
                2,
            )
            .unwrap(),
            count * (size_of::<ReducedRecordKey>() + size_of::<GroupRange>() + 40)
                + size_of::<Frame>()
        );
    }

    #[test]
    fn frame_packing_crosses_raw_reductions_and_ignores_caller_partitions() {
        let groups = [2, 1, 4, 3, 6].map(|tap| [record(tap)]);
        let mut reference = None;
        for partition in [1, 2, 5] {
            let mut encoder =
                BoundedRecordEncoder::new(GriddedNormalRecordLayout::Scalar, false, 2, 3, 1)
                    .expect("bounded encoder");
            let mut frames = Vec::new();
            let mut sink = |bytes: &[u8], count, _| {
                frames.push((bytes.to_vec(), count));
                Ok(())
            };
            for chunk in groups.chunks(partition) {
                for group in chunk {
                    encoder.push_group(group, &mut sink).expect("group");
                }
            }
            encoder.finish(&mut sink).expect("finish");
            assert_eq!(
                frames.iter().map(|frame| frame.1).collect::<Vec<_>>(),
                vec![3, 2]
            );
            assert_eq!(
                frames
                    .iter()
                    .flat_map(|frame| frame.0.iter().copied())
                    .collect::<Vec<_>>(),
                oracle(
                    [1, 2, 3, 4, 6]
                        .map(|tap| ReducedRecordGroup {
                            records: vec![record(tap)],
                            multiplicity: 1.0,
                        })
                        .into(),
                    false
                )
            );
            if let Some(reference) = &reference {
                assert_eq!(&frames, reference);
            } else {
                reference = Some(frames);
            }
            assert_eq!(encoder.peak_raw_records(), 2);
            assert_eq!(encoder.peak_frame_records(), 3);
            assert_eq!(encoder.flushes(), 2);
        }
    }

    #[test]
    fn aw_borrowed_encoding_matches_the_existing_codec() {
        let mut key = record(0);
        key.chart_ordinal = 2;
        key.output_channel = 3;
        key.aw = Some(AwRecordCoordinates {
            frequency_hz: 1.4e9_f64.to_bits(),
            uvw_m: [1.0_f64.to_bits(), (-2.0_f64).to_bits(), 3.0_f64.to_bits()],
            prediction_w_m: (-4.0_f64).to_bits(),
            parallactic_angle_deg: 15.0_f64.to_bits(),
            pointing_phase_gradient_rad_per_grid_cell: [0.125_f64.to_bits(), (-0.25_f64).to_bits()],
            mueller_element: 15,
        });
        let mut encoder =
            BoundedRecordEncoder::new(GriddedNormalRecordLayout::Scalar, true, 2, 1, 1)
                .expect("AW encoder");
        let mut bytes = Vec::new();
        let mut sink = |frame: &[u8], count, _| {
            assert_eq!(count, 1);
            bytes.extend_from_slice(frame);
            Ok(())
        };
        for _ in 0..2 {
            encoder
                .push_group(std::slice::from_ref(&key), &mut sink)
                .expect("AW group");
        }
        encoder.finish(&mut sink).expect("finish AW");
        assert_eq!(
            bytes,
            oracle(
                vec![ReducedRecordGroup {
                    records: vec![key],
                    multiplicity: 2.0
                }],
                true
            )
        );
    }

    #[test]
    fn taylor_preserves_source_order_compensated_moments_inside_each_raw_chunk() {
        let plan = crate::block_normal::BlockNormalPlan::taylor(1.0e9, 3).expect("Taylor plan");
        let keys = [
            (9, 1.5e9, 1.0e16),
            (3, 1.4e9, 2.5),
            (9, 1.500_000_1e9, 1.0),
            (9, 0.5e9, 1.0e16),
            (9, 1.4e9, 3.0),
        ]
        .map(
            |(taps, frequency_hz, imaging_weight): (u64, f64, f64)| TaylorRecordKey {
                taps,
                frequency_hz: frequency_hz.to_bits(),
                imaging_weight: imaging_weight.to_bits(),
            },
        );
        for raw_capacity in [1, 2, keys.len()] {
            let mut encoder = BoundedRecordEncoder::new(
                GriddedNormalRecordLayout::Taylor(plan),
                false,
                raw_capacity,
                1,
                1,
            )
            .expect("Taylor encoder");
            let pointer = encoder.frames[0].bytes.as_ptr();
            let mut actual = Vec::new();
            let mut sink = |frame: &[u8], count, _| {
                assert_eq!(count, 1);
                assert_eq!(frame.as_ptr(), pointer);
                actual.extend_from_slice(frame);
                Ok(())
            };
            for key in keys {
                encoder.push_taylor(key, &mut sink).expect("Taylor key");
            }
            encoder.finish(&mut sink).expect("finish Taylor");
            let mut expected = Vec::new();
            for chunk in keys.chunks(raw_capacity) {
                let mut measurements = GriddedNormalOperatorBlockMeasurements::default();
                let (reduced, _) =
                    group_and_reduce_taylor::<false>(chunk.to_vec(), plan, &mut measurements)
                        .expect("reference Taylor reduction");
                expected.extend_from_slice(
                    &encode_taylor_and_checksum(reduced, plan, &mut measurements)
                        .expect("reference Taylor encoding")
                        .0,
                );
            }
            assert_eq!(actual, expected);
            assert_eq!(
                encoder.reduced_groups() as usize,
                actual.len() / encoder.frames[0].record_bytes
            );
            assert_eq!(encoder.peak_raw_records(), raw_capacity);
            assert_eq!(encoder.peak_frame_records(), 1);
        }
    }

    #[test]
    fn capacities_are_checked_before_use_and_workspace_matches_fixed_arenas() {
        for (raw, frame, atom) in [
            (0, 1, 1),
            (1, 0, 1),
            (1, 2, 2),
            (2, 1, 2),
            (1, 1, 0),
            (usize::MAX, 1, 1),
        ] {
            assert!(
                BoundedRecordEncoder::new(
                    GriddedNormalRecordLayout::Scalar,
                    false,
                    raw,
                    frame,
                    atom
                )
                .is_err()
            );
        }
        let mut encoder =
            BoundedRecordEncoder::new(GriddedNormalRecordLayout::Scalar, false, 3, 4, 2)
                .expect("valid capacity");
        assert_eq!(
            BoundedRecordEncoder::workspace_bytes(
                GriddedNormalRecordLayout::Scalar,
                false,
                3,
                4,
                2
            )
            .expect("workspace"),
            3 * (size_of::<ReducedRecordKey>() + size_of::<GroupRange>())
                + 4 * 40
                + size_of::<Frame>()
        );
        let mut sink = |_: &[u8], _, _| -> Result<(), SpectralOperatorError> {
            panic!("oversized atom never emits")
        };
        assert!(matches!(
            encoder.push_group(&[record(1), record(2), record(3)], &mut sink),
            Err(SpectralOperatorError::ResidencyOverflow)
        ));
        assert_eq!(encoder.peak_raw_records(), 0);
    }

    #[test]
    fn sink_failure_poisons_pending_records_without_retrying_the_sink() {
        let mut encoder =
            BoundedRecordEncoder::new(GriddedNormalRecordLayout::Scalar, false, 1, 1, 1)
                .expect("minimal encoder");
        let mut calls = 0;
        let mut sink = |_: &[u8], _, _| {
            calls += 1;
            Err(SpectralOperatorError::DiagnosticStop)
        };
        encoder
            .push_group(&[record(1)], &mut sink)
            .expect("first raw atom");
        encoder
            .push_group(&[record(2)], &mut sink)
            .expect("first frame remains buffered");
        assert!(matches!(
            encoder.push_group(&[record(3)], &mut sink),
            Err(SpectralOperatorError::DiagnosticStop)
        ));
        assert!(matches!(
            encoder.finish(&mut sink),
            Err(SpectralOperatorError::GriddedCompilationPoisoned)
        ));
        assert!(matches!(
            encoder.push_group(&[record(4)], &mut sink),
            Err(SpectralOperatorError::GriddedCompilationPoisoned)
        ));
        assert_eq!(calls, 1);
        assert_eq!(encoder.flushes(), 0);
    }

    #[test]
    fn optional_phase_timings_exclude_synchronous_sink_duration() {
        let mut encoder =
            BoundedRecordEncoder::new(GriddedNormalRecordLayout::Scalar, false, 1, 1, 1)
                .expect("timed encoder");
        encoder.observe_timings(true);
        let mut sink = |_: &[u8], _, _| {
            std::thread::sleep(Duration::from_millis(2));
            Ok(())
        };
        let started = Instant::now();
        for tap in [1, 2, 3] {
            encoder
                .push_group(&[record(tap)], &mut sink)
                .expect("timed group");
        }
        encoder.finish(&mut sink).expect("timed finish");
        let elapsed = started.elapsed();
        let timings = encoder.timings();
        assert!(encoder.sink_duration() >= Duration::from_millis(6));
        assert!(
            timings.grouping_reduction + timings.encoding_checksum + encoder.sink_duration()
                <= elapsed
        );
        assert_eq!(timings.record_key_construction, Duration::ZERO);
        assert_eq!(timings.completion, Duration::ZERO);
    }
}

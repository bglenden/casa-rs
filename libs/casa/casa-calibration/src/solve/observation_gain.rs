// SPDX-License-Identifier: LGPL-3.0-or-later
//! Time-binned G solving through the shared observation traversal.

use std::collections::{BTreeMap, BTreeSet};
use std::path::Path;

use casa_core::measures::MjdHighPrec;
use casa_ms::MsSelection;
use casa_ms::observation_io::{
    CalibrationObservation, is_msv4_store, with_calibration_observation,
};
use casa_observation::{
    AccessRequest, BatchBuffers, CorrelationRequest, FieldSelection, GroupKey, Grouping,
    ObservationError, PartitionId, ReadEvent, Roles, SigmaFallback, TimeSelection, VisibilityBatch,
    WeightRequest,
};
use ndarray::{ArrayView2, s};

use super::grouping::{
    SolveAccumulator, SolveGroupOptions, SolveSampleRow, SolveSamples, all_antenna_ids,
    resolve_refants, time_bin, time_bin_origin,
};
use super::{
    GainSolveCombine, GainSolveError, GainSolveInterval, GainSolveMode, GainSolveModelSource,
    GainSolveReport, GainSolveRequest, GainType, RefAntSelector, SolvedGains,
    apply_reference_antenna, preaverage_span, refant_distance_order, solve_group_with_stats,
    spectral_window_ids, tally_references,
};
use crate::output::CalibrationOutputMode;
use crate::selected_data::SelectedDataBuilder;
use crate::solve_stats::SolveStatsBuilder;

pub(crate) fn supports(request: &GainSolveRequest) -> bool {
    matches!(request.solve_interval, GainSolveInterval::Seconds(seconds) if seconds.is_finite() && seconds > 0.0)
        && request.gain_type == GainType::G
        && request.solve_mode == GainSolveMode::AmplitudePhase
        && request.combine == GainSolveCombine::default()
        && only_cross_correlations(&request.selection)
        && request.preaverage_seconds <= 0.0
        && !request.correlation_dependent_flags
        && request.prior_calibration_tables.is_empty()
        && !request.parang
        && !request.normalize_average_amplitude
        && !request.robustness.enabled()
        && request.model_source == GainSolveModelSource::PointSource
        && request.smodel[0].is_finite()
        && request.smodel[0] > 0.0
        && request.smodel[1..] == [0.0; 3]
}

fn only_cross_correlations(selection: &MsSelection) -> bool {
    let predicate = selection.msselect.as_deref();
    matches!(predicate, None | Some("ANTENNA1!=ANTENNA2"))
        && *selection
            == MsSelection {
                msselect: selection.msselect.clone(),
                ..MsSelection::default()
            }
}

pub(crate) fn solve(
    path: &Path,
    request: &GainSolveRequest,
    mode: CalibrationOutputMode,
) -> Result<Option<GainSolveReport>, GainSolveError> {
    let is_msv4 = is_msv4_store(path);
    let opening = crate::timing::Phase::start("gain_observation_open");
    let result = with_calibration_observation(path, |input| {
        drop(opening);
        if let Some(what) = unrepresented_layout(&input) {
            return if is_msv4 {
                Err(ObservationError::Unsupported { what }.into())
            } else {
                Ok(None)
            };
        }
        let gather = crate::timing::Phase::start("gain_observation_gather");
        let solved = read_solutions(&input, request, input.available_bytes)?;
        drop(gather);
        super::writer::write_gain_caltable_named(
            input.subtables,
            path.display().to_string(),
            request,
            &solved,
            mode,
        )
        .map(Some)
    });
    match result {
        Ok(outcome) => outcome,
        Err(ObservationError::Unsupported { .. } | ObservationError::InvalidStore { .. })
            if !is_msv4 =>
        {
            Ok(None)
        }
        Err(error) => Err(error.into()),
    }
}

fn unrepresented_layout(input: &CalibrationObservation<'_>) -> Option<&'static str> {
    let catalogue = input.observation.catalogue();
    let mut groups = BTreeSet::new();
    for (partition, persisted) in catalogue.partitions.iter().zip(input.partitions) {
        let Some(array) = persisted.array else {
            return Some("multiple arrays in one calibration visibility partition");
        };
        if partition.scans.is_empty()
            || partition
                .scans
                .iter()
                .any(|id| catalogue.scans[id.0 as usize].number.is_none())
        {
            return Some("a calibration partition without numeric scan identities");
        }
        if partition.data_group("base").is_none() {
            return Some("a calibration partition without a base visibility data group");
        }
        for &field in &partition.fields {
            for &scan in &partition.scans {
                if !groups.insert((
                    array,
                    persisted.data_description,
                    partition.observation,
                    field,
                    scan,
                )) {
                    return Some("calibration solution groups spanning observation partitions");
                }
            }
        }
    }
    None
}

struct GainAccumulator<'a> {
    request: &'a GainSolveRequest,
    groups: BTreeMap<GroupKey, ScanAccumulator>,
    width_s: f64,
    rows: Vec<super::kernel::SolutionRow>,
    stats: SolveStatsBuilder,
    available_antennas: BTreeSet<i32>,
    preferred: i32,
    attempted_spws: BTreeSet<i32>,
    selection: SelectedDataBuilder,
    flags: Vec<bool>,
}

struct ScanAccumulator {
    origin: f64,
    bin: u64,
    interval: SolveAccumulator,
}

fn read_solutions(
    input: &CalibrationObservation<'_>,
    request: &GainSolveRequest,
    batch_bytes: usize,
) -> Result<SolvedGains, GainSolveError> {
    let GainSolveInterval::Seconds(width_s) = request.solve_interval else {
        return Err(GainSolveError::UnsupportedSolveInterval {
            solve_interval: request.solve_interval,
        });
    };
    let mut refants = resolve_refants(input.subtables, &request.refant)?.into_iter();
    let preferred = refants.next().unwrap_or(-1);
    let available_antennas = all_antenna_ids(input.subtables)?;
    let mut state = GainAccumulator {
        request,
        groups: BTreeMap::new(),
        width_s,
        rows: Vec::new(),
        stats: SolveStatsBuilder::new(
            request.gain_type.polarizations(),
            spectral_window_ids(input.subtables)?,
            &available_antennas,
        ),
        available_antennas,
        preferred,
        attempted_spws: BTreeSet::new(),
        selection: SelectedDataBuilder::default(),
        flags: Vec::new(),
    };
    for id in input.observation.catalogue().partition_ids() {
        read_partition(input, id, batch_bytes, &mut state)?;
    }

    if preferred >= 0 {
        let strict = matches!(
            request.refant,
            RefAntSelector::Expression { strict: true, .. }
        );
        let mut alternates = refants.collect::<Vec<_>>();
        if !strict {
            let last = alternates.last().copied().unwrap_or(preferred);
            let mut nearby = refant_distance_order(input.subtables, last)?;
            nearby.retain(|id| *id != preferred && !alternates.contains(id));
            alternates.extend(nearby);
        }
        apply_reference_antenna(&mut state.rows, preferred, &alternates, strict);
    }
    if state.selection.is_empty() {
        return Err(GainSolveError::EmptySelection);
    }
    tally_references(&state.rows, &mut state.stats);
    Ok(SolvedGains {
        selected_data: state.selection.finish(input.subtables)?,
        rows: state.rows,
        refant_id: preferred,
        attempted_spw_ids: state.attempted_spws,
        channel_selections: BTreeMap::new(),
        solve_stats: state.stats.finish(),
    })
}

fn read_partition(
    input: &CalibrationObservation<'_>,
    id: PartitionId,
    budget: usize,
    state: &mut GainAccumulator<'_>,
) -> Result<(), GainSolveError> {
    let partition = input.observation.catalogue().partition(id);
    let persisted = &input.partitions[id.0 as usize];
    let request = partition_access(input, id, budget);
    let mut traversal = input.observation.bind_read(&request)?;
    let mut buffers = BatchBuffers::default();
    let channels: Vec<_> = request.channels.clone().collect();
    let codes: Vec<_> = partition
        .correlations
        .iter()
        .map(|code| code.casacore_code())
        .collect();
    while let Some(event) = traversal.next(&mut buffers)? {
        match event {
            ReadEvent::GroupEnd(key) => state.finish_group(key),
            ReadEvent::Batch(batch) => {
                for fragment in batch.fragments {
                    state.observe(
                        &batch,
                        fragment,
                        &channels,
                        &codes,
                        (
                            persisted.window as i32,
                            persisted.data_description as i32,
                            partition.observation.0 as i32,
                        ),
                    )?;
                    if fragment.position.closes() {
                        state.finish_group(fragment.key);
                    }
                }
            }
        }
    }
    if !state.groups.is_empty() {
        return Err(ObservationError::InvalidBatch {
            reason: "calibration scan ended without closing its groups".to_owned(),
        }
        .into());
    }
    Ok(())
}

fn partition_access(
    input: &CalibrationObservation<'_>,
    id: PartitionId,
    budget: usize,
) -> AccessRequest {
    let partition = input.observation.catalogue().partition(id);
    AccessRequest {
        partitions: vec![id],
        data_group: "base".to_owned(),
        roles: Roles {
            uvw: false,
            exposure: false,
            ..Roles::ALL
        },
        channels: 0..partition.frequencies_hz.len(),
        correlations: CorrelationRequest::ALL,
        times: TimeSelection::All,
        fields: FieldSelection::All,
        weights: WeightRequest::InverseSigmaSquared {
            fallback: SigmaFallback::StoredWeights,
        },
        grouping: Grouping::PerScan {
            combine_fields: false,
        },
        // Leave room for the fragment's validity cube, which holds fewer
        // bytes than the visibility batch it is built from.
        batch_bytes: budget / 2,
    }
}

impl GainAccumulator<'_> {
    fn observe(
        &mut self,
        batch: &VisibilityBatch<'_>,
        fragment: &casa_observation::GroupFragment,
        channels: &[usize],
        codes: &[i32],
        (spw, description, observation): (i32, i32, i32),
    ) -> Result<(), GainSolveError> {
        let values = batch.group.values.expect("the solve requests values");
        let weights = batch.group.weights.expect("the solve requests weights");
        let centroid = batch.timing.centroid.expect("the solve requests centroids");
        let visibility = batch
            .sample_window(fragment.samples.clone())
            .expect("a fragment is whole times");
        visibility
            .validity()
            .mask_into(&mut self.flags, visibility.shape());
        let cell = channels.len() * codes.len();
        let per_time = batch.samples_per_time();
        let zero = MjdHighPrec::new(0.0, 0.0);
        let options = SolveGroupOptions {
            input_rows: Default::default(),
            gain_type: self.request.gain_type,
            model_source: self.request.model_source,
            point_stokes: self.request.smodel,
            rotate_model: false,
            solve_interval: self.request.solve_interval,
            combine: self.request.combine,
            legacy_g_receptors: false,
            channelized: false,
            polynomial_bandpass: false,
            preaverage_max_span: preaverage_span(
                self.request.solve_interval,
                self.request.preaverage_seconds,
            ),
            robust_seed: false,
            correlation_dependent_flags: false,
        };
        for sample in fragment.samples.clone() {
            let coordinates = batch.sample(sample);
            let weights = weights.slice(s![sample, .., ..]).reversed_axes();
            if coordinates.baseline.is_auto() || weights.iter().all(|weight| weight.is_nan()) {
                continue;
            }
            let field = batch.field[sample / per_time].0 as i32;
            let scan = batch
                .scan(sample)
                .and_then(|id| batch.catalogue.scans[id.0 as usize].number)
                .ok_or(ObservationError::Unsupported {
                    what: "a solve sample without a numeric scan",
                })?;
            let antenna1 = coordinates.baseline.antenna1.0 as i32;
            let antenna2 = coordinates.baseline.antenna2.0 as i32;
            self.selection
                .add([antenna1, antenna2], field, observation, scan, spw);
            let time = batch.time.seconds_since(sample / per_time, zero);
            self.advance_group(fragment.key, time, (field, spw, observation));
            let flags_from = (sample - fragment.samples.start) * cell;
            let flags = ArrayView2::from_shape(
                (channels.len(), codes.len()),
                &self.flags[flags_from..flags_from + cell],
            )
            .expect("one complete flag cell")
            .reversed_axes();
            let group = self.groups.get_mut(&fragment.key).expect("open scan");
            group.interval.observe_samples(
                &SolveSampleRow {
                    field_id: field,
                    data_desc_id: description,
                    antenna1,
                    antenna2,
                    time_seconds: time,
                    time_centroid_seconds: centroid.seconds_since(sample, zero),
                    interval_seconds: batch
                        .interval_s(sample)
                        .expect("the solve requests intervals"),
                    scan_number: scan,
                },
                &SolveSamples {
                    data: values.slice(s![sample, .., ..]).reversed_axes(),
                    flags,
                    weights,
                    model: None,
                    point_model: None,
                },
                codes,
                channels,
                &options,
            )?;
        }
        Ok(())
    }

    fn advance_group(&mut self, key: GroupKey, time: f64, (field, spw, obs): (i32, i32, i32)) {
        let group = self.groups.entry(key).or_insert_with(|| {
            let origin = time_bin_origin(time);
            ScanAccumulator {
                origin,
                bin: time_bin(time, origin, self.width_s),
                interval: SolveAccumulator::new(field, spw, obs),
            }
        });
        let bin = time_bin(time, group.origin, self.width_s);
        if bin != group.bin {
            group.bin = bin;
            let previous =
                std::mem::replace(&mut group.interval, SolveAccumulator::new(field, spw, obs));
            self.finish_interval(previous);
        }
    }

    fn finish_group(&mut self, key: GroupKey) {
        if let Some(group) = self.groups.remove(&key) {
            self.finish_interval(group.interval);
        }
    }

    fn finish_interval(&mut self, mut group: SolveAccumulator) {
        if group.attempted {
            self.attempted_spws.insert(group.spw_id);
        }
        group.finalize_for_solve(self.request.solve_mode, false);
        if let Some(rows) = solve_group_with_stats(
            group,
            &self.available_antennas,
            self.request,
            self.preferred,
            false,
            &mut self.stats,
        ) {
            self.rows.extend(rows);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn gaincal_keeps_time_bins_across_physical_batch_boundaries() {
        let root = tempfile::tempdir().unwrap();
        let archive =
            include_bytes!("../../../../casacore/casa-ms/tests/fixtures/ms_trial/ms_trial.ms.tgz");
        tar::Archive::new(flate2::read::GzDecoder::new(&archive[..]))
            .unpack(root.path())
            .unwrap();
        let request = super::super::batch_trial::gain_request(root.path().join("unused.cal"));
        with_calibration_observation(&root.path().join("ms_trial.ms"), |input| {
            let id = PartitionId(0);
            let unlimited = partition_access(&input, id, input.available_bytes);
            let estimate = input.observation.estimate(&unlimited).unwrap();
            let one_time_limit = estimate.partitions[0].held_bytes_per_time() * 2;
            let split = partition_access(&input, id, one_time_limit);
            let mut reads = input.observation.bind_read(&split).unwrap();
            let mut buffers = BatchBuffers::default();
            let mut fragments = 0;
            while let Some(event) = reads.next(&mut buffers).unwrap() {
                if let ReadEvent::Batch(data) = event {
                    assert_eq!(data.times(), 1);
                    fragments += data.fragments.len();
                }
            }
            assert!(fragments > 2);

            let whole = read_solutions(&input, &request, input.available_bytes).unwrap();
            let fragmented = read_solutions(&input, &request, one_time_limit).unwrap();
            assert_eq!(whole.selected_data, fragmented.selected_data);
            assert_eq!(whole.solve_stats, fragmented.solve_stats);
            assert_eq!(whole.rows.len(), 10);
            assert_eq!(whole.rows.len(), fragmented.rows.len());
            for (whole, split) in whole.rows.iter().zip(&fragmented.rows) {
                assert_eq!(whole.time_seconds, split.time_seconds);
                assert_eq!(whole.antenna_id, split.antenna_id);
                assert_eq!(whole.flags, split.flags);
                assert_eq!(whole.gains, split.gains);
            }
        })
        .unwrap();
    }
}

// SPDX-License-Identifier: LGPL-3.0-or-later
//! Time-binned G solving through the shared observation traversal.

use std::collections::{BTreeMap, BTreeSet};
use std::path::Path;

use casa_core::measures::MjdHighPrec;
use casa_core::memory::btree_node_bytes;
use casa_ms::observation_io::{
    CalibrationObservation, CalibrationPartition, is_msv4_store, with_calibration_observation,
};
use casa_ms::selection::syntax::parse_numeric_row_id_ranges;
use casa_ms::selection::{FieldSelectorRow, resolve_field_selector_rows};
use casa_ms::{MeasurementSet, MsSelection};
use casa_observation::{
    AccessRequest, BatchBuffers, Catalogue, CorrelationRequest, FieldId, FieldSelection, GroupKey,
    Grouping, ObservationError, ObservationId, PartitionEntry, PartitionId, ReadEvent, Roles,
    ScanId, SigmaFallback, TimeSelection, VisibilityBatch, WeightRequest,
};
use ndarray::{ArrayView2, s};

use super::grouping::{
    SolveAccumulator, SolveGroupOptions, SolveSampleRow, SolveSamples, all_antenna_ids,
    resolve_refants, time_bin, time_bin_origin,
};
use super::{
    GainSolveCombine, GainSolveError, GainSolveInterval, GainSolveModelSource, GainSolveReport,
    GainSolveRequest, GainType, LegacyRefantSlots, RefAntSelector, SolvedGains,
    apply_reference_antenna, finish_gain_amplitudes, legacy_gain_receptors, preaverage_span,
    record_legacy_refant_slots, refant_distance_order, solve_group_with_stats, spectral_window_ids,
    tally_references,
};
use crate::output::CalibrationOutputMode;
use crate::selected_data::SelectedDataBuilder;
use crate::solve_stats::SolveStatsBuilder;

pub(crate) fn supports(request: &GainSolveRequest) -> bool {
    matches!(request.solve_interval, GainSolveInterval::Seconds(seconds) if seconds.is_finite() && seconds > 0.0)
        && request.gain_type == GainType::G
        && request.combine == GainSolveCombine::default()
        && supported_selection(&request.selection)
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

fn supported_selection(selection: &MsSelection) -> bool {
    let predicate = selection.msselect.as_deref();
    matches!(predicate, None | Some("ANTENNA1!=ANTENNA2"))
        && *selection
            == MsSelection {
                field: selection.field.clone(),
                scan: selection.scan.clone(),
                msselect: selection.msselect.clone(),
                ..MsSelection::default()
            }
}

#[derive(Default)]
struct GainObservationSelection {
    fields: FieldSelection,
    scans: Option<Vec<ScanId>>,
}

impl GainObservationSelection {
    fn resolve(
        input: &CalibrationObservation<'_>,
        request: &MsSelection,
    ) -> Result<Self, GainSolveError> {
        let catalogue = input.observation.catalogue();
        let field = request
            .field
            .as_deref()
            .filter(|text| !text.trim().is_empty());
        let scan = request.scan.as_deref().filter(|text| !text.is_empty());
        let required = selector_bytes(catalogue, field, scan);
        if required > input.available_bytes {
            return Err(ObservationError::OverBudget {
                what: "gaincal field and scan selectors",
                required,
                budget: input.available_bytes,
            }
            .into());
        }
        let mut selection = Self::default();
        if let Some(field) = field {
            let rows = catalogue
                .fields
                .iter()
                .map(|entry| FieldSelectorRow {
                    name: &entry.name,
                    code: entry.code.as_deref(),
                    flagged: entry.flagged.unwrap_or(false),
                })
                .collect::<Vec<_>>();
            let ids = resolve_field_selector_rows(&rows, field)
                .map_err(|error| selector_error("field", error))?;
            selection.fields =
                FieldSelection::Only(ids.into_iter().map(|id| FieldId(id as u32)).collect());
        }
        if let Some(scan) = scan {
            let ranges = parse_numeric_row_id_ranges(scan, "scan")
                .map_err(|error| selector_error("scan", error))?;
            let ids = catalogue
                .scans
                .iter()
                .enumerate()
                .filter_map(|(id, entry)| {
                    let number = entry.number?;
                    let index = ranges.partition_point(|&(_, end)| end < number);
                    ranges
                        .get(index)
                        .filter(|&&(start, _)| start <= number)
                        .map(|_| ScanId(id as u32))
                })
                .collect::<Vec<_>>();
            if ids.is_empty() {
                return Err(selector_error(
                    "scan",
                    format!("{scan:?} matches no observation scan"),
                ));
            }
            selection.scans = Some(ids);
        }
        Ok(selection)
    }

    fn holds_scan(&self, scan: Option<ScanId>) -> bool {
        self.scans
            .as_ref()
            .is_none_or(|ids| scan.is_some_and(|id| ids.binary_search(&id).is_ok()))
    }

    fn holds_partition(
        &self,
        partition: &PartitionEntry,
        persisted: &CalibrationPartition<'_>,
    ) -> bool {
        let fields = matches!(self.fields, FieldSelection::All);
        let scans = self.scans.is_none();
        (fields || partition.fields.iter().any(|&id| self.fields.holds(id)))
            && (scans || partition.scans.iter().any(|&id| self.holds_scan(Some(id))))
            && (fields
                || scans
                || persisted.fields_and_scans.is_none_or(|(fields, scans)| {
                    fields.iter().zip(scans).any(|(&field, &scan)| {
                        self.fields.holds(field) && self.holds_scan(Some(scan))
                    })
                }))
    }

    fn retained_bytes(&self) -> usize {
        let fields = match &self.fields {
            // The active reader also receives its own copy of the field list.
            FieldSelection::Only(ids) => 2 * ids.capacity() * size_of::<FieldId>(),
            FieldSelection::All => 0,
        };
        fields
            + self
                .scans
                .as_ref()
                .map_or(0, |scans| scans.capacity() * size_of::<ScanId>())
    }
}

fn selector_error(parameter: &'static str, reason: impl std::fmt::Display) -> GainSolveError {
    GainSolveError::ObservationSelector {
        parameter,
        reason: reason.to_string(),
    }
}

fn selector_bytes(catalogue: &Catalogue, field: Option<&str>, scan: Option<&str>) -> usize {
    let fields = field.map_or(0, |selector| {
        // Each term can collect all FIELD rows before duplicates are removed.
        let ids_per_row = selector.split(',').count().saturating_mul(2).max(4);
        catalogue.fields.len().saturating_mul(
            size_of::<FieldSelectorRow<'_>>() + ids_per_row.saturating_mul(size_of::<i32>()),
        )
    });
    let scans = scan.map_or(0, |selector| {
        selector
            .split(',')
            .count()
            .saturating_mul(2)
            .max(4)
            .saturating_mul(size_of::<(i32, i32)>())
            .saturating_add(
                catalogue
                    .scans
                    .len()
                    .saturating_mul(2)
                    .max(4)
                    .saturating_mul(size_of::<ScanId>()),
            )
    });
    fields.saturating_add(scans)
}

pub(crate) fn solve(
    path: &Path,
    request: &GainSolveRequest,
    mode: CalibrationOutputMode,
) -> Result<Option<GainSolveReport>, GainSolveError> {
    let is_msv4 = is_msv4_store(path);
    let opening = crate::timing::Phase::start("gain_observation_open");
    let result = with_calibration_observation(path, admit_msv2, |input| {
        drop(opening);
        let selection = match GainObservationSelection::resolve(&input, &request.selection) {
            Err(GainSolveError::ObservationSelector { .. }) if !is_msv4 => return Ok(None),
            result => result?,
        };
        let budget = input
            .available_bytes
            .saturating_sub(selection.retained_bytes());
        if let Some(what) = unrepresented_layout(
            input.observation.catalogue(),
            input.partitions,
            budget,
            &selection,
        )? {
            return if is_msv4 {
                Err(ObservationError::Unsupported { what }.into())
            } else {
                Ok(None)
            };
        }
        let gather = crate::timing::Phase::start("gain_observation_gather");
        let legacy_receptors = legacy_gain_receptors(request, mode)?;
        let solved = match read_solutions(&input, request, budget, legacy_receptors, &selection) {
            Err(GainSolveError::EmptySelection)
                if !is_msv4
                    && (request.selection.field.is_some() || request.selection.scan.is_some()) =>
            {
                return Ok(None);
            }
            result => result?,
        };
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

fn admit_msv2(ms: &MeasurementSet) -> Result<(), ObservationError> {
    // CASA probes jointly present spectral weights and sigmas per visibility
    // buffer. The observation weight policy is per partition.
    if ms.main_table().schema().is_some_and(|schema| {
        schema.contains_column("WEIGHT_SPECTRUM") && schema.contains_column("SIGMA_SPECTRUM")
    }) {
        return Err(ObservationError::Unsupported {
            what: "per-buffer spectral weights and sigmas in MSv2 gaincal",
        });
    }
    Ok(())
}

fn unrepresented_layout(
    catalogue: &Catalogue,
    partitions: &[CalibrationPartition<'_>],
    budget: usize,
    selection: &GainObservationSelection,
) -> Result<Option<&'static str>, ObservationError> {
    type SolveIdentity = (i32, u32, ObservationId, FieldId, ScanId);
    let mut owners = BTreeMap::<SolveIdentity, usize>::new();
    for (id, (partition, persisted)) in catalogue.partitions.iter().zip(partitions).enumerate() {
        if !selection.holds_partition(partition, persisted) {
            continue;
        }
        let Some(array) = persisted.array else {
            return Ok(Some(
                "multiple arrays in one calibration visibility partition",
            ));
        };
        if partition.scans.is_empty()
            || partition
                .scans
                .iter()
                .any(|id| catalogue.scans[id.0 as usize].number.is_none())
        {
            return Ok(Some(
                "a calibration partition without numeric scan identities",
            ));
        }
        if partition.data_group("base").is_none() {
            return Ok(Some(
                "a calibration partition without a base visibility data group",
            ));
        }
        let Some((fields, scans)) = persisted.fields_and_scans else {
            return Ok(Some(
                "a calibration data group without per-time field and scan identities",
            ));
        };
        for (&field, &scan) in fields.iter().zip(scans) {
            if !selection.fields.holds(field) || !selection.holds_scan(Some(scan)) {
                continue;
            }
            let identity = (
                array,
                persisted.data_description,
                partition.observation,
                field,
                scan,
            );
            if let Some(&owner) = owners.get(&identity) {
                if owner != id {
                    return Ok(Some(
                        "calibration solution groups spanning observation partitions",
                    ));
                }
                continue;
            }
            let required = btree_node_bytes::<SolveIdentity, usize>(owners.len() + 1);
            if required > budget {
                return Err(ObservationError::OverBudget {
                    what: "calibration partition group identities",
                    required,
                    budget,
                });
            }
            owners.insert(identity, id);
        }
    }
    Ok(None)
}

struct GainAccumulator<'a> {
    request: &'a GainSolveRequest,
    groups: BTreeMap<GroupKey, ScanAccumulator>,
    width_s: f64,
    legacy_receptors: bool,
    legacy_refant_slots: LegacyRefantSlots,
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
    legacy_receptors: bool,
    selection: &GainObservationSelection,
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
        legacy_receptors,
        legacy_refant_slots: LegacyRefantSlots::new(),
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
        if selection.holds_partition(
            input.observation.catalogue().partition(id),
            &input.partitions[id.0 as usize],
        ) {
            read_partition(input, id, batch_bytes, selection, &mut state)?;
        }
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
        apply_reference_antenna(
            &mut state.rows,
            preferred,
            &alternates,
            strict,
            &state.legacy_refant_slots,
        );
    }
    if state.selection.is_empty() {
        return Err(GainSolveError::EmptySelection);
    }
    finish_gain_amplitudes(&mut state.rows, request);
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
    selection: &GainObservationSelection,
    state: &mut GainAccumulator<'_>,
) -> Result<(), GainSolveError> {
    let partition = input.observation.catalogue().partition(id);
    let persisted = &input.partitions[id.0 as usize];
    let request = partition_access(input, id, budget, selection.fields.clone());
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
                    if !selection.holds_scan(fragment.key.scan) {
                        continue;
                    }
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
    fields: FieldSelection,
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
        fields,
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
            legacy_g_receptors: self.legacy_receptors,
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
        record_legacy_refant_slots(&mut self.legacy_refant_slots, &group);
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
    use crate::solve::GainSolveMode;

    fn trial_ms() -> tempfile::TempDir {
        let root = tempfile::tempdir().unwrap();
        let archive =
            include_bytes!("../../../../casacore/casa-ms/tests/fixtures/ms_trial/ms_trial.ms.tgz");
        tar::Archive::new(flate2::read::GzDecoder::new(&archive[..]))
            .unpack(root.path())
            .unwrap();
        root
    }

    #[test]
    fn layout_checks_only_observed_field_scan_pairs_and_charges_their_index() {
        let root = trial_ms();
        with_calibration_observation(&root.path().join("ms_trial.ms"), admit_msv2, |input| {
            let mut catalogue = Catalogue::clone(input.observation.catalogue());
            catalogue.scans.push(casa_observation::ScanEntry {
                name: "2".to_owned(),
                number: Some(2),
                intents: vec![],
            });
            catalogue.fields.push(catalogue.fields[0].clone());
            catalogue.partitions[0].fields = vec![FieldId(0), FieldId(1)];
            catalogue.partitions[0].scans = vec![ScanId(0), ScanId(1)];
            catalogue.partitions.push(catalogue.partitions[0].clone());
            let fields = [FieldId(0), FieldId(1), FieldId(0)];
            let scans = [ScanId(0), ScanId(1), ScanId(0)];
            let disjoint_scans = [ScanId(1), ScanId(0), ScanId(1)];
            let partition = |scans| CalibrationPartition {
                window: 0,
                data_description: 0,
                array: Some(0),
                fields_and_scans: Some((&fields, scans)),
            };
            let disjoint = [partition(&scans), partition(&disjoint_scans)];
            let all = GainObservationSelection::default();
            assert_eq!(
                unrepresented_layout(&catalogue, &disjoint, 4096, &all).unwrap(),
                None
            );
            assert!(matches!(
                unrepresented_layout(&catalogue, &disjoint, 0, &all),
                Err(ObservationError::OverBudget {
                    what: "calibration partition group identities",
                    ..
                })
            ));
            let overlaps = [partition(&scans), partition(&scans)];
            assert_eq!(
                unrepresented_layout(&catalogue, &overlaps, 4096, &all).unwrap(),
                Some("calibration solution groups spanning observation partitions"),
            );
            let unobserved = GainObservationSelection {
                fields: FieldSelection::Only(vec![FieldId(1)]),
                scans: Some(vec![ScanId(0)]),
            };
            assert_eq!(
                unrepresented_layout(&catalogue, &overlaps, 0, &unobserved).unwrap(),
                None
            );
        })
        .unwrap();
    }

    #[test]
    fn gaincal_keeps_time_bins_across_physical_batch_boundaries() {
        let root = trial_ms();
        let mut request = super::super::batch_trial::gain_request(root.path().join("unused.cal"));
        with_calibration_observation(&root.path().join("ms_trial.ms"), admit_msv2, |input| {
            let id = PartitionId(0);
            let unlimited =
                partition_access(&input, id, input.available_bytes, FieldSelection::All);
            let estimate = input.observation.estimate(&unlimited).unwrap();
            let one_time_limit = estimate.partitions[0].held_bytes_per_time() * 2;
            let split = partition_access(&input, id, one_time_limit, FieldSelection::All);
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

            for mode in [
                GainSolveMode::AmplitudePhase,
                GainSolveMode::Phase,
                GainSolveMode::Amplitude,
            ] {
                request.solve_mode = mode;
                let all = GainObservationSelection::default();
                let whole =
                    read_solutions(&input, &request, input.available_bytes, false, &all).unwrap();
                let fragmented =
                    read_solutions(&input, &request, one_time_limit, false, &all).unwrap();
                assert_eq!(whole.selected_data, fragmented.selected_data, "{mode:?}");
                assert_eq!(whole.solve_stats, fragmented.solve_stats, "{mode:?}");
                assert_eq!(whole.rows.len(), 10, "{mode:?}");
                assert_eq!(whole.rows.len(), fragmented.rows.len(), "{mode:?}");
                for (whole, split) in whole.rows.iter().zip(&fragmented.rows) {
                    assert_eq!(whole.time_seconds, split.time_seconds, "{mode:?}");
                    assert_eq!(whole.antenna_id, split.antenna_id, "{mode:?}");
                    assert_eq!(whole.flags, split.flags, "{mode:?}");
                    assert_eq!(whole.gains, split.gains, "{mode:?}");
                }
            }
        })
        .unwrap();
    }
}

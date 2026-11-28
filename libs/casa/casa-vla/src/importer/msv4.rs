// SPDX-License-Identifier: LGPL-3.0-or-later
//! Write archive integrations directly to the shared MSv4 creation writer.
//! The first pass declares the time grids, then a replay fills the batches.
//! Only the metadata and the current archive record's payload are retained.

use std::collections::BTreeSet;

use casa_core::measures::MjdHighPrec;
use casa_ms::observation_io::creation::{
    Loss, Msv2CreationPartition, catalogue_for_creation, default_memory_bytes, losses,
};
use casa_ms::observation_io::{Msv4Codec, Msv4ProcessingSetWriter, Msv4StorageOptions, Msv4Writer};
use casa_observation::{
    AntennaId, BatchBuffers, BatchHeader, Catalogue, CorrelationAxis, FieldId, ObservationError,
    ObservationId, OrderedBaseline, PartitionId, Roles, ScanEntry, ScanId, Sharing,
    TimeBaselineGrid, WeightRule,
};

use super::*;

const UNIX_EPOCH: f64 = 40_587.0;
const DAY_S: f64 = 86_400.0;
const DATA_GROUPS: [&str; 3] = ["base", "corrected", "model"];

#[derive(Clone, Copy, Debug, Hash, PartialEq, Eq)]
struct Key {
    session: ObservationId,
    description: usize,
    scan: ScanId,
    array: i32,
    interval: u64,
}

impl From<Integration> for Key {
    fn from(value: Integration) -> Self {
        Self {
            session: ObservationId(value.observation as u32),
            description: value.data_description as usize,
            scan: ScanId((value.scan - 1) as u32),
            array: value.array,
            interval: value.interval.to_bits(),
        }
    }
}

struct DatasetPlan {
    key: Key,
    baselines: BTreeSet<OrderedBaseline>,
    times_s: Vec<f64>,
    fields: Vec<FieldId>,
}

impl DatasetPlan {
    fn grid(self) -> (Key, TimeBaselineGrid) {
        let Self {
            key,
            baselines,
            times_s,
            fields,
        } = self;
        let scan_per_time = vec![key.scan; times_s.len()];
        (
            key,
            TimeBaselineGrid {
                baselines: baselines.into_iter().collect(),
                time_scale: EpochRef::UTC,
                time_origin: MjdHighPrec::new(UNIX_EPOCH, 0.0),
                times_s,
                field_per_time: fields,
                scan_per_time,
                interval_s: f64::from_bits(key.interval),
            },
        )
    }
}

struct Inventory {
    ids: HashMap<Key, PartitionId>,
    plans: Vec<DatasetPlan>,
    frame: MeasFrame,
    budget: usize,
    planned_bytes: usize,
}

impl Inventory {
    fn add(
        &mut self,
        integration: Integration,
        antennas: &[i32],
        baselines: &[NormalizedBaselineRow],
    ) -> Result<(), VlaError> {
        let key = Key::from(integration);
        let utc = unix_utc(integration.time, &self.frame)?;
        // The retained time/field/scan indexes also feed the writer's
        // coordinate arrays. Baseline trees are replaced by typed grids.
        let index = match self.ids.get(&key) {
            Some(id) => *id,
            None => {
                self.charge(
                    size_of::<DatasetPlan>() + size_of::<Key>() + size_of::<TimeBaselineGrid>(),
                )?;
                let id = PartitionId(self.plans.len() as u32);
                self.ids.insert(key, id);
                self.plans.push(DatasetPlan {
                    key,
                    baselines: BTreeSet::new(),
                    times_s: Vec::new(),
                    fields: Vec::new(),
                });
                id
            }
        };
        let id = index.0 as usize;
        if self.plans[id]
            .times_s
            .last()
            .is_some_and(|&last| utc <= last)
        {
            return Err(refused(
                "an archive scan and spectral window contain repeated or nonascending integration times",
            ));
        }
        self.charge(4 * (size_of::<f64>() + size_of::<FieldId>() + size_of::<ScanId>()))?;
        for baseline in baselines {
            let key = ordered_baseline([
                antennas[baseline.antenna1_archive],
                antennas[baseline.antenna2_archive],
            ]);
            if !self.plans[id].baselines.contains(&key) {
                self.charge(4 * size_of::<OrderedBaseline>())?;
                self.plans[id].baselines.insert(key);
            }
        }
        let dataset = &mut self.plans[id];
        dataset.times_s.push(utc);
        dataset.fields.push(FieldId(integration.field as u32));
        Ok(())
    }

    fn charge(&mut self, bytes: usize) -> Result<(), VlaError> {
        let next = self.planned_bytes.saturating_add(bytes);
        if next > self.budget {
            return Err(refused(format!(
                "the MSv4 archive grids need {next} bytes, above the {} byte planning budget",
                self.budget
            )));
        }
        self.planned_bytes = next;
        Ok(())
    }
}

pub(super) fn import(
    options: &ImportVlaOptions,
    selection: &ArchiveSelection,
    vis: PathBuf,
    record_history: bool,
) -> Result<ImportReport, VlaError> {
    if vis.symlink_metadata().is_ok() {
        return Err(VlaError::InvalidArgument {
            argument: "vis",
            message: format!("{} already exists", vis.display()),
        });
    }
    let mut describe = metadata(options)?;
    let mut inventory = Inventory {
        ids: HashMap::new(),
        plans: Vec::new(),
        frame: MeasFrame::new().with_measures(describe.measures.clone()),
        budget: default_memory_bytes().map_err(provider_error)?,
        planned_bytes: 0,
    };
    process_archive_records(&mut describe, options, selection, |state, record| {
        state.visit_integrations(record, |_, integration, antennas, baselines| {
            inventory.add(integration, antennas, &baselines)
        })
    })?;
    if inventory.plans.is_empty() {
        return Err(VlaError::import(format!(
            "visibility file is empty: {}",
            vis.display()
        )));
    }
    let loss_report = loss_metadata(&describe.ms, inventory.ids.keys(), record_history)?;
    let grids: Vec<_> = std::mem::take(&mut inventory.plans)
        .into_iter()
        .map(DatasetPlan::grid)
        .collect();
    let catalogue = describe_catalogue(&describe, &grids)?;
    drop(describe);
    let set = Msv4ProcessingSetWriter::create(&vis).map_err(provider_error)?;
    let mut datasets = start_datasets(&set, &catalogue, &grids, &vis)?;
    let mut buffers =
        std::array::from_fn(|_| BatchBuffers::with_envelope(inventory.budget / DATA_GROUPS.len()));
    let mut row_flags = false;
    let mut replay = metadata(options)?;
    process_archive_records(&mut replay, options, selection, |state, record| {
        state.visit_integrations(record, |writer, integration, antennas, baselines| {
            let id = *inventory.ids.get(&integration.into()).ok_or_else(changed)?;
            let time = unix_utc(integration.time, &inventory.frame)?;
            let rows = baselines.len();
            row_flags |= write_integration(
                &mut datasets[id.0 as usize],
                &mut buffers,
                &catalogue,
                id,
                (integration, time),
                antennas,
                baselines,
            )?;
            writer.main_rows_written += rows;
            Ok(())
        })
    })?;
    for dataset in datasets {
        dataset.finish().map_err(provider_error)?;
    }
    set.finish().map_err(provider_error)?;
    let mut losses = loss_report;
    losses.extend(losses::unused_antennas(&catalogue));
    if row_flags {
        losses.push(Loss::RowFlags);
    }
    losses.sort();
    losses.dedup();
    Ok(replay.report(vis, losses.iter().map(ToString::to_string).collect()))
}

fn metadata(options: &ImportVlaOptions) -> Result<VlaImportState, VlaError> {
    let ms = MeasurementSet::create_memory(import_builder())
        .map_err(|error| VlaError::import(error.to_string()))?;
    VlaImportState::new_inner(options.clone(), ms, None)
}

fn describe_catalogue(
    state: &VlaImportState,
    grids: &[(Key, TimeBaselineGrid)],
) -> Result<Arc<Catalogue>, VlaError> {
    let scans = (1..state.next_scan_number)
        .map(|number| ScanEntry {
            name: number.to_string(),
            number: Some(number),
            intents: Vec::new(),
        })
        .collect();
    let specs: Vec<_> = grids
        .iter()
        .map(|(key, grid)| Msv2CreationPartition {
            observation: key.session,
            data_description: key.description,
            feeds: [0, 0],
            grid,
        })
        .collect();
    catalogue_for_creation(&state.ms, scans, &specs)
        .map(Arc::new)
        .map_err(provider_error)
}

fn loss_metadata<'a>(
    ms: &MeasurementSet,
    keys: impl Iterator<Item = &'a Key>,
    record_history: bool,
) -> Result<Vec<Loss>, VlaError> {
    let mut found = losses::msv4_subtable_losses(ms).map_err(provider_error)?;
    found.extend([
        Loss::FlagCategories,
        Loss::Sigma,
        Loss::Column("OBSERVATION.TIME_RANGE and RELEASE_DATE".into()),
    ]);
    if keys.map(|key| key.array).any(|array| array != 0) {
        found.push(Loss::Column("ARRAY_ID".into()));
    }
    if record_history {
        found.push(Loss::Table("HISTORY".into()));
        found.push(Loss::Table("Original flag-version snapshot".into()));
    }
    Ok(found)
}

fn start_datasets(
    set: &Msv4ProcessingSetWriter,
    catalogue: &Catalogue,
    grids: &[(Key, TimeBaselineGrid)],
    vis: &Path,
) -> Result<Vec<Msv4Writer>, VlaError> {
    let stem = vis
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or("observation");
    let stem = stem.strip_suffix(".ps.zarr").unwrap_or(stem);
    let digits = (grids.len() - 1).to_string().len();
    let date = casa_core::quanta::time::utc_now_iso8601();
    grids
        .iter()
        .enumerate()
        .map(|(index, (_, grid))| {
            let id = PartitionId(index as u32);
            let entry = catalogue.partition(id);
            let options = Msv4StorageOptions {
                dataset_name: format!("{stem}_{index:0digits$}"),
                creator: "casa-rs".into(),
                creation_date: date.clone(),
                // Each archive integration fills complete chunks, independent
                // of which scan/window the next record belongs to.
                chunks: [
                    1,
                    grid.baselines.len() as u64,
                    entry.frequencies_hz.len() as u64,
                    entry.correlations.len() as u64,
                ],
                codec: Msv4Codec::BloscLz4,
            };
            set.dataset(catalogue, id, &DATA_GROUPS, grid, &options)
                .map_err(provider_error)
        })
        .collect()
}

fn ordered_baseline([first, second]: [i32; 2]) -> OrderedBaseline {
    OrderedBaseline {
        antenna1: AntennaId(first.min(second) as u32),
        antenna2: AntennaId(first.max(second) as u32),
    }
}

fn unix_utc(seconds_tai: f64, frame: &MeasFrame) -> Result<f64, VlaError> {
    if !seconds_tai.is_finite() {
        return Err(refused(
            "a nonfinite archive timestamp cannot be written as MSv4 UNIX/UTC",
        ));
    }
    let tai = MEpoch::new(
        MjdHighPrec::new(
            seconds_tai.div_euclid(DAY_S),
            seconds_tai.rem_euclid(DAY_S) / DAY_S,
        ),
        EpochRef::TAI,
    );
    let convert = |epoch: &MEpoch, scale| {
        epoch.convert_to(scale, frame).map_err(|error| {
            refused(format!(
                "cannot convert an archive timestamp to MSv4 UNIX/UTC: {error}"
            ))
        })
    };
    let utc = convert(&tai, EpochRef::UTC)?;
    let midnight = utc.value().day();
    let begin = convert(&MEpoch::from_mjd(midnight, EpochRef::UTC), EpochRef::TAI)?;
    let end = convert(
        &MEpoch::from_mjd(midnight + 1.0, EpochRef::UTC),
        EpochRef::TAI,
    )?;
    // The writer uses 86,400 Unix seconds per civil day. A leap day
    // would need XRADIO's leap-second-aware mapping, which it lacks.
    let roundtrip = convert(&utc, EpochRef::TAI)?;
    if ((end.value() - begin.value()) * DAY_S - DAY_S).abs() > 1e-6
        || ((roundtrip.value() - tai.value()) * DAY_S).abs() > 1e-6
    {
        return Err(refused(
            "archive timestamps on a leap-second day are outside the MSv4 UNIX/UTC profile",
        ));
    }
    Ok((utc.value() - MjdHighPrec::new(UNIX_EPOCH, 0.0)) * DAY_S)
}

fn provider_error(error: ObservationError) -> VlaError {
    match error {
        ObservationError::Unsupported { .. }
        | ObservationError::InvalidRequest { .. }
        | ObservationError::OverBudget { .. } => refused(error.to_string()),
        _ => VlaError::import(error.to_string()),
    }
}

fn changed() -> VlaError {
    VlaError::import("archive integrations changed between MSv4 declaration and write")
}

fn refused(message: impl Into<String>) -> VlaError {
    VlaError::InvalidArgument {
        argument: "outformat",
        message: message.into(),
    }
}

fn write_integration(
    dataset: &mut Msv4Writer,
    storage: &mut [BatchBuffers; 3],
    catalogue: &Arc<Catalogue>,
    id: PartitionId,
    (integration, time): (Integration, f64),
    antennas: &[i32],
    baselines: Vec<NormalizedBaselineRow>,
) -> Result<bool, VlaError> {
    let entry = catalogue.partition(id);
    let samples = entry.baselines.len();
    let correlations = entry.correlations.len();
    let channels = entry.frequencies_hz.len();
    let cell = correlations * channels;
    let mut flag_row = false;
    let mut fills = Vec::with_capacity(DATA_GROUPS.len());
    for (group, storage) in storage.iter_mut().enumerate() {
        let roles = if group == 0 {
            Roles::ALL
        } else {
            Roles {
                values: true,
                scan: true,
                flags: false,
                weights: false,
                uvw: false,
                time_centroid: false,
                interval: false,
                exposure: false,
            }
        };
        let fill = storage
            .begin(
                BatchHeader {
                    partition: id,
                    data_group: group,
                    times: 1,
                    first_channel: 0,
                    channels,
                    correlations: CorrelationAxis::All,
                    roles,
                    weights_per_channel: false,
                    weight_rule: WeightRule::Stored,
                    interval: Sharing::Constant(integration.interval),
                    exposure: Sharing::PerSample,
                },
                catalogue,
            )
            .map_err(provider_error)?;
        fills.push(fill);
    }
    let [base, corrected, model] = fills.as_mut_slice() else {
        unreachable!()
    };
    let [base, corrected, model] = [base.arrays(), corrected.arrays(), model.arrays()];
    for each in [&mut *base, &mut *corrected, &mut *model] {
        each.field.push(FieldId(integration.field as u32));
        each.scan.push(ScanId((integration.scan - 1) as u32));
        each.time_offsets_s.push(time);
        each.values
            .resize(samples * cell, Complex32::new(f32::NAN, f32::NAN));
    }
    base.flags.resize(samples * cell, true);
    base.weights.resize(samples * correlations, f32::NAN);
    base.uvw.resize(samples * 3, f64::NAN);
    base.centroid_offsets_s.resize(samples, time);
    base.exposure_s.resize(samples, 0.0);
    let mut seen = vec![false; samples];
    for row in baselines {
        let [first, second] = [
            antennas[row.antenna1_archive],
            antennas[row.antenna2_archive],
        ];
        let index = entry
            .baselines
            .binary_search(&ordered_baseline([first, second]))
            .map_err(|_| changed())?;
        if std::mem::replace(&mut seen[index], true) {
            return Err(refused(
                "more than one archive visibility for the same baseline and integration",
            ));
        }
        let uvw = if first <= second {
            row.uvw_m
        } else {
            row.uvw_m.map(|component| -component)
        };
        let (_, _, data, correction, model_data, flags, _, weights, _, _) =
            reorder_baseline_for_ms(
                first,
                second,
                row.data,
                row.corrected_data,
                row.model_data,
                row.flag,
                row.flag_category,
                row.weight,
                row.sigma,
                &row.corr_types,
            )?;
        let row_flagged = flag_row_value(&flags)?;
        flag_row |= row_flagged;
        let begin = index * cell;
        if !row_flagged {
            copy_flags(&flags, &mut base.flags[begin..begin + cell])?;
        }
        for (target, values) in [
            (&mut *base, data),
            (&mut *corrected, correction),
            (&mut *model, model_data),
        ] {
            copy_visibility(values, &mut target.values[begin..begin + cell])?;
        }
        let ArrayValue::Float32(weights) = weights else {
            return Err(changed());
        };
        copy_cell(
            &weights,
            &mut base.weights[index * correlations..(index + 1) * correlations],
        )?;
        base.uvw[index * 3..(index + 1) * 3].copy_from_slice(&uvw);
        base.exposure_s[index] = integration.interval;
    }
    for fill in fills {
        fill.finish().map_err(provider_error)?;
    }
    let batches = storage
        .each_ref()
        .map(|group| group.batch().expect("finished all three groups"));
    dataset.write_groups(&batches).map_err(provider_error)?;
    Ok(flag_row)
}

fn copy_visibility(value: ArrayValue, output: &mut [Complex32]) -> Result<(), VlaError> {
    let ArrayValue::Complex32(array) = value else {
        return Err(changed());
    };
    copy_cell(&array, output)
}

fn copy_flags(value: &ArrayValue, output: &mut [bool]) -> Result<(), VlaError> {
    let ArrayValue::Bool(array) = value else {
        return Err(changed());
    };
    copy_cell(array, output)
}

fn copy_cell<T: Copy>(source: &ArrayD<T>, output: &mut [T]) -> Result<(), VlaError> {
    if source.len() != output.len() {
        return Err(changed());
    }
    for (output, source) in output.iter_mut().zip(source.view().reversed_axes()) {
        *output = *source;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn unix_profile_checks_leap_days_and_converts_tai() {
        let runtime = casa_measures::MeasuresRuntime::open_discovered(Default::default()).unwrap();
        let frame = MeasFrame::new().with_measures(Arc::new(runtime));
        // 2016-12-31 ended at TAI 00:00:37 of 2017-01-01. The leap
        // second and the preceding UTC day have no encoding in this
        // writer's uniform 86,400-seconds-per-day convention.
        let next = 57_754.0 * DAY_S;
        for time in [next - 1.0, next + 36.5] {
            let error = unix_utc(time, &frame).unwrap_err();
            assert!(matches!(
                error,
                VlaError::InvalidArgument {
                    argument: "outformat",
                    ..
                }
            ));
            assert!(error.to_string().contains("leap-second day"), "{error}");
        }
        let expected = (57_754.0 - UNIX_EPOCH) * DAY_S + 0.5;
        assert!((unix_utc(next + 37.5, &frame).unwrap() - expected).abs() <= 1e-6);
    }
}

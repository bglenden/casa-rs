// SPDX-License-Identifier: LGPL-3.0-or-later

//! Compile's survey of the selected observation: one traversal of the
//! partitions the request selects ([`super::partitions::candidates`]) that
//! reads no visibilities, for the facts the later stages need.
//!
//! The traversal reads each partition's selected fields' times and, when a
//! UV-distance interval or a W-projecting gridder needs them, the samples'
//! UVW; nothing else. A time's field comes with the batch, and a
//! partition's samples share one spectral window and observing session, so
//! the survey learns the selected fields, observations, times and windows
//! from the batches' coordinates. The epochs (time, field) of each window's
//! selected samples are what its frequency conversions are later evaluated
//! at ([`super::envelope`]).

use std::collections::{BTreeMap, BTreeSet};

use casa_imaging_model::{CorrelationProduct, PartitionSelection, RowSelection, UvSelection};
use casa_imaging_runtime::{Admitted, Demand, HostResources, ResourcePolicy, admit};
use casa_ms::observation_io::Msv2Observation;
use casa_observation::{
    AccessRequest, BatchBuffers, CorrelationRequest, Deliver, FlagsFrom, Grouping, Observation,
    PartitionEntry, ReadEvent, Roles, TimeSelection, VisibilityBatch, WeightRequest,
};
use casa_types::measures::MjdHighPrec;

use super::PrepareError;
use super::envelope::SpectralEnvelope;
use super::partitions::{Candidate, Candidates, field_selection};
use super::selection::{SourceSpectralWindow, WindowChannels};

/// The least bytes one batch of the survey holds: enough for several times
/// of a large array, so a partition's survey is not a batch per time.
const SURVEY_BATCH_BYTES: usize = 1 << 18;

/// The `|w|` envelope of the selected samples, in metres.
#[derive(Clone, Copy)]
pub(super) struct WRange {
    pub(super) maximum_abs_m: f64,
    pub(super) minimum_abs_m: f64,
    pub(super) sum_squares_m2: f64,
    pub(super) samples: u64,
}

impl WRange {
    const EMPTY: Self = Self {
        maximum_abs_m: 0.0,
        minimum_abs_m: f64::INFINITY,
        sum_squares_m2: 0.0,
        samples: 0,
    };

    fn observe(&mut self, w_m: f64) {
        self.maximum_abs_m = self.maximum_abs_m.max(w_m.abs());
        self.minimum_abs_m = self.minimum_abs_m.min(w_m.abs());
        self.sum_squares_m2 += w_m * w_m;
        self.samples += 1;
    }
}

/// One (time, field) a window's selected samples are observed at: what its
/// frequency conversions are evaluated at.
#[derive(Clone, Copy, Debug, PartialEq)]
pub(super) struct Epoch {
    /// Seconds since MJD 0 in the observation's time scale.
    pub(super) time_mjd_seconds: f64,
    pub(super) field_id: i32,
}

/// The earliest selected cross-correlation sample not wholly flagged: what
/// native AW evaluates its parallactic angle at.
#[derive(Clone, Copy, Debug)]
pub(super) struct FirstCrossSample {
    pub(super) time_mjd_seconds: f64,
    pub(super) field_id: i32,
    pub(super) observation_id: i32,
}

/// What the survey establishes.
pub(super) struct Survey {
    /// The per-sample predicates.
    pub(super) row_selection: RowSelection,
    /// The partitions holding a selected sample, in partition order, with
    /// every correlation each holds.
    pub(super) partitions: Vec<PartitionSelection>,
    /// The selected samples, padding excluded.
    pub(super) samples: u64,
    /// The selected partitions' spectral windows, in one source frame.
    pub(super) spectral_windows: Vec<SourceSpectralWindow>,
    pub(super) source_frequency_reference: casa_types::measures::frequency::FrequencyRef,
    pub(super) fields: BTreeSet<i32>,
    pub(super) observation_ids: BTreeSet<i32>,
    /// The earliest selected time: the anchor of the spectral frame and of
    /// the image's observation date.
    pub(super) first_time_mjd_seconds: f64,
    pub(super) time_bounds_mjd_seconds: [f64; 2],
    /// Read only for native AW ([`first_cross_sample`]).
    pub(super) first_cross: Option<FirstCrossSample>,
    /// Read only when a W-projecting gridder or a UV-distance interval needs
    /// UVW; empty otherwise.
    pub(super) w: WRange,
    /// Each selected window's epochs, ascending in time, each once.
    pub(super) epochs: BTreeMap<usize, Vec<Epoch>>,
    /// A continuum run's selected channels per window and their envelope.
    pub(super) continuum: Option<(WindowChannels, SpectralEnvelope)>,
}

impl Survey {
    /// The epochs of window `spw_id`'s selected samples.
    pub(super) fn window_epochs(&self, spw_id: usize) -> &[Epoch] {
        self.epochs.get(&spw_id).map_or(&[], Vec::as_slice)
    }

    /// The earliest selected time of field `field_id`, if it has one.
    pub(super) fn first_time_of(&self, field_id: i32) -> Option<f64> {
        self.epochs
            .values()
            .filter_map(|epochs| epochs.iter().find(|epoch| epoch.field_id == field_id))
            .map(|epoch| epoch.time_mjd_seconds)
            .reduce(f64::min)
    }
}

/// What the survey reads, and the observation it reads it from.
pub(super) struct SurveyPlan<'a> {
    pub(super) observation: &'a dyn Observation,
    /// The MeasurementSet, when the store is one ([`super::OpenedStore::msv2`]).
    pub(super) msv2: Option<&'a Msv2Observation>,
    pub(super) candidates: Candidates,
    pub(super) row_selection: RowSelection,
    /// Whether the survey reads UVW for the W range, besides a UV-distance
    /// interval's test.
    pub(super) w_range: bool,
    /// Whether native AW needs its first cross-correlation sample.
    pub(super) first_cross: bool,
}

/// The facts gathered over every partition.
struct Facts {
    fields: BTreeSet<i32>,
    observation_ids: BTreeSet<i32>,
    time_bounds_mjd_seconds: [f64; 2],
    w: WRange,
    epochs: BTreeMap<usize, Vec<Epoch>>,
}

impl SurveyPlan<'_> {
    fn reads_uvw(&self) -> bool {
        self.w_range || !matches!(self.row_selection.uv_distances(), UvSelection::All)
    }

    /// Whether the store holds its own padding, which only a NaN weight
    /// marks: an MSv4 (XRADIO's converter leaves its `FLAG` false). The
    /// MSv2 adapter pads its grids itself, counts what it pads
    /// ([`casa_observation::PartitionEstimate::padded_fraction`]) and states
    /// each row's `FLAG_ROW`.
    fn stored_padding(&self) -> bool {
        self.msv2.is_none()
    }

    /// The request surveying `candidate` (`entry` in the catalogue): its
    /// selected fields' times of its first channel, with UVW when read and a
    /// store's own padding's weights; with `flags`, every channel's flags
    /// instead of the weights.
    fn request(
        &self,
        candidate: &Candidate,
        entry: &PartitionEntry,
        flags: bool,
        batch_bytes: usize,
    ) -> AccessRequest {
        AccessRequest {
            partitions: vec![candidate.id],
            data_group: self.candidates.data_group.clone(),
            roles: Roles {
                values: false,
                flags,
                weights: !flags && self.stored_padding(),
                uvw: self.reads_uvw(),
                time_centroid: false,
                interval: false,
                exposure: false,
                scan: false,
            },
            weights: WeightRequest::Stored,
            channels: if flags {
                0..entry.frequencies_hz.len()
            } else {
                0..1
            },
            times: TimeSelection::All,
            fields: field_selection(self.row_selection.fields()),
            correlations: CorrelationRequest {
                deliver: Deliver::All,
                flags_from: FlagsFrom::Delivered,
            },
            grouping: Grouping::None,
            batch_bytes,
        }
    }

    /// The bytes one time of `candidate`'s survey holds, with the cursor's
    /// scratch: of its flags pass when `flags`.
    fn bytes_per_time(&self, candidate: &Candidate, flags: bool) -> Result<usize, PrepareError> {
        let entry = self.observation.catalogue().partition(candidate.id);
        let estimate =
            self.observation
                .estimate(&self.request(candidate, entry, flags, usize::MAX))?;
        Ok(estimate
            .partitions
            .first()
            .map_or(0, casa_observation::PartitionEstimate::held_bytes_per_time))
    }
}

/// Survey `plan`'s partitions, admitted on `host` under `policy`: the
/// largest batch of the traversal (and of native AW's flags pass on a store
/// holding its own padding) and each
/// window's epochs, one per selected time of each of its partitions at
/// most. The charge is held until the problem compiles, since the epochs
/// are read until then.
///
/// # Errors
///
/// When the admission is refused, a traversal fails, or the selection
/// holds no sample.
pub(super) fn survey(
    plan: SurveyPlan<'_>,
    spectral_windows: Vec<SourceSpectralWindow>,
    (host, policy): (&HostResources, &ResourcePolicy),
) -> Result<Admitted<Survey>, PrepareError> {
    let catalogue = plan.observation.catalogue();
    let mut batch_bytes = SURVEY_BATCH_BYTES;
    let mut epoch_bound = BTreeMap::<usize, usize>::new();
    for candidate in &plan.candidates.partitions {
        batch_bytes = batch_bytes.max(plan.bytes_per_time(candidate, false)?);
        if plan.first_cross && plan.stored_padding() {
            batch_bytes = batch_bytes.max(plan.bytes_per_time(candidate, true)?);
        }
        let entry = catalogue.partition(candidate.id);
        let estimate =
            plan.observation
                .estimate(&plan.request(candidate, entry, false, batch_bytes))?;
        *epoch_bound.entry(candidate.spectral_window).or_default() += estimate
            .partitions
            .first()
            .map_or(0, |partition| partition.times);
    }
    let epoch_bytes = epoch_bound.values().sum::<usize>() * size_of::<Epoch>();
    // The survey's buffers grow to its largest batch, the cursor's scratch
    // with them.
    let reservation = admit(
        host,
        policy,
        &Demand {
            phase: "observation survey",
            memory: (2 * batch_bytes + epoch_bytes) as u64,
        },
    )?;
    let mut facts = Facts {
        fields: BTreeSet::new(),
        observation_ids: BTreeSet::new(),
        time_bounds_mjd_seconds: [f64::INFINITY, f64::NEG_INFINITY],
        w: WRange::EMPTY,
        epochs: epoch_bound
            .into_iter()
            .map(|(window, bound)| (window, Vec::with_capacity(bound)))
            .collect(),
    };
    let mut buffers = BatchBuffers::default();
    let mut partitions = Vec::new();
    let mut samples = 0;
    let mut first_cross: Option<FirstCrossSample> = None;
    for candidate in &plan.candidates.partitions {
        let entry = catalogue.partition(candidate.id);
        let request = plan.request(candidate, entry, false, batch_bytes);
        let selected = survey_partition(&plan, candidate, &request, &mut buffers, &mut facts)?;
        if selected == 0 {
            continue;
        }
        samples += selected;
        if plan.first_cross
            && let Some(sample) = first_cross_sample(
                &plan,
                candidate,
                &plan.request(candidate, entry, plan.stored_padding(), batch_bytes),
                &mut buffers,
            )?
            && first_cross.is_none_or(|first| sample.time_mjd_seconds < first.time_mjd_seconds)
        {
            first_cross = Some(sample);
        }
        partitions.push(PartitionSelection::new(
            candidate.id,
            u32::try_from(candidate.spectral_window).expect("an SPW id is a stored i32"),
            entry
                .correlations
                .iter()
                .enumerate()
                .map(|(index, correlation)| {
                    CorrelationProduct::new(
                        u32::try_from(index).expect("a CORR_TYPE index fits u32"),
                        *correlation,
                    )
                })
                .collect(),
        ));
    }
    drop(buffers);
    if samples == 0 {
        return Err(PrepareError::NoSelectedSamples);
    }
    for epochs in facts.epochs.values_mut() {
        epochs.sort_unstable_by(|a, b| {
            a.time_mjd_seconds
                .total_cmp(&b.time_mjd_seconds)
                .then(a.field_id.cmp(&b.field_id))
        });
        epochs.dedup();
    }
    facts.epochs.retain(|_, epochs| !epochs.is_empty());
    let mut spectral_windows = spectral_windows;
    spectral_windows.retain(|window| facts.epochs.contains_key(&window.spw_id));
    let source_frequency_reference = super::selection::one_source_frame(&spectral_windows)?;
    Ok(Admitted {
        value: Survey {
            row_selection: plan.row_selection,
            partitions,
            samples,
            spectral_windows,
            source_frequency_reference,
            fields: facts.fields,
            observation_ids: facts.observation_ids,
            first_time_mjd_seconds: facts.time_bounds_mjd_seconds[0],
            time_bounds_mjd_seconds: facts.time_bounds_mjd_seconds,
            first_cross,
            w: facts.w,
            epochs: facts.epochs,
            continuum: None,
        },
        reservation,
    })
}

/// Survey one partition into `facts`: the epochs, fields, observation,
/// times and W range of its selected samples. Returns how many it holds.
///
/// Without a UV-distance interval every delivered sample but padding is
/// selected. On an MSv2 the estimate counts them
/// ([`casa_observation::PartitionEstimate::padded_fraction`]): a delivered
/// time always holds one, since a time is on a partition's grid only when a
/// sample of it was acquired. A store holding its own padding (an MSv4)
/// marks it only by NaN weights, which the survey reads and counts past
/// ([`stored_padding`]). With a UV-distance interval, the survey tests each
/// sample that is not padding and whose UVW is finite (the MSv2 adapter's
/// padding's is NaN).
fn survey_partition(
    plan: &SurveyPlan<'_>,
    candidate: &Candidate,
    request: &AccessRequest,
    buffers: &mut BatchBuffers,
    facts: &mut Facts,
) -> Result<u64, PrepareError> {
    let uv = plan.row_selection.uv_distances();
    let tested = !matches!(uv, UvSelection::All);
    let counted = tested || plan.stored_padding();
    let mut traversal = plan.observation.bind_read(request)?;
    let estimate = traversal_samples(&traversal.estimate);
    let mut samples = if counted { 0 } else { estimate };
    let epochs = facts
        .epochs
        .get_mut(&candidate.spectral_window)
        .expect("the survey bounds every candidate's window");
    let origin = MjdHighPrec::new(0.0, 0.0);
    while let Some(event) = traversal.next(buffers)? {
        let ReadEvent::Batch(batch) = event else {
            continue;
        };
        for time in 0..batch.times() {
            let selected = if plan.reads_uvw() || plan.stored_padding() {
                let held = observe_time(
                    &batch,
                    time,
                    (tested.then_some((uv, candidate)), plan.stored_padding()),
                    &mut facts.w,
                );
                samples += if counted { held } else { 0 };
                held > 0
            } else {
                true
            };
            if !selected {
                continue;
            }
            let time_mjd_seconds = batch.time.seconds_since(time, origin);
            let field_id = i32::try_from(batch.field[time].0).expect("a FIELD row is an i32 id");
            facts.fields.insert(field_id);
            let bounds = &mut facts.time_bounds_mjd_seconds;
            bounds[0] = bounds[0].min(time_mjd_seconds);
            bounds[1] = bounds[1].max(time_mjd_seconds);
            epochs.push(Epoch {
                time_mjd_seconds,
                field_id,
            });
        }
    }
    if samples > 0 {
        let entry = plan.observation.catalogue().partition(candidate.id);
        facts
            .observation_ids
            .insert(i32::try_from(entry.observation.0).expect("an OBSERVATION row is an i32 id"));
    }
    Ok(samples)
}

/// The selected samples of time `time` of `batch`, observed into `w` when
/// the batch holds UVW: those that are not the store's own padding when
/// `stored` ([`stored_padding`]) and, with UVW, those whose UVW is finite
/// and that `uv`, when tested, holds for its candidate's wavelength.
/// Returns how many.
fn observe_time(
    batch: &VisibilityBatch<'_>,
    time: usize,
    (uv, stored): (Option<(&UvSelection, &Candidate)>, bool),
    w: &mut WRange,
) -> u64 {
    let per_time = batch.samples_per_time();
    let mut held = 0;
    for sample in time * per_time..(time + 1) * per_time {
        if stored && stored_padding(batch, sample) {
            continue;
        }
        if let Some(uvw) = batch.group.uvw {
            let uvw_m = uvw.metres_at(sample);
            if !uvw_m.iter().all(|value| value.is_finite()) {
                continue;
            }
            if uv.is_some_and(|(uv, candidate)| !uv.holds(uvw_m, candidate.wavelength_m)) {
                continue;
            }
            w.observe(uvw_m[2]);
        }
        held += 1;
    }
    held
}

/// Whether sample `sample` of `batch` is padding its store holds: every
/// weight of its first delivered channel not finite. A NaN weight is
/// padding's one mark (`docs/measurement-set/contracts.md`, "padding"),
/// and XRADIO's converter and casa-rs's writer put it on every channel and
/// product of a cell no acquisition filled.
fn stored_padding(batch: &VisibilityBatch<'_>, sample: usize) -> bool {
    batch
        .group
        .weights
        .expect("the survey reads a store's own padding's weights")
        .slice(ndarray::s![sample, 0, ..])
        .iter()
        .all(|weight| !weight.is_finite())
}

/// The samples `estimate`'s one partition delivers that are not padding.
fn traversal_samples(estimate: &casa_observation::Estimate) -> u64 {
    let partition = estimate
        .partitions
        .first()
        .expect("a survey estimates one partition");
    let cells = partition.times * partition.baselines;
    let padded = (cells as f64 * partition.padded_fraction).round() as usize;
    u64::try_from(cells.saturating_sub(padded)).expect("a sample count fits u64")
}

/// The first sample of `candidate`'s selected times, in time order, that
/// correlates two antennas, is selected by UV distance and is not
/// row-flagged: CASA's AW projection evaluates its parallactic angle at the
/// first row of the first buffer that is a cross-correlation and not
/// row-flagged (`getPA`), whatever its cell flags. On an MSv2 the adapter
/// states each row's `FLAG_ROW` and padding has no row; `request` is then
/// the survey's own. An MSv4 holds no row flags (XRADIO's converter folds
/// `FLAG_ROW` into `FLAG`), so there the sample must hold a value not
/// flagged, its padding flagged throughout; `request` reads every channel's
/// flags. The traversal stops at the sample.
fn first_cross_sample(
    plan: &SurveyPlan<'_>,
    candidate: &Candidate,
    request: &AccessRequest,
    buffers: &mut BatchBuffers,
) -> Result<Option<FirstCrossSample>, PrepareError> {
    let uv = plan.row_selection.uv_distances();
    let mut traversal = plan.observation.bind_read(request)?;
    let observation = plan
        .observation
        .catalogue()
        .partition(candidate.id)
        .observation;
    let origin = MjdHighPrec::new(0.0, 0.0);
    while let Some(event) = traversal.next(buffers)? {
        let ReadEvent::Batch(batch) = event else {
            continue;
        };
        for sample in 0..batch.samples() {
            let baseline = batch.baselines[sample % batch.samples_per_time()];
            if baseline.is_auto() {
                continue;
            }
            if let Some(uvw) = batch.group.uvw
                && !uv.holds(uvw.metres_at(sample), candidate.wavelength_m)
            {
                continue;
            }
            let time = sample / batch.samples_per_time();
            let time_mjd_seconds = batch.time.seconds_since(time, origin);
            let unflagged = match plan.msv2 {
                Some(ms) => {
                    ms.row_flagged(candidate.id, time_mjd_seconds, baseline)
                        .map_err(PrepareError::Opening)?
                        == Some(false)
                }
                None => !batch
                    .group
                    .flags
                    .expect("the pass reads an MSv4's flags")
                    .slice(ndarray::s![sample, .., ..])
                    .iter()
                    .all(|flag| *flag),
            };
            if !unflagged {
                continue;
            }
            return Ok(Some(FirstCrossSample {
                time_mjd_seconds,
                field_id: i32::try_from(batch.field[time].0).expect("a FIELD row is an i32 id"),
                observation_id: i32::try_from(observation.0)
                    .expect("an OBSERVATION row is an i32 id"),
            }));
        }
    }
    Ok(None)
}

#[cfg(test)]
mod tests {
    use casa_imaging_model::{IdSelection, IntentSelection};
    use casa_ms::MeasurementSet;
    use casa_types::{ArrayValue, ScalarValue, Value};

    use super::*;

    const T0: f64 = 5_000_000_000.0;

    /// Native AW's reference time on `ms_trial.ms` cut to two
    /// cross-correlation rows of one baseline an hour apart, each row's
    /// (`FLAG_ROW`, every `FLAG` set) as `rows` gives them.
    fn first_cross_time(rows: [(bool, bool); 2]) -> Option<f64> {
        let root = tempfile::tempdir().expect("a temporary directory");
        let fixture = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("../casa-ms/tests/fixtures/ms_trial/ms_trial.ms.tgz");
        tar::Archive::new(flate2::read::GzDecoder::new(
            std::fs::File::open(fixture).expect("the fixture"),
        ))
        .unpack(root.path())
        .expect("unpacked");
        let path = root.path().join("ms_trial.ms");
        let mut ms = MeasurementSet::open(&path).expect("an editable MeasurementSet");
        let remove: Vec<usize> = (2..ms.main_table().row_count()).collect();
        ms.main_table_mut()
            .remove_rows(&remove)
            .expect("rows removed");
        for column in [
            "DATA_DESC_ID",
            "FIELD_ID",
            "OBSERVATION_ID",
            "ARRAY_ID",
            "STATE_ID",
            "PROCESSOR_ID",
            "FEED1",
            "FEED2",
            "SCAN_NUMBER",
            "INTERVAL",
        ] {
            let value = ms
                .main_table()
                .column_accessor(column)
                .expect("a key column")
                .get(0)
                .expect("row 0")
                .expect("a value")
                .clone();
            ms.main_table_mut()
                .cell_accessor_mut(1, column)
                .expect("row 1")
                .set(value)
                .expect("set");
        }
        for (row, (row_flag, all_flagged)) in rows.into_iter().enumerate() {
            let Value::Array(ArrayValue::Bool(mut flags)) = ms
                .main_table()
                .column_accessor("FLAG")
                .expect("FLAG")
                .get(row)
                .expect("a row")
                .expect("a cell")
                .clone()
            else {
                panic!("FLAG is a Bool array");
            };
            flags.fill(all_flagged);
            for (column, value) in [
                ("FLAG", Value::Array(ArrayValue::Bool(flags))),
                ("FLAG_ROW", Value::Scalar(ScalarValue::Bool(row_flag))),
                ("ANTENNA1", Value::Scalar(ScalarValue::Int32(0))),
                ("ANTENNA2", Value::Scalar(ScalarValue::Int32(1))),
                (
                    "TIME",
                    Value::Scalar(ScalarValue::Float64(T0 + row as f64 * 3600.0)),
                ),
            ] {
                ms.main_table_mut()
                    .cell_accessor_mut(row, column)
                    .expect("a cell")
                    .set(value)
                    .expect("set");
            }
        }
        ms.save().expect("saved");
        drop(ms);
        let observation = Msv2Observation::open(&path).expect("the cut MeasurementSet opens");
        let id = observation.catalogue().partition_ids()[0];
        let entry = observation.catalogue().partition(id);
        let candidate = Candidate {
            id,
            spectral_window: entry.window.0 as usize,
            wavelength_m: None,
        };
        let plan = SurveyPlan {
            observation: &observation,
            msv2: Some(&observation),
            candidates: Candidates {
                data_group: "base".into(),
                partitions: vec![],
            },
            row_selection: RowSelection::new(
                IdSelection::All,
                UvSelection::All,
                IntentSelection::All,
            ),
            w_range: false,
            first_cross: true,
        };
        let request = plan.request(&candidate, entry, false, 1 << 20);
        first_cross_sample(&plan, &candidate, &request, &mut BatchBuffers::default())
            .expect("the pass reads")
            .map(|sample| sample.time_mjd_seconds)
    }

    /// The samples `observation`'s survey selects, with no selection.
    fn surveyed(observation: &dyn Observation, msv2: Option<&Msv2Observation>) -> u64 {
        let plan = SurveyPlan {
            observation,
            msv2,
            candidates: Candidates {
                data_group: "base".into(),
                partitions: vec![],
            },
            row_selection: RowSelection::new(
                IdSelection::All,
                UvSelection::All,
                IntentSelection::All,
            ),
            w_range: false,
            first_cross: false,
        };
        let catalogue = observation.catalogue();
        let mut total = 0;
        for id in catalogue.partition_ids() {
            let entry = catalogue.partition(id);
            let candidate = Candidate {
                id,
                spectral_window: entry.window.0 as usize,
                wavelength_m: None,
            };
            let mut facts = Facts {
                fields: BTreeSet::new(),
                observation_ids: BTreeSet::new(),
                time_bounds_mjd_seconds: [f64::INFINITY, f64::NEG_INFINITY],
                w: WRange::EMPTY,
                epochs: [(candidate.spectral_window, Vec::new())].into(),
            };
            let request = plan.request(&candidate, entry, false, 1 << 20);
            total += survey_partition(
                &plan,
                &candidate,
                &request,
                &mut BatchBuffers::default(),
                &mut facts,
            )
            .expect("the survey reads");
        }
        total
    }

    /// An MSv4 holds its own padding, whose NaN weights the survey counts
    /// past: the MSv4 conversion of `ms_trial.ms` less one row selects the
    /// rows the MeasurementSet holds (Astra's review of #1013).
    #[test]
    fn an_msv4_survey_counts_its_acquisitions_not_its_padding() {
        use casa_ms::observation_io::Msv4Observation;
        use casa_ms::observation_io::convert::{ConversionOptions, LossKind, PaddingRows, to_msv4};
        let root = tempfile::tempdir().expect("a temporary directory");
        let fixture = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("../casa-ms/tests/fixtures/ms_trial/ms_trial.ms.tgz");
        tar::Archive::new(flate2::read::GzDecoder::new(
            std::fs::File::open(fixture).expect("the fixture"),
        ))
        .unpack(root.path())
        .expect("unpacked");
        let path = root.path().join("ms_trial.ms");
        let mut ms = MeasurementSet::open(&path).expect("an editable MeasurementSet");
        ms.main_table_mut()
            .remove_rows(&[7])
            .expect("a row removed");
        let rows = ms.main_table().row_count() as u64;
        ms.save().expect("saved");
        drop(ms);
        let v4 = root.path().join("sparse.ps.zarr");
        to_msv4(
            &path,
            &v4,
            &ConversionOptions {
                data_groups: None,
                accept: vec![LossKind::Sigma, LossKind::Tables],
                batch_bytes: 1 << 20,
                workers: 1,
                chunks: [5, 0, 0, 0],
                codec: casa_ms::observation_io::Msv4Codec::BloscLz4,
                padding: PaddingRows::Flagged,
                creator: "survey law".into(),
                creation_date: "2026-10-30T00:00:00+00:00".into(),
            },
        )
        .expect("converted");
        let original = Msv2Observation::open(&path).expect("the MeasurementSet opens");
        assert_eq!(surveyed(&original, Some(&original)), rows);
        let converted = Msv4Observation::open(&v4).expect("the processing set opens");
        assert_eq!(
            surveyed(&converted, None),
            rows,
            "an acquisition the MSv4 holds as padding is not a selected sample"
        );
    }

    #[test]
    fn a_row_not_row_flagged_counts_however_its_cells_are_flagged() {
        assert_eq!(first_cross_time([(false, true), (false, false)]), Some(T0));
        assert_eq!(first_cross_time([(false, true), (false, true)]), Some(T0));
    }

    #[test]
    fn a_row_flagged_row_does_not_count_however_its_cells_are_flagged() {
        assert_eq!(
            first_cross_time([(true, false), (false, false)]),
            Some(T0 + 3600.0)
        );
        assert_eq!(first_cross_time([(true, false), (true, false)]), None);
    }
}

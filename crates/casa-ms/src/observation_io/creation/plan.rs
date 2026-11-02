// SPDX-License-Identifier: LGPL-3.0-or-later
//! A new MeasurementSet v2 planned from observations, then written:
//! subtables from the merged catalogue, and one MAIN row per grid cell (or
//! per acquisition, padding omitted), input after input, each input's
//! partitions in output-partition order and each partition's rows in time
//! and baseline order.

use std::fs;
use std::path::{Path, PathBuf};

use casa_observation::{Catalogue, ObservationError, Roles, VisibilityUnits};
use casa_tables::table_quantum::TableQuantumDesc;
use casa_types::measures::EpochRef;
use casa_types::measures::direction::DirectionRef;

use super::combine::{self, Combined};
use super::inputs::Input;
use super::memory::{
    Ledger, MemoryCharges, catalogue_bytes, combined_bytes, history_bytes, merge_bound,
    subtables_bytes, written_bound,
};
use super::msv2_pointing::{self, Pointed};
use super::msv2_rows::{MainRows, Occupancy, RowStates, StateIds};
use super::pointing_plan::{self, PointingPlan};
use super::replace::{Replacement, Stage};
use super::{
    CreationInput, CreationOptions, CreationReport, Destination, HistoryRow, Loss, PaddingRows,
    lockstep, losses, msv2_ephemeris, msv2_tables, occupancy, writing,
};
use crate::observation_io::Msv2Observation;
use crate::observation_io::update::{request_error, storage};
use crate::write_session::MeasurementSetWritePlan;
use crate::{MeasurementSet, OptionalMainColumn};

/// A new MeasurementSet v2 planned from observations, checked before
/// anything is written ([`CreationPlan::new`]) and then written
/// ([`CreationPlan::write`]).
///
/// The inputs' catalogues are merged as CASA's `concat` merges
/// MeasurementSets' subtables (`casa_observation::merge`), their scans
/// offset as it offsets them, and their partitions combined by the key the
/// MSv2 reader partitions rows by; a combined partition's inputs must not
/// acquire one time and baseline twice. The selected data groups
/// ([`CreationOptions::data_groups`], by default every input's) become
/// `DATA` (`base`), `CORRECTED_DATA` (`corrected`) and `MODEL_DATA`
/// (`model`), sharing the first group's flags, weights and UVW; an input
/// without `corrected` is written from its `base`, and one without `model`
/// with CASA's initial model. Every grid cell of one input becomes one
/// row; padding becomes a row whose every `FLAG` is set, with NaN data,
/// weights and `UVW`, the grid's interval, no exposure and `FLAG_ROW` false
/// (its flags already exclude it), as D23 decided. With
/// [`PaddingRows::Omitted`], which partitions combined from several inputs
/// need, padding becomes no row. `WEIGHT` is the input's times its
/// [`CreationInput::weight_scale`], and `SIGMA` is derived from it: a
/// loss ([`Loss::SigmaDerived`], which the caller must accept as
/// [`LossKind::Sigma`](super::LossKind::Sigma)) unless the input's stored
/// `SIGMA` is already `1/√WEIGHT` to within one unit in the last place
/// (an MSv4, or an MSv2 with `WEIGHT_SPECTRUM`, always takes it). Names
/// are the inputs': an
/// MSv4 converted from an MSv2 holds XRADIO's suffixed names
/// (`NAME_FIELDID`, `NAME_STATION`, ...), so a round trip renames.
///
/// A MeasurementSet's row count is fixed before its rows are written. With
/// padding omitted it is the acquisitions, which only the weights record,
/// so the plan reads them to count; otherwise it is the grids' cells.
pub struct CreationPlan {
    inputs: Vec<Input>,
    pointing: Vec<PointingPlan>,
    combined: Combined,
    /// Each data column, and the units it declares.
    columns: Vec<(OptionalMainColumn, Option<String>)>,
    losses: Vec<Loss>,
    occupancy: Occupancy,
    output: Catalogue,
    /// MAIN's write plan, within the memory admitted.
    main: MeasurementSetWritePlan,
    charges: MemoryCharges,
    /// What writing takes from the options: the `HISTORY` rows, the
    /// padding policy and the batch budget.
    history: Vec<HistoryRow>,
    padding: PaddingRows,
    batch_bytes: usize,
    /// The date the ephemeris tables written state.
    creation_date: String,
    publication: Publication,
}

/// How the planned MeasurementSet is published.
enum Publication {
    /// Renamed from its staging directory to a new path.
    New(PathBuf),
    /// Exchanged with the MeasurementSet it replaces, whose lock the plan
    /// holds.
    Replace(Replacement),
}

impl CreationPlan {
    /// Open `inputs` and plan the MeasurementSet created from them at
    /// `destination`, in input order, reading what the plan must know before
    /// writing: each input's losses, its pointing, its grids and, with
    /// padding omitted, its acquisitions. A [`Destination::Replace`]
    /// destination is locked first. What making the plan holds, and what
    /// writing it will hold, are admitted against
    /// [`CreationOptions::memory_bytes`] before they are allocated
    /// ([`Self::charges`]).
    ///
    /// # Errors
    ///
    /// Nothing is written by a plan. Memory beyond
    /// [`CreationOptions::memory_bytes`], or a batch of
    /// [`CreationOptions::batch_bytes`] that cannot hold one integration of
    /// every selected group ([`ObservationError::OverBudget`]). An existing
    /// path for a
    /// [`Destination::New`], or for a [`Destination::Replace`] no
    /// MeasurementSet, one another process holds for writing, or a
    /// filesystem that cannot exchange directories. A weight scale that is
    /// not finite and positive, no input, or padding flagged where
    /// partitions combine ([`ObservationError::InvalidRequest`]); a loss not
    /// accepted
    /// ([`ObservationError::InvalidRequest`]); fields whose direction is a
    /// polynomial in time, fields that follow an ephemeris in a combination
    /// of several inputs, or an ephemeris whose samples are not evenly
    /// spaced or whose radial velocities are not geocentric; a data group
    /// without an MSv2 column, or selected twice; times in another scale
    /// than UTC; feeds other than 0; a window without channels; fields in
    /// another frame than J2000; a scan without a casacore number; feeds
    /// whose receptors differ; a correlation product the feeds cannot name
    /// (a Stokes product, or a receptor they lack); one column's groups in
    /// different units; what the merge and the combination refuse; a time
    /// and baseline two inputs both acquire, or two fields or scans at one
    /// time, or two intervals, of a combined partition; inputs with nothing
    /// to write. Weights that vary over channels (an MSv2 `WEIGHT` holds one
    /// per correlation), refused here when padding is omitted and otherwise
    /// when writing reaches them. Any error opening or reading an input.
    pub fn new(
        inputs: &[CreationInput],
        options: &CreationOptions,
        destination: Destination,
    ) -> Result<Self, ObservationError> {
        if inputs.is_empty() {
            return Err(request_error("a MeasurementSet is created from an input"));
        }
        let publication = match destination {
            Destination::New(path) => {
                if path.symlink_metadata().is_ok() {
                    return Err(request_error(format!(
                        "{} already exists; a new MeasurementSet is created at a free path",
                        path.display()
                    )));
                }
                Publication::New(path)
            }
            // A plan writes an MSv2, so it replaces only an MSv2.
            Destination::Replace(path) if crate::observation_io::is_msv4_store(&path) => {
                return Err(request_error(format!(
                    "{} is an MSv4, which a MeasurementSet v2 cannot replace",
                    path.display()
                )));
            }
            Destination::Replace(path) => Publication::Replace(Replacement::hold(&path)?),
        };
        let mut ledger = Ledger::new(options.memory_bytes);
        // The plan keeps a copy of the HISTORY rows to write.
        ledger.admit("the HISTORY rows", history_bytes(&options.history))?;
        let inputs = inputs
            .iter()
            .map(|input| Input::open(input, &mut ledger))
            .collect::<Result<Vec<_>, _>>()?;
        let groups = selected_groups(&inputs, options.data_groups.as_deref())?;
        let names = groups
            .iter()
            .map(|group| column(group))
            .collect::<Result<Vec<_>, _>>()?;
        let mut found = Vec::new();
        let mut pointing = Vec::with_capacity(inputs.len());
        for input in &inputs {
            let (path, observation) = (&input.path, input.observation());
            let catalogue = observation.catalogue();
            // An MSv2's scan opens it again and loads the subtables its
            // catalogue was read from.
            let scans = losses::scan_bytes(catalogue)
                + input.msv2().map_or(0, Msv2Observation::tables_bytes);
            found.extend(ledger.during("an input's loss scans", scans, |_| {
                losses::inventory(path, catalogue, &groups, losses::Store::Msv2)
            })?);
            let scan = pointing_plan::planning_bytes(path, observation, options.batch_bytes)?;
            let (plan, lost) = ledger.during("an input's pointing scan", scan, |_| {
                pointing_plan::plan(path, observation, losses::Store::Msv2, options.batch_bytes)
            })?;
            found.extend(lost);
            pointing.push(plan);
        }
        // One POINTING holds one frame: the inputs' must agree.
        let mut frames = pointing.iter().filter_map(PointingPlan::frame);
        if let Some(first) = frames.next()
            && frames.any(|frame| frame != first)
        {
            return Err(ObservationError::Unsupported {
                what: "writing MSv2 pointing of inputs in different frames",
            });
        }
        found.sort();
        found.dedup();
        losses::refuse_unaccepted(&found, &options.accept)?;
        for input in &inputs {
            refuse_unwritable(input.observation().catalogue())?;
        }
        // A conversion writes the ephemerides its fields follow (#942);
        // combining them, as concat merges ephemeris tables, is not done.
        if inputs.len() > 1
            && inputs.iter().any(|input| {
                let catalogue = input.observation().catalogue();
                catalogue
                    .fields
                    .iter()
                    .any(|field| field.ephemeris.is_some())
            })
        {
            return Err(ObservationError::Unsupported {
                what: "combining fields that follow an ephemeris from several inputs",
            });
        }
        let merging = merge_bound(
            inputs
                .iter()
                .map(|input| catalogue_bytes(input.observation().catalogue()))
                .sum(),
        );
        ledger.admit("merging the catalogues", merging)?;
        let combined = combine::combine(&inputs, &groups, &options.tolerance)?;
        ledger.trim("the merged catalogue", merging, combined_bytes(&combined))?;
        if options.padding == PaddingRows::Flagged
            && combined
                .contributors
                .iter()
                .any(|contributors| contributors.len() > 1)
        {
            return Err(request_error(
                "partitions combined from several inputs are written with padding omitted \
                 (PaddingRows::Omitted): a padded row of one input could fall on an \
                 acquisition of another",
            ));
        }
        refuse_unwritable(&combined.catalogue)?;
        found.extend(combined.losses.iter().cloned());
        found.extend(msv2_ephemeris::filled_rows(&combined.catalogue));
        found.extend(
            msv2_ephemeris::followed(&combined.catalogue)
                .0
                .into_iter()
                .filter_map(|ephemeris| losses::extrapolated(ephemeris)),
        );
        let grids = occupancy::grids(&inputs, &combined, &mut ledger)?;
        // What writing the subtables defaults or leaves out, found by writing
        // them in memory, and where they put each partition; the catalogue
        // read back from them stays.
        let promised = written_bound(&combined);
        ledger.admit("the written catalogue", promised)?;
        let rehearsing = subtables_bytes(&combined.catalogue);
        let msv2_tables::Rehearsal {
            layout,
            losses: rehearsed,
            read_back,
        } = ledger.during("the subtables rehearsed", rehearsing, |_| {
            msv2_tables::rehearse(&combined.catalogue, &grids.spans, &combined.intents)
        })?;
        found.extend(rehearsed);
        found.sort();
        found.dedup();
        losses::refuse_unaccepted(&found, &options.accept)?;
        let columns: Vec<(OptionalMainColumn, Option<String>)> = names
            .into_iter()
            .zip(&combined.units)
            .map(|(name, units)| (name, unit(units)))
            .collect();
        let output = written(&combined, &layout, read_back);
        ledger.trim("the written catalogue", promised, catalogue_bytes(&output))?;
        let occupancy = grids.occupancy(
            &inputs,
            &combined,
            (options.padding, options.batch_bytes),
            &mut ledger,
        )?;
        if occupancy.rows.iter().sum::<usize>() == 0 {
            return Err(request_error(
                "the inputs hold nothing to write as an MSv2 row",
            ));
        }
        let data_columns: Vec<&'static str> =
            columns.iter().map(|(column, _)| column.name()).collect();
        let writing = writing::admit(
            &mut ledger,
            (&inputs, &combined, &pointing),
            (&data_columns, &occupancy, &options.history),
            options.batch_bytes,
        )?;
        Ok(Self {
            inputs,
            pointing,
            combined,
            columns,
            losses: found,
            occupancy,
            output,
            main: writing.plan,
            charges: writing.charges,
            history: options.history.clone(),
            padding: options.padding,
            batch_bytes: options.batch_bytes,
            creation_date: options.creation_date.clone(),
            publication,
        })
    }

    /// The memory the plan admitted against
    /// [`CreationOptions::memory_bytes`]: what it keeps, the most making it
    /// held, and what writing it will hold.
    #[must_use]
    pub fn charges(&self) -> MemoryCharges {
        self.charges
    }

    /// Every loss the MeasurementSet takes, all accepted, found before
    /// anything is written.
    #[must_use]
    pub fn losses(&self) -> &[Loss] {
        &self.losses
    }

    /// What reopening the MeasurementSet written gives, entity by entity:
    /// the merged antennas, fields, sources, states, processors and
    /// observing sessions, the windows a partition uses, the output's scans,
    /// and its partitions in the MSv2 reader's order with the states and
    /// feeds their rows are written with and their UVW frame as MAIN labels
    /// it. Each entity is read back from the subtables the plan wrote in
    /// memory, so a member written as MSv2's default ([`Loss::Defaulted`])
    /// reads as that default, and an MSv4 input's intents as the `STATE`
    /// rows written for them.
    #[must_use]
    pub fn output(&self) -> &Catalogue {
        &self.output
    }

    /// Entities the merge appended because a predicate lacked what it
    /// needs, each said once.
    #[must_use]
    pub fn notices(&self) -> &[String] {
        &self.combined.notices
    }

    /// Write the planned MeasurementSet, by one thread, in a staging
    /// directory beside its destination, then publish it: renamed into a
    /// new path, or exchanged with the MeasurementSet it replaces, which is
    /// then deleted. No input is written, except a replaced one, whose
    /// directory then holds the new MeasurementSet.
    ///
    /// # Errors
    ///
    /// A destination that appeared or changed since the plan was made.
    /// Weights that vary over channels, when padding was not omitted. Then
    /// any error reading an input or writing the MeasurementSet, which then
    /// is not published: the stage is removed, and a replaced
    /// MeasurementSet is as it was.
    pub fn write(self) -> Result<CreationReport, ObservationError> {
        let stage = match &self.publication {
            Publication::New(path) => Stage::reserve(path, "casa-rs-creating")?,
            Publication::Replace(replacing) => {
                Stage::reserve(replacing.path(), "casa-rs-replacing")?
            }
        };
        let created = self.create(stage.path());
        let Self {
            inputs,
            publication,
            losses,
            ..
        } = self;
        // The inputs' read handles go first: a replaced MeasurementSet's
        // directory is exchanged and deleted.
        drop(inputs);
        let published = created.and_then(|report| match publication {
            Publication::New(path) => stage.publish_new(&path).map(|()| report),
            Publication::Replace(replacing) => replacing
                .publish(stage.path())
                .map(|leftover| CreationReport { leftover, ..report }),
        });
        match published {
            Ok(report) => {
                stage.release();
                Ok(CreationReport { losses, ..report })
            }
            Err(error) => {
                // The stage, which the plan reserved, is the only output.
                stage.remove();
                Err(error)
            }
        }
    }

    /// Create the MeasurementSet at `staging`: its subtables, then MAIN's
    /// rows from a lockstep read of every input partition.
    fn create(&self, staging: &Path) -> Result<CreationReport, ObservationError> {
        let catalogue = &self.combined.catalogue;
        let optional: Vec<OptionalMainColumn> =
            self.columns.iter().map(|(column, _)| *column).collect();
        let builder = msv2_tables::builder(catalogue, &optional);
        let mut ms = MeasurementSet::create(staging, builder).map_err(storage)?;
        // The MAIN writers write into the directory before the save.
        fs::create_dir_all(staging).map_err(storage)?;
        for (column, unit) in &self.columns {
            if let Some(unit) = unit {
                TableQuantumDesc::with_unit(column.name(), unit)
                    .write(ms.main_table_mut())
                    .map_err(storage)?;
            }
        }
        let (layout, _) = msv2_tables::populate(
            &mut ms,
            catalogue,
            &self.occupancy.spans,
            &self.combined.intents,
        )?;
        msv2_ephemeris::mark_fields(&mut ms, catalogue)?;
        let pointed: Vec<Pointed<'_>> = self
            .inputs
            .iter()
            .zip(&self.pointing)
            .zip(&self.combined.rows)
            .map(|((input, plan), map)| Pointed {
                observation: input.observation(),
                plan,
                antennas: &map.antennas,
            })
            .collect();
        msv2_pointing::write(&mut ms, &pointed)?;
        super::msv2_identity::history(&mut ms, &self.history)?;
        let names = optional.iter().map(|column| column.name()).collect();
        let mut main = MainRows::start(
            (staging, self.main.clone()),
            catalogue,
            names,
            &self.occupancy,
            layout,
            self.padding,
        )?;
        let mut report = CreationReport::default();
        let mut row_states = RowStates::default();
        for (input, map) in self.inputs.iter().zip(&self.combined.rows) {
            let observation = input.observation();
            let mut ids = observation.catalogue().partition_ids();
            ids.sort_by_key(|id| map.partitions[id.0 as usize].output);
            for id in ids {
                let placement = &map.partitions[id.0 as usize];
                let per_row = match (&placement.state, input.msv2()) {
                    (StateIds::PerRow(_), Some(msv2)) => Some(msv2),
                    _ => None,
                };
                row_states.restart();
                let read = lockstep::read(
                    observation,
                    id,
                    &placement.groups,
                    Roles::ALL,
                    (self.batch_bytes, None),
                    |batches| {
                        let states = per_row
                            .map(|msv2| row_states.of(msv2, id, &batches[0]))
                            .transpose()?;
                        main.push(map, batches, states)
                    },
                )?;
                report.partitions += 1;
                report.batches += read.batches;
                report.samples += read.samples;
            }
        }
        report.rows = main.finish(&mut ms)?;
        msv2_ephemeris::write_tables(staging, catalogue, &self.creation_date)?;
        Ok(report)
    }
}

/// The data groups to write: `requested`, or every input's, `base` first.
fn selected_groups(
    inputs: &[Input],
    requested: Option<&[String]>,
) -> Result<Vec<String>, ObservationError> {
    let groups: Vec<String> = match requested {
        Some(requested) => requested.to_vec(),
        None => {
            let mut groups = vec!["base".to_string()];
            for input in inputs {
                for entry in &input.observation().catalogue().partitions {
                    for group in &entry.data_groups {
                        if !groups.contains(&group.name) {
                            groups.push(group.name.clone());
                        }
                    }
                }
            }
            groups
        }
    };
    if groups.first().map(String::as_str) != Some("base") {
        return Err(request_error(
            "a MeasurementSet is written with the base data group first",
        ));
    }
    if let Some(group) = groups
        .iter()
        .enumerate()
        .find_map(|(index, group)| groups[..index].contains(group).then_some(group))
    {
        return Err(request_error(format!(
            "data group {group} is selected twice"
        )));
    }
    Ok(groups)
}

/// The MSv2 column of a data group, as XRADIO's converter names them.
fn column(group: &str) -> Result<OptionalMainColumn, ObservationError> {
    match group {
        "base" => Ok(OptionalMainColumn::Data),
        "corrected" => Ok(OptionalMainColumn::CorrectedData),
        "model" => Ok(OptionalMainColumn::ModelData),
        other => Err(request_error(format!(
            "data group {other} has no MSv2 column: base (DATA), corrected \
             (CORRECTED_DATA) and model (MODEL_DATA) have"
        ))),
    }
}

/// The `QuantumUnits` a column of `units` declares: none for undeclared
/// units, as CASA's `DATA` declares none.
fn unit(units: &VisibilityUnits) -> Option<String> {
    match units {
        VisibilityUnits::Undeclared => None,
        VisibilityUnits::Jansky => Some("Jy".to_string()),
        VisibilityUnits::Declared(unit) => Some(unit.clone()),
    }
}

/// Refuse what the MSv2 written cannot say: another time scale than UTC
/// (`TIME` is written with the MeasurementSet's default reference), a feed
/// other than the one `FEED` row per antenna the writer writes, a window
/// without channels, a field in another frame than J2000, what its
/// subtables cannot say ([`msv2_tables::refuse_unwritable`]) and an
/// ephemeris its table cannot hold ([`msv2_ephemeris::refuse_unwritable`]).
fn refuse_unwritable(catalogue: &Catalogue) -> Result<(), ObservationError> {
    if catalogue
        .fields
        .iter()
        .any(|field| field.phase_centre.refer() != DirectionRef::J2000)
    {
        return Err(ObservationError::Unsupported {
            what: "writing MSv2 fields in another frame than J2000",
        });
    }
    for entry in &catalogue.partitions {
        if entry.frequencies_hz.is_empty() {
            return Err(ObservationError::Unsupported {
                what: "writing an MSv2 spectral window without channels",
            });
        }
        if entry.time_scale != EpochRef::UTC {
            return Err(ObservationError::Unsupported {
                what: "writing MSv2 times in another scale than UTC",
            });
        }
        if entry.feeds.is_some_and(|feeds| feeds != [0, 0]) {
            return Err(ObservationError::Unsupported {
                what: "writing MSv2 rows of feeds other than 0",
            });
        }
    }
    msv2_tables::refuse_unwritable(catalogue)?;
    msv2_ephemeris::refuse_unwritable(catalogue)
}

/// What reopening the written MeasurementSet gives of `combined`, whose
/// subtables `layout` places and read back as `subtables` with MAIN's
/// `uvw_frame`: every entity as the adapter reads it from the subtables
/// written (the members written as MSv2's defaults, and the states written
/// for intents, included); each partition naming the `SPECTRAL_WINDOW` row
/// and the states and feeds its rows are written with, its data groups' UVW
/// in the frame MAIN's keywords give, in the order the MSv2 reader keys
/// them (data description, `OBS_MODE`, observation).
fn written(
    combined: &Combined,
    layout: &msv2_tables::Layout,
    (subtables, uvw_frame): (Catalogue, casa_observation::UvwFrame),
) -> Catalogue {
    let mut catalogue = Catalogue {
        scans: combined.catalogue.scans.clone(),
        partitions: combined.catalogue.partitions.clone(),
        ..subtables
    };
    // The ephemerides fields follow are written as tables of their own
    // beside the subtables rehearsed (`msv2_ephemeris`).
    for (field, merged) in catalogue.fields.iter_mut().zip(&combined.catalogue.fields) {
        field.ephemeris.clone_from(&merged.ephemeris);
    }
    for (output, entry) in catalogue.partitions.iter_mut().enumerate() {
        entry.window = casa_observation::WindowId(
            u32::try_from(layout.spectral_window[output]).expect("a written window row"),
        );
        // Every row is written with feeds 0, which `refuse_unwritable` has
        // checked an MSv2 input states.
        entry.feeds = Some([0, 0]);
        for group in &mut entry.data_groups {
            group.uvw_frame = uvw_frame;
        }
        let mut states: Vec<casa_observation::StateId> = combined.contributors[output]
            .iter()
            .flat_map(
                |(input, id)| match &combined.rows[*input].partitions[id.0 as usize].state {
                    StateIds::One(state) => vec![*state],
                    StateIds::Intents => layout.intent_states[output].into_iter().collect(),
                    StateIds::PerRow(states) => states.clone(),
                },
            )
            .filter_map(|state| u32::try_from(state).ok().map(casa_observation::StateId))
            .collect();
        states.sort_unstable();
        states.dedup();
        entry.states = states;
    }
    let mut order: Vec<usize> = (0..catalogue.partitions.len()).collect();
    order.sort_by(|a, b| {
        (
            layout.data_description[*a],
            &combined.modes[*a],
            catalogue.partitions[*a].observation,
        )
            .cmp(&(
                layout.data_description[*b],
                &combined.modes[*b],
                catalogue.partitions[*b].observation,
            ))
    });
    let partitions = std::mem::take(&mut catalogue.partitions);
    catalogue.partitions = order
        .into_iter()
        .map(|index| partitions[index].clone())
        .collect();
    catalogue
}

/// An in-memory MeasurementSet holding no rows and the subtables an MSv2
/// converted from `observation` would hold, and the `SPECTRAL_WINDOW` row
/// of each partition: the namespace a calibration table solved on a store
/// without one (MSv4) persists. Antennas, fields and sessions keep their
/// catalogue order.
///
/// # Errors
///
/// As [`CreationPlan::new`] for what an MSv2's subtables cannot say.
#[cfg(feature = "msv4")]
pub(crate) fn catalogue_subtables(
    observation: &dyn casa_observation::Observation,
) -> Result<(MeasurementSet, Vec<i32>), ObservationError> {
    let catalogue = observation.catalogue();
    refuse_unwritable(catalogue)?;
    let spans = match catalogue
        .partitions
        .first()
        .and_then(|entry| entry.data_groups.first())
    {
        Some(group) => occupancy::spans(observation, &group.name)?,
        None => super::msv2_rows::Spans {
            time_range: [0.0, 0.0],
            observation_ranges: vec![[0.0, 0.0]; catalogue.observations.len()],
            field_first: vec![None; catalogue.fields.len()],
        },
    };
    let mut ms =
        MeasurementSet::create_memory(msv2_tables::builder(catalogue, &[])).map_err(storage)?;
    let intents = super::msv2_identity::intent_partitions(catalogue);
    let (layout, _) = msv2_tables::populate(&mut ms, catalogue, &spans, &intents)?;
    Ok((ms, layout.spectral_window))
}

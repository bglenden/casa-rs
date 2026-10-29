// SPDX-License-Identifier: LGPL-3.0-or-later
//! The subtables of an MSv2 a plan creates, from the catalogue: one antenna
//! per catalogue antenna, one field, observation, state, processor and
//! source record per catalogue entry ([`super::msv2_identity`],
//! [`super::msv2_sources`]), one spectral window per window a partition
//! uses, the feeds the partitions' receptors need, and polarizations and
//! data descriptions shared by the partitions whose content is the same, so
//! long as no two partitions share a data description, `OBS_MODE` and
//! observation (an MSv2 would read them back as one). The layout says where
//! each partition's rows point. What the catalogue leaves unstated is
//! written as MSv2's default, and noted ([`Notes`]) as a loss.

use std::collections::{BTreeMap, BTreeSet};

use casa_observation::{AntennaId, Catalogue, Correlation, ObservationError, Receptors};
use casa_types::quanta::Unit;
use num_complex::Complex32;

use super::msv2_rows::Spans;
use super::{Loss, msv2_identity, msv2_sources};
use crate::observation_io::update::{request_error, storage};
use crate::schema::{self, SubtableId};
use crate::subtable_rows::{
    b, complex_array, f, f64_array, i, i32_array, row_from_defs, s, string_array, subtable_mut,
};
use crate::{MeasurementSet, MeasurementSetBuilder, OptionalMainColumn};

/// The MeasurementSet to create for `catalogue` with the visibility
/// `columns`: a `SOURCE` subtable when the catalogue holds sources or some
/// field names its source.
pub(super) fn builder(
    catalogue: &Catalogue,
    columns: &[OptionalMainColumn],
) -> MeasurementSetBuilder {
    let builder = columns
        .iter()
        .fold(MeasurementSetBuilder::new(), |builder, column| {
            builder.with_main_column(*column)
        });
    if !catalogue.sources.is_empty()
        || catalogue
            .fields
            .iter()
            .any(|field| field.source_name.is_some())
    {
        builder.with_optional_subtable(SubtableId::Source)
    } else {
        builder
    }
}

/// Where each partition's rows point, in catalogue partition order.
pub(super) struct Layout {
    /// `DATA_DESC_ID` of each partition.
    pub(super) data_description: Vec<i32>,
    /// `SPECTRAL_WINDOW` row of each partition.
    pub(super) spectral_window: Vec<i32>,
    /// The `STATE_ID` of each partition's state of intents, for its rows
    /// that state intents but no state.
    pub(super) intent_states: Vec<Option<i32>>,
}

/// What writing a catalogue's subtables writes as MSv2's default, or
/// leaves out, each a loss.
#[derive(Default)]
pub(super) struct Notes(BTreeSet<Loss>);

impl Notes {
    /// `value`, or when unstated `default`, noting `column` as defaulted.
    pub(super) fn or<T>(
        &mut self,
        value: Option<T>,
        column: &str,
        default: impl FnOnce() -> T,
    ) -> T {
        value.unwrap_or_else(|| {
            self.defaulted(column);
            default()
        })
    }

    /// Note `column` as written with MSv2's default for some row.
    pub(super) fn defaulted(&mut self, column: &str) {
        self.0.insert(Loss::Defaulted(column.to_string()));
    }

    /// Note `loss`.
    pub(super) fn lost(&mut self, loss: Loss) {
        self.0.insert(loss);
    }

    /// Every loss noted, in order.
    pub(super) fn into_losses(self) -> Vec<Loss> {
        self.0.into_iter().collect()
    }
}

/// Refuse what an MSv2's subtables cannot say of `catalogue`, before
/// anything is written: a scan without a casacore number, feeds whose
/// receptors differ, or a correlation product the feeds cannot name (a
/// Stokes product, or a receptor they lack).
pub(super) fn refuse_unwritable(catalogue: &Catalogue) -> Result<(), ObservationError> {
    scan_numbers(catalogue)?;
    let receptors = feed_receptors(catalogue)?;
    for entry in &catalogue.partitions {
        for correlation in &entry.correlations {
            receptor_pair(*correlation, receptors)?;
        }
    }
    Ok(())
}

/// Each catalogue scan's `SCAN_NUMBER`.
fn scan_numbers(catalogue: &Catalogue) -> Result<Vec<i32>, ObservationError> {
    catalogue
        .scans
        .iter()
        .map(|scan| {
            scan.number.ok_or_else(|| {
                request_error(format!(
                    "scan {} has no casacore scan number to write as SCAN_NUMBER",
                    scan.name
                ))
            })
        })
        .collect()
}

/// The receptors `CORR_PRODUCT` indexes: those of every antenna a
/// partition states receptors for, which must agree (ignoring case, as
/// [`receptor_pair`] matches them).
fn feed_receptors(catalogue: &Catalogue) -> Result<&[String], ObservationError> {
    let mut fed = catalogue
        .partitions
        .iter()
        .flat_map(|entry| entry.receptors.values())
        .map(|receptors| receptors.types.as_slice())
        .filter(|types| !types.is_empty());
    let first = fed.next().unwrap_or(&[]);
    let same = |receptors: &[String]| {
        receptors.len() == first.len()
            && receptors
                .iter()
                .zip(first)
                .all(|(label, first)| label.eq_ignore_ascii_case(first))
    };
    if fed.any(|receptors| !same(receptors)) {
        return Err(ObservationError::Unsupported {
            what: "writing MSv2 polarizations for feeds whose receptors differ",
        });
    }
    Ok(first)
}

/// The subtables of `catalogue` written into a MeasurementSet in memory,
/// for where they place each partition and what writing them notes and
/// refuses: a plan learns its losses, and that its subtables can be
/// written, before anything is. `intents` says which partitions need a
/// state of intents.
pub(super) fn rehearse(
    catalogue: &Catalogue,
    spans: &Spans,
    intents: &[bool],
) -> Result<(Layout, Vec<Loss>), ObservationError> {
    let mut ms = MeasurementSet::create_memory(builder(catalogue, &[])).map_err(storage)?;
    populate(&mut ms, catalogue, spans, intents)
        .map(|(layout, notes)| (layout, notes.into_losses()))
}

/// Fill `ms`'s subtables from `catalogue`, whose data span the times
/// `spans` gives (MJD seconds), `intents` saying which partitions need a
/// state of intents; [`refuse_unwritable`] has accepted it. Antennas,
/// fields, sessions, states, processors and sources keep their catalogue
/// order, and windows theirs among those a partition uses.
pub(super) fn populate(
    ms: &mut MeasurementSet,
    catalogue: &Catalogue,
    spans: &Spans,
    intents: &[bool],
) -> Result<(Layout, Notes), ObservationError> {
    let time_range = spans.time_range;
    let mut notes = Notes::default();
    antennas(ms, catalogue, &mut notes)?;
    let named_sources = msv2_sources::named(catalogue);
    msv2_identity::fields(ms, catalogue, (&named_sources, time_range[0]), &mut notes)?;
    msv2_identity::observations(ms, catalogue, &spans.observation_ranges, &mut notes)?;
    let window_rows = msv2_identity::spectral_windows(ms, catalogue, &mut notes)?;
    let states = msv2_identity::states(ms, catalogue, intents, &mut notes)?;
    msv2_identity::processors(ms, catalogue, &mut notes)?;
    msv2_sources::write(
        ms,
        catalogue,
        (&named_sources, &window_rows),
        time_range,
        &mut notes,
    )?;
    let mut polarizations: Vec<&[Correlation]> = Vec::new();
    let mut descriptions: Vec<(i32, usize)> = Vec::new();
    let mut keys: Vec<(usize, &str, u32)> = Vec::new();
    let mut layout = Layout {
        data_description: Vec::new(),
        spectral_window: Vec::new(),
        intent_states: states.intent_states,
    };
    for (index, entry) in catalogue.partitions.iter().enumerate() {
        let window = window_rows
            .get(entry.window.0 as usize)
            .copied()
            .flatten()
            .ok_or_else(|| request_error("a partition names a window the catalogue lacks"))?;
        let polarization = position_or_push(&mut polarizations, &entry.correlations, |known| {
            *known == entry.correlations.as_slice()
        });
        // Partitions an MSv2 keys alike (data description, OBS_MODE,
        // observation) would read back as one: a later one gets a data
        // description of its own.
        let key = |description| {
            (
                description,
                states.modes[index].as_str(),
                entry.observation.0,
            )
        };
        let description = (0..descriptions.len())
            .find(|index| {
                descriptions[*index] == (window, polarization) && !keys.contains(&key(*index))
            })
            .unwrap_or_else(|| {
                descriptions.push((window, polarization));
                descriptions.len() - 1
            });
        keys.push(key(description));
        layout.data_description.push(description as i32);
        layout.spectral_window.push(window);
    }
    feeds(ms, catalogue, &layout.spectral_window, time_range)?;
    let receptors = feed_receptors(catalogue)?;
    for correlations in &polarizations {
        polarization(ms, correlations, receptors)?;
    }
    for (window, polarization) in &descriptions {
        subtable_mut(ms, SubtableId::DataDescription)
            .and_then(|table| {
                table
                    .add_row(row_from_defs(
                        schema::data_description::REQUIRED_COLUMNS,
                        &[
                            ("SPECTRAL_WINDOW_ID", i(*window)),
                            ("POLARIZATION_ID", i(*polarization as i32)),
                            ("FLAG_ROW", b(false)),
                        ],
                    ))
                    .map_err(Into::into)
            })
            .map_err(storage)?;
    }
    Ok((layout, notes))
}

/// The index of the first of `known` that `is` matches, after pushing
/// `item` when none does.
fn position_or_push<T: Copy>(known: &mut Vec<T>, item: T, is: impl Fn(&T) -> bool) -> usize {
    known.iter().position(is).unwrap_or_else(|| {
        known.push(item);
        known.len() - 1
    })
}

fn antennas(
    ms: &mut MeasurementSet,
    catalogue: &Catalogue,
    notes: &mut Notes,
) -> Result<(), ObservationError> {
    let metres = Unit::new("m").expect("metres");
    let table = subtable_mut(ms, SubtableId::Antenna).map_err(storage)?;
    for antenna in &catalogue.antennas {
        let diameter = antenna
            .dish_diameter
            .get_value_in(&metres)
            .map_err(|_| request_error("a dish diameter is not a length"))?;
        let flagged = notes.or(antenna.flagged, "ANTENNA FLAG_ROW", || false);
        let row = row_from_defs(
            schema::antenna::REQUIRED_COLUMNS,
            &[
                ("NAME", s(&antenna.name)),
                ("STATION", s(&antenna.station)),
                ("TYPE", s("GROUND-BASED")),
                ("MOUNT", s(&antenna.mount)),
                ("POSITION", f64_array(&antenna.position.as_itrf(), vec![3])),
                ("OFFSET", f64_array(&[0.0; 3], vec![3])),
                ("DISH_DIAMETER", f(diameter)),
                ("FLAG_ROW", b(flagged)),
            ],
        );
        table.add_row(row).map_err(storage)?;
    }
    Ok(())
}

/// The `FEED` rows of `catalogue`'s antennas, each partition in its window
/// (`windows`: each partition's), written as [`feed_plan`] plans them.
fn feeds(
    ms: &mut MeasurementSet,
    catalogue: &Catalogue,
    windows: &[i32],
    time_range: [f64; 2],
) -> Result<(), ObservationError> {
    let named: Vec<Vec<AntennaId>> = catalogue
        .partitions
        .iter()
        .map(|entry| {
            let mut named: Vec<AntennaId> = entry
                .baselines
                .iter()
                .flat_map(|baseline| [baseline.antenna1, baseline.antenna2])
                .collect();
            named.sort_unstable();
            named.dedup();
            named
        })
        .collect();
    let partitions = catalogue
        .partitions
        .iter()
        .zip(windows)
        .zip(&named)
        .map(|((entry, window), named)| (*window, named.as_slice(), &entry.receptors));
    for (antenna, window, receptors) in feed_plan(catalogue.antennas.len(), partitions)? {
        feed_row(ms, antenna, window, receptors, time_range)?;
    }
    Ok(())
}

/// The `FEED` rows (antenna, window, receptors) of `antennas` antennas that
/// read back as each partition's receptors (`partitions`: its window, the
/// antennas its baselines name and the receptors it states) under
/// casacore's rule (`MSIter::setFeedInfo`, as XRADIO's converter reads it
/// too): once any row names a window, a window takes its own rows only and
/// rows for every window (-1) are ignored. So when every antenna states the
/// same receptors in every window it appears in, each has one row for every
/// window (-1); otherwise each has a row per window it states receptors in,
/// and a window where it states none (ALMA's WVR window) has no row, as
/// ALMA writes `FEED`. An antenna stating none has no row, unless no antenna
/// states any, when each has a row of none. Partitions of one window that
/// state an antenna's receptors differently are refused: one `FEED` row
/// holds them.
fn feed_plan<'a>(
    antennas: usize,
    partitions: impl Iterator<Item = (i32, &'a [AntennaId], &'a BTreeMap<AntennaId, Receptors>)>,
) -> Result<Vec<(usize, i32, &'a Receptors)>, ObservationError> {
    static NONE: Receptors = Receptors {
        types: Vec::new(),
        angles: Vec::new(),
    };
    // Each antenna's receptors in each window it appears in.
    let mut seen: Vec<Vec<(i32, Option<&Receptors>)>> = vec![Vec::new(); antennas];
    for (window, named, receptors) in partitions {
        for antenna in named.iter().chain(receptors.keys()) {
            let Some(own) = seen.get_mut(antenna.0 as usize) else {
                return Err(request_error("receptors of an antenna the catalogue lacks"));
            };
            let stated = receptors.get(antenna);
            match own.iter().find(|(known, _)| *known == window) {
                Some((_, known)) if *known != stated => {
                    return Err(ObservationError::Unsupported {
                        what: "partitions of one window with different receptors for an antenna",
                    });
                }
                Some(_) => {}
                None => own.push((window, stated)),
            }
        }
    }
    if !seen.iter().flatten().any(|(_, stated)| stated.is_some()) {
        return Ok((0..antennas).map(|antenna| (antenna, -1, &NONE)).collect());
    }
    let per_window = seen
        .iter()
        .any(|own| own.iter().any(|(_, stated)| *stated != own[0].1));
    Ok(seen
        .into_iter()
        .enumerate()
        .flat_map(|(antenna, own)| {
            let rows: Vec<(i32, &Receptors)> = if per_window {
                own.into_iter()
                    .filter_map(|(window, stated)| Some((window, stated?)))
                    .collect()
            } else {
                own.first()
                    .and_then(|(_, stated)| *stated)
                    .map(|receptors| (-1, receptors))
                    .into_iter()
                    .collect()
            };
            rows.into_iter()
                .map(move |(window, receptors)| (antenna, window, receptors))
        })
        .collect())
}

/// One `FEED` row: antenna `index`'s feed 0 in spectral window `window`.
fn feed_row(
    ms: &mut MeasurementSet,
    index: usize,
    window: i32,
    own: &Receptors,
    time_range: [f64; 2],
) -> Result<(), ObservationError> {
    let radians = Unit::new("rad").expect("radians");
    let receptors = own.types.len();
    let angles = own
        .angles
        .iter()
        .map(|angle| {
            angle
                .get_value_in(&radians)
                .map_err(|_| request_error("a receptor angle is not an angle"))
        })
        .collect::<Result<Vec<_>, _>>()?;
    let types: Vec<&str> = own.types.iter().map(String::as_str).collect();
    let identity: Vec<Complex32> = (0..receptors * receptors)
        .map(|k| {
            if k % (receptors + 1) == 0 {
                Complex32::new(1.0, 0.0)
            } else {
                Complex32::new(0.0, 0.0)
            }
        })
        .collect();
    let row = row_from_defs(
        schema::feed::REQUIRED_COLUMNS,
        &[
            ("ANTENNA_ID", i(index as i32)),
            ("BEAM_ID", i(-1)),
            (
                "BEAM_OFFSET",
                f64_array(&vec![0.0; 2 * receptors], vec![2, receptors]),
            ),
            ("FEED_ID", i(0)),
            ("INTERVAL", f(time_range[1] - time_range[0])),
            ("NUM_RECEPTORS", i(receptors as i32)),
            (
                "POL_RESPONSE",
                complex_array(&identity, vec![receptors, receptors]),
            ),
            ("POLARIZATION_TYPE", string_array(&types, vec![receptors])),
            ("POSITION", f64_array(&[0.0; 3], vec![3])),
            ("RECEPTOR_ANGLE", f64_array(&angles, vec![receptors])),
            ("SPECTRAL_WINDOW_ID", i(window)),
            ("TIME", f(0.5 * (time_range[0] + time_range[1]))),
        ],
    );
    subtable_mut(ms, SubtableId::Feed)
        .and_then(|table| table.add_row(row).map_err(Into::into))
        .map_err(storage)
}

/// The receptors a correlation product correlates, by their positions in
/// the feed's `receptors` (`R`, `L`, `X`, `Y`), as `CORR_PRODUCT` holds
/// them.
fn receptor_pair(
    correlation: Correlation,
    receptors: &[String],
) -> Result<[i32; 2], ObservationError> {
    use Correlation::{LL, LR, RL, RR, XX, XY, YX, YY};
    let [first, second] = match correlation {
        RR => ["R", "R"],
        RL => ["R", "L"],
        LR => ["L", "R"],
        LL => ["L", "L"],
        XX => ["X", "X"],
        XY => ["X", "Y"],
        YX => ["Y", "X"],
        YY => ["Y", "Y"],
        _ => {
            return Err(ObservationError::Unsupported {
                what: "writing MSv2 correlation products other than a feed pair's",
            });
        }
    };
    let position = |label: &str| {
        receptors
            .iter()
            .position(|receptor| receptor.eq_ignore_ascii_case(label))
            .map(|index| index as i32)
            .ok_or(ObservationError::Unsupported {
                what: "writing an MSv2 correlation product of a receptor the feeds lack",
            })
    };
    Ok([position(first)?, position(second)?])
}

fn polarization(
    ms: &mut MeasurementSet,
    correlations: &[Correlation],
    receptors: &[String],
) -> Result<(), ObservationError> {
    let count = correlations.len();
    let types: Vec<i32> = correlations
        .iter()
        .map(|correlation| correlation.casacore_code())
        .collect();
    let mut products = vec![0; 2 * count];
    for (index, correlation) in correlations.iter().enumerate() {
        let [first, second] = receptor_pair(*correlation, receptors)?;
        products[index] = first;
        products[count + index] = second;
    }
    let row = row_from_defs(
        schema::polarization::REQUIRED_COLUMNS,
        &[
            ("NUM_CORR", i(count as i32)),
            ("CORR_TYPE", i32_array(&types, vec![count])),
            ("CORR_PRODUCT", i32_array(&products, vec![2, count])),
            ("FLAG_ROW", b(false)),
        ],
    );
    subtable_mut(ms, SubtableId::Polarization)
        .and_then(|table| table.add_row(row).map_err(Into::into))
        .map_err(storage)
}

#[cfg(test)]
mod tests {
    use casa_observation::Correlation;

    use std::collections::BTreeMap;

    use casa_observation::{AntennaId, Receptors};
    use casa_types::quanta::Quantity;

    use super::{feed_plan, position_or_push, receptor_pair};

    #[test]
    fn products_name_receptors_by_their_place_in_the_feed() {
        let feed = |labels: &[&str]| labels.iter().map(ToString::to_string).collect::<Vec<_>>();
        assert_eq!(
            receptor_pair(Correlation::RL, &feed(&["R", "L"])).unwrap(),
            [0, 1]
        );
        assert_eq!(
            receptor_pair(Correlation::XY, &feed(&["Y", "X"])).unwrap(),
            [1, 0]
        );
        assert_eq!(
            receptor_pair(Correlation::LL, &feed(&["L"])).unwrap(),
            [0, 0]
        );
        assert!(receptor_pair(Correlation::RR, &feed(&["X", "Y"])).is_err());
    }

    #[test]
    fn equal_content_shares_one_row() {
        let mut known = Vec::new();
        assert_eq!(position_or_push(&mut known, 3, |k| *k == 3), 0);
        assert_eq!(position_or_push(&mut known, 5, |k| *k == 5), 1);
        assert_eq!(position_or_push(&mut known, 3, |k| *k == 3), 0);
        assert_eq!(known, [3, 5]);
    }

    /// Receptors of angle `angle` for each antenna `antennas` names.
    fn receptors(antennas: &[u32], angle: f64) -> BTreeMap<AntennaId, Receptors> {
        antennas
            .iter()
            .map(|antenna| {
                (
                    AntennaId(*antenna),
                    Receptors {
                        types: vec!["R".to_string(), "L".to_string()],
                        angles: vec![Quantity::new(angle, "rad").expect("rad"); 2],
                    },
                )
            })
            .collect()
    }

    /// The plan for three antennas whose partitions (window, antennas
    /// named, receptors) are `partitions`, as (antenna, window, first angle,
    /// receptors).
    fn plan(
        partitions: &[(i32, Vec<AntennaId>, BTreeMap<AntennaId, Receptors>)],
    ) -> Vec<(usize, i32, f64, usize)> {
        feed_plan(
            3,
            partitions
                .iter()
                .map(|(window, named, own)| (*window, named.as_slice(), own)),
        )
        .unwrap()
        .into_iter()
        .map(|(antenna, window, own)| {
            let angle = own.angles.first().map_or(f64::NAN, Quantity::value);
            (antenna, window, angle, own.types.len())
        })
        .collect()
    }

    /// Partitions naming antennas 0 and 1 in `window`, stating `receptors`.
    fn named(
        window: i32,
        receptors: BTreeMap<AntennaId, Receptors>,
    ) -> (i32, Vec<AntennaId>, BTreeMap<AntennaId, Receptors>) {
        (window, vec![AntennaId(0), AntennaId(1)], receptors)
    }

    /// Windows that agree give each antenna stating receptors one row for
    /// every window; once one antenna differs by window, or a window states
    /// none for an antenna it names (ALMA's WVR window), each has a row per
    /// window it states receptors in. Rows of none are written only when no
    /// antenna states any.
    #[test]
    fn feed_rows_are_per_window_once_any_antenna_differs() {
        let agree = plan(&[
            named(0, receptors(&[0, 1], 0.0)),
            named(1, receptors(&[0, 1], 0.0)),
        ]);
        assert_eq!(agree, [(0, -1, 0.0, 2), (1, -1, 0.0, 2)]);

        let mut differ = receptors(&[0, 1], 0.0);
        differ.extend(receptors(&[0], 0.25));
        let mixed = plan(&[named(0, receptors(&[0, 1], 0.0)), named(1, differ)]);
        assert_eq!(
            mixed,
            [
                (0, 0, 0.0, 2),
                (0, 1, 0.25, 2),
                (1, 0, 0.0, 2),
                (1, 1, 0.0, 2)
            ]
        );

        let wvr = plan(&[named(0, receptors(&[0, 1], 0.0)), named(1, BTreeMap::new())]);
        assert_eq!(wvr, [(0, 0, 0.0, 2), (1, 0, 0.0, 2)], "no rows in window 1");

        let none = plan(&[named(0, BTreeMap::new())]);
        assert_eq!(
            none.iter()
                .map(|row| (row.0, row.1, row.3))
                .collect::<Vec<_>>(),
            [(0, -1, 0), (1, -1, 0), (2, -1, 0)]
        );

        let clash = [
            named(0, receptors(&[0], 0.0)),
            named(0, receptors(&[0], 0.5)),
        ];
        assert!(
            feed_plan(
                3,
                clash
                    .iter()
                    .map(|(window, antennas, own)| (*window, antennas.as_slice(), own))
            )
            .is_err(),
            "one window, two sets"
        );
    }
}

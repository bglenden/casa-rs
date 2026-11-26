// SPDX-License-Identifier: LGPL-3.0-or-later
//! Baseline and integration axes for the public MS visibility record.

use std::borrow::Cow;
use std::cmp::Reverse;
use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::f64::consts::{PI, TAU};
use std::sync::{Arc, OnceLock};

use casa_core::measures::direction::{DirectionRef, MDirection};
use casa_core::measures::{EpochRef, MEpoch, MeasFrame, MeasuresProvider};
use casa_core::{ArrayValue, RecordField, RecordValue, Value};
use casa_tables::{RequiredScalarColumnValues, Table};
use ndarray::ArrayD;

use super::{averaging, input, shaped, typed_scalar_column, vector, visibility_product};
use casa_ms::subtables::{FieldDirectionColumn, MetadataDirection, SubTable};
use casa_ms::subtables::{TimeColumn, antenna_position};
use casa_ms::{MeasurementSet, MsResult};

pub(super) fn rows_by_time<'a>(main: &Table, rows: &'a [usize]) -> MsResult<Cow<'a, [usize]>> {
    if rows.len() < 2 {
        return Ok(Cow::Borrowed(rows));
    }
    let values = main
        .column_accessor("TIME")?
        .scalar_cells_owned_for_rows(rows)?;
    let RequiredScalarColumnValues::Float64(times) = typed_scalar_column("TIME", values)? else {
        return Err(input("TIME must contain a double"));
    };
    if times.iter().any(|time| !time.is_finite()) {
        return Err(input("TIME contains a nonfinite value"));
    }
    if times.is_sorted() {
        return Ok(Cow::Borrowed(rows));
    }
    let mut ordered = times
        .into_iter()
        .zip(rows.iter().copied())
        .collect::<Vec<_>>();
    ordered.sort_by(|(left, _), (right, _)| left.partial_cmp(right).unwrap());
    Ok(Cow::Owned(
        ordered.into_iter().map(|(_, row)| row).collect(),
    ))
}

pub(super) struct BaselineGrid {
    baselines: BTreeMap<(i32, i32), usize>,
    explicit_axis: Option<Vec<((i32, i32), usize)>>,
    baseline_count: usize,
    times: Vec<f64>,
    first_rows: Vec<usize>,
    row_count: usize,
    legacy: bool,
    // The final source row for each occupied slot, ordered in Fortran layout.
    occupied: Vec<(usize, usize)>,
}

fn baseline_columns(
    columns: &HashMap<String, RequiredScalarColumnValues>,
) -> MsResult<(&[f64], &[i32], &[i32])> {
    let (
        RequiredScalarColumnValues::Float64(time),
        RequiredScalarColumnValues::Int32(first),
        RequiredScalarColumnValues::Int32(second),
    ) = (&columns["TIME"], &columns["ANTENNA1"], &columns["ANTENNA2"])
    else {
        return Err(input("invalid TIME or antenna ID column type"));
    };
    Ok((time, first, second))
}

impl BaselineGrid {
    pub(super) fn new(
        columns: &HashMap<String, RequiredScalarColumnValues>,
        selection: Option<&[(i32, i32)]>,
        gap: i32,
    ) -> MsResult<Self> {
        Self::build(columns, selection, gap, false)
    }

    pub(super) fn legacy(
        columns: &HashMap<String, RequiredScalarColumnValues>,
        selection: Option<&[(i32, i32)]>,
        gap: i32,
    ) -> MsResult<Self> {
        Self::build(columns, selection, gap, true)
    }

    fn build(
        columns: &HashMap<String, RequiredScalarColumnValues>,
        selection: Option<&[(i32, i32)]>,
        gap: i32,
        legacy: bool,
    ) -> MsResult<Self> {
        let (time, first, second) = baseline_columns(columns)?;
        if first.len() != time.len() || second.len() != time.len() {
            return Err(input("antenna ID and TIME column lengths differ"));
        }
        if time.iter().any(|time| !time.is_finite()) {
            return Err(input("TIME contains a nonfinite value"));
        }

        let row_baselines = first
            .iter()
            .copied()
            .zip(second.iter().copied())
            .collect::<Vec<_>>();
        let explicit = selection.filter(|_| legacy);
        let mut ordered = BTreeSet::new();
        if explicit.is_none() {
            ordered.extend(selection.unwrap_or(&row_baselines).iter().copied());
        }
        let mut explicit_axis = explicit.map(|pairs| Vec::with_capacity(pairs.len()));
        let mut previous = None;
        let mut baseline_count = 0_usize;
        let mut baselines = BTreeMap::new();
        let gap = gap.max(0) as usize;
        for pair in explicit.into_iter().flatten().copied().chain(ordered) {
            let antenna = pair.0;
            let increase = if previous.is_some_and(|prior| antenna > prior) {
                1 + gap
            } else {
                1
            };
            baseline_count = baseline_count
                .checked_add(increase)
                .filter(|&length| length <= i32::MAX as usize)
                .ok_or_else(|| input("baseline axis exceeds the supported length"))?;
            let slot = baseline_count - 1;
            baselines.insert(pair, slot);
            if let Some(axis) = &mut explicit_axis {
                axis.push((pair, slot));
            }
            previous = Some(antenna);
        }

        let mut order = (0..time.len()).collect::<Vec<_>>();
        let descriptions = if legacy {
            let RequiredScalarColumnValues::Int32(descriptions) = &columns["DATA_DESC_ID"] else {
                return Err(input("DATA_DESC_ID must contain an integer"));
            };
            Some(descriptions)
        } else {
            order.sort_unstable_by(|&left, &right| {
                // Finite times, including signed zero, use numeric ordering.
                time[left]
                    .partial_cmp(&time[right])
                    .unwrap()
                    .then(left.cmp(&right))
            });
            None
        };
        let mut times = Vec::new();
        let mut first_rows = Vec::new();
        let mut occupied = Vec::with_capacity(order.len());
        let mut time_start = 0_usize;
        let mut descriptions_at_time = HashMap::new();
        for row in order {
            let new_time = times.last() != Some(&time[row]);
            let description = descriptions.map(|ids| ids[row]);
            if new_time {
                descriptions_at_time.clear();
            }
            let prior_slot = description.and_then(|id| descriptions_at_time.get(&id).copied());
            if let Some(slot) = prior_slot {
                time_start = slot;
            } else if new_time || description.is_some() {
                time_start = times
                    .len()
                    .checked_mul(baseline_count)
                    .and_then(|start| start.checked_add(baseline_count).map(|_| start))
                    .ok_or_else(|| input("baseline grid exceeds the supported length"))?;
                times.push(time[row]);
                first_rows.push(row);
                if let Some(description) = description {
                    descriptions_at_time.insert(description, time_start);
                }
            }
            if let Some(&baseline) = baselines.get(&row_baselines[row]) {
                occupied.push((time_start + baseline, row));
            }
        }
        occupied.sort_unstable_by_key(|&(slot, row)| (slot, Reverse(row)));
        occupied.dedup_by_key(|entry| entry.0);
        if legacy {
            // Metadata for each slot comes from its first populated baseline.
            for (slot, row) in occupied.iter().rev() {
                first_rows[slot / baseline_count] = *row;
            }
        }
        Ok(Self {
            baselines,
            explicit_axis,
            baseline_count,
            times,
            first_rows,
            row_count: time.len(),
            legacy,
            occupied,
        })
    }

    pub(super) fn first_row(&self) -> Option<usize> {
        self.first_rows.first().copied()
    }

    pub(super) fn baseline_count(&self) -> usize {
        self.baseline_count
    }

    pub(super) fn time_count(&self) -> usize {
        self.times.len()
    }

    pub(super) fn write_positions(
        &self,
        columns: &HashMap<String, RequiredScalarColumnValues>,
    ) -> MsResult<Vec<[usize; 2]>> {
        let (times, first, second) = baseline_columns(columns)?;
        // A collapsed grid slot can supply several physical rows on write.
        times
            .iter()
            .zip(first)
            .zip(second)
            .map(|((&time, &first), &second)| {
                let baseline = self
                    .baselines
                    .get(&(first, second))
                    .copied()
                    .ok_or_else(|| input("putdata baseline axis omits a selected row"))?;
                let integration = self
                    .times
                    .binary_search_by(|value| value.partial_cmp(&time).unwrap())
                    .expect("the write grid was constructed from these finite times");
                Ok([baseline, integration])
            })
            .collect()
    }

    pub(super) fn reduction_rows(&self) -> impl Iterator<Item = (usize, usize)> + '_ {
        self.occupied
            .iter()
            .map(|&(slot, source)| (slot % self.baseline_count, source))
    }

    pub(super) fn occupied_slots(&self) -> &[(usize, usize)] {
        &self.occupied
    }

    fn axis_entries(&self) -> impl Iterator<Item = ((i32, i32), usize)> + '_ {
        self.explicit_axis.iter().flatten().copied().chain(
            self.baselines
                .iter()
                .filter(|_| self.explicit_axis.is_none())
                .map(|(&pair, &slot)| (pair, slot)),
        )
    }

    pub(super) fn numbers(&self) -> MsResult<ArrayValue> {
        let mut numbers = filled(self.baseline_count, -1)?;
        for ((first, second), slot) in self.axis_entries() {
            numbers[slot] = first.wrapping_mul(1000).wrapping_add(second);
        }
        Ok(ArrayValue::Int32(vector(numbers)))
    }

    pub(super) fn axes(
        &self,
        ms: &MeasurementSet,
        names: &BTreeSet<String>,
        columns: &HashMap<String, RequiredScalarColumnValues>,
        measures: Option<&OnceLock<Arc<dyn MeasuresProvider>>>,
        time_source: (&Table, usize),
        average: bool,
    ) -> MsResult<(RecordValue, RecordValue)> {
        let antenna = ms.antenna()?;
        let prefix = if self.legacy && antenna.row_count() != 0 {
            let first = antenna.name(0)?;
            let last = antenna.name(antenna.row_count() - 1)?;
            first
                .chars()
                .zip(last.chars())
                .take_while(|(a, b)| a == b)
                .map(|(character, _)| character.len_utf8())
                .sum::<usize>()
        } else {
            0
        };
        let mut metadata = HashMap::new();
        let mut labels = filled(self.baseline_count, String::new())?;
        let mut shorts = filled(self.baseline_count, String::new())?;
        let mut lengths = filled(self.baseline_count, if self.legacy { -1.0 } else { 0.0 })?;
        for ((first, second), slot) in self.axis_entries() {
            let first = usize::try_from(first).map_err(|_| input("negative antenna ID"))?;
            let second = usize::try_from(second).map_err(|_| input("negative antenna ID"))?;
            for id in [first, second] {
                if let std::collections::hash_map::Entry::Vacant(entry) = metadata.entry(id) {
                    let name = antenna.name(id)?;
                    let short = if self.legacy {
                        if prefix > 1 && name.is_char_boundary(prefix) {
                            prefix
                        } else {
                            0
                        }
                    } else {
                        name.char_indices().rev().nth(1).map_or(0, |(i, _)| i)
                    };
                    let short = name[short..].to_owned();
                    entry.insert((name, short, antenna.position(id)?));
                }
            }
            let (left, left_short, left_pos) = &metadata[&first];
            let (right, right_short, right_pos) = &metadata[&second];
            labels[slot] = format!("{left}-{right}");
            shorts[slot] = format!("{left_short}-{right_short}");
            lengths[slot] = left_pos
                .iter()
                .zip(right_pos)
                .map(|(left, right)| (left - right).powi(2))
                .sum::<f64>()
                .sqrt();
        }
        let numbers = if self.legacy {
            ArrayValue::Int32(vector(
                self.axis_entries()
                    .map(|((a, b), _)| a.wrapping_mul(1000).wrapping_add(b))
                    .collect(),
            ))
        } else {
            self.numbers()?
        };
        let ifr = RecordValue::new(vec![
            RecordField::new("ifr_number", Value::Array(numbers)),
            RecordField::new("ifr_name", Value::Array(ArrayValue::String(vector(labels)))),
            RecordField::new(
                "ifr_shortname",
                Value::Array(ArrayValue::String(vector(shorts))),
            ),
            RecordField::new(
                "baseline",
                Value::Array(ArrayValue::Float64(vector(lengths))),
            ),
        ]);
        let time = self.time_axis(ms, names, columns, measures, time_source, average)?;
        Ok((ifr, time))
    }

    fn time_axis(
        &self,
        ms: &MeasurementSet,
        names: &BTreeSet<String>,
        columns: &HashMap<String, RequiredScalarColumnValues>,
        measures: Option<&OnceLock<Arc<dyn MeasuresProvider>>>,
        time_source: (&Table, usize),
        average: bool,
    ) -> MsResult<RecordValue> {
        const DAY: f64 = 86400.0;
        let coordinate = |name, values| {
            RecordField::new(name, Value::Array(ArrayValue::Float64(vector(values))))
        };
        let times = self.integration_times(average);
        let mut axis = RecordValue::new(vec![coordinate("MJDseconds", times.clone())]);
        let midnight_source = if self.legacy {
            self.times.first()
        } else {
            times.get(1).or_else(|| times.first())
        };
        if names.contains("ut")
            && let Some(time) = midnight_source
        {
            let midnight = (time / DAY).trunc() * DAY;
            axis.push(coordinate(
                "UT",
                times.iter().map(|time| time - midnight).collect(),
            ));
        }
        let want_ha = names.contains("ha");
        let want_last = names.contains("last");
        if !want_ha && !want_last {
            return Ok(axis);
        }
        // MSDerivedValues::setAntennas selects antenna zero as its measure frame.
        let position = antenna_position(ms.antenna()?.table(), 0)?;
        let provider = match measures.and_then(OnceLock::get) {
            Some(provider) => Arc::clone(provider),
            None => {
                let provider = casa_ms::open_measures_runtime()?;
                if let Some(cache) = measures {
                    Arc::clone(cache.get_or_init(|| provider))
                } else {
                    provider
                }
            }
        };
        let (main, first_selected) = time_source;
        let reference = TimeColumn::read(main)?.reference_at(main, first_selected)?;
        let hour_angle_fields = if want_ha {
            let RequiredScalarColumnValues::Int32(ids) = &columns["FIELD_ID"] else {
                return Err(input("FIELD_ID must contain an integer"));
            };
            let mut ids = self
                .first_rows
                .iter()
                .map(|&row| ids[row])
                .collect::<Vec<_>>();
            if average {
                if self.legacy {
                    ids.truncate(1);
                } else {
                    averaging::common_id(&mut ids);
                }
            }
            Some((ms.field()?, ids))
        } else {
            None
        };
        let mut ha = Vec::with_capacity(times.len());
        let mut last = Vec::with_capacity(times.len());
        for (index, &time) in times.iter().enumerate() {
            let epoch = MEpoch::from_mjd(time / DAY, reference);
            let frame = MeasFrame::new()
                .with_measures(provider.clone())
                .with_position(position.clone())
                .with_epoch(epoch.clone());
            if let Some((fields, ids)) = &hour_angle_fields {
                let field = usize::try_from(ids[index]).map_err(|_| input("negative FIELD_ID"))?;
                let field_time = if fields.num_poly(field)? == 0 {
                    0.0
                } else {
                    time
                };
                let direction = fields.metadata_direction_at_seconds(
                    field,
                    FieldDirectionColumn::Phase,
                    field_time,
                )?;
                let lon = direction_at_frame(&direction, &frame)?
                    .convert_to(DirectionRef::HADEC, &frame)?
                    .longitude_rad();
                let signed = if lon > PI { lon - TAU } else { lon };
                ha.push(signed * (DAY / TAU));
            }
            if want_last {
                last.push(epoch.convert_to(EpochRef::LAST, &frame)?.value().as_mjd());
            }
        }
        if want_ha {
            axis.push(coordinate("HA", ha));
        }
        if want_last {
            axis.push(coordinate("LAST", last));
        }
        Ok(axis)
    }

    fn integration_times(&self, average: bool) -> Vec<f64> {
        if average && !self.times.is_empty() {
            vec![self.times.iter().sum::<f64>() / self.times.len() as f64]
        } else {
            self.times.clone()
        }
    }

    pub(super) fn remap(
        &self,
        fields: &mut BTreeMap<String, Value>,
        average: bool,
        reduced_flags: bool,
    ) -> MsResult<()> {
        let ids = |mut values| {
            if average {
                averaging::common_id(&mut values);
            }
            vector(values)
        };
        for (name, value) in fields {
            match (name.as_str(), value) {
                ("flag_sum", _) => {}
                ("flag", _) if reduced_flags => {}
                ("ifr_number", Value::Array(ArrayValue::Int32(values))) => {
                    if average && !self.legacy {
                        *values = ids(values.iter().copied().collect());
                    }
                }
                ("time", Value::Array(value)) => {
                    *value = ArrayValue::Float64(vector(self.integration_times(average)));
                }
                (
                    "array_id" | "data_desc_id" | "feed1" | "feed2" | "field_id" | "scan_number",
                    Value::Array(ArrayValue::Int32(values)),
                ) => {
                    *values = ids(self.first_rows.iter().map(|&row| values[row]).collect());
                }
                ("antenna1" | "antenna2", Value::Array(ArrayValue::Int32(values))) => {
                    if self.legacy {
                        let mut antennas = filled(self.baseline_count, -1)?;
                        for ((first, second), slot) in self.axis_entries() {
                            antennas[slot] = if name == "antenna1" { first } else { second };
                        }
                        *values = vector(antennas);
                    } else {
                        *values = self.scatter(values, true, 0)?;
                        if average {
                            *values = ids(values.iter().copied().collect());
                        }
                    }
                }
                ("sigma" | "weight", _) if average && (name == "sigma" || self.legacy) => {}
                (name, _) if average && visibility_product(name) => {}
                (_, Value::Array(array)) if !array.is_empty() => {
                    self.scatter_array(array)?;
                }
                _ => {}
            }
        }
        Ok(())
    }

    fn scatter_array(&self, array: &mut ArrayValue) -> MsResult<()> {
        match array {
            ArrayValue::Bool(values) => *values = self.scatter(values, false, self.legacy)?,
            ArrayValue::Float32(values) => *values = self.scatter(values, false, 0.0)?,
            ArrayValue::Float64(values) => *values = self.scatter(values, false, 0.0)?,
            ArrayValue::Complex32(values) => {
                *values = self.scatter(values, false, Default::default())?
            }
            ArrayValue::Complex64(values) => {
                *values = self.scatter(values, false, Default::default())?
            }
            _ => return Err(input("unexpected type for baseline-axis array")),
        }
        Ok(())
    }

    fn scatter<T: Copy>(
        &self,
        array: &ArrayD<T>,
        first_time: bool,
        fill: T,
    ) -> MsResult<ArrayD<T>> {
        let Some((&rows, cell_shape)) = array.shape().split_last() else {
            return Err(input("visibility array has no row axis"));
        };
        if rows != self.row_count {
            return Err(input(
                "visibility array row count does not match baseline grid",
            ));
        }
        let cell_width = element_count(cell_shape)?;
        let mut shape = cell_shape.to_vec();
        shape.push(self.baseline_count);
        if !first_time && (self.legacy || self.times.len() > 1) {
            shape.push(self.times.len());
        }
        let memory = array
            .as_slice_memory_order()
            .ok_or_else(|| input("visibility block is not contiguous"))?;
        let mut output = filled(element_count(&shape)?, fill)?;
        for &(slot, row) in &self.occupied {
            if first_time && slot >= self.baseline_count {
                break;
            }
            let start = row * cell_width;
            let dest = slot * cell_width;
            output[dest..dest + cell_width].copy_from_slice(&memory[start..start + cell_width]);
        }
        shaped(&shape, output)
    }

    pub(super) fn flag_sums(&self, flags: &ArrayD<bool>) -> MsResult<ArrayD<i64>> {
        let shape = match flags.shape() {
            [correlations, channels, rows] if *rows == self.row_count => {
                [*correlations, *channels, self.baseline_count]
            }
            _ if self.row_count == 0 => [0, 0, 0],
            _ => return Err(input("FLAG dimensions do not match baseline grid")),
        };
        let width = shape[0] * shape[1];
        let memory = flags
            .as_slice_memory_order()
            .ok_or_else(|| input("FLAG block is not contiguous"))?;
        let count_base = if self.legacy {
            self.time_count() as i64
        } else {
            0
        };
        let mut output = filled(element_count(&shape)?, count_base)?;
        for &(slot, row) in &self.occupied {
            let dest = slot % self.baseline_count * width;
            let source = row * width;
            for (count, &flag) in output[dest..dest + width]
                .iter_mut()
                .zip(&memory[source..source + width])
            {
                *count += i64::from(flag) - i64::from(self.legacy);
            }
        }
        shaped(&shape, output)
    }
}

pub(super) fn element_count(shape: &[usize]) -> MsResult<usize> {
    shape.iter().try_fold(1_usize, |length, &axis| {
        length
            .checked_mul(axis)
            .ok_or_else(|| input("visibility shape is too large"))
    })
}

fn direction_at_frame(value: &MetadataDirection, frame: &MeasFrame) -> MsResult<MDirection> {
    let Some(offset) = &value.offset else {
        return Ok(value.direction.clone());
    };
    let reference = value.direction.refer();
    let offset = direction_at_frame(offset, frame)?.convert_to(reference, frame)?;
    let base = value.direction.cosines();
    let offset = offset.cosines();
    Ok(MDirection::from_cosines(
        std::array::from_fn(|axis| base[axis] + offset[axis]),
        reference,
    ))
}

pub(super) fn filled<T: Clone>(length: usize, value: T) -> MsResult<Vec<T>> {
    let mut result = Vec::new();
    result
        .try_reserve_exact(length)
        .map_err(|_| input("cannot allocate baseline-axis array"))?;
    result.resize(length, value);
    Ok(result)
}

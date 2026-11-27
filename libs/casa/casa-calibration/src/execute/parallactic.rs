// SPDX-License-Identifier: LGPL-3.0-or-later

use super::*;

pub(crate) struct ParallacticAngleState {
    pub(super) engine: MsCalEngine,
    pub(super) feed_rows: HashMap<(i32, i32), Vec<FeedAngleRow>>,
}

#[derive(Debug, Clone, Copy)]
pub(super) struct FeedAngleRow {
    pub(super) spectral_window_id: i32,
    pub(super) time_seconds: f64,
    pub(super) interval_seconds: f64,
    pub(super) receptor0_angle_rad: f64,
}

pub(super) fn apply_parallactic_step(
    row: &ApplyRowPlan,
    state: &ParallacticAngleState,
    output: &mut ApplyBuffer<'_>,
    forward: bool,
    profile: Option<&mut ApplyRowComputeProfile>,
) -> Result<(), ApplyExecutionError> {
    let started = profile.as_ref().map(|_| Instant::now());
    let angle = |antenna, feed| {
        state
            .feed_angle_components(
                row.time_seconds,
                row.field_id,
                antenna,
                feed,
                row.data_spw_id,
            )
            // The visibility iterator adds the mount and receptor angles as CASA Floats.
            .map(|(mount, receptor)| f64::from(mount as f32 + receptor as f32))
    };
    let apply = if forward {
        parang::corrupt
    } else {
        parang::apply
    };
    apply(
        row.data_desc_id,
        output.correlation_types,
        angle(row.antenna1, row.feed1)?,
        angle(row.antenna2, row.feed2)?,
        output.corrected,
    )?;
    if output.propagate_flags {
        *output.newly_flagged_samples +=
            parang::propagate_flags(output.correlation_types, output.flags);
    }
    if let Some(profile) = profile {
        ApplyRowComputeProfile::add_elapsed(&mut profile.parallactic_angle_ns, started);
    }
    Ok(())
}

impl ParallacticAngleState {
    pub(crate) fn feed_parallactic_angle(
        &self,
        time_seconds: f64,
        field_id: i32,
        antenna_id: i32,
        feed_id: i32,
        data_spw_id: i32,
    ) -> Result<f64, ApplyExecutionError> {
        self.feed_angle_components(time_seconds, field_id, antenna_id, feed_id, data_spw_id)
            .map(|(mount, receptor)| mount + receptor)
    }

    fn feed_angle_components(
        &self,
        time_seconds: f64,
        field_id: i32,
        antenna_id: i32,
        feed_id: i32,
        data_spw_id: i32,
    ) -> Result<(f64, f64), ApplyExecutionError> {
        let receptor0_angle = self
            .lookup_receptor0_angle(antenna_id, feed_id, data_spw_id, time_seconds)
            .unwrap_or(0.0);
        self.engine
            .feed_parallactic_angle(
                time_seconds,
                usize::try_from(field_id).unwrap_or(usize::MAX),
                usize::try_from(antenna_id).unwrap_or(usize::MAX),
            )
            .map(|mount_angle| (mount_angle, receptor0_angle))
            .map_err(|source| ApplyExecutionError::MutateMeasurementSet {
                path: "<measurement-set derived parallactic angle>".to_string(),
                source,
            })
    }

    pub(super) fn lookup_receptor0_angle(
        &self,
        antenna_id: i32,
        feed_id: i32,
        data_spw_id: i32,
        time_seconds: f64,
    ) -> Option<f64> {
        let rows = self.feed_rows.get(&(antenna_id, feed_id))?;
        let mut exact_spw = rows
            .iter()
            .copied()
            .filter(|row| row.spectral_window_id == data_spw_id)
            .collect::<Vec<_>>();
        if exact_spw.is_empty() {
            exact_spw = rows
                .iter()
                .copied()
                .filter(|row| row.spectral_window_id < 0)
                .collect::<Vec<_>>();
        }
        exact_spw
            .into_iter()
            .min_by(|left, right| {
                feed_row_distance(left, time_seconds)
                    .total_cmp(&feed_row_distance(right, time_seconds))
            })
            .map(|row| row.receptor0_angle_rad)
    }
}

pub(super) fn interval_distance(epoch: f64, interval: f64, time_seconds: f64) -> f64 {
    if interval > 0.0 {
        let half = interval / 2.0;
        let start = epoch - half;
        let end = epoch + half;
        if (start..=end).contains(&time_seconds) {
            return 0.0;
        }
    }
    (epoch - time_seconds).abs()
}

pub(super) fn feed_row_distance(row: &FeedAngleRow, time_seconds: f64) -> f64 {
    interval_distance(row.time_seconds, row.interval_seconds, time_seconds)
}

pub(super) fn receptor0_angle(value: &ArrayValue) -> f64 {
    match value {
        ArrayValue::Float64(values) if !values.is_empty() => values[[0]],
        ArrayValue::Float32(values) if !values.is_empty() => f64::from(values[[0]]),
        _ => 0.0,
    }
}

/// Find one feed angle without retaining a table-wide index. Exact spectral
/// windows precede wildcard rows, then the nearest interval wins.
pub(crate) fn single_receptor_angle(
    ms: &MeasurementSet,
    antenna: i32,
    feed_id: i32,
    spw: i32,
    time: f64,
) -> MsResult<f64> {
    let Ok(feed) = ms.feed() else {
        return Ok(0.0);
    };
    let mut best = None;
    for row in 0..feed.row_count() {
        if feed.i32(row, "ANTENNA_ID")? != antenna || feed.i32(row, "FEED_ID")? != feed_id {
            continue;
        }
        let window = feed.i32(row, "SPECTRAL_WINDOW_ID")?;
        if window != spw && window >= 0 {
            continue;
        }
        let wildcard = window != spw;
        let distance = interval_distance(feed.f64(row, "TIME")?, feed.f64(row, "INTERVAL")?, time);
        if best.is_none_or(|(old_wildcard, old_distance, _)| {
            wildcard
                .cmp(&old_wildcard)
                .then_with(|| distance.total_cmp(&old_distance))
                .is_lt()
        }) {
            best = Some((wildcard, distance, row));
        }
    }
    best.map_or(Ok(0.0), |(_, _, row)| {
        feed.array(row, "RECEPTOR_ANGLE").map(receptor0_angle)
    })
}

pub(crate) fn load_parallactic_angle_state(ms: &MeasurementSet) -> MsResult<ParallacticAngleState> {
    let engine = MsCalEngine::for_calibration(ms)?;
    let mut feed_rows = HashMap::<(i32, i32), Vec<FeedAngleRow>>::new();
    if let Ok(feed) = ms.feed() {
        for row_index in 0..feed.row_count() {
            let antenna_id = feed.i32(row_index, "ANTENNA_ID")?;
            let feed_id = feed.i32(row_index, "FEED_ID")?;
            let spectral_window_id = feed.i32(row_index, "SPECTRAL_WINDOW_ID")?;
            let time_seconds = feed.f64(row_index, "TIME")?;
            let interval_seconds = feed.f64(row_index, "INTERVAL")?;
            let receptor0_angle_rad = receptor0_angle(feed.array(row_index, "RECEPTOR_ANGLE")?);
            feed_rows
                .entry((antenna_id, feed_id))
                .or_default()
                .push(FeedAngleRow {
                    spectral_window_id,
                    time_seconds,
                    interval_seconds,
                    receptor0_angle_rad,
                });
        }
    }
    Ok(ParallacticAngleState { engine, feed_rows })
}

pub(super) fn parallactic_angle_gain(
    correlation_type: i32,
    data_desc_id: i32,
    all_correlation_types: &[i32],
    ant1_feed_pa: f64,
    ant2_feed_pa: f64,
) -> Result<Complex32, ApplyExecutionError> {
    match correlation_type {
        5 => Ok(circular_parang_gain(-ant1_feed_pa + ant2_feed_pa)),
        6 => Ok(circular_parang_gain(-ant1_feed_pa - ant2_feed_pa)),
        7 => Ok(circular_parang_gain(ant1_feed_pa + ant2_feed_pa)),
        8 => Ok(circular_parang_gain(ant1_feed_pa - ant2_feed_pa)),
        9..=12 => Err(ApplyExecutionError::UnsupportedParallacticAngleBasis {
            data_desc_id,
            correlation_types: all_correlation_types
                .iter()
                .map(|code| stokes_name(*code).to_string())
                .collect(),
        }),
        _ => Err(ApplyExecutionError::UnsupportedCorrelationLayout {
            data_desc_id,
            correlation_types: all_correlation_types
                .iter()
                .map(|code| stokes_name(*code).to_string())
                .collect(),
        }),
    }
}

pub(super) fn circular_parang_gain(angle_rad: f64) -> Complex32 {
    Complex32::new(angle_rad.cos() as f32, angle_rad.sin() as f32)
}

pub(super) fn load_correlation_types_by_ddid(
    ms: &MeasurementSet,
) -> MsResult<HashMap<i32, Vec<i32>>> {
    let dd = ms.data_description()?;
    let pol = ms.polarization()?;
    let mut out = HashMap::new();
    for row in 0..dd.row_count() {
        let pol_id = dd.polarization_id(row)?;
        out.insert(
            pol_id_of_ddid(row),
            pol.corr_type(usize::try_from(pol_id).unwrap_or(usize::MAX))?,
        );
    }
    Ok(out)
}

pub(super) fn pol_id_of_ddid(ddid_row: usize) -> i32 {
    ddid_row as i32
}

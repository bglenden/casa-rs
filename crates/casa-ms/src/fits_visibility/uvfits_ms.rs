// SPDX-License-Identifier: LGPL-3.0-or-later
//! Populate standard MeasurementSet subtables from UVFITS metadata.

use casa_core::measures::direction::{DirectionRef, MDirection};
use casa_core::measures::frequency::FrequencyRef;
use casa_core::measures::{MPosition, PositionRef};
use casa_core::quanta::{Quantity, Unit};
use casa_core::{Complex32, Correlation, PrimitiveType, Value};
use casa_measures::table_measures::{MeasureType, TableMeasDesc};
use casa_tables::{ColumnSchema, Table};

use super::groups::GroupLayout;
use super::invalid;
use super::metadata::{Array, Extensions, Header, Source, SpectralWindow};
use super::uvfits_import::{Usage, UvFitsImportOptions};
use crate::schema::{self, SubtableId};
use crate::subtable_rows::{
    b, complex_array, f, f64_array, i, i32_array, row_from_defs, s, string_array, subtable_mut,
};
use crate::{MeasurementSet, MsResult};

pub(super) fn write_metadata(
    ms: &mut MeasurementSet,
    header: &Header,
    layout: &GroupLayout,
    extensions: &Extensions,
    usage: &Usage,
    options: UvFitsImportOptions,
) -> MsResult<()> {
    let time = fits_date(&header.date)?;
    let frame = direction_frame(header.epoch, &header.telescope);
    let epoch = match extensions.array.time_system.as_str() {
        "TAI" | "IAT" => "TAI",
        _ => "UTC",
    };
    configure_references(ms, frame, epoch)?;
    write_observation(ms, header, time)?;
    write_antennas(
        ms,
        &extensions.array,
        &header.telescope,
        options.new_antenna_names,
    )?;
    write_polarization(ms, &layout.corr)?;
    write_feeds(ms, layout, &extensions.array, time)?;
    write_spectral_windows(ms, header, layout, extensions, usage.windows)?;
    write_sources(ms, header, layout, extensions, usage, time)?;
    Ok(())
}

fn insert(ms: &mut MeasurementSet, id: SubtableId, fields: &[(&str, Value)]) -> MsResult<()> {
    subtable_mut(ms, id)?.add_row(row_from_defs(schema::required_columns(id), fields))?;
    Ok(())
}

fn fits_date(value: &str) -> MsResult<f64> {
    let value = value.trim();
    if value.is_empty() {
        return Ok(0.0);
    }
    let formatted = if value.as_bytes().get(4) == Some(&b'-') {
        value
            .replace(['-', 'T'], "/")
            .trim_end_matches('Z')
            .to_owned()
    } else if let [day, month, year] = value.split('/').collect::<Vec<_>>().as_slice()
        && year.len() == 2
        && let Ok(year) = year.parse::<i32>()
    {
        format!("{}/{month}/{day}", 1900 + year)
    } else {
        value.to_owned()
    };
    let days = Unit::new("d").map_err(invalid)?;
    Ok(Quantity::read(&formatted)
        .map_err(invalid)?
        .get_value_in(&days)
        .map_err(invalid)?
        * 86_400.0)
}

fn direction_frame(epoch: f64, telescope: &str) -> DirectionRef {
    if (epoch - 1950.0).abs() < 0.01 {
        if telescope == "VLA" {
            DirectionRef::B1950Vla
        } else {
            DirectionRef::B1950
        }
    } else {
        DirectionRef::J2000
    }
}

fn configure_references(ms: &mut MeasurementSet, frame: DirectionRef, epoch: &str) -> MsResult<()> {
    for column in ["TIME", "TIME_CENTROID"] {
        TableMeasDesc::new_fixed(column, MeasureType::Epoch, epoch).write(ms.main_table_mut())?;
    }
    // The native schema installs MEASINFO for UVW. Keep its type and units.
    if let Some(mut keywords) = ms.main_table().column_keywords("UVW").cloned()
        && let Some(Value::Record(mut record)) = keywords.get("MEASINFO").cloned()
    {
        record.upsert("Ref", s(&frame.to_string()));
        keywords.upsert("MEASINFO", Value::Record(record));
        ms.main_table_mut().set_column_keywords("UVW", keywords);
    }
    for id in [
        SubtableId::Field,
        SubtableId::Pointing,
        SubtableId::Source,
        SubtableId::Observation,
        SubtableId::History,
        SubtableId::Feed,
        SubtableId::FlagCmd,
    ] {
        for definition in schema::required_columns(id) {
            let refer = match definition.measure_type {
                Some(MeasureType::Epoch) => epoch.to_owned(),
                Some(MeasureType::Direction) => frame.to_string(),
                _ => continue,
            };
            TableMeasDesc::new_fixed(definition.name, definition.measure_type.unwrap(), &refer)
                .write(subtable_mut(ms, id)?)?;
        }
    }
    Ok(())
}

fn write_observation(ms: &mut MeasurementSet, header: &Header, time: f64) -> MsResult<()> {
    let telescope = if header.telescope == "HATCREEK" {
        "BIMA"
    } else {
        &header.telescope
    };
    let mut observation = row_from_defs(
        schema::observation::REQUIRED_COLUMNS,
        &[
            ("TIME_RANGE", f64_array(&[time, time], vec![2])),
            ("RELEASE_DATE", f(fits_date(&header.release_date)?)),
            ("OBSERVER", s(&header.observer)),
            ("TELESCOPE_NAME", s(telescope)),
        ],
    );
    observation.remove("LOG");
    observation.remove("SCHEDULE");
    subtable_mut(ms, SubtableId::Observation)?.add_row(observation)?;
    for entry in header.history.iter().take_while(|entry| !entry.is_empty()) {
        insert(
            ms,
            SubtableId::History,
            &[
                ("OBSERVATION_ID", i(0)),
                ("TIME", f(time)),
                ("PRIORITY", s("NORMAL")),
                ("ORIGIN", s("MSFitsInput::fillObsTables")),
                ("APPLICATION", s("ms")),
                ("CLI_COMMAND", string_array(&[""], vec![1])),
                ("APP_PARAMS", string_array(&[""], vec![1])),
                ("MESSAGE", s(entry)),
            ],
        )?;
    }
    Ok(())
}

fn write_antennas(
    ms: &mut MeasurementSet,
    array: &Array,
    telescope: &str,
    new_names: bool,
) -> MsResult<()> {
    let (origin, rotate) = observatory_origin(array)?;
    let (sin, cos) = origin[1].atan2(origin[0]).sin_cos();
    let count = array.antennas.last_key_value().map_or(0, |(&id, _)| id + 1);
    for id in 0..count {
        let Some(antenna) = array.antennas.get(&id) else {
            insert(ms, SubtableId::Antenna, &[("FLAG_ROW", b(true))])?;
            continue;
        };
        let name = match (new_names, telescope) {
            (true, "CARMA") => format!("CA{}", antenna.number),
            (true, "EVLA") => format!("EA{:02}", antenna.number),
            (true, "VLA") => format!("VA{:02}", antenna.number),
            _ => antenna.number.to_string(),
        };
        let mounts = [
            "ALT-AZ",
            "EQUATORIAL",
            "ORBITING",
            "X-Y",
            "ALT-AZ+NASMYTH-R",
            "ALT-AZ+NASMYTH-L",
            "ALT-AZ+BWG-R",
            "ALT-AZ+BWG-L",
            "BIZARRE",
        ];
        let mount = mounts
            .get(antenna.mount as usize)
            .copied()
            .unwrap_or("SPACE_HALCA");
        let [mut x, mut y, z] = antenna.position;
        if rotate {
            (x, y) = (cos * x - sin * y, sin * x + cos * y);
        }
        if array.left_handed {
            y = -y;
        }
        let position = [origin[0] + x, origin[1] + y, origin[2] + z];
        insert(
            ms,
            SubtableId::Antenna,
            &[
                ("NAME", s(&name)),
                ("STATION", s(&antenna.station)),
                ("MOUNT", s(mount)),
                ("TYPE", s("GROUND-BASED")),
                (
                    "OFFSET",
                    f64_array(&[antenna.axis_offset, 0.0, 0.0], vec![3]),
                ),
                ("POSITION", f64_array(&position, vec![3])),
                ("DISH_DIAMETER", f(antenna.diameter)),
            ],
        )?;
    }
    let table = subtable_mut(ms, SubtableId::Antenna)?;
    let keywords = table.keywords_mut();
    keywords.upsert("RDATE", f(fits_date(&array.date)?));
    keywords.upsert("GSTIA0", f(array.gst));
    keywords.upsert("DEGPDY", f(array.degrees_per_day));
    let time_system = if array.time_system == "IAT" {
        "TAI"
    } else {
        &array.time_system
    };
    keywords.upsert("TIMSYS", s(time_system));
    column_keyword(table, "NAME", "ARRAY_NAME", s(&array.name));
    column_keyword(
        table,
        "POSITION",
        "ARRAY_POSITION",
        f64_array(&origin, vec![3]),
    );
    Ok(())
}

fn observatory_origin(array: &Array) -> MsResult<([f64; 3], bool)> {
    let norm = |vector: [f64; 3]| {
        vector
            .into_iter()
            .map(|value| value * value)
            .sum::<f64>()
            .sqrt()
    };
    if array.name != "VLA" || (array.frame_defined && norm(array.origin) <= 1.0e6) {
        return Ok((array.origin, false));
    }
    let provider = crate::open_measures_runtime()?;
    let position = MPosition::casa_observatory("VLA", provider.as_ref())
        .ok_or_else(|| invalid("Measures catalog has no VLA observatory"))?;
    let center = position.convert_to(PositionRef::ITRF)?.values();
    let difference = std::array::from_fn(|axis| array.origin[axis] - center[axis]);
    Ok(if norm(difference) <= 10_000.0 {
        (center, true)
    } else {
        (array.origin, false)
    })
}

fn column_keyword(table: &mut Table, column: &str, keyword: &str, value: Value) {
    let mut keywords = table.column_keywords(column).cloned().unwrap_or_default();
    keywords.upsert(keyword, value);
    table.set_column_keywords(column, keywords);
}

fn write_polarization(ms: &mut MeasurementSet, corr: &[Correlation]) -> MsResult<()> {
    let codes = corr
        .iter()
        .map(|&correlation| correlation.casacore_code())
        .collect::<Vec<_>>();
    let mut receptors = Vec::with_capacity(codes.len() * 2);
    for second in [false, true] {
        receptors.extend(corr.iter().map(|correlation| {
            i32::from(match correlation {
                Correlation::LL | Correlation::YY => true,
                Correlation::RL | Correlation::XY => second,
                Correlation::LR | Correlation::YX => !second,
                _ => false,
            })
        }));
    }
    insert(
        ms,
        SubtableId::Polarization,
        &[
            ("NUM_CORR", i(codes.len() as i32)),
            ("CORR_TYPE", i32_array(&codes, vec![codes.len()])),
            ("CORR_PRODUCT", i32_array(&receptors, vec![2, codes.len()])),
        ],
    )
}

fn write_feeds(
    ms: &mut MeasurementSet,
    layout: &GroupLayout,
    array: &Array,
    time: f64,
) -> MsResult<()> {
    let endpoints =
        [layout.corr[0], layout.corr[layout.corr.len() - 1]].map(Correlation::casacore_code);
    let polarization = if endpoints[0] >= 5 && endpoints[1] <= 8 {
        ["R", "L"]
    } else if endpoints[0] >= 9 && endpoints[1] <= 12 {
        ["X", "Y"]
    } else {
        ["?", "?"]
    };
    let count = array.antennas.last_key_value().map_or(0, |(&id, _)| id + 1);
    let mut angles = vec![[0.0; 2]; count as usize];
    for (row, antenna) in array.antennas.values().enumerate() {
        angles[row] = antenna.angles;
    }
    let response = [
        Complex32::new(1.0, 0.0),
        Complex32::ZERO,
        Complex32::ZERO,
        Complex32::new(1.0, 0.0),
    ];
    for (id, angles) in angles.into_iter().enumerate() {
        insert(
            ms,
            SubtableId::Feed,
            &[
                ("ANTENNA_ID", i(id as i32)),
                ("BEAM_ID", i(-1)),
                ("SPECTRAL_WINDOW_ID", i(-1)),
                ("TIME", f(time)),
                ("NUM_RECEPTORS", i(2)),
                ("BEAM_OFFSET", f64_array(&[0.0; 4], vec![2, 2])),
                ("POLARIZATION_TYPE", string_array(&polarization, vec![2])),
                ("POL_RESPONSE", complex_array(&response, vec![2, 2])),
                ("RECEPTOR_ANGLE", f64_array(&angles, vec![2])),
            ],
        )?;
    }
    Ok(())
}

fn spectral_reference(header: &Header) -> FrequencyRef {
    if let Some(reference) = header.frame.as_deref() {
        return reference.trim().parse().unwrap_or(FrequencyRef::TOPO);
    }
    match header.velocity_reference.unwrap_or(3).rem_euclid(256) {
        1 => FrequencyRef::LSRK,
        2 => FrequencyRef::BARY,
        4 => FrequencyRef::LSRD,
        5 => FrequencyRef::GEO,
        6 => FrequencyRef::REST,
        7 => FrequencyRef::GALACTO,
        _ => FrequencyRef::TOPO,
    }
}

fn write_spectral_windows(
    ms: &mut MeasurementSet,
    header: &Header,
    layout: &GroupLayout,
    extensions: &Extensions,
    windows: usize,
) -> MsResult<()> {
    let reference = spectral_reference(header);
    let axis = &layout.frequency;
    for id in 0..windows {
        let mut window = extensions.spectral.as_ref().map_or(
            SpectralWindow {
                frequency: axis.reference,
                increment: axis.increment,
                bandwidth: (axis.increment * axis.length as f64).abs(),
            },
            |windows| windows[id],
        );
        let mut pixel = axis.pixel;
        let mut channel_width = window.increment;
        if let Some(velocity) = header.alternate_value
            && header.frame.is_none()
            && reference != FrequencyRef::TOPO
            && header.rest_frequency > 0.0
        {
            let beta = velocity / 299_792_458.0;
            window.frequency = header.rest_frequency
                * if header.velocity_reference.unwrap_or(0) > 256 {
                    1.0 - beta
                } else {
                    (1.0 + beta).recip()
                };
            pixel = header.alternate_pixel.unwrap_or(pixel);
            channel_width = axis.increment;
        }
        let frequency = (0..axis.length)
            .map(|channel| window.frequency + (channel as f64 + 1.0 - pixel) * channel_width)
            .collect::<Vec<_>>();
        let width = if extensions.spectral.is_some() {
            window.increment.abs()
        } else {
            window.increment
        };
        let width = f64_array(&vec![width; axis.length], vec![axis.length]);
        let band = i32::from(window.increment > 0.0) * 2 - 1;
        insert(
            ms,
            SubtableId::SpectralWindow,
            &[
                ("NAME", s("none")),
                ("FREQ_GROUP_NAME", s("none")),
                ("IF_CONV_CHAIN", i((id % layout.windows) as i32)),
                ("NUM_CHAN", i(axis.length as i32)),
                ("REF_FREQUENCY", f(window.frequency)),
                ("CHAN_FREQ", f64_array(&frequency, vec![axis.length])),
                ("CHAN_WIDTH", width.clone()),
                ("EFFECTIVE_BW", width.clone()),
                ("RESOLUTION", width),
                ("TOTAL_BANDWIDTH", f(window.bandwidth)),
                ("NET_SIDEBAND", i(band)),
                ("FREQ_GROUP", i((id / layout.windows) as i32)),
                ("MEAS_FREQ_REF", i(reference.casacore_code())),
            ],
        )?;
        insert(
            ms,
            SubtableId::DataDescription,
            &[("SPECTRAL_WINDOW_ID", i(id as i32))],
        )?;
    }
    Ok(())
}

fn write_sources(
    ms: &mut MeasurementSet,
    header: &Header,
    layout: &GroupLayout,
    extensions: &Extensions,
    usage: &Usage,
    time: f64,
) -> MsResult<()> {
    let defaults = Source {
        name: header.object.clone(),
        code: " ".into(),
        epoch: header.epoch,
        direction_deg: layout.direction_deg,
        apparent_deg: layout.direction_deg,
        motion_per_day: [0.0; 2],
        rest_frequency: 0.0,
        velocity: 0.0,
    };
    let fields = extensions.sources.as_ref().map_or(1, |sources| {
        sources.last_key_value().map_or(0, |(&id, _)| id + 1)
    });
    let source = |id| {
        extensions
            .sources
            .as_ref()
            .map_or(Some(&defaults), |sources| sources.get(&id))
    };
    for id in 0..fields {
        let (name, code, direction, count, flag) = if let Some(source) = source(id) {
            let (direction, order) = source_direction(source)?;
            (
                source.name.as_str(),
                source.code.as_str(),
                direction,
                order,
                false,
            )
        } else {
            ("", "", f64_array(&[0.0; 2], vec![2, 1]), 0, true)
        };
        let present = usage.occurrences.iter().any(|&(used, _, _)| used == id);
        insert(
            ms,
            SubtableId::Field,
            &[
                ("NAME", s(name)),
                ("CODE", s(code)),
                ("TIME", f(time)),
                ("NUM_POLY", i(count)),
                ("SOURCE_ID", i(if present { id } else { -1 })),
                ("FLAG_ROW", b(flag)),
                ("PHASE_DIR", direction.clone()),
                ("DELAY_DIR", direction.clone()),
                ("REFERENCE_DIR", direction),
            ],
        )?;
    }
    let names = ["REST_FREQUENCY", "SYSVEL", "TRANSITION"];
    let defs = schema::source::OPTIONAL_COLUMNS
        .iter()
        .filter(|definition| names.contains(&definition.name))
        .copied()
        .collect::<Vec<_>>();
    let schema = crate::column_def::build_table_schema(&defs)?;
    let table = subtable_mut(ms, SubtableId::Source)?;
    for column in schema.columns() {
        table.add_column(column.clone(), None)?;
    }
    table.add_column(
        ColumnSchema::array_variable("POSITION", PrimitiveType::Float64, Some(1)),
        None,
    )?;
    TableMeasDesc::new_fixed("POSITION", MeasureType::Position, "ITRF").write(table)?;
    table.add_column(ColumnSchema::record("SOURCE_MODEL"), None)?;
    for &(id, window, start) in &usage.occurrences {
        let source = source(id).ok_or_else(|| invalid("missing observed UVFITS source"))?;
        let has_motion = source.motion_per_day.iter().any(|rate| rate.abs() > 0.0);
        let angle = canonical_direction(if has_motion {
            source.apparent_deg
        } else {
            source.direction_deg
        });
        let motion = source
            .motion_per_day
            .map(|value| value.to_radians() / 86_400.0);
        let mut row = row_from_defs(
            schema::source::REQUIRED_COLUMNS,
            &[
                ("SOURCE_ID", i(id)),
                ("SPECTRAL_WINDOW_ID", i(window)),
                ("TIME", f(start)),
                ("INTERVAL", f(f64::MAX)),
                ("NAME", s(&source.name)),
                ("CALIBRATION_GROUP", i(-1)),
                ("NUM_LINES", i(1)),
                ("DIRECTION", f64_array(&angle, vec![2])),
                ("PROPER_MOTION", f64_array(&motion, vec![2])),
            ],
        );
        row.upsert(
            "REST_FREQUENCY",
            f64_array(&[source.rest_frequency], vec![1]),
        );
        row.upsert("SYSVEL", f64_array(&[source.velocity], vec![1]));
        row.upsert("TRANSITION", string_array(&[""], vec![1]));
        table.add_row(row)?;
    }
    Ok(())
}

fn source_direction(source: &Source) -> MsResult<(Value, i32)> {
    let motion = source
        .motion_per_day
        .map(|value| value.to_radians() / 86_400.0);
    if motion.iter().all(|&value| value == 0.0) {
        if (source.epoch - 2000.0).abs() > 20.0
            && (source.epoch - 1950.0).abs() > 0.01
            && source.epoch != -1.0
        {
            return Err(invalid(format!(
                "Cannot handle epoch in UVFITS source table: {}",
                source.epoch
            )));
        }
        Ok((
            f64_array(&canonical_direction(source.direction_deg), vec![2, 1]),
            0,
        ))
    } else {
        let position = canonical_direction(source.apparent_deg);
        Ok((
            f64_array(
                &[position[0], motion[0], position[1], motion[1]],
                vec![2, 2],
            ),
            1,
        ))
    }
}

fn canonical_direction(angles_deg: [f64; 2]) -> [f64; 2] {
    let [lon, lat] = angles_deg.map(f64::to_radians);
    let (longitude, latitude) = MDirection::from_angles(lon, lat, DirectionRef::J2000).as_angles();
    [longitude.sin().atan2(longitude.cos()), latitude]
}

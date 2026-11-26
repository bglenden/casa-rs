// SPDX-License-Identifier: LGPL-3.0-or-later
//! The task-only 2012 standard, including finite bandwidth and useephemdir.

use std::collections::BTreeMap;
use std::path::Path;

use casa_components::ComponentList;
use casa_contracts::{CasaFloat, CasaValue, FloatDtype};
use casa_ms::derived::engine::MsCalEngine;
use casa_ms::schema::main_table::VisibilityDataColumn;
use casa_ms::virtual_model::selected_model_combinations;
use casa_ms::{
    MeasurementSet, MsSelection, ResolvedMsSelection, VirtualModelEntry, VirtualModelKind,
    parse_numeric_row_id_ranges,
};
use casa_task_runtime::{TaskFailure, TaskInvocation};
use casa_types::ScalarValue;
use casa_types::measures::direction::MDirection;
use casa_types::measures::epoch::{EpochRef, MEpoch};
use casa_types::measures::frequency::MFrequency;
use casa_types::measures::position::MPosition;

use super::{entry, execution, model_files, record_history, requested_windows};
use crate::attachment::OpenedMs;
use crate::ft::setjy::solar2012::{Solar2012, bands};
use crate::ft::write_model_scratch;
use crate::solve::grouping::VisibilityModels;

pub(super) fn run(
    task: &TaskInvocation<'_>,
    path: &Path,
    opened: OpenedMs,
    selection: &MsSelection,
    selected: &ResolvedMsSelection,
    partition: bool,
) -> Result<CasaValue, TaskFailure> {
    // The wrapper resolves a supplied model image before entering the Solar
    // System branch, which then uses its own components.
    model_files::task_image(task)?;
    let mut opened = opened
        .initialize(VisibilityDataColumn::ModelData)
        .map_err(execution)?;
    record_history(task, path, partition);
    let engine = MsCalEngine::for_calibration(&opened.measurement_set).map_err(execution)?;
    let by_channel = task.flag("scalebychan")?;
    let ephemeris_direction = task.flag("useephemdir")?;
    let requested = requested_windows(&opened.measurement_set, selection.spw.as_deref())?;
    let mut groups = BTreeMap::<i32, BTreeMap<i32, Vec<usize>>>::new();
    for row in &selected.selected_rows {
        groups
            .entry(row.field_id)
            .or_default()
            .entry(row.spectral_window_id)
            .or_default()
            .push(row.row_index);
    }
    let observation_id = selection
        .observation
        .as_deref()
        .map(|ids| parse_numeric_row_id_ranges(ids, "observation"))
        .transpose()
        .map_err(execution)?
        .and_then(|ranges| ranges.first().map(|&(first, _)| first))
        .unwrap_or(0);
    let mut fields = Vec::with_capacity(groups.len() + 1);
    for (field, windows) in groups {
        let prepared = model(
            &opened.measurement_set,
            &engine,
            field,
            observation_id,
            WindowSelection {
                requested: &requested,
                rows: &windows,
            },
            by_channel,
            ephemeris_direction,
        );
        let prepared = match prepared {
            Ok(model) => model,
            Err(error) => {
                tracing::warn!(field, message = %error.message, "no 2012 Solar System flux model");
                return Err(execution("No componentlist is generated."));
            }
        };
        let mut selection = selection.clone();
        selection.field = Some(field.to_string());
        let mut rows = opened.resolve(&selection).map_err(execution)?;
        rows.channel_selections.clear();
        let ms = &opened.measurement_set;
        let visibility = VisibilityModels::from_entries(ms, &rows.selected_rows, &prepared.models)
            .and_then(|models| models.with_calibration_field_directions(ms))
            .map_err(execution)?
            .with_phase_centre_time(prepared.time);
        write_model_scratch(
            &mut opened.measurement_set,
            &rows,
            &prepared.models,
            false,
            true,
            Some(visibility),
            &mut opened.lock,
        )
        .map_err(execution)?;
        super::super::history::record_setjy_flux(path, prepared.history);
        fields.push(entry(field.to_string(), prepared.returned));
    }
    opened.release().map_err(execution)?;
    fields.push(entry(
        "format",
        CasaValue::String {
            value: "{field Id: {spw Id: {fluxd:[I,Q,U,V] in Jy}, 'fieldName':field name}}".into(),
        },
    ));
    Ok(CasaValue::Dict { entries: fields })
}

struct WindowSelection<'a> {
    requested: &'a [i32],
    rows: &'a BTreeMap<i32, Vec<usize>>,
}

struct PreparedField {
    models: Vec<VirtualModelEntry>,
    returned: CasaValue,
    time: f64,
    history: Vec<String>,
}

fn model(
    ms: &MeasurementSet,
    engine: &MsCalEngine,
    field: i32,
    observation_id: i32,
    selection: WindowSelection<'_>,
    by_channel: bool,
    use_ephemeris_direction: bool,
) -> Result<PreparedField, TaskFailure> {
    let fields = ms.field().map_err(execution)?;
    let name = fields.name(field as usize).map_err(execution)?;
    let time = model_time(ms, field, selection.rows)?;
    let mut frame = engine
        .spectral_frame_observatory_direction(
            time,
            engine
                .observation_direction_j2000(time, field as usize)
                .map_err(execution)?,
        )
        .map_err(execution)?;
    let observatory = ms
        .observation()
        .map_err(execution)?
        .string(observation_id as usize, "TELESCOPE_NAME")
        .map_err(execution)?;
    let provider = frame
        .measures()
        .ok_or_else(|| execution("no available measures data"))?;
    let position = MPosition::from_observatory_name(&observatory, provider)
        .map_err(execution)?
        .ok_or_else(|| execution(format!("unknown Solar System observatory {observatory}")))?;
    let epoch = MEpoch::from_mjd_seconds(time, EpochRef::UTC);
    let solar = Solar2012::open(&name, epoch.value().as_mjd()).map_err(execution)?;
    let direction = if use_ephemeris_direction {
        // CASA passes each angle through quanta.tos before creating the
        // scratch component. The 1 nrad rounding is visible on long baselines.
        let (lon, lat) = solar.direction().as_angles();
        let rounded = |angle: f64| (angle * 1e9).round_ties_even() / 1e9;
        let signed_lon = lon.sin().atan2(lon.cos());
        MDirection::from_angles(rounded(signed_lon), rounded(lat), solar.direction().refer())
    } else {
        frame
            .direction()
            .expect("the field direction is set")
            .clone()
    };
    frame = frame
        .with_epoch(epoch)
        .with_position(position)
        .with_direction(solar.direction().clone());
    let windows = ms.spectral_window().map_err(execution)?;
    let mut models = Vec::with_capacity(selection.rows.len());
    let mut returned = Vec::with_capacity(selection.requested.len() + 1);
    let mut history = Vec::with_capacity(selection.requested.len());
    let mut last_supported = false;
    for &window in selection.requested {
        let centers = windows.chan_freq(window as usize).map_err(execution)?;
        let width = windows.chan_width(window as usize).map_err(execution)?;
        if centers.is_empty() || centers.len() != width.len() {
            return Err(execution("invalid channel centres or widths"));
        }
        let frame_ref = windows
            .frequency_reference(window as usize)
            .map_err(execution)?;
        let bands = bands(&centers, &width, by_channel);
        let frequencies: Vec<_> = if by_channel {
            centers
                .iter()
                .map(|&frequency| MFrequency::new(frequency, frame_ref))
                .collect()
        } else {
            let [low, high] = bands[0];
            vec![MFrequency::new(low + (high - low) * 0.5, frame_ref)]
        };
        let (model, flux, supported) = solar
            .component(direction.clone(), &frame, &frequencies, &bands, by_channel)
            .map_err(execution)?;
        last_supported = supported;
        if let Some(selected) = selection.rows.get(&window) {
            let container = ComponentList::from(vec![model])
                .to_record()
                .map_err(execution)?;
            models.push(VirtualModelEntry {
                kind: VirtualModelKind::ComponentList,
                index_combination: selected_model_combinations(ms, selected)
                    .map_err(execution)?
                    .index_combination,
                channel_selection: vec![[
                    window,
                    0,
                    i32::try_from(centers.len()).map_err(execution)?,
                    1,
                ]],
                container,
            });
        }
        let stokes = [CasaFloat(flux), CasaFloat(0.), CasaFloat(0.), CasaFloat(0.)];
        let window_label = if selection.requested.len() == 1 {
            window.to_string()
        } else {
            format!("spw{window}")
        };
        history.push(format!(
            " {name}: spw {window_label} Flux:[I={flux},Q=0.0,U=0.0,V=0.0] +/- [I=0.0,Q=0.0,U=0.0,V=0.0] Jy"
        ));
        let flux = if selection.requested.len() == 1 {
            CasaValue::List {
                items: stokes
                    .into_iter()
                    .map(|value| CasaValue::Float { value })
                    .collect(),
            }
        } else {
            CasaValue::FloatArray {
                dtype: FloatDtype::Float64,
                shape: vec![4],
                data: stokes.to_vec(),
            }
        };
        returned.push(entry(
            window.to_string(),
            CasaValue::Dict {
                entries: vec![entry("fluxd", flux)],
            },
        ));
    }
    if !last_supported {
        return Err(execution(
            "the last spectral window is outside the 2012 Solar System model",
        ));
    }
    returned.push(entry("fieldName", CasaValue::String { value: name }));
    Ok(PreparedField {
        models,
        returned: CasaValue::Dict { entries: returned },
        time,
        history,
    })
}

fn model_time(
    ms: &MeasurementSet,
    field: i32,
    rows: &BTreeMap<i32, Vec<usize>>,
) -> Result<f64, TaskFailure> {
    let fields = ms.field().map_err(execution)?;
    if fields
        .ephemeris_id(field as usize)
        .map_err(execution)?
        .is_some_and(|id| id >= 0)
    {
        return fields.time(field as usize).map_err(execution);
    }
    let times = ms.main_table().column_accessor("TIME").map_err(execution)?;
    rows.values()
        .flatten()
        .try_fold(f64::INFINITY, |earliest, &row| {
            let ScalarValue::Float64(time) = times.scalar_cell(row).map_err(execution)? else {
                return Err(execution("expected a double-precision visibility epoch"));
            };
            Ok(earliest.min(*time))
        })
}

// SPDX-License-Identifier: LGPL-3.0-or-later
//! Manual calibrator point models on the shared calibration task front end.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use casa_components::{ComponentList, SkyComponent};
use casa_ms::derived::engine::MsCalEngine;
use casa_ms::schema::main_table::VisibilityDataColumn;
use casa_ms::virtual_model::{
    VirtualModelPublication, selected_model_combinations, virtual_model_clear_ids,
};
use casa_ms::{
    MeasurementSet, MsSelection, ResolvedMsSelection, VirtualModelEntry, VirtualModelKind,
};
use casa_provider_contracts::{
    CasaEntry, CasaFloat, CasaKey, CasaValue, FloatDtype, ParameterValue,
};
use casa_tables::TableWriteLock;
use casa_task_runtime::{TaskFailure, TaskInvocation, TaskSuccess};
use casa_types::ScalarValue;
use casa_types::measures::direction::MDirection;
use casa_types::measures::epoch::MEpoch;
use casa_types::measures::frame::MeasFrame;
use casa_types::measures::frequency::{CasaFrequencyFrame, FrequencyRef, MFrequency};
use casa_types::quanta::{Quantity, Unit};

use crate::attachment::OpenedMs;
use crate::ft::image::ImageModel;
use crate::ft::setjy::solar2010::Solar2010;
use crate::ft::setjy::standards::fixed::FixedScale;
use crate::ft::setjy::standards::{CalibratorSpectrum, CatalogMatch, FluxCatalog, FluxScale};
use crate::ft::setjy::{ManualFlux, sampled_point};
use crate::ft::virtual_publication::VirtualFtPublication;
use crate::ft::write_model_scratch;
use crate::interpolation::Curve;
use crate::solve::grouping::VisibilityModels;

pub(super) mod direct;
mod fluxdict;
mod model_files;
mod resolved;
mod solar2012;

enum FluxParameters {
    Manual(ManualFlux),
    Solar2010(PathBuf),
    Standard {
        catalog: FluxCatalog,
        interpolation: Curve,
    },
}

#[derive(Default)]
struct FieldModel {
    entries: Vec<VirtualModelEntry>,
    images: Vec<ImageModel>,
}

type FieldModels = BTreeMap<i32, FieldModel>;

pub(super) fn run(task: &TaskInvocation<'_>) -> Result<TaskSuccess, TaskFailure> {
    if task.flag("listmodels")? {
        return model_files::list(task);
    }
    let path = task.path("vis")?;
    let table_path = task.workspace().join(&path);
    if !table_path.exists() {
        return Err(execution(format!(
            "Table {} does not exist",
            table_path.display()
        )));
    }
    let scratch = task.flag("usescratch")?;
    let value = if let Some(paths) = super::multi_ms::partition_paths(&path)? {
        run_multi(task, &path, &paths, scratch)?
    } else {
        let opened = OpenedMs::open(&path).map_err(execution)?;
        run_ms(task, &path, opened, &mut [], scratch, false)?
    };
    TaskSuccess::new(value, &())
}

fn run_multi(
    task: &TaskInvocation<'_>,
    path: &Path,
    paths: &[PathBuf],
    scratch: bool,
) -> Result<CasaValue, TaskFailure> {
    // CASA gives every member a MODEL_DATA column even when a partition
    // has no selected rows or the requested prediction is virtual.
    for part in paths {
        OpenedMs::open(part)
            .and_then(|opened| opened.initialize_calibrater(false, false, true))
            .and_then(OpenedMs::release)
            .map_err(execution)?;
    }
    if !scratch {
        let root = OpenedMs::open(path).map_err(execution)?;
        let mut parts = paths
            .iter()
            .map(|part| OpenedMs::open(part).map_err(execution))
            .collect::<Result<Vec<_>, _>>()?;
        let outcome = run_ms(task, path, root, &mut parts, false, false);
        for part in parts {
            part.release().map_err(execution)?;
        }
        return outcome;
    }

    let mut first = None;
    let mut last_failure = None;
    for part in paths {
        let opened = OpenedMs::open(part).map_err(execution)?;
        match run_ms(task, part, opened, &mut [], true, true) {
            Ok(value) if matches!(&value, CasaValue::Dict { entries } if !entries.is_empty()) => {
                first.get_or_insert(value);
            }
            Ok(_) => {}
            Err(error) => {
                tracing::warn!(partition = %part.display(), message = %error.message,
                    "setjy failed for Multi-MS partition");
                last_failure = Some(error);
            }
        }
    }
    first.ok_or_else(|| {
        let reason =
            last_failure.map_or_else(|| "no selected rows".to_owned(), |error| error.message);
        execution(format!("Error in parallel processing of MMS: {reason}"))
    })
}

fn run_ms(
    task: &TaskInvocation<'_>,
    path: &Path,
    mut opened: OpenedMs,
    parts: &mut [OpenedMs],
    scratch: bool,
    partition: bool,
) -> Result<CasaValue, TaskFailure> {
    let standard = task.text("standard")?;
    let solar_2012 = standard.as_deref() == Some("Butler-JPL-Horizons 2012");
    if !scratch
        && let Some(scale) = standard
            .as_deref()
            .filter(|scale| scale.starts_with("Butler-JPL-Horizons"))
    {
        return Err(execution(format!(
            "usescratch=False and standard={scale}. Virtual model can not be used for an ephemeris object calibrator. Use usescratch=True."
        )));
    }
    let mut selection = selection(task)?;
    if standard.as_deref() == Some("fluxscale") {
        // The wrapper validates the dictionary before its virtual imager
        // call applies time, scan or observation predicates.
        if scratch {
            add_time_selection(task, &mut selection)?;
        }
        let selected = opened.resolve(&selection).map_err(execution)?;
        if selected.selected_rows.is_empty() {
            return empty_selection(task, partition);
        }
        let result = fluxdict::run(task, opened, parts, selection, &selected, scratch)?;
        record_history(task, path, partition);
        return Ok(result);
    }
    // The task first checks field, SPW and intent for virtual models.
    // The imager then applies time, scan and observation to the model selection.
    let time_filtered = ["timerange", "scan", "observation"].into_iter().any(
        |name| !matches!(task.value(name), Some(ParameterValue::String(value)) if value.is_empty()),
    );
    if !scratch && time_filtered {
        let initial = opened.resolve(&selection).map_err(execution)?;
        if initial.selected_rows.is_empty() {
            return empty_selection(task, partition);
        }
    }
    add_time_selection(task, &mut selection)?;
    let mut selected = opened.resolve(&selection).map_err(execution)?;
    if selected.selected_rows.is_empty() {
        if solar_2012 {
            return Ok(CasaValue::Dict {
                entries: Vec::new(),
            });
        }
        if !scratch && time_filtered {
            return Ok(flux_dictionary(Vec::new()));
        }
        return empty_selection(task, partition);
    }
    if solar_2012 {
        return solar2012::run(task, path, opened, &selection, &selected, partition);
    }
    let Some(flux) = flux_parameters(task)? else {
        if scratch {
            opened = opened
                .initialize(VisibilityDataColumn::ModelData)
                .map_err(execution)?;
        }
        opened.release().map_err(execution)?;
        return Ok(CasaValue::Dict {
            entries: Vec::new(),
        });
    };
    // Imager::sjy_make_visibilities selects all channels of each chosen SPW.
    selected.channel_selections.clear();
    let spws = requested_windows(&opened.measurement_set, selection.spw.as_deref())?;
    let image = model_files::task_image(task)?;
    let calibration_directions = matches!(flux, FluxParameters::Solar2010(_)) && image.is_none();
    let (models, fluxes, completed) = prepare(
        &opened.measurement_set,
        &selected,
        &flux,
        &spws,
        task.flag("scalebychan")?,
        image.as_deref().map(|path| (path, scratch)),
    )?;
    let has_models = models.values().any(|model| !model.entries.is_empty());
    let partially_written = !completed && has_models;
    if scratch {
        opened = opened
            .initialize(VisibilityDataColumn::ModelData)
            .map_err(execution)?;
        // An unrecognized calibrator reports unit flux and leaves any
        // existing scratch cells alone.
        if (!completed || models.values().any(|model| model.entries.is_empty())) && has_models {
            selection.field = Some(
                models
                    .iter()
                    .filter(|(_, model)| !model.entries.is_empty())
                    .map(|(field, _)| field.to_string())
                    .collect::<Vec<_>>()
                    .join(","),
            );
            selected = opened.resolve(&selection).map_err(execution)?;
            selected.channel_selections.clear();
        }
    }
    publish_models(
        &mut opened,
        parts,
        &selected,
        models,
        &spws,
        scratch,
        calibration_directions,
    )?;
    opened.release().map_err(execution)?;
    record_history(task, path, partition);
    if partially_written {
        // CASA's cleanup after the later failure raises once an earlier
        // component list has already been applied and retired.
        return Err(execution(
            "flux coefficients are missing for a later field after earlier models were written",
        ));
    }
    Ok(fluxes)
}

fn record_history(task: &TaskInvocation<'_>, path: &Path, partition: bool) {
    if !partition {
        super::history::record_task(task, path);
        return;
    }
    let mut session = task.session().clone();
    let updated = session
        .set(
            "vis",
            ParameterValue::String(path.to_string_lossy().into_owned()),
        )
        .and_then(|()| session.set("ismms", ParameterValue::Bool(true)));
    if let Err(error) = updated {
        tracing::warn!(partition = %path.display(), %error, "updating setjy partition HISTORY");
        return;
    }
    super::history::record_task(&TaskInvocation::new(&session, task.workspace()), path);
}

fn publish_models(
    opened: &mut OpenedMs,
    parts: &mut [OpenedMs],
    selected: &ResolvedMsSelection,
    models: FieldModels,
    spws: &[i32],
    scratch: bool,
    calibration_directions: bool,
) -> Result<(), TaskFailure> {
    if scratch {
        publish_scratch(
            &mut opened.measurement_set,
            &mut opened.lock,
            selected,
            models,
            calibration_directions,
        )?;
    } else if parts.is_empty() {
        publish_virtual(&mut opened.measurement_set, &mut opened.lock, models, spws)?;
    } else {
        for (field, model) in models.into_iter().rev() {
            let inputs = std::iter::once(&opened.measurement_set)
                .chain(parts.iter().map(|part| &part.measurement_set));
            let components = model
                .entries
                .into_iter()
                .filter(|entry| entry.kind == VirtualModelKind::ComponentList)
                .collect();
            let mut publication = VirtualFtPublication::replacing_field(
                inputs,
                components,
                model.images,
                field,
                spws,
            )
            .map_err(execution)?;
            publication
                .publish(
                    std::iter::once((&mut opened.measurement_set, &mut opened.lock)).chain(
                        parts
                            .iter_mut()
                            .map(|part| (&mut part.measurement_set, &mut part.lock)),
                    ),
                )
                .map_err(execution)?;
        }
    }
    Ok(())
}

fn publish_scratch(
    ms: &mut MeasurementSet,
    lock: &mut TableWriteLock,
    selected: &ResolvedMsSelection,
    models: FieldModels,
    calibration_directions: bool,
) -> Result<(), TaskFailure> {
    // Keep the temporary image owners alive through the scratch write.
    let entries: Vec<_> = models
        .values()
        .flat_map(|model| model.entries.iter().cloned())
        .collect();
    let geometry = if calibration_directions && !entries.is_empty() {
        Some(
            VisibilityModels::from_entries(ms, &selected.selected_rows, &entries)
                .and_then(|models| models.with_calibration_field_directions(ms))
                .map_err(execution)?,
        )
    } else {
        None
    };
    write_model_scratch(ms, selected, &entries, false, true, geometry, lock).map_err(execution)
}

fn publish_virtual(
    ms: &mut MeasurementSet,
    lock: &mut TableWriteLock,
    models: FieldModels,
    windows: &[i32],
) -> Result<(), TaskFailure> {
    for (field, mut model) in models.into_iter().rev() {
        let mut publication = VirtualModelPublication::default();
        let published =
            publication.replace_windows_holding(ms, &model.entries, lock, field, windows);
        if publication.may_reference_images() {
            for image in &mut model.images {
                image.retain();
            }
        }
        published.map_err(execution)?;
    }
    Ok(())
}

fn empty_selection(task: &TaskInvocation<'_>, partition: bool) -> Result<CasaValue, TaskFailure> {
    if partition || task.flag("ismms")? {
        return Ok(CasaValue::Dict {
            entries: Vec::new(),
        });
    }
    Err(execution(
        "No rows were selected. Please check your data selection",
    ))
}

fn flux_parameters(task: &TaskInvocation<'_>) -> Result<Option<FluxParameters>, TaskFailure> {
    let pending =
        |name| TaskFailure::validation("this setjy model configuration is pending #1519").at(name);
    let standard = task.text("standard")?;
    let scale = match standard.as_deref() {
        Some("manual" | "Butler-JPL-Horizons 2010") => None,
        Some(name) => Some(FluxScale::named(name).ok_or_else(|| pending("standard"))?),
        None => return Err(pending("standard")),
    };
    if !matches!(task.value("fluxdict"), Some(ParameterValue::Table(_))) {
        return Err(TaskFailure::validation("fluxdict must be a dictionary").at("fluxdict"));
    }
    let mut index = task.numbers("spix")?;
    if index.is_empty() {
        index.push(0.);
    }
    if index[0].abs() > 10. {
        let limit = if index[0] < -10. {
            "less than -10"
        } else {
            "greater than 10"
        };
        return Err(TaskFailure::validation(format!(
            "The spectral index is {limit}! Please check the spix parameter."
        ))
        .at("spix"));
    }
    let reference = task.text("reffreq")?.unwrap_or_else(|| "1GHz".into());
    let Some(reference) = reference_frequency(&reference) else {
        return Ok(None);
    };
    let input = task.numbers("fluxdensity")?;
    let Some(&intensity) = input.first() else {
        return Err(TaskFailure::validation("fluxdensity must contain Stokes I").at("fluxdensity"));
    };
    if intensity != 0. && standard.as_deref() == Some("Butler-JPL-Horizons 2010") {
        return Ok(Some(FluxParameters::Solar2010(
            task.workspace().to_path_buf(),
        )));
    }
    let scale = scale
        .or_else(|| (intensity < 0.).then_some(FluxScale::Fixed(FixedScale::PerleyButler2010)));
    if let Some(scale) = scale {
        let interpolation = if scale.time_interpolation() {
            match task.text("interpolation")?.as_deref() {
                Some("nearest") => Curve::Nearest,
                Some("linear") => Curve::Linear,
                Some("spline") => Curve::Spline,
                Some("cubic") => Curve::Cubic,
                _ => return Ok(None),
            }
        } else {
            Curve::Nearest
        };
        // The task suppresses positive user flux on a named standard.
        // An explicit zero passes through to the manual imager branch.
        if intensity != 0. {
            let catalog = FluxCatalog::open(scale).map_err(execution)?;
            return Ok(Some(FluxParameters::Standard {
                catalog,
                interpolation,
            }));
        }
    }
    if input.len() > 4 {
        return Err(
            TaskFailure::validation("manual setjy needs one to four Stokes flux values")
                .at("fluxdensity"),
        );
    }
    let mut flux = [0.; 4];
    flux[..input.len()].copy_from_slice(&input);
    ManualFlux::new(
        flux,
        reference,
        index,
        task.numbers("polindex")?,
        task.numbers("polangle")?,
        task.number("rotmeas")?,
    )
    .map(|flux| Some(FluxParameters::Manual(flux)))
    .map_err(execution)
}

fn reference_frequency(text: &str) -> Option<MFrequency> {
    let text = text.trim();
    let mut frame = FrequencyRef::LSRK;
    let mut quantity = text;
    if let Some((prefix, rest)) = text.split_once(char::is_whitespace)
        && let Ok(CasaFrequencyFrame::Defined(parsed)) = prefix.parse()
    {
        frame = parsed;
        quantity = rest.trim();
    }
    let quantity = quantity.parse::<Quantity>().ok()?;
    let hz = quantity.get_value_in(&Unit::new("Hz").expect("Hz")).ok()?;
    Some(MFrequency::new(hz, frame))
}

fn selection(task: &TaskInvocation<'_>) -> Result<MsSelection, TaskFailure> {
    let text = |name| -> Result<Option<String>, TaskFailure> {
        Ok(task.text(name)?.filter(|value| !value.is_empty()))
    };
    Ok(MsSelection {
        field: text("field")?,
        spw: text("spw")?,
        intent: text("intent")?,
        ..MsSelection::default()
    })
}

fn add_time_selection(
    task: &TaskInvocation<'_>,
    selection: &mut MsSelection,
) -> Result<(), TaskFailure> {
    let text = |name, argument| match task.value(name) {
        Some(ParameterValue::String(s)) => Ok((!s.is_empty()).then(|| s.clone())),
        _ => Err(TaskFailure::validation(format!("argument {argument} must be a string")).at(name)),
    };
    selection.timerange = text("timerange", "_time")?;
    selection.scan = text("scan", "_scan")?;
    selection.observation = super::casa_read::selector_text(task.value("observation"));
    Ok(())
}

fn prepare(
    ms: &MeasurementSet,
    selected: &ResolvedMsSelection,
    flux: &FluxParameters,
    spws: &[i32],
    by_channel: bool,
    image: Option<(&Path, bool)>,
) -> Result<(FieldModels, CasaValue, bool), TaskFailure> {
    let mut groups = BTreeMap::<i32, BTreeMap<i32, Vec<usize>>>::new();
    for row in &selected.selected_rows {
        groups
            .entry(row.field_id)
            .or_default()
            .entry(row.spectral_window_id)
            .or_default()
            .push(row.row_index);
    }
    let engine = if matches!(flux, FluxParameters::Solar2010(_)) && image.is_none() {
        MsCalEngine::for_calibration(ms)
    } else {
        MsCalEngine::new(ms)
    }
    .map_err(execution)?;
    let field_table = ms.field().map_err(execution)?;
    let windows = ms.spectral_window().map_err(execution)?;
    let time = ms.main_table().column_accessor("TIME").map_err(execution)?;
    let ScalarValue::Float64(epoch) = time.scalar_cell(0).map_err(execution)? else {
        return Err(execution("expected a double-precision visibility epoch"));
    };
    let frame = engine
        .antenna_frame(*epoch, selected.selected_rows[0].antenna1_id as usize)
        .map_err(execution)?
        .with_position(engine.observatory_position().clone());
    let mut models = FieldModels::new();
    let mut returned = BTreeMap::new();
    let mut completed = true;
    for (field, windows_for_field) in groups.into_iter().rev() {
        let direction = engine
            .observation_direction_j2000(*epoch, field as usize)
            .map_err(execution)?;
        let frame = frame.clone().with_direction(direction.clone());
        let name = field_table.name(field as usize).map_err(execution)?;
        let spectrum = match flux {
            FluxParameters::Standard {
                catalog,
                interpolation,
            } => {
                let midpoint = field_midpoint(&time, &windows_for_field)?;
                match catalog
                    .spectrum(&name, &direction, midpoint, *interpolation)
                    .map_err(execution)?
                {
                    CatalogMatch::Spectrum(spectrum) => Some(spectrum),
                    CatalogMatch::UnknownSource => None,
                    CatalogMatch::MissingCoefficients(source) => {
                        // The failed field never reaches the model clear. Earlier
                        // fields remain eligible for publication.
                        tracing::warn!(field = %name, source, "source has no coefficients for the requested flux scale");
                        completed = false;
                        break;
                    }
                }
            }
            FluxParameters::Manual(_) | FluxParameters::Solar2010(_) => None,
        };
        let solar = if let FluxParameters::Solar2010(workspace) = flux
            && image.is_none()
        {
            let midpoint = MEpoch::from_mjd(
                field_midpoint(&time, &windows_for_field)?,
                engine.time_reference(),
            );
            match Solar2010::open(workspace, &name, &midpoint, &frame) {
                Ok(solar) => Some(solar),
                Err(error) => {
                    tracing::warn!(field = %name, %error, "no 2010 Solar System flux model");
                    completed = false;
                    break;
                }
            }
        } else {
            None
        };
        let model = models.entry(field).or_default();
        let mut field_flux = BTreeMap::new();
        let mut first_jy = None;
        for &spw in spws {
            let all_hz = windows.chan_freq(spw as usize).map_err(execution)?;
            let frequency_frame = windows
                .frequency_reference(spw as usize)
                .map_err(execution)?;
            let num_channels = i32::try_from(all_hz.len()).map_err(execution)?;
            // CASA assigns the scalar median through Vector::set, which
            // constructs an MFrequency in the default LSRK frame.
            let frame_ref = if by_channel {
                frequency_frame
            } else {
                FrequencyRef::LSRK
            };
            let planned = selected_frequencies(all_hz, frame_ref, by_channel)?;
            let (component, stokes) = if image.is_some() {
                (
                    None,
                    [
                        spectrum
                            .as_ref()
                            .map_or(1., |spectrum| spectrum.flux(planned[0].hz())),
                        0.,
                        0.,
                        0.,
                    ],
                )
            } else if let Some(solar) = &solar {
                let (component, stokes) = solar
                    .component(Some(direction.clone()), &planned)
                    .map_err(execution)?;
                (Some(component), stokes)
            } else {
                point_model(flux, spectrum.as_ref(), &direction, &planned, &frame)?
            };
            first_jy.get_or_insert(stokes[0]);
            if let (Some(rows), Some(component)) = (windows_for_field.get(&spw), component) {
                let container = ComponentList::from(vec![component])
                    .to_record()
                    .map_err(execution)?;
                model.entries.push(VirtualModelEntry {
                    kind: VirtualModelKind::ComponentList,
                    index_combination: selected_model_combinations(ms, rows)
                        .map_err(execution)?
                        .index_combination,
                    channel_selection: vec![[spw, 0, num_channels, 1]],
                    container,
                });
            }
            field_flux.insert(
                spw.to_string(),
                CasaValue::Dict {
                    entries: vec![entry(
                        "fluxd",
                        CasaValue::FloatArray {
                            dtype: FloatDtype::Float64,
                            shape: vec![4],
                            data: stokes.map(CasaFloat).to_vec(),
                        },
                    )],
                },
            );
        }
        if let Some((path, scratch)) = image {
            let params = resolved::ImageParameters {
                path,
                by_channel,
                scratch,
                windows: spws,
            };
            use resolved::ResolvedFlux;
            let image_spectrum = match flux {
                FluxParameters::Standard {
                    catalog,
                    interpolation,
                } if by_channel => {
                    match catalog
                        .spectrum(&name, &direction, *epoch / 86_400., *interpolation)
                        .map_err(execution)?
                    {
                        CatalogMatch::Spectrum(spectrum) => Some(spectrum),
                        _ => None,
                    }
                }
                _ => None,
            };
            let flux = match flux {
                FluxParameters::Manual(manual) if manual.intensity_jy() != 0. => {
                    ResolvedFlux::Manual(manual)
                }
                FluxParameters::Standard { .. } => {
                    ResolvedFlux::Catalog(image_spectrum.as_ref(), first_jy.unwrap_or(1.))
                }
                FluxParameters::Solar2010(_) => ResolvedFlux::Catalog(None, 1.),
                _ => ResolvedFlux::Unscaled,
            };
            model.images = params.prepare(ms, selected, &frame, field, flux)?;
            model
                .entries
                .extend(model.images.iter().map(|image| image.entry.clone()));
        }
        field_flux.insert("fieldName".into(), CasaValue::String { value: name });
        returned.insert(
            field.to_string(),
            CasaValue::Dict {
                entries: field_flux
                    .into_iter()
                    .map(|(key, value)| entry(key, value))
                    .collect(),
            },
        );
    }
    let entries = returned
        .into_iter()
        .map(|(key, value)| entry(key, value))
        .collect();
    let returned = if completed {
        flux_dictionary(entries)
    } else {
        CasaValue::Dict { entries }
    };
    Ok((models, returned, completed))
}

fn field_midpoint(
    time: &casa_tables::TableColumn<'_>,
    windows: &BTreeMap<i32, Vec<usize>>,
) -> Result<f64, TaskFailure> {
    let rows = windows.values().flatten().copied();
    let first = rows.clone().min().expect("a selected field contains rows");
    let last = rows.max().expect("a selected field contains rows");
    let (ScalarValue::Float64(first), ScalarValue::Float64(last)) = (
        time.scalar_cell(first).map_err(execution)?,
        time.scalar_cell(last).map_err(execution)?,
    ) else {
        return Err(execution("expected double-precision visibility epochs"));
    };
    Ok((first + (last - first) * 0.5) / 86_400.)
}

fn point_model(
    flux: &FluxParameters,
    spectrum: Option<&CalibratorSpectrum<'_>>,
    direction: &MDirection,
    frequencies: &[MFrequency],
    frame: &MeasFrame,
) -> Result<(Option<SkyComponent>, [f64; 4]), TaskFailure> {
    let (component, stokes) = match (flux, spectrum) {
        (FluxParameters::Manual(manual), _) => {
            manual.component(direction.clone(), frequencies, frame)
        }
        (_, Some(spectrum)) => {
            let samples = frequencies
                .iter()
                .map(|frequency| [spectrum.flux(frequency.hz()), 0., 0., 0.])
                .collect();
            sampled_point(direction.clone(), frequencies, samples)
        }
        (_, None) => return Ok((None, [1., 0., 0., 0.])),
    }
    .map_err(execution)?;
    Ok((Some(component), stokes))
}

fn requested_windows(ms: &MeasurementSet, spw: Option<&str>) -> Result<Vec<i32>, TaskFailure> {
    if spw.is_some() {
        return virtual_model_clear_ids(ms, None, spw)
            .map(|(_, spws)| spws)
            .map_err(execution);
    }
    let count = ms.spectral_window().map_err(execution)?.row_count();
    Ok((0..i32::try_from(count).map_err(execution)?).collect())
}

fn flux_dictionary(mut entries: Vec<CasaEntry>) -> CasaValue {
    entries.push(entry(
        "format",
        CasaValue::String {
            value: "{field Id: {spw Id: {fluxd: [I,Q,U,V] in Jy}, 'fieldName':field name }}".into(),
        },
    ));
    CasaValue::Dict { entries }
}

fn selected_frequencies(
    mut frequencies: Vec<f64>,
    frame: FrequencyRef,
    by_channel: bool,
) -> Result<Vec<MFrequency>, TaskFailure> {
    if frequencies.is_empty() || frequencies.iter().any(|f| !f.is_finite() || *f <= 0.) {
        return Err(execution("setjy needs positive finite channel frequencies"));
    }
    if !by_channel {
        // casacore averages the two middle values only for even counts up to 100.
        let count = frequencies.len();
        let (_, &mut lower, upper) =
            frequencies.select_nth_unstable_by((count - 1) / 2, f64::total_cmp);
        let median = if count.is_multiple_of(2) && count <= 100 {
            0.5 * (lower + upper.iter().copied().fold(f64::INFINITY, f64::min))
        } else {
            lower
        };
        frequencies = vec![median];
    }
    Ok(frequencies
        .into_iter()
        .map(|hz| MFrequency::new(hz, frame))
        .collect())
}

fn entry(key: impl Into<String>, value: CasaValue) -> CasaEntry {
    CasaEntry {
        key: CasaKey::String(key.into()),
        value,
    }
}

fn execution(error: impl std::fmt::Display) -> TaskFailure {
    TaskFailure::execution(error.to_string())
}

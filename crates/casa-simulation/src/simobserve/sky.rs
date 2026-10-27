// SPDX-License-Identifier: LGPL-3.0-or-later
//! The sky model: `modifymodel` and `flatimage` of `skymodel` into
//! `<stem>.skymodel` and `<stem>.skymodel.flat`, or, with a component list
//! alone, the `<stem>.compskymodel` and its flat image once the antenna list
//! gives the resolution its cell is drawn from.

use std::path::{Path, PathBuf};

use casa_components::ComponentList;
use casa_types::quanta::Quantity;

use super::antennas::Array;
use super::{Run, SimobserveError, entry_exists, exists, make_way, python, remove};
use crate::simutil::qa;
use crate::simutil::skymodel::{
    ComponentModel, ModelOverrides, SkyModelError, flat_image, modify_model,
};

/// What the task knows of its model: `modifymodel`'s return, or what it
/// makes of a component list.
pub(super) struct Model {
    /// `model_refdir`: the model's centre, as `simutil` writes directions.
    pub(super) refdir: String,
    /// `model_cell`: both axes' cells, positive. A component-only model's
    /// is the resolution over 20, set once the antenna list is read.
    pub(super) cell: [Quantity; 2],
    /// `model_size`: the model's extent along each axis.
    pub(super) size: [Quantity; 2],
    /// `model_nchan`: the channels.
    pub(super) nchan: usize,
    /// `model_specrefval`: the reference channel's frequency.
    pub(super) specrefval: Quantity,
    /// `model_specrefpix`: the reference channel.
    pub(super) specrefpix: f64,
    /// `model_width`: the channel width.
    pub(super) width: Quantity,
    /// `model_stokes`: the Stokes planes, `"I"` to `"IQUV"`.
    pub(super) stokes: String,
}

/// The sky model before the antenna list is read.
pub(super) struct SkyInput {
    /// `components_only`: no model image, a component list alone.
    pub(super) components_only: bool,
    pub(super) model: Model,
    kind: InputKind,
}

enum InputKind {
    /// The model image written (`newmodel`) and its flat image.
    Image { image: PathBuf, flat: PathBuf },
    /// The component-only model and its list.
    Components {
        model: Box<ComponentModel>,
        list: ComponentList,
    },
}

/// The sky model the task observes.
pub(super) struct Sky {
    /// `components_only`: a component list alone.
    pub(super) components_only: bool,
    pub(super) model: Model,
    /// The image `predict` reads (`newmodel`); none for a component list
    /// alone, which `predict` reads as a list.
    pub(super) image: Option<PathBuf>,
    /// `modelflat`: the flat model.
    pub(super) flat: PathBuf,
}

/// The model image or the component list, as far as CASA takes them before
/// reading the antenna list; and the map's size when `mapsize` is larger
/// than a component-only model.
pub(super) fn prepare(run: &mut Run<'_>) -> Result<SkyInput, SimobserveError> {
    let skymodel = run.skymodel();
    let mut input = if exists(&skymodel) {
        image_model(run, Path::new(&skymodel))?
    } else {
        component_model(run)?
    };
    let mapsize = &run.parameters.mapsize;
    if let InputKind::Components { model, .. } = &mut input.kind
        && let Some(given) = mapsize.first().filter(|size| !size.is_empty())
    {
        let map_arcsec = qa::value_in(&qa::read(given)?, "arcsec")?;
        let model_arcsec = qa::value_in(&input.model.size[0], "arcsec")?;
        if map_arcsec > model_arcsec {
            let size = format!("{map_arcsec:.6}arcsec");
            model.size = [size.clone(), size.clone()];
            let size = qa::read(&size)?;
            input.model.size = [size.clone(), size];
        }
    }
    Ok(input)
}

/// `modifymodel` of `skymodel` into `<stem>.skymodel` (`<stem>.newmodel`
/// when `skymodel` is named `<stem>.skymodel`), and `flatimage` of that,
/// with `complist`'s components added, into `<stem>.skymodel.flat`.
fn image_model(run: &mut Run<'_>, skymodel: &Path) -> Result<SkyInput, SimobserveError> {
    let parameters = run.parameters;
    let default_model = format!("{}.skymodel", run.stem);
    let newmodel = if skymodel == Path::new(&default_model) {
        run.file(".newmodel")
    } else {
        run.file(".skymodel")
    };
    if entry_exists(&newmodel) {
        if same_file(skymodel, &newmodel) {
            return Err(SimobserveError::raised(format!(
                "skymodel {} is the model image simobserve writes; copy it elsewhere first",
                skymodel.display()
            )));
        }
        if !parameters.overwrite {
            return Err(SimobserveError::raised(format!(
                "{} exists -- please delete it, change skymodel, or set overwrite=T",
                newmodel.display()
            )));
        }
        remove(&newmodel)?;
    }
    let one = |text: &str| -> Vec<String> {
        if text.is_empty() {
            Vec::new()
        } else {
            vec![text.to_string()]
        }
    };
    let overrides = ModelOverrides {
        inbright: parameters.inbright.clone(),
        indirection: one(&parameters.indirection),
        incell: one(&parameters.incell),
        incenter: parameters.incenter.clone(),
        inwidth: parameters.inwidth.clone(),
    };
    let summary = modify_model(skymodel, &newmodel, &overrides)?;
    run.wrote(&newmodel);
    let flat = run.file(".skymodel.flat");
    if flat.exists() && !run.predict {
        tracing::warn!(
            "flat sky model {} exists, predict not requested",
            flat.display()
        );
        tracing::warn!(
            " working from existing model image - please delete it if you wish to overwrite."
        );
    } else {
        let complist =
            Some(Path::new(&parameters.complist)).filter(|path| !path.as_os_str().is_empty());
        make_way(&flat)?;
        make_way(&flat_of(&newmodel))?;
        let written = flat_image(&newmodel, complist)?;
        if written != flat {
            remove(&flat)?;
            std::fs::rename(&written, &flat).map_err(SimobserveError::io(&written))?;
        }
        run.wrote(&flat);
    }
    let model = Model {
        refdir: summary.refdir,
        cell: summary.cell,
        size: summary.size,
        nchan: summary.nchan,
        specrefval: summary.specrefval.quantity()?,
        specrefpix: summary.specrefpix,
        width: summary.width.quantity()?,
        stokes: summary.stokes,
    };
    Ok(SkyInput {
        components_only: false,
        model,
        kind: InputKind::Image {
            image: newmodel,
            flat,
        },
    })
}

/// `<image>.flat`, the flat image `flatimage` and a component model write
/// beside `image`.
fn flat_of(image: &Path) -> PathBuf {
    PathBuf::from(format!("{}.flat", image.display()))
}

/// Whether `a` and `b` are the same file.
fn same_file(a: &Path, b: &Path) -> bool {
    match (a.canonicalize(), b.canonicalize()) {
        (Ok(a), Ok(b)) => a == b,
        _ => false,
    }
}

/// The component-only model `simobserve` makes of `complist`.
fn component_model(run: &Run<'_>) -> Result<SkyInput, SimobserveError> {
    tracing::info!("component-only simulation");
    let parameters = run.parameters;
    let channels = usize::try_from(parameters.comp_nchan)
        .ok()
        .filter(|&channels| channels > 0)
        .ok_or_else(|| {
            SimobserveError::raised(format!(
                "comp_nchan must be at least 1, not {}",
                parameters.comp_nchan
            ))
        })?;
    let list = ComponentList::read_table(&parameters.complist).map_err(SkyModelError::from)?;
    let model = ComponentModel::from_list(&list, &parameters.compwidth, channels)?;
    tracing::info!("scaling model bandwidth by model_nchan");
    let size = [qa::read(&model.size[0])?, qa::read(&model.size[1])?];
    let placeholder = qa::number(0.0);
    Ok(SkyInput {
        components_only: true,
        model: Model {
            refdir: model.refdir.clone(),
            cell: [placeholder.clone(), placeholder],
            size,
            nchan: model.nchan,
            specrefval: model.specrefval.clone(),
            specrefpix: model.specrefpix,
            width: model.width.clone(),
            stokes: model.stokes.clone(),
        },
        kind: InputKind::Components {
            model: Box::new(model),
            list,
        },
    })
}

/// The model once the antenna list gives the resolution: a component-only
/// model's cell and its `.compskymodel`; then CASA's check that the cell
/// samples the resolution.
pub(super) fn complete(
    run: &mut Run<'_>,
    input: SkyInput,
    array: &Array,
) -> Result<Sky, SimobserveError> {
    let SkyInput {
        components_only,
        mut model,
        kind,
    } = input;
    let (image, flat) = match kind {
        InputKind::Image { image, flat } => (Some(image), flat),
        InputKind::Components {
            model: components,
            list,
        } => {
            let cell = qa::read(&format!("{}arcsec", python(array.resolution_arcsec / 20.)))?;
            model.cell = [cell.clone(), cell];
            let image = run.file(".compskymodel");
            if entry_exists(&image) {
                if !run.parameters.overwrite {
                    return Err(SimobserveError::raised(format!(
                        "{} exists and is inconsistent with required size={} and direction. \
                         Please set overwrite=True",
                        image.display(),
                        components.image_size(&model.cell[0])?
                    )));
                }
                remove(&image)?;
            }
            make_way(&flat_of(&image))?;
            let flat = components.write(&list, &model.cell, &image)?;
            run.wrote(&image);
            run.wrote(&flat);
            (None, flat)
        }
    };
    let resolution = array.resolution_arcsec;
    let cell_arcsec = qa::value_in(&model.cell[0], "arcsec")?;
    if resolution < cell_arcsec {
        remove(&flat)?;
        run.products.retain(|known| *known != flat);
        return Err(SimobserveError::raised(format!(
            "Sky model cell of {} asec is very large compared to highest resolution {} asec - \
             this will lead to blank or erroneous output. (Did you set incell?)",
            python(cell_arcsec),
            python(resolution)
        )));
    }
    if resolution < 2. * cell_arcsec {
        tracing::warn!(
            "Sky model cell of {} asec is large compared to highest resolution {} asec. (Did \
             you set incell?)",
            python(cell_arcsec),
            python(resolution)
        );
    }
    Ok(Sky {
        components_only,
        model,
        image,
        flat,
    })
}

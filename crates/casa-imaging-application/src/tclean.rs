// SPDX-License-Identifier: LGPL-3.0-or-later

//! CASA `tclean`'s parameters, as the catalog's `tclean` surface resolves
//! them, read into one [`ImagingRequest`] (#931).
//!
//! The surface keeps all 92 of tclean's parameters active whatever the
//! modes, as CASA's task takes them, and this reader applies CASA's rules for
//! which parameters a mode reads (`task_tclean.py` and the
//! `SynthesisParams*::fromRecord` readers): a parameter the request's modes
//! do not read is ignored, as CASA ignores it (`nterms` without `mtmfs`,
//! `start` in `mfs`, `wprojplanes` with the standard gridder), and a value
//! casa-rs cannot honour in those modes is refused, naming the parameter and
//! the issue that tracks it. The catalog has already refused a non-default
//! value of a parameter casa-rs implements in no mode. Every parameter is
//! read exactly once, and a test resolves the surface and checks that none is
//! left over.
//!
//! The request holds a few values in casa-rs's own form: tclean's
//! `phasecenter` is a direction or a field id (`phasecenter`,
//! `phasecenter_field`), its `mask` a mask image or pixel boxes (`mask_image`,
//! `mask_box`), and its `cycleniter` the minor-cycle length
//! (`minor_cycle_length`); `nchan` is a cube's `channel_count`.

use std::{collections::BTreeSet, fmt, str::FromStr};

use casa_types::quanta::{Quantity, Unit};
use serde_json::{Map, Value, json};

use crate::ImagingRequest;

/// A tclean value casa-rs refuses, and the parameter that holds it.
#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
#[error("{message}")]
pub struct TcleanRefusal {
    /// The tclean parameter.
    pub parameter: String,
    /// Why it is refused.
    pub message: String,
}

impl TcleanRefusal {
    fn new(parameter: &str, message: impl Into<String>) -> Self {
        Self {
            parameter: parameter.to_string(),
            message: message.into(),
        }
    }

    /// `name = value`, which casa-rs does not implement yet; `issue` tracks
    /// it.
    fn not_yet(name: &str, value: impl fmt::Display, issue: u32) -> Self {
        Self::new(
            name,
            format!("{name}={value} is not implemented by casa-rs yet (#{issue})"),
        )
    }
}

impl ImagingRequest {
    /// The request tclean's resolved parameters `values` make: every
    /// parameter of the catalog's `tclean` surface, active or not, by name.
    /// The scan and observation selectors' catalog concepts (revision 2)
    /// keep their exact text; only an exactly empty string is unset.
    ///
    /// # Errors
    ///
    /// A value casa-rs cannot honour in the request's modes, or one the
    /// request cannot read (`imsize = [0]`), refused at its tclean parameter.
    pub fn from_tclean(values: &Map<String, Value>) -> Result<Self, TcleanRefusal> {
        let (request, _) = read(values)?;
        let request: Self = serde_path_to_error::deserialize(Value::Object(request)).map_err(
            |error: serde_path_to_error::Error<serde_json::Error>| {
                let parameter = match error.path().iter().next() {
                    Some(serde_path_to_error::Segment::Map { key }) => tclean_parameter(key),
                    _ => "",
                };
                TcleanRefusal::new(
                    parameter,
                    format!("tclean parameter {parameter:?}: {}", error.inner()),
                )
            },
        )?;
        // casa-rs's automatic mask is one plane per image; CASA masks each
        // Stokes plane and channel of its own (`autoMaskByMultiThreshold`).
        if request.usemask == crate::UseMask::AutoMultithresh
            && request.niter > 0
            && (request.stokes.len() > 1 || request.specmode != crate::SpecMode::Mfs)
        {
            return Err(TcleanRefusal::not_yet(
                "usemask",
                "'auto-multithresh' with a cube or more than one Stokes plane",
                980,
            ));
        }
        Ok(request)
    }
}

/// The tclean parameter a request field is read from.
fn tclean_parameter(field: &str) -> &str {
    match field {
        "phasecenter_field" => "phasecenter",
        "mask_box" | "mask_image" | "explicit_empty_mask" => "mask",
        "minor_cycle_length" => "cycleniter",
        other => other,
    }
}

/// The request's fields from tclean's `values`, and the parameters read.
fn read(
    values: &Map<String, Value>,
) -> Result<(Map<String, Value>, BTreeSet<&str>), TcleanRefusal> {
    let mut tclean = Tclean {
        values,
        read: BTreeSet::new(),
        request: Map::new(),
    };
    let modes = tclean.modes()?;
    tclean.selection()?;
    tclean.geometry()?;
    tclean.spectral(&modes)?;
    tclean.gridding(&modes)?;
    tclean.weighting(&modes)?;
    tclean.deconvolution(&modes)?;
    tclean.iteration()?;
    tclean.masking(&modes)?;
    tclean.extras()?;
    Ok((tclean.request, tclean.read))
}

/// The modes that decide which parameters the request reads.
struct Modes {
    cube: bool,
    gridder: &'static str,
    /// Whether CASA weighs density by field, as it does for the gridder
    /// named `mosaic` alone: it sets `mosweight=False` for `mosaicft` and
    /// `ftmosaic` (`task_tclean.py`).
    per_field_density: bool,
    deconvolver: String,
    weighting: String,
    density_weighting: bool,
    user_mask: bool,
}

/// tclean's values, each read once, and the request made from them.
struct Tclean<'a> {
    values: &'a Map<String, Value>,
    read: BTreeSet<&'a str>,
    request: Map<String, Value>,
}

impl<'a> Tclean<'a> {
    fn value(&mut self, name: &str) -> Result<&'a Value, TcleanRefusal> {
        let (key, value) = self.values.get_key_value(name).ok_or_else(|| {
            TcleanRefusal::new(name, format!("tclean parameter {name:?} is missing"))
        })?;
        self.read.insert(key.as_str());
        Ok(value)
    }

    fn mismatch(name: &str, expected: &str, value: &Value) -> TcleanRefusal {
        TcleanRefusal::new(
            name,
            format!("tclean parameter {name:?}: expected {expected}, found {value}"),
        )
    }

    fn text(&mut self, name: &str) -> Result<&'a str, TcleanRefusal> {
        let value = self.value(name)?;
        value
            .as_str()
            .map(str::trim)
            .ok_or_else(|| Self::mismatch(name, "text", value))
    }

    fn flag(&mut self, name: &str) -> Result<bool, TcleanRefusal> {
        let value = self.value(name)?;
        value
            .as_bool()
            .ok_or_else(|| Self::mismatch(name, "True or False", value))
    }

    fn integer(&mut self, name: &str) -> Result<i64, TcleanRefusal> {
        let value = self.value(name)?;
        value
            .as_i64()
            .ok_or_else(|| Self::mismatch(name, "an integer", value))
    }

    /// The non-blank items of a list of text, or of one text.
    fn texts(&mut self, name: &str) -> Result<Vec<&'a str>, TcleanRefusal> {
        let value = self.value(name)?;
        let items = match value {
            Value::Array(items) => items.iter().collect(),
            value => vec![value],
        };
        items
            .into_iter()
            .map(|item| {
                item.as_str()
                    .map(str::trim)
                    .ok_or_else(|| Self::mismatch(name, "text", value))
            })
            .filter(|item| !matches!(item, Ok("")))
            .collect()
    }

    /// A list of numbers, or one number, as casa-rs's comma-separated text.
    fn number_list(&mut self, name: &str) -> Result<String, TcleanRefusal> {
        let value = self.value(name)?;
        let items = match value {
            Value::Array(items) => items.iter().collect(),
            value => vec![value],
        };
        items
            .into_iter()
            .map(|item| {
                item.as_f64()
                    .map(|number| number.to_string())
                    .ok_or_else(|| Self::mismatch(name, "numbers", value))
            })
            .collect::<Result<Vec<_>, _>>()
            .map(|numbers| numbers.join(","))
    }

    /// Read parameters the request does not use: those its modes do not
    /// read, and those the catalog holds at the default casa-rs implements.
    fn ignore(&mut self, names: &[&str]) -> Result<(), TcleanRefusal> {
        for name in names {
            self.value(name)?;
        }
        Ok(())
    }

    fn set(&mut self, field: &str, value: impl Into<Value>) {
        self.request.insert(field.to_string(), value.into());
    }

    /// Copy `names` into the request unchanged.
    fn copy(&mut self, names: &[&str]) -> Result<(), TcleanRefusal> {
        for name in names {
            let value = self.value(name)?.clone();
            self.set(name, value);
        }
        Ok(())
    }

    /// Copy `names` when the catalog holds them: casa-rs's options of one
    /// gridder, inactive with the others.
    fn copy_present(&mut self, names: &[&str]) -> Result<(), TcleanRefusal> {
        for name in names {
            if self.values.contains_key(*name) {
                self.copy(&[name])?;
            }
        }
        Ok(())
    }

    fn modes(&mut self) -> Result<Modes, TcleanRefusal> {
        let specmode = match self.text("specmode")? {
            // CASA reads `cont` as `mfs` (`task_tclean.py`).
            "cont" | "mfs" => "mfs",
            "mvc" => return Err(TcleanRefusal::not_yet("specmode", "'mvc'", 823)),
            cube => cube,
        };
        self.set("specmode", specmode);
        // CASA projects w only with planes to project, `wprojplanes > 1` or
        // `-1`: with one plane, its default, wproject and widefield run
        // gridft (`SynthesisParamsGrid::fromRecord`; CASA 6.7.6.14 logs
        // "with ftmachine : gridft"), and with none, or fewer than -1, CASA
        // refuses them.
        let planes = self.values.get("wprojplanes").and_then(Value::as_i64);
        let gridder_name = self.text("gridder")?;
        let gridder = match gridder_name {
            // CASA's gridders by their FTMachine names (`SynthesisParamsGrid`).
            "standard" | "ft" | "gridft" => "standard",
            "wproject" | "wprojectft" | "widefield" => match planes {
                Some(planes) if planes > 1 || planes == -1 => "wproject",
                Some(1) => "standard",
                _ => {
                    return Err(TcleanRefusal::new(
                        "wprojplanes",
                        "the wproject gridder must be accompanied with wprojplanes>1 or \
                         wprojplanes=-1, as CASA's tclean requires",
                    ));
                }
            },
            "mosaic" | "mosaicft" | "ftmosaic" => "mosaic",
            "awproject" => "awproject",
            "awprojectft" => {
                return Err(TcleanRefusal::not_yet(
                    "gridder",
                    "'awprojectft', CASA's other name for 'awproject' (give gridder='awproject', \
                     which casa-rs reads its AW options with)",
                    980,
                ));
            }
            other => return Err(TcleanRefusal::not_yet("gridder", format!("{other:?}"), 980)),
        };
        self.set("gridder", gridder);
        let deconvolver = self.text("deconvolver")?;
        if !matches!(deconvolver, "hogbom" | "clark" | "multiscale" | "mtmfs") {
            // `asp` is #223's; #823 decides the other modes.
            let issue = if deconvolver == "asp" { 223 } else { 823 };
            return Err(TcleanRefusal::not_yet(
                "deconvolver",
                format!("{deconvolver:?}"),
                issue,
            ));
        }
        self.set("deconvolver", deconvolver);
        let weighting = self.text("weighting")?;
        if !matches!(
            weighting,
            "natural"
                | "uniform"
                | "superuniform"
                | "radial"
                | "briggs"
                | "briggsabs"
                | "briggsbwtaper"
        ) {
            return Err(TcleanRefusal::not_yet(
                "weighting",
                format!("{weighting:?}"),
                980,
            ));
        }
        // CASA's tclean images nothing for this pair (`task_tclean.py`).
        if weighting == "briggsbwtaper" && specmode == "mfs" {
            return Err(TcleanRefusal::new(
                "weighting",
                "weighting='briggsbwtaper' tapers each channel's bandwidth: CASA's tclean \
                 refuses it with specmode='mfs', and so does casa-rs",
            ));
        }
        self.set("weighting", weighting);
        let usemask = self.text("usemask")?;
        if usemask == "pb" {
            return Err(TcleanRefusal::not_yet("usemask", "'pb'", 823));
        }
        self.set("usemask", usemask);
        Ok(Modes {
            cube: specmode != "mfs",
            gridder,
            per_field_density: gridder_name == "mosaic",
            deconvolver: deconvolver.to_string(),
            weighting: weighting.to_string(),
            // `VisImagingWeight("natural")` and `("radial")` grid no density.
            density_weighting: !matches!(weighting, "natural" | "radial"),
            user_mask: usemask == "user",
        })
    }

    fn selection(&mut self) -> Result<(), TcleanRefusal> {
        self.copy(&["vis", "imagename"])?;
        // CASA's tclean selects whatever selectdata says: its parameter
        // classes never read it (`task_tclean.py`).
        self.ignore(&["selectdata"])?;
        let field = self.text("field")?;
        self.set("field", field);
        for name in ["spw", "uvrange", "intent"] {
            let value = self.text(name)?;
            self.set(name, value);
        }
        // Scan, observation, antenna and time selectors keep their exact text:
        // CASA's tclean refuses whitespace-only text and `none`, and its
        // grammars give padding and case their own meanings.
        for name in ["scan", "observation", "antenna", "timerange"] {
            let value = self.value(name)?;
            let text = value
                .as_str()
                .ok_or_else(|| Self::mismatch(name, "text", value))?;
            self.set(name, text);
        }
        let column = match self.text("datacolumn")?.to_ascii_lowercase().as_str() {
            // CASA images DATA when CORRECTED_DATA is absent.
            "corrected" => "none",
            "data" | "observed" => "data",
            other => {
                return Err(TcleanRefusal::new(
                    "datacolumn",
                    format!("datacolumn={other:?} is neither 'data' nor 'corrected'"),
                ));
            }
        };
        self.set("datacolumn", column);
        Ok(())
    }

    fn geometry(&mut self) -> Result<(), TcleanRefusal> {
        self.copy(&["imsize", "stokes", "startmodel", "outlierfile"])?;
        let cell = self.value("cell")?;
        if cell.as_array().is_some_and(Vec::is_empty) {
            return Err(TcleanRefusal::new(
                "cell",
                "cell is required: give the cell size, for example cell='1arcsec', as CASA's \
                 tclean requires",
            ));
        }
        self.set("cell", cell.clone());
        // A field id (`3`, or `'3'`) or a direction.
        let centre = self.text("phasecenter")?;
        let field = !centre.is_empty() && centre.bytes().all(|byte| byte.is_ascii_digit());
        self.set(
            "phasecenter",
            if field || centre.is_empty() {
                "none"
            } else {
                centre
            },
        );
        self.set("phasecenter_field", if field { centre } else { "none" });
        self.ignore(&["projection", "psfphasecenter", "vptable"])
    }

    /// Refuse the cube axes CASA derives from the channels `spw` selects,
    /// which casa-rs does not yet (#980). CASA's `getImFreq` passes
    /// `nchan=-1` and its `start` and `width` to
    /// `MSTransformRegridder::calcChanFreqs`, which starts the axis at
    /// `start` or the first selected channel and counts the selected
    /// channels from there in steps of `width`; casa-rs's axis takes as
    /// many channels as the window has, wherever it starts.
    fn refuse_derived_extent(
        &mut self,
        nchan: i64,
        counted: bool,
        start: &str,
        width: &str,
    ) -> Result<(), TcleanRefusal> {
        let channels_selected = self.text("spw")?.contains(':');
        let first_given = !start.is_empty()
            || self
                .value("channel_start")?
                .as_str()
                .is_none_or(|text| text.trim() != "none");
        let wide = !matches!(width, "" | "1");
        if nchan == -1 && !counted && (channels_selected || wide || !start.is_empty()) {
            return Err(TcleanRefusal::not_yet(
                "nchan",
                "-1 with a channel selection in spw, a start or a width, whose extent CASA \
                 derives from the selected channels; give nchan",
                980,
            ));
        }
        if channels_selected && !first_given {
            return Err(TcleanRefusal::not_yet(
                "start",
                "'' with a channel selection in spw, where CASA starts the cube at the first \
                 selected channel; give start",
                980,
            ));
        }
        Ok(())
    }

    fn spectral(&mut self, modes: &Modes) -> Result<(), TcleanRefusal> {
        // CASA reads reffreq in every mode; a cube ignores it after reading.
        self.copy(&["reffreq"])?;
        self.copy(&["channel_start", "perchanweightdensity"])?;
        let nchan = self.integer("nchan")?;
        let count = self.value("channel_count")?.clone();
        let counted = count
            .as_str()
            .is_none_or(|text| !matches!(text.trim(), "" | "none"));
        let channel_count = match nchan {
            -1 => count,
            _ if !modes.cube => count,
            n if n < 1 => {
                return Err(TcleanRefusal::new(
                    "nchan",
                    format!("nchan={n}: give -1 for every channel or a positive count"),
                ));
            }
            _ if counted => {
                return Err(TcleanRefusal::new(
                    "channel_count",
                    "give a cube's channel count once: nchan, or casa-rs's channel_count",
                ));
            }
            n => json!(n),
        };
        self.set("channel_count", channel_count);
        let start = self.text("start")?;
        let width = self.text("width")?;
        let outframe = self.text("outframe")?;
        let veltype = self.text("veltype")?;
        let restfreq = self.rest_frequencies()?;
        let interpolation = self.text("interpolation")?;
        // CASA's image header takes its spectral frame and rest frequencies
        // from these in every mode (`buildCoordinateSystemCore`).
        let frame = output_frame(outframe)?;
        self.set("restfreq", restfreq);
        if !modes.cube {
            // An mfs image's REST axis would grid the data's frequencies
            // unconverted (CASA's `freqFrameValid` is false), which casa-rs's
            // continuum does not.
            if frame == "REST" {
                return Err(TcleanRefusal::not_yet(
                    "outframe",
                    "'REST' with specmode='mfs'",
                    980,
                ));
            }
            self.set("outframe", frame);
            // CASA reads none of the cube's other axis controls in mfs.
            for (field, value) in [
                ("start", "none"),
                ("width", "none"),
                ("veltype", "RADIO"),
                ("interpolation", "linear"),
            ] {
                self.set(field, value);
            }
            return Ok(());
        }
        self.refuse_derived_extent(nchan, counted, start, width)?;
        self.set("start", start);
        self.set("width", width);
        self.set("outframe", frame);
        self.set("veltype", velocity_convention(veltype)?);
        if interpolation == "cubic" {
            return Err(TcleanRefusal::not_yet("interpolation", "'cubic'", 823));
        }
        self.set("interpolation", interpolation);
        Ok(())
    }

    /// tclean's rest frequencies, `none` when it gives none. CASA's first is
    /// the image's rest frequency and the others follow it in the header
    /// (`buildCoordinateSystemCore`); CASA reads a blank entry as a zero
    /// frequency, which casa-rs does not represent.
    fn rest_frequencies(&mut self) -> Result<Value, TcleanRefusal> {
        let value = self.value("restfreq")?;
        let items = match value {
            Value::Array(items) => items.iter().collect(),
            value => vec![value],
        };
        let texts = items
            .into_iter()
            .map(|item| {
                item.as_str()
                    .map(str::trim)
                    .ok_or_else(|| Self::mismatch("restfreq", "text", value))
            })
            .collect::<Result<Vec<_>, _>>()?;
        match texts.as_slice() {
            [] | [""] => Ok(json!("none")),
            texts if texts.contains(&"") => Err(TcleanRefusal::not_yet(
                "restfreq",
                "a list of rest frequencies with a blank entry",
                980,
            )),
            texts => Ok(json!(texts)),
        }
    }

    fn gridding(&mut self, modes: &Modes) -> Result<(), TcleanRefusal> {
        self.copy(&["pblimit"])?;
        self.ignore(&["computepastep", "rotatepastep"])?;
        let usepointing = self.flag("usepointing")?;
        if matches!(modes.gridder, "mosaic" | "awproject") {
            self.set("usepointing", usepointing);
        }
        let facets = self.integer("facets")?;
        match modes.gridder {
            "standard" => self.set("facets", facets),
            "mosaic" if facets != 1 => {
                return Err(TcleanRefusal::new(
                    "facets",
                    "the combination of mosaic gridding with multiple facets is not supported, \
                     as CASA's tclean refuses it",
                ));
            }
            "wproject" | "awproject" if facets != 1 => {
                return Err(TcleanRefusal::not_yet(
                    "facets",
                    format!("{facets} with gridder='{}'", modes.gridder),
                    980,
                ));
            }
            _ => {}
        }
        let planes = self.integer("wprojplanes")?;
        match (modes.gridder, planes) {
            ("wproject" | "awproject", -1) => self.set("wprojplanes", "none"),
            ("wproject" | "awproject", n) if n >= 1 => self.set("wprojplanes", n),
            ("wproject" | "awproject", n) => {
                return Err(TcleanRefusal::new(
                    "wprojplanes",
                    format!("wprojplanes={n}: give -1 to derive the planes, or a positive count"),
                ));
            }
            _ => {}
        }
        let normtype = self.text("normtype")?;
        if matches!(modes.gridder, "mosaic" | "awproject") {
            if normtype == "pbsquare" {
                return Err(TcleanRefusal::not_yet("normtype", "'pbsquare'", 823));
            }
            self.set("normtype", normtype);
        }
        // CASA weighs density by field only for the gridder named mosaic
        // (`task_tclean.py`); casa-rs's mosaic weighs it over all fields,
        // as CASA does for mosaicft and ftmosaic.
        if self.flag("mosweight")? && modes.per_field_density && modes.density_weighting {
            return Err(TcleanRefusal::not_yet(
                "mosweight",
                "True with density weighting (casa-rs weighs density over all fields)",
                818,
            ));
        }
        self.aw_terms(modes)?;
        let cfcache = self.text("cfcache")?;
        let pointing = self.number_list("pointingoffsetsigdev")?;
        if modes.gridder == "awproject" {
            self.set(
                "pointingoffsetsigdev",
                if pointing.is_empty() {
                    "none".to_string()
                } else {
                    pointing
                },
            );
            if self.values.get("aw_cf_source").and_then(Value::as_str) == Some("casa-import") {
                if cfcache.is_empty() {
                    return Err(TcleanRefusal::new(
                        "cfcache",
                        "awproject with aw_cf_source='casa-import' reads an existing CASA \
                         convolution-function cache: name it with cfcache",
                    ));
                }
                self.set("cfcache", cfcache);
            }
        }
        self.copy_present(&[
            "aw_cf_source",
            "cf_resident_mb",
            "native_cf_cache",
            "evla_surface",
            "native_cf_policy",
            "native_cf_working_size",
            "native_cf_oversampling",
            "native_cf_cache_bytes",
            "native_cf_maximum_cells",
        ])
    }

    /// casa-rs's AW projection runs as CASA's `aterm=True, psterm=False,
    /// wbawp=True, conjbeams=True`, and its mosaic as `conjbeams=False`.
    fn aw_terms(&mut self, modes: &Modes) -> Result<(), TcleanRefusal> {
        let fixed: &[(&str, bool)] = match modes.gridder {
            "awproject" => &[
                ("aterm", true),
                ("psterm", false),
                ("wbawp", true),
                ("conjbeams", true),
            ],
            "mosaic" => &[("conjbeams", false)],
            _ => &[],
        };
        for name in ["aterm", "psterm", "wbawp", "conjbeams"] {
            let value = self.flag(name)?;
            if let Some((_, required)) = fixed.iter().find(|(fixed, _)| *fixed == name)
                && value != *required
            {
                let given = if value { "True" } else { "False" };
                return Err(TcleanRefusal::not_yet(
                    name,
                    format!("{given} with gridder='{}'", modes.gridder),
                    980,
                ));
            }
        }
        Ok(())
    }

    /// `npixels` and `noise` as `SynthesisImagerVi2::weight` reads them for
    /// the weighting (`task_tclean.py`'s `ImagerParameters` maps
    /// `briggsabs` to Briggs with `rmode = "abs"`, the only mode that reads
    /// `noise`).
    fn weighting(&mut self, modes: &Modes) -> Result<(), TcleanRefusal> {
        let npixels = self.integer("npixels")?;
        let noise = self.value("noise")?;
        let cells = match modes.weighting.as_str() {
            // `VisImagingWeight("natural")` and `("radial")` grid no density.
            "natural" | "radial" => 0,
            // `actualNpix = npixels`, or 3 when `npixels <= 0`.
            "superuniform" if npixels <= 0 => 3,
            // `task_tclean.py` returns before imaging.
            "briggsbwtaper" if npixels != 0 => {
                return Err(TcleanRefusal::new(
                    "npixels",
                    "weighting='briggsbwtaper' takes npixels=0: CASA's tclean images nothing \
                     otherwise, and casa-rs refuses it",
                ));
            }
            _ if npixels < 0 => {
                return Err(TcleanRefusal::new(
                    "npixels",
                    format!(
                        "npixels={npixels}: a uniform or Briggs density grid needs 0 (the \
                         image's cells) or a positive size; CASA's of negative size fails"
                    ),
                ));
            }
            _ => npixels,
        };
        self.set("npixels", cells);
        if modes.weighting != "briggsabs" {
            self.set("noise", "none");
            return Ok(());
        }
        // `noise.get("Jy")`: a flux density, which a bare number is not.
        let jy = noise
            .as_str()
            .and_then(|text| text.trim().parse::<Quantity>().ok())
            .filter(|quantity| !quantity.unit().name().is_empty())
            .and_then(|quantity| quantity.get_value_in(&Unit::from_str("Jy").ok()?).ok())
            .filter(|jy| jy.is_finite())
            .ok_or_else(|| {
                TcleanRefusal::new(
                    "noise",
                    format!("noise {noise}: weighting='briggsabs' needs a flux density, as 1.0Jy"),
                )
            })?;
        // CASA's denominator robust²·D + 2·noise² would vanish:
        // `SynthesisImagerVi2::weight` refuses this pair for a cube, and a
        // continuum image weighs every visibility infinite.
        if jy == 0.0 && self.values.get("robust").and_then(Value::as_f64) == Some(0.0) {
            return Err(TcleanRefusal::new(
                "noise",
                "weighting='briggsabs' with robust=0 and noise=0 divides by zero: CASA refuses \
                 it for a cube, and casa-rs in every mode",
            ));
        }
        self.set("noise", jy);
        Ok(())
    }

    fn deconvolution(&mut self, modes: &Modes) -> Result<(), TcleanRefusal> {
        self.copy(&[
            "smallscalebias",
            "pbcor",
            "robust",
            "uvtaper",
            "restoration",
        ])?;
        // `asp` reads these, and casa-rs refuses `asp`.
        self.ignore(&["fusedthreshold", "largestscale"])?;
        let scales = self.number_list("scales")?;
        let multiscale = matches!(modes.deconvolver.as_str(), "multiscale" | "mtmfs");
        self.set(
            "scales",
            if multiscale && !scales.is_empty() {
                scales
            } else {
                "none".to_string()
            },
        );
        let nterms = self.integer("nterms")?;
        if modes.deconvolver == "mtmfs" && nterms == 1 {
            return Err(TcleanRefusal::not_yet(
                "nterms",
                "1 with deconvolver='mtmfs'",
                980,
            ));
        }
        self.set(
            "nterms",
            if modes.deconvolver == "mtmfs" {
                nterms
            } else {
                1
            },
        );
        let beam = self.restoring_beam()?;
        self.set("restoringbeam", beam);
        Ok(())
    }

    /// CASA's `restoringbeam` (`SynthesisParamsDeconv::fromRecord`), one
    /// text or a list: `''` or `[]` restores each plane with its fitted
    /// beam and `'common'` with one beam enclosing them; one, two or three
    /// entries are a beam's `[major, minor, angle]`, a circle of the one
    /// axis or an angle of zero when not given. CASA reads any other number
    /// of entries as no beam, and restores with the fitted beams.
    fn restoring_beam(&mut self) -> Result<Value, TcleanRefusal> {
        let value = self.value("restoringbeam")?;
        let items = match value {
            Value::Array(items) => items.iter().collect(),
            value => vec![value],
        };
        let texts = items
            .into_iter()
            .map(|item| {
                item.as_str()
                    .map(str::trim)
                    .ok_or_else(|| Self::mismatch("restoringbeam", "text", value))
            })
            .collect::<Result<Vec<_>, _>>()?;
        Ok(match texts.as_slice() {
            [] | [""] => json!("none"),
            ["common"] => json!("common"),
            [axis] => json!([axis, axis, "0deg"]),
            [major, minor] => json!([major, minor, "0deg"]),
            [major, minor, angle] => json!([major, minor, angle]),
            _ => json!("none"),
        })
    }

    fn iteration(&mut self) -> Result<(), TcleanRefusal> {
        self.copy(&[
            "niter",
            "gain",
            "threshold",
            "nsigma",
            "cyclefactor",
            "minpsffraction",
            "maxpsffraction",
            "nmajor",
            "psfcutoff",
            "parallel",
            "savemodel",
        ])?;
        self.ignore(&[
            "interactive",
            "fullsummary",
            "restart",
            "calcres",
            "calcpsf",
        ])?;
        if self.values.get("savemodel").and_then(Value::as_str) == Some("virtual") {
            return Err(TcleanRefusal::not_yet("savemodel", "'virtual'", 823));
        }
        let niter = self
            .values
            .get("niter")
            .and_then(Value::as_i64)
            .unwrap_or(0);
        // CASA's `checkAndFixIterationPars`: a cycleniter of zero or less,
        // or more than niter, is niter.
        let length = match self.integer("cycleniter")? {
            n if n <= 0 || n > niter => niter.max(1),
            n => n,
        };
        self.set("minor_cycle_length", length);
        Ok(())
    }

    fn masking(&mut self, modes: &Modes) -> Result<(), TcleanRefusal> {
        self.copy(&[
            "sidelobethreshold",
            "noisethreshold",
            "lownoisethreshold",
            "negativethreshold",
            "minbeamfrac",
            "growiterations",
            "cutthreshold",
            "dogrowprune",
            "minpercentchange",
            "verbose",
            "fastnoise",
        ])?;
        // CASA's automatic mask smooths by smoothfactor beams and fails on a
        // beam of no positive width (`GaussianBeam::setMajorMinor`).
        let smoothfactor = self.value("smoothfactor")?.clone();
        let cleans = self
            .values
            .get("niter")
            .and_then(Value::as_i64)
            .unwrap_or(0)
            > 0;
        if !modes.user_mask && cleans && smoothfactor.as_f64().is_some_and(|factor| factor <= 0.0) {
            return Err(TcleanRefusal::new(
                "smoothfactor",
                "CASA's auto-multithresh smooths its mask by a beam of positive width: \
                 smoothfactor must be positive, as CASA's tclean requires",
            ));
        }
        self.set("smoothfactor", smoothfactor);
        self.ignore(&["pbmask"])?;
        let entries = self.texts("mask")?;
        // CASA reads `mask` with usemask='user' only.
        let (boxes, image) = if modes.user_mask {
            user_mask(&entries)?
        } else {
            (Vec::new(), None)
        };
        self.set(
            "explicit_empty_mask",
            modes.user_mask && !entries.is_empty() && boxes.is_empty() && image.is_none(),
        );
        self.set(
            "mask_box",
            if boxes.is_empty() {
                "none".to_string()
            } else {
                boxes.join(";")
            },
        );
        self.set("mask_image", image.unwrap_or("none"));
        Ok(())
    }

    fn extras(&mut self) -> Result<(), TcleanRefusal> {
        self.copy(&[
            "ddid",
            "hogbom_iteration_mode",
            "write_pb",
            "fitspw",
            "fitorder",
            "save_continuum_residual",
            "backend",
            "gridprecision",
            "stage_timing",
            "metal_stage_timing",
            "science_trace",
        ])
    }
}

/// An image's spectral frame, in casacore's spelling.
fn output_frame(text: &str) -> Result<String, TcleanRefusal> {
    let frame = text.to_ascii_uppercase();
    match frame.as_str() {
        "LSRK" | "BARY" | "TOPO" | "REST" => Ok(frame),
        _ => Err(TcleanRefusal::not_yet("outframe", format!("{text:?}"), 980)),
    }
}

/// A cube's velocity convention, in casacore's spelling: `z` is optical and
/// `relativistic` (or `true`) is `beta`, as in `MDoppler`.
fn velocity_convention(text: &str) -> Result<&'static str, TcleanRefusal> {
    match text.to_ascii_lowercase().as_str() {
        "radio" => Ok("RADIO"),
        "optical" | "z" => Ok("OPTICAL"),
        "beta" | "relativistic" | "true" => Ok("BETA"),
        _ => Err(TcleanRefusal::not_yet("veltype", format!("{text:?}"), 980)),
    }
}

/// tclean's user mask: CRTF pixel boxes (`box[[10pix,10pix],[20pix,20pix]]`,
/// inclusive), in the text or in a region file, or casa-rs's `x0,y0,x1,y1`
/// boxes, as `x0,y0,x1,y1` texts, or one mask image. The items are read as
/// one text, so that a command line's comma-separated list (`--mask
/// box[[1pix,1pix],[4pix,4pix]]`) reads as the regions it names. A mask
/// image is a directory, as every CASA image is; a file is a region file.
fn user_mask<'a>(entries: &[&'a str]) -> Result<(Vec<String>, Option<&'a str>), TcleanRefusal> {
    let text = entries.join(",");
    if text.contains('[') {
        return Ok((crtf_pixel_boxes(&text)?, None));
    }
    if let Some(boxes) = pixel_box_list(&text) {
        return Ok((boxes, None));
    }
    match entries {
        [] => Ok((Vec::new(), None)),
        [region] if std::path::Path::new(region).is_file() => {
            let text = std::fs::read_to_string(region).map_err(|error| {
                TcleanRefusal::new("mask", format!("read the region file {region:?}: {error}"))
            })?;
            Ok((crtf_pixel_boxes(&text)?, None))
        }
        [image] => Ok((Vec::new(), Some(image))),
        _ => Err(TcleanRefusal::not_yet(
            "mask",
            "more than one mask image",
            980,
        )),
    }
}

/// casa-rs's `x0,y0,x1,y1` boxes separated by `;`, if `text` is that.
fn pixel_box_list(text: &str) -> Option<Vec<String>> {
    text.split(';')
        .map(|corners| {
            let corners = corners
                .split(',')
                .map(|corner| corner.trim().parse::<i64>().ok())
                .collect::<Option<Vec<_>>>()?;
            let [x0, y0, x1, y1] = corners.as_slice() else {
                return None;
            };
            Some(format!("{x0},{y0},{x1},{y1}"))
        })
        .collect()
}

/// The `box[[Xpix, Ypix], [Xpix, Ypix]]` regions of one CRTF text, as
/// `x0,y0,x1,y1` with the lower corner first; a `#` line is a comment, and
/// any other region is refused.
fn crtf_pixel_boxes(text: &str) -> Result<Vec<String>, TcleanRefusal> {
    let refused = || {
        TcleanRefusal::not_yet(
            "mask",
            format!("the region {text:?} (casa-rs takes pixel boxes)"),
            980,
        )
    };
    let pixel = |corner: &str| {
        corner
            .trim()
            .strip_suffix("pix")
            .and_then(|number| number.trim().parse::<f64>().ok())
            .filter(|number| number.fract() == 0.0)
            .ok_or_else(refused)
    };
    let mut boxes = Vec::new();
    for line in text
        .lines()
        .map(str::trim)
        .filter(|line| !line.starts_with('#'))
    {
        let mut rest = line;
        while !rest.is_empty() {
            let body = rest
                .strip_prefix("box")
                .map(str::trim_start)
                .and_then(|body| body.strip_prefix("[["))
                .ok_or_else(refused)?;
            let (inside, after) = body.split_once("]]").ok_or_else(refused)?;
            let corners = inside
                .split(',')
                .map(|corner| pixel(corner.trim_matches(['[', ']', ' '])))
                .collect::<Result<Vec<_>, _>>()?;
            let [xa, ya, xb, yb] = corners.as_slice() else {
                return Err(refused());
            };
            boxes.push(format!(
                "{},{},{},{}",
                xa.min(*xb),
                ya.min(*yb),
                xa.max(*xb),
                ya.max(*yb)
            ));
            rest = after.trim_start_matches([',', ' ', '\t']);
        }
    }
    Ok(boxes)
}

#[cfg(test)]
#[path = "tclean_tests.rs"]
mod tests;

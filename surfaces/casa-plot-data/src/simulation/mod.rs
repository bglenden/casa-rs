// SPDX-License-Identifier: LGPL-3.0-or-later
//! Simulation plot-document builders and `SimulationPlotSink` implementation.

mod document;
mod ephemeris;
mod image;
mod model;
mod observation;
mod simalma;
mod simanalyze;
mod simutil;

use std::path::{Path, PathBuf};

use casa_imaging_runtime::charge_process_caches;
use casa_simulation::{
    SimalmaFinishInput, SimanalyzePlotInput, SimobservePlotInput, SimulationPlotSink,
    simalma::{SimalmaRequest, simalma_with_plot_sink},
    simanalyze::{SimanalyzeOutcome, SimanalyzeRequest, simanalyze_with_plot_sink},
    simobserve::{
        SimobserveError, SimobserveOutcome, SimobserveRequest, simobserve_with_plot_sink,
    },
};

use crate::plot_document::{PlotDocument, PlotFigure, PlotPayloadWriter};

pub use simutil::{
    EphemerisPlotRequest, ImagePlotOptions, antenna_plot, ephemeris_plot, image_plot,
};

fn failure(error: impl std::fmt::Display) -> SimobserveError {
    SimobserveError::Raised(format!("simobserve graphics: {error}"))
}

/// [`SimulationPlotSink`] assembling `PlotDocument` figures into a shared
/// binary payload file.
pub struct PlotDocumentSink {
    payload_path: PathBuf,
    initialized: bool,
    figures: Vec<PlotFigure>,
}

impl PlotDocumentSink {
    /// Create a plot sink that writes array payloads into `payload_path`.
    #[must_use]
    pub fn new(payload_path: &Path) -> Self {
        Self {
            payload_path: payload_path.to_path_buf(),
            initialized: false,
            figures: Vec::new(),
        }
    }

    /// Consume the sink and return the assembled figures.
    #[must_use]
    pub fn into_figures(self) -> Vec<PlotFigure> {
        self.figures
    }

    fn open_writer(&mut self) -> Result<PlotPayloadWriter, SimobserveError> {
        let writer = if self.initialized {
            PlotPayloadWriter::append(&self.payload_path)
        } else {
            self.initialized = true;
            PlotPayloadWriter::open(&self.payload_path)
        };
        writer.map_err(failure)
    }

    fn record_valid_figures(
        &mut self,
        figures: Vec<PlotFigure>,
        bytes: u64,
    ) -> Result<(), SimobserveError> {
        for plot in &figures {
            plot.document.validate_payload(bytes).map_err(failure)?;
        }
        self.figures.extend(figures);
        Ok(())
    }
}

impl SimulationPlotSink for PlotDocumentSink {
    fn record_simobserve(&mut self, input: SimobservePlotInput<'_>) -> Result<(), SimobserveError> {
        let resources = input.resources;
        charge_process_caches(resources.host(), resources.policy()).map_err(failure)?;
        let mut payload = self.open_writer()?;
        let mut plots = Vec::new();
        let model = model::prepare(&input, &mut payload)?;
        plots.push(simobserve_figure(
            &input,
            ".skymodel.png",
            document::document("skymodel", vec![model], 1),
        ));
        if input.predict {
            let mut panels = vec![ephemeris::prepare(&input, &mut payload)?];
            if input.uvmode {
                panels.push(observation::antennas(&input, &mut payload)?);
                panels.push(observation::uv(&input, &mut payload)?);
                panels.push(image::image_panel(
                    &input.quick_psf,
                    &mut payload,
                    resources,
                    true,
                )?);
            }
            let columns = if input.uvmode { 2 } else { 1 };
            plots.push(simobserve_figure(
                &input,
                ".observe.png",
                document::document("observe", panels, columns),
            ));
        }
        let bytes = payload.finish().map_err(failure)?;
        self.record_valid_figures(plots, bytes)
    }

    fn record_simanalyze(&mut self, input: SimanalyzePlotInput<'_>) -> Result<(), SimobserveError> {
        let mut payload = self.open_writer()?;
        let plots = simanalyze::prepare(&input, &mut payload)?;
        let bytes = payload.finish().map_err(failure)?;
        self.record_valid_figures(plots, bytes)
    }

    fn finish_simalma(&mut self, input: SimalmaFinishInput<'_>) -> Result<(), SimobserveError> {
        let path = self.payload_path.clone();
        let initialized = &mut self.initialized;
        simalma::finish(
            &input,
            &path,
            || {
                let writer = if *initialized {
                    PlotPayloadWriter::append(&path)
                } else {
                    *initialized = true;
                    PlotPayloadWriter::open(&path)
                };
                writer.map_err(failure)
            },
            &mut self.figures,
        )
    }
}

fn simobserve_figure(
    input: &SimobservePlotInput<'_>,
    suffix: &str,
    document: PlotDocument,
) -> PlotFigure {
    let mode = input.graphics_mode;
    let file = input.directory.join(format!("{}{suffix}", input.stem));
    PlotFigure {
        output_file: matches!(mode, "file" | "both").then_some(file),
        show: matches!(mode, "screen" | "both"),
        document,
    }
}

/// Run [`casa_simulation::simobserve`] and prepare native plot figures in `payload`.
///
/// # Errors
/// Simulation failures, an unwritable payload file, or plot memory refusal.
pub fn simobserve_with_plots(
    request: &SimobserveRequest,
    payload: &Path,
) -> Result<(SimobserveOutcome, Vec<PlotFigure>), SimobserveError> {
    let mut sink = PlotDocumentSink::new(payload);
    let outcome = simobserve_with_plot_sink(request, &mut sink)?;
    Ok((outcome, sink.into_figures()))
}

/// Run [`casa_simulation::simanalyze`] and prepare native plot figures in `payload`.
///
/// # Errors
/// Simulation failures, an unwritable payload file, or plot memory refusal.
pub fn simanalyze_with_plots(
    request: &SimanalyzeRequest,
    payload: &Path,
) -> Result<(SimanalyzeOutcome, Vec<PlotFigure>), SimobserveError> {
    let mut sink = PlotDocumentSink::new(payload);
    let outcome = simanalyze_with_plot_sink(request, &mut sink)?;
    Ok((outcome, sink.into_figures()))
}

/// Run [`casa_simulation::simalma`] and prepare native plot figures in `payload`.
///
/// # Errors
/// Simulation failures, an unwritable payload file, or plot memory refusal.
pub fn simalma_with_plots(
    request: &SimalmaRequest,
    payload: &Path,
) -> Result<(SimobserveOutcome, Vec<PlotFigure>), SimobserveError> {
    let mut sink = PlotDocumentSink::new(payload);
    let outcome = simalma_with_plot_sink(request, &mut sink)?;
    Ok((outcome, sink.into_figures()))
}

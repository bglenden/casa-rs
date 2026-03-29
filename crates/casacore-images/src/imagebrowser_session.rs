// SPDX-License-Identifier: LGPL-3.0-or-later
//! Long-lived image browser session state.

use std::path::Path;

use casacore_imagebrowser_protocol::{
    ImageBrowserAxisValue, ImageBrowserCapabilities, ImageBrowserCommand, ImageBrowserFocus,
    ImageBrowserParameters, ImageBrowserProbe, ImageBrowserSnapshot, ImageBrowserView,
    ImageBrowserViewport, ImageDisplayAxisState, ImageNavigationMetrics, ImageNonDisplayAxisState,
    ImagePlaneCursorState, ImagePlaneRaster, ImageProfilePayload, ImageProfileSampleState,
};
use casacore_types::measures::direction::{
    format_declination_labeled, format_right_ascension_labeled,
};

use crate::error::ImageError;
use crate::image_view::{
    PlaneAutoscaleMode, PlaneStretchPreset, PlaneStretchSettings, format_numeric_value_with_unit,
};
use crate::{
    ImageAxisValue, ImageDisplayAxis, ImageMetadataSection, ImageNonDisplayAxis, ImageProbe,
    ImageProfile, ImageProfileSample, ImageViewCapabilities, ImageViewWindow, OpenedImageView,
    PlaneRaster,
};

/// Long-lived read-only image browser session.
#[derive(Debug)]
pub struct ImageBrowserSession {
    view: OpenedImageView,
    window: ImageViewWindow,
    stretch: SessionStretchState,
    frozen_clip_bounds: Option<(f64, f64)>,
    active_view: ImageBrowserView,
    focus: ImageBrowserFocus,
    viewport: ImageBrowserViewport,
    cursor_x: usize,
    cursor_y: usize,
    non_display_indices: Vec<usize>,
    selected_profile_axis: Option<usize>,
    content_offset: usize,
}

#[derive(Debug, Clone, PartialEq)]
struct SessionStretchState {
    preset: PlaneStretchPreset,
    autoscale: PlaneAutoscaleMode,
    manual_clip: Option<(f64, f64)>,
}

impl Default for SessionStretchState {
    fn default() -> Self {
        Self {
            preset: PlaneStretchPreset::Percentile99,
            autoscale: PlaneAutoscaleMode::PerPlane,
            manual_clip: None,
        }
    }
}

impl SessionStretchState {
    fn plane_settings(&self) -> PlaneStretchSettings {
        PlaneStretchSettings {
            preset: self.preset,
            autoscale: self.autoscale,
            manual_clip: self.manual_clip,
        }
    }
}

impl ImageBrowserSession {
    /// Opens a new browser session rooted at the provided image path.
    pub fn open(
        path: impl AsRef<Path>,
        viewport: ImageBrowserViewport,
    ) -> Result<Self, ImageError> {
        Self::open_with_parameters(path, viewport, None)
    }

    /// Opens a new browser session rooted at the provided image path and parameters.
    pub fn open_with_parameters(
        path: impl AsRef<Path>,
        viewport: ImageBrowserViewport,
        parameters: Option<&ImageBrowserParameters>,
    ) -> Result<Self, ImageError> {
        let view = OpenedImageView::open(path)?;
        let non_display_axis_count = session_non_display_axis_count(&view);
        let stretch = parameters
            .map(parse_stretch_parameters)
            .transpose()?
            .unwrap_or_default();
        let window = match parameters {
            Some(parameters) => {
                view.window_from_text(&parameters.blc, &parameters.trc, &parameters.inc)?
            }
            None => view.default_window(),
        };
        let default_display_pixels = centered_display_pixels(&view, &window);
        let default_non_display_pixels = centered_non_display_pixels(&view, &window);
        let active_view = if view.capabilities().renderable_plane {
            ImageBrowserView::Plane
        } else {
            ImageBrowserView::Metadata
        };
        let mut session = Self {
            view,
            window,
            stretch,
            frozen_clip_bounds: None,
            active_view,
            focus: ImageBrowserFocus::Content,
            viewport,
            cursor_x: 0,
            cursor_y: 0,
            non_display_indices: vec![0; non_display_axis_count],
            selected_profile_axis: None,
            content_offset: 0,
        };
        session.selected_profile_axis = session.view.preferred_profile_axis();
        session.clamp_cursor_to_window(
            None,
            None,
            default_display_pixels,
            Some(default_non_display_pixels),
        );
        Ok(session)
    }

    /// Replaces the current root image.
    pub fn reopen(
        &mut self,
        path: impl AsRef<Path>,
        viewport: ImageBrowserViewport,
        parameters: Option<&ImageBrowserParameters>,
    ) -> Result<ImageBrowserSnapshot, ImageError> {
        *self = Self::open_with_parameters(path, viewport, parameters)?;
        self.snapshot()
    }

    /// Applies a session command and returns the updated snapshot.
    pub fn handle_command(
        &mut self,
        command: ImageBrowserCommand,
    ) -> Result<ImageBrowserSnapshot, ImageError> {
        match command {
            ImageBrowserCommand::OpenRoot {
                path,
                viewport,
                parameters,
            } => self.reopen(path, viewport, parameters.as_ref()),
            ImageBrowserCommand::Resize { viewport } => {
                self.viewport = viewport;
                self.snapshot()
            }
            ImageBrowserCommand::CycleView { forward } => {
                self.active_view = cycle_view(self.available_views(), self.active_view, forward);
                self.content_offset = 0;
                self.snapshot()
            }
            ImageBrowserCommand::SetFocus { focus } => {
                self.focus = focus;
                self.snapshot()
            }
            ImageBrowserCommand::MoveCursor { dx, dy } => {
                self.move_cursor(dx, dy)?;
                self.snapshot()
            }
            ImageBrowserCommand::SetCursor { x, y } => {
                self.set_cursor_pixels(x, y);
                self.snapshot()
            }
            ImageBrowserCommand::StepNonDisplayAxis { axis, delta } => {
                self.step_non_display_axis(axis, delta);
                self.snapshot()
            }
            ImageBrowserCommand::SetSelectedNonDisplayAxis { axis } => {
                self.set_selected_profile_axis(axis);
                self.snapshot()
            }
            ImageBrowserCommand::SetViewWindow { parameters } => {
                self.set_view_window(&parameters)?;
                self.snapshot()
            }
            ImageBrowserCommand::GetSnapshot => self.snapshot(),
        }
    }

    /// Returns the current snapshot without changing state.
    pub fn snapshot(&mut self) -> Result<ImageBrowserSnapshot, ImageError> {
        let plane_raster = if self.active_view == ImageBrowserView::Plane
            && self.view.capabilities().renderable_plane
        {
            let clip_override = match self.stretch.autoscale {
                PlaneAutoscaleMode::PerPlane => None,
                PlaneAutoscaleMode::Frozen => self.frozen_clip_bounds,
            };
            let raster = self.view.render_plane_with_window_and_axes_and_stretch(
                self.plane_pixel_viewport(),
                &self.window,
                &self.non_display_indices,
                &self.stretch.plane_settings(),
                clip_override,
            )?;
            if self.stretch.autoscale == PlaneAutoscaleMode::Frozen
                && self.frozen_clip_bounds.is_none()
                && !raster.no_finite_values
            {
                self.frozen_clip_bounds = Some((raster.clip_min, raster.clip_max));
            }
            Some(raster)
        } else {
            None
        };
        let mut inspector_lines = self.inspector_lines()?;
        inspector_lines.extend(self.plane_display_lines(plane_raster.as_ref()));
        let profile = if self.view.capabilities().renderable_plane
            && matches!(
                self.active_view,
                ImageBrowserView::Plane | ImageBrowserView::Spectrum
            ) {
            self.selected_profile_axis()
                .map(|profile_axis| {
                    self.view.profile_with_window_and_axes(
                        (self.cursor_x, self.cursor_y),
                        &self.window,
                        &self.non_display_indices,
                        profile_axis,
                    )
                })
                .transpose()?
        } else {
            None
        };
        let inspector_height = if self.viewport.inspector_height == 0 {
            inspector_lines.len().max(1)
        } else {
            self.viewport.inspector_height as usize
        };
        let content_height = self.viewport.height.max(1) as usize;
        let all_content_lines = self.all_content_lines(plane_raster.as_ref(), profile.as_ref())?;
        let total_items = all_content_lines.len();
        let max_offset = total_items.saturating_sub(content_height);
        let content_offset = self.content_offset.min(max_offset);
        let content_lines = clip_lines(&all_content_lines, content_offset, content_height);
        let plane = plane_raster.map(map_plane_raster);
        let display_axes = self
            .view
            .display_axes_with_window(&self.window)
            .into_iter()
            .map(map_display_axis)
            .collect::<Vec<_>>();
        let probe = if self.active_view == ImageBrowserView::Plane
            && self.view.capabilities().renderable_plane
        {
            Some(map_probe(self.view.probe_with_window_and_axes(
                (self.cursor_x, self.cursor_y),
                &self.window,
                &self.non_display_indices,
            )?))
        } else {
            None
        };
        let non_display_axes = self
            .view
            .non_display_axes_with_window(&self.window, &self.non_display_indices)?
            .into_iter()
            .map(map_non_display_axis)
            .collect::<Vec<_>>();

        let navigation = if self.active_view == ImageBrowserView::Plane
            && self.view.capabilities().renderable_plane
        {
            if let Some(display_axes) = self.view.axis_model().display_axes {
                ImageNavigationMetrics {
                    selected_index: self.cursor_y.min(
                        self.window
                            .sampled_axis_len(display_axes[1])
                            .saturating_sub(1),
                    ),
                    total_items: self.window.sampled_axis_len(display_axes[1]),
                    viewport_items: content_height,
                }
            } else {
                ImageNavigationMetrics {
                    selected_index: 0,
                    total_items: 0,
                    viewport_items: content_height,
                }
            }
        } else {
            ImageNavigationMetrics {
                selected_index: content_offset,
                total_items,
                viewport_items: content_height,
            }
        };

        Ok(ImageBrowserSnapshot {
            status_line: self.view.status_line(),
            active_view: self.active_view,
            focus: self.focus,
            shape: self.view.shape().to_vec(),
            parameters: self.parameter_state(),
            inspector_lines: clip_lines(&inspector_lines, 0, inspector_height),
            content_lines,
            navigation,
            plane,
            probe,
            profile: profile.as_ref().map(map_profile),
            display_axes,
            plane_cursor: self.current_plane_cursor(),
            non_display_axes,
            capabilities: map_capabilities(self.view.capabilities()),
        })
    }

    fn current_plane_cursor(&self) -> Option<ImagePlaneCursorState> {
        let display_axes = self.view.axis_model().display_axes?;
        Some(ImagePlaneCursorState {
            sampled_x: self.cursor_x,
            sampled_y: self.cursor_y,
            pixel_x: self
                .window
                .sampled_axis_value(display_axes[0], self.cursor_x)
                .unwrap_or(self.window.blc()[display_axes[0]]),
            pixel_y: self
                .window
                .sampled_axis_value(display_axes[1], self.cursor_y)
                .unwrap_or(self.window.blc()[display_axes[1]]),
        })
    }

    fn available_views(&self) -> &'static [ImageBrowserView] {
        if self.view.capabilities().renderable_plane && !self.non_display_indices.is_empty() {
            &[
                ImageBrowserView::Metadata,
                ImageBrowserView::Coordinates,
                ImageBrowserView::Plane,
                ImageBrowserView::Spectrum,
            ]
        } else if self.view.capabilities().renderable_plane {
            &[
                ImageBrowserView::Metadata,
                ImageBrowserView::Coordinates,
                ImageBrowserView::Plane,
            ]
        } else {
            &[ImageBrowserView::Metadata, ImageBrowserView::Coordinates]
        }
    }

    fn move_cursor(&mut self, dx: i32, dy: i32) -> Result<(), ImageError> {
        if self.active_view == ImageBrowserView::Plane
            && self.focus == ImageBrowserFocus::Content
            && self.view.capabilities().renderable_plane
        {
            let Some(display_axes) = self.view.axis_model().display_axes else {
                return Ok(());
            };
            let max_x = self
                .window
                .sampled_axis_len(display_axes[0])
                .saturating_sub(1) as i32;
            let max_y = self
                .window
                .sampled_axis_len(display_axes[1])
                .saturating_sub(1) as i32;
            self.cursor_x = (self.cursor_x as i32 + dx).clamp(0, max_x) as usize;
            self.cursor_y = (self.cursor_y as i32 + dy).clamp(0, max_y) as usize;
            return Ok(());
        }

        let content_height = self.viewport.height.max(1) as usize;
        let total_items = self.all_content_lines(None, None)?.len();
        let max_offset = total_items.saturating_sub(content_height) as i32;
        self.content_offset = (self.content_offset as i32 + dy).clamp(0, max_offset) as usize;
        Ok(())
    }

    fn step_non_display_axis(&mut self, axis: usize, delta: i32) {
        let Some(position) = self
            .view
            .axis_model()
            .non_display_axes
            .iter()
            .position(|candidate| *candidate == axis)
        else {
            return;
        };
        let length = self.window.sampled_axis_len(axis);
        if length == 0 {
            self.non_display_indices[position] = 0;
            return;
        }
        self.non_display_indices[position] = (self.non_display_indices[position] as i32 + delta)
            .clamp(0, length.saturating_sub(1) as i32)
            as usize;
    }

    fn inspector_lines(&self) -> Result<Vec<String>, ImageError> {
        let mut lines = vec![
            format!("View: {}", self.active_view.label()),
            format!(
                "Focus: {}",
                match self.focus {
                    ImageBrowserFocus::Inspector => "Inspector",
                    ImageBrowserFocus::Content => "Content",
                }
            ),
            format!("Status: {}", self.view.status_line()),
            format!("Shape: {:?}", self.view.shape()),
            format!("Pixel type: {:?}", self.view.pixel_type()),
            format!("BLC: {}", self.window.format_blc()),
            format!("TRC: {}", self.window.format_trc()),
            format!("INC: {}", self.window.format_inc()),
        ];
        if self.active_view == ImageBrowserView::Spectrum
            && let Some(axis) = self.selected_profile_axis()
            && let Some(axis_state) = self
                .view
                .non_display_axes_with_window(&self.window, &self.non_display_indices)?
                .into_iter()
                .find(|candidate| candidate.axis == axis)
        {
            lines.push(format!(
                "Profile axis {} ({}): pixel {} [{}/{}]",
                axis_state.name,
                axis_state.axis,
                axis_state.pixel,
                axis_state.index,
                axis_state.length.saturating_sub(1)
            ));
            if let Some(display_axes) = self.view.axis_model().display_axes {
                let pixel_x = self
                    .window
                    .sampled_axis_value(display_axes[0], self.cursor_x)
                    .unwrap_or(self.window.blc()[display_axes[0]]);
                let pixel_y = self
                    .window
                    .sampled_axis_value(display_axes[1], self.cursor_y)
                    .unwrap_or(self.window.blc()[display_axes[1]]);
                lines.push(format!("Plane cursor: x={pixel_x} y={pixel_y}"));
            }
        }
        if self.active_view == ImageBrowserView::Plane && self.view.capabilities().renderable_plane
        {
            let probe = self.view.probe_with_window_and_axes(
                (self.cursor_x, self.cursor_y),
                &self.window,
                &self.non_display_indices,
            )?;
            let Some(display_axes) = self.view.axis_model().display_axes else {
                return Ok(lines);
            };
            lines.push(format!(
                "Cursor: x={} y={}",
                probe.pixel_indices[display_axes[0]], probe.pixel_indices[display_axes[1]]
            ));
            lines.push(format!(
                "Value: {}",
                format_probe_value(&probe, self.view.brightness_unit())
            ));
            if probe.masked {
                lines.push("Masked: true".into());
            }
            for axis in probe.world_axes {
                lines.push(format_world_axis_line(&axis));
            }
        }
        Ok(lines)
    }

    fn content_lines(&self) -> Result<Vec<String>, ImageError> {
        let mut sections = self.view.metadata_sections()?;
        let filtered = match self.active_view {
            ImageBrowserView::Metadata => {
                filter_sections(&sections, &["Summary", "Observation", "Axes", "Misc"])
            }
            ImageBrowserView::Coordinates => {
                if let Some(active_cursor) = self.active_cursor_section()? {
                    sections.insert(0, active_cursor);
                }
                filter_sections(&sections, &["Active Cursor", "Coordinates", "Axes"])
            }
            ImageBrowserView::Spectrum => Vec::new(),
            ImageBrowserView::Plane => Vec::new(),
        };
        Ok(flatten_sections(&filtered))
    }

    fn all_content_lines(
        &self,
        plane_raster: Option<&PlaneRaster>,
        profile: Option<&ImageProfile>,
    ) -> Result<Vec<String>, ImageError> {
        if self.active_view == ImageBrowserView::Spectrum {
            Ok(render_profile_lines(profile))
        } else if let Some(raster) = plane_raster {
            self.plane_content_lines(raster)
        } else {
            self.content_lines()
        }
    }

    fn plane_content_lines(&self, _raster: &PlaneRaster) -> Result<Vec<String>, ImageError> {
        self.view.render_plane_value_grid_with_window_and_axes(
            (
                self.viewport.width.max(1) as usize,
                self.viewport.height.max(1) as usize,
            ),
            &self.window,
            &self.non_display_indices,
            (self.cursor_x, self.cursor_y),
        )
    }

    fn selected_profile_axis(&self) -> Option<usize> {
        self.selected_profile_axis
            .filter(|axis| self.view.axis_model().non_display_axes.contains(axis))
            .or_else(|| self.view.preferred_profile_axis())
    }

    fn active_cursor_section(&self) -> Result<Option<ImageMetadataSection>, ImageError> {
        if !self.view.capabilities().renderable_plane {
            return Ok(None);
        }
        let probe = self.view.probe_with_window_and_axes(
            (self.cursor_x, self.cursor_y),
            &self.window,
            &self.non_display_indices,
        )?;
        let mut lines = vec![
            format!("pixel: {}", join_usize_list(&probe.pixel_indices)),
            format!(
                "value: {}",
                format_probe_value(&probe, self.view.brightness_unit())
            ),
        ];
        if probe.masked {
            lines.push("masked: true".into());
        }
        if !probe.finite {
            lines.push("finite: false".into());
        }
        for axis in &probe.world_axes {
            lines.push(format_world_axis_line(axis));
        }
        Ok(Some(ImageMetadataSection {
            title: "Active Cursor".into(),
            lines,
        }))
    }

    fn parameter_state(&self) -> ImageBrowserParameters {
        ImageBrowserParameters {
            blc: self.window.format_blc(),
            trc: self.window.format_trc(),
            inc: self.window.format_inc(),
            stretch: stretch_preset_name(self.stretch.preset).into(),
            autoscale: autoscale_mode_name(self.stretch.autoscale).into(),
            clip_low: self
                .stretch
                .manual_clip
                .map(|(low, _)| trim_float_text(format!("{low:.6}")))
                .unwrap_or_default(),
            clip_high: self
                .stretch
                .manual_clip
                .map(|(_, high)| trim_float_text(format!("{high:.6}")))
                .unwrap_or_default(),
        }
    }

    fn plane_pixel_viewport(&self) -> (usize, usize) {
        (
            usize::from(if self.viewport.plane_pixel_width > 0 {
                self.viewport.plane_pixel_width
            } else {
                self.viewport.width.max(1)
            }),
            usize::from(if self.viewport.plane_pixel_height > 0 {
                self.viewport.plane_pixel_height
            } else {
                self.viewport.height.max(1)
            }),
        )
    }

    fn set_view_window(&mut self, parameters: &ImageBrowserParameters) -> Result<(), ImageError> {
        let old_display_pixels = self.current_display_pixels();
        let old_non_display_pixels = self.current_non_display_pixels();
        self.stretch = parse_stretch_parameters(parameters)?;
        self.frozen_clip_bounds = None;
        let window =
            self.view
                .window_from_text(&parameters.blc, &parameters.trc, &parameters.inc)?;
        self.window = window;
        self.clamp_cursor_to_window(old_display_pixels, old_non_display_pixels, None, None);
        Ok(())
    }

    fn plane_display_lines(&self, raster: Option<&PlaneRaster>) -> Vec<String> {
        let mut lines = vec![
            format!("Stretch: {}", stretch_preset_label(self.stretch.preset)),
            format!(
                "Autoscale: {}",
                match self.stretch.autoscale {
                    PlaneAutoscaleMode::PerPlane => "per-plane",
                    PlaneAutoscaleMode::Frozen => "frozen",
                }
            ),
        ];
        if let Some((low, high)) = self.stretch.manual_clip {
            lines.push(format!(
                "Manual clip: {} .. {}",
                format_numeric_value_with_unit(low, self.view.brightness_unit()),
                format_numeric_value_with_unit(high, self.view.brightness_unit()),
            ));
        }
        if let Some(raster) = raster
            && !raster.no_finite_values
        {
            lines.push(format!(
                "Display clip: {} .. {}",
                format_numeric_value_with_unit(raster.clip_min, &raster.value_unit),
                format_numeric_value_with_unit(raster.clip_max, &raster.value_unit),
            ));
            lines.push(format!(
                "Plane range: {} .. {}",
                format_numeric_value_with_unit(raster.data_min, &raster.value_unit),
                format_numeric_value_with_unit(raster.data_max, &raster.value_unit),
            ));
        }
        lines
    }

    fn set_cursor_pixels(&mut self, x: usize, y: usize) {
        let Some(display_axes) = self.view.axis_model().display_axes else {
            return;
        };
        self.cursor_x = self.window.nearest_sample_index(display_axes[0], x);
        self.cursor_y = self.window.nearest_sample_index(display_axes[1], y);
    }

    fn set_selected_profile_axis(&mut self, axis: usize) {
        if self.view.axis_model().non_display_axes.contains(&axis) {
            self.selected_profile_axis = Some(axis);
        }
    }

    fn current_display_pixels(&self) -> Option<(usize, usize)> {
        let display_axes = self.view.axis_model().display_axes?;
        Some((
            self.window
                .sampled_axis_value(display_axes[0], self.cursor_x)
                .unwrap_or(self.window.blc()[display_axes[0]]),
            self.window
                .sampled_axis_value(display_axes[1], self.cursor_y)
                .unwrap_or(self.window.blc()[display_axes[1]]),
        ))
    }

    fn current_non_display_pixels(&self) -> Option<Vec<usize>> {
        self.view
            .axis_model()
            .non_display_axes
            .iter()
            .copied()
            .zip(self.non_display_indices.iter().copied())
            .map(|(axis, index)| self.window.sampled_axis_value(axis, index))
            .collect()
    }

    fn clamp_cursor_to_window(
        &mut self,
        old_display_pixels: Option<(usize, usize)>,
        old_non_display_pixels: Option<Vec<usize>>,
        default_display_pixels: Option<(usize, usize)>,
        default_non_display_pixels: Option<Vec<usize>>,
    ) {
        if let Some(display_axes) = self.view.axis_model().display_axes {
            let (old_x, old_y) = old_display_pixels.or(default_display_pixels).unwrap_or((
                self.window.blc()[display_axes[0]],
                self.window.blc()[display_axes[1]],
            ));
            self.cursor_x = self.window.nearest_sample_index(display_axes[0], old_x);
            self.cursor_y = self.window.nearest_sample_index(display_axes[1], old_y);
        } else {
            self.cursor_x = 0;
            self.cursor_y = 0;
        }
        let old_pixels = old_non_display_pixels.unwrap_or_default();
        let default_pixels = default_non_display_pixels.unwrap_or_default();
        self.non_display_indices = self
            .view
            .axis_model()
            .non_display_axes
            .iter()
            .enumerate()
            .map(|(position, &axis)| {
                let pixel = old_pixels
                    .get(position)
                    .copied()
                    .or_else(|| default_pixels.get(position).copied())
                    .unwrap_or(self.window.blc()[axis]);
                self.window.nearest_sample_index(axis, pixel)
            })
            .collect();
        if self.non_display_indices.len() < self.view.axis_model().non_display_axes.len() {
            self.non_display_indices
                .resize(self.view.axis_model().non_display_axes.len(), 0);
        }
    }
}

fn centered_display_pixels(
    view: &OpenedImageView,
    window: &ImageViewWindow,
) -> Option<(usize, usize)> {
    let display_axes = view.axis_model().display_axes?;
    Some((
        centered_sample_pixel(window, display_axes[0]),
        centered_sample_pixel(window, display_axes[1]),
    ))
}

fn centered_non_display_pixels(view: &OpenedImageView, window: &ImageViewWindow) -> Vec<usize> {
    view.axis_model()
        .non_display_axes
        .iter()
        .map(|&axis| centered_sample_pixel(window, axis))
        .collect()
}

fn centered_sample_pixel(window: &ImageViewWindow, axis: usize) -> usize {
    let center_index = window.sampled_axis_len(axis) / 2;
    window
        .sampled_axis_value(axis, center_index)
        .unwrap_or(window.blc()[axis])
}

fn cycle_view(
    available: &[ImageBrowserView],
    current: ImageBrowserView,
    forward: bool,
) -> ImageBrowserView {
    let index = available
        .iter()
        .position(|view| *view == current)
        .unwrap_or(0);
    let next = if forward {
        (index + 1) % available.len()
    } else if index == 0 {
        available.len() - 1
    } else {
        index - 1
    };
    available[next]
}

fn clip_lines(lines: &[String], offset: usize, height: usize) -> Vec<String> {
    lines
        .iter()
        .skip(offset)
        .take(height.max(1))
        .cloned()
        .collect()
}

fn filter_sections(
    sections: &[ImageMetadataSection],
    titles: &[&str],
) -> Vec<ImageMetadataSection> {
    sections
        .iter()
        .filter(|section| titles.contains(&section.title.as_str()))
        .cloned()
        .collect()
}

fn flatten_sections(sections: &[ImageMetadataSection]) -> Vec<String> {
    let mut lines = Vec::new();
    for (index, section) in sections.iter().enumerate() {
        if index > 0 {
            lines.push(String::new());
        }
        lines.push(format!("== {} ==", section.title));
        lines.extend(section.lines.clone());
    }
    lines
}

fn render_profile_lines(profile: Option<&ImageProfile>) -> Vec<String> {
    let Some(profile) = profile else {
        return vec!["No non-display axis available for spectrum/profile view.".into()];
    };
    let value_header = if profile.value_unit.is_empty() {
        "value".to_string()
    } else {
        format!("value [{}]", profile.value_unit)
    };
    let mut lines = vec![
        format!(
            "Profile axis: {} ({}) [{}]",
            profile.axis_name, profile.axis, profile.coord_type
        ),
        format!(
            "Selected sample: {}",
            format_profile_sample(
                profile.samples.get(profile.selected_sample_index),
                &profile.value_unit,
            )
        ),
        String::new(),
        format!("sel idx pixel coord {value_header}"),
    ];
    lines.extend(profile.samples.iter().map(|sample| {
        let marker = if sample.sample_index == profile.selected_sample_index {
            ">"
        } else {
            " "
        };
        format!(
            "{marker} {:>3} {:>5} {:<28} {}",
            sample.sample_index,
            sample.pixel_index,
            format_profile_world_value(sample),
            format_profile_value(sample, &profile.value_unit, true, false),
        )
    }));
    lines
}

fn format_profile_sample(sample: Option<&ImageProfileSample>, value_unit: &str) -> String {
    let Some(sample) = sample else {
        return "<none>".into();
    };
    format!(
        "idx={} pixel={} world={} value={}",
        sample.sample_index,
        sample.pixel_index,
        format_profile_world_value(sample),
        format_profile_value(sample, value_unit, true, true),
    )
}

fn format_profile_world_value(sample: &ImageProfileSample) -> String {
    sample
        .world_axis
        .as_ref()
        .map(|axis| format_world_axis_value(&axis.name, &axis.unit, axis.value))
        .unwrap_or_else(|| format!("pixel {}", sample.pixel_index))
}

fn format_profile_value(
    sample: &ImageProfileSample,
    value_unit: &str,
    scientific_preferred: bool,
    include_unit: bool,
) -> String {
    if sample.masked {
        "masked".into()
    } else {
        format_pixel_value(
            sample.value,
            sample.finite,
            value_unit,
            scientific_preferred,
            include_unit,
        )
    }
}

fn format_probe_value(probe: &ImageProbe, value_unit: &str) -> String {
    if probe.masked {
        "masked".into()
    } else {
        format_pixel_value(probe.value, probe.finite, value_unit, false, true)
    }
}

fn format_pixel_value(
    value: f64,
    finite: bool,
    value_unit: &str,
    scientific_preferred: bool,
    include_unit: bool,
) -> String {
    let text = if !finite {
        value.to_string()
    } else if scientific_preferred || (value != 0.0 && (value.abs() < 1e-3 || value.abs() >= 1e4)) {
        format!("{value:.6e}")
    } else {
        trim_float_text(format!("{value:.6}"))
    };
    if include_unit && !value_unit.is_empty() {
        format!("{text} {value_unit}")
    } else {
        text
    }
}

fn trim_float_text(mut text: String) -> String {
    while text.contains('.') && text.ends_with('0') {
        text.pop();
    }
    if text.ends_with('.') {
        text.pop();
    }
    if text == "-0" { "0".into() } else { text }
}

fn map_capabilities(capabilities: &ImageViewCapabilities) -> ImageBrowserCapabilities {
    ImageBrowserCapabilities {
        renderable_plane: capabilities.renderable_plane,
        world_coords_available: capabilities.world_coords_available,
        pixel_only_mode: capabilities.pixel_only_mode,
        non_display_axis_selectors: capabilities.non_display_axis_selectors,
        mask_present: capabilities.mask_present,
        complex_unsupported: capabilities.complex_unsupported,
    }
}

fn map_plane_raster(raster: PlaneRaster) -> ImagePlaneRaster {
    ImagePlaneRaster {
        width: raster.width,
        height: raster.height,
        pixels_u8: raster.pixels_u8,
        clip_min: raster.clip_min,
        clip_max: raster.clip_max,
        data_min: raster.data_min,
        data_max: raster.data_max,
        value_unit: raster.value_unit,
        histogram_bins: raster.histogram_bins,
        masked_or_non_finite_count: raster.masked_or_non_finite_count,
        no_finite_values: raster.no_finite_values,
    }
}

fn map_probe(probe: ImageProbe) -> ImageBrowserProbe {
    ImageBrowserProbe {
        pixel_indices: probe.pixel_indices,
        pixel_axes: probe.pixel_axes.into_iter().map(map_axis_value).collect(),
        value: probe.value,
        masked: probe.masked,
        finite: probe.finite,
        world_axes: probe.world_axes.into_iter().map(map_axis_value).collect(),
    }
}

fn map_profile(profile: &ImageProfile) -> ImageProfilePayload {
    ImageProfilePayload {
        axis: profile.axis,
        axis_name: profile.axis_name.clone(),
        axis_unit: profile.axis_unit.clone(),
        value_unit: profile.value_unit.clone(),
        coord_type: profile.coord_type.to_string(),
        selected_sample_index: profile.selected_sample_index,
        samples: profile.samples.iter().map(map_profile_sample).collect(),
    }
}

fn map_profile_sample(sample: &ImageProfileSample) -> ImageProfileSampleState {
    ImageProfileSampleState {
        sample_index: sample.sample_index,
        pixel_index: sample.pixel_index,
        value: sample.value,
        masked: sample.masked,
        finite: sample.finite,
        world_axis: sample.world_axis.clone().map(map_axis_value),
    }
}

fn map_axis_value(value: ImageAxisValue) -> ImageBrowserAxisValue {
    ImageBrowserAxisValue {
        name: value.name,
        unit: value.unit,
        value: value.value,
    }
}

fn map_display_axis(axis: ImageDisplayAxis) -> ImageDisplayAxisState {
    ImageDisplayAxisState {
        axis: axis.axis,
        name: axis.name,
        unit: axis.unit,
        blc: axis.blc,
        trc: axis.trc,
        inc: axis.inc,
        sampled_len: axis.sampled_len,
        world_increment: axis.world_increment,
    }
}

fn map_non_display_axis(axis: ImageNonDisplayAxis) -> ImageNonDisplayAxisState {
    ImageNonDisplayAxisState {
        axis: axis.axis,
        label: axis.name,
        index: axis.index,
        length: axis.length,
        pixel: axis.pixel,
    }
}

fn session_non_display_axis_count(view: &OpenedImageView) -> usize {
    view.axis_model().non_display_axes.len()
}

fn join_usize_list(values: &[usize]) -> String {
    values
        .iter()
        .map(|value| value.to_string())
        .collect::<Vec<_>>()
        .join(", ")
}

fn format_world_axis_line(axis: &ImageAxisValue) -> String {
    format!(
        "{}: {}",
        axis.name,
        format_world_axis_value(&axis.name, &axis.unit, axis.value)
    )
}

fn format_world_axis_value(axis_name: &str, unit: &str, value: f64) -> String {
    if axis_name.eq_ignore_ascii_case("Right Ascension") || axis_name.eq_ignore_ascii_case("RA") {
        return format_right_ascension_labeled(value, 6);
    }
    if axis_name.eq_ignore_ascii_case("Declination") || axis_name.eq_ignore_ascii_case("DEC") {
        return format_declination_labeled(value, 5);
    }
    if unit.is_empty() {
        format!("{value} unitless")
    } else {
        format_numeric_value_with_unit(value, unit)
    }
}

fn parse_stretch_parameters(
    parameters: &ImageBrowserParameters,
) -> Result<SessionStretchState, ImageError> {
    let preset = match parameters.stretch.trim() {
        "" | "percentile99" => PlaneStretchPreset::Percentile99,
        "percentile95" => PlaneStretchPreset::Percentile95,
        "minmax" => PlaneStretchPreset::MinMax,
        "zscale" => PlaneStretchPreset::ZScale,
        "manual" => PlaneStretchPreset::Manual,
        other => {
            return Err(ImageError::InvalidMetadata(format!(
                "unsupported stretch preset: {other}"
            )));
        }
    };
    let autoscale = match parameters.autoscale.trim() {
        "" | "per_plane" => PlaneAutoscaleMode::PerPlane,
        "frozen" => PlaneAutoscaleMode::Frozen,
        other => {
            return Err(ImageError::InvalidMetadata(format!(
                "unsupported autoscale mode: {other}"
            )));
        }
    };
    let clip_low = parse_optional_clip("clip_low", &parameters.clip_low)?;
    let clip_high = parse_optional_clip("clip_high", &parameters.clip_high)?;
    let manual_clip = match (clip_low, clip_high) {
        (Some(low), Some(high)) if low < high => Some((low, high)),
        (Some(_), Some(_)) => {
            return Err(ImageError::InvalidMetadata(
                "clip_low must be smaller than clip_high".into(),
            ));
        }
        (None, None) => None,
        _ => {
            return Err(ImageError::InvalidMetadata(
                "manual clip requires both clip_low and clip_high".into(),
            ));
        }
    };
    if preset == PlaneStretchPreset::Manual && manual_clip.is_none() {
        return Err(ImageError::InvalidMetadata(
            "manual stretch requires clip_low and clip_high".into(),
        ));
    }
    Ok(SessionStretchState {
        preset,
        autoscale,
        manual_clip,
    })
}

fn parse_optional_clip(field: &str, value: &str) -> Result<Option<f64>, ImageError> {
    let value = value.trim();
    if value.is_empty() {
        return Ok(None);
    }
    let parsed = value.parse::<f64>().map_err(|error| {
        ImageError::InvalidMetadata(format!("invalid {field} value '{value}': {error}"))
    })?;
    if !parsed.is_finite() {
        return Err(ImageError::InvalidMetadata(format!(
            "{field} must be finite"
        )));
    }
    Ok(Some(parsed))
}

fn stretch_preset_name(preset: PlaneStretchPreset) -> &'static str {
    match preset {
        PlaneStretchPreset::Percentile99 => "percentile99",
        PlaneStretchPreset::Percentile95 => "percentile95",
        PlaneStretchPreset::MinMax => "minmax",
        PlaneStretchPreset::ZScale => "zscale",
        PlaneStretchPreset::Manual => "manual",
    }
}

fn stretch_preset_label(preset: PlaneStretchPreset) -> &'static str {
    match preset {
        PlaneStretchPreset::Percentile99 => "percentile 1/99",
        PlaneStretchPreset::Percentile95 => "percentile 5/95",
        PlaneStretchPreset::MinMax => "min/max",
        PlaneStretchPreset::ZScale => "zscale-like",
        PlaneStretchPreset::Manual => "manual",
    }
}

fn autoscale_mode_name(mode: PlaneAutoscaleMode) -> &'static str {
    match mode {
        PlaneAutoscaleMode::PerPlane => "per_plane",
        PlaneAutoscaleMode::Frozen => "frozen",
    }
}

#[cfg(test)]
mod tests {
    use casacore_coordinates::{
        CoordinateSystem, DirectionCoordinate, ObsInfo, Projection, ProjectionType,
        SpectralCoordinate,
    };
    use casacore_imagebrowser_protocol::ImageBrowserCommand;
    use casacore_types::ArrayD;
    use casacore_types::measures::direction::DirectionRef;
    use casacore_types::measures::epoch::{EpochRef, MEpoch};
    use casacore_types::measures::frequency::FrequencyRef;
    use casacore_types::measures::position::MPosition;
    use ndarray::IxDyn;

    use super::*;
    use crate::image::PagedImage;

    fn cube_coords() -> CoordinateSystem {
        let mut coords = CoordinateSystem::new();
        coords.add_coordinate(Box::new(DirectionCoordinate::new(
            DirectionRef::J2000,
            Projection::new(ProjectionType::SIN),
            [0.0, std::f64::consts::FRAC_PI_4],
            [-1e-4, 1e-4],
            [1.0, 1.0],
        )));
        coords.add_coordinate(Box::new(SpectralCoordinate::new(
            FrequencyRef::LSRK,
            1.42e9,
            1.0e6,
            0.0,
            1.42040575e9,
        )));
        coords
    }

    fn cube_coords_with_obs_info() -> CoordinateSystem {
        cube_coords().with_obs_info(
            ObsInfo::new("ALMA")
                .with_observer("Test Observer")
                .with_date(MEpoch::from_mjd(59000.25, EpochRef::UTC))
                .with_telescope_position(MPosition::new_itrf(
                    2_225_142.18,
                    -5_440_307.37,
                    -2_481_029.85,
                ))
                .with_pointing_center(0.0, std::f64::consts::FRAC_PI_4),
        )
    }

    #[test]
    fn session_open_resize_and_snapshot() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("session.image");
        let mut image =
            PagedImage::<f32>::create(vec![4, 4], CoordinateSystem::new(), &path).unwrap();
        image
            .put_slice(
                &ArrayD::from_shape_vec(
                    IxDyn(&[4, 4]),
                    (0..16).map(|value| value as f32).collect(),
                )
                .unwrap(),
                &[0, 0],
            )
            .unwrap();
        image.save().unwrap();

        let mut session =
            ImageBrowserSession::open(&path, ImageBrowserViewport::new(4, 4)).unwrap();
        let snapshot = session.snapshot().unwrap();
        assert_eq!(snapshot.active_view, ImageBrowserView::Plane);
        assert!(snapshot.plane.is_some());
        assert_eq!(snapshot.plane_cursor.as_ref().unwrap().pixel_x, 2);
        assert_eq!(snapshot.plane_cursor.as_ref().unwrap().pixel_y, 2);
        assert_eq!(snapshot.probe.as_ref().unwrap().pixel_indices, vec![2, 2]);

        let plane = session.snapshot().unwrap();
        assert_eq!(plane.active_view, ImageBrowserView::Plane);
        assert!(plane.plane.is_some());

        let resized = session
            .handle_command(ImageBrowserCommand::Resize {
                viewport: ImageBrowserViewport::new(2, 2),
            })
            .unwrap();
        assert_eq!(resized.plane.as_ref().unwrap().width, 2);

        let high_res = session
            .handle_command(ImageBrowserCommand::Resize {
                viewport: ImageBrowserViewport::with_plane_pixels(2, 2, 0, 16, 16),
            })
            .unwrap();
        assert_eq!(high_res.plane.as_ref().unwrap().width, 4);
        assert_eq!(high_res.plane.as_ref().unwrap().height, 4);
    }

    #[test]
    fn session_cycles_views_and_scrolls_text() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("text.image");
        let mut image =
            PagedImage::<f32>::create(vec![2, 2], CoordinateSystem::new(), &path).unwrap();
        image.save().unwrap();

        let mut session =
            ImageBrowserSession::open(&path, ImageBrowserViewport::new(40, 3)).unwrap();
        session
            .handle_command(ImageBrowserCommand::CycleView { forward: true })
            .unwrap();
        let coordinates = session
            .handle_command(ImageBrowserCommand::CycleView { forward: true })
            .unwrap();
        assert_eq!(coordinates.active_view, ImageBrowserView::Coordinates);
        assert!(!coordinates.content_lines.is_empty());

        let moved = session
            .handle_command(ImageBrowserCommand::MoveCursor { dx: 0, dy: 1 })
            .unwrap();
        assert_eq!(moved.active_view, ImageBrowserView::Coordinates);
    }

    #[test]
    fn metadata_view_includes_observation_section() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("observation-session.image");
        let mut image =
            PagedImage::<f32>::create(vec![2, 2, 2], cube_coords_with_obs_info(), &path).unwrap();
        image.save().unwrap();

        let mut session =
            ImageBrowserSession::open(&path, ImageBrowserViewport::new(80, 24)).unwrap();
        session
            .handle_command(ImageBrowserCommand::CycleView { forward: true })
            .unwrap();
        let snapshot = session
            .handle_command(ImageBrowserCommand::CycleView { forward: true })
            .unwrap();

        assert!(
            snapshot
                .content_lines
                .iter()
                .any(|line| line == "== Observation ==")
        );
        assert!(snapshot.content_lines.iter().any(|line| {
            line.contains("pointing center: 00:00:00.000000 hms, +45.00.00.00000 dms")
        }));
    }

    #[test]
    fn session_spectrum_view_renders_profile_lines() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("spectrum.image");
        let mut image = PagedImage::<f32>::create(vec![2, 2, 3], cube_coords(), &path).unwrap();
        image
            .put_slice(
                &ArrayD::from_shape_vec(
                    IxDyn(&[2, 2, 3]),
                    vec![
                        1.0, 10.0, 100.0, 2.0, 20.0, 200.0, 3.0, 30.0, 300.0, 4.0, 40.0, 400.0,
                    ],
                )
                .unwrap(),
                &[0, 0, 0],
            )
            .unwrap();
        image.set_units("Jy/beam").unwrap();
        image.save().unwrap();

        let mut session =
            ImageBrowserSession::open(&path, ImageBrowserViewport::new(80, 12)).unwrap();
        session
            .handle_command(ImageBrowserCommand::MoveCursor { dx: 1, dy: 1 })
            .unwrap();
        let snapshot = session
            .handle_command(ImageBrowserCommand::CycleView { forward: true })
            .unwrap();

        assert_eq!(snapshot.active_view, ImageBrowserView::Spectrum);
        assert!(
            snapshot
                .content_lines
                .iter()
                .any(|line| line.contains("Profile axis: Frequency (2) [Spectral]"))
        );
        assert!(
            snapshot
                .content_lines
                .iter()
                .any(|line| line.contains("Selected sample: idx=1"))
        );
        assert!(
            snapshot
                .inspector_lines
                .iter()
                .any(|line| line.contains("Plane cursor: x=1 y=1"))
        );
        assert!(
            snapshot
                .content_lines
                .iter()
                .any(|line| line.contains("value [Jy/beam]"))
        );
        assert!(
            snapshot
                .content_lines
                .iter()
                .any(|line| line.contains("4.000000e2"))
        );
        assert!(snapshot.profile.is_some());
    }

    #[test]
    fn session_plane_view_snapshot_includes_profile_payload() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("linked-plane.image");
        let mut image = PagedImage::<f32>::create(vec![2, 2, 3], cube_coords(), &path).unwrap();
        image
            .put_slice(
                &ArrayD::from_shape_vec(
                    IxDyn(&[2, 2, 3]),
                    vec![
                        1.0, 10.0, 100.0, 2.0, 20.0, 200.0, 3.0, 30.0, 300.0, 4.0, 40.0, 400.0,
                    ],
                )
                .unwrap(),
                &[0, 0, 0],
            )
            .unwrap();
        image.set_units("Jy/beam").unwrap();
        image.save().unwrap();

        let mut session =
            ImageBrowserSession::open(&path, ImageBrowserViewport::new(80, 12)).unwrap();
        session
            .handle_command(ImageBrowserCommand::MoveCursor { dx: 1, dy: 1 })
            .unwrap();
        let snapshot = session.snapshot().unwrap();

        let profile = snapshot.profile.expect("profile payload");
        assert_eq!(snapshot.active_view, ImageBrowserView::Plane);
        assert_eq!(profile.axis_name, "Frequency");
        assert_eq!(profile.coord_type, "Spectral");
        assert_eq!(profile.value_unit, "Jy/beam");
        assert_eq!(profile.selected_sample_index, 1);
        assert_eq!(profile.samples.len(), 3);
        assert_eq!(profile.samples[2].pixel_index, 2);
        assert_eq!(profile.samples[2].value, 400.0);
    }

    #[test]
    fn frozen_autoscale_keeps_clip_bounds_across_plane_stepping() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("frozen-scale.image");
        let mut image = PagedImage::<f32>::create(vec![2, 2, 2], cube_coords(), &path).unwrap();
        image
            .put_slice(
                &ArrayD::from_shape_vec(
                    IxDyn(&[2, 2, 2]),
                    vec![1.0, 100.0, 2.0, 200.0, 3.0, 300.0, 4.0, 400.0],
                )
                .unwrap(),
                &[0, 0, 0],
            )
            .unwrap();
        image.set_units("Jy/beam").unwrap();
        image.save().unwrap();

        let mut session = ImageBrowserSession::open_with_parameters(
            &path,
            ImageBrowserViewport::new(32, 12),
            Some(&ImageBrowserParameters {
                blc: "0,0,0".into(),
                trc: "1,1,1".into(),
                inc: "1,1,1".into(),
                stretch: "percentile99".into(),
                autoscale: "frozen".into(),
                clip_low: String::new(),
                clip_high: String::new(),
            }),
        )
        .unwrap();
        let first = session.snapshot().unwrap();
        let first_plane = first.plane.expect("first plane");
        assert_eq!(first_plane.clip_min, 100.0);
        assert_eq!(first_plane.clip_max, 400.0);

        let second = session
            .handle_command(ImageBrowserCommand::StepNonDisplayAxis { axis: 2, delta: -1 })
            .unwrap();
        let second_plane = second.plane.expect("second plane");
        assert_eq!(second_plane.clip_min, 100.0);
        assert_eq!(second_plane.clip_max, 400.0);
        assert_eq!(second_plane.data_min, 1.0);
        assert_eq!(second_plane.data_max, 4.0);
    }

    #[test]
    fn session_moves_cursor_and_steps_non_display_axis() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("cube.image");
        let mut image = PagedImage::<f32>::create(vec![2, 2, 3], cube_coords(), &path).unwrap();
        image
            .put_slice(
                &ArrayD::from_shape_vec(
                    IxDyn(&[2, 2, 3]),
                    vec![
                        1.0, 10.0, 100.0, 2.0, 20.0, 200.0, 3.0, 30.0, 300.0, 4.0, 40.0, 400.0,
                    ],
                )
                .unwrap(),
                &[0, 0, 0],
            )
            .unwrap();
        image.save().unwrap();

        let mut session =
            ImageBrowserSession::open(&path, ImageBrowserViewport::new(2, 2)).unwrap();
        let moved = session
            .handle_command(ImageBrowserCommand::MoveCursor { dx: 1, dy: 1 })
            .unwrap();
        assert_eq!(moved.probe.as_ref().unwrap().pixel_indices, vec![1, 1, 1]);

        let stepped = session
            .handle_command(ImageBrowserCommand::StepNonDisplayAxis { axis: 2, delta: 2 })
            .unwrap();
        assert_eq!(stepped.non_display_axes.first().unwrap().index, 2);
        assert_eq!(stepped.probe.as_ref().unwrap().value, 400.0);
    }

    #[test]
    fn session_applies_window_parameters_to_plane_and_probe() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("windowed-session.image");
        let values = (0..5)
            .flat_map(|x| (0..4).map(move |y| (x * 10 + y) as f32))
            .collect::<Vec<_>>();
        let mut image =
            PagedImage::<f32>::create(vec![5, 4], CoordinateSystem::new(), &path).unwrap();
        image
            .put_slice(
                &ArrayD::from_shape_vec(IxDyn(&[5, 4]), values).unwrap(),
                &[0, 0],
            )
            .unwrap();
        image.save().unwrap();

        let mut session =
            ImageBrowserSession::open(&path, ImageBrowserViewport::new(80, 6)).unwrap();
        let snapshot = session
            .handle_command(ImageBrowserCommand::SetViewWindow {
                parameters: ImageBrowserParameters {
                    blc: "1,1".into(),
                    trc: "4,3".into(),
                    inc: "2,1".into(),
                    stretch: "percentile99".into(),
                    autoscale: "per_plane".into(),
                    clip_low: String::new(),
                    clip_high: String::new(),
                },
            })
            .unwrap();
        assert_eq!(snapshot.parameters.blc, "1,1");
        assert_eq!(snapshot.parameters.trc, "4,3");
        assert_eq!(snapshot.parameters.inc, "2,1");
        assert!(
            snapshot
                .content_lines
                .first()
                .is_some_and(|line| line.contains("1"))
        );
        assert!(
            snapshot
                .content_lines
                .first()
                .is_some_and(|line| line.contains("3"))
        );
        assert_eq!(snapshot.probe.as_ref().unwrap().pixel_indices, vec![1, 2]);

        let moved = session
            .handle_command(ImageBrowserCommand::SetCursor { x: 3, y: 3 })
            .unwrap();
        assert_eq!(moved.probe.as_ref().unwrap().pixel_indices, vec![3, 3]);
        assert_eq!(moved.probe.as_ref().unwrap().value, 33.0);
    }

    #[test]
    fn plane_view_renders_numeric_grid_content() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("grid.image");
        let mut image =
            PagedImage::<f32>::create(vec![3, 3], CoordinateSystem::new(), &path).unwrap();
        image
            .put_slice(
                &ArrayD::from_shape_vec(
                    IxDyn(&[3, 3]),
                    vec![1.0, 2.0, 3.0, 10.0, 20.0, 30.0, 100.0, 200.0, 300.0],
                )
                .unwrap(),
                &[0, 0],
            )
            .unwrap();
        image.save().unwrap();

        let mut session =
            ImageBrowserSession::open(&path, ImageBrowserViewport::new(48, 6)).unwrap();
        let snapshot = session.snapshot().unwrap();
        assert_eq!(snapshot.active_view, ImageBrowserView::Plane);
        assert!(snapshot.content_lines.first().unwrap().contains("y/x"));
        assert!(
            snapshot
                .content_lines
                .iter()
                .any(|line| line.contains('[') && line.contains(']'))
        );
        assert!(
            snapshot
                .content_lines
                .iter()
                .any(|line| line.contains("200"))
        );
    }

    #[test]
    fn inspector_formats_radec_probe_axes() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("radec-cube.image");
        let mut image = PagedImage::<f32>::create(vec![2, 2, 3], cube_coords(), &path).unwrap();
        image
            .put_slice(
                &ArrayD::from_shape_vec(
                    IxDyn(&[2, 2, 3]),
                    vec![
                        1.0, 10.0, 100.0, 2.0, 20.0, 200.0, 3.0, 30.0, 300.0, 4.0, 40.0, 400.0,
                    ],
                )
                .unwrap(),
                &[0, 0, 0],
            )
            .unwrap();
        image.save().unwrap();

        let mut session =
            ImageBrowserSession::open(&path, ImageBrowserViewport::new(48, 8)).unwrap();
        let snapshot = session
            .handle_command(ImageBrowserCommand::MoveCursor { dx: 1, dy: 1 })
            .unwrap();

        assert!(
            snapshot
                .inspector_lines
                .iter()
                .any(|line| line.contains("Right Ascension: 00:00:00.000000 hms"))
        );
        assert!(
            snapshot
                .inspector_lines
                .iter()
                .any(|line| line.contains("Declination: +45.00.00.00000 dms"))
        );
    }

    #[test]
    fn coordinates_view_includes_active_cursor_world_readout() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("coordinates-cube.image");
        let mut image = PagedImage::<f32>::create(vec![2, 2, 3], cube_coords(), &path).unwrap();
        image
            .put_slice(
                &ArrayD::from_shape_vec(
                    IxDyn(&[2, 2, 3]),
                    vec![
                        1.0, 10.0, 100.0, 2.0, 20.0, 200.0, 3.0, 30.0, 300.0, 4.0, 40.0, 400.0,
                    ],
                )
                .unwrap(),
                &[0, 0, 0],
            )
            .unwrap();
        image.save().unwrap();

        let mut session =
            ImageBrowserSession::open(&path, ImageBrowserViewport::new(80, 12)).unwrap();
        session
            .handle_command(ImageBrowserCommand::MoveCursor { dx: 1, dy: 1 })
            .unwrap();
        let snapshot = session
            .handle_command(ImageBrowserCommand::CycleView { forward: false })
            .unwrap();

        assert_eq!(snapshot.active_view, ImageBrowserView::Coordinates);
        assert!(
            snapshot
                .content_lines
                .iter()
                .any(|line| line == "== Active Cursor ==")
        );
        assert!(
            snapshot
                .content_lines
                .iter()
                .any(|line| line.contains("pixel: 1, 1, 1"))
        );
        assert!(
            snapshot
                .content_lines
                .iter()
                .any(|line| line.contains("Right Ascension: 00:00:00.000000 hms"))
        );
        assert!(
            snapshot
                .content_lines
                .iter()
                .any(|line| line.contains("Declination: +45.00.00.00000 dms"))
        );
        assert!(
            snapshot
                .content_lines
                .iter()
                .any(|line| line.contains("Frequency: 1.421 GHz"))
        );
    }

    #[test]
    fn plane_grid_content_can_exceed_viewport_width() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("wide-grid.image");
        let mut image =
            PagedImage::<f32>::create(vec![8, 2], CoordinateSystem::new(), &path).unwrap();
        image
            .put_slice(
                &ArrayD::from_shape_vec(
                    IxDyn(&[8, 2]),
                    (0..16).map(|value| value as f32).collect(),
                )
                .unwrap(),
                &[0, 0],
            )
            .unwrap();
        image.save().unwrap();

        let mut session =
            ImageBrowserSession::open(&path, ImageBrowserViewport::new(24, 4)).unwrap();
        let snapshot = session.snapshot().unwrap();

        assert!(
            snapshot
                .content_lines
                .iter()
                .any(|line| line.chars().count() > 24)
        );
    }

    #[test]
    fn session_squeezes_degenerate_axes_for_plane_mode() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("degenerate.image");
        let mut image =
            PagedImage::<f32>::create(vec![2, 2, 1, 3], CoordinateSystem::new(), &path).unwrap();
        image
            .put_slice(
                &ArrayD::from_shape_vec(
                    IxDyn(&[2, 2, 1, 3]),
                    vec![
                        1.0, 10.0, 100.0, 2.0, 20.0, 200.0, 3.0, 30.0, 300.0, 4.0, 40.0, 400.0,
                    ],
                )
                .unwrap(),
                &[0, 0, 0, 0],
            )
            .unwrap();
        image.save().unwrap();

        let mut session =
            ImageBrowserSession::open(&path, ImageBrowserViewport::new(2, 2)).unwrap();
        let snapshot = session.snapshot().unwrap();
        assert_eq!(snapshot.active_view, ImageBrowserView::Plane);
        assert_eq!(snapshot.non_display_axes.first().unwrap().index, 1);
        assert_eq!(snapshot.non_display_axes.first().unwrap().length, 3);
        assert_eq!(
            snapshot.probe.as_ref().unwrap().pixel_indices,
            vec![1, 1, 0, 1]
        );

        let stepped = session
            .handle_command(ImageBrowserCommand::StepNonDisplayAxis { axis: 3, delta: 2 })
            .unwrap();
        assert_eq!(
            stepped.probe.as_ref().unwrap().pixel_indices,
            vec![1, 1, 0, 2]
        );
        assert_eq!(stepped.probe.as_ref().unwrap().value, 400.0);
    }

    #[test]
    fn reopen_replaces_root_image() {
        let dir = tempfile::tempdir().unwrap();
        let first_path = dir.path().join("first.image");
        let second_path = dir.path().join("second.image");
        let mut first =
            PagedImage::<f32>::create(vec![2, 2], CoordinateSystem::new(), &first_path).unwrap();
        first.save().unwrap();
        let mut second =
            PagedImage::<f32>::create(vec![2, 2, 2, 2], CoordinateSystem::new(), &second_path)
                .unwrap();
        second.save().unwrap();

        let mut session =
            ImageBrowserSession::open(&first_path, ImageBrowserViewport::new(2, 2)).unwrap();
        let snapshot = session
            .handle_command(ImageBrowserCommand::OpenRoot {
                path: second_path.display().to_string(),
                viewport: ImageBrowserViewport::new(2, 2),
                parameters: None,
            })
            .unwrap();
        assert_eq!(
            snapshot.status_line,
            "pixel-only mode: coordinate reconstruction unavailable"
        );
        assert_eq!(snapshot.non_display_axes.len(), 2);
    }
}

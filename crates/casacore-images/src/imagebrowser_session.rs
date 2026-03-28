// SPDX-License-Identifier: LGPL-3.0-or-later
//! Long-lived image browser session state.

use std::path::Path;

use casacore_imagebrowser_protocol::{
    ImageBrowserAxisValue, ImageBrowserCapabilities, ImageBrowserCommand, ImageBrowserFocus,
    ImageBrowserProbe, ImageBrowserSnapshot, ImageBrowserView, ImageBrowserViewport,
    ImageHiddenAxisState, ImageNavigationMetrics, ImagePlaneRaster,
};

use crate::error::ImageError;
use crate::{
    ImageAxisValue, ImageMetadataSection, ImageProbe, ImageViewCapabilities, OpenedImageView,
    PlaneRaster,
};

/// Long-lived read-only image browser session.
#[derive(Debug)]
pub struct ImageBrowserSession {
    view: OpenedImageView,
    active_view: ImageBrowserView,
    focus: ImageBrowserFocus,
    viewport: ImageBrowserViewport,
    cursor_x: usize,
    cursor_y: usize,
    hidden_index: usize,
    content_offset: usize,
}

impl ImageBrowserSession {
    /// Opens a new browser session rooted at the provided image path.
    pub fn open(
        path: impl AsRef<Path>,
        viewport: ImageBrowserViewport,
    ) -> Result<Self, ImageError> {
        let view = OpenedImageView::open(path)?;
        let active_view = ImageBrowserView::Metadata;
        Ok(Self {
            view,
            active_view,
            focus: ImageBrowserFocus::Content,
            viewport,
            cursor_x: 0,
            cursor_y: 0,
            hidden_index: 0,
            content_offset: 0,
        })
    }

    /// Replaces the current root image.
    pub fn reopen(
        &mut self,
        path: impl AsRef<Path>,
        viewport: ImageBrowserViewport,
    ) -> Result<ImageBrowserSnapshot, ImageError> {
        *self = Self::open(path, viewport)?;
        self.snapshot()
    }

    /// Applies a session command and returns the updated snapshot.
    pub fn handle_command(
        &mut self,
        command: ImageBrowserCommand,
    ) -> Result<ImageBrowserSnapshot, ImageError> {
        match command {
            ImageBrowserCommand::OpenRoot { path, viewport } => self.reopen(path, viewport),
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
            ImageBrowserCommand::StepHiddenAxis { delta } => {
                self.step_hidden_axis(delta);
                self.snapshot()
            }
            ImageBrowserCommand::GetSnapshot => self.snapshot(),
        }
    }

    /// Returns the current snapshot without changing state.
    pub fn snapshot(&self) -> Result<ImageBrowserSnapshot, ImageError> {
        let inspector_lines = self.inspector_lines()?;
        let plane_raster = if self.active_view == ImageBrowserView::Plane
            && self.view.capabilities().renderable_plane
        {
            Some(self.view.render_plane(
                (
                    self.viewport.width.max(1) as usize,
                    self.viewport.height.max(1) as usize,
                ),
                self.hidden_index,
            )?)
        } else {
            None
        };
        let inspector_height = if self.viewport.inspector_height == 0 {
            inspector_lines.len().max(1)
        } else {
            self.viewport.inspector_height as usize
        };
        let content_height = self.viewport.height.max(1) as usize;
        let all_content_lines = if let Some(raster) = plane_raster.as_ref() {
            self.plane_content_lines(raster)?
        } else {
            self.content_lines()?
        };
        let total_items = all_content_lines.len();
        let max_offset = total_items.saturating_sub(content_height);
        let content_offset = self.content_offset.min(max_offset);
        let content_lines = clip_lines(&all_content_lines, content_offset, content_height);
        let plane = plane_raster.map(map_plane_raster);
        let probe = if self.active_view == ImageBrowserView::Plane
            && self.view.capabilities().renderable_plane
        {
            Some(map_probe(
                self.view
                    .probe((self.cursor_x, self.cursor_y), self.hidden_index)?,
            ))
        } else {
            None
        };

        let navigation = if self.active_view == ImageBrowserView::Plane
            && self.view.capabilities().renderable_plane
        {
            if let Some(display_axes) = self.view.axis_model().display_axes {
                ImageNavigationMetrics {
                    selected_index: self
                        .cursor_y
                        .min(self.view.shape()[display_axes[1]].saturating_sub(1)),
                    total_items: self.view.shape()[display_axes[1]],
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
            inspector_lines: clip_lines(&inspector_lines, 0, inspector_height),
            content_lines,
            navigation,
            plane,
            probe,
            hidden_axis: self
                .view
                .axis_model()
                .hidden_axis
                .map(|_| ImageHiddenAxisState {
                    label: self
                        .view
                        .axis_model()
                        .hidden_axis_name
                        .clone()
                        .unwrap_or_else(|| "Axis".into()),
                    index: self.hidden_index,
                    length: self.view.axis_model().hidden_axis_len.unwrap_or(0),
                }),
            capabilities: map_capabilities(self.view.capabilities()),
        })
    }

    fn available_views(&self) -> &'static [ImageBrowserView] {
        if self.view.capabilities().renderable_plane {
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
            let max_x = self.view.shape()[display_axes[0]].saturating_sub(1) as i32;
            let max_y = self.view.shape()[display_axes[1]].saturating_sub(1) as i32;
            self.cursor_x = (self.cursor_x as i32 + dx).clamp(0, max_x) as usize;
            self.cursor_y = (self.cursor_y as i32 + dy).clamp(0, max_y) as usize;
            return Ok(());
        }

        let content_height = self.viewport.height.max(1) as usize;
        let total_items = self.content_lines()?.len();
        let max_offset = total_items.saturating_sub(content_height) as i32;
        self.content_offset = (self.content_offset as i32 + dy).clamp(0, max_offset) as usize;
        Ok(())
    }

    fn step_hidden_axis(&mut self, delta: i32) {
        let Some(length) = self.view.axis_model().hidden_axis_len else {
            return;
        };
        if length == 0 {
            self.hidden_index = 0;
            return;
        }
        self.hidden_index =
            (self.hidden_index as i32 + delta).clamp(0, length.saturating_sub(1) as i32) as usize;
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
        ];
        if let Some(hidden_axis) = self.view.axis_model().hidden_axis {
            lines.push(format!(
                "Hidden axis {} ({}): {}/{}",
                self.view
                    .axis_model()
                    .hidden_axis_name
                    .as_deref()
                    .unwrap_or("Axis"),
                hidden_axis,
                self.hidden_index,
                self.view
                    .axis_model()
                    .hidden_axis_len
                    .unwrap_or(0)
                    .saturating_sub(1)
            ));
        }
        if self.active_view == ImageBrowserView::Plane && self.view.capabilities().renderable_plane
        {
            lines.push(format!("Cursor: x={} y={}", self.cursor_x, self.cursor_y));
            let probe = self
                .view
                .probe((self.cursor_x, self.cursor_y), self.hidden_index)?;
            lines.push(format!("Value: {}", probe.value));
            if probe.masked {
                lines.push("Masked: true".into());
            }
            for axis in probe.world_axes {
                lines.push(format!(
                    "{}: {} {}",
                    axis.name,
                    axis.value,
                    if axis.unit.is_empty() { "" } else { &axis.unit }
                ));
            }
        }
        Ok(lines)
    }

    fn content_lines(&self) -> Result<Vec<String>, ImageError> {
        let sections = self.view.metadata_sections()?;
        let filtered = match self.active_view {
            ImageBrowserView::Metadata => filter_sections(&sections, &["Summary", "Axes", "Misc"]),
            ImageBrowserView::Coordinates => filter_sections(&sections, &["Coordinates", "Axes"]),
            ImageBrowserView::Plane => Vec::new(),
        };
        Ok(flatten_sections(&filtered))
    }

    fn plane_content_lines(&self, _raster: &PlaneRaster) -> Result<Vec<String>, ImageError> {
        self.view.render_plane_value_grid(
            (
                self.viewport.width.max(1) as usize,
                self.viewport.height.max(1) as usize,
            ),
            self.hidden_index,
            (self.cursor_x, self.cursor_y),
        )
    }
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

fn map_capabilities(capabilities: &ImageViewCapabilities) -> ImageBrowserCapabilities {
    ImageBrowserCapabilities {
        renderable_plane: capabilities.renderable_plane,
        world_coords_available: capabilities.world_coords_available,
        pixel_only_mode: capabilities.pixel_only_mode,
        single_hidden_axis_stepper: capabilities.single_hidden_axis_stepper,
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

fn map_axis_value(value: ImageAxisValue) -> ImageBrowserAxisValue {
    ImageBrowserAxisValue {
        name: value.name,
        unit: value.unit,
        value: value.value,
    }
}

#[cfg(test)]
mod tests {
    use casacore_coordinates::{
        CoordinateSystem, DirectionCoordinate, Projection, ProjectionType, SpectralCoordinate,
    };
    use casacore_imagebrowser_protocol::ImageBrowserCommand;
    use casacore_types::ArrayD;
    use casacore_types::measures::direction::DirectionRef;
    use casacore_types::measures::frequency::FrequencyRef;
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
        assert_eq!(snapshot.active_view, ImageBrowserView::Metadata);
        assert!(snapshot.plane.is_none());

        session
            .handle_command(ImageBrowserCommand::CycleView { forward: true })
            .unwrap();
        let plane = session
            .handle_command(ImageBrowserCommand::CycleView { forward: true })
            .unwrap();
        assert_eq!(plane.active_view, ImageBrowserView::Plane);
        assert!(plane.plane.is_some());

        let resized = session
            .handle_command(ImageBrowserCommand::Resize {
                viewport: ImageBrowserViewport::new(2, 2),
            })
            .unwrap();
        assert_eq!(resized.plane.as_ref().unwrap().width, 2);
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
    fn session_moves_cursor_and_steps_hidden_axis() {
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
        session
            .handle_command(ImageBrowserCommand::CycleView { forward: true })
            .unwrap();
        session
            .handle_command(ImageBrowserCommand::CycleView { forward: true })
            .unwrap();
        let moved = session
            .handle_command(ImageBrowserCommand::MoveCursor { dx: 1, dy: 1 })
            .unwrap();
        assert_eq!(moved.probe.as_ref().unwrap().pixel_indices, vec![1, 1, 0]);

        let stepped = session
            .handle_command(ImageBrowserCommand::StepHiddenAxis { delta: 2 })
            .unwrap();
        assert_eq!(stepped.hidden_axis.as_ref().unwrap().index, 2);
        assert_eq!(stepped.probe.as_ref().unwrap().value, 400.0);
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
        session
            .handle_command(ImageBrowserCommand::CycleView { forward: true })
            .unwrap();
        let snapshot = session
            .handle_command(ImageBrowserCommand::CycleView { forward: true })
            .unwrap();
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
        session
            .handle_command(ImageBrowserCommand::CycleView { forward: true })
            .unwrap();
        let snapshot = session
            .handle_command(ImageBrowserCommand::CycleView { forward: true })
            .unwrap();
        assert_eq!(snapshot.active_view, ImageBrowserView::Plane);
        assert_eq!(snapshot.hidden_axis.as_ref().unwrap().index, 0);
        assert_eq!(snapshot.hidden_axis.as_ref().unwrap().length, 3);

        let stepped = session
            .handle_command(ImageBrowserCommand::StepHiddenAxis { delta: 2 })
            .unwrap();
        assert_eq!(
            stepped.probe.as_ref().unwrap().pixel_indices,
            vec![0, 0, 0, 2]
        );
        assert_eq!(stepped.probe.as_ref().unwrap().value, 100.0);
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
            })
            .unwrap();
        assert_eq!(
            snapshot.status_line,
            "viewer supports one 2D plane plus at most one non-degenerate hidden axis in wave 1"
        );
        assert!(snapshot.plane.is_none());
    }
}

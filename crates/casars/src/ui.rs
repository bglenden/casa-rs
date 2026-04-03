// SPDX-License-Identifier: LGPL-3.0-or-later
#[path = "ui/browser_manager.rs"]
mod browser_manager;

use ratatui::Frame;
use ratatui::layout::Alignment;
use ratatui::layout::{Constraint, Direction, Layout, Rect};
use ratatui::style::{Modifier, Style};
use ratatui::text::{Line, Span};
use ratatui::widgets::{
    Block, Borders, Clear, List, ListItem, Padding, Paragraph, Scrollbar, ScrollbarOrientation,
    ScrollbarState, Wrap,
};
use ratatui_image::Image as PanelImage;

use crate::app::{
    AppState, BrowserTab, FormRowKind, FormSelection, OutputPane, PaneFocus, PlotControlTarget,
    PlotPaneFocus, ResultContent, ResultTab, VisibleTextBuffer, VisibleTextLine, VisibleTextRole,
};
use crate::config::ThemeMode;
use crate::registry::RegistryApp;
use crate::theme::{Theme, theme};
pub(crate) use browser_manager::{browser_mode_picker_area, browser_mode_picker_list_area};
use browser_manager::{
    browser_mode_selector_rect, draw_browser_mode_picker, selector_button_label,
    visible_browser_manager_hits,
};

#[derive(Debug, Clone)]
pub(crate) struct UiLayout {
    pub header: Rect,
    pub body: Rect,
    pub footer: Rect,
    pub form_block: Rect,
    pub form_inner: Rect,
    pub form_rows: Vec<FormRowHit>,
    pub browser_manager_rows: Vec<FormRowHit>,
    pub browser_mode_selector: Option<Rect>,
    pub divider: Rect,
    pub result_block: Rect,
    pub result_status: Rect,
    pub result_tabs: Rect,
    pub result_content: Rect,
    pub result_scrollbar: Option<Rect>,
    pub result_hscrollbar: Option<Rect>,
    pub plot_workspace: Option<PlotWorkspaceLayout>,
    pub tab_hits: Vec<TabHit>,
    pub browser_tab_hits: Vec<BrowserTabHit>,
}

#[derive(Debug, Clone)]
pub(crate) struct FormRowHit {
    pub target: FormSelection,
    pub rect: Rect,
}

#[derive(Debug, Clone)]
pub(crate) struct TabHit {
    pub tab: ResultTab,
    pub rect: Rect,
}

#[derive(Debug, Clone)]
pub(crate) struct BrowserTabHit {
    pub tab: BrowserTab,
    pub rect: Rect,
}

#[derive(Debug, Clone)]
pub(crate) struct PlotWorkspaceLayout {
    pub catalog_block: Rect,
    pub catalog_inner: Rect,
    pub catalog_hits: Vec<PlotCatalogHit>,
    pub canvas_block: Rect,
    pub canvas_inner: Rect,
    pub controls_block: Rect,
    pub controls_inner: Rect,
    pub control_hits: Vec<PlotControlHit>,
}

#[derive(Debug, Clone)]
pub(crate) struct PlotCatalogHit {
    pub tab: crate::app::PlotCatalogRowView,
    pub rect: Rect,
}

#[derive(Debug, Clone)]
pub(crate) struct PlotControlHit {
    pub target: PlotControlTarget,
    pub rect: Rect,
}

#[derive(Debug, Clone)]
struct VisiblePlotCatalog {
    rows: Vec<crate::app::PlotCatalogRowView>,
    hidden_above: usize,
    hidden_below: usize,
}

#[derive(Debug, Clone, Copy)]
pub(crate) struct ImagePlaneCanvasLayout {
    pub canvas: Rect,
}

#[derive(Debug, Clone, Copy)]
pub(crate) struct ImagePlaneWorkspaceLayout {
    pub plane: ImagePlaneCanvasLayout,
    pub divider: Option<Rect>,
    pub divider_toggle: Option<Rect>,
    pub spectrum_canvas: Option<Rect>,
}

impl UiLayout {
    pub(crate) fn in_divider_toggle(&self, column: u16, row: u16) -> bool {
        let Some(rect) = divider_toggle_rect(self.divider) else {
            return false;
        };
        rect_contains(rect, column, row)
    }

    pub(crate) fn form_target_at(&self, column: u16, row: u16) -> Option<FormSelection> {
        self.form_rows
            .iter()
            .chain(self.browser_manager_rows.iter())
            .find(|hit| rect_contains(hit.rect, column, row))
            .map(|hit| hit.target)
    }

    pub(crate) fn result_tab_at(&self, column: u16, row: u16) -> Option<ResultTab> {
        self.tab_hits
            .iter()
            .find(|hit| rect_contains(hit.rect, column, row))
            .map(|hit| hit.tab)
    }

    pub(crate) fn browser_tab_at(&self, column: u16, row: u16) -> Option<BrowserTab> {
        self.browser_tab_hits
            .iter()
            .find(|hit| rect_contains(hit.rect, column, row))
            .map(|hit| hit.tab)
    }

    pub(crate) fn in_divider(&self, column: u16, row: u16) -> bool {
        if self.divider.width == 0 || self.divider.height == 0 {
            return false;
        }
        let hit_x = self.divider.x.saturating_sub(1);
        let hit_width = self.divider.width.saturating_add(2);
        rect_contains(
            Rect {
                x: hit_x,
                y: self.divider.y,
                width: hit_width,
                height: self.divider.height,
            },
            column,
            row,
        )
    }

    pub(crate) fn in_form_block(&self, column: u16, row: u16) -> bool {
        rect_contains(self.form_block, column, row)
    }

    pub(crate) fn in_browser_mode_selector(&self, column: u16, row: u16) -> bool {
        self.browser_mode_selector
            .is_some_and(|rect| rect_contains(rect, column, row))
    }

    pub(crate) fn in_result_block(&self, column: u16, row: u16) -> bool {
        rect_contains(self.result_block, column, row)
    }

    pub(crate) fn in_result_scrollbar(&self, column: u16, row: u16) -> bool {
        self.result_scrollbar
            .is_some_and(|rect| rect_contains(rect, column, row))
    }

    pub(crate) fn in_result_hscrollbar(&self, column: u16, row: u16) -> bool {
        self.result_hscrollbar
            .is_some_and(|rect| rect_contains(rect, column, row))
    }

    pub(crate) fn plot_catalog_at(
        &self,
        column: u16,
        row: u16,
    ) -> Option<crate::app::PlotCatalogRowView> {
        self.plot_workspace
            .as_ref()?
            .catalog_hits
            .iter()
            .find(|hit| rect_contains(hit.rect, column, row))
            .map(|hit| hit.tab.clone())
    }

    pub(crate) fn plot_control_at(&self, column: u16, row: u16) -> Option<PlotControlTarget> {
        self.plot_workspace
            .as_ref()?
            .control_hits
            .iter()
            .find(|hit| rect_contains(hit.rect, column, row))
            .map(|hit| hit.target)
    }

    pub(crate) fn in_plot_canvas(&self, column: u16, row: u16) -> bool {
        self.plot_workspace
            .as_ref()
            .is_some_and(|workspace| rect_contains(workspace.canvas_inner, column, row))
    }
}

pub(crate) fn compute_layout(area: Rect, app: &AppState) -> UiLayout {
    let palette = theme(app.theme_mode());
    let vertical = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(1),
            Constraint::Min(8),
            Constraint::Length(1),
        ])
        .split(area);

    let body = vertical[1];
    let collapsed = app.parameters_pane_collapsed();
    let divider_width = u16::from(body.width > 0);
    let available_width = body.width.saturating_sub(divider_width);
    let (left_width, right_width) = if collapsed {
        (0, body.width)
    } else {
        let min_pane = 24.min(available_width / 2);
        let mut left_width = ((available_width as f32) * app.pane_split_ratio()).round() as u16;
        if available_width > min_pane.saturating_mul(2) {
            left_width = left_width.clamp(min_pane, available_width.saturating_sub(min_pane));
        } else {
            left_width = available_width / 2;
        }
        let right_width = body.width.saturating_sub(left_width + divider_width);
        (left_width, right_width)
    };
    let horizontal = Layout::default()
        .direction(Direction::Horizontal)
        .constraints([
            Constraint::Length(left_width),
            Constraint::Length(divider_width),
            Constraint::Length(right_width),
        ])
        .split(body);

    let form_block = horizontal[0];
    let divider = horizontal[1];
    let result_block = horizontal[2];
    let form_inner = form_block_widget(app, palette).inner(form_block);
    let result_inner = result_block_widget(app, palette).inner(result_block);

    let form_rows = visible_form_hits(form_inner, app);
    let browser_mode_selector = browser_mode_selector_rect(form_inner, form_rows.len(), app);
    let browser_manager_rows =
        visible_browser_manager_hits(form_inner, form_rows.len(), browser_mode_selector, app);

    let status_height = if result_inner.height >= 5 {
        3
    } else {
        result_inner.height.min(2)
    };
    let tabs_height = if result_inner.height > status_height {
        1
    } else {
        0
    };
    let result_status = Rect {
        x: result_inner.x,
        y: result_inner.y,
        width: result_inner.width,
        height: status_height,
    };
    let result_tabs = Rect {
        x: result_inner.x,
        y: result_inner.y.saturating_add(status_height),
        width: result_inner.width,
        height: tabs_height,
    };
    let result_content = Rect {
        x: result_inner.x,
        y: result_tabs.y.saturating_add(tabs_height),
        width: result_inner.width,
        height: result_inner
            .height
            .saturating_sub(status_height + tabs_height),
    };

    let tab_hits = if app.browser_is_active() {
        Vec::new()
    } else {
        visible_tab_hits(result_tabs, app)
    };
    let browser_tab_hits = if app.browser_is_active() {
        visible_browser_tab_hits(result_tabs, app)
    } else {
        Vec::new()
    };
    let result_scrollbar = result_scrollbar_rect(app, result_content);
    let result_hscrollbar = result_hscrollbar_rect(app, result_content);
    let plot_workspace = if !app.browser_is_active() && app.active_result_tab() == ResultTab::Plots
    {
        Some(build_plot_workspace_layout(result_content, app))
    } else {
        None
    };

    UiLayout {
        header: vertical[0],
        body,
        footer: vertical[2],
        form_block,
        form_inner,
        form_rows,
        browser_manager_rows,
        browser_mode_selector,
        divider,
        result_block,
        result_status,
        result_tabs,
        result_content,
        result_scrollbar,
        result_hscrollbar,
        plot_workspace,
        tab_hits,
        browser_tab_hits,
    }
}

pub(crate) fn draw(frame: &mut Frame<'_>, app: &AppState, layout: &UiLayout) {
    let palette = theme(app.theme_mode());

    let header = Paragraph::new(header_line(app, palette));
    frame.render_widget(header, layout.header);

    let footer_text = app.footer_text();
    let footer = Paragraph::new(footer_line(&footer_text, palette));
    frame.render_widget(footer, layout.footer);

    let divider_glyph = if app.parameters_pane_collapsed() {
        "▶"
    } else {
        "◀"
    };
    let divider_lines = (0..layout.divider.height)
        .map(|index| {
            if index == 0 {
                Line::from(Span::styled(
                    divider_glyph,
                    Style::default()
                        .fg(palette.divider_fg)
                        .add_modifier(Modifier::BOLD),
                ))
            } else {
                Line::from(palette.divider_glyph)
            }
        })
        .collect::<Vec<_>>();
    let divider = Paragraph::new(divider_lines).style(Style::default().fg(palette.divider_fg));
    frame.render_widget(divider, layout.divider);

    draw_form(frame, app, layout, palette);
    draw_result(frame, app, layout, palette);
    if app.path_chooser_active() {
        draw_path_chooser(frame, app, layout, palette);
    } else if app.browser_mode_picker_active() {
        draw_browser_mode_picker(frame, app, layout, palette);
    }
    if app.help_visible() {
        draw_help_overlay(frame, app, palette);
    }
}

pub(crate) fn draw_launcher(frame: &mut Frame<'_>, apps: &[RegistryApp], selected: usize) {
    let palette = theme(ThemeMode::DenseAnsi);
    let area = frame.area();
    let vertical = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(1),
            Constraint::Min(8),
            Constraint::Length(1),
        ])
        .split(area);

    frame.render_widget(
        Paragraph::new(Line::from(vec![
            Span::styled(
                "casars",
                Style::default()
                    .fg(palette.header_fg)
                    .add_modifier(Modifier::BOLD),
            ),
            Span::styled("  ◆  ", Style::default().fg(palette.header_dim_fg)),
            Span::styled(
                "Select Application",
                Style::default()
                    .fg(palette.header_fg)
                    .add_modifier(Modifier::BOLD),
            ),
        ])),
        vertical[0],
    );
    frame.render_widget(
        Paragraph::new(footer_line("Up/Down select  Enter launch  q quit", palette)),
        vertical[2],
    );

    let panel = centered_rect(72, apps.len() as u16 + 8, vertical[1]);
    frame.render_widget(Clear, panel);
    let block = Block::default()
        .title("Applications")
        .title_style(
            Style::default()
                .fg(palette.header_fg)
                .add_modifier(Modifier::BOLD),
        )
        .borders(Borders::ALL)
        .border_set(palette.border_set)
        .border_style(Style::default().fg(palette.active_pane_border_fg))
        .padding(Padding::new(1, 1, 1, 1));
    let inner = block.inner(panel);
    frame.render_widget(block, panel);

    let description = Paragraph::new(vec![
        Line::from("Choose the application to launch."),
        Line::from(""),
    ])
    .style(Style::default().fg(palette.footer_fg))
    .alignment(Alignment::Left);
    let description_area = Rect {
        x: inner.x,
        y: inner.y,
        width: inner.width,
        height: inner.height.min(2),
    };
    frame.render_widget(description, description_area);

    let list_area = Rect {
        x: inner.x,
        y: inner.y.saturating_add(2),
        width: inner.width,
        height: inner.height.saturating_sub(2),
    };
    let items = apps
        .iter()
        .enumerate()
        .map(|(index, app)| {
            let marker = if index == selected {
                palette.selection_glyph
            } else {
                " "
            };
            let line = format!(
                "{marker} {:<14}  {} / {}",
                app.id, app.category, app.display_name
            );
            let style = if index == selected {
                Style::default()
                    .fg(palette.field_selected_fg)
                    .bg(palette.field_selected_bg)
            } else {
                Style::default().fg(palette.footer_fg)
            };
            ListItem::new(Line::from(line)).style(style)
        })
        .collect::<Vec<_>>();
    frame.render_widget(List::new(items), list_area);
}

fn draw_form(frame: &mut Frame<'_>, app: &AppState, layout: &UiLayout, palette: Theme) {
    let block = form_block_widget(app, palette);
    frame.render_widget(block, layout.form_block);

    if app.browser_is_active() && !app.browser_uses_parameter_pane() {
        if let Some(buffer) = app.visible_text_buffer(OutputPane::LeftOutput, layout) {
            render_visible_text_buffer(
                frame,
                &buffer,
                app.output_selection_rect(OutputPane::LeftOutput),
                palette,
            );
        }
        return;
    }

    let rows = app.form_rows();
    let visible_targets = layout
        .form_rows
        .iter()
        .map(|row| row.target)
        .collect::<Vec<_>>();
    let items = rows
        .into_iter()
        .filter(|row| visible_targets.contains(&row.target))
        .map(|row| {
            let line =
                render_form_row_text(&row, app.pane_focus(), palette, layout.form_inner.width);
            let style = match (row.kind, row.selected, app.pane_focus()) {
                (FormRowKind::Section { .. }, true, PaneFocus::Parameters) => Style::default()
                    .fg(palette.section_selected_fg)
                    .bg(palette.section_selected_bg)
                    .add_modifier(Modifier::BOLD),
                (FormRowKind::Section { .. }, true, PaneFocus::Result) => Style::default()
                    .fg(palette.inactive_selection_fg)
                    .bg(palette.inactive_selection_bg)
                    .add_modifier(Modifier::BOLD),
                (FormRowKind::Section { .. }, false, _) => Style::default()
                    .fg(palette.section_fg)
                    .add_modifier(Modifier::BOLD),
                (FormRowKind::Field, true, PaneFocus::Parameters) => Style::default()
                    .fg(palette.field_selected_fg)
                    .bg(palette.field_selected_bg),
                (FormRowKind::Field, true, PaneFocus::Result) => Style::default()
                    .fg(palette.inactive_selection_fg)
                    .bg(palette.inactive_selection_bg),
                (FormRowKind::Field, false, _) => Style::default(),
            };
            ListItem::new(Line::from(line)).style(style)
        })
        .collect::<Vec<_>>();

    if app.browser_is_active() && app.browser_uses_parameter_pane() {
        let rows_height = layout
            .form_rows
            .len()
            .min(layout.form_inner.height as usize) as u16;
        let form_area = Rect {
            x: layout.form_inner.x,
            y: layout.form_inner.y,
            width: layout.form_inner.width,
            height: rows_height,
        };
        if form_area.height > 0 {
            frame.render_widget(List::new(items), form_area);
        }

        let gap = u16::from(form_area.height > 0);
        let summary_area = Rect {
            x: layout.form_inner.x,
            y: layout
                .form_inner
                .y
                .saturating_add(form_area.height)
                .saturating_add(gap),
            width: layout.form_inner.width,
            height: layout
                .form_inner
                .height
                .saturating_sub(form_area.height.saturating_add(gap)),
        };
        if summary_area.height > 0 {
            if let Some(selector) = layout.browser_mode_selector {
                let style = if app.browser_mode_picker_active() {
                    Style::default()
                        .fg(palette.field_selected_fg)
                        .bg(palette.field_selected_bg)
                } else {
                    Style::default().fg(palette.footer_fg)
                };
                frame.render_widget(
                    Paragraph::new(selector_button_label(
                        app.browser_mode_picker_selection()
                            .unwrap_or(app.image_browser_left_pane_mode_for_ui())
                            .label(),
                    ))
                    .style(style),
                    selector,
                );
            }
            let manager_targets = layout
                .browser_manager_rows
                .iter()
                .filter_map(|row| match row.target {
                    FormSelection::BrowserPane(target) => Some(target),
                    _ => None,
                })
                .collect::<Vec<_>>();
            let manager_items = app
                .browser_manager_rows()
                .into_iter()
                .filter(|row| manager_targets.contains(&row.target))
                .map(|row| {
                    let line = render_manager_row_text(
                        &row,
                        app.pane_focus(),
                        palette,
                        layout.form_inner.width,
                    );
                    let style = match (row.selected, app.pane_focus()) {
                        (true, PaneFocus::Parameters) => Style::default()
                            .fg(palette.field_selected_fg)
                            .bg(palette.field_selected_bg),
                        (true, PaneFocus::Result) => Style::default()
                            .fg(palette.inactive_selection_fg)
                            .bg(palette.inactive_selection_bg),
                        (false, _) => Style::default(),
                    };
                    ListItem::new(Line::from(line)).style(style)
                })
                .collect::<Vec<_>>();
            if !manager_items.is_empty() {
                let manager_area = Rect {
                    x: summary_area.x,
                    y: summary_area.y.saturating_add(2),
                    width: summary_area.width,
                    height: layout.browser_manager_rows.len() as u16,
                };
                frame.render_widget(List::new(manager_items), manager_area);
            }
            let summary_lines = app.browser_parameter_summary_lines();
            let heading = app.browser_parameter_summary_heading();
            let mut lines = vec![Line::styled(
                heading,
                Style::default()
                    .fg(palette.section_fg)
                    .add_modifier(Modifier::BOLD),
            )];
            lines.extend(summary_lines.into_iter().map(Line::from));
            let summary_text_area = if layout.browser_mode_selector.is_some() {
                Rect {
                    x: summary_area.x,
                    y: summary_area
                        .y
                        .saturating_add(2)
                        .saturating_add(layout.browser_manager_rows.len() as u16)
                        .saturating_add(u16::from(!layout.browser_manager_rows.is_empty())),
                    width: summary_area.width,
                    height: summary_area
                        .height
                        .saturating_sub(2)
                        .saturating_sub(layout.browser_manager_rows.len() as u16)
                        .saturating_sub(u16::from(!layout.browser_manager_rows.is_empty())),
                }
            } else {
                summary_area
            };
            if summary_text_area.height > 0 {
                frame.render_widget(
                    Paragraph::new(lines)
                        .style(Style::default().fg(palette.footer_fg))
                        .wrap(Wrap { trim: false }),
                    summary_text_area,
                );
            }
        }
        return;
    }

    frame.render_widget(List::new(items), layout.form_inner);
}

fn draw_result(frame: &mut Frame<'_>, app: &AppState, layout: &UiLayout, palette: Theme) {
    let block = result_block_widget(app, palette);
    frame.render_widget(block, layout.result_block);

    if layout.result_status.height > 0 {
        let status_style = match app.result_status_kind() {
            "ok" => Style::default().fg(palette.status_ok_fg),
            "error" => Style::default().fg(palette.status_error_fg),
            "running" => Style::default().fg(palette.status_running_fg),
            "warning" => Style::default().fg(palette.banner_fg),
            _ => Style::default(),
        };
        let status = Paragraph::new(
            app.result_status_lines()
                .into_iter()
                .map(Line::from)
                .collect::<Vec<_>>(),
        )
        .style(status_style)
        .wrap(Wrap { trim: false });
        frame.render_widget(status, layout.result_status);
    }

    if app.browser_is_active() {
        draw_browser_result(frame, app, layout, palette);
        return;
    }

    if layout.result_tabs.height > 0 {
        let mut spans = Vec::<Span<'static>>::new();
        for tab in app.result_tabs() {
            if !spans.is_empty() {
                spans.push(Span::raw(" "));
            }
            let label = tab_label(*tab, *tab == app.active_result_tab(), palette);
            let style = if *tab == app.active_result_tab() {
                Style::default()
                    .fg(palette.active_tab_fg)
                    .bg(palette.active_tab_bg)
                    .add_modifier(Modifier::BOLD)
            } else {
                Style::default().fg(palette.tab_fg)
            };
            spans.push(Span::styled(label, style));
        }
        frame.render_widget(Paragraph::new(Line::from(spans)), layout.result_tabs);
    }

    if layout.result_content.height == 0 {
        return;
    }

    let content = app.active_result_content();
    if let ResultContent::Graphic(summary) = &content {
        if app.active_result_tab() == ResultTab::Plots {
            draw_plot_workspace(frame, app, layout, palette, summary);
        } else {
            draw_graphic_result(frame, app, layout, palette, summary);
        }
        return;
    }
    let vertical_scrollbar =
        result_scrollbar_state(&content, app.active_result_scroll(), layout.result_content);
    let horizontal_scrollbar =
        result_hscrollbar_state(&content, app.active_result_hscroll(), layout.result_content);
    let content_area = content_viewport_area(
        layout.result_content,
        vertical_scrollbar.is_some(),
        horizontal_scrollbar.is_some(),
    );
    if let Some(buffer) = app.visible_text_buffer(OutputPane::Result, layout) {
        render_visible_text_buffer(
            frame,
            &VisibleTextBuffer {
                area: content_area,
                ..buffer
            },
            app.output_selection_rect(OutputPane::Result),
            palette,
        );
    }

    if let Some(mut state) = vertical_scrollbar {
        frame.render_stateful_widget(
            Scrollbar::default()
                .orientation(ScrollbarOrientation::VerticalRight)
                .begin_symbol(None)
                .end_symbol(None)
                .track_style(Style::default().fg(palette.scrollbar_track_fg))
                .thumb_style(Style::default().fg(palette.scrollbar_thumb_fg)),
            layout.result_scrollbar.unwrap_or(layout.result_content),
            &mut state,
        );
    }

    if let Some(mut state) = horizontal_scrollbar {
        frame.render_stateful_widget(
            Scrollbar::default()
                .orientation(ScrollbarOrientation::HorizontalBottom)
                .begin_symbol(None)
                .end_symbol(None)
                .track_style(Style::default().fg(palette.scrollbar_track_fg))
                .thumb_style(Style::default().fg(palette.scrollbar_thumb_fg)),
            layout.result_hscrollbar.unwrap_or(layout.result_content),
            &mut state,
        );
    }
}

fn draw_graphic_result(
    frame: &mut Frame<'_>,
    app: &AppState,
    layout: &UiLayout,
    palette: Theme,
    summary: &str,
) {
    let content_area = layout.result_content;
    if content_area.height == 0 || content_area.width == 0 {
        return;
    }

    if let Some(protocol) = app.plot_protocol() {
        frame.render_widget(PanelImage::new(protocol), content_area);
    } else {
        let message = if let Some(error) = app.plot_last_error() {
            format!("{summary}\n\n{error}")
        } else if app.plot_pending() {
            format!("{summary}\n\nRendering plot...")
        } else {
            summary.to_string()
        };
        let paragraph = Paragraph::new(message)
            .style(Style::default().fg(palette.footer_fg))
            .alignment(Alignment::Center)
            .wrap(Wrap { trim: false });
        frame.render_widget(paragraph, content_area);
    }
}

fn draw_plot_workspace(
    frame: &mut Frame<'_>,
    app: &AppState,
    layout: &UiLayout,
    palette: Theme,
    summary: &str,
) {
    let Some(workspace) = layout.plot_workspace.as_ref() else {
        draw_graphic_result(frame, app, layout, palette, summary);
        return;
    };

    let selected_plot = app.selected_plot_label();
    if app.is_msexplore_app() {
        let sidebar_title = match app.plot_focus() {
            PlotPaneFocus::Catalog | PlotPaneFocus::Controls => "Plots [focus]",
            PlotPaneFocus::Canvas => "Plots",
        };
        let canvas_title = match app.plot_focus() {
            PlotPaneFocus::Canvas => format!("{selected_plot} [focus]"),
            _ => selected_plot.clone(),
        };

        frame.render_widget(
            Block::default()
                .borders(Borders::ALL)
                .title(sidebar_title)
                .border_style(Style::default().fg(palette.divider_fg)),
            workspace.catalog_block,
        );
        frame.render_widget(
            Block::default()
                .borders(Borders::ALL)
                .title(canvas_title)
                .border_style(Style::default().fg(palette.divider_fg)),
            workspace.canvas_block,
        );

        let mut sidebar_lines = Vec::<Line<'static>>::new();
        sidebar_lines.push(Line::from(Span::styled(
            "Presets",
            Style::default()
                .fg(palette.section_fg)
                .add_modifier(Modifier::BOLD),
        )));
        let visible_catalog = visible_plot_catalog_rows(
            app.plot_catalog_rows(),
            available_msexplore_catalog_height(workspace.catalog_inner.height, app),
        );
        if visible_catalog.hidden_above > 0 {
            sidebar_lines.push(Line::from(Span::styled(
                format!("↑ {} more", visible_catalog.hidden_above),
                Style::default().fg(palette.footer_fg),
            )));
        }
        sidebar_lines.extend(visible_catalog.rows.into_iter().map(|row| {
            let style = if row.selected {
                Style::default()
                    .fg(palette.active_tab_fg)
                    .bg(palette.active_tab_bg)
                    .add_modifier(Modifier::BOLD)
            } else {
                Style::default().fg(palette.footer_fg)
            };
            Line::from(Span::styled(row.label, style))
        }));
        if visible_catalog.hidden_below > 0 {
            sidebar_lines.push(Line::from(Span::styled(
                format!("↓ {} more", visible_catalog.hidden_below),
                Style::default().fg(palette.footer_fg),
            )));
        }
        sidebar_lines.push(Line::from(""));
        if let Some(banner) = app.plot_dirty_banner() {
            sidebar_lines.push(Line::from(Span::styled(
                banner.to_string(),
                Style::default()
                    .fg(palette.banner_fg)
                    .add_modifier(Modifier::BOLD),
            )));
            sidebar_lines.push(Line::from(""));
        }
        sidebar_lines.push(Line::from(Span::styled(
            "Actions",
            Style::default()
                .fg(palette.section_fg)
                .add_modifier(Modifier::BOLD),
        )));
        sidebar_lines.extend(app.plot_control_rows().into_iter().map(|row| {
            let style = if row.selected {
                Style::default()
                    .fg(palette.active_tab_fg)
                    .bg(palette.active_tab_bg)
                    .add_modifier(Modifier::BOLD)
            } else {
                Style::default().fg(palette.footer_fg)
            };
            Line::from(Span::styled(row.text, style))
        }));
        frame.render_widget(
            Paragraph::new(sidebar_lines).wrap(Wrap { trim: false }),
            workspace.catalog_inner,
        );

        if let Some(protocol) = app.plot_protocol() {
            frame.render_widget(PanelImage::new(protocol), workspace.canvas_inner);
        } else {
            let message = if let Some(error) = app.plot_last_error() {
                format!("{summary}\n\n{error}")
            } else if app.plot_pending() {
                format!("{summary}\n\nRendering plot...")
            } else {
                summary.to_string()
            };
            frame.render_widget(
                Paragraph::new(message)
                    .style(Style::default().fg(palette.footer_fg))
                    .alignment(Alignment::Center)
                    .wrap(Wrap { trim: false }),
                workspace.canvas_inner,
            );
        }
        return;
    }

    let catalog_title = match app.plot_focus() {
        PlotPaneFocus::Catalog => "Catalog [focus]",
        _ => "Catalog",
    };
    let canvas_title = match app.plot_focus() {
        PlotPaneFocus::Canvas => format!("{selected_plot} [focus]"),
        _ => selected_plot.clone(),
    };
    let controls_title = match app.plot_focus() {
        PlotPaneFocus::Controls => "Controls [focus]",
        _ => "Controls",
    };

    frame.render_widget(
        Block::default()
            .borders(Borders::ALL)
            .title(catalog_title)
            .border_style(Style::default().fg(palette.divider_fg)),
        workspace.catalog_block,
    );
    frame.render_widget(
        Block::default()
            .borders(Borders::ALL)
            .title(canvas_title)
            .border_style(Style::default().fg(palette.divider_fg)),
        workspace.canvas_block,
    );
    frame.render_widget(
        Block::default()
            .borders(Borders::ALL)
            .title(controls_title)
            .border_style(Style::default().fg(palette.divider_fg)),
        workspace.controls_block,
    );

    let catalog_items = app
        .plot_catalog_rows()
        .into_iter()
        .map(|row| {
            let style = if row.selected {
                Style::default()
                    .fg(palette.active_tab_fg)
                    .bg(palette.active_tab_bg)
                    .add_modifier(Modifier::BOLD)
            } else {
                Style::default().fg(palette.footer_fg)
            };
            ListItem::new(Line::from(Span::styled(row.label, style)))
        })
        .collect::<Vec<_>>();
    frame.render_widget(List::new(catalog_items), workspace.catalog_inner);

    if let Some(protocol) = app.plot_protocol() {
        frame.render_widget(PanelImage::new(protocol), workspace.canvas_inner);
    } else {
        let message = if let Some(error) = app.plot_last_error() {
            format!("{summary}\n\n{error}")
        } else if app.plot_pending() {
            format!("{summary}\n\nRendering plot...")
        } else {
            summary.to_string()
        };
        frame.render_widget(
            Paragraph::new(message)
                .style(Style::default().fg(palette.footer_fg))
                .alignment(Alignment::Center)
                .wrap(Wrap { trim: false }),
            workspace.canvas_inner,
        );
    }

    let mut control_lines = Vec::<Line<'static>>::new();
    if let Some(banner) = app.plot_dirty_banner() {
        control_lines.push(Line::from(Span::styled(
            banner.to_string(),
            Style::default()
                .fg(palette.banner_fg)
                .add_modifier(Modifier::BOLD),
        )));
        control_lines.push(Line::from(""));
    }
    control_lines.extend(app.plot_control_rows().into_iter().map(|row| {
        let style = if row.selected {
            Style::default()
                .fg(palette.active_tab_fg)
                .bg(palette.active_tab_bg)
                .add_modifier(Modifier::BOLD)
        } else {
            Style::default().fg(palette.footer_fg)
        };
        Line::from(Span::styled(row.text, style))
    }));
    frame.render_widget(
        Paragraph::new(control_lines).wrap(Wrap { trim: false }),
        workspace.controls_inner,
    );
}

fn draw_browser_result(frame: &mut Frame<'_>, app: &AppState, layout: &UiLayout, palette: Theme) {
    if layout.result_tabs.height > 0 {
        let mut spans = Vec::<Span<'static>>::new();
        for tab in app.browser_tabs() {
            if !spans.is_empty() {
                spans.push(Span::raw(" "));
            }
            let active = app.active_browser_tab_label() == Some(tab.label());
            let style = if active {
                Style::default()
                    .fg(palette.active_tab_fg)
                    .bg(palette.active_tab_bg)
                    .add_modifier(Modifier::BOLD)
            } else {
                Style::default().fg(palette.tab_fg)
            };
            spans.push(Span::styled(
                browser_tab_label(*tab, active, palette),
                style,
            ));
        }
        frame.render_widget(Paragraph::new(Line::from(spans)), layout.result_tabs);
    }

    if layout.result_content.height == 0 {
        return;
    }

    if app.image_raster_plane_active() {
        draw_image_plane_workspace(frame, app, layout, palette);
        return;
    }

    let vertical_scrollbar = browser_scrollbar_state(app, layout.result_content);
    let horizontal_scrollbar = browser_hscrollbar_state(app, layout.result_content);
    let content_area = content_viewport_area(
        layout.result_content,
        vertical_scrollbar.is_some(),
        horizontal_scrollbar.is_some(),
    );

    let Some(buffer) = app.visible_text_buffer(OutputPane::Result, layout) else {
        return;
    };
    render_visible_text_buffer(
        frame,
        &VisibleTextBuffer {
            area: content_area,
            ..buffer
        },
        app.output_selection_rect(OutputPane::Result),
        palette,
    );

    if let Some(mut state) = vertical_scrollbar {
        frame.render_stateful_widget(
            Scrollbar::default()
                .orientation(ScrollbarOrientation::VerticalRight)
                .begin_symbol(None)
                .end_symbol(None)
                .track_style(Style::default().fg(palette.scrollbar_track_fg))
                .thumb_style(Style::default().fg(palette.scrollbar_thumb_fg)),
            layout.result_scrollbar.unwrap_or(layout.result_content),
            &mut state,
        );
    }

    if let Some(mut state) = horizontal_scrollbar {
        frame.render_stateful_widget(
            Scrollbar::default()
                .orientation(ScrollbarOrientation::HorizontalBottom)
                .begin_symbol(None)
                .end_symbol(None)
                .track_style(Style::default().fg(palette.scrollbar_track_fg))
                .thumb_style(Style::default().fg(palette.scrollbar_thumb_fg)),
            layout.result_hscrollbar.unwrap_or(layout.result_content),
            &mut state,
        );
    }
}

fn draw_image_plane_workspace(
    frame: &mut Frame<'_>,
    app: &AppState,
    layout: &UiLayout,
    palette: Theme,
) {
    let workspace = image_plane_workspace_layout(
        layout.result_content,
        app.image_plane_has_linked_profile(),
        app.image_workspace_split_ratio(),
    );

    if workspace.plane.canvas.is_empty() {
        return;
    }

    if app.image_movie_terminal_looping_active() || app.image_movie_direct_overlay_active() {
        // Leave the canvas untouched while the direct Kitty layer owns this rectangle.
    } else if let Some(protocol) = app.image_plane_protocol() {
        frame.render_widget(PanelImage::new(protocol), workspace.plane.canvas);
    } else {
        let message = if let Some(error) = app.image_plane_last_error() {
            format!("Plane rendering failed.\n\n{error}")
        } else if app.image_plane_pending() {
            "Rendering plane...".to_string()
        } else {
            "Plane raster unavailable.".to_string()
        };
        frame.render_widget(
            Paragraph::new(message)
                .style(Style::default().fg(palette.footer_fg))
                .alignment(Alignment::Center)
                .wrap(Wrap { trim: false }),
            workspace.plane.canvas,
        );
    }

    if let Some(rect) = workspace.divider {
        let label = app
            .image_profile_title_line()
            .unwrap_or_else(|| "Spectrum".to_string());
        let text_width = usize::from(
            rect.width
                .saturating_sub(workspace.divider_toggle.map_or(0, |toggle| toggle.width)),
        );
        let label = truncate_to_width(&label, text_width);
        let label_rect = workspace.divider_toggle.map_or(rect, |toggle| Rect {
            x: rect.x,
            y: rect.y,
            width: rect.width.saturating_sub(toggle.width),
            height: rect.height,
        });
        frame.render_widget(
            Paragraph::new(label).style(Style::default().fg(palette.divider_fg)),
            label_rect,
        );
        if let Some(toggle_rect) = workspace.divider_toggle {
            let toggle = if app.image_spectrum_pane_collapsed() {
                "▸"
            } else {
                "▾"
            };
            frame.render_widget(
                Paragraph::new(toggle)
                    .style(Style::default().fg(palette.divider_fg))
                    .alignment(Alignment::Center),
                toggle_rect,
            );
        }
    }

    if let Some(canvas) = workspace.spectrum_canvas {
        if let Some(protocol) = app.image_spectrum_protocol() {
            frame.render_widget(PanelImage::new(protocol), canvas);
        } else {
            let message = if let Some(error) = app.image_spectrum_last_error() {
                format!("Spectrum rendering failed.\n\n{error}")
            } else if app.image_spectrum_pending() {
                "Rendering spectrum...".to_string()
            } else {
                "Spectrum unavailable.".to_string()
            };
            frame.render_widget(
                Paragraph::new(message)
                    .style(Style::default().fg(palette.footer_fg))
                    .alignment(Alignment::Center)
                    .wrap(Wrap { trim: false }),
                canvas,
            );
        }
    }
}

fn draw_path_chooser(frame: &mut Frame<'_>, app: &AppState, layout: &UiLayout, palette: Theme) {
    let area = path_chooser_area(layout.body);
    let list_area = path_chooser_list_area(area);
    frame.render_widget(Clear, area);
    let block = Block::default()
        .title(
            app.path_chooser_title()
                .unwrap_or_else(|| "Browse Path".to_string()),
        )
        .title_style(
            Style::default()
                .fg(palette.header_fg)
                .add_modifier(Modifier::BOLD),
        )
        .borders(Borders::ALL)
        .border_set(palette.border_set)
        .border_style(Style::default().fg(palette.active_pane_border_fg));
    let inner = block.inner(area);
    frame.render_widget(block, area);

    let cwd = app.path_chooser_cwd().unwrap_or_default();
    frame.render_widget(
        Paragraph::new(format!("cwd: {cwd}"))
            .style(Style::default().fg(palette.footer_fg))
            .wrap(Wrap { trim: false }),
        Rect {
            x: inner.x,
            y: inner.y,
            width: inner.width,
            height: 1,
        },
    );

    let entries = app.path_chooser_entries().unwrap_or_default();
    let selected = entries
        .iter()
        .position(|(_, selected)| *selected)
        .unwrap_or(0);
    let visible_height = list_area.height as usize;
    let visible_start = if entries.len() <= visible_height || visible_height == 0 {
        0
    } else {
        selected
            .saturating_sub(visible_height / 2)
            .min(entries.len().saturating_sub(visible_height))
    };
    let items = entries
        .into_iter()
        .skip(visible_start)
        .take(visible_height)
        .map(|(text, selected)| {
            let style = if selected {
                Style::default()
                    .fg(palette.active_tab_fg)
                    .bg(palette.active_tab_bg)
                    .add_modifier(Modifier::BOLD)
            } else {
                Style::default().fg(palette.footer_fg)
            };
            ListItem::new(text).style(style)
        })
        .collect::<Vec<_>>();
    frame.render_widget(List::new(items), list_area);

    if let Some(error) = app.path_chooser_error() {
        frame.render_widget(
            Paragraph::new(error).style(Style::default().fg(palette.status_error_fg)),
            Rect {
                x: inner.x,
                y: inner.y + inner.height.saturating_sub(2),
                width: inner.width,
                height: 1,
            },
        );
    }
    frame.render_widget(
        Paragraph::new("Enter/Space choose  Right/l open dir  Backspace parent  Arrows/jk move")
            .style(Style::default().fg(palette.footer_fg)),
        Rect {
            x: inner.x,
            y: inner.y + inner.height.saturating_sub(1),
            width: inner.width,
            height: 1,
        },
    );
}

fn render_visible_text_buffer(
    frame: &mut Frame<'_>,
    buffer: &VisibleTextBuffer,
    selection: Option<(usize, usize, usize, usize)>,
    palette: Theme,
) {
    let lines = buffer
        .lines
        .iter()
        .enumerate()
        .map(|(row, line)| render_visible_text_line(line, row, selection, palette))
        .collect::<Vec<_>>();
    frame.render_widget(
        Paragraph::new(lines).wrap(Wrap { trim: false }),
        buffer.area,
    );
}

fn render_visible_text_line(
    line: &VisibleTextLine,
    row: usize,
    selection: Option<(usize, usize, usize, usize)>,
    palette: Theme,
) -> Line<'static> {
    let selection_style = Style::default()
        .fg(palette.section_selected_fg)
        .bg(palette.section_selected_bg)
        .add_modifier(Modifier::BOLD);

    let mut spans = Vec::new();
    let mut current_text = String::new();
    let mut current_style = None;

    for (column, ch) in line.text.chars().enumerate() {
        let mut style = base_style_for_role(
            line.roles
                .get(column)
                .copied()
                .unwrap_or(VisibleTextRole::Plain),
            palette,
        );
        if selection_contains(selection, row, column) {
            style = selection_style;
        }
        if current_style == Some(style) {
            current_text.push(ch);
        } else {
            if let Some(existing) = current_style {
                spans.push(Span::styled(current_text.clone(), existing));
                current_text.clear();
            }
            current_style = Some(style);
            current_text.push(ch);
        }
    }

    if let Some(style) = current_style {
        spans.push(Span::styled(current_text, style));
    }

    Line::from(spans)
}

fn base_style_for_role(role: VisibleTextRole, palette: Theme) -> Style {
    match role {
        VisibleTextRole::Plain => Style::default(),
        VisibleTextRole::TableHeader => Style::default()
            .fg(palette.table_header_fg)
            .bg(palette.table_header_bg)
            .add_modifier(Modifier::BOLD),
        VisibleTextRole::BrowserSeparator => Style::default().fg(palette.divider_fg),
        VisibleTextRole::BrowserSelectedCell => Style::default()
            .fg(palette.field_selected_fg)
            .bg(palette.field_selected_bg)
            .add_modifier(Modifier::BOLD),
    }
}

fn selection_contains(
    selection: Option<(usize, usize, usize, usize)>,
    row: usize,
    column: usize,
) -> bool {
    let Some((row_start, row_end, col_start, col_end)) = selection else {
        return false;
    };
    row >= row_start && row <= row_end && column >= col_start && column <= col_end
}

fn visible_form_hits(area: Rect, app: &AppState) -> Vec<FormRowHit> {
    let rows = app.form_rows();
    let selected_row = rows.iter().position(|row| row.selected).unwrap_or(0);
    let visible_height = area.height as usize;
    let scroll = selected_row.saturating_sub(visible_height.saturating_sub(1));

    rows.iter()
        .skip(scroll)
        .take(visible_height)
        .enumerate()
        .map(|(offset, row)| FormRowHit {
            target: row.target,
            rect: Rect {
                x: area.x,
                y: area.y + offset as u16,
                width: area.width,
                height: 1,
            },
        })
        .collect()
}

fn visible_tab_hits(area: Rect, app: &AppState) -> Vec<TabHit> {
    if area.height == 0 || area.width == 0 {
        return Vec::new();
    }

    let palette = theme(app.theme_mode());
    let mut hits = Vec::new();
    let mut x = area.x;
    for tab in app.result_tabs() {
        let label = tab_label(*tab, *tab == app.active_result_tab(), palette);
        let width = label.chars().count() as u16;
        if x >= area.x + area.width {
            break;
        }
        let rect = Rect {
            x,
            y: area.y,
            width: width.min(area.x + area.width - x),
            height: 1,
        };
        hits.push(TabHit { tab: *tab, rect });
        x = x.saturating_add(width + 1);
    }
    hits
}

fn visible_browser_tab_hits(area: Rect, app: &AppState) -> Vec<BrowserTabHit> {
    if area.height == 0 || area.width == 0 {
        return Vec::new();
    }

    let palette = theme(app.theme_mode());
    let mut hits = Vec::new();
    let mut x = area.x;
    for tab in app.browser_tabs() {
        let active = app.active_browser_tab_label() == Some(tab.label());
        let label = browser_tab_label(*tab, active, palette);
        let width = label.chars().count() as u16;
        if x >= area.x + area.width {
            break;
        }
        let rect = Rect {
            x,
            y: area.y,
            width: width.min(area.x + area.width - x),
            height: 1,
        };
        hits.push(BrowserTabHit { tab: *tab, rect });
        x = x.saturating_add(width + 1);
    }
    hits
}

fn pane_border_style(theme: Theme, focus: PaneFocus, pane: PaneFocus) -> Style {
    if focus == pane {
        Style::default().fg(theme.active_pane_border_fg)
    } else {
        Style::default().fg(theme.pane_border_fg)
    }
}

fn form_block_widget(app: &AppState, palette: Theme) -> Block<'static> {
    Block::default()
        .title(app.parameter_title())
        .title_style(
            Style::default()
                .fg(palette.header_fg)
                .add_modifier(Modifier::BOLD),
        )
        .borders(Borders::ALL)
        .border_set(palette.border_set)
        .border_style(pane_border_style(
            palette,
            app.pane_focus(),
            PaneFocus::Parameters,
        ))
        .padding(Padding::new(1, 1, 0, 0))
}

fn result_block_widget(app: &AppState, palette: Theme) -> Block<'static> {
    Block::default()
        .title(app.result_title())
        .title_style(
            Style::default()
                .fg(palette.header_fg)
                .add_modifier(Modifier::BOLD),
        )
        .borders(Borders::ALL)
        .border_set(palette.border_set)
        .border_style(pane_border_style(
            palette,
            app.pane_focus(),
            PaneFocus::Result,
        ))
        .padding(Padding::new(1, 1, 0, 0))
}

fn header_line(app: &AppState, palette: Theme) -> Line<'static> {
    let mut spans = Vec::new();
    spans.push(Span::styled(
        "casars",
        Style::default()
            .fg(palette.header_fg)
            .add_modifier(Modifier::BOLD),
    ));
    spans.push(Span::styled(
        "  ◆  ",
        Style::default().fg(palette.header_dim_fg),
    ));
    spans.push(Span::styled(
        format!("{} / {}", app.app_category(), app.app_name()),
        Style::default()
            .fg(palette.header_fg)
            .add_modifier(Modifier::BOLD),
    ));
    Line::from(spans)
}

fn footer_line(text: &str, palette: Theme) -> Line<'static> {
    let mut spans = Vec::new();
    for (index, segment) in text.split("  ").enumerate() {
        if index > 0 {
            spans.push(Span::raw("  "));
        }
        if let Some((key, action)) = segment.split_once('=') {
            spans.push(Span::styled(
                format!("{key}="),
                Style::default()
                    .fg(palette.footer_key_fg)
                    .add_modifier(Modifier::BOLD),
            ));
            spans.push(Span::styled(
                action.to_string(),
                Style::default().fg(palette.footer_fg),
            ));
        } else {
            spans.push(Span::styled(
                segment.to_string(),
                Style::default()
                    .fg(palette.footer_key_fg)
                    .add_modifier(Modifier::BOLD),
            ));
        }
    }
    Line::from(spans)
}

fn draw_help_overlay(frame: &mut Frame<'_>, app: &AppState, palette: Theme) {
    let area = centered_rect(
        frame.area().width.saturating_mul(3) / 4,
        frame.area().height.saturating_mul(2) / 3,
        frame.area(),
    );
    frame.render_widget(Clear, area);
    let lines = app
        .help_overlay_lines()
        .into_iter()
        .map(Line::from)
        .collect::<Vec<_>>();
    let block = Block::default()
        .title("Help")
        .title_style(
            Style::default()
                .fg(palette.header_fg)
                .add_modifier(Modifier::BOLD),
        )
        .borders(Borders::ALL)
        .border_set(palette.border_set)
        .border_style(Style::default().fg(palette.active_pane_border_fg))
        .padding(Padding::new(1, 1, 0, 0));
    let inner = block.inner(area);
    frame.render_widget(block, area);
    frame.render_widget(
        Paragraph::new(lines)
            .wrap(Wrap { trim: false })
            .style(Style::default().fg(palette.footer_fg)),
        inner,
    );
}

fn render_form_row_text(
    row: &crate::app::FormRowView,
    focus: PaneFocus,
    palette: Theme,
    width: u16,
) -> String {
    match row.kind {
        FormRowKind::Section { collapsed } => {
            let disclosure = if collapsed {
                palette.section_closed_glyph
            } else {
                palette.section_open_glyph
            };
            fit_text(&format!("  {disclosure} {}", row.text), width as usize)
        }
        FormRowKind::Field => {
            let marker = if row.selected {
                if focus == PaneFocus::Parameters {
                    palette.selection_glyph
                } else {
                    palette.inactive_selection_glyph
                }
            } else {
                " "
            };
            let prefix = format!("{marker} ");
            let available = width as usize;
            if available <= prefix.chars().count() {
                fit_text(&prefix, available)
            } else {
                format!(
                    "{prefix}{}",
                    fit_text_preserving_suffix(
                        &row.text,
                        available - prefix.chars().count(),
                        " [browse]"
                    )
                )
            }
        }
    }
}

fn render_manager_row_text(
    row: &crate::app::BrowserManagerRowView<crate::app::BrowserPaneSelection>,
    focus: PaneFocus,
    palette: Theme,
    width: u16,
) -> String {
    let marker = if row.selected {
        if focus == PaneFocus::Parameters {
            palette.selection_glyph
        } else {
            palette.inactive_selection_glyph
        }
    } else {
        " "
    };
    let prefix = format!("{marker} ");
    let available = width as usize;
    if available <= prefix.chars().count() {
        fit_text(&prefix, available)
    } else {
        format!(
            "{}{}",
            prefix,
            fit_text(&row.display_text(), available - prefix.chars().count())
        )
    }
}

fn tab_label(tab: ResultTab, active: bool, palette: Theme) -> String {
    let short = match tab {
        ResultTab::Overview => "Overview",
        ResultTab::Observations => "Obs",
        ResultTab::Scans => "Scans",
        ResultTab::Fields => "Fields",
        ResultTab::Spws => "SPWs",
        ResultTab::Sources => "Sources",
        ResultTab::Antennas => "Ant",
        ResultTab::Plots => "Plots",
        ResultTab::Stdout => "Out",
        ResultTab::Stderr => "Err",
    };
    if active {
        format!("◖ {} ◗", tab.label())
    } else if palette.selection_glyph == "▌" {
        format!("·{short}·")
    } else {
        format!("[{}]", tab.label())
    }
}

fn build_plot_workspace_layout(area: Rect, app: &AppState) -> PlotWorkspaceLayout {
    if app.is_msexplore_app() {
        let horizontal = Layout::default()
            .direction(Direction::Horizontal)
            .constraints([Constraint::Length(42), Constraint::Min(30)])
            .split(area);

        let catalog_block = horizontal[0];
        let canvas_block = horizontal[1];
        let controls_block = Rect::new(0, 0, 0, 0);
        let catalog_inner = Block::default().borders(Borders::ALL).inner(catalog_block);
        let canvas_inner = Block::default().borders(Borders::ALL).inner(canvas_block);
        let controls_inner = Rect::new(0, 0, 0, 0);

        let visible_catalog = visible_plot_catalog_rows(
            app.plot_catalog_rows(),
            available_msexplore_catalog_height(catalog_inner.height, app),
        );
        let catalog_hits = visible_catalog
            .rows
            .into_iter()
            .enumerate()
            .map(|(index, row)| PlotCatalogHit {
                tab: row,
                rect: Rect {
                    x: catalog_inner.x,
                    y: catalog_inner.y
                        + 1
                        + u16::from(visible_catalog.hidden_above > 0)
                        + index as u16,
                    width: catalog_inner.width,
                    height: 1,
                },
            })
            .collect::<Vec<_>>();

        let action_rows_start = msexplore_action_row_start(
            catalog_inner.y,
            catalog_hits.len(),
            visible_catalog.hidden_above > 0,
            visible_catalog.hidden_below > 0,
            app,
        );
        let control_hits = app
            .plot_control_rows()
            .into_iter()
            .enumerate()
            .filter_map(|(index, row)| {
                let y = action_rows_start + index as u16;
                ((y - catalog_inner.y) < catalog_inner.height).then_some(PlotControlHit {
                    target: row.target,
                    rect: Rect {
                        x: catalog_inner.x,
                        y,
                        width: catalog_inner.width,
                        height: 1,
                    },
                })
            })
            .collect::<Vec<_>>();

        return PlotWorkspaceLayout {
            catalog_block,
            catalog_inner,
            catalog_hits,
            canvas_block,
            canvas_inner,
            controls_block,
            controls_inner,
            control_hits,
        };
    }

    let horizontal = Layout::default()
        .direction(Direction::Horizontal)
        .constraints([Constraint::Length(28), Constraint::Min(30)])
        .split(area);
    let right = Layout::default()
        .direction(Direction::Vertical)
        .constraints([Constraint::Min(12), Constraint::Length(11)])
        .split(horizontal[1]);

    let catalog_block = horizontal[0];
    let canvas_block = right[0];
    let controls_block = right[1];
    let catalog_inner = Block::default().borders(Borders::ALL).inner(catalog_block);
    let canvas_inner = Block::default().borders(Borders::ALL).inner(canvas_block);
    let controls_inner = Block::default().borders(Borders::ALL).inner(controls_block);

    let catalog_hits = app
        .plot_catalog_rows()
        .into_iter()
        .enumerate()
        .filter_map(|(index, row)| {
            (index < catalog_inner.height as usize).then_some(PlotCatalogHit {
                tab: row,
                rect: Rect {
                    x: catalog_inner.x,
                    y: catalog_inner.y + index as u16,
                    width: catalog_inner.width,
                    height: 1,
                },
            })
        })
        .collect::<Vec<_>>();

    let control_hits = app
        .plot_control_rows()
        .into_iter()
        .enumerate()
        .filter_map(|(index, row)| {
            let offset = if app.plot_dirty_banner().is_some() {
                2usize
            } else {
                0usize
            };
            let y = controls_inner.y + offset as u16 + index as u16;
            ((offset + index) < controls_inner.height as usize).then_some(PlotControlHit {
                target: row.target,
                rect: Rect {
                    x: controls_inner.x,
                    y,
                    width: controls_inner.width,
                    height: 1,
                },
            })
        })
        .collect::<Vec<_>>();

    PlotWorkspaceLayout {
        catalog_block,
        catalog_inner,
        catalog_hits,
        canvas_block,
        canvas_inner,
        controls_block,
        controls_inner,
        control_hits,
    }
}

pub(crate) fn plot_canvas_area(layout: &UiLayout) -> Option<Rect> {
    layout
        .plot_workspace
        .as_ref()
        .map(|workspace| workspace.canvas_inner)
}

fn visible_plot_catalog_rows(
    rows: Vec<crate::app::PlotCatalogRowView>,
    available_height: usize,
) -> VisiblePlotCatalog {
    if available_height == 0 || rows.is_empty() {
        return VisiblePlotCatalog {
            rows: Vec::new(),
            hidden_above: 0,
            hidden_below: 0,
        };
    }
    if rows.len() <= available_height {
        return VisiblePlotCatalog {
            rows,
            hidden_above: 0,
            hidden_below: 0,
        };
    }
    let selected_index = rows.iter().position(|row| row.selected).unwrap_or(0);
    let half_window = available_height / 2;
    let total_rows = rows.len();
    let max_start = total_rows.saturating_sub(available_height);
    let start = selected_index.saturating_sub(half_window).min(max_start);
    let end = start + available_height;
    VisiblePlotCatalog {
        rows: rows
            .into_iter()
            .skip(start)
            .take(available_height)
            .collect(),
        hidden_above: start,
        hidden_below: total_rows.saturating_sub(end),
    }
}

fn available_msexplore_catalog_height(sidebar_height: u16, app: &AppState) -> usize {
    let reserved_lines = 1usize
        + 1usize
        + usize::from(app.plot_dirty_banner().is_some()) * 2usize
        + 1usize
        + app.plot_control_rows().len();
    usize::from(sidebar_height).saturating_sub(reserved_lines)
}

fn msexplore_action_row_start(
    catalog_y: u16,
    catalog_row_count: usize,
    has_hidden_above: bool,
    has_hidden_below: bool,
    app: &AppState,
) -> u16 {
    let mut y = catalog_y + 1 + u16::from(has_hidden_above) + catalog_row_count as u16;
    if has_hidden_below {
        y += 1;
    }
    y += 1;
    if app.plot_dirty_banner().is_some() {
        y += 2;
    }
    y + 1
}

fn divider_toggle_rect(divider: Rect) -> Option<Rect> {
    if divider.width == 0 || divider.height == 0 {
        return None;
    }
    Some(Rect {
        x: divider.x,
        y: divider.y,
        width: divider.width,
        height: 1,
    })
}

fn image_workspace_divider_toggle_rect(divider: Rect) -> Option<Rect> {
    if divider.width < 3 || divider.height == 0 {
        return None;
    }
    Some(Rect {
        x: divider.x + divider.width.saturating_sub(3),
        y: divider.y,
        width: 3,
        height: 1,
    })
}

fn truncate_to_width(text: &str, width: usize) -> String {
    if width == 0 {
        return String::new();
    }
    text.chars().take(width).collect()
}

fn browser_tab_label(tab: BrowserTab, active: bool, palette: Theme) -> String {
    let short = match tab {
        BrowserTab::Overview => "Overview",
        BrowserTab::Columns => "Cols",
        BrowserTab::Keywords => "Keys",
        BrowserTab::Cells => "Cells",
        BrowserTab::Subtables => "Links",
        BrowserTab::Plane => "Plane",
        BrowserTab::Spectrum => "Spec",
        BrowserTab::Metadata => "Meta",
        BrowserTab::Coordinates => "Coords",
    };
    if active {
        format!("◖ {} ◗", tab.label())
    } else if palette.selection_glyph == "▌" {
        format!("·{short}·")
    } else {
        format!("[{}]", tab.label())
    }
}

fn result_scrollbar_state(
    content: &ResultContent,
    scroll: u16,
    area: Rect,
) -> Option<ScrollbarState> {
    let (content_length, viewport_length) = match content {
        ResultContent::Lines(lines) => (lines.len(), area.height as usize),
        ResultContent::Table(table) => (table.rows.len(), area.height.saturating_sub(1) as usize),
        ResultContent::Graphic(_) => return None,
    };

    if content_length <= viewport_length || viewport_length == 0 {
        return None;
    }

    Some(
        ScrollbarState::new(content_length)
            .position(scroll as usize)
            .viewport_content_length(viewport_length),
    )
}

fn result_hscrollbar_state(
    content: &ResultContent,
    scroll: u16,
    area: Rect,
) -> Option<ScrollbarState> {
    let content_width = match content {
        ResultContent::Lines(lines) => lines
            .iter()
            .map(|line| line.chars().count())
            .max()
            .unwrap_or(0),
        ResultContent::Table(table) => table.content_width(),
        ResultContent::Graphic(_) => return None,
    };
    let viewport_width = area.width as usize;
    if viewport_width == 0 || content_width <= viewport_width {
        return None;
    }
    Some(
        ScrollbarState::new(content_width)
            .position(scroll as usize)
            .viewport_content_length(viewport_width),
    )
}

fn scrollbar_state_from_metrics(
    metrics: Option<(usize, usize)>,
    scroll: u16,
) -> Option<ScrollbarState> {
    let (content_length, viewport_length) = metrics?;
    if content_length <= viewport_length || viewport_length == 0 {
        return None;
    }
    Some(
        ScrollbarState::new(content_length)
            .position(scroll as usize)
            .viewport_content_length(viewport_length),
    )
}

fn browser_scrollbar_state(app: &AppState, area: Rect) -> Option<ScrollbarState> {
    scrollbar_state_from_metrics(
        app.active_browser_scroll_metrics(area.height),
        app.active_browser_scroll(),
    )
}

fn browser_hscrollbar_state(app: &AppState, area: Rect) -> Option<ScrollbarState> {
    scrollbar_state_from_metrics(
        app.active_browser_hscroll_metrics(area.width),
        app.active_browser_hscroll(),
    )
}

fn content_viewport_area(area: Rect, has_vertical: bool, has_horizontal: bool) -> Rect {
    Rect {
        x: area.x,
        y: area.y,
        width: area.width.saturating_sub(if has_vertical { 1 } else { 0 }),
        height: area
            .height
            .saturating_sub(if has_horizontal { 1 } else { 0 }),
    }
}

pub(crate) fn image_plane_canvas_layout(area: Rect) -> ImagePlaneCanvasLayout {
    ImagePlaneCanvasLayout { canvas: area }
}

#[cfg(test)]
pub(crate) fn image_plane_canvas_area(layout: &UiLayout) -> Rect {
    image_plane_canvas_layout(layout.result_content).canvas
}

pub(crate) fn image_plane_workspace_layout(
    area: Rect,
    show_spectrum: bool,
    split_ratio: f32,
) -> ImagePlaneWorkspaceLayout {
    if !show_spectrum || area.height < 4 {
        return ImagePlaneWorkspaceLayout {
            plane: image_plane_canvas_layout(area),
            divider: None,
            divider_toggle: None,
            spectrum_canvas: None,
        };
    }

    let fixed_rows = 1u16;
    let available_canvas = area.height.saturating_sub(fixed_rows);
    if available_canvas < 3 {
        return ImagePlaneWorkspaceLayout {
            plane: image_plane_canvas_layout(area),
            divider: None,
            divider_toggle: None,
            spectrum_canvas: None,
        };
    }

    let collapsed = split_ratio >= 1.0 || available_canvas < 6;
    let plane_canvas_height = if collapsed {
        available_canvas
    } else {
        ((available_canvas as f32) * split_ratio)
            .round()
            .clamp(3.0, f32::from(available_canvas.saturating_sub(3))) as u16
    };
    let spectrum_canvas_height = available_canvas.saturating_sub(plane_canvas_height);
    let plane = ImagePlaneCanvasLayout {
        canvas: Rect {
            x: area.x,
            y: area.y,
            width: area.width,
            height: plane_canvas_height,
        },
    };
    let divider_y = plane.canvas.y + plane.canvas.height;
    let divider = Rect {
        x: area.x,
        y: divider_y,
        width: area.width,
        height: 1,
    };
    ImagePlaneWorkspaceLayout {
        plane,
        divider: Some(divider),
        divider_toggle: image_workspace_divider_toggle_rect(divider),
        spectrum_canvas: (!collapsed && spectrum_canvas_height > 0).then_some(Rect {
            x: area.x,
            y: divider_y + 1,
            width: area.width,
            height: spectrum_canvas_height,
        }),
    }
}

pub(crate) fn image_plane_canvas_area_for_browser(
    layout: &UiLayout,
    show_spectrum: bool,
    split_ratio: f32,
) -> Rect {
    image_plane_workspace_layout(layout.result_content, show_spectrum, split_ratio)
        .plane
        .canvas
}

pub(crate) fn image_spectrum_canvas_area(
    layout: &UiLayout,
    show_spectrum: bool,
    split_ratio: f32,
) -> Option<Rect> {
    image_plane_workspace_layout(layout.result_content, show_spectrum, split_ratio).spectrum_canvas
}

pub(crate) fn image_workspace_divider_area(
    layout: &UiLayout,
    show_spectrum: bool,
    split_ratio: f32,
) -> Option<Rect> {
    image_plane_workspace_layout(layout.result_content, show_spectrum, split_ratio).divider
}

pub(crate) fn image_workspace_divider_toggle_area(
    layout: &UiLayout,
    show_spectrum: bool,
    split_ratio: f32,
) -> Option<Rect> {
    image_plane_workspace_layout(layout.result_content, show_spectrum, split_ratio).divider_toggle
}

fn result_scrollbar_rect(app: &AppState, area: Rect) -> Option<Rect> {
    if app.browser_is_active() {
        browser_scrollbar_state(app, area)?;
        let has_horizontal = browser_hscrollbar_state(app, area).is_some();
        return Some(Rect {
            x: area.x + area.width.saturating_sub(1),
            y: area.y,
            width: 1,
            height: area
                .height
                .saturating_sub(if has_horizontal { 1 } else { 0 }),
        });
    }

    let content = app.active_result_content();
    let scroll = app.active_result_scroll();
    result_scrollbar_state(&content, scroll, area)?;
    let has_horizontal =
        result_hscrollbar_state(&content, app.active_result_hscroll(), area).is_some();
    Some(Rect {
        x: area.x + area.width.saturating_sub(1),
        y: area.y,
        width: 1,
        height: area
            .height
            .saturating_sub(if has_horizontal { 1 } else { 0 }),
    })
}

fn result_hscrollbar_rect(app: &AppState, area: Rect) -> Option<Rect> {
    if app.browser_is_active() {
        browser_hscrollbar_state(app, area)?;
        let has_vertical = browser_scrollbar_state(app, area).is_some();
        return Some(Rect {
            x: area.x,
            y: area.y + area.height.saturating_sub(1),
            width: area.width.saturating_sub(if has_vertical { 1 } else { 0 }),
            height: 1,
        });
    }

    let content = app.active_result_content();
    let hscroll = app.active_result_hscroll();
    result_hscrollbar_state(&content, hscroll, area)?;
    let has_vertical = result_scrollbar_state(&content, app.active_result_scroll(), area).is_some();
    Some(Rect {
        x: area.x,
        y: area.y + area.height.saturating_sub(1),
        width: area.width.saturating_sub(if has_vertical { 1 } else { 0 }),
        height: 1,
    })
}

fn rect_contains(rect: Rect, column: u16, row: u16) -> bool {
    column >= rect.x
        && column < rect.x.saturating_add(rect.width)
        && row >= rect.y
        && row < rect.y.saturating_add(rect.height)
}

fn centered_rect(width: u16, height: u16, area: Rect) -> Rect {
    let width = width.min(area.width);
    let height = height.min(area.height);
    Rect {
        x: area.x + area.width.saturating_sub(width) / 2,
        y: area.y + area.height.saturating_sub(height) / 2,
        width,
        height,
    }
}

pub(crate) fn path_chooser_area(body: Rect) -> Rect {
    centered_rect(body.width.min(84), body.height.min(24), body)
}

pub(crate) fn path_chooser_list_area(area: Rect) -> Rect {
    let inner = Rect {
        x: area.x.saturating_add(1),
        y: area.y.saturating_add(1),
        width: area.width.saturating_sub(2),
        height: area.height.saturating_sub(2),
    };
    Rect {
        x: inner.x,
        y: inner.y.saturating_add(1),
        width: inner.width,
        height: inner.height.saturating_sub(3),
    }
}

fn fit_text(text: &str, width: usize) -> String {
    if width == 0 {
        return String::new();
    }
    if text.chars().count() <= width {
        return text.to_string();
    }
    if width <= 3 {
        return ".".repeat(width);
    }
    let mut out = text.chars().take(width - 3).collect::<String>();
    out.push_str("...");
    out
}

fn fit_text_preserving_suffix(text: &str, width: usize, suffix: &str) -> String {
    if width == 0 {
        return String::new();
    }
    if text.chars().count() <= width {
        return text.to_string();
    }
    let Some(prefix) = text.strip_suffix(suffix) else {
        return fit_text(text, width);
    };
    let suffix_width = suffix.chars().count();
    if width <= suffix_width {
        return fit_text(suffix, width);
    }
    format!("{}{}", fit_text(prefix, width - suffix_width), suffix)
}

#[cfg(test)]
mod tests {
    use super::fit_text_preserving_suffix;

    #[test]
    fn preserves_browse_suffix_when_truncating() {
        let rendered = fit_text_preserving_suffix(
            "MeasurementSet Path /very/long/path/to/data.ms [browse]",
            32,
            " [browse]",
        );
        assert!(rendered.ends_with(" [browse]"));
        assert!(rendered.contains("..."));
    }

    #[test]
    fn falls_back_to_normal_fit_without_suffix() {
        let rendered = fit_text_preserving_suffix("abcdef", 5, " [browse]");
        assert_eq!(rendered, "ab...");
    }
}

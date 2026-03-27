// SPDX-License-Identifier: LGPL-3.0-or-later
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
    AppState, BrowserTab, FormRowKind, FormSelection, OutputPane, PaneFocus, ResultContent,
    ResultTab, VisibleTextBuffer, VisibleTextLine, VisibleTextRole,
};
use crate::config::ThemeMode;
use crate::registry::RegistryApp;
use crate::theme::{Theme, theme};

#[derive(Debug, Clone)]
pub(crate) struct UiLayout {
    pub header: Rect,
    pub body: Rect,
    pub footer: Rect,
    pub form_block: Rect,
    pub form_inner: Rect,
    pub form_rows: Vec<FormRowHit>,
    pub divider: Rect,
    pub result_block: Rect,
    pub result_status: Rect,
    pub result_tabs: Rect,
    pub result_content: Rect,
    pub result_scrollbar: Option<Rect>,
    pub result_hscrollbar: Option<Rect>,
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

impl UiLayout {
    pub(crate) fn form_target_at(&self, column: u16, row: u16) -> Option<FormSelection> {
        self.form_rows
            .iter()
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
    let divider_width = if collapsed { 0 } else { 1 };
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

    UiLayout {
        header: vertical[0],
        body,
        footer: vertical[2],
        form_block,
        form_inner,
        form_rows,
        divider,
        result_block,
        result_status,
        result_tabs,
        result_content,
        result_scrollbar,
        result_hscrollbar,
        tab_hits,
        browser_tab_hits,
    }
}

pub(crate) fn draw(frame: &mut Frame<'_>, app: &AppState, layout: &UiLayout) {
    let palette = theme(app.theme_mode());

    let header = Paragraph::new(header_line(app, palette));
    frame.render_widget(header, layout.header);

    let footer = Paragraph::new(footer_line(app.footer_text(), palette));
    frame.render_widget(footer, layout.footer);

    let divider_lines = (0..layout.divider.height)
        .map(|_| Line::from(palette.divider_glyph))
        .collect::<Vec<_>>();
    let divider = Paragraph::new(divider_lines).style(Style::default().fg(palette.divider_fg));
    frame.render_widget(divider, layout.divider);

    draw_form(frame, app, layout, palette);
    draw_result(frame, app, layout, palette);
    if app.path_chooser_active() {
        draw_path_chooser(frame, app, layout, palette);
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

    if app.browser_is_active() {
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
        draw_graphic_result(frame, app, layout, palette, summary);
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

    if let Some(protocol) = app.uv_plot_protocol() {
        frame.render_widget(PanelImage::new(protocol), content_area);
    } else {
        let message = if let Some(error) = app.uv_plot_last_error() {
            format!("{summary}\n\n{error}")
        } else if app.uv_plot_pending() {
            format!("{summary}\n\nRendering UV plot...")
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

fn tab_label(tab: ResultTab, active: bool, palette: Theme) -> String {
    let short = match tab {
        ResultTab::Overview => "Overview",
        ResultTab::Observations => "Obs",
        ResultTab::Scans => "Scans",
        ResultTab::Fields => "Fields",
        ResultTab::Spws => "SPWs",
        ResultTab::Sources => "Sources",
        ResultTab::Antennas => "Ant",
        ResultTab::Uv => "UV",
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

fn browser_tab_label(tab: BrowserTab, active: bool, palette: Theme) -> String {
    let short = match tab {
        BrowserTab::Overview => "Overview",
        BrowserTab::Columns => "Cols",
        BrowserTab::Keywords => "Keys",
        BrowserTab::Cells => "Cells",
        BrowserTab::Subtables => "Links",
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
    use ratatui::layout::Rect;
    use ratatui::style::Modifier;
    use ratatui::widgets::ScrollbarState;

    use super::{
        UiLayout, browser_tab_label, centered_rect, compute_layout, content_viewport_area,
        fit_text, fit_text_preserving_suffix, footer_line, path_chooser_area,
        path_chooser_list_area, rect_contains, render_form_row_text, render_visible_text_line,
        result_hscrollbar_state, result_scrollbar_state, selection_contains, tab_label,
    };
    use crate::app::{
        AppState, BrowserTab, FormRowKind, FormRowView, PaneFocus, ResultContent, ResultTab,
        TableView, VisibleTextLine, VisibleTextRole,
    };
    use crate::config::ThemeMode;
    use crate::registry::listobs_app;
    use crate::theme::theme;
    use casacore_ms::listobs::cli::command_schema;

    fn test_app() -> AppState {
        AppState::from_schema(listobs_app(), command_schema("listobs"))
    }

    #[test]
    fn fit_text_handles_zero_and_short_widths() {
        assert_eq!(fit_text("abcdef", 0), "");
        assert_eq!(fit_text("abcdef", 2), "..");
        assert_eq!(fit_text("abcdef", 3), "...");
        assert_eq!(fit_text("abcdef", 5), "ab...");
        assert_eq!(fit_text("abc", 5), "abc");
    }

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

    #[test]
    fn footer_line_splits_key_action_segments() {
        let line = footer_line("q=quit  Enter launch  arrows", theme(ThemeMode::DenseAnsi));
        let rendered = line
            .spans
            .iter()
            .map(|span| span.content.as_ref())
            .collect::<String>();
        assert_eq!(rendered, "q=quit  Enter launch  arrows");
        assert_eq!(
            line.spans[0].style.fg,
            Some(theme(ThemeMode::DenseAnsi).footer_key_fg)
        );
        assert!(line.spans[0].style.add_modifier.contains(Modifier::BOLD));
        assert_eq!(
            line.spans[1].style.fg,
            Some(theme(ThemeMode::DenseAnsi).footer_fg)
        );
    }

    #[test]
    fn render_form_row_text_formats_sections_and_fields() {
        let palette = theme(ThemeMode::DenseAnsi);
        let section = FormRowView {
            target: crate::app::FormSelection::Section(0),
            text: "Input".to_string(),
            kind: FormRowKind::Section { collapsed: false },
            selected: false,
        };
        assert_eq!(
            render_form_row_text(&section, PaneFocus::Parameters, palette, 40),
            "  [-] Input"
        );

        let field = FormRowView {
            target: crate::app::FormSelection::Field(0),
            text: "MeasurementSet Path [browse]".to_string(),
            kind: FormRowKind::Field,
            selected: true,
        };
        let selected = render_form_row_text(&field, PaneFocus::Parameters, palette, 20);
        assert!(selected.starts_with("> "));
        assert!(selected.ends_with("[browse]"));

        let inactive = render_form_row_text(&field, PaneFocus::Result, palette, 20);
        assert!(inactive.starts_with("  "));
    }

    #[test]
    fn result_tab_and_browser_tab_labels_follow_theme_rules() {
        let dense = theme(ThemeMode::DenseAnsi);
        let rich = theme(ThemeMode::RichPanel);
        assert_eq!(tab_label(ResultTab::Overview, true, dense), "◖ Overview ◗");
        assert_eq!(tab_label(ResultTab::Stdout, false, dense), "[Stdout]");
        assert_eq!(tab_label(ResultTab::Stdout, false, rich), "·Out·");
        assert_eq!(
            browser_tab_label(BrowserTab::Overview, true, dense),
            "◖ Overview ◗"
        );
        assert_eq!(
            browser_tab_label(BrowserTab::Subtables, false, dense),
            "[Subtables]"
        );
        assert_eq!(
            browser_tab_label(BrowserTab::Subtables, false, rich),
            "·Links·"
        );
    }

    #[test]
    fn selection_contains_and_rect_contains_respect_bounds() {
        assert!(selection_contains(Some((1, 2, 3, 4)), 1, 3));
        assert!(selection_contains(Some((1, 2, 3, 4)), 2, 4));
        assert!(!selection_contains(Some((1, 2, 3, 4)), 0, 3));
        assert!(!selection_contains(Some((1, 2, 3, 4)), 1, 5));

        let rect = Rect::new(2, 4, 3, 2);
        assert!(rect_contains(rect, 2, 4));
        assert!(rect_contains(rect, 4, 5));
        assert!(!rect_contains(rect, 5, 5));
        assert!(!rect_contains(rect, 4, 6));
    }

    #[test]
    fn render_visible_text_line_groups_styles_and_selection() {
        let palette = theme(ThemeMode::DenseAnsi);
        let line = VisibleTextLine {
            text: "ABC".to_string(),
            roles: vec![
                VisibleTextRole::Plain,
                VisibleTextRole::TableHeader,
                VisibleTextRole::BrowserSeparator,
            ],
        };
        let rendered = render_visible_text_line(&line, 0, Some((0, 0, 2, 2)), palette);
        assert_eq!(
            rendered
                .spans
                .iter()
                .map(|span| span.content.as_ref())
                .collect::<String>(),
            "ABC"
        );
        assert_eq!(rendered.spans.len(), 3);
        assert_eq!(rendered.spans[0].style.fg, None);
        assert_eq!(rendered.spans[1].style.fg, Some(palette.table_header_fg));
        assert_eq!(
            rendered.spans[2].style.bg,
            Some(palette.section_selected_bg)
        );
        assert!(
            rendered.spans[2]
                .style
                .add_modifier
                .contains(Modifier::BOLD)
        );
    }

    #[test]
    fn scrollbar_helpers_cover_lines_tables_and_graphics() {
        let lines = ResultContent::Lines(vec!["one".into(), "two".into(), "three".into()]);
        let vscroll =
            result_scrollbar_state(&lines, 1, Rect::new(0, 0, 10, 2)).expect("lines scrollbar");
        assert_eq!(
            vscroll,
            ScrollbarState::new(3)
                .position(1)
                .viewport_content_length(2)
        );

        let table = ResultContent::Table(TableView {
            header: "abcd".into(),
            rows: vec!["row1".into(), "row2".into(), "row3".into()],
        });
        let hscroll =
            result_hscrollbar_state(&table, 2, Rect::new(0, 0, 3, 2)).expect("table hscroll");
        assert_eq!(
            hscroll,
            ScrollbarState::new(4)
                .position(2)
                .viewport_content_length(3)
        );

        assert!(
            result_scrollbar_state(
                &ResultContent::Graphic("uv".into()),
                0,
                Rect::new(0, 0, 4, 4)
            )
            .is_none()
        );
        assert!(
            result_hscrollbar_state(
                &ResultContent::Graphic("uv".into()),
                0,
                Rect::new(0, 0, 4, 4)
            )
            .is_none()
        );
    }

    #[test]
    fn viewport_and_centering_helpers_adjust_dimensions() {
        assert_eq!(
            content_viewport_area(Rect::new(1, 2, 10, 5), true, true),
            Rect::new(1, 2, 9, 4)
        );
        assert_eq!(
            centered_rect(20, 10, Rect::new(0, 0, 12, 6)),
            Rect::new(0, 0, 12, 6)
        );
        assert_eq!(
            centered_rect(6, 2, Rect::new(10, 10, 20, 10)),
            Rect::new(17, 14, 6, 2)
        );
    }

    #[test]
    fn path_chooser_geometry_stays_within_body() {
        let body = Rect::new(0, 0, 120, 40);
        let chooser = path_chooser_area(body);
        assert_eq!(chooser.width, 84);
        assert_eq!(chooser.height, 24);
        let list = path_chooser_list_area(chooser);
        assert_eq!(list.x, chooser.x + 1);
        assert_eq!(list.y, chooser.y + 2);
        assert_eq!(list.width, chooser.width - 2);
        assert_eq!(list.height, chooser.height - 5);
    }

    #[test]
    fn compute_layout_provides_interactive_hit_regions() {
        let app = test_app();
        let layout: UiLayout = compute_layout(Rect::new(0, 0, 120, 40), &app);
        assert!(layout.form_block.width > 0);
        assert!(layout.result_block.width > 0);
        assert!(!layout.form_rows.is_empty());
        assert!(
            layout
                .form_target_at(layout.form_rows[0].rect.x, layout.form_rows[0].rect.y)
                .is_some()
        );
        assert!(layout.in_form_block(layout.form_block.x, layout.form_block.y));
        assert!(layout.in_result_block(layout.result_block.x, layout.result_block.y));
        assert!(layout.in_divider(layout.divider.x, layout.divider.y));
        assert!(!layout.tab_hits.is_empty());
        assert_eq!(
            layout.result_tab_at(layout.tab_hits[0].rect.x, layout.tab_hits[0].rect.y),
            Some(ResultTab::Overview)
        );
    }

    #[test]
    fn layout_exposes_scrollbar_hit_regions_when_result_is_scrollable() {
        let app = test_app();
        let layout = compute_layout(Rect::new(0, 0, 80, 20), &app);
        if let Some(rect) = layout.result_scrollbar {
            assert!(layout.in_result_scrollbar(rect.x, rect.y));
        }
        if let Some(rect) = layout.result_hscrollbar {
            assert!(layout.in_result_hscrollbar(rect.x, rect.y));
        }
    }
}

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
            fit_text(&format!("{marker} {}", row.text), width as usize)
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

// SPDX-License-Identifier: LGPL-3.0-or-later

use super::*;

pub(super) fn draw_browser_mode_picker(
    frame: &mut Frame<'_>,
    app: &AppState,
    layout: &UiLayout,
    palette: Theme,
) {
    let area = browser_mode_picker_area(layout.browser_mode_selector, layout.form_block);
    let list_area = browser_mode_picker_list_area(area);
    frame.render_widget(Clear, area);
    let block = Block::default()
        .title("Choose Left Pane View")
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
    let selected = app.browser_mode_picker_selection();
    let items = crate::app::ImageBrowserLeftPaneMode::all()
        .into_iter()
        .map(|mode| {
            let style = if Some(mode) == selected {
                Style::default()
                    .fg(palette.field_selected_fg)
                    .bg(palette.field_selected_bg)
            } else {
                Style::default().fg(palette.footer_fg)
            };
            ListItem::new(Line::from(format!("  {}", mode.label()))).style(style)
        })
        .collect::<Vec<_>>();
    frame.render_widget(List::new(items), list_area);
    frame.render_widget(
        Paragraph::new("Enter/click choose  Esc cancel  Arrows/jk move")
            .style(Style::default().fg(palette.footer_fg)),
        Rect {
            x: inner.x,
            y: inner.y + inner.height.saturating_sub(1),
            width: inner.width,
            height: 1,
        },
    );
}

pub(super) fn visible_browser_manager_hits(
    form_inner: Rect,
    form_row_count: usize,
    selector: Option<Rect>,
    app: &AppState,
) -> Vec<FormRowHit> {
    let Some(selector) = selector else {
        return Vec::new();
    };
    let rows = app.browser_manager_rows();
    if rows.is_empty() {
        return Vec::new();
    }
    let form_rows_height = form_row_count.min(form_inner.height as usize) as u16;
    let gap = u16::from(form_rows_height > 0);
    let summary_area = Rect {
        x: form_inner.x,
        y: form_inner
            .y
            .saturating_add(form_rows_height)
            .saturating_add(gap),
        width: form_inner.width,
        height: form_inner
            .height
            .saturating_sub(form_rows_height.saturating_add(gap)),
    };
    let start_y = selector.y.saturating_add(selector.height).saturating_add(1);
    let available_height = summary_area
        .y
        .saturating_add(summary_area.height)
        .saturating_sub(start_y);
    rows.into_iter()
        .take(available_height as usize)
        .enumerate()
        .map(|(offset, row)| FormRowHit {
            target: FormSelection::BrowserPane(row.target),
            rect: Rect {
                x: summary_area.x,
                y: start_y + offset as u16,
                width: summary_area.width,
                height: 1,
            },
        })
        .collect()
}

pub(crate) fn browser_mode_picker_area(anchor: Option<Rect>, bounds: Rect) -> Rect {
    crate::pane_manager::selector_popup_area(
        anchor,
        bounds,
        crate::app::ImageBrowserLeftPaneMode::all().len(),
    )
}

pub(crate) fn browser_mode_picker_list_area(area: Rect) -> Rect {
    crate::pane_manager::selector_popup_list_area(area)
}

pub(super) fn browser_mode_selector_rect(
    form_inner: Rect,
    form_rows_len: usize,
    app: &AppState,
) -> Option<Rect> {
    if !(app.browser_is_active() && app.browser_uses_parameter_pane()) {
        return None;
    }
    let rows_height = form_rows_len.min(form_inner.height as usize) as u16;
    let gap = u16::from(rows_height > 0);
    let summary_area = Rect {
        x: form_inner.x,
        y: form_inner.y.saturating_add(rows_height).saturating_add(gap),
        width: form_inner.width,
        height: form_inner
            .height
            .saturating_sub(rows_height.saturating_add(gap)),
    };
    if summary_area.height == 0 {
        return None;
    }
    Some(Rect {
        x: summary_area.x,
        y: summary_area.y,
        width: summary_area.width,
        height: 1,
    })
}

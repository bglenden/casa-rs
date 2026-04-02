// SPDX-License-Identifier: LGPL-3.0-or-later

use ratatui::layout::Rect;

#[derive(Debug, Clone)]
pub(crate) struct PaneManagerRowView<T> {
    pub target: T,
    pub label: String,
    pub checked: Option<bool>,
    pub selected: bool,
}

impl<T> PaneManagerRowView<T> {
    pub(crate) fn display_text(&self) -> String {
        match self.checked {
            Some(checked) => format!("{} {}", if checked { "[x]" } else { "[ ]" }, self.label),
            None => self.label.clone(),
        }
    }
}

pub(crate) fn selector_button_label(current_label: &str) -> String {
    format!("View [ {current_label} ▼ ]")
}

pub(crate) fn selector_popup_area(anchor: Option<Rect>, bounds: Rect, item_count: usize) -> Rect {
    let width = bounds.width.clamp(1, 24);
    let content_height = item_count.min(8) as u16;
    let height = content_height
        .saturating_add(3)
        .clamp(5, bounds.height.max(1));
    let Some(anchor) = anchor else {
        return centered_rect(width, height, bounds);
    };
    let mut x = anchor.x;
    if x.saturating_add(width) > bounds.x.saturating_add(bounds.width) {
        x = bounds.x.saturating_add(bounds.width.saturating_sub(width));
    }
    let below_y = anchor.y.saturating_add(anchor.height);
    let above_y = anchor.y.saturating_sub(height);
    let y = if below_y.saturating_add(height) <= bounds.y.saturating_add(bounds.height) {
        below_y
    } else if anchor.y >= bounds.y.saturating_add(height) {
        above_y
    } else {
        bounds.y
    };
    Rect {
        x,
        y,
        width,
        height,
    }
}

pub(crate) fn selector_popup_list_area(area: Rect) -> Rect {
    Rect {
        x: area.x.saturating_add(1),
        y: area.y.saturating_add(1),
        width: area.width.saturating_sub(2),
        height: area.height.saturating_sub(3),
    }
}

pub(crate) fn popup_index_at(
    list_area: Rect,
    column: u16,
    row: u16,
    item_count: usize,
) -> Option<usize> {
    if !rect_contains(list_area, column, row) {
        return None;
    }
    let index = row.saturating_sub(list_area.y) as usize;
    (index < item_count).then_some(index)
}

pub(crate) fn checkbox_hit(column: u16, row_rect: Rect, enabled: bool) -> bool {
    enabled && column < row_rect.x.saturating_add(4)
}

fn centered_rect(width: u16, height: u16, area: Rect) -> Rect {
    let popup_width = width.min(area.width);
    let popup_height = height.min(area.height);
    Rect {
        x: area.x + area.width.saturating_sub(popup_width) / 2,
        y: area.y + area.height.saturating_sub(popup_height) / 2,
        width: popup_width,
        height: popup_height,
    }
}

fn rect_contains(rect: Rect, column: u16, row: u16) -> bool {
    column >= rect.x
        && row >= rect.y
        && column < rect.x.saturating_add(rect.width)
        && row < rect.y.saturating_add(rect.height)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn selector_popup_area_anchors_below_when_there_is_room() {
        let bounds = Rect::new(0, 0, 40, 20);
        let anchor = Rect::new(5, 3, 12, 1);
        let area = selector_popup_area(Some(anchor), bounds, 3);
        assert_eq!(area.x, anchor.x);
        assert_eq!(area.y, anchor.y + anchor.height);
        assert_eq!(area.width, 24);
        assert_eq!(area.height, 6);
    }

    #[test]
    fn selector_popup_area_clamps_to_bounds_and_prefers_above_when_needed() {
        let bounds = Rect::new(0, 0, 10, 6);
        let anchor = Rect::new(8, 5, 2, 1);
        let area = selector_popup_area(Some(anchor), bounds, 8);
        assert_eq!(area.width, 10);
        assert_eq!(area.height, 6);
        assert_eq!(area.x, 0);
        assert_eq!(area.y, 0);
    }

    #[test]
    fn popup_index_at_only_returns_visible_rows() {
        let list_area = Rect::new(2, 4, 10, 3);
        assert_eq!(popup_index_at(list_area, 2, 4, 5), Some(0));
        assert_eq!(popup_index_at(list_area, 8, 6, 5), Some(2));
        assert_eq!(popup_index_at(list_area, 8, 7, 5), None);
        assert_eq!(popup_index_at(list_area, 1, 4, 5), None);
        assert_eq!(popup_index_at(list_area, 8, 5, 1), None);
    }

    #[test]
    fn checkbox_hit_only_uses_checkbox_prefix_when_enabled() {
        let row_rect = Rect::new(10, 2, 20, 1);
        assert!(checkbox_hit(10, row_rect, true));
        assert!(checkbox_hit(13, row_rect, true));
        assert!(!checkbox_hit(14, row_rect, true));
        assert!(!checkbox_hit(10, row_rect, false));
    }
}

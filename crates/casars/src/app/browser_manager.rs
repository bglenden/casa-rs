// SPDX-License-Identifier: LGPL-3.0-or-later

use super::*;

#[derive(Debug, Clone)]
pub(crate) struct BrowserManagerRowView<T> {
    pub target: T,
    pub label: String,
    pub checked: Option<bool>,
    pub selected: bool,
}

impl<T> BrowserManagerRowView<T> {
    pub(crate) fn display_text(&self) -> String {
        match self.checked {
            Some(checked) => format!("{} {}", if checked { "[x]" } else { "[ ]" }, self.label),
            None => self.label.clone(),
        }
    }
}

impl AppState {
    pub(crate) fn browser_mode_picker_active(&self) -> bool {
        self.browser_mode_picker.is_some()
    }

    pub(crate) fn browser_mode_picker_selection(&self) -> Option<ImageBrowserLeftPaneMode> {
        self.browser_mode_picker
    }

    pub(crate) fn image_browser_left_pane_mode_for_ui(&self) -> ImageBrowserLeftPaneMode {
        self.image_browser_session_state()
            .map(|state| state.left_pane_mode)
            .unwrap_or(ImageBrowserLeftPaneMode::Live)
    }

    pub(super) fn browser_pane_checkbox_hit(
        &self,
        index: usize,
        column: u16,
        layout: &UiLayout,
    ) -> bool {
        let Some(state) = self.image_browser_session_state() else {
            return false;
        };
        let target = match state.left_pane_mode {
            ImageBrowserLeftPaneMode::Regions => {
                FormSelection::BrowserPane(BrowserPaneSelection::SavedRegion(index))
            }
            ImageBrowserLeftPaneMode::Masks => {
                FormSelection::BrowserPane(BrowserPaneSelection::Mask(index))
            }
            ImageBrowserLeftPaneMode::Live => return false,
        };
        let Some(row) = layout
            .form_rows
            .iter()
            .chain(layout.browser_manager_rows.iter())
            .find(|row| row.target == target)
        else {
            return false;
        };
        checkbox_hit(column, row.rect, true)
    }

    pub(super) fn handle_browser_mode_picker_key(&mut self, key_event: KeyEvent) {
        if key_event.kind != KeyEventKind::Press {
            return;
        }
        match key_event.code {
            KeyCode::Esc if key_event.modifiers.is_empty() => self.close_browser_mode_picker(),
            KeyCode::Enter | KeyCode::Char(' ') if key_event.modifiers.is_empty() => {
                self.commit_browser_mode_picker();
            }
            KeyCode::Up | KeyCode::Char('k')
                if key_event.modifiers.is_empty() || key_event.modifiers == KeyModifiers::SHIFT =>
            {
                self.cycle_browser_mode_picker(false);
            }
            KeyCode::Down | KeyCode::Char('j')
                if key_event.modifiers.is_empty() || key_event.modifiers == KeyModifiers::SHIFT =>
            {
                self.cycle_browser_mode_picker(true);
            }
            KeyCode::Left | KeyCode::Char('h')
                if key_event.modifiers.is_empty() || key_event.modifiers == KeyModifiers::SHIFT =>
            {
                self.cycle_browser_mode_picker(false);
            }
            KeyCode::Right | KeyCode::Char('l')
                if key_event.modifiers.is_empty() || key_event.modifiers == KeyModifiers::SHIFT =>
            {
                self.cycle_browser_mode_picker(true);
            }
            _ => {}
        }
    }

    pub(super) fn handle_browser_mode_picker_mouse(
        &mut self,
        mouse_event: MouseEvent,
        layout: &UiLayout,
    ) {
        let area =
            crate::ui::browser_mode_picker_area(layout.browser_mode_selector, layout.form_block);
        let list_area = crate::ui::browser_mode_picker_list_area(area);
        if mouse_event.kind != MouseEventKind::Down(MouseButton::Left) {
            return;
        }
        if !rect_contains(area, mouse_event.column, mouse_event.row) {
            self.close_browser_mode_picker();
            return;
        }
        if !rect_contains(list_area, mouse_event.column, mouse_event.row) {
            return;
        }
        let Some(row_offset) = popup_index_at(
            list_area,
            mouse_event.column,
            mouse_event.row,
            ImageBrowserLeftPaneMode::all().len(),
        ) else {
            return;
        };
        let Some(mode) = ImageBrowserLeftPaneMode::all().get(row_offset).copied() else {
            return;
        };
        self.browser_mode_picker = Some(mode);
        self.commit_browser_mode_picker();
    }

    pub(crate) fn browser_manager_rows(&self) -> Vec<BrowserManagerRowView<BrowserPaneSelection>> {
        let Some(state) = self.image_browser_session_state() else {
            return Vec::new();
        };
        let mut rows = Vec::new();

        match state.left_pane_mode {
            ImageBrowserLeftPaneMode::Live => {}
            ImageBrowserLeftPaneMode::Regions => {
                if state.snapshot.saved_region_names.is_empty() {
                    rows.push(BrowserManagerRowView {
                        target: BrowserPaneSelection::Mode(ImageBrowserLeftPaneMode::Regions),
                        label: "No saved regions.".to_string(),
                        checked: None,
                        selected: false,
                    });
                } else {
                    rows.extend(state.snapshot.saved_region_names.iter().enumerate().map(
                        |(index, name)| {
                            let selected = self.selected_form
                                == FormSelection::BrowserPane(BrowserPaneSelection::SavedRegion(
                                    index,
                                ));
                            let active =
                                state.active_region_definition_name() == Some(name.as_str());
                            let row_name = if selected
                                && self.edit_state.as_ref().is_some_and(|edit| {
                                    edit.target == EditTarget::RenameImageRegionDefinition
                                }) {
                                self.edit_state
                                    .as_ref()
                                    .map(|edit| format!("{}|", edit.buffer))
                                    .unwrap_or_else(|| name.to_string())
                            } else {
                                name.to_string()
                            };
                            BrowserManagerRowView {
                                target: BrowserPaneSelection::SavedRegion(index),
                                label: row_name,
                                checked: Some(active),
                                selected,
                            }
                        },
                    ));
                }
            }
            ImageBrowserLeftPaneMode::Masks => {
                if state.snapshot.mask_names.is_empty() {
                    rows.push(BrowserManagerRowView {
                        target: BrowserPaneSelection::Mode(ImageBrowserLeftPaneMode::Masks),
                        label: "No masks.".to_string(),
                        checked: None,
                        selected: false,
                    });
                } else {
                    rows.extend(state.snapshot.mask_names.iter().enumerate().map(
                        |(index, name)| {
                            let selected = self.selected_form
                                == FormSelection::BrowserPane(BrowserPaneSelection::Mask(index));
                            let default =
                                state.snapshot.default_mask_name.as_deref() == Some(name.as_str());
                            BrowserManagerRowView {
                                target: BrowserPaneSelection::Mask(index),
                                label: name.clone(),
                                checked: Some(default),
                                selected,
                            }
                        },
                    ));
                }
            }
        }
        rows
    }

    pub(super) fn open_browser_mode_picker(&mut self) {
        let Some(mode) = self
            .image_browser_session_state()
            .map(|state| state.left_pane_mode)
        else {
            return;
        };
        self.browser_mode_picker = Some(mode);
        self.selected_form = FormSelection::BrowserPane(BrowserPaneSelection::Mode(mode));
    }

    pub(super) fn cycle_browser_mode_picker(&mut self, forward: bool) {
        let Some(mode) = self.browser_mode_picker else {
            return;
        };
        self.browser_mode_picker = Some(mode.cycle(forward));
    }

    pub(super) fn commit_browser_mode_picker(&mut self) {
        let Some(mode) = self.browser_mode_picker else {
            return;
        };
        self.select_image_browser_left_pane_mode(mode);
    }

    pub(super) fn close_browser_mode_picker(&mut self) {
        self.browser_mode_picker = None;
    }

    pub(super) fn load_selected_image_region_definition(&mut self) {
        let Some(state) = self.image_browser_session_state() else {
            return;
        };
        let Some(name) = state.selected_saved_region_name().map(str::to_string) else {
            self.result.status_line = "No saved region selected.".into();
            self.result.status_kind = StatusKind::Warning;
            return;
        };
        self.send_browser_command(BrowserRequest::LoadImageRegionDefinition { name: name.clone() });
        self.result.status_line = format!("Loading saved region {name}...");
        self.result.status_kind = StatusKind::Info;
    }

    pub(super) fn rename_image_region_definition(&mut self) {
        let Some(state) = self.image_browser_session_state() else {
            return;
        };
        let Some(name) = state.selected_saved_region_name() else {
            self.result.status_line = "Select a saved region before renaming it.".into();
            self.result.status_kind = StatusKind::Warning;
            return;
        };
        self.edit_state = Some(EditState {
            target: EditTarget::RenameImageRegionDefinition,
            buffer: name.to_string(),
        });
    }

    pub(super) fn delete_image_region_definition(&mut self) {
        let Some(state) = self.image_browser_session_state() else {
            return;
        };
        let Some(name) = state.selected_saved_region_name().map(str::to_string) else {
            self.result.status_line = "Select a saved region before deleting it.".into();
            self.result.status_kind = StatusKind::Warning;
            return;
        };
        self.send_browser_command(BrowserRequest::DeleteImageRegionDefinition {
            name: name.clone(),
        });
        self.result.status_line = format!("Deleting saved region {name}...");
        self.result.status_kind = StatusKind::Info;
    }

    pub(super) fn set_selected_image_mask_default(&mut self) {
        let Some(state) = self.image_browser_session_state() else {
            return;
        };
        let Some(name) = state.selected_mask_name().map(str::to_string) else {
            self.result.status_line = "No mask selected.".into();
            self.result.status_kind = StatusKind::Warning;
            return;
        };
        self.send_browser_command(BrowserRequest::SetImageDefaultMask { name: name.clone() });
        self.result.status_line = format!("Setting default mask to {name}...");
        self.result.status_kind = StatusKind::Info;
    }

    pub(super) fn unset_image_default_mask(&mut self) {
        self.send_browser_command(BrowserRequest::UnsetImageDefaultMask);
        self.result.status_line = "Clearing default mask...".into();
        self.result.status_kind = StatusKind::Info;
    }

    pub(super) fn delete_selected_image_mask(&mut self) {
        let Some(state) = self.image_browser_session_state() else {
            return;
        };
        let Some(name) = state.selected_mask_name().map(str::to_string) else {
            self.result.status_line = "No mask selected.".into();
            self.result.status_kind = StatusKind::Warning;
            return;
        };
        self.send_browser_command(BrowserRequest::DeleteImageMask { name: name.clone() });
        self.result.status_line = format!("Deleting mask {name}...");
        self.result.status_kind = StatusKind::Info;
    }

    pub(super) fn activate_browser_pane_selection(&mut self, target: BrowserPaneSelection) {
        match target {
            BrowserPaneSelection::Mode(_) => self.open_browser_mode_picker(),
            BrowserPaneSelection::SavedRegion(index) => {
                let active = self
                    .image_browser_session_state()
                    .and_then(|state| state.snapshot.saved_region_names.get(index))
                    .is_some_and(|name| {
                        self.image_browser_session_state()
                            .and_then(ImageBrowserSessionState::active_region_definition_name)
                            == Some(name.as_str())
                    });
                if let Some(state) = self.image_browser_session_state_mut() {
                    state.left_pane_mode = ImageBrowserLeftPaneMode::Regions;
                    state.selected_saved_region_index = index;
                }
                if active {
                    self.clear_image_region();
                } else {
                    self.load_selected_image_region_definition();
                }
            }
            BrowserPaneSelection::Mask(index) => {
                let default = self
                    .image_browser_session_state()
                    .and_then(|state| state.snapshot.mask_names.get(index))
                    .is_some_and(|name| {
                        self.image_browser_session_state()
                            .and_then(|state| state.snapshot.default_mask_name.as_deref())
                            == Some(name.as_str())
                    });
                if let Some(state) = self.image_browser_session_state_mut() {
                    state.left_pane_mode = ImageBrowserLeftPaneMode::Masks;
                    state.selected_mask_index = index;
                }
                if default {
                    self.unset_image_default_mask();
                } else {
                    self.set_selected_image_mask_default();
                }
            }
        }
    }
}

fn popup_index_at(list_area: Rect, column: u16, row: u16, item_count: usize) -> Option<usize> {
    if !rect_contains(list_area, column, row) {
        return None;
    }
    let index = row.saturating_sub(list_area.y) as usize;
    (index < item_count).then_some(index)
}

fn checkbox_hit(column: u16, row_rect: Rect, enabled: bool) -> bool {
    enabled && column < row_rect.x.saturating_add(4)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::registry::msexplore_app;
    use casa_ms::msexplore::cli::command_schema;

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

    #[test]
    fn row_view_display_text_covers_checked_and_plain_labels() {
        let plain = BrowserManagerRowView {
            target: 1usize,
            label: "plain".to_string(),
            checked: None,
            selected: false,
        };
        let enabled = BrowserManagerRowView {
            target: 2usize,
            label: "enabled".to_string(),
            checked: Some(true),
            selected: true,
        };
        let disabled = BrowserManagerRowView {
            target: 3usize,
            label: "disabled".to_string(),
            checked: Some(false),
            selected: false,
        };

        assert_eq!(plain.display_text(), "plain");
        assert_eq!(enabled.display_text(), "[x] enabled");
        assert_eq!(disabled.display_text(), "[ ] disabled");
    }

    #[test]
    fn browser_manager_helpers_noop_cleanly_without_image_session() {
        let mut app = AppState::from_schema(msexplore_app(), command_schema("msexplore"));
        let layout = crate::ui::compute_layout(Rect::new(0, 0, 120, 30), &app);

        assert!(!app.browser_mode_picker_active());
        assert_eq!(app.browser_mode_picker_selection(), None);
        assert_eq!(
            app.image_browser_left_pane_mode_for_ui(),
            ImageBrowserLeftPaneMode::Live
        );
        assert!(!app.browser_pane_checkbox_hit(0, 0, &layout));
        assert!(app.browser_manager_rows().is_empty());

        app.open_browser_mode_picker();
        assert!(!app.browser_mode_picker_active());
        app.cycle_browser_mode_picker(true);
        app.commit_browser_mode_picker();
        app.close_browser_mode_picker();
        assert_eq!(app.browser_mode_picker_selection(), None);

        app.load_selected_image_region_definition();
        app.rename_image_region_definition();
        app.delete_image_region_definition();
        app.set_selected_image_mask_default();
        app.delete_selected_image_mask();
        assert_eq!(app.result_status_kind(), "info");
    }
}

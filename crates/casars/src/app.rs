use std::ffi::OsString;
use std::time::{Duration, Instant};

use casacore_ms::ListObsSummary;
use casacore_ms::listobs::cli::{
    UiArgumentParser, UiArgumentSchema, UiCommandSchema, UiManagedOutputSchema,
};
use casacore_types::quanta::{MvAngle, MvTime};
use crossterm::event::{
    KeyCode, KeyEvent, KeyEventKind, KeyModifiers, MouseButton, MouseEvent, MouseEventKind,
};

use crate::config::{ConfigStore, ThemeMode};
use crate::execution::{ExecutionEvent, ExecutionPlan, RunningProcess, spawn_process};
use crate::registry::RegistryApp;
use crate::ui::UiLayout;

const DENSE_SPINNER_FRAMES: &[&str] = &["|", "/", "-", "\\"];
const RICH_SPINNER_FRAMES: &[&str] = &["⠋", "⠙", "⠹", "⠸", "⠴", "⠦"];
const DOUBLE_CLICK_WINDOW: Duration = Duration::from_millis(450);
const HORIZONTAL_SCROLL_STEP: i16 = 8;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum PaneFocus {
    Parameters,
    Result,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) enum FormSelection {
    Section(usize),
    Field(usize),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) enum ResultTab {
    Overview,
    Observations,
    Scans,
    Fields,
    Spws,
    Sources,
    Antennas,
    Stdout,
    Stderr,
}

impl ResultTab {
    pub(crate) const ALL: [Self; 9] = [
        Self::Overview,
        Self::Observations,
        Self::Scans,
        Self::Fields,
        Self::Spws,
        Self::Sources,
        Self::Antennas,
        Self::Stdout,
        Self::Stderr,
    ];

    pub(crate) fn label(self) -> &'static str {
        match self {
            Self::Overview => "Overview",
            Self::Observations => "Observations",
            Self::Scans => "Scans",
            Self::Fields => "Fields",
            Self::Spws => "SPWs",
            Self::Sources => "Sources",
            Self::Antennas => "Antennas",
            Self::Stdout => "Stdout",
            Self::Stderr => "Stderr",
        }
    }

    fn index(self) -> usize {
        match self {
            Self::Overview => 0,
            Self::Observations => 1,
            Self::Scans => 2,
            Self::Fields => 3,
            Self::Spws => 4,
            Self::Sources => 5,
            Self::Antennas => 6,
            Self::Stdout => 7,
            Self::Stderr => 8,
        }
    }
}

#[derive(Debug, Clone)]
pub(crate) struct FormRowView {
    pub target: FormSelection,
    pub text: String,
    pub kind: FormRowKind,
    pub selected: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum FormRowKind {
    Section { collapsed: bool },
    Field,
}

#[derive(Debug, Clone)]
pub(crate) struct TableView {
    pub header: String,
    pub rows: Vec<String>,
}

impl TableView {
    pub(crate) fn content_width(&self) -> usize {
        std::iter::once(self.header.chars().count())
            .chain(self.rows.iter().map(|row| row.chars().count()))
            .max()
            .unwrap_or(0)
    }
}

#[derive(Debug, Clone)]
pub(crate) enum ResultContent {
    Lines(Vec<String>),
    Table(TableView),
}

#[derive(Debug)]
pub(crate) struct AppState {
    app: RegistryApp,
    config_store: ConfigStore,
    schema: Option<UiCommandSchema>,
    schema_error: Option<String>,
    fields: Vec<FormField>,
    sections: Vec<FormSection>,
    selected_form: FormSelection,
    show_advanced: bool,
    pane_focus: PaneFocus,
    edit_state: Option<EditState>,
    result: ResultState,
    active_result_tab: ResultTab,
    result_scrolls: [u16; 9],
    result_hscrolls: [u16; 9],
    running: Option<RunningState>,
    spinner_frame: usize,
    dragging_divider: bool,
    dragging_result_scrollbar: bool,
    dragging_result_hscrollbar: bool,
    dragging_result_hscrollbar_grab: u16,
    last_click: Option<ClickState>,
    quit: bool,
}

#[derive(Debug)]
struct RunningState {
    process: RunningProcess,
    renderer: Option<String>,
    file_output_path: Option<String>,
    cancel_requested: bool,
}

#[derive(Debug, Default)]
struct ResultState {
    status_line: String,
    status_kind: StatusKind,
    stdout: String,
    stderr: String,
    structured: Option<ListObsSummary>,
    structured_error: Option<String>,
    file_output_path: Option<String>,
    exit_code: Option<i32>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
enum StatusKind {
    #[default]
    Info,
    Running,
    Ok,
    Error,
    Warning,
}

#[derive(Debug)]
struct EditState {
    field_index: usize,
    buffer: String,
}

#[derive(Debug)]
struct FormField {
    schema: UiArgumentSchema,
    value: FormValue,
}

#[derive(Debug)]
enum FormValue {
    Text(String),
    Toggle(bool),
    Choice { value: String, choices: Vec<String> },
}

#[derive(Debug)]
struct FormSection {
    name: String,
    field_indices: Vec<usize>,
    collapsed: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ClickTarget {
    Divider,
    ResultScrollbar,
    ResultHorizontalScrollbar,
    Pane(PaneFocus),
    Section(usize),
    Field(usize),
    Tab(ResultTab),
}

#[derive(Debug, Clone, Copy)]
struct ClickState {
    target: ClickTarget,
    at: Instant,
}

impl AppState {
    pub(crate) fn from_schema(app: RegistryApp, schema: UiCommandSchema) -> Self {
        Self::from_schema_with_config(app, schema, ConfigStore::load_default())
    }

    pub(crate) fn from_schema_with_config(
        app: RegistryApp,
        schema: UiCommandSchema,
        config_store: ConfigStore,
    ) -> Self {
        let fields = schema
            .arguments
            .iter()
            .filter_map(FormField::from_schema)
            .collect::<Vec<_>>();
        let sections = build_sections(&fields);
        let selected_form = initial_form_selection(&sections, &fields, false);

        Self {
            app,
            config_store,
            schema: Some(schema),
            schema_error: None,
            fields,
            sections,
            selected_form,
            show_advanced: false,
            pane_focus: PaneFocus::Parameters,
            edit_state: None,
            result: ResultState {
                status_line: "Ready. Press r to run listobs.".to_string(),
                status_kind: StatusKind::Info,
                ..ResultState::default()
            },
            active_result_tab: ResultTab::Overview,
            result_scrolls: [0; 9],
            result_hscrolls: [0; 9],
            running: None,
            spinner_frame: 0,
            dragging_divider: false,
            dragging_result_scrollbar: false,
            dragging_result_hscrollbar: false,
            dragging_result_hscrollbar_grab: 0,
            last_click: None,
            quit: false,
        }
    }

    pub(crate) fn schema_error(app: RegistryApp, error: String) -> Self {
        Self::schema_error_with_config(app, error, ConfigStore::load_default())
    }

    pub(crate) fn schema_error_with_config(
        app: RegistryApp,
        error: String,
        config_store: ConfigStore,
    ) -> Self {
        Self {
            app,
            config_store,
            schema: None,
            schema_error: Some(error.clone()),
            fields: Vec::new(),
            sections: Vec::new(),
            selected_form: FormSelection::Section(0),
            show_advanced: false,
            pane_focus: PaneFocus::Result,
            edit_state: None,
            result: ResultState {
                status_line: "Failed to load UI schema.".to_string(),
                status_kind: StatusKind::Error,
                stderr: format!("{error}\n"),
                structured_error: Some(error),
                ..ResultState::default()
            },
            active_result_tab: ResultTab::Stderr,
            result_scrolls: [0; 9],
            result_hscrolls: [0; 9],
            running: None,
            spinner_frame: 0,
            dragging_divider: false,
            dragging_result_scrollbar: false,
            dragging_result_hscrollbar: false,
            dragging_result_hscrollbar_grab: 0,
            last_click: None,
            quit: false,
        }
    }

    pub(crate) fn should_quit(&self) -> bool {
        self.quit
    }

    pub(crate) fn on_tick(&mut self) {
        self.spinner_frame = (self.spinner_frame + 1) % spinner_frames(self.theme_mode()).len();
    }

    pub(crate) fn handle_key_event(&mut self, key_event: KeyEvent) {
        if key_event.kind != KeyEventKind::Press {
            return;
        }

        match key_event.code {
            KeyCode::Char('q') if key_event.modifiers.is_empty() => {
                if self.running.is_some() {
                    self.cancel_current();
                }
                self.quit = true;
                return;
            }
            KeyCode::Char('t') if key_event.modifiers.is_empty() => {
                self.toggle_theme();
                return;
            }
            KeyCode::Tab | KeyCode::BackTab => {
                self.toggle_focus();
                return;
            }
            KeyCode::Char('r') if key_event.modifiers.is_empty() && self.edit_state.is_none() => {
                if self.running.is_none() {
                    self.start_run();
                }
                return;
            }
            KeyCode::Char('a') if key_event.modifiers.is_empty() && self.running.is_none() => {
                self.toggle_advanced();
                return;
            }
            KeyCode::Char('x') if key_event.modifiers.is_empty() => {
                self.cancel_current();
                return;
            }
            _ => {}
        }

        if self.edit_state.is_some() {
            self.handle_edit_key(key_event);
            return;
        }

        if self.running.is_some() {
            self.handle_result_key(key_event);
            return;
        }

        match self.pane_focus {
            PaneFocus::Parameters => self.handle_parameter_key(key_event),
            PaneFocus::Result => self.handle_result_key(key_event),
        }
    }

    pub(crate) fn handle_paste(&mut self, text: String) {
        let pasted = text.trim_end_matches(['\r', '\n']).to_string();
        if pasted.is_empty() {
            return;
        }

        if let Some(edit_state) = self.edit_state.as_mut() {
            edit_state.buffer.push_str(&pasted);
            return;
        }

        let FormSelection::Field(field_index) = self.selected_form else {
            return;
        };
        let Some(field) = self.fields.get_mut(field_index) else {
            return;
        };
        if matches!(field.value, FormValue::Text(_)) {
            field.set_text(pasted);
        }
    }

    pub(crate) fn handle_mouse_event(&mut self, mouse_event: MouseEvent, layout: &UiLayout) {
        match mouse_event.kind {
            MouseEventKind::Down(MouseButton::Left) => {
                self.handle_left_mouse_down(mouse_event, layout)
            }
            MouseEventKind::Up(MouseButton::Left) => {
                self.dragging_divider = false;
                self.dragging_result_scrollbar = false;
                self.dragging_result_hscrollbar = false;
                self.dragging_result_hscrollbar_grab = 0;
            }
            MouseEventKind::Drag(MouseButton::Left) => {
                self.handle_left_mouse_drag(mouse_event, layout)
            }
            MouseEventKind::ScrollUp => self.handle_mouse_scroll(mouse_event, layout, -3),
            MouseEventKind::ScrollDown => self.handle_mouse_scroll(mouse_event, layout, 3),
            MouseEventKind::ScrollLeft => {
                self.handle_mouse_hscroll(mouse_event, layout, -HORIZONTAL_SCROLL_STEP)
            }
            MouseEventKind::ScrollRight => {
                self.handle_mouse_hscroll(mouse_event, layout, HORIZONTAL_SCROLL_STEP)
            }
            _ => {}
        }
    }

    pub(crate) fn drain_execution_events(&mut self) {
        loop {
            let event = match self.running.as_ref() {
                Some(running) => running.process.try_recv(),
                None => return,
            };
            match event {
                Ok(ExecutionEvent::Stdout(chunk)) => self.result.stdout.push_str(&chunk),
                Ok(ExecutionEvent::Stderr(chunk)) => self.result.stderr.push_str(&chunk),
                Ok(ExecutionEvent::Exited(exit)) => {
                    self.finish_execution(exit.code, exit.success);
                    break;
                }
                Err(std::sync::mpsc::TryRecvError::Empty) => break,
                Err(std::sync::mpsc::TryRecvError::Disconnected) => {
                    self.result.status_line =
                        "Execution channel disconnected unexpectedly.".to_string();
                    self.result.status_kind = StatusKind::Error;
                    self.running = None;
                    break;
                }
            }
        }
    }

    pub(crate) fn app_category(&self) -> &str {
        self.app.category
    }

    pub(crate) fn app_name(&self) -> &str {
        self.app.display_name
    }

    pub(crate) fn footer_text(&self) -> &'static str {
        if self.edit_state.is_some() {
            "Tab pane  Enter commit  Esc cancel  Bksp delete  t theme  q quit"
        } else if self.running.is_some() {
            "Tab pane  h/l tabs  j/k scroll  [/] horiz  x cancel  t theme  q quit"
        } else {
            "Tab pane  Up/Down move  Enter act  h/l tabs  j/k scroll  [/] horiz  a adv  r run  t theme  q quit"
        }
    }

    pub(crate) fn parameter_title(&self) -> String {
        let focus = if self.pane_focus == PaneFocus::Parameters {
            " [focus]"
        } else {
            ""
        };
        let title = match self.theme_mode() {
            ThemeMode::DenseAnsi => "Parameters",
            ThemeMode::RichPanel => "◈ Parameters",
        };
        if self.running.is_some() {
            format!(
                "{title} [{}] locked{}",
                spinner_frames(self.theme_mode())[self.spinner_frame],
                focus
            )
        } else if self.schema_error.is_some() {
            format!("{title} (schema unavailable){focus}")
        } else {
            format!("{title}{focus}")
        }
    }

    pub(crate) fn result_title(&self) -> String {
        let focus = if self.pane_focus == PaneFocus::Result {
            " [focus]"
        } else {
            ""
        };
        let title = match self.theme_mode() {
            ThemeMode::DenseAnsi => "Result",
            ThemeMode::RichPanel => "◉ Result",
        };
        format!("{title}{focus}")
    }

    pub(crate) fn theme_mode(&self) -> ThemeMode {
        self.config_store.theme_mode()
    }

    pub(crate) fn pane_split_ratio(&self) -> f32 {
        self.config_store.pane_split_ratio()
    }

    pub(crate) fn pane_focus(&self) -> PaneFocus {
        self.pane_focus
    }

    pub(crate) fn form_rows(&self) -> Vec<FormRowView> {
        let mut rows = Vec::new();
        for (section_index, section) in self.sections.iter().enumerate() {
            let visible_fields = section
                .field_indices
                .iter()
                .copied()
                .filter(|index| self.show_advanced || !self.fields[*index].schema.advanced)
                .collect::<Vec<_>>();
            if visible_fields.is_empty() {
                continue;
            }

            rows.push(FormRowView {
                target: FormSelection::Section(section_index),
                text: section.name.clone(),
                kind: FormRowKind::Section {
                    collapsed: section.collapsed,
                },
                selected: self.selected_form == FormSelection::Section(section_index),
            });

            if section.collapsed {
                continue;
            }

            for field_index in visible_fields {
                rows.push(FormRowView {
                    target: FormSelection::Field(field_index),
                    text: self.fields[field_index]
                        .render_line(self.edit_state.as_ref(), field_index),
                    kind: FormRowKind::Field,
                    selected: self.selected_form == FormSelection::Field(field_index),
                });
            }
        }

        if rows.is_empty() {
            rows.push(FormRowView {
                target: FormSelection::Section(0),
                text: "No editable parameters available.".to_string(),
                kind: FormRowKind::Field,
                selected: false,
            });
        }

        rows
    }

    pub(crate) fn active_result_tab(&self) -> ResultTab {
        self.active_result_tab
    }

    pub(crate) fn active_result_scroll(&self) -> u16 {
        self.result_scrolls[self.active_result_tab.index()]
    }

    pub(crate) fn active_result_hscroll(&self) -> u16 {
        self.result_hscrolls[self.active_result_tab.index()]
    }

    pub(crate) fn result_tabs(&self) -> &'static [ResultTab] {
        self.visible_result_tabs()
    }

    pub(crate) fn result_status_lines(&self) -> Vec<String> {
        let mut lines = vec![format!("Status: {}", self.result.status_line)];
        if let Some(code) = self.result.exit_code {
            lines.push(format!("Exit code: {code}"));
        } else if self.running.is_some() {
            lines.push("Exit code: running".to_string());
        } else {
            lines.push(String::new());
        }

        if let Some(path) = &self.result.file_output_path {
            lines.push(format!("Output: {path}"));
        } else if let Some(error) = &self.result.structured_error {
            lines.push(error.clone());
        } else if let Some(error) = &self.schema_error {
            lines.push(error.clone());
        } else if self.running.is_some() {
            lines.push("Structured output will appear when the subprocess exits.".to_string());
        } else {
            lines.push(format!(
                "View: {}  Theme: {}  Verbose: {}",
                self.active_result_tab.label(),
                match self.theme_mode() {
                    ThemeMode::DenseAnsi => "dense_ansi",
                    ThemeMode::RichPanel => "rich_panel",
                },
                if self.verbose_enabled() { "on" } else { "off" }
            ));
        }
        lines
    }

    pub(crate) fn result_status_kind(&self) -> &'static str {
        match self.result.status_kind {
            StatusKind::Info => "info",
            StatusKind::Running => "running",
            StatusKind::Ok => "ok",
            StatusKind::Error => "error",
            StatusKind::Warning => "warning",
        }
    }

    pub(crate) fn active_result_content(&self) -> ResultContent {
        match self.active_result_tab {
            ResultTab::Overview => ResultContent::Lines(self.overview_lines()),
            ResultTab::Observations => match self.result.structured.as_ref() {
                Some(summary) => ResultContent::Table(build_observations_table(summary)),
                None => {
                    ResultContent::Lines(vec!["No observation table available yet.".to_string()])
                }
            },
            ResultTab::Scans => match self.result.structured.as_ref() {
                Some(summary) => {
                    ResultContent::Table(build_scans_table(summary, self.listunfl_enabled()))
                }
                None => ResultContent::Lines(vec!["No scan table available yet.".to_string()]),
            },
            ResultTab::Fields => match self.result.structured.as_ref() {
                Some(summary) => {
                    ResultContent::Table(build_fields_table(summary, self.listunfl_enabled()))
                }
                None => ResultContent::Lines(vec!["No field table available yet.".to_string()]),
            },
            ResultTab::Spws => match self.result.structured.as_ref() {
                Some(summary) => ResultContent::Table(build_spws_table(summary)),
                None => ResultContent::Lines(vec![
                    "No spectral-window table available yet.".to_string(),
                ]),
            },
            ResultTab::Sources => match self.result.structured.as_ref() {
                Some(summary) => ResultContent::Table(build_sources_table(summary)),
                None => ResultContent::Lines(vec!["No source table available yet.".to_string()]),
            },
            ResultTab::Antennas => match self.result.structured.as_ref() {
                Some(summary) if self.verbose_enabled() => {
                    ResultContent::Table(build_antennas_table(summary))
                }
                Some(summary) => ResultContent::Lines(build_compact_antenna_lines(summary)),
                None => ResultContent::Lines(vec!["No antenna table available yet.".to_string()]),
            },
            ResultTab::Stdout => ResultContent::Lines(raw_lines("stdout", &self.result.stdout)),
            ResultTab::Stderr => ResultContent::Lines(raw_lines("stderr", &self.result.stderr)),
        }
    }

    #[cfg(test)]
    pub(crate) fn set_text_value(&mut self, id: &str, value: &str) {
        if let Some(field) = self.fields.iter_mut().find(|field| field.schema.id == id) {
            field.set_text(value.to_string());
        }
    }

    #[cfg(test)]
    pub(crate) fn set_toggle_value(&mut self, id: &str, value: bool) {
        if let Some(field) = self.fields.iter_mut().find(|field| field.schema.id == id) {
            field.set_toggle(value);
        }
    }

    #[cfg(test)]
    pub(crate) fn set_active_result_tab(&mut self, tab: ResultTab) {
        self.active_result_tab = tab;
    }

    #[cfg(test)]
    pub(crate) fn active_result_hscroll_for_test(&self) -> u16 {
        self.active_result_hscroll()
    }

    #[cfg(test)]
    pub(crate) fn set_result_for_test(&mut self, stdout: &str, stderr: &str) {
        self.result.stdout = stdout.to_string();
        self.result.stderr = stderr.to_string();
        self.result.status_line = "Test result".to_string();
        self.result.status_kind = StatusKind::Info;
    }

    #[cfg(test)]
    pub(crate) fn start_run_for_test(&mut self) {
        self.start_run();
    }

    #[cfg(test)]
    pub(crate) fn cancel_for_test(&mut self) {
        self.cancel_current();
    }

    #[cfg(test)]
    pub(crate) fn wait_for_idle_for_test(&mut self, timeout: Duration) -> bool {
        let start = Instant::now();
        while self.running.is_some() && start.elapsed() < timeout {
            self.drain_execution_events();
            std::thread::sleep(Duration::from_millis(25));
        }
        self.drain_execution_events();
        self.running.is_none()
    }

    #[cfg(test)]
    pub(crate) fn is_running_for_test(&self) -> bool {
        self.running.is_some()
    }

    #[cfg(test)]
    pub(crate) fn status_line_for_test(&self) -> &str {
        &self.result.status_line
    }

    #[cfg(test)]
    pub(crate) fn stderr_for_test(&self) -> &str {
        &self.result.stderr
    }

    #[cfg(test)]
    pub(crate) fn structured_for_test(&self) -> Option<&ListObsSummary> {
        self.result.structured.as_ref()
    }

    #[cfg(test)]
    pub(crate) fn file_output_path_for_test(&self) -> Option<&str> {
        self.result.file_output_path.as_deref()
    }

    #[cfg(test)]
    pub(crate) fn theme_mode_for_test(&self) -> ThemeMode {
        self.theme_mode()
    }

    #[cfg(test)]
    pub(crate) fn pane_split_ratio_for_test(&self) -> f32 {
        self.pane_split_ratio()
    }

    #[cfg(test)]
    pub(crate) fn pane_focus_for_test(&self) -> PaneFocus {
        self.pane_focus
    }

    #[cfg(test)]
    pub(crate) fn section_collapsed_for_test(&self, name: &str) -> Option<bool> {
        self.sections
            .iter()
            .find(|section| section.name == name)
            .map(|section| section.collapsed)
    }

    #[cfg(test)]
    pub(crate) fn edit_buffer_for_test(&self) -> Option<&str> {
        self.edit_state.as_ref().map(|state| state.buffer.as_str())
    }

    #[cfg(test)]
    pub(crate) fn selected_form_text_for_test(&self) -> Option<String> {
        self.form_rows()
            .into_iter()
            .find(|row| row.selected)
            .map(|row| row.text)
    }

    fn handle_parameter_key(&mut self, key_event: KeyEvent) {
        match key_event.code {
            KeyCode::Up | KeyCode::Char('k')
                if key_event.modifiers.is_empty() || key_event.modifiers == KeyModifiers::SHIFT =>
            {
                self.select_previous_form_item();
            }
            KeyCode::Down | KeyCode::Char('j')
                if key_event.modifiers.is_empty() || key_event.modifiers == KeyModifiers::SHIFT =>
            {
                self.select_next_form_item();
            }
            KeyCode::Left => self.adjust_selected_choice(false),
            KeyCode::Right => self.adjust_selected_choice(true),
            KeyCode::Enter | KeyCode::Char(' ') if key_event.modifiers.is_empty() => {
                self.activate_selected_form_item();
            }
            _ => {}
        }
    }

    fn handle_result_key(&mut self, key_event: KeyEvent) {
        match key_event.code {
            KeyCode::Left | KeyCode::Char('h')
                if key_event.modifiers.is_empty() || key_event.modifiers == KeyModifiers::SHIFT =>
            {
                self.cycle_visible_result_tab(false);
            }
            KeyCode::Right | KeyCode::Char('l')
                if key_event.modifiers.is_empty() || key_event.modifiers == KeyModifiers::SHIFT =>
            {
                self.cycle_visible_result_tab(true);
            }
            KeyCode::Up | KeyCode::Char('k')
                if key_event.modifiers.is_empty() || key_event.modifiers == KeyModifiers::SHIFT =>
            {
                self.scroll_active_result(-1);
            }
            KeyCode::Down | KeyCode::Char('j')
                if key_event.modifiers.is_empty() || key_event.modifiers == KeyModifiers::SHIFT =>
            {
                self.scroll_active_result(1);
            }
            KeyCode::PageUp => self.scroll_active_result(-10),
            KeyCode::PageDown => self.scroll_active_result(10),
            KeyCode::Char('[') if key_event.modifiers.is_empty() => {
                self.scroll_active_result_horizontal(-HORIZONTAL_SCROLL_STEP)
            }
            KeyCode::Char(']') if key_event.modifiers.is_empty() => {
                self.scroll_active_result_horizontal(HORIZONTAL_SCROLL_STEP)
            }
            KeyCode::Left if key_event.modifiers == KeyModifiers::CONTROL => {
                self.scroll_active_result_horizontal(-HORIZONTAL_SCROLL_STEP)
            }
            KeyCode::Right if key_event.modifiers == KeyModifiers::CONTROL => {
                self.scroll_active_result_horizontal(HORIZONTAL_SCROLL_STEP)
            }
            KeyCode::Char('v') if key_event.modifiers.is_empty() => {
                self.cycle_visible_result_tab(true);
            }
            _ => {}
        }
    }

    fn handle_edit_key(&mut self, key_event: KeyEvent) {
        let Some(edit_state) = self.edit_state.as_mut() else {
            return;
        };
        match key_event.code {
            KeyCode::Esc => self.edit_state = None,
            KeyCode::Enter => {
                if let Some(field) = self.fields.get_mut(edit_state.field_index) {
                    field.set_text(edit_state.buffer.clone());
                }
                self.edit_state = None;
            }
            KeyCode::Backspace => {
                edit_state.buffer.pop();
            }
            KeyCode::Char(character)
                if key_event.modifiers.is_empty() || key_event.modifiers == KeyModifiers::SHIFT =>
            {
                edit_state.buffer.push(character);
            }
            _ => {}
        }
    }

    fn handle_left_mouse_down(&mut self, mouse_event: MouseEvent, layout: &UiLayout) {
        if layout.in_divider(mouse_event.column, mouse_event.row) {
            self.dragging_divider = true;
            self.dragging_result_scrollbar = false;
            self.last_click = Some(ClickState {
                target: ClickTarget::Divider,
                at: Instant::now(),
            });
            return;
        }

        if layout.in_result_scrollbar(mouse_event.column, mouse_event.row) {
            self.pane_focus = PaneFocus::Result;
            self.dragging_divider = false;
            self.dragging_result_scrollbar = true;
            self.dragging_result_hscrollbar = false;
            self.scroll_result_to_mouse(mouse_event.row, layout);
            self.last_click = Some(ClickState {
                target: ClickTarget::ResultScrollbar,
                at: Instant::now(),
            });
            return;
        }

        if layout.in_result_hscrollbar(mouse_event.column, mouse_event.row) {
            self.pane_focus = PaneFocus::Result;
            self.dragging_divider = false;
            self.dragging_result_scrollbar = false;
            self.dragging_result_hscrollbar = true;
            self.dragging_result_hscrollbar_grab =
                self.result_hscrollbar_grab_offset(mouse_event.column, layout);
            self.scroll_result_horizontally_to_mouse(mouse_event.column, layout);
            self.last_click = Some(ClickState {
                target: ClickTarget::ResultHorizontalScrollbar,
                at: Instant::now(),
            });
            return;
        }

        if let Some(tab) = layout.result_tab_at(mouse_event.column, mouse_event.row) {
            self.pane_focus = PaneFocus::Result;
            self.active_result_tab = tab;
            self.last_click = Some(ClickState {
                target: ClickTarget::Tab(tab),
                at: Instant::now(),
            });
            return;
        }

        if layout.in_result_block(mouse_event.column, mouse_event.row) {
            self.pane_focus = PaneFocus::Result;
            self.last_click = Some(ClickState {
                target: ClickTarget::Pane(PaneFocus::Result),
                at: Instant::now(),
            });
            return;
        }

        if self.running.is_some() {
            return;
        }

        if let Some(target) = layout.form_target_at(mouse_event.column, mouse_event.row) {
            self.pane_focus = PaneFocus::Parameters;
            let click_target = match target {
                FormSelection::Section(index) => ClickTarget::Section(index),
                FormSelection::Field(index) => ClickTarget::Field(index),
            };
            let double_click = self.last_click.is_some_and(|last| {
                last.target == click_target && last.at.elapsed() <= DOUBLE_CLICK_WINDOW
            });

            self.last_click = Some(ClickState {
                target: click_target,
                at: Instant::now(),
            });

            match target {
                FormSelection::Section(section_index) => {
                    self.selected_form = FormSelection::Section(section_index);
                    self.toggle_section(section_index);
                }
                FormSelection::Field(field_index) => {
                    self.selected_form = FormSelection::Field(field_index);
                    if double_click {
                        self.enter_edit_mode(field_index);
                    }
                }
            }
            return;
        }

        if layout.in_form_block(mouse_event.column, mouse_event.row) {
            self.pane_focus = PaneFocus::Parameters;
            self.last_click = Some(ClickState {
                target: ClickTarget::Pane(PaneFocus::Parameters),
                at: Instant::now(),
            });
        }
    }

    fn handle_left_mouse_drag(&mut self, mouse_event: MouseEvent, layout: &UiLayout) {
        if self.dragging_divider {
            let relative = mouse_event
                .column
                .saturating_sub(layout.body.x)
                .min(layout.body.width.saturating_sub(1));
            let ratio = f32::from(relative) / f32::from(layout.body.width.max(1));
            self.config_store.set_pane_split_ratio(ratio);
            return;
        }
        if self.dragging_result_scrollbar {
            self.scroll_result_to_mouse(mouse_event.row, layout);
            return;
        }
        if self.dragging_result_hscrollbar {
            self.scroll_result_horizontally_to_mouse(mouse_event.column, layout);
        }
    }

    fn handle_mouse_scroll(&mut self, mouse_event: MouseEvent, layout: &UiLayout, delta: i16) {
        if layout.in_result_block(mouse_event.column, mouse_event.row) {
            self.pane_focus = PaneFocus::Result;
            self.scroll_active_result(delta);
            return;
        }
        if layout.in_form_block(mouse_event.column, mouse_event.row) && self.running.is_none() {
            self.pane_focus = PaneFocus::Parameters;
            if delta.is_negative() {
                for _ in 0..delta.unsigned_abs() {
                    self.select_next_form_item();
                }
            } else {
                for _ in 0..delta as u16 {
                    self.select_previous_form_item();
                }
            }
        }
    }

    fn handle_mouse_hscroll(&mut self, mouse_event: MouseEvent, layout: &UiLayout, delta: i16) {
        if layout.in_result_block(mouse_event.column, mouse_event.row) {
            self.pane_focus = PaneFocus::Result;
            self.scroll_active_result_horizontal(delta);
        }
    }

    fn toggle_theme(&mut self) {
        let next = self.theme_mode().toggle();
        self.config_store.set_theme_mode(next);
    }

    fn toggle_focus(&mut self) {
        self.pane_focus = match self.pane_focus {
            PaneFocus::Parameters => PaneFocus::Result,
            PaneFocus::Result => {
                if self.running.is_some() || self.sections.is_empty() {
                    PaneFocus::Result
                } else {
                    PaneFocus::Parameters
                }
            }
        };
    }

    fn toggle_advanced(&mut self) {
        self.show_advanced = !self.show_advanced;
        self.ensure_visible_form_selection();
    }

    fn visible_form_targets(&self) -> Vec<FormSelection> {
        self.form_rows().into_iter().map(|row| row.target).collect()
    }

    fn select_previous_form_item(&mut self) {
        let visible = self.visible_form_targets();
        if visible.is_empty() {
            return;
        }
        let position = visible
            .iter()
            .position(|target| *target == self.selected_form)
            .unwrap_or(0);
        let next = if position == 0 {
            visible.len() - 1
        } else {
            position - 1
        };
        self.selected_form = visible[next];
    }

    fn select_next_form_item(&mut self) {
        let visible = self.visible_form_targets();
        if visible.is_empty() {
            return;
        }
        let position = visible
            .iter()
            .position(|target| *target == self.selected_form)
            .unwrap_or(0);
        self.selected_form = visible[(position + 1) % visible.len()];
    }

    fn activate_selected_form_item(&mut self) {
        match self.selected_form {
            FormSelection::Section(section_index) => self.toggle_section(section_index),
            FormSelection::Field(field_index) => {
                let Some(field) = self.fields.get(field_index) else {
                    return;
                };
                match &field.value {
                    FormValue::Text(_) => self.enter_edit_mode(field_index),
                    FormValue::Toggle(_) => self.toggle_field(field_index),
                    FormValue::Choice { .. } => self.cycle_field_choice(field_index, true),
                }
            }
        }
    }

    fn toggle_section(&mut self, section_index: usize) {
        let Some(section) = self.sections.get_mut(section_index) else {
            return;
        };
        section.collapsed = !section.collapsed;
        self.selected_form = FormSelection::Section(section_index);
        self.ensure_visible_form_selection();
    }

    fn enter_edit_mode(&mut self, field_index: usize) {
        let Some(field) = self.fields.get(field_index) else {
            return;
        };
        if let FormValue::Text(value) = &field.value {
            self.edit_state = Some(EditState {
                field_index,
                buffer: value.clone(),
            });
        }
    }

    fn toggle_field(&mut self, field_index: usize) {
        if let Some(field) = self.fields.get_mut(field_index) {
            field.toggle();
        }
        self.sync_result_tab_visibility();
    }

    fn cycle_field_choice(&mut self, field_index: usize, forward: bool) {
        if let Some(field) = self.fields.get_mut(field_index) {
            field.cycle_choice(forward);
        }
    }

    fn adjust_selected_choice(&mut self, forward: bool) {
        if let FormSelection::Field(field_index) = self.selected_form {
            self.cycle_field_choice(field_index, forward);
        }
    }

    fn scroll_active_result(&mut self, delta: i16) {
        let scroll = &mut self.result_scrolls[self.active_result_tab.index()];
        if delta.is_negative() {
            *scroll = scroll.saturating_sub(delta.unsigned_abs());
        } else {
            *scroll = scroll.saturating_add(delta as u16);
        }
    }

    fn set_active_result_scroll(&mut self, scroll: usize) {
        self.result_scrolls[self.active_result_tab.index()] = scroll.min(u16::MAX as usize) as u16;
    }

    fn scroll_active_result_horizontal(&mut self, delta: i16) {
        let current = self.result_hscrolls[self.active_result_tab.index()] as usize;
        let next = if delta.is_negative() {
            current.saturating_sub(delta.unsigned_abs() as usize)
        } else {
            current.saturating_add(delta as usize)
        };
        self.set_active_result_hscroll(next);
    }

    fn set_active_result_hscroll(&mut self, scroll: usize) {
        self.result_hscrolls[self.active_result_tab.index()] = scroll.min(u16::MAX as usize) as u16;
    }

    fn scroll_result_to_mouse(&mut self, row: u16, layout: &UiLayout) {
        let Some(track) = layout.result_scrollbar else {
            return;
        };
        let Some((content_length, viewport_length)) =
            self.active_result_scroll_metrics(track.height)
        else {
            return;
        };
        if content_length <= viewport_length || track.height == 0 {
            self.set_active_result_scroll(0);
            return;
        }

        let max_scroll = content_length.saturating_sub(viewport_length);
        let row_offset = row
            .saturating_sub(track.y)
            .min(track.height.saturating_sub(1)) as usize;
        let denominator = track.height.saturating_sub(1) as usize;
        let scroll = if denominator == 0 {
            0
        } else {
            (row_offset * max_scroll + denominator / 2) / denominator
        };
        self.set_active_result_scroll(scroll);
    }

    fn scroll_result_horizontally_to_mouse(&mut self, column: u16, layout: &UiLayout) {
        let Some(track) = layout.result_hscrollbar else {
            return;
        };
        let Some((content_width, viewport_width)) = self.active_result_hscroll_metrics(track.width)
        else {
            return;
        };
        if content_width <= viewport_width || track.width == 0 {
            self.set_active_result_hscroll(0);
            return;
        }

        let max_scroll = content_width.saturating_sub(viewport_width);
        let thumb_length =
            scrollbar_thumb_length(track.width as usize, content_width, viewport_width);
        let max_thumb_offset = track.width as usize - thumb_length;
        let scroll = if max_thumb_offset == 0 {
            0
        } else {
            let column_offset = column.saturating_sub(track.x) as usize;
            let adjusted =
                column_offset.saturating_sub(self.dragging_result_hscrollbar_grab as usize);
            let thumb_offset = adjusted.min(max_thumb_offset);
            (thumb_offset * max_scroll + max_thumb_offset / 2) / max_thumb_offset
        };
        self.set_active_result_hscroll(scroll);
    }

    fn active_result_scroll_metrics(&self, viewport_height: u16) -> Option<(usize, usize)> {
        let content = self.active_result_content();
        let viewport_length = match &content {
            ResultContent::Lines(_) => viewport_height as usize,
            ResultContent::Table(_) => viewport_height.saturating_sub(1) as usize,
        };
        if viewport_length == 0 {
            return None;
        }
        let content_length = match content {
            ResultContent::Lines(lines) => lines.len(),
            ResultContent::Table(table) => table.rows.len(),
        };
        Some((content_length, viewport_length))
    }

    fn active_result_hscroll_metrics(&self, viewport_width: u16) -> Option<(usize, usize)> {
        let content = self.active_result_content();
        let viewport_width = viewport_width as usize;
        if viewport_width == 0 {
            return None;
        }
        let content_width = match content {
            ResultContent::Lines(lines) => lines
                .iter()
                .map(|line| line.chars().count())
                .max()
                .unwrap_or(0),
            ResultContent::Table(table) => table.content_width(),
        };
        Some((content_width, viewport_width))
    }

    fn result_hscrollbar_grab_offset(&self, column: u16, layout: &UiLayout) -> u16 {
        let Some(track) = layout.result_hscrollbar else {
            return 0;
        };
        let Some((content_width, viewport_width)) = self.active_result_hscroll_metrics(track.width)
        else {
            return 0;
        };
        if content_width <= viewport_width || track.width == 0 {
            return 0;
        }

        let thumb_length =
            scrollbar_thumb_length(track.width as usize, content_width, viewport_width);
        let max_scroll = content_width.saturating_sub(viewport_width);
        let max_thumb_offset = track.width as usize - thumb_length;
        if max_scroll == 0 || max_thumb_offset == 0 {
            return 0;
        }

        let thumb_offset = (self.active_result_hscroll() as usize * max_thumb_offset
            + max_scroll / 2)
            / max_scroll;
        let thumb_start = track.x as usize + thumb_offset;
        let thumb_end = thumb_start + thumb_length;
        let click = column as usize;
        if (thumb_start..thumb_end).contains(&click) {
            (click - thumb_start) as u16
        } else {
            (thumb_length / 2) as u16
        }
    }

    fn cycle_visible_result_tab(&mut self, forward: bool) {
        let tabs = self.visible_result_tabs();
        let Some(position) = tabs.iter().position(|tab| *tab == self.active_result_tab) else {
            self.active_result_tab = ResultTab::Overview;
            return;
        };
        let next = if forward {
            (position + 1) % tabs.len()
        } else if position == 0 {
            tabs.len() - 1
        } else {
            position - 1
        };
        self.active_result_tab = tabs[next];
    }

    fn ensure_visible_form_selection(&mut self) {
        let visible = self.visible_form_targets();
        if visible.is_empty() {
            self.selected_form = FormSelection::Section(0);
            return;
        }
        if !visible.contains(&self.selected_form) {
            self.selected_form = visible[0];
        }
    }

    fn start_run(&mut self) {
        self.commit_edit_buffer();

        if self.schema.is_none() {
            self.result.status_line = "Cannot run without a loaded UI schema.".to_string();
            self.result.status_kind = StatusKind::Error;
            self.active_result_tab = ResultTab::Stderr;
            return;
        }

        match self.build_execution_plan() {
            Ok(plan) => match spawn_process(&plan) {
                Ok(process) => {
                    self.result = ResultState {
                        status_line: format!("Running {}...", self.app.id),
                        status_kind: StatusKind::Running,
                        file_output_path: plan.file_output_path.clone(),
                        ..ResultState::default()
                    };
                    self.edit_state = None;
                    self.pane_focus = PaneFocus::Result;
                    self.active_result_tab = ResultTab::Overview;
                    self.result_scrolls = [0; 9];
                    self.result_hscrolls = [0; 9];
                    self.running = Some(RunningState {
                        process,
                        renderer: plan.renderer,
                        file_output_path: plan.file_output_path,
                        cancel_requested: false,
                    });
                }
                Err(error) => {
                    self.result.status_line = format!("Failed to launch {}.", self.app.id);
                    self.result.status_kind = StatusKind::Error;
                    self.result.stderr = format!("{error}\n");
                    self.active_result_tab = ResultTab::Stderr;
                }
            },
            Err(error) => {
                self.result.status_line = "Cannot start command.".to_string();
                self.result.status_kind = StatusKind::Error;
                self.result.stderr = format!("{error}\n");
                self.active_result_tab = ResultTab::Stderr;
            }
        }
    }

    fn build_execution_plan(&self) -> Result<ExecutionPlan, String> {
        let schema = self
            .schema
            .as_ref()
            .ok_or_else(|| "missing command schema".to_string())?;

        let mut arguments = Vec::<OsString>::new();
        let force_selectdata = self.selection_inputs_present();
        for field in &self.fields {
            if field.schema.id == "selectdata" {
                continue;
            }
            field.append_arguments(&mut arguments)?;
        }
        self.append_effective_selectdata_argument(&mut arguments, force_selectdata)?;

        let output = self.field_text("output");
        let listfile = self.field_text("listfile");
        if output.as_deref().is_some_and(|value| !value.is_empty())
            && listfile.as_deref().is_some_and(|value| !value.is_empty())
        {
            return Err("Choose either --output or --listfile, not both.".to_string());
        }

        let file_output_path = output
            .filter(|value| !value.is_empty())
            .or_else(|| listfile.filter(|value| !value.is_empty()));

        if let Some(managed_output) = &schema.managed_output {
            inject_managed_arguments(&mut arguments, managed_output);
        }

        Ok(ExecutionPlan {
            command: self.app.resolve_command()?,
            arguments,
            renderer: schema
                .managed_output
                .as_ref()
                .map(|value| value.renderer.clone()),
            file_output_path,
        })
    }

    fn commit_edit_buffer(&mut self) {
        let Some(edit_state) = self.edit_state.take() else {
            return;
        };
        if let Some(field) = self.fields.get_mut(edit_state.field_index) {
            field.set_text(edit_state.buffer);
        }
    }

    fn field_text(&self, id: &str) -> Option<String> {
        self.fields
            .iter()
            .find(|field| field.schema.id == id)
            .and_then(|field| field.text_value())
    }

    fn bool_field_value(&self, id: &str) -> Option<bool> {
        self.fields
            .iter()
            .find(|field| field.schema.id == id)
            .and_then(|field| match &field.value {
                FormValue::Toggle(value) => Some(*value),
                _ => None,
            })
    }

    fn verbose_enabled(&self) -> bool {
        self.bool_field_value("verbose").unwrap_or(true)
    }

    fn listunfl_enabled(&self) -> bool {
        self.bool_field_value("listunfl").unwrap_or(false)
    }

    fn visible_result_tabs(&self) -> &'static [ResultTab] {
        const COMPACT: [ResultTab; 7] = [
            ResultTab::Overview,
            ResultTab::Observations,
            ResultTab::Fields,
            ResultTab::Spws,
            ResultTab::Antennas,
            ResultTab::Stdout,
            ResultTab::Stderr,
        ];
        if self.verbose_enabled() {
            &ResultTab::ALL
        } else {
            &COMPACT
        }
    }

    fn sync_result_tab_visibility(&mut self) {
        if !self.visible_result_tabs().contains(&self.active_result_tab) {
            self.active_result_tab = ResultTab::Overview;
        }
    }

    fn selection_inputs_present(&self) -> bool {
        self.fields.iter().any(|field| {
            field.schema.group == "Selection"
                && field.schema.id != "selectdata"
                && match &field.value {
                    FormValue::Text(value) => !value.trim().is_empty(),
                    FormValue::Choice { value, .. } => !value.trim().is_empty(),
                    FormValue::Toggle(_) => false,
                }
        })
    }

    fn append_effective_selectdata_argument(
        &self,
        arguments: &mut Vec<OsString>,
        force_selectdata: bool,
    ) -> Result<(), String> {
        let Some(field) = self
            .fields
            .iter()
            .find(|field| field.schema.id == "selectdata")
        else {
            return Ok(());
        };
        let FormValue::Toggle(raw_value) = &field.value else {
            return Err("internal selectdata field mismatch".to_string());
        };
        let effective = *raw_value || force_selectdata;
        let UiArgumentParser::Toggle {
            true_flags,
            false_flags,
        } = &field.schema.parser
        else {
            return Err("internal selectdata parser mismatch".to_string());
        };
        match (effective, true_flags.first(), false_flags.first()) {
            (true, Some(flag), _) => arguments.push(OsString::from(flag)),
            (false, _, Some(flag)) => arguments.push(OsString::from(flag)),
            _ => {}
        }
        Ok(())
    }

    fn cancel_current(&mut self) {
        let Some(running) = self.running.as_mut() else {
            return;
        };
        if running.cancel_requested {
            return;
        }
        match running.process.cancel() {
            Ok(()) => {
                running.cancel_requested = true;
                self.result.status_line = format!("Cancel requested for {}...", self.app.id);
                self.result.status_kind = StatusKind::Warning;
            }
            Err(error) => {
                self.result.status_line = "Failed to cancel running command.".to_string();
                self.result.status_kind = StatusKind::Error;
                self.result.stderr.push_str(&format!("{error}\n"));
                self.active_result_tab = ResultTab::Stderr;
            }
        }
    }

    fn finish_execution(&mut self, exit_code: Option<i32>, success: bool) {
        let Some(running) = self.running.take() else {
            return;
        };
        self.result.exit_code = exit_code;
        self.result.file_output_path = running.file_output_path.clone();

        if running.cancel_requested {
            self.result.status_line = "Execution canceled.".to_string();
            self.result.status_kind = StatusKind::Warning;
            self.result.structured = None;
            self.result.structured_error = Some(
                "The child process was canceled before a structured result was available."
                    .to_string(),
            );
            self.active_result_tab = if !self.result.stderr.is_empty() {
                ResultTab::Stderr
            } else {
                ResultTab::Stdout
            };
            return;
        }

        if success {
            self.result.status_line = "Execution completed successfully.".to_string();
            self.result.status_kind = StatusKind::Ok;
            if let Some(path) = running.file_output_path {
                self.result.structured = None;
                self.result.structured_error = None;
                self.result.file_output_path = Some(path);
                self.active_result_tab = ResultTab::Overview;
                return;
            }

            if matches!(running.renderer.as_deref(), Some("listobs-summary-v1")) {
                match serde_json::from_str::<ListObsSummary>(&self.result.stdout) {
                    Ok(summary) => {
                        self.result.structured = Some(summary);
                        self.result.structured_error = None;
                        self.active_result_tab = ResultTab::Overview;
                    }
                    Err(error) => {
                        self.result.structured = None;
                        self.result.structured_error =
                            Some(format!("Failed to parse structured output: {error}"));
                        self.result.status_line =
                            "Execution completed, but structured rendering failed.".to_string();
                        self.result.status_kind = StatusKind::Warning;
                        self.active_result_tab = if !self.result.stdout.is_empty() {
                            ResultTab::Stdout
                        } else {
                            ResultTab::Stderr
                        };
                    }
                }
            } else {
                self.active_result_tab = if !self.result.stdout.is_empty() {
                    ResultTab::Stdout
                } else {
                    ResultTab::Overview
                };
            }
        } else {
            self.result.status_line = "Execution failed.".to_string();
            self.result.status_kind = StatusKind::Error;
            self.result.structured = None;
            self.result.structured_error = None;
            self.active_result_tab = if !self.result.stderr.is_empty() {
                ResultTab::Stderr
            } else {
                ResultTab::Stdout
            };
        }
    }

    fn overview_lines(&self) -> Vec<String> {
        if let Some(summary) = &self.result.structured {
            let mut lines = Vec::new();
            lines.push("MeasurementSet".to_string());
            lines.push(format!(
                "Path: {}",
                summary
                    .measurement_set
                    .path
                    .clone()
                    .unwrap_or_else(|| "<in-memory>".to_string())
            ));
            lines.push(format!(
                "Rows: {}   Observations: {}   Fields: {}   SPWs: {}   Antennas: {}",
                summary.measurement_set.row_count,
                summary.measurement_set.observation_count,
                summary.measurement_set.field_count,
                summary.measurement_set.spectral_window_count,
                summary.measurement_set.antenna_count
            ));
            lines.push(format!(
                "Time range: {} -> {}",
                format_optional_mjd_timestamp(summary.measurement_set.start_mjd_seconds),
                format_optional_mjd_timestamp(summary.measurement_set.end_mjd_seconds)
            ));
            lines.push(String::new());
            lines.push("Badges".to_string());
            lines.push(format!(
                "MS_VERSION={}  TimeRef={}  Sources={}  DDIDs={}",
                format_optional_float(summary.measurement_set.ms_version),
                summary
                    .measurement_set
                    .time_reference
                    .clone()
                    .unwrap_or_else(|| "unknown".to_string()),
                summary.measurement_set.source_count,
                summary.measurement_set.data_description_count
            ));
            lines.push(String::new());
            lines.push("Current tabs".to_string());
            lines.push(
                "Use h/l or Left/Right to switch between Overview, Observations, Fields, SPWs, Antennas, Stdout, and Stderr."
                    .to_string(),
            );
            return lines;
        }

        if let Some(path) = &self.result.file_output_path {
            return vec![
                "Overview".to_string(),
                format!("Output written to: {path}"),
                "Switch to Stdout or Stderr if the command emitted terminal output.".to_string(),
            ];
        }

        if self.running.is_some() {
            return vec![
                "Overview".to_string(),
                "The process is still running.".to_string(),
                "Structured result tabs will populate when the command exits.".to_string(),
            ];
        }

        if let Some(error) = &self.result.structured_error {
            return vec![
                "Overview".to_string(),
                error.clone(),
                "Switch to Stdout or Stderr for raw process output.".to_string(),
            ];
        }

        vec![
            "Overview".to_string(),
            "No structured result available yet.".to_string(),
        ]
    }
}

fn spinner_frames(theme_mode: ThemeMode) -> &'static [&'static str] {
    match theme_mode {
        ThemeMode::DenseAnsi => DENSE_SPINNER_FRAMES,
        ThemeMode::RichPanel => RICH_SPINNER_FRAMES,
    }
}

impl FormField {
    fn from_schema(schema: &UiArgumentSchema) -> Option<Self> {
        if schema.hidden_in_tui || matches!(schema.parser, UiArgumentParser::Action { .. }) {
            return None;
        }

        let value = match &schema.parser {
            UiArgumentParser::Positional { .. } => {
                FormValue::Text(schema.default.clone().unwrap_or_default())
            }
            UiArgumentParser::Option { choices, .. } => {
                if choices.is_empty() {
                    FormValue::Text(schema.default.clone().unwrap_or_default())
                } else {
                    FormValue::Choice {
                        value: schema.default.clone().unwrap_or_else(|| choices[0].clone()),
                        choices: choices.clone(),
                    }
                }
            }
            UiArgumentParser::Toggle { .. } => {
                FormValue::Toggle(schema.default_bool().unwrap_or(false))
            }
            UiArgumentParser::Action { .. } => return None,
        };

        Some(Self {
            schema: schema.clone(),
            value,
        })
    }

    fn render_line(&self, edit_state: Option<&EditState>, field_index: usize) -> String {
        let value = match (&self.value, edit_state) {
            (_, Some(edit_state)) if edit_state.field_index == field_index => {
                format!("{}|", edit_state.buffer)
            }
            (FormValue::Text(value), _) if value.trim().is_empty() && self.schema.required => {
                "<required>".to_string()
            }
            (FormValue::Text(value), _) if value.trim().is_empty() => "<empty>".to_string(),
            (FormValue::Text(value), _) => value.clone(),
            (FormValue::Toggle(value), _) => {
                if *value {
                    "[x]".to_string()
                } else {
                    "[ ]".to_string()
                }
            }
            (FormValue::Choice { value, .. }, _) => value.clone(),
        };
        format!("{:<18} {}", self.schema.label, value)
    }

    fn append_arguments(&self, arguments: &mut Vec<OsString>) -> Result<(), String> {
        match (&self.schema.parser, &self.value) {
            (UiArgumentParser::Positional { .. }, FormValue::Text(value)) => {
                if self.schema.required && value.trim().is_empty() {
                    return Err(format!("{} is required.", self.schema.label));
                }
                if !value.trim().is_empty() {
                    arguments.push(OsString::from(value));
                }
            }
            (UiArgumentParser::Option { flags, .. }, FormValue::Text(value)) => {
                if !value.trim().is_empty() {
                    arguments.push(OsString::from(&flags[0]));
                    arguments.push(OsString::from(value));
                }
            }
            (UiArgumentParser::Option { flags, .. }, FormValue::Choice { value, .. }) => {
                arguments.push(OsString::from(&flags[0]));
                arguments.push(OsString::from(value));
            }
            (
                UiArgumentParser::Toggle {
                    true_flags,
                    false_flags,
                },
                FormValue::Toggle(value),
            ) => match (*value, true_flags.first(), false_flags.first()) {
                (true, Some(flag), _) => arguments.push(OsString::from(flag)),
                (false, _, Some(flag)) => arguments.push(OsString::from(flag)),
                _ => {}
            },
            _ => return Err(format!("internal argument mismatch for {}", self.schema.id)),
        }
        Ok(())
    }

    fn cycle_choice(&mut self, forward: bool) {
        let FormValue::Choice { value, choices } = &mut self.value else {
            return;
        };
        if choices.is_empty() {
            return;
        }
        let position = choices
            .iter()
            .position(|choice| choice == value)
            .unwrap_or(0);
        let next = if forward {
            (position + 1) % choices.len()
        } else if position == 0 {
            choices.len() - 1
        } else {
            position - 1
        };
        *value = choices[next].clone();
    }

    fn toggle(&mut self) {
        if let FormValue::Toggle(value) = &mut self.value {
            *value = !*value;
        }
    }

    fn set_text(&mut self, value: String) {
        if let FormValue::Text(current) = &mut self.value {
            *current = value;
        }
    }

    #[cfg(test)]
    fn set_toggle(&mut self, value: bool) {
        if let FormValue::Toggle(current) = &mut self.value {
            *current = value;
        }
    }

    fn text_value(&self) -> Option<String> {
        match &self.value {
            FormValue::Text(value) => Some(value.clone()),
            FormValue::Choice { value, .. } => Some(value.clone()),
            FormValue::Toggle(_) => None,
        }
    }
}

fn build_sections(fields: &[FormField]) -> Vec<FormSection> {
    let mut sections = Vec::<FormSection>::new();
    for (field_index, field) in fields.iter().enumerate() {
        if let Some(section) = sections
            .iter_mut()
            .find(|section| section.name == field.schema.group)
        {
            section.field_indices.push(field_index);
        } else {
            sections.push(FormSection {
                collapsed: !matches!(
                    field.schema.group.as_str(),
                    "Input" | "Presentation" | "Selection"
                ),
                name: field.schema.group.clone(),
                field_indices: vec![field_index],
            });
        }
    }
    sections
}

fn initial_form_selection(
    sections: &[FormSection],
    fields: &[FormField],
    show_advanced: bool,
) -> FormSelection {
    for (section_index, section) in sections.iter().enumerate() {
        let visible_fields = section
            .field_indices
            .iter()
            .copied()
            .filter(|index| show_advanced || !fields[*index].schema.advanced)
            .collect::<Vec<_>>();
        if visible_fields.is_empty() {
            continue;
        }
        if section.collapsed {
            return FormSelection::Section(section_index);
        }
        return FormSelection::Field(visible_fields[0]);
    }
    FormSelection::Section(0)
}

fn inject_managed_arguments(arguments: &mut Vec<OsString>, managed_output: &UiManagedOutputSchema) {
    for argument in &managed_output.inject_arguments {
        arguments.push(OsString::from(&argument.flag));
        arguments.push(OsString::from(&argument.value));
    }
}

fn raw_lines(label: &str, text: &str) -> Vec<String> {
    if text.trim().is_empty() {
        vec![format!("No {label} output.")]
    } else {
        text.lines().map(ToString::to_string).collect()
    }
}

fn build_observations_table(summary: &ListObsSummary) -> TableView {
    let header_cells = vec![
        "Obs".to_string(),
        "Telescope".to_string(),
        "Observer".to_string(),
        "Project".to_string(),
        "Start".to_string(),
        "End".to_string(),
    ];
    let mut widths = header_cells
        .iter()
        .map(|cell| cell.chars().count())
        .collect::<Vec<_>>();
    let row_cells = summary
        .observations
        .iter()
        .map(|observation| {
            let cells = vec![
                observation.observation_id.to_string(),
                truncate(&observation.telescope_name, 10),
                truncate(&observation.observer, 10),
                truncate(&observation.project, 12),
                format_optional_mjd_timestamp(observation.start_mjd_seconds),
                format_optional_mjd_timestamp(observation.end_mjd_seconds),
            ];
            for (index, cell) in cells.iter().enumerate() {
                widths[index] = widths[index].max(cell.chars().count());
            }
            cells
        })
        .collect::<Vec<_>>();
    let rows = row_cells
        .iter()
        .map(|cells| format_columns(cells, &widths))
        .collect();
    TableView {
        header: format_columns(&header_cells, &widths),
        rows,
    }
}

fn build_scans_table(summary: &ListObsSummary, listunfl: bool) -> TableView {
    let rows = summary
        .scans
        .iter()
        .map(|scan| {
            let start = MvTime::from_mjd_seconds(scan.start_mjd_seconds).format_dmy(1);
            let end = MvTime::from_mjd_seconds(scan.end_mjd_seconds).format_time(1);
            let field_names = if scan.field_names.is_empty() {
                scan.field_name.clone()
            } else {
                scan.field_names.join(",")
            };
            let intents = if scan.scan_intents.is_empty() {
                "-".to_string()
            } else {
                scan.scan_intents.join(",")
            };
            if listunfl {
                format!(
                    "{:<3} {:<3} {:<23} {:<10} {:<4} {:<4} {:<18} {:<7} {:<8} {:<7} {:<8} {}",
                    scan.observation_id,
                    scan.array_id,
                    start,
                    end,
                    scan.scan_number,
                    scan.field_id,
                    truncate(&field_names, 18),
                    scan.row_count,
                    format_float_compact(scan.unflagged_row_count.unwrap_or(0.0), 2),
                    format_i32_list(&scan.spectral_window_ids),
                    format_float_list(&scan.mean_interval_seconds_by_spw, 2),
                    truncate(&intents, 28),
                )
            } else {
                format!(
                    "{:<3} {:<3} {:<23} {:<10} {:<4} {:<4} {:<20} {:<7} {:<7} {:<8} {}",
                    scan.observation_id,
                    scan.array_id,
                    start,
                    end,
                    scan.scan_number,
                    scan.field_id,
                    truncate(&field_names, 20),
                    scan.row_count,
                    format_i32_list(&scan.spectral_window_ids),
                    format_float_list(&scan.mean_interval_seconds_by_spw, 2),
                    truncate(&intents, 32),
                )
            }
        })
        .collect();
    let header = if listunfl {
        format!(
            "{:<3} {:<3} {:<23} {:<10} {:<4} {:<4} {:<18} {:<7} {:<8} {:<7} {:<8} {}",
            "Obs",
            "Arr",
            "Start",
            "End",
            "Scan",
            "Fld",
            "Field",
            "Rows",
            "nUnfl",
            "Spws",
            "Int(s)",
            "Intent"
        )
    } else {
        format!(
            "{:<3} {:<3} {:<23} {:<10} {:<4} {:<4} {:<20} {:<7} {:<7} {:<8} {}",
            "Obs",
            "Arr",
            "Start",
            "End",
            "Scan",
            "Fld",
            "Field",
            "Rows",
            "Spws",
            "Int(s)",
            "Intent"
        )
    };
    TableView { header, rows }
}

fn build_fields_table(summary: &ListObsSummary, listunfl: bool) -> TableView {
    let rows = summary
        .fields
        .iter()
        .map(|field| {
            let ra = MvAngle::from_radians(field.phase_direction_radians[0])
                .normalized(0.0)
                .format_time(6);
            let dec = MvAngle::from_radians(field.phase_direction_radians[1]).format_angle_dig2(5);
            if listunfl {
                format!(
                    "{:<5} {:<5} {:<20} {:<16} {:<16} {:<14} {:<6} {:<7} {}",
                    field.field_id,
                    truncate(&field.code, 5),
                    truncate(&field.name, 20),
                    ra,
                    dec,
                    truncate(field.direction_reference.as_deref().unwrap_or("?"), 14),
                    field.source_id,
                    field.row_count,
                    format_float_compact(field.unflagged_row_count.unwrap_or(0.0), 2),
                )
            } else {
                format!(
                    "{:<5} {:<5} {:<20} {:<16} {:<16} {:<14} {:<6} {}",
                    field.field_id,
                    truncate(&field.code, 5),
                    truncate(&field.name, 20),
                    ra,
                    dec,
                    truncate(field.direction_reference.as_deref().unwrap_or("?"), 14),
                    field.source_id,
                    field.row_count,
                )
            }
        })
        .collect();
    let header = if listunfl {
        format!(
            "{:<5} {:<5} {:<20} {:<16} {:<16} {:<14} {:<6} {:<7} {}",
            "ID", "Code", "Name", "RA", "Dec", "Epoch", "SrcId", "nRows", "nUnflRows"
        )
    } else {
        format!(
            "{:<5} {:<5} {:<20} {:<16} {:<16} {:<14} {:<6} {}",
            "ID", "Code", "Name", "RA", "Dec", "Epoch", "SrcId", "nRows"
        )
    };
    TableView { header, rows }
}

fn build_spws_table(summary: &ListObsSummary) -> TableView {
    let rows = summary
        .spectral_windows
        .iter()
        .map(|spw| {
            format!(
                "{:<5} {:<8} {:<6} {:<7} {:<11} {:<12} {:<11} {:<12} {}",
                spw.spectral_window_id,
                truncate(&spw_display_name(spw.name.as_str()), 8),
                spw.num_channels,
                truncate(spw.frame.as_deref().unwrap_or("unknown"), 7),
                format!("{:.3}", spw.first_channel_frequency_hz / 1.0e6),
                format!("{:.3}", spw.channel_width_hz / 1.0e3),
                format!("{:.1}", spw.total_bandwidth_hz / 1.0e3),
                format!("{:.4}", spw.center_frequency_hz / 1.0e6),
                join_corrs(&spw.correlation_types),
            )
        })
        .collect();
    TableView {
        header: format!(
            "{:<5} {:<8} {:<6} {:<7} {:<11} {:<12} {:<11} {:<12} {}",
            "SpwID",
            "Name",
            "#Chans",
            "Frame",
            "Ch0(MHz)",
            "ChanWid(kHz)",
            "TotBW(kHz)",
            "CtrFreq(MHz)",
            "Corrs"
        ),
        rows,
    }
}

fn build_sources_table(summary: &ListObsSummary) -> TableView {
    let rows = summary
        .sources
        .iter()
        .map(|source| {
            format!(
                "{:<5} {:<20} {:<5} {:<14} {}",
                source.source_id,
                truncate(&source.name, 20),
                if source.spectral_window_id < 0 {
                    "any".to_string()
                } else {
                    source.spectral_window_id.to_string()
                },
                source
                    .rest_frequency_hz
                    .map(|value| format!("{:.6}", value / 1.0e6))
                    .unwrap_or_else(|| "-".to_string()),
                source
                    .system_velocity_m_s
                    .map(|value| format_float_compact(value / 1.0e3, 3))
                    .unwrap_or_else(|| "-".to_string()),
            )
        })
        .collect();
    TableView {
        header: format!(
            "{:<5} {:<20} {:<5} {:<14} {}",
            "ID", "Name", "SpwId", "RestFreq(MHz)", "SysVel(km/s)"
        ),
        rows,
    }
}

fn build_antennas_table(summary: &ListObsSummary) -> TableView {
    let rows = summary
        .antennas
        .iter()
        .map(|antenna| {
            format!(
                "{:<4} {:<6} {:<10} {:>5.1} {:<14} {:<13} {:>9.1} {:>9.1} {:>10.1} {:>14.3} {:>14.3} {:>14.3}",
                antenna.antenna_id,
                truncate(&antenna.name, 6),
                truncate(&antenna.station, 10),
                antenna.dish_diameter_m,
                MvAngle::from_radians(antenna.longitude_radians).format_angle(1),
                MvAngle::from_radians(antenna.latitude_radians).format_angle_dig2(1),
                antenna.offset_from_observatory_m[0],
                antenna.offset_from_observatory_m[1],
                antenna.offset_from_observatory_m[2],
                antenna.position_m[0],
                antenna.position_m[1],
                antenna.position_m[2],
            )
        })
        .collect();
    TableView {
        header: format!(
            "{:<4} {:<6} {:<10} {:<5} {:<14} {:<13} {:>9} {:>9} {:>10} {:>14} {:>14} {:>14}",
            "ID",
            "Name",
            "Station",
            "Diam.",
            "Long.",
            "Lat.",
            "East",
            "North",
            "Elev",
            "ITRF x",
            "ITRF y",
            "ITRF z"
        ),
        rows,
    }
}

fn build_compact_antenna_lines(summary: &ListObsSummary) -> Vec<String> {
    let mut lines = vec![format!(
        "Antennas: {} 'name'='station'",
        summary.antennas.len()
    )];
    if summary.antennas.is_empty() {
        return lines;
    }
    let mut line = String::new();
    let mut first_id = summary.antennas[0].antenna_id;
    let mut last_id = summary.antennas[0].antenna_id;
    let max_id = summary
        .antennas
        .iter()
        .map(|antenna| antenna.antenna_id)
        .max()
        .unwrap_or(first_id);

    for antenna in &summary.antennas {
        let fragment = format!("'{}'='{}'", antenna.name, antenna.station);
        let mut candidate = line.clone();
        if !candidate.is_empty() {
            candidate.push_str(", ");
        }
        candidate.push_str(&fragment);
        if candidate.len() > 55 && !line.is_empty() {
            lines.push(format!("ID={first_id}-{last_id}: {line}"));
            line = fragment;
            first_id = antenna.antenna_id;
        } else {
            line = candidate;
        }
        last_id = antenna.antenna_id;
        if antenna.antenna_id == max_id {
            lines.push(format!("ID={first_id}-{last_id}: {line}"));
        }
    }
    lines
}

fn format_optional_float(value: Option<f64>) -> String {
    value
        .map(|value| format!("{value:.3}"))
        .unwrap_or_else(|| "n/a".to_string())
}

fn format_optional_mjd_timestamp(value: Option<f64>) -> String {
    value
        .map(|value| MvTime::from_mjd_seconds(value).format_dmy(1))
        .unwrap_or_else(|| "n/a".to_string())
}

fn format_float_compact(value: f64, decimals: usize) -> String {
    let mut text = format!("{value:.decimals$}");
    while text.contains('.') && text.ends_with('0') {
        text.pop();
    }
    if text.ends_with('.') {
        text.pop();
    }
    text
}

fn format_i32_list(values: &[i32]) -> String {
    format!(
        "[{}]",
        values
            .iter()
            .map(i32::to_string)
            .collect::<Vec<_>>()
            .join(",")
    )
}

fn format_float_list(values: &[f64], decimals: usize) -> String {
    format!(
        "[{}]",
        values
            .iter()
            .map(|value| format_float_compact(*value, decimals))
            .collect::<Vec<_>>()
            .join(",")
    )
}

fn join_corrs(values: &[String]) -> String {
    values
        .iter()
        .map(|value| format!("{value:<4}"))
        .collect::<String>()
        .trim_end()
        .to_string()
}

fn spw_display_name(name: &str) -> String {
    if name.is_empty() {
        "none".to_string()
    } else {
        name.to_string()
    }
}

fn truncate(value: &str, width: usize) -> String {
    if value.chars().count() <= width {
        return value.to_string();
    }
    if width <= 3 {
        return ".".repeat(width);
    }
    let mut truncated = value.chars().take(width - 3).collect::<String>();
    truncated.push_str("...");
    truncated
}

fn format_columns(cells: &[String], widths: &[usize]) -> String {
    let mut line = String::new();
    for (index, cell) in cells.iter().enumerate() {
        if index > 0 {
            line.push(' ');
        }
        line.push_str(cell);
        let padding = widths[index].saturating_sub(cell.chars().count());
        if padding > 0 {
            line.push_str(&" ".repeat(padding));
        }
    }
    line
}

fn scrollbar_thumb_length(
    track_length: usize,
    content_length: usize,
    viewport_length: usize,
) -> usize {
    if track_length == 0 || content_length == 0 {
        return 1;
    }
    let thumb = (track_length * viewport_length).div_ceil(content_length);
    thumb.clamp(1, track_length)
}

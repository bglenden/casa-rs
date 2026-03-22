// SPDX-License-Identifier: LGPL-3.0-or-later
use std::ffi::OsString;
use std::time::{Duration, Instant};

use casacore_ms::ListObsSummary;
use casacore_ms::listobs::cli::{
    UiArgumentParser, UiArgumentSchema, UiCommandSchema, UiManagedOutputSchema,
};
use casacore_tablebrowser_protocol::{
    BrowserCommand, BrowserComplex32Value, BrowserComplex64Value, BrowserInspectorSnapshot,
    BrowserScalarValue, BrowserSnapshot, BrowserValueNode, BrowserView as TableBrowserView,
    BrowserViewport,
};
use casacore_types::quanta::{MvAngle, MvTime};
use crossterm::event::{
    KeyCode, KeyEvent, KeyEventKind, KeyModifiers, MouseButton, MouseEvent, MouseEventKind,
};
use ratatui::layout::Rect;

use crate::browser_client::BrowserClient;
use crate::clipboard;
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

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) enum BrowserTab {
    Overview,
    Columns,
    Keywords,
    Cells,
    Subtables,
}

impl BrowserTab {
    pub(crate) const ALL: [Self; 5] = [
        Self::Overview,
        Self::Columns,
        Self::Keywords,
        Self::Cells,
        Self::Subtables,
    ];

    pub(crate) fn label(self) -> &'static str {
        match self {
            Self::Overview => "Overview",
            Self::Columns => "Columns",
            Self::Keywords => "Keywords",
            Self::Cells => "Cells",
            Self::Subtables => "Subtables",
        }
    }

    fn from_view(view: TableBrowserView) -> Self {
        match view {
            TableBrowserView::Overview => Self::Overview,
            TableBrowserView::Columns => Self::Columns,
            TableBrowserView::Keywords => Self::Keywords,
            TableBrowserView::Cells => Self::Cells,
            TableBrowserView::Subtables => Self::Subtables,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum OutputPane {
    Result,
    LeftOutput,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum OutputSelectionMode {
    Pending,
    Dragging,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct BufferPoint {
    row: usize,
    col: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct OutputSelection {
    target: OutputPane,
    anchor: BufferPoint,
    cursor: BufferPoint,
    mode: OutputSelectionMode,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum VisibleTextRole {
    Plain,
    TableHeader,
    BrowserSeparator,
    BrowserSelectedCell,
}

#[derive(Debug, Clone)]
pub(crate) struct VisibleTextLine {
    pub text: String,
    pub roles: Vec<VisibleTextRole>,
}

#[derive(Debug, Clone)]
pub(crate) struct VisibleTextBuffer {
    pub area: Rect,
    pub lines: Vec<VisibleTextLine>,
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
    browser_session: Option<BrowserSession>,
    spinner_frame: usize,
    dragging_divider: bool,
    dragging_result_scrollbar: bool,
    dragging_result_hscrollbar: bool,
    dragging_result_hscrollbar_grab: u16,
    output_selection: Option<OutputSelection>,
    cached_result_text_area: Option<Rect>,
    cached_left_output_area: Option<Rect>,
    last_click: Option<ClickState>,
    quit: bool,
    return_to_launcher: bool,
}

#[derive(Debug)]
struct RunningState {
    process: RunningProcess,
    renderer: Option<String>,
    file_output_path: Option<String>,
    cancel_requested: bool,
}

#[derive(Debug)]
struct BrowserSession {
    client: BrowserClient,
    snapshot: BrowserSnapshot,
    viewport: BrowserViewport,
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
        let ready_status_line = app.ready_status_line().to_string();
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
                status_line: ready_status_line,
                status_kind: StatusKind::Info,
                ..ResultState::default()
            },
            active_result_tab: ResultTab::Overview,
            result_scrolls: [0; 9],
            result_hscrolls: [0; 9],
            running: None,
            browser_session: None,
            spinner_frame: 0,
            dragging_divider: false,
            dragging_result_scrollbar: false,
            dragging_result_hscrollbar: false,
            dragging_result_hscrollbar_grab: 0,
            output_selection: None,
            cached_result_text_area: None,
            cached_left_output_area: None,
            last_click: None,
            quit: false,
            return_to_launcher: false,
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
            browser_session: None,
            spinner_frame: 0,
            dragging_divider: false,
            dragging_result_scrollbar: false,
            dragging_result_hscrollbar: false,
            dragging_result_hscrollbar_grab: 0,
            output_selection: None,
            cached_result_text_area: None,
            cached_left_output_area: None,
            last_click: None,
            quit: false,
            return_to_launcher: false,
        }
    }

    pub(crate) fn should_quit(&self) -> bool {
        self.quit
    }

    pub(crate) fn should_return_to_launcher(&self) -> bool {
        self.return_to_launcher
    }

    fn has_active_session(&self) -> bool {
        self.running.is_some() || self.browser_session.is_some()
    }

    fn browser_session(&self) -> Option<&BrowserSession> {
        self.browser_session.as_ref()
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
                if self.has_active_session() {
                    self.cancel_current();
                }
                self.quit = true;
                return;
            }
            KeyCode::Char('b') if key_event.modifiers.is_empty() && self.edit_state.is_none() => {
                if self.has_active_session() {
                    self.cancel_current();
                }
                self.return_to_launcher = true;
                return;
            }
            KeyCode::Char('t') if key_event.modifiers.is_empty() => {
                self.toggle_theme();
                return;
            }
            KeyCode::Char('p') if key_event.modifiers.is_empty() && self.edit_state.is_none() => {
                self.toggle_parameters_pane();
                return;
            }
            KeyCode::Char('y') if key_event.modifiers.is_empty() && self.edit_state.is_none() => {
                self.copy_output_selection();
                return;
            }
            KeyCode::Char('c')
                if is_browser_copy_modifier(key_event.modifiers) && self.edit_state.is_none() =>
            {
                self.copy_output_selection();
                return;
            }
            KeyCode::Tab | KeyCode::BackTab if self.browser_session.is_some() => {
                self.handle_browser_key(key_event);
                return;
            }
            KeyCode::Tab | KeyCode::BackTab => {
                self.toggle_focus();
                return;
            }
            KeyCode::Char('r') if key_event.modifiers.is_empty() && self.edit_state.is_none() => {
                if !self.has_active_session() {
                    self.start_run();
                }
                return;
            }
            KeyCode::Char('a') if key_event.modifiers.is_empty() && !self.has_active_session() => {
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

        if key_event.code == KeyCode::Esc && self.output_selection.is_some() {
            self.clear_output_selection();
            return;
        }

        if self.has_active_session() {
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
        self.cache_output_layout(layout);
        match mouse_event.kind {
            MouseEventKind::Down(MouseButton::Left) => {
                self.handle_left_mouse_down(mouse_event, layout)
            }
            MouseEventKind::Up(MouseButton::Left) => {
                self.finalize_output_selection();
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
            "Tab pane  Enter save  Esc cancel  Bksp delete  p pane  t theme  q quit"
        } else if self.browser_session.is_some() {
            "Tab view  Arrows  PgUp/PgDn  Enter  y copy  Esc clear  Bksp up  p pane  b apps  x close  t theme  q quit"
        } else if self.running.is_some() {
            "Tab pane  h/l tabs  j/k scroll  [/] hscr  y copy  p pane  b apps  x cancel  t theme  q quit"
        } else {
            "Tab pane  h/l tabs  Arrows/jk  [/] hscr  y copy  a adv  p pane  b apps  r run  t theme  q quit"
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
        } else if self.browser_session.is_some() {
            match self.theme_mode() {
                ThemeMode::DenseAnsi => format!("Inspector [live]{focus}"),
                ThemeMode::RichPanel => format!("◈ Inspector [live]{focus}"),
            }
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

    pub(crate) fn parameters_pane_collapsed(&self) -> bool {
        self.pane_split_ratio() <= 0.0
    }

    pub(crate) fn pane_focus(&self) -> PaneFocus {
        self.pane_focus
    }

    pub(crate) fn form_rows(&self) -> Vec<FormRowView> {
        if self.browser_session.is_some() {
            return self.browser_inspector_rows();
        }

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
        } else if let Some(session) = self.browser_session() {
            lines.push(format!(
                "View: {}  Table: {}  Breadcrumb: {}",
                session.snapshot.view.label(),
                session.snapshot.table_path,
                session
                    .snapshot
                    .breadcrumb
                    .iter()
                    .map(|entry| entry.label.as_str())
                    .collect::<Vec<_>>()
                    .join(" / ")
            ));
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

    pub(crate) fn browser_is_active(&self) -> bool {
        self.browser_session.is_some()
    }

    pub(crate) fn browser_tabs(&self) -> &'static [BrowserTab] {
        &BrowserTab::ALL
    }

    pub(crate) fn active_browser_tab_label(&self) -> Option<&'static str> {
        self.active_browser_tab().map(BrowserTab::label)
    }

    pub(crate) fn active_browser_scroll_metrics(
        &self,
        _viewport_height: u16,
    ) -> Option<(usize, usize)> {
        let session = self.browser_session()?;
        let lines = self.browser_main_content_lines()?;
        match session.snapshot.view {
            TableBrowserView::Overview => None,
            TableBrowserView::Columns => {
                let (_, total) = browser_fraction(&lines, "selected=")?;
                Some((total, browser_columns_viewport(&lines).max(1)))
            }
            TableBrowserView::Keywords => {
                let (_, total) = browser_fraction(&lines, "selected=")?;
                Some((total, lines.len().saturating_sub(1).max(1)))
            }
            TableBrowserView::Cells => {
                let (_, total) = browser_fraction(&lines, "row=")?;
                Some((total, lines.len().saturating_sub(2).max(1)))
            }
            TableBrowserView::Subtables => {
                let (_, total) = browser_fraction(&lines, "selected=")?;
                Some((total, lines.len().saturating_sub(1).max(1)))
            }
        }
    }

    pub(crate) fn active_browser_hscroll_metrics(
        &self,
        _viewport_width: u16,
    ) -> Option<(usize, usize)> {
        let session = self.browser_session()?;
        let lines = self.browser_main_content_lines()?;
        match session.snapshot.view {
            TableBrowserView::Cells => {
                let (_, total) = browser_fraction(&lines, "col=")?;
                Some((total, browser_visible_cell_columns(&lines).max(1)))
            }
            _ => None,
        }
    }

    pub(crate) fn browser_inspector_lines(&self) -> Option<Vec<String>> {
        let session = self.browser_session()?;
        let inspector = session.snapshot.inspector.as_ref()?;
        Some(browser_inspector_lines(inspector))
    }

    fn browser_inspector_rows(&self) -> Vec<FormRowView> {
        let lines = self.browser_inspector_lines().unwrap_or_else(|| {
            vec![
                "Inspector".to_string(),
                String::new(),
                "No value selected.".to_string(),
            ]
        });

        lines
            .into_iter()
            .enumerate()
            .map(|(index, text)| FormRowView {
                target: FormSelection::Section(index),
                text,
                kind: FormRowKind::Field,
                selected: false,
            })
            .collect()
    }

    fn browser_main_content_lines(&self) -> Option<Vec<String>> {
        let session = self.browser_session()?;
        Some(browser_main_content_lines(&session.snapshot))
    }

    pub(crate) fn sync_browser_viewport(&mut self, width: u16, height: u16) {
        let Some(current_viewport) = self
            .browser_session
            .as_ref()
            .map(|session| session.viewport)
        else {
            return;
        };
        let viewport = BrowserViewport::new(width, height);
        if viewport == current_viewport {
            return;
        }
        self.clear_output_selection();
        let Some(session) = self.browser_session.as_mut() else {
            return;
        };
        match session.client.request(BrowserCommand::Resize { viewport }) {
            Ok(snapshot) => {
                session.viewport = viewport;
                session.snapshot = snapshot.clone();
                self.result.status_line = snapshot.status_line;
                self.result.status_kind = StatusKind::Info;
            }
            Err(error) => {
                let stderr = session.client.stderr_text();
                let details = if stderr.trim().is_empty() {
                    format!("{error}\n")
                } else {
                    format!("{error}\n{stderr}")
                };
                if let Some(session) = self.browser_session.take() {
                    let _ = session.client.cancel();
                }
                self.report_browser_error(
                    "Browser session resize failed. Session closed.",
                    details,
                );
            }
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
        self.apply_startup_text_value(id, value.to_string())
            .expect("set text value in test");
    }

    #[cfg(test)]
    pub(crate) fn set_toggle_value(&mut self, id: &str, value: bool) {
        self.apply_startup_toggle_value(id, value)
            .expect("set toggle value in test");
    }

    pub(crate) fn apply_startup_text_value(
        &mut self,
        id: &str,
        value: String,
    ) -> Result<(), String> {
        let field = self
            .fields
            .iter_mut()
            .find(|field| field.schema.id == id)
            .ok_or_else(|| format!("unknown startup field {id:?} for {}", self.app.id))?;
        field.apply_text_value(value)
    }

    pub(crate) fn apply_startup_toggle_value(
        &mut self,
        id: &str,
        value: bool,
    ) -> Result<(), String> {
        let field = self
            .fields
            .iter_mut()
            .find(|field| field.schema.id == id)
            .ok_or_else(|| format!("unknown startup field {id:?} for {}", self.app.id))?;
        field.apply_toggle_value(value)
    }

    pub(crate) fn start_run_on_launch(&mut self) {
        self.start_run();
    }

    #[cfg(test)]
    pub(crate) fn set_active_result_tab(&mut self, tab: ResultTab) {
        self.clear_output_selection();
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
    pub(crate) fn should_return_to_launcher_for_test(&self) -> bool {
        self.return_to_launcher
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

    pub(crate) fn cache_output_layout(&mut self, layout: &UiLayout) {
        let next_result = result_text_area(layout);
        let next_left = left_output_area(self, layout);
        if self.cached_result_text_area != Some(next_result)
            && self
                .output_selection
                .is_some_and(|selection| selection.target == OutputPane::Result)
        {
            self.clear_output_selection();
        }
        if self.cached_left_output_area != next_left
            && self
                .output_selection
                .is_some_and(|selection| selection.target == OutputPane::LeftOutput)
        {
            self.clear_output_selection();
        }
        self.cached_result_text_area = Some(next_result);
        self.cached_left_output_area = next_left;
    }

    pub(crate) fn visible_text_buffer(
        &self,
        target: OutputPane,
        layout: &UiLayout,
    ) -> Option<VisibleTextBuffer> {
        let area = match target {
            OutputPane::Result => result_text_area(layout),
            OutputPane::LeftOutput => left_output_area(self, layout)?,
        };
        self.visible_text_buffer_for_area(target, area)
    }

    pub(crate) fn output_selection_rect(
        &self,
        target: OutputPane,
    ) -> Option<(usize, usize, usize, usize)> {
        let selection = self.output_selection?;
        if selection.target != target || selection.mode != OutputSelectionMode::Dragging {
            return None;
        }
        Some(normalize_selection(selection))
    }

    fn visible_text_buffer_for_area(
        &self,
        target: OutputPane,
        area: Rect,
    ) -> Option<VisibleTextBuffer> {
        if area.width == 0 || area.height == 0 {
            return None;
        }
        match target {
            OutputPane::Result => {
                if self.browser_is_active() {
                    let lines = self.browser_main_content_lines()?;
                    let browser_cells =
                        self.active_browser_tab_label() == Some(BrowserTab::Cells.label());
                    Some(VisibleTextBuffer {
                        area,
                        lines: lines
                            .into_iter()
                            .take(area.height as usize)
                            .map(|line| {
                                if browser_cells {
                                    browser_cells_visible_line(&line)
                                } else {
                                    VisibleTextLine::plain(line)
                                }
                            })
                            .collect(),
                    })
                } else {
                    let content = self.active_result_content();
                    match content {
                        ResultContent::Lines(lines) => Some(VisibleTextBuffer {
                            area,
                            lines: lines
                                .iter()
                                .skip(self.active_result_scroll() as usize)
                                .take(area.height as usize)
                                .map(|line| {
                                    VisibleTextLine::plain(slice_visible_text(
                                        line,
                                        self.active_result_hscroll() as usize,
                                        area.width as usize,
                                    ))
                                })
                                .collect(),
                        }),
                        ResultContent::Table(table) => {
                            let mut lines = Vec::new();
                            lines.push(VisibleTextLine::table_header(slice_visible_text(
                                &table.header,
                                self.active_result_hscroll() as usize,
                                area.width as usize,
                            )));
                            let body_height = area.height.saturating_sub(1) as usize;
                            lines.extend(
                                table
                                    .rows
                                    .iter()
                                    .skip(self.active_result_scroll() as usize)
                                    .take(body_height)
                                    .map(|row| {
                                        VisibleTextLine::plain(slice_visible_text(
                                            row,
                                            self.active_result_hscroll() as usize,
                                            area.width as usize,
                                        ))
                                    }),
                            );
                            Some(VisibleTextBuffer { area, lines })
                        }
                    }
                }
            }
            OutputPane::LeftOutput => Some(VisibleTextBuffer {
                area,
                lines: self
                    .browser_inspector_lines()?
                    .into_iter()
                    .take(area.height as usize)
                    .map(VisibleTextLine::plain)
                    .collect(),
            }),
        }
    }

    fn clear_output_selection(&mut self) {
        self.output_selection = None;
    }

    fn clear_output_selection_for_target(&mut self, target: OutputPane) {
        if self
            .output_selection
            .is_some_and(|selection| selection.target == target)
        {
            self.output_selection = None;
        }
    }

    fn active_selected_text(&self) -> Option<String> {
        let selection = self.output_selection?;
        if selection.mode != OutputSelectionMode::Dragging {
            return None;
        }
        let area = match selection.target {
            OutputPane::Result => self.cached_result_text_area?,
            OutputPane::LeftOutput => self.cached_left_output_area?,
        };
        let buffer = self.visible_text_buffer_for_area(selection.target, area)?;
        let text = extract_selected_text(&buffer, selection);
        if text.is_empty() { None } else { Some(text) }
    }

    fn selection_point_at(
        &self,
        column: u16,
        row: u16,
        layout: &UiLayout,
    ) -> Option<(OutputPane, BufferPoint)> {
        if let Some(buffer) = self.visible_text_buffer(OutputPane::Result, layout)
            && rect_contains(buffer.area, column, row)
        {
            return Some((
                OutputPane::Result,
                clamp_point_to_buffer(&buffer, column, row),
            ));
        }
        if let Some(buffer) = self.visible_text_buffer(OutputPane::LeftOutput, layout)
            && rect_contains(buffer.area, column, row)
        {
            return Some((
                OutputPane::LeftOutput,
                clamp_point_to_buffer(&buffer, column, row),
            ));
        }
        None
    }

    fn clamped_selection_point(
        &self,
        target: OutputPane,
        column: u16,
        row: u16,
    ) -> Option<BufferPoint> {
        let area = match target {
            OutputPane::Result => self.cached_result_text_area?,
            OutputPane::LeftOutput => self.cached_left_output_area?,
        };
        let buffer = self.visible_text_buffer_for_area(target, area)?;
        Some(clamp_point_to_buffer(&buffer, column, row))
    }

    fn begin_output_selection(&mut self, target: OutputPane, point: BufferPoint) {
        self.output_selection = Some(OutputSelection {
            target,
            anchor: point,
            cursor: point,
            mode: OutputSelectionMode::Pending,
        });
    }

    fn update_output_selection(&mut self, column: u16, row: u16) -> bool {
        let Some(selection) = self.output_selection else {
            return false;
        };
        let Some(point) = self.clamped_selection_point(selection.target, column, row) else {
            return false;
        };
        let mode = if point == selection.anchor {
            selection.mode
        } else {
            OutputSelectionMode::Dragging
        };
        self.output_selection = Some(OutputSelection {
            cursor: point,
            mode,
            ..selection
        });
        true
    }

    fn finalize_output_selection(&mut self) {
        let Some(selection) = self.output_selection else {
            return;
        };
        if selection.mode == OutputSelectionMode::Pending {
            self.output_selection = None;
            return;
        }
        if let Some(text) = self.active_selected_text() {
            self.copy_text_to_clipboard(text, "selection");
        }
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
        if self.browser_session.is_some() {
            self.handle_browser_key(key_event);
            return;
        }

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

    fn handle_browser_key(&mut self, key_event: KeyEvent) {
        match key_event.code {
            KeyCode::Tab if key_event.modifiers.is_empty() => {
                self.send_browser_command(BrowserCommand::CycleView {
                    forward: true,
                    viewport: None,
                });
            }
            KeyCode::BackTab => {
                self.send_browser_command(BrowserCommand::CycleView {
                    forward: false,
                    viewport: None,
                });
            }
            KeyCode::Left | KeyCode::Char('h')
                if key_event.modifiers.is_empty() || key_event.modifiers == KeyModifiers::SHIFT =>
            {
                self.send_browser_command(BrowserCommand::MoveLeft {
                    steps: 1,
                    viewport: None,
                });
            }
            KeyCode::Right | KeyCode::Char('l')
                if key_event.modifiers.is_empty() || key_event.modifiers == KeyModifiers::SHIFT =>
            {
                self.send_browser_command(BrowserCommand::MoveRight {
                    steps: 1,
                    viewport: None,
                });
            }
            KeyCode::Up | KeyCode::Char('k')
                if key_event.modifiers.is_empty() || key_event.modifiers == KeyModifiers::SHIFT =>
            {
                self.send_browser_command(BrowserCommand::MoveUp {
                    steps: 1,
                    viewport: None,
                });
            }
            KeyCode::Down | KeyCode::Char('j')
                if key_event.modifiers.is_empty() || key_event.modifiers == KeyModifiers::SHIFT =>
            {
                self.send_browser_command(BrowserCommand::MoveDown {
                    steps: 1,
                    viewport: None,
                });
            }
            KeyCode::PageUp => self.send_browser_command(BrowserCommand::PageUp {
                pages: 1,
                viewport: None,
            }),
            KeyCode::PageDown => self.send_browser_command(BrowserCommand::PageDown {
                pages: 1,
                viewport: None,
            }),
            KeyCode::Enter if key_event.modifiers.is_empty() => {
                self.send_browser_command(BrowserCommand::Activate { viewport: None })
            }
            KeyCode::Backspace => {
                self.send_browser_command(BrowserCommand::Back { viewport: None })
            }
            KeyCode::Esc => self.send_browser_command(BrowserCommand::Escape { viewport: None }),
            _ => {}
        }
    }

    fn copy_output_selection(&mut self) {
        let payload = self
            .active_selected_text()
            .map(|text| (text, "selection"))
            .or_else(|| {
                if self.browser_session.is_some() {
                    self.browser_clipboard_payload()
                } else {
                    None
                }
            });
        let Some((text, label)) = payload else {
            self.result.status_line = "Nothing copyable is selected.".to_string();
            self.result.status_kind = StatusKind::Warning;
            return;
        };
        self.copy_text_to_clipboard(text, label);
    }

    fn copy_text_to_clipboard(&mut self, text: String, label: &str) {
        match clipboard::copy_text(&text) {
            Ok(()) => {
                self.result.status_line = format!("Copied {label} to clipboard.");
                self.result.status_kind = StatusKind::Ok;
            }
            Err(error) => {
                self.result.status_line = "Failed to copy to clipboard.".to_string();
                self.result.status_kind = StatusKind::Error;
                self.result.stderr = format!("{error}\n");
            }
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
            self.clear_output_selection();
            self.dragging_divider = true;
            self.dragging_result_scrollbar = false;
            self.last_click = Some(ClickState {
                target: ClickTarget::Divider,
                at: Instant::now(),
            });
            return;
        }

        if layout.in_result_scrollbar(mouse_event.column, mouse_event.row) {
            self.clear_output_selection_for_target(OutputPane::Result);
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
            self.clear_output_selection_for_target(OutputPane::Result);
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
            self.clear_output_selection_for_target(OutputPane::Result);
            self.pane_focus = PaneFocus::Result;
            self.active_result_tab = tab;
            self.last_click = Some(ClickState {
                target: ClickTarget::Tab(tab),
                at: Instant::now(),
            });
            return;
        }

        if let Some((target, point)) =
            self.selection_point_at(mouse_event.column, mouse_event.row, layout)
        {
            self.pane_focus = if target == OutputPane::Result {
                PaneFocus::Result
            } else {
                PaneFocus::Parameters
            };
            self.begin_output_selection(target, point);
            self.last_click = Some(ClickState {
                target: ClickTarget::Pane(self.pane_focus),
                at: Instant::now(),
            });
            return;
        }

        if layout.in_result_block(mouse_event.column, mouse_event.row) {
            self.pane_focus = PaneFocus::Result;
            self.clear_output_selection_for_target(OutputPane::Result);
            self.last_click = Some(ClickState {
                target: ClickTarget::Pane(PaneFocus::Result),
                at: Instant::now(),
            });
            return;
        }

        if self.has_active_session() {
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
            self.clear_output_selection();
            let relative = mouse_event
                .column
                .saturating_sub(layout.body.x)
                .min(layout.body.width.saturating_sub(1));
            let ratio = if relative <= 1 {
                0.0
            } else {
                f32::from(relative) / f32::from(layout.body.width.max(1))
            };
            self.config_store.set_pane_split_ratio(ratio);
            return;
        }
        if self.dragging_result_scrollbar {
            self.clear_output_selection_for_target(OutputPane::Result);
            self.scroll_result_to_mouse(mouse_event.row, layout);
            return;
        }
        if self.dragging_result_hscrollbar {
            self.clear_output_selection_for_target(OutputPane::Result);
            self.scroll_result_horizontally_to_mouse(mouse_event.column, layout);
            return;
        }
        if self.update_output_selection(mouse_event.column, mouse_event.row) {
            self.last_click = None;
        }
    }

    fn handle_mouse_scroll(&mut self, mouse_event: MouseEvent, layout: &UiLayout, delta: i16) {
        if layout.in_result_block(mouse_event.column, mouse_event.row) {
            self.clear_output_selection_for_target(OutputPane::Result);
            self.pane_focus = PaneFocus::Result;
            if self.browser_session.is_some() {
                self.scroll_active_browser(delta);
            } else {
                self.scroll_active_result(delta);
            }
            return;
        }
        if layout.in_form_block(mouse_event.column, mouse_event.row) && !self.has_active_session() {
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
            self.clear_output_selection_for_target(OutputPane::Result);
            self.pane_focus = PaneFocus::Result;
            if self.browser_session.is_some() {
                self.scroll_active_browser_horizontal(delta);
            } else {
                self.scroll_active_result_horizontal(delta);
            }
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
                if self.has_active_session() || self.sections.is_empty() {
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

    fn active_browser_tab(&self) -> Option<BrowserTab> {
        self.browser_session()
            .map(|session| BrowserTab::from_view(session.snapshot.view))
    }

    fn toggle_parameters_pane(&mut self) {
        self.clear_output_selection();
        let next = if self.parameters_pane_collapsed() {
            self.config_store.pane_restore_ratio()
        } else {
            0.0
        };
        self.config_store.set_pane_split_ratio(next);
        if next == 0.0 {
            self.pane_focus = PaneFocus::Result;
        }
    }

    pub(crate) fn active_browser_scroll(&self) -> u16 {
        let Some(session) = self.browser_session() else {
            return 0;
        };
        let lines = self
            .browser_main_content_lines()
            .unwrap_or_else(|| session.snapshot.content_lines.clone());
        let selected = match session.snapshot.view {
            TableBrowserView::Overview => 0,
            TableBrowserView::Columns
            | TableBrowserView::Keywords
            | TableBrowserView::Subtables => browser_fraction(&lines, "selected=")
                .map(|(selected, _)| selected.saturating_sub(1))
                .unwrap_or(0),
            TableBrowserView::Cells => browser_fraction(&lines, "row=")
                .map(|(selected, _)| selected.saturating_sub(1))
                .unwrap_or(0),
        };
        selected.min(u16::MAX as usize) as u16
    }

    pub(crate) fn active_browser_hscroll(&self) -> u16 {
        let Some(session) = self.browser_session() else {
            return 0;
        };
        let lines = self
            .browser_main_content_lines()
            .unwrap_or_else(|| session.snapshot.content_lines.clone());
        let selected = match session.snapshot.view {
            TableBrowserView::Cells => browser_fraction(&lines, "col=")
                .map(|(selected, _)| selected.saturating_sub(1))
                .unwrap_or(0),
            _ => 0,
        };
        selected.min(u16::MAX as usize) as u16
    }

    fn scroll_active_browser(&mut self, delta: i16) {
        self.clear_output_selection_for_target(OutputPane::Result);
        if delta.is_negative() {
            self.send_browser_command(BrowserCommand::MoveUp {
                steps: delta.unsigned_abs() as usize,
                viewport: None,
            });
        } else {
            self.send_browser_command(BrowserCommand::MoveDown {
                steps: delta as usize,
                viewport: None,
            });
        }
    }

    fn set_active_browser_scroll(&mut self, scroll: usize) {
        let current = self.active_browser_scroll() as usize;
        if scroll > current {
            self.scroll_active_browser((scroll - current).min(i16::MAX as usize) as i16);
        } else if current > scroll {
            self.scroll_active_browser(-((current - scroll).min(i16::MAX as usize) as i16));
        }
    }

    fn scroll_active_browser_horizontal(&mut self, delta: i16) {
        self.clear_output_selection_for_target(OutputPane::Result);
        if delta.is_negative() {
            self.send_browser_command(BrowserCommand::MoveLeft {
                steps: delta.unsigned_abs() as usize,
                viewport: None,
            });
        } else {
            self.send_browser_command(BrowserCommand::MoveRight {
                steps: delta as usize,
                viewport: None,
            });
        }
    }

    fn set_active_browser_hscroll(&mut self, scroll: usize) {
        let current = self.active_browser_hscroll() as usize;
        if scroll > current {
            self.scroll_active_browser_horizontal((scroll - current).min(i16::MAX as usize) as i16);
        } else if current > scroll {
            self.scroll_active_browser_horizontal(
                -((current - scroll).min(i16::MAX as usize) as i16),
            );
        }
    }

    fn scroll_active_result(&mut self, delta: i16) {
        self.clear_output_selection_for_target(OutputPane::Result);
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
        self.clear_output_selection_for_target(OutputPane::Result);
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
        let metrics = if self.browser_session.is_some() {
            self.active_browser_scroll_metrics(track.height)
        } else {
            self.active_result_scroll_metrics(track.height)
        };
        let Some((content_length, viewport_length)) = metrics else {
            return;
        };
        if content_length <= viewport_length || track.height == 0 {
            if self.browser_session.is_some() {
                self.set_active_browser_scroll(0);
            } else {
                self.set_active_result_scroll(0);
            }
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
        if self.browser_session.is_some() {
            self.set_active_browser_scroll(scroll);
        } else {
            self.set_active_result_scroll(scroll);
        }
    }

    fn scroll_result_horizontally_to_mouse(&mut self, column: u16, layout: &UiLayout) {
        let Some(track) = layout.result_hscrollbar else {
            return;
        };
        let metrics = if self.browser_session.is_some() {
            self.active_browser_hscroll_metrics(track.width)
        } else {
            self.active_result_hscroll_metrics(track.width)
        };
        let Some((content_width, viewport_width)) = metrics else {
            return;
        };
        if content_width <= viewport_width || track.width == 0 {
            if self.browser_session.is_some() {
                self.set_active_browser_hscroll(0);
            } else {
                self.set_active_result_hscroll(0);
            }
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
        if self.browser_session.is_some() {
            self.set_active_browser_hscroll(scroll);
        } else {
            self.set_active_result_hscroll(scroll);
        }
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
        let metrics = if self.browser_session.is_some() {
            self.active_browser_hscroll_metrics(track.width)
        } else {
            self.active_result_hscroll_metrics(track.width)
        };
        let Some((content_width, viewport_width)) = metrics else {
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

        let active_hscroll = if self.browser_session.is_some() {
            self.active_browser_hscroll()
        } else {
            self.active_result_hscroll()
        };
        let thumb_offset =
            (active_hscroll as usize * max_thumb_offset + max_scroll / 2) / max_scroll;
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
        self.clear_output_selection_for_target(OutputPane::Result);
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
        self.clear_output_selection();
        self.commit_edit_buffer();

        if self.schema.is_none() {
            self.result.status_line = "Cannot run without a loaded UI schema.".to_string();
            self.result.status_kind = StatusKind::Error;
            self.active_result_tab = ResultTab::Stderr;
            return;
        }

        if self.app.is_browser_session() {
            self.start_table_browser();
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

    fn start_table_browser(&mut self) {
        self.clear_output_selection();
        let Some(path) = self
            .field_text("table_path")
            .filter(|value| !value.trim().is_empty())
        else {
            self.result.status_line = "Table Path is required.".to_string();
            self.result.status_kind = StatusKind::Error;
            return;
        };

        let viewport = BrowserViewport::new(120, 24);
        match self
            .app
            .resolve_command()
            .and_then(|command| BrowserClient::spawn(&command))
        {
            Ok(client) => match client.request_startup(BrowserCommand::OpenRoot { path, viewport })
            {
                Ok(snapshot) => {
                    self.result = ResultState {
                        status_line: snapshot.status_line.clone(),
                        status_kind: StatusKind::Info,
                        ..ResultState::default()
                    };
                    self.edit_state = None;
                    self.pane_focus = PaneFocus::Result;
                    self.browser_session = Some(BrowserSession {
                        client,
                        snapshot,
                        viewport,
                    });
                }
                Err(error) => {
                    let _ = client.cancel();
                    self.report_browser_error(
                        "Failed to open table browser.",
                        format!("{error}\n"),
                    );
                }
            },
            Err(error) => {
                self.report_browser_error("Failed to launch table browser.", format!("{error}\n"));
            }
        }
    }

    fn send_browser_command(&mut self, command: BrowserCommand) {
        let result = {
            let Some(session) = self.browser_session.as_mut() else {
                return;
            };
            match session.client.request(command) {
                Ok(snapshot) => {
                    session.snapshot = snapshot.clone();
                    Ok(snapshot)
                }
                Err(error) => Err((error, session.client.stderr_text())),
            }
        };

        match result {
            Ok(snapshot) => {
                self.clear_output_selection();
                self.result.status_line = snapshot.status_line;
                self.result.status_kind = StatusKind::Info;
            }
            Err((error, stderr)) => {
                if let Some(session) = self.browser_session.take() {
                    let _ = session.client.cancel();
                }
                let details = if stderr.trim().is_empty() {
                    format!("{error}\n")
                } else {
                    format!("{error}\n{stderr}")
                };
                self.report_browser_error("Browser command failed. Session closed.", details);
            }
        }
    }

    fn report_browser_error(&mut self, status_line: &str, stderr: String) {
        self.result.status_line = status_line.to_string();
        self.result.status_kind = StatusKind::Error;
        self.result.stderr = stderr;
        self.active_result_tab = ResultTab::Stderr;
        self.pane_focus = PaneFocus::Result;
    }

    fn browser_clipboard_payload(&self) -> Option<(String, &'static str)> {
        let session = self.browser_session()?;
        let inspector = session.snapshot.inspector.as_ref()?;
        Some(copyable_browser_text(inspector))
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
            self.clear_output_selection_for_target(OutputPane::Result);
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
        if let Some(session) = self.browser_session.take() {
            let _ = session.client.cancel();
            self.result.status_line = "Browser session closed.".to_string();
            self.result.status_kind = StatusKind::Info;
            self.pane_focus = PaneFocus::Parameters;
            return;
        }

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

        if let Some(session) = self.browser_session() {
            return session.snapshot.content_lines.clone();
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
        let _ = self.apply_text_value(value);
    }

    fn apply_text_value(&mut self, value: String) -> Result<(), String> {
        match &mut self.value {
            FormValue::Text(current) => {
                *current = value;
                Ok(())
            }
            FormValue::Choice {
                value: current,
                choices,
            } => {
                if !choices.is_empty() && !choices.iter().any(|choice| choice == &value) {
                    return Err(format!(
                        "{} expects one of: {}",
                        self.schema.label,
                        choices.join(", ")
                    ));
                }
                *current = value;
                Ok(())
            }
            FormValue::Toggle(_) => {
                Err(format!("{} does not accept text input", self.schema.label))
            }
        }
    }

    fn apply_toggle_value(&mut self, value: bool) -> Result<(), String> {
        match &mut self.value {
            FormValue::Toggle(current) => {
                *current = value;
                Ok(())
            }
            _ => Err(format!("{} is not a toggle", self.schema.label)),
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

impl VisibleTextLine {
    fn plain(text: String) -> Self {
        let roles = text.chars().map(|_| VisibleTextRole::Plain).collect();
        Self { text, roles }
    }

    fn table_header(text: String) -> Self {
        let roles = text.chars().map(|_| VisibleTextRole::TableHeader).collect();
        Self { text, roles }
    }
}

fn rect_contains(rect: Rect, column: u16, row: u16) -> bool {
    column >= rect.x
        && column < rect.x.saturating_add(rect.width)
        && row >= rect.y
        && row < rect.y.saturating_add(rect.height)
}

fn result_text_area(layout: &UiLayout) -> Rect {
    Rect {
        x: layout.result_content.x,
        y: layout.result_content.y,
        width: layout
            .result_content
            .width
            .saturating_sub(if layout.result_scrollbar.is_some() {
                1
            } else {
                0
            }),
        height: layout.result_content.height.saturating_sub(
            if layout.result_hscrollbar.is_some() {
                1
            } else {
                0
            },
        ),
    }
}

fn left_output_area(app: &AppState, layout: &UiLayout) -> Option<Rect> {
    if app.browser_is_active() && layout.form_inner.width > 0 && layout.form_inner.height > 0 {
        Some(layout.form_inner)
    } else {
        None
    }
}

fn normalize_selection(selection: OutputSelection) -> (usize, usize, usize, usize) {
    (
        selection.anchor.row.min(selection.cursor.row),
        selection.anchor.row.max(selection.cursor.row),
        selection.anchor.col.min(selection.cursor.col),
        selection.anchor.col.max(selection.cursor.col),
    )
}

fn clamp_point_to_buffer(buffer: &VisibleTextBuffer, column: u16, row: u16) -> BufferPoint {
    let relative_row = row.saturating_sub(buffer.area.y) as usize;
    let row = relative_row.min(buffer.lines.len().saturating_sub(1));
    let line_len = buffer
        .lines
        .get(row)
        .map(|line| line.text.chars().count())
        .unwrap_or(0);
    let relative_col = column.saturating_sub(buffer.area.x) as usize;
    let col = if line_len == 0 {
        0
    } else {
        relative_col.min(line_len.saturating_sub(1))
    };
    BufferPoint { row, col }
}

fn extract_selected_text(buffer: &VisibleTextBuffer, selection: OutputSelection) -> String {
    let (row_start, row_end, col_start, col_end) = normalize_selection(selection);
    (row_start..=row_end)
        .filter_map(|row| buffer.lines.get(row))
        .map(|line| {
            slice_chars(&line.text, col_start, col_end.saturating_add(1))
                .trim_end_matches(' ')
                .to_string()
        })
        .collect::<Vec<_>>()
        .join("\n")
}

fn browser_cells_visible_line(raw_line: &str) -> VisibleTextLine {
    if !raw_line.contains('|') {
        return VisibleTextLine::plain(raw_line.to_string());
    }

    let is_header = raw_line.trim_start().starts_with("row ");
    let mut text = String::new();
    let mut roles = Vec::new();
    for (index, segment) in raw_line.split('|').enumerate() {
        if index > 0 {
            text.push('│');
            roles.push(VisibleTextRole::BrowserSeparator);
        }
        let (visible, role) = if let Some(selected) = strip_browser_selection_markers(segment) {
            (selected, VisibleTextRole::BrowserSelectedCell)
        } else if is_header {
            (segment.to_string(), VisibleTextRole::TableHeader)
        } else {
            (segment.to_string(), VisibleTextRole::Plain)
        };
        roles.extend(visible.chars().map(|_| role));
        text.push_str(&visible);
    }
    VisibleTextLine { text, roles }
}

fn strip_browser_selection_markers(segment: &str) -> Option<String> {
    let mut chars = segment.chars().collect::<Vec<_>>();
    let first = chars
        .iter()
        .position(|character| !character.is_whitespace())?;
    let last = chars
        .iter()
        .rposition(|character| !character.is_whitespace())?;
    if chars[first] != '>' || chars[last] != '<' {
        return None;
    }
    chars[first] = ' ';
    chars[last] = ' ';
    Some(chars.into_iter().collect())
}

fn slice_visible_text(text: &str, offset: usize, width: usize) -> String {
    if width == 0 {
        return String::new();
    }
    let sliced = text.chars().skip(offset).collect::<String>();
    fit_visible_text(&sliced, width)
}

fn fit_visible_text(text: &str, width: usize) -> String {
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

fn slice_chars(text: &str, start: usize, end: usize) -> String {
    text.chars()
        .skip(start)
        .take(end.saturating_sub(start))
        .collect()
}

fn is_browser_copy_modifier(modifiers: KeyModifiers) -> bool {
    modifiers == KeyModifiers::SUPER || modifiers == (KeyModifiers::CONTROL | KeyModifiers::SHIFT)
}

fn copyable_browser_text(inspector: &BrowserInspectorSnapshot) -> (String, &'static str) {
    match &inspector.node {
        BrowserValueNode::Undefined => ("<undefined>".to_string(), "undefined value"),
        BrowserValueNode::Scalar { value } => (render_browser_scalar(value), "value"),
        BrowserValueNode::TableRef { resolved_path, .. } => (resolved_path.clone(), "table path"),
        BrowserValueNode::Array {
            shape,
            total_elements,
            elements,
            ..
        } if elements.len() == *total_elements => (
            format!(
                "[{}]",
                elements
                    .iter()
                    .map(|element| render_browser_scalar(&element.value))
                    .collect::<Vec<_>>()
                    .join(", ")
            ),
            if shape.len() > 1 {
                "array value"
            } else {
                "value"
            },
        ),
        _ => (
            inspector.rendered_lines.join("\n").trim().to_string(),
            "inspector view",
        ),
    }
}

fn render_browser_scalar(value: &BrowserScalarValue) -> String {
    match value {
        BrowserScalarValue::Bool(value) => value.to_string(),
        BrowserScalarValue::UInt8(value) => value.to_string(),
        BrowserScalarValue::UInt16(value) => value.to_string(),
        BrowserScalarValue::UInt32(value) => value.to_string(),
        BrowserScalarValue::Int16(value) => value.to_string(),
        BrowserScalarValue::Int32(value) => value.to_string(),
        BrowserScalarValue::Int64(value) => value.to_string(),
        BrowserScalarValue::Float32(value) => format_browser_float(f64::from(*value)),
        BrowserScalarValue::Float64(value) => format_browser_float(*value),
        BrowserScalarValue::Complex32(value) => render_complex32(value),
        BrowserScalarValue::Complex64(value) => render_complex64(value),
        BrowserScalarValue::String(value) => format!("{value:?}"),
    }
}

fn render_complex32(value: &BrowserComplex32Value) -> String {
    format!(
        "{}{:+}i",
        format_browser_float(f64::from(value.re)),
        f64::from(value.im)
    )
}

fn render_complex64(value: &BrowserComplex64Value) -> String {
    format!("{}{:+}i", format_browser_float(value.re), value.im)
}

fn format_browser_float(value: f64) -> String {
    if value.is_finite() && value.fract() == 0.0 {
        format!("{value:.1}")
    } else {
        let rendered = format!("{value:.15}");
        rendered
            .trim_end_matches('0')
            .trim_end_matches('.')
            .to_string()
    }
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

fn browser_fraction(lines: &[String], prefix: &str) -> Option<(usize, usize)> {
    let line = lines.first()?;
    let token = line
        .split_whitespace()
        .find(|token| token.starts_with(prefix))?
        .strip_prefix(prefix)?;
    let (selected, total) = token.split_once('/')?;
    Some((selected.parse().ok()?, total.parse().ok()?))
}

fn browser_columns_viewport(lines: &[String]) -> usize {
    lines
        .iter()
        .skip(1)
        .take_while(|line| !line.is_empty() && line.as_str() != "-- Column Details --")
        .count()
}

fn browser_visible_cell_columns(lines: &[String]) -> usize {
    let Some(header) = lines.get(1) else {
        return 0;
    };
    header.split('|').count().saturating_sub(2)
}

fn browser_main_content_lines(snapshot: &BrowserSnapshot) -> Vec<String> {
    let mut lines = snapshot.content_lines.clone();
    let Some(inspector) = snapshot.inspector.as_ref() else {
        return lines;
    };
    let inspector_lines = &inspector.rendered_lines;
    if inspector_lines.is_empty() || lines.len() < inspector_lines.len() {
        return lines;
    }
    if lines.ends_with(inspector_lines) {
        let new_len = lines.len().saturating_sub(inspector_lines.len());
        lines.truncate(new_len);
        if lines.last().is_some_and(String::is_empty) {
            lines.pop();
        }
    }
    lines
}

fn browser_inspector_lines(inspector: &BrowserInspectorSnapshot) -> Vec<String> {
    let mut lines = vec![inspector.title.clone()];
    if !inspector.trail.is_empty() {
        lines.push(format!(
            "Path: {}",
            inspector
                .trail
                .iter()
                .map(|entry| entry.label.as_str())
                .collect::<Vec<_>>()
                .join(" / ")
        ));
    }
    if !lines.is_empty() {
        lines.push(String::new());
    }
    lines.extend(inspector.rendered_lines.iter().cloned());
    lines
}

// SPDX-License-Identifier: LGPL-3.0-or-later
use std::fs;
use std::fs::File;
use std::path::{Path, PathBuf};
use std::sync::MutexGuard;
use std::time::{Duration, Instant};

use casacore_imagebrowser_protocol::{
    ImageBrowserAxisValue, ImageBrowserCapabilities, ImageBrowserFocus as ProtocolImageFocus,
    ImageBrowserParameters, ImageBrowserProbe, ImageBrowserResponse, ImageBrowserResponseEnvelope,
    ImageBrowserSnapshot, ImageBrowserView as ProtocolImageView, ImageDisplayAxisState,
    ImageNavigationMetrics, ImageNonDisplayAxisState, ImagePlaneCursorState, ImagePlaneRaster,
    ImageProfilePayload, ImageProfileSampleState, ImageRegionOverlayShapeState,
    ImageRegionOverlayVertex, ImageRegionState, ImageRegionStatsState,
};
use casacore_ms::column_def::{ColumnDef, ColumnKind};
use casacore_ms::listobs::cli::command_schema as listobs_command_schema;
use casacore_ms::msexplore::cli::command_schema as msexplore_command_schema;
use casacore_ms::schema;
use casacore_ms::{
    ListObsPlotKind, MeasurementSet, MeasurementSetBuilder, OptionalMainColumn, SubtableId,
};
use casacore_tablebrowser_protocol::{
    BrowserBreadcrumbEntry, BrowserCapabilities, BrowserFocus, BrowserInspectorSnapshot,
    BrowserInspectorTrailEntry, BrowserNavigationMetrics, BrowserResponseEnvelope,
    BrowserScalarValue, BrowserSnapshot, BrowserValueNode, BrowserView as ProtocolBrowserView,
    BrowserViewport,
};
use casacore_tables::{ColumnSchema, Table, TableOptions, TableSchema};
use casacore_types::{
    ArrayD, ArrayValue, Complex32, PrimitiveType, RecordField, RecordValue, ScalarValue, Value,
    quanta::MvTime,
};
use crossterm::event::{KeyCode, KeyEvent, KeyModifiers, MouseButton, MouseEvent, MouseEventKind};
use flate2::read::GzDecoder;
use ratatui::Terminal;
use ratatui::backend::TestBackend;
use ratatui_graphics::{ProtocolType, TerminalCapabilities};
use tar::Archive;
use tempfile::tempdir;

use crate::app::{
    AppState, BrowserPaneFocus, ImageBrowserLeftPaneMode, OutputPane, PaneFocus, PlotCatalogTarget,
    PlotControlTarget, PlotPaneFocus, ResultTab, image_plane_draw_rect,
};
use crate::config::{ConfigStore, ThemeMode};
use crate::is_suspend_key;
use crate::registry::{
    imexplore_app, listobs_app, msexplore_app, registered_apps, tablebrowser_app,
};
use crate::theme::theme;
use crate::ui;
use crate::{KittyMovieOverlayMode, kitty_movie_overlay_mode, test_env_lock};

#[test]
fn launcher_lists_registered_apps_in_expected_order() {
    let apps = registered_apps();
    let ids = apps.iter().map(|app| app.id).collect::<Vec<_>>();
    assert_eq!(ids, vec!["msexplore", "tablebrowser", "imexplore"]);
}

#[test]
fn launcher_screen_renders_available_apps() {
    let apps = registered_apps();
    let backend = TestBackend::new(100, 24);
    let mut terminal = Terminal::new(backend).expect("test terminal");
    terminal
        .draw(|frame| ui::draw_launcher(frame, &apps, 1))
        .expect("draw launcher");
    let buffer = terminal.backend().buffer().clone();
    let mut rendered = String::new();
    for y in 0..buffer.area.height {
        for x in 0..buffer.area.width {
            rendered.push_str(buffer[(x, y)].symbol());
        }
        rendered.push('\n');
    }

    assert!(rendered.contains("Select Application"));
    assert!(rendered.contains("msexplore"));
    assert!(rendered.contains("tablebrowser"));
    assert!(rendered.contains("imexplore"));
    assert!(rendered.contains("MSExplore"));
    assert!(rendered.contains("Table Browser"));
    assert!(rendered.contains("ImExplore"));
}

#[test]
fn back_to_launcher_requests_launcher_from_idle_app() {
    let (_temp, mut app) = test_app();
    app.handle_key_event(KeyEvent::new(KeyCode::Char('b'), KeyModifiers::NONE));
    assert!(app.should_return_to_launcher_for_test());
}

#[test]
fn ctrl_z_is_reserved_for_terminal_suspend() {
    assert!(is_suspend_key(KeyEvent::new(
        KeyCode::Char('z'),
        KeyModifiers::CONTROL,
    )));
    assert!(!is_suspend_key(KeyEvent::new(
        KeyCode::Char('z'),
        KeyModifiers::NONE,
    )));
    assert!(!is_suspend_key(KeyEvent::new(
        KeyCode::Char('x'),
        KeyModifiers::CONTROL,
    )));
}

#[test]
fn ctrl_c_requests_quit() {
    let (_temp, mut app) = test_app();
    app.handle_key_event(KeyEvent::new(KeyCode::Char('c'), KeyModifiers::CONTROL));
    assert!(app.should_quit());
}

#[test]
fn kitty_overlay_defaults_to_software_direct_without_animation_opt_in() {
    let _guard = test_env_lock();
    unsafe {
        std::env::remove_var("CASARS_IMEXPLORE_ENABLE_KITTY_ANIMATION_OVERLAY");
        std::env::remove_var("CASARS_IMEXPLORE_DISABLE_DIRECT_OVERLAY");
    }

    let capabilities = TerminalCapabilities {
        panel_protocol: ProtocolType::Kitty,
        direct_kitty_layers: true,
        direct_kitty_animations: true,
    };

    assert_eq!(
        kitty_movie_overlay_mode(&capabilities),
        KittyMovieOverlayMode::SoftwareDirect
    );
}

#[test]
fn kitty_overlay_env_flags_select_disabled_or_animation_modes() {
    let _guard = test_env_lock();
    let capabilities = TerminalCapabilities {
        panel_protocol: ProtocolType::Kitty,
        direct_kitty_layers: true,
        direct_kitty_animations: true,
    };

    unsafe {
        std::env::remove_var("CASARS_IMEXPLORE_ENABLE_KITTY_ANIMATION_OVERLAY");
        std::env::set_var("CASARS_IMEXPLORE_DISABLE_DIRECT_OVERLAY", "1");
    }
    assert_eq!(
        kitty_movie_overlay_mode(&capabilities),
        KittyMovieOverlayMode::Disabled
    );

    unsafe {
        std::env::set_var("CASARS_IMEXPLORE_ENABLE_KITTY_ANIMATION_OVERLAY", "1");
    }
    assert_eq!(
        kitty_movie_overlay_mode(&capabilities),
        KittyMovieOverlayMode::KittyAnimation
    );

    unsafe {
        std::env::remove_var("CASARS_IMEXPLORE_ENABLE_KITTY_ANIMATION_OVERLAY");
        std::env::remove_var("CASARS_IMEXPLORE_DISABLE_DIRECT_OVERLAY");
    }
}

#[test]
fn renders_idle_layout_with_ready_status() {
    let (_temp, app) = test_app();
    let rendered = render_app(&app, 100, 30);
    assert!(rendered.contains("casars"));
    assert!(rendered.contains("MeasurementSet / MSExplore"));
    assert!(rendered.contains("MeasurementSet Path"));
    assert!(rendered.contains("Ready. Press r to run the selected command."));
    assert!(rendered.contains("Overview"));
}

#[test]
fn default_section_visibility_and_toggle_work() {
    let (_temp, mut app) = test_app();
    assert_eq!(app.section_collapsed_for_test("Input"), Some(false));
    assert_eq!(app.section_collapsed_for_test("Selection"), Some(false));
    assert_eq!(app.section_collapsed_for_test("Output"), Some(true));

    app.handle_key_event(KeyEvent::new(KeyCode::Char('a'), KeyModifiers::NONE));
    let rendered = render_app(&app, 100, 30);
    assert!(rendered.contains("[+] Output"));
}

#[test]
fn theme_mode_persists_across_app_instances() {
    let temp = tempdir().expect("tempdir");
    let config_path = temp.path().join("casars.toml");

    let schema = msexplore_command_schema("msexplore");
    let mut first = AppState::from_schema_with_config(
        msexplore_app(),
        schema.clone(),
        ConfigStore::load_for_tests(config_path.clone()),
    );
    assert_eq!(first.theme_mode_for_test(), ThemeMode::DenseAnsi);
    first.handle_key_event(KeyEvent::new(KeyCode::Char('t'), KeyModifiers::NONE));
    assert_eq!(first.theme_mode_for_test(), ThemeMode::RichPanel);

    let second = AppState::from_schema_with_config(
        msexplore_app(),
        schema,
        ConfigStore::load_for_tests(config_path),
    );
    assert_eq!(second.theme_mode_for_test(), ThemeMode::RichPanel);
}

#[test]
fn pane_split_ratio_persists_after_drag() {
    let temp = tempdir().expect("tempdir");
    let config_path = temp.path().join("casars.toml");
    let mut app = AppState::from_schema_with_config(
        msexplore_app(),
        msexplore_command_schema("msexplore"),
        ConfigStore::load_for_tests(config_path.clone()),
    );
    let layout = ui::compute_layout(ratatui::layout::Rect::new(0, 0, 120, 30), &app);

    app.handle_mouse_event(
        mouse(
            MouseEventKind::Down(MouseButton::Left),
            layout.divider.x,
            layout.divider.y + 2,
        ),
        &layout,
    );
    app.handle_mouse_event(
        mouse(
            MouseEventKind::Drag(MouseButton::Left),
            layout.body.x + 72,
            layout.divider.y + 2,
        ),
        &layout,
    );
    app.handle_mouse_event(
        mouse(
            MouseEventKind::Up(MouseButton::Left),
            layout.body.x + 72,
            layout.divider.y + 2,
        ),
        &layout,
    );
    assert!(app.pane_split_ratio_for_test() > 0.55);

    let reloaded = AppState::from_schema_with_config(
        msexplore_app(),
        msexplore_command_schema("msexplore"),
        ConfigStore::load_for_tests(config_path),
    );
    assert!(reloaded.pane_split_ratio_for_test() > 0.55);
}

#[test]
fn divider_drag_starts_from_adjacent_border_column() {
    let temp = tempdir().expect("tempdir");
    let config_path = temp.path().join("casars.toml");
    let mut app = AppState::from_schema_with_config(
        msexplore_app(),
        msexplore_command_schema("msexplore"),
        ConfigStore::load_for_tests(config_path),
    );
    let layout = ui::compute_layout(ratatui::layout::Rect::new(0, 0, 120, 30), &app);
    let border_column = layout.form_block.x + layout.form_block.width.saturating_sub(1);

    app.handle_mouse_event(
        mouse(
            MouseEventKind::Down(MouseButton::Left),
            border_column,
            layout.divider.y + 3,
        ),
        &layout,
    );
    app.handle_mouse_event(
        mouse(
            MouseEventKind::Drag(MouseButton::Left),
            layout.body.x + 72,
            layout.divider.y + 3,
        ),
        &layout,
    );
    app.handle_mouse_event(
        mouse(
            MouseEventKind::Up(MouseButton::Left),
            layout.body.x + 72,
            layout.divider.y + 3,
        ),
        &layout,
    );

    assert!(app.pane_split_ratio_for_test() > 0.55);
}

#[test]
fn pane_toggle_can_collapse_parameters_pane() {
    let (_temp, mut app) = test_app();
    app.handle_key_event(KeyEvent::new(KeyCode::Char('p'), KeyModifiers::NONE));
    let layout = ui::compute_layout(ratatui::layout::Rect::new(0, 0, 120, 30), &app);
    assert!(app.parameters_pane_collapsed());
    assert_eq!(layout.form_block.width, 0);
    assert_eq!(layout.divider.width, 1);
    assert_eq!(
        layout.result_block.width + layout.divider.width,
        layout.body.width
    );
}

#[test]
fn pane_toggle_restores_previous_noncollapsed_size() {
    let (_temp, mut app) = test_app();
    let original = app.pane_split_ratio_for_test();

    let layout = ui::compute_layout(ratatui::layout::Rect::new(0, 0, 120, 30), &app);
    app.handle_mouse_event(
        mouse(
            MouseEventKind::Down(MouseButton::Left),
            layout.divider.x,
            layout.divider.y + 2,
        ),
        &layout,
    );
    app.handle_mouse_event(
        mouse(
            MouseEventKind::Drag(MouseButton::Left),
            layout.body.x + 72,
            layout.divider.y + 2,
        ),
        &layout,
    );
    app.handle_mouse_event(
        mouse(
            MouseEventKind::Up(MouseButton::Left),
            layout.body.x + 72,
            layout.divider.y + 2,
        ),
        &layout,
    );

    let resized = app.pane_split_ratio_for_test();
    assert!(resized > original);

    app.handle_key_event(KeyEvent::new(KeyCode::Char('p'), KeyModifiers::NONE));
    assert!(app.parameters_pane_collapsed());
    app.handle_key_event(KeyEvent::new(KeyCode::Char('p'), KeyModifiers::NONE));

    assert!((app.pane_split_ratio_for_test() - resized).abs() < f32::EPSILON);
}

#[test]
fn renders_toggled_boolean_fields() {
    let (_temp, mut app) = test_app();
    app.set_toggle_value("listunfl", true);
    let rendered = render_app(&app, 100, 30);
    assert!(rendered.contains("List Unflagged Rows"));
    assert!(rendered.contains("[x]"));
}

#[test]
fn pastes_text_into_selected_field_without_edit_mode() {
    let (_temp, mut app) = test_app();
    app.handle_paste("/tmp/example.ms\n".to_string());

    let rendered = render_app(&app, 140, 30);
    assert!(rendered.contains("example.ms"));
}

#[test]
fn ctrl_o_opens_path_chooser_for_path_field_and_escape_cancels() {
    let temp = tempdir().expect("tempdir");
    let path = temp.path().join("cancel.ms");
    std::fs::write(&path, "").expect("write fake ms");

    let (_temp, mut app) = test_app();
    app.set_text_value("ms_path", path.to_string_lossy().as_ref());
    app.handle_key_event(KeyEvent::new(KeyCode::Char('o'), KeyModifiers::CONTROL));
    assert!(app.path_chooser_active());

    app.handle_key_event(KeyEvent::new(KeyCode::Esc, KeyModifiers::NONE));
    assert!(!app.path_chooser_active());
    assert_eq!(
        app.field_text_for_test("ms_path").as_deref(),
        Some(path.to_string_lossy().as_ref())
    );
}

#[test]
fn path_chooser_enter_confirms_selected_path() {
    let temp = tempdir().expect("tempdir");
    let path = temp.path().join("selected.ms");
    std::fs::write(&path, "").expect("write fake ms");

    let (_temp, mut app) = test_app();
    app.set_text_value("ms_path", path.to_string_lossy().as_ref());
    app.handle_key_event(KeyEvent::new(KeyCode::Char('o'), KeyModifiers::CONTROL));
    assert!(app.path_chooser_active());

    app.handle_key_event(KeyEvent::new(KeyCode::Enter, KeyModifiers::NONE));
    assert!(!app.path_chooser_active());
    let expected = path.canonicalize().expect("canonical path");
    assert_eq!(
        app.field_text_for_test("ms_path").as_deref(),
        Some(expected.to_string_lossy().as_ref())
    );
}

#[test]
fn clicking_path_browse_affordance_opens_chooser() {
    let (_temp, mut app) = test_app();
    let layout = ui::compute_layout(ratatui::layout::Rect::new(0, 0, 120, 30), &app);
    let row_hit = layout
        .form_rows
        .iter()
        .find(|row| matches!(row.target, crate::app::FormSelection::Field(_)))
        .expect("path field row");
    let row_text = app
        .form_rows()
        .into_iter()
        .find(|row| row.target == row_hit.target)
        .expect("row text")
        .text;
    let x = row_hit
        .rect
        .x
        .saturating_add(row_text.chars().count() as u16)
        .saturating_sub(3);

    app.handle_mouse_event(
        mouse(MouseEventKind::Down(MouseButton::Left), x, row_hit.rect.y),
        &layout,
    );

    assert!(app.path_chooser_active());
}

#[test]
fn typing_directory_then_opening_chooser_confirms_selected_file() {
    let temp = tempdir().expect("tempdir");
    let ms_path = temp.path().join("selected.ms");
    std::fs::write(&ms_path, "").expect("write fake ms");

    let (_temp, mut app) = test_app();
    app.handle_key_event(KeyEvent::new(KeyCode::Enter, KeyModifiers::NONE));
    while app
        .edit_buffer_for_test()
        .is_some_and(|buffer| !buffer.is_empty())
    {
        app.handle_key_event(KeyEvent::new(KeyCode::Backspace, KeyModifiers::NONE));
    }
    for character in temp.path().display().to_string().chars() {
        app.handle_key_event(KeyEvent::new(KeyCode::Char(character), KeyModifiers::NONE));
    }
    app.handle_key_event(KeyEvent::new(KeyCode::Enter, KeyModifiers::NONE));
    assert_eq!(
        app.field_text_for_test("ms_path").as_deref(),
        Some(temp.path().to_string_lossy().as_ref())
    );
    app.handle_key_event(KeyEvent::new(KeyCode::Char('o'), KeyModifiers::CONTROL));

    assert!(app.path_chooser_active());
    assert_eq!(
        app.path_chooser_cwd().as_deref(),
        Some(temp.path().to_string_lossy().as_ref())
    );
    let selected = app
        .path_chooser_entries()
        .expect("chooser entries")
        .into_iter()
        .find(|(_, selected)| *selected)
        .expect("selected entry");
    assert!(selected.0.contains("selected.ms"));

    app.handle_key_event(KeyEvent::new(KeyCode::Enter, KeyModifiers::NONE));
    assert!(!app.path_chooser_active());
    assert!(app.edit_buffer_for_test().is_none());
    let expected = ms_path.canonicalize().expect("canonical path");
    assert_eq!(
        app.field_text_for_test("ms_path").as_deref(),
        Some(expected.to_string_lossy().as_ref())
    );
}

#[test]
fn path_chooser_enter_selects_directory_path() {
    let temp = tempdir().expect("tempdir");
    let ms_path = temp.path().join("selected.ms");
    std::fs::create_dir(&ms_path).expect("create fake ms directory");

    let (_temp, mut app) = test_app();
    app.set_text_value("ms_path", temp.path().to_string_lossy().as_ref());
    app.handle_key_event(KeyEvent::new(KeyCode::Char('o'), KeyModifiers::CONTROL));
    assert!(app.path_chooser_active());

    let selected = app
        .path_chooser_entries()
        .expect("chooser entries")
        .into_iter()
        .find(|(_, selected)| *selected)
        .expect("selected entry");
    assert!(selected.0.contains("selected.ms/"));

    app.handle_key_event(KeyEvent::new(KeyCode::Enter, KeyModifiers::NONE));
    assert!(!app.path_chooser_active());
    let expected = ms_path.canonicalize().expect("canonical path");
    assert_eq!(
        app.field_text_for_test("ms_path").as_deref(),
        Some(expected.to_string_lossy().as_ref())
    );
}

#[test]
fn path_chooser_right_descends_into_selected_directory() {
    let temp = tempdir().expect("tempdir");
    let ms_path = temp.path().join("selected.ms");
    std::fs::create_dir(&ms_path).expect("create fake ms directory");

    let (_temp, mut app) = test_app();
    app.set_text_value("ms_path", temp.path().to_string_lossy().as_ref());
    app.handle_key_event(KeyEvent::new(KeyCode::Char('o'), KeyModifiers::CONTROL));
    assert!(app.path_chooser_active());

    app.handle_key_event(KeyEvent::new(KeyCode::Right, KeyModifiers::NONE));
    assert!(app.path_chooser_active());
    assert_eq!(
        app.path_chooser_cwd().as_deref(),
        Some(ms_path.to_string_lossy().as_ref())
    );
}

#[test]
fn double_click_enters_text_edit_mode() {
    let (_temp, mut app) = test_app();
    let layout = ui::compute_layout(ratatui::layout::Rect::new(0, 0, 100, 30), &app);
    let target = layout
        .form_rows
        .iter()
        .find(|row| matches!(row.target, crate::app::FormSelection::Field(_)))
        .expect("field row");
    let x = target.rect.x + 1;
    let y = target.rect.y;

    app.handle_mouse_event(
        mouse(MouseEventKind::Down(MouseButton::Left), x, y),
        &layout,
    );
    app.handle_mouse_event(
        mouse(MouseEventKind::Down(MouseButton::Left), x, y),
        &layout,
    );
    app.handle_paste("/tmp/double-click.ms".to_string());
    assert_eq!(app.edit_buffer_for_test(), Some("/tmp/double-click.ms"));
}

#[test]
fn clicking_result_pane_changes_focus_and_tab_click_selects_tab() {
    let (_temp, mut app) = test_app();
    let layout = ui::compute_layout(ratatui::layout::Rect::new(0, 0, 120, 30), &app);
    let fields_tab = layout
        .tab_hits
        .iter()
        .find(|hit| hit.tab == ResultTab::Fields)
        .expect("fields tab");

    app.handle_mouse_event(
        mouse(
            MouseEventKind::Down(MouseButton::Left),
            fields_tab.rect.x,
            fields_tab.rect.y,
        ),
        &layout,
    );

    assert_eq!(app.pane_focus_for_test(), PaneFocus::Result);
    assert_eq!(app.active_result_tab(), ResultTab::Fields);
}

#[test]
fn wheel_scroll_changes_active_result_offset() {
    let (_temp, mut app) = test_app();
    app.set_result_for_test(
        &(0..50)
            .map(|index| format!("line {index}\n"))
            .collect::<String>(),
        "",
    );
    app.set_active_result_tab(ResultTab::Stdout);
    let layout = ui::compute_layout(ratatui::layout::Rect::new(0, 0, 120, 30), &app);

    app.handle_mouse_event(
        mouse(
            MouseEventKind::ScrollDown,
            layout.result_content.x + 1,
            layout.result_content.y + 1,
        ),
        &layout,
    );

    assert!(app.active_result_scroll() > 0);
}

#[test]
fn dragging_result_scrollbar_changes_active_result_offset() {
    let (_temp, mut app) = test_app();
    app.set_result_for_test(
        &(0..80)
            .map(|index| format!("line {index}\n"))
            .collect::<String>(),
        "",
    );
    app.set_active_result_tab(ResultTab::Stdout);
    let layout = ui::compute_layout(ratatui::layout::Rect::new(0, 0, 120, 30), &app);
    let scrollbar = layout.result_scrollbar.expect("result scrollbar");

    app.handle_mouse_event(
        mouse(
            MouseEventKind::Down(MouseButton::Left),
            scrollbar.x,
            scrollbar.y + scrollbar.height.saturating_sub(1),
        ),
        &layout,
    );

    assert!(app.active_result_scroll() > 0);
}

#[test]
fn wheel_scroll_down_in_form_moves_selection_downward() {
    let (_temp, mut wheel_app) = test_app();
    let (_temp, mut key_app) = test_app();
    let layout = ui::compute_layout(ratatui::layout::Rect::new(0, 0, 120, 30), &wheel_app);

    wheel_app.handle_mouse_event(
        mouse(
            MouseEventKind::ScrollDown,
            layout.form_inner.x + 1,
            layout.form_inner.y + 1,
        ),
        &layout,
    );
    for _ in 0..3 {
        key_app.handle_key_event(KeyEvent::new(KeyCode::Down, KeyModifiers::NONE));
    }

    let wheel_after = wheel_app
        .selected_form_text_for_test()
        .expect("selected form text after wheel scroll");
    let key_after = key_app
        .selected_form_text_for_test()
        .expect("selected form text after key scroll");
    assert_eq!(wheel_after, key_after);
}

#[test]
fn rich_panel_theme_uses_rounded_chrome() {
    let (_temp, mut app) = test_app();
    app.handle_key_event(KeyEvent::new(KeyCode::Char('t'), KeyModifiers::NONE));
    let rendered = render_app(&app, 100, 30);
    assert_eq!(app.theme_mode_for_test(), ThemeMode::RichPanel);
    assert!(rendered.contains("◈ Parameters"));
    assert!(rendered.contains("◖ Overview ◗"));
}

#[test]
fn rich_panel_keeps_content_clear_of_the_frame() {
    let (_temp, mut app) = test_app();
    app.handle_key_event(KeyEvent::new(KeyCode::Char('t'), KeyModifiers::NONE));
    let rendered = render_app(&app, 100, 30);
    assert!(rendered.contains("▾ Input"));
    assert!(!rendered.contains("▾ ▾ Input"));
}

#[test]
fn rich_panel_footer_keeps_theme_toggle_visible() {
    let (_temp, mut app) = test_app();
    app.handle_key_event(KeyEvent::new(KeyCode::Char('t'), KeyModifiers::NONE));
    let rendered = render_app(&app, 140, 30);
    assert!(rendered.contains("t theme"));
}

#[test]
fn tablebrowser_session_opens_cells_and_linked_subtables() {
    let _guard = launcher_env_lock();
    clear_tablebrowser_launcher_bin();

    let temp = tempdir().expect("tempdir");
    let table_path = create_fixture_table(temp.path());
    let schema = tablebrowser_app()
        .load_schema()
        .expect("tablebrowser schema");
    let config = ConfigStore::load_for_tests(temp.path().join("casars.toml"));
    let mut app = AppState::from_schema_with_config(tablebrowser_app(), schema, config);
    app.set_text_value("table_path", table_path.to_string_lossy().as_ref());
    app.start_run_for_test();

    assert!(app.browser_is_active());
    app.sync_browser_viewport(90, 25, 10);
    let overview = render_app(&app, 180, 30);
    assert!(overview.contains("Tables / Table Browser"));
    assert!(overview.contains("Columns"));

    app.handle_key_event(KeyEvent::new(KeyCode::Char(']'), KeyModifiers::NONE));
    app.handle_key_event(KeyEvent::new(KeyCode::Char(']'), KeyModifiers::NONE));
    app.handle_key_event(KeyEvent::new(KeyCode::Char(']'), KeyModifiers::NONE));
    let cells = render_app(&app, 180, 30);
    assert!(cells.contains("Cells"));
    assert!(cells.contains("\"alpha\""));

    app.handle_key_event(KeyEvent::new(KeyCode::Char(']'), KeyModifiers::NONE));
    let subtables = render_app(&app, 180, 30);
    assert!(subtables.contains("child.tab"));

    app.handle_key_event(KeyEvent::new(KeyCode::Enter, KeyModifiers::NONE));
    let child = render_app(&app, 180, 30);
    assert!(child.contains("child.tab"));

    app.handle_key_event(KeyEvent::new(KeyCode::Backspace, KeyModifiers::NONE));
    let parent = render_app(&app, 180, 30);
    assert!(parent.contains("parent.tab / child.tab") || parent.contains("parent.tab"));
}

#[test]
fn browser_footer_describes_escape_and_backspace_semantics() {
    let _guard = launcher_env_lock();
    clear_tablebrowser_launcher_bin();

    let temp = tempdir().expect("tempdir");
    let table_path = create_fixture_table(temp.path());
    let schema = tablebrowser_app()
        .load_schema()
        .expect("tablebrowser schema");
    let config = ConfigStore::load_for_tests(temp.path().join("casars.toml"));
    let mut app = AppState::from_schema_with_config(tablebrowser_app(), schema, config);
    app.set_text_value("table_path", table_path.to_string_lossy().as_ref());
    app.start_run_for_test();

    assert!(app.browser_is_active());
    assert!(app.footer_text().contains("Esc back"));
    assert!(app.footer_text().contains("Bksp parent table"));
}

#[test]
fn help_overlay_toggles_with_question_mark_and_escape() {
    let (_temp, mut app) = test_app();
    assert!(!app.help_visible());

    app.handle_key_event(KeyEvent::new(KeyCode::Char('?'), KeyModifiers::NONE));
    assert!(app.help_visible());
    let rendered = render_app(&app, 140, 30);
    assert!(rendered.contains("Key Help"));
    assert!(rendered.contains("Tab/Shift-Tab focus"));

    app.handle_key_event(KeyEvent::new(KeyCode::Esc, KeyModifiers::NONE));
    assert!(!app.help_visible());
}

#[cfg(unix)]
#[test]
fn imexplore_help_overlay_lists_plane_controls() {
    let _guard = launcher_env_lock();
    let temp = tempdir().expect("tempdir");
    let script = write_fake_imexplore_script(
        temp.path(),
        &[fake_imexplore_snapshot_json_with_profile(
            ProtocolImageView::Plane,
            ProtocolImageFocus::Content,
            "Image ready",
            vec!["raster".to_string()],
            vec![
                "View: Plane".to_string(),
                "Hidden axis Frequency (2): 0/2".to_string(),
            ],
            Some(ImageBrowserProbe {
                pixel_indices: vec![0, 0, 0],
                pixel_axes: vec![],
                value: 1.0,
                masked: false,
                finite: true,
                world_axes: vec![],
            }),
            Some(fake_image_profile_payload()),
            Some(ImageNonDisplayAxisState {
                axis: 2,
                label: "Frequency".to_string(),
                index: 0,
                length: 3,
                pixel: 0,
            }),
            image_parameters("0,0,0", "3,3,2", "1,1,1"),
        )],
        None,
    );
    set_imexplore_launcher_bin(&script);

    let schema = imexplore_app()
        .load_schema()
        .expect("load fake imexplore schema");
    let config = ConfigStore::load_for_tests(temp.path().join("casars.toml"));
    let mut app = AppState::from_schema_with_config(imexplore_app(), schema, config);
    app.set_text_value("image_path", "/tmp/fake.image");
    app.start_run_for_test();

    app.handle_key_event(KeyEvent::new(KeyCode::Char('?'), KeyModifiers::NONE));
    let rendered = render_app(&app, 140, 34);
    assert!(rendered.contains("g toggle raster/spreadsheet"));
    assert!(rendered.contains("+/- zoom"));
    assert!(rendered.contains("0 reset view"));
    assert!(rendered.contains("H/J/K/L pan view"));
    assert!(rendered.contains("c cycle colormap"));
    assert!(rendered.contains("i invert"));
    assert!(rendered.contains("Display params:"));
    assert!(rendered.contains("stretch/autoscale"));
    assert!(rendered.contains("clip_low/clip_high"));
    assert!(rendered.contains("R start/add polygon"));
}

#[cfg(unix)]
#[test]
fn imexplore_left_pane_switches_between_live_regions_and_masks() {
    let _guard = launcher_env_lock();
    let temp = tempdir().expect("tempdir");
    let snapshot = fake_imexplore_snapshot_json_with_saved_items(
        fake_imexplore_snapshot_json(
            ProtocolImageView::Plane,
            ProtocolImageFocus::Content,
            "Image ready",
            vec!["raster".to_string()],
            vec!["View: Plane".to_string()],
            Some(ImageBrowserProbe {
                pixel_indices: vec![0, 0, 0],
                pixel_axes: vec![],
                value: 1.0,
                masked: false,
                finite: true,
                world_axes: vec![],
            }),
            Some(ImageNonDisplayAxisState {
                axis: 2,
                label: "Frequency".to_string(),
                index: 0,
                length: 3,
                pixel: 0,
            }),
        ),
        &["Region 1", "Region 2"],
        Some("Region 1"),
        &["mask0", "mask1"],
        Some("mask0"),
    );
    let script = write_fake_imexplore_script(temp.path(), &[snapshot], None);
    set_imexplore_launcher_bin(&script);

    let schema = imexplore_app()
        .load_schema()
        .expect("load fake imexplore schema");
    let config = ConfigStore::load_for_tests(temp.path().join("casars.toml"));
    let mut app = AppState::from_schema_with_config(imexplore_app(), schema, config);
    app.set_text_value("image_path", "/tmp/fake.image");
    app.start_run_for_test();

    let rendered = render_app(&app, 140, 34);
    assert!(rendered.contains("View [ Live ▼ ]"));
    assert!(!rendered.contains("[x] Region 1"));

    let layout = ui::compute_layout(ratatui::layout::Rect::new(0, 0, 140, 34), &app);
    let mode_row = layout.browser_mode_selector.expect("mode selector");
    app.handle_mouse_event(
        mouse(
            MouseEventKind::Down(MouseButton::Left),
            mode_row.x + 1,
            mode_row.y,
        ),
        &layout,
    );
    assert!(app.browser_mode_picker_active());
    let picker_area = ui::browser_mode_picker_area(layout.browser_mode_selector, layout.form_block);
    let picker_list_area = ui::browser_mode_picker_list_area(picker_area);
    app.handle_mouse_event(
        mouse(
            MouseEventKind::Down(MouseButton::Left),
            picker_list_area.x + 1,
            picker_list_area.y + 1,
        ),
        &layout,
    );
    let rendered = render_app(&app, 140, 34);
    assert!(rendered.contains("Regions"));
    assert!(rendered.contains("[x] Region 1"));
    assert!(rendered.contains("[ ] Region 2"));
    assert!(rendered.contains("View [ Regions ▼ ]"));

    let layout = ui::compute_layout(ratatui::layout::Rect::new(0, 0, 140, 34), &app);
    let mode_row = layout.browser_mode_selector.expect("mode selector");
    app.handle_mouse_event(
        mouse(
            MouseEventKind::Down(MouseButton::Left),
            mode_row.x + 1,
            mode_row.y,
        ),
        &layout,
    );
    assert!(app.browser_mode_picker_active());
    let picker_area = ui::browser_mode_picker_area(layout.browser_mode_selector, layout.form_block);
    let picker_list_area = ui::browser_mode_picker_list_area(picker_area);
    app.handle_mouse_event(
        mouse(
            MouseEventKind::Down(MouseButton::Left),
            picker_list_area.x + 1,
            picker_list_area.y + 2,
        ),
        &layout,
    );
    let rendered = render_app(&app, 140, 34);
    assert!(rendered.contains("Masks"));
    assert!(rendered.contains("[x] mask0"));
    assert!(rendered.contains("[ ] mask1"));
    assert!(rendered.contains("View [ Masks ▼ ]"));
}

#[cfg(unix)]
#[test]
fn imexplore_left_pane_picker_keyboard_selects_and_dismisses() {
    let _guard = launcher_env_lock();
    let temp = tempdir().expect("tempdir");
    let snapshot = fake_imexplore_snapshot_json_with_saved_items(
        fake_imexplore_snapshot_json(
            ProtocolImageView::Plane,
            ProtocolImageFocus::Content,
            "Image ready",
            vec!["raster".to_string()],
            vec!["View: Plane".to_string()],
            Some(ImageBrowserProbe {
                pixel_indices: vec![0, 0, 0],
                pixel_axes: vec![],
                value: 1.0,
                masked: false,
                finite: true,
                world_axes: vec![],
            }),
            Some(ImageNonDisplayAxisState {
                axis: 2,
                label: "Frequency".to_string(),
                index: 0,
                length: 3,
                pixel: 0,
            }),
        ),
        &["Region 1"],
        Some("Region 1"),
        &["mask0"],
        Some("mask0"),
    );
    let script = write_fake_imexplore_script(temp.path(), &[snapshot], None);
    set_imexplore_launcher_bin(&script);

    let schema = imexplore_app()
        .load_schema()
        .expect("load fake imexplore schema");
    let config = ConfigStore::load_for_tests(temp.path().join("casars.toml"));
    let mut app = AppState::from_schema_with_config(imexplore_app(), schema, config);
    app.set_text_value("image_path", "/tmp/fake.image");
    app.start_run_for_test();

    let layout = ui::compute_layout(ratatui::layout::Rect::new(0, 0, 140, 34), &app);
    let mode_row = layout.browser_mode_selector.expect("mode selector");
    app.handle_mouse_event(
        mouse(
            MouseEventKind::Down(MouseButton::Left),
            mode_row.x + 1,
            mode_row.y,
        ),
        &layout,
    );
    assert!(app.browser_mode_picker_active());

    app.handle_key_event(KeyEvent::new(KeyCode::Down, KeyModifiers::NONE));
    app.handle_key_event(KeyEvent::new(KeyCode::Enter, KeyModifiers::NONE));

    assert!(!app.browser_mode_picker_active());
    let rendered = render_app(&app, 140, 34);
    assert!(rendered.contains("View [ Regions ▼ ]"));
}

#[cfg(unix)]
#[test]
fn imexplore_left_pane_picker_renders_and_supports_hjkl_space_and_escape() {
    let _guard = launcher_env_lock();
    let temp = tempdir().expect("tempdir");
    let snapshot = fake_imexplore_snapshot_json_with_saved_items(
        fake_imexplore_snapshot_json(
            ProtocolImageView::Plane,
            ProtocolImageFocus::Content,
            "Image ready",
            vec!["raster".to_string()],
            vec!["View: Plane".to_string()],
            Some(ImageBrowserProbe {
                pixel_indices: vec![0, 0, 0],
                pixel_axes: vec![],
                value: 1.0,
                masked: false,
                finite: true,
                world_axes: vec![],
            }),
            Some(ImageNonDisplayAxisState {
                axis: 2,
                label: "Frequency".to_string(),
                index: 0,
                length: 3,
                pixel: 0,
            }),
        ),
        &["Region 1"],
        Some("Region 1"),
        &["mask0"],
        Some("mask0"),
    );
    let script = write_fake_imexplore_script(temp.path(), &[snapshot], None);
    set_imexplore_launcher_bin(&script);

    let schema = imexplore_app()
        .load_schema()
        .expect("load fake imexplore schema");
    let config = ConfigStore::load_for_tests(temp.path().join("casars.toml"));
    let mut app = AppState::from_schema_with_config(imexplore_app(), schema, config);
    app.set_text_value("image_path", "/tmp/fake.image");
    app.start_run_for_test();

    let layout = ui::compute_layout(ratatui::layout::Rect::new(0, 0, 140, 34), &app);
    let mode_row = layout.browser_mode_selector.expect("mode selector");
    app.handle_mouse_event(
        mouse(
            MouseEventKind::Down(MouseButton::Left),
            mode_row.x + 1,
            mode_row.y,
        ),
        &layout,
    );

    let rendered = render_app(&app, 140, 34);
    assert!(rendered.contains("Choose Left Pane View"));
    assert!(rendered.contains("Live"));
    assert!(rendered.contains("Regions"));
    assert!(rendered.contains("Masks"));
    assert_eq!(
        app.browser_mode_picker_selection(),
        Some(ImageBrowserLeftPaneMode::Live)
    );

    app.handle_key_event(KeyEvent::new(KeyCode::Char('l'), KeyModifiers::NONE));
    assert_eq!(
        app.browser_mode_picker_selection(),
        Some(ImageBrowserLeftPaneMode::Regions)
    );
    app.handle_key_event(KeyEvent::new(KeyCode::Char('h'), KeyModifiers::NONE));
    assert_eq!(
        app.browser_mode_picker_selection(),
        Some(ImageBrowserLeftPaneMode::Live)
    );

    app.handle_key_event(KeyEvent::new(KeyCode::Esc, KeyModifiers::NONE));
    assert!(!app.browser_mode_picker_active());

    let layout = ui::compute_layout(ratatui::layout::Rect::new(0, 0, 140, 34), &app);
    let mode_row = layout.browser_mode_selector.expect("mode selector");
    app.handle_mouse_event(
        mouse(
            MouseEventKind::Down(MouseButton::Left),
            mode_row.x + 1,
            mode_row.y,
        ),
        &layout,
    );
    app.handle_key_event(KeyEvent::new(KeyCode::Char('j'), KeyModifiers::NONE));
    app.handle_key_event(KeyEvent::new(KeyCode::Char(' '), KeyModifiers::NONE));

    assert!(!app.browser_mode_picker_active());
    let rendered = render_app(&app, 140, 34);
    assert!(rendered.contains("View [ Regions ▼ ]"));
}

#[cfg(unix)]
#[test]
fn imexplore_left_pane_picker_click_outside_dismisses_without_changing_mode() {
    let _guard = launcher_env_lock();
    let temp = tempdir().expect("tempdir");
    let snapshot = fake_imexplore_snapshot_json_with_saved_items(
        fake_imexplore_snapshot_json(
            ProtocolImageView::Plane,
            ProtocolImageFocus::Content,
            "Image ready",
            vec!["raster".to_string()],
            vec!["View: Plane".to_string()],
            Some(ImageBrowserProbe {
                pixel_indices: vec![0, 0, 0],
                pixel_axes: vec![],
                value: 1.0,
                masked: false,
                finite: true,
                world_axes: vec![],
            }),
            Some(ImageNonDisplayAxisState {
                axis: 2,
                label: "Frequency".to_string(),
                index: 0,
                length: 3,
                pixel: 0,
            }),
        ),
        &["Region 1"],
        Some("Region 1"),
        &["mask0"],
        Some("mask0"),
    );
    let script = write_fake_imexplore_script(temp.path(), &[snapshot], None);
    set_imexplore_launcher_bin(&script);

    let schema = imexplore_app()
        .load_schema()
        .expect("load fake imexplore schema");
    let config = ConfigStore::load_for_tests(temp.path().join("casars.toml"));
    let mut app = AppState::from_schema_with_config(imexplore_app(), schema, config);
    app.set_text_value("image_path", "/tmp/fake.image");
    app.start_run_for_test();

    let layout = ui::compute_layout(ratatui::layout::Rect::new(0, 0, 140, 34), &app);
    let mode_row = layout.browser_mode_selector.expect("mode selector");
    app.handle_mouse_event(
        mouse(
            MouseEventKind::Down(MouseButton::Left),
            mode_row.x + 1,
            mode_row.y,
        ),
        &layout,
    );
    assert!(app.browser_mode_picker_active());

    app.handle_mouse_event(
        mouse(MouseEventKind::Down(MouseButton::Left), 139, 33),
        &layout,
    );

    assert!(!app.browser_mode_picker_active());
    let rendered = render_app(&app, 140, 34);
    assert!(rendered.contains("View [ Live ▼ ]"));
}

#[cfg(unix)]
#[test]
fn imexplore_regions_mode_empty_state_and_region_actions_warn_cleanly() {
    let _guard = launcher_env_lock();
    let temp = tempdir().expect("tempdir");
    let snapshot = fake_imexplore_snapshot_json_with_saved_items(
        fake_imexplore_snapshot_json(
            ProtocolImageView::Plane,
            ProtocolImageFocus::Content,
            "Image ready",
            vec!["raster".to_string()],
            vec!["View: Plane".to_string()],
            Some(ImageBrowserProbe {
                pixel_indices: vec![0, 0, 0],
                pixel_axes: vec![],
                value: 1.0,
                masked: false,
                finite: true,
                world_axes: vec![],
            }),
            Some(ImageNonDisplayAxisState {
                axis: 2,
                label: "Frequency".to_string(),
                index: 0,
                length: 3,
                pixel: 0,
            }),
        ),
        &[],
        None,
        &[],
        None,
    );
    let script = write_fake_imexplore_script(temp.path(), &[snapshot], None);
    set_imexplore_launcher_bin(&script);

    let schema = imexplore_app()
        .load_schema()
        .expect("load fake imexplore schema");
    let config = ConfigStore::load_for_tests(temp.path().join("casars.toml"));
    let mut app = AppState::from_schema_with_config(imexplore_app(), schema, config);
    app.set_text_value("image_path", "/tmp/fake.image");
    app.start_run_for_test();
    switch_imexplore_left_pane_mode(&mut app, 1);

    let rendered = render_app(&app, 140, 34);
    assert!(rendered.contains("No saved regions."));

    app.handle_key_event(KeyEvent::new(KeyCode::Char('E'), KeyModifiers::SHIFT));
    let rendered = render_app(&app, 140, 34);
    assert!(rendered.contains("Select a saved region before renaming it."));

    app.handle_key_event(KeyEvent::new(KeyCode::Delete, KeyModifiers::NONE));
    let rendered = render_app(&app, 140, 34);
    assert!(rendered.contains("Select a saved region before deleting it."));
}

#[cfg(unix)]
#[test]
fn imexplore_masks_mode_empty_state_and_mask_actions_warn_cleanly() {
    let _guard = launcher_env_lock();
    let temp = tempdir().expect("tempdir");
    let snapshot = fake_imexplore_snapshot_json_with_saved_items(
        fake_imexplore_snapshot_json(
            ProtocolImageView::Plane,
            ProtocolImageFocus::Content,
            "Image ready",
            vec!["raster".to_string()],
            vec!["View: Plane".to_string()],
            Some(ImageBrowserProbe {
                pixel_indices: vec![0, 0, 0],
                pixel_axes: vec![],
                value: 1.0,
                masked: false,
                finite: true,
                world_axes: vec![],
            }),
            Some(ImageNonDisplayAxisState {
                axis: 2,
                label: "Frequency".to_string(),
                index: 0,
                length: 3,
                pixel: 0,
            }),
        ),
        &[],
        None,
        &[],
        None,
    );
    let script = write_fake_imexplore_script(temp.path(), &[snapshot], None);
    set_imexplore_launcher_bin(&script);

    let schema = imexplore_app()
        .load_schema()
        .expect("load fake imexplore schema");
    let config = ConfigStore::load_for_tests(temp.path().join("casars.toml"));
    let mut app = AppState::from_schema_with_config(imexplore_app(), schema, config);
    app.set_text_value("image_path", "/tmp/fake.image");
    app.start_run_for_test();
    switch_imexplore_left_pane_mode(&mut app, 2);

    let rendered = render_app(&app, 140, 34);
    assert!(rendered.contains("No masks."));

    app.handle_key_event(KeyEvent::new(KeyCode::Delete, KeyModifiers::NONE));
    let rendered = render_app(&app, 140, 34);
    assert!(rendered.contains("No mask selected."));
}

#[cfg(unix)]
#[test]
fn imexplore_browser_manager_rows_start_below_selector_and_clip_to_available_space() {
    let _guard = launcher_env_lock();
    let temp = tempdir().expect("tempdir");
    let snapshot = fake_imexplore_snapshot_json_with_saved_items(
        fake_imexplore_snapshot_json(
            ProtocolImageView::Plane,
            ProtocolImageFocus::Content,
            "Image ready",
            vec!["raster".to_string()],
            vec!["View: Plane".to_string()],
            Some(ImageBrowserProbe {
                pixel_indices: vec![0, 0, 0],
                pixel_axes: vec![],
                value: 1.0,
                masked: false,
                finite: true,
                world_axes: vec![],
            }),
            Some(ImageNonDisplayAxisState {
                axis: 2,
                label: "Frequency".to_string(),
                index: 0,
                length: 3,
                pixel: 0,
            }),
        ),
        &[
            "Region 1", "Region 2", "Region 3", "Region 4", "Region 5", "Region 6",
        ],
        Some("Region 1"),
        &[],
        None,
    );
    let script = write_fake_imexplore_script(temp.path(), &[snapshot], None);
    set_imexplore_launcher_bin(&script);

    let schema = imexplore_app()
        .load_schema()
        .expect("load fake imexplore schema");
    let config = ConfigStore::load_for_tests(temp.path().join("casars.toml"));
    let mut app = AppState::from_schema_with_config(imexplore_app(), schema, config);
    app.set_text_value("image_path", "/tmp/fake.image");
    app.start_run_for_test();
    switch_imexplore_left_pane_mode(&mut app, 1);

    let layout = ui::compute_layout(ratatui::layout::Rect::new(0, 0, 70, 12), &app);
    let selector = layout.browser_mode_selector.expect("mode selector");
    assert!(!layout.browser_manager_rows.is_empty());
    assert!(layout.browser_manager_rows.len() < 6);
    assert!(layout.browser_manager_rows[0].rect.y > selector.y);
}

#[cfg(unix)]
#[test]
fn imexplore_left_pane_actions_target_selected_region_and_mask() {
    let _guard = launcher_env_lock();
    let temp = tempdir().expect("tempdir");
    let startup = fake_imexplore_snapshot_json_with_saved_items(
        fake_imexplore_snapshot_json(
            ProtocolImageView::Plane,
            ProtocolImageFocus::Content,
            "Image ready",
            vec!["raster".to_string()],
            vec!["View: Plane".to_string()],
            Some(ImageBrowserProbe {
                pixel_indices: vec![0, 0, 0],
                pixel_axes: vec![],
                value: 1.0,
                masked: false,
                finite: true,
                world_axes: vec![],
            }),
            Some(ImageNonDisplayAxisState {
                axis: 2,
                label: "Frequency".to_string(),
                index: 0,
                length: 3,
                pixel: 0,
            }),
        ),
        &["Region 1", "Science Region"],
        None,
        &["mask0", "science_mask"],
        Some("mask0"),
    );
    let after_region_load = fake_imexplore_snapshot_json_with_saved_items(
        startup.clone(),
        &["Region 1", "Science Region"],
        Some("Science Region"),
        &["mask0", "science_mask"],
        Some("mask0"),
    );
    let after_mask_default = fake_imexplore_snapshot_json_with_saved_items(
        startup.clone(),
        &["Region 1", "Science Region"],
        Some("Science Region"),
        &["mask0", "science_mask"],
        Some("science_mask"),
    );
    let script = write_fake_imexplore_script(
        temp.path(),
        &[startup, after_region_load, after_mask_default],
        None,
    );
    set_imexplore_launcher_bin(&script);

    let schema = imexplore_app()
        .load_schema()
        .expect("load fake imexplore schema");
    let config = ConfigStore::load_for_tests(temp.path().join("casars.toml"));
    let mut app = AppState::from_schema_with_config(imexplore_app(), schema, config);
    app.set_text_value("image_path", "/tmp/fake.image");
    app.start_run_for_test();

    let layout = ui::compute_layout(ratatui::layout::Rect::new(0, 0, 140, 34), &app);
    let mode_row = layout.browser_mode_selector.expect("mode selector");
    app.handle_mouse_event(
        mouse(
            MouseEventKind::Down(MouseButton::Left),
            mode_row.x + 1,
            mode_row.y,
        ),
        &layout,
    );
    let picker_area = ui::browser_mode_picker_area(layout.browser_mode_selector, layout.form_block);
    let picker_list_area = ui::browser_mode_picker_list_area(picker_area);
    app.handle_mouse_event(
        mouse(
            MouseEventKind::Down(MouseButton::Left),
            picker_list_area.x + 1,
            picker_list_area.y + 1,
        ),
        &layout,
    );

    let layout = ui::compute_layout(ratatui::layout::Rect::new(0, 0, 140, 34), &app);
    let region_row = layout
        .browser_manager_rows
        .iter()
        .find(|row| {
            row.target
                == crate::app::FormSelection::BrowserPane(
                    crate::app::BrowserPaneSelection::SavedRegion(1),
                )
        })
        .expect("second region row");
    app.handle_mouse_event(
        mouse(
            MouseEventKind::Down(MouseButton::Left),
            region_row.rect.x + 1,
            region_row.rect.y,
        ),
        &layout,
    );
    assert_eq!(
        app.image_browser_snapshot_for_test()
            .and_then(|snapshot| snapshot.active_region_definition_name.as_deref()),
        Some("Science Region")
    );

    let layout = ui::compute_layout(ratatui::layout::Rect::new(0, 0, 140, 34), &app);
    let mode_row = layout.browser_mode_selector.expect("mode selector");
    app.handle_mouse_event(
        mouse(
            MouseEventKind::Down(MouseButton::Left),
            mode_row.x + 1,
            mode_row.y,
        ),
        &layout,
    );
    let picker_area = ui::browser_mode_picker_area(layout.browser_mode_selector, layout.form_block);
    let picker_list_area = ui::browser_mode_picker_list_area(picker_area);
    app.handle_mouse_event(
        mouse(
            MouseEventKind::Down(MouseButton::Left),
            picker_list_area.x + 1,
            picker_list_area.y + 2,
        ),
        &layout,
    );

    let layout = ui::compute_layout(ratatui::layout::Rect::new(0, 0, 140, 34), &app);
    let mask_row = layout
        .browser_manager_rows
        .iter()
        .find(|row| {
            row.target
                == crate::app::FormSelection::BrowserPane(crate::app::BrowserPaneSelection::Mask(1))
        })
        .expect("second mask row");
    app.handle_mouse_event(
        mouse(
            MouseEventKind::Down(MouseButton::Left),
            mask_row.rect.x + 1,
            mask_row.rect.y,
        ),
        &layout,
    );
    assert_eq!(
        app.image_browser_snapshot_for_test()
            .and_then(|snapshot| snapshot.default_mask_name.as_deref()),
        Some("science_mask")
    );
}

#[cfg(unix)]
#[test]
fn imexplore_mask_checkbox_toggles_default_mask_off() {
    let _guard = launcher_env_lock();
    let temp = tempdir().expect("tempdir");
    let startup = fake_imexplore_snapshot_json_with_saved_items(
        fake_imexplore_snapshot_json(
            ProtocolImageView::Plane,
            ProtocolImageFocus::Content,
            "Image ready",
            vec!["raster".to_string()],
            vec!["View: Plane".to_string()],
            Some(ImageBrowserProbe {
                pixel_indices: vec![0, 0, 0],
                pixel_axes: vec![],
                value: 1.0,
                masked: false,
                finite: true,
                world_axes: vec![],
            }),
            Some(ImageNonDisplayAxisState {
                axis: 2,
                label: "Frequency".to_string(),
                index: 0,
                length: 3,
                pixel: 0,
            }),
        ),
        &[],
        None,
        &["mask0", "mask1"],
        Some("mask0"),
    );
    let after_unset = fake_imexplore_snapshot_json_with_saved_items(
        startup.clone(),
        &[],
        None,
        &["mask0", "mask1"],
        None,
    );
    let script = write_fake_imexplore_script(temp.path(), &[startup, after_unset], None);
    set_imexplore_launcher_bin(&script);

    let schema = imexplore_app()
        .load_schema()
        .expect("load fake imexplore schema");
    let config = ConfigStore::load_for_tests(temp.path().join("casars.toml"));
    let mut app = AppState::from_schema_with_config(imexplore_app(), schema, config);
    app.set_text_value("image_path", "/tmp/fake.image");
    app.start_run_for_test();
    switch_imexplore_left_pane_mode(&mut app, 2);

    let layout = ui::compute_layout(ratatui::layout::Rect::new(0, 0, 140, 34), &app);
    let mask_row = layout
        .browser_manager_rows
        .iter()
        .find(|row| {
            row.target
                == crate::app::FormSelection::BrowserPane(crate::app::BrowserPaneSelection::Mask(0))
        })
        .expect("first mask row");
    app.handle_mouse_event(
        mouse(
            MouseEventKind::Down(MouseButton::Left),
            mask_row.rect.x + 1,
            mask_row.rect.y,
        ),
        &layout,
    );

    assert_eq!(
        app.image_browser_snapshot_for_test()
            .and_then(|snapshot| snapshot.default_mask_name.as_deref()),
        None
    );
}

#[cfg(unix)]
#[test]
fn imexplore_clicking_region_name_opens_rename_prompt() {
    let _guard = launcher_env_lock();
    let temp = tempdir().expect("tempdir");
    let snapshot = fake_imexplore_snapshot_json_with_saved_items(
        fake_imexplore_snapshot_json(
            ProtocolImageView::Plane,
            ProtocolImageFocus::Content,
            "Image ready",
            vec!["raster".to_string()],
            vec!["View: Plane".to_string()],
            Some(ImageBrowserProbe {
                pixel_indices: vec![0, 0, 0],
                pixel_axes: vec![],
                value: 1.0,
                masked: false,
                finite: true,
                world_axes: vec![],
            }),
            Some(ImageNonDisplayAxisState {
                axis: 2,
                label: "Frequency".to_string(),
                index: 0,
                length: 3,
                pixel: 0,
            }),
        ),
        &["Region 1", "Science Region"],
        None,
        &["mask0"],
        Some("mask0"),
    );
    let script = write_fake_imexplore_script(temp.path(), &[snapshot], None);
    set_imexplore_launcher_bin(&script);

    let schema = imexplore_app()
        .load_schema()
        .expect("load fake imexplore schema");
    let config = ConfigStore::load_for_tests(temp.path().join("casars.toml"));
    let mut app = AppState::from_schema_with_config(imexplore_app(), schema, config);
    app.set_text_value("image_path", "/tmp/fake.image");
    app.start_run_for_test();

    let layout = ui::compute_layout(ratatui::layout::Rect::new(0, 0, 140, 34), &app);
    let mode_row = layout.browser_mode_selector.expect("mode selector");
    app.handle_mouse_event(
        mouse(
            MouseEventKind::Down(MouseButton::Left),
            mode_row.x + 1,
            mode_row.y,
        ),
        &layout,
    );
    let picker_area = ui::browser_mode_picker_area(layout.browser_mode_selector, layout.form_block);
    let picker_list_area = ui::browser_mode_picker_list_area(picker_area);
    app.handle_mouse_event(
        mouse(
            MouseEventKind::Down(MouseButton::Left),
            picker_list_area.x + 1,
            picker_list_area.y + 1,
        ),
        &layout,
    );

    let layout = ui::compute_layout(ratatui::layout::Rect::new(0, 0, 140, 34), &app);
    let region_row = layout
        .browser_manager_rows
        .iter()
        .find(|row| {
            row.target
                == crate::app::FormSelection::BrowserPane(
                    crate::app::BrowserPaneSelection::SavedRegion(1),
                )
        })
        .expect("second region row");
    app.handle_mouse_event(
        mouse(
            MouseEventKind::Down(MouseButton::Left),
            region_row.rect.x + 8,
            region_row.rect.y,
        ),
        &layout,
    );

    let rendered = render_app(&app, 140, 34);
    assert!(rendered.contains("Science Region|"));
}

#[cfg(unix)]
#[test]
fn imexplore_region_summary_auto_scales_stat_units() {
    let _guard = launcher_env_lock();
    let temp = tempdir().expect("tempdir");
    let snapshot = fake_imexplore_snapshot_json_with_saved_items(
        fake_imexplore_snapshot_json_with_region_stats(
            fake_imexplore_snapshot_json(
                ProtocolImageView::Plane,
                ProtocolImageFocus::Content,
                "Image ready",
                vec!["raster".to_string()],
                vec!["View: Plane".to_string()],
                Some(ImageBrowserProbe {
                    pixel_indices: vec![0, 0, 0],
                    pixel_axes: vec![],
                    value: 1.0,
                    masked: false,
                    finite: true,
                    world_axes: vec![],
                }),
                Some(ImageNonDisplayAxisState {
                    axis: 2,
                    label: "Frequency".to_string(),
                    index: 0,
                    length: 3,
                    pixel: 0,
                }),
            ),
            ImageRegionStatsState {
                pixel_count: 9,
                median: 0.08,
                min: 0.05,
                max: 0.25,
                mean: 0.1,
                sigma: 0.02,
                rms: 0.12,
                sum: 0.9,
                value_unit: "Jy/beam".to_string(),
            },
        ),
        &["Region 1"],
        Some("Region 1"),
        &[],
        None,
    );
    let script = write_fake_imexplore_script(temp.path(), &[snapshot], None);
    set_imexplore_launcher_bin(&script);

    let schema = imexplore_app()
        .load_schema()
        .expect("load fake imexplore schema");
    let config = ConfigStore::load_for_tests(temp.path().join("casars.toml"));
    let mut app = AppState::from_schema_with_config(imexplore_app(), schema, config);
    app.set_text_value("image_path", "/tmp/fake.image");
    app.start_run_for_test();

    let layout = ui::compute_layout(ratatui::layout::Rect::new(0, 0, 140, 34), &app);
    let mode_row = layout.browser_mode_selector.expect("mode selector");
    app.handle_mouse_event(
        mouse(
            MouseEventKind::Down(MouseButton::Left),
            mode_row.x + 1,
            mode_row.y,
        ),
        &layout,
    );
    let picker_area = ui::browser_mode_picker_area(layout.browser_mode_selector, layout.form_block);
    let picker_list_area = ui::browser_mode_picker_list_area(picker_area);
    app.handle_mouse_event(
        mouse(
            MouseEventKind::Down(MouseButton::Left),
            picker_list_area.x + 1,
            picker_list_area.y + 1,
        ),
        &layout,
    );

    let rendered = render_app(&app, 140, 34);
    assert!(rendered.contains("Mean: 100 mJy/beam"));
    assert!(rendered.contains("Sigma: 20 mJy/beam"));
    assert!(rendered.contains("Median: 80 mJy/beam"));
    assert!(rendered.contains("Min / Max: 50 mJy/beam / 250 mJy/beam"));
}

#[cfg(unix)]
#[test]
fn imexplore_local_display_controls_update_inspector_lines() {
    let _guard = launcher_env_lock();
    let temp = tempdir().expect("tempdir");
    let script = write_fake_imexplore_script(
        temp.path(),
        &[fake_imexplore_snapshot_json(
            ProtocolImageView::Plane,
            ProtocolImageFocus::Content,
            "Image ready",
            vec!["raster".to_string()],
            vec!["View: Plane".to_string()],
            Some(ImageBrowserProbe {
                pixel_indices: vec![0, 0],
                pixel_axes: vec![],
                value: 1.0,
                masked: false,
                finite: true,
                world_axes: vec![],
            }),
            Some(ImageNonDisplayAxisState {
                axis: 2,
                label: "Frequency".to_string(),
                index: 0,
                length: 3,
                pixel: 0,
            }),
        )],
        None,
    );
    set_imexplore_launcher_bin(&script);

    let schema = imexplore_app()
        .load_schema()
        .expect("load fake imexplore schema");
    let config = ConfigStore::load_for_tests(temp.path().join("casars.toml"));
    let mut app = AppState::from_schema_with_config(imexplore_app(), schema, config);
    app.set_text_value("image_path", "/tmp/fake.image");
    app.start_run_for_test();

    app.handle_key_event(KeyEvent::new(KeyCode::Char('c'), KeyModifiers::NONE));
    app.handle_key_event(KeyEvent::new(KeyCode::Char('i'), KeyModifiers::NONE));

    let rendered = render_app(&app, 140, 30);
    assert!(rendered.contains("Colormap: viridis"));
    assert!(rendered.contains("Invert: on"));
    assert!(rendered.contains("Movie FPS: 1"));

    app.set_text_value_and_apply("fps", "4");
    let rendered = render_app(&app, 140, 30);
    assert!(rendered.contains("Movie FPS: 4"));
}

#[cfg(unix)]
#[test]
fn imexplore_cycles_to_spectrum_tab() {
    let _guard = launcher_env_lock();
    let temp = tempdir().expect("tempdir");
    let script = write_fake_imexplore_script(
        temp.path(),
        &[
            fake_imexplore_snapshot_json(
                ProtocolImageView::Metadata,
                ProtocolImageFocus::Content,
                "Image ready",
                vec!["== Summary ==".to_string()],
                vec!["View: Metadata".to_string()],
                None,
                Some(ImageNonDisplayAxisState {
                    axis: 2,
                    label: "Frequency".to_string(),
                    index: 0,
                    length: 3,
                    pixel: 0,
                }),
            ),
            fake_imexplore_snapshot_json(
                ProtocolImageView::Coordinates,
                ProtocolImageFocus::Content,
                "Image ready",
                vec!["== Coordinates ==".to_string()],
                vec!["View: Coordinates".to_string()],
                None,
                Some(ImageNonDisplayAxisState {
                    axis: 2,
                    label: "Frequency".to_string(),
                    index: 0,
                    length: 3,
                    pixel: 0,
                }),
            ),
            fake_imexplore_snapshot_json(
                ProtocolImageView::Plane,
                ProtocolImageFocus::Content,
                "Image ready",
                vec!["raster".to_string()],
                vec!["View: Plane".to_string()],
                Some(ImageBrowserProbe {
                    pixel_indices: vec![0, 0, 0],
                    pixel_axes: vec![],
                    value: 1.0,
                    masked: false,
                    finite: true,
                    world_axes: vec![],
                }),
                Some(ImageNonDisplayAxisState {
                    axis: 2,
                    label: "Frequency".to_string(),
                    index: 0,
                    length: 3,
                    pixel: 0,
                }),
            ),
            fake_imexplore_snapshot_json(
                ProtocolImageView::Spectrum,
                ProtocolImageFocus::Content,
                "Image ready",
                vec![
                    "Profile axis: Frequency (2) [Spectral]".to_string(),
                    "Selected sample: idx=0 pixel=0 world=115020000000 Hz value=1".to_string(),
                ],
                vec!["View: Spectrum".to_string()],
                Some(ImageBrowserProbe {
                    pixel_indices: vec![0, 0, 0],
                    pixel_axes: vec![],
                    value: 1.0,
                    masked: false,
                    finite: true,
                    world_axes: vec![],
                }),
                Some(ImageNonDisplayAxisState {
                    axis: 2,
                    label: "Frequency".to_string(),
                    index: 0,
                    length: 3,
                    pixel: 0,
                }),
            ),
        ],
        None,
    );
    set_imexplore_launcher_bin(&script);

    let schema = imexplore_app()
        .load_schema()
        .expect("load fake imexplore schema");
    let config = ConfigStore::load_for_tests(temp.path().join("casars.toml"));
    let mut app = AppState::from_schema_with_config(imexplore_app(), schema, config);
    app.set_text_value("image_path", "/tmp/fake.image");
    app.start_run_for_test();

    app.handle_key_event(KeyEvent::new(KeyCode::Char(']'), KeyModifiers::NONE));
    app.handle_key_event(KeyEvent::new(KeyCode::Char(']'), KeyModifiers::NONE));
    app.handle_key_event(KeyEvent::new(KeyCode::Char(']'), KeyModifiers::NONE));

    assert_eq!(app.active_browser_tab_label(), Some("Spectrum"));
    let rendered = render_app(&app, 120, 24);
    assert!(rendered.contains("Profile axis: Frequency (2) [Spectral]"));
}

#[test]
fn edit_tab_commits_and_moves_to_next_field() {
    let (_temp, mut app) = test_app();
    app.handle_key_event(KeyEvent::new(KeyCode::Enter, KeyModifiers::NONE));
    app.handle_paste("/tmp/demo.ms".to_string());
    app.handle_key_event(KeyEvent::new(KeyCode::Tab, KeyModifiers::NONE));

    assert!(app.edit_buffer_for_test().is_none());
    assert_eq!(
        app.field_text_for_test("ms_path").as_deref(),
        Some("/tmp/demo.ms")
    );
    assert!(
        app.selected_form_text_for_test()
            .is_some_and(|text| text.contains("Verbose Report"))
    );
}

#[test]
fn plot_tab_tab_cycles_focus_ring_and_skips_collapsed_sidebar() {
    let (_temp, mut app) = test_app();
    app.set_active_result_tab(ResultTab::Plots);

    app.handle_key_event(KeyEvent::new(KeyCode::Tab, KeyModifiers::NONE));
    assert_eq!(app.pane_focus_for_test(), PaneFocus::Result);
    assert_eq!(app.plot_focus(), PlotPaneFocus::Catalog);

    app.handle_key_event(KeyEvent::new(KeyCode::Tab, KeyModifiers::NONE));
    assert_eq!(app.plot_focus(), PlotPaneFocus::Canvas);

    app.handle_key_event(KeyEvent::new(KeyCode::Tab, KeyModifiers::NONE));
    assert_eq!(app.plot_focus(), PlotPaneFocus::Controls);

    app.handle_key_event(KeyEvent::new(KeyCode::Char('p'), KeyModifiers::NONE));
    assert!(app.parameters_pane_collapsed());
    assert_eq!(app.plot_focus(), PlotPaneFocus::Catalog);

    app.handle_key_event(KeyEvent::new(KeyCode::BackTab, KeyModifiers::NONE));
    assert_eq!(app.pane_focus_for_test(), PaneFocus::Result);
    assert_eq!(app.plot_focus(), PlotPaneFocus::Controls);
}

#[cfg(unix)]
#[test]
fn browser_tab_moves_focus_and_brackets_switch_views() {
    let _guard = launcher_env_lock();
    let temp = tempdir().expect("tempdir");
    let inspector = Some(BrowserInspectorSnapshot {
        title: "Cell row=0 column=NAME".to_string(),
        trail: vec![BrowserInspectorTrailEntry {
            label: "root".to_string(),
            summary: "scalar".to_string(),
        }],
        node: BrowserValueNode::Scalar {
            value: BrowserScalarValue::String("alpha".to_string()),
        },
        rendered_lines: vec!["scalar: alpha".to_string()],
    });
    let script = write_fake_tablebrowser_script(
        temp.path(),
        &[
            fake_browser_snapshot_with_focus_and_metrics_json(
                ProtocolBrowserView::Cells,
                BrowserFocus::Main,
                "Fake cells",
                vec!["Cells".to_string(), "\"alpha\"".to_string()],
                None,
                None,
                inspector.clone(),
            ),
            fake_browser_snapshot_with_focus_and_metrics_json(
                ProtocolBrowserView::Cells,
                BrowserFocus::Inspector,
                "Fake cells inspector",
                vec!["Cells".to_string(), "\"alpha\"".to_string()],
                None,
                None,
                inspector,
            ),
            fake_browser_snapshot_json(
                ProtocolBrowserView::Columns,
                "Fake columns",
                vec!["Columns".to_string(), "NAME".to_string()],
            ),
        ],
        None,
    );
    set_tablebrowser_launcher_bin(&script);

    let schema = tablebrowser_app()
        .load_schema()
        .expect("load fake tablebrowser schema");
    let config = ConfigStore::load_for_tests(temp.path().join("casars.toml"));
    let mut app = AppState::from_schema_with_config(tablebrowser_app(), schema, config);
    app.set_text_value("table_path", "/tmp/fake.ms");
    app.start_run_for_test();

    assert_eq!(app.browser_focus_for_test(), Some(BrowserPaneFocus::Main));
    app.handle_key_event(KeyEvent::new(KeyCode::Tab, KeyModifiers::NONE));
    assert_eq!(
        app.browser_focus_for_test(),
        Some(BrowserPaneFocus::Inspector)
    );
    assert_eq!(app.pane_focus_for_test(), PaneFocus::Parameters);
    assert_eq!(app.active_browser_tab_label(), Some("Cells"));

    app.handle_key_event(KeyEvent::new(KeyCode::Char(']'), KeyModifiers::NONE));
    assert_eq!(app.browser_focus_for_test(), Some(BrowserPaneFocus::Main));
    assert_eq!(app.pane_focus_for_test(), PaneFocus::Result);
    assert_eq!(app.active_browser_tab_label(), Some("Columns"));
}

#[cfg(unix)]
#[test]
fn browser_tabs_are_clickable() {
    let _guard = launcher_env_lock();
    let temp = tempdir().expect("tempdir");
    let script = write_fake_tablebrowser_script(
        temp.path(),
        &[
            fake_browser_snapshot_json(
                ProtocolBrowserView::Overview,
                "Fake overview",
                vec!["Overview".to_string()],
            ),
            fake_browser_snapshot_json(
                ProtocolBrowserView::Columns,
                "Fake columns",
                vec!["Columns".to_string()],
            ),
            fake_browser_snapshot_json(
                ProtocolBrowserView::Keywords,
                "Fake keywords",
                vec!["Keywords".to_string()],
            ),
            fake_browser_snapshot_json(
                ProtocolBrowserView::Cells,
                "Fake cells",
                vec!["Cells".to_string()],
            ),
        ],
        None,
    );
    set_tablebrowser_launcher_bin(&script);

    let schema = tablebrowser_app()
        .load_schema()
        .expect("load fake tablebrowser schema");
    let config = ConfigStore::load_for_tests(temp.path().join("casars.toml"));
    let mut app = AppState::from_schema_with_config(tablebrowser_app(), schema, config);
    app.set_text_value("table_path", "/tmp/fake.ms");
    app.start_run_for_test();

    let layout = ui::compute_layout(ratatui::layout::Rect::new(0, 0, 120, 30), &app);
    let cells_tab = layout
        .browser_tab_hits
        .iter()
        .find(|hit| hit.tab == crate::app::BrowserTab::Cells)
        .expect("cells browser tab");
    app.handle_mouse_event(
        mouse(
            MouseEventKind::Down(MouseButton::Left),
            cells_tab.rect.x,
            cells_tab.rect.y,
        ),
        &layout,
    );

    assert_eq!(app.active_browser_tab_label(), Some("Cells"));
}

#[test]
fn back_to_launcher_closes_active_browser_session() {
    let _guard = launcher_env_lock();
    clear_tablebrowser_launcher_bin();

    let temp = tempdir().expect("tempdir");
    let table_path = create_fixture_table(temp.path());
    let schema = tablebrowser_app()
        .load_schema()
        .expect("tablebrowser schema");
    let config = ConfigStore::load_for_tests(temp.path().join("casars.toml"));
    let mut app = AppState::from_schema_with_config(tablebrowser_app(), schema, config);
    app.set_text_value("table_path", table_path.to_string_lossy().as_ref());
    app.start_run_for_test();

    assert!(app.browser_is_active());
    app.handle_key_event(KeyEvent::new(KeyCode::Char('b'), KeyModifiers::NONE));
    assert!(app.should_return_to_launcher_for_test());
    assert!(!app.browser_is_active());
}

#[cfg(unix)]
#[test]
fn browser_cells_expose_scrollbar_metrics() {
    let _guard = launcher_env_lock();
    let temp = tempdir().expect("tempdir");
    let script = write_fake_tablebrowser_script(
        temp.path(),
        &[fake_browser_snapshot_with_metrics_json(
            ProtocolBrowserView::Cells,
            "Fake cells",
            vec![
                "Cells  focus=Main".to_string(),
                "row | NAME<str> | UVW<f64[]>[m] |".to_string(),
                "  11 | \"alpha\"    | [1.0, 2.0, 3.0] m |".to_string(),
                "  12 | \"beta\"     | [4.0, 5.0, 6.0] m |".to_string(),
            ],
            Some(BrowserNavigationMetrics {
                selected_index: 11,
                total_items: 100,
                viewport_items: 2,
            }),
            Some(BrowserNavigationMetrics {
                selected_index: 2,
                total_items: 8,
                viewport_items: 2,
            }),
            None,
        )],
        None,
    );
    set_tablebrowser_launcher_bin(&script);

    let schema = tablebrowser_app()
        .load_schema()
        .expect("load fake tablebrowser schema");
    let config = ConfigStore::load_for_tests(temp.path().join("casars.toml"));
    let mut app = AppState::from_schema_with_config(tablebrowser_app(), schema, config);
    app.set_text_value("table_path", "/tmp/fake.ms");
    app.start_run_for_test();

    assert_eq!(app.active_browser_scroll(), 11);
    assert_eq!(app.active_browser_hscroll(), 2);
    assert_eq!(app.active_browser_scroll_metrics(12), Some((100, 2)));
    assert_eq!(app.active_browser_hscroll_metrics(40), Some((8, 2)));
}

#[cfg(unix)]
#[test]
fn browser_cells_render_styled_separators_and_strip_selection_markers() {
    let _guard = launcher_env_lock();
    let temp = tempdir().expect("tempdir");
    let script = write_fake_tablebrowser_script(
        temp.path(),
        &[fake_browser_snapshot_json(
            ProtocolBrowserView::Cells,
            "Fake cells",
            vec![
                "Cells  row=1/10  col=1/3  focus=Main".to_string(),
                "row | NAME<str> | UVW<f64[3]>[m] |".to_string(),
                "   0 | >\"alpha\"< | [1.0, 2.0, 3.0] |".to_string(),
            ],
        )],
        None,
    );
    set_tablebrowser_launcher_bin(&script);

    let schema = tablebrowser_app()
        .load_schema()
        .expect("load fake tablebrowser schema");
    let config = ConfigStore::load_for_tests(temp.path().join("casars.toml"));
    let mut app = AppState::from_schema_with_config(tablebrowser_app(), schema, config);
    app.set_text_value("table_path", "/tmp/fake.ms");
    app.start_run_for_test();

    let rendered = render_app(&app, 160, 28);
    assert!(rendered.contains("│"));
    assert!(!rendered.contains(">\"alpha\"<"));
    assert!(rendered.contains("\"alpha\""));
}

#[cfg(unix)]
#[test]
fn browser_inspector_renders_in_left_pane_without_duplicate_result_content() {
    let _guard = launcher_env_lock();
    let temp = tempdir().expect("tempdir");
    let script = write_fake_tablebrowser_script(
        temp.path(),
        &[fake_browser_snapshot_with_inspector_json(
            ProtocolBrowserView::Cells,
            "Fake cells",
            vec![
                "Cells  row=1/10  col=1/3  focus=Main".to_string(),
                "row | NAME<str> |".to_string(),
                "   0 | >\"alpha\"< |".to_string(),
                String::new(),
                "-- Inspector (Main) --".to_string(),
                "scalar: \"alpha\"".to_string(),
            ],
            Some(BrowserInspectorSnapshot {
                title: "Cell row=0 column=NAME".to_string(),
                trail: vec![BrowserInspectorTrailEntry {
                    label: "root".to_string(),
                    summary: "\"alpha\"".to_string(),
                }],
                node: BrowserValueNode::Scalar {
                    value: casacore_tablebrowser_protocol::BrowserScalarValue::String(
                        "alpha".to_string(),
                    ),
                },
                rendered_lines: vec![
                    "-- Inspector (Main) --".to_string(),
                    "scalar: \"alpha\"".to_string(),
                ],
            }),
        )],
        None,
    );
    set_tablebrowser_launcher_bin(&script);

    let schema = tablebrowser_app()
        .load_schema()
        .expect("load fake tablebrowser schema");
    let config = ConfigStore::load_for_tests(temp.path().join("casars.toml"));
    let mut app = AppState::from_schema_with_config(tablebrowser_app(), schema, config);
    app.set_text_value("table_path", "/tmp/fake.ms");
    app.start_run_for_test();

    let rendered = render_app(&app, 160, 28);
    assert!(rendered.contains("Cell row=0 column=NAME"));
    assert!(rendered.contains("scalar: \"alpha\""));
    assert_eq!(rendered.matches("-- Inspector (Main) --").count(), 1);
}

#[cfg(unix)]
#[test]
fn browser_copy_shortcut_writes_selected_value_to_clipboard() {
    let _guard = launcher_env_lock();
    let temp = tempdir().expect("tempdir");
    let clipboard_path = temp.path().join("clipboard.txt");
    set_test_clipboard_file(&clipboard_path);
    let script = write_fake_tablebrowser_script(
        temp.path(),
        &[fake_browser_snapshot_with_inspector_json(
            ProtocolBrowserView::Cells,
            "Fake cells",
            vec![
                "Cells  row=1/10  col=1/3  focus=Main".to_string(),
                "row | NAME<str> |".to_string(),
                "   0 | >\"alpha\"< |".to_string(),
            ],
            Some(BrowserInspectorSnapshot {
                title: "Cell row=0 column=NAME".to_string(),
                trail: vec![],
                node: BrowserValueNode::Scalar {
                    value: BrowserScalarValue::String("alpha".to_string()),
                },
                rendered_lines: vec!["\"alpha\"".to_string()],
            }),
        )],
        None,
    );
    set_tablebrowser_launcher_bin(&script);

    let schema = tablebrowser_app()
        .load_schema()
        .expect("load fake tablebrowser schema");
    let config = ConfigStore::load_for_tests(temp.path().join("casars.toml"));
    let mut app = AppState::from_schema_with_config(tablebrowser_app(), schema, config);
    app.set_text_value("table_path", "/tmp/fake.ms");
    app.start_run_for_test();
    app.handle_key_event(KeyEvent::new(KeyCode::Char('y'), KeyModifiers::NONE));

    let clipboard = std::fs::read_to_string(&clipboard_path).expect("clipboard contents");
    assert_eq!(clipboard, "\"alpha\"");
    let rendered = render_app(&app, 120, 28);
    assert!(rendered.contains("Copied value to clipboard."));
    clear_test_clipboard_file();
}

#[cfg(unix)]
#[test]
fn output_selection_copy_shortcuts_match_visible_plain_text() {
    let _guard = launcher_env_lock();
    let temp = tempdir().expect("tempdir");
    let clipboard_path = temp.path().join("clipboard.txt");
    set_test_clipboard_file(&clipboard_path);

    let (_temp, mut app) = test_app();
    app.set_result_for_test("alpha beta\ngamma delta", "");
    app.set_active_result_tab(ResultTab::Stdout);
    drag_select_visible_text(&mut app, 120, 28, OutputPane::Result, "alpha");

    for key in [
        KeyEvent::new(KeyCode::Char('y'), KeyModifiers::NONE),
        KeyEvent::new(KeyCode::Char('c'), KeyModifiers::SUPER),
        KeyEvent::new(
            KeyCode::Char('c'),
            KeyModifiers::CONTROL | KeyModifiers::SHIFT,
        ),
    ] {
        app.handle_key_event(key);
        let clipboard = std::fs::read_to_string(&clipboard_path).expect("clipboard contents");
        assert_eq!(clipboard, "alpha");
    }

    clear_test_clipboard_file();
}

#[cfg(unix)]
#[test]
fn drag_selection_copies_visible_plain_text_on_mouse_up() {
    let _guard = launcher_env_lock();
    let temp = tempdir().expect("tempdir");
    let clipboard_path = temp.path().join("clipboard.txt");
    set_test_clipboard_file(&clipboard_path);

    let (_temp, mut app) = test_app();
    app.set_result_for_test("alpha beta\ngamma delta", "");
    app.set_active_result_tab(ResultTab::Stdout);
    drag_select_visible_text(&mut app, 120, 28, OutputPane::Result, "alpha");

    let clipboard = std::fs::read_to_string(&clipboard_path).expect("clipboard contents");
    assert_eq!(clipboard, "alpha");
    assert!(
        app.status_line_for_test()
            .contains("Copied selection to clipboard.")
    );
    clear_test_clipboard_file();
}

#[cfg(unix)]
#[test]
fn output_selection_copy_works_for_structured_table_rows() {
    let _guard = launcher_env_lock();
    let temp = tempdir().expect("tempdir");
    let clipboard_path = temp.path().join("clipboard.txt");
    set_test_clipboard_file(&clipboard_path);

    let ms_path = create_fixture_ms(temp.path());
    let schema = listobs_app()
        .load_schema()
        .expect("load schema from listobs");
    let config = ConfigStore::load_for_tests(temp.path().join("casars.toml"));
    let mut app = AppState::from_schema_with_config(listobs_app(), schema, config);
    app.set_text_value("ms_path", ms_path.to_string_lossy().as_ref());
    app.start_run_for_test();
    assert!(app.wait_for_idle_for_test(Duration::from_secs(60)));
    app.set_active_result_tab(ResultTab::Fields);

    drag_select_visible_text(&mut app, 140, 32, OutputPane::Result, "3C286");
    app.handle_key_event(KeyEvent::new(KeyCode::Char('y'), KeyModifiers::NONE));

    let clipboard = std::fs::read_to_string(&clipboard_path).expect("clipboard contents");
    assert_eq!(clipboard, "3C286");
    clear_test_clipboard_file();
}

#[cfg(unix)]
#[test]
fn drag_selection_copies_browser_inspector_text_on_mouse_up() {
    let _guard = launcher_env_lock();
    let temp = tempdir().expect("tempdir");
    let clipboard_path = temp.path().join("clipboard.txt");
    set_test_clipboard_file(&clipboard_path);
    let script = write_fake_tablebrowser_script(
        temp.path(),
        &[
            fake_browser_snapshot_with_inspector_json(
                ProtocolBrowserView::Cells,
                "Fake cells",
                vec![
                    "Cells  row=1/10  col=1/3  focus=Main".to_string(),
                    "row | NAME<str> |".to_string(),
                    "   0 | >\"alpha\"< |".to_string(),
                ],
                Some(BrowserInspectorSnapshot {
                    title: "Cell row=0 column=NAME".to_string(),
                    trail: vec![BrowserInspectorTrailEntry {
                        label: "root".to_string(),
                        summary: "scalar".to_string(),
                    }],
                    node: BrowserValueNode::Scalar {
                        value: BrowserScalarValue::String("alpha".to_string()),
                    },
                    rendered_lines: vec!["scalar: alpha beta".to_string()],
                }),
            ),
            fake_browser_snapshot_with_inspector_json(
                ProtocolBrowserView::Cells,
                "Fake cells inspector focus",
                vec![
                    "Cells  row=1/10  col=1/3  focus=Inspector".to_string(),
                    "row | NAME<str> |".to_string(),
                    "   0 | >\"alpha\"< |".to_string(),
                ],
                Some(BrowserInspectorSnapshot {
                    title: "Cell row=0 column=NAME".to_string(),
                    trail: vec![BrowserInspectorTrailEntry {
                        label: "root".to_string(),
                        summary: "scalar".to_string(),
                    }],
                    node: BrowserValueNode::Scalar {
                        value: BrowserScalarValue::String("alpha".to_string()),
                    },
                    rendered_lines: vec!["scalar: alpha beta".to_string()],
                }),
            ),
        ],
        None,
    );
    set_tablebrowser_launcher_bin(&script);

    let schema = tablebrowser_app()
        .load_schema()
        .expect("load fake tablebrowser schema");
    let config = ConfigStore::load_for_tests(temp.path().join("casars.toml"));
    let mut app = AppState::from_schema_with_config(tablebrowser_app(), schema, config);
    app.set_text_value("table_path", "/tmp/fake.ms");
    app.start_run_for_test();

    drag_select_visible_text(&mut app, 160, 28, OutputPane::LeftOutput, "alpha beta");

    let clipboard = std::fs::read_to_string(&clipboard_path).expect("clipboard contents");
    assert_eq!(clipboard, "alpha beta");
    assert!(
        app.status_line_for_test()
            .contains("Copied selection to clipboard.")
    );
    clear_test_clipboard_file();
}

#[cfg(unix)]
#[test]
fn browser_result_selection_copies_visible_text_in_every_view() {
    let _guard = launcher_env_lock();
    let temp = tempdir().expect("tempdir");
    let clipboard_path = temp.path().join("clipboard.txt");
    set_test_clipboard_file(&clipboard_path);
    let script = write_fake_tablebrowser_script(
        temp.path(),
        &[
            fake_browser_snapshot_json(
                ProtocolBrowserView::Overview,
                "Fake overview",
                vec!["Overview root".to_string(), "token-overview".to_string()],
            ),
            fake_browser_snapshot_json(
                ProtocolBrowserView::Columns,
                "Fake columns",
                vec![
                    "Columns  selected=1/3  focus=Main".to_string(),
                    "token-columns".to_string(),
                ],
            ),
            fake_browser_snapshot_json(
                ProtocolBrowserView::Keywords,
                "Fake keywords",
                vec![
                    "Keywords  selected=1/3  focus=Main".to_string(),
                    "token-keywords".to_string(),
                ],
            ),
            fake_browser_snapshot_json(
                ProtocolBrowserView::Cells,
                "Fake cells",
                vec![
                    "Cells  row=1/10  col=1/3  focus=Main".to_string(),
                    "row | NAME<str> |".to_string(),
                    "   0 | >token-cells< |".to_string(),
                ],
            ),
            fake_browser_snapshot_json(
                ProtocolBrowserView::Subtables,
                "Fake subtables",
                vec![
                    "Subtables  selected=1/3  focus=Main".to_string(),
                    "token-subtables".to_string(),
                ],
            ),
        ],
        None,
    );
    set_tablebrowser_launcher_bin(&script);

    let schema = tablebrowser_app()
        .load_schema()
        .expect("load fake tablebrowser schema");
    let config = ConfigStore::load_for_tests(temp.path().join("casars.toml"));
    let mut app = AppState::from_schema_with_config(tablebrowser_app(), schema, config);
    app.set_text_value("table_path", "/tmp/fake.ms");
    app.start_run_for_test();

    for (index, expected) in [
        "token-overview",
        "token-columns",
        "token-keywords",
        "token-cells",
        "token-subtables",
    ]
    .into_iter()
    .enumerate()
    {
        if index > 0 {
            app.handle_key_event(KeyEvent::new(KeyCode::Char(']'), KeyModifiers::NONE));
        }
        drag_select_visible_text(&mut app, 160, 28, OutputPane::Result, expected);
        app.handle_key_event(KeyEvent::new(KeyCode::Char('y'), KeyModifiers::NONE));
        let clipboard = std::fs::read_to_string(&clipboard_path).expect("clipboard contents");
        assert_eq!(clipboard, expected);
    }

    clear_test_clipboard_file();
}

#[cfg(unix)]
#[test]
fn browser_inspector_selection_copies_visible_text() {
    let _guard = launcher_env_lock();
    let temp = tempdir().expect("tempdir");
    let clipboard_path = temp.path().join("clipboard.txt");
    set_test_clipboard_file(&clipboard_path);
    let script = write_fake_tablebrowser_script(
        temp.path(),
        &[
            fake_browser_snapshot_with_inspector_json(
                ProtocolBrowserView::Cells,
                "Fake cells",
                vec![
                    "Cells  row=1/10  col=1/3  focus=Main".to_string(),
                    "row | NAME<str> |".to_string(),
                    "   0 | >\"alpha\"< |".to_string(),
                ],
                Some(BrowserInspectorSnapshot {
                    title: "Cell row=0 column=NAME".to_string(),
                    trail: vec![BrowserInspectorTrailEntry {
                        label: "root".to_string(),
                        summary: "scalar".to_string(),
                    }],
                    node: BrowserValueNode::Scalar {
                        value: BrowserScalarValue::String("alpha".to_string()),
                    },
                    rendered_lines: vec!["scalar: alpha beta".to_string()],
                }),
            ),
            fake_browser_snapshot_with_inspector_json(
                ProtocolBrowserView::Cells,
                "Fake cells inspector focus",
                vec![
                    "Cells  row=1/10  col=1/3  focus=Inspector".to_string(),
                    "row | NAME<str> |".to_string(),
                    "   0 | >\"alpha\"< |".to_string(),
                ],
                Some(BrowserInspectorSnapshot {
                    title: "Cell row=0 column=NAME".to_string(),
                    trail: vec![BrowserInspectorTrailEntry {
                        label: "root".to_string(),
                        summary: "scalar".to_string(),
                    }],
                    node: BrowserValueNode::Scalar {
                        value: BrowserScalarValue::String("alpha".to_string()),
                    },
                    rendered_lines: vec!["scalar: alpha beta".to_string()],
                }),
            ),
        ],
        None,
    );
    set_tablebrowser_launcher_bin(&script);

    let schema = tablebrowser_app()
        .load_schema()
        .expect("load fake tablebrowser schema");
    let config = ConfigStore::load_for_tests(temp.path().join("casars.toml"));
    let mut app = AppState::from_schema_with_config(tablebrowser_app(), schema, config);
    app.set_text_value("table_path", "/tmp/fake.ms");
    app.start_run_for_test();

    drag_select_visible_text(&mut app, 160, 28, OutputPane::LeftOutput, "alpha beta");
    app.handle_key_event(KeyEvent::new(KeyCode::Char('y'), KeyModifiers::NONE));

    let clipboard = std::fs::read_to_string(&clipboard_path).expect("clipboard contents");
    assert_eq!(clipboard, "alpha beta");
    clear_test_clipboard_file();
}

#[cfg(unix)]
#[test]
fn non_browser_copy_without_selection_reports_nothing_selected() {
    let _guard = launcher_env_lock();
    let temp = tempdir().expect("tempdir");
    let clipboard_path = temp.path().join("clipboard.txt");
    set_test_clipboard_file(&clipboard_path);

    let (_temp, mut app) = test_app();
    app.set_result_for_test("alpha beta", "");
    app.set_active_result_tab(ResultTab::Stdout);
    app.handle_key_event(KeyEvent::new(KeyCode::Char('y'), KeyModifiers::NONE));

    assert!(
        app.status_line_for_test()
            .contains("Nothing copyable is selected")
    );
    assert!(!clipboard_path.exists());
    clear_test_clipboard_file();
}

#[test]
fn escape_clears_active_output_selection() {
    let (_temp, mut app) = test_app();
    app.set_result_for_test("alpha beta", "");
    app.set_active_result_tab(ResultTab::Stdout);
    drag_select_visible_text(&mut app, 120, 28, OutputPane::Result, "alpha");

    assert!(app.output_selection_rect(OutputPane::Result).is_some());
    app.handle_key_event(KeyEvent::new(KeyCode::Esc, KeyModifiers::NONE));
    assert!(app.output_selection_rect(OutputPane::Result).is_none());
}

#[test]
fn result_tab_switch_clears_active_output_selection() {
    let (_temp, mut app) = test_app();
    app.set_result_for_test("alpha beta", "stderr line");
    app.set_active_result_tab(ResultTab::Stdout);
    drag_select_visible_text(&mut app, 120, 28, OutputPane::Result, "alpha");

    assert!(app.output_selection_rect(OutputPane::Result).is_some());
    app.set_active_result_tab(ResultTab::Stderr);
    assert!(app.output_selection_rect(OutputPane::Result).is_none());
}

#[cfg(unix)]
#[test]
fn browser_view_change_clears_active_output_selection() {
    let _guard = launcher_env_lock();
    let temp = tempdir().expect("tempdir");
    let script = write_fake_tablebrowser_script(
        temp.path(),
        &[
            fake_browser_snapshot_json(
                ProtocolBrowserView::Overview,
                "Fake overview",
                vec!["Overview root".to_string(), "token-overview".to_string()],
            ),
            fake_browser_snapshot_json(
                ProtocolBrowserView::Columns,
                "Fake columns",
                vec![
                    "Columns  selected=1/3  focus=Main".to_string(),
                    "token-columns".to_string(),
                ],
            ),
        ],
        None,
    );
    set_tablebrowser_launcher_bin(&script);

    let schema = tablebrowser_app()
        .load_schema()
        .expect("load fake tablebrowser schema");
    let config = ConfigStore::load_for_tests(temp.path().join("casars.toml"));
    let mut app = AppState::from_schema_with_config(tablebrowser_app(), schema, config);
    app.set_text_value("table_path", "/tmp/fake.ms");
    app.start_run_for_test();

    drag_select_visible_text(&mut app, 160, 28, OutputPane::Result, "token-overview");
    assert!(app.output_selection_rect(OutputPane::Result).is_some());

    app.handle_key_event(KeyEvent::new(KeyCode::Char(']'), KeyModifiers::NONE));
    assert!(app.output_selection_rect(OutputPane::Result).is_none());
}

#[test]
fn pane_toggle_clears_active_output_selection() {
    let (_temp, mut app) = test_app();
    app.set_result_for_test("alpha beta", "");
    app.set_active_result_tab(ResultTab::Stdout);
    drag_select_visible_text(&mut app, 120, 28, OutputPane::Result, "alpha");

    assert!(app.output_selection_rect(OutputPane::Result).is_some());
    app.handle_key_event(KeyEvent::new(KeyCode::Char('p'), KeyModifiers::NONE));
    assert!(app.output_selection_rect(OutputPane::Result).is_none());
}

#[test]
fn divider_and_scrollbar_drag_do_not_create_output_selection() {
    let (_temp, mut app) = test_app();
    let stdout = (0..80)
        .map(|index| format!("line-{index:02}"))
        .collect::<Vec<_>>()
        .join("\n");
    app.set_result_for_test(&stdout, "");
    app.set_active_result_tab(ResultTab::Stdout);

    let layout = ui::compute_layout(ratatui::layout::Rect::new(0, 0, 120, 28), &app);
    app.handle_mouse_event(
        mouse(
            MouseEventKind::Down(MouseButton::Left),
            layout.divider.x,
            layout.divider.y + 2,
        ),
        &layout,
    );
    app.handle_mouse_event(
        mouse(
            MouseEventKind::Drag(MouseButton::Left),
            layout.body.x + 70,
            layout.divider.y + 2,
        ),
        &layout,
    );
    app.handle_mouse_event(
        mouse(
            MouseEventKind::Up(MouseButton::Left),
            layout.body.x + 70,
            layout.divider.y + 2,
        ),
        &layout,
    );
    assert!(app.output_selection_rect(OutputPane::Result).is_none());

    let layout = ui::compute_layout(ratatui::layout::Rect::new(0, 0, 120, 28), &app);
    let scrollbar = layout.result_scrollbar.expect("result scrollbar");
    app.handle_mouse_event(
        mouse(
            MouseEventKind::Down(MouseButton::Left),
            scrollbar.x,
            scrollbar.y + 1,
        ),
        &layout,
    );
    app.handle_mouse_event(
        mouse(
            MouseEventKind::Drag(MouseButton::Left),
            scrollbar.x,
            scrollbar.y + 4,
        ),
        &layout,
    );
    app.handle_mouse_event(
        mouse(
            MouseEventKind::Up(MouseButton::Left),
            scrollbar.x,
            scrollbar.y + 4,
        ),
        &layout,
    );
    assert!(app.output_selection_rect(OutputPane::Result).is_none());
}

#[cfg(unix)]
#[test]
fn fake_tablebrowser_session_drives_casars_navigation() {
    let _guard = launcher_env_lock();
    let temp = tempdir().expect("tempdir");
    let script = write_fake_tablebrowser_script(
        temp.path(),
        &[
            fake_browser_snapshot_json(
                ProtocolBrowserView::Overview,
                "Fake overview",
                vec!["Overview root".to_string(), "alpha".to_string()],
            ),
            fake_browser_snapshot_json(
                ProtocolBrowserView::Cells,
                "Fake cells",
                vec!["Cells".to_string(), "\"alpha\"".to_string()],
            ),
            fake_browser_snapshot_json(
                ProtocolBrowserView::Cells,
                "Fake moved cells",
                vec!["Cells".to_string(), "\"beta\"".to_string()],
            ),
            fake_browser_snapshot_json(
                ProtocolBrowserView::Subtables,
                "Fake child table",
                vec!["Subtables".to_string(), "child.tab".to_string()],
            ),
            fake_browser_snapshot_json(
                ProtocolBrowserView::Overview,
                "Fake back",
                vec!["Overview root".to_string(), "returned".to_string()],
            ),
        ],
        None,
    );
    set_tablebrowser_launcher_bin(&script);

    let schema = tablebrowser_app()
        .load_schema()
        .expect("load fake tablebrowser schema");
    let config = ConfigStore::load_for_tests(temp.path().join("casars.toml"));
    let mut app = AppState::from_schema_with_config(tablebrowser_app(), schema, config);
    app.set_text_value("table_path", "/tmp/fake.ms");
    app.start_run_for_test();

    let overview = render_app(&app, 160, 28);
    assert!(overview.contains("Overview root"));

    app.handle_key_event(KeyEvent::new(KeyCode::Char(']'), KeyModifiers::NONE));
    let cells = render_app(&app, 160, 28);
    assert!(cells.contains("\"alpha\""));

    app.handle_key_event(KeyEvent::new(KeyCode::Down, KeyModifiers::NONE));
    let moved = render_app(&app, 160, 28);
    assert!(moved.contains("\"beta\""));

    app.handle_key_event(KeyEvent::new(KeyCode::Enter, KeyModifiers::NONE));
    let child = render_app(&app, 160, 28);
    assert!(child.contains("child.tab"));

    app.handle_key_event(KeyEvent::new(KeyCode::Backspace, KeyModifiers::NONE));
    let back = render_app(&app, 160, 28);
    assert!(back.contains("returned"));
}

#[cfg(unix)]
#[test]
fn imexplore_session_starts_from_image_path_and_prepares_raster_plane_view() {
    let _guard = launcher_env_lock();
    let temp = tempdir().expect("tempdir");
    let script = write_fake_imexplore_script(
        temp.path(),
        &[fake_imexplore_snapshot_json(
            ProtocolImageView::Plane,
            ProtocolImageFocus::Content,
            "Image ready",
            vec!["..@.".to_string(), ".##.".to_string()],
            vec![
                "View: Plane".to_string(),
                "Shape: [4, 4, 3]".to_string(),
                "Hidden axis Frequency (2): 0/2".to_string(),
                "Value: 42".to_string(),
            ],
            Some(ImageBrowserProbe {
                pixel_indices: vec![2, 0, 0],
                pixel_axes: vec![],
                value: 42.0,
                masked: false,
                finite: true,
                world_axes: vec![ImageBrowserAxisValue {
                    name: "Frequency".to_string(),
                    unit: "Hz".to_string(),
                    value: 1.42040575e9,
                }],
            }),
            Some(ImageNonDisplayAxisState {
                axis: 2,
                label: "Frequency".to_string(),
                index: 0,
                length: 3,
                pixel: 0,
            }),
        )],
        None,
    );
    set_imexplore_launcher_bin(&script);

    let schema = imexplore_app()
        .load_schema()
        .expect("load fake imexplore schema");
    let config = ConfigStore::load_for_tests(temp.path().join("casars.toml"));
    let mut app = AppState::from_schema_with_config(imexplore_app(), schema, config);
    app.set_text_value("image_path", "/tmp/fake.image");
    app.start_run_for_test();

    let rendered = render_app(&app, 160, 28);
    assert!(app.browser_is_active());
    assert_eq!(app.active_browser_tab_label(), Some("Plane"));
    assert_eq!(app.image_plane_mode_label_for_test(), Some("raster"));
    assert!(rendered.contains("Image ready"));
    assert!(rendered.contains("Hidden axis Frequency (2): 0/2"));
    assert!(rendered.contains("Plane mode: raster"));

    app.prepare_graphics_for_test(160, 28);
    let deadline = Instant::now() + Duration::from_secs(1);
    while app.image_plane_image_size_for_test().is_none() && Instant::now() < deadline {
        app.on_tick();
        std::thread::sleep(Duration::from_millis(10));
    }
    let layout = ui::compute_layout(ratatui::layout::Rect::new(0, 0, 160, 28), &app);
    let canvas = ui::image_plane_canvas_area(&layout);
    assert!(
        app.image_plane_protocol().is_some()
            || app.image_plane_pending()
            || app.image_plane_last_error().is_some()
    );
    let image_size = app
        .image_plane_image_size_for_test()
        .expect("rendered raster image size");
    assert!(image_size.0 > u32::from(canvas.width));
    assert!(image_size.1 > u32::from(canvas.height));
}

#[cfg(unix)]
#[test]
fn imexplore_plane_view_prepares_linked_spectrum_plot() {
    let _guard = launcher_env_lock();
    let temp = tempdir().expect("tempdir");
    let script = write_fake_imexplore_script(
        temp.path(),
        &[fake_imexplore_snapshot_json_with_profile(
            ProtocolImageView::Plane,
            ProtocolImageFocus::Content,
            "Image ready",
            vec!["raster".to_string()],
            vec![
                "View: Plane".to_string(),
                "Shape: [4, 4, 3]".to_string(),
                "Value: 5".to_string(),
            ],
            Some(ImageBrowserProbe {
                pixel_indices: vec![1, 1, 1],
                pixel_axes: vec![],
                value: 5.0,
                masked: false,
                finite: true,
                world_axes: vec![ImageBrowserAxisValue {
                    name: "Frequency".to_string(),
                    unit: "Hz".to_string(),
                    value: 1.150_230_333_39e11,
                }],
            }),
            Some(fake_image_profile_payload()),
            Some(ImageNonDisplayAxisState {
                axis: 2,
                label: "Frequency".to_string(),
                index: 1,
                length: 3,
                pixel: 1,
            }),
            image_parameters("0,0,0", "3,3,2", "1,1,1"),
        )],
        None,
    );
    set_imexplore_launcher_bin(&script);

    let schema = imexplore_app()
        .load_schema()
        .expect("load fake imexplore schema");
    let config = ConfigStore::load_for_tests(temp.path().join("casars.toml"));
    let mut app = AppState::from_schema_with_config(imexplore_app(), schema, config);
    app.set_text_value("image_path", "/tmp/fake.image");
    app.start_run_for_test();
    app.prepare_graphics_for_test(160, 32);

    let deadline = Instant::now() + Duration::from_secs(1);
    while (app.image_plane_image_size_for_test().is_none()
        || app.image_spectrum_image_size_for_test().is_none())
        && Instant::now() < deadline
    {
        app.on_tick();
        std::thread::sleep(Duration::from_millis(10));
    }

    assert!(app.image_plane_image_size_for_test().is_some());
    assert!(app.image_spectrum_image_size_for_test().is_some());
}

#[cfg(unix)]
#[test]
fn imexplore_uses_live_parameters_pane() {
    let _guard = launcher_env_lock();
    let temp = tempdir().expect("tempdir");
    let script = write_fake_imexplore_script(
        temp.path(),
        &[fake_imexplore_snapshot_json(
            ProtocolImageView::Plane,
            ProtocolImageFocus::Content,
            "Image ready",
            vec!["..@.".to_string(), ".##.".to_string()],
            vec![
                "View: Plane".to_string(),
                "Shape: [4, 4, 3]".to_string(),
                "Hidden axis Frequency (2): 0/2".to_string(),
                "Value: 42".to_string(),
            ],
            Some(ImageBrowserProbe {
                pixel_indices: vec![2, 0, 0],
                pixel_axes: vec![],
                value: 42.0,
                masked: false,
                finite: true,
                world_axes: vec![],
            }),
            Some(ImageNonDisplayAxisState {
                axis: 2,
                label: "Frequency".to_string(),
                index: 0,
                length: 3,
                pixel: 0,
            }),
        )],
        None,
    );
    set_imexplore_launcher_bin(&script);

    let schema = imexplore_app()
        .load_schema()
        .expect("load fake imexplore schema");
    let config = ConfigStore::load_for_tests(temp.path().join("casars.toml"));
    let mut app = AppState::from_schema_with_config(imexplore_app(), schema, config);
    app.set_text_value("image_path", "/tmp/fake.image");
    app.start_run_for_test();

    let rendered = render_app(&app, 160, 28);
    assert!(rendered.contains("Parameters [live]"));
    assert!(rendered.contains("Image Path"));
    assert!(rendered.contains("Hidden axis Frequency (2): 0/2"));

    app.handle_key_event(KeyEvent::new(KeyCode::Tab, KeyModifiers::NONE));
    assert_eq!(app.pane_focus_for_test(), PaneFocus::Parameters);
    app.handle_key_event(KeyEvent::new(KeyCode::Char('o'), KeyModifiers::CONTROL));
    assert!(app.path_chooser_active());
}

#[cfg(unix)]
#[test]
fn imexplore_defers_backend_resize_while_dragging_divider() {
    let _guard = launcher_env_lock();
    let temp = tempdir().expect("tempdir");
    let script = write_fake_imexplore_script(
        temp.path(),
        &[
            fake_imexplore_snapshot_json(
                ProtocolImageView::Plane,
                ProtocolImageFocus::Content,
                "Image ready",
                vec!["raster".to_string()],
                vec!["View: Plane".to_string()],
                Some(ImageBrowserProbe {
                    pixel_indices: vec![0, 0],
                    pixel_axes: vec![],
                    value: 1.0,
                    masked: false,
                    finite: true,
                    world_axes: vec![],
                }),
                None,
            ),
            fake_imexplore_snapshot_json(
                ProtocolImageView::Plane,
                ProtocolImageFocus::Content,
                "Resized",
                vec!["raster".to_string()],
                vec!["View: Plane".to_string()],
                Some(ImageBrowserProbe {
                    pixel_indices: vec![0, 0],
                    pixel_axes: vec![],
                    value: 2.0,
                    masked: false,
                    finite: true,
                    world_axes: vec![],
                }),
                None,
            ),
        ],
        None,
    );
    set_imexplore_launcher_bin(&script);

    let schema = imexplore_app()
        .load_schema()
        .expect("load fake imexplore schema");
    let config = ConfigStore::load_for_tests(temp.path().join("casars.toml"));
    let mut app = AppState::from_schema_with_config(imexplore_app(), schema, config);
    app.set_text_value("image_path", "/tmp/fake.image");
    app.start_run_for_test();

    let layout = ui::compute_layout(ratatui::layout::Rect::new(0, 0, 140, 30), &app);
    let divider_row = layout.divider.y.saturating_add(1);
    app.handle_mouse_event(
        mouse(
            MouseEventKind::Down(MouseButton::Left),
            layout.divider.x,
            divider_row,
        ),
        &layout,
    );
    app.handle_mouse_event(
        mouse(
            MouseEventKind::Drag(MouseButton::Left),
            layout.divider.x.saturating_add(8),
            divider_row,
        ),
        &layout,
    );

    app.sync_browser_viewport(80, 20, 10);
    assert_eq!(
        app.image_browser_snapshot_for_test()
            .expect("imexplore snapshot")
            .status_line,
        "Image ready"
    );

    app.handle_mouse_event(
        mouse(
            MouseEventKind::Up(MouseButton::Left),
            layout.divider.x.saturating_add(8),
            divider_row,
        ),
        &layout,
    );
    app.sync_browser_viewport(80, 20, 10);
    assert_eq!(
        app.image_browser_snapshot_for_test()
            .expect("imexplore snapshot")
            .status_line,
        "Resized"
    );
}

#[cfg(unix)]
#[test]
fn imexplore_workspace_split_ratio_persists_after_drag() {
    let _guard = launcher_env_lock();
    let temp = tempdir().expect("tempdir");
    let script = write_fake_imexplore_script(
        temp.path(),
        &[fake_imexplore_snapshot_json_with_profile(
            ProtocolImageView::Plane,
            ProtocolImageFocus::Content,
            "Image ready",
            vec!["raster".to_string()],
            vec!["View: Plane".to_string()],
            Some(ImageBrowserProbe {
                pixel_indices: vec![0, 0, 0],
                pixel_axes: vec![],
                value: 1.0,
                masked: false,
                finite: true,
                world_axes: vec![],
            }),
            Some(fake_image_profile_payload()),
            Some(ImageNonDisplayAxisState {
                axis: 2,
                label: "Frequency".to_string(),
                index: 0,
                length: 3,
                pixel: 0,
            }),
            image_parameters("0,0,0", "3,3,2", "1,1,1"),
        )],
        None,
    );
    set_imexplore_launcher_bin(&script);

    let config_path = temp.path().join("casars.toml");
    let schema = imexplore_app()
        .load_schema()
        .expect("load fake imexplore schema");
    let mut app = AppState::from_schema_with_config(
        imexplore_app(),
        schema,
        ConfigStore::load_for_tests(config_path.clone()),
    );
    app.set_text_value("image_path", "/tmp/fake.image");
    app.start_run_for_test();

    let original = app.image_workspace_split_ratio_for_test();
    let layout = ui::compute_layout(ratatui::layout::Rect::new(0, 0, 140, 36), &app);
    let divider =
        ui::image_workspace_divider_area(&layout, true, original).expect("linked divider area");
    let target_row = divider
        .y
        .saturating_add(3)
        .min(layout.result_content.y + layout.result_content.height.saturating_sub(2));
    let target_col = divider.x.saturating_add(1);

    app.handle_mouse_event(
        mouse(
            MouseEventKind::Down(MouseButton::Left),
            target_col,
            divider.y,
        ),
        &layout,
    );
    app.handle_mouse_event(
        mouse(
            MouseEventKind::Drag(MouseButton::Left),
            target_col,
            target_row,
        ),
        &layout,
    );
    app.handle_mouse_event(
        mouse(
            MouseEventKind::Up(MouseButton::Left),
            target_col,
            target_row,
        ),
        &layout,
    );

    let resized = app.image_workspace_split_ratio_for_test();
    assert!(resized > original);

    let reloaded = AppState::from_schema_with_config(
        imexplore_app(),
        imexplore_app()
            .load_schema()
            .expect("reload fake imexplore schema"),
        ConfigStore::load_for_tests(config_path),
    );
    assert!((reloaded.image_workspace_split_ratio_for_test() - resized).abs() < f32::EPSILON);
}

#[cfg(unix)]
#[test]
fn imexplore_workspace_toggle_collapses_and_restores_spectrum() {
    let _guard = launcher_env_lock();
    let temp = tempdir().expect("tempdir");
    let script = write_fake_imexplore_script(
        temp.path(),
        &[fake_imexplore_snapshot_json_with_profile(
            ProtocolImageView::Plane,
            ProtocolImageFocus::Content,
            "Image ready",
            vec!["raster".to_string()],
            vec!["View: Plane".to_string()],
            Some(ImageBrowserProbe {
                pixel_indices: vec![0, 0, 0],
                pixel_axes: vec![],
                value: 1.0,
                masked: false,
                finite: true,
                world_axes: vec![],
            }),
            Some(fake_image_profile_payload()),
            Some(ImageNonDisplayAxisState {
                axis: 2,
                label: "Frequency".to_string(),
                index: 0,
                length: 3,
                pixel: 0,
            }),
            image_parameters("0,0,0", "3,3,2", "1,1,1"),
        )],
        None,
    );
    set_imexplore_launcher_bin(&script);

    let schema = imexplore_app()
        .load_schema()
        .expect("load fake imexplore schema");
    let config = ConfigStore::load_for_tests(temp.path().join("casars.toml"));
    let mut app = AppState::from_schema_with_config(imexplore_app(), schema, config);
    app.set_text_value("image_path", "/tmp/fake.image");
    app.start_run_for_test();

    let original = app.image_workspace_split_ratio_for_test();
    let layout = ui::compute_layout(ratatui::layout::Rect::new(0, 0, 140, 36), &app);
    let toggle = ui::image_workspace_divider_toggle_area(&layout, true, original)
        .expect("linked divider toggle");

    app.handle_mouse_event(
        mouse(
            MouseEventKind::Down(MouseButton::Left),
            toggle.x + 1,
            toggle.y,
        ),
        &layout,
    );
    app.handle_mouse_event(
        mouse(
            MouseEventKind::Up(MouseButton::Left),
            toggle.x + 1,
            toggle.y,
        ),
        &layout,
    );
    assert_eq!(app.image_workspace_split_ratio_for_test(), 1.0);

    let collapsed_layout = ui::compute_layout(ratatui::layout::Rect::new(0, 0, 140, 36), &app);
    assert!(
        ui::image_spectrum_canvas_area(
            &collapsed_layout,
            true,
            app.image_workspace_split_ratio_for_test(),
        )
        .is_none()
    );

    let toggle = ui::image_workspace_divider_toggle_area(
        &collapsed_layout,
        true,
        app.image_workspace_split_ratio_for_test(),
    )
    .expect("collapsed divider toggle");
    app.handle_mouse_event(
        mouse(
            MouseEventKind::Down(MouseButton::Left),
            toggle.x + 1,
            toggle.y,
        ),
        &collapsed_layout,
    );
    app.handle_mouse_event(
        mouse(
            MouseEventKind::Up(MouseButton::Left),
            toggle.x + 1,
            toggle.y,
        ),
        &collapsed_layout,
    );

    assert!((app.image_workspace_split_ratio_for_test() - original).abs() < f32::EPSILON);
}

#[cfg(unix)]
#[test]
fn imexplore_keyboard_toggle_collapses_and_restores_spectrum() {
    let _guard = launcher_env_lock();
    let temp = tempdir().expect("tempdir");
    let script = write_fake_imexplore_script(
        temp.path(),
        &[fake_imexplore_snapshot_json_with_profile(
            ProtocolImageView::Plane,
            ProtocolImageFocus::Content,
            "Image ready",
            vec!["raster".to_string()],
            vec!["View: Plane".to_string()],
            Some(ImageBrowserProbe {
                pixel_indices: vec![0, 0, 0],
                pixel_axes: vec![],
                value: 1.0,
                masked: false,
                finite: true,
                world_axes: vec![],
            }),
            Some(fake_image_profile_payload()),
            Some(ImageNonDisplayAxisState {
                axis: 2,
                label: "Frequency".to_string(),
                index: 0,
                length: 3,
                pixel: 0,
            }),
            image_parameters("0,0,0", "3,3,2", "1,1,1"),
        )],
        None,
    );
    set_imexplore_launcher_bin(&script);

    let schema = imexplore_app()
        .load_schema()
        .expect("load fake imexplore schema");
    let config = ConfigStore::load_for_tests(temp.path().join("casars.toml"));
    let mut app = AppState::from_schema_with_config(imexplore_app(), schema, config);
    app.set_text_value("image_path", "/tmp/fake.image");
    app.start_run_for_test();

    let original = app.image_workspace_split_ratio_for_test();
    app.handle_key_event(KeyEvent::new(KeyCode::Char('s'), KeyModifiers::NONE));
    assert_eq!(app.image_workspace_split_ratio_for_test(), 1.0);

    app.handle_key_event(KeyEvent::new(KeyCode::Char('s'), KeyModifiers::NONE));
    assert!((app.image_workspace_split_ratio_for_test() - original).abs() < f32::EPSILON);
}

#[cfg(unix)]
#[test]
fn imexplore_live_window_parameters_update_plane_view() {
    let _guard = launcher_env_lock();
    let temp = tempdir().expect("tempdir");
    let startup = fake_imexplore_snapshot_json_with_parameters(
        ProtocolImageView::Plane,
        ProtocolImageFocus::Content,
        "Image ready",
        vec![
            "y/x |           0           1           2           3".to_string(),
            "  0 | [        10]          20          30          40".to_string(),
            "  1 |          11          21          31          41".to_string(),
        ],
        vec!["View: Plane".to_string(), "Value: 10".to_string()],
        Some(ImageBrowserProbe {
            pixel_indices: vec![0, 0],
            pixel_axes: vec![],
            value: 10.0,
            masked: false,
            finite: true,
            world_axes: vec![],
        }),
        None,
        image_parameters("0,0", "3,1", "1,1"),
    );
    let updated = fake_imexplore_snapshot_json_with_parameters(
        ProtocolImageView::Plane,
        ProtocolImageFocus::Content,
        "Window updated",
        vec![
            "y/x |           1           3".to_string(),
            "  0 | [        20]          40".to_string(),
            "  1 |          21          41".to_string(),
        ],
        vec!["View: Plane".to_string(), "Value: 20".to_string()],
        Some(ImageBrowserProbe {
            pixel_indices: vec![1, 0],
            pixel_axes: vec![],
            value: 20.0,
            masked: false,
            finite: true,
            world_axes: vec![],
        }),
        None,
        image_parameters("1,0", "3,1", "2,1"),
    );
    let script = write_fake_imexplore_script(temp.path(), &[startup, updated], None);
    set_imexplore_launcher_bin(&script);

    let schema = imexplore_app()
        .load_schema()
        .expect("load fake imexplore schema");
    let config = ConfigStore::load_for_tests(temp.path().join("casars.toml"));
    let mut app = AppState::from_schema_with_config(imexplore_app(), schema, config);
    app.set_text_value("image_path", "/tmp/fake.image");
    app.start_run_for_test();
    app.handle_key_event(KeyEvent::new(KeyCode::Char('g'), KeyModifiers::NONE));
    app.set_text_value("trc", "3,1");
    app.set_text_value("inc", "2,1");
    app.set_text_value_and_apply("blc", "1,0");

    let rendered = render_app(&app, 120, 24);
    assert!(rendered.contains("Window updated"));
    assert_eq!(app.field_text_for_test("blc").as_deref(), Some("1,0"));
    assert_eq!(app.field_text_for_test("trc").as_deref(), Some("3,1"));
    assert_eq!(app.field_text_for_test("inc").as_deref(), Some("2,1"));
    assert!(rendered.contains("y/x |           1           3"));
}

#[cfg(unix)]
#[test]
fn imexplore_invalid_live_window_parameters_keep_session_open() {
    let _guard = launcher_env_lock();
    let temp = tempdir().expect("tempdir");
    let startup = fake_imexplore_snapshot_json_with_parameters(
        ProtocolImageView::Plane,
        ProtocolImageFocus::Content,
        "Image ready",
        vec![
            "y/x |           0           1".to_string(),
            "  0 | [         1]           2".to_string(),
        ],
        vec!["View: Plane".to_string(), "Value: 1".to_string()],
        Some(ImageBrowserProbe {
            pixel_indices: vec![0, 0],
            pixel_axes: vec![],
            value: 1.0,
            masked: false,
            finite: true,
            world_axes: vec![],
        }),
        None,
        image_parameters("0,0", "1,0", "1,1"),
    );
    let error = serde_json::to_string(&ImageBrowserResponseEnvelope::error(
        "command_failed",
        "INC axis 0 must be >= 1",
    ))
    .expect("serialize imexplore error");
    let script = write_fake_imexplore_script(temp.path(), &[startup, error], None);
    set_imexplore_launcher_bin(&script);

    let schema = imexplore_app()
        .load_schema()
        .expect("load fake imexplore schema");
    let config = ConfigStore::load_for_tests(temp.path().join("casars.toml"));
    let mut app = AppState::from_schema_with_config(imexplore_app(), schema, config);
    app.set_text_value("image_path", "/tmp/fake.image");
    app.start_run_for_test();
    app.set_text_value_and_apply("inc", "0,1");

    assert!(app.browser_is_active());
    assert!(app.status_line_for_test().contains("command_failed"));
    assert!(
        app.status_line_for_test()
            .contains("INC axis 0 must be >= 1")
    );
    assert!(
        app.stderr_for_test()
            .contains("command_failed: INC axis 0 must be >= 1")
    );
}

#[cfg(unix)]
#[test]
fn imexplore_pane_toggle_and_chevron_work() {
    let _guard = launcher_env_lock();
    let temp = tempdir().expect("tempdir");
    let script = write_fake_imexplore_script(
        temp.path(),
        &[fake_imexplore_snapshot_json(
            ProtocolImageView::Metadata,
            ProtocolImageFocus::Content,
            "Image ready",
            vec!["Metadata line".to_string()],
            vec!["View: Metadata".to_string(), "Shape: [4, 4, 3]".to_string()],
            None,
            None,
        )],
        None,
    );
    set_imexplore_launcher_bin(&script);

    let schema = imexplore_app()
        .load_schema()
        .expect("load fake imexplore schema");
    let config = ConfigStore::load_for_tests(temp.path().join("casars.toml"));
    let mut app = AppState::from_schema_with_config(imexplore_app(), schema, config);
    app.set_text_value("image_path", "/tmp/fake.image");
    app.start_run_for_test();

    assert!(!app.parameters_pane_collapsed());

    app.handle_key_event(KeyEvent::new(KeyCode::Char('p'), KeyModifiers::NONE));
    let collapsed = ui::compute_layout(ratatui::layout::Rect::new(0, 0, 160, 28), &app);
    assert!(app.parameters_pane_collapsed());
    assert_eq!(collapsed.form_block.width, 0);

    app.handle_mouse_event(
        mouse(
            MouseEventKind::Down(MouseButton::Left),
            collapsed.divider.x,
            collapsed.divider.y,
        ),
        &collapsed,
    );

    let expanded = ui::compute_layout(ratatui::layout::Rect::new(0, 0, 160, 28), &app);
    assert!(!app.parameters_pane_collapsed());
    assert!(expanded.form_block.width > 0);
    assert!(expanded.result_block.width < collapsed.result_block.width);
}

#[cfg(unix)]
#[test]
fn imexplore_plane_selected_cell_uses_highlight_background() {
    let _guard = launcher_env_lock();
    let temp = tempdir().expect("tempdir");
    let script = write_fake_imexplore_script(
        temp.path(),
        &[fake_imexplore_snapshot_json(
            ProtocolImageView::Plane,
            ProtocolImageFocus::Content,
            "Image ready",
            vec![
                "y/x |       0       1".to_string(),
                "  0 |       1 [     2]".to_string(),
                "  1 |       3       4".to_string(),
            ],
            vec!["View: Plane".to_string(), "Value: 2".to_string()],
            Some(ImageBrowserProbe {
                pixel_indices: vec![1, 0],
                pixel_axes: vec![],
                value: 2.0,
                masked: false,
                finite: true,
                world_axes: vec![],
            }),
            None,
        )],
        None,
    );
    set_imexplore_launcher_bin(&script);

    let schema = imexplore_app()
        .load_schema()
        .expect("load fake imexplore schema");
    let config = ConfigStore::load_for_tests(temp.path().join("casars.toml"));
    let mut app = AppState::from_schema_with_config(imexplore_app(), schema, config);
    app.set_text_value("image_path", "/tmp/fake.image");
    app.start_run_for_test();
    app.handle_key_event(KeyEvent::new(KeyCode::Char('g'), KeyModifiers::NONE));

    let backend = TestBackend::new(80, 24);
    let mut terminal = Terminal::new(backend).expect("test terminal");
    terminal
        .draw(|frame| {
            let layout = ui::compute_layout(frame.area(), &app);
            ui::draw(frame, &app, &layout);
        })
        .expect("draw app");

    let palette = theme(ThemeMode::DenseAnsi);
    let buffer = terminal.backend().buffer().clone();
    let highlighted_digit = (0..buffer.area.height).any(|y| {
        (0..buffer.area.width).any(|x| {
            let cell = &buffer[(x, y)];
            cell.symbol() == "2" && cell.bg == palette.field_selected_bg
        })
    });
    assert!(
        highlighted_digit,
        "expected selected plane value to be highlighted"
    );
}

#[cfg(unix)]
#[test]
fn imexplore_copy_shortcut_writes_probe_summary_to_clipboard() {
    let _guard = launcher_env_lock();
    let temp = tempdir().expect("tempdir");
    let clipboard_path = temp.path().join("clipboard.txt");
    set_test_clipboard_file(&clipboard_path);
    let script = write_fake_imexplore_script(
        temp.path(),
        &[fake_imexplore_snapshot_json(
            ProtocolImageView::Plane,
            ProtocolImageFocus::Content,
            "Image ready",
            vec!["@".to_string()],
            vec!["View: Plane".to_string(), "Value: 42".to_string()],
            Some(ImageBrowserProbe {
                pixel_indices: vec![0, 0],
                pixel_axes: vec![],
                value: 42.0,
                masked: false,
                finite: true,
                world_axes: vec![ImageBrowserAxisValue {
                    name: "Frequency".to_string(),
                    unit: "Hz".to_string(),
                    value: 1.42040575e9,
                }],
            }),
            None,
        )],
        None,
    );
    set_imexplore_launcher_bin(&script);

    let schema = imexplore_app()
        .load_schema()
        .expect("load fake imexplore schema");
    let config = ConfigStore::load_for_tests(temp.path().join("casars.toml"));
    let mut app = AppState::from_schema_with_config(imexplore_app(), schema, config);
    app.set_text_value("image_path", "/tmp/fake.image");
    app.start_run_for_test();
    app.handle_key_event(KeyEvent::new(KeyCode::Char('y'), KeyModifiers::NONE));

    let clipboard = std::fs::read_to_string(&clipboard_path).expect("clipboard contents");
    assert!(clipboard.contains("value: 42"));
    assert!(clipboard.contains("Frequency: 1.420406 GHz"));
    clear_test_clipboard_file();
}

#[cfg(unix)]
#[test]
fn imexplore_copy_formats_radec_probe_axes() {
    let _guard = launcher_env_lock();
    let temp = tempdir().expect("tempdir");
    let clipboard_path = temp.path().join("clipboard-radec.txt");
    set_test_clipboard_file(&clipboard_path);
    let script = write_fake_imexplore_script(
        temp.path(),
        &[fake_imexplore_snapshot_json(
            ProtocolImageView::Plane,
            ProtocolImageFocus::Content,
            "Image ready",
            vec!["@".to_string()],
            vec!["View: Plane".to_string(), "Value: 42".to_string()],
            Some(ImageBrowserProbe {
                pixel_indices: vec![0, 0],
                pixel_axes: vec![],
                value: 42.0,
                masked: false,
                finite: true,
                world_axes: vec![
                    ImageBrowserAxisValue {
                        name: "Right Ascension".to_string(),
                        unit: "rad".to_string(),
                        value: -0.25 * std::f64::consts::TAU,
                    },
                    ImageBrowserAxisValue {
                        name: "Declination".to_string(),
                        unit: "rad".to_string(),
                        value: (-12.5_f64).to_radians(),
                    },
                ],
            }),
            None,
        )],
        None,
    );
    set_imexplore_launcher_bin(&script);

    let schema = imexplore_app()
        .load_schema()
        .expect("load fake imexplore schema");
    let config = ConfigStore::load_for_tests(temp.path().join("casars.toml"));
    let mut app = AppState::from_schema_with_config(imexplore_app(), schema, config);
    app.set_text_value("image_path", "/tmp/fake.image");
    app.start_run_for_test();
    app.handle_key_event(KeyEvent::new(KeyCode::Char('y'), KeyModifiers::NONE));

    let clipboard = std::fs::read_to_string(&clipboard_path).expect("clipboard contents");
    assert!(clipboard.contains("Right Ascension: 18:00:00.000000 hms"));
    assert!(clipboard.contains("Declination: -12.30.00.00000 dms"));
    clear_test_clipboard_file();
}

#[cfg(unix)]
#[test]
fn imexplore_tab_focuses_live_parameters_pane() {
    let _guard = launcher_env_lock();
    let temp = tempdir().expect("tempdir");
    let script = write_fake_imexplore_script(
        temp.path(),
        &[
            fake_imexplore_snapshot_json(
                ProtocolImageView::Plane,
                ProtocolImageFocus::Content,
                "Image ready",
                vec!["..@.".to_string()],
                vec![
                    "View: Plane".to_string(),
                    "Hidden axis Frequency (2): 0/2".to_string(),
                ],
                Some(ImageBrowserProbe {
                    pixel_indices: vec![2, 0, 0],
                    pixel_axes: vec![],
                    value: 1.0,
                    masked: false,
                    finite: true,
                    world_axes: vec![],
                }),
                Some(ImageNonDisplayAxisState {
                    axis: 2,
                    label: "Frequency".to_string(),
                    index: 0,
                    length: 3,
                    pixel: 0,
                }),
            ),
            fake_imexplore_snapshot_json(
                ProtocolImageView::Plane,
                ProtocolImageFocus::Inspector,
                "Image ready",
                vec!["..@.".to_string()],
                vec![
                    "View: Plane".to_string(),
                    "Hidden axis Frequency (2): 0/2".to_string(),
                ],
                Some(ImageBrowserProbe {
                    pixel_indices: vec![2, 0, 0],
                    pixel_axes: vec![],
                    value: 1.0,
                    masked: false,
                    finite: true,
                    world_axes: vec![],
                }),
                Some(ImageNonDisplayAxisState {
                    axis: 2,
                    label: "Frequency".to_string(),
                    index: 0,
                    length: 3,
                    pixel: 0,
                }),
            ),
            fake_imexplore_snapshot_json(
                ProtocolImageView::Plane,
                ProtocolImageFocus::Inspector,
                "Image ready",
                vec!["..@.".to_string()],
                vec![
                    "View: Plane".to_string(),
                    "Hidden axis Frequency (2): 1/2".to_string(),
                ],
                Some(ImageBrowserProbe {
                    pixel_indices: vec![2, 0, 1],
                    pixel_axes: vec![],
                    value: 10.0,
                    masked: false,
                    finite: true,
                    world_axes: vec![],
                }),
                Some(ImageNonDisplayAxisState {
                    axis: 2,
                    label: "Frequency".to_string(),
                    index: 1,
                    length: 3,
                    pixel: 1,
                }),
            ),
        ],
        None,
    );
    set_imexplore_launcher_bin(&script);

    let schema = imexplore_app()
        .load_schema()
        .expect("load fake imexplore schema");
    let config = ConfigStore::load_for_tests(temp.path().join("casars.toml"));
    let mut app = AppState::from_schema_with_config(imexplore_app(), schema, config);
    app.set_text_value("image_path", "/tmp/fake.image");
    app.start_run_for_test();

    app.handle_key_event(KeyEvent::new(KeyCode::Tab, KeyModifiers::NONE));
    assert_eq!(app.pane_focus_for_test(), PaneFocus::Parameters);
    assert_eq!(app.browser_focus_for_test(), Some(BrowserPaneFocus::Main));
    assert!(
        app.selected_form_text_for_test()
            .is_some_and(|text| text.contains("Image Path"))
    );
    let rendered = render_app(&app, 160, 28);
    assert!(rendered.contains("Hidden axis Frequency (2): 0/2"));
}

#[cfg(unix)]
#[test]
fn imexplore_exposes_and_applies_horizontal_scrollbar() {
    let _guard = launcher_env_lock();
    let temp = tempdir().expect("tempdir");
    let long_line = "Cols: 00 01 02 03 04 05 06 07 08 09 10 11 12 13 14 15 RIGHT_EDGE".to_string();
    let snapshot = fake_imexplore_snapshot_json(
        ProtocolImageView::Metadata,
        ProtocolImageFocus::Content,
        "Image ready",
        vec![long_line],
        vec![
            "View: Metadata".to_string(),
            "Shape: [16, 16, 8]".to_string(),
        ],
        None,
        None,
    );
    let script = write_fake_imexplore_script(temp.path(), &[snapshot.clone(), snapshot], None);
    set_imexplore_launcher_bin(&script);

    let schema = imexplore_app()
        .load_schema()
        .expect("load fake imexplore schema");
    let config = ConfigStore::load_for_tests(temp.path().join("casars.toml"));
    let mut app = AppState::from_schema_with_config(imexplore_app(), schema, config);
    app.set_text_value("image_path", "/tmp/fake.image");
    app.start_run_for_test();

    let width = 56;
    let height = 24;
    let provisional_layout =
        ui::compute_layout(ratatui::layout::Rect::new(0, 0, width, height), &app);
    app.sync_browser_viewport(
        provisional_layout.result_content.width,
        provisional_layout.result_content.height,
        provisional_layout.form_inner.height,
    );
    let initial = render_app(&app, width, height);
    assert!(!initial.contains("RIGHT_EDGE"));

    let layout = ui::compute_layout(ratatui::layout::Rect::new(0, 0, width, height), &app);
    let _scrollbar = layout
        .result_hscrollbar
        .expect("image browser horizontal scrollbar");
    for _ in 0..40 {
        app.handle_mouse_event(
            mouse(
                MouseEventKind::ScrollRight,
                layout.result_content.x.saturating_add(1),
                layout.result_content.y,
            ),
            &layout,
        );
    }

    assert!(app.active_browser_hscroll() > 0);
    let scrolled = render_app(&app, width, height);
    assert!(scrolled.contains("RIGHT_EDGE"));
}

#[cfg(unix)]
#[test]
fn imexplore_auto_scrolls_plane_view_to_keep_selected_pixel_visible() {
    let _guard = launcher_env_lock();
    let temp = tempdir().expect("tempdir");
    let header = format!(
        "{:>3} | {:>11} {:>11} {:>11} {:>11} {:>11} {:>11} {:>11} {:>11} {:>11}",
        "y/x", 0, 1, 2, 3, 4, 5, 6, 7, 8
    );
    let row_initial = format!(
        "{:>3} | [{:>9}] {:>11} {:>11} {:>11} {:>11} {:>11} {:>11} {:>11} {:>11}",
        0, 1, 2, 3, 4, 5, 6, 7, 8, 888_888
    );
    let row_scrolled = format!(
        "{:>3} | {:>11} {:>11} {:>11} {:>11} {:>11} {:>11} {:>11} {:>11} [{:>9}]",
        0, 1, 2, 3, 4, 5, 6, 7, 8, 888_888
    );
    let script = write_fake_imexplore_script(
        temp.path(),
        &[
            fake_imexplore_snapshot_json(
                ProtocolImageView::Plane,
                ProtocolImageFocus::Content,
                "Image ready",
                vec![header.clone(), row_initial.clone()],
                vec!["View: Plane".to_string(), "Value: 1".to_string()],
                Some(ImageBrowserProbe {
                    pixel_indices: vec![0, 0],
                    pixel_axes: vec![],
                    value: 1.0,
                    masked: false,
                    finite: true,
                    world_axes: vec![],
                }),
                None,
            ),
            fake_imexplore_snapshot_json(
                ProtocolImageView::Plane,
                ProtocolImageFocus::Content,
                "Image ready",
                vec![header.clone(), row_initial.clone()],
                vec!["View: Plane".to_string(), "Value: 1".to_string()],
                Some(ImageBrowserProbe {
                    pixel_indices: vec![0, 0],
                    pixel_axes: vec![],
                    value: 1.0,
                    masked: false,
                    finite: true,
                    world_axes: vec![],
                }),
                None,
            ),
            fake_imexplore_snapshot_json(
                ProtocolImageView::Plane,
                ProtocolImageFocus::Content,
                "Image ready",
                vec![header.clone(), row_initial],
                vec!["View: Plane".to_string(), "Value: 1".to_string()],
                Some(ImageBrowserProbe {
                    pixel_indices: vec![0, 0],
                    pixel_axes: vec![],
                    value: 1.0,
                    masked: false,
                    finite: true,
                    world_axes: vec![],
                }),
                None,
            ),
            fake_imexplore_snapshot_json(
                ProtocolImageView::Plane,
                ProtocolImageFocus::Content,
                "Image ready",
                vec![header, row_scrolled],
                vec!["View: Plane".to_string(), "Value: 888888".to_string()],
                Some(ImageBrowserProbe {
                    pixel_indices: vec![8, 0],
                    pixel_axes: vec![],
                    value: 888_888.0,
                    masked: false,
                    finite: true,
                    world_axes: vec![],
                }),
                None,
            ),
        ],
        None,
    );
    set_imexplore_launcher_bin(&script);

    let schema = imexplore_app()
        .load_schema()
        .expect("load fake imexplore schema");
    let config = ConfigStore::load_for_tests(temp.path().join("casars.toml"));
    let mut app = AppState::from_schema_with_config(imexplore_app(), schema, config);
    app.set_text_value("image_path", "/tmp/fake.image");
    app.start_run_for_test();
    app.handle_key_event(KeyEvent::new(KeyCode::Char('g'), KeyModifiers::NONE));

    let width = 64;
    let height = 24;
    let provisional_layout =
        ui::compute_layout(ratatui::layout::Rect::new(0, 0, width, height), &app);
    app.sync_browser_viewport(
        provisional_layout.result_content.width,
        provisional_layout.result_content.height,
        provisional_layout.form_inner.height,
    );

    app.handle_key_event(KeyEvent::new(KeyCode::Right, KeyModifiers::NONE));

    assert!(app.active_browser_hscroll() > 0);
    let rendered = render_app(&app, width, height);
    assert!(rendered.contains("[   888888]"));
}

#[cfg(unix)]
#[test]
fn imexplore_clicking_plane_cell_moves_active_pixel() {
    let _guard = launcher_env_lock();
    let temp = tempdir().expect("tempdir");
    let header = format!("{:>3} | {:>11} {:>11}", "y/x", 0, 1);
    let row0_initial = format!("{:>3} | [{:>9}] {:>11}", 0, 1, 400);
    let row0_selected = format!("{:>3} | {:>11} [{:>9}]", 0, 1, 400);
    let row1 = format!("{:>3} | {:>11} {:>11}", 1, 3, 4);
    let script = write_fake_imexplore_script(
        temp.path(),
        &[
            fake_imexplore_snapshot_json(
                ProtocolImageView::Plane,
                ProtocolImageFocus::Content,
                "Image ready",
                vec![header.clone(), row0_initial, row1.clone()],
                vec!["View: Plane".to_string(), "Value: 1".to_string()],
                Some(ImageBrowserProbe {
                    pixel_indices: vec![0, 0],
                    pixel_axes: vec![],
                    value: 1.0,
                    masked: false,
                    finite: true,
                    world_axes: vec![],
                }),
                None,
            ),
            fake_imexplore_snapshot_json(
                ProtocolImageView::Plane,
                ProtocolImageFocus::Content,
                "Image ready",
                vec![header, row0_selected, row1],
                vec!["View: Plane".to_string(), "Value: 400".to_string()],
                Some(ImageBrowserProbe {
                    pixel_indices: vec![1, 0],
                    pixel_axes: vec![],
                    value: 400.0,
                    masked: false,
                    finite: true,
                    world_axes: vec![],
                }),
                None,
            ),
        ],
        None,
    );
    set_imexplore_launcher_bin(&script);

    let schema = imexplore_app()
        .load_schema()
        .expect("load fake imexplore schema");
    let config = ConfigStore::load_for_tests(temp.path().join("casars.toml"));
    let mut app = AppState::from_schema_with_config(imexplore_app(), schema, config);
    app.set_text_value("image_path", "/tmp/fake.image");
    app.start_run_for_test();
    app.handle_key_event(KeyEvent::new(KeyCode::Char('g'), KeyModifiers::NONE));

    let layout = ui::compute_layout(ratatui::layout::Rect::new(0, 0, 80, 24), &app);
    let buffer = app
        .visible_text_buffer(OutputPane::Result, &layout)
        .expect("visible text buffer");
    let (row, col) = buffer
        .lines
        .iter()
        .enumerate()
        .find_map(|(row, line)| {
            line.text
                .find("400")
                .map(|byte_index| (row, line.text[..byte_index].chars().count()))
        })
        .expect("plane value 400");

    app.handle_mouse_event(
        mouse(
            MouseEventKind::Down(MouseButton::Left),
            buffer.area.x + col as u16 + 1,
            buffer.area.y + row as u16,
        ),
        &layout,
    );

    let rendered = render_app(&app, 80, 24);
    assert!(rendered.contains("Value: 400"));
}

#[cfg(unix)]
#[test]
fn imexplore_clicking_raster_plane_moves_active_pixel() {
    let _guard = launcher_env_lock();
    let temp = tempdir().expect("tempdir");
    let script = write_fake_imexplore_script(
        temp.path(),
        &[
            fake_imexplore_snapshot_json_with_parameters(
                ProtocolImageView::Plane,
                ProtocolImageFocus::Content,
                "Image ready",
                vec!["raster".to_string()],
                vec!["View: Plane".to_string(), "Value: 1".to_string()],
                Some(ImageBrowserProbe {
                    pixel_indices: vec![0, 0],
                    pixel_axes: vec![],
                    value: 1.0,
                    masked: false,
                    finite: true,
                    world_axes: vec![],
                }),
                None,
                image_parameters("0,0", "3,3", "1,1"),
            ),
            fake_imexplore_snapshot_json_with_parameters(
                ProtocolImageView::Plane,
                ProtocolImageFocus::Content,
                "Image ready",
                vec!["raster".to_string()],
                vec!["View: Plane".to_string(), "Value: 12".to_string()],
                Some(ImageBrowserProbe {
                    pixel_indices: vec![2, 1],
                    pixel_axes: vec![],
                    value: 12.0,
                    masked: false,
                    finite: true,
                    world_axes: vec![],
                }),
                None,
                image_parameters("0,0", "3,3", "1,1"),
            ),
        ],
        None,
    );
    set_imexplore_launcher_bin(&script);

    let schema = imexplore_app()
        .load_schema()
        .expect("load fake imexplore schema");
    let config = ConfigStore::load_for_tests(temp.path().join("casars.toml"));
    let mut app = AppState::from_schema_with_config(imexplore_app(), schema, config);
    app.set_text_value("image_path", "/tmp/fake.image");
    app.start_run_for_test();
    app.prepare_graphics_for_test(80, 24);

    let layout = ui::compute_layout(ratatui::layout::Rect::new(0, 0, 80, 24), &app);
    let canvas = ui::image_plane_canvas_area(&layout);
    let draw_rect = image_plane_draw_rect(
        canvas,
        app.image_browser_snapshot_for_test()
            .expect("imexplore session snapshot"),
        app.image_plane_font_size_for_test(),
    )
    .expect("raster draw rect");
    app.handle_mouse_event(
        mouse(
            MouseEventKind::Down(MouseButton::Left),
            draw_rect.x + draw_rect.width / 2,
            draw_rect.y + draw_rect.height / 3,
        ),
        &layout,
    );

    let rendered = render_app(&app, 80, 24);
    assert!(rendered.contains("Value: 12"));
}

#[cfg(unix)]
#[test]
fn imexplore_dragging_raster_plane_updates_active_pixel() {
    let _guard = launcher_env_lock();
    let temp = tempdir().expect("tempdir");
    let script = write_fake_imexplore_script(
        temp.path(),
        &[
            fake_imexplore_snapshot_json_with_parameters(
                ProtocolImageView::Plane,
                ProtocolImageFocus::Content,
                "Image ready",
                vec!["raster".to_string()],
                vec!["View: Plane".to_string(), "Value: 1".to_string()],
                Some(ImageBrowserProbe {
                    pixel_indices: vec![0, 0],
                    pixel_axes: vec![],
                    value: 1.0,
                    masked: false,
                    finite: true,
                    world_axes: vec![],
                }),
                None,
                image_parameters("0,0", "3,3", "1,1"),
            ),
            fake_imexplore_snapshot_json_with_parameters(
                ProtocolImageView::Plane,
                ProtocolImageFocus::Content,
                "Image ready",
                vec!["raster".to_string()],
                vec!["View: Plane".to_string(), "Value: 4".to_string()],
                Some(ImageBrowserProbe {
                    pixel_indices: vec![1, 1],
                    pixel_axes: vec![],
                    value: 4.0,
                    masked: false,
                    finite: true,
                    world_axes: vec![],
                }),
                None,
                image_parameters("0,0", "3,3", "1,1"),
            ),
            fake_imexplore_snapshot_json_with_parameters(
                ProtocolImageView::Plane,
                ProtocolImageFocus::Content,
                "Image ready",
                vec!["raster".to_string()],
                vec!["View: Plane".to_string(), "Value: 16".to_string()],
                Some(ImageBrowserProbe {
                    pixel_indices: vec![3, 3],
                    pixel_axes: vec![],
                    value: 16.0,
                    masked: false,
                    finite: true,
                    world_axes: vec![],
                }),
                None,
                image_parameters("0,0", "3,3", "1,1"),
            ),
        ],
        None,
    );
    set_imexplore_launcher_bin(&script);

    let schema = imexplore_app()
        .load_schema()
        .expect("load fake imexplore schema");
    let config = ConfigStore::load_for_tests(temp.path().join("casars.toml"));
    let mut app = AppState::from_schema_with_config(imexplore_app(), schema, config);
    app.set_text_value("image_path", "/tmp/fake.image");
    app.start_run_for_test();
    app.prepare_graphics_for_test(80, 24);

    let layout = ui::compute_layout(ratatui::layout::Rect::new(0, 0, 80, 24), &app);
    let canvas = ui::image_plane_canvas_area(&layout);
    let draw_rect = image_plane_draw_rect(
        canvas,
        app.image_browser_snapshot_for_test()
            .expect("imexplore session snapshot"),
        app.image_plane_font_size_for_test(),
    )
    .expect("raster draw rect");
    app.handle_mouse_event(
        mouse(
            MouseEventKind::Down(MouseButton::Left),
            draw_rect.x + draw_rect.width / 3,
            draw_rect.y + draw_rect.height / 3,
        ),
        &layout,
    );
    app.handle_mouse_event(
        mouse(
            MouseEventKind::Drag(MouseButton::Left),
            draw_rect.x + draw_rect.width.saturating_sub(1),
            draw_rect.y + draw_rect.height.saturating_sub(1),
        ),
        &layout,
    );

    let rendered = render_app(&app, 80, 24);
    assert!(rendered.contains("Value: 16"));
}

#[cfg(unix)]
#[test]
fn imexplore_clicking_linked_spectrum_updates_plane() {
    let _guard = launcher_env_lock();
    let temp = tempdir().expect("tempdir");
    let script = write_fake_imexplore_script(
        temp.path(),
        &[
            fake_imexplore_snapshot_json_with_profile(
                ProtocolImageView::Plane,
                ProtocolImageFocus::Content,
                "Image ready",
                vec!["raster".to_string()],
                vec!["View: Plane".to_string(), "Value: 1".to_string()],
                Some(ImageBrowserProbe {
                    pixel_indices: vec![0, 0, 0],
                    pixel_axes: vec![],
                    value: 1.0,
                    masked: false,
                    finite: true,
                    world_axes: vec![],
                }),
                Some(ImageProfilePayload {
                    selected_sample_index: 0,
                    ..fake_image_profile_payload()
                }),
                Some(ImageNonDisplayAxisState {
                    axis: 2,
                    label: "Frequency".to_string(),
                    index: 0,
                    length: 3,
                    pixel: 0,
                }),
                image_parameters("0,0,0", "3,3,2", "1,1,1"),
            ),
            fake_imexplore_snapshot_json_with_profile(
                ProtocolImageView::Plane,
                ProtocolImageFocus::Content,
                "Image ready",
                vec!["raster".to_string()],
                vec!["View: Plane".to_string(), "Value: 2".to_string()],
                Some(ImageBrowserProbe {
                    pixel_indices: vec![0, 0, 2],
                    pixel_axes: vec![],
                    value: 2.0,
                    masked: false,
                    finite: true,
                    world_axes: vec![],
                }),
                Some(ImageProfilePayload {
                    selected_sample_index: 2,
                    ..fake_image_profile_payload()
                }),
                Some(ImageNonDisplayAxisState {
                    axis: 2,
                    label: "Frequency".to_string(),
                    index: 2,
                    length: 3,
                    pixel: 2,
                }),
                image_parameters("0,0,0", "3,3,2", "1,1,1"),
            ),
        ],
        None,
    );
    set_imexplore_launcher_bin(&script);

    let schema = imexplore_app()
        .load_schema()
        .expect("load fake imexplore schema");
    let config = ConfigStore::load_for_tests(temp.path().join("casars.toml"));
    let mut app = AppState::from_schema_with_config(imexplore_app(), schema, config);
    app.set_text_value("image_path", "/tmp/fake.image");
    app.start_run_for_test();
    app.prepare_graphics_for_test(120, 32);

    let layout = ui::compute_layout(ratatui::layout::Rect::new(0, 0, 120, 32), &app);
    let spectrum =
        ui::image_spectrum_canvas_area(&layout, true, app.image_workspace_split_ratio_for_test())
            .expect("linked spectrum area");
    app.handle_mouse_event(
        mouse(
            MouseEventKind::Down(MouseButton::Left),
            spectrum.x + spectrum.width.saturating_sub(1),
            spectrum.y + spectrum.height / 2,
        ),
        &layout,
    );

    let rendered = render_app(&app, 120, 32);
    assert!(rendered.contains("Value: 2"));
}

#[cfg(unix)]
#[test]
fn imexplore_clicking_raster_letterbox_keeps_active_pixel() {
    let _guard = launcher_env_lock();
    let temp = tempdir().expect("tempdir");
    let script = write_fake_imexplore_script(
        temp.path(),
        &[fake_imexplore_snapshot_json_with_parameters(
            ProtocolImageView::Plane,
            ProtocolImageFocus::Content,
            "Image ready",
            vec!["raster".to_string()],
            vec!["View: Plane".to_string(), "Value: 1".to_string()],
            Some(ImageBrowserProbe {
                pixel_indices: vec![0, 0],
                pixel_axes: vec![],
                value: 1.0,
                masked: false,
                finite: true,
                world_axes: vec![],
            }),
            None,
            image_parameters("0,0", "3,3", "1,1"),
        )],
        None,
    );
    set_imexplore_launcher_bin(&script);

    let schema = imexplore_app()
        .load_schema()
        .expect("load fake imexplore schema");
    let config = ConfigStore::load_for_tests(temp.path().join("casars.toml"));
    let mut app = AppState::from_schema_with_config(imexplore_app(), schema, config);
    app.set_text_value("image_path", "/tmp/fake.image");
    app.start_run_for_test();
    app.prepare_graphics_for_test(80, 24);

    let layout = ui::compute_layout(ratatui::layout::Rect::new(0, 0, 80, 24), &app);
    let canvas = ui::image_plane_canvas_area(&layout);
    let draw_rect = image_plane_draw_rect(
        canvas,
        app.image_browser_snapshot_for_test()
            .expect("imexplore session snapshot"),
        app.image_plane_font_size_for_test(),
    )
    .expect("raster draw rect");
    let (click_x, click_y) = if draw_rect.x > canvas.x {
        (draw_rect.x - 1, draw_rect.y + draw_rect.height / 2)
    } else if draw_rect.y > canvas.y {
        (draw_rect.x + draw_rect.width / 2, draw_rect.y - 1)
    } else if draw_rect.x + draw_rect.width < canvas.x + canvas.width {
        (
            draw_rect.x + draw_rect.width,
            draw_rect.y + draw_rect.height / 2,
        )
    } else if draw_rect.y + draw_rect.height < canvas.y + canvas.height {
        (
            draw_rect.x + draw_rect.width / 2,
            draw_rect.y + draw_rect.height,
        )
    } else {
        panic!("expected raster gutter inside canvas");
    };
    app.handle_mouse_event(
        mouse(MouseEventKind::Down(MouseButton::Left), click_x, click_y),
        &layout,
    );

    let rendered = render_app(&app, 80, 24);
    assert!(rendered.contains("Value: 1"));
}

#[cfg(unix)]
#[test]
fn imexplore_movie_mode_steps_and_loops_hidden_axis() {
    let _guard = launcher_env_lock();
    let temp = tempdir().expect("tempdir");
    let script = write_fake_imexplore_script(
        temp.path(),
        &[
            fake_imexplore_snapshot_json(
                ProtocolImageView::Plane,
                ProtocolImageFocus::Content,
                "Image ready",
                vec!["raster".to_string()],
                vec![
                    "View: Plane".to_string(),
                    "Hidden axis Frequency (2): 0/2".to_string(),
                    "Value: 1".to_string(),
                ],
                Some(ImageBrowserProbe {
                    pixel_indices: vec![0, 0, 0],
                    pixel_axes: vec![],
                    value: 1.0,
                    masked: false,
                    finite: true,
                    world_axes: vec![],
                }),
                Some(ImageNonDisplayAxisState {
                    axis: 2,
                    label: "Frequency".to_string(),
                    index: 0,
                    length: 3,
                    pixel: 0,
                }),
            ),
            fake_imexplore_snapshot_json(
                ProtocolImageView::Plane,
                ProtocolImageFocus::Content,
                "Image ready",
                vec!["raster".to_string()],
                vec![
                    "View: Plane".to_string(),
                    "Hidden axis Frequency (2): 1/2".to_string(),
                    "Value: 10".to_string(),
                ],
                Some(ImageBrowserProbe {
                    pixel_indices: vec![0, 0, 1],
                    pixel_axes: vec![],
                    value: 10.0,
                    masked: false,
                    finite: true,
                    world_axes: vec![],
                }),
                Some(ImageNonDisplayAxisState {
                    axis: 2,
                    label: "Frequency".to_string(),
                    index: 1,
                    length: 3,
                    pixel: 1,
                }),
            ),
            fake_imexplore_snapshot_json(
                ProtocolImageView::Plane,
                ProtocolImageFocus::Content,
                "Image ready",
                vec!["raster".to_string()],
                vec![
                    "View: Plane".to_string(),
                    "Hidden axis Frequency (2): 2/2".to_string(),
                    "Value: 100".to_string(),
                ],
                Some(ImageBrowserProbe {
                    pixel_indices: vec![0, 0, 2],
                    pixel_axes: vec![],
                    value: 100.0,
                    masked: false,
                    finite: true,
                    world_axes: vec![],
                }),
                Some(ImageNonDisplayAxisState {
                    axis: 2,
                    label: "Frequency".to_string(),
                    index: 2,
                    length: 3,
                    pixel: 2,
                }),
            ),
            fake_imexplore_snapshot_json(
                ProtocolImageView::Plane,
                ProtocolImageFocus::Content,
                "Image ready",
                vec!["raster".to_string()],
                vec![
                    "View: Plane".to_string(),
                    "Hidden axis Frequency (2): 0/2".to_string(),
                    "Value: 1".to_string(),
                ],
                Some(ImageBrowserProbe {
                    pixel_indices: vec![0, 0, 0],
                    pixel_axes: vec![],
                    value: 1.0,
                    masked: false,
                    finite: true,
                    world_axes: vec![],
                }),
                Some(ImageNonDisplayAxisState {
                    axis: 2,
                    label: "Frequency".to_string(),
                    index: 0,
                    length: 3,
                    pixel: 0,
                }),
            ),
        ],
        None,
    );
    set_imexplore_launcher_bin(&script);

    let schema = imexplore_app()
        .load_schema()
        .expect("load fake imexplore schema");
    let config = ConfigStore::load_for_tests(temp.path().join("casars.toml"));
    let mut app = AppState::from_schema_with_config(imexplore_app(), schema, config);
    app.set_text_value("image_path", "/tmp/fake.image");
    app.start_run_for_test();
    app.prepare_graphics_for_test(120, 28);
    app.on_tick();
    app.note_image_plane_presented();
    app.set_text_value_and_apply("fps", "4");

    assert!(!app.image_movie_playing_for_test());
    app.handle_key_event(KeyEvent::new(KeyCode::Char('m'), KeyModifiers::NONE));
    assert!(app.image_movie_playing_for_test());

    let mut wait_for_hidden_axis = |expected: &str| {
        let deadline = Instant::now() + Duration::from_secs(5);
        while Instant::now() < deadline {
            std::thread::sleep(Duration::from_millis(50));
            app.prepare_graphics_for_test(120, 28);
            app.on_tick();
            if app.image_plane_image_size_for_test().is_some() {
                app.note_image_plane_presented();
            }
            if app
                .browser_inspector_lines()
                .unwrap_or_default()
                .iter()
                .any(|line| line.contains(expected))
            {
                return;
            }
        }
        panic!("timed out waiting for inspector line containing {expected:?}");
    };

    wait_for_hidden_axis("Hidden axis Frequency (2): 1/2");
    wait_for_hidden_axis("Hidden axis Frequency (2): 2/2");
    wait_for_hidden_axis("Hidden axis Frequency (2): 0/2");

    app.handle_key_event(KeyEvent::new(KeyCode::Right, KeyModifiers::NONE));
    assert!(!app.image_movie_playing_for_test());
}

#[test]
fn imexplore_direct_movie_frame_preserves_full_pane_render_size() {
    let _guard = launcher_env_lock();
    let temp = tempdir().expect("tempdir");
    let script = write_fake_imexplore_script(
        temp.path(),
        &[fake_imexplore_snapshot_json(
            ProtocolImageView::Plane,
            ProtocolImageFocus::Content,
            "Image ready",
            vec!["raster".to_string()],
            vec![
                "View: Plane".to_string(),
                "Hidden axis Frequency (2): 0/2".to_string(),
                "Value: 1".to_string(),
            ],
            Some(ImageBrowserProbe {
                pixel_indices: vec![0, 0, 0],
                pixel_axes: vec![],
                value: 1.0,
                masked: false,
                finite: true,
                world_axes: vec![],
            }),
            Some(ImageNonDisplayAxisState {
                axis: 2,
                label: "Frequency".to_string(),
                index: 0,
                length: 3,
                pixel: 0,
            }),
        )],
        None,
    );
    set_imexplore_launcher_bin(&script);

    let schema = imexplore_app()
        .load_schema()
        .expect("load fake imexplore schema");
    let config = ConfigStore::load_for_tests(temp.path().join("casars.toml"));
    let mut app = AppState::from_schema_with_config(imexplore_app(), schema, config);
    app.set_text_value("image_path", "/tmp/fake.image");
    app.start_run_for_test();
    app.prepare_graphics_for_test(120, 28);

    app.set_text_value_and_apply("fps", "10");
    app.handle_key_event(KeyEvent::new(KeyCode::Char('m'), KeyModifiers::NONE));
    app.prepare_graphics_for_test(120, 28);
    let deadline = Instant::now() + Duration::from_secs(1);
    while app.image_plane_image_size_for_test().is_none() && Instant::now() < deadline {
        app.on_tick();
        std::thread::sleep(Duration::from_millis(10));
    }
    let layout = ui::compute_layout(ratatui::layout::Rect::new(0, 0, 120, 28), &app);
    let font_size = app.image_plane_font_size_for_test();
    let direct_frame = app
        .current_direct_image_movie_frame(&layout)
        .expect("direct movie frame");
    let full_width = u32::from(direct_frame.canvas.width.max(1)) * u32::from(font_size.0.max(1));
    let full_height = u32::from(direct_frame.canvas.height.max(1)) * u32::from(font_size.1.max(1));

    assert_eq!(direct_frame.rendered_image.width(), full_width);
    assert_eq!(direct_frame.rendered_image.height(), full_height);
}

#[test]
fn imexplore_direct_overlay_skips_plane_panel_requests() {
    let _guard = launcher_env_lock();
    let temp = tempdir().expect("tempdir");
    let script = write_fake_imexplore_script(
        temp.path(),
        &[fake_imexplore_snapshot_json(
            ProtocolImageView::Plane,
            ProtocolImageFocus::Content,
            "Image ready",
            vec!["raster".to_string()],
            vec![
                "View: Plane".to_string(),
                "Hidden axis Frequency (2): 0/2".to_string(),
                "Value: 1".to_string(),
            ],
            Some(ImageBrowserProbe {
                pixel_indices: vec![0, 0, 0],
                pixel_axes: vec![],
                value: 1.0,
                masked: false,
                finite: true,
                world_axes: vec![],
            }),
            Some(ImageNonDisplayAxisState {
                axis: 2,
                label: "Frequency".to_string(),
                index: 0,
                length: 3,
                pixel: 0,
            }),
        )],
        None,
    );
    set_imexplore_launcher_bin(&script);

    let schema = imexplore_app()
        .load_schema()
        .expect("load fake imexplore schema");
    let config = ConfigStore::load_for_tests(temp.path().join("casars.toml"));
    let mut app = AppState::from_schema_with_config(imexplore_app(), schema, config);
    app.set_text_value("image_path", "/tmp/fake.image");
    app.start_run_for_test();
    app.set_text_value_and_apply("fps", "10");
    app.handle_key_event(KeyEvent::new(KeyCode::Char('m'), KeyModifiers::NONE));
    app.set_image_movie_direct_overlay(true);

    app.prepare_graphics_for_test(120, 28);

    assert!(!app.image_plane_pending());
    assert!(app.image_plane_protocol().is_none());
}

#[test]
fn imexplore_direct_overlay_skips_spectrum_panel_requests() {
    let _guard = launcher_env_lock();
    let temp = tempdir().expect("tempdir");
    let script = write_fake_imexplore_script(
        temp.path(),
        &[fake_imexplore_snapshot_json_with_profile(
            ProtocolImageView::Plane,
            ProtocolImageFocus::Content,
            "Image ready",
            vec!["raster".to_string()],
            vec![
                "View: Plane".to_string(),
                "Shape: [4, 4, 3]".to_string(),
                "Value: 5".to_string(),
            ],
            Some(ImageBrowserProbe {
                pixel_indices: vec![1, 1, 1],
                pixel_axes: vec![],
                value: 5.0,
                masked: false,
                finite: true,
                world_axes: vec![ImageBrowserAxisValue {
                    name: "Frequency".to_string(),
                    unit: "Hz".to_string(),
                    value: 1.150_230_333_39e11,
                }],
            }),
            Some(fake_image_profile_payload()),
            Some(ImageNonDisplayAxisState {
                axis: 2,
                label: "Frequency".to_string(),
                index: 1,
                length: 3,
                pixel: 1,
            }),
            image_parameters("0,0,0", "3,3,2", "1,1,1"),
        )],
        None,
    );
    set_imexplore_launcher_bin(&script);

    let schema = imexplore_app()
        .load_schema()
        .expect("load fake imexplore schema");
    let config = ConfigStore::load_for_tests(temp.path().join("casars.toml"));
    let mut app = AppState::from_schema_with_config(imexplore_app(), schema, config);
    app.set_text_value("image_path", "/tmp/fake.image");
    app.start_run_for_test();
    app.set_text_value_and_apply("fps", "10");
    app.handle_key_event(KeyEvent::new(KeyCode::Char('m'), KeyModifiers::NONE));
    app.set_image_movie_direct_overlay(true);

    app.prepare_graphics_for_test(120, 32);

    assert!(!app.image_spectrum_pending());
    assert!(app.image_spectrum_protocol().is_none());
}

#[cfg(unix)]
#[test]
fn imexplore_stopping_movie_preserves_pane_state() {
    let _guard = launcher_env_lock();
    let temp = tempdir().expect("tempdir");
    let script = write_fake_imexplore_script(
        temp.path(),
        &[fake_imexplore_snapshot_json_with_profile(
            ProtocolImageView::Plane,
            ProtocolImageFocus::Content,
            "Image ready",
            vec!["raster".to_string()],
            vec!["View: Plane".to_string()],
            Some(ImageBrowserProbe {
                pixel_indices: vec![0, 0, 0],
                pixel_axes: vec![],
                value: 1.0,
                masked: false,
                finite: true,
                world_axes: vec![],
            }),
            Some(fake_image_profile_payload()),
            Some(ImageNonDisplayAxisState {
                axis: 2,
                label: "Frequency".to_string(),
                index: 0,
                length: 3,
                pixel: 0,
            }),
            image_parameters("0,0,0", "3,3,2", "1,1,1"),
        )],
        None,
    );
    set_imexplore_launcher_bin(&script);

    let config_path = temp.path().join("casars.toml");
    let schema = imexplore_app()
        .load_schema()
        .expect("load fake imexplore schema");
    let mut app = AppState::from_schema_with_config(
        imexplore_app(),
        schema,
        ConfigStore::load_for_tests(config_path),
    );
    app.set_text_value("image_path", "/tmp/fake.image");
    app.start_run_for_test();

    let main_ratio = app.pane_split_ratio_for_test();
    let workspace_ratio = app.image_workspace_split_ratio_for_test();
    let before_layout = ui::compute_layout(ratatui::layout::Rect::new(0, 0, 140, 36), &app);
    assert!(!app.parameters_pane_collapsed());
    assert!(
        ui::image_spectrum_canvas_area(&before_layout, true, workspace_ratio).is_some(),
        "spectrum pane should be visible before starting movie"
    );

    app.handle_key_event(KeyEvent::new(KeyCode::Char('m'), KeyModifiers::NONE));
    assert!(app.image_movie_playing_for_test());
    app.set_image_movie_direct_overlay(true);
    app.handle_key_event(KeyEvent::new(KeyCode::Char('m'), KeyModifiers::NONE));

    assert!(!app.image_movie_playing_for_test());
    assert!((app.pane_split_ratio_for_test() - main_ratio).abs() < f32::EPSILON);
    assert!((app.image_workspace_split_ratio_for_test() - workspace_ratio).abs() < f32::EPSILON);
    assert!(!app.parameters_pane_collapsed());
    assert!(app.image_plane_has_linked_profile());

    let after_layout = ui::compute_layout(ratatui::layout::Rect::new(0, 0, 140, 36), &app);
    assert!(
        ui::image_spectrum_canvas_area(
            &after_layout,
            app.image_plane_has_linked_profile(),
            app.image_workspace_split_ratio_for_test(),
        )
        .is_some(),
        "spectrum pane should remain visible after stopping movie"
    );
}

#[cfg(unix)]
#[test]
fn imexplore_stopping_movie_keeps_frozen_spectrum_workspace_visible() {
    let _guard = launcher_env_lock();
    let temp = tempdir().expect("tempdir");
    let script = write_fake_imexplore_script(
        temp.path(),
        &[
            fake_imexplore_snapshot_json_with_profile(
                ProtocolImageView::Plane,
                ProtocolImageFocus::Content,
                "Image ready",
                vec!["raster".to_string()],
                vec!["View: Plane".to_string()],
                Some(ImageBrowserProbe {
                    pixel_indices: vec![0, 0, 0],
                    pixel_axes: vec![],
                    value: 1.0,
                    masked: false,
                    finite: true,
                    world_axes: vec![],
                }),
                Some(fake_image_profile_payload()),
                Some(ImageNonDisplayAxisState {
                    axis: 2,
                    label: "Frequency".to_string(),
                    index: 0,
                    length: 3,
                    pixel: 0,
                }),
                image_parameters("0,0,0", "3,3,2", "1,1,1"),
            ),
            fake_imexplore_snapshot_json(
                ProtocolImageView::Plane,
                ProtocolImageFocus::Content,
                "Image ready",
                vec!["raster".to_string()],
                vec!["View: Plane".to_string()],
                Some(ImageBrowserProbe {
                    pixel_indices: vec![0, 0, 1],
                    pixel_axes: vec![],
                    value: 2.0,
                    masked: false,
                    finite: true,
                    world_axes: vec![],
                }),
                Some(ImageNonDisplayAxisState {
                    axis: 2,
                    label: "Frequency".to_string(),
                    index: 1,
                    length: 3,
                    pixel: 1,
                }),
            ),
        ],
        None,
    );
    set_imexplore_launcher_bin(&script);

    let schema = imexplore_app()
        .load_schema()
        .expect("load fake imexplore schema");
    let config = ConfigStore::load_for_tests(temp.path().join("casars.toml"));
    let mut app = AppState::from_schema_with_config(imexplore_app(), schema, config);
    app.set_text_value("image_path", "/tmp/fake.image");
    app.start_run_for_test();
    app.seed_image_spectrum_content_for_test((320, 120));

    assert!(app.image_spectrum_image_size_for_test().is_some());
    assert!(app.image_profile_title_line().is_some());

    app.set_text_value_and_apply("fps", "30");
    app.handle_key_event(KeyEvent::new(KeyCode::Char('m'), KeyModifiers::NONE));
    app.set_image_movie_direct_overlay(true);
    app.clear_image_profile_for_test();

    assert!(app.image_movie_playing_for_test());
    assert!(
        app.image_profile_title_line().is_none(),
        "test setup should simulate a movie-stepped snapshot without a live profile"
    );

    app.handle_key_event(KeyEvent::new(KeyCode::Char('m'), KeyModifiers::NONE));
    assert!(!app.image_movie_playing_for_test());
    assert!(app.image_plane_has_linked_profile());

    let layout = ui::compute_layout(ratatui::layout::Rect::new(0, 0, 160, 32), &app);
    assert!(
        ui::image_spectrum_canvas_area(
            &layout,
            app.image_plane_has_linked_profile(),
            app.image_workspace_split_ratio_for_test(),
        )
        .is_some(),
        "spectrum workspace should remain visible when frozen content exists"
    );
}

#[cfg(unix)]
#[test]
fn kitty_enoent_response_invalidates_movie_store_cache() {
    let _guard = launcher_env_lock();
    let temp = tempdir().expect("tempdir");
    let script = write_fake_imexplore_script(
        temp.path(),
        &[fake_imexplore_snapshot_json(
            ProtocolImageView::Plane,
            ProtocolImageFocus::Content,
            "Image ready",
            vec!["raster".to_string()],
            vec!["View: Plane".to_string()],
            Some(ImageBrowserProbe {
                pixel_indices: vec![0, 0, 0],
                pixel_axes: vec![],
                value: 1.0,
                masked: false,
                finite: true,
                world_axes: vec![],
            }),
            Some(ImageNonDisplayAxisState {
                axis: 2,
                label: "Frequency".to_string(),
                index: 0,
                length: 3,
                pixel: 0,
            }),
        )],
        None,
    );
    set_imexplore_launcher_bin(&script);

    let schema = imexplore_app()
        .load_schema()
        .expect("load fake imexplore schema");
    let config = ConfigStore::load_for_tests(temp.path().join("casars.toml"));
    let mut app = AppState::from_schema_with_config(imexplore_app(), schema, config);
    app.set_text_value("image_path", "/tmp/fake.image");
    app.start_run_for_test();

    app.handle_key_event(KeyEvent::new(KeyCode::Char('_'), KeyModifiers::ALT));
    for ch in [
        'G', 'i', '=', '1', '0', '0', '1', '0', '0', '1', ',', 'p', '=', '1', '0', '0', '0', '0',
        '0', '0', ';', 'E', 'N', 'O', 'E', 'N', 'T', ':', ' ', 'i', 'm', 'a', 'g', 'e', ' ', 'n',
        'o', 't', ' ', 'f', 'o', 'u', 'n', 'd',
    ] {
        app.handle_key_event(KeyEvent::new(KeyCode::Char(ch), KeyModifiers::NONE));
    }
    app.handle_key_event(KeyEvent::new(KeyCode::Char('\\'), KeyModifiers::ALT));

    assert!(app.kitty_movie_store_invalidated_for_test());
    assert!(app.take_kitty_movie_store_invalidated());
    assert!(!app.kitty_movie_store_invalidated_for_test());
}

#[cfg(unix)]
#[test]
fn imexplore_late_kitty_response_after_movie_stop_does_not_toggle_ui_state() {
    let _guard = launcher_env_lock();
    let temp = tempdir().expect("tempdir");
    let script = write_fake_imexplore_script(
        temp.path(),
        &[fake_imexplore_snapshot_json_with_profile(
            ProtocolImageView::Plane,
            ProtocolImageFocus::Content,
            "Image ready",
            vec!["raster".to_string()],
            vec!["View: Plane".to_string()],
            Some(ImageBrowserProbe {
                pixel_indices: vec![0, 0, 0],
                pixel_axes: vec![],
                value: 1.0,
                masked: false,
                finite: true,
                world_axes: vec![],
            }),
            Some(fake_image_profile_payload()),
            Some(ImageNonDisplayAxisState {
                axis: 2,
                label: "Frequency".to_string(),
                index: 0,
                length: 3,
                pixel: 0,
            }),
            image_parameters("0,0,0", "3,3,2", "1,1,1"),
        )],
        None,
    );
    set_imexplore_launcher_bin(&script);

    let schema = imexplore_app()
        .load_schema()
        .expect("load fake imexplore schema");
    let config = ConfigStore::load_for_tests(temp.path().join("casars.toml"));
    let mut app = AppState::from_schema_with_config(imexplore_app(), schema, config);
    app.set_text_value("image_path", "/tmp/fake.image");
    app.start_run_for_test();

    let original_ratio = app.pane_split_ratio_for_test();
    app.handle_key_event(KeyEvent::new(KeyCode::Char('m'), KeyModifiers::NONE));
    assert!(app.image_movie_playing_for_test());
    app.handle_key_event(KeyEvent::new(KeyCode::Char('m'), KeyModifiers::NONE));
    assert!(!app.image_movie_playing_for_test());

    app.handle_key_event(KeyEvent::new(KeyCode::Char('_'), KeyModifiers::ALT));
    for ch in ['G', 'i', '=', '1', ',', 'p', '=', '1', ';', 'O', 'K'] {
        app.handle_key_event(KeyEvent::new(KeyCode::Char(ch), KeyModifiers::NONE));
    }
    app.handle_key_event(KeyEvent::new(KeyCode::Char('\\'), KeyModifiers::ALT));

    assert!((app.pane_split_ratio_for_test() - original_ratio).abs() < f32::EPSILON);
    assert_eq!(app.image_plane_mode_label_for_test(), Some("raster"));
    assert_eq!(app.image_plane_invert_for_test(), Some(false));
}

#[test]
fn imexplore_perf_disabled_does_not_create_trace_files() {
    let _guard = launcher_env_lock();
    clear_imexplore_perf_env();
    let temp = tempdir().expect("tempdir");
    let script = write_fake_imexplore_script(
        temp.path(),
        &[fake_imexplore_snapshot_json(
            ProtocolImageView::Plane,
            ProtocolImageFocus::Content,
            "Image ready",
            vec!["raster".to_string()],
            vec![
                "View: Plane".to_string(),
                "Hidden axis Frequency (2): 0/2".to_string(),
                "Value: 1".to_string(),
            ],
            Some(ImageBrowserProbe {
                pixel_indices: vec![0, 0, 0],
                pixel_axes: vec![],
                value: 1.0,
                masked: false,
                finite: true,
                world_axes: vec![],
            }),
            Some(ImageNonDisplayAxisState {
                axis: 2,
                label: "Frequency".to_string(),
                index: 0,
                length: 3,
                pixel: 0,
            }),
        )],
        None,
    );
    set_imexplore_launcher_bin(&script);

    let schema = imexplore_app()
        .load_schema()
        .expect("load fake imexplore schema");
    let config = ConfigStore::load_for_tests(temp.path().join("casars.toml"));
    let mut app = AppState::from_schema_with_config(imexplore_app(), schema, config);
    app.set_text_value("image_path", "/tmp/fake.image");
    app.start_run_for_test();

    assert!(app.movie_perf_json_path_for_test().is_none());
    assert!(app.movie_perf_log_path_for_test().is_none());
}

#[cfg(unix)]
#[test]
fn imexplore_perf_trace_emits_ordered_frame_events_and_summary() {
    let _guard = launcher_env_lock();
    let temp = tempdir().expect("tempdir");
    let perf_dir = temp.path().join("perf");
    let _perf_guard = set_imexplore_perf_env(&perf_dir);
    let script = write_fake_imexplore_script(
        temp.path(),
        &[
            fake_imexplore_snapshot_json(
                ProtocolImageView::Plane,
                ProtocolImageFocus::Content,
                "Image ready",
                vec!["raster".to_string()],
                vec![
                    "View: Plane".to_string(),
                    "Hidden axis Frequency (2): 0/1".to_string(),
                    "Value: 1".to_string(),
                ],
                Some(ImageBrowserProbe {
                    pixel_indices: vec![0, 0, 0],
                    pixel_axes: vec![],
                    value: 1.0,
                    masked: false,
                    finite: true,
                    world_axes: vec![],
                }),
                Some(ImageNonDisplayAxisState {
                    axis: 2,
                    label: "Frequency".to_string(),
                    index: 0,
                    length: 2,
                    pixel: 0,
                }),
            ),
            fake_imexplore_snapshot_json(
                ProtocolImageView::Plane,
                ProtocolImageFocus::Content,
                "Image ready",
                vec!["raster".to_string()],
                vec![
                    "View: Plane".to_string(),
                    "Hidden axis Frequency (2): 1/1".to_string(),
                    "Value: 10".to_string(),
                ],
                Some(ImageBrowserProbe {
                    pixel_indices: vec![0, 0, 1],
                    pixel_axes: vec![],
                    value: 10.0,
                    masked: false,
                    finite: true,
                    world_axes: vec![],
                }),
                Some(ImageNonDisplayAxisState {
                    axis: 2,
                    label: "Frequency".to_string(),
                    index: 1,
                    length: 2,
                    pixel: 1,
                }),
            ),
        ],
        None,
    );
    set_imexplore_launcher_bin(&script);

    let schema = imexplore_app()
        .load_schema()
        .expect("load fake imexplore schema");
    let config = ConfigStore::load_for_tests(temp.path().join("casars.toml"));
    let mut app = AppState::from_schema_with_config(imexplore_app(), schema, config);
    app.set_text_value("image_path", "/tmp/fake.image");
    app.start_run_for_test();
    app.prepare_graphics_for_test(120, 28);
    app.on_tick();
    app.note_image_plane_presented();

    let json_path = app
        .movie_perf_json_path_for_test()
        .expect("json path")
        .to_path_buf();
    let log_path = app
        .movie_perf_log_path_for_test()
        .expect("log path")
        .to_path_buf();

    app.set_text_value_and_apply("fps", "30");
    app.handle_key_event(KeyEvent::new(KeyCode::Char('m'), KeyModifiers::NONE));
    let start = Instant::now();
    let mut saw_movie_scheduler_progress = false;
    while start.elapsed() < Duration::from_secs(2) {
        app.prepare_graphics_for_test(120, 28);
        app.on_tick();
        if app.image_plane_image_size_for_test().is_some() {
            app.note_image_plane_presented();
        }
        let kinds = read_perf_events(&json_path)
            .into_iter()
            .filter_map(|event| {
                event
                    .get("kind")
                    .and_then(|kind| kind.as_str())
                    .map(str::to_string)
            })
            .collect::<Vec<_>>();
        if kinds.iter().any(|kind| {
            matches!(
                kind.as_str(),
                "preview_requested"
                    | "preview_received"
                    | "bundle_render_requested"
                    | "bundle_ready"
                    | "bundle_presented"
            )
        }) {
            saw_movie_scheduler_progress = true;
            break;
        }
        std::thread::sleep(Duration::from_millis(20));
    }
    assert!(saw_movie_scheduler_progress);

    unsafe {
        libc::raise(libc::SIGUSR1);
    }
    app.on_tick();

    let events = read_perf_events(&json_path);
    let frame_events = events
        .iter()
        .filter(|event| event.get("frame_seq") == Some(&serde_json::json!(1u64)))
        .collect::<Vec<_>>();
    let kinds = frame_events
        .iter()
        .filter_map(|event| event.get("kind").and_then(|kind| kind.as_str()))
        .collect::<Vec<_>>();
    let legacy_prefix = [
        "frame_requested",
        "browser_command_sent",
        "browser_snapshot_received",
        "plane_render_requested",
    ];
    let movie_prefix = [
        "frame_requested",
        "preview_requested",
        "preview_received",
        "bundle_render_requested",
    ];
    assert!(kinds.starts_with(&legacy_prefix) || kinds.starts_with(&movie_prefix));
    if kinds.starts_with(&movie_prefix) {
        assert!(kinds.contains(&"preview_requested"));
        assert!(kinds.contains(&"preview_received"));
        assert!(kinds.contains(&"bundle_render_requested"));
    } else {
        assert!(kinds.contains(&"plane_render_requested"));
    }

    let render_request_hashes = frame_events
        .iter()
        .filter_map(|event| event.get("render_request_key_hash"))
        .filter_map(|value| value.as_u64())
        .collect::<Vec<_>>();
    assert!(!render_request_hashes.is_empty());
    assert!(render_request_hashes.iter().all(|hash| *hash > 0));

    let summary_events = events
        .iter()
        .filter(|event| event.get("kind") == Some(&serde_json::json!("summary")))
        .collect::<Vec<_>>();
    assert!(!summary_events.is_empty());

    let summary_log = fs::read_to_string(&log_path).expect("read perf log");
    assert!(summary_log.contains("summary achieved_fps="));
}

#[cfg(unix)]
#[test]
fn imexplore_perf_sigusr1_flushes_summary_without_stopping_movie() {
    let _guard = launcher_env_lock();
    let temp = tempdir().expect("tempdir");
    let perf_dir = temp.path().join("perf");
    let _perf_guard = set_imexplore_perf_env(&perf_dir);
    let script = write_fake_imexplore_script(
        temp.path(),
        &[
            fake_imexplore_snapshot_json(
                ProtocolImageView::Plane,
                ProtocolImageFocus::Content,
                "Image ready",
                vec!["raster".to_string()],
                vec![
                    "View: Plane".to_string(),
                    "Hidden axis Frequency (2): 0/1".to_string(),
                    "Value: 1".to_string(),
                ],
                Some(ImageBrowserProbe {
                    pixel_indices: vec![0, 0, 0],
                    pixel_axes: vec![],
                    value: 1.0,
                    masked: false,
                    finite: true,
                    world_axes: vec![],
                }),
                Some(ImageNonDisplayAxisState {
                    axis: 2,
                    label: "Frequency".to_string(),
                    index: 0,
                    length: 2,
                    pixel: 0,
                }),
            ),
            fake_imexplore_snapshot_json(
                ProtocolImageView::Plane,
                ProtocolImageFocus::Content,
                "Image ready",
                vec!["raster".to_string()],
                vec![
                    "View: Plane".to_string(),
                    "Hidden axis Frequency (2): 1/1".to_string(),
                    "Value: 10".to_string(),
                ],
                Some(ImageBrowserProbe {
                    pixel_indices: vec![0, 0, 1],
                    pixel_axes: vec![],
                    value: 10.0,
                    masked: false,
                    finite: true,
                    world_axes: vec![],
                }),
                Some(ImageNonDisplayAxisState {
                    axis: 2,
                    label: "Frequency".to_string(),
                    index: 1,
                    length: 2,
                    pixel: 1,
                }),
            ),
        ],
        None,
    );
    set_imexplore_launcher_bin(&script);

    let schema = imexplore_app()
        .load_schema()
        .expect("load fake imexplore schema");
    let config = ConfigStore::load_for_tests(temp.path().join("casars.toml"));
    let mut app = AppState::from_schema_with_config(imexplore_app(), schema, config);
    app.set_text_value("image_path", "/tmp/fake.image");
    app.start_run_for_test();
    app.set_text_value_and_apply("fps", "30");
    app.handle_key_event(KeyEvent::new(KeyCode::Char('m'), KeyModifiers::NONE));

    std::thread::sleep(Duration::from_millis(50));
    app.on_tick();
    assert!(app.image_movie_playing_for_test());

    let log_path = app
        .movie_perf_log_path_for_test()
        .expect("log path")
        .to_path_buf();
    unsafe {
        libc::raise(libc::SIGUSR1);
    }
    app.on_tick();

    assert!(app.image_movie_playing_for_test());
    let summary_log = fs::read_to_string(&log_path).expect("read perf log");
    assert!(summary_log.contains("summary achieved_fps="));
}

#[cfg(unix)]
#[test]
fn imexplore_mouse_move_does_not_stop_movie_mode() {
    let _guard = launcher_env_lock();
    let temp = tempdir().expect("tempdir");
    let script = write_fake_imexplore_script(
        temp.path(),
        &[fake_imexplore_snapshot_json(
            ProtocolImageView::Plane,
            ProtocolImageFocus::Content,
            "Image ready",
            vec!["raster".to_string()],
            vec![
                "View: Plane".to_string(),
                "Hidden axis Frequency (2): 0/2".to_string(),
                "Value: 1".to_string(),
            ],
            Some(ImageBrowserProbe {
                pixel_indices: vec![0, 0, 0],
                pixel_axes: vec![],
                value: 1.0,
                masked: false,
                finite: true,
                world_axes: vec![],
            }),
            Some(ImageNonDisplayAxisState {
                axis: 2,
                label: "Frequency".to_string(),
                index: 0,
                length: 3,
                pixel: 0,
            }),
        )],
        None,
    );
    set_imexplore_launcher_bin(&script);

    let schema = imexplore_app()
        .load_schema()
        .expect("load fake imexplore schema");
    let config = ConfigStore::load_for_tests(temp.path().join("casars.toml"));
    let mut app = AppState::from_schema_with_config(imexplore_app(), schema, config);
    app.set_text_value("image_path", "/tmp/fake.image");
    app.start_run_for_test();

    app.handle_key_event(KeyEvent::new(KeyCode::Char('m'), KeyModifiers::NONE));
    assert!(app.image_movie_playing_for_test());

    let layout = ui::compute_layout(ratatui::layout::Rect::new(0, 0, 120, 28), &app);
    app.handle_mouse_event(
        mouse(
            MouseEventKind::Moved,
            layout.result_content.x + 5,
            layout.result_content.y + 5,
        ),
        &layout,
    );

    assert!(app.image_movie_playing_for_test());
}

#[cfg(unix)]
#[test]
fn imexplore_unmapped_key_does_not_stop_movie_mode() {
    let _guard = launcher_env_lock();
    let temp = tempdir().expect("tempdir");
    let script = write_fake_imexplore_script(
        temp.path(),
        &[fake_imexplore_snapshot_json(
            ProtocolImageView::Plane,
            ProtocolImageFocus::Content,
            "Image ready",
            vec!["raster".to_string()],
            vec![
                "View: Plane".to_string(),
                "Hidden axis Frequency (2): 0/2".to_string(),
                "Value: 1".to_string(),
            ],
            Some(ImageBrowserProbe {
                pixel_indices: vec![0, 0, 0],
                pixel_axes: vec![],
                value: 1.0,
                masked: false,
                finite: true,
                world_axes: vec![],
            }),
            Some(ImageNonDisplayAxisState {
                axis: 2,
                label: "Frequency".to_string(),
                index: 0,
                length: 3,
                pixel: 0,
            }),
        )],
        None,
    );
    set_imexplore_launcher_bin(&script);

    let schema = imexplore_app()
        .load_schema()
        .expect("load fake imexplore schema");
    let config = ConfigStore::load_for_tests(temp.path().join("casars.toml"));
    let mut app = AppState::from_schema_with_config(imexplore_app(), schema, config);
    app.set_text_value("image_path", "/tmp/fake.image");
    app.start_run_for_test();

    app.handle_key_event(KeyEvent::new(KeyCode::Char('m'), KeyModifiers::NONE));
    assert!(app.image_movie_playing_for_test());

    app.handle_key_event(KeyEvent::new(KeyCode::F(24), KeyModifiers::NONE));

    assert!(app.image_movie_playing_for_test());
}

#[cfg(unix)]
#[test]
fn imexplore_quit_keys_bypass_movie_stop_handling() {
    let _guard = launcher_env_lock();
    let temp = tempdir().expect("tempdir");
    let script = write_fake_imexplore_script(
        temp.path(),
        &[fake_imexplore_snapshot_json(
            ProtocolImageView::Plane,
            ProtocolImageFocus::Content,
            "Image ready",
            vec!["raster".to_string()],
            vec![
                "View: Plane".to_string(),
                "Hidden axis Frequency (2): 0/2".to_string(),
                "Value: 1".to_string(),
            ],
            Some(ImageBrowserProbe {
                pixel_indices: vec![0, 0, 0],
                pixel_axes: vec![],
                value: 1.0,
                masked: false,
                finite: true,
                world_axes: vec![],
            }),
            Some(ImageNonDisplayAxisState {
                axis: 2,
                label: "Frequency".to_string(),
                index: 0,
                length: 3,
                pixel: 0,
            }),
        )],
        None,
    );
    set_imexplore_launcher_bin(&script);

    let schema = imexplore_app()
        .load_schema()
        .expect("load fake imexplore schema");
    let config = ConfigStore::load_for_tests(temp.path().join("casars.toml"));
    let mut app = AppState::from_schema_with_config(imexplore_app(), schema, config);
    app.set_text_value("image_path", "/tmp/fake.image");
    app.start_run_for_test();

    app.handle_key_event(KeyEvent::new(KeyCode::Char('m'), KeyModifiers::NONE));
    assert!(app.image_movie_playing_for_test());

    assert!(
        !app.key_event_stops_movie_for_test(KeyEvent::new(KeyCode::Char('q'), KeyModifiers::NONE,))
    );
    assert!(
        !app.key_event_stops_movie_for_test(KeyEvent::new(
            KeyCode::Char('c'),
            KeyModifiers::CONTROL,
        ))
    );
    assert!(
        app.key_event_stops_movie_for_test(KeyEvent::new(KeyCode::Char('i'), KeyModifiers::NONE,))
    );
}

#[cfg(unix)]
#[test]
fn imexplore_ignores_embedded_kitty_protocol_responses() {
    let _guard = launcher_env_lock();
    let temp = tempdir().expect("tempdir");
    let script = write_fake_imexplore_script(
        temp.path(),
        &[fake_imexplore_snapshot_json(
            ProtocolImageView::Plane,
            ProtocolImageFocus::Content,
            "Image ready",
            vec!["raster".to_string()],
            vec![
                "View: Plane".to_string(),
                "Hidden axis Frequency (2): 0/2".to_string(),
                "Value: 1".to_string(),
            ],
            Some(ImageBrowserProbe {
                pixel_indices: vec![0, 0, 0],
                pixel_axes: vec![],
                value: 1.0,
                masked: false,
                finite: true,
                world_axes: vec![],
            }),
            Some(ImageNonDisplayAxisState {
                axis: 2,
                label: "Frequency".to_string(),
                index: 0,
                length: 3,
                pixel: 0,
            }),
        )],
        None,
    );
    set_imexplore_launcher_bin(&script);

    let schema = imexplore_app()
        .load_schema()
        .expect("load fake imexplore schema");
    let config = ConfigStore::load_for_tests(temp.path().join("casars.toml"));
    let mut app = AppState::from_schema_with_config(imexplore_app(), schema, config);
    app.set_text_value("image_path", "/tmp/fake.image");
    app.start_run_for_test();

    app.handle_key_event(KeyEvent::new(KeyCode::Char('m'), KeyModifiers::NONE));
    assert!(app.image_movie_playing_for_test());
    assert_eq!(app.image_plane_mode_label_for_test(), Some("raster"));

    for key_event in [
        KeyEvent::new(KeyCode::Char('_'), KeyModifiers::ALT),
        KeyEvent::new(KeyCode::Char('G'), KeyModifiers::SHIFT),
        KeyEvent::new(KeyCode::Char('i'), KeyModifiers::NONE),
        KeyEvent::new(KeyCode::Char('='), KeyModifiers::NONE),
        KeyEvent::new(KeyCode::Char('1'), KeyModifiers::NONE),
        KeyEvent::new(KeyCode::Char(','), KeyModifiers::NONE),
        KeyEvent::new(KeyCode::Char('p'), KeyModifiers::NONE),
        KeyEvent::new(KeyCode::Char('='), KeyModifiers::NONE),
        KeyEvent::new(KeyCode::Char('1'), KeyModifiers::NONE),
        KeyEvent::new(KeyCode::Char(';'), KeyModifiers::NONE),
        KeyEvent::new(KeyCode::Char('O'), KeyModifiers::SHIFT),
        KeyEvent::new(KeyCode::Char('K'), KeyModifiers::SHIFT),
        KeyEvent::new(KeyCode::Char('\\'), KeyModifiers::ALT),
    ] {
        app.handle_key_event(key_event);
    }

    assert!(app.image_movie_playing_for_test());
    assert_eq!(app.image_plane_mode_label_for_test(), Some("raster"));
}

#[cfg(unix)]
#[test]
fn imexplore_escape_toggles_live_reticle() {
    let _guard = launcher_env_lock();
    let temp = tempdir().expect("tempdir");
    let script = write_fake_imexplore_script(
        temp.path(),
        &[fake_imexplore_snapshot_json(
            ProtocolImageView::Plane,
            ProtocolImageFocus::Content,
            "Image ready",
            vec!["raster".to_string()],
            vec!["View: Plane".to_string()],
            Some(ImageBrowserProbe {
                pixel_indices: vec![1, 1, 0],
                pixel_axes: vec![],
                value: 11.0,
                masked: false,
                finite: true,
                world_axes: vec![],
            }),
            Some(ImageNonDisplayAxisState {
                axis: 2,
                label: "Frequency".to_string(),
                index: 0,
                length: 3,
                pixel: 0,
            }),
        )],
        None,
    );
    set_imexplore_launcher_bin(&script);

    let schema = imexplore_app()
        .load_schema()
        .expect("load fake imexplore schema");
    let config = ConfigStore::load_for_tests(temp.path().join("casars.toml"));
    let mut app = AppState::from_schema_with_config(imexplore_app(), schema, config);
    app.set_text_value("image_path", "/tmp/fake.image");
    app.start_run_for_test();

    assert!(app.image_live_reticle_visible_for_test());
    app.handle_key_event(KeyEvent::new(KeyCode::Esc, KeyModifiers::NONE));
    assert!(!app.image_live_reticle_visible_for_test());
    assert!(
        app.browser_inspector_lines()
            .unwrap_or_default()
            .iter()
            .any(|line| line.contains("Reticle: hidden"))
    );

    app.handle_key_event(KeyEvent::new(KeyCode::Esc, KeyModifiers::NONE));
    assert!(app.image_live_reticle_visible_for_test());
}

#[cfg(unix)]
#[test]
fn imexplore_region_start_hides_live_reticle() {
    let _guard = launcher_env_lock();
    let temp = tempdir().expect("tempdir");
    let snapshot = fake_imexplore_snapshot_json(
        ProtocolImageView::Plane,
        ProtocolImageFocus::Content,
        "Image ready",
        vec!["raster".to_string()],
        vec!["View: Plane".to_string()],
        Some(ImageBrowserProbe {
            pixel_indices: vec![1, 1, 0],
            pixel_axes: vec![],
            value: 11.0,
            masked: false,
            finite: true,
            world_axes: vec![],
        }),
        Some(ImageNonDisplayAxisState {
            axis: 2,
            label: "Frequency".to_string(),
            index: 0,
            length: 3,
            pixel: 0,
        }),
    );
    let script = write_fake_imexplore_script(temp.path(), &[snapshot.clone(), snapshot], None);
    set_imexplore_launcher_bin(&script);

    let schema = imexplore_app()
        .load_schema()
        .expect("load fake imexplore schema");
    let config = ConfigStore::load_for_tests(temp.path().join("casars.toml"));
    let mut app = AppState::from_schema_with_config(imexplore_app(), schema, config);
    app.set_text_value("image_path", "/tmp/fake.image");
    app.start_run_for_test();

    assert!(app.image_live_reticle_visible_for_test());
    app.handle_key_event(KeyEvent::new(KeyCode::Char('R'), KeyModifiers::NONE));
    assert!(!app.image_live_reticle_visible_for_test());
}

#[cfg(unix)]
#[test]
fn imexplore_region_display_suppresses_point_reticle_in_plane_render() {
    let _guard = launcher_env_lock();
    let temp = tempdir().expect("tempdir");
    let snapshot = fake_imexplore_snapshot_json_with_region(fake_imexplore_snapshot_json(
        ProtocolImageView::Plane,
        ProtocolImageFocus::Content,
        "Image ready",
        vec!["raster".to_string()],
        vec!["View: Plane".to_string()],
        Some(ImageBrowserProbe {
            pixel_indices: vec![1, 1, 0],
            pixel_axes: vec![],
            value: 11.0,
            masked: false,
            finite: true,
            world_axes: vec![],
        }),
        Some(ImageNonDisplayAxisState {
            axis: 2,
            label: "Frequency".to_string(),
            index: 0,
            length: 3,
            pixel: 0,
        }),
    ));
    let script = write_fake_imexplore_script(temp.path(), &[snapshot], None);
    set_imexplore_launcher_bin(&script);

    let schema = imexplore_app()
        .load_schema()
        .expect("load fake imexplore schema");
    let config = ConfigStore::load_for_tests(temp.path().join("casars.toml"));
    let mut app = AppState::from_schema_with_config(imexplore_app(), schema, config);
    app.set_text_value("image_path", "/tmp/fake.image");
    app.start_run_for_test();

    let layout = ui::compute_layout(ratatui::layout::Rect::new(0, 0, 140, 48), &app);
    assert!(
        app.image_plane_cursor_sample_for_test(&layout, (8, 16))
            .is_none()
    );
}

#[cfg(unix)]
#[test]
fn imexplore_can_pin_cycle_and_remove_probes() {
    let _guard = launcher_env_lock();
    let temp = tempdir().expect("tempdir");
    let mut profile_a = fake_image_profile_payload();
    profile_a.selected_sample_index = 0;
    let mut profile_b = fake_image_profile_payload();
    profile_b.selected_sample_index = 2;
    let script = write_fake_imexplore_script(
        temp.path(),
        &[
            fake_imexplore_snapshot_json_with_profile(
                ProtocolImageView::Plane,
                ProtocolImageFocus::Content,
                "Image ready",
                vec!["raster".to_string()],
                vec!["View: Plane".to_string()],
                Some(ImageBrowserProbe {
                    pixel_indices: vec![1, 1, 0],
                    pixel_axes: vec![],
                    value: 11.0,
                    masked: false,
                    finite: true,
                    world_axes: vec![],
                }),
                Some(profile_a.clone()),
                Some(ImageNonDisplayAxisState {
                    axis: 2,
                    label: "Frequency".to_string(),
                    index: 0,
                    length: 3,
                    pixel: 0,
                }),
                image_parameters("0,0,0", "3,3,2", "1,1,1"),
            ),
            fake_imexplore_snapshot_json_with_profile(
                ProtocolImageView::Plane,
                ProtocolImageFocus::Content,
                "Image ready",
                vec!["raster".to_string()],
                vec!["View: Plane".to_string()],
                Some(ImageBrowserProbe {
                    pixel_indices: vec![2, 1, 0],
                    pixel_axes: vec![],
                    value: 22.0,
                    masked: false,
                    finite: true,
                    world_axes: vec![],
                }),
                Some(profile_b.clone()),
                Some(ImageNonDisplayAxisState {
                    axis: 2,
                    label: "Frequency".to_string(),
                    index: 0,
                    length: 3,
                    pixel: 0,
                }),
                image_parameters("0,0,0", "3,3,2", "1,1,1"),
            ),
            fake_imexplore_snapshot_json_with_profile(
                ProtocolImageView::Plane,
                ProtocolImageFocus::Content,
                "Image ready",
                vec!["raster".to_string()],
                vec!["View: Plane".to_string()],
                Some(ImageBrowserProbe {
                    pixel_indices: vec![1, 1, 0],
                    pixel_axes: vec![],
                    value: 11.0,
                    masked: false,
                    finite: true,
                    world_axes: vec![],
                }),
                Some(profile_a),
                Some(ImageNonDisplayAxisState {
                    axis: 2,
                    label: "Frequency".to_string(),
                    index: 0,
                    length: 3,
                    pixel: 0,
                }),
                image_parameters("0,0,0", "3,3,2", "1,1,1"),
            ),
        ],
        None,
    );
    set_imexplore_launcher_bin(&script);

    let schema = imexplore_app()
        .load_schema()
        .expect("load fake imexplore schema");
    let config = ConfigStore::load_for_tests(temp.path().join("casars.toml"));
    let mut app = AppState::from_schema_with_config(imexplore_app(), schema, config);
    app.set_text_value("image_path", "/tmp/fake.image");
    app.start_run_for_test();

    app.handle_key_event(KeyEvent::new(KeyCode::Char('P'), KeyModifiers::SHIFT));
    assert_eq!(
        app.image_pinned_probe_labels_for_test(),
        vec!["P1".to_string()]
    );
    assert_eq!(app.selected_image_pinned_probe_label_for_test(), None);

    app.handle_key_event(KeyEvent::new(KeyCode::Right, KeyModifiers::NONE));
    app.handle_key_event(KeyEvent::new(KeyCode::Char('P'), KeyModifiers::SHIFT));
    assert_eq!(
        app.image_pinned_probe_labels_for_test(),
        vec!["P1".to_string(), "P2".to_string()]
    );
    assert!(
        app.browser_inspector_lines()
            .unwrap_or_default()
            .iter()
            .any(|line| line.contains("Pinned probes: 2"))
    );

    app.handle_key_event(KeyEvent::new(KeyCode::Char('n'), KeyModifiers::NONE));
    assert_eq!(
        app.selected_image_pinned_probe_label_for_test(),
        Some("P1".to_string())
    );
    assert_eq!(
        app.image_browser_snapshot_for_test()
            .expect("snapshot after selecting probe")
            .probe
            .as_ref()
            .expect("probe after selecting probe")
            .pixel_indices,
        vec![1, 1, 0]
    );

    app.handle_key_event(KeyEvent::new(KeyCode::Esc, KeyModifiers::NONE));
    assert_eq!(app.selected_image_pinned_probe_label_for_test(), None);

    app.handle_key_event(KeyEvent::new(KeyCode::Char('n'), KeyModifiers::NONE));
    app.handle_key_event(KeyEvent::new(KeyCode::Char('u'), KeyModifiers::NONE));
    assert_eq!(
        app.image_pinned_probe_labels_for_test(),
        vec!["P2".to_string()]
    );
    assert_eq!(app.selected_image_pinned_probe_label_for_test(), None);
}

#[cfg(unix)]
#[test]
fn browser_session_reports_structured_open_errors() {
    let _guard = launcher_env_lock();
    let temp = tempdir().expect("tempdir");
    let error_json = serde_json::to_string(&BrowserResponseEnvelope::error(
        "open_root_failed",
        "fake open failure",
    ))
    .expect("serialize error response");
    let script = write_fake_tablebrowser_script(temp.path(), &[error_json], None);
    set_tablebrowser_launcher_bin(&script);

    let schema = tablebrowser_app()
        .load_schema()
        .expect("load fake tablebrowser schema");
    let config = ConfigStore::load_for_tests(temp.path().join("casars.toml"));
    let mut app = AppState::from_schema_with_config(tablebrowser_app(), schema, config);
    app.set_text_value("table_path", "/tmp/fake.ms");
    app.start_run_for_test();

    assert!(
        app.status_line_for_test()
            .contains("Failed to open table browser")
    );
    assert!(
        app.stderr_for_test()
            .contains("open_root_failed: fake open failure")
    );
    let rendered = render_app(&app, 160, 28);
    assert!(rendered.contains("open_root_failed: fake open failure"));
}

#[cfg(unix)]
#[test]
fn browser_session_reports_malformed_backend_responses() {
    let _guard = launcher_env_lock();
    let temp = tempdir().expect("tempdir");
    let script = write_fake_tablebrowser_script(temp.path(), &[], Some("not-json".to_string()));
    set_tablebrowser_launcher_bin(&script);

    let schema = tablebrowser_app()
        .load_schema()
        .expect("load fake tablebrowser schema");
    let config = ConfigStore::load_for_tests(temp.path().join("casars.toml"));
    let mut app = AppState::from_schema_with_config(tablebrowser_app(), schema, config);
    app.set_text_value("table_path", "/tmp/fake.ms");
    app.start_run_for_test();

    assert!(
        app.status_line_for_test()
            .contains("Failed to open table browser")
    );
    assert!(app.stderr_for_test().contains("invalid_response"));
    let rendered = render_app(&app, 160, 28);
    assert!(rendered.contains("invalid_response"));
}

#[cfg(unix)]
#[test]
fn browser_command_errors_close_the_session_and_surface_stderr() {
    let _guard = launcher_env_lock();
    let temp = tempdir().expect("tempdir");
    let open_snapshot = fake_browser_snapshot_json(
        ProtocolBrowserView::Overview,
        "Opened",
        vec!["Overview root".to_string()],
    );
    let command_error = serde_json::to_string(&BrowserResponseEnvelope::error(
        "browser_error",
        "fake command failure",
    ))
    .expect("serialize browser error");
    let script = write_fake_tablebrowser_script(temp.path(), &[open_snapshot, command_error], None);
    set_tablebrowser_launcher_bin(&script);

    let schema = tablebrowser_app()
        .load_schema()
        .expect("load fake tablebrowser schema");
    let config = ConfigStore::load_for_tests(temp.path().join("casars.toml"));
    let mut app = AppState::from_schema_with_config(tablebrowser_app(), schema, config);
    app.set_text_value("table_path", "/tmp/fake.ms");
    app.start_run_for_test();

    assert!(app.browser_is_active());
    app.handle_key_event(KeyEvent::new(KeyCode::Char(']'), KeyModifiers::NONE));

    assert!(!app.browser_is_active());
    assert!(
        app.status_line_for_test()
            .contains("Browser command failed. Session closed.")
    );
    assert!(
        app.stderr_for_test()
            .contains("browser_error: fake command failure")
    );
    let rendered = render_app(&app, 160, 28);
    assert!(rendered.contains("browser_error: fake command failure"));
}

#[test]
fn verbose_off_hides_detail_tabs() {
    let (_temp, mut app) = test_app();
    app.set_toggle_value("verbose", false);
    let rendered = render_app(&app, 160, 30);
    assert!(rendered.contains("Overview"));
    assert!(rendered.contains("[Observations]"));
    assert!(rendered.contains("[Fields]"));
    assert!(rendered.contains("[SPWs]"));
    assert!(rendered.contains("[Antennas]"));
    assert!(rendered.contains("[Stdout]"));
    assert!(rendered.contains("[Stderr]"));
    assert!(!rendered.contains("[Scans]"));
    assert!(!rendered.contains("[Sources]"));
}

#[test]
fn selected_section_keeps_its_disclosure_glyph() {
    let (_temp, mut app) = test_app();
    app.handle_key_event(KeyEvent::new(KeyCode::Char('t'), KeyModifiers::NONE));
    let layout = ui::compute_layout(ratatui::layout::Rect::new(0, 0, 100, 30), &app);
    let presentation = layout
        .form_rows
        .iter()
        .find(|row| row.rect.y == 4)
        .expect("presentation row");
    app.handle_mouse_event(
        mouse(
            MouseEventKind::Down(MouseButton::Left),
            presentation.rect.x + 2,
            presentation.rect.y,
        ),
        &layout,
    );
    let rendered = render_app(&app, 100, 30);
    assert!(rendered.contains("▸ Presentation") || rendered.contains("▾ Presentation"));
}

#[test]
fn executes_listobs_and_parses_structured_output_into_tabs() {
    let _guard = launcher_env_lock();
    clear_launcher_bin();

    let temp = tempdir().expect("tempdir");
    let ms_path = create_fixture_ms(temp.path());

    let schema = listobs_app()
        .load_schema()
        .expect("load schema from listobs");
    let config = ConfigStore::load_for_tests(temp.path().join("casars.toml"));
    let mut app = AppState::from_schema_with_config(listobs_app(), schema, config);
    app.set_text_value("ms_path", ms_path.to_string_lossy().as_ref());
    app.start_run_for_test();
    assert!(app.wait_for_idle_for_test(Duration::from_secs(60)));
    assert!(
        app.status_line_for_test()
            .contains("completed successfully")
    );

    let summary = app.structured_for_test().expect("structured summary");
    assert_eq!(summary.measurement_set.row_count, 2);
    assert_eq!(summary.fields[0].name, "3C286");
    let rendered = render_app(&app, 120, 32);
    assert!(rendered.contains("Overview"));
    assert!(rendered.contains("Fields"));
    assert!(rendered.contains("MeasurementSet"));
}

#[test]
fn plots_tab_loads_uv_coverage_after_listobs_run() {
    let _guard = launcher_env_lock();
    clear_launcher_bin();

    let temp = tempdir().expect("tempdir");
    let ms_path = create_fixture_ms(temp.path());

    let schema = listobs_app()
        .load_schema()
        .expect("load schema from listobs");
    let config = ConfigStore::load_for_tests(temp.path().join("casars.toml"));
    let mut app = AppState::from_schema_with_config(listobs_app(), schema, config);
    app.set_text_value("ms_path", ms_path.to_string_lossy().as_ref());
    app.start_run_for_test();
    assert!(app.wait_for_idle_for_test(Duration::from_secs(60)));
    assert!(app.uv_coverage_for_test().is_none());

    app.set_active_result_tab(ResultTab::Plots);
    app.prepare_graphics_for_test(140, 32);
    assert!(
        wait_for_plot_render(&mut app, 140, 32, Duration::from_secs(5)),
        "status={} pending={} last_error={:?} stderr={}",
        app.status_line_for_test(),
        app.plot_pending(),
        app.plot_last_error(),
        app.stderr_for_test()
    );

    let coverage = app.uv_coverage_for_test().expect("uv coverage");
    assert!(coverage.sample_count > 0);
    assert_eq!(app.selected_plot_kind(), ListObsPlotKind::UvCoverage);
    assert_eq!(app.plot_focus(), PlotPaneFocus::Catalog);
    assert!(
        app.plot_protocol().is_some() || app.plot_pending(),
        "pending={} last_error={:?} stderr={}",
        app.plot_pending(),
        app.plot_last_error(),
        app.stderr_for_test()
    );
    match app.active_result_content() {
        crate::app::ResultContent::Graphic(summary) => {
            assert!(summary.contains("UV Coverage from run 1."));
        }
        other => panic!("expected graphic result, got {other:?}"),
    }
    let rendered = render_app(&app, 140, 32);
    assert!(rendered.contains("Catalog"));
    assert!(rendered.contains("Controls"));
    assert!(rendered.contains("UV Coverage"));
}

#[test]
fn plots_tab_catalog_lists_raw_visibility_plot_kinds() {
    let (_temp, mut app) = test_app();
    app.set_active_result_tab(ResultTab::Plots);

    let rendered = render_app(&app, 180, 36);
    assert!(rendered.contains("Amplitude vs Time"));
    assert!(rendered.contains("Phase vs Time"));
    assert!(rendered.contains("Amplitude vs UV Distance"));
}

#[test]
fn msexplore_plots_tab_previews_current_form_without_run() {
    let _guard = launcher_env_lock();
    clear_launcher_bin();

    let (_fixture_temp, ms_path) = unpack_casacore_ms_fixture("mssel_test_small.ms.tgz");
    let temp = tempdir().expect("tempdir");
    let schema = msexplore_command_schema("msexplore");
    let config = ConfigStore::load_for_tests(temp.path().join("casars.toml"));
    let mut app = AppState::from_schema_with_config(msexplore_app(), schema, config);
    app.set_text_value("ms_path", ms_path.to_string_lossy().as_ref());
    app.set_text_value("preset", "amplitude_vs_time");

    app.set_active_result_tab(ResultTab::Plots);
    app.prepare_graphics_for_test(140, 32);
    assert!(
        wait_for_plot_render(&mut app, 140, 32, Duration::from_secs(5)),
        "status={} pending={} last_error={:?} stderr={}",
        app.status_line_for_test(),
        app.plot_pending(),
        app.plot_last_error(),
        app.stderr_for_test()
    );

    assert!(
        app.plot_protocol().is_some() || app.plot_pending(),
        "pending={} last_error={:?} stderr={}",
        app.plot_pending(),
        app.plot_last_error(),
        app.stderr_for_test()
    );
    match app.active_result_content() {
        crate::app::ResultContent::Graphic(summary) => {
            assert!(summary.contains("current msexplore form"));
        }
        other => panic!("expected graphic result, got {other:?}"),
    }
    let rendered = render_app(&app, 140, 32);
    assert!(rendered.contains("Amplitude vs Time"));
    assert!(rendered.contains("Presets"));
    assert!(rendered.contains("Actions"));
    assert!(rendered.contains("Refresh Preview"));
    assert!(!rendered.contains("Controls"));
    assert!(!rendered.contains("Run listobs"));
}

#[test]
fn msexplore_start_run_on_launch_opens_plots_preview_instead_of_spawning_process() {
    let _guard = launcher_env_lock();
    clear_launcher_bin();

    let (_fixture_temp, ms_path) = unpack_casacore_ms_fixture("mssel_test_small.ms.tgz");
    let temp = tempdir().expect("tempdir");
    let schema = msexplore_command_schema("msexplore");
    let config = ConfigStore::load_for_tests(temp.path().join("casars.toml"));
    let mut app = AppState::from_schema_with_config(msexplore_app(), schema, config);
    app.set_text_value("ms_path", ms_path.to_string_lossy().as_ref());
    app.set_text_value("preset", "amplitude_vs_time");

    app.start_run_on_launch();

    assert!(!app.is_running_for_test());
    assert_eq!(app.active_result_tab(), ResultTab::Plots);
    assert_eq!(app.pane_focus_for_test(), PaneFocus::Result);
    assert!(
        app.status_line_for_test()
            .contains("Opening interactive msexplore preview"),
        "status={}",
        app.status_line_for_test()
    );

    app.prepare_graphics_for_test(140, 32);
    assert!(
        wait_for_plot_render(&mut app, 140, 32, Duration::from_secs(5)),
        "status={} pending={} last_error={:?} stderr={}",
        app.status_line_for_test(),
        app.plot_pending(),
        app.plot_last_error(),
        app.stderr_for_test()
    );
    assert!(app.plot_protocol().is_some() || app.plot_pending());
}

#[test]
fn msexplore_summary_tabs_populate_from_current_form_without_subprocess_run() {
    let _guard = launcher_env_lock();
    clear_launcher_bin();

    let temp = tempdir().expect("tempdir");
    let ms_path = create_fixture_ms(temp.path());

    let schema = msexplore_command_schema("msexplore");
    let config = ConfigStore::load_for_tests(temp.path().join("casars.toml"));
    let mut app = AppState::from_schema_with_config(msexplore_app(), schema, config);
    app.set_text_value("ms_path", ms_path.to_string_lossy().as_ref());

    app.set_active_result_tab(ResultTab::Observations);

    let summary = app.structured_for_test().expect("structured summary");
    assert_eq!(summary.measurement_set.row_count, 2);
    let rendered = render_app(&app, 220, 32);
    assert!(rendered.contains("Observations"));
    assert!(rendered.contains("Start"));
    assert!(rendered.contains("End"));
}

#[test]
fn msexplore_plots_tab_copy_cli_and_export_png_use_current_form() {
    let _guard = launcher_env_lock();
    clear_launcher_bin();

    let (_fixture_temp, ms_path) = unpack_casacore_ms_fixture("mssel_test_small.ms.tgz");
    let temp = tempdir().expect("tempdir");
    let export_path = temp.path().join("msexplore-preview.png");
    let clipboard_path = temp.path().join("clipboard.txt");
    set_test_clipboard_file(&clipboard_path);

    let schema = msexplore_command_schema("msexplore");
    let config = ConfigStore::load_for_tests(temp.path().join("casars.toml"));
    let mut app = AppState::from_schema_with_config(msexplore_app(), schema, config);
    app.set_text_value("ms_path", ms_path.to_string_lossy().as_ref());
    app.set_text_value("preset", "amplitude_vs_time");
    app.set_text_value("plot_output", export_path.to_string_lossy().as_ref());

    app.set_active_result_tab(ResultTab::Plots);
    app.prepare_graphics_for_test(140, 32);
    assert!(wait_for_plot_render(
        &mut app,
        140,
        32,
        Duration::from_secs(5)
    ));

    let layout = ui::compute_layout(ratatui::layout::Rect::new(0, 0, 140, 32), &app);
    let first_control = layout
        .plot_workspace
        .as_ref()
        .expect("plot workspace")
        .control_hits
        .first()
        .expect("plot control");
    app.handle_mouse_event(
        mouse(
            MouseEventKind::Down(MouseButton::Left),
            first_control.rect.x + 1,
            first_control.rect.y,
        ),
        &layout,
    );

    move_plot_control_selection_to(&mut app, PlotControlTarget::CopyCli);
    app.handle_key_event(KeyEvent::new(KeyCode::Enter, KeyModifiers::NONE));
    let clipboard = std::fs::read_to_string(&clipboard_path).unwrap_or_else(|error| {
        panic!(
            "clipboard contents: {error} status={} stderr={}",
            app.status_line_for_test(),
            app.stderr_for_test()
        )
    });
    assert!(clipboard.contains("msexplore"));
    assert!(clipboard.contains("--preset amplitude_vs_time"));
    assert!(clipboard.contains("--showlegend"));
    assert!(clipboard.contains("--legendposition exteriorRight"));
    assert!(clipboard.contains(ms_path.to_string_lossy().as_ref()));
    assert!(clipboard.contains(export_path.to_string_lossy().as_ref()));
    assert!(!clipboard.contains("--flag-action"));

    move_plot_control_selection_to(&mut app, PlotControlTarget::ExportPng);
    app.handle_key_event(KeyEvent::new(KeyCode::Enter, KeyModifiers::NONE));
    assert!(export_path.is_file());
    let png = std::fs::read(&export_path).expect("png bytes");
    assert!(png.starts_with(b"\x89PNG\r\n\x1a\n"));
    clear_test_clipboard_file();
}

#[test]
fn msexplore_plots_sidebar_lists_standard_presets() {
    let temp = tempdir().expect("tempdir");
    let schema = msexplore_command_schema("msexplore");
    let config = ConfigStore::load_for_tests(temp.path().join("casars.toml"));
    let app = AppState::from_schema_with_config(msexplore_app(), schema, config);

    let rows = app.plot_catalog_rows();
    assert_eq!(rows.len(), 26);
    let labels = rows.into_iter().map(|row| row.label).collect::<Vec<_>>();
    assert!(labels.contains(&"Amplitude vs Time".to_string()));
    assert!(labels.contains(&"Phase vs Time".to_string()));
    assert!(labels.contains(&"Amplitude / Phase vs Time (Stacked)".to_string()));
    assert!(labels.contains(&"Amplitude vs Velocity".to_string()));
    assert!(labels.contains(&"Real vs Imaginary".to_string()));
    assert_eq!(
        app.field_text_for_test("legendposition").as_deref(),
        Some("exteriorRight")
    );
}

#[test]
fn msexplore_plots_sidebar_shows_overflow_indicator_for_hidden_presets() {
    let temp = tempdir().expect("tempdir");
    let schema = msexplore_command_schema("msexplore");
    let config = ConfigStore::load_for_tests(temp.path().join("casars.toml"));
    let mut app = AppState::from_schema_with_config(msexplore_app(), schema, config);
    app.set_active_result_tab(ResultTab::Plots);

    let layout = ui::compute_layout(ratatui::layout::Rect::new(0, 0, 140, 32), &app);
    let visible_rows = layout
        .plot_workspace
        .as_ref()
        .expect("plot workspace")
        .catalog_hits
        .len();
    let total_rows = app.plot_catalog_rows().len();
    assert!(visible_rows < total_rows);

    let rendered = render_app(&app, 140, 32);
    assert!(rendered.contains("more"));
    assert!(!rendered.contains("Amplitude vs Velocity"));
}

#[test]
fn msexplore_clicking_catalog_preset_updates_preview_cli() {
    let _guard = launcher_env_lock();
    clear_launcher_bin();

    let (_fixture_temp, ms_path) = unpack_casacore_ms_fixture("mssel_test_small.ms.tgz");
    let temp = tempdir().expect("tempdir");
    let clipboard_path = temp.path().join("clipboard.txt");
    set_test_clipboard_file(&clipboard_path);

    let schema = msexplore_command_schema("msexplore");
    let config = ConfigStore::load_for_tests(temp.path().join("casars.toml"));
    let mut app = AppState::from_schema_with_config(msexplore_app(), schema, config);
    app.set_text_value("ms_path", ms_path.to_string_lossy().as_ref());
    app.set_active_result_tab(ResultTab::Plots);

    let layout = ui::compute_layout(ratatui::layout::Rect::new(0, 0, 160, 40), &app);
    let phase_hit = layout
        .plot_workspace
        .as_ref()
        .expect("plot workspace")
        .catalog_hits
        .iter()
        .find(|hit| {
            hit.tab.target
                == PlotCatalogTarget::MsExplorePreset(casacore_ms::MsPlotPreset::PhaseVsTime)
        })
        .expect("phase preset hit");
    app.handle_mouse_event(
        mouse(
            MouseEventKind::Down(MouseButton::Left),
            phase_hit.rect.x + 1,
            phase_hit.rect.y,
        ),
        &layout,
    );

    assert_eq!(
        app.field_text_for_test("preset").as_deref(),
        Some("phase_vs_time")
    );

    app.prepare_graphics_for_test(160, 40);
    assert!(wait_for_plot_render(
        &mut app,
        160,
        40,
        Duration::from_secs(5)
    ));

    let layout = ui::compute_layout(ratatui::layout::Rect::new(0, 0, 160, 40), &app);
    let first_control = layout
        .plot_workspace
        .as_ref()
        .expect("plot workspace")
        .control_hits
        .first()
        .expect("plot control");
    app.handle_mouse_event(
        mouse(
            MouseEventKind::Down(MouseButton::Left),
            first_control.rect.x + 1,
            first_control.rect.y,
        ),
        &layout,
    );

    move_plot_control_selection_to(&mut app, PlotControlTarget::CopyCli);
    app.handle_key_event(KeyEvent::new(KeyCode::Enter, KeyModifiers::NONE));
    let clipboard = std::fs::read_to_string(&clipboard_path).expect("clipboard contents");
    assert!(clipboard.contains("--preset phase_vs_time"));
    assert!(clipboard.contains("--showlegend"));
    assert!(clipboard.contains("--legendposition exteriorRight"));
    clear_test_clipboard_file();
}

#[test]
fn msexplore_selecting_preset_immediately_invalidates_existing_preview() {
    let (_fixture_temp, ms_path) = unpack_casacore_ms_fixture("mssel_test_small.ms.tgz");
    let temp = tempdir().expect("tempdir");
    let schema = msexplore_command_schema("msexplore");
    let config = ConfigStore::load_for_tests(temp.path().join("casars.toml"));
    let mut app = AppState::from_schema_with_config(msexplore_app(), schema, config);
    app.set_text_value("ms_path", ms_path.to_string_lossy().as_ref());
    app.set_text_value("preset", "amplitude_vs_time");
    app.set_active_result_tab(ResultTab::Plots);

    app.prepare_graphics_for_test(160, 40);
    assert!(wait_for_plot_render(
        &mut app,
        160,
        40,
        Duration::from_secs(5)
    ));
    assert!(
        app.plot_protocol().is_some(),
        "{}",
        app.status_line_for_test()
    );

    let layout = ui::compute_layout(ratatui::layout::Rect::new(0, 0, 160, 40), &app);
    let target =
        PlotCatalogTarget::MsExplorePreset(casacore_ms::MsPlotPreset::AmplitudePhaseVsTimeStacked);
    let hit = layout
        .plot_workspace
        .as_ref()
        .expect("plot workspace")
        .catalog_hits
        .iter()
        .find(|hit| hit.tab.target == target)
        .expect("stacked preset hit");
    app.handle_mouse_event(
        mouse(
            MouseEventKind::Down(MouseButton::Left),
            hit.rect.x + 1,
            hit.rect.y,
        ),
        &layout,
    );

    assert_eq!(
        app.field_text_for_test("preset").as_deref(),
        Some("amplitude_phase_vs_time_stacked")
    );
    assert!(app.plot_protocol().is_none());
    assert!(app.plot_pending());
    assert!(
        app.status_line_for_test()
            .contains("Selected Amplitude / Phase vs Time (Stacked). Rendering preview"),
        "{}",
        app.status_line_for_test()
    );

    let rendered = render_app(&app, 160, 40);
    assert!(rendered.contains("Amplitude / Phase vs Time (Stacked)"));
    assert!(rendered.contains("Rendering plot..."));
}

#[test]
fn msexplore_catalog_scroll_selects_velocity_preset_with_down_arrow() {
    let temp = tempdir().expect("tempdir");
    let schema = msexplore_command_schema("msexplore");
    let config = ConfigStore::load_for_tests(temp.path().join("casars.toml"));
    let mut app = AppState::from_schema_with_config(msexplore_app(), schema, config);
    app.set_active_result_tab(ResultTab::Plots);

    let all_rows = app.plot_catalog_rows();
    let layout = ui::compute_layout(ratatui::layout::Rect::new(0, 0, 140, 32), &app);
    let last_visible_hit = layout
        .plot_workspace
        .as_ref()
        .expect("plot workspace")
        .catalog_hits
        .last()
        .expect("last visible preset");
    app.handle_mouse_event(
        mouse(
            MouseEventKind::Down(MouseButton::Left),
            last_visible_hit.rect.x + 1,
            last_visible_hit.rect.y,
        ),
        &layout,
    );
    let current_index = all_rows
        .iter()
        .position(|row| row.target == last_visible_hit.tab.target)
        .expect("visible row index");
    let velocity_index = all_rows
        .iter()
        .position(|row| {
            row.target
                == PlotCatalogTarget::MsExplorePreset(
                    casacore_ms::MsPlotPreset::AmplitudeVsVelocity,
                )
        })
        .expect("velocity preset index");
    assert!(velocity_index > current_index);

    for _ in current_index..velocity_index {
        app.handle_key_event(KeyEvent::new(KeyCode::Down, KeyModifiers::NONE));
    }
    assert_eq!(
        app.field_text_for_test("preset").as_deref(),
        Some("amplitude_vs_velocity")
    );

    let rendered = render_app(&app, 140, 32);
    assert!(rendered.contains("▶ Amplitude vs Velocity"));
}

#[test]
fn msexplore_problem_presets_can_be_clicked_when_visible() {
    let temp = tempdir().expect("tempdir");
    let schema = msexplore_app()
        .load_schema()
        .expect("load live msexplore schema");
    let config = ConfigStore::load_for_tests(temp.path().join("casars.toml"));
    let mut app = AppState::from_schema_with_config(msexplore_app(), schema, config);
    app.set_active_result_tab(ResultTab::Plots);

    let layout = ui::compute_layout(ratatui::layout::Rect::new(0, 0, 160, 52), &app);
    let workspace = layout.plot_workspace.as_ref().expect("plot workspace");
    let targets = [
        (
            PlotCatalogTarget::MsExplorePreset(
                casacore_ms::MsPlotPreset::AmplitudePhaseVsTimeStacked,
            ),
            "amplitude_phase_vs_time_stacked",
        ),
        (
            PlotCatalogTarget::MsExplorePreset(casacore_ms::MsPlotPreset::AmplitudeVsVelocity),
            "amplitude_vs_velocity",
        ),
        (
            PlotCatalogTarget::MsExplorePreset(casacore_ms::MsPlotPreset::PhaseVsVelocity),
            "phase_vs_velocity",
        ),
    ];

    for (target, expected) in targets {
        let hit = workspace
            .catalog_hits
            .iter()
            .find(|hit| hit.tab.target == target)
            .expect("preset hit");
        app.handle_mouse_event(
            mouse(
                MouseEventKind::Down(MouseButton::Left),
                hit.rect.x + 1,
                hit.rect.y,
            ),
            &layout,
        );
        assert_eq!(app.field_text_for_test("preset").as_deref(), Some(expected));
    }
}

#[test]
fn msexplore_problem_presets_show_explicit_selected_marker() {
    let temp = tempdir().expect("tempdir");
    let schema = msexplore_app()
        .load_schema()
        .expect("load live msexplore schema");
    let config = ConfigStore::load_for_tests(temp.path().join("casars.toml"));
    let mut app = AppState::from_schema_with_config(msexplore_app(), schema, config);
    app.set_active_result_tab(ResultTab::Plots);

    let layout = ui::compute_layout(ratatui::layout::Rect::new(0, 0, 160, 52), &app);
    let workspace = layout.plot_workspace.as_ref().expect("plot workspace");
    let target = workspace
        .catalog_hits
        .iter()
        .find(|hit| {
            hit.tab.target
                == PlotCatalogTarget::MsExplorePreset(
                    casacore_ms::MsPlotPreset::AmplitudeVsVelocity,
                )
        })
        .expect("velocity preset hit");
    app.handle_mouse_event(
        mouse(
            MouseEventKind::Down(MouseButton::Left),
            target.rect.x + 1,
            target.rect.y,
        ),
        &layout,
    );

    let rendered = render_app(&app, 160, 52);
    assert!(rendered.contains("▶ Amplitude vs Velocity"));
}

#[test]
fn divider_chevron_can_collapse_parameters_sidebar_from_plots() {
    let (_temp, mut app) = test_app();
    app.set_active_result_tab(ResultTab::Plots);

    let before = ui::compute_layout(ratatui::layout::Rect::new(0, 0, 160, 34), &app);
    assert!(!app.parameters_pane_collapsed());
    app.handle_mouse_event(
        mouse(
            MouseEventKind::Down(MouseButton::Left),
            before.divider.x,
            before.divider.y,
        ),
        &before,
    );

    let after = ui::compute_layout(ratatui::layout::Rect::new(0, 0, 160, 34), &app);
    assert!(app.parameters_pane_collapsed());
    assert_eq!(after.divider.width, 1);
    assert!(after.result_block.width > before.result_block.width);
}

#[test]
fn plots_tab_shows_dirty_banner_and_copy_cli_uses_last_successful_run() {
    let _guard = launcher_env_lock();
    clear_launcher_bin();

    let temp = tempdir().expect("tempdir");
    let first_ms = create_fixture_ms(temp.path());
    let second_root = temp.path().join("second");
    std::fs::create_dir(&second_root).expect("create second root");
    let second_ms = create_fixture_ms(&second_root);
    let clipboard_path = temp.path().join("clipboard.txt");
    set_test_clipboard_file(&clipboard_path);

    let schema = listobs_app()
        .load_schema()
        .expect("load schema from listobs");
    let config = ConfigStore::load_for_tests(temp.path().join("casars.toml"));
    let mut app = AppState::from_schema_with_config(listobs_app(), schema, config);
    app.set_text_value("ms_path", first_ms.to_string_lossy().as_ref());
    app.start_run_for_test();
    assert!(app.wait_for_idle_for_test(Duration::from_secs(60)));

    app.set_active_result_tab(ResultTab::Plots);
    app.prepare_graphics_for_test(140, 32);
    assert!(wait_for_plot_render(
        &mut app,
        140,
        32,
        Duration::from_secs(5)
    ));

    app.set_text_value("ms_path", second_ms.to_string_lossy().as_ref());
    assert!(app.plot_snapshot_dirty_for_test());

    let rendered = render_app(&app, 160, 34);
    assert!(rendered.contains("Plots reflect the last successful run"));

    let layout = ui::compute_layout(ratatui::layout::Rect::new(0, 0, 160, 34), &app);
    let visible_control_hit = layout
        .plot_workspace
        .as_ref()
        .expect("plot workspace")
        .control_hits
        .iter()
        .last()
        .expect("visible control hit");
    app.handle_mouse_event(
        mouse(
            MouseEventKind::Down(MouseButton::Left),
            visible_control_hit.rect.x + 1,
            visible_control_hit.rect.y,
        ),
        &layout,
    );
    move_plot_control_selection_to(&mut app, PlotControlTarget::CopyCli);
    app.handle_key_event(KeyEvent::new(KeyCode::Enter, KeyModifiers::NONE));

    let clipboard = std::fs::read_to_string(&clipboard_path).expect("clipboard contents");
    let first_ms_display = first_ms.to_string_lossy();
    let second_ms_display = second_ms.to_string_lossy();
    assert!(clipboard.contains("--plot uv_coverage"));
    assert!(clipboard.contains(first_ms_display.as_ref()));
    assert!(!clipboard.contains(second_ms_display.as_ref()));
    assert!(
        app.status_line_for_test()
            .contains("Copied plot CLI to clipboard.")
    );
    clear_test_clipboard_file();
}

#[test]
fn plot_workspace_mouse_selection_and_export_pdf_work() {
    let _guard = launcher_env_lock();
    clear_launcher_bin();

    let temp = tempdir().expect("tempdir");
    let ms_path = create_fixture_ms(temp.path());
    let export_path = temp.path().join("antenna-layout-check.pdf");

    let schema = listobs_app()
        .load_schema()
        .expect("load schema from listobs");
    let config = ConfigStore::load_for_tests(temp.path().join("casars.toml"));
    let mut app = AppState::from_schema_with_config(listobs_app(), schema, config);
    app.set_text_value("ms_path", ms_path.to_string_lossy().as_ref());
    app.start_run_for_test();
    assert!(app.wait_for_idle_for_test(Duration::from_secs(60)));

    app.set_active_result_tab(ResultTab::Plots);
    app.prepare_graphics_for_test(160, 36);
    assert!(wait_for_plot_render(
        &mut app,
        160,
        36,
        Duration::from_secs(5)
    ));

    let layout = ui::compute_layout(ratatui::layout::Rect::new(0, 0, 160, 36), &app);
    let antenna_hit = layout
        .plot_workspace
        .as_ref()
        .expect("plot workspace")
        .catalog_hits
        .iter()
        .find(|hit| hit.tab.target == PlotCatalogTarget::ListObs(ListObsPlotKind::AntennaLayout))
        .expect("antenna hit");
    app.handle_mouse_event(
        mouse(
            MouseEventKind::Down(MouseButton::Left),
            antenna_hit.rect.x + 1,
            antenna_hit.rect.y,
        ),
        &layout,
    );
    assert_eq!(app.selected_plot_kind(), ListObsPlotKind::AntennaLayout);
    assert_eq!(app.plot_focus(), PlotPaneFocus::Catalog);

    let layout = ui::compute_layout(ratatui::layout::Rect::new(0, 0, 160, 36), &app);
    let export_path_hit = layout
        .plot_workspace
        .as_ref()
        .expect("plot workspace")
        .control_hits
        .iter()
        .find(|hit| hit.target == PlotControlTarget::ExportPath)
        .expect("export path hit");
    app.handle_mouse_event(
        mouse(
            MouseEventKind::Down(MouseButton::Left),
            export_path_hit.rect.x + 1,
            export_path_hit.rect.y,
        ),
        &layout,
    );
    assert_eq!(app.plot_focus(), PlotPaneFocus::Controls);
    app.handle_key_event(KeyEvent::new(KeyCode::Enter, KeyModifiers::NONE));
    let existing = app.edit_buffer_for_test().expect("plot export path editor");
    for _ in 0..existing.chars().count() {
        app.handle_key_event(KeyEvent::new(KeyCode::Backspace, KeyModifiers::NONE));
    }
    app.handle_paste(export_path.display().to_string());
    app.handle_key_event(KeyEvent::new(KeyCode::Enter, KeyModifiers::NONE));

    move_plot_control_selection_to(&mut app, PlotControlTarget::ExportPdf);
    app.handle_key_event(KeyEvent::new(KeyCode::Enter, KeyModifiers::NONE));
    assert!(export_path.is_file());
    let pdf = std::fs::read(&export_path).expect("pdf bytes");
    assert!(pdf.starts_with(b"%PDF-"));
    assert!(app.status_line_for_test().contains("Saved"));
}

#[test]
fn verbose_on_exposes_scans_and_sources_tabs() {
    let (_temp, app) = test_app();
    let rendered = render_app(&app, 140, 30);
    assert!(rendered.contains("[Scans]"));
    assert!(rendered.contains("[Sources]"));
}

#[test]
fn spw_table_shows_channel_and_total_bandwidth() {
    let _guard = launcher_env_lock();
    clear_launcher_bin();

    let temp = tempdir().expect("tempdir");
    let ms_path = create_fixture_ms(temp.path());

    let schema = listobs_app()
        .load_schema()
        .expect("load schema from listobs");
    let config = ConfigStore::load_for_tests(temp.path().join("casars.toml"));
    let mut app = AppState::from_schema_with_config(listobs_app(), schema, config);
    app.set_text_value("ms_path", ms_path.to_string_lossy().as_ref());
    app.start_run_for_test();
    assert!(app.wait_for_idle_for_test(Duration::from_secs(60)));

    app.set_active_result_tab(ResultTab::Spws);
    let rendered = render_app(&app, 220, 32);
    assert!(rendered.contains("Ch0(MHz)"));
    assert!(rendered.contains("ChanWid(kHz)"));
    assert!(rendered.contains("TotBW(kHz)"));
    assert!(rendered.contains("CtrFreq(MHz)"));
    assert!(rendered.contains("1000.000"));
    assert!(rendered.contains("2000.0"));
}

#[test]
fn fields_table_shows_sky_position_columns() {
    let _guard = launcher_env_lock();
    clear_launcher_bin();

    let temp = tempdir().expect("tempdir");
    let ms_path = create_fixture_ms(temp.path());

    let schema = listobs_app()
        .load_schema()
        .expect("load schema from listobs");
    let config = ConfigStore::load_for_tests(temp.path().join("casars.toml"));
    let mut app = AppState::from_schema_with_config(listobs_app(), schema, config);
    app.set_text_value("ms_path", ms_path.to_string_lossy().as_ref());
    app.start_run_for_test();
    assert!(app.wait_for_idle_for_test(Duration::from_secs(60)));

    app.set_active_result_tab(ResultTab::Fields);
    let rendered = render_app(&app, 220, 32);
    assert!(rendered.contains("RA"));
    assert!(rendered.contains("Dec"));
    assert!(rendered.contains("Epoch"));
    assert!(rendered.contains("SrcId"));
}

#[test]
fn observations_table_shows_formatted_timestamps() {
    let _guard = launcher_env_lock();
    clear_launcher_bin();

    let temp = tempdir().expect("tempdir");
    let ms_path = create_fixture_ms(temp.path());

    let schema = listobs_app()
        .load_schema()
        .expect("load schema from listobs");
    let config = ConfigStore::load_for_tests(temp.path().join("casars.toml"));
    let mut app = AppState::from_schema_with_config(listobs_app(), schema, config);
    app.set_text_value("ms_path", ms_path.to_string_lossy().as_ref());
    app.start_run_for_test();
    assert!(app.wait_for_idle_for_test(Duration::from_secs(60)));

    app.set_active_result_tab(ResultTab::Observations);
    let rendered = render_app(&app, 220, 32);
    let expected = MvTime::from_mjd_seconds(4_981_000_000.0).format_dmy(1);
    assert!(rendered.contains("Start"));
    assert!(rendered.contains("End"));
    assert!(rendered.contains(&expected));
    assert!(!rendered.contains("4981000000.000"));
}

#[test]
fn sources_table_shows_rest_frequency_and_velocity() {
    let _guard = launcher_env_lock();
    clear_launcher_bin();

    let temp = tempdir().expect("tempdir");
    let ms_path = create_fixture_ms(temp.path());

    let schema = listobs_app()
        .load_schema()
        .expect("load schema from listobs");
    let config = ConfigStore::load_for_tests(temp.path().join("casars.toml"));
    let mut app = AppState::from_schema_with_config(listobs_app(), schema, config);
    app.set_text_value("ms_path", ms_path.to_string_lossy().as_ref());
    app.start_run_for_test();
    assert!(app.wait_for_idle_for_test(Duration::from_secs(60)));

    app.set_active_result_tab(ResultTab::Sources);
    let rendered = render_app(&app, 160, 32);
    assert!(rendered.contains("RestFreq(MHz)"));
    assert!(rendered.contains("SysVel(km/s)"));
}

#[test]
fn antenna_verbose_table_shows_geodetic_and_itrf_columns() {
    let _guard = launcher_env_lock();
    clear_launcher_bin();

    let temp = tempdir().expect("tempdir");
    let ms_path = create_fixture_ms(temp.path());

    let schema = listobs_app()
        .load_schema()
        .expect("load schema from listobs");
    let config = ConfigStore::load_for_tests(temp.path().join("casars.toml"));
    let mut app = AppState::from_schema_with_config(listobs_app(), schema, config);
    app.set_text_value("ms_path", ms_path.to_string_lossy().as_ref());
    app.start_run_for_test();
    assert!(app.wait_for_idle_for_test(Duration::from_secs(60)));

    app.set_active_result_tab(ResultTab::Antennas);
    let left_rendered = render_app(&app, 160, 32);
    assert!(left_rendered.contains("Long."));
    assert!(left_rendered.contains("Lat."));

    let layout = ui::compute_layout(ratatui::layout::Rect::new(0, 0, 160, 32), &app);
    let scrollbar = layout
        .result_hscrollbar
        .expect("result horizontal scrollbar");
    app.handle_mouse_event(
        mouse(
            MouseEventKind::Down(MouseButton::Left),
            scrollbar.x + scrollbar.width.saturating_sub(1),
            scrollbar.y,
        ),
        &layout,
    );

    let right_rendered = render_app(&app, 160, 32);
    assert!(right_rendered.contains("ITRF x"));
    assert!(right_rendered.contains("ITRF y"));
    assert!(right_rendered.contains("ITRF z"));
}

#[test]
fn scans_table_shows_scan_metadata_columns() {
    let _guard = launcher_env_lock();
    clear_launcher_bin();

    let temp = tempdir().expect("tempdir");
    let ms_path = create_fixture_ms(temp.path());

    let schema = listobs_app()
        .load_schema()
        .expect("load schema from listobs");
    let config = ConfigStore::load_for_tests(temp.path().join("casars.toml"));
    let mut app = AppState::from_schema_with_config(listobs_app(), schema, config);
    app.set_text_value("ms_path", ms_path.to_string_lossy().as_ref());
    app.start_run_for_test();
    assert!(app.wait_for_idle_for_test(Duration::from_secs(60)));

    app.set_active_result_tab(ResultTab::Scans);
    let rendered = render_app(&app, 500, 32);
    assert!(rendered.contains("Scan"));
    assert!(rendered.contains("Field"));
    assert!(rendered.contains("Spws"));
    assert!(rendered.contains("Int(s)"));
    assert!(rendered.contains("Intent"));
}

#[test]
fn listunfl_adds_unflag_columns_to_tables() {
    let _guard = launcher_env_lock();
    clear_launcher_bin();

    let temp = tempdir().expect("tempdir");
    let ms_path = create_fixture_ms(temp.path());

    let schema = listobs_app()
        .load_schema()
        .expect("load schema from listobs");
    let config = ConfigStore::load_for_tests(temp.path().join("casars.toml"));
    let mut app = AppState::from_schema_with_config(listobs_app(), schema, config);
    app.set_text_value("ms_path", ms_path.to_string_lossy().as_ref());
    app.set_toggle_value("listunfl", true);
    app.start_run_for_test();
    assert!(app.wait_for_idle_for_test(Duration::from_secs(60)));

    app.set_active_result_tab(ResultTab::Fields);
    let fields = render_app(&app, 220, 32);
    assert!(fields.contains("nUnflRows"));

    app.set_active_result_tab(ResultTab::Scans);
    let scans = render_app(&app, 220, 32);
    assert!(scans.contains("nUnfl"));
}

#[test]
fn keyboard_horizontal_scroll_changes_result_offset() {
    let (_temp, mut app) = test_app();
    app.set_result_for_test(
        &(0..40)
            .map(|index| format!("line {index}\n"))
            .collect::<String>(),
        "",
    );
    app.set_active_result_tab(ResultTab::Stdout);
    let layout = ui::compute_layout(ratatui::layout::Rect::new(0, 0, 120, 30), &app);
    app.handle_mouse_event(
        mouse(
            MouseEventKind::Down(MouseButton::Left),
            layout.result_block.x + 1,
            layout.result_block.y + 1,
        ),
        &layout,
    );
    app.handle_key_event(KeyEvent::new(KeyCode::Char('l'), KeyModifiers::NONE));
    assert!(app.active_result_hscroll_for_test() > 0);
}

#[test]
fn dragging_horizontal_scrollbar_changes_result_offset() {
    let _guard = launcher_env_lock();
    clear_launcher_bin();

    let temp = tempdir().expect("tempdir");
    let ms_path = create_fixture_ms(temp.path());

    let schema = listobs_app()
        .load_schema()
        .expect("load schema from listobs");
    let config = ConfigStore::load_for_tests(temp.path().join("casars.toml"));
    let mut app = AppState::from_schema_with_config(listobs_app(), schema, config);
    app.set_text_value("ms_path", ms_path.to_string_lossy().as_ref());
    app.start_run_for_test();
    assert!(app.wait_for_idle_for_test(Duration::from_secs(60)));
    app.set_active_result_tab(ResultTab::Antennas);

    let layout = ui::compute_layout(ratatui::layout::Rect::new(0, 0, 120, 30), &app);
    let scrollbar = layout
        .result_hscrollbar
        .expect("result horizontal scrollbar");
    app.handle_mouse_event(
        mouse(
            MouseEventKind::Down(MouseButton::Left),
            scrollbar.x + scrollbar.width.saturating_sub(1),
            scrollbar.y,
        ),
        &layout,
    );

    assert!(app.active_result_hscroll_for_test() > 0);
}

#[test]
fn horizontal_scroll_offset_persists_across_tab_switches() {
    let _guard = launcher_env_lock();
    clear_launcher_bin();

    let temp = tempdir().expect("tempdir");
    let ms_path = create_fixture_ms(temp.path());

    let schema = listobs_app()
        .load_schema()
        .expect("load schema from listobs");
    let config = ConfigStore::load_for_tests(temp.path().join("casars.toml"));
    let mut app = AppState::from_schema_with_config(listobs_app(), schema, config);
    app.set_text_value("ms_path", ms_path.to_string_lossy().as_ref());
    app.start_run_for_test();
    assert!(app.wait_for_idle_for_test(Duration::from_secs(60)));
    app.set_active_result_tab(ResultTab::Antennas);

    let layout = ui::compute_layout(ratatui::layout::Rect::new(0, 0, 120, 30), &app);
    let scrollbar = layout
        .result_hscrollbar
        .expect("result horizontal scrollbar");
    app.handle_mouse_event(
        mouse(
            MouseEventKind::Down(MouseButton::Left),
            scrollbar.x + scrollbar.width.saturating_sub(1),
            scrollbar.y,
        ),
        &layout,
    );
    let before = app.active_result_hscroll_for_test();
    app.set_active_result_tab(ResultTab::Fields);
    app.set_active_result_tab(ResultTab::Antennas);
    assert_eq!(app.active_result_hscroll_for_test(), before);
}

#[test]
fn start_run_commits_active_text_edit_before_spawning() {
    let _guard = launcher_env_lock();
    clear_launcher_bin();

    let temp = tempdir().expect("tempdir");
    let ms_path = create_fixture_ms(temp.path());

    let schema = listobs_app()
        .load_schema()
        .expect("load schema from listobs");
    let config = ConfigStore::load_for_tests(temp.path().join("casars.toml"));
    let mut app = AppState::from_schema_with_config(listobs_app(), schema, config);

    app.handle_key_event(KeyEvent::new(KeyCode::Enter, KeyModifiers::NONE));
    app.handle_paste(ms_path.to_string_lossy().into_owned());
    app.start_run_for_test();

    assert!(app.wait_for_idle_for_test(Duration::from_secs(60)));
    assert!(
        app.status_line_for_test()
            .contains("completed successfully"),
        "status={} stderr={}",
        app.status_line_for_test(),
        app.stderr_for_test()
    );
    assert!(app.structured_for_test().is_some());
}

#[test]
fn records_output_file_path_for_advanced_output_mode() {
    let _guard = launcher_env_lock();
    clear_launcher_bin();

    let temp = tempdir().expect("tempdir");
    let ms_path = create_fixture_ms(temp.path());
    let output_path = temp.path().join("summary.json");

    let schema = listobs_app()
        .load_schema()
        .expect("load schema from listobs");
    let config = ConfigStore::load_for_tests(temp.path().join("casars.toml"));
    let mut app = AppState::from_schema_with_config(listobs_app(), schema, config);
    app.set_text_value("ms_path", ms_path.to_string_lossy().as_ref());
    app.set_text_value("output", output_path.to_string_lossy().as_ref());
    app.start_run_for_test();
    assert!(app.wait_for_idle_for_test(Duration::from_secs(60)));
    assert!(output_path.is_file());
    assert_eq!(
        app.file_output_path_for_test(),
        Some(output_path.to_string_lossy().as_ref())
    );
}

#[test]
fn selection_inputs_force_selectdata_on_run() {
    let _guard = launcher_env_lock();
    clear_launcher_bin();

    let temp = tempdir().expect("tempdir");
    let ms_path = create_fixture_ms(temp.path());

    let schema = listobs_app()
        .load_schema()
        .expect("load schema from listobs");
    let config = ConfigStore::load_for_tests(temp.path().join("casars.toml"));
    let mut app = AppState::from_schema_with_config(listobs_app(), schema, config);
    app.set_text_value("ms_path", ms_path.to_string_lossy().as_ref());
    app.set_toggle_value("selectdata", false);
    app.set_text_value("field", "3C286");
    app.start_run_for_test();
    assert!(app.wait_for_idle_for_test(Duration::from_secs(60)));
    assert!(
        app.status_line_for_test()
            .contains("completed successfully")
    );
}

#[cfg(unix)]
#[test]
fn falls_back_to_raw_stderr_when_subprocess_fails() {
    let _guard = launcher_env_lock();
    let temp = tempdir().expect("tempdir");
    let script = write_fake_listobs_script(
        temp.path(),
        r#"echo "fake failure" >&2
exit 1
"#,
    );
    set_launcher_bin(&script);

    let schema = listobs_app().load_schema().expect("load fake schema");
    let config = ConfigStore::load_for_tests(temp.path().join("casars.toml"));
    let mut app = AppState::from_schema_with_config(listobs_app(), schema, config);
    app.set_text_value("ms_path", "/tmp/fake.ms");
    app.start_run_for_test();
    assert!(app.wait_for_idle_for_test(Duration::from_secs(10)));
    assert!(app.status_line_for_test().contains("Execution failed"));
    assert!(app.stderr_for_test().contains("fake failure"));
    assert_eq!(app.active_result_tab(), ResultTab::Stderr);
}

#[cfg(unix)]
#[test]
fn can_cancel_a_running_subprocess() {
    let _guard = launcher_env_lock();
    let temp = tempdir().expect("tempdir");
    let script = write_fake_listobs_script(
        temp.path(),
        r#"sleep 5
echo "completed unexpectedly"
exit 0
"#,
    );
    set_launcher_bin(&script);

    let schema = listobs_app().load_schema().expect("load fake schema");
    let config = ConfigStore::load_for_tests(temp.path().join("casars.toml"));
    let mut app = AppState::from_schema_with_config(listobs_app(), schema, config);
    app.set_text_value("ms_path", "/tmp/fake.ms");
    app.start_run_for_test();
    assert!(app.is_running_for_test());
    std::thread::sleep(Duration::from_millis(100));
    app.cancel_for_test();
    assert!(app.wait_for_idle_for_test(Duration::from_secs(10)));
    assert!(app.status_line_for_test().contains("canceled"));
}

fn render_app(app: &AppState, width: u16, height: u16) -> String {
    let backend = TestBackend::new(width, height);
    let mut terminal = Terminal::new(backend).expect("test terminal");
    terminal
        .draw(|frame| {
            let layout = ui::compute_layout(frame.area(), app);
            ui::draw(frame, app, &layout)
        })
        .expect("draw app");
    let buffer = terminal.backend().buffer().clone();
    let mut rendered = String::new();
    for y in 0..buffer.area.height {
        for x in 0..buffer.area.width {
            rendered.push_str(buffer[(x, y)].symbol());
        }
        rendered.push('\n');
    }
    rendered
}

fn wait_for_plot_render(app: &mut AppState, width: u16, height: u16, timeout: Duration) -> bool {
    let start = Instant::now();
    while start.elapsed() < timeout {
        app.prepare_graphics_for_test(width, height);
        app.on_tick();
        if app.plot_protocol().is_some() || app.plot_last_error().is_some() {
            return true;
        }
        std::thread::sleep(Duration::from_millis(20));
    }
    app.prepare_graphics_for_test(width, height);
    app.on_tick();
    app.plot_protocol().is_some() || app.plot_last_error().is_some()
}

fn move_plot_control_selection_to(app: &mut AppState, target: PlotControlTarget) {
    let rows = app.plot_control_rows();
    let current = rows
        .iter()
        .position(|row| row.selected)
        .expect("selected control");
    let target_index = rows
        .iter()
        .position(|row| row.target == target)
        .expect("target control");
    if current < target_index {
        for _ in current..target_index {
            app.handle_key_event(KeyEvent::new(KeyCode::Down, KeyModifiers::NONE));
        }
    } else {
        for _ in target_index..current {
            app.handle_key_event(KeyEvent::new(KeyCode::Up, KeyModifiers::NONE));
        }
    }
}

fn drag_select_visible_text(
    app: &mut AppState,
    width: u16,
    height: u16,
    target: OutputPane,
    needle: &str,
) {
    let layout = ui::compute_layout(ratatui::layout::Rect::new(0, 0, width, height), app);
    let buffer = app
        .visible_text_buffer(target, &layout)
        .expect("visible text buffer");
    let (row, col) = buffer
        .lines
        .iter()
        .enumerate()
        .find_map(|(row, line)| {
            line.text
                .find(needle)
                .map(|byte_index| (row, line.text[..byte_index].chars().count()))
        })
        .unwrap_or_else(|| panic!("visible text {needle:?} not found"));
    let start_column = buffer.area.x + col as u16;
    let end_column = start_column + needle.chars().count().saturating_sub(1) as u16;
    let row = buffer.area.y + row as u16;

    app.handle_mouse_event(
        mouse(MouseEventKind::Down(MouseButton::Left), start_column, row),
        &layout,
    );
    app.handle_mouse_event(
        mouse(MouseEventKind::Drag(MouseButton::Left), end_column, row),
        &layout,
    );
    app.handle_mouse_event(
        mouse(MouseEventKind::Up(MouseButton::Left), end_column, row),
        &layout,
    );
}

fn create_fixture_table(root: &Path) -> PathBuf {
    let parent_path = root.join("parent.tab");
    let child_path = root.join("child.tab");

    let child_schema =
        TableSchema::new(vec![ColumnSchema::scalar("id", PrimitiveType::Int32)]).unwrap();
    let mut child = Table::with_schema(child_schema);
    child
        .add_row(RecordValue::new(vec![RecordField::new(
            "id",
            Value::Scalar(ScalarValue::Int32(7)),
        )]))
        .unwrap();
    child.save(TableOptions::new(&child_path)).unwrap();

    let parent_schema = TableSchema::new(vec![
        ColumnSchema::scalar("name", PrimitiveType::String),
        ColumnSchema::scalar("value", PrimitiveType::Float64),
    ])
    .unwrap();
    let mut parent = Table::with_schema(parent_schema);
    parent
        .add_row(RecordValue::new(vec![
            RecordField::new("name", Value::Scalar(ScalarValue::String("alpha".into()))),
            RecordField::new("value", Value::Scalar(ScalarValue::Float64(1.5))),
        ]))
        .unwrap();
    parent
        .keywords_mut()
        .upsert("CHILD", Value::table_ref("child.tab"));
    parent.save(TableOptions::new(&parent_path)).unwrap();

    parent_path
}

fn test_app() -> (tempfile::TempDir, AppState) {
    let temp = tempdir().expect("tempdir");
    let config = ConfigStore::load_for_tests(temp.path().join("casars.toml"));
    let app = AppState::from_schema_with_config(
        msexplore_app(),
        msexplore_command_schema("msexplore"),
        config,
    );
    (temp, app)
}

fn unpack_casacore_ms_fixture(archive_name: &str) -> (tempfile::TempDir, PathBuf) {
    let temp = tempdir().expect("tempdir");
    let archive_path = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../casacore-ms/tests/fixtures")
        .join(archive_name);
    let archive_file = File::open(&archive_path).expect("open fixture archive");
    let mut archive = Archive::new(GzDecoder::new(archive_file));
    archive.unpack(temp.path()).expect("unpack fixture archive");

    let ms_dir_name = archive_name
        .strip_suffix(".tgz")
        .expect("fixture archive suffix");
    let ms_path = temp.path().join(ms_dir_name);
    assert!(
        ms_path.is_dir(),
        "expected unpacked MS at {}",
        ms_path.display()
    );
    (temp, ms_path)
}

fn mouse(kind: MouseEventKind, column: u16, row: u16) -> MouseEvent {
    MouseEvent {
        kind,
        column,
        row,
        modifiers: KeyModifiers::NONE,
    }
}

fn switch_imexplore_left_pane_mode(app: &mut AppState, row_offset: u16) {
    let layout = ui::compute_layout(ratatui::layout::Rect::new(0, 0, 140, 34), app);
    let mode_row = layout.browser_mode_selector.expect("mode selector");
    app.handle_mouse_event(
        mouse(
            MouseEventKind::Down(MouseButton::Left),
            mode_row.x + 1,
            mode_row.y,
        ),
        &layout,
    );
    let picker_area = ui::browser_mode_picker_area(layout.browser_mode_selector, layout.form_block);
    let picker_list_area = ui::browser_mode_picker_list_area(picker_area);
    app.handle_mouse_event(
        mouse(
            MouseEventKind::Down(MouseButton::Left),
            picker_list_area.x + 1,
            picker_list_area.y + row_offset,
        ),
        &layout,
    );
}

fn launcher_env_lock() -> MutexGuard<'static, ()> {
    crate::test_env_lock()
}

fn clear_launcher_bin() {
    // Tests hold a process-global mutex before mutating the environment.
    unsafe {
        std::env::remove_var("CASARS_LISTOBS_BIN");
    }
}

fn set_launcher_bin(path: &Path) {
    // Tests hold a process-global mutex before mutating the environment.
    unsafe {
        std::env::set_var("CASARS_LISTOBS_BIN", path);
    }
}

fn clear_tablebrowser_launcher_bin() {
    // Tests hold a process-global mutex before mutating the environment.
    unsafe {
        std::env::remove_var("CASARS_TABLEBROWSER_BIN");
    }
}

fn clear_test_clipboard_file() {
    unsafe {
        std::env::remove_var("CASARS_TEST_CLIPBOARD_FILE");
    }
}

fn clear_imexplore_perf_env() {
    unsafe {
        std::env::remove_var("CASARS_IMEXPLORE_PERF");
        std::env::remove_var("CASARS_IMEXPLORE_PERF_DIR");
    }
}

struct ImexplorePerfEnvGuard;

impl Drop for ImexplorePerfEnvGuard {
    fn drop(&mut self) {
        clear_imexplore_perf_env();
    }
}

fn set_imexplore_perf_env(dir: &Path) -> ImexplorePerfEnvGuard {
    unsafe {
        std::env::set_var("CASARS_IMEXPLORE_PERF", "1");
        std::env::set_var("CASARS_IMEXPLORE_PERF_DIR", dir);
    }
    ImexplorePerfEnvGuard
}

fn set_test_clipboard_file(path: &Path) {
    unsafe {
        std::env::set_var("CASARS_TEST_CLIPBOARD_FILE", path);
    }
}

fn set_tablebrowser_launcher_bin(path: &Path) {
    // Tests hold a process-global mutex before mutating the environment.
    unsafe {
        std::env::set_var("CASARS_TABLEBROWSER_BIN", path);
    }
}

fn set_imexplore_launcher_bin(path: &Path) {
    unsafe {
        std::env::set_var("CASARS_IMEXPLORE_BIN", path);
    }
}

fn read_perf_events(path: &Path) -> Vec<serde_json::Value> {
    fs::read_to_string(path)
        .expect("read perf jsonl")
        .lines()
        .filter(|line| !line.trim().is_empty())
        .map(|line| serde_json::from_str(line).expect("decode perf json event"))
        .collect()
}

#[cfg(unix)]
fn write_fake_listobs_script(root: &Path, body: &str) -> PathBuf {
    use std::fs;
    use std::os::unix::fs::PermissionsExt;

    let schema_json = listobs_command_schema("listobs")
        .render_json_pretty()
        .expect("serialize schema");
    let path = root.join("fake-listobs.sh");
    let script = format!(
        "#!/bin/sh\nif [ \"$1\" = \"--ui-schema\" ]; then\ncat <<'EOF'\n{schema_json}\nEOF\nexit 0\nfi\n{body}"
    );
    fs::write(&path, script).expect("write fake script");
    let mut permissions = fs::metadata(&path).expect("metadata").permissions();
    permissions.set_mode(0o755);
    fs::set_permissions(&path, permissions).expect("chmod script");
    path
}

#[cfg(unix)]
fn write_fake_tablebrowser_script(
    root: &Path,
    responses: &[String],
    raw_response: Option<String>,
) -> PathBuf {
    use std::fs;
    use std::os::unix::fs::PermissionsExt;

    let schema_json = fake_tablebrowser_schema_json();
    let mut session_body = String::new();
    if let Some(raw_response) = raw_response {
        session_body.push_str("IFS= read -r line || exit 0\n");
        session_body.push_str(&format!("printf '%s\\n' '{}'\n", raw_response));
    } else {
        session_body.push_str("count=0\n");
        session_body.push_str("while IFS= read -r line; do\n");
        session_body.push_str("  count=$((count + 1))\n");
        session_body.push_str("  case \"$count\" in\n");
        for (index, response) in responses.iter().enumerate() {
            let case_index = index + 1;
            session_body.push_str(&format!(
                "    {case_index}) printf '%s\\n' '{response}' ;;\n"
            ));
        }
        if let Some(last_response) = responses.last() {
            session_body.push_str(&format!("    *) printf '%s\\n' '{last_response}' ;;\n"));
        } else {
            session_body.push_str("    *) exit 0 ;;\n");
        }
        session_body.push_str("  esac\n");
        session_body.push_str("done\n");
    }

    let path = root.join("fake-tablebrowser.sh");
    let script = format!(
        "#!/bin/sh\nif [ \"$1\" = \"--ui-schema\" ]; then\ncat <<'EOF'\n{schema_json}\nEOF\nexit 0\nfi\nif [ \"$1\" = \"--session\" ]; then\n{session_body}exit 0\nfi\necho \"unexpected args: $@\" >&2\nexit 1\n"
    );
    fs::write(&path, script).expect("write fake tablebrowser script");
    let mut permissions = fs::metadata(&path).expect("metadata").permissions();
    permissions.set_mode(0o755);
    fs::set_permissions(&path, permissions).expect("chmod script");
    path
}

fn fake_tablebrowser_schema_json() -> String {
    serde_json::json!({
        "schema_version": 1,
        "command_id": "tablebrowser",
        "invocation_name": "tablebrowser",
        "display_name": "Table Browser",
        "category": "Tables",
        "summary": "browse arbitrary casacore tables",
        "usage": "tablebrowser <table-path>",
        "arguments": [
            {
                "id": "table_path",
                "label": "Table Path",
                "order": 0,
                "parser": {
                    "kind": "positional",
                    "metavar": "table-path"
                },
                "value_kind": "path",
                "required": true,
                "default": null,
                "help": "Path to the casacore table root directory",
                "group": "Input",
                "advanced": false,
                "hidden_in_tui": false
            }
        ],
        "managed_output": null
    })
    .to_string()
}

#[cfg(unix)]
fn write_fake_imexplore_script(
    root: &Path,
    responses: &[String],
    raw_response: Option<String>,
) -> PathBuf {
    use std::fs;
    use std::os::unix::fs::PermissionsExt;

    let schema_json = fake_imexplore_schema_json();
    let mut session_body = String::new();
    if let Some(raw_response) = raw_response {
        session_body.push_str("IFS= read -r line || exit 0\n");
        session_body.push_str(&format!("printf '%s\\n' '{}'\n", raw_response));
    } else {
        session_body.push_str("count=0\n");
        session_body.push_str("while IFS= read -r line; do\n");
        session_body.push_str("  count=$((count + 1))\n");
        session_body.push_str("  case \"$count\" in\n");
        for (index, response) in responses.iter().enumerate() {
            let case_index = index + 1;
            session_body.push_str(&format!(
                "    {case_index}) printf '%s\\n' '{response}' ;;\n"
            ));
        }
        if let Some(last_response) = responses.last() {
            session_body.push_str(&format!("    *) printf '%s\\n' '{last_response}' ;;\n"));
        } else {
            session_body.push_str("    *) exit 0 ;;\n");
        }
        session_body.push_str("  esac\n");
        session_body.push_str("done\n");
    }

    let path = root.join("fake-imexplore.sh");
    let script = format!(
        "#!/bin/sh\nif [ \"$1\" = \"--ui-schema\" ]; then\ncat <<'EOF'\n{schema_json}\nEOF\nexit 0\nfi\nif [ \"$1\" = \"--session\" ]; then\n{session_body}exit 0\nfi\necho \"unexpected args: $@\" >&2\nexit 1\n"
    );
    fs::write(&path, script).expect("write fake imexplore script");
    let mut permissions = fs::metadata(&path).expect("metadata").permissions();
    permissions.set_mode(0o755);
    fs::set_permissions(&path, permissions).expect("chmod script");
    path
}

fn fake_imexplore_schema_json() -> String {
    serde_json::json!({
        "schema_version": 1,
        "command_id": "imexplore",
        "invocation_name": "imexplore",
        "display_name": "ImExplore",
        "category": "Images",
        "summary": "browse persistent casacore images",
        "usage": "imexplore <image-path>",
        "arguments": [
            {
                "id": "image_path",
                "label": "Image Path",
                "order": 0,
                "parser": {
                    "kind": "positional",
                    "metavar": "image-path"
                },
                "value_kind": "path",
                "required": true,
                "default": null,
                "help": "Path to the casacore image root directory",
                "group": "Input",
                "advanced": false,
                "hidden_in_tui": false
            },
            {
                "id": "blc",
                "label": "BLC",
                "order": 1,
                "parser": {
                    "kind": "option",
                    "flags": ["--blc"],
                    "metavar": "BLC",
                    "choices": []
                },
                "value_kind": "string",
                "required": false,
                "default": "",
                "help": "Comma-separated inclusive bottom-left-corner pixel indices",
                "group": "View",
                "advanced": false,
                "hidden_in_tui": false
            },
            {
                "id": "trc",
                "label": "TRC",
                "order": 2,
                "parser": {
                    "kind": "option",
                    "flags": ["--trc"],
                    "metavar": "TRC",
                    "choices": []
                },
                "value_kind": "string",
                "required": false,
                "default": "",
                "help": "Comma-separated inclusive top-right-corner pixel indices",
                "group": "View",
                "advanced": false,
                "hidden_in_tui": false
            },
            {
                "id": "inc",
                "label": "INC",
                "order": 3,
                "parser": {
                    "kind": "option",
                    "flags": ["--inc"],
                    "metavar": "INC",
                    "choices": []
                },
                "value_kind": "string",
                "required": false,
                "default": "",
                "help": "Comma-separated per-axis pixel increments",
                "group": "View",
                "advanced": false,
                "hidden_in_tui": false
            },
            {
                "id": "stretch",
                "label": "Stretch",
                "order": 4,
                "parser": {
                    "kind": "option",
                    "flags": ["--stretch"],
                    "metavar": "STRETCH",
                    "choices": ["percentile99", "percentile95", "minmax", "zscale", "manual"]
                },
                "value_kind": "choice",
                "required": false,
                "default": "percentile99",
                "help": "Plane stretch preset",
                "group": "Display",
                "advanced": false,
                "hidden_in_tui": false
            },
            {
                "id": "autoscale",
                "label": "Autoscale",
                "order": 5,
                "parser": {
                    "kind": "option",
                    "flags": ["--autoscale"],
                    "metavar": "AUTOSCALE",
                    "choices": ["per_plane", "frozen"]
                },
                "value_kind": "choice",
                "required": false,
                "default": "per_plane",
                "help": "Whether clip bounds update per plane or stay frozen while stepping cubes",
                "group": "Display",
                "advanced": false,
                "hidden_in_tui": false
            },
            {
                "id": "clip_low",
                "label": "Clip Low",
                "order": 6,
                "parser": {
                    "kind": "option",
                    "flags": ["--clip-low"],
                    "metavar": "LOW",
                    "choices": []
                },
                "value_kind": "string",
                "required": false,
                "default": "",
                "help": "Manual lower clip bound in image value units",
                "group": "Display",
                "advanced": false,
                "hidden_in_tui": false
            },
            {
                "id": "clip_high",
                "label": "Clip High",
                "order": 7,
                "parser": {
                    "kind": "option",
                    "flags": ["--clip-high"],
                    "metavar": "HIGH",
                    "choices": []
                },
                "value_kind": "string",
                "required": false,
                "default": "",
                "help": "Manual upper clip bound in image value units",
                "group": "Display",
                "advanced": false,
                "hidden_in_tui": false
            },
            {
                "id": "fps",
                "label": "FPS",
                "order": 8,
                "parser": {
                    "kind": "option",
                    "flags": ["--fps"],
                    "metavar": "FPS",
                    "choices": []
                },
                "value_kind": "string",
                "required": false,
                "default": "1",
                "help": "Movie playback frames per second",
                "group": "Display",
                "advanced": false,
                "hidden_in_tui": false
            }
        ],
        "managed_output": null
    })
    .to_string()
}

fn fake_browser_snapshot_json(
    view: ProtocolBrowserView,
    status_line: &str,
    content_lines: Vec<String>,
) -> String {
    fake_browser_snapshot_with_focus_and_metrics_json(
        view,
        BrowserFocus::Main,
        status_line,
        content_lines,
        None,
        None,
        None,
    )
}

fn fake_browser_snapshot_with_metrics_json(
    view: ProtocolBrowserView,
    status_line: &str,
    content_lines: Vec<String>,
    vertical_metrics: Option<BrowserNavigationMetrics>,
    horizontal_metrics: Option<BrowserNavigationMetrics>,
    inspector: Option<BrowserInspectorSnapshot>,
) -> String {
    fake_browser_snapshot_with_focus_and_metrics_json(
        view,
        BrowserFocus::Main,
        status_line,
        content_lines,
        vertical_metrics,
        horizontal_metrics,
        inspector,
    )
}

fn fake_browser_snapshot_with_focus_and_metrics_json(
    view: ProtocolBrowserView,
    focus: BrowserFocus,
    status_line: &str,
    content_lines: Vec<String>,
    vertical_metrics: Option<BrowserNavigationMetrics>,
    horizontal_metrics: Option<BrowserNavigationMetrics>,
    inspector: Option<BrowserInspectorSnapshot>,
) -> String {
    serde_json::to_string(&BrowserResponseEnvelope::snapshot(BrowserSnapshot {
        capabilities: BrowserCapabilities { editable: false },
        view,
        focus,
        table_path: "/tmp/fake.ms".to_string(),
        breadcrumb: vec![BrowserBreadcrumbEntry {
            label: "fake.ms".to_string(),
            path: "/tmp/fake.ms".to_string(),
        }],
        viewport: BrowserViewport::new(120, 24),
        status_line: status_line.to_string(),
        content_lines,
        vertical_metrics,
        horizontal_metrics,
        selected_address: None,
        inspector,
    }))
    .expect("serialize fake snapshot")
}

fn fake_browser_snapshot_with_inspector_json(
    view: ProtocolBrowserView,
    status_line: &str,
    content_lines: Vec<String>,
    inspector: Option<BrowserInspectorSnapshot>,
) -> String {
    fake_browser_snapshot_with_metrics_json(view, status_line, content_lines, None, None, inspector)
}

fn fake_imexplore_snapshot_json(
    view: ProtocolImageView,
    focus: ProtocolImageFocus,
    status_line: &str,
    content_lines: Vec<String>,
    inspector_lines: Vec<String>,
    probe: Option<ImageBrowserProbe>,
    non_display_axis: Option<ImageNonDisplayAxisState>,
) -> String {
    let ndim = probe
        .as_ref()
        .map_or(2, |probe| probe.pixel_indices.len().max(2));
    fake_imexplore_snapshot_json_full(
        view,
        focus,
        status_line,
        content_lines,
        inspector_lines,
        probe,
        None,
        non_display_axis,
        image_parameters(
            &std::iter::repeat_n("0", ndim).collect::<Vec<_>>().join(","),
            &std::iter::repeat_n("0", ndim).collect::<Vec<_>>().join(","),
            &std::iter::repeat_n("1", ndim).collect::<Vec<_>>().join(","),
        ),
    )
}

#[allow(clippy::too_many_arguments)]
fn fake_imexplore_snapshot_json_with_parameters(
    view: ProtocolImageView,
    focus: ProtocolImageFocus,
    status_line: &str,
    content_lines: Vec<String>,
    inspector_lines: Vec<String>,
    probe: Option<ImageBrowserProbe>,
    non_display_axis: Option<ImageNonDisplayAxisState>,
    parameter_values: ImageBrowserParameters,
) -> String {
    fake_imexplore_snapshot_json_full(
        view,
        focus,
        status_line,
        content_lines,
        inspector_lines,
        probe,
        None,
        non_display_axis,
        parameter_values,
    )
}

#[allow(clippy::too_many_arguments)]
fn fake_imexplore_snapshot_json_with_profile(
    view: ProtocolImageView,
    focus: ProtocolImageFocus,
    status_line: &str,
    content_lines: Vec<String>,
    inspector_lines: Vec<String>,
    probe: Option<ImageBrowserProbe>,
    profile: Option<ImageProfilePayload>,
    non_display_axis: Option<ImageNonDisplayAxisState>,
    parameter_values: ImageBrowserParameters,
) -> String {
    fake_imexplore_snapshot_json_full(
        view,
        focus,
        status_line,
        content_lines,
        inspector_lines,
        probe,
        profile,
        non_display_axis,
        parameter_values,
    )
}

#[allow(clippy::too_many_arguments)]
fn fake_imexplore_snapshot_json_full(
    view: ProtocolImageView,
    focus: ProtocolImageFocus,
    status_line: &str,
    content_lines: Vec<String>,
    inspector_lines: Vec<String>,
    probe: Option<ImageBrowserProbe>,
    profile: Option<ImageProfilePayload>,
    non_display_axis: Option<ImageNonDisplayAxisState>,
    parameter_values: ImageBrowserParameters,
) -> String {
    let ndim = parameter_values
        .blc
        .split(',')
        .filter(|part| !part.trim().is_empty())
        .count()
        .max(
            parameter_values
                .trc
                .split(',')
                .filter(|part| !part.trim().is_empty())
                .count(),
        )
        .max(
            parameter_values
                .inc
                .split(',')
                .filter(|part| !part.trim().is_empty())
                .count(),
        )
        .max(
            probe
                .as_ref()
                .map_or(2, |probe| probe.pixel_indices.len().max(2)),
        )
        .max(2);
    let blc = parse_parameter_axis_values(&parameter_values.blc, ndim, 0);
    let trc = parse_parameter_axis_values(&parameter_values.trc, ndim, 0);
    let inc = parse_parameter_axis_values(&parameter_values.inc, ndim, 1);
    let sampled_x_len = sampled_axis_len(blc[0], trc[0], inc[0]);
    let sampled_y_len = sampled_axis_len(blc[1], trc[1], inc[1]);
    let display_axes = probe
        .as_ref()
        .map(|probe| {
            vec![
                ImageDisplayAxisState {
                    axis: 0,
                    name: probe
                        .pixel_axes
                        .first()
                        .map(|axis| axis.name.clone())
                        .unwrap_or_else(|| "X".to_string()),
                    unit: probe
                        .pixel_axes
                        .first()
                        .map(|axis| axis.unit.clone())
                        .unwrap_or_default(),
                    blc: blc[0],
                    trc: trc[0],
                    inc: inc[0],
                    sampled_len: sampled_x_len,
                    world_increment: None,
                },
                ImageDisplayAxisState {
                    axis: 1,
                    name: probe
                        .pixel_axes
                        .get(1)
                        .map(|axis| axis.name.clone())
                        .unwrap_or_else(|| "Y".to_string()),
                    unit: probe
                        .pixel_axes
                        .get(1)
                        .map(|axis| axis.unit.clone())
                        .unwrap_or_default(),
                    blc: blc[1],
                    trc: trc[1],
                    inc: inc[1],
                    sampled_len: sampled_y_len,
                    world_increment: None,
                },
            ]
        })
        .unwrap_or_else(|| {
            vec![
                ImageDisplayAxisState {
                    axis: 0,
                    name: "X".to_string(),
                    unit: String::new(),
                    blc: blc[0],
                    trc: trc[0],
                    inc: inc[0],
                    sampled_len: sampled_x_len,
                    world_increment: None,
                },
                ImageDisplayAxisState {
                    axis: 1,
                    name: "Y".to_string(),
                    unit: String::new(),
                    blc: blc[1],
                    trc: trc[1],
                    inc: inc[1],
                    sampled_len: sampled_y_len,
                    world_increment: None,
                },
            ]
        });
    let plane_cursor = probe.as_ref().map(|probe| ImagePlaneCursorState {
        sampled_x: sample_index_for_pixel(
            probe.pixel_indices.first().copied().unwrap_or_default(),
            blc[0],
            inc[0],
        ),
        sampled_y: sample_index_for_pixel(
            probe.pixel_indices.get(1).copied().unwrap_or_default(),
            blc[1],
            inc[1],
        ),
        pixel_x: probe.pixel_indices.first().copied().unwrap_or_default(),
        pixel_y: probe.pixel_indices.get(1).copied().unwrap_or_default(),
    });
    let non_display_axes = non_display_axis.into_iter().collect::<Vec<_>>();
    serde_json::to_string(&ImageBrowserResponseEnvelope::snapshot(
        ImageBrowserSnapshot {
            status_line: status_line.to_string(),
            active_view: view,
            focus,
            shape: trc
                .iter()
                .map(|value| value.saturating_add(1).max(1))
                .collect(),
            parameters: parameter_values,
            inspector_lines,
            content_lines: content_lines.clone(),
            navigation: ImageNavigationMetrics {
                selected_index: 0,
                total_items: content_lines.len(),
                viewport_items: content_lines.len().max(1),
            },
            plane: Some(ImagePlaneRaster {
                width: content_lines.first().map_or(0, |line| line.chars().count()),
                height: content_lines.len(),
                pixels_u8: vec![128; content_lines.iter().map(|line| line.chars().count()).sum()],
                clip_min: 0.0,
                clip_max: 1.0,
                data_min: 0.0,
                data_max: 1.0,
                value_unit: "Jy/beam".to_string(),
                histogram_bins: vec![0, 1, 0, 0],
                masked_or_non_finite_count: 0,
                no_finite_values: false,
            }),
            probe,
            profile,
            display_axes,
            plane_cursor,
            non_display_axes,
            region: None,
            saved_region_names: Vec::new(),
            active_region_definition_name: None,
            mask_names: Vec::new(),
            default_mask_name: None,
            backend_timing: None,
            capabilities: ImageBrowserCapabilities {
                renderable_plane: true,
                world_coords_available: true,
                pixel_only_mode: false,
                non_display_axis_selectors: true,
                mask_present: false,
                complex_unsupported: false,
            },
        },
    ))
    .expect("serialize fake imexplore snapshot")
}

fn fake_imexplore_snapshot_json_with_saved_items(
    base: String,
    saved_regions: &[&str],
    active_region: Option<&str>,
    mask_names: &[&str],
    default_mask: Option<&str>,
) -> String {
    let mut envelope: ImageBrowserResponseEnvelope =
        serde_json::from_str(&base).expect("parse fake imexplore snapshot");
    let ImageBrowserResponse::Snapshot(snapshot) = &mut envelope.response else {
        panic!("expected snapshot response");
    };
    snapshot.saved_region_names = saved_regions
        .iter()
        .map(|name| (*name).to_string())
        .collect();
    snapshot.active_region_definition_name = active_region.map(str::to_string);
    snapshot.mask_names = mask_names.iter().map(|name| (*name).to_string()).collect();
    snapshot.default_mask_name = default_mask.map(str::to_string);
    snapshot.capabilities.mask_present = !snapshot.mask_names.is_empty();
    serde_json::to_string(&envelope).expect("serialize fake imexplore snapshot")
}

fn fake_imexplore_snapshot_json_with_region(base: String) -> String {
    fake_imexplore_snapshot_json_with_region_stats(
        base,
        ImageRegionStatsState {
            pixel_count: 9,
            median: 22.0,
            min: 11.0,
            max: 33.0,
            mean: 22.0,
            sigma: (606.0f64 / 9.0).sqrt(),
            rms: (4962.0f64 / 9.0).sqrt(),
            sum: 198.0,
            value_unit: "Jy/beam".to_string(),
        },
    )
}

fn fake_imexplore_snapshot_json_with_region_stats(
    base: String,
    stats: ImageRegionStatsState,
) -> String {
    let mut envelope: ImageBrowserResponseEnvelope =
        serde_json::from_str(&base).expect("parse fake imexplore snapshot");
    let ImageBrowserResponse::Snapshot(snapshot) = &mut envelope.response else {
        panic!("expected snapshot response");
    };
    snapshot.region = Some(ImageRegionState {
        label: "Region 1".to_string(),
        shape_count: 1,
        closed_shape_count: 1,
        editing: false,
        active_shape_vertices: 0,
        overlay_shapes: vec![ImageRegionOverlayShapeState {
            vertices: vec![
                ImageRegionOverlayVertex {
                    sampled_x: 1.0,
                    sampled_y: 1.0,
                },
                ImageRegionOverlayVertex {
                    sampled_x: 3.0,
                    sampled_y: 1.0,
                },
                ImageRegionOverlayVertex {
                    sampled_x: 3.0,
                    sampled_y: 3.0,
                },
                ImageRegionOverlayVertex {
                    sampled_x: 1.0,
                    sampled_y: 3.0,
                },
            ],
            closed: true,
        }],
        stats: Some(stats),
    });
    serde_json::to_string(&envelope).expect("serialize fake imexplore snapshot")
}

fn fake_image_profile_payload() -> ImageProfilePayload {
    ImageProfilePayload {
        axis: 2,
        axis_name: "Frequency".to_string(),
        axis_unit: "Hz".to_string(),
        value_unit: "Jy/beam".to_string(),
        coord_type: "Spectral".to_string(),
        selected_sample_index: 1,
        samples: vec![
            ImageProfileSampleState {
                sample_index: 0,
                pixel_index: 0,
                value: 1.0,
                masked: false,
                finite: true,
                world_axis: Some(ImageBrowserAxisValue {
                    name: "Frequency".to_string(),
                    unit: "Hz".to_string(),
                    value: 1.150_220_333_39e11,
                }),
            },
            ImageProfileSampleState {
                sample_index: 1,
                pixel_index: 1,
                value: 5.0,
                masked: false,
                finite: true,
                world_axis: Some(ImageBrowserAxisValue {
                    name: "Frequency".to_string(),
                    unit: "Hz".to_string(),
                    value: 1.150_230_333_39e11,
                }),
            },
            ImageProfileSampleState {
                sample_index: 2,
                pixel_index: 2,
                value: 2.0,
                masked: false,
                finite: true,
                world_axis: Some(ImageBrowserAxisValue {
                    name: "Frequency".to_string(),
                    unit: "Hz".to_string(),
                    value: 1.150_240_333_39e11,
                }),
            },
        ],
    }
}

fn image_parameters(blc: &str, trc: &str, inc: &str) -> ImageBrowserParameters {
    ImageBrowserParameters {
        blc: blc.to_string(),
        trc: trc.to_string(),
        inc: inc.to_string(),
        stretch: "percentile99".to_string(),
        autoscale: "per_plane".to_string(),
        clip_low: String::new(),
        clip_high: String::new(),
    }
}

fn parse_parameter_axis_values(text: &str, expected_len: usize, default: usize) -> Vec<usize> {
    let mut values = text
        .split(',')
        .map(|part| part.trim().parse::<usize>().unwrap_or(default))
        .collect::<Vec<_>>();
    values.resize(expected_len, default);
    values
}

fn sampled_axis_len(blc: usize, trc: usize, inc: usize) -> usize {
    ((trc.saturating_sub(blc)) / inc.max(1)) + 1
}

fn sample_index_for_pixel(pixel: usize, blc: usize, inc: usize) -> usize {
    pixel.saturating_sub(blc) / inc.max(1)
}

fn create_fixture_ms(root: &Path) -> PathBuf {
    let ms_path = root.join("listobs_fixture.ms");
    let mut ms = MeasurementSet::create(
        &ms_path,
        MeasurementSetBuilder::new().with_main_column(OptionalMainColumn::Data),
    )
    .expect("create MS");
    add_observation_row(&mut ms, 4_981_000_000.0, 4_981_000_030.0);
    add_field_row(&mut ms, "3C286", "C", 0, 4_981_000_000.0, [1.234, 0.456]);
    add_field_row(&mut ms, "SECOND", "S", 1, 4_981_000_015.0, [1.334, 0.556]);
    add_state_row(&mut ms, "CALIBRATE_PHASE.ON_SOURCE");
    add_state_row(&mut ms, "TARGET.ON_SOURCE");
    add_spectral_window_row(&mut ms, "SPW0", 1.4e9);
    add_spectral_window_row(&mut ms, "SPW1", 2.8e9);
    add_polarization_row(&mut ms, &[9, 12]);
    add_polarization_row(&mut ms, &[5, 8]);
    add_data_description_row(&mut ms, 0, 0);
    add_data_description_row(&mut ms, 1, 1);
    add_antenna_rows(&mut ms);
    add_main_row(&mut ms, 4_981_000_000.0, 1, 0, 1, 0, 0, [30.0, 40.0, 0.0]);
    add_main_row(&mut ms, 4_981_000_015.0, 0, 1, 2, 1, 1, [300.0, 400.0, 0.0]);
    set_main_row_data_matrix(
        &mut ms,
        0,
        ArrayD::from_shape_vec(
            vec![2, 2],
            vec![
                Complex32::new(1.0, 0.0),
                Complex32::new(2.0, 0.5),
                Complex32::new(3.0, -0.5),
                Complex32::new(4.0, 0.0),
            ],
        )
        .unwrap(),
    );
    set_main_row_data_matrix(
        &mut ms,
        1,
        ArrayD::from_shape_vec(
            vec![2, 2],
            vec![
                Complex32::new(5.0, 0.0),
                Complex32::new(6.0, 0.25),
                Complex32::new(7.0, -0.25),
                Complex32::new(8.0, 0.0),
            ],
        )
        .unwrap(),
    );
    set_main_row_flag_matrix(
        &mut ms,
        0,
        ArrayD::from_shape_vec(vec![2, 2], vec![false, false, false, true]).unwrap(),
    );
    set_main_row_flag_matrix(
        &mut ms,
        1,
        ArrayD::from_shape_vec(vec![2, 2], vec![true, true, true, true]).unwrap(),
    );
    ms.save().expect("save MS");
    ms_path
}

fn add_observation_row(ms: &mut MeasurementSet, start: f64, end: f64) {
    let table = ms
        .subtable_mut(SubtableId::Observation)
        .expect("OBSERVATION table");
    table
        .add_row(RecordValue::new(vec![
            RecordField::new("FLAG_ROW", Value::Scalar(ScalarValue::Bool(false))),
            RecordField::new(
                "LOG",
                Value::Array(ArrayValue::String(
                    ArrayD::from_shape_vec(vec![1], vec!["log".to_string()]).unwrap(),
                )),
            ),
            RecordField::new(
                "OBSERVER",
                Value::Scalar(ScalarValue::String("TESTER".to_string())),
            ),
            RecordField::new(
                "PROJECT",
                Value::Scalar(ScalarValue::String("CASA-RS".to_string())),
            ),
            RecordField::new("RELEASE_DATE", Value::Scalar(ScalarValue::Float64(end))),
            RecordField::new(
                "SCHEDULE",
                Value::Array(ArrayValue::String(
                    ArrayD::from_shape_vec(vec![1], vec!["default".to_string()]).unwrap(),
                )),
            ),
            RecordField::new(
                "SCHEDULE_TYPE",
                Value::Scalar(ScalarValue::String("standard".to_string())),
            ),
            RecordField::new(
                "TELESCOPE_NAME",
                Value::Scalar(ScalarValue::String("VLA".to_string())),
            ),
            RecordField::new(
                "TIME_RANGE",
                Value::Array(ArrayValue::Float64(
                    ArrayD::from_shape_vec(vec![2], vec![start, end]).unwrap(),
                )),
            ),
        ]))
        .unwrap();
}

fn add_field_row(
    ms: &mut MeasurementSet,
    name: &str,
    code: &str,
    source_id: i32,
    time: f64,
    direction_pair: [f64; 2],
) {
    let table = ms.subtable_mut(SubtableId::Field).expect("FIELD table");
    let direction =
        ArrayValue::Float64(ArrayD::from_shape_vec(vec![2, 1], direction_pair.to_vec()).unwrap());
    table
        .add_row(RecordValue::new(vec![
            RecordField::new("CODE", Value::Scalar(ScalarValue::String(code.to_string()))),
            RecordField::new("DELAY_DIR", Value::Array(direction.clone())),
            RecordField::new("FLAG_ROW", Value::Scalar(ScalarValue::Bool(false))),
            RecordField::new("NAME", Value::Scalar(ScalarValue::String(name.to_string()))),
            RecordField::new("NUM_POLY", Value::Scalar(ScalarValue::Int32(0))),
            RecordField::new("PHASE_DIR", Value::Array(direction.clone())),
            RecordField::new("REFERENCE_DIR", Value::Array(direction)),
            RecordField::new("SOURCE_ID", Value::Scalar(ScalarValue::Int32(source_id))),
            RecordField::new("TIME", Value::Scalar(ScalarValue::Float64(time))),
        ]))
        .unwrap();
}

fn add_spectral_window_row(ms: &mut MeasurementSet, name: &str, ref_frequency_hz: f64) {
    let table = ms
        .subtable_mut(SubtableId::SpectralWindow)
        .expect("SPECTRAL_WINDOW table");
    let freq = ArrayValue::Float64(ArrayD::from_shape_vec(vec![2], vec![1.4e9, 1.401e9]).unwrap());
    let width = ArrayValue::Float64(ArrayD::from_shape_vec(vec![2], vec![1.0e6, 1.0e6]).unwrap());
    table
        .add_row(RecordValue::new(vec![
            RecordField::new("CHAN_FREQ", Value::Array(freq)),
            RecordField::new("CHAN_WIDTH", Value::Array(width.clone())),
            RecordField::new("EFFECTIVE_BW", Value::Array(width.clone())),
            RecordField::new("FLAG_ROW", Value::Scalar(ScalarValue::Bool(false))),
            RecordField::new("FREQ_GROUP", Value::Scalar(ScalarValue::Int32(0))),
            RecordField::new(
                "FREQ_GROUP_NAME",
                Value::Scalar(ScalarValue::String("GROUP0".to_string())),
            ),
            RecordField::new("IF_CONV_CHAIN", Value::Scalar(ScalarValue::Int32(0))),
            RecordField::new("MEAS_FREQ_REF", Value::Scalar(ScalarValue::Int32(5))),
            RecordField::new("NAME", Value::Scalar(ScalarValue::String(name.to_string()))),
            RecordField::new("NET_SIDEBAND", Value::Scalar(ScalarValue::Int32(1))),
            RecordField::new("NUM_CHAN", Value::Scalar(ScalarValue::Int32(2))),
            RecordField::new(
                "REF_FREQUENCY",
                Value::Scalar(ScalarValue::Float64(ref_frequency_hz)),
            ),
            RecordField::new("RESOLUTION", Value::Array(width.clone())),
            RecordField::new(
                "TOTAL_BANDWIDTH",
                Value::Scalar(ScalarValue::Float64(2.0e6)),
            ),
        ]))
        .unwrap();
}

fn add_polarization_row(ms: &mut MeasurementSet, corr_types: &[i32]) {
    let table = ms
        .subtable_mut(SubtableId::Polarization)
        .expect("POLARIZATION table");
    let corr_product = match corr_types.len() {
        2 => vec![0, 1, 0, 1],
        4 => vec![0, 0, 1, 1, 0, 1, 0, 1],
        len => vec![0; len * 2],
    };
    table
        .add_row(RecordValue::new(vec![
            RecordField::new(
                "CORR_PRODUCT",
                Value::Array(ArrayValue::Int32(
                    ArrayD::from_shape_vec(vec![2, corr_types.len()], corr_product).unwrap(),
                )),
            ),
            RecordField::new(
                "CORR_TYPE",
                Value::Array(ArrayValue::Int32(
                    ArrayD::from_shape_vec(vec![corr_types.len()], corr_types.to_vec()).unwrap(),
                )),
            ),
            RecordField::new("FLAG_ROW", Value::Scalar(ScalarValue::Bool(false))),
            RecordField::new(
                "NUM_CORR",
                Value::Scalar(ScalarValue::Int32(corr_types.len() as i32)),
            ),
        ]))
        .unwrap();
}

fn add_data_description_row(ms: &mut MeasurementSet, polarization_id: i32, spw_id: i32) {
    let table = ms
        .subtable_mut(SubtableId::DataDescription)
        .expect("DATA_DESCRIPTION table");
    table
        .add_row(RecordValue::new(vec![
            RecordField::new("FLAG_ROW", Value::Scalar(ScalarValue::Bool(false))),
            RecordField::new(
                "POLARIZATION_ID",
                Value::Scalar(ScalarValue::Int32(polarization_id)),
            ),
            RecordField::new(
                "SPECTRAL_WINDOW_ID",
                Value::Scalar(ScalarValue::Int32(spw_id)),
            ),
        ]))
        .unwrap();
}

fn add_state_row(ms: &mut MeasurementSet, obs_mode: &str) {
    let table = ms.subtable_mut(SubtableId::State).expect("STATE table");
    table
        .add_row(RecordValue::new(vec![
            RecordField::new("CAL", Value::Scalar(ScalarValue::Float64(0.0))),
            RecordField::new("FLAG_ROW", Value::Scalar(ScalarValue::Bool(false))),
            RecordField::new("LOAD", Value::Scalar(ScalarValue::Float64(0.0))),
            RecordField::new(
                "OBS_MODE",
                Value::Scalar(ScalarValue::String(obs_mode.to_string())),
            ),
            RecordField::new("REF", Value::Scalar(ScalarValue::Bool(false))),
            RecordField::new("SIG", Value::Scalar(ScalarValue::Bool(true))),
            RecordField::new("SUB_SCAN", Value::Scalar(ScalarValue::Int32(0))),
        ]))
        .unwrap();
}

fn add_antenna_rows(ms: &mut MeasurementSet) {
    let mut antenna = ms.antenna_mut().expect("ANTENNA accessor");
    antenna
        .add_antenna(
            "VLA01",
            "N01",
            "GROUND-BASED",
            "ALT-AZ",
            [0.0, 10.0, 20.0],
            [0.0, 0.0, 0.0],
            25.0,
        )
        .unwrap();
    antenna
        .add_antenna(
            "VLA02",
            "N02",
            "GROUND-BASED",
            "ALT-AZ",
            [1.0, 11.0, 21.0],
            [0.0, 0.0, 0.0],
            25.0,
        )
        .unwrap();
}

#[allow(clippy::too_many_arguments)]
fn add_main_row(
    ms: &mut MeasurementSet,
    time: f64,
    antenna2: i32,
    field_id: i32,
    scan_number: i32,
    data_desc_id: i32,
    state_id: i32,
    uvw: [f64; 3],
) {
    let schema = ms.main_table().schema().unwrap().clone();
    let fields = schema
        .columns()
        .iter()
        .map(|column| match column.name() {
            "ANTENNA1" => RecordField::new("ANTENNA1", Value::Scalar(ScalarValue::Int32(0))),
            "ANTENNA2" => RecordField::new("ANTENNA2", Value::Scalar(ScalarValue::Int32(antenna2))),
            "ARRAY_ID" => RecordField::new("ARRAY_ID", Value::Scalar(ScalarValue::Int32(0))),
            "DATA_DESC_ID" => RecordField::new(
                "DATA_DESC_ID",
                Value::Scalar(ScalarValue::Int32(data_desc_id)),
            ),
            "EXPOSURE" => RecordField::new("EXPOSURE", Value::Scalar(ScalarValue::Float64(15.0))),
            "FIELD_ID" => RecordField::new("FIELD_ID", Value::Scalar(ScalarValue::Int32(field_id))),
            "INTERVAL" => RecordField::new("INTERVAL", Value::Scalar(ScalarValue::Float64(15.0))),
            "OBSERVATION_ID" => {
                RecordField::new("OBSERVATION_ID", Value::Scalar(ScalarValue::Int32(0)))
            }
            "SCAN_NUMBER" => RecordField::new(
                "SCAN_NUMBER",
                Value::Scalar(ScalarValue::Int32(scan_number)),
            ),
            "STATE_ID" => RecordField::new("STATE_ID", Value::Scalar(ScalarValue::Int32(state_id))),
            "TIME" => RecordField::new("TIME", Value::Scalar(ScalarValue::Float64(time))),
            "TIME_CENTROID" => {
                RecordField::new("TIME_CENTROID", Value::Scalar(ScalarValue::Float64(time)))
            }
            "WEIGHT" => RecordField::new(
                "WEIGHT",
                default_array_value(main_column_def("WEIGHT").data_type, vec![2]),
            ),
            "SIGMA" => RecordField::new(
                "SIGMA",
                default_array_value(main_column_def("SIGMA").data_type, vec![2]),
            ),
            "UVW" => RecordField::new(
                "UVW",
                Value::Array(ArrayValue::Float64(
                    ArrayD::from_shape_vec(vec![3], uvw.to_vec()).unwrap(),
                )),
            ),
            name => RecordField::new(name, default_value_for_def(main_column_def(name))),
        })
        .collect::<Vec<_>>();
    ms.main_table_mut()
        .add_row(RecordValue::new(fields))
        .unwrap();
}

fn set_main_row_flag_matrix(ms: &mut MeasurementSet, row: usize, flags: ArrayD<bool>) {
    ms.main_table_mut()
        .set_cell(row, "FLAG", Value::Array(ArrayValue::Bool(flags)))
        .unwrap();
}

fn set_main_row_data_matrix(ms: &mut MeasurementSet, row: usize, data: ArrayD<Complex32>) {
    ms.main_table_mut()
        .set_cell(row, "DATA", Value::Array(ArrayValue::Complex32(data)))
        .unwrap();
}

fn main_column_def(name: &str) -> &'static ColumnDef {
    schema::main_table::REQUIRED_COLUMNS
        .iter()
        .chain(schema::main_table::OPTIONAL_COLUMNS.iter())
        .find(|column| column.name == name)
        .expect("main column definition")
}

fn default_value_for_def(column: &ColumnDef) -> Value {
    match column.column_kind {
        ColumnKind::Scalar => match column.data_type {
            PrimitiveType::Bool => Value::Scalar(ScalarValue::Bool(false)),
            PrimitiveType::UInt8 => Value::Scalar(ScalarValue::UInt8(0)),
            PrimitiveType::UInt16 => Value::Scalar(ScalarValue::UInt16(0)),
            PrimitiveType::UInt32 => Value::Scalar(ScalarValue::UInt32(0)),
            PrimitiveType::Int16 => Value::Scalar(ScalarValue::Int16(0)),
            PrimitiveType::Int32 => Value::Scalar(ScalarValue::Int32(0)),
            PrimitiveType::Int64 => Value::Scalar(ScalarValue::Int64(0)),
            PrimitiveType::Float32 => Value::Scalar(ScalarValue::Float32(0.0)),
            PrimitiveType::Float64 => Value::Scalar(ScalarValue::Float64(0.0)),
            PrimitiveType::Complex32 => Value::Scalar(ScalarValue::Complex32(Default::default())),
            PrimitiveType::Complex64 => Value::Scalar(ScalarValue::Complex64(Default::default())),
            PrimitiveType::String => Value::Scalar(ScalarValue::String(String::new())),
        },
        ColumnKind::FixedArray { shape } => default_array_value(column.data_type, shape.to_vec()),
        ColumnKind::VariableArray { ndim } => default_array_value(column.data_type, vec![1; ndim]),
    }
}

fn default_array_value(data_type: PrimitiveType, shape: Vec<usize>) -> Value {
    let total = shape.iter().product();
    let array = match data_type {
        PrimitiveType::Bool => {
            ArrayValue::Bool(ArrayD::from_shape_vec(shape, vec![false; total]).unwrap())
        }
        PrimitiveType::UInt8 => {
            ArrayValue::UInt8(ArrayD::from_shape_vec(shape, vec![0; total]).unwrap())
        }
        PrimitiveType::UInt16 => {
            ArrayValue::UInt16(ArrayD::from_shape_vec(shape, vec![0; total]).unwrap())
        }
        PrimitiveType::UInt32 => {
            ArrayValue::UInt32(ArrayD::from_shape_vec(shape, vec![0; total]).unwrap())
        }
        PrimitiveType::Int16 => {
            ArrayValue::Int16(ArrayD::from_shape_vec(shape, vec![0; total]).unwrap())
        }
        PrimitiveType::Int32 => {
            ArrayValue::Int32(ArrayD::from_shape_vec(shape, vec![0; total]).unwrap())
        }
        PrimitiveType::Int64 => {
            ArrayValue::Int64(ArrayD::from_shape_vec(shape, vec![0; total]).unwrap())
        }
        PrimitiveType::Float32 => {
            ArrayValue::Float32(ArrayD::from_shape_vec(shape, vec![0.0; total]).unwrap())
        }
        PrimitiveType::Float64 => {
            ArrayValue::Float64(ArrayD::from_shape_vec(shape, vec![0.0; total]).unwrap())
        }
        PrimitiveType::Complex32 => ArrayValue::Complex32(
            ArrayD::from_shape_vec(shape, vec![Default::default(); total]).unwrap(),
        ),
        PrimitiveType::Complex64 => ArrayValue::Complex64(
            ArrayD::from_shape_vec(shape, vec![Default::default(); total]).unwrap(),
        ),
        PrimitiveType::String => {
            ArrayValue::String(ArrayD::from_shape_vec(shape, vec![String::new(); total]).unwrap())
        }
    };
    Value::Array(array)
}

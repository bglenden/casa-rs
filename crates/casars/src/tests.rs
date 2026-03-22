use std::path::{Path, PathBuf};
use std::sync::{Mutex, MutexGuard, OnceLock};
use std::time::Duration;

use casacore_ms::column_def::{ColumnDef, ColumnKind};
use casacore_ms::listobs::cli::command_schema;
use casacore_ms::schema;
use casacore_ms::{MeasurementSet, MeasurementSetBuilder, SubtableId};
use casacore_types::{
    ArrayD, ArrayValue, PrimitiveType, RecordField, RecordValue, ScalarValue, Value, quanta::MvTime,
};
use crossterm::event::{KeyCode, KeyEvent, KeyModifiers, MouseButton, MouseEvent, MouseEventKind};
use ratatui::Terminal;
use ratatui::backend::TestBackend;
use tempfile::tempdir;

use crate::app::{AppState, PaneFocus, ResultTab};
use crate::config::{ConfigStore, ThemeMode};
use crate::registry::listobs_app;
use crate::ui;

#[test]
fn renders_idle_layout_with_ready_status() {
    let (_temp, app) = test_app();
    let rendered = render_app(&app, 100, 30);
    assert!(rendered.contains("casars"));
    assert!(rendered.contains("MeasurementSet / ListObs"));
    assert!(rendered.contains("MeasurementSet Path"));
    assert!(rendered.contains("Ready. Press r to run listobs."));
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

    let schema = command_schema("listobs");
    let mut first = AppState::from_schema_with_config(
        listobs_app(),
        schema.clone(),
        ConfigStore::load_for_tests(config_path.clone()),
    );
    assert_eq!(first.theme_mode_for_test(), ThemeMode::DenseAnsi);
    first.handle_key_event(KeyEvent::new(KeyCode::Char('t'), KeyModifiers::NONE));
    assert_eq!(first.theme_mode_for_test(), ThemeMode::RichPanel);

    let second = AppState::from_schema_with_config(
        listobs_app(),
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
        listobs_app(),
        command_schema("listobs"),
        ConfigStore::load_for_tests(config_path.clone()),
    );
    let layout = ui::compute_layout(ratatui::layout::Rect::new(0, 0, 120, 30), &app);

    app.handle_mouse_event(
        mouse(
            MouseEventKind::Down(MouseButton::Left),
            layout.divider.x,
            layout.divider.y,
        ),
        &layout,
    );
    app.handle_mouse_event(
        mouse(
            MouseEventKind::Drag(MouseButton::Left),
            layout.body.x + 72,
            layout.divider.y,
        ),
        &layout,
    );
    app.handle_mouse_event(
        mouse(
            MouseEventKind::Up(MouseButton::Left),
            layout.body.x + 72,
            layout.divider.y,
        ),
        &layout,
    );
    assert!(app.pane_split_ratio_for_test() > 0.55);

    let reloaded = AppState::from_schema_with_config(
        listobs_app(),
        command_schema("listobs"),
        ConfigStore::load_for_tests(config_path),
    );
    assert!(reloaded.pane_split_ratio_for_test() > 0.55);
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

    let rendered = render_app(&app, 100, 30);
    assert!(rendered.contains("/tmp/example.ms"));
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
    let (_temp, mut app) = test_app();
    let before = app
        .selected_form_text_for_test()
        .expect("selected form text before scroll");
    let layout = ui::compute_layout(ratatui::layout::Rect::new(0, 0, 120, 30), &app);

    app.handle_mouse_event(
        mouse(
            MouseEventKind::ScrollDown,
            layout.form_inner.x + 1,
            layout.form_inner.y + 1,
        ),
        &layout,
    );

    let after = app
        .selected_form_text_for_test()
        .expect("selected form text after scroll");
    assert_ne!(before, after);
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
    let rendered = render_app(&app, 100, 30);
    assert!(rendered.contains("t theme"));
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
    app.handle_key_event(KeyEvent::new(KeyCode::Char(']'), KeyModifiers::NONE));
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

fn test_app() -> (tempfile::TempDir, AppState) {
    let temp = tempdir().expect("tempdir");
    let config = ConfigStore::load_for_tests(temp.path().join("casars.toml"));
    let app = AppState::from_schema_with_config(listobs_app(), command_schema("listobs"), config);
    (temp, app)
}

fn mouse(kind: MouseEventKind, column: u16, row: u16) -> MouseEvent {
    MouseEvent {
        kind,
        column,
        row,
        modifiers: KeyModifiers::NONE,
    }
}

fn launcher_env_lock() -> MutexGuard<'static, ()> {
    static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
    LOCK.get_or_init(|| Mutex::new(()))
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
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

#[cfg(unix)]
fn write_fake_listobs_script(root: &Path, body: &str) -> PathBuf {
    use std::fs;
    use std::os::unix::fs::PermissionsExt;

    let schema_json = command_schema("listobs")
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

fn create_fixture_ms(root: &Path) -> PathBuf {
    let ms_path = root.join("listobs_fixture.ms");
    let mut ms = MeasurementSet::create(&ms_path, MeasurementSetBuilder::new()).expect("create MS");
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

fn main_column_def(name: &str) -> &'static ColumnDef {
    schema::main_table::REQUIRED_COLUMNS
        .iter()
        .find(|column| column.name == name)
        .expect("required main column definition")
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

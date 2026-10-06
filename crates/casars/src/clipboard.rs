// SPDX-License-Identifier: LGPL-3.0-or-later
use std::fs;
use std::path::PathBuf;

pub(crate) fn copy_text(text: &str) -> Result<(), String> {
    if let Some(path) = clipboard_override_path() {
        fs::write(&path, text)
            .map_err(|error| format!("write clipboard override {}: {error}", path.display()))?;
        return Ok(());
    }

    let mut clipboard =
        arboard::Clipboard::new().map_err(|error| format!("open system clipboard: {error}"))?;
    clipboard
        .set_text(text.to_string())
        .map_err(|error| format!("set system clipboard: {error}"))
}

fn clipboard_override_path() -> Option<PathBuf> {
    #[cfg(test)]
    if let Some(path) = TEST_CLIPBOARD_FILE.with(|cell| cell.borrow().clone()) {
        return Some(path);
    }
    std::env::var_os("CASARS_TEST_CLIPBOARD_FILE").map(PathBuf::from)
}

#[cfg(test)]
thread_local! {
    static TEST_CLIPBOARD_FILE: std::cell::RefCell<Option<PathBuf>> =
        const { std::cell::RefCell::new(None) };
}

/// Bind the clipboard override for the calling test thread only.
///
/// A process-global environment override races with concurrent TUI tests that
/// finalize a mouse selection and would otherwise write into another test's
/// clipboard path.
#[cfg(test)]
pub(crate) fn set_test_clipboard_file(path: Option<PathBuf>) {
    TEST_CLIPBOARD_FILE.with(|cell| *cell.borrow_mut() = path);
}

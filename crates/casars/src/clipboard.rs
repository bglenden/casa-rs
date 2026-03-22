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
    std::env::var_os("CASARS_TEST_CLIPBOARD_FILE").map(PathBuf::from)
}

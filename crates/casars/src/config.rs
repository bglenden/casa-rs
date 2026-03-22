// SPDX-License-Identifier: LGPL-3.0-or-later
use std::fs;
use std::path::{Path, PathBuf};

use directories::ProjectDirs;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum ThemeMode {
    DenseAnsi,
    RichPanel,
}

impl ThemeMode {
    pub(crate) fn toggle(self) -> Self {
        match self {
            Self::DenseAnsi => Self::RichPanel,
            Self::RichPanel => Self::DenseAnsi,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct UiConfig {
    pub theme_mode: ThemeMode,
    pub pane_split_ratio: f32,
}

impl Default for UiConfig {
    fn default() -> Self {
        Self {
            theme_mode: ThemeMode::DenseAnsi,
            pane_split_ratio: 0.42,
        }
    }
}

#[derive(Debug, Clone)]
pub(crate) struct ConfigStore {
    path: Option<PathBuf>,
    config: UiConfig,
}

impl ConfigStore {
    pub(crate) fn load_default() -> Self {
        let path = default_config_path();
        Self::load_at(path)
    }

    #[cfg(test)]
    pub(crate) fn load_for_tests(path: PathBuf) -> Self {
        Self::load_at(Some(path))
    }

    fn load_at(path: Option<PathBuf>) -> Self {
        let config = path
            .as_deref()
            .and_then(read_config_file)
            .unwrap_or_default();
        Self { path, config }
    }

    pub(crate) fn theme_mode(&self) -> ThemeMode {
        self.config.theme_mode
    }

    pub(crate) fn pane_split_ratio(&self) -> f32 {
        self.config.pane_split_ratio
    }

    pub(crate) fn set_theme_mode(&mut self, theme_mode: ThemeMode) {
        self.config.theme_mode = theme_mode;
        let _ = self.save();
    }

    pub(crate) fn set_pane_split_ratio(&mut self, pane_split_ratio: f32) {
        self.config.pane_split_ratio = pane_split_ratio.clamp(0.25, 0.75);
        let _ = self.save();
    }

    fn save(&self) -> Result<(), String> {
        let Some(path) = &self.path else {
            return Ok(());
        };
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent).map_err(|error| {
                format!("create config directory {}: {error}", parent.display())
            })?;
        }
        let content = toml::to_string_pretty(&self.config)
            .map_err(|error| format!("serialize casars config: {error}"))?;
        fs::write(path, content)
            .map_err(|error| format!("write config {}: {error}", path.display()))
    }
}

fn default_config_path() -> Option<PathBuf> {
    let project_dirs = ProjectDirs::from("", "", "casa-rs")?;
    Some(project_dirs.config_dir().join("casars.toml"))
}

fn read_config_file(path: &Path) -> Option<UiConfig> {
    let content = fs::read_to_string(path).ok()?;
    toml::from_str(&content).ok().map(normalize_config)
}

fn normalize_config(mut config: UiConfig) -> UiConfig {
    config.pane_split_ratio = config.pane_split_ratio.clamp(0.25, 0.75);
    config
}

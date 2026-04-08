// SPDX-License-Identifier: LGPL-3.0-or-later
//! Shared presentation helpers for the framework-owned `casars` shell family.

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct InspectOverviewDisplay {
    pub dataset_path: Option<String>,
    pub current_view: String,
    pub current_plot: Option<String>,
    pub tab_labels: Vec<String>,
    pub guidance: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct BrowserOverviewDisplay {
    pub root_path: String,
    pub active_view: String,
    pub shell_tab: String,
    pub status: String,
    pub browser_kind: Option<String>,
    pub guidance: String,
}

pub(crate) fn render_inspect_overview_lines(entry: &InspectOverviewDisplay) -> Vec<String> {
    let mut lines = vec!["Inspect Session".to_string()];
    if let Some(path) = &entry.dataset_path {
        lines.push(format!("Dataset: {path}"));
    }
    lines.push(format!("Current view: {}", entry.current_view));
    if let Some(plot) = &entry.current_plot {
        lines.push(format!("Current plot: {plot}"));
    }
    if !entry.tab_labels.is_empty() {
        lines.push(format!("Tabs: {}", entry.tab_labels.join(", ")));
    }
    lines.push(String::new());
    lines.push("Shell".to_string());
    lines.push(entry.guidance.clone());
    lines
}

pub(crate) fn render_browser_overview_lines(entry: &BrowserOverviewDisplay) -> Vec<String> {
    let mut lines = vec![
        "Browser Session".to_string(),
        format!("Path: {}", entry.root_path),
        format!(
            "View: {}   Shell tab: {}",
            entry.active_view, entry.shell_tab
        ),
        format!("Status: {}", entry.status),
        String::new(),
        "Shell".to_string(),
        entry.guidance.clone(),
    ];
    if let Some(kind) = &entry.browser_kind {
        lines.push(format!("Browser kind: {kind}"));
    }
    lines
}

#[cfg(test)]
mod tests {
    use super::{
        BrowserOverviewDisplay, InspectOverviewDisplay, render_browser_overview_lines,
        render_inspect_overview_lines,
    };

    #[test]
    fn render_inspect_overview_lines_includes_tabs_and_guidance() {
        let lines = render_inspect_overview_lines(&InspectOverviewDisplay {
            dataset_path: Some("/tmp/example.ms".to_string()),
            current_view: "Fields".to_string(),
            current_plot: Some("Amplitude vs Time".to_string()),
            tab_labels: vec![
                "Overview".to_string(),
                "Data".to_string(),
                "Plots".to_string(),
            ],
            guidance: "Use Views for summary tables and Plots for the active preset.".to_string(),
        });
        assert_eq!(lines[0], "Inspect Session");
        assert!(lines.iter().any(|line| line.contains("/tmp/example.ms")));
        assert!(
            lines
                .iter()
                .any(|line| line.contains("Current view: Fields"))
        );
        assert!(
            lines
                .iter()
                .any(|line| line.contains("Current plot: Amplitude vs Time"))
        );
        assert!(
            lines
                .iter()
                .any(|line| line.contains("Tabs: Overview, Data, Plots"))
        );
    }

    #[test]
    fn render_browser_overview_lines_includes_kind() {
        let lines = render_browser_overview_lines(&BrowserOverviewDisplay {
            root_path: "/tmp/example.image".to_string(),
            active_view: "Metadata".to_string(),
            shell_tab: "Overview".to_string(),
            status: "ready".to_string(),
            browser_kind: Some("image".to_string()),
            guidance: "Use Structure and Content for the selected browser view.".to_string(),
        });
        assert_eq!(lines[0], "Browser Session");
        assert!(lines.iter().any(|line| line.contains("/tmp/example.image")));
        assert!(
            lines
                .iter()
                .any(|line| line.contains("View: Metadata   Shell tab: Overview"))
        );
        assert!(
            lines
                .iter()
                .any(|line| line.contains("Browser kind: image"))
        );
    }
}

// SPDX-License-Identifier: LGPL-3.0-or-later
//! File/screen/both policies across all composed ALMA/ACA/TP figures.
use casa_simulation::{
    ExecutionControls, SessionResources,
    simalma::{simalma, simalma_with_plots},
};
use serde_json::json;
#[path = "common/simalma_fixture.rs"]
mod fixture;

#[test]
fn composed_figures_follow_casas_file_screen_both_and_none_policies() {
    let resources = SessionResources::from_controls(&ExecutionControls::default()).unwrap();
    let (scratch, mut request, _) = fixture::fixture(resources);
    let payload = scratch.path().join("arrays.bin");
    std::fs::write(&payload, b"previous invocation").unwrap();
    for mode in ["file", "screen", "both", "none"] {
        request.parameters.insert("graphics".into(), json!(mode));
        let result = if mode == "none" {
            simalma(&request).unwrap()
        } else {
            simalma_with_plots(&request, &payload).unwrap()
        };
        if mode == "none" {
            assert!(result.plots.is_empty());
            continue;
        }
        assert_eq!(result.plots.len(), 14);
        let bytes = std::fs::metadata(&payload).unwrap().len();
        for figure in result.plots {
            assert_eq!(
                figure.output_file.is_some(),
                matches!(mode, "file" | "both")
            );
            assert_eq!(figure.show, matches!(mode, "screen" | "both"));
            figure.document.validate_payload(bytes).unwrap();
        }
    }

    // CASA retains '.noisy' when searching model names. With no exact
    // match it falls back to the first sorted candidate (ACA), as recorded
    // independently by the full noisy M51 graphics oracle.
    let project = scratch.path().join("combo");
    let noisy = project.join("combo.alma.test.noisy.ms");
    std::fs::rename(project.join("combo.alma.test.ms"), &noisy).unwrap();
    let result = casa_simulation::simanalyze::simanalyze_with_plots(
        &casa_simulation::simanalyze::SimanalyzeRequest {
            resources,
            parameters: json!({"project":project,"vis":noisy,"graphics":"file"})
                .as_object()
                .unwrap()
                .clone(),
        },
        &payload,
    )
    .unwrap();
    assert!(
        result.plots[0].document.panels[0]
            .title
            .ends_with("combo.aca.test.skymodel.flat.regrid")
    );
}

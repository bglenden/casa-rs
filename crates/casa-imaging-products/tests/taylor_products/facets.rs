// SPDX-License-Identifier: LGPL-3.0-or-later

use super::*;

#[test]
fn taylor_facets_publish_each_signed_weight_at_its_facet_pixel() {
    let problem = taylor_problem_with_beam_terms(
        32,
        &TAYLOR_PRODUCTS,
        InstrumentResponse::Scalar,
        0.1,
        RestoringBeamPolicy::Common,
        TERMS,
        FacetLayout::Regular {
            columns: 2,
            rows: 2,
        },
    );
    let join = run_round(&problem);
    let sums = std::collections::BTreeMap::from([(
        0,
        vec![
            vec![16.0, -4.0, 2.0],
            vec![12.0, -2.0, 1.0],
            vec![8.0, -1.0, 0.5],
            vec![4.0, -0.5, 0.25],
        ],
    )]);
    let inputs =
        ContinuumProductInputs::from_major_cycle(&problem, &join).with_facet_sum_weights(&sums);
    let planned = PlannedContinuumGeneration::new(&inputs, &ContinuumProductControls::default())
        .expect("facet products");
    let output = ImageOutput::for_problem(&problem);
    let produced =
        produce_continuum_members(&planned, &inputs, full_window(&planned), &(), &output)
            .expect("publish facet products");
    let generated = GeneratedProducts::from_output(&planned, &produced, &output);
    for (term, expected) in [
        [16.0, 8.0, 12.0, 4.0],
        [-4.0, -1.0, -2.0, -0.5],
        [2.0, 0.5, 1.0, 0.25],
    ]
    .into_iter()
    .enumerate()
    {
        let sumwt = member(&generated, &format!(".sumwt.tt{term}"));
        assert_eq!(sumwt.payload(), expected, "moment {term}");
    }
}

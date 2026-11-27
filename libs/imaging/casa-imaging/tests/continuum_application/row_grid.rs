// SPDX-License-Identifier: LGPL-3.0-or-later

use super::*;

fn add_processor(ms: &mut MeasurementSet, mode: i32) {
    ms.subtable_mut(SubtableId::Processor)
        .expect("PROCESSOR")
        .add_row(required_row(
            schema::processor::REQUIRED_COLUMNS,
            &[
                (
                    "TYPE",
                    Value::Scalar(ScalarValue::String("CORRELATOR".into())),
                ),
                ("MODE_ID", int(mode)),
            ],
        ))
        .expect("processor");
}

/// Two acquisitions of the same baseline at the same time, with a
/// different field, scan, or processor. Both are phase-centred, with
/// per-hand weights 1 and 3 and fluxes 2 and 6 Jy. The dirty image must
/// therefore have 5 Jy at the phase centre and a sum of weights of 8.
/// A regular grid with the same visibilities, UVW and weights at distinct
/// times supplies the full-plane comparison.
#[test]
fn simultaneous_acquisitions_contribute_their_own_flux_and_weight() {
    let _execution_guard = EXECUTION_LOCK.lock().expect("execution lock");
    let root = tempfile::tempdir().expect("root");
    let mut reference: Option<PathBuf> = None;
    for (column, label, moved) in [
        ("TIME", "regular", 0),
        ("FIELD_ID", "field", 1),
        ("SCAN_NUMBER", "scan", 2),
        ("PROCESSOR_ID", "processor", 1),
    ] {
        let path = unequal_linear_parallel_hand_measurement_set(
            root.path(),
            &format!("{label}.ms"),
            [1.0, 1.0],
        );
        let mut ms = MeasurementSet::open(&path).expect("fixture");
        if column == "FIELD_ID" {
            let fields = ms.subtable_mut(SubtableId::Field).expect("FIELD");
            let field = fields.row_accessor().row(0).expect("first field").clone();
            fields.add_row(field).expect("second field");
        }
        if column == "PROCESSOR_ID" {
            add_processor(&mut ms, 0);
            add_processor(&mut ms, 1);
        }
        let main = ms.main_table_mut();
        main.row_accessor_mut()
            .set_cell(0, "PROCESSOR_ID", int(0))
            .expect("first processor");
        main.add_row(main.row_accessor().row(0).expect("first row").clone())
            .expect("second acquisition");
        for (row, flux, weight) in [(0, 2.0, 1.0), (1, 6.0, 3.0)] {
            let data = ArrayD::from_elem(vec![4, 1], Complex32::new(flux, 0.0));
            let weights = ArrayD::from_elem(vec![4], weight);
            main.row_accessor_mut()
                .set_cell(row, "DATA", Value::Array(ArrayValue::Complex32(data)))
                .expect("DATA");
            main.row_accessor_mut()
                .set_cell(row, "WEIGHT", Value::Array(ArrayValue::Float32(weights)))
                .expect("WEIGHT");
        }
        if column == "TIME" {
            for time_column in ["TIME", "TIME_CENTROID"] {
                let ScalarValue::Float64(time) = main
                    .cell_accessor(0, time_column)
                    .and_then(|cell| cell.scalar())
                    .expect(time_column)
                else {
                    panic!("timestamp");
                };
                let next = float(time + 10.0);
                main.row_accessor_mut()
                    .set_cell(1, time_column, next)
                    .expect(time_column);
            }
        } else {
            main.row_accessor_mut()
                .set_cell(1, column, int(moved))
                .expect("second row's key");
        }
        ms.save().expect("save");
        drop(ms);

        let image = root.path().join(label);
        let imaging = request(
            &path,
            &image,
            json!({
                "niter": 0, "field": "", "weighting": "natural",
                "backend": "cpu", "gridprecision": "f64",
            }),
        );
        let outcome = casa_imaging::execute(
            &imaging,
            context_with(ResourcePolicy::Explicit {
                workers: 1,
                memory: 512 << 20,
            }),
        )
        .unwrap_or_else(|error| panic!("{label}: {error}"));
        assert_dirty_products(&image, &outcome.product_names());
        let sumwt = product_plane(&image, ".sumwt")[[0, 0, 0, 0]];
        let flux = product_plane(&image, ".residual")[[8, 8, 0, 0]];
        eprintln!(
            "{label}: sumwt {sumwt}, centre flux {flux}; {:?} {:?}, {} worker, {} bytes",
            outcome.summary.backend,
            outcome.summary.precision,
            outcome.summary.workers,
            outcome.summary.capacity_bytes,
        );
        assert!((sumwt - 8.0).abs() <= 8.0 * 5e-5, "{label}: sumwt {sumwt}");
        assert!((flux - 5.0).abs() <= 5.0 * 1e-3, "{label}: flux {flux}");
        if let Some(reference) = &reference {
            for suffix in DIRTY_PRODUCT_SUFFIXES {
                let nrms = casa_test_support::image_nrms(
                    &reference.with_extension(&suffix[1..]),
                    &image.with_extension(&suffix[1..]),
                )
                .expect("comparable products");
                eprintln!("{label} {suffix}: NRMS {nrms:.6e}");
                assert!(nrms <= casa_test_support::IMAGING_NRMS_TOLERANCE);
            }
        } else {
            reference = Some(image);
        }
    }
}

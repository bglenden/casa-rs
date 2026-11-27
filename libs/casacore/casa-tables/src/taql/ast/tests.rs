// SPDX-License-Identifier: LGPL-3.0-or-later
use super::*;

#[test]
fn aggregate_from_name_basic() {
    assert_eq!(aggregate_from_name("gcount"), Some(AggregateFunc::Count));
    assert_eq!(aggregate_from_name("GMIN"), Some(AggregateFunc::Min));
    assert_eq!(aggregate_from_name("gmax"), Some(AggregateFunc::Max));
    assert_eq!(aggregate_from_name("gmean"), Some(AggregateFunc::Avg));
    assert_eq!(aggregate_from_name("gfirst"), Some(AggregateFunc::First));
    assert_eq!(aggregate_from_name("glast"), Some(AggregateFunc::Last));
    assert_eq!(
        aggregate_from_name("gproduct"),
        Some(AggregateFunc::Product)
    );
    assert_eq!(aggregate_from_name("gsumsqr"), Some(AggregateFunc::SumSqr));
    assert_eq!(aggregate_from_name("grms"), Some(AggregateFunc::Rms));
    assert_eq!(aggregate_from_name("gany"), Some(AggregateFunc::Any));
    assert_eq!(aggregate_from_name("gall"), Some(AggregateFunc::All));
    assert_eq!(aggregate_from_name("gntrue"), Some(AggregateFunc::NTrue));
    assert_eq!(aggregate_from_name("gnfalse"), Some(AggregateFunc::NFalse));
    assert_eq!(aggregate_from_name("gmedian"), Some(AggregateFunc::Median));
    assert_eq!(
        aggregate_from_name("gfractile"),
        Some(AggregateFunc::Fractile)
    );
}

#[test]
fn aggregate_from_name_aliases() {
    assert_eq!(aggregate_from_name("gavg"), Some(AggregateFunc::Avg));
    assert_eq!(
        aggregate_from_name("gvariance"),
        Some(AggregateFunc::Variance)
    );
    assert_eq!(
        aggregate_from_name("gsamplevariance"),
        Some(AggregateFunc::SampleVariance)
    );
    assert_eq!(aggregate_from_name("gstddev"), Some(AggregateFunc::StdDev));
    assert_eq!(
        aggregate_from_name("gsamplestddev"),
        Some(AggregateFunc::SampleStdDev)
    );
}

#[test]
fn aggregate_from_name_rejects_non_aggregates() {
    assert_eq!(aggregate_from_name("foo"), None);
    assert_eq!(aggregate_from_name("g"), None);
    assert_eq!(aggregate_from_name(""), None);
    assert_eq!(aggregate_from_name("gfoo"), None);
    assert_eq!(aggregate_from_name("gabcdef"), None);
}

#[test]
fn aggregate_from_name_accepts_unprefixed() {
    // The original 5 aggregates are recognized without g-prefix
    assert_eq!(aggregate_from_name("count"), Some(AggregateFunc::Count));
    assert_eq!(aggregate_from_name("sum"), Some(AggregateFunc::Sum));
    assert_eq!(aggregate_from_name("avg"), Some(AggregateFunc::Avg));
    assert_eq!(aggregate_from_name("min"), Some(AggregateFunc::Min));
    assert_eq!(aggregate_from_name("max"), Some(AggregateFunc::Max));
}

#[test]
fn aggregate_display_roundtrip() {
    for func in [
        AggregateFunc::Count,
        AggregateFunc::Sum,
        AggregateFunc::Avg,
        AggregateFunc::Min,
        AggregateFunc::Max,
        AggregateFunc::First,
        AggregateFunc::Last,
        AggregateFunc::Product,
        AggregateFunc::SumSqr,
        AggregateFunc::Variance,
        AggregateFunc::SampleVariance,
        AggregateFunc::StdDev,
        AggregateFunc::SampleStdDev,
        AggregateFunc::Rms,
        AggregateFunc::Any,
        AggregateFunc::All,
        AggregateFunc::NTrue,
        AggregateFunc::NFalse,
        AggregateFunc::Median,
        AggregateFunc::Fractile,
        AggregateFunc::Aggr,
        AggregateFunc::RowId,
        AggregateFunc::Hist,
    ] {
        let displayed = func.to_string();
        let parsed = aggregate_from_name(&displayed);
        assert_eq!(parsed, Some(func), "roundtrip failed for {displayed}");
    }
}

#[test]
fn statement_display_covers_non_select_variants() {
    let statements = [
        "CALC meas.help('freq')",
        "ALTER TABLE ADD COLUMN field STRING",
        "ALTER TABLE DROP COLUMN field",
        "ALTER TABLE RENAME COLUMN old TO new",
        "ALTER TABLE ADD ROW 3",
        "ALTER TABLE SET KEYWORD source = 'VLA'",
        "UPDATE mytab SET flux = 2.0 WHERE id = 1 LIMIT 1",
        "INSERT INTO mytab (id, flux) VALUES (1, 2.0), (2, 3.0)",
        "DELETE FROM mytab WHERE id = 1 LIMIT 1",
        "COUNT SELECT * WHERE id > 0",
        "CREATE TABLE scratch (id INT32, name STRING)",
        "DROP TABLE scratch",
    ];

    for sql in statements {
        let statement = crate::taql::parse(sql).unwrap();
        let displayed = statement.to_string();
        assert!(
            !displayed.is_empty(),
            "display output should not be empty for {sql}"
        );
    }
}

#[test]
fn select_display_covers_clauses_and_aliases() {
    let statement = crate::taql::parse(
        "USING STYLE PYTHON SELECT DISTINCT t.id AS source_id, SUM(t.flux) \
         FROM t AS t LEFT JOIN t AS t2 ON t.id = t2.id WHERE t.id > 0 \
         GROUP BY t.id HAVING SUM(t.flux) > 1 ORDER BY source_id DESC \
         LIMIT 2 OFFSET 1 GIVING output AS MEMORY",
    )
    .unwrap();
    let displayed = statement.to_string();
    assert!(displayed.contains("USING STYLE PYTHON SELECT DISTINCT"));
    assert!(displayed.contains("LEFT JOIN"));
    assert!(displayed.contains("GROUP BY"));
    assert!(displayed.contains("HAVING"));
    assert!(displayed.contains("ORDER BY"));
    assert!(displayed.contains("LIMIT 2"));
    assert!(displayed.contains("OFFSET 1"));
    assert!(displayed.contains("GIVING output AS MEMORY"));
}

// SPDX-License-Identifier: LGPL-3.0-or-later

use super::*;

fn parse(query: &str) -> Statement {
    Parser::new(query).parse_statement().unwrap()
}

fn parse_err(query: &str) -> TaqlError {
    Parser::new(query).parse_statement().unwrap_err()
}

/// Parse → Display → parse → assert ASTs equal.
fn roundtrip(query: &str) {
    let ast1 = parse(query);
    let displayed = ast1.to_string();
    let ast2 = parse(&displayed);
    assert_eq!(
        ast1, ast2,
        "round-trip failed:\n  input:     {query}\n  displayed: {displayed}"
    );
}

// ── SELECT tests ──

#[test]
fn select_star() {
    let stmt = parse("SELECT *");
    match stmt {
        Statement::Select(s) => {
            assert!(s.columns.is_empty());
            assert!(!s.distinct);
        }
        _ => panic!("expected SELECT"),
    }
}

#[test]
fn select_columns() {
    let stmt = parse("SELECT col1, col2");
    match stmt {
        Statement::Select(s) => {
            assert_eq!(s.columns.len(), 2);
            assert_eq!(
                s.columns[0].expr,
                Expr::ColumnRef(ColumnRef {
                    table: None,
                    column: "col1".to_string()
                })
            );
        }
        _ => panic!("expected SELECT"),
    }
}

#[test]
fn select_alias() {
    let stmt = parse("SELECT col1 AS c1");
    match stmt {
        Statement::Select(s) => {
            assert_eq!(s.columns[0].alias, Some("c1".to_string()));
        }
        _ => panic!("expected SELECT"),
    }
}

#[test]
fn select_where() {
    let stmt = parse("SELECT * WHERE flux > 1.0");
    match stmt {
        Statement::Select(s) => {
            assert!(s.where_clause.is_some());
        }
        _ => panic!("expected SELECT"),
    }
}

#[test]
fn select_compound_where() {
    let stmt = parse("SELECT * WHERE flux > 1.0 AND id < 10 OR name = 'test'");
    match stmt {
        Statement::Select(s) => {
            // Should parse as: (flux > 1.0 AND id < 10) OR (name = 'test')
            assert!(matches!(
                s.where_clause.as_ref().unwrap(),
                Expr::Binary {
                    op: BinaryOp::Or,
                    ..
                }
            ));
        }
        _ => panic!("expected SELECT"),
    }
}

#[test]
fn select_like() {
    let stmt = parse("SELECT * WHERE name LIKE 'test%'");
    match stmt {
        Statement::Select(s) => {
            assert!(matches!(
                s.where_clause.as_ref().unwrap(),
                Expr::Like {
                    negated: false,
                    case_insensitive: false,
                    ..
                }
            ));
        }
        _ => panic!("expected SELECT"),
    }
}

#[test]
fn select_ilike() {
    let stmt = parse("SELECT * WHERE name ILIKE 'test%'");
    match stmt {
        Statement::Select(s) => {
            assert!(matches!(
                s.where_clause.as_ref().unwrap(),
                Expr::Like {
                    negated: false,
                    case_insensitive: true,
                    ..
                }
            ));
        }
        _ => panic!("expected SELECT"),
    }
}

#[test]
fn select_between() {
    let stmt = parse("SELECT * WHERE id BETWEEN 1 AND 10");
    match stmt {
        Statement::Select(s) => {
            assert!(matches!(
                s.where_clause.as_ref().unwrap(),
                Expr::Between { negated: false, .. }
            ));
        }
        _ => panic!("expected SELECT"),
    }
}

#[test]
fn select_in() {
    let stmt = parse("SELECT * WHERE id IN (1, 2, 3)");
    match stmt {
        Statement::Select(s) => match s.where_clause.as_ref().unwrap() {
            Expr::In {
                negated, values, ..
            } => {
                assert!(!negated);
                assert_eq!(values.len(), 3);
            }
            other => panic!("expected IN, got {other:?}"),
        },
        _ => panic!("expected SELECT"),
    }
}

#[test]
fn select_is_null() {
    let stmt = parse("SELECT * WHERE name IS NULL");
    match stmt {
        Statement::Select(s) => {
            assert!(matches!(
                s.where_clause.as_ref().unwrap(),
                Expr::IsNull { negated: false, .. }
            ));
        }
        _ => panic!("expected SELECT"),
    }
}

#[test]
fn select_is_not_null() {
    let stmt = parse("SELECT * WHERE name IS NOT NULL");
    match stmt {
        Statement::Select(s) => {
            assert!(matches!(
                s.where_clause.as_ref().unwrap(),
                Expr::IsNull { negated: true, .. }
            ));
        }
        _ => panic!("expected SELECT"),
    }
}

#[test]
fn select_order_by() {
    let stmt = parse("SELECT * ORDER BY id ASC, flux DESC");
    match stmt {
        Statement::Select(s) => {
            assert_eq!(s.order_by.len(), 2);
            assert!(s.order_by[0].ascending);
            assert!(!s.order_by[1].ascending);
        }
        _ => panic!("expected SELECT"),
    }
}

#[test]
fn select_limit_offset() {
    let stmt = parse("SELECT * LIMIT 10 OFFSET 5");
    match stmt {
        Statement::Select(s) => {
            assert!(s.limit.is_some());
            assert!(s.offset.is_some());
        }
        _ => panic!("expected SELECT"),
    }
}

#[test]
fn select_distinct() {
    let stmt = parse("SELECT DISTINCT col1");
    match stmt {
        Statement::Select(s) => {
            assert!(s.distinct);
        }
        _ => panic!("expected SELECT"),
    }
}

#[test]
fn select_expression_column() {
    let stmt = parse("SELECT flux * 2.0 AS double_flux");
    match stmt {
        Statement::Select(s) => {
            assert_eq!(s.columns.len(), 1);
            assert!(matches!(
                &s.columns[0].expr,
                Expr::Binary {
                    op: BinaryOp::Mul,
                    ..
                }
            ));
            assert_eq!(s.columns[0].alias, Some("double_flux".to_string()));
        }
        _ => panic!("expected SELECT"),
    }
}

#[test]
fn operator_precedence_add_mul() {
    // 1 + 2 * 3 should parse as 1 + (2 * 3)
    let stmt = parse("SELECT 1 + 2 * 3 AS x");
    match stmt {
        Statement::Select(s) => match &s.columns[0].expr {
            Expr::Binary {
                op: BinaryOp::Add,
                right,
                ..
            } => {
                assert!(matches!(
                    **right,
                    Expr::Binary {
                        op: BinaryOp::Mul,
                        ..
                    }
                ));
            }
            other => panic!("expected Add, got {other:?}"),
        },
        _ => panic!("expected SELECT"),
    }
}

#[test]
fn operator_precedence_power_right_assoc() {
    // 2 ** 3 ** 4 should parse as 2 ** (3 ** 4)
    let stmt = parse("SELECT 2 ** 3 ** 4 AS x");
    match stmt {
        Statement::Select(s) => match &s.columns[0].expr {
            Expr::Binary {
                op: BinaryOp::Power,
                right,
                ..
            } => {
                assert!(matches!(
                    **right,
                    Expr::Binary {
                        op: BinaryOp::Power,
                        ..
                    }
                ));
            }
            other => panic!("expected Power, got {other:?}"),
        },
        _ => panic!("expected SELECT"),
    }
}

#[test]
fn select_not_in() {
    let stmt = parse("SELECT * WHERE id NOT IN (1, 2)");
    match stmt {
        Statement::Select(s) => {
            assert!(matches!(
                s.where_clause.as_ref().unwrap(),
                Expr::In { negated: true, .. }
            ));
        }
        _ => panic!("expected SELECT"),
    }
}

#[test]
fn select_function_call() {
    let stmt = parse("SELECT sqrt(flux) AS root_flux");
    match stmt {
        Statement::Select(s) => {
            assert!(
                matches!(&s.columns[0].expr, Expr::FunctionCall { name, args } if name == "sqrt" && args.len() == 1)
            );
        }
        _ => panic!("expected SELECT"),
    }
}

#[test]
fn select_aggregate() {
    let stmt = parse("SELECT COUNT(*), SUM(flux), AVG(flux)");
    match stmt {
        Statement::Select(s) => {
            assert_eq!(s.columns.len(), 3);
            assert!(matches!(
                &s.columns[0].expr,
                Expr::Aggregate {
                    func: AggregateFunc::Count,
                    ..
                }
            ));
        }
        _ => panic!("expected SELECT"),
    }
}

#[test]
fn select_group_by_having() {
    let stmt = parse("SELECT category, COUNT(*) GROUP BY category HAVING COUNT(*) > 1");
    match stmt {
        Statement::Select(s) => {
            assert_eq!(s.group_by.len(), 1);
            assert!(s.having.is_some());
        }
        _ => panic!("expected SELECT"),
    }
}

#[test]
fn select_rowid() {
    let stmt = parse("SELECT ROWID(), col1");
    match stmt {
        Statement::Select(s) => {
            assert_eq!(s.columns.len(), 2);
            assert!(matches!(&s.columns[0].expr, Expr::RowNumber));
        }
        _ => panic!("expected SELECT"),
    }
}

#[test]
fn select_qualified_column() {
    let stmt = parse("SELECT t.col1 FROM mytable AS t");
    match stmt {
        Statement::Select(s) => {
            assert!(matches!(
                &s.columns[0].expr,
                Expr::ColumnRef(ColumnRef { table: Some(t), column: c })
                if t == "t" && c == "col1"
            ));
        }
        _ => panic!("expected SELECT"),
    }
}

// ── UPDATE tests ──

#[test]
fn update_basic() {
    let stmt = parse("UPDATE SET flux = 1.0 WHERE id = 1");
    match stmt {
        Statement::Update(u) => {
            assert_eq!(u.assignments.len(), 1);
            assert_eq!(u.assignments[0].column, "flux");
            assert!(u.where_clause.is_some());
        }
        _ => panic!("expected UPDATE"),
    }
}

#[test]
fn update_expression_rhs() {
    let stmt = parse("UPDATE SET flux = flux * 2.0");
    match stmt {
        Statement::Update(u) => {
            assert!(matches!(
                &u.assignments[0].value,
                Expr::Binary {
                    op: BinaryOp::Mul,
                    ..
                }
            ));
        }
        _ => panic!("expected UPDATE"),
    }
}

// ── INSERT tests ──

#[test]
fn insert_basic() {
    let stmt = parse("INSERT INTO VALUES (1, 2.0, 'hello')");
    match stmt {
        Statement::Insert(i) => {
            assert_eq!(i.values.len(), 1);
            assert_eq!(i.values[0].len(), 3);
        }
        _ => panic!("expected INSERT"),
    }
}

#[test]
fn insert_with_columns() {
    let stmt = parse("INSERT INTO (id, flux) VALUES (1, 2.0)");
    match stmt {
        Statement::Insert(i) => {
            assert_eq!(i.columns, vec!["id", "flux"]);
            assert_eq!(i.values.len(), 1);
        }
        _ => panic!("expected INSERT"),
    }
}

#[test]
fn insert_multiple_rows() {
    let stmt = parse("INSERT INTO VALUES (1, 'a'), (2, 'b')");
    match stmt {
        Statement::Insert(i) => {
            assert_eq!(i.values.len(), 2);
        }
        _ => panic!("expected INSERT"),
    }
}

// ── DELETE tests ──

#[test]
fn delete_basic() {
    let stmt = parse("DELETE FROM WHERE id > 5");
    match stmt {
        Statement::Delete(d) => {
            assert!(d.where_clause.is_some());
        }
        _ => panic!("expected DELETE"),
    }
}

#[test]
fn delete_with_limit() {
    let stmt = parse("DELETE FROM WHERE id > 5 LIMIT 3");
    match stmt {
        Statement::Delete(d) => {
            assert!(d.limit.is_some());
        }
        _ => panic!("expected DELETE"),
    }
}

// ── JOIN tests ──

#[test]
fn select_inner_join() {
    let stmt = parse("SELECT * FROM t1 INNER JOIN t2 ON t1.id = t2.id");
    match stmt {
        Statement::Select(s) => {
            assert_eq!(s.joins.len(), 1);
            assert_eq!(s.joins[0].join_type, JoinType::Inner);
            assert!(s.joins[0].on.is_some());
        }
        _ => panic!("expected SELECT"),
    }
}

#[test]
fn select_cross_join() {
    let stmt = parse("SELECT * FROM t1 CROSS JOIN t2");
    match stmt {
        Statement::Select(s) => {
            assert_eq!(s.joins.len(), 1);
            assert_eq!(s.joins[0].join_type, JoinType::Cross);
            assert!(s.joins[0].on.is_none());
        }
        _ => panic!("expected SELECT"),
    }
}

// ── Round-trip tests ──

#[test]
fn roundtrip_select_star() {
    roundtrip("SELECT *");
}

#[test]
fn roundtrip_select_columns() {
    roundtrip("SELECT col1, col2");
}

#[test]
fn roundtrip_select_where() {
    roundtrip("SELECT * WHERE (flux > 1.0)");
}

#[test]
fn roundtrip_select_order_by() {
    roundtrip("SELECT * ORDER BY id ASC, flux DESC");
}

#[test]
fn roundtrip_update() {
    roundtrip("UPDATE SET flux = 1.0 WHERE (id = 1)");
}

#[test]
fn roundtrip_insert() {
    roundtrip("INSERT INTO VALUES (1, 2.0, 'hello')");
}

#[test]
fn roundtrip_delete() {
    roundtrip("DELETE FROM WHERE (id > 5)");
}

// ── Error tests ──

#[test]
fn error_empty() {
    let err = parse_err("");
    assert!(matches!(err, TaqlError::UnexpectedEnd { .. }));
}

#[test]
fn error_unexpected_token() {
    let err = parse_err("FROBNICATE");
    assert!(matches!(err, TaqlError::ParseError { .. }));
}

#[test]
fn error_trailing_token() {
    let err = parse_err("SELECT * WHERE flux > 1.0 EXTRA");
    assert!(matches!(err, TaqlError::ParseError { .. }));
}

// ── Comment test ──

#[test]
fn comment_skipped() {
    let stmt = parse("# this is a comment\nSELECT *");
    assert!(matches!(stmt, Statement::Select(_)));
}

#[test]
fn case_insensitive_keywords() {
    let stmt = parse("select * where flux > 1.0");
    assert!(matches!(stmt, Statement::Select(_)));
}

#[test]
fn unary_negate() {
    let stmt = parse("SELECT -1 AS neg");
    match stmt {
        Statement::Select(s) => {
            assert!(matches!(
                &s.columns[0].expr,
                Expr::Unary {
                    op: UnaryOp::Negate,
                    ..
                }
            ));
        }
        _ => panic!("expected SELECT"),
    }
}

#[test]
fn not_between() {
    let stmt = parse("SELECT * WHERE id NOT BETWEEN 1 AND 10");
    match stmt {
        Statement::Select(s) => {
            assert!(matches!(
                s.where_clause.as_ref().unwrap(),
                Expr::Between { negated: true, .. }
            ));
        }
        _ => panic!("expected SELECT"),
    }
}

#[test]
fn select_not_like() {
    let stmt = parse("SELECT * WHERE name NOT LIKE 'test%'");
    match stmt {
        Statement::Select(s) => {
            assert!(matches!(
                s.where_clause.as_ref().unwrap(),
                Expr::Like {
                    negated: true,
                    case_insensitive: false,
                    ..
                }
            ));
        }
        _ => panic!("expected SELECT"),
    }
}

// ── CALC statement tests ──

#[test]
fn calc_simple_expr() {
    let stmt = parse("CALC 1 + 2");
    match stmt {
        Statement::Calc(c) => {
            assert!(c.from.is_none());
            assert!(matches!(c.expr, Expr::Binary { .. }));
        }
        _ => panic!("expected Calc"),
    }
}

#[test]
fn calc_with_from() {
    let stmt = parse("CALC SUM(col) FROM my_table");
    match stmt {
        Statement::Calc(c) => {
            assert!(c.from.is_some());
            assert_eq!(c.from.unwrap().name, "my_table");
        }
        _ => panic!("expected Calc"),
    }
}

// ── ALTER TABLE tests ──

#[test]
fn alter_add_column() {
    let stmt = parse("ALTER TABLE ADD COLUMN new_col INT");
    match stmt {
        Statement::AlterTable(a) => {
            assert!(matches!(
                a.operation,
                AlterOperation::AddColumn { ref name, ref data_type }
                if name == "new_col" && data_type == "INT"
            ));
        }
        _ => panic!("expected AlterTable"),
    }
}

#[test]
fn alter_drop_column() {
    let stmt = parse("ALTER TABLE DROP COLUMN old_col");
    match stmt {
        Statement::AlterTable(a) => {
            assert!(matches!(
                a.operation,
                AlterOperation::DropColumn { ref name } if name == "old_col"
            ));
        }
        _ => panic!("expected AlterTable"),
    }
}

#[test]
fn alter_rename_column() {
    let stmt = parse("ALTER TABLE RENAME COLUMN a TO b");
    match stmt {
        Statement::AlterTable(a) => {
            assert!(matches!(
                a.operation,
                AlterOperation::RenameColumn { ref old_name, ref new_name }
                if old_name == "a" && new_name == "b"
            ));
        }
        _ => panic!("expected AlterTable"),
    }
}

#[test]
fn alter_add_row() {
    let stmt = parse("ALTER TABLE ADD ROW");
    match stmt {
        Statement::AlterTable(a) => {
            assert!(matches!(
                a.operation,
                AlterOperation::AddRow { count: None }
            ));
        }
        _ => panic!("expected AlterTable"),
    }
}

#[test]
fn alter_set_keyword() {
    let stmt = parse("ALTER TABLE SET KEYWORD my_key = 42");
    match stmt {
        Statement::AlterTable(a) => {
            assert!(matches!(
                a.operation,
                AlterOperation::SetKeyword { ref name, .. }
                if name == "my_key"
            ));
        }
        _ => panic!("expected AlterTable"),
    }
}

// ── g-prefix aggregate tests ──

#[test]
fn gmin_aggregate_in_select() {
    let stmt = parse("SELECT GMIN(col) FROM t GROUP BY grp");
    match stmt {
        Statement::Select(s) => {
            assert_eq!(s.columns.len(), 1);
            match &s.columns[0].expr {
                Expr::Aggregate { func, .. } => {
                    assert_eq!(*func, AggregateFunc::Min);
                }
                other => panic!("expected Aggregate, got {other:?}"),
            }
        }
        _ => panic!("expected SELECT"),
    }
}

// ── Wave 2: Bitwise operators ──

#[test]
fn bitwise_and() {
    let stmt = parse("SELECT x & 255 AS masked");
    match stmt {
        Statement::Select(s) => match &s.columns[0].expr {
            Expr::Binary {
                op: BinaryOp::BitAnd,
                ..
            } => {}
            other => panic!("expected BitAnd, got {other:?}"),
        },
        _ => panic!("expected SELECT"),
    }
}

#[test]
fn bitwise_or_xor() {
    let stmt = parse("SELECT x | y ^ z");
    match stmt {
        Statement::Select(s) => {
            // Should parse; exact shape depends on precedence
            assert_eq!(s.columns.len(), 1);
        }
        _ => panic!("expected SELECT"),
    }
}

// ── Wave 2: Regex literals and matching ──

#[test]
fn regex_literal() {
    let stmt = parse("SELECT * WHERE name =~ p/foo.*/i");
    match stmt {
        Statement::Select(s) => match s.where_clause.as_ref().unwrap() {
            Expr::RegexMatch {
                negated, pattern, ..
            } => {
                assert!(!negated);
                assert!(matches!(
                    pattern.as_ref(),
                    Expr::Literal(Literal::Regex {
                        pattern: p,
                        flags: f,
                    }) if p == "foo.*" && f == "i"
                ));
            }
            other => panic!("expected RegexMatch, got {other:?}"),
        },
        _ => panic!("expected SELECT"),
    }
}

#[test]
fn regex_not_match() {
    let stmt = parse("SELECT * WHERE name !~ p/bad/");
    match stmt {
        Statement::Select(s) => {
            assert!(matches!(
                s.where_clause.as_ref().unwrap(),
                Expr::RegexMatch { negated: true, .. }
            ));
        }
        _ => panic!("expected SELECT"),
    }
}

// ── Wave 2: IN with bracket ranges ──

#[test]
fn in_bracket_values() {
    let stmt = parse("SELECT * WHERE x IN [1, 2, 3]");
    match stmt {
        Statement::Select(s) => match s.where_clause.as_ref().unwrap() {
            Expr::InSet {
                elements, negated, ..
            } => {
                assert!(!negated);
                assert_eq!(elements.len(), 3);
                assert!(matches!(&elements[0], InSetElement::Value(_)));
            }
            other => panic!("expected InSet, got {other:?}"),
        },
        _ => panic!("expected SELECT"),
    }
}

#[test]
fn in_bracket_range() {
    let stmt = parse("SELECT * WHERE x IN [1:10]");
    match stmt {
        Statement::Select(s) => match s.where_clause.as_ref().unwrap() {
            Expr::InSet { elements, .. } => {
                assert_eq!(elements.len(), 1);
                assert!(matches!(
                    &elements[0],
                    InSetElement::Range {
                        start: Some(_),
                        end: Some(_),
                        step: None,
                    }
                ));
            }
            other => panic!("expected InSet, got {other:?}"),
        },
        _ => panic!("expected SELECT"),
    }
}

#[test]
fn in_bracket_range_with_step() {
    let stmt = parse("SELECT * WHERE x IN [0:10:2]");
    match stmt {
        Statement::Select(s) => match s.where_clause.as_ref().unwrap() {
            Expr::InSet { elements, .. } => {
                assert_eq!(elements.len(), 1);
                assert!(matches!(
                    &elements[0],
                    InSetElement::Range {
                        start: Some(_),
                        end: Some(_),
                        step: Some(_),
                    }
                ));
            }
            other => panic!("expected InSet, got {other:?}"),
        },
        _ => panic!("expected SELECT"),
    }
}

// ── Wave 2: Array indexing (basic parsing, full impl in Wave 3) ──

#[test]
fn array_index_single() {
    let stmt = parse("SELECT arr[0]");
    match stmt {
        Statement::Select(s) => {
            assert!(matches!(&s.columns[0].expr, Expr::ArrayIndex { .. }));
        }
        _ => panic!("expected SELECT"),
    }
}

#[test]
fn array_index_slice() {
    let stmt = parse("SELECT arr[1:3]");
    match stmt {
        Statement::Select(s) => match &s.columns[0].expr {
            Expr::ArrayIndex { indices, .. } => {
                assert_eq!(indices.len(), 1);
                assert!(matches!(&indices[0], IndexElement::Slice { .. }));
            }
            other => panic!("expected ArrayIndex, got {other:?}"),
        },
        _ => panic!("expected SELECT"),
    }
}

// ── Wave 3: USING STYLE and multi-dim indexing ──

#[test]
fn using_style_python() {
    let stmt = parse("USING STYLE PYTHON SELECT arr[0]");
    match stmt {
        Statement::Select(s) => {
            assert_eq!(s.style, IndexStyle::Python);
        }
        _ => panic!("expected SELECT"),
    }
}

#[test]
fn using_style_glish() {
    let stmt = parse("USING STYLE GLISH SELECT arr[1]");
    match stmt {
        Statement::Select(s) => {
            assert_eq!(s.style, IndexStyle::Glish);
        }
        _ => panic!("expected SELECT"),
    }
}

#[test]
fn using_style_case_insensitive() {
    let stmt = parse("using style python SELECT arr[0]");
    match stmt {
        Statement::Select(s) => {
            assert_eq!(s.style, IndexStyle::Python);
        }
        _ => panic!("expected SELECT"),
    }
}

#[test]
fn no_using_style_defaults_glish() {
    let stmt = parse("SELECT arr[1]");
    match stmt {
        Statement::Select(s) => {
            assert_eq!(s.style, IndexStyle::Glish);
        }
        _ => panic!("expected SELECT"),
    }
}

#[test]
fn multi_dim_index() {
    let stmt = parse("SELECT arr[1, 2]");
    match stmt {
        Statement::Select(s) => match &s.columns[0].expr {
            Expr::ArrayIndex { indices, .. } => {
                assert_eq!(indices.len(), 2);
                assert!(matches!(&indices[0], IndexElement::Single(_)));
                assert!(matches!(&indices[1], IndexElement::Single(_)));
            }
            other => panic!("expected ArrayIndex, got {other:?}"),
        },
        _ => panic!("expected SELECT"),
    }
}

#[test]
fn multi_dim_slice() {
    let stmt = parse("SELECT arr[1:3, 2:4]");
    match stmt {
        Statement::Select(s) => match &s.columns[0].expr {
            Expr::ArrayIndex { indices, .. } => {
                assert_eq!(indices.len(), 2);
                assert!(matches!(&indices[0], IndexElement::Slice { .. }));
                assert!(matches!(&indices[1], IndexElement::Slice { .. }));
            }
            other => panic!("expected ArrayIndex, got {other:?}"),
        },
        _ => panic!("expected SELECT"),
    }
}

#[test]
fn slice_with_step() {
    let stmt = parse("SELECT arr[1:10:2]");
    match stmt {
        Statement::Select(s) => match &s.columns[0].expr {
            Expr::ArrayIndex { indices, .. } => {
                assert_eq!(indices.len(), 1);
                match &indices[0] {
                    IndexElement::Slice { start, end, step } => {
                        assert!(start.is_some());
                        assert!(end.is_some());
                        assert!(step.is_some());
                    }
                    _ => panic!("expected Slice"),
                }
            }
            other => panic!("expected ArrayIndex, got {other:?}"),
        },
        _ => panic!("expected SELECT"),
    }
}

#[test]
fn using_style_python_roundtrip() {
    roundtrip("USING STYLE PYTHON SELECT arr[0]");
}

#[test]
fn gmean_aggregate_in_select() {
    let stmt = parse("SELECT GMEAN(col) FROM t GROUP BY grp");
    match stmt {
        Statement::Select(s) => match &s.columns[0].expr {
            Expr::Aggregate { func, .. } => {
                assert_eq!(*func, AggregateFunc::Avg);
            }
            other => panic!("expected Aggregate, got {other:?}"),
        },
        _ => panic!("expected SELECT"),
    }
}

// ── Wave 8: COUNT SELECT parsing ─────────────────────────────

#[test]
fn count_select_basic() {
    let stmt = parse("COUNT SELECT *");
    match stmt {
        Statement::CountSelect(s) => {
            assert!(s.columns.is_empty()); // * = empty
            assert!(s.where_clause.is_none());
        }
        _ => panic!("expected CountSelect"),
    }
}

#[test]
fn count_select_with_where() {
    let stmt = parse("COUNT SELECT * WHERE x > 5");
    match stmt {
        Statement::CountSelect(s) => {
            assert!(s.where_clause.is_some());
        }
        _ => panic!("expected CountSelect"),
    }
}

#[test]
fn count_select_roundtrip() {
    let stmt = parse("COUNT SELECT *");
    let displayed = stmt.to_string();
    assert!(displayed.starts_with("COUNT "));
    let reparsed = parse(&displayed);
    assert!(matches!(reparsed, Statement::CountSelect(_)));
}

#[test]
fn dotted_function_call() {
    let stmt = parse("SELECT meas.epoch('TAI', 51544.5)");
    match stmt {
        Statement::Select(s) => {
            assert!(
                matches!(&s.columns[0].expr, Expr::FunctionCall { name, args } if name == "meas.epoch" && args.len() == 2)
            );
        }
        _ => panic!("expected SELECT"),
    }
}

#[test]
fn dotted_function_vs_column_ref() {
    // Without parens, it should be a qualified column reference
    let stmt = parse("SELECT t1.col1");
    match stmt {
        Statement::Select(s) => {
            assert!(matches!(
                &s.columns[0].expr,
                Expr::ColumnRef(ColumnRef {
                    table: Some(t),
                    column: c,
                }) if t == "t1" && c == "col1"
            ));
        }
        _ => panic!("expected SELECT"),
    }
}

#[test]
fn quantity_suffix_literal_parses_in_meas_call() {
    let stmt = parse("CALC meas.igrf(200km, 'SUN', 51544.5, 'VLA')");
    match stmt {
        Statement::Calc(calc) => match calc.expr {
            Expr::FunctionCall { name, args } => {
                assert_eq!(name, "meas.igrf");
                assert!(matches!(
                    &args[0],
                    Expr::Literal(Literal::Quantity { value, unit })
                        if (*value - 200.0).abs() < 1e-12 && unit == "km"
                ));
            }
            other => panic!("expected function call, got {other:?}"),
        },
        other => panic!("expected CALC, got {other:?}"),
    }
}

#[test]
fn quantity_string_literal_parses_in_meas_call() {
    let stmt = parse("CALC meas.radvel('BARY', 1 'km/s')");
    match stmt {
        Statement::Calc(calc) => match calc.expr {
            Expr::FunctionCall { name, args } => {
                assert_eq!(name, "meas.radvel");
                assert!(matches!(
                    &args[1],
                    Expr::Literal(Literal::Quantity { value, unit })
                        if (*value - 1.0).abs() < 1e-12 && unit == "km/s"
                ));
            }
            other => panic!("expected function call, got {other:?}"),
        },
        other => panic!("expected CALC, got {other:?}"),
    }
}

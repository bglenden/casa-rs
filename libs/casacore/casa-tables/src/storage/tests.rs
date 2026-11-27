// SPDX-License-Identifier: LGPL-3.0-or-later

use super::TableInfo;

#[test]
fn table_info_parse_round_trip() {
    let info = TableInfo {
        table_type: "MeasurementSet".to_string(),
        sub_type: "UVFITS".to_string(),
        readme: Vec::new(),
    };
    let text = info.to_string();
    let parsed = TableInfo::parse(&text);
    assert_eq!(info, parsed);
}

#[test]
fn table_info_parse_empty() {
    let parsed = TableInfo::parse("");
    assert_eq!(parsed, TableInfo::default());
}

#[test]
fn table_info_parse_type_only() {
    let parsed = TableInfo::parse("Type = Catalog\n");
    assert_eq!(parsed.table_type, "Catalog");
    assert_eq!(parsed.sub_type, "");
    assert!(parsed.readme.is_empty());
}

#[test]
fn table_info_parse_readme() {
    let parsed = TableInfo::parse(
        "Type = LOG\nSubType = \n\nRepository for software-generated logging messages\n",
    );
    assert_eq!(parsed.table_type, "LOG");
    assert_eq!(parsed.sub_type, "");
    assert_eq!(
        parsed.readme,
        vec!["Repository for software-generated logging messages".to_string()]
    );

    let text = parsed.to_string();
    assert!(text.contains("\n\nRepository for software-generated logging messages\n"));
}

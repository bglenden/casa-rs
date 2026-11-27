// SPDX-License-Identifier: LGPL-3.0-or-later
mod common;
const FIXTURE_ROOT: &str = "tests/data";
#[path = "cases/casa_fixture.rs"]
mod casa_fixture;
#[path = "cases/read_errors.rs"]
mod read_errors;
#[path = "cases/records.rs"]
mod records;
#[path = "cases/rewrite.rs"]
mod rewrite;

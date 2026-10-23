// SPDX-License-Identifier: LGPL-3.0-or-later
//! `ComponentList::rewrite_table` replaces a list in place, as CASA's
//! `writeTable` rewrites a list the `cl` tool has open for update
//! (simulation Phase 2, #841): the table then holds the new list, and no
//! staging or replaced table is left beside it. The rewrite takes casacore's
//! write lock on the table, waiting while another process holds it, and
//! keeps the table's directory and lock file, so a process holding the table
//! open re-reads the new list (ADR-0008).

use casa_components::{ComponentList, ComponentShape, Flux, SkyComponent, SpectralModel};
use casa_types::measures::direction::{DirectionRef, MDirection};
use casa_types::measures::frequency::{FrequencyRef, MFrequency};

/// The environment variable naming the table [`hold_the_write_lock`] locks.
#[cfg(unix)]
const HELD_TABLE: &str = "CASA_COMPONENTS_TEST_HELD_TABLE";

fn list(fluxes: &[f64]) -> ComponentList {
    let mut list = ComponentList::new();
    for &flux in fluxes {
        list.push(SkyComponent::new(
            Flux::stokes_jy(flux, 0.0, 0.0, 0.0),
            ComponentShape::point(MDirection::from_angles(1.0, 0.5, DirectionRef::J2000)),
            SpectralModel::constant(MFrequency::new(1.4e9, FrequencyRef::LSRK)),
        ));
    }
    list
}

fn fluxes(list: &ComponentList) -> Vec<f64> {
    list.iter()
        .map(|component| component.flux().values()[0].re)
        .collect()
}

#[test]
fn a_rewrite_replaces_the_list_and_leaves_nothing_beside_it() {
    let scratch = tempfile::tempdir().expect("scratch");
    let path = scratch.path().join("list.cl");
    list(&[1.0]).write_table(&path).expect("write");
    list(&[2.0, 3.0]).rewrite_table(&path).expect("rewrite");
    assert_eq!(
        fluxes(&ComponentList::read_table(&path).expect("read")),
        [2.0, 3.0]
    );
    let entries: Vec<_> = std::fs::read_dir(scratch.path())
        .expect("scratch entries")
        .map(|entry| entry.expect("entry").file_name())
        .collect();
    assert_eq!(entries, ["list.cl"]);
    // A rewrite of a table not yet written creates it.
    let fresh = scratch.path().join("fresh.cl");
    list(&[4.0])
        .rewrite_table(&fresh)
        .expect("rewrite a new table");
    assert_eq!(
        fluxes(&ComponentList::read_table(&fresh).expect("read")),
        [4.0]
    );
}

/// Not a law: the other process of
/// [`a_rewrite_waits_for_a_write_lock_another_process_holds`]. Run with
/// [`HELD_TABLE`] set, it takes casacore's write lock on that table through
/// casa-tables' lock API, as a CASA session's `tb.lock()` does, and holds it
/// until a line arrives on its standard input. At the next line it takes a
/// read lock, which re-reads a table another process wrote meanwhile, and
/// reports the rows it sees.
#[cfg(unix)]
#[test]
#[ignore = "run by another test as its lock-holding process"]
fn hold_the_write_lock() {
    use casa_tables::{LockMode, LockOptions, LockType, Table, TableOptions};

    let Some(path) = std::env::var_os(HELD_TABLE) else {
        return;
    };
    let mut table = Table::open_with_lock(
        TableOptions::new(&path),
        LockOptions::new(LockMode::UserLocking),
    )
    .expect("open the held table");
    assert!(table.lock(LockType::Write, 1).expect("take the write lock"));
    println!("held");
    let mut line = String::new();
    std::io::stdin()
        .read_line(&mut line)
        .expect("wait to release");
    table.unlock().expect("release the write lock");
    line.clear();
    std::io::stdin()
        .read_line(&mut line)
        .expect("wait to relock");
    assert!(table.lock(LockType::Read, 1).expect("take a read lock"));
    println!("rows {}", table.row_count());
    table.unlock().expect("release the read lock");
}

/// The review of #873 held a CASA write lock while a list was rewritten: the
/// rewrite swapped the directory under the holder. A rewrite now waits for
/// the holder's write lock, as casacore's `writeTable` does, then writes the
/// same table (its directory and `table.lock` are the ones the holder has
/// open), and announces the write, so the holder re-reads the new rows.
#[cfg(unix)]
#[test]
fn a_rewrite_waits_for_a_write_lock_another_process_holds() {
    use std::io::{BufRead, BufReader, Write};
    use std::os::unix::fs::MetadataExt;
    use std::path::Path;
    use std::process::{Command, Stdio};
    use std::time::Duration;

    let scratch = tempfile::tempdir().expect("scratch");
    let path = scratch.path().join("list.cl");
    list(&[1.0]).write_table(&path).expect("write");
    // Reading opens the table with locking, which makes its `table.lock`.
    assert_eq!(
        fluxes(&ComponentList::read_table(&path).expect("read")),
        [1.0]
    );
    // Only `stat`, never open: closing a descriptor of `table.lock` drops
    // every `fcntl` lock this process holds on it.
    let identity = |path: &Path| {
        let inode = |path: &Path| std::fs::metadata(path).expect("stat").ino();
        (inode(path), inode(&path.join("table.lock")))
    };
    let before = identity(&path);

    let mut holder = Command::new(std::env::current_exe().expect("test binary"))
        .args(["hold_the_write_lock", "--exact", "--ignored", "--nocapture"])
        .env(HELD_TABLE, &path)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .spawn()
        .expect("start the holding process");
    let mut stdout = BufReader::new(holder.stdout.take().expect("holder output"));
    // What the holder says from `what` on: libtest may print the test's name
    // on the same line first.
    let mut said = |what: &str| {
        let mut line = String::new();
        while !line.contains(what) {
            line.clear();
            assert!(
                stdout.read_line(&mut line).expect("read the holder") > 0,
                "the holder exited before saying {what:?}"
            );
        }
        let start = line.find(what).expect("the line says it");
        line[start..].trim_end().to_string()
    };
    said("held");

    let rewriting = {
        let path = path.clone();
        std::thread::spawn(move || list(&[2.0, 3.0]).rewrite_table(&path))
    };
    std::thread::sleep(Duration::from_millis(500));
    assert!(
        !rewriting.is_finished(),
        "the rewrite went ahead while another process held the write lock"
    );
    assert_eq!(identity(&path), before);

    let mut stdin = holder.stdin.take().expect("holder input");
    writeln!(stdin, "release").expect("tell the holder to release");
    rewriting
        .join()
        .expect("the rewrite thread")
        .expect("rewrite once the lock is free");
    assert_eq!(
        identity(&path),
        before,
        "the rewrite replaced the table instead of writing it"
    );
    writeln!(stdin, "relock").expect("tell the holder to relock");
    assert_eq!(said("rows"), "rows 2", "the holder kept the old table");
    drop(stdin);
    assert!(holder.wait().expect("the holder ends").success());
    assert_eq!(
        fluxes(&ComponentList::read_table(&path).expect("read")),
        [2.0, 3.0]
    );
}

// SPDX-License-Identifier: LGPL-3.0-or-later

use std::fs;
use std::os::unix::fs::{MetadataExt, symlink};
use std::path::Path;

use casa_core::{PrimitiveType, RecordField, RecordValue, ScalarValue, Value};
use casa_tables::{ColumnSchema, Table, TableError, TableOptions, TableSchema};

fn value(id: i32) -> ScalarValue {
    ScalarValue::Int32(id)
}

fn saved_table(path: &Path) -> Table {
    let schema = TableSchema::new(vec![ColumnSchema::scalar("id", PrimitiveType::Int32)]).unwrap();
    let mut table = Table::with_schema(schema);
    table
        .add_row(RecordValue::new(vec![RecordField::new(
            "id",
            Value::Scalar(value(1)),
        )]))
        .unwrap();
    table.save(TableOptions::new(path)).unwrap();
    Table::open(TableOptions::new(path)).unwrap()
}

fn set_id(table: &mut Table, id: i32) {
    table
        .row_accessor_mut()
        .set_cell(0, "id", Value::Scalar(value(id)))
        .unwrap();
}

fn read_id(path: &Path) -> ScalarValue {
    let table = Table::open(TableOptions::new(path)).expect("reopen the original table");
    table
        .cell_accessor(0, "id")
        .unwrap()
        .scalar()
        .unwrap()
        .clone()
}

fn linked_descriptor_is_refused_before_row_writes(
    save: impl Fn(&mut Table, &Path) -> Result<(), TableError>,
) {
    for hard_link in [false, true] {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("table");
        let mut table = saved_table(&path);
        let descriptor = path.join("table.dat");
        let payload = path.join("table.f0");
        let referent = dir.path().join("original.dat");
        if hard_link {
            fs::hard_link(&descriptor, &referent).unwrap();
        } else {
            fs::rename(&descriptor, &referent).unwrap();
            symlink(&referent, &descriptor).unwrap();
        }
        let descriptor_bytes = fs::read(&descriptor).unwrap();
        let payload_bytes = fs::read(&payload).unwrap();
        let original_inode = descriptor.symlink_metadata().unwrap().ino();

        set_id(&mut table, 2);
        let error = save(&mut table, &path).expect_err("linked descriptor must be refused");
        assert!(
            error.to_string().contains(if hard_link {
                "hard-linked table.dat"
            } else {
                "regular table.dat"
            }),
            "{error}"
        );
        assert_eq!(read_id(&path), value(1), "hard link: {hard_link}");
        assert_eq!(fs::read(payload).unwrap(), payload_bytes);
        assert_eq!(fs::read(&descriptor).unwrap(), descriptor_bytes);
        assert_eq!(fs::read(referent).unwrap(), descriptor_bytes);
        assert_eq!(descriptor.symlink_metadata().unwrap().ino(), original_inode);
    }
}

#[test]
fn full_save_refuses_a_linked_descriptor_before_replacing_rows() {
    linked_descriptor_is_refused_before_row_writes(|table, path| {
        table.save(TableOptions::new(path))
    });
}

#[test]
fn bound_save_refuses_a_linked_descriptor_before_replacing_rows() {
    linked_descriptor_is_refused_before_row_writes(|table, path| {
        table.save_with_bindings(TableOptions::new(path), &Default::default())
    });
}

#[test]
fn selected_row_save_refuses_a_linked_descriptor_before_replacing_rows() {
    linked_descriptor_is_refused_before_row_writes(|table, _| {
        table.prepare_write().save_selected_rows(&["id"], &[0])
    });
}

#[cfg(target_os = "linux")]
#[test]
fn full_save_preserves_descriptor_group_in_a_setgid_directory() {
    use std::os::unix::fs::{PermissionsExt, chown};

    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("table");
    let mut table = saved_table(&path);
    let descriptor = path.join("table.dat");
    let before = fs::metadata(&descriptor).unwrap();
    // SAFETY: getgroups with size zero queries the number of groups.
    let count = unsafe { libc::getgroups(0, std::ptr::null_mut()) };
    assert!(count >= 0);
    let mut groups = vec![0; count as usize];
    // SAFETY: groups holds count writable gid_t elements.
    let count = unsafe { libc::getgroups(count, groups.as_mut_ptr()) };
    assert!(count >= 0);
    groups.truncate(count as usize);
    let Some(group) = groups.into_iter().find(|&gid| gid != before.gid()) else {
        eprintln!("skipping setgid preservation law: no different supplementary group");
        return;
    };
    chown(&path, None, Some(group)).unwrap();
    fs::set_permissions(&path, fs::Permissions::from_mode(0o2700)).unwrap();
    let staged = tempfile::NamedTempFile::new_in(&path).unwrap();
    assert_eq!(staged.as_file().metadata().unwrap().gid(), group);
    set_id(&mut table, 2);
    table.save(TableOptions::new(&path)).unwrap();
    assert_eq!(read_id(&path), value(2));
    let after = fs::metadata(&descriptor).unwrap();
    assert_eq!(
        (after.uid(), after.gid(), after.mode()),
        (before.uid(), before.gid(), before.mode())
    );
}

#[cfg(target_os = "macos")]
#[test]
fn full_save_refuses_a_macos_extended_acl_before_replacing_rows() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("table");
    let mut table = saved_table(&path);
    let descriptor = path.join("table.dat");
    assert!(
        std::process::Command::new("/bin/chmod")
            .args(["+a", "everyone allow read"])
            .arg(&descriptor)
            .status()
            .unwrap()
            .success()
    );
    let before = fs::read(&descriptor).unwrap();
    let inode = fs::metadata(&descriptor).unwrap().ino();
    set_id(&mut table, 2);
    let error = table.save(TableOptions::new(&path)).unwrap_err();
    assert!(
        error.to_string().contains("macOS extended table.dat ACL"),
        "{error}"
    );
    assert_eq!(read_id(&path), value(1));
    assert_eq!(fs::read(&descriptor).unwrap(), before);
    assert_eq!(fs::metadata(&descriptor).unwrap().ino(), inode);
}

#[cfg(target_os = "linux")]
mod linux {
    use super::*;
    use std::ffi::{CStr, CString};
    use std::os::unix::ffi::OsStrExt;

    const ACCESS: &CStr = c"system.posix_acl_access";
    const DEFAULT: &CStr = c"system.posix_acl_default";

    fn set_named_acl(path: &Path, name: &CStr) -> bool {
        let path = CString::new(path.as_os_str().as_bytes()).unwrap();
        let mut acl = 2u32.to_le_bytes().to_vec(); // Linux POSIX ACL xattr version.
        // The effective UID is mapped even inside a user namespace. A named
        // entry for it is distinct from the ACL's owning-user entry.
        // SAFETY: geteuid takes no arguments and has no failure result.
        let named_user = unsafe { libc::geteuid() };
        for (tag, permissions, id) in [
            (1u16, if name == DEFAULT { 7u16 } else { 6u16 }, u32::MAX),
            (2, 4, named_user),
            (4, 4, u32::MAX),
            (16, 4, u32::MAX),
            (32, 0, u32::MAX),
        ] {
            acl.extend(tag.to_le_bytes());
            acl.extend(permissions.to_le_bytes());
            acl.extend(id.to_le_bytes());
        }
        // SAFETY: the names and ACL bytes remain live for the call.
        let status = unsafe {
            libc::setxattr(
                path.as_ptr(),
                name.as_ptr(),
                acl.as_ptr().cast(),
                acl.len(),
                0,
            )
        };
        if status == 0 {
            return true;
        }
        let error = std::io::Error::last_os_error();
        if error.raw_os_error() == Some(libc::EOPNOTSUPP) {
            eprintln!("skipping POSIX ACL law: filesystem does not support ACLs");
            return false;
        }
        panic!("could not set POSIX ACL: {error}");
    }

    fn access_acl(path: &Path) -> Option<Vec<u8>> {
        let path = CString::new(path.as_os_str().as_bytes()).unwrap();
        let mut acl = vec![0u8; 128];
        // SAFETY: the names and destination of the specified size remain live.
        let size = unsafe {
            libc::getxattr(
                path.as_ptr(),
                ACCESS.as_ptr(),
                acl.as_mut_ptr().cast(),
                acl.len(),
            )
        };
        if size < 0 {
            assert_eq!(
                std::io::Error::last_os_error().raw_os_error(),
                Some(libc::ENODATA)
            );
            return None;
        }
        acl.truncate(size as usize);
        Some(acl)
    }

    #[test]
    fn full_save_preserves_a_named_descriptor_access_acl() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("table");
        let mut table = saved_table(&path);
        let descriptor = path.join("table.dat");
        if !set_named_acl(&descriptor, ACCESS) {
            return;
        }
        let expected = access_acl(&descriptor).expect("named ACL");
        let before = fs::metadata(&descriptor).unwrap();
        set_id(&mut table, 2);
        table.save(TableOptions::new(&path)).unwrap();
        assert_eq!(read_id(&path), value(2));
        assert_eq!(access_acl(&descriptor), Some(expected));
        let after = fs::metadata(&descriptor).unwrap();
        assert_eq!(
            (after.uid(), after.gid(), after.mode()),
            (before.uid(), before.gid(), before.mode())
        );
    }

    #[test]
    fn selected_row_save_does_not_inherit_a_new_access_acl() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("table");
        let mut table = saved_table(&path);
        let descriptor = path.join("table.dat");
        assert!(access_acl(&descriptor).is_none());
        if !set_named_acl(&path, DEFAULT) {
            return;
        }
        let probe = tempfile::NamedTempFile::new_in(&path).unwrap();
        assert!(
            access_acl(probe.path()).is_some(),
            "staging must inherit the default ACL"
        );
        set_id(&mut table, 2);
        table
            .prepare_write()
            .save_selected_rows(&["id"], &[0])
            .unwrap();
        assert_eq!(read_id(&path), value(2));
        assert!(access_acl(&descriptor).is_none());
    }
}

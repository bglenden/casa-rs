// SPDX-License-Identifier: LGPL-3.0-or-later

//! CASA single-namespace restart=False filename selection (#944).

use crate::PrepareError;
use std::{
    fs, io,
    path::{Path, PathBuf},
};

pub(crate) fn next_prefix(prefix: &Path) -> Result<PathBuf, PrepareError> {
    // Match the actual publication directory, including a trailing slash.
    let mut probe = prefix.as_os_str().to_os_string();
    probe.push(".model");
    let probe = PathBuf::from(probe);
    let directory = probe
        .parent()
        .filter(|p| !p.as_os_str().is_empty())
        .unwrap_or(Path::new("."));
    let stem = probe
        .file_name()
        .map(|s| s.as_encoded_bytes())
        .and_then(|s| s.strip_suffix(b".model"))
        .filter(|s| !s.is_empty())
        .ok_or_else(|| PrepareError::Restart {
            path: prefix.into(),
            reason: "restart=False requires a nonempty image basename (#944)".into(),
        })?;
    let entries = match fs::read_dir(directory) {
        Ok(entries) => entries,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(prefix.into()),
        Err(source) => {
            return Err(PrepareError::InspectOutputDirectory {
                path: directory.into(),
                source,
            });
        }
    };
    let mut maximum: Option<String> = None;
    for entry in entries {
        let entry = entry.map_err(|source| PrepareError::InspectOutputDirectory {
            path: directory.into(),
            source,
        })?;
        let name = entry.file_name();
        let name = name.as_encoded_bytes();
        let Some(id) = product_id(stem, name, &entry.path())? else {
            continue;
        };
        let current = maximum.get_or_insert_with(|| "1".into());
        if (id.len(), id.as_str()) > (current.len(), current.as_str()) {
            *current = id;
        }
    }
    let Some(maximum) = maximum else {
        return Ok(prefix.into());
    };
    // Append to the original bytes, preserving relative and absolute directories.
    let mut selected = prefix.as_os_str().to_os_string();
    selected.push(format!("_{}", increment(&maximum)));
    Ok(selected.into())
}

fn product_id(stem: &[u8], name: &[u8], path: &Path) -> Result<Option<String>, PrepareError> {
    let Some((root, extension)) = split_extension(name) else {
        return Ok(None);
    };
    if !matches!(
        extension,
        b"image" | b"residual" | b"model" | b"psf" | b"sumwt" | b"tt0"
    ) {
        return Ok(None);
    }
    if let Some(tail) = name.strip_prefix(stem).and_then(|s| s.strip_prefix(b"_")) {
        let id = tail
            .split(|&byte| byte == b'.')
            .next()
            .expect("split has first element");
        // POSIX surrogateescaped bytes are not digits in CASA. Invalid bytes
        // elsewhere in the filename must not suppress recognized products.
        let Ok(id) = std::str::from_utf8(id) else {
            return Ok(None);
        };
        if id.is_empty() || !id.chars().all(casa_digit) {
            return Ok(None);
        }
        if !id.is_ascii() {
            return Err(PrepareError::Restart {
                path: path.into(),
                reason: "CASA rejects Unicode digit image suffixes when evaluating their integer ID (#944)".into(),
            });
        }
        // Python eval permits all-zero integer literals but rejects 02, 001, etc.
        if id.len() > 1 && id.starts_with('0') && id.bytes().any(|b| b != b'0') {
            return Err(PrepareError::Restart { path: path.into(), reason: "CASA rejects numbered image names with a nonzero leading-zero integer suffix (#944)".into() });
        }
        return Ok(Some(if id.bytes().all(|b| b == b'0') {
            "0".into()
        } else {
            id.into()
        }));
    }
    let root = if extension == b"tt0" {
        split_extension(root).map_or(root, |(root, _)| root)
    } else {
        root
    };
    Ok((root == stem).then(|| "1".into()))
}

fn split_extension(name: &[u8]) -> Option<(&[u8], &[u8])> {
    let index = name.iter().rposition(|&byte| byte == b'.')?;
    // Python splitext treats leading dots as part of the basename.
    if name[..index].iter().all(|&byte| byte == b'.') {
        return None;
    }
    Some((&name[..index], &name[index + 1..]))
}

// Exact non-ASCII str.isdigit ranges from CASA 6.7.6.14's Python Unicode 15.0.0.
// Numeric-but-not-digit text (e.g. 1/2 symbol) must remain ignored, not refused.
fn casa_digit(character: char) -> bool {
    character.is_ascii_digit()
        || matches!(character as u32, 0xB2..=0xB3 | 0xB9 | 0x660..=0x669 | 0x6F0..=0x6F9 | 0x7C0..=0x7C9 | 0x966..=0x96F | 0x9E6..=0x9EF | 0xA66..=0xA6F | 0xAE6..=0xAEF | 0xB66..=0xB6F | 0xBE6..=0xBEF | 0xC66..=0xC6F | 0xCE6..=0xCEF | 0xD66..=0xD6F | 0xDE6..=0xDEF | 0xE50..=0xE59 | 0xED0..=0xED9 | 0xF20..=0xF29 | 0x1040..=0x1049 | 0x1090..=0x1099 | 0x1369..=0x1371 | 0x17E0..=0x17E9 | 0x1810..=0x1819 | 0x1946..=0x194F | 0x19D0..=0x19DA | 0x1A80..=0x1A89 | 0x1A90..=0x1A99 | 0x1B50..=0x1B59 | 0x1BB0..=0x1BB9 | 0x1C40..=0x1C49 | 0x1C50..=0x1C59 | 0x2070 | 0x2074..=0x2079 | 0x2080..=0x2089 | 0x2460..=0x2468 | 0x2474..=0x247C | 0x2488..=0x2490 | 0x24EA | 0x24F5..=0x24FD | 0x24FF | 0x2776..=0x277E | 0x2780..=0x2788 | 0x278A..=0x2792 | 0xA620..=0xA629 | 0xA8D0..=0xA8D9 | 0xA900..=0xA909 | 0xA9D0..=0xA9D9 | 0xA9F0..=0xA9F9 | 0xAA50..=0xAA59 | 0xABF0..=0xABF9 | 0xFF10..=0xFF19 | 0x104A0..=0x104A9 | 0x10A40..=0x10A43 | 0x10D30..=0x10D39 | 0x10E60..=0x10E68 | 0x11052..=0x1105A | 0x11066..=0x1106F | 0x110F0..=0x110F9 | 0x11136..=0x1113F | 0x111D0..=0x111D9 | 0x112F0..=0x112F9 | 0x11450..=0x11459 | 0x114D0..=0x114D9 | 0x11650..=0x11659 | 0x116C0..=0x116C9 | 0x11730..=0x11739 | 0x118E0..=0x118E9 | 0x11950..=0x11959 | 0x11C50..=0x11C59 | 0x11D50..=0x11D59 | 0x11DA0..=0x11DA9 | 0x11F50..=0x11F59 | 0x16A60..=0x16A69 | 0x16AC0..=0x16AC9 | 0x16B50..=0x16B59 | 0x1D7CE..=0x1D7FF | 0x1E140..=0x1E149 | 0x1E2F0..=0x1E2F9 | 0x1E4F0..=0x1E4F9 | 0x1E950..=0x1E959 | 0x1F100..=0x1F10A | 0x1FBF0..=0x1FBF9)
}

fn increment(number: &str) -> String {
    let mut digits = number.as_bytes().to_vec();
    for digit in digits.iter_mut().rev() {
        if *digit < b'9' {
            *digit += 1;
            return String::from_utf8(digits).expect("ASCII decimal");
        }
        *digit = b'0';
    }
    digits.insert(0, b'1');
    String::from_utf8(digits).expect("ASCII decimal")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn matches_pinned_casa_numbering_cases() {
        for (names, expected) in [
            (vec![], "image"),
            (vec!["image.image"], "image_2"),
            (
                vec!["image.image", "image_2.psf", "image_7.residual"],
                "image_8",
            ),
            (vec!["image.psf.tt0", "image_3.image.tt0"], "image_4"),
            (vec!["image.psf.tt1", "image.pb", "image.mask"], "image"),
            (vec!["image_8.sumwt"], "image_9"),
            (
                vec!["imageElse.image", "image_4x.model", "image.summary.json"],
                "image",
            ),
            (vec!["image.image", "image_99.pb"], "image_2"),
            (vec!["image_6.extra.image"], "image_7"),
            (
                vec!["image_99999999999999999999999999999999999999999.model"],
                "image_100000000000000000000000000000000000000000",
            ),
        ] {
            let directory = tempfile::tempdir().unwrap();
            for name in &names {
                fs::write(directory.path().join(name), "preserve").unwrap();
            }
            assert_eq!(
                next_prefix(&directory.path().join("image")).unwrap(),
                directory.path().join(expected)
            );
            for name in names {
                assert_eq!(
                    fs::read_to_string(directory.path().join(name)).unwrap(),
                    "preserve"
                );
            }
        }
    }

    #[test]
    fn leading_zero_nonzero_id_is_refused() {
        let directory = tempfile::tempdir().unwrap();
        fs::write(directory.path().join("image_02.image"), "preserve").unwrap();
        assert!(matches!(
            next_prefix(&directory.path().join("image")),
            Err(PrepareError::Restart { .. })
        ));
    }

    #[test]
    fn selected_orphan_namespace_remains_protected() {
        let directory = tempfile::tempdir().unwrap();
        for name in ["image.image", "image_2.pb"] {
            fs::write(directory.path().join(name), "preserve").unwrap();
        }
        let selected = next_prefix(&directory.path().join("image")).unwrap();
        assert_eq!(selected, directory.path().join("image_2"));
        assert!(super::super::check_prefix(&selected).is_err());
    }
    #[test]
    fn relative_directory_and_fresh_request_policy() {
        let directory = tempfile::tempdir_in(".").unwrap();
        let prefix = directory.path().join("image");
        fs::write(directory.path().join("image.image"), "preserve").unwrap();
        let request = crate::request::tests::request(serde_json::json!({"imagename": prefix}));
        let selected = request.with_tclean_output_policy(false).unwrap();
        assert_eq!(selected.imagename, directory.path().join("image_2"));
        assert!(!selected.tclean_restart);
        assert_eq!(request.imagename, prefix);
        let continued = request.with_tclean_output_policy(true).unwrap();
        assert_eq!(continued.imagename, prefix);
        assert!(continued.tclean_restart);
        let mut outlier = request;
        outlier.outlierfile = Some(directory.path().join("outliers.txt"));
        assert!(outlier.with_tclean_output_policy(false).is_err());
    }
    #[test]
    fn unicode_digits_match_casa_eval_refusal_but_other_numeric_text_is_ignored() {
        for id in ["٢", "²", "２", "1٢"] {
            let name = format!("image_{id}.image");
            assert!(matches!(
                product_id(b"image", name.as_bytes(), Path::new(&name)),
                Err(PrepareError::Restart { .. })
            ));
        }
        assert_eq!(
            product_id(
                b"image",
                "image_½.image".as_bytes(),
                Path::new("image_½.image")
            )
            .unwrap(),
            None
        );
    }
    #[cfg(unix)]
    #[test]
    fn non_utf8_product_bytes_match_casa_surrogateescape_numbering() {
        use std::os::unix::ffi::OsStrExt;
        for (name, id, expected) in [
            (b"image.\xff.tt0".as_slice(), Some("1"), "image_2"),
            (b"image_6.\xff.image".as_slice(), Some("6"), "image_7"),
            (b"image_\xff.image".as_slice(), None, "image"),
        ] {
            let directory = tempfile::tempdir().unwrap();
            let product = directory.path().join(std::ffi::OsStr::from_bytes(name));
            // Always test parsing, even on APFS which cannot store such names.
            assert_eq!(product_id(b"image", name, &product).unwrap().as_deref(), id);
            match fs::write(&product, "preserve") {
                Ok(()) => {}
                // macOS APFS rejects invalid filename bytes with EILSEQ (#944).
                Err(error) if cfg!(target_os = "macos") && error.raw_os_error() == Some(92) => {
                    continue;
                }
                Err(error) => panic!("creating byte-name fixture: {error}"),
            }
            assert_eq!(
                next_prefix(&directory.path().join("image")).unwrap(),
                directory.path().join(expected)
            );
            assert_eq!(fs::read_to_string(product).unwrap(), "preserve");
        }
    }
}

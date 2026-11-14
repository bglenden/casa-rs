// SPDX-License-Identifier: LGPL-3.0-or-later
//! Every mode the simulator refuses as not yet implemented names the issue
//! that records it (#1053): a user who meets the refusal can find where the
//! work stands. A programme issue alone, or a phase name, does not count.

use std::fs;
use std::path::{Path, PathBuf};

/// The constructor of a refusal for something CASA does and casa-rs does
/// not yet.
const REFUSAL: &str = "SimulatorError::not_yet_implemented(";

/// Programme issues, which say what a mode belongs to but not where its
/// work stands.
const PROGRAMMES: [&str; 1] = ["#841"];

fn rust_files(directory: &Path, found: &mut Vec<PathBuf>) {
    for entry in fs::read_dir(directory).expect("read source directory") {
        let path = entry.expect("directory entry").path();
        if path.is_dir() {
            rust_files(&path, found);
        } else if path.extension().is_some_and(|extension| extension == "rs") {
            found.push(path);
        }
    }
}

/// The text of the string literals in the call that starts at `source[0]`
/// (just after its opening parenthesis), up to its closing parenthesis.
fn call_literals(source: &str) -> String {
    let mut literals = String::new();
    let mut depth = 1_usize;
    let mut characters = source.chars().peekable();
    while let Some(character) = characters.next() {
        match character {
            '"' => {
                while let Some(inner) = characters.next() {
                    match inner {
                        '\\' => match characters.next() {
                            // A continued line: Rust drops the newline and
                            // the next line's leading whitespace.
                            Some('\n') => {
                                while characters.next_if(|next| next.is_whitespace()).is_some() {}
                            }
                            Some(escaped) => literals.push(escaped),
                            None => {}
                        },
                        '"' => break,
                        other => literals.push(other),
                    }
                }
                literals.push(' ');
            }
            '(' | '[' | '{' => depth += 1,
            ')' | ']' | '}' => {
                depth -= 1;
                if depth == 0 {
                    break;
                }
            }
            _ => {}
        }
    }
    literals
}

/// Whether `text` holds an issue reference `(#N)`, or `(#N):` and `(#N);`
/// as a message continues.
fn names_an_issue(text: &str) -> bool {
    text.match_indices("(#").any(|(at, _)| {
        let digits: String = text[at + 2..]
            .chars()
            .take_while(char::is_ascii_digit)
            .collect();
        !digits.is_empty()
            && text[at + 2 + digits.len()..].starts_with(')')
            && !PROGRAMMES.contains(&format!("#{digits}").as_str())
    })
}

#[test]
fn every_not_yet_implemented_refusal_names_an_issue() {
    let source = Path::new(env!("CARGO_MANIFEST_DIR")).join("src");
    let mut files = Vec::new();
    rust_files(&source, &mut files);
    // Check that the scan reaches the simulator, independently of how many
    // capabilities remain unimplemented (eventually there should be none).
    assert!(
        files.contains(&source.join("session/mod.rs")),
        "source scan did not find the simulator session"
    );
    let mut unnamed = Vec::new();
    for file in files {
        let text = fs::read_to_string(&file).expect("read source file");
        for (at, _) in text.match_indices(REFUSAL) {
            let literals = call_literals(&text[at + REFUSAL.len()..]);
            if !names_an_issue(&literals) || literals.contains("Phase") {
                let line = text[..at].lines().count();
                unnamed.push(format!("{}:{line}: {literals}", file.display()));
            }
        }
    }
    assert!(
        unnamed.is_empty(),
        "refusals that name no issue:\n{}",
        unnamed.join("\n")
    );
}

#[test]
fn the_scan_tells_a_named_refusal_from_an_unnamed_one() {
    let call = r#"format!(
                "setoptions(ftmachine='{ftmachine}') is not yet implemented in casa-rs (#1053); \
                 'SF' is",
                options.grid_function
            )));"#;
    let literals = call_literals(call);
    assert!(literals.contains("casa-rs (#1053); 'SF' is"), "{literals}");
    assert!(names_an_issue(&literals));
    assert!(!names_an_issue("is not yet implemented in casa-rs"));
    assert!(!names_an_issue("is not yet implemented in casa-rs (#841)"));
    assert!(!names_an_issue("see #1053"));
    assert!(names_an_issue(
        "not yet implemented (#1053): CASA leaves it"
    ));
}

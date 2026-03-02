use std::path::{Path, PathBuf};
use std::process::Command;

use casacore_aipsio::demo::run_taipsio_like_demo;
use casacore_test_support::cpp_backend_available;

#[test]
fn taipsio_demo_output_matches_cpp_skip_mode() {
    if !cpp_backend_available() {
        eprintln!("skipping demo parity test: C++ casacore backend unavailable");
        return;
    }

    let source = casacore_taipsio_source_path();
    if !source.exists() {
        eprintln!(
            "skipping demo parity test: C++ source not found at {}",
            source.display()
        );
        return;
    }

    let temp_dir = std::env::temp_dir().join(format!(
        "casa-rs-taipsio-parity-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_nanos())
            .unwrap_or(0)
    ));
    std::fs::create_dir_all(&temp_dir).expect("create temp dir");

    let cxx = std::env::var("CXX").unwrap_or_else(|_| "c++".to_string());
    let binary = temp_dir.join("tAipsIO");
    compile_cpp_demo(&cxx, &source, &binary);

    let cpp_output = Command::new(&binary)
        .arg("skip_exceptions")
        .output()
        .expect("run C++ tAipsIO demo");
    assert!(
        cpp_output.status.success(),
        "C++ tAipsIO failed: {}",
        String::from_utf8_lossy(&cpp_output.stderr)
    );

    let cpp_stdout = String::from_utf8(cpp_output.stdout).expect("utf-8 C++ demo output");
    let rust_stdout = run_taipsio_like_demo().expect("run Rust t_aipsio demo");

    assert_eq!(normalize_lines(&rust_stdout), normalize_lines(&cpp_stdout));

    let _ = std::fs::remove_dir_all(&temp_dir);
}

fn casacore_taipsio_source_path() -> PathBuf {
    let manifest_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    manifest_dir.join("../../../casacore/casa/IO/test/tAipsIO.cc")
}

fn compile_cpp_demo(cxx: &str, source: &Path, output_bin: &Path) {
    let cflags = command_stdout("pkg-config", &["--cflags", "casacore"]);
    let libs = command_stdout("pkg-config", &["--libs", "casacore"]);

    let mut cmd = Command::new(cxx);
    cmd.arg("-std=c++17");
    for arg in cflags.split_whitespace() {
        cmd.arg(arg);
    }
    cmd.arg(source);
    cmd.arg("-o");
    cmd.arg(output_bin);
    for arg in libs.split_whitespace() {
        cmd.arg(arg);
    }

    let output = cmd.output().expect("compile C++ tAipsIO");
    assert!(
        output.status.success(),
        "C++ compilation failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
}

fn command_stdout(program: &str, args: &[&str]) -> String {
    let output = Command::new(program)
        .args(args)
        .output()
        .unwrap_or_else(|err| panic!("failed to run {program}: {err}"));
    assert!(
        output.status.success(),
        "{program} {} failed: {}",
        args.join(" "),
        String::from_utf8_lossy(&output.stderr)
    );
    String::from_utf8(output.stdout).expect("utf-8 command output")
}

fn normalize_lines(text: &str) -> String {
    text.replace("\r\n", "\n").trim_end().to_string()
}

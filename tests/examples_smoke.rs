//! Smoke tests that run the public examples to ensure they build and complete.
//!
//! These are marked `#[ignore]` by default to keep `cargo test` fast and
//! deterministic in environments without sufficient resources. Run them locally
//! with:
//!
//!     cargo test --test examples_smoke -- --ignored --nocapture
//!
//! Notes:
//! - Examples perform short sleeps (~0.4–0.6s) and complete quickly.
//! - We assert a successful exit status and capture stdout for debugging.

use std::process::Command;

fn run_example(name: &str, extra: &[&str]) {
    let mut cmd = Command::new("cargo");
    cmd.args(["run", "--example", name, "--quiet"]).args(extra);
    // Run from crate root (tests execute with current_dir already at crate root)
    let output = cmd.output().expect("failed to spawn example");
    if !output.status.success() {
        eprintln!(
            "\n===== STDERR ({name}) =====\n{}\n",
            String::from_utf8_lossy(&output.stderr)
        );
        eprintln!(
            "\n===== STDOUT ({name}) =====\n{}\n",
            String::from_utf8_lossy(&output.stdout)
        );
    }
    assert!(
        output.status.success(),
        "example `{}` exited with failure",
        name
    );
}

#[ignore]
#[test]
fn example_local_runs() {
    run_example("local", &[]);
}

#[ignore]
#[test]
fn example_sharded_runs() {
    run_example("sharded", &[]);
}

#[ignore]
#[test]
fn example_replicated_causal_runs() {
    // replicated supports choosing lattice via flag
    run_example("replicated", &["--", "--lattice", "causal"]);
}

#[ignore]
#[test]
fn example_replicated_lww_runs() {
    run_example("replicated", &["--", "--lattice", "lww"]);
}

#[ignore]
#[test]
fn example_sharded_replicated_runs() {
    run_example("sharded_replicated", &[]);
}

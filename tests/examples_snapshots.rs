// Snapshot tests for example outputs using insta.
// These validate that the user-facing stdout of our examples remains stable.
//
// To update snapshots locally:
//   INSTA_UPDATE=always cargo test -q --test examples_snapshots -- --nocapture
// or use the review tool:
//   cargo insta review

use std::process::Command;

// Retain only stable, user-facing semantic lines; drop internal process launch noise.
fn sanitize(output: &str) -> String {
    output
        .lines()
        .filter(|l| {
            let trimmed = l.trim_start();
            trimmed.starts_with("🚀") ||
            trimmed.starts_with("📋") ||
            trimmed.starts_with("Cluster specification") ||
            trimmed.starts_with("Shards (") ||
            trimmed.starts_with("Replicas per shard") ||
            trimmed.starts_with("Total nodes") ||
            trimmed.starts_with("→ ") ||
            trimmed.starts_with("✅") ||
            // shard info lines start with an arrow after spaces
            trimmed.starts_with("→ shard")
        })
        .collect::<Vec<_>>()
        .join("\n")
}

fn run_example(name: &str, extra: &[&str]) -> (String, String) {
    let mut cmd = Command::new("cargo");
    cmd.args(["run", "--example", name, "--quiet"]).args(extra);
    let output = cmd.output().expect("failed to spawn example");
    let stdout = String::from_utf8_lossy(&output.stdout).to_string();
    let stderr = String::from_utf8_lossy(&output.stderr).to_string();
    if !output.status.success() {
        eprintln!("example `{}` failed. status={:?}\nSTDERR:\n{}\nSTDOUT:\n{}", name, output.status, stderr, stdout);
        panic!("example `{}` exited with failure", name);
    }
    (stdout, stderr)
}

#[test]
fn snapshot_local() {
    let (stdout, _stderr) = run_example("local", &[]);
    insta::assert_snapshot!("example_local_stdout", sanitize(&stdout));
}

#[test]
fn snapshot_sharded() {
    let (stdout, _stderr) = run_example("sharded", &[]);
    insta::assert_snapshot!("example_sharded_stdout", sanitize(&stdout));
}

#[test]
fn snapshot_replicated_causal() {
    let (stdout, _stderr) = run_example("replicated", &["--", "--lattice", "causal"]);
    insta::assert_snapshot!("example_replicated_causal_stdout", sanitize(&stdout));
}

#[test]
fn snapshot_replicated_lww() {
    let (stdout, _stderr) = run_example("replicated", &[]);
    insta::assert_snapshot!("example_replicated_lww_stdout", sanitize(&stdout));
}

#[test]
fn snapshot_sharded_replicated() {
    let (stdout, _stderr) = run_example("sharded_replicated", &[]);
    insta::assert_snapshot!("example_sharded_replicated_stdout", sanitize(&stdout));
}

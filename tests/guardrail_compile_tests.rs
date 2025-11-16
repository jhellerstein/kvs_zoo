#[test]
fn leaf_guardrails_enforced() {
    let t = trybuild::TestCases::new();
    t.compile_fail("tests/compile_fail/leaf_requires_leafcompatible.rs");
    t.pass("tests/compile_pass/leaf_compatible_responder.rs");
}

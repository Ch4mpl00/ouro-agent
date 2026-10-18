// The specification's own examples (tests/spec-examples, see schema/README.md):
// each one is valid by the bundled schema, and our parser either accepts it
// or rejects it only for a construct this runtime does not support. Any
// other rejection means the parser disagrees with the specification.
use owf::model::{Invalid, conforms, parse};
use std::path::PathBuf;

/// Messages a deliberate subset produces (see model.rs, section 2).
const UNSUPPORTED: &[&str] =
    &["not supported", "scheduler", "only jq", "only strict", "only JSON Schema", "inline the `document`"];

fn examples() -> Vec<(String, serde_json::Value)> {
    let dir = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/spec-examples");
    let mut out: Vec<_> = std::fs::read_dir(dir)
        .unwrap()
        .map(|e| e.unwrap().path())
        .map(|p| {
            (
                p.file_name().unwrap().to_string_lossy().into_owned(),
                serde_yaml_ng::from_str(&std::fs::read_to_string(&p).unwrap()).unwrap(),
            )
        })
        .collect();
    out.sort_by(|a, b| a.0.cmp(&b.0));
    out
}

#[test]
fn every_example_conforms_to_the_bundled_schema() {
    for (name, doc) in examples() {
        conforms(&doc).unwrap_or_else(|e| panic!("{name}: {e}"));
    }
}

#[test]
fn the_parser_accepts_examples_or_names_the_unsupported_construct() {
    let mut accepted = Vec::new();
    for (name, doc) in examples() {
        match parse(doc) {
            Ok(_) => accepted.push(name),
            Err(Invalid(problems)) => {
                for p in &problems {
                    let m = p.to_string();
                    assert!(UNSUPPORTED.iter().any(|u| m.contains(u)), "{name}: unexpected rejection {m}");
                }
            }
        }
    }
    // The supported core of the DSL is exercised by real examples.
    for expected in [
        "do-multiple.yaml",
        "for-inline-array.yaml",
        "fork.yaml",
        "conditional-task.yaml",
        "raise-inline.yaml",
        "try-catch.yaml",
        "switch-then-string.yaml",
        "wait-duration-inline.yaml",
    ] {
        assert!(accepted.iter().any(|a| a == expected), "{expected} should parse; accepted: {accepted:?}");
    }
}

#[test]
fn the_schema_rejects_what_the_parser_would_otherwise_miss() {
    let doc: serde_json::Value = serde_yaml_ng::from_str(
        "document: { dsl: '1.0.4', namespace: t, name: w, version: 'not-semver' }\ndo: [ { a: { set: { x: 1 } } } ]",
    )
    .unwrap();
    assert!(conforms(&doc).is_err(), "the schema constrains document.version to semver");
}

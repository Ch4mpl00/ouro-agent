// Port of src/workflow/variables.test.ts, one test per case.
use agent::workflow::{VariableStore, WorkflowError, substitute};
use serde_json::{Map, Value, json};
use std::sync::Arc;

fn store(initial: Value) -> Arc<VariableStore> {
    VariableStore::new(initial.as_object().cloned().unwrap_or_else(Map::new))
}

fn missing(error: anyhow::Error) -> bool {
    matches!(error.downcast_ref::<WorkflowError>(), Some(WorkflowError::Missing(_)))
}

// ── createStore ─────────────────────────────────────────────────────────────

#[test]
fn returns_primitive_top_level_values() {
    let s = store(json!({"count":7,"name":"alice"}));
    assert_eq!(s.get("count").unwrap(), 7);
    assert_eq!(s.get("name").unwrap(), "alice");
}

#[test]
fn walks_dot_notation_paths_into_nested_objects() {
    let s = store(json!({"env":{"chatId":42,"tz":"Europe/Kiev"}}));
    assert_eq!(s.get("env.chatId").unwrap(), 42);
    assert_eq!(s.get("env.tz").unwrap(), "Europe/Kiev");
}

#[test]
fn throws_missing_binding_on_a_missing_top_level_key() {
    assert!(matches!(store(json!({"env":{}})).get("posts"), Err(WorkflowError::Missing(_))));
}

#[test]
fn throws_missing_binding_on_a_missing_nested_key() {
    assert!(matches!(store(json!({"env":{"tz":"UTC"}})).get("env.chatId"), Err(WorkflowError::Missing(_))));
}

#[test]
fn throws_missing_binding_when_traversing_through_a_non_object() {
    assert!(matches!(store(json!({"env":"not-an-object"})).get("env.foo"), Err(WorkflowError::Missing(_))));
}

#[test]
fn preserves_null_vs_missing_explicit_null_is_found_a_missing_key_is_not() {
    let s = store(json!({"env":{"chatId":null}}));
    assert!(s.has("env.chatId"));
    assert_eq!(s.get("env.chatId").unwrap(), Value::Null);
    assert!(!s.has("env.tz"));
}

#[test]
fn has_returns_true_for_existing_paths_false_for_missing() {
    let s = store(json!({"a":{"b":1}}));
    assert!(s.has("a"));
    assert!(s.has("a.b"));
    assert!(!s.has("a.c"));
    assert!(!s.has("x"));
}

#[test]
fn set_adds_a_new_binding() {
    let s = store(json!({}));
    s.set("posts", json!([1, 2, 3])).unwrap();
    assert_eq!(s.get("posts").unwrap(), json!([1, 2, 3]));
}

#[test]
fn set_throws_duplicate_binding_on_a_second_write_to_the_same_name() {
    let s = store(json!({"posts":[]}));
    assert!(matches!(s.set("posts", json!([1])), Err(WorkflowError::Duplicate(_))));
}

#[test]
fn snapshot_returns_a_plain_copy_of_the_store() {
    let s = store(json!({"a":1}));
    s.set("b", json!(2)).unwrap();
    assert_eq!(Value::Object(s.snapshot()), json!({"a":1,"b":2}));
}

// ── substitute — string mode ────────────────────────────────────────────────

#[test]
fn a_whole_string_placeholder_returns_the_raw_value_preserving_its_type() {
    let s = store(json!({"posts":[{"id":1},{"id":2}]}));
    assert_eq!(substitute(&json!("${posts}"), &s).unwrap(), json!([{"id":1},{"id":2}]));
}

#[test]
fn a_whole_string_placeholder_returns_a_number_as_a_number() {
    assert_eq!(substitute(&json!("${count}"), &store(json!({"count":7}))).unwrap(), json!(7));
}

#[test]
fn a_whole_string_placeholder_returns_null_as_null() {
    assert_eq!(substitute(&json!("${x}"), &store(json!({"x":null}))).unwrap(), Value::Null);
}

#[test]
fn interpolation_stringifies_non_string_values() {
    let s = store(json!({"name":"Bob","count":7}));
    assert_eq!(substitute(&json!("Hello ${name}, count=${count}"), &s).unwrap(), "Hello Bob, count=7");
}

#[test]
fn interpolation_json_stringifies_object_values() {
    assert_eq!(substitute(&json!("payload=${x}"), &store(json!({"x":{"a":1}}))).unwrap(), "payload={\"a\":1}");
}

#[test]
fn interpolation_walks_nested_paths() {
    let s = store(json!({"env":{"chatId":42,"tz":"Europe/Kiev"}}));
    assert_eq!(substitute(&json!("chat=${env.chatId} tz=${env.tz}"), &s).unwrap(), "chat=42 tz=Europe/Kiev");
}

#[test]
fn a_missing_binding_in_whole_string_mode_throws_missing_binding() {
    assert!(missing(substitute(&json!("${posts}"), &store(json!({}))).unwrap_err()));
}

#[test]
fn a_missing_binding_in_interpolation_mode_throws_missing_binding() {
    assert!(missing(substitute(&json!("hi ${name}"), &store(json!({}))).unwrap_err()));
}

#[test]
fn strings_without_placeholders_pass_through_unchanged() {
    assert_eq!(substitute(&json!("plain text"), &store(json!({}))).unwrap(), "plain text");
}

// ── substitute — recursive walks ────────────────────────────────────────────

#[test]
fn walks_object_values_recursively() {
    let s = store(json!({"env":{"chatId":42},"text":"hello"}));
    assert_eq!(
        substitute(&json!({"chatId":"${env.chatId}","text":"${text}"}), &s).unwrap(),
        json!({"chatId":42,"text":"hello"})
    );
}

#[test]
fn walks_array_values_recursively() {
    let s = store(json!({"a":1,"b":2}));
    assert_eq!(substitute(&json!(["${a}", "${b}", "literal"]), &s).unwrap(), json!([1, 2, "literal"]));
}

#[test]
fn does_not_mutate_the_input() {
    let input = json!({"chatId":"${env.chatId}"});
    let out = substitute(&input, &store(json!({"env":{"chatId":42}}))).unwrap();
    assert_eq!(input["chatId"], "${env.chatId}");
    assert_eq!(out["chatId"], 42);
}

#[test]
fn passes_non_string_primitives_through_unchanged() {
    let s = store(json!({}));
    assert_eq!(substitute(&json!(42), &s).unwrap(), json!(42));
    assert_eq!(substitute(&json!(true), &s).unwrap(), json!(true));
    assert_eq!(substitute(&Value::Null, &s).unwrap(), Value::Null);
}

#[test]
fn handles_deeply_nested_args_without_losing_types() {
    let s = store(json!({"env":{"chatId":285083560},"digest":"📰 News"}));
    let args = json!({"chatId":"${env.chatId}","text":"${digest}","meta":{"recipient":"${env.chatId}"}});
    assert_eq!(
        substitute(&args, &s).unwrap(),
        json!({"chatId":285083560,"text":"📰 News","meta":{"recipient":285083560}})
    );
}

// ── array index paths (map-reduce chunks) ───────────────────────────────────

#[test]
fn resolves_bind_chunks_n_to_the_nth_array_element_as_is() {
    let chunk0 = json!([{"body":"a"},{"body":"b"}]);
    let chunk1 = json!([{"body":"c"}]);
    let s = store(json!({"posts":{"count":3,"chunks":[chunk0, chunk1, []]}}));
    assert_eq!(substitute(&json!("${posts.chunks.0}"), &s).unwrap(), chunk0);
    assert_eq!(substitute(&json!("${posts.chunks.1}"), &s).unwrap(), chunk1);
    assert_eq!(substitute(&json!("${posts.chunks.2}"), &s).unwrap(), json!([]));
}

#[test]
fn throws_missing_binding_for_an_out_of_range_index() {
    let error = substitute(&json!("${posts.chunks.5}"), &store(json!({"posts":{"chunks":[[]]}}))).unwrap_err();
    assert!(error.to_string().contains("posts.chunks.5"), "{error}");
}

//! The built-in workflows under the engine's dialect (migration step 1).
//!
//! The engine (`ows`) reads its extensions only under `metadata.ouro` and
//! silently ignores flat `metadata` keys, the dialect of the interpreter the
//! agent ran before (removed in step 6, design 07). `to_engine_dialect` is the
//! conversion step 2 applied to the built-ins; it is kept as the check that no
//! flat key is left in them, since the engine would drop one without a word.

use serde_yaml_ng::{Mapping, Value as Yaml};

const TASK_KEYS: &[&str] = &["concurrency", "hold", "delivery"];
const DOCUMENT_KEYS: &[&str] = &["limits", "at", "timezone", "catchup", "overlap"];
const TASK_KINDS: &[&str] =
    &["call", "do", "for", "fork", "set", "switch", "try", "raise", "wait", "listen", "emit", "run"];
/// Subtrees that hold data, not tasks: never searched for `metadata`.
const DATA_KEYS: &[&str] = &["with", "set", "input", "output", "export", "schedule"];

/// Moves the flat extension keys under `metadata.ouro`, on the document and on
/// every task. A YAML value transform: other metadata (`loop`, `route`, …) stays.
fn to_engine_dialect(yaml: &str) -> String {
    let mut doc: Yaml = serde_yaml_ng::from_str(yaml).expect("yaml");
    if let Some(document) = doc.get_mut("document").and_then(Yaml::as_mapping_mut) {
        nest(document, DOCUMENT_KEYS);
    }
    if let Some(root) = doc.as_mapping_mut() {
        walk_tasks(root);
    }
    serde_yaml_ng::to_string(&doc).expect("yaml")
}

fn nest(owner: &mut Mapping, keys: &[&str]) {
    let Some(metadata) = owner.get_mut("metadata").and_then(Yaml::as_mapping_mut) else {
        return;
    };
    let moved: Vec<(Yaml, Yaml)> =
        keys.iter().filter_map(|k| metadata.remove(*k).map(|v| (Yaml::from(*k), v))).collect();
    if moved.is_empty() {
        return;
    }
    let slot = metadata.entry("ouro".into()).or_insert_with(|| Yaml::Mapping(Mapping::new()));
    let ouro = slot.as_mapping_mut().expect("metadata.ouro is a mapping");
    ouro.extend(moved);
}

fn walk_tasks(node: &mut Mapping) {
    if TASK_KINDS.iter().any(|k| node.contains_key(*k)) {
        nest(node, TASK_KEYS);
    }
    for (key, value) in node.iter_mut() {
        if key.as_str().is_some_and(|k| DATA_KEYS.contains(&k)) {
            continue;
        }
        walk_value(value);
    }
}

fn walk_value(value: &mut Yaml) {
    match value {
        Yaml::Mapping(m) => walk_tasks(m),
        Yaml::Sequence(items) => items.iter_mut().for_each(walk_value),
        _ => {}
    }
}

/// What a task means to the interpreter, whichever crate parsed it.
#[derive(Debug, PartialEq)]
struct Effective {
    path: String,
    kind: &'static str,
    delivery: String,
    hold_ms: u128,
    concurrency: Option<usize>,
}

fn ows_effective(tasks: &ows::model::TaskList, at: &str, out: &mut Vec<Effective>) {
    use ows::model::Kind;
    for (name, task) in tasks {
        let path = format!("{at}/{name}");
        let concurrency = match &task.kind {
            Kind::For { concurrency, .. } => *concurrency,
            _ => None,
        };
        out.push(Effective {
            path: path.clone(),
            kind: task.kind.name(),
            delivery: format!("{:?}", task.delivery),
            hold_ms: task.hold.as_millis(),
            concurrency,
        });
        match &task.kind {
            Kind::Do(inner) | Kind::For { tasks: inner, .. } => ows_effective(inner, &path, out),
            Kind::Fork { branches, .. } => ows_effective(branches, &path, out),
            Kind::Try { tasks: inner, catch } => {
                ows_effective(inner, &path, out);
                if let Some(handler) = &catch.tasks {
                    ows_effective(handler, &format!("{path}/catch"), out);
                }
            }
            _ => {}
        }
    }
}

struct AcceptAll;

impl ows::model::Catalog for AcceptAll {
    fn has(&self, _: &str) -> bool {
        true
    }
    fn events(&self) -> bool {
        true
    }
    fn emits(&self) -> bool {
        true
    }
    fn children(&self) -> bool {
        true
    }
}

fn ows_workflow(text: &str) -> ows::model::Workflow {
    let workflow = ows::model::parse_yaml(text).expect("ows parses");
    ows::model::validate(&workflow, &AcceptAll).expect("ows validates");
    workflow
}

fn ows_tasks(text: &str) -> Vec<Effective> {
    let mut out = Vec::new();
    ows_effective(&ows_workflow(text).tasks, "", &mut out);
    out
}

/// Whether any task carries an extension (the file has something to lose).
fn has_extensions(tasks: &[Effective]) -> bool {
    tasks.iter().any(|t| t.delivery != "AtLeastOnce" || t.hold_ms > 0 || t.concurrency.is_some())
}

const SYNTHETIC: &str = r#"
document:
  dsl: '1.0.4'
  namespace: agent
  name: synthetic
  version: '1.0.0'
  metadata: { loop: { description: kept }, limits: { max_jumps: 5 } }
do:
  - each:
      for: { each: item, in: '${ [1, 2] }' }
      metadata: { concurrency: 2, note: kept }
      do:
        - send:
            call: send
            metadata: { delivery: at_most_once }
  - wait:
      listen:
        to: { one: { with: { type: ping } } }
      metadata: { hold: PT7S }
"#;

const SCHEDULED: &str = r#"
document:
  dsl: '1.0.4'
  namespace: agent
  name: scheduled
  version: '1.0.0'
  metadata: { timezone: Europe/Berlin, catchup: none, overlap: queue }
schedule: { cron: '0 9 * * *' }
do:
  - tick: { call: tick }
"#;

// What the synthetic definition's flat keys mean, read under
// `metadata.ouro`: every extension it carries.
#[test]
fn converted_flat_keys_reach_the_engine() {
    let tasks = ows_tasks(&to_engine_dialect(SYNTHETIC));
    let each = tasks.iter().find(|t| t.path == "/each").expect("each");
    assert_eq!(each.concurrency, Some(2));
    let send = tasks.iter().find(|t| t.path == "/each/send").expect("send");
    assert_eq!(send.delivery, "AtMostOnce");
    let wait = tasks.iter().find(|t| t.path == "/wait").expect("wait");
    assert_eq!(wait.hold_ms, 7000);
}

// Since step 2 the built-ins are engine definitions: they keep their
// extensions under `metadata.ouro`, the conversion leaves them as they are,
// and they opt out of the breaker.
#[test]
fn the_built_ins_are_written_in_the_engine_dialect() {
    for (name, yaml) in agent::flows::builtins() {
        let tasks = ows_tasks(yaml);
        assert_eq!(ows_tasks(&to_engine_dialect(yaml)), tasks, "{name}");
        let wf = ows_workflow(yaml);
        assert_eq!(wf.definition["document"]["metadata"]["ouro"]["breaker"], "off", "{name}");
    }
    assert!(has_extensions(&ows_tasks(agent::agent::AGENT_LOOP)));
    assert!(has_extensions(&ows_tasks(agent::flows::GUARD)));
}

// Why the check above matters: a flat key is no error to the engine, it is
// dropped.
#[test]
fn the_engine_silently_drops_flat_keys() {
    let unconverted = ows_tasks(SYNTHETIC);
    assert!(!has_extensions(&unconverted), "ows read a flat key");
    assert_ne!(unconverted, ows_tasks(&to_engine_dialect(SYNTHETIC)));
}

#[test]
fn conversion_keeps_other_metadata() {
    let converted: Yaml = serde_yaml_ng::from_str(&to_engine_dialect(SYNTHETIC)).unwrap();
    let document = &converted["document"]["metadata"];
    assert_eq!(document["loop"]["description"], Yaml::from("kept"));
    assert_eq!(document["ouro"]["limits"]["max_jumps"], Yaml::from(5));
    assert!(document.get("limits").is_none());
    let each = &converted["do"][0]["each"]["metadata"];
    assert_eq!(each["note"], Yaml::from("kept"));
    assert_eq!(each["ouro"]["concurrency"], Yaml::from(2));
    assert!(each.get("concurrency").is_none());
}

#[test]
fn document_extensions_reach_the_engine() {
    let schedule = ows_workflow(&to_engine_dialect(SCHEDULED)).schedule.expect("schedule");
    assert_eq!(schedule.timezone, Some(chrono_tz::Tz::Europe__Berlin));
    assert_eq!(schedule.catchup, ows::model::Catchup::None);
    assert_eq!(schedule.overlap, ows::model::Overlap::Queue);

    let flat = ows_workflow(SCHEDULED).schedule.expect("schedule");
    assert_eq!(flat.timezone, None, "flat keys are ignored");

    let limited = ows_workflow(&to_engine_dialect(SYNTHETIC));
    assert_eq!(limited.budget.max_jumps, Some(5));
    assert_eq!(ows_workflow(SYNTHETIC).budget.max_jumps, None);
}

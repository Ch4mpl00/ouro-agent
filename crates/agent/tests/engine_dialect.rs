//! The built-in workflows under the engine's dialect (migration step 1).
//!
//! The running `owf` reads its extensions as flat `metadata` keys; the engine
//! (`ows`) reads them only under `metadata.ouro` and silently ignores flat
//! ones. `to_engine_dialect` is the conversion step 2 applies to the files; here
//! it is shown to preserve what each task means.

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

macro_rules! effective {
    ($engine:ident, $name:ident) => {
        fn $name(tasks: &$engine::model::TaskList, at: &str, out: &mut Vec<Effective>) {
            use $engine::model::Kind;
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
                    Kind::Do(inner) | Kind::For { tasks: inner, .. } => $name(inner, &path, out),
                    Kind::Fork { branches, .. } => $name(branches, &path, out),
                    Kind::Try { tasks: inner, catch } => {
                        $name(inner, &path, out);
                        if let Some(handler) = &catch.tasks {
                            $name(handler, &format!("{path}/catch"), out);
                        }
                    }
                    _ => {}
                }
            }
        }
    };
}
effective!(owf, owf_effective);
effective!(ows, ows_effective);

struct AcceptAll;

impl owf::model::Catalog for AcceptAll {
    fn has(&self, _: &str) -> bool {
        true
    }
    fn events(&self) -> bool {
        true
    }
}

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

fn owf_tasks(text: &str) -> Vec<Effective> {
    let workflow = owf::model::parse_yaml(text).expect("owf parses");
    owf::model::validate(&workflow, &AcceptAll).expect("owf validates");
    let mut out = Vec::new();
    owf_effective(&workflow.tasks, "", &mut out);
    out
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

fn builtins() -> Vec<(&'static str, String)> {
    let workflows = concat!(env!("CARGO_MANIFEST_DIR"), "/workflows");
    let fixtures = concat!(env!("CARGO_MANIFEST_DIR"), "/tests/fixtures");
    let mut files: Vec<(&'static str, String)> = ["agent-loop.yaml", "signal.yaml", "guard.yaml"]
        .into_iter()
        .map(|n| (n, std::fs::read_to_string(format!("{workflows}/{n}")).expect("built-in")))
        .collect();
    files.push((
        "telegram-loop.yaml",
        std::fs::read_to_string(format!("{fixtures}/telegram-loop.yaml")).expect("fixture"),
    ));
    files.push(("synthetic", SYNTHETIC.to_string()));
    files
}

#[test]
fn converted_workflows_mean_the_same_task_by_task() {
    let mut with_extensions = 0;
    for (name, original) in builtins() {
        let converted = to_engine_dialect(&original);
        let flat = owf_tasks(&original);
        let nested = ows_tasks(&converted);
        assert_eq!(flat, nested, "{name}");
        if has_extensions(&flat) {
            with_extensions += 1;
        }
    }
    // agent-loop, guard and the synthetic one carry extensions.
    assert!(with_extensions >= 3, "{with_extensions}");
}

#[test]
fn the_engine_silently_drops_flat_keys() {
    for (name, original) in builtins() {
        let flat = owf_tasks(&original);
        if !has_extensions(&flat) {
            continue;
        }
        let unconverted = ows_tasks(&original);
        assert!(!has_extensions(&unconverted), "{name}: ows read a flat key");
        assert_ne!(flat, unconverted, "{name}");
    }
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

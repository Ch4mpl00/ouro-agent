// Ports of the workflow property tests in packages/agent/test:
//   - workflow.test.ts: the variable store against plain JSON, `${}`
//     substitution against its contract, and the plan validator against the
//     executor — an accepted plan never dies of a missing or duplicate
//     binding at run time;
//   - workflow-replan.test.ts: plan → act → replan with the real compiler and
//     executor and a scripted planner;
//   - workflow-patch.test.ts: the improver's patch overlay on the workflow
//     path, on a real skill store;
//   - workflow-execute.test.ts: the executor under generated plans and
//     faults, with nothing left running once it returns.
mod support;
use agent::{
    agent::{EngineDeps, Providers, create_engine},
    codex::Codex,
    decisions::decision_client,
    models::default_presets,
    providers::{ChatProvider, CompletionParams, CompletionResult, JsonApi, ProviderKind, ToolDef, assistant, role},
    skills::FsSkillStore,
    tracing::{NullTracer, null_scope},
    workflow::{
        ExecContext, ExecResult, RunResult, VariableStore, Workflow, WorkflowError, WorkflowRun, WorkflowSchema,
        WorkflowSignal, create_workflow_runner, substitute,
    },
};
use async_trait::async_trait;
use proptest::prelude::*;
use proptest::test_runner::{Config, TestCaseError, TestRunner};
use serde_json::{Map, Value, json};
use std::collections::{BTreeSet, HashMap};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, LazyLock, Mutex};
use std::time::Duration;
use support::{AnySkill, NoCodex, PersistLog, ToolMcp, task_context};
use tokio_util::sync::CancellationToken;

fn runs(default: u32) -> u32 {
    std::env::var("PROPERTY_RUNS").ok().and_then(|v| v.parse().ok()).unwrap_or(default)
}

fn runtime() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap()
}

// ── the variable store against plain JSON ───────────────────────────────────

// A path exists when every segment names an OWN property of the value
// reached so far — as JS sees own properties: object keys, array indices
// and an array's length.
fn walk(root: &Map<String, Value>, path: &str) -> Option<Value> {
    let mut current = Value::Object(root.clone());
    for seg in path.split('.') {
        current = match &current {
            Value::Object(m) => m.get(seg).cloned()?,
            Value::Array(a) if seg == "length" => json!(a.len()),
            Value::Array(a) => {
                seg.parse::<usize>().ok().filter(|i| i.to_string() == seg).and_then(|i| a.get(i).cloned())?
            }
            _ => return None,
        };
    }
    Some(current)
}

const INHERITED: [&str; 7] = ["constructor", "toString", "__proto__", "hasOwnProperty", "valueOf", "length", "map"];

// fc.jsonValue({ maxDepth: 3 }); keys include names JS objects inherit.
fn json_value() -> impl Strategy<Value = Value> {
    let leaf = prop_oneof![
        Just(Value::Null),
        any::<bool>().prop_map(Value::from),
        any::<i32>().prop_map(Value::from),
        ".{0,8}".prop_map(Value::from),
    ];
    let key = prop_oneof![3 => "[a-z]{1,4}", 1 => prop::sample::select(INHERITED.to_vec()).prop_map(str::to_owned), 1 => "[0-9]".prop_map(String::from)];
    leaf.prop_recursive(3, 24, 4, move |inner| {
        prop_oneof![
            prop::collection::vec(inner.clone(), 0..4).prop_map(Value::Array),
            prop::collection::btree_map(key.clone(), inner, 0..4).prop_map(|m| Value::Object(m.into_iter().collect())),
        ]
    })
}

fn bindings() -> impl Strategy<Value = Map<String, Value>> {
    prop::collection::btree_map(prop::sample::select(vec!["posts", "user", "result", "items"]), json_value(), 1..=4)
        .prop_map(|m| m.into_iter().map(|(k, v)| (k.to_owned(), v)).collect())
}

// Paths into the bindings: real keys, plus inherited names and plain misses
// at any depth.
fn paths_into(b: &Map<String, Value>) -> impl Strategy<Value = String> + use<> {
    let mut real = Vec::new();
    fn visit(value: &Value, prefix: &str, depth: usize, real: &mut Vec<String>) {
        if depth > 3 {
            return;
        }
        let keys: Vec<(String, &Value)> = match value {
            Value::Object(m) => m.iter().map(|(k, v)| (k.clone(), v)).collect(),
            Value::Array(a) => a.iter().enumerate().map(|(i, v)| (i.to_string(), v)).collect(),
            _ => return,
        };
        for (key, child) in keys {
            // Not addressable in `${a.b}` syntax: a dot splits, a brace ends it.
            if key.contains('.') || key.contains('}') || key.is_empty() {
                continue;
            }
            let path = format!("{prefix}.{key}");
            real.push(path.clone());
            visit(child, &path, depth + 1, real);
        }
    }
    for (name, value) in b {
        real.push(name.clone());
        visit(value, name, 0, &mut real);
    }
    let base = prop::sample::select(real);
    let mut tails: Vec<&str> = INHERITED.to_vec();
    tails.extend(["missing", "0"]);
    prop_oneof![
        3 => base.clone(),
        2 => (base, prop::sample::select(tails)).prop_map(|(p, s)| format!("{p}.{s}")),
        1 => prop::sample::select([INHERITED.to_vec(), vec!["nobody"]].concat()).prop_map(str::to_owned),
    ]
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(runs(300)))]
    #[test]
    fn resolves_exactly_the_paths_that_exist_in_the_bound_data(
        (b, paths) in bindings().prop_flat_map(|b| { let paths = prop::collection::vec(paths_into(&b), 1..=10); (Just(b), paths) })
    ) {
        let store = VariableStore::new(b.clone());
        for path in paths {
            let want = walk(&b, &path);
            prop_assert_eq!(store.has(&path), want.is_some(), "has({})", path);
            match want {
                Some(value) => prop_assert_eq!(store.get(&path).unwrap(), value, "get({})", path),
                None => prop_assert!(matches!(store.get(&path), Err(WorkflowError::Missing(_))), "get({}) must fail", path),
            }
        }
    }

    // Interpolation: literal text stays, each placeholder becomes its value —
    // strings as-is, null spelled out, anything else as JSON. A value that
    // itself looks like a placeholder is never expanded again. A whole-string
    // placeholder is the value itself, type and all.
    #[test]
    fn substitutes_placeholders_by_their_values_and_nothing_else(
        (b, parts, tail) in bindings().prop_flat_map(|b| {
            let literal = || ".{0,12}".prop_filter("no placeholder", |s: &String| !s.contains("${"));
            let parts = prop::collection::vec((literal(), paths_into(&b)), 1..=4);
            (Just(b), parts, literal())
        }),
        sneaky in prop::sample::select(vec!["${posts}", "${user.name}"]),
    ) {
        let mut seeded = b.clone();
        seeded.insert("sneaky".into(), json!(sneaky));
        let store = VariableStore::new(seeded);
        let template = parts.iter().map(|(lit, path)| format!("{lit}${{{path}}}")).collect::<String>() + &tail;
        let resolvable = parts.iter().all(|(_, path)| walk(&b, path).is_some());
        if !resolvable {
            let error = substitute(&json!(template), &store).unwrap_err();
            prop_assert!(matches!(error.downcast_ref::<WorkflowError>(), Some(WorkflowError::Missing(_))));
            return Ok(());
        }
        let render = |v: Value| v.as_str().map(str::to_owned).unwrap_or_else(|| v.to_string());
        let want = parts.iter().map(|(lit, path)| format!("{lit}{}", render(walk(&b, path).unwrap()))).collect::<String>() + &tail;
        if parts.len() == 1 && parts[0].0.is_empty() && tail.is_empty() {
            prop_assert_eq!(substitute(&json!(template), &store).unwrap(), walk(&b, &parts[0].1).unwrap());
        } else {
            prop_assert_eq!(substitute(&json!(template), &store).unwrap(), json!(want));
        }
        prop_assert_eq!(substitute(&json!("x ${sneaky} y"), &store).unwrap(), json!(format!("x {sneaky} y")));
    }
}

// ── the plan validator against the executor ─────────────────────────────────

const NAMES: [&str; 6] = ["a", "b", "c", "env", "signal", "context"];

// Text that may reference names: plain words, root placeholders, paths into
// the seeded env / signal, and placeholder look-alikes that aren't ones.
fn text_ref() -> impl Strategy<Value = String> {
    prop::collection::vec(
        prop_oneof![
            prop::sample::select(vec!["hi ", "", "price $5 ", "${} ", "$ {a} ", "${ a } "]).prop_map(str::to_owned),
            prop::sample::select(NAMES.to_vec()).prop_map(|n| format!("${{{n}}} ")),
            prop::sample::select(vec!["${env.timezone} ", "${signal.content} "]).prop_map(str::to_owned),
        ],
        0..=3,
    )
    .prop_map(|xs| {
        let joined = xs.concat();
        if joined.is_empty() { "go".into() } else { joined }
    })
}

fn bind() -> impl Strategy<Value = String> {
    prop::sample::select(vec!["a", "b", "c", "env"]).prop_map(str::to_owned)
}

fn leaf() -> impl Strategy<Value = Value> {
    prop_oneof![
        (text_ref(), prop::option::of(bind())).prop_map(|(q, b)| {
            let mut s = json!({"kind":"tool","tool":"t_read","args":{"q":q}});
            if let Some(b) = b {
                s["bind"] = json!(b);
            }
            s
        }),
        (text_ref(), text_ref(), bind()).prop_map(|(p, d, b)| json!({"kind":"llm_compose","preset":"base","prompt":p,"input":{"data":d},"bind":b})),
        (text_ref(), bind()).prop_map(|(p, b)| json!({"kind":"llm_agent","preset":"base","skill":"writer","prompt":p,"tools":["t_read"],"maxIterations":3,"bind":b})),
        (text_ref(), text_ref(), bind()).prop_map(|(t, d, b)| json!({"kind":"code_agent","task":t,"data":d,"bind":b})),
        (prop::sample::select(vec!["a", "b", "c"]), text_ref(), bind())
            .prop_map(|(n, p, b)| json!({"kind":"decide","preset":"filter","items":format!("${{{n}}}"),"prompt":p,"bind":b})),
    ]
}

fn step() -> impl Strategy<Value = Value> {
    prop_oneof![
        6 => leaf(),
        2 => prop::collection::vec(leaf(), 2..=3).prop_map(|steps| json!({"kind":"parallel","steps":steps})),
        1 => Just(json!({"kind":"terminal"})),
    ]
}

// Answers "[]" to everything.
struct Brackets;

#[async_trait]
impl ChatProvider for Brackets {
    fn kind(&self) -> ProviderKind {
        ProviderKind::Openai
    }
    async fn complete(&self, _: &mut CompletionParams<'_>) -> anyhow::Result<CompletionResult> {
        Ok(CompletionResult { message: assistant("[]"), finish_reason: Some("stop".into()), usage: None })
    }
}

struct BracketsCodex;

#[async_trait]
impl Codex for BracketsCodex {
    async fn run(&self, _: Value, _: &CancellationToken) -> anyhow::Result<String> {
        Ok("[]".into())
    }
}

struct Refusing;

#[async_trait]
impl JsonApi for Refusing {
    async fn post(&self, _: &str, _: Value, _: &CancellationToken) -> anyhow::Result<Value> {
        anyhow::bail!("the fake decision model refuses")
    }
}

const BINDING_FAILURE: [&str; 2] = ["missing_binding", "duplicate_binding"];

// Every call succeeds and returns an empty JSON array, so the only way a run
// can fail is the plan itself (or the refusing decision model).
async fn run_ok_engine(workflow: &Workflow, with_context: bool) -> ExecResult {
    let provider: Arc<dyn ChatProvider> = Arc::new(Brackets);
    let engine = create_engine(EngineDeps {
        providers: Providers {
            openai: provider.clone(),
            gemini: provider.clone(),
            deepseek: provider,
            openrouter: None,
        },
        mcp: ToolMcp::returning(vec![ToolDef::new("t_read", "t_read", json!({"type":"object"}))], "[]"),
        presets: default_presets(),
        skills: Vec::new(),
        skill_store: Arc::new(AnySkill),
        memory: Arc::new(PersistLog::default()),
        tracer: Arc::new(NullTracer),
        codex: Arc::new(BracketsCodex),
        decisions: Some(decision_client("fake".into(), Arc::new(Refusing))),
        workflows: std::sync::Arc::new(agent::flows::MemoryWorkflows::default()),
        schedule: std::sync::Arc::new(agent::schedule::MemorySchedule::default()),
    });
    let runner = create_workflow_runner(engine, vec!["writer".into()]).unwrap();
    let mut initial = json!({
        "env": {"timezone":"UTC","now":"2026-10-12T00:00:00.000Z"},
        "signal": {"source":"scheduler","content":"tick","id":1},
    });
    if with_context {
        initial["context"] = json!({"prior":1});
    }
    let ctx = ExecContext {
        session: task_context("t"),
        store: VariableStore::new(initial.as_object().unwrap().clone()),
        scope: null_scope(),
        signal_label: "scheduler:1".into(),
        cancel: CancellationToken::new(),
    };
    runner.execute(workflow, &ctx).await.unwrap()
}

fn validator_schema() -> WorkflowSchema {
    WorkflowSchema::new(vec!["t_read".into(), "t_send".into()], vec!["writer".into()]).unwrap()
}

#[test]
fn accepts_a_plan_only_if_it_cant_die_of_a_missing_or_duplicate_binding_and_only_rejects_such_plans() {
    let seen = Mutex::new(BTreeSet::new());
    let mut runner = TestRunner::new(Config { cases: runs(300), ..Config::default() });
    runner
        .run(&(prop::collection::vec(step(), 1..=5), any::<bool>()), |(steps, with_context)| {
            let input = json!({"version":1,"steps":steps});
            let seeds: Vec<&str> = if with_context { vec!["env", "signal", "context"] } else { vec!["env", "signal"] };
            let parsed = validator_schema().parse(input.clone(), Some(&seeds));
            let workflow: Workflow = serde_json::from_value(input.clone()).unwrap();
            let result = runtime().block_on(run_ok_engine(&workflow, with_context));
            let failure = match &result {
                ExecResult::Failed { reason, error, .. } => Some((*reason, error.to_string())),
                ExecResult::Ok { .. } => None,
            };
            let binding_failure = failure.as_ref().is_some_and(|(r, _)| BINDING_FAILURE.contains(r));
            match parsed {
                Ok(_) => {
                    seen.lock().unwrap().insert("accepted");
                    prop_assert!(!binding_failure, "accepted, yet {:?}: {}", failure, input);
                }
                Err(errors) => {
                    seen.lock().unwrap().insert("rejected");
                    // A sibling reference inside `parallel` only fails under
                    // some schedules, so it is rejected without having to fail
                    // this run.
                    let only_order_dependent = errors.iter().all(|e| e.contains("parallel"));
                    // The fake decision model refuses, so a decide step ends
                    // the run before a later binding error can show.
                    let decides = input.to_string().contains("\"kind\":\"decide\"");
                    if !only_order_dependent && !decides {
                        prop_assert!(binding_failure, "rejected ({}), yet ran: {}", errors.join("; "), input);
                    }
                }
            }
            Ok(())
        })
        .unwrap();
    assert_eq!(seen.into_inner().unwrap().into_iter().collect::<Vec<_>>(), ["accepted", "rejected"]);
}

#[test]
fn names_the_step_and_the_binding_in_each_error_so_the_compiler_can_fix_it() {
    let errors = validator_schema()
        .parse(
            json!({"version":1,"steps":[
                {"kind":"tool","tool":"t_send","args":{"text":"${draft}"}},
                {"kind":"llm_compose","preset":"base","prompt":"write","input":{},"bind":"draft"},
                {"kind":"llm_compose","preset":"base","prompt":"again","input":{},"bind":"draft"},
            ]}),
            Some(&["env", "signal"]),
        )
        .unwrap_err();
    assert!(errors.iter().any(|e| e.contains("steps[0]") && e.contains("draft")), "{errors:?}");
    assert!(errors.iter().any(|e| e.contains("steps[2]") && e.contains("draft")), "{errors:?}");
}

// ── plan, act, replan (workflow-replan.test.ts) ─────────────────────────────

const MAX_ATTEMPTS: usize = 3;

#[derive(Clone, Copy, Debug, PartialEq)]
enum Pass {
    Act,
    Fail,
    Replan,
    Invalid,
}

// The plan the planner emits on pass `p`.
fn plan_for(p: usize, kind: Pass) -> String {
    let carried = if p > 0 { format!(" with ${{context.v{}}}", p - 1) } else { String::new() };
    match kind {
        Pass::Invalid => "{ not a plan".into(),
        Pass::Act => json!({"version":1,"steps":[{"kind":"tool","tool":"t","args":{"q":format!("act {p}{carried}")}},{"kind":"terminal"}]}).to_string(),
        Pass::Fail => json!({"version":1,"steps":[{"kind":"tool","tool":"t","args":{"q":format!("FAIL {p}{carried}")}}]}).to_string(),
        Pass::Replan => json!({"version":1,"steps":[
            {"kind":"tool","tool":"t","args":{"q":format!("gather {p}{carried}")},"bind":format!("v{p}")},
            {"kind":"replan","context":[format!("v{p}"),"ghost"],"note":format!("note from {p}")},
        ]})
        .to_string(),
    }
}

struct Planner {
    script: Vec<Pass>,
    pass: AtomicUsize,
    attempts: AtomicUsize,
    prompts: Mutex<Vec<(usize, String)>>,
}

#[async_trait]
impl ChatProvider for Planner {
    fn kind(&self) -> ProviderKind {
        ProviderKind::Openai
    }
    async fn complete(&self, p: &mut CompletionParams<'_>) -> anyhow::Result<CompletionResult> {
        let user =
            p.messages.iter().find(|m| role(m) == "user").and_then(|m| m["content"].as_str()).unwrap_or_default();
        let pass = self.pass.load(Ordering::SeqCst);
        self.prompts.lock().unwrap().push((pass, user.to_owned()));
        let kind = self.script[pass];
        let attempts = self.attempts.fetch_add(1, Ordering::SeqCst) + 1;
        let content = plan_for(pass, kind);
        // The next planner call belongs to the next pass once this pass
        // produced a plan, or ran out of attempts.
        if kind != Pass::Invalid || attempts == MAX_ATTEMPTS {
            self.pass.fetch_add(1, Ordering::SeqCst);
            self.attempts.store(0, Ordering::SeqCst);
        }
        Ok(CompletionResult { message: assistant(&content), finish_reason: Some("stop".into()), usage: None })
    }
}

async fn plan_act_replan(max_passes: usize, script: Vec<Pass>) -> Result<(), TestCaseError> {
    let planner = Arc::new(Planner {
        script: script.clone(),
        pass: AtomicUsize::new(0),
        attempts: AtomicUsize::new(0),
        prompts: Mutex::default(),
    });
    let provider: Arc<dyn ChatProvider> = planner.clone();
    let mcp = ToolMcp::returning(
        vec![ToolDef::new(
            "t",
            "probe",
            json!({"type":"object","properties":{"q":{"type":"string"}},"required":["q"]}),
        )],
        "",
    );
    mcp.always(Arc::new(|_, args, _| {
        let q = args["q"].as_str().unwrap_or_default().to_owned();
        Box::pin(async move {
            if q.starts_with("FAIL") {
                return Ok("[tool error] boom".into());
            }
            Ok(json!({"gathered": q.split(' ').take(2).collect::<Vec<_>>().join(" ")}).to_string())
        })
    }));
    let engine = create_engine(EngineDeps {
        providers: Providers {
            openai: provider.clone(),
            gemini: provider.clone(),
            deepseek: provider,
            openrouter: None,
        },
        mcp: mcp.clone(),
        presets: default_presets(),
        skills: Vec::new(),
        skill_store: Arc::new(AnySkill),
        memory: Arc::new(PersistLog::default()),
        tracer: Arc::new(NullTracer),
        codex: Arc::new(NoCodex),
        decisions: None,
        workflows: std::sync::Arc::new(agent::flows::MemoryWorkflows::default()),
        schedule: std::sync::Arc::new(agent::schedule::MemorySchedule::default()),
    });
    let mut runner = create_workflow_runner(engine, vec!["s".into()]).unwrap();
    {
        let runner = Arc::get_mut(&mut runner).unwrap();
        runner.max_attempts = MAX_ATTEMPTS;
        runner.max_passes = max_passes;
    }
    let mut session = task_context("s");
    Arc::get_mut(&mut session).unwrap().env.now = "2026-10-12T10:00:00Z".parse().unwrap();
    let result = runner
        .run_signal(
            &WorkflowSignal { id: 1, source: "scheduler".into(), content: "tick".into(), env_context: None },
            session,
            null_scope(),
            &CancellationToken::new(),
        )
        .await
        .unwrap();

    // Expected from the script: the first non-replan pass decides; all
    // replans within the budget means exhaustion.
    let reached = max_passes.min(script.iter().position(|k| *k != Pass::Replan).map_or(max_passes, |i| i + 1));
    let last = script[reached - 1];
    let at = format!("maxPasses={max_passes} script={script:?}");
    let ending = match last {
        Pass::Replan => "replan_exhausted",
        Pass::Invalid => "compile",
        Pass::Fail => "execute",
        Pass::Act => "ok",
    };
    let got = match result {
        RunResult::Ok { .. } => "ok",
        RunResult::CompileFailed { .. } => "compile",
        RunResult::ExecuteFailed { .. } => "execute",
        RunResult::ReplanExhausted { .. } => "replan_exhausted",
    };
    prop_assert_eq!(got, ending, "{}: how the signal ends", at);

    let prompts = planner.prompts.lock().unwrap().clone();
    let planner_calls: usize =
        script[..reached].iter().map(|k| if *k == Pass::Invalid { MAX_ATTEMPTS } else { 1 }).sum();
    prop_assert_eq!(prompts.len(), planner_calls, "{}: planner calls", at);

    // Pass p acts with p-1's gathered value seeded as context.v{p-1}.
    let want_calls: Vec<String> = script[..reached]
        .iter()
        .enumerate()
        .filter(|(_, k)| **k != Pass::Invalid)
        .map(|(p, k)| {
            let carried = if p > 0 {
                format!(" with {}", json!({"gathered": format!("gather {}", p - 1)}))
            } else {
                String::new()
            };
            let verb = match k {
                Pass::Act => "act",
                Pass::Fail => "FAIL",
                _ => "gather",
            };
            format!("{verb} {p}{carried}")
        })
        .collect();
    let calls: Vec<String> =
        mcp.calls.lock().unwrap().iter().map(|(_, a)| a["q"].as_str().unwrap().to_owned()).collect();
    prop_assert_eq!(calls, want_calls, "{}: each pass runs its own steps, with the previous pass's data", at);

    for (p, user) in prompts {
        prop_assert_eq!(
            user.contains("<context>"),
            p > 0,
            "{}: only replan passes are told about a previous pass (pass {})",
            at,
            p
        );
        if p == 0 {
            continue;
        }
        prop_assert!(user.contains(&format!("\"v{}\"", p - 1)), "{}: pass {} sees the gathered data", at, p);
        prop_assert!(!user.contains("\"ghost\""), "{}: a name never bound is not carried", at);
        prop_assert!(user.contains(&format!("note from {}", p - 1)), "{}: the note travels", at);
        prop_assert_eq!(
            user.contains("LAST pass"),
            p == max_passes - 1,
            "{}: pass {} is the last one iff it is",
            at,
            p
        );
    }
    Ok(())
}

#[test]
fn ends_the_way_the_script_says_carrying_exactly_the_gathered_context_forward() {
    let mut runner = TestRunner::new(Config { cases: runs(150), ..Config::default() });
    let pass = prop::sample::select(vec![Pass::Act, Pass::Fail, Pass::Replan, Pass::Replan, Pass::Invalid]);
    runner
        .run(&(1usize..=4, prop::collection::vec(pass, 4..=4)), |(max_passes, script)| {
            runtime().block_on(plan_act_replan(max_passes, script))
        })
        .unwrap();
}

// ── improver patches on the workflow path (workflow-patch.test.ts) ──────────

#[derive(Clone, Debug)]
enum Overlay {
    None,
    Written(&'static str),
    Removed(&'static str),
}

fn overlay() -> impl Strategy<Value = Overlay> {
    prop_oneof![
        Just(Overlay::None),
        prop::sample::select(vec![
            "Prefer short items.",
            "  \n",
            "- lesson one\n- lesson two\n\n",
            "Never invent links."
        ])
        .prop_map(Overlay::Written),
        Just(Overlay::Removed("Prefer short items.")),
    ]
}

static PATCH_PLAN: LazyLock<String> = LazyLock::new(|| {
    json!({"version":1,"steps":[{"kind":"llm_compose","preset":"smartest","skill":"writer","input":{},"bind":"d"},{"kind":"terminal"}]})
        .to_string()
});

#[derive(Default)]
struct Systems(Mutex<Vec<(&'static str, String)>>);

#[async_trait]
impl ChatProvider for Systems {
    fn kind(&self) -> ProviderKind {
        ProviderKind::Openai
    }
    async fn complete(&self, p: &mut CompletionParams<'_>) -> anyhow::Result<CompletionResult> {
        let first = &p.messages[0];
        let system = if role(first) == "system" {
            first["content"].as_str().unwrap_or_default().to_owned()
        } else {
            String::new()
        };
        let call = if system.contains("# Planner body") { "planner" } else { "writer" };
        self.0.lock().unwrap().push((call, system));
        let content = if call == "planner" { PATCH_PLAN.as_str() } else { "composed" };
        Ok(CompletionResult { message: assistant(content), finish_reason: Some("stop".into()), usage: None })
    }
}

async fn patched(
    planner_patch: Overlay,
    writer_patch: Overlay,
    witnessed: &Mutex<BTreeSet<String>>,
) -> Result<(), TestCaseError> {
    let dir = tempfile::tempdir().unwrap();
    let defaults_dir = dir.path().join("defaults");
    let live_dir = dir.path().join("live");
    std::fs::create_dir(&defaults_dir).unwrap();
    std::fs::write(defaults_dir.join("planner.md"), "---\ntools: []\n---\n# Planner body\n\nPlan well.\n").unwrap();
    std::fs::write(defaults_dir.join("writer.md"), "---\ntools: []\n---\n# Writer body\n\nWrite well.\n").unwrap();
    // What the improver's savePatch / deletePatch leave on disk.
    for (name, o) in [("planner", &planner_patch), ("writer", &writer_patch)] {
        let target = live_dir.join(format!("{name}.patch.md"));
        match o {
            Overlay::None => {}
            Overlay::Written(text) => {
                std::fs::create_dir_all(&live_dir).unwrap();
                std::fs::write(&target, text).unwrap();
            }
            Overlay::Removed(text) => {
                std::fs::create_dir_all(&live_dir).unwrap();
                std::fs::write(&target, text).unwrap();
                std::fs::remove_file(&target).unwrap();
            }
        }
    }
    let systems = Arc::new(Systems::default());
    let provider: Arc<dyn ChatProvider> = systems.clone();
    let engine = create_engine(EngineDeps {
        providers: Providers {
            openai: provider.clone(),
            gemini: provider.clone(),
            deepseek: provider,
            openrouter: None,
        },
        mcp: ToolMcp::returning(
            vec![ToolDef::new("t", "probe", json!({"type":"object","properties":{},"required":[]}))],
            "ok",
        ),
        presets: default_presets(),
        skills: Vec::new(),
        skill_store: Arc::new(FsSkillStore { live_dir, defaults_dir }),
        memory: Arc::new(PersistLog::default()),
        tracer: Arc::new(NullTracer),
        codex: Arc::new(NoCodex),
        decisions: None,
        workflows: std::sync::Arc::new(agent::flows::MemoryWorkflows::default()),
        schedule: std::sync::Arc::new(agent::schedule::MemorySchedule::default()),
    });
    let runner = create_workflow_runner(engine, vec!["writer".into()]).unwrap();
    let result = runner
        .run_signal(
            &WorkflowSignal { id: 1, source: "scheduler".into(), content: "tick".into(), env_context: None },
            task_context("s"),
            null_scope(),
            &CancellationToken::new(),
        )
        .await
        .unwrap();
    prop_assert!(matches!(result, RunResult::Ok { .. }), "the run succeeds");
    let systems = systems.0.lock().unwrap().clone();
    prop_assert_eq!(systems.iter().map(|s| s.0).collect::<Vec<_>>(), ["planner", "writer"]);

    for (call, system) in systems {
        let o = if call == "planner" { &planner_patch } else { &writer_patch };
        let body = if call == "planner" { "Plan well." } else { "Write well." };
        let in_force = if let Overlay::Written(text) = o { text.trim() } else { "" };
        let at = format!("{call}: {o:?}");
        prop_assert!(system.contains(body), "{}: the body is there", at);
        if in_force.is_empty() {
            if matches!(o, Overlay::Removed(_)) {
                witnessed.lock().unwrap().insert("a removed patch".into());
            }
            prop_assert!(!system.contains("improver-patch"), "{}: no patch, no patch section", at);
            prop_assert!(!system.contains("Prefer short items."), "{}: a removed patch is gone", at);
            continue;
        }
        witnessed.lock().unwrap().insert(format!("a {call} patch in force"));
        // The patch comes last — after the body and, for the planner, after
        // the static tool / skill reference — and only once.
        prop_assert!(system.trim_end().ends_with(in_force), "{}: the patch ends the instructions", at);
        prop_assert!(system.find(body).unwrap() < system.rfind(in_force).unwrap(), "{}: body before patch", at);
        prop_assert_eq!(system.matches(in_force).count(), 1, "{}: the patch once", at);
    }
    Ok(())
}

#[test]
fn each_model_call_is_told_its_skill_body_followed_by_exactly_the_patch_in_force() {
    let witnessed = Mutex::new(BTreeSet::new());
    let mut runner = TestRunner::new(Config { cases: 40, ..Config::default() });
    runner.run(&(overlay(), overlay()), |(p, w)| runtime().block_on(patched(p, w, &witnessed))).unwrap();
    assert_eq!(
        witnessed.into_inner().unwrap().into_iter().collect::<Vec<_>>(),
        ["a planner patch in force", "a removed patch", "a writer patch in force"]
    );
}

// ── the executor under generated plans and faults (workflow-execute.test.ts) ─
//
// Every leaf step carries a marker; the fake engine looks the marker up in a
// generated script (succeed or fail, after a delay, honouring cancellation or
// not) and logs when each call starts and ends. The rules then say, from the
// plan and the script alone, which calls may run, in what order, what the
// result is, what the store holds — and that nothing is still running, or
// still writing, once execute has returned. A future dropped mid-call counts
// as stopped: in Rust there is nothing left to run.

#[derive(Clone, Copy, Debug, PartialEq)]
enum Kind {
    Tool,
    Compose,
    Agent,
    Code,
    Decide,
}

#[derive(Clone, Copy, Debug)]
struct Script {
    fail: bool,
    delay_ms: u64,
    // Whether the call stops when its cancellation fires.
    honours_abort: bool,
}

#[derive(Clone, Debug)]
struct Leaf {
    kind: Kind,
    marker: String,
    bind: Option<String>,
    script: Script,
}

#[derive(Clone, Debug)]
enum Item {
    Leaf(Leaf),
    Parallel(Vec<Leaf>),
    Terminal,
    Replan(Vec<String>),
}

fn script() -> impl Strategy<Value = Script> {
    (prop_oneof![3 => Just(false), 1 => Just(true)], prop::sample::select(vec![0u64, 0, 1, 3, 8]), any::<bool>())
        .prop_map(|(fail, delay_ms, honours_abort)| Script { fail, delay_ms, honours_abort })
}

#[derive(Clone, Debug)]
enum Shape {
    Leaf(Kind, Script, bool),
    Parallel(Vec<(Kind, Script, bool)>),
    Terminal,
    Replan,
}

fn kind() -> impl Strategy<Value = Kind> {
    prop::sample::select(vec![Kind::Tool, Kind::Compose, Kind::Agent, Kind::Code, Kind::Decide])
}

// Markers and binds are unique per plan; a replan carries every bind so far
// plus one that was never bound.
fn items() -> impl Strategy<Value = Vec<Item>> {
    let leaf_shape = || (kind(), script(), any::<bool>());
    let shape = prop_oneof![
        6 => leaf_shape().prop_map(|(k, s, b)| Shape::Leaf(k, s, b)),
        2 => prop::collection::vec(leaf_shape(), 2..=5).prop_map(Shape::Parallel),
        1 => Just(Shape::Terminal),
        1 => Just(Shape::Replan),
    ];
    prop::collection::vec(shape, 1..=6).prop_map(|shapes| {
        let mut n = 0;
        let mut binds: Vec<String> = Vec::new();
        let mut leaf = |(kind, script, bound): (Kind, Script, bool), binds: &mut Vec<String>| {
            n += 1;
            let bind = (bound || kind != Kind::Tool).then(|| format!("b{n}"));
            if let Some(b) = &bind {
                binds.push(b.clone());
            }
            Leaf { kind, marker: format!("m{n}"), bind, script }
        };
        shapes
            .into_iter()
            .map(|s| match s {
                Shape::Leaf(k, sc, b) => Item::Leaf(leaf((k, sc, b), &mut binds)),
                Shape::Parallel(ls) => Item::Parallel(ls.into_iter().map(|l| leaf(l, &mut binds)).collect()),
                Shape::Terminal => Item::Terminal,
                Shape::Replan => Item::Replan([binds.clone(), vec!["never_bound".into()]].concat()),
            })
            .collect()
    })
}

fn to_step(l: &Leaf) -> Value {
    let bind = l.bind.clone().unwrap_or_default();
    match l.kind {
        Kind::Tool => {
            let mut s = json!({"kind":"tool","tool":"t","args":{"q":l.marker}});
            if let Some(b) = &l.bind {
                s["bind"] = json!(b);
            }
            s
        }
        Kind::Compose => json!({"kind":"llm_compose","preset":"base","prompt":l.marker,"input":{},"bind":bind}),
        Kind::Agent => {
            json!({"kind":"llm_agent","preset":"base","skill":"s","prompt":l.marker,"tools":["t"],"maxIterations":2,"bind":bind})
        }
        Kind::Code => json!({"kind":"code_agent","task":l.marker,"bind":bind}),
        Kind::Decide => json!({"kind":"decide","preset":"filter","items":"${list}","prompt":l.marker,"bind":bind}),
    }
}

fn to_workflow(items: &[Item]) -> Workflow {
    let steps: Vec<Value> = items
        .iter()
        .map(|i| match i {
            Item::Leaf(l) => to_step(l),
            Item::Parallel(ls) => json!({"kind":"parallel","steps":ls.iter().map(to_step).collect::<Vec<_>>()}),
            Item::Terminal => json!({"kind":"terminal"}),
            Item::Replan(context) => json!({"kind":"replan","context":context}),
        })
        .collect();
    serde_json::from_value(json!({"version":1,"steps":steps})).unwrap()
}

fn reason(kind: Kind) -> &'static str {
    match kind {
        Kind::Tool => "tool_error",
        Kind::Compose => "llm_error",
        Kind::Agent | Kind::Code => "step_failed",
        Kind::Decide => "decision_error",
    }
}

#[derive(Clone, Debug)]
struct Event {
    at: usize,
    marker: String,
    what: &'static str,
}

#[derive(Default)]
struct Log {
    events: Mutex<Vec<Event>>,
}

impl Log {
    fn log(&self, marker: &str, what: &'static str) {
        let mut events = self.events.lock().unwrap();
        let at = events.len();
        events.push(Event { at, marker: marker.into(), what });
    }
}

static MARKER: LazyLock<regex::Regex> = LazyLock::new(|| regex::Regex::new(r"\bm\d+\b").unwrap());

fn find_marker(text: &str) -> String {
    MARKER
        .find(text)
        .unwrap_or_else(|| panic!("no marker in {}", text.chars().take(200).collect::<String>()))
        .as_str()
        .to_owned()
}

// Logs "aborted" if the call's future is dropped before it ends.
struct Running<'a> {
    log: &'a Log,
    marker: String,
    ended: bool,
}

impl Drop for Running<'_> {
    fn drop(&mut self) {
        if !self.ended {
            self.log.log(&self.marker, "aborted");
        }
    }
}

struct World {
    log: Log,
    scripts: HashMap<String, Script>,
}

impl World {
    // One scripted call: waits its delay (cut short by cancellation when the
    // call honours it), then succeeds or fails.
    async fn perform(&self, marker: String, cancel: &CancellationToken) -> anyhow::Result<()> {
        let s = *self.scripts.get(&marker).unwrap_or_else(|| panic!("no script for {marker}"));
        self.log.log(&marker, "start");
        let mut running = Running { log: &self.log, marker: marker.clone(), ended: false };
        // Like a JS timer: never completes within the poll that started it.
        tokio::task::yield_now().await;
        let wait = tokio::time::sleep(Duration::from_millis(s.delay_ms));
        if s.honours_abort {
            tokio::select! {
                _ = cancel.cancelled() => {
                    running.ended = true;
                    self.log.log(&marker, "aborted");
                    return Err(agent::errors::aborted());
                }
                _ = wait => {}
            }
        } else {
            wait.await;
        }
        running.ended = true;
        if s.fail {
            self.log.log(&marker, "fail");
            anyhow::bail!("scripted failure");
        }
        self.log.log(&marker, "ok");
        Ok(())
    }
}

struct WorldProvider(Arc<World>);

#[async_trait]
impl ChatProvider for WorldProvider {
    fn kind(&self) -> ProviderKind {
        ProviderKind::Openai
    }
    async fn complete(&self, p: &mut CompletionParams<'_>) -> anyhow::Result<CompletionResult> {
        let text: Vec<&str> = p.messages.iter().filter_map(|m| m["content"].as_str()).collect();
        let marker = find_marker(&text.join(" "));
        self.0
            .perform(marker, &p.cancel)
            .await
            .map_err(|e| if agent::errors::is_aborted(&e) { e } else { anyhow::anyhow!("provider: 500") })?;
        Ok(CompletionResult { message: assistant("composed"), finish_reason: Some("stop".into()), usage: None })
    }
}

struct WorldMcp(Arc<World>);

#[async_trait]
impl agent::mcp::Mcp for WorldMcp {
    fn tools(&self) -> Vec<ToolDef> {
        vec![ToolDef::new("t", "t", json!({"type":"object"}))]
    }
    async fn call_tool(&self, _: &str, args: Map<String, Value>, cancel: &CancellationToken) -> anyhow::Result<String> {
        let marker = find_marker(&Value::Object(args).to_string());
        match self.0.perform(marker, cancel).await {
            Ok(()) => Ok("{\"ok\":true}".into()),
            Err(e) if agent::errors::is_aborted(&e) => Err(e),
            Err(_) => Ok("[tool error] boom".into()),
        }
    }
}

struct WorldDecisions(Arc<World>);

#[async_trait]
impl JsonApi for WorldDecisions {
    async fn post(&self, _: &str, body: Value, cancel: &CancellationToken) -> anyhow::Result<Value> {
        let marker = find_marker(body["state"]["rules"].as_str().unwrap());
        self.0
            .perform(marker, cancel)
            .await
            .map_err(|e| if agent::errors::is_aborted(&e) { e } else { anyhow::anyhow!("decisions: 503") })?;
        let answers: Map<String, Value> = body["questions"]
            .as_object()
            .unwrap()
            .keys()
            .map(|k| (k.clone(), json!({"type":"noul","noul":1})))
            .collect();
        Ok(json!({"model":"fake","answers":answers}))
    }
}

struct WorldCodex(Arc<World>);

#[async_trait]
impl Codex for WorldCodex {
    async fn run(&self, request: Value, cancel: &CancellationToken) -> anyhow::Result<String> {
        let marker = find_marker(request["prompt"].as_str().unwrap().rsplit("Task:").next().unwrap());
        self.0
            .perform(marker, cancel)
            .await
            .map_err(|e| if agent::errors::is_aborted(&e) { e } else { anyhow::anyhow!("codex: exit 1") })?;
        Ok(" result ".into())
    }
}

fn ran(item: &Item) -> Vec<Leaf> {
    match item {
        Item::Leaf(l) => vec![l.clone()],
        Item::Parallel(ls) => ls.clone(),
        _ => Vec::new(),
    }
}

async fn faults(items: Vec<Item>, witnessed: &Mutex<BTreeSet<&'static str>>) -> Result<(), TestCaseError> {
    let leaves: Vec<Leaf> = items.iter().flat_map(ran).collect();
    let world =
        Arc::new(World { log: Log::default(), scripts: leaves.iter().map(|l| (l.marker.clone(), l.script)).collect() });
    let provider: Arc<dyn ChatProvider> = Arc::new(WorldProvider(world.clone()));
    let engine = create_engine(EngineDeps {
        providers: Providers {
            openai: provider.clone(),
            gemini: provider.clone(),
            deepseek: provider,
            openrouter: None,
        },
        mcp: Arc::new(WorldMcp(world.clone())),
        presets: default_presets(),
        skills: Vec::new(),
        skill_store: Arc::new(AnySkill),
        memory: Arc::new(PersistLog::default()),
        tracer: Arc::new(NullTracer),
        codex: Arc::new(WorldCodex(world.clone())),
        decisions: Some(decision_client("fake".into(), Arc::new(WorldDecisions(world.clone())))),
        workflows: std::sync::Arc::new(agent::flows::MemoryWorkflows::default()),
        schedule: std::sync::Arc::new(agent::schedule::MemorySchedule::default()),
    });
    let runner = create_workflow_runner(engine.clone(), vec!["s".into()]).unwrap();
    let store = VariableStore::new(json!({"list":[1,2]}).as_object().unwrap().clone());
    let ctx = ExecContext {
        session: task_context("t"),
        store: store.clone(),
        scope: null_scope(),
        signal_label: "scheduler:1".into(),
        cancel: CancellationToken::new(),
    };
    let result = runner.execute(&to_workflow(&items), &ctx).await.unwrap();
    let at_return = world.log.events.lock().unwrap().len();
    let mut keys_at_return: Vec<String> = store.snapshot().keys().cloned().collect();
    keys_at_return.sort();
    let open_loops = engine.loop_ids().await;
    tokio::time::sleep(Duration::from_millis(15)).await;
    let events = world.log.events.lock().unwrap().clone();
    let at = format!("{items:?}");

    // Expected, from the plan and the script alone.
    let stop_at = items.iter().position(|i| matches!(i, Item::Terminal | Item::Replan(_)));
    let reachable: Vec<Item> = match stop_at {
        None => items.clone(),
        Some(s) => items[..=s].to_vec(),
    };
    let fail_at = reachable.iter().position(|i| ran(i).iter().any(|l| l.script.fail));
    let upto = |end: usize| reachable[..end].iter().flat_map(ran).collect::<Vec<_>>();
    let must_run = upto(fail_at.unwrap_or(reachable.len()));
    let may_run: BTreeSet<String> =
        upto(fail_at.map_or(reachable.len(), |f| f + 1)).into_iter().map(|l| l.marker).collect();
    let started: Vec<String> = events.iter().filter(|e| e.what == "start").map(|e| e.marker.clone()).collect();

    prop_assert_eq!(started.iter().collect::<BTreeSet<_>>().len(), started.len(), "{}: no call runs twice", at);
    for m in &started {
        prop_assert!(may_run.contains(m), "{}: {} ran after the plan stopped", at, m);
    }
    for l in &must_run {
        prop_assert!(started.contains(&l.marker), "{}: {} had to run", at, l.marker);
    }

    // Top-level steps are sequential: every call of step k starts after every
    // call of step k-1 has ended.
    let end_of = |m: &str| {
        events
            .iter()
            .find(|e| e.marker == m && ["ok", "fail", "aborted"].contains(&e.what))
            .map_or(usize::MAX, |e| e.at)
    };
    let start_of = |m: &str| events.iter().find(|e| e.marker == m && e.what == "start").map_or(usize::MAX, |e| e.at);
    for k in 1..reachable.len() {
        for now in ran(&reachable[k]) {
            if !started.contains(&now.marker) {
                continue;
            }
            for before in ran(&reachable[k - 1]) {
                prop_assert!(
                    start_of(&now.marker) > end_of(&before.marker),
                    "{}: {} started before {} ended",
                    at,
                    now.marker,
                    before.marker
                );
            }
        }
    }

    // Up to four children of a parallel run at once: all of them start
    // before the first one ends.
    for item in &reachable[..fail_at.map_or(reachable.len(), |f| f + 1)] {
        let Item::Parallel(ls) = item else { continue };
        if ls.len() > 4 {
            continue;
        }
        let first_end = ls.iter().map(|l| end_of(&l.marker)).min().unwrap();
        for l in ls {
            prop_assert!(start_of(&l.marker) < first_end, "{}: {} waited for a sibling", at, l.marker);
        }
    }

    match fail_at {
        None => {
            let ExecResult::Ok { replan, .. } = &result else {
                return Err(TestCaseError::fail(format!("{at}: nothing failed, so the run succeeds")));
            };
            let binds: Vec<String> = must_run.iter().filter_map(|l| l.bind.clone()).collect();
            if let Some(Item::Replan(_)) = reachable.last() {
                witnessed.lock().unwrap().insert("replan");
                let mut carried: Vec<String> =
                    replan.as_ref().map(|r| r.context.keys().cloned().collect()).unwrap_or_default();
                carried.sort();
                let mut want = binds.clone();
                want.sort();
                prop_assert_eq!(carried, want, "{}: replan carries exactly the bound names", at);
            } else {
                prop_assert!(replan.is_none(), "{}: no replan without a replan step", at);
            }
            let mut want = [vec!["list".to_owned()], binds].concat();
            want.sort();
            prop_assert_eq!(&keys_at_return, &want, "{}: the store holds every bind and nothing else", at);
        }
        Some(f) => {
            witnessed.lock().unwrap().insert(if matches!(reachable[f], Item::Parallel(_)) {
                "a parallel fails"
            } else {
                "a step fails"
            });
            let ExecResult::Failed { step_index, reason: got, .. } = &result else {
                return Err(TestCaseError::fail(format!("{at}: a failing call fails the run")));
            };
            prop_assert_eq!(*step_index, f, "{}: the first failing step is reported", at);
            let failing: Vec<&str> =
                ran(&reachable[f]).iter().filter(|l| l.script.fail).map(|l| reason(l.kind)).collect();
            prop_assert!(failing.contains(got), "{}: reason {}", at, got);
            let before: Vec<String> = upto(f).into_iter().filter_map(|l| l.bind).collect();
            let same_step: Vec<String> = ran(&reachable[f]).into_iter().filter_map(|l| l.bind).collect();
            for b in &before {
                prop_assert!(keys_at_return.contains(b), "{}: earlier binds stay", at);
            }
            for k in &keys_at_return {
                prop_assert!(
                    k == "list" || before.contains(k) || same_step.contains(k),
                    "{}: {} bound by a step that never ran",
                    at,
                    k
                );
            }
        }
    }

    // Nothing outlives execute: no store write after it returned, no call
    // still running, every agent loop ended.
    let mut keys_now: Vec<String> = store.snapshot().keys().cloned().collect();
    keys_now.sort();
    prop_assert_eq!(keys_now, keys_at_return, "{}: the store changed after execute returned", at);
    let still_running: Vec<&String> = started.iter().filter(|m| end_of(m) >= at_return).collect();
    prop_assert!(still_running.is_empty(), "{}: calls still running after execute returned: {:?}", at, still_running);
    prop_assert!(open_loops.is_empty(), "{}: every agent loop ended before execute returned: {:?}", at, open_loops);
    if events.iter().any(|e| e.what == "aborted") {
        witnessed.lock().unwrap().insert("a sibling is interrupted");
    }
    Ok(())
}

#[test]
fn runs_what_the_plan_allows_stops_where_it_must_and_leaves_nothing_running() {
    let witnessed = Mutex::new(BTreeSet::new());
    let mut runner = TestRunner::new(Config { cases: runs(150), ..Config::default() });
    runner.run(&items(), |items| runtime().block_on(faults(items, &witnessed))).unwrap();
    assert_eq!(
        witnessed.into_inner().unwrap().into_iter().collect::<Vec<_>>(),
        ["a parallel fails", "a sibling is interrupted", "a step fails", "replan"]
    );
}

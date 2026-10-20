// The agent's schedule, judged from the outside: tasks are created, listed
// and cancelled through the tools, the user's zone changes as MCP's setting
// would, and the tick runs on a controlled clock. Every step is checked
// against the rows agent.db holds before and after, and every fire against
// what the cron means on the user's wall clock (support/wall_cron.rs — not
// croner). A port of crates/mcp/tests/scheduler.rs and the scenarios of
// crates/mcp/specs/scheduler.feature from when MCP owned the schedule.
//
// Rules: outcome (from request + pre-state), effect (the reply and the
// changed rows), frame (what must not change).
//
// PROPERTY_RUNS raises the number of generated sequences.

#[path = "support/wall_cron.rs"]
mod wall_cron;

use agent::{
    db::AgentDb,
    flows::{MemoryWorkflows, WorkflowStore},
    mcp::Mcp,
    providers::ToolDef,
    schedule::{self, FIRST_FIRE_ID, Fire, FireSink, Fired, MemorySchedule, NewTask, ScheduleStore, TaskRow},
};
use async_trait::async_trait;
use chrono::{DateTime, TimeZone, Utc};
use chrono_tz::Tz;
use proptest::prelude::*;
use proptest::test_runner::{Config, TestRunner};
use serde_json::{Map, Value, json};
use std::collections::{BTreeMap, BTreeSet, HashSet};
use std::sync::Mutex;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use tokio_util::sync::CancellationToken;
use wall_cron::{days_exist, first_owed, legitimate_slot, parse_wall_cron};

const MINUTE: i64 = 60_000;
const HOUR: i64 = 60 * MINUTE;

fn property_runs(default: u32) -> u32 {
    std::env::var("PROPERTY_RUNS").ok().and_then(|v| v.parse().ok()).unwrap_or(default)
}

fn ms(s: &str) -> i64 {
    DateTime::parse_from_rfc3339(s).unwrap().timestamp_millis()
}

fn at(ms: i64) -> DateTime<Utc> {
    Utc.timestamp_millis_opt(ms).unwrap()
}

fn iso(ms: i64) -> String {
    at(ms).to_rfc3339_opts(chrono::SecondsFormat::Millis, true)
}

fn tz_of(name: &str) -> Tz {
    name.parse().expect("a known zone")
}

// ── the world ───────────────────────────────────────────────────────────────

// What a fire carried, as the runner received it.
#[derive(Default)]
struct Recorder {
    fires: Mutex<Vec<Fire>>,
    // Answer NotStarted, as a supervisor that is stopping does.
    refuse: AtomicBool,
}

#[async_trait]
impl FireSink for Recorder {
    async fn fire(&self, fire: Fire) -> Fired {
        // Let overlapping ticks interleave here, where a real fire awaits.
        tokio::task::yield_now().await;
        if self.refuse.load(Ordering::SeqCst) {
            return Fired::NotStarted;
        }
        self.fires.lock().unwrap().push(fire);
        Fired::Started
    }
}

#[derive(Debug, Clone, PartialEq)]
struct Task {
    id: i64,
    cron: String,
    recurring: bool,
    prompt: String,
    source: Option<String>,
    workflow: Option<String>,
    last_run_at: Option<i64>, // unix seconds of the slot last fired for
    created_ms: i64,
}

fn tasks(db: &AgentDb) -> BTreeMap<i64, Task> {
    let conn = db.conn();
    let mut statement = conn
        .prepare(
            "SELECT id, cron_expr, recurring, prompt, source, workflow, last_run_at, created_ms FROM scheduled_tasks",
        )
        .unwrap();
    statement
        .query_map([], |r| {
            Ok(Task {
                id: r.get(0)?,
                cron: r.get(1)?,
                recurring: r.get::<_, i64>(2)? == 1,
                prompt: r.get(3)?,
                source: r.get(4)?,
                workflow: r.get(5)?,
                last_run_at: r.get(6)?,
                created_ms: r.get(7)?,
            })
        })
        .unwrap()
        .map(|t| t.map(|t| (t.id, t)))
        .collect::<rusqlite::Result<_>>()
        .unwrap()
}

fn active(t: &Task) -> bool {
    t.recurring || t.last_run_at.is_none()
}

// What the next fire is counted from: the slot last fired for, or creation.
fn anchor_ms(t: &Task) -> i64 {
    t.last_run_at.map_or(t.created_ms, |s| s * 1000)
}

// A fire's text, read back the way a skill parses its header.
#[derive(Debug, Clone)]
struct Header {
    task_id: i64,
    cron: String,
    slot_ms: i64,
    now_iso: String,
    previous: String,
    recurring: String,
    body: String,
}

fn parse_header(fire: &Fire) -> Header {
    let lines: Vec<&str> = fire.content.split('\n').collect();
    let header = |i: usize, label: &str| -> String {
        let line = lines.get(i).copied().unwrap_or_default();
        line.strip_prefix(label)
            .unwrap_or_else(|| panic!("fire {}: line {i} is not {label:?}…: {line:?}", fire.id))
            .to_owned()
    };
    let id = regex::Regex::new(r"^Scheduled task #(\d+) fired\.$").unwrap();
    let caps = id.captures(lines.first().copied().unwrap_or_default()).filter(|_| lines.get(6) == Some(&""));
    let caps = caps.unwrap_or_else(|| panic!("fire {}: not a scheduler fire: {:.200}", fire.id, fire.content));
    Header {
        task_id: caps[1].parse().unwrap(),
        cron: header(1, "Cron: "),
        slot_ms: DateTime::parse_from_rfc3339(&header(2, "Slot: ")).expect("a slot").timestamp_millis(),
        now_iso: header(3, "Now: "),
        previous: header(4, "Previous fire: "),
        recurring: header(5, "Recurring: "),
        body: lines[7..].join("\n"),
    }
}

// The one saved workflow with an active version.
fn workflows() -> MemoryWorkflows {
    let store = MemoryWorkflows::default();
    let saved = store.save("digest", "definition", None).unwrap();
    store.activate("digest", saved.version).unwrap();
    store
}

async fn call(
    db: &AgentDb,
    workflows: &dyn WorkflowStore,
    tz: &str,
    name: &str,
    args: Value,
    now: i64,
) -> anyhow::Result<Value> {
    let args = args.as_object().cloned().unwrap_or_default();
    let raw = schedule::tool(db, workflows, tz, name, &args, at(now)).await?;
    Ok(serde_json::from_str(&raw).unwrap())
}

async fn insert(db: &dyn ScheduleStore, cron: &str, recurring: bool, prompt: &str, created: i64) -> TaskRow {
    let task = NewTask {
        cron_expr: cron.into(),
        recurring,
        prompt: prompt.into(),
        source: None,
        workflow: None,
        last_run_at: None,
        created: at(created),
    };
    db.insert(&task).await.unwrap()
}

fn store() -> (tempfile::TempDir, AgentDb) {
    let dir = tempfile::tempdir().unwrap();
    let db = AgentDb::open(dir.path().join("agent.db")).unwrap();
    db.conn().execute("DELETE FROM scheduled_tasks", []).unwrap();
    (dir, db)
}

// ── operations ──────────────────────────────────────────────────────────────

// Not Australia/Lord_Howe: its 30-minute DST shifts were mishandled by the
// TS server's cron library (see F5 in the ledger).
const ZONES: [&str; 4] = ["UTC", "Europe/Kiev", "America/New_York", "Asia/Kolkata"];

// Realistic agent-written crons, plus a few the tool must refuse.
fn cron() -> impl Strategy<Value = String> {
    (
        proptest::sample::select(vec!["0", "15", "30", "59", "*/15", "*/5", "*", "0,30"]),
        proptest::sample::select(vec!["*", "0", "2", "3", "9", "23", "*/6", "9,18", "1-3"]),
        proptest::sample::select(vec!["*", "*", "*", "1", "15", "29", "31"]),
        proptest::sample::select(vec!["*", "*", "*", "3", "10", "11"]),
        proptest::sample::select(vec!["*", "*", "*", "1-5", "0", "6", "7"]),
    )
        .prop_map(|(m, h, dom, mon, dow)| format!("{m} {h} {dom} {mon} {dow}"))
}

fn bad_cron() -> impl Strategy<Value = String> {
    proptest::sample::select(vec![
        "every day at nine",
        "61 * * * *",
        "0 25 * * *",
        "* * * *",
        "30 9 * * * *",
        "0 9 * * * * * *",
        "@daily",
    ])
    .prop_map(str::to_owned)
}

// Clock starts near the year's DST transitions (Kiev, New York) or anywhere.
fn start() -> impl Strategy<Value = i64> {
    prop_oneof![
        proptest::sample::select(vec![
            ms("2026-03-07T20:00:00Z"), // New York springs forward Mar 8
            ms("2026-03-28T20:00:00Z"), // Kiev springs forward Mar 29
            ms("2026-10-24T20:00:00Z"), // Kiev falls back Oct 25
            ms("2026-10-31T20:00:00Z"), // New York falls back Nov 1
        ]),
        ms("2026-01-01T00:00:00Z")..=ms("2026-12-31T00:00:00Z"),
    ]
}

#[derive(Debug, Clone)]
enum Op {
    Schedule { cron: String, recurring: bool, prompt: String, workflow: Option<&'static str> },
    Seed { cron: String, source: &'static str, prompt: String },
    Cancel { pick: usize, exists: bool },
    Timezone { tz: &'static str },
    List,
    Advance { ms: i64 },
    AdvanceToFire { pick: usize, offset_ms: i64 },
    Tick,
}

fn prompt() -> impl Strategy<Value = String> {
    "\\PC{1,40}"
}

fn nat() -> impl Strategy<Value = usize> {
    0usize..(i32::MAX as usize)
}

fn op() -> impl Strategy<Value = Op> {
    prop_oneof![
        4 => (
            prop_oneof![9 => cron(), 1 => bad_cron()],
            any::<bool>(),
            prompt(),
            prop_oneof![6 => Just(None), 3 => Just(Some("digest")), 1 => Just(Some("missing"))],
        )
            .prop_map(|(cron, recurring, prompt, workflow)| Op::Schedule { cron, recurring, prompt, workflow }),
        3 => (cron(), proptest::sample::select(vec!["dreaming", "news-digest"]), prompt())
            .prop_map(|(cron, source, prompt)| Op::Seed { cron, source, prompt }),
        2 => (nat(), any::<bool>()).prop_map(|(pick, exists)| Op::Cancel { pick, exists }),
        1 => proptest::sample::select(&ZONES[..]).prop_map(|tz| Op::Timezone { tz }),
        1 => Just(Op::List),
        3 => prop_oneof![
            proptest::sample::select(vec![1000, 29_000, 30_000, MINUTE, 15 * MINUTE, HOUR, 6 * HOUR, 24 * HOUR]),
            1..=26 * HOUR,
        ].prop_map(|ms| Op::Advance { ms }),
        3 => (nat(), proptest::sample::select(vec![-1000i64, 0, 1000]))
            .prop_map(|(pick, offset_ms)| Op::AdvanceToFire { pick, offset_ms }),
        6 => Just(Op::Tick),
    ]
}

// ── rules ───────────────────────────────────────────────────────────────────

#[derive(Default)]
struct Violations(Vec<String>);

impl Violations {
    fn check(&mut self, rule: &str, holds: bool, detail: impl FnOnce() -> String) {
        if !holds {
            self.0.push(format!("{rule}: {}", detail()));
        }
    }
}

fn tasks_frame(
    v: &mut Violations,
    rule: &str,
    before: &BTreeMap<i64, Task>,
    after: &BTreeMap<i64, Task>,
    except: &HashSet<i64>,
) {
    for (id, t) in before {
        if except.contains(id) {
            continue;
        }
        v.check(rule, after.get(id) == Some(t), || format!("{id}: {t:?} → {:?}", after.get(id)));
    }
}

const MAX_SCAN: i64 = 40 * 24 * HOUR;

// Upcoming fires a tool reports: legitimate, increasing, and nothing owed
// skipped between them (from `from_ms`, the time of the call).
fn check_upcoming(v: &mut Violations, rule: &str, cron_text: &str, tz: Tz, from_ms: i64, upcoming: &[Value]) {
    let c = parse_wall_cron(cron_text).expect("a valid cron");
    let mut prev = from_ms;
    for u in upcoming {
        let text = u.as_str().expect("an ISO string");
        let at = DateTime::parse_from_rfc3339(text).expect("ISO").timestamp_millis();
        v.check(&format!("{rule}: a legitimate fire time"), legitimate_slot(&c, tz, at), || {
            format!("{cron_text} {tz} {text}")
        });
        v.check(&format!("{rule}: increasing and in the future"), at > prev, || format!("{text} after {}", iso(prev)));
        if at - prev < MAX_SCAN {
            let owed = first_owed(&c, tz, prev, at - 1);
            v.check(&format!("{rule}: no owed fire skipped"), owed.is_none(), || {
                format!("{cron_text} {tz} {text} skipped {:?}", owed.map(iso))
            });
        }
        prev = at;
    }
}

fn keys(v: &Value) -> BTreeSet<String> {
    v.as_object().map(|m| m.keys().cloned().collect()).unwrap_or_default()
}

fn set(keys: &[&str]) -> BTreeSet<String> {
    keys.iter().map(|k| (*k).to_owned()).collect()
}

const TASK_KEYS: [&str; 8] =
    ["id", "cron_expr", "recurring", "prompt", "source", "workflow", "last_run_at", "created_at"];

static WITNESSED: Mutex<BTreeSet<&'static str>> = Mutex::new(BTreeSet::new());

fn witness(w: &'static str) {
    WITNESSED.lock().unwrap().insert(w);
}

struct World {
    db: AgentDb,
    workflows: MemoryWorkflows,
    recorder: Recorder,
    zone: &'static str,
    clock: i64,
}

async fn step(w: &mut World, o: &Op) -> Vec<String> {
    let mut v = Violations::default();
    let before = tasks(&w.db);
    let tz = tz_of(w.zone);
    let none = HashSet::new();
    match o {
        Op::Advance { ms } => w.clock += ms,
        Op::AdvanceToFire { pick, offset_ms } => {
            let candidates: Vec<&Task> = before.values().filter(|t| active(t)).collect();
            if let Some(t) = candidates.get(pick % candidates.len().max(1)) {
                let c = parse_wall_cron(&t.cron).expect("stored crons are valid");
                if let Some(owed) = first_owed(&c, tz, anchor_ms(t).max(w.clock - MINUTE), w.clock + 3 * 24 * HOUR) {
                    w.clock = owed + offset_ms;
                }
            }
        }
        Op::Schedule { cron, recurring, prompt, workflow } => {
            let mut args = json!({ "cron_expr": cron, "recurring": recurring, "prompt": prompt });
            if let Some(workflow) = workflow {
                args["workflow"] = json!(workflow);
            }
            let reply = call(&w.db, &w.workflows, w.zone, "schedule_task", args, w.clock).await.unwrap();
            let after = tasks(&w.db);
            tasks_frame(&mut v, "schedule frame: existing tasks untouched", &before, &after, &none);
            if reply["ok"] == false {
                v.check("schedule: a refusal is {ok, error}", keys(&reply) == set(&["ok", "error"]), || {
                    reply.to_string()
                });
                v.check("schedule frame: a refusal adds no task", after.len() == before.len(), || cron.clone());
            } else {
                v.check(
                    "schedule: reply shape",
                    keys(&reply) == set(&["ok", "task", "timezone", "upcoming_fires"])
                        && keys(&reply["task"]) == set(&TASK_KEYS),
                    || reply.to_string(),
                );
            }
            // Valid: five fields whose days exist in some selected month. Days
            // that don't exist but a restricted weekday could still match —
            // either answer is accepted.
            let valid = match parse_wall_cron(cron) {
                Ok(c) if days_exist(&c) => Some(true),
                Ok(c) if c.dow_restricted => None,
                _ => Some(false),
            };
            let Some(valid) = valid else { return v.0 };
            if !valid {
                witness("an invalid cron is refused");
                v.check("schedule: an invalid cron is refused", reply["ok"] == false, || format!("{cron:?} {reply}"));
                return v.0;
            }
            if *workflow == Some("missing") {
                witness("a workflow without an active version is refused");
                v.check("schedule: a workflow needs an active version", reply["ok"] == false, || reply.to_string());
                return v.0;
            }
            v.check("schedule: a valid cron is accepted", reply["ok"] == true, || format!("{cron:?} {reply}"));
            if reply["ok"] != true {
                return v.0;
            }
            let id = reply["task"]["id"].as_i64().unwrap();
            let row = after.get(&id);
            v.check(
                "schedule: exactly one task is added",
                after.len() == before.len() + 1 && !before.contains_key(&id),
                || id.to_string(),
            );
            v.check(
                "schedule: the task holds what was asked",
                row.is_some_and(|r| {
                    r.cron == *cron
                        && r.recurring == *recurring
                        && r.prompt == *prompt
                        && r.source.is_none()
                        && r.workflow.as_deref() == *workflow
                        && r.last_run_at.is_none()
                        && r.created_ms == w.clock
                }),
                || format!("{row:?}"),
            );
            v.check(
                "schedule: the reply echoes the stored task",
                reply["task"]["cron_expr"] == json!(cron)
                    && reply["task"]["prompt"] == json!(prompt)
                    && reply["task"]["recurring"] == json!(i64::from(*recurring))
                    && reply["task"]["workflow"] == json!(workflow),
                || reply["task"].to_string(),
            );
            v.check("schedule: timezone is the user's", reply["timezone"] == json!(w.zone), || {
                format!("{} vs {}", reply["timezone"], w.zone)
            });
            let upcoming = reply["upcoming_fires"].as_array().cloned().unwrap_or_default();
            v.check(
                "schedule: 3 upcoming fires for recurring, 1 for one-shot",
                upcoming.len() == if *recurring { 3 } else { 1 },
                || format!("{upcoming:?}"),
            );
            check_upcoming(&mut v, "schedule upcoming", cron, tz, w.clock, &upcoming);
        }
        Op::Seed { cron, source, prompt } => {
            let task = NewTask {
                cron_expr: cron.clone(),
                recurring: true,
                prompt: prompt.clone(),
                source: Some((*source).into()),
                workflow: None,
                last_run_at: None,
                created: at(w.clock),
            };
            w.db.insert(&task).await.unwrap();
        }
        Op::Cancel { pick, exists } => {
            let ids: Vec<i64> = before.keys().copied().collect();
            let id = if *exists && !ids.is_empty() {
                ids[pick % ids.len()]
            } else {
                ids.iter().copied().max().unwrap_or(0).max(0) + 1 + (pick % 5) as i64
            };
            let reply =
                call(&w.db, &w.workflows, w.zone, "cancel_scheduled_task", json!({ "id": id }), w.clock).await.unwrap();
            let after = tasks(&w.db);
            let existed = before.contains_key(&id);
            v.check("cancel: reply shape", keys(&reply) == set(&["ok", "id"]), || reply.to_string());
            v.check("cancel: ok exactly when the task existed", reply["ok"] == existed && reply["id"] == id, || {
                format!("{id} {existed} {reply}")
            });
            v.check("cancel: the task is gone", !after.contains_key(&id), || id.to_string());
            v.check("cancel: nothing else is removed", after.len() == before.len() - usize::from(existed), || {
                id.to_string()
            });
            tasks_frame(&mut v, "cancel frame: other tasks untouched", &before, &after, &HashSet::from([id]));
            if existed {
                witness("a task is cancelled");
            }
        }
        Op::Timezone { tz } => {
            // The zone is MCP's setting; the schedule reads it on every tick.
            w.zone = tz;
            tasks_frame(&mut v, "timezone frame: tasks untouched", &before, &tasks(&w.db), &none);
        }
        Op::List => {
            let reply = call(&w.db, &w.workflows, w.zone, "list_scheduled_tasks", json!({}), w.clock).await.unwrap();
            tasks_frame(&mut v, "list frame: listing changes nothing", &before, &tasks(&w.db), &none);
            v.check("list: reply shape", keys(&reply) == set(&["timezone", "count", "tasks"]), || reply.to_string());
            let mut expected: Vec<i64> = before.values().filter(|t| active(t)).map(|t| t.id).collect();
            expected.sort();
            let listed = reply["tasks"].as_array().cloned().unwrap_or_default();
            let ids: Vec<i64> = listed.iter().map(|t| t["id"].as_i64().unwrap()).collect();
            v.check("list: exactly the tasks that may still fire", ids == expected, || {
                format!("{ids:?} vs {expected:?}")
            });
            v.check("list: count matches", reply["count"] == json!(listed.len()), || reply["count"].to_string());
            v.check("list: timezone is the user's", reply["timezone"] == json!(w.zone), || {
                reply["timezone"].to_string()
            });
            let listed_keys = set(&[&TASK_KEYS[..], &["last_run_at_iso", "upcoming_fires"]].concat());
            for t in &listed {
                v.check("list: row shape", keys(t) == listed_keys, || t.to_string());
                let s = before.get(&t["id"].as_i64().unwrap());
                v.check(
                    "list: each row as stored",
                    s.is_some_and(|s| {
                        t["cron_expr"] == json!(s.cron)
                            && t["prompt"] == json!(s.prompt)
                            && t["recurring"] == json!(s.recurring)
                            && t["source"] == json!(s.source)
                            && t["workflow"] == json!(s.workflow)
                            && t["last_run_at"] == json!(s.last_run_at)
                    }),
                    || format!("{t} vs {s:?}"),
                );
                let want_iso = t["last_run_at"].as_i64().map(|s| iso(s * 1000));
                v.check("list: last_run_at_iso is last_run_at", t["last_run_at_iso"] == json!(want_iso), || {
                    t.to_string()
                });
                let upcoming = t["upcoming_fires"].as_array().cloned().unwrap_or_default();
                check_upcoming(&mut v, "list upcoming", t["cron_expr"].as_str().unwrap(), tz, w.clock, &upcoming);
            }
        }
        Op::Tick => {
            let now = w.clock;
            let seen = w.recorder.fires.lock().unwrap().len();
            let n = schedule::tick(&w.db, &w.recorder, tz, at(now)).await.unwrap();
            let after = tasks(&w.db);
            let fires: Vec<Fire> = w.recorder.fires.lock().unwrap()[seen..].to_vec();
            v.check("tick: returns how many fired", n == fires.len(), || format!("{n} vs {}", fires.len()));
            let mut fired_tasks = HashSet::new();
            for f in &fires {
                let h = parse_header(f);
                v.check("tick: the fire names its task", h.task_id == f.task_id, || format!("{h:?} {f:?}"));
                v.check("tick: fire ids stay above MCP's signal ids", f.id > FIRST_FIRE_ID, || f.id.to_string());
                let t = before.get(&f.task_id);
                v.check("tick: fires only an existing, active task", t.is_some_and(active), || format!("{f:?} {t:?}"));
                let Some(t) = t else { continue };
                v.check("tick: at most one fire per task per tick", !fired_tasks.contains(&t.id), || t.id.to_string());
                fired_tasks.insert(t.id);
                let c = parse_wall_cron(&t.cron).unwrap();
                v.check(
                    "tick: source is the task's, or scheduler",
                    f.source == t.source.clone().unwrap_or_else(|| "scheduler".into()),
                    || format!("{} vs {:?}", f.source, t.source),
                );
                v.check("tick: the fire runs the task's workflow", f.workflow == t.workflow, || format!("{f:?}"));
                v.check(
                    "tick: the fire carries the task",
                    h.cron == t.cron
                        && h.body == t.prompt
                        && h.recurring == if t.recurring { "yes" } else { "no (one-shot)" },
                    || format!("{h:?}"),
                );
                v.check("tick: the header's slot is the fire's", h.slot_ms == f.slot.timestamp_millis(), || {
                    format!("{h:?} {f:?}")
                });
                v.check("tick: Now is the tick's time", h.now_iso == iso(now), || h.now_iso.clone());
                let want_previous = t.last_run_at.map_or("never (this is the first run)".to_owned(), |s| iso(s * 1000));
                v.check("tick: Previous fire is the slot last fired for", h.previous == want_previous, || {
                    format!("{} vs {want_previous}", h.previous)
                });
                v.check("tick: never early", h.slot_ms <= now, || format!("{} > {}", iso(h.slot_ms), iso(now)));
                v.check("tick: only after the anchor", h.slot_ms > anchor_ms(t), || {
                    format!("{} vs {}", iso(h.slot_ms), iso(anchor_ms(t)))
                });
                v.check("tick: the slot is a legitimate fire time", legitimate_slot(&c, tz, h.slot_ms), || {
                    format!("{} {tz} {}", t.cron, iso(h.slot_ms))
                });
                let skipped = first_owed(&c, tz, anchor_ms(t), h.slot_ms - 1);
                v.check("tick: no owed fire is skipped", skipped.is_none(), || {
                    format!(
                        "{} {tz} anchor {} slot {} skipped {:?}",
                        t.cron,
                        iso(anchor_ms(t)),
                        iso(h.slot_ms),
                        skipped.map(iso)
                    )
                });
                let a = after.get(&t.id);
                v.check(
                    "tick: last_run_at becomes the slot",
                    a.and_then(|a| a.last_run_at) == Some(h.slot_ms / 1000),
                    || format!("{a:?} vs {}", h.slot_ms / 1000),
                );
                if !t.recurring {
                    witness("a one-shot fires");
                }
                if t.last_run_at.is_some() && t.recurring {
                    witness("a recurring task fires again");
                }
                if now - h.slot_ms > HOUR {
                    witness("a late tick catches up");
                }
                if t.source.is_some() {
                    witness("a seeded task fires under its own source");
                }
                if t.workflow.is_some() {
                    witness("a fire runs a saved workflow");
                }
                if w.zone != "UTC" {
                    witness("a fire in a non-UTC zone");
                }
            }
            // Progress: every active task with an owed fire by now fires.
            for t in before.values() {
                if !active(t) || fired_tasks.contains(&t.id) {
                    continue;
                }
                let owed = first_owed(&parse_wall_cron(&t.cron).unwrap(), tz, anchor_ms(t), now);
                v.check("tick: a task with an owed fire fires", owed.is_none(), || {
                    format!(
                        "task {} {} {tz} anchor {} owed {:?} now {}",
                        t.id,
                        t.cron,
                        iso(anchor_ms(t)),
                        owed.map(iso),
                        iso(now)
                    )
                });
            }
            if fires.is_empty() && before.values().any(active) {
                witness("a tick with nothing due");
            }
            tasks_frame(&mut v, "tick frame: tasks that didn't fire are untouched", &before, &after, &fired_tasks);
            v.check("tick frame: no task added or removed", after.len() == before.len(), || {
                format!("{} → {}", before.len(), after.len())
            });
        }
    }
    v.0
}

fn runtime() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap()
}

// ── suites ──────────────────────────────────────────────────────────────────

#[test]
fn keeps_every_rule_across_generated_schedules_cancels_zone_changes_and_ticks() {
    let rt = runtime();
    TestRunner::new(Config::with_cases(property_runs(60)))
        .run(
            &(start(), proptest::sample::select(&ZONES[..]), proptest::collection::vec(op(), 1..=40)),
            |(t0, zone, ops)| {
                rt.block_on(async {
                    let (_dir, db) = store();
                    let mut w = World { db, workflows: workflows(), recorder: Recorder::default(), zone, clock: t0 };
                    for (i, o) in ops.iter().enumerate() {
                        let violations = step(&mut w, o).await;
                        assert!(
                            violations.is_empty(),
                            "step {i} at {} {o:?}\n  {}",
                            iso(w.clock),
                            violations.join("\n  ")
                        );
                    }
                });
                Ok(())
            },
        )
        .unwrap();
    assert_eq!(
        WITNESSED.lock().unwrap().iter().copied().collect::<Vec<_>>(),
        [
            "a fire in a non-UTC zone",
            "a fire runs a saved workflow",
            "a late tick catches up",
            "a one-shot fires",
            "a recurring task fires again",
            "a seeded task fires under its own source",
            "a task is cancelled",
            "a tick with nothing due",
            "a workflow without an active version is refused",
            "an invalid cron is refused",
        ]
    );
}

// A daily reminder fires exactly once on every local day across both DST
// transitions, ticking every 10 minutes like a slightly slow tick.
fn daily_across_dst(zone: &str, cron_text: &str, from: &str) {
    runtime().block_on(async {
        let (_dir, db) = store();
        let tz = tz_of(zone);
        let start = ms(from);
        insert(&db, cron_text, true, "pills", start).await;
        let recorder = Recorder::default();
        let mut now = start;
        while now <= start + 5 * 24 * HOUR {
            schedule::tick(&db, &recorder, tz, at(now)).await.unwrap();
            now += 10 * MINUTE;
        }
        let days: Vec<chrono::NaiveDate> =
            recorder.fires.lock().unwrap().iter().map(|f| f.slot.with_timezone(&tz).date_naive()).collect();
        assert!(days.len() >= 4, "fired on {days:?}");
        let gaps: Vec<i64> = days.windows(2).map(|w| (w[1] - w[0]).num_days()).collect();
        assert!(gaps.iter().all(|g| *g == 1), "fired on {days:?}");
    });
}

#[test]
fn a_daily_task_fires_once_per_local_day_across_dst_kiev_9() {
    daily_across_dst("Europe/Kiev", "0 9 * * *", "2026-03-27T00:00:00Z");
}

#[test]
fn a_daily_task_fires_once_per_local_day_across_dst_new_york_9() {
    daily_across_dst("America/New_York", "0 9 * * *", "2026-03-06T00:00:00Z");
}

#[test]
fn a_daily_task_fires_once_per_local_day_across_dst_kiev_8_autumn() {
    daily_across_dst("Europe/Kiev", "0 8 * * *", "2026-10-23T00:00:00Z");
}

#[test]
fn a_daily_task_fires_once_per_local_day_across_dst_kiev_3_30_spring() {
    daily_across_dst("Europe/Kiev", "30 3 * * *", "2026-03-27T00:00:00Z");
}

#[test]
fn a_daily_task_fires_once_per_local_day_across_dst_kiev_3_30_autumn() {
    daily_across_dst("Europe/Kiev", "30 3 * * *", "2026-10-23T00:00:00Z");
}

#[test]
fn a_daily_task_fires_once_per_local_day_across_dst_new_york_1_30_autumn() {
    daily_across_dst("America/New_York", "30 1 * * *", "2026-10-30T00:00:00Z");
}

#[test]
fn a_daily_task_fires_once_per_local_day_across_dst_new_york_2_30_spring() {
    daily_across_dst("America/New_York", "30 2 * * *", "2026-03-06T00:00:00Z");
}

#[test]
fn a_daily_task_fires_once_per_local_day_across_dst_lord_howe_1_45() {
    daily_across_dst("Australia/Lord_Howe", "45 1 * * *", "2026-04-03T00:00:00Z");
}

// A time inside Lord Howe's 30-minute spring-forward gap. Skipped in the TS
// suite, whose cron library skips the day; next_slot shifts it past the gap
// like any other.
#[test]
fn a_daily_task_inside_a_30_minute_dst_gap_still_fires_that_day() {
    daily_across_dst("Australia/Lord_Howe", "15 2 * * *", "2026-10-02T00:00:00Z");
}

// ── the scenarios of scheduler.feature ──────────────────────────────────────

// One task ticked once: the slots that fired.
async fn fired_once(zone: &str, cron: &str, recurring: bool, created: &str, ticks: &[&str]) -> Vec<String> {
    let (_dir, db) = store();
    insert(&db, cron, recurring, "tick", ms(created)).await;
    let recorder = Recorder::default();
    for now in ticks {
        schedule::tick(&db, &recorder, tz_of(zone), at(ms(now))).await.unwrap();
    }
    let fires = recorder.fires.lock().unwrap().clone();
    assert!(fires.iter().all(|f| f.source == "scheduler"), "{fires:?}");
    fires.iter().map(|f| iso(f.slot.timestamp_millis())).collect()
}

#[tokio::test]
async fn a_task_fires_once_its_slot_has_passed_in_the_users_time_zone() {
    let cases = [
        ("UTC", "0 9 * * *", "2026-07-01T00:00:00Z", "2026-07-01T09:01:00Z", "2026-07-01T09:00:00.000Z"),
        ("Europe/Kiev", "0 9 * * *", "2026-07-01T00:00:00Z", "2026-07-01T06:01:00Z", "2026-07-01T06:00:00.000Z"),
        ("UTC", "0 0 1 * 1", "2026-10-02T00:00:00Z", "2026-10-05T00:00:30Z", "2026-10-05T00:00:00.000Z"),
    ];
    for (zone, cron, created, now, slot) in cases {
        assert_eq!(fired_once(zone, cron, true, created, &[now]).await, [slot], "{zone} {cron}");
    }
}

#[tokio::test]
async fn a_task_does_not_fire_before_its_slot() {
    for (zone, now) in [("UTC", "2026-07-01T08:59:00Z"), ("Europe/Kiev", "2026-07-01T05:59:00Z")] {
        assert!(fired_once(zone, "0 9 * * *", true, "2026-07-01T00:00:00Z", &[now]).await.is_empty(), "{zone}");
    }
}

#[tokio::test]
async fn a_late_tick_catches_up_one_slot_at_a_time() {
    let (_dir, db) = store();
    let row = insert(&db, "0 * * * *", true, "hourly", ms("2026-10-01T00:00:00Z")).await;
    let last = ms("2026-10-02T10:00:00Z") / 1000;
    assert!(db.swap_last_run(row.id, None, Some(last)).await.unwrap());
    let recorder = Recorder::default();
    schedule::tick(&db, &recorder, Tz::UTC, at(ms("2026-10-02T13:05:00Z"))).await.unwrap();
    let slots: Vec<String> = recorder.fires.lock().unwrap().iter().map(|f| iso(f.slot.timestamp_millis())).collect();
    assert_eq!(slots, ["2026-10-02T11:00:00.000Z"]);
}

#[tokio::test]
async fn a_one_shot_task_fires_once_and_is_gone() {
    let (_dir, db) = store();
    insert(&db, "30 14 2 10 *", false, "take pills", ms("2026-10-01T00:00:00Z")).await;
    let recorder = Recorder::default();
    for now in ["2026-10-02T14:31:00Z", "2026-10-02T14:40:00Z"] {
        schedule::tick(&db, &recorder, Tz::UTC, at(ms(now))).await.unwrap();
    }
    assert_eq!(recorder.fires.lock().unwrap().len(), 1);
    let listed = call(&db, &workflows(), "UTC", "list_scheduled_tasks", json!({}), ms("2026-10-02T14:41:00Z")).await;
    assert_eq!(listed.unwrap()["count"], 0);
}

// `now` is a one-shot whatever `recurring` says: the next tick fires it with
// the workflow it names, once; a runner that was stopping leaves it for the
// tick after.
#[tokio::test]
async fn a_now_task_fires_once_on_the_next_tick() {
    let (_dir, db) = store();
    let created = ms("2026-10-02T14:30:10Z");
    let args = json!({"cron_expr": "now", "recurring": true, "prompt": "run the digest", "workflow": "digest"});
    let reply = call(&db, &workflows(), "Europe/Kiev", "schedule_task", args, created).await.unwrap();
    assert_eq!((&reply["ok"], &reply["task"]["recurring"]), (&json!(true), &json!(0)), "{reply}");
    assert_eq!(reply["upcoming_fires"], json!([iso(created)]));
    let listed = call(&db, &workflows(), "Europe/Kiev", "list_scheduled_tasks", json!({}), created + 1_000).await;
    assert_eq!(listed.unwrap()["tasks"][0]["upcoming_fires"], json!([iso(created + 1_000)]));

    let recorder = Recorder::default();
    recorder.refuse.store(true, Ordering::SeqCst);
    schedule::tick(&db, &recorder, tz_of("Europe/Kiev"), at(created + 20_000)).await.unwrap();
    recorder.refuse.store(false, Ordering::SeqCst);
    for now in [created + 50_000, created + 80_000] {
        schedule::tick(&db, &recorder, tz_of("Europe/Kiev"), at(now)).await.unwrap();
    }
    let fires = recorder.fires.lock().unwrap().clone();
    assert_eq!(fires.len(), 1, "{fires:?}");
    assert_eq!(fires[0].workflow.as_deref(), Some("digest"));
    assert!(fires[0].content.contains("run the digest"), "{}", fires[0].content);
    let listed = call(&db, &workflows(), "Europe/Kiev", "list_scheduled_tasks", json!({}), created + 90_000).await;
    assert_eq!(listed.unwrap()["count"], 0);
}

#[tokio::test]
async fn schedule_task_refuses_what_would_never_fire_as_written() {
    let (_dir, db) = store();
    let now = ms("2026-07-01T00:00:00Z");
    let refused = [
        (json!({ "cron_expr": "every day at nine", "recurring": true, "prompt": "x" }), "Invalid cron"),
        (json!({ "cron_expr": "30 9 * * * *", "recurring": true, "prompt": "x" }), "expected 5 fields"),
        (json!({ "cron_expr": "0 9 31 11 *", "recurring": true, "prompt": "x" }), "never matches"),
        (
            json!({ "cron_expr": "0 9 * * *", "recurring": true, "prompt": "x", "workflow": "missing" }),
            "no active version",
        ),
    ];
    for (args, reason) in refused {
        let reply = call(&db, &workflows(), "UTC", "schedule_task", args.clone(), now).await.unwrap();
        assert_eq!(reply["ok"], false, "{args}");
        assert!(reply["error"].as_str().unwrap().contains(reason), "{args}: {reply}");
    }
    let missing =
        call(&db, &workflows(), "UTC", "schedule_task", json!({ "cron_expr": "0 9 * * *", "prompt": "x" }), now).await;
    assert!(missing.unwrap_err().to_string().contains("recurring"));
    assert!(tasks(&db).is_empty(), "nothing refused was stored");
}

// ── races and failures ──────────────────────────────────────────────────────

// Ticks overlap when one runs long. Every slot still fires once.
#[tokio::test]
async fn overlapping_ticks_fire_every_slot_once() {
    let (_dir, db) = store();
    let created = ms("2026-07-01T00:00:00Z");
    for i in 0..20 {
        insert(&db, "*/5 * * * *", i % 2 == 0, &format!("t{i}"), created).await;
    }
    let recorder = Recorder::default();
    let now = at(created + 7 * MINUTE);
    let (a, b, c) = tokio::join!(
        schedule::tick(&db, &recorder, Tz::UTC, now),
        schedule::tick(&db, &recorder, Tz::UTC, now),
        schedule::tick(&db, &recorder, Tz::UTC, now)
    );
    for result in [a, b, c] {
        result.unwrap();
    }
    let fired = recorder.fires.lock().unwrap().clone();
    let keys: Vec<String> = fired.iter().map(|f| format!("{}@{}", f.task_id, f.slot)).collect();
    assert_eq!(keys.len(), keys.iter().collect::<HashSet<_>>().len(), "duplicate fires: {keys:?}");
    assert_eq!(fired.iter().map(|f| f.task_id).collect::<HashSet<_>>().len(), 20);
    let ids: HashSet<i64> = fired.iter().map(|f| f.id).collect();
    assert_eq!(ids.len(), fired.len(), "every fire has its own id");
}

// A store whose listing is held until the test lets it go, or whose fire log
// fails once.
struct Wrapped<'a> {
    inner: &'a AgentDb,
    gate: Option<tokio::sync::Notify>,
    fail_records: AtomicUsize,
}

#[async_trait]
impl ScheduleStore for Wrapped<'_> {
    async fn insert(&self, task: &NewTask) -> anyhow::Result<TaskRow> {
        self.inner.insert(task).await
    }
    async fn list_active(&self) -> anyhow::Result<Vec<TaskRow>> {
        let rows = self.inner.list_active().await;
        if let Some(gate) = &self.gate {
            gate.notified().await;
        }
        rows
    }
    async fn swap_last_run(&self, id: i64, expected: Option<i64>, next: Option<i64>) -> anyhow::Result<bool> {
        self.inner.swap_last_run(id, expected, next).await
    }
    async fn delete(&self, id: i64) -> anyhow::Result<bool> {
        self.inner.delete(id).await
    }
    async fn record_fire(&self, task_id: i64, slot: i64) -> anyhow::Result<i64> {
        if self.fail_records.load(Ordering::SeqCst) > 0 {
            self.fail_records.fetch_sub(1, Ordering::SeqCst);
            anyhow::bail!("disk I/O error");
        }
        self.inner.record_fire(task_id, slot).await
    }
    async fn initialize(&self, tasks: &[NewTask]) -> anyhow::Result<bool> {
        self.inner.initialize(tasks).await
    }
}

// A task cancelled after a tick read the task list, but before the tick got
// to it, must not fire: the user was told it is gone.
#[tokio::test]
async fn a_task_cancelled_while_a_tick_is_in_flight_does_not_fire() {
    let (_dir, db) = store();
    let created = ms("2026-07-01T00:00:00Z");
    let row = insert(&db, "0 9 * * *", false, "take pills", created).await;
    let gated = Wrapped { inner: &db, gate: Some(tokio::sync::Notify::new()), fail_records: AtomicUsize::new(0) };
    let recorder = Recorder::default();
    let in_flight = schedule::tick(&gated, &recorder, Tz::UTC, at(created + 10 * HOUR));
    let cancel = async {
        let reply = call(&db, &workflows(), "UTC", "cancel_scheduled_task", json!({ "id": row.id }), created).await;
        assert_eq!(reply.unwrap()["ok"], true);
        gated.gate.as_ref().unwrap().notify_one();
    };
    let (fired, ()) = tokio::join!(in_flight, cancel);
    fired.unwrap();
    assert!(recorder.fires.lock().unwrap().is_empty());
}

// Intent is not delivery: a slot whose fire could not be logged, or that the
// runner did not start (it was stopping), is delivered by a later tick
// instead of being marked as done with nothing run.
#[tokio::test]
async fn a_slot_that_did_not_run_is_delivered_by_the_next_tick() {
    for case in ["the fire log failed", "the runner was stopping"] {
        let (_dir, db) = store();
        let created = ms("2026-07-01T00:00:00Z");
        let row = insert(&db, "30 14 1 7 *", false, "take pills", created).await;
        let failing = usize::from(case == "the fire log failed");
        let wrapped = Wrapped { inner: &db, gate: None, fail_records: AtomicUsize::new(failing) };
        let recorder = Recorder::default();
        recorder.refuse.store(case == "the runner was stopping", Ordering::SeqCst);
        let _ = schedule::tick(&wrapped, &recorder, Tz::UTC, at(created + 15 * HOUR)).await;
        assert!(recorder.fires.lock().unwrap().is_empty(), "{case}");
        assert_eq!(tasks(&db)[&row.id].last_run_at, None, "{case}: the slot went back");
        recorder.refuse.store(false, Ordering::SeqCst);
        schedule::tick(&wrapped, &recorder, Tz::UTC, at(created + 15 * HOUR + 30_000)).await.unwrap();
        let fired: Vec<(i64, String)> =
            recorder.fires.lock().unwrap().iter().map(|f| (f.task_id, iso(f.slot.timestamp_millis()))).collect();
        assert_eq!(fired, [(row.id, "2026-07-01T14:30:00.000Z".to_owned())], "{case}");
    }
}

#[tokio::test]
async fn fire_ids_continue_above_every_signal_id_mcp_handed_out() {
    let (_dir, db) = store();
    let first = db.record_fire(1, 0).await.unwrap();
    assert_eq!(first, FIRST_FIRE_ID + 1);
    assert_eq!(db.record_fire(1, 60).await.unwrap(), first + 1);
    assert_eq!(MemorySchedule::default().record_fire(1, 0).await.unwrap(), first, "both stores agree");
}

// ── the first start ─────────────────────────────────────────────────────────

// MCP as the first start sees it: its old schedule, or none.
struct OldMcp {
    tasks: Option<Value>,
    fail: AtomicBool,
}

#[async_trait]
impl Mcp for OldMcp {
    fn tools(&self) -> Vec<ToolDef> {
        self.tasks.iter().map(|_| ToolDef::new("list_scheduled_tasks", "list", json!({"type":"object"}))).collect()
    }
    async fn call_tool(&self, name: &str, _: Map<String, Value>, _: &CancellationToken) -> anyhow::Result<String> {
        anyhow::ensure!(name == "list_scheduled_tasks", "unexpected call {name}");
        anyhow::ensure!(!self.fail.load(Ordering::SeqCst), "connection reset");
        Ok(json!({ "timezone": "UTC", "count": 1, "tasks": self.tasks.clone().unwrap() }).to_string())
    }
}

#[tokio::test]
async fn the_first_start_imports_mcps_tasks_with_their_last_fire() {
    let (_dir, db) = store();
    let old = json!([
        {"id": 7, "cron_expr": "0 9 * * *", "recurring": true, "prompt": "digest", "source": null,
         "last_run_at": 1_782_896_400, "created_at": "2026-06-01 10:00:00"},
        {"id": 9, "cron_expr": "0 4 * * *", "recurring": true, "prompt": "dream", "source": "dreaming",
         "last_run_at": null, "created_at": "2026-06-02 11:30:00"},
    ]);
    let mcp = OldMcp { tasks: Some(old), fail: AtomicBool::new(true) };
    let cancel = CancellationToken::new();
    assert!(schedule::initialize(&db, &mcp, &cancel).await.is_err());
    assert!(tasks(&db).is_empty(), "a failed import changes nothing");
    mcp.fail.store(false, Ordering::SeqCst);
    assert_eq!(schedule::initialize(&db, &mcp, &cancel).await.unwrap(), 2);
    let imported: Vec<(String, Option<String>, Option<i64>, i64)> =
        tasks(&db).into_values().map(|t| (t.prompt, t.source, t.last_run_at, t.created_ms)).collect();
    assert_eq!(
        imported,
        [
            ("digest".to_owned(), None, Some(1_782_896_400), ms("2026-06-01T10:00:00Z")),
            ("dream".to_owned(), Some("dreaming".to_owned()), None, ms("2026-06-02T11:30:00Z")),
        ]
    );
    // Never again: what the user cancels since stays cancelled.
    let first = *tasks(&db).keys().next().unwrap();
    db.delete(first).await.unwrap();
    assert_eq!(schedule::initialize(&db, &mcp, &cancel).await.unwrap(), 0);
    assert_eq!(tasks(&db).len(), 1);
}

#[tokio::test]
async fn a_fresh_install_seeds_the_system_tasks_once() {
    for mcp in [
        OldMcp { tasks: None, fail: AtomicBool::new(false) },
        OldMcp { tasks: Some(json!([])), fail: AtomicBool::new(false) },
    ] {
        let (_dir, db) = store();
        let cancel = CancellationToken::new();
        assert_eq!(schedule::initialize(&db, &mcp, &cancel).await.unwrap(), 1);
        let seeded: Vec<(String, Option<String>)> = tasks(&db).into_values().map(|t| (t.cron, t.source)).collect();
        // Only the generic dreaming tick: user-specific tasks (digests) are not shipped.
        assert_eq!(seeded, [("0 4 * * *".to_owned(), Some("dreaming".to_owned()))]);
        let id = *tasks(&db).keys().next().unwrap();
        db.delete(id).await.unwrap();
        assert_eq!(schedule::initialize(&db, &mcp, &cancel).await.unwrap(), 0, "a cancelled system task stays gone");
        assert!(tasks(&db).is_empty());
    }
}

// What a generated call is, and how it is made concrete. A port of
// packages/mcp/model/calls.ts.
//
// proptest generates ABSTRACT steps: "a quote from the current text", "the
// current version", "the newest patch". Each is made concrete against the
// model's view of the document just before it is sent, so quotes actually
// match, versions are usually right and reverts name patches that exist —
// otherwise almost every random call would bounce off a validation error.

use proptest::prelude::*;
use serde_json::{Value, json};

use crate::model::{Cmd, Doc, Edit, World, chars};

pub const DOCS: [&str; 2] = ["a.md", "b.md"];
pub const PROJECT: &str = "p";

// An index the model never reaches, for a patch the server does not have.
pub const NO_PATCH: usize = 1_000_000;
// Never a real id: real ones are "pa:" + 6 hex digits.
pub const MISSING_PID: &str = "pa:zzzzzz";

// Few distinct characters, so quotes collide, overlap and nearly match
// ("ё"/"е", "—"/"-") as often as possible.
pub const ALPHABET: [char; 11] = ['a', 'b', 'c', 'x', ' ', '\n', '#', 'ё', 'е', '—', '-'];

#[derive(Debug, Clone, Copy)]
pub enum Version {
    Current,
    Stale,
    Ahead,
    Omitted,
}

// A slice of the current text (start and length, wrapped to fit), or a literal.
#[derive(Debug, Clone)]
pub enum Quote {
    Slice(usize, usize),
    Literal(String),
}

#[derive(Debug, Clone, Copy)]
pub enum PatchPick {
    Newest,
    Index(usize),
    Missing,
}

#[derive(Debug, Clone)]
pub enum Step {
    Write { doc: usize, body: String, version: Version },
    Append { doc: usize, text: String },
    Patch { doc: usize, version: Version, edits: Vec<(Quote, String)> },
    Revert { doc: usize, pick: PatchPick, rollback: bool },
    Read { doc: usize },
    History { doc: usize },
}

impl Step {
    pub fn doc(&self) -> &'static str {
        let i = match self {
            Step::Write { doc, .. }
            | Step::Append { doc, .. }
            | Step::Patch { doc, .. }
            | Step::Revert { doc, .. }
            | Step::Read { doc }
            | Step::History { doc } => *doc,
        };
        DOCS[i]
    }
}

fn alphabet() -> impl Strategy<Value = char> {
    proptest::sample::select(&ALPHABET[..])
}

// Mostly random characters, sometimes a run of one character: repeats are
// where quotes overlap ("aa" occurs twice in "aaa"), and uniformly random
// text almost never has them.
pub fn text(max_length: usize) -> impl Strategy<Value = String> {
    prop_oneof![
        3 => proptest::collection::vec(alphabet(), 0..=max_length).prop_map(|cs| cs.into_iter().collect()),
        2 => (alphabet(), 0..=max_length).prop_map(|(c, n)| std::iter::repeat_n(c, n).collect()),
    ]
}

fn version() -> impl Strategy<Value = Version> {
    prop_oneof![6 => Just(Version::Current), 2 => Just(Version::Stale), 1 => Just(Version::Ahead), 1 => Just(Version::Omitted)]
}

fn quote() -> impl Strategy<Value = Quote> {
    prop_oneof![
        4 => (0usize..=63, 1usize..=5).prop_map(|(start, len)| Quote::Slice(start, len)),
        1 => text(3).prop_map(Quote::Literal),
    ]
}

// Rollback in roughly three reverts out of ten.
fn rollback() -> impl Strategy<Value = bool> {
    prop_oneof![3 => Just(true), 7 => Just(false)]
}

fn pick() -> impl Strategy<Value = PatchPick> {
    prop_oneof![3 => Just(PatchPick::Newest), 3 => (0usize..=7).prop_map(PatchPick::Index), 1 => Just(PatchPick::Missing)]
}

// `docs`: how many documents the steps spread over (1 for a concurrent
// session, so the sessions collide on one document).
pub fn step(docs: usize) -> impl Strategy<Value = Step> {
    let doc = 0..docs;
    prop_oneof![
        3 => (doc.clone(), text(12), version()).prop_map(|(doc, body, version)| Step::Write { doc, body, version }),
        3 => (doc.clone(), text(6)).prop_map(|(doc, text)| Step::Append { doc, text }),
        5 => (doc.clone(), version(), proptest::collection::vec((quote(), text(4)), 1..=2))
            .prop_map(|(doc, version, edits)| Step::Patch { doc, version, edits }),
        2 => (doc.clone(), pick(), rollback()).prop_map(|(doc, pick, rollback)| Step::Revert { doc, pick, rollback }),
        1 => doc.clone().prop_map(|doc| Step::Read { doc }),
        1 => doc.prop_map(|doc| Step::History { doc }),
    ]
}

// A concrete call, as both sides receive it: the model's command, plus — for
// a revert — the same patch's id on the server.
#[derive(Debug, Clone)]
pub struct Call {
    pub cmd: Cmd,
    pub pid: Option<String>,
}

// `patch_ids`: the server's patch ids for the step's document, oldest first,
// so index i is the model's patch i. None for model-only checks, where the
// index is enough.
pub fn concretise(step: &Step, world: &World, patch_ids: Option<&[String]>) -> Call {
    let doc = step.doc().to_owned();
    let view = world.get(&doc).cloned().unwrap_or_default();
    let expected = |v: Version| -> Option<i64> {
        match v {
            Version::Current => Some(view.version),
            Version::Stale => Some((view.version - 1).max(0)),
            Version::Ahead => Some(view.version + 1),
            Version::Omitted => None,
        }
    };
    let cmd = |cmd: Cmd| Call { cmd, pid: None };
    match step {
        Step::Write { body, version, .. } => cmd(Cmd::Write { doc, body: body.clone(), expected: expected(*version) }),
        Step::Append { text, .. } => cmd(Cmd::Append { doc, text: text.clone() }),
        Step::Patch { version, edits, .. } => {
            let body = chars(&view.body);
            let edits = edits
                .iter()
                .map(|(q, replacement)| {
                    let old = match q {
                        Quote::Literal(literal) => literal.clone(),
                        Quote::Slice(..) if body.is_empty() => "zz".to_owned(),
                        Quote::Slice(start, len) => {
                            let start = start % body.len();
                            body[start..(start + len).min(body.len())].iter().collect()
                        }
                    };
                    Edit { old, new: replacement.clone() }
                })
                .collect();
            cmd(Cmd::Patch { doc, expected: expected(*version).unwrap_or(0), edits })
        }
        Step::Revert { pick, rollback, .. } => {
            let patches = view.history.len();
            let index = match pick {
                PatchPick::Newest => patches.saturating_sub(1),
                PatchPick::Missing => patches + 5,
                PatchPick::Index(i) => *i,
            };
            let Some(ids) = patch_ids else { return cmd(Cmd::Revert { doc, index, rollback: *rollback }) };
            match ids.get(index) {
                // Unknown on the server: an id that never exists, and an index
                // the model will never reach, even if patches arrive meanwhile.
                None => Call {
                    cmd: Cmd::Revert { doc, index: NO_PATCH, rollback: *rollback },
                    pid: Some(MISSING_PID.to_owned()),
                },
                Some(pid) => Call { cmd: Cmd::Revert { doc, index, rollback: *rollback }, pid: Some(pid.clone()) },
            }
        }
        Step::Read { .. } => cmd(Cmd::Read { doc }),
        Step::History { .. } => cmd(Cmd::History { doc }),
    }
}

// The same call as an MCP tool call.
pub fn to_tool(call: &Call) -> (&'static str, Value) {
    match &call.cmd {
        Cmd::Write { doc, body, expected } => {
            let mut args = json!({ "project": PROJECT, "doc": doc, "body": body });
            if let Some(v) = expected {
                args["expected_version"] = json!(v);
            }
            ("write_doc", args)
        }
        Cmd::Append { doc, text } => ("append_doc", json!({ "project": PROJECT, "doc": doc, "text": text })),
        Cmd::Patch { doc, expected, edits } => {
            let edits: Vec<Value> = edits.iter().map(|e| json!({ "old": e.old, "new": e.new })).collect();
            ("patch_doc", json!({ "project": PROJECT, "doc": doc, "expected_version": expected, "edits": edits }))
        }
        Cmd::Revert { doc, rollback, .. } => {
            ("revert_patch", json!({ "project": PROJECT, "doc": doc, "patch_id": call.pid, "rollback": rollback }))
        }
        Cmd::Read { doc } => ("read_doc", json!({ "project": PROJECT, "doc": doc })),
        Cmd::History { doc } => ("doc_history", json!({ "project": PROJECT, "doc": doc, "limit": 100 })),
    }
}

pub fn describe_call(call: &Call) -> String {
    format!("{:?}", call.cmd)
}

#[allow(dead_code)]
pub fn empty_doc() -> Doc {
    Doc::default()
}

// Unified memory: one memory every agent shares — the droplet supervisor,
// Claude Code sessions, ChatGPT through the tunnel. Design and rationale in
// .claude/tasks/unified-memory.md (the D-numbers below refer to it).
//
// Two read models behind one search projection (D8): projects made of
// markdown documents, and flat facts. Both feed `memory_index`, the only
// table with embeddings. The agent's flow is two-step: `recall` returns refs,
// `read_doc` / `get_fact` loads the whole thing. A third read model, the
// journal, sits outside search: operational records a workflow appends and
// reads back by recency.
//
// Sections:
//   1. types      — states, projects, documents, facts, patches, index rows
//   2. refs       — `doc:<project>/<name>#<chunk>` / `fact:<id>`, parsed strictly
//   3. patch      — search/replace edits, near-match hints, append, invert
//   4. projection — markdown chunking, index text, recency-aware ranking
//   5. store      — the dumb-CRUD port + its Postgres implementation
//   6. indexer    — keeps `memory_index` in step with the read models
//   7. service    — every rule that protects a document
//   8. import     — one-shot copy of knowledge_base_notes into facts
//   9. tools      — `memory` toolset
//
// The rules live in the service and the store underneath is dumb, so the
// whole contract is tested against an in-memory store without a Postgres.

use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use rmcp::handler::server::wrapper::Parameters;
use rmcp::{schemars, tool, tool_router};
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value, json};

use crate::embeddings::{BackfillBatch, SharedEmbedder, backfill_rows};
use crate::pg::{PgPool, vector_literal};
use crate::server::{McpTools, ToolResult, invalid_params, json_result, tool_failed};
use crate::time::iso;

// ── 1. types ─────────────────────────────────────────────────────────────────

// D7 — decay is an explicit state plus a recency boost at ranking time.
// Nothing is ever deleted.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(rename_all = "lowercase")]
pub enum MemoryState {
    Active,
    Done,
    Archived,
}

impl MemoryState {
    pub fn as_str(self) -> &'static str {
        match self {
            MemoryState::Active => "active",
            MemoryState::Done => "done",
            MemoryState::Archived => "archived",
        }
    }

    pub fn parse(s: &str) -> Self {
        match s {
            "done" => MemoryState::Done,
            "archived" => MemoryState::Archived,
            _ => MemoryState::Active,
        }
    }
}

fn ser_iso<S: serde::Serializer>(t: &DateTime<Utc>, s: S) -> Result<S::Ok, S::Error> {
    s.serialize_str(&iso(*t))
}

#[derive(Debug, Clone, Serialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct Project {
    pub id: i64,
    pub slug: String,
    pub title: String,
    #[serde(serialize_with = "ser_iso")]
    pub created_at: DateTime<Utc>,
    #[serde(serialize_with = "ser_iso")]
    pub updated_at: DateTime<Utc>,
}

// Enough to choose a document without loading it; the summary is what keeps
// a project from growing notes.md / notes2.md / progress-new.md (D5).
#[derive(Debug, Clone, Serialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct DocSummary {
    pub searchable: bool,
    pub name: String,
    pub summary: Option<String>,
    pub version: i32,
    pub size_bytes: usize,
    #[serde(serialize_with = "ser_iso")]
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct Doc {
    pub searchable: bool,
    pub id: i64,
    pub project_id: i64,
    pub name: String,
    pub summary: Option<String>,
    pub body: String,
    pub version: i32,
    pub size_bytes: usize,
    #[serde(serialize_with = "ser_iso")]
    pub updated_at: DateTime<Utc>,
}

impl Doc {
    fn summary_view(&self) -> DocSummary {
        DocSummary {
            searchable: self.searchable,
            name: self.name.clone(),
            summary: self.summary.clone(),
            version: self.version,
            size_bytes: self.size_bytes,
            updated_at: self.updated_at,
        }
    }
}

#[derive(Debug, Clone, Serialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct Fact {
    pub id: i64,
    pub body: String,
    pub tags: Vec<String>,
    pub source: Option<String>,
    pub state: MemoryState,
    #[serde(serialize_with = "ser_iso")]
    pub created_at: DateTime<Utc>,
    #[serde(serialize_with = "ser_iso")]
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum PatchKind {
    Write,
    Append,
    Patch,
    Revert,
}

impl PatchKind {
    fn as_str(self) -> &'static str {
        match self {
            PatchKind::Write => "write",
            PatchKind::Append => "append",
            PatchKind::Patch => "patch",
            PatchKind::Revert => "revert",
        }
    }

    fn parse(s: &str) -> Self {
        match s {
            "append" => PatchKind::Append,
            "patch" => PatchKind::Patch,
            "revert" => PatchKind::Revert,
            _ => PatchKind::Write,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, schemars::JsonSchema)]
pub struct Edit {
    /// Exact text to find, unique in the document.
    pub old: String,
    /// Replacement. Empty string deletes.
    pub new: String,
}

// One row per write. `body_before` is the whole previous document: it makes
// "roll roadmap.md back to v7" answerable and is what a failed mid-stack
// revert falls back to (D9).
#[derive(Debug, Clone, PartialEq)]
pub struct DocPatch {
    // Short and random: sequential ids invite guessing a neighbour's.
    pub pid: String,
    pub doc_id: i64,
    pub kind: PatchKind,
    pub edits: Vec<Edit>,
    pub body_before: String,
    pub version_before: i32,
    pub version_after: i32,
    pub actor: String,
    pub rationale: Option<String>,
    pub created_at: DateTime<Utc>,
}

// A row on its way into the search projection. `source_ref` is the owning
// object and what a re-index deletes by — exact equality, never a prefix, so
// `fact:88` can't wipe `fact:880`.
#[derive(Debug, Clone)]
pub struct IndexUpsert {
    pub source_ref: String,
    pub r#ref: String,
    pub text: String,
    pub tags: Vec<String>,
    pub actor: Option<String>,
    pub state: MemoryState,
    pub ts: DateTime<Utc>,
    pub embedding: Option<Vec<f32>>,
}

#[derive(Debug, Clone)]
pub struct IndexHit {
    pub id: i64,
    pub r#ref: String,
    pub text: String,
    pub tags: Vec<String>,
    pub actor: Option<String>,
    pub state: MemoryState,
    pub ts: DateTime<Utc>,
    pub distance: f64,
}

// The journal — the third read model. Operational records a workflow writes
// and reads back by recency (the last digests, a run log), as opposed to
// knowledge. Append-only, never indexed, no patch history: an entry is
// immutable and leaves only when its stream outgrows its capacity.
#[derive(Debug, Clone, Serialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct JournalEntry {
    pub id: i64,
    pub stream: String,
    pub body: String,
    pub actor: String,
    // Provenance the writer attaches (e.g. the trace an activity note was
    // derived from). Stored as given, never interpreted: no dedup, no lookup.
    pub metadata: Option<Map<String, Value>>,
    #[serde(serialize_with = "ser_iso")]
    pub created_at: DateTime<Utc>,
}

fn ser_iso_opt<S: serde::Serializer>(t: &Option<DateTime<Utc>>, s: S) -> Result<S::Ok, S::Error> {
    match t {
        Some(t) => s.serialize_str(&iso(*t)),
        None => s.serialize_none(),
    }
}

// A stream is a named category of entries. Its capacity is stored with it,
// so every writer trims by the same rule without repeating it.
#[derive(Debug, Clone, Serialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct JournalStream {
    pub name: String,
    pub capacity: i32,
    pub size: i64,
    #[serde(serialize_with = "ser_iso_opt")]
    pub last_at: Option<DateTime<Utc>>,
}

// The entry and its stream after the trim.
#[derive(Debug, Clone, Serialize, PartialEq)]
pub struct AppendedEntry {
    pub entry: JournalEntry,
    pub stream: JournalStream,
}

// ── 2. refs ──────────────────────────────────────────────────────────────────

// Refs travel through an LLM, so they are parsed strictly: one that doesn't
// round-trip is a bug to see, not something to guess at.
fn slug_re() -> regex::Regex {
    regex::Regex::new(r"^[a-z0-9][a-z0-9-]*$").expect("valid regex")
}

// Markdown filenames, no directories: a project is a flat folder, and a
// separator would make refs ambiguous.
fn doc_name_re() -> regex::Regex {
    regex::Regex::new(r"^[a-z0-9][a-z0-9._-]*\.md$").expect("valid regex")
}

// Journal stream names: project-slug segments joined by "/", so a family of
// streams reads as one ("digest/news", "digest/tech"). The separator is a
// naming convention only — every name is its own stream.
pub const JOURNAL_STREAM_RE: &str = r"^[a-z0-9][a-z0-9-]*(/[a-z0-9][a-z0-9-]*)*$";
pub const JOURNAL_STREAM_MAX_LENGTH: usize = 100;

pub fn is_journal_stream(name: &str) -> bool {
    name.len() <= JOURNAL_STREAM_MAX_LENGTH && regex::Regex::new(JOURNAL_STREAM_RE).expect("valid regex").is_match(name)
}

pub fn assert_project_slug(slug: &str) -> anyhow::Result<()> {
    anyhow::ensure!(
        slug_re().is_match(slug),
        "Invalid project slug \"{slug}\". Use lowercase letters, digits and hyphens, e.g. \"leetcode-graphs\"."
    );
    Ok(())
}

pub fn assert_doc_name(name: &str) -> anyhow::Result<()> {
    anyhow::ensure!(
        doc_name_re().is_match(name),
        "Invalid document name \"{name}\". Use a lowercase markdown filename with no directories, e.g. \"roadmap.md\"."
    );
    Ok(())
}

pub fn doc_ref(project: &str, doc: &str, chunk: Option<usize>) -> String {
    match chunk {
        Some(c) => format!("doc:{project}/{doc}#{c}"),
        None => format!("doc:{project}/{doc}"),
    }
}

pub fn fact_ref(id: i64) -> String {
    format!("fact:{id}")
}

#[derive(Debug, PartialEq)]
pub enum MemoryRef {
    Doc { project: String, doc: String, chunk: Option<usize> },
    Fact { id: i64 },
}

pub fn parse_ref(raw: &str) -> Option<MemoryRef> {
    if let Some(id) = raw.strip_prefix("fact:") {
        if id.is_empty() || !id.bytes().all(|b| b.is_ascii_digit()) {
            return None;
        }
        return Some(MemoryRef::Fact { id: id.parse().ok()? });
    }
    let caps = regex::Regex::new(r"^doc:([^/]+)/([^#]+)(?:#(\d+))?$").expect("valid regex").captures(raw)?;
    let (project, doc) = (caps[1].to_owned(), caps[2].to_owned());
    if !slug_re().is_match(&project) || !doc_name_re().is_match(&doc) {
        return None;
    }
    Some(MemoryRef::Doc { project, doc, chunk: caps.get(3).and_then(|c| c.as_str().parse().ok()) })
}

// ── 3. patch ─────────────────────────────────────────────────────────────────

// Search/replace on quoted literals, never line numbers (D10): an off-by-one
// line number corrupts silently, a quote that doesn't match fails loudly and
// changes nothing. Pure — body in, body out.

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum EditFailureReason {
    Empty,
    NotFound,
    Ambiguous,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct EditFailure {
    // Position in the caller's edits, so the model fixes one edit rather
    // than resending the whole call blind.
    pub index: usize,
    pub old: String,
    pub reason: EditFailureReason,
    pub occurrences: usize,
    // Literal text that *nearly* matched. Suggested, never applied:
    // fuzzy-applying is how an agent silently edits the wrong sentence.
    pub suggestions: Vec<String>,
}

// Overlapping matches count: in "   " the quote "  " could mean either pair,
// so it is ambiguous, not "the first one".
fn count_occurrences(haystack: &str, needle: &str) -> usize {
    haystack.char_indices().filter(|&(at, _)| haystack[at..].starts_with(needle)).count()
}

// Atomic: all edits apply or none do. Applied in order to a working copy, so
// a later edit may target text an earlier one produced.
pub fn apply_edits(body: &str, edits: &[Edit]) -> Result<String, Vec<EditFailure>> {
    let failure = |index, old: &str, reason, occurrences, suggestions| EditFailure {
        index,
        old: old.to_owned(),
        reason,
        occurrences,
        suggestions,
    };
    if edits.is_empty() {
        return Err(vec![failure(0, "", EditFailureReason::Empty, 0, Vec::new())]);
    }
    let mut working = body.to_owned();
    let mut failures = Vec::new();
    for (index, edit) in edits.iter().enumerate() {
        // An empty `old` matches everywhere; "insert at the start" is what
        // append_doc is for.
        if edit.old.is_empty() {
            failures.push(failure(index, &edit.old, EditFailureReason::Empty, 0, Vec::new()));
            continue;
        }
        match count_occurrences(&working, &edit.old) {
            1 => working = working.replacen(&edit.old, &edit.new, 1),
            // Never "take the first match": quote more context.
            0 => failures.push(failure(
                index,
                &edit.old,
                EditFailureReason::NotFound,
                0,
                find_near_matches(&working, &edit.old, 3),
            )),
            n => failures.push(failure(index, &edit.old, EditFailureReason::Ambiguous, n, Vec::new())),
        }
    }
    if failures.is_empty() { Ok(working) } else { Err(failures) }
}

// The inverse of a patch, for reverting one that is no longer the newest. A
// deletion has no inverse — re-inserting needs a position whose anchor is
// gone — so callers fall back to a whole-document rollback.
pub fn invert_edits(edits: &[Edit]) -> Option<Vec<Edit>> {
    edits.iter().rev().map(|e| (!e.new.is_empty()).then(|| Edit { old: e.new.clone(), new: e.old.clone() })).collect()
}

// The usual miss is a *normalised* quote: an em-dash retyped as a hyphen, a
// «quote» straightened, ё written as е, whitespace reflowed. Normalising both
// sides and mapping the hit back to the original characters hands the agent
// the exact literal to retry with.
struct Normalised {
    text: Vec<char>,
    // origin[i] = index in the source chars of normalised char i.
    origin: Vec<usize>,
}

fn is_dash(c: char) -> bool {
    ('\u{2010}'..='\u{2015}').contains(&c) || c == '\u{2212}'
}

fn is_quote(c: char) -> bool {
    matches!(c, '«' | '»' | '“' | '”' | '„' | '‘' | '’' | '′' | '″')
}

fn normalise(source: &[char]) -> Normalised {
    let mut text = Vec::with_capacity(source.len());
    let mut origin = Vec::with_capacity(source.len());
    let mut pending_space = false;
    for (i, &raw) in source.iter().enumerate() {
        if raw.is_whitespace() || raw == '\u{feff}' {
            // Collapse runs; never let one start the string.
            pending_space = !text.is_empty();
            continue;
        }
        if pending_space {
            text.push(' ');
            origin.push(i);
            pending_space = false;
        }
        for c in raw.to_lowercase() {
            let mapped = if is_dash(c) {
                '-'
            } else if is_quote(c) {
                '"'
            } else if c == 'ё' {
                'е'
            } else {
                c
            };
            text.push(mapped);
            origin.push(i);
        }
    }
    Normalised { text, origin }
}

fn find_chars(haystack: &[char], needle: &[char], from: usize) -> Option<usize> {
    if needle.is_empty() || needle.len() > haystack.len() {
        return None;
    }
    (from..=haystack.len() - needle.len()).find(|&at| haystack[at..at + needle.len()] == *needle)
}

pub fn find_near_matches(body: &str, needle: &str, limit: usize) -> Vec<String> {
    let body_chars: Vec<char> = body.chars().collect();
    let n_body = normalise(&body_chars);
    let n_needle = normalise(&needle.chars().collect::<Vec<_>>());
    if n_needle.text.is_empty() {
        return Vec::new();
    }
    let mut spans = Vec::new();
    let mut from = 0;
    while spans.len() < limit {
        let Some(at) = find_chars(&n_body.text, &n_needle.text, from) else { break };
        let (start, end) = (n_body.origin[at], n_body.origin[at + n_needle.text.len() - 1]);
        spans.push(body_chars[start..=end].iter().collect());
        from = at + n_needle.text.len().max(1);
    }
    if !spans.is_empty() {
        return spans;
    }
    // Nothing matched even loosely: the most similar lines at least tell the
    // agent where to look.
    let target: Vec<char> = n_needle.text.iter().take(200).copied().collect();
    let mut scored: Vec<(&str, f64)> = body
        .split('\n')
        .filter(|line| !line.trim().is_empty())
        .map(|line| (line, dice(&normalise(&line.chars().collect::<Vec<_>>()).text, &target)))
        .filter(|(_, score)| *score >= 0.4)
        .collect();
    scored.sort_by(|a, b| b.1.total_cmp(&a.1));
    scored.into_iter().take(limit).map(|(line, _)| line.to_owned()).collect()
}

// Bigram Dice coefficient: cheap, and enough to rank "which line did they
// mean" without pretending to be a diff.
fn dice(a: &[char], b: &[char]) -> f64 {
    if a == b {
        return 1.0;
    }
    if a.len() < 2 || b.len() < 2 {
        return 0.0;
    }
    let mut bigrams: HashMap<(char, char), usize> = HashMap::new();
    for w in a.windows(2) {
        *bigrams.entry((w[0], w[1])).or_default() += 1;
    }
    let mut hits = 0;
    for w in b.windows(2) {
        if let Some(left) = bigrams.get_mut(&(w[0], w[1]))
            && *left > 0
        {
            *left -= 1;
            hits += 1;
        }
    }
    (2 * hits) as f64 / (a.len() - 1 + b.len() - 1) as f64
}

#[derive(Debug, Clone, PartialEq)]
pub struct Heading {
    pub level: usize,
    pub text: String,
    pub line: usize,
}

fn heading_re() -> regex::Regex {
    regex::Regex::new(r"^(#{1,6})\s+(.+?)\s*$").expect("valid regex")
}

pub fn list_headings(body: &str) -> Vec<Heading> {
    let re = heading_re();
    body.split('\n')
        .enumerate()
        .filter_map(|(line, text)| {
            let caps = re.captures(text)?;
            Some(Heading { level: caps[1].len(), text: caps[2].to_owned(), line })
        })
        .collect()
}

// The safe default (D10, op 1): it cannot destroy text and needs no read.
// Headings are the anchors because they survive edits elsewhere.
pub fn append_to_body(body: &str, text: &str, under_heading: Option<&str>) -> Result<String, Vec<String>> {
    let addition = text.trim();
    let Some(heading) = under_heading.filter(|h| !h.trim().is_empty()) else {
        return Ok(join_blocks(body, addition));
    };
    let headings = list_headings(body);
    // "Progress" as readily as "## Progress".
    let wanted = normalise(&heading.trim_start_matches('#').trim_start().chars().collect::<Vec<_>>()).text;
    let Some(target) = headings.iter().find(|h| normalise(&h.text.chars().collect::<Vec<_>>()).text == wanted) else {
        return Err(headings.iter().map(|h| format!("{} {}", "#".repeat(h.level), h.text)).collect());
    };
    let lines: Vec<&str> = body.split('\n').collect();
    // The section runs to the next heading of the same or higher rank; a
    // deeper sub-heading is still part of it.
    let end = headings.iter().find(|h| h.line > target.line && h.level <= target.level).map_or(lines.len(), |h| h.line);
    let (head, tail) = (lines[..end].join("\n"), lines[end..].join("\n"));
    let merged = join_blocks(&head, addition);
    Ok(if tail.is_empty() { merged } else { format!("{merged}\n{tail}") })
}

// One blank line between blocks, exactly one trailing newline: stable
// formatting keeps later quotes matching what the agent last saw.
fn join_blocks(existing: &str, addition: &str) -> String {
    let base = existing.trim_end();
    match (base.is_empty(), addition.is_empty()) {
        (true, true) => String::new(),
        (false, true) => format!("{base}\n"),
        (true, false) => format!("{addition}\n"),
        (false, false) => format!("{base}\n\n{addition}\n"),
    }
}

// ── 4. projection ────────────────────────────────────────────────────────────

pub const DEFAULT_CHUNK_CHARS: usize = 1200;

#[derive(Debug, Clone, PartialEq)]
pub struct MarkdownChunk {
    pub text: String,
    // Breadcrumb of enclosing headings ("Progress > Notes"); empty before
    // the first heading.
    pub heading_path: String,
}

fn char_len(s: &str) -> usize {
    s.chars().count()
}

// Paragraphs packed up to `max_chars`, split at every heading (the strongest
// topical boundary markdown has), never mid-paragraph unless one alone is
// oversized. A month of daily entries must not become one vector (D8).
pub fn chunk_markdown(body: &str, max_chars: usize) -> Vec<MarkdownChunk> {
    let heading = heading_re();
    let mut chunks = Vec::new();
    let mut stack: Vec<(usize, String)> = Vec::new();
    let mut buffer: Vec<String> = Vec::new();
    let mut buffer_path = String::new();
    let path_now = |stack: &[(usize, String)]| stack.iter().map(|(_, t)| t.as_str()).collect::<Vec<_>>().join(" > ");
    let flush = |buffer: &mut Vec<String>, path: &str, chunks: &mut Vec<MarkdownChunk>| {
        let text = buffer.join("\n\n").trim().to_owned();
        buffer.clear();
        if !text.is_empty() {
            chunks.push(MarkdownChunk { text, heading_path: path.to_owned() });
        }
    };

    for block in regex::Regex::new(r"\n\s*\n").expect("valid regex").split(body) {
        let paragraph = block.trim();
        if paragraph.is_empty() {
            continue;
        }
        if let Some(caps) = heading.captures(paragraph) {
            flush(&mut buffer, &buffer_path, &mut chunks);
            let level = caps[1].len();
            while stack.last().is_some_and(|(l, _)| *l >= level) {
                stack.pop();
            }
            stack.push((level, caps[2].to_owned()));
            buffer_path = path_now(&stack);
            continue;
        }
        if buffer.is_empty() {
            buffer_path = path_now(&stack);
        }
        let projected = buffer.iter().map(|p| char_len(p)).sum::<usize>() + 2 * buffer.len() + char_len(paragraph);
        if !buffer.is_empty() && projected > max_chars {
            flush(&mut buffer, &buffer_path, &mut chunks);
            buffer_path = path_now(&stack);
        }
        if char_len(paragraph) > max_chars {
            // A pasted log or a long table still has to fit the embedder.
            let chars: Vec<char> = paragraph.chars().collect();
            for piece in chars.chunks(max_chars) {
                chunks.push(MarkdownChunk { text: piece.iter().collect(), heading_path: path_now(&stack) });
            }
            continue;
        }
        buffer.push(paragraph.to_owned());
    }
    flush(&mut buffer, &buffer_path, &mut chunks);
    chunks
}

// Chunks must be self-contained; documents must not (D8). "stuck on
// Dijkstra" matches nothing alone, so the subject goes into the indexed text
// while the stored document stays clean.
pub fn build_index_text(project_title: &str, doc_name: &str, heading_path: &str, text: &str) -> String {
    if heading_path.is_empty() {
        format!("{project_title} — {doc_name}\n\n{text}")
    } else {
        format!("{project_title} — {doc_name} · {heading_path}\n\n{text}")
    }
}

#[derive(Debug, Clone, Copy)]
pub struct RankOpts {
    // Days for the recency bonus to halve.
    pub half_life_days: f64,
    // How much a brand-new row may improve its distance. Small on purpose:
    // recency breaks ties, it must not float an irrelevant note over a
    // relevant one (D7).
    pub recency_weight: f64,
}

pub const DEFAULT_RANK: RankOpts = RankOpts { half_life_days: 30.0, recency_weight: 0.05 };

pub fn rank_hits(hits: Vec<IndexHit>, now: DateTime<Utc>, opts: RankOpts) -> Vec<(IndexHit, f64)> {
    let mut ranked: Vec<(IndexHit, f64)> = hits
        .into_iter()
        .map(|hit| {
            let age_days = ((now - hit.ts).num_milliseconds() as f64 / 86_400_000.0).max(0.0);
            let boost = opts.recency_weight * 2f64.powf(-age_days / opts.half_life_days);
            let score = hit.distance - boost;
            (hit, score)
        })
        .collect();
    ranked.sort_by(|a, b| a.1.total_cmp(&b.1));
    ranked
}

// ── 5. store ─────────────────────────────────────────────────────────────────

// The patch a write records. The store writes it in the same transaction as
// the body it describes, so a document is never ahead of its history.
pub struct NewPatch {
    pub pid: String,
    pub kind: PatchKind,
    pub edits: Vec<Edit>,
    pub body_before: String,
    pub version_before: i32,
    pub version_after: i32,
    pub actor: String,
    pub rationale: Option<String>,
}

pub struct AppendEntry<'a> {
    pub stream: &'a str,
    pub body: &'a str,
    pub actor: &'a str,
    pub metadata: Option<&'a Map<String, Value>>,
    // Stored on the stream when given; None keeps the stream's capacity, or
    // `default_capacity` for a stream this append creates.
    pub capacity: Option<i32>,
    pub default_capacity: i32,
}

// What an index row set was built from: the fact as it was read, or the
// document version.
#[derive(Debug, Clone, PartialEq)]
pub enum IndexBasis {
    Fact { id: i64, body: String, tags: Vec<String>, state: MemoryState },
    Doc { doc_id: i64, version: i32 },
}

pub struct FactUpdate<'a> {
    pub body: Option<&'a str>,
    pub tags: Option<Vec<String>>,
    pub state: Option<MemoryState>,
}

// Dumb CRUD: no rules here, only the persistence the service's rules are
// expressed in. The one piece of real logic is the compare-and-swap.
#[async_trait]
pub trait MemoryStore: Send + Sync {
    async fn create_project(&self, slug: &str, title: &str) -> anyhow::Result<Project>;
    async fn get_project(&self, slug: &str) -> anyhow::Result<Option<Project>>;
    async fn list_projects(&self) -> anyhow::Result<Vec<Project>>;

    async fn list_docs(&self, project_id: i64) -> anyhow::Result<Vec<DocSummary>>;
    async fn get_doc(&self, project_id: i64, name: &str) -> anyhow::Result<Option<Doc>>;
    // The document and its first patch, atomically. None when another writer
    // created the same name first.
    async fn create_doc(
        &self,
        project_id: i64,
        name: &str,
        summary: Option<&str>,
        body: &str,
        searchable: bool,
        patch: NewPatch,
    ) -> anyhow::Result<Option<(Doc, DocPatch)>>;
    // The new body and its patch, atomically. None when `expected_version` no
    // longer matches — the whole concurrency story for several agents on one
    // document (D1). `summary: None` keeps it.
    async fn update_doc(
        &self,
        doc_id: i64,
        expected_version: i32,
        body: &str,
        summary: Option<Option<&str>>,
        searchable: Option<bool>,
        patch: NewPatch,
    ) -> anyhow::Result<Option<(Doc, DocPatch)>>;

    // Newest first: by the version each patch produced, which is the order
    // the writes won their compare-and-swap in.
    async fn list_patches(&self, doc_id: i64, limit: i64) -> anyhow::Result<Vec<DocPatch>>;
    async fn get_patch(&self, doc_id: i64, pid: &str) -> anyhow::Result<Option<DocPatch>>;

    async fn create_fact(&self, body: &str, tags: &[String], source: Option<&str>) -> anyhow::Result<Fact>;
    async fn get_fact(&self, id: i64) -> anyhow::Result<Option<Fact>>;
    // By provenance — what makes the knowledge_base_notes import re-runnable.
    async fn get_fact_by_source(&self, source: &str) -> anyhow::Result<Option<Fact>>;
    async fn update_fact(&self, id: i64, update: FactUpdate<'_>) -> anyhow::Result<Option<Fact>>;

    // Replaces every row owned by `source_ref`, so a shrunken document leaves
    // no orphaned chunks answering recalls — but only while the source still
    // looks like `basis` (what the entries were built from). Two writes racing
    // on one source finish embedding in either order; the stale one finds the
    // source moved on and writes nothing, leaving the rows to the later
    // write's re-index. Re-indexes of one source run one at a time. Returns
    // whether the rows were written.
    async fn replace_index(
        &self,
        source_ref: &str,
        entries: Vec<IndexUpsert>,
        basis: &IndexBasis,
    ) -> anyhow::Result<bool>;
    async fn search_index(
        &self,
        embedding: &[f32],
        limit: i64,
        states: &[MemoryState],
        tags: &[String],
    ) -> anyhow::Result<Vec<IndexHit>>;
    // Rows whose inline embed failed (provider down), past `after_id` in id
    // order. Drained by the backfill.
    async fn list_unembedded(&self, limit: i64, after_id: i64) -> anyhow::Result<Vec<(i64, String)>>;
    async fn set_embedding(&self, id: i64, embedding: &[f32]) -> anyhow::Result<()>;

    // The journal. One append creates the stream if needed, stores the entry
    // and drops the entries beyond the stream's capacity, oldest first — all
    // in one transaction, serialised per stream, so no reader ever sees a
    // stream over capacity and racing appends all land.
    async fn append_entry(&self, input: AppendEntry<'_>) -> anyhow::Result<AppendedEntry>;
    // Newest first, by append order. Empty for a stream that does not exist.
    async fn tail_entries(&self, stream: &str, limit: i64) -> anyhow::Result<Vec<JournalEntry>>;
    // By name, in byte order.
    async fn list_streams(&self) -> anyhow::Result<Vec<JournalStream>>;
}

pub struct PgMemoryStore {
    pool: PgPool,
}

impl PgMemoryStore {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }
}

fn project_of(r: &tokio_postgres::Row) -> Project {
    Project {
        id: r.get("id"),
        slug: r.get("slug"),
        title: r.get("title"),
        created_at: r.get("created_at"),
        updated_at: r.get("updated_at"),
    }
}

fn doc_of(r: &tokio_postgres::Row) -> Doc {
    let body: String = r.get("body_md");
    Doc {
        searchable: r.get("searchable"),
        id: r.get("id"),
        project_id: r.get("project_id"),
        name: r.get("name"),
        summary: r.get("summary"),
        size_bytes: body.len(),
        body,
        version: r.get("version"),
        updated_at: r.get("updated_at"),
    }
}

fn patch_of(r: &tokio_postgres::Row) -> DocPatch {
    DocPatch {
        pid: r.get("pid"),
        doc_id: r.get("doc_id"),
        kind: PatchKind::parse(r.get("kind")),
        edits: serde_json::from_value(r.get("edits")).unwrap_or_default(),
        body_before: r.get("body_before"),
        version_before: r.get("version_before"),
        version_after: r.get("version_after"),
        actor: r.get("actor"),
        rationale: r.get("rationale"),
        created_at: r.get("created_at"),
    }
}

fn fact_of(r: &tokio_postgres::Row) -> Fact {
    Fact {
        id: r.get("id"),
        body: r.get("body"),
        tags: r.get("tags"),
        source: r.get("source"),
        state: MemoryState::parse(r.get("state")),
        created_at: r.get("created_at"),
        updated_at: r.get("updated_at"),
    }
}

async fn insert_patch(tx: &deadpool_postgres::Transaction<'_>, doc_id: i64, p: &NewPatch) -> anyhow::Result<DocPatch> {
    let row = tx
        .query_one(
            "INSERT INTO memory_doc_patches (doc_id, pid, kind, edits, body_before, version_before, version_after, actor, rationale)
             VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9) RETURNING *",
            &[
                &doc_id,
                &p.pid,
                &p.kind.as_str(),
                &serde_json::to_value(&p.edits)?,
                &p.body_before,
                &p.version_before,
                &p.version_after,
                &p.actor,
                &p.rationale,
            ],
        )
        .await?;
    Ok(patch_of(&row))
}

#[async_trait]
impl MemoryStore for PgMemoryStore {
    async fn create_project(&self, slug: &str, title: &str) -> anyhow::Result<Project> {
        let row = self
            .pool
            .get()
            .await?
            .query_one("INSERT INTO memory_projects (slug, title) VALUES ($1, $2) RETURNING *", &[&slug, &title])
            .await?;
        Ok(project_of(&row))
    }

    async fn get_project(&self, slug: &str) -> anyhow::Result<Option<Project>> {
        let row =
            self.pool.get().await?.query_opt("SELECT * FROM memory_projects WHERE slug = $1 LIMIT 1", &[&slug]).await?;
        Ok(row.as_ref().map(project_of))
    }

    async fn list_projects(&self) -> anyhow::Result<Vec<Project>> {
        let rows = self.pool.get().await?.query("SELECT * FROM memory_projects ORDER BY slug", &[]).await?;
        Ok(rows.iter().map(project_of).collect())
    }

    async fn list_docs(&self, project_id: i64) -> anyhow::Result<Vec<DocSummary>> {
        let rows = self
            .pool
            .get()
            .await?
            .query("SELECT * FROM memory_project_docs WHERE project_id = $1 ORDER BY name", &[&project_id])
            .await?;
        Ok(rows.iter().map(|r| doc_of(r).summary_view()).collect())
    }

    async fn get_doc(&self, project_id: i64, name: &str) -> anyhow::Result<Option<Doc>> {
        let row = self
            .pool
            .get()
            .await?
            .query_opt(
                "SELECT * FROM memory_project_docs WHERE project_id = $1 AND name = $2 LIMIT 1",
                &[&project_id, &name],
            )
            .await?;
        Ok(row.as_ref().map(doc_of))
    }

    async fn create_doc(
        &self,
        project_id: i64,
        name: &str,
        summary: Option<&str>,
        body: &str,
        searchable: bool,
        patch: NewPatch,
    ) -> anyhow::Result<Option<(Doc, DocPatch)>> {
        let mut client = self.pool.get().await?;
        let tx = client.transaction().await?;
        let Some(row) = tx
            .query_opt(
                "INSERT INTO memory_project_docs (project_id, name, summary, body_md, searchable) VALUES ($1, $2, $3, $4, $5)
                 ON CONFLICT DO NOTHING RETURNING *",
                &[&project_id, &name, &summary, &body, &searchable],
            )
            .await?
        else {
            return Ok(None);
        };
        let doc = doc_of(&row);
        let patch = insert_patch(&tx, doc.id, &patch).await?;
        tx.commit().await?;
        Ok(Some((doc, patch)))
    }

    async fn update_doc(
        &self,
        doc_id: i64,
        expected_version: i32,
        body: &str,
        summary: Option<Option<&str>>,
        searchable: Option<bool>,
        patch: NewPatch,
    ) -> anyhow::Result<Option<(Doc, DocPatch)>> {
        // The version in the WHERE clause matches no row once another agent
        // has moved on; the caller reports a conflict instead of overwriting.
        let mut client = self.pool.get().await?;
        let tx = client.transaction().await?;
        let row = tx
            .query_opt(
                "UPDATE memory_project_docs SET body_md = $1,
                 summary = CASE WHEN $2 THEN $3 ELSE summary END,
                 searchable = COALESCE($4, searchable), version = $5, updated_at = now()
             WHERE id = $6 AND version = $7 RETURNING *",
                &[
                    &body,
                    &summary.is_some(),
                    &summary.flatten(),
                    &searchable,
                    &(expected_version + 1),
                    &doc_id,
                    &expected_version,
                ],
            )
            .await?;
        let Some(row) = row else { return Ok(None) };
        let patch = insert_patch(&tx, doc_id, &patch).await?;
        tx.commit().await?;
        Ok(Some((doc_of(&row), patch)))
    }

    async fn list_patches(&self, doc_id: i64, limit: i64) -> anyhow::Result<Vec<DocPatch>> {
        let rows = self
            .pool
            .get()
            .await?
            .query(
                "SELECT * FROM memory_doc_patches WHERE doc_id = $1 ORDER BY version_after DESC, id DESC LIMIT $2",
                &[&doc_id, &limit],
            )
            .await?;
        Ok(rows.iter().map(patch_of).collect())
    }

    async fn get_patch(&self, doc_id: i64, pid: &str) -> anyhow::Result<Option<DocPatch>> {
        let row = self
            .pool
            .get()
            .await?
            .query_opt("SELECT * FROM memory_doc_patches WHERE doc_id = $1 AND pid = $2 LIMIT 1", &[&doc_id, &pid])
            .await?;
        Ok(row.as_ref().map(patch_of))
    }

    async fn create_fact(&self, body: &str, tags: &[String], source: Option<&str>) -> anyhow::Result<Fact> {
        let row = self
            .pool
            .get()
            .await?
            .query_one(
                "INSERT INTO memory_facts (body, tags, source) VALUES ($1, $2, $3) RETURNING *",
                &[&body, &tags, &source],
            )
            .await?;
        Ok(fact_of(&row))
    }

    async fn get_fact(&self, id: i64) -> anyhow::Result<Option<Fact>> {
        let row = self.pool.get().await?.query_opt("SELECT * FROM memory_facts WHERE id = $1 LIMIT 1", &[&id]).await?;
        Ok(row.as_ref().map(fact_of))
    }

    async fn get_fact_by_source(&self, source: &str) -> anyhow::Result<Option<Fact>> {
        let row = self
            .pool
            .get()
            .await?
            .query_opt("SELECT * FROM memory_facts WHERE source = $1 LIMIT 1", &[&source])
            .await?;
        Ok(row.as_ref().map(fact_of))
    }

    async fn update_fact(&self, id: i64, update: FactUpdate<'_>) -> anyhow::Result<Option<Fact>> {
        let row = self
            .pool
            .get()
            .await?
            .query_opt(
                "UPDATE memory_facts SET body = COALESCE($1, body), tags = COALESCE($2, tags),
                        state = COALESCE($3, state), updated_at = now()
                  WHERE id = $4 RETURNING *",
                &[&update.body, &update.tags, &update.state.map(MemoryState::as_str), &id],
            )
            .await?;
        Ok(row.as_ref().map(fact_of))
    }

    async fn replace_index(
        &self,
        source_ref: &str,
        entries: Vec<IndexUpsert>,
        basis: &IndexBasis,
    ) -> anyhow::Result<bool> {
        // Delete-then-insert in one transaction rather than upsert: a
        // re-indexed document can have fewer chunks than before. The source
        // row is locked first, so re-indexes of one source can't interleave
        // their delete and insert (which doubled the rows), and the check sees
        // the source's latest committed state.
        let mut client = self.pool.get().await?;
        let tx = client.transaction().await?;
        let current = match basis {
            IndexBasis::Fact { id, body, tags, state } => tx
                .query_opt("SELECT body, tags, state FROM memory_facts WHERE id = $1 FOR UPDATE", &[id])
                .await?
                .is_some_and(|r| {
                    r.get::<_, String>(0) == *body
                        && r.get::<_, Vec<String>>(1) == *tags
                        && r.get::<_, String>(2) == state.as_str()
                }),
            IndexBasis::Doc { doc_id, version } => tx
                .query_opt("SELECT version FROM memory_project_docs WHERE id = $1 FOR UPDATE", &[doc_id])
                .await?
                .is_some_and(|r| r.get::<_, i32>(0) == *version),
        };
        if !current {
            return Ok(false);
        }
        tx.execute("DELETE FROM memory_index WHERE source_ref = $1", &[&source_ref]).await?;
        for e in &entries {
            let embedding = e.embedding.as_deref().map(vector_literal);
            tx.execute(
                "INSERT INTO memory_index (source_ref, ref, text, tags, actor, state, ts, embedding, embedded_at)
                 VALUES ($1, $2, $3, $4, $5, $6, $7, $8::text::vector, CASE WHEN $8::text IS NULL THEN NULL ELSE now() END)",
                &[&e.source_ref, &e.r#ref, &e.text, &e.tags, &e.actor, &e.state.as_str(), &e.ts, &embedding],
            )
            .await?;
        }
        tx.commit().await?;
        Ok(true)
    }

    async fn search_index(
        &self,
        embedding: &[f32],
        limit: i64,
        states: &[MemoryState],
        tags: &[String],
    ) -> anyhow::Result<Vec<IndexHit>> {
        let states: Vec<&str> = states.iter().map(|s| s.as_str()).collect();
        let tags: Vec<String> = tags.iter().filter(|t| !t.is_empty()).cloned().collect();
        let rows = self
            .pool
            .get()
            .await?
            .query(
                "SELECT id, ref, text, tags, actor, state, ts, (embedding <=> $1::text::vector) AS distance
                   FROM memory_index
                  WHERE embedding IS NOT NULL AND state = ANY($2) AND (cardinality($3::text[]) = 0 OR tags && $3)
                  ORDER BY distance LIMIT $4",
                &[&vector_literal(embedding), &states, &tags, &limit],
            )
            .await?;
        Ok(rows
            .iter()
            .map(|r| IndexHit {
                id: r.get("id"),
                r#ref: r.get("ref"),
                text: r.get("text"),
                tags: r.get("tags"),
                actor: r.get("actor"),
                state: MemoryState::parse(r.get("state")),
                ts: r.get("ts"),
                distance: r.get("distance"),
            })
            .collect())
    }

    async fn list_unembedded(&self, limit: i64, after_id: i64) -> anyhow::Result<Vec<(i64, String)>> {
        let rows = self
            .pool
            .get()
            .await?
            .query(
                "SELECT id, text FROM memory_index WHERE embedding IS NULL AND id > $2 ORDER BY id LIMIT $1",
                &[&limit, &after_id],
            )
            .await?;
        Ok(rows.iter().map(|r| (r.get(0), r.get(1))).collect())
    }

    async fn set_embedding(&self, id: i64, embedding: &[f32]) -> anyhow::Result<()> {
        self.pool
            .get()
            .await?
            .execute(
                "UPDATE memory_index SET embedding = $1::text::vector, embedded_at = now() WHERE id = $2",
                &[&vector_literal(embedding), &id],
            )
            .await?;
        Ok(())
    }

    async fn append_entry(&self, input: AppendEntry<'_>) -> anyhow::Result<AppendedEntry> {
        let mut client = self.pool.get().await?;
        let tx = client.transaction().await?;
        // The upsert always updates, even when the capacity stays, because the
        // row lock it takes is the whole concurrency story: a second append to
        // the stream waits here until the first commits, so its entry id is
        // drawn later and its trim sees the first one's entry. A race to
        // create the stream resolves the same way on the unique name.
        let stream = tx
            .query_one(
                "INSERT INTO memory_journal_streams (name, capacity) VALUES ($1, $2)
                 ON CONFLICT (name) DO UPDATE SET capacity = COALESCE($3, memory_journal_streams.capacity)
                 RETURNING id, name, capacity",
                &[&input.stream, &input.capacity.unwrap_or(input.default_capacity), &input.capacity],
            )
            .await?;
        let (stream_id, name, capacity): (i64, String, i32) = (stream.get(0), stream.get(1), stream.get(2));
        let metadata = input.metadata.map(|m| Value::Object(m.clone()));
        let row = tx
            .query_one(
                "INSERT INTO memory_journal_entries (stream_id, body, actor, metadata) VALUES ($1, $2, $3, $4)
                 RETURNING id, body, actor, metadata, created_at",
                &[&stream_id, &input.body, &input.actor, &metadata],
            )
            .await?;
        tx.execute(
            "DELETE FROM memory_journal_entries WHERE stream_id = $1 AND id NOT IN (
                 SELECT id FROM memory_journal_entries WHERE stream_id = $1 ORDER BY id DESC LIMIT $2)",
            &[&stream_id, &i64::from(capacity)],
        )
        .await?;
        let size: i64 = tx
            .query_one("SELECT count(*) FROM memory_journal_entries WHERE stream_id = $1", &[&stream_id])
            .await?
            .get(0);
        tx.commit().await?;
        let entry = entry_of(&row, &name);
        Ok(AppendedEntry { stream: JournalStream { name, capacity, size, last_at: Some(entry.created_at) }, entry })
    }

    async fn tail_entries(&self, stream: &str, limit: i64) -> anyhow::Result<Vec<JournalEntry>> {
        let rows = self
            .pool
            .get()
            .await?
            .query(
                "SELECT e.id, e.body, e.actor, e.metadata, e.created_at
                   FROM memory_journal_entries e JOIN memory_journal_streams s ON s.id = e.stream_id
                  WHERE s.name = $1 ORDER BY e.id DESC LIMIT $2",
                &[&stream, &limit],
            )
            .await?;
        Ok(rows.iter().map(|r| entry_of(r, stream)).collect())
    }

    async fn list_streams(&self) -> anyhow::Result<Vec<JournalStream>> {
        // Byte order, the order the in-memory store's sort gives — a locale
        // collation would sort "digest/news" around "digest-x".
        let rows = self
            .pool
            .get()
            .await?
            .query(
                "SELECT s.name, s.capacity, count(e.id) AS size, max(e.created_at) AS last_at
                   FROM memory_journal_streams s LEFT JOIN memory_journal_entries e ON e.stream_id = s.id
                  GROUP BY s.id ORDER BY s.name COLLATE \"C\"",
                &[],
            )
            .await?;
        Ok(rows
            .iter()
            .map(|r| JournalStream { name: r.get(0), capacity: r.get(1), size: r.get(2), last_at: r.get(3) })
            .collect())
    }
}

fn entry_of(r: &tokio_postgres::Row, stream: &str) -> JournalEntry {
    let metadata: Option<Value> = r.get("metadata");
    JournalEntry {
        id: r.get("id"),
        stream: stream.to_owned(),
        body: r.get("body"),
        actor: r.get("actor"),
        metadata: match metadata {
            Some(Value::Object(map)) => Some(map),
            _ => None,
        },
        created_at: r.get("created_at"),
    }
}

// ── 6. indexer ───────────────────────────────────────────────────────────────

// Chunk, denormalise, embed — inline after each write. With the provider
// down a row still lands with a NULL vector: reads and patches never depend
// on OpenAI, only searchability may lag (D4).
#[derive(Clone)]
pub struct Indexer {
    store: Arc<dyn MemoryStore>,
    embedder: SharedEmbedder,
    chunk_chars: usize,
}

impl Indexer {
    pub fn new(store: Arc<dyn MemoryStore>, embedder: SharedEmbedder) -> Self {
        Self { store, embedder, chunk_chars: DEFAULT_CHUNK_CHARS }
    }

    async fn embed_texts(&self, texts: &[String]) -> Vec<Option<Vec<f32>>> {
        if texts.is_empty() {
            return Vec::new();
        }
        match self.embedder.embed_batch(texts).await {
            Ok(vectors) => {
                let mut vectors = vectors.into_iter();
                texts.iter().map(|_| vectors.next()).collect()
            }
            Err(err) => {
                tracing::error!(chunks = texts.len(), error = %format!("{err:#}"), "memory index embed failed");
                texts.iter().map(|_| None).collect()
            }
        }
    }

    pub async fn index_doc(&self, project: &Project, doc: &Doc) -> anyhow::Result<()> {
        let chunks = if doc.searchable { chunk_markdown(&doc.body, self.chunk_chars) } else { Vec::new() };
        let texts: Vec<String> =
            chunks.iter().map(|c| build_index_text(&project.title, &doc.name, &c.heading_path, &c.text)).collect();
        let vectors = self.embed_texts(&texts).await;
        let owner = doc_ref(&project.slug, &doc.name, None);
        let entries = texts
            .into_iter()
            .zip(vectors)
            .enumerate()
            .map(|(i, (text, embedding))| IndexUpsert {
                source_ref: owner.clone(),
                r#ref: doc_ref(&project.slug, &doc.name, Some(i)),
                text,
                tags: Vec::new(),
                actor: None,
                // Documents have no lifecycle of their own.
                state: MemoryState::Active,
                ts: doc.updated_at,
                embedding,
            })
            .collect();
        // Always replace, even with nothing: an emptied document must stop
        // answering from its old chunks.
        self.store.replace_index(&owner, entries, &IndexBasis::Doc { doc_id: doc.id, version: doc.version }).await?;
        Ok(())
    }

    pub async fn index_fact(&self, fact: &Fact) -> anyhow::Result<()> {
        let embedding = self.embed_texts(std::slice::from_ref(&fact.body)).await.into_iter().next().flatten();
        let owner = fact_ref(fact.id);
        self.store
            .replace_index(
                &owner,
                vec![IndexUpsert {
                    source_ref: owner.clone(),
                    r#ref: owner.clone(),
                    text: fact.body.clone(),
                    tags: fact.tags.clone(),
                    actor: fact.source.clone(),
                    // Archiving drops a fact from default recall, deleting nothing.
                    state: fact.state,
                    ts: fact.updated_at,
                    embedding,
                }],
                &IndexBasis::Fact { id: fact.id, body: fact.body.clone(), tags: fact.tags.clone(), state: fact.state },
            )
            .await?;
        Ok(())
    }

    // One batch of the rows a failed inline embed left NULL, past `after_id`
    // in id order — see embeddings::drain_missing.
    pub async fn embed_missing_batch(&self, batch: i64, after_id: i64) -> anyhow::Result<BackfillBatch> {
        let rows = self.store.list_unembedded(batch, after_id).await?;
        let store = &self.store;
        backfill_rows(&*self.embedder, &rows, |id, vector| async move { store.set_embedding(id, &vector).await }).await
    }

    // None when the provider is unreachable — recall reports "search is
    // down" rather than an empty memory.
    async fn embed_query(&self, query: &str) -> Option<Vec<f32>> {
        self.embed_texts(&[query.to_owned()]).await.into_iter().next().flatten()
    }
}

// ── 7. service ───────────────────────────────────────────────────────────────

// Expected failures are values, not stack traces: an agent that gets
// `{ error: "version_conflict", currentVersion: 9 }` knows what to do next.
#[derive(Debug, Clone, thiserror::Error)]
#[error("{message}")]
pub struct MemoryError {
    pub code: &'static str,
    pub message: String,
    pub details: Map<String, Value>,
}

fn memory_error(code: &'static str, message: impl Into<String>, details: Value) -> anyhow::Error {
    let details = match details {
        Value::Object(map) => map,
        _ => Map::new(),
    };
    MemoryError { code, message: message.into(), details }.into()
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct WriteResult {
    pub project: String,
    pub doc: String,
    pub version: i32,
    pub patch_id: String,
    pub size_bytes: usize,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct HistoryEntry {
    pub patch_id: String,
    pub kind: PatchKind,
    pub actor: String,
    pub rationale: Option<String>,
    pub version_before: i32,
    pub version_after: i32,
    pub edit_count: usize,
    #[serde(serialize_with = "ser_iso")]
    pub created_at: DateTime<Utc>,
}

#[derive(Debug, Serialize)]
pub struct RecallHit {
    #[serde(rename = "ref")]
    pub r#ref: String,
    pub text: String,
    pub score: f64,
    pub distance: f64,
    pub state: MemoryState,
    pub actor: Option<String>,
    #[serde(serialize_with = "ser_iso")]
    pub ts: DateTime<Utc>,
}

#[derive(Debug, Serialize)]
pub struct DocList {
    pub project: Project,
    pub docs: Vec<DocSummary>,
}

pub struct WriteDoc<'a> {
    // None preserves an existing setting; new documents are searchable.
    pub searchable: Option<bool>,
    pub project: &'a str,
    pub doc: &'a str,
    pub body: &'a str,
    // None keeps the stored summary.
    pub summary: Option<&'a str>,
    // Required once the document exists; absent or 0 when creating it.
    pub expected_version: Option<i32>,
    pub actor: &'a str,
    pub rationale: Option<&'a str>,
}

// The journal's bounds. Capacity is per stream (stored with it); these are
// the defaults and the limits a caller may set within.
pub const JOURNAL_DEFAULT_CAPACITY: i32 = 100;
pub const JOURNAL_MAX_CAPACITY: i64 = 1000;
pub const JOURNAL_TAIL_DEFAULT: i64 = 5;
pub const JOURNAL_TAIL_MAX: i64 = 100;
// A digest is ~5k characters; the cap keeps one runaway write from making
// every later tail expensive.
pub const JOURNAL_MAX_BODY_CHARS: usize = 32_000;
// Metadata is a provenance label (a trace id, a source), not a second body.
pub const JOURNAL_MAX_METADATA_CHARS: usize = 2_000;

// Lengths are counted the way the TS server counts them — UTF-16 code
// units, JavaScript's string length — so both answer a limit alike.
pub fn js_length(s: &str) -> usize {
    s.encode_utf16().count()
}

pub struct JournalAppend<'a> {
    pub stream: &'a str,
    pub body: &'a str,
    pub actor: &'a str,
    // Writer's provenance, stored with the entry and returned by the tail.
    pub metadata: Option<&'a Map<String, Value>>,
    // Stored on the stream; None keeps whatever the stream already has.
    pub capacity: Option<i64>,
}

#[derive(Debug, Serialize, PartialEq)]
pub struct JournalTail {
    pub stream: String,
    pub entries: Vec<JournalEntry>,
}

// append_doc takes no version — it is the version-free safe op — so it
// absorbs a race by re-reading instead of pushing it onto the caller.
const APPEND_CAS_ATTEMPTS: usize = 3;

pub type PatchIdGen = Arc<dyn Fn() -> String + Send + Sync>;

// One write, as `commit` records it.
struct Change<'a> {
    searchable: Option<bool>,
    body: &'a str,
    // None keeps the stored summary.
    summary: Option<Option<&'a str>>,
    kind: PatchKind,
    edits: Vec<Edit>,
    actor: &'a str,
    rationale: Option<String>,
}

#[derive(Clone)]
pub struct MemoryService {
    store: Arc<dyn MemoryStore>,
    indexer: Indexer,
    new_patch_id: PatchIdGen,
}

fn random_patch_id() -> String {
    format!("pa:{:06x}", rand::random::<u32>() & 0xff_ffff)
}

fn normalize_tags(tags: Option<&[String]>) -> Vec<String> {
    crate::knowledge::normalize_tags(tags)
}

impl MemoryService {
    pub fn new(store: Arc<dyn MemoryStore>, embedder: SharedEmbedder) -> Self {
        let indexer = Indexer::new(store.clone(), embedder);
        Self { store, indexer, new_patch_id: Arc::new(random_patch_id) }
    }

    #[cfg(test)]
    fn with_patch_ids(mut self, generator: PatchIdGen) -> Self {
        self.new_patch_id = generator;
        self
    }

    pub fn indexer(&self) -> &Indexer {
        &self.indexer
    }

    pub fn store(&self) -> &Arc<dyn MemoryStore> {
        &self.store
    }

    async fn require_project(&self, slug: &str) -> anyhow::Result<Project> {
        assert_project_slug(slug)?;
        if let Some(project) = self.store.get_project(slug).await? {
            return Ok(project);
        }
        let projects: Vec<String> = self.store.list_projects().await?.into_iter().map(|p| p.slug).collect();
        Err(memory_error("project_not_found", format!("No project \"{slug}\"."), json!({ "projects": projects })))
    }

    async fn require_doc(&self, project: &Project, name: &str) -> anyhow::Result<Doc> {
        assert_doc_name(name)?;
        if let Some(doc) = self.store.get_doc(project.id, name).await? {
            return Ok(doc);
        }
        // Listing what does exist is the cheapest defence against notes.md /
        // notes2.md multiplying (D5).
        let docs: Vec<String> = self.store.list_docs(project.id).await?.into_iter().map(|d| d.name).collect();
        Err(memory_error(
            "doc_not_found",
            format!("No document \"{name}\" in project \"{}\".", project.slug),
            json!({ "project": project.slug, "docs": docs }),
        ))
    }

    // The document is the truth; searchability may lag (D4).
    async fn reindex_doc(&self, project: &Project, doc: &Doc) {
        if let Err(err) = self.indexer.index_doc(project, doc).await {
            tracing::error!(project = project.slug, doc = doc.name, error = %format!("{err:#}"), "memory indexing failed");
        }
    }

    async fn index_fact_quietly(&self, fact: &Fact) {
        if let Err(err) = self.indexer.index_fact(fact).await {
            tracing::error!(fact = fact.id, error = %format!("{err:#}"), "memory fact indexing failed");
        }
    }

    // The shared tail of every mutating op: swap the body under a version
    // check, record the patch, re-index. None when the CAS lost.
    async fn commit(&self, project: &Project, doc: &Doc, change: Change<'_>) -> anyhow::Result<Option<WriteResult>> {
        let patch = NewPatch {
            pid: (self.new_patch_id)(),
            kind: change.kind,
            edits: change.edits,
            body_before: doc.body.clone(),
            version_before: doc.version,
            version_after: doc.version + 1,
            actor: change.actor.to_owned(),
            rationale: change.rationale,
        };
        let Some((updated, patch)) =
            self.store.update_doc(doc.id, doc.version, change.body, change.summary, change.searchable, patch).await?
        else {
            return Ok(None);
        };
        self.reindex_doc(project, &updated).await;
        Ok(Some(write_result(project, &updated, patch.pid)))
    }

    pub async fn create_project(&self, slug: &str, title: &str) -> anyhow::Result<Project> {
        assert_project_slug(slug)?;
        if let Some(existing) = self.store.get_project(slug).await? {
            return Err(memory_error(
                "project_exists",
                format!("Project \"{slug}\" already exists."),
                json!({ "project": existing.slug, "title": existing.title }),
            ));
        }
        let title = if title.trim().is_empty() { slug } else { title.trim() };
        self.store.create_project(slug, title).await
    }

    pub async fn list_projects(&self) -> anyhow::Result<Vec<Project>> {
        self.store.list_projects().await
    }

    pub async fn list_docs(&self, slug: &str) -> anyhow::Result<DocList> {
        let project = self.require_project(slug).await?;
        let docs = self.store.list_docs(project.id).await?;
        Ok(DocList { project, docs })
    }

    pub async fn read_doc(&self, slug: &str, name: &str) -> anyhow::Result<Doc> {
        let project = self.require_project(slug).await?;
        self.require_doc(&project, name).await
    }

    // The creating half of write_doc. `Err(theirs)` when another writer
    // created the same name between our read and our insert.
    async fn create_doc(&self, project: &Project, input: &WriteDoc<'_>) -> anyhow::Result<Result<WriteResult, Doc>> {
        if input.expected_version.is_some_and(|v| v != 0) {
            return Err(memory_error(
                "version_conflict",
                format!("Document \"{}\" does not exist yet; expected_version must be omitted or 0.", input.doc),
                json!({ "project": project.slug, "doc": input.doc, "currentVersion": 0 }),
            ));
        }
        let patch = NewPatch {
            pid: (self.new_patch_id)(),
            kind: PatchKind::Write,
            edits: Vec::new(),
            body_before: String::new(),
            version_before: 0,
            version_after: 1,
            actor: input.actor.to_owned(),
            rationale: input.rationale.map(str::to_owned),
        };
        let Some((created, patch)) = self
            .store
            .create_doc(project.id, input.doc, input.summary, input.body, input.searchable.unwrap_or(true), patch)
            .await?
        else {
            let theirs = self.store.get_doc(project.id, input.doc).await?;
            return Ok(Err(theirs.ok_or_else(|| raced(&project.slug, input.doc))?));
        };
        self.reindex_doc(project, &created).await;
        Ok(Ok(write_result(project, &created, patch.pid)))
    }

    pub async fn write_doc(&self, input: WriteDoc<'_>) -> anyhow::Result<WriteResult> {
        let project = self.require_project(input.project).await?;
        assert_doc_name(input.doc)?;
        let existing = match self.store.get_doc(project.id, input.doc).await? {
            Some(doc) => doc,
            None => match self.create_doc(&project, &input).await? {
                Ok(result) => return Ok(result),
                // Created by someone else in the meantime: this call is now an
                // overwrite of their document, judged as one.
                Err(theirs) => theirs,
            },
        };

        // write_doc is the only op that can lose content: never blind.
        let Some(expected) = input.expected_version else {
            return Err(memory_error(
                "version_required",
                format!(
                    "Document \"{}\" exists at version {}; read it and pass expected_version.",
                    input.doc, existing.version
                ),
                json!({ "project": project.slug, "doc": input.doc, "currentVersion": existing.version }),
            ));
        };
        if expected != existing.version {
            return Err(version_conflict(&project.slug, input.doc, existing.version, expected));
        }
        let committed = self
            .commit(
                &project,
                &existing,
                Change {
                    searchable: input.searchable,
                    body: input.body,
                    summary: input.summary.map(Some),
                    kind: PatchKind::Write,
                    edits: Vec::new(),
                    actor: input.actor,
                    rationale: input.rationale.map(str::to_owned),
                },
            )
            .await?;
        committed.ok_or_else(|| raced(&project.slug, input.doc))
    }

    pub async fn append_doc(
        &self,
        slug: &str,
        name: &str,
        text: &str,
        under_heading: Option<&str>,
        actor: &str,
        rationale: Option<&str>,
    ) -> anyhow::Result<WriteResult> {
        let project = self.require_project(slug).await?;
        for _ in 0..APPEND_CAS_ATTEMPTS {
            let doc = self.require_doc(&project, name).await?;
            let body = append_to_body(&doc.body, text, under_heading).map_err(|headings| {
                memory_error(
                    "heading_not_found",
                    format!("No heading \"{}\" in \"{name}\".", under_heading.unwrap_or_default()),
                    json!({ "project": project.slug, "doc": name, "headings": headings }),
                )
            })?;
            if let Some(result) = self
                .commit(
                    &project,
                    &doc,
                    Change {
                        searchable: None,
                        body: &body,
                        summary: None,
                        kind: PatchKind::Append,
                        edits: Vec::new(),
                        actor,
                        rationale: rationale.map(str::to_owned),
                    },
                )
                .await?
            {
                return Ok(result);
            }
        }
        Err(raced(&project.slug, name))
    }

    pub async fn patch_doc(
        &self,
        slug: &str,
        name: &str,
        expected_version: i32,
        edits: Vec<Edit>,
        actor: &str,
        rationale: Option<&str>,
    ) -> anyhow::Result<WriteResult> {
        let project = self.require_project(slug).await?;
        let doc = self.require_doc(&project, name).await?;
        if doc.version != expected_version {
            return Err(version_conflict(&project.slug, name, doc.version, expected_version));
        }
        let body = apply_edits(&doc.body, &edits).map_err(|failures| edit_failure(&project.slug, name, failures))?;
        let committed = self
            .commit(
                &project,
                &doc,
                Change {
                    searchable: None,
                    body: &body,
                    summary: None,
                    kind: PatchKind::Patch,
                    edits,
                    actor,
                    rationale: rationale.map(str::to_owned),
                },
            )
            .await?;
        committed.ok_or_else(|| raced(&project.slug, name))
    }

    pub async fn history(&self, slug: &str, name: &str, limit: i64) -> anyhow::Result<Vec<HistoryEntry>> {
        let project = self.require_project(slug).await?;
        let doc = self.require_doc(&project, name).await?;
        Ok(self
            .store
            .list_patches(doc.id, limit)
            .await?
            .into_iter()
            .map(|p| HistoryEntry {
                patch_id: p.pid,
                kind: p.kind,
                actor: p.actor,
                rationale: p.rationale,
                version_before: p.version_before,
                version_after: p.version_after,
                edit_count: p.edits.len(),
                created_at: p.created_at,
            })
            .collect())
    }

    pub async fn revert(
        &self,
        slug: &str,
        name: &str,
        patch_id: &str,
        rollback: bool,
        actor: &str,
    ) -> anyhow::Result<WriteResult> {
        let project = self.require_project(slug).await?;
        let doc = self.require_doc(&project, name).await?;
        let Some(patch) = self.store.get_patch(doc.id, patch_id).await? else {
            // The model will hallucinate ids: a hard error listing real ones,
            // never a fuzzy match (D9).
            let known: Vec<String> = self.store.list_patches(doc.id, 10).await?.into_iter().map(|p| p.pid).collect();
            return Err(memory_error(
                "patch_not_found",
                format!("No patch \"{patch_id}\" on \"{name}\"."),
                json!({ "project": project.slug, "doc": name, "knownPatchIds": known }),
            ));
        };
        // Newest = it produced the version the document is at. Judged from the
        // document already read, so the commit's CAS makes it stick.
        let is_newest = patch.version_after == doc.version;

        let body = if rollback || is_newest {
            // Exact by construction: every patch stored the body it replaced.
            patch.body_before.clone()
        } else {
            let inverse = if patch.edits.is_empty() { None } else { invert_edits(&patch.edits) };
            let Some(inverse) = inverse else { return Err(revert_conflict(&project.slug, name, &patch, Vec::new())) };
            apply_edits(&doc.body, &inverse)
                .map_err(|failures| revert_conflict(&project.slug, name, &patch, failures))?
        };
        let rationale = if rollback && !is_newest {
            format!("rollback to v{} (discards patches after {})", patch.version_before, patch.pid)
        } else {
            format!("revert {}", patch.pid)
        };
        let committed = self
            .commit(
                &project,
                &doc,
                Change {
                    searchable: None,
                    body: &body,
                    summary: None,
                    kind: PatchKind::Revert,
                    edits: Vec::new(),
                    actor,
                    rationale: Some(rationale),
                },
            )
            .await?;
        committed.ok_or_else(|| raced(&project.slug, name))
    }

    pub async fn remember(&self, body: &str, tags: Option<&[String]>, source: Option<&str>) -> anyhow::Result<Fact> {
        let body = body.trim();
        if body.is_empty() {
            return Err(memory_error("empty_body", "A fact needs a body.", json!({})));
        }
        let fact = self.store.create_fact(body, &normalize_tags(tags), source).await?;
        self.index_fact_quietly(&fact).await;
        Ok(fact)
    }

    pub async fn get_fact(&self, id: i64) -> anyhow::Result<Fact> {
        self.store
            .get_fact(id)
            .await?
            .ok_or_else(|| memory_error("fact_not_found", format!("No fact {id}."), json!({ "id": id })))
    }

    pub async fn update_fact(
        &self,
        id: i64,
        body: Option<&str>,
        tags: Option<&[String]>,
        state: Option<MemoryState>,
    ) -> anyhow::Result<Fact> {
        let body = body.map(str::trim);
        if body == Some("") {
            return Err(memory_error("empty_body", "A fact needs a body.", json!({})));
        }
        let update = FactUpdate { body, tags: tags.map(|t| normalize_tags(Some(t))), state };
        let updated = self
            .store
            .update_fact(id, update)
            .await?
            .ok_or_else(|| memory_error("fact_not_found", format!("No fact {id}."), json!({ "id": id })))?;
        self.index_fact_quietly(&updated).await;
        Ok(updated)
    }

    pub async fn recall(
        &self,
        query: &str,
        limit: Option<i64>,
        states: Option<&[MemoryState]>,
        tags: Option<&[String]>,
        now: DateTime<Utc>,
    ) -> anyhow::Result<Vec<RecallHit>> {
        let limit = limit.unwrap_or(10).clamp(1, 50);
        let Some(embedding) = self.indexer.embed_query(query).await else {
            // "Search is down" and "we remember nothing" must not look alike:
            // one is worth retrying.
            return Err(memory_error(
                "search_unavailable",
                "Recall needs the embedding provider, which is currently unreachable. Documents still read and patch.",
                json!({}),
            ));
        };
        // Over-fetch so the recency boost has something to reorder.
        let hits = self
            .store
            .search_index(&embedding, limit * 3, states.unwrap_or(&[MemoryState::Active]), tags.unwrap_or_default())
            .await?;
        Ok(rank_hits(hits, now, DEFAULT_RANK)
            .into_iter()
            .take(limit as usize)
            .map(|(hit, score)| RecallHit {
                r#ref: hit.r#ref,
                text: hit.text,
                score,
                distance: hit.distance,
                state: hit.state,
                actor: hit.actor,
                ts: hit.ts,
            })
            .collect())
    }

    // Never indexed, never patched: the journal is records, not knowledge.
    pub async fn journal_append(&self, input: JournalAppend<'_>) -> anyhow::Result<AppendedEntry> {
        require_stream(input.stream)?;
        if input.body.trim().is_empty() {
            return Err(memory_error("empty_body", "A journal entry needs a body.", json!({})));
        }
        let length = js_length(input.body);
        if length > JOURNAL_MAX_BODY_CHARS {
            return Err(memory_error(
                "body_too_large",
                format!("A journal entry is at most {JOURNAL_MAX_BODY_CHARS} characters; this one is {length}."),
                json!({ "max": JOURNAL_MAX_BODY_CHARS, "length": length }),
            ));
        }
        if let Some(capacity) = input.capacity
            && !(1..=JOURNAL_MAX_CAPACITY).contains(&capacity)
        {
            return Err(memory_error(
                "invalid_capacity",
                format!("Capacity is a whole number of entries from 1 to {JOURNAL_MAX_CAPACITY}."),
                json!({ "capacity": capacity, "max": JOURNAL_MAX_CAPACITY }),
            ));
        }
        let metadata_chars = input.metadata.map_or(0, |m| js_length(&serde_json::to_string(m).unwrap_or_default()));
        if metadata_chars > JOURNAL_MAX_METADATA_CHARS {
            return Err(memory_error(
                "metadata_too_large",
                format!(
                    "Journal metadata is at most {JOURNAL_MAX_METADATA_CHARS} characters of JSON; this one is {metadata_chars}."
                ),
                json!({ "max": JOURNAL_MAX_METADATA_CHARS, "length": metadata_chars }),
            ));
        }
        self.store
            .append_entry(AppendEntry {
                stream: input.stream,
                body: input.body,
                actor: input.actor,
                metadata: input.metadata,
                capacity: input.capacity.map(|c| c as i32),
                default_capacity: JOURNAL_DEFAULT_CAPACITY,
            })
            .await
    }

    pub async fn journal_tail(&self, stream: &str, n: Option<i64>) -> anyhow::Result<JournalTail> {
        require_stream(stream)?;
        let n = n.unwrap_or(JOURNAL_TAIL_DEFAULT);
        if !(1..=JOURNAL_TAIL_MAX).contains(&n) {
            return Err(memory_error(
                "invalid_n",
                format!("n is a whole number of entries from 1 to {JOURNAL_TAIL_MAX}."),
                json!({ "n": n, "max": JOURNAL_TAIL_MAX }),
            ));
        }
        Ok(JournalTail { stream: stream.to_owned(), entries: self.store.tail_entries(stream, n).await? })
    }

    pub async fn list_journals(&self) -> anyhow::Result<Vec<JournalStream>> {
        self.store.list_streams().await
    }
}

fn require_stream(stream: &str) -> anyhow::Result<()> {
    if is_journal_stream(stream) {
        return Ok(());
    }
    Err(memory_error(
        "invalid_stream",
        format!(
            "Invalid stream \"{stream}\". Use lowercase letters, digits and hyphens, segments joined by \"/\", \
             at most {JOURNAL_STREAM_MAX_LENGTH} characters, e.g. \"news-digest\" or \"digest/news\"."
        ),
        json!({ "stream": stream }),
    ))
}

fn write_result(project: &Project, doc: &Doc, patch_id: String) -> WriteResult {
    WriteResult {
        project: project.slug.clone(),
        doc: doc.name.clone(),
        version: doc.version,
        patch_id,
        size_bytes: doc.size_bytes,
    }
}

fn version_conflict(project: &str, doc: &str, current: i32, expected: i32) -> anyhow::Error {
    memory_error(
        "version_conflict",
        format!("Document \"{doc}\" is at version {current}, not {expected}. Re-read it and retry."),
        json!({ "project": project, "doc": doc, "currentVersion": current }),
    )
}

fn raced(project: &str, doc: &str) -> anyhow::Error {
    memory_error(
        "version_conflict",
        format!("Document \"{doc}\" changed while this write was in flight. Re-read it and retry."),
        json!({ "project": project, "doc": doc }),
    )
}

fn summarise_failures(failures: &[EditFailure]) -> String {
    failures
        .iter()
        .map(|f| match f.reason {
            EditFailureReason::Empty => format!("edit {}: `old` is empty; use append_doc to add text", f.index),
            EditFailureReason::Ambiguous => {
                format!("edit {}: `old` matches {} times; quote more context", f.index, f.occurrences)
            }
            EditFailureReason::NotFound => {
                let hint = f
                    .suggestions
                    .first()
                    .map(|s| format!("; did you mean: {}", serde_json::to_string(s).unwrap_or_default()))
                    .unwrap_or_default();
                format!("edit {}: `old` not found{hint}", f.index)
            }
        })
        .collect::<Vec<_>>()
        .join("; ")
}

fn edit_failure(project: &str, doc: &str, failures: Vec<EditFailure>) -> anyhow::Error {
    // Nothing was written: the document is exactly as it was.
    memory_error(
        "edit_failed",
        summarise_failures(&failures),
        json!({ "project": project, "doc": doc, "applied": false, "failures": failures }),
    )
}

fn revert_conflict(project: &str, doc: &str, patch: &DocPatch, failures: Vec<EditFailure>) -> anyhow::Error {
    memory_error(
        "revert_conflict",
        format!(
            "Patch {} cannot be undone in place because later patches touched the same text. \
             Retry with rollback=true to restore version {}, discarding everything written after it.",
            patch.pid, patch.version_before
        ),
        json!({ "project": project, "doc": doc, "patchId": patch.pid, "rollbackToVersion": patch.version_before, "failures": failures }),
    )
}

// ── 8. import ────────────────────────────────────────────────────────────────

pub struct LegacyNote {
    pub id: i64,
    pub body: String,
    pub tags: Vec<String>,
}

pub fn legacy_note_source(note_id: i64) -> String {
    format!("knowledge_base_notes:{note_id}")
}

// A copy, not a transformation: the original phrasing is what a later
// structuring pass needs. Provenance in `source` makes it re-runnable.
pub async fn import_legacy_notes(notes: &[LegacyNote], service: &MemoryService) -> anyhow::Result<(usize, usize)> {
    let (mut imported, mut skipped) = (0, 0);
    for note in notes {
        let provenance = legacy_note_source(note.id);
        let body = note.body.trim();
        if service.store.get_fact_by_source(&provenance).await?.is_some() || body.is_empty() {
            skipped += 1;
            continue;
        }
        let fact = service.store.create_fact(body, &note.tags, Some(&provenance)).await?;
        service.index_fact_quietly(&fact).await;
        imported += 1;
    }
    Ok((imported, skipped))
}

// ── 9. tools ─────────────────────────────────────────────────────────────────

// A MemoryError becomes `{ ok: false, error, message, …details }` — a result
// the model can act on. Anything else (a malformed slug, a DB outage) stays a
// tool failure. Payloads are spread flat into `{ ok: true, … }`.
fn run<T: Serialize>(result: anyhow::Result<T>) -> ToolResult {
    match result {
        Ok(value) => {
            let mut out = Map::new();
            out.insert("ok".into(), true.into());
            match serde_json::to_value(value) {
                Ok(Value::Object(fields)) => out.extend(fields),
                Ok(other) => {
                    out.insert("value".into(), other);
                }
                Err(err) => return tool_failed(err),
            }
            json_result(&Value::Object(out))
        }
        Err(err) => match err.downcast::<MemoryError>() {
            Ok(e) => {
                let mut out = Map::new();
                out.insert("ok".into(), false.into());
                out.insert("error".into(), e.code.into());
                out.insert("message".into(), e.message.into());
                out.extend(e.details);
                json_result(&Value::Object(out))
            }
            Err(err) => tool_failed(format!("{err:#}")),
        },
    }
}

fn nonempty_tags(tags: Option<&Vec<String>>, max: usize) -> bool {
    tags.is_none_or(|t| t.len() <= max && t.iter().all(|x| !x.is_empty()))
}

#[derive(Deserialize, schemars::JsonSchema)]
struct RecallParams {
    /// What to look for, in natural language.
    query: String,
    /// Max hits (default 10).
    #[schemars(range(min = 1, max = 50))]
    limit: Option<i64>,
    /// Keep only hits carrying one of these tags.
    tags: Option<Vec<String>>,
}

#[derive(Deserialize, schemars::JsonSchema)]
struct RememberParams {
    /// The fact, as a self-contained sentence including its subject.
    body: String,
    /// 3–6 short lowercase topical tags.
    tags: Option<Vec<String>>,
    /// Optional provenance, e.g. "telegram".
    source: Option<String>,
}

#[derive(Deserialize, schemars::JsonSchema)]
struct GetFactParams {
    /// Fact id, the number in a fact:<id> ref.
    #[schemars(range(min = 1))]
    id: i64,
}

#[derive(Deserialize, schemars::JsonSchema)]
struct UpdateFactParams {
    #[schemars(range(min = 1))]
    id: i64,
    /// Replacement text.
    body: Option<String>,
    /// Replacement tags (not merged).
    tags: Option<Vec<String>>,
    /// active = current, done = finished, archived = out of the way.
    state: Option<MemoryState>,
}

#[derive(Deserialize, schemars::JsonSchema)]
struct ListMemoryParams {
    /// Project slug. Omit to list all projects.
    project: Option<String>,
}

#[derive(Deserialize, schemars::JsonSchema)]
struct JournalAppendParams {
    /// Stream name: lowercase segments joined by "/", e.g. "news-digest" or "digest/news".
    stream: String,
    /// The record, stored exactly as given.
    body: String,
    /// Set the stream's capacity (kept for later appends). Omit to keep the current one.
    #[schemars(range(min = 1, max = 1000))]
    capacity: Option<i64>,
    /// Provenance stored with the entry and returned by journal_tail, e.g. { "trace_id": "…" }. Not an identity: appending the same metadata twice stores two entries.
    metadata: Option<Map<String, Value>>,
}

#[derive(Deserialize, schemars::JsonSchema)]
struct JournalTailParams {
    /// Stream name, e.g. "news-digest".
    stream: String,
    /// How many entries (default 5).
    #[schemars(range(min = 1, max = 100))]
    n: Option<i64>,
}

#[derive(Deserialize, schemars::JsonSchema)]
struct CreateProjectParams {
    /// Lowercase id, hyphens only, e.g. "leetcode-graphs".
    slug: String,
    /// Human-readable name.
    title: String,
}

#[derive(Deserialize, schemars::JsonSchema)]
struct ReadDocParams {
    project: String,
    /// Document filename, e.g. "roadmap.md".
    doc: String,
}

#[derive(Deserialize, schemars::JsonSchema)]
struct AppendDocParams {
    project: String,
    doc: String,
    /// Markdown to append. One blank line is inserted before it.
    text: String,
    /// Append at the end of this section instead of the file, e.g. "Progress".
    under_heading: Option<String>,
    /// The user's own words that prompted this write.
    rationale: Option<String>,
}

#[derive(Deserialize, schemars::JsonSchema)]
struct PatchDocParams {
    project: String,
    doc: String,
    /// Version from the read_doc you just did.
    #[schemars(range(min = 1))]
    expected_version: i32,
    edits: Vec<Edit>,
    /// The user's own words that prompted this change.
    rationale: Option<String>,
}

#[derive(Deserialize, schemars::JsonSchema)]
struct WriteDocParams {
    /// Whether this document participates in recall/embeddings. Default true on creation; omitted preserves it.
    searchable: Option<bool>,
    project: String,
    /// Filename, e.g. "roadmap.md".
    doc: String,
    /// Full markdown content.
    body: String,
    /// One line describing what this document is for.
    summary: Option<String>,
    /// Required when the document already exists. Omit when creating.
    #[schemars(range(min = 0))]
    expected_version: Option<i32>,
    rationale: Option<String>,
}

#[derive(Deserialize, schemars::JsonSchema)]
struct DocHistoryParams {
    project: String,
    doc: String,
    /// Default 20.
    #[schemars(range(min = 1, max = 100))]
    limit: Option<i64>,
}

#[derive(Deserialize, schemars::JsonSchema)]
struct RevertParams {
    project: String,
    doc: String,
    /// Patch id from doc_history, e.g. pa:4f2a1c.
    patch_id: String,
    /// Discard everything after this patch and restore the document as it was before it.
    rollback: Option<bool>,
}

#[tool_router(router = memory_tools, vis = "pub(crate)")]
impl McpTools {
    // Public recall is deliberately active-only: letting the model pick
    // lifecycle states made ordinary queries opt into archived facts and
    // defeated archive as a discovery boundary.
    #[tool(
        name = "recall",
        // The query is sent to the external embedding provider.
        annotations(read_only_hint = true, open_world_hint = true),
        title = "Search everything the agents remember",
        description = "Semantic search across ALL shared memory — project documents and \
            standalone facts alike. Use it whenever the user refers to something \
            from the past (\"what did we have on X\", \"remind me about Y\") and \
            before starting work that might already have a project. Archived facts \
            are intentionally excluded from this ordinary recall path.\n\n\
            Returns REFS, not full content: `doc:<project>/<file>#<chunk>` or \
            `fact:<id>`. Follow the interesting ones with read_doc / get_fact — \
            the snippet is for choosing, the document is for answering."
    )]
    async fn recall(&self, Parameters(p): Parameters<RecallParams>) -> ToolResult {
        if p.query.is_empty()
            || p.limit.is_some_and(|l| !(1..=50).contains(&l))
            || !nonempty_tags(p.tags.as_ref(), usize::MAX)
        {
            return Err(invalid_params("query must be non-empty, limit 1–50, tags non-empty"));
        }
        let memory = &self.deps.memory;
        run(memory
            .recall(&p.query, p.limit, None, p.tags.as_deref(), Utc::now())
            .await
            .map(|hits| json!({ "hits": hits })))
    }

    #[tool(
        name = "remember",
        annotations(open_world_hint = true),
        title = "Store a standalone fact",
        description = "Persist a freeform fact the user asked you to remember (\"remember that …\"). \
            For anything with ongoing progress use a project document instead — a \
            fact is a single self-contained statement, not a running log.\n\n\
            YOU generate the tags: 3–6 short lowercase topical words. Recall runs \
            over the TEXT, so write a body that names its own subject (\"Lyosha \
            pays for the internet on the 1st\", not \"pays on the 1st\")."
    )]
    async fn remember(&self, Parameters(p): Parameters<RememberParams>) -> ToolResult {
        if p.body.is_empty() || !nonempty_tags(p.tags.as_ref(), 12) {
            return Err(invalid_params("body must be non-empty; at most 12 non-empty tags"));
        }
        let memory = &self.deps.memory;
        run(memory.remember(&p.body, p.tags.as_deref(), p.source.as_deref()).await.map(|fact| json!({ "fact": fact })))
    }

    #[tool(
        name = "get_fact",
        annotations(read_only_hint = true, open_world_hint = false),
        title = "Read one fact in full",
        description = "Load a fact by id — the read half of a `fact:<id>` ref returned by recall."
    )]
    async fn get_fact(&self, Parameters(p): Parameters<GetFactParams>) -> ToolResult {
        if p.id < 1 {
            return Err(invalid_params("id must be a positive integer"));
        }
        let memory = &self.deps.memory;
        run(memory.get_fact(p.id).await.map(|fact| json!({ "fact": fact })))
    }

    #[tool(
        name = "update_fact",
        annotations(open_world_hint = true),
        title = "Correct a fact or retire it",
        description = "Change a fact's text, tags or lifecycle state. Nothing is ever deleted: \
            mark a finished or obsolete item `done` / `archived` and it drops out of \
            the default recall while staying findable on request."
    )]
    async fn update_fact(&self, Parameters(p): Parameters<UpdateFactParams>) -> ToolResult {
        if p.id < 1 || p.body.as_deref().is_some_and(str::is_empty) || !nonempty_tags(p.tags.as_ref(), 12) {
            return Err(invalid_params("id must be positive; body non-empty; at most 12 non-empty tags"));
        }
        let memory = &self.deps.memory;
        run(memory
            .update_fact(p.id, p.body.as_deref(), p.tags.as_deref(), p.state)
            .await
            .map(|fact| json!({ "fact": fact })))
    }

    #[tool(
        name = "list_memory",
        annotations(read_only_hint = true, open_world_hint = false),
        title = "List projects, or the documents in one",
        description = "Without `project`: every project, plus every journal stream with its \
            capacity and size. With it: that project's documents, \
            each with a one-line summary, version and size.\n\n\
            ALWAYS call this before creating a document. It is what keeps a \
            project from growing notes.md, notes2.md and progress-new.md until \
            nobody knows which one is current."
    )]
    async fn list_memory(&self, Parameters(p): Parameters<ListMemoryParams>) -> ToolResult {
        let memory = &self.deps.memory;
        match p.project {
            None => run(async {
                let projects = memory.list_projects().await?;
                let journals = memory.list_journals().await?;
                Ok(json!({ "projects": projects, "journals": journals }))
            }
            .await),
            Some(project) => run(memory.list_docs(&project).await),
        }
    }

    #[tool(
        name = "create_project",
        annotations(open_world_hint = false),
        title = "Start a new project",
        description = "Create an empty project — a folder of markdown documents with progress, \
            e.g. preparing for an interview on a topic. Check list_memory first; \
            reusing an existing project is almost always right."
    )]
    async fn create_project(&self, Parameters(p): Parameters<CreateProjectParams>) -> ToolResult {
        if p.slug.is_empty() || p.title.is_empty() {
            return Err(invalid_params("slug and title must be non-empty"));
        }
        let memory = &self.deps.memory;
        run(memory.create_project(&p.slug, &p.title).await.map(|project| json!({ "project": project })))
    }

    #[tool(
        name = "read_doc",
        annotations(read_only_hint = true, open_world_hint = false),
        title = "Read a project document",
        description = "Return a document's full markdown plus its `version`. You need that \
            version to patch it, and the text has to be fresh or your quoted \
            `old` strings won't match. Read immediately before writing."
    )]
    async fn read_doc(&self, Parameters(p): Parameters<ReadDocParams>) -> ToolResult {
        let memory = &self.deps.memory;
        run(memory.read_doc(&p.project, &p.doc).await.map(|doc| json!({ "doc": doc })))
    }

    #[tool(
        name = "append_doc",
        annotations(open_world_hint = true),
        title = "Add text to the end of a document (safe default)",
        description = "Append to a document, or to one section of it. THE PREFERRED WRITE: it \
            cannot destroy existing text and needs no prior read or version.\n\n\
            Use it for anything that is a record of what happened — progress \
            entries, notes, mistakes. Progress is history: correct an old entry by \
            appending a correction, never by editing the past."
    )]
    async fn append_doc(&self, Parameters(p): Parameters<AppendDocParams>) -> ToolResult {
        if p.text.is_empty() {
            return Err(invalid_params("text must be non-empty"));
        }
        let memory = &self.deps.memory;
        let actor = &self.deps.memory_actor;
        run(memory
            .append_doc(&p.project, &p.doc, &p.text, p.under_heading.as_deref(), actor, p.rationale.as_deref())
            .await)
    }

    #[tool(
        name = "patch_doc",
        annotations(open_world_hint = true),
        title = "Change existing text in a document",
        description = "Apply search/replace edits. Each `old` must appear EXACTLY ONCE — copy \
            it verbatim from a fresh read_doc, including punctuation, dashes and \
            ё. `new: \"\"` deletes. All edits apply or none do.\n\n\
            Failures come back with what you need to fix them: `version_conflict` \
            gives the current version, an ambiguous quote gives the match count, \
            and a miss gives the literal from the document that nearly matched."
    )]
    async fn patch_doc(&self, Parameters(p): Parameters<PatchDocParams>) -> ToolResult {
        if p.expected_version < 1 || p.edits.is_empty() || p.edits.iter().any(|e| e.old.is_empty()) {
            return Err(invalid_params("expected_version must be ≥1; edits non-empty, each with a non-empty `old`"));
        }
        let memory = &self.deps.memory;
        let actor = &self.deps.memory_actor;
        run(memory.patch_doc(&p.project, &p.doc, p.expected_version, p.edits, actor, p.rationale.as_deref()).await)
    }

    #[tool(
        name = "write_doc",
        annotations(open_world_hint = true),
        title = "Create a document, or replace one wholesale",
        description = "Create a new document, or overwrite an existing one entirely. THE ONLY \
            OP THAT CAN LOSE CONTENT — prefer append_doc for additions and \
            patch_doc for changes; use this to create, or when the user explicitly \
            asks to rewrite from scratch.\n\n\
            Creating: omit expected_version. Overwriting: read_doc first and pass \
            its version. Always give a `summary` — it is the line other agents see \
            in list_memory."
    )]
    async fn write_doc(&self, Parameters(p): Parameters<WriteDocParams>) -> ToolResult {
        if p.expected_version.is_some_and(|v| v < 0) {
            return Err(invalid_params("expected_version must be ≥0"));
        }
        let memory = &self.deps.memory;
        run(memory
            .write_doc(WriteDoc {
                searchable: p.searchable,
                project: &p.project,
                doc: &p.doc,
                body: &p.body,
                summary: p.summary.as_deref(),
                expected_version: p.expected_version,
                actor: &self.deps.memory_actor,
                rationale: p.rationale.as_deref(),
            })
            .await)
    }

    #[tool(
        name = "doc_history",
        annotations(read_only_hint = true, open_world_hint = false),
        title = "Who changed a document, when and why",
        description = "List a document's patches, newest first: patch id, kind, actor, the \
            rationale recorded at the time, and the versions it moved between. \
            Patch ids come from here — never invent one."
    )]
    async fn doc_history(&self, Parameters(p): Parameters<DocHistoryParams>) -> ToolResult {
        if p.limit.is_some_and(|l| !(1..=100).contains(&l)) {
            return Err(invalid_params("limit must be 1–100"));
        }
        let memory = &self.deps.memory;
        run(memory
            .history(&p.project, &p.doc, p.limit.unwrap_or(20))
            .await
            .map(|history| json!({ "history": history })))
    }

    #[tool(
        name = "revert_patch",
        annotations(open_world_hint = true),
        title = "Undo a recorded change",
        description = "Undo one patch from doc_history. The newest patch always reverts \
            exactly. An older one is undone in place when later patches left its \
            text alone; when they didn't, the call fails and tells you which \
            version a rollback would restore.\n\n\
            `rollback: true` then restores the whole document to the state before \
            that patch, DISCARDING everything written after it — only do that when \
            the user asked for it. Reverting is itself recorded; history is never \
            rewritten."
    )]
    async fn revert_patch(&self, Parameters(p): Parameters<RevertParams>) -> ToolResult {
        if p.patch_id.is_empty() {
            return Err(invalid_params("patch_id must be non-empty"));
        }
        let memory = &self.deps.memory;
        run(memory.revert(&p.project, &p.doc, &p.patch_id, p.rollback == Some(true), &self.deps.memory_actor).await)
    }

    #[tool(
        name = "journal_append",
        annotations(open_world_hint = false),
        title = "Add a record to a journal stream",
        description = "Append one immutable entry to a journal stream — operational records \
            read back by recency, like the last digests sent or a run log. Not \
            for knowledge: entries are never searched by recall, never edited and \
            keep no history. Use a project document or a fact for anything worth \
            finding by meaning.\n\n\
            The stream is created by its first entry. It keeps at most its \
            capacity (100 unless set), dropping the oldest entries first."
    )]
    async fn journal_append(&self, Parameters(p): Parameters<JournalAppendParams>) -> ToolResult {
        if p.stream.is_empty()
            || p.body.is_empty()
            || js_length(&p.body) > JOURNAL_MAX_BODY_CHARS
            || p.capacity.is_some_and(|c| !(1..=JOURNAL_MAX_CAPACITY).contains(&c))
        {
            return Err(invalid_params(
                "stream and body must be non-empty, body at most 32000 characters, capacity 1–1000",
            ));
        }
        let memory = &self.deps.memory;
        run(memory
            .journal_append(JournalAppend {
                stream: &p.stream,
                body: &p.body,
                actor: &self.deps.memory_actor,
                metadata: p.metadata.as_ref(),
                capacity: p.capacity,
            })
            .await)
    }

    #[tool(
        name = "journal_tail",
        annotations(read_only_hint = true, open_world_hint = false),
        title = "Read the newest records of a journal stream",
        description = "Return the last `n` entries of a stream, newest first. A stream nobody \
            has written to reads as an empty list, not an error."
    )]
    async fn journal_tail(&self, Parameters(p): Parameters<JournalTailParams>) -> ToolResult {
        if p.stream.is_empty() || p.n.is_some_and(|n| !(1..=JOURNAL_TAIL_MAX).contains(&n)) {
            return Err(invalid_params("stream must be non-empty, n 1–100"));
        }
        let memory = &self.deps.memory;
        run(memory.journal_tail(&p.stream, p.n).await)
    }
}

// ── tests: an in-memory store, literal enough that passing here means the
// same thing against Postgres ────────────────────────────────────────────────

#[cfg(test)]
pub mod testing {
    use std::sync::Mutex;

    use futures::future::BoxFuture;

    use super::*;

    #[derive(Default)]
    struct State {
        seq: i64,
        projects: Vec<Project>,
        docs: Vec<Doc>,
        patches: Vec<DocPatch>,
        facts: Vec<Fact>,
        index: Vec<(i64, IndexUpsert)>,
        // Name → (capacity, surviving entries in append order).
        streams: std::collections::BTreeMap<String, (i32, Vec<JournalEntry>)>,
    }

    fn describe_stream(name: &str, capacity: i32, entries: &[JournalEntry]) -> JournalStream {
        JournalStream {
            name: name.to_owned(),
            capacity,
            size: entries.len() as i64,
            last_at: entries.last().map(|e| e.created_at),
        }
    }

    pub type Hook = Box<dyn Fn(i64) -> BoxFuture<'static, ()> + Send + Sync>;

    #[derive(Default)]
    pub struct InMemoryStore {
        state: Mutex<State>,
        // Fires just before an update_doc CAS: simulates another agent
        // committing between this one's read and write.
        pub before_update: Mutex<Option<Hook>>,
    }

    impl InMemoryStore {
        fn lock(&self) -> std::sync::MutexGuard<'_, State> {
            self.state.lock().unwrap()
        }
    }

    fn next(state: &mut State) -> i64 {
        state.seq += 1;
        state.seq
    }

    fn record(state: &mut State, doc_id: i64, p: NewPatch) -> DocPatch {
        let patch = DocPatch {
            pid: p.pid,
            doc_id,
            kind: p.kind,
            edits: p.edits,
            body_before: p.body_before,
            version_before: p.version_before,
            version_after: p.version_after,
            actor: p.actor,
            rationale: p.rationale,
            created_at: Utc::now(),
        };
        state.patches.push(patch.clone());
        patch
    }

    fn cosine(a: &[f32], b: &[f32]) -> f64 {
        let dot: f64 = a.iter().zip(b).map(|(x, y)| f64::from(*x) * f64::from(*y)).sum();
        let na: f64 = a.iter().map(|x| f64::from(*x).powi(2)).sum::<f64>().sqrt();
        let nb: f64 = b.iter().map(|x| f64::from(*x).powi(2)).sum::<f64>().sqrt();
        if na == 0.0 || nb == 0.0 { 1.0 } else { 1.0 - dot / (na * nb) }
    }

    #[async_trait]
    impl MemoryStore for InMemoryStore {
        async fn create_project(&self, slug: &str, title: &str) -> anyhow::Result<Project> {
            let mut s = self.lock();
            anyhow::ensure!(!s.projects.iter().any(|p| p.slug == slug), "project {slug} already exists");
            let now = Utc::now();
            let project =
                Project { id: next(&mut s), slug: slug.into(), title: title.into(), created_at: now, updated_at: now };
            s.projects.push(project.clone());
            Ok(project)
        }

        async fn get_project(&self, slug: &str) -> anyhow::Result<Option<Project>> {
            Ok(self.lock().projects.iter().find(|p| p.slug == slug).cloned())
        }

        async fn list_projects(&self) -> anyhow::Result<Vec<Project>> {
            let mut out = self.lock().projects.clone();
            out.sort_by(|a, b| a.slug.cmp(&b.slug));
            Ok(out)
        }

        async fn list_docs(&self, project_id: i64) -> anyhow::Result<Vec<DocSummary>> {
            let mut out: Vec<DocSummary> =
                self.lock().docs.iter().filter(|d| d.project_id == project_id).map(Doc::summary_view).collect();
            out.sort_by(|a, b| a.name.cmp(&b.name));
            Ok(out)
        }

        async fn get_doc(&self, project_id: i64, name: &str) -> anyhow::Result<Option<Doc>> {
            Ok(self.lock().docs.iter().find(|d| d.project_id == project_id && d.name == name).cloned())
        }

        async fn create_doc(
            &self,
            project_id: i64,
            name: &str,
            summary: Option<&str>,
            body: &str,
            searchable: bool,
            patch: NewPatch,
        ) -> anyhow::Result<Option<(Doc, DocPatch)>> {
            let mut s = self.lock();
            if s.docs.iter().any(|d| d.project_id == project_id && d.name == name) {
                return Ok(None);
            }
            let doc = Doc {
                searchable,
                id: next(&mut s),
                project_id,
                name: name.into(),
                summary: summary.map(str::to_owned),
                body: body.into(),
                version: 1,
                size_bytes: body.len(),
                updated_at: Utc::now(),
            };
            s.docs.push(doc.clone());
            let patch = record(&mut s, doc.id, patch);
            Ok(Some((doc, patch)))
        }

        async fn update_doc(
            &self,
            doc_id: i64,
            expected_version: i32,
            body: &str,
            summary: Option<Option<&str>>,
            searchable: Option<bool>,
            patch: NewPatch,
        ) -> anyhow::Result<Option<(Doc, DocPatch)>> {
            let hook = self.before_update.lock().unwrap().as_ref().map(|h| h(doc_id));
            if let Some(hook) = hook {
                hook.await;
            }
            let mut s = self.lock();
            let Some(doc) = s.docs.iter_mut().find(|d| d.id == doc_id) else { return Ok(None) };
            if doc.version != expected_version {
                return Ok(None);
            }
            doc.body = body.into();
            if let Some(searchable) = searchable {
                doc.searchable = searchable;
            }
            if let Some(summary) = summary {
                doc.summary = summary.map(str::to_owned);
            }
            doc.version += 1;
            doc.size_bytes = doc.body.len();
            doc.updated_at = Utc::now();
            let doc = doc.clone();
            let patch = record(&mut s, doc_id, patch);
            Ok(Some((doc, patch)))
        }

        async fn list_patches(&self, doc_id: i64, limit: i64) -> anyhow::Result<Vec<DocPatch>> {
            let mut out: Vec<DocPatch> = self.lock().patches.iter().filter(|p| p.doc_id == doc_id).cloned().collect();
            out.sort_by_key(|p| std::cmp::Reverse(p.version_after));
            out.truncate(limit as usize);
            Ok(out)
        }

        async fn get_patch(&self, doc_id: i64, pid: &str) -> anyhow::Result<Option<DocPatch>> {
            Ok(self.lock().patches.iter().find(|p| p.doc_id == doc_id && p.pid == pid).cloned())
        }

        async fn create_fact(&self, body: &str, tags: &[String], source: Option<&str>) -> anyhow::Result<Fact> {
            let mut s = self.lock();
            let now = Utc::now();
            let fact = Fact {
                id: next(&mut s),
                body: body.into(),
                tags: tags.to_vec(),
                source: source.map(str::to_owned),
                state: MemoryState::Active,
                created_at: now,
                updated_at: now,
            };
            s.facts.push(fact.clone());
            Ok(fact)
        }

        async fn get_fact(&self, id: i64) -> anyhow::Result<Option<Fact>> {
            Ok(self.lock().facts.iter().find(|f| f.id == id).cloned())
        }

        async fn get_fact_by_source(&self, source: &str) -> anyhow::Result<Option<Fact>> {
            Ok(self.lock().facts.iter().find(|f| f.source.as_deref() == Some(source)).cloned())
        }

        async fn update_fact(&self, id: i64, update: FactUpdate<'_>) -> anyhow::Result<Option<Fact>> {
            let mut s = self.lock();
            let Some(fact) = s.facts.iter_mut().find(|f| f.id == id) else { return Ok(None) };
            if let Some(body) = update.body {
                fact.body = body.into();
            }
            if let Some(tags) = update.tags {
                fact.tags = tags;
            }
            if let Some(state) = update.state {
                fact.state = state;
            }
            fact.updated_at = Utc::now();
            Ok(Some(fact.clone()))
        }

        async fn replace_index(
            &self,
            source_ref: &str,
            entries: Vec<IndexUpsert>,
            basis: &IndexBasis,
        ) -> anyhow::Result<bool> {
            let mut s = self.lock();
            let current = match basis {
                IndexBasis::Fact { id, body, tags, state } => s
                    .facts
                    .iter()
                    .find(|f| f.id == *id)
                    .is_some_and(|f| f.body == *body && f.tags == *tags && f.state == *state),
                IndexBasis::Doc { doc_id, version } => {
                    s.docs.iter().find(|d| d.id == *doc_id).is_some_and(|d| d.version == *version)
                }
            };
            if !current {
                return Ok(false);
            }
            s.index.retain(|(_, e)| e.source_ref != source_ref);
            for entry in entries {
                let id = next(&mut s);
                s.index.push((id, entry));
            }
            Ok(true)
        }

        async fn search_index(
            &self,
            embedding: &[f32],
            limit: i64,
            states: &[MemoryState],
            tags: &[String],
        ) -> anyhow::Result<Vec<IndexHit>> {
            let wanted: Vec<&String> = tags.iter().filter(|t| !t.is_empty()).collect();
            let mut hits: Vec<IndexHit> = self
                .lock()
                .index
                .iter()
                .filter(|(_, e)| e.embedding.is_some() && states.contains(&e.state))
                .filter(|(_, e)| wanted.is_empty() || e.tags.iter().any(|t| wanted.contains(&t)))
                .map(|(id, e)| IndexHit {
                    id: *id,
                    r#ref: e.r#ref.clone(),
                    text: e.text.clone(),
                    tags: e.tags.clone(),
                    actor: e.actor.clone(),
                    state: e.state,
                    ts: e.ts,
                    distance: cosine(e.embedding.as_deref().unwrap_or_default(), embedding),
                })
                .collect();
            hits.sort_by(|a, b| a.distance.total_cmp(&b.distance));
            hits.truncate(limit as usize);
            Ok(hits)
        }

        async fn list_unembedded(&self, limit: i64, after_id: i64) -> anyhow::Result<Vec<(i64, String)>> {
            Ok(self
                .lock()
                .index
                .iter()
                .filter(|(id, e)| e.embedding.is_none() && *id > after_id)
                .take(limit as usize)
                .map(|(id, e)| (*id, e.text.clone()))
                .collect())
        }

        async fn set_embedding(&self, id: i64, embedding: &[f32]) -> anyhow::Result<()> {
            if let Some((_, e)) = self.lock().index.iter_mut().find(|(i, _)| *i == id) {
                e.embedding = Some(embedding.to_vec());
            }
            Ok(())
        }

        // One lock from the read to the trim, so an append is atomic here the
        // way the per-stream row lock makes it atomic in Postgres.
        async fn append_entry(&self, input: AppendEntry<'_>) -> anyhow::Result<AppendedEntry> {
            let mut s = self.lock();
            let id = next(&mut s);
            let entry = JournalEntry {
                id,
                stream: input.stream.to_owned(),
                body: input.body.to_owned(),
                actor: input.actor.to_owned(),
                metadata: input.metadata.cloned(),
                created_at: Utc::now(),
            };
            let (capacity, entries) =
                s.streams.entry(input.stream.to_owned()).or_insert_with(|| (input.default_capacity, Vec::new()));
            if let Some(c) = input.capacity {
                *capacity = c;
            }
            entries.push(entry.clone());
            let keep = (*capacity).max(0) as usize;
            if entries.len() > keep {
                entries.drain(..entries.len() - keep);
            }
            let stream = describe_stream(input.stream, *capacity, entries);
            Ok(AppendedEntry { entry, stream })
        }

        async fn tail_entries(&self, stream: &str, limit: i64) -> anyhow::Result<Vec<JournalEntry>> {
            let s = self.lock();
            let Some((_, entries)) = s.streams.get(stream) else { return Ok(Vec::new()) };
            Ok(entries.iter().rev().take(limit.max(0) as usize).cloned().collect())
        }

        async fn list_streams(&self) -> anyhow::Result<Vec<JournalStream>> {
            Ok(self
                .lock()
                .streams
                .iter()
                .map(|(name, (capacity, entries))| describe_stream(name, *capacity, entries))
                .collect())
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};

    use super::testing::InMemoryStore;
    use super::*;
    use crate::embeddings::testing::FakeEmbedder;

    // ── patch ────────────────────────────────────────────────────────────────

    const ROADMAP: &str = "# Roadmap\n\n- [ ] BFS\n- [ ] Dijkstra\n- [ ] A*\n";

    fn edit(old: &str, new: &str) -> Edit {
        Edit { old: old.into(), new: new.into() }
    }

    #[test]
    fn replaces_a_unique_literal_and_deletes_with_empty_new() {
        assert_eq!(
            apply_edits(ROADMAP, &[edit("- [ ] Dijkstra", "- [x] Dijkstra")]).unwrap(),
            "# Roadmap\n\n- [ ] BFS\n- [x] Dijkstra\n- [ ] A*\n"
        );
        assert_eq!(
            apply_edits(ROADMAP, &[edit("- [ ] A*\n", "")]).unwrap(),
            "# Roadmap\n\n- [ ] BFS\n- [ ] Dijkstra\n"
        );
    }

    #[test]
    fn refuses_ambiguous_empty_and_partial_edit_lists() {
        let failures =
            apply_edits("- [ ] review\n- [ ] review\n- [ ] review\n", &[edit("- [ ] review", "x")]).unwrap_err();
        assert_eq!(
            failures,
            vec![EditFailure {
                index: 0,
                old: "- [ ] review".into(),
                reason: EditFailureReason::Ambiguous,
                occurrences: 3,
                suggestions: vec![]
            }]
        );
        assert_eq!(
            apply_edits(ROADMAP, &[edit("- [ ] BFS", "- [x] BFS"), edit("- [ ] Floyd", "x")]).unwrap_err()[0].index,
            1
        );
        assert_eq!(apply_edits(ROADMAP, &[edit("", "anything")]).unwrap_err()[0].reason, EditFailureReason::Empty);
        assert!(apply_edits(ROADMAP, &[]).is_err());
        let both = apply_edits(ROADMAP, &[edit("nope one", "x"), edit("nope two", "y")]).unwrap_err();
        assert_eq!(both.iter().map(|f| f.index).collect::<Vec<_>>(), [0, 1]);
        // Overlapping matches count: "  " could be either pair of "   ".
        // Found by the reference model (tests/model.rs).
        assert_eq!(apply_edits("a   b", &[edit("  ", " ")]).unwrap_err()[0].occurrences, 2);
    }

    #[test]
    fn a_later_edit_may_target_text_an_earlier_one_produced() {
        let out = apply_edits(
            ROADMAP,
            &[edit("- [ ] A*", "- [ ] A*\n- [ ] Bellman-Ford"), edit("- [ ] Bellman-Ford", "- [x] Bellman-Ford")],
        );
        assert_eq!(out.unwrap(), "# Roadmap\n\n- [ ] BFS\n- [ ] Dijkstra\n- [ ] A*\n- [x] Bellman-Ford\n");
    }

    #[test]
    fn near_matches_recover_normalised_quotes() {
        assert_eq!(
            find_near_matches("Goal — finish graphs in a month.\n", "Goal - finish graphs in a month.", 3),
            ["Goal — finish graphs in a month."]
        );
        assert_eq!(
            find_near_matches("- [ ] Walk the graph from Орёл\n", "Walk the graph from Орел", 3),
            ["Walk the graph from Орёл"]
        );
        assert_eq!(find_near_matches("No\n    plan\n", "No plan", 3), ["No\n    plan"]);
        assert_eq!(
            find_near_matches("Project «Graphs» started.\n", "Project \"Graphs\" started.", 3),
            ["Project «Graphs» started."]
        );
        assert_eq!(
            find_near_matches(
                "# Roadmap\n\n- [ ] Dijkstra shortest path\n- [ ] Unrelated topic\n",
                "- [ ] Dijkstra shortest paths",
                3
            ),
            ["- [ ] Dijkstra shortest path"]
        );
        assert!(find_near_matches("# Roadmap\n\n- [ ] BFS\n", "completely unrelated sentence", 3).is_empty());
        let failure = &apply_edits("Goal — finish graphs.\n", &[edit("Goal - finish graphs.", "x")]).unwrap_err()[0];
        assert_eq!(
            (failure.reason, failure.suggestions.clone()),
            (EditFailureReason::NotFound, vec!["Goal — finish graphs.".to_owned()])
        );
    }

    #[test]
    fn inverts_edits_except_deletions() {
        assert_eq!(invert_edits(&[edit("a", "b"), edit("c", "d")]).unwrap(), [edit("d", "c"), edit("b", "a")]);
        assert_eq!(invert_edits(&[edit("gone", "")]), None);
    }

    #[test]
    fn appends_with_stable_spacing_and_under_headings() {
        assert_eq!(
            append_to_body("# Progress\n\nDay 1: BFS\n", "Day 2: Dijkstra", None).unwrap(),
            "# Progress\n\nDay 1: BFS\n\nDay 2: Dijkstra\n"
        );
        assert_eq!(append_to_body("# Progress\n\n\n\n", "Day 1", None).unwrap(), "# Progress\n\nDay 1\n");
        assert_eq!(append_to_body("", "first note", None).unwrap(), "first note\n");
        let body = "# Doc\n\n## Progress\n\nDay 1\n\n## Mistakes\n\nForgot visited set\n";
        assert_eq!(
            append_to_body(body, "Day 2", Some("Progress")).unwrap(),
            "# Doc\n\n## Progress\n\nDay 1\n\nDay 2\n\n## Mistakes\n\nForgot visited set\n"
        );
        let body = "## Progress\n\nDay 1\n\n### Notes\n\nn\n\n## Mistakes\n\nm\n";
        assert_eq!(
            append_to_body(body, "Day 2", Some("## Progress")).unwrap(),
            "## Progress\n\nDay 1\n\n### Notes\n\nn\n\nDay 2\n\n## Mistakes\n\nm\n"
        );
        assert_eq!(
            append_to_body("## Progress\n\nDay 1\n\n## Mistakes\n\nm\n", "x", Some("Roadmap")).unwrap_err(),
            ["## Progress", "## Mistakes"]
        );
    }

    #[test]
    fn lists_headings() {
        assert_eq!(
            list_headings("# A\n\ntext\n\n### B\n#not a heading\n"),
            vec![Heading { level: 1, text: "A".into(), line: 0 }, Heading { level: 3, text: "B".into(), line: 4 }]
        );
    }

    // ── projection ───────────────────────────────────────────────────────────

    fn chunk(text: &str, path: &str) -> MarkdownChunk {
        MarkdownChunk { text: text.into(), heading_path: path.into() }
    }

    #[test]
    fn chunks_at_headings_with_breadcrumbs() {
        let body = "# Project\n\nIntro line.\n\n## Progress\n\nDay 1\n\n### Notes\n\nWatch out\n";
        assert_eq!(
            chunk_markdown(body, DEFAULT_CHUNK_CHARS),
            vec![
                chunk("Intro line.", "Project"),
                chunk("Day 1", "Project > Progress"),
                chunk("Watch out", "Project > Progress > Notes")
            ]
        );
        let paths: Vec<String> = chunk_markdown("## A\n\na\n\n### A1\n\na1\n\n## B\n\nb\n", 1200)
            .into_iter()
            .map(|c| c.heading_path)
            .collect();
        assert_eq!(paths, ["A", "A > A1", "B"]);
    }

    #[test]
    fn packs_and_splits_paragraphs_by_budget() {
        assert_eq!(chunk_markdown("## Log\n\none\n\ntwo\n\nthree\n", 100), vec![chunk("one\n\ntwo\n\nthree", "Log")]);
        let body = ["## Log", &"a".repeat(60), &"b".repeat(60), &"c".repeat(60)].join("\n\n");
        let chunks = chunk_markdown(&body, 100);
        assert_eq!(chunks.len(), 3);
        assert!(chunks.iter().all(|c| c.text.len() <= 100 && c.heading_path == "Log"));
        let lengths: Vec<usize> = chunk_markdown(&"x".repeat(250), 100).iter().map(|c| c.text.len()).collect();
        assert_eq!(lengths, [100, 100, 50]);
        assert_eq!(chunk_markdown("loose note\n", 1200), vec![chunk("loose note", "")]);
        assert!(chunk_markdown("", 1200).is_empty() && chunk_markdown("# Title\n", 1200).is_empty());
    }

    #[test]
    fn index_text_names_its_subject() {
        assert_eq!(
            build_index_text("Graphs for interviews", "progress.md", "Progress", "stuck on Dijkstra"),
            "Graphs for interviews — progress.md · Progress\n\nstuck on Dijkstra"
        );
        assert_eq!(build_index_text("P", "d.md", "", "body"), "P — d.md\n\nbody");
    }

    #[test]
    fn recency_breaks_ties_but_never_beats_relevance() {
        let now = Utc::now();
        let hit = |id: i64, distance: f64, days_ago: f64| IndexHit {
            id,
            r#ref: format!("fact:{id}"),
            text: String::new(),
            tags: vec![],
            actor: None,
            state: MemoryState::Active,
            ts: now - chrono::Duration::milliseconds((days_ago * 86_400_000.0) as i64),
            distance,
        };
        let order = |hits| rank_hits(hits, now, DEFAULT_RANK).into_iter().map(|(h, _)| h.id).collect::<Vec<_>>();
        assert_eq!(order(vec![hit(1, 0.3, 400.0), hit(2, 0.3, 0.0)]), [2, 1]);
        assert_eq!(order(vec![hit(1, 0.10, 400.0), hit(2, 0.40, 0.0)]), [1, 2]);
        let score = |h| rank_hits(vec![h], now, DEFAULT_RANK)[0].1;
        assert!((score(hit(1, 0.5, 0.0)) - 0.45).abs() < 1e-9);
        assert!((score(hit(1, 0.5, 30.0)) - 0.475).abs() < 1e-6);
        assert!((score(hit(1, 0.5, -10.0)) - 0.45).abs() < 1e-9);
    }

    #[test]
    fn refs_parse_strictly() {
        assert_eq!(parse_ref("fact:88"), Some(MemoryRef::Fact { id: 88 }));
        assert_eq!(
            parse_ref("doc:leetcode-graphs/roadmap.md#2"),
            Some(MemoryRef::Doc { project: "leetcode-graphs".into(), doc: "roadmap.md".into(), chunk: Some(2) })
        );
        assert_eq!(parse_ref("doc:Bad/roadmap.md"), None);
        assert_eq!(parse_ref("fact:x"), None);
        assert_eq!(doc_ref("p", "a.md", Some(1)), "doc:p/a.md#1");
    }

    // ── service ──────────────────────────────────────────────────────────────

    const ACTOR: &str = "supervisor";

    struct Harness {
        service: MemoryService,
        store: Arc<InMemoryStore>,
        embedder: Arc<FakeEmbedder>,
    }

    fn harness() -> Harness {
        let store = Arc::new(InMemoryStore::default());
        let embedder = FakeEmbedder::new();
        let seq = Arc::new(AtomicUsize::new(0));
        let service = MemoryService::new(store.clone(), embedder.clone())
            .with_patch_ids(Arc::new(move || format!("pa:{:04x}", seq.fetch_add(1, Ordering::SeqCst) + 1)));
        Harness { service, store, embedder }
    }

    fn code(err: anyhow::Error) -> (&'static str, Map<String, Value>) {
        let e = err.downcast::<MemoryError>().expect("a MemoryError");
        (e.code, e.details)
    }

    fn write<'a>(project: &'a str, doc: &'a str, body: &'a str) -> WriteDoc<'a> {
        WriteDoc {
            searchable: None,
            project,
            doc,
            body,
            summary: None,
            expected_version: None,
            actor: ACTOR,
            rationale: None,
        }
    }

    #[tokio::test]
    async fn round_trips_a_project_and_lists_docs_with_summaries() {
        let h = harness();
        h.service.create_project("leetcode-graphs", "Graphs for interviews").await.unwrap();
        h.service
            .write_doc(WriteDoc {
                searchable: None,
                summary: Some("Project goal and scope"),
                ..write("leetcode-graphs", "passport.md", "# Passport\n\nGoal.\n")
            })
            .await
            .unwrap();
        h.service
            .write_doc(WriteDoc {
                searchable: None,
                summary: Some("List of topics"),
                ..write("leetcode-graphs", "roadmap.md", "# Roadmap\n")
            })
            .await
            .unwrap();

        let list = h.service.list_docs("leetcode-graphs").await.unwrap();
        assert_eq!(list.project.title, "Graphs for interviews");
        let docs: Vec<(String, Option<String>, i32)> =
            list.docs.iter().map(|d| (d.name.clone(), d.summary.clone(), d.version)).collect();
        assert_eq!(
            docs,
            [
                ("passport.md".into(), Some("Project goal and scope".into()), 1),
                ("roadmap.md".into(), Some("List of topics".into()), 1)
            ]
        );
        assert_eq!(h.service.read_doc("leetcode-graphs", "roadmap.md").await.unwrap().body, "# Roadmap\n");
    }

    #[tokio::test]
    async fn appends_without_a_version_and_names_what_exists_on_misses() {
        let h = harness();
        h.service.create_project("p", "P").await.unwrap();
        h.service.write_doc(write("p", "progress.md", "# Progress\n\nDay 1: BFS\n")).await.unwrap();
        let written = h.service.append_doc("p", "progress.md", "Day 2: Dijkstra", None, ACTOR, None).await.unwrap();
        assert_eq!(written.version, 2);
        assert_eq!(
            h.service.read_doc("p", "progress.md").await.unwrap().body,
            "# Progress\n\nDay 1: BFS\n\nDay 2: Dijkstra\n"
        );

        let (c, d) = code(h.service.append_doc("p", "roadmap2.md", "y", None, ACTOR, None).await.unwrap_err());
        assert_eq!((c, &d["docs"]), ("doc_not_found", &json!(["progress.md"])));
        let (c, d) = code(h.service.read_doc("q", "a.md").await.unwrap_err());
        assert_eq!((c, &d["projects"]), ("project_not_found", &json!(["p"])));
        assert!(
            h.service.create_project("Bad Slug", "x").await.unwrap_err().to_string().contains("Invalid project slug")
        );
        assert!(
            h.service
                .write_doc(write("p", "../etc/passwd", "x"))
                .await
                .unwrap_err()
                .to_string()
                .contains("Invalid document name")
        );
        assert_eq!(code(h.service.create_project("p", "Other").await.unwrap_err()).0, "project_exists");
    }

    #[tokio::test]
    async fn write_doc_never_runs_blind() {
        let h = harness();
        h.service.create_project("p", "P").await.unwrap();
        assert_eq!(h.service.write_doc(write("p", "a.md", "one\n")).await.unwrap().version, 1);
        let (c, d) = code(h.service.write_doc(write("p", "a.md", "two\n")).await.unwrap_err());
        assert_eq!((c, &d["currentVersion"]), ("version_required", &json!(1)));
        assert_eq!(h.service.read_doc("p", "a.md").await.unwrap().body, "one\n");
        let ahead = WriteDoc { searchable: None, expected_version: Some(3), ..write("p", "new.md", "x") };
        assert_eq!(code(h.service.write_doc(ahead).await.unwrap_err()).0, "version_conflict");
    }

    #[tokio::test]
    async fn patch_doc_checks_versions_and_leaves_failures_untouched() {
        let h = harness();
        h.service.create_project("p", "P").await.unwrap();
        h.service.write_doc(write("p", "roadmap.md", ROADMAP)).await.unwrap();
        let ok = h
            .service
            .patch_doc("p", "roadmap.md", 1, vec![edit("- [ ] Dijkstra", "- [x] Dijkstra")], ACTOR, Some("mark it"))
            .await;
        assert_eq!(ok.unwrap().version, 2);

        let (c, d) = code(
            h.service.patch_doc("p", "roadmap.md", 1, vec![edit("- [ ] BFS", "x")], ACTOR, None).await.unwrap_err(),
        );
        assert_eq!((c, &d["currentVersion"]), ("version_conflict", &json!(2)));

        let (c, d) = code(
            h.service
                .patch_doc(
                    "p",
                    "roadmap.md",
                    2,
                    vec![edit("- [ ] BFS", "- [x] BFS"), edit("- [ ] Floyd", "x")],
                    ACTOR,
                    None,
                )
                .await
                .unwrap_err(),
        );
        assert_eq!((c, &d["applied"]), ("edit_failed", &json!(false)));
        let doc = h.service.read_doc("p", "roadmap.md").await.unwrap();
        assert_eq!((doc.version, doc.body.contains("- [ ] BFS")), (2, true));
    }

    #[tokio::test]
    async fn append_absorbs_a_write_that_lands_between_read_and_commit() {
        let h = harness();
        h.service.create_project("p", "P").await.unwrap();
        h.service.write_doc(write("p", "log.md", "start\n")).await.unwrap();
        let store = h.store.clone();
        let sneaked = Arc::new(std::sync::atomic::AtomicBool::new(false));
        *h.store.before_update.lock().unwrap() = Some(Box::new(move |doc_id| {
            let (store, sneaked) = (store.clone(), sneaked.clone());
            Box::pin(async move {
                if sneaked.swap(true, Ordering::SeqCst) {
                    return;
                }
                let current = store.get_doc(1, "log.md").await.unwrap().unwrap();
                *store.before_update.lock().unwrap() = None;
                let theirs = NewPatch {
                    pid: "pa:other".into(),
                    kind: PatchKind::Append,
                    edits: Vec::new(),
                    body_before: current.body.clone(),
                    version_before: current.version,
                    version_after: current.version + 1,
                    actor: "other".into(),
                    rationale: None,
                };
                let body = format!("{}\nfrom other agent\n", current.body);
                store.update_doc(doc_id, current.version, &body, None, None, theirs).await.unwrap();
            })
        }));
        h.service.append_doc("p", "log.md", "mine", None, ACTOR, None).await.unwrap();
        let body = h.service.read_doc("p", "log.md").await.unwrap().body;
        assert!(body.contains("from other agent") && body.contains("mine"), "{body}");
    }

    #[tokio::test]
    async fn history_and_revert_follow_the_rules() {
        let h = harness();
        h.service.create_project("p", "P").await.unwrap();
        h.service.write_doc(write("p", "roadmap.md", "- [ ] BFS\n- [ ] Dijkstra\n")).await.unwrap();
        let first = h
            .service
            .patch_doc("p", "roadmap.md", 1, vec![edit("- [ ] BFS", "- [x] BFS")], "claude-code", Some("BFS done"))
            .await
            .unwrap();
        let history = h.service.history("p", "roadmap.md", 20).await.unwrap();
        assert_eq!((history[0].kind, history[0].edit_count, history[1].version_before), (PatchKind::Patch, 1, 0));
        assert!(history[0].patch_id.starts_with("pa:"));

        // Newest reverts exactly, and the revert is itself a patch.
        h.service.revert("p", "roadmap.md", &first.patch_id, false, ACTOR).await.unwrap();
        let doc = h.service.read_doc("p", "roadmap.md").await.unwrap();
        assert_eq!((doc.body.as_str(), doc.version), ("- [ ] BFS\n- [ ] Dijkstra\n", 3));

        // Mid-stack: untouched text reverts in place, an extended line still
        // reverts, a rewritten anchor conflicts and offers a rollback.
        let a =
            h.service.patch_doc("p", "roadmap.md", 3, vec![edit("- [ ] BFS", "- [x] BFS")], ACTOR, None).await.unwrap();
        h.service
            .patch_doc("p", "roadmap.md", 4, vec![edit("- [x] BFS", "- [x] BFS (repeat)")], ACTOR, None)
            .await
            .unwrap();
        h.service.revert("p", "roadmap.md", &a.patch_id, false, ACTOR).await.unwrap();
        assert_eq!(h.service.read_doc("p", "roadmap.md").await.unwrap().body, "- [ ] BFS (repeat)\n- [ ] Dijkstra\n");

        let b = h
            .service
            .patch_doc("p", "roadmap.md", 6, vec![edit("- [ ] Dijkstra", "- [x] Dijkstra")], ACTOR, None)
            .await
            .unwrap();
        h.service
            .patch_doc("p", "roadmap.md", 7, vec![edit("- [x] Dijkstra", "- [x] Shortest paths")], ACTOR, None)
            .await
            .unwrap();
        let (c, d) = code(h.service.revert("p", "roadmap.md", &b.patch_id, false, ACTOR).await.unwrap_err());
        assert_eq!((c, &d["rollbackToVersion"]), ("revert_conflict", &json!(6)));
        h.service.revert("p", "roadmap.md", &b.patch_id, true, ACTOR).await.unwrap();
        assert_eq!(h.service.read_doc("p", "roadmap.md").await.unwrap().body, "- [ ] BFS (repeat)\n- [ ] Dijkstra\n");

        let (c, d) = code(h.service.revert("p", "roadmap.md", "pa:beef", false, ACTOR).await.unwrap_err());
        assert_eq!(c, "patch_not_found");
        assert!(!d["knownPatchIds"].as_array().unwrap().is_empty());
    }

    #[tokio::test]
    async fn an_append_cannot_be_undone_in_place_but_names_the_rollback() {
        let h = harness();
        h.service.create_project("p", "P").await.unwrap();
        h.service.write_doc(write("p", "r.md", "x\n")).await.unwrap();
        let appended = h.service.append_doc("p", "r.md", "- [ ] Floyd", None, ACTOR, None).await.unwrap();
        h.service.append_doc("p", "r.md", "- [ ] Kruskal", None, ACTOR, None).await.unwrap();
        let (c, d) = code(h.service.revert("p", "r.md", &appended.patch_id, false, ACTOR).await.unwrap_err());
        assert_eq!((c, &d["rollbackToVersion"]), ("revert_conflict", &json!(1)));
    }

    #[tokio::test]
    async fn facts_round_trip_and_archive_out_of_recall() {
        let h = harness();
        let fact = h
            .service
            .remember("Lyosha pays for the internet on the 1st", Some(&["internet".into()]), None)
            .await
            .unwrap();
        assert_eq!(h.service.get_fact(fact.id).await.unwrap().body, fact.body);
        assert_eq!(code(h.service.remember("  ", None, None).await.unwrap_err()).0, "empty_body");
        assert_eq!(code(h.service.get_fact(999).await.unwrap_err()).0, "fact_not_found");

        let hits = h.service.recall("internet Lyosha", None, None, None, Utc::now()).await.unwrap();
        assert_eq!(hits[0].r#ref, fact_ref(fact.id));
        h.service.update_fact(fact.id, None, None, Some(MemoryState::Archived)).await.unwrap();
        assert!(h.service.recall("internet Lyosha", None, None, None, Utc::now()).await.unwrap().is_empty());
        let archived =
            h.service.recall("internet Lyosha", None, Some(&[MemoryState::Archived]), None, Utc::now()).await.unwrap();
        assert_eq!(archived.len(), 1);
    }

    #[tokio::test]
    async fn recall_spans_documents_and_drops_stale_chunks() {
        let h = harness();
        h.service.create_project("graphs", "Graphs").await.unwrap();
        h.service.write_doc(write("graphs", "progress.md", "## Progress\n\nstuck on dijkstra\n")).await.unwrap();
        h.service.remember("buy milk and bread", None, None).await.unwrap();
        let hits = h.service.recall("dijkstra progress graphs", Some(1), None, None, Utc::now()).await.unwrap();
        assert_eq!(hits[0].r#ref, "doc:graphs/progress.md#0");

        h.service
            .write_doc(WriteDoc { searchable: None, expected_version: Some(1), ..write("graphs", "progress.md", "") })
            .await
            .unwrap();
        let hits = h.service.recall("dijkstra progress graphs", None, None, None, Utc::now()).await.unwrap();
        assert!(hits.iter().all(|hit| !hit.r#ref.starts_with("doc:")));
    }

    #[tokio::test]
    async fn unsearchable_documents_keep_versions_and_history_and_stay_out_of_recall() {
        let h = harness();
        h.service.create_project("system", "System").await.unwrap();
        h.service
            .write_doc(WriteDoc { searchable: Some(false), ..write("system", "index.md", "alpha") })
            .await
            .unwrap();
        assert!(!h.service.read_doc("system", "index.md").await.unwrap().searchable);
        assert!(h.service.recall("beta gamma alpha", None, None, None, Utc::now()).await.unwrap().is_empty());
        h.service.patch_doc("system", "index.md", 1, vec![edit("alpha", "beta")], ACTOR, None).await.unwrap();
        let doc = h.service.read_doc("system", "index.md").await.unwrap();
        assert_eq!((doc.body.as_str(), doc.version, doc.searchable), ("beta", 2, false));
        assert_eq!(h.service.history("system", "index.md", 10).await.unwrap().len(), 2);
        assert!(h.service.recall("beta gamma alpha", None, None, None, Utc::now()).await.unwrap().is_empty());
        h.service
            .write_doc(WriteDoc {
                searchable: Some(true),
                expected_version: Some(2),
                ..write("system", "index.md", "beta")
            })
            .await
            .unwrap();
        assert!(!h.service.recall("beta gamma alpha", None, None, None, Utc::now()).await.unwrap().is_empty());
        h.service
            .write_doc(WriteDoc {
                searchable: Some(false),
                expected_version: Some(3),
                ..write("system", "index.md", "gamma")
            })
            .await
            .unwrap();
        assert!(
            h.service.recall("beta gamma alpha", None, None, None, Utc::now()).await.unwrap().is_empty(),
            "disabling search removes existing chunks"
        );
    }

    #[tokio::test]
    async fn the_embedder_being_down_degrades_search_not_writes() {
        let h = harness();
        h.embedder.set_down(true);
        h.service.create_project("p", "P").await.unwrap();
        h.service.write_doc(write("p", "a.md", "draft\n")).await.unwrap();
        h.service.patch_doc("p", "a.md", 1, vec![edit("draft", "new draft")], ACTOR, None).await.unwrap();
        assert_eq!(
            code(h.service.recall("draft", None, None, None, Utc::now()).await.unwrap_err()).0,
            "search_unavailable"
        );

        h.embedder.set_down(false);
        let drained = h.service.indexer().embed_missing_batch(100, 0).await.unwrap();
        assert_eq!((drained.embedded, drained.failed), (1, 0));
        assert_eq!(h.service.indexer().embed_missing_batch(100, 0).await.unwrap().embedded, 0);
        assert_eq!(h.service.recall("new draft", None, None, None, Utc::now()).await.unwrap()[0].r#ref, "doc:p/a.md#0");
    }

    #[tokio::test]
    async fn imports_legacy_notes_idempotently() {
        let h = harness();
        let notes = vec![
            LegacyNote { id: 1, body: "router password is on the sticker".into(), tags: vec!["router".into()] },
            LegacyNote { id: 2, body: "   ".into(), tags: vec![] },
        ];
        assert_eq!(import_legacy_notes(&notes, &h.service).await.unwrap(), (1, 1));
        assert_eq!(import_legacy_notes(&notes, &h.service).await.unwrap(), (0, 2));
        let fact = h.store.get_fact_by_source(&legacy_note_source(1)).await.unwrap().unwrap();
        assert_eq!(fact.tags, ["router"]);
    }

    // The same rules against Postgres: the CAS, patch history order, revert,
    // index replacement and recall filters are SQL there, not Rust.
    #[tokio::test]
    async fn pg_store_upholds_the_contract() {
        let Some(pool) = crate::pg::test_pool().await else { return };
        let service = MemoryService::new(Arc::new(PgMemoryStore::new(pool)), FakeEmbedder::with_dims(1536));
        let slug = format!("pg-test-{}", rand::random::<u32>());
        service.create_project(&slug, "PG test").await.unwrap();
        assert_eq!(code(service.create_project(&slug, "again").await.unwrap_err()).0, "project_exists");

        let created = service
            .write_doc(WriteDoc {
                searchable: None,
                summary: Some("roadmap"),
                ..write(&slug, "roadmap.md", "## Topics\n\n- [ ] BFS\n- [ ] Dijkstra\n")
            })
            .await
            .unwrap();
        assert_eq!(created.version, 1);
        let patched = service
            .patch_doc(&slug, "roadmap.md", 1, vec![edit("- [ ] BFS", "- [x] BFS")], ACTOR, Some("BFS done"))
            .await
            .unwrap();
        assert_eq!(
            code(service.patch_doc(&slug, "roadmap.md", 1, vec![edit("x", "y")], ACTOR, None).await.unwrap_err()).0,
            "version_conflict"
        );
        service.append_doc(&slug, "roadmap.md", "- [ ] A*", Some("Topics"), ACTOR, None).await.unwrap();

        let doc = service.read_doc(&slug, "roadmap.md").await.unwrap();
        assert_eq!((doc.version, doc.summary.as_deref()), (3, Some("roadmap")));
        assert_eq!(doc.body, "## Topics\n\n- [x] BFS\n- [ ] Dijkstra\n\n- [ ] A*\n");
        let history = service.history(&slug, "roadmap.md", 10).await.unwrap();
        assert_eq!(
            history.iter().map(|h| h.kind).collect::<Vec<_>>(),
            [PatchKind::Append, PatchKind::Patch, PatchKind::Write]
        );
        assert_eq!(history[1].rationale.as_deref(), Some("BFS done"));

        service.revert(&slug, "roadmap.md", &patched.patch_id, false, ACTOR).await.unwrap();
        assert!(service.read_doc(&slug, "roadmap.md").await.unwrap().body.contains("- [ ] BFS"));

        let hits = service.recall("Dijkstra topics", Some(5), None, None, Utc::now()).await.unwrap();
        assert!(hits.iter().any(|h| h.r#ref == format!("doc:{slug}/roadmap.md#0")));

        let tag = format!("tag{}", rand::random::<u32>());
        let fact =
            service.remember("the cat loves sour cream", Some(std::slice::from_ref(&tag)), Some("test")).await.unwrap();
        let tagged = service
            .recall("cat sour cream", Some(5), None, Some(std::slice::from_ref(&tag)), Utc::now())
            .await
            .unwrap();
        assert_eq!(tagged.iter().map(|h| h.r#ref.clone()).collect::<Vec<_>>(), [fact_ref(fact.id)]);
        service.update_fact(fact.id, Some(" the cat loves cream "), None, Some(MemoryState::Archived)).await.unwrap();
        assert_eq!(service.get_fact(fact.id).await.unwrap().body, "the cat loves cream");
        assert!(
            service
                .recall("cat", Some(5), None, Some(std::slice::from_ref(&tag)), Utc::now())
                .await
                .unwrap()
                .is_empty()
        );
        let archived =
            service.recall("cat", Some(5), Some(&[MemoryState::Archived]), Some(&[tag]), Utc::now()).await.unwrap();
        assert_eq!(archived.len(), 1);

        // Emptying a document drops its chunks from the projection.
        service
            .write_doc(WriteDoc { searchable: None, expected_version: Some(4), ..write(&slug, "roadmap.md", "") })
            .await
            .unwrap();
        let after = service.recall("Dijkstra topics", Some(50), None, None, Utc::now()).await.unwrap();
        assert!(after.iter().all(|h| !h.r#ref.starts_with(&format!("doc:{slug}/"))));
    }

    #[test]
    fn tool_envelope_flattens_payloads_and_errors() {
        let text = |r: ToolResult| {
            serde_json::from_str::<Value>(&r.unwrap().content[0].as_text().expect("text").text).unwrap()
        };
        let ok = text(run(Ok(WriteResult {
            project: "p".into(),
            doc: "a.md".into(),
            version: 2,
            patch_id: "pa:1".into(),
            size_bytes: 3,
        })));
        assert_eq!(
            ok,
            json!({ "ok": true, "project": "p", "doc": "a.md", "version": 2, "patchId": "pa:1", "sizeBytes": 3 })
        );
        let err = text(run::<()>(Err(version_conflict("p", "a.md", 9, 1))));
        assert_eq!(err["ok"], false);
        assert_eq!(err["error"], "version_conflict");
        assert_eq!(err["currentVersion"], 9);
        // A malformed slug is not a MemoryError: it fails the call.
        assert_eq!(run::<()>(Err(anyhow::anyhow!("Invalid project slug"))).unwrap().is_error, Some(true));
    }
}

// ── tests: patch properties ──────────────────────────────────────────────────
// The property half of packages/mcp/src/services/memory/patch.test.ts.

#[cfg(test)]
mod patch_properties {
    use proptest::prelude::*;

    use super::*;

    fn piece() -> impl Strategy<Value = String> {
        prop_oneof![
            "\\PC{0,8}",
            proptest::sample::select(vec!["$&", "$1", "$$", "$`", "$'", "$<n>", "\\"]).prop_map(str::to_owned),
        ]
    }

    fn text() -> impl Strategy<Value = String> {
        proptest::collection::vec(piece(), 0..=6).prop_map(|ps| ps.concat())
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(500))]

        // applyEdits, replacement text: lands verbatim, whatever it contains.
        #[test]
        fn replacement_text_lands_verbatim_whatever_it_contains(before in text(), after in text(), replacement in text()) {
            let marker = "\u{1}unique\u{1}";
            let body = format!("{before}{marker}{after}");
            let result = apply_edits(&body, &[Edit { old: marker.to_owned(), new: replacement.clone() }]);
            prop_assert_eq!(result, Ok(format!("{before}{replacement}{after}")));
        }
    }
}

// ── tests: the journal ───────────────────────────────────────────────────────
// A port of packages/mcp/src/services/memory/journal.test.ts (one test per
// requirement: F = functional, N = non-functional, I = invariant) and
// journal.properties.test.ts (the invariants against a reference model on
// random call sequences). Same names, same examples, same generators.

#[cfg(test)]
mod journal_tests {
    use std::sync::atomic::{AtomicUsize, Ordering};

    use proptest::prelude::*;

    use super::testing::InMemoryStore;
    use super::*;
    use crate::embeddings::Embedder;

    const ACTOR: &str = "supervisor";

    // Counts every call, so N1 can say the journal never embeds.
    #[derive(Default)]
    struct CountingEmbedder {
        calls: AtomicUsize,
    }

    #[async_trait]
    impl Embedder for CountingEmbedder {
        async fn embed_batch(&self, texts: &[String]) -> anyhow::Result<Vec<Vec<f32>>> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            Ok(texts.iter().map(|_| vec![1.0, 0.0, 0.0]).collect())
        }
    }

    fn harness() -> (MemoryService, Arc<CountingEmbedder>) {
        let embedder = Arc::new(CountingEmbedder::default());
        (MemoryService::new(Arc::new(InMemoryStore::default()), embedder.clone()), embedder)
    }

    fn service() -> MemoryService {
        harness().0
    }

    fn code_of<T: std::fmt::Debug>(result: anyhow::Result<T>) -> &'static str {
        result
            .expect_err("expected a MemoryError, the call succeeded")
            .downcast::<MemoryError>()
            .expect("a MemoryError")
            .code
    }

    async fn append(
        service: &MemoryService,
        stream: &str,
        body: &str,
        capacity: Option<i64>,
    ) -> anyhow::Result<AppendedEntry> {
        service.journal_append(JournalAppend { stream, body, actor: ACTOR, metadata: None, capacity }).await
    }

    async fn bodies(service: &MemoryService, stream: &str, n: Option<i64>) -> Vec<String> {
        service.journal_tail(stream, n).await.unwrap().entries.into_iter().map(|e| e.body).collect()
    }

    // ── functional ───────────────────────────────────────────────────────────

    #[tokio::test]
    async fn f1_append_returns_the_entry_and_the_stream_it_landed_in() {
        let service = service();
        let out = append(&service, "news-digest", "digest #1", None).await.unwrap();
        assert_eq!(
            (out.entry.stream.as_str(), out.entry.body.as_str(), out.entry.actor.as_str()),
            ("news-digest", "digest #1", ACTOR)
        );
        assert!(out.entry.id > 0);
        assert_eq!(
            (out.stream.name.as_str(), out.stream.capacity, out.stream.size),
            ("news-digest", JOURNAL_DEFAULT_CAPACITY, 1)
        );
    }

    #[tokio::test]
    async fn f2_tail_returns_the_last_n_entries_newest_first() {
        let service = service();
        for b in ["a", "b", "c", "d"] {
            append(&service, "s", b, None).await.unwrap();
        }
        assert_eq!(bodies(&service, "s", Some(2)).await, ["d", "c"]);
        assert_eq!(bodies(&service, "s", Some(10)).await, ["d", "c", "b", "a"]);
    }

    #[tokio::test]
    async fn f2_tail_defaults_to_5_entries_and_caps_n_at_100() {
        let service = service();
        for i in 0..JOURNAL_TAIL_DEFAULT + 3 {
            append(&service, "s", &format!("e{i}"), None).await.unwrap();
        }
        assert_eq!(bodies(&service, "s", None).await.len() as i64, JOURNAL_TAIL_DEFAULT);
        assert_eq!(code_of(service.journal_tail("s", Some(JOURNAL_TAIL_MAX + 1)).await), "invalid_n");
        assert_eq!(code_of(service.journal_tail("s", Some(0)).await), "invalid_n");
    }

    #[tokio::test]
    async fn f3_a_stream_needs_no_setup_the_first_append_creates_it_a_missing_one_reads_empty() {
        let service = service();
        assert_eq!(
            service.journal_tail("never-written", None).await.unwrap(),
            JournalTail { stream: "never-written".into(), entries: Vec::new() }
        );
        append(&service, "fresh", "x", None).await.unwrap();
        assert_eq!(bodies(&service, "fresh", None).await, ["x"]);
    }

    #[tokio::test]
    async fn f4_capacity_is_a_property_of_the_stream_set_once_kept_by_later_appends() {
        let service = service();
        append(&service, "s", "a", Some(2)).await.unwrap();
        let out = append(&service, "s", "b", None).await.unwrap();
        assert_eq!(out.stream.capacity, 2);
        append(&service, "s", "c", None).await.unwrap();
        assert_eq!(bodies(&service, "s", None).await, ["c", "b"]);
    }

    #[tokio::test]
    async fn f4_lowering_the_capacity_trims_the_stream_on_that_append() {
        let service = service();
        for b in ["a", "b", "c", "d"] {
            append(&service, "s", b, None).await.unwrap();
        }
        let out = append(&service, "s", "e", Some(2)).await.unwrap();
        assert_eq!((out.stream.capacity, out.stream.size), (2, 2));
        assert_eq!(bodies(&service, "s", None).await, ["e", "d"]);
    }

    // The TS test also tries 1.5; an i64 can't hold one, and the tool's
    // schema rejects it before the service (journal_tool_rejects_*).
    #[tokio::test]
    async fn f4_capacity_must_be_an_integer_in_1_to_1000() {
        let service = service();
        for capacity in [0, -1, JOURNAL_MAX_CAPACITY + 1] {
            assert_eq!(code_of(append(&service, "s", "x", Some(capacity)).await), "invalid_capacity");
        }
        assert_eq!(service.list_journals().await.unwrap(), []);
        // Both ends of the range are capacities.
        for capacity in [1, JOURNAL_MAX_CAPACITY] {
            let out = append(&service, &format!("s/{capacity}"), "x", Some(capacity)).await.unwrap();
            assert_eq!(i64::from(out.stream.capacity), capacity);
        }
    }

    #[tokio::test]
    async fn f5_list_journals_names_every_stream_with_its_capacity_size_and_last_write() {
        let service = service();
        append(&service, "news-digest", "a", None).await.unwrap();
        append(&service, "digest/tech", "b", Some(3)).await.unwrap();
        append(&service, "digest/tech", "c", None).await.unwrap();
        let listed: Vec<(String, i32, i64, bool)> = service
            .list_journals()
            .await
            .unwrap()
            .into_iter()
            .map(|s| (s.name, s.capacity, s.size, s.last_at.is_some()))
            .collect();
        assert_eq!(
            listed,
            [("digest/tech".into(), 3, 2, true), ("news-digest".into(), JOURNAL_DEFAULT_CAPACITY, 1, true)]
        );
    }

    #[tokio::test]
    async fn f6_the_actor_is_recorded_on_every_entry() {
        let service = service();
        service
            .journal_append(JournalAppend { stream: "s", body: "x", actor: "chatgpt", metadata: None, capacity: None })
            .await
            .unwrap();
        assert_eq!(service.journal_tail("s", None).await.unwrap().entries[0].actor, "chatgpt");
    }

    #[tokio::test]
    async fn f7_metadata_is_stored_with_the_entry_never_deduplicates_and_is_bounded() {
        let service = service();
        let metadata = json!({ "trace_id": "telegram:7", "source": "telegram" }).as_object().unwrap().clone();
        for (body, metadata) in [("x", Some(&metadata)), ("x", Some(&metadata)), ("y", None)] {
            service
                .journal_append(JournalAppend { stream: "s", body, actor: ACTOR, metadata, capacity: None })
                .await
                .unwrap();
        }
        let entries = service.journal_tail("s", Some(10)).await.unwrap().entries;
        assert_eq!(
            entries.iter().map(|e| e.metadata.clone()).collect::<Vec<_>>(),
            [None, Some(metadata.clone()), Some(metadata)]
        );

        let huge = json!({ "note": "x".repeat(JOURNAL_MAX_METADATA_CHARS) }).as_object().unwrap().clone();
        let result = service
            .journal_append(JournalAppend {
                stream: "s",
                body: "z",
                actor: ACTOR,
                metadata: Some(&huge),
                capacity: None,
            })
            .await;
        assert_eq!(code_of(result), "metadata_too_large");
        assert_eq!(service.journal_tail("s", Some(10)).await.unwrap().entries.len(), 3);
    }

    // ── non-functional ───────────────────────────────────────────────────────

    #[tokio::test]
    async fn n1_writing_and_reading_the_journal_never_calls_the_embedder() {
        let (service, embedder) = harness();
        append(&service, "s", "a digest about Odesa power outages", None).await.unwrap();
        service.journal_tail("s", None).await.unwrap();
        service.list_journals().await.unwrap();
        assert_eq!(embedder.calls.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn n3_a_body_is_non_empty_and_at_most_32000_characters() {
        let service = service();
        assert_eq!(code_of(append(&service, "s", "  \n", None).await), "empty_body");
        assert_eq!(
            code_of(append(&service, "s", &"x".repeat(JOURNAL_MAX_BODY_CHARS + 1), None).await),
            "body_too_large"
        );
        append(&service, "s", &"x".repeat(JOURNAL_MAX_BODY_CHARS), None).await.unwrap();
        assert_eq!(service.journal_tail("s", None).await.unwrap().entries.len(), 1);
    }

    #[tokio::test]
    async fn n3_the_body_is_stored_exactly_as_given() {
        let service = service();
        append(&service, "s", "  line one\n\nline two  \n", None).await.unwrap();
        assert_eq!(bodies(&service, "s", None).await, ["  line one\n\nline two  \n"]);
    }

    // ── invariants (examples) ────────────────────────────────────────────────

    #[tokio::test]
    async fn i3_a_stream_never_holds_more_than_its_capacity_only_the_oldest_leave() {
        let service = service();
        for i in 0..7 {
            append(&service, "s", &format!("e{i}"), Some(3)).await.unwrap();
        }
        assert_eq!(bodies(&service, "s", Some(10)).await, ["e6", "e5", "e4"]);
    }

    #[tokio::test]
    async fn i5_concurrent_appends_all_land_and_the_newest_survive_the_trim() {
        let service = service();
        append(&service, "s", "seed", Some(5)).await.unwrap();
        let results = futures::future::join_all((0..20).map(|i| {
            let service = service.clone();
            async move { append(&service, "s", &format!("p{i}"), None).await.unwrap() }
        }))
        .await;
        let tail = service.journal_tail("s", Some(10)).await.unwrap().entries;
        assert_eq!(tail.len(), 5);
        let mut newest: Vec<i64> = results.iter().map(|r| r.entry.id).collect();
        newest.sort_by(|a, b| b.cmp(a));
        newest.truncate(5);
        assert_eq!(tail.iter().map(|e| e.id).collect::<Vec<_>>(), newest);
    }

    #[tokio::test]
    async fn i6_writing_one_stream_never_trims_another_even_one_sharing_a_prefix() {
        let service = service();
        append(&service, "digest", "keep me", Some(1)).await.unwrap();
        for i in 0..5 {
            append(&service, "digest/news", &format!("n{i}"), Some(1)).await.unwrap();
        }
        assert_eq!(bodies(&service, "digest", None).await, ["keep me"]);
    }

    #[tokio::test]
    async fn i7_recall_never_returns_journal_entries() {
        let service = service();
        append(&service, "s", "Odesa power outages schedule", None).await.unwrap();
        let hits = service.recall("Odesa power outages schedule", None, None, None, Utc::now()).await.unwrap();
        assert!(hits.is_empty());
    }

    #[tokio::test]
    async fn i8_the_journal_is_not_a_project() {
        let service = service();
        append(&service, "s", "x", None).await.unwrap();
        assert_eq!(service.list_projects().await.unwrap(), []);
    }

    #[tokio::test]
    async fn i9_stream_names_lowercase_segments_joined_by_slash_at_most_100_characters() {
        let service = service();
        let long = "x".repeat(100);
        let ok = ["news-digest", "digest/news", "a/b/c-1", long.as_str()];
        for name in ok {
            append(&service, name, "x", None).await.unwrap();
        }
        let too_long = "x".repeat(101);
        for bad in ["", "News", "a//b", "/a", "a/", "-a", "a/-b", "a b", "a.b", too_long.as_str()] {
            assert_eq!(code_of(append(&service, bad, "x", None).await), "invalid_stream", "{bad}");
            assert_eq!(code_of(service.journal_tail(bad, None).await), "invalid_stream", "{bad}");
        }
        let mut listed: Vec<String> = service.list_journals().await.unwrap().into_iter().map(|s| s.name).collect();
        listed.sort();
        let mut expected: Vec<String> = ok.iter().map(|s| s.to_string()).collect();
        expected.sort();
        assert_eq!(listed, expected);
    }

    // ── on Postgres (journal.pg.test.ts) ─────────────────────────────────────
    // The invariants only Postgres can break: append and trim are one
    // transaction (I4), and appends racing on one stream on separate
    // connections all land with the newest `capacity` surviving (I5). Run
    // against TEST_DATABASE_URL; each test writes streams of its own.

    async fn pg_service() -> Option<MemoryService> {
        let pool = crate::pg::test_pool().await?;
        Some(MemoryService::new(Arc::new(PgMemoryStore::new(pool)), Arc::new(CountingEmbedder::default())))
    }

    fn unique(name: &str) -> String {
        format!("{name}-{:08x}", rand::random::<u32>())
    }

    #[tokio::test]
    async fn pg_i5_racing_appends_all_land_and_exactly_the_newest_capacity_survive() {
        let Some(service) = pg_service().await else { return };
        let stream = unique("race");
        let capacity = 5;
        append(&service, &stream, "seed", Some(capacity)).await.unwrap();
        let results = futures::future::join_all((0..40).map(|i| {
            let (service, stream) = (service.clone(), stream.clone());
            tokio::spawn(async move { append(&service, &stream, &format!("p{i}"), None).await.unwrap() })
        }))
        .await;
        let results: Vec<AppendedEntry> = results.into_iter().map(|r| r.unwrap()).collect();
        for r in &results {
            assert!(r.stream.size <= capacity);
        }
        let entries = service.journal_tail(&stream, Some(100)).await.unwrap().entries;
        let mut newest: Vec<i64> = results.iter().map(|r| r.entry.id).collect();
        newest.sort_by(|a, b| b.cmp(a));
        newest.truncate(capacity as usize);
        assert_eq!(entries.iter().map(|e| e.id).collect::<Vec<_>>(), newest);
    }

    #[tokio::test]
    async fn pg_i5_the_first_appends_to_a_brand_new_stream_race_safely_too() {
        let Some(service) = pg_service().await else { return };
        let stream = unique("born-racing");
        let results = futures::future::join_all((0..20).map(|i| {
            let (service, stream) = (service.clone(), stream.clone());
            tokio::spawn(async move { append(&service, &stream, &format!("p{i}"), Some(3)).await.unwrap() })
        }))
        .await;
        assert!(results.iter().all(Result::is_ok));
        let listed: Vec<JournalStream> =
            service.list_journals().await.unwrap().into_iter().filter(|s| s.name == stream).collect();
        assert_eq!(listed.iter().map(|s| (s.capacity, s.size)).collect::<Vec<_>>(), [(3, 3)]);
    }

    #[tokio::test]
    async fn pg_i2_tail_is_newest_first_and_round_trips_the_body_byte_for_byte() {
        let Some(service) = pg_service().await else { return };
        let stream = unique("order");
        let body = "  multi\nline «body» ё  \n";
        append(&service, &stream, "older", None).await.unwrap();
        append(&service, &stream, body, None).await.unwrap();
        assert_eq!(bodies(&service, &stream, Some(2)).await, [body, "older"]);
    }

    #[tokio::test]
    async fn pg_f4_lowering_the_capacity_trims_on_that_append() {
        let Some(service) = pg_service().await else { return };
        let stream = unique("shrink");
        for i in 0..6 {
            append(&service, &stream, &format!("e{i}"), None).await.unwrap();
        }
        let out = append(&service, &stream, "last", Some(2)).await.unwrap();
        assert_eq!((out.stream.capacity, out.stream.size), (2, 2));
        assert_eq!(bodies(&service, &stream, Some(10)).await, ["last", "e5"]);
    }

    // ── properties against the reference model ───────────────────────────────

    fn cases() -> u32 {
        std::env::var("MODEL_CASES").ok().and_then(|v| v.parse().ok()).unwrap_or(300)
    }

    fn block_on<F: std::future::Future>(future: F) -> F::Output {
        tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap().block_on(future)
    }

    // Few streams, one a prefix of another, so sequences actually collide (I6).
    const STREAMS: [&str; 3] = ["digest", "digest/news", "log"];

    #[derive(Debug, Clone)]
    enum Op {
        Append { stream: usize, body: String, capacity: Option<i64> },
        Tail { stream: usize, n: i64 },
    }

    // fast-check's default string: printable ASCII.
    fn body() -> impl Strategy<Value = String> {
        "[ -~]{1,12}".prop_filter("not blank", |s| !s.trim().is_empty())
    }

    // Small capacities so trimming happens within a 30-step sequence.
    fn op() -> impl Strategy<Value = Op> {
        prop_oneof![
            3 => (0..STREAMS.len(), body(), proptest::option::of(1i64..=6))
                .prop_map(|(stream, body, capacity)| Op::Append { stream, body, capacity }),
            1 => (0..STREAMS.len(), 1i64..=10).prop_map(|(stream, n)| Op::Tail { stream, n }),
        ]
    }

    // The reference model: per stream, a capacity and the surviving entries in
    // append order. Entries are the service's own return values, so comparing
    // them checks immutability (I1) for free.
    struct ModelStream {
        capacity: i32,
        entries: Vec<JournalEntry>,
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(cases()))]

        // I1, I2, I3, I6
        #[test]
        fn behaves_like_a_bounded_list_per_stream(ops in proptest::collection::vec(op(), 0..=30)) {
            block_on(async {
                let service = service();
                let mut model: std::collections::BTreeMap<&str, ModelStream> = Default::default();
                for op in ops {
                    match op {
                        Op::Append { stream, body, capacity } => {
                            let stream = STREAMS[stream];
                            let out = append(&service, stream, &body, capacity).await.unwrap();
                            let m = model.entry(stream).or_insert(ModelStream { capacity: JOURNAL_DEFAULT_CAPACITY, entries: Vec::new() });
                            if let Some(c) = capacity {
                                m.capacity = c as i32;
                            }
                            m.entries.push(out.entry.clone());
                            let keep = m.capacity as usize;
                            if m.entries.len() > keep {
                                m.entries.drain(..m.entries.len() - keep);
                            }
                            assert_eq!(out.entry.body, body);
                            assert_eq!((out.stream.name.as_str(), out.stream.capacity, out.stream.size), (stream, m.capacity, m.entries.len() as i64));
                        }
                        Op::Tail { stream, n } => {
                            let stream = STREAMS[stream];
                            let entries = service.journal_tail(stream, Some(n)).await.unwrap().entries;
                            let expected: Vec<JournalEntry> = model.get(stream).map_or_else(Vec::new, |m| m.entries.iter().rev().take(n as usize).cloned().collect());
                            assert_eq!(entries, expected);
                        }
                    }
                }

                // The whole state, not just what the sequence happened to tail.
                for name in STREAMS {
                    let m = model.get(name);
                    let entries = service.journal_tail(name, Some(100)).await.unwrap().entries;
                    let expected: Vec<JournalEntry> = m.map_or_else(Vec::new, |m| m.entries.iter().rev().cloned().collect());
                    assert_eq!(entries, expected);
                    assert!(entries.len() <= m.map_or(0, |m| m.capacity as usize));
                }
                let mut listed: Vec<(String, i32, i64)> = service.list_journals().await.unwrap().into_iter().map(|s| (s.name, s.capacity, s.size)).collect();
                listed.sort();
                let expected: Vec<(String, i32, i64)> = model.iter().map(|(name, m)| (name.to_string(), m.capacity, m.entries.len() as i64)).collect();
                assert_eq!(listed, expected);
            });
        }

        // I5
        #[test]
        fn keeps_the_newest_capacity_entries_when_appends_race(capacity in 1i64..=8, count in 1usize..=25) {
            block_on(async {
                let service = service();
                append(&service, "race", "seed", Some(capacity)).await.unwrap();
                let results = futures::future::join_all((0..count).map(|i| {
                    let service = service.clone();
                    async move { append(&service, "race", &format!("p{i}"), None).await.unwrap() }
                }))
                .await;
                let entries = service.journal_tail("race", Some(100)).await.unwrap().entries;
                let mut appended: Vec<i64> = results.iter().map(|r| r.entry.id).collect();
                assert_eq!(appended.iter().collect::<std::collections::HashSet<_>>().len(), count);
                assert_eq!(entries.len(), (capacity as usize).min(count + 1));
                if count >= capacity as usize {
                    appended.sort_by(|a, b| b.cmp(a));
                    appended.truncate(capacity as usize);
                    assert_eq!(entries.iter().map(|e| e.id).collect::<Vec<_>>(), appended);
                }
            });
        }

        // I9
        #[test]
        fn accepts_exactly_the_names_the_grammar_allows_and_a_rejected_call_writes_nothing(
            stream in prop_oneof![
                "[a-z0-9][a-z0-9-]*(/[a-z0-9][a-z0-9-]*)*".prop_filter("≤ 100", |s| s.len() <= 100),
                "[ -~]{0,20}",
                "[a/\\-1A.]{0,8}",
            ]
        ) {
            block_on(async {
                let service = service();
                let valid = regex::Regex::new(JOURNAL_STREAM_RE).unwrap().is_match(&stream) && stream.len() <= 100;
                match append(&service, &stream, "x", None).await {
                    Ok(_) => {
                        assert!(valid);
                        let names: Vec<String> = service.list_journals().await.unwrap().into_iter().map(|s| s.name).collect();
                        assert_eq!(names, std::slice::from_ref(&stream));
                    }
                    Err(err) => {
                        assert_eq!(err.downcast::<MemoryError>().expect("a MemoryError").code, "invalid_stream");
                        assert!(!valid);
                        assert_eq!(service.list_journals().await.unwrap(), []);
                    }
                }
            });
        }
    }
}

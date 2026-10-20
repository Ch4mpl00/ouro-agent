// Golden-set eval of the news-digest significance filter: a typed decision
// (Jev, one call per post) against a reference labelling (opus.json, written
// by an Opus agent from the same interests rubric). See
// .claude/tasks/jev-system-one.md, candidate 1.
//
//   news-filter-eval jev [--batch N] [--concurrency 8] [--model typesafe/jev-1.13]
//   news-filter-eval compare [--run jev.json]
//   news-filter-eval pipeline        # filter + covered + dedup in one pass
//   news-filter-eval compare-dedup [--covered 0.5]
//   news-filter-eval filter          # the shipped path: filter preset +
//                                    # the planner anchor's prompt over
//                                    # list_news-shaped items → jev-filter.json
//
// --batch N packs N posts into one request: the rubric goes into `state` once
// and each post gets its own short `noul` question, so the rubric is paid for
// once per batch instead of once per post. Writes jev-batchN.json.
//
// Fixture dir: eval-fixtures/news-filter/ under the repo root (gitignored)
//   items.json     the posts {id, channel, posted_at, text}
//   interests.md   the rubric both labellers get
//   opus.json      reference labels  (from the Opus agent)
//   jev.json       Jev's answers     (written by `jev`)
//   report.md      the comparison    (written by `compare`)
use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::Instant;

use agent::decisions::{ChoiceAnswer, DecisionClient, DecisionRequest, NoulAnswer, Question, choice, filter, noul};
use agent::models::DEFAULT_DECISION_MODEL;
use agent::providers::{JsonApi, openrouter_api};
use agent::tracing::null_scope;
use async_trait::async_trait;
use evals::env::{load_env, root};
use futures::StreamExt;
use regex::Regex;
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value, json};
use tokio_util::sync::CancellationToken;

fn dir() -> anyhow::Result<PathBuf> {
    Ok(root()?.join("eval-fixtures/news-filter"))
}

fn flag(name: &str) -> Option<String> {
    let args: Vec<String> = std::env::args().collect();
    let i = args.iter().position(|a| a == name)?;
    args.get(i + 1).cloned()
}

fn read_json<T: for<'de> Deserialize<'de>>(name: &str) -> anyhow::Result<T> {
    let path = dir()?.join(name);
    let text = std::fs::read_to_string(&path).map_err(|e| anyhow::anyhow!("{}: {e}", path.display()))?;
    Ok(serde_json::from_str(&text)?)
}

fn write_json(name: &str, value: &impl Serialize) -> anyhow::Result<()> {
    // JSON.stringify(run, null, 1): one-space indent.
    let mut out = Vec::new();
    let mut ser = serde_json::Serializer::with_formatter(&mut out, serde_json::ser::PrettyFormatter::with_indent(b" "));
    value.serialize(&mut ser)?;
    out.push(b'\n');
    std::fs::write(dir()?.join(name), out)?;
    Ok(())
}

#[derive(Clone, Debug, Deserialize)]
struct Item {
    id: i64,
    channel: Option<String>,
    #[allow(dead_code)]
    posted_at: String,
    text: String,
}

#[derive(Clone, Debug, Deserialize)]
struct OpusLabel {
    id: i64,
    keep: bool,
    confidence: String,
    category: String,
    reason: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct JevResult {
    id: i64,
    probability: f64,
    latency_ms: f64,
    cost_usd: f64,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct JevRun {
    model: String,
    concurrency: usize,
    #[serde(default = "one")]
    batch: usize,
    wall_ms: f64,
    cost_usd: f64,
    input_tokens: u64,
    results: Vec<JevResult>,
}

// (post id, P(keep)) for each post a call answered.
type Answers = Vec<(i64, f64)>;

// One call: its latency, cost, input tokens and answers.
struct BatchRun {
    latency_ms: f64,
    cost_usd: f64,
    input_tokens: u64,
    answers: Answers,
}

fn one() -> usize {
    1
}

fn decisions(model: &str, api: Arc<dyn JsonApi>) -> DecisionClient {
    DecisionClient::with_retry(model.to_owned(), api, agent::providers::RetryOpts::default())
}

fn router_key() -> anyhow::Result<String> {
    std::env::var("OPEN_ROUTER_API_KEY")
        .ok()
        .filter(|k| !k.is_empty())
        .ok_or_else(|| anyhow::anyhow!("OPEN_ROUTER_API_KEY is not set"))
}

fn request(state: Value) -> DecisionRequest {
    DecisionRequest::new(state, null_scope(), CancellationToken::new())
}

// ─── jev ─────────────────────────────────────────────────────────────

async fn run_jev() -> anyhow::Result<()> {
    let key = router_key()?;
    let model = flag("--model").unwrap_or_else(|| DEFAULT_DECISION_MODEL.into());
    let concurrency: usize = flag("--concurrency").and_then(|c| c.parse().ok()).unwrap_or(8);
    let items: Vec<Item> = read_json("items.json")?;
    let rubric = std::fs::read_to_string(dir()?.join("interests.md"))?;
    let client = decisions(&model, Arc::new(openrouter_api(&key)));
    let batch: usize = flag("--batch").and_then(|b| b.parse().ok()).unwrap_or(1);
    let batches: Vec<Vec<Item>> = items.chunks(batch.max(1)).map(<[Item]>::to_vec).collect();

    // One post per call: the rubric rides in the question. Several posts per
    // call: the rubric and the posts ride in `state`, one short question each.
    let decide_batch = async |posts: Vec<Item>| -> anyhow::Result<(Answers, Option<agent::decisions::Decision>)> {
        if batch == 1 {
            let Some(item) = posts.first() else { return Ok((Vec::new(), None)) };
            let keep = noul(
                &format!(
                    "This post is worth including in the reader's digest according to the selection rules.\n\n{rubric}"
                ),
                Some(("Include", "Skip")),
            );
            let decision = client
                .decide(request(json!({ "channel": item.channel, "text": item.text })).question("keep", &keep))
                .await?;
            let p = decision.answer("keep", &keep)?.probability;
            return Ok((vec![(item.id, p)], Some(decision)));
        }
        let questions: Vec<Question<NoulAnswer>> = (0..posts.len())
            .map(|i| {
                noul(
                    &format!(
                        "Post #{} in posts is worth including in the reader's digest according to the selection rules in rules.",
                        i + 1
                    ),
                    Some(("Include", "Skip")),
                )
            })
            .collect();
        let state = json!({
            "rules": rubric,
            "posts": posts.iter().enumerate().map(|(i, item)| json!({ "n": i + 1, "channel": item.channel, "text": item.text })).collect::<Vec<_>>(),
        });
        let mut req = request(state);
        for (i, q) in questions.iter().enumerate() {
            req = req.question(&format!("post_{}", i + 1), q);
        }
        let decision = client.decide(req).await?;
        let answers = posts
            .iter()
            .enumerate()
            .map(|(i, item)| Ok((item.id, decision.answer(&format!("post_{}", i + 1), &questions[i])?.probability)))
            .collect::<anyhow::Result<Vec<_>>>()?;
        Ok((answers, Some(decision)))
    };

    let started = Instant::now();
    let per_batch: Vec<BatchRun> = futures::stream::iter(batches.iter().cloned())
        .map(|posts| async {
            let t0 = Instant::now();
            let first = posts.first().map(|p| p.id).unwrap_or_default();
            let n = posts.len();
            let (answers, decision) = decide_batch(posts).await?;
            let latency_ms = t0.elapsed().as_millis() as f64;
            let tokens = decision.as_ref().and_then(|d| d.usage.as_ref()).map_or(0, |u| u.input);
            println!("{n} post(s) from {first}  {latency_ms}ms  {tokens} tok");
            let cost_usd = decision.as_ref().and_then(|d| d.cost_usd).unwrap_or(0.0);
            anyhow::Ok(BatchRun { latency_ms, cost_usd, input_tokens: tokens, answers })
        })
        .buffered(concurrency.max(1))
        .collect::<Vec<_>>()
        .await
        .into_iter()
        .collect::<anyhow::Result<_>>()?;

    let run = JevRun {
        model,
        concurrency,
        batch,
        wall_ms: started.elapsed().as_millis() as f64,
        cost_usd: per_batch.iter().map(|b| b.cost_usd).sum(),
        input_tokens: per_batch.iter().map(|b| b.input_tokens).sum(),
        // Per-post cost and latency are the batch's, split evenly.
        results: per_batch
            .iter()
            .flat_map(|b| {
                b.answers.iter().map(move |(id, p)| JevResult {
                    id: *id,
                    probability: *p,
                    latency_ms: b.latency_ms,
                    cost_usd: b.cost_usd / b.answers.len() as f64,
                })
            })
            .collect(),
    };
    let out = if batch == 1 { "jev.json".to_owned() } else { format!("jev-batch{batch}.json") };
    write_json(&out, &run)?;
    println!(
        "\n{} posts in {} call(s), {}ms, ${:.6}, {} input tokens → {out}",
        items.len(),
        batches.len(),
        run.wall_ms,
        run.cost_usd,
        run.input_tokens
    );
    Ok(())
}

// ─── compare ─────────────────────────────────────────────────────────

struct Scores {
    threshold: f64,
    tp: usize,
    fp: usize,
    fn_: usize,
    tn: usize,
    accuracy: f64,
    precision: f64,
    recall: f64,
    f1: f64,
}

fn score(pairs: &[(bool, f64)], threshold: f64) -> Scores {
    let (mut tp, mut fp, mut fn_, mut tn) = (0, 0, 0, 0);
    for &(gold, p) in pairs {
        let keep = p >= threshold;
        match (keep, gold) {
            (true, true) => tp += 1,
            (true, false) => fp += 1,
            (false, true) => fn_ += 1,
            (false, false) => tn += 1,
        }
    }
    let precision = if tp + fp == 0 { 0.0 } else { tp as f64 / (tp + fp) as f64 };
    let recall = if tp + fn_ == 0 { 0.0 } else { tp as f64 / (tp + fn_) as f64 };
    let f1 = if precision + recall == 0.0 { 0.0 } else { 2.0 * precision * recall / (precision + recall) };
    Scores { threshold, tp, fp, fn_, tn, accuracy: (tp + tn) as f64 / pairs.len() as f64, precision, recall, f1 }
}

// Area under the ROC curve: P(a random keep post outranks a random skip post).
fn auc(pairs: &[(bool, f64)]) -> f64 {
    let pos: Vec<f64> = pairs.iter().filter(|x| x.0).map(|x| x.1).collect();
    let neg: Vec<f64> = pairs.iter().filter(|x| !x.0).map(|x| x.1).collect();
    let mut wins = 0.0;
    for a in &pos {
        for b in &neg {
            wins += if a > b {
                1.0
            } else if a == b {
                0.5
            } else {
                0.0
            };
        }
    }
    if pos.len() * neg.len() == 0 { f64::NAN } else { wins / (pos.len() * neg.len()) as f64 }
}

fn pct(x: f64) -> String {
    format!("{:.0}%", x * 100.0)
}

fn row(s: &Scores) -> String {
    format!(
        "| {:.2} | {} | {} | {} | {} | {} | {} | {} | {:.2} |",
        s.threshold,
        s.tp,
        s.fp,
        s.fn_,
        s.tn,
        pct(s.accuracy),
        pct(s.precision),
        pct(s.recall),
        s.f1
    )
}

fn squash(text: &str, n: usize) -> String {
    Regex::new(r"\s+").unwrap().replace_all(text, " ").chars().take(n).collect()
}

fn compare() -> anyhow::Result<()> {
    let items: Vec<Item> = read_json("items.json")?;
    let opus: HashMap<i64, OpusLabel> =
        read_json::<Vec<OpusLabel>>("opus.json")?.into_iter().map(|l| (l.id, l)).collect();
    let run_file = flag("--run").unwrap_or_else(|| "jev.json".into());
    let jev: JevRun = read_json(&run_file)?;
    let jev_by_id: HashMap<i64, &JevResult> = jev.results.iter().map(|r| (r.id, r)).collect();

    let mut joined = Vec::new();
    for item in &items {
        let (Some(gold), Some(answer)) = (opus.get(&item.id), jev_by_id.get(&item.id)) else {
            anyhow::bail!("post {} is missing from opus.json or jev.json", item.id);
        };
        joined.push((item, gold, answer.probability));
    }
    let pairs: Vec<(bool, f64)> = joined.iter().map(|j| (j.1.keep, j.2)).collect();
    let sure: Vec<(bool, f64)> = joined.iter().filter(|j| j.1.confidence == "high").map(|j| (j.1.keep, j.2)).collect();

    let thresholds = [0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9];
    let sweep: Vec<Scores> = thresholds.iter().map(|t| score(&pairs, *t)).collect();
    let best = sweep.iter().fold(&sweep[0], |a, b| if b.f1 > a.f1 { b } else { a });
    let at = score(&pairs, 0.5);
    let mut latencies: Vec<f64> = jev.results.iter().map(|r| r.latency_ms).collect();
    latencies.sort_by(f64::total_cmp);
    let q = |f: f64| {
        latencies
            .get(((f * latencies.len() as f64).floor() as usize).min(latencies.len().saturating_sub(1)))
            .copied()
            .unwrap_or(0.0)
    };

    let mut misses: Vec<_> = joined.iter().filter(|j| j.1.keep != (j.2 >= best.threshold)).collect();
    misses.sort_by(|a, b| (b.2 - best.threshold).abs().total_cmp(&(a.2 - best.threshold).abs()));

    let keep_n = pairs.iter().filter(|p| p.0).count();
    let mut lines = vec![
        format!("# News filter: Opus (reference) vs {}", jev.model),
        String::new(),
        format!(
            "{} latest channel posts. Reference: Opus, {keep_n} keep / {} skip, {} marked low-confidence.",
            items.len(),
            pairs.len() - keep_n,
            joined.len() - sure.len()
        ),
        String::new(),
        "## Jev cost and speed".into(),
        String::new(),
        format!(
            "- {} calls, {} post(s) each, concurrency {}",
            jev.results.len().div_ceil(jev.batch.max(1)),
            jev.batch,
            jev.concurrency
        ),
        format!(
            "- wall time {:.1} s; per call p50 {} ms, p90 {} ms, max {} ms",
            jev.wall_ms / 1000.0,
            q(0.5),
            q(0.9),
            q(1.0)
        ),
        format!(
            "- cost ${:.5} total, {} input tokens (≈ ${:.3} per 1000 posts)",
            jev.cost_usd,
            jev.input_tokens,
            jev.cost_usd / jev.results.len() as f64 * 1000.0
        ),
        String::new(),
        "## Agreement".into(),
        String::new(),
        format!("- ROC AUC {:.3} (all), {:.3} (Opus high-confidence only)", auc(&pairs), auc(&sure)),
        format!(
            "- at p ≥ 0.5: accuracy {}, precision {}, recall {}",
            pct(at.accuracy),
            pct(at.precision),
            pct(at.recall)
        ),
        format!(
            "- best F1 threshold {:.2}: accuracy {}, precision {}, recall {}",
            best.threshold,
            pct(best.accuracy),
            pct(best.precision),
            pct(best.recall)
        ),
        String::new(),
        "| threshold | TP | FP | FN | TN | accuracy | precision | recall | F1 |".into(),
        "|---|---|---|---|---|---|---|---|---|".into(),
    ];
    lines.extend(sweep.iter().map(row));
    lines.push(String::new());
    lines.push(format!("## Disagreements at threshold {:.2} ({})", best.threshold, misses.len()));
    lines.push(String::new());
    lines.push("| id | Opus | Jev p | conf | category | post | Opus reason |".into());
    lines.push("|---|---|---|---|---|---|---|".into());
    for (item, gold, p) in misses {
        lines.push(format!(
            "| {} | {} | {p:.2} | {} | {} | {} | {} |",
            item.id,
            if gold.keep { "keep" } else { "skip" },
            gold.confidence,
            gold.category,
            squash(&item.text, 140).replace('|', "/"),
            gold.reason.replace('|', "/")
        ));
    }
    lines.push(String::new());
    let report = Regex::new(r"\.json$")
        .unwrap()
        .replace(&Regex::new(r"^jev").unwrap().replace(&run_file, "report"), ".md")
        .into_owned();
    std::fs::write(dir()?.join(report), lines.join("\n"))?;
    println!("{}", lines.iter().take(28).cloned().collect::<Vec<_>>().join("\n"));
    Ok(())
}

// ─── pipeline: filter + dedup in one pass ────────────────────────────
//
// One Jev request per batch answers three questions per post:
//   keep_n     noul    passes the rubric
//   covered_n  noul    already in the previous digest, nothing new
//   dup_n      choice  repeats which other post that has at least as many facts
// Every post's text, the rubric and the previous digest are sent once per
// batch. The embeddings (already stored per post) are free: they pick each
// post's dup candidates and pack candidates into the same batch, so a post
// with no near neighbour gets no dup question at all.

const NEIGHBOUR_MIN_COSINE: f64 = 0.55;
const MAX_NEIGHBOURS: usize = 5;
const MAX_BATCH: usize = 25;

#[derive(Clone, Debug, Deserialize)]
struct LastDigest {
    id: i64,
    #[allow(dead_code)]
    sent_at: String,
    text: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct Neighbour {
    id: i64,
    cosine: f64,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct PipelineResult {
    id: i64,
    keep: f64,
    covered: f64,
    neighbours: Vec<Neighbour>,
    // P(each candidate) from the dup choice; None when the post had no
    // in-batch candidate. "none" is the no-duplicate option.
    dup: Option<Map<String, Value>>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct PipelineRun {
    model: String,
    wall_ms: f64,
    cost_usd: f64,
    input_tokens: u64,
    batches: Vec<Vec<i64>>,
    results: Vec<PipelineResult>,
}

fn unit(v: &[f64]) -> Vec<f64> {
    let n = v.iter().map(|x| x * x).sum::<f64>().sqrt();
    v.iter().map(|x| x / n).collect()
}

fn dot(a: &[f64], b: &[f64]) -> f64 {
    a.iter().enumerate().map(|(i, x)| x * b.get(i).copied().unwrap_or(0.0)).sum()
}

fn vectors(items: &[Item]) -> anyhow::Result<HashMap<i64, Vec<f64>>> {
    let raw: HashMap<String, Vec<f64>> = read_json("embeddings.json")?;
    Ok(items.iter().map(|i| (i.id, unit(raw.get(&i.id.to_string()).map(Vec::as_slice).unwrap_or(&[])))).collect())
}

fn neighbour_graph(ids: &[i64], vectors: &HashMap<i64, Vec<f64>>) -> HashMap<i64, Vec<Neighbour>> {
    let empty = Vec::new();
    ids.iter()
        .map(|a| {
            let va = vectors.get(a).unwrap_or(&empty);
            let mut near: Vec<Neighbour> = ids
                .iter()
                .filter(|b| *b != a)
                .map(|b| Neighbour { id: *b, cosine: dot(va, vectors.get(b).unwrap_or(&empty)) })
                .filter(|n| n.cosine >= NEIGHBOUR_MIN_COSINE)
                .collect();
            near.sort_by(|x, y| y.cosine.total_cmp(&x.cosine));
            near.truncate(MAX_NEIGHBOURS);
            (*a, near)
        })
        .collect()
}

// Union-find over undirected edges → groups, singletons included, in
// first-seen order of their root.
fn components(ids: &[i64], edges: &[(i64, i64)]) -> Vec<Vec<i64>> {
    let mut parent: HashMap<i64, i64> = ids.iter().map(|id| (*id, *id)).collect();
    fn find(parent: &mut HashMap<i64, i64>, x: i64) -> i64 {
        let p = *parent.get(&x).unwrap_or(&x);
        if p == x {
            return x;
        }
        let root = find(parent, p);
        parent.insert(x, root);
        root
    }
    for &(a, b) in edges {
        let (ra, rb) = (find(&mut parent, a), find(&mut parent, b));
        parent.insert(ra, rb);
    }
    let mut order: Vec<i64> = Vec::new();
    let mut groups: HashMap<i64, Vec<i64>> = HashMap::new();
    for id in ids {
        let root = find(&mut parent, *id);
        if !groups.contains_key(&root) {
            order.push(root);
        }
        groups.entry(root).or_default().push(*id);
    }
    order.into_iter().map(|r| groups.remove(&r).unwrap_or_default()).collect()
}

// First-fit decreasing: neighbour groups stay inside one batch.
fn pack_batches(groups: &[Vec<i64>]) -> Vec<Vec<i64>> {
    let mut chunks: Vec<Vec<i64>> = groups.iter().flat_map(|g| g.chunks(MAX_BATCH).map(<[i64]>::to_vec)).collect();
    chunks.sort_by_key(|c| std::cmp::Reverse(c.len()));
    let mut bins: Vec<Vec<i64>> = Vec::new();
    for chunk in chunks {
        match bins.iter_mut().find(|b| b.len() + chunk.len() <= MAX_BATCH) {
            Some(bin) => bin.extend(chunk),
            None => bins.push(chunk),
        }
    }
    bins
}

async fn run_pipeline() -> anyhow::Result<()> {
    let key = router_key()?;
    let model = flag("--model").unwrap_or_else(|| DEFAULT_DECISION_MODEL.into());
    let items: Vec<Item> = read_json("items.json")?;
    let by_id: HashMap<i64, &Item> = items.iter().map(|i| (i.id, i)).collect();
    let rubric = std::fs::read_to_string(dir()?.join("interests.md"))?;
    let previous: LastDigest = read_json("last-digest.json")?;
    let vectors = vectors(&items)?;
    let ids: Vec<i64> = items.iter().map(|i| i.id).collect();
    let graph = neighbour_graph(&ids, &vectors);
    let edges: Vec<(i64, i64)> = ids.iter().flat_map(|a| graph[a].iter().map(move |n| (*a, n.id))).collect();
    let batches = pack_batches(&components(&ids, &edges));
    let client = decisions(&model, Arc::new(openrouter_api(&key)));

    let run_batch = async |batch: Vec<i64>| -> anyhow::Result<(Vec<PipelineResult>, f64, u64)> {
        let n: HashMap<i64, usize> = batch.iter().enumerate().map(|(i, id)| (*id, i + 1)).collect();
        let mut keeps: Vec<(usize, Question<NoulAnswer>)> = Vec::new();
        let mut covers: Vec<(usize, Question<NoulAnswer>)> = Vec::new();
        let mut dups: HashMap<usize, Question<ChoiceAnswer>> = HashMap::new();
        let mut req = request(json!({
            "rules": rubric,
            "previous_digest": previous.text,
            "posts": batch.iter().map(|id| json!({ "n": n[id], "channel": by_id.get(id).map(|i| i.channel.clone()), "text": by_id.get(id).map(|i| i.text.clone()) })).collect::<Vec<_>>(),
        }));
        for id in &batch {
            let k = n[id];
            keeps.push((
                k,
                noul(
                    &format!(
                        "Post #{k} in posts is worth including in the reader's digest according to the selection rules in rules."
                    ),
                    None,
                ),
            ));
            covers.push((
                k,
                noul(
                    &format!(
                        "The event of post #{k} is already in previous_digest, and the post adds no new facts to it."
                    ),
                    None,
                ),
            ));
            let candidates: Vec<usize> = graph[id].iter().filter_map(|c| n.get(&c.id).copied()).collect();
            if !candidates.is_empty() {
                let mut options = Map::new();
                options.insert("none".into(), "No such post".into());
                for m in candidates {
                    options.insert(m.to_string(), format!("Post #{m}").into());
                }
                dups.insert(k, choice(&format!("Post #{k} reports the same event as another post in posts that has at least as many facts. Which post is it?"), options)?);
            }
        }
        let mut count = 0;
        for id in &batch {
            let k = n[id];
            req =
                req.question(&format!("keep_{k}"), &keeps[k - 1].1).question(&format!("covered_{k}"), &covers[k - 1].1);
            count += 2;
            if let Some(q) = dups.get(&k) {
                req = req.question(&format!("dup_{k}"), q);
                count += 1;
            }
        }
        let decision = client.decide(req).await?;
        let mut results = Vec::new();
        for id in &batch {
            let k = n[id];
            let keep = decision.answer(&format!("keep_{k}"), &keeps[k - 1].1).map_or(f64::NAN, |a| a.probability);
            let covered =
                decision.answer(&format!("covered_{k}"), &covers[k - 1].1).map_or(f64::NAN, |a| a.probability);
            let dup = match dups.get(&k) {
                None => None,
                Some(q) => Some(
                    decision
                        .answer(&format!("dup_{k}"), q)?
                        .probabilities
                        .into_iter()
                        .map(|(option, p)| {
                            let key = if option == "none" {
                                "none".to_owned()
                            } else {
                                option
                                    .parse::<usize>()
                                    .ok()
                                    .and_then(|m| batch.get(m - 1))
                                    .map(i64::to_string)
                                    .unwrap_or(option)
                            };
                            (key, p)
                        })
                        .collect(),
                ),
            };
            results.push(PipelineResult { id: *id, keep, covered, neighbours: graph[id].clone(), dup });
        }
        let tokens = decision.usage.as_ref().map_or(0, |u| u.input);
        println!("batch of {}: {count} questions, {tokens} tok", batch.len());
        Ok((results, decision.cost_usd.unwrap_or(0.0), tokens))
    };

    let started = Instant::now();
    let per_batch: Vec<_> = futures::stream::iter(batches.iter().cloned())
        .map(&run_batch)
        .buffered(8)
        .collect::<Vec<_>>()
        .await
        .into_iter()
        .collect::<anyhow::Result<_>>()?;
    let run = PipelineRun {
        model,
        wall_ms: started.elapsed().as_millis() as f64,
        cost_usd: per_batch.iter().map(|b| b.1).sum(),
        input_tokens: per_batch.iter().map(|b| b.2).sum(),
        batches: batches.clone(),
        results: per_batch.into_iter().flat_map(|b| b.0).collect(),
    };
    write_json("pipeline.json", &run)?;
    let sizes = batches.iter().map(|b| b.len().to_string()).collect::<Vec<_>>().join(", ");
    println!(
        "\n{} posts in {} call(s) [{sizes}], {}ms, ${:.6}, {} input tokens → pipeline.json",
        items.len(),
        batches.len(),
        run.wall_ms,
        run.cost_usd,
        run.input_tokens
    );
    Ok(())
}

// ─── compare-dedup ───────────────────────────────────────────────────

#[derive(Clone, Debug, Deserialize)]
struct GoldEvent {
    event: String,
    post_ids: Vec<i64>,
    fullest: i64,
}

#[derive(Clone, Debug, Deserialize)]
struct GoldCovered {
    id: i64,
    covered: bool,
    #[allow(dead_code)]
    bullet: Option<i64>,
    confidence: String,
    reason: String,
}

#[derive(Clone, Debug, Deserialize)]
struct DedupGold {
    events: Vec<GoldEvent>,
    covered: Vec<GoldCovered>,
}

const KEEP_THRESHOLD: f64 = 0.3;

struct PairScores {
    precision: f64,
    recall: f64,
    f1: f64,
    pairs: usize,
}

// Pairwise clustering agreement: a pair counts when both posts share a group.
fn pair_scores(predicted: &[Vec<i64>], gold: &[Vec<i64>]) -> PairScores {
    let pairs_of = |groups: &[Vec<i64>]| -> HashSet<(i64, i64)> {
        groups
            .iter()
            .flat_map(|g| {
                g.iter().enumerate().flat_map(move |(i, a)| g[i + 1..].iter().map(move |b| (*a.min(b), *a.max(b))))
            })
            .collect()
    };
    let (p, g) = (pairs_of(predicted), pairs_of(gold));
    let hit = p.iter().filter(|x| g.contains(x)).count();
    let precision = if p.is_empty() { 1.0 } else { hit as f64 / p.len() as f64 };
    let recall = if g.is_empty() { 1.0 } else { hit as f64 / g.len() as f64 };
    let f1 = if precision + recall == 0.0 { 0.0 } else { 2.0 * precision * recall / (precision + recall) };
    PairScores { precision, recall, f1, pairs: p.len() }
}

// The post a dup answer points at, when Jev thinks it is a duplicate at all.
fn dup_target(dup: Option<&Map<String, Value>>) -> Option<i64> {
    let dup = dup?;
    if dup.get("none").and_then(Value::as_f64).unwrap_or(0.0) >= 0.5 {
        return None;
    }
    let mut ranked: Vec<(&String, f64)> =
        dup.iter().filter(|(k, _)| *k != "none").map(|(k, v)| (k, v.as_f64().unwrap_or(0.0))).collect();
    ranked.sort_by(|a, b| b.1.total_cmp(&a.1));
    ranked.first().and_then(|(k, _)| k.parse().ok())
}

fn compare_dedup() -> anyhow::Result<()> {
    let covered_threshold: f64 = flag("--covered").and_then(|c| c.parse().ok()).unwrap_or(0.5);
    let items: Vec<Item> = read_json("items.json")?;
    let length: HashMap<i64, usize> = items.iter().map(|i| (i.id, i.text.encode_utf16().count())).collect();
    let ids: Vec<i64> = items.iter().map(|i| i.id).collect();
    let opus: HashMap<i64, OpusLabel> =
        read_json::<Vec<OpusLabel>>("opus.json")?.into_iter().map(|l| (l.id, l)).collect();
    let gold: DedupGold = read_json("dedup-opus.json")?;
    let gold_covered: HashMap<i64, &GoldCovered> = gold.covered.iter().map(|c| (c.id, c)).collect();
    let event_of: HashMap<i64, usize> =
        gold.events.iter().enumerate().flat_map(|(i, e)| e.post_ids.iter().map(move |id| (*id, i))).collect();
    let run: PipelineRun = read_json("pipeline.json")?;
    let res: HashMap<i64, &PipelineResult> = run.results.iter().map(|r| (r.id, r)).collect();
    let gold_groups: Vec<Vec<i64>> = gold.events.iter().map(|e| e.post_ids.clone()).collect();
    let multi: Vec<&GoldEvent> = gold.events.iter().filter(|e| e.post_ids.len() > 1).collect();
    let keep_of = |id: &i64| res.get(id).map_or(0.0, |r| r.keep);
    let covered_of = |id: &i64| res.get(id).map_or(0.0, |r| r.covered);

    // Filter quality with two extra questions per post in the same request.
    let keep_pairs: Vec<(bool, f64)> =
        ids.iter().map(|id| (opus.get(id).is_some_and(|o| o.keep), keep_of(id))).collect();
    let keep_sure: Vec<(bool, f64)> = ids
        .iter()
        .filter(|id| opus.get(id).is_some_and(|o| o.confidence == "high"))
        .map(|id| (opus.get(id).is_some_and(|o| o.keep), keep_of(id)))
        .collect();
    // Covered vs the previous digest.
    let cov_pairs: Vec<(bool, f64)> =
        ids.iter().map(|id| (gold_covered.get(id).is_some_and(|c| c.covered), covered_of(id))).collect();
    let cov = score(&cov_pairs, covered_threshold);

    // Dedup clusters: Jev's edges vs embedding-threshold baselines.
    let jev_edges: Vec<(i64, i64)> =
        ids.iter().filter_map(|id| dup_target(res.get(id).and_then(|r| r.dup.as_ref())).map(|t| (*id, t))).collect();
    let jev_clusters = components(&ids, &jev_edges);
    let vectors = vectors(&items)?;
    let empty = Vec::new();
    let all_pairs: Vec<(i64, i64, f64)> = ids
        .iter()
        .enumerate()
        .flat_map(|(i, a)| ids[i + 1..].iter().map(move |b| (*a, *b)))
        .map(|(a, b)| (a, b, dot(vectors.get(&a).unwrap_or(&empty), vectors.get(&b).unwrap_or(&empty))))
        .collect();
    let baseline = |tau: f64| {
        let edges: Vec<(i64, i64)> = all_pairs.iter().filter(|p| p.2 >= tau).map(|p| (p.0, p.1)).collect();
        pair_scores(&components(&ids, &edges), &gold_groups)
    };
    let candidate_edges: Vec<(i64, i64)> =
        run.results.iter().flat_map(|r| r.neighbours.iter().map(move |n| (r.id, n.id))).collect();
    let ceiling = pair_scores(&components(&ids, &candidate_edges), &gold_groups);

    // Fullest wins: in a cluster the survivor is the post nobody else outranks —
    // a post with an outgoing dup edge is dominated. Ties/cycles → longest.
    let longer = |a: i64, b: i64| {
        if length.get(&b).copied().unwrap_or(0) > length.get(&a).copied().unwrap_or(0) { b } else { a }
    };
    let survivor_of = |group: &[i64], edges: &[(i64, i64)]| -> i64 {
        let dominated: HashSet<i64> =
            edges.iter().filter(|(a, b)| group.contains(a) && group.contains(b)).map(|(a, _)| *a).collect();
        let free: Vec<i64> = group.iter().copied().filter(|id| !dominated.contains(id)).collect();
        let pool = if free.is_empty() { group.to_vec() } else { free };
        pool.into_iter().reduce(longer).unwrap_or_default()
    };
    let longest = |group: &[i64]| group.iter().copied().reduce(longer).unwrap_or_default();
    let fullest_rows: Vec<(&GoldEvent, usize, i64, i64)> = multi
        .iter()
        .map(|e| {
            let cluster =
                jev_clusters.iter().find(|c| c.contains(&e.fullest)).cloned().unwrap_or_else(|| vec![e.fullest]);
            let merged: Vec<i64> = e.post_ids.iter().copied().filter(|id| cluster.contains(id)).collect();
            (*e, merged.len(), survivor_of(&merged, &jev_edges), longest(&e.post_ids))
        })
        .collect();

    // End to end: what would reach the composer.
    let passes = |id: &i64| keep_of(id) >= KEEP_THRESHOLD && res.get(id).map_or(1.0, |r| r.covered) < covered_threshold;
    let kept: Vec<i64> = ids.iter().copied().filter(passes).collect();
    let kept_edges: Vec<(i64, i64)> =
        jev_edges.iter().copied().filter(|(a, b)| kept.contains(a) && kept.contains(b)).collect();
    let final_posts: Vec<i64> = components(&kept, &kept_edges).iter().map(|g| survivor_of(g, &jev_edges)).collect();
    let filter_only: Vec<i64> = ids.iter().copied().filter(|id| keep_of(id) >= KEEP_THRESHOLD).collect();
    let expected: HashSet<Option<usize>> = ids
        .iter()
        .filter(|id| opus.get(id).is_some_and(|o| o.keep) && !gold_covered.get(id).is_some_and(|c| c.covered))
        .map(|id| event_of.get(id).copied())
        .collect();
    let end_to_end = |posts: &[i64]| {
        let events: Vec<Option<usize>> = posts.iter().map(|id| event_of.get(id).copied()).collect();
        let hit: HashSet<Option<usize>> = events.iter().copied().filter(|e| expected.contains(e)).collect();
        let in_expected = events.iter().filter(|e| expected.contains(e)).count();
        (posts.len(), hit.len() as f64 / expected.len() as f64, in_expected - hit.len(), events.len() - in_expected)
    };
    let e2e = [
        ("all posts".to_owned(), end_to_end(&ids)),
        (format!("filter only (keep ≥ {KEEP_THRESHOLD})"), end_to_end(&filter_only)),
        ("filter + covered + dedup".to_owned(), end_to_end(&final_posts)),
    ];

    let snippet = |id: i64| squash(items.iter().find(|i| i.id == id).map_or("", |i| &i.text), 90).replace('|', "/");
    let f = |x: f64| format!("{x:.2}");
    let last: LastDigest = read_json("last-digest.json")?;
    let jev_scores = pair_scores(&jev_clusters, &gold_groups);
    let sizes = run.batches.iter().map(|b| b.len().to_string()).collect::<Vec<_>>().join(", ");
    let mut lines = vec![
        format!("# News filter + dedup in one Jev pass: {}", run.model),
        String::new(),
        format!("{} posts, previous digest #{}. Reference: Opus (opus.json, dedup-opus.json).", items.len(), last.id),
        String::new(),
        "## Cost".into(),
        String::new(),
        format!(
            "- {} calls [{sizes}], {:.1} s wall, ${:.5}, {} input tokens",
            run.batches.len(),
            run.wall_ms / 1000.0,
            run.cost_usd,
            run.input_tokens
        ),
        format!(
            "- dup candidates: cosine ≥ {NEIGHBOUR_MIN_COSINE}, top {MAX_NEIGHBOURS}; {} posts got a dup question",
            run.results.iter().filter(|r| r.dup.is_some()).count()
        ),
        String::new(),
        "## End to end (events the composer receives)".into(),
        String::new(),
        format!("Expected: {} events (some post is Opus-keep and not covered by the previous digest).", expected.len()),
        String::new(),
        "| stage | posts | event recall | duplicate posts | off-rubric/covered posts |".into(),
        "|---|---|---|---|---|".into(),
    ];
    lines.extend(e2e.iter().map(|(name, s)| format!("| {name} | {} | {} | {} | {} |", s.0, pct(s.1), s.2, s.3)));
    lines.extend([
        String::new(),
        "## Filter (keep) — with the extra questions in the same request".into(),
        String::new(),
        format!(
            "- ROC AUC {:.3} (all), {:.3} (Opus high-confidence); filter-only batch25 was 0.919 / 0.995",
            auc(&keep_pairs),
            auc(&keep_sure)
        ),
        String::new(),
        "## Covered by the previous digest".into(),
        String::new(),
        format!(
            "- gold: {} covered; ROC AUC {:.3}; at p ≥ {covered_threshold}: precision {}, recall {} (TP {}, FP {}, FN {})",
            cov_pairs.iter().filter(|p| p.0).count(),
            auc(&cov_pairs),
            pct(cov.precision),
            pct(cov.recall),
            cov.tp,
            cov.fp,
            cov.fn_
        ),
        String::new(),
        "## Dedup among the posts (pairwise vs Opus events)".into(),
        String::new(),
        format!("Gold: {} events, {} with 2+ posts.", gold.events.len(), multi.len()),
        String::new(),
        "| method | predicted pairs | precision | recall | F1 |".into(),
        "|---|---|---|---|---|".into(),
        format!(
            "| **Jev on candidates** | {} | {} | {} | {} |",
            jev_scores.pairs,
            pct(jev_scores.precision),
            pct(jev_scores.recall),
            f(jev_scores.f1)
        ),
        format!(
            "| candidates, all merged (recall ceiling) | {} | {} | {} | {} |",
            ceiling.pairs,
            pct(ceiling.precision),
            pct(ceiling.recall),
            f(ceiling.f1)
        ),
    ]);
    for t in [0.6, 0.65, 0.7, 0.8, 0.9] {
        let s = baseline(t);
        lines.push(format!(
            "| embeddings only, cosine ≥ {t} | {} | {} | {} | {} |",
            s.pairs,
            pct(s.precision),
            pct(s.recall),
            f(s.f1)
        ));
    }
    lines.extend([
        String::new(),
        "## Fullest version wins".into(),
        String::new(),
        format!(
            "Of {} multi-post events: Jev survivor = Opus fullest in {}; longest text = Opus fullest in {}.",
            multi.len(),
            fullest_rows.iter().filter(|r| r.2 == r.0.fullest).count(),
            fullest_rows.iter().filter(|r| r.3 == r.0.fullest).count()
        ),
        String::new(),
        "| event | posts | merged by Jev | Opus fullest | Jev survivor | longest |".into(),
        "|---|---|---|---|---|---|".into(),
    ]);
    for (e, merged, jev, long) in &fullest_rows {
        let posts = e.post_ids.iter().map(i64::to_string).collect::<Vec<_>>().join(", ");
        lines.push(format!("| {} | {posts} | {merged} | {} | {jev} | {long} |", e.event.replace('|', "/"), e.fullest));
    }
    lines.extend([
        String::new(),
        "## Covered disagreements".into(),
        String::new(),
        "| id | Opus | Jev p | conf | post | Opus reason |".into(),
        "|---|---|---|---|---|---|".into(),
    ]);
    for id in &ids {
        let g = gold_covered.get(id);
        if g.is_some_and(|c| c.covered) == (covered_of(id) >= covered_threshold) {
            continue;
        }
        lines.push(format!(
            "| {id} | {} | {} | {} | {} | {} |",
            if g.is_some_and(|c| c.covered) { "covered" } else { "new" },
            f(covered_of(id)),
            g.map_or("undefined".into(), |c| c.confidence.clone()),
            snippet(*id),
            g.map_or(String::new(), |c| c.reason.replace('|', "/"))
        ));
    }
    lines.push(String::new());
    std::fs::write(dir()?.join("report-dedup.md"), lines.join("\n"))?;
    println!("{}", lines.join("\n"));
    Ok(())
}

// ─── filter: the shipped path ────────────────────────────────────────

// The filter prompt exactly as the planner anchor pins it, whitespace folded
// the way the compiler re-emits it.
fn anchor_filter_prompt(root: &Path) -> anyhow::Result<String> {
    let planner = std::fs::read_to_string(root.join("skills.default/planner.md"))?;
    let re = Regex::new(r#""kind":"decide","preset":"filter"[\s\S]*?"prompt":"([^"]*)""#)?;
    let prompt = re.captures(&planner).map(|c| c[1].to_owned()).filter(|p| !p.is_empty());
    let prompt =
        prompt.ok_or_else(|| anyhow::anyhow!(r#"no {{"kind":"decide","preset":"filter"}} prompt in planner.md"#))?;
    Ok(Regex::new(r"\s+")?.replace_all(&prompt, " ").trim().to_owned())
}

// Sums what each decide call reports, without touching the client: the
// endpoint it posts to is wrapped instead.
struct Metered {
    inner: Arc<dyn JsonApi>,
    totals: Mutex<(f64, u64)>,
}

#[async_trait]
impl JsonApi for Metered {
    async fn post(&self, path: &str, body: Value, cancel: &CancellationToken) -> anyhow::Result<Value> {
        let res = self.inner.post(path, body, cancel).await?;
        let mut totals = self.totals.lock().unwrap_or_else(|e| e.into_inner());
        totals.0 += res["usage"]["cost"].as_f64().unwrap_or(0.0);
        totals.1 += res["usage"]["input_tokens"].as_u64().unwrap_or(0);
        Ok(res)
    }
}

#[derive(Deserialize)]
struct NewsRow {
    id: i64,
    item: Map<String, Value>,
}

async fn run_filter() -> anyhow::Result<()> {
    let key = router_key()?;
    let model = flag("--model").unwrap_or_else(|| DEFAULT_DECISION_MODEL.into());
    let rows: Vec<NewsRow> = read_json("list-news-items.json")?;
    let metered = Arc::new(Metered { inner: Arc::new(openrouter_api(&key)), totals: Mutex::new((0.0, 0)) });
    let client = decisions(&model, metered.clone());
    let items: Vec<Value> = rows.iter().map(|r| Value::Object(r.item.clone())).collect();
    let started = Instant::now();
    let result =
        filter(&client, &items, &anchor_filter_prompt(&root()?)?, Some(0.01), &null_scope(), &CancellationToken::new())
            .await?;
    let wall_ms = started.elapsed().as_millis() as f64;
    let (cost, tokens) = *metered.totals.lock().unwrap_or_else(|e| e.into_inner());
    let run = JevRun {
        model,
        concurrency: 16,
        batch: 25,
        wall_ms,
        cost_usd: cost,
        input_tokens: tokens,
        results: rows
            .iter()
            .enumerate()
            .map(|(i, r)| JevResult {
                id: r.id,
                probability: result.probabilities.get(i).copied().unwrap_or(0.0),
                latency_ms: wall_ms,
                cost_usd: cost / rows.len() as f64,
            })
            .collect(),
    };
    write_json("jev-filter.json", &run)?;
    println!("{} items, {wall_ms}ms, ${:.6}, {tokens} input tokens → jev-filter.json", rows.len(), run.cost_usd);
    Ok(())
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    load_env();
    match std::env::args().nth(1).as_deref() {
        Some("jev") => run_jev().await,
        Some("compare") => compare(),
        Some("pipeline") => run_pipeline().await,
        Some("compare-dedup") => compare_dedup(),
        Some("filter") => run_filter().await,
        _ => anyhow::bail!("usage: news-filter-eval jev | compare | pipeline | compare-dedup | filter"),
    }
}

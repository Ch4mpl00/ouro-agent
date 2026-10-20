// Replay a compose generation over a captured trace under another model.
// The recorded input pins the tool results; inspect also reads legacy planner nodes.
//
//   judge-replay <traceId>
//   judge-replay <traceId> --compose <model> [--thinking [low|medium|high]] [--judge]
//
// With --judge, the original (A) and each B sample go to a pairwise judge
// (gpt-5.4) over the SAME frozen candidates — both in one call (one salience
// bar), each pair judged in BOTH orders and a winner kept only if it survives
// the swap (a verdict that flips with position is the judge's position bias,
// counted as a tie). JUDGE_PAIRS caps how many samples get the (costly) swap.
use std::sync::Arc;

use agent::providers::{HttpApi, OPENAI_BASE_URL};
use agent::trace_model::{Observation, ObservationType};
use evals::backend::openai_structured;
use evals::env::load_env;
use evals::gate::{ChatMessage, as_text, recorded_messages};
use evals::gate_runtime::{complete, replay_retry, to_param};
use evals::improver::truncate;
use evals::langfuse_api::Langfuse;
use evals::materials::millis;
use serde_json::{Value, json};

fn arg_flag(name: &str) -> Option<String> {
    let args: Vec<String> = std::env::args().collect();
    let i = args.iter().position(|a| a == name)?;
    Some(args.get(i + 1).cloned().unwrap_or_default())
}

fn has_flag(name: &str) -> bool {
    std::env::args().any(|a| a == name)
}

// The last generation whose name starts with `prefix`. For the compiler that's
// the last `attempt-N` (the successful one after retries); compose is single.
fn find_generation<'a>(obs: &'a [Observation], prefix: &str) -> Option<&'a Observation> {
    let mut matches: Vec<&Observation> =
        obs.iter().filter(|o| o.kind == ObservationType::Generation && o.name.starts_with(prefix)).collect();
    matches.sort_by_key(|o| millis(&o.start_time));
    matches.last().copied()
}

fn output_text(generation: &Observation) -> String {
    as_text(&generation.output)
}

// Retry 429 (rate limit) and 5xx with exponential backoff, tuned wider than
// the runtime's: free-tier Gemini rate-limits hard, and the gpt-5.4 judge
// blows the per-minute token budget when several swap-pairs send ~160k-char
// candidate sets at once.
async fn replay_generation(
    messages: &[ChatMessage],
    model: &str,
    json_mode: bool,
    effort: Option<&str>,
) -> anyhow::Result<String> {
    complete(model, messages.iter().map(to_param).collect(), json_mode, effort).await
}

// ─── modes ───────────────────────────────────────────────────────────

fn inspect(observations: &[Observation]) {
    for (label, prefix) in [("PLANNER (Test A input)", "attempt-"), ("COMPOSE (Test B input)", "llm_compose")] {
        println!("\n=== {label} ===");
        let Some(generation) = find_generation(observations, prefix) else {
            println!("(not found)");
            continue;
        };
        let msgs = recorded_messages(&generation.input);
        println!(
            "name={}  model={}  messages={}",
            generation.name,
            generation.model.as_deref().unwrap_or("—"),
            msgs.len()
        );
        for m in &msgs {
            let text = as_text(&m.content);
            println!(
                "  • {} ({} chars): {}",
                m.role,
                text.encode_utf16().count(),
                truncate(&text, 180).replace('\n', " ")
            );
        }
        let out = output_text(generation);
        println!("output ({} chars): {}", out.encode_utf16().count(), truncate(&out, 300));
    }
}

// Sample independent replies over the same frozen input.
const SAMPLES: usize = 5;

// Test B: replay the composer's input under model B, SAMPLES times. The input
// already holds the frozen tool results (the rendered candidate posts), so this
// is a pure compose-model A/B. Output is prose — no JSON validation; with
// `judge` A and each B sample are compared by the swap-debiased judge (below).
async fn test_compose(
    observations: &[Observation],
    model_b: &str,
    effort: Option<&str>,
    with_judge: bool,
) -> anyhow::Result<()> {
    let compose = find_generation(observations, "llm_compose")
        .ok_or_else(|| anyhow::anyhow!("no llm_compose generation in trace"))?;
    let messages = recorded_messages(&compose.input);
    let original = output_text(compose);
    println!("\n=== COMPOSE A — original ({}) ===", compose.model.as_deref().unwrap_or("—"));
    println!("{original}");
    let think = effort.map(|e| format!(" (thinking={e})")).unwrap_or_default();
    println!("\n=== COMPOSE B — {model_b}{think}, {SAMPLES} independent samples ===");
    let samples =
        futures::future::try_join_all((0..SAMPLES).map(|_| replay_generation(&messages, model_b, false, effort)))
            .await?;
    for (i, out) in samples.iter().enumerate() {
        println!("\n[sample {}] ({} chars)", i + 1, out.encode_utf16().count());
        println!("{out}");
    }
    if with_judge {
        judge_compose(compose, &original, &samples, model_b).await?;
    }
    Ok(())
}

// ─── pairwise judge with swap-debias (stage 3) ───────────────────────
// Both digests go to the judge in ONE call, so it applies ONE salience bar to
// the pair — fixing the flaw pointwise had (critiquing each digest alone let the
// bar drift: the same miss got penalized for A but waved through for B). The
// price of one-call pairwise is position bias: with a small real gap the judge
// drifts to whichever digest is listed first. We cancel it by judging each pair
// TWICE with the order swapped, and keeping a winner ONLY when it wins in BOTH
// layouts; a verdict that flips with position IS the bias — recorded as tie.
//
// query_formulation is out of scope — both digests share one frozen input, so
// orchestration is identical; only the three composer axes differ. Judge is
// gpt-5.4, a different family from both generators.

const JUDGE_MODEL: &str = "gpt-5.4";

// How many B samples to swap-judge. Each pair costs TWO gpt-5.4 calls over the
// full candidate set (~150k chars), so this is the expensive knob — generation
// stays at SAMPLES (cheap calls we still eyeball), judging is capped here.
const JUDGE_PAIRS: usize = 3;

fn verdict_schema() -> Value {
    json!({
        "type": "object",
        "properties": {
            "winner": { "type": "string", "enum": ["digest_1", "digest_2", "tie"] },
            "why": { "type": "string" },
        },
        "required": ["winner", "why"],
        "additionalProperties": false,
    })
}

fn pairwise_response_schema() -> Value {
    json!({
        "type": "object",
        "properties": {
            "coverage": verdict_schema(),
            "faithfulness": {
                "type": "object",
                "properties": {
                    "winner": { "type": "string", "enum": ["digest_1", "digest_2", "tie"] },
                    "why": { "type": "string" },
                    "digest_1_issues": { "type": "array", "items": { "type": "string" } },
                    "digest_2_issues": { "type": "array", "items": { "type": "string" } },
                },
                "required": ["winner", "why", "digest_1_issues", "digest_2_issues"],
                "additionalProperties": false,
            },
            "composition": verdict_schema(),
            "overall": verdict_schema(),
        },
        "required": ["coverage", "faithfulness", "composition", "overall"],
        "additionalProperties": false,
    })
}

pub const PAIRWISE_SYSTEM: &str = r#"You are a rigorous PAIRWISE evaluation judge for an AI news-digest composer.

Context — the agent runs in two stages: an ORCHESTRATOR gathers candidate posts (RAG + chat history), then a COMPOSER skill filters them and writes the final digest. BOTH digests below were written by COMPOSER models from the EXACT SAME candidates and the SAME contract — orchestration was identical and is NOT under test. Judge ONLY composition quality, head to head, applying ONE consistent salience bar to both digests.

You receive:
- COMPOSER_CONTRACT — how candidates must be filtered and the digest composed (format, thresholds, tone, no-fabrication).
- CANDIDATES — every post both composers saw. This is the ground truth (R) for coverage and faithfulness; it also contains the bot's previous digests (for dedup), which are context, not new material — their absence is never a coverage loss.
- DIGEST_1 and DIGEST_2 — two competing outputs. The numbering is RANDOM and carries no signal about quality or origin; do not assume 1 precedes or beats 2.

Compare on three axes, each a winner of {digest_1, digest_2, tie} with a one-sentence why citing specific items:
- coverage — which digest better included the salient, contract-fitting events from CANDIDATES and dropped the noise. Use the SAME bar for both: if a dropped event (a power restoration, a cross-border strike, a named operation) counts against one digest, it counts against the other too. Dropping a major event (casualties, strikes) is a serious loss.
- composition — which better follows the contract's format, dedup, category placement, tone, length and threshold rules.
- faithfulness — which has fewer fabricated/unsupported specifics (numbers, dates, names) relative to CANDIDATES. List each side's unsupported claims in digest_1_issues / digest_2_issues; fewer/less-severe wins. CRITICAL: a claim that IS present in CANDIDATES is faithful even if it contradicts your outside knowledge — verify against the candidate text, never against what you believe is true in the real world.

Then an overall winner.

Rules:
- tie is a legitimate, encouraged verdict when the two are genuinely comparable on an axis — do not invent a difference to break it.
- Reward neither length nor fluency. A tighter correct digest beats a longer verbose one.
- Never penalize a digest for omitting something that is not in CANDIDATES, and never credit a fabrication."#;

fn build_pairwise_prompt(contract: &str, candidates: &str, digest1: &str, digest2: &str) -> String {
    format!(
        "<composer_contract>\n{contract}\n</composer_contract>\n\n<candidates>\n{candidates}\n</candidates>\n\n<digest_1>\n{digest1}\n</digest_1>\n\n<digest_2>\n{digest2}\n</digest_2>\n\nBoth digests were composed from the SAME candidates above by two different models; orchestration was identical and is not under test. Compare them per axis with one consistent salience bar, and return JSON matching the schema."
    )
}

const AXES: [&str; 4] = ["coverage", "faithfulness", "composition", "overall"];

// One pairwise verdict, shape-checked as the zod schema did.
fn check_pairwise(v: &Value) -> anyhow::Result<()> {
    for axis in AXES {
        let winner = v[axis]["winner"].as_str().unwrap_or("");
        anyhow::ensure!(["digest_1", "digest_2", "tie"].contains(&winner), "pairwise judge: bad {axis}.winner");
        anyhow::ensure!(v[axis]["why"].is_string(), "pairwise judge: {axis}.why missing");
    }
    for side in ["digest_1_issues", "digest_2_issues"] {
        let ok = v["faithfulness"][side].as_array().is_some_and(|a| a.iter().all(Value::is_string));
        anyhow::ensure!(ok, "pairwise judge: faithfulness.{side} missing");
    }
    Ok(())
}

async fn judge_pair(
    api: &HttpApi,
    contract: &str,
    candidates: &str,
    digest1: &str,
    digest2: &str,
) -> anyhow::Result<Value> {
    let user = build_pairwise_prompt(contract, candidates, digest1, digest2);
    let v = openai_structured(
        api,
        JUDGE_MODEL,
        "pairwise",
        PAIRWISE_SYSTEM,
        &user,
        &pairwise_response_schema(),
        &replay_retry(),
    )
    .await?;
    check_pairwise(&v)?;
    Ok(v)
}

// digest_1/digest_2 are page positions; map to A/B for THIS layout (flipped =
// the B,A layout, where digest_1 is B).
fn resolve_winner(winner: &str, flipped: bool) -> &'static str {
    if winner == "tie" {
        return "tie";
    }
    let is_a = if flipped { winner == "digest_2" } else { winner == "digest_1" };
    if is_a { "A" } else { "B" }
}

// Keep a winner only if it holds in BOTH layouts; a verdict that flips with
// position (or is a tie in either layout) collapses to tie — that flip IS the
// position bias we set out to cancel.
fn reconcile(w1: &'static str, w2: &'static str) -> &'static str {
    if w1 == w2 && w1 != "tie" { w1 } else { "tie" }
}

async fn judge_compose(compose: &Observation, original: &str, samples: &[String], model_b: &str) -> anyhow::Result<()> {
    let msgs = recorded_messages(&compose.input);
    let contract =
        msgs.iter().filter(|m| m.role == "system").map(|m| as_text(&m.content)).collect::<Vec<_>>().join("\n\n");
    let candidates =
        msgs.iter().filter(|m| m.role != "system").map(|m| as_text(&m.content)).collect::<Vec<_>>().join("\n\n");
    let model_a = compose.model.as_deref().unwrap_or("original");
    let judged = &samples[..samples.len().min(JUDGE_PAIRS)];
    let key = std::env::var("OPENAI_API_KEY").unwrap_or_default();
    let api = Arc::new(HttpApi::new(OPENAI_BASE_URL, &key));

    println!("\n{}", "═".repeat(72));
    println!("SWAP-PAIRWISE JUDGE ({JUDGE_MODEL}) — A={model_a}  vs  B={model_b}");
    println!(
        "{} chars of candidates · {} pairs × 2 layouts · winner counts only if it holds in BOTH layouts (else tie)\n",
        candidates.encode_utf16().count(),
        judged.len()
    );
    // Each pair judged in BOTH layouts — (A,B) and the swap (B,A) — all in
    // parallel.
    let results = futures::future::try_join_all(judged.iter().map(|b| {
        let (api, contract, candidates) = (api.clone(), &contract, &candidates);
        async move {
            let (ab, ba) = futures::try_join!(
                judge_pair(&api, contract, candidates, original, b),
                judge_pair(&api, contract, candidates, b, original)
            )?;
            anyhow::Ok((ab, ba))
        }
    }))
    .await?;

    let mut tally: Vec<(usize, usize, usize)> = vec![(0, 0, 0); AXES.len()];
    for (i, (ab, ba)) in results.iter().enumerate() {
        println!("[sample {}]  layout A,B  vs  swap B,A", i + 1);
        for (j, axis) in AXES.iter().enumerate() {
            let w1 = resolve_winner(ab[axis]["winner"].as_str().unwrap_or("tie"), false);
            let w2 = resolve_winner(ba[axis]["winner"].as_str().unwrap_or("tie"), true);
            let fin = reconcile(w1, w2);
            match fin {
                "A" => tally[j].0 += 1,
                "B" => tally[j].1 += 1,
                _ => tally[j].2 += 1,
            }
            let flip = if w1 != w2 { "  ⟂ flipped→bias" } else { "" };
            println!("  {axis:<13} AB:{w1:<3} BA:{w2:<3} ⟹ {fin}{flip}");
        }
        // Faithfulness defect lists from the A,B layout (digest_1=A, digest_2=B).
        let issues = |side: &str| -> Vec<String> {
            ab["faithfulness"][side]
                .as_array()
                .into_iter()
                .flatten()
                .filter_map(|x| x.as_str().map(str::to_owned))
                .collect()
        };
        let (a, b) = (issues("digest_1_issues"), issues("digest_2_issues"));
        if !a.is_empty() {
            println!("    A unsupported: {}", a.join(" · "));
        }
        if !b.is_empty() {
            println!("    B unsupported: {}", b.join(" · "));
        }
        println!("    coverage why (A,B): {}", ab["coverage"]["why"].as_str().unwrap_or(""));
        println!();
    }
    println!("── tally over {} swap-pairs · A={model_a}, B={model_b} ──", judged.len());
    for (j, axis) in AXES.iter().enumerate() {
        let (a, b, t) = tally[j];
        println!("  {axis:<13} A:{a}  B:{b}  tie:{t}");
    }
    Ok(())
}

// `--thinking` with an optional level; a bare flag means high.
fn parse_thinking() -> Option<String> {
    let args: Vec<String> = std::env::args().collect();
    let i = args.iter().position(|a| a == "--thinking")?;
    match args.get(i + 1).map(String::as_str) {
        Some(level @ ("low" | "medium" | "high")) => Some(level.to_owned()),
        _ => Some("high".into()),
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    load_env();
    anyhow::ensure!(
        !has_flag("--plan") && !has_flag("--planner-file"),
        "planner replay is no longer supported; use --compose to replay a compose generation"
    );
    let trace_id = std::env::args().nth(1).filter(|a| !a.starts_with("--"));
    let Some(trace_id) = trace_id else {
        eprintln!("usage: judge-replay <traceId> [--compose <model>] [--thinking [low|medium|high]] [--judge]");
        std::process::exit(1);
    };
    let (trace, observations) = Langfuse::from_env()?.trace_by_id(&trace_id).await?;
    println!("trace {trace_id} · {} · tags=[{}]", trace.name, trace.tags.join(","));
    let thinking = parse_thinking();
    let or_default = |m: String| if m.is_empty() { "gpt-5.4-mini".to_owned() } else { m };
    if let Some(model) = arg_flag("--compose") {
        test_compose(&observations, &or_default(model), thinking.as_deref(), has_flag("--judge")).await
    } else {
        inspect(&observations);
        Ok(())
    }
}

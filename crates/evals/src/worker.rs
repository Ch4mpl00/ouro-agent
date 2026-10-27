// The online judge: polls the local trace mirror for complete runs without a
// judgement at the current (provider, prompt version), judges every node of
// each, and writes the verdicts back.

use std::time::Duration;

use agent::skills::FsSkillStore;

use crate::backend::{JudgeProvider, judge_backend};
use crate::env::{bool_env, num_env};
use crate::materials::assemble_node_materials;
use crate::node_judge::judge_node;
use crate::schema::JUDGE_PROMPT_VERSION;
use crate::scores::{NodeScoreWriteOpts, ScoreWriter};
use crate::source::TraceSource;

pub struct JudgeWorkerDeps<'a> {
    pub source: &'a dyn TraceSource,
    pub skills: &'a FsSkillStore,
    pub writer: &'a ScoreWriter,
}

#[derive(Clone, Debug)]
pub struct JudgeWorkerOpts {
    pub provider: JudgeProvider,
    pub poll_interval_ms: u64,
    pub recent_limit: u32,
    pub dry_run: bool,
    pub once: bool,
}

impl JudgeWorkerOpts {
    pub fn from_env() -> JudgeWorkerOpts {
        JudgeWorkerOpts {
            provider: JudgeProvider::OpenAi,
            poll_interval_ms: num_env("JUDGE_POLL_INTERVAL_MS", 60_000.0) as u64,
            recent_limit: num_env("JUDGE_RECENT_LIMIT", 20.0) as u32,
            dry_run: !bool_env("JUDGE_WRITE_SCORES", false),
            once: bool_env("JUDGE_ONCE", false),
        }
    }
}

async fn process_trace(trace_id: &str, deps: &JudgeWorkerDeps<'_>, opts: &JudgeWorkerOpts) -> anyhow::Result<()> {
    let (_, nodes) = assemble_node_materials(deps.source, deps.skills, trace_id).await?;
    if nodes.is_empty() {
        // No generative node to score (e.g. a pure-agentic fallback run whose
        // only generations live inside an AGENT black box). Nothing to
        // persist; the trace stays unjudged but is cheap to re-skip.
        println!("[judge-worker] {trace_id} has no judgeable nodes — skipping");
        return Ok(());
    }
    let backend = judge_backend(opts.provider)?;
    println!("[judge-worker] judging {trace_id} provider={} nodes={}", opts.provider.as_str(), nodes.len());

    // Judge ALL nodes first. If any fails (e.g. a usage limit), we persist
    // NOTHING and the trace reappears next tick — whole-trace auto-retry,
    // per node. Sequential keeps the judge within its rate limit.
    let mut judged = Vec::new();
    for node in &nodes {
        judged.push((node, judge_node(&*backend, &node.judge_input(), false).await?));
    }
    for (node, verdict) in judged {
        deps.writer
            .write(
                &verdict,
                &NodeScoreWriteOpts {
                    trace_id,
                    observation_id: &node.observation_id,
                    node_kind: node.kind,
                    skill: &node.skill,
                    provider: opts.provider.as_str(),
                    prompt_version: JUDGE_PROMPT_VERSION,
                    dry_run: opts.dry_run,
                },
            )
            .await?;
    }
    Ok(())
}

async fn tick(deps: &JudgeWorkerDeps<'_>, opts: &JudgeWorkerOpts) -> anyhow::Result<()> {
    // The local source returns only COMPLETE runs (written on trace.end()) that
    // lack ANY judgement for this (provider, version) — so no age filter and no
    // separate dedup are needed. A trace that fails writes no rows and
    // reappears next tick: auto-retry.
    let traces =
        deps.source.recent_traces(opts.recent_limit, Some((opts.provider.as_str(), JUDGE_PROMPT_VERSION))).await?;
    for trace in traces {
        if let Err(err) = process_trace(&trace.id, deps, opts).await {
            eprintln!("[judge-worker] {} failed: {err:#}", trace.id);
        }
    }
    Ok(())
}

pub async fn run_judge_worker(deps: &JudgeWorkerDeps<'_>, opts: &JudgeWorkerOpts) -> anyhow::Result<()> {
    println!(
        "[judge-worker] start provider={} recent={} interval={}ms dryRun={}",
        opts.provider.as_str(),
        opts.recent_limit,
        opts.poll_interval_ms,
        opts.dry_run
    );
    loop {
        tick(deps, opts).await?;
        if opts.once {
            return Ok(());
        }
        // SIGINT / SIGTERM end the wait, and the loop with it.
        let mut term = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())?;
        tokio::select! {
            _ = tokio::time::sleep(Duration::from_millis(opts.poll_interval_ms)) => {}
            _ = tokio::signal::ctrl_c() => return Ok(()),
            _ = term.recv() => return Ok(()),
        }
    }
}

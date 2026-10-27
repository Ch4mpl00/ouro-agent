// Measure judge repeatability noise (σ_judge): re-judge the SAME nodes K times
// with an UNCHANGED judge, then report per-axis spread. The improver's gate uses
// this as the noise floor — a patch's Δ on an axis is only believable when it
// clears σ_judge. Noise is model-specific, so the result is keyed by
// (model, prompt version) and merged into a committed baseline file.
//
//   judge-noise <traceId...>            # nodes from these traces
//   judge-noise --recent 5              # nodes from 5 latest Langfuse traces
//   judge-noise --provider openai --runs 5 <traceId>
//   judge-noise --model gpt-5.4 --out path.json <traceId>
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use agent::db::AgentDb;
use agent::skills::FsSkillStore;
use evals::backend::{JudgeProvider, judge_backend};
use evals::env::{db_path, load_env, root};
use evals::langfuse_api::Langfuse;
use evals::materials::{NodeMaterial, assemble_node_materials};
use evals::node_judge::judge_node;
use evals::noise::{NodeNoiseMeta, NoiseContext, NoiseReport, collect_node_noise, summarize_noise};
use evals::schema::{JUDGE_MODEL, JUDGE_PROMPT_VERSION};
use evals::sigma::baseline_path;
use evals::source::LocalSource;
use futures::StreamExt;
use serde_json::{Map, Value};

struct Cli {
    provider: JudgeProvider,
    runs: usize,
    concurrency: usize,
    model: Option<String>,
    out: Option<String>,
    recent: Option<u32>,
    trace_ids: Vec<String>,
}

fn or(v: &str, fallback: f64) -> f64 {
    v.parse::<f64>().ok().filter(|x| *x != 0.0 && !x.is_nan()).unwrap_or(fallback)
}

fn parse_args(argv: &[String]) -> anyhow::Result<Cli> {
    let mut cli = Cli {
        provider: JudgeProvider::OpenAi,
        runs: 5,
        concurrency: 1,
        model: None,
        out: None,
        recent: None,
        trace_ids: Vec::new(),
    };
    let mut i = 0;
    while i < argv.len() {
        let arg = argv[i].clone();
        let flag = arg.split('=').next().unwrap_or("").to_owned();
        let mut value = || -> String {
            if let Some(v) = arg.strip_prefix(&format!("{flag}=")) {
                return v.to_owned();
            }
            i += 1;
            argv.get(i).cloned().unwrap_or_default()
        };
        match flag.as_str() {
            "--provider" => cli.provider = JudgeProvider::parse(&value())?,
            "--runs" => cli.runs = or(&value(), 5.0).max(2.0) as usize,
            "--concurrency" => cli.concurrency = or(&value(), 1.0).max(1.0) as usize,
            "--model" => cli.model = Some(value()).filter(|m| !m.is_empty()),
            "--out" => cli.out = Some(value()).filter(|o| !o.is_empty()),
            "--recent" => cli.recent = Some(or(&value(), 5.0).max(1.0) as u32),
            f if f.starts_with("--") => anyhow::bail!("unknown flag: {arg}"),
            _ => cli.trace_ids.push(arg.clone()),
        }
        i += 1;
    }
    Ok(cli)
}

fn print_report(r: &NoiseReport) {
    println!("\n=== JUDGE NOISE · {} ({}) · prompt {} ===", r.model, r.provider, r.prompt_version);
    println!("nodes={} · runs/node={} · judge calls={}\n", r.sample_nodes, r.runs, r.total_judge_calls);
    let head = ["axis", "nodes", "pooledσ", "meanσ", "p90σ", "maxσ", "meanScore", "flips"];
    let cells: Vec<String> =
        head.iter().enumerate().map(|(i, h)| if i == 0 { format!("{h:<18}") } else { format!("{h:>9}") }).collect();
    println!("{}", cells.join(" "));
    for a in &r.axes {
        println!(
            "{:<18} {:>9} {:>9.4} {:>9.4} {:>9.4} {:>9.4} {:>9.3} {:>9}",
            a.axis.as_str(),
            a.nodes,
            a.pooled_sigma,
            a.mean_sigma,
            a.p90_sigma,
            a.max_sigma,
            a.mean_score,
            a.applicability_flips
        );
    }
    println!(
        "\nGate guidance: use pooledσ as the per-axis noise floor; accept a patch's Δ only when Δ > k·pooledσ (k≈2) AND no holdout regression."
    );
}

// Merge this run into the committed baseline, keyed by (model|promptVersion) so
// each judge model accumulates its own known noise; re-measuring overwrites.
fn merge_baseline(out: &std::path::Path, report: &NoiseReport) -> anyhow::Result<()> {
    let mut existing: Map<String, Value> =
        std::fs::read_to_string(out).ok().and_then(|t| serde_json::from_str(&t).ok()).unwrap_or_default();
    let mut entry = serde_json::to_value(report)?;
    entry["measuredAt"] = agent::local_recorder::now_iso().into();
    let key = format!("{}|{}", report.model, report.prompt_version);
    existing.insert(key.clone(), entry);
    let mut sorted: Vec<(String, Value)> = existing.into_iter().collect();
    sorted.sort_by(|a, b| a.0.cmp(&b.0));
    std::fs::write(out, format!("{}\n", serde_json::to_string_pretty(&Value::Object(sorted.into_iter().collect()))?))?;
    println!("\n[judge-noise] baseline updated → {} (key {key})", out.display());
    Ok(())
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    load_env();
    let argv: Vec<String> = std::env::args().skip(1).collect();
    let cli = parse_args(&argv)?;
    let root = root()?;
    let skills = FsSkillStore::new(&root);
    let langfuse = Langfuse::from_env()?;
    let source = LocalSource { db: AgentDb::open(db_path(&root))?, fallback: Some(Arc::new(langfuse.clone())) };

    let trace_ids = match cli.recent {
        Some(n) => langfuse.recent_traces(n).await?.into_iter().map(|t| t.id).collect(),
        None => cli.trace_ids.clone(),
    };
    if trace_ids.is_empty() {
        eprintln!(
            "usage: judge-noise [--provider openai] [--runs K] [--concurrency C] [--model label] [--out file] (<traceId...> | --recent N)"
        );
        std::process::exit(1);
    }

    // Flatten every judgeable node across the requested traces.
    let mut nodes: Vec<NodeMaterial> = Vec::new();
    for trace_id in &trace_ids {
        match assemble_node_materials(&source, &skills, trace_id).await {
            Ok((_, found)) => {
                eprintln!("[judge-noise] {trace_id} → {} nodes", found.len());
                nodes.extend(found);
            }
            Err(err) => eprintln!("[judge-noise] {trace_id} failed: {err:#}"),
        }
    }
    if nodes.is_empty() {
        eprintln!("[judge-noise] no judgeable nodes found");
        std::process::exit(1);
    }

    let backend = judge_backend(cli.provider)?;
    eprintln!(
        "[judge-noise] judging {} nodes × {} runs (provider={}, concurrency={})",
        nodes.len(),
        cli.runs,
        cli.provider.as_str(),
        cli.concurrency
    );
    // Re-judge each node K times. One flat task list keeps the concurrency cap
    // honest across nodes; callers leave concurrency=1 to respect rate limits.
    let tasks: Vec<&NodeMaterial> = nodes.iter().flat_map(|n| std::iter::repeat_n(n, cli.runs)).collect();
    let total = tasks.len();
    let done = AtomicUsize::new(0);
    let verdicts: Vec<(String, _)> = futures::stream::iter(tasks)
        .map(|node| {
            let (backend, done) = (&backend, &done);
            async move {
                let verdict = judge_node(&**backend, &node.judge_input(), false).await?;
                let n = done.fetch_add(1, Ordering::SeqCst) + 1;
                if n % 5 == 0 || n == total {
                    eprintln!("[judge-noise] {n}/{total} judge calls");
                }
                anyhow::Ok((node.observation_id.clone(), verdict))
            }
        })
        .buffered(cli.concurrency)
        .collect::<Vec<_>>()
        .await
        .into_iter()
        .collect::<anyhow::Result<_>>()?;

    // Regroup verdicts by node, in node order.
    let mut by_node: HashMap<String, Vec<_>> = HashMap::new();
    for (id, verdict) in verdicts {
        by_node.entry(id).or_default().push(verdict);
    }
    let node_noise: Vec<_> = nodes
        .iter()
        .map(|n| {
            let meta = NodeNoiseMeta {
                observation_id: n.observation_id.clone(),
                label: n.label.clone(),
                kind: n.kind.as_str().into(),
                skill: n.skill.clone(),
            };
            collect_node_noise(meta, by_node.get(&n.observation_id).map(Vec::as_slice).unwrap_or(&[]))
        })
        .collect();
    let model = cli.model.clone().unwrap_or_else(|| match cli.provider {
        JudgeProvider::OpenAi => JUDGE_MODEL.into(),
    });
    let report = summarize_noise(
        NoiseContext {
            model,
            provider: cli.provider.as_str().into(),
            prompt_version: JUDGE_PROMPT_VERSION.into(),
            runs: cli.runs,
        },
        &node_noise,
    );
    print_report(&report);
    let out = cli.out.map(std::path::PathBuf::from).unwrap_or_else(|| baseline_path(&root));
    merge_baseline(&out, &report)
}

// Improver gate CLI (Phase 2, slice 1): A/B a CANDIDATE patch on a skill's nodes
// over a frozen recorded trace, re-judge, and report whether the target axis
// moved beyond σ_judge (the noise floor from judge-noise).
//
//   judge-gate <traceId> --skill <skill> --patch <file.md>
//              [--node <labelSubstr>] [--axis <axis>] [--samples N] [--provider openai] [--k 2]
//
// The generator is re-run under the RECORDED model (what prod uses); the judge
// uses the OpenAI judge. Δ is graded against the committed
// noise baseline for (judge model | prompt version).
use std::collections::HashMap;
use std::sync::Arc;

use agent::db::AgentDb;
use agent::skills::FsSkillStore;
use evals::backend::{JudgeProvider, judge_backend};
use evals::env::{db_path, load_env, root};
use evals::gate::{GateVerdict, run_node_gate};
use evals::gate_runtime::{ModelGenerator, build_gate_target};
use evals::langfuse_api::Langfuse;
use evals::materials::assemble_node_materials;
use evals::node_judge::judge_node;
use evals::noise::{NoiseAxis, extract_axis_scores};
use evals::schema::{JUDGE_MODEL, JUDGE_PROMPT_VERSION, JudgeNodeInput};
use evals::sigma::load_sigma_baseline;
use evals::source::{LocalSource, TraceSource};

struct Cli {
    trace_id: String,
    skill: String,
    patch_file: String,
    axis: Option<NoiseAxis>,
    node: Option<String>,
    samples: usize,
    provider: JudgeProvider,
    k: f64,
}

fn or(v: &str, fallback: f64) -> f64 {
    v.parse::<f64>().ok().filter(|x| *x != 0.0 && !x.is_nan()).unwrap_or(fallback)
}

fn parse_args(argv: &[String]) -> anyhow::Result<Cli> {
    let (mut skill, mut patch_file, mut axis, mut node) = (None, None, None, None);
    let (mut samples, mut provider, mut k) = (3usize, JudgeProvider::OpenAi, 2.0);
    let mut rest = Vec::new();
    let mut i = 0;
    while i < argv.len() {
        let arg = argv[i].clone();
        let flag = arg.split('=').next().unwrap_or("").to_owned();
        let mut val = || -> String {
            if let Some(v) = arg.strip_prefix(&format!("{flag}=")) {
                return v.to_owned();
            }
            i += 1;
            argv.get(i).cloned().unwrap_or_default()
        };
        match flag.as_str() {
            "--skill" => skill = Some(val()),
            "--patch" => patch_file = Some(val()),
            "--node" => node = Some(val()),
            "--axis" => axis = Some(NoiseAxis::parse(&val())?),
            "--samples" => samples = or(&val(), 3.0).max(1.0) as usize,
            "--k" => k = or(&val(), 2.0).max(0.0),
            "--provider" => provider = JudgeProvider::parse(&val())?,
            f if f.starts_with("--") => anyhow::bail!("unknown flag: {arg}"),
            _ => rest.push(arg.clone()),
        }
        i += 1;
    }
    match (rest.into_iter().next(), skill, patch_file) {
        (Some(trace_id), Some(skill), Some(patch_file)) if !skill.is_empty() && !patch_file.is_empty() => {
            Ok(Cli { trace_id, skill, patch_file, axis, node, samples, provider, k })
        }
        _ => {
            eprintln!(
                "usage: judge-gate <traceId> --skill <skill> --patch <file.md> [--node <labelSubstr>] [--axis <axis>] [--samples N] [--provider openai] [--k 2]"
            );
            std::process::exit(1);
        }
    }
}

fn glyph(v: GateVerdict) -> &'static str {
    match v {
        GateVerdict::Improve => "✅ improve",
        GateVerdict::Regress => "🔻 regress",
        GateVerdict::Noise => "≈ noise",
        GateVerdict::NoBaseline => "? no-baseline",
        GateVerdict::NotApplicable => "— n/a",
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    load_env();
    let argv: Vec<String> = std::env::args().skip(1).collect();
    let cli = parse_args(&argv)?;
    let patch = std::fs::read_to_string(&cli.patch_file)?;
    let root = root()?;
    let skills = FsSkillStore::new(&root);
    let source = LocalSource { db: AgentDb::open(db_path(&root))?, fallback: Some(Arc::new(Langfuse::from_env()?)) };

    let (trace, observations) = source.get_trace(&cli.trace_id).await?;
    let by_id: HashMap<&str, _> = observations.iter().map(|o| (o.id.as_str(), o)).collect();
    let (_, nodes) = assemble_node_materials(&source, &skills, &cli.trace_id).await?;
    let targets: Vec<_> = nodes
        .iter()
        .filter(|n| n.skill == cli.skill)
        .filter(|n| cli.node.as_ref().is_none_or(|sub| n.label.contains(sub.as_str())))
        .collect();
    if targets.is_empty() {
        let mut present: Vec<&str> = Vec::new();
        for n in &nodes {
            if !present.contains(&n.skill.as_str()) {
                present.push(&n.skill);
            }
        }
        let present = if present.is_empty() { "(none)".into() } else { present.join(", ") };
        eprintln!("[judge-gate] no nodes for skill \"{}\" in {}. Skills present: {present}", cli.skill, cli.trace_id);
        std::process::exit(1);
    }

    let judge_model = match cli.provider {
        JudgeProvider::OpenAi => JUDGE_MODEL.to_owned(),
    };
    let (sigma, found) = load_sigma_baseline(&root, &judge_model, JUDGE_PROMPT_VERSION);

    println!("\n=== IMPROVER GATE · trace {} · {} ===", cli.trace_id, trace.name);
    println!(
        "skill={} · patch={} ({} chars) · judge={}({judge_model}) prompt={JUDGE_PROMPT_VERSION} · samples={} · k={}",
        cli.skill,
        cli.patch_file,
        patch.trim().encode_utf16().count(),
        cli.provider.as_str(),
        cli.samples,
        cli.k
    );
    if !found {
        println!(
            "⚠ no σ baseline for {judge_model}|{JUDGE_PROMPT_VERSION} — run `judge-noise --provider {}` first; verdicts will read \"no-baseline\".",
            cli.provider.as_str()
        );
    }
    println!("{} target node(s)\n", targets.len());

    let backend = judge_backend(cli.provider)?;
    for node in targets {
        let Some(target) = build_gate_target(node, by_id.get(node.observation_id.as_str()).copied()) else {
            println!("── {} · SKIP (no recorded model and AGENT_MODEL unset) ──\n", node.label);
            continue;
        };
        println!("── node {} · {} · model {} ──", node.label, node.kind.as_str(), target.model);
        // Standalone A/B tool: no stored corpus score here, so derive the
        // "before" by judging the recorded output ONCE (the improver path
        // reads it for free).
        let before = judge_node(
            &*backend,
            &JudgeNodeInput {
                kind: target.kind,
                skill: target.skill.clone(),
                contract: target.contract.clone(),
                input_text: target.input_text.clone(),
                output_text: target.original_output.clone(),
            },
            false,
        )
        .await?;
        let stored = extract_axis_scores(&before);
        let result =
            run_node_gate(&*backend, &ModelGenerator, &target, &patch, cli.samples, &sigma, cli.k, &stored, false)
                .await?;
        for g in result.grades.iter().filter(|g| cli.axis.is_none_or(|a| a == g.axis)) {
            let fmt = |x: Option<f64>| x.map_or(" —  ".into(), |x| format!("{x:.3}"));
            let dz = match (g.delta, g.sigma) {
                (Some(d), Some(s)) if s != 0.0 => format!(" ({:.1}σ)", d / s),
                _ => String::new(),
            };
            let delta = g.delta.map_or(" — ".into(), |d| format!("{}{d:.3}", if d >= 0.0 { "+" } else { "" }));
            println!(
                "  {:<18} before {} → after {}  Δ {delta}{dz}  [{}]",
                g.axis.as_str(),
                fmt(g.before_mean),
                fmt(g.after_mean),
                glyph(g.verdict)
            );
        }
        println!();
    }
    println!(
        "Gate rule: accept the patch only if the TARGET axis is `improve` AND no other axis flips to `regress` (holdout regression check comes in Phase 3)."
    );
    Ok(())
}

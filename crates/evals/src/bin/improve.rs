// Closed-loop improver CLI (Phase 3): read the judged corpus for one (skill,
// axis), select recent failures (absolute+σ), induce a failure-mode taxonomy,
// author an append-only lesson for the dominant mode, GATE it (replay over the
// frozen cluster + holdout, stored before, Δ vs σ), and — with --apply — ship it
// to skills/<skill>.patch.md. Without --apply it's propose-only. The cron worker
// (improve-worker) drives the SAME cycle per (skill, axis) on a loop, plus the
// live-trend monitor + auto-revert that span runs.
//
//   improve --skill news-digest --axis coverage [--cluster 3] [--holdout 3]
//           [--samples 2] [--absMax 0.6] [--bar 0.75] [--holdoutMin 0.85]
//           [--recentDays 14] [--k 2] [--budget 8] [--maxAttempts 1]
//           [--guard-faithfulness] [--provider openai] [--apply]
use agent::db::AgentDb;
use agent::skills::FsSkillStore;
use evals::backend::{JudgeProvider, judge_backend};
use evals::env::{db_path, load_env, root};
use evals::gate_runtime::ModelGenerator;
use evals::improve_cycle::{CycleOutcome, ImproveCycleOpts, ImproveDeps, run_improve_cycle};
use evals::noise::NoiseAxis;
use evals::schema::JUDGE_PROMPT_VERSION;
use evals::sigma::load_sigma_baseline;
use evals::source::local_with_langfuse;

const USAGE: &str = "usage: improve --skill <skill> --axis <axis> [--cluster N] [--holdout M] [--samples K] [--absMax 0.6] [--bar 0.75] [--holdoutMin 0.85] [--recentDays 14] [--k 2] [--budget 8] [--maxAttempts 1] [--guard-faithfulness] [--provider openai] [--apply]";

// JS `Number(v) || fallback`: unparsable or zero falls back.
fn or(v: &str, fallback: f64) -> f64 {
    v.parse::<f64>().ok().filter(|x| *x != 0.0 && !x.is_nan()).unwrap_or(fallback)
}

fn parse_args(argv: &[String]) -> anyhow::Result<ImproveCycleOpts> {
    let mut o = ImproveCycleOpts {
        skill: String::new(),
        axis: NoiseAxis::Coverage,
        provider: JudgeProvider::OpenAi,
        cluster: 3,
        holdout: 3,
        samples: 2,
        abs_max: 0.6,
        bar: 0.75,
        holdout_min: 0.85,
        recent_days: 14.0,
        k: 2.0,
        apply: false,
        max_attempts: 1,
        budget: 8,
        guard_faithfulness: false,
        now_ms: chrono::Utc::now().timestamp_millis(),
    };
    let mut axis: Option<NoiseAxis> = None;
    let mut i = 0;
    while i < argv.len() {
        let arg = argv[i].clone();
        let mut val = |flag: &str| -> String {
            if let Some(v) = arg.strip_prefix(&format!("{flag}=")) {
                return v.to_owned();
            }
            i += 1;
            argv.get(i).cloned().unwrap_or_default()
        };
        let flag = arg.split('=').next().unwrap_or("").to_owned();
        match flag.as_str() {
            "--skill" => o.skill = val("--skill"),
            "--axis" => axis = Some(NoiseAxis::parse(&val("--axis"))?),
            "--cluster" => o.cluster = or(&val("--cluster"), 3.0).max(1.0) as usize,
            "--holdout" => o.holdout = or(&val("--holdout"), 3.0).max(0.0) as usize,
            "--samples" => o.samples = or(&val("--samples"), 2.0).max(1.0) as usize,
            "--absMax" => o.abs_max = or(&val("--absMax"), 0.6),
            "--bar" => o.bar = or(&val("--bar"), 0.75),
            "--holdoutMin" => o.holdout_min = or(&val("--holdoutMin"), 0.85),
            "--recentDays" => o.recent_days = or(&val("--recentDays"), 14.0).max(0.0),
            "--k" => o.k = or(&val("--k"), 2.0).max(0.0),
            "--budget" => o.budget = or(&val("--budget"), 8.0).max(1.0) as usize,
            "--maxAttempts" => o.max_attempts = or(&val("--maxAttempts"), 1.0).max(1.0) as usize,
            "--guard-faithfulness" => o.guard_faithfulness = true,
            "--provider" => o.provider = JudgeProvider::parse(&val("--provider"))?,
            "--apply" => o.apply = true,
            _ => anyhow::bail!("unknown flag: {arg}"),
        }
        i += 1;
    }
    match axis {
        Some(a) if !o.skill.is_empty() => o.axis = a,
        _ => {
            eprintln!("{USAGE}");
            std::process::exit(1);
        }
    }
    Ok(o)
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    load_env();
    let argv: Vec<String> = std::env::args().skip(1).collect();
    let opts = parse_args(&argv)?;
    let root = root()?;
    let db = AgentDb::open(db_path(&root))?;
    let source = local_with_langfuse(db.clone())?;
    let skills = FsSkillStore::new(&root);
    let backend = judge_backend(opts.provider)?;
    let judge_model = match opts.provider {
        JudgeProvider::OpenAi => "openai".to_owned(),
    };
    let (sigma, _) = load_sigma_baseline(&root, &judge_model, JUDGE_PROMPT_VERSION);
    let axis_sigma = sigma.get(&opts.axis).copied();

    println!("\n=== IMPROVER · skill={} · axis={} ===", opts.skill, opts.axis.as_str());
    println!(
        "judge={} prompt={JUDGE_PROMPT_VERSION} · σ={} · candidate<{} & <{}−{}σ · recent={}d · cluster≤{} · holdout≥{} ×{} · samples={}/1 · budget={} · apply={}",
        opts.provider.as_str(),
        axis_sigma.map_or("—".into(), |s| format!("{s:.3}")),
        opts.abs_max,
        opts.bar,
        opts.k,
        opts.recent_days,
        opts.cluster,
        opts.holdout_min,
        opts.holdout,
        opts.samples,
        opts.budget,
        opts.apply
    );
    let log = |m: &str| println!("{m}");
    let deps = ImproveDeps {
        db: &db,
        source: &source,
        skills: &skills,
        backend: &*backend,
        generator: &ModelGenerator,
        sigma: &sigma,
        log: &log,
    };
    let result = run_improve_cycle(&deps, &opts).await?;
    println!("\n── outcome: {} ──", result.outcome.as_str());
    match result.outcome {
        CycleOutcome::Shipped => println!(
            "Shipped to {}. Monitor the live {} trend; revert = delete the lesson/file.",
            result.patch_path.unwrap_or_default(),
            opts.axis.as_str()
        ),
        CycleOutcome::Accepted => {
            println!("Accepted (propose-only). Re-run with --apply to ship to skills/{}.patch.md.", opts.skill)
        }
        _ => {}
    }
    Ok(())
}

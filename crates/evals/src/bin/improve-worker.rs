// Closed-loop improver cron (Phase 3, part 3; compose service `improve-worker`,
// `evals` profile): walks every (skill, axis) in the judged corpus, monitors
// the prod trend of the last ship (auto-reverting if it didn't hold), and
// otherwise runs one improve cycle. Ships ONLY when IMPROVE_APPLY is set —
// otherwise shadow mode (proposes + gates, never writes a .patch.md).
use std::io::Write;

use agent::db::AgentDb;
use agent::skills::FsSkillStore;
use evals::backend::{JudgeProvider, judge_backend};
use evals::env::{db_path, load_env, root};
use evals::gate_runtime::ModelGenerator;
use evals::improve_cycle::ImproveDeps;
use evals::improve_worker::{ImproveWorkerOpts, WorkerDeps, run_improve_worker};
use evals::schema::JUDGE_PROMPT_VERSION;
use evals::sigma::load_sigma_baseline;
use evals::source::local_with_langfuse;
use serde_json::Value;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    load_env();
    let opts = ImproveWorkerOpts::from_env();
    let root = root()?;
    let db = AgentDb::open(db_path(&root))?;
    let source = local_with_langfuse(db.clone())?;
    let skills = FsSkillStore::new(&root);
    let backend = judge_backend(opts.provider)?;
    let judge_model = match opts.provider {
        JudgeProvider::OpenAi => "openai".to_owned(),
        JudgeProvider::Codex => std::env::var("CODEX_JUDGE_MODEL").unwrap_or_else(|_| "codex".into()),
    };
    let (sigma, found) = load_sigma_baseline(&root, &judge_model, JUDGE_PROMPT_VERSION);
    if !found {
        eprintln!(
            "[improve-worker] no σ baseline for {judge_model} — gate verdicts read \"no-baseline\" and the monitor can't bound noise. Run `judge-noise --provider {}` first.",
            opts.provider.as_str()
        );
    }
    // Durable audit trail on the agent-data volume — survives container
    // recreates (unlike `docker compose logs`).
    let audit_path =
        std::env::var("IMPROVE_AUDIT_PATH").unwrap_or_else(|_| "packages/agent/data/improver-audit.jsonl".into());
    let audit = move |entry: Value| {
        let written = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(&audit_path)
            .and_then(|mut f| writeln!(f, "{entry}"));
        if let Err(err) = written {
            eprintln!("[improve-worker] audit write failed: {err}");
        }
    };
    let log = |m: &str| println!("{m}");
    let deps = WorkerDeps {
        cycle: ImproveDeps {
            db: &db,
            source: &source,
            skills: &skills,
            backend: &*backend,
            generator: &ModelGenerator,
            sigma: &sigma,
            log: &log,
        },
        audit: &audit,
    };
    run_improve_worker(&deps, &opts).await;
    Ok(())
}

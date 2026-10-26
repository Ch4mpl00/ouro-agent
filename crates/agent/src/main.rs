// Process composition root. All environment, clients, filesystem roots and
// background tasks are owned here. No production pollers live in this crate.
use agent::{
    activity::{ActivityOptions, Notes, create_activity_worker},
    agent::{EngineDeps, Providers, create_engine},
    codex::codex_client,
    db::{AgentDb, DEFAULT_DB_PATH},
    decisions::{DecisionClient, OPENROUTER_DECISIONS_URL},
    errors::{is_aborted, or_cancel},
    langfuse::LangfuseTracer,
    local_recorder::local_recorder_tracer,
    mcp::{ConnectOptions, Mcp, McpClient, RETRY_UNTIL_UP, open_from_env},
    models::{DEFAULT_DECISION_MODEL, Presets, default_presets},
    providers::{
        DEEPSEEK_BASE_URL, GEMINI_BASE_URL, HttpApi, OPENAI_BASE_URL, ProviderKind, RetryOpts, SharedProvider,
        openrouter_api, provider, with_retry,
    },
    schedule::{Schedules, run_drain, run_zone_watch},
    session::{EnvConfig, parse_telegram_topics, telegram_env_note},
    skills::FsSkillStore,
    supervisor::{ERROR_BACKOFF, NextSignal, create_supervisor_with_cancel, run_poll_loop},
    tracing::{Tracer, tee_tracer},
};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;
use tokio_util::sync::CancellationToken;

fn required(name: &str, detail: &str) -> anyhow::Result<String> {
    std::env::var(name)
        .ok()
        .filter(|s| !s.is_empty())
        .ok_or_else(|| anyhow::anyhow!("{name} is not set in .env{detail}"))
}
fn positive(name: &str, fallback: u64) -> anyhow::Result<u64> {
    match std::env::var(name).ok().filter(|s| !s.is_empty()) {
        None => Ok(fallback),
        Some(raw) => raw
            .parse::<u64>()
            .ok()
            .filter(|n| *n > 0)
            .ok_or_else(|| anyhow::anyhow!("{name} must be a positive integer")),
    }
}
fn root() -> anyhow::Result<PathBuf> {
    if let Some(path) = std::env::var_os("AGENT_ROOT") {
        return Ok(PathBuf::from(path));
    }
    let mut path = std::env::current_dir()?;
    loop {
        if path.join("skills.default").is_dir() {
            return Ok(path);
        }
        if !path.pop() {
            anyhow::bail!("cannot find skills.default; set AGENT_ROOT or run from the repository root");
        }
    }
}
struct Config {
    root: PathBuf,
    db: PathBuf,
    deepseek_key: String,
    openai_key: String,
    gemini_key: String,
    router_key: String,
    presets: Presets,
    journal: bool,
    journal_model: String,
    journal_options: ActivityOptions,
    // The environment note a scheduled fire carries.
    env_context: Option<String>,
    // A workflow's default destination (`env.telegramChatId`).
    telegram_chat_id: Option<String>,
}
fn configuration() -> anyhow::Result<Config> {
    let deepseek_key = required("DEEPSEEK_API_KEY", "")?;
    let openai_key = required("OPENAI_API_KEY", "")?;
    let gemini_key = required("GEMINI_API_KEY", " (smart agents)")?;
    let router_key = required("OPEN_ROUTER_API_KEY", " (decide steps)")?;
    let root = root()?;
    let db = std::env::var_os("AGENT_DB_PATH").map(PathBuf::from).unwrap_or_else(|| root.join(DEFAULT_DB_PATH));
    let mut presets = default_presets();
    for (preset, name) in [
        (&mut presets.base, "AGENT_BASE_MODEL"),
        (&mut presets.smart, "AGENT_SMART_MODEL"),
        (&mut presets.smartest, "AGENT_SMARTEST_MODEL"),
        (&mut presets.compiler, "AGENT_COMPILER_MODEL"),
        (&mut presets.compose, "AGENT_COMPOSE_MODEL"),
    ] {
        if let Ok(model) = std::env::var(name) {
            preset.model = model;
        }
    }
    let mut journal_options = ActivityOptions::default();
    journal_options.poll_interval = Duration::from_millis(positive(
        "ACTIVITY_JOURNAL_POLL_INTERVAL_MS",
        journal_options.poll_interval.as_millis() as u64,
    )?);
    journal_options.batch_size =
        usize::try_from(positive("ACTIVITY_JOURNAL_BATCH_SIZE", journal_options.batch_size as u64)?)?;
    journal_options.max_generation_attempts =
        u32::try_from(positive("ACTIVITY_JOURNAL_MAX_ATTEMPTS", journal_options.max_generation_attempts.into())?)?;
    journal_options.budget.total_chars =
        usize::try_from(positive("ACTIVITY_JOURNAL_MAX_INPUT_CHARS", journal_options.budget.total_chars as u64)?)?;
    let journal_model = std::env::var("ACTIVITY_JOURNAL_MODEL").unwrap_or_else(|_| presets.base.model.clone());
    // The same two variables MCP reads for the note on a queued signal.
    let var = |name: &str| std::env::var(name).ok().filter(|v| !v.is_empty());
    let default_chat_id = var("TELEGRAM_DEFAULT_CHAT_ID");
    if default_chat_id.is_none() {
        eprintln!("[config] TELEGRAM_DEFAULT_CHAT_ID is not set: scheduled tasks run without a default chat");
    }
    let topics = parse_telegram_topics(var("TELEGRAM_TOPICS_JSON").as_deref());
    let env_context = telegram_env_note(default_chat_id.as_deref(), &topics);
    let telegram_chat_id = default_chat_id;
    Ok(Config {
        root,
        db,
        deepseek_key,
        openai_key,
        gemini_key,
        router_key,
        presets,
        journal: std::env::var("ACTIVITY_JOURNAL").as_deref() != Ok("off"),
        journal_model,
        journal_options,
        env_context,
        telegram_chat_id,
    })
}
fn retry_provider(kind: ProviderKind, api: Arc<dyn agent::providers::JsonApi>) -> SharedProvider {
    Arc::new(with_retry(Arc::new(provider(kind, api)), RetryOpts::default()))
}
async fn stop_signal(cancel: CancellationToken) -> anyhow::Result<()> {
    #[cfg(unix)]
    {
        let mut term = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())?;
        tokio::select! {_=tokio::signal::ctrl_c()=>{},_=term.recv()=>{},_=cancel.cancelled()=>return Ok(())}
    }
    #[cfg(not(unix))]
    {
        tokio::select! {_=tokio::signal::ctrl_c()=>{},_=cancel.cancelled()=>return Ok(())}
    }
    cancel.cancel();
    Ok(())
}
fn log_journal_result(result: Result<anyhow::Result<()>, tokio::task::JoinError>) {
    match result {
        Ok(Err(error)) if !is_aborted(&error) => eprintln!("[supervisor] activity journal stopped: {error}"),
        Err(error) => eprintln!("[supervisor] activity journal task failed: {error}"),
        _ => {}
    }
}
async fn run(config: Config, once: bool) -> anyhow::Result<()> {
    let stopping = CancellationToken::new();
    let signals = tokio::spawn(stop_signal(stopping.clone()));
    let result = async {
        let db = Arc::new(AgentDb::open(&config.db)?);
        let skills = Arc::new(FsSkillStore::new(&config.root));
        let openai_url = std::env::var("OPENAI_BASE_URL").unwrap_or_else(|_| OPENAI_BASE_URL.into());
        let router = Arc::new(openrouter_api(&config.router_key));
        let providers = Providers {
            deepseek: retry_provider(
                ProviderKind::Deepseek,
                Arc::new(HttpApi::new(DEEPSEEK_BASE_URL, &config.deepseek_key)),
            ),
            openai: retry_provider(ProviderKind::Openai, Arc::new(HttpApi::new(&openai_url, &config.openai_key))),
            gemini: retry_provider(ProviderKind::Gemini, Arc::new(HttpApi::new(GEMINI_BASE_URL, &config.gemini_key))),
            openrouter: Some(retry_provider(ProviderKind::Openrouter, router.clone())),
        };
        eprintln!("[supervisor] connecting to mcp…");
        let mcp = Arc::new(
            or_cancel(
                &stopping,
                McpClient::connect(
                    open_from_env()?,
                    ConnectOptions { startup_retry: RETRY_UNTIL_UP, ..Default::default() },
                ),
            )
            .await?,
        );
        let mut registry = mcp.tools().into_iter().map(|t| t.name).collect::<Vec<_>>();
        registry.sort();
        if let Err(error) = skills.validate_all(&registry).await {
            mcp.close().await;
            return Err(error);
        }
        eprintln!("[supervisor] mcp connected; skill validation passed ({} tools)", registry.len());
        let local: Arc<dyn Tracer> = Arc::new(local_recorder_tracer(db.clone()));
        let tracer: Arc<dyn Tracer> = if let Some(config) = LangfuseTracer::from_env() {
            Arc::new(tee_tracer(Arc::new(LangfuseTracer::new(config)), local))
        } else {
            local
        };
        // The engine's store: engine.db beside agent.db (runs.rs §1), and a
        // read-only view of it for dry runs, opened once it migrated.
        let store = agent::runs::open_store(&config.db)?;
        let dry_store = agent::runs::open_store_view(&config.db)?;
        let engine = create_engine(EngineDeps {
            providers,
            mcp: mcp.clone(),
            presets: config.presets.clone(),
            skills: vec!["orchestrator".into()],
            skill_store: skills.clone(),
            memory: db.clone(),
            schedule: Arc::new(Schedules::new(db.clone())),
            env: EnvConfig {
                user_email: std::env::var("USER_EMAIL").ok(),
                telegram_chat_id: config.telegram_chat_id.clone(),
            },
            db: (*db).clone(),
            store,
            dry_store,
            tracer,
            codex: codex_client(&std::env::var("CODEX_URL").unwrap_or_else(|_| "http://localhost:3010".into())),
            decisions: Some(Arc::new(
                DecisionClient::with_retry(
                    std::env::var("AGENT_DECISION_MODEL").unwrap_or_else(|_| DEFAULT_DECISION_MODEL.into()),
                    router,
                    RetryOpts::default(),
                )
                .with_endpoint(
                    std::env::var("OPENROUTER_DECISIONS_URL").unwrap_or_else(|_| OPENROUTER_DECISIONS_URL.into()),
                ),
            )),
        });
        let setup = async {
            // MCP answered its tools/list above: the snapshot grows by its
            // tools, and only now does the engine worker start resuming runs
            // (design 03, decision 10). The built-ins are installed first.
            engine.refresh_tools()?;
            engine.ready().await?;
            let supervisor = create_supervisor_with_cancel(engine.clone(), stopping.child_token());
            let journal_cancel = CancellationToken::new();
            let mut journal = None;
            if config.journal && !once {
                let notes = Arc::new(Notes {
                    provider: engine.resolve_provider(&config.journal_model)?,
                    model: config.journal_model.clone(),
                    reasoning_effort: config.presets.base.reasoning_effort,
                    timeout: Duration::from_secs(120),
                });
                let worker = create_activity_worker(db.clone(), notes, mcp.clone(), config.journal_options)?;
                let cancel = journal_cancel.clone();
                journal = Some(tokio::spawn(async move { worker.run(cancel).await }));
            }
            eprintln!(
                "[supervisor] entering main loop (scheduler → saved workflow when named, otherwise → agent-loop)"
            );
            // Scheduled tasks are engine definitions (`task-<id>`): their
            // triggers record fires, and the drain runs each one through the
            // supervisor, one signal at a time with the queue's. Nothing is
            // seeded (schedule.rs header); the zone watch keeps recurring
            // tasks on the user's zone (schedule.rs, design 06). Not in
            // `--once`: fires recorded then wait for the next normal start.
            let schedule_cancel = stopping.child_token();
            let mut scheduled = Vec::new();
            if !once {
                scheduled.push(tokio::spawn(run_drain(
                    supervisor.clone(),
                    config.env_context.clone(),
                    schedule_cancel.clone(),
                )));
                scheduled.push(tokio::spawn(run_zone_watch(engine.clone(), schedule_cancel.clone())));
            }
            // The drain and the watch end before the supervisor shuts down.
            let stop_scheduled = async |scheduled: &mut Vec<tokio::task::JoinHandle<()>>| {
                schedule_cancel.cancel();
                for task in scheduled.drain(..) {
                    let _ = task.await;
                }
            };
            let outcome = if once {
                let raw = mcp.call_tool("get_next_signal", serde_json::Map::new(), &stopping).await;
                match raw {
                    Ok(raw) => match serde_json::from_str::<NextSignal>(&raw) {
                        Ok(next) => {
                            if let Some(signal) = next.signal {
                                supervisor.run_signal(&signal).await.map(|_| ())
                            } else {
                                Ok(())
                            }
                        }
                        Err(error) => Err(error.into()),
                    },
                    Err(error) => Err(error),
                }
            } else {
                let mcp = mcp.clone();
                let supervisor_run = supervisor.clone();
                let poll_cancel = stopping.clone();
                let mut poll =
                    tokio::spawn(async move { run_poll_loop(mcp, supervisor_run, poll_cancel, ERROR_BACKOFF).await });
                tokio::select! {
                    result = &mut poll => result.map_err(anyhow::Error::from).and_then(|result| result),
                    _ = stopping.cancelled() => {
                        journal_cancel.cancel();
                        if let Some(task) = journal.take() { log_journal_result(task.await); }
                        stop_scheduled(&mut scheduled).await;
                        supervisor.shutdown().await;
                        poll.await.map_err(anyhow::Error::from).and_then(|result| result)
                    }
                }
            };
            stop_scheduled(&mut scheduled).await;
            journal_cancel.cancel();
            if let Some(task) = journal {
                log_journal_result(task.await);
            }
            supervisor.shutdown().await;
            outcome
        }
        .await;
        engine.shutdown().await;
        setup
    }
    .await;
    stopping.cancel();
    let _ = signals.await;
    match result {
        Err(error) if is_aborted(&error) => Ok(()),
        result => result,
    }
}
#[tokio::main]
async fn main() {
    // dotenv/config semantics: cwd only, no parent-directory credential search.
    let _ = dotenvy::from_path(".env");
    let args: Vec<_> = std::env::args().skip(1).collect();
    if args.iter().any(|a| a == "--help" || a == "-h") {
        println!(
            "agent-supervisor [--once | --check-config]\nDefault: serial signal supervisor with the activity journal.\n--once: handle at most one signal, then close all resources.\n--check-config: validate environment without opening databases or services."
        );
        return;
    }
    let result = async {
        anyhow::ensure!(args.iter().all(|a| a == "--once" || a == "--check-config"), "unknown argument; use --help");
        let config = configuration()?;
        if args.iter().any(|a| a == "--check-config") {
            println!("[supervisor] configuration valid");
            return Ok(());
        }
        run(config, args.iter().any(|a| a == "--once")).await
    }
    .await;
    if let Err(error) = result {
        eprintln!("[supervisor] fatal: {error:#}");
        std::process::exit(1);
    }
}

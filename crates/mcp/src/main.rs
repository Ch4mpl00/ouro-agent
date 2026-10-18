// Composition root of the MCP server — the only place that knows the whole
// graph. Builds every long-lived dependency once, threads it into the tool
// handler and the pollers, and owns shutdown.

use std::path::PathBuf;
use std::sync::Arc;

use anyhow::Context;
use mcp_tools::db::Db;
use mcp_tools::embeddings::{OpenAiEmbedder, SharedEmbedder};
use mcp_tools::gateway::{self, Gateway};
use mcp_tools::gmail::GmailModule;
use mcp_tools::knowledge::KnowledgeRepository;
use mcp_tools::memory::{MemoryService, PgMemoryStore};
use mcp_tools::monobank::Monobank;
use mcp_tools::news::{Habr, HackerNews, NewsProvider, NewsRepository, TelegramChannels};
use mcp_tools::scheduler::Scheduler;
use mcp_tools::server::{Deps, HttpOptions, McpTools};
use mcp_tools::settings::Settings;
use mcp_tools::signals::Signals;
use mcp_tools::skills::SkillCatalog;
use mcp_tools::telegram::{TelegramConfig, TelegramModule};
use mcp_tools::userbot::Userbot;
use mcp_tools::{fetch, fs, news, pg, telegram, toolsets};
use tokio_util::sync::CancellationToken;
use tracing_subscriber::EnvFilter;

const DEFAULT_GATEWAY_CONFIG: &str = "crates/mcp/gateway.config.json";
const DEFAULT_ALLOWED_HOSTS: &str = "localhost,127.0.0.1,::1";

fn env(name: &str) -> Option<String> {
    std::env::var(name).ok().filter(|v| !v.trim().is_empty())
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // `./.env` only, like dotenv/config. dotenvy::dotenv() would also walk up
    // parent directories and pick up some other checkout's secrets.
    let _ = dotenvy::from_path(".env");
    // Logs go to stderr: on the stdio transport stdout is the MCP channel.
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")))
        .with_writer(std::io::stderr)
        .init();

    // MCP_TOOLSETS narrows the surface for an instance serving one audience
    // (the ChatGPT tunnel). Unset → everything.
    let selection = toolsets::parse_toolsets(env("MCP_TOOLSETS").as_deref())?;
    if selection.restricted {
        let names: Vec<&str> = selection.names.iter().map(|t| t.name()).collect();
        tracing::info!(toolsets = names.join(","), "restricted tool surface");
    }
    // MCP_NO_POLLERS: tools only. The Telegram getUpdates poll is exclusive
    // per bot, so a second poller against the same bot would 409 the droplet.
    let pollers_enabled = env("MCP_NO_POLLERS").as_deref() != Some("1");

    let http = reqwest::Client::builder().build()?;
    let fetcher: fetch::SharedFetch = Arc::new(fetch::GuardedFetch::new());
    // Both databases must be up and migrated before any handler or poller
    // touches them: the news / memory store, and `mcp_state` beside it.
    let database_url = pg::database_url()?;
    let pool = pg::connect(&database_url)?;
    pg::migrate(&pool).await.context("applying pg migrations")?;
    let db = Db::connect(&database_url).await.context("opening the state database")?;
    let embedder: SharedEmbedder = Arc::new(OpenAiEmbedder::from_env(http.clone())?);
    let news_repo = NewsRepository::new(pool.clone(), embedder.clone());
    let settings = Settings::new(db.clone());
    let signals = Signals::new(db.clone());
    let scheduler = Scheduler::new(db.clone());
    let telegram = TelegramModule::new(TelegramConfig::from_env(), http.clone(), db.clone());
    let gmail = GmailModule::new(db.clone(), http.clone());
    let userbot = Userbot::new(db.clone());

    // Who this instance writes to shared memory as — a property of the
    // instance, not something a client declares.
    let memory_actor = env("MCP_MEMORY_ACTOR").unwrap_or_else(|| "mcp".into());
    let router = toolsets::compose_router(&selection.names);

    // A restricted instance never fronts upstreams: their tools arrive
    // namespaced at runtime and can't be expressed in the allow-list.
    let gateway = if selection.restricted {
        None
    } else {
        let path = env("GATEWAY_CONFIG").map_or_else(|| PathBuf::from(DEFAULT_GATEWAY_CONFIG), PathBuf::from);
        let upstreams = gateway::load_config(&path, &|name| std::env::var(name).ok())?;
        if upstreams.is_empty() {
            None
        } else {
            let own: Vec<String> = router.list_all().into_iter().map(|t| t.name.into_owned()).collect();
            Some(Arc::new(Gateway::connect(upstreams, &own).await))
        }
    };

    let deps = Arc::new(Deps {
        settings,
        signals: signals.clone(),
        scheduler,
        telegram: telegram.clone(),
        gmail: gmail.clone(),
        monobank: Monobank::new(http.clone()),
        userbot: Arc::new(userbot.clone()),
        skills: SkillCatalog::new(PathBuf::from("skills"), PathBuf::from("skills.default")),
        fetcher: fetcher.clone(),
        files: {
            let cwd = std::env::current_dir()?;
            fs::FileAccess::new(&fs::default_read_roots(&|name| env(name), &cwd), &cwd)
        },
        storage_dir: env("STORAGE_DIR").map_or_else(|| PathBuf::from("./storage"), PathBuf::from),
        news: news_repo.clone(),
        knowledge: KnowledgeRepository::new(pool.clone(), embedder.clone()),
        memory: MemoryService::new(Arc::new(PgMemoryStore::new(pool.clone())), embedder),
        memory_actor,
        gateway,
    });
    let make_tools = move || McpTools::new(deps.clone(), router.clone());

    let cancel = CancellationToken::new();
    tokio::spawn({
        let cancel = cancel.clone();
        async move {
            shutdown_signal().await;
            tracing::info!("shutting down");
            cancel.cancel();
        }
    });

    let mut background = tokio::task::JoinSet::new();
    // Keep-alives behind start_typing / telegram_send_status: part of the
    // tools, not pollers, so they run on every instance.
    background.spawn(telegram.typing.clone().run(cancel.clone()));
    background.spawn(telegram.status.clone().run(cancel.clone()));
    if pollers_enabled {
        background.spawn(telegram::run_poller(
            telegram.bot.clone(),
            telegram.log.clone(),
            signals.clone(),
            telegram.config.clone(),
            cancel.clone(),
        ));
        background.spawn(gmail.run_poller(signals.clone(), cancel.clone()));
        let providers: Vec<Box<dyn NewsProvider>> = vec![
            Box::new(HackerNews::new(http.clone(), fetcher.clone())),
            Box::new(Habr::new(http.clone(), fetcher.clone())),
            Box::new(TelegramChannels::new(Arc::new(userbot), pool.clone())),
        ];
        background.spawn(news::run_poller(providers, news_repo, cancel.clone()));
    } else {
        tracing::info!("MCP_NO_POLLERS=1 — tools only, pollers disabled");
    }

    let transport = env("MCP_TRANSPORT").unwrap_or_else(|| "stdio".into()).to_lowercase();
    let served = if transport == "http" {
        let bind_address =
            env("MCP_HOST").unwrap_or_else(|| "0.0.0.0".into()).parse().context("MCP_HOST must be an IP address")?;
        let port = env("MCP_PORT").map_or(Ok(3000), |p| p.parse()).context("MCP_PORT must be a port number")?;
        // "*" turns Host validation off — for a listener only reachable inside
        // the compose network, where the caller's Host header isn't ours to
        // predict (tunnel-client).
        let raw_hosts = env("MCP_ALLOWED_HOSTS").unwrap_or_else(|| DEFAULT_ALLOWED_HOSTS.into());
        let allowed_hosts = if raw_hosts.trim() == "*" {
            Vec::new()
        } else {
            raw_hosts.split(',').map(|h| h.trim().to_owned()).filter(|h| !h.is_empty()).collect()
        };
        let options = HttpOptions { bind_address, port, allowed_hosts, multi_session: selection.restricted };
        mcp_tools::server::serve_http(make_tools, options, cancel.clone()).await
    } else {
        mcp_tools::server::serve_stdio(make_tools(), cancel.clone()).await
    };

    // The transport ending (stdin closed, or a shutdown signal) ends the
    // process; background tasks observe the cancel and finish their tick.
    cancel.cancel();
    while background.join_next().await.is_some() {}
    served
}

async fn shutdown_signal() {
    let ctrl_c = tokio::signal::ctrl_c();
    #[cfg(unix)]
    {
        let mut term = match tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate()) {
            Ok(term) => term,
            Err(err) => {
                tracing::warn!(%err, "SIGTERM handler unavailable, listening for Ctrl-C only");
                let _ = ctrl_c.await;
                return;
            }
        };
        tokio::select! {
            _ = ctrl_c => {}
            _ = term.recv() => {}
        }
    }
    #[cfg(not(unix))]
    let _ = ctrl_c.await;
}

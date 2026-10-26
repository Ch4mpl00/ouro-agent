// Standalone workflow optimization and strict mock scenarios. No production
// MCP transport, workflow activation, schedule or persistent agent stores.
use std::{
    collections::BTreeSet,
    path::PathBuf,
    sync::{Arc, Mutex},
    time::Duration,
};

use agent::agent::Providers;
use agent::models::{ModelPreset, ReasoningEffort, default_presets};
use agent::providers::{
    DEEPSEEK_BASE_URL, GEMINI_BASE_URL, HttpApi, OPENAI_BASE_URL, ProviderKind, RetryOpts, SharedProvider,
    is_openrouter_model, openrouter_api, provider, with_retry,
};
use agent::skills::{FsSkillStore, SkillStore};
use agent::tracing::null_scope;
use evals::env::{load_env, root};
use evals::langfuse_api::{Langfuse, observation, trace_from};
use evals::optimizer::{
    Evidence, IterativeOptions, MAX_OPTIMIZER_TURNS, Meter, MeteredProvider, OptimizerTools, Scenario, Trial,
    Workbench, compact_trace, fenced_yaml, isolated_engine, optimize_iterative, prompt, proposal_yaml, propose,
    workbench_prompt,
};
use serde_json::{Value, json};
use tokio_util::sync::CancellationToken;

const USAGE: &str = "optimize <trace-id>... [--trace-file FULL-TRACE.json] [--definition FILE] [--model MODEL]\n\
    [--compose-model MODEL] [--out REPORT.md] [--artifact FILE.json] [--dump | --one-shot]\n\
    [--max-iterations 30] [--timeout-seconds SECONDS]\n\
    [--mock-only] [--catalog FILE.json]\n\
  optimize --scenario SCENARIO.json [--trace-file FULL-TRACE.json] [--catalog FILE.json]\n\
    [--mock-only | --compose-model MODEL] [--artifact FILE.json]\n\
  Default: iterative optimizer using local evidence/capabilities/try tools.\n\
  --dump: prompt only; --one-shot: original proposal-only generation, no tools.\n\
  --scenario: execute YAML/input/mocks directly, without an optimizer model.\n\
  Default real permission: llm_compose only; --mock-only removes it. Explicit mocks always win.\n\
  Catalog is host-supplied; absent schemas are marked unknown and writes are conservative.\n\
  LIMIT: standalone pipelines only; admitted signal/agent/guard/child/event contexts are rejected.\n\
  No live MCP fallback, real side effects, persistent workflow writes or activation.\n\
  At most 30 optimizer turns. No token, generation-count or overall time ceiling by default.\n\
  --timeout-seconds optionally adds an overall time limit. Usage is recorded; no monetary pricing.";

fn chat_provider(model: &str) -> anyhow::Result<SharedProvider> {
    let (kind, key_name, url) = if is_openrouter_model(model) {
        (ProviderKind::Openrouter, "OPEN_ROUTER_API_KEY", "")
    } else if model.starts_with("deepseek") {
        (ProviderKind::Deepseek, "DEEPSEEK_API_KEY", DEEPSEEK_BASE_URL)
    } else if model.starts_with("gemini") {
        (ProviderKind::Gemini, "GEMINI_API_KEY", GEMINI_BASE_URL)
    } else {
        (ProviderKind::Openai, "OPENAI_API_KEY", OPENAI_BASE_URL)
    };
    let key = std::env::var(key_name)
        .ok()
        .filter(|k| !k.is_empty())
        .ok_or_else(|| anyhow::anyhow!("{key_name} missing in env"))?;
    let api = if kind == ProviderKind::Openrouter {
        openrouter_api(&key)
    } else {
        let url = if kind == ProviderKind::Openai {
            std::env::var("OPENAI_BASE_URL").unwrap_or_else(|_| url.into())
        } else {
            url.into()
        };
        HttpApi::new(&url, &key)
    };
    Ok(Arc::new(with_retry(Arc::new(provider(kind, Arc::new(api))), RetryOpts::default())))
}

struct NoModel;
#[async_trait::async_trait]
impl agent::providers::ChatProvider for NoModel {
    fn kind(&self) -> ProviderKind {
        ProviderKind::Openai
    }
    async fn complete(
        &self,
        _: &mut agent::providers::CompletionParams<'_>,
    ) -> anyhow::Result<agent::providers::CompletionResult> {
        anyhow::bail!("models disabled by mock-only scenario policy")
    }
}

fn configured_presets() -> agent::models::Presets {
    let mut presets = default_presets();
    for (key, preset) in [
        ("BASE", &mut presets.base),
        ("SMART", &mut presets.smart),
        ("SMARTEST", &mut presets.smartest),
        ("COMPOSE", &mut presets.compose),
    ] {
        if let Ok(model) = std::env::var(format!("AGENT_{key}_MODEL")) {
            preset.model = model;
        }
    }
    presets
}

struct Unavailable(String);
#[async_trait::async_trait]
impl agent::providers::ChatProvider for Unavailable {
    fn kind(&self) -> ProviderKind {
        ProviderKind::Openai
    }
    async fn complete(
        &self,
        _: &mut agent::providers::CompletionParams<'_>,
    ) -> anyhow::Result<agent::providers::CompletionResult> {
        anyhow::bail!("{}", self.0)
    }
}

// Construct each family independently at startup. Missing credentials become
// a normal generation failure only if that family is actually requested; a
// mock-only scenario requires no credentials at all. Candidate presets retain
// their own model contracts rather than borrowing the outer optimizer model.
fn configured_providers(meter: &Arc<Mutex<Meter>>, disabled: bool) -> Providers {
    let family = |model| -> SharedProvider {
        let inner: SharedProvider = if disabled {
            Arc::new(NoModel)
        } else {
            chat_provider(model).unwrap_or_else(|e| Arc::new(Unavailable(e.to_string())))
        };
        Arc::new(MeteredProvider { provider: inner, meter: meter.clone() })
    };
    Providers {
        openai: family("gpt"),
        deepseek: family("deepseek"),
        gemini: family("gemini"),
        openrouter: Some(family("openrouter/provider/model")),
    }
}

/// The proposed definition and where it came from. A final message that only
/// points back at a definition shown earlier ("the full YAML is above") still
/// has one: the last candidate it ran.
fn final_definition(report: &str, trials: &[Trial]) -> anyhow::Result<(String, &'static str)> {
    let (yaml, source) = match fenced_yaml(report)? {
        Some(yaml) => (yaml, "report"),
        None => trials
            .last()
            .map(|t| (t.scenario.yaml.clone(), "last_trial"))
            .ok_or_else(|| anyhow::anyhow!("optimizer returned no fenced YAML definition and ran no candidate"))?,
    };
    ows::model::parse_yaml(&yaml)?;
    Ok((yaml, source))
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    load_env();
    let mut args = std::env::args().skip(1);
    let (mut ids, mut files) = (Vec::new(), Vec::new());
    let (mut definition, mut output, mut artifact, mut catalog, mut scenario) = (None, None, None, None, None);
    let (mut dump, mut one_shot, mut mock_only) = (false, false, false);
    let (mut model, mut compose_model) = (None, None);
    let (mut iterations, mut seconds) = (MAX_OPTIMIZER_TURNS, None::<u64>);
    while let Some(arg) = args.next() {
        let mut next = || args.next().ok_or_else(|| anyhow::anyhow!("{arg} requires a value"));
        match arg.as_str() {
            "--definition" => definition = Some(PathBuf::from(next()?)),
            "--out" => output = Some(PathBuf::from(next()?)),
            "--artifact" => artifact = Some(PathBuf::from(next()?)),
            "--catalog" => catalog = Some(PathBuf::from(next()?)),
            "--scenario" => scenario = Some(PathBuf::from(next()?)),
            "--trace-file" => files.push(PathBuf::from(next()?)),
            "--model" => model = Some(next()?),
            "--compose-model" => compose_model = Some(next()?),
            "--max-iterations" => iterations = next()?.parse()?,
            "--timeout-seconds" => seconds = Some(next()?.parse()?),
            "--dump" => dump = true,
            "--one-shot" => one_shot = true,
            "--mock-only" => mock_only = true,
            "--help" | "-h" => {
                println!("{USAGE}");
                return Ok(());
            }
            flag if flag.starts_with('-') => anyhow::bail!("unknown option: {flag}\n{USAGE}"),
            id => ids.push(id.to_owned()),
        }
    }
    anyhow::ensure!(scenario.is_some() || !ids.is_empty() || !files.is_empty(), "{USAGE}");
    anyhow::ensure!(
        (1..=MAX_OPTIMIZER_TURNS).contains(&iterations),
        "optimizer turn limit must be between 1 and {MAX_OPTIMIZER_TURNS}"
    );
    anyhow::ensure!(seconds.is_none_or(|seconds| seconds > 0), "timeout must be positive");
    anyhow::ensure!(
        !(dump && one_shot) && !(scenario.is_some() && (dump || one_shot)),
        "scenario, dump and one-shot modes are exclusive"
    );
    let root = root()?;
    let skills: Arc<dyn SkillStore> = Arc::new(FsSkillStore::new(&root));
    let cancel = CancellationToken::new();
    let signal = cancel.clone();
    let ctrl_c = tokio::spawn(async move {
        if tokio::signal::ctrl_c().await.is_ok() {
            signal.cancel();
        }
    });
    let mut evidence = Evidence::default();
    let mut traces = Vec::new();
    for file in files {
        let raw: Value = serde_json::from_slice(&std::fs::read(file)?)?;
        let observations = raw["observations"]
            .as_array()
            .ok_or_else(|| anyhow::anyhow!("full trace file requires observations array"))?
            .iter()
            .cloned()
            .map(observation)
            .collect::<anyhow::Result<Vec<_>>>()?;
        let trace = trace_from(&raw, vec![]);
        evidence.add(&trace, &observations);
        traces.push(compact_trace(&trace, &observations));
    }
    if !ids.is_empty() {
        let source = Langfuse::from_env()?;
        for id in ids {
            let (trace, observations) = agent::errors::or_cancel(&cancel, source.trace_by_id(&id)).await?;
            let compact = compact_trace(&trace, &observations);
            eprintln!("[optimize] {id}: {} observations; models={}", observations.len(), compact["models"]);
            evidence.add(&trace, &observations);
            traces.push(compact);
        }
    }
    let mut host_catalog = evidence.catalog();
    if let Some(path) = catalog {
        let explicit: Vec<evals::optimizer::Capability> = serde_json::from_slice(&std::fs::read(path)?)?;
        let mut names = BTreeSet::new();
        for capability in explicit {
            anyhow::ensure!(
                names.insert(capability.name.clone()),
                "duplicate explicit catalog name: {}",
                capability.name
            );
            host_catalog.0.retain(|c| c.name != capability.name);
            host_catalog.0.push(capability);
        }
    }
    let definition =
        std::fs::read_to_string(definition.unwrap_or_else(|| root.join("crates/agent/workflows/agent-loop.yaml")))?;
    let proposal_prompt = if scenario.is_none() {
        let author =
            skills.read_skill("workflow-author").await?.ok_or_else(|| anyhow::anyhow!("workflow-author missing"))?;
        let optimizer = skills
            .read_skill("workflow-optimizer")
            .await?
            .ok_or_else(|| anyhow::anyhow!("workflow-optimizer missing"))?;
        Some(if one_shot || dump {
            prompt(&author.body, &optimizer.body, &definition, &traces)?
        } else {
            workbench_prompt(&author.body, &optimizer.body, &definition, &traces)?
        })
    } else {
        None
    };
    if dump {
        let prompt = proposal_prompt.as_ref().unwrap();
        let report = format!("# System\n\n{}\n\n# User\n\n{}\n", prompt.system, prompt.user);
        println!("{report}");
        if let Some(path) = output {
            std::fs::write(path, report)?;
        }
        ctrl_c.abort();
        return Ok(());
    }
    let model = model
        .unwrap_or_else(|| std::env::var("AGENT_SMARTEST_MODEL").unwrap_or_else(|_| default_presets().smartest.model));
    let compose_model = compose_model
        .unwrap_or_else(|| std::env::var("AGENT_COMPOSE_MODEL").unwrap_or_else(|_| default_presets().compose.model));
    let meter = Arc::new(Mutex::new(Meter::default()));
    let metered = |provider| -> SharedProvider { Arc::new(MeteredProvider { provider, meter: meter.clone() }) };
    let primary: SharedProvider = if scenario.is_some() { Arc::new(NoModel) } else { metered(chat_provider(&model)?) };
    if one_shot {
        let preset = ModelPreset { model, reasoning_effort: ReasoningEffort::Max };
        let result = propose(&primary, &preset, proposal_prompt.as_ref().unwrap(), &null_scope(), cancel).await?;
        println!("{}", result.report);
        if let Some(path) = output {
            std::fs::write(path, &result.report)?;
        }
        proposal_yaml(&result.report)?;
        eprintln!("[optimize] schema passed; proposal only, no execution or function-contract verification");
        ctrl_c.abort();
        return Ok(());
    }
    let mut candidate_presets = configured_presets();
    candidate_presets.compose.model = compose_model.clone();
    let candidate_engine = isolated_engine(
        configured_providers(&meter, mock_only),
        candidate_presets.clone(),
        skills.clone(),
        Arc::new(host_catalog.clone()),
    )?;
    let adapter = agent::flows::standalone(
        &candidate_engine,
        &null_scope().span(agent::tracing::SpanStart::new("workbench", agent::tracing::SpanKind::Chain)),
    );
    let allowed = if mock_only { BTreeSet::new() } else { BTreeSet::from(["llm_compose".into()]) };
    let mut workbench = Workbench::new(
        adapter,
        host_catalog,
        evidence.clone(),
        allowed,
        json!({"presets": {"base":{"model":candidate_presets.base.model,"effort":candidate_presets.base.reasoning_effort.as_str()},"smart":{"model":candidate_presets.smart.model,"effort":candidate_presets.smart.reasoning_effort.as_str()},"smartest":{"model":candidate_presets.smartest.model,"effort":candidate_presets.smartest.reasoning_effort.as_str()},"compose":{"model":candidate_presets.compose.model,"effort":candidate_presets.compose.reasoning_effort.as_str()}},"mock_only":mock_only}),
    )?;
    workbench.timeout = Duration::from_secs(seconds.unwrap_or(120).min(120));
    workbench.meter = Some(meter.clone());
    let workbench = Arc::new(workbench);
    let tools = Arc::new(OptimizerTools::new(workbench.clone(), evidence));
    let artifact = artifact.unwrap_or_else(|| {
        output.as_ref().map(|p| p.with_extension("artifact.json")).unwrap_or_else(|| {
            std::env::temp_dir().join(format!("optimizer-{}.json", chrono::Utc::now().format("%Y%m%dT%H%M%S%f")))
        })
    });
    anyhow::ensure!(output.as_ref() != Some(&artifact), "report and artifact paths must differ");
    let (report, error, mode) = if let Some(path) = scenario {
        let scenario: Scenario = serde_json::from_slice(&std::fs::read(path)?)?;
        let args = serde_json::to_value(scenario)?.as_object().unwrap().clone();
        let report = agent::mcp::Mcp::call_tool(&*tools, "optimizer_try", args, &cancel).await?;
        (report, None, "scenario")
    } else {
        let mut optimizer_presets = configured_presets();
        optimizer_presets.smartest = ModelPreset { model: model.clone(), reasoning_effort: ReasoningEffort::Max };
        let optimizer_providers = Providers {
            openai: primary.clone(),
            deepseek: primary.clone(),
            gemini: primary.clone(),
            openrouter: Some(primary.clone()),
        };
        let engine = isolated_engine(optimizer_providers, optimizer_presets, skills, tools.clone())?;
        eprintln!(
            "[optimize] iterative model={model}; real={}; standalone workflows only",
            if mock_only { "none" } else { "llm_compose" }
        );
        let result = optimize_iterative(
            &engine,
            proposal_prompt.as_ref().unwrap(),
            &IterativeOptions { max_iterations: iterations, timeout: seconds.map(Duration::from_secs) },
            cancel.clone(),
        )
        .await;
        engine.shutdown().await;
        match result {
            Ok(report) => (report, None, "iterative"),
            Err(error) => (String::new(), Some(format!("{error:#}")), "iterative"),
        }
    };
    let yaml = (mode == "iterative" && error.is_none()).then(|| final_definition(&report, &tools.trials()));
    let found: Option<&(String, &str)> = yaml.as_ref().and_then(|y| y.as_ref().ok());
    let final_tested = found.is_some_and(|(y, _)| {
        tools.trials().iter().any(|t| t.scenario.yaml.trim() == y.trim() && t.report["status"] == "completed")
    });
    let artifact_value = json!({"version":1,"mode":mode,"report":report,"error":error,"definition":found.map(|(y, _)| y),"definition_source":found.map(|(_, s)| s),"final_candidate_completed":final_tested,"capabilities":workbench.capabilities(),"trials":tools.trials(),"usage":meter.lock().unwrap().clone(),"cost":null});
    std::fs::write(&artifact, serde_json::to_string_pretty(&artifact_value)?)?;
    let report = match found {
        Some((yaml, "last_trial")) => {
            format!("{report}\n\n## Definition (the last candidate run)\n\n```yaml\n{yaml}```\n")
        }
        _ => report,
    };
    println!("{report}");
    if let Some(path) = output {
        std::fs::write(path, &report)?;
    }
    eprintln!("[optimize] full artifact: {}", artifact.display());
    candidate_engine.shutdown().await;
    ctrl_c.abort();
    if let Some(error) = error {
        anyhow::bail!("{error}");
    }
    if let Some(yaml) = yaml {
        let (_, source) = yaml?;
        if source == "last_trial" {
            eprintln!("[optimize] the final message had no YAML; using the last candidate run");
        }
        eprintln!("[optimize] final candidate completed in workbench: {final_tested}");
    }
    Ok(())
}
